# -*- coding: utf-8 -*-
"""
Copyline → Satu YML (flat <offers>), версия: copyline-2025-09-19.6

Основано на исходном рабочем скрипте пользователя (cl.txt), приведено к общему
шаблону проекта:
- фильтр по ПРЕФИКСАМ из docs/copyline_keywords.txt (товарное имя должно НАЧИНАТЬСЯ с ключа);
- из XLSX берём имя, артикул, цену; категории/заголовки отсекаем;
- для каждого артикула находим КАРТОЧКУ и забираем:
  * picture:  img[itemprop="image"][id^="main_image_"] @src  (fallback: meta[property=og:image])
  * vendor:   div[itemprop="brand"] span[itemprop="name"]     (fallback: .manufacturer_name)
  * desc+ТХ:  div[itemprop="description"].jshop_prod_description → <p> + таблицы
- пишем YML в windows-1251, вверху FEED_META по-русски.
"""

from __future__ import annotations
import os, re, io, time, html, unicodedata, hashlib, random
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from xml.etree import ElementTree as ET

# ---------------- настройки ----------------
SCRIPT_VERSION = "copyline-2025-09-19.6"

BASE_URL  = "https://copyline.kz"
XLSX_URL  = os.getenv("SUPPLIER_URL", f"{BASE_URL}/files/price-CLA.xlsx")
OUT_FILE  = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC       = (os.getenv("OUTPUT_ENCODING", "windows-1251") or "windows-1251")
TIMEOUT   = int(os.getenv("TIMEOUT_S", "25"))
RETRIES   = int(os.getenv("RETRIES", "4"))
BACKOFF   = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES = int(os.getenv("MIN_BYTES", "900"))

KEYS_PATH   = os.getenv("COPYLINE_KEYWORDS_PATH", "docs/copyline_keywords.txt")
KEYS_MODE   = os.getenv("COPYLINE_KEYWORDS_MODE", "include").lower()  # include|exclude
ALLOW_TRIM  = os.getenv("COPYLINE_PREFIX_ALLOW_TRIM", "1").lower() in {"1","true","yes"}

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "CL")
FILL_DESC_FROM_NAME = os.getenv("FILL_DESC_FROM_NAME", "1").lower() in {"1","true","yes"}

UA = {"User-Agent": "Mozilla/5.0 (compatible; CopylineFeed/1.0)"}

# ---------------- утилиты ----------------
def log(s): print(s, flush=True)
def warn(s): print(f"WARN: {s}", flush=True)
def die(s):  print(f"ERROR: {s}", flush=True); raise SystemExit(1)

def now_utc(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return time.strftime("%Y-%m-%d %H:%M:%S")

def nfkc(s: str): return unicodedata.normalize("NFKC", s or "")
def norm(s: str) -> str:
    s = nfkc(s).replace("\u00A0", " ").replace("ё","е").strip().lower()
    return re.sub(r"\s+", " ", s)

def http_get(url: str) -> Optional[bytes]:
    last = None
    for a in range(1, RETRIES+1):
        try:
            r = requests.get(url, headers=UA, timeout=TIMEOUT)
            if r.status_code == 200 and len(r.content) >= (MIN_BYTES if url.endswith(".xlsx") else 1):
                return r.content
            last = f"http {r.status_code} size={len(r.content)}"
        except Exception as e:
            last = repr(e)
        time.sleep(BACKOFF * a * (1 + random.uniform(-0.2, 0.2)))
    warn(f"http_get failed: {url} | {last}")
    return None

def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "html.parser")

def to_price(x) -> Optional[float]:
    if x is None: return None
    s = str(x).replace("\xa0"," ").strip()
    s = s.replace(" ", "").replace("₸","").replace("KZT","").replace("kzt","").replace(",",".")
    if not re.search(r"\d", s): return None
    try:
        v = float(s)
        return v if v > 0 else None
    except Exception:
        m = re.search(r"[\d.]+", s)
        return float(m.group(0)) if m else None

def is_category_row(name: str, sku: str, price: Optional[float]) -> bool:
    if not name: return True
    if not sku or price is None: return True
    # кричащие заголовки без цифр короткие — вероятные категории
    letters = [c for c in name if c.isalpha()]
    if letters:
        upp = sum(1 for c in letters if c.upper()==c)
        if upp/len(letters) > 0.95 and not re.search(r"\d", name) and len(name) <= 64:
            return True
    return False

# ---------------- ключевые слова (строгий префикс) ----------------
def load_keywords(path: str) -> List[str]:
    data = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f:
                data = f.read()
            break
        except Exception:
            continue
    if data is None:
        return []
    out=[]
    for ln in data.splitlines():
        s = ln.strip()
        if s and not s.startswith("#"):
            out.append(norm(s))
    return out

def startswith_any(title: str, keys: List[str]) -> bool:
    if not keys: return True
    t = norm(title)
    if ALLOW_TRIM:
        t = re.sub(r'^[\s\-\–\—•·|:/\\\[\]\(\)«»"“”„\']+', "", t)
    return any(t.startswith(k) for k in keys)

# ---------------- XLSX ----------------
def load_xlsx(url: str):
    b = http_get(url)
    if not b: die("Не удалось скачать XLSX.")
    wb = load_workbook(io.BytesIO(b), data_only=True, read_only=True)
    # авто-подбор листа с шапкой в 2 строки: "Номенклатура" + "Артикул/Цена"
    best = (None, {}, -1)
    for ws in wb.worksheets:
        mapping, header_r = find_header(ws)
        if mapping and header_r > 0:
            best = (ws, mapping, header_r)
            break
    ws, mapping, header_row = best
    if not ws: die("Не удалось найти шапку.")
    return ws, mapping, header_row

def find_header(ws) -> Tuple[Dict[str,int], int]:
    def low(v): return str(v or "").strip().lower()
    for r in range(1, 80):  # сканим первые ~80 строк
        row0 = [low(ws.cell(r, c).value) for c in range(1, 40)]
        row1 = [low(ws.cell(r+1, c).value) for c in range(1, 40)]
        if any("номенклатура" in v for v in row0):
            name_col = next((i for i,v in enumerate(row0, start=1) if "номенклатура" in v or "наимен" in v), None)
            sku_col  = next((i for i,v in enumerate(row1, start=1) if "артикул" in v), None)
            price_col= next((i for i,v in enumerate(row1, start=1) if "цена" in v or "опт" in v), None)
            if name_col and sku_col and price_col:
                return {"name":name_col,"sku":sku_col,"price":price_col}, r
    return {}, -1

# ---------------- поиск карточки и скрейп ----------------
def fetch_html(url: str) -> Optional[str]:
    b = http_get(url)
    return b.decode("utf-8","replace") if b else None

def find_product_page_by_article(art: str) -> Optional[str]:
    if not art: return None
    candidates = [
        f"{BASE_URL}/search/?searchstring={art}",
        f"{BASE_URL}/search?searchstring={art}",
        f"{BASE_URL}/?searchstring={art}",
    ]
    for url in candidates:
        html = fetch_html(url)
        if not html: continue
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select("a[href]"):
            href = a.get("href","")
            if not href or href.startswith("#"): continue
            href = urljoin(BASE_URL, href)
            # берём первую, которая похожа на товар и содержит art в тексте/ссылке
            if re.search(r"/goods/[^/]+\.html", href) and (art.lower() in href.lower() or art.lower() in a.get_text(" ", strip=True).lower()):
                # провалимся и валидацией проверим
                if is_product_page(href):
                    return href
    return None

def is_product_page(url: str) -> bool:
    html = fetch_html(url)
    if not html: return False
    soup = BeautifulSoup(html, "html.parser")
    # сигналы карточки: блок описания и/или og:type=product и главная картинка
    has_desc = bool(soup.select_one('div[itemprop="description"].jshop_prod_description'))
    og_prod  = soup.select_one('meta[property="og:type"][content="product"]') is not None
    has_img  = soup.select_one('img[itemprop="image"][id^="main_image_"]') is not None
    return has_desc or (og_prod and has_img)

def scrape_product(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Возвращает (picture, vendor, description_full)."""
    html = fetch_html(url)
    if not html: return None, None, None
    soup = BeautifulSoup(html, "html.parser")

    # фото
    picture = None
    img = soup.select_one('img[itemprop="image"][id^="main_image_"]')
    if img and img.get("src"):
        picture = urljoin(BASE_URL, img["src"].strip())
    if not picture:
        og = soup.select_one('meta[property="og:image"]')
        if og and og.get("content"):
            picture = og["content"].strip()

    # бренд
    vendor = None
    v = soup.select_one('div[itemprop="brand"] [itemprop="name"]')
    if v:
        vendor = v.get_text(" ", strip=True)
    if not vendor:
        manu = soup.select_one(".manufacturer_name")
        if manu:
            vendor = manu.get_text(" ", strip=True)

    # описание + ТХ
    desc_full = None
    block = soup.select_one('div[itemprop="description"].jshop_prod_description') \
         or soup.select_one("div.jshop_prod_description") \
         or soup.select_one('[itemprop="description"]')
    if block:
        parts: List[str] = []
        # тексты
        for p in block.find_all(["p","h3","h4","h5","ul","ol"], recursive=False):
            tag = p.name.lower()
            if tag in {"p","h3","h4","h5"}:
                t = p.get_text(" ", strip=True)
                if t: parts.append(t)
            elif tag in {"ul","ol"}:
                for li in p.find_all("li", recursive=False):
                    t = li.get_text(" ", strip=True)
                    if t: parts.append(f"- {t}")
        # таблицы как «- K: V»
        specs = []
        for tbl in block.find_all("table"):
            for tr in tbl.find_all("tr"):
                cells = tr.find_all(["th","td"])
                if len(cells) >= 2:
                    k = cells[0].get_text(" ", strip=True)
                    v = cells[1].get_text(" ", strip=True)
                    if k and v: specs.append(f"- {k}: {v}")
        if specs:
            # добавим заголовок, если его нет в free-тексте
            if not any("технические характеристики" in norm(x) for x in parts):
                parts.append("Технические характеристики:")
            parts.extend(specs)

        desc_full = "\n".join([re.sub(r"\s+"," ",x).strip() for x in parts if x]).strip()
        if desc_full and re.match(r"^https?://", desc_full):  # защита
            desc_full = None

    return picture, vendor, (desc_full or None)

# ---------------- FEED_META ----------------
def feed_meta(stats: Dict[str,str]) -> str:
    order = [
        "script_version","supplier","source",
        "rows_read","rows_after_cat","rows_after_keys",
        "offers_written","picture_found","vendor_found","desc_found",
        "built_utc","built_Asia/Almaty",
    ]
    comments = {
        "script_version":"Версия скрипта",
        "supplier":"Метка поставщика",
        "source":"URL исходного XLSX",
        "rows_read":"Строк считано (после шапки)",
        "rows_after_cat":"После удаления категорий/шапок",
        "rows_after_keys":"После фильтра по префиксам",
        "offers_written":"Офферов записано в YML",
        "picture_found":"Сколько товаров получили фото",
        "vendor_found":"Сколько товаров получили бренд",
        "desc_found":"Сколько товаров получили описание/ТХ",
        "built_utc":"Время сборки (UTC)",
        "built_Asia/Almaty":"Время сборки (Алматы)",
    }
    mk = max(len(k) for k in order)
    left = [f"{k.ljust(mk)} = {stats.get(k,'n/a')}" for k in order]
    ml = max(len(x) for x in left)
    lines = ["FEED_META"]
    for k, l in zip(order, left):
        lines.append(f"{l.ljust(ml)}  | {comments[k]}")
    return "\n".join(lines)

# ---------------- main ----------------
def main():
    log(f"Source: {XLSX_URL}")
    ws, mapping, header_row = load_xlsx(XLSX_URL)

    keys = load_keywords(KEYS_PATH)
    if KEYS_MODE == "include" and not keys:
        die("COPYLINE_KEYWORDS_MODE=include, но ключей нет в docs/copyline_keywords.txt")

    name_c = mapping["name"]; sku_c = mapping["sku"]; price_c = mapping["price"]

    rows_read = rows_after_cat = rows_after_keys = 0
    items: List[Dict[str,Any]] = []

    for r in range(header_row + 2, ws.max_row + 1):
        name  = str(ws.cell(r, name_c).value or "").strip()
        sku   = str(ws.cell(r, sku_c).value or "").strip()
        price = to_price(ws.cell(r, price_c).value)

        if name or sku or price is not None:
            rows_read += 1
        if is_category_row(name, sku, price):
            continue
        rows_after_cat += 1

        ok = startswith_any(name, keys)
        if (KEYS_MODE == "include" and not ok) or (KEYS_MODE == "exclude" and ok):
            continue
        rows_after_keys += 1
        items.append({"name": name, "sku": sku, "price": price})

    # XML
    root = ET.Element("yml_catalog", date=time.strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(root, "shop")
    offers = ET.SubElement(shop, "offers")

    pic_cnt = ven_cnt = desc_cnt = 0

    for it in items:
        name, sku, price = it["name"], it["sku"], it["price"]

        picture = vendor = desc = None
        page = find_product_page_by_article(sku)
        if page:
            pic, ven, des = scrape_product(page)
            if pic:    picture = pic;    pic_cnt += 1
            if ven:    vendor  = ven;    ven_cnt += 1
            if des:    desc    = des;    desc_cnt += 1

        offer = ET.SubElement(offers, "offer", {"id": sku})
        ET.SubElement(offer, "name").text = name
        if vendor:
            ET.SubElement(offer, "vendor").text = vendor
        ET.SubElement(offer, "vendorCode").text = f"{VENDORCODE_PREFIX}{sku}"
        if picture:
            ET.SubElement(offer, "picture").text = picture
        ET.SubElement(offer, "description").text = desc if desc else (name if FILL_DESC_FROM_NAME else "")
        if price is not None:
            ET.SubElement(offer, "price").text = str(int(price))
            ET.SubElement(offer, "currencyId").text = "KZT"
        ET.SubElement(offer, "available").text = "true"

    try:
        ET.indent(root, space="  ")
    except Exception:
        pass

    stats = {
        "script_version": SCRIPT_VERSION,
        "supplier": "copyline",
        "source": XLSX_URL,
        "rows_read": str(rows_read),
        "rows_after_cat": str(rows_after_cat),
        "rows_after_keys": str(rows_after_keys),
        "offers_written": str(len(list(offers.findall("offer")))),
        "picture_found": str(pic_cnt),
        "vendor_found": str(ven_cnt),
        "desc_found": str(desc_cnt),
        "built_utc": now_utc(),
        "built_Asia/Almaty": now_almaty(),
    }
    root.insert(0, ET.Comment(feed_meta(stats)))

    xml = ET.tostring(root, encoding=ENC, xml_declaration=True).decode(ENC, "replace")
    # чтобы не было "--><shop>"
    xml = re.sub(r"(-->)\s*(<shop>)", r"\1\n  \2", xml)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=ENC, newline="\n") as f:
        f.write(xml)

    log(f"Wrote: {OUT_FILE} | offers={len(list(offers.findall('offer')))} | encoding={ENC}")

if __name__ == "__main__":
    main()
