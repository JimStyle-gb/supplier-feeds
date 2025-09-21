# -*- coding: utf-8 -*-
"""
Copyline → Satu YML (flat <offers>)
script_version = copyline-2025-09-21.1

Главные правила (как ты просил):
- Ищем карточку ТОЛЬКО по артикулу из прайса.
- <picture> берём СТРОГО из <img itemprop="image" id="main_image_*" src="..."> → @src.
  (Если блока нет — фото НЕ пишем; никаких ог-фолбэков.)
- <description> берём из <div itemprop="description" class="jshop_prod_description">: все <p>/<h3–h5>,
  списки и таблицы. Таблицы превращаем в список "- Ключ: Значение" и добавляем заголовок "Технические характеристики:".
- <vendor> берём из блока бренда: <div itemprop="brand"><span itemprop="name">...</span> (или .manufacturer_name).
- Фильтр по словам из docs/copyline_keywords.txt (по умолчанию: "contains"; можно переключить на "startswith").
- Пишем один файл: docs/copyline.yml (Windows-1251). Вверху FEED_META на русском.
"""

from __future__ import annotations
import os, re, io, time, random, unicodedata
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# ===================== НАСТРОЙКИ =====================

SCRIPT_VERSION = "copyline-2025-09-21.1"

BASE_URL     = "https://copyline.kz"
SUPPLIER_URL = os.getenv("SUPPLIER_URL", f"{BASE_URL}/files/price-CLA.xlsx")

OUT_FILE        = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S   = int(os.getenv("TIMEOUT_S", "25"))
RETRIES     = int(os.getenv("RETRIES", "4"))
RETRY_BACK  = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES   = int(os.getenv("MIN_BYTES", "1200"))

# Файл с ключевыми словами (по одному в строке)
KEYWORDS_PATH = os.getenv("COPYLINE_KEYWORDS_PATH", "docs/copyline_keywords.txt")
# Режим сопоставления по ключам: include (оставить совпавшие) / exclude (отбросить совпавшие)
KEYWORDS_MODE = os.getenv("COPYLINE_KEYWORDS_MODE", "include").lower()  # include|exclude
# Тип совпадения: contains (по умолчанию) или startswith (как у Akcent)
KEYWORDS_MATCH = os.getenv("COPYLINE_MATCH_MODE", "contains").lower()   # contains|startswith
# Разрешить чистку шумовых символов в начале названия (тире, точки, кавычки и т.п.)
PREFIX_TRIM_NOISE = os.getenv("COPYLINE_PREFIX_ALLOW_TRIM", "1").lower() in {"1","true","yes"}

# Если описания не нашли на карточке — подставить name
FILL_DESC_FROM_NAME = os.getenv("FILL_DESC_FROM_NAME", "1").lower() in {"1","true","yes"}

# Префикс для vendorCode
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "CL")

UA = {"User-Agent": "Mozilla/5.0 (compatible; CopylineFeed/2025.09)"}

# ===================== УТИЛИТЫ =====================

def log(msg: str): print(msg, flush=True)
def warn(msg: str): print(f"WARN: {msg}", flush=True)
def die(msg: str):  print(f"ERROR: {msg}", flush=True); raise SystemExit(1)

def now_utc(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return time.strftime("%Y-%m-%d %H:%M:%S")

def _nfkc(s: str): return unicodedata.normalize("NFKC", s or "")

def _norm(s: str) -> str:
    s = _nfkc(s).replace("\u00A0"," ").replace("ё","е").strip().lower()
    return re.sub(r"\s+", " ", s)

def parse_money(x) -> Optional[float]:
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

# ===================== HTTP =====================

def fetch_bytes(url: str) -> Optional[bytes]:
    last = None
    for i in range(1, RETRIES+1):
        try:
            r = requests.get(url, headers=UA, timeout=TIMEOUT_S)
            if r.status_code == 200 and (len(r.content) >= MIN_BYTES if url.endswith(".xlsx") else True):
                return r.content
            last = f"http {r.status_code} size={len(r.content)}"
        except Exception as e:
            last = repr(e)
        time.sleep(RETRY_BACK * i * (1 + random.uniform(-0.2, 0.2)))
    warn(f"fetch failed: {url} | {last}")
    return None

def fetch_html(url: str) -> Optional[str]:
    b = fetch_bytes(url)
    return b.decode("utf-8", "replace") if b else None

# ===================== XLSX: ШАПКА И ЧТЕНИЕ =====================

def merge_two_rows(r1: List[str], r2: List[str]) -> List[str]:
    out = []
    n = max(len(r1), len(r2))
    for i in range(n):
        a = (r1[i] if i < len(r1) else "") or ""
        b = (r2[i] if i < len(r2) else "") or ""
        a, b = a.strip(), b.strip()
        out.append(f"{a}.{b}" if a and b else (b or a))
    return out

def map_headers(vals: List[str]) -> Dict[int, str]:
    m = {}
    for idx, raw in enumerate(vals, start=1):
        v = _norm(raw)
        if not v: continue
        if ("наимен" in v) or v == "номенклатура": m[idx] = "name"
        if "артикул" in v:                         m[idx] = "sku"
        if "цена"    in v:                         m[idx] = "price"
    return m

def find_header(ws: Worksheet, scan_rows: int = 80, max_cols: int = 40):
    best_map, best_row, best_score = {}, -1, -1
    for r in range(1, scan_rows):
        vals1 = [str(ws.cell(r, c).value or "").strip() for c in range(1, max_cols+1)]
        vals2 = [str(ws.cell(r+1, c).value or "").strip() for c in range(1, max_cols+1)]
        merged = merge_two_rows(vals1, vals2)
        for vals in (vals1, merged):
            m = map_headers(vals)
            score = len([f for f in m.values() if f in {"name","sku","price"}])
            if ("name" in m.values()) and ("sku" in m.values()):
                if score > best_score:
                    best_map, best_row, best_score = m, r, score
    return best_map, best_row

def select_best_sheet(wb):
    best = (None, {}, -1, -1)
    for ws in wb.worksheets:
        m, r = find_header(ws)
        score = len([f for f in m.values() if f in {"name","sku","price"}])
        if score > best[3]:
            best = (ws, m, r, score)
    ws, m, r, _ = best
    if not ws or not m or r < 1:
        die("Не удалось найти шапку.")
    return ws, m, r

# ===================== КЛЮЧЕВЫЕ СЛОВА =====================

def load_keywords(path: str) -> List[str]:
    if not os.path.exists(path): return []
    data = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f:
                data = f.read()
            data = data.replace("\ufeff","").replace("\x00","")
            break
        except Exception:
            continue
    if data is None:
        with open(path,"r",encoding="utf-8",errors="ignore") as f:
            data = f.read().replace("\x00","")
    keys=[]
    for ln in data.splitlines():
        s = ln.strip()
        if s and not s.startswith("#"):
            keys.append(_norm(s))
    return keys

def name_matches(name: str, keys: List[str]) -> bool:
    if not keys: return True
    nm = _norm(name)
    if PREFIX_TRIM_NOISE:
        nm = re.sub(r'^[\s\-\–\—•·|:/\\\[\]\(\)«»"“”„\']+', "", nm)
    if KEYWORDS_MATCH == "startswith":
        return any(nm.startswith(k) for k in keys)
    # contains (по умолчанию)
    return any(k in nm for k in keys)

# ===================== ОТСЕВ КАТЕГОРИЙ =====================

def is_category_row(name: str, sku: str, price: Optional[float]) -> bool:
    """Пытаемся отфильтровать "заголовки разделов" и пустые строки."""
    if not name: return True
    if not sku or price is None: return True
    letters = [ch for ch in name if ch.isalpha()]
    if letters:
        upp = sum(1 for ch in letters if ch.upper()==ch)
        if upp/len(letters) > 0.95 and not re.search(r"\d", name) and len(name) <= 64:
            return True
    return False

# ===================== ПОИСК КАРТОЧКИ И СКРЕЙП =====================

def is_product_page(soup: BeautifulSoup) -> bool:
    """Признаки карточки: есть описание и/или главный img main_image_*."""
    has_desc = soup.select_one('div[itemprop="description"].jshop_prod_description') is not None
    has_img  = soup.select_one('img[itemprop="image"][id^="main_image_"]') is not None
    return has_desc or has_img

def fetch_product_soup(url: str) -> Optional[BeautifulSoup]:
    html = fetch_html(url)
    if not html: return None
    return BeautifulSoup(html, "html.parser")

def find_product_page_by_article(article: str) -> Optional[str]:
    art = (article or "").strip()
    if not art: return None
    queries = [
        f"{BASE_URL}/search/?searchstring={art}",
        f"{BASE_URL}/search?searchstring={art}",
        f"{BASE_URL}/?searchstring={art}",
    ]
    for url in queries:
        html = fetch_html(url)
        if not html: continue
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select("a[href]"):
            href = a.get("href","")
            if not href or href.startswith("#"): continue
            href = urljoin(BASE_URL, href)
            if re.search(r"/goods/[^/]+\.html", href, flags=re.I) and (art.lower() in href.lower() or art.lower() in a.get_text(" ", strip=True).lower()):
                psoup = fetch_product_soup(href)
                if psoup and is_product_page(psoup):
                    return href
    return None

def scrape_product(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Возвращает (picture_url, vendor, description_full).

    Важно:
    - picture: ТОЛЬКО img[itemprop="image"][id^="main_image_"]/@src (абсолютный URL).
    - vendor:  div[itemprop="brand"] [itemprop="name"]  (фолбэк .manufacturer_name).
    - desc:    p/h3–h5, списки и таблицы из div.jshop_prod_description. Таблицы → "- Ключ: Значение".
    """
    html = fetch_html(url)
    if not html: return None, None, None
    soup = BeautifulSoup(html, "html.parser")

    # Фото (строго main_image_ → @src)
    picture = None
    img = soup.select_one('img[itemprop="image"][id^="main_image_"]')
    if img and img.get("src"):
        picture = urljoin(BASE_URL, img["src"].strip())
        if not re.match(r"^https?://", picture, flags=re.I):
            picture = None  # защита: только абсолютный URL

    # Бренд
    vendor = None
    b = soup.select_one('div[itemprop="brand"] [itemprop="name"]')
    if b:
        vendor = b.get_text(" ", strip=True)
    if not vendor:
        manu = soup.select_one(".manufacturer_name")
        if manu:
            vendor = manu.get_text(" ", strip=True)

    # Описание + ТХ
    desc = None
    block = soup.select_one('div[itemprop="description"].jshop_prod_description') \
         or soup.select_one('div.jshop_prod_description') \
         or soup.select_one('[itemprop="description"]')
    if block:
        parts: List[str] = []
        # абзацы/заголовки/списки
        for ch in block.find_all(["p","h3","h4","h5","ul","ol"], recursive=False):
            tag = ch.name.lower()
            if tag in {"p","h3","h4","h5"}:
                t = re.sub(r"\s+"," ", ch.get_text(" ", strip=True)).strip()
                if t: parts.append(t)
            elif tag in {"ul","ol"}:
                for li in ch.find_all("li", recursive=False):
                    t = re.sub(r"\s+"," ", li.get_text(" ", strip=True)).strip()
                    if t: parts.append(f"- {t}")
        # таблицы → список "- Ключ: Значение"
        specs: List[str] = []
        for tbl in block.find_all("table"):
            for tr in tbl.find_all("tr"):
                cells = tr.find_all(["th","td"])
                if len(cells) >= 2:
                    k = re.sub(r"\s+"," ", cells[0].get_text(" ", strip=True)).strip()
                    v = re.sub(r"\s+"," ", cells[1].get_text(" ", strip=True)).strip()
                    if k and v:
                        specs.append(f"- {k}: {v}")
        if specs and not any("технические характеристики" in _norm(x) for x in parts):
            parts.append("Технические характеристики:")
        parts.extend(specs)

        txt = "\n".join([p for p in parts if p]).strip()
        if txt and not re.match(r"^https?://", txt, flags=re.I):
            desc = txt

    return picture, vendor, desc

# ===================== FEED_META =====================

def render_feed_meta(pairs: Dict[str, str]) -> str:
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
        "rows_after_keys":"После фильтра по словам",
        "offers_written":"Офферов записано в YML",
        "picture_found":"Сколько товаров с фото (main_image_)",
        "vendor_found":"Сколько товаров с брендом",
        "desc_found":"Сколько товаров с описанием/ТХ",
        "built_utc":"Время сборки (UTC)",
        "built_Asia/Almaty":"Время сборки (Алматы)",
    }
    mk = max(len(k) for k in order)
    left = [f"{k.ljust(mk)} = {pairs.get(k,'n/a')}" for k in order]
    ml = max(len(x) for x in left)
    lines = ["FEED_META"]
    for k, l in zip(order, left):
        lines.append(f"{l.ljust(ml)}  | {comments[k]}")
    return "\n".join(lines)

# ===================== MAIN =====================

def main():
    # 1) Загружаем прайс
    log(f"Source: {SUPPLIER_URL}")
    b = fetch_bytes(SUPPLIER_URL)
    if not b: die("Не удалось скачать XLSX.")
    wb = load_workbook(io.BytesIO(b), data_only=True, read_only=True)

    # 2) Находим шапку
    ws, mapping, header_row = select_best_sheet(wb)
    name_c = next(k for k,v in mapping.items() if v=="name")
    sku_c  = next(k for k,v in mapping.items() if v=="sku")
    price_c= next((k for k,v in mapping.items() if v=="price"), None)

    # 3) Ключевые слова
    keys = load_keywords(KEYWORDS_PATH)
    if KEYWORDS_MODE == "include" and not keys:
        die("COPYLINE_KEYWORDS_MODE=include, но список ключей пуст (docs/copyline_keywords.txt).")

    # 4) Проходим строки
    rows_read = rows_after_cat = rows_after_keys = 0
    items: List[Dict[str,Any]] = []

    for r in range(header_row + 2, ws.max_row + 1):
        name  = str(ws.cell(r, name_c).value or "").strip()
        sku   = str(ws.cell(r, sku_c).value or "").strip()
        price = parse_money(ws.cell(r, price_c).value) if price_c else None

        if name or sku or price is not None:
            rows_read += 1

        if is_category_row(name, sku, price):
            continue
        rows_after_cat += 1

        ok = name_matches(name, keys)
        if (KEYWORDS_MODE == "include" and not ok) or (KEYWORDS_MODE == "exclude" and ok):
            continue
        rows_after_keys += 1

        items.append({"name": name, "sku": sku, "price": price})

    # 5) Собираем YML
    root = ET.Element("yml_catalog", date=time.strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(root, "shop")
    offers = ET.SubElement(shop, "offers")

    pic_cnt = ven_cnt = desc_cnt = 0

    for it in items:
        name, sku, price = it["name"], it["sku"], it["price"]

        picture = vendor = desc = None
        try:
            page = find_product_page_by_article(sku)
            if page:
                pic, ven, des = scrape_product(page)
                picture = pic or None
                vendor  = (ven or "").strip() or None
                desc    = (des or "").strip() or None
        except Exception as e:
            warn(f"scrape error for {sku}: {e}")

        offer = ET.SubElement(offers, "offer", {"id": sku})
        ET.SubElement(offer, "name").text = name

        if vendor:
            ET.SubElement(offer, "vendor").text = vendor
            ven_cnt += 1

        ET.SubElement(offer, "vendorCode").text = f"{VENDORCODE_PREFIX}{sku}"

        if picture:
            ET.SubElement(offer, "picture").text = picture
            pic_cnt += 1

        # Если полного описания нет — можно подставить name (если FILL_DESC_FROM_NAME=1)
        if desc:
            ET.SubElement(offer, "description").text = desc
            desc_cnt += 1
        elif FILL_DESC_FROM_NAME:
            ET.SubElement(offer, "description").text = name

        if price is not None:
            ET.SubElement(offer, "price").text = str(int(price))
            ET.SubElement(offer, "currencyId").text = "KZT"

        ET.SubElement(offer, "available").text = "true"

    # 6) Красиво форматируем и пишем
    try:
        ET.indent(root, space="  ")
    except Exception:
        pass

    meta = {
        "script_version": SCRIPT_VERSION,
        "supplier": "copyline",
        "source": SUPPLIER_URL,
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
    root.insert(0, ET.Comment(render_feed_meta(meta)))

    xml = ET.tostring(root, encoding=OUTPUT_ENCODING, xml_declaration=True).decode(OUTPUT_ENCODING, "replace")
    # чтобы не было слитно "--><shop>"
    xml = re.sub(r"(-->)\s*(<shop>)", r"\1\n  \2", xml)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml)

    log(f"Wrote: {OUT_FILE} | offers={len(list(offers.findall('offer')))} | encoding={OUTPUT_ENCODING}")

if __name__ == "__main__":
    main()
