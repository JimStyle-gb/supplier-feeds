# -*- coding: utf-8 -*-
"""
Copyline → Satu YML (flat <offers>)
script_version = copyline-2025-09-21.3

Важно: НЕ меняем принцип твоего кода:
- Берём товары из XLSX.
- Фильтруем по словам из docs/copyline_keywords.txt.
- Для каждого артикула ищем СТРАНИЦУ ТОВАРА через поиск на сайте и парсим карточку.
- <picture> строго из <img itemprop="image" id="main_image_*" src="...">.
- <description> — полный текст из div.jshop_prod_description (+ таблицы как "- Ключ: Значение").
- <vendor> — бренд из div[itemprop="brand"] span[itemprop="name"] (fallback .manufacturer_name).
- Теги <picture> и <description> создаются ВСЕГДА (даже если пустые).
- Выход: docs/copyline.yml (windows-1251) с FEED_META на русском.
"""

from __future__ import annotations
import os, re, io, time, random, unicodedata
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# ===================== НАСТРОЙКИ =====================

SCRIPT_VERSION = "copyline-2025-09-21.3"

BASE_URL     = "https://copyline.kz"
SUPPLIER_URL = os.getenv("SUPPLIER_URL", f"{BASE_URL}/files/price-CLA.xlsx")

OUT_FILE        = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S   = int(os.getenv("TIMEOUT_S", "25"))
RETRIES     = int(os.getenv("RETRIES", "4"))
RETRY_BACK  = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES   = int(os.getenv("MIN_BYTES", "900"))

# Ключевые слова для фильтра названий
KEYWORDS_PATH   = os.getenv("COPYLINE_KEYWORDS_PATH", "docs/copyline_keywords.txt")
KEYWORDS_MODE   = os.getenv("COPYLINE_KEYWORDS_MODE", "include").lower()   # include|exclude
KEYWORDS_MATCH  = os.getenv("COPYLINE_MATCH_MODE", "startswith").lower()   # startswith|contains
PREFIX_TRIM     = os.getenv("COPYLINE_PREFIX_ALLOW_TRIM", "1").lower() in {"1","true","yes"}

# Параллельность (ускоряем, но не агрессивно)
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", "8"))
REQ_DELAY_MS    = int(os.getenv("REQUEST_DELAY_MS", "80"))  # лёгкая пауза между запросами

# Поведение описаний
FILL_DESC_FROM_NAME = os.getenv("FILL_DESC_FROM_NAME", "1").lower() in {"1","true","yes"}

# Префикс для vendorCode
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "CL")

UA = {"User-Agent": "Mozilla/5.0 (compatible; CopylineFeed/2025.09-fast)"}

# ===================== УТИЛИТЫ =====================

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

def sleep_jitter(ms: int):
    time.sleep(max(0.0, ms/1000.0) * (1 + random.uniform(-0.2, 0.2)))

# ===================== HTTP =====================

def fetch_bytes(url: str) -> Optional[bytes]:
    last = None
    delay = RETRY_BACK
    for _ in range(RETRIES):
        try:
            r = requests.get(url, headers=UA, timeout=TIMEOUT_S)
            if r.status_code == 200 and (len(r.content) >= MIN_BYTES if url.endswith(".xlsx") else True):
                return r.content
            last = f"http {r.status_code} size={len(r.content)}"
        except Exception as e:
            last = repr(e)
        sleep_jitter(REQ_DELAY_MS)
        time.sleep(delay)
        delay *= 1.7
    warn(f"fetch failed: {url} | {last}")
    return None

def fetch_html(url: str) -> Optional[str]:
    b = fetch_bytes(url)
    return b.decode("utf-8","replace") if b else None

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

# ===================== КЛЮЧЕВЫЕ СЛОВА (ФИЛЬТР) =====================

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

def name_matches(title: str, keys: List[str]) -> bool:
    if not keys: return True
    t = _norm(title)
    if PREFIX_TRIM:
        t = re.sub(r'^[\s\-\–\—•·|:/\\\[\]\(\)«»"“”„\']+', "", t)
    if KEYWORDS_MATCH == "startswith":
        return any(t.startswith(k) for k in keys)
    return any(k in t for k in keys)  # contains

# ===================== ПОИСК КАРТОЧКИ ПО АРТИКУЛУ =====================

def is_product_page(soup: BeautifulSoup) -> bool:
    has_desc = soup.select_one('div[itemprop="description"].jshop_prod_description') is not None
    has_img  = soup.select_one('img[itemprop="image"][id^="main_image_"]') is not None
    return has_desc or has_img

def find_product_page_by_article(article: str) -> Optional[str]:
    """Ищем карточку только через встроенный поиск, как в твоём коде."""
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
        s = BeautifulSoup(html, "html.parser")
        # берём ссылку вида /goods/....html, где в href или тексте присутствует артикул
        for a in s.select("a[href]"):
            href = a.get("href","")
            if not href or href.startswith("#"): continue
            absu = urljoin(BASE_URL, href)
            if not re.search(r"/goods/[^/]+\.html", absu, flags=re.I):
                continue
            txt = a.get_text(" ", strip=True).lower()
            if art.lower() in absu.lower() or art.lower() in txt:
                # валидация, что это карточка
                page_html = fetch_html(absu)
                if not page_html: continue
                psoup = BeautifulSoup(page_html, "html.parser")
                if is_product_page(psoup):
                    return absu
        # микропаузa между запросами
        sleep_jitter(REQ_DELAY_MS)
    return None

def scrape_product(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Возвращает (picture_url, vendor, description_full).

    - picture: ТОЛЬКО из <img itemprop="image" id="main_image_*" src="..."> (или data-src).
    - vendor:  div[itemprop="brand"] [itemprop="name"]  (фолбэк .manufacturer_name).
    - desc:    все p/h3–h5/ul/ol + таблицы из div.jshop_prod_description (таблицы → "- Ключ: Значение").
    """
    html = fetch_html(url)
    if not html: return None, None, None
    s = BeautifulSoup(html, "html.parser")

    # фото
    picture = None
    img = s.select_one('img[itemprop="image"][id^="main_image_"]')
    if img:
        src = (img.get("src") or img.get("data-src") or "").strip()
        if src:
            picture = urljoin(BASE_URL, src)
            if not re.match(r"^https?://", picture, flags=re.I):
                picture = None

    # бренд
    vendor = None
    b = s.select_one('div[itemprop="brand"] [itemprop="name"]')
    if b:
        vendor = b.get_text(" ", strip=True)
    if not vendor:
        manu = s.select_one(".manufacturer_name")
        if manu:
            vendor = manu.get_text(" ", strip=True)

    # описание + ТХ
    desc = None
    block = s.select_one('div[itemprop="description"].jshop_prod_description') \
         or s.select_one('div.jshop_prod_description') \
         or s.select_one('[itemprop="description"]')
    if block:
        parts: List[str] = []
        for ch in block.find_all(["p","h3","h4","h5","ul","ol"], recursive=False):
            tag = ch.name.lower()
            if tag in {"p","h3","h4","h5"}:
                t = re.sub(r"\s+"," ", ch.get_text(" ", strip=True)).strip()
                if t: parts.append(t)
            elif tag in {"ul","ol"}:
                for li in ch.find_all("li", recursive=False):
                    t = re.sub(r"\s+"," ", li.get_text(" ", strip=True)).strip()
                    if t: parts.append(f"- {t}")
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
        "rows_read","rows_after_filter",
        "offers_written","picture_found","vendor_found","desc_found",
        "built_utc","built_Asia/Almaty",
    ]
    comments = {
        "script_version":"Версия скрипта",
        "supplier":"Метка поставщика",
        "source":"URL исходного XLSX",
        "rows_read":"Строк считано (после шапки)",
        "rows_after_filter":"После фильтра по словам",
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
    # 1) XLSX
    log(f"Source: {SUPPLIER_URL}")
    b = fetch_bytes(SUPPLIER_URL)
    if not b: die("Не удалось скачать XLSX.")
    wb = load_workbook(io.BytesIO(b), data_only=True, read_only=True)

    # 2) Шапка
    ws, mapping, header_row = select_best_sheet(wb)
    name_c = next(k for k,v in mapping.items() if v=="name")
    sku_c  = next(k for k,v in mapping.items() if v=="sku")
    price_c= next((k for k,v in mapping.items() if v=="price"), None)

    # 3) Ключи
    keys = load_keywords(KEYWORDS_PATH)
    if KEYWORDS_MODE == "include" and not keys:
        die("COPYLINE_KEYWORDS_MODE=include, но список ключей пуст (docs/copyline_keywords.txt).")

    # 4) Читаем строки + фильтр
    rows_read = rows_after = 0
    items: List[Dict[str,Any]] = []

    for r in range(header_row + 2, ws.max_row + 1):
        name  = str(ws.cell(r, name_c).value or "").strip()
        sku   = str(ws.cell(r, sku_c).value or "").strip()
        price = parse_money(ws.cell(r, price_c).value) if price_c else None

        if name or sku or price is not None:
            rows_read += 1
        if not name or not sku or price is None or price <= 0:
            continue

        if (KEYWORDS_MODE == "include" and not name_matches(name, keys)) \
           or (KEYWORDS_MODE == "exclude" and name_matches(name, keys)):
            continue

        rows_after += 1
        items.append({"name": name, "sku": sku, "price": price})

    if not items:
        die("После фильтра по словам не осталось строк.")

    # 5) Параллельно ищем карточки и парсим
    results: Dict[str, Dict[str, Optional[str]]] = {}

    def worker(it):
        sku = it["sku"]
        try:
            url = find_product_page_by_article(sku)
            picture = vendor = desc = None
            if url:
                picture, vendor, desc = scrape_product(url)
            return sku, {"url": url, "picture": picture, "vendor": vendor, "desc": desc}
        except Exception as e:
            warn(f"{sku}: {e}")
            return sku, {"url": None, "picture": None, "vendor": None, "desc": None}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(worker, it) for it in items]
        for f in as_completed(futs):
            sku, payload = f.result()
            results[sku] = payload

    # 6) Собираем YML
    root = ET.Element("yml_catalog", date=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(root, "shop")
    offers = ET.SubElement(shop, "offers")

    pic_cnt = ven_cnt = desc_cnt = 0

    for it in items:
        name, sku, price = it["name"], it["sku"], it["price"]
        payload = results.get(sku, {}) if results else {}

        picture = payload.get("picture")
        vendor  = payload.get("vendor")
        desc    = payload.get("desc")

        offer = ET.SubElement(offers, "offer", {"id": sku})
        ET.SubElement(offer, "name").text = name

        if vendor:
            ET.SubElement(offer, "vendor").text = vendor
            ven_cnt += 1

        ET.SubElement(offer, "vendorCode").text = f"{VENDORCODE_PREFIX}{sku}"

        # picture — ВСЕГДА создаём тег
        pic_el = ET.SubElement(offer, "picture")
        if picture:
            pic_el.text = picture
            pic_cnt += 1

        # description — ВСЕГДА создаём тег
        desc_el = ET.SubElement(offer, "description")
        if desc:
            desc_el.text = desc
            desc_cnt += 1
        elif FILL_DESC_FROM_NAME:
            desc_el.text = name  # минимум — название

        ET.SubElement(offer, "price").text = str(int(price))
        ET.SubElement(offer, "currencyId").text = "KZT"
        ET.SubElement(offer, "available").text = "true"

    try:
        ET.indent(root, space="  ")
    except Exception:
        pass

    meta = {
        "script_version": SCRIPT_VERSION,
        "supplier": "copyline",
        "source": SUPPLIER_URL,
        "rows_read": str(rows_read),
        "rows_after_filter": str(rows_after),
        "offers_written": str(len(list(offers.findall("offer")))),
        "picture_found": str(pic_cnt),
        "vendor_found": str(ven_cnt),
        "desc_found": str(desc_cnt),
        "built_utc": now_utc(),
        "built_Asia/Almaty": now_almaty(),
    }
    root.insert(0, ET.Comment(render_feed_meta(meta)))

    xml = ET.tostring(root, encoding=OUTPUT_ENCODING, xml_declaration=True).decode(OUTPUT_ENCODING, "replace")
    # аккуратно, чтобы не было "--><shop>"
    xml = re.sub(r"(-->)\s*(<shop>)", r"\1\n  \2", xml)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml)

    log(f"Wrote: {OUT_FILE} | offers={len(list(offers.findall('offer')))} | encoding={OUTPUT_ENCODING}")

if __name__ == "__main__":
    main()
