# -*- coding: utf-8 -*-
"""
Сборщик YML для поставщика Copyline (плоский <offers> для Satu)
script_version = copyline-2025-09-19.5

Что делает:
- Загружает XLSX прайс по SUPPLIER_URL.
- Находит рабочую шапку (имя, артикул, цена).
- Удаляет строки-категории и прочий "мусор".
- Фильтрует товары по префиксам из docs/copyline_keywords.txt (режим include|exclude).
- Для каждого артикула ищет карточку на сайте Copyline, берёт:
  * фото: img[itemprop="image"][id^="main_image_"] -> @src
  * бренд: div[itemprop="brand"] span[itemprop="name"] (или .manufacturer_name)
  * описание и ТХ: div[itemprop="description"].jshop_prod_description (p/h3-h5 + таблицы)
- Собирает docs/copyline.yml в кодировке windows-1251.
- FEED_META на русском, без слипшихся символов перед <shop>.
"""

from __future__ import annotations
import os, sys, re, io, time, random, unicodedata
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# -------------------- НАСТРОЙКИ --------------------

SCRIPT_VERSION = "copyline-2025-09-19.5"

SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "copyline")
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://copyline.kz/files/price-CLA.xlsx")

OUT_FILE        = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S   = int(os.getenv("TIMEOUT_S", "30"))
RETRIES     = int(os.getenv("RETRIES", "4"))
BACKOFF_S   = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES   = int(os.getenv("MIN_BYTES", "2000"))

KEYWORDS_PATH     = os.getenv("COPYLINE_KEYWORDS_PATH", "docs/copyline_keywords.txt")
KEYWORDS_MODE     = os.getenv("COPYLINE_KEYWORDS_MODE", "include").lower()  # include|exclude
PREFIX_TRIM_NOISE = os.getenv("COPYLINE_PREFIX_ALLOW_TRIM", "1").lower() in {"1", "true", "yes"}

FILL_DESC_FROM_NAME = os.getenv("FILL_DESC_FROM_NAME", "1").lower() in {"1","true","yes"}

# vendorCode = CL + <артикул из XLSX>
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "CL")

# -------------------- УТИЛИТЫ --------------------

def log(msg: str): print(msg, flush=True)
def warn(msg: str): print(f"WARN: {msg}", file=sys.stderr, flush=True)
def err(msg: str): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(1)

def now_utc(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty():
    try:
        if ZoneInfo:
            return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        pass
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _nfkc(s: str): return unicodedata.normalize("NFKC", s or "")

def _norm(s: str) -> str:
    s = _nfkc(s).replace("\u00A0", " ").replace("ё", "е").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def parse_money(raw) -> Optional[float]:
    if raw is None: return None
    s = str(raw).strip()
    if not s: return None
    s = (s.replace("\xa0", " ")
           .replace(" ", "")
           .replace("KZT", "").replace("kzt", "")
           .replace("₸", "").replace(",", "."))
    try:
        v = float(s)
        return v if v > 0 else None
    except Exception:
        return None

def is_url(s: Optional[str]) -> bool:
    return bool(s) and bool(re.match(r"^https?://", s.strip(), flags=re.I))

# -------------------- СЕТЬ --------------------

def fetch_bytes(url: str, timeout: int = TIMEOUT_S) -> bytes:
    sess = requests.Session()
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = sess.get(url, timeout=timeout, headers={"User-Agent": "supplier-feed-bot/1.0"})
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            if len(r.content) < MIN_BYTES and url.endswith(".xlsx"):
                raise RuntimeError(f"too small ({len(r.content)} bytes)")
            return r.content
        except Exception as e:
            last = e
            if attempt < RETRIES:
                time.sleep(BACKOFF_S * attempt * (1 + random.uniform(-0.2, 0.2)))
    raise RuntimeError(f"fetch failed: {last}")

def fetch_html(url: str) -> Optional[str]:
    try:
        return fetch_bytes(url, timeout=TIMEOUT_S).decode("utf-8", errors="replace")
    except Exception as e:
        warn(f"html fetch fail: {url} | {e}")
        return None

# -------------------- ШАПКА (склейка 2 строк) --------------------

def merge_two_rows(r1: List[str], r2: List[str]) -> List[str]:
    out = []
    ln = max(len(r1), len(r2))
    for i in range(ln):
        a = (r1[i] if i < len(r1) else "") or ""
        b = (r2[i] if i < len(r2) else "") or ""
        a = a.strip()
        b = b.strip()
        out.append(f"{a}.{b}" if a and b else (b or a))
    return out

def map_headers(vals: List[str]) -> Dict[int, str]:
    mapping = {}
    for idx, raw in enumerate(vals, start=1):
        v = _norm(raw)
        if not v:
            continue
        if ("наимен" in v) or v == "номенклатура": mapping[idx] = "name"
        if "артикул" in v:                         mapping[idx] = "sku"
        if "цена"    in v:                         mapping[idx] = "price"
    return mapping

def find_header(ws: Worksheet, scan_rows: int = 80, max_cols: int = 40) -> Tuple[Dict[int, str], int]:
    best_map: Dict[int, str] = {}
    best_row = -1
    best_score = -1
    for r in range(1, scan_rows):
        vals1 = [str(ws.cell(r, c).value or "").strip() for c in range(1, max_cols + 1)]
        vals2 = [str(ws.cell(r + 1, c).value or "").strip() for c in range(1, max_cols + 1)]
        merged = merge_two_rows(vals1, vals2)
        for vals in (vals1, merged):
            mapping = map_headers(vals)
            score = len([f for f in mapping.values() if f in {"name", "sku", "price"}])
            if ("name" in mapping.values()) and ("sku" in mapping.values()):
                if score > best_score:
                    best_map, best_row, best_score = mapping, r, score
    return best_map, best_row

def select_best_sheet(wb) -> Tuple[Worksheet, Dict[int, str], int]:
    best = (None, {}, -1, -1)
    for ws in wb.worksheets:
        mapping, row = find_header(ws)
        score = len([f for f in mapping.values() if f in {"name", "sku", "price"}])
        if score > best[3]:
            best = (ws, mapping, row, score)
    ws, mapping, row, _ = best
    if not ws or not mapping or row < 1:
        err("Не удалось найти шапку.")
    return ws, mapping, row

# -------------------- ФИЛЬТР ПО ПРЕФИКСАМ --------------------

def load_keywords(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    data = None
    for enc in ("utf-8-sig", "utf-8", "utf-16", "utf-16-le", "utf-16-be", "windows-1251"):
        try:
            with open(path, "r", encoding=enc) as f:
                data = f.read()
            data = data.replace("\ufeff", "").replace("\x00", "")
            break
        except Exception:
            continue
    if data is None:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read().replace("\x00", "")
    keys = []
    for line in data.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        keys.append(_norm(s))
    return keys

def name_passes_prefix(name: str, keys: List[str]) -> bool:
    if not keys:
        return True
    nm = _norm(name)
    if PREFIX_TRIM_NOISE:
        nm = re.sub(r'^[\s\-\–\—•·|:/\\\[\]\(\)«»"“”„\']+', "", nm)
    return any(nm.startswith(k) for k in keys)

# -------------------- ОТСЕВ КАТЕГОРИЙ --------------------

def is_category_row(name: str, sku: str, price: Optional[float]) -> bool:
    """Возвращает True, если строка похожа на категорию/подзаголовок, а не на товар."""
    if not name:
        return True
    if not sku or price is None:
        return True
    s = _nfkc(name).strip()
    if s.lower() == "товары":
        return True
    letters = [ch for ch in s if ch.isalpha()]
    if letters:
        upp = sum(1 for ch in letters if ch.upper() == ch)
        if upp / len(letters) > 0.95 and not re.search(r"\d", s) and len(s) <= 64:
            return True
    return False

# -------------------- СКРЕЙПИНГ ТОВАРА --------------------

def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = _nfkc(s).replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _desc_from_block(block: BeautifulSoup) -> List[str]:
    """Собираем p/h3-h5/ul-li в многострочный текст. Таблицы оставляем для отдельного парсинга."""
    parts: List[str] = []
    for child in block.find_all(recursive=False):
        tag = child.name.lower() if child.name else ""
        if tag == "p":
            t = _clean_text(child.get_text(" ", strip=True))
            if t:
                parts.append(t)
        elif tag in {"h3", "h4", "h5"}:
            t = _clean_text(child.get_text(" ", strip=True))
            if t:
                parts.append(t)
        elif tag in {"ul", "ol"}:
            for li in child.find_all("li", recursive=False):
                t = _clean_text(li.get_text(" ", strip=True))
                if t:
                    parts.append(f"- {t}")
        elif tag == "table":
            # таблицу обработаем другой функцией
            continue
    return parts

def _specs_from_block(block: BeautifulSoup) -> List[str]:
    """Из всех таблиц блока берём пары 'Ключ: Значение'."""
    lines: List[str] = []
    for tbl in block.find_all("table"):
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) >= 2:
                k = _clean_text(cells[0].get_text(" ", strip=True))
                v = _clean_text(cells[1].get_text(" ", strip=True))
                if k and v:
                    lines.append(f"- {k}: {v}")
    return lines

def find_product_page_by_article(article: str) -> Optional[str]:
    art = (article or "").strip()
    if not art:
        return None
    candidates = [
        f"https://copyline.kz/search/?searchstring={art}",
        f"https://copyline.kz/search?searchstring={art}",
        f"https://copyline.kz/?searchstring={art}",
        f"https://copyline.kz/?q={art}",
    ]
    for url in candidates:
        html = fetch_html(url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select("a"):
            href = a.get("href", "")
            text = (a.get_text(" ", strip=True) or "")
            if not href or href.startswith("#"):
                continue
            if "copyline.kz" not in href:
                if href.startswith("/"):
                    href = "https://copyline.kz" + href
                else:
                    continue
            if re.search(r"/goods/|/catalog/|/product|/shop/", href) or art.lower() in href.lower() or art.lower() in text.lower():
                return href
    return None

def scrape_product_details(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Возвращает: (picture_url, vendor, description_full)
    picture: img[itemprop="image"][id^="main_image_"] -> @src
    vendor:  div[itemprop="brand"] span[itemprop="name"] или .manufacturer_name
    desc:    из div[itemprop="description"].jshop_prod_description (p/h3-h5 + таблицы)
    """
    html = fetch_html(url)
    if not html:
        return None, None, None
    soup = BeautifulSoup(html, "html.parser")

    # 1) Фото
    picture = None
    img = soup.select_one('img[itemprop="image"][id^="main_image_"]')
    if img and img.get("src"):
        picture = img["src"].strip()
        if picture.startswith("/"):
            picture = "https://copyline.kz" + picture
    if picture and not is_url(picture):
        picture = None

    # 2) Бренд
    vendor = None
    brand_name = soup.select_one('div[itemprop="brand"] [itemprop="name"]')
    if brand_name:
        vtxt = _clean_text(brand_name.get_text(" ", strip=True))
        if vtxt:
            vendor = vtxt
    if not vendor:
        manu = soup.select_one("div.manufacturer_name")
        if manu:
            vtxt = _clean_text(manu.get_text(" ", strip=True))
            if vtxt:
                vendor = vtxt

    # 3) Описание + ТХ
    desc_full = None
    block = (soup.select_one('div[itemprop="description"].jshop_prod_description')
             or soup.select_one('div.jshop_prod_description')
             or soup.select_one('[itemprop="description"]'))
    if block:
        text_lines = _desc_from_block(block)
        spec_lines = _specs_from_block(block)
        if spec_lines and not any("технические характеристики" in _norm(x) for x in text_lines):
            text_lines.append("Технические характеристики:")
        if spec_lines:
            text_lines.extend(spec_lines)
        desc_full = "\n".join([l for l in text_lines if l]).strip()
        if desc_full and is_url(desc_full):
            desc_full = None

    return picture, vendor, desc_full

# -------------------- FEED_META --------------------

def render_feed_meta(pairs: Dict[str, str]) -> str:
    order = [
        "script_version", "supplier", "source",
        "rows_read", "rows_after_cat_filter", "rows_after_keyword_filter",
        "offers_written", "picture_found", "vendor_found", "desc_filled_from_site",
        "built_utc", "built_Asia/Almaty",
    ]
    comments = {
        "script_version": "Версия скрипта",
        "supplier": "Метка поставщика",
        "source": "URL исходного XLSX",
        "rows_read": "Строк считано (после шапки)",
        "rows_after_cat_filter": "После удаления категорий/шапок",
        "rows_after_keyword_filter": "После фильтра по префиксам",
        "offers_written": "Офферов записано в YML",
        "picture_found": "Сколько товаров получили фото (img[itemprop=image])",
        "vendor_found": "Сколько товаров получили бренд",
        "desc_filled_from_site": "Сколько товаров получили описание/ТХ (jshop_prod_description)",
        "built_utc": "Время сборки (UTC)",
        "built_Asia/Almaty": "Время сборки (Алматы)",
    }
    max_key = max(len(k) for k in order)
    lefts = [f"{k.ljust(max_key)} = {pairs.get(k, 'n/a')}" for k in order]
    max_left = max(len(x) for x in lefts)
    lines = ["FEED_META"]
    for left, k in zip(lefts, order):
        lines.append(f"{left.ljust(max_left)}  | {comments[k]}")
    return "\n".join(lines)

# -------------------- MAIN --------------------

def main():
    log(f"Source: {SUPPLIER_URL}")
    xbytes = fetch_bytes(SUPPLIER_URL)
    wb = load_workbook(io.BytesIO(xbytes), data_only=True, read_only=True)

    ws, mapping, header_row = select_best_sheet(wb)
    log(f"Sheet: {ws.title} | header_row={header_row} | cols={len(mapping)}")

    keys = load_keywords(KEYWORDS_PATH)
    if KEYWORDS_MODE == "include" and len(keys) == 0:
        err("COPYLINE_KEYWORDS_MODE=include, но ключей не найдено. Проверь docs/copyline_keywords.txt.")

    rows_read = 0
    rows_after_cat = 0
    rows_after_keys = 0
    records: List[Dict[str, str]] = []

    name_col = next(k for k, v in mapping.items() if v == "name")
    sku_col  = next(k for k, v in mapping.items() if v == "sku")
    price_col = next((k for k, v in mapping.items() if v == "price"), None)

    for r in range(header_row + 2, ws.max_row + 1):
        name = str(ws.cell(r, name_col).value or "").strip()
        sku  = str(ws.cell(r, sku_col).value or "").strip()
        price_raw = ws.cell(r, price_col).value if price_col else None
        price = parse_money(price_raw)

        if name or sku or price is not None:
            rows_read += 1

        if is_category_row(name, sku, price):
            continue
        rows_after_cat += 1

        if KEYWORDS_MODE == "include" and not name_passes_prefix(name, keys):
            continue
        if KEYWORDS_MODE == "exclude" and name_passes_prefix(name, keys):
            continue
        rows_after_keys += 1

        records.append({"name": name, "sku": sku, "price": price})

    root = ET.Element("yml_catalog")
    root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(root, "shop")
    offers = ET.SubElement(shop, "offers")

    picture_found = 0
    vendor_found = 0
    desc_from_site = 0

    for rec in records:
        name = rec["name"]; sku = rec["sku"]; price = rec["price"]

        picture = vendor = desc = None
        try:
            page = find_product_page_by_article(sku)
            if page:
                pic, ven, des = scrape_product_details(page)
                picture = pic if pic and is_url(pic) else None
                vendor  = (ven or "").strip() or None
                desc    = (des or "").strip() or None
                if desc and is_url(desc):  # защита
                    desc = None
        except Exception as e:
            warn(f"scrape error for {sku}: {e}")

        offer = ET.SubElement(offers, "offer", {"id": sku})
        ET.SubElement(offer, "name").text = name

        if vendor:
            ET.SubElement(offer, "vendor").text = vendor
            vendor_found += 1

        ET.SubElement(offer, "vendorCode").text = f"{VENDORCODE_PREFIX}{sku}"

        if picture:
            ET.SubElement(offer, "picture").text = picture
            picture_found += 1

        if desc:
            ET.SubElement(offer, "description").text = desc
            desc_from_site += 1
        elif FILL_DESC_FROM_NAME:
            ET.SubElement(offer, "description").text = name

        if price is not None:
            ET.SubElement(offer, "price").text = str(int(price))
            ET.SubElement(offer, "currencyId").text = "KZT"

        ET.SubElement(offer, "available").text = "true"

    try:
        ET.indent(root, space="  ")
    except Exception:
        pass

    meta = {
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "rows_read": rows_read,
        "rows_after_cat_filter": rows_after_cat,
        "rows_after_keyword_filter": rows_after_keys,
        "offers_written": len(list(offers.findall("offer"))),
        "picture_found": picture_found,
        "vendor_found": vendor_found,
        "desc_filled_from_site": desc_from_site,
        "built_utc": now_utc(),
        "built_Asia/Almaty": now_almaty(),
    }
    # Комментарий над <shop>, без склейки "--><shop>"
    root.insert(0, ET.Comment(render_feed_meta(meta)))
    xml_bytes = ET.tostring(root, encoding=OUTPUT_ENCODING, xml_declaration=True)
    xml_text  = xml_bytes.decode(OUTPUT_ENCODING, errors="replace")
    xml_text  = re.sub(r"(-->)\s*(<shop>)", lambda m: f"{m.group(1)}\n  {m.group(2)}", xml_text)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml_text)

    log(f"Wrote: {OUT_FILE} | offers={len(list(offers.findall('offer')))} | encoding={OUTPUT_ENCODING}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
