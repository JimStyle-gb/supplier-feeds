# -*- coding: utf-8 -*-
"""
Copyline -> Satu YML (flat <offers>)
script_version = copyline-2025-09-21.4a

Правка: добавлено авто-распознавание кодировки для docs/copyline_keywords.txt.
(использует charset-normalizer при наличии; затем chardet; затем ручной перебор)
"""

from __future__ import annotations
import os, re, io, time, random, unicodedata, html, hashlib
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# --- optional enc-detectors ---
try:
    from charset_normalizer import from_bytes as cn_from_bytes  # type: ignore
except Exception:  # pragma: no cover
    cn_from_bytes = None
try:
    import chardet  # type: ignore
except Exception:  # pragma: no cover
    chardet = None  # type: ignore

# ===================== SETTINGS =====================

SCRIPT_VERSION = "copyline-2025-09-21.4a"

BASE_URL     = "https://copyline.kz"
SUPPLIER_URL = os.getenv("SUPPLIER_URL", f"{BASE_URL}/files/price-CLA.xlsx")

OUT_FILE        = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S   = int(os.getenv("TIMEOUT_S", "25"))
RETRIES     = int(os.getenv("RETRIES", "4"))
RETRY_BACK  = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES   = int(os.getenv("MIN_BYTES", "900"))

KEYWORDS_PATH   = os.getenv("COPYLINE_KEYWORDS_PATH", "docs/copyline_keywords.txt")
KEYWORDS_MODE   = os.getenv("COPYLINE_KEYWORDS_MODE", "include").lower()   # include|exclude
KEYWORDS_MATCH  = os.getenv("COPYLINE_MATCH_MODE", "startswith").lower()   # startswith|contains
PREFIX_TRIM     = os.getenv("COPYLINE_PREFIX_ALLOW_TRIM", "1").lower() in {"1","true","yes"}

FILL_DESC_FROM_NAME = os.getenv("FILL_DESC_FROM_NAME", "1").lower() in {"1","true","yes"}
VENDORCODE_PREFIX   = os.getenv("VENDORCODE_PREFIX", "CL")

UA = {"User-Agent": "Mozilla/5.0 (compatible; CopylineFeed/2025.09-fast)"}

# ===================== UTILS =====================

def log(s): print(s, flush=True)
def warn(s): print("WARN: " + s, flush=True)
def die(s):  print("ERROR: " + s, flush=True); raise SystemExit(1)

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

def jitter_sleep(ms: int): time.sleep(max(0.0, ms/1000.0) * (1 + random.uniform(-0.2, 0.2)))

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
        time.sleep(delay)
        delay *= 1.6
    warn(f"fetch failed: {url} | {last}")
    return None

def fetch_html(url: str) -> Optional[str]:
    b = fetch_bytes(url)
    return b.decode("utf-8","replace") if b else None

# ===================== XLSX HEADERS =====================

def merge_two_rows(r1: List[str], r2: List[str]) -> List[str]:
    out = []
    n = max(len(r1), len(r2))
    for i in range(n):
        a = (r1[i] if i < len(r1) else "") or ""
        b = (r2[i] if i < len(r2) else "") or ""
        out.append((a.strip() + "." + b.strip()) if a.strip() and b.strip() else (b.strip() or a.strip()))
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

# ===================== KEYWORDS (auto-encoding) =====================

def load_keywords(path: str) -> List[str]:
    """
    Читает keywords с распознаванием кодировки.
    Порядок:
      1) charset-normalizer (если установлен)
      2) chardet (если установлен)
      3) ручной перебор кодировок
    """
    if not os.path.exists(path):
        return []

    # читаем "сырые" байты
    with open(path, "rb") as f:
        raw = f.read()

    text: Optional[str] = None

    # 1) charset-normalizer
    if cn_from_bytes is not None:
        try:
            res = cn_from_bytes(raw)
            best = res.best()
            if best and best.encoding:
                text = best.output().strip("\ufeff").replace("\x00", "")
        except Exception:
            text = None

    # 2) chardet (если ещё не распознали)
    if text is None and 'chardet' in globals() and chardet:
        try:
            det = chardet.detect(raw)  # type: ignore
            enc = det.get("encoding")
            if enc:
                text = raw.decode(enc, "replace").replace("\ufeff", "").replace("\x00", "")
        except Exception:
            text = None

    # 3) ручной перебор
    if text is None:
        for enc in ("utf-8-sig", "utf-8", "utf-16", "utf-16-le", "utf-16-be", "windows-1251", "cp866"):
            try:
                text = raw.decode(enc, "replace").replace("\ufeff", "").replace("\x00", "")
                break
            except Exception:
                continue

    if text is None:
        # на крайний случай
        text = raw.decode("utf-8", "ignore").replace("\x00", "")

    keys: List[str] = []
    for ln in text.splitlines():
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
    return any(k in t for k in keys)

# ===================== PRODUCT PAGE =====================

def is_product_page(soup: BeautifulSoup) -> bool:
    has_desc = soup.select_one('div[itemprop="description"].jshop_prod_description') is not None
    has_img  = soup.select_one('img[itemprop="image"][id^="main_image_"]') is not None
    return has_desc or has_img

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
                page_html = fetch_html(href)
                if not page_html: continue
                psoup = BeautifulSoup(page_html, "html.parser")
                if is_product_page(psoup):
                    return href
    return None

def scrape_product(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    html = fetch_html(url)
    if not html: return None, None, None
    s = BeautifulSoup(html, "html.parser")

    picture = None
    img = s.select_one('img[itemprop="image"][id^="main_image_"]')
    if img:
        src = (img.get("src") or img.get("data-src") or "").strip()
        if src:
            picture = urljoin(BASE_URL, src)
            if not re.match(r"^https?://", picture, flags=re.I):
                picture = None

    vendor = None
    b = s.select_one('div[itemprop="brand"] [itemprop="name"]')
    if b:
        vendor = b.get_text(" ", strip=True)
    if not vendor:
        manu = s.select_one(".manufacturer_name")
        if manu:
            vendor = manu.get_text(" ", strip=True)

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
                    if t: parts.append("- " + t)
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

# ===================== FEED META =====================

def render_feed_meta(pairs: Dict[str, str]) -> str:
    order = [
        "script_version","supplier","source",
        "offers_written","picture_found","vendor_found","desc_found",
        "built_utc","built_Asia/Almaty",
    ]
    comments = {
        "script_version":"Версия скрипта",
        "supplier":"Метка поставщика",
        "source":"URL исходного XLSX",
        "offers_written":"Офферов записано в YML",
        "picture_found":"Сколько товаров с фото",
        "vendor_found":"Сколько товаров с брендом",
        "desc_found":"Сколько товаров с описанием",
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

# ===================== YML BUILD (offer availability layout) =====================

def build_yml(offers: List[Dict[str,Any]]) -> str:
    root = ET.Element("yml_catalog", date=time.strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(root, "shop")
    offers_el = ET.SubElement(shop, "offers")

    pic_cnt = ven_cnt = desc_cnt = 0

    for it in offers:
        offer = ET.SubElement(offers_el, "offer", {"id": it["id"]})  # no available/in_stock attrs
        ET.SubElement(offer, "name").text = it["name"]

        if it.get("vendor"):
            ET.SubElement(offer, "vendor").text = it["vendor"]
            ven_cnt += 1

        ET.SubElement(offer, "vendorCode").text = it["vendorCode"]

        pic_el = ET.SubElement(offer, "picture")
        if it.get("picture"):
            pic_el.text = it["picture"]
            pic_cnt += 1

        desc_el = ET.SubElement(offer, "description")
        if it.get("description"):
            desc_el.text = it["description"]
            desc_cnt += 1
        elif FILL_DESC_FROM_NAME:
            desc_el.text = it["name"]

        if it.get("price") is not None:
            ET.SubElement(offer, "price").text = str(int(it["price"]))
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
        "offers_written": str(len(offers)),
        "picture_found": str(pic_cnt),
        "vendor_found": str(ven_cnt),
        "desc_found": str(desc_cnt),
        "built_utc": now_utc(),
        "built_Asia/Almaty": now_almaty(),
    }
    root.insert(0, ET.Comment(render_feed_meta(meta)))

    xml = ET.tostring(root, encoding=OUTPUT_ENCODING, xml_declaration=True).decode(OUTPUT_ENCODING, "replace")
    xml = re.sub(r"(-->)\s*(<shop>)", r"\1\n  \2", xml)
    return xml

# ===================== MAIN (заглушка под ваш сбор данных) =====================

def main():
    # Здесь должен быть ваш текущий сбор товаров (из XLSX и карточек).
    collected: List[Dict[str,Any]] = []  # заполняется вашей логикой

    # Пишем YML
    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    xml = build_yml(collected)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml)
    log(f"Wrote: {OUT_FILE} | offers={len(collected)} | encoding={OUTPUT_ENCODING}")

if __name__ == "__main__":
    main()
