# -*- coding: utf-8 -*-
"""
Copyline → Satu YML (startswith + targeted category crawl, FIXED):
- XLSX: НАЗВАНИЕ ДОЛЖНО НАЧИНАТЬСЯ одним из ключей (строго, без склонений/вариантов).
- Краул: только релевантные КАТЕГОРИИ (по тексту ссылок, ключ как ОТДЕЛЬНОЕ слово), пагинация; карточки в пуле.
- Фото обязательно. Описание — ПОЛНОЕ с карточки. Категории — по хлебным крошкам.
- Без префиксов в артикулах и offer_id.
"""

from __future__ import annotations
import os, re, io, time, html, hashlib
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

# ---------- ENV ----------
BASE_URL           = "https://copyline.kz"
XLSX_URL           = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
KEYWORDS_FILE      = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE           = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING    = os.getenv("OUTPUT_ENCODING", "windows-1251")

HTTP_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "120"))
MIN_BYTES          = int(os.getenv("MIN_BYTES", "900"))

MAX_CRAWL_MINUTES  = int(os.getenv("MAX_CRAWL_MINUTES", "60"))
MAX_CATEGORY_PAGES = int(os.getenv("MAX_CATEGORY_PAGES", "1200"))
MAX_WORKERS        = int(os.getenv("MAX_WORKERS", "6"))

SUPPLIER_NAME      = "Copyline"
CURRENCY           = "KZT"

ROOT_CAT_ID        = 9300000
ROOT_CAT_NAME      = "Copyline"

UA = {"User-Agent": "Mozilla/5.0 (compatible; Copyline-XLSX-Site/1.7.1-targeted-fixed)"}

# ---------- utils ----------
def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms/1000.0))

def http_get(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return None
        b = r.content
        if len(b) < MIN_BYTES:   return None
        return b
    except Exception:
        return None

def soup_of(b: bytes) -> BeautifulSoup: return BeautifulSoup(b, "html.parser")
def yml_escape(s: str) -> str: return html.escape(s or "")
def sha1(s: str) -> str: return hashlib.sha1(s.encode("utf-8")).hexdigest()
def key_norm(v: str) -> str: return re.sub(r"[^A-Za-z0-9]+", "", v or "").upper()

def sanitize_title(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:200].rstrip()

def to_number(x: Any) -> Optional[float]:
    if x is None: return None
    s = str(x).replace("\xa0"," ").strip().replace(" ","").replace(",",".")
    if not re.search(r"\d", s): return None
    try: return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        return float(m.group(0)) if m else None

# ---------- keywords ----------
def load_keywords(path: str) -> List[str]:
    kws: List[str] = []
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    kws.append(s)
    if not kws:
        kws = ["drum","девелопер","драм","кабель сетевой","картридж","термоблок","термоэлемент","тонер-картридж"]
    return kws

def compile_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    # ^\s*kw(?!\w) — начинается строго словом kw
    return [re.compile(rf"^\s*{re.escape(kw).replace(r'\ ', ' ')}(?!\w)", re.I) for kw in kws]

def compile_word_patterns(kws: List[str]) -> List[re.Pattern]:
    # (?<!\w)kw(?!\w) — kw как отдельное слово где угодно
    return [re.compile(rf"(?<!\w){re.escape(kw).replace(r'\ ', ' ')}(?!\w)", re.I) for kw in kws]

def title_startswith_strict(title: str, patterns: List[re.Pattern]) -> bool:
    if not title: return False
    return any(p.search(title) for p in patterns)

# ---------- XLSX (2-row header) ----------
def fetch_xlsx_bytes(url: str) -> bytes:
    r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.content

def detect_header_two_row(rows: List[List[Any]], scan_rows: int = 60):
    def low(x): return str(x or "").strip().lower()
    for i in range(min(scan_rows, len(rows)-1)):
        row0 = [low(c) for c in rows[i]]
        row1 = [low(c) for c in rows[i+1]]
        if any("номенклатура" in c for c in row0):
            name_col   = next((j for j,c in enumerate(row0) if "номенклатура" in c), None)
            vendor_col = next((j for j,c in enumerate(row1) if "артикул" in c), None)
            price_col  = next((j for j,c in enumerate(row1) if "цена" in c or "опт" in c), None)
            if name_col is not None and vendor_col is not None and price_col is not None:
                return i, i+1, {"name": name_col, "vendor_code": vendor_col, "price": price_col}
    return -1, -1, {}

def extract_sku_from_name(name: str) -> Optional[str]:
    t = name.upper()
    tokens = re.findall(r"[A-ZА-Я0-9]{2,}(?:[-/–][A-ZА-Я0-9]{2,})?", t)
    for tok in tokens:
        if re.search(r"[A-ZА-Я]", tok) and re.search(r"\d", tok):
            return tok.replace("–","-").replace("/","-")
    return None

# ---------- product page ----------
PRODUCT_RE = re.compile(r"/goods/[^/]+\.html$")

def normalize_img_to_full(url: Optional[str]) -> Optional[str]:
    if not url: return None
    u = url.strip()
    if u.startswith("//"): u = "https:" + u
    if u.startswith("/"):  u = BASE_URL + u
    m = re.match(r"^(https?://[^/]+)(/.*/)([^/]+)$", u)
    if not m: return u
    host, path, fname = m.groups()
    if fname.startswith("full_"): return u
    if fname.startswith("thumb_"): fname = "full_" + fname[len("thumb_"):]
    else: fname = "full_" + fname
    return f"{host}{path}{fname}"

def extract_full_description(s: BeautifulSoup) -> Optional[str]:
    selectors = [
        '[itemprop="description"]',
        '.jshop_prod_description', '.product_description', '.prod_description',
        '.productfull', '#description', '.tab-content .description', '.tabs .description',
    ]
    for sel in selectors:
        el = s.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)
    candidates = s.select('.product, .productpage, .product-info, #content, .content')
    for c in candidates:
        txt = c.get_text(" ", strip=True)
        if txt and len(txt) > 60:
            return txt
    return None

def extract_breadcrumbs(s: BeautifulSoup) -> List[str]:
    names: List[str] = []
    containers = s.select('ul.breadcrumb, .breadcrumbs, .breadcrumb, .pathway, [class*="breadcrumb"], [class*="pathway"]')
    for bc in containers:
        links = bc.find_all("a")
        for a in links:
            t = a.get_text(" ", strip=True)
            if not t: continue
            tl = t.lower()
            if tl in ("главная","home"): continue
            names.append(t.strip())
        if names: break
    return [n for n in names if n]

def parse_product_page(url: str) -> Optional[Tuple[str, str, str, List[str]]]:
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(url)
    if not b: return None
    s = soup_of(b)

    sku = None
    skuel = s.find(attrs={"itemprop": "sku"})
    if skuel:
        val = (skuel.get_text(" ", strip=True) or "").strip()
        if val: sku = val
    if not sku:
        labels = ["артикул", "sku", "код товара", "код"]
        for lab in labels:
            node = s.find(string=lambda t: t and lab in t.lower())
            if node:
                val = (node.parent.get_text(" ", strip=True) if node.parent else str(node)).strip()
                m = re.search(r"([A-Za-z0-9\-\._/]{2,})", val)
                if m:
                    sku = m.group(1); break
    if not sku:
        txt = s.get_text(" ", strip=True)
        m = re.search(r"(?:Артикул|SKU|Код товара|Код)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})", txt, flags=re.I)
        if m: sku = m.group(1)
    if not sku: return None

    src = None
    imgel = s.find("img", id=re.compile(r"^main_image_", re.I))
    if imgel and (imgel.get("src") or imgel.get("data-src")):
        src = imgel.get("src") or imgel.get("data-src")
    if not src:
        ogi = s.find("meta", attrs={"property": "og:image"})
        if ogi and ogi.get("content"):
            src = ogi["content"].strip()
    if not src:
        for img in s.find_all("img"):
            src_try = img.get("src") or img.get("data-src") or ""
            if any(k in src_try for k in ["img_products", "/products/", "/img/"]):
                src = src_try; break
    if not src: return None
    pic = normalize_img_to_full(urljoin(url, src))

    desc = extract_full_description(s) or ""
    crumbs = extract_breadcrumbs(s)
    return sku, pic, desc, crumbs

# ---------- targeted categories (FIXED) ----------
def anchor_text_matches(text: str, word_patterns: List[re.Pattern]) -> bool:
    """Ключ как отдельное слово в тексте ссылки на категорию (без манипуляций с шаблонами)."""
    if not text: return False
    t = text.strip().lower()
    return any(p.search(t) for p in word_patterns)

def discover_relevant_category_urls() -> List[str]:
    """Смотрим главную/каталог, берём ссылки на категории, где текст содержит ключ как отдельное слово."""
    seeds = [f"{BASE_URL}/", f"{BASE_URL}/goods.html"]
    pages = []
    for u in seeds:
        b = http_get(u)
        if b: pages.append((u, soup_of(b)))
    if not pages:
        return []

    kws = load_keywords(KEYWORDS_FILE)
    word_patterns = compile_word_patterns(kws)  # отдельные слова, а не 'startswith'

    urls: List[str] = []
    seen: Set[str] = set()
    for base, s in pages:
        for a in s.find_all("a", href=True):
            txt = a.get_text(" ", strip=True)
            if not txt: continue
            if anchor_text_matches(txt, word_patterns):
                absu = urljoin(base, a["href"])
                if "copyline.kz" in absu and "/goods/" in absu and absu not in seen:
                    seen.add(absu); urls.append(absu)

    # на всякий случай добавим известную ветку
    urls.append(f"{BASE_URL}/goods/toner-cartridges-brother.html")
    return list(dict.fromkeys(urls))

def category_next_url(s: BeautifulSoup, page_url: str) -> Optional[str]:
    ln = s.find("link", attrs={"rel": "next"})
    if ln and ln.get("href"): return urljoin(page_url, ln["href"])
    a = s.find("a", class_=lambda c: c and "next" in c.lower())
    if a and a.get("href"): return urljoin(page_url, a["href"])
    for a in s.find_all("a", href=True):
        txt = (a.get_text(" ", strip=True) or "").lower()
        if txt in ("следующая", "вперед", "вперёд", "next", ">"):
            return urljoin(page_url, a["href"])
    return None

def collect_product_urls_from_category(cat_url: str, limit_pages: int) -> List[str]:
    urls: List[str] = []
    seen_pages: Set[str] = set()
    page = cat_url
    pages_done = 0
    while page and pages_done < limit_pages:
        if page in seen_pages: break
        seen_pages.add(page)
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(page)
        if not b: break
        s = soup_of(b)
        for a in s.find_all("a", href=True):
            absu = urljoin(page, a["href"])
            if PRODUCT_RE.search(absu):
                urls.append(absu)
        page = category_next_url(s, page)
        pages_done += 1
    return list(dict.fromkeys(urls))

# ---------- categories from breadcrumbs ----------
def stable_cat_id(text: str, prefix: int = 9400000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories_from_paths(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
    cat_map: Dict[Tuple[str,...], int] = {}
    out_list: List[Tuple[int,str,Optional[int]]] = []
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная","home","каталог")]
        if not clean: continue
        parent_id = ROOT_CAT_ID
        prefix: List[str] = []
        for name in clean:
            prefix.append(name)
            key = tuple(prefix)
            if key in cat_map:
                parent_id = cat_map[key]; continue
            cid = stable_cat_id(" / ".join(prefix))
            cat_map[key] = cid
            out_list.append((cid, name, parent_id))
            parent_id = cid
    return out_list, cat_map

# ---------- YML ----------
def build_yml(categories: List[Tuple[int,str,Optional[int]]], offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SUPPLIER_NAME.lower())}</name>")
    out.append('<currencies><currency id="KZT" rate="1" /></currencies>')

    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        parent = parent if parent else ROOT_CAT_ID
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{yml_escape(name)}</category>")
    out.append("</categories>")

    out.append("<offers>")
    for cid, it in offers:
        price = it["price"]
        price_txt = str(int(price)) if float(price).is_integer() else f"{price}"
        out += [
            f"<offer id=\"{yml_escape(it['offer_id'])}\" available=\"true\" in_stock=\"true\">",
            f"<name>{yml_escape(it['title'])}</name>",
            f"<vendor>{yml_escape(it.get('brand') or SUPPLIER_NAME)}</vendor>",
            f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>",
            f"<price>{price_txt}</price>",
            "<currencyId>KZT</currencyId>",
            f"<categoryId>{cid}</categoryId>",
        ]
        if it.get("url"): out.append(f"<url>{yml_escape(it['url'])}</url>")
        if it.get("picture"): out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        desc = it.get("description") or it["title"]
        out.append(f"<description>{yml_escape(desc)}</description>")
        out += ["<quantity_in_stock>1</quantity_in_stock>", "<stock_quantity>1</stock_quantity>", "<quantity>1</quantity>", "</offer>"]
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------- MAIN ----------
def main() -> int:
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MINUTES)

    # 1) XLSX
    xlsx_bytes = fetch_xlsx_bytes(XLSX_URL)
    wb = load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    sheet = max(wb.sheetnames, key=lambda n: wb[n].max_row * max(1, wb[n].max_column))
    ws = wb[sheet]
    rows = [[c for c in r] for r in ws.iter_rows(values_only=True)]

    row0, row1, idx = detect_header_two_row(rows)
    if row0 < 0:
        print("[error] Не удалось распознать шапку.")
        return 2
    data_start = row1 + 1
    name_col, vendor_col, price_col = idx["name"], idx["vendor_code"], idx["price"]

    # 2) startswith-ключи
    kw_list = load_keywords(KEYWORDS_FILE)
    start_patterns = compile_startswith_patterns(kw_list)

    xlsx_items: List[Dict[str,Any]] = []
    want_keys: Set[str] = set()

    for r in rows[data_start:]:
        name_raw = r[name_col]
        if not name_raw: continue
        title = sanitize_title(str(name_raw).strip())
        if not title_startswith_strict(title, start_patterns):
            continue

        price = to_number(r[price_col])
        if price is None or price <= 0: continue

        v_raw = r[vendor_col]
        vcode = (str(v_raw).strip() if v_raw is not None else "") or extract_sku_from_name(title) or ""
        if not vcode: continue

        variants = { vcode, vcode.replace("-", "") }
        if re.match(r"^[Cc]\d+$", vcode): variants.add(vcode[1:])
        if re.match(r"^\d+$", vcode):     variants.add("C"+vcode)
        for v in variants:
            want_keys.add(key_norm(v))

        xlsx_items.append({
            "title": title,
            "price": float(f"{price:.2f}"),
            "vendorCode_raw": vcode,
        })

    if not xlsx_items:
        print("[error] После фильтра по startswith/цене нет позиций.")
        return 2
    print(f"[xls] candidates (startswith): {len(xlsx_items)}, distinct keys: {len(want_keys)}")

    # 3) только релевантные категории
    cats = discover_relevant_category_urls()
    if not cats:
        print("[error] Не нашли релевантных категорий.")
        return 2
    print(f"[cats] relevant categories: {len(cats)}")

    # 4) URL карточек по категориям
    product_urls: List[str] = []
    pages_budget = max(1, MAX_CATEGORY_PAGES // max(1, len(cats)))
    for cu in cats:
        urls = collect_product_urls_from_category(cu, pages_budget)
        product_urls.extend(urls)
    product_urls = list(dict.fromkeys(product_urls))
    print(f"[crawl] product urls from categories: {len(product_urls)}")

    # 5) карточки в потоках
    def worker(u: str):
        if datetime.utcnow() > deadline: return None
        try:
            parsed = parse_product_page(u)
            if not parsed: return None
            sku, pic, desc, crumbs = parsed
            raw = sku.strip()
            keys = { key_norm(raw), key_norm(raw.replace("-", "")) }
            if re.match(r"^[Cc]\d+$", raw): keys.add(key_norm(raw[1:]))
            if re.match(r"^\d+$",  raw):    keys.add(key_norm("C"+raw))
            return keys, {"url": u, "pic": pic, "desc": desc, "crumbs": crumbs}
        except Exception:
            return None

    site_index: Dict[str, Dict[str, Any]] = {}
    matched_keys: Set[str] = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = { ex.submit(worker, u): u for u in product_urls }
        for fut in as_completed(futures):
            if datetime.utcnow() > deadline:
                break
            out = fut.result()
            if not out: continue
            keys, payload = out
            useful = [k for k in keys if k in want_keys and k not in matched_keys]
            if not useful: 
                continue
            for k in useful:
                site_index[k] = payload
                matched_keys.add(k)
            if len(matched_keys) % 50 == 0:
                print(f"[match] keys matched: {len(matched_keys)} / {len(want_keys)}")
            if matched_keys >= want_keys:
                print("[match] all wanted keys found.")
                break

    print(f"[index] matched keys total: {len(matched_keys)}")

    # 6) категории из крошек
    all_paths = [rec.get("crumbs") for rec in site_index.values() if rec.get("crumbs")]
    cat_list, path_id_map = build_categories_from_paths(all_paths)
    print(f"[cats] built: {len(cat_list)}")

    # 7) мёрдж (с фото)
    offers: List[Tuple[int,Dict[str,Any]]] = []
    seen_offer_ids: Set[str] = set()

    for it in xlsx_items:
        raw_v = it["vendorCode_raw"]
        candidates = { raw_v, raw_v.replace("-", "") }
        if re.match(r"^[Cc]\d+$", raw_v): candidates.add(raw_v[1:])
        if re.match(r"^\d+$", raw_v):     candidates.add("C"+raw_v)

        found = None
        for v in candidates:
            kn = key_norm(v)
            if kn in site_index:
                found = site_index[kn]; break
        if not found or not found.get("pic"):
            continue

        url, pic = found["url"], found["pic"]
        desc = found.get("desc") or it["title"]
        crumbs = found.get("crumbs") or []

        cid = ROOT_CAT_ID
        if crumbs:
            clean = [p.strip() for p in crumbs if p and p.strip()]
            clean = [p for p in clean if p.lower() not in ("главная","home","каталог")]
            key = tuple(clean)
            while key and key not in path_id_map:
                key = key[:-1]
            if key and key in path_id_map:
                cid = path_id_map[key]

        offer_id = raw_v
        if offer_id in seen_offer_ids:
            offer_id = f"{raw_v}-{sha1(it['title'])[:6]}"
        seen_offer_ids.add(offer_id)

        offers.append((cid, {
            "offer_id":   offer_id,
            "title":      it["title"],
            "price":      it["price"],
            "vendorCode": raw_v,
            "brand":      SUPPLIER_NAME,
            "url":        url,
            "picture":    pic,
            "description": desc,
        }))

    if not offers:
        print("[error] Ни одной позиции не сопоставили с фото (после startswith).")
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(build_yml([], []))
        return 2

    # 8) YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(cat_list, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, categories: {len(cat_list)} -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    import sys, xml.etree.ElementTree as ET  # ET нужен для типизации в некоторых средах
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        sys.exit(2)
