# -*- coding: utf-8 -*-
"""
Satu YML для Copyline (fast + cache + BFS fallback):
- Фильтр: НАЗВАНИЕ ДОЛЖНО НАЧИНАТЬСЯ одной из фраз из списка (строго, без склонений).
- Матч по артикулу с карточками сайта; фото ОБЯЗАТЕЛЬНО.
- Описание: ПОЛНОЕ с карточки; категории: по хлебным крошкам (структура как на сайте).
- Turbo: таргет-краул только нужных SKU, параллельно; кэш индекса (JSON); лимит времени.
"""

from __future__ import annotations
import os, re, io, time, html, json, hashlib, xml.etree.ElementTree as ET
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

HTTP_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "20"))
MIN_BYTES          = int(os.getenv("MIN_BYTES", "900"))
MAX_WORKERS        = int(os.getenv("MAX_WORKERS", "6"))
MAX_CRAWL_MINUTES  = int(os.getenv("MAX_CRAWL_MINUTES", "25"))
MAX_SITEMAP_URLS   = int(os.getenv("MAX_SITEMAP_URLS", "20000"))
MAX_VISIT_PAGES    = int(os.getenv("MAX_VISIT_PAGES", "5000"))
SITE_INDEX_CACHE   = os.getenv("SITE_INDEX_CACHE", "docs/copyline_site_index.json")

SUPPLIER_NAME      = "Copyline"
CURRENCY           = "KZT"

ROOT_CAT_ID        = 9300000
ROOT_CAT_NAME      = "Copyline"

UA = {"User-Agent": "Mozilla/5.0 (compatible; Copyline-XLSX-Site/1.5)"}

# ---------- utils ----------
def http_get(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        b = r.content
        if len(b) < MIN_BYTES:
            return None
        return b
    except Exception:
        return None

def soup_of(b: bytes) -> BeautifulSoup: return BeautifulSoup(b, "html.parser")
def yml_escape(s: str) -> str: return html.escape(s or "")

def sanitize_title(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:200].rstrip()

def to_number(x: Any) -> Optional[float]:
    if x is None: return None
    s = str(x).replace("\xa0", " ").strip().replace(" ", "").replace(",", ".")
    if not re.search(r"\d", s): return None
    try:
        return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        if m:
            try: return float(m.group(0))
            except: return None
        return None

def key_norm(v: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", v or "").upper()

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

# ---------- keywords (STRICT startswith) ----------
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
    # '^\\s*картридж(?!\\w)' — "картридж ..." (но не "картриджа ...", не "тонер картридж ...")
    pats: List[re.Pattern] = []
    for kw in kws:
        esc = re.escape(kw).replace(r"\ ", " ")
        pats.append(re.compile(rf"^\s*{esc}(?!\w)", flags=re.IGNORECASE))
    return pats

def title_startswith_strict(title: str, patterns: List[re.Pattern]) -> bool:
    if not title: return False
    return any(p.search(title) for p in patterns)

# ---------- XLSX (2-row header) ----------
def fetch_xlsx_bytes(url: str) -> bytes:
    r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.content

def detect_header_two_row(rows: List[List[Any]], scan_rows: int = 60) -> Tuple[int,int,Dict[str,int]]:
    def low(x): return str(x or "").strip().lower()
    for i in range(min(scan_rows, len(rows)-1)):
        row0 = [low(c) for c in rows[i]]
        row1 = [low(c) for c in rows[i+1]]
        if any("номенклатура" in c for c in row0):
            name_col = next((j for j,c in enumerate(row0) if "номенклатура" in c), None)
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

# ---------- site parsing: product, picture, description, breadcrumbs ----------
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
            t = a.get_text(strip=True)
            if not t: continue
            tl = t.lower()
            if tl in ("главная","home"):
                continue
            names.append(t.strip())
        if names: break
    return [n for n in names if n]

def parse_product_page(url: str) -> Optional[Tuple[str, str, str, List[str]]]:
    b = http_get(url)
    if not b: return None
    s = soup_of(b)

    # SKU: расширенный поиск (itemprop, "Артикул", "Код товара", "SKU")
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

    # picture (main_image / og:image / fallback)
    src = None
    imgel = s.find("img", id=re.compile(r"^main_image_", re.I))
    if imgel and (imgel.get("src") or imgel.get("data-src")):
        src = imgel.get("src") or imgel.get("data-src")
    if not src:
        ogi = s.find("meta", attrs={"property": "og:image"})
        if ogi and ogi.get("content"):
            src = ogi["content"].strip()
    if not src:
        # мягкий фоллбэк: первое большое изображение из каталожных путей
        for img in s.find_all("img"):
            src_try = img.get("src") or img.get("data-src") or ""
            if any(k in src_try for k in ["img_products", "/products/", "/img/"]):
                src = src_try; break
    if not src: return None
    pic = normalize_img_to_full(urljoin(url, src))

    desc = extract_full_description(s) or ""
    crumbs = extract_breadcrumbs(s)

    return sku, pic, desc, crumbs

def parse_sitemap_xml(xml_bytes: bytes) -> List[str]:
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []
    locs = []
    for el in root.iter():
        t = (el.tag or "").lower()
        if t.endswith("loc") and el.text:
            locs.append(el.text.strip())
    return locs

def fetch_sitemap_product_urls() -> List[str]:
    candidates = [
        f"{BASE_URL}/sitemap.xml",
        f"{BASE_URL}/sitemap_index.xml",
        f"{BASE_URL}/sitemap-index.xml",
        f"{BASE_URL}/sitemap-products.xml",
        f"{BASE_URL}/sitemap1.xml",
        f"{BASE_URL}/sitemap2.xml",
        f"{BASE_URL}/sitemap3.xml",
    ]
    urls: List[str] = []
    seen: Set[str] = set()
    for u in candidates:
        b = http_get(u)
        if not b: continue
        for loc in parse_sitemap_xml(b):
            if loc.lower().endswith(".xml"):
                bx = http_get(loc)
                if bx:
                    for loc2 in parse_sitemap_xml(bx):
                        if loc2 not in seen:
                            seen.add(loc2); urls.append(loc2)
            else:
                if loc not in seen:
                    seen.add(loc); urls.append(loc)
    prods = [u for u in urls if PRODUCT_RE.search(u)]
    prods = list(dict.fromkeys(prods))
    if len(prods) > MAX_SITEMAP_URLS:
        prods = prods[:MAX_SITEMAP_URLS]
    return prods

def bfs_collect_product_urls(limit_pages: int) -> List[str]:
    """Обход каталога, чтобы собрать /goods/*.html (если sitemap пустой)."""
    seeds = [
        f"{BASE_URL}/",
        f"{BASE_URL}/goods.html",
        f"{BASE_URL}/goods/toner-cartridges-brother.html",
    ]
    queue: List[str] = list(dict.fromkeys(seeds))
    visited: Set[str] = set()
    found: List[str] = []

    while queue and len(visited) < limit_pages:
        page = queue.pop(0)
        if page in visited: continue
        visited.add(page)

        b = http_get(page)
        if not b: continue
        s = soup_of(b)

        # rel=next
        ln = s.find("link", attrs={"rel": "next"})
        if ln and ln.get("href"):
            queue.append(urljoin(page, ln["href"]))

        for a in s.find_all("a", href=True):
            href = a["href"].strip()
            absu = urljoin(page, href)
            if "copyline.kz" not in absu: 
                continue
            if PRODUCT_RE.search(absu) and not absu.endswith("/goods.html"):
                found.append(absu)
            if ("/goods/" in href or "page=" in href or "PAGEN_" in href or "/page/" in href or "limit=" in href):
                if absu not in visited and absu not in queue:
                    queue.append(absu)

    return list(dict.fromkeys(found))

# ---------- cache ----------
def load_cache(path: str) -> Dict[str, Dict[str, Any]]:
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(path: str, data: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# ---------- categories from breadcrumbs ----------
def stable_cat_id(text: str, prefix: int = 9400000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories_from_paths(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
    cat_map: Dict[Tuple[str,...], int] = {}
    out_list: List[Tuple[int,str,Optional[int]]] = []
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная", "home", "каталог")]
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
    start_time = datetime.utcnow()
    deadline   = start_time + timedelta(minutes=MAX_CRAWL_MINUTES)

    # 1) XLSX
    xlsx_bytes = fetch_xlsx_bytes(XLSX_URL)
    wb = load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    sheet = max(wb.sheetnames, key=lambda n: wb[n].max_row * max(1, wb[n].max_column))
    ws = wb[sheet]
    rows = [ [c for c in r] for r in ws.iter_rows(values_only=True) ]

    row0, row1, idx = detect_header_two_row(rows)
    if row0 < 0:
        print("[error] Header not detected.")
        return 2
    data_start = row1 + 1
    name_col, vendor_col, price_col = idx["name"], idx["vendor_code"], idx["price"]

    # 2) строгий startswith
    kw_list = load_keywords(KEYWORDS_FILE)
    start_patterns = compile_startswith_patterns(kw_list)

    # 3) кандидаты из XLSX + ключи матчей
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
        print("[error] No items after startswith filter.")
        return 2
    print(f"[xls] candidates (startswith): {len(xlsx_items)}, distinct match-keys: {len(want_keys)}")

    # 4) кэш индекса
    cache = load_cache(SITE_INDEX_CACHE)
    cache_keys = set(cache.keys())
    missing_keys = [k for k in want_keys if k not in cache_keys]
    print(f"[cache] hit={len(cache_keys & want_keys)}, miss={len(missing_keys)}")

    # 5) таргет-краул (sitemap -> BFS fallback), параллельно, с тайм-лимитом
    def time_left() -> bool: return datetime.utcnow() < deadline

    if missing_keys and time_left():
        urls = fetch_sitemap_product_urls()
        if not urls:
            print("[crawl] sitemap empty → BFS fallback…")
            urls = bfs_collect_product_urls(MAX_VISIT_PAGES)
        else:
            print(f"[crawl] sitemap urls: {len(urls)}")

        if not urls:
            print("[crawl] no urls to crawl.")
        else:
            def worker(u: str):
                if not time_left(): return None
                res = parse_product_page(u)
                if not res: return None
                sku, pic, desc, crumbs = res
                raw = sku.strip()
                keys = { key_norm(raw), key_norm(raw.replace("-", "")) }
                if re.match(r"^[Cc]\d+$", raw): keys.add(key_norm(raw[1:]))
                if re.match(r"^\d+$",  raw):    keys.add(key_norm("C"+raw))
                # индексируем только то, что нам нужно (экономия времени/кэша)
                targ_keys = [k for k in keys if k in missing_keys]
                if not targ_keys: 
                    return None
                payload = {"url": u, "pic": pic, "desc": desc, "crumbs": crumbs}
                return targ_keys, payload

            found = 0
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = { ex.submit(worker, u): u for u in urls[:MAX_SITEMAP_URLS] }
                for fut in as_completed(futures):
                    if not time_left(): break
                    out = fut.result()
                    if not out: continue
                    keys, payload = out
                    for k in keys:
                        cache[k] = payload
                        found
