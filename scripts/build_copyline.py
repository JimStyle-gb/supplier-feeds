# -*- coding: utf-8 -*-
"""
Copyline → Satu YML (flat <offers>)
script_version = copyline-2025-09-21.3

Важно: НЕ меняем принцип твоего кода:# -*- coding: utf-8 -*-
"""
Copyline → Satu YML (строго по началу названия + широкий охват категорий)
(БАЗОВЫЙ РАБОЧИЙ ВАРИАНТ С ПРАВКОЙ <offer> И <available>)

Изменение по просьбе:
- Внутри YML тега <offers> теперь создаём офферы так:
    <offer id="...">  # БЕЗ available="true" и без in_stock="true"
      ...
      <available>true</available>
    </offer>
- Удалены лишние теги количества: <quantity_in_stock>, <stock_quantity>, <quantity>.

Остальная логика сбора ДАННЫХ не менялась.
"""

from __future__ import annotations
import os, re, io, time, html, hashlib
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

# ---------- ENV ----------
BASE_URL           = "https://copyline.kz"
XLSX_URL           = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
KEYWORDS_FILE      = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE           = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC                = (os.getenv("OUTPUT_ENCODING", "windows-1251") or "").lower()
FILE_ENCODING      = "cp1251" if "1251" in ENC else ENC
XML_ENCODING       = "windows-1251" if "1251" in ENC else ENC

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

UA = {"User-Agent": "Mozilla/5.0 (compatible; Copyline-XLSX-Site/1.9-ideal)"}

# ---------- утилиты ----------
def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms/1000.0))

def http_get(url: str, tries: int = 3) -> Optional[bytes]:
    delay = max(0.05, REQUEST_DELAY_MS / 1000.0)
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
            if r.status_code == 200 and len(r.content) >= MIN_BYTES:
                return r.content
            last = f"http {r.status_code} size={len(r.content)}"
        except Exception as e:
            last = repr(e)
        time.sleep(delay)
        delay *= 1.7
    return None

def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "html.parser")

def yml_escape(s: str) -> str:
    return html.escape(s or "")

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def key_norm(v: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", v or "").upper()

def sanitize_title(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:200].rstrip()

def to_number(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).replace("\xa0", " ").strip().replace(" ", "").replace(",", ".")
    if not re.search(r"\d", s):
        return None
    try:
        return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        return float(m.group(0)) if m else None

# ---------- ключевые слова ----------
def load_keywords(path: str) -> List[str]:
    kws: List[str] = []
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    kws.append(s)
    if not kws:
        # резерв по умолчанию
        kws = ["drum", "девелопер", "драм", "картридж", "термоблок", "термоэлемент", "тонер-картридж"]
    return kws

def compile_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    pats: List[re.Pattern] = []
    for kw in kws:
        esc = re.escape(kw).replace(r"\ ", " ")
        pats.append(re.compile(r"^\s*" + esc + r"(?!\w)", re.I))
    return pats

def title_startswith_strict(title: str, patterns: List[re.Pattern]) -> bool:
    if not title:
        return False
    return any(p.search(title) for p in patterns)

# ---------- XLSX (двухстрочная шапка) ----------
def fetch_xlsx_bytes(url: str) -> bytes:
    b = http_get(url, tries=3)
    if not b:
        raise RuntimeError("Не удалось скачать XLSX.")
    return b

def detect_header_two_row(rows: List[List[Any]], scan_rows: int = 60):
    def low(x): return str(x or "").strip().lower()
    for i in range(min(scan_rows, len(rows) - 1)):
        row0 = [low(c) for c in rows[i]]
        row1 = [low(c) for c in rows[i + 1]]
        if any("номенклатура" in c for c in row0):
            name_col = next((j for j, c in enumerate(row0) if "номенклатура" in c), None)
            vendor_col = next((j for j, c in enumerate(row1) if "артикул" in c), None)
            price_col = next((j for j, c in enumerate(row1) if "цена" in c or "опт" in c), None)
            if name_col is not None and vendor_col is not None and price_col is not None:
                return i, i + 1, {"name": name_col, "vendor_code": vendor_col, "price": price_col}
    return -1, -1, {}

def extract_sku_from_name(name: str) -> Optional[str]:
    t = name.upper()
    tokens = re.findall(r"[A-ZА-Я0-9]{2,}(?:[-/–][A-ZА-Я0-9]{2,})?", t)
    for tok in tokens:
        if re.search(r"[A-ZА-Я]", tok) and re.search(r"\d", tok):
            return tok.replace("–", "-").replace("/", "-")
    return None

# ---------- карточка ----------
PRODUCT_RE = re.compile(r"/goods/[^/]+\.html$")

def normalize_img_to_full(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if u.startswith("//"): u = "https:" + u
    if u.startswith("/"):  u = BASE_URL + u
    m = re.match(r"^(https?://[^/]+)(/.*/)([^/]+)$", u)
    if not m:
        return u
    host, path, fname = m.groups()
    if fname.startswith("full_"):
        return u
    if fname.startswith("thumb_"):
        fname = "full_" + fname[len("thumb_"):]
    else:
        fname = "full_" + fname
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
    for c in s.select('.product, .productpage, .product-info, #content, .content'):
        txt = c.get_text(" ", strip=True)
        if txt and len(txt) > 60:
            return txt
    return None

def extract_breadcrumbs(s: BeautifulSoup) -> List[str]:
    names: List[str] = []
    for bc in s.select('ul.breadcrumb, .breadcrumbs, .breadcrumb, .pathway, [class*="breadcrumb"], [class*="pathway"]'):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if not t: continue
            tl = t.lower()
            if tl in ("главная", "home"): continue
            names.append(t.strip())
        if names: break
    return [n for n in names if n]

def parse_product_page(url: str) -> Optional[Tuple[str, str, str, List[str]]]:
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(url)
    if not b: return None
    s = soup_of(b)

    # SKU
    sku = None
    skuel = s.find(attrs={"itemprop": "sku"})
    if skuel:
        val = (skuel.get_text(" ", strip=True) or "").strip()
        if val: sku = val
    if not sku:
        for lab in ["артикул", "sku", "код товара", "код"]:
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

    # picture
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

# ---------- категории ----------
CATEGORY_HINTS = [
    "драм", "drum", "картридж", "тонер", "тонер-картридж", "девелопер",
    "фьюзер", "узел закрепления", "термоблок", "термоэлемент",
]

def text_contains_hint(t: str) -> bool:
    tl = t.lower()
    return any(h in tl for h in CATEGORY_HINTS)

def anchor_text_matches_for_category(text: str, user_keywords: List[str]) -> bool:
    if not text: return False
    t = text.strip().lower()
    for kw in user_keywords:
        esc = re.escape(kw).replace(r"\ ", " ")
        rx = re.compile(r"(?<!\w)" + esc + r"(?:[a-zа-я\-]{0,6})?(?!\w)", re.IGNORECASE)
        if rx.search(t):
            return True
    if text_contains_hint(t):
        return True
    return False

def discover_relevant_category_urls() -> List[str]:
    seeds = [f"{BASE_URL}/", f"{BASE_URL}/goods.html"]
    pages = []
    for u in seeds:
        b = http_get(u)
        if b: pages.append((u, soup_of(b)))
    if not pages:
        return []
    kws = load_keywords(KEYWORDS_FILE)
    urls: List[str] = []
    seen: Set[str] = set()
    for base, s in pages:
        for a in s.find_all("a", href=True):
            txt = a.get_text(" ", strip=True) or ""
            href = a["href"]
            absu = urljoin(base, href)
            if "copyline.kz" not in absu:
                continue
            if "/goods/" not in absu and not absu.endswith("/goods.html"):
                continue
            ok = False
            if anchor_text_matches_for_category(txt, kws):
                ok = True
            else:
                slug = absu.lower()
                if any(h in slug for h in ["drum", "developer", "fuser", "toner", "cartridge",
                                           "драм", "девелопер", "фьюзер", "термоблок", "термоэлемент"]):
                    ok = True
            if ok and absu not in seen:
                seen.add(absu); urls.append(absu)
    urls.extend([
        f"{BASE_URL}/goods/toner-cartridges-brother.html",
        f"{BASE_URL}/goods/drum-units.html",
        f"{BASE_URL}/goods/developer.html",
        f"{BASE_URL}/goods/fuser.html",
    ])
    return list(dict.fromkeys(urls))

def category_next_url(s: BeautifulSoup, page_url: str) -> Optional[str]:
    ln = s.find("link", attrs={"rel": "next"})
    if ln and ln.get("href"):
        return urljoin(page_url, ln["href"])
    a = s.find("a", class_=lambda c: c and "next" in c.lower())
    if a and a.get("href"):
        return urljoin(page_url, a["href"])
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

# ---------- дерево категорий по крошкам ----------
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

# ---------- YML (ИЗМЕНЕНО) ----------
def build_yml(categories: List[Tuple[int,str,Optional[int]]], offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    out.append(f"<?xml version='1.0' encoding='{XML_ENCODING}'?>")
    out.append(f"<yml_catalog date='{ts}'><shop>")
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
            # ВАЖНО: тут убрали available="true" и in_stock="true"
            f"<offer id=\"{yml_escape(it['offer_id'])}\">",
            f"<name>{yml_escape(it['title'])}</name>",
            f"<vendor>{yml_escape(it.get('brand') or SUPPLIER_NAME)}</vendor>",
            f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>",
            f"<price>{price_txt}</price>",
            "<currencyId>KZT</currencyId>",
            f"<categoryId>{cid}</categoryId>",
        ]
        if it.get("url"):
            out.append(f"<url>{yml_escape(it['url'])}</url>")
        if it.get("picture"):
            out.append(f"<picture>{yml_escape(it['picture'])}</picture>")

        desc = it.get("description") or it["title"]
        out.append(f"<description>{yml_escape(desc)}</description>")

        # НОВОЕ: отдельный тег наличия (по умолчанию true, как ранее)
        out.append("<available>true</available>")

        # Лишние теги количества УДАЛЕНЫ
        out.append("</offer>")
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
    print(f"[xls] sheet: {sheet}, rows: {len(rows)}", flush=True)

    row0, row1, idx = detect_header_two_row(rows)
    if row0 < 0:
        print("[error] Не удалось распознать шапку.", flush=True)
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
        print("[error] После фильтра по startswith/цене нет позиций.", flush=True)
        return 2
    print(f"[xls] candidates (startswith): {len(xlsx_items)}, distinct keys: {len(want_keys)}", flush=True)

    # 3) релевантные категории
    cats = discover_relevant_category_urls()
    if not cats:
        print("[error] Не нашли релевантных категорий.", flush=True)
        return 2
    print(f"[cats] relevant categories: {len(cats)}", flush=True)

    # 4) ссылки на карточки из категорий
    product_urls: List[str] = []
    pages_budget = max(1, MAX_CATEGORY_PAGES // max(1, len(cats)))
    for cu in cats:
        urls = collect_product_urls_from_category(cu, pages_budget)
        product_urls.extend(urls)
    product_urls = list(dict.fromkeys(product_urls))
    print(f"[crawl] product urls from categories: {len(product_urls)}", flush=True)

    # 5) парс карточек
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
                print(f"[match] keys matched: {len(matched_keys)} / {len(want_keys)}", flush=True)
            if matched_keys >= want_keys:
                print("[match] all wanted keys found.", flush=True)
                break

    print(f"[index] matched keys total: {len(matched_keys)}", flush=True)

    # 6) категории (по крошкам)
    all_paths = [rec.get("crumbs") for rec in site_index.values() if rec.get("crumbs")]
    cat_list, path_id_map = build_categories_from_paths(all_paths)
    print(f"[cats] built: {len(cat_list)}", flush=True)

    # 7) мёрдж (только с фото)
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
            clean = [p for p in clean if p.lower() not in ("главная", "home", "каталог")]
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
        print("[error] Ни одной позиции не сопоставили с фото (после startswith).", flush=True)
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with open(OUT_FILE, "w", encoding=FILE_ENCODING, errors="replace") as f:
            f.write(build_yml([], []))
        return 2

    # 8) YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(cat_list, offers)
    with open(OUT_FILE, "w", encoding=FILE_ENCODING, errors="replace") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, categories: {len(cat_list)} -> {OUT_FILE}", flush=True)
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e, flush=True)
        sys.exit(2)

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
