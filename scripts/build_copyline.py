# scripts/build_copyline.py
# -*- coding: utf-8 -*-
"""
Copyline -> Satu YML (flat <offers>)
script_version = copyline-2025-09-21.7

Изменения в этой версии:
- НЕ выводим тег <url>.
- Добавлен дополнительный пустой перенос строки между офферами
  (после </offer> вставляется пустая строка).
"""

from __future__ import annotations
import os, re, io, time, html, hashlib, random
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

# ---------- ENV / CONST ----------
BASE_URL           = "https://copyline.kz"
XLSX_URL           = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
KEYWORDS_FILE      = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE           = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC                = (os.getenv("OUTPUT_ENCODING", "windows-1251") or "").lower()
FILE_ENCODING      = "cp1251" if "1251" in ENC else (ENC or "utf-8")
XML_ENCODING       = "windows-1251" if "1251" in ENC else (ENC or "utf-8")

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

UA = {"User-Agent": "Mozilla/5.0 (compatible; Copyline-XLSX-Site/2.0)"}

# Запрещённые "бренды-поставщики"
BLOCK_SUPPLIER_BRANDS = {"copyline", "alstyle", "vtt"}

# Известные бренды печати/расходников (алиасы -> отображаемое имя)
BRAND_ALIASES = {
    # OEM
    "hp": "HP", "hewlettpackard": "HP",
    "canon": "Canon",
    "xerox": "Xerox",
    "brother": "Brother",
    "kyocera": "Kyocera",
    "ricoh": "Ricoh",
    "konicaminolta": "Konica Minolta", "minolta": "Konica Minolta",
    "epson": "Epson",
    "samsung": "Samsung",
    "lexmark": "Lexmark",
    "panasonic": "Panasonic",
    "sharp": "Sharp",
    "oki": "OKI",
    "toshiba": "Toshiba",
    "dell": "Dell",
    # Aftermarket
    "europrint": "Euro Print", "euro print": "Euro Print",
    "nvprint": "NV Print", "nv print": "NV Print",
    "hiblack": "Hi-Black", "hi-black": "Hi-Black", "hi black": "Hi-Black",
    "profiline": "ProfiLine", "profi line": "ProfiLine",
    "staticcontrol": "Static Control", "static control": "Static Control",
    "gg": "G&G", "g&g": "G&G",
    "cactus": "Cactus",
    "patron": "Patron",
    "pitatel": "Pitatel",
    "mito": "Mito",
    "7q": "7Q",
    "uniton": "Uniton",
    "printpro": "PrintPro",
    "sakura": "Sakura",
}

# Общие слова, которые не должны становиться брендом в мягком фолбэке
STOPWORDS_BRAND = {
    # RU
    "картридж","тонер","тонер-картридж","драм","фотобарабан","узел","узел закрепления",
    "термоблок","термоэлемент","девелопер","порошок","бумага","пленка","ремкомплект",
    "для","без","с","набор","черный","чёрный","цветной","голубой","пурпурный","желтый",
    "серый","оранжевый","зеленый","фиолетовый","лазерный","струйный","принтер","мфу",
    "ресурс","оригинальный","совместимый","фото","блок","картриджа",
    # EN
    "cartridge","toner","drum","developer","fuser","kit","unit","black","cyan","magenta",
    "yellow","gray","orange","green","violet","laser","inkjet","printer","mfp","resource",
    "original","compatible","photo","block","for","without","with"
}

# ---------- helpers ----------
def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms/1000.0) * (1 + random.uniform(-0.15, 0.15)))

def http_get(url: str, tries: int = 3) -> Optional[bytes]:
    delay = max(0.05, REQUEST_DELAY_MS / 1000.0)
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
            if r.status_code == 200 and (len(r.content) >= MIN_BYTES if url.endswith(".xlsx") else True):
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
    import hashlib
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def norm_ascii(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def title_clean(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:200].rstrip()

def to_number(x: Any) -> Optional[float]:
    if x is None: return None
    s = str(x).replace("\xa0"," ").strip().replace(" ", "").replace(",", ".")
    if not re.search(r"\d", s): return None
    try:
        return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        return float(m.group(0)) if m else None

# ---------- keywords (простая загрузка с перебором кодировок) ----------
def load_keywords(path: str) -> List[str]:
    if not os.path.isfile(path):
        return []
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
            keys.append(s)
    return keys

def compile_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    pats: List[re.Pattern] = []
    for kw in kws:
        esc = re.escape(kw).replace(r"\ ", " ")
        pats.append(re.compile(r"^\s*" + esc + r"(?!\w)", re.I))
    return pats

def title_startswith_strict(title: str, patterns: List[re.Pattern]) -> bool:
    if not title: return False
    return any(p.search(title) for p in patterns)

# ---------- xlsx ----------
def fetch_xlsx_bytes(url: str) -> bytes:
    b = http_get(url, tries=3)
    if not b: raise RuntimeError("Не удалось скачать XLSX.")
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

# ---------- product page parsing ----------
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

def extract_specs_and_text(block: BeautifulSoup) -> Tuple[str, Dict[str,str]]:
    parts: List[str] = []
    specs: List[str] = []
    kv: Dict[str,str] = {}

    for ch in block.find_all(["p","h3","h4","h5","ul","ol"], recursive=False):
        tag = ch.name.lower()
        if tag in {"p","h3","h4","h5"}:
            t = re.sub(r"\s+"," ", ch.get_text(" ", strip=True)).strip()
            if t: parts.append(t)
        elif tag in {"ul","ol"}:
            for li in ch.find_all("li", recursive=False):
                t = re.sub(r"\s+"," ", li.get_text(" ", strip=True)).strip()
                if t: parts.append("- " + t)

    for tbl in block.find_all("table"):
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["th","td"])
            if len(cells) >= 2:
                k = re.sub(r"\s+"," ", cells[0].get_text(" ", strip=True)).strip()
                v = re.sub(r"\s+"," ", cells[1].get_text(" ", strip=True)).strip()
                if k and v:
                    specs.append(f"- {k}: {v}")
                    kv[k.strip().lower()] = v.strip()

    if specs and not any("технические характеристики" in (p.lower()) for p in parts):
        parts.append("Технические характеристики:")
    parts.extend(specs)

    txt = "\n".join([p for p in parts if p]).strip()
    return txt, kv

def extract_brand_from_specs_kv(kv: Dict[str,str]) -> Optional[str]:
    if not kv: return None
    for key in list(kv.keys()):
        k = key.strip().lower()
        if k in ("производитель", "бренд", "торговая марка", "trademark", "brand", "manufacturer"):
            val = kv[key].strip()
            if val: return val
    return None

def brand_from_text_heuristics(title: str, desc: str) -> Optional[str]:
    hay = f"{title or ''} {desc or ''}".strip().lower()
    if not hay: return None
    if norm_ascii(hay) in BRAND_ALIASES:
        return BRAND_ALIASES[norm_ascii(hay)]
    for norm, display in BRAND_ALIASES.items():
        if norm in BLOCK_SUPPLIER_BRANDS:
            continue
        if norm in norm_ascii(hay) or display.lower() in hay:
            return display
    return None

def sanitize_brand(b: Optional[str]) -> Optional[str]:
    if not b: return None
    b_stripped = re.sub(r"\s{2,}", " ", b).strip()
    n = norm_ascii(b_stripped)
    if not n: return None
    if n in BRAND_ALIASES:
        out = BRAND_ALIASES[n]
    else:
        out = b_stripped
    if norm_ascii(out) in BLOCK_SUPPLIER_BRANDS:
        return None
    return out

def brand_soft_fallback(title: str, desc: str) -> Optional[str]:
    text = f"{title or ''} {desc or ''}"
    if not text.strip():
        return None
    words = re.findall(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё\-]{1,20}", text)

    # биграммы (Euro Print, Konica Minolta, NV Print)
    for i in range(len(words)-1):
        pair = f"{words[i]} {words[i+1]}".strip()
        n = norm_ascii(pair)
        if n and n not in BLOCK_SUPPLIER_BRANDS and n not in STOPWORDS_BRAND and n in BRAND_ALIASES:
            return BRAND_ALIASES[n]

    # однословные кандидаты
    for w in words:
        n = norm_ascii(w)
        if not n or n in BLOCK_SUPPLIER_BRANDS or n in STOPWORDS_BRAND:
            continue
        if re.match(r"^[a-zа-я]+$", n) and 2 <= len(n) <= 20:
            return BRAND_ALIASES.get(n, w.strip().title())
    return None

def parse_product_page(url: str) -> Optional[Tuple[str, str, str, List[str], Optional[str]]]:
    """
    Возвращает: (sku, picture, description, breadcrumbs, brand)
    brand: markup -> specs -> эвристика (без мягкого фолбэка, он позже).
    """
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(url)
    if not b: return None
    s = soup_of(b)

    # sku
    sku = None
    skuel = s.find(attrs={"itemprop": "sku"})
    if skuel:
        val = (skuel.get_text(" ", strip=True) or "").strip()
        if val: sku = val
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

    # title
    h1 = s.find(["h1","h2"], attrs={"itemprop": "name"}) or s.find("h1") or s.find("h2")
    title = (h1.get_text(" ", strip=True) if h1 else "").strip()

    # description + specs-kv
    desc_txt = ""
    specs_kv: Dict[str,str] = {}
    block = s.select_one('div[itemprop="description"].jshop_prod_description') \
         or s.select_one('div.jshop_prod_description') \
         or s.select_one('[itemprop="description"]')
    if block:
        desc_txt, specs_kv = extract_specs_and_text(block)

    # breadcrumbs
    crumbs: List[str] = []
    for bc in s.select('ul.breadcrumb, .breadcrumbs, .breadcrumb, .pathway, [class*="breadcrumb"], [class*="pathway"]'):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            tl = t.lower()
            if t and tl not in ("главная","home"): crumbs.append(t)
        if crumbs: break

    # brand priority: markup -> specs -> heuristics (без мягкого фолбэка)
    brand = None
    bnode = s.select_one('div[itemprop="brand"] [itemprop="name"]') or s.select_one(".manufacturer_name")
    if bnode:
        brand = sanitize_brand(bnode.get_text(" ", strip=True))
    if not brand:
        brand = sanitize_brand(extract_brand_from_specs_kv(specs_kv))
    if not brand:
        brand = sanitize_brand(brand_from_text_heuristics(title, desc_txt))

    return sku, pic, (desc_txt or title), crumbs, brand

# ---------- categories helpers ----------
CATEGORY_HINTS = [
    "драм", "drum", "картридж", "тонер", "тонер-картридж", "девелопер",
    "фьюзер", "узел закрепления", "термоблок", "термоэлемент",
]

def text_contains_hint(t: str) -> bool:
    tl = (t or "").lower()
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
                if any(h in slug for h in ["drum","developer","fuser","toner","cartridge","драм","девелопер","фьюзер","термоблок","термоэлемент"]):
                    ok = True
            if ok and absu not in seen:
                seen.add(absu)
                urls.append(absu)
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
        if txt in ("следующая","вперед","вперёд","next",">"):
            return urljoin(page_url, a.get("href"))
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

# ---------- categories tree ----------
def stable_cat_id(text: str, prefix: int = 9400000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories_from_paths(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
    cat_map: Dict[Tuple[str,...], int] = {}
    out_list: List[Tuple[int,str,Optional[int]]] = []
    parent_root = ROOT_CAT_ID
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная","home","каталог")]
        if not clean: continue
        parent_id = parent_root
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
            f"<offer id=\"{yml_escape(it['offer_id'])}\">",
            f"<name>{yml_escape(it['title'])}</name>",
        ]
        brand = it.get("brand")
        if brand:
            out.append(f"<vendor>{yml_escape(brand)}</vendor>")
        out += [
            f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>",
            f"<price>{price_txt}</price>",
            "<currencyId>KZT</currencyId>",
            f"<categoryId>{cid}</categoryId>",
        ]
        # НЕ выводим <url>
        if it.get("picture"):
            out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        desc = it.get("description") or it["title"]
        out.append(f"<description>{yml_escape(desc)}</description>")
        out.append("<available>true</available>")
        out.append("</offer>")
        out.append("")  # дополнительный пустой перенос строки между офферами
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------- MAIN ----------
def main() -> int:
    # 1) XLSX
    b = fetch_xlsx_bytes(XLSX_URL)
    wb = load_workbook(io.BytesIO(b), read_only=True, data_only=True)
    sheet = max(wb.sheetnames, key=lambda n: wb[n].max_row * max(1, wb[n].max_column))
    ws = wb[sheet]
    rows = [[c for c in r] for r in ws.iter_rows(values_only=True)]
    print(f"[xls] sheet: {sheet}, rows: {len(rows)}", flush=True)

    # 2) шапка
    row0, row1, idx = detect_header_two_row(rows)
    if row0 < 0:
        print("[error] Не удалось распознать шапку.", flush=True)
        return 2
    data_start = row1 + 1
    name_col, vendor_col, price_col = idx["name"], idx["vendor_code"], idx["price"]

    # 3) keywords: strict startswith
    kw_list = load_keywords(KEYWORDS_FILE)
    start_patterns = compile_startswith_patterns(kw_list)

    xlsx_items: List[Dict[str,Any]] = []
    want_keys: Set[str] = set()

    for r in rows[data_start:]:
        name_raw = r[name_col]
        if not name_raw: continue
        title = title_clean(str(name_raw).strip())
        if not title_startswith_strict(title, start_patterns):
            continue

        price = to_number(r[price_col])
        if price is None or price <= 0: continue

        v_raw = r[vendor_col]
        vcode = (str(v_raw).strip() if v_raw is not None else "")
        if not vcode:
            m = re.search(r"[A-ZА-Я0-9]{2,}(?:[-/–][A-ZА-Я0-9]{2,})?", title.upper())
            if m: vcode = m.group(0).replace("–","-").replace("/","-")
        if not vcode: continue

        variants = { vcode, vcode.replace("-", "") }
        if re.match(r"^[Cc]\d+$", vcode): variants.add(vcode[1:])
        if re.match(r"^\d+$", vcode):     variants.add("C"+vcode)
        for v in variants:
            want_keys.add(norm_ascii(v))

        xlsx_items.append({
            "title": title,
            "price": float(f"{price:.2f}"),
            "vendorCode_raw": vcode,
        })

    if not xlsx_items:
        print("[error] После фильтра по startswith/цене нет позиций.", flush=True)
        return 2
    print(f"[xls] candidates: {len(xlsx_items)}, distinct keys: {len(want_keys)}", flush=True)

    # 4) категории и ссылки на карточки
    cats = discover_relevant_category_urls()
    if not cats:
        print("[error] Не нашли релевантных категорий.", flush=True)
        return 2
    pages_budget = max(1, MAX_CATEGORY_PAGES // max(1, len(cats)))

    product_urls: List[str] = []
    for cu in cats:
        product_urls.extend(collect_product_urls_from_category(cu, pages_budget))
    product_urls = list(dict.fromkeys(product_urls))
    print(f"[crawl] product urls: {len(product_urls)}", flush=True)

    # 5) парс карточек (с брендом)
    def worker(u: str):
        try:
            parsed = parse_product_page(u)
            if not parsed: return None
            sku, pic, desc, crumbs, brand = parsed
            raw = sku.strip()
            keys = { norm_ascii(raw), norm_ascii(raw.replace("-", "")) }
            if re.match(r"^[Cc]\d+$", raw): keys.add(norm_ascii(raw[1:]))
            if re.match(r"^\d+$",  raw):    keys.add(norm_ascii("C"+raw))
            return keys, {"url": u, "pic": pic, "desc": desc, "crumbs": crumbs, "brand": brand}
        except Exception:
            return None

    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MINUTES)
    site_index: Dict[str, Dict[str, Any]] = {}
    matched_keys: Set[str] = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = { ex.submit(worker, u): u for u in product_urls }
        for fut in as_completed(futures):
            if datetime.utcnow() > deadline: break
            out = fut.result()
            if not out: continue
            keys, payload = out
            useful = [k for k in keys if k in want_keys and k not in matched_keys]
            if not useful: continue
            for k in useful:
                site_index[k] = payload
                matched_keys.add(k)
            if len(matched_keys) % 50 == 0:
                print(f"[match] {len(matched_keys)} / {len(want_keys)}", flush=True)
            if matched_keys >= want_keys:
                print("[match] all wanted keys found.", flush=True)
                break

    print(f"[index] matched keys: {len(matched_keys)}", flush=True)

    # 6) дерево категорий
    all_paths = [rec.get("crumbs") for rec in site_index.values() if rec.get("crumbs")]
    cat_list, path_id_map = build_categories_from_paths(all_paths)
    print(f"[cats] built: {len(cat_list)}", flush=True)

    # 7) сборка офферов (brand с мягким фолбэком)
    offers: List[Tuple[int,Dict[str,Any]]] = []
    seen_offer_ids: Set[str] = set()

    for it in xlsx_items:
        raw_v = it["vendorCode_raw"]
        candidates = { raw_v, raw_v.replace("-", "") }
        if re.match(r"^[Cc]\d+$", raw_v): candidates.add(raw_v[1:])
        if re.match(r"^\d+$", raw_v):     candidates.add("C"+raw_v)

        found = None
        for v in candidates:
            kn = norm_ascii(v)
            if kn in site_index:
                found = site_index[kn]; break
        if not found or not found.get("pic"):
            continue

        url, pic = found["url"], found["pic"]
        desc = found.get("desc") or it["title"]
        crumbs = found.get("crumbs") or []
        brand = sanitize_brand(found.get("brand"))

        if not brand:
            brand = sanitize_brand(brand_from_text_heuristics(it["title"], desc))
        if not brand:
            brand = sanitize_brand(brand_soft_fallback(it["title"], desc))
        if brand and norm_ascii(brand) in BLOCK_SUPPLIER_BRANDS:
            brand = None

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
            "brand":      brand,            # может быть None -> <vendor> не пишем
            "url":        url,              # храним, но НЕ выводим в YML
            "picture":    pic,
            "description": desc,
        }))

    if not offers:
        print("[error] Ничего не сопоставили с фото.", flush=True)
        os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
        with open(OUT_FILE, "w", encoding=FILE_ENCODING, errors="replace") as f:
            f.write(build_yml([], []))
        return 2

    # 8) запись YML
    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
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
