# -*- coding: utf-8 -*-
"""
Build Copyline YML feed from XLSX + site pages (photos, full descriptions, site categories).
"""

import os
import re
import io
import time
import html
import hashlib
import random
from typing import List, Dict, Tuple, Optional
import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://copyline.kz"
XLSX_URL = os.environ.get("XLSX_URL", f"{BASE}/files/price-CLA.xlsx")
KEYWORDS_FILE = os.environ.get("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE = os.environ.get("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = os.environ.get("OUTPUT_ENCODING", "windows-1251")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS = int(os.environ.get("REQUEST_DELAY_MS", "120"))
MIN_BYTES = int(os.environ.get("MIN_BYTES", "900"))
ALLOW_PREFIX_BRANDS = [w.strip().lower() for w in os.environ.get(
    "ALLOW_PREFIX_BRANDS",
    "ripo,hp,canon,samsung,xerox,brother,pantum,lexmark,kyocera,konica,minolta,ricoh,panasonic"
).split(",") if w.strip()]

MAX_SITEMAP_URLS = int(os.environ.get("MAX_SITEMAP_URLS", "12000"))
MAX_VISIT_PAGES = int(os.environ.get("MAX_VISIT_PAGES", "3500"))

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

ROOT_CAT_ID = 9300000
ROOT_CAT_NAME = "Copyline"

def sleep_jitter(ms: int):
    base = ms / 1000.0
    jitter = random.uniform(-0.12, 0.12) * base
    time.sleep(max(0.0, base + jitter))

def http_get(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            print(f"[warn] GET {url} -> {r.status_code}")
            return None
        c = r.content
        if len(c) < MIN_BYTES:
            print(f"[warn] too small ({len(c)} bytes): {url}")
            return None
        return c
    except Exception as e:
        print(f"[err] GET {url} -> {e}")
        return None

def make_soup(html_bytes: bytes) -> BeautifulSoup:
    return BeautifulSoup(html_bytes, "html.parser")

def normalize_img_to_full(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = BASE + url
    m = re.match(r"^(https?://[^/]+)(/.*/)([^/]+)$", url)
    if not m:
        return url
    host, path, fname = m.groups()
    if fname.startswith("full_"):
        return url
    if fname.startswith("thumb_"):
        fname = "full_" + fname[len("thumb_"):]
    else:
        fname = "full_" + fname
    return f"{host}{path}{fname}"

def clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def slugify(text: str) -> str:
    t = text.strip().lower()
    t = re.sub(r"[^\w\s-]+", "", t)
    t = re.sub(r"\s+", "-", t)
    t = re.sub(r"-{2,}", "-", t)
    return (t.strip("-")[:80] or "item")

def stable_id(seed: str, prefix: int = 9400000) -> int:
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

COL_ALIASES = {
    "name": ["наименование", "название", "товар", "номенклатура", "наим.", "наим"],
    "price": ["цена", "цена, тг", "цена тг", "розничная цена", "стоимость"],
    "sku": ["артикул", "код", "код товара", "vendorcode", "sku"],
}

def choose_column(df: pd.DataFrame, keys: List[str]) -> Optional[str]:
    low = {c.strip().lower(): c for c in df.columns}
    for k in keys:
        for c_low, c_real in low.items():
            if k == c_low or c_low.startswith(k):
                return c_real
    return None

def read_xlsx(url: str) -> pd.DataFrame:
    b = http_get(url)
    if not b:
        raise RuntimeError("Не удалось скачать XLSX.")
    df = pd.read_excel(io.BytesIO(b), engine="openpyxl")
    name_col = choose_column(df, COL_ALIASES["name"])
    price_col = choose_column(df, COL_ALIASES["price"])
    sku_col = choose_column(df, COL_ALIASES["sku"])
    if not name_col or not price_col:
        raise RuntimeError("Нет обязательных столбцов 'Название'/'Цена'.")
    out = pd.DataFrame({
        "name": df[name_col].astype(str),
        "price": pd.to_numeric(df[price_col], errors="coerce"),
    })
    out["sku"] = df[sku_col].astype(str) if sku_col else ""
    out = out[~out["name"].str.strip().eq("")]
    out = out[out["price"].fillna(0) > 0]
    out.reset_index(drop=True, inplace=True)
    return out

def load_keywords(path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    except FileNotFoundError:
        return []

def build_startswith_patterns(keywords: List[str]) -> List[re.Pattern]:
    patterns = []
    brand_part = ""
    if ALLOW_PREFIX_BRANDS:
        brand_escaped = [re.escape(b) for b in ALLOW_PREFIX_BRANDS if b]
        brand_part = r"(?:\s*(?:%s)\s+)?" % ("|".join(brand_escaped))
    for kw in keywords:
        kw = kw.strip()
        if not kw:
            continue
        kw_esc = re.escape(kw)
        pat = r"^\s*" + brand_part + kw_esc + r"(?:\b|[^0-9A-Za-zА-Яа-я])"
        patterns.append(re.compile(pat, re.IGNORECASE))
    return patterns

def name_startswith_keyword(name: str, patterns: List[re.Pattern]) -> bool:
    s = name.strip()
    for p in patterns:
        m = p.search(s)
        if m and m.start() == 0:
            return True
    return False

URL_TOKEN_HINTS = [
    "drum", "dr-", "dr", "drunit", "drum-unit",
    "toner", "cartridge", "tn-",
    "developer", "dev-",
    "cable", "kabel", "kab", "patch",
    "fuser", "termoblock", "termo", "heat", "heater",
]

def extract_code_tokens(text: str) -> List[str]:
    t = text.upper()
    tokens = set()
    for m in re.findall(r"\b[A-ZА-Я0-9]{1,6}(?:-[A-ZА-Я0-9]{1,6}){0,3}\b", t):
        if any(ch.isdigit() for ch in m) and len(m) >= 3:
            tokens.add(m)
    for m in re.findall(r"\bC-EXV\d{1,3}\b", t):
        tokens.add(m)
    return list(tokens)

def norm_token(s: str) -> str:
    return re.sub(r"[^0-9a-zа-я]", "", s.lower())

def fetch_sitemap_urls(root_url: str) -> List[str]:
    seen = set()
    out = []

    def _grab(url: str):
        if url in seen or len(out) >= MAX_SITEMAP_URLS:
            return
        seen.add(url)
        data = http_get(url)
        if not data:
            return
        soup = BeautifulSoup(data, "xml")
        for loc in soup.find_all("loc"):
            u = loc.get_text(strip=True)
            if not u:
                continue
            if u.endswith(".xml"):
                _grab(u)
            else:
                out.append(u)

    _grab(f"{BASE}/sitemap.xml")
    return list(dict.fromkeys(out))

def parse_product_page(url: str) -> Optional[Dict]:
    sleep_jitter(REQUEST_DELAY_MS)
    html_b = http_get(url)
    if not html_b:
        return None
    soup = make_soup(html_b)

    name = ""
    h1 = soup.find("h1")
    if h1:
        name = clean_text(h1.get_text(" ", strip=True))
    if not name and soup.title:
        name = clean_text(soup.title.get_text(" ", strip=True))
    if not name:
        return None

    img = soup.find("img", attrs={"id": re.compile(r"^main_image_")}) or soup.find("img", attrs={"itemprop": "image"})
    if not img:
        for c in soup.find_all("img"):
            src = c.get("src") or c.get("data-src") or ""
            if "img_products" in src:
                img = c
                break
        if not img:
            imgs = soup.find_all("img")
            img = imgs[0] if imgs else None
    pic = ""
    if img:
        src = img.get("src") or img.get("data-src") or ""
        pic = normalize_img_to_full(src)

    desc = ""
    for css in ["jshop_prod_description", "product_description", "prod_description", "description"]:
        el = soup.find(True, class_=lambda c: c and css in c)
        if el:
            desc = clean_text(el.get_text(" ", strip=True))
            if desc:
                break
    if not desc:
        main = soup.find("div", {"id": "content"}) or soup.find("div", {"class": re.compile("content|product", re.I)})
        if main:
            desc = clean_text(main.get_text(" ", strip=True))[:4000]

    cats = []
    bc = soup.find("ul", class_=re.compile("breadcrumb"))
    if bc:
        for a in bc.find_all("a"):
            t = clean_text(a.get_text(" ", strip=True))
            if t and t.lower() not in ("главная", "home", "наш каталог", "наши товары"):
                cats.append(t)
    cats = [c for c in cats if len(c) >= 2][:5]

    return {"url": url, "name": name, "picture": pic, "description": desc, "categories": cats}

def to_price_str(x: float) -> str:
    return str(int(x)) if float(x).is_integer() else f"{x:.2f}".rstrip("0").rstrip(".")

def build_yml(categories: List[Tuple[int, str, Optional[int]]], offers: List[Dict]) -> str:
    out = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append("<name>copyline</name>")
    out.append("<currencies><currency id=\"KZT\" rate=\"1\" /></currencies>")

    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{html.escape(ROOT_CAT_NAME)}</category>")
    for cid, cname, parent in categories:
        if parent:
            out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{html.escape(cname)}</category>")
        else:
            out.append(f"<category id=\"{cid}\">{html.escape(cname)}</category>")
    out.append("</categories>")

    out.append("<offers>")
    for o in offers:
        offer_id = f"copyline:{slugify(o['name'])}:{hashlib.md5((o['url'] + o.get('sku','')).encode('utf-8')).hexdigest()[:8]}"
        out.append(f"<offer id=\"{offer_id}\" available=\"true\" in_stock=\"true\">")
        out.append(f"<name>{html.escape(o['name'])}</name>")
        out.append(f"<vendor>Copyline</vendor>")
        if o.get("sku"):
            out.append(f"<vendorCode>{html.escape(o['sku'])}</vendorCode>")
        out.append(f"<price>{to_price_str(o['price'])}</price>")
        out.append(f"<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{o['categoryId']}</categoryId>")
        out.append(f"<url>{html.escape(o['url'])}</url>")
        if o.get("picture"):
            out.append(f"<picture>{html.escape(o['picture'])}</picture>")
        if o.get("description"):
            out.append(f"<description>{html.escape(o['description'])}</description>")
        out.append("<quantity_in_stock>1</quantity_in_stock>")
        out.append("<stock_quantity>1</stock_quantity>")
        out.append("<quantity>1</quantity>")
        out.append("</offer>")
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

def extract_code_tokens(text: str) -> List[str]:
    t = text.upper()
    tokens = set()
    for m in re.findall(r"\b[A-ZА-Я0-9]{1,6}(?:-[A-ZА-Я0-9]{1,6}){0,3}\b", t):
        if any(ch.isdigit() for ch in m) and len(m) >= 3:
            tokens.add(m)
    for m in re.findall(r"\bC-EXV\d{1,3}\b", t):
        tokens.add(m)
    return list(tokens)

def norm_token(s: str) -> str:
    return re.sub(r"[^0-9a-zа-я]", "", s.lower())

def main():
    df = read_xlsx(XLSX_URL)
    keywords = load_keywords(KEYWORDS_FILE)
    if not keywords:
        raise RuntimeError("Файл ключей пустой. Проверь docs/copyline_keywords.txt")
    patterns = build_startswith_patterns(keywords)

    df["name_clean"] = df["name"].astype(str).str.strip()
    fdf = df[df["name_clean"].apply(lambda s: name_startswith_keyword(s, patterns))].copy()
    fdf.drop(columns=["name_clean"], inplace=True)
    if fdf.empty:
        raise RuntimeError("После фильтрации по ключам не осталось ни одного товара.")

    all_tokens = set()
    for s in fdf["name"].tolist():
        for t in extract_code_tokens(s):
            all_tokens.add(t)
    norm_tokens = {norm_token(t) for t in all_tokens if t.strip()}

    urls = fetch_sitemap_urls(f"{BASE}/sitemap.xml")
    urls = [u for u in urls if "/goods/" in u][:MAX_SITEMAP_URLS]

    URL_TOKEN_HINTS = [
        "drum","dr-","dr","drunit","drum-unit",
        "toner","cartridge","tn-",
        "developer","dev-",
        "cable","kabel","kab","patch",
        "fuser","termoblock","termo","heat","heater",
    ]
    def url_ok(u: str) -> bool:
        path = u.lower()
        path_norm = norm_token(path)
        for tk in norm_tokens:
            if tk and tk in path_norm:
                return True
        for hint in URL_TOKEN_HINTS:
            if hint in path:
                return True
        return False

    cand_urls = [u for u in urls if url_ok(u)][:MAX_VISIT_PAGES]
    print(f"[site] sitemap goods: {len(urls)}, candidates to visit: {len(cand_urls)}")

    page_data: Dict[str, Dict] = {}
    for i, u in enumerate(cand_urls, 1):
        html_b = http_get(u)
        if not html_b:
            continue
        soup = BeautifulSoup(html_b, "html.parser")
        h1 = soup.find("h1")
        name = clean_text(h1.get_text(" ", strip=True)) if h1 else (clean_text(soup.title.get_text(" ", strip=True)) if soup.title else "")
        img = soup.find("img", attrs={"id": re.compile(r"^main_image_")}) or soup.find("img", attrs={"itemprop": "image"})
        if not img:
            for c in soup.find_all("img"):
                src = c.get("src") or c.get("data-src") or ""
                if "img_products" in src:
                    img = c
                    break
        pic = ""
        if img:
            src = img.get("src") or img.get("data-src") or ""
            pic = normalize_img_to_full(src)
        desc = ""
        for css in ["jshop_prod_description", "product_description", "prod_description", "description"]:
            el = soup.find(True, class_=lambda c: c and css in c)
            if el:
                desc = clean_text(el.get_text(" ", strip=True))
                if desc:
                    break
        if not desc:
            main = soup.find("div", {"id": "content"}) or soup.find("div", {"class": re.compile("content|product", re.I)})
            if main:
                desc = clean_text(main.get_text(" ", strip=True))[:4000]
        cats = []
        bc = soup.find("ul", class_=re.compile("breadcrumb"))
        if bc:
            for a in bc.find_all("a"):
                t = clean_text(a.get_text(" ", strip=True))
                if t and t.lower() not in ("главная", "home", "наш каталог", "наши товары"):
                    cats.append(t)
        cats = [c for c in cats if len(c) >= 2][:5]
        if name:
            page_data[u] = {"url": u, "name": name, "picture": pic, "description": desc, "categories": cats}
        if i % 50 == 0:
            print(f"[site] parsed: {i}/{len(cand_urls)}")

    if not page_data:
        raise RuntimeError("Не удалось разобрать ни одной карточки из сайтовых кандидатов.")

    token2urls: Dict[str, List[str]] = {}
    for u, pdict in page_data.items():
        for t in extract_code_tokens(pdict["name"]):
            token2urls.setdefault(norm_token(t), []).append(u)

    cat_tree: Dict[Tuple[str, Optional[str]], int] = {}
    def ensure_cat_id(path: List[str]) -> int:
        parent_id = ROOT_CAT_ID
        parent_name = ROOT_CAT_NAME
        for cname in path:
            key = (cname, parent_name)
            if key not in cat_tree:
                cid = stable_id(parent_name + ">" + cname)
                cat_tree[key] = cid
            parent_id = cat_tree[key]
            parent_name = cname
        return parent_id

    offers: List[Dict] = []
    used_urls = set()

    def pick_url_for_row(row: pd.Series) -> Optional[str]:
        tokens = extract_code_tokens(str(row.get("name", "")) + " " + str(row.get("sku", "")))
        candidates = []
        for t in tokens:
            nt = norm_token(t)
            for u in token2urls.get(nt, []):
                candidates.append(u)
        if not candidates:
            rn = str(row["name"]).lower()
            base_opts = []
            for kw in keywords:
                if kw.lower() in rn:
                    base_opts.append(kw.lower())
            base_opts = base_opts[:2]
            for u, pdict in page_data.items():
                h1 = pdict["name"].lower()
                if all(k in h1 for k in base_opts):
                    candidates.append(u)
        if not candidates:
            return None
        name_low = str(row["name"]).lower()
        brand_hits = [b for b in ALLOW_PREFIX_BRANDS if b in name_low]
        def score(u: str) -> int:
            sc = 0
            for t in tokens:
                if norm_token(t) in norm_token(u):
                    sc += len(t)
            for b in brand_hits:
                if b in u.lower():
                    sc += 3
            return sc
        candidates = sorted(set(candidates), key=lambda x: (-score(x), len(x)))
        for u in candidates:
            if u not in used_urls:
                return u
        return candidates[0]

    patterns = build_startswith_patterns(keywords)

    for _, row in fdf.iterrows():
        url = pick_url_for_row(row)
        if not url:
            continue
        pdata = page_data.get(url)
        if not pdata:
            continue
        if not name_startswith_keyword(pdata["name"], patterns):
            continue
        if not pdata.get("picture"):
            continue
        cat_path = pdata.get("categories") or ["Наши товары"]
        cat_id = ensure_cat_id(cat_path)
        offers.append({
            "name": pdata["name"],
            "price": float(row["price"]),
            "sku": str(row.get("sku", "") or ""),
            "url": url,
            "picture": pdata["picture"],
            "description": pdata.get("description", ""),
            "categoryId": cat_id
        })
        used_urls.add(url)

    if not offers:
        raise RuntimeError("No matched items with photos after filtering and mapping.")

    categories_out: List[Tuple[int, str, Optional[int]]] = []
    parent_map: Dict[str, List[str]] = {}
    name2parent: Dict[str, Optional[str]] = {}
    name_parent2id: Dict[Tuple[str, Optional[str]], int] = {}

    for (name, parent), cid in cat_tree.items():
        name_parent2id[(name, parent)] = cid
        parent_map.setdefault(parent, []).append(name)
        name2parent[name] = parent

    emitted = set()
    def emit_chain(name: str):
        if name in emitted:
            return
        parent = name2parent.get(name)
        if parent and parent != ROOT_CAT_NAME:
            emit_chain(parent)
            pid = name_parent2id[(parent, name2parent.get(parent))]
        else:
            pid = ROOT_CAT_ID
        cid = name_parent2id[(name, parent)]
        categories_out.append((cid, name, pid if pid != ROOT_CAT_ID else ROOT_CAT_ID))
        emitted.add(name)

    for name in list(name2parent.keys()):
        emit_chain(name)

    xml = build_yml(categories_out, offers)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] offers: {len(offers)} -> {OUT_FILE}")

if __name__ == "__main__":
    main()
