# -*- coding: utf-8 -*-
"""
Build Copyline feed for Satu.kz

1) Берём XLSX прайс по URL (XLSX_URL).
2) Фильтруем позиции: название ДОЛЖНО начинаться с одного из ключевых слов
   (KEYWORDS_FILE), но допускается короткий бренд-префикс до ключа (белый список).
3) Краулим ТОЛЬКО целевые разделы сайта (CATEGORY_SEEDS), собираем страницы
   товаров /goods/*.html и строим индекс по артикулу и нормализованному имени.
4) Мерджим: берём цену из XLSX, фото/описание/крошки — со страницы товара.
   Если фото нет — товар пропускаем (по требованиям).
5) Пишем YML в кодировке windows-1251.

ENV:
  XLSX_URL, KEYWORDS_FILE, OUT_FILE, OUTPUT_ENCODING, HTTP_TIMEOUT,
  REQUEST_DELAY_MS, MIN_BYTES, MAX_CRAWL_MINUTES, MAX_CATEGORY_PAGES, MAX_WORKERS,
  BRAND_PREFIXES (через запятую)
"""

import os, re, io, time, html, hashlib, random, pathlib, concurrent.futures
from typing import List, Dict, Tuple, Optional
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ----------------- Config / ENV -----------------
BASE = "https://copyline.kz"
XLSX_URL = os.getenv("XLSX_URL", f"{BASE}/files/price-CLA.xlsx")
KEYWORDS_FILE = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "120"))
MIN_BYTES = int(os.getenv("MIN_BYTES", "900"))
MAX_CRAWL_MINUTES = int(os.getenv("MAX_CRAWL_MINUTES", "60"))
MAX_CATEGORY_PAGES = int(os.getenv("MAX_CATEGORY_PAGES", "800"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

# Разделы, где лежат нужные нам товары (можно дополнять/менять при надобности):
CATEGORY_SEEDS = [
    f"{BASE}/goods/drum-units.html",              # Драм-юниты
    f"{BASE}/goods/toner-cartridges.html",        # Тонер-картриджи
    f"{BASE}/goods/laser-cartridges.html",        # Картриджи лазерные
    f"{BASE}/goods/developer.html",               # Девелопер
    f"{BASE}/goods/fuser-unit.html",              # Термоблок
    f"{BASE}/goods/fuser-film.html",              # Термоэлемент
    f"{BASE}/goods/network-hardware.html",        # Сетевое оборудование (кабели, патч-корды)
]

# Белый список бренд-префиксов перед ключом:
_BRAND_PREFIXES_DEFAULT = [
    "RIPO", "EURO PRINT", "EUROPRINT", "OEM",
    "БУЛАТ", "BULAT", "INTEGRAL", "RETECH",
    "КАТЮША", "KATYUSHA", "PANTUM",
]
_env_prefixes = [p.strip() for p in os.getenv("BRAND_PREFIXES", "").split(",") if p.strip()]
BRAND_PREFIXES = _env_prefixes or _BRAND_PREFIXES_DEFAULT

ROOT_CAT_ID = 9300000
ROOT_CAT_NAME = "Copyline"

# --------------- Utils -----------------
def sleep_jitter(ms: int):
    base = ms / 1000.0
    time.sleep(max(0.0, base + random.uniform(-0.15, 0.15) * base))

def http_get(url: str) -> Optional[bytes]:
    try:
        sleep_jitter(REQUEST_DELAY_MS)
        r = requests.get(url, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            print(f"[warn] GET {url} -> {r.status_code}")
            return None
        b = r.content
        if len(b) < MIN_BYTES:
            print(f"[warn] small body {len(b)}: {url}")
            return None
        return b
    except Exception as e:
        print(f"[err] GET {url}: {e}")
        return None

def make_soup(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "html.parser")

def clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    return s

def stable_id(seed: str, prefix: int = 9400000) -> int:
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

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

# --------------- XLSX ---------------
NAME_COL_CANDIDATES = ["Название", "Наименование", "Товар", "Нименование", "Name"]
PRICE_COL_CANDIDATES = ["Цена", "Цена розничная", "Цена, тг", "Стоимость", "Розничная цена", "Price"]
SKU_COL_CANDIDATES = ["Артикул", "Код", "Код товара", "Артикул производителя", "VendorCode"]

def pick_col(df: pd.DataFrame, cand: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for want in cand:
        w = want.lower()
        if w in cols:
            return cols[w]
    # мягкий поиск по вхождению
    for c in df.columns:
        lc = c.lower()
        for want in cand:
            if want.lower() in lc:
                return c
    return None

def download_xlsx(url: str) -> pd.DataFrame:
    print(f"[xls] GET {url}")
    b = http_get(url)
    if not b:
        raise SystemExit("Error: не удалось скачать XLSX.")
    df = pd.read_excel(io.BytesIO(b), engine="openpyxl")
    # уберём полностью пустые строки
    df = df.dropna(how="all")
    if df.shape[0] == 0:
        raise SystemExit("Error: пустой XLSX.")
    return df

def read_keywords(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        kws = [clean_text(x) for x in f.read().splitlines()]
    kws = [k for k in kws if k and not k.startswith("#")]
    return kws

def _esc_spaces(s: str) -> str:
    # re.escape + вернуть обычные пробелы
    return re.escape(s).replace(r"\ ", " ")

def compile_startswith_patterns(kws: List[str], prefixes: List[str]) -> List[re.Pattern]:
    pats: List[re.Pattern] = []
    prefix_alt = "|".join(_esc_spaces(p) for p in prefixes) if prefixes else ""
    prefix_group = "(?:(?:" + prefix_alt + r")\s+){0,2}" if prefix_alt else ""
    for kw in kws:
        kw_esc = _esc_spaces(kw)
        regex = r"^\s*" + prefix_group + kw_esc + r"(?!\w)"
        pats.append(re.compile(regex, re.I))
    return pats

def name_startswith_allowed(name: str, pats: List[re.Pattern]) -> bool:
    s = clean_text(name)
    for p in pats:
        if p.search(s):
            return True
    return False

# --------------- Crawl targeted categories ---------------
def collect_product_links(cat_html: bytes) -> List[str]:
    soup = make_soup(cat_html)
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = BASE + href
        if re.search(r"https?://[^/]+/goods/[^?#]+\.html$", href):
            urls.append(href)
    # pagination: соберём ссылки, но реальный обход — снаружи
    return list(dict.fromkeys(urls))

def find_next_pages(cat_html: bytes) -> List[str]:
    soup = make_soup(cat_html)
    pages = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "start=" in href or "page=" in href or "limitstart" in href:
            if href.startswith("/"):
                href = BASE + href
            elif href.startswith("//"):
                href = "https:" + href
            if href.startswith(BASE):
                pages.add(href)
    return list(pages)

def parse_breadcrumbs(soup: BeautifulSoup) -> List[str]:
    # разные варианты разметки
    crumbs = []
    # <ul class="breadcrumb"> … <li>Категория</li> …
    crumb_ul = soup.find("ul", class_=lambda c: c and "breadcrumb" in c)
    if crumb_ul:
        for li in crumb_ul.find_all("li"):
            t = clean_text(li.get_text(" ", strip=True))
            if t and t.lower() not in ("главная", "каталог", "home"):
                crumbs.append(t)
    if not crumbs:
        # запасной вариант
        nav = soup.find("div", class_=lambda c: c and "breadcrumb" in c)
        if nav:
            for a in nav.find_all("a"):
                t = clean_text(a.get_text(" ", strip=True))
                if t and t.lower() not in ("главная", "каталог", "home"):
                    crumbs.append(t)
    return crumbs

def find_name(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return clean_text(h1.get_text(" ", strip=True))
    if soup.title and soup.title.get_text(strip=True):
        return clean_text(soup.title.get_text(" ", strip=True))
    return None

def find_sku(soup: BeautifulSoup) -> Optional[str]:
    # 1) itemprop
    sku_el = soup.find(attrs={"itemprop": "sku"})
    if sku_el:
        v = clean_text(sku_el.get_text(" ", strip=True))
        if v:
            return v
    # 2) метки
    labels = ["артикул", "sku", "код товара", "код:"]
    lab_el = soup.find(string=lambda t: t and any(x in t.lower() for x in labels))
    if lab_el:
        parent = lab_el.parent
        probes = []
        if parent:
            probes.extend(parent.find_all_next(string=True, limit=3))
        probes.append(lab_el.next_sibling)
        for s in probes:
            if not s:
                continue
            val = clean_text(str(s))
            if any(x in val.lower() for x in labels):
                continue
            m = re.search(r"([A-Za-z0-9\-]{2,})", val)
            if m:
                return m.group(1)
    # 3) грубый паттерн
    m = re.search(r"Артикул\W+([A-Za-z0-9\-]{2,})", soup.get_text(" ", strip=True), re.I)
    if m:
        return m.group(1)
    return None

def find_description(soup: BeautifulSoup) -> str:
    for css in ["jshop_prod_description", "description", "product_description", "prod_description"]:
        el = soup.find(True, class_=lambda c: c and css in c)
        if el:
            txt = clean_text(el.get_text(" ", strip=True))
            if txt:
                return txt[:10000]  # практически «полное»
    return ""

def find_picture(soup: BeautifulSoup) -> str:
    img = soup.find("img", attrs={"id": re.compile(r"^main_image_")})
    if not img:
        img = soup.find("img", attrs={"itemprop": "image"})
    if not img:
        for c in soup.find_all("img"):
            src = c.get("src") or c.get("data-src") or ""
            if "img_products" in src:
                img = c
                break
        if not img:
            imgs = soup.find_all("img")
            if imgs:
                img = imgs[0]
    if not img:
        return ""
    src = img.get("src") or img.get("data-src") or ""
    return normalize_img_to_full(src)

def crawl_targeted() -> Dict[str, Dict]:
    """
    Возвращает индекс карточек:
      by_sku[sku] = {...}
      by_name[norm_name] = {...}
    """
    seen_pages = set()
    product_urls = set()
    started = time.time()

    # очередь кат-страниц
    queue = list(CATEGORY_SEEDS)
    while queue and len(seen_pages) < MAX_CATEGORY_PAGES and (time.time() - started) < MAX_CRAWL_MINUTES * 60:
        url = queue.pop(0)
        if url in seen_pages:
            continue
        seen_pages.add(url)
        b = http_get(url)
        if not b:
            continue
        links = collect_product_links(b)
        product_urls.update(links)
        # пагинация
        for nxt in find_next_pages(b):
            if nxt not in seen_pages:
                queue.append(nxt)

    print(f"[crawl] category pages: {len(seen_pages)}, product urls found: {len(product_urls)}")

    by_sku: Dict[str, Dict] = {}
    by_name: Dict[str, Dict] = {}

    def worker(purl: str):
        b = http_get(purl)
        if not b:
            return
        soup = make_soup(b)
        name = find_name(soup) or ""
        sku = find_sku(soup) or ""
        pic = find_picture(soup)
        desc = find_description(soup)
        crumbs = parse_breadcrumbs(soup)
        if not pic:
            return  # фото обязательно
        data = {
            "url": purl,
            "name": name,
            "vendorCode": sku,
            "picture": pic,
            "description": desc,
            "crumbs": crumbs,
        }
        if sku:
            by_sku.setdefault(sku.strip().upper(), data)
        if name:
            key = re.sub(r"\s+", " ", name.strip().lower())
            by_name.setdefault(key, data)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(ex.map(worker, list(product_urls)))

    print(f"[crawl] indexed: by_sku={len(by_sku)}, by_name={len(by_name)}")
    return {"by_sku": by_sku, "by_name": by_name}

# --------------- Build YML -----------------
def slugify(text: str) -> str:
    t = text.strip().lower()
    t = re.sub(r"[^\w\s-]+", "", t, flags=re.UNICODE)
    t = re.sub(r"\s+", "-", t, flags=re.UNICODE)
    t = re.sub(r"-{2,}", "-", t, flags=re.UNICODE)
    return t.strip("-")[:80] or "item"

def build_categories(all_crumbs: List[List[str]]) -> Tuple[List[Tuple[int, Optional[int], str]], Dict[Tuple[str, ...], int]]:
    # формируем дерево id по path
    path2id: Dict[Tuple[str, ...], int] = {}
    cats: List[Tuple[int, Optional[int], str]] = []
    # добавим корень
    path2id[()] = ROOT_CAT_ID
    cats.append((ROOT_CAT_ID, None, ROOT_CAT_NAME))

    for crumbs in all_crumbs:
        path = []
        parent_id = ROOT_CAT_ID
        for c in crumbs:
            path.append(c)
            key = tuple(path)
            if key not in path2id:
                cid = stable_id(" / ".join(path))
                path2id[key] = cid
                cats.append((cid, parent_id, c))
            parent_id = path2id[key]
    return cats, path2id

def build_yml(offers: List[Dict]) -> str:
    # собрать список категорий из крошек
    crumbs_list = [o.get("crumbs") or [] for o in offers]
    cats, path2id = build_categories(crumbs_list)

    out = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append("<name>copyline</name>")
    out.append("<currencies><currency id=\"KZT\" rate=\"1\" /></currencies>")

    out.append("<categories>")
    for cid, parent_id, title in cats:
        t = html.escape(title)
        if parent_id:
            out.append(f"<category id=\"{cid}\" parentId=\"{parent_id}\">{t}</category>")
        else:
            out.append(f"<category id=\"{cid}\">{t}</category>")
    out.append("</categories>")

    out.append("<offers>")
    for o in offers:
        name_xml = html.escape(o["name"])
        url_xml = html.escape(o["url"])
        pic_xml = html.escape(o["picture"])
        sku_xml = html.escape(o.get("vendorCode") or "")
        desc_xml = html.escape(o.get("description") or "")
        price_val = o["price"]
        price_str = str(int(price_val)) if float(price_val).is_integer() else str(price_val)

        # offer id — стабильный
        raw_id = f"copyline:{slugify(o['name'])}:{hashlib.md5((o['url']+(sku_xml or 'n/a')).encode('utf-8')).hexdigest()[:8]}"

        # category id по крошкам
        crumbs = o.get("crumbs") or []
        parent_id = ROOT_CAT_ID
        path = []
        for c in crumbs:
            path.append(c)
            parent_id = stable_id(" / ".join(path))
        cid = parent_id

        out.append(f"<offer id=\"{raw_id}\" available=\"true\" in_stock=\"true\">")
        out.append(f"<name>{name_xml}</name>")
        out.append(f"<vendor>Copyline</vendor>")
        if sku_xml:
            out.append(f"<vendorCode>{sku_xml}</vendorCode>")
        out.append(f"<price>{price_str}</price>")
        out.append(f"<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        out.append(f"<url>{url_xml}</url>")
        out.append(f"<picture>{pic_xml}</picture>")
        if desc_xml:
            out.append(f"<description>{desc_xml}</description>")
        out.append(f"<quantity_in_stock>1</quantity_in_stock>")
        out.append(f"<stock_quantity>1</stock_quantity>")
        out.append(f"<quantity>1</quantity>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# --------------- Main pipeline ---------------
def main():
    # 1) XLSX
    df = download_xlsx(XLSX_URL)
    name_col = pick_col(df, NAME_COL_CANDIDATES)
    price_col = pick_col(df, PRICE_COL_CANDIDATES)
    sku_col = pick_col(df, SKU_COL_CANDIDATES)

    if not name_col or not price_col:
        raise SystemExit("Error: не удалось найти столбцы названия/цены в XLSX.")

    # 2) keywords + patterns (strict startswith with allowed brand prefixes)
    if not os.path.exists(KEYWORDS_FILE):
        raise SystemExit(f"Error: нет файла ключевых слов: {KEYWORDS_FILE}")
    kw_list = read_keywords(KEYWORDS_FILE)
    start_patterns = compile_startswith_patterns(kw_list, BRAND_PREFIXES)

    # 3) отфильтруем строки
    def good_row(row) -> bool:
        nm = str(row[name_col]) if pd.notna(row[name_col]) else ""
        if not nm.strip():
            return False
        return name_startswith_allowed(nm, start_patterns)

    df2 = df[df.apply(good_row, axis=1)].copy()
    df2 = df2[[c for c in [name_col, price_col, sku_col] if c is not None]].reset_index(drop=True)
    print(f"[xls] candidates (startswith): {len(df2)}")

    if len(df2) == 0:
        raise SystemExit("Error: после фильтрации по ключевым словам в начале — ничего не осталось.")

    # 4) краулим целевые категории и строим индекс
    index = crawl_targeted()
    by_sku = index["by_sku"]
    by_name = index["by_name"]

    # 5) сопоставляем и собираем офферы
    offers: List[Dict] = []
    miss = 0

    for _, row in df2.iterrows():
        nm = clean_text(str(row[name_col]))
        price_raw = row[price_col]
        try:
            price = float(str(price_raw).replace(" ", "").replace(",", "."))
        except Exception:
            continue
        # ищем карточку
        page = None
        sku_val = None
        if sku_col and pd.notna(row.get(sku_col, None)):
            sku_val = str(row[sku_col]).strip().upper()
            if sku_val and sku_val in by_sku:
                page = by_sku[sku_val]
        if not page:
            key = re.sub(r"\s+", " ", nm.strip().lower())
            if key in by_name:
                page = by_name[key]
        if not page or not page.get("picture"):
            miss += 1
            continue  # фото обязательно

        offers.append({
            "name": page["name"] or nm,  # берём название со страницы (обычно чище)
            "url": page["url"],
            "picture": page["picture"],
            "vendorCode": page.get("vendorCode") or (sku_val or ""),
            "description": page.get("description") or "",
            "crumbs": page.get("crumbs") or [],
            "price": max(0.01, round(price, 2)),
        })

    print(f"[match] offers with photos: {len(offers)}, skipped (no photo/no page): {miss}")

    if not offers:
        raise SystemExit("Error: нет совпадений с фото. Проверьте CATEGORY_SEEDS и ключевые слова.")

    xml_text = build_yml(offers)
    pathlib.Path(os.path.dirname(OUT_FILE) or ".").mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml_text)
    print(f"[done] wrote: {OUT_FILE}, offers: {len(offers)}")

if __name__ == "__main__":
    main()
