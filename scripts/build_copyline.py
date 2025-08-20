# -*- coding: utf-8 -*-
import os, re, json, time, math, hashlib, unicodedata, html
from pathlib import Path
from urllib.parse import urljoin, quote_plus

import requests
from bs4 import BeautifulSoup
import pandas as pd
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
XLSX_PATH = DOCS / "copyline.xlsx"     # Прайс-лист от Copyline (клади сюда)
FILTER_PATH = DOCS / "categories_copyline.txt"
OUT_PATH = DOCS / "copyline.yml"
CACHE_PATH = DOCS / "copyline_cache.json"

SITE = "https://copyline.kz/"

# --------- утилиты ---------
def load_cache():
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache: dict):
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def norm_text(s):
    return (s or "").strip()

def as_float_kzt(s):
    if s is None: return None
    st = str(s)
    st = st.replace("\xa0"," ").replace(" "," ")
    st = st.replace(" ", "").replace(",", ".")
    m = re.search(r"(\d+(\.\d+)?)", st)
    return float(m.group(1)) if m else None

def as_int_stock(s):
    if s is None: return 0
    st = str(s).strip()
    if st.startswith(">") or st.startswith("≥"):
        m = re.search(r"(\d+)", st)
        return int(m.group(1)) if m else 50
    if st.startswith("<") or st.startswith("≤"):
        m = re.search(r"(\d+)", st)
        return max(1, int(m.group(1))) if m else 1
    m = re.search(r"(\d+)", st)
    return int(m.group(1)) if m else 0

def _slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-").lower()
    return s or "na"

SEEN_IDS = set()
BRAND_ALIASES = {
    "canon":"canon","hewlett-packard":"hp","hp":"hp","samsung":"samsung","xerox":"xerox",
    "kyocera":"kyocera","ricoh":"ricoh","lexmark":"lexmark","brother":"brother",
    "pantum":"pantum","toshiba":"toshiba","epson":"epson","cet":"cet","euro print":"europrint","europrint":"europrint",
}
def guess_brand(name, vendor=None):
    v = (vendor or "").strip().lower()
    if v in BRAND_ALIASES: return BRAND_ALIASES[v]
    n = (name or "").lower()
    for k,val in BRAND_ALIASES.items():
        if k in n: return val
    return ""

def make_unique_id(vendor_code: str, name: str, vendor: str|None) -> str:
    base = vendor_code.strip() if vendor_code else ""
    brand = guess_brand(name, vendor)
    raw = _slug(base) if base else _slug(name)[:50]
    uid = f"copyline:{raw}" + (f"-{brand}" if brand else "")
    if uid in SEEN_IDS:
        h = hashlib.sha1((name or "").encode("utf-8")).hexdigest()[:6]
        uid = f"{uid}-{h}"
    SEEN_IDS.add(uid)
    return uid

# --------- HTTP ---------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; CopylineFeedBot/1.0; +https://github.com/JimStyle-gb)",
    "Accept-Language": "ru,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
})

def get_html(url, retries=2, timeout=15):
    for i in range(retries+1):
        r = SESSION.get(url, timeout=timeout)
        if r.status_code == 200 and r.text:
            return r.text
        time.sleep(1.5 * (i+1))
    return ""

def find_product_link_by_search(query: str) -> str|None:
    # JoomShopping поиск по имени/артикулу
    search_url = f"{SITE}index.php?option=com_jshopping&controller=search&task=view&search_name={quote_plus(query)}"
    html = get_html(search_url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    # Ссылки на товары
    for a in soup.select("a"):
        href = a.get("href") or ""
        if "/goods/" in href or "controller=product" in href:
            return urljoin(SITE, href)
    return None

def extract_main_image(product_url: str) -> str|None:
    html = get_html(product_url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    # 1) <img itemprop="image" id="main_image_XXXX" src="...">
    img = soup.find("img", attrs={"itemprop":"image"})
    if not img:
        # иногда только по id начинается main_image_
        img = soup.find("img", id=re.compile(r"^main_image_\d+"))
    if img and img.get("src"):
        return urljoin(SITE, img["src"])
    return None

def find_picture_for(article: str, name: str, cache: dict) -> str|None:
    key = f"{article}|{name}"
    if key in cache and cache[key].get("image"):
        return cache[key]["image"]

    # шаг 1: пробуем поиск по артикулу
    if article:
        link = find_product_link_by_search(article)
        if link:
            img = extract_main_image(link)
            cache[key] = {"url": link, "image": img}
            return img

    # шаг 2: пробуем поиск по названию (укоротим до 60 символов)
    base_name = re.sub(r"\s+", " ", (name or "")).strip()
    if base_name:
        link = find_product_link_by_search(base_name[:60])
        if link:
            img = extract_main_image(link)
            cache[key] = {"url": link, "image": img}
            return img

    # шаг 3: эвристика по адресу картинки (напр. DR-1075.jpg, TN-2075.jpg)
    candidates = []
    code = re.sub(r"[^A-Za-z0-9_-]+","", (article or ""))
    if code:
        candidates += [
            f"{SITE}components/com_jshopping/files/img_products/{code}.jpg",
            f"{SITE}components/com_jshopping/files/img_products/{code.upper()}.jpg",
            f"{SITE}components/com_jshopping/files/img_products/{code.lower()}.jpg",
        ]
    # иногда имя содержит артикул
    m = re.search(r"\b([A-Za-z]{1,4}[-_]?\d{2,6}[A-Za-z]?)\b", base_name or "")
    if m:
        p = m.group(1)
        candidates += [
            f"{SITE}components/com_jshopping/files/img_products/{p}.jpg",
            f"{SITE}components/com_jshopping/files/img_products/{p.upper()}.jpg",
            f"{SITE}components/com_jshopping/files/img_products/{p.lower()}.jpg",
        ]

    for u in candidates:
        try:
            r = SESSION.get(u, timeout=8)
            if r.status_code == 200 and r.headers.get("Content-Type","").startswith("image"):
                cache[key] = {"url": cache.get(key,{}).get("url"), "image": u}
                return u
        except Exception:
            pass

    cache[key] = {"url": cache.get(key,{}).get("url"), "image": None}
    return None

# --------- категории (ал-стайл структура, используем только нужные) ---------
CAT_TREE = {
    "root": ("3557", None, "Картриджи и комплектующие"),
    "laser": ("5010", "3557", "Лазерные картриджи"),
    "parts": ("5002", "3557", "Запчасти и комплектующие"),

    # запчасти подкатегории
    "blade":  ("5011", "5002", "Дозирующие лезвия"),
    "rakel":  ("3574", "5002", "Ракельные ножи"),
    "pcr":    ("3560", "5002", "Валы заряда"),
    "dev":    ("3572", "5002", "Магнитные валы и валы проявки"),
    "pickup": ("3576", "5002", "Ролики захвата"),
    "sep":    ("5012", "5002", "Сепараторы (тормозные площадки)"),
    "rub":    ("3575", "5002", "Резиновые валы"),

    # бренды лазерных
    "hp":      ("3566", "5010", "Лазерные HP"),
    "samsung": ("3570", "5010", "Лазерные Samsung"),
    "ricoh":   ("3569", "5010", "Лазерные Ricoh"),
    "lexmark": ("4895", "5010", "Лазерные Lexmark"),
    "kyocera": ("3567", "5010", "Лазерные Kyocera"),
    "pantum":  ("21666","5010", "Лазерные Pantum"),
    "toshiba": ("5017", "5010", "Лазерные Toshiba"),
    "xerox":   ("5075", "3571" if False else "5010", "Совместимые Xerox"),  # оставим под 5010

    # canon делим на оригинал/совместимые
    "canon_o": ("21665","3565", "Оригинальные Canon"),
    "canon_c": ("21664","3565", "Совместимые Canon"),
}

def pick_category(name, vendor):
    n = (name or "").lower()
    b = guess_brand(name, vendor)
    # запчасти
    if "дозирующ" in n: return CAT_TREE["blade"][0]
    if "ракел" in n:    return CAT_TREE["rakel"][0]
    if "вал заряда" in n or "pcr" in n: return CAT_TREE["pcr"][0]
    if "вал прояв" in n or "dr " in n or "developer" in n: return CAT_TREE["dev"][0]
    if "ролик захв" in n or "ролик подач" in n: return CAT_TREE["pickup"][0]
    if "сепаратор" in n: return CAT_TREE["sep"][0]
    if "резиновый вал" in n: return CAT_TREE["rub"][0]

    # лазерные картриджи по бренду
    if b == "canon":
        if "oem" in n or "оригинал" in n or "original" in n:
            return CAT_TREE["canon_o"][0]
        return CAT_TREE["canon_c"][0]
    if b in ("hp","samsung","ricoh","lexmark","kyocera","pantum","toshiba","xerox"):
        return CAT_TREE[b][0]
    # по умолчанию
    return CAT_TREE["laser"][0]

def used_categories(ids):
    # собрать категории, которые реально нужны + их родители
    used = set(ids)
    by_id = {v[0]: (k,v) for k,v in CAT_TREE.items()}
    changed = True
    while changed:
        changed = False
        for _, v in CAT_TREE.items():
            cid, parent, _ = v
            if cid in used and parent:
                if parent not in used:
                    used.add(parent); changed = True
    return [by_id[cid][1] for cid in used if cid in by_id]

# --------- загрузка XLSX ---------
if not XLSX_PATH.exists():
    raise SystemExit(f"ERROR: {XLSX_PATH} не найден.")

df = pd.read_excel(XLSX_PATH, engine="openpyxl")

# найти колонки по подстрокам
cols = {c: str(c) for c in df.columns}
def find_col(*keys):
    for c in df.columns:
        s = str(c).lower()
        if all(k in s for k in keys):
            return c
    return None

col_name  = find_col("номенклатура")
col_art   = find_col("артик")
col_stock = find_col("остат")
col_price = find_col("цена")

if col_name is None or col_price is None:
    raise SystemExit("ERROR: не найдены колонки 'Номенклатура' и/или 'Цена' в docs/copyline.xlsx")

# фильтр по брендам
filters = []
if FILTER_PATH.exists():
    for line in FILTER_PATH.read_text(encoding="utf-8").splitlines():
        t = line.strip().lower()
        if t and not t.startswith("#"):
            filters.append(t)

def pass_filter(name):
    if not filters: return True
    ln = (name or "").lower()
    return any(f in ln for f in filters)

rows = []
for _, r in df.iterrows():
    name = norm_text(r.get(col_name))
    if not name or name.lower()=="товары": 
        continue
    if not pass_filter(name):
        continue
    article = norm_text(r.get(col_art))
    price = as_float_kzt(r.get(col_price))
    stock = as_int_stock(r.get(col_stock))
    rows.append({
        "name": name,
        "article": article,
        "price": price if price is not None else 0.0,
        "stock": stock,
    })

# --------- строим YML ---------
cache = load_cache()

y = ET.Element("yml_catalog")
shop = ET.SubElement(y, "shop")
ET.SubElement(shop, "name").text = "copyline-xlsx"
curr = ET.SubElement(shop, "currencies")
c = ET.SubElement(curr, "currency"); c.set("id","KZT"); c.set("rate","1")

offers_el = ET.SubElement(shop, "offers")

cat_ids_used = set()

for row in rows:
    name = row["name"]
    article = row["article"]
    price = row["price"]
    stock = row["stock"]
    vendor = ""  # можно попытаться выдернуть из имени, но сейчас не критично

    cat_id = pick_category(name, vendor)
    cat_ids_used.add(cat_id)

    offer = ET.SubElement(offers_el, "offer")
    offer.set("id", make_unique_id(article, name, vendor))
    offer.set("available", "true" if stock > 0 else "false")
    ET.SubElement(offer, "name").text = name
    if price and price > 0:
        ET.SubElement(offer, "price").text = str(int(round(price)))
    ET.SubElement(offer, "currencyId").text = "KZT"
    ET.SubElement(offer, "categoryId").text = cat_id
    if article:
        ET.SubElement(offer, "vendorCode").text = article
    # количество
    ET.SubElement(offer, "quantity_in_stock").text = str(stock)
    ET.SubElement(offer, "stock_quantity").text = str(stock)
    ET.SubElement(offer, "quantity").text = str(stock)

    # картинка — строго как ты просил: вытащить src с товарной страницы
    pic = find_picture_for(article, name, cache)
    if pic:
        ET.SubElement(offer, "picture").text = pic

# категории (вставляем перед offers)
cats_el = ET.SubElement(shop, "categories")
for cid, parent, title in used_categories(cat_ids_used):
    ce = ET.SubElement(cats_el, "category")
    ce.set("id", str(cid))
    if parent:
        ce.set("parentId", str(parent))
    ce.text = title

# сохранить кэш картинок/URL
save_cache(cache)

# выводим в CP1251
tree = ET.ElementTree(y)
xml_bytes = ET.tostring(y, encoding="windows-1251", xml_declaration=True)
OUT_PATH.write_bytes(xml_bytes)

print(f"[OK] Wrote {OUT_PATH}")
