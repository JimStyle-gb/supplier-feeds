# -*- coding: utf-8 -*-
"""
Генератор YML для Satu.kz из прайса Copyline + фото с сайта copyline.kz.

Что делает:
1) Скачивает XLSX, находит шапку (поддержка двустрочной 'Номенклатура / Номенклатура.Артикул' + 'ОПТ / Цена').
2) Берёт товары, фильтрует по ключам (docs/copyline_keywords.txt), цена > 0.
3) Собирает с сайта список карточек /goods/*.html (через sitemap; fallback — обход сайта),
   парсит артикул (SKU) и главное фото (<img id="main_image_*"> или og:image), нормализует к 'full_*'.
4) Мёрджит товары из XLSX с карточками по артикулу. Если фото/URL не найден — товар пропускается.
5) Пишет docs/copyline.yml (Windows-1251), валюта KZT, категории определяет по названию (тонер, драм и т.п.).

Зависимости: requests, beautifulsoup4, openpyxl
"""

from __future__ import annotations
import os, re, io, time, html, hashlib, random, xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

# -------- ENV / конфиг --------
BASE_URL           = "https://copyline.kz"
XLSX_URL           = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
KEYWORDS_FILE      = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE           = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING    = os.getenv("OUTPUT_ENCODING", "windows-1251")
HTTP_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES          = int(os.getenv("MIN_BYTES", "1200"))
MAX_SITEMAP_URLS   = int(os.getenv("MAX_SITEMAP_URLS", "8000"))
MAX_VISIT_PAGES    = int(os.getenv("MAX_VISIT_PAGES", "2000"))

SUPPLIER_NAME = "Copyline"
CURRENCY      = "KZT"

ROOT_CAT_ID   = 9300000
ROOT_CAT_NAME = "Copyline"

UA = {"User-Agent": "Mozilla/5.0 (compatible; Copyline-XLSX-Site/1.0)"}

# -------- общие утилиты --------
def jitter_sleep(ms: int) -> None:
    base = ms / 1000.0
    time.sleep(max(0.0, base + random.uniform(-0.15, 0.15) * base))

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

def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "html.parser")

def yml_escape(s: str) -> str: return html.escape(s or "")

def sanitize_title(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:110].rstrip()

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

def normalize_vendor_code(v: Any) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    if not s or s.lower() == "nan": return None
    return s

def add_c_prefix_if_digits(vcode: str) -> str:
    """Для YML: если артикул — только цифры, добавляем префикс 'C' (только в вывод)."""
    if re.fullmatch(r"\d+", vcode):
        return f"C{vcode}"
    return vcode

def key_for_match(v: str) -> str:
    """Ключ сопоставления SKU: убираем всё, кроме букв/цифр; в верхний регистр."""
    return re.sub(r"[^A-Za-z0-9]+", "", v or "").upper()

def sha1(s: str) -> str: return hashlib.sha1(s.encode("utf-8")).hexdigest()

# -------- ключевые слова / категории --------
def load_keywords(path: str) -> List[str]:
    kws: List[str] = []
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip().lower()
                if s and not s.startswith("#"):
                    kws.append(s)
    if not kws:
        kws = ["drum","девелопер","драм","кабель сетевой","картридж","термоблок","термоэлемент","тонер-картридж"]
    return kws

def title_has_keyword(title: str, kws: List[str]) -> bool:
    t = title.lower().replace("ё", "е")
    t = re.sub(r"[\s\-]+", "", t)
    for kw in kws:
        k = kw.lower().replace("ё", "е")
        k = re.sub(r"[\s\-]+", "", k)
        if k and k in t:
            return True
    return False

def classify_category(title: str) -> Tuple[int, str]:
    tl = title.lower()
    if any(w in tl for w in ["драм","drum"]): return stable_cat_id("Драм-юниты"), "Драм-юниты"
    if "девелопер" in tl:                     return stable_cat_id("Девелоперы"), "Девелоперы"
    if "термоэлемент" in tl:                  return stable_cat_id("Термоэлементы"), "Термоэлементы"
    if "термоблок" in tl or "печка" in tl or "fuser" in tl:
                                               return stable_cat_id("Термоблоки"), "Термоблоки"
    if "кабель" in tl and "сет" in tl:        return stable_cat_id("Сетевые кабели"), "Сетевые кабели"
    return stable_cat_id("Тонер-картриджи"), "Тонер-картриджи"

def stable_cat_id(name: str, prefix: int = 9400000) -> int:
    h = hashlib.md5(name.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

# -------- XLSX parser (двухстрочная шапка) --------
def fetch_xlsx_bytes(url: str) -> bytes:
    r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.content

def detect_header_two_row(rows: List[List[Any]], scan_rows: int = 60) -> Tuple[int,int,Dict[str,int]]:
    """
    Возвращает (row0, row1, mapping) — индексы двух строк шапки и индексы колонок:
      mapping['name'], mapping['vendor_code'], mapping['price'].
    Грубая эвристика под файл Copyline: строка с "Номенклатура" + следующая со "Артикул"/"Цена".
    """
    def low(x): return str(x or "").strip().lower()
    for i in range(min(scan_rows, len(rows)-1)):
        row0 = [low(c) for c in rows[i]]
        row1 = [low(c) for c in rows[i+1]]
        if any("номенклатура" in c for c in row0):
            # кол-во колонок может отличаться, ищем по текстам
            name_col = next((j for j,c in enumerate(row0) if "номенклатура" in c), None)
            # во второй строке ищем 'артикул' и 'цена'
            vendor_col = next((j for j,c in enumerate(row1) if "артикул" in c), None)
            price_col  = next((j for j,c in enumerate(row1) if "цена" in c), None)
            if name_col is not None and (vendor_col is not None) and (price_col is not None):
                return i, i+1, {"name": name_col, "vendor_code": vendor_col, "price": price_col}
    # fallback: попробуем первую найденную «цена» во второй строке
    for i in range(min(scan_rows, len(rows)-1)):
        row0 = [low(c) for c in rows[i]]
        row1 = [low(c) for c in rows[i+1]]
        name_col = next((j for j,c in enumerate(row0) if any(k in c for k in ["номенклатура","наименован"])), None)
        price_col = next((j for j,c in enumerate(row1) if "цена" in c or "опт" in c), None)
        vendor_col = next((j for j,c in enumerate(row1) if "артикул" in c), None)
        if name_col is not None and price_col is not None and vendor_col is not None:
            return i, i+1, {"name": name_col, "vendor_code": vendor_col, "price": price_col}
    return -1, -1, {}

def extract_sku_from_name(name: str) -> Optional[str]:
    """Пытаемся выцепить SKU из названия: форматы типа DR-2300, CF226A и т.п."""
    t = name.upper()
    tokens = re.findall(r"[A-ZА-Я0-9]{2,}(?:[-/–][A-ZА-Я0-9]{2,})?", t)
    for tok in tokens:
        if re.search(r"[A-ZА-Я]", tok) and re.search(r"\d", tok):
            return tok.replace("–","-").replace("/","-")
    return None

# -------- сайт: сбор карточек и фото --------
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

def parse_product_page(url: str) -> Optional[Tuple[str, str]]:
    """Возвращает (vendor_code, picture_url) или None."""
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
        txt = s.get_text(" ", strip=True)
        m = re.search(r"(?:Артикул|SKU|Код)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})", txt, flags=re.I)
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
    if not src: return None
    pic = normalize_img_to_full(urljoin(url, src))
    return sku, pic

def fetch_sitemap_product_urls() -> List[str]:
    candidates = [
        f"{BASE_URL}/sitemap.xml",
        f"{BASE_URL}/sitemap_index.xml",
        f"{BASE_URL}/sitemap-index.xml",
        f"{BASE_URL}/sitemap1.xml",
        f"{BASE_URL}/sitemap-products.xml",
    ]
    urls: List[str] = []
    seen: Set[str] = set()
    def parse_sitemap(xml_bytes: bytes):
        try: root = ET.fromstring(xml_bytes)
        except Exception: return []
        locs = []
        for el in root.iter():
            t = el.tag.lower()
            if t.endswith("loc") and el.text:
                locs.append(el.text.strip())
        return locs
    # первичные
    for u in candidates:
        b = http_get(u)
        if b:
            for loc in parse_sitemap(b):
                if loc.lower().endswith(".xml"):
                    bx = http_get(loc)
                    if bx:
                        for loc2 in parse_sitemap(bx):
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

def site_bfs_products() -> List[str]:
    """Fallback обход: стартуем с корня и каталога, собираем /goods/*.html с пагинацией."""
    seeds = [f"{BASE_URL}/goods.html", f"{BASE_URL}/goods/toner-cartridges-brother.html", BASE_URL + "/"]
    queue: List[str] = seeds[:]
    visited: Set[str] = set()
    found: List[str] = []
    while queue and len(visited) < MAX_VISIT_PAGES:
        page = queue.pop(0)
        if page in visited: continue
        visited.add(page)
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(page)
        if not b: continue
        s = soup_of(b)
        for a in s.find_all("a", href=True):
            href = a["href"].strip()
            absu = urljoin(page, href)
            if "copyline.kz" not in absu: continue
            if PRODUCT_RE.search(absu) and not absu.endswith("/goods.html"):
                found.append(absu)
            if any(p in href for p in ["page=", "PAGEN_", "/page/"]) and absu not in visited:
                queue.append(absu)
    return list(dict.fromkeys(found))

def build_site_index(target_keys: Set[str]) -> Dict[str, Tuple[str,str]]:
    """
    Возвращает map: norm_key(SKU) -> (product_url, picture_url).
    Стараемся остановиться, когда нашли все таргетные ключи.
    """
    urls = fetch_sitemap_product_urls()
    if not urls:
        urls = site_bfs_products()

    index: Dict[str, Tuple[str,str]] = {}
    target_left = set(target_keys)

    for i, u in enumerate(urls, 1):
        parsed = parse_product_page(u)
        if not parsed: continue
        sku, pic = parsed
        # добавим несколько ключей для матчей (с/без 'C', без разделителей)
        keys = set()
        raw = sku.strip()
        keys.add(key_for_match(raw))
        if re.fullmatch(r"\d+", raw):
            keys.add(key_for_match("C"+raw))
        # также без дефисов
        keys.add(key_for_match(raw.replace("-", "")))
        for k in keys:
            # фиксируем только если нужен (ускоряет)
            if not target_keys or k in target_keys:
                index[k] = (u, pic)
                if k in target_left:
                    target_left.remove(k)
        if i % 200 == 0:
            print(f"[crawl] parsed {i}/{len(urls)}, matched={len(index)}")
        # если уже нашли все — можно завершать
        if target_keys and not target_left:
            break

    return index

# -------- генерация YML --------
def build_yml(categories: List[Tuple[int,str]], offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SUPPLIER_NAME.lower())}</name>")
    out.append('<currencies><currency id="KZT" rate="1" /></currencies>')

    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, cname in categories:
        out.append(f"<category id=\"{cid}\" parentId=\"{ROOT_CAT_ID}\">{yml_escape(cname)}</category>")
    out.append("</categories>")

    out.append("<offers>")
    for cid, it in offers:
        oid   = yml_escape(it["offer_id"])
        name  = yml_escape(it["title"])
        price = it["price"]
        price_txt = str(int(price)) if float(price).is_integer() else f"{price}"
        url   = yml_escape(it.get("url") or "")
        pic   = yml_escape(it.get("picture") or "")
        brand = yml_escape(it.get("brand") or SUPPLIER_NAME)
        vcode = yml_escape(it.get("vendorCode") or "")

        out.append(f'<offer id="{oid}" available="true" in_stock="true">')
        out.append(f"<name>{name}</name>")
        out.append(f"<vendor>{brand}</vendor>")
        if vcode:
            out.append(f"<vendorCode>{vcode}</vendorCode>")
        out.append(f"<price>{price_txt}</price>")
        out.append("<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        if url: out.append(f"<url>{url}</url>")
        if pic: out.append(f"<picture>{pic}</picture>")
        desc = re.sub(r"[^\x00-\x7F\u0400-\u04FF]+", " ", it.get("description") or it["title"])
        out.append(f"<description>{yml_escape(desc)}</description>")
        out += ["<quantity_in_stock>1</quantity_in_stock>","<stock_quantity>1</stock_quantity>","<quantity>1</quantity>","</offer>"]
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# -------- MAIN --------
def main() -> int:
    # 1) Загружаем XLSX
    xlsx_bytes = fetch_xlsx_bytes(XLSX_URL)
    wb = load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    # Берём самый «плотный» лист
    sheet = max(wb.sheetnames, key=lambda n: wb[n].max_row * max(1, wb[n].max_column))
    ws = wb[sheet]
    rows = [ [c for c in r] for r in ws.iter_rows(values_only=True) ]

    # 2) Ищем двухстрочную шапку
    row0, row1, idx = detect_header_two_row(rows)
    if row0 < 0:
        print("[error] Не удалось распознать шапку (двухстрочный заголовок).")
        return 2

    data_start = row1 + 1
    name_col   = idx["name"]
    vendor_col = idx["vendor_code"]
    price_col  = idx["price"]

    # 3) Ключи
    keywords = load_keywords(KEYWORDS_FILE)

    # 4) Собираем товары из XLSX (фильтр по ключам; только с ценой >0; SKU обязателен)
    xlsx_items: List[Dict[str,Any]] = []
    want_keys: Set[str] = set()  # ключи сопоставления для парсера сайта
    for r in rows[data_start:]:
        name_raw = r[name_col]
        if not name_raw: continue
        title = sanitize_title(str(name_raw).strip())
        if not title or not title_has_keyword(title, keywords):
            continue

        price = to_number(r[price_col])
        if price is None or price <= 0:
            continue

        v_raw = r[vendor_col]
        vcode = normalize_vendor_code(v_raw)
        if not vcode:
            # fallback: выцепить из названия
            guess = extract_sku_from_name(title)
            vcode = normalize_vendor_code(guess)
        if not vcode:
            continue

        # ключи для сопоставления (с/без 'C', без небуквенных символов)
        k1 = key_for_match(vcode)
        want_keys.add(k1)
        if re.fullmatch(r"\d+", vcode):
            want_keys.add(key_for_match("C"+vcode))
        want_keys.add(key_for_match(vcode.replace("-", "")))

        xlsx_items.append({
            "title": title,
            "price": float(f"{price:.2f}"),
            "vendorCode_raw": vcode,  # сырой из прайса
        })

    if not xlsx_items:
        print("[error] После фильтра по ключам/цене не осталось позиций.")
        return 2

    # 5) Индекс сайта: SKU -> (url, picture)
    site_index = build_site_index(want_keys)

    # 6) Мёрдж и генерация офферов (строго только с фото)
    categories: List[Tuple[int,str]] = []
    seen_cat: Set[int] = set()
    offers: List[Tuple[int,Dict[str,Any]]] = []
    seen_offer_ids: Set[str] = set()

    for it in xlsx_items:
        raw_v = it["vendorCode_raw"]
        # возможные ключи поиска
        keys = [
            key_for_match(raw_v),
            key_for_match(raw_v.replace("-", "")),
        ]
        if re.fullmatch(r"\d+", raw_v):
            keys.append(key_for_match("C"+raw_v))
        # ищем в индексе
        found = None
        for k in keys:
            if k in site_index:
                found = site_index[k]
                break
        if not found:
            continue  # без фото/URL — пропуск
        url, picture = found
        if not picture:
            continue

        title = it["title"]
        cid, cname = classify_category(title)
        if cid not in seen_cat:
            categories.append((cid, cname)); seen_cat.add(cid)

        v_for_yml = add_c_prefix_if_digits(raw_v)
        offer_id = v_for_yml or ("C" + sha1(title)[:16])
        if offer_id in seen_offer_ids:
            continue
        seen_offer_ids.add(offer_id)

        offers.append((cid, {
            "offer_id":   offer_id,
            "title":      title,
            "price":      it["price"],
            "vendorCode": v_for_yml,
            "brand":      SUPPLIER_NAME,
            "url":        url,
            "picture":    picture,
            "description": title,
        }))

    if not offers:
        print("[error] Ни одного товара не сопоставилось с карточкой с фото — проверь артикулы/ключевые слова.")
        return 2

    # 7) Пишем YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(categories, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, categories: {len(categories)} → {OUT_FILE}")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        sys.exit(2)
