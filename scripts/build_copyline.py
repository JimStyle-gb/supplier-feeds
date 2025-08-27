# -*- coding: utf-8 -*-
"""
Satu YML из XLSX Copyline:
- Фильтр по ключам СТРОГО: фразы из списка без склонений/вариаций (регистронезависимо).
- Матч по артикулу с карточками сайта; фото обязательно.
- Описание: ПОЛНОЕ с карточки (без обрезки).
- Категории: по хлебным крошкам сайта, дерево как на copyline.kz (root -> lvl1 -> lvl2 ...).
- Префиксы у артикулов не добавляем (vendorCode и offer_id = как в прайсе).
"""

from __future__ import annotations
import os, re, io, time, html, hashlib, random, xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

# ---------- ENV / конфиг ----------
BASE_URL           = "https://copyline.kz"
XLSX_URL           = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
KEYWORDS_FILE      = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE           = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING    = os.getenv("OUTPUT_ENCODING", "windows-1251")

HTTP_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES          = int(os.getenv("MIN_BYTES", "1000"))
MAX_SITEMAP_URLS   = int(os.getenv("MAX_SITEMAP_URLS", "20000"))
MAX_VISIT_PAGES    = int(os.getenv("MAX_VISIT_PAGES", "6000"))

SUPPLIER_NAME      = "Copyline"
CURRENCY           = "KZT"

ROOT_CAT_ID        = 9300000
ROOT_CAT_NAME      = "Copyline"

UA = {"User-Agent": "Mozilla/5.0 (compatible; Copyline-XLSX-Site/1.2)"}

# ---------- утилиты ----------
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

def yml_escape(s: str) -> str:
    return html.escape(s or "")

def sanitize_title(s: str) -> str:
    if not s:
        return ""
    # убираем хвосты вида "(Артикул ...)"
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:200].rstrip()  # чуть длиннее оставим

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
    """Только A-Z0-9, верхний регистр."""
    return re.sub(r"[^A-Za-z0-9]+", "", v or "").upper()

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

# ---------- ключевые слова: СТРОГИЙ матч ----------
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

def compile_strict_patterns(kws: List[str]) -> List[re.Pattern]:
    """
    Строгое совпадение фраз/слов:
    - регистронезависимо
    - границы слова по краям (\b), НО внутри фразы символы ровно как в списке
      (например, 'тонер-картридж' НЕ совпадёт с 'тонер картридж').
    - одиночные слова НЕ совпадут с 'картриджа', 'драмовый' и т.п. благодаря \b.
    """
    pats: List[re.Pattern] = []
    for kw in kws:
        # экранируем спецсимволы, кроме пробелов/дефисов (их оставляем как есть)
        # но \b корректно работает для кириллицы в Python 3
        esc = re.escape(kw).replace(r"\ ", " ")
        patt = rf"(?<!\w){esc}(?!\w)"
        pats.append(re.compile(patt, flags=re.IGNORECASE))
    return pats

def title_matches_strict(title: str, patterns: List[re.Pattern]) -> bool:
    if not title:
        return False
    for p in patterns:
        if p.search(title):
            return True
    return False

# ---------- XLSX: двухстрочная шапка ----------
def fetch_xlsx_bytes(url: str) -> bytes:
    r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.content

def detect_header_two_row(rows: List[List[Any]], scan_rows: int = 60) -> Tuple[int,int,Dict[str,int]]:
    """Ищем 'Номенклатура' и следующую строку с 'Артикул'/'Цена'."""
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

# ---------- сайт: карточки (SKU, фото, описание, хлебные крошки) ----------
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
    # основные варианты блоков описания
    selectors = [
        '[itemprop="description"]',
        '.jshop_prod_description',
        '.product_description',
        '.prod_description',
        '.productfull',
        '#description',
        '.tab-content .description',
        '.tabs .description',
    ]
    for sel in selectors:
        el = s.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)
    # запасной вариант: крупный контент-блок карточки
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
            if not t: 
                continue
            tl = t.lower()
            if tl in ("главная", "home"):
                continue
            names.append(t.strip())
        if names:
            break
    # удаляем возможный дубликат названия товара в конце (если не ссылка)
    if names:
        names = [n for n in names if n]
    return names

def parse_product_page(url: str) -> Optional[Tuple[str, str, str, List[str]]]:
    """Возвращает (sku, picture_url, full_description, breadcrumbs) или None."""
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
    if not src:
        return None
    pic = normalize_img_to_full(urljoin(url, src))

    # description (полное)
    desc = extract_full_description(s) or ""

    # breadcrumbs
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

def site_bfs_products() -> List[str]:
    seeds = [
        f"{BASE_URL}/",
        f"{BASE_URL}/goods.html",
        f"{BASE_URL}/goods/toner-cartridges-brother.html",
    ]
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
            if ("/goods/" in href or "page=" in href or "PAGEN_" in href or "/page/" in href):
                if absu not in visited and absu not in queue:
                    queue.append(absu)

    return list(dict.fromkeys(found))

def build_site_index(target_keys: Set[str]) -> Dict[str, Dict[str, Any]]:
    """
    Возвращает map: norm_key(SKU) -> {url, pic, desc, crumbs}
    Останавливаемся, когда нашли все нужные ключи.
    """
    urls = fetch_sitemap_product_urls()
    if not urls:
        urls = site_bfs_products()

    index: Dict[str, Dict[str, Any]] = {}
    target_left = set(target_keys)

    for i, u in enumerate(urls, 1):
        parsed = parse_product_page(u)
        if not parsed:
            continue
        sku, pic, desc, crumbs = parsed
        raw = sku.strip()

        # ключи для матчей
        keys = { key_norm(raw), key_norm(raw.replace("-", "")) }
        if re.match(r"^[Cc]\d+$", raw):
            keys.add(key_norm(raw[1:]))
        if re.match(r"^\d+$", raw):
            keys.add(key_norm("C"+raw))

        for k in keys:
            if not target_keys or k in target_keys:
                index[k] = {"url": u, "pic": pic, "desc": desc, "crumbs": crumbs}
                if k in target_left:
                    target_left.remove(k)

        if i % 200 == 0:
            print(f"[crawl] parsed {i}/{len(urls)} | matched_keys={len(index)} | remaining={len(target_left)}")
        if target_keys and not target_left:
            break

    print(f"[crawl] site index built: {len(index)} keys (needed {len(target_keys)})")
    return index

# ---------- категории по крошкам ----------
def stable_cat_id(text: str, prefix: int = 9400000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories_from_paths(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
    """
    На вход: список путей крошек для разных товаров, напр.:
      ["Каталог", "Картриджи", "HP"]  или ["Картриджи", "HP"]
    Выход:
      - список категорий [(id, name, parentId)], где parentId может быть None (тогда parent=ROOT)
      - map path_tuple -> id
    """
    cat_map: Dict[Tuple[str,...], int] = {}
    cats: List[Tuple[int,str,Optional[int]]] = {}

    # Используем обычный список для порядка
    out_list: List[Tuple[int,str,Optional[int]]] = []

    for path in paths:
        # очищаем шум
        clean = [p.strip() for p in path if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная", "home", "каталог")]
        if not clean:
            continue
        # строим цепочку
        parent_id = ROOT_CAT_ID
        prefix: List[str] = []
        for name in clean:
            prefix.append(name)
            key = tuple(prefix)
            if key in cat_map:
                parent_id = cat_map[key]
                continue
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

    # Категории (с сохранением дерева)
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        if parent and parent != ROOT_CAT_ID:
            out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{yml_escape(name)}</category>")
        else:
            out.append(f"<category id=\"{cid}\" parentId=\"{ROOT_CAT_ID}\">{yml_escape(name)}</category>")
    out.append("</categories>")

    # Офферы
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
        # ПОЛНОЕ описание
        desc = it.get("description") or it["title"]
        out.append(f"<description>{yml_escape(desc)}</description>")
        out += ["<quantity_in_stock>1</quantity_in_stock>","<stock_quantity>1</stock_quantity>","<quantity>1</quantity>","</offer>"]
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------- MAIN ----------
def main() -> int:
    # 1) XLSX
    xlsx_bytes = fetch_xlsx_bytes(XLSX_URL)
    wb = load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    sheet = max(wb.sheetnames, key=lambda n: wb[n].max_row * max(1, wb[n].max_column))
    ws = wb[sheet]
    rows = [ [c for c in r] for r in ws.iter_rows(values_only=True) ]

    # 2) шапка
    row0, row1, idx = detect_header_two_row(rows)
    if row0 < 0:
        print("[error] Не удалось распознать шапку.")
        return 2

    data_start = row1 + 1
    name_col   = idx["name"]
    vendor_col = idx["vendor_code"]
    price_col  = idx["price"]

    # 3) ключи (строго)
    kw_list = load_keywords(KEYWORDS_FILE)
    kw_patterns = compile_strict_patterns(kw_list)

    # 4) кандидаты из XLSX
    xlsx_items: List[Dict[str,Any]] = []
    want_keys: Set[str] = set()
    for r in rows[data_start:]:
        name_raw = r[name_col]
        if not name_raw:
            continue
        title = sanitize_title(str(name_raw).strip())

        # СТРОГИЙ фильтр
        if not title_matches_strict(title, kw_patterns):
            continue

        price = to_number(r[price_col])
        if price is None or price <= 0:
            continue

        v_raw = r[vendor_col]
        vcode = (str(v_raw).strip() if v_raw is not None else "") or extract_sku_from_name(title) or ""
        if not vcode:
            continue

        # Варианты ключей для матчей (и сайт может иметь/не иметь 'C', дефисы и т.п.)
        variants = { vcode, vcode.replace("-", "") }
        if re.match(r"^[Cc]\d+$", vcode):
            variants.add(vcode[1:])
        if re.match(r"^\d+$", vcode):
            variants.add("C"+vcode)

        for v in variants:
            want_keys.add(key_norm(v))

        xlsx_items.append({
            "title": title,
            "price": float(f"{price:.2f}"),
            "vendorCode_raw": vcode,
        })

    if not xlsx_items:
        print("[error] После строгого фильтра по ключам/цене нет позиций.")
        return 2
    print(f"[xls] candidates: {len(xlsx_items)}, distinct match-keys: {len(want_keys)}")

    # 5) индекс сайта
    site_index = build_site_index(want_keys)

    # 6) категории по крошкам — собираем пути заранее
    all_paths: List[List[str]] = []
    for k, rec in site_index.items():
        crumbs = rec.get("crumbs") or []
        if crumbs:
            all_paths.append(crumbs)
    cat_list, path_id_map = build_categories_from_paths(all_paths)

    # 7) мёрдж (строго с фото) + назначение категорий из крошек
    offers: List[Tuple[int,Dict[str,Any]]] = []
    seen_offer_ids: Set[str] = set()

    for it in xlsx_items:
        raw_v = it["vendorCode_raw"]
        # ищем карточку
        found = None
        for v in { raw_v, raw_v.replace("-", "") } | ({raw_v[1:]} if re.match(r"^[Cc]\d+$", raw_v) else set()) | ({ "C"+raw_v } if re.match(r"^\d+$", raw_v) else set()):
            kn = key_norm(v)
            if kn in site_index:
                found = site_index[kn]
                break
        if not found:
            continue

        url  = found["url"]
        pic  = found["pic"]
        desc = found.get("desc") or it["title"]
        crumbs = found.get("crumbs") or []

        if not pic:
            continue

        # категория = последняя крошка, если дерево построено
        cid = ROOT_CAT_ID
        if crumbs:
            # берём полный путь и находим id последнего узла (если есть)
            clean = [p.strip() for p in crumbs if p and p.strip()]
            clean = [p for p in clean if p.lower() not in ("главная","home","каталог")]
            if clean:
                key = tuple(clean)
                # если нет полного пути, попробуем укороченные префиксы
                while key and key not in path_id_map:
                    key = key[:-1]
                if key and key in path_id_map:
                    cid = path_id_map[key]

        offer_id = raw_v  # без префиксов
        if offer_id in seen_offer_ids:
            # сделаем уникальный id, если вдруг дубль артикула
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
            "description": desc,  # ПОЛНОЕ описание с карточки
        }))

    if not offers:
        print("[error] Ни одной позиции не сопоставили с фото.")
        return 2

    # 8) YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(cat_list, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, categories: {len(cat_list)} -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        sys.exit(2)
