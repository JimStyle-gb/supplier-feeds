# -*- coding: utf-8 -*-
import os, re, json, sys
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from openpyxl import load_workbook

HOST = "https://copyline.kz"

XLSX_PATH = "docs/copyline.xlsx"           # прайс
URLS_FILE = "docs/copyline_urls.txt"       # список URL-ов категорий для обхода (если нет — возьмём дефолтный список ниже)
KEEP_FILE = "docs/categories_copyline.txt" # фильтр по названию (брендам), по строкам (нижний регистр)
CACHE_JSON = "docs/cache_copyline.json"    # кеш: { "tokenmap": { TOKEN: {"image":..., "url":...} } }
OUT_YML    = "docs/copyline.yml"

ROOT_CAT_ID = "9300000"
SHOP_NAME   = "copyline-xlsx"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
TIMEOUT = 20

DEFAULT_CATEGORY_URLS = [
    "https://copyline.kz/goods/developers.html",
    "https://copyline.kz/goods/drum-unit-brother.html",
    "https://copyline.kz/goods/drum-unit-canon.html",
    "https://copyline.kz/goods/drum-unit-epson.html",
    "https://copyline.kz/goods/drum-unit-konica-minolta.html",
    "https://copyline.kz/goods/drum-unit-kyocera.html",
    "https://copyline.kz/goods/drum-unit-lexmark.html",
    "https://copyline.kz/goods/drum-unit-panasonic.html",
    "https://copyline.kz/goods/drum-unit-ricoh.html",
    "https://copyline.kz/goods/drum-unit-samsung.html",
    "https://copyline.kz/goods/drum-unit-xerox.html",
    "https://copyline.kz/goods/laser-cartridges-canon.html",
    "https://copyline.kz/goods/laser-cartridges-hp.html",
    "https://copyline.kz/goods/laser-cartridges-hp-clj.html",
    "https://copyline.kz/goods/laser-cartridges-lexmark.html",
    "https://copyline.kz/goods/laser-cartridges-ricoh.html",
    "https://copyline.kz/goods/laser-cartridges-samsung.html",
    "https://copyline.kz/goods/laser-cartridges-xerox.html",
    "https://copyline.kz/goods/network-hardware.html",
    "https://copyline.kz/goods/toner-cartridges-brother.html",
    "https://copyline.kz/goods/toner-cartridges-canon.html",
    "https://copyline.kz/goods/toner-cartridges-epson.html",
    "https://copyline.kz/goods/toner-cartridges-konica-minolta.html",
    "https://copyline.kz/goods/toner-cartridges-kyocera.html",
    "https://copyline.kz/goods/toner-cartridges-oki.html",
    "https://copyline.kz/goods/toner-cartridges-panasonic.html",
    "https://copyline.kz/goods/toner-cartridges-ricoh.html",
    "https://copyline.kz/goods/toner-cartridges-samsung.html",
    "https://copyline.kz/goods/toner-cartridges-sharp.html",
    "https://copyline.kz/goods/toner-cartridges-toshiba.html",
    "https://copyline.kz/goods/toner-cartridges-xerox.html",
]

def log(*a): print("[build_copyline]", *a, file=sys.stderr)

def read_lines(path):
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return [x.strip() for x in f if x.strip() and not x.strip().startswith("#")]

def load_cache():
    if os.path.exists(CACHE_JSON):
        try:
            return json.load(open(CACHE_JSON, "r", encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(obj):
    os.makedirs(os.path.dirname(CACHE_JSON), exist_ok=True)
    json.dump(obj, open(CACHE_JSON, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

def req(url):
    r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

TOKEN_RE = re.compile(
    r"(?:[A-Z]{1,6}-?\d{2,6}[A-Z0-9]{0,2}|\d{2,6}[A-Z]{1,6}\d{0,3}|106R\d{5}|CE\d{3}\w?)"
)

def extract_tokens(text):
    if not text: return []
    up = re.sub(r"\s+", " ", str(text)).upper()
    toks = TOKEN_RE.findall(up)
    seen, out = set(), []
    for t in toks:
        if t not in seen:
            seen.add(t); out.append(t)
    return out

def find_product_links(html):
    soup = BeautifulSoup(html, "lxml")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"): href = urljoin(HOST, href)
        if ("/goods/" in href) and href.endswith(".html"):
            links.add(href)
    return list(links)

def parse_product_page(url):
    try:
        html = req(url)
    except Exception:
        return None, None
    soup = BeautifulSoup(html, "lxml")
    img = soup.find("img", id=re.compile(r"^main_image"))
    if not img or not img.get("src"):
        return None, None
    img_url = urljoin(HOST, img["src"])
    h1 = soup.find("h1")
    name = h1.get_text(strip=True) if h1 else (img.get("alt") or "")
    return name, img_url

def crawl_and_fill_tokenmap(category_urls, tokenmap):
    visited = set()
    for cu in category_urls:
        try:
            html = req(cu)
        except Exception:
            continue
        for prod_url in find_product_links(html):
            if prod_url in visited: 
                continue
            visited.add(prod_url)
            name, image = parse_product_page(prod_url)
            if not image:
                continue
            tokens = extract_tokens(name) or extract_tokens(image)
            for t in tokens:
                if t not in tokenmap:
                    tokenmap[t] = {"image": image, "url": prod_url}
    return tokenmap

def norm(s):
    s = str(s if s is not None else "").strip()
    s = s.replace("\xa0"," ").replace("ё","е").lower()
    return s

def to_int_price(x):
    s = str(x if x is not None else "").strip()
    s = s.replace("\xa0"," ").replace(" ","").replace(",",".")
    try:
        v = float(s)
        return int(round(v))
    except:
        # иногда цена «11 405,00   шт» из-за склеек — вытащим первое число
        import re as _re
        m = _re.search(r"(\d+[.,]?\d*)", s)
        if m:
            try:
                return int(round(float(m.group(1).replace(",","."))))
            except:
                return 0
        return 0

def stock_to_flags(s):
    txt = str(s if s is not None else "").strip()
    if txt in ("-", "", "нет"): 
        return False, 0
    m = re.search(r"\d+", txt)
    if m:
        n = int(m.group(0))
        return (n > 0), n
    if txt.startswith(">"):
        return True, 50
    return False, 0

def load_copyline_xlsx(xlsx_path):
    """Читаем любой «кривой» прайс: сами находим строку-шапку и нужные колонки."""
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"Файл не найден: {xlsx_path}")

    wb = load_workbook(xlsx_path, data_only=True, read_only=True)
    # ищем по всем листам первую строку, где встречаются Номенклатура / Артикул / Цена / Остаток
    wanted = {
        "name": ["номенклатура","наименование","товар"],
        "article": ["номенклатура.артикул","артикул","код"],
        "price": ["опт","цена"],
        "stock": ["остаток","наличие","кол-во","количество"]
    }

    def match_colnames(cells):
        cols = [norm(c.value) for c in cells]
        idx = {}
        for key, keys in wanted.items():
            best = None
            for i, val in enumerate(cols):
                if any(k in val for k in keys):
                    # приоритет: для price — «опт» важнее «цена»
                    if key=="price" and "опт" in val:
                        best = i; break
                    if best is None:
                        best = i
            idx[key] = best
        # минимум: name + price; article/stock опционально
        if idx["name"] is not None and idx["price"] is not None:
            return idx
        return None

    sheet_found = None
    header_row = None
    col_idx = None

    for ws in wb.worksheets:
        # просканируем первые 60 строк, ищем шапку
        for r in ws.iter_rows(min_row=1, max_row=60):
            idx = match_colnames(r)
            if idx:
                sheet_found = ws
                header_row = r[0].row
                col_idx = idx
                break
        if sheet_found:
            break

    if not sheet_found:
        raise RuntimeError("Не нашёл строку-шапку с колонками (Номенклатура/Артикул/Цена/Остаток).")

    rows = []
    # данные начинаются со следующей строки
    for r in sheet_found.iter_rows(min_row=header_row+1):
        get = lambda key: r[col_idx[key]].value if col_idx[key] is not None and col_idx[key] < len(r) else None
        name = str(get("name") or "").strip()
        if not name:
            continue
        article = str(get("article") or "").strip()
        price = to_int_price(get("price"))
        stock = get("stock")
        rows.append({
            "name": name,
            "article": article,
            "price": price,
            "stock": stock
        })

    return rows

def write_yml(rows, tokenmap, keep_words):
    root = ET.Element("yml_catalog")
    shop = ET.SubElement(root, "shop")
    ET.SubElement(shop, "name").text = SHOP_NAME
    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id="KZT", rate="1")
    cats = ET.SubElement(shop, "categories")
    ET.SubElement(cats, "category", id=ROOT_CAT_ID).text = "Copyline"
    offers = ET.SubElement(shop, "offers")

    for i, r in enumerate(rows):
        name = (r.get("name") or "").strip()
        if not name:
            continue

        if keep_words:
            nm = name.lower()
            if not any(k in nm for k in keep_words):
                continue

        price = int(r.get("price") or 0)
        art   = (r.get("article") or "").strip()
        available, qty = stock_to_flags(r.get("stock"))

        # если в прайсе нет артикула — попробуем вытащить токен из названия
        if not art:
            toks = extract_tokens(name)
            if toks: art = toks[0]

        offer_id = f"copyline:{art or i}"

        o = ET.SubElement(offers, "offer", id=offer_id, available="true" if available else "false", in_stock="true" if available else "false")
        ET.SubElement(o, "name").text = name
        if price > 0:
            ET.SubElement(o, "price").text = str(price)
        ET.SubElement(o, "currencyId").text = "KZT"
        ET.SubElement(o, "categoryId").text = ROOT_CAT_ID
        if art:
            ET.SubElement(o, "vendorCode").text = art

        # картинка/URL из tokenmap по токенам из названия
        pic = None; purl = None
        for t in extract_tokens(name):
            hit = tokenmap.get(t)
            if hit:
                pic = hit.get("image"); purl = hit.get("url"); break
        if pic:
            ET.SubElement(o, "picture").text = pic
        if purl:
            ET.SubElement(o, "url").text = purl

        ET.SubElement(o, "quantity_in_stock").text = str(qty)
        ET.SubElement(o, "stock_quantity").text    = str(qty)
        ET.SubElement(o, "quantity").text          = str(qty)

    tree = ET.ElementTree(root)
    os.makedirs(os.path.dirname(OUT_YML), exist_ok=True)
    tree.write(OUT_YML, encoding="windows-1251", xml_declaration=True)
    log(f"YML готов: {OUT_YML}")

def main():
    print(f"[build_copyline] XLSX: {os.path.abspath(XLSX_PATH)}", file=sys.stderr)

    # кеш токенов -> (image,url)
    cache = load_cache()
    tokenmap = cache.get("tokenmap", {})

    # URLs категорий (для сбора картинок)
    urls = read_lines(URLS_FILE)
    if not urls:
        urls = DEFAULT_CATEGORY_URLS[:]
        log("docs/copyline_urls.txt не найден — берём дефолтный список категорий.")

    log(f"Категорий для обхода: {len(urls)}")
    tokenmap = crawl_and_fill_tokenmap(urls, tokenmap)
    save_cache({"tokenmap": tokenmap})
    log(f"Кеш токенов: {len(tokenmap)}")

    # грузим прайс из XLSX (устойчивый парсер)
    rows = load_copyline_xlsx(XLSX_PATH)

    # фильтры по названиям (если есть)
    keep_words = [x.lower() for x in read_lines(KEEP_FILE)]
    if keep_words:
        log(f"Фильтр по названиям: {keep_words}")

    write_yml(rows, tokenmap, keep_words)

if __name__ == "__main__":
    main()
