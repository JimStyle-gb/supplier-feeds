# -*- coding: utf-8 -*-
import os, re, json, sys, time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd
import xml.etree.ElementTree as ET

HOST = "https://copyline.kz"

XLSX_PATH = "docs/copyline.xlsx"           # прайс (если есть)
URLS_FILE = "docs/copyline_urls.txt"       # список URL-ов категорий для обхода
KEEP_FILE = "docs/categories_copyline.txt" # бренды/ключи для фильтра (каждый с новой строки)
CACHE_JSON = "docs/cache_copyline.json"    # кеш { token -> {image,url} }
OUT_YML    = "docs/copyline.yml"

ROOT_CAT_ID = "9300000"    # корневая «Copyline»
SHOP_NAME   = "copyline-xlsx"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
TIMEOUT = 15

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
    # уникальные, по порядку
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
    """Вернёт (name, main_image_url) либо (None, None)"""
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
    name = h1.get_text(strip=True) if h1 else img.get("alt") or ""
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
            tokens = extract_tokens(name)
            if not tokens:
                tokens = extract_tokens(image)
            for t in tokens:
                # Не трогаем уже известные (кеш побеждает)
                if t not in tokenmap:
                    tokenmap[t] = {"image": image, "url": prod_url}
    return tokenmap

def load_xlsx_table(xlsx_path):
    if not os.path.exists(xlsx_path):
        log("XLSX отсутствует, будем работать только по парсингу категорий.")
        return pd.DataFrame(columns=["name","article","price","stock"])

    df = pd.read_excel(xlsx_path, engine="openpyxl")
    cols = list(df.columns)

    def pick(*need):
        for c in cols:
            lc = str(c).lower().replace("ё", "е")
            for n in need:
                if n in lc:
                    return c
        return None

    c_name  = pick("номенклатура") or pick("наименование") or pick("товар")
    c_art   = pick("артикул")
    c_price = pick("цена") or pick("опт")
    c_stock = pick("остаток") or pick("наличие")

    if not c_name or not c_price:
        raise RuntimeError(f"Не хватает колонок. Нашёл: name={c_name}, article={c_art}, price={c_price}, stock={c_stock}")

    out = pd.DataFrame()
    out["name"]    = df[c_name].astype(str)
    out["article"] = df[c_art].astype(str) if c_art else ""
    out["price"]   = pd.to_numeric(df[c_price], errors="coerce").fillna(0).astype(int)
    out["stock"]   = df[c_stock].astype(str) if c_stock else "1"
    return out

def stock_to_flags(s):
    txt = str(s).strip()
    # примеры: "<50", ">100", "10", "-", ""
    if txt in ("-", ""): return False, 0
    # вытащим число
    num = 0
    m = re.search(r"\d+", txt)
    if m:
        num = int(m.group(0))
    avail = num > 0 or txt.startswith(">")
    return avail, num if num>0 else (50 if txt.startswith(">") else 0)

def write_yml(rows, tokenmap):
    root = ET.Element("yml_catalog")
    shop = ET.SubElement(root, "shop")
    ET.SubElement(shop, "name").text = SHOP_NAME
    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id="KZT", rate="1")
    cats = ET.SubElement(shop, "categories")
    ET.SubElement(cats, "category", id=ROOT_CAT_ID).text = "Copyline"

    offers = ET.SubElement(shop, "offers")

    for idx, r in rows.iterrows():
        name = str(r.get("name","")).strip()
        if not name: 
            continue

        # фильтрация по KEEP_FILE (бренды/ключи)
        keep_words = [x.lower() for x in read_lines(KEEP_FILE)]
        if keep_words:
            nm = name.lower()
            if not any(k in nm for k in keep_words):
                continue

        price = int(r.get("price", 0) or 0)
        art   = str(r.get("article","")).strip()
        st    = r.get("stock","")
        available, qty = stock_to_flags(st)

        offer_id = f"copyline:{art or idx}"

        o = ET.SubElement(offers, "offer", id=offer_id, available="true" if available else "false", in_stock="true" if available else "false")
        ET.SubElement(o, "name").text = name
        if price>0:
            ET.SubElement(o, "price").text = str(price)
        ET.SubElement(o, "currencyId").text = "KZT"
        ET.SubElement(o, "categoryId").text = ROOT_CAT_ID
        if art:
            ET.SubElement(o, "vendorCode").text = art

        # КАРТИНКИ: 1 основная — из tokenmap, ничего не скачиваем
        pic = None; purl=None
        tokens = extract_tokens(name)
        for t in tokens:
            hit = tokenmap.get(t)
            if hit:
                pic  = hit.get("image")
                purl = hit.get("url")
                break
        if pic: 
            ET.SubElement(o, "picture").text = pic
        if purl:
            ET.SubElement(o, "url").text = purl

        # Кол-во (несколько полей — как просил ранее)
        ET.SubElement(o, "quantity_in_stock").text = str(qty)
        ET.SubElement(o, "stock_quantity").text    = str(qty)
        ET.SubElement(o, "quantity").text          = str(qty)

    tree = ET.ElementTree(root)
    os.makedirs(os.path.dirname(OUT_YML), exist_ok=True)
    tree.write(OUT_YML, encoding="windows-1251", xml_declaration=True)
    log(f"YML готов: {OUT_YML}")

def main():
    print(f"[build_copyline] XLSX: {os.path.abspath(XLSX_PATH)}", file=sys.stderr)

    # 1) грузим/обновляем кеш по категориям — для картинок
    tokenmap = load_cache().get("tokenmap", {})
    urls = read_lines(URLS_FILE)
    if urls:
        log(f"Обход категорий (для картинок): {len(urls)} URL")
        tokenmap = crawl_and_fill_tokenmap(urls, tokenmap)
        cache = {"tokenmap": tokenmap}
        save_cache(cache)
        log(f"В кеш добавлено токенов: {len(tokenmap)}")
    else:
        log("docs/copyline_urls.txt не найден — картинки будут, только если токены уже есть в кеше.")

    # 2) читаем прайс (если есть)
    df = load_xlsx_table(XLSX_PATH)

    # 3) строим YML
    write_yml(df, tokenmap)

if __name__ == "__main__":
    main()
