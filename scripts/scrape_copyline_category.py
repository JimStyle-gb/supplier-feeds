#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import html
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit
import posixpath

import requests
from bs4 import BeautifulSoup

# ----------------------- ENV -----------------------
BASE_URL         = os.getenv("BASE_URL", "https://copyline.kz").strip()
CATEGORY_URL     = os.getenv("CATEGORY_URL", "").strip()
OUT_FILE         = os.getenv("OUT_FILE", "docs/copyline.yml").strip()
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "700"))
PAGE_TIMEOUT_S   = int(os.getenv("PAGE_TIMEOUT_S", "30"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; copyline-scraper/1.1; +https://example.org)",
}
# ---------------------------------------------------

def make_soup(html_text: str) -> BeautifulSoup:
    return BeautifulSoup(html_text, "html.parser")

def get(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=PAGE_TIMEOUT_S)
        if r.status_code == 200 and len(r.content) >= MIN_BYTES:
            return r.text
        return None
    finally:
        time.sleep(REQUEST_DELAY_MS / 1000.0)

def absolutize(url_or_path: str, base: str = BASE_URL) -> str:
    return urljoin(base, url_or_path)

# -------- изображения: принудительно full_ ----------
def force_full_image(url: str) -> str:
    """
    Превращаем .../img_products/NAME.jpg → .../img_products/full_NAME.jpg
    И thumb_NAME.jpg → full_NAME.jpg. Остальное не трогаем.
    """
    if not url:
        return url
    parts = urlsplit(url)
    # применяем только к каталогу картинок copyline jshopping
    if "/components/com_jshopping/files/img_products/" not in parts.path:
        return url
    dirname, filename = posixpath.split(parts.path)
    if not filename:
        return url
    if filename.startswith("full_"):
        new_filename = filename
    elif filename.startswith("thumb_"):
        new_filename = "full_" + filename[len("thumb_"):]
    else:
        new_filename = "full_" + filename
    new_path = posixpath.join(dirname, new_filename)
    return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))
# ----------------------------------------------------

PRICE_CLEAN_RE = re.compile(r"[^\d,\.]+", re.ASCII)
COMMA_RE       = re.compile(r",")

def parse_price(raw: str) -> Optional[int]:
    if not raw:
        return None
    s = PRICE_CLEAN_RE.sub("", raw).strip()
    if not s:
        return None
    s = COMMA_RE.sub(".", s)
    try:
        return int(round(float(s)))
    except ValueError:
        return None

def extract_sku_from_product(soup: BeautifulSoup) -> Optional[str]:
    text = soup.get_text(" ", strip=True)
    m = re.search(r"Артикул\s*[:\-]?\s*([A-Za-z0-9\-_]+)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    for tr in soup.select("table tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.select("td,th")]
        if any("артикул" in c.lower() for c in cells):
            for c in cells:
                if "артикул" not in c.lower():
                    mm = re.search(r"([A-Za-z0-9\-_]+)", c)
                    if mm:
                        return mm.group(1)
    return None

def extract_description_from_product(soup: BeautifulSoup) -> str:
    candidates = [
        ".jshop_prod_description",
        ".prod_description",
        "[itemprop='description']",
        "#description",
        ".product_description",
        ".tab-content #description",
        ".descr",
    ]
    for sel in candidates:
        node = soup.select_one(sel)
        if node:
            txt = node.get_text(" ", strip=True)
            if len(txt) >= 10:
                return txt
    meta = soup.select_one("meta[name='description']")
    if meta and meta.get("content"):
        txt = meta["content"].strip()
        if len(txt) >= 10:
            return txt
    return ""

def extract_product_cards(html_cat: str, base_url: str) -> List[Dict]:
    soup = make_soup(html_cat)
    list_container = soup.select_one(".jshop_list_product") or soup.select_one(".jshop_products")
    if not list_container:
        list_container = soup

    cards = []
    for card in list_container.select(".product, .jshop_product"):
        a = card.select_one(".name a[href], a[href*='/goods/']")
        if not a:
            continue
        url = absolutize(a.get("href", ""), base_url)
        name = a.get_text(" ", strip=True)

        price_node = None
        for node in card.select("[class*='price']"):
            if node.get_text(strip=True):
                price_node = node
                break
        price_str = price_node.get_text(" ", strip=True) if price_node else ""

        img = None
        img_node = card.select_one("img[src]")
        if img_node and img_node.get("src"):
            img = force_full_image(absolutize(img_node["src"], base_url))

        cards.append({"name": name, "url": url, "img": img, "price_str": price_str})

    seen = set()
    clean = []
    for c in cards:
        u = c["url"]
        if "/goods/" not in u or not u.endswith(".html"):
            continue
        if u in seen:
            continue
        seen.add(u)
        clean.append(c)
    return clean

def enrich_with_product_page(card: Dict) -> Dict:
    html_prod = get(card["url"])
    if not html_prod:
        card["vendorCode"] = None
        card["description"] = ""
        return card

    soup = make_soup(html_prod)
    card["vendorCode"]  = extract_sku_from_product(soup)
    card["description"] = extract_description_from_product(soup)

    if not card.get("img"):
        main_img = soup.select_one("img#main_image, img#main_image_*, img[itemprop='image'], .image img, .productfull img[src]")
        if main_img and main_img.get("src"):
            card["img"] = force_full_image(absolutize(main_img["src"]))

    price_node = soup.select_one(".prod_price, .price, [class*='price']")
    if price_node:
        p = parse_price(price_node.get_text(" ", strip=True))
        if p:
            card["price"] = p

    return card

def cdata(text: str) -> str:
    if not text:
        return ""
    safe = text.replace("]]>", "]]&gt;")
    return f"<![CDATA[{safe}]]>"

def xml(text: str) -> str:
    return html.escape(text or "", quote=True)

def write_yml(category_name: str, items: List[Dict], out_path: str, encoding: str = "windows-1251") -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding=encoding, newline="") as f:
        f.write(f"<?xml version='1.0' encoding='{encoding}'?>\n")
        f.write("<yml_catalog><shop>")
        f.write("<name>copyline</name>")
        f.write("<currencies><currency id=\"KZT\" rate=\"1\" /></currencies>")
        f.write("<categories>")
        f.write("<category id=\"9300000\">Copyline</category>")
        f.write(f"<category id=\"9402524\" parentId=\"9300000\">{xml(category_name)}</category>")
        f.write("</categories>")
        f.write("<offers>")

        for it in items:
            path = urlparse(it["url"]).path.strip("/").replace("/", "-").replace(".html", "")
            h = hex(abs(hash(it["url"])) & 0xFFFFFFFF)[2:]
            offer_id = f"copyline:{path}:{h}"

            name = it.get("name") or ""
            price = it.get("price")
            if price is None:
                price = parse_price(it.get("price_str", "")) or 0

            f.write(f"<offer id=\"{xml(offer_id)}\" available=\"true\" in_stock=\"true\">")
            f.write(f"<name>{xml(name)}</name>")
            f.write(f"<price>{price}</price>")
            f.write("<currencyId>KZT</currencyId>")
            f.write("<categoryId>9402524</categoryId>")
            f.write(f"<url>{xml(it['url'])}</url>")
            if it.get("img"):
                f.write(f"<picture>{xml(it['img'])}</picture>")
            if it.get("vendorCode"):
                f.write(f"<vendorCode>{xml(it['vendorCode'])}</vendorCode>")
            desc = it.get("description", "").strip()
            if desc:
                f.write(f"<description>{cdata(desc)}</description>")
            f.write("<quantity_in_stock>1</quantity_in_stock>")
            f.write("<stock_quantity>1</stock_quantity>")
            f.write("<quantity>1</quantity>")
            f.write("</offer>")

        f.write("</offers></shop></yml_catalog>")

def main():
    if not CATEGORY_URL:
        raise SystemExit("ERROR: CATEGORY_URL не задан")
    html_cat = get(CATEGORY_URL)
    if not html_cat:
        raise SystemExit(f"ERROR: не удалось загрузить CATEGORY_URL: {CATEGORY_URL}")

    cards = extract_product_cards(html_cat, BASE_URL)
    if not cards:
        raise SystemExit("ERROR: не удалось найти товары на странице категории")

    enriched: List[Dict] = []
    for idx, c in enumerate(cards, start=1):
        try:
            c2 = enrich_with_product_page(c)
            enriched.append(c2)
            print(f"[ok] {idx:03d}/{len(cards)} | SKU={c2.get('vendorCode') or '-'} | {c2['url']}")
        except Exception as e:
            print(f"[err] {idx:03d}/{len(cards)} | {c['url']} | {e}")

    soup_cat = make_soup(html_cat)
    cat_name = ""
    h1 = soup_cat.select_one("h1, .jshop h1, .content h1")
    if h1:
        cat_name = h1.get_text(" ", strip=True)
    if not cat_name:
        cat_name = "Категория"

    write_yml(cat_name, enriched, OUT_FILE, OUTPUT_ENCODING)
    print(f"[done] Сохранено: {OUT_FILE} (товаров: {len(enriched)})")

if __name__ == "__main__":
    main()
