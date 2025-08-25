# scripts/test_copyline_photo_lookup.py
from __future__ import annotations
import os, re, io, sys, urllib.parse
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, ElementTree

BASE_URL = os.getenv("COPYLINE_BASE", "https://copyline.kz")
OUTPUT_YML = os.getenv("OUTPUT_YML", "docs/copyline_photo_test.yml")
INPUT_YML = os.getenv("INPUT_YML", "docs/copyline.yml")  # опционально: взять name/price/category из основного YML
ENC = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def absolutize(url: str) -> str:
    if not url:
        return url
    return url if url.startswith("http") else urllib.parse.urljoin(BASE_URL, url)

def fetch_html(url: str) -> str:
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return r.text

def search_product_url(query: str) -> str | None:
    qs = urllib.parse.quote_plus(query)
    candidates = [
        f"{BASE_URL}/?search={qs}",
        f"{BASE_URL}/?searchword={qs}",
        f"{BASE_URL}/search?searchword={qs}",
    ]
    for su in candidates:
        try:
            html = fetch_html(su)
        except Exception:
            continue
        soup = BeautifulSoup(html, "lxml")
        # Явные карточки
        for a in soup.select('a[href*="/goods/"], a[href*="/product/"]'):
            href = a.get("href") or ""
            if href.endswith(".html"):
                return absolutize(href)
        # Запасной вариант
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ("/goods/" in href or "/product/" in href) and href.endswith(".html"):
                return absolutize(href)
    return None

def extract_name_and_main_image(product_url: str) -> tuple[str | None, str | None]:
    try:
        html = fetch_html(product_url)
    except Exception:
        return None, None
    soup = BeautifulSoup(html, "lxml")
    title = soup.find("h1")
    name = norm(title.get_text()) if title else None
    img = soup.find("img", {"itemprop": "image"})
    pic = absolutize(img.get("src")) if img and img.get("src") else None
    return name, pic

def load_input_yml(path: str) -> dict[str, dict]:
    """Минимальный парс исходного YML: vendorCode -> {name, price, categoryId, picture}."""
    if not path or not os.path.exists(path):
        return {}
    import xml.etree.ElementTree as ET
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        out: dict[str, dict] = {}
        for offer in root.findall(".//offer"):
            vc = norm(offer.findtext("vendorCode"))
            if not vc:
                continue
            out[vc] = {
                "name": offer.findtext("name") or "",
                "price": offer.findtext("price") or "",
                "categoryId": offer.findtext("categoryId") or "9300000",
                "picture": offer.findtext("picture") or "",
            }
        return out
    except Exception:
        return {}

def build_yml(records: dict[str, dict], base_map: dict[str, dict]) -> bytes:
    root = Element("yml_catalog"); shop = SubElement(root, "shop")
    SubElement(shop, "name").text = "copyline-photo-test"
    curr = SubElement(shop, "currencies"); SubElement(curr, "currency", {"id": "KZT", "rate": "1"})
    cats = SubElement(shop, "categories"); SubElement(cats, "category", {"id": "9300000"}).text = "Copyline"
    offers = SubElement(shop, "offers")

    for art, info in records.items():
        base = base_map.get(art, {})
        name = info.get("name") or base.get("name") or art
        price = base.get("price")
        cat = base.get("categoryId") or "9300000"
        pic = info.get("picture") or ""

        o = SubElement(offers, "offer", {"id": f"copyline:{art}", "available": "true", "in_stock": "true"})
        SubElement(o, "name").text = name
        if price:
            SubElement(o, "price").text = str(price)
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cat
        SubElement(o, "vendorCode").text = art
        if pic:
            SubElement(o, "picture").text = pic
        for tag in ("quantity_in_stock", "stock_quantity", "quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

def main():
    arts_env = os.getenv("ARTICLES", "").strip()
    if not arts_env:
        print("ERROR: set ARTICLES env var with codes (comma/space/newline separated).", file=sys.stderr)
        sys.exit(1)

    # парсим список артикулов
    articles = [a for a in re.split(r"[,\s]+", arts_env) if a]
    base_map = load_input_yml(INPUT_YML)

    results: dict[str, dict] = {}
    for art in articles:
        url = search_product_url(art)
        if not url:
            results[art] = {"name": base_map.get(art, {}).get("name") or "", "picture": ""}
            print(f"{art}\tNOT_FOUND\t", flush=True)
            continue
        name, pic = extract_name_and_main_image(url)
        results[art] = {"name": name or base_map.get(art, {}).get("name") or "", "picture": pic or ""}
        print(f"{art}\t{url}\t{pic or ''}", flush=True)

    os.makedirs(os.path.dirname(OUTPUT_YML), exist_ok=True)
    yml_bytes = build_yml(results, base_map)
    with open(OUTPUT_YML, "wb") as f:
        f.write(yml_bytes)
    print(f"Wrote YML: {OUTPUT_YML} (records: {len(results)})")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
