# scripts/test_copyline_photo_lookup.py
# Тест: ищем фото по списку артикулов через поиск на copyline.kz и вытаскиваем <img itemprop="image" ... src="...">

import time
import re
import sys
import html
import urllib.parse
import requests
from bs4 import BeautifulSoup

BASE = "https://copyline.kz"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

ARTICLES = [
    "105140",
    "103826",
    "103824",
    "104151",
    "104152",
    "104153",
    "104154",
]

def norm(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def abs_url(u: str) -> str:
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return urllib.parse.urljoin(BASE, u)

def fetch(url: str) -> str:
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def find_product_link_in_search(html_text: str, code: str) -> str | None:
    soup = BeautifulSoup(html_text, "lxml")

    # Правило: берем первую ссылку на карточку товара (/goods/...html или /product/...html),
    # у которой либо в href, либо в тексте/атрибутах встречается код.
    cand = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".html" in href and ("/goods/" in href or "/product/" in href):
            txt = " ".join([a.get_text(" "), a.get("title",""), a.get("alt","")])
            blob = (href + " " + txt).upper()
            if code.upper() in blob:
                cand.append(abs_url(href))
    if cand:
        return cand[0]

    # Фолбэк: если ничего не нашли по коду — возьмём первую карточку в выдаче.
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".html" in href and ("/goods/" in href or "/product/" in href):
            return abs_url(href)
    return None

def extract_main_image(product_html: str) -> str | None:
    soup = BeautifulSoup(product_html, "lxml")
    # Строго: только главный тег <img itemprop="image" ... src="...">
    img = soup.find("img", attrs={"itemprop": "image"})
    if img and img.get("src"):
        return abs_url(img["src"])
    return None

def search_endpoints_for(code: str) -> list[str]:
    q = urllib.parse.quote_plus(code)
    return [
        f"{BASE}/search?searchword={q}",  # com_search
        f"{BASE}/?option=com_search&searchword={q}",
        # Варианты jshopping-поиска:
        f"{BASE}/index.php?option=com_jshopping&controller=search&task=result&setsearchdata=1&search=name&category_id=0&text={q}",
        f"{BASE}/index.php?option=com_jshopping&controller=search&task=result&search=name&category_id=0&text={q}",
    ]

def find_product_page_by_code(code: str) -> str | None:
    # Пробуем несколько эндпоинтов поиска
    for url in search_endpoints_for(code):
        try:
            html_text = fetch(url)
        except Exception:
            continue
        link = find_product_link_in_search(html_text, code)
        if link:
            return link
        time.sleep(0.4)
    return None

def main():
    print("article\tproduct_url\timage_url")
    for code in ARTICLES:
        try:
            product_url = find_product_page_by_code(code)
            if not product_url:
                print(f"{code}\t\t", flush=True)
                time.sleep(0.6)
                continue

            html_text = fetch(product_url)
            img = extract_main_image(html_text) or ""
            print(f"{code}\t{product_url}\t{img}", flush=True)
            time.sleep(0.6)  # бережно к сайту
        except Exception as e:
            print(f"{code}\t\t\tERROR: {e}", flush=True)
            time.sleep(0.8)

if __name__ == "__main__":
    SESSION = requests.Session()
    main()
