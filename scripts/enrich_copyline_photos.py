from __future__ import annotations
import os, re, sys, time, json, urllib.parse
from typing import Optional, Tuple
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

BASE = "https://copyline.kz"
UA_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

YML_PATH          = os.getenv("YML_PATH", "docs/copyline.yml")
ENC               = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
PHOTO_INDEX_PATH  = os.getenv("PHOTO_INDEX_PATH", "docs/copyline_photo_index.json")
PHOTO_OVERRIDES   = os.getenv("PHOTO_OVERRIDES", "docs/copyline_photo_overrides.json")
PHOTO_BLACKLIST   = os.getenv("PHOTO_BLACKLIST", "docs/copyline_photo_blacklist.json")
PHOTO_FETCH_LIMIT = int(os.getenv("PHOTO_FETCH_LIMIT", "80"))
REQ_DELAY_MS      = int(os.getenv("REQUEST_DELAY_MS", "600"))
BACKOFF_MAX_MS    = int(os.getenv("BACKOFF_MAX_MS", "12000"))
FLUSH_EVERY_N     = int(os.getenv("FLUSH_EVERY_N", "20"))

SEARCH_TEMPLATES = [
    # Joomla/JoomShopping варианты поиска — пробуем по очереди
    "/search?searchword={q}",
    "/?searchword={q}&option=com_search&searchphrase=all&ordering=newest",
    "/index.php?option=com_jshopping&controller=search&task=result&search={q}",
    "/index.php?option=com_jshopping&controller=search&task=result&sword={q}",
]

def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "")).strip()

def absolutize(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return urllib.parse.urljoin(BASE, url)

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=UA_HEADERS, timeout=40)
    r.raise_for_status()
    return r

def sleep_ms(ms: int):
    time.sleep(ms/1000.0)

def load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def save_json(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def parse_search_results(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    hrefs = set()
    # типичные ссылки на карточки
    for a in soup.select('a[href*="/product/"], a[href*="/goods/"]'):
        href = a.get("href") or ""
        if href.endswith(".html"):
            hrefs.add(absolutize(href))
    # подстраховка: любые .html из goods/product
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".html") and ("/goods/" in href or "/product/" in href):
            hrefs.add(absolutize(href))
    return list(hrefs)

def extract_main_image_from_product(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    img = soup.select_one('img[itemprop="image"]')
    if img and img.get("src"):
        return absolutize(img["src"])
    # Если элемент есть, но src пуст — ничего не придумываем
    return None

def search_product_url_by_article(article: str) -> Optional[str]:
    q = urllib.parse.quote(article)
    for tpl in SEARCH_TEMPLATES:
        url = absolutize(tpl.format(q=q))
        try:
            r = fetch(url)
        except Exception:
            continue
        # если это сразу карточка товара (редиректом попали)
        if ("/goods/" in r.url or "/product/" in r.url) and r.url.endswith(".html"):
            return r.url
        # иначе это страница результатов
        links = parse_search_results(r.text)
        if links:
            return links[0]
        sleep_ms(REQ_DELAY_MS)
    return None

def find_image_for_article(article: str) -> Optional[str]:
    """1) Ищем карточку по артикулу; 2) Открываем; 3) Берём src из img[itemprop=image]."""
    url = search_product_url_by_article(article)
    if not url:
        return None
    try:
        r = fetch(url)
    except Exception:
        return None
    return extract_main_image_from_product(r.text)

def load_yml_offers(path: str):
    tree = ET.parse(path)
    root = tree.getroot()
    shop = root.find("shop")
    offers = shop.find("offers") if shop is not None else None
    if offers is None:
        raise RuntimeError("Не найден <offers> в YML")
    return tree, root, offers

def get_text(elem, tag: str) -> str:
    x = elem.find(tag)
    return norm(x.text if x is not None else "")

def set_picture(elem, url: str):
    pic = elem.find("picture")
    if pic is None:
        pic = ET.SubElement(elem, "picture")
    pic.text = url

def main():
    # 1) Загружаем YML и служебные JSON
    tree, root, offers = load_yml_offers(YML_PATH)
    overrides = load_json(PHOTO_OVERRIDES)     # артикул/имя -> фикс-URL
    blacklist = set(load_json(PHOTO_BLACKLIST).get("block", []))  # артикулы/имена в стопе
    index = load_json(PHOTO_INDEX_PATH)        # кэш: артикул/имя -> URL
    if not isinstance(index, dict): index = {}

    total = 0
    updated = 0
    fetched = 0
    backoff = REQ_DELAY_MS

    for offer in offers.findall("offer"):
        total += 1
        # уже есть картинка — пропускаем
        if offer.find("picture") is not None:
            continue

        article = get_text(offer, "vendorCode")   # ОРИГИНАЛЬНЫЙ артикул из прайса
        name    = get_text(offer, "name")

        key_candidates = [article, name]
        key_candidates = [k for k in key_candidates if k]

        # 0) стоп-лист
        if any(k in blacklist for k in key_candidates):
            continue

        img_url = None

        # 1) ручные правки в приоритете
        for k in key_candidates:
            if k in overrides:
                img_url = overrides[k]; break

        # 2) кэш
        if img_url is None:
            for k in key_candidates:
                if k in index:
                    img_url = index[k]; break

        # 3) онлайн-поиск по артикулу через поиск на сайте
        if img_url is None and article:
            img_url = find_image_for_article(article)
            if img_url:
                index[article] = img_url
                fetched += 1
                # плавный троттлинг
                sleep_ms(backoff)
                backoff = min(backoff + 200, BACKOFF_MAX_MS)

        if img_url:
            set_picture(offer, img_url)
            updated += 1

        # периодическая запись прогресса
        if fetched and (fetched % FLUSH_EVERY_N == 0):
            tree.write(YML_PATH, encoding=ENC, xml_declaration=True)
            save_json(PHOTO_INDEX_PATH, index)

        # защитный лимит за прогон
        if PHOTO_FETCH_LIMIT and fetched >= PHOTO_FETCH_LIMIT:
            break

    # финальная запись
    tree.write(YML_PATH, encoding=ENC, xml_declaration=True)
    save_json(PHOTO_INDEX_PATH, index)

    print(f"[OK] offers={total} | updated={updated} | newly_fetched={fetched} | cache_now={len(index)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
