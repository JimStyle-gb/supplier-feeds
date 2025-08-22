# scripts/enrich_copyline_photos.py
from __future__ import annotations
import os, re, io, sys, time, json, urllib.parse
from datetime import datetime, timezone
from typing import Optional, Tuple
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import ElementTree, Element

BASE = "https://copyline.kz"
UA_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ---- настройки через ENV ----
YML_PATH           = os.getenv("YML_PATH", "docs/copyline.yml")
OUT_ENCODING       = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
PHOTO_INDEX_PATH   = os.getenv("PHOTO_INDEX_PATH", "docs/copyline_photo_index.json")
PHOTO_OVERRIDES    = os.getenv("PHOTO_OVERRIDES", "docs/copyline_photo_overrides.json")
PHOTO_BLACKLIST    = os.getenv("PHOTO_BLACKLIST", "docs/copyline_photo_blacklist.json")
PHOTO_FETCH_LIMIT  = int(os.getenv("PHOTO_FETCH_LIMIT", "80"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "600"))
BACKOFF_MAX_MS     = int(os.getenv("BACKOFF_MAX_MS", "8000"))

SEARCH_ENDPOINTS = [
    # основной поиск JoomShopping
    "/index.php?option=com_jshopping&controller=search&task=result&search={q}",
    # запасной общий поиск Joomla
    "/index.php?option=com_search&searchword={q}",
]

# ---- утилиты ----
def load_json(path: str) -> dict:
    try:
        with open(path,"r",encoding="utf-8") as f: return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def save_json(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp,"w",encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def absolutize(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"): return url
    return urllib.parse.urljoin(BASE, url)

def sleep_ms(ms: int):
    time.sleep(max(ms,0)/1000.0)

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=UA_HEADERS, timeout=60)
    # мягкая анти-DDoS: если нас ограничили — сделаем backoff и прервём текущую пачку
    if r.status_code in (403, 429):
        raise RuntimeError(f"rate_limited:{r.status_code}")
    r.raise_for_status()
    return r

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---- логика работы с YML ----
def read_xml(path: str) -> ElementTree:
    return ElementTree(file=path)

def write_xml(tree: ElementTree, path: str, enc: str):
    # ElementTree.write не умеет windows-1251 с pretty-print — но нам это и не нужно
    tree.write(path, encoding=enc, xml_declaration=True)

def iter_offers(root: Element):
    shop = root.find("shop")
    if shop is None: return
    offers = shop.find("offers")
    if offers is None: return
    for o in offers.findall("offer"):
        yield o

def get_vendor_code_raw(offer: Element) -> Optional[str]:
    """
    Получаем исходный артикул из оффера:
    - если есть vendorCode:
        - если он вида 'c12345' -> вернём '12345'
        - иначе вернём как есть (CF283A и т.п.)
    - если vendorCode нет -> None
    """
    vc = offer.findtext("vendorCode")
    if not vc: return None
    s = vc.strip()
    if re.fullmatch(r"c\d+", s):   # наш префикс для числовых
        return s[1:]
    return s

def offer_has_picture(offer: Element) -> bool:
    return offer.find("picture") is not None and (offer.findtext("picture") or "").strip() != ""

def set_offer_picture(offer: Element, url: str):
    pic = offer.find("picture")
    if pic is None:
        pic = Element("picture")
        offer.append(pic)
    pic.text = url

# ---- поиск карточки и фото по артикулу (Вариант A: минимализм) ----
def search_product_page_by_article(article: str) -> Optional[str]:
    q = urllib.parse.quote_plus(article)
    for tmpl in SEARCH_ENDPOINTS:
        url = absolutize(tmpl.format(q=q))
        try:
            r = fetch(url)
        except RuntimeError as e:
            if str(e).startswith("rate_limited"):
                raise
            return None
        except Exception:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        # собрать ссылки на возможные карточки
        links=set()
        for a in soup.select('a[href*="/product/"], a[href*="/goods/"]'):
            href = a.get("href") or ""
            if href.endswith(".html"):
                links.add(absolutize(href))
        # если ничего не нашли — возможна другая верстка
        if not links:
            for a in soup.find_all("a", href=True):
                href=a["href"]
                if href.endswith(".html") and ("/goods/" in href or "/product/" in href):
                    links.add(absolutize(href))
        # пройти по найденным ссылкам и проверить H1 на вхождение артикула
        for link in list(links)[:5]:
            sleep_ms(REQUEST_DELAY_MS)
            try:
                pr = fetch(link)
            except RuntimeError as e:
                if str(e).startswith("rate_limited"):
                    raise
                continue
            except Exception:
                continue
            psoup = BeautifulSoup(pr.text, "lxml")
            h1 = psoup.find("h1")
            h1txt = norm(h1.get_text()) if h1 else ""
            if article.upper() not in h1txt.upper():
                continue
            return link
    return None

def extract_first_itemprop_image(page_html: str) -> Optional[str]:
    soup = BeautifulSoup(page_html, "lxml")
    img = soup.find("img", {"itemprop":"image"})
    if img and img.get("src"):
        return absolutize(img.get("src"))
    return None

# ---- основной процесс ----
def main():
    # кэши метаданных (без картинок)
    index = load_json(PHOTO_INDEX_PATH)
    overrides = load_json(PHOTO_OVERRIDES) or {}
    blacklist = set((load_json(PHOTO_BLACKLIST) or {}).keys())

    # читаем YML
    try:
        tree = read_xml(YML_PATH)
    except Exception as e:
        print(f"ERROR: cannot read YML: {e}", file=sys.stderr)
        sys.exit(1)
    root = tree.getroot()

    total = 0
    updated = 0
    skipped = 0
    processed = 0

    # соберём список офферов без картинок
    offers_to_process = []
    for offer in iter_offers(root):
        total += 1
        if offer_has_picture(offer):
            continue
        raw = get_vendor_code_raw(offer)
        if not raw:
            continue
        offers_to_process.append((offer, raw))

    # стабильный порядок, чтобы каждый ран продвигался
    offers_to_process.sort(key=lambda x: (x[1],))

    backoff_ms = 0
    for offer, raw_article in offers_to_process:
        if processed >= PHOTO_FETCH_LIMIT:
            break

        processed += 1

        # 1) ручной override
        if raw_article in overrides:
            url = overrides[raw_article]
            if url and url not in blacklist:
                set_offer_picture(offer, url)
                index[raw_article] = {
                    "img_url": url,
                    "page_url": index.get(raw_article,{}).get("page_url",""),
                    "locked": True,
                    "checked_at": now_iso(),
                    "source": "override",
                }
                updated += 1
                continue

        # 2) уже найдено ранее и зафиксировано
        info = index.get(raw_article)
        if info and info.get("locked") and info.get("img_url") and info["img_url"] not in blacklist:
            set_offer_picture(offer, info["img_url"])
            updated += 1
            continue

        # 3) поиск карточки и извлечение itemprop=image
        try:
            if backoff_ms:
                sleep_ms(backoff_ms)
            page_url = search_product_page_by_article(raw_article)
            backoff_ms = 0
        except RuntimeError as e:
            # rate limited — увеличим backoff и прервём пачку
            print(f"RATE_LIMIT: {e}", file=sys.stderr)
            backoff_ms = min(max(REQUEST_DELAY_MS*4, 2000), BACKOFF_MAX_MS)
            break

        if not page_url:
            skipped += 1
            continue

        sleep_ms(REQUEST_DELAY_MS)
        try:
            pr = fetch(page_url)
        except RuntimeError as e:
            if str(e).startswith("rate_limited"):
                print(f"RATE_LIMIT fetching page: {page_url}", file=sys.stderr)
                break
            skipped += 1
            continue
        except Exception:
            skipped += 1
            continue

        img_url = extract_first_itemprop_image(pr.text)
        if not img_url or img_url in blacklist:
            skipped += 1
            continue

        # 4) записываем картинку в оффер и в кэш
        set_offer_picture(offer, img_url)
        index[raw_article] = {
            "img_url": img_url,
            "page_url": page_url,
            "locked": True,
            "checked_at": now_iso(),
            "source": "itemprop-image",
        }
        updated += 1

        # щадящая пауза
        sleep_ms(REQUEST_DELAY_MS)

    # если что-то добавили — сохранить XML и кэш
    if updated > 0:
        write_xml(tree, YML_PATH, OUT_ENCODING)
        os.makedirs(os.path.dirname(PHOTO_INDEX_PATH) or ".", exist_ok=True)
        save_json(PHOTO_INDEX_PATH, index)

    print(f"[PHOTO] total_offers={total} | candidates={len(offers_to_process)} | updated={updated} | skipped={skipped} | processed={processed}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
