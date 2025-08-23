# scripts/enrich_copyline_photos.py
from __future__ import annotations
import os, re, sys, time, json, urllib.parse
from datetime import datetime, timezone
from typing import Optional
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import ElementTree, Element

BASE = "https://copyline.kz"
UA_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# -------- ENV --------
YML_PATH           = os.getenv("YML_PATH", "docs/copyline.yml")
OUT_ENCODING       = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
PHOTO_INDEX_PATH   = os.getenv("PHOTO_INDEX_PATH", "docs/copyline_photo_index.json")
PHOTO_OVERRIDES    = os.getenv("PHOTO_OVERRIDES", "docs/copyline_photo_overrides.json")
PHOTO_BLACKLIST    = os.getenv("PHOTO_BLACKLIST", "docs/copyline_photo_blacklist.json")
PHOTO_FETCH_LIMIT  = int(os.getenv("PHOTO_FETCH_LIMIT", "80"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "600"))
BACKOFF_MAX_MS     = int(os.getenv("BACKOFF_MAX_MS", "8000"))
FLUSH_EVERY_N      = int(os.getenv("FLUSH_EVERY_N", "0"))  # 0 = писать один раз в конце

SEARCH_ENDPOINTS = [
    "/index.php?option=com_jshopping&controller=search&task=result&search={q}",
    "/index.php?option=com_search&searchword={q}",
]

def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

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

def absolutize(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"): return url
    return urllib.parse.urljoin(BASE, url)

def sleep_ms(ms: int):
    time.sleep(max(ms,0)/1000.0)

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=UA_HEADERS, timeout=60)
    if r.status_code in (403, 429):
        raise RuntimeError(f"rate_limited:{r.status_code}")
    r.raise_for_status()
    return r

# -------- XML --------
def read_xml(path: str) -> ElementTree:
    return ElementTree(file=path)

def write_xml(tree: ElementTree, path: str, enc: str):
    tree.write(path, encoding=enc, xml_declaration=True)

def iter_offers(root: Element):
    shop = root.find("shop")
    if shop is None: return
    offers = shop.find("offers")
    if offers is None: return
    for o in offers.findall("offer"):
        yield o

def offer_has_picture(offer: Element) -> bool:
    return offer.find("picture") is not None and (offer.findtext("picture") or "").strip() != ""

def get_article(offer: Element) -> Optional[str]:
    vc = offer.findtext("vendorCode")
    return (vc or "").strip() or None

def set_offer_picture(offer: Element, url: str):
    pic = offer.find("picture")
    if pic is None:
        pic = Element("picture")
        offer.append(pic)
    pic.text = url

# -------- поиск карточки --------
def search_product_page_by_article(article: str) -> Optional[str]:
    q = urllib.parse.quote_plus(article)
    for tmpl in SEARCH_ENDPOINTS:
        url = absolutize(tmpl.format(q=q))
        try:
            r = fetch(url)
        except RuntimeError:
            raise
        except Exception:
            continue

        soup = BeautifulSoup(r.text, "lxml")

        links=set()
        for a in soup.select('a[href*="/product/"], a[href*="/goods/"]'):
            href = a.get("href") or ""
            if href.endswith(".html"):
                links.add(absolutize(href))
        if not links:
            for a in soup.find_all("a", href=True):
                href=a["href"]
                if href.endswith(".html") and ("/goods/" in href or "/product/" in href):
                    links.add(absolutize(href))

        for link in list(links)[:5]:
            sleep_ms(REQUEST_DELAY_MS)
            try:
                pr = fetch(link)
            except RuntimeError:
                raise
            except Exception:
                continue
            psoup = BeautifulSoup(pr.text, "lxml")
            h1 = psoup.find("h1")
            h1txt = norm(h1.get_text()) if h1 else ""
            if article.upper() in h1txt.upper():
                return link
    return None

def extract_first_itemprop_image(page_html: str) -> Optional[str]:
    soup = BeautifulSoup(page_html, "lxml")
    img = soup.find("img", {"itemprop":"image"})
    if img and img.get("src"):
        return absolutize(img.get("src"))
    return None

# -------- main --------
def main():
    index = load_json(PHOTO_INDEX_PATH)
    overrides = load_json(PHOTO_OVERRIDES) or {}
    blacklist = set((load_json(PHOTO_BLACKLIST) or {}).keys())

    try:
        tree = read_xml(YML_PATH)
    except Exception as e:
        print(f"ERROR: cannot read YML: {e}", file=sys.stderr)
        sys.exit(1)
    root = tree.getroot()

    todo = []
    total = 0
    for offer in iter_offers(root):
        total += 1
        if offer_has_picture(offer):
            continue
        art = get_article(offer)
        if not art:
            continue
        todo.append((offer, art))

    todo.sort(key=lambda x: x[1])

    updated = 0
    skipped = 0
    processed = 0
    backoff_ms = 0

    def flush_progress():
        # сохраняем XML и индекс, если что-то менялось
        write_xml(tree, YML_PATH, OUT_ENCODING)
        os.makedirs(os.path.dirname(PHOTO_INDEX_PATH) or ".", exist_ok=True)
        save_json(PHOTO_INDEX_PATH, index)

    for offer, article in todo:
        if processed >= PHOTO_FETCH_LIMIT:
            break
        processed += 1

        # override
        if article in overrides:
            url = overrides[article]
            if url and url not in blacklist:
                set_offer_picture(offer, url)
                index[article] = {"img_url": url, "page_url": "", "locked": True, "checked_at": now_iso(), "source": "override"}
                updated += 1
                if FLUSH_EVERY_N and updated % FLUSH_EVERY_N == 0:
                    flush_progress()
                continue

        # cache
        info = index.get(article)
        if info and info.get("locked") and info.get("img_url") and info["img_url"] not in blacklist:
            set_offer_picture(offer, info["img_url"])
            updated += 1
            if FLUSH_EVERY_N and updated % FLUSH_EVERY_N == 0:
                flush_progress()
            continue

        # search
        try:
            if backoff_ms:
                sleep_ms(backoff_ms)
            page_url = search_product_page_by_article(article)
            backoff_ms = 0
        except RuntimeError as e:
            print(f"RATE_LIMIT: {e}", file=sys.stderr)
            break

        if not page_url:
            skipped += 1
            continue

        sleep_ms(REQUEST_DELAY_MS)
        try:
            pr = fetch(page_url)
        except RuntimeError as e:
            print(f"RATE_LIMIT fetching page: {page_url}", file=sys.stderr)
            break
        except Exception:
            skipped += 1
            continue

        img_url = extract_first_itemprop_image(pr.text)
        if not img_url or img_url in blacklist:
            skipped += 1
            continue

        set_offer_picture(offer, img_url)
        index[article] = {
            "img_url": img_url,
            "page_url": page_url,
            "locked": True,
            "checked_at": now_iso(),
            "source": "itemprop-image",
        }
        updated += 1

        if FLUSH_EVERY_N and updated % FLUSH_EVERY_N == 0:
            flush_progress()

        sleep_ms(REQUEST_DELAY_MS)

    if updated > 0 and (not FLUSH_EVERY_N or updated % FLUSH_EVERY_N != 0):
        flush_progress()

    print(f"[PHOTO A] total_offers={total} | candidates={len(todo)} | updated={updated} | skipped={skipped} | processed={processed}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
