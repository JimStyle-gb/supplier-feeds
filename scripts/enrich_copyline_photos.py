from __future__ import annotations
import os, re, io, sys, time, json, urllib.parse
from typing import Optional, Dict, Any
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import ElementTree

BASE = "https://copyline.kz"
UA_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

YML_PATH           = os.getenv("YML_PATH", "docs/copyline.yml")
ENC                = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
PHOTO_INDEX_PATH   = os.getenv("PHOTO_INDEX_PATH", "docs/copyline_photo_index.json")
PHOTO_OVERRIDES    = os.getenv("PHOTO_OVERRIDES", "docs/copyline_photo_overrides.json")
PHOTO_BLACKLIST    = os.getenv("PHOTO_BLACKLIST", "docs/copyline_photo_blacklist.json")
FETCH_LIMIT        = int(os.getenv("PHOTO_FETCH_LIMIT", "200"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "600"))
BACKOFF_MAX_MS     = int(os.getenv("BACKOFF_MAX_MS", "12000"))
FLUSH_EVERY_N      = int(os.getenv("FLUSH_EVERY_N", "20"))

# ---------- utils ----------
def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def absolutize(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"): return url
    return urllib.parse.urljoin(BASE, url)

def fetch(url: str) -> str:
    """GET с простым экспоненциальным бэкоффом на 429/503."""
    delay = REQUEST_DELAY_MS / 1000.0
    while True:
        r = requests.get(url, headers=UA_HEADERS, timeout=45)
        if r.status_code in (429, 503):
            time.sleep(min(BACKOFF_MAX_MS/1000.0, delay))
            delay = min(delay * 2, BACKOFF_MAX_MS/1000.0)
            continue
        r.raise_for_status()
        return r.text

def looks_like_placeholder(url: str) -> bool:
    u = url.lower()
    return ("noimage" in u) or u.endswith("/placeholder.png")

def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# ---------- поиск товара и извлечение картинки ----------
def search_first_product_link_by_query(query: str) -> Optional[str]:
    q = urllib.parse.urlencode({"search": query})
    url = f"{BASE}/?{q}"
    html = fetch(url)
    soup = BeautifulSoup(html, "lxml")

    # приоритетные ссылки на карточки
    for a in soup.select('a[href*="/goods/"], a[href*="/product/"]'):
        href = a.get("href") or ""
        if href.endswith(".html"):
            return absolutize(href)
    # общий фолбэк
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ("/goods/" in href or "/product/" in href) and href.endswith(".html"):
            return absolutize(href)
    return None

def extract_main_image(product_url: str) -> Optional[str]:
    html = fetch(product_url)
    soup = BeautifulSoup(html, "lxml")

    # 1) как просил: берём первую главную картинку <img itemprop="image" ... src="...">
    img = soup.find("img", attrs={"itemprop": "image"})
    if img and img.get("src"):
        src = absolutize(img["src"])
        if not looks_like_placeholder(src):
            return src

    # 2) og:image
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        src = absolutize(og["content"])
        if not looks_like_placeholder(src):
            return src

    # 3) первое изображение в карточке
    any_img = soup.select_one("img[src]")
    if any_img:
        src = absolutize(any_img["src"])
        if not looks_like_placeholder(src):
            return src

    return None

# ---------- токены из названия (фолбэк) ----------
CODE_SLASH   = re.compile(r"\b([A-Z]{1,8}-)(\d{2,6})(?:/(\d{2,6}))+\b", re.I)
CODE_SIMPLE  = re.compile(r"\b([A-Z]{1,8}(?:-[A-Z]{1,3})?[-_ ]?\d{2,6}[A-Z]{0,3})\b", re.I)
CODE_NUMONLY = re.compile(r"\b(\d{2,6})\b")

def tokens_from_name(name: str) -> list[str]:
    t = norm(name)
    out = []
    m = CODE_SLASH.search(t)
    if m:
        out.append((m.group(1)+m.group(2)).replace(" ","-"))
    for m in CODE_SIMPLE.finditer(t):
        out.append(re.sub(r"[ _]", "-", m.group(1)))
    for m in CODE_NUMONLY.finditer(t):
        out.append(m.group(1))
    # уникализация
    seen=set(); res=[]
    for c in out:
        c = c.upper()
        if c not in seen:
            seen.add(c); res.append(c)
    return res

# ---------- YML helpers ----------
def _ensure_picture(offer_el, url: str):
    pic = offer_el.find("picture")
    if pic is None:
        from xml.etree.ElementTree import SubElement
        pic = SubElement(offer_el, "picture")
    pic.text = url

def _flush(tree: ElementTree, root, photo_idx: dict):
    # сохранить YML
    buf = io.BytesIO()
    ElementTree(element=root).write(buf, encoding=ENC, xml_declaration=True)
    with open(YML_PATH, "wb") as f:
        f.write(buf.getvalue())
    # сохранить индекс
    os.makedirs(os.path.dirname(PHOTO_INDEX_PATH) or ".", exist_ok=True)
    save_json(PHOTO_INDEX_PATH, photo_idx)

# ---------- main ----------
def main():
    # загрузка YML
    with open(YML_PATH, "rb") as f:
        raw = f.read()
    tree = ElementTree()
    root = tree.fromstring(raw)

    # словари управления
    overrides  = load_json(PHOTO_OVERRIDES, {})       # ключ: "code:12345" или "name:...lower"
    blacklist  = set(load_json(PHOTO_BLACKLIST, []))  # те же ключи
    photo_idx  = load_json(PHOTO_INDEX_PATH, {})      # кэш по тем же ключам

    offers = root.findall(".//offer")
    updated = 0
    scanned = 0

    for o in offers:
        if scanned >= FETCH_LIMIT:
            break

        # имеем ли уже картинку?
        have_pic = False
        p = o.find("picture")
        if p is not None and norm(p.text):
            have_pic = True
        if have_pic:
            continue

        # ключи: сначала по коду, иначе по имени
        name_el = o.find("name")
        name = norm(name_el.text) if name_el is not None else ""

        vcode_el = o.find("vendorCode")
        vendor_code = norm(vcode_el.text) if vcode_el is not None else ""

        key_code = f"code:{vendor_code}" if vendor_code else None
        key_name = f"name:{name.lower()}" if name else None

        # чёрный список
        if (key_code and key_code in blacklist) or (key_name and key_name in blacklist):
            continue

        # overrides
        if key_code and key_code in overrides:
            _ensure_picture(o, overrides[key_code])
            photo_idx[key_code] = overrides[key_code]
            updated += 1; scanned += 1
            if updated % FLUSH_EVERY_N == 0: _flush(tree, root, photo_idx)
            continue
        if key_name and key_name in overrides:
            _ensure_picture(o, overrides[key_name])
            photo_idx[key_name] = overrides[key_name]
            updated += 1; scanned += 1
            if updated % FLUSH_EVERY_N == 0: _flush(tree, root, photo_idx)
            continue

        # кэш
        if key_code and key_code in photo_idx:
            _ensure_picture(o, photo_idx[key_code])
            updated += 1; scanned += 1
            if updated % FLUSH_EVERY_N == 0: _flush(tree, root, photo_idx)
            continue
        if key_name and key_name in photo_idx:
            _ensure_picture(o, photo_idx[key_name])
            updated += 1; scanned += 1
            if updated % FLUSH_EVERY_N == 0: _flush(tree, root, photo_idx)
            continue

        pic_url: Optional[str] = None

        # === ВАРИАНТ А (как ты просил): ПОИСК СНАЧАЛА ПО КОДУ ТОВАРА ===
        if vendor_code:
            try:
                product_url = search_first_product_link_by_query(vendor_code)
                if product_url:
                    pic = extract_main_image(product_url)
                    if pic:
                        pic_url = pic
            except Exception:
                pass
            finally:
                time.sleep(REQUEST_DELAY_MS / 1000.0)

        # === ФОЛБЭК: если по коду не нашли — пробуем по модельным токенам из названия ===
        if not pic_url and name:
            for t in tokens_from_name(name):
                try:
                    product_url = search_first_product_link_by_query(t)
                    if not product_url:
                        continue
                    pic = extract_main_image(product_url)
                    if pic:
                        pic_url = pic
                        break
                except Exception:
                    pass
                finally:
                    time.sleep(REQUEST_DELAY_MS / 1000.0)

        if pic_url:
            _ensure_picture(o, pic_url)
            if key_code:
                photo_idx[key_code] = pic_url
            elif key_name:
                photo_idx[key_name] = pic_url
            updated += 1

        scanned += 1
        if updated and updated % FLUSH_EVERY_N == 0:
            _flush(tree, root, photo_idx)

    _flush(tree, root, photo_idx)
    print(f"[OK] enriched: {updated} | scanned: {scanned} | limit={FETCH_LIMIT}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
