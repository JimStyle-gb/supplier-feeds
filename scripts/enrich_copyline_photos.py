from __future__ import annotations
import os, re, io, sys, time, json, random, urllib.parse
from typing import Optional, Dict, Any, List, Tuple
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

BASE = "https://copyline.kz"
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

# ===== env =====
YML_PATH           = os.getenv("YML_PATH", "docs/copyline.yml")
ENC                = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
PHOTO_INDEX_PATH   = os.getenv("PHOTO_INDEX_PATH", "docs/copyline_photo_index.json")
PHOTO_OVERRIDES    = os.getenv("PHOTO_OVERRIDES", "docs/copyline_photo_overrides.json")
PHOTO_BLACKLIST    = os.getenv("PHOTO_BLACKLIST", "docs/copyline_photo_blacklist.json")
FETCH_LIMIT        = int(os.getenv("PHOTO_FETCH_LIMIT", "200"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "600"))
BACKOFF_MAX_MS     = int(os.getenv("BACKOFF_MAX_MS", "12000"))
FLUSH_EVERY_N      = int(os.getenv("FLUSH_EVERY_N", "20"))

# ===== utils =====
def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def absolutize(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return urllib.parse.urljoin(BASE, url)

def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def jitter_sleep(ms_base: int):
    ms = ms_base + random.randint(0, max(50, ms_base // 5))
    time.sleep(ms / 1000.0)

def fetch(url: str) -> str:
    """GET с экспоненциальным бэкоффом: 403/429/502/503 ретраим."""
    delay = REQUEST_DELAY_MS / 1000.0
    while True:
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=45)
            if r.status_code in (403, 429, 502, 503):
                time.sleep(min(BACKOFF_MAX_MS/1000.0, delay))
                delay = min(delay * 2, BACKOFF_MAX_MS/1000.0)
                continue
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            # краткая пауза и ещё попытка
            time.sleep(min(BACKOFF_MAX_MS/1000.0, delay))
            delay = min(delay * 2, BACKOFF_MAX_MS/1000.0)

def looks_like_placeholder(url: str) -> bool:
    u = url.lower()
    return ("noimage" in u) or u.endswith("/placeholder.png")

# ===== нормализация строки для сравнения кодов =====
ALNUM_RE = re.compile(r"[^0-9a-zа-я]+", re.I)
def unify(s: str) -> str:
    # в верхний регистр, ё->е, убрать все кроме цифр и букв
    return ALNUM_RE.sub("", (s or "").strip().lower().replace("ё","е")).upper()

# ===== поиск ссылки на товар по коду =====
SEARCH_ENDPOINTS = [
    "{base}/?{q}",  # ?search=XXXX
    "{base}/index.php?{q}",
    "{base}/search?{q}",
]
SEARCH_PARAM_KEYS = ["search", "searchword"]

def collect_candidates_from_search_html(html: str, query: str) -> List[Tuple[str,int]]:
    """
    Возвращает список (href, score), где score выше = более похоже на страницу товара.
    """
    soup = BeautifulSoup(html, "lxml")
    query_u = unify(query)

    # собрать все кандидаты
    raw_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ("/product/" in href or "/goods/" in href) and not href.startswith("#"):
            raw_links.append((a, absolutize(href)))

    cand: List[Tuple[str,int]] = []
    for a, href in raw_links:
        score = 0
        if "/product/" in href: score += 5  # предпочитаем страницу товара
        if "/goods/" in href:   score += 2  # категория / листинг

        # совпадение по текстам вокруг
        text_blk = " ".join([
            a.get_text(" ", strip=True) or "",
            a.get("title") or "",
        ])

        img = None
        # ближайшая картинка рядом в том же блоке (для возможного фолбэка)
        for par in [a, a.parent, a.parent.parent if a.parent else None]:
            if par:
                img = par.find("img", src=True)
                if img: break

        if unify(text_blk).find(query_u) != -1:
            score += 3
        elif img:
            itext = " ".join([img.get("alt") or "", img.get("title") or ""])
            if unify(itext).find(query_u) != -1:
                score += 2

        cand.append((href, score))

    # уникализировать, отсортировать по score desc, затем по порядку появления
    seen=set(); uniq=[]
    for href, score in cand:
        if href not in seen:
            uniq.append((href, score)); seen.add(href)
    uniq.sort(key=lambda x: x[1], reverse=True)
    return uniq

def verify_page_matches_code(html: str, code: str) -> bool:
    code_u = unify(code)
    soup = BeautifulSoup(html, "lxml")
    # h1
    h1 = soup.find("h1")
    if h1 and unify(h1.get_text()).find(code_u) != -1:
        return True
    # явные подписи типа "Артикул"
    full_text = unify(soup.get_text(" ", strip=True))
    if full_text.find(code_u) != -1:
        return True
    return False

def search_product_page_for_code(code: str) -> Optional[str]:
    """Пытаемся найти именно страницу товара по коду."""
    if not code:
        return None

    for endpoint in SEARCH_ENDPOINTS:
        for key in SEARCH_PARAM_KEYS:
            q = urllib.parse.urlencode({key: code})
            url = endpoint.format(base=BASE, q=q)
            html = fetch(url)
            cands = collect_candidates_from_search_html(html, code)
            # идём по кандидатам от лучшего к худшему; проверяем, что внутри страница 'про этот код'
            for href, _score in cands:
                try:
                    page = fetch(href)
                except Exception:
                    continue
                if verify_page_matches_code(page, code):
                    return href
            # небольшая пауза между запросами
            jitter_sleep(REQUEST_DELAY_MS // 2)

    return None

# ===== выдёргивание главного изображения со страницы товара =====
def pick_src_from_img_tag(img) -> Optional[str]:
    # приоритет: data-src -> srcset -> src
    for attr in ("data-src", "data-original", "data-large", "data-image", "srcset", "src"):
        val = img.get(attr)
        if not val: continue
        if attr == "srcset":
            # взять первый URL из srcset
            first = val.split(",")[0].strip().split(" ")[0].strip()
            if first: return absolutize(first)
        else:
            return absolutize(val)
    return None

def extract_main_image(product_url: str) -> Optional[str]:
    html = fetch(product_url)
    soup = BeautifulSoup(html, "lxml")

    # 1) ровно то, что вы просили/ждали
    img = soup.find("img", attrs={"itemprop": "image"})
    if img:
        src = pick_src_from_img_tag(img)
        if src and not looks_like_placeholder(src):
            return src

    # 2) id=main_image_****
    img = soup.select_one('img[id^="main_image_"]')
    if img:
        src = pick_src_from_img_tag(img)
        if src and not looks_like_placeholder(src):
            return src

    # 3) og:image / link rel=image_src
    tag = soup.find("meta", attrs={"property": "og:image"})
    if tag and tag.get("content"):
        src = absolutize(tag["content"])
        if not looks_like_placeholder(src):
            return src
    link = soup.find("link", rel=lambda v: v and "image_src" in v)
    if link and link.get("href"):
        src = absolutize(link["href"])
        if not looks_like_placeholder(src):
            return src

    # 4) общие варианты на странице товара
    for sel in [
        "img.product-image", "img.jshop_img", ".product-image img", ".main-image img", "img[src]"
    ]:
        img = soup.select_one(sel)
        if img:
            src = pick_src_from_img_tag(img)
            if src and not looks_like_placeholder(src):
                return src

    # 5) крайний случай: большая картинка в ссылке (lightbox и т.п.)
    a = soup.select_one('a[href$=".jpg"], a[href$=".jpeg"], a[href$=".png"]')
    if a and a.get("href"):
        src = absolutize(a["href"])
        if not looks_like_placeholder(src):
            return src

    return None

# ===== фолбэк: картинка рядом с ссылкой из результатов поиска =====
def try_image_from_search_context(query: str) -> Optional[str]:
    """Если на выдаче поиска возле ссылки лежит миниатюра — берём её."""
    for endpoint in SEARCH_ENDPOINTS:
        for key in SEARCH_PARAM_KEYS:
            q = urllib.parse.urlencode({key: query})
            url = endpoint.format(base=BASE, q=q)
            html = fetch(url)
            soup = BeautifulSoup(html, "lxml")
            # типичный список
            for img in soup.select("img[src]"):
                src = pick_src_from_img_tag(img)
                if src and not looks_like_placeholder(src):
                    return src
            jitter_sleep(REQUEST_DELAY_MS // 2)
    return None

# ===== токены из названия (как раньше) =====
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
    seen=set(); res=[]
    for c in out:
        c = c.upper()
        if c not in seen:
            seen.add(c); res.append(c)
    return res

# ===== YML helpers =====
def _ensure_picture(offer_el, url: str):
    pic = offer_el.find("picture")
    if pic is None:
        pic = ET.SubElement(offer_el, "picture")
    pic.text = url

def _flush(tree: ET.ElementTree, photo_idx: dict):
    tree.write(YML_PATH, encoding=ENC, xml_declaration=True)
    save_json(PHOTO_INDEX_PATH, photo_idx)

# ===== main =====
def main():
    # загрузили YML
    with open(YML_PATH, "rb") as f:
        raw = f.read()
    root = ET.fromstring(raw)
    tree = ET.ElementTree(root)

    overrides  = load_json(PHOTO_OVERRIDES, {})       # key: code:XXXX / name:yyyy
    blacklist  = set(load_json(PHOTO_BLACKLIST, []))
    photo_idx  = load_json(PHOTO_INDEX_PATH, {})

    offers = root.findall(".//offer")
    updated = 0
    scanned = 0

    for o in offers:
        if scanned >= FETCH_LIMIT:
            break

        # если уже стоит картинка — пропускаем
        p = o.find("picture")
        if p is not None and norm(p.text):
            continue

        name_el = o.find("name")
        name = norm(name_el.text) if name_el is not None else ""

        vcode_el = o.find("vendorCode")
        vendor_code = norm(vcode_el.text) if vcode_el is not None else ""

        key_code = f"code:{vendor_code}" if vendor_code else None
        key_name = f"name:{name.lower()}" if name else None

        # чёрный список
        if (key_code and key_code in blacklist) or (key_name and key_name in blacklist):
            scanned += 1
            continue

        # overrides
        if key_code and key_code in overrides:
            _ensure_picture(o, overrides[key_code])
            photo_idx[key_code] = overrides[key_code]
            updated += 1; scanned += 1
            if updated % FLUSH_EVERY_N == 0: _flush(tree, photo_idx)
            continue
        if key_name and key_name in overrides:
            _ensure_picture(o, overrides[key_name])
            photo_idx[key_name] = overrides[key_name]
            updated += 1; scanned += 1
            if updated % FLUSH_EVERY_N == 0: _flush(tree, photo_idx)
            continue

        # кэш
        if key_code and key_code in photo_idx:
            _ensure_picture(o, photo_idx[key_code])
            updated += 1; scanned += 1
            if updated % FLUSH_EVERY_N == 0: _flush(tree, photo_idx)
            continue
        if key_name and key_name in photo_idx:
            _ensure_picture(o, photo_idx[key_name])
            updated += 1; scanned += 1
            if updated % FLUSH_EVERY_N == 0: _flush(tree, photo_idx)
            continue

        pic_url: Optional[str] = None

        # === 1) Ищем страницу товара строго по коду ===
        if vendor_code:
            try:
                product_url = search_product_page_for_code(vendor_code)
                if product_url:
                    pic = extract_main_image(product_url)
                    if pic: pic_url = pic
            except Exception:
                pass
            finally:
                jitter_sleep(REQUEST_DELAY_MS)

        # === 2) Если по коду не нашли, пробуем токены из названия ===
        if not pic_url and name:
            for t in tokens_from_name(name):
                try:
                    product_url = search_product_page_for_code(t)
                    if not product_url:
                        continue
                    pic = extract_main_image(product_url)
                    if pic:
                        pic_url = pic
                        break
                except Exception:
                    pass
                finally:
                    jitter_sleep(REQUEST_DELAY_MS)

        # === 3) Фолбэк: миниатюра прямо на странице результатов поиска ===
        if not pic_url and vendor_code:
            try:
                thumb = try_image_from_search_context(vendor_code)
                if thumb:
                    pic_url = thumb
            except Exception:
                pass
            finally:
                jitter_sleep(REQUEST_DELAY_MS // 2)

        if not pic_url and name:
            try:
                for t in tokens_from_name(name):
                    thumb = try_image_from_search_context(t)
                    if thumb:
                        pic_url = thumb
                        break
            except Exception:
                pass
            finally:
                jitter_sleep(REQUEST_DELAY_MS // 2)

        if pic_url:
            _ensure_picture(o, pic_url)
            if key_code:
                photo_idx[key_code] = pic_url
            elif key_name:
                photo_idx[key_name] = pic_url
            updated += 1

        scanned += 1
        if updated and updated % FLUSH_EVERY_N == 0:
            _flush(tree, photo_idx)

    _flush(tree, photo_idx)
    print(f"[OK] enriched: {updated} | scanned: {scanned} | limit={FETCH_LIMIT}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
