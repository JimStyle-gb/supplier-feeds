# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, json, time, hashlib, urllib.parse
from typing import Optional, Dict, Any, List, Tuple
import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from xml.etree.ElementTree import Element, SubElement, ElementTree
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========= НАСТРОЙКИ =========
BASE_URL    = "https://copyline.kz"
XLSX_URL    = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
OUT_FILE    = os.getenv("OUT_FILE",  "docs/copyline.yml")
ENC         = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

# Кэш фоток (ускоряет повторы)
IMG_CACHE_FILE       = os.getenv("IMG_CACHE_FILE", "docs/copyline_photos.json")

# Сетевые лимиты (чтобы ран не висел)
IMG_LOOKUPS_PER_RUN  = int(os.getenv("IMG_LOOKUPS_PER_RUN", "200"))  # сколько товаров искать за прогон
IMG_WORKERS          = int(os.getenv("IMG_WORKERS", "6"))            # параллельность поиска
REQ_TIMEOUT          = float(os.getenv("REQ_TIMEOUT", "15"))         # таймаут на запрос

UA_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
SESSION = requests.Session()

# ========= УТИЛЫ =========
def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def fetch_xlsx(url_or_path: str) -> bytes:
    if re.match(r"^https?://", url_or_path, re.I):
        r = SESSION.get(url_or_path, headers=UA_HEADERS, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        return r.content
    with open(url_or_path, "rb") as f:
        return f.read()

def absolutize(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"): return url
    return urllib.parse.urljoin(BASE_URL, url)

# ========= ШАПКА/КОЛОНКИ =========
HEADER_HINTS = {
    "name":     ["номенклатура","наименование","наименование товара","название","товар","описание","product name","item"],
    "article":  ["артикул","номенклатура.артикул","sku","код товара","код","модель","part number","pn","p/n"],
    "price":    ["цена","опт","опт. цена","розница","стоимость","цена, тг","цена тг","retail","price"],
    "unit":     ["ед.","ед","единица","unit"],
    "category": ["категория","раздел","группа","тип","category"],
}

def best_header(ws):
    def score(arr):
        low=[norm(x).lower() for x in arr]
        got=set()
        for k,hints in HEADER_HINTS.items():
            for cell in low:
                if any(h in cell for h in hints):
                    got.add(k); break
        return len(got)

    rows=[]
    for row in ws.iter_rows(min_row=1, max_row=40, values_only=True):
        rows.append([norm("" if v is None else str(v)) for v in row])

    best_row, best_idx, best_sc = [], None, -1
    for i,r in enumerate(rows):
        sc=score(r)
        if sc>best_sc:
            best_row, best_idx, best_sc = r, i+1, sc

    # склейка соседних строк; при равенстве — предпочитаем склейку
    for i in range(len(rows)-1):
        a,b=rows[i],rows[i+1]
        m=max(len(a),len(b)); merged=[]
        for j in range(m):
            x=a[j] if j<len(a) else ""
            y=b[j] if j<len(b) else ""
            merged.append((" ".join([x,y])).strip())
        sc=score(merged)
        if sc>best_sc or (sc==best_sc and sc>0):
            best_row, best_idx, best_sc = merged, i+2, sc
    return best_row, (best_idx or 1)+1

def map_cols(headers):
    low=[h.lower() for h in headers]
    def find(keys, avoid=None):
        avoid = avoid or []
        for i,cell in enumerate(low):
            if any(k in cell for k in keys) and not any(a in cell for a in avoid):
                return i
        for i,cell in enumerate(low):
            if any(k in cell for k in keys):
                return i
        return None
    name_idx = find(HEADER_HINTS["name"], avoid=["артикул","sku","код товара","p/n","part number"])
    return {
        "name":     name_idx,
        "article":  find(HEADER_HINTS["article"]),
        "price":    find(HEADER_HINTS["price"]),
        "unit":     find(HEADER_HINTS["unit"]),
        "category": find(HEADER_HINTS["category"]),
    }

def parse_price(v) -> Optional[int]:
    if v is None: return None
    t = norm(str(v)).replace("₸","").replace("тг","")
    digits = re.sub(r"[^\d]", "", t)
    return int(digits) if digits else None

# ========= КЭШ ФОТО =========
def read_img_cache() -> Dict[str, Any]:
    try:
        with open(IMG_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"by_article":{}, "by_name":{}, "ts": int(time.time())}
    except Exception:
        return {"by_article":{}, "by_name":{}, "ts": int(time.time())}

def save_img_cache(cache: Dict[str, Any]):
    ensure_dir_for(IMG_CACHE_FILE)
    tmp = IMG_CACHE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    os.replace(tmp, IMG_CACHE_FILE)

# ========= ПОИСК КАРТОЧКИ И ИЗВЛЕЧЕНИЕ ССЫЛКИ НА ФОТО =========
def _search_urls(query: str) -> List[str]:
    q = urllib.parse.quote(query)
    return [
        f"{BASE_URL}/search?searchword={q}",
        f"{BASE_URL}/?searchword={q}&searchphrase=all&option=com_search",
        f"{BASE_URL}/index.php?option=com_jshopping&controller=search&task=result&search={q}",
    ]

def _extract_product_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    hrefs=set()
    for a in soup.select('a[href*="/product/"], a[href*="/goods/"]'):
        href = a.get("href") or ""
        if href.endswith(".html"):
            hrefs.add(urllib.parse.urljoin(BASE_URL, href))
    return list(hrefs)

def _get(url: str) -> Optional[str]:
    try:
        r = SESSION.get(url, headers=UA_HEADERS, timeout=REQ_TIMEOUT)
        return r.text if r.status_code==200 else None
    except Exception:
        return None

def _title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    return norm(h1.get_text()) if h1 else ""

def _first_itemprop_image_src(html: str) -> Optional[str]:
    """БЕРЁМ ТОЛЬКО src из ПЕРВОГО <img itemprop="image">. Без любых догадок."""
    soup = BeautifulSoup(html, "lxml")
    img = soup.select_one('img[itemprop="image"][src]')
    if img:
        return absolutize(img["src"])
    return None

def _page_matches(title: str, article: str, name: str) -> bool:
    t = title.lower()
    if article and article.lower() in t:
        return True
    # маленькая страховка: 2+ токена из name в заголовке
    tokens = [w for w in re.split(r"[^a-zа-я0-9]+", (name or "").lower()) if len(w) >= 3]
    return sum(1 for w in tokens if w in t) >= 2

def find_image_for_item(article: str, name: str) -> Optional[str]:
    """1) ищем карточку; 2) открываем; 3) копируем src из <img itemprop='image'>."""
    queries = []
    if article: queries.append(article)
    if name and name not in queries: queries.append(name)

    for q in queries:
        for su in _search_urls(q):
            html = _get(su)
            if not html: 
                continue
            links = _extract_product_links(html)
            for purl in links[:3]:  # смотрим первые 3 карточки
                phtml = _get(purl)
                if not phtml:
                    continue
                soup = BeautifulSoup(phtml, "lxml")
                if not _page_matches(_title(soup), article, name):
                    continue
                pic = _first_itemprop_image_src(phtml)  # только itemprop="image"
                if pic:
                    return pic
    return None

def resolve_images_for_items(items: List[Dict[str, Any]]) -> Dict[int, Optional[str]]:
    """Возвращает индекс -> url. Сначала кэш, затем сетевой поиск с лимитом и параллельностью."""
    cache = read_img_cache()
    by_article: Dict[str,str] = cache.get("by_article", {})
    by_name: Dict[str,str] = cache.get("by_name", {})
    results: Dict[int, Optional[str]] = {}
    to_fetch: List[int] = []

    # кэш
    for idx, it in enumerate(items):
        article = norm(it.get("article"))
        name    = norm(it.get("name"))
        url = None
        if article and article in by_article:
            url = by_article.get(article) or None
        elif name and name in by_name:
            url = by_name.get(name) or None
        if url:
            results[idx] = url
        else:
            to_fetch.append(idx)

    to_fetch = to_fetch[:max(0, IMG_LOOKUPS_PER_RUN)]

    def worker(idx: int) -> Tuple[int, Optional[str]]:
        it = items[idx]
        return idx, find_image_for_item(norm(it.get("article")), norm(it.get("name")))

    if to_fetch:
        with ThreadPoolExecutor(max_workers=max(1, IMG_WORKERS)) as ex:
            futs = [ex.submit(worker, idx) for idx in to_fetch]
            for fu in as_completed(futs):
                idx, pic = fu.result()
                results[idx] = pic
                it = items[idx]
                a = norm(it.get("article"))
                n = norm(it.get("name"))
                if a:
                    by_article[a] = pic or ""
                if n:
                    by_name[n] = pic or ""

        cache["by_article"] = by_article
        cache["by_name"] = by_name
        cache["ts"] = int(time.time())
        save_img_cache(cache)

    for i in range(len(items)):
        results.setdefault(i, None)
    return results

# ========= YML =========
ROOT_CAT_ID = "9300000"
def hash_int(s): return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)
def cat_id_for(name): return str(9300001 + (hash_int(name.lower()) % 400000))

def offer_id(it):
    art = norm(it.get("article"))
    if art: return f"copyline:{art}"
    base = re.sub(r"[^a-z0-9]+", "-", norm(it.get("name","")).lower())
    h = hashlib.md5((norm(it.get('name','')).lower()+"|"+norm(it.get('category','')).lower()).encode('utf-8')).hexdigest()[:8]
    return f"copyline:{base}:{h}"

def build_yml(items: List[Dict[str,Any]], pictures: Dict[int, Optional[str]]) -> bytes:
    cats = {}
    for it in items:
        nm = it.get("category") or "Copyline"
        if nm.strip().lower() != "copyline":
            cats.setdefault(nm, cat_id_for(nm))

    root = Element("yml_catalog"); shop = SubElement(root, "shop")
    SubElement(shop, "name").text = "copyline"
    curr = SubElement(shop, "currencies"); SubElement(curr, "currency", {"id":"KZT", "rate":"1"})

    xml_cats = SubElement(shop, "categories")
    SubElement(xml_cats, "category", {"id": ROOT_CAT_ID}).text = "Copyline"
    for nm, cid in cats.items():
        SubElement(xml_cats, "category", {"id": cid, "parentId": ROOT_CAT_ID}).text = nm

    offers = SubElement(shop, "offers")
    used = set()
    for idx, it in enumerate(items):
        oid = offer_id(it)
        if oid in used:
            extra = hashlib.md5((it.get("name","") + str(it.get("price"))).encode("utf-8")).hexdigest()[:6]
            i = 2
            while f"{oid}-{extra}-{i}" in used: i += 1
            oid = f"{oid}-{extra}-{i}"
        used.add(oid)

        nm = it.get("category") or "Copyline"
        cid = ROOT_CAT_ID if nm.strip().lower()=="copyline" else cats.get(nm, ROOT_CAT_ID)

        o = SubElement(offers, "offer", {
            "id": oid,
            "available": "true",
            "in_stock": "true",
        })
        SubElement(o, "name").text = it.get("name","")
        if it.get("price") is not None: SubElement(o, "price").text = str(it["price"])
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cid
        if it.get("article"): SubElement(o, "vendorCode").text = it["article"]

        pic = pictures.get(idx)
        if pic:
            SubElement(o, "picture").text = pic

        for tag in ("quantity_in_stock", "stock_quantity", "quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

# ========= MAIN =========
def main():
    ensure_dir_for(OUT_FILE)

    # 1) читаем XLSX
    xls = fetch_xlsx(XLSX_URL)
    wb = load_workbook(io.BytesIO(xls), read_only=True, data_only=True)

    # 2) собираем позиции
    items: List[Dict[str,Any]] = []
    found_name_sheet = False

    for ws in wb.worksheets:
        headers, start = best_header(ws)
        cols = map_cols(headers)
        if cols.get("name") is None:
            continue
        found_name_sheet = True

        for row in ws.iter_rows(min_row=start, values_only=True):
            row = list(row)
            def getc(i): return None if i is None or i >= len(row) else row[i]
            name     = norm(getc(cols["name"]))
            if not name:
                continue
            article  = norm(getc(cols.get("article")))
            price    = parse_price(getc(cols.get("price")))
            category = norm(getc(cols.get("category"))) or "Copyline"

            items.append({
                "name": name,
                "article": article,
                "category": category,
                "price": price,
            })

    if not found_name_sheet:
        print("ERROR: Не найден лист с колонкой наименования (name).", file=sys.stderr)
        sys.exit(1)

    # 3) фото: открываем карточку и берём src из <img itemprop="image">
    pictures = resolve_images_for_items(items)

    # 4) пишем YML
    yml = build_yml(items, pictures)
    with open(OUT_FILE, "wb") as f:
        f.write(yml)

    filled = sum(1 for v in pictures.values() if v)
    print(f"[OK] {OUT_FILE}: items={len(items)} | pictures_set_now={filled} | looked_up<= {IMG_LOOKUPS_PER_RUN} | workers={IMG_WORKERS}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
