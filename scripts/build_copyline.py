# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, json, time, hashlib, urllib.parse
from typing import Optional, Dict, Any, List
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, ElementTree
from openpyxl import load_workbook
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========= НАСТРОЙКИ =========
BASE_URL    = "https://copyline.kz"
XLSX_URL    = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
OUT_FILE    = os.getenv("OUT_FILE",  "docs/copyline.yml")
ENC         = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

# кэш для картинок (метод 2: поиск «на лету» с хешем/артикулом/именем)
IMG_CACHE_FILE       = os.getenv("IMG_CACHE_FILE", "docs/copyline_photos.json")
IMG_LOOKUPS_PER_RUN  = int(os.getenv("IMG_LOOKUPS_PER_RUN", "300"))  # ограничиваем, чтобы ран не тянулся
IMG_WORKERS          = int(os.getenv("IMG_WORKERS", "6"))            # параллельность запросов
REQ_TIMEOUT          = float(os.getenv("REQ_TIMEOUT", "30"))

UA_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ========= УТИЛЫ =========
def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def fetch_xlsx(url_or_path: str) -> bytes:
    if re.match(r"^https?://", url_or_path, re.I):
        with requests.get(url_or_path, headers=UA_HEADERS, timeout=REQ_TIMEOUT) as r:
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
    # одиночные строки
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
        # 1-й проход — избегаем «артикульных» заголовков для name
        for i,cell in enumerate(low):
            if any(k in cell for k in keys) and not any(a in cell for a in avoid):
                return i
        # 2-й проход — любое совпадение
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

# ========= ПОИСК ФОТО (метод 2 с кэшем) =========
def read_img_cache() -> Dict[str, Any]:
    try:
        with open(IMG_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"by_article":{}, "by_name":{}, "ts": int(time.time())}
    except Exception:
        return {"by_article":{}, "by_name":{}, "ts": int(time.time())}

def save_img_cache(cache: Dict[str, Any]):
    os.makedirs(os.path.dirname(IMG_CACHE_FILE), exist_ok=True)
    tmp = IMG_CACHE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    os.replace(tmp, IMG_CACHE_FILE)

def _fetch(url: str) -> Optional[str]:
    try:
        with requests.get(url, headers=UA_HEADERS, timeout=REQ_TIMEOUT) as r:
            if r.status_code == 200:
                return r.text
            return None
    except Exception:
        return None

def _extract_product_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    hrefs=set()
    for a in soup.select('a[href*="/product/"], a[href*="/goods/"]'):
        href = a.get("href") or ""
        if href.endswith(".html"):
            hrefs.add(absolutize(href))
    # подстраховка
    for a in soup.find_all("a", href=True):
        href=a["href"]
        if href.endswith(".html") and ("/goods/" in href or "/product/" in href):
            hrefs.add(absolutize(href))
    return list(hrefs)

def _title_text(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    return norm(h1.get_text()) if h1 else ""

def _first_main_img_src(soup: BeautifulSoup) -> Optional[str]:
    img = soup.find("img", {"itemprop":"image"})
    if img and img.get("src"):
        return absolutize(img.get("src"))
    # fallback: часто лежит в компонентах jshopping
    for im in soup.find_all("img", src=True):
        src = im.get("src") or ""
        if "components/com_jshopping/files/img_products/" in src:
            return absolutize(src)
    # как крайний случай — первый img
    im = soup.find("img", src=True)
    return absolutize(im["src"]) if im else None

def _build_search_urls(query: str) -> List[str]:
    q = urllib.parse.quote(query)
    return [
        f"{BASE_URL}/search?searchword={q}",
        f"{BASE_URL}/?searchword={q}&searchphrase=all&option=com_search",
        f"{BASE_URL}/index.php?option=com_jshopping&controller=search&task=result&search={q}",
        f"{BASE_URL}/?option=com_jshopping&controller=search&task=result&search={q}",
        f"{BASE_URL}/?controller=search&task=result&search={q}",
    ]

def _confidence(title: str, article: str, name: str) -> bool:
    t = title.lower()
    if article and article.lower() in t:
        return True
    # проверяем по токенам названия (минимум 2 совпадения)
    tokens = [w for w in re.split(r"[^a-zа-я0-9]+", name.lower()) if len(w) >= 3]
    hit = sum(1 for w in tokens if w in t)
    return hit >= 2

def find_image_for_item(article: str, name: str) -> Optional[str]:
    """Поиск страницы товара через поиск сайта и возврат src первой главной картинки."""
    # запрос сначала по артикулу (если есть), иначе по названию
    queries = []
    if article: queries.append(article)
    # иногда артикул встречается внутри названия — но добавим и само имя
    if name and name not in queries:
        queries.append(name)

    for q in queries:
        for url in _build_search_urls(q):
            html = _fetch(url)
            if not html: 
                continue
            links = _extract_product_links(html)
            # перебираем первые 3-5 карточек
            for purl in links[:5]:
                phtml = _fetch(purl)
                if not phtml:
                    continue
                soup = BeautifulSoup(phtml, "lxml")
                title = _title_text(soup)
                if not _confidence(title, article, name):
                    continue
                pic = _first_main_img_src(soup)
                if pic:
                    return pic
    return None

def resolve_images_for_items(items: List[Dict[str, Any]]) -> Dict[int, Optional[str]]:
    """Возвращает dict: индекс -> url картинки (или None). Использует кэш и ограничение по количеству."""
    cache = read_img_cache()
    by_article: Dict[str,str] = cache.get("by_article", {})
    by_name: Dict[str,str] = cache.get("by_name", {})
    results: Dict[int, Optional[str]] = {}
    to_fetch: List[int] = []

    # сначала заполним из кэша
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

    # ограничиваем число запросов за один прогон
    to_fetch = to_fetch[:max(0, IMG_LOOKUPS_PER_RUN)]

    # параллельный поиск
    def worker(idx: int) -> (int, Optional[str]):
        it = items[idx]
        return idx, find_image_for_item(norm(it.get("article")), norm(it.get("name")))

    if to_fetch:
        with ThreadPoolExecutor(max_workers=max(1, IMG_WORKERS)) as ex:
            futs = [ex.submit(worker, idx) for idx in to_fetch]
            for fu in as_completed(futs):
                idx, pic = fu.result()
                it = items[idx]
                article = norm(it.get("article"))
                name    = norm(it.get("name"))
                results[idx] = pic
                # обновляем кэш
                if article:
                    by_article[article] = pic or ""
                if name:
                    by_name[name] = pic or ""

        cache["by_article"] = by_article
        cache["by_name"] = by_name
        cache["ts"] = int(time.time())
        save_img_cache(cache)

    # для остальных (вышедших за лимит) оставим None — дособерётся в следующий раз
    for idx in range(len(items)):
        if idx not in results:
            results[idx] = None

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
    # не создаём дочернюю категорию "Copyline", чтобы не дублировать корень
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
    d = os.path.dirname(OUT_FILE)
    if d: os.makedirs(d, exist_ok=True)

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

    # 3) ищем фото (метод 2, «на лету», с кэшем и лимитом за прогон)
    pictures = resolve_images_for_items(items)

    # 4) пишем YML
    yml = build_yml(items, pictures)
    with open(OUT_FILE, "wb") as f:
        f.write(yml)

    # статистика
    filled = sum(1 for v in pictures.values() if v)
    print(f"[OK] {OUT_FILE}: items={len(items)} | pictures_set={filled} | looked_up_this_run<= {IMG_LOOKUPS_PER_RUN} | workers={IMG_WORKERS}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
