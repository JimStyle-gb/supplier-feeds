# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, json, time, hashlib, mimetypes, pathlib
import requests
from urllib.parse import urljoin, urlencode, urlparse
from collections import Counter
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ========= ENV =========
XLSX_URL   = os.getenv("XLSX_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE   = os.getenv("OUT_FILE",  "docs/copyline.yml")
ENC        = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

# База для ссылок на картинки в фиде (GitHub Pages)
# Воркфлоу её устанавливает автоматически (см. build_copyline.yml)
PAGES_BASE = os.getenv("PAGES_BASE", "https://example.github.io/supplier-feeds")

IMG_DIR    = os.getenv("IMG_DIR", "docs/copyline_img")
CACHE_FILE = os.getenv("CACHE_FILE", "data/copyline_cache.json")
SEEN_FILE  = os.getenv("SEEN_FILE",  "docs/copyline_seen.txt")

# Троттлинг и лимиты
RATE_DELAY = float(os.getenv("RATE_DELAY", "0.35"))  # пауза между сетевыми запросами
MAX_IMG_PER_ITEM = int(os.getenv("MAX_IMG_PER_ITEM", "6"))
MAX_SEARCH = int(os.getenv("MAX_SEARCH", "4000"))    # артикулов на поиск за прогон

SITE = "https://copyline.kz"
UA   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ========= УТИЛЫ =========
def norm(s): return re.sub(r"\s+"," ", (s or "").strip())
def md5(s: str) -> str: return hashlib.md5(s.encode("utf-8")).hexdigest()
def ensure_dirs():
    pathlib.Path(os.path.dirname(OUT_FILE)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(os.path.dirname(CACHE_FILE)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(IMG_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path("docs/.nojekyll").write_text("", encoding="utf-8")

def normalize_article(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[\s\-_]+","", s)
    s = s.replace("—","").replace("–","")
    return s.upper()

def fetch(session: requests.Session, url: str) -> requests.Response:
    r = session.get(url, headers=UA, timeout=60)
    r.raise_for_status()
    return r

def parse_price(v) -> int|None:
    if v is None: return None
    t = norm(str(v)).replace("₸","").replace("тг","")
    digits = re.sub(r"[^\d]","", t)
    return int(digits) if digits else None

def is_available(v) -> tuple[bool,str]:
    s = norm(str(v)).lower()
    if not s or s in {"-","—","н/д","нет"}: return False, "0"
    if s.isdigit(): q=int(s); return (q>0), str(q)
    if re.search(r">\s*\d+", s): return True, "10"
    if "есть" in s or "в наличии" in s or "да" in s: return True, "1"
    return True, "1"

HEADER_HINTS = {
    "name":        ["номенклатура","наименование","название","товар","описание"],
    "article":     ["артикул","номенклатура.артикул","sku","код товара","код","модель","part number","pn","p/n"],
    "availability":["остаток","наличие","кол-во","количество","qty","stock","остатки"],
    "price":       ["цена","опт","розница","стоимость","цена, тг","цена тг"],
    "unit":        ["ед.","ед","единица"],
    "category":    ["категория","раздел","группа","тип"],
}

def best_header(ws):
    def score(row_vals):
        low=[norm(x).lower() for x in row_vals]
        matched=set()
        for key,hints in HEADER_HINTS.items():
            for cell in low:
                if any(h in cell for h in hints): matched.add(key); break
        return len(matched)
    rows=[]
    for row in ws.iter_rows(min_row=1, max_row=40, values_only=True):
        rows.append([norm("" if v is None else str(v)) for v in row])
    best=([], None, -1)
    for i,r in enumerate(rows):
        sc=score(r)
        if sc>best[2]: best=(r, i+1, sc)
    for i in range(len(rows)-1):
        a,b=rows[i],rows[i+1]
        m=max(len(a),len(b)); merged=[]
        for j in range(m):
            x=a[j] if j<len(a) else ""; y=b[j] if j<len(b) else ""
            merged.append(" ".join(t for t in (x,y) if t).strip())
        sc=score(merged)
        if sc>best[2]: best=(merged, i+2, sc)
    return best[0], best[1]+1

def map_columns(headers):
    cols={}
    low=[h.lower() for h in headers]
    def find_any(keys):
        for i,cell in enumerate(low):
            if any(k in cell for k in keys): return i
        return None
    cols["name"]        = find_any(HEADER_HINTS["name"])
    cols["article"]     = find_any(HEADER_HINTS["article"])
    cols["availability"]= find_any(HEADER_HINTS["availability"])
    cols["price"]       = find_any(HEADER_HINTS["price"])
    cols["unit"]        = find_any(HEADER_HINTS["unit"])
    cols["category"]    = find_any(HEADER_HINTS["category"])
    return cols

# ========= КЭШ =========
def load_cache() -> dict:
    if not os.path.exists(CACHE_FILE): return {}
    try:
        return json.loads(pathlib.Path(CACHE_FILE).read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_cache(cache: dict):
    pathlib.Path(CACHE_FILE).write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def row_hash(it: dict) -> str:
    key = f"{it.get('name','')}|{it.get('price','')}|{it.get('available','')}|{it.get('qty','')}|{it.get('category','')}"
    return md5(key)

# ========= ПОИСК ССЫЛКИ НА ТОВАР И ВЫТАСКИВАНИЕ КАРТИНОК =========
ARTICLE_RE = re.compile(r"(?:артикул|sku|код|pn|p/n)\s*[:#\-]?\s*([A-Za-z0-9\-\._/]+)", re.I)

def extract_article_from_page(soup: BeautifulSoup) -> str|None:
    sku = soup.select_one('[itemprop="sku"]')
    if sku and sku.get_text(strip=True):
        return normalize_article(sku.get_text(strip=True))
    txt = soup.get_text(" ", strip=True)
    m = ARTICLE_RE.search(txt)
    if m: return normalize_article(m.group(1))
    return None

def extract_images_from_page(soup: BeautifulSoup, base_url: str, limit=8) -> list[str]:
    out=[]
    def pick(img):
        for a in ("data-src","data-original","src"):
            v=img.get(a)
            if v: return v
        return None
    for img in soup.find_all("img"):
        src = pick(img)
        if not src: continue
        u = urljoin(base_url, src)
        if u not in out:
            out.append(u)
            if len(out)>=limit: break
    return out

def search_product_url_by_article(session: requests.Session, art: str) -> str|None:
    for qurl in [f"{SITE}/search/?{urlencode({'q': art})}",
                 f"{SITE}/?s={art}"]:
        try:
            r = fetch(session, qurl)
        except Exception:
            continue
        time.sleep(RATE_DELAY)
        soup = BeautifulSoup(r.text, "lxml")
        a = soup.select_one('a[href*="/goods/"][href$=".html"]')
        if a and a.get("href"):
            u = urljoin(qurl, a.get("href"))
            if urlparse(u).netloc == urlparse(SITE).netloc:
                return u
    return None

def mirror_image(session: requests.Session, url: str, save_to: pathlib.Path) -> str|None:
    try:
        r = session.get(url, headers=UA, timeout=60, stream=True)
        r.raise_for_status()
    except Exception:
        return None
    # Определяем расширение
    ext = None
    ct = r.headers.get("Content-Type","").lower()
    if "image/" in ct:
        ext = mimetypes.guess_extension(ct.split(";")[0].strip()) or ".jpg"
    if not ext:
        p = urlparse(url).path
        ext = os.path.splitext(p)[1] or ".jpg"
    save_to = save_to.with_suffix(ext)
    save_to.parent.mkdir(parents=True, exist_ok=True)
    with open(save_to, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk: f.write(chunk)
    # Вернём публичный URL на GH Pages
    repo_path = str(save_to).replace("\\","/").split("docs/",1)[1]
    return f"{PAGES_BASE}/{repo_path}"

# ========= СБОР ДАННЫХ И ВЫГРУЗКА =========
ROOT_CAT_ID = "9300000"
def cat_id_for(name: str) -> str:
    h=int(hashlib.md5(name.lower().encode("utf-8")).hexdigest()[:6],16)
    return str(9300001 + (h % 400000))

def build_yml(items):
    cats={}
    for it in items:
        cn = it.get("category") or "Copyline"
        if cn not in cats: cats[cn]=cat_id_for(cn)

    root=Element("yml_catalog"); shop=SubElement(root,"shop")
    SubElement(shop,"name").text="copyline-xlsx"
    curr=SubElement(shop,"currencies"); SubElement(curr,"currency",{"id":"KZT","rate":"1"})

    xml_cats=SubElement(shop,"categories")
    SubElement(xml_cats,"category",{"id":ROOT_CAT_ID}).text="Copyline"
    for nm,cid in cats.items():
        SubElement(xml_cats,"category",{"id":cid,"parentId":ROOT_CAT_ID}).text=nm

    offers=SubElement(shop,"offers")
    used=set()
    for it in items:
        art = (it.get("article") or "").strip()
        oid = f"copyline:{art}" if art else f"copyline:{hashlib.md5((it.get('name','')+it.get('category','')).encode()).hexdigest()[:8]}"
        if oid in used:
            i=2
            while f"{oid}-{i}" in used: i+=1
            oid=f"{oid}-{i}"
        used.add(oid)

        cid = cats.get(it.get("category") or "Copyline", ROOT_CAT_ID)
        o = SubElement(offers,"offer",{
            "id":oid,
            "available":"true" if it["available"] else "false",
            "in_stock":"true" if it["available"] else "false",
        })
        SubElement(o,"name").text = it["name"]
        if it.get("price") is not None: SubElement(o,"price").text=str(it["price"])
        SubElement(o,"currencyId").text="KZT"
        SubElement(o,"categoryId").text=cid
        if it.get("brand"): SubElement(o,"vendor").text=it["brand"]
        if art: SubElement(o,"vendorCode").text=art
        q = it.get("qty") or ("1" if it["available"] else "0")
        for tag in ("quantity_in_stock","stock_quantity","quantity"): SubElement(o,tag).text=q
        for p in it.get("images", []):
            SubElement(o,"picture").text = p

    buf=io.BytesIO(); ElementTree(root).write(buf, encoding=ENC, xml_declaration=True); return buf.getvalue()

def main():
    ensure_dirs()
    cache = load_cache()
    session = requests.Session()

    # 1) XLSX -> items (быстро)
    x = fetch(session, XLSX_URL).content
    wb = load_workbook(io.BytesIO(x), read_only=True, data_only=True)

    raw=[]; seen_sheets=Counter()
    for ws in wb.worksheets:
        headers, start = best_header(ws)
        if not headers: continue
        cols = map_columns(headers)
        if cols.get("name") is None: continue

        current_cat=None
        for row in ws.iter_rows(min_row=start, values_only=True):
            row=list(row)
            def getc(idx): return None if idx is None or idx>=len(row) else row[idx]
            name    = norm(getc(cols.get("name")))
            article = norm(getc(cols.get("article")))
            price   = parse_price(getc(cols.get("price")))
            avail, qty = is_available(getc(cols.get("availability")))
            raw_cat = norm(getc(cols.get("category"))) if cols.get("category") is not None else ""

            # Заголовок секции
            if name and not article and price is None and len(name)>3 and not re.search(r"^(цены|прайс|тенге|лист|итог)", name.lower()):
                current_cat=name; continue
            if not name and not article: continue

            cat = raw_cat or current_cat or norm(ws.title)
            seen_sheets[ws.title]+=1

            item = {
                "article": article,
                "name": name or article,
                "brand": "",
                "category": cat,
                "price": price,
                "available": avail,
                "qty": qty,
            }
            raw.append(item)

    # 2) Дедуп: по артикулу, иначе по (name+category)
    dedup={}
    for it in raw:
        key = ("a", it["article"]) if it["article"] else ("n", it["name"].lower(), (it["category"] or "").lower())
        if key not in dedup: dedup[key]=it
        else:
            old=dedup[key]
            better = it if (it["price"] and not old["price"]) or (it["available"] and not old["available"]) else old
            dedup[key]=better
    items=list(dedup.values())

    # 3) ДЕЛЬТА: решаем, кому искать фото (только новые/изменённые и без фото в кэше)
    todo=[]; keep=0
    for it in items:
        art = normalize_article(it.get("article") or "")
        h = row_hash(it)
        c = cache.get(art)
        if c and c.get("row_hash")==h and c.get("images"):
            # ничего не делаем, берём из кэша
            it["images"]=c["images"]
            keep+=1
        else:
            todo.append(it)

    # 4) Поиск ссылок и зеркалирование фото (ТОЛЬКО по артикулу, без обхода категорий)
    searched=0; added=0
    for it in todo:
        if searched>=MAX_SEARCH: break
        art_raw = it.get("article") or ""
        art = normalize_article(art_raw)
        if not art:
            cache[art_raw] = {"row_hash": row_hash(it), "images": []}
            continue

        # Поиск страницы
        url = search_product_url_by_article(session, art)
        searched += 1
        time.sleep(RATE_DELAY)
        imgs_final=[]

        if url:
            # Открываем карточку, подтверждаем артикул, тянем картинки
            try:
                r = fetch(session, url); time.sleep(RATE_DELAY)
                soup = BeautifulSoup(r.text, "lxml")
                art_page = extract_article_from_page(soup)
                if art_page == art:
                    imgs = extract_images_from_page(soup, url, limit=MAX_IMG_PER_ITEM)
                    # Зеркалим
                    out_dir = pathlib.Path(IMG_DIR) / art
                    i=1
                    for src in imgs:
                        murl = mirror_image(session, src, out_dir / f"{i}")
                        if murl:
                            imgs_final.append(murl); i+=1
                            if len(imgs_final)>=MAX_IMG_PER_ITEM: break
            except Exception:
                pass

        it["images"] = imgs_final
        cache[art] = {
            "row_hash": row_hash(it),
            "images": imgs_final,
            "product_url": url or "",
            "updated_at": int(time.time())
        }
        if imgs_final: added+=1

    # 5) Запись кэша, отчёт
    save_cache(cache)
    with open(SEEN_FILE,"w",encoding="utf-8") as f:
        f.write(f"Sheets: {sum(seen_sheets.values())} rows read across {len(seen_sheets)} sheets\n")
        f.write(f"Items total: {len(items)} | From cache (images): {keep} | Newly imaged: {added} | Searched: {searched}\n")
        f.write(f"Images dir: {IMG_DIR}\n")
        f.write(f"Pages base: {PAGES_BASE}\n")

    # 6) YML → GH Pages
    yml = build_yml(items)
    with open(OUT_FILE,"wb") as f: f.write(yml)
    print(f"{OUT_FILE}: {len(items)} items; cache={CACHE_FILE}; images in {IMG_DIR}")
    print(f"Seen → {SEEN_FILE}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
