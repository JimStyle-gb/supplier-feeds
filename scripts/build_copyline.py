# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, time, json, hashlib, pathlib, mimetypes, requests
from collections import Counter
from urllib.parse import urljoin, urlparse, urlencode
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ===== ENV =====
XLSX_URL     = os.getenv("XLSX_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE     = os.getenv("OUT_FILE",  "docs/copyline.yml")
ENC          = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
CATS_FILE    = os.getenv("CATEGORIES_FILE", "docs/categories_copyline.txt")
SEEN_FILE    = os.getenv("SEEN_FILE", "docs/copyline_seen.txt")
CACHE_FILE   = os.getenv("CACHE_FILE", "docs/copyline_cache.json")
MIRROR_DIR   = os.getenv("MIRROR_DIR", "docs/img_copyline")
MIRROR_IMAGES= (os.getenv("MIRROR_IMAGES") or "1").strip().lower() in {"1","true","yes","on"}
MAX_MIRROR_PER_ITEM = int(os.getenv("MAX_MIRROR_PER_ITEM","1"))  # оставляем 1, т.к. берём ровно одно фото

# быстрый режим (дельта)
STRICT_IMAGE_MATCH   = (os.getenv("STRICT_IMAGE_MATCH") or "1").strip().lower() in {"1","true","yes","on"}
SEARCH_FALLBACK      = (os.getenv("SEARCH_FALLBACK") or "1").strip().lower() in {"1","true","yes","on"}
MAX_SEARCH    = int(os.getenv("MAX_SEARCH", "2000"))
CRAWL_DELAY   = float(os.getenv("CRAWL_DELAY", "0.4"))

# усиление качества фото
FORCE_REFETCH_NO_PHOTO = (os.getenv("FORCE_REFETCH_NO_PHOTO") or "1").strip().lower() in {"1","true","yes","on"}
CHECK_IMAGE_BYTES      = (os.getenv("CHECK_IMAGE_BYTES") or "1").strip().lower() in {"1","true","yes","on"}
MIN_IMAGE_BYTES        = int(os.getenv("MIN_IMAGE_BYTES", "7000"))  # <7 KB считаем мусором

UA_HEADERS    = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
SITE_ORIGIN   = "https://copyline.kz"

def pages_base() -> str:
    repo = os.getenv("GITHUB_REPOSITORY", "").strip()
    if "/" in repo:
        owner, name = repo.split("/", 1)
        return f"https://{owner}.github.io/{name}"
    return (os.getenv("PAGES_BASE","").strip().rstrip("/"))
PAGES_BASE = pages_base()

# ===== UTILS =====
def norm(s): return re.sub(r"\s+"," ", (s or "").strip())
def normalize_article(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[\s\-_]+","", s)
    s = s.replace("—","").replace("–","")
    return s.upper()

HEADER_HINTS = {
    "name":        ["номенклатура","наименование","название","товар","описание"],
    "article":     ["артикул","номенклатура.артикул","sku","код товара","код","модель","part number","pn","p/n"],
    "availability":["остаток","наличие","кол-во","количество","qty","stock","остатки"],
    "price":       ["цена","опт цена","опт. цена","розница","стоимость","цена, тг","цена тг","retail","опт"],
    "unit":        ["ед.","ед","единица"],
    "category":    ["категория","раздел","группа","тип"],
}

def ensure_files():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    if not os.path.exists(CATS_FILE):
        with open(CATS_FILE, "w", encoding="utf-8") as f:
            f.write("# Паттерны-ВКЛЮЧЕНИЯ (если пусто — берём всё)\n")
    os.makedirs(MIRROR_DIR, exist_ok=True)

def load_patterns(path):
    subs, regs = [], []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s=line.strip()
            if not s or s.startswith("#"): continue
            if s.lower().startswith("re:"):
                try: regs.append(re.compile(s[3:], re.I))
                except Exception: pass
            else:
                subs.append(s.lower())
    return subs, regs

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=UA_HEADERS, timeout=60)
    r.raise_for_status()
    return r

def fetch_xlsx(url: str) -> bytes:
    return fetch(url).content

# ===== HEADER PARSING =====
def best_header(ws):
    def score(row_vals):
        low = [norm(x).lower() for x in row_vals]
        matched=set()
        for key,hints in HEADER_HINTS.items():
            for cell in low:
                if any(h in cell for h in hints):
                    matched.add(key); break
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
        m=max(len(a),len(b))
        merged=[]
        for j in range(m):
            x=a[j] if j<len(a) else ""
            y=b[j] if j<len(b) else ""
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

def pass_filters(cat: str, sheet: str, name: str, subs, regs) -> bool:
    if not subs and not regs: return True
    hay = (cat.lower(), sheet.lower(), name.lower())
    if any(sub in h for sub in subs for h in hay): return True
    if any(r.search(h) for r in regs for h in hay): return True
    return False

def hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6],16)

def slug(s: str, maxlen: int = 60) -> str:
    s = re.sub(r"[^a-z0-9]+","-", s.lower()).strip("-")
    return s[:maxlen] if len(s)>maxlen else s

# ===== IMAGE & URL EXTRACTION =====
ARTICLE_RE = re.compile(r"(?:артикул|код|sku|модель|pn|p/n)\s*[:#\-]?\s*([A-Za-z0-9\-\._/]+)", re.I)
DIGITS_RE  = re.compile(r"\b\d{4,}\b")
BAD_IMG_PAT = re.compile(r"(no[-_ ]?photo|placeholder|spacer|logo|blank|stub|1x1|pixel)", re.I)

def extract_article_from_product_page(soup: BeautifulSoup) -> str|None:
    sku = soup.select_one('[itemprop="sku"]')
    if sku and sku.get_text(strip=True):
        return normalize_article(sku.get_text(strip=True))
    txt = soup.get_text(" ", strip=True)
    for m in ARTICLE_RE.finditer(txt):
        return normalize_article(m.group(1))
    for m in DIGITS_RE.finditer(txt):
        return normalize_article(m.group(0))
    return None

def looks_bad_image(url: str) -> bool:
    if BAD_IMG_PAT.search(url): return True
    if url.lower().startswith("data:"): return True
    return False

def ok_by_head(url: str) -> bool:
    if not CHECK_IMAGE_BYTES: return True
    try:
        h = requests.head(url, headers=UA_HEADERS, timeout=20, allow_redirects=True)
        ct = (h.headers.get("Content-Type") or "").lower()
        if "image" not in ct: return False
        cl = h.headers.get("Content-Length")
        if cl is None: return True
        return int(cl) >= MIN_IMAGE_BYTES
    except Exception:
        return True  # не блокируем, если HEAD не дался

def extract_main_image_url(soup: BeautifulSoup, base_url: str) -> str|None:
    """
    БЕРЁМ РОВНО ОДНО ФОТО:
    1) <img itemprop="image"> или id="main_image_*"
    2) если нет — <meta property="og:image">
    Фильтр: путь должен содержать 'img_products' (как в твоём образце).
    """
    # 1) itemprop="image"
    img = soup.select_one('img[itemprop="image"]')
    if not img:
        img = soup.select_one('img[id^="main_image_"]')
    if img:
        src = img.get("data-src") or img.get("data-original") or img.get("src")
        if src:
            u = urljoin(base_url, src)
            if "img_products" in u and not looks_bad_image(u) and ok_by_head(u):
                return u

    # 2) og:image
    meta = soup.select_one('meta[property="og:image"], meta[name="og:image"]')
    if meta:
        u = (meta.get("content") or "").strip()
        if u:
            u = urljoin(base_url, u)
            if "img_products" in u and not looks_bad_image(u) and ok_by_head(u):
                return u

    return None

def search_product_url_by_article(article: str) -> str|None:
    q1 = f"{SITE_ORIGIN}/search/?{urlencode({'q': article})}"
    q2 = f"{SITE_ORIGIN}/?s={article}"
    for qurl in (q1, q2):
        try:
            r = fetch(qurl)
        except Exception:
            continue
        time.sleep(CRAWL_DELAY)
        soup = BeautifulSoup(r.text, "lxml")
        a = soup.select_one('a[href*="/goods/"][href$=".html"]')
        if a and a.get("href"):
            u = urljoin(qurl, a.get("href"))
            if urlparse(u).netloc == urlparse(SITE_ORIGIN).netloc:
                return u
    return None

# ===== CACHE =====
def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE,"r",encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache: dict):
    tmp = CACHE_FILE + ".tmp"
    with open(tmp,"w",encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CACHE_FILE)

def row_signature(name, price, qty, category) -> str:
    base = f"{norm(name)}|{price or ''}|{qty or ''}|{norm(category)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def mirror_one(url: str, art: str, idx: int) -> str|None:
    if not MIRROR_IMAGES or not PAGES_BASE:
        return None
    try:
        r = requests.get(url, headers=UA_HEADERS, timeout=60, stream=True)
        r.raise_for_status()
        ext = pathlib.Path(urlparse(url).path).suffix.lower()
        if not ext:
            ext = mimetypes.guess_extension(r.headers.get("Content-Type","").split(";")[0].strip() or "") or ".jpg"
        safe_art = slug(art.lower(), maxlen=80)
        d = pathlib.Path(MIRROR_DIR) / safe_art
        d.mkdir(parents=True, exist_ok=True)
        out = d / f"{idx}{ext}"
        with open(out, "wb") as f:
            for chunk in r.iter_content(65536):
                if chunk: f.write(chunk)
        rel = str(out).replace("docs/","").lstrip("/")
        return f"{PAGES_BASE}/{rel}"
    except Exception:
        return None

# ===== YML =====
ROOT_CAT_ID = "9300000"
def cat_id_for(name: str) -> str:
    return str(9300001 + (hash_int(name.lower()) % 400000))

def make_offer_id(it: dict) -> str:
    art = (it.get("article") or "").strip()
    if art: return f"copyline:{art}"
    base = slug(it.get("name",""))
    h = hashlib.md5((it.get("name","").lower()+"|"+(it.get("category") or "").lower()).encode("utf-8")).hexdigest()[:8]
    return f"copyline:{base}:{h}"

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
    used_ids=set()
    for it in items:
        oid = make_offer_id(it)
        if oid in used_ids:
            extra = hashlib.md5((it.get("name","")+str(it.get("price"))+(it.get("qty") or "")).encode("utf-8")).hexdigest()[:6]
            oid = f"{oid}-{extra}"
            i=2
            while oid in used_ids:
                oid=f"{oid}-{i}"; i+=1
        used_ids.add(oid)

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
        if it.get("url"): SubElement(o,"url").text = it["url"]
        if it.get("brand"): SubElement(o,"vendor").text=it["brand"]
        if it.get("article"): SubElement(o,"vendorCode").text=it["article"]
        q = it.get("qty") or ("1" if it["available"] else "0")
        for tag in ("quantity_in_stock","stock_quantity","quantity"): SubElement(o,tag).text=q
        # РОВНО ОДНО ФОТО
        pic = None
        if it.get("mirrored_images"):
            pic = it["mirrored_images"][0]
        elif it.get("images"):
            pic = it["images"][0]
        if pic:
            SubElement(o,"picture").text = pic

    buf=io.BytesIO(); ElementTree(root).write(buf, encoding=ENC, xml_declaration=True); return buf.getvalue()

# ===== MAIN =====
def main():
    ensure_files()
    subs, regs = load_patterns(CATS_FILE)

    # 1) XLSX → items
    data = fetch_xlsx(XLSX_URL)
    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)

    raw_items=[]
    seen_cats=Counter(); seen_sheets=Counter()

    def map_cols(ws):
        headers, data_start = best_header(ws)
        if not headers: return None, None
        cols = map_columns(headers)
        if cols.get("name") is None: return None, None
        return cols, data_start

    for ws in wb.worksheets:
        cols, data_start = map_cols(ws)
        if not cols:
            seen_sheets[ws.title]+=0; continue
        current_cat=None
        for row in ws.iter_rows(min_row=data_start, values_only=True):
            row=list(row)
            def getc(idx): return None if idx is None or idx>=len(row) else row[idx]
            name    = norm(getc(cols.get("name")))
            article = norm(getc(cols.get("article")))
            price   = parse_price(getc(cols.get("price")))
            avail, qty = is_available(getc(cols.get("availability")))
            raw_cat = norm(getc(cols.get("category"))) if cols.get("category") is not None else ""

            # Заголовок секции
            if name and not article and price is None:
                if len(name)>3 and not re.search(r"^(цены|прайс|тенге|лист|итог)", name.lower()):
                    current_cat=name; seen_cats[current_cat]+=0
                    continue

            if not name and not article: continue

            cat = raw_cat or current_cat or norm(ws.title)
            if not pass_filters(cat, ws.title, name or article, subs, regs): continue

            raw_items.append({
                "article": article,
                "name": name or article,
                "brand": "",
                "category": cat,
                "price": price,
                "available": avail,
                "qty": qty,
                "url": "",
            })
            seen_cats[cat]+=1; seen_sheets[ws.title]+=1

    # 2) Дедуп
    dedup={}
    for it in raw_items:
        key = ("a", it["article"]) if it["article"] else ("n", it["name"].lower(), (it["category"] or "").lower())
        if key in dedup:
            old = dedup[key]
            better = it if (it["price"] and not old["price"]) or (it["available"] and not old["available"]) else old
            dedup[key] = better
        else:
            dedup[key] = it
    items = list(dedup.values())

    # 3) КЭШ
    cache = load_cache()
    if "items" not in cache: cache["items"] = {}
    cache_changed = False

    # 4) Дельта для поиска
    to_fetch = []
    for it in items:
        art = normalize_article(it.get("article") or "")
        if not art: continue
        sig = row_signature(it.get("name"), it.get("price"), it.get("qty"), it.get("category"))
        entry = cache["items"].get(art)
        need = False
        if not entry:
            need = True
        else:
            if entry.get("row_sig") != sig:
                need = True
            if FORCE_REFETCH_NO_PHOTO and not entry.get("images"):
                need = True
            if not entry.get("source_url"):
                need = True
        if need:
            to_fetch.append((art, it))
        else:
            it["images"] = entry.get("images", [])
            it["mirrored_images"] = entry.get("mirrored_images", [])
            it["url"] = entry.get("source_url", "")

    # 5) Поиск карточки + РОВНО одно фото
    fetched_count = 0
    for art, it in to_fetch[:MAX_SEARCH]:
        prod_url = search_product_url_by_article(art)
        img_url, mirrored = None, []
        if prod_url:
            try:
                r = fetch(prod_url)
                time.sleep(CRAWL_DELAY)
                soup = BeautifulSoup(r.text, "lxml")
                art2 = extract_article_from_product_page(soup)
                if not STRICT_IMAGE_MATCH or (art2 == art):
                    img_url = extract_main_image_url(soup, prod_url)  # ← берём 1 фото
                    it["url"] = prod_url
            except Exception:
                pass

        # зеркалим 1 фото (если есть)
        if img_url and MIRROR_IMAGES and PAGES_BASE:
            m = mirror_one(img_url, art, 1)
            if m: mirrored = [m]

        # обновляем кэш
        sig = row_signature(it.get("name"), it.get("price"), it.get("qty"), it.get("category"))
        cache["items"][art] = {
            "row_sig": sig,
            "images": [img_url] if img_url else [],
            "mirrored_images": mirrored,
            "source_url": it.get("url","") or (prod_url or ""),
            "updated_at": int(time.time())
        }
        it["images"] = [img_url] if img_url else []
        it["mirrored_images"] = mirrored
        cache_changed = True
        fetched_count += 1

    # 6) Отчёт
    with open(SEEN_FILE,"w",encoding="utf-8") as f:
        with_pic = sum(1 for it in items if (it.get('mirrored_images') or it.get('images')))
        with_urls = sum(1 for it in items if it.get('url'))
        f.write("=== CATEGORIES (from XLSX) ===\n")
        for k,v in seen_cats.most_common(300):
            f.write(f"{k}\t{v}\n")
        f.write(f"\nTotal items: {len(items)} | Fetched this run: {fetched_count} | With 1 photo now: {with_pic} | With URL: {with_urls}\n")
        f.write(f"Delta: MAX_SEARCH={MAX_SEARCH}, delay={CRAWL_DELAY}s | CHECK_IMAGE_BYTES={CHECK_IMAGE_BYTES} MIN_IMAGE_BYTES={MIN_IMAGE_BYTES}\n")

    if cache_changed:
        save_cache(cache)

    # 7) YML
    yml = build_yml(items)
    with open(OUT_FILE,"wb") as f: f.write(yml)
    print(f"{OUT_FILE}: {len(items)} items | fetched_delta={fetched_count}")
    print(f"Seen → {SEEN_FILE} | Cache → {CACHE_FILE}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
