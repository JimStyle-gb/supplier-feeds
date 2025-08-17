# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, time, hashlib, requests
from collections import Counter, deque
from urllib.parse import urljoin, urlparse, urlencode
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ====== ENV ======
XLSX_URL     = os.getenv("XLSX_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE     = os.getenv("OUT_FILE",  "docs/copyline.yml")
ENC          = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
CATS_FILE    = os.getenv("CATEGORIES_FILE", "docs/categories_copyline.txt")
SEEN_FILE    = os.getenv("SEEN_FILE", "docs/copyline_seen.txt")

# авто-поиск категорий/товаров и сбор фото
AUTO_DISCOVER        = (os.getenv("AUTO_DISCOVER") or "1").strip().lower() in {"1","true","yes","on"}
FOLLOW_PRODUCT_PAGES = (os.getenv("FOLLOW_PRODUCT_PAGES") or "1").strip().lower() in {"1","true","yes","on"}
STRICT_IMAGE_MATCH   = (os.getenv("STRICT_IMAGE_MATCH") or "1").strip().lower() in {"1","true","yes","on"}
SEARCH_FALLBACK      = (os.getenv("SEARCH_FALLBACK") or "1").strip().lower() in {"1","true","yes","on"}

MAX_PAGES     = int(os.getenv("MAX_PAGES", "600"))       # максимум страниц категорий
MAX_PRODUCTS  = int(os.getenv("MAX_PRODUCTS", "3000"))   # максимум карточек для захода внутрь
MAX_SEARCH    = int(os.getenv("MAX_SEARCH", "2000"))     # максимум артикулов для поиска
CRAWL_DELAY   = float(os.getenv("CRAWL_DELAY", "0.6"))   # пауза между запросами

UA_HEADERS    = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
SITE_ORIGIN   = "https://copyline.kz"

# ====== UTILS ======
def norm(s): return re.sub(r"\s+"," ", (s or "").strip())
def domain(u): return urlparse(u).netloc

def normalize_article(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[\s\-_]+","", s)             # убрать пробелы/дефисы/подчёркивания
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
    # фильтр категорий (может быть пустым)
    if not os.path.exists(CATS_FILE):
        with open(CATS_FILE, "w", encoding="utf-8") as f:
            f.write("# Паттерны-ВКЛЮЧЕНИЯ (если пусто — берём всё)\n")

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

# ====== HEADER PARSING (двухстрочная шапка) ======
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

def slug(s: str, maxlen: int = 40) -> str:
    s = re.sub(r"[^a-z0-9]+","-", s.lower()).strip("-")
    return s[:maxlen]

ROOT_CAT_ID = "9300000"
def cat_id_for(name: str) -> str:
    return str(9300001 + (hash_int(name.lower()) % 400000))

def make_offer_id(it: dict) -> str:
    art = (it.get("article") or "").strip()
    if art: return f"copyline:{art}"
    base = slug(it.get("name",""))
    h = hashlib.md5((it.get("name","").lower()+"|"+(it.get("category") or "").lower()).encode("utf-8")).hexdigest()[:8]
    return f"copyline:{base}:{h}"

# ====== IMAGE SCRAPING (ТОЛЬКО ПРИ ТОЧНОМ СОВПАДЕНИИ АРТИКУЛА) ======
ARTICLE_RE = re.compile(r"(?:артикул|код|sku|модель|pn|p/n)\s*[:#\-]?\s*([A-Za-z0-9\-\._/]+)", re.I)
DIGITS_RE  = re.compile(r"\b\d{4,}\b")

def extract_article_candidates(text: str):
    out=set()
    for m in ARTICLE_RE.finditer(text): out.add(m.group(1))
    for m in DIGITS_RE.finditer(text):  out.add(m.group(0))
    return out

def pick_img_src(img):
    for attr in ("data-src","data-original","src"):
        val = img.get(attr)
        if val: return val
    return None

def extract_article_from_product_page(soup: BeautifulSoup) -> str|None:
    # 1) itemprop=sku, или явный текст "Артикул"
    sku = soup.select_one('[itemprop="sku"]')
    if sku and sku.get_text(strip=True):
        return normalize_article(sku.get_text(strip=True))
    txt = soup.get_text(" ", strip=True)
    cands = extract_article_candidates(txt)
    for c in cands:
        return normalize_article(c)
    return None

def extract_images_from_product_page(soup: BeautifulSoup, base_url: str) -> list[str]:
    urls=[]
    for img in soup.find_all("img"):
        src = pick_img_src(img)
        if not src: continue
        u = urljoin(base_url, src)
        if u not in urls:
            urls.append(u)
        if len(urls) >= 8: break
    return urls

def discover_category_urls() -> list[str]:
    urls=set()
    # sitemap
    for sm in (f"{SITE_ORIGIN}/sitemap.xml", f"{SITE_ORIGIN}/sitemap_index.xml"):
        try:
            r = fetch(sm)
            soup = BeautifulSoup(r.text, "xml")
            for loc in soup.find_all("loc"):
                u = (loc.text or "").strip()
                if "/goods/" in u and domain(u) == domain(SITE_ORIGIN):
                    urls.add(u)
        except Exception:
            pass
    # стартовая /goods/
    try:
        r = fetch(f"{SITE_ORIGIN}/goods/")
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select('a[href*="/goods/"]'):
            href = a.get("href") or ""
            if not href: continue
            u = urljoin(SITE_ORIGIN, href)
            if domain(u) != domain(SITE_ORIGIN): continue
            if "/goods/" in u: urls.add(u)
    except Exception:
        pass
    return sorted(urls)

def crawl_categories_and_products(seed_urls: list[str], max_pages=400, delay=0.6, max_products=2000):
    """Собираем ссылки на товарные страницы из листингов."""
    seen=set(); q=deque(); product_links=set(); pages=0
    for u in seed_urls:
        if "/goods/" in u and domain(u) == domain(SITE_ORIGIN):
            q.append(u)

    while q and pages < max_pages and len(product_links) < max_products:
        url = q.popleft()
        if url in seen: continue
        seen.add(url); pages += 1
        try:
            r = fetch(url)
        except Exception:
            continue
        time.sleep(delay)
        soup = BeautifulSoup(r.text, "lxml")

        # товарные ссылки (заканчиваются .html)
        for a in soup.select('a[href*="/goods/"]'):
            href = a.get("href") or ""
            if not href: continue
            u = urljoin(url, href)
            if domain(u) != domain(SITE_ORIGIN): continue
            if re.search(r"/goods/.+\.html?$", u, re.I):
                product_links.add(u)
                if len(product_links) >= max_products: break

        # пагинация
        for link in soup.find_all("a", href=True):
            href = link["href"]
            u = urljoin(url, href)
            if domain(u) != domain(SITE_ORIGIN): continue
            if "/goods/" not in u: continue
            if u not in seen and (re.search(r"page|PAGEN|PAGEN_1|\d", u, re.I) or any(x in (link.get_text() or "").lower() for x in ("след", "далее", "next"))):
                q.append(u)

    return sorted(product_links)

def search_product_urls_by_articles(articles: list[str], limit=1000, delay=0.6) -> dict[str,str]:
    """Ищем страницы товара по артикулу (поиск сайта). Возвращает map: article_norm -> product_url."""
    found={}
    cnt=0
    for art in articles:
        if cnt >= limit: break
        q1 = f"{SITE_ORIGIN}/search/?{urlencode({'q': art})}"
        q2 = f"{SITE_ORIGIN}/?s={art}"
        for qurl in (q1, q2):
            try:
                r = fetch(qurl)
            except Exception:
                continue
            time.sleep(delay)
            soup = BeautifulSoup(r.text, "lxml")
            a = soup.select_one('a[href*="/goods/"][href$=".html"]')
            if a and a.get("href"):
                u = urljoin(qurl, a.get("href"))
                if domain(u) == domain(SITE_ORIGIN):
                    found[normalize_article(art)] = u
                    break
        cnt += 1
    return found

# ====== BUILD YML ======
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
        if it.get("brand"): SubElement(o,"vendor").text=it["brand"]
        if it.get("article"): SubElement(o,"vendorCode").text=it["article"]
        q = it.get("qty") or ("1" if it["available"] else "0")
        for tag in ("quantity_in_stock","stock_quantity","quantity"): SubElement(o,tag).text=q
        for p in it.get("images", []):
            SubElement(o,"picture").text = p

    buf=io.BytesIO(); ElementTree(root).write(buf, encoding=ENC, xml_declaration=True); return buf.getvalue()

# ====== MAIN ======
def main():
    ensure_files()
    subs, regs = load_patterns(CATS_FILE)

    # 1) XLSX → items
    data = fetch_xlsx(XLSX_URL)
    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)

    raw_items=[]
    seen_cats=Counter(); seen_sheets=Counter()

    for ws in wb.worksheets:
        headers, data_start = best_header(ws)
        if not headers:
            seen_sheets[ws.title]+=0; continue
        cols = map_columns(headers)
        if cols.get("name") is None:
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
            seen_cats[cat]+=1; seen_sheets[ws.title]+=1

            if not pass_filters(cat, ws.title, name or article, subs, regs): continue

            raw_items.append({
                "article": article,
                "name": name or article,
                "brand": "",
                "category": cat,
                "price": price,
                "available": avail,
                "qty": qty,
            })

    # 2) Дедуп по артикулу/имени+категории
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

    # 3) Сбор фото (строго по артикулу)
    target_articles = [normalize_article(it["article"]) for it in items if it.get("article")]
    target_set = set(target_articles)

    images_by_article: dict[str, list[str]] = {}

    if AUTO_DISCOVER:
        seeds = discover_category_urls()
        product_links = []
        if FOLLOW_PRODUCT_PAGES and seeds:
            product_links = crawl_categories_and_products(seeds, max_pages=MAX_PAGES, delay=CRAWL_DELAY, max_products=MAX_PRODUCTS)
        # обходим страницы товаров
        visited=0
        for purl in product_links:
            if visited >= MAX_PRODUCTS: break
            try:
                r = fetch(purl)
            except Exception:
                continue
            time.sleep(CRAWL_DELAY)
            visited += 1
            soup = BeautifulSoup(r.text, "lxml")
            art = extract_article_from_product_page(soup)
            if not art:
                continue
            if art not in target_set and STRICT_IMAGE_MATCH:
                continue
            imgs = extract_images_from_product_page(soup, purl)
            if imgs:
                images_by_article.setdefault(art, [])
                for s in imgs:
                    if s not in images_by_article[art] and len(images_by_article[art]) < 8:
                        images_by_article[art].append(s)

    # Поиск по сайту для тех, у кого ещё нет фото
    if SEARCH_FALLBACK and target_set:
        need = [a for a in target_articles if a not in images_by_article]
        need = need[:MAX_SEARCH]
        url_by_art = search_product_urls_by_articles(need, limit=MAX_SEARCH, delay=CRAWL_DELAY)
        for art, purl in url_by_art.items():
            try:
                r = fetch(purl)
            except Exception:
                continue
            time.sleep(CRAWL_DELAY)
            soup = BeautifulSoup(r.text, "lxml")
            art2 = extract_article_from_product_page(soup) or art
            if STRICT_IMAGE_MATCH and art2 != art:
                continue
            imgs = extract_images_from_product_page(soup, purl)
            if imgs:
                images_by_article.setdefault(art, [])
                for s in imgs:
                    if s not in images_by_article[art] and len(images_by_article[art]) < 8:
                        images_by_article[art].append(s)

    # Присвоим фото к items
    for it in items:
        art = normalize_article(it.get("article") or "")
        it["images"] = images_by_article.get(art, [])

    # 4) Отчёт
    with open(SEEN_FILE,"w",encoding="utf-8") as f:
        f.write("=== CATEGORIES (inferred from XLSX) ===\n")
        for k,v in seen_cats.most_common(500):
            f.write(f"{k}\t{v}\n")
        got_photos = sum(1 for it in items if it.get('images'))
        f.write(f"\nRaw items: {len(raw_items)} | After dedup: {len(items)} | With photos: {got_photos}\n")
        f.write(f"AUTO_DISCOVER={AUTO_DISCOVER} FOLLOW_PRODUCT_PAGES={FOLLOW_PRODUCT_PAGES} STRICT_IMAGE_MATCH={STRICT_IMAGE_MATCH} SEARCH_FALLBACK={SEARCH_FALLBACK}\n")

    # 5) Выгрузка
    yml = build_yml(items)
    with open(OUT_FILE,"wb") as f: f.write(yml)
    print(f"{OUT_FILE}: {len(items)} items")
    print(f"Seen → {SEEN_FILE}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
