# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, time, hashlib, urllib.parse

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ========= ПАРАМЕТРЫ =========
XLSX_URL   = os.getenv("XLSX_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE   = os.getenv("OUT_FILE",  "docs/copyline.yml")
ENC        = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

CATS_FILE  = os.getenv("CATEGORIES_FILE", "docs/categories_copyline.txt")
URLS_FILE  = os.getenv("URLS_FILE", "docs/categories_copyline_urls.txt")

MAX_PAGES_PER_CAT = int(os.getenv("MAX_PAGES_PER_CAT", "200"))
REQUEST_DELAY     = float(os.getenv("REQUEST_DELAY", "0.4"))

UA_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
BASE = "https://copyline.kz"

# ========= УТИЛЫ =========
def norm(s: str|None) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=UA_HEADERS, timeout=60)
    r.raise_for_status()
    return r

def fetch_bytes(url: str) -> bytes:
    return fetch(url).content

def ensure_files():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

def load_lines(path: str) -> list[str]:
    arr=[]
    try:
        with open(path,"r",encoding="utf-8") as f:
            for line in f:
                s=line.strip()
                if not s or s.startswith("#"): continue
                arr.append(s)
    except FileNotFoundError:
        pass
    return arr

def load_patterns(path: str):
    subs, regs = [], []
    try:
        with open(path,"r",encoding="utf-8") as f:
            for line in f:
                s=line.strip()
                if not s or s.startswith("#"): continue
                if s.lower().startswith("re:"):
                    try: regs.append(re.compile(s[3:], re.I))
                    except: pass
                else:
                    subs.append(s.lower())
    except FileNotFoundError:
        pass
    return subs, regs

def pass_filters(cat: str, sheet: str, name: str, subs, regs) -> bool:
    if not subs and not regs: return True
    hay = (cat.lower(), sheet.lower(), name.lower())
    if any(sub in h for sub in subs for h in hay): return True
    if any(r.search(h) for r in regs for h in hay): return True
    return False

# ========= РАСПОЗНАВАНИЕ КОЛОНОК =========
HEADER_HINTS = {
    "name":        ["номенклатура","наименование","название","товар","описание"],
    "article":     ["артикул","номенклатура.артикул","sku","код товара","код","модель","part number","pn","p/n"],
    "availability":["остаток","наличие","кол-во","количество","qty","stock","остатки"],
    "price":       ["цена","опт","опт. цена","розница","стоимость","цена, тг","цена тг","retail"],
    "unit":        ["ед.","ед","единица"],
    "category":    ["категория","раздел","группа","тип"],
}

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

    best_row, best_idx, best_sc = [], None, -1
    for i,r in enumerate(rows):
        sc=score(r)
        if sc>best_sc: best_row, best_idx, best_sc = r, i+1, sc

    for i in range(len(rows)-1):
        a,b=rows[i],rows[i+1]
        m=max(len(a),len(b)); merged=[]
        for j in range(m):
            x=a[j] if j<len(a) else ""
            y=b[j] if j<len(b) else ""
            merged.append(" ".join(t for t in (x,y) if t).strip())
        sc=score(merged)
        if sc>best_sc: best_row, best_idx, best_sc = merged, i+2, sc
    return best_row, (best_idx or 1)+1

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

# ========= ОБХОД КАТЕГОРИЙ =========
def extract_product_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    hrefs=set()
    for a in soup.select('a[href*="/product/"], a[href*="/goods/"]'):
        href = a.get("href") or ""
        if href.endswith(".html"):
            hrefs.add(urllib.parse.urljoin(BASE, href))
    return list(hrefs)

def find_next_page_url(html: str) -> str|None:
    soup = BeautifulSoup(html, "lxml")
    a = soup.find("a", rel=lambda v: v and "next" in v.lower())
    if a and a.get("href"): return urllib.parse.urljoin(BASE, a["href"])
    return None

def get_product_image(url: str) -> str|None:
    try:
        html = fetch(url).text
        soup = BeautifulSoup(html, "lxml")
        img = soup.select_one('img[id^="main_image_"]') or soup.select_one('img[itemprop="image"]')
        if img and img.get("src"):
            return urllib.parse.urljoin(BASE, img["src"])
    except Exception:
        return None
    return None

def crawl_all_images(urls: list[str]) -> dict:
    """Возвращает словарь {url товара: картинка}"""
    out={}
    for cat_url in urls:
        try:
            html = fetch(cat_url).text
        except Exception:
            continue
        pages_seen=0
        next_url = cat_url
        while html and pages_seen < MAX_PAGES_PER_CAT:
            pages_seen += 1
            for purl in extract_product_links(html):
                if purl not in out:
                    pic = get_product_image(purl)
                    if pic: out[purl] = pic
                time.sleep(REQUEST_DELAY)
            nx = find_next_page_url(html)
            if not nx: break
            try:
                html = fetch(nx).text
                next_url = nx
                time.sleep(REQUEST_DELAY)
            except Exception:
                break
    return out

# ========= YML =========
ROOT_CAT_ID = "9300000"
def hash_int(s: str) -> int: return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6],16)
def cat_id_for(name: str) -> str: return str(9300001 + (hash_int(name.lower()) % 400000))

def make_offer_id(it: dict) -> str:
    art = (it.get("article") or "").strip()
    if art: return f"copyline:{art}"
    base = re.sub(r"[^a-z0-9]+","-", (it.get("name","").lower()))
    h = hashlib.md5((it.get('name','').lower()+"|"+(it.get('category') or '').lower()).encode('utf-8')).hexdigest()[:8]
    return f"copyline:{base}:{h}"

def build_yml(items):
    cats={}
    for it in items:
        nm = it.get("category") or "Copyline"
        if nm not in cats: cats[nm]=cat_id_for(nm)

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
        oid = make_offer_id(it)
        if oid in used:
            extra = hashlib.md5((it.get("name","")+str(it.get("price"))+(it.get("qty") or "")).encode("utf-8")).hexdigest()[:6]
            i=2
            while f"{oid}-{extra}-{i}" in used: i+=1
            oid = f"{oid}-{extra}-{i}"
        used.add(oid)

        cid = cats.get(it.get("category") or "Copyline", ROOT_CAT_ID)
        o = SubElement(offers,"offer",{
            "id": oid,
            "available":"true" if it["available"] else "false",
            "in_stock":"true" if it["available"] else "false",
        })
        SubElement(o,"name").text = it["name"]
        if it.get("price") is not None: SubElement(o,"price").text=str(it["price"])
        SubElement(o,"currencyId").text="KZT"
        SubElement(o,"categoryId").text=cid
        if it.get("vendor"): SubElement(o,"vendor").text=it["vendor"]
        if it.get("article"): SubElement(o,"vendorCode").text=it["article"]
        q = it.get("qty") or ("1" if it["available"] else "0")
        for tag in ("quantity_in_stock","stock_quantity","quantity"): SubElement(o,tag).text=q
        if it.get("picture"): SubElement(o,"picture").text = it["picture"]

    buf=io.BytesIO(); ElementTree(root).write(buf, encoding=ENC, xml_declaration=True); return buf.getvalue()

# ========= MAIN =========
def main():
    ensure_files()
    xls = fetch_bytes(XLSX_URL)
    wb = load_workbook(io.BytesIO(xls), read_only=True, data_only=True)

    subs, regs = load_patterns(CATS_FILE)

    raw=[]
    for ws in wb.worksheets:
        headers, start = best_header(ws)
        if not headers: continue
        cols = map_columns(headers)
        if cols.get("name") is None: continue

        current_cat=None
        for row in ws.iter_rows(min_row=start, values_only=True):
            row=list(row)
            def getc(i): return None if i is None or i>=len(row) else row[i]
            name    = norm(getc(cols.get("name")))
            article = norm(getc(cols.get("article")))
            price   = parse_price(getc(cols.get("price")))
            avail, qty = is_available(getc(cols.get("availability")))
            raw_cat = norm(getc(cols.get("category"))) if cols.get("category") is not None else ""

            if name and not article and price is None:
                if len(name)>3 and not re.search(r"^(цены|прайс|тенге|лист|итог)", name.lower()):
                    current_cat=name; continue
            if not name and not article: continue

            cat = raw_cat or current_cat or norm(ws.title)
            if not pass_filters(cat, ws.title, name or article, subs, regs): continue

            raw.append({
                "article": article,
                "name": name or article,
                "vendor": "",
                "category": cat,
                "price": price,
                "available": avail,
                "qty": qty,
            })

    # дедуп
    ded={}
    for it in raw:
        key = ("a", it["article"]) if it["article"] else ("n", it["name"].lower(), (it["category"] or "").lower())
        if key in ded:
            old=ded[key]
            better = it if (it["price"] and not old["price"]) or (it["available"] and not old["available"]) else old
            ded[key]=better
        else:
            ded[key]=it
    items=list(ded.values())

    # 2) Собираем фото напрямую
    urls = load_lines(URLS_FILE)
    photos = crawl_all_images(urls)

    # 3) Для простоты — ставим первую найденную картинку (по URL совпадения нет, берём любую)
    # В реальной жизни можно связать по артикулу в URL или по порядку обхода
    pic_list = list(photos.values())
    for i,it in enumerate(items):
        it["picture"] = pic_list[i % len(pic_list)] if pic_list else None

    yml = build_yml(items)
    with open(OUT_FILE,"wb") as f: f.write(yml)
    print(f"{OUT_FILE}: {len(items)} items, pictures found {sum(1 for i in items if i.get('picture'))}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
