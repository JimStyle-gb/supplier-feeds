# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, time, hashlib
from typing import Optional, Any, List, Dict
import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ================= НАСТРОЙКИ =================
XLSX_URL     = os.getenv("XLSX_URL", "https://copyline.kz/files/price-CLA.xlsx")  # публичная ссылка ИЛИ путь к файлу
OUT_FILE     = os.getenv("OUT_FILE", "docs/copyline.yml")                          # куда писать YML
ENC          = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()            # кодировка yml
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.15"))                          # пауза между запросами
MAX_ROWS      = int(os.getenv("MAX_ROWS", "0"))                                    # 0 = без лимита

UA_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ================= УТИЛЫ =================
def ensure_outdir():
    d = os.path.dirname(OUT_FILE)
    if d: os.makedirs(d, exist_ok=True)

def fetch_bytes(url_or_path: str) -> bytes:
    if re.match(r"^https?://", url_or_path, re.I):
        r = requests.get(url_or_path, headers=UA_HEADERS, timeout=60)
        r.raise_for_status()
        return r.content
    with open(url_or_path, "rb") as f:
        return f.read()

def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def url_or_none(v: Any) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    if s.startswith("http://") or s.startswith("https://"):
        return s
    return None

# ================= ПРАЙС: колонки =================
HEADER_HINTS = {
    "name":        ["номенклатура","наименование","название","товар","описание"],
    "article":     ["артикул","номенклатура.артикул","sku","код товара","код","модель","part number","pn","p/n"],
    "availability":["остаток","наличие","кол-во","количество","qty","stock","остатки"],
    "price":       ["цена","опт","опт. цена","розница","стоимость","цена, тг","цена тг","retail"],
    "unit":        ["ед.","ед","единица"],
    "category":    ["категория","раздел","группа","тип"],
    "url":         ["url","ссылка","ссылка на товар","product url","page url","страница","товар url"],
}

def best_header(ws):
    def score(row_vals):
        low = [norm("" if x is None else str(x)).lower() for x in row_vals]
        matched=set()
        for key,hints in HEADER_HINTS.items():
            for cell in low:
                if any(h in cell for h in hints):
                    matched.add(key); break
        return len(matched)

    rows=[]
    for row in ws.iter_rows(min_row=1, max_row=40, values_only=True):
        rows.append([("" if v is None else str(v)) for v in row])

    best_row, best_idx, best_sc = [], None, -1
    for i,r in enumerate(rows):
        sc=score(r)
        if sc>best_sc: best_row, best_idx, best_sc = r, i+1, sc

    # если шапка в 2 строки — пробуем слить
    for i in range(len(rows)-1):
        a,b=rows[i],rows[i+1]
        m=max(len(a),len(b)); merged=[]
        for j in range(m):
            x=a[j] if j<len(a) else ""
            y=b[j] if j<len(b) else ""
            merged.append(" ".join(t for t in (x,y) if t).strip())
        sc=score(merged)
        if sc>best_sc: best_row, best_idx, best_sc = merged, i+2, sc

    return best_row, (best_idx or 1)+1  # данные начинаются со следующей строки

def map_columns(headers):
    cols={}
    low=[norm(h).lower() for h in headers]
    def find_any(keys):
        for i,cell in enumerate(low):
            if any(k in cell for k in keys):
                return i
        return None
    cols["name"]        = find_any(HEADER_HINTS["name"])
    cols["article"]     = find_any(HEADER_HINTS["article"])
    cols["availability"]= find_any(HEADER_HINTS["availability"])
    cols["price"]       = find_any(HEADER_HINTS["price"])
    cols["unit"]        = find_any(HEADER_HINTS["unit"])
    cols["category"]    = find_any(HEADER_HINTS["category"])
    cols["url"]         = find_any(HEADER_HINTS["url"])  # нужна колонка со ссылкой на карточку
    return cols

def parse_price(v) -> Optional[int]:
    if v is None: return None
    t = norm(str(v)).replace("₸","").replace("тг","")
    digits = re.sub(r"[^\d]","", t)
    return int(digits) if digits else None

def is_available(v) -> tuple[bool,str]:
    s = norm("" if v is None else str(v)).lower()
    if not s or s in {"-","—","н/д","нет"}: return False, "0"
    if s.isdigit():
        q=int(s); return (q>0), str(q)
    if re.search(r">\s*\d+", s): return True, "10"
    if "есть" in s or "в наличии" in s or "да" in s: return True, "1"
    return True, "1"

# ================= ФОТО: ТОЛЬКО src у <img itemprop="image"> =================
def get_primary_image_src(product_url: str) -> Optional[str]:
    """
    Заходим на страницу товара и берём РОВНО значение атрибута src="..."
    у ПЕРВОГО тега <img itemprop="image">. Больше ничего.
    """
    try:
        r = requests.get(product_url, headers=UA_HEADERS, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
    except Exception:
        return None

    img = soup.select_one('img[itemprop="image"]')
    if img and img.get("src"):
        return img["src"]   # БЕЗ каких-либо преобразований — ровно то, что в кавычках
    return None

# ================= YML =================
ROOT_CAT_ID = "9300000"
def hash_int(s: str) -> int: return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6],16)
def cat_id_for(name: str) -> str: return str(9300001 + (hash_int(name.lower()) % 400000))

def make_offer_id(it: dict) -> str:
    art = (it.get("article") or "").strip()
    if art: return f"copyline:{art}"
    base = re.sub(r"[^a-z0-9]+","-", (it.get("name","").lower()))
    h = hashlib.md5((it.get('name','').lower()+"|"+(it.get('category') or '').lower()).encode('utf-8')).hexdigest()[:8]
    return f"copyline:{base}:{h}"

def build_yml(items: List[dict]) -> bytes:
    root=Element("yml_catalog"); shop=SubElement(root,"shop")
    SubElement(shop,"name").text="copyline"
    curr=SubElement(shop,"currencies"); SubElement(curr,"currency",{"id":"KZT","rate":"1"})

    # категории (простые)
    cats={}
    for it in items:
        nm = it.get("category") or "Copyline"
        cats.setdefault(nm, cat_id_for(nm))

    xml_cats=SubElement(shop,"categories")
    SubElement(xml_cats,"category",{"id":"9300000"}).text="Copyline"
    for nm,cid in cats.items():
        SubElement(xml_cats,"category",{"id":cid,"parentId":"9300000"}).text=nm

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

        cid = cats.get(it.get("category") or "Copyline", "9300000")
        o = SubElement(offers,"offer",{
            "id": oid,
            "available":"true" if it.get("available") else "false",
            "in_stock":"true" if it.get("available") else "false",
        })
        SubElement(o,"name").text = it.get("name","")
        if it.get("price") is not None: SubElement(o,"price").text=str(it["price"])
        SubElement(o,"currencyId").text="KZT"
        SubElement(o,"categoryId").text=cid
        if it.get("vendor"): SubElement(o,"vendor").text=it["vendor"]
        if it.get("article"): SubElement(o,"vendorCode").text=it["article"]
        q = it.get("qty") or ("1" if it.get("available") else "0")
        for tag in ("quantity_in_stock","stock_quantity","quantity"):
            SubElement(o,tag).text=q
        if it.get("picture"): SubElement(o,"picture").text = it["picture"]

    buf=io.BytesIO(); ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

# ================= MAIN =================
def main():
    ensure_outdir()

    # 1) читаем прайс
    xls = fetch_bytes(XLSX_URL)
    wb = load_workbook(io.BytesIO(xls), read_only=True, data_only=True)

    items=[]; total=0
    for ws in wb.worksheets:
        headers, start = best_header(ws)
        if not headers: continue
        cols = map_columns(headers)
        if cols.get("name") is None: continue

        url_col = cols.get("url")
        for row in ws.iter_rows(min_row=start, values_only=True):
            row=list(row)
            def getc(i): return None if i is None or i>=len(row) else row[i]

            name     = norm(getc(cols.get("name")))
            article  = norm(getc(cols.get("article")))
            price    = parse_price(getc(cols.get("price")))
            avail, qty = is_available(getc(cols.get("availability")))
            category = norm(getc(cols.get("category")))
            purl     = url_or_none(getc(url_col)) if url_col is not None else None

            if not name and not article and not purl:
                continue

            pic = None
            if purl:
                pic = get_primary_image_src(purl)
                time.sleep(REQUEST_DELAY)

            items.append({
                "name": name or article or "",
                "article": article,
                "category": category or "Copyline",
                "price": price,
                "available": avail,
                "qty": qty,
                "picture": pic
            })

            total += 1
            if MAX_ROWS and total >= MAX_ROWS:
                break
        if MAX_ROWS and total >= MAX_ROWS:
            break

    # 2) пишем yml
    yml = build_yml(items)
    with open(OUT_FILE,"wb") as f:
        f.write(yml)

    pics = sum(1 for it in items if it.get("picture"))
    print(f"{OUT_FILE}: {len(items)} items | pictures grabbed: {pics}")
    if any(it.get('picture') is None for it in items):
        print("⚠️ Для части строк нет URL в прайсе или на странице нет <img itemprop=\"image\"> — фото пропущены.")

if __name__ == "__main__":
    try: main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
