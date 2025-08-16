# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, hashlib, requests
from openpyxl import load_workbook
from xml.etree.ElementTree import Element, SubElement, ElementTree

XLSX_URL    = os.getenv("XLSX_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE    = os.getenv("OUT_FILE",  "docs/copyline.yml")
ENC         = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
CATS_FILE   = os.getenv("CATEGORIES_FILE", "docs/categories_copyline.txt")

def norm(s): return re.sub(r"\s+", " ", (s or "").strip())

HEADER_MAP = {
    "article":   ["артикул","код","sku","код товара","артикул поставщика","модель","part number","pn","p/n"],
    "name":      ["наименование","название","товар","описание","наименов"],
    "brand":     ["бренд","производитель","марка","brand","vendor"],
    "price":     ["цена","цена, тг","цена тг","стоимость","retail","опт"],
    "availability":["наличие","остаток","кол-во","количество","qty","stock","остатки"],
    "category":  ["категория","раздел","группа","тип"],
}

def ensure_files():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    if not os.path.exists(CATS_FILE):
        with open(CATS_FILE, "w", encoding="utf-8") as f:
            f.write("# подстроки для фильтрации по колонке категории (например: toner)\n")

def load_patterns(path):
    pats=[]
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            s=line.strip()
            if not s or s.startswith("#"): continue
            pats.append(s.lower())
    return pats  # пусто -> берём все строки

def fetch_xlsx(url: str) -> bytes:
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.content

def find_header(ws):
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=30, values_only=True), start=1):
        vals = [norm(str(x)) for x in row if norm(str(x))]
        if len(vals) >= 3:
            return i, [norm(str(x)) for x in row]
    return None, None

def map_columns(headers):
    cols = {}
    low = [h.lower() for h in headers]
    for key, keys in HEADER_MAP.items():
        for i, h in enumerate(low):
            if any(k in h for k in keys):
                cols[key] = i
                break
    return cols

def parse_price(v) -> int | None:
    if v is None: return None
    t = norm(str(v)).replace("₸","").replace("тг","")
    digits = re.sub(r"[^\d]", "", t)
    return int(digits) if digits else None

def is_available(v) -> tuple[bool,str]:
    s = norm(str(v)).lower()
    if not s: return False, "0"
    if s.isdigit():
        q = int(s); return (q > 0), str(q)
    if re.search(r">\s*\d+", s): return True, "10"
    if "есть" in s or "в наличии" in s or "да" in s: return True, "1"
    if "нет" in s or s == "0": return False, "0"
    return True, "1"

def hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)

ROOT_CAT_ID = "9300000"
def cat_id_for(name: str) -> str:
    return str(9300001 + (hash_int(name.lower()) % 400000))

def build_yml(items: list[dict]) -> bytes:
    cats = {}
    for it in items:
        cn = it.get("category") or "Copyline"
        if cn not in cats:
            cats[cn] = cat_id_for(cn)

    root = Element("yml_catalog")
    shop = SubElement(root, "shop")
    SubElement(shop, "name").text = "copyline-xlsx"
    curr = SubElement(shop, "currencies")
    SubElement(curr, "currency", {"id":"KZT","rate":"1"})

    xml_cats = SubElement(shop, "categories")
    SubElement(xml_cats, "category", {"id": ROOT_CAT_ID}).text = "Copyline"
    for name, cid in cats.items():
        SubElement(xml_cats, "category", {"id": cid, "parentId": ROOT_CAT_ID}).text = name

    offers = SubElement(shop, "offers")
    seen=set()
    for it in items:
        article = it.get("article") or ""
        oid = f"copyline:{article}" if article else f"copyline:{hash_int(it.get('name',''))}"
        if oid in seen: oid = f"{oid}-{hash_int(it.get('brand','')+it.get('name',''))}"
        seen.add(oid)

        cid = cats.get(it.get("category") or "Copyline", ROOT_CAT_ID)
        o = SubElement(offers, "offer", {
            "id": oid,
            "available": "true" if it["available"] else "false",
            "in_stock":  "true" if it["available"] else "false",
        })
        SubElement(o, "name").text = it["name"]
        if it.get("price") is not None: SubElement(o, "price").text = str(it["price"])
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cid
        if it.get("brand"):   SubElement(o, "vendor").text = it["brand"]
        if article:           SubElement(o, "vendorCode").text = article
        q = it.get("qty") or ("1" if it["available"] else "0")
        for tag in ("quantity_in_stock","stock_quantity","quantity"): SubElement(o, tag).text = q

    import io
    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

def main():
    ensure_files()
    patterns = load_patterns(CATS_FILE)  # подстроки категорий (lower)

    data = fetch_xlsx(XLSX_URL)
    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)

    items = []
    for ws in wb.worksheets:
        hdr_row, headers = find_header(ws)
        if not headers: continue
        cols = map_columns(headers)
        if not set(cols.keys()) & {"article","name","price"}: continue

        for row in ws.iter_rows(min_row=hdr_row+1, values_only=True):
            def get(key):
                i = cols.get(key); return None if i is None else row[i]

            name = norm(get("name"))
            article = norm(get("article"))
            if not name and not article: continue

            brand = norm(get("brand"))
            price = parse_price(get("price"))
            cat   = norm(get("category")) or norm(ws.title)
            avail, qty = is_available(get("availability"))

            # фильтр категорий:
            if patterns:
                cl = cat.lower()
                if not any(p in cl for p in patterns):
                    continue

            items.append({
                "article": article, "name": name or article, "brand": brand,
                "category": cat, "price": price, "available": avail, "qty": qty
            })

    yml = build_yml(items)
    with open(OUT_FILE, "wb") as f:
        f.write(yml)
    print(f"{OUT_FILE}: {len(items)} items; sheets={len(wb.worksheets)}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
