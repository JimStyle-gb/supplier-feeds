# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, hashlib
import requests
from xml.etree.ElementTree import Element, SubElement, ElementTree
from openpyxl import load_workbook

# ====== НАСТРОЙКИ ======
XLSX_URL   = os.getenv("XLSX_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE   = os.getenv("OUT_FILE",  "docs/copyline.yml")
ENC        = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
UA_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ====== УТИЛЫ ======
def norm(s):
    return re.sub(r"\s+"," ", ("" if s is None else str(s)).strip())

def fetch_xlsx(url_or_path: str) -> bytes:
    if re.match(r"^https?://", url_or_path, re.I):
        r = requests.get(url_or_path, headers=UA_HEADERS, timeout=60)
        r.raise_for_status()
        return r.content
    with open(url_or_path, "rb") as f:
        return f.read()

# ====== ШАПКА/КОЛОНКИ ======
HEADER_HINTS = {
    "name":     ["номенклатура","наименование","наименование товара","название","товар","описание","product name","item"],
    "article":  ["артикул","номенклатура.артикул","sku","код товара","код","модель","part number","pn","p/n"],
    "price":    ["цена","опт","опт. цена","розница","стоимость","цена, тг","цена тг","retail","price"],
    "unit":     ["ед.","ед","единица","unit"],
    "category": ["категория","раздел","группа","тип","category"],
}

def best_header(ws):
    def score(arr):
        low = [norm(x).lower() for x in arr]
        got = set()
        for k, hints in HEADER_HINTS.items():
            for cell in low:
                if any(h in cell for h in hints):
                    got.add(k); break
        return len(got)

    rows = []
    for row in ws.iter_rows(min_row=1, max_row=40, values_only=True):
        rows.append([norm("" if v is None else str(v)) for v in row])

    best_row, best_idx, best_sc = [], None, -1
    for i, r in enumerate(rows):
        sc = score(r)
        if sc > best_sc:
            best_row, best_idx, best_sc = r, i+1, sc

    # пробуем «склейку» двух строк шапки
    for i in range(len(rows) - 1):
        a, b = rows[i], rows[i+1]
        m = max(len(a), len(b)); merged = []
        for j in range(m):
            x = a[j] if j < len(a) else ""
            y = b[j] if j < len(b) else ""
            merged.append((" ".join([x, y])).strip())
        sc = score(merged)
        if sc > best_sc:
            best_row, best_idx, best_sc = merged, i+2, sc
    return best_row, (best_idx or 1) + 1

def map_cols(headers):
    low = [h.lower() for h in headers]
    def find(keys):
        for i, c in enumerate(low):
            if any(k in c for k in keys):
                return i
        return None
    return {
        "name":     find(HEADER_HINTS["name"]),
        "article":  find(HEADER_HINTS["article"]),
        "price":    find(HEADER_HINTS["price"]),
        "unit":     find(HEADER_HINTS["unit"]),
        "category": find(HEADER_HINTS["category"]),
    }

def parse_price(v):
    if v is None: return None
    t = norm(v).replace("₸","").replace("тг","")
    digits = re.sub(r"[^\d]", "", t)
    return int(digits) if digits else None

# ====== YML ======
ROOT_CAT_ID = "9300000"
def hash_int(s): return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)
def cat_id_for(name): return str(9300001 + (hash_int(name.lower()) % 400000))
def offer_id(it):
    art = norm(it.get("article"))
    if art: return f"copyline:{art}"
    base = re.sub(r"[^a-z0-9]+", "-", norm(it.get("name","")).lower())
    h = hashlib.md5((norm(it.get('name','')).lower()+"|"+norm(it.get('category','')).lower()).encode('utf-8')).hexdigest()[:8]
    return f"copyline:{base}:{h}"

def build_yml(items):
    cats = {}
    for it in items:
        nm = it.get("category") or "Copyline"
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
    for it in items:
        oid = offer_id(it)
        if oid in used:
            extra = hashlib.md5((it.get("name","") + str(it.get("price"))).encode("utf-8")).hexdigest()[:6]
            i = 2
            while f"{oid}-{extra}-{i}" in used: i += 1
            oid = f"{oid}-{extra}-{i}"
        used.add(oid)

        cid = cats.get(it.get("category") or "Copyline", ROOT_CAT_ID)
        o = SubElement(offers, "offer", {
            "id": oid,
            "available": "true",   # ВСЕГДА в наличии
            "in_stock": "true",
        })
        SubElement(o, "name").text = it.get("name","")
        if it.get("price") is not None: SubElement(o, "price").text = str(it["price"])
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cid
        if it.get("article"): SubElement(o, "vendorCode").text = it["article"]
        for tag in ("quantity_in_stock", "stock_quantity", "quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

# ====== MAIN ======
def main():
    d = os.path.dirname(OUT_FILE)
    if d: os.makedirs(d, exist_ok=True)

    xls = fetch_xlsx(XLSX_URL)
    wb = load_workbook(io.BytesIO(xls), read_only=True, data_only=True)

    items = []
    found_name_sheet = False

    for ws in wb.worksheets:
        headers, start = best_header(ws)
        cols = map_cols(headers)

        # Требуем наличие колонки названия
        if cols.get("name") is None:
            continue

        found_name_sheet = True

        for row in ws.iter_rows(min_row=start, values_only=True):
            row = list(row)
            def getc(i): return None if i is None or i >= len(row) else row[i]

            name     = norm(getc(cols["name"]))
            if not name:
                continue  # пропускаем строки без наименования

            article  = norm(getc(cols.get("article")))
            price    = parse_price(getc(cols.get("price")))
            category = norm(getc(cols.get("category"))) or "Copyline"

            items.append({
                "name": name,            # всегда человекочитаемое наименование
                "article": article,      # артикул идёт в <vendorCode>
                "category": category,
                "price": price,
            })

    if not found_name_sheet:
        print("ERROR: Не найден лист с колонкой наименования (name). Обнови HEADER_HINTS['name'] под шапку прайса.", file=sys.stderr)
        sys.exit(1)

    yml = build_yml(items)
    with open(OUT_FILE, "wb") as f:
        f.write(yml)
    print(f"{OUT_FILE}: {len(items)} items")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
