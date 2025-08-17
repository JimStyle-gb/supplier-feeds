# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, hashlib, requests
from collections import Counter
from openpyxl import load_workbook
from xml.etree.ElementTree import Element, SubElement, ElementTree

XLSX_URL   = os.getenv("XLSX_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE   = os.getenv("OUT_FILE",  "docs/copyline.yml")
ENC        = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
CATS_FILE  = os.getenv("CATEGORIES_FILE", "docs/categories_copyline.txt")
SEEN_FILE  = os.getenv("SEEN_FILE", "docs/copyline_seen.txt")
UA_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def norm(s): return re.sub(r"\s+"," ", (s or "").strip())

HEADER_HINTS = {
    "name":        ["номенклатура","наименование","название","товар","описание"],
    "article":     ["артикул","номенклатура.артикул","sku","код товара","модель","part number","pn","p/n"],
    "availability":["остаток","наличие","кол-во","количество","qty","stock","остатки"],
    "price":       ["цена","опт цена","опт. цена","розница","стоимость","цена, тг","цена тг","retail","опт"],
    "unit":        ["ед.","ед","единица"],
    "category":    ["категория","раздел","группа","тип"],  # в прайсе нет — будет вычислена из заголовков секций
}

def ensure_files():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    if not os.path.exists(CATS_FILE):
        with open(CATS_FILE, "w", encoding="utf-8") as f:
            f.write(
"""# Паттерны-ВКЛЮЧЕНИЯ (один в строке). Если пусто — берём ВСЁ.
# Совпадение ищется в: КАТЕГОРИИ (заголовок секции), НАЗВАНИИ ЛИСТА и НАЗВАНИИ ТОВАРА.
# Просто подстрока (регистр не важен) или regex с префиксом re:
тонер
картридж
фотобарабан
re:драм|drum
""")

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
    return subs, regs  # оба пустые -> включаем всё

def fetch_xlsx(url: str) -> bytes:
    r = requests.get(url, timeout=180, headers=UA_HEADERS)
    r.raise_for_status()
    return r.content

def best_header(ws):
    """
    Находит лучшую строку заголовка ИЛИ пару строк, склеивая их по столбцам.
    Возвращает: (headers:list[str], data_start_row:int)
    """
    def score(row_vals):
        low = [norm(x).lower() for x in row_vals]
        matched = set()
        for key, hints in HEADER_HINTS.items():
            for i, cell in enumerate(low):
                if any(h in cell for h in hints):
                    matched.add(key)
                    break
        return len(matched)

    # собираем первые 40 строк
    rows = []
    for row in ws.iter_rows(min_row=1, max_row=40, values_only=True):
        rows.append([norm("" if v is None else str(v)) for v in row])

    best = ([], None, -1)  # headers, data_start_row, score
    # одиночная строка
    for i, r in enumerate(rows):
        sc = score(r)
        if sc > best[2]:
            best = (r, i+1, sc)
    # пара строк (склейка по столбцам)
    for i in range(len(rows)-1):
        a, b = rows[i], rows[i+1]
        m = max(len(a), len(b))
        merged = []
        for j in range(m):
            x = (a[j] if j < len(a) else "")
            y = (b[j] if j < len(b) else "")
            cell = " ".join([t for t in (x, y) if t]).strip()
            merged.append(cell)
        sc = score(merged)
        if sc > best[2]:
            best = (merged, i+2, sc)  # данные начинаются ПОСЛЕ второй строки
    return best[0], best[1] + 1  # ещё на 1 вниз, чтобы не цеплять заголовок

def map_columns(headers):
    cols = {}
    low = [h.lower() for h in headers]
    def find_any(keys):
        for i, cell in enumerate(low):
            if any(k in cell for k in keys):
                return i
        return None
    cols["name"]        = find_any(HEADER_HINTS["name"])
    cols["article"]     = find_any(HEADER_HINTS["article"])
    cols["availability"]= find_any(HEADER_HINTS["availability"])
    cols["price"]       = find_any(HEADER_HINTS["price"])
    cols["unit"]        = find_any(HEADER_HINTS["unit"])
    cols["category"]    = find_any(HEADER_HINTS["category"])  # чаще None
    return cols

def parse_price(v) -> int|None:
    if v is None: return None
    t = norm(str(v)).replace("₸","").replace("тг","")
    digits = re.sub(r"[^\d]","", t)
    return int(digits) if digits else None

def is_available(v) -> tuple[bool,str]:
    s = norm(str(v)).lower()
    if not s or s in {"-","—","н/д","нет"}: return False, "0"
    if s.isdigit():
        q=int(s); return (q>0), str(q)
    if re.search(r">\s*\d+", s): return True, "10"
    if "есть" in s or "в наличии" in s or "да" in s: return True, "1"
    return True, "1"

def pass_filters(cat: str, sheet: str, name: str, subs, regs) -> bool:
    if not subs and not regs:  # фильтров нет -> берём всё
        return True
    hay = (cat.lower(), sheet.lower(), name.lower())
    if any(sub in h for sub in subs for h in hay): return True
    if any(r.search(h) for r in regs for h in hay): return True
    return False

def hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6],16)

ROOT_CAT_ID = "9300000"
def cat_id_for(name: str) -> str:
    return str(9300001 + (hash_int(name.lower()) % 400000))

def build_yml(items):
    cats={}
    for it in items:
        cn = it.get("category") or "Copyline"
        if cn not in cats: cats[cn] = cat_id_for(cn)

    root=Element("yml_catalog"); shop=SubElement(root,"shop")
    SubElement(shop,"name").text="copyline-xlsx"
    curr=SubElement(shop,"currencies"); SubElement(curr,"currency",{"id":"KZT","rate":"1"})

    xml_cats=SubElement(shop,"categories")
    SubElement(xml_cats,"category",{"id":ROOT_CAT_ID}).text="Copyline"
    for nm,cid in cats.items():
        SubElement(xml_cats,"category",{"id":cid,"parentId":ROOT_CAT_ID}).text=nm

    offers=SubElement(shop,"offers")
    seen=set()
    for it in items:
        article = it.get("article") or ""
        oid = f"copyline:{article}" if article else f"copyline:{hash_int(it.get('name',''))}"
        if oid in seen: oid = f"{oid}-{hash_int(it.get('brand','')+it.get('name',''))}"
        seen.add(oid)

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
        if article:         SubElement(o,"vendorCode").text=article
        q = it.get("qty") or ("1" if it["available"] else "0")
        for tag in ("quantity_in_stock","stock_quantity","quantity"):
            SubElement(o,tag).text=q
    buf=io.BytesIO(); ElementTree(root).write(buf, encoding=ENC, xml_declaration=True); return buf.getvalue()

def main():
    ensure_files()
    subs, regs = load_patterns(CATS_FILE)

    data = fetch_xlsx(XLSX_URL)
    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)

    items=[]
    seen_cats=Counter(); seen_sheets=Counter()

    for ws in wb.worksheets:
        headers, data_start = best_header(ws)
        if not headers: 
            seen_sheets[ws.title]+=0
            continue
        cols = map_columns(headers)
        # нужно хотя бы name и price/availability
        if cols.get("name") is None: 
            seen_sheets[ws.title]+=0
            continue

        current_cat = None  # будем брать из строк-заголовков
        for row in ws.iter_rows(min_row=data_start, values_only=True):
            row = list(row)
            def getc(idx): 
                return None if idx is None or idx>=len(row) else row[idx]

            name    = norm(getc(cols.get("name")))
            article = norm(getc(cols.get("article")))
            price   = parse_price(getc(cols.get("price")))
            avail, qty = is_available(getc(cols.get("availability")))
            raw_cat = norm(getc(cols.get("category"))) if cols.get("category") is not None else ""

            # строка-заголовок секции (категория): есть имя, НО нет артикла и нет цены
            if name and not article and price is None:
                # защитимся от «служебных» строк
                if len(name) > 3 and not re.search(r"^цены|^прайс|^тенге|^лист|^итого", name.lower()):
                    current_cat = name
                    seen_cats[current_cat]+=0
                    continue

            if not name and not article:
                continue

            cat = raw_cat or current_cat or norm(ws.title)
            seen_cats[cat]+=1; seen_sheets[ws.title]+=1

            if not pass_filters(cat, ws.title, name or article, subs, regs):
                continue

            items.append({
                "article": article,
                "name": name or article,
                "brand": "",             # в этом прайсе бренда нет явного столбца
                "category": cat,
                "price": price,
                "available": avail,
                "qty": qty,
            })

    # отчёт
    with open(SEEN_FILE,"w",encoding="utf-8") as f:
        f.write("=== SHEETS ===\n")
        for s,c in seen_sheets.most_common():
            f.write(f"{s}\t{c}\n")
        f.write("\n=== CATEGORIES (inferred) ===\n")
        for k,v in seen_cats.most_common(500):
            f.write(f"{k}\t{v}\n")

    yml = build_yml(items)
    with open(OUT_FILE,"wb") as f: f.write(yml)
    print(f"{OUT_FILE}: {len(items)} items; sheets={len(wb.worksheets)}")
    print(f"Seen → {SEEN_FILE} | Filters: {len(subs)} subs, {len(regs)} regex")

if __name__=="__main__":
    try: main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
