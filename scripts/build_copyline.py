# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, hashlib
from typing import Optional, Dict, Any, List
import requests
from openpyxl import load_workbook
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ===== настройки =====
BASE_URL = "https://copyline.kz"
XLSX_URL = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
OUT_FILE = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC      = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

UA_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ===== утилы =====
def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def fetch_xlsx(url_or_path: str) -> bytes:
    if re.match(r"^https?://", url_or_path, re.I):
        r = requests.get(url_or_path, headers=UA_HEADERS, timeout=60)
        r.raise_for_status()
        return r.content
    with open(url_or_path, "rb") as f:
        return f.read()

# ===== префикс поставщика для артикула (Copyline -> 'c') =====
SUPPLIER_PREFIX = "c"

def apply_supplier_prefix(article_raw: Optional[str]) -> str:
    """
    Если артикул только из цифр — добавляем префикс поставщика (например, 41212 -> c41212).
    Иначе возвращаем как есть (CF283A, DR-1075 и т.п. не трогаем).
    """
    art = (article_raw or "").strip()
    compact = re.sub(r"\s+", "", art)
    if compact.isdigit() and compact != "":
        # защита от двойного префикса вида "c12345"
        if compact.startswith(SUPPLIER_PREFIX) and compact[1:].isdigit():
            return compact
        return f"{SUPPLIER_PREFIX}{compact}"
    return art

# ===== определение шапки и колонок =====
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
    # склейка соседних строк
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
        # сначала избегаем артикульные колонки для name
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

# ===== СТРОГИЙ ФИЛЬТР: ключевые слова/фразы ТОЛЬКО В НАЧАЛЕ НАЗВАНИЯ =====
# Разрешённые одиночные слова (точная форма)
ALLOW_SINGLE = {
    "drum",
    "девелопер",
    "драм",
    "картридж",
    "термоблок",
    "термоэлемент",
}

# Разрешённые фразы (только такое написание, ТОЛЬКО в начале)
ALLOW_PHRASES = [
    ["кабель", "сетевой"],
    ["сетевой", "кабель"],  # оба порядка
]

# Стоп-слова — если встречаются где угодно в названии, товар исключаем
DISALLOW_TOKENS = {"chip", "чип", "reset", "ресет"}

TOKEN_RE = re.compile(r"[A-Za-zА-Яа-я0-9]+", re.IGNORECASE)

def _tokenize_ru(text: str) -> list[str]:
    """Нормализуем и разбиваем на токены: буквы/цифры, ё -> е."""
    s = norm(text).lower().replace("ё", "е")
    return TOKEN_RE.findall(s)

def name_matches_filter(name: str) -> bool:
    tokens = _tokenize_ru(name)
    if not tokens:
        return False

    # 1) отбрасываем чипы/ресеты сразу
    if any(t in DISALLOW_TOKENS for t in tokens):
        return False

    # 2) фразы: ДОЛЖНЫ стоять в самом начале (anchored)
    for phrase in ALLOW_PHRASES:
        m = len(phrase)
        if len(tokens) >= m and tokens[:m] == phrase:
            return True

    # 3) одиночные слова: первый токен должен быть из разрешённых
    if tokens[0] in ALLOW_SINGLE:
        return True

    return False

# ===== сборка YML =====
ROOT_CAT_ID = "9300000"
def hash_int(s): return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)
def cat_id_for(name): return str(9300001 + (hash_int(name.lower()) % 400000))

def offer_id(it):
    """
    Для стабильности ID привяжем к ИТОГОВОМУ (с префиксом) артикулу, если он есть.
    Иначе — от имени+категории (как резерв).
    """
    raw_article = norm(it.get("article"))
    final_article = apply_supplier_prefix(raw_article) if raw_article else ""
    if final_article:
        return f"copyline:{final_article}"
    base = re.sub(r"[^a-z0-9]+", "-", norm(it.get("name","")).lower())
    h = hashlib.md5((norm(it.get('name','')).lower()+"|"+norm(it.get('category','')).lower()).encode('utf-8')).hexdigest()[:8]
    return f"copyline:{base}:{h}"

def build_yml(items: List[Dict[str,Any]]) -> bytes:
    cats={}
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
    used=set()
    for it in items:
        oid = offer_id(it)
        if oid in used:
            extra = hashlib.md5((it.get("name","")+str(it.get("price"))).encode("utf-8")).hexdigest()[:6]
            i=2
            while f"{oid}-{extra}-{i}" in used: i+=1
            oid = f"{oid}-{extra}-{i}"
        used.add(oid)

        nm = it.get("category") or "Copyline"
        cid = ROOT_CAT_ID if nm.strip().lower()=="copyline" else cats.get(nm, ROOT_CAT_ID)

        o = SubElement(offers, "offer", {"id": oid, "available":"true", "in_stock":"true"})
        SubElement(o, "name").text = it.get("name","")
        if it.get("price") is not None: SubElement(o, "price").text = str(it["price"])
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cid

        # vendorCode: применяем префикс только к чисто цифровым артикулам
        raw_article = norm(it.get("article"))
        final_article = apply_supplier_prefix(raw_article) if raw_article else ""
        if final_article:
            SubElement(o, "vendorCode").text = final_article

        for tag in ("quantity_in_stock","stock_quantity","quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

# ===== MAIN =====
def main():
    ensure_dir_for(OUT_FILE)

    xls = fetch_xlsx(XLSX_URL)
    wb = load_workbook(io.BytesIO(xls), read_only=True, data_only=True)

    items: List[Dict[str,Any]] = []
    found_name = False

    for ws in wb.worksheets:
        headers, start = best_header(ws)
        cols = map_cols(headers)
        if cols.get("name") is None:
            continue
        found_name = True

        for row in ws.iter_rows(min_row=start, values_only=True):
            row = list(row)
            def getc(i): return None if i is None or i>=len(row) else row[i]
            name = norm(getc(cols["name"]))
            if not name:
                continue

            # === ЖЁСТКИЙ ФИЛЬТР: нужное слово/фраза ДОЛЖНЫ быть в начале названия ===
            if not name_matches_filter(name):
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

    if not found_name:
        print("ERROR: Не найдена колонка Наименование/Номенклатура.", file=sys.stderr)
        sys.exit(1)

    yml = build_yml(items)
    with open(OUT_FILE, "wb") as f:
        f.write(yml)

    print(f"[OK] {OUT_FILE}: items={len(items)} | strict-name filter + supplier prefix (c...)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
