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
    "name":     ["номенклатура","наименование","название","товар","описание","наименование товара"],
    "article":  ["артикул","номенклатура.артикул","sku","код товара","код","модель","part number","pn","p/n"],
    "price":    ["цена","опт","опт. цена","розница","стоимость","цена, тг","цена тг","retail"],
    "unit":     ["ед.","ед","единица"],
    "category": ["категория","раздел","группа","тип"],
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
        for i, c in enume
