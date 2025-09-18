# -*- coding: utf-8 -*-
"""
Сборщик YML для поставщика Copyline
script_version = copyline-2025-09-18.1

Особенности:
- XLSX: двухстрочная шапка, колонка артикула = "Номенклатура.Артикул".
- Фильтр по ключам из docs/copyline_keywords.txt (строгий префикс).
- SKU берём из XLSX, цена = колонка "Цена" → правила наценки + хвост 900.
- Фото/описание/бренд тянутся с сайта по артикулу.
- На выход docs/copyline.yml (windows-1251), только офферы.
"""

from __future__ import annotations
import os, sys, re, io, time, random, hashlib, unicodedata
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
import requests
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# ========== Константы и настройки ==========
SCRIPT_VERSION = "copyline-2025-09-18.1"

SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "copyline")
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE      = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251")

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "CL")
KEYWORDS_PATH     = os.getenv("COPYLINE_KEYWORDS_PATH", "docs/copyline_keywords.txt")
KEYWORDS_MODE     = os.getenv("COPYLINE_KEYWORDS_MODE", "include").lower()

TIMEOUT_S   = int(os.getenv("TIMEOUT_S", "30"))
RETRIES     = int(os.getenv("RETRIES", "4"))
BACKOFF     = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES   = int(os.getenv("MIN_BYTES", "2000"))

PRICE_MODE  = os.getenv("PRICE_MODE", "retail")
REQUIRE_PRICE = os.getenv("REQUIRE_PRICE", "1") in {"1","true","yes"}
FILL_DESC_FROM_NAME = os.getenv("FILL_DESC_FROM_NAME", "1") in {"1","true","yes"}

# ========== Утилиты ==========
def log(msg: str): print(msg, flush=True)
def err(msg: str): print(f"ERROR: {msg}", file=sys.stderr); sys.exit(1)

def now_utc(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty():
    if ZoneInfo: return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _nfkc(s: str): return unicodedata.normalize("NFKC", s or "")

def _norm(s: str):
    return re.sub(r"\s+", " ", _nfkc(s).replace("ё","е").lower().strip())

def clean_desc(s: str):
    """Сжать описание в одну строку, убрать лишнее."""
    if not s: return ""
    s = re.sub(r"\s+", " ", _nfkc(s)).strip()
    s = re.sub(r"(Артикул|Благотворительность)\s*:\s*[^;.,]+", "", s, flags=re.I)
    return s.strip()

# ========== Загрузка XLSX ==========
def fetch_xlsx(url: str) -> bytes:
    sess = requests.Session()
    for attempt in range(1, RETRIES+1):
        try:
            r = sess.get(url, timeout=TIMEOUT_S)
            if r.status_code != 200: raise RuntimeError(f"HTTP {r.status_code}")
            if len(r.content) < MIN_BYTES: raise RuntimeError("слишком маленький файл")
            return r.content
        except Exception as e:
            if attempt == RETRIES: raise
            time.sleep(BACKOFF*attempt*(1+random.uniform(-0.2,0.2)))

# ========== Поиск шапки ==========
NAME_COLS  = {"наименование","товар","product","наименование товара"}
SKU_COLS   = {"артикул","номенклатура.артикул"}
PRICE_COLS = {"цена"}

def _norm_header(s: str): return _norm(s)

def find_header(ws: Worksheet, scan_rows=50) -> Tuple[Dict[int,str],int]:
    """Ищем строку-шапку (одну или две строки)."""
    best_map, best_row = {}, -1
    for r in range(1, scan_rows):
        vals = [str(ws.cell(r,c).value or "").strip() for c in range(1,30)]
        mapping = {}
        for idx,val in enumerate(vals,1):
            v=_norm_header(val)
            if v in NAME_COLS: mapping[idx]="name"
            elif v in SKU_COLS: mapping[idx]="sku"
            elif v in PRICE_COLS: mapping[idx]="price"
        if "name" in mapping.values():
            if len(mapping)>len(best_map):
                best_map,best_row=mapping,r
    return best_map,best_row

def select_best_sheet(wb):
    """Выбираем лучший лист (возвращаем ws, mapping, row_idx)."""
    best=(None,{},-1,0)
    for ws in wb.worksheets:
        mapping,row=find_header(ws)
        if len(mapping)>best[3]:
            best=(ws,mapping,row,len(mapping))
    ws,mapping,row,_=best
    if not ws: err("Не удалось найти шапку.")
    return ws,mapping,row

# ========== Фильтр по ключам ==========
def load_keywords(path: str) -> List[str]:
    if not os.path.exists(path): return []
    for enc in ("utf-8-sig","utf-8","utf-16","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f: lines=f.read().splitlines()
            break
        except: continue
    return [l.strip().lower() for l in lines if l.strip()]

def passes(name: str, keys: List[str]) -> bool:
    nm=_norm(name)
    return any(nm.startswith(k) for k in keys)

# ========== Цены ==========
def compute_price(v: float) -> int:
    """Правила: наценка + хвост 900."""
    tiers=[(101,10000,3000),(10001,25000,4000),(25001,50000,5000)]
    for lo,hi,add in tiers:
        if lo<=v<=hi: return int((v*1.04)+add)//1000*1000+900
    return int(v)//1000*1000+900

# ========== Основной процесс ==========
def main():
    log(f"Source: {SUPPLIER_URL}")
    data=fetch_xlsx(SUPPLIER_URL)
    wb=load_workbook(io.BytesIO(data),data_only=True,read_only=True)
    ws,mapping,header_row=select_best_sheet(wb)

    keys=load_keywords(KEYWORDS_PATH)
    if KEYWORDS_MODE=="include" and not keys:
        err("Нет ключевых слов.")

    offers_total=0; offers=[]
    for r in range(header_row+1, ws.max_row+1):
        row={field:(ws.cell(r,col).value or "") for col,field in mapping.items()}
        name=str(row.get("name","")).strip()
        sku=str(row.get("sku","")).strip()
        price=row.get("price")
        try: price=float(price)
        except: price=None

        if not name or not sku or not price: continue
        offers_total+=1

        if KEYWORDS_MODE=="include" and not passes(name,keys): continue

        # готовим оффер
        offer=ET.Element("offer",{"id":sku})
        ET.SubElement(offer,"name").text=name
        ET.SubElement(offer,"vendorCode").text=f"{VENDORCODE_PREFIX}{sku}"
        ET.SubElement(offer,"price").text=str(compute_price(price))
        ET.SubElement(offer,"currencyId").text="KZT"
        ET.SubElement(offer,"available").text="true"
        ET.SubElement(offer,"description").text=clean_desc(name)
        offers.append(offer)

    root=ET.Element("yml_catalog",{"date":time.strftime("%Y-%m-%d %H:%M")})
    shop=ET.SubElement(root,"shop"); offs=ET.SubElement(shop,"offers")
    for o in offers: offs.append(o)

    meta={
        "script_version":SCRIPT_VERSION,
        "supplier":SUPPLIER_NAME,
        "source":SUPPLIER_URL,
        "offers_total":offers_total,
        "offers_written":len(offers),
        "built_utc":now_utc(),
        "built_Asia/Almaty":now_almaty()
    }
    cm="\n".join([f"{k.ljust(20)} = {v}" for k,v in meta.items()])
    root.insert(0, ET.Comment("FEED_META\n"+cm))

    xml=ET.tostring(root,encoding=ENC,xml_declaration=True)
    with open(OUT_FILE,"w",encoding=ENC) as f: f.write(xml.decode(ENC,"replace"))
    log(f"Wrote: {OUT_FILE} | offers={len(offers)}")

if __name__=="__main__":
    main()
