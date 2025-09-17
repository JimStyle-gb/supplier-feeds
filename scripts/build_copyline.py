# -*- coding: utf-8 -*-
"""
Сборщик YML для поставщика Copyline (плоский <offers> для Satu)
script_version = copyline-2025-09-17.1

Апдейты .1:
- Жёсткая нормализация Юникода (NFKC) и пробелов для имен и ключей.
- Стрип «мусора» в начале имени (кавычки, «», -, —, •, скобки и т.п.) перед проверкой префикса.
- Расширен список заголовков для колонки 'name' (например, 'Наименование товара', 'Номенклатура').
- Отладочные логи по ключам/совпадениям (включаются переменной окружения COPYLINE_KEYWORDS_DEBUG=1).
"""

from __future__ import annotations
import os, sys, re, time, random, urllib.parse, io, hashlib, unicodedata
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests
from openpyxl import load_workbook

# ========================== НАСТРОЙКИ ===========================

SCRIPT_VERSION = "copyline-2025-09-17.1"

SUPPLIER_NAME    = os.getenv("SUPPLIER_NAME", "copyline")
SUPPLIER_URL     = os.getenv("SUPPLIER_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE_YML     = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC              = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S        = int(os.getenv("TIMEOUT_S", "30"))
RETRIES          = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF    = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "2000"))

COPYLINE_KEYWORDS_PATH  = os.getenv("COPYLINE_KEYWORDS_PATH", "docs/copyline_keywords.txt")
COPYLINE_KEYWORDS_MODE  = os.getenv("COPYLINE_KEYWORDS_MODE", "include").lower()  # include|exclude
COPYLINE_KEYWORDS_DEBUG = os.getenv("COPYLINE_KEYWORDS_DEBUG", "0").lower() in {"1","true","yes"}
COPYLINE_DEBUG_MAX_HITS = int(os.getenv("COPYLINE_DEBUG_MAX_HITS", "40"))
COPYLINE_PREFIX_ALLOW_TRIM = os.getenv("COPYLINE_PREFIX_ALLOW_TRIM", "1").lower() in {"1","true","yes"}

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "CL")
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "1").lower() in {"1","true","yes"}

DRY_RUN = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# ================================ УТИЛИТЫ ==================================

def log(msg: str) -> None: print(msg, flush=True)
def warn(msg: str) -> None: print(f"WARN: {msg}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1) -> None: print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_utc_str() -> str: return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty_str() -> str:
    if ZoneInfo: return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")

def _norm(s: str) -> str:
    """NFKC → NBSP→пробел → ё→е → lower → схлоп пробелов."""
    s = _nfkc(s).replace("\u00A0", " ").replace("ё", "е").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

_LEADING_NOISE_RE = re.compile(r'^[\s\-\–\—•·|:/\\\[\]\(\)«»"“”„\']+')

def _strip_leading_noise(s_norm: str) -> str:
    """Снять 'мусор' в начале нормализованной строки (кавычки, тире, буллеты, скобки и т.п.)."""
    return _LEADING_NOISE_RE.sub("", s_norm)

def _clean_one_line(s: str) -> str:
    if not s: return ""
    s = _nfkc(s).replace("\r\n","\n").replace("\r","\n").replace("\u00A0"," ")
    s = re.sub(r"&nbsp;?", " ", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_money(raw: str) -> Optional[float]:
    if raw is None: return None
    s = str(raw)
    s = (s.strip().replace("\xa0"," ").replace(" ","").replace("KZT","").replace("kzt","").replace("₸","").replace(",","."))
    if not s: return None
    try:
        v = float(s); return v if v>0 else None
    except Exception:
        return None

def stable_id_from(text: str) -> str:
    h = hashlib.sha1((_nfkc(text)).encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"CL-{h}"

# ======================= ЗАГРУЗКА XLSX ========================

def fetch_xlsx_bytes(url: str) -> bytes:
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, RETRIES+1):
        try:
            r = sess.get(url, headers=headers, timeout=TIMEOUT_S, stream=True)
            if r.status_code != 200: raise RuntimeError(f"HTTP {r.status_code}")
            data = r.content
            if len(data) < MIN_BYTES: raise RuntimeError(f"too small ({len(data)} bytes)")
            return data
        except Exception as e:
            last_exc = e
            back = RETRY_BACKOFF * attempt * (1.0 + random.uniform(-0.2, 0.2))
            warn(f"fetch attempt {attempt}/{RETRIES} failed: {e}; sleep {back:.2f}s")
            if attempt < RETRIES: time.sleep(back)
    raise RuntimeError(f"fetch failed after {RETRIES} attempts: {last_exc}")

# ===================== КЛЮЧИ (префиксы/регулярки) ==================

class KeySpec:
    __slots__=("raw","kind","norm","pattern")
    def __init__(self, raw: str, kind: str, norm: Optional[str], pattern: Optional[re.Pattern]):
        self.raw, self.kind, self.norm, self.pattern = raw, kind, norm, pattern

def load_keywords(path: str) -> List[KeySpec]:
    if not path or not os.path.exists(path): return []
    data = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f: data=f.read()
            data = data.replace("\ufeff","").replace("\x00","")
            break
        except Exception:
            continue
    if data is None:
        with open(path,"r",encoding="utf-8",errors="ignore") as f:
            data = f.read().replace("\x00","")
    keys: List[KeySpec]=[]
    for line in data.splitlines():
        s = line.strip()
        if not s or s.lstrip().startswith("#"): continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try:
                pat = re.compile(s[1:-1], re.I)
                keys.append(KeySpec(s,"regex",None,pat))
            except Exception:
                pass
        else:
            keys.append(KeySpec(s,"prefix",_norm(s),None))
    return keys

def name_passes_prefix(name: str, keys: List[KeySpec]) -> Tuple[bool, Optional[str]]:
    if not keys: return True, None
    nm = _norm(name)
    nm_trim = _strip_leading_noise(nm) if COPYLINE_PREFIX_ALLOW_TRIM else nm
    for ks in keys:
        if ks.kind=="prefix":
            if ks.norm and (nm_trim.startswith(ks.norm) or nm.startswith(ks.norm)):
                return True, ks.raw
        else:
            if ks.pattern and ks.pattern.match(name or ""):
                return True, ks.raw
    return False, None

# =========================== НОРМАЛИЗАЦИЯ БРЕНДА ===========================

def _norm_brand_key(s: str) -> str:
    if not s: return ""
    s = _nfkc(s).strip().lower().replace("ё","е")
    s = re.sub(r"[-_/]+"," ",s)
    s = re.sub(r"\s+"," ",s)
    return s

SUPPLIER_BLOCKLIST = {_norm_brand_key(x) for x in ["copyline","copy line","копилайн","alstyle","akcent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

def normalize_brand(raw: str) -> str:
    k=_norm_brand_key(raw or "")
    if (not k) or (k in SUPPLIER_BLOCKLIST): return ""
    return raw.strip()

# ============================== ЦЕНЫ ============================

PriceRule = Tuple[int,int,float,int]
PRICING_RULES: List[PriceRule] = [
    (   101,    10000, 4.0,  3000),
    ( 10001,    25000, 4.0,  4000),
    ( 25001,    50000, 4.0,  5000),
    ( 50001,    75000, 4.0,  7000),
    ( 75001,   100000, 4.0, 10000),
    (100001,   150000, 4.0, 12000),
    (150001,   200000, 4.0, 15000),
    (200001,   300000, 4.0, 20000),
    (300001,   400000, 4.0, 25000),
    (400001,   500000, 4.0, 30000),
    (500001,   750000, 4.0, 40000),
    (750001,  1000000, 4.0, 50000),
    (1000001, 1500000, 4.0, 70000),
    (1500001, 2000000, 4.0, 90000),
    (2000001,100000000,4.0,100000),
]

def price_tail_900(n: float) -> int:
    i=int(n); k=max(i//1000,0); out=k*1000+900; return out if out>=900 else 900

def compute_retail(dealer: float) -> Optional[int]:
    for lo,hi,pct,add in PRICING_RULES:
        if lo<=dealer<=hi:
            return price_tail_900(dealer*(1.0+pct/100.0)+add)
    return None

# ============================ РАЗБОР XLSX ============================

NAME_COLS   = {
    "name","наименование","название","товар","product",
    "наименование товара","номенклатура"
}
SKU_COLS    = {"артикул","sku","код","part","part number","partnumber","модель"}
PRICE_COLS  = {"цена","цена закуп","опт","dealer","закуп","b2b","стоимость","price","opt","rrp","розница"}
BRAND_COLS  = {"бренд","производитель","vendor","brand","maker"}
DESC_COLS   = {"описание","description","описание товара","характеристики","spec","specs"}
URL_COLS    = {"url","ссылка","link"}
IMG_COLS    = {"image","картинка","фото","picture","image url","img"}
AVAIL_COLS  = {"наличие","stock","количество","qty","остаток","доступно"}

def map_headers(ws) -> Dict[int, str]:
    header_row=None
    for r in ws.iter_rows(min_row=1, max_row=1, values_only=True):
        header_row=[str(x or "").strip() for x in r]; break
    if not header_row: err("XLSX: не найдена строка заголовков")
    mapping: Dict[int,str]={}
    for idx, raw in enumerate(header_row, start=1):
        key=_norm(raw)
        if key in NAME_COLS: mapping[idx]="name"
        elif key in SKU_COLS: mapping[idx]="sku"
        elif key in BRAND_COLS: mapping[idx]="brand"
        elif key in DESC_COLS: mapping[idx]="desc"
        elif key in URL_COLS: mapping[idx]="url"
        elif key in IMG_COLS: mapping[idx]="img"
        elif key in AVAIL_COLS: mapping[idx]="avail"
        elif key in PRICE_COLS: mapping[idx]="price"
    if "name" not in mapping.values():
        err(f"XLSX: не найдена колонка с названием товара. Заголовки: {header_row}")
    return mapping

def row_to_dict(row_vals: List, mapping: Dict[int,str]) -> Dict[str,str]:
    out: Dict[str,str]={}
    for col_idx, field in mapping.items():
        val = row_vals[col_idx-1] if col_idx-1 < len(row_vals) else None
        s = "" if val is None else str(val).strip()
        if not s: continue
        if field=="price":
            prev=out.get("_price_candidates",[]); prev.append(s); out["_price_candidates"]=prev
        else:
            out[field]=s
    return out

def best_dealer_price(row: Dict[str,str]) -> Optional[float]:
    vals=[]
    for s in row.get("_price_candidates", []):
        v=parse_money(s)
        if v is not None: vals.append(v)
    return min(vals) if vals else None

# ======================= vendorCode / артикул ==============================

ARTICUL_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)

def norm_code(s: str) -> str:
    if not s: return ""
    s=re.sub(r"[\s_]+","",s)
    s=s.replace("—","-").replace("–","-")
    s=re.sub(r"[^A-Za-z0-9\-]+","",s)
    return s.upper()

def extract_article_from_any(row: Dict[str,str]) -> str:
    art = norm_code(row.get("sku",""))
    if art: return art
    name=row.get("name","")
    m=ARTICUL_RE.search(name or "")
    if m: return norm_code(m.group(1))
    url=row.get("url","")
    if url:
        try:
            last=urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
            last=re.sub(r"\.(html?|php|aspx?|htm)$","",last,flags=re.I)
            m2=ARTICUL_RE.search(last)
            return norm_code(m2.group(1) if m2 else last)
        except Exception:
            pass
    return ""

# =============================== FEED_META =================================

def render_feed_meta(pairs: Dict[str, str]) -> str:
    order=[
        "script_version","supplier","source",
        "offers_total","offers_written","filtered_by_keywords",
        "prices_updated","vendors_detected","available_set_true",
        "built_utc","built_Asia/Almaty",
    ]
    comments={
        "script_version":"Версия скрипта (для контроля в CI)",
        "supplier":"Метка поставщика",
        "source":"URL исходного XLSX",
        "offers_total":"Позиции в исходном файле (до фильтра)",
        "offers_written":"Офферов записано (после фильтра и очистки)",
        "filtered_by_keywords":"Сколько позиций отфильтровано по префиксам",
        "prices_updated":"Скольким товарам рассчитали price",
        "vendors_detected":"Скольким товарам распознали бренд",
        "available_set_true":"Скольким офферам выставлено available=true",
        "built_utc":"Время сборки (UTC)",
        "built_Asia/Almaty":"Время сборки (Алматы)",
    }
    max_key=max(len(k) for k in order)
    lefts=[f"{k.ljust(max_key)} = {pairs.get(k,'n/a')}" for k in order]
    max_left=max(len(x) for x in lefts)
    lines=["FEED_META"]
    for left,k in zip(lefts,order):
        lines.append(f"{left.ljust(max_left)}  | {comments[k]}")
    return "\n".join(lines)

# ================================= MAIN ====================================

def main()->None:
    log(f"Source: {SUPPLIER_URL}")
    data=fetch_xlsx_bytes(SUPPLIER_URL)
    wb=load_workbook(io.BytesIO(data), data_only=True, read_only=True)
    ws=wb.active

    mapping=map_headers(ws)
    keys=load_keywords(COPYLINE_KEYWORDS_PATH)
    if COPYLINE_KEYWORDS_MODE=="include" and len(keys)==0:
        err("COPYLINE_KEYWORDS_MODE=include, но ключей не найдено. Проверь docs/copyline_keywords.txt.", 2)

    raw_rows=[]
    for r in ws.iter_rows(min_row=2, values_only=True):
        row_vals=[x if x is not None else "" for x in r]
        d=row_to_dict(row_vals, mapping)
        if "name" not in d or not d["name"].strip(): continue
        raw_rows.append(d)

    offers_total=len(raw_rows)

    # DEBUG: показать первые имена и первые ключи
    if COPYLINE_KEYWORDS_DEBUG:
        log(f"[DEBUG] loaded keywords: {len(keys)}")
        for i, ks in enumerate(keys[:10], 1):
            log(f"[DEBUG] key[{i}]: {ks.raw} ({ks.kind})")
        for i, d in enumerate(raw_rows[:COPYLINE_DEBUG_MAX_HITS], 1):
            log(f"[DEBUG] name[{i}]: {d.get('name','')[:120]}")

    filtered_rows=[]; filtered_out=0
    for d in raw_rows:
        ok,_=name_passes_prefix(d.get("name",""), keys)
        drop=(COPYLINE_KEYWORDS_MODE=="exclude" and ok) or (COPYLINE_KEYWORDS_MODE=="include" and not ok)
        if drop: filtered_out+=1
        else: filtered_rows.append(d)

    root=ET.Element("yml_catalog"); root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    shop=ET.SubElement(root,"shop"); offers=ET.SubElement(shop,"offers")

    prices_updated=0; vendors_detected=0; available_true=0

    for row in filtered_rows:
        name=row.get("name","").strip()
        if not name: continue

        article=extract_article_from_any(row)
        offer_id=row.get("sku") or article or stable_id_from(name)
        offer=ET.SubElement(offers,"offer",{"id":offer_id})

        ET.SubElement(offer,"name").text=name

        img=row.get("img","").strip()
        if img:
            ET.SubElement(offer,"picture").text=img

        brand=normalize_brand(row.get("brand","").strip())
        if brand:
            ET.SubElement(offer,"vendor").text=brand
            vendors_detected+=1

        desc=_clean_one_line(row.get("desc",""))
        if desc:
            ET.SubElement(offer,"description").text=desc

        if VENDORCODE_CREATE_IF_MISSING or article:
            ET.SubElement(offer,"vendorCode").text=f"{VENDORCODE_PREFIX}{article}"

        dealer=best_dealer_price(row)
        if dealer is not None and dealer>100:
            retail=compute_retail(dealer)
            if retail is not None:
                ET.SubElement(offer,"price").text=str(int(retail))
                ET.SubElement(offer,"currencyId").text="KZT"
                prices_updated+=1

        avail_txt=_norm(row.get("avail",""))
        is_avail=True
        if avail_txt and re.search(r"\b(0|нет|no|false|out|нет в наличии)\b", avail_txt, re.I):
            is_avail=False
        ET.SubElement(offer,"available").text="true" if is_avail else "false"
        if is_avail: available_true+=1

    try: ET.indent(root, space="  ")
    except Exception: pass

    meta={
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "offers_total": offers_total,
        "offers_written": len(list(offers.findall("offer"))),
        "filtered_by_keywords": filtered_out,
        "prices_updated": prices_updated,
        "vendors_detected": vendors_detected,
        "available_set_true": available_true,
        "built_utc": now_utc_str(),
        "built_Asia/Almaty": now_almaty_str(),
    }
    root.insert(0, ET.Comment(render_feed_meta(meta)))

    xml_bytes=ET.tostring(root, encoding=ENC, xml_declaration=True)
    xml_text=xml_bytes.decode(ENC, errors="replace")
    xml_text=re.sub(r"(-->)\s*(<shop>)", lambda m: f"{m.group(1)}\n  {m.group(2)}", xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] File not written."); return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML,"w",encoding=ENC, newline="\n") as f: f.write(xml_text)

    docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
    try:
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir,".nojekyll"),"wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | offers={meta['offers_written']} | encoding={ENC} | script={SCRIPT_VERSION}")

if __name__=="__main__":
    try: main()
    except Exception as e: err(str(e))
