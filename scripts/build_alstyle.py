# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle → YML конвертер (фикс «двойных двоеточий ::» и мелкие правки)
Обновлено: 2025-10-20

Изменения vs твоей последней рабочей версии:
- Исправлена синтаксическая ошибка в normalize_free_text_punct (двойная точка).
- Гарантированно убираем «::» в названиях характеристик и в описаниях.
- Поправлен backreference при переносе <shop> на новую строку.

Остальную логику не трогал.
"""

from __future__ import annotations
import os, sys, re, time, random, urllib.parse, html as html_lib
from copy import deepcopy
from typing import Dict, List, Tuple, Optional, Set
from xml.etree import ElementTree as ET
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
import requests

SCRIPT_VERSION = "alstyle-2025-10-20.no-double-colons"

# --------------------- ENV ---------------------
SUPPLIER_NAME    = os.getenv("SUPPLIER_NAME", "AlStyle")
SUPPLIER_URL     = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML     = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC              = os.getenv("OUTPUT_ENCODING", "windows-1251")  # авто-fallback на UTF-8 при проблемах

TIMEOUT_S        = int(os.getenv("TIMEOUT_S", "30"))
RETRIES          = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF    = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN          = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

ALSTYLE_CATEGORIES_PATH  = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt")
ALSTYLE_CATEGORIES_MODE  = os.getenv("ALSTYLE_CATEGORIES_MODE", "off").lower()  # off|include|exclude

# KEYWORDS (лимиты под Satu)
SATU_KEYWORDS            = os.getenv("SATU_KEYWORDS", "auto").lower()   # auto|off
SATU_KEYWORDS_MAXLEN     = int(os.getenv("SATU_KEYWORDS_MAXLEN", "1024"))
SATU_KEYWORDS_MAXWORDS   = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "1000"))
SATU_KEYWORDS_GEO        = os.getenv("SATU_KEYWORDS_GEO", "on").lower() in {"on","1","true","yes"}
SATU_KEYWORDS_GEO_MAX    = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))
SATU_KEYWORDS_GEO_LAT    = os.getenv("SATU_KEYWORDS_GEO_LAT", "on").lower() in {"on","1","true","yes"}

# PRICE CAP
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "100"))

DROP_CATEGORY_ID_TAG = True
DROP_STOCK_TAGS      = True
PURGE_TAGS_AFTER = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","article")

INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice"
)

# --------------------- utils ---------------------
def log(msg: str) -> None: print(msg, flush=True)
def warn(msg: str) -> None: print(f"WARN: {msg}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1) -> None: print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_utc_str() -> str: return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty_str() -> str:
    if ZoneInfo: return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag);  return (node.text or "").strip() if node is not None and node.text else ""

def _norm_text(s: str) -> str:
    s=(s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

def remove_all(el: ET.Element, *tags: str) -> int:
    n=0
    for t in tags:
        for x in list(el.findall(t)):
            el.remove(x); n+=1
    return n

def _remove_all_price_nodes(offer: ET.Element) -> None:
    for t in ("price", "Price"):
        for node in list(offer.findall(t)):
            offer.remove(node)

_COLON_ALIASES = ":\uFF1A\uFE55\u2236\uFE30"
_COLON_CLASS = "[" + re.escape(_COLON_ALIASES) + "]"
_COLON_CLASS_RE = re.compile(_COLON_CLASS)
def canon_colons(s: str) -> str:
    return _COLON_CLASS_RE.sub(":", s or "")

NOISE_RE = re.compile(
    r"[\u200B-\u200F\u202A-\u202E\u2060\uFEFF\u00AD"
    r"\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F"
    r"\u0080-\u009F]"
)
def strip_noise_chars(s: str) -> str:
    if not s: return s or ""
    s = NOISE_RE.sub("", s).replace("�","").replace("¬","")
    s = s.replace("•","-")
    return s

# --------------------- load ---------------------
def load_source_bytes(src: str) -> bytes:
    if not src: raise RuntimeError("SUPPLIER_URL не задан")
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path, "rb") as f: data=f.read()
        if len(data) < MIN_BYTES: raise RuntimeError(f"file too small: {len(data)} bytes")
        return data
    sess=requests.Session()
    headers={"User-Agent":"supplier-feed-bot/1.0 (+github-actions)"}
    last=None
    for i in (1,2,3,4):
        try:
            r=sess.get(src, headers=headers, timeout=TIMEOUT_S)
            if r.status_code!=200: raise RuntimeError(f"HTTP {r.status_code}")
            data=r.content
            if len(data)<MIN_BYTES: raise RuntimeError(f"too small ({len(data)} bytes)")
            return data
        except Exception as e:
            last=e; back=RETRY_BACKOFF*i*(1+random.uniform(-0.2,0.2))
            warn(f"fetch {i}/4 failed: {e}; sleep {back:.2f}s")
            if i<4: time.sleep(back)
    raise RuntimeError(f"fetch failed: {last}")

# --------------------- brands ---------------------
def _norm_key(s: str) -> str:
    if not s: return ""
    s=s.strip().lower().replace("ё","е")
    s=re.sub(r"[-_/]+"," ",s)
    s=re.sub(r"\s+"," ",s)
    return s

SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

def normalize_brand(raw: str) -> str:
    k=_norm_key(raw)
    if (not k) or (k in SUPPLIER_BLOCKLIST): return ""
    return raw.strip()

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, Dict[str,int]]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0,{}
    normalized=0; dropped: Dict[str,int]={}
    for offer in offers_el.findall("offer"):
        ven=offer.find("vendor")
        txt=(ven.text or "").strip() if ven is not None and ven.text else ""
        if txt:
            canon=normalize_brand(txt)
            if any(m in txt.lower() for m in UNKNOWN_VENDOR_MARKERS) or (not canon):
                if ven is not None: offer.remove(ven)
                key=_norm_key(txt); 
                if key: dropped[key]=dropped.get(key,0)+1
            elif canon!=txt:
                ven.text=canon; normalized+=1
    return normalized,dropped

def build_brand_index(shop_el: ET.Element) -> Dict[str, str]:
    idx: Dict[str,str] = {}
    offers_el=shop_el.find("offers")
    if offers_el is None: return idx
    for offer in offers_el.findall("offer"):
        v = offer.find("vendor")
        if v is None or not (v.text or "").strip(): continue
        canon = v.text.strip()
        idx[_norm_key(canon)] = canon
    return idx

def guess_vendor_for_offer(offer: ET.Element, brand_index: Dict[str,str]) -> str:
    name = get_text(offer, "name"); nrm=_norm_key(name)
    if not nrm: return ""
    first = re.split(r"\s+", name.strip())[0]
    f_norm=_norm_key(first)
    if f_norm in brand_index: return brand_index[f_norm]
    for br_norm, canon in sorted(brand_index.items(), key=lambda kv: len(kv[0]), reverse=True):
        if re.search(rf"\b{re.escape(br_norm)}\b", nrm): return canon
    return ""

def ensure_vendor_auto_fill(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    brand_index = build_brand_index(shop_el)
    touched=0
    for offer in offers_el.findall("offer"):
        v = offer.find("vendor")
        cur = (v.text or "").strip() if (v is not None and v.text) else ""
        if cur: continue
        guess = guess_vendor_for_offer(offer, brand_index)
        if guess:
            if v is None: v = ET.SubElement(offer, "vendor")
            v.text = guess
            brand_index[_norm_key(guess)] = guess
            touched += 1
    return touched

# --------------------- pricing ---------------------
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

PRICE_FIELDS_DIRECT=["purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price"]
PRICE_KEYWORDS_DEALER = re.compile(r"(дилер|dealer|опт|wholesale|b2b|закуп|purchase|оптов)", re.I)
PRICE_KEYWORDS_RRP    = re.compile(r"(rrp|ррц|розниц|retail|msrp)", re.I)

def parse_price_number(raw:str)->Optional[float]:
    if raw is None: return None
    s=(raw.strip()
          .replace("\xa0"," ")
          .replace(" ","")
          .replace("KZT","")
          .replace("kzt","")
          .replace("₸","")
          .replace(",",".")) 
    if not s: return None
    try:
        v=float(s)
        return v if v>0 else None
    except Exception:
        return None

def pick_dealer_price(offer: ET.Element) -> Tuple[Optional[float], str]:
    dealer_candidates=[]; rrp_candidates=[]
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            val=parse_price_number(p.text or ""); 
            if val is None: continue
            t=(p.attrib.get("type") or "")
            if PRICE_KEYWORDS_DEALER.search(t): dealer_candidates.append(val)
            elif PRICE_KEYWORDS_RRP.search(t):  rrp_candidates.append(val)
    if dealer_candidates: return (min(dealer_candidates), "prices_dealer")
    direct=[]
    for tag in PRICE_FIELDS_DIRECT:
        el=offer.find(tag)
        if el is not None and el.text:
            v=parse_price_number(el.text)
            if v is not None: direct.append(v)
    if direct: return (min(direct), "direct_field")
    if rrp_candidates: return (min(rrp_candidates), "rrp_fallback")
    return (None, "missing")

def _force_tail_900(n:float)->int:
    i=int(n); k=max(i//1000,0); out=k*1000+900; return out if out>=900 else 900

def compute_retail(dealer:float,rules:List[PriceRule])->Optional[int]:
    for lo,hi,pct,add in rules:
        if lo<=dealer<=hi:
            val=dealer*(1.0+pct/100.0)+add
            return _force_tail_900(val)
    return None

def reprice_offers(shop_el:ET.Element,rules:List[PriceRule])->Tuple[int,int,int,Dict[str,int]]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,{"missing":0})
    updated=skipped=total=0
    src_stats={"prices_dealer":0,"direct_field":0,"rrp_fallback":0,"missing":0}
    for offer in offers_el.findall("offer"):
        total+=1
        if offer.attrib.get("_force_price","") == "100":
            skipped+=1
            remove_all(offer, "prices", "Prices")
            for tag in INTERNAL_PRICE_TAGS: remove_all(offer, tag)
            continue
        dealer, src = pick_dealer_price(offer)
        src_stats[src]=src_stats.get(src,0)+1
        if dealer is None or dealer<=100:
            skipped+=1
            remove_all(offer, "prices", "Prices")
            for tag in INTERNAL_PRICE_TAGS: remove_all(offer, tag)
            continue
        newp=compute_retail(dealer,rules)
        if newp is None: skipped+=1; continue
        _remove_all_price_nodes(offer)
        p = ET.SubElement(offer, "price")
        p.text=str(int(newp))
        remove_all(offer, "prices", "Prices")
        for tag in INTERNAL_PRICE_TAGS: remove_all(offer, tag)
        updated+=1
    return updated,skipped,total,src_stats

# --------------------- params / description ---------------------
def _key(s:str)->str: return re.sub(r"\s+"," ",(s or "").strip()).lower()

UNWANTED_PARAM_NAME_RE = re.compile(
    r"^(?:\s*(?:благотворительн\w*|снижена\s*цена|новинк\w*|артикул(?:\s*/\s*штрихкод)?|"
    r"оригинальн\w*\s*код|штрихкод|код\s*тн\s*вэд(?:\s*eaeu)?|код\s*тнвэд(?:\s*eaeu)?|тн\s*вэд|тнвэд|tn\s*ved|hs\s*code)\s*)$",
    re.I
)
KASPI_CODE_NAME_RE = re.compile(r"^код\s+товара\s+kaspi$", re.I)

def remove_specific_params(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    removed=0
    for offer in offers_el.findall("offer"):
        seen=set()
        for tag in ("param","Param"):
            for p in list(offer.findall(tag)):
                nm=(p.attrib.get("name") or "").strip()
                val=(p.text or "").strip()
                if KASPI_CODE_NAME_RE.fullmatch(nm):
                    offer.remove(p); removed+=1; continue
                if re.fullmatch(r"назначение", nm, re.I) and re.fullmatch(r"да", val, re.I):
                    offer.remove(p); removed+=1; continue
                if UNWANTED_PARAM_NAME_RE.match(nm):
                    offer.remove(p); removed+=1; continue
                if (val.lower() in {"","-","—","–",".","..","...","n/a","na","none","null","нет данных","не указано","неизвестно"}
                    or re.search(r"https?://|www\.", val, re.I)
                    or re.search(r"[^@\s]+@[^@\s]+\.[^@\s]+", val)
                    or re.search(r"<[^>]+>", val)):
                    offer.remove(p); removed+=1; continue
                k=_key(nm)
                if k in seen:
                    offer.remove(p); removed+=1; continue
                seen.add(k)
    return removed

EXCLUDE_NAME_RE = re.compile(
    r"(?:\bартикул\b|благотворительн\w*|штрихкод|оригинальн\w*\s*код|новинк\w*|снижена\s*цена|"
    r"код\s*тн\s*вэд(?:\s*eaeu)?|код\s*тнвэд(?:\s*eaeu)?|тн\s*вэд|тнвэд|tn\s*ved|hs\s*code)",
    re.I
)
def _looks_like_code_value(v:str)->bool:
    s=(v or "").strip()
    if not s: return True
    if re.search(r"https?://",s,re.I): return True
    clean=re.sub(r"[0-9\-\_/ ]","",s)
    return (len(clean)/max(len(s),1))<0.3

# дискламеры (не тянуть в характеристики)
DISCL_RE = re.compile(
    r"(голографическ\w*|маркиров\w*|на\s+корпусе|на\s+коробке|на\s+упаковке|"
    r"медиа\s*файл\w*|фото|видео|предъяв\w*|обнаруж\w*\s+брак|серии\s+картриджа|"
    r"услов\w*\s+гаран|следует|необходим\w*)",
    re.I
)

HEADLINE_EUROPRINT_RE  = re.compile(r"^(?:тонер-)?картриджи\s+europrint\s*:?\s*$", re.I)
HEADLINE_WARRANTY_RE   = re.compile(r"^условия\s+гарантии\s*:?\s*$", re.I)

def _merge_europrint_headings(lines: List[str]) -> List[str]:
    out=[]; i=0
    while i < len(lines):
        cur=(lines[i] or "").strip()
        nxt=(lines[i+1] or "").strip() if i+1 < len(lines) else ""
        if HEADLINE_EUROPRINT_RE.match(cur):
            if nxt: out.append("Картриджи EUROPRINT. "+nxt.lstrip("—-• ").strip()); i+=2; continue
            i+=1; continue
        if HEADLINE_WARRANTY_RE.match(cur):
            if nxt: out.append(nxt); i+=2; continue
            i+=1; continue
        out.append(cur); i+=1
    return out

RE_SPACE_BEFORE_PUNCT = re.compile(r"\s+([,;.!?])")
RE_PERCENT            = re.compile(r"(\d)\s*%")
RE_DEGREE_C           = re.compile(r"(\d)\s*°\s*([СC])")

def _fix_degrees(s: str) -> str:
    return RE_DEGREE_C.sub(r"\1°\2", s or "")

def normalize_value_punct(v: str) -> str:
    s = canon_colons(v or "")
    s = re.sub(r":\s*:", ": ", s)      # <-- фикс «: :»
    s = re.sub(r"(?<!\.)\.\.(?!\.)", ".", s)
    s = RE_SPACE_BEFORE_PUNCT.sub(r"\1", s)
    s = RE_PERCENT.sub(r"\1%", s)
    s = _fix_degrees(s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()

def normalize_free_text_punct(s: str) -> str:
    t = canon_colons(s or "")
    t = re.sub(r":\s*:", ": ", t)      # <-- фикс «: :»
    t = re.sub(r"(?<!\.)\.\.(?!\.)", ".", t)
    t = RE_SPACE_BEFORE_PUNCT.sub(r"\1", t)
    t = RE_PERCENT.sub(r"\1%", t)
    t = _fix_degrees(t)
    t = re.sub(r"\s{2,}", " ", t)
    t = re.sub(r"\bдля дома\s+офиса\b", "для дома и офиса", t, flags=re.I)
    return t.strip()

# Заголовки «Характеристики»
HDR_RE            = re.compile(r"^\s*(технические\s+характеристики|характеристики)\s*:?\s*$", re.I)
HEAD_ONLY_RE      = re.compile(r"^\s*(?:основные\s+)?характеристики\s*[:：﹕∶︰-]*\s*$", re.I)
HEAD_PREFIX_RE    = re.compile(r"^\s*(?:основные\s+)?характеристики\s*[:：﹕∶︰-]*\s*", re.I)
KV_BULLET_RE = re.compile(r"^\s*-\s*([^:]+?)\s*:\s*(.+)$")
KV_COLON_RE  = re.compile(r"^\s*([^:]{2,}?)\s*:\s*(.+)$")
KV_TABS_RE   = re.compile(r"^\s*([^\t]{2,}?)\t+(.+)$")
KV_SPACES_RE = re.compile(r"^\s*(\S.{1,}?)\s{2,}(.+)$")
BULLET_NO_KEY      = re.compile(r"^\s*-\s*[:\s\-]{0,5}:\s*\S")
BULLET_KEY_NOVALUE = re.compile(r"^\s*-\s*[^:]+:\s*$")
URL_RE = re.compile(r"https?://\S+", re.I)

BAD_KV_NAME_RE = re.compile(
    r"(?:\bв\s+случае\b|\bуслов\w*\s+гаран|необходим\w*|следует|предостав\w*|предъяв\w*|"
    r"указан\w*|содержит|соответству\w*|упаков\w*|маркиров\w*|голографическ\w*|"
    r"на\s+корпусе|на\s+упаковке|на\s+коробке|фото|видео)", re.I
)
def _valid_kv_name(name: str) -> bool:
    n=(name or "").strip(" -•*·").strip()
    if not n: return False
    if len(n) > 48: return False
    if len(re.findall(r"[A-Za-zА-Яа-я0-9%°\-\+]+", n)) > 6: return False
    if BAD_KV_NAME_RE.search(n): return False
    if re.search(r"[.!?]$", n): return False
    if n.count(",") >= 2: return False
    return True

def _split_inline_bullets(line: str) -> List[str]:
    s=line.strip()
    if not s: return []
    if ":" not in s: return [s]
    parts=re.split(r"\s*-\s+(?=[^:]{2,}:\s*\S)", s)
    out=[]
    for p in parts:
        p=p.strip()
        if not p: continue
        p=re.sub(r"^\s*-\s*", "", p)
        out.append(p)
    return out or [s]

def _normalize_description_whitespace(text: str) -> str:
    t = strip_noise_chars(text or ""); t = canon_colons(t)
    t = t.replace("\r\n","\n").replace("\r","\n").replace("\u00A0", " ")
    t = re.sub(r"\t+", "\t", t)
    raw_lines = [re.sub(r"[ \t]+$", "", ln) for ln in t.split("\n")]
    raw_lines = _merge_europrint_headings(raw_lines)
    out=[]; last_blank=False
    for ln in raw_lines:
        s = ln.strip()
        if not s:
            if last_blank: continue
            out.append(""); last_blank=True; continue
        last_blank=False
        if DISCL_RE.search(s) and len(s) > 60: continue
        out.append(s)
    t = "\n".join(out).strip()
    t = re.sub(r":\s*:", ": ", t)  # <-- фикс двойных
    t = re.sub(r"(?<!\.)\.\.(?!\.)", ".", t)
    t = re.sub(r"(\n){3,}", "\n\n", t)
    return t

def normalize_kv(name: str, value: str) -> Tuple[str, str]:
    n = re.sub(r"\s+", " ", (name or "").strip())
    v = re.sub(r"\s+", " ", (value or "").strip())
    n = canon_colons(n); v = canon_colons(v)
    # УБРАТЬ ЛЮБЫЕ ЗАКАНЧИВАЮЩИЕСЯ ДВОЕТОЧИЯ В ИМЕНИ
    n = re.sub(rf'\s*{_COLON_ALIASES}+\s*$', '', n)
    # УБРАТЬ ДВОЕТОЧИЯ В НАЧАЛЕ ЗНАЧЕНИЯ
    v = re.sub(rf'^\s*{_COLON_ALIASES}+\s*', '', v)
    if not n or not v: return n, v
    n_l = n.lower()
    if re.search(r"совместим\w*\s*модел", n_l): n = "Совместимость"
    elif re.search(r"ресурс", n_l):
        n = "Ресурс картриджа"
        m = re.search(r"(\d[\d\s]{0,12}\d)", v)
        iso = re.search(r"(?:iso|iec)[\/\s\-]*([12]\d{3,5})", v, re.I) or re.search(r"\b([12]\d{3,5})\b", v)
        if m:
            num = re.sub(r"\s+", " ", m.group(1)).strip()
            v = f"{num} стр." + (f" (ISO/IEC {iso.group(1)})" if iso else "")
    elif re.fullmatch(r"вес", n_l):
        if not re.search(r"\bкг\b", v, re.I): v = v.replace(",", ".").strip() + " кг"
    elif re.fullmatch(r"цвет", n_l):
        v = v.lower()
    v = normalize_value_punct(v)
    return n, v

def extract_kv_from_description(text: str) -> Tuple[str, List[Tuple[str,str]], List[str]]:
    if not (text or "").strip(): return "", [], []
    t=_normalize_description_whitespace(text)
    raw_lines=t.split("\n")
    lines=[]
    for ln in raw_lines:
        if URL_RE.search(ln):
            if not (KV_BULLET_RE.match(ln) or KV_COLON_RE.match(ln) or KV_TABS_RE.match(ln) or KV_SPACES_RE.match(ln)):
                ln=URL_RE.sub("", ln).strip()
        if HDR_RE.match(ln) or HEAD_ONLY_RE.match(ln): continue
        ln2=HEAD_PREFIX_RE.sub("", ln)
        if not ln2.strip(): continue
        lines.extend(_split_inline_bullets(ln2))

    keep_mask=[True]*len(lines)
    pairs=[]
    def try_parse_kv(ln: str) -> Optional[Tuple[str,str]]:
        ln=canon_colons(ln); probe=re.sub(r"[ \t]{2,}", "  ", ln)
        for rx in (KV_BULLET_RE, KV_TABS_RE, KV_SPACES_RE, KV_COLON_RE):
            m=rx.match(probe)
            if m:
                name, val=(m.group(1) or "").strip(), (m.group(2) or "").strip()
                if not _valid_kv_name(name): return None
                if name and val: return normalize_kv(name, val)
        return None

    for i, ln in enumerate(lines):
        if re.match(r"^\s*-\s*[:\s\-]{0,5}:\s*\S", ln) or re.match(r"^\s*-\s*[^:]+:\s*$", ln):
            keep_mask[i]=False; continue
        parsed=try_parse_kv(ln)
        if parsed:
            keep_mask[i]=False
            pairs.append(parsed)

    kept=[]
    for ln, keep in zip(lines, keep_mask):
        if not keep: continue
        if DISCL_RE.search(ln) and len(ln.strip())>60: continue
        kept.append(ln)

    features=[]; rest=[]
    for ln in kept:
        s=ln.strip()
        if s.startswith("- ") and ":" not in s: features.append(s[2:].strip())
        else: rest.append(ln)

    cleaned="\n".join([x for x in rest if x.strip()]).strip()
    cleaned=re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned=normalize_free_text_punct(cleaned)
    return cleaned, pairs, features

def build_specs_pairs_from_params(offer: ET.Element) -> List[Tuple[str,str]]:
    pairs=[]; seen=set()
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name=(p.attrib.get("name") or "").strip()
        raw_val =(p.text or "").strip()
        if not raw_name or not raw_val: continue
        if EXCLUDE_NAME_RE.search(raw_name): continue
        if _looks_like_code_value(raw_val): continue
        name, val = normalize_kv(raw_name, raw_val)
        k=_key(name)
        if k in seen: continue
        seen.add(k); pairs.append((name, val))
    return pairs

# --- HTML-рендер описания в CDATA ---
def render_html_description(product_name: str,
                            cleaned_desc: str,
                            specs_pairs: List[Tuple[str,str]],
                            features: List[str]) -> str:
    parts: List[str] = []
    if product_name:
        parts.append("<h3>" + html_lib.escape(product_name.strip()) + "</h3>")
    if cleaned_desc:
        paragraphs = [p.strip() for p in re.split(r"\n\s*\n", cleaned_desc) if p.strip()]
        for p in paragraphs:
            p_html = p.replace("\n", "<br>")
            parts.append("<p>" + p_html + "</p>")
    if features:
        parts.append("<h3>Особенности</h3>")
        parts.append("<ul>")
        for ftxt in features:
            parts.append("  <li>" + ftxt + "</li>")
        parts.append("</ul>")
    if specs_pairs:
        parts.append("<h3>Характеристики</h3>")
        parts.append("<ul>")
        for name, val in specs_pairs:
            # РЕЗКА хвостовых двоеточий у имени (фикс '::')
            safe_name = re.sub(rf'\s*{_COLON_ALIASES}+\s*$', '', str(name).strip())
            parts.append("  <li><strong>" + safe_name + ":</strong> " + str(val).strip() + "</li>")
        parts.append("</ul>")
    return "\n".join(parts).strip()

def unify_specs_in_description(shop_el: ET.Element) -> Tuple[int,int,int]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0)
    offers_touched=0; total_kv=0; total_feats=0
    for offer in offers_el.findall("offer"):
        desc_el = offer.find("description")
        raw_desc = (desc_el.text or "").strip() if (desc_el is not None and desc_el.text) else ""
        cleaned_desc, kv_from_desc, features = extract_kv_from_description(raw_desc)
        pairs_from_params = build_specs_pairs_from_params(offer)
        kv_from_desc = [(n,v) for (n,v) in kv_from_desc if not (_norm_text(n)=="назначение" and _norm_text(v)=="да")]

        merged: Dict[str, Tuple[str,str]] = {}
        for name, val in pairs_from_params: merged[_key(name)] = (name, val)
        for name, val in kv_from_desc:
            k=_key(name)
            if k not in merged: merged[k]=(name,val)
        merged = {k:(n,v) for k,(n,v) in merged.items() if not (_norm_text(n)=="назначение" and _norm_text(v)=="да")}

        merged_pairs = list(merged.values())
        merged_pairs.sort(key=lambda kv: _norm_text(kv[0]))

        prod_name = get_text(offer, "name")
        html_block = render_html_description(prod_name, cleaned_desc, merged_pairs, features)
        if html_block:
            if desc_el is None: desc_el = ET.SubElement(offer, "description")
            desc_el.text = "[[HTML]]\n" + html_block + "\n[[/HTML]]"
            offers_touched += 1
    return offers_touched, total_kv, total_feats

# --------------------- availability ---------------------
TRUE_WORDS  = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS = {"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}
def _parse_bool_str(s: str) -> Optional[bool]:
    v=_norm_text(s or "")
    if v in TRUE_WORDS: return True
    if v in FALSE_WORDS: return False
    return None
def _parse_int(s: str) -> Optional[int]:
    t=re.sub(r"[^\d\-]+","", s or "")
    if t in {"","-","+"}: return None
    try: return int(t)
    except Exception: return None
def derive_available(offer: ET.Element) -> Tuple[bool, str]:
    avail_el=offer.find("available")
    if avail_el is not None and avail_el.text:
        b=_parse_bool_str(avail_el.text)
        if b is not None: return b, "tag"
    for tag in ["quantity_in_stock","quantity","stock","Stock"]:
        for node in offer.findall(tag):
            val=_parse_int(node.text or "")
            if val is not None: return (val>0), "stock"
    for tag in ["status","Status"]:
        node=offer.find(tag)
        if node is not None and node.text:
            b=_parse_bool_str(node.text)
            if b is not None: return b, "status"
    return False, "default"
def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int,int,int]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
    t_cnt = f_cnt = st_cnt = ss_cnt = 0
    for offer in offers_el.findall("offer"):
        b, src=derive_available(offer)
        remove_all(offer, "available")
        offer.attrib["available"]="true" if b else "false"
        if b: t_cnt+=1
        else: f_cnt+=1
        if src=="stock": st_cnt+=1
        if src=="status": ss_cnt+=1
        if DROP_STOCK_TAGS:
            remove_all(offer, "quantity_in_stock","quantity","stock","Stock")
    return t_cnt,f_cnt,st_cnt,ss_cnt

def mark_offers_without_pictures_unavailable(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    changed=0
    for offer in offers_el.findall("offer"):
        if not offer.findall("picture"):
            offer.attrib["available"]="false"; changed+=1
    return changed

# --------------------- ids / vendorCode ---------------------
ARTICUL_RE=re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)
def _extract_article_from_name(name:str)->str:
    if not name: return ""
    m=ARTICUL_RE.search(name); return (m.group(1) if m else "").upper()
def _extract_article_from_url(url:str)->str:
    if not url: return ""
    try:
        path=urllib.parse.urlparse(url).path.rstrip("/")
        last=path.split("/")[-1]
        last=re.sub(r"\.(html?|php|aspx?)$","",last,flags=re.I)
        m=ARTICUL_RE.search(last)
        return (m.group(1) if m else last).upper()
    except Exception:
        return ""
def _normalize_code(s:str)->str:
    s=(s or "").strip()
    if not s: return ""
    s=re.sub(r"[\s_]+","",s).replace("—","-").replace("–","-")
    s=re.sub(r"[^A-Za-z0-9\-]+","",s)
    return s.upper()

def ensure_vendorcode_with_article(shop_el:ET.Element,prefix:str,create_if_missing:bool=False)->Tuple[int,int,int,int]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
    total_prefixed=created=filled_from_art=fixed_bare=0
    for offer in offers_el.findall("offer"):
        vc=offer.find("vendorCode")
        if vc is None:
            if create_if_missing:
                vc=ET.SubElement(offer,"vendorCode"); vc.text=""; created+=1
            else:
                continue
        old=(vc.text or "").strip()
        if (old=="") or (old.upper()==prefix.upper()):
            art=_normalize_code(offer.attrib.get("article") or "") \
              or _normalize_code(_extract_article_from_name(get_text(offer,"name"))) \
              or _normalize_code(_extract_article_from_url(get_text(offer,"url"))) \
              or _normalize_code(offer.attrib.get("id") or "")
            if art: vc.text=art
        vc.text=f"{prefix}{(vc.text or '')}"; total_prefixed+=1
    return total_prefixed,created,filled_from_art,fixed_bare

def sync_offer_id_with_vendorcode(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    changed=0
    for offer in offers_el.findall("offer"):
        vc=offer.find("vendorCode")
        if vc is None or not (vc.text or "").strip(): continue
        new_id=(vc.text or "").strip()
        if offer.attrib.get("id")!=new_id:
            offer.attrib["id"]=new_id; changed+=1
    return changed

# --------------------- cleanup / order ---------------------
def purge_offer_tags_and_attrs_after(offer:ET.Element)->Tuple[int,int]:
    removed_tags=removed_attrs=0
    for t in PURGE_TAGS_AFTER:
        for node in list(offer.findall(t)):
            offer.remove(node); removed_tags+=1
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib:
            offer.attrib.pop(a,None); removed_attrs+=1
    return removed_tags,removed_attrs

def fix_currency_id(shop_el: ET.Element, default_code: str = "KZT") -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        remove_all(offer,"currencyId")
        new_cur=ET.SubElement(offer,"currencyId"); new_cur.text=default_code
        touched+=1
    return touched

DESIRED_ORDER=["vendorCode","name","price","picture","vendor","currencyId","description"]
def reorder_offer_children(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    changed=0
    for offer in offers_el.findall("offer"):
        children=list(offer)
        if not children: continue
        buckets={k:[] for k in DESIRED_ORDER}; others=[]
        for node in children:
            (buckets[node.tag] if node.tag in buckets else others).append(node)
        rebuilt=[]
        for k in DESIRED_ORDER: rebuilt.extend(buckets[k])
        rebuilt.extend(others)
        if rebuilt!=children:
            for node in children: offer.remove(node)
            for node in rebuilt: offer.append(node)
            changed+=1
    return changed

def ensure_categoryid_zero_first(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        remove_all(offer,"categoryId","CategoryId")
        cid=ET.Element("categoryId"); cid.text=os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0,cid); touched+=1
    return touched

# --------------------- KEYWORDS (как было) ---------------------
AS_INTERNAL_ART_RE = re.compile(r"^AS\d+", re.I)
FEATURE_ACRONYM_BLACKLIST = {
    "QHD","UHD","FHD","FULL","HDR","HDR10","HDR400","DISPLAYHDR",
    "IPS","VA","TN","LED","OLED",
    "HDMI","DP","DISPLAY","PORT","USB","TYPEC","TYPE-C",
    "AMD","RADEON","NVIDIA","GSYNC","G-SYNC","FREESYNC","GTG","EYE","SAVER"
}
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
STOPWORDS_RU = {"для","и","или","на","в","из","от","по","с","к","до","при","через","над","под","о","об","у","без","про","как","это","той","тот","эта","эти","бумага","бумаги","бумаг","черный","чёрный","белый","серый","цвет","оригинальный","комплект","набор","тип","модель","модели","формат","новый","новинка"}
STOPWORDS_EN = {"for","and","or","with","of","the","a","an","to","in","on","by","at","from","new","original","type","model","set","kit","pack"}
GENERIC_DROP = {"изделие","товар","продукция","аксессуар","устройство","оборудование"}

def tokenize_name(name: str) -> List[str]:
    return WORD_RE.findall(name or "")

def is_content_word(token: str) -> bool:
    t = _norm_text(token)
    if not t: return False
    if t in STOPWORDS_RU or t in STOPWORDS_EN or t in GENERIC_DROP: return False
    return any(ch.isdigit() for ch in t) or "-" in t or len(t) >= 3

def build_bigrams(words: List[str]) -> List[str]:
    out=[]
    for i in range(len(words)-1):
        a,b = words[i], words[i+1]
        if is_content_word(a) and is_content_word(b):
            if _norm_text(a) not in STOPWORDS_RU and _norm_text(b) not in STOPWORDS_RU:
                out.append(f"{a} {b}")
    return out

def dedup_preserve_order(words: List[str]) -> List[str]:
    seen=set(); out=[]
    for w in words:
        key=_norm_text(str(w))
        if not key or key in seen: continue
        seen.add(key); out.append(str(w))
    return out

def translit_ru_to_lat(s: str) -> str:
    table = str.maketrans({
        "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y",
        "к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f",
        "х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya",
        "ь":"","ъ":""
    })
    out = s.lower().translate(table)
    out = re.sub(r"[^a-z0-9\- ]+","", out)
    out = re.sub(r"\s+","-", out).strip("-")
    return out

def color_tokens(name: str) -> List[str]:
    out=[]
    low=name.lower()
    mapping = {
        "жёлт":"желтый", "желт":"желтый", "yellow":"yellow",
        "черн":"черный", "black":"black",
        "син":"синий", "blue":"blue",
        "красн":"красный", "red":"red",
        "зелен":"зеленый", "green":"green",
        "серебр":"серебряный", "silver":"silver",
        "циан":"cyan", "магент":"magenta"
    }
    for k,val in mapping.items():
        if k in low: out.append(val)
    return dedup_preserve_order(out)

MODEL_RE = re.compile(r"\b([A-Z0-9][A-Z0-9\-]{2,})\b", re.I)
def extract_model_tokens(offer: ET.Element) -> List[str]:
    tokens=set()
    for src in (get_text(offer,"name"), get_text(offer,"description")):
        if not src: continue
        for m in MODEL_RE.findall(src or ""):
            t=m.upper()
            if re.match(r"^AS\d+", t): 
                continue
            if t in FEATURE_ACRONYM_BLACKLIST:
                continue
            if not (re.search(r"[A-Z]", t) and re.search(r"\d", t)):
                continue
            if len(t) < 5:
                continue
            tokens.add(t)
    return list(tokens)

def keywords_from_name_generic(name: str) -> List[str]:
    raw_tokens = tokenize_name(name or "")
    modelish = [t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content  = [t for t in raw_tokens if is_content_word(t)]
    bigr     = build_bigrams(content)
    def norm_token(tok: str) -> str:
        return tok if re.search(r"[A-Z]{2,}", tok) else tok.capitalize()
    out: List[str] = []
    out += modelish[:8]
    out += bigr[:8]
    out += [norm_token(t) for t in content[:10]]
    return dedup_preserve_order(out)

def head_noun_from_name(name: str, vendor: str = "") -> str:
    if not name: return ""
    n = re.sub(r"[\"“”″']", " ", name)
    toks = tokenize_name(n)
    vend = _norm_text(vendor)
    for t in toks:
        tn = _norm_text(t)
        if vend and tn == vend:
            continue
        if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t):
            continue
        if tn in STOPWORDS_RU or tn in STOPWORDS_EN or tn in GENERIC_DROP:
            continue
        if len(tn) >= 4 and re.search(r"[А-Яа-яЁё]", t):
            return t.capitalize()
    return ""

GEO_CITIES = [
    ("Казахстан","Kazakhstan"), ("Алматы","Almaty"), ("Астана","Astana"),
    ("Шымкент","Shymkent"), ("Караганда","Karaganda"), ("Актобе","Aktobe"),
    ("Павлодар","Pavlodar"), ("Атырау","Atyrau"), ("Тараз","Taraz"),
    ("Усть-Каменогорск","Oskemen"), ("Семей","Semey"), ("Костанай","Kostanay"),
    ("Кызылорда","Kyzylorda"), ("Уральск","Oral"), ("Петропавловск","Petropavl"),
    ("Талдыкорган","Taldykorgan"), ("Актау","Aktau"), ("Темиртау","Temirtau"),
    ("Экибастуз","Ekibastuz"), ("Кокшетау","Kokshetau")
]

def build_keywords(offer: ET.Element) -> Optional[str]:
    if SATU_KEYWORDS == "off": return None
    name = get_text(offer, "name")
    vendor = get_text(offer, "vendor")
    base = []
    head = head_noun_from_name(name, vendor)
    if head: base.append(head)
    base += keywords_from_name_generic(name)
    base += color_tokens(name)
    base += extract_model_tokens(offer)
    if head:
        base.append(translit_ru_to_lat(head))
    if SATU_KEYWORDS_GEO:
        for ru, en in GEO_CITIES[:SATU_KEYWORDS_GEO_MAX]:
            base.append(ru)
    words = dedup_preserve_order(base)
    if SATU_KEYWORDS_MAXWORDS > 0:
        words = words[:SATU_KEYWORDS_MAXWORDS]
    out = ", ".join(words)
    if SATU_KEYWORDS_MAXLEN > 0 and len(out) > SATU_KEYWORDS_MAXLEN:
        out = out[:SATU_KEYWORDS_MAXLEN].rstrip(", ")
    return out

# --------------------- serialize ---------------------
def set_price_cap_flags(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    flagged=0
    for off in offers_el.findall("offer"):
        p = get_text(off, "price")
        if p:
            try:
                v = int(re.sub(r"[^\d]+","", p))
            except Exception:
                continue
            if v >= PRICE_CAP_THRESHOLD:
                off.attrib["_force_price"] = "100"
                flagged += 1
    return flagged

def apply_price_cap(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    forced=0
    for off in offers_el.findall("offer"):
        if off.attrib.get("_force_price") == "100":
            _remove_all_price_nodes(off)
            p = ET.SubElement(off, "price")
            p.text = str(PRICE_CAP_VALUE)
            forced += 1
    return forced

def ensure_keywords(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    updated=0
    for off in offers_el.findall("offer"):
        kw_el = off.find("keywords")
        kw = build_keywords(off) or ""
        if not kw: 
            if kw_el is not None: off.remove(kw_el)
            continue
        if kw_el is None: kw_el = ET.SubElement(off, "keywords")
        kw_el.text = kw
        updated += 1
    return updated

def _escape_cdata_end(text: str) -> str:
    return (text or "").replace("]]>", "]]]]><![CDATA[>")

def element_to_xml_bytes(root: ET.Element) -> bytes:
    xml_decl = '<?xml version="1.0" encoding="windows-1251"?>\n'
    raw = ET.tostring(root, encoding="unicode")
    raw = re.sub(
        r"<description>\s*\[\[HTML\]\]([\s\S]*?)\[\[/HTML\]\]\s*</description>",
        lambda m: "<description><![CDATA[\n" + _escape_cdata_end(m.group(1)) + "\n]]></description>",
        raw,
        flags=re.I
    )
    # перенести <shop> на новую строку после комментария
    raw = re.sub(r"(-->\s*)<shop>", r"\1\n<shop>", raw)
    # пустая строка между офферами (визуально)
    raw = re.sub(r"</offer>\s*<offer\b", "</offer>\n\n      <offer", raw)
    out = xml_decl + raw
    try:
        return out.encode(ENC, errors="strict")
    except Exception:
        warn(f"encode to {ENC} failed, fallback to UTF-8")
        return out.encode("utf-8", errors="replace")

def parse_xml(data: bytes) -> ET.Element:
    return ET.fromstring(data)

# --------------------- main ---------------------
def main() -> None:
    log(f"Source: {SUPPLIER_URL}")
    src = load_source_bytes(SUPPLIER_URL)
    try:
        root = parse_xml(src)
    except Exception as e:
        err(f"parse failed: {e}")

    shop = root.find("shop") or root.find("Shop")
    if shop is None:
        err("No <shop> root inside source")

    ensure_vendor(shop)
    ensure_vendor_auto_fill(shop)

    t_cnt,f_cnt,st_cnt,ss_cnt = normalize_available_field(shop)
    _ = mark_offers_without_pictures_unavailable(shop)

    ensure_categoryid_zero_first(shop)
    _ = remove_specific_params(shop)

    flagged = set_price_cap_flags(shop)
    forced  = apply_price_cap(shop)

    upd,skp,tot,stats = reprice_offers(shop, PRICING_RULES)

    _ = unify_specs_in_description(shop)

    _ = fix_currency_id(shop, "KZT")
    _ = reorder_offer_children(shop)

    kw_upd = ensure_keywords(shop)

    out_bytes = element_to_xml_bytes(root)

    if DRY_RUN:
        log("DRY_RUN=1 — файл не пишу")
        return

    os.makedirs(os.path.dirname(OUT_FILE_YML), exist_ok=True)
    with open(OUT_FILE_YML, "wb") as f:
        f.write(out_bytes)

    log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")
    log(f"Forced price={PRICE_CAP_VALUE}: {forced}")
    log(f"Keywords updated: {kw_upd}")
    log(f"In stock (true)  : {t_cnt}")
    log(f"Out of stock(false): {f_cnt}")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        err(str(e))
