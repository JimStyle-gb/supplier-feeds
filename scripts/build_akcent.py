#!/usr/bin/env python3
# scripts/build_akcent.py
# -*- coding: utf-8 -*-

"""
Akcent feed builder — режим: характеристики только из <param>, "родное" описание переписываем в SEO-вид.

Главные отличия:
- Блок «Характеристики» берется ТОЛЬКО из <param> оффера (после фильтра).
- «Родное» описание поставщика не выводим как есть; вместо него — короткое, чистое SEO-описание,
  сформированное на основе имени, бренда и ключевых <param>.
- Из текста НИЧЕГО не добавляем в <param> (фича отключена).

Остальное (фильтр по товарам, фильтр <param> под Satu, репрайсинг, плейсхолдеры, keywords, FEED_META) — как было.
"""

from __future__ import annotations

import os, sys, re, time, json, random, hashlib, urllib.parse
from typing import Optional, List, Tuple, Dict
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from html import unescape as _unescape
from xml.etree import ElementTree as ET

try:
    import requests
except Exception:
    print("ERROR: 'requests' is required (pip install requests)", file=sys.stderr)
    raise

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

SCRIPT_VERSION = "akcent-2025-10-27.params-only-specs.v1"

# ===================== ENV / CONST =====================
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "Akcent").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/akcent.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()
TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# ---- Фильтр по названиям (file + env) ----
AKCENT_KEYWORDS_PATH  = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt").strip()
AKCENT_KEYWORDS_MODE  = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()  # include|exclude|off
AKCENT_KEYWORDS_LIST  = [s.strip() for s in os.getenv("AKCENT_KEYWORDS", "").split(",") if s.strip()]

# ---- Pricing ----
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "100"))
PriceRule = Tuple[int,int,float,int]
PRICING_RULES: List[PriceRule] = [
    (101,10000,4.0,3000),(10001,25000,4.0,4000),(25001,50000,4.0,5000),
    (50001,75000,4.0,7000),(75001,100000,4.0,10000),(100001,150000,4.0,12000),
    (150001,200000,4.0,15000),(200001,300000,4.0,20000),(300001,400000,4.0,25000),
    (400001,500000,4.0,30000),(500001,750000,4.0,40000),(750001,1000000,4.0,50000),
    (1000001,1500000,4.0,70000),(1500001,2000000,4.0,90000),(2000001,100000000,4.0,100000),
]
INTERNAL_PRICE_TAGS=("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                     "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice","max_price","maxPrice","oldprice")
PRICE_FIELDS_DIRECT=["purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price"]
PRICE_KEYWORDS_DEALER = re.compile(r"(дилер|dealer|опт|wholesale|b2b|закуп|purchase|оптов)", re.I)
PRICE_KEYWORDS_RRP    = re.compile(r"(rrp|ррц|розниц|retail|msrp)", re.I)

# ---- Placeholders ----
PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.akcent.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.akcent.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.akcent.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# ---- Чистки/порядок ----
DROP_CATEGORY_ID_TAG   = True
DROP_STOCK_TAGS        = True
PURGE_TAGS_AFTER       = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER= ("type","article")
DESIRED_ORDER          = ["vendorCode","name","price","picture","vendor","currencyId","description","keywords"]

# ---- Keywords ----
SATU_KEYWORDS_MAXLEN=1024
SATU_KEYWORDS_GEO=True
SATU_KEYWORDS_GEO_MAX=20
SATU_KEYWORDS_GEO_LAT=True

# ===================== PARAM FILTER (Satu/SEO) =====================
PARAM_FILTER_ENABLE = os.getenv("PARAM_FILTER_ENABLE", "1").lower() in {"1","true","yes","on"}

def _norm_param_name(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").strip().lower().replace("ё","е"))

# Белый список + паттерны (как было)
DEFAULT_PARAM_WHITELIST = {
    # Совместимость/принтеры
    "совместимость", "совместимость с моделями", "принтеры", "подходит для", "модели",
    # Мониторы/видео
    "диагональ", "разрешение", "тип матрицы", "частота обновления", "яркость", "контрастность",
    "время отклика", "угол обзора", "hdr", "версия hdmi",
    # Порты/связь
    "разъем", "разъемы", "разьем", "разьемы", "разъём", "разъёмы",
    "интерфейс", "интерфейсы", "usb", "hdmi", "displayport", "dp", "wi-fi", "wi fi", "bluetooth", "bt", "lan", "ethernet",
    # Энергия
    "мощность", "напряжение", "частота", "энергопотребление",
    # Общие
    "цвет", "страна", "страна производитель", "гарантия", "комплектация",
    "вес", "размеры", "габариты",
}
_PARAM_WL_NORM = {_norm_param_name(x) for x in DEFAULT_PARAM_WHITELIST}
_PARAM_ALLOWED_PATTERNS = [re.compile(p, re.I) for p in [
    r"^совместим", r"^принтер", r"^подходит", r"^модел",
    r"^диагонал", r"^разрешени", r"^тип матрицы$", r"^частот", r"^яркост", r"^контраст", r"^время отклик", r"^угол обзора$", r"^hdr$",
    r"^раз(ъ|е)м", r"^интерфейс(ы)?$", r"^(usb|hdmi|displayport|dp|wi-?fi|bluetooth|bt|lan|ethernet)$",
    r"^мощност", r"^напряжен", r"^энергопотреблен", r"^(вес|габарит(ы)?|размер(ы)?)$",
    r"^страна( производитель)?$", r"^гаранти", r"^комплектац", r"^цвет$",
]]

def _attr_ci(el: ET.Element, key: str) -> Optional[str]:
    k = key.lower()
    for a,v in el.attrib.items():
        if a.lower()==k:
            return v
    return None

def _param_allowed(name_raw: Optional[str]) -> bool:
    if not name_raw: return False
    n = _norm_param_name(name_raw)
    if n in _PARAM_WL_NORM: return True
    return any(p.search(n) for p in _PARAM_ALLOWED_PATTERNS)

def filter_params_for_satu(out_shop: ET.Element) -> Tuple[int,int,int]:
    if not PARAM_FILTER_ENABLE: return (0,0,0)
    off_el = out_shop.find("offers")
    if off_el is None: return (0,0,0)
    touched = kept = dropped = 0
    for offer in off_el.findall("offer"):
        changed=False
        for node in list(offer):
            if node.tag.lower()!="param": continue
            name_val = _attr_ci(node, "name")
            if _param_allowed(name_val):
                kept += 1; continue
            offer.remove(node); dropped += 1; changed=True
        if changed: touched += 1
    return (touched, kept, dropped)

# ===================== UTILS =====================
log  = lambda m: print(m, flush=True)
warn = lambda m: print(f"WARN: {m}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_utc() -> datetime: return datetime.now(timezone.utc)
def now_almaty() -> datetime:
    try:   return datetime.now(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(time.time()+5*3600)
    except Exception: return datetime.utcfromtimestamp(time.time()+5*3600)
def format_dt_almaty(dt: datetime) -> str: return dt.strftime("%d:%m:%Y - %H:%M:%S")
def next_build_time_almaty() -> datetime:
    cur = now_almaty(); t = cur.replace(hour=1, minute=0, second=0, microsecond=0)
    return t + timedelta(days=1) if cur >= t else t

def inner_html(el: ET.Element) -> str:
    if el is None: return ""
    parts=[]
    if el.text: parts.append(el.text)
    for ch in el:
        parts.append(ET.tostring(ch, encoding="unicode"))
        if ch.tail: parts.append(ch.tail)
    return "".join(parts).strip()

def _html_escape_in_cdata_safe(s: str) -> str:
    return (s or "").replace("]]>", "]]&gt;")

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag); return (node.text or "").strip() if node is not None and node.text else ""

def remove_all(el: ET.Element, *tags: str) -> int:
    n=0
    for t in tags:
        for x in list(el.findall(t)): el.remove(x); n+=1
    return n

# ===================== LOAD SOURCE =====================
def load_source_bytes(src: str) -> bytes:
    if not src: raise RuntimeError("SUPPLIER_URL не задан")
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path, "rb") as f: data=f.read()
        if len(data) < MIN_BYTES: raise RuntimeError(f"file too small: {len(data)}")
        return data
    sess=requests.Session(); headers={"User-Agent":"supplier-feed-bot/1.0 (+github-actions)"}
    last=None
    for i in range(1, RETRIES+1):
        try:
            r=sess.get(src, headers=headers, timeout=TIMEOUT_S)
            if r.status_code!=200: raise RuntimeError(f"HTTP {r.status_code}")
            data=r.content
            if len(data)<MIN_BYTES: raise RuntimeError(f"too small ({len(data)})")
            return data
        except Exception as e:
            last=e; back=RETRY_BACKOFF*i*(1+random.uniform(-0.2,0.2))
            warn(f"fetch {i}/{RETRIES} failed: {e}; sleep {back:.2f}s")
            if i<RETRIES: time.sleep(back)
    raise RuntimeError(f"fetch failed: {last}")

# ===================== NAME FILTER (file + env) =====================
class KeySpec:
    __slots__=("raw","kind","norm","pattern")
    def __init__(self, raw: str, kind: str, norm: Optional[str], pattern: Optional[re.Pattern]):
        self.raw, self.kind, self.norm, self.pattern = raw, kind, norm, pattern

def _norm_name(s: str) -> str:
    s=(s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ", s).strip()

def load_name_filter(path: str) -> List[KeySpec]:
    if not path or not os.path.exists(path): return []
    data=None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f: data=f.read()
            break
        except Exception: continue
    if data is None:
        with open(path,"r",encoding="utf-8",errors="ignore") as f: data=f.read()
    data = data.replace("\ufeff","").replace("\x00","")

    keys: List[KeySpec]=[]
    for ln in data.splitlines():
        s=ln.strip()
        if not s or s.startswith("#"): continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try: keys.append(KeySpec(s,"regex",None,re.compile(s[1:-1],re.I)))
            except Exception: pass
        else:
            n=_norm_name(s)
            if n: keys.append(KeySpec(s,"substr",n,None))
    return keys

def name_matches(name: str, keys: List[KeySpec]) -> bool:
    if not keys: return False
    n=_norm_name(name or "")
    for ks in keys:
        if ks.kind=="substr" and (n.startswith(ks.norm) or (ks.norm in n)):
            return True
        if ks.kind=="regex" and ks.pattern and ks.pattern.search(name or ""):
            return True
    return False

# ===================== BRAND / PRICE / AVAIL / ORDER =====================
def _norm_key(s: str) -> str:
    if not s: return ""
    s=s.strip().lower().replace("ё","е")
    s=re.sub(r"[-_/]+"," ",s); s=re.sub(r"\s+"," ",s); return s

SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a","китай","china")
COMMON_BRANDS=["Canon","HP","Hewlett-Packard","Xerox","Brother","Epson","BenQ","ViewSonic","Optoma","Acer","Panasonic","Sony",
               "Konica Minolta","Ricoh","Kyocera","Sharp","OKI","Pantum","Lenovo","Dell","ASUS","Samsung","Apple","MSI"]
BRAND_ALIASES={"hewlett packard":"HP","konica":"Konica Minolta","konica-minolta":"Konica Minolta",
               "viewsonic proj":"ViewSonic","epson proj":"Epson","epson projector":"Epson","benq proj":"BenQ",
               "hp inc":"HP","nvprint":"NV Print","nv print":"NV Print","gg":"G&G","g&g":"G&G"}

def normalize_brand(raw: str) -> str:
    k=_norm_key(raw)
    return "" if (not k) or (k in SUPPLIER_BLOCKLIST) else (BRAND_ALIASES.get(k) or raw.strip())

def build_brand_index(shop_el: ET.Element) -> Dict[str,str]:
    idx={}
    off_el=shop_el.find("offers")
    if off_el is None: return idx
    for offer in off_el.findall("offer"):
        v=offer.find("vendor")
        if v is not None and (v.text or "").strip():
            canon=v.text.strip(); idx[_norm_key(canon)] = canon
    return idx

def _find_brand_in_text(text: str) -> str:
    t=(text or "").lower()
    for a,canon in BRAND_ALIASES.items():
        if re.search(rf"\b{re.escape(a)}\b", t): return canon
    for b in COMMON_BRANDS:
        if re.search(rf"\b{re.escape(b.lower())}\b", t): return b
    m=re.match(r"^([A-Za-zА-Яа-яЁё]+)\b", (text or "").strip())
    if m:
        cand=m.group(1)
        for b in COMMON_BRANDS:
            if b.lower()==cand.lower(): return b
    return ""

def guess_vendor_for_offer(offer: ET.Element, brand_index: Dict[str,str]) -> str:
    name=get_text(offer,"name"); desc=inner_html(offer.find("description"))
    first=(re.split(r"\s+", name.strip())[0] if name else "")
    f_norm=_norm_key(first)
    if f_norm in brand_index: return brand_index[f_norm]
    b = _find_brand_in_text(name) or _find_brand_in_text(desc)
    return b

def ensure_vendor(shop_el: ET.Element) -> Tuple[int,int,int]:
    off_el=shop_el.find("offers")
    if off_el is None: return (0,0,0)
    idx=build_brand_index(shop_el); normalized=0; filled=0; removed=0
    for offer in off_el.findall("offer"):
        ven=offer.find("vendor")
        txt=(ven.text or "").strip() if ven is not None and ven.text else ""
        if txt:
            canon=normalize_brand(txt)
            alias=BRAND_ALIASES.get(_norm_key(txt))
            final=alias or canon
            if any(m in txt.lower() for m in UNKNOWN_VENDOR_MARKERS) or (not final):
                if ven is not None: offer.remove(ven); removed+=1
            elif final!=txt:
                ven.text=final; normalized+=1
        else:
            guess=guess_vendor_for_offer(offer, idx)
            if guess:
                if ven is None: ven=ET.SubElement(offer,"vendor")
                ven.text=guess; filled+=1
    return (normalized, filled, removed)

def parse_price_number(raw:str)->Optional[float]:
    if raw is None: return None
    s=raw.strip().replace("\xa0"," ").replace(" ","").replace("KZT","").replace("kzt","").replace("₸","").replace(",",".")
    if not s: return None
    try: v=float(s); return v if v>0 else None
    except Exception: return None

def pick_dealer_price(offer: ET.Element) -> Optional[float]:
    dealer=[]
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            val=parse_price_number(p.text or "")
            if val is None: continue
            t=(p.attrib.get("type") or "")
            if PRICE_KEYWORDS_DEALER.search(t) or not PRICE_KEYWORDS_RRP.search(t):
                dealer.append(val)
    for tag in PRICE_FIELDS_DIRECT:
        el=offer.find(tag)
        if el is not None and el.text:
            v=parse_price_number(el.text)
            if v is not None: dealer.append(v)
    return min(dealer) if dealer else None

_force_tail_900 = lambda n: max(int(n)//1000,0)*1000+900

def compute_retail(d:float,rules:List[PriceRule])->Optional[int]:
    for lo,hi,pct,add in rules:
        if lo<=d<=hi: return _force_tail_900(d*(1+pct/100.0)+add)
    return None

def _remove_all_price_nodes(offer: ET.Element):
    for t in ("price","Price"):
        for node in list(offer.findall(t)): offer.remove(node)

def strip_supplier_price_blocks(offer: ET.Element):
    remove_all(offer,"prices","Prices")
    for tag in INTERNAL_PRICE_TAGS: remove_all(offer, tag)

def reprice_offers(out_shop:ET.Element,rules:List[PriceRule])->None:
    off_el=out_shop.find("offers")
    if off_el is None: return
    for offer in off_el.findall("offer"):
        if offer.attrib.get("_force_price","")=="100":
            strip_supplier_price_blocks(offer); _remove_all_price_nodes(offer)
            ET.SubElement(offer,"price").text=str(PRICE_CAP_VALUE)
            offer.attrib.pop("_force_price",None); continue
        dealer=pick_dealer_price(offer)
        if dealer is None or dealer<=100:
            strip_supplier_price_blocks(offer); continue
        newp=compute_retail(dealer,rules)
        if newp is None:
            strip_supplier_price_blocks(offer); continue
        _remove_all_price_nodes(offer); ET.SubElement(offer,"price").text=str(int(newp)); strip_supplier_price_blocks(offer)

def flag_unrealistic_supplier_prices(out_shop: ET.Element) -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    flagged=0
    for offer in off_el.findall("offer"):
        try:
            src_p = float((get_text(offer,"price") or "").replace(",",".")) if get_text(offer,"price") else None
        except Exception:
            src_p = None
        if src_p is not None and src_p >= PRICE_CAP_THRESHOLD:
            offer.attrib["_force_price"]=str(PRICE_CAP_VALUE); flagged+=1
    return flagged

TRUE_WORDS={"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS={"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}
def _parse_bool_str(s: str)->Optional[bool]:
    v=(s or "").strip().lower()
    return True if v in TRUE_WORDS else False if v in FALSE_WORDS else None

def derive_available(offer: ET.Element) -> bool:
    avail_el=offer.find("available")
    if avail_el is not None and avail_el.text:
        b=_parse_bool_str(avail_el.text)
        if b is not None: return b
    for tag in ["quantity_in_stock","quantity","stock","Stock"]:
        for node in offer.findall(tag):
            try:
                val=int(re.sub(r"[^\d\-]+","", node.text or ""))
                return val>0
            except Exception:
                continue
    for tag in ["status","Status"]:
        node=offer.find(tag)
        if node is not None and node.text:
            b=_parse_bool_str(node.text)
            if b is not None: return b
    return False

def normalize_available_field(out_shop: ET.Element) -> Tuple[int,int]:
    off_el=out_shop.find("offers")
    if off_el is None: return (0,0)
    t=f=0
    for offer in off_el.findall("offer"):
        b=derive_available(offer)
        remove_all(offer,"available")
        offer.attrib["available"]="true" if b else "false"
        if DROP_STOCK_TAGS: remove_all(offer,"quantity_in_stock","quantity","stock","Stock")
        t+=1 if b else 0; f+=0 if b else 1
    return t,f

def fix_currency_id(out_shop: ET.Element, default_code: str = "KZT") -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    touched=0
    for offer in off_el.findall("offer"):
        remove_all(offer,"currencyId"); ET.SubElement(offer,"currencyId").text=default_code; touched+=1
    return touched

def ensure_categoryid_zero_first(out_shop: ET.Element) -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    touched=0
    for offer in off_el.findall("offer"):
        remove_all(offer,"categoryId","CategoryId")
        cid=ET.Element("categoryId"); cid.text=os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0,cid); touched+=1
    return touched

def reorder_offer_children(out_shop: ET.Element) -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    changed=0
    for offer in off_el.findall("offer"):
        children=list(offer)
        buckets={k:[] for k in DESIRED_ORDER}; others=[]
        for n in children: (buckets[n.tag] if n.tag in buckets else others).append(n)
        rebuilt=[*sum((buckets[k] for k in DESIRED_ORDER), []), *others]
        if rebuilt!=children:
            for n in children: offer.remove(n)
            for n in rebuilt:  offer.append(n)
            changed+=1
    return changed

# ===================== Kind detector =====================
def detect_kind(name: str) -> str:
    n=(name or "").lower()
    if "картридж" in n or "тонер" in n or "тонер-" in n: return "cartridge"
    if "ибп" in n or "ups" in n or "источник бесперебойного питания" in n: return "ups"
    if "проектор" in n or "projector" in n or "монитор" in n: return "display"
    if "принтер" in n or "mfp" in n or "мфу" in n: return "mfp"
    return "other"

# ===================== NEW: Specs from <param> only =====================
# Порядок отображения характеристик
PARAM_ORDER = [
    "диагональ","разрешение","тип матрицы","частота обновления","время отклика","яркость","контрастность","угол обзора","hdr",
    "интерфейсы","интерфейс","разъем","разъемы","разъём","разъёмы","usb","hdmi","displayport","dp",
    "мощность","напряжение","энергопотребление",
    "цвет","вес","размеры","габариты","страна","страна производитель","гарантия","комплектация",
    "совместимость","подходит для","модели","принтеры",
]
PARAM_ORDER_IDX = {k:i for i,k in enumerate(PARAM_ORDER)}

def collect_params(offer: ET.Element) -> List[Tuple[str,str]]:
    params=[]
    for p in offer.findall("param"):
        name=_attr_ci(p,"name") or ""
        val=(p.text or "").strip()
        if not name or not val: continue
        if not _param_allowed(name):  # уважаем whitelist
            continue
        params.append((name.strip(), val))
    # сортируем по нашему порядку, затем по алфавиту
    def _key(t):
        n=_norm_param_name(t[0])
        return (PARAM_ORDER_IDX.get(n, 999), n)
    params.sort(key=_key)
    return params

def render_specs_from_params(offer: ET.Element) -> str:
    pairs=collect_params(offer)
    if not pairs: return ""
    out=["<h3>Характеристики</h3>","<ul>"]
    for k,v in pairs:
        if _norm_param_name(k)=="совместимость":
            v=v.replace(";", "; ").replace(",", ", ")
            out.append(f'  <li><strong>{_html_escape_in_cdata_safe(k)}:</strong><br>{_html_escape_in_cdata_safe(v)}</li>')
        else:
            out.append(f'  <li><strong>{_html_escape_in_cdata_safe(k)}:</strong> { _html_escape_in_cdata_safe(v) }</li>')
    out.append("</ul>")
    return "\n".join(out)

# ===================== NEW: SEO rewrite of native description =====================
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
def _html_to_text(desc_html: str) -> str:
    t = re.sub(r"<br\s*/?>", "\n", desc_html or "", flags=re.I)
    t = re.sub(r"</p\s*>", "\n", t, flags=re.I)
    t = re.sub(r"<a\b[^>]*>.*?</a>", "", t, flags=re.I|re.S)
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace("\u00A0"," ")
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def _pick(d: Dict[str,str], *keys: str) -> Optional[str]:
    for k in keys:
        for kk,v in d.items():
            if _norm_param_name(kk)==_norm_param_name(k) and v: return v
    return None

def build_seo_description(offer: ET.Element) -> str:
    """Формирует короткое чистое описание из name/vendor + ключевых <param>."""
    name=get_text(offer,"name").strip()
    vendor=get_text(offer,"vendor").strip()
    kind=detect_kind(name)
    params=dict(collect_params(offer))

    diag=_pick(params,"Диагональ")
    res=_pick(params,"Разрешение")
    panel=_pick(params,"Тип матрицы")
    hz=_pick(params,"Частота обновления")
    rt=_pick(params,"Время отклика")
    bright=_pick(params,"Яркость")
    contrast=_pick(params,"Контрастность")
    ifaces=_pick(params,"Интерфейсы","Интерфейс","Разъем","Разъём","Разъемы","Разъёмы","USB","HDMI","DisplayPort","DP")
    weight=_pick(params,"Вес")
    dims=_pick(params,"Размеры","Габариты")
    color=_pick(params,"Цвет")

    title = f"{name}"
    if vendor and vendor.lower() not in title.lower():
        title = f"{vendor} {title}"

    lines=[]
    # Абзац 1 — что это и ключевая связка параметров
    p1_bits=[]
    if kind in {"display","projector","other"}:
        if diag: p1_bits.append(f'диагональ {diag}')
        if res: p1_bits.append(f'разрешение {res}')
        if panel: p1_bits.append(f'матрица {panel}')
        if hz: p1_bits.append(f'частота {hz}')
    elif kind in {"mfp","cartridge","ups"}:
        if res: p1_bits.append(f'разрешение {res}')
    if p1_bits:
        lines.append(f"{title} — {', '.join(p1_bits)}.")
    else:
        lines.append(f"{title} — надёжное решение для повседневных задач.")

    # Абзац 2 — преимущества по читаемым цифрам
    p2=[]
    if bright: p2.append(f"яркость {bright}")
    if contrast: p2.append(f"контрастность {contrast}")
    if rt: p2.append(f"отклик {rt}")
    if ifaces: p2.append(f"интерфейсы: {ifaces}")
    if color: p2.append(f"цвет: {color}")
    if weight: p2.append(f"вес {weight}")
    if dims: p2.append(f"габариты {dims}")
    if p2:
        lines.append("Ключевые особенности: " + "; ".join(p2) + ".")
    else:
        lines.append("Сбалансированные характеристики и простая установка.")

    # Абзац 3 — мягкий SEO-хвост
    tails={
        "display":"Подходит для работы, развлечений и точной цветопередачи.",
        "mfp":"Подходит для дома и офиса: быстрая печать и простая настройка.",
        "cartridge":"Обеспечивает стабильную печать и чистые отпечатки.",
        "ups":"Защищает технику от перебоев питания и помех.",
        "other":"Практичный выбор для дома и офиса."
    }
    lines.append(tails.get(kind,"Практичный выбор для дома и офиса."))

    html = [
        "<h3>Описание</h3>",
        f"<p>{_html_escape_in_cdata_safe(' '.join(lines))}</p>"
    ]
    return "\n".join(html)

def _replace_html_placeholders_with_cdata(xml_text: str) -> str:
    def repl(m):
        inner=m.group(1).replace("[[[HTML]]]", "").replace("[[[/HTML]]]", "")
        inner=_unescape(inner); inner=_html_escape_in_cdata_safe(inner)
        return f"<description><![CDATA[\n{inner}\n]]></description>"
    return re.sub(
        r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>",
        repl,
        xml_text,
        flags=re.S
    )

# ===================== KEYWORDS (как было) =====================
def tokenize(s: str) -> List[str]: return WORD_RE.findall(s or "")
def dedup(words: List[str]) -> List[str]:
    seen=set(); out=[]
    for w in words:
        k=w.lower()
        if k and k not in seen: seen.add(k); out.append(w)
    return out

def translit_ru_to_lat(s: str) -> str:
    table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    out=s.lower().translate(table); out=re.sub(r"[^a-z0-9\- ]+","", out); return re.sub(r"\s+","-", out).strip("-")

AS_INTERNAL_ART_RE = re.compile(r"^AS\d+|^AK\d+|^AC\d+", re.I)
MODEL_RE = re.compile(r"\b([A-Z][A-Z0-9\-]{2,})\b", re.I)
def extract_models(text_srcs: List[str]) -> List[str]:
    tokens=set()
    for src in text_srcs:
        if not src: continue
        for m in MODEL_RE.findall(src or ""):
            t=m.upper()
            if AS_INTERNAL_ART_RE.match(t) or not (re.search(r"[A-Z]", t) and re.search(r"\d", t)) or len(t)<5: continue
            tokens.add(t)
    return list(tokens)

def is_content_word(t: str) -> bool:
    x=t.lower()
    STOP_RU=set("""
для
и
или
на
в
из
от
по
с
к
до
при
через
над
под
о
об
у
без
про
как
это
тип
модель
комплект
формат
новый
новинка
оригинальный
""".strip().split())
    STOP_EN={"for","and","or","with","of","the","a","an","to","in","on","by","at","from","new","original","type","model","set","kit","pack"}
    GENERIC={"изделие","товар","продукция","аксессуар","устройство","оборудование"}
    return (x not in STOP_RU) and (x not in STOP_EN) and (x not in GENERIC) and (any(ch.isdigit() for ch in x) or "-" in x or len(x)>=3)

def build_keywords_for_offer(offer: ET.Element) -> str:
    name=get_text(offer,"name"); vendor=get_text(offer,"vendor").strip()
    desc_html=inner_html(offer.find("description"))
    base=[vendor] if vendor else []
    raw_tokens=tokenize(name or "")
    modelish=[t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content=[t for t in raw_tokens if is_content_word(t)]
    bigr=[]
    for i in range(len(content)-1):
        a,b=content[i],content[i+1]
        if is_content_word(a) and is_content_word(b): bigr.append(f"{a} {b}")
    base += extract_models([name, desc_html]) + modelish[:8] + bigr[:8] + [t.capitalize() if not re.search(r"[A-Z]{2,}",t) else t for t in content[:10]]
    colors=[]; low=name.lower()
    mapping={"жёлт":"желтый","желт":"желтый","yellow":"yellow","черн":"черный","black":"black","син":"синий","blue":"blue",
             "красн":"красный","red":"red","зелен":"зеленый","green":"green","серебр":"серебряный","silver":"silver","циан":"cyan","магент":"magenta"}
    for k,val in mapping.items():
        if k in low and val not in colors: colors.append(val)
    base += colors
    extra=[]
    for w in base:
        if re.search(r"[А-Яа-яЁё]", str(w)):
            tr=translit_ru_to_lat(str(w))
            if tr and tr not in extra: extra.append(tr)
    base += extra
    if SATU_KEYWORDS_GEO:
        geo=["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
             "Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавл","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]
        if SATU_KEYWORDS_GEO_LAT:
            geo += ["Kazakhstan","Almaty","Astana","Shymkent","Karaganda","Aktobe","Pavlodar","Atyrau","Taraz","Oskemen","Semey","Kostanay","Kyzylorda","Oral","Petropavl","Taldykorgan","Aktau","Temirtau","Ekibastuz","Kokshetau","Rudny"]
        base += geo[:SATU_KEYWORDS_GEO_MAX]
    parts=dedup([p for p in base if p])
    res=[]; total=0
    for p in parts:
        add=((", " if res else "")+p)
        if total+len(add)>SATU_KEYWORDS_MAXLEN: break
        res.append(p); total+=len(add)
    return ", ".join(res)

def ensure_keywords(out_shop: ET.Element) -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    touched=0
    for offer in off_el.findall("offer"):
        kw=build_keywords_for_offer(offer)
        node=offer.find("keywords")
        if not kw:
            if node is not None: offer.remove(node)
            continue
        if node is None:
            node=ET.SubElement(offer,"keywords"); node.text=kw; touched+=1
        else:
            if (node.text or "")!=kw: node.text=kw; touched+=1
    return touched

# ===================== VENDORCODE/ID & FEED_META =====================
ARTICUL_RE=re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)
def _extract_article_from_name(name:str)->str:
    if not name: return ""
    m=ARTICUL_RE.search(name); return (m.group(1) if m else "").upper()
def _extract_article_from_url(url:str)->str:
    if not url: return ""
    try:
        path=urllib.parse.urlparse(url).path.rstrip("/")
        last=re.sub(r"\.(html?|php|aspx?)$","",path.split("/")[-1],flags=re.I)
        m=ARTICUL_RE.search(last); return (m.group(1) if m else last).upper()
    except Exception: return ""
def _normalize_code(s:str)->str:
    s=(s or "").strip()
    if not s: return ""
    s=re.sub(r"[\s_]+","",s).replace("—","-").replace("–","-")
    return re.sub(r"[^A-Za-z0-9\-]+","",s).upper()

def ensure_vendorcode_with_article(out_shop:ET.Element,prefix:str="AC",create_if_missing:bool=True)->None:
    off_el=out_shop.find("offers")
    if off_el is None: return
    for offer in off_el.findall("offer"):
        vc=offer.find("vendorCode")
        if vc is None:
            if create_if_missing:
                vc=ET.SubElement(offer,"vendorCode"); vc.text=""
            else:
                continue
        old=(vc.text or "").strip()
        if (old=="") or (old.upper()==prefix.upper()):
            art=_normalize_code(offer.attrib.get("article") or "") \
              or _normalize_code(_extract_article_from_name(get_text(offer,"name"))) \
              or _normalize_code(_extract_article_from_url(get_text(offer,"url"))) \
              or _normalize_code(offer.attrib.get("id") or "")
            if art: vc.text=art
        vc.text=f"{prefix}{(vc.text or '')}"

def sync_offer_id_with_vendorcode(out_shop: ET.Element) -> None:
    off_el=out_shop.find("offers")
    if off_el is None: return
    for offer in off_el.findall("offer"):
        vc=offer.find("vendorCode")
        if vc is None or not (vc.text or "").strip(): continue
        new_id=(vc.text or "").strip()
        if offer.attrib.get("id")!=new_id: offer.attrib["id"]=new_id

def render_feed_meta_comment(pairs:Dict[str,str]) -> str:
    rows=[
        ("Поставщик", pairs.get("supplier","")),
        ("URL поставщика", pairs.get("source","")),
        ("Время сборки (Алматы)", pairs.get("built_alm","")),
        ("Ближайшее время сборки (Алматы)", pairs.get("next_build_alm","")),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total","0"))),
        ("Сколько товаров у поставщика после фильтра", str(pairs.get("offers_written","0"))),
        ("Сколько товаров есть в наличии (true)", str(pairs.get("available_true","0"))),
        ("Сколько товаров нет в наличии (false)", str(pairs.get("available_false","0"))),
        ("Версия скрипта", SCRIPT_VERSION),
    ]
    key_w=max(len(k) for k,_ in rows)
    lines = ["FEED_META"] + [f"{k.ljust(key_w)} | {v}" for k,v in rows]
    return "\n".join(lines)

# ===================== PLACEHOLDERS =====================
_url_head_cache: Dict[str,bool]={}
def url_exists(url: str) -> bool:
    if not url: return False
    if url in _url_head_cache: return _url_head_cache[url]
    try:
        r=requests.head(url, timeout=PLACEHOLDER_HEAD_TIMEOUT, allow_redirects=True)
        ok=(200<=r.status_code<400)
    except Exception:
        ok=False
    _url_head_cache[url]=ok; return ok

def _slug(s: str) -> str:
    if not s: return "unknown"
    table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    base=(s or "").lower().translate(table); base=re.sub(r"[^a-z0-9\- ]+","", base)
    return re.sub(r"\s+","-", base).strip("-") or "unknown"

def _placeholder_url_brand(vendor: str) -> str: return f"{PLACEHOLDER_BRAND_BASE}/{_slug(vendor)}.{PLACEHOLDER_EXT}"
def _placeholder_url_category(kind: str) -> str: return f"{PLACEHOLDER_CATEGORY_BASE}/{kind}.{PLACEHOLDER_EXT}"

def ensure_placeholder_pictures(out_shop: ET.Element) -> int:
    if not PLACEHOLDER_ENABLE: return 0
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    added=0
    for offer in off_el.findall("offer"):
        pics=list(offer.findall("picture"))
        has_pic=any((p.text or "").strip() for p in pics)
        if has_pic: continue
        vendor=get_text(offer,"vendor").strip(); name=get_text(offer,"name").strip()
        kind=detect_kind(name)
        picked=""
        if vendor:
            u=_placeholder_url_brand(vendor)
            if url_exists(u): picked=u
        if not picked:
            u=_placeholder_url_category(kind)
            if url_exists(u): picked=u
        if not picked: picked=PLACEHOLDER_DEFAULT_URL
        ET.SubElement(offer,"picture").text=picked; added+=1
    return added

# ===================== INJECT: description (rewritten) + specs from <param> =====================
def inject_descriptions_and_specs_from_params(out_shop: ET.Element) -> Tuple[int,int]:
    """Возвращает: offers_changed, offers_with_specs"""
    off_el=out_shop.find("offers")
    if off_el is None: return (0,0)
    changed=0; with_specs=0
    for offer in off_el.findall("offer"):
        # 1) новое описание
        desc_html = build_seo_description(offer)
        # 2) характеристики — только из <param>
        specs_html = render_specs_from_params(offer)

        full_html = "\n".join([s for s in [desc_html, specs_html] if s]).strip()
        placeholder=f"[[[HTML]]]{full_html}[[[/HTML]]]"

        d=offer.find("description")
        if d is None:
            d=ET.SubElement(offer,"description"); d.text=placeholder; changed+=1
        else:
            if (d.text or "").strip()!=placeholder:
                d.text=placeholder; changed+=1
        if specs_html: with_specs+=1
    return changed, with_specs

# ===================== MAIN =====================
def main()->None:
    log("Run set -e                       # прерывать шаг при любой ошибке")
    log(f"Python {sys.version.split()[0]}")
    log(f"Source: {SUPPLIER_URL}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")
    offers_in=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in is None: err("XML: <offers> not found")
    src_offers=list(offers_in.findall("offer"))

    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")

    # 1) Копируем офферы (убираем categoryId на входе)
    for o in src_offers:
        mod=deepcopy(o)
        if DROP_CATEGORY_ID_TAG:
            for node in list(mod.findall("categoryId"))+list(mod.findall("CategoryId")): mod.remove(node)
        out_offers.append(mod)

    # 2) Фильтр по названиям
    filtered_out=0
    keys_file = load_name_filter(AKCENT_KEYWORDS_PATH)
    keys: List[KeySpec] = keys_file[:]
    if not keys and AKCENT_KEYWORDS_LIST:
        keys = [KeySpec(k, "substr", _norm_name(k), None) for k in AKCENT_KEYWORDS_LIST]

    if AKCENT_KEYWORDS_MODE in {"include","exclude"}:
        if not keys:
            if AKCENT_KEYWORDS_MODE=="include":
                err(f"AKCENT_KEYWORDS_MODE=include, но ключи не заданы (нет {AKCENT_KEYWORDS_PATH} и пустой AKCENT_KEYWORDS).")
            else:
                log("Filter disabled (exclude), ключей нет.")
        else:
            before=len(list(out_offers.findall("offer")))
            hits=0
            for off in list(out_offers.findall("offer")):
                nm=get_text(off,"name")
                hit=name_matches(nm, keys)
                drop=(AKCENT_KEYWORDS_MODE=="exclude" and hit) or (AKCENT_KEYWORDS_MODE=="include" and not hit)
                if hit: hits+=1
                if drop:
                    out_offers.remove(off); filtered_out+=1
            kept=before-filtered_out
            src=("file" if keys_file else "env")
            log(f"Filter mode: {AKCENT_KEYWORDS_MODE} | Source: {src} | Keys: {len(keys)} | Offers before: {before} | Matched: {hits} | Removed: {filtered_out} | Kept: {kept}")
    else:
        log("Filter disabled (AKCENT_KEYWORDS_MODE=off)")

    # 3) Фильтр <param> исходных параметров
    p_touched, p_kept, p_dropped = filter_params_for_satu(out_shop)
    log(f"Param filter (source params): offers touched={p_touched}, kept={p_kept}, dropped={p_dropped}")

    # 4) Кэп «нереальных» цен
    flagged = flag_unrealistic_supplier_prices(out_shop)
    log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")

    # 5) Вендоры
    v_norm, v_filled, v_removed = ensure_vendor(out_shop)
    log(f"Vendors normalized={v_norm}, filled={v_filled}, removed_bad={v_removed}")

    # 6) vendorCode + id
    ensure_vendorcode_with_article(out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AC"), create_if_missing=True)
    sync_offer_id_with_vendorcode(out_shop)

    # 7) Репрайсинг
    reprice_offers(out_shop, PRICING_RULES)

    # 8) Плейсхолдеры фото
    ph_added=ensure_placeholder_pictures(out_shop)
    log(f"Placeholders added: {ph_added}")

    # 9) Новая сборка описаний и характеристик из <param>
    desc_changed, specs_from_params = inject_descriptions_and_specs_from_params(out_shop)
    log(f"Descriptions updated: {desc_changed}, specs blocks (from <param>): {specs_from_params}")

    # 10) Наличие, валюта
    t_true, t_false = normalize_available_field(out_shop)
    fix_currency_id(out_shop, default_code="KZT")

    # 11) Чистка служебных тегов/атрибутов
    for off in out_offers.findall("offer"):
        for t in PURGE_TAGS_AFTER:
            for node in list(off.findall(t)): off.remove(node)
        for a in PURGE_OFFER_ATTRS_AFTER:
            if a in off.attrib: off.attrib.pop(a,None)

    # 12) Порядок + <categoryId> первым
    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)

    # 13) <keywords>
    kw_touched=ensure_keywords(out_shop)
    log(f"Keywords updated: {kw_touched}")

    # 14) FEED_META
    built_alm=now_almaty()
    meta_pairs={
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "offers_total": len(src_offers),
        "offers_written": len(list(out_offers.findall("offer"))),
        "available_true": str(t_true),
        "available_false": str(t_false),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # 15) Запись
    try: ET.indent(out_root, space="  ")
    except Exception: pass
    xml_bytes=ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text=xml_bytes.decode(ENC, errors="replace")
    xml_text=re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_text)
    xml_text=re.sub(r"(?s)(-->)\s*(<shop\b)", r"\1\n\2", xml_text)
    xml_text=_replace_html_placeholders_with_cdata(xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written."); return
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    try:
        with open(OUT_FILE_YML,"w",encoding=ENC, newline="\n") as f: f.write(xml_text)
    except UnicodeEncodeError as e:
        warn(f"{ENC} encode issue ({e}); using xmlcharrefreplace fallback")
        with open(OUT_FILE_YML,"wb") as f: f.write(xml_text.encode(ENC, errors="xmlcharrefreplace"))

    try:
        docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e: warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | script={SCRIPT_VERSION}")

# ===================== ENTRY =====================
if __name__ == "__main__":
    try: main()
    except Exception as e: err(str(e))
