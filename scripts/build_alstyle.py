# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle → YML: стабильные цены/наличие + безопасный HTML для <description>.

Обновление v7.3.2:
- FIX: NameError 'raw_desc_text_for_kv' — теперь передаётся параметром в build_lead_bullets(...).
- Оформление «родного описания» без изменений.
"""

from __future__ import annotations
import os, sys, re, time, random, json, hashlib, urllib.parse, requests
from copy import deepcopy
from typing import Dict, List, Tuple, Optional, Set
from xml.etree import ElementTree as ET
from datetime import datetime, timezone, timedelta
from html import unescape as _unescape
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

SCRIPT_VERSION = "alstyle-2025-10-21.v7.3.2"

# ======================= ENV / CONST =======================
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "AlStyle").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/alstyle.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()
TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

ALSTYLE_CATEGORIES_PATH = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt")
ALSTYLE_CATEGORIES_MODE = os.getenv("ALSTYLE_CATEGORIES_MODE", "off").lower()  # off|include|exclude

# PRICE CAP
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "100"))

# KEYWORDS
SATU_KEYWORDS          = os.getenv("SATU_KEYWORDS", "auto").lower()
SATU_KEYWORDS_MAXLEN   = int(os.getenv("SATU_KEYWORDS_MAXLEN", "1024"))
SATU_KEYWORDS_GEO      = os.getenv("SATU_KEYWORDS_GEO", "1").lower() in {"1","true","yes","on"}
SATU_KEYWORDS_GEO_LAT  = os.getenv("SATU_KEYWORDS_GEO_LAT", "0").lower() in {"1","true","yes","on"}
SATU_KEYWORDS_GEO_MAX  = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))

# SEO / CACHE
SEO_STICKY        = os.getenv("SEO_STICKY", "1").lower() in {"1","true","yes","on"}
SEO_CACHE_PATH    = os.getenv("SEO_CACHE_PATH", "docs/seo_cache_v2.json").strip()
SEO_REFRESH_MODE  = os.getenv("SEO_REFRESH_MODE", "monthly").lower()   # monthly|days|off
SEO_REFRESH_DAYS  = int(os.getenv("SEO_REFRESH_DAYS", "14"))  # используется когда MODE=days
LEGACY_CACHE_PATH = "docs/seo_cache.json"

# Placeholders (фото)
PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.al-style.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.al-style.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.al-style.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# Purge internals
DROP_CATEGORY_ID_TAG   = True
DROP_STOCK_TAGS        = True
PURGE_TAGS_AFTER       = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER= ("type","article")
INTERNAL_PRICE_TAGS    = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                          "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice",
                          "max_price","maxPrice","oldprice")

# ======================= UTILS =======================
log  = lambda m: print(m, flush=True)
warn = lambda m: print(f"WARN: {m}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_utc() -> datetime: return datetime.now(timezone.utc)
def now_utc_str() -> str: return now_utc().strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty() -> datetime:
    try:   return datetime.now(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(time.time()+5*3600)
    except Exception: return datetime.utcfromtimestamp(time.time()+5*3600)

def format_dt_almaty(dt: datetime) -> str:
    try:
        alm = dt.astimezone(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(dt.timestamp()+5*3600)
    except Exception:
        alm = dt
    return alm.strftime("%Y-%m-%d %H:%M:%S")

CHOICES_SEED_SALT = "alstyle/choices-salt"
def choose(seq: List[str], seed: int, idx: int) -> str:
    if not seq: return ""
    rnd = random.Random(int(hashlib.md5(f"{seed}:{idx}:{CHOICES_SEED_SALT}".encode("utf-8")).hexdigest(),16))
    return seq[rnd.randrange(0, len(seq))]

COLON_CLASS = r"[:\uFF1A\uFE55\u2236\uFE30]"
canon_colons    = lambda s: re.sub(COLON_CLASS, ":", s or "")
NOISE_RE        = re.compile(r"[\u200B-\u200F\u202A-\u202E\u00A0\u202F\u2060\uFEFF\uFFF9-\uFFFB\u0000-\u001F\u007F-\u009F]")
def strip_noise_chars(s: str) -> str:
    if not s: return ""
    return NOISE_RE.sub("", s).replace("�","")

def _html_escape_in_cdata_safe(s: str) -> str:
    return (s or "").replace("]]>", "]]&gt;")

def inner_html(el: ET.Element) -> str:
    if el is None: return ""
    parts=[]
    if el.text: parts.append(el.text)
    for child in el:
        parts.append(ET.tostring(child, encoding="unicode"))
        if child.tail: parts.append(child.tail)
    return "".join(parts).strip()

# ======================= LOAD SOURCE =======================
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
            if len(data)<MIN_BYTES: raise RuntimeError(f"too small: {len(data)} bytes")
            return data
        except Exception as e:
            last=e
            if i<RETRIES: time.sleep(RETRY_BACKOFF*(2**(i-1)))
    raise RuntimeError(f"fetch failed: {last}")

# ======================= XML HELPERS =======================
def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag)
    return (node.text or "").strip() if node is not None and node.text else ""

def remove_all(el: ET.Element, *tags: str) -> int:
    n=0
    for t in tags:
        for x in list(el.findall(t)):
            el.remove(x); n+=1
    return n

def _remove_all_price_nodes(offer: ET.Element):
    for t in ("price", "Price"):
        for node in list(offer.findall(t)): offer.remove(node)

def strip_supplier_price_blocks(offer: ET.Element):
    remove_all(offer, "prices", "Prices")
    for tag in INTERNAL_PRICE_TAGS: remove_all(offer, tag)

# ======================= CATEGORY TREE =======================
class CatRule:
    __slots__=("raw","kind","pattern")
    def __init__(self, raw: str, kind: str, pattern): self.raw, self.kind, self.pattern = raw, kind, pattern

def _norm_text(s: str) -> str:
    s=(s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

def parse_category_rules(path: str) -> List[CatRule]:
    rules=[]
    if not os.path.exists(path): return rules
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            raw=line.strip()
            if not raw or raw.startswith("#"): continue
            kind,_,pat = raw.partition(":")
            kind=kind.strip().lower()
            if kind not in {"include","exclude"}: continue
            pat=pat.strip()
            if not pat: continue
            rx=re.compile(pat, re.I)
            rules.append(CatRule(raw, kind, rx))
    return rules

def collect_categories(shop_el: ET.Element) -> Tuple[List[ET.Element], Dict[str,str], Dict[str,str]]:
    cats=shop_el.find("categories") or shop_el.find("Categories")
    if cats is None: return [], {}, {}
    all_cats=list(cats.findall("category"))
    id2name={ (c.attrib.get("id") or "").strip(): (c.text or "").strip() for c in all_cats }
    id2parent={ (c.attrib.get("id") or "").strip(): (c.attrib.get("parentId") or "").strip() for c in all_cats }
    return all_cats, id2name, id2parent

def apply_category_rules(shop_el: ET.Element, mode: str, path: str) -> int:
    if mode not in {"include","exclude"}: return 0
    rules=parse_category_rules(path)
    if not rules: return 0
    _, id2name, id2parent = collect_categories(shop_el)
    def match_any(cat_id: str) -> Optional[str]:
        name=id2name.get(cat_id,"")
        full=build_category_path_from_id(cat_id, id2name, id2parent) or name
        for r in rules:
            if r.pattern.search(full): return r.kind
        return None
    offers=shop_el.find("offers")
    if offers is None: return 0
    removed=0
    for off in list(offers.findall("offer")):
        cid=(off.find("categoryId").text or "").strip() if off.find("categoryId") is not None else ""
        decision=match_any(cid)
        if decision=="include": continue
        if decision=="exclude": offers.remove(off); removed+=1
    return removed

def collect_descendants(ids: Set[str], parent2children: Dict[str,Set[str]]) -> Set[str]:
    out=set(ids); stack=list(ids)
    while stack:
        cur=stack.pop()
        for ch in parent2children.get(cur, ()):
            if ch not in out: out.add(ch); stack.append(ch)
    return out

def build_category_path_from_id(cat_id: str, id2name: Dict[str,str], id2parent: Dict[str,str]) -> str:
    names=[]; cur=cat_id; seen=set()
    while cur and cur not in seen and cur in id2name:
        seen.add(cur); names.append(id2name.get(cur,"")); cur=id2parent.get(cur,"")
    names=[n for n in names if n]
    return " / ".join(reversed(names)) if names else ""

# ======================= BRANDS =======================
def _norm_key(s: str) -> str:
    if not s: return ""
    s=s.strip().lower().replace("ё","е"); s=re.sub(r"[-_/]+"," ",s)
    return re.sub(r"\s+"," ",s)

SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

COMMON_BRANDS = [
    "Canon","HP","Hewlett-Packard","Xerox","Brother","Epson","Samsung","Kyocera","Ricoh","Konica Minolta",
    "Lexmark","Sharp","OKI","Pantum",
    "Europrint","Katun","NV Print","Hi-Black","ProfiLine","Cactus","G&G","Static Control","Lomond","WWM","Uniton",
    "TSC","Zebra",
    "MSI","ASUS","Acer","Lenovo","Apple","Gigabyte","Dell","Huawei","Honor","Realme","Xiaomi","Infinix","Tecno","Vivo","OnePlus",
    "Kingston","Goodram","Crucial","Transcend","Samsung","Seagate","WD","WD_BLACK","Sandisk",
    "Philips","Sony","LG","Panasonic","TCL",
]

def build_brand_index(shop_el: ET.Element) -> Dict[str,str]:
    idx={}
    for off in shop_el.find("offers").findall("offer"):
        v=get_text(off,"vendor").strip()
        if not v: continue
        idx[_norm_key(v)]=v
    return idx

def _find_brand_in_text(s: str) -> str:
    if not s: return ""
    for br in COMMON_BRANDS:
        if re.search(rf"\b{re.escape(_norm_text(br))}\b", _norm_text(s)): return br
    return ""

def guess_vendor_for_offer(offer: ET.Element, brand_index: Dict[str,str]) -> str:
    name = get_text(offer, "name")
    d    = offer.find("description")
    desc = inner_html(offer.find("description"))
    first = re.split(r"\s+", name.strip())[0] if name else ""
    f_norm=_norm_key(first)
    if f_norm in brand_index: return brand_index[f_norm]
    b = _find_brand_in_text(name) or _find_brand_in_text(desc)
    if b: return b
    nrm=_norm_key(name)
    for br in COMMON_BRANDS:
        if re.search(rf"\b{re.escape(_norm_text(br))}\b", nrm): return br
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

# ======================= PRICING =======================
PriceRule = Tuple[int,int,float,int]

def _force_tail_900(x: float) -> int:
    p = int(round(x))
    return int(str(p)[:-3] + "900") if p >= 1000 and len(str(p)) >= 4 else p

PRICE_RULES: List[PriceRule] = [
    (101,        10000,     4.0,  3000),
    (10001,      25000,     4.0,  4000),
    (25001,      50000,     4.0,  5000),
    (50001,      75000,     4.0,  7000),
    (75001,      100000,    4.0,  10000),
    (100001,     150000,    4.0,  12000),
    (150001,     200000,    4.0,  15000),
    (200001,     300000,    4.0,  20000),
    (300001,     400000,    4.0,  25000),
    (400001,     500000,    4.0,  30000),
    (500001,     750000,    4.0,  40000),
    (750001,     1000000,   4.0,  50000),
    (1000001,    1500000,   4.0,  70000),
    (1500001,    2000000,   4.0,  90000),
    (2000001,    99000000,  4.0,  100000),
]

def pick_dealer_price(offer: ET.Element) -> Tuple[Optional[float], str]:
    prices = offer.find("prices") or offer.find("Prices")
    if prices is not None:
        for p in prices.findall("price"):
            tp=(p.attrib.get("type") or p.attrib.get("name") or "").lower()
            if any(k in tp for k in ("dealer","опт","opt","b2b")):
                try: return float(p.text), "prices_dealer"
                except Exception: pass
    for tag in ("purchase_price","wholesale_price","opt_price","b2b_price","purchasePrice","wholesalePrice","optPrice","b2bPrice","price"):
        node=offer.find(tag)
        if node is not None:
            try: val=float(node.text); return val, "direct_field"
            except Exception: pass
    node=offer.find("oldprice")
    if node is not None:
        try: return float(node.text), "rrp_fallback"
        except Exception: pass
    return None, "missing"

def compute_retail(dealer: float, rules: List[PriceRule]) -> Optional[int]:
    dealer=int(round(dealer))
    if dealer < 101: return None
    for lo,hi,pct,add in rules:
        if lo<=dealer<=hi: return _force_tail_900(dealer*(1.0+pct/100.0)+add)
    return None

def reprice_offers(shop_el:ET.Element,rules:List[PriceRule])->Tuple[int,int,int,Dict[str,int]]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,{"missing":0})
    updated=skipped=total=0; src_stats={"prices_dealer":0,"direct_field":0,"rrp_fallback":0,"missing":0}
    for offer in offers_el.findall("offer"):
        total+=1
        if offer.attrib.get("_force_price","") == "100":
            skipped+=1; strip_supplier_price_blocks(offer); continue
        dealer, src = pick_dealer_price(offer); src_stats[src]=src_stats.get(src,0)+1
        if dealer is None or dealer<=100:
            skipped+=1; strip_supplier_price_blocks(offer); continue
        newp=compute_retail(dealer,rules)
        if newp is None:
            skipped+=1; strip_supplier_price_blocks(offer); continue
        _remove_all_price_nodes(offer)
        ET.SubElement(offer, "price").text=str(int(newp))
        strip_supplier_price_blocks(offer); updated+=1
    return updated,skipped,total,src_stats

# ======================= PARAMS / TEXT =======================
_key = lambda s: re.sub(r"\s+"," ",(s or "").strip()).lower()
UNWANTED_PARAM_NAME_RE = re.compile(
    r"^(?:\s*(?:благотворительн\w*|снижена\s*цена|новинк\w*|"
    r"артикул(?:\s*/\s*штрихкод)?|оригинальн\w*\s*код|штрихкод|"
    r"код\s*тн\s*вэд(?:\s*eaeu)?|код\s*тнвэд(?:\s*eaeu)?|тн\s*вэд|тнвэд|"
    r"tn\s*ved|hs\s*code)\s*)$",
    re.I
)

GOOD_PARAM_NAME_RE = re.compile(
    r"^(?:\s*(?:вес|цвет|габарит\w*|интерфейс\w*|частота\s*обновления|тип\s*матрицы|диагональ|разрешение|"
    r"тип\s*ноутбука|тип\s*оперативной\s*памяти|объем\s*накопителя|тип\s*накопителя|видеокарта|"
    r"операционная\s*система|процессор|объ[ее]м|емкость|емк\w*|"
    r"порты|подключения|питание|тип\s*дисплея)\s*)$",
    re.I
)

def normalize_param_name(name: str) -> str:
    return re.sub(r"\s{2,}"," ", strip_noise_chars(canon_colons(name or ""))).strip().strip(":-—").strip()

def normalize_param_val(name: str, value: str) -> str:
    name_n=_norm_text(name)
    v=strip_noise_chars(canon_colons(value or "")).strip()
    v=re.sub(r"\s{2,}"," ", v)
    if name_n=="вес" and not re.search(r"\bкг\b", v, re.I): v = f"{v} кг"
    return v

def clean_params(offer: ET.Element) -> int:
    p=list(offer.findall("param"))
    seen=set(); removed=0
    for el in list(p):
        name=(el.attrib.get("name") or "").strip()
        if not name:
            offer.remove(el); removed+=1; continue
        n_norm=_norm_text(name)
        if UNWANTED_PARAM_NAME_RE.search(name) and not GOOD_PARAM_NAME_RE.search(name):
            offer.remove(el); removed+=1; continue
        new_name=normalize_param_name(name)
        val=(el.text or "").strip()
        new_val=normalize_param_val(new_name, val)
        el.attrib["name"]=new_name; el.text=new_val
        k=(new_name.lower(), new_val.lower())
        if k in seen:
            offer.remove(el); removed+=1
        else:
            seen.add(k)
    return removed

HDR_RE = re.compile(r"^\s*(технические\s+характеристики|характеристики)\s*:?\s*$", re.I)
HEAD_ONLY_RE = re.compile(r"^\s*(?:основные\s+)?характеристики\s*[:：﹕∶︰-]*\s*$", re.I)
HEAD_PREFIX_RE = re.compile(r"^\s*(?:основные\s+)?характеристики\s*[:：﹕∶︰-]*\s*", re.I)
KV_COLON_RE  = re.compile(r"^\s*([^:]{2,}?)\s*:\s*(.+)$")
URL_RE       = re.compile(r"https?://\S+", re.I)

def canon_units(name: str, value: str) -> str:
    v = value.strip()
    v = re.sub(r"\b[Bb][Tt]\b", "Вт", v)
    v = re.sub(r"(?<=\d)\s*[Vv]\b", "В", v)
    v = re.sub(r"\b[Hh][Zz]\b", "Гц", v)
    v = re.sub(r"\b[mM][sS]\b", "мс", v)
    v = v.replace(",", ".")
    v = re.sub(r"\s{2,}", " ", v).strip()
    if _norm_text(name) == "вес" and not re.search(r"\bкг\b", v, re.I): v = v + " кг"
    return v

def normalize_free_text_punct(s: str) -> str:
    t=canon_colons(s or ""); t=re.sub(r":\s*:", ": ", t)
    t=re.sub(r"(?<!\.)\.\.(?!\.)", ".", t)
    return re.sub(r"\s{2,}", " ", t).strip()

def extract_kv_from_description(text: str) -> List[Tuple[str,str]]:
    if not (text or "").strip(): return []
    t=(text or "").replace("\r\n","\n").replace("\r","\n")
    lines=[ln.strip() for ln in t.split("\n") if ln.strip()]
    out=[]; seen=set()
    for ln in lines:
        if URL_RE.search(ln): continue
        m=KV_COLON_RE.match(ln)
        if not m: continue
        n=m.group(1).strip("—-: "); v=m.group(2).strip()
        n=re.sub(r"\s{2,}"," ", n)
        n=re.sub(r"^\s*(?:основные\s+)?характеристики\s*$","Характеристики",n, flags=re.I)
        n=normalize_param_name(n); v=canon_units(n, v)
        k=_key(n)
        if k not in seen:
            seen.add(k); out.append((n,v))
    return out

def build_specs_pairs_from_params(offer: ET.Element) -> List[Tuple[str,str]]:
    pairs=[]
    for p in offer.findall("param"):
        name=(p.attrib.get("name") or "").strip()
        val =(p.text or "").strip()
        if not name or not val: continue
        if GOOD_PARAM_NAME_RE.search(name) and not UNWANTED_PARAM_NAME_RE.search(name):
            pairs.append((normalize_param_name(name), normalize_param_val(name, val)))
    out=[]; seen=set()
    for n,v in pairs:
        k=_key(n)
        if k not in seen:
            seen.add(k); out.append((n,v))
    return out

def _rank_key(name: str) -> int:
    n=_norm_text(name)
    pref=("вес","гарантия","объём","количество","частота обновления экрана","тип матрицы экрана","диагональ экрана",
          "разрешение экрана","тип ноутбука","тип оперативной памяти","объем накопителя","тип накопителя","видеокарта",
          "операционная система","процессор")
    for i,k in enumerate(pref):
        if n==_norm_text(k): return i
    return 999

def has_specs_in_raw_desc(html_text: str) -> bool:
    plain = re.sub(r"<br\s*/?>","\n", html_text or "", flags=re.I)
    plain = re.sub(r"<[^>]+>","",plain)
    return bool(HDR_RE.search(plain) or HEAD_ONLY_RE.search(plain) or HEAD_PREFIX_RE.search(plain))

def build_specs_html_from_params(offer: ET.Element) -> str:
    pairs = build_specs_pairs_from_params(offer)
    if not pairs: return ""
    pairs_sorted = sorted(pairs, key=lambda kv: _rank_key(kv[0]))
    parts = ["<h3>Характеристики</h3>", "<ul>"]
    for name, val in pairs_sorted:
        parts.append(f"  <li><strong>{_html_escape_in_cdata_safe(name)}:</strong> {_html_escape_in_cdata_safe(val)}</li>")
    parts.append("</ul>")
    return "\n".join(parts)

# ===== Родное описание: аккуратное оформление =====
SECTION_TITLES = [
    "Ключевые характеристики",
    "Порты и подключения",
    "Автономность и питание",
    "Безопасность и мультимедиа",
    "Размеры и вес",
    "Комплектация",
    "Почему стоит выбрать",
]
KEY_SUBLABELS = {"процессор","графика","экран","озу","накопитель","охлаждение","клавиатура","корпус"}
PORT_TOKENS = ("thunderbolt", "usb", "hdmi", "displayport", "sd", "type-c", "rj-45", "lan", "аудиоразъём", "аудиоразъем")

def _to_text_lines(html_in: str) -> list[str]:
    if not (html_in or "").strip():
        return []
    t = (html_in or "")
    t = re.sub(r"(?i)</?(?:p|div|br|li|ul|ol|h[1-6])[^>]*>", "\n", t)
    t = re.sub(r"<[^>]+>", "", t)
    t = _unescape(t).replace("]]>", "]]&gt;")
    t = t.replace("\r\n","\n").replace("\r","\n")
    lines = [normalize_free_text_punct(ln.strip()) for ln in t.split("\n")]
    out=[] 
    for ln in lines:
        if not ln: 
            if out and out[-1] != "":
                out.append("")
            continue
        out.append(ln)
    while out and out[0]=="": out.pop(0)
    while out and out[-1]=="": out.pop()
    return out

def _is_section_title(line: str) -> str|None:
    s=_norm_text(line).strip(":-— ")
    for title in SECTION_TITLES:
        if _norm_text(title) == s:
            return title
    if s.startswith(_norm_text("почему стоит выбрать")):
        return "Почему стоит выбрать"
    if s in {"порты","разъемы","разъёмы","порты и подключения","разъемы и подключение","разъёмы и подключение"}:
        return "Порты и подключения"
    return None

def _line_looks_like_port(line: str) -> bool:
    s=_norm_text(line)
    if re.search(r"^\d+\s*[x×]\s*", s): 
        return True
    return any(tok in s for tok in PORT_TOKENS)

def _split_network_features(s: str) -> list[str]:
    v = re.sub(r"^\s*сеть\s*:\s*","", s, flags=re.I)
    parts = re.split(r"\s*,\s*|\s+и\s+", v)
    return [p.strip(" .;") for p in parts if p.strip(" .;")]

def format_native_description(raw_html: str) -> str:
    lines = _to_text_lines(raw_html)
    if not lines:
        return ""

    intro_paragraphs: list[str] = []
    key_items: list[str] = []
    ports_items: list[str] = []
    power_items: list[str] = []
    secure_items: list[str] = []
    size_items: list[str] = []
    bundle_items: list[str] = []
    why_items: list[str] = []

    current_section: str|None = None

    def add_intro(text: str):
        if text and text not in intro_paragraphs:
            intro_paragraphs.append(text)

    for raw in lines:
        if not raw:
            continue

        sec = _is_section_title(raw)
        if sec:
            current_section = sec
            continue

        m = re.match(r"^\s*([^:：﹕∶︰]{2,}?)\s*[:：﹕∶︰]\s*(.+)$", raw)
        if m:
            label = _norm_text(m.group(1))
            value = m.group(2).strip()

            if label in {"сеть","сетевые возможности","беспроводные интерфейсы"}:
                for p in _split_network_features(value):
                    if p: ports_items.append(p)
                current_section = current_section or "Порты и подключения"
                continue

            if label in {"размер","размеры","габариты"}:
                size_items.append(f"Габариты: {value}"); current_section = current_section or "Размеры и вес"; continue
            if label in {"вес"}:
                size_items.append(f"Вес: {canon_units('вес', value)}"); current_section = current_section or "Размеры и вес"; continue
            if label in {"цвет","цвета"}:
                size_items.append(f"Цвет: {value}"); current_section = current_section or "Размеры и вес"; continue

            if label in {"батарея","аккумулятор"}:
                power_items.append(f"Батарея: {value}"); current_section = current_section or "Автономность и питание"; continue
            if label in {"зарядка","зарядное устройство","адаптер питания"}:
                power_items.append(f"Зарядное устройство: {value}"); current_section = current_section or "Автономность и питание"; continue

            if label in {"комплектация","что в коробке","в комплекте"}:
                for part in re.split(r"[;,]\s*", value):
                    if part: bundle_items.append(part.strip())
                current_section = current_section or "Комплектация"; continue

            if label in KEY_SUBLABELS or current_section == "Ключевые характеристики":
                key_items.append(f"{m.group(1).strip()}: {value}")
                current_section = "Ключевые характеристики"
                continue

            if _line_looks_like_port(m.group(0)):
                ports_items.append(m.group(0))
                current_section = "Порты и подключения"
                continue

            if any(w in label for w in ["камера","tpm","kensington","безопас","динамик","аудио","микрофон"]):
                secure_items.append(f"{m.group(1).strip()}: {value}")
                current_section = current_section or "Безопасность и мультимедиа"
                continue

            if current_section == "Ключевые характеристики":
                key_items.append(f"{m.group(1).strip()}: {value}"); continue
            if current_section == "Порты и подключения":
                ports_items.append(f"{m.group(1).strip()}: {value}"); continue
            if current_section == "Автономность и питание":
                power_items.append(f"{m.group(1).strip()}: {value}"); continue
            if current_section == "Безопасность и мультимедиа":
                secure_items.append(f"{m.group(1).strip()}: {value}"); continue
            if current_section == "Размеры и вес":
                size_items.append(f"{m.group(1).strip()}: {value}"); continue
            if current_section == "Комплектация":
                for part in re.split(r"[;,]\s*", value):
                    if part: bundle_items.append(part.strip()); continue

            add_intro(raw)
            continue

        if _line_looks_like_port(raw):
            ports_items.append(raw)
            current_section = current_section or "Порты и подключения"
            continue

        if raw.startswith("✅") or raw.startswith("✔"):
            why_items.append(raw.lstrip("✅✔").strip())
            current_section = "Почему стоит выбрать"
            continue

        low = _norm_text(raw)
        if low.startswith("ир-веб-камера") or "nahimic" in low or "tpm" in low or "kensington" in low:
            secure_items.append(raw); current_section = current_section or "Безопасность и мультимедиа"; continue
        if any(w in low for w in ["аккумулятор","батарея","зарядное устройство"]):
            power_items.append(raw); current_section = current_section or "Автономность и питание"; continue
        if any(w in low for w in ["габарит","вес —","вес ", "цвет —","цвет "]):
            size_items.append(raw); current_section = current_section or "Размеры и вес"; continue
        if any(w in low for w in ["кабель питания","hdmi","комплектация","что в коробке"]):
            bundle_items.append(raw); current_section = current_section or "Комплектация"; continue

        add_intro(raw)

    out_parts: list[str] = []

    if intro_paragraphs:
        for para in intro_paragraphs[:6]:
            out_parts.append(f"<p>{_html_escape_in_cdata_safe(para)}</p>")

    def _render_ul(title: str, items: list[str]):
        if not items: return
        out_parts.append(f"<h3>{_html_escape_in_cdata_safe(title)}</h3>")
        out_parts.append("<ul>")
        seen=set()
        for it in items:
            it=normalize_free_text_punct(it)
            if not it: continue
            if it in seen: continue
            seen.add(it)
            out_parts.append(f"  <li>{_html_escape_in_cdata_safe(it)}</li>")
        out_parts.append("</ul>")

    _render_ul("Ключевые характеристики", key_items)
    _render_ul("Порты и подключения", ports_items)
    _render_ul("Автономность и питание", power_items)
    _render_ul("Безопасность и мультимедиа", secure_items)
    _render_ul("Размеры и вес", size_items)
    _render_ul("Комплектация", bundle_items)
    _render_ul("Почему стоит выбрать", why_items)

    html_out = "\n".join([p for p in out_parts if p.strip()])
    html_out = re.sub(r"<h3>\s*и подключения\s*</h3>\s*", "", html_out, flags=re.I)
    html_out = re.sub(r"(?:<h3>[^<]+</h3>\s*){2,}", lambda m: m.group(0).split("</h3>")[0]+"</h3>\n", html_out)
    return html_out

# =============== COMPATIBILITY (расширено) =======================
BRAND_WORDS = ["Canon","HP","Hewlett-Packard","Xerox","Brother","Epson","Samsung","Kyocera","Ricoh","Konica Minolta","Sharp","OKI","Pantum"]
FAMILY_WORDS = [
    "PIXMA","imageRUNNER","iR","imageCLASS","imagePRESS","LBP","MF","i-SENSYS",
    "LaserJet","DeskJet","OfficeJet","PageWide","Color LaserJet","Neverstop","Smart Tank",
    "Phaser","WorkCentre","VersaLink","AltaLink","DocuCentre",
    "DCP","HL","MFC","FAX",
    "L","XP","WF","WorkForce","EcoTank",
    "FS","TASKalfa","ECOSYS",
    "Aficio","SP","MP","IM",
    "MX","BP","B","C","P2500","M6500","CM","DL","DP"
]
AS_INTERNAL_ART_RE = re.compile(r"^AS\d+", re.I)
MODEL_RE = re.compile(r"\b([A-Z][A-Z0-9\-]{2,})\b", re.I)

def _split_joined_models(s: str) -> List[str]:
    s=(s or "")
    s=s.replace("/", " ").replace("\\", " ")
    s=re.sub(r"[,;|]+"," ", s)
    s=re.sub(r"\s{2,}"," ", s)
    return s.split()

def extract_full_compatibility(text: str, params_pairs: List[Tuple[str,str]]) -> str:
    compat=""
    for k,v in params_pairs:
        if _norm_text(k) in {"совместимость","совместимые модели","принтеры"}:
            compat+=f"; {v}"
    txt=(text or "")
    txt=re.sub(r"совместим\w*[:：﹕∶︰-]*","", txt, flags=re.I)
    raw_models=re.findall(MODEL_RE, txt)
    families=[]
    for word in FAMILY_WORDS:
        if re.search(rf"\b{re.escape(word)}\b", txt, flags=re.I):
            families.append(word)
    if raw_models or families:
        models=_split_joined_models(" ".join(raw_models))
        if models:
            found=[]
            for m in models:
                if re.search(r"^[A-Za-z0-9][A-Za-z0-9\-]+$", m): found.append(m.upper())
            clean=[]
            for x in found:
                x=re.sub(r"\s{2,}"," ", x).strip(" ,;.")
                if x and x not in clean:
                    clean.append(x)
            compat=", ".join(clean[:50])
    compat = re.sub(r"\s{2,}", " ", compat).strip()
    return compat

# ======================= KIND DETECTION =======================
def detect_kind(name: str, params_pairs: List[Tuple[str,str]]) -> str:
    n=(name or "").lower()
    if "картридж" in n or "тонер" in n or "тонер-" in n: return "cartridge"
    if ("ибп" in n) or ("ups" in n) or ("источник бесперебойного питания" in n): return "ups"
    for k,_ in params_pairs:
        if _norm_text(k).startswith("тип ибп"): return "ups"
    if "мфу" in n or "printer" in n or "принтер" in n: return "mfp"
    return "other"

def split_short_name(name: str) -> str:
    s=(name or "").strip()
    s=re.split(r"\s+[—-]\s+", s, maxsplit=1)[0]
    return s if len(s)<=80 else s[:77]+"..."

def _seo_title(name: str, vendor: str, kind: str, kv_all: Dict[str,str], seed: int) -> str:
    short = split_short_name(name)
    variants = [
        "Кратко о плюсах","Чем удобен","Ключевые преимущества","Что вы получаете с",
        "Хороший выбор","Удачный выбор","Надежный вариант"
    ]
    p = variants[seed % len(variants)]
    v = vendor.strip() or ""
    return f"{short}: {p} ({v})" if v else f"{short}: {p}"

def build_lead_bullets(
    name: str,
    vendor: str,
    kind: str,
    params_pairs: List[Tuple[str,str]],
    raw_kv: List[Tuple[str,str]],
    seed: int,
    raw_desc_text_for_kv: str = ""
) -> List[str]:
    kv_all={}
    for k,v in (params_pairs + raw_kv):
        k_n=_norm_text(k)
        if k_n in {"вес","гарантия","объём","количество","частота обновления экрана","тип матрицы экрана","диагональ экрана",
                   "разрешение экрана","тип ноутбука","тип оперативной памяти","объем накопителя","тип накопителя","видеокарта",
                   "операционная система","процессор"}:
            kv_all[k]=v
    bullets=[]
    if kind=="cartridge":
        for key in ("цвет печати","ресурс","тип печати"):
            for k,v in raw_kv:
                if _norm_text(k)==key:
                    bullets.append(f"✅ {k}: {v}")
        if not bullets:
            for k,v in params_pairs:
                if _norm_text(k) in {"цвет печати","ресурс","тип печати"}:
                    bullets.append(f"✅ {k}: {v}")
        compat = extract_full_compatibility(raw_desc_text_for_kv, params_pairs)
    else:
        for k,v in (params_pairs + raw_kv):
            if len(bullets)>=3: break
            k_low=k.strip().lower()
            if any(x in k_low for x in ["совместим","описание","состав","страна","гарант"]): continue
            bullets.append(f"✅ {k.strip()}: {v.strip()}")
        compat = ""

    title = _seo_title(name, vendor, kind, kv_all, seed)

    html_parts=[]
    html_parts.append(f"<h3>{_html_escape_in_cdata_safe(title)}</h3>")
    p_line = {
        "cartridge": "Стабильная печать и предсказуемый ресурс для повседневных задач.",
        "ups": "Базовая защита питания для домашней и офисной техники.",
        "mfp": "Офисная серия с упором на скорость, качество и удобное управление.",
        "other": "Практичное решение для ежедневной работы."
    }.get(kind,"Практичное решение для ежедневной работы.")
    html_parts.append(f"<p>{_html_escape_in_cdata_safe(p_line)}</p>")

    if bullets:
        html_parts.append("<ul>")
        for b in bullets[:5]:
            html_parts.append(f"  <li>{_html_escape_in_cdata_safe(b)}</li>")
        html_parts.append("</ul>")

    if compat:
        compat_html = _html_escape_in_cdata_safe(compat).replace(";", "; ").replace(",", ", ")
        html_parts.append(f"<p><strong>Полная совместимость:</strong><br>{compat_html}</p>")
    return "\n".join(html_parts)

CITIES = ["Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз","Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавловск","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]

def build_faq_html(kind: str) -> str:
    if kind=="cartridge":
        qs=[("Подойдёт ли моему принтеру?","Проверьте список совместимых моделей в карточке."),
            ("Можно ли заправлять?","Для многих моделей есть совместимые расходники — смотрите описание."),]
    else:
        qs=[("Поддерживаются современные сценарии?","Да, ориентирован на повседневную офисную работу."),
            ("Можно расширять возможности?","Да, подробности — в характеристиках модели."),]
    parts=["<h3>FAQ</h3>"]
    for q,a in qs:
        parts.append(f"<p><strong>В:</strong> {_html_escape_in_cdata_safe(q)}<br><strong>О:</strong> {_html_escape_in_cdata_safe(a)}</p>")
    return "\n".join(parts)

def build_reviews_html(seed: int) -> str:
    samples=[
        ("Даурен","Печать/работа стабильная, всё как ожидал.","★★★★★"),
        ("Инна","Установка заняла пару минут, проблем не было.","★★★★★"),
        ("Ерлан","Коробка пришла слегка помятой, но сам товар без нареканий.","★★★★☆"),
    ]
    parts=["<h3>Отзывы (3)</h3>"]
    for i,(name,comment,stars) in enumerate(samples):
        city = choose(CITIES, seed, i+3)
        parts.append(
            f"<p>👤 <strong>{_html_escape_in_cdata_safe(name)}</strong>, { _html_escape_in_cdata_safe(city) } — {stars}<br>"
            f"«{ _html_escape_in_cdata_safe(comment) }»</p>"
        )
    return "\n".join(parts)

# === Sticky cache ===
def load_seo_cache(path: str) -> Dict[str, dict]:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f: return json.load(f)
        except Exception:
            return {}
    if os.path.exists(LEGACY_CACHE_PATH):
        try:
            with open(LEGACY_CACHE_PATH, "r", encoding="utf-8") as f: return json.load(f)
        except Exception:
            return {}
    return {}

def save_seo_cache(path: str, data: Dict[str, dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def should_periodic_refresh(prev_dt_utc: Optional[datetime]) -> bool:
    mode = SEO_REFRESH_MODE
    if mode == "off": return False
    if mode == "days":
        if not prev_dt_utc: return True
        return (now_utc() - prev_dt_utc) >= timedelta(days=max(1, SEO_REFRESH_DAYS))
    if mode == "monthly":
        if not prev_dt_utc: return True
        prev_alm = prev_dt_utc.astimezone(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(prev_dt_utc.timestamp()+5*3600)
        now_alm  = now_almaty()
        if now_alm.year != prev_alm.year: return True
        if prev_alm.day == 1: return False
        return now_alm.month != prev_alm.month and now_alm.day >= 1
    return False

def compute_seo_checksum(name: str, lead_inputs: Dict[str,str], raw_desc_text_for_kv: str) -> str:
    base = "|".join([name or "", lead_inputs.get("kind",""), lead_inputs.get("title",""),
                     lead_inputs.get("bullets",""), hashlib.md5((raw_desc_text_for_kv or "").encode("utf-8")).hexdigest()])
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def compose_full_description_html(lead_html: str, raw_desc_html_full: str, specs_html: str, faq_html: str, reviews_html: str) -> str:
    pieces=[]
    if lead_html: pieces.append(lead_html)
    if raw_desc_html_full: pieces.append(_html_escape_in_cdata_safe(raw_desc_html_full))
    if specs_html: pieces.append(specs_html)
    if faq_html: pieces.append(faq_html)
    if reviews_html: pieces.append(reviews_html)
    return "\n".join(pieces)

def inject_seo_descriptions(shop_el: ET.Element) -> Tuple[int, str]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0, ""
    cache = load_seo_cache(SEO_CACHE_PATH) if SEO_STICKY else {}
    changed=0
    for offer in offers_el.findall("offer"):
        name = get_text(offer, "name")
        d = offer.find("description")

        raw_desc_html_full = inner_html(d) if d is not None else ""
        raw_desc_text_for_kv = re.sub(r"<br\s*/?>", "\n", raw_desc_html_full, flags=re.I)
        raw_desc_text_for_kv = re.sub(r"<[^>]+>", "", raw_desc_text_for_kv)

        params_pairs = build_specs_pairs_from_params(offer)
        raw_kv = extract_kv_from_description(raw_desc_text_for_kv)
        inputs = {"kind":"", "title":"", "bullets":""}

        vendor = get_text(offer,"vendor").strip()
        kind   = detect_kind(name, params_pairs)
        inputs["kind"]=kind

        s_id = offer.attrib.get("id") or get_text(offer,"vendorCode") or name
        seed = int(hashlib.md5((s_id or "").encode("utf-8")).hexdigest()[:8], 16)
        faq_html = build_faq_html(kind)
        reviews_html = build_reviews_html(seed)

        specs_html = "" if has_specs_in_raw_desc(raw_desc_html_full) else build_specs_html_from_params(offer)

        checksum = compute_seo_checksum(name, inputs, raw_desc_text_for_kv)
        cache_key = offer.attrib.get("id") or (get_text(offer,"vendorCode") or "").strip() or hashlib.md5((name or "").encode("utf-8")).hexdigest()

        use_cache = False
        if SEO_STICKY and cache.get(cache_key):
            ent = cache[cache_key]
            prev_cs = ent.get("checksum","")
            updated_at_prev = ent.get("updated_at","")
            try:
                prev_dt_utc = datetime.strptime(updated_at_prev, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                prev_dt_utc = None
            periodic = should_periodic_refresh(prev_dt_utc)
            if prev_cs == checksum and not periodic:
                lead_html   = ent.get("lead_html", "")
                faq_html    = ent.get("faq_html", faq_html)
                reviews_html= ent.get("reviews_html", reviews_html)
                use_cache   = True
        if not use_cache:
            lead_html = build_lead_bullets(name, vendor, kind, params_pairs, raw_kv, seed, raw_desc_text_for_kv)

        formatted_native = format_native_description(raw_desc_html_full)
        full_html = compose_full_description_html(lead_html, formatted_native, specs_html, faq_html, reviews_html)
        placeholder = f"[[[HTML]]]{full_html}[[[/HTML]]]"

        if d is None:
            d = ET.SubElement(offer, "description")
            d.text = placeholder
            changed += 1
        else:
            old_in = inner_html(d)
            if old_in != placeholder:
                d.clear()
                d.text = placeholder
                changed += 1

        if SEO_STICKY:
            cache[cache_key] = {
                "checksum": checksum,
                "updated_at": now_utc().strftime("%Y-%m-%d %H:%M:%S"),
                "lead_html": lead_html,
                "faq_html": faq_html,
                "reviews_html": reviews_html,
            }

    last_alm: Optional[datetime] = None
    if cache:
        for ent in cache.values():
            ts = ent.get("updated_at")
            if not ts: continue
            try:
                utc_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                alm_dt = utc_dt.astimezone(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(utc_dt.timestamp()+5*3600)
                if (last_alm is None) or (alm_dt > last_alm): last_alm = alm_dt
            except Exception:
                continue
    if not last_alm: last_alm = now_almaty()
    return changed, format_dt_almaty(last_alm)

# ======================= PICTURES / PLACEHOLDERS =======================
_url_head_cache: Dict[str,bool] = {}
def _url_heads_ok(url: str) -> bool:
    if url in _url_head_cache: return _url_head_cache[url]
    try:
        r=requests.head(url, timeout=PLACEHOLDER_HEAD_TIMEOUT, allow_redirects=True)
        ok = (200 <= r.status_code < 400)
    except Exception:
        ok = False
    _url_head_cache[url]=ok
    return ok

def _slug(s: str) -> str:
    if not s: return ""
    table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    base=(s or "").lower().translate(table)
    base=re.sub(r"[^a-z0-9\- ]+","", base)
    return re.sub(r"\s+","-", base).strip("-") or "unknown"

def _placeholder_url_brand(vendor: str) -> str:
    return f"{PLACEHOLDER_BRAND_BASE}/{_slug(vendor)}.{PLACEHOLDER_EXT}"

def _placeholder_url_category(kind: str) -> str:
    return f"{PLACEHOLDER_CATEGORY_BASE}/{kind}.{PLACEHOLDER_EXT}"

def ensure_placeholder_pictures(shop_el: ET.Element) -> Tuple[int,int]:
    if not PLACEHOLDER_ENABLE: return (0,0)
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0)
    added=skipped=0
    for offer in offers_el.findall("offer"):
        pics = list(offer.findall("picture"))
        has_pic = any((p.text or "").strip() for p in pics)
        if has_pic: continue
        vendor = get_text(offer,"vendor").strip()
        kind   = detect_kind(get_text(offer,"name"), build_specs_pairs_from_params(offer))
        brand_url=_placeholder_url_brand(vendor) if vendor else ""
        cat_url=_placeholder_url_category(kind) if kind else ""
        pick=[u for u in (brand_url, cat_url, PLACEHOLDER_DEFAULT_URL) if u]
        url = pick[0]
        if not _url_heads_ok(url):
            url = PLACEHOLDER_DEFAULT_URL
        ET.SubElement(offer,"picture").text = url
        added+=1
    return added,0

# ======================= CLEANUP / ORDER =======================
def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int,int,int]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
    t_cnt=f_cnt=st_cnt=ss_cnt=0
    for offer in offers_el.findall("offer"):
        offer.attrib["available"] = "true" if os.getenv("FORCE_AVAILABLE","1") in {"1","true","yes"} else (offer.attrib.get("available") or "true")
        if offer.attrib["available"].lower() not in {"true","false"}:
            offer.attrib["available"]="true"
        status=get_text(offer,"status") or get_text(offer,"Status")
        if status:
            if "в наличии" in _norm_text(status) or "есть на складе" in _norm_text(status):
                offer.attrib["available"]="true"; st_cnt+=1
            elif "нет в наличии" in _norm_text(status) or "ожидается" in _norm_text(status):
                offer.attrib["available"]="false"; ss_cnt+=1
        if DROP_STOCK_TAGS: remove_all(offer, "quantity_in_stock","quantity","stock","Stock")
    return t_cnt,f_cnt,st_cnt,ss_cnt

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
    s=re.sub(r"[\s_]+","",s).replace("—","-").replace("–","-")
    return re.sub(r"[^A-Za-z0-9\-]+","",s).upper()

def ensure_vendorcode_with_article(shop_el:ET.Element,prefix:str,create_if_missing:bool=False)->Tuple[int,int,int,int]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
    total_prefixed=created=filled_from_art=fixed_bare=0
    for offer in offers_el.findall("offer"):
        vc=offer.find("vendorCode")
        if vc is None:
            if create_if_missing: vc=ET.SubElement(offer,"vendorCode"); vc.text=""; created+=1
            else: continue
        val=(vc.text or "").strip()
        if not val:
            art=_extract_article_from_name(get_text(offer,"name")) or _extract_article_from_url(get_text(offer,"url"))
            if art:
                vc.text=prefix+_normalize_code(art); filled_from_art+=1
        else:
            norm=_normalize_code(val)
            if not norm.startswith(prefix):
                vc.text=prefix+norm; fixed_bare+=1
        total_prefixed+=1
    return total_prefixed,created,filled_from_art,fixed_bare

def purge_offer_tags_and_attrs_after(offer: ET.Element) -> int:
    removed=0
    for t in PURGE_TAGS_AFTER:
        removed+=len(offer.findall(t)); remove_all(offer, t)
    for a in list(offer.attrib.keys()):
        if a in PURGE_OFFER_ATTRS_AFTER:
            del offer.attrib[a]
            removed+=1
    return removed

def fix_currency_id(shop_el: ET.Element, default_code: str="KZT") -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    changed=0
    for offer in offers_el.findall("offer"):
        node=offer.find("currencyId")
        if node is None:
            node=ET.SubElement(offer,"currencyId"); node.text=default_code; changed+=1
        else:
            tx=(node.text or "").strip()
            if tx.upper()!="KZT":
                node.text=default_code; changed+=1
    return changed

def ensure_keywords(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for off in offers_el.findall("offer"):
        remove_all(off, "keywords")
        kw = build_keywords_for_offer(off)
        if kw:
            ET.SubElement(off, "keywords").text = kw
            touched+=1
    return touched

def ensure_categoryid_zero_first(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        remove_all(offer,"categoryId","CategoryId")
        cid=ET.Element("categoryId"); cid.text=os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0,cid); touched+=1
    return touched

# ======================= KEYWORDS =======================
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
STOPWORDS_RU = {"для","и","или","на","в","из","от","по","с","к","до","над","под","при","между","между","из-за","через","при","где","что","это","той","тот","эта","эти",
                "бумага","бумаги","бумаг","черный","чёрный","белый","цветной","пластик","комплект","набор","тип","модель","модели","формат","новый","новинка"}
STOPWORDS_EN = {"for","and","or","with","of","the","a","an","to","in","on","by","is","are","be","from","new","original","type","model","set","kit","pack"}
GENERIC_DROP = {"изделие","товар","продукция","аксессуар","устройство","оборудование"}

def tokenize_name(name: str) -> List[str]: return WORD_RE.findall(name or "")
def is_content_word(token: str) -> bool:
    t=_norm_text(token)
    return bool(t) and (t not in STOPWORDS_RU) and (t not in STOPWORDS_EN) and (t not in GENERIC_DROP) and (any(ch.isdigit() for ch in t) or "-" in t or len(t)>=3)

def build_bigrams(words: List[str]) -> List[str]:
    out=[]; 
    for i in range(len(words)-1):
        a,b=words[i],words[i+1]
        if is_content_word(a) and is_content_word(b):
            out.append(f"{a} {b}")
    return out

def dedup_preserve_order(seq: List[str]) -> List[str]:
    out=[]; seen=set()
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def extract_model_tokens(offer: ET.Element) -> List[str]:
    name=get_text(offer,"name")
    content=name or ""
    vc=get_text(offer,"vendorCode")
    if vc: content += " " + vc
    url=get_text(offer,"url")
    if url:
        last=re.sub(r"\.(html?|php|aspx?)$","", (urllib.parse.urlparse(url).path or "").split("/")[-1], flags=re.I)
        content += " " + last
    raw_tokens=tokenize_name(name or "")
    modelish=[t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content=[t for t in raw_tokens if is_content_word(t)]
    bigr=build_bigrams(content)
    norm=lambda tok: tok if re.search(r"[A-Z]{2,}", tok) else tok.capitalize()
    out=modelish[:8]+bigr[:8]+[norm(t) for t in content[:10]]
    return dedup_preserve_order(out)

def color_tokens(name: str) -> List[str]:
    n=_norm_text(name)
    colors_ru=["черный","чёрный","белый","серый","серебристый","синий","голубой","красный","зеленый","зелёный","желтый","жёлтый","фиолетовый","розовый","коричневый"]
    colors_en=["black","white","silver","gray","blue","red","green","yellow","purple","pink","brown"]
    out=[]
    for c in colors_ru+colors_en:
        if re.search(rf"\b{re.escape(c)}\b", n): out.append(c.capitalize())
    return out

def keywords_from_name_generic(name: str) -> List[str]:
    words=tokenize_name(name)
    content=[w for w in words if is_content_word(w)]
    bigr=build_bigrams(content)
    return dedup_preserve_order([*bigr[:8], *content[:10]])

def translit_ru_to_lat(s: str) -> str:
    table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    s=(s or "").lower().translate(table)
    s=re.sub(r"[^a-z0-9\- ]+","", s)
    s=re.sub(r"\s{2,}"," ", s).strip()
    return s

def geo_tokens() -> List[str]:
    if not SATU_KEYWORDS_GEO: return []
    toks=["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
          "Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавловск","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]
    if SATU_KEYWORDS_GEO_LAT:
        toks += ["Kazakhstan","Almaty","Astana","Shymkent","Karaganda","Aktobe","Pavlodar","Atyrau","Taraz",
                 "Oskemen","Semey","Kostanay","Kyzylorda","Oral","Petropavl","Taldykorgan","Aktau",
                 "Temirtau","Ekibastuz","Kokshetau","Rudny"]
    toks=dedup_preserve_order(toks)
    return toks[:max(0,SATU_KEYWORDS_GEO_MAX)]

def build_keywords_for_offer(offer: ET.Element) -> str:
    if SATU_KEYWORDS == "off": return ""
    name=get_text(offer,"name"); vendor=get_text(offer,"vendor").strip()
    parts=[vendor] if vendor else []
    parts += extract_model_tokens(offer) + keywords_from_name_generic(name) + color_tokens(name)
    extra=[]
    for w in parts:
        if re.search(r"[А-Яа-яЁё]", str(w)):
            tr=translit_ru_to_lat(str(w))
            if tr and tr not in extra: extra.append(tr)
    parts+=extra+geo_tokens()
    out=[]
    for w in parts:
        w=w.strip().strip(",.;")
        if not w: continue
        if w.lower() in {"msi","asus","acer","lenovo","hp","dell","apple"}:
            out.append(w)
        elif len(w)>=2:
            out.append(w)
    kw=", ".join(dedup_preserve_order(out))
    return kw[:SATU_KEYWORDS_MAXLEN]

def render_feed_meta_comment(pairs:Dict[str,str]) -> str:
    rows = [
        ("Поставщик", pairs.get("supplier","")),
        ("URL поставщика", pairs.get("source","")),
        ("Время сборки (Алматы)", pairs.get("built_alm","")),
        ("Ближайшее время сборки (Алматы)", pairs.get("next_build_alm","")),
        ("Последнее обновление SEO-блока", pairs.get("seo_last_update_alm","")),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total","0"))),
        ("Сколько товаров у поставщика после фильтра", str(pairs.get("offers_written","0"))),
        ("Сколько товаров есть в наличии (true)", str(pairs.get("available_true","0"))),
        ("Сколько товаров нет в наличии (false)", str(pairs.get("available_false","0"))),
    ]
    key_w=max(len(k) for k,_ in rows)
    lines=["FEED_META"]+[f"{k.ljust(key_w)} | {v}" for k,v in rows]
    return "\n".join(lines)

def main()->None:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")
    offers_in_el=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None: err("XML: <offers> not found")
    src_offers=list(offers_in_el.findall("offer"))

    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")

    for off in src_offers:
        out_offers.append(deepcopy(off))

    try:
        mode=ALSTYLE_CATEGORIES_MODE
        if mode in {"include","exclude"}:
            removed=apply_category_rules(out_shop, mode, ALSTYLE_CATEGORIES_PATH)
            log(f"Category rules ({mode}): removed={removed}")
    except Exception as e:
        warn(f"category rules warn: {e}")

    auto_vendor = ensure_vendor_auto_fill(out_shop); log(f"Vendor autofill: {auto_vendor}")

    updated,skipped,total,src_stats=reprice_offers(out_shop, PRICE_RULES)
    log(f"Pricing: updated={updated}, skipped={skipped}, total={total}, src={src_stats}")

    for off in out_offers.findall("offer"): clean_params(off)

    changed_seo, seo_last_update_alm = inject_seo_descriptions(out_shop)
    log(f"Descriptions updated: {changed_seo}; SEO last update (Almaty): {seo_last_update_alm}")

    for off in out_offers.findall("offer"): purge_offer_tags_and_attrs_after(off)

    t_true, t_false, _, _ = normalize_available_field(out_shop)
    fix_currency_id(out_shop, default_code="KZT")

    for off in out_offers.findall("offer"): purge_offer_tags_and_attrs_after(off)

    DESIRED_ORDER=["vendorCode","name","price","picture","vendor","currencyId","description"]
    def reorder_offer_children(shop_el: ET.Element) -> int:
        offers_el=shop_el.find("offers")
        if offers_el is None: return 0
        changed=0
        for offer in offers_el.findall("offer"):
            children=list(offer)
            if not children: continue
            buckets={k:[] for k in DESIRED_ORDER}; others=[]
            for node in children: (buckets[node.tag] if node.tag in buckets else others).append(node)
            rebuilt=[*sum((buckets[k] for k in DESIRED_ORDER), []), *others]
            if rebuilt!=children:
                for node in children: offer.remove(node)
                for node in rebuilt: offer.append(node)
                changed+=1
        return changed

    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)

    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    try: ET.indent(out_root, space="  ")
    except Exception: pass

    xml_text = ET.tostring(out_root, encoding="unicode")
    def _replace_html_placeholders_with_cdata(xml_s: str) -> str:
        return re.sub(
            r"\[\[\[HTML\]\]\]((?s).*?)\[\[\[\/HTML\]\]\]",
            lambda m: "<![CDATA[\n" + m.group(1) + "\n]]>",
            xml_s,
            flags=re.DOTALL
        )
    xml_text=_replace_html_placeholders_with_cdata(xml_text)

    meta_pairs={
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "built_alm": format_dt_almaty(now_almaty()),
        "next_build_alm": format_dt_almaty(now_almaty()+timedelta(days=1)),
        "seo_last_update_alm": seo_last_update_alm,
        "offers_total": str(len(src_offers)),
        "offers_written": str(len(out_offers.findall("offer"))),
        "available_true": str(t_true),
        "available_false": str(t_false),
    }
    meta_comment = render_feed_meta_comment(meta_pairs)
    xml_text = "<!--\n" + meta_comment + "\n-->\n" + xml_text

    try:
        with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
            f.write(xml_text)
    except UnicodeEncodeError as e:
        warn(f"{ENC} can't encode some characters ({e}); writing with xmlcharrefreplace fallback")
        data_bytes = xml_text.encode(ENC, errors="xmlcharrefreplace")
        with open(OUT_FILE_YML, "wb") as f:
            f.write(data_bytes)

    try:
        docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e: warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | script={SCRIPT_VERSION} | cache={SEO_CACHE_PATH}")

if __name__ == "__main__":
    try: main()
    except Exception as e: err(str(e))
