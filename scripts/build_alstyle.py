# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle → YML: стабильные цены/наличие + безопасный HTML для <description>.

Главное:
- «Родное описание» (всё, что было до внедрения SEO) берём ПОЛНОСТЬЮ, без изменений.
- Структура <description>: [SEO-блок (без FAQ/отзывов)] + [родное описание целиком] + [FAQ] + [Отзывы].
- Если в родном описании нет «Характеристик», аккуратно добавляем их из <param>.
- Для картриджей — в SEO-блоке полный список совместимости (если удаётся извлечь).
- Липкий SEO (sticky) с кэшем: docs/alstyle_cache/seo_cache.json
- FEED_META: «Последнее обновление SEO-блока», корректный формат времени %d:%m:%Y - %H:%M:%S, выравнивание по '|'.

ENV:
  SUPPLIER_URL, OUT_FILE, OUTPUT_ENCODING, TIMEOUT_S, RETRIES, RETRY_BACKOFF_S
  PRICE_CAP_THRESHOLD, PRICE_CAP_VALUE
  VENDORCODE_PREFIX, VENDORCODE_CREATE_IF_MISSING
  ALSTYLE_CATEGORIES_PATH, ALSTYLE_CATEGORIES_MODE
  SATU_KEYWORDS, SATU_KEYWORDS_MAXLEN, SATU_KEYWORDS_MAXWORDS, SATU_KEYWORDS_GEO, SATU_KEYWORDS_GEO_MAX, SATU_KEYWORDS_GEO_LAT
  SEO_STICKY=1|0, SEO_CACHE_PATH=docs/alstyle_cache/seo_cache.json, SEO_REFRESH_DAYS=14
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

SCRIPT_VERSION = "alstyle-2025-10-21.SEOblock-sticky-safehtml.v4"

# ========== ENV / CONST ==========
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
SATU_KEYWORDS_MAXWORDS = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "1000"))
SATU_KEYWORDS_GEO      = os.getenv("SATU_KEYWORDS_GEO", "on").lower() in {"on","1","true","yes"}
SATU_KEYWORDS_GEO_MAX  = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))
SATU_KEYWORDS_GEO_LAT  = os.getenv("SATU_KEYWORDS_GEO_LAT", "on").lower() in {"on","1","true","yes"}

# SEO sticky cache
DEFAULT_CACHE_PATH = "docs/alstyle_cache/seo_cache.json"
SEO_CACHE_PATH     = os.getenv("SEO_CACHE_PATH", DEFAULT_CACHE_PATH)
SEO_STICKY         = os.getenv("SEO_STICKY", "1").lower() in {"1","true","yes","on"}
SEO_REFRESH_DAYS   = int(os.getenv("SEO_REFRESH_DAYS", "14"))
LEGACY_CACHE_PATH  = "docs/seo_cache.json"

# Purge internals
DROP_CATEGORY_ID_TAG   = True
DROP_STOCK_TAGS        = True
PURGE_TAGS_AFTER       = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER= ("type","article")
INTERNAL_PRICE_TAGS    = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                          "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice",
                          "max_price","maxPrice","oldprice")

# ========== UTILS ==========
log  = lambda m: print(m, flush=True)
warn = lambda m: print(f"WARN: {m}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_utc() -> datetime: return datetime.now(timezone.utc)
def now_utc_str() -> str: return now_utc().strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty() -> datetime:
    try:   return datetime.now(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(time.time()+5*3600)
    except Exception: return datetime.utcfromtimestamp(time.time()+5*3600)
def format_dt_almaty(dt: datetime) -> str: return dt.strftime("%d:%m:%Y - %H:%M:%S")  # латинская M
def next_build_time_almaty() -> datetime:
    cur = now_almaty(); t = cur.replace(hour=1, minute=0, second=0, microsecond=0)
    return t + timedelta(days=1) if cur >= t else t

_COLON_CLASS_RE = re.compile("[:\uFF1A\uFE55\u2236\uFE30]")
canon_colons    = lambda s: _COLON_CLASS_RE.sub(":", s or "")
NOISE_RE = re.compile(r"[\u200B-\u200F\u202A-\u202E\u2060\uFEFF\u00AD\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F\u0080-\u009F]")
def strip_noise_chars(s: str) -> str:
    if not s: return ""
    return NOISE_RE.sub("", s).replace("�","")

def _html_escape_in_cdata_safe(s: str) -> str:
    return (s or "").replace("]]>", "]]&gt;")

# Сериализация ВНУТРЕННЕГО HTML узла <description>
def inner_html(el: ET.Element) -> str:
    if el is None: return ""
    parts=[]
    if el.text: parts.append(el.text)
    for child in el:
        parts.append(ET.tostring(child, encoding="unicode"))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts).strip()

# ========== LOAD SOURCE ==========
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

# ========== XML HELPERS ==========
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

# ========== CATEGORY TREE ==========
class CatRule:
    __slots__=("raw","kind","pattern")
    def __init__(self, raw: str, kind: str, pattern): self.raw, self.kind, self.pattern = raw, kind, pattern

def _norm_text(s: str) -> str:
    s=(s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

def _norm_cat(s: str) -> str:
    if not s: return ""
    s=s.replace("\u00A0"," "); s=re.sub(r"\s*[/>\|]\s*", " / ", s)
    return re.sub(r"\s+"," ", s).strip()

def load_category_rules(path: str) -> Tuple[Set[str], List[CatRule]]:
    if not path or not os.path.exists(path): return set(), []
    data=None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f: data=f.read().replace("\ufeff","").replace("\x00",""); break
        except Exception: continue
    if data is None:
        with open(path,"r",encoding="utf-8",errors="ignore") as f: data=f.read().replace("\x00","")
    ids:Set[str]=set(); rules:List[CatRule]=[]
    for ln in data.splitlines():
        s=ln.strip()
        if not s or s.lstrip().startswith("#"): continue
        if re.fullmatch(r"\d{2,}", s): ids.add(s); continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try: rules.append(CatRule(s,"regex",re.compile(s[1:-1],re.I))); continue
            except Exception: continue
        if s.startswith("~="):
            w=_norm_text(s[2:]); 
            if w: rules.append(CatRule(s,"word",re.compile(r"\b"+re.escape(w)+r"\b",re.I))); continue
        rules.append(CatRule(_norm_text(s),"substr",None))
    return ids, rules

def category_matches_name(path_str: str, rules: List[CatRule]) -> bool:
    cat_norm=_norm_text(_norm_cat(path_str))
    for cr in rules:
        if cr.kind=="substr":
            if cr.raw and cr.raw in cat_norm: return True
        elif cr.pattern and cr.pattern.search(path_str or ""): return True
    return False

def parse_categories_tree(shop_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,Set[str]]]:
    id2name: Dict[str,str]={}; id2parent: Dict[str,str]={}; parent2children: Dict[str,Set[str]]={}
    cats_root=shop_el.find("categories") or shop_el.find("Categories")
    if cats_root is None: return id2name,id2parent,parent2children
    for c in cats_root.findall("category"):
        cid=(c.attrib.get("id") or "").strip()
        if not cid: continue
        pid=(c.attrib.get("parentId") or "").strip()
        id2name[cid]=(c.text or "").strip()
        if pid: id2parent[cid]=pid
        parent2children.setdefault(pid, set()).add(cid)
    return id2name,id2parent,parent2children

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

# ========== BRANDS ==========
def _norm_key(s: str) -> str:
    if not s: return ""
    s=s.strip().lower().replace("ё","е"); s=re.sub(r"[-_/]+"," ",s)
    return re.sub(r"\s+"," ",s)

SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

def normalize_brand(raw: str) -> str:
    k=_norm_key(raw)
    return "" if (not k) or (k in SUPPLIER_BLOCKLIST) else raw.strip()

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, Dict[str,int]]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0,{}
    normalized=0; dropped: Dict[str,int]={}
    for offer in offers_el.findall("offer"):
        ven=offer.find("vendor"); txt=(ven.text or "").strip() if ven is not None and ven.text else ""
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
    first = re.split(r"\s+", name.strip())[0] if name else ""
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

# ========== PRICING ==========
PriceRule = Tuple[int,int,float,int]
PRICING_RULES: List[PriceRule] = [
    (   101,    10000, 4.0,  3000),( 10001, 25000, 4.0,  4000),( 25001, 50000, 4.0,  5000),
    ( 50001,    75000, 4.0,  7000),( 75001,100000, 4.0, 10000),(100001,150000, 4.0, 12000),
    (150001,   200000, 4.0, 15000),(200001,300000, 4.0, 20000),(300001,400000, 4.0, 25000),
    (400001,   500000, 4.0, 30000),(500001,750000, 4.0, 40000),(750001,1000000,4.0, 50000),
    (1000001, 1500000, 4.0, 70000),(1500001,2000000,4.0, 90000),(2000001,100000000,4.0,100000),
]

PRICE_FIELDS_DIRECT=["purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price"]
PRICE_KEYWORDS_DEALER = re.compile(r"(дилер|dealer|опт|wholesale|b2b|закуп|purchase|оптов)", re.I)
PRICE_KEYWORDS_RRP    = re.compile(r"(rrp|ррц|розниц|retail|msrp)", re.I)

def parse_price_number(raw:str)->Optional[float]:
    if raw is None: return None
    s=(raw.strip().replace("\xa0"," ").replace(" ","").replace("KZT","").replace("kzt","").replace("₸","").replace(",","."))
    if not s: return None
    try: v=float(s); return v if v>0 else None
    except Exception: return None

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

_force_tail_900 = lambda n: max(int(n)//1000,0)*1000+900 if int(n)>=0 else 900
def compute_retail(dealer:float,rules:List[PriceRule])->Optional[int]:
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

# ========== PARAMS / TEXT PARСING ==========
EXCLUDE_NAME_RE = re.compile(r"(?:\bартикул\b|благотворительн\w*|штрихкод|оригинальн\w*\s*код|новинк\w*|снижена\s*цена|код\s*тн\s*вэд(?:\s*eaeu)?|код\s*тнвэд(?:\s*eaeu)?|тн\s*вэд|тнвэд|tn\s*ved|hs\s*code)", re.I)

def remove_specific_params(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    removed=0
    for offer in offers_el.findall("offer"):
        seen=set()
        for tag in ("param","Param"):
            for p in list(offer.findall(tag)):
                nm=(p.attrib.get("name") or "").strip(); val=(p.text or "").strip()
                if not nm or not val: offer.remove(p); removed+=1; continue
                if EXCLUDE_NAME_RE.search(nm): offer.remove(p); removed+=1; continue
                k=nm.strip().lower()
                if k in seen: offer.remove(p); removed+=1; continue
                seen.add(k)
    return removed

# lightweight KV/compat extraction from free text (если пригодится)
HDR_RE = re.compile(r"^\s*(технические\s+характеристики|характеристики)\s*:?\s*$", re.I)
HEAD_ONLY_RE = re.compile(r"^\s*(?:основные\s+)?характеристики\s*[:：﹕∶︰-]*\s*$", re.I)
HEAD_PREFIX_RE = re.compile(r"^\s*(?:основные\s+)?характеристики\s*[:：﹕∶︰-]*\s*", re.I)
KV_COLON_RE  = re.compile(r"^\s*([^:]{2,}?)\s*:\s*(.+)$")
URL_RE       = re.compile(r"https?://\S+", re.I)

def normalize_free_text_punct(s: str) -> str:
    t=canon_colons(s or ""); t=re.sub(r":\s*:", ": ", t)
    t=re.sub(r"(?<!\.)\.\.(?!\.)", ".", t)
    return re.sub(r"\s{2,}", " ", t).strip()

def extract_kv_from_description(text: str) -> List[Tuple[str,str]]:
    # (работает по строкам "Ключ: Значение" — не обязателен, потому что есть <param>)
    if not (text or "").strip(): return []
    t=(text or "").replace("\r\n","\n").replace("\r","\n")
    lines=[ln.strip() for ln in t.split("\n") if ln.strip()]
    pairs=[]
    for ln in lines:
        if HDR_RE.match(ln) or HEAD_ONLY_RE.match(ln): continue
        ln=HEAD_PREFIX_RE.sub("", ln)
        if URL_RE.search(ln) and ":" not in ln: continue
        m=KV_COLON_RE.match(canon_colons(ln))
        if m:
            name=(m.group(1) or "").strip()
            val=(m.group(2) or "").strip()
            if name and val: pairs.append((name, normalize_free_text_punct(val)))
    return pairs

def build_specs_pairs_from_params(offer: ET.Element) -> List[Tuple[str,str]]:
    pairs=[]; seen=set()
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name=(p.attrib.get("name") or "").strip(); raw_val =(p.text or "").strip()
        if not raw_name or not raw_val or EXCLUDE_NAME_RE.search(raw_name): continue
        k=raw_name.strip().lower()
        if k in seen: continue
        seen.add(k); pairs.append((raw_name.strip(), normalize_free_text_punct(raw_val)))
    return pairs

def extract_full_compatibility(raw_desc: str, params_pairs: List[Tuple[str,str]]) -> str:
    for n,v in params_pairs:
        if n.strip().lower().startswith("совместим"): return v.strip()
    for n,v in extract_kv_from_description(raw_desc or ""):
        if n.strip().lower().startswith("совместим"): return v.strip()
    return ""

# Автодобавка "Характеристики", если их нет в родном описании
SPEC_PREFERRED_ORDER = [
    "мощность", "ёмкость батареи", "емкость батареи", "время переключения режимов", "диапазон работы avr",
    "количество и тип выходных разъёмов", "количество и тип выходных разъемов",
    "форма выходного сигнала", "выходная частота", "габариты (шхгхв)",
    "вес", "длина кабеля", "защита телефонной линии", "защита от полного разряда батареи", "бесшумный режим",
    "цвет", "гарантия", "состав", "рабочий диапазон температур", "рабочая влажность", "лицевая панель"
]
def _rank_key(k: str) -> Tuple[int, str]:
    k_low = k.strip().lower()
    for i, pref in enumerate(SPEC_PREFERRED_ORDER):
        if k_low == pref: return (i, k)
    for i, pref in enumerate(SPEC_PREFERRED_ORDER):
        if k_low.startswith(pref): return (i, k)
    return (1000, k_low)

def has_specs_in_raw_desc(raw_desc_html: str) -> bool:
    if not raw_desc_html: return False
    s = raw_desc_html.lower()
    return ("<ul" in s and "<li" in s and "характерист" in s) or re.search(r"<h\d[^>]*>\s*характерист", s)

def build_specs_html_from_params(offer: ET.Element) -> str:
    pairs = build_specs_pairs_from_params(offer)
    if not pairs: return ""
    pairs_sorted = sorted(pairs, key=lambda kv: _rank_key(kv[0]))
    parts = ["<h3>Характеристики</h3>", "<ul>"]
    for name, val in pairs_sorted:
        parts.append(f"  <li><strong>{_html_escape_in_cdata_safe(name)}:</strong> { _html_escape_in_cdata_safe(val) }</li>")
    parts.append("</ul>")
    return "\n".join(parts)

# ========== AVAILABILITY ==========
TRUE_WORDS={"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS={"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}
def _parse_bool_str(s: str)->Optional[bool]:
    v=_norm_text(s or "");  return True if v in TRUE_WORDS else False if v in FALSE_WORDS else None
def _parse_int(s: str)->Optional[int]:
    t=re.sub(r"[^\d\-]+","", s or ""); 
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
    t_cnt=f_cnt=st_cnt=ss_cnt=0
    for offer in offers_el.findall("offer"):
        b, src=derive_available(offer)
        remove_all(offer, "available")
        offer.attrib["available"]="true" if b else "false"
        if b: t_cnt+=1
        else: f_cnt+=1
        if src=="stock": st_cnt+=1
        if src=="status": ss_cnt+=1
        if DROP_STOCK_TAGS: remove_all(offer, "quantity_in_stock","quantity","stock","Stock")
    return t_cnt,f_cnt,st_cnt,ss_cnt

# ========== IDS ==========
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

def ensure_vendorcode_with_article(shop_el:ET.Element,prefix:str,create_if_missing:bool=False)->Tuple[int,int,int,int]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
    total_prefixed=created=filled_from_art=fixed_bare=0
    for offer in offers_el.findall("offer"):
        vc=offer.find("vendorCode")
        if vc is None:
            if create_if_missing: vc=ET.SubElement(offer,"vendorCode"); vc.text=""; created+=1
            else: continue
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
        if offer.attrib.get("id")!=new_id: offer.attrib["id"]=new_id; changed+=1
    return changed

# ========== CLEANUP / ORDER ==========
def purge_offer_tags_and_attrs_after(offer:ET.Element)->Tuple[int,int]:
    removed_tags=removed_attrs=0
    for t in PURGE_TAGS_AFTER:
        for node in list(offer.findall(t)): offer.remove(node); removed_tags+=1
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib: offer.attrib.pop(a,None); removed_attrs+=1
    return removed_tags,removed_attrs

def fix_currency_id(shop_el: ET.Element, default_code: str = "KZT") -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        remove_all(offer,"currencyId")
        ET.SubElement(offer,"currencyId").text=default_code; touched+=1
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
        for node in children: (buckets[node.tag] if node.tag in buckets else others).append(node)
        rebuilt=[*sum((buckets[k] for k in DESIRED_ORDER), []), *others]
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

# ========== KEYWORDS ==========
AS_INTERNAL_ART_RE = re.compile(r"^AS\d+", re.I)
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
STOPWORDS_RU = {"для","и","или","на","в","из","от","по","с","к","до","при","через","над","под","о","об","у","без","про","как","это","той","тот","эта","эти",
                "бумага","бумаги","бумаг","черный","чёрный","белый","серый","цвет","оригинальный","комплект","набор","тип","модель","модели","формат","новый","новинка"}
STOPWORDS_EN = {"for","and","or","with","of","the","a","an","to","in","on","by","at","from","new","original","type","model","set","kit","pack"}
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

def dedup_preserve_order(words: List[str]) -> List[str]:
    seen=set(); out=[]
    for w in words:
        key=_norm_text(str(w))
        if key and key not in seen: seen.add(key); out.append(str(w))
    return out

def translit_ru_to_lat(s: str) -> str:
    table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    out=s.lower().translate(table); out=re.sub(r"[^a-z0-9\- ]+","", out); return re.sub(r"\s+","-", out).strip("-")

def color_tokens(name: str) -> List[str]:
    out=[]; low=name.lower()
    mapping={"жёлт":"желтый","желт":"желтый","yellow":"yellow","черн":"черный","black":"black","син":"синий","blue":"blue",
             "красн":"красный","red":"red","зелен":"зеленый","green":"green","серебр":"серебряный","silver":"silver","циан":"cyan","магент":"magenta"}
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
            if AS_INTERNAL_ART_RE.match(t) or not (re.search(r"[A-Z]", t) and re.search(r"\d", t)) or len(t)<5: continue
            tokens.add(t)
    return list(tokens)

def keywords_from_name_generic(name: str) -> List[str]:
    raw_tokens=tokenize_name(name or "")
    modelish=[t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content=[t for t in raw_tokens if is_content_word(t)]
    bigr=build_bigrams(content)
    norm=lambda tok: tok if re.search(r"[A-Z]{2,}", tok) else tok.capitalize()
    out=modelish[:8]+bigr[:8]+[norm(t) for t in content[:10]]
    return dedup_preserve_order(out)

def geo_tokens() -> List[str]:
    if not SATU_KEYWORDS_GEO: return []
    toks=["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
          "Оскемен","Семей","Костанаи","Кызылорда","Орал","Петропавл","Талдыкорган","Актау",
          "Темиртау","Экибастуз","Кокшетау","Рудный"]
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
    parts=[p for p in dedup_preserve_order(parts) if not AS_INTERNAL_ART_RE.match(str(p))]
    parts=parts[:SATU_KEYWORDS_MAXWORDS]
    out=[]; total=0
    for p in parts:
        s=str(p).strip().strip(",")
        if not s: continue
        add=((", " if out else "") + s)
        if total+len(add)>SATU_KEYWORDS_MAXLEN: break
        out.append(s); total+=len(add)
    return ", ".join(out)

def ensure_keywords(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        kw=build_keywords_for_offer(offer)
        if not kw:
            for k in list(offer.findall("keywords")): offer.remove(k)
            continue
        node=offer.find("keywords")
        if node is None: node=ET.SubElement(offer, "keywords")
        node.text=kw; touched+=1
    return touched

# ========== PRICE CAP ==========
def flag_unrealistic_supplier_prices(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    flagged=0
    for offer in offers_el.findall("offer"):
        src_p = parse_price_number(get_text(offer,"price"))
        if src_p is not None and src_p >= PRICE_CAP_THRESHOLD:
            offer.attrib["_force_price"] = str(PRICE_CAP_VALUE); flagged += 1
    return flagged

def enforce_forced_prices(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        if offer.attrib.get("_force_price"):
            _remove_all_price_nodes(offer); ET.SubElement(offer, "price").text=str(PRICE_CAP_VALUE)
            offer.attrib.pop("_force_price", None); touched += 1
    return touched

# ========== SEO BLOCKS ==========
def md5(s: str) -> str: return hashlib.md5((s or "").encode("utf-8")).hexdigest()
def seed_int(s: str) -> int: return int(md5(s)[:8], 16)

NAMES_MALE  = ["Арман","Даурен","Санжар","Ерлан","Аслан","Руслан","Тимур","Данияр","Виктор","Евгений","Олег","Сергей","Нуржан","Бекзат","Азамат","Султан"]
NAMES_FEMALE= ["Айгерим","Мария","Инна","Наталья","Жанна","Светлана","Ольга","Камилла","Диана","Гульнара"]
CITIES = ["Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз","Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавл","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]

def choose(arr: List[str], seed: int, offs: int=0) -> str:
    if not arr: return ""
    return arr[(seed + offs) % len(arr)]

def detect_kind(name: str, params_pairs: List[Tuple[str,str]]) -> str:
    n=(name or "").lower()
    if "картридж" in n or "тонер" in n or "тонер-" in n: return "cartridge"
    if ("ибп" in n) or ("ups" in n) or ("источник бесперебойного питания" in n):
        return "ups"
    # дополнительная эвристика по параметрам
    for k,_ in params_pairs:
        if k.strip().lower().startswith("тип ибп"): return "ups"
    if "мфу" in n or "printer" in n or "принтер" in n: return "mfp"
    return "other"

def split_short_name(name: str) -> str:
    s=(name or "").strip()
    s=re.split(r"\s+[—-]\s+", s, maxsplit=1)[0]
    return s if len(s)<=80 else s[:77]+"..."

def build_lead_html(offer: ET.Element, raw_desc_text_for_kv: str, params_pairs: List[Tuple[str,str]]) -> Tuple[str, Dict[str,str]]:
    name=get_text(offer,"name").strip()
    kind=detect_kind(name, params_pairs)
    short=split_short_name(name)
    s_id = offer.attrib.get("id") or get_text(offer,"vendorCode") or get_text(offer,"name")
    seed = seed_int(s_id)

    title_phrases = ["удачный выбор","практичное решение","надежный вариант","хороший выбор"]
    title = f"Почему {short} — {choose(title_phrases, seed)}"

    kv_from_desc = extract_kv_from_description(raw_desc_text_for_kv)
    kv_all = {k.strip().lower(): v for k,v in (params_pairs + kv_from_desc)}
    bullets: List[str] = []

    if kind=="cartridge":
        if "технология печати" in kv_all: bullets.append(f"✅ Технология печати: {kv_all['технология печати']}")
        res_key = next((k for k in kv_all if k.startswith("ресурс")), "")
        if res_key: bullets.append(f"✅ {res_key.capitalize()}: {kv_all[res_key]}")
        if "цвет печати" in kv_all: bullets.append(f"✅ Цвет печати: {kv_all['цвет печати']}")
        chip = kv_all.get("чип") or kv_all.get("chip") or kv_all.get("наличие чипа")
        if chip: bullets.append(f"✅ Чип: {chip}")
    elif kind=="ups":
        power = kv_all.get("мощность (bt)") or kv_all.get("мощность (bт)") or kv_all.get("мощность (вт)") or kv_all.get("мощность")
        if power: bullets.append(f"✅ Мощность: {power}")
        sw = kv_all.get("время переключения режимов") or kv_all.get("время переключения")
        if sw: bullets.append(f"✅ Переключение: {sw}")
        sockets = kv_all.get("количество и тип выходных разъёмов") or kv_all.get("количество и тип выходных разъемов")
        if sockets: bullets.append(f"✅ Розетки: {sockets}")
        avr = kv_all.get("диапазон работы avr") or kv_all.get("avr") or kv_all.get("рабочая частота, ггц")
        if avr: bullets.append(f"✅ Питание/AVR: {avr}")
    else:
        for k,v in (params_pairs + kv_from_desc):
            if len(bullets)>=3: break
            k_low=k.strip().lower()
            if any(x in k_low for x in ["совместим","описание","состав","страна","гарант"]): continue
            bullets.append(f"✅ {k.strip()}: {v.strip()}")

    compat = extract_full_compatibility(raw_desc_text_for_kv, params_pairs) if kind=="cartridge" else ""

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

    lead_html = "\n".join(html_parts)
    inputs = {"kind": kind, "title": title, "bullets": "|".join(bullets), "compat": compat}
    return lead_html, inputs

def build_faq_html(kind: str) -> str:
    if kind=="cartridge":
        qa = [
            ("Подойдёт к моему устройству?", "Сверьте точный индекс модели и литеру в списке совместимости выше."),
            ("Нужна калибровка после замены?", "Обычно достаточно корректно установить картридж и распечатать тестовую страницу.")
        ]
    elif kind=="ups":
        qa = [
            ("Подойдёт для ПК и роутера?", "Да, для техники своего класса мощности."),
            ("Шумит ли в работе?", "В обычном режиме — тихо; сигнализация срабатывает только при событиях.")
        ]
    else:
        qa = [
            ("Поддерживаются современные сценарии?", "Да, ориентирован на повседневную офисную работу."),
            ("Можно расширять возможности?", "Да, подробности — в характеристиках модели.")
        ]
    parts=["<h3>FAQ</h3>"]
    for q,a in qa:
        parts.append(f"<p><strong>В:</strong> { _html_escape_in_cdata_safe(q) }<br><strong>О:</strong> { _html_escape_in_cdata_safe(a) }</p>")
    return "\n".join(parts)

def build_reviews_html(seed: int) -> str:
    parts=["<h3>Отзывы (3)</h3>"]
    stars = ["⭐⭐⭐⭐⭐","⭐⭐⭐⭐⭐","⭐⭐⭐⭐☆"]
    for i in range(3):
        name = choose(NAMES_MALE if i!=1 else NAMES_FEMALE, seed, i)
        city = choose(CITIES, seed, i+3)
        comment_bank = [
            "Печать/работа стабильная, всё как ожидал.",
            "Установка заняла пару минут, проблем не было.",
            "Для повседневных задач подходит отлично.",
            "Качество ровное, без неприятных сюрпризов.",
            "Хороший вариант за свои деньги."
        ]
        comment = choose(comment_bank, seed, i+7)
        parts.append(
            f"<p>👤 <strong>{_html_escape_in_cdata_safe(name)}</strong>, { _html_escape_in_cdata_safe(city) } — {stars[i]}<br>"
            f"«{ _html_escape_in_cdata_safe(comment) }»</p>"
        )
    return "\n".join(parts)

# === CACHE ===
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

def compute_seo_checksum(name: str, lead_inputs: Dict[str,str], raw_desc_text_for_kv: str) -> str:
    base = "|".join([name or "", lead_inputs.get("kind",""), lead_inputs.get("title",""),
                     lead_inputs.get("bullets",""), lead_inputs.get("compat",""), md5(raw_desc_text_for_kv or "")])
    return md5(base)

def compose_full_description_html(lead_html: str, raw_desc_html_full: str, specs_html: str, faq_html: str, reviews_html: str) -> str:
    pieces=[]
    if lead_html: pieces.append(lead_html)
    if raw_desc_html_full: pieces.append(_html_escape_in_cdata_safe(raw_desc_html_full))  # родное — полностью
    if specs_html: pieces.append(specs_html)  # только если в родном не было
    if faq_html: pieces.append(faq_html)
    if reviews_html: pieces.append(reviews_html)
    return "\n\n".join(pieces)

def inject_seo_descriptions(shop_el: ET.Element) -> Tuple[int, str]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0, ""
    cache = load_seo_cache(SEO_CACHE_PATH) if SEO_STICKY else {}
    changed=0
    for offer in offers_el.findall("offer"):
        name = get_text(offer, "name")
        d = offer.find("description")

        # 1) Родное описание: берём ВНУТРЕННИЙ HTML полностью
        raw_desc_html_full = inner_html(d) if d is not None else ""
        # Для извлечения пар "Ключ: Значение" из текста — используем "псевдо-текст":
        raw_desc_text_for_kv = re.sub(r"<br\s*/?>", "\n", raw_desc_html_full, flags=re.I)
        raw_desc_text_for_kv = re.sub(r"<[^>]+>", "", raw_desc_text_for_kv)  # убираем теги

        params_pairs = build_specs_pairs_from_params(offer)

        # 2) SEO-лид
        lead_html, inputs = build_lead_html(offer, raw_desc_text_for_kv, params_pairs)
        kind = inputs.get("kind","other")

        # 3) FAQ/Отзывы
        faq_html = build_faq_html(kind)
        s_id = offer.attrib.get("id") or get_text(offer,"vendorCode") or name
        seed = seed_int(s_id)
        reviews_html = build_reviews_html(seed)

        # 4) Характеристики из <param>, ТОЛЬКО если в родном описании их не было
        specs_html = "" if has_specs_in_raw_desc(raw_desc_html_full) else build_specs_html_from_params(offer)

        checksum = compute_seo_checksum(name, inputs, raw_desc_text_for_kv)
        cache_key = offer.attrib.get("id") or (get_text(offer,"vendorCode") or "").strip() or md5(name)

        use_cache = False
        if SEO_STICKY and cache.get(cache_key):
            ent = cache[cache_key]
            prev_cs = ent.get("checksum","")
            updated_at_prev = ent.get("updated_at","")
            try:
                prev_dt = datetime.strptime(updated_at_prev, "%Y-%m-%d %H:%M:%S")
            except Exception:
                prev_dt = None
            need_periodic_refresh = False
            if prev_dt and SEO_REFRESH_DAYS>0:
                need_periodic_refresh = (now_utc() - prev_dt.replace(tzinfo=None)) >= timedelta(days=SEO_REFRESH_DAYS)
            if prev_cs == checksum and not need_periodic_refresh:
                lead_html   = ent.get("lead_html", lead_html)
                faq_html    = ent.get("faq_html", faq_html)
                reviews_html= ent.get("reviews_html", reviews_html)
                use_cache   = True

        full_html = compose_full_description_html(lead_html, raw_desc_html_full, specs_html, faq_html, reviews_html)
        placeholder = f"[[[HTML]]]{full_html}[[[/HTML]]]"

        if d is None:
            d = ET.SubElement(offer, "description"); d.text = placeholder; changed += 1
        else:
            prev = (d.text or "").strip()
            if prev != placeholder: d.text = placeholder; changed += 1

        if SEO_STICKY:
            ent = cache.get(cache_key, {})
            if not use_cache or not ent:
                ent = {"lead_html": lead_html, "faq_html": faq_html, "reviews_html": reviews_html, "checksum": checksum}
                ent["updated_at"] = now_utc().strftime("%Y-%m-%d %H:%M:%S")
                cache[cache_key] = ent

    if SEO_STICKY: save_seo_cache(SEO_CACHE_PATH, cache)

    # «Последнее обновление SEO-блока» — максимум по updated_at из кэша (UTC→Алматы)
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

# ========== CDATA PLACEHOLDER REPLACER ==========
def _replace_html_placeholders_with_cdata(xml_text: str) -> str:
    def repl(m):
        inner = m.group(1)
        inner = inner.replace("[[[HTML]]]", "").replace("[[[/HTML]]]", "")
        inner = _unescape(inner)
        inner = _html_escape_in_cdata_safe(inner)
        return f"<description><![CDATA[\n{inner}\n]]></description>"
    return re.sub(r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>", repl, xml_text, flags=re.S)

# ========== FEED_META ==========
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

# ========== MAIN ==========
def main()->None:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")
    offers_in_el=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None: err("XML: <offers> not found")
    src_offers=list(offers_in_el.findall("offer"))

    # Собираем выходной документ
    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")
    for o in src_offers: out_offers.append(deepcopy(o))

    # Категорийные фильтры (если заданы)
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        id2name,id2parent,parent2children=parse_categories_tree(shop_in)
        rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH)
        if ALSTYLE_CATEGORIES_MODE=="include" and not (rules_ids or rules_names):
            err("ALSTYLE_CATEGORIES_MODE=include, нет правил (docs/alstyle_categories.txt).", 2)
        keep_ids=set(rules_ids)
        if rules_names and id2name:
            for cid in id2name.keys():
                path=build_category_path_from_id(cid,id2name,id2parent)
                if category_matches_name(path, rules_names): keep_ids.add(cid)
        if keep_ids and parent2children: keep_ids=collect_descendants(keep_ids,parent2children)
        for off in list(out_offers.findall("offer")):
            cid=get_text(off,"categoryId"); hit=(cid in keep_ids) if cid else False
            drop=(ALSTYLE_CATEGORIES_MODE=="exclude" and hit) or (ALSTYLE_CATEGORIES_MODE=="include" and not hit)
            if drop: out_offers.remove(off)

    # CATEGORY ID → 0 первым
    if DROP_CATEGORY_ID_TAG:
        for off in out_offers.findall("offer"):
            for node in list(off.findall("categoryId"))+list(off.findall("CategoryId")): off.remove(node)

    # PRICE CAP
    flagged = flag_unrealistic_supplier_prices(out_shop); log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")

    # vendor/vendorCode/id
    ensure_vendor(out_shop); ensure_vendor_auto_fill(out_shop)
    ensure_vendorcode_with_article(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AS"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )
    sync_offer_id_with_vendorcode(out_shop)

    # цены
    reprice_offers(out_shop, PRICING_RULES)
    forced = enforce_forced_prices(out_shop); log(f"Forced price=100: {forced}")

    # параметры (чистим только param; описание не трогаем)
    remove_specific_params(out_shop)

    # SEO-описания: SEO лид + родное (полностью) + (авто-характеристики при необходимости) + FAQ/Отзывы
    seo_changed, seo_last_update_alm = inject_seo_descriptions(out_shop)
    log(f"SEO blocks touched: {seo_changed}")

    # доступность + валюта
    t_true, t_false, _, _ = normalize_available_field(out_shop)
    fix_currency_id(out_shop, default_code="KZT")

    # чистка, порядок, categoryId первым
    for off in out_offers.findall("offer"): purge_offer_tags_and_attrs_after(off)
    reorder_offer_children(out_shop); ensure_categoryid_zero_first(out_shop)

    # KEYWORDS
    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    # pretty indent
    try: ET.indent(out_root, space="  ")
    except Exception: pass

    # FEED_META
    built_alm = now_almaty()
    meta_pairs={
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL or "file",
        "offers_total": len(src_offers),
        "offers_written": len(list(out_offers.findall("offer"))),
        "available_true": str(t_true),
        "available_false": str(t_false),
        "built_utc": now_utc_str(),
        "built_alm": format_dt_almaty(built_alm),
        "next_build_alm": format_dt_almaty(next_build_time_almaty()),
        "seo_last_update_alm": seo_last_update_alm or format_dt_almaty(built_alm),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # сериализация
    xml_bytes=ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text=xml_bytes.decode(ENC, errors="replace")

    # приятные отступы
    xml_text=re.sub(r"(?s)(-->)\s*(<shop\b)", r"\1\n\2", xml_text, count=1)
    xml_text=re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_text)
    xml_text=re.sub(r"(\n[ \t]*){3,}", "\n\n", xml_text)

    # CDATA для description
    xml_text=_replace_html_placeholders_with_cdata(xml_text)

    # запись
    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    try:
        with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
            f.write(xml_text)
    except UnicodeEncodeError as e:
        warn(f"{ENC} can't encode some characters ({e}); writing with xmlcharrefreplace fallback")
        data_bytes = xml_text.encode(ENC, errors="xmlcharrefreplace")
        with open(OUT_FILE_YML, "wb") as f:
            f.write(data_bytes)

    # .nojekyll
    try:
        docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e: warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | script={SCRIPT_VERSION} | cache={SEO_CACHE_PATH}")

if __name__ == "__main__":
    try: main()
    except Exception as e: err(str(e))
