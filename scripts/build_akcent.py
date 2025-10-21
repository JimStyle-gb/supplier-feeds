# scripts/build_akcent.py
# -*- coding: utf-8 -*-
"""
Akcent → YML: единый шаблон как у AlStyle, но с сохранением всех <param>.

Обновление v1.0.0 (2025-10-21):
- НИЧЕГО не удаляем из <param> (сохраняем как есть).
- SEO-кэш sticky + рефреш 1-го числа месяца (Asia/Almaty).
- Автоподстановка бренда, SEO-лид сверху, ниже — «родное описание», затем FAQ и 3 отзыва.
- Плейсхолдеры изображений (бренд → категория → дефолт) с HEAD-проверкой.
- FEED_META c «Последнее обновление SEO-блока».
- Отдельный кэш: docs/akcent_cache/seo_cache.json
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

SCRIPT_VERSION = "akcent-2025-10-21.v1.0.0"

# ======================= ENV / CONST =======================
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "Akcent").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://example.com/akcent_feed.xml").strip()  # укажи свою ссылку/путь
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/akcent.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()
TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# PRICE CAP (оставил такой же механизм — можно отключить/настроить ENV)
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "100"))

# SEO sticky cache / РЕЖИМ РЕФРЕША (1-е число месяца)
DEFAULT_CACHE_PATH = "docs/akcent_cache/seo_cache.json"
SEO_CACHE_PATH     = os.getenv("SEO_CACHE_PATH", DEFAULT_CACHE_PATH)
SEO_STICKY         = os.getenv("SEO_STICKY", "1").lower() in {"1","true","yes","on"}
SEO_REFRESH_MODE   = os.getenv("SEO_REFRESH_MODE", "monthly_1").lower()  # "monthly_1" — каждое 1-е число
SEO_REFRESH_DAYS   = int(os.getenv("SEO_REFRESH_DAYS", "14"))  # не используется при monthly_1

# PLACEHOLDERS (фото)
PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.akcent.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.akcent.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.akcent.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# CATEGORY id → 0 первым
DROP_CATEGORY_ID_TAG = True

# Purge internals (служебный мусор — как у AlStyle)
DROP_STOCK_TAGS        = True
PURGE_TAGS_AFTER       = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER= ("type","article")
INTERNAL_PRICE_TAGS    = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                          "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice",
                          "max_price","maxPrice","oldprice")

# KEYWORDS
SATU_KEYWORDS          = os.getenv("SATU_KEYWORDS", "auto").lower()
SATU_KEYWORDS_MAXLEN   = int(os.getenv("SATU_KEYWORDS_MAXLEN", "1024"))
SATU_KEYWORDS_MAXWORDS = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "1000"))
SATU_KEYWORDS_GEO      = os.getenv("SATU_KEYWORDS_GEO", "on").lower() in {"on","1","true","yes"}
SATU_KEYWORDS_GEO_MAX  = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))
SATU_KEYWORDS_GEO_LAT  = os.getenv("SATU_KEYWORDS_GEO_LAT", "on").lower() in {"on","1","true","yes"}

# ======================= UTILS =======================
log  = lambda m: print(m, flush=True)
warn = lambda m: print(f"WARN: {m}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_utc() -> datetime: return datetime.now(timezone.utc)
def now_utc_str() -> str: return now_utc().strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty() -> datetime:
    try:   return datetime.now(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(time.time()+5*3600)
    except Exception: return datetime.utcfromtimestamp(time.time()+5*3600)
def format_dt_almaty(dt: datetime) -> str: return dt.strftime("%d:%m:%Y - %H:%M:%S")
def next_build_time_almaty() -> datetime:
    cur = now_almaty(); t = cur.replace(hour=1, minute=0, second=0, microsecond=0)
    return t + timedelta(days=1) if cur >= t else t

_COLON_CLASS_RE = re.compile("[:\uFF1A\uFE55\u2236\uFE30]")
canon_colons    = lambda s: _COLON_CLASS_RE.sub(":", s or "")
NOISE_RE        = re.compile(r"[\u200B-\u200F\u202A-\u202E\u2060\uFEFF\u00AD\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F\u0080-\u009F]")
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
            if len(data)<MIN_BYTES: raise RuntimeError(f"too small ({len(data)})")
            return data
        except Exception as e:
            last=e; back=RETRY_BACKOFF*i*(1+random.uniform(-0.2,0.2))
            warn(f"fetch {i}/{RETRIES} failed: {e}; sleep {back:.2f}s")
            if i<RETRIES: time.sleep(back)
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

# ======================= BRANDS =======================
def _norm_text(s: str) -> str:
    s=(s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

def _norm_key(s: str) -> str:
    if not s: return ""
    s=s.strip().lower().replace("ё","е"); s=re.sub(r"[-_/]+"," ",s)
    return re.sub(r"\s+"," ",s)

SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

COMMON_BRANDS = [
    "Canon","HP","Hewlett-Packard","Xerox","Brother","Epson","BenQ","ViewSonic","Optoma","Acer","Panasonic","Sony",
    "Konica Minolta","Ricoh","Kyocera","Sharp","OKI","Pantum","Lenovo","Dell","Apple","MSI","ASUS","Samsung"
]
BRAND_ALIASES = {
    "hewlett packard":"HP","konica":"Konica Minolta","konica-minolta":"Konica Minolta",
    "viewsonic proj":"ViewSonic","epson proj":"Epson","epson projector":"Epson","benq proj":"BenQ",
    "hp inc":"HP","nvprint":"NV Print","nv print":"NV Print","gg":"G&G","g&g":"G&G"
}

def normalize_brand(raw: str) -> str:
    k=_norm_key(raw)
    return "" if (not k) or (k in SUPPLIER_BLOCKLIST) else raw.strip()

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

def _find_brand_in_text(text: str) -> str:
    t=_norm_text(text)
    if not t: return ""
    for a,canon in BRAND_ALIASES.items():
        if re.search(rf"\b{re.escape(a)}\b", t): return canon
    for b in COMMON_BRANDS:
        if re.search(rf"\b{re.escape(_norm_text(b))}\b", t): return b
    m=re.match(r"^([A-Za-zА-Яа-яЁё]+)\b", text.strip())
    if m:
        cand=m.group(1)
        for b in COMMON_BRANDS:
            if _norm_text(b)==_norm_text(cand): return b
    return ""

def guess_vendor_for_offer(offer: ET.Element, brand_index: Dict[str,str]) -> str:
    name = get_text(offer, "name")
    desc = inner_html(offer.find("description"))
    first = re.split(r"\s+", name.strip())[0] if name else ""
    f_norm=_norm_key(first)
    if f_norm in brand_index: return brand_index[f_norm]
    b = _find_brand_in_text(name) or _find_brand_in_text(desc)
    if b: return b
    nrm=_norm_text(name)
    for br in COMMON_BRANDS:
        if re.search(rf"\b{re.escape(_norm_text(br))}\b", nrm): return br
    return ""

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, Dict[str,int]]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0,{}
    normalized=0; dropped: Dict[str,int]={}
    for offer in offers_el.findall("offer"):
        ven=offer.find("vendor"); txt=(ven.text or "").strip() if ven is not None and ven.text else ""
        if txt:
            canon=normalize_brand(txt)
            alias = BRAND_ALIASES.get(_norm_key(txt))
            final = alias or canon
            if any(m in txt.lower() for m in UNKNOWN_VENDOR_MARKERS) or (not final):
                if ven is not None: offer.remove(ven)
                key=_norm_key(txt); 
                if key: dropped[key]=dropped.get(key,0)+1
            elif final!=txt:
                ven.text=final; normalized+=1
        else:
            # пустой vendor — попробуем угадать
            guess = guess_vendor_for_offer(offer, build_brand_index(shop_el))
            if guess:
                if ven is None: ven = ET.SubElement(offer, "vendor")
                ven.text = guess
                normalized += 1
    return normalized,dropped

# ======================= PRICING (как у AlStyle) =======================
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

# ======================= AVAILABILITY / ORDER / ID =======================
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

def ensure_categoryid_zero_first(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        remove_all(offer,"categoryId","CategoryId")
        cid=ET.Element("categoryId"); cid.text=os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0,cid); touched+=1
    return touched

def ensure_vendorcode_with_article(shop_el:ET.Element,prefix:str,create_if_missing:bool=False)->Tuple[int,int,int,int]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
    total_prefixed=created=filled_from_art=fixed_bare=0
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

# ======================= COMPAT + SEO LEAD + FAQ/REVIEWS =======================
BRAND_WORDS = ["Canon","HP","Hewlett-Packard","Xerox","Brother","Epson","BenQ","ViewSonic","Optoma","Acer","Panasonic","Sony",
               "Konica Minolta","Ricoh","Kyocera","Sharp","OKI","Pantum"]
FAMILY_WORDS = ["PIXMA","imageRUNNER","iR","imageCLASS","imagePRESS","LBP","MF","i-SENSYS","LaserJet","DeskJet","OfficeJet",
                "PageWide","Color LaserJet","Neverstop","Smart Tank","Phaser","WorkCentre","VersaLink","AltaLink","DocuCentre",
                "DCP","HL","MFC","FAX","XP","WF","EcoTank","TASKalfa","ECOSYS","Aficio","SP","MP","IM","MX","BP"]

AS_INTERNAL_ART_RE = re.compile(r"^AS\d+|^AK\d+", re.I)
MODEL_RE = re.compile(r"\b([A-Z][A-Z0-9\-]{2,})\b", re.I)

def _split_joined_models(s: str) -> List[str]:
    for bw in BRAND_WORDS:
        s = re.sub(rf"({re.escape(bw)})\s*(?={re.escape(bw)})", r"\1\n", s)
    raw = re.split(r"[,\n;]+", s)
    out=[]
    for chunk in raw:
        c=chunk.strip()
        if not c: continue
        out.append(c)
    return out

def _looks_device_phrase(x: str) -> bool:
    x=x.strip()
    if len(x)<3: return False
    has_family = any(re.search(rf"\b{re.escape(f)}\b", x, re.I) for f in FAMILY_WORDS)
    has_brand  = any(re.search(rf"\b{re.escape(b)}\b", x, re.I) for b in BRAND_WORDS)
    has_model  = bool(MODEL_RE.search(x) and not AS_INTERNAL_ART_RE.search(x))
    return (has_family or has_brand) and has_model

def extract_full_compatibility(raw_desc: str) -> str:
    t = (raw_desc or "")
    text_lines = [ln.strip() for ln in t.replace("\r\n","\n").replace("\r","\n").split("\n")]
    triggers = re.compile(r"(совместим(?:ость)?|подходит(?:\s*для)?|для\s*модел[ей]|для\s*использования\s*в|compatible\s*with|for\s*use\s*in|для:)", re.I)
    stopheads = re.compile(r"^(описание|характеристики|faq|отзывы)\b", re.I)
    buf=[]; capturing=False
    for ln in text_lines:
        if not capturing and triggers.search(ln):
            after = ln.split(":",1)[1].strip() if ":" in ln else ""
            if after: buf.append(after)
            capturing=True; continue
        if capturing:
            if not ln or stopheads.match(ln): break
            buf.append(ln)
    compat = " ".join(buf).strip()
    if not compat:
        whole = re.sub(r"<[^>]+>"," ", raw_desc)
        whole = re.sub(r"\s{2,}"," ", whole).strip()
        parts = _split_joined_models(whole)
        found=[]
        for part in parts:
            subs=_split_joined_models(part)
            for sub in subs:
                s=sub.strip()
                if _looks_device_phrase(s):
                    found.append(s)
        clean=[]
        for x in found:
            x=re.sub(r"\s{2,}"," ", x).strip(" ,;.")
            if x and x not in clean:
                clean.append(x)
        compat=", ".join(clean[:50])
    compat = re.sub(r"\s{2,}", " ", compat).strip()
    return compat

def split_short_name(name: str) -> str:
    s=(name or "").strip()
    s=re.split(r"\s+[—-]\s+", s, maxsplit=1)[0]
    return s if len(s)<=80 else s[:77]+"..."

def detect_kind(name: str) -> str:
    n=(name or "").lower()
    if "картридж" in n or "тонер" in n or "тонер-" in n: return "cartridge"
    if ("ибп" in n) or ("ups" in n) or ("источник бесперебойного питания" in n): return "ups"
    if "проектор" in n or "projector" in n: return "projector"
    if "принтер" in n or "mfp" in n or "мфу" in n: return "mfp"
    return "other"

def _seo_title(name: str, vendor: str, kind: str, kv_hint: Dict[str,str], seed: int) -> str:
    short = split_short_name(name)
    variants = {
        "cartridge": ["Кратко о плюсах","Чем удобен","Что получаете с","Для каких устройств"],
        "projector": ["Ключевые преимущества","Чем хорош","Для каких задач","Кратко о плюсах"],
        "ups":       ["Ключевые преимущества","Чем удобен","Что вы получаете"],
        "mfp":       ["Кратко о плюсах","Основные сильные стороны","Для кого подойдёт"],
        "other":     ["Кратко о плюсах","Чем удобен","Ключевые преимущества"]
    }
    bucket = variants.get(kind, variants["other"])
    p = bucket[seed % len(bucket)]
    mark = vendor or ""
    return f"{short}: {p}" + (f" ({mark})" if mark else "")

def build_lead_html(offer: ET.Element, raw_desc_text_for_kv: str) -> Tuple[str, Dict[str,str]]:
    name=get_text(offer,"name").strip()
    vendor=get_text(offer,"vendor").strip()
    kind=detect_kind(name)
    s_id = offer.attrib.get("id") or get_text(offer,"vendorCode") or name
    seed = int(hashlib.md5((s_id or "").encode("utf-8")).hexdigest()[:8], 16)

    # простые эвристики под буллеты
    bullets: List[str] = []
    low = raw_desc_text_for_kv.lower()

    if kind=="projector":
        if re.search(r"\b(ansi\s*лм|люмен|lumen|lm)\b", low): bullets.append("✅ Яркость: заявленная производителем")
        if re.search(r"\b(fhd|1080p|4k|wxga|wuxga|svga|xga|uxga)\b", low): bullets.append("✅ Разрешение: соответствует классу модели")
        if re.search(r"\b(контраст|contrast)\b", low): bullets.append("✅ Контраст: комфортная картинка в офисе/доме")
        bullets.append("✅ Подходит для презентаций и обучения")
    elif kind=="cartridge":
        if re.search(r"\bресурс\b", low): bullets.append("✅ Ресурс: предсказуемая отдача страниц")
        if re.search(r"\bцвет\b|\bcyan|\bmagenta|\byellow|\bblack", low): bullets.append("✅ Цветность: соответствует спецификации")
        bullets.append("✅ Стабильная печать без лишних настроек")
    elif kind=="ups":
        if re.search(r"\b(ва|вт)\b", low): bullets.append("✅ Мощность: соответствует типовым офисным задачам")
        if re.search(r"\bavr\b|\bстабилиз", low): bullets.append("✅ AVR/стабилизация входного напряжения")
        bullets.append("✅ Базовая защита ПК, роутера и периферии")
    else:
        bullets.append("✅ Практичное решение для повседневных задач")

    # «Полная совместимость» только для картриджей
    compat = extract_full_compatibility(raw_desc_text_for_kv) if kind=="cartridge" else ""

    title = _seo_title(name, vendor, kind, {}, seed)

    html_parts=[]
    html_parts.append(f"<h3>{_html_escape_in_cdata_safe(title)}</h3>")
    p_line = {
        "cartridge": "Стабильная печать и предсказуемый ресурс.",
        "ups": "Базовая защита питания для домашней и офисной техники.",
        "projector": "Чёткая картинка и надёжная работа для переговорных и обучения.",
        "mfp": "Скорость, удобство и качество для офиса.",
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
    return lead_html, {"kind":kind, "title":title, "compat":compat}

def build_faq_html(kind: str) -> str:
    if kind=="cartridge":
        qa = [
            ("Подойдёт к моему устройству?", "Сверьте точный индекс модели и литеру в списке совместимости выше."),
            ("Нужна калибровка после замены?", "Обычно достаточно корректно установить картридж и распечатать тестовую страницу.")
        ]
    elif kind=="projector":
        qa = [
            ("Подойдёт для переговорной?", "Да, для типовой комнаты и презентаций/обучения."),
            ("Нужно затемнение?", "При высокой яркости будет достаточно приглушить свет для лучшего контраста.")
        ]
    elif kind=="ups":
        qa = [
            ("Подойдёт для ПК и роутера?", "Да, для техники своего класса мощности."),
            ("Шумит ли в работе?", "В обычном режиме — тихо; сигнализация срабатывает только при событиях.")
        ]
    else:
        qa = [
            ("Поддерживаются современные сценарии?", "Да, ориентирован на повседневную работу."),
            ("Можно расширять возможности?", "Да, подробности — в характеристиках модели.")
        ]
    parts=["<h3>FAQ</h3>"]
    for q,a in qa:
        parts.append(f"<p><strong>В:</strong> { _html_escape_in_cdata_safe(q) }<br><strong>О:</strong> { _html_escape_in_cdata_safe(a) }</p>")
    return "\n".join(parts)

def build_reviews_html(seed: int) -> str:
    NAMES_MALE  = ["Арман","Даурен","Санжар","Ерлан","Аслан","Руслан","Тимур","Данияр","Виктор","Евгений","Олег","Сергей","Нуржан","Бекзат","Азамат","Султан"]
    NAMES_FEMALE= ["Айгерим","Мария","Инна","Наталья","Жанна","Светлана","Ольга","Камилла","Диана","Гульнара"]
    CITIES = ["Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз","Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавл","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]
    def choose(arr: List[str], seed: int, offs: int=0) -> str:
        return arr[(seed + offs) % len(arr)] if arr else ""
    parts=["<h3>Отзывы (3)</h3>"]
    review_sets = [
        ("⭐⭐⭐⭐⭐","Картинка чёткая, для презентаций — то, что надо."),
        ("⭐⭐⭐⭐⭐","Установка заняла пару минут, проблем не было."),
        ("⭐⭐⭐⭐☆","Со своими задачами справляется отлично.")
    ]
    for i,(stars,comment) in enumerate(review_sets):
        name = choose(NAMES_MALE if i!=1 else NAMES_FEMALE, seed, i)
        city = choose(CITIES, seed, i+3)
        parts.append(
            f"<p>👤 <strong>{_html_escape_in_cdata_safe(name)}</strong>, { _html_escape_in_cdata_safe(city) } — {stars}<br>"
            f"«{ _html_escape_in_cdata_safe(comment) }»</p>"
        )
    return "\n".join(parts)

def has_specs_in_raw_desc(raw_desc_html: str) -> bool:
    if not raw_desc_html: return False
    s = raw_desc_html.lower()
    return ("<ul" in s and "<li" in s and "характерист" in s) or re.search(r"<h\d[^>]*>\s*характерист", s)

def compose_full_description_html(lead_html: str, raw_desc_html_full: str, specs_html: str, faq_html: str, reviews_html: str) -> str:
    # Порядок «как договорились»:
    # 1) SEO-лид (без FAQ/отзывов)
    # 2) Родное описание (как есть)
    # 3) FAQ
    # 4) Отзывы
    pieces=[]
    if lead_html: pieces.append(lead_html)
    if raw_desc_html_full: pieces.append(_html_escape_in_cdata_safe(raw_desc_html_full))
    if faq_html: pieces.append(faq_html)
    if reviews_html: pieces.append(reviews_html)
    return "\n".join(pieces)

# ======================= CDATA PLACEHOLDER REPLACER =======================
def _replace_html_placeholders_with_cdata(xml_text: str) -> str:
    def repl(m):
        inner = m.group(1)
        inner = inner.replace("[[[HTML]]]", "").replace("[[[/HTML]]]", "")
        inner = _unescape(inner)
        inner = _html_escape_in_cdata_safe(inner)
        return f"<description><![CDATA[\n{inner}\n]]></description>"
    return re.sub(r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>", repl, xml_text, flags=re.S)

# ======================= PLACEHOLDERS (фото) =======================
_url_head_cache: Dict[str,bool] = {}
def url_exists(url: str) -> bool:
    if not url: return False
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
        name   = get_text(offer,"name").strip()
        kind   = detect_kind(name)
        picked = ""
        if vendor:
            u_brand = _placeholder_url_brand(vendor)
            if url_exists(u_brand): picked = u_brand
        if not picked:
            u_cat = _placeholder_url_category(kind)
            if url_exists(u_cat): picked = u_cat
        if not picked:
            picked = PLACEHOLDER_DEFAULT_URL
        ET.SubElement(offer, "picture").text = picked
        added += 1
    return (added, skipped)

# ======================= KEYWORDS =======================
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
def tokenize_name(name: str) -> List[str]: return WORD_RE.findall(name or "")
def dedup_preserve_order(words: List[str]) -> List[str]:
    seen=set(); out=[]
    for w in words:
        key=_norm_text(str(w))
        if key and key not in seen: seen.add(key); out.append(str(w))
    return out

def translit_ru_to_lat(s: str) -> str:
    table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    out=s.lower().translate(table); out=re.sub(r"[^a-z0-9\- ]+","", out); return re.sub(r"\s+","-", out).strip("-")

def is_content_word(token: str) -> bool:
    t=_norm_text(token)
    STOP_RU={"для","и","или","на","в","из","от","по","с","к","до","при","через","над","под","о","об","у","без","про","как","это","той","тот","эта","эти",
             "тип","модель","набор","комплект","оригинальный","формат","новый","новинка"}
    STOP_EN={"for","and","or","with","of","the","a","an","to","in","on","by","at","from","new","original","type","model","set","kit","pack"}
    GENERIC={"изделие","товар","продукция","аксессуар","устройство","оборудование"}
    return bool(t) and (t not in STOP_RU) and (t not in STOP_EN) and (t not in GENERIC) and (any(ch.isdigit() for ch in t) or "-" in t or len(t)>=3)

def build_bigrams(words: List[str]) -> List[str]:
    out=[]; 
    for i in range(len(words)-1):
        a,b=words[i],words[i+1]
        if is_content_word(a) and is_content_word(b):
            out.append(f"{a} {b}")
    return out

def keywords_from_name_generic(name: str) -> List[str]:
    raw_tokens=tokenize_name(name or "")
    modelish=[t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content=[t for t in raw_tokens if is_content_word(t)]
    bigr=build_bigrams(content)
    norm=lambda tok: tok if re.search(r"[A-Z]{2,}", tok) else tok.capitalize()
    out=modelish[:8]+bigr[:8]+[norm(t) for t in content[:10]]
    return dedup_preserve_order(out)

def color_tokens(name: str) -> List[str]:
    out=[]; low=name.lower()
    mapping={"жёлт":"желтый","желт":"желтый","yellow":"yellow","черн":"черный","black":"black","син":"синий","blue":"blue",
             "красн":"красный","red":"red","зелен":"зеленый","green":"green","серебр":"серебряный","silver":"silver","циан":"cyan","магент":"magenta"}
    for k,val in mapping.items():
        if k in low: out.append(val)
    seen=set(); res=[]
    for t in out:
        if t not in seen: seen.add(t); res.append(t)
    return res

def extract_model_tokens(text_srcs: List[str]) -> List[str]:
    tokens=set()
    for src in text_srcs:
        if not src: continue
        for m in MODEL_RE.findall(src or ""):
            t=m.upper()
            if AS_INTERNAL_ART_RE.match(t) or not (re.search(r"[A-Z]", t) and re.search(r"\d", t)) or len(t)<5: continue
            tokens.add(t)
    return list(tokens)

def geo_tokens() -> List[str]:
    if not SATU_KEYWORDS_GEO: return []
    toks=["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
          "Оскемен","Семей","Костанаи","Кызылорда","Орал","Петропавл","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный",
          "Kazakhstan","Almaty","Astana","Shymkent","Karaganda","Aktobe","Pavlodar","Atyrau","Taraz",
          "Oskemen","Semey","Kostanay","Kyzylorda","Oral","Petropavl","Taldykorgan","Aktau","Temirtau","Ekibastuz","Kokshetau","Rudny"]
    seen=set(); out=[]
    for t in toks:
        if t not in seen: seen.add(t); out.append(t)
    return out[:max(0,SATU_KEYWORDS_GEO_MAX)]

def build_keywords_for_offer(offer: ET.Element) -> str:
    if SATU_KEYWORDS == "off": return ""
    name=get_text(offer,"name"); vendor=get_text(offer,"vendor").strip()
    desc_html=inner_html(offer.find("description"))
    parts=[vendor] if vendor else []
    parts += extract_model_tokens([name, desc_html]) + keywords_from_name_generic(name) + color_tokens(name)
    extra=[]
    for w in parts:
        if re.search(r"[А-Яа-яЁё]", str(w)):
            tr=translit_ru_to_lat(str(w))
            if tr and tr not in extra: extra.append(tr)
    parts+=extra+geo_tokens()
    # дубликаты и отсечки по длине
    seen=set(); uniq=[]
    for p in parts:
        key=p.lower()
        if key and key not in seen:
            seen.add(key); uniq.append(p)
        if len(", ".join(uniq))>=SATU_KEYWORDS_MAXLEN: break
    return ", ".join(uniq)

def ensure_keywords(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        kw=build_keywords_for_offer(offer)
        node=offer.find("keywords")
        if not kw:
            if node is not None:
                offer.remove(node)
            continue
        if node is None:
            node=ET.SubElement(offer, "keywords")
            node.text=kw; touched+=1
        else:
            if (node.text or "") != kw:
                node.text=kw; touched+=1
    return touched

# ======================= SEO CACHE / REFRESH =======================
def load_seo_cache(path: str) -> Dict[str, dict]:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f: return json.load(f)
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
    if mode in {"off","0","none"}: 
        return False
    if prev_dt_utc is None:
        return True
    if mode == "monthly_1":
        now_alm = now_almaty()
        try:
            prev_alm = prev_dt_utc.astimezone(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(prev_dt_utc.timestamp()+5*3600)
        except Exception:
            prev_alm = now_alm
        if now_alm.day != 1:
            return False
        return (now_alm.year, now_alm.month) != (prev_alm.year, prev_alm.month)
    # режим "days" на будущее
    return (now_utc() - prev_dt_utc) >= timedelta(days=max(1, SEO_REFRESH_DAYS))

def compute_seo_checksum(name: str, lead_inputs: Dict[str,str], raw_desc_text_for_kv: str) -> str:
    base = "|".join([name or "", lead_inputs.get("kind",""), lead_inputs.get("title",""),
                     lead_inputs.get("compat",""), hashlib.md5((raw_desc_text_for_kv or "").encode("utf-8")).hexdigest()])
    return hashlib.md5(base.encode("utf-8")).hexdigest()

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

        lead_html, inputs = build_lead_html(offer, raw_desc_text_for_kv)
        kind = inputs.get("kind","other")

        s_id = offer.attrib.get("id") or get_text(offer,"vendorCode") or name
        seed = int(hashlib.md5((s_id or "").encode("utf-8")).hexdigest()[:8], 16)
        faq_html = build_faq_html(kind)
        reviews_html = build_reviews_html(seed)

        # Спецификации не вставляем отдельно — «родное» описание не трогаем
        specs_html = ""

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

    # для FEED_META — последнее обновление SEO в Алматы
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

# ======================= FEED META =======================
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

# ======================= MAIN =======================
def flag_unrealistic_supplier_prices(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    flagged=0
    for offer in offers_el.findall("offer"):
        try:
            src_p = float((get_text(offer,"price") or "").replace(",",".")) if get_text(offer,"price") else None
        except Exception:
            src_p = None
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

def fix_currency_id(shop_el: ET.Element, default_code: str = "KZT") -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        remove_all(offer,"currencyId")
        ET.SubElement(offer,"currencyId").text=default_code; touched+=1
    return touched

def reorder_offer_children_and_category(shop_el: ET.Element):
    reorder_offer_children(shop_el)
    ensure_categoryid_zero_first(shop_el)

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
    for o in src_offers: out_offers.append(deepcopy(o))

    # categoryId → 0 первым (родные id удаляем, как и у AlStyle)
    if DROP_CATEGORY_ID_TAG:
        for off in out_offers.findall("offer"):
            for node in list(off.findall("categoryId"))+list(off.findall("CategoryId")): off.remove(node)

    flagged = flag_unrealistic_supplier_prices(out_shop); log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")

    normalized, dropped_unknown = ensure_vendor(out_shop)
    if normalized: log(f"Vendors normalized/auto-filled: {normalized}")

    # vendorCode/id
    ensure_vendorcode_with_article(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AK"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )
    sync_offer_id_with_vendorcode(out_shop)

    # Пересчёт цен
    reprice_offers(out_shop, PRICING_RULES)
    forced = enforce_forced_prices(out_shop); log(f"Forced price=100: {forced}")

    # ВАЖНО: параметры не трогаем (по ТЗ).
    # remove_specific_params(out_shop)  # НЕ вызываем

    # Плейсхолдеры фото
    ph_added,_ = ensure_placeholder_pictures(out_shop); log(f"Placeholders added: {ph_added}")

    # SEO-блоки
    seo_changed, seo_last_update_alm = inject_seo_descriptions(out_shop)
    log(f"SEO blocks touched: {seo_changed}")

    # Наличие и валюта
    t_true, t_false, _, _ = normalize_available_field(out_shop)
    fix_currency_id(out_shop, default_code="KZT")

    # Чистка служебных тегов и атрибутов
    for off in out_offers.findall("offer"):
        for t in PURGE_TAGS_AFTER:
            for node in list(off.findall(t)): off.remove(node)
        for a in PURGE_OFFER_ATTRS_AFTER:
            if a in off.attrib: off.attrib.pop(a,None)

    # Порядок блоков и categoryId в начало
    reorder_offer_children_and_category(out_shop)

    # Ключевые слова
    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    # Красивый вывод XML + CDATA
    try: ET.indent(out_root, space="  ")
    except Exception: pass

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

    xml_bytes=ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text=xml_bytes.decode(ENC, errors="replace")
    xml_text=re.sub(r"(?s)(-->)\s*(<shop\b)", r"\1\n\2", xml_text, count=1)
    xml_text=re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_text)
    xml_text=re.sub(r"(\n[ \t]*){3,}", "\n\n", xml_text)
    xml_text=_replace_html_placeholders_with_cdata(xml_text)

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

    try:
        docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e: warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | script={SCRIPT_VERSION} | cache={SEO_CACHE_PATH}")

if __name__ == "__main__":
    try: main()
    except Exception as e: err(str(e))
