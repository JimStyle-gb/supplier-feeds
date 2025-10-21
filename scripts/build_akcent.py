# scripts/build_akcent.py
# -*- coding: utf-8 -*-
#
# Akcent → SATU feed normalizer
# - РАННИЙ фильтр по docs/akcent_keywords.txt (include|exclude; префикс и /regex/)
# - Сохраняем ВСЕ <param> из источника (ничего из них не удаляем)
# - Автоподстановка бренда из name/description (без «фантазий», только чёткие совпадения)
# - vendorCode = AC + артикул; id синхронизирован с vendorCode
# - Пересчёт цен из «дилерских/закупочных» с наценкой и «хвостом 900»
# - available, currencyId=KZT
# - Плейсхолдеры фото (brand → category → default) с HEAD-проверкой
# - SEO-блок (лид + РОДНОЕ описание + FAQ + 3 отзыва), кэш, «освежение» 1-го числа (Asia/Almaty)
# - <keywords> — базовая генерация (не связана с akcent_keywords.txt)
# - FEED_META-комментарий с выровненной колонкой "|"
#
# Выход: docs/akcent.yml (по умолчанию), encoding=windows-1251

from __future__ import annotations
import os, sys, re, time, random, json, hashlib, urllib.parse, requests
from copy import deepcopy
from typing import Dict, List, Tuple, Optional
from xml.etree import ElementTree as ET
from datetime import datetime, timezone, timedelta
from html import unescape as _unescape
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

SCRIPT_VERSION = "akcent-2025-10-21.v1.3.1"

# ========= ENV / CONST =========
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "Akcent").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/akcent.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()
TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# РАННИЙ фильтр офферов
AKCENT_KEYWORDS_PATH  = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt")
AKCENT_KEYWORDS_MODE  = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()  # include|exclude

# Цены
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

# Фото-плейсхолдеры
PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.akcent.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.akcent.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.akcent.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# Прочее
DROP_CATEGORY_ID_TAG = True
DROP_STOCK_TAGS      = True
PURGE_TAGS_AFTER     = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","article")
DESIRED_ORDER=["vendorCode","name","price","picture","vendor","currencyId","description"]

# SEO-кэш (обновление 1-го числа)
DEFAULT_CACHE_PATH="docs/akcent_cache/seo_cache.json"
SEO_CACHE_PATH=os.getenv("SEO_CACHE_PATH", DEFAULT_CACHE_PATH)
SEO_STICKY=os.getenv("SEO_STICKY","1").lower() in {"1","true","yes","on"}
SEO_REFRESH_MODE=os.getenv("SEO_REFRESH_MODE","monthly_1").lower()

# <keywords>
SATU_KEYWORDS_MAXLEN=1024
SATU_KEYWORDS_GEO=True
SATU_KEYWORDS_GEO_MAX=20
SATU_KEYWORDS_GEO_LAT=True

# ========= UTILS =========
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
def _html_escape_in_cdata_safe(s: str) -> str: return (s or "").replace("]]>", "]]&gt;")
def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag); return (node.text or "").strip() if node is not None and node.text else ""
def remove_all(el: ET.Element, *tags: str) -> int:
    n=0
    for t in tags:
        for x in list(el.findall(t)): el.remove(x); n+=1
    return n

# ========= Загрузка исходника =========
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

# ========= РАННИЙ фильтр по имени =========
class KeySpec:
    __slots__=("raw","kind","norm","pattern")
    def __init__(self, raw: str, kind: str, norm: Optional[str], pattern: Optional[re.Pattern]):
        self.raw, self.kind, self.norm, self.pattern = raw, kind, norm, pattern

def _norm_name(s: str) -> str:
    s=(s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

def load_name_filter(path: str) -> List[KeySpec]:
    if not path or not os.path.exists(path): return []
    data=None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f: txt=f.read()
            data = txt.replace("\ufeff","").replace("\x00","")
            break
        except Exception: continue
    if data is None:
        with open(path,"r",encoding="utf-8",errors="ignore") as f: data=f.read().replace("\x00","")
    keys=[]
    for ln in data.splitlines():
        s=ln.strip()
        if not s or s.startswith("#"): continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try: keys.append(KeySpec(s,"regex",None,re.compile(s[1:-1],re.I)))
            except Exception: pass
        else:
            n=_norm_name(s)
            if n: keys.append(KeySpec(s,"prefix",n,None))
    return keys

def name_matches(name: str, keys: List[KeySpec]) -> bool:
    if not keys: return False
    norm = _norm_name(name)
    for ks in keys:
        if ks.kind=="prefix" and norm.startswith(ks.norm): return True
        if ks.kind=="regex"  and ks.pattern and ks.pattern.search(name or ""): return True
    return False

# ========= Бренд / vendor =========
def _norm_key(s: str) -> str:
    if not s: return ""
    s=s.strip().lower().replace("ё","е")
    s=re.sub(r"[-_/]+"," ",s); s=re.sub(r"\s+"," ",s); return s
SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")
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
    """Возвращает (normalized, filled, removed_unknown)."""
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

# ========= Цены =========
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

_force_tail_900 = lambda n: max(int(n)//1000,0)*1000+900 if int(n)>=0 else 900
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

def reprice_offers(shop_el:ET.Element,rules:List[PriceRule])->None:
    off_el=shop_el.find("offers")
    if off_el is None: return
    for offer in off_el.findall("offer"):
        if offer.attrib.get("_force_price","")=="100":
            strip_supplier_price_blocks(offer); _remove_all_price_nodes(offer); ET.SubElement(offer,"price").text=str(PRICE_CAP_VALUE); offer.attrib.pop("_force_price",None); continue
        dealer=pick_dealer_price(offer)
        if dealer is None or dealer<=100:
            strip_supplier_price_blocks(offer); continue
        newp=compute_retail(dealer,rules)
        if newp is None:
            strip_supplier_price_blocks(offer); continue
        _remove_all_price_nodes(offer); ET.SubElement(offer,"price").text=str(int(newp)); strip_supplier_price_blocks(offer)

def flag_unrealistic_supplier_prices(shop_el: ET.Element) -> int:
    off_el=shop_el.find("offers")
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

# ========= available / currency / reorder =========
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

def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int]:
    off_el=shop_el.find("offers")
    if off_el is None: return (0,0)
    t=f=0
    for offer in off_el.findall("offer"):
        b=derive_available(offer)
        remove_all(offer,"available")
        offer.attrib["available"]="true" if b else "false"
        if DROP_STOCK_TAGS: remove_all(offer,"quantity_in_stock","quantity","stock","Stock")
        t+=1 if b else 0; f+=0 if b else 1
    return t,f

def fix_currency_id(shop_el: ET.Element, default_code: str = "KZT") -> int:
    off_el=shop_el.find("offers")
    if off_el is None: return 0
    touched=0
    for offer in off_el.findall("offer"):
        remove_all(offer,"currencyId"); ET.SubElement(offer,"currencyId").text=default_code; touched+=1
    return touched

def ensure_categoryid_zero_first(shop_el: ET.Element) -> int:
    off_el=shop_el.find("offers")
    if off_el is None: return 0
    touched=0
    for offer in off_el.findall("offer"):
        remove_all(offer,"categoryId","CategoryId")
        cid=ET.Element("categoryId"); cid.text=os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0,cid); touched+=1
    return touched

def reorder_offer_children(shop_el: ET.Element) -> int:
    off_el=shop_el.find("offers")
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

# ========= Плейсхолдеры фото =========
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

def detect_kind(name: str) -> str:
    n=(name or "").lower()
    if "картридж" in n or "тонер" in n or "тонер-" in n: return "cartridge"
    if ("ибп" in n) or ("ups" in n) or ("источник бесперебойного питания" in n): return "ups"
    if "проектор" in n or "projector" in n: return "projector"
    if "принтер" in n or "mfp" in n or "мфу" in n: return "mfp"
    return "other"

def ensure_placeholder_pictures(shop_el: ET.Element) -> int:
    if not PLACEHOLDER_ENABLE: return 0
    off_el=shop_el.find("offers")
    if off_el is None: return 0
    added=0
    for offer in off_el.findall("offer"):
        pics=list(offer.findall("picture"))
        has_pic=any((p.text or "").strip() for p in pics)
        if has_pic: continue
        vendor=get_text(offer,"vendor").strip(); name=get_text(offer,"name").strip(); kind=detect_kind(name)
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

# ========= SEO (лид + FAQ + отзывы), кэш ежемесячный =========
AS_INTERNAL_ART_RE = re.compile(r"^AS\d+|^AK\d+|^AC\d+", re.I)
MODEL_RE = re.compile(r"\b([A-Z][A-Z0-9\-]{2,})\b", re.I)
BRAND_WORDS = ["Canon","HP","Hewlett-Packard","Xerox","Brother","Epson","BenQ","ViewSonic","Optoma","Acer","Panasonic","Sony",
               "Konica Minolta","Ricoh","Kyocera","Sharp","OKI","Pantum"]
FAMILY_WORDS = ["PIXMA","imageRUNNER","iR","imageCLASS","imagePRESS","LBP","MF","i-SENSYS","LaserJet","DeskJet","OfficeJet",
                "PageWide","Color LaserJet","Neverstop","Smart Tank","Phaser","WorkCentre","VersaLink","AltaLink","DocuCentre",
                "DCP","HL","MFC","FAX","XP","WF","EcoTank","TASKalfa","ECOSYS","Aficio","SP","MP","IM","MX","BP"]

def _replace_html_placeholders_with_cdata(xml_text: str) -> str:
    def repl(m):
        inner=m.group(1).replace("[[[HTML]]]", "").replace("[[[/HTML]]]", "")
        inner=_unescape(inner); inner=_html_escape_in_cdata_safe(inner)
        return f"<description><![CDATA[\n{inner}\n]]></description>"
    # ВАЖНО: передаём сам текст третьим аргументом!
    return re.sub(
        r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>",
        repl,
        xml_text,
        flags=re.S
    )

def split_short_name(name: str) -> str:
    s=(name or "").strip(); s=re.split(r"\s+[—-]\s+", s, maxsplit=1)[0]
    return s if len(s)<=80 else s[:77]+"..."

def _seo_title(name: str, vendor: str, kind: str, seed: int) -> str:
    short=split_short_name(name)
    variants={"cartridge":["Кратко о плюсах","Чем удобен","Что получаете с","Для каких устройств"],
              "projector":["Ключевые преимущества","Чем хорош","Для каких задач","Кратко о плюсах"],
              "ups":["Ключевые преимущества","Чем удобен","Что вы получаете"],
              "mfp":["Кратко о плюсах","Основные сильные стороны","Для кого подойдёт"],
              "other":["Кратко о плюсах","Чем удобен","Ключевые преимущества"]}
    p=variants.get(kind,variants["other"])[seed % len(variants.get(kind,variants["other"]))]
    return f"{short}: {p}" + (f" ({vendor})" if vendor else "")

def _split_joined_models(s: str) -> List[str]:
    for bw in BRAND_WORDS:
        s=re.sub(rf"({re.escape(bw)})\s*(?={re.escape(bw)})", r"\1\n", s)
    raw=re.split(r"[,\n;]+", s); return [c.strip() for c in raw if c.strip()]

def _looks_device_phrase(x: str) -> bool:
    if len(x.strip())<3: return False
    has_family=any(re.search(rf"\b{re.escape(f)}\b", x, re.I) for f in FAMILY_WORDS)
    has_brand =any(re.search(rf"\b{re.escape(b)}\b", x, re.I) for b in BRAND_WORDS)
    has_model=bool(MODEL_RE.search(x) and not AS_INTERNAL_ART_RE.search(x))
    return (has_family or has_brand) and has_model

def extract_full_compatibility(raw_desc: str) -> str:
    t=(raw_desc or "")
    text=re.sub(r"<br\s*/?>","\n",t,flags=re.I); text=re.sub(r"<[^>]+>"," ",text)
    parts=_split_joined_models(text); found=[]
    for sub in parts:
        s=sub.strip()
        if _looks_device_phrase(s): found.append(s)
    clean=[]
    for x in found:
        x=re.sub(r"\s{2,}"," ",x).strip(" ,;.")
        if x and x not in clean: clean.append(x)
    return ", ".join(clean[:50])

def build_lead_faq_reviews(offer: ET.Element) -> Tuple[str,str,str,str]:
    name=get_text(offer,"name"); vendor=get_text(offer,"vendor").strip()
    desc_html=inner_html(offer.find("description"))
    raw_text=re.sub(r"<[^>]+>"," ", re.sub(r"<br\s*/?>","\n",desc_html or "", flags=re.I))
    kind=detect_kind(name)
    s_id=offer.attrib.get("id") or get_text(offer,"vendorCode") or name
    seed=int(hashlib.md5((s_id or "").encode("utf-8")).hexdigest()[:8],16)

    title=_seo_title(name,vendor,kind,seed)
    bullets=[]
    low=raw_text.lower()
    if kind=="projector":
        if re.search(r"\b(ansi\s*лм|люмен|lumen|lm)\b",low): bullets.append("✅ Яркость: заявленная производителем")
        if re.search(r"\b(fhd|1080p|4k|wxga|wuxga|svga|xga|uxga)\b",low): bullets.append("✅ Разрешение: соответствует классу модели")
        if re.search(r"\b(контраст|contrast)\b",low): bullets.append("✅ Контраст: комфортная картинка в офисе/доме")
        bullets.append("✅ Подходит для презентаций и обучения")
    elif kind=="cartridge":
        if re.search(r"\bресурс\b",low): bullets.append("✅ Ресурс: предсказуемая отдача страниц")
        if re.search(r"\bцвет\b|\bcyan|\bmagenta|\byellow|\bblack",low): bullets.append("✅ Цветность: соответствует спецификации")
        bullets.append("✅ Стабильная печать без лишних настроек")
    elif kind=="ups":
        if re.search(r"\b(ва|вт)\b",low): bullets.append("✅ Мощность: соответствует типовым офисным задачам")
        if re.search(r"\bavr\b|\bстабилиз",low): bullets.append("✅ AVR/стабилизация входного напряжения")
        bullets.append("✅ Базовая защита ПК, роутера и периферии")
    else:
        bullets.append("✅ Практичное решение для повседневных задач")

    compat = extract_full_compatibility(desc_html) if kind=="cartridge" else ""
    lead=[]
    lead.append(f"<h3>{_html_escape_in_cdata_safe(title)}</h3>")
    p_line={"cartridge":"Стабильная печать и предсказуемый ресурс.",
            "ups":"Базовая защита питания для домашней и офисной техники.",
            "projector":"Чёткая картинка и надёжная работа для переговорных и обучения.",
            "mfp":"Скорость, удобство и качество для офиса.",
            "other":"Практичное решение для ежедневной работы."}.get(kind,"Практичное решение для ежедневной работы.")
    lead.append(f"<p>{_html_escape_in_cdata_safe(p_line)}</p>")
    if bullets:
        lead.append("<ul>")
        for b in bullets[:5]: lead.append(f"  <li>{_html_escape_in_cdata_safe(b)}</li>")
        lead.append("</ul>")
    if compat:
        compat_html=_html_escape_in_cdata_safe(compat).replace(";", "; ").replace(",", ", ")
        lead.append(f"<p><strong>Полная совместимость:</strong><br>{compat_html}</p>")
    lead_html="\n".join(lead)

    # FAQ
    if kind=="cartridge":
        qa=[("Подойдёт к моему устройству?","Сверьте индекс модели в списке совместимости выше."),
            ("Нужна калибровка после замены?","Обычно достаточно корректно установить и распечатать тестовую страницу.")]
    elif kind=="projector":
        qa=[("Подойдёт для переговорной?","Да, для типовой комнаты и презентаций/обучения."),
            ("Нужно затемнение?","При высокой яркости лучше приглушить свет для контраста.")]
    elif kind=="ups":
        qa=[("Подойдёт для ПК и роутера?","Да, для техники своего класса мощности."),
            ("Шумит ли в работе?","В обычном режиме — тихо; сигнализация только при событиях.")]
    else:
        qa=[("Поддерживаются современные сценарии?","Да, ориентирован на повседневную работу."),
            ("Можно расширять возможности?","Да, подробности — в характеристиках модели.")]
    faq=["<h3>FAQ</h3>"]+[f"<p><strong>В:</strong> { _html_escape_in_cdata_safe(q) }<br><strong>О:</strong> { _html_escape_in_cdata_safe(a) }</p>" for q,a in qa]
    faq_html="\n".join(faq)

    # Отзывы (3)
    NAMES_M=["Арман","Даурен","Санжар","Ерлан","Аслан","Руслан","Тимур","Данияр","Виктор","Евгений","Олег","Сергей","Нуржан","Бекзат","Азамат","Султан"]
    NAMES_F=["Айгерим","Мария","Инна","Наталья","Жанна","Светлана","Ольга","Камилла","Диана","Гульнара"]
    CITIES=["Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз","Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавл","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]
    pick=lambda arr,offs=0: arr[(seed+offs)%len(arr)]
    reviews=["<h3>Отзывы (3)</h3>"]
    rv=[("⭐⭐⭐⭐⭐","Картинка чёткая, для презентаций — то, что надо."),
        ("⭐⭐⭐⭐⭐","Установка заняла пару минут, проблем не было."),
        ("⭐⭐⭐⭐☆","Со своими задачами справляется отлично.")]
    for i,(stars,comment) in enumerate(rv):
        name=(pick(NAMES_M,i) if i!=1 else pick(NAMES_F,i))
        city=pick(CITIES,i+3)
        reviews.append(f"<p>👤 <strong>{_html_escape_in_cdata_safe(name)}</strong>, { _html_escape_in_cdata_safe(city) } — {stars}<br>«{ _html_escape_in_cdata_safe(comment) }»</p>")
    reviews_html="\n".join(reviews)
    return lead_html, faq_html, reviews_html, kind

def should_periodic_refresh(prev_dt_utc: Optional[datetime]) -> bool:
    if SEO_REFRESH_MODE in {"off","0","none"}: return False
    if prev_dt_utc is None: return True
    if SEO_REFRESH_MODE=="monthly_1":
        now_alm=now_almaty()
        try: prev_alm=prev_dt_utc.astimezone(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(prev_dt_utc.timestamp()+5*3600)
        except Exception: prev_alm=now_alm
        if now_alm.day!=1: return False
        return (now_alm.year,now_alm.month)!=(prev_alm.year,prev_alm.month)
    return False

def load_seo_cache(path: str) -> Dict[str, dict]:
    if os.path.exists(path):
        try:
            with open(path,"r",encoding="utf-8") as f: return json.load(f)
        except Exception: return {}
    return {}
def save_seo_cache(path: str, data: Dict[str, dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp=path+".tmp"
    with open(tmp,"w",encoding="utf-8") as f: json.dump(data,f,ensure_ascii=False,indent=2)
    os.replace(tmp,path)

def compute_seo_checksum(name: str, kind: str, desc_html: str) -> str:
    base="|".join([name or "", kind or "", hashlib.md5((desc_html or "").encode("utf-8")).hexdigest()])
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def inject_seo_descriptions(shop_el: ET.Element) -> Tuple[int,str]:
    off_el=shop_el.find("offers")
    if off_el is None: return 0,""
    cache=load_seo_cache(SEO_CACHE_PATH) if SEO_STICKY else {}
    changed=0
    for offer in off_el.findall("offer"):
        d=offer.find("description")
        raw = inner_html(d)
        lead_html, faq_html, reviews_html, kind = build_lead_faq_reviews(offer)
        checksum=compute_seo_checksum(get_text(offer,"name"), kind, raw)
        cache_key = offer.attrib.get("id") or (get_text(offer,"vendorCode") or "").strip() or hashlib.md5((get_text(offer,"name") or "").encode("utf-8")).hexdigest()

        use_cache=False
        if SEO_STICKY and cache.get(cache_key):
            ent=cache[cache_key]; prev_cs=ent.get("checksum",""); updated_at_prev=ent.get("updated_at","")
            try: prev_dt_utc=datetime.strptime(updated_at_prev,"%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception: prev_dt_utc=None
            if prev_cs==checksum and not should_periodic_refresh(prev_dt_utc):
                lead_html=ent.get("lead_html",lead_html); faq_html=ent.get("faq_html",faq_html); reviews_html=ent.get("reviews_html",reviews_html)
                use_cache=True

        # Структура: SEO-лид → РОДНОЕ описание → FAQ → отзывы
        full_html = "\n".join([lead_html, _html_escape_in_cdata_safe(raw), faq_html, reviews_html]).strip()
        placeholder=f"[[[HTML]]]{full_html}[[[/HTML]]]"
        if d is None:
            d=ET.SubElement(offer,"description"); d.text=placeholder; changed+=1
        else:
            if (d.text or "").strip()!=placeholder:
                d.text=placeholder; changed+=1

        if SEO_STICKY:
            ent=cache.get(cache_key,{})
            if not use_cache or not ent:
                ent={"lead_html":lead_html,"faq_html":faq_html,"reviews_html":reviews_html,"checksum":checksum}
            ent["updated_at"]=now_utc().strftime("%Y-%m-%d %H:%M:%S")
            cache[cache_key]=ent
    if SEO_STICKY: save_seo_cache(SEO_CACHE_PATH, cache)

    last_alm=None
    if cache:
        for ent in cache.values():
            ts=ent.get("updated_at")
            if not ts: continue
            try:
                utc_dt=datetime.strptime(ts,"%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                alm=utc_dt.astimezone(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(utc_dt.timestamp()+5*3600)
                if (last_alm is None) or (alm>last_alm): last_alm=alm
            except Exception:
                continue
    if not last_alm: last_alm=now_almaty()
    return changed, format_dt_almaty(last_alm)

# ========= KEYWORDS =========
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
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

AS_INTERNAL_ART_RE2 = AS_INTERNAL_ART_RE
def extract_models(text_srcs: List[str]) -> List[str]:
    tokens=set()
    for src in text_srcs:
        if not src: continue
        for m in MODEL_RE.findall(src or ""):
            t=m.upper()
            if AS_INTERNAL_ART_RE2.match(t) or not (re.search(r"[A-Z]", t) and re.search(r"\d", t)) or len(t)<5: continue
            tokens.add(t)
    return list(tokens)

def is_content_word(t: str) -> bool:
    x=t.lower()
    STOP_RU={"для","и","или","на","в","из","от","по","с","к","до","при","через","над","под","о","об","у","без","про","как","это","тип","модель","комплект","оригинальный","формат","новый","новинка"}
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
    # биграммы
    bigr=[]
    for i in range(len(content)-1):
        a,b=content[i],content[i+1]
        if is_content_word(a) and is_content_word(b): bigr.append(f"{a} {b}")
    base += extract_models([name, desc_html]) + modelish[:8] + bigr[:8] + [t.capitalize() if not re.search(r"[A-Z]{2,}",t) else t for t in content[:10]]
    # цвета
    colors=[]; low=name.lower()
    mapping={"жёлт":"желтый","желт":"желтый","yellow":"yellow","черн":"черный","black":"black","син":"синий","blue":"blue",
             "красн":"красный","red":"red","зелен":"зеленый","green":"green","серебр":"серебряный","silver":"silver","циан":"cyan","магент":"magenta"}
    for k,val in mapping.items():
        if k in low and val not in colors: colors.append(val)
    base += colors
    # транслит + гео
    extra=[]
    for w in base:
        if re.search(r"[А-Яа-яЁё]", str(w)):
            tr=translit_ru_to_lat(str(w))
            if tr and tr not in extra: extra.append(tr)
    base += extra
    if SATU_KEYWORDS_GEO:
        geo=["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
             "Оскемен","Семей","Костанаи","Кызылорда","Орал","Петропавл","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]
        if SATU_KEYWORDS_GEO_LAT:
            geo += ["Kazakhstan","Almaty","Astana","Shymkent","Karaganda","Aktobe","Pavlodar","Atyrau","Taraz","Oskemen","Semey","Kostanay","Kyzylorda","Oral","Petropavl","Taldykorgan","Aktau","Temirtau","Ekibastuz","Kokshetau","Rudny"]
        base += geo[:SATU_KEYWORDS_GEO_MAX]
    # сборка
    parts=dedup([p for p in base if p])
    res=[]; total=0
    for p in parts:
        add=((", " if res else "")+p)
        if total+len(add)>SATU_KEYWORDS_MAXLEN: break
        res.append(p); total+=len(add)
    return ", ".join(res)

def ensure_keywords(shop_el: ET.Element) -> int:
    off_el=shop_el.find("offers")
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

# ========= vendorCode / id =========
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

def ensure_vendorcode_with_article(shop_el:ET.Element,prefix:str,create_if_missing:bool=False)->None:
    off_el=shop_el.find("offers")
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

def sync_offer_id_with_vendorcode(shop_el: ET.Element) -> None:
    off_el=shop_el.find("offers")
    if off_el is None: return
    for offer in off_el.findall("offer"):
        vc=offer.find("vendorCode")
        if vc is None or not (vc.text or "").strip(): continue
        new_id=(vc.text or "").strip()
        if offer.attrib.get("id")!=new_id: offer.attrib["id"]=new_id

# ========= FEED_META =========
def render_feed_meta_comment(pairs:Dict[str,str]) -> str:
    rows=[
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
    return "\n".join(["FEED_META"]+[f"{k.ljust(key_w)} | {v}" for k,v in rows])

# ========= MAIN =========
def main()->None:
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

    # 1) Копируем офферы; categoryId удаляю прямо сейчас (потом выставлю 0 в начало)
    for o in src_offers:
        mod=deepcopy(o)
        if DROP_CATEGORY_ID_TAG:
            for node in list(mod.findall("categoryId"))+list(mod.findall("CategoryId")): mod.remove(node)
        out_offers.append(mod)

    # 2) РАННИЙ фильтр
    keys=load_name_filter(AKCENT_KEYWORDS_PATH)
    if AKCENT_KEYWORDS_MODE=="include" and len(keys)==0:
        err("AKCENT_KEYWORDS_MODE=include, но файл docs/akcent_keywords.txt пуст или не найден.", 2)
    filtered_out=0
    if (AKCENT_KEYWORDS_MODE in {"include","exclude"}) and len(keys)>0:
        before=len(list(out_offers.findall("offer")))
        hits=0
        for off in list(out_offers.findall("offer")):
            nm=get_text(off,"name")
            hit=name_matches(nm,keys)
            if hit: hits+=1
            drop_this=(AKCENT_KEYWORDS_MODE=="exclude" and hit) or (AKCENT_KEYWORDS_MODE=="include" and not hit)
            if drop_this:
                out_offers.remove(off); filtered_out+=1
        kept=before-filtered_out
        log(f"Filter mode: {AKCENT_KEYWORDS_MODE} | Keywords loaded: {len(keys)} | Offers before: {before} | Matched: {hits} | Removed: {filtered_out} | Kept: {kept}")
    else:
        log("Filter disabled: no keys or mode not in {include,exclude}")

    # 3) Флаги цен-поставщика (слишком большие) → принудительная цена
    flagged = flag_unrealistic_supplier_prices(out_shop)
    log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")

    # 4) Вендоры
    v_norm, v_filled, v_removed = ensure_vendor(out_shop)
    log(f"Vendors auto-filled: {v_filled}")

    # 5) vendorCode/id
    ensure_vendorcode_with_article(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AC"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )
    sync_offer_id_with_vendorcode(out_shop)

    # 6) Цены
    reprice_offers(out_shop, PRICING_RULES)

    # 7) Фото-плейсхолдеры
    ph_added=ensure_placeholder_pictures(out_shop)
    log(f"Placeholders added: {ph_added}")

    # 8) SEO-блок (липкий + обновление каждое 1-е число)
    seo_changed, seo_last = inject_seo_descriptions(out_shop)
    log(f"SEO blocks touched: {seo_changed}")

    # 9) Наличие/валюта
    t_true, t_false = normalize_available_field(out_shop)
    fix_currency_id(out_shop, default_code="KZT")

    # 10) Чистка служебных (НЕ <param>!)
    for off in out_offers.findall("offer"):
        for t in PURGE_TAGS_AFTER:
            for node in list(off.findall(t)): off.remove(node)
        for a in PURGE_OFFER_ATTRS_AFTER:
            if a in off.attrib: off.attrib.pop(a,None)

    # 11) Порядок + categoryId=0 в начало
    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)

    # 12) <keywords>
    kw_touched=ensure_keywords(out_shop)
    log(f"Keywords updated: {kw_touched}")

    # FEED_META
    built_alm=now_almaty()
    meta_pairs={
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "offers_total": len(src_offers),
        "offers_written": len(list(out_offers.findall("offer"))),
        "available_true": str(t_true),
        "available_false": str(t_false),
        "built_alm": format_dt_almaty(built_alm),
        "next_build_alm": format_dt_almaty(next_build_time_almaty()),
        "seo_last_update_alm": seo_last or format_dt_almaty(built_alm),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # сериализация
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

if __name__ == "__main__":
    try: main()
    except Exception as e: err(str(e))
