# scripts/build_akcent.py
# -*- coding: utf-8 -*-

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

SCRIPT_VERSION = "akcent-2025-10-22.v1.6.2"

# ====== ENV / CONST (без изменений важной логики) ======
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "Akcent").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/akcent.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()
TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

AKCENT_KEYWORDS_PATH  = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt")
AKCENT_KEYWORDS_MODE  = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()  # include|exclude

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

PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.akcent.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.akcent.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.akcent.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

DROP_CATEGORY_ID_TAG = True
DROP_STOCK_TAGS      = True
PURGE_TAGS_AFTER     = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","article")
DESIRED_ORDER=["vendorCode","name","price","picture","vendor","currencyId","description"]

DEFAULT_CACHE_PATH="docs/akcent_cache/seo_cache.json"
SEO_CACHE_PATH=os.getenv("SEO_CACHE_PATH", DEFAULT_CACHE_PATH)
SEO_STICKY=os.getenv("SEO_STICKY","1").lower() in {"1","true","yes","on"}
SEO_REFRESH_MODE=os.getenv("SEO_REFRESH_MODE","monthly_1").lower()

SATU_KEYWORDS_MAXLEN=1024
SATU_KEYWORDS_GEO=True
SATU_KEYWORDS_GEO_MAX=20
SATU_KEYWORDS_GEO_LAT=True

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

# ====== Загрузка исходника (без изменений) ======
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

# ====== Фильтр имён (ранний) ======
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

# ====== Вендор / цена / доступность / фото (без изменений логики) ======
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

# ====== K/V и чистка «родного» (ЗДЕСЬ ФИКС) ======
KV_KEYS_MAP = {
    "вид":"Вид",
    "назначение":"Назначение",
    "цвет печати":"Цвет печати",
    "поддерживаемые модели принтеров":"Совместимость",
    "совместимость":"Совместимость",
    "ресурс":"Ресурс",
    "технология печати":"Технология печати",
    "тип":"Тип",
}

MORE_PHRASES_RE = re.compile(r"^\s*(подробнее|читать далее|узнать больше|все детали|подробности|смотреть на сайте производителя|скачать инструкцию)\s*\.?\s*$", re.I)
URL_RE = re.compile(r"https?://\S+", re.I)

def autocorrect_minor_typos_in_html(html: str) -> str:
    s = html or ""
    s = re.sub(r"\bвысококачетсвенную\b", "высококачественную", s, flags=re.I)
    s = re.sub(r"\bприентеров\b", "принтеров", s, flags=re.I)
    s = re.sub(r"\bSC-\s*P(\d{3,4}\b)", r"SC-P\1", s)
    s = re.sub(r"SureColor\s+SC-\s*P", "SureColor SC-P", s)
    s = re.sub(r"(\d)\s*мл\b", r"\1 мл", s, flags=re.I)
    s = re.sub(r"[ ]{2,}", " ", s)
    return s

def _html_to_text(desc_html: str) -> str:
    t = re.sub(r"<br\s*/?>", "\n", desc_html or "", flags=re.I)
    t = re.sub(r"</p\s*>", "\n", t, flags=re.I)
    t = re.sub(r"<a\b[^>]*>.*?</a>", "", t, flags=re.I|re.S)  # вырезаем анкоры
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace("\u00A0"," ")
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def _normalize_models_list(val: str) -> str:
    x = val or ""
    x = re.sub(r"\bSC-\s*P(\d{3,4}\b)", r"SC-P\1", x)
    x = re.sub(r"\s{2,}", " ", x)
    parts = re.split(r"[,\n;]+", x)
    parts = [p.strip(" .") for p in parts if p.strip()]
    seen=set(); out=[]
    for p in parts:
        if p not in seen:
            seen.add(p); out.append(p)
    return "; ".join(out)

def extract_kv_specs_and_clean_native(desc_html: str, product_name: str) -> Tuple[List[Tuple[str,str]], str, int, int]:
    """
    Возвращает:
      specs: список (Ключ, Значение),
      native_plain: очищённый «родной» текст БЕЗ K/V,
      removed_links_count,
      kv_moved_flag (0/1)
    """
    txt = _html_to_text(desc_html)
    # 1) убрать ссылки и «Подробнее»
    tmp=[]
    removed_links=0
    for l in [l.strip() for l in txt.split("\n")]:
        if not l:
            tmp.append("")
            continue
        if URL_RE.search(l) or MORE_PHRASES_RE.match(l):
            removed_links += 1
            continue
        tmp.append(l)
    # 2) если первая строка дублирует name — убрать
    def _norm(s:str)->str:
        s=(s or "").lower()
        s=re.sub(r"[\s\-–—:;,.]+"," ", s)
        return s.strip()
    if tmp and _norm(tmp[0]) and _norm(tmp[0])==_norm(product_name):
        tmp=tmp[1:]

    # 3) пройтись и ВЫРЕЗАТЬ K/V из «родного», складывая их в specs
    specs=[]
    out_lines=[]
    i=0
    removed_any_kv=0
    while i < len(tmp):
        key_raw = tmp[i].strip().strip(":").lower()
        norm_key = KV_KEYS_MAP.get(key_raw)
        if norm_key:
            # потребляем значения до следующего ключа
            i+=1
            vals=[]
            while i < len(tmp):
                nxt = tmp[i].strip()
                if KV_KEYS_MAP.get(nxt.strip(":").lower()):
                    break
                if nxt!="":
                    vals.append(nxt)
                i+=1
            value=" ".join(vals).strip()
            if value:
                if norm_key=="Совместимость":
                    value=_normalize_models_list(value)
                specs.append((norm_key, value))
                removed_any_kv=1
            # ничего не добавляем в out_lines для этого ключа и его значений — они удаляются из «родного»
        else:
            out_lines.append(tmp[i])
            i+=1

    native_plain="\n".join(out_lines)
    native_plain=re.sub(r"\n{3,}", "\n\n", native_plain).strip()
    return specs, native_plain, removed_links, (1 if removed_any_kv else 0)

def render_specs_html(specs: List[Tuple[str,str]]) -> str:
    if not specs: return ""
    out=["<h3>Характеристики</h3>","<ul>"]
    for k,v in specs:
        if k=="Совместимость":
            out.append(f'  <li><strong>{k}:</strong><br>{_html_escape_in_cdata_safe(v)}</li>')
        else:
            out.append(f'  <li><strong>{k}:</strong> { _html_escape_in_cdata_safe(v) }</li>')
    out.append("</ul>")
    return "\n".join(out)

# ====== SEO (лид/FAQ/Отзывы), кэш, совместимость (как было) ======
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
    return re.sub(
        r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>",
        repl,
        xml_text,
        flags=re.S
    )

def split_short_name(name: str) -> str:
    s=(name or "").strip(); s=re.split(r"\s+[—-]\s+", s, maxsplit=1)[0]
    return s if len(s)<=80 else s[:77]+"..."

def detect_kind(name: str) -> str:
    n=(name or "").lower()
    if "картридж" in n or "тонер" in n or "тонер-" in n: return "cartridge"
    if ("ибп" in n) or ("ups" in n) or ("источник бесперебойного питания" in n): return "ups"
    if "проектор" in n or "projector" in n: return "projector"
    if "принтер" in n or "mfp" in n or "мфу" in n: return "mfp"
    return "other"

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

    if kind=="cartridge":
        qa=[("Подойдёт к моему устройству?","Сверьте индекс модели в списке совместимости ниже."),
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

# ====== Кэш/ключевые слова/ID и FEED_META (без функциональных изменений) ======
# ... (оставлено как в твоей текущей версии v1.6.1 — см. предыдущий файл)
# ЧТОБЫ УМЕСТИТЬСЯ, я не повторяю здесь те блоки без изменений.
# Вставь сюда те же функции: load_seo_cache/save, should_periodic_refresh, compute_seo_checksum,
# inject_seo_descriptions (использует уже обновлённую extract_kv_specs_and_clean_native),
# keywords (build_keywords_for_offer/ensure_keywords), vendorCode/id sync,
# render_feed_meta_comment, main().
