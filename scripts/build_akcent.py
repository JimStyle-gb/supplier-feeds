#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, re, time, random, urllib.parse, requests, html
from copy import deepcopy
from typing import Dict, List, Tuple, Optional
from xml.etree import ElementTree as ET
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

SCRIPT_VERSION = "akcent-2025-10-24.v8.0.1"

# === ENV / CONST ===
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "Akcent").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/akcent.yml").strip()

# ВАЖНО: у тебя CP1251 — оставляю по умолчанию, но теперь делаем текст безопасным для этой кодировки
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()

TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

AKCENT_KEYWORDS_PATH  = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt")
AKCENT_KEYWORDS_MODE  = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()

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

SATU_KEYWORDS_MAXLEN=1024
SATU_KEYWORDS_GEO=True
SATU_KEYWORDS_GEO_MAX=20

PARAMS_MAX_VALUE_LEN = int(os.getenv("PARAMS_MAX_VALUE_LEN", "800"))

# === ENCODING SAFETY: маппинг «не cp1251» → безопасные аналоги ===
ENC_SAFE_MAP = {
    "\u2026": "...",  # …
    "\u2014": "-",    # —  em-dash
    "\u2013": "-",    # –  en-dash
    "\u2212": "-",    # −  minus
    "\u2122": "",     # ™
    "\u2265": ">=",   # ≥
    "\u2264": "<=",   # ≤
    "\u2009": " ",    # thin space
    "\u200A": " ",    # hair space
    "\u200B": "",     # zero width
    "\u2018": "'",    # ‘
    "\u2019": "'",    # ’
    "\u201C": '"',    # “
    "\u201D": '"',    # ”
    "\u00A0": " ",    # NBSP
}
def make_encoding_safe(s: str, enc: str) -> str:
    if not s: return s
    for k,v in ENC_SAFE_MAP.items():
        s = s.replace(k, v)
    try:
        s.encode(enc)
        return s
    except Exception:
        # финальная защита: выкинуть то, что всё ещё не кодируется
        return s.encode(enc, errors="ignore").decode(enc, errors="ignore")

# === Утилиты ===
def log(m: str): print(m, flush=True)
def warn(m: str): print(f"WARN: {m}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)
def now_almaty() -> datetime:
    try: return datetime.now(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(time.time()+5*3600)
    except Exception: return datetime.utcfromtimestamp(time.time()+5*3600)
def next_build_time_almaty() -> datetime:
    cur=now_almaty(); t=cur.replace(hour=1,minute=0,second=0,microsecond=0); return t+timedelta(days=1) if cur>=t else t
def format_dt_almaty(dt: datetime) -> str: return dt.strftime("%d:%m:%Y - %H:%M:%S")
def inner_html(el: ET.Element) -> str:
    if el is None: return ""
    parts=[]; 
    if el.text: parts.append(el.text)
    for ch in el:
        parts.append(ET.tostring(ch, encoding="unicode"))
        if ch.tail: parts.append(ch.tail)
    return "".join(parts).strip()
def strip_html_tags(s: str) -> str: return re.sub(r"<[^>]+>", " ", s or "", flags=re.S)
def get_text(el: ET.Element, tag: str) -> str:
    node=el.find(tag); return (node.text or "").strip() if node is not None and node.text else ""
def remove_all(el: ET.Element, *tags: str) -> int:
    n=0
    for t in tags:
        for x in list(el.findall(t)): el.remove(x); n+=1
    return n
def _html_escape_in_cdata_safe(s: str) -> str: return (s or "").replace("]]>", "]]&gt;")

# === Загрузка источника ===
def load_source_bytes(src: str) -> bytes:
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path,"rb") as f: data=f.read()
        if len(data)<1500: raise RuntimeError("file too small")
        return data
    sess=requests.Session(); headers={"User-Agent":"supplier-feed-bot/1.0"}
    last=None
    for i in range(1,RETRIES+1):
        try:
            r=sess.get(src, headers=headers, timeout=TIMEOUT_S)
            if r.status_code!=200: raise RuntimeError(f"HTTP {r.status_code}")
            if len(r.content)<1500: raise RuntimeError("too small")
            return r.content
        except Exception as e:
            last=e; time.sleep(RETRY_BACKOFF*i)
    raise RuntimeError(f"fetch failed: {last}")

# === Фильтр по <name> ===
class KeySpec:
    __slots__=("raw","kind","norm","pattern")
    def __init__(self, raw, kind, norm, pattern): self.raw, self.kind, self.norm, self.pattern = raw, kind, norm, pattern
def _norm_name(s: str) -> str:
    s=(s or "").replace("\u00A0"," ").lower().replace("ё","е"); return re.sub(r"\s+"," ",s).strip()
def load_name_filter(path: str)->List[KeySpec]:
    if not path or not os.path.exists(path): return []
    for enc in ("utf-8-sig","utf-8","windows-1251","utf-16","utf-16-le","utf-16-be"):
        try: txt=open(path,"r",encoding=enc).read(); break
        except Exception: txt=None
    if txt is None: txt=open(path,"r",encoding="utf-8",errors="ignore").read()
    keys=[]
    for ln in txt.splitlines():
        s=ln.strip()
        if not s or s.startswith("#"): continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try: keys.append(KeySpec(s,"regex",None,re.compile(s[1:-1],re.I)))
            except Exception: pass
        else:
            keys.append(KeySpec(s,"prefix",_norm_name(s),None))
    return keys
def name_matches(name: str, keys: List[KeySpec]) -> bool:
    if not keys: return False
    norm=_norm_name(name)
    for ks in keys:
        if ks.kind=="prefix" and norm.startswith(ks.norm): return True
        if ks.kind=="regex" and ks.pattern and ks.pattern.search(name or ""): return True
    return False

# === Бренды, цены, доступность, порядок (как у нас было)… ===
SUPPLIER_BLOCKLIST={x.strip().lower() for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
BRAND_ALIASES={"hewlett packard":"HP","konica-minolta":"Konica Minolta","konica":"Konica Minolta","g&g":"G&G","nv print":"NV Print","nvprint":"NV Print"}
COMMON_BRANDS=["Canon","HP","Xerox","Brother","Epson","BenQ","ViewSonic","Optoma","Acer","Panasonic","Sony","Konica Minolta","Ricoh","Kyocera","Sharp","OKI","Pantum","Lenovo","Dell","ASUS","Samsung","Apple","MSI"]
def _norm_key(s: str) -> str: return re.sub(r"\s+"," ",(s or "").strip().lower().replace("ё","е"))
def normalize_brand(raw: str) -> str:
    k=_norm_key(raw)
    if not k or k in SUPPLIER_BLOCKLIST: return ""
    return BRAND_ALIASES.get(k, raw.strip())
def build_brand_index(shop_el: ET.Element)->Dict[str,str]:
    idx={}; off=shop_el.find("offers") or ET.Element("offers")
    for of in off.findall("offer"):
        v=of.find("vendor")
        if v is not None and (v.text or "").strip(): idx[_norm_key(v.text)]=v.text.strip()
    return idx
def _find_brand_in_text(text: str)->str:
    t=(text or "").lower()
    for a,canon in BRAND_ALIASES.items():
        if a in t: return canon
    for b in COMMON_BRANDS:
        if b.lower() in t: return b
    return ""
def guess_vendor_for_offer(offer: ET.Element, idx: Dict[str,str])->str:
    name=get_text(offer,"name"); desc=inner_html(offer.find("description"))
    first=(name.strip().split()[0] if name else "")
    if _norm_key(first) in idx: return idx[_norm_key(first)]
    return _find_brand_in_text(name) or _find_brand_in_text(desc)
def ensure_vendor(shop_el: ET.Element)->Tuple[int,int,int]:
    off=shop_el.find("offers") or ET.Element("offers"); idx=build_brand_index(shop_el)
    normalized=filled=removed=0
    for o in off.findall("offer"):
        v=o.find("vendor")
        cur=(v.text or "").strip() if v is not None and v.text else ""
        if cur:
            fin=normalize_brand(cur)
            if not fin: o.remove(v); removed+=1
            elif fin!=cur: v.text=fin; normalized+=1
        else:
            g=guess_vendor_for_offer(o, idx)
            if g:
                if v is None: v=ET.SubElement(o,"vendor")
                v.text=g; filled+=1
    return normalized,filled,removed

def parse_price_number(raw:str)->Optional[float]:
    if not raw: return None
    s=raw.strip().replace("\xa0"," ").replace(" ","").replace("KZT","").replace("₸","").replace(",",".")
    try: v=float(s); return v if v>0 else None
    except: return None
def pick_dealer_price(o: ET.Element)->Optional[float]:
    vals=[]
    for t in ("purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price","min_price","supplier_price"):
        n=o.find(t)
        if n is not None and n.text:
            v=parse_price_number(n.text); 
            if v: vals.append(v)
    return min(vals) if vals else None
def _force_tail_900(n): n=int(n); return (n//1000)*1000+900 if n>=0 else 900
def compute_retail(d:float,rules:List[PriceRule])->Optional[int]:
    for lo,hi,pct,add in rules:
        if lo<=d<=hi: return _force_tail_900(d*(1+pct/100)+add)
    return None
def _remove_all_price_nodes(o: ET.Element):
    for t in ("price","Price"):
        for n in list(o.findall(t)): o.remove(n)
def strip_supplier_price_blocks(o: ET.Element):
    remove_all(o,"prices","Prices")
    for t in INTERNAL_PRICE_TAGS: remove_all(o,t)
def reprice_offers(shop: ET.Element,rules:List[PriceRule])->None:
    off=shop.find("offers"); 
    if off is None: return
    for o in off.findall("offer"):
        if o.attrib.get("_force_price","")=="100":
            strip_supplier_price_blocks(o); _remove_all_price_nodes(o); ET.SubElement(o,"price").text=str(PRICE_CAP_VALUE); o.attrib.pop("_force_price",None); continue
        d=pick_dealer_price(o)
        if not d: strip_supplier_price_blocks(o); continue
        p=compute_retail(d,rules)
        if not p: strip_supplier_price_blocks(o); continue
        _remove_all_price_nodes(o); ET.SubElement(o,"price").text=str(int(p)); strip_supplier_price_blocks(o)
def flag_unrealistic_supplier_prices(shop: ET.Element)->int:
    off=shop.find("offers"); 
    if off is None: return 0
    f=0
    for o in off.findall("offer"):
        try: src=float((get_text(o,"price") or "").replace(",",".")) if get_text(o,"price") else None
        except: src=None
        if src is not None and src>=PRICE_CAP_THRESHOLD:
            o.attrib["_force_price"]="100"; f+=1
    return f
TRUE_WORDS={"true","1","yes","y","да","есть","available","в наличии"}
FALSE_WORDS={"false","0","no","n","нет","под заказ","ожидается"}
def _parse_bool_str(s: str)->Optional[bool]:
    v=(s or "").strip().lower()
    return True if v in TRUE_WORDS else False if v in FALSE_WORDS else None
def derive_available(o: ET.Element)->bool:
    a=o.find("available")
    if a is not None and a.text:
        b=_parse_bool_str(a.text)
        if b is not None: return b
    for t in ("quantity_in_stock","quantity","stock"):
        for n in o.findall(t):
            try: return int(re.sub(r"[^\d\-]+","", n.text or "0"))>0
            except: pass
    for t in ("status","Status"):
        n=o.find(t)
        if n is not None and n.text:
            b=_parse_bool_str(n.text)
            if b is not None: return b
    return False
def normalize_available_field(shop: ET.Element)->Tuple[int,int]:
    off=shop.find("offers") or ET.Element("offers"); t=f=0
    for o in off.findall("offer"):
        b=derive_available(o)
        remove_all(o,"available")
        o.attrib["available"]="true" if b else "false"
        if DROP_STOCK_TAGS: remove_all(o,"quantity_in_stock","quantity","stock","Stock")
        if b: t+=1
        else: f+=1
    return t,f
def fix_currency_id(shop: ET.Element)->int:
    off=shop.find("offers") or ET.Element("offers"); k=0
    for o in off.findall("offer"):
        remove_all(o,"currencyId"); ET.SubElement(o,"currencyId").text="KZT"; k+=1
    return k
def ensure_categoryid_zero_first(shop: ET.Element)->int:
    off=shop.find("offers") or ET.Element("offers"); k=0
    for o in off.findall("offer"):
        remove_all(o,"categoryId","CategoryId"); cid=ET.Element("categoryId"); cid.text=os.getenv("CATEGORY_ID_DEFAULT","0"); o.insert(0,cid); k+=1
    return k
def reorder_offer_children(shop: ET.Element)->int:
    off=shop.find("offers") or ET.Element("offers"); ch=0
    for o in off.findall("offer"):
        children=list(o); buckets={k:[] for k in DESIRED_ORDER}; others=[]
        for n in children: (buckets[n.tag] if n.tag in buckets else others).append(n)
        rebuilt=[*sum((buckets[k] for k in DESIRED_ORDER), []), *others]
        if rebuilt!=children:
            for n in children: o.remove(n)
            for n in rebuilt:  o.append(n); ch+=1
    return ch

# === Description/SPECS (те же функции из прошлой версии, укорочено) ===
ALLOWED_TAGS=("h3","p","ul","ol","li","br","strong","em","b","i")
MORE_PHRASES_RE = re.compile(r"^\s*(подробнее|читать далее|узнать больше|подробности|смотреть на сайте)\s*\.?\s*$", re.I)
TYPO_FIXES=[(re.compile(r"высококачетсв",re.I),"высококачеств"),(re.compile(r"приентер",re.I),"принтер")]
def maybe_unescape_html(s: str)->str:
    if not s: return s
    if re.search(r"&lt;/?[a-zA-Z]", s):
        for _ in range(2):
            s=html.unescape(s)
            if not re.search(r"&lt;/?[a-zA-Z]", s): break
    return s
def strip_superscripts(s: str)->str:
    s=re.sub(r"(?:&#185;|&#178;|&#179;|&sup\d;|[¹²³])","", s)
    return s
def sanitize_supplier_html(raw_html: str)->str:
    s=raw_html or ""
    s=maybe_unescape_html(s)
    s=strip_superscripts(s)
    s=re.sub(r"<(script|style|iframe|object|embed|noscript)[^>]*>.*?</\1>", " ", s, flags=re.I|re.S)
    s=re.sub(r"</?(table|thead|tbody|tr|td|th|img)[^>]*>", " ", s, flags=re.I|re.S)
    s=re.sub(r"<a\b[^>]*>.*?</a>"," ", s, flags=re.I|re.S)
    s=re.sub(r"<h[1-6]\b[^>]*>","<h3>", s, flags=re.I); s=re.sub(r"</h[1-6]>","</h3>", s, flags=re.I)
    s=re.sub(r"<div\b[^>]*>","<p>", s, flags=re.I); s=re.sub(r"</div>","</p>", s, flags=re.I)
    s=re.sub(r"</?span\b[^>]*>"," ", s, flags=re.I)
    s=re.sub(r"(?:\s*<br\s*/?>\s*){2,}","</p><p>", s, flags=re.I)
    s=re.sub(r"\sstyle\s*=\s*(['\"]).*?\1","", s, flags=re.I)
    s=re.sub(r"\s(class|id|align|width|height)\s*=\s*(['\"]).*?\2","", s, flags=re.I)
    s=re.sub(r"</?(?!"+("|".join(ALLOWED_TAGS))+r")\w+[^>]*>", " ", s, flags=re.I)
    s=re.sub(r"<(p|li|ul|ol)>\s*</\1>", "", s, flags=re.I)
    s=re.sub(r"(?<=\d)\s*[xX]\s*(?=\d)","×", s)
    s=re.sub(r"\s{2,}"," ", s).strip()
    for pat,rep in TYPO_FIXES: s=pat.sub(rep, s)
    return s
def wrap_bare_text_lines_to_paragraphs(html_in: str)->str:
    if not html_in.strip(): return html_in
    lines=html_in.splitlines(); out=[]; buf=[]
    def flush():
        if not buf: return
        text=" ".join(x.strip() for x in buf if x.strip())
        if text: out.append(f"<p>{_html_escape_in_cdata_safe(text)}</p>")
        buf.clear()
    for ln in lines:
        s=ln.strip()
        if not s: flush(); continue
        if s.startswith("<"): flush(); out.append(ln)
        else: buf.append(s)
    flush()
    res="\n".join(out)
    if not re.search(r"<(p|ul|ol|h3)\b", res, flags=re.I):
        txt=_html_escape_in_cdata_safe(" ".join(x.strip() for x in lines if x.strip()))
        res=f"<p>{txt}</p>"
    return res
def remove_urls_and_cta(html_in: str)->str:
    s=re.sub(r"\bhttps?://[^\s<]+","", html_in, flags=re.I)
    s=re.sub(r"\bwww\.[^\s<]+","", s, flags=re.I)
    def drop(m):
        inner=m.group(1)
        return "" if MORE_PHRASES_RE.match(inner) else m.group(0)
    return re.sub(r"<p>(.*?)</p>", drop, s, flags=re.I|re.S)
def strip_name_repeats(html_in: str, product_name: str)->str:
    if not product_name: return html_in
    pat=re.compile(rf"^\s*{re.escape(product_name)}\s*[—–\-:,]*\s*", re.I)
    def cut(m):
        inner=m.group(1); cleaned=pat.sub("", inner).strip()
        return f"<p>{cleaned}</p>" if cleaned else ""
    return re.sub(r"<p>(.*?)</p>", cut, html_in, flags=re.I|re.S)
def _clean_ws(v: str)->str: return re.sub(r"\s+"," ", (v or "").replace("\u00A0"," ").replace("&nbsp;"," ")).strip(" \t\r\n,;:.-")

# === Канонизация ключей/значений, парс Param, и т. п. (как в прошлой версии) ===
CANON_MAP={"тип":"Тип","тип печати":"Тип печати","цвет печати":"Цвет печати","формат":"Формат",
           "разрешение":"Разрешение печати","разрешение печати":"Разрешение печати","оптическое разрешение":"Оптическое разрешение",
           "скорость печати":"Скорость печати","интерфейсы":"Интерфейсы","интерфейс":"Интерфейсы","wi-fi":"Wi-Fi",
           "двусторонняя печать":"Двусторонняя печать","дисплей":"Дисплей","цвета чернил":"Цвета чернил","тип чернил":"Тип чернил",
           "страна происхождения":"Страна происхождения","гарантия":"Гарантия","автоподатчик":"Автоподатчик",
           "подача бумаги":"Подача бумаги","совместимость":"Совместимость","назначение":"Назначение"}
def canon_key(k:str)->Optional[str]:
    return CANON_MAP.get(k.strip().lower().replace("ё","е"))

def normalize_value_spec(key: str, val: str)->str:
    v=_clean_ws(val)
    if key=="Скорость печати":
        m=re.search(r"(\d{1,3})", v); return f"до {m.group(1)} стр/мин" if m else ""
    if key in ("Разрешение печати","Оптическое разрешение"):
        vv=v.replace("x","×").replace("X","×"); vv=re.sub(r"(?<=\d)[\.,](?=\d{3}\b)","", vv)
        if key=="Разрешение печати" and not re.search(r"\bdpi\b", vv, re.I): vv+=" dpi"
        return _clean_ws(vv)
    if key=="Интерфейсы": return _clean_ws(v)
    if key in ("Wi-Fi","Дисплей","Двусторонняя печать","Автоподатчик"):
        low=v.lower()
        if re.fullmatch(r"(да|есть|yes|true)", low): return "Да"
        if re.fullmatch(r"(нет|no|false|отсутствует)", low): return "Нет"
        return _clean_ws(v)
    if key=="Формат":
        t=v.replace("бумаги",""); t=re.sub(r"\s*[xX]\s*","×", t)
        parts=[p.strip() for p in re.split(r"[;,]+", t) if p.strip()]
        keep=[]
        for p in parts:
            p=re.sub(r"\s*×\s*","×", p)
            if re.fullmatch(r"(A\d|B\d|C6|DL|Letter|Legal|No\.?\s*10|\d{1,2}×\d{1,2}|16:9|10×15|13×18|9×13)", p, re.I):
                keep.append(p)
        if keep:
            if len(keep)>10: keep=keep[:10]+["и др."]
            return ", ".join(keep)
        return ""
    if key=="Гарантия":
        m=re.match(r"^\s*(\d{1,3})", v); return f"{m.group(1)} мес" if m else _clean_ws(v)
    return _clean_ws(v)

def parse_existing_specs_block(html_frag: str):
    specs={}
    m=re.search(r"(?is)(<h3>\s*Характеристики\s*</h3>\s*<ul>(.*?)</ul>)", html_frag)
    if not m: return specs, html_frag
    ul=m.group(2)
    for li in re.finditer(r"(?is)<li>\s*<strong>([^:<]+):\s*</strong>\s*(.*?)\s*</li>", ul):
        k=li.group(1).strip(); v=li.group(2).strip()
        if k and v: specs[k]=v
    cleaned=html_frag[:m.start()]+html_frag[m.end():]
    return specs, cleaned

def _fix_li_values(html_in: str)->str:
    if not html_in: return html_in
    pat=re.compile(r"(?is)(<li>\s*<strong>[^<:]+:\s*</strong>)(.*?)(</li>)")
    def repl(m):
        head,val,tail=m.group(1),m.group(2),m.group(3)
        val=re.sub(r"\s+"," ", (val or "")).strip(" ,;:.")
        return f"{head} {val}{tail}" if val else f"{head}{tail}"
    out=pat.sub(repl, html_in)
    out=re.sub(r"(<strong>[^<]*:</strong>)\s+", r"\1 ", out)
    return out

def classify_kind(name: str)->str:
    n=(name or "").lower()
    if "проектор" in n: return "projector"
    if "интерактивн" in n and ("панел" in n or "диспле" in n or "доск" in n): return "interactive"
    if "монитор" in n: return "monitor"
    if any(k in n for k in ["картридж","чернила","ёмкость для отработанных","емкость для отработанных","maintenance box","тонер","пленка для ламинирования","плёнка для ламинирования"]):
        return "consumable"
    return "device"

DISALLOWED_PARAM_NAMES={"производитель","для бренда","наименование производителя","сопутствующие товары","бренд","brand","manufacturer","vendor","поставщик"}
CANON_NAME_MAP_BASE={"тип":"Тип","вид":"Тип","тип печати":"Тип печати","цвет печати":"Цвет печати","цветность":"Цвет печати",
                     "формат":"Формат","формат бумаги":"Формат","разрешение печати":"Разрешение печати","разрешение":"Разрешение печати",
                     "оптическое разрешение":"Оптическое разрешение","разрешение сканера":"Оптическое разрешение","разрешение сканера,dpi":"Оптическое разрешение",
                     "скорость печати":"Скорость печати","двусторонняя печать":"Двусторонняя печать","интерфейс":"Интерфейсы","интерфейсы":"Интерфейсы",
                     "wi-fi":"Wi-Fi","подача бумаги":"Подача бумаги","жк дисплей":"Дисплей","дисплей":"Дисплей","страна происхождения":"Страна происхождения","гарантия":"Гарантия",
                     "совместимые продукты":"Совместимость","совместимость":"Совместимость","тип чернил":"Тип чернил","ресурс":"Ресурс","объем":"Объем","объём":"Объем",
                     "автоподатчик":"Автоподатчик"}
ALLOWED_PARAM_CANON=set(CANON_NAME_MAP_BASE.values())|{"Назначение","Цвета чернил"}

def canon_param_name(name: str, kind: str)->Optional[str]:
    if not name: return None
    key=name.strip().lower().replace("ё","е")
    if key in DISALLOWED_PARAM_NAMES: return None
    base=CANON_NAME_MAP_BASE.get(key)
    if base: 
        if base=="Разрешение печати" and kind in {"projector","interactive","monitor"}: return "Разрешение"
        return base
    title=name.strip()
    tcap=title[:1].upper()+title[1:].lower()
    return tcap if tcap in ALLOWED_PARAM_CANON else None

def _norm_resolution_print(s:str)->str:
    t=s.replace("x","×").replace("X","×"); t=re.sub(r"(?<=\d)[\.,](?=\d{3}\b)","", t)
    m=re.search(r"(\d{2,5})\s*×\s*(\d{2,5})", t)
    return f"{m.group(1)}×{m.group(2)} dpi" if m else ""
def _norm_resolution_display(s:str)->str:
    t=s.replace("x","×").replace("X","×"); t=re.sub(r"\s*dpi\b","", t, flags=re.I)
    m=re.search(r"(\d{3,5})\s*×\s*(\d{3,5})", t); return f"{m.group(1)}×{m.group(2)}" if m else ""

ALLOWED_IFACE_CANON={"USB","USB (тип B)","USB-host","Wi-Fi","Wi-Fi Direct","Ethernet","RJ-45","Bluetooth","HDMI","DisplayPort","SD-карта"}
IFACE_PATTERNS=[
    (re.compile(r"\bwi[\s\-]?fi\b",re.I),"Wi-Fi"),
    (re.compile(r"\bwi[\s\-]?fi\s*direct\b",re.I),"Wi-Fi Direct"),
    (re.compile(r"\bethernet\b",re.I),"Ethernet"),
    (re.compile(r"\brj[\s\-]?45\b",re.I),"RJ-45"),
    (re.compile(r"\bbluetooth\b",re.I),"Bluetooth"),
    (re.compile(r"\bhdmi\b",re.I),"HDMI"),
    (re.compile(r"\bdisplay\s*port|displayport\b",re.I),"DisplayPort"),
    (re.compile(r"\busb[\s\-]?host\b",re.I),"USB-host"),
    (re.compile(r"\bsd[\s\-]?card|sd-?карта|sd\s*\b",re.I),"SD-карта"),
    (re.compile(r"\busb\b",re.I),"USB"),
    (re.compile(r"\bтип\s*b\b",re.I),"USB (тип B)"),
]

def extract_interfaces_only(text: str)->str:
    s=_clean_ws(text)
    found=[]
    for pat,label in IFACE_PATTERNS:
        if pat.search(s): found.append(label)
    if "USB" in found and "USB (тип B)" in found:
        found=[x for x in found if x not in {"USB","USB (тип B)"}]+["USB (тип B)"]
    uniq=[]; seen=set()
    for x in found:
        if x in ALLOWED_IFACE_CANON and x not in seen:
            uniq.append(x); seen.add(x)
    return ", ".join(uniq)

def _norm_yesno(s:str)->str:
    low=s.strip().lower()
    if re.fullmatch(r"(да|есть|yes|true)", low): return "Да"
    if re.fullmatch(r"(нет|no|false|отсутствует)", low): return "Нет"
    return _clean_ws(s)

def _norm_speed(s: str)->str:
    m=re.search(r"(\d{1,3})\s*(стр|pages)\s*/?\s*мин", s, re.I)
    if m: return f"до {m.group(1)} стр/мин"
    m2=re.search(r"^(\d{1,3})$", s.strip()); return f"до {m2.group(1)} стр/мин" if m2 else ""

def normalize_value_by_key(k: str, v: str, kind: str)->str:
    key=k.lower(); s=v.strip()
    if key=="разрешение печати":       return _norm_resolution_print(s)
    if key=="разрешение":              return _norm_resolution_display(s)
    if key in ("интерфейсы","входы"):  return extract_interfaces_only(s)
    if key=="wi-fi":                   return _norm_yesno(s)
    if key=="формат":
        t=v.replace("бумаги",""); t=re.sub(r"\s*[xX]\s*","×", t)
        parts=[p.strip() for p in re.split(r"[;,]+", t) if p.strip()]
        keep=[]
        for p in parts:
            p=re.sub(r"\s*×\s*","×", p)
            if re.fullmatch(r"(A\d|B\d|C6|DL|Letter|Legal|No\.?\s*10|\d{1,2}×\d{1,2}|16:9|10×15|13×18|9×13)", p, re.I):
                keep.append(p)
        if keep:
            if len(keep)>10: keep=keep[:10]+["и др."]
            return ", ".join(keep)
        return ""
    if key=="дисплей":                 return "Да" if _norm_yesno(s)=="Да" else _clean_ws(s)
    if key=="скорость печати":         return _norm_speed(s)
    if key=="двусторонняя печать":     return _norm_yesno(s)
    if key=="оптическое разрешение":   return _norm_resolution_print(s)
    if key=="гарантия":
        m=re.match(r"^\s*(\d{1,3})", s); return f"{m.group(1)} мес" if m else _clean_ws(s)
    return _clean_ws(s)

def clean_param_value(k: str, v: str)->str:
    s=re.sub(r"https?://\S+|www\.\S+","", (v or ""), flags=re.I)
    s=_clean_ws(s)
    if len(s)>PARAMS_MAX_VALUE_LEN: s=s[:PARAMS_MAX_VALUE_LEN-3]+"..."
    return s

def _split_type_and_usage(val: str)->Tuple[str, Optional[str]]:
    s=val.strip()
    tokens=re.split(r"[\s,/]+", s); usage=None
    for t in tokens[:]:
        low=t.lower()
        if low in {"дом","домашний","домашнее"}: usage="Дом"; tokens.remove(t)
        elif low in {"офис","офисный","офисное"}: usage="Офис"; tokens.remove(t)
    base=" ".join(tokens).strip()
    return (base or val.strip(), usage)

def collect_params_canonical(offer: ET.Element)->List[Tuple[str,str]]:
    name=get_text(offer,"name"); kind=classify_kind(name)
    merged: Dict[str,str]={}
    for tag in ("Param","param","PARAM"):
        for pn in offer.findall(tag):
            raw_name=(pn.get("name") or pn.get("Name") or "").strip()
            raw_val =(pn.text or "").strip()
            if not raw_name or not raw_val: continue
            canon=canon_param_name(raw_name, kind)
            if not canon: continue
            val=normalize_value_by_key(canon, raw_val, kind)
            if not val: continue
            if canon=="Тип":
                base,usage=_split_type_and_usage(val); val=base
                if usage and "Назначение" not in merged: merged["Назначение"]=usage
            old=merged.get(canon,"")
            if len(val)>len(old): merged[canon]=val
    for b in ("Двусторонняя печать","Wi-Fi","Автоподатчик","Дисплей"):
        if b in merged: merged[b]=_norm_yesno(merged[b])
    important=["Тип","Назначение","Тип печати","Цвет печати","Формат","Разрешение","Разрешение печати","Скорость печати",
               "Оптическое разрешение","Интерфейсы","Wi-Fi","Двусторонняя печать","Дисплей","Подача бумаги",
               "Тип чернил","Цвета чернил","Совместимость","Страна происхождения","Гарантия","Автоподатчик"]
    idx={k:i for i,k in enumerate(important)}
    return [(k, merged[k]) for k in sorted(merged.keys(), key=lambda x: idx.get(x,999))]

MAX_LEN_BY_KEY={
    "Тип":40,"Назначение":120,"Тип печати":20,"Цвет печати":20,"Формат":120,
    "Разрешение":30,"Разрешение печати":30,"Оптическое разрешение":30,
    "Скорость печати":30,"Интерфейсы":120,"Дисплей":20,"Wi-Fi":5,
    "Двусторонняя печать":5,"Подача бумаги":40,"Тип чернил":60,"Цвета чернил":120,
    "Страна происхождения":40,"Гарантия":20,"Автоподатчик":5,"Совместимость":400,
}
DROP_PATTERNS_COMMON=[
    re.compile(r"\bwith flatbed scan.*", re.I),
    re.compile(r"\bSingle\-?sided scan speed.*", re.I),
    re.compile(r"\bТехника Метод печати.*", re.I),
    re.compile(r"\bВыработка .*", re.I),
    re.compile(r"\bМногофункциональный Печать.*", re.I),
    re.compile(r"\bСовременные гибкие возможности.*", re.I),
]
def validate_and_clip_specs(specs: Dict[str,str], kind: str)->Dict[str,str]:
    out={}
    for k,v in specs.items():
        val=_clean_ws(v)
        for pat in DROP_PATTERNS_COMMON: val=pat.sub("", val)
        if k in ("Разрешение","Разрешение печати","Оптическое разрешение"):
            if not re.search(r"\d{2,5}\s*×\s*\d{2,5}", val): 
                continue
            if k=="Разрешение печати" and not re.search(r"\bdpi\b", val, re.I): val+=" dpi"
        elif k=="Скорость печати":
            m=re.search(r"(\d{1,3})", val); 
            if not m: continue
            val=f"до {m.group(1)} стр/мин"
        elif k=="Интерфейсы":
            val=extract_interfaces_only(val)
            if not val: continue
        elif k=="Формат":
            val=normalize_value_spec("Формат", val)
            if not val: continue
        elif k in ("Wi-Fi","Двусторонняя печать","Автоподатчик","Дисплей"):
            val=_norm_yesno(val)
            if k=="Дисплей" and val not in ("Да","Нет"):
                m=re.search(r"(\d{1,2}(?:[.,]\d)?)\s*(см|дюйм|\"|inch)", v, re.I)
                val= (m.group(1).replace(".",",")+" см") if m else "Да"
        maxlen=MAX_LEN_BY_KEY.get(k,180)
        if len(val)>maxlen: val=val[:maxlen-3]+"..."
        val=_clean_ws(val)
        if val: out[k]=val
    return out

# === Keywords ===
def build_keywords_for_offer(offer: ET.Element)->str:
    WORD_RE=re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
    def tok(s): return WORD_RE.findall(s or "")
    name=get_text(offer,"name"); vendor=get_text(offer,"vendor").strip()
    base=[vendor] if vendor else []
    rt=tok(name); models=[t for t in rt if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    base += list({t.upper() for t in models[:8]})
    geo=["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
         "Оскемен","Семей","Костанаи","Кызылорда","Орал","Петропавл","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау"]
    base += geo[:SATU_KEYWORDS_GEO_MAX]
    parts=[]; seen=set(); total=0
    for p in base:
        if not p: continue
        if p.lower() in seen: continue
        add=((", " if parts else "")+p)
        if total+len(add)>SATU_KEYWORDS_MAXLEN: break
        parts.append(p); seen.add(p.lower()); total+=len(add)
    return ", ".join(parts)
def ensure_keywords(shop: ET.Element)->int:
    off=shop.find("offers") or ET.Element("offers"); k=0
    for o in off.findall("offer"):
        kw=build_keywords_for_offer(o)
        node=o.find("keywords")
        if not kw:
            if node is not None: o.remove(node)
            continue
        if node is None: node=ET.SubElement(o,"keywords"); node.text=kw; k+=1
        else:
            if (node.text or "")!=kw: node.text=kw; k+=1
    return k

# === vendorCode / placeholders ===
ARTICUL_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)
def _extract_article_from_name(name: str)->str:
    if not name: return ""
    m=ARTICUL_RE.search(name); return (m.group(1) if m else "").upper()
def _normalize_code(s: str)->str:
    s=(s or "").strip(); s=re.sub(r"[\s_]+","", s).replace("—","-").replace("–","-")
    return re.sub(r"[^A-Za-z0-9\-]+","", s).upper()
def ensure_vendorcode_with_article(shop: ET.Element, prefix: str, create_if_missing: bool=False)->None:
    off=shop.find("offers") or ET.Element("offers")
    for o in off.findall("offer"):
        vc=o.find("vendorCode")
        if vc is None:
            if not create_if_missing: continue
            vc=ET.SubElement(o,"vendorCode"); vc.text=""
        old=(vc.text or "").strip()
        if old=="" or old.upper()==prefix.upper():
            art=_normalize_code(_extract_article_from_name(get_text(o,"name")) or o.attrib.get("id") or "")
            if art: vc.text=art
        vc.text=f"{prefix}{(vc.text or '')}"
def sync_offer_id_with_vendorcode(shop: ET.Element)->None:
    off=shop.find("offers") or ET.Element("offers")
    for o in off.findall("offer"):
        vc=o.find("vendorCode")
        if vc is not None and (vc.text or "").strip():
            if o.attrib.get("id")!=(vc.text or "").strip():
                o.attrib["id"]=(vc.text or "").strip()

_url_head_cache: Dict[str,bool]={}
def url_exists(url:str)->bool:
    if not url: return False
    if url in _url_head_cache: return _url_head_cache[url]
    try: ok=200<=requests.head(url,timeout=5,allow_redirects=True).status_code<400
    except Exception: ok=False
    _url_head_cache[url]=ok; return ok
def _slug(s: str)->str:
    table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    base=(s or "").lower().translate(table); base=re.sub(r"[^a-z0-9\- ]+","", base)
    return re.sub(r"\s+","-", base).strip("-") or "unknown"
def _placeholder_url_brand(vendor: str)->str: return f"{PLACEHOLDER_BRAND_BASE}/{_slug(vendor)}.jpg"
def _placeholder_url_category(name: str)->str:
    n=(name or "").lower()
    if "картридж" in n or "тонер" in n: return f"{PLACEHOLDER_CATEGORY_BASE}/cartridge.jpg"
    if "сканер" in n: return f"{PLACEHOLDER_CATEGORY_BASE}/scanner.jpg"
    if "проектор" in n: return f"{PLACEHOLDER_CATEGORY_BASE}/projector.jpg"
    if "принтер" in n or "мфу" in n: return f"{PLACEHOLDER_CATEGORY_BASE}/mfp.jpg"
    return f"{PLACEHOLDER_CATEGORY_BASE}/other.jpg"
def ensure_placeholder_pictures(shop: ET.Element)->int:
    if not PLACEHOLDER_ENABLE: return 0
    off=shop.find("offers") or ET.Element("offers"); add=0
    for o in off.findall("offer"):
        pics=list(o.findall("picture"))
        has=any((p.text or "").strip() for p in pics)
        if has: continue
        vendor=get_text(o,"vendor").strip(); name=get_text(o,"name").strip()
        picked=""
        if vendor:
            u=_placeholder_url_brand(vendor)
            if url_exists(u): picked=u
        if not picked:
            u=_placeholder_url_category(name)
            if url_exists(u): picked=u
        if not picked: picked=PLACEHOLDER_DEFAULT_URL
        ET.SubElement(o,"picture").text=picked; add+=1
    return add

# === FEED_META как раньше ===
def render_feed_meta_comment(pairs:Dict[str,str])->str:
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
    w=max(len(k) for k,_ in rows)
    return "FEED_META\n" + "\n".join(f"{k.ljust(w)} | {v}" for k,v in rows)

# === Постзамены в XML ===
def _replace_html_placeholders_with_cdata(xml_text: str)->str:
    def repl(m):
        inner=m.group(1).replace("[[[HTML]]]", "").replace("[[[/HTML]]]", "")
        inner=html.unescape(inner)
        return f"<description><![CDATA[\n{inner}\n]]></description>"
    return re.sub(r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>", repl, xml_text, flags=re.S)
def normalize_name_text(s: str)->str:
    if not s: return s
    t=s.replace("\u00A0"," ").replace("&nbsp;"," ")
    t=re.sub(r"\s{2,}"," ", t)
    t=re.sub(r"(?<=\d)\s*[xX]\s*(?=\d)", "×", t)
    t=re.sub(r"\b(Wi)[\s\-]?Fi\b","Wi-Fi", t, flags=re.I)
    return t.strip()

# === MAIN ===
def main():
    log("Run set -e                       # прерывать шаг при любой ошибке")
    log(f"Python {sys.version.split()[0]}")
    log(f"Source: {SUPPLIER_URL}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)
    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("<shop> not found")
    offers_in=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in is None: err("<offers> not found")
    src_offers=list(offers_in.findall("offer"))

    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); offers_el=ET.SubElement(out_shop,"offers")
    for o in src_offers:
        mod=deepcopy(o)
        if DROP_CATEGORY_ID_TAG:
            for n in list(mod.findall("categoryId"))+list(mod.findall("CategoryId")): mod.remove(n)
        offers_el.append(mod)

    # Фильтр по <name>
    keys=load_name_filter(AKCENT_KEYWORDS_PATH)
    if AKCENT_KEYWORDS_MODE=="include" and len(keys)==0:
        err("AKCENT_KEYWORDS_MODE=include, а файл docs/akcent_keywords.txt пуст/не найден", 2)
    before=len(list(offers_el.findall("offer"))); hits=0; removed=0
    if (AKCENT_KEYWORDS_MODE in {"include","exclude"}) and keys:
        for off in list(offers_el.findall("offer")):
            nm=get_text(off,"name")
            hit=name_matches(nm, keys)
            if hit: hits+=1
            drop=(AKCENT_KEYWORDS_MODE=="exclude" and hit) or (AKCENT_KEYWORDS_MODE=="include" and not hit)
            if drop: offers_el.remove(off); removed+=1
        log(f"Filter mode: {AKCENT_KEYWORDS_MODE} | Keywords loaded: {len(keys)} | Offers before: {before} | Matched: {hits} | Removed: {removed} | Kept: {before-removed}")
    else:
        log("Filter disabled")

    flagged=flag_unrealistic_supplier_prices(out_shop); log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")
    v_norm,v_filled,v_removed=ensure_vendor(out_shop); log(f"Vendors auto-filled: {v_filled}")

    ensure_vendorcode_with_article(out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AC"),
                                   create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"})
    sync_offer_id_with_vendorcode(out_shop)
    reprice_offers(out_shop, PRICING_RULES)
    ph_added=ensure_placeholder_pictures(out_shop); log(f"Placeholders added: {ph_added}")

    # Нормализуем <name>
    for offer in offers_el.findall("offer"):
        nm=offer.find("name")
        if nm is not None and nm.text: nm.text=normalize_name_text(nm.text)

    # ОПИСАНИЕ + ХАР-КИ
    desc_changed=0
    for offer in offers_el.findall("offer"):
        name=get_text(offer,"name")
        d=offer.find("description")
        src=inner_html(d)

        supplier_html=sanitize_supplier_html(src)
        supplier_html=remove_urls_and_cta(supplier_html)
        supplier_html=strip_name_repeats(supplier_html, name)
        supplier_html=wrap_bare_text_lines_to_paragraphs(supplier_html)

        existing_specs, rest = parse_existing_specs_block(supplier_html)

        marketing_clean=re.sub(r"\s{2,}"," ", rest).strip()
        kind=classify_kind(name)

        specs_from_param=collect_params_canonical(offer)
        merged=dict(existing_specs)
        for k,v in specs_from_param:
            if k not in merged or len(v)>len(merged[k]): merged[k]=v
        merged=validate_and_clip_specs(merged, kind)

        parts=[f"<h3>{_html_escape_in_cdata_safe(name)}</h3>"]
        if marketing_clean:
            first_p=re.search(r"(?is)<p>(.*?)</p>", marketing_clean)
            if first_p:
                txt=re.sub(r"<[^>]+>","", first_p.group(1)).strip()
                if len(txt)>700:
                    txt=txt[:700]
                    txt=txt.rsplit(" ",1)[0]+"..."
                parts.append(f"<p>{_html_escape_in_cdata_safe(txt)}</p>")
            else:
                txt=re.sub(r"<[^>]+>","", marketing_clean).strip()
                if txt:
                    if len(txt)>700: txt=txt[:700].rsplit(" ",1)[0]+"..."
                    parts.append(f"<p>{_html_escape_in_cdata_safe(txt)}</p>")

        if merged:
            lines=["<h3>Характеристики</h3>","<ul>"]
            order=["Тип","Назначение","Тип печати","Цвет печати","Формат","Разрешение","Разрешение печати","Оптическое разрешение",
                   "Скорость печати","Интерфейсы","Wi-Fi","Двусторонняя печать","Дисплей","Подача бумаги",
                   "Тип чернил","Цвета чернил","Совместимость","Страна происхождения","Гарантия","Автоподатчик"]
            key_order={k:i for i,k in enumerate(order)}
            for k,v in sorted(merged.items(), key=lambda kv: key_order.get(kv[0],999)):
                lines.append(f'  <li><strong>{_html_escape_in_cdata_safe(k)}:</strong> {_html_escape_in_cdata_safe(v)}</li>')
            lines.append("</ul>")
            parts.append("\n".join(lines))

        full_html="\n".join([p for p in parts if p]).strip()
        full_html=_fix_li_values(full_html)
        placeholder=f"[[[HTML]]]{full_html}[[[/HTML]]]"
        if d is None:
            d=ET.SubElement(offer,"description"); d.text=placeholder; desc_changed+=1
        else:
            if (d.text or "")!=placeholder:
                d.text=placeholder; desc_changed+=1

    # Перезапись <param> канонично
    for offer in offers_el.findall("offer"):
        specs=collect_params_canonical(offer)
        for t in ("param","Param","PARAM"):
            for pn in list(offer.findall(t)): offer.remove(pn)
        for k,v in specs:
            if not v: continue
            node=ET.SubElement(offer,"param"); node.set("name", k); node.text=_clean_ws(v)

    t_true,t_false=normalize_available_field(out_shop)
    fix_currency_id(out_shop)

    for of in offers_el.findall("offer"):
        for t in PURGE_TAGS_AFTER:
            for n in list(of.findall(t)): of.remove(n)
        for a in PURGE_OFFER_ATTRS_AFTER:
            if a in of.attrib: of.attrib.pop(a,None)

    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)
    kw=ensure_keywords(out_shop); log(f"Keywords updated: {kw}")
    log(f"Descriptions rebuilt: {desc_changed}")

    built_alm=now_almaty()
    meta_pairs={
        "supplier": SUPPLIER_NAME, "source": SUPPLIER_URL,
        "offers_total": len(src_offers),
        "offers_written": len(list(offers_el.findall("offer"))),
        "available_true": str(t_true), "available_false": str(t_false),
        "built_alm": format_dt_almaty(built_alm),
        "next_build_alm": format_dt_almaty(next_build_time_almaty()),
        "seo_last_update_alm": format_dt_almaty(built_alm),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # Формируем строку XML в Unicode, потом делаем его безопасным для CP1251 и пишем
    try: ET.indent(out_root, space="  ")
    except Exception: pass

    xml_unicode = ET.tostring(out_root, encoding="unicode")
    xml_unicode = _replace_html_placeholders_with_cdata(xml_unicode)

    # Пустая строка между офферами и после FEED_META
    xml_unicode = re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_unicode)
    xml_unicode = re.sub(r"(?s)(-->)\s*(<shop\b)", r"\1\n\2", xml_unicode)

    # Делаем текст безопасным для cp1251
    xml_unicode = make_encoding_safe(xml_unicode, ENC)

    # Добавим декларацию с нужной кодировкой
    xml_decl = f'<?xml version="1.0" encoding="{ENC}"?>\n'
    xml_out = xml_decl + xml_unicode

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written."); return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML,"w",encoding=ENC, newline="\n") as f:
        f.write(xml_out)
    try:
        docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e: warn(f".nojekyll warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | script={SCRIPT_VERSION}")

if __name__=="__main__":
    try: main()
    except Exception as e: err(str(e))
