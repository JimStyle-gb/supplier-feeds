# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
Alstyle → YML for Satu (same output as v34, but single-pass & slimmer)
"""

from __future__ import annotations
import os, re, time, random, urllib.parse, sys
from typing import Dict, List, Tuple, Optional, Set
from xml.etree import ElementTree as ET
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
import requests

# ===== ENV =====
SCRIPT_VERSION = "alstyle-2025-10-14.v34-slim"
SUPPLIER_NAME  = os.getenv("SUPPLIER_NAME", "AlStyle")
SUPPLIER_URL   = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML   = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC            = os.getenv("OUTPUT_ENCODING", "windows-1251")
TIMEOUT_S      = int(os.getenv("TIMEOUT_S", "30"))
RETRIES        = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF  = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES      = int(os.getenv("MIN_BYTES", "1500"))

ALSTYLE_CATEGORIES_PATH = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt")
ALSTYLE_CATEGORIES_MODE = os.getenv("ALSTYLE_CATEGORIES_MODE", "include").lower()  # off|include|exclude

# Satu
SATU_MODE              = os.getenv("SATU_MODE", "full").lower()    # lean|full
SATU_KEYWORDS          = os.getenv("SATU_KEYWORDS", "auto").lower() # auto|off
SATU_KEYWORDS_MAXLEN   = int(os.getenv("SATU_KEYWORDS_MAXLEN", "160"))
SATU_KEYWORDS_MAXWORDS = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "16"))
SATU_KEYWORDS_GEO      = os.getenv("SATU_KEYWORDS_GEO", "on").lower() in {"on","1","true","yes"}

# Policies
PURGE_TAGS_AFTER = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","article")  # 'available' не удаляем здесь
INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice","prices","Prices"
)

# ===== Utils =====
log = lambda m: print(m, flush=True)
def err(msg: str, code: int = 1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_alm_str():
    try: return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception: return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def load_source_bytes(src: str) -> bytes:
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        data = open(path,"rb").read()
        if len(data)<MIN_BYTES: raise RuntimeError(f"file too small: {len(data)}")
        return data
    s=requests.Session(); last=None
    for i in range(1,RETRIES+1):
        try:
            r=s.get(src, headers={"User-Agent":"supplier-feed-bot/1.0"}, timeout=TIMEOUT_S)
            r.raise_for_status(); data=r.content
            if len(data)<MIN_BYTES: raise RuntimeError(f"too small: {len(data)} bytes")
            return data
        except Exception as e:
            last=e; back=RETRY_BACKOFF*i*(1+random.uniform(-0.2,0.2))
            if i<RETRIES: time.sleep(back)
    raise RuntimeError(f"fetch failed: {last}")

def get_text(el: ET.Element, tag: str) -> str:
    n=el.find(tag); return (n.text or "").strip() if n is not None and n.text else ""

def _norm_text(s: str) -> str:
    s=(s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

def remove_all(el: ET.Element, *tags: str):
    for t in tags:
        for x in list(el.findall(t)): el.remove(x)

# ===== Categories (only if include/exclude) =====
class CatRule:
    __slots__=("raw","kind","pattern")
    def __init__(self, raw: str, kind: str, pattern): self.raw, self.kind, self.pattern = raw, kind, pattern

def load_category_rules(path: str) -> Tuple[Set[str], List[CatRule]]:
    if not path or not os.path.exists(path): return set(), []
    txt=open(path, "r", encoding="utf-8", errors="ignore").read().replace("\ufeff","").replace("\x00","")
    ids:set=set(); rules:List[CatRule]=[]
    for ln in txt.splitlines():
        s=ln.strip()
        if not s or s.startswith("#"): continue
        if re.fullmatch(r"\d{2,}", s): ids.add(s); continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try: rules.append(CatRule(s,"regex",re.compile(s[1:-1],re.I))); continue
            except Exception: continue
        if s.startswith("~="):
            w=_norm_text(s[2:]); 
            if w: rules.append(CatRule(s,"word",re.compile(r"\b"+re.escape(w)+r"\b",re.I))); 
            continue
        rules.append(CatRule(_norm_text(s),"substr",None))
    return ids, rules

def parse_categories_tree(shop_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,Set[str]]]:
    id2name={}; id2parent={}; tree={}
    cats=shop_el.find("categories") or shop_el.find("Categories")
    if cats is None: return id2name,id2parent,tree
    for c in cats.findall("category"):
        cid=(c.attrib.get("id") or "").strip()
        if not cid: continue
        pid=(c.attrib.get("parentId") or "").strip()
        nm =(c.text or "").strip()
        id2name[cid]=nm
        if pid: id2parent[cid]=pid
        tree.setdefault(pid,set()).add(cid)
    return id2name,id2parent,tree

def build_category_path_from_id(cat_id: str, id2name: Dict[str,str], id2parent: Dict[str,str]) -> str:
    names=[]; cur=cat_id; seen=set()
    while cur and cur not in seen and cur in id2name:
        seen.add(cur); names.append(id2name.get(cur,"")); cur=id2parent.get(cur,"")
    names=[n for n in names if n]
    return " / ".join(reversed(names)) if names else ""

def category_matches_name(path_str: str, rules: List[CatRule]) -> bool:
    cat_norm=_norm_text(re.sub(r"\s*[/>\|]\s*", " / ", (path_str or "")))
    for cr in rules:
        if cr.kind=="substr" and cr.raw and cr.raw in cat_norm: return True
        if cr.pattern and cr.pattern.search(path_str or ""): return True
    return False

def descendants(ids:Set[str], tree:Dict[str,Set[str]])->Set[str]:
    out=set(ids); stack=list(ids)
    while stack:
        cur=stack.pop()
        for ch in tree.get(cur,()):
            if ch not in out: out.add(ch); stack.append(ch)
    return out

# ===== Price rules (same as v34) =====
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
DEALER_HINT  = re.compile(r"(дилер|dealer|опт|wholesale|b2b|закуп|purchase|оптов)", re.I)
RRP_HINT     = re.compile(r"(rrp|ррц|розниц|retail|msrp)", re.I)

def parse_price_number(raw:str)->Optional[float]:
    if raw is None: return None
    s=(raw.strip().replace("\xa0"," ").replace(" ","").replace("KZT","").replace("kzt","").replace("₸","").replace(",",".")); 
    if not s: return None
    try: v=float(s); return v if v>0 else None
    except Exception: return None

def pick_dealer_price(offer: ET.Element) -> Tuple[Optional[float], str]:
    dealers=[]; rrps=[]
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            val=parse_price_number(p.text or ""); 
            if val is None: continue
            t=(p.attrib.get("type") or "")
            if DEALER_HINT.search(t): dealers.append(val)
            elif RRP_HINT.search(t): rrps.append(val)
    if dealers: return (min(dealers), "prices_dealer")
    direct=[]
    for tag in PRICE_FIELDS_DIRECT:
        el=offer.find(tag)
        if el is not None and el.text:
            v=parse_price_number(el.text)
            if v is not None: direct.append(v)
    if direct: return (min(direct), "direct_field")
    if rrps:   return (min(rrps), "rrp_fallback")
    return (None, "missing")

def _force_tail_900(n: float) -> int:
    i=int(n); k=max(i//1000,0); out=k*1000+900
    return out if out>=900 else 900

def compute_retail(dealer: float) -> Optional[int]:
    for lo,hi,pct,add in PRICING_RULES:
        if lo<=dealer<=hi:
            val=dealer*(1.0+pct/100.0)+add
            return _force_tail_900(val)
    return None

# ===== Params → description =====
UNWANTED_PARAM_NAME_RE = re.compile(
    r"^(?:\s*(?:благотворительн\w*|снижена\s*цена|новинк\w*|артикул(?:\s*/\s*штрихкод)?|оригинальн\w*\s*код|штрихкод)\s*)$",
    re.I
)
BAD_LINE_START = re.compile(
    r"^\s*(?:артикул(?:\s*/\s*штрихкод)?|оригинальн\w*\s*код|штрихкод|благотворительн\w*|новинк\w*|снижена\s*цена)\b",
    re.I
)
def _looks_like_code_value(v:str)->bool:
    s=(v or "").strip()
    if not s: return True
    if re.search(r"https?://",s,re.I): return True
    clean=re.sub(r"[0-9\-\_/ ]","",s)
    return (len(clean)/max(len(s),1))<0.3

# ===== Available (→ tag) =====
TRUE_WORDS  = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS = {"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}
def _parse_bool_str(s: str) -> Optional[bool]:
    v=_norm_text(s)
    if v in TRUE_WORDS: return True
    if v in FALSE_WORDS: return False
    return None
def _parse_int(s: str) -> Optional[int]:
    t=re.sub(r"[^\d\-]+","", s or "")
    if t in {"","-","+"}: return None
    try: return int(t)
    except Exception: return None
def derive_available(offer: ET.Element) -> bool:
    a=offer.find("available")
    if a is not None and a.text:
        b=_parse_bool_str(a.text)
        if b is not None: return b
    for tag in ("quantity_in_stock","quantity","stock","Stock"):
        for node in offer.findall(tag):
            val=_parse_int(node.text or "")
            if val is not None: return val>0
    for tag in ("status","Status"):
        node=offer.find(tag)
        if node is not None and node.text:
            b=_parse_bool_str(node.text)
            if b is not None: return b
    return False

# ===== Keywords =====
STOPWORDS={"для","и","или","с","на","в","к","по","под","от","до","из","без","при"}
TRANSLIT=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n",
                         "о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya"})
def translit_ru_en(s:str)->str: return "".join(ch.translate(TRANSLIT) if ch.lower() in "абвгдежзийклмнопрстуфхцчшщыэюя" else ch for ch in s.lower())
GEO_TOKENS=["Казахстан","Алматы","Астана","Шымкент","Караганда"]

# ===== One-pass per offer =====
DESIRED_ORDER=["vendorCode","name","price","picture","vendor","currencyId","description"]  # available пойдёт «в хвост»

def transform_offer(offer: ET.Element) -> None:
    # 1) price
    dealer, _ = pick_dealer_price(offer)
    if dealer and dealer>100:
        rp=compute_retail(dealer)
        if rp:
            p=offer.find("price"); p = p if p is not None else ET.SubElement(offer,"price")
            p.text=str(int(rp))
    # чистка внутренних цен
    remove_all(offer, *INTERNAL_PRICE_TAGS)

    # 2) available → ТЕГ; убрать атрибут и складские поля
    for n in list(offer.findall("available")): offer.remove(n)
    offer.attrib.pop("available", None)
    ET.SubElement(offer, "available").text = ("true" if derive_available(offer) else "false")
    remove_all(offer, "quantity_in_stock","quantity","stock","Stock")

    # 3) params → build specs + drop unwanted
    spec_lines=[]; seen=set()
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        nm=(p.attrib.get("name") or "").strip()
        val=(p.text or "").strip()
        if not nm or not val: offer.remove(p); continue
        if UNWANTED_PARAM_NAME_RE.match(nm): offer.remove(p); continue
        if _looks_like_code_value(val): offer.remove(p); continue
        k=re.sub(r"\s+"," ",nm.lower())
        if k in seen: offer.remove(p); continue
        seen.add(k); spec_lines.append(f"- {nm}: {val}")
        offer.remove(p)
    if spec_lines:
        d=offer.find("description")
        cur=(d.text or "").strip() if (d is not None and d.text) else ""
        block="Характеристики:\n"+"\n".join(spec_lines)
        new=(cur+"\n\n"+block).strip() if cur else block
        if d is None: d=ET.SubElement(offer,"description")
        d.text=new
    # подчистка строк-«кв» в описании (только чёрный список)
    d=offer.find("description")
    if d is not None and (d.text or "").strip():
        kept=[ln for ln in (d.text or "").splitlines() if not BAD_LINE_START.search(ln)]
        d.text="\n".join(kept).strip()

    # 4) currency → KZT (жёстко, как в v34)
    remove_all(offer,"currencyId")
    ET.SubElement(offer,"currencyId").text="KZT"

    # 5) purge прочего мусора
    for t in PURGE_TAGS_AFTER: remove_all(offer, t)
    for a in PURGE_OFFER_ATTRS_AFTER: offer.attrib.pop(a, None)

    # 6) reorder children
    kids=list(offer); buckets={k:[] for k in DESIRED_ORDER}; others=[]
    for n in kids:
        (buckets[n.tag] if n.tag in buckets else others).append(n)
    rebuilt=[]; [rebuilt.extend(buckets[k]) for k in DESIRED_ORDER]; rebuilt.extend(others)
    if rebuilt!=kids:
        for n in kids: offer.remove(n)
        for n in rebuilt: offer.append(n)

    # 7) categoryId первым = 0
    remove_all(offer,"categoryId","CategoryId")
    offer.insert(0, ET.Element("categoryId", {})); offer[0].text = os.getenv("CATEGORY_ID_DEFAULT","0")

    # 8) keywords (если авто)
    if SATU_MODE=="full" and SATU_KEYWORDS=="auto":
        if offer.find("keywords") is None or not (offer.find("keywords").text or "").strip():
            name=get_text(offer,"name"); vendor=get_text(offer,"vendor"); vc=get_text(offer,"vendorCode")
            tokens=[]
            for part in (vendor, vc, name):
                for t in re.split(r"[,\s\|\-/]+",(part or "")):
                    t=t.strip()
                    if len(t)<2: continue
                    if _norm_text(t) in STOPWORDS: continue
                    tokens.append(t)
            uniq=[]; seen=set()
            for t in tokens:
                k=_norm_text(t)
                if k in seen: continue
                seen.add(k); uniq.append(t)
            extra=[translit_ru_en(t) for t in uniq if not re.fullmatch(r"[0-9\-]+",t) and translit_ru_en(t)!=t.lower()]
            geo=GEO_TOKENS if SATU_KEYWORDS_GEO else []
            out=[]
            for t in uniq+extra+geo:
                if not t: continue
                cand=", ".join(out+[t])
                if len(out)<SATU_KEYWORDS_MAXWORDS and len(cand)<=SATU_KEYWORDS_MAXLEN:
                    out.append(t)
            if out: ET.SubElement(offer,"keywords").text=", ".join(out)

# ===== FEED_META =====
def feed_meta_comment(pairs:Dict[str,str])->str:
    try: now_alm=datetime.now(ZoneInfo("Asia/Almaty"))
    except Exception: now_alm=datetime.utcfromtimestamp(time.time()+5*3600)
    fmt=lambda dt: dt.strftime("%d:%m:%Y - %H:%M:%S")
    lines=[
        "FEED_META",
        f"Поставщик              | {pairs.get('supplier','')}",
        f"URL поставщика         | {pairs.get('source','')}",
        f"Время сборки (Алматы)  | {fmt(now_alm)}",
        f"Сколько товаров у поставщика до фильтра | {pairs.get('offers_total','0')}",
        f"Сколько товаров у поставщика после фильтра | {pairs.get('offers_written','0')}",
        f"Сколько товаров есть в наличии (true) | {pairs.get('available_true','0')}",
        f"Сколько товаров нет в наличии (false) | {pairs.get('available_false','0')}",
    ]
    return "\n".join(lines)

# ===== MAIN =====
def main()->None:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")
    offers_in_el=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None: err("XML: <offers> not found")

    # фильтр категорий (как в v34)
    src_offers=list(offers_in_el.findall("offer"))
    id2name,id2parent,tree = parse_categories_tree(shop_in)
    rules_ids, rules_names = (set(),[])
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH)
        if ALSTYLE_CATEGORIES_MODE=="include" and not (rules_ids or rules_names):
            err("ALSTYLE_CATEGORIES_MODE=include, но правил нет.", 2)

    keep_ids: Set[str]=set(rules_ids)
    if rules_names and id2name:
        for cid in id2name.keys():
            path=build_category_path_from_id(cid,id2name,id2parent)
            if category_matches_name(path, rules_names): keep_ids.add(cid)
    if keep_ids and tree: keep_ids=descendants(keep_ids, tree)

    # сборка
    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")

    av_true=av_false=0
    for o in src_offers:
        # фильтр по категориям
        if ALSTYLE_CATEGORIES_MODE in {"include","exclude"} and (rules_ids or rules_names):
            cid=get_text(o,"categoryId"); hit=(cid in keep_ids) if cid else False
            drop=(ALSTYLE_CATEGORIES_MODE=="exclude" and hit) or (ALSTYLE_CATEGORIES_MODE=="include" and not hit)
            if drop: continue

        offer=ET.fromstring(ET.tostring(o, encoding="utf-8"))  # deepcopy
        transform_offer(offer)
        # статистика доступности (после transform_offer у нас тег available)
        b = (offer.find("available").text or "").strip().lower()=="true"
        if b: av_true+=1
        else: av_false+=1
        out_offers.append(offer)

    # форматирование и запись
    try: ET.indent(out_root, space="  ")
    except Exception: pass
    pairs={"supplier":SUPPLIER_NAME,"source":SUPPLIER_URL or "file","offers_total":len(src_offers),
           "offers_written":len(list(out_offers.findall("offer"))),
           "available_true":av_true,"available_false":av_false}
    out_root.insert(0, ET.Comment(feed_meta_comment(pairs)))

    xml_bytes=ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text = xml_bytes.decode(ENC, errors="replace").replace("\r\n","\n")
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML,"w",encoding=ENC,newline="\n") as f: f.write(xml_text)
    log(f"Wrote: {OUT_FILE_YML} | offers={pairs['offers_written']} | encoding={ENC} | script={SCRIPT_VERSION}")

if __name__=="__main__":
    main()
