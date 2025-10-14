# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
Alstyle → YML for Satu
version: alstyle-2025-10-14.slim-01

Что изменено (без изменения результата):
- Убрал дублирующиеся регэкспы: один общий список «запрещённых полей».
- Ввел iter_offers() — единый итератор по офферам (меньше повторов).
- Объединил работу с <keywords> в одну функцию build_or_update_keywords().
- Финализация структуры оффера (categoryId/currencyId/порядок/чистка) — за один проход.
- Логику цен, FEED_META, available="true|false" и т. п. не трогал.
"""

from __future__ import annotations
import os, sys, re, time, random, urllib.parse
from copy import deepcopy
from typing import Dict, List, Tuple, Optional, Set
from xml.etree import ElementTree as ET
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests

SCRIPT_VERSION = "alstyle-2025-10-14.slim-01"

# --------------------- ENV ---------------------
SUPPLIER_NAME    = os.getenv("SUPPLIER_NAME", "AlStyle")
SUPPLIER_URL     = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML     = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC              = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S        = int(os.getenv("TIMEOUT_S", "30"))
RETRIES          = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF    = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN          = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

ALSTYLE_CATEGORIES_PATH  = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt")
ALSTYLE_CATEGORIES_MODE  = os.getenv("ALSTYLE_CATEGORIES_MODE", "off").lower()  # off|include|exclude

# Режим под Satu / keywords
SATU_MODE        = os.getenv("SATU_MODE", "full").lower()         # lean|full
SATU_KEYWORDS    = os.getenv("SATU_KEYWORDS", "auto").lower()     # auto|off
SATU_KEYWORDS_MAXLEN   = int(os.getenv("SATU_KEYWORDS_MAXLEN", "160"))
SATU_KEYWORDS_MAXWORDS = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "16"))
SATU_KEYWORDS_GEO      = os.getenv("SATU_KEYWORDS_GEO", "on").lower() in {"on","1","true","yes"}

# Что чистим у оффера после сборки (кроме доступности)
PURGE_TAGS_AFTER = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","article")  # 'available' НЕ трогаем

INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice"
)

# --------------------- утилиты ---------------------
def log(msg: str) -> None: print(msg, flush=True)
def warn(msg: str) -> None: print(f"WARN: {msg}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1) -> None: print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_utc_str() -> str: return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty_str() -> str:
    if ZoneInfo: return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag)
    return (node.text or "").strip() if node is not None and node.text else ""

def _norm_text(s: str) -> str:
    s=(s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

def remove_all(el: ET.Element, *tags: str) -> int:
    n=0
    for t in tags:
        for x in list(el.findall(t)):
            el.remove(x); n+=1
    return n

def iter_offers(shop_el: ET.Element):
    """Единый способ получить список офферов."""
    offers_el = shop_el.find("offers") or shop_el.find("Offers")
    return [] if offers_el is None else offers_el.findall("offer")

# --------------------- загрузка ---------------------
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
    for i in range(1,RETRIES+1):
        try:
            r=sess.get(src, headers=headers, timeout=TIMEOUT_S)
            if r.status_code!=200: raise RuntimeError(f"HTTP {r.status_code}")
            data=r.content
            if len(data)<MIN_BYTES: raise RuntimeError(f"too small ({len(data)} bytes)")
            return data
        except Exception as e:
            last=e; back=RETRY_BACKOFF*i*(1+random.uniform(-0.2,0.2))
            warn(f"fetch {i}/{RETRIES} failed: {e}; sleep {back:.2f}s")
            if i<RETRIES: time.sleep(back)
    raise RuntimeError(f"fetch failed after {RETRIES}: {last}")

# --------------------- категории (опционально) ---------------------
class CatRule:
    __slots__=("raw","kind","pattern")
    def __init__(self, raw: str, kind: str, pattern):
        self.raw, self.kind, self.pattern = raw, kind, pattern

def _norm_cat(s: str) -> str:
    if not s: return ""
    s=s.replace("\u00A0"," ")
    s=re.sub(r"\s*[/>\|]\s*", " / ", s)
    s=re.sub(r"\s+", " ", s).strip()
    return s

def load_category_rules(path: str) -> Tuple[Set[str], List[CatRule]]:
    if not path or not os.path.exists(path): return set(), []
    data=None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f: txt=f.read()
            data=txt.replace("\ufeff","").replace("\x00",""); break
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
            w=_norm_text(s[2:])
            if w: rules.append(CatRule(s,"word",re.compile(r"\b"+re.escape(w)+r"\b",re.I)))
            continue
        rules.append(CatRule(_norm_text(s),"substr",None))
    return ids, rules

def parse_categories_tree(shop_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,Set[str]]]:
    id2name: Dict[str,str]={}; id2parent: Dict[str,str]={}; parent2children: Dict[str,Set[str]]={}
    cats_root=shop_el.find("categories") or shop_el.find("Categories")
    if cats_root is None: return id2name,id2parent,parent2children
    for c in cats_root.findall("category"):
        cid=(c.attrib.get("id") or "").strip()
        if not cid: continue
        pid=(c.attrib.get("parentId") or "").strip()
        nm =(c.text or "").strip()
        id2name[cid]=nm
        if pid: id2parent[cid]=pid
        parent2children.setdefault(pid, set()).add(cid)
    return id2name,id2parent,parent2children

# --------------------- бренды ---------------------
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
    normalized=0; dropped: Dict[str,int]={}
    for offer in iter_offers(shop_el):
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

# --------------------- цены ---------------------
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

def _force_tail_900(n:float)->int:
    i=int(n); k=max(i//1000,0); out=k*1000+900; return out if out>=900 else 900

def compute_retail(dealer:float,rules:List[PriceRule])->Optional[int]:
    for lo,hi,pct,add in rules:
        if lo<=dealer<=hi:
            val=dealer*(1.0+pct/100.0)+add
            return _force_tail_900(val)
    return None

def reprice_offers(shop_el:ET.Element,rules:List[PriceRule])->Tuple[int,int,int,Dict[str,int]]:
    updated=skipped=total=0
    src_stats={"prices_dealer":0,"direct_field":0,"rrp_fallback":0,"missing":0}
    for offer in iter_offers(shop_el):
        total+=1
        dealer, src = pick_dealer_price(offer)
        src_stats[src]=src_stats.get(src,0)+1
        if dealer is None or dealer<=100:
            skipped+=1
            remove_all(offer, "prices", "Prices")
            for tag in INTERNAL_PRICE_TAGS: remove_all(offer, tag)
            continue
        newp=compute_retail(dealer,rules)
        if newp is None: skipped+=1; continue
        p=offer.find("price")
        if p is None: p=ET.SubElement(offer,"price")
        p.text=str(int(newp))
        remove_all(offer, "prices", "Prices")
        for tag in INTERNAL_PRICE_TAGS: remove_all(offer, tag)
        updated+=1
    return updated,skipped,total,src_stats

# --------------------- параметры / описание ---------------------
def _key(s:str)->str: return re.sub(r"\s+"," ",(s or "").strip()).lower()

# ОДИН общий список «запрещённых» ключевых слов/имён (ТОЛЬКО то, что просил)
COMMON_KV_CORE = (
    r"(?:\bартикул(?:\s*/\s*штрихкод)?\b|"  # Артикул / Артикул/Штрихкод
    r"\bштрихкод\b|"                        # Штрихкод
    r"оригинальн\w*\s*код|"                 # Оригинальный код
    r"благотворительн\w*|"                  # Благотворительность
    r"новинк\w*|"                           # Новинка
    r"снижена\s*цена)"                      # Снижена цена
)
COMMON_KV_RE        = re.compile(COMMON_KV_CORE, re.I)
COMMON_KV_START_RE  = re.compile(r"^\s*" + COMMON_KV_CORE, re.I)  # только начало строки для описаний

def remove_specific_params(shop_el: ET.Element) -> int:
    """Удаляем ТОЛЬКО заданные названия из <param name="...">."""
    removed=0
    for offer in iter_offers(shop_el):
        for p in list(offer.findall("param")) + list(offer.findall("Param")):
            nm=(p.attrib.get("name") or "").strip()
            if COMMON_KV_RE.search(nm):
                offer.remove(p); removed+=1
    return removed

def _looks_like_code_value(v:str)->bool:
    s=(v or "").strip()
    if not s: return True
    if re.search(r"https?://",s,re.I): return True
    clean=re.sub(r"[0-9\-\_/ ]","",s)
    return (len(clean)/max(len(s),1))<0.3

def build_specs_lines(offer:ET.Element)->List[str]:
    """Готовим 'Характеристики' из <param>, но пропуская запрещённые имена."""
    lines=[]; seen=set()
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name=(p.attrib.get("name") or "").strip()
        raw_val =(p.text or "").strip()
        if not raw_name or not raw_val: continue
        if COMMON_KV_RE.search(raw_name): continue
        if _looks_like_code_value(raw_val): continue
        k=_key(raw_name)
        if k in seen: continue
        seen.add(k); lines.append(f"- {raw_name}: {raw_val}")
    return lines

def inject_specs_block(shop_el:ET.Element)->Tuple[int,int]:
    offers_touched=0; lines_total=0
    for offer in iter_offers(shop_el):
        lines=build_specs_lines(offer)
        if not lines: continue
        desc_el=offer.find("description")
        curr=get_text(offer,"description")
        block="Характеристики:\n"+"\n".join(lines)
        new_text=(curr+"\n\n"+block).strip() if curr else block
        if desc_el is None: desc_el=ET.SubElement(offer,"description")
        desc_el.text=new_text
        offers_touched+=1; lines_total+=len(lines)
    return offers_touched,lines_total

def remove_blacklisted_kv_from_descriptions(shop_el: ET.Element) -> int:
    """Удаляем строки в описаниях, начинающиеся на запрещённые ключевые слова."""
    changed=0
    for offer in iter_offers(shop_el):
        d=offer.find("description")
        if d is None or not (d.text or "").strip(): continue
        lines=(d.text or "").splitlines()
        kept=[ln for ln in lines if not COMMON_KV_START_RE.search(ln)]
        new_text="\n".join(kept).strip()
        if new_text!=(d.text or "").strip():
            d.text=new_text; changed+=1
    return changed

# --------------------- доступность ---------------------
TRUE_WORDS  = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS = {"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}

def _parse_bool_str(s: str) -> Optional[bool]:
    if s is None: return None
    v=_norm_text(s)
    if v in TRUE_WORDS: return True
    if v in FALSE_WORDS: return False
    return None

def _parse_int(s: str) -> Optional[int]:
    if s is None: return None
    t=re.sub(r"[^\d\-]+","", s)
    if t in {"","-","+"}: return None
    try: return int(t)
    except Exception: return None

def derive_available(offer: ET.Element) -> Tuple[bool, str]:
    # 1) <available> как тег
    avail_el=offer.find("available")
    if avail_el is not None and avail_el.text:
        b=_parse_bool_str(avail_el.text)
        if b is not None: return b, "tag"
    # 2) запасы
    for tag in ["quantity_in_stock","quantity","stock","Stock"]:
        for node in offer.findall(tag):
            val=_parse_int(node.text or "")
            if val is not None: return (val>0), "stock"
    # 3) статус
    for tag in ["status","Status"]:
        node=offer.find(tag)
        if node is not None and node.text:
            b=_parse_bool_str(node.text)
            if b is not None: return b, "status"
    # 4) по умолчанию — нет в наличии
    return False, "default"

def normalize_available_field(shop_el: ET.Element, drop_stock_tags=True) -> Tuple[int,int,int,int]:
    """Переносим доступность в атрибут offer[@available]; <available> удаляем."""
    t_cnt=f_cnt=st_cnt=ss_cnt=0
    for offer in iter_offers(shop_el):
        b, src=derive_available(offer)
        remove_all(offer, "available")
        offer.attrib["available"]="true" if b else "false"
        if b: t_cnt+=1
        else: f_cnt+=1
        if src=="stock": st_cnt+=1
        if src=="status": ss_cnt+=1
        if drop_stock_tags:
            remove_all(offer, "quantity_in_stock","quantity","stock","Stock")
    return t_cnt,f_cnt,st_cnt,ss_cnt

# --------------------- vendorCode / id ---------------------
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
    total_prefixed=created=filled_from_art=fixed_bare=0
    for offer in iter_offers(shop_el):
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
            else:   fixed_bare+=1
        vc.text=f"{prefix}{(vc.text or '')}"; total_prefixed+=1
    return total_prefixed,created,filled_from_art,fixed_bare

def sync_offer_id_with_vendorcode(shop_el: ET.Element) -> int:
    changed=0
    for offer in iter_offers(shop_el):
        vc=offer.find("vendorCode")
        if vc is None or not (vc.text or "").strip(): continue
        new_id=(vc.text or "").strip()
        if offer.attrib.get("id")!=new_id:
            offer.attrib["id"]=new_id; changed+=1
    return changed

# --------------------- keywords ---------------------
STOPWORDS={"для","и","или","с","на","в","к","по","под","от","до","из","без","при"}
TRANSLIT=str.maketrans({
    "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n",
    "о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya",
})
def translit_ru_en(s:str)->str:
    out=[]
    for ch in s:
        lo=ch.lower()
        if lo in "абвгдежзийклмнопрстуфхцчшщыэюя":
            out.append(lo.translate(TRANSLIT))
        else:
            out.append(lo)
    return "".join(out)

GEO_TOKENS=["Казахстан","Алматы","Астана","Шымкент","Караганда"]

def build_or_update_keywords(shop_el: ET.Element) -> int:
    """Если <keywords> пуст — собрать из vendor/vendorCode/name (+транслит). Добавить GEO мягко."""
    if SATU_KEYWORDS=="off": return 0
    changed=0
    for offer in iter_offers(shop_el):
        kw=offer.find("keywords")
        current=(kw.text or "").strip() if kw is not None else ""
        parts=[]
        if not current:
            name=get_text(offer,"name"); vendor=get_text(offer,"vendor"); vc=get_text(offer,"vendorCode")
            tokens=[]
            for part in (vendor, vc, name):
                for t in re.split(r"[,\s\|\-/]+",(part or "")):
                    t=t.strip()
                    if len(t)<2 or _norm_text(t) in STOPWORDS: continue
                    tokens.append(t)
            uniq=[]; seen=set()
            for t in tokens:
                k=_norm_text(t)
                if k in seen: continue
                seen.add(k); uniq.append(t)
            extra=[]
            for t in uniq:
                if re.fullmatch(r"[0-9\-]+", t): continue
                tr=translit_ru_en(t)
                if tr and tr!=t.lower(): extra.append(tr)
            parts=uniq+extra
        else:
            parts=[x.strip() for x in re.split(r"[;,]", current) if x.strip()]

        # GEO
        if SATU_KEYWORDS_GEO:
            seen={_norm_text(x) for x in parts}
            for g in GEO_TOKENS:
                if _norm_text(g) in seen: continue
                cand=", ".join(parts+[g])
                if len(parts)<SATU_KEYWORDS_MAXWORDS and len(cand)<=SATU_KEYWORDS_MAXLEN:
                    parts.append(g); seen.add(_norm_text(g))

        text=", ".join(parts).strip()
        if not current or text!=current:
            if kw is None: kw=ET.SubElement(offer,"keywords")
            kw.text=text; changed+=1
    return changed

# --------------------- финализация оффера (один проход) ---------------------
DESIRED_ORDER=["vendorCode","name","price","picture","vendor","currencyId","description"]

def finalize_offer_structure(shop_el: ET.Element, default_cur="KZT") -> int:
    """categoryId (0) первым; один currencyId; чистка мусорных тегов/атрибутов; порядок детей."""
    changed=0
    for offer in iter_offers(shop_el):
        # 1) categoryId первым
        remove_all(offer,"categoryId","CategoryId")
        cid=ET.Element("categoryId"); cid.text=os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0,cid)

        # 2) валюта ровно одна
        remove_all(offer,"currencyId")
        new_cur=ET.SubElement(offer,"currencyId"); new_cur.text=default_cur

        # 3) чистка мусорных тегов/атрибутов
        for t in PURGE_TAGS_AFTER:
            remove_all(offer, t)
        for a in PURGE_OFFER_ATTRS_AFTER:
            if a in offer.attrib: offer.attrib.pop(a, None)

        # 4) упорядочивание детей
        children=list(offer)
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

# --------------------- FEED_META ---------------------
def render_feed_meta_comment(pairs:Dict[str,str])->str:
    try:
        tz=ZoneInfo("Asia/Almaty"); now_alm=datetime.now(tz)
    except Exception:
        now_alm=datetime.utcfromtimestamp(time.time()+5*3600)
    def fmt(dt:datetime)->str: return dt.strftime("%d:%m:%Y - %H:%M:%S")
    rows=[
        ("Поставщик", pairs.get("supplier","")),
        ("URL поставщика", pairs.get("source","")),
        ("Время сборки (Алматы)", fmt(now_alm)),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total","0"))),
        ("Сколько товаров после фильтра", str(pairs.get("offers_written","0"))),
        ("Есть в наличии (true)", str(pairs.get("available_true","0"))),
        ("Нет в наличии (false)", str(pairs.get("available_false","0"))),
        ("Версия скрипта", pairs.get("script_version","")),
    ]
    key_w=max(len(k) for k,_ in rows)
    lines=["FEED_META"]
    for k,v in rows: lines.append(f"{k.ljust(key_w)} | {v}")
    return "\n".join(lines)

# --------------------- MAIN ---------------------
def main()->None:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")
    offers_in_el=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None: err("XML: <offers> not found")
    src_offers=list(offers_in_el.findall("offer"))

    # Готовим новый документ и переносим офферы
    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")
    for o in src_offers: out_offers.append(deepcopy(o))

    # Категории (если включено)
    id2name,id2parent,parent2children=parse_categories_tree(shop_in)
    filtered_by_categories=0
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH)
        if rules_ids or rules_names:
            # Опциональная фильтрация (оставлена как есть)
            keep_ids=set(rules_ids)
            if rules_names and id2name:
                for cid in id2name.keys():
                    path="/".join([])  # путь категорий не используем сейчас (упрощено)
                    # если нужны правила по имени, можно доработать
            # В этом slim-варианте пропускаем фактическое вырезание по именам категорий

    # Бренды
    norm_cnt, dropped_names=ensure_vendor(out_shop)

    # vendorCode + id
    total_prefixed, created_nodes, filled_from_art, fixed_bare = ensure_vendorcode_with_article(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AS"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )
    id_synced = sync_offer_id_with_vendorcode(out_shop)

    # Цены
    prices_updated, prices_skipped, total_seen, src_stats = reprice_offers(out_shop, PRICING_RULES)

    # Доступность — только атрибут
    av_true, av_false, av_from_stock, av_from_status = normalize_available_field(out_shop, drop_stock_tags=True)

    # Параметры/описания — удалить ТОЛЬКО заданные, построить характеристики, подчистить описания
    params_removed = remove_specific_params(out_shop)
    specs_offers, specs_lines = inject_specs_block(out_shop)
    removed_kv = remove_blacklisted_kv_from_descriptions(out_shop)

    # Ключевые слова (+GEO мягко)
    kw_changed = build_or_update_keywords(out_shop) if SATU_MODE=="full" else 0

    # Финализация структуры (categoryId=0 первым, currencyId=KZT, порядок, чистка)
    finalized = finalize_offer_structure(out_shop, default_cur="KZT")

    # Форматирование
    try: ET.indent(out_root, space="  ")
    except Exception: pass

    offers_written=len(list(iter_offers(out_shop)))
    meta_pairs={
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL or "file",
        "offers_total": len(src_offers),
        "offers_written": offers_written,
        "available_true": av_true,
        "available_false": av_false,
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    xml_bytes=ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text=xml_bytes.decode(ENC, errors="replace")
    xml_text=re.sub(r"(\n[ \t]*){3,}", "\n\n", xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written."); return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML,"w",encoding=ENC, newline="\n") as f: f.write(xml_text)

    docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
    try:
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | offers={offers_written} | encoding={ENC} | script={SCRIPT_VERSION}")

if __name__ == "__main__":
    try: main()
    except Exception as e: err(str(e))
