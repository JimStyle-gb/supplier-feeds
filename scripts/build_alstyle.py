# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""AlStyle → YML (вариант A): HTML-оформление <description> для Satu.
— Фото НЕ влияет на available.
— FEED_META не трогаем.
— В <description> вставляем HTML с заголовком <h3>ИМЯ ТОВАРА</h3>,
  далее «Описание» (как есть) и «Характеристики» списком <ul><li>, где ключи жирные (<strong>Ключ:</strong> Значение).
— Текст НЕ редактируем по смыслу: только структура и теги.
"""

from __future__ import annotations
import os, sys, re, time, random, urllib.parse, requests
from copy import deepcopy
from typing import Dict, List, Tuple, Optional, Set
from xml.etree import ElementTree as ET
from datetime import datetime, timezone
from html import unescape as _unescape
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

SCRIPT_VERSION = "alstyle-2025-10-20.A-html-desc"

# --- ENV ---
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "AlStyle")
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251")
TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

ALSTYLE_CATEGORIES_PATH = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt")
ALSTYLE_CATEGORIES_MODE = os.getenv("ALSTYLE_CATEGORIES_MODE", "off").lower()  # off|include|exclude

# KEYWORDS (под Satu)
SATU_KEYWORDS          = os.getenv("SATU_KEYWORDS", "auto").lower()   # auto|off
SATU_KEYWORDS_MAXLEN   = int(os.getenv("SATU_KEYWORDS_MAXLEN", "1024"))
SATU_KEYWORDS_MAXWORDS = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "1000"))
SATU_KEYWORDS_GEO      = os.getenv("SATU_KEYWORDS_GEO", "on").lower() in {"on","1","true","yes"}
SATU_KEYWORDS_GEO_MAX  = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))
SATU_KEYWORDS_GEO_LAT  = os.getenv("SATU_KEYWORDS_GEO_LAT", "on").lower() in {"on","1","true","yes"}

# PRICE CAP
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "100"))

DROP_CATEGORY_ID_TAG   = True
DROP_STOCK_TAGS        = True
PURGE_TAGS_AFTER       = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER= ("type","article")
INTERNAL_PRICE_TAGS    = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                          "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice",
                          "max_price","maxPrice","oldprice")

# --- утилиты ---
log  = lambda m: print(m, flush=True)
warn = lambda m: print(f"WARN: {m}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)
now_utc_str    = lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty_str():
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

def _remove_all_price_nodes(offer: ET.Element):
    for t in ("price", "Price"):
        for node in list(offer.findall(t)): offer.remove(node)

def strip_supplier_price_blocks(offer: ET.Element):
    remove_all(offer, "prices", "Prices")
    for tag in INTERNAL_PRICE_TAGS: remove_all(offer, tag)

_COLON_CLASS_RE = re.compile("[:\uFF1A\uFE55\u2236\uFE30]")
canon_colons    = lambda s: _COLON_CLASS_RE.sub(":", s or "")

NOISE_RE = re.compile(r"[\u200B-\u200F\u202A-\u202E\u2060\uFEFF\u00AD\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F\u0080-\u009F]")
def strip_noise_chars(s: str) -> str:
    if not s: return ""
    return NOISE_RE.sub("", s).replace("�","").replace("¬","").replace("•","-")

# --- загрузка исходника ---
def load_source_bytes(src: str) -> bytes:
    if not src: raise RuntimeError("SUPPLIER_URL не задан")
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path, "rb") as f: data=f.read()
        if len(data) < MIN_BYTES: raise RuntimeError(f"file too small: {len(data)}")
        return data
    sess=requests.Session(); headers={"User-Agent":"supplier-feed-bot/1.0 (+github-actions)"}
    last=None
    for i in range(1, 4+1):
        try:
            r=sess.get(src, headers=headers, timeout=TIMEOUT_S)
            if r.status_code!=200: raise RuntimeError(f"HTTP {r.status_code}")
            data=r.content
            if len(data)<MIN_BYTES: raise RuntimeError(f"too small ({len(data)})")
            return data
        except Exception as e:
            last=e; back=RETRY_BACKOFF*i*(1+random.uniform(-0.2,0.2))
            warn(f"fetch {i}/4 failed: {e}; sleep {back:.2f}s")
            if i<4: time.sleep(back)
    raise RuntimeError(f"fetch failed: {last}")

# --- категории ---
class CatRule:
    __slots__=("raw","kind","pattern")
    def __init__(self, raw: str, kind: str, pattern): self.raw, self.kind, self.pattern = raw, kind, pattern

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

# --- бренды ---
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
    if not nrm: return ""
    first = re.split(r"\s+", name.strip())[0]; f_norm=_norm_key(first)
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

# --- цены ---
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
        if lo<=dealer<=hi:
            return _force_tail_900(dealer*(1.0+pct/100.0)+add)
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

# --- params / description (минимальные изменения, как и раньше) ---
_key=lambda s: re.sub(r"\s+"," ",(s or "").strip()).lower()
UNWANTED_PARAM_NAME_RE = re.compile(
    r"^(?:\s*(?:благотворительн\w*|снижена\s*цена|новинк\w*|артикул(?:\s*/\s*штрихкод)?|оригинальн\w*\s*код|"
    r"штрихкод|код\s*тн\s*вэд(?:\s*eaeu)?|код\s*тнвэд(?:\s*eaeu)?|тн\s*вэд|тнвэд|tn\s*ved|hs\s*code)\s*)$",
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
                nm=(p.attrib.get("name") or "").strip(); val=(p.text or "").strip()
                if KASPI_CODE_NAME_RE.fullmatch(nm) or (re.fullmatch(r"назначение", nm, re.I) and re.fullmatch(r"да", val, re.I)) or UNWANTED_PARAM_NAME_RE.match(nm):
                    offer.remove(p); removed+=1; continue
                if (val.lower() in {"","-","—","–",".","..","...","n/a","na","none","null","нет данных","не указано","неизвестно"}
                    or re.search(r"https?://|www\.", val, re.I) or re.search(r"[^@\s]+@[^@\s]+\.[^@\s]+", val) or re.search(r"<[^>]+>", val)):
                    offer.remove(p); removed+=1; continue
                k=_key(nm)
                if k in seen: offer.remove(p); removed+=1; continue
                seen.add(k)
    return removed

EXCLUDE_NAME_RE = re.compile(r"(?:\bартикул\b|благотворительн\w*|штрихкод|оригинальн\w*\s*код|новинк\w*|снижена\s*цена|код\s*тн\s*вэд(?:\s*eaeu)?|код\s*тнвэд(?:\s*eaeu)?|тн\s*вэд|тнвэд|tn\s*ved|hs\s*code)", re.I)
def _looks_like_code_value(v:str)->bool:
    s=(v or "").strip()
    if not s or re.search(r"https?://",s,re.I): return True
    clean=re.sub(r"[0-9\-\_/ ]","",s)
    return (len(clean)/max(len(s),1))<0.3

DISCL_RE = re.compile(r"(голографическ\w*|маркиров\w*|на\s+корпусе|на\s+коробке|на\s+упаковке|медиа\s*файл\w*|фото|видео|предъяв\w*|обнаруж\w*\s+брак|серии\s+картриджа|услов\w*\s+гаран|следует|необходим\w*)", re.I)
HEADLINE_EUROPRINT_RE  = re.compile(r"^(?:тонер-)?картриджи\s+europrint\s*:?\s*$", re.I)
HEADLINE_WARRANTY_RE   = re.compile(r"^условия\s+гарантии\s*:?\s*$", re.I)
RE_SPACE_BEFORE_PUNCT = re.compile(r"\s+([,;.!?])")
RE_PERCENT            = re.compile(r"(\d)\s*%")
RE_DEGREE_C           = re.compile(r"(\d)\s*°\s*([СC])")

def _fix_degrees(s: str) -> str: return RE_DEGREE_C.sub(r"\1°\2", s or "")
def normalize_value_punct(v: str) -> str:
    s=canon_colons(v or "")
    s=re.sub(r":\s*:", ": ", s); s=re.sub(r"(?<!\.)\.\.(?!\.)", ".", s)
    s=RE_SPACE_BEFORE_PUNCT.sub(r"\1", s); s=RE_PERCENT.sub(r"\1%", s)
    s=_fix_degrees(s); s=re.sub(r"\s{2,}", " ", s)
    return s.strip()

def normalize_free_text_punct(s: str) -> str:
    t=canon_colons(s or "")
    t=re.sub(r":\s*:", ": ", t); t=re.sub(r"(?<!\.)\.\.(?!\.)", ".", t)
    t=RE_SPACE_BEFORE_PUNCT.sub(r"\1", t); t=RE_PERCENT.sub(r"\1%", t)
    t=_fix_degrees(t); t=re.sub(r"\s{2,}", " ", t)
    return re.sub(r"\bдля дома\s+офиса\b", "для дома и офиса", t, flags=re.I).strip()

HDR_RE = re.compile(r"^\s*(технические\s+характеристики|характеристики)\s*:?\s*$", re.I)
HEAD_ONLY_RE = re.compile(r"^\s*(?:основные\s+)?характеристики\s*[:：﹕∶︰-]*\s*$", re.I)
HEAD_PREFIX_RE = re.compile(r"^\s*(?:основные\s+)?характеристики\s*[:：﹕∶︰-]*\s*", re.I)
KV_BULLET_RE = re.compile(r"^\s*-\s*([^:]+?)\s*:\s*(.+)$"); KV_COLON_RE  = re.compile(r"^\s*([^:]{2,}?)\s*:\s*(.+)$")
KV_TABS_RE   = re.compile(r"^\s*([^\t]{2,}?)\t+(.+)$");     KV_SPACES_RE = re.compile(r"^\s*(\S.{1,}?)\s{2,}(.+)$")
BULLET_NO_KEY=re.compile(r"^\s*-\s*[:\s\-]{0,5}:\s*\S");     BULLET_KEY_NOVALUE=re.compile(r"^\s*-\s*[^:]+:\s*$")
URL_RE       = re.compile(r"https?://\S+", re.I)
BAD_KV_NAME_RE = re.compile(r"(?:\bв\s+случае\b|\bуслов\w*\s+гаран|необходим\w*|следует|предостав\w*|предъяв\w*|указан\w*|содержит|соответству\w*|упаков\w*|маркиров\w*|голографическ\w*|на\s+корпусе|на\s+упаковке|на\s+коробке|фото|видео)", re.I)
def _valid_kv_name(name: str) -> bool:
    n=(name or "").strip(" -•*·").strip()
    if not n or len(n)>48 or len(re.findall(r"[A-Za-zА-Яа-я0-9%°\-\+]+", n))>6: return False
    return not (BAD_KV_NAME_RE.search(n) or re.search(r"[.!?]$", n) or n.count(",")>=2)

def _merge_europrint_headings(lines: List[str]) -> List[str]:
    out=[]; i=0
    while i < len(lines):
        cur=(lines[i] or "").strip(); nxt=(lines[i+1] or "").strip() if i+1<len(lines) else ""
        if HEADLINE_EUROPRINT_RE.match(cur): 
            if nxt: out.append("Картриджи EUROPRINT. "+nxt.lstrip("—-• ").strip()); i+=2; continue
            i+=1; continue
        if HEADLINE_WARRANTY_RE.match(cur):
            if nxt: out.append(nxt); i+=2; continue
            i+=1; continue
        out.append(cur); i+=1
    return out

def _split_inline_bullets(line: str) -> List[str]:
    s=line.strip()
    if not s: return []
    if ":" not in s: return [s]
    parts=re.split(r"\s*-\s+(?=[^:]{2,}:\s*\S)", s)
    out=[]
    for p in parts:
        p=p.strip()
        if p: out.append(re.sub(r"^\s*-\s*", "", p))
    return out or [s]

def _normalize_description_whitespace(text: str) -> str:
    t=strip_noise_chars(text or ""); t=canon_colons(t)
    t=t.replace("\r\n","\n").replace("\r","\n").replace("\u00A0"," "); t=re.sub(r"\t+","\t", t)
    raw=[re.sub(r"[ \t]+$", "", ln) for ln in t.split("\n")]
    raw=_merge_europrint_headings(raw)
    out=[]; last_blank=False
    for ln in raw:
        s=ln.strip()
        if not s:
            if not last_blank: out.append(""); last_blank=True; continue
        last_blank=False
        if DISCL_RE.search(s) and len(s)>60: continue
        out.append(s)
    t="\n".join(out).strip()
    t=re.sub(r":\s*:", ": ", t); t=re.sub(r"(?<!\.)\.\.(?!\.)", ".", t); t=re.sub(r"(\n){3,}", "\n\n", t)
    return t

def normalize_kv(name: str, value: str) -> Tuple[str, str]:
    n=re.sub(r"\s+"," ",(name or "").strip()); v=re.sub(r"\s+"," ",(value or "").strip())
    n=canon_colons(n); v=canon_colons(v); n=re.sub(rf"\s*{_COLON_CLASS_RE.pattern}+\s*$","",n); v=re.sub(rf"^\s*{_COLON_CLASS_RE.pattern}+\s*","",v)
    if not n or not v: return n, v
    n_l=n.lower()
    if re.search(r"совместим\w*\s*модел", n_l): n="Совместимость"
    elif re.search(r"ресурс", n_l):
        n="Ресурс картриджа"; m=re.search(r"(\d[\d\s]{0,12}\d)", v)
        iso = re.search(r"(?:iso|iec)[\/\s\-]*([12]\d{3,5})", v, re.I) or re.search(r"\b([12]\d{3,5})\b", v)
        if m:
            num=re.sub(r"\s+"," ", m.group(1)).strip()
            v=f"{num} стр." + (f" (ISO/IEC {iso.group(1)})" if iso else "")
    elif re.fullmatch(r"вес", n_l) and not re.search(r"\bкг\b", v, re.I):
        v=v.replace(",", ".").strip() + " кг"
    elif re.fullmatch(r"цвет", n_l):
        v=v.lower()
    return n, normalize_value_punct(v)

def extract_kv_from_description(text: str) -> Tuple[str, List[Tuple[str,str]], List[str]]:
    if not (text or "").strip(): return "", [], []
    t=_normalize_description_whitespace(text); raw_lines=t.split("\n"); lines=[]
    for ln in raw_lines:
        if URL_RE.search(ln) and not (KV_BULLET_RE.match(ln) or KV_COLON_RE.match(ln) or KV_TABS_RE.match(ln) or KV_SPACES_RE.match(ln)):
            ln=URL_RE.sub("", ln).strip()
        if HDR_RE.match(ln) or HEAD_ONLY_RE.match(ln): continue
        ln2=HEAD_PREFIX_RE.sub("", ln)
        if ln2.strip(): lines.extend(_split_inline_bullets(ln2))
    keep=[True]*len(lines); pairs=[]
    def try_parse_kv(ln: str)->Optional[Tuple[str,str]]:
        ln=canon_colons(ln); probe=re.sub(r"[ \t]{2,}", "  ", ln)
        for rx in (KV_BULLET_RE, KV_TABS_RE, KV_SPACES_RE, KV_COLON_RE):
            m=rx.match(probe)
            if m:
                name,val=(m.group(1) or "").strip(), (m.group(2) or "").strip()
                if _valid_kv_name(name) and name and val: return normalize_kv(name, val)
        return None
    for i, ln in enumerate(lines):
        if re.match(BULLET_NO_KEY, ln) or re.match(BULLET_KEY_NOVALUE, ln): keep[i]=False
        parsed=try_parse_kv(ln)
        if parsed: keep[i]=False; pairs.append(parsed)
    kept=[ln for ln,k in zip(lines,keep) if k and not (DISCL_RE.search(ln) and len(ln.strip())>60)]
    features=[s[2:].strip() for s in kept if s.strip().startswith("- ") and ":" not in s]
    rest=[ln for ln in kept if not (ln.strip().startswith("- ") and ":" not in ln)]
    cleaned="\n".join([x for x in rest if x.strip()]).strip()
    cleaned=re.sub(r"\n{3,}", "\n\n", cleaned)
    return normalize_free_text_punct(cleaned), pairs, features

def build_specs_pairs_from_params(offer: ET.Element) -> List[Tuple[str,str]]:
    pairs=[]; seen=set()
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name=(p.attrib.get("name") or "").strip(); raw_val =(p.text or "").strip()
        if not raw_name or not raw_val or EXCLUDE_NAME_RE.search(raw_name) or _looks_like_code_value(raw_val): continue
        name, val = normalize_kv(raw_name, raw_val); k=_key(name)
        if k in seen: continue
        seen.add(k); pairs.append((name, val))
    return pairs

def unify_specs_in_description(shop_el: ET.Element) -> Tuple[int,int,int]:
    """Как раньше: собираем «Описание», «Особенности» и «Характеристики» в текстовом виде.
       Дальше мы этот текст ПРЕОБРАЗУЕМ в HTML, сохранив слова 1:1."""
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0)
    offers_touched=0
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
        parts=[]
        if cleaned_desc: parts.append(cleaned_desc)
        if features:
            feats = [f"- {f}" for f in features]  # НЕ правим текст, только маркер
            parts.append("Особенности:\n" + "\n".join(feats))
        if merged:
            merged_pairs = list(merged.values()); merged_pairs.sort(key=lambda kv: _norm_text(kv[0]))
            lines = [f"- {n}: {v}" for n, v in merged_pairs]
            parts.append("Характеристики:\n" + "\n".join(lines))
        if parts:
            if desc_el is None: desc_el = ET.SubElement(offer, "description")
            desc_el.text = "\n\n".join(parts).strip(); offers_touched += 1
    return offers_touched, 0, 0

BAD_LINE_START = re.compile(r"^\s*(?:артикул(?:\s*/\s*штрихкод)?|оригинальн\w*\s*код|штрихкод|благотворительн\w*|новинк\w*|снижена\s*цена|код\s*тн\s*вэд(?:\s*eaeu)?|код\s*тнвэд(?:\s*eaeu)?|тн\s*вэд|тнвэд|tn\s*ved|hs\s*code)\b", re.I)
def remove_blacklisted_kv_from_descriptions(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    changed=0
    for offer in offers_el.findall("offer"):
        d=offer.find("description")
        if d is None or not (d.text or "").strip(): continue
        lines=(d.text or "").splitlines()
        kept=[ln for ln in lines if not (BAD_LINE_START.search(ln) or (DISCL_RE.search(ln) and len(ln.strip())>60))]
        new_text="\n".join(kept).strip()
        if new_text!=(d.text or "").strip(): d.text=new_text; changed+=1
    return changed

# --- availability ---
TRUE_WORDS={"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS={"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}
def _parse_bool_str(s: str)->Optional[bool]:
    v=_norm_text(s or "");  return True if v in TRUE_WORDS else False if v in FALSE_WORDS else None
def _parse_int(s: str)->Optional[int]:
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
    t_cnt=f_cnt=st_cnt=ss_cnt=0
    for offer in offers_el.findall("offer"):
        b, src=derive_available(offer)
        remove_all(offer, "available"); offer.attrib["available"]="true" if b else "false"
        if b: t_cnt+=1
        else: f_cnt+=1
        if src=="stock": st_cnt+=1
        if src=="status": ss_cnt+=1
        if DROP_STOCK_TAGS: remove_all(offer, "quantity_in_stock","quantity","stock","Stock")
    return t_cnt,f_cnt,st_cnt,ss_cnt

# --- ids / vendorCode ---
ARTICUL_RE=re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)
def _extract_article_from_name(name:str)->str:
    if not name: return ""
    m=ARTICUL_RE.search(name); return (m.group(1) if m else "").upper()
def _extract_article_from_url(url:str)->str:
    if not url: return ""
    try:
        path=urllib.parse.urlparse(url).path.rstrip("/"); last=re.sub(r"\.(html?|php|aspx?)$","",path.split("/")[-1],flags=re.I)
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

# --- cleanup / order ---
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

# --- KEYWORDS (как раньше, без изменений поведения) ---
AS_INTERNAL_ART_RE = re.compile(r"^AS\d+", re.I)
FEATURE_ACRONYM_BLACKLIST = {"QHD","UHD","FHD","FULL","HDR","HDR10","HDR400","DISPLAYHDR","IPS","VA","TN","LED","OLED",
                             "HDMI","DP","DISPLAY","PORT","USB","TYPEC","TYPE-C","AMD","RADEON","NVIDIA","GSYNC","G-SYNC","FREESYNC","GTG","EYE","SAVER"}
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
        if is_content_word(a) and is_content_word(b) and (_norm_text(a) not in STOPWORDS_RU) and (_norm_text(b) not in STOPWORDS_RU):
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
            if AS_INTERNAL_ART_RE.match(t) or t in FEATURE_ACRONYM_BLACKLIST or not (re.search(r"[A-Z]", t) and re.search(r"\d", t)) or len(t)<5: continue
            tokens.add(t)
    return list(tokens)

def keywords_from_name_generic(name: str) -> List[str]:
    raw_tokens=tokenize_name(name or ""); modelish=[t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content=[t for t in raw_tokens if is_content_word(t)]; bigr=build_bigrams(content)
    norm=lambda tok: tok if re.search(r"[A-Z]{2,}", tok) else tok.capitalize()
    out=modelish[:8]+bigr[:8]+[norm(t) for t in content[:10]]
    return dedup_preserve_order(out)

def head_noun_from_name(name: str, vendor: str = "") -> str:
    if not name: return ""
    toks=tokenize_name(re.sub(r"[\"“”″']", " ", name)); vend=_norm_text(vendor)
    for t in toks:
        tn=_norm_text(t)
        if vend and tn==vend: continue
        if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t): continue
        if tn in STOPWORDS_RU or tn in STOPWORDS_EN or tn in GENERIC_DROP: continue
        if len(tn)>=4 and re.search(r"[А-Яа-яЁё]", t): return t.capitalize()
    return ""

GEO_CITIES=[("Казахстан","Kazakhstan"),("Алматы","Almaty"),("Астана","Astana"),("Шымкент","Shymkent"),("Караганда","Karaganda"),
            ("Актобе","Aktobe"),("Павлодар","Pavlodar"),("Атырау","Atyrau"),("Тараз","Taraz"),("Усть-Каменогорск","Oskemen"),
            ("Семей","Semey"),("Костанай","Kostanay"),("Кызылорда","Kyzylorda"),("Уральск","Oral"),("Петропавловск","Petropavl"),
            ("Талдыкорган","Taldykorgan"),("Актау","Aktau"),("Темиртау","Temirtau"),("Экибастуз","Ekibastuz"),("Кокшетау","Kokshetau"),("Рудный","Rudny")]

_GEO_TOKENS: Optional[List[str]] = None
def geo_tokens() -> List[str]:
    global _GEO_TOKENS
    if _GEO_TOKENS is not None: return _GEO_TOKENS
    if not SATU_KEYWORDS_GEO: _GEO_TOKENS=[]
    else:
        toks=[ru for ru,_ in GEO_CITIES]
        if SATU_KEYWORDS_GEO_LAT: toks += [en for _,en in GEO_CITIES]
        toks=dedup_preserve_order(toks)
        _GEO_TOKENS=toks[:max(0,SATU_KEYWORDS_GEO_MAX)]
    return _GEO_TOKENS

def build_keywords_for_offer(offer: ET.Element) -> str:
    if SATU_KEYWORDS == "off": return ""
    name=get_text(offer,"name"); vendor=get_text(offer,"vendor").strip()
    parts=[vendor] if vendor else []
    head=head_noun_from_name(name, vendor); 
    if head: parts.append(head)
    m_size=re.search(r'(\d{2,3})\s*["″]', name or "")
    if m_size: parts.append(f"{m_size.group(1)} дюйма")
    parts+=extract_model_tokens(offer)+keywords_from_name_generic(name)+color_tokens(name)
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

# --- PRICE CAP ---
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

# --- HTML описание для Satu ---
HEAD_FEATURES_RE = re.compile(r"^\s*особенности\s*:?\s*$", re.I)
HEAD_SPECS_RE    = re.compile(r"^\s*характеристик[аи]\s*:?\s*$", re.I)
BULLET_MARK_RE   = re.compile(r"^\s*[-–—•*]\s*")

def _split_description_sections(desc_text: str) -> Tuple[str, List[str], List[str]]:
    """Возвращает: intro_text, features_lines, specs_lines. Текст НЕ правим — только режем по секциям."""
    if not (desc_text or "").strip():
        return "", [], []
    lines = (desc_text or "").replace("\r\n","\n").replace("\r","\n").split("\n")
    section = "intro"
    intro_lines: List[str] = []
    features: List[str] = []
    specs: List[str] = []
    for raw in lines:
        ln = raw.rstrip()
        if HEAD_FEATURES_RE.match(ln):
            section = "features"; continue
        if HEAD_SPECS_RE.match(ln):
            section = "specs"; continue
        if section == "intro":
            intro_lines.append(ln)
        elif section == "features":
            features.append(BULLET_MARK_RE.sub("", ln))  # убираем только маркер «- », сам текст как есть
        else:
            specs.append(BULLET_MARK_RE.sub("", ln))
    # вычищаем пустые строки на концах секций
    def _strip_empty(arr: List[str]) -> List[str]:
        a = [x for x in arr]
        while a and not a[0].strip(): a.pop(0)
        while a and not a[-1].strip(): a.pop()
        return a
    return "\n".join(_strip_empty(intro_lines)).strip(), _strip_empty(features), _strip_empty(specs)

def _html_escape_in_cdata_safe(s: str) -> str:
    """CDATA не требует экранирования <>&, но последовательность ']]>' ломает CDATA — чиним её."""
    return (s or "").replace("]]>", "]]&gt;")

def _build_html_description(name: str, intro: str, features: List[str], specs: List[str]) -> str:
    """Собираем финальный HTML. Ключи в specs делаем жирными (до двоеточия)."""
    parts: List[str] = []
    title = _html_escape_in_cdata_safe(name or "")
    parts.append(f"<h3>{title}</h3>")
    if intro:
        intro_html = _html_escape_in_cdata_safe(intro).replace("\n", "<br>\n")
        parts.append(f"<p>{intro_html}</p>")
    if features:
        parts.append("<h3>Особенности</h3>")
        parts.append("<ul>")
        for item in features:
            if not item.strip(): continue
            parts.append(f"  <li>{_html_escape_in_cdata_safe(item)}</li>")
        parts.append("</ul>")
    if specs:
        parts.append("<h3>Характеристики</h3>")
        parts.append("<ul>")
        for item in specs:
            txt = item.strip()
            if not txt: continue
            # жирним часть ДО первого двоеточия, но сам текст не меняем
            if ":" in txt:
                key, val = txt.split(":", 1)
                parts.append(f"  <li><strong>{_html_escape_in_cdata_safe(key.strip())}:</strong>{_html_escape_in_cdata_safe(val)}</li>")
            else:
                parts.append(f"  <li>{_html_escape_in_cdata_safe(txt)}</li>")
        parts.append("</ul>")
    return "\n".join(parts)

def convert_descriptions_to_html(shop_el: ET.Element) -> int:
    """Меняет <description> на HTML-версию. Вставляем плейсхолдеры, потом заменим их на CDATA на этапе сериализации."""
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        name = get_text(offer, "name")
        d = offer.find("description")
        raw = (d.text or "").strip() if (d is not None and d.text) else ""
        intro, feats, specs = _split_description_sections(raw)
        html = _build_html_description(name, intro, feats, specs)
        placeholder = f"[[[HTML]]]{html}[[[/HTML]]]"
        if d is None: d = ET.SubElement(offer, "description")
        d.text = placeholder
        touched += 1
    return touched

def _replace_html_placeholders_with_cdata(xml_text: str) -> str:
    """На готовой строке XML ищем <description>[[[HTML]]]...[[[/HTML]]]</description> и подменяем на CDATA-блок."""
    def repl(m):
        inner = m.group(1)
        # убираем маркеры и обратное экранирование &lt;h3&gt; → <h3>
        inner = inner.replace("[[[HTML]]]", "").replace("[[[/HTML]]]", "")
        inner = _unescape(inner)
        inner = _html_escape_in_cdata_safe(inner)
        return f"<description><![CDATA[\n{inner}\n]]></description>"
    return re.sub(r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>", repl, xml_text, flags=re.S)

# --- FEED_META ---
def render_feed_meta_comment(pairs:Dict[str,str])->str:
    try: tz=ZoneInfo("Asia/Almaty"); now_alm=datetime.now(tz)
    except Exception: now_alm=datetime.utcfromtimestamp(time.time()+5*3600)
    today_01=datetime(now_alm.year,now_alm.month,now_alm.day,1,0,0,tzinfo=getattr(now_alm,"tzinfo",None))
    base_ts=today_01.timestamp(); next_ts=base_ts+86400 if now_alm.timestamp()>=base_ts else base_ts
    next_alm=datetime.fromtimestamp(next_ts,getattr(now_alm,"tzinfo",None))
    fmt=lambda dt: dt.strftime("%d:%m:%Y - %H:%M:%S")
    rows=[("Поставщик",pairs.get("supplier","")),("URL поставщика",pairs.get("source","")),
          ("Время сборки (Алматы)",fmt(now_alm)),("Ближайшее время сборки (Алматы)",fmt(next_alm)),
          ("Сколько товаров у поставщика до фильтра",str(pairs.get("offers_total","0"))),
          ("Сколько товаров у поставщика после фильтра",str(pairs.get("offers_written","0"))),
          ("Сколько товаров есть в наличии (true)",str(pairs.get("available_true","0"))),
          ("Сколько товаров нет в наличии (false)",str(pairs.get("available_false","0")))]
    key_w=max(len(k) for k,_ in rows); lines=["FEED_META"]+[f"{k.ljust(key_w)} | {v}" for k,v in rows]
    return "\n".join(lines)

# --- MAIN ---
def main()->None:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")
    offers_in_el=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None: err("XML: <offers> not found")
    src_offers=list(offers_in_el.findall("offer"))

    id2name,id2parent,parent2children=parse_categories_tree(shop_in)

    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")
    for o in src_offers: out_offers.append(deepcopy(o))

    # фильтр категорий (опционально, как раньше)
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
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

    # убрать старые categoryId — и поставить '0' первым
    if DROP_CATEGORY_ID_TAG:
        for off in out_offers.findall("offer"):
            for node in list(off.findall("categoryId"))+list(off.findall("CategoryId")): off.remove(node)

    # PRICE CAP
    flagged = flag_unrealistic_supplier_prices(out_shop); log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")

    # бренды
    ensure_vendor(out_shop); ensure_vendor_auto_fill(out_shop)

    # vendorCode + id
    ensure_vendorcode_with_article(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AS"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )
    sync_offer_id_with_vendorcode(out_shop)

    # цены
    reprice_offers(out_shop, PRICING_RULES)
    forced = enforce_forced_prices(out_shop); log(f"Forced price=100: {forced}")

    # описание (как раньше собираем текст) → затем HTML
    remove_specific_params(out_shop)
    unify_specs_in_description(out_shop)
    remove_blacklisted_kv_from_descriptions(out_shop)
    html_touched = convert_descriptions_to_html(out_shop)
    log(f"Descriptions HTML-converted: {html_touched}")

    # доступность / валюта (вариант A — фото не используем)
    t_true, t_false, _, _ = normalize_available_field(out_shop)
    fix_currency_id(out_shop, default_code="KZT")

    # чистка, порядок, categoryId первым
    for off in out_offers.findall("offer"): purge_offer_tags_and_attrs_after(off)
    reorder_offer_children(out_shop); ensure_categoryid_zero_first(out_shop)

    # KEYWORDS
    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    # форматирование (переносы)
    try: ET.indent(out_root, space="  ")
    except Exception: pass

    meta_pairs={"script_version": SCRIPT_VERSION,"supplier": SUPPLIER_NAME,"source": SUPPLIER_URL or "file",
                "offers_total": len(src_offers),"offers_written": len(list(out_offers.findall("offer"))),
                "available_true": str(t_true),"available_false": str(t_false),
                "built_utc": now_utc_str(),"built_Asia/Almaty": now_almaty_str()}
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # сериализация
    xml_bytes=ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text=xml_bytes.decode(ENC, errors="replace")
    # перевод строки после комментария FEED_META
    xml_text=re.sub(r"(?s)(-->)\s*(<shop\b)", r"\1\n\2", xml_text, count=1)
    # пустая строка между офферами
    xml_text=re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_text)
    # без тройных пустых строк
    xml_text=re.sub(r"(\n[ \t]*){3,}", "\n\n", xml_text)
    # ВАЖНО: подмена плейсхолдеров описаний на CDATA с HTML
    xml_text=_replace_html_placeholders_with_cdata(xml_text)

    if DRY_RUN: log("[DRY_RUN=1] Files not written."); return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML,"w",encoding=ENC, newline="\n") as f: f.write(xml_text)
    try:
        docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e: warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | script={SCRIPT_VERSION}")

if __name__ == "__main__":
    try: main()
    except Exception as e: err(str(e))
