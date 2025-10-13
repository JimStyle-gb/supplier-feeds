# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
Alstyle → YML for Satu (плоские <offer> внутри <offers>)
script_version = alstyle-2025-09-23.10

.d10:
- Добавлен ensure_categoryid_zero_first(): вставляет <categoryId>0</categoryId> ПЕРВЫМ узлом в каждом <offer>.
- Вызов ensure_categoryid_zero_first() поставлен ПОСЛЕ reorder_offer_children(), чтобы categoryId остался сверху.
- Мелкие упрощения из прошлого шага сохранены, формат минут в FEED_META исправлен на %M.
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

# ========================== КОНСТАНТЫ И НАСТРОЙКИ ==========================

SCRIPT_VERSION = "alstyle-2025-09-23.10"

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
ALSTYLE_CATEGORIES_MODE  = os.getenv("ALSTYLE_CATEGORIES_MODE", "include").lower()  # off|include|exclude

DROP_CATEGORY_ID_TAG = True         # ранние categoryId от поставщика удаляем
DROP_STOCK_TAGS      = True
PURGE_TAGS_AFTER = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","available","article")

INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice"
)

# =============================== УТИЛИТЫ ===============================

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
    s = (s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

def remove_all(el: ET.Element, *tags: str) -> int:
    """Удалить все дочерние узлы el с указанными тегами. Вернуть количество удалённых."""
    cnt = 0
    for t in tags:
        for n in list(el.findall(t)):
            el.remove(n)
            cnt += 1
    return cnt

# ============================= ЗАГРУЗКА =============================

def load_source_bytes(src: str) -> bytes:
    if not src:
        raise RuntimeError("SUPPLIER_URL не задан")
    # Локальный файл или file://
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path, "rb") as f:
            data = f.read()
        if len(data) < MIN_BYTES:
            raise RuntimeError(f"file too small: {len(data)} bytes")
        return data
    # HTTP(S)
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = sess.get(src, headers=headers, timeout=TIMEOUT_S)  # stream не нужен
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            data = r.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)} bytes)")
            return data
        except Exception as e:
            last_exc = e
            sleep = RETRY_BACKOFF * attempt * (1.0 + random.uniform(-0.2, 0.2))
            warn(f"fetch attempt {attempt}/{RETRIES} failed: {e}; sleep {sleep:.2f}s")
            if attempt < RETRIES:
                time.sleep(sleep)
    raise RuntimeError(f"fetch failed after {RETRIES} attempts: {last_exc}")

# ========================= КАТЕГОРИИ =========================

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

def category_matches_name(path_str: str, rules: List[CatRule]) -> bool:
    cat_norm=_norm_text(_norm_cat(path_str))
    for cr in rules:
        if cr.kind=="substr":
            if cr.raw and cr.raw in cat_norm: return True
        else:
            if cr.pattern and cr.pattern.search(path_str or ""): return True
    return False

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

def collect_descendants(ids: Set[str], parent2children: Dict[str,Set[str]]) -> Set[str]:
    if not ids: return set()
    out=set(ids); stack=list(ids)
    while stack:
        cur=stack.pop()
        for ch in parent2children.get(cur, ()):
            if ch not in out:
                out.add(ch); stack.append(ch)
    return out

def build_category_path_from_id(cat_id: str, id2name: Dict[str,str], id2parent: Dict[str,str]) -> str:
    names=[]; cur=cat_id; seen=set()
    while cur and cur not in seen and cur in id2name:
        seen.add(cur); names.append(id2name.get(cur,"")); cur=id2parent.get(cur,"")
    names=[n for n in names if n]
    return " / ".join(reversed(names)) if names else ""

# ============================ БРЕНДЫ ============================

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
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0,{}
    normalized=0; dropped: Dict[str,int]={}
    for offer in offers_el.findall("offer"):
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

# ============================ ЦЕНЫ ============================

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
    try:
        v=float(s); return v if v>0 else None
    except Exception: return None

def pick_dealer_price(offer: ET.Element) -> Tuple[Optional[float], str]:
    dealer_candidates=[]
    rrp_candidates=[]
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            val=parse_price_number(p.text or "")
            if val is None: continue
            t=(p.attrib.get("type") or "")
            if PRICE_KEYWORDS_DEALER.search(t): dealer_candidates.append(val)
            elif PRICE_KEYWORDS_RRP.search(t):  rrp_candidates.append(val)
    if dealer_candidates:
        return (min(dealer_candidates), "prices_dealer")
    direct=[]
    for tag in PRICE_FIELDS_DIRECT:
        el=offer.find(tag)
        if el is not None and el.text:
            v=parse_price_number(el.text)
            if v is not None: direct.append(v)
    if direct:
        return (min(direct), "direct_field")
    if rrp_candidates:
        return (min(rrp_candidates), "rrp_fallback")
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
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,{"missing":0})
    updated=skipped=total=0
    src_stats={"prices_dealer":0,"direct_field":0,"rrp_fallback":0,"missing":0}
    for offer in offers_el.findall("offer"):
        total+=1
        dealer, src = pick_dealer_price(offer)
        src_stats[src]=src_stats.get(src,0)+1
        if dealer is None or dealer<=100:
            skipped+=1
            remove_all(offer, "prices", "Prices")
            for tag in INTERNAL_PRICE_TAGS:
                remove_all(offer, tag)
            continue
        newp=compute_retail(dealer,rules)
        if newp is None:
            skipped+=1
            continue
        p=offer.find("price")
        if p is None: p=ET.SubElement(offer,"price")
        p.text=str(int(newp))
        remove_all(offer, "prices", "Prices")
        for tag in INTERNAL_PRICE_TAGS:
            remove_all(offer, tag)
        updated+=1
    return updated,skipped,total,src_stats

# ===== ПАРАМЕТРЫ → «Характеристики» =====

def _key(s:str)->str: return re.sub(r"\s+"," ",(s or "").strip()).lower()

EXCLUDE_NAME_RE=re.compile(
    r"(новинк|акци|скидк|уценк|снижена\s*цена|хит продаж|топ продаж|лидер продаж|лучшая цена|"
    r"рекомендуем|подарок|к[еэ]шб[еэ]к|предзаказ|статус|ед(иница)?\s*измерени|базовая единиц|"
    r"vat|ндс|налог|доставк|самовывоз|срок поставки|кредит|рассрочк|наличие\b|^артикул\b|^благотворительн)",
    re.I
)

def _looks_like_code_value(v:str)->bool:
    s=(v or "").strip()
    if not s: return True
    if re.search(r"https?://",s,re.I): return True
    clean=re.sub(r"[0-9\-\_/ ]","",s)
    return (len(clean)/max(len(s),1))<0.3

def build_specs_lines(offer:ET.Element)->List[str]:
    lines=[]; seen=set()
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name=(p.attrib.get("name") or "").strip()
        raw_val =(p.text or "").strip()
        if not raw_name or not raw_val: continue
        if EXCLUDE_NAME_RE.search(raw_name): continue
        if _looks_like_code_value(raw_val): continue
        k=_key(raw_name)
        if k in seen: continue
        seen.add(k); lines.append(f"- {raw_name}: {raw_val}")
    return lines

def inject_specs_block(shop_el:ET.Element)->Tuple[int,int]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0)
    offers_touched=0; lines_total=0
    spec_re=re.compile(r"\[SPECS_BEGIN\].*?\[SPECS_END\]", re.S)
    for offer in offers_el.findall("offer"):
        lines=build_specs_lines(offer)
        if not lines: continue
        desc_el=offer.find("description")
        curr=get_text(offer,"description")
        if curr: curr=spec_re.sub("",curr).strip()
        block="Характеристики:\n"+"\n".join(lines)
        new_text=(curr+"\n\n"+block).strip() if curr else block
        if desc_el is None: desc_el=ET.SubElement(offer,"description")
        desc_el.text=new_text
        offers_touched+=1; lines_total+=len(lines)
    return offers_touched,lines_total

def strip_all_params(shop_el:ET.Element)->int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    removed=0
    for offer in offers_el.findall("offer"):
        removed += remove_all(offer, "param")
        removed += remove_all(offer, "Param")
    return removed

# === ДОП. ЧИСТКА ОПИСАНИЙ ===

RE_KV_LINE = re.compile(r"(^|\n)\s*[-–—]?\s*(Артикул|Благотворительн\w*)\s*:\s*.*?(?=\n|$)", re.I)

def remove_blacklisted_kv_from_descriptions(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    changed=0
    for offer in offers_el.findall("offer"):
        d=offer.find("description")
        if d is None or not d.text: continue
        text=d.text
        new_text = RE_KV_LINE.sub(lambda m: ("" if m.group(1)=="" else m.group(1)), text)
        lines = [ln for ln in new_text.splitlines()]
        out_lines=[]; i=0
        while i < len(lines):
            ln = lines[i]
            if re.match(r"^\s*Характеристики\s*:\s*$", ln, re.I):
                j=i+1; kept=[]
                while j < len(lines) and not re.match(r"^\s*\S.*:$", lines[j]):
                    if lines[j].strip()!="":
                        kept.append(lines[j])
                    j+=1
                if any(re.match(r"^\s*[-–—]\s*\S", x) for x in kept):
                    out_lines.append(ln); out_lines.extend(kept)
                i=j; continue
            else:
                out_lines.append(ln); i+=1
        rebuilt="\n".join(out_lines).strip()
        if rebuilt!=d.text:
            d.text=rebuilt; changed+=1
    return changed

# ======================= ДОСТУПНОСТЬ =======================

TRUE_WORDS  = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS = {"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}

def _parse_bool_str(s: str) -> Optional[bool]:
    if s is None: return None
    v = _norm_text(s)
    if v in TRUE_WORDS:  return True
    if v in FALSE_WORDS: return False
    return None

def _parse_int(s: str) -> Optional[int]:
    if s is None: return None
    t = re.sub(r"[^\d\-]+","", s)
    if t in {"","-","+"}: return None
    try: return int(t)
    except Exception: return None

def derive_available(offer: ET.Element) -> Tuple[bool, str]:
    avail_el = offer.find("available")
    if avail_el is not None and avail_el.text:
        b = _parse_bool_str(avail_el.text)
        if b is not None: return b, "tag"
    for tag in ["quantity_in_stock","quantity","stock","Stock"]:
        for node in offer.findall(tag):
            val = _parse_int(node.text or "")
            if val is not None: return (val > 0), "stock"
    for tag in ["status","Status"]:
        node = offer.find(tag)
        if node is not None and node.text:
            b = _parse_bool_str(node.text)
            if b is not None: return b, "status"
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        nm = (p.attrib.get("name") or "").strip().lower()
        if "статус" in nm or "налич" in nm:
            b = _parse_bool_str(p.text or "")
            if b is not None: return b, "status"
    return False, "default"

def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int,int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
    true_cnt = false_cnt = from_stock_cnt = from_status_cnt = 0
    for offer in offers_el.findall("offer"):
        b, src = derive_available(offer)
        if "available" in offer.attrib:
            offer.attrib.pop("available", None)
        remove_all(offer, "available")
        avail = ET.SubElement(offer, "available")
        avail.text = "true" if b else "false"
        if b: true_cnt += 1
        else: false_cnt += 1
        if src == "stock": from_stock_cnt += 1
        if src == "status": from_status_cnt += 1
        if DROP_STOCK_TAGS:
            remove_all(offer, "quantity_in_stock", "quantity", "stock", "Stock")
    return true_cnt, false_cnt, from_stock_cnt, from_status_cnt

# ===================== vendorCode / артикул =====================

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
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
    total_prefixed=created=filled_from_art=fixed_bare=0
    for offer in offers_el.findall("offer"):
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

# === СИНХРОНИЗАЦИЯ offer/@id С vendorCode (id = vendorCode) ===

def sync_offer_id_with_vendorcode(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None: return 0
    changed = 0
    for offer in offers_el.findall("offer"):
        vc = offer.find("vendorCode")
        if vc is None or not (vc.text or "").strip():
            continue
        new_id = (vc.text or "").strip()
        if offer.attrib.get("id") != new_id:
            offer.attrib["id"] = new_id
            changed += 1
    return changed

# ===================== ЧИСТКА ТЕГОВ/АТРИБУТОВ =====================

def purge_offer_tags_and_attrs_after(offer:ET.Element)->Tuple[int,int]:
    removed_tags=0
    for t in PURGE_TAGS_AFTER:
        for node in list(offer.findall(t)):
            offer.remove(node); removed_tags+=1
    removed_attrs=0
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib:
            offer.attrib.pop(a,None); removed_attrs+=1
    return removed_tags,removed_attrs

def count_category_ids(offer_el: ET.Element) -> int:
    return len(list(offer_el.findall("categoryId"))) + len(list(offer_el.findall("CategoryId")))

# ===================== currencyId: дедуп =====================

def fix_currency_id(shop_el: ET.Element, default_code: str = "KZT") -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        remove_all(offer, "currencyId")
        new_cur = ET.SubElement(offer, "currencyId")
        new_cur.text = default_code
        touched += 1
    return touched

# ===================== СОРТИРОВКА ДЕТЕЙ ВНУТРИ <offer> =====================

DESIRED_ORDER = ["vendorCode","name","price","picture","vendor","currencyId","available","description"]

def reorder_offer_children(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None: return 0
    changed = 0

    for offer in offers_el.findall("offer"):
        children = list(offer)
        if not children:
            continue

        buckets: Dict[str, List[ET.Element]] = {k: [] for k in DESIRED_ORDER}
        others: List[ET.Element] = []

        for node in children:
            tag = node.tag
            if tag in buckets:
                buckets[tag].append(node)
            else:
                others.append(node)

        rebuilt = []
        for k in DESIRED_ORDER:
            rebuilt.extend(buckets[k])
        rebuilt.extend(others)

        if rebuilt != children:
            for node in children:
                offer.remove(node)
            for node in rebuilt:
                offer.append(node)
            changed += 1

    return changed

# ===================== ДОБАВИТЬ <categoryId>0 САМЫМ ВЕРХОМ =====================

def ensure_categoryid_zero_first(shop_el: ET.Element) -> int:
    """
    Для каждого <offer> удаляет все categoryId/CategoryId и вставляет
    <categoryId>0</categoryId> ПЕРВЫМ дочерним узлом.
    """
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        remove_all(offer, "categoryId", "CategoryId")
        cid = ET.Element("categoryId")
        cid.text = "0"
        offer.insert(0, cid)
        touched += 1
    return touched

# ========================== FEED_META ========================

def render_feed_meta_comment(pairs:Dict[str,str])->str:
    try:
        tz = ZoneInfo("Asia/Almaty")
        now_alm = datetime.now(tz)
    except Exception:
        now_alm = datetime.utcfromtimestamp(time.time() + 5*3600)

    today_01 = datetime(
        now_alm.year, now_alm.month, now_alm.day, 1, 0, 0,
        tzinfo=getattr(now_alm, "tzinfo", None)
    )
    base_ts = today_01.timestamp()
    next_ts = base_ts + 86400 if now_alm.timestamp() >= base_ts else base_ts
    next_alm = datetime.fromtimestamp(next_ts, getattr(now_alm, "tzinfo", None))

    def fmt(dt: datetime) -> str:
        return dt.strftime("%d:%m:%Y - %H:%M:%S")  # латинская %M

    rows = [
        ("Поставщик", pairs.get("supplier","")),
        ("URL поставщика", pairs.get("source","")),
        ("Время сборки (Алматы)", fmt(now_alm)),
        ("Ближайшее время сборки (Алматы)", fmt(next_alm)),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total","0"))),
        ("Сколько товаров у поставщика после фильтра", str(pairs.get("offers_written","0"))),
        ("Сколько товаров есть в наличии (true)", str(pairs.get("available_true","0"))),
        ("Сколько товаров нет в наличии (false)", str(pairs.get("available_false","0"))),
    ]
    key_w = max(len(k) for k,_ in rows)
    lines = ["FEED_META"]
    for k,v in rows:
        lines.append(f"{k.ljust(key_w)} | {v}")
    return "\n".join(lines)

def top_dropped(d:Dict[str,int], n:int=10)->str:
    items=sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return(",".join(f"{k}:{v}" for k,v in items) if items else "n/a")

# ================================= MAIN ====================================

def main()->None:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")
    offers_in_el = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None: err("XML: <offers> not found")
    src_offers=list(offers_in_el.findall("offer"))

    id2name, id2parent, parent2children = parse_categories_tree(shop_in)
    catid_to_drop_total=sum(count_category_ids(o) for o in src_offers)

    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")

    for o in src_offers: out_offers.append(deepcopy(o))

    # фильтр категорий
    rules_ids, rules_names = (set(),[])
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH)
        if ALSTYLE_CATEGORIES_MODE=="include" and not (rules_ids or rules_names):
            err("ALSTYLE_CATEGORIES_MODE=include, но правил категорий не найдено. Проверь docs/alstyle_categories.txt.", 2)
    filtered_by_categories = 0
    if (ALSTYLE_CATEGORIES_MODE in {"include","exclude"}) and (rules_ids or rules_names):
        keep_ids: Set[str] = set(rules_ids)
        if rules_names and id2name:
            for cid in id2name.keys():
                path = build_category_path_from_id(cid, id2name, id2parent)
                if category_matches_name(path, rules_names):
                    keep_ids.add(cid)
        if keep_ids and parent2children:
            keep_ids = collect_descendants(keep_ids, parent2children)
        for off in list(out_offers.findall("offer")):
            cid = get_text(off, "categoryId")
            hit = (cid in keep_ids) if cid else False
            drop_this = (ALSTYLE_CATEGORIES_MODE=="exclude" and hit) or (ALSTYLE_CATEGORIES_MODE=="include" and not hit)
            if drop_this:
                out_offers.remove(off); filtered_by_categories += 1

    if DROP_CATEGORY_ID_TAG:
        for off in out_offers.findall("offer"):
            for node in list(off.findall("categoryId")) + list(off.findall("CategoryId")):
                off.remove(node)

    norm_cnt, dropped_names = ensure_vendor(out_shop)

    total_prefixed, created_nodes, filled_from_art, fixed_bare = ensure_vendorcode_with_article(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AS"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )

    # ID = vendorCode
    sync_offer_id_with_vendorcode(out_shop)

    # ПЕРЕСЧЁТ ЦЕН
    upd, skipped, total, src_stats = reprice_offers(out_shop, PRICING_RULES)

    # ДЕДУП и нормализация <available>
    av_true, av_false, av_from_stock, av_from_status = normalize_available_field(out_shop)

    specs_offers, specs_lines = inject_specs_block(out_shop)
    removed_params = strip_all_params(out_shop)
    removed_kv = remove_blacklisted_kv_from_descriptions(out_shop)

    # Один currencyId на оффер
    fix_currency_id(out_shop, default_code="KZT")

    for off in out_offers.findall("offer"): purge_offer_tags_and_attrs_after(off)

    # Жёсткий порядок тегов внутри <offer>
    reorder_offer_children(out_shop)

    # === ВСТАВИТЬ <categoryId>0 ПЕРВЫМ УЗЛОМ В КАЖДЫЙ <offer> ===
    ensure_categoryid_zero_first(out_shop)

    # Разделительные комментарии и отступы
    children=list(out_offers)
    for i in range(len(children)-1, 0, -1):
        out_offers.insert(i, ET.Comment("OFFSEP"))
    try: ET.indent(out_root, space="  ")
    except Exception: pass

    offers_written=len(list(out_offers.findall("offer")))
    meta_pairs={
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL or "file",
        "offers_total": len(src_offers),
        "offers_written": offers_written,
        "categories_mode": ALSTYLE_CATEGORIES_MODE if (rules_ids or rules_names) else "off",
        "categories_total": len(rules_ids) + len(rules_names),
        "filtered_by_categories": filtered_by_categories,
        "prices_updated": upd,
        "prices_skipped": skipped,
        "dealer_src_prices_dealer": src_stats.get("prices_dealer",0),
        "dealer_src_direct_field": src_stats.get("direct_field",0),
        "dealer_src_rrp_fallback": src_stats.get("rrp_fallback",0),
        "dealer_src_missing": src_stats.get("missing",0),
        "params_removed": specs_lines,
        "vendors_recovered": norm_cnt,
        "dropped_top": top_dropped(dropped_names),
        "available_true": av_true,
        "available_false": av_false,
        "available_from_stock": av_from_stock,
        "available_from_status": av_from_status,
        "categoryId_dropped": catid_to_drop_total,
        "vendorcodes_filled_from_article": filled_from_art,
        "vendorcodes_created": created_nodes,
        "built_utc": now_utc_str(),
        "built_Asia/Almaty": now_almaty_str(),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")
    xml_text = re.sub(r"\s*<!--OFFSEP-->\s*", "\n\n  ", xml_text)
    xml_text = re.sub(r"(\n[ \t]*){3,}", "\n\n", xml_text)
    xml_text = re.sub(r"(-->)\s*(<shop>)", lambda m: f"{m.group(1)}\n  {m.group(2)}", xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written."); return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f: f.write(xml_text)

    docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
    try:
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | offers={offers_written} | encoding={ENC} | script={SCRIPT_VERSION}")

if __name__ == "__main__":
    try: main()
    except Exception as e: err(str(e))
