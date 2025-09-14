# -*- coding: utf-8 -*-
"""
Build Alstyle YML (flat <offers>) for Satu — script_version=alstyle-2025-09-14.3

Особенности Alstyle:
- <available> берём у поставщика: явный тег → остатки → текстовый статус → по умолчанию false.
- После расчёта оставляем единый тег <available>, складские/служебные теги удаляем.
- Генерируем ТОЛЬКО docs/alstyle.yml; описания сжимаем в одну строку; FEED_META на русском с выровненными колонками.
"""

from __future__ import annotations
import os, sys, re, time, random, urllib.parse
from copy import deepcopy
from typing import Dict, List, Set, Tuple, Optional
from xml.etree import ElementTree as ET
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests

# ========================== ГЛОБАЛЬНЫЕ НАСТРОЙКИ ===========================

SCRIPT_VERSION = "alstyle-2025-09-14.3"  # для FEED_META/логов

SUPPLIER_NAME    = os.getenv("SUPPLIER_NAME", "alstyle")
# ✅ Дефолт — официальный экспорт Alstyle
SUPPLIER_URL     = os.getenv(
    "SUPPLIER_URL",
    "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
).strip()
OUT_FILE_YML     = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC              = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S        = int(os.getenv("TIMEOUT_S", "30"))
RETRIES          = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF    = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))

DRY_RUN          = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS")
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "1").lower() in {"1","true","yes"}

ALSTYLE_KEYWORDS_PATH  = os.getenv("ALSTYLE_KEYWORDS_PATH", "docs/alstyle_keywords.txt")
ALSTYLE_KEYWORDS_MODE  = os.getenv("ALSTYLE_KEYWORDS_MODE", "off").lower()  # off|include|exclude
ALSTYLE_KEYWORDS_DEBUG = os.getenv("ALSTYLE_KEYWORDS_DEBUG", "0").lower() in {"1","true","yes"}
ALSTYLE_DEBUG_MAX_HITS = int(os.getenv("ALSTYLE_DEBUG_MAX_HITS", "40"))

DROP_CATEGORY_ID_TAG     = True
DROP_STOCK_TAGS          = True
PURGE_TAGS_AFTER = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","available","article")

INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice"
)

# ================================ УТИЛИТЫ ==================================

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARN: {msg}", file=sys.stderr, flush=True)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def now_almaty_str() -> str:
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag); return (node.text or "").strip() if node is not None and node.text else ""

def _norm_name(s: str) -> str:
    s = (s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

# ====================== ЗАГРУЗКА ИСХОДНОГО XML ============================

def load_source_bytes(src: str) -> bytes:
    """Загружает исходник из URL/файла."""
    if not src:
        raise RuntimeError("SUPPLIER_URL не задан")
    if src.startswith("file://"):
        path = src[7:]
        with open(path, "rb") as f: data = f.read()
        if len(data) < MIN_BYTES: raise RuntimeError(f"file too small: {len(data)} bytes")
        return data
    if "://" not in src:
        with open(src, "rb") as f: data = f.read()
        if len(data) < MIN_BYTES: raise RuntimeError(f"file too small: {len(data)} bytes")
        return data
    sess = requests.Session(); headers = {"User-Agent":"supplier-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, RETRIES+1):
        try:
            r = sess.get(src, headers=headers, timeout=TIMEOUT_S, stream=True)
            if r.status_code != 200: raise RuntimeError(f"HTTP {r.status_code}")
            data = r.content
            if len(data) < MIN_BYTES: raise RuntimeError(f"too small ({len(data)} bytes)")
            return data
        except Exception as e:
            last_exc = e
            sleep = RETRY_BACKOFF * attempt * (1.0 + random.uniform(-0.2, 0.2))
            warn(f"fetch attempt {attempt}/{RETRIES} failed: {e}; sleep {sleep:.2f}s")
            if attempt < RETRIES: time.sleep(sleep)
    raise RuntimeError(f"fetch failed after {RETRIES} attempts: {last_exc}")

# ==================== КЛЮЧИ ДЛЯ ФИЛЬТРА ПО НАЗВАНИЮ =======================

class KeySpec:
    __slots__=("raw","kind","pattern")
    def __init__(self, raw: str, kind: str, pattern): self.raw, self.kind, self.pattern = raw, kind, pattern

def load_keywords(path: str) -> List[KeySpec]:
    if not path or not os.path.exists(path): return []
    data = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f: txt=f.read()
            data = txt.replace("\ufeff","").replace("\x00",""); break
        except Exception: continue
    if data is None:
        with open(path,"r",encoding="utf-8",errors="ignore") as f: data=f.read().replace("\x00","")
    keys: List[KeySpec]=[]
    for ln in data.splitlines():
        s=ln.strip()
        if not s or s.lstrip().startswith("#"): continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try: keys.append(KeySpec(s,"regex",re.compile(s[1:-1],re.I))); continue
            except Exception: continue
        if s.startswith("~="):
            w=_norm_name(s[2:]); 
            if w: keys.append(KeySpec(s,"word",re.compile(r"\b"+re.escape(w)+r"\b",re.I))); 
            continue
        keys.append(KeySpec(_norm_name(s),"substr",None))
    return keys

def name_matches(name:str, keys:List[KeySpec]) -> Tuple[bool, Optional[str]]:
    n=_norm_name(name)
    for ks in keys:
        if ks.kind=="substr":
            if ks.raw and ks.raw in n: return True,ks.raw
        else:
            if ks.pattern and ks.pattern.search(name or ""): return True,ks.raw
    return False,None

# ============================ НОРМАЛИЗАЦИЯ БРЕНДА ==========================

def _norm_key(s: str) -> str:
    if not s: return ""
    s=s.strip().lower().replace("ё","е"); s=re.sub(r"[-_/]+"," ",s); s=re.sub(r"\s+"," ",s); return s

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

# ============================== ЦЕНООБРАЗОВАНИЕ ============================

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

PRICE_FIELDS=["purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price","price","oldprice"]

def parse_price_number(raw:str)->Optional[float]:
    if raw is None: return None
    s=(raw.strip().replace("\xa0"," ").replace(" ","").replace("KZT","").replace("kzt","").replace("₸","").replace(",","."))
    if not s: return None
    try: v=float(s); return v if v>0 else None
    except Exception: return None

def get_dealer_price(offer:ET.Element)->Optional[float]:
    vals=[]
    for tag in PRICE_FIELDS:
        el=offer.find(tag)
        if el is not None and el.text:
            v=parse_price_number(el.text)
            if v is not None: vals.append(v)
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            v=parse_price_number(p.text or "")
            if v is not None: vals.append(v)
    return min(vals) if vals else None

def _force_tail_900(n:float)->int:
    i=int(n); k=max(i//1000,0); out=k*1000+900; return out if out>=900 else 900

def compute_retail(dealer:float,rules:List[PriceRule])->Optional[int]:
    for lo,hi,pct,add in rules:
        if lo<=dealer<=hi:
            val=dealer*(1.0+pct/100.0)+add
            return _force_tail_900(val)
    return None

def reprice_offers(shop_el:ET.Element,rules:List[PriceRule])->Tuple[int,int,int]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0)
    updated=skipped=total=0
    for offer in offers_el.findall("offer"):
        total+=1
        dealer=get_dealer_price(offer)
        if dealer is None or dealer<=100:
            skipped+=1
            node=offer.find("oldprice"); 
            if node is not None: offer.remove(node)
            continue
        newp=compute_retail(dealer,rules)
        if newp is None:
            skipped+=1
            node=offer.find("oldprice"); 
            if node is not None: offer.remove(node)
            continue
        p=offer.find("price") or ET.SubElement(offer,"price"); p.text=str(int(newp))
        cur=offer.find("currencyId") or ET.SubElement(offer,"currencyId"); cur.text="KZT"
        for node in list(offer.findall("prices")) + list(offer.findall("Prices")): offer.remove(node)
        for tag in INTERNAL_PRICE_TAGS:
            node=offer.find(tag)
            if node is not None: offer.remove(node)
        updated+=1
    return updated,skipped,total

# ===================== ПАРАМЕТРЫ → «ХАРАКТЕРИСТИКИ» =======================

def _key(s:str)->str: return re.sub(r"\s+"," ",(s or "").strip()).lower()

EXCLUDE_NAME_RE=re.compile(
    r"(новинк|акци|скидк|уценк|снижена\s*цена|хит продаж|топ продаж|лидер продаж|лучшая цена|"
    r"рекомендуем|подарок|к[еэ]шб[еэ]к|предзаказ|статус|ед(иница)?\s*измерени|базовая единиц|"
    r"vat|ндс|налог|доставк|самовывоз|срок поставки|кредит|рассрочк|наличие\b)", re.I
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
        for p in list(offer.findall("param")) + list(offer.findall("Param")):
            offer.remove(p); removed+=1
    return removed

# ==================== ОПИСАНИЯ: СЖАТЬ В ОДНУ СТРОКУ =======================

_HTML_NBSP_RE = re.compile(r"&nbsp;", re.I)

def _clean_description_text_one_line(s:str)->str:
    if not s: return s
    s=s.replace("\r\n","\n").replace("\r","\n").replace("\u00A0"," ")
    s=_HTML_NBSP_RE.sub(" ", s)
    s=re.sub(r"\s+"," ", s)
    return s.strip()

def clean_all_descriptions_one_line(shop_el:ET.Element)->int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        d=offer.find("description")
        if d is not None and d.text:
            cleaned=_clean_description_text_one_line(d.text)
            if cleaned!=d.text:
                d.text=cleaned; touched+=1
    return touched

# =========================== НАЛИЧИЕ/СКЛАД (ALSTYLE) =======================

TRUE_WORDS  = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS = {"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}

def _parse_bool_str(s: str) -> Optional[bool]:
    if s is None: return None
    v = _norm_name(s)
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
        if "available" in offer.attrib: offer.attrib.pop("available", None)
        avail = offer.find("available") or ET.SubElement(offer, "available")
        avail.text = "true" if b else "false"
        if b: true_cnt += 1
        else: false_cnt += 1
        if src == "stock": from_stock_cnt += 1
        if src == "status": from_status_cnt += 1
        if DROP_STOCK_TAGS:
            for tag in ["quantity_in_stock","quantity","stock","Stock"]:
                for node in list(offer.findall(tag)): offer.remove(node)
    return true_cnt, false_cnt, from_stock_cnt, from_status_cnt

# ======================= vendorCode / артикул ==============================

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
            if art: vc.text=art; filled_from_art+=1
            else:   fixed_bare+=1
        vc.text=f"{prefix}{(vc.text or '')}"; total_prefixed+=1
    return total_prefixed,created,filled_from_art,fixed_bare

# ================= ФИНАЛЬНАЯ ЧИСТКА ТЕГОВ/АТРИБУТОВ =======================

def purge_offer_tags_and_attrs_after(offer:ET.Element)->Tuple[int,int]:
    removed_tags=0
    for t in PURGE_TAGS_AFTER:
        for node in list(offer.findall(t)):
            offer.remove(node); removed_tags+=1
    removed_attrs=0
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib: offer.attrib.pop(a,None); removed_attrs+=1
    return removed_tags,removed_attrs

def count_category_ids(offer_el:ET.Element)->int:
    return len(list(offer_el.findall("categoryId"))) + len(list(offer_el.findall("CategoryId")))

# =========================== FEED_META (комментарий) =======================

def render_feed_meta_comment(pairs:Dict[str,str])->str:
    order=[
        "script_version","supplier","source","offers_total","offers_written",
        "keywords_mode","keywords_total","filtered_by_keywords",
        "prices_updated","params_removed","vendors_recovered","dropped_top",
        "available_true","available_false","available_from_stock","available_from_status",
        "categoryId_dropped","vendorcodes_filled_from_article","vendorcodes_created",
        "built_utc","built_Asia/Almaty",
    ]
    comments={
        "script_version":"Версия скрипта (для контроля в CI)",
        "supplier":"Метка поставщика",
        "source":"URL/файл исходного XML",
        "offers_total":"Офферов у поставщика до очистки",
        "offers_written":"Офферов записано (после очистки)",
        "keywords_mode":"Режим фильтра (off/include/exclude)",
        "keywords_total":"Сколько ключей загружено",
        "filtered_by_keywords":"Сколько офферов отфильтровано по keywords",
        "prices_updated":"Скольким товарам пересчитали price",
        "params_removed":"Сколько строк параметров добавлено в описание",
        "vendors_recovered":"Скольким товарам нормализован/восстановлен vendor",
        "dropped_top":"ТОП часто отброшенных названий бренда",
        "available_true":"Сколько офферов доступны (true)",
        "available_false":"Сколько офферов недоступны (false)",
        "available_from_stock":"Сколько доступностей определено по остаткам",
        "available_from_status":"Сколько доступностей определено по статусу",
        "categoryId_dropped":"Сколько тегов categoryId удалено",
        "vendorcodes_filled_from_article":"Скольким офферам проставили vendorCode из артикула",
        "vendorcodes_created":"Сколько узлов vendorCode было создано",
        "built_utc":"Время сборки (UTC)",
        "built_Asia/Almaty":"Время сборки (Алматы)",
    }
    max_key=max(len(k) for k in order)
    left=[f"{k.ljust(max_key)} = {pairs.get(k,'n/a')}" for k in order]
    max_left=max(len(s) for s in left)
    lines=["FEED_META"]
    for l,k in zip(left,order):
        lines.append(f"{l.ljust(max_left)}  | {comments.get(k,'')}")
    return "\n".join(lines)

def top_dropped(d:Dict[str,int], n:int=10)->str:
    items=sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return ",".join(f"{k}:{v}" for k,v in items) if items else "n/a"

# ================================= MAIN ====================================

def main()->None:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")

    offers_in=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in is None: err("XML: <offers> not found")

    src_offers=list(offers_in.findall("offer"))
    catid_to_drop_total=sum(count_category_ids(o) for o in src_offers)

    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")

    for o in src_offers:
        mod=deepcopy(o)
        if DROP_CATEGORY_ID_TAG:
            for node in list(mod.findall("categoryId")) + list(mod.findall("CategoryId")): mod.remove(node)
        out_offers.append(mod)

    keys=[]
    if ALSTYLE_KEYWORDS_MODE in {"include","exclude"}:
        keys=load_keywords(ALSTYLE_KEYWORDS_PATH)
        if ALSTYLE_KEYWORDS_MODE=="include" and len(keys)==0:
            err("ALSTYLE_KEYWORDS_MODE=include, но ключей не найдено. Проверь docs/alstyle_keywords.txt.", 2)

    filtered_out=0
    if (ALSTYLE_KEYWORDS_MODE in {"include","exclude"}) and len(keys)>0:
        for off in list(out_offers.findall("offer")):
            nm=get_text(off,"name")
            hit,_=name_matches(nm,keys)
            drop_this=(ALSTYLE_KEYWORDS_MODE=="exclude" and hit) or (ALSTYLE_KEYWORDS_MODE=="include" and not hit)
            if drop_this: out_offers.remove(off); filtered_out+=1

    norm_cnt, dropped_names = ensure_vendor(out_shop)
    total_prefixed, created_nodes, filled_from_art, fixed_bare = ensure_vendorcode_with_article(
        out_shop, prefix=VENDORCODE_PREFIX, create_if_missing=VENDORCODE_CREATE_IF_MISSING
    )
    upd, skipped, total = reprice_offers(out_shop, PRICING_RULES)

    av_true, av_false, av_from_stock, av_from_status = normalize_available_field(out_shop)

    specs_offers, specs_lines = inject_specs_block(out_shop)
    removed_params = strip_all_params(out_shop)
    cleaned_desc = clean_all_descriptions_one_line(out_shop)

    for off in out_offers.findall("offer"):
        purge_offer_tags_and_attrs_after(off)

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
        "keywords_mode": ALSTYLE_KEYWORDS_MODE if len(keys)>0 else "off",
        "keywords_total": len(keys),
        "filtered_by_keywords": filtered_out,
        "prices_updated": upd,
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

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written."); return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f: f.write(xml_text)

    docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
    try:
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | offers={offers_written} | encoding={ENC} | script={SCRIPT_VERSION} | desc_cleaned={cleaned_desc}")

if __name__ == "__main__":
    try: main()
    except Exception as e: err(str(e))
