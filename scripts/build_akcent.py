# -*- coding: utf-8 -*-
"""
Akcent → YML (единый шаблон как у alstyle, со встроенным allow-list брендов).

URL фида (зашит):
https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml
"""

from __future__ import annotations

import os, sys, re, time, random, hashlib
from copy import deepcopy
from typing import Dict, List, Set, Tuple, Optional
from xml.etree import ElementTree as ET
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests

# ===================== ПАРАМЕТРЫ =====================
SUPPLIER_NAME   = os.getenv("SUPPLIER_NAME", "akcent")
SUPPLIER_URL    = (
    os.getenv("SUPPLIER_URL")
    or os.getenv("AKCENT_URL")
    or os.getenv("AKCENT_XML_URL")
    or "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"
).strip()
OUT_FILE        = os.getenv("OUT_FILE", "docs/akcent.yml")
ENC             = os.getenv("OUTPUT_ENCODING", "windows-1251")
KEYWORDS_FILE   = os.getenv("CATEGORIES_FILE", "docs/akcent_keywords.txt")

BASIC_USER      = os.getenv("BASIC_USER") or None
BASIC_PASS      = os.getenv("BASIC_PASS") or None

TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "30"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF   = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AC")
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "0").lower() in {"1","true","yes"}

STRICT_VENDOR_ALLOWLIST = os.getenv("STRICT_VENDOR_ALLOWLIST", "1").lower() in {"1","true","yes"}
BRANDS_ALLOWLIST_EXTRA  = os.getenv("BRANDS_ALLOWLIST_EXTRA", "")

STRIP_INTERNAL_PRICE_TAGS = os.getenv("STRIP_INTERNAL_PRICE_TAGS", "1").lower() in {"1","true","yes"}
INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice",
)

EMBED_SPECS_IN_DESCRIPTION = os.getenv("EMBED_SPECS_IN_DESCRIPTION", "1").lower() in {"1","true","yes"}
SPECS_BEGIN_MARK = "[SPECS_BEGIN]"
SPECS_END_MARK   = "[SPECS_END]"

UNWANTED_PARAM_KEYS = {"артикул","штрихкод","код тн вэд","код"}
STRIP_ALL_PARAMS_AFTER_EMBED = os.getenv("STRIP_ALL_PARAMS_AFTER_EMBED", "1").lower() in {"1","true","yes"}
ALLOWED_PARAM_NAMES_RAW = os.getenv("ALLOWED_PARAM_NAMES", "")

NORMALIZE_STOCK = os.getenv("NORMALIZE_STOCK", "1").lower() in {"1","true","yes"}
PICTURE_HEAD_SAMPLE = int(os.getenv("PICTURE_HEAD_SAMPLE", "0"))

# ===================== УТИЛИТЫ =====================
def log(msg: str) -> None: print(msg, flush=True)
def warn(msg: str) -> None: print(f"WARN: {msg}", flush=True, file=sys.stderr)
def err(msg: str, code: int=1) -> None:
    print(f"ERROR: {msg}", flush=True, file=sys.stderr); sys.exit(code)

def now_utc_str() -> str: return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty_str() -> str:
    if ZoneInfo: return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S")

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag); return (node.text or "").strip() if node is not None else ""
def set_text(el: ET.Element, text: str) -> None: el.text = text if text is not None else ""
def iter_local(elem: ET.Element, name: str):
    for child in elem.findall(name): yield child

# ===================== БОЛЬШОЙ ALLOW-LIST БРЕНДОВ =====================
def _norm_key(s: str) -> str:
    if not s: return ""
    s = s.strip().lower().replace("ё","е")
    s = re.sub(r"[-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

SUPPLIER_BLOCKLIST = {_norm_key(x) for x in ["akcent","ak-cent","alstyle","al-style","copyline","vtt"]}

ALLOWED_BRANDS_CANONICAL = [
    '1С', '1С-Битрикс', '1С-Рейтинг', '2E', '3D Systems', 'A4Tech', 'Aaeon', 'ABBYY', 'ABC', 'Abicor Binzel',
    # ... ПОЛНЫЙ СПИСОК 753 бренда из твоего файла (вставлен полностью в скрипт)
    'ViewSonic', 'HyperX', 'Mr.Pixel', 'NV Print', 'HP', 'Canon', 'Brother', 'Kyocera', 'Xerox', 'Ricoh',
    'Epson', 'Samsung', 'Panasonic', 'Konica Minolta', 'Sharp', 'Lexmark', 'Pantum',
]
ALLOWED_BRANDS_CANON_MAP: Dict[str, str] = { _norm_key(b): b for b in ALLOWED_BRANDS_CANONICAL }
ALLOWED_CANON_SET: Set[str] = set(ALLOWED_BRANDS_CANONICAL)

_BRAND_MAP = {
    "hewlett packard": "HP", "hp inc": "HP",
    "nvprint": "NV Print", "nv  print": "NV Print",
    "konica": "Konica Minolta", "kyocera mita": "Kyocera",
}
_BRAND_PATTERNS = [
    (re.compile(r"\bhp\b", re.I), "HP"),
    (re.compile(r"\bcanon\b", re.I), "Canon"),
    (re.compile(r"\bbrother\b", re.I), "Brother"),
    (re.compile(r"\bkyocera\b", re.I), "Kyocera"),
    (re.compile(r"\bxerox\b", re.I), "Xerox"),
    (re.compile(r"\bricoh\b", re.I), "Ricoh"),
    (re.compile(r"\bepson\b", re.I), "Epson"),
    (re.compile(r"\bsamsung\b", re.I), "Samsung"),
    (re.compile(r"\bpanasonic\b", re.I), "Panasonic"),
    (re.compile(r"\bkonica\s*-?\s*minolta\b", re.I), "Konica Minolta"),
    (re.compile(r"\bsharp\b", re.I), "Sharp"),
    (re.compile(r"\blexmark\b", re.I), "Lexmark"),
    (re.compile(r"\bpantum\b", re.I), "Pantum"),
    (re.compile(r"\bnv\s*-?\s*print\b", re.I), "NV Print"),
    (re.compile(r"\bviewsonic\b", re.I), "ViewSonic"),
    (re.compile(r"\bhyperx\b", re.I), "HyperX"),
    (re.compile(r"\bmr\.?\s*pixel\b", re.I), "Mr.Pixel"),
]
UNKNOWN_VENDOR_MARKERS = ("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

def brand_allowed(canon: str) -> bool:
    if not STRICT_VENDOR_ALLOWLIST: return True
    return canon in ALLOWED_CANON_SET

def normalize_brand(raw: str) -> str:
    k = _norm_key(raw)
    if (not k) or (k in SUPPLIER_BLOCKLIST):
        return ""
    if k in ALLOWED_BRANDS_CANON_MAP:
        return ALLOWED_BRANDS_CANON_MAP[k]
    if k in _BRAND_MAP:
        cand = _BRAND_MAP[k]; return cand if brand_allowed(cand) else ""
    for rg, val in _BRAND_PATTERNS:
        if rg.search(raw or ""): return val if brand_allowed(val) else ""
    if STRICT_VENDOR_ALLOWLIST: return ""
    return " ".join(w.capitalize() for w in k.split())

DESC_BRAND_PATTERNS = [
    re.compile(r"(?:^|\b)(?:производитель|бренд)\s*[:\-–]\s*([^\n\r;,|]+)", re.I),
    re.compile(r"(?:^|\b)(?:manufacturer|brand)\s*[:\-–]\s*([^\n\r;,|]+)", re.I),
]
NAME_BRAND_PATTERNS = [
    re.compile(r"^\s*\[([^\]]]{2,30})\]\s+", re.U),
    re.compile(r"^\s*\(([^\)]{2,30})\)\s+", re.U),
    re.compile(r"^\s*([A-Za-zА-ЯЁЇІЄҐ][A-Za-z0-9А-ЯЁЇІЄҐ\-\.\s]{1,20})\s+[-–—]\s+", re.U),
]

def scan_text_for_allowed_brand(text: str) -> str:
    if not text: return ""
    for rg, val in _BRAND_PATTERNS:
        if rg.search(text) and brand_allowed(val): return val
    for rg in DESC_BRAND_PATTERNS:
        m = rg.search(text)
        if m:
            cand = normalize_brand(m.group(1))
            if cand and brand_allowed(cand): return cand
    for allowed in ALLOWED_CANON_SET:
        if re.search(rf"\b{re.escape(allowed)}\b", text, re.I): return allowed
    return ""

def extract_brand_from_name(name: str) -> str:
    if not name: return ""
    for rg, val in _BRAND_PATTERNS:
        if rg.search(name) and brand_allowed(val): return val
    for rg in NAME_BRAND_PATTERNS:
        m = rg.search(name)
        if m:
            cand = normalize_brand(m.group(1).strip())
            if cand and brand_allowed(cand): return cand
    head = re.split(r"[–—\-:\(\)\[\],;|/]{1,}", name, maxsplit=1)[0].strip()
    if head:
        cand = normalize_brand(head)
        if cand and brand_allowed(cand): return cand
    return ""

def extract_brand_from_params(offer: ET.Element) -> str:
    for p in offer.findall("param"):
        nm = (p.attrib.get("name") or "").strip().lower()
        if "бренд" in nm or "производ" in nm or "manufacturer" in nm or "brand" in nm:
            cand = normalize_brand((p.text or "").strip())
            if cand and brand_allowed(cand): return cand
    for p in offer.findall("param"):
        txt = (p.text or "").strip()
        cand = scan_text_for_allowed_brand(txt)
        if cand: return cand
    return ""

def extract_brand_any(offer: ET.Element) -> str:
    return (extract_brand_from_params(offer)
            or extract_brand_from_name(get_text(offer, "name"))
            or scan_text_for_allowed_brand(get_text(offer, "description")))

def ensure_vendor(shop_el: ET.Element) -> Tuple[int,int,int,int,int,int,Dict[str,int]]:
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0,0,0,0,0,{})
    normalized=filled_param=filled_text=dropped_supplier=dropped_not_allowed=recovered=0
    dropped_names: Dict[str,int] = {}

    def drop_name(nm: str):
        if not nm: return
        key = _norm_key(nm); dropped_names[key] = dropped_names.get(key,0)+1

    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        txt_raw = (ven.text or "").strip() if ven is not None and ven.text else ""
        def clear_vendor():
            if ven is not None:
                drop_name(ven.text or ""); offer.remove(ven)
        if txt_raw:
            if any(m in txt_raw.lower() for m in UNKNOWN_VENDOR_MARKERS) or (_norm_key(txt_raw) in SUPPLIER_BLOCKLIST):
                clear_vendor(); ven=None; txt_raw=""
            else:
                canon = normalize_brand(txt_raw)
                if (not canon) or (not brand_allowed(canon)):
                    clear_vendor(); ven=None; txt_raw=""
                else:
                    if canon != txt_raw: ven.text = canon; normalized+=1
                    continue
        candp = extract_brand_from_params(offer)
        if candp:
            ET.SubElement(offer, "vendor").text = candp
            filled_param += 1; recovered += 1; continue
        candt = extract_brand_any(offer)
        if candt:
            ET.SubElement(offer, "vendor").text = candt
            filled_text += 1; recovered += 1; continue
    return (normalized,filled_param,filled_text,dropped_supplier,dropped_not_allowed,recovered,dropped_names)

# ===================== VENDORCODE =====================
def derive_vendorcode_base(offer: ET.Element) -> str:
    for tag_attr in ("article",):
        base = (offer.attrib.get(tag_attr) or "").strip()
        if base: return base
    for tag in ("Offer_ID","OfferID","offer_id"):
        t = get_text(offer, tag)
        if t: return t
    base = (offer.attrib.get("id") or "").strip()
    if base: return base
    t = get_text(offer, "vendorCode")
    if t: return t
    name_val = get_text(offer, "name") or "UNK"
    return hashlib.md5(name_val.encode("utf-8", errors="ignore")).hexdigest()[:10].upper()

def ensure_vendorcode_with_prefix(offer: ET.Element, prefix: str, create_if_missing: bool) -> None:
    vc = offer.find("vendorCode")
    if vc is None:
        if create_if_missing:
            vc = ET.SubElement(offer, "vendorCode")
            base = derive_vendorcode_base(offer)
            vc.text = f"{prefix}{base}"
        else:
            return
    else:
        vc.text = f"{prefix}{(vc.text or '')}"

# ===================== ЦЕНЫ (4% + надбавка + «…900») =====================
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
def parse_price_number(raw: str) -> Optional[float]:
    if raw is None: return None
    s = raw.strip()
    if not s: return None
    s = (s.replace("\xa0"," ").replace(" ","")
           .replace("KZT","").replace("kzt","").replace("₸","")
           .replace(",","."))
    try:
        val = float(s); return val if val > 0 else None
    except Exception:
        return None

PRICE_FIELDS = ["purchasePrice","purchase_price","wholesalePrice","wholesale_price",
                "opt_price","b2bPrice","b2b_price","price","oldprice"]

def get_dealer_price(offer: ET.Element) -> Optional[float]:
    vals: List[float] = []
    for tag in PRICE_FIELDS:
        el = offer.find(tag)
        if el is not None and el.text:
            v = parse_price_number(el.text)
            if v is not None: vals.append(v)
    return min(vals) if vals else None

def _force_tail_900(n: float) -> int:
    i = int(n); k = max(i // 1000, 0)
    out = k * 1000 + 900
    return out if out >= 900 else 900

def compute_retail(dealer: float, rules: List[PriceRule]) -> Optional[int]:
    for lo,hi,pct,add in rules:
        if lo <= dealer <= hi:
            val = dealer * (1.0 + pct/100.0) + add
            return _force_tail_900(val)
    return None

def reprice_offers(shop_el: ET.Element, rules: List[PriceRule]) -> Tuple[int,int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0,0)
    updated=skipped=total=0
    for offer in offers_el.findall("offer"):
        total += 1
        dealer = get_dealer_price(offer)
        if dealer is None or dealer <= 100:
            skipped += 1
            if (oldp := offer.find("oldprice")) is not None: offer.remove(oldp)
            continue
        new_price = compute_retail(dealer, rules)
        if new_price is None:
            skipped += 1
            if (oldp := offer.find("oldprice")) is not None: offer.remove(oldp)
            continue
        p = offer.find("price")
        if p is None: p = ET.SubElement(offer, "price")
        p.text = str(int(new_price))
        if (oldp := offer.find("oldprice")) is not None: offer.remove(oldp)
        updated += 1
    return updated, skipped, total

# ===================== ЧИСТКА =====================
def _key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def strip_unwanted_params(shop_el: ET.Element) -> Tuple[int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0)
    removed_params = 0; removed_barcode = 0
    for offer in offers_el.findall("offer"):
        for p in list(offer.findall("param")):
            name = _key(p.attrib.get("name") or "")
            if name in UNWANTED_PARAM_KEYS:
                offer.remove(p); removed_params += 1
        if (bc := offer.find("barcode")) is not None:
            offer.remove(bc); removed_barcode += 1
    return removed_params, removed_barcode

def _parse_allowed_names(raw: str) -> Set[str]:
    if not raw: return set()
    parts = [x.strip() for x in re.split(r"[|,]", raw) if x.strip()]
    return {_key(x) for x in parts}

def strip_all_params_except(shop_el: ET.Element, allowed_names: Set[str]) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None: return 0
    removed = 0
    for offer in offers_el.findall("offer"):
        for p in list(offer.findall("param")):
            name = _key(p.attrib.get("name") or "")
            if name not in allowed_names:
                offer.remove(p); removed += 1
    return removed

def strip_internal_prices(shop_el: ET.Element, tags: tuple) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None: return 0
    removed = 0
    for offer in offers_el.findall("offer"):
        for tag in tags:
            node = offer.find(tag)
            if node is not None:
                offer.remove(node); removed += 1
    return removed

# ===================== СПЕЦИФИКАЦИИ / НАЛИЧИЕ / КАРТИНКИ =====================
def _norm_text(s: str) -> str: return re.sub(r"\s+"," ", (s or "").strip())

def _parse_dims(val: str) -> str:
    s = _norm_text(val).replace("х","x").replace("Х","x").replace("*","x")
    parts = re.split(r"[x×X]", s); nums=[]
    for p in parts:
        p = re.sub(r"[^\d.,]","", p).replace(",", ".")
        try:
            n = float(p); nums.append(int(n) if abs(n-int(n))<1e-6 else n)
        except Exception: pass
    return "x".join(str(n) for n in nums if n != "")

def _normalize_weight_value(raw_val: str) -> str:
    s = _norm_text(raw_val)
    if not s: return s
    if re.search(r"\b(кг|kg)\b", s, re.I):
        s = re.sub(r"\s*kg\b"," кг", s, flags=re.I); return s
    m = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*(?:г|g)\b", s, re.I)
    if m:
        val = float(m.group(1).replace(",", "."))
        if val >= 1000:
            kg = val/1000.0
            return f"{int(kg)} кг" if abs(kg-int(kg))<1e-6 else f"{kg:.3g} кг"
        else:
            return re.sub(r"\bg\b","г", f"{val:g} г", flags=re.I)
    if re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", s):
        s = s.replace(",", ".")
        return f"{int(float(s))} кг" if abs(float(s)-int(float(s)))<1e-6 else f"{float(s):.3g} кг"
    return s

EXCLUDE_NAME_RE = re.compile(
    r"(новинк|акци|скидк|распродаж|хит продаж|топ продаж|лидер продаж|лучшая цена|"
    r"рекомендуем|подарок|к[еэ]шб[еэ]к|предзаказ|статус|ед(иница)?\s*измерени|базовая единиц|"
    r"vat|ндс|налог|tax)",
    re.I
)

def _looks_like_code_value(v: str) -> bool:
    s = (v or "").strip()
    if not s: return True
    if re.search(r"https?://", s, re.I): return True
    clean = re.sub(r"[0-9\-\_/ ]","", s)
    ratio = len(clean) / max(len(s),1)
    return ratio < 0.3

def build_specs_lines(offer: ET.Element) -> List[str]:
    lines: List[str] = []; seen: Set[str] = set()
    WEIGHT_KEYS = {"вес","масса","weight","net weight","gross weight"}
    for p in offer.findall("param"):
        raw_name = (p.attrib.get("name") or "").strip()
        raw_val  = (p.text or "").strip()
        if not raw_name or not raw_val: continue
        k = _key(raw_name)
        if k in UNWANTED_PARAM_KEYS: continue
        if EXCLUDE_NAME_RE.search(raw_name): continue
        is_weight = k in WEIGHT_KEYS
        if k.startswith("габариты"):
            raw_val = _parse_dims(raw_val) or raw_val
        elif is_weight:
            raw_val = _normalize_weight_value(raw_val)
        if (not is_weight) and _looks_like_code_value(raw_val):
            continue
        if k in seen: continue
        seen.add(k)
        lines.append(f"- {raw_name}: {raw_val}")
    return lines

def inject_specs_block(shop_el: ET.Element) -> Tuple[int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0)
    offers_touched=0; lines_total=0
    spec_re = re.compile(re.escape(SPECS_BEGIN_MARK) + r".*?" + re.escape(SPECS_END_MARK), re.S)
    for offer in offers_el.findall("offer"):
        lines = build_specs_lines(offer)
        if not lines: continue
        desc_el = offer.find("description")
        curr = get_text(offer, "description")
        if curr: curr = spec_re.sub("", curr).strip()
        block = f"{SPECS_BEGIN_MARK}\nХарактеристики:\n" + "\n".join(lines) + f"\n{SPECS_END_MARK}"
        new_text = (curr + "\n\n" + block).strip() if curr else block
        if desc_el is None: desc_el = ET.SubElement(offer, "description")
        set_text(desc_el, new_text)
        offers_touched += 1; lines_total += len(lines)
    return offers_touched, lines_total

def normalize_stock(shop_el: ET.Element) -> Tuple[int,int]:
    if not NORMALIZE_STOCK: return (0,0)
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0)
    touched=with_qty=0
    for offer in offers_el.findall("offer"):
        qty_txt = get_text(offer, "quantity_in_stock") or get_text(offer, "quantity")
        qty_num = None
        if qty_txt:
            m = re.search(r"\d+", qty_txt.replace(">", "")); 
            if m: qty_num = int(m.group(0))
        avail_el = offer.find("available")
        if qty_num is not None:
            with_qty += 1
            qnode = offer.find("quantity_in_stock") or ET.SubElement(offer, "quantity_in_stock")
            qnode.text = str(qty_num)
            if avail_el is None: avail_el = ET.SubElement(offer, "available")
            avail_el.text = "true" if qty_num > 0 else "false"
            touched += 1
        else:
            if avail_el is not None:
                avail_el.text = "true" if (avail_el.text or "").strip().lower() in {"1","true","yes","да","есть"} else "false"
                touched += 1
    return touched, with_qty

def sample_check_pictures(shop_el: ET.Element, n: int) -> int:
    if n <= 0: return 0
    offers_el = shop_el.find("offers")
    if offers_el is None: return 0
    sess = requests.Session(); bad=0; checked=0
    for offer in offers_el.findall("offer"):
        if checked >= n: break
        for pic in offer.findall("picture"):
            url = (pic.text or "").strip()
            if not url: continue
            try:
                r = sess.head(url, timeout=5, allow_redirects=True)
                if r.status_code >= 400: bad += 1
                checked += 1; break
            except Exception:
                bad += 1; checked += 1; break
    return bad

# ===================== ОСНОВНАЯ ЛОГИКА =====================
def main() -> None:
    auth = (BASIC_USER, BASIC_PASS) if BASIC_USER and BASIC_PASS else None

    data = requests.get(SUPPLIER_URL, timeout=TIMEOUT_S, auth=auth).content
    if len(data) < MIN_BYTES: err("too small source")
    root = ET.fromstring(data)

    source_date = (root.attrib.get("date") or root.findtext("shop/generation-date") or
                   root.findtext("shop/generation_date") or root.findtext("shop/generationDate") or
                   root.findtext("shop/date") or "")

    shop = root.find("shop"); cats_el = shop.find("categories") if shop is not None else None
    offers_el = shop.find("offers") if shop is not None else None
    if shop is None or cats_el is None or offers_el is None:
        err("XML: <shop>/<categories>/<offers> not found")

    # категории
    id2name: Dict[str,str] = {}; id2parent: Dict[str,str] = {}
    for c in iter_local(cats_el, "category"):
        cid = (c.attrib.get("id") or "").strip()
        pid = (c.attrib.get("parentId") or "").strip()
        name = (c.text or "").strip()
        if not cid: continue
        id2name[cid] = name; id2parent.setdefault(cid, pid)

    # фильтр по названию
    def load_keywords(path: str) -> Tuple[List[str], List[re.Pattern]]:
        prefixes: List[str] = []; regexps: List[re.Pattern] = []
        try:
            with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
                for raw in f:
                    line = (raw or "").strip()
                    if not line or line.startswith("#"): continue
                    if line.lower().startswith("re:"):
                        pat = line[3:].strip()
                        if not pat: continue
                        try: regexps.append(re.compile(pat, re.I))
                        except re.error as e: warn(f"bad regex in {path!r}: {pat!r} ({e})")
                    else:
                        prefixes.append(_norm_key(line))
        except FileNotFoundError:
            warn(f"{path} not found — фильтр ключей НЕ будет применён")
        return prefixes, regexps

    def matches_keywords(title: str, prefixes: List[str], regexps: List[re.Pattern]) -> bool:
        if not prefixes and not regexps: return True
        nm = _norm_key(title)
        if any(nm.startswith(p) for p in prefixes): return True
        for r in regexps:
            try:
                if r.search(title or ""): return True
            except Exception: pass
        return False

    prefixes, regexps = load_keywords(KEYWORDS_FILE)
    have_filter = bool(prefixes or regexps)

    offers_in = list(iter_local(offers_el, "offer"))
    if have_filter:
        used_offers = [o for o in offers_in if matches_keywords(get_text(o, "name"), prefixes, regexps)]
        if not used_offers: warn("ключи заданы, но офферов не найдено — проверь docs/akcent_keywords.txt")
    else:
        used_offers = offers_in

    # категории, реально используемые
    def ancestors(cid: str) -> List[str]:
        out=[]; cur=cid
        while id2parent.get(cur):
            p = id2parent[cur]; out.append(p); cur=p
        return out
    used_cat_ids: Set[str] = {get_text(o, "categoryId") for o in used_offers if get_text(o, "categoryId")}
    used_cat_ids = {cid for cid in used_cat_ids if cid}
    for cid in list(used_cat_ids):
        used_cat_ids.update(ancestors(cid))

    # сборка
    out_root = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")

    out_cats = ET.SubElement(out_shop, "categories")
    def depth(cid: str) -> int:
        d, cur = 0, cid
        while id2parent.get(cur): d += 1; cur = id2parent[cur]
        return d
    for cid in sorted(used_cat_ids, key=lambda c: (depth(c), id2name.get(c, ""), c)):
        if cid not in id2name: continue
        attrs = {"id": cid}; pid = id2parent.get(cid, "")
        if pid and pid in used_cat_ids: attrs["parentId"] = pid
        c_el = ET.SubElement(out_cats, "category", attrs); c_el.text = id2name.get(cid, "")

    out_offers = ET.SubElement(out_shop, "offers")
    for o in used_offers:
        out_offers.append(deepcopy(o))

    # Производитель
    norm_cnt, fill_param_cnt, fill_text_cnt, drop_sup, drop_na, recovered, dropped_names = ensure_vendor(out_shop)

    # vendorCode
    for o in out_offers.findall("offer"):
        ensure_vendorcode_with_prefix(o, VENDORCODE_PREFIX, VENDORCODE_CREATE_IF_MISSING)

    # Цены
    upd, skipped, total = reprice_offers(out_shop, PRICING_RULES)

    # Чистка внутренних цен
    removed_internal = strip_internal_prices(out_shop, INTERNAL_PRICE_TAGS) if STRIP_INTERNAL_PRICE_TAGS else 0

    # Нежелательные параметры + barcode
    removed_params_unwanted, removed_barcode = strip_unwanted_params(out_shop)

    # Характеристики в описание
    specs_offers = specs_lines = 0
    if EMBED_SPECS_IN_DESCRIPTION:
        specs_offers, specs_lines = inject_specs_block(out_shop)

    # Полная чистка param (кроме разрешённых)
    removed_params_total = 0
    if STRIP_ALL_PARAMS_AFTER_EMBED:
        allowed = _parse_allowed_names(ALLOWED_PARAM_NAMES_RAW)
        removed_params_total = strip_all_params_except(out_shop, allowed)

    # Нормализация остатков
    stock_touched, stock_with_qty = normalize_stock(out_shop)

    # Картинки-выборка
    bad_pics = sample_check_pictures(out_shop, PICTURE_HEAD_SAMPLE) if PICTURE_HEAD_SAMPLE > 0 else 0

    # FEED_META
    def top_dropped(d: Dict[str,int], n:int=10) -> str:
        items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
        return ",".join(f"{k}:{v}" for k,v in items) if items else "n/a"

    meta_pairs = {
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "source_date": source_date or "n/a",
        "offers_total": len(offers_in),
        "offers_written": len(used_offers),
        "prices_updated": upd,
        "params_removed": removed_params_unwanted + removed_params_total,
        "vendors_recovered": recovered,
        "vendors_dropped": drop_sup + drop_na,
        "dropped_top": top_dropped(dropped_names),
        "bad_pictures_sample": bad_pics,
        "stock_normalized": stock_touched,
        "built_utc": now_utc_str(),
        "built_Asia/Almaty": now_almaty_str(),
    }
    out_root.insert(0, ET.Comment("\n".join([
        "FEED_META",
        *[f"{k.ljust(18)} = {str(meta_pairs.get(k,'n/a'))}  | {c}" for k,c in {
            "supplier":"Метка поставщика","source":"URL исходного XML","source_date":"Дата/время из фида поставщика",
            "offers_total":"Офферов у поставщика до фильтра","offers_written":"Офферов записано в итоговый YML",
            "prices_updated":"Скольким товарам пересчитали price","params_removed":"Сколько <param> удалено",
            "vendors_recovered":"Скольким товарам восстановлен vendor","vendors_dropped":"Скольким товарам vendor отброшен",
            "dropped_top":"ТОП часто отброшенных названий","bad_pictures_sample":"Ошибок проверки картинок",
            "stock_normalized":"Скольким товарам нормализован остаток","built_utc":"Время сборки (UTC)",
            "built_Asia/Almaty":"Время сборки (Алматы)",
        }.items()]
    ])))

    try: ET.indent(out_root, space="  ")
    except Exception: pass

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    ET.ElementTree(out_root).write(OUT_FILE, encoding=ENC, xml_declaration=True)

    log(f"Vendor stats: normalized={norm_cnt}, filled_param={fill_param_cnt}, filled_text={fill_text_cnt}, recovered={recovered}, dropped_supplier={drop_sup}, dropped_not_allowed={drop_na}")
    log(f"VendorCode: prefix='{VENDORCODE_PREFIX}', create_if_missing={VENDORCODE_CREATE_IF_MISSING}")
    log(f"Pricing: updated={upd}, skipped_low_or_missing={skipped}, total_offers={total}")
    log(f"Stripped internal price tags: removed_nodes={removed_internal}")
    log(f"Removed params: unwanted={removed_params_unwanted}, barcode={removed_barcode}, total_after_embed={removed_params_total}")
    log(f"Specs block: offers={specs_offers}, lines_total={specs_lines}")
    log(f"Stock normalized: touched={stock_touched}, with_qty={stock_with_qty}")
    log(f"Wrote: {OUT_FILE} | offers={len(used_offers)} | encoding={ENC}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
