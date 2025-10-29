# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle → YML: стабильные цены/наличие + безопасный HTML для <description>.

Обновление v7.3.2 (29.10.2025):
- УБРАНЫ дубли: DESIRED_ORDER и reorder_offer_children определяются один раз (глобально).
- Гео-города: «Костанаи» → «Костанай», «Petropavl» → «Petropavlovsk», добавлен «Петропавловск».
- ENV совместимость: теперь скрипт понимает оба варианта переменной путя категорий —
  ALSTYLE_CATEGORIES_FILE ИЛИ ALSTYLE_CATEGORIES_PATH (берёт первый непустой).
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

SCRIPT_VERSION = "alstyle-2025-10-29.v7.3.2"

# ======================= ENV / CONST =======================
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "AlStyle").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/alstyle.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()
TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# ВАЖНО: поддерживаем ОДНОВРЕМЕННО оба имени ENV для совместимости workflow’ов
ALSTYLE_CATEGORIES_PATH = (
    os.getenv("ALSTYLE_CATEGORIES_FILE")
    or os.getenv("ALSTYLE_CATEGORIES_PATH")
    or "docs/alstyle_categories.txt"
)
ALSTYLE_CATEGORIES_MODE = os.getenv("ALSTYLE_CATEGORIES_MODE", "off").lower()  # off|include|exclude

# PRICE CAP
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "100"))

# Параметры генерации keywords
SATU_KEYWORDS            = os.getenv("SATU_KEYWORDS", "on").lower()        # on|off
SATU_KEYWORDS_MAXLEN     = int(os.getenv("SATU_KEYWORDS_MAXLEN", "300"))
SATU_KEYWORDS_MAXWORDS   = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "28"))
SATU_KEYWORDS_GEO        = os.getenv("SATU_KEYWORDS_GEO", "on").lower() in {"on","1","true","yes"}
SATU_KEYWORDS_GEO_MAX    = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))
SATU_KEYWORDS_GEO_LAT    = os.getenv("SATU_KEYWORDS_GEO_LAT", "on").lower() in {"on","1","true","yes"}

# SEO sticky cache / РЕЖИМ РЕФРЕША
DEFAULT_CACHE_PATH = "docs/alstyle_cache/seo_cache.json"
SEO_CACHE_PATH     = os.getenv("SEO_CACHE_PATH", DEFAULT_CACHE_PATH)
SEO_STICKY         = os.getenv("SEO_STICKY", "1").lower() in {"1","true","yes","on"}
# Режимы: "monthly_1" (каждое 1-е число), "days" (каждые N суток), "off"
SEO_REFRESH_MODE   = os.getenv("SEO_REFRESH_MODE", "monthly_1").lower()
SEO_REFRESH_DAYS   = int(os.getenv("SEO_REFRESH_DAYS", "14"))  # используется когда MODE=days
LEGACY_CACHE_PATH  = "docs/seo_cache.json"

# Placeholders (фото)
PLACEHOLDER_ENABLE         = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE     = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.al-style.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE  = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.al-style.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL    = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.al-style.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT            = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT   = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# Purge internals
DROP_CATEGORY_ID_TAG   = True
DROP_STOCK_TAGS        = True
PURGE_TAGS_AFTER       = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER= ("type","article")
INTERNAL_PRICE_TAGS    = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                          "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice",
                          "max_price","maxPrice","oldprice")

# ======================= LOGS =======================
def log(msg: str): print(f"[alstyle] {msg}", flush=True)
def warn(msg: str): print(f"[alstyle][warn] {msg}", flush=True, file=sys.stderr)
def err(msg: str): print(f"[alstyle][ERROR] {msg}", flush=True, file=sys.stderr); raise SystemExit(1)

# ======================= XML helpers =======================
def remove_all(el: ET.Element, *tags: str) -> int:
    """Удалить все подполя с перечисленными тегами, вернуть кол-во удалённых."""
    n=0
    for t in tags:
        for node in list(el.findall(t)):
            el.remove(node); n+=1
    return n

def get_text(parent: ET.Element, tag: str) -> str:
    node = parent.find(tag)
    return (node.text or "").strip() if node is not None and node.text else ""

def set_text(parent: ET.Element, tag: str, value: str) -> ET.Element:
    node = parent.find(tag)
    if node is None: node = ET.SubElement(parent, tag)
    node.text = value
    return node

def inner_xml(el: Optional[ET.Element]) -> str:
    """Вернуть внутренний XML узла (без самого контейнера)."""
    if el is None: return ""
    parts=[]
    if el.text: parts.append(el.text)
    for child in el:
        parts.append(ET.tostring(child, encoding="unicode"))
        if child.tail: parts.append(child.tail)
    return "".join(parts).strip()

# ======================= LOAD SOURCE =======================
def load_source_bytes(src: str) -> bytes:
    """Загрузить XML: поддержка file:// и http(s) с ретраями."""
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

# ======================= BRAND normalize =======================
SUPPLIER_BLOCKLIST={"alstyle","al-style","copyline","vtt","akcent","ak-cent"}
UNKNOWN_VENDOR_MARKERS={"нет бренда","unknown","noname","no name","неизвестн","без бренда","-","—"}

BRAND_ALLOW = [
    "HP","Canon","Epson","Brother","Kyocera","Ricoh","Konica Minolta","Xerox","Lexmark","Samsung",
    "OKI","Pantum","Sharp","Toshiba","Dell","Mitsubishi","Minolta","FujiFilm","Fujifilm",
    "NV Print","Hi-Black","G&G","Static Control","Katun","ProfiLine","Cactus","Lomond","WWM","Uniton",
    "TSC","Zebra",
    "SVC","APC","Powercom","PCM","Ippon","Eaton","Vinga",
    "MSI","ASUS","Acer","Lenovo","Apple"
]
BRAND_ALIASES = {
    "hewlett packard":"HP","konica":"Konica Minolta","konica-minolta":"Konica Minolta",
    "powercom":"Powercom","pcm":"Powercom","apc":"APC","msi":"MSI",
    "nvprint":"NV Print","nv print":"NV Print",
    "hi black":"Hi-Black","hiblack":"Hi-Black","hi-black":"Hi-Black",
    "g&g":"G&G","gg":"G&G"
}
def _norm_key(s: str) -> str: return re.sub(r"\s+"," ",(s or "").strip().lower())

def normalize_brand(raw: str) -> str:
    k=_norm_key(raw)
    return "" if (not k) or (k in SUPPLIER_BLOCKLIST) else raw.strip()

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, Dict[str,int]]:
    """Чистим/нормализуем <vendor>: убираем мусорные/поставщицкие бренды."""
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
        else:
            # Пытаемся авто-определить по name/description
            name=get_text(offer,"name"); desc=get_text(offer,"description")
            guess=""
            for b in BRAND_ALLOW:
                if re.search(rf"\b{re.escape(b)}\b", name, re.I) or re.search(rf"\b{re.escape(b)}\b", desc, re.I):
                    guess=b; break
            if guess:
                ET.SubElement(offer,"vendor").text=guess; normalized+=1
    return normalized, dropped

# ======================= PRICE =======================
PriceRule = Tuple[int,int,float,int]  # (lo, hi, percent, adder)

PRICING_RULES: List[PriceRule] = [
    (101,10_000,4.0,3_000),
    (10_001,25_000,4.0,4_000),
    (25_001,50_000,4.0,5_000),
    (50_001,75_000,4.0,7_000),
    (75_001,100_000,4.0,10_000),
    (100_001,150_000,4.0,12_000),
    (150_001,200_000,4.0,15_000),
    (200_001,300_000,4.0,20_000),
    (300_001,400_000,4.0,25_000),
    (400_001,500_000,4.0,30_000),
    (500_001,750_000,4.0,40_000),
    (750_001,1_000_000,4.0,50_000),
    (1_000_001,1_500_000,4.0,70_000),
    (1_500_001,2_000_000,4.0,90_000),
    (2_000_001, 9_999_999,4.0,100_000),
]

def _force_tail_900(x: float) -> int:
    v=int(round(x))
    return int(str(v)[:-3]+"900") if v>=1000 else (900 if v>0 else 0)

def pick_dealer_price(offer: ET.Element) -> Tuple[Optional[float], str]:
    """Ищем «дилерскую» цену (приоритет: <prices type~dealer/опт/b2b> → прямые поля → RRP)."""
    # 1) Внутренний блок <prices> поставщика
    dealer=None
    prices_node=offer.find("prices")
    if prices_node is not None:
        for p in prices_node.findall("price"):
            t=(p.attrib.get("type") or "").lower()
            if any(k in t for k in ("dealer","опт","wholesale","b2b")):
                try:
                    dealer=float((p.text or "0").replace(",","."))
                    if dealer>0: return dealer, "prices_dealer"
                except Exception: pass
    # 2) Прямые поля
    for fname in INTERNAL_PRICE_TAGS:
        node=offer.find(fname)
        if node is not None:
            try:
                dealer=float((node.text or "0").replace(",","."))
                if dealer>0: return dealer, "direct_field"
            except Exception: pass
    # 3) RRP (fallback)
    for fname in ("rrp","RRP","oldprice","oldPrice","price"):
        node=offer.find(fname)
        if node is not None:
            try:
                dealer=float((node.text or "0").replace(",","."))
                if dealer>0: return dealer, "rrp_fallback"
            except Exception: pass
    return None, "missing"

def strip_supplier_price_blocks(offer: ET.Element) -> int:
    """Убираем все внутренние ценовые теги поставщика из публичного YML."""
    removed=0
    for tag in (list(INTERNAL_PRICE_TAGS)+["prices","oldprice","oldPrice"]):
        for node in list(offer.findall(tag)):
            offer.remove(node); removed+=1
    return removed

def _remove_all_price_nodes(offer: ET.Element) -> int:
    removed=0
    for node in list(offer.findall("price")):
        offer.remove(node); removed+=1
    return removed

def compute_retail(dealer: float, rules: List[PriceRule]) -> Optional[int]:
    """Применяем проценты+надбавки → форсим окончание ...900."""
    if dealer<=0: return None
    if dealer>=PRICE_CAP_THRESHOLD: return PRICE_CAP_VALUE
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

# ======================= PARAMS / TEXT =======================
_key = lambda s: re.sub(r"\s+"," ",(s or "").strip()).lower()
UNWANTED_PARAM_NAME_RE = re.compile(
    r"^(?:\s*(?:благотворительн\w*|снижена\s*цена|новинк\w*|"
    r"артикул(?:\s*/\s*штрихкод)?|оригинальн\w*\s*код|штрихкод|"
    r"код\s*тн\s*вэд(?:\s*eaeu)?|код\s*тнвэд(?:\s*eaeu)?|тн\s*вэд|тнвэд|"
    r"tn\s*ved|hs\s*code)\s*)$",
    re.I
)
SAFE_SPEC_WHITELIST = {"напряжение","мощность","ёмкость","емкость","технология печати","цвет печати","ресурс"}
KASPI_CODE_NAME_RE = re.compile(r"^\s*(?:Код|Артикул)\s*$", re.I)

def _norm_text(s: str) -> str:
    s=(s or "").strip()
    s=re.sub(r"\s+", " ", s)
    s=s.replace("�","")
    return s

def _looks_like_code_value(v: str) -> bool:
    return bool(re.search(r"\b(?:AS|CL|VT|AC|NP)?\d{4,}\b", v, re.I))

SPEC_PREFERRED_ORDER = [
    "модель","совместим","ресурс","цвет печати","технология печати","ёмкость","емкость",
    "мощность","напряжение","входное напряжение","выходное напряжение","частота"
]

def _rank_key(k: str) -> Tuple[int,str]:
    k_low=_norm_text(k).lower()
    for i, pref in enumerate(SPEC_PREFERRED_ORDER):
        if k_low.startswith(pref): return (i, k)
    return (1000, k_low)

def has_specs_in_raw_desc(raw_desc_html: str) -> bool:
    if not raw_desc_html: return False
    s = raw_desc_html.lower()
    return ("<ul" in s and "<li" in s and "характерист" in s) or re.search(r"<h\d[^>]*>\s*характерист", s)

def build_specs_pairs_from_params(offer: ET.Element) -> List[Tuple[str,str]]:
    pairs=[]; seen=set()
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name=(p.attrib.get("name") or "").strip()
        raw_val =(p.text or "").strip()
        if not raw_name or not raw_val: continue
        name_norm = _norm_text(raw_name); val_norm = raw_val.strip()
        if KASPI_CODE_NAME_RE.fullmatch(raw_name) or UNWANTED_PARAM_NAME_RE.match(raw_name): continue
        if name_norm == "назначение" and val_norm.lower() == "да": continue
        if _looks_like_code_value(val_norm) and name_norm not in SAFE_SPEC_WHITELIST: continue
        pairs.append((raw_name.strip(), canon_units(raw_name, raw_val.strip())))
    out=[]; seen=set()
    for n,v in pairs:
        k=_norm_text(n)
        if k in seen: continue
        seen.add(k); out.append((n,v))
    return out

def build_specs_html_from_params(offer: ET.Element) -> str:
    """HTML-список <li> из тегов <param>. Показываем только если в «родном» описании нет характеристик."""
    pairs = build_specs_pairs_from_params(offer)
    if not pairs: return ""
    pairs_sorted = sorted(pairs, key=lambda kv: _rank_key(kv[0]))
    parts = ["<h3>Характеристики</h3>", "<ul>"]
    for name, val in pairs_sorted:
        parts.append(f"  <li><strong>{_html_escape_in_cdata_safe(name)}:</strong> {_html_escape_in_cdata_safe(val)}</li>")
    parts.append("</ul>")
    return "\n".join(parts)

# ======= Детект «типа» для лид-блока =======
BRAND_WORDS = [
    "HP","Canon","Epson","Brother","Kyocera","Ricoh","Konica","Minolta","Xerox","Lexmark","Pantum",
    "ThinkPad","IdeaPad","Legion","ProArt","ROG","TUF","Nitro","Predator","Vostro","Latitude","Inspiron","MacBook","iMac",
    "Pro","Max","Ultra","Series","Elite","DeskJet","LaserJet","OfficeJet","PIXMA","EcoTank","WorkForce","Laser","Ink"
]
MODEL_ANCHORS = [
    "L","XP","WF","WorkForce","EcoTank",
    "FS","TASKalfa","ECOSYS",
    "Aficio","SP","MP","IM",
    "MX","BP","B","C","P2500","M6500","CM","DL","DP"
]
AS_INTERNAL_ART_RE = re.compile(r"^AS\d+", re.I)
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

def detect_kind(name: str, params_pairs: List[Tuple[str,str]]) -> str:
    """Грубый детектор вида товара (упрощённый)."""
    s=(name or "").lower()
    if any(k in s for k in ("картридж","toner")): return "cartridge"
    if any(k in s for k in ("ups","ибп","бесперебойник")): return "ups"
    return "generic"

def canon_units(name: str, val: str) -> str:
    """Нормализация единиц измерения (Вт, В и т.п.)."""
    v = val
    v = re.sub(r"\bватт(?:ов)?\b", "Вт", v, flags=re.I)
    v = re.sub(r"\bвольт(?:ов)?\b", "В", v, flags=re.I)
    v = re.sub(r"\b(ампер[-\s]?час(?:ов)?)\b", "А·ч", v, flags=re.I)
    v = v.replace(" град.", " °C").replace("градусов", "°C")
    return v

def extract_kv_from_description(raw_desc: str) -> List[Tuple[str,str]]:
    """Пытаемся вытащить пары «ключ: значение» из исходного описания."""
    out=[]
    for m in re.finditer(r"(?:^|[\n\r])\s*([A-Za-zА-Яа-яЁё0-9\s\-\.\(\)/]+?)\s*:\s*([^\n\r]+)", raw_desc):
        k=_norm_text(m.group(1)); v=m.group(2).strip()
        if not k or not v: continue
        if _looks_like_code_value(v) and k not in SAFE_SPEC_WHITELIST: continue
        out.append((k,v))
    return out

def _html_escape_in_cdata_safe(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<","&lt;").replace(">","&gt;")

def choose(seq: List[str], seed: int, salt: int) -> str:
    random.seed((seed+salt) & 0xFFFFFFFF)
    return seq[random.randint(0, len(seq)-1)] if seq else ""

def build_lead_text(name: str, vendor: str, kind: str, kv_all: Dict[str,str], seed: int) -> str:
    """Генерация первой строки-лида (краткий маркер ключ.признака)."""
    short = "Коротко"
    variants = [
        "Кратко о плюсах","Чем удобен","Ключевые преимущества","Что вы получаете с",
        "Хороший выбор","Удачный выбор","Надежный вариант"
    ]
    p = variants[seed % len(variants)]
    mark = ""
    if vendor: mark = vendor
    if kind=="cartridge":
        res_key = next((k for k in kv_all if k.startswith("ресурс")), "")
        if res_key: mark = (mark+" • "+kv_all[res_key]) if mark else kv_all[res_key]
        elif "цвет печати" in kv_all: mark = (mark+" • "+kv_all["цвет печати"]) if mark else kv_all["цвет печати"]
    elif kind=="ups":
        power = kv_all.get("мощность (bt)") or kv_all.get("мощно... (bт)") or kv_all.get("мощность (вт)") or kv_all.get("мощность")
        if power: mark = (mark+" • "+power) if mark else power
    return f"{short}: {p}" + (f" ({mark})" if mark else "")

def build_lead_html(offer: ET.Element, raw_desc_text_for_kv: str, params_pairs: List[Tuple[str,str]]) -> Tuple[str, Dict[str,str]]:
    """Лид + собранные характеристики для последующего HTML."""
    name=get_text(offer,"name").strip()
    vendor=get_text(offer,"vendor").strip()
    kind=detect_kind(name, params_pairs)
    s_id = offer.attrib.get("id") or get_text(offer,"vendorCode") or get_text(offer,"name")
    seed = int(hashlib.md5((s_id or "").encode("utf-8")).hexdigest()[:8], 16)

    kv_from_desc = extract_kv_from_description(raw_desc_text_for_kv)
    kv_all = {k.strip().lower(): v for k,v in (params_pairs + kv_from_desc)}
    lead = build_lead_text(name, vendor, kind, kv_all, seed)
    return lead, kv_all

def build_reviews_html(offer: ET.Element) -> str:
    """Блок «Отзывы» (3 шт.)."""
    s_id = offer.attrib.get("id") or get_text(offer,"vendorCode") or get_text(offer,"name")
    seed = int(hashlib.md5((s_id or "").encode("utf-8")).hexdigest()[:8], 16)
    names = ["Алексей","Мария","Сергей","Дана","Ирина","Руслан","Ержан","Алина","Кайрат","Жанна"]
    comments = [
        "Сделал(а) выбор и не пожалел(а). Качество отличное.",
        "Работает как и ожидал(а). Доставка быстрая.",
        "Соответствует описанию. Советую к покупке!"
    ]
    parts=[]
    for i in range(3):
        name = choose(names, seed, i+1)
        comment = choose(comments, seed, i+2)
        stars = "★"*5
        city = choose(CITIES, seed, i+3)
        parts.append(
            f"<p>👤 <strong>{_html_escape_in_cdata_safe(name)}</strong>, { _html_escape_in_cdata_safe(city) } — {stars}<br>"
            f"«{ _html_escape_in_cdata_safe(comment) }»</p>"
        )
    return "\n".join(parts)

# === Sticky cache ===
def load_seo_cache(path: str) -> Dict[str, dict]:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f: return json.load(f)
        except Exception:
            return {}
    if os.path.exists(LEGACY_CACHE_PATH):
        try:
            with open(LEGACY_CACHE_PATH, "r", encoding="utf-8") as f: return json.load(f)
        except Exception:
            return {}
    return {}

def save_seo_cache(path: str, data: Dict[str, dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def now_almaty() -> datetime:
    if ZoneInfo: return datetime.now(ZoneInfo("Asia/Almaty"))
    return datetime.utcnow() + timedelta(hours=5)

def now_utc_str() -> str: return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def format_dt_almaty(dt: datetime) -> str:
    if not dt.tzinfo and ZoneInfo: dt = dt.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Asia/Almaty"))
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def next_build_time_almaty() -> datetime:
    n=now_almaty()
    return (n.replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=1))

def should_periodic_refresh(prev_dt_utc: Optional[datetime]) -> bool:
    mode = SEO_REFRESH_MODE
    if mode == "off": return False
    now_alm = now_almaty()
    if mode == "monthly_1":
        # Обновляем 1-го числа каждого месяца
        return now_alm.day == 1
    if mode == "days":
        if not prev_dt_utc: return True
        last_alm = prev_dt_utc if prev_dt_utc.tzinfo else prev_dt_utc.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Asia/Almaty") if ZoneInfo else timezone(timedelta(hours=5)))
        return (now_alm - last_alm) >= timedelta(days=SEO_REFRESH_DAYS)
    return False

def extract_last_seo_update_alm(shop_el: ET.Element) -> Tuple[bool, str]:
    """Возвращает (changed, last_update_alm_str). changed=True если рефреш по правилам."""
    # Ищем комментарий FEED_META предыдущей сборки для даты
    out_shop = shop_el
    last_alm = None
    for node in list(out_shop.iter()):
        if isinstance(node.tag, str): continue
        # Комментарий → пытаемся найти built_alm
        txt = str(node) if hasattr(node, '__str__') else ""
        for ts in re.findall(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", txt):
            try:
                utc_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                alm_dt = utc_dt.astimezone(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(utc_dt.timestamp()+5*3600)
                if (last_alm is None) or (alm_dt > last_alm): last_alm = alm_dt
            except Exception:
                continue
    if not last_alm: last_alm = now_almaty()
    return should_periodic_refresh(last_alm.astimezone(timezone.utc)), format_dt_almaty(last_alm)

# ======================= CDATA PLACEHOLDER REPLACER =======================
def _replace_html_placeholders_with_cdata(xml_text: str) -> str:
    """Заменяет <description>[[[HTML]]]...[[[/HTML]]]</description> на CDATA-блок."""
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

def ensure_placeholder_pictures(shop_el: ET.Element) -> Tuple[int,int]:
    """Если у оффера нет <picture> — пытаемся подставить бренд/категорию/дефолт."""
    if not PLACEHOLDER_ENABLE: return 0,0
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0,0
    added=checked=0
    for off in offers_el.findall("offer"):
        pics=list(off.findall("picture"))
        if pics: continue
        checked+=1
        brand=(get_text(off,"vendor") or "").strip()
        tried=[]
        if brand:
            url=f"{PLACEHOLDER_BRAND_BASE}/{urllib.parse.quote(brand)}.{PLACEHOLDER_EXT}"
            tried.append(url)
            if url_exists(url):
                ET.SubElement(off,"picture").text=url; added+=1; continue
        # категория → по слову из name
        name=get_text(off,"name").lower()
        cat = "printer" if "принтер" in name else "laptop" if "ноутбук" in name else ""
        if cat:
            url=f"{PLACEHOLDER_CATEGORY_BASE}/{cat}.{PLACEHOLDER_EXT}"
            tried.append(url)
            if url_exists(url):
                ET.SubElement(off,"picture").text=url; added+=1; continue
        # дефолт
        if PLACEHOLDER_DEFAULT_URL:
            ET.SubElement(off,"picture").text=PLACEHOLDER_DEFAULT_URL; added+=1
    return added,checked

# ======================= VENDORCODE =======================
def _normalize_code(s: str) -> str:
    s=(s or "").strip()
    s=re.sub(r"[^A-Za-z0-9]+","",s)
    return s

def _extract_article_from_name(name: str) -> str:
    # Ищем «похожее на артикул» токен в name
    for tok in re.findall(r"[A-Za-z]{1,6}\d{2,}", name or ""):
        return tok
    return ""

def _extract_article_from_url(url: str) -> str:
    try:
        tail=url.rsplit("/",1)[-1]
        return _normalize_code(tail)
    except Exception:
        return ""

def ensure_vendorcode_with_article(shop_el: ET.Element, prefix: str="AS", create_if_missing: bool=True) -> Tuple[int,int,int,int]:
    """Гарантируем vendorCode с префиксом и синхронизируем offer/@id."""
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

def purge_offer_tags_and_attrs_after(offer:ET.Element)->Tuple[int,int]:
    """Удаляем мусорные теги + атрибуты у конкретного оффера (после основных преобразований)."""
    removed_tags=removed_attrs=0
    for t in PURGE_TAGS_AFTER:
        for n in list(offer.findall(t)):
            offer.remove(n); removed_tags+=1
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib: del offer.attrib[a]; removed_attrs+=1
    if DROP_STOCK_TAGS:
        for t in ("stock_quantity","quantity_in_stock","quantity","Stock"):
            for n in list(offer.findall(t)):
                offer.remove(n); removed_tags+=1
    return removed_tags, removed_attrs

# ======================= AVAILABLE / CURRENCY =======================
def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int,int,Dict[str,int]]:
    """Переносим available в атрибут <offer available="...">, удаляем лишнее."""
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,{})
    t_true=t_false=t_missed=0; src={}
    for offer in offers_el.findall("offer"):
        val=None
        # разные варианты у поставщика
        for t in ("available","isAvailable","instock","inStock","Available"):
            node=offer.find(t)
            if node is not None and node.text:
                val = (node.text.strip().lower() in {"1","true","yes","есть","в наличии"})
                offer.remove(node); break
        if val is None:
            # по умолчанию считаем в наличии
            val=True
        offer.attrib["available"] = "true" if val else "false"
        t_true += 1 if val else 0
        t_false += 0 if val else 1
    return t_true,t_false,0,src

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
    """Глобальная функция: упорядочить теги внутри каждого оффера по DESIRED_ORDER."""
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
    """Удаляем любые <categoryId> и добавляем <categoryId>0</categoryId> первым узлом внутри оффера."""
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        remove_all(offer,"categoryId","CategoryId")
        cid=ET.Element("categoryId"); cid.text=os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0,cid); touched+=1
    return touched

# ======================= KEYWORDS =======================
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
STOPWORDS_RU = {"для","и","или","на","в","из","от","по","с","к","до","над","под","о","об","у","без","про","как","это","той","тот","эта","эти",
                "бумага","бумаги","бумаг","черный","чёрный","блок","комплект","набор","тип","модель","модели","формат","новый","новинка"}
STOPWORDS_EN = {"for","and","or","with","of","the","a","an","in","on","to","from","by","this","that","these","those",
                "paper","black","set","type","model","format","new","original","kit","pack"}
GENERIC_DROP = {"изделие","товар","продукция","аксессуар","устройство","оборудование"}

def tokenize_name(name: str) -> List[str]: return WORD_RE.findall(name or "")
def is_content_word(token: str) -> bool:
    t=_norm_text(token)
    return bool(t) and (t not in STOPWORDS_RU) and (t not in STOPWORDS_EN) and (t not in GENERIC_DROP) and (any(ch.isdigit() for ch in t) or "-" in t or len(t)>=3)

def build_bigrams(words: List[str]) -> List[str]:
    out=[]
    for i in range(len(words)-1):
        out.append(f"{words[i]} {words[i+1]}")
    return out

def dedup_preserve_order(seq: List[str]) -> List[str]:
    seen=set(); out=[]
    for x in seq:
        if x in seen: continue
        seen.add(x); out.append(x)
    return out

def translit_ru_to_lat(s: str) -> str:
    """Очень простой транслит (для keywords дублей на латинице)."""
    table = str.maketrans("абвгдеёжзийклмнопрстуфхцыэюяъь", "abvgdeejziyklmnoprstufhcyeyuya''")
    return re.sub(r"[^A-Za-z0-9\- ]+", "", s.lower().translate(table))

def extract_model_tokens(offer: ET.Element) -> List[str]:
    s = get_text(offer,"name") + " " + get_text(offer,"vendorCode")
    # расклеиваем склеенные модели: HPHP → HP HP, и т.п.
    s = "\n".join(_split_joined_models(s))
    out=[]
    for m in MODEL_RE.findall(s):
        if m.upper() in BRAND_WORDS: continue
        out.append(m.upper())
    return dedup_preserve_order(out)

def keywords_from_name_generic(name: str) -> List[str]:
    raw_tokens=tokenize_name(name or "")
    modelish=[t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content=[t for t in raw_tokens if is_content_word(t)]
    bigr=build_bigrams(content)
    norm=lambda tok: tok if re.search(r"[A-Z]{2,}", tok) else tok.capitalize()
    out=modelish[:8]+bigr[:8]+[norm(t) for t in content[:10]]
    return dedup_preserve_order(out)

def geo_tokens() -> List[str]:
    if not SATU_KEYWORDS_GEO: return []
    toks=[
        "Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
        "Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавловск","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"
    ]
    if SATU_KEYWORDS_GEO_LAT:
        toks += [
            "Kazakhstan","Almaty","Astana","Shymkent","Karaganda","Aktobe","Pavlodar","Atyrau","Taraz",
            "Oskemen","Semey","Kostanay","Kyzylorda","Oral","Petropavlovsk","Taldykorgan","Aktau","Temirtau","Ekibastuz","Kokshetau","Rudny"
        ]
    toks=dedup_preserve_order(toks)
    return toks[:max(0,SATU_KEYWORDS_GEO_MAX)]

def build_keywords_for_offer(offer: ET.Element) -> str:
    if SATU_KEYWORDS == "off": return ""
    name=get_text(offer,"name"); vendor=get_text(offer,"vendor").strip()
    parts=[vendor] if vendor else []
    parts += extract_model_tokens(offer) + keywords_from_name_generic(name) + color_tokens(name)
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
    return "".join(out)

COLOR_WORDS = {"черный","чёрный","black","white","white","серый","gray","silver","gold","blue","red","green","pink","beige","brown"}
def color_tokens(name: str) -> List[str]:
    out=[]
    low=(name or "").lower()
    for c in COLOR_WORDS:
        if c in low: out.append(c)
    return out

# ======================= HTML DESCRIPTION =======================
def inject_seo_descriptions(shop_el: ET.Element) -> Tuple[int, Optional[str]]:
    """Генерация HTML-блока: CTA WhatsApp → лид → преимущества → (характеристики из <param> если нужно) → отзывы."""
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0, None
    changed=0
    cache=load_seo_cache(SEO_CACHE_PATH) if SEO_STICKY else {}
    do_refresh, last_update_alm = extract_last_seo_update_alm(shop_el)

    for off in offers_el.findall("offer"):
        name=get_text(off,"name").strip()
        vendor=get_text(off,"vendor").strip()
        raw_desc_html = get_text(off, "description")
        raw_desc_text_for_kv = re.sub(r"<[^>]+>"," ", raw_desc_html or "")

        params_pairs = build_specs_pairs_from_params(off)
        lead_text, kv_all = build_lead_html(off, raw_desc_text_for_kv, params_pairs)

        # Контент HTML
        parts=[
            "[[[HTML]]]",
            '<div style="font-family: Cambria, \'Times New Roman\', serif;">',
            '  <center>',
            '    <a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0"'
            '       style="display:inline-block;background:#27ae60;color:#ffffff;text-decoration:none;padding:10px 20px;border-radius:10px;font-weight:700;">'
            '      НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!',
            '    </a>',
            '  </center>',
            f'  <p><strong>{_html_escape_in_cdata_safe(name)}</strong></p>',
            f'  <p>{_html_escape_in_cdata_safe(lead_text)}</p>',
            '  <h3>Преимущества</h3>',
            '  <ul>',
            '    <li>Надёжная поставка и быстрая обработка заказа</li>',
            '    <li>Гарантия качества и поддержка</li>',
            '    <li>Оптимальная цена с учётом рынка</li>',
            '  </ul>',
        ]

        # Если в «родном» описании нет готовых характеристик — строим из <param>
        if not has_specs_in_raw_desc(raw_desc_html or ""):
            specs_html = build_specs_html_from_params(off)
            if specs_html: parts.append("  " + specs_html)

        parts += [
            '  <h3>Отзывы</h3>',
               build_reviews_html(off),
            '</div>',
            "[[[/HTML]]]"
        ]
        html = "\n".join(parts)

        set_text(off, "description", html)
        changed+=1

    # Кэш (при необходимости)
    if SEO_STICKY:
        save_seo_cache(SEO_CACHE_PATH, cache)

    return changed, last_update_alm

# ======================= KEYWORDS (CITIES) =======================
CITIES = ["Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз","Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавловск","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]

# ======================= MAIN =======================
def ensure_keywords(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for off in offers_el.findall("offer"):
        kw = build_keywords_for_offer(off)
        remove_all(off, "keywords","Keywords")
        if kw: ET.SubElement(off,"keywords").text = kw; touched+=1
    return touched

def ensure_vendor_auto_fill(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    filled=0
    for off in offers_el.findall("offer"):
        ven=off.find("vendor")
        if ven is None or not (ven.text or "").strip():
            # пробуем вытащить из name
            nm=get_text(off,"name")
            for b in BRAND_ALLOW:
                if re.search(rf"\b{re.escape(b)}\b", nm, re.I):
                    ET.SubElement(off,"vendor").text=b; filled+=1; break
    return filled

def enforce_forced_prices(shop_el: ET.Element) -> int:
    """Если на оффере есть _force_price=100 — ставим 100 KZT и убираем флаги."""
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        if offer.attrib.get("_force_price"):
            _remove_all_price_nodes(offer); ET.SubElement(offer, "price").text=str(PRICE_CAP_VALUE)
            offer.attrib.pop("_force_price", None); touched += 1
    return touched

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
    text = "\n".join([f"{k}: {v}" for k,v in rows])
    return f"\n{text}\n"

def remove_specific_params(shop_el: ET.Element) -> int:
    """Удаляем мусорные/служебные <param>."""
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    removed=0
    for off in offers_el.findall("offer"):
        for p in list(off.findall("param")) + list(off.findall("Param")):
            name=(p.attrib.get("name") or "").strip()
            val=(p.text or "").strip()
            if not name or not val: off.remove(p); removed+=1; continue
            if KASPI_CODE_NAME_RE.fullmatch(name) or UNWANTED_PARAM_NAME_RE.match(name):
                off.remove(p); removed+=1; continue
    return removed

def parse_categories_rule(path: str) -> Set[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return {re.sub(r"\s+"," ",line.strip()) for line in f if line.strip()}
    except Exception:
        return set()

def filter_offers_by_categories(shop_el: ET.Element, mode: str, rules_path: str) -> Tuple[int,int]:
    """Фильтрация по categoryId (режим include/exclude)."""
    if mode not in {"include","exclude"}: return 0,0
    rules = parse_categories_rule(rules_path)
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0,0
    kept=removed=0
    for off in list(offers_el.findall("offer")):
        cids=[(n.text or "").strip() for n in off.findall("categoryId")]
        ok = any(cid in rules for cid in cids) if cids else False
        if (mode=="include" and not ok) or (mode=="exclude" and ok):
            offers_el.remove(off); removed+=1
        else:
            kept+=1
    return kept, removed

def build_output_tree(src_root: ET.Element) -> ET.Element:
    """Создаёт каркас выходного YML с <shop><offers> и копирует офферы."""
    out_root=ET.Element("yml_catalog"); shop=ET.SubElement(out_root,"shop"); offers=ET.SubElement(shop,"offers")
    for off in src_root.find("shop").findall("offer"):
        offers.append(deepcopy(off))
    return out_root

def colorize_name(name: str) -> str: return name  # placeholder: без вмешательств в <name>

def main() -> None:
    # 1) Загрузка исходника
    log(f"source: {SUPPLIER_URL}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    # 2) Каркас
    out_root=build_output_tree(src_root)
    out_shop=out_root.find("shop"); out_offers=out_shop.find("offers")

    # 3) (опционально) фильтр по категориям
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        kept, removed = filter_offers_by_categories(out_shop, ALSTYLE_CATEGORIES_MODE, ALSTYLE_CATEGORIES_PATH)
        log(f"Category filter {ALSTYLE_CATEGORIES_MODE}: kept={kept}, removed={removed}")

    # 4) Нормализация брендов/кодов и цен
    ensure_vendor(out_shop)
    filled = ensure_vendor_auto_fill(out_shop); log(f"Vendors auto-filled: {filled}")

    ensure_vendorcode_with_article(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AS"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )
    sync_offer_id_with_vendorcode(out_shop)

    reprice_offers(out_shop, PRICING_RULES)
    forced = enforce_forced_prices(out_shop); log(f"Forced price=100: {forced}")

    removed_params = remove_specific_params(out_shop); log(f"Params removed: {removed_params}")

    ph_added,_ = ensure_placeholder_pictures(out_shop); log(f"Placeholders added: {ph_added}")

    seo_changed, seo_last_update_alm = inject_seo_descriptions(out_shop)
    log(f"SEO blocks touched: {seo_changed}")

    t_true, t_false, _, _ = normalize_available_field(out_shop)
    fix_currency_id(out_shop, default_code="KZT")

    # Финальная чистка мусорных тегов/атрибутов
    for off in out_offers.findall("offer"): purge_offer_tags_and_attrs_after(off)
    # Упорядочивание блоков (используем глобальные DESIRED_ORDER и reorder_offer_children)
    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)

    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    # Красивые отступы для читаемости (Python 3.9+)
    try: ET.indent(out_root, space="  ")
    except Exception: pass

    # FEED_META
    built_alm = now_almaty()
    meta_pairs={
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL or "file",
        "offers_total": len(src_root.find("shop").findall("offer")),
        "offers_written": len(list(out_offers.findall("offer"))),
        "available_true": str(t_true),
        "available_false": str(t_false),
        "built_utc": now_utc_str(),
        "built_alm": format_dt_almaty(built_alm),
        "next_build_alm": format_dt_almaty(next_build_time_almaty()),
        "seo_last_update_alm": seo_last_update_alm or format_dt_almaty(built_alm),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # Запись файла (с CDATA для description)
    xml_text = ET.tostring(out_root, encoding="unicode")
    xml_text = _replace_html_placeholders_with_cdata(xml_text)

    if DRY_RUN:
        print(xml_text)
        return

    try:
        data_bytes = xml_text.encode(ENC, errors="strict")
        with open(OUT_FILE_YML, "wb") as f: f.write(data_bytes)
    except Exception as e:
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
