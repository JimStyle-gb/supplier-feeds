# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle → YML (plain-description cleanup only)

Требование пользователя:
— НЕ менять общую логику пайплайна (цены, вендор, категории, порядок тегов и т.д.).
— В самом конце сделать мягкую чистку ТОЛЬКО содержимого <description> без HTML-оформления
  (убрать лишние пустые строки, хвосты-обрубки, декодировать HTML-сущности, нормализовать 'x' → '×' между цифрами).
"""

from __future__ import annotations

import os, sys, re, time, random, hashlib, urllib.parse, requests, html
from typing import Dict, List, Tuple, Optional, Set
from copy import deepcopy
from xml.etree import ElementTree as ET
from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo  # для времени Алматы в FEED_META
except Exception:
    ZoneInfo = None

# ======================= ПАРАМЕТРЫ ОКРУЖЕНИЯ =======================
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "AlStyle").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/alstyle.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()

TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# Категории: include/exclude/off + правила из файла
ALSTYLE_CATEGORIES_PATH = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt")
ALSTYLE_CATEGORIES_MODE = os.getenv("ALSTYLE_CATEGORIES_MODE", "off").lower()  # off|include|exclude

# Префикс для vendorCode/id
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS")

# Цены: наценка по диапазонам и форс-цена при завышенных исходных
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "100"))

# Ключевые слова
SATU_KEYWORDS          = os.getenv("SATU_KEYWORDS", "auto").lower()  # auto|off
SATU_KEYWORDS_MAXLEN   = int(os.getenv("SATU_KEYWORDS_MAXLEN", "1024"))
SATU_KEYWORDS_MAXWORDS = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "1000"))
SATU_KEYWORDS_GEO      = os.getenv("SATU_KEYWORDS_GEO", "on").lower() in {"on","1","true","yes"}
SATU_KEYWORDS_GEO_MAX  = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))
SATU_KEYWORDS_GEO_LAT  = os.getenv("SATU_KEYWORDS_GEO_LAT", "on").lower() in {"on","1","true","yes"}

# Фото-заглушки (если нет картинок)
PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.al-style.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.al-style.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.al-style.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# Публичный YML: вычищаем внутренние теги
DROP_CATEGORY_ID_TAG    = True
DROP_STOCK_TAGS         = True
PURGE_TAGS_AFTER        = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","article")
INTERNAL_PRICE_TAGS     = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                           "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice",
                           "max_price","maxPrice","oldprice")

# ======================= УТИЛИТЫ =======================
def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARN: {msg}", file=sys.stderr, flush=True)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_almaty() -> datetime:
    try:
        if ZoneInfo:
            try:
                return datetime.now(ZoneInfo("Asia/Almaty"))
            except Exception:
                pass
    except Exception:
        pass
    # fallback: UTC+5
    return datetime.utcfromtimestamp(time.time() + 5*3600)

def format_dt_almaty(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def next_build_time_almaty() -> datetime:
    cur = now_almaty()
    t = cur.replace(hour=1, minute=0, second=0, microsecond=0)
    return t + timedelta(days=1) if cur >= t else t

def load_source_bytes(src: str) -> bytes:
    """Скачиваем/читаем исходный XML поставщика."""
    if not src:
        raise RuntimeError("SUPPLIER_URL не задан")
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path, "rb") as f:
            data = f.read()
        if len(data) < MIN_BYTES:
            raise RuntimeError(f"file too small: {len(data)}")
        return data
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    last_err: Optional[Exception] = None
    for i in range(1, RETRIES + 1):
        try:
            r = sess.get(src, timeout=TIMEOUT_S, headers=headers)
            if r.status_code >= 400:
                raise RuntimeError(f"http {r.status_code}")
            data = r.content or b""
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"body too small: {len(data)}")
            return data
        except Exception as e:
            last_err = e
            time.sleep(RETRY_BACKOFF * (i**1.5))
    raise RuntimeError(f"load failed: {last_err}")

def parse_xml(data: bytes) -> ET.ElementTree:
    return ET.ElementTree(ET.fromstring(data))

def get_text(el: Optional[ET.Element], tag: str) -> str:
    if el is None:
        return ""
    x = el.find(tag)
    return (x.text or "").strip() if x is not None and x.text else ""

def remove_all(el: ET.Element, *tags: str) -> None:
    for t in tags:
        for n in list(el.findall(t)):
            el.remove(n)

def _norm_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().strip())

def inner_html(el: Optional[ET.Element]) -> str:
    """Возвращает «внутренний HTML/текст» узла (без внешнего тега)."""
    if el is None:
        return ""
    parts: List[str] = []
    if el.text:
        parts.append(el.text)
    for child in el:
        parts.append(ET.tostring(child, encoding="unicode"))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts).strip()

# ======================= КАТЕГОРИИ: include/exclude =======================
class CatRule:
    __slots__ = ("raw", "kind", "pattern")
    def __init__(self, raw: str, kind: str, pattern):
        self.raw, self.kind, self.pattern = raw, kind, pattern

def _norm_text(s: str) -> str:
    s = (s or "").replace("\u00A0", " ").lower().replace("ё", "е")
    return re.sub(r"\s+", " ", s).strip()

def _norm_cat(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s*[/>\|]\s*", " / ", s)
    return re.sub(r"\s+"," ", s).strip()

def load_category_rules(path: str) -> Tuple[Set[str], List[CatRule]]:
    """Читаем docs/alstyle_categories.txt: чистые ID и строки/регексы для имён путей."""
    if not path or not os.path.exists(path):
        return set(), []
    data: Optional[str] = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path, "rb") as f:
                b = f.read()
            data = b.decode(enc)
            break
        except Exception:
            continue
    if not data:
        return set(), []
    ids: Set[str] = set()
    rules: List[CatRule] = []
    for raw in (line.strip() for line in data.splitlines()):
        if not raw or raw.startswith("#"):
            continue
        if raw.isdigit():
            ids.add(raw)
            continue
        if raw.startswith("re:"):
            patt = raw[3:].strip()
            try:
                rules.append(CatRule(raw, "regex", re.compile(patt, re.I)))
            except Exception:
                continue
        else:
            rules.append(CatRule(_norm_text(raw), "substr", None))
    return ids, rules

def parse_categories_tree(tree: ET.ElementTree) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,Set[str]]]:
    root = tree.getroot()
    shop_el = root.find("shop") or root.find("Shop")
    id2name: Dict[str,str] = {}
    id2parent: Dict[str,str] = {}
    parent2children: Dict[str,Set[str]] = {}
    cats_root = shop_el.find("categories") or shop_el.find("Categories")
    if cats_root is None:
        return id2name, id2parent, parent2children
    for c in cats_root.findall("category"):
        cid = (c.attrib.get("id") or "").strip()
        if not cid:
            continue
        pid = (c.attrib.get("parentId") or "").strip()
        id2name[cid] = (c.text or "").strip()
        if pid:
            id2parent[cid] = pid
        parent2children.setdefault(pid, set()).add(cid)
    return id2name, id2parent, parent2children

def build_category_path_from_id(cat_id: str, id2name: Dict[str,str], id2parent: Dict[str,str]) -> str:
    names: List[str] = []
    cur = cat_id
    seen: Set[str] = set()
    while cur and cur not in seen and cur in id2name:
        seen.add(cur)
        names.append(id2name.get(cur, ""))
        cur = id2parent.get(cur, "")
    names = [n for n in names if n]
    return " ".join(reversed(names)) if names else ""

def category_matches_name(path_str: str, rules: List[CatRule]) -> bool:
    cat_norm = _norm_text(_norm_cat(path_str))
    for cr in rules:
        if cr.kind == "substr":
            if cr.raw and cr.raw in cat_norm:
                return True
        elif cr.kind == "regex":
            if cr.pattern and cr.pattern.search(cat_norm):
                return True
    return False

def collect_descendants(seed: Set[str], parent2children: Dict[str,Set[str]]) -> Set[str]:
    out: Set[str] = set(seed)
    q = list(seed)
    seen: Set[str] = set(seed)
    while q:
        cur = q.pop(0)
        for ch in parent2children.get(cur, set()):
            if ch not in seen:
                seen.add(ch); out.add(ch); q.append(ch)
    return out

# ======================= ВЕНДОР =======================
UNKNOWN_VENDOR_MARKERS = {"no brand","noname","unknown","неизвест","без бренда","none","—","-"}
SUPPLIER_BLOCKLIST = {"alstyle","al-style","copyline","vtt","akcent","ak-cent"}
COMMON_BRANDS = [
    "HP","Hewlett Packard","Canon","Epson","Brother","Ricoh","Kyocera","Xerox","Lexmark","Panasonic","OKI",
    "Mitsubishi","Sakura","NV Print","Hi-Black","Cactus","G&G",
    "Samsung","Apple","Asus","Acer","Lenovo","Dell","Philips","Sony","Toshiba",
    "Dahua","Hikvision","Imou","Reolink","TP-Link","MikroTik","Ubiquiti","D-Link","Zyxel","Netgear",
    "AOC","ViewSonic","BenQ","Gigabyte","MSI","LG","Huawei","Honor","Realme","Vivo","Xiaomi","OnePlus","Nokia",
    "GeForce","Radeon","NVIDIA","AMD","Intel",
    "Kingston","Crucial","Apacer","A-Data","ADATA","Transcend","Kioxia","Samsung Memory",
    "Seagate","Western Digital","WD","Toshiba Storage",
    "Corsair","Cooler Master","Deepcool","Noctua","be quiet!","Arctic",
    "Thermaltake","NZXT","Lian Li","Fractal Design",
    "Logitech","Razer","SteelSeries","HyperX","Edifier","JBL","Sony Audio",
    "Bosch","Makita","DeWalt","Metabo",
    "TSC","Zebra",
    "SVC","APC","Powercom","PCM","Ippon","Eaton","Vinga"
]
BRAND_ALIASES = {
    "hewlett packard":"HP","konica":"Konica Minolta","konica-minolta":"Konica Minolta",
    "powercom":"Powercom","pcm":"Powercom","apc":"APC","msi":"MSI",
    "nvprint":"NV Print","nv print":"NV Print",
    "hi black":"Hi-Black","hiblack":"Hi-Black","hi-black":"Hi-Black",
    "g&g":"G&G","gg":"G&G"
}

def normalize_brand(raw: str) -> str:
    k = _norm_key(raw)
    return "" if (not k) or (k in SUPPLIER_BLOCKLIST) else raw.strip()

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, Dict[str,int]]:
    """Чистим/нормализуем <vendor>: удаляем мусор/пустое, supplier-бренды, оставляем валидные значения."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0, {}
    normalized = 0
    dropped: Dict[str,int] = {}
    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        txt = (ven.text or "").strip() if ven is not None and ven.text else ""
        if txt:
            canon = normalize_brand(txt)
            if any(m in txt.lower() for m in UNKNOWN_VENDOR_MARKERS) or (not canon):
                if ven is not None:
                    offer.remove(ven)
                    dropped[txt] = dropped.get(txt, 0) + 1
            else:
                if canon != txt and ven is not None:
                    ven.text = canon
                    normalized += 1
        else:
            if ven is not None:
                offer.remove(ven)
    return normalized, dropped

def _find_brand_in_text(txt: str) -> str:
    if not txt:
        return ""
    txt_n = _norm_text(txt)
    ali = { _norm_text(k):v for k,v in BRAND_ALIASES.items() }
    for k, v in ali.items():
        if re.search(rf"\b{k}\b", txt_n, flags=re.I):
            return v
    # прямое совпадение
    for w in COMMON_BRANDS:
        if re.search(rf"\b{re.escape(_norm_text(w))}\b", txt_n, flags=re.I):
            return w
    # первое слово как бренд (если совпадает)
    m = re.search(r"^([a-zа-яё0-9\-\+]+)", txt_n)
    if m:
        cand = m.group(1)
        for b in COMMON_BRANDS:
            if _norm_text(b) == _norm_text(cand):
                return b
    return ""

def build_brand_index(shop_el: ET.Element) -> Dict[str,str]:
    idx: Dict[str,str] = {}
    for b in COMMON_BRANDS:
        idx[_norm_key(b)] = b
    return idx

def guess_vendor_for_offer(offer: ET.Element, brand_index: Dict[str,str]) -> str:
    name  = get_text(offer, "name")
    desc  = inner_html(offer.find("description"))  # читаем, но НЕ меняем
    first = re.split(r"\s+", name.strip())[0] if name else ""
    f_norm = _norm_key(first)
    if f_norm in brand_index:
        return brand_index[f_norm]
    b = _find_brand_in_text(name) or _find_brand_in_text(desc)
    if b:
        return b
    nrm = _norm_text(name)
    for br in COMMON_BRANDS:
        if re.search(rf"\b{re.escape(_norm_text(br))}\b", nrm, flags=re.I):
            return br
    return ""

def ensure_vendor_auto_fill(shop_el: ET.Element) -> int:
    """Если <vendor> пуст — пытаемся угадать по name/description (только чтение)."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    brand_index = build_brand_index(shop_el)
    touched = 0
    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        cur = (ven.text or "").strip() if ven is not None and ven.text else ""
        if cur:
            continue
        guessed = guess_vendor_for_offer(offer, brand_index)
        if guessed:
            ET.SubElement(offer, "vendor").text = guessed
            touched += 1
    return touched

# ======================= ЦЕНЫ =======================
PriceRule = Tuple[int,int,float,int]

PRICE_RULES: List[PriceRule] = [
    (101,        10000,     4.0,  3000),
    (10001,      25000,     4.0,  4000),
    (25001,      50000,     4.0,  5000),
    (50001,      75000,     4.0,  7000),
    (75001,      100000,    4.0, 10000),
    (100001,     150000,    4.0, 12000),
    (150001,     200000,    4.0, 15000),
    (200001,     300000,    4.0, 20000),
    (300001,     400000,    4.0, 25000),
    (400001,     500000,    4.0, 30000),
    (500001,     750000,    4.0, 40000),
    (750001,     1000000,   4.0, 50000),
    (1000001,    1500000,   4.0, 70000),
    (1500001,    2000000,   4.0, 90000),
    (2000001,  10_000_000,  4.0, 100000),
]

PRICE_KEYWORDS_DEALER = re.compile(r"(dealer|дилер|опт|opt|b2b|закуп|закупоч|wholesale|purchase)", re.I)
PRICE_KEYWORDS_RRP    = re.compile(r"(rrp|ррц|ррп|розничн|реком|\bmsrp\b|\bmrp\b)", re.I)

def _force_last_three_to_900(val: float) -> int:
    try:
        n = int(round(float(val)))
        return int(str(n)[:-3] + "900") if n >= 1000 else 900
    except Exception:
        return int(val)

def compute_retail(base_price: float, rules: List[PriceRule]) -> Optional[float]:
    if base_price is None or base_price <= 0:
        return None
    for lo, hi, pct, plus in rules:
        if lo <= base_price <= hi:
            return _force_last_three_to_900(base_price * (1 + pct/100.0) + plus)
    return _force_last_three_to_900(base_price * 1.04 + 100000)  # fallback

def parse_price_number(s: str) -> Optional[float]:
    s = str(s or "").strip()
    s = (s.replace("\xa0", " ")
           .replace(" ", "")
           .replace("KZT","")
           .replace("kzt","")
           .replace("₸","")
           .replace(",", "."))
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except Exception:
        return None

def pick_dealer_price(offer: ET.Element) -> Tuple[Optional[float], str]:
    dealer_candidates: List[float] = []
    rrp_candidates: List[float] = []
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            val = parse_price_number(p.text or "")
            if val is None:
                continue
            t = (p.attrib.get("type") or "")
            if PRICE_KEYWORDS_DEALER.search(t):
                dealer_candidates.append(val)
            elif PRICE_KEYWORDS_RRP.search(t):
                rrp_candidates.append(val)
    if dealer_candidates:
        return (min(dealer_candidates), "prices_dealer")

    # прямые поля
    for tag in ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                "b2b_price","b2bPrice","price","oldprice","Price","Oldprice"):
        n = offer.find(tag)
        val = parse_price_number(n.text or "") if n is not None else None
        if val and val > 0:
            return (val, "direct_field")

    if rrp_candidates:
        return (min(rrp_candidates), "rrp_fallback")
    return (None, "missing")

def _remove_all_price_nodes(offer: ET.Element) -> None:
    for tag in ("price","oldprice","Price","Oldprice"):
        remove_all(offer, tag)

def strip_supplier_price_blocks(offer: ET.Element) -> int:
    removed = 0
    for tag in INTERNAL_PRICE_TAGS:
        removed += len(list(offer.findall(tag)))
        remove_all(offer, tag)
    return removed

def reprice_offers(shop_el: ET.Element, rules: List[PriceRule]) -> Tuple[int,int,int,Dict[str,int]]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0,0,{"missing":0})
    updated = skipped = total = 0
    src_stats = {"prices_dealer":0,"direct_field":0,"rrp_fallback":0,"missing":0}
    for offer in offers_el.findall("offer"):
        total += 1
        if offer.attrib.get("_force_price", "") == "100":
            skipped += 1
            strip_supplier_price_blocks(offer)
            continue
        dealer, src = pick_dealer_price(offer)
        src_stats[src] = src_stats.get(src, 0) + 1
        if dealer is None or dealer <= 100:
            skipped += 1
            strip_supplier_price_blocks(offer)
            continue
        newp = compute_retail(dealer, rules)
        if newp is None:
            skipped += 1
            strip_supplier_price_blocks(offer)
            continue
        _remove_all_price_nodes(offer)
        ET.SubElement(offer, "price").text = str(int(newp))
        strip_supplier_price_blocks(offer)
        updated += 1
    return updated, skipped, total, src_stats

# ======================= ПАРАМЕТРЫ/МУСОР =======================
KASPI_CODE_NAME_RE = re.compile(r"^(mb|mbrk|kaspi|код|артикул|sku|vendorcode)[ _\-]*", re.I)
UNWANTED_PARAM_NAME_RE = re.compile(r"^(назначение|особенности|безопасность|подарок|сертификат|таможня|бренд|vendor|поставщик|supplier|цвет|color|гарантия|вес|объем|объём)\b", re.I)

def _value_is_empty_or_noise(val: str) -> bool:
    t = (val or "").strip()
    return (not t) or (t in {"-","—","/","."}) or (len(_norm_text(t)) <= 1)

def cleanup_param_blocks(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    removed = 0
    for offer in offers_el.findall("offer"):
        seen: Set[str] = set()
        for tag in ("param","Param"):
            for p in list(offer.findall(tag)):
                nm = (p.attrib.get("name") or "").strip()
                val = (p.text or "").strip()
                if KASPI_CODE_NAME_RE.fullmatch(nm) or UNWANTED_PARAM_NAME_RE.match(nm):
                    offer.remove(p); removed += 1; continue
                if _value_is_empty_or_noise(val):
                    offer.remove(p); removed += 1; continue
                key = _norm_text(nm)
                if key in seen:
                    offer.remove(p); removed += 1; continue
                seen.add(key)
    return removed

# ======================= ФОТО-ПЛЕЙСХОЛДЕРЫ ==============
_url_head_cache: Dict[str,bool] = {}
def url_exists(url: str) -> bool:
    if not url:
        return False
    if url in _url_head_cache:
        return _url_head_cache[url]
    try:
        r = requests.head(url, timeout=PLACEHOLDER_HEAD_TIMEOUT, allow_redirects=True)
        ok = (200 <= r.status_code < 400)
    except Exception:
        ok = False
    _url_head_cache[url] = ok
    return ok

def _slug(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\-]+","-", s)
    return re.sub(r"\-+","-", s).strip("-")

def _placeholder_url_brand(vendor: str) -> str:
    return f"{PLACEHOLDER_BRAND_BASE}/{_slug(vendor)}.{PLACEHOLDER_EXT}"

def detect_kind(name: str) -> str:
    n = (name or "").lower()
    if "ноутбук" in n or "laptop" in n or "ultrabook" in n: return "laptop"
    if "монитор" in n or "monitor" in n: return "monitor"
    if "принтер" in n or "printer" in n: return "printer"
    if "мыш" in n or "mouse" in n: return "mouse"
    if "клавиатур" in n or "keyboard" in n: return "keyboard"
    if "роутер" in n or "маршрутизатор" in n or "router" in n: return "router"
    return "generic"

def _placeholder_url_category(kind: str) -> str:
    return f"{PLACEHOLDER_CATEGORY_BASE}/{_slug(kind)}.{PLACEHOLDER_EXT}"

def ensure_placeholder_pictures(shop_el: ET.Element) -> Tuple[int,int]:
    if not PLACEHOLDER_ENABLE:
        return (0,0)
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0)
    added = skipped = 0
    for offer in offers_el.findall("offer"):
        pics = list(offer.findall("picture"))
        has_pic = any((p.text or "").strip() for p in pics)
        if has_pic:
            continue
        vendor = get_text(offer, "vendor").strip()
        name   = get_text(offer, "name").strip()
        kind   = detect_kind(name)
        picked = ""
        if vendor:
            u_brand = _placeholder_url_brand(vendor)
            if url_exists(u_brand):
                picked = u_brand
        if not picked:
            u_cat = _placeholder_url_category(kind)
            if url_exists(u_cat):
                picked = u_cat
        if not picked:
            picked = PLACEHOLDER_DEFAULT_URL
        ET.SubElement(offer, "picture").text = picked
        added += 1
    return (added, skipped)

# ======================= НАЛИЧИЕ/ID/ПОРЯДОК/ВАЛЮТА =======================
TRUE_WORDS  = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS = {"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}

def _parse_bool_str(s: str) -> Optional[bool]:
    v = _norm_text(s or "")
    return True if v in TRUE_WORDS else False if v in FALSE_WORDS else None

def derive_available(offer: ET.Element) -> Tuple[bool,str]:
    # 1) quantity_in_stock / quantity
    for tag in ("quantity_in_stock","quantity","stock","Stock"):
        n = offer.find(tag)
        if n is not None:
            try:
                q = int(re.sub(r"[^\d]+","", n.text or "0"))
                return (q > 0, "stock")
            except Exception:
                pass
    # 2) status
    for tag in ("status","Status"):
        n = offer.find(tag)
        if n is not None:
            s = (n.text or "").lower().strip()
            if s in {"в наличии","есть","in stock","available","true","1"}:
                return (True, "status")
            if s in {"нет в наличии","нет","под заказ","preorder","unavailable","false","0"}:
                return (False, "status")
    # 3) fallback: если price>0 → true
    p = offer.find("price")
    if p is not None:
        try:
            return (int(re.sub(r"[^\d]+","", p.text or "0")) > 0, "price")
        except Exception:
            pass
    return (False, "fallback")

def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int,int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0,0,0)
    t_cnt=f_cnt=st_cnt=ss_cnt=0
    for offer in offers_el.findall("offer"):
        b, src = derive_available(offer)
        remove_all(offer, "available")
        offer.attrib["available"] = "true" if b else "false"
        if b: t_cnt+=1
        else: f_cnt+=1
        if src=="stock": st_cnt+=1
        if src=="status": ss_cnt+=1
        if DROP_STOCK_TAGS:
            remove_all(offer, "quantity_in_stock","quantity","stock","Stock")
    return t_cnt, f_cnt, st_cnt, ss_cnt

ARTICUL_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)

def _extract_article_from_name(name: str) -> str:
    if not name: return ""
    m = ARTICUL_RE.search(name)
    return (m.group(1) if m else "").upper()

def _extract_article_from_url(url: str) -> str:
    if not url: return ""
    try:
        path = urllib.parse.urlparse(url).path.rstrip("/")
        last = re.sub(r"\.(html?|php|aspx?)$","", path.split("/")[-1], flags=re.I)
        m = ARTICUL_RE.search(last)
        return (m.group(1) if m else last).upper()
    except Exception:
        return ""

def ensure_id_vendorcode(shop_el: ET.Element) -> Tuple[int,int,int,int]:
    """Проставляем/правим vendorCode и id (id=vendorCode). Префиксуем AS при необходимости."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0,0,0)
    total_prefixed = created = filled_from_art = fixed_bare = 0
    for offer in offers_el.findall("offer"):
        vc = offer.find("vendorCode")
        txt = (vc.text or "").strip() if vc is not None and vc.text else ""
        if not txt:
            art = _extract_article_from_name(get_text(offer, "name")) or _extract_article_from_url(get_text(offer, "url"))
            if art:
                vc = vc or ET.SubElement(offer, "vendorCode")
                vc.text = art
                filled_from_art += 1
                txt = art
            else:
                continue
        # префикс
        if not txt.upper().startswith(VENDORCODE_PREFIX.upper()):
            vc.text = f"{VENDORCODE_PREFIX}{txt}"
            total_prefixed += 1
        # id=vendorCode
        if offer.attrib.get("id") != (vc.text or "").strip():
            offer.attrib["id"] = (vc.text or "").strip()
            fixed_bare += 1
    return total_prefixed, created, filled_from_art, fixed_bare

def sync_offer_id_with_vendorcode(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
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

def purge_offer_tags_and_attrs_after(offer: ET.Element) -> Tuple[int,int]:
    removed_tags = removed_attrs = 0
    for t in PURGE_TAGS_AFTER:
        for node in list(offer.findall(t)):
            offer.remove(node); removed_tags += 1
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib:
            offer.attrib.pop(a, None); removed_attrs += 1
    return removed_tags, removed_attrs

def fix_currency_id(shop_el: ET.Element, default_code: str = "KZT") -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    fixed = 0
    for offer in offers_el.findall("offer"):
        c = offer.find("currencyId")
        if c is None:
            ET.SubElement(offer, "currencyId").text = default_code
            fixed += 1
        else:
            cur = (c.text or "").strip().upper()
            if not cur:
                c.text = default_code; fixed += 1
            elif cur not in {"KZT","KGS","RUB","USD","EUR"}:
                c.text = default_code; fixed += 1
    return fixed

def reorder_offer_children(shop_el: ET.Element) -> None:
    """Переупорядочиваем теги внутри offer по твоему стандарту."""
    order = ["categoryId","vendorCode","name","price","picture","vendor","currencyId",
             "description","param","keywords"]
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return
    for off in offers_el.findall("offer"):
        kids = list(off)
        def _key(node: ET.Element) -> Tuple[int,int]:
            tag = node.tag
            try:
                i = order.index(tag)
            except Exception:
                i = 999
            if tag == "picture":
                return (i, 0)
            return (i, 1)
        kids.sort(key=_key)
        for k in list(off):
            off.remove(k)
        for k in kids:
            off.append(k)

def ensure_categoryid_zero_first(shop_el: ET.Element) -> int:
    """Вставляем <categoryId>0</categoryId> первым элементом оффера."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        remove_all(offer, "categoryId", "CategoryId")
        cid = ET.Element("categoryId"); cid.text = os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0, cid)
        touched += 1
    return touched

# ======================= ПОСТ-ЧИСТКА ОПИСАНИЙ (только текст) =======================
_CLEAN_STOPLINES = {
    "и подключения",
}

def _clean_desc_text(s: str) -> str:
    if not s:
        return s
    try:
        # 1) нормализуем переводы строк и пробелы
        s = s.replace("\r\n", "\n").replace("\r", "\n").replace("\u00A0", " ")
        # 2) декодируем HTML-сущности (&nbsp;,&quot;,&amp;...)
        try:
            s = html.unescape(s)
        except Exception:
            pass
        # 3) удаляем хвостовые пробелы на строках
        s = re.sub(r"[ \t]+\n", "\n", s, flags=re.M)
        # 4) схлопываем пачки пустых строк (3+ → 1)
        s = re.sub(r"\n{3,}", "\n\n", s)
        # 5) удаляем одиночные «обрубки»-строки из стоп-листа
        lines = s.split("\n")
        out_lines = []
        for ln in lines:
            ln_stripped = ln.strip().strip(":").strip()
            if ln_stripped.lower() in _CLEAN_STOPLINES and ln_stripped.lower() == ln.strip().lower():
                continue
            out_lines.append(ln)
        s = "\n".join(out_lines)
        # 6) нормализуем 'x'/'х' между цифрами → знак умножения ×
        s = re.sub(r"(?<=\d)[xхXХ](?=\d)", "×", s)
        # 7) финальный trim
        s = s.strip()
        return s
    except Exception:
        return s

def post_clean_descriptions(shop_el: ET.Element) -> int:
    """Мягкая чистка <description>: без HTML-оформления, только текстовая нормализация."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        d = offer.find("description")
        if d is None:
            continue
        raw = inner_html(d)  # читаем как есть
        cleaned = _clean_desc_text(raw)
        if cleaned != raw:
            # перезаписываем содержимое description как простой текст
            for child in list(d):
                d.remove(child)
            d.text = cleaned
            touched += 1
    return touched

# ======================= КЛЮЧЕВЫЕ СЛОВА =======================
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
STOPWORDS_RU = {"для","и","или","на","в","из","от","по","с","к","до","при","через","над","под","при","между","из-за","из-под","поэтому","также","же","ли","да","нет","бы","были","был","была","быть","есть","будет","будут","у","во","со","ко","ну","а","но","же","то","же","то","ещё","еще","такое","такой","такая","такие","этот","эта","эти","тот","та","те","и т.д.","и др.","др.","т.д.","и пр.","прочее","разное","разн.","все","всё","всего","всем","всеми","всех","раз","раза","разов","штук","шт","штука","штуки","штук","в комплекте","комплект","набор","тип","модель","модели","формат","новый","новинка"}
STOPWORDS_EN = {"for","and","or","with","of","the","a","an","to","in","on","by","is","are","be","this","that","these","those","new","original","type","model","set","kit","pack"}
GENERIC_DROP = {"изделие","товар","продукция","аксессуар","устройство","оборудование"}

def tokenize_name(name: str) -> List[str]:
    return WORD_RE.findall(name or "")

def is_content_word(token: str) -> bool:
    t = _norm_text(token)
    return bool(t) and (t not in STOPWORDS_RU) and (t not in STOPWORDS_EN) and (t not in GENERIC_DROP) and (any(ch.isdigit() for ch in t) or "-" in t or len(t)>=3)

def build_bigrams(words: List[str]) -> List[str]:
    out: List[str] = []
    for i in range(len(words)-1):
        a,b = words[i], words[i+1]
        if is_content_word(a) and is_content_word(b):
            out.append(f"{a} {b}")
    return out

def dedup_preserve_order(seq: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def translit_ru_to_lat(s: str) -> str:
    table = str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    out = (s or "").lower().translate(table)
    out = re.sub(r"[^a-z0-9\- ]+","", out)
    return re.sub(r"\s+","-", out).strip("-")

def color_tokens(name: str) -> List[str]:
    out: List[str] = []
    low = (name or "").lower()
    mapping = {"жёлт":"желтый","желт":"желтый","yellow":"yellow","черн":"черный","black":"black","син":"синий","blue":"blue",
               "красн":"красный","red":"red","зелен":"зеленый","green":"green","бел":"белый","white":"white","сер":"серый","grey":"grey","silver":"silver","серебр":"серебряный","cyan":"cyan","magenta":"magenta"}
    for k,val in mapping.items():
        if k in low:
            out.append(val)
    return dedup_preserve_order(out)

MODEL_RE = re.compile(r"\b([A-Z][A-Z0-9\-]{2,})\b", re.I)
AS_INTERNAL_ART_RE = re.compile(r"^AS\d+", re.I)

def extract_model_tokens(offer: ET.Element) -> List[str]:
    """Извлекаем модельные токены из name/description (description только читаем)."""
    tokens: Set[str] = set()
    for src in (get_text(offer,"name"), inner_html(offer.find("description"))):
        if not src:
            continue
        for m in MODEL_RE.findall(src or ""):
            t = m.upper()
            if AS_INTERNAL_ART_RE.match(t) or not (re.search(r"[A-Z]", t) and re.search(r"\d", t)) or len(t) < 5:
                continue
            tokens.add(t)
    return sorted(tokens, key=lambda s: (len(s), s))

def keywords_from_name_generic(name: str) -> List[str]:
    toks = tokenize_name(name)
    toks += build_bigrams(toks)
    toks = [t for t in toks if is_content_word(t)]
    return dedup_preserve_order(toks)

def geo_tokens() -> List[str]:
    if not SATU_KEYWORDS_GEO:
        return []
    toks = ["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз","Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавловск","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный",
            "Kazakhstan","Almaty","Astana","Shymkent","Karaganda","Aktobe","Pavlodar","Atyrau","Taraz","Oskemen","Semey","Kostanay","Kyzylorda","Oral","Petropavlovsk","Taldykorgan","Aktau","Temirtau","Ekibastuz","Kokshetau","Rudny"]
    toks = dedup_preserve_order(toks)
    return toks[:max(0, SATU_KEYWORDS_GEO_MAX)]

def build_keywords_for_offer(offer: ET.Element) -> str:
    if SATU_KEYWORDS == "off":
        return ""
    name   = get_text(offer, "name")
    vendor = get_text(offer, "vendor").strip()
    parts: List[str] = [vendor] if vendor else []
    parts += extract_model_tokens(offer) + keywords_from_name_generic(name) + color_tokens(name)
    extra: List[str] = []
    for w in parts:
        if re.search(r"[А-Яа-яЁё]", str(w) or ""):
            tr = translit_ru_to_lat(str(w))
            if tr and tr not in extra:
                extra.append(tr)
    parts += extra + geo_tokens()
    parts = [p for p in dedup_preserve_order(parts) if not AS_INTERNAL_ART_RE.match(str(p))]
    parts = parts[:SATU_KEYWORDS_MAXWORDS]
    out: List[str] = []
    total = 0
    for p in parts:
        s = str(p).strip().strip(",")
        if not s:
            continue
        add = ((", " if out else "") + s)
        if total + len(add) > SATU_KEYWORDS_MAXLEN:
            break
        out.append(s)
        total += len(add)
    return ", ".join(out)

def ensure_keywords(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        remove_all(offer, "keywords")
        kw = build_keywords_for_offer(offer)
        if kw:
            ET.SubElement(offer, "keywords").text = kw
            touched += 1
    return touched

# ======================= ФОРС-ЦЕНЫ ПО ПЕРЕКУШЕННЫМ ИСХОДНИКАМ =======================
def flag_forced_price_if_needed(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    flagged = 0
    for offer in offers_el.findall("offer"):
        src_p = parse_price_number(get_text(offer, "price"))
        if src_p is not None and src_p >= PRICE_CAP_THRESHOLD:
            offer.attrib["_force_price"] = str(PRICE_CAP_VALUE)
            flagged += 1
    return flagged

def enforce_forced_prices(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        if offer.attrib.get("_force_price"):
            _remove_all_price_nodes(offer)
            ET.SubElement(offer, "price").text = str(PRICE_CAP_VALUE)
            offer.attrib.pop("_force_price", None)
            touched += 1
    return touched

def render_feed_meta_comment(pairs: Dict[str,str]) -> str:
    rows = [
        ("Поставщик", pairs.get("supplier","")),
        ("URL поставщика", pairs.get("source","")),
        ("Время сборки (Алматы)", pairs.get("built_alm","")),
        ("Ближайшая сборка (Алматы)", pairs.get("next_build_alm","")),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total","0"))),
        ("Сколько товаров у поставщика после фильтра", str(pairs.get("offers_written","0"))),
        ("Сколько товаров есть в наличии (true)", str(pairs.get("available_true","0"))),
        ("Сколько товаров нет в наличии (false)", str(pairs.get("available_false","0"))),
    ]
    key_w = max(len(k) for k,_ in rows)
    lines = [f"  {k:<{key_w}} : {v}" for k,v in rows]
    return "<!--\n" + "\n".join(lines) + "\n-->\n"

# ======================= MAIN =======================
def main() -> None:
    log("Run set -e")
    log(f"Source: {SUPPLIER_URL}")

    # 1) загрузка исходника
    raw = load_source_bytes(SUPPLIER_URL)
    tree_in = parse_xml(raw)
    root_in = tree_in.getroot()
    shop_in = root_in.find("shop") or root_in.find("Shop")
    if shop_in is None:
        err("bad source: no <shop>")

    # метрики источника
    src_offers = shop_in.find("offers")
    src_offers_list = list(src_offers.findall("offer")) if src_offers is not None else []
    src_total = len(src_offers_list)
    log(f"Found in source: offers={src_total}, categories=on mode={ALSTYLE_CATEGORIES_MODE}")

    # 2) создаём новый корень
    out_root = ET.Element("yml_catalog")
    out_shop = ET.SubElement(out_root, "shop")
    ET.SubElement(out_shop, "name").text = SUPPLIER_NAME

    # 3) создаём пустой <offers>
    out_offers = ET.SubElement(out_shop, "offers")

    # 4) Копируем офферы 1:1 (описание НЕ трогаем — уйдет как в источнике)
    for o in src_offers_list:
        out_offers.append(deepcopy(o))

    # 5) Фильтр категорий (include/exclude по ID/названию)
    removed_count = 0
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        id2name,id2parent,parent2children = parse_categories_tree(shop_in)
        rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH)
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
            drop = (ALSTYLE_CATEGORIES_MODE == "exclude" and hit) or (ALSTYLE_CATEGORIES_MODE == "include" and not hit)
            if drop:
                out_offers.remove(off); removed_count += 1

        log(f"Category rules ({ALSTYLE_CATEGORIES_MODE}): removed={removed_count}")
    else:
        log("Category rules (off): removed=0")

    # 6) vendor cleanup + авто-дозаполнение, но НЕ трогаем description
    norm_cnt, dropped = ensure_vendor(out_shop)
    auto_fill = ensure_vendor_auto_fill(out_shop)
    log(f"Vendor autofill: {auto_fill}")

    # 7) цены
    flagged = flag_forced_price_if_needed(out_shop)
    up, sk, tot, src_stats = reprice_offers(out_shop, PRICE_RULES)
    log(f"Pricing: updated={up}, skipped={sk}, total={tot}, src={src_stats}")
    touched_enforce = enforce_forced_prices(out_shop)

    # 8) чистим параметры
    cleanup_param_blocks(out_shop)

    # 9) наличие
    t_true, t_false, _, _ = normalize_available_field(out_shop)

    # 10) Валюта
    fix_currency_id(out_shop, default_code="KZT")

    # 11) Чистка служебных тегов/атрибутов
    for off in out_offers.findall("offer"):
        purge_offer_tags_and_attrs_after(off)

    # 12) Порядок тегов + categoryId=0 первым
    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)

    # 13) Ключевые слова (НЕ трогаем description, только читаем при необходимости)
    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    # 14) Пост-очистка описаний (только текст, без HTML-оформления) — САМЫЙ КОНЕЦ
    desc_touched = post_clean_descriptions(out_shop)
    log(f"Descriptions cleaned (plain): {desc_touched}")

    # Красивые отступы (Python 3.9+)
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # FEED_META
    built_alm = now_almaty()
    meta_pairs = {
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL or "file",
        "built_alm": format_dt_almaty(built_alm),
        "next_build_alm": format_dt_almaty(next_build_time_almaty()),
        "offers_total": src_total,
        "offers_written": len(list(out_offers.findall("offer"))),
        "available_true": t_true,
        "available_false": t_false,
    }
    meta = render_feed_meta_comment(meta_pairs)

    # запись
    by = ET.tostring(out_root, encoding="utf-8")
    out = meta.encode("utf-8") + by

    if DRY_RUN:
        sys.stdout.buffer.write(out)
        return

    try:
        docs_dir = os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    with open(OUT_FILE_YML, "wb") as f:
        try:
            f.write(out.decode("utf-8").encode(ENC, errors="strict"))
        except Exception as e:
            warn(f"{ENC} can't encode some characters ({e}); writing with xmlcharrefreplace fallback")
            f.write(out.decode("utf-8").encode(ENC, errors="xmlcharrefreplace"))

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | description=AS IS")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
