# -*- coding: utf-8 -*-
"""
Универсальный генератор YML (шаблон на базе al-style).

Новое:
- В блок «Характеристики» вес теперь попадает всегда: сначала нормализуем единицы
  (добавляем «кг», при необходимости конвертируем граммы → килограммы), и только
  потом применяем анти-спам фильтры. Вес больше не «теряется».
- FEED_META многострочный (ключ = значение  | пояснение) — удобно читать глазами.

Остальное: цены 4% + надбавка + хвост …900, allowlist брендов (запрет имён поставщиков,
NV Print разрешён), префикс vendorCode, перенос характеристик в описание (почти всё,
кроме «мусора»), чистка <param> и служебных цен, защита от пустых выборок, DRY_RUN и т. д.
"""

from __future__ import annotations

import os, sys, re, time, random
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
SUPPLIER_NAME   = os.getenv("SUPPLIER_NAME", "alstyle")  # для FEED_META
SUPPLIER_URL    = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php")
OUT_FILE        = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC             = os.getenv("OUTPUT_ENCODING", "windows-1251")
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "docs/categories_alstyle.txt")

BASIC_USER      = os.getenv("BASIC_USER") or None
BASIC_PASS      = os.getenv("BASIC_PASS") or None

TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "30"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF   = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))

SKIP_WRITE_IF_EMPTY = os.getenv("SKIP_WRITE_IF_EMPTY", "1").lower() in {"1","true","yes"}
DRY_RUN              = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# Префикс для <vendorCode>
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS")
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "0").lower() in {"1","true","yes"}

# Вендоры
STRICT_VENDOR_ALLOWLIST = os.getenv("STRICT_VENDOR_ALLOWLIST", "1").lower() in {"1","true","yes"}
BRANDS_ALLOWLIST_EXTRA = os.getenv("BRANDS_ALLOWLIST_EXTRA", "")

# Чистка служебных цен
STRIP_INTERNAL_PRICE_TAGS = os.getenv("STRIP_INTERNAL_PRICE_TAGS", "1").lower() in {"1","true","yes"}
INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice",
)

# === Характеристики → описание (политика: почти всё, кроме чёрного списка) ===
EMBED_SPECS_IN_DESCRIPTION = os.getenv("EMBED_SPECS_IN_DESCRIPTION", "1").lower() in {"1","true","yes"}
SPECS_BEGIN_MARK = "[SPECS_BEGIN]"
SPECS_END_MARK   = "[SPECS_END]"
SPECS_EXCLUDE_EMPTY = True

# НЕ включать в блок описания (ключи по имени param, регистронезависимо)
SPECS_BLOCK_EXCLUDE_KEYS: Set[str] = {
    # жёстко по твоему требованию
    "артикул", "штрихкод", "код тн вэд", "код",
    # любые штрих/ID коды и ссылки
    "barcode", "ean", "upc", "jan", "qr", "sku",
    "код товара", "код производителя",
    "ссылка", "url", "link",
    # маркетинговые ярлыки / служебное
    "новинка", "снижена цена", "скидка", "акция", "распродажа",
    "топ продаж", "хит продаж", "лидер продаж", "лучшая цена",
    "рекомендуем", "подарок", "кэшбэк", "кешбэк", "кешбек",
    "предзаказ", "статус", "статус товара",
    "благотворительность",
    "базовая единица", "единица измерения", "ед. изм.",
    "ндс", "ставка ндс", "vat", "налог", "tax",
}

# Параметры, которые удаляем из оффера до сборки описания (жёстко)
UNWANTED_PARAM_KEYS = {"артикул","штрихкод","код тн вэд","код"}

# После встраивания «Характеристик» чистим ВСЕ <param>, кроме явно разрешённых
STRIP_ALL_PARAMS_AFTER_EMBED = os.getenv("STRIP_ALL_PARAMS_AFTER_EMBED", "1").lower() in {"1","true","yes"}
ALLOWED_PARAM_NAMES_RAW = os.getenv("ALLOWED_PARAM_NAMES", "")  # "Цвет|Материал" или "Цвет,Материал"

# Нормализация остатков
NORMALIZE_STOCK = os.getenv("NORMALIZE_STOCK", "1").lower() in {"1","true","yes"}

# Проверка картинок (первые N офферов)
PICTURE_HEAD_SAMPLE = int(os.getenv("PICTURE_HEAD_SAMPLE", "0"))

# ===================== УТИЛИТЫ =====================
def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARN: {msg}", flush=True, file=sys.stderr)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", flush=True, file=sys.stderr)
    sys.exit(code)

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def now_almaty_str() -> str:
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def read_prev_http_meta(path: str) -> Tuple[Optional[str], Optional[str]]:
    """Извлекает http_last_modified / etag из прошлого файла (работает и с многострочным комментарием)."""
    try:
        with open(path, "r", encoding=ENC, errors="ignore") as f:
            head = f.read(4096)
        m1 = re.search(r"http_last_modified=([^\s>]+)", head)
        m2 = re.search(r'etag="([^"]+)"', head)
        return (m1.group(1) if m1 else None, m2.group(1) if m2 else None)
    except Exception:
        return (None, None)

def fetch_xml(url: str, timeout: int, retries: int, backoff: float, auth=None,
              if_modified_since: Optional[str]=None, etag: Optional[str]=None) -> Tuple[Optional[bytes], dict, int]:
    """GET с ретраями, джиттером и условными заголовками (If-Modified-Since / If-None-Match)."""
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    if if_modified_since:
        headers["If-Modified-Since"] = if_modified_since
    if etag:
        headers["If-None-Match"] = etag

    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = sess.get(url, headers=headers, timeout=timeout, auth=auth, stream=True)
            if resp.status_code == 304:
                return (None, resp.headers, 304)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            data = resp.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)} bytes)")
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if not any(t in ctype for t in ("xml","text/plain","application/octet-stream")):
                head = data[:64].lstrip()
                if not head.startswith(b"<"):
                    raise RuntimeError(f"unexpected content-type: {ctype!r}")
            return (data, resp.headers, resp.status_code)
        except Exception as e:
            last_exc = e
            jitter = backoff * attempt * max(0.5, 1.0 + random.uniform(-0.2, 0.2))
            warn(f"fetch attempt {attempt}/{retries} failed: {e}; sleep {jitter:.2f}s")
            if attempt < retries:
                time.sleep(jitter)
    raise RuntimeError(f"fetch failed after {retries} attempts: {last_exc}")

def parse_xml_bytes(data: bytes) -> ET.Element:
    return ET.fromstring(data)

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag)
    return (node.text or "").strip() if node is not None else ""

def set_text(el: ET.Element, text: str) -> None:
    el.text = text if text is not None else ""

def iter_local(elem: ET.Element, name: str):
    for child in elem.findall(name):
        yield child

# ===================== КАТЕГОРИИ =====================
def build_category_graph(cats_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,Set[str]]]:
    id2name: Dict[str, str] = {}
    id2parent: Dict[str, str] = {}
    parent2children: Dict[str, Set[str]] = {}
    for c in iter_local(cats_el, "category"):
        cid = (c.attrib.get("id") or "").strip()
        pid = (c.attrib.get("parentId") or "").strip()
        name = (c.text or "").strip()
        if not cid:
            continue
        id2name[cid] = name
        if pid:
            id2parent[cid] = pid
            parent2children.setdefault(pid, set()).add(cid)
        else:
            id2parent.setdefault(cid, "")
    return id2name, id2parent, parent2children

def collect_descendants(start_ids: Set[str], parent2children: Dict[str,Set[str]]) -> Set[str]:
    out: Set[str] = set()
    stack = list(start_ids)
    while stack:
        x = stack.pop()
        if x in out:
            continue
        out.add(x)
        for ch in parent2children.get(x, ()):
            stack.append(ch)
    return out

def collect_ancestors(ids: Set[str], id2parent: Dict[str,str]) -> Set[str]:
    out: Set[str] = set()
    for cid in ids:
        cur = cid
        while True:
            pid = id2parent.get(cur, "")
            if not pid:
                break
            out.add(pid)
            cur = pid
    return out

# ===================== ФИЛЬТР КАТЕГОРИЙ =====================
def parse_selectors(path: str) -> Tuple[Set[str], List[str], List[re.Pattern]]:
    ids_filter: Set[str] = set()
    substrings: List[str] = []
    regexps: List[re.Pattern] = []
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("re:"):
                    pat = line[3:].strip()
                    if pat:
                        try:
                            regexps.append(re.compile(pat, re.I))
                        except re.error as e:
                            warn(f"bad regex in {path!r}: {pat!r} ({e})")
                    continue
                if line.isdigit() or ":" not in line:
                    ids_filter.add(line)
                else:
                    substrings.append(line.lower())
    except FileNotFoundError:
        warn(f"{path} not found — фильтр категорий НЕ будет применён")
    return ids_filter, substrings, regexps

def cat_matches(name: str, cid: str, ids_filter: Set[str], subs: List[str], regs: List[re.Pattern]) -> bool:
    if cid in ids_filter:
        return True
    lname = (name or "").lower()
    for s in subs:
        if s and s in lname:
            return True
    for r in regs:
        try:
            if r.search(name or ""):
                return True
        except Exception:
            continue
    return False

# ===================== БРЕНДЫ =====================
def _norm_key(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

# Блокируем названия поставщиков (исключение: NV Print — разрешить во всех видах написания)
SUPPLIER_BLOCKLIST = {_norm_key(x) for x in ["alstyle","al-style","copyline","vtt","akcent","ak-cent","nvprint","nv print"]}
SUPPLIER_BLOCKLIST -= {"nv print", "nvprint"}

UNKNOWN_VENDOR_MARKERS = ("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

_BRAND_MAP = {
    "hp":"HP","hewlett packard":"HP","hewlett packard inc":"HP","hp inc":"HP",
    "canon":"Canon","canon inc":"Canon","brother":"Brother","kyocera":"Kyocera","kyocera mita":"Kyocera",
    "xerox":"Xerox","ricoh":"Ricoh","epson":"Epson","samsung":"Samsung","panasonic":"Panasonic",
    "konica minolta":"Konica Minolta","konica":"Konica Minolta","sharp":"Sharp","lexmark":"Lexmark",
    "pantum":"Pantum","nv print":"NV Print","nvprint":"NV Print","nv  print":"NV Print",
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
]
ALLOWED_BRANDS_BASE = {
    "HP","Canon","Brother","Kyocera","Xerox","Ricoh","Epson","Samsung",
    "Panasonic","Konica Minolta","Sharp","Lexmark","Pantum","NV Print",
}
def _split_extra_brands(raw: str) -> List[str]:
    if not raw: return []
    return [p.strip() for p in re.split(r"[|,]+", raw) if p.strip()]
ALLOWED_BRANDS: Set[str] = set(ALLOWED_BRANDS_BASE)
for extra in _split_extra_brands(BRANDS_ALLOWLIST_EXTRA):
    key = _norm_key(extra)
    can = _BRAND_MAP.get(key) or " ".join(w.capitalize() for w in key.split())
    if can: ALLOWED_BRANDS.add(can)

HEAD_STOPWORDS = {"картридж","картриджи","чернила","тонер","порошок","бумага","фотобумага","пленка","плівка",
                  "сумка","пакет","папка","ручка","кабель","переходник","адаптер","лента","лентa","скотч",
                  "матова","глянцевая","глянцева","матовая","для","совместим","совместимый","универсальный",}

def _looks_unknown(txt: str) -> bool:
    t = (txt or "").strip().lower()
    return any(mark in t for mark in UNKNOWN_VENDOR_MARKERS)

def normalize_brand(raw: str) -> str:
    k = _norm_key(raw)
    if (not k) or (k in SUPPLIER_BLOCKLIST): return ""
    if k in _BRAND_MAP: return _BRAND_MAP[k]
    for rg, val in _BRAND_PATTERNS:
        if rg.search(raw or ""): return val
    return " ".join(w.capitalize() for w in k.split())

def brand_allowed(canon: str) -> bool:
    return True if not STRICT_VENDOR_ALLOWLIST else (canon in ALLOWED_BRANDS)

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
    for allowed in ALLOWED_BRANDS:
        if re.search(rf"\b{re.escape(allowed)}\b", text, re.I): return allowed
    return ""

def extract_brand_from_name(name: str) -> str:
    if not name: return ""
    for rg, val in _BRAND_PATTERNS:
        if rg.search(name) and brand_allowed(val): return val
    for rg in NAME_BRAND_PATTERNS:
        m = rg.search(name)
        if m:
            head = m.group(1).strip()
            if _norm_key(head) in HEAD_STOPWORDS: return ""
            cand = normalize_brand(head)
            if cand and brand_allowed(cand): return cand
    head = re.split(r"[–—\-:\(\)\[\],;|/]{1,}", name, maxsplit=1)[0].strip()
    if head and _norm_key(head) not in HEAD_STOPWORDS:
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
    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        txt_raw = (ven.text or "").strip() if ven is not None and ven.text else ""
        def drop_name(nm: str):
            if not nm: return
            key = _norm_key(nm)
            dropped_names[key] = dropped_names.get(key, 0) + 1
        def clear_vendor(reason: str):
            nonlocal dropped_supplier, dropped_not_allowed
            if ven is not None:
                drop_name(ven.text or "")
                offer.remove(ven)
            if reason=="supplier": dropped_supplier+=1
            elif reason=="not_allowed": dropped_not_allowed+=1
        if txt_raw:
            if _looks_unknown(txt_raw) or _norm_key(txt_raw) in (SUPPLIER_BLOCKLIST):
                clear_vendor("supplier"); ven=None; txt_raw=""
            else:
                canon = normalize_brand(txt_raw)
                if (not canon) or (not brand_allowed(canon)):
                    clear_vendor("not_allowed"); ven=None; txt_raw=""
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
def force_prefix_vendorcode(shop_el: ET.Element, prefix: str, create_if_missing: bool=False) -> Tuple[int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0)
    total=created=0
    for offer in offers_el.findall("offer"):
        vc = offer.find("vendorCode")
        if vc is None:
            if create_if_missing:
                vc = ET.SubElement(offer, "vendorCode"); created+=1; old=""
            else:
                continue
        else:
            old = vc.text or ""
        vc.text = f"{prefix}{old}"; total+=1
    return total,created

# ===================== ЦЕНООБРАЗОВАНИЕ (4% + …900) =====================
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

# ===================== ЧИСТКА ПАРАМОВ/ЦЕН =====================
def _key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def strip_unwanted_params(shop_el: ET.Element) -> Tuple[int,int]:
    """Удаляет <param> по UNWANTED_PARAM_KEYS и тег <barcode>."""
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
    """Удаляет все <param>, кроме перечисленных в allowed_names (по имени)."""
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
def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _parse_dims(val: str) -> str:
    s = _norm(val).replace("х", "x").replace("Х", "x").replace("*", "x")
    parts = re.split(r"[x×X]", s)
    nums = []
    for p in parts:
        p = re.sub(r"[^\d.,]", "", p).replace(",", ".")
        try:
            n = float(p); nums.append(int(n) if abs(n-int(n))<1e-6 else n)
        except Exception:
            pass
    return "x".join(str(n) for n in nums if n != "")

def _normalize_weight_value(raw_val: str) -> str:
    """
    Нормализует вес:
    - "18" -> "18 кг" (если нет единиц, считаем кг);
    - "18000 г" -> "18 кг" (если >= 1000 г → переводим в кг);
    - "18kg" -> "18 кг", "18.5 kg" -> "18.5 кг";
    - оставляет как есть, если уже есть 'кг'/'g' и конвертация не требуется.
    """
    s = _norm(raw_val)
    if not s:
        return s
    # если явные кг
    if re.search(r"\b(кг|kg)\b", s, re.I):
        s = re.sub(r"\s*kg\b", " кг", s, flags=re.I)
        return s
    # если граммы
    m = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*(?:г|g)\b", s, re.I)
    if m:
        val = float(m.group(1).replace(",", "."))
        if val >= 1000:
            kg = val / 1000.0
            if abs(kg - int(kg)) < 1e-6:
                return f"{int(kg)} кг"
            return f"{kg:.3g} кг"
        else:
            # маленькие веса — оставим в граммах
            return re.sub(r"\bg\b", "г", f"{val:g} г", flags=re.I)
    # чистое число — считаем кг
    if re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", s):
        s = s.replace(",", ".")
        if abs(float(s) - int(float(s))) < 1e-6:
            return f"{int(float(s))} кг"
        return f"{float(s):.3g} кг"
    return s

def _format_weight(val: str) -> str:
    return _normalize_weight_value(val)

# отбрасываем значения, похожие на коды/URL/чистые ID (неинформативно)
def _looks_like_code_value(v: str) -> bool:
    s = (v or "").strip()
    if not s: return True
    if re.search(r"https?://", s, re.I): return True
    clean = re.sub(r"[0-9\-\_/ ]", "", s)
    ratio = len(clean) / max(len(s), 1)
    return ratio < 0.3  # мало букв => скорее код

# опциональный «маркетинговый» паттерн на имя параметра
EXCLUDE_NAME_RE = re.compile(
    r"(новинк|акци|скидк|распродаж|хит продаж|топ продаж|лидер продаж|лучшая цена|"
    r"рекомендуем|подарок|к[еэ]шб[еэ]к|предзаказ|статус|ед(иница)?\s*измерени|базовая единиц|"
    r"vat|ндс|налог|tax)",
    re.I
)

def build_specs_lines(offer: ET.Element) -> List[str]:
    """
    Собираем почти все параметры в блок «Характеристики», исключая:
    - ключи из SPECS_BLOCK_EXCLUDE_KEYS,
    - имена, совпавшие с EXCLUDE_NAME_RE (маркетинг/служебное),
    - пустые значения,
    - значения, похожие на коды/URL (НО вес пропускаем мимо этого фильтра).
    Нормализуем Габариты и Вес (единицы, конвертация грамм→кг).
    """
    lines: List[str] = []
    seen_keys: Set[str] = set()

    WEIGHT_KEYS = {"вес", "масса", "weight", "net weight", "gross weight"}

    for p in offer.findall("param"):
        raw_name = (p.attrib.get("name") or "").strip()
        raw_val  = (p.text or "").strip()
        if not raw_name or not raw_val:
            continue

        k = _key(raw_name)

        # Базовые исключения по имени
        if k in SPECS_BLOCK_EXCLUDE_KEYS:
            continue
        if EXCLUDE_NAME_RE.search(raw_name):
            continue

        # Нормализация значений
        is_weight = k in WEIGHT_KEYS
        if k.startswith("габариты"):
            raw_val = _parse_dims(raw_val) or raw_val
        elif is_weight:
            raw_val = _format_weight(raw_val)

        # Фильтр «похоже на код/URL» — ДЛЯ ВЕСА НЕ ПРИМЕНЯЕМ
        if not is_weight and _looks_like_code_value(raw_val):
            continue

        # Дедуп по имени
        if k in seen_keys:
            continue
        seen_keys.add(k)

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
    """Делает available=true/false и приводит quantity_in_stock к числу, если возможно."""
    if not NORMALIZE_STOCK: return (0,0)
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0)
    touched=with_qty=0
    for offer in offers_el.findall("offer"):
        qty_txt = get_text(offer, "quantity_in_stock") or get_text(offer, "quantity")
        qty_num = None
        if qty_txt:
            m = re.search(r"\d+", qty_txt.replace(">", ""))
            if m:
                qty_num = int(m.group(0))
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
    """HEAD проверка первых N картинок (диагностика)."""
    if n <= 0: return 0
    offers_el = shop_el.find("offers")
    if offers_el is None: return 0
    sess = requests.Session()
    bad = 0; checked = 0
    for offer in offers_el.findall("offer"):
        if checked >= n: break
        for pic in offer.findall("picture"):
            url = (pic.text or "").strip()
            if not url: continue
            try:
                r = sess.head(url, timeout=5, allow_redirects=True)
                if r.status_code >= 400:
                    bad += 1
                checked += 1
                break
            except Exception:
                bad += 1; checked += 1
                break
    return bad

# ===================== РЕНДЕР FEED_META =====================
def render_feed_meta_comment(pairs: Dict[str, str]) -> str:
    """Многострочный комментарий с колонками: ключ = значение  | пояснение."""
    order = [
        "supplier","source","source_date","http_last_modified","etag","not_modified",
        "offers_total","offers_written","prices_updated","params_removed",
        "vendors_recovered","vendors_dropped","dropped_top",
        "bad_pictures_sample","stock_normalized","built_utc","built_Asia/Almaty",
    ]
    comments = {
        "supplier": "Метка поставщика",
        "source": "URL исходного XML",
        "source_date": "Дата/время из фида поставщика",
        "http_last_modified": "Заголовок Last-Modified от сервера",
        "etag": "Заголовок ETag от сервера",
        "not_modified": "1=HTTP 304 (не изменился), 0=есть изменения",
        "offers_total": "Офферов у поставщика до фильтра",
        "offers_written": "Офферов записано в итоговый YML",
        "prices_updated": "Скольким товарам пересчитали price",
        "params_removed": "Сколько <param> удалено",
        "vendors_recovered": "Скольким товарам восстановлен vendor",
        "vendors_dropped": "Скольким товарам vendor отброшен",
        "dropped_top": "ТОП часто отброшенных названий",
        "bad_pictures_sample": "Ошибок при выборочной проверке картинок",
        "stock_normalized": "Скольким товарам нормализован остаток/наличие",
        "built_utc": "Время сборки (UTC)",
        "built_Asia/Almaty": "Время сборки (Алматы)",
    }
    maxk = max(len(k) for k in order)
    maxv = 0
    for k in order:
        v = str(pairs.get(k, "n/a"))
        if len(v) > maxv: maxv = len(v)
    lines = ["FEED_META"]
    for k in order:
        v = str(pairs.get(k, "n/a"))
        c = comments.get(k, "")
        lines.append(f"{k.ljust(maxk)} = {v.ljust(maxv)}  | {c}")
    return "\n".join(lines)

# ===================== ОСНОВНАЯ ЛОГИКА =====================
def main() -> None:
    prev_lm, prev_etag = read_prev_http_meta(OUT_FILE)
    auth = (BASIC_USER, BASIC_PASS) if BASIC_USER and BASIC_PASS else None

    log(f"Source: {SUPPLIER_URL}")
    log(f"Categories file: {CATEGORIES_FILE}")
    data, resp_headers, status = fetch_xml(
        SUPPLIER_URL, TIMEOUT_S, RETRIES, RETRY_BACKOFF, auth=auth,
        if_modified_since=prev_lm, etag=prev_etag
    )
    http_last_modified = resp_headers.get("Last-Modified") or ""
    http_etag = resp_headers.get("ETag") or ""
    not_modified = (status == 304)

    if not_modified:
        log("HTTP 304 Not Modified — исходный фид не изменился.")

    root = parse_xml_bytes(data) if data is not None else None
    if root is None and not_modified:
        try:
            with open(OUT_FILE, "rb") as f:
                _ = f.read()
        except Exception:
            err("304 получен, но предыдущий OUT_FILE не найден — нечего публиковать.")
        log("304: Публикацию пропускаем, т.к. данных нет и файл не пересобираем.")
        return

    source_date = root.attrib.get("date") or ""
    if not source_date:
        source_date = (root.findtext("shop/generation-date") or root.findtext("shop/date") or "")

    shop = root.find("shop")
    if shop is None: err("XML: <shop> not found")
    cats_el = shop.find("categories"); offers_el = shop.find("offers")
    if cats_el is None or offers_el is None: err("XML: <categories> or <offers> not found")

    id2name, id2parent, parent2children = build_category_graph(cats_el)

    ids_filter, subs, regs = parse_selectors(CATEGORIES_FILE)
    have_selectors = bool(ids_filter or subs or regs)

    offers_in = list(iter_local(offers_el, "offer"))
    if have_selectors:
        keep_cat_ids = {cid for cid, nm in id2name.items() if cat_matches(nm, cid, ids_filter, subs, regs)}
        keep_cat_ids = collect_descendants(keep_cat_ids, parent2children) if keep_cat_ids else set()
        used_offers = [o for o in offers_in if get_text(o, "categoryId") in keep_cat_ids]
        if not used_offers:
            warn("фильтры заданы, но офферов не найдено — проверь файл категорий")
    else:
        used_offers = offers_in

    def key_offer(o: ET.Element) -> Tuple[str,str,str]:
        return (get_text(o,"categoryId"), get_text(o,"vendorCode"), get_text(o,"name"))
    used_offers = sorted(used_offers, key=key_offer)

    used_cat_ids = {get_text(o, "categoryId") for o in used_offers if get_text(o, "categoryId")}
    used_cat_ids |= collect_ancestors(used_cat_ids, id2parent)

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

    # Префикс vendorCode
    total_prefixed, created_nodes = force_prefix_vendorcode(out_shop, prefix=VENDORCODE_PREFIX, create_if_missing=VENDORCODE_CREATE_IF_MISSING)

    # Пересчёт цен
    upd, skipped, total = reprice_offers(out_shop, PRICING_RULES)

    # Чистка внутренних цен
    removed_internal = strip_internal_prices(out_shop, INTERNAL_PRICE_TAGS) if STRIP_INTERNAL_PRICE_TAGS else 0

    # Удаление нежелательных параметров (и <barcode>) до сборки описания
    removed_params_unwanted, removed_barcode = strip_unwanted_params(out_shop)

    # Встраивание характеристик в описание (почти всё, кроме чёрного списка/маркетинга)
    specs_offers = specs_lines = 0
    if EMBED_SPECS_IN_DESCRIPTION:
        specs_offers, specs_lines = inject_specs_block(out_shop)

    # Полная чистка param после встраивания
    removed_params_total = 0
    if STRIP_ALL_PARAMS_AFTER_EMBED:
        allowed = _parse_allowed_names(ALLOWED_PARAM_NAMES_RAW)
        removed_params_total = strip_all_params_except(out_shop, allowed)

    # Нормализация остатков
    stock_touched, stock_with_qty = normalize_stock(out_shop)

    # Проверка картинок (первые N)
    bad_pics = sample_check_pictures(out_shop, PICTURE_HEAD_SAMPLE) if PICTURE_HEAD_SAMPLE > 0 else 0

    try: ET.indent(out_root, space="  ")
    except Exception: pass

    if SKIP_WRITE_IF_EMPTY and len(used_offers) == 0:
        log("offers=0 -> запись файла пропущена (SKIP_WRITE_IF_EMPTY=1)")
        return

    def top_dropped(d: Dict[str,int], n: int=10) -> str:
        items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
        return ",".join(f"{k}:{v}" for k,v in items) if items else "n/a"

    meta_pairs = {
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "source_date": source_date or "n/a",
        "http_last_modified": http_last_modified or "n/a",
        "etag": f'"{http_etag or ""}"',
        "not_modified": 1 if not_modified else 0,
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
    meta_block = render_feed_meta_comment(meta_pairs)
    out_root.insert(0, ET.Comment(meta_block))

    if DRY_RUN:
        log("[DRY_RUN=1] Файл НЕ записан. Все расчёты выполнены.")
        return

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    ET.ElementTree(out_root).write(OUT_FILE, encoding=ENC, xml_declaration=True)

    log(f"Vendor stats: normalized={norm_cnt}, filled_param={fill_param_cnt}, filled_text={fill_text_cnt}, recovered={recovered}, dropped_supplier={drop_sup}, dropped_not_allowed={drop_na}")
    log(f"VendorCode: prefixed={total_prefixed}, created_nodes={created_nodes}, prefix='{VENDORCODE_PREFIX}'")
    log(f"Pricing: updated={upd}, skipped_low_or_missing={skipped}, total_offers={total}")
    log(f"Stripped internal price tags: enabled={STRIP_INTERNAL_PRICE_TAGS}, removed_nodes={removed_internal}")
    log(f"Removed params: unwanted={removed_params_unwanted}, barcode_tags={removed_barcode}, total_after_embed={removed_params_total}")
    log(f"Specs block (filtered): offers={specs_offers}, lines_total={specs_lines}")
    log(f"Stock normalized: touched={stock_touched}, with_qty={stock_with_qty}")
    log(f"Pictures sample checked={PICTURE_HEAD_SAMPLE}, bad={bad_pics}")
    log(f"Wrote: {OUT_FILE} | offers={len(used_offers)} | cats={len(used_cat_ids)} | encoding={ENC}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
