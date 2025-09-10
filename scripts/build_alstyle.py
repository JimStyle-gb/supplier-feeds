# -*- coding: utf-8 -*-
"""
Генератор YML (Yandex Market Language) для al-style:
- Скачивает исходный XML.
- Фильтрует офферы по списку категорий из docs/categories_alstyle.txt.
- Копирует офферы (deepcopy) и сохраняет только используемые категории + их предков.
- <vendor>:
    • НИКОГДА не ставит имена поставщиков (alstyle/copyline/vtt/akcent), NV Print разрешён
    • Сверяет бренд с "базой Satu" (allowlist OEM+NV Print)
    • Если текущий vendor не из базы — ищет допустимый бренд в карточке:
        1) param "бренд/производитель"
        2) любые param (напр. "Для принтеров Xerox ...")
        3) name (шаблоны/префиксы)
        4) description (маркеры)
      Нашёл разрешённый — подставляет; иначе vendor очищается.
- <vendorCode>: форс-префикс (по умолчанию AS без дефиса), опционально создаёт, если отсутствует.
- Цены:
    • ВСЕГДА пересчитывает <price> от минимальной дилерской цены по зашитым правилам
    • Процент наценки: 4.0% (было 3.0%)
    • <oldprice> удаляет
    • Итоговую цену ОКРУГЛЯЕТ ВНИЗ до вида …900 (психологическое ценообразование),
      напр. 104423 → 103900; 219980 → 219900.
- (по умолчанию) вычищает служебные ценовые теги (<purchase_price> и др.) из публичного YML:
    STRIP_INTERNAL_PRICE_TAGS=1 (можно выключить =0)
- Добавляет комментарий FEED_META (supplier/source/source_date/built_*).
"""

from __future__ import annotations

import os, sys, re, time
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
SUPPLIER_NAME   = os.getenv("SUPPLIER_NAME", "alstyle")  # только для FEED_META
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

# Префикс для <vendorCode> (всегда добавляется, даже если уже есть похожий).
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS")  # без дефиса
# Создавать <vendorCode>, если он отсутствует.
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "0").lower() in {"1","true","yes"}

# ====== База "Satu" (allowlist) и режим ======
# По умолчанию ВКЛЮЧЕНО: сохраняем <vendor> только если бренд в allowlist (OEM + NV Print)
STRICT_VENDOR_ALLOWLIST = os.getenv("STRICT_VENDOR_ALLOWLIST", "1").lower() in {"1","true","yes"}
BRANDS_ALLOWLIST_EXTRA = os.getenv("BRANDS_ALLOWLIST_EXTRA", "")

# ====== Стриппинг внутренних цен (по умолчанию включён) ======
STRIP_INTERNAL_PRICE_TAGS = os.getenv("STRIP_INTERNAL_PRICE_TAGS", "1").lower() in {"1","true","yes"}
INTERNAL_PRICE_TAGS = (
    "purchase_price", "purchasePrice",
    "wholesale_price", "wholesalePrice",
    "opt_price", "optPrice",
    "b2b_price", "b2bPrice",
    "supplier_price", "supplierPrice",
    "min_price", "minPrice",
    "max_price", "maxPrice",
    # <oldprice> удаляем отдельно в логике ценообразования
)


# ===================== УТИЛИТЫ =====================

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARN: {msg}", flush=True, file=sys.stderr)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", flush=True, file=sys.stderr)
    sys.exit(code)

def fetch_xml(url: str, timeout: int, retries: int, backoff: float, auth=None) -> bytes:
    sess = requests.Session()
    headers = {"User-Agent": "alstyle-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = sess.get(url, headers=headers, timeout=timeout, auth=auth, stream=True)
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            data = resp.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)} bytes)")
            if not any(t in ctype for t in ("xml", "text/plain", "application/octet-stream")):
                head = data[:64].lstrip()
                if not head.startswith(b"<"):
                    raise RuntimeError(f"unexpected content-type: {ctype!r}")
            return data
        except Exception as e:
            last_exc = e
            warn(f"fetch attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"fetch failed after {retries} attempts: {last_exc}")

def parse_xml_bytes(data: bytes) -> ET.Element:
    return ET.fromstring(data)

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag)
    return (node.text or "").strip() if node is not None else ""

def iter_local(elem: ET.Element, name: str):
    for child in elem.findall(name):
        yield child

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def now_almaty_str() -> str:
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


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


# ===================== БРЕНДЫ / ПРОИЗВОДИТЕЛЬ =====================

def _norm_key(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

SUPPLIER_BLOCKLIST = {_norm_key(x) for x in ["alstyle","al-style","copyline","vtt","akcent","ak-cent"]}
UNKNOWN_VENDOR_MARKERS = ("неизвест", "unknown", "без бренда", "no brand", "noname", "no-name", "n/a")

_BRAND_MAP = {
    "hp": "HP", "hewlett packard": "HP", "hewlett packard inc": "HP", "hp inc": "HP",
    "canon": "Canon", "canon inc": "Canon",
    "brother": "Brother",
    "kyocera": "Kyocera", "kyocera mita": "Kyocera",
    "xerox": "Xerox",
    "ricoh": "Ricoh",
    "epson": "Epson",
    "samsung": "Samsung",
    "panasonic": "Panasonic",
    "konica minolta": "Konica Minolta", "konica": "Konica Minolta",
    "sharp": "Sharp",
    "lexmark": "Lexmark",
    "pantum": "Pantum",
    "nv print": "NV Print", "nvprint": "NV Print", "nv  print": "NV Print",
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
    "HP", "Canon", "Brother", "Kyocera", "Xerox", "Ricoh", "Epson", "Samsung",
    "Panasonic", "Konica Minolta", "Sharp", "Lexmark", "Pantum", "NV Print",
}
def _split_extra_brands(raw: str) -> List[str]:
    if not raw: return []
    return [p.strip() for p in re.split(r"[|,]+", raw) if p.strip()]
ALLOWED_BRANDS: Set[str] = set(ALLOWED_BRANDS_BASE)
for extra in _split_extra_brands(BRANDS_ALLOWLIST_EXTRA):
    key = _norm_key(extra)
    can = _BRAND_MAP.get(key) or " ".join(w.capitalize() for w in key.split())
    if can: ALLOWED_BRANDS.add(can)

HEAD_STOPWORDS = {
    "картридж","картриджи","чернила","тонер","порошок","бумага","фотобумага","пленка","плівка",
    "сумка","пакет","папка","ручка","кабель","переходник","адаптер","лента","лентa","скотч",
    "матова","глянцевая","глянцева","матовая","для","совместим","совместимый","универсальный",
}

def _looks_unknown(txt: str) -> bool:
    t = (txt or "").strip().lower()
    return any(mark in t for mark in UNKNOWN_VENDOR_MARKERS)

def normalize_brand(raw: str) -> str:
    k = _norm_key(raw)
    if (not k) or (k in SUPPLIER_BLOCKLIST):
        return ""
    if k in _BRAND_MAP:
        return _BRAND_MAP[k]
    for rg, val in _BRAND_PATTERNS:
        if rg.search(raw or ""):
            return val
    return " ".join(w.capitalize() for w in k.split())

def brand_allowed(canon: str) -> bool:
    return True if not STRICT_VENDOR_ALLOWLIST else (canon in ALLOWED_BRANDS)

DESC_BRAND_PATTERNS = [
    re.compile(r"(?:^|\b)(?:производитель|бренд)\s*[:\-–]\s*([^\n\r;,|]+)", re.I),
    re.compile(r"(?:^|\b)(?:manufacturer|brand)\s*[:\-–]\s*([^\n\r;,|]+)", re.I),
]
NAME_BRAND_PATTERNS = [
    re.compile(r"^\s*\[([^\]]{2,30})\]\s+", re.U),
    re.compile(r"^\s*\(([^\)]{2,30})\)\s+", re.U),
    re.compile(r"^\s*([A-Za-zА-ЯЁЇІЄҐ][A-Za-z0-9А-ЯЁЇІЄҐ\-\.\s]{1,20})\s+[-–—]\s+", re.U),
]

def scan_text_for_allowed_brand(text: str) -> str:
    if not text: return ""
    for rg, val in _BRAND_PATTERNS:
        if rg.search(text):
            if brand_allowed(val): 
                return val
    for rg in DESC_BRAND_PATTERNS:
        m = rg.search(text)
        if m:
            cand = normalize_brand(m.group(1))
            if cand and brand_allowed(cand):
                return cand
    for allowed in ALLOWED_BRANDS:
        pat = re.compile(rf"\b{re.escape(allowed)}\b", re.I)
        if pat.search(text):
            return allowed
    return ""

def extract_brand_from_name(name: str) -> str:
    if not name: return ""
    for rg, val in _BRAND_PATTERNS:
        if rg.search(name):
            if brand_allowed(val): 
                return val
    for rg in NAME_BRAND_PATTERNS:
        m = rg.search(name)
        if m:
            head = m.group(1).strip()
            if _norm_key(head) in HEAD_STOPWORDS:
                continue
            cand = normalize_brand(head)
            if cand and brand_allowed(cand):
                return cand
    head = re.split(r"[–—\-:\(\)\[\],;|/]{1,}", name, maxsplit=1)[0].strip()
    if head and _norm_key(head) not in HEAD_STOPWORDS:
        cand = normalize_brand(head)
        if cand and brand_allowed(cand):
            return cand
    return ""

def extract_brand_from_params(offer: ET.Element) -> str:
    params = offer.findall("param")
    for p in params:
        nm = (p.attrib.get("name") or "").strip().lower()
        if "бренд" in nm or "производ" in nm or "manufacturer" in nm or "brand" in nm:
            cand = normalize_brand((p.text or "").strip())
            if cand and brand_allowed(cand):
                return cand
    for p in params:
        txt = (p.text or "").strip()
        cand = scan_text_for_allowed_brand(txt)
        if cand:
            return cand
    return ""

def extract_brand_any(offer: ET.Element) -> str:
    cand = extract_brand_from_params(offer)
    if cand: return cand
    cand = extract_brand_from_name(get_text(offer, "name"))
    if cand: return cand
    return scan_text_for_allowed_brand(get_text(offer, "description"))

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, int, int, int, int, int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0, 0, 0, 0, 0, 0)

    normalized = 0
    filled_param = 0
    filled_text = 0
    dropped_supplier = 0
    dropped_not_allowed = 0
    recovered = 0

    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        txt_raw = (ven.text or "").strip() if ven is not None and ven.text else ""

        def clear_vendor(reason: str):
            nonlocal dropped_supplier, dropped_not_allowed
            if ven is not None:
                offer.remove(ven)
            if reason == "supplier":
                dropped_supplier += 1
            elif reason == "not_allowed":
                dropped_not_allowed += 1

        # 0) проверка существующего vendor
        if txt_raw:
            if _looks_unknown(txt_raw) or _norm_key(txt_raw) in SUPPLIER_BLOCKLIST:
                clear_vendor("supplier"); ven = None; txt_raw = ""
            else:
                canon = normalize_brand(txt_raw)
                if (not canon) or (not brand_allowed(canon)):
                    clear_vendor("not_allowed"); ven = None; txt_raw = ""
                else:
                    if canon != txt_raw:
                        ven.text = canon
                        normalized += 1
                    continue

        # 1) param=бренд
        params = offer.findall("param")
        param_brand = ""
        for p in params:
            nm = (p.attrib.get("name") or "").strip().lower()
            if "бренд" in nm or "производ" in nm or "manufacturer" in nm or "brand" in nm:
                param_brand = (p.text or "").strip()
                break
        if param_brand:
            cand = normalize_brand(param_brand)
            if cand and brand_allowed(cand):
                ven_new = ET.SubElement(offer, "vendor")
                ven_new.text = cand
                filled_param += 1
                recovered += 1
                continue

        # 2) любые param → name → description
        cand2 = extract_brand_any(offer)
        if cand2:
            ven_new = ET.SubElement(offer, "vendor")
            ven_new.text = cand2
            filled_text += 1
            recovered += 1
            continue

    return (normalized, filled_param, filled_text, dropped_supplier, dropped_not_allowed, recovered)


# ===================== VENDORCODE =====================

def force_prefix_vendorcode(shop_el: ET.Element, prefix: str, create_if_missing: bool = False) -> Tuple[int, int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0, 0
    total = 0
    created = 0
    for offer in offers_el.findall("offer"):
        vc = offer.find("vendorCode")
        if vc is None:
            if create_if_missing:
                vc = ET.SubElement(offer, "vendorCode")
                created += 1
                old = ""
            else:
                continue
        else:
            old = vc.text or ""
        vc.text = f"{prefix}{old}"
        total += 1
    return total, created


# ===================== ЦЕНООБРАЗОВАНИЕ (4%) =====================

PriceRule = Tuple[int, int, float, int]  # (min_incl, max_incl, percent, add_abs)

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
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None
    s = s.replace("\xa0", " ").replace(" ", "")
    s = s.replace("KZT", "").replace("kzt", "").replace("₸", "")
    s = s.replace(",", ".")
    try:
        val = float(s)
        return val if val > 0 else None
    except Exception:
        return None

PRICE_FIELDS = [
    "purchasePrice", "purchase_price",
    "wholesalePrice", "wholesale_price", "opt_price",
    "b2bPrice", "b2b_price",
    "price", "oldprice",
]

def get_dealer_price(offer: ET.Element) -> Optional[float]:
    vals: List[float] = []
    for tag in PRICE_FIELDS:
        el = offer.find(tag)
        if el is not None and el.text:
            v = parse_price_number(el.text)
            if v is not None:
                vals.append(v)
    if not vals:
        return None
    return min(vals)

def _align_price_psych_900(n: float) -> int:
    """
    Психологическое округление ВНИЗ до цены, оканчивающейся на …900.
    Примеры: 104423 → 103900; 219980 → 219900; 100100 → 99_900.
    """
    base = (int(n) // 1000) * 1000 + 900
    if base > n:
        base -= 1000
    return max(base, 900)  # на всякий случай нижний предел

def compute_retail(dealer: float, rules: List[PriceRule]) -> Optional[int]:
    """
    Находит диапазон (включительно) и считает: dealer * (1 + pct/100) + add,
    затем ОКРУГЛЯЕТ ВНИЗ до формата …900.
    """
    for lo, hi, pct, add in rules:
        if lo <= dealer <= hi:
            val = dealer * (1.0 + pct / 100.0) + add
            return _align_price_psych_900(val)
    return None

def reprice_offers(shop_el: ET.Element, rules: List[PriceRule]) -> Tuple[int, int, int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0, 0, 0)
    updated = 0
    skipped = 0
    total = 0
    for offer in offers_el.findall("offer"):
        total += 1
        dealer = get_dealer_price(offer)
        if dealer is None or dealer <= 100:
            skipped += 1
            oldp = offer.find("oldprice")
            if oldp is not None:
                offer.remove(oldp)
            continue
        new_price = compute_retail(dealer, rules)
        if new_price is None:
            skipped += 1
            oldp = offer.find("oldprice")
            if oldp is not None:
                offer.remove(oldp)
            continue
        p = offer.find("price")
        if p is None:
            p = ET.SubElement(offer, "price")
        p.text = str(int(new_price))
        oldp = offer.find("oldprice")
        if oldp is not None:
            offer.remove(oldp)
        updated += 1
    return updated, skipped, total


# ===================== СТРИП СЛУЖЕБНЫХ ЦЕН =====================

def strip_internal_prices(shop_el: ET.Element, tags: tuple) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    removed = 0
    for offer in offers_el.findall("offer"):
        for tag in tags:
            node = offer.find(tag)
            if node is not None:
                offer.remove(node)
                removed += 1
    return removed


# ===================== ОСНОВНАЯ ЛОГИКА =====================

def main() -> None:
    auth = (BASIC_USER, BASIC_PASS) if BASIC_USER and BASIC_PASS else None

    log(f"Source: {SUPPLIER_URL}")
    log(f"Categories file: {CATEGORIES_FILE}")
    data = fetch_xml(SUPPLIER_URL, TIMEOUT_S, RETRIES, RETRY_BACKOFF, auth=auth)
    root = parse_xml_bytes(data)

    # Дата исходного фида
    source_date = root.attrib.get("date") or ""
    if not source_date:
        source_date = (root.findtext("shop/generation-date") or
                       root.findtext("shop/date") or "")

    shop = root.find("shop")
    if shop is None:
        err("XML: <shop> not found")

    cats_el = shop.find("categories")
    offers_el = shop.find("offers")
    if cats_el is None or offers_el is None:
        err("XML: <categories> or <offers> not found")

    id2name, id2parent, parent2children = build_category_graph(cats_el)

    # Фильтр категорий
    ids_filter, subs, regs = parse_selectors(CATEGORIES_FILE)
    have_selectors = bool(ids_filter or subs or regs)

    offers_in = list(iter_local(offers_el, "offer"))
    if have_selectors:
        keep_cat_ids = {cid for cid, nm in id2name.items() if cat_matches(nm, cid, ids_filter, subs, regs)}
        keep_cat_ids = collect_descendants(keep_cat_ids, parent2children) if keep_cat_ids else set()
        used_offers = [o for o in offers_in if get_text(o, "categoryId") in keep_cat_ids]
        if not used_offers:
            warn("фильтры заданы, но офферов не найдено — проверь docs/categories_alstyle.txt")
    else:
        used_offers = offers_in

    used_cat_ids = {get_text(o, "categoryId") for o in used_offers if get_text(o, "categoryId")}
    used_cat_ids |= collect_ancestors(used_cat_ids, id2parent)

    # Сборка выходного XML
    out_root = ET.Element("yml_catalog")
    out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")

    # FEED_META
    meta = (
        f"FEED_META supplier={SUPPLIER_NAME} "
        f"source={SUPPLIER_URL} "
        f"source_date={source_date or 'n/a'} "
        f"built_utc={now_utc_str()} "
        f"built_Asia/Almaty={now_almaty_str()} "
    )
    out_root.insert(0, ET.Comment(meta))

    # Категории
    out_cats = ET.SubElement(out_shop, "categories")
    def depth(cid: str) -> int:
        d, cur = 0, cid
        while id2parent.get(cur):
            d += 1
            cur = id2parent[cur]
        return d
    for cid in sorted(used_cat_ids, key=lambda c: (depth(c), id2name.get(c, ""), c)):
        if cid not in id2name:
            continue
        attrs = {"id": cid}
        pid = id2parent.get(cid, "")
        if pid and pid in used_cat_ids:
            attrs["parentId"] = pid
        c_el = ET.SubElement(out_cats, "category", attrs)
        c_el.text = id2name.get(cid, "")

    # Офферы — глубокая копия исходных узлов
    out_offers = ET.SubElement(out_shop, "offers")
    for o in used_offers:
        out_offers.append(deepcopy(o))

    # Производитель (строгая сверка + восстановление из карточки)
    norm_cnt, fill_param_cnt, fill_text_cnt, drop_sup, drop_na, recovered = ensure_vendor(out_shop)

    # Префикс к vendorCode
    total_prefixed, created_nodes = force_prefix_vendorcode(
        out_shop, prefix=VENDORCODE_PREFIX, create_if_missing=VENDORCODE_CREATE_IF_MISSING
    )

    # Ценообразование (4% + …900)
    upd, skipped, total = reprice_offers(out_shop, PRICING_RULES)

    # Стрип внутренних цен (если включено)
    removed_internal = 0
    if STRIP_INTERNAL_PRICE_TAGS:
        removed_internal = strip_internal_prices(out_shop, INTERNAL_PRICE_TAGS)

    # Красивый вывод
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    ET.ElementTree(out_root).write(OUT_FILE, encoding=ENC, xml_declaration=True)

    # Логи
    log(f"Vendor allowlist: strict={STRICT_VENDOR_ALLOWLIST} | base={len(ALLOWED_BRANDS_BASE)} | total={len(ALLOWED_BRANDS)}")
    log(f"Vendor stats: normalized={norm_cnt}, filled_param={fill_param_cnt}, filled_text={fill_text_cnt}, recovered={recovered}, dropped_supplier={drop_sup}, dropped_not_allowed={drop_na}")
    log(f"VendorCode: prefixed={total_prefixed}, created_nodes={created_nodes}, prefix='{VENDORCODE_PREFIX}'")
    log(f"Pricing (4% + …900): updated={upd}, skipped_low_or_missing={skipped}, total_offers={total}")
    log(f"Stripped internal price tags: enabled={STRIP_INTERNAL_PRICE_TAGS}, removed_nodes={removed_internal}")
    log(f"Wrote: {OUT_FILE} | offers={len(used_offers)} | cats={len(used_cat_ids)} | encoding={ENC}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
