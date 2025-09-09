# -*- coding: utf-8 -*-
"""
Генератор YML (Yandex Market Language) для al-style:
- Забирает исходный XML у поставщика (с ретраями и проверкой ответа).
- Фильтрует офферы по списку категорий из docs/categories_alstyle.txt.
- Сохраняет ПОЛНУЮ структуру offer (deepcopy: все атрибуты и вложенные теги).
- Собирает дерево <categories> только по используемым категориям + их предкам.
- Нормализует/дозаполняет <vendor>:
    • никогда не ставит названия твоих поставщиков: alstyle, copyline, vtt, akcent
    • NV Print разрешён
    • В «строгом режиме» добавляет <vendor> только если бренд в ALLOWLIST (эмуляция базы Satu)
- Форсированно добавляет префикс к <vendorCode> (по умолчанию AS, без дефиса).
- ВСЕГДА пересчитывает <price> по “дилерской” цене (минимум из ценовых полей) и правилам наценки (зашиты в коде).
- Удаляет <oldprice>.
- ВСТАВЛЯЕТ КОММЕНТАРИЙ FEED_META: supplier, source, source_date, built_utc и built_Asia/Almaty.
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
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS")  # без дефиса по умолчанию
# Создавать <vendorCode>, если он отсутствует.
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "0").lower() in {"1","true","yes"}

# ====== РЕЖИМ СТРОГОГО КОНТРОЛЯ БРЕНДОВ (эмуляция базы Satu) ======
# Если STRICT_VENDOR_ALLOWLIST=1, <vendor> сохраняем ТОЛЬКО если бренд в ALLOWLIST.
STRICT_VENDOR_ALLOWLIST = os.getenv("STRICT_VENDOR_ALLOWLIST", "1").lower() in {"1", "true", "yes"}

# Доп. бренды можно добавить без правки кода:
#   BRANDS_ALLOWLIST_EXTRA="Colorfix, SomeBrand|Another Brand"
BRANDS_ALLOWLIST_EXTRA = os.getenv("BRANDS_ALLOWLIST_EXTRA", "")


# ===================== УТИЛИТЫ =====================

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARN: {msg}", flush=True, file=sys.stderr)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", flush=True, file=sys.stderr)
    sys.exit(code)

def fetch_xml(url: str, timeout: int, retries: int, backoff: float, auth=None) -> bytes:
    """
    Надёжное скачивание XML с ретраями и проверками.
    """
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
    return time.strftime("%Y-%m-%d %H:%М:%S", time.localtime())


# ===================== РАБОТА С КАТЕГОРИЯМИ =====================

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


# ===================== ПАРСИНГ ФАЙЛА ФИЛЬТРОВ =====================

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


# ===================== НОРМАЛИЗАЦИЯ/ЗАПОЛНЕНИЕ <vendor> =====================

def _norm_key(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

# Названия поставщиков, которые НЕЛЬЗЯ использовать как бренд
SUPPLIER_BLOCKLIST = {
    _norm_key(x) for x in [
        "alstyle", "al-style",
        "copyline",
        "vtt",
        "akcent", "ak-cent",
        # NV Print НЕ в блок-листе — бренд разрешён
    ]
}

UNKNOWN_VENDOR_MARKERS = ("неизвест", "unknown", "без бренда", "no brand", "noname", "no-name", "n/a")

# OEM-бренды + NV Print (разрешён)
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
    (re.compile(r"^\s*hp\b", re.I), "HP"),
    (re.compile(r"^\s*canon\b", re.I), "Canon"),
    (re.compile(r"^\s*brother\b", re.I), "Brother"),
    (re.compile(r"^\s*kyocera\b", re.I), "Kyocera"),
    (re.compile(r"^\s*xerox\b", re.I), "Xerox"),
    (re.compile(r"^\s*ricoh\b", re.I), "Ricoh"),
    (re.compile(r"^\s*epson\b", re.I), "Epson"),
    (re.compile(r"^\s*samsung\b", re.I), "Samsung"),
    (re.compile(r"^\s*panasonic\b", re.I), "Panasonic"),
    (re.compile(r"^\s*konica\s*-?\s*minolta\b", re.I), "Konica Minolta"),
    (re.compile(r"^\s*sharp\b", re.I), "Sharp"),
    (re.compile(r"^\s*lexmark\b", re.I), "Lexmark"),
    (re.compile(r"^\s*pantum\b", re.I), "Pantum"),
    (re.compile(r"^\s*nv\s*-?\s*print\b", re.I), "NV Print"),
]

def _looks_unknown(txt: str) -> bool:
    t = (txt or "").strip().lower()
    return any(mark in t for mark in UNKNOWN_VENDOR_MARKERS)

def normalize_brand(raw: str) -> str:
    """Канонизирует бренд: OEM + NV Print, прочие — Title Case; поставщиков отбрасываем."""
    k = _norm_key(raw)
    if not k or k in SUPPLIER_BLOCKLIST:
        return ""
    if k in _BRAND_MAP:
        return _BRAND_MAP[k]
    for pat, val in _BRAND_PATTERNS:
        if pat.search(raw or ""):
            return val
    # Прочие — аккуратный Title Case (если это не поставщик)
    return " ".join(w.capitalize() for w in k.split())

def _split_extra_brands(raw: str) -> List[str]:
    if not raw:
        return []
    parts = re.split(r"[|,]+", raw)
    out = []
    for p in parts:
        p = p.strip()
        if p:
            out.append(p)
    return out

# БЕЛЫЙ СПИСОК «известных» брендов (эмуляция базы Satu) — КАНОНИЧЕСКИЕ имена:
ALLOWED_BRANDS_BASE = {
    "HP", "Canon", "Brother", "Kyocera", "Xerox", "Ricoh", "Epson", "Samsung",
    "Panasonic", "Konica Minolta", "Sharp", "Lexmark", "Pantum", "NV Print",
}

# Дополняем allowlist из переменной окружения
ALLOWED_BRANDS: Set[str] = set(ALLOWED_BRANDS_BASE)
for extra in _split_extra_brands(BRANDS_ALLOWLIST_EXTRA):
    can = normalize_brand(extra)
    if can:
        ALLOWED_BRANDS.add(can)

def brand_allowed(canon: str) -> bool:
    if not STRICT_VENDOR_ALLOWLIST:
        return True
    return canon in ALLOWED_BRANDS


def ensure_vendor(shop_el: ET.Element) -> Tuple[int, int, int, int, int]:
    """
    Нормализуем vendor:
    - удаляем «неизвестный» и названия поставщиков (alstyle/copyline/vtt/akcent);
    - заполняем из param/name/description; NV Print разрешён;
    - если STRICT_VENDOR_ALLOWLIST=1 — сохраняем только бренды из ALLOWED_BRANDS.
    Возврат: (normalized, filled_param, filled_name, dropped_supplier, dropped_not_allowed)
    """
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0, 0, 0, 0, 0)

    normalized = 0
    filled_param = 0
    filled_name = 0
    dropped_supplier = 0
    dropped_not_allowed = 0

    def _set_vendor(offer: ET.Element, value: str, src: str) -> bool:
        """Устанавливает vendor если разрешён; возвращает True если установлен."""
        canon = normalize_brand(value)
        if not canon:
            return False
        if not brand_allowed(canon):
            return False
        ven = offer.find("vendor")
        if ven is None:
            ven = ET.SubElement(offer, "vendor")
        ven.text = canon
        return True

    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        txt_raw = (ven.text or "").strip() if ven is not None and ven.text else ""

        # уже задан — проверка на «неизвестный» и поставщика
        if txt_raw:
            if _looks_unknown(txt_raw) or _norm_key(txt_raw) in SUPPLIER_BLOCKLIST:
                if ven is not None:
                    offer.remove(ven)
                if _norm_key(txt_raw) in SUPPLIER_BLOCKLIST:
                    dropped_supplier += 1
            else:
                # нормализация + allowlist
                canon = normalize_brand(txt_raw)
                if not canon or not brand_allowed(canon):
                    if ven is not None:
                        offer.remove(ven)
                    if canon:  # был нормальный бренд, но не в allowlist
                        dropped_not_allowed += 1
                else:
                    if canon != txt_raw:
                        ven.text = canon
                        normalized += 1
                    continue  # vendor принят, идём к следующему офферу

        # из param
        candidate = ""
        for p in offer.findall("param"):
            nm = (p.attrib.get("name") or "").strip().lower()
            if "бренд" in nm or "производ" in nm:
                candidate = (p.text or "").strip()
                if candidate:
                    break
        if candidate and _set_vendor(offer, candidate, "param"):
            filled_param += 1
            continue

        # из name (по паттернам) / description
        name_val = get_text(offer, "name")
        placed = False
        if name_val:
            for pat, brand in _BRAND_PATTERNS:
                if pat.search(name_val) and _set_vendor(offer, brand, "name"):
                    filled_name += 1
                    placed = True
                    break
        if not placed:
            descr = get_text(offer, "description")
            if descr:
                for pat, brand in _BRAND_PATTERNS:
                    if pat.search(descr) and _set_vendor(offer, brand, "description"):
                        filled_name += 1
                        placed = True
                        break

        if not placed and name_val:
            # аккуратная эвристика — первое слово до разделителя
            head = re.split(r"[–—\-:\(\)\[\],;|/]{1,}", name_val, maxsplit=1)[0]
            if _set_vendor(offer, head, "guess"):
                filled_name += 1
            else:
                dropped_not_allowed += 1  # был кандидат, но не прошёл allowlist

    return (normalized, filled_param, filled_name, dropped_supplier, dropped_not_allowed)


# ===================== ПОСТ-ОБРАБОТКА <vendorCode> =====================

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


# ===================== ПРАВИЛА ЦЕНООБРАЗОВАНИЯ (ЗАШИТЫ В КОДЕ) =====================

PriceRule = Tuple[int, int, float, int]  # (min_incl, max_incl, percent, add_abs)

PRICING_RULES: List[PriceRule] = [
    (   101,    10000, 3.0,  3000),
    ( 10001,    25000, 3.0,  4000),
    ( 25001,    50000, 3.0,  5000),
    ( 50001,    75000, 3.0,  7000),
    ( 75001,   100000, 3.0, 10000),
    (100001,   150000, 3.0, 12000),
    (150001,   200000, 3.0, 15000),
    (200001,   300000, 3.0, 20000),
    (300001,   400000, 3.0, 25000),
    (400001,   500000, 3.0, 30000),
    (500001,   750000, 3.0, 40000),
    (750001,  1000000, 3.0, 50000),
    (1000001, 1500000, 3.0, 70000),
    (1500001, 2000000, 3.0, 90000),
    (2000001,100000000,3.0,100000),
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

def compute_retail(dealer: float, rules: List[PriceRule]) -> Optional[int]:
    """
    Находит диапазон (включительно) и считает: dealer * (1 + pct/100) + add.
    Округляет до целых KZT.
    """
    for lo, hi, pct, add in rules:
        if lo <= dealer <= hi:
            val = dealer * (1.0 + pct / 100.0) + add
            return int(round(val))
    return None  # если не попали ни в один диапазон


# ===================== ОБРАБОТКА ЦЕН В ОФФЕРАХ =====================

def reprice_offers(shop_el: ET.Element, rules: List[PriceRule]) -> Tuple[int, int, int]:
    """
    Пересчитывает <price> у каждого оффера по правилам.
    Удаляет <oldprice>.
    Возвращает (updated, skipped_low_or_missing, total)
    """
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


# ===================== ОСНОВНАЯ ЛОГИКА =====================

def main() -> None:
    auth = (BASIC_USER, BASIC_PASS) if BASIC_USER and BASIC_PASS else None

    log(f"Source: {SUPPLIER_URL}")
    log(f"Categories file: {CATEGORIES_FILE}")
    data = fetch_xml(SUPPLIER_URL, TIMEOUT_S, RETRIES, RETRY_BACKOFF, auth=auth)
    root = parse_xml_bytes(data)

    # Дата из исходного фида
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

    # Читаем фильтры
    ids_filter, subs, regs = parse_selectors(CATEGORIES_FILE)
    have_selectors = bool(ids_filter or subs or regs)

    # Определяем сохраняемые категории
    matched_cat_ids = {cid for cid, nm in id2name.items() if cat_matches(nm, cid, ids_filter, subs, regs)}
    keep_cat_ids = collect_descendants(matched_cat_ids, parent2children) if matched_cat_ids else set()

    # Фильтрация офферов (fail-closed при наличии селекторов)
    offers_in = list(iter_local(offers_el, "offer"))
    if have_selectors:
        used_offers = [o for o in offers_in if get_text(o, "categoryId") in keep_cat_ids]
        if not used_offers:
            warn("фильтры заданы, но офферов не найдено — проверь docs/categories_alstyle.txt")
    else:
        used_offers = offers_in

    # Фактические категории по найденным офферам + их предки
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

    # Категории — только используемые
    out_cats = ET.SubElement(out_shop, "categories")

    def depth(cid: str) -> int:
        d = 0
        cur = cid
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

    # Пост-обработка: производитель (c allowlist-контролем)
    norm_cnt, fill_param_cnt, fill_name_cnt, drop_sup, drop_na = ensure_vendor(out_shop)

    # Пост-обработка: префикс к <vendorCode>
    total_prefixed, created_nodes = force_prefix_vendorcode(
        out_shop,
        prefix=VENDORCODE_PREFIX,
        create_if_missing=VENDORCODE_CREATE_IF_MISSING,
    )

    # === ЦЕНООБРАЗОВАНИЕ (зашитые правила) ===
    upd, skipped, total = reprice_offers(out_shop, PRICING_RULES)

    # Красивый вывод
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    ET.ElementTree(out_root).write(OUT_FILE, encoding=ENC, xml_declaration=True)

    # Логи
    log(f"Selectors: ids={len(ids_filter)}, subs={len(subs)}, regs={len(regs)} (present={have_selectors})")
    log(f"Vendor fixed: normalized={norm_cnt}, filled_from_param={fill_param_cnt}, filled_from_name={fill_name_cnt}, dropped_supplier={drop_sup}, dropped_not_allowed={drop_na}, strict_allowlist={STRICT_VENDOR_ALLOWLIST}")
    log(f"Vendor allowlist size: base={len(ALLOWED_BRANDS_BASE)} (+ extra={len(ALLOWED_BRANDS)-len(ALLOWED_BRANDS_BASE)})")
    log(f"VendorCode: total_prefixed={total_prefixed}, created_nodes={created_nodes}, prefix='{VENDORCODE_PREFIX}', create_if_missing={VENDORCODE_CREATE_IF_MISSING}")
    log(f"Pricing: updated={upd}, skipped_low_or_missing={skipped}, total_offers={total}")
    log(f"Source date: {source_date or 'n/a'}")
    log(f"Wrote: {OUT_FILE} | offers={len(used_offers)} | cats={len(used_cat_ids)} | encoding={ENC}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
