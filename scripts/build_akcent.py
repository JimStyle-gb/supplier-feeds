# -*- coding: utf-8 -*-
"""
Build Akcent YML/XML (flat <offers>) for Satu — script_version=akcent-2025-09-14.3

Изменения в этой версии:
- <!--FEED_META--> красиво выровнен по колонкам.
- Чистка описаний: убираем лишние пробелы/табуляции, тройные и более переносы, хвостовые пробелы.
- Читаемость: добавляем пустую строку между <offer> элементами.
- Всё остальное как договорено (фильтр include по <name>, бренды не удаляем кроме имён поставщиков,
  vendorCode из артикула с префиксом AC, цены, чистка служебных тегов и пр.)
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

# ===================== ENV / SETTINGS =====================
SCRIPT_VERSION   = "akcent-2025-09-14.3"

SUPPLIER_NAME    = os.getenv("SUPPLIER_NAME", "akcent")
SUPPLIER_URL     = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml")

OUT_FILE_YML     = os.getenv("OUT_FILE", "docs/akcent.yml")
OUT_FILE_XML     = "docs/akcent.xml"
ENC              = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S        = int(os.getenv("TIMEOUT_S", "30"))
RETRIES          = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF    = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))

DRY_RUN          = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AC")
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "1").lower() in {"1","true","yes"}

# ===== Ключи =====
AKCENT_KEYWORDS_PATH  = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt")
AKCENT_KEYWORDS_MODE  = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()  # include|exclude
AKCENT_KEYWORDS_DEBUG = os.getenv("AKCENT_KEYWORDS_DEBUG", "0").lower() in {"1","true","yes"}
AKCENT_DEBUG_MAX_HITS = int(os.getenv("AKCENT_DEBUG_MAX_HITS", "40"))

# Чистка
DROP_CATEGORY_ID_TAG     = True
DROP_STOCK_TAGS          = True
STRIP_INTERNAL_PRICE_TAGS= True

# Какие теги убираем в самом конце (url тоже убираем)
PURGE_TAGS_AFTER = (
    "Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url"
)
# Какие атрибуты <offer> убираем в самом конце
PURGE_OFFER_ATTRS_AFTER = ("type","available","article")

# Ценовые теги, которые считаем «внутренними» и удаляем
INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice"
)

# ===================== UTILS =====================
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

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag)
    return (node.text or "").strip() if node is not None and node.text else ""

def set_text(el: ET.Element, text: str) -> None:
    el.text = text if text is not None else ""

def _norm_name(s: str) -> str:
    s = (s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

# ===================== FETCH =====================
def fetch_xml(url: str, timeout: int, retries: int, backoff: float) -> bytes:
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, retries+1):
        try:
            r = sess.get(url, headers=headers, timeout=timeout, stream=True)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            data = r.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)} bytes)")
            return data
        except Exception as e:
            last_exc = e
            sleep = backoff * attempt * (1.0 + random.uniform(-0.2, 0.2))
            warn(f"fetch attempt {attempt}/{retries} failed: {e}; sleep {sleep:.2f}s")
            if attempt < retries:
                time.sleep(sleep)
    raise RuntimeError(f"fetch failed after {retries} attempts: {last_exc}")

# ===================== KEYWORDS (name-only) =====================
class KeySpec:
    __slots__=("raw","kind","pattern")
    # kind: "substr" | "regex" | "word"
    def __init__(self, raw: str, kind: str, pattern):
        self.raw, self.kind, self.pattern = raw, kind, pattern

def load_keywords(path: str) -> List[KeySpec]:
    if not path or not os.path.exists(path):
        return []
    data = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path, "r", encoding=enc) as f:
                txt = f.read()
            txt = txt.replace("\ufeff","").replace("\x00","")
            data = txt
            break
        except Exception:
            continue
    if data is None:
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                data = f.read().replace("\x00","")
        except Exception:
            return []
    keys: List[KeySpec] = []
    for ln in data.splitlines():
        s = ln.strip()
        if not s or s.lstrip().startswith("#"):
            continue
        if len(s) >= 2 and s[0] == "/" and s[-1] == "/":
            try:
                rg = re.compile(s[1:-1], re.I)
                keys.append(KeySpec(s, "regex", rg))
                continue
            except Exception:
                continue
        if s.startswith("~="):
            w = _norm_name(s[2:])
            if not w:
                continue
            rg = re.compile(r"\b" + re.escape(w) + r"\b", re.I)
            keys.append(KeySpec(s, "word", rg))
            continue
        norm = _norm_name(s)
        if norm:
            keys.append(KeySpec(norm, "substr", None))
    return keys

def name_matches(name: str, keys: List[KeySpec]) -> Tuple[bool, Optional[str]]:
    n = _norm_name(name)
    for ks in keys:
        if ks.kind == "substr":
            if ks.raw and ks.raw in n:
                return True, ks.raw
        else:
            if ks.pattern and ks.pattern.search(name or ""):
                return True, ks.raw
    return False, None

# ===================== VENDOR =====================
def _norm_key(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower().replace("ё","е")
    s = re.sub(r"[-_/]+"," ", s)
    s = re.sub(r"\s+"," ", s)
    return s

# Блокируем ТОЛЬКО имена поставщиков; все остальные бренды сохраняем
SUPPLIER_BLOCKLIST = {_norm_key(x) for x in ["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS = ("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

def normalize_brand(raw: str) -> str:
    k = _norm_key(raw)
    if (not k) or (k in SUPPLIER_BLOCKLIST):
        return ""
    return raw.strip()

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, Dict[str,int]]:
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
                key = _norm_key(txt)
                if key:
                    dropped[key] = dropped.get(key, 0) + 1
            elif canon != txt:
                ven.text = canon
                normalized += 1
    return normalized, dropped

# ===================== PRICING =====================
PriceRule = Tuple[int, int, float, int]
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

PRICE_FIELDS = ["purchasePrice","purchase_price","wholesalePrice","wholesale_price",
                "opt_price","b2bPrice","b2b_price","price","oldprice"]

def parse_price_number(raw: str) -> Optional[float]:
    if raw is None:
        return None
    s = (raw.strip()
            .replace("\xa0"," ")
            .replace(" ", "")
            .replace("KZT","").replace("kzt","").replace("₸","")
            .replace(",", "."))
    if not s:
        return None
    try:
        val = float(s)
        return val if val > 0 else None
    except Exception:
        return None

def get_dealer_price(offer: ET.Element) -> Optional[float]:
    vals: List[float] = []
    for tag in PRICE_FIELDS:
        el = offer.find(tag)
        if el is not None and el.text:
            v = parse_price_number(el.text)
            if v is not None:
                vals.append(v)
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            v = parse_price_number(p.text or "")
            if v is not None:
                vals.append(v)
    return min(vals) if vals else None

def _force_tail_900(n: float) -> int:
    i = int(n)
    k = max(i // 1000, 0)
    out = k * 1000 + 900
    return out if out >= 900 else 900

def compute_retail(dealer: float, rules: List[PriceRule]) -> Optional[int]:
    for lo, hi, pct, add in rules:
        if lo <= dealer <= hi:
            val = dealer * (1.0 + pct/100.0) + add
            return _force_tail_900(val)
    return None

def reprice_offers(shop_el: ET.Element, rules: List[PriceRule]) -> Tuple[int,int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0,0)
    updated = skipped = total = 0
    for offer in offers_el.findall("offer"):
        total += 1
        dealer = get_dealer_price(offer)
        if dealer is None or dealer <= 100:
            skipped += 1
            node = offer.find("oldprice")
            if node is not None:
                offer.remove(node)
            continue
        new_price = compute_retail(dealer, rules)
        if new_price is None:
            skipped += 1
            node = offer.find("oldprice")
            if node is not None:
                offer.remove(node)
            continue
        p = offer.find("price") or ET.SubElement(offer, "price")
        p.text = str(int(new_price))
        cur = offer.find("currencyId") or ET.SubElement(offer, "currencyId")
        cur.text = "KZT"
        # убираем <prices> и внутренние ценовые поля
        for node in list(offer.findall("prices")) + list(offer.findall("Prices")):
            offer.remove(node)
        for tag in INTERNAL_PRICE_TAGS:
            node = offer.find(tag)
            if node is not None:
                offer.remove(node)
        updated += 1
    return updated, skipped, total

# ===================== PARAMS / DESCRIPTIONS =====================
def _key(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").strip()).lower()

EXCLUDE_NAME_RE = re.compile(
    r"(новинк|акци|скидк|уценк|снижена\s*цена|хит продаж|топ продаж|лидер продаж|лучшая цена|"
    r"рекомендуем|подарок|к[еэ]шб[еэ]к|предзаказ|статус|ед(иница)?\s*измерени|базовая единиц|"
    r"vat|ндс|налог|доставк|самовывоз|срок поставки|кредит|рассрочк|наличие\b)",
    re.I
)

def _normalize_weight_value(raw_val: str) -> str:
    s = re.sub(r"\s+"," ", (raw_val or "").strip())
    if not s:
        return s
    if re.search(r"\b(кг|kg)\b", s, re.I):
        return re.sub(r"\s*kg\b", " кг", s, flags=re.I)
    m = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*(?:г|g)\b", s, re.I)
    if m:
        val = float(m.group(1).replace(",", "."))
        if val >= 1000:
            kg = val / 1000.0
            return f"{int(kg)} кг" if abs(kg - int(kg)) < 1e-6 else f"{kg:.3g} кг"
        else:
            return re.sub(r"\bg\b", "г", f"{val:g} г", flags=re.I)
    if re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", s):
        v = float(s.replace(",", "."))
        return f"{int(v)} кг" if abs(v - int(v)) < 1e-6 else f"{v:.3g} кг"
    return s

def _looks_like_code_value(v: str) -> bool:
    s = (v or "").strip()
    if not s:
        return True
    if re.search(r"https?://", s, re.I):
        return True
    clean = re.sub(r"[0-9\-\_/ ]", "", s)
    ratio = len(clean) / max(len(s), 1)
    return ratio < 0.3

def build_specs_lines(offer: ET.Element) -> List[str]:
    lines: List[str] = []
    seen: Set[str] = set()
    WEIGHT_KEYS = {"вес","масса","weight","net weight","gross weight"}
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name = (p.attrib.get("name") or "").strip()
        raw_val  = (p.text or "").strip()
        if not raw_name or not raw_val:
            continue
        k = _key(raw_name)
        if k in {
            "артикул","штрихкод","код тн вэд","код","снижена цена","скидка","акция","уценка","новинка","хит продаж","топ продаж",
            "лидер продаж","лучшая цена","рекомендуем","подарок","кэшбэк","кешбэк","предзаказ","статус","доставка","самовывоз",
            "срок поставки","наличие","кредит","рассрочка","единица измерения","базовая единица","vat","ндс","налог","сертификат",
            "сертификация","благотворительность",
        }:
            continue
        if EXCLUDE_NAME_RE.search(raw_name):
            continue
        is_weight = k in WEIGHT_KEYS
        if is_weight:
            raw_val = _normalize_weight_value(raw_val)
        if (not is_weight) and _looks_like_code_value(raw_val):
            continue
        if k in seen:
            continue
        seen.add(k)
        lines.append(f"- {raw_name}: {raw_val}")
    return lines

def inject_specs_block(shop_el: ET.Element) -> Tuple[int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0)
    offers_touched = 0
    lines_total = 0
    spec_re = re.compile(r"\[SPECS_BEGIN\].*?\[SPECS_END\]", re.S)
    for offer in offers_el.findall("offer"):
        lines = build_specs_lines(offer)
        if not lines:
            continue
        desc_el = offer.find("description")
        curr = get_text(offer, "description")
        if curr:
            curr = spec_re.sub("", curr).strip()
        block = "Характеристики:\n" + "\n".join(lines)
        new_text = (curr + "\n\n" + block).strip() if curr else block
        if desc_el is None:
            desc_el = ET.SubElement(offer, "description")
        desc_el.text = new_text
        offers_touched += 1
        lines_total += len(lines)
    return offers_touched, lines_total

def strip_all_params(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    removed = 0
    for offer in offers_el.findall("offer"):
        for p in list(offer.findall("param")) + list(offer.findall("Param")):
            offer.remove(p)
            removed += 1
    return removed

# --- Дополнительная чистка описаний ---
_WS_RE = re.compile(r"[ \t]+")
_MULTI_NL_RE = re.compile(r"\n{3,}")
_LINE_END_WS_RE = re.compile(r"[ \t]+\n")
def _clean_description_text(s: str) -> str:
    if not s:
        return s
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # убираем хвостовые пробелы на концах строк
    s = _LINE_END_WS_RE.sub("\n", s)
    # схлопываем подряд идущие пробелы/табуляции
    s = _WS_RE.sub(" ", s)
    # схлопываем три и более переносов в два
    s = _MULTI_NL_RE.sub("\n\n", s)
    # финальный общий trim
    return s.strip()

def clean_all_descriptions(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        d = offer.find("description")
        if d is not None and d.text:
            cleaned = _clean_description_text(d.text)
            if cleaned != d.text:
                d.text = cleaned
                touched += 1
    return touched

# ===================== STOCK =====================
def normalize_stock_always_true(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        # child-тег available=true
        avail = offer.find("available") or ET.SubElement(offer, "available")
        avail.text = "true"
        touched += 1
        if DROP_STOCK_TAGS:
            for tag in ["quantity_in_stock","quantity","stock","Stock"]:
                for node in list(offer.findall(tag)):
                    offer.remove(node)
    return touched

# ===================== VENDORCODE / ARTICLE =====================
ARTICUL_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)

def _extract_article_from_name(name: str) -> str:
    if not name:
        return ""
    m = ARTICUL_RE.search(name)
    return (m.group(1) if m else "").upper()

def _extract_article_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        path = urllib.parse.urlparse(url).path.rstrip("/")
        last = path.split("/")[-1]
        last = re.sub(r"\.(html?|php|aspx?)$", "", last, flags=re.I)
        m = ARTICUL_RE.search(last)
        return (m.group(1) if m else last).upper()
    except Exception:
        return ""

def _normalize_code(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = re.sub(r"[\s_]+","", s)
    s = s.replace("—","-").replace("–","-")
    s = re.sub(r"[^A-Za-z0-9\-]+","", s)
    return s.upper()

def ensure_vendorcode_with_article(shop_el: ET.Element, prefix: str, create_if_missing: bool=False) -> Tuple[int,int,int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0,0,0)
    total_prefixed = created = filled_from_art = fixed_bare = 0
    for offer in offers_el.findall("offer"):
        vc = offer.find("vendorCode")
        if vc is None:
            if create_if_missing:
                vc = ET.SubElement(offer, "vendorCode")
                vc.text = ""
                created += 1
            else:
                continue
        old = (vc.text or "").strip()
        if (old == "") or (old.upper() == prefix.upper()):
            art = _normalize_code(offer.attrib.get("article") or "") \
               or _normalize_code(_extract_article_from_name(get_text(offer, "name"))) \
               or _normalize_code(_extract_article_from_url(get_text(offer, "url"))) \
               or _normalize_code(offer.attrib.get("id") or "")
            if art:
                vc.text = art
                filled_from_art += 1
            else:
                fixed_bare += 1
        vc.text = f"{prefix}{(vc.text or '')}"
        total_prefixed += 1
    return total_prefixed, created, filled_from_art, fixed_bare

def purge_offer_tags_and_attrs_after(offer: ET.Element) -> Tuple[int,int]:
    removed_tags = 0
    for t in PURGE_TAGS_AFTER:
        for node in list(offer.findall(t)):
            offer.remove(node)
            removed_tags += 1
    removed_attrs = 0
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib:
            offer.attrib.pop(a, None)
            removed_attrs += 1
    return removed_tags, removed_attrs

def count_category_ids(offer_el: ET.Element) -> int:
    return len(list(offer_el.findall("categoryId"))) + len(list(offer_el.findall("CategoryId")))

# ===================== FEED_META =====================
def render_feed_meta_comment(pairs: Dict[str,str]) -> str:
    # порядок ключей в мета-комментарии
    order = [
        "script_version","supplier","source","offers_total","offers_written",
        "keywords_mode","keywords_total","filtered_by_keywords",
        "prices_updated","params_removed","vendors_recovered","dropped_top",
        "available_forced","categoryId_dropped","vendorcodes_filled_from_article","vendorcodes_created",
        "built_utc","built_Asia/Almaty",
    ]
    comments = {
        "script_version": "Версия скрипта (для контроля в CI)",
        "supplier": "Метка поставщика",
        "source": "URL исходного XML",
        "offers_total": "Офферов у поставщика до очистки",
        "offers_written": "Офферов записано (после очистки)",
        "keywords_mode": "Режим фильтра (include/exclude)",
        "keywords_total": "Сколько ключей загружено",
        "filtered_by_keywords": "Сколько офферов отфильтровано по keywords",
        "prices_updated": "Скольким товарам пересчитали price",
        "params_removed": "Сколько строк параметров добавлено в описание",
        "vendors_recovered": "Скольким товарам нормализован/восстановлен vendor",
        "dropped_top": "ТОП часто отброшенных названий бренда",
        "available_forced": "Сколько офферов получили available=true",
        "categoryId_dropped": "Сколько тегов categoryId удалено",
        "vendorcodes_filled_from_article": "Скольким офферам проставили vendorCode из артикула",
        "vendorcodes_created": "Сколько узлов vendorCode было создано",
        "built_utc": "Время сборки (UTC)",
        "built_Asia/Almaty": "Время сборки (Алматы)",
    }
    # аккуратное выравнивание колонок
    max_key = max(len(k) for k in order)
    # строим строки с выравниванием ключей и равенства
    lines = ["FEED_META"]
    for k in order:
        v = str(pairs.get(k, "n/a"))
        c = comments.get(k, "")
        lines.append(f"{k.ljust(max_key)} = {v}  | {c}")
    return "\n".join(lines)

def top_dropped(d: Dict[str,int], n: int = 10) -> str:
    items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return ",".join(f"{k}:{v}" for k, v in items) if items else "n/a"

# ===================== MAIN =====================
def main() -> None:
    log(f"Source: {SUPPLIER_URL}")
    data = fetch_xml(SUPPLIER_URL, TIMEOUT_S, RETRIES, RETRY_BACKOFF)
    src_root = ET.fromstring(data)

    shop_in = src_root.find("shop") if src_root.tag.lower() != "shop" else src_root
    if shop_in is None:
        err("XML: <shop> not found")

    offers_in = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in is None:
        err("XML: <offers> not found")

    # Подсчёт categoryId (для меты)
    src_offers = list(offers_in.findall("offer"))
    catid_to_drop_total = sum(count_category_ids(o) for o in src_offers)

    # Выходной документ
    out_root = ET.Element("yml_catalog")
    out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")
    out_offers = ET.SubElement(out_shop, "offers")

    # 0) Копируем офферы, снимаем categoryId
    for o in src_offers:
        mod = deepcopy(o)
        if DROP_CATEGORY_ID_TAG:
            for node in list(mod.findall("categoryId")) + list(mod.findall("CategoryId")):
                mod.remove(node)
        out_offers.append(mod)

    # 1) Фильтр ключей (ТОЛЬКО <name>) — САМЫЙ ПЕРВЫЙ
    keys = load_keywords(AKCENT_KEYWORDS_PATH)
    if AKCENT_KEYWORDS_MODE == "include" and len(keys) == 0:
        err("AKCENT_KEYWORDS_MODE=include, но ключей не найдено/не прочитаны. Проверь docs/akcent_keywords.txt.", 2)

    use_filter = AKCENT_KEYWORDS_MODE in {"include","exclude"} and len(keys) > 0
    filtered_out = 0
    if use_filter:
        if AKCENT_KEYWORDS_DEBUG:
            log(f"[FILTER] mode={AKCENT_KEYWORDS_MODE} keys={len(keys)} name-only")
        for off in list(out_offers.findall("offer")):
            nm = get_text(off, "name")
            hit, kraw = name_matches(nm, keys)
            drop_this = (AKCENT_KEYWORDS_MODE == "exclude" and hit) or (AKCENT_KEYWORDS_MODE == "include" and not hit)
            if drop_this:
                if AKCENT_KEYWORDS_DEBUG and filtered_out < AKCENT_DEBUG_MAX_HITS:
                    log(f"[HIT] drop id={off.attrib.get('id')} name_match={kraw if hit else 'NO_MATCH'} name='{nm}'")
                out_offers.remove(off)
                filtered_out += 1

    # 2) Vendor
    norm_cnt, dropped_names = ensure_vendor(out_shop)

    # 3) VendorCode
    total_prefixed, created_nodes, filled_from_art, fixed_bare = ensure_vendorcode_with_article(
        out_shop, prefix=VENDORCODE_PREFIX, create_if_missing=VENDORCODE_CREATE_IF_MISSING
    )

    # 4) Pricing
    upd, skipped, total = reprice_offers(out_shop, PRICING_RULES)

    # 5) Specs -> Description, потом чистка описаний
    specs_offers, specs_lines = inject_specs_block(out_shop)
    removed_params = strip_all_params(out_shop)
    cleaned_desc = clean_all_descriptions(out_shop)

    # 6) Stock
    available_forced = normalize_stock_always_true(out_shop)

    # 7) Финальная чистка
    for off in out_offers.findall("offer"):
        purge_offer_tags_and_attrs_after(off)

    # Красиво отформатировать
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # Добавляем пустые строки между <offer> для читабельности
    offers_list = out_offers.findall("offer")
    for i, off in enumerate(offers_list):
        if i < len(offers_list) - 1:
            off.tail = "\n\n  "   # пустая строка между офферами + базовая индентация
        else:
            off.tail = "\n"       # последний оффер заканчиваем переводом строки

    # FEED_META
    offers_written = len(list(out_offers.findall("offer")))
    meta_pairs = {
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "offers_total": len(src_offers),
        "offers_written": offers_written,
        "keywords_mode": AKCENT_KEYWORDS_MODE if use_filter else ("off" if len(keys)==0 else AKCENT_KEYWORDS_MODE),
        "keywords_total": len(keys),
        "filtered_by_keywords": filtered_out,
        "prices_updated": upd,
        "params_removed": specs_lines,
        "vendors_recovered": norm_cnt,
        "dropped_top": top_dropped(dropped_names),
        "available_forced": available_forced,
        "categoryId_dropped": catid_to_drop_total,
        "vendorcodes_filled_from_article": filled_from_art,
        "vendorcodes_created": created_nodes,
        "built_utc": now_utc_str(),
        "built_Asia/Almaty": now_almaty_str(),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    # Запись файлов
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    ET.ElementTree(out_root).write(OUT_FILE_YML, encoding=ENC, xml_declaration=True)
    ET.ElementTree(out_root).write(OUT_FILE_XML, encoding=ENC, xml_declaration=True)

    # .nojekyll
    docs_dir = os.path.dirname(OUT_FILE_YML) or "docs"
    try:
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(
        "Wrote: %s & %s | offers=%s | encoding=%s | script=%s | desc_cleaned=%s" %
        (OUT_FILE_YML, OUT_FILE_XML, offers_written, ENC, SCRIPT_VERSION, cleaned_desc)
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
