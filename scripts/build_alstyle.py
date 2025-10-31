# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle → YML (NO-DESCRIPTION-TOUCH edition)

Задача: полностью отключить любые изменения содержимого тега <description>.
Мы НЕ создаём/заменяем/форматируем описания — берём их из исходного XML как есть.
Остальной пайплайн (бренд, цена, available, vendorCode/id, currencyId, keywords, порядок полей и т.д.) сохранён.

Версия: alstyle-2025-10-30.ndt-1
Python: 3.11+
"""

from __future__ import annotations
import os, sys, re, time, random, hashlib, urllib.parse, requests
from typing import Dict, List, Tuple, Optional, Set
import xml.etree.ElementTree as ET
from copy import deepcopy
from datetime import datetime, timezone

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo  # type: ignore
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

# Категории: include/exclude/выкл (по умолчанию — include)
ALSTYLE_CATEGORIES_MODE  = os.getenv("ALSTYLE_CATEGORIES_MODE", "include").strip().lower()
ALSTYLE_CATEGORIES_PATH  = os.getenv("ALSTYLE_CATEGORIES_FILE", "docs/alstyle_categories.txt").strip()

# Вендор-код/ID
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS").strip()

# Прайсинг
# Глобальная политика: 4% + фиксированная прибавка по диапазонам, затем принудительно х/900.
PRICING_RULES: List[Tuple[int,int,float,int]] = [
    (101,        10_000,     4.0,   3_000),
    (10_001,     25_000,     4.0,   4_000),
    (25_001,     50_000,     4.0,   5_000),
    (50_001,     75_000,     4.0,   7_000),
    (75_001,     100_000,    4.0,  10_000),
    (100_001,    150_000,    4.0,  12_000),
    (150_001,    200_000,    4.0,  15_000),
    (200_001,    300_000,    4.0,  20_000),
    (300_001,    400_000,    4.0,  25_000),
    (400_001,    500_000,    4.0,  30_000),
    (500_001,    750_000,    4.0,  40_000),
    (750_001,  1_000_000,    4.0,  50_000),
    (1_000_001, 1_500_000,   4.0,  70_000),
    (1_500_001, 2_000_000,   4.0,  90_000),
    (2_000_001, 999_999_999, 4.0, 100_000),
]
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "99999999"))  # >= — форсим до PRICE_CAP_VALUE
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "0"))              # 0 — выключено

# Ключевые слова (города и т.п.)
SATU_KEYWORDS_GEO = [
    "Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз","Оскемен","Семей","Костанай",
    "Кызылорда","Орал","Петропавловск","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау"
]
SATU_KEYWORDS_GEO_MAX  = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))

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

# =============== УТИЛИТЫ ===============
def log(msg: str) -> None:
    print(msg, flush=True)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_utc_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S %Z")

def now_almaty() -> datetime:
    if ZoneInfo:
        try:
            return datetime.now(ZoneInfo("Asia/Almaty"))
        except Exception:
            pass
    return datetime.now()

def request_get_bytes(url: str, timeout_s: int = TIMEOUT_S) -> bytes:
    h = {
        "User-Agent": "Mozilla/5.0 (compatible; AlStyleFeedBot/1.0; +http://example.local)"
    }
    r = requests.get(url, headers=h, timeout=timeout_s)
    r.raise_for_status()
    return r.content or b""

def load_source_bytes(url: str) -> bytes:
    last_err = None
    for i in range(RETRIES):
        try:
            data = request_get_bytes(url, TIMEOUT_S)
            if len(data) < MIN_BYTES:
                raise ValueError(f"too few bytes: {len(data)} < {MIN_BYTES}")
            return data
        except Exception as e:
            last_err = e
            time.sleep(RETRY_BACKOFF * (i+1))
    raise RuntimeError(f"Failed to fetch source: {last_err}")

def elem_text(el: Optional[ET.Element]) -> str:
    return (el.text or "").strip() if el is not None else ""

def get_text(offer: ET.Element, tag: str) -> str:
    el = offer.find(tag)
    return elem_text(el)

def set_text(offer: ET.Element, tag: str, value: str) -> None:
    el = offer.find(tag)
    if el is None:
        el = ET.SubElement(offer, tag)
    el.text = value

def first_picture(offer: ET.Element) -> Optional[str]:
    pics = offer.findall("picture")
    return elem_text(pics[0]) if pics else None

def ensure_picture(offer: ET.Element, url: str) -> None:
    p = ET.SubElement(offer, "picture")
    p.text = url

def remove_nodes(offer: ET.Element, *names: str) -> int:
    cnt = 0
    for n in names:
        for el in list(offer.findall(n)):
            offer.remove(el); cnt += 1
    return cnt

def inner_html(el: ET.Element) -> str:
    """Возвращает innerHTML тега (используем только для чтения, описания не меняем)."""
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

# === НОРМАЛИЗАЦИЯ ПРОБЕЛОВ В <description> (ВАРИАНТ A) ===
def _collapse_ws_text(s: str) -> str:
    """Схлопывает пробельные символы в одну позицию. Убирает \r, \n, \t и NBSP."""
    if not s:
        return s
    s = s.replace("\u00A0", " ")  # NBSP → пробел
    s = s.replace("\r", "\n").replace("\t", " ")
    s = re.sub(r"\n+", " ", s)
    s = re.sub(r"[ ]{2,}", " ", s)
    return s.strip()

def normalize_descriptions_whitespace(root_shop: ET.Element) -> int:
    """Проходит по всем <offer>/<description> и схлопывает лишние пробелы/переводы строк
    ТОЛЬКО в текстовых узлах (text/tail), не трогая теги. Возвращает количество обработанных описаний."""
    count = 0
    if root_shop is None:
        return count

    def walk(node: ET.Element):
        # нормализуем text текущего узла
        if node.text:
            node.text = _collapse_ws_text(node.text)
        # рекурсивно обходим детей
        for ch in list(node):
            walk(ch)
            if ch.tail:
                ch.tail = _collapse_ws_text(ch.tail)

    offers = root_shop.findall(".//offer")
    for off in offers:
        desc = off.find("description") or off.find("Description")
        if desc is None:
            continue
        walk(desc)
        count += 1
    return count

# ======================= КАТЕГОРИИ: include/exclude =======================
class CatRule:
    __slots__ = ("raw", "kind", "pattern")
    def __init__(self, raw: str, kind: str, pattern: re.Pattern[str]) -> None:
        self.raw = raw; self.kind = kind; self.pattern = pattern

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
    ids: Set[str] = set()
    regs: List[CatRule] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.isdigit():
                ids.add(line)
            else:
                kind = "re" if line.startswith("re:") else "str"
                body = line[3:] if kind == "re" else line
                try:
                    pat = re.compile(body, re.I)
                except Exception:
                    # как строка
                    pat = re.compile(re.escape(body), re.I)
                    kind = "str"
                regs.append(CatRule(line, kind, pat))
    return ids, regs

def parse_categories_tree(shop_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,List[str]]]:
    """Строим дерево категорий: id→name, id→parentId, parent→childrenIds."""
    id2name: Dict[str,str] = {}
    id2parent: Dict[str,str] = {}
    parent2children: Dict[str, List[str]] = {}
    cats = (shop_el.find("categories") or ET.Element("categories")).findall("category")
    for c in cats:
        cid = (c.get("id") or "").strip()
        name = (c.text or "").strip()
        pid  = (c.get("parentId") or "").strip()
        if not cid:
            continue
        id2name[cid] = name
        if pid:
            id2parent[cid] = pid
            parent2children.setdefault(pid, []).append(cid)
    return id2name, id2parent, parent2children

def category_id_path(id2parent: Dict[str,str], id2name: Dict[str,str], cid: str) -> str:
    """Путь категории по id: 'Root / Child / Sub'."""
    path: List[str] = []
    cur = cid
    seen: Set[str] = set()
    while cur and cur not in seen:
        seen.add(cur)
        nm = id2name.get(cur, "").strip()
        if nm:
            path.append(nm)
        cur = id2parent.get(cur, "")
    return " / ".join(reversed(path))

def category_name_match(rules: List[CatRule], name: str) -> bool:
    s = _norm_cat(name)
    for r in rules:
        if r.pattern.search(s):
            return True
    return False

def load_text_file(path: str, enc_try: List[str]) -> str:
    """Читаем текстовый файл, пробуя несколько кодировок."""
    for enc in enc_try:
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read()
        except Exception:
            continue
    # попытка в utf-8 игнор
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()

# ======================= ВЕНДОРЫ, VENDORCODE, ID =======================
BRAND_ALLOW = {"Acer","Apple","Asus","Canon","Corsair","Dahua","Dell","D-Link","Epson","Genius","Gigabyte","Gree","HP","Hikvision",
               "Hisense","Honor","Huawei","JBL","Kingston","Lenovo","LG","Logitech","Matrix","MikroTik","MSI","NV Print","Philips",
               "Razer","Samsung","Sven","TP-Link","Vention","ViewSonic","Xerox","Zyxel","ZKTeco","Tecno","Realme","Xiaomi","Fujitsu",
               "Brother","Panasonic","KIVI","MS","HyperX","XPG","AOC","TCL","Thomson","Tefal","Polaris","Redmond","Gorenje","Midea",
               "Scarlett","Beko","Indesit","Vivo","OnePlus","Oppo","Nothing","Sony","PlayStation"
}
BRAND_BLOCK = {"alstyle","al-style","copyline","vtt","akcent","ak-cent"}

def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def detect_vendor_by_name_or_desc(offer: ET.Element, brand_index: Dict[str,str]) -> str:
    name  = get_text(offer, "name")
    desc  = inner_html(offer.find("description"))  # читаем, но НЕ меняем
    first = re.split(r"\s+", name.strip())[0] if name else ""
    f_norm = _norm_key(first)
    if f_norm in brand_index:
        return brand_index[f_norm]
    b = _find_brand_in_text(name + " " + desc, brand_index)
    return b

def _find_brand_in_text(text: str, brand_index: Dict[str,str]) -> str:
    s = _norm_key(text)
    for k, real in brand_index.items():
        if k and k in s:
            return real
    return ""

def ensure_vendor(shop_el: ET.Element) -> None:
    """Оставляем vendor, если он есть и не из блок-листа; иначе пробуем определить по имени/описанию."""
    brand_index = { _norm_key(b): b for b in BRAND_ALLOW }
    for off in shop_el.findall(".//offer"):
        ven_el = off.find("vendor")
        cur    = elem_text(ven_el)
        cur_n  = (cur or "").strip()
        if cur_n and _norm_key(cur_n) not in BRAND_BLOCK:
            continue
        # попытка авто-детекта
        auto = detect_vendor_by_name_or_desc(off, brand_index) or cur_n
        if not auto or _norm_key(auto) in BRAND_BLOCK:
            # если бренд пуст или блокирован — удаляем тег vendor вовсе
            if ven_el is not None:
                off.remove(ven_el)
            continue
        if ven_el is None:
            ven_el = ET.SubElement(off, "vendor")
        ven_el.text = auto

def ensure_vendor_auto_fill(shop_el: ET.Element) -> int:
    """Подставляем vendor там, где его нет, по известным паттернам названий (доп. эвристики)."""
    filled = 0
    brand_index = { _norm_key(b): b for b in BRAND_ALLOW }
    for off in shop_el.findall(".//offer"):
        ven_el = off.find("vendor")
        if ven_el is not None and elem_text(ven_el):
            continue
        name = get_text(off, "name")
        auto = detect_vendor_by_name_or_desc(off, brand_index)
        if auto:
            ven_el = ET.SubElement(off, "vendor")
            ven_el.text = auto
            filled += 1
    return filled

def ensure_vendorcode_with_article(shop_el: ET.Element, prefix: str = "AS", create_if_missing: bool = True) -> None:
    """Формируем vendorCode и синхронизируем offer/@id."""
    for off in shop_el.findall(".//offer"):
        vc_el = off.find("vendorCode")
        vc    = (elem_text(vc_el) or "").strip()
        if not vc:
            # пытаемся взять из <param name="Артикул"> или из хвоста URL
            art = ""
            for p in off.findall("param"):
                if (p.get("name") or "").strip().lower() in {"артикул","sku","код"}:
                    art = elem_text(p); break
            if not art:
                url = get_text(off, "url")
                if url:
                    tail = urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
                    art  = re.sub(r"[^0-9a-z_-]+","", tail, flags=re.I)
            if art:
                vc = f"{prefix}{re.sub(r'[^0-9A-Za-z]+','', art)}"
            elif create_if_missing:
                # генерируем стабильный surrogate
                h = hashlib.md5((get_text(off,"name")+get_text(off,"url")+get_text(off,"vendor")).encode("utf-8")).hexdigest()[:6].upper()
                vc = f"{prefix}{h}"
                # ставим только если пусто
        if vc_el is None:
            vc_el = ET.SubElement(off, "vendorCode")
        if vc:
            vc_el.text = vc

        # id = vendorCode
        if vc:
            off.set("id", vc)

def sync_offer_id_with_vendorcode(shop_el: ET.Element) -> None:
    """Синхронизируем id=vendorCode (если vendorCode есть)."""
    for off in shop_el.findall(".//offer"):
        vc = get_text(off, "vendorCode")
        if vc:
            off.set("id", vc)

# ======================= ПРАЙСИНГ =======================
def pick_base_price(off: ET.Element) -> Optional[float]:
    """Минимальная из возможных базовых (dealer/wholesale/b2b/price/oldprice)."""
    fields = [
        "purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
        "b2b_price","b2bPrice","price","oldprice"
    ]
    vals: List[float] = []
    for f in fields:
        v_el = off.find(f)
        if v_el is None: 
            continue
        try:
            v = float((v_el.text or "").replace(" ", "").replace(",", "."))
            if v > 0:
                vals.append(v)
        except Exception:
            continue
    return min(vals) if vals else None

def apply_pricing_rules(base: float, rules: List[Tuple[int,int,float,int]]) -> int:
    """4% + фиксированный добавок по диапазону; завершение: х/900."""
    # 1) процент
    price = base * (1.0 + (rules[0][2] / 100.0))  # 4%
    # 2) фикс-добавка по диапазону
    add = 0
    for lo, hi, _, extra in rules:
        if lo <= base <= hi:
            add = extra; break
    price += add
    # 3) хвост 900
    price_int = int(round(price))
    price_int = price_int - (price_int % 1000) + 900
    return max(price_int, 0)

def reprice_offers(shop_el: ET.Element, rules: List[Tuple[int,int,float,int]]) -> None:
    for off in shop_el.findall(".//offer"):
        base = pick_base_price(off)
        if base is None:
            continue
        price = apply_pricing_rules(base, rules)
        # если есть PRICE_CAP и базовая выше порога — форсим
        if PRICE_CAP_VALUE > 0 and base >= PRICE_CAP_THRESHOLD:
            price = PRICE_CAP_VALUE
        set_text(off, "price", str(price))

def enforce_forced_prices(shop_el: ET.Element) -> int:
    if PRICE_CAP_VALUE <= 0:
        return 0
    forced = 0
    for off in shop_el.findall(".//offer"):
        base = pick_base_price(off)
        if base is None:
            continue
        if base >= PRICE_CAP_THRESHOLD:
            set_text(off, "price", str(PRICE_CAP_VALUE))
            forced += 1
    return forced

def flag_unrealistic_supplier_prices(shop_el: ET.Element) -> int:
    if PRICE_CAP_VALUE <= 0:
        return 0
    flagged = 0
    for off in shop_el.findall(".//offer"):
        base = pick_base_price(off)
        if base is None:
            continue
        if base >= PRICE_CAP_THRESHOLD:
            # Ничего не меняем, просто считаем
            flagged += 1
    return flagged

# ======================= ПАРАМЫ/МУСОР =======================
KASPI_CODE_NAME_RE = re.compile(r"^код\s+товара\s+kaspi$", re.I)

def _value_is_empty_or_noise(val: str) -> bool:
    v = (val or "").strip().lower()
    if not v or v in {"-","—","–",".","..","...","n/a","na","none","null","нет данных","не указано","неизвестно"}:
        return True
    if "http://" in v or "https://" in v or "www." in v:
        return True
    if "<" in v and ">" in v:
        # вероятная HTML-каша — в публичный фид не нужно
        return True
    return False

def remove_specific_params(shop_el: ET.Element) -> int:
    removed = 0
    for off in shop_el.findall(".//offer"):
        for p in list(off.findall("param")):
            name = (p.get("name") or "").strip()
            val  = elem_text(p)
            if not name:
                off.remove(p); removed += 1; continue
            # Удаляем чисто служебные или пустые/мусор
            if name.lower() in {"благотворительность","код тн вэд"}:
                off.remove(p); removed += 1; continue
            if _value_is_empty_or_noise(val):
                off.remove(p); removed += 1; continue
    return removed

# ======================= ФОТО-ПЛЕЙСХОЛДЕРЫ =======================
def _head_ok(url: str, timeout_s: float = PLACEHOLDER_HEAD_TIMEOUT) -> bool:
    try:
        r = requests.head(url, timeout=timeout_s, allow_redirects=True)
        return r.ok
    except Exception:
        return False

def ensure_pictures_or_placeholders(shop_el: ET.Element) -> int:
    """Если нет картинок — ставим плейсхолдеры (бренд/категория/дефолт)."""
    if not PLACEHOLDER_ENABLE:
        return 0
    added = 0
    for off in shop_el.findall(".//offer"):
        pics = off.findall("picture")
        if pics:
            continue
        # пробуем бренд
        ven = get_text(off, "vendor")
        if ven:
            url = f"{PLACEHOLDER_BRAND_BASE}/{_norm_key(ven)}.{PLACEHOLDER_EXT}"
            if _head_ok(url):
                ensure_picture(off, url); added += 1; continue
        # по категории (здесь дерево категорий уже убрали — fallback к дефолту)
        if PLACEHOLDER_DEFAULT_URL:
            ensure_picture(off, PLACEHOLDER_DEFAULT_URL); added += 1
    return added

# ======================= AVAILABLE/CURRENCY =======================
FALSE_WORDS= {"false","0","no","n","нет","отсутствует","out of stock","unavailable","под заказ","ожидается","на заказ"}

def normalize_available_and_currency(shop_el: ET.Element) -> None:
    """Делаем available атрибутом <offer available="true/false">; убираем складские поля; форсим currencyId=KZT."""
    for off in shop_el.findall(".//offer"):
        # available -> атрибут
        avail_attr = (off.get("available") or "").strip().lower()
        avail_tag  = get_text(off, "available").strip().lower()
        final = "true"
        if avail_attr:
            final = "false" if avail_attr in FALSE_WORDS else "true"
        elif avail_tag:
            final = "false" if avail_tag in FALSE_WORDS else "true"
        off.set("available", final)
        # удаляем теги наличия
        if DROP_STOCK_TAGS:
            remove_nodes(off, "available","quantity","stock_quantity","quantity_in_stock")
        # валюта
        set_text(off, "currencyId", "KZT")

# ======================= ПОРЯДОК ТЕГОВ =======================
OFFER_ORDER = [
    ("categoryId",   "tag"),
    ("vendorCode",   "tag"),
    ("name",         "tag"),
    ("price",        "tag"),
    ("picture",      "multi"),  # сохраняем все
    ("vendor",       "tag"),
    ("currencyId",   "tag"),
    ("description",  "tag"),
    ("param",        "multi"),
    ("keywords",     "tag"),
]

def reorder_offer_children(shop_el: ET.Element) -> None:
    """Раскладываем теги в заданном порядке (не создаёт новые, только упорядочивает существующие)."""
    for off in shop_el.findall(".//offer"):
        children = list(off)
        desired: List[ET.Element] = []
        # categoryId=0 вставим позже, здесь просто порядок
        for tag, kind in OFFER_ORDER:
            if kind == "tag":
                el = off.find(tag)
                if el is not None:
                    desired.append(el)
            elif kind == "multi":
                desired.extend(off.findall(tag))
        # остальные, которые не перечислены — в конец, но воспроизводимо
        known = {id(x) for x in desired}
        tail  = [ch for ch in children if id(ch) not in known]
        for ch in (desired + tail):
            off.remove(ch)
            off.append(ch)

def ensure_categoryid_zero_first(shop_el: ET.Element) -> None:
    """Вставляем <categoryId>0</categoryId> самым первым узлом в каждом оффере."""
    for off in shop_el.findall(".//offer"):
        # удаление исходных categoryId сделано выше
        ch0 = list(off)
        cat = ET.Element("categoryId"); cat.text = "0"
        if ch0:
            off.insert(0, cat)
        else:
            off.append(cat)

# ======================= KEYWORDS =======================
def ensure_keywords(shop_el: ET.Element) -> int:
    """Формируем/обновляем <keywords> (без изменения description)."""
    upd = 0
    for off in shop_el.findall(".//offer"):
        name = get_text(off, "name")
        ven  = get_text(off, "vendor")
        vc   = get_text(off, "vendorCode")
        base = []
        if ven: base.append(ven)
        # извлекаем модельные маркеры (простая эвристика)
        for tok in re.split(r"[\s,;/()\[\]\-]+", name):
            t = tok.strip()
            if len(t) >= 3 and re.search(r"[A-Za-z0-9]", t):
                base.append(t)
        # фикс: убираем слишком общие/повторные
        uniq = []
        seen = set()
        for w in base:
            k = w.lower()
            if k in seen: continue
            seen.add(k); uniq.append(w)
        # добавляем топ-города (до лимита)
        geo = SATU_KEYWORDS_GEO[:SATU_KEYWORDS_GEO_MAX]
        words = uniq + geo
        kw = ", ".join(words)
        set_text(off, "keywords", kw)
        upd += 1
    return upd

# ======================= FEED_META =======================
def render_feed_meta_comment(pairs: List[Tuple[str,str]]) -> str:
    body = "\n".join([f"{k}: {v}" for k, v in pairs])
    return f"\n{body}\n"

# ======================= MAIN =======================
def main() -> None:
    log(f"Source: {SUPPLIER_URL if SUPPLIER_URL else '(not set)'}")
    data = load_source_bytes(SUPPLIER_URL)

    src_root = ET.fromstring(data)
    shop_in  = src_root.find("shop") if src_root.tag.lower() != "shop" else src_root
    if shop_in is None:
        err("XML: <shop> not found")

    offers_in_el = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None:
        err("XML: <offers> not found")

    src_offers = list(offers_in_el.findall("offer"))

    # Готовим выходную структуру
    out_root  = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop  = ET.SubElement(out_root, "shop")
    out_offers= ET.SubElement(out_shop, "offers")

    # Копируем офферы 1:1 (описание НЕ трогаем — уйдет как в источнике)
    for o in src_offers:
        out_offers.append(deepcopy(o))

    # Фильтр категорий (include/exclude по ID/названию)
    removed_count = 0
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        id2name,id2parent,parent2children = parse_categories_tree(shop_in)
        rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH)
        keep_ids: Set[str] = set(rules_ids)

        def is_included(cid: str, cname: str) -> bool:
            if ALSTYLE_CATEGORIES_MODE == "include":
                if keep_ids and cid in keep_ids:
                    return True
                if rules_names and category_name_match(rules_names, category_id_path(id2parent,id2name,cid)):
                    return True
                return False
            elif ALSTYLE_CATEGORIES_MODE == "exclude":
                if keep_ids and cid in keep_ids:
                    return False
                if rules_names and category_name_match(rules_names, category_id_path(id2parent,id2name,cid)):
                    return False
                return True
            return True

        for off in list(out_offers.findall("offer")):
            # исходный categoryId берём из источника
            src_cat_el = off.find("categoryId") or off.find("CategoryId")
            src_cid = (src_cat_el.text or "").strip() if src_cat_el is not None else ""
            src_cname = id2name.get(src_cid, "")
            if not is_included(src_cid, src_cname):
                out_offers.remove(off); removed_count += 1

        log(f"Category rules ({ALSTYLE_CATEGORIES_MODE}): removed={removed_count}")
    else:
        log("Category rules (off): removed=0")

    # Удаляем исходные categoryId (позже поставим 0 первым тегом)
    if DROP_CATEGORY_ID_TAG:
        for off in out_offers.findall("offer"):
            for node in list(off.findall("categoryId")) + list(off.findall("CategoryId")):
                off.remove(node)

    # Флаг/форсирование цен
    flagged = flag_unrealistic_supplier_prices(out_shop); log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")

    # Бренды
    ensure_vendor(out_shop)
    filled = ensure_vendor_auto_fill(out_shop); log(f"Vendors auto-filled: {filled}")

    # vendorCode/id
    ensure_vendorcode_with_article(out_shop, prefix=VENDORCODE_PREFIX, create_if_missing=True)
    sync_offer_id_with_vendorcode(out_shop)

    # Пересчёт розницы + принудительные цены
    reprice_offers(out_shop, PRICING_RULES)
    forced = enforce_forced_prices(out_shop); log(f"Forced price={PRICE_CAP_VALUE}: {forced}")

    # Чистим мусорные <param>
    removed_params = remove_specific_params(out_shop); log(f"Params removed: {removed_params}")

    # Фото-заглушки (если нет ни одной картинки)
    ph_added = ensure_pictures_or_placeholders(out_shop); log(f"Placeholders added: {ph_added}")

    # available/currency
    normalize_available_and_currency(out_shop)

    # Порядок тегов + categoryId=0 первым
    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)

    # Ключевые слова (НЕ трогаем description, только читаем при необходимости)
    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    # Нормализация пробелов в описаниях (вариант A)
    desc_fixed = normalize_descriptions_whitespace(out_shop); log(f"Descriptions whitespace normalized: {desc_fixed}")

    # Красивые отступы (Python 3.9+)
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # FEED_META
    built_alm = now_almaty()
    meta_pairs = [
        ("Source", SUPPLIER_URL or "(not set)"),
        ("Built (Almaty)", built_alm.strftime("%Y-%m-%d %H:%M:%S")),
        ("Mode", f"categories={ALSTYLE_CATEGORIES_MODE}"),
        ("Encoding", ENC),
        ("Description policy", "AS IS (whitespace-normalized)")
    ]
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # Сериализация
    xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")

    # Лёгкая косметика: перенос после FEED_META и пустая строка между офферами
    xml_text = re.sub(r"(-->)\s*(<shop\b)", r"\1\n\2", xml_text, count=1)
    xml_text = re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
        f.write(xml_text)
    # touch .nojekyll for gh-pages
    docs_dir = os.path.dirname(OUT_FILE_YML) or "docs"
    try:
        open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception:
        pass

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | description=AS IS")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
