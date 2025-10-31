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
from copy import deepcopy
from xml.etree import ElementTree as ET
from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo  # для времени Алматы в FEED_META
except Exception:
    ZoneInfo = None  # fallback на UTC+5 ниже

# ======================= ПАРАМЕТРЫ ОКРУЖЕНИЯ =======================
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "AlStyle").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/alstyle.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()

TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
REQUEST_TRIES = int(os.getenv("REQUEST_TRIES", "2"))
REQUEST_DELAY = int(os.getenv("REQUEST_DELAY_MS", "700")) / 1000.0

# КАТЕГОРИИ: include mode (читаем строго из файла)
CATS_FILE     = os.getenv("ALSTYLE_CATS_FILE", "docs/alstyle_categories.txt").strip()
CAT_MODE      = os.getenv("ALSTYLE_CATS_MODE", "include").strip().lower()  # include|all
DRY_RUN       = os.getenv("DRY_RUN", "0").strip() == "1"

# Ценообразование (НЕ ТРОГАЕМ — как в исходнике)
# base = минимальная дилерская; +4% и фикс-надбавка по диапазонам; последние 3 цифры → 900
PRICING_RULES: List[Tuple[int,int,float,int]] = [
    (101,        10_000,     4.0,  3_000),
    (10_001,     25_000,     4.0,  4_000),
    (25_001,     50_000,     4.0,  5_000),
    (50_001,     75_000,     4.0,  7_000),
    (75_001,     100_000,    4.0, 10_000),
    (100_001,    150_000,    4.0, 12_000),
    (150_001,    200_000,    4.0, 15_000),
    (200_001,    300_000,    4.0, 20_000),
    (300_001,    400_000,    4.0, 25_000),
    (400_001,    500_000,    4.0, 30_000),
    (500_001,    750_000,    4.0, 40_000),
    (750_001,  1_000_000,    4.0, 50_000),
    (1_000_001, 1_500_000,   4.0, 70_000),
    (1_500_001, 2_000_000,   4.0, 90_000),
    (2_000_001, 99_999_999,  4.0, 100_000),
]
PRICE_CAP_VALUE = int(os.getenv("PRICE_CAP_VALUE", "0"))  # 0 = выкл, иначе форс цена

# Кейворды (НЕ ТРОГАЕМ — как в исходнике)
SATU_KEYWORDS        = os.getenv("SATU_KEYWORDS", "on").strip()       # on/off
SATU_KEYWORDS_BRAND  = os.getenv("SATU_KEYWORDS_BRAND", "on").strip() # on/off
SATU_KEYWORDS_GEO    = os.getenv("SATU_KEYWORDS_GEO", "on").strip()   # on/off
SATU_KEYWORDS_GEO_LAT= os.getenv("SATU_KEYWORDS_GEO_LAT", "off").strip() # on/off
SATU_KEYWORDS_GEO_MAX= int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))

# ======================= УТИЛИТЫ =======================
def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARNING: {msg}", file=sys.stderr, flush=True)

def now_almaty() -> datetime:
    # Алматы UTC+5, если доступна ZoneInfo — используем Asia/Almaty
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Almaty"))
    return datetime.now(timezone(timedelta(hours=5)))

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
            return f.read()
    last_err = None
    for i in range(REQUEST_TRIES):
        try:
            r = requests.get(src, timeout=TIMEOUT_S)
            if r.status_code == 200 and r.content:
                return r.content
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
        time.sleep(REQUEST_DELAY + random.random()*0.25)
    raise RuntimeError(f"Failed to fetch supplier XML: {last_err}")

def get_text(el: ET.Element, tag: str, default: str="") -> str:
    """Безопасно вернуть .text у первого потомка tag."""
    if el is None:
        return default
    ch = el.find(tag)
    return (ch.text or "").strip() if ch is not None and ch.text else default

def set_text(el: ET.Element, tag: str, value: str) -> None:
    """Создать tag если нет и поставить .text=value."""
    ch = el.find(tag)
    if ch is None:
        ch = ET.SubElement(el, tag)
    ch.text = value

def remove_all(el: ET.Element, tag: str) -> int:
    """Удалить все потомки с именем tag. Вернуть сколько удалили."""
    cnt = 0
    for child in list(el):
        if child.tag == tag:
            el.remove(child)
            cnt += 1
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

# ======================= КАТЕГОРИИ: include/exclude =======================
class CatRule:
    __slots__ = ("raw", "kind", "pattern")
    def __init__(self, raw: str, kind: str, pattern):
        self.raw, self.kind, self.pattern = raw, kind, pattern

def _norm_text(s: str) -> str:
    s = (s or "").replace("\u00A0", " ").lower().replace("ё", "е")
    return re.sub(r"\s+", " ", s).strip()

def load_cat_rules(path: str) -> List[CatRule]:
    rules: List[CatRule] = []
    if not path or not os.path.exists(path):
        return rules
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            rules.append(CatRule(raw=raw, kind="include", pattern=re.compile(re.escape(raw), re.I)))
    return rules

def category_allowed(cat_id: str, cat_name: str, rules: List[CatRule]) -> bool:
    if CAT_MODE != "include":
        return True
    if not rules:
        return True
    key = f"{cat_id} {cat_name}".strip()
    n = _norm_text(key)
    for r in rules:
        if r.pattern.search(n):
            return True
    return False

# ======================= БРЕНДЫ / ВЕНДОР =======================
COMMON_BRANDS = [
    "HP","Hewlett-Packard","Samsung","LG","Sony","Acer","Asus","Lenovo",
    "MSI","Xerox","Canon","Brother","NV Print","Europrint","Epson","Dell",
    "Gigabyte","Apple","Kingston","Micron","Crucial","Seagate","Western Digital",
    "Zyxel","TP-Link","MikroTik","Logitech","Philips","Tefal","Panasonic","Midea",
    "Sokany","Redmond","Bosch","Siemens","Haier","Toshiba","Huawei","Honor"
]

def build_brand_index(shop: ET.Element) -> Dict[str,str]:
    idx: Dict[str,str] = {}
    cats = shop.find("categories")
    if cats is not None:
        for c in cats.findall("category"):
            nm = (c.text or "").strip()
            if nm:
                idx[_norm_text(nm).split(" ")[0]] = nm
    # добавим популярные бренды напрямую
    for b in COMMON_BRANDS:
        idx[_norm_text(b).split(" ")[0]] = b
    return idx

def _norm_key(s: str) -> str:
    return _norm_text(s).split(" ")[0]

def _find_brand_in_text(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    low = _norm_text(s)
    for b in COMMON_BRANDS:
        if _norm_text(b) in low:
            return b
    return ""

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
        v = offer.find("vendor")
        cur = (v.text or "").strip() if v is not None and v.text else ""
        if cur:
            continue
        guess = guess_vendor_for_offer(offer, brand_index)
        if guess:
            if v is None:
                v = ET.SubElement(offer, "vendor")
            v.text = guess
            touched += 1
    return touched

# ======================= ПОЛЯ / ПОРЯДОК / ЧИСТКА =======================
def purge_offer_tags_and_attrs_after(offer: ET.Element) -> None:
    """Удаляем служебные атрибуты и теги, которые больше не нужны покупателю."""
    offer.attrib.pop("_force_price", None)
    # вычищаем внутренние ценовые теги, чтобы не светить дилерку
    for t in ("oldprice","purchase_price","purchasePrice","wholesale","wholesale_price","opt_price","b2b","b2b_price"):
        remove_all(offer, t)

PREFERRED_ORDER = [
    "categoryId","vendorCode","name","price",
    "picture","picture","picture","vendor","currencyId","description","param","keywords"
]

def reorder_offer_children(shop_el: ET.Element) -> None:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return
    for off in offers_el.findall("offer"):
        kids = list(off)
        # Собираем по предпочтительному порядку
        new_kids: List[ET.Element] = []
        # categoryId первым (значение позже)
        cid = off.find("categoryId")
        if cid is not None:
            new_kids.append(cid)
        # vendorCode, name, price
        for tag in ("vendorCode","name","price"):
            el = off.find(tag)
            if el is not None: new_kids.append(el)
        # картинки
        for el in kids:
            if el.tag == "picture":
                new_kids.append(el)
        # vendor, currencyId, description, param*, keywords
        for tag in ("vendor","currencyId","description"):
            el = off.find(tag)
            if el is not None: new_kids.append(el)
        for el in kids:
            if el.tag == "param":
                new_kids.append(el)
        el = off.find("keywords")
        if el is not None: new_kids.append(el)

        # Добавим все остальное, что не попало (на всякий)
        present = set(id(k) for k in new_kids)
        for el in kids:
            if id(el) not in present:
                new_kids.append(el)

        # Перезапишем
        for el in list(off):
            off.remove(el)
        for el in new_kids:
            off.append(el)

def ensure_categoryid_zero_first(shop_el: ET.Element) -> None:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return
    for off in offers_el.findall("offer"):
        # если уже есть categoryId — поднимем вверх (reorder это делает), если нет — создадим "0"
        cid = off.find("categoryId")
        if cid is None:
            cid = ET.SubElement(off, "categoryId")
        cid.text = "0"

def fix_currency_id(shop_el: ET.Element, default_code: str="KZT") -> None:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return
    for off in offers_el.findall("offer"):
        cur = off.find("currencyId")
        if cur is None:
            cur = ET.SubElement(off, "currencyId")
        if not (cur.text or "").strip():
            cur.text = default_code

# ======================= ДОСТУПНОСТЬ / СКЛАД =======================
TRUE_WORDS  = {"true","1","yes","y","есть","в наличии","наличие","on","да","готово","available"}
FALSE_WORDS = {"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}

def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int,int,int]:
    """Переносим available в атрибут оффера + чистим складские поля. Возвращаем статистику."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0,0,0)
    t_true=t_false=t_q=t_qis=0
    for off in offers_el.findall("offer"):
        # переносим <available> в атрибут
        av = off.find("available")
        if av is not None and (av.text or "").strip():
            val = (av.text or "").strip().lower()
            is_true = (val in TRUE_WORDS)
            off.set("available", "true" if is_true else "false")
            off.remove(av)
            if is_true: t_true += 1
            else: t_false += 1
        else:
            # если совсем нет — считаем false
            if "available" not in off.attrib:
                off.set("available", "false")
                t_false += 1
        # чистим складские
        for t in ("quantity","quantity_in_stock","stock_quantity"):
            if off.find(t) is not None:
                off.remove(off.find(t))
                if t == "quantity": t_q += 1
                if t == "quantity_in_stock": t_qis += 1
    return (t_true,t_false,t_q,t_qis)

# ======================= ПРАЙСЫ (НЕ ТРОГАЕМ ЛОГИКУ) =======================
def _parse_price(val: str) -> Optional[int]:
    if not val:
        return None
    s = re.sub(r"[^\d]", "", val)
    return int(s) if s.isdigit() else None

def _min_dealer_price(off: ET.Element) -> Optional[int]:
    # приоритет: <prices type~dealer|опт|b2b> > прямые поля > <price>
    # (в текущей выгрузке у AlStyle — прямые поля)
    fields = ["purchase_price","wholesale","opt_price","b2b_price","price"]
    vals: List[int] = []
    for t in fields:
        el = off.find(t)
        if el is not None and el.text:
            v = _parse_price(el.text)
            if v is not None:
                vals.append(v)
    if not vals:
        return None
    return min(vals)

def _apply_margin(base: int, rules: List[Tuple[int,int,float,int]]) -> int:
    for lo, hi, pct, add in rules:
        if lo <= base <= hi:
            out = int(round(base * (1.0 + pct/100.0))) + int(add)
            # последние 3 цифры → 900
            return int(str(out)[:-3] + "900") if out >= 1000 else 900
    # если не попали никуда — по верхнему правилу
    out = int(round(base * 1.04)) + 100_000
    return int(str(out)[:-3] + "900") if out >= 1000 else 900

def reprice_offers(shop_el: ET.Element, rules: List[Tuple[int,int,float,int]]) -> None:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return
    for off in offers_el.findall("offer"):
        base = _min_dealer_price(off)
        if base is None:
            continue
        newp = _apply_margin(base, rules)
        # Заменяем/создаём единственный <price>
        remove_all(off, "price")
        ET.SubElement(off, "price").text = str(newp)

def enforce_forced_prices(shop_el: ET.Element) -> int:
    """Если PRICE_CAP_VALUE > 0, выставляем всем офферам price=VALUE (жёстко)."""
    if PRICE_CAP_VALUE <= 0:
        return 0
    touched = 0
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    for offer in offers_el.findall("offer"):
        if offer.attrib.get("_force_price"):
            remove_all(offer, "price")
            ET.SubElement(offer, "price").text = str(PRICE_CAP_VALUE)
            offer.attrib.pop("_force_price", None)
            touched += 1
    return touched

# ======================= КАРТИНКИ (НЕ ТРОГАЕМ) =======================
def ensure_placeholder_pictures(shop_el: ET.Element) -> Tuple[int,int]:
    """Если у оффера нет ни одной <picture> — добавляем одну-заглушку (минимально)."""
    added = skipped = 0
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0)
    for off in offers_el.findall("offer"):
        pics = [p for p in off.findall("picture") if (p.text or "").strip()]
        if pics:
            skipped += 1
            continue
        ET.SubElement(off, "picture").text = "https://via.placeholder.com/600x600.png?text=No+Image"
        added += 1
    return (added, skipped)

# ======================= KEYWORDS (НЕ ТРОГАЕМ) =======================
def tokenize_name(s: str) -> List[str]:
    s = (s or "").strip()
    s = re.sub(r"[«»“”\"'’]", " ", s)
    s = re.sub(r"[^\w\s\-+]", " ", s, flags=re.U)
    s = re.sub(r"\s+", " ", s).strip()
    return [t for t in s.split(" ") if t]

COLOR_MAP = {"черн":"черный","black":"black","бел":"белый","white":"white","син":"синий","blue":"blue","сер":"серый",
             "gray":"gray","красн":"красный","red":"red","зелен":"зеленый","green":"green","серебр":"серебряный",
             "silver":"silver","циан":"cyan","магент":"magenta"}

def color_tokens_from_name(name: str) -> List[str]:
    low = name.lower()
    out: List[str] = []
    mapping = COLOR_MAP
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
    return list(tokens)

def dedup_preserve_order(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def geo_tokens() -> List[str]:
    if not SATU_KEYWORDS_GEO:
        return []
    toks = ["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
            "Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавловск","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]
    if SATU_KEYWORDS_GEO_LAT:
        toks += ["Kazakhstan","Almaty","Astana","Shymkent","Karaganda","Aktobe","Pavlodar","Atyrau","Taraz",
                 "Oskemen","Semey","Kostanay","Kyzylorda","Oral","Petropavlovsk","Taldykorgan","Aktau","Temirtau","Ekibastuz","Kokshetau","Rudny"]
    toks = dedup_preserve_order(toks)
    return toks[:max(0, SATU_KEYWORDS_GEO_MAX)]

def keywords_from_name_generic(name: str) -> List[str]:
    raw_tokens = tokenize_name(name)
    # убираем чисто цифровые токены
    raw_tokens = [t for t in raw_tokens if not t.isdigit()]
    # выносим цвета
    colors = color_tokens_from_name(name)
    # собираем итог
    out = dedup_preserve_order(raw_tokens + colors)
    return out

def build_keywords_for_offer(offer: ET.Element) -> str:
    if SATU_KEYWORDS == "off":
        return ""
    name   = get_text(offer, "name")
    vendor = get_text(offer, "vendor").strip()
    toks = []
    if SATU_KEYWORDS_BRAND and vendor:
        toks.append(vendor)
    toks += extract_model_tokens(offer)
    toks += keywords_from_name_generic(name)
    toks += geo_tokens()
    toks = dedup_preserve_order(toks)
    return ", ".join(toks)

def ensure_keywords(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for off in offers_el.findall("offer"):
        remove_all(off, "keywords")
        kw = build_keywords_for_offer(off)
        if kw:
            ET.SubElement(off, "keywords").text = kw
            touched += 1
    return touched

# ======================= КОПИЯ ИСХОДНЫХ ДАННЫХ (AS IS) =======================
def build_output_from_source(shop_in: ET.Element, cat_rules: List[CatRule]) -> ET.Element:
    """Копируем исходные офферы AS IS (описание не трогаем), фильтруем по категориям, нормализуем поля."""
    out_root = ET.Element("yml_catalog")
    out_shop = ET.SubElement(out_root, "shop")

    # Копируем categories как есть
    cats_in = shop_in.find("categories")
    if cats_in is not None:
        out_cats = ET.SubElement(out_shop, "categories")
        for c in cats_in.findall("category"):
            out_c = ET.SubElement(out_cats, "category", attrib=dict(c.attrib))
            out_c.text = (c.text or "").strip()

    offers_in = shop_in.find("offers")
    out_offers = ET.SubElement(out_shop, "offers")

    # Фильтрация по include-правилам
    rules = cat_rules
    total = 0
    kept  = 0
    for off in offers_in.findall("offer") if offers_in is not None else []:
        total += 1
        cat_id = get_text(off, "categoryId")
        cat_nm = ""
        # у AlStyle categories->category нет имён для каждой айди прямо в оффере — не критично
        if not category_allowed(cat_id, cat_nm, rules):
            continue

        # создаем новый оффер
        new_off = ET.SubElement(out_offers, "offer", attrib={"id": f"AS{get_text(off,'vendorCode') or get_text(off,'id') or '0'}"})
        # перенесём базовые теги в порядке, который потом упорядочим окончательно
        for tag in ("categoryId","vendorCode","name","price"):
            el = off.find(tag)
            if el is not None:
                ET.SubElement(new_off, tag).text = (el.text or "").strip()

        # картинки
        for p in off.findall("picture"):
            if (p.text or "").strip():
                ET.SubElement(new_off, "picture").text = p.text.strip()

        # vendor
        v = off.find("vendor")
        if v is not None and (v.text or "").strip():
            ET.SubElement(new_off, "vendor").text = v.text.strip()

        # currencyId
        c = off.find("currencyId")
        if c is not None and (c.text or "").strip():
            ET.SubElement(new_off, "currencyId").text = c.text.strip()

        # description — как у поставщика, без форматирования
        d = off.find("description")
        if d is not None:
            # Берём ровно текст (у поставщика это простая строка даже если в <span>)
            desc_txt = inner_html(d)
            ET.SubElement(new_off, "description").text = desc_txt

        # param — переносим как есть (но позже вычистим лишнее)
        for prm in off.findall("param"):
            np = ET.SubElement(new_off, "param", attrib=dict(prm.attrib))
            np.text = (prm.text or "").strip()

        # перенос служебных складских полей/available — обработаем позже
        for t in ("available","quantity","quantity_in_stock","stock_quantity","purchase_price","wholesale","opt_price","b2b_price","oldprice"):
            el = off.find(t)
            if el is not None and (el.text or "").strip():
                ET.SubElement(new_off, t).text = (el.text or "").strip()

        kept += 1

    log(f"Found in source: offers={total}, categories={'on' if shop_in.find('categories') is not None else 'off'} mode={CAT_MODE}")
    return out_root

def remove_specific_params(shop_el: ET.Element) -> int:
    """Убираем 'Артикул' и 'Благотворительность' как просили."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    rm = 0
    for off in offers_el.findall("offer"):
        for p in list(off.findall("param")):
            nm = (p.attrib.get("name","") or "").strip().lower()
            if nm in {"артикул","благотворительность"}:
                off.remove(p); rm += 1
    return rm

# ======================= FINAL, SOFT SANITIZE OF <description> =======================
def final_soft_sanitize_description(out_shop: ET.Element) -> int:
    """
    Пост-обработка описаний БЕЗ изменения смысла/структуры:
    • Убираем лишние переводы строк и схлопываем двойные пробелы
    • Чиним пробелы перед , : ;
    • Гарантируем один пробел после , : ;
    • Нормализуем размеры: 1920x1080 → 1920×1080; 10 x 20 x 30 → 10×20×30
    • Заменяем '...' и длиннее на многоточие, а 'двойные точки' на одну
    • Убираем подряд дубли “Характеристики” (если встречаются слитно)
    НИЧЕГО не делаем, если описания нет.
    """
    offers_el = out_shop.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for off in offers_el.findall("offer"):
        d = off.find("description")
        if d is None:
            continue
        raw = inner_html(d)
        if not raw:
            continue
        txt = raw

        # 1) В одну строку, убрать лишние переносы
        txt = re.sub(r"\s*\n+\s*", " ", txt)

        # 2) Схлопнуть повторные пробелы/табуляции
        txt = re.sub(r"[ \t]{2,}", " ", txt)

        # 3) Убрать пробелы ПЕРЕД пунктуацией , : ;
        txt = re.sub(r"\s+([,;:])", r"\1", txt)

        # 4) Гарантировать один пробел ПОСЛЕ пунктуации , : ;
        txt = re.sub(r"([,;:])(?!\s|$)", r"\1 ", txt)

        # 5) Нормализовать размеры: цифра x/X цифра → ×
        txt = re.sub(r"(?<=\d)\s*[xX]\s*(?=\d)", "×", txt)

        # 6) Многоточия / двойные точки
        txt = re.sub(r"\.{3,}", "…", txt)       # 3+ точек → многоточие
        txt = re.sub(r"(?<!\.)\.\.(?!\.)", ".", txt)  # двойная точка вне многоточий → одна

        # 7) Дедуп заголовочного слова "Характеристики" если подряд/почти подряд
        txt = re.sub(r"(?:\bХарактеристики\b[\s—:-]*){2,}", "Характеристики: ", txt, flags=re.I)

        # 8) Финальная зачистка: по одному пробелу, обрезать края
        txt = re.sub(r"\s{2,}", " ", txt).strip()

        if txt != raw:
            # Поскольку у нас внутри <description> только текст, достаточно заменить .text
            # Если там когда-либо появятся дети, inner_html() уже дал плоский текст — это устраивает текущую стратегию
            for child in list(d):
                d.remove(child)
            d.text = txt
            touched += 1
    return touched

# ======================= MAIN =======================
def main() -> None:
    log(f"Source: {SUPPLIER_URL if SUPPLIER_URL else '(not set)'}")
    data = load_source_bytes(SUPPLIER_URL)

    src_root = ET.fromstring(data)
    shop_in  = src_root.find("shop") if src_root.tag.lower() != "shop" else src_root
    if shop_in is None:
        raise RuntimeError("В исходном файле не найден <shop>")

    # Правила категорий
    rules = load_cat_rules(CATS_FILE) if CAT_MODE == "include" else []

    # Строим аутпут из исходника (AS IS), применяя фильтр категорий
    out_root = build_output_from_source(shop_in, rules)
    out_shop = out_root.find("shop")
    if out_shop is None:
        raise RuntimeError("internal: no <shop> built")
    out_offers = out_shop.find("offers")
    src_offers = shop_in.find("offers").findall("offer") if shop_in.find("offers") is not None else []

    # Авто-дозаполнение vendor, если пусто (читаем name/description — описания не меняем)
    v_auto = ensure_vendor_auto_fill(out_shop); log(f"Vendor autofill: {v_auto}")

    # Пересчёт розницы + принудительные цены (как в исходнике)
    reprice_offers(out_shop, PRICING_RULES)
    forced = enforce_forced_prices(out_shop); log(f"Forced price={PRICE_CAP_VALUE}: {forced}")

    # Чистим мусорные <param>
    removed_params = remove_specific_params(out_shop); log(f"Params removed: {removed_params}")

    # Фото-заглушки (если нет ни одной картинки)
    ph_added, _ = ensure_placeholder_pictures(out_shop); log(f"Placeholders added: {ph_added}")

    # available → в атрибут оффера, удаляем складские поля
    t_true, t_false, _, _ = normalize_available_field(out_shop)

    # Валюта
    fix_currency_id(out_shop, default_code="KZT")

    # Чистка служебных тегов/атрибутов
    for off in out_offers.findall("offer"):
        purge_offer_tags_and_attrs_after(off)

    # Порядок тегов + categoryId=0 первым
    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)

    # Ключевые слова (НЕ трогаем description, только читаем при необходимости)
    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    # Красивые отступы (Python 3.9+)
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # Финальная мягкая санитарка описаний (без форматирования/HTML)
    desc_fixed = final_soft_sanitize_description(out_shop); log(f"Descriptions sanitized: {desc_fixed}")

    # FEED_META
    built_alm = now_almaty()
    meta_pairs = {
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL or "file",
        "built_alm": format_dt_almaty(built_alm),
        "next_build_alm": format_dt_almaty(next_build_time_almaty()),
        "offers_total": len(src_offers),
        "offers_written": len(list(out_offers.findall("offer"))),
        "available_true": str(t_true),
        "available_false": str(t_false),
    }
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
    try:
        with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
            f.write(xml_text)
    except UnicodeEncodeError as e:
        # Безопасное сохранение с заменой неподдерживаемых символов на XML-референсы
        warn(f"{ENC} can't encode some characters ({e}); writing with xmlcharrefreplace fallback}")
        data_bytes = xml_text.encode(ENC, errors="xmlcharrefreplace")
        with open(OUT_FILE_YML, "wb") as f:
            f.write(data_bytes)

    # .nojekyll для GitHub Pages
    try:
        docs_dir = os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True)
        with open(os.path.join(docs_dir, ".nojekyll"), "w", encoding="utf-8") as f:
            f.write("")
    except Exception as e:
        warn(f"nojekyll: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | description=AS IS + soft sanitize | in={len(src_offers)} | out={len(list(out_offers.findall('offer')))}")

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
    lines = ["FEED_META"] + [f"{k.ljust(key_w)} | {v}" for k,v in rows]
    return "\n".join(lines)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        warn(str(e))
        sys.exit(1)
