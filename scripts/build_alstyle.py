# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle -> YML (DESC-FLAT edition)

База = ваш КОД2 без изменений логики.
Единственное добавление: в самом конце ПЛОСКАЯ нормализация <description>
(удаляем теги внутри description, склеиваем всё в одну строку, схлопываем
много пробелов/переносов; пустые описания не трогаем).
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

REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "700"))
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
SATU_KEYWORDS          = os.getenv("SATU_KEYWORDS", "auto")
SATU_KEYWORDS_GEO_MAX  = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))
SATU_KEYWORDS_INCLUDE  = os.getenv("SATU_KEYWORDS_INCLUDE", "1").lower() in {"1","true","yes"}

# Прочее поведение
DROP_CATEGORY_ID_TAG   = os.getenv("DROP_CATEGORY_ID_TAG", "1").lower() in {"1","true","yes"}
PUT_CATEGORYID_ZERO    = os.getenv("PUT_CATEGORYID_ZERO", "1").lower() in {"1","true","yes"}

# ======================= УТИЛИТЫ =======================
def log(msg: str) -> None:
    print(msg, flush=True)

def sleep_ms(ms: int) -> None:
    time.sleep(ms / 1000.0)

def now_almaty() -> datetime:
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Almaty"))
    return datetime.utcnow() + timedelta(hours=5)

def fetch_bytes(url: str, timeout: int = TIMEOUT_S, retries: int = RETRIES) -> bytes:
    last_exc = None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200 and len(r.content) >= MIN_BYTES:
                return r.content
            last_exc = Exception(f"Bad HTTP {r.status_code} or too small ({len(r.content)} bytes)")
        except Exception as e:
            last_exc = e
        sleep_ms(REQUEST_DELAY_MS)
    raise last_exc or Exception("Unknown fetch error")

def parse_xml_bytes(b: bytes) -> ET.ElementTree:
    return ET.ElementTree(ET.fromstring(b))

def text_or_none(el: Optional[ET.Element]) -> Optional[str]:
    if el is None: return None
    t = el.text if el.text is not None else ""
    t = t.strip()
    return t if t else None

def get_text(parent: ET.Element, tag: str, default: Optional[str] = None) -> Optional[str]:
    node = parent.find(tag)
    if node is None:
        return default
    t = node.text if node.text is not None else ""
    t = t.strip()
    return t if t else default

def set_text(parent: ET.Element, tag: str, value: str) -> ET.Element:
    node = parent.find(tag)
    if node is None:
        node = ET.SubElement(parent, tag)
    node.text = value
    return node

def sanitize_price(p: str) -> Optional[int]:
    if p is None: return None
    p = re.sub(r"[^\d]", "", str(p))
    if not p: return None
    try:
        return int(p)
    except Exception:
        try:
            return int(float(p))
        except Exception:
            return None

def ensure_prefix_vendorcode(raw_vc: str) -> str:
    if not raw_vc:
        return VENDORCODE_PREFIX
    # уже с префиксом?
    if raw_vc.startswith(VENDORCODE_PREFIX):
        return raw_vc
    # без дефиса
    return f"{VENDORCODE_PREFIX}{raw_vc}"

def force_last_3_digits_900(n: int) -> int:
    if n < 0: return n
    return int(str(n)[:-3] + "900") if n >= 1000 else (900 if n else 0)

def add_price_margin(base: int) -> int:
    # Глобальная наценка 4% + ступеньки (как в памяти проекта)
    # Диапазоны — по тирагам из вашей памяти; здесь упрощённо с несколькими уровнями
    adders = [
        (101,      10000,   3000),
        (10001,    25000,   4000),
        (25001,    50000,   5000),
        (50001,    75000,   7000),
        (75001,    100000,  10000),
        (100001,   150000,  12000),
        (150001,   200000,  15000),
        (200001,   300000,  20000),
        (300001,   400000,  25000),
        (400001,   500000,  30000),
        (500001,   750000,  40000),
        (750001,   1000000, 50000),
        (1000001,  1500000, 70000),
        (1500001,  2000000, 90000),
        (2000001,  999999999, 100000),
    ]
    margin = max(0, int(round(base * 0.04)))
    for lo, hi, ad in adders:
        if lo <= base <= hi:
            return base + margin + ad
    return base + margin

def compute_price(src: ET.Element) -> Optional[int]:
    # База: dealer/opt/b2b -> поля прямые -> RRP
    priors = [
        "prices_dealer", "price_dealer", "dealer_price",
        "wholesalePrice","wholesale_price","opt_price",
        "b2bPrice","b2b_price",
        "purchasePrice","purchase_price",
        "price","oldprice"
    ]
    base = None
    for tag in priors:
        t = get_text(src, tag)
        val = sanitize_price(t) if t else None
        if val:
            base = val
            break
    if base is None:
        return None
    res = add_price_margin(base)
    res = force_last_3_digits_900(res)
    # Кэп завышенных
    if res > PRICE_CAP_THRESHOLD:
        res = PRICE_CAP_VALUE
    return res

# ======================= ПАРСИНГ И КОПИРОВАНИЕ =======================
def load_source_tree() -> ET.ElementTree:
    b = fetch_bytes(SUPPLIER_URL)
    return parse_xml_bytes(b)

def copy_offers(src_root: ET.Element) -> Tuple[ET.Element, ET.Element]:
    out_root = ET.Element("yml_catalog")
    shop = ET.SubElement(out_root, "shop")

    # categories передадим как есть (нужно для include/exclude)
    cats_src = src_root.find("shop/categories")
    cats_out = ET.SubElement(shop, "categories")
    if cats_src is not None:
        for c in cats_src.findall("category"):
            ET.SubElement(cats_out, "category", c.attrib).text = (c.text or "").strip()

    offers_out = ET.SubElement(shop, "offers")

    offers_src = src_root.find("shop/offers")
    if offers_src is None:
        return out_root, shop
    for off in offers_src.findall("offer"):
        new = ET.SubElement(offers_out, "offer", {"id": ""})
        # копируем только нужные поля: потом отредактируем
        for tag in ["price","currencyId","categoryId","url","vendorCode","picture","available","quantity","quantity_in_stock","vendor","name","description"]:
            for node in off.findall(tag):
                new_node = ET.SubElement(new, tag)
                new_node.text = (node.text or "").strip()
        # переносим все <param>
        for p in off.findall("param"):
            p_out = ET.SubElement(new, "param", p.attrib)
            p_out.text = (p.text or "").strip()
    return out_root, shop

# ======================= ОЧИСТКА/НОРМАЛИЗАЦИЯ ПОЛЕЙ (КРОМЕ DESCRIPTION) =======================
def normalize_offer_fields(shop: ET.Element) -> None:
    offers = shop.find("offers")
    if offers is None: return
    for off in offers.findall("offer"):
        # id и vendorCode
        vc_src = get_text(off, "vendorCode") or ""
        vc = ensure_prefix_vendorcode(re.sub(r"\D+", "", vc_src) or vc_src)
        off.set("id", vc)
        set_text(off, "vendorCode", vc)

        # price
        p_raw = get_text(off, "price")
        price = sanitize_price(p_raw)
        if price is None:
            price = compute_price(off)
        if price is not None:
            set_text(off, "price", str(price))

        # vendor (не трогаем логику, только авто-LG кейс ниже не включаем)
        v = get_text(off, "vendor")
        if v:
            set_text(off, "vendor", v.strip())

        # name
        nm = get_text(off, "name")
        if nm:
            nm = re.sub(r"\s+", " ", nm).strip()
            set_text(off, "name", nm)

        # currencyId
        cur = get_text(off, "currencyId") or "KZT"
        set_text(off, "currencyId", cur)

        # pictures: уникальные и без пустых
        pics = [ (p.text or "").strip() for p in off.findall("picture") if (p.text or "").strip() ]
        for p in list(off.findall("picture")):
            off.remove(p)
        seen = set()
        for url in pics:
            if url in seen: continue
            seen.add(url)
            ET.SubElement(off, "picture").text = url

        # available -> атрибут + перенос в available="true/false" уже есть в src?
        avail = get_text(off, "available")
        if avail is not None:
            # как есть, но схлопнем
            a = avail.strip().lower()
            true_words  = {"true","1","yes","y","да","в наличии","есть","instock","in stock","available"}
            false_words = {"false","0","no","n","нет","отсутствует","out of stock","unavailable","под заказ","ожидается","на заказ"}
            is_true = (a in true_words) or a.startswith("true")
            is_false= (a in false_words) or a.startswith("false")
            off.set("available", "true" if (is_true and not is_false) else "false")
            # сам тег оставляем как есть
        else:
            # если нет — не создаём

            pass

        # quantity/quantity_in_stock: подчистим пробелы
        for qtag in ("quantity","quantity_in_stock"):
            qt = get_text(off, qtag)
            if qt is not None:
                set_text(off, qtag, re.sub(r"\s+", " ", qt))

        # categoryId оставим, позже двинем в начало при записи
        # url трогать не будем

# ======================= КАТЕГОРИИ include/exclude =======================
def load_category_rules(path: str) -> Set[str]:
    keep = set()
    if not os.path.exists(path):
        return keep
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            keep.add(line)
    return keep

def apply_category_rules(shop: ET.Element) -> int:
    offers = shop.find("offers")
    if offers is None: return 0
    mode = ALSTYLE_CATEGORIES_MODE
    if mode not in {"include","exclude"}:  # off
        return 0
    keep_ids = load_category_rules(ALSTYLE_CATEGORIES_PATH)
    removed = 0
    for off in list(offers.findall("offer")):
        cid = get_text(off, "categoryId")
        hit = (cid in keep_ids) if cid else False
        drop = (mode == "exclude" and hit) or (mode == "include" and not hit)
        if drop:
            offers.remove(off); removed += 1
    return removed

# ======================= КЛЮЧЕВЫЕ СЛОВА =======================
GEO_CITIES = [
    "Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау",
    "Тараз","Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавловск","Талдыкорган",
    "Актау","Темиртау","Экибастуз","Кокшетау"
]

def ensure_keywords(shop: ET.Element) -> int:
    """auto: собираем name + вендор + модели из description (только чтение текста) + гео."""
    if not SATU_KEYWORDS_INCLUDE or SATU_KEYWORDS.lower() != "auto":
        return 0
    offers = shop.find("offers")
    if offers is None: return 0
    touched = 0
    for off in offers.findall("offer"):
        name = get_text(off, "name") or ""
        vendor = get_text(off, "vendor") or ""
        # выдёргиваем кандидаты из description как простой текст (если есть)
        desc = get_text(off, "description") or ""
        # простая выжимка моделей (латиница/цифры/дефис)
        models = re.findall(r"[A-Za-z0-9][A-Za-z0-9\-]{2,}", desc)
        parts = []
        if vendor: parts.append(vendor)
        if name: parts.append(name)
        parts.extend(models[:8])
        # GEO
        parts.extend(GEO_CITIES[:SATU_KEYWORDS_GEO_MAX])
        kw = ", ".join(parts)
        set_text(off, "keywords", kw)
        touched += 1
    return touched

# ======================= ПЛОСКАЯ НОРМАЛИЗАЦИЯ DESCRIPTION (ПОДХОД 2) =======================
def _flatten_desc_text(desc_node: ET.Element) -> Optional[str]:
    """Вернуть плоскую строку (без HTML) из узла <description>.
       Если описания нет или оно пустое — вернуть None (значит, не трогаем).
    """
    # Если внутри чистый текст — берём
    base = desc_node.text or ""
    pieces = []
    if base and base.strip():
        pieces.append(base)
    # Добавим тексты детей — с пробелами вместо тэгов
    for ch in desc_node.iter():
        if ch is desc_node:
            continue
        if ch.text and ch.text.strip():
            pieces.append(ch.text)
        if ch.tail and ch.tail.strip():
            pieces.append(ch.tail)
    if not pieces:
        t = (desc_node.text or "").strip()
        if not t:
            return None
        return re.sub(r"\s+", " ", t)
    flat = " ".join(pieces)
    # Декодируем сущности и схлопываем пробелы/переводы строк
    flat = html.unescape(flat).replace("\xa0", " ")
    flat = re.sub(r"\s+", " ", flat).strip()
    return flat if flat else None

def flatten_all_descriptions(shop_el: ET.Element) -> int:
    """Подход 2: превратить любое содержимое <description> в одну чистую строку текста.
       Пустые описания не трогаем. Никаких HTML-тегов не добавляем.
    """
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        d = offer.find("description")
        if d is None:
            continue
        new_text = _flatten_desc_text(d)
        if new_text is None:
            # есть description, но пустое — оставим как есть
            continue
        # Заменяем текст, удаляем всех детей (чтобы Tree не расставлял отступы внутри)
        d.text = new_text
        for ch in list(d):
            d.remove(ch)
        touched += 1
    return touched

# ======================= ФИНАЛЬНАЯ ПОЛИРОВКА ПЛОСКОГО DESCRIPTION (Только текст) =======================
def polish_all_descriptions(shop_el: ET.Element) -> int:
    """Финальная лёгкая полировка текста описаний:
    - декодируем HTML-сущности (&nbsp; → пробел и т.д.)
    - схлопываем все виды пробелов/переводов строк в один пробел
    - убираем пробелы ПЕРЕД знаками препинания и добавляем ОДИН пробел ПОСЛЕ (кроме конца строки)
    - удаляем одиночные маркеры вроде •/●/▪ и одиночные '>'
    - убираем повторы слов вида "Xerox Xerox" → "Xerox"
    Пустые описания не трогаем.
    """
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        d = offer.find("description")
        if d is None:
            continue
        s = d.text if (d.text is not None) else ""
        if not s or not s.strip():
            continue
        # 1) Декодировать сущности и заменить неразрывные пробелы на обычные
        s = html.unescape(s).replace("\xa0", " ")
        # 2) Схлопнуть все пробельные последовательности (включая переводы строк) в один пробел
        s = re.sub(r"\s+", " ", s).strip()
        # 3) Убрать пробелы перед знаками препинания ,.;:!? и обеспечить один пробел после, где нужно
        s = re.sub(r"\s+([,.;:!?])", r"\1", s)
        s = re.sub(r"([,.;:!?])(?!\s|$)", r"\1 ", s)
        s = re.sub(r"\s{2,}", " ", s).strip()
        # 4) Удалить одиночные маркеры и лишние '>' (не в HTML, т.к. мы работаем с плоским текстом)
        s = re.sub(r"(?<!\w)[•●▪►▶›»](?!\w)", "", s)
        s = re.sub(r"\s*>\s*", " ", s)
        s = re.sub(r"\s{2,}", " ", s).strip()
        # 5) Удалить повторы соседних слов (регистр не учитываем)
        s = re.sub(r"\b(\w+)(\s+\1\b)+", r"\1", s, flags=re.IGNORECASE)
        # Назначить обратно
        if s != d.text:
            d.text = s
            touched += 1
    return touched

# ======================= СБОРКА SHOP =======================
def build_output_tree(src_root: ET.Element) -> Tuple[ET.ElementTree, ET.Element]:
    out_root, out_shop = copy_offers(src_root)

    # normalize fields (кроме description)
    normalize_offer_fields(out_shop)

    # категории include/exclude
    removed = apply_category_rules(out_shop)
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        log(f"Category rules ({ALSTYLE_CATEGORIES_MODE}): removed={removed}")
    else:
        log("Category rules (off): removed=0")

    return ET.ElementTree(out_root), out_shop

# ======================= СЕРИАЛИЗАЦИЯ =======================
def ensure_categoryid_zero_first(shop: ET.Element) -> None:
    """Переставим <categoryId> сразу после <offer ...> как первый тег, если требуется."""
    if not PUT_CATEGORYID_ZERO:
        return
    offers = shop.find("offers")
    if offers is None: return
    for off in offers.findall("offer"):
        cid = off.find("categoryId")
        if cid is None:
            cid = ET.Element("categoryId"); cid.text = "0"
        # удалить все старые
        for old in off.findall("categoryId"):
            off.remove(old)
        # вставить первым
        children = list(off)
        off.clear()
        for ch in [cid] + children:
            off.append(ch)

def write_yml(tree: ET.ElementTree, shop: ET.Element, path: str, enc: str) -> None:
    # Перед записью — гарантируем categoryId первым
    ensure_categoryid_zero_first(shop)

    # Красивые отступы, если поддерживается
    try:
        ET.indent(tree, space="  ")
    except Exception:
        pass

    xml_decl = '<?xml version="1.0" encoding="%s"?>\n' % enc.upper()
    data = ET.tostring(tree.getroot(), encoding=enc, xml_declaration=False)
    # В некоторых случаях кодировка может не потянуть символы — подстрахуемся
    try:
        data.decode(enc)
        payload = data
    except Exception as e:
        log(f"WARNING: {enc} can't encode some characters ({e}); using xmlcharrefreplace")
        payload = data.decode(enc, errors="xmlcharrefreplace").encode(enc, errors="xmlcharrefreplace")

    if DRY_RUN:
        log(f"[DRY_RUN] Would write: {path} | encoding={enc} | bytes={len(payload)}")
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(xml_decl.encode(enc, errors="ignore"))
        f.write(payload)
    log(f"Wrote: {path} | encoding={enc}")

# ======================= MAIN =======================
def main() -> None:
    log(f"Source: {SUPPLIER_URL if SUPPLIER_URL else '(not set)'}")

    # 1) Загружаем исходный XML
    src_bytes = fetch_bytes(SUPPLIER_URL)
    src_tree  = parse_xml_bytes(src_bytes)
    src_root  = src_tree.getroot()

    # 2) Копируем offers и нормализуем поля (не трогаем <description>)
    out_tree, out_shop = build_output_tree(src_root)
    out_root = out_tree.getroot()

    # 3) categoryId = 0 первым тегом
    ensure_categoryid_zero_first(out_shop)

    # 14) Ключевые слова (описание только ЧИТАЕМ при извлечении моделей)
    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    # 15) ПЛОСКАЯ нормализация описаний (ПОДХОД 2): одна строка, без HTML-тегов
    desc_touched = flatten_all_descriptions(out_shop); log(f"Descriptions flattened: {desc_touched}")
    # 15b) Финальная полировка плоского текста описаний
    desc_polished = polish_all_descriptions(out_shop); log(f"Descriptions polished: {desc_polished}")

    # Красивые отступы (Python 3.9+). На плоский текст внутри <description> это не влияет.
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # FEED_META
    built_alm = now_almaty()
    fm = ET.SubElement(out_shop, "feed_meta")
    ET.SubElement(fm, "built_at_almaty").text = built_alm.strftime("%Y-%m-%d %H:%M:%S")
    ET.SubElement(fm, "source").text = SUPPLIER_URL

    # 16) Запись
    write_yml(out_tree, out_shop, OUT_FILE_YML, ENC)

if __name__ == "__main__":
    try:
        print("Run set - e".replace(" - e"," -e"))
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
