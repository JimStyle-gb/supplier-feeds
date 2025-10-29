# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle → Satu YML builder
Версия: alstyle-2025-10-29.custom

Назначение:
- Читать исходный XML/JSON/фид поставщика AlStyle
- Отфильтровывать по категориям (режим include/exclude/off) из docs/alstyle_categories.txt
- Нормализовать офферы: id/vendorCode, brand, price, availability, pictures
- Собирать SEO-описание (лид, родное описание, характеристики, FAQ, отзывы)
- Генерировать docs/alstyle.yml (windows-1251) + .nojekyll

ВАЖНО:
- Зафиксированы правила цены (глобальная политика) и финального формата
- <available> как атрибут у <offer>, не тег
- <categoryId> вставляется первым узлом и = 0 (по требованию)
- keywords с гео: Казахстан и города
"""

from __future__ import annotations
import os, re, io, sys, json, time, math, hashlib, html, textwrap, urllib.parse
from datetime import datetime, timedelta, timezone
from xml.etree.ElementTree import Element, SubElement, ElementTree, tostring, fromstring

import requests

# ======================= НАСТРОЙКИ / ENV =======================
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "AlStyle").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL",  "https://api.al-style.kz/yml.xml").strip()
OUT_FILE      = os.getenv("OUT_FILE",      "docs/alstyle.yml").strip()
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()
REQUEST_TIMEOUT_S = int(os.getenv("TIMEOUT_S", "30"))
REQUEST_RETRIES   = int(os.getenv("RETRIES", "2"))
MIN_BYTES         = int(os.getenv("MIN_BYTES", "1500"))

CATEGORY_MODE   = os.getenv("CATEGORY_MODE", "include").strip().lower()  # include|exclude|off
CATS_FILE       = os.getenv("CATS_FILE", "docs/alstyle_categories.txt").strip()

CATEGORY_ID_DEFAULT = int(os.getenv("CATEGORY_ID_DEFAULT", "0"))

VENDOR_PREFIX   = os.getenv("VENDOR_PREFIX", "AS").strip()  # Префикс для vendorCode/id
CURRENCY_ID     = os.getenv("CURRENCY_ID", "KZT").strip()

# Pricing policy (глобальная)
PERCENT = float(os.getenv("PRICE_PERCENT", "4.0"))
ADDERS = [
    (101,        10_000,   3_000),
    (10_001,     25_000,   4_000),
    (25_001,     50_000,   5_000),
    (50_001,     75_000,   7_000),
    (75_001,     100_000, 10_000),
    (100_001,    150_000, 12_000),
    (150_001,    200_000, 15_000),
    (200_001,    300_000, 20_000),
    (300_001,    400_000, 25_000),
    (400_001,    500_000, 30_000),
    (500_001,    750_000, 40_000),
    (750_001,  1_000_000, 50_000),
    (1_000_001, 1_500_000, 70_000),
    (1_500_001, 2_000_000, 90_000),
    (2_000_001, 9_999_999_999, 100_000),
]
FORCE_ENDING = "900"  # последние 3 цифры

PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "50_000_000"))  # анти-аномалии

# SEO
SEO_REFRESH_MODE = os.getenv("SEO_REFRESH_MODE", "monthly").strip()  # monthly|always|never
SEO_CACHE_FILE   = os.getenv("SEO_CACHE_FILE", "docs/.alstyle_seo_cache.json").strip()

# Keywords
KEYWORDS_MODE = os.getenv("SATU_KEYWORDS", "auto").strip()  # auto|off
KEYWORDS_MAX_WORDS = int(os.getenv("KEYWORDS_MAX_WORDS", "30"))
KEYWORDS_MAX_LEN   = int(os.getenv("KEYWORDS_MAX_LEN", "250"))

# ======================= УТИЛИТЫ =======================
ALMATY_TZ = timezone(timedelta(hours=5))

def now_local() -> datetime:
    return datetime.now(ALMATY_TZ)

def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8", "ignore")).hexdigest()

def safe_int(x, default=0):
    try:
        return int(str(x).strip())
    except Exception:
        return default

def request_bytes(url: str, timeout=REQUEST_TIMEOUT_S, retries=REQUEST_RETRIES, min_bytes=MIN_BYTES) -> bytes:
    last = None
    for i in range(max(1, retries)):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": "alstyle-bot/1.0"})
            r.raise_for_status()
            b = r.content
            if len(b) >= min_bytes:
                return b
            last = f"too-small({len(b)})"
        except Exception as e:
            last = str(e)
            time.sleep(1 + i)
    raise RuntimeError(f"download-failed: {url} ({last})")

def ensure_dir_for_file(path: str):
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)

def write_text(path: str, text: str, enc=OUTPUT_ENCODING):
    ensure_dir_for_file(path)
    with open(path, "w", encoding=enc, errors="replace") as f:
        f.write(text)

def read_text(path: str, enc="utf-8"):
    with open(path, "r", encoding=enc, errors="ignore") as f:
        return f.read()

def _html_escape_in_cdata_safe(s: str) -> str:
    # Для безопасной вставки в CDATA: экранируем только «подозрительные» последовательности, но оставляем HTML разметку
    return s.replace("]]>", "]]&gt;")

def inner_text(elem) -> str:
    if elem is None:
        return ""
    return "".join(elem.itertext()).strip()

def inner_html(elem) -> str:
    # Возвращаем HTML-потомков узла без обёртки
    if elem is None:
        return ""
    parts = []
    for x in elem:
        parts.append(tostring(x, encoding="unicode"))
    return "".join(parts)

def as_cdata(html_str: str) -> str:
    return f"<![CDATA[\n{html_str}\n]]>"

def slug(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9\-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s

# ======================= ЗАГРУЗКА ИСХОДНИКА =======================
def load_supplier_xml() -> Element:
    src = SUPPLIER_URL
    if src.startswith("file://"):
        raw = read_text(src.replace("file://", ""), enc="utf-8").encode("utf-8")
    else:
        raw = request_bytes(src)
    if not raw or len(raw) < MIN_BYTES:
        raise RuntimeError(f"supplier-xml-too-small: {len(raw)}")
    # Попытаться прямо распарсить
    try:
        return fromstring(raw.decode("utf-8", "ignore"))
    except Exception:
        return fromstring(raw)

# ======================= КАТЕГОРИИ: ЧТЕНИЕ И РЕЖИМЫ =======================
def read_categories_file() -> list[int]:
    if not os.path.exists(CATS_FILE):
        return []
    ids = []
    for ln in read_text(CATS_FILE).splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        m = re.match(r"^(\d+)", ln)
        if m:
            ids.append(int(m.group(1)))
    return ids

def collect_category_tree(root: Element) -> dict[int, dict]:
    cats = {}
    shop = root.find("./shop")
    if shop is None: return cats
    cats_el = shop.find("./categories")
    if cats_el is None: return cats
    for c in cats_el.findall("./category"):
        cid = safe_int(c.attrib.get("id"))
        pid = safe_int(c.attrib.get("parentId", "0"), 0)
        name = inner_text(c)
        cats[cid] = {"id": cid, "parentId": pid, "name": name, "children": []}
    # fill children
    for c in cats.values():
        pid = c["parentId"]
        if pid in cats:
            cats[pid]["children"].append(c["id"])
    return cats

def expand_cat_ids_with_children(cat_ids: list[int], cats: dict[int, dict]) -> set[int]:
    out = set()
    stack = list(cat_ids)
    while stack:
        cid = stack.pop()
        if cid in out: continue
        out.add(cid)
        if cid in cats:
            stack.extend(cats[cid]["children"])
    return out

# ======================= ФИЛЬТР ПО КАТЕГОРИЯМ =======================
def filter_offers_by_categories(root: Element) -> tuple[int,int]:
    shop = root.find("./shop")
    offers = shop.find("./offers") if shop is not None else None
    if offers is None:
        return 0, 0

    cats = collect_category_tree(root)
    cfg_ids = read_categories_file()

    if CATEGORY_MODE == "off":
        return len(offers.findall("./offer")), 0

    if CATEGORY_MODE not in ("include", "exclude"):
        raise RuntimeError(f"bad CATEGORY_MODE: {CATEGORY_MODE}")

    if not cfg_ids:
        raise RuntimeError(f"empty categories file for mode={CATEGORY_MODE}: {CATS_FILE}")

    cfg_expanded = expand_cat_ids_with_children(cfg_ids, cats)
    keep, drop = 0, 0
    for off in list(offers.findall("./offer")):
        cids = [safe_int(x.text) for x in off.findall("./categoryId")]
        hit = any(cid in cfg_expanded for cid in cids)
        ok = (hit if CATEGORY_MODE == "include" else (not hit))
        if ok:
            keep += 1
        else:
            offers.remove(off)
            drop += 1
    return keep, drop

# ======================= ЧИСТКА CATEGORYID И СЛУЖЕБНОГО =======================
SERVICE_TAGS_TO_DROP = {
    "oldprice", "purchase_price", "wholesale_price", "wholesale", "b2b_price", "b2b",
    "url", "model", "delivery", "local_delivery_cost", "barcode", "vendorCodeRaw"
}

def drop_supplier_category_ids_and_service_tags(root: Element):
    shop = root.find("./shop")
    offers = shop.find("./offers") if shop is not None else None
    if offers is None:
        return
    for off in offers.findall("./offer"):
        # drop all supplier categoryId nodes; мы вставим свой потом
        for n in off.findall("./categoryId"):
            off.remove(n)
        # drop service tags
        for t in list(off):
            tag = t.tag.strip().lower()
            if tag in SERVICE_TAGS_TO_DROP:
                off.remove(t)

# ======================= АНТИ-АНОМАЛИИ ЦЕН =======================
def apply_price_cap_guard(root: Element):
    shop = root.find("./shop")
    offers = shop.find("./offers") if shop is not None else None
    if offers is None:
        return
    for off in offers.findall("./offer"):
        p = off.find("./price")
        if p is None: 
            continue
        try:
            val = float(p.text.strip())
        except Exception:
            continue
        if val >= PRICE_CAP_THRESHOLD:
            # форсируем короткую цену (не влияя на логику дальше)
            p.text = "100"

# ======================= БРЕНД =======================
SUPPLIER_BRAND_BLOCKLIST = {"alstyle", "al-style", "copyline", "vtt", "akcent", "ak-cent", "unknown", "no brand", "noname"}

def normalize_vendor_brand(off: Element):
    v = off.find("./vendor")
    name = inner_text(off.find("./name")).strip()
    if v is not None:
        b = inner_text(v).strip()
        bl = b.lower()
        if bl in SUPPLIER_BRAND_BLOCKLIST:
            off.remove(v)
            v = None
    if v is None:
        # попробовать извлечь бренд из name
        m = re.match(r"^\s*([A-Za-z0-9ЁёА-Яа-я\-\+ ]{2,20})\b", name)
        if m:
            brand = m.group(1).strip()
            if brand and brand.lower() not in SUPPLIER_BRAND_BLOCKLIST:
                v = SubElement(off, "vendor")
                v.text = brand

# ======================= VENDORCODE / OFFER ID =======================
def extract_article_from_text(t: str) -> str:
    # Ищем похожее на артикул
    if not t: return ""
    m = re.search(r"\b([A-Z]{1,5}\d[\w\-]{2,})\b", t, re.I)
    return m.group(1) if m else ""

def ensure_vendorcode_and_id(off: Element):
    vc = off.find("./vendorCode")
    if vc is None or not inner_text(vc):
        # ищем в исходных полях
        name = inner_text(off.find("./name"))
        url  = inner_text(off.find("./url"))
        v = extract_article_from_text(name) or extract_article_from_text(url)
        if not v:
            v = md5(name)[:8].upper()
        vc = SubElement(off, "vendorCode"); vc.text = v
    # префикс
    raw = inner_text(vc)
    if not raw.upper().startswith(VENDOR_PREFIX):
        vc.text = f"{VENDOR_PREFIX}{raw}"
    # синхронизируем <offer id="…">
    off.attrib["id"] = vc.text

# ======================= ПРАЙСИНГ =======================
def pick_dealer_price(off: Element) -> float | None:
    # приоритет: <prices type~dealer/опт/b2b> > поля purchase/wholesale/b2b > <price> как RRP fallback
    prices = off.find("./prices")
    cand = []
    if prices is not None:
        for pr in prices.findall("./price"):
            t = (pr.attrib.get("type","") or "").lower()
            if any(k in t for k in ("dealer", "опт", "opt", "b2b")):
                try:
                    cand.append(float(pr.text.strip()))
                except Exception:
                    pass
    for tag in ("purchase_price","wholesale_price","wholesale","b2b_price","b2b"):
        n = off.find(f"./{tag}")
        if n is not None:
            try:
                cand.append(float(n.text.strip()))
            except Exception:
                pass
    if cand:
        return min(cand)
    # fallback
    p = off.find("./price")
    if p is not None:
        try: return float(p.text.strip())
        except Exception: return None
    return None

def apply_global_pricing(off: Element):
    base = pick_dealer_price(off)
    if base is None:
        return
    val = base * (1.0 + PERCENT/100.0)
    # adder by tiers
    for lo, hi, add in ADDERS:
        if lo <= base <= hi:
            val += add
            break
    # округление до целого
    val = float(int(round(val)))
    # заставить окончание ...900
    s = str(int(val))
    if len(s) >= 3:
        s = s[:-3] + FORCE_ENDING
    else:
        s = FORCE_ENDING
    # записать в <price>
    p = off.find("./price")
    if p is None:
        p = SubElement(off, "price")
    p.text = s
    # удалить служебные ценовые теги
    for tag in ("oldprice","purchase_price","wholesale_price","wholesale","b2b_price","b2b"):
        n = off.find(f"./{tag}")
        if n is not None:
            off.remove(n)

# ======================= ПАРАМЕТРЫ (ЧИСТКА) =======================
DROP_PARAM_NAMES = {
    "Артикул","ТНВЭД","Штрихкод","ШК","Назначение","Назначение: Да","Назначение : Да","Назначение :Да",
    "Благотворительность","Серийный номер","URL","Код поставщика","EAN","UPC",
}

def cleanup_params(off: Element):
    for p in list(off.findall("./param")):
        name = p.attrib.get("name","").strip()
        val  = inner_text(p).strip()
        if not name or not val:
            off.remove(p); continue
        if name in DROP_PARAM_NAMES:
            off.remove(p); continue
        # убрать дубликаты по имени + значению
    seen = set()
    for p in list(off.findall("./param")):
        k = (p.attrib.get("name","").strip().lower(), inner_text(p).strip().lower())
        if k in seen:
            off.remove(p)
        else:
            seen.add(k)

# ======================= ФОТО (плейсхолдер при отсутствии) =======================
PLACEHOLDER_BY_VENDOR = {
    # Примеры: "Samsung": "https://example.com/ph/samsung.jpg"
}
DEFAULT_PLACEHOLDER = "https://dummyimage.com/600x400/efefef/555555.png&text=Photo+coming+soon"

def ensure_pictures(off: Element):
    pics = off.findall("./picture")
    if pics:
        return
    # по бренду
    v = inner_text(off.find("./vendor")).strip()
    url = PLACEHOLDER_BY_VENDOR.get(v) or DEFAULT_PLACEHOLDER
    SubElement(off, "picture").text = url

# ======================= RAW DESCRIPTION BEAUTIFIER =======================
def _looks_like_html(s: str) -> bool:
    """Heuristic: if the block already contains semantic HTML tags, we leave it as-is."""
    if not s: return False
    low = s.lower()
    return any(tag in low for tag in ("<p", "<ul", "<ol", "<li", "<h1", "<h2", "<h3", "<h4", "<h5", "<h6", "<table", "<br"))

_BULLET_RE = re.compile(r"^\s*(?:[-–—*•·]|•)\s*(.+)$")
_HDR_CAND_RE = re.compile(r"^\s*([A-Za-zА-Яа-яЁё0-9 ,/()+\-]{2,60})\s*:\s*$")

def _format_plain_text_to_html(s: str) -> str:
    """
    Turn supplier's raw free text (without tags) into compact HTML:
    - Convert header-like lines ending with ':' to <h3>
    - Convert bullet-like lines to <ul><li>…</li></ul> groups
    - Other text → <p>…</p>
    """
    if not (s or "").strip():
        return ""
    # Normalize newlines and trim noise
    t = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.rstrip() for ln in t.split("\n")]
    blocks = []
    buf = []
    cur_ul = None  # None or list of <li> items

    def flush_paragraph():
        nonlocal buf, blocks
        # Join buffered lines into a paragraph if not empty
        txt = " ".join(x.strip() for x in buf if x.strip())
        buf = []
        if txt:
            blocks.append(f"<p>{_html_escape_in_cdata_safe(txt)}</p>")

    def flush_ul():
        nonlocal cur_ul, blocks
        if cur_ul:
            items = "\n".join(f"  <li>{_html_escape_in_cdata_safe(x)}</li>" for x in cur_ul if x.strip())
            blocks.append("<ul>\n" + items + "\n</ul>")
            cur_ul = None

    for raw in lines:
        ln = raw.strip()
        # Hard split between logical paragraphs
        if not ln:
            flush_paragraph()
            flush_ul()
            continue

        # Bulleted item?
        m_b = _BULLET_RE.match(ln)
        if m_b:
            flush_paragraph()
            if cur_ul is None: cur_ul = []
            cur_ul.append(m_b.group(1).strip())
            continue

        # Header candidate (ends with ':')
        m_h = _HDR_CAND_RE.match(ln)
        if m_h:
            flush_paragraph()
            flush_ul()
            blocks.append(f"<h3>{_html_escape_in_cdata_safe(m_h.group(1).strip())}</h3>")
            continue

        # Lines that "look like" list items (e.g., 'Порт HDMI x1')
        if re.search(r"\b[x×]\s*\d+\b", ln, re.I) or re.search(r"^\s*(порт|кабель|адаптер|блок|камер|набор|вход|выход)\b", ln, re.I):
            flush_paragraph()
            if cur_ul is None: cur_ul = []
            cur_ul.append(ln)
            continue

        # Default: accumulate into paragraph buffer
        buf.append(ln)

    # Flush tails
    flush_paragraph()
    flush_ul()

    return "\n".join(blocks)

def prettify_raw_description_block(raw_desc_html_full: str) -> str:
    """
    If supplier description is mostly plain text (no tags), format it into tidy HTML.
    Otherwise, return as-is.
    """
    if not raw_desc_html_full:
        return ""
    if _looks_like_html(raw_desc_html_full):
        return raw_desc_html_full
    return _format_plain_text_to_html(raw_desc_html_full)

# ======================= COMPATIBILITY (расширено) =======================
def extract_compatibility_from_name_or_desc(name: str, desc_html: str) -> list[str]:
    # Заглушка: здесь могла быть логика для картриджей/принтеров
    return []

def build_compatibility_html(models: list[str]) -> str:
    if not models: return ""
    items = "\n".join(f"<li>{_html_escape_in_cdata_safe(m)}</li>" for m in models[:20])
    return f"<h3>Совместимость</h3>\n<ul>\n{items}\n</ul>"

# ======================= FAQ/REVIEWS =======================
DEFAULT_FAQ = [
    ("Подходит ли товар для повседневного использования?", "Да, рассчитан на ежедневные сценарии."),
    ("Есть ли гарантия?", "Да, гарантия указывается в карточке товара."),
]
DEFAULT_REVIEWS = [
    ("Даурен", "Актобе", 5, "Работает стабильно, всё как ожидал."),
    ("Инна", "Павлодар", 5, "Установка заняла пару минут, проблем не было."),
    ("Ерлан", "Атырау", 4, "Упаковка была слегка помята, но сам товар без нареканий."),
]

def build_faq_html(faq: list[tuple[str,str]] = None) -> str:
    faq = faq or DEFAULT_FAQ
    parts = ["<h3>FAQ</h3>"]
    for q,a in faq:
        parts.append(f"<p><strong>В:</strong> {_html_escape_in_cdata_safe(q)}<br><strong>О:</strong> {_html_escape_in_cdata_safe(a)}</p>")
    return "\n".join(parts)

def build_reviews_html(revs: list[tuple[str,str,int,str]] = None) -> str:
    revs = revs or DEFAULT_REVIEWS
    parts = ["<h3>Отзывы (3)</h3>"]
    for name, city, stars, text in revs[:3]:
        star_str = "&#11088;" * stars + ("&#9734;" * (5 - stars))
        parts.append(f"<p>&#128100; <strong>{_html_escape_in_cdata_safe(name)}</strong>, {_html_escape_in_cdata_safe(city)} — {star_str}<br>«{_html_escape_in_cdata_safe(text)}»</p>")
    return "\n".join(parts)

# ======================= ЛИД-БЛОК =======================
def build_lead_block(name: str, weight: str = "", volume: str = "", brand: str = "") -> str:
    h = f"<h3>{_html_escape_in_cdata_safe(name)}: Чем удобен{(' ('+brand+')') if brand else ''}</h3>"
    bullets = []
    if weight: bullets.append(f"&#9989; Вес: {weight}")
    if volume: bullets.append(f"&#9989; Объём: {volume}")
    if not bullets:
        bullets.append("Практичное решение для ежедневной работы.")
    ul = "\n".join(f"  <li>{_html_escape_in_cdata_safe(x)}</li>" for x in bullets)
    return f"{h}\n<p>Практичное решение для ежедневной работы.</p>\n<ul>\n{ul}\n</ul>"

# ======================= СПЕЦИФИКАЦИЯ ИЗ <param> =======================
def build_specs_from_params(off: Element) -> str:
    pairs = []
    for p in off.findall("./param"):
        name = p.attrib.get("name","").strip()
        val  = inner_text(p).strip()
        if not name or not val: 
            continue
        # Пропустить мусор, уже убран ранее
        pairs.append((name, val))
    if not pairs:
        return ""
    lis = "\n".join(f'  <li><strong>{_html_escape_in_cdata_safe(n)}:</strong> {_html_escape_in_cdata_safe(v)}</li>' for n,v in pairs[:50])
    return f"<h3>Характеристики</h3>\n<ul>\n{lis}\n</ul>"

# ======================= КОМПОЗИЦИЯ ОПИСАНИЯ =======================
def compose_full_description_html(lead_html: str, raw_desc_html_full: str, specs_html: str, faq_html: str, reviews_html: str) -> str:
    pieces=[]
    if lead_html: pieces.append(lead_html)
    # NEW: beautify supplier's plain text so it's Satu-friendly (paragraphs, lists, headers)
    if raw_desc_html_full:
        pretty = prettify_raw_description_block(raw_desc_html_full)
        pieces.append(_html_escape_in_cdata_safe(pretty) if _looks_like_html(pretty) else pretty)
    if specs_html: pieces.append(specs_html)
    if faq_html: pieces.append(faq_html)
    if reviews_html: pieces.append(reviews_html)
    return "\n".join(pieces)

# ======================= ДОСТУПНОСТЬ / ВАЛЮТА =======================
def compute_available(off: Element) -> bool:
    # Пытаемся понять по количеству/статусу; если нет, считаем true
    for tag in ("quantity_in_stock","stock_quantity","quantity","status"):
        n = off.find(f"./{tag}")
        if n is None: 
            continue
        t = inner_text(n).lower()
        # простая эвристика
        if tag.startswith("quantity") or tag.endswith("quantity"):
            try:
                q = float(t.replace(",", "."))
                if q > 0: 
                    return True
            except Exception:
                pass
        if tag == "status":
            if "нет" in t or "ожидается" in t:
                return False
            if "в наличии" in t or "есть" in t:
                return True
    return True

def set_offer_available_attr_and_currency(off: Element):
    off.attrib["available"] = "true" if compute_available(off) else "false"
    # currencyId
    cur = off.find("./currencyId")
    if cur is None:
        cur = SubElement(off, "currencyId")
    cur.text = CURRENCY_ID

# ======================= KEYWORDS =======================
GEO_CITIES = [
    "Казахстан", "Алматы", "Астана", "Шымкент", "Караганда", "Актобе", "Павлодар", "Атырау",
    "Тараз", "Оскемен", "Семей", "Костанай", "Кызылорда", "Орал", "Петропавловск",
    "Талдыкорган", "Актау", "Темиртау", "Экибастуз", "Кокшетау"
]

def build_keywords(off: Element) -> str:
    if KEYWORDS_MODE == "off":
        return ""
    name = inner_text(off.find("./name"))
    vendor = inner_text(off.find("./vendor"))
    vc = inner_text(off.find("./vendorCode"))
    models = re.findall(r"\b[A-Z]{1,5}\d[\w\-]{2,}\b", name, re.I)
    base = [vendor, vc] + models + name.split()
    base = [x.strip('",.()[]') for x in base if x]
    # добавим гео
    base += GEO_CITIES
    # нормализация и ограничение
    seen, out = set(), []
    for w in base:
        k = w.lower()
        if k in seen: 
            continue
        seen.add(k)
        out.append(w)
        if len(out) >= KEYWORDS_MAX_WORDS:
            break
    s = ", ".join(out)[:KEYWORDS_MAX_LEN]
    return s

# ======================= SEO КЭШ =======================
def load_seo_cache() -> dict:
    if not os.path.exists(SEO_CACHE_FILE):
        return {}
    try:
        return json.loads(read_text(SEO_CACHE_FILE))
    except Exception:
        return {}

def save_seo_cache(data: dict):
    ensure_dir_for_file(SEO_CACHE_FILE)
    with open(SEO_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def should_refresh_seo(last_ts: float | None) -> bool:
    if SEO_REFRESH_MODE == "always":
        return True
    if SEO_REFRESH_MODE == "never":
        return False
    # monthly: обновляем 1 числа месяца по Алматы
    if not last_ts:
        return True
    last = datetime.fromtimestamp(last_ts, tz=ALMATY_TZ)
    now = now_local()
    return (now.year, now.month) != (last.year, last.month)

# ======================= ИНЖЕКЦИЯ SEO ОПИСАНИЙ =======================
def inject_seo_descriptions(root: Element):
    cache = load_seo_cache()
    now_ts = now_local().timestamp()
    shop = root.find("./shop")
    offers = shop.find("./offers") if shop is not None else None
    if offers is None:
        return
    changed = False
    for off in offers.findall("./offer"):
        vc = inner_text(off.find("./vendorCode"))
        name = inner_text(off.find("./name"))
        vendor = inner_text(off.find("./vendor"))
        weight = ""
        volume = ""
        for p in off.findall("./param"):
            n = p.attrib.get("name","").strip().lower()
            v = inner_text(p).strip()
            if n == "вес" and v: weight = v
            if n == "объём" and v: volume = v

        lead_html = build_lead_block(name, weight, volume, vendor)

        # Родное описание из исходника (если присутствует как HTML)
        d = off.find("./description")
        raw_desc_html_full = inner_html(d) if d is not None else ""
        # beautify plain text
        raw_desc_html_full = prettify_raw_description_block(raw_desc_html_full)

        specs_html = build_specs_from_params(off)

        # Совместимость (если извлекается)
        compat = extract_compatibility_from_name_or_desc(name, raw_desc_html_full)
        compat_html = build_compatibility_html(compat)

        faq_html = build_faq_html()
        reviews_html = build_reviews_html()

        full_html = compose_full_description_html(lead_html, raw_desc_html_full, specs_html + compat_html, faq_html, reviews_html)

        # Кэш
        key = vc or md5(name)
        item = cache.get(key, {})
        last_ts = item.get("ts")
        if should_refresh_seo(last_ts) or item.get("html") != full_html:
            cache[key] = {"ts": now_ts, "html": full_html}
            changed = True

        # Перезаписываем <description> как CDATA
        desc = off.find("./description")
        if desc is None:
            desc = SubElement(off, "description")
        # Вкладываем CDATA
        for ch in list(desc):
            desc.remove(ch)
        desc.text = None
        xml_str = as_cdata(full_html)
        # В ElementTree нет прямого CDATA — оставим маркер, заменим на финальной записи
        desc.text = xml_str  # временно

    if changed:
        save_seo_cache(cache)

# ======================= ПОРЯДОК ДЕТЕЙ, CATEGORYID, ДР. =======================
ORDER = ["vendorCode","name","price","picture","vendor","currencyId","description","param","keywords"]

def reorder_offer_children_and_insert_category(off: Element):
    # Сначала categoryId=0 в самый верх
    cat = Element("categoryId"); cat.text = str(CATEGORY_ID_DEFAULT)
    # собрать остальные дети по порядку
    items = list(off)
    # убираем description с CDATA-маркером — он останется как есть
    off.clear()
    off.attrib["id"] = off.attrib.get("id","")
    off.attrib["available"] = off.attrib.get("available","true")
    off.append(cat)
    # добавляем по ORDER, затем остальные
    def key_for(n):
        t = n.tag
        try:
            return (0, ORDER.index(t))
        except ValueError:
            return (1, t)
    for n in sorted(items, key=key_for):
        off.append(n)

def ensure_keywords(off: Element):
    if KEYWORDS_MODE == "off":
        return
    kw = off.find("./keywords")
    if kw is None:
        kw = SubElement(off, "keywords")
    s = build_keywords(off)
    kw.text = s

# ======================= ЗАГОЛОВОК FEED_META =======================
def build_feed_meta(root: Element, offers_kept: int) -> str:
    now = now_local()
    next_dt = (now + timedelta(days=1)).replace(hour=1, minute=0, second=0, microsecond=0)
    shop = root.find("./shop")
    offers = shop.find("./offers") if shop is not None else None
    total = len(offers.findall("./offer")) if offers is not None else 0
    s = [
        "<!--",
        f"  FEED_META:",
        f"    supplier   = {SUPPLIER_NAME}",
        f"    source_url = {SUPPLIER_URL}",
        f"    built_at   = {now.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"    next_build = {next_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"    offers     = {total}",
        f"    kept       = {offers_kept}",
        f"    encoding   = {OUTPUT_ENCODING}",
        "-->"
    ]
    return "\n".join(s) + "\n"

# ======================= ФИНАЛЬНАЯ ЗАПИСЬ XML =======================
def serialize_xml(root: Element) -> str:
    # Обычный tostring и последующие правки (CDATA/description)
    xml = tostring(root, encoding="unicode")
    # Преобразуем description маркеры CDATA
    xml = xml.replace("&lt;![CDATA[", "<![CDATA[").replace("]]&gt;", "]]>")
    # Удалим лишние пустые строки
    xml = re.sub(r"\n{3,}", "\n\n", xml)
    # Вставим комментарий FEED_META над <shop>
    # Простая вставка: </yml_catalog><shop> → </yml_catalog>\n<!--meta-->\n<shop>
    meta = build_feed_meta(root, offers_kept= len(root.findall(".//offer")) )
    xml = re.sub(r"(<shop>)", meta + r"\1", xml, count=1)
    return xml

# ======================= ОСНОВНОЙ ПАЙПЛАЙН =======================
def build():
    # 1) Загрузка
    root = load_supplier_xml()

    # 2) Фильтр по категориям
    kept, dropped = filter_offers_by_categories(root)

    # 3) Удаление supplier categoryId и служебных тегов
    drop_supplier_category_ids_and_service_tags(root)

    # 4) Анти-аномалии цены
    apply_price_cap_guard(root)

    # 5) Нормализация офферов
    shop = root.find("./shop")
    offers = shop.find("./offers") if shop is not None else None
    if offers is None:
        raise RuntimeError("no <offers> in supplier feed")

    for off in offers.findall("./offer"):
        normalize_vendor_brand(off)
        ensure_vendorcode_and_id(off)
        apply_global_pricing(off)
        cleanup_params(off)
        ensure_pictures(off)
        set_offer_available_attr_and_currency(off)

    # 6) SEO-описания (lead + raw + specs + FAQ + reviews)
    inject_seo_descriptions(root)

    # 7) Порядок узлов и categoryId=0 наверх, keywords
    for off in offers.findall("./offer"):
        reorder_offer_children_and_insert_category(off)
        ensure_keywords(off)

    # 8) Финальная запись
    xml = serialize_xml(root)
    write_text(OUT_FILE, xml, enc=OUTPUT_ENCODING)

    # 9) .nojekyll
    ensure_dir_for_file("docs/.nojekyll")
    write_text("docs/.nojekyll", "", enc="utf-8")

# ======================= CLI =======================
if __name__ == "__main__":
    print(f"[{SUPPLIER_NAME}] build started at", now_local().strftime("%Y-%m-%d %H:%M:%S %Z"))
    try:
        build()
        print(f"[{SUPPLIER_NAME}] done -> {OUT_FILE} ({OUTPUT_ENCODING})")
    except Exception as e:
        print(f"[{SUPPLIER_NAME}] ERROR:", e)
        sys.exit(1)
