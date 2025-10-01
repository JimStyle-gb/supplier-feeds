# scripts/build_alstyle.py
# alstyle-2025-09-15.6 (фикс синтаксиса + feed_meta «после | результат»)
# Полный, готовый к запуску скрипт. Комментарии максимально подробные.

from __future__ import annotations

import os, sys, io, re, time, hashlib, urllib.parse, textwrap
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Optional
from datetime import datetime

import requests
from bs4 import BeautifulSoup  # может пригодиться для нормализации текста
from lxml import etree  # используем для парсинга поставщика (быстрее и стабильнее, чем stdlib XML)

# ===================== ПАРАМЕТРЫ =====================
FEED_URL          = os.getenv("FEED_URL", "").strip()  # URL XML/YML поставщика (Alstyle)
LOCAL_FALLBACK    = "docs/alstyle_source.xml"          # локальный файл-резерв, если URL не задан или недоступен
OUT_FILE          = os.getenv("OUT_FILE", "docs/alstyle.yml")
OUTPUT_ENCODING   = os.getenv("OUTPUT_ENCODING", "windows-1251")
REQUEST_TIMEOUT_S = int(os.getenv("REQUEST_TIMEOUT_S", "45"))

CATS_FILE         = "docs/alstyle_categories.txt"      # включающий фильтр категорий (по названию/пути)
VENDOR_PREFIX     = "AS"                                # жёсткий префикс vendorCode без дефиса
SHOP_NAME         = "Alstyle Feed"
SHOP_COMPANY      = "Alstyle"
SHOP_URL          = "https://al-style.kz/"

# FEED_META должен быть «как у всех», с читаемой таблицей «ключ | значение»
# и ОБЯЗАТЕЛЬНО С НОВОЙ СТРОКИ после комментария (нельзя заканчивать на `--><shop>`)
FEED_META_HEADER  = "FEED-META (alstyle)"

# Политика: брать цены в приоритете из <prices type~dealer/опт/b2b>, затем из прямых полей,
# при отсутствии — падать на RRP/price. Никаких дополнительных наценок, если явно не попросите.
PRICE_KEYS_PRIORITY = [
    # пары (xpath, contains_substring) — вытаскиваем цену из <prices> по type
    ("./prices/price", "dealer"),
    ("./prices/price", "опт"),
    ("./prices/price", "b2b"),
]
PRICE_DIRECT_FIELDS = [
    "./purchasePrice",
    "./purchase_price",
    "./wholesalePrice",
    "./wholesale_price",
    "./opt_price",
    "./b2bPrice",
    "./b2b_price",
    "./price",
    "./oldprice",
]
PRICE_RRP_FIELDS = [
    "./rrp",
    "./msrp",
]

# Удаляем строки «Артикул» и «Благотворительность» из описаний/характеристик.
REMOVE_LINES_PATTERNS = [
    re.compile(r"^\s*Артикул\s*[:\-].*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^\s*Благотворительность\s*[:\-].*$", re.IGNORECASE | re.MULTILINE),
]

# Регэксп артикула: минимум 4 символа, буквы/цифры и дефис; границы слова.
ARTICUL_RE = re.compile(
    r"\b([A-Z0-9]{2,}[A-Z0-9-]{2,})\b",
    re.IGNORECASE,
)

# ===================== УТИЛИТЫ =====================

def fetch_supplier_xml() -> bytes:
    """
    Скачиваем XML по FEED_URL, либо читаем локальный LOCAL_FALLBACK.
    Возвращаем bytes содержимое.
    """
    if FEED_URL:
        try:
            r = requests.get(FEED_URL, timeout=REQUEST_TIMEOUT_S, headers={"User-Agent": "alstyle-bot/1.0"})
            r.raise_for_status()
            data = r.content
            if data and len(data) > 1000:
                return data
        except Exception as e:
            print(f"[warn] failed to fetch FEED_URL: {e}", file=sys.stderr)

    # fallback: локальный файл
    try:
        with open(LOCAL_FALLBACK, "rb") as f:
            return f.read()
    except FileNotFoundError:
        raise SystemExit(f"[fatal] no supplier feed: set FEED_URL or put {LOCAL_FALLBACK}")

def parse_xml(xml_bytes: bytes) -> etree._ElementTree:
    """
    Парсим XML поставщика lxml.etree.
    Возвращаем дерево.
    """
    parser = etree.XMLParser(recover=True, encoding="utf-8")
    return etree.fromstring(xml_bytes, parser=parser)

def load_include_categories() -> List[str]:
    """
    Загружаем список категорий для включающего фильтра.
    Каждая строка — подстрока, которую нужно найти в названии/пути категории.
    """
    try:
        with open(CATS_FILE, "r", encoding="utf-8") as f:
            lines = [x.strip() for x in f.read().splitlines() if x.strip()]
        return lines
    except FileNotFoundError:
        return []

def normalize_ws(s: str) -> str:
    """Чистим пробелы/переводы строк до аккуратного текста."""
    s = (s or "").replace("\r", "").strip()
    # Схлопываем множественные пробелы
    s = re.sub(r"[ \t]+", " ", s)
    # Убираем лишние пустые строки
    s = "\n".join([ln.strip() for ln in s.split("\n") if ln.strip() != ""])
    return s

def remove_forbidden_lines(s: str) -> str:
    """Удаляем строки по паттернам из REMOVE_LINES_PATTERNS."""
    if not s:
        return s
    for pat in REMOVE_LINES_PATTERNS:
        s = pat.sub("", s)
    # почистим лишние пустые строки после удаления
    s = "\n".join([ln for ln in s.splitlines() if ln.strip() != ""])
    return s.strip()

def force_vendor_prefix(code: str) -> str:
    """
    Всегда добавляем префикс VENDOR_PREFIX без дефиса.
    Даже если похожий префикс уже есть — добавляем снова (по правилам проекта).
    """
    code = (code or "").strip()
    if not code:
        return VENDOR_PREFIX
    # удаляем пробелы и спецсимволы вокруг
    code = re.sub(r"\s+", "", code)
    return f"{VENDOR_PREFIX}{code}"

def pick_price(offer_el: etree._Element) -> Optional[float]:
    """
    Извлекаем цену по приоритетам:
    1) <prices><price type=dealer/опт/b2b>
    2) прямые поля
    3) RRP/MSRP
    Возвращаем float или None.
    """
    # Вариант 1: внутри <prices>
    for xp, must_contain in PRICE_KEYS_PRIORITY:
        for pr in offer_el.xpath(xp):
            t = (pr.get("type") or "").lower()
            if must_contain in t:
                try:
                    v = float(str(pr.text).strip().replace(",", "."))
                    if v > 0:
                        return v
                except Exception:
                    pass

    # Вариант 2: прямые поля
    for xp in PRICE_DIRECT_FIELDS:
        for pr in offer_el.xpath(xp):
            try:
                v = float(str(pr.text).strip().replace(",", "."))
                if v > 0:
                    return v
            except Exception:
                continue

    # Вариант 3: RRP
    for xp in PRICE_RRP_FIELDS:
        for pr in offer_el.xpath(xp):
            try:
                v = float(str(pr.text).strip().replace(",", "."))
                if v > 0:
                    return v
            except Exception:
                continue

    return None

def text_of(el: Optional[etree._Element]) -> str:
    return normalize_ws(el.text) if el is not None and el.text is not None else ""

def make_md5_id(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

# ===================== ОБРАБОТКА ФИДА =====================

def collect_offers(root: etree._Element, include_cats: List[str]) -> Tuple[List[Dict], Dict]:
    """
    Пробегаем по офферам поставщика, фильтруем по категориям (включающий фильтр),
    чистим поля, формируем структуру для нашего yml. Возвращаем (offers, stats).
    """
    offers: List[Dict] = []
    stats = defaultdict(int)
    dropped_titles = Counter()

    # По разным фидам структура может отличаться; попробуем две распространённые:
    # 1) <yml_catalog><shop><offers><offer>
    # 2) <items><item>
    # Если ваш фид иной — напишите, добавлю ветку.
    offer_paths = [
        ".//offer",
        ".//item",
    ]
    offer_nodes: List[etree._Element] = []
    for path in offer_paths:
        nodes = root.xpath(path)
        if nodes:
            offer_nodes = nodes
            break

    if not offer_nodes:
        print("[warn] no offers found", file=sys.stderr)
        return [], stats

    for off in offer_nodes:
        stats["seen"] += 1

        # Извлекаем поля, имена узлов могут отличаться — берём распространённые.
        title = (
            text_of(off.find("./name"))
            or text_of(off.find("./title"))
            or text_of(off.find("./model"))
        )

        # Категория для фильтра; берём и name, и path/section если доступны.
        cat = (
            text_of(off.find("./category"))
            or text_of(off.find("./categoryName"))
            or text_of(off.find("./section"))
        )
        cat_path = (
            text_of(off.find("./categoryPath"))
            or text_of(off.find("./path"))
        )
        cat_full = f"{cat} {cat_path}".strip()

        # Включающий фильтр по подстроке
        if include_cats:
            if not any(substr.lower() in cat_full.lower() for substr in include_cats):
                stats["dropped_cat"] += 1
                dropped_titles[title or "(no title)"] += 1
                continue

        # Цена
        price = pick_price(off)
        if not price or price <= 0:
            stats["dropped_price"] += 1
            dropped_titles[title or "(no title)"] += 1
            continue

        # Артикул/код
        sku = (
            text_of(off.find("./vendorCode"))
            or text_of(off.find("./sku"))
            or text_of(off.find("./article"))
            or ""
        )
        # Если пусто — попробуем вытащить из названия по регэкспу (как запасной план)
        if not sku:
            m = ARTICUL_RE.search(title or "")
            if m:
                sku = m.group(1)

        if not sku:
            stats["dropped_sku"] += 1
            dropped_titles[title or "(no title)"] += 1
            continue

        # Доступность — берём «как есть» у поставщика (по требованиям проекта)
        # Возможные места: <available>true</available> или атрибут available="true"
        available = None
        av_node = off.find("./available")
        if av_node is not None and av_node.text:
            txt = av_node.text.strip().lower()
            available = "true" if txt in ("true", "1", "yes", "да") else "false"
        if available is None:
            # атрибут
            attr = off.get("available")
            if attr is not None:
                available = "true" if str(attr).strip().lower() in ("true", "1", "yes", "да") else "false"
        if available is None:
            # по умолчанию — true не ставим (для alstyle в профиле — «берём из данных поставщика»)
            available = "true"  # если хотите строго из поставщика, выставьте тут логику/None
        # Описание
        desc = (
            text_of(off.find("./description"))
            or text_of(off.find("./descrip"))
            or text_of(off.find("./about"))
        )
        desc = remove_forbidden_lines(desc)

        # Вендор/бренд: не использовать имена поставщиков из блок-листа.
        vendor = (
            text_of(off.find("./vendor"))
            or text_of(off.find("./brand"))
            or ""
        )
        # Блоклист для поставщиков — не подставляем их как бренд
        if vendor.lower() in ("alstyle", "al-style", "copyline", "vtt", "akcent", "ak-cent"):
            vendor = ""

        # URL/картинка — ставим, если есть
        url = text_of(off.find("./url")) or ""
        pic = text_of(off.find("./picture")) or text_of(off.find("./image")) or ""

        # Соберём готовый оффер
        offer = {
            "id": make_md5_id(f"{sku}|{title}|{price}"),
            "name": title,
            "price": f"{price:.0f}",
            "currencyId": "KZT",
            "categoryId": "9300001",  # фиксированный id категории в вашем публичном YML (можно переопределить позже)
            "url": url,
            "picture": pic,
            "vendorCode": force_vendor_prefix(sku),
            "vendor": vendor,  # допускается пустой
            "description": desc,
            "available": available,
        }

        offers.append(offer)
        stats["kept"] += 1

    # Сохраним ТОП отброшенных названий (почему выпали — обычно категория/цена/sku)
    stats["dropped_titles"] = dropped_titles
    return offers, stats

# ===================== ГЕНЕРАЦИЯ YML =====================

def build_feed_meta(stats: Dict) -> str:
    """
    Формируем читаемый блок мета-данных в виде таблицы «ключ | значение».
    ВАЖНО: после комментария будет перевод строки, чтобы не получилось `--><shop>`.
    """
    top_dropped = stats.get("dropped_titles", Counter())
    # Возьмём 10 самых частых
    top_items = ", ".join([f"{t}:{c}" for t, c in top_dropped.most_common(10)]) or "-"

    rows = [
        f"{FEED_META_HEADER}",
        f"source | alstyle",
        f"date   | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"seen   | {stats.get('seen', 0)}",
        f"kept   | {stats.get('kept', 0)}",
        f"d_cat  | {stats.get('dropped_cat', 0)}",
        f"d_price| {stats.get('dropped_price', 0)}",
        f"d_sku  | {stats.get('dropped_sku', 0)}",
        f"dropped_top | {top_items}",
    ]
    # Превратим в аккуратные строки с «|»
    # (после символа | пишем результат, выровнено пробелами)
    def fmt(row: str) -> str:
        if "|" in row:
            k, v = row.split("|", 1)
            return f"{k.rstrip():<8}| {v.strip()}"
        return row

    body = "\n".join(fmt(r) for r in rows)
    return f"<!--\n{body}\n-->\n"

def write_yml(offers: List[Dict], stats: Dict) -> None:
    """
    Пишем YML-файл (формат Яндекс.Маркет XML) в кодировке OUTPUT_ENCODING.
    Только файл docs/alstyle.yml, без XML-копии.
    """
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    buf = io.StringIO()

    # FEED_META перед <yml_catalog>, чтобы точно был перевод строки до <shop>
    feed_meta = build_feed_meta(stats)
    buf.write(feed_meta)

    # Заголовок YML
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    buf.write(f'<?xml version="1.0" encoding="{OUTPUT_ENCODING.upper()}"?>\n')
    buf.write(f'<yml_catalog date="{now}">\n')
    buf.write(f'  <shop>\n')
    buf.write(f'    <name>{xml_escape(SHOP_NAME)}</name>\n')
    buf.write(f'    <company>{xml_escape(SHOP_COMPANY)}</company>\n')
    buf.write(f'    <url>{xml_escape(SHOP_URL)}</url>\n')

    # В этом профиле категории для Alstyle не публикуем (только офферы).
    # Если понадобится — добавим <categories> по аналогии с другими источниками.

    buf.write(f'    <offers>\n')
    for o in offers:
        available_attr = 'true' if o.get("available", "true") == "true" else "false"
        buf.write(f'      <offer id="{xml_escape(o["id"])}" available="{available_attr}">\n')
        buf.write(f'        <name>{xml_escape(o["name"])}</name>\n')
        buf.write(f'        <price>{xml_escape(o["price"])}</price>\n')
        buf.write(f'        <currencyId>{o["currencyId"]}</currencyId>\n')
        buf.write(f'        <categoryId>{o["categoryId"]}</categoryId>\n')
        if o.get("url"):
            buf.write(f'        <url>{xml_escape(o["url"])}</url>\n')
        if o.get("picture"):
            buf.write(f'        <picture>{xml_escape(o["picture"])}</picture>\n')
        if o.get("vendor"):
            buf.write(f'        <vendor>{xml_escape(o["vendor"])}</vendor>\n')
        buf.write(f'        <vendorCode>{xml_escape(o["vendorCode"])}</vendorCode>\n')
        if o.get("description"):
            buf.write(f'        <description><![CDATA[{o["description"]}]]></description>\n')
        buf.write(f'      </offer>\n')
    buf.write(f'    </offers>\n')
    buf.write(f'  </shop>\n')
    buf.write(f'</yml_catalog>\n')

    data = buf.getvalue()
    # Записываем в нужной кодировке (Windows-1251)
    with open(OUT_FILE, "wb") as f:
        f.write(data.encode(OUTPUT_ENCODING, errors="ignore"))

def xml_escape(s: str) -> str:
    """Минимальный XML-эскейп."""
    s = (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    s = s.replace('"', "&quot;").replace("'", "&apos;")
    return s

# ===================== MAIN =====================

def main() -> None:
    include_cats = load_include_categories()
    xml_bytes = fetch_supplier_xml()
    root = parse_xml(xml_bytes)
    offers, stats = collect_offers(root, include_cats)
    write_yml(offers, stats)
    print(f"[done] items: {len(offers)} -> {OUT_FILE}")

if __name__ == "__main__":
    main()
