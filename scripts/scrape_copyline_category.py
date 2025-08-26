# scripts/scrape_copyline_category.py
# -*- coding: utf-8 -*-
"""
ГЛАВНЫЙ СКРИПТ СБОРА ТОВАРОВ ИЗ СПИСКА КАТЕГОРИЙ COPYLINE.KZ

Что делает:
  1) Берёт список категорий из docs/copyline_categories.txt
     (если файла нет — создаёт его с дефолтной категорией Brother).
  2) Для каждой категории:
     - скачивает страницу категории;
     - извлекает КАНДИДАТЫ-ссылки (/goods/*.html), как правило это карточки;
     - по каждой ссылке заходит ВНУТРЬ карточки товара и парсит:
         * Название товара
         * Артикул (строго: должен быть; иначе товар просто пропускаем)
         * Цена (берём из карточки; парсим число)
         * Картинка: переводим в "full_" (full_<basename>.jpg)
         * Описание (если есть)
     - определяет название категории (H1) и генерирует ей стабильный id
  3) Пишет единый YML в docs/copyline.yml в кодировке windows-1251.

Конфиг через env-переменные (необязательно):
  BASE_URL        — базовый хост (по умолчанию https://copyline.kz)
  CATS_FILE       — путь к списку категорий (по умолчанию docs/copyline_categories.txt)
  OUT_FILE        — путь к YML (по умолчанию docs/copyline.yml)
  REQUEST_DELAY_MS— задержка между HTTP (по умолчанию 700)
  TIMEOUT_S       — таймаут одного запроса (по умолчанию 30)
  MIN_BYTES       — минимальный размер ответа, чтобы считать страницу валидной (по умолчанию 1500)

Зависимости:
  requests, beautifulsoup4

Пример запуска:
  python scripts/scrape_copyline_category.py
"""

import os
import re
import time
import hashlib
import html
import pathlib
import random
from typing import List, Dict, Tuple, Optional
import requests
from bs4 import BeautifulSoup

# ------------------------------ Константы и дефолты ------------------------------

BASE_URL = os.environ.get("BASE_URL", "https://copyline.kz").rstrip("/")
CATS_FILE = os.environ.get("CATS_FILE", "docs/copyline_categories.txt")
OUT_FILE = os.environ.get("OUT_FILE", "docs/copyline.yml")

REQUEST_DELAY_MS = int(os.environ.get("REQUEST_DELAY_MS", "700"))
TIMEOUT_S = int(os.environ.get("TIMEOUT_S", "30"))
MIN_BYTES = int(os.environ.get("MIN_BYTES", "1500"))

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

# Корневая категория магазина в YML
ROOT_CAT_ID = 9300000
ROOT_CAT_NAME = "Copyline"

# ------------------------------ Вспомогательные функции ------------------------------

def ensure_dirs():
    """Создаёт папку docs при необходимости и дефолтный файл категорий, если его нет."""
    pathlib.Path("docs").mkdir(parents=True, exist_ok=True)
    if not os.path.exists(CATS_FILE):
        # Если файла со списком категорий нет — создаём с дефолтной категорией Brother
        with open(CATS_FILE, "w", encoding="utf-8") as f:
            f.write("# Список категорий Copyline.kz (по одной ссылке на строку)\n")
            f.write("# Строки, начинающиеся с #, игнорируются\n")
            f.write("https://copyline.kz/goods/toner-cartridges-brother.html\n")
        print(f"[init] создан {CATS_FILE} с дефолтной категорией")

def read_categories() -> List[str]:
    """Читает список категорий из файла, игнорируя пустые и закомментированные строки."""
    cats = []
    with open(CATS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            # Нормализуем относительные ссылки в абсолютные
            if s.startswith("/"):
                s = BASE_URL + s
            cats.append(s)
    if not cats:
        # На всякий случай подстрахуемся дефолтом, если файл пуст
        cats = [f"{BASE_URL}/goods/toner-cartridges-brother.html"]
    return cats

def sleep_jitter(ms: int):
    """Пауза между запросами с небольшим джиттером, чтобы быть похожими на человека."""
    base = ms / 1000.0
    jitter = random.uniform(-0.15, 0.15) * base
    time.sleep(max(0.0, base + jitter))

def http_get(url: str) -> Optional[bytes]:
    """
    Аккуратно скачивает страницу с таймаутом и базовыми заголовками.
    Возвращает bytes либо None при неудаче/маленьком ответе.
    """
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT_S)
        if resp.status_code != 200:
            print(f"[warn] GET {url} -> {resp.status_code}")
            return None
        content = resp.content
        if len(content) < MIN_BYTES:
            print(f"[warn] слишком мало данных ({len(content)} байт): {url}")
            return None
        return content
    except Exception as e:
        print(f"[err] GET {url} -> {e}")
        return None

def make_soup(html_bytes: bytes) -> BeautifulSoup:
    """
    Создаёт объект BeautifulSoup. Используем встроенный парсер 'html.parser',
    чтобы не требовать 'lxml' (уменьшаем риски на CI).
    """
    return BeautifulSoup(html_bytes, "html.parser")

def slugify(text: str) -> str:
    """Делает дружелюбный slug из названия для offer id."""
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]+", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "-", text, flags=re.UNICODE)
    text = re.sub(r"-{2,}", "-", text, flags=re.UNICODE)
    return text.strip("-")[:80] or "item"

def stable_id(seed: str, prefix: int = 9400000) -> int:
    """
    Стабильный числовой id из строки (для категорий).
    Берём md5, первые 6 hex-символов -> int, плюс смещение prefix.
    """
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def normalize_img_to_full(url: str) -> str:
    """
    Переводит ссылку на изображение к формату full_<basename>.*
    Логика:
      - если уже есть 'full_' — оставляем как есть;
      - если есть 'thumb_' — заменяем на 'full_';
      - если просто basename — добавляем 'full_' перед именем файла;
      - если относительная ссылка — превращаем в абсолютную.
    """
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = BASE_URL + url

    try:
        m = re.match(r"^(https?://[^/]+)(/.*/)([^/]+)$", url)
        if not m:
            return url
        host, path, fname = m.groups()
        if fname.startswith("full_"):
            return url
        if fname.startswith("thumb_"):
            fname = "full_" + fname[len("thumb_"):]
        else:
            fname = "full_" + fname
        return f"{host}{path}{fname}"
    except Exception:
        return url

def extract_price_text(soup: BeautifulSoup) -> Optional[str]:
    """
    Пытаемся достать цену в текстовом виде с карточки.
    Ищем элементы с классами, содержащими 'price', или знак '₸', 'тг', 'тенге'.
    Возвращаем строку с цифрами/разделителями, либо None.
    """
    # 1) Популярные классы
    for css in ["jshop_price", "price", "prod_price", "product_price"]:
        el = soup.find(True, class_=lambda c: c and css in c)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)

    # 2) Любой текст с символом валюты/названием
    texts = soup.find_all(text=True)
    for t in texts:
        s = t.strip()
        if not s:
            continue
        if "₸" in s or "тг" in s.lower() or "тенге" in s.lower() or "kzt" in s.lower():
            return s
    return None

def parse_price_to_number(price_text: Optional[str]) -> Optional[float]:
    """Из строки с ценой вынимает число (float) или возвращает None."""
    if not price_text:
        return None
    # Заменяем запятые на точки, убираем пробелы неразрывные, оставляем цифры и точку/запятую
    s = price_text.replace("\xa0", " ").strip()
    # Выцепляем первое похожее на цену число
    m = re.search(r"(\d[\d\s.,]*)", s)
    if not m:
        return None
    num = m.group(1).replace(" ", "").replace(",", ".")
    try:
        return float(num)
    except Exception:
        return None

def clean_text(s: str) -> str:
    """Подчистка текстов: схлопываем пробелы, убираем HTML-артефакты."""
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    return s

def find_sku_on_product_page(soup: BeautifulSoup) -> Optional[str]:
    """
    Ищем Артикул в карточке.
    Стратегии (по порядку):
      1) Текстовые метки 'Артикул' / 'SKU' / 'Код товара' рядом со значением.
      2) Элементы с itemprop='sku'.
      3) Прямое совпадение шаблона 'Артикул: XXXXX' в тексте страницы.
    """
    # 1) По соседству с лейблами
    labels = ["артикул", "sku", "код товара", "код товара:", "код:"]
    for lab in labels:
        lab_el = soup.find(string=lambda t: t and lab in t.lower())
        if lab_el:
            # Пытаемся вытащить значение в том же контейнере/рядом
            parent = lab_el.parent
            # варианты: <td>Артикул</td><td>12345</td> или <span>Артикул:</span> 12345
            # попробуем ближайший next_sibling с текстом
            sibs = []
            if parent:
                sibs.extend(parent.find_all_next(string=True, limit=3))
            else:
                sibs.extend([lab_el.next_sibling])
            for s in sibs:
                if not s:
                    continue
                val = clean_text(str(s))
                # пропускаем повтор метки
                if any(x in val.lower() for x in labels):
                    continue
                # берём первые 32 алфанумерика/дефиса
                m = re.search(r"([A-Za-z0-9\-]{2,})", val)
                if m:
                    return m.group(1)

    # 2) itemprop=sku
    sku_el = soup.find(attrs={"itemprop": "sku"})
    if sku_el:
        val = clean_text(sku_el.get_text(" ", strip=True))
        if val:
            return val

    # 3) Грубый шаблон 'Артикул: X...'
    m = re.search(r"Артикул\W+([A-Za-z0-9\-]{2,})", soup.get_text(" ", strip=True), flags=re.IGNORECASE)
    if m:
        return m.group(1)

    return None

def find_name_on_product_page(soup: BeautifulSoup) -> Optional[str]:
    """Пытаемся достать название товара (обычно в H1)."""
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return clean_text(h1.get_text(" ", strip=True))
    # fallback — title
    if soup.title and soup.title.get_text(strip=True):
        return clean_text(soup.title.get_text(" ", strip=True))
    return None

def find_desc_on_product_page(soup: BeautifulSoup) -> Optional[str]:
    """
    Пытаемся вытащить описание: блоки с классами, содержащими 'description'.
    Берём короткий чистый текст.
    """
    for css in ["jshop_prod_description", "description", "product_description", "prod_description"]:
        el = soup.find(True, class_=lambda c: c and css in c)
        if el:
            txt = clean_text(el.get_text(" ", strip=True))
            if txt:
                return txt[:2000]  # ограничим разумно
    # fallback: ничего
    return None

def find_picture_on_product_page(soup: BeautifulSoup) -> Optional[str]:
    """
    Ищем основное изображение:
      - <img id="main_image_..."> или itemprop="image"
      - либо <img> в блоке карточки
    Приводим к 'full_' ссылке.
    """
    # По конкретным признакам
    img = soup.find("img", attrs={"id": re.compile(r"^main_image_")})
    if not img:
        img = soup.find("img", attrs={"itemprop": "image"})
    if not img:
        # Любое крупное изображение в карточке товара
        candidates = soup.find_all("img")
        if candidates:
            # Выберем первый, у кого src указывает на папку img_products
            for c in candidates:
                src = c.get("src") or c.get("data-src") or ""
                if "img_products" in src:
                    img = c
                    break
            if not img:
                img = candidates[0]

    if not img:
        return None

    src = img.get("src") or img.get("data-src") or ""
    if not src:
        return None
    return normalize_img_to_full(src)

def collect_product_links_from_category(cat_html: bytes) -> List[str]:
    """
    Извлекаем кандидаты-ссылки на товары из HTML категории.
    Стратегия:
      - берём ВСЕ <a> с href вида /goods/*.html или https://copyline.kz/goods/*.html
      - нормализуем в абсолютные
      - де-дублим
    Фильтрация по типу страницы происходит уже ПОСЛЕ — на уровне карточки (там ищем Артикул).
    Это гарантирует, что лишние меню/категории будут отброшены автоматически.
    """
    soup = make_soup(cat_html)
    anchors = soup.find_all("a", href=True)
    urls = []
    for a in anchors:
        href = a["href"].strip()
        # нормализуем
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = BASE_URL + href
        # отбор только /goods/*.html
        if re.search(r"https?://[^/]+/goods/[^?#]+\.html$", href):
            urls.append(href)

    # де-дубль
    uniq = list(dict.fromkeys(urls))
    return uniq

def category_title(cat_html: bytes) -> str:
    """Название категории (обычно H1). Если не нашли — берём <title>."""
    soup = make_soup(cat_html)
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return clean_text(h1.get_text(" ", strip=True))
    if soup.title and soup.title.get_text(strip=True):
        return clean_text(soup.title.get_text(" ", strip=True))
    return "Категория"

# ------------------------------ Главная логика ------------------------------

def crawl_product(url: str) -> Optional[Dict]:
    """
    Полный разбор карточки товара.
    Возвращает словарь данных или None, если это не товар (нет артикула/названия/цены).
    """
    sleep_jitter(REQUEST_DELAY_MS)
    html_bytes = http_get(url)
    if not html_bytes:
        return None
    soup = make_soup(html_bytes)

    sku = find_sku_on_product_page(soup)
    if not sku:
        # Строго: без артикула не считаем товаром
        return None

    name = find_name_on_product_page(soup) or ""
    name = clean_text(name)
    if not name:
        return None

    price_text = extract_price_text(soup)
    price = parse_price_to_number(price_text)
    if not price or price <= 0:
        # На Сату нужна >0, если не нашли — отбрасываем
        return None

    picture = find_picture_on_product_page(soup)
    desc = find_desc_on_product_page(soup)

    return {
        "url": url,
        "name": name,
        "price": max(0.01, round(price, 2)),
        "vendorCode": sku,
        "picture": picture,
        "description": desc or "",
    }

def build_yml(categories: List[Tuple[int, str]], offers: List[Tuple[int, Dict]]) -> str:
    """
    Собирает финальный YML как строку.
      categories: список (categoryId, categoryName)
      offers:     список (categoryId, offer_data)
    """
    # Заголовок
    out = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>copyline</name>")
    out.append("<currencies><currency id=\"KZT\" rate=\"1\" /></currencies>")

    # Категории
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{html.escape(ROOT_CAT_NAME)}</category>")
    for cid, cname in categories:
        cname_xml = html.escape(cname)
        out.append(f"<category id=\"{cid}\" parentId=\"{ROOT_CAT_ID}\">{cname_xml}</category>")
    out.append("</categories>")

    # Офферы
    out.append("<offers>")
    for cid, o in offers:
        # Генерируем стабильный offer id
        raw_id = f"copyline:{slugify(o['name'])}:{hashlib.md5((o['url']+o['vendorCode']).encode('utf-8')).hexdigest()[:8]}"
        name_xml = html.escape(o["name"])
        url_xml = html.escape(o["url"])
        pic_xml = html.escape(o["picture"] or "")
        sku_xml = html.escape(o["vendorCode"])
        desc_xml = html.escape(o.get("description", ""))

        out.append(f"<offer id=\"{raw_id}\" available=\"true\" in_stock=\"true\">")
        out.append(f"<name>{name_xml}</name>")
        out.append(f"<price>{int(o['price']) if o['price'].is_integer() else o['price']}</price>")
        out.append(f"<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        out.append(f"<url>{url_xml}</url>")
        if pic_xml:
            out.append(f"<picture>{pic_xml}</picture>")
        out.append(f"<vendorCode>{sku_xml}</vendorCode>")
        if desc_xml:
            out.append(f"<description>{desc_xml}</description>")
        # минимальные складские поля
        out.append(f"<quantity_in_stock>1</quantity_in_stock>")
        out.append(f"<stock_quantity>1</stock_quantity>")
        out.append(f"<quantity>1</quantity>")
        out.append("</offer>")
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    # Склеиваем
    xml_text = "\n".join(out)
    return xml_text

def main():
    ensure_dirs()
    cats = read_categories()
    print(f"[info] категорий в списке: {len(cats)}")

    all_categories: List[Tuple[int, str]] = []
    all_offers: List[Tuple[int, Dict]] = []

    for idx, cat_url in enumerate(cats, 1):
        print(f"[cat] {idx:03d}/{len(cats)} -> {cat_url}")
        sleep_jitter(REQUEST_DELAY_MS)

        cat_html = http_get(cat_url)
        if not cat_html:
            print(f"[warn] пропускаю категорию (нет HTML): {cat_url}")
            continue

        cat_name = category_title(cat_html)
        cat_id = stable_id(cat_name)
        all_categories.append((cat_id, cat_name))

        # собираем кандидаты и затем фильтруем по факту наличия артикула
        candidates = collect_product_links_from_category(cat_html)
        print(f"[cat] найдено кандидат-ссылок: {len(candidates)}")

        added = 0
        skipped = 0
        for i, purl in enumerate(candidates, 1):
            data = crawl_product(purl)
            if data:
                all_offers.append((cat_id, data))
                added += 1
                print(f"[ok] {i:03d}/{len(candidates)} | SKU={data['vendorCode']} | {purl}")
            else:
                skipped += 1
                print(f"[skip] {i:03d}/{len(candidates)} | не товар/нет данных | {purl}")

        print(f"[cat] итог: добавлено {added}, пропущено {skipped}")

    if not all_offers:
        print("[error] не удалось собрать ни одной карточки (нет офферов).")
        # даже в этом случае создадим формальный пустой YML, чтобы было что смотреть
    xml_text = build_yml(all_categories, all_offers)

    # Пишем в windows-1251 (как просил)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml_text)

    print(f"[done] записано: {OUT_FILE}")
    print(f"[stat] категорий: {len(all_categories)}, офферов: {len(all_offers)}")

if __name__ == "__main__":
    main()
