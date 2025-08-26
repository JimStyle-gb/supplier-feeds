# -*- coding: utf-8 -*-
"""
Простой и устойчивый сборщик одной категории Copyline:
- На странице категории ищем именно товарные карточки по img с src содержащим '/img_products/thumb_'
- По каждой карточке переходим в товар: тянем артикул, цену, имя, фото, URL
- Пишем YML в docs/copyline.yml
Все ключевые места подробно прокомментированы.
"""

from __future__ import annotations
import os, io, time, hashlib, re, sys
from typing import List, Dict, Any, Optional, Tuple
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ------------------ Настройки через ENV ------------------
BASE_URL         = os.getenv("BASE_URL", "https://copyline.kz").rstrip("/")
CATEGORY_URL     = os.getenv("CATEGORY_URL", "").strip()
OUT_FILE         = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC              = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "700"))
PAGE_TIMEOUT_S   = int(os.getenv("PAGE_TIMEOUT_S", "30"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) scraper/1.0"}

# ------------------ Утилиты ------------------
def sleep():
    """Деликатная задержка между запросами."""
    time.sleep(max(REQUEST_DELAY_MS, 0) / 1000.0)

def get(url: str) -> bytes:
    """
    GET с простыми повторами.
    Возвращает bytes тела или кидает исключение.
    """
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=PAGE_TIMEOUT_S)
            r.raise_for_status()
            if len(r.content) < MIN_BYTES:
                raise RuntimeError(f"too small response: {len(r.content)} bytes")
            return r.content
        except Exception as e:
            if attempt == 2:
                raise
            sleep()

def soup_from(url: str) -> BeautifulSoup:
    """Скачиваем страницу и отдаём BeautifulSoup."""
    body = get(url)
    return BeautifulSoup(body, "html.parser")

def norm(s: Optional[str]) -> str:
    """Трим + схлопывание пробелов."""
    return re.sub(r"\s+", " ", (s or "").strip())

def ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

# ------------------ Извлечение товаров из категории ------------------
def parse_category_products(cat_url: str) -> List[Dict[str, str]]:
    """
    На странице категории маркируем товарные плитки через img с 'thumb_' в src.
    Это резко сужает область и исключает ссылки из меню/футера/брендов.
    Возвращаем список уникальных товаров: url, name (черновик), thumb.
    """
    soup = soup_from(cat_url)

    # Заголовок категории (для YML-каталога)
    cat_name = soup.find("h1")
    category_name = norm(cat_name.get_text()) if cat_name else "Copyline"

    # Находим все изображения товаров в сетке по 'thumb_'
    imgs = soup.find_all("img", src=re.compile(r"/components/com_jshopping/files/img_products/thumb_", re.I))
    products: Dict[str, Dict[str, str]] = {}

    for img in imgs:
        # Ищем ближайшую ссылку на товар в пределах карточки
        a = img.find_parent("a", href=True)
        href = a["href"] if a else None
        if not href or "/goods/" not in href:
            # Иногда ссылка на товар не родительская, а в блоке имени — ищем рядом
            name_div = img.find_parent().find_next("a", href=True)
            if name_div and "/goods/" in name_div["href"]:
                href = name_div["href"]

        if not href or "/goods/" not in href:
            continue

        # Делаем абсолютный URL
        url = href if href.startswith("http") else (BASE_URL + href if href.startswith("/") else BASE_URL + "/" + href)
        # Черновое имя — из alt, часто нормальное
        draft_name = norm(img.get("alt") or "")
        thumb_url = img.get("src") or ""
        thumb_url = thumb_url if thumb_url.startswith("http") else (BASE_URL + thumb_url)

        # Уникализируем по URL товара
        products.setdefault(url, {"url": url, "name": draft_name, "thumb": thumb_url})

    items = list(products.values())
    print(f"[info] Товаров найдено на странице категории: {len(items)}")
    return items, category_name

# ------------------ Извлечение данных из карточки товара ------------------
SKU_PATTERNS = [
    # Явные узлы с кодом
    ("css", {"class": re.compile(r"(jshop_code|prod_code|product-code|sku)", re.I)}),
    ("css", {"id": re.compile(r"(code|sku|vendor|artikul)", re.I)}),
]

# Компилируем текстовые регэкспы (важно: НИГДЕ потом не передавать flags=... ещё раз!)
RE_SKU = [
    re.compile(r"(?:Артикул|Код\s*товара|Модель)\s*[:#№]?\s*([A-Za-zА-Яа-я0-9\-._/]+)", re.I),
    re.compile(r"\bSKU\s*[:#№]?\s*([A-Za-z0-9\-._/]+)", re.I),
]
RE_PRICE = [
    re.compile(r"(\d[\d\s]{0,12}\d)\s*₸", re.I),
    re.compile(r"Цена\s*[:\s]\s*(\d[\d\s]{0,12}\d)", re.I),
]

def parse_product(url: str, thumb_fallback: str) -> Optional[Dict[str, Any]]:
    """
    Заходим в карточку:
    - name: h1
    - price: по шаблонам (число + '₸' или 'Цена: ...')
    - sku: строго через блоки и/или через текстовые регэкспы
    - picture: главное фото (img#main_image_*), иначе thumb->full
    Возвращаем dict или None (если нет артикула).
    """
    soup = soup_from(url)

    # Название
    h1 = soup.find("h1")
    name = norm(h1.get_text()) if h1 else ""

    # Цена
    text = soup.get_text(" ", strip=True)
    price_val: Optional[int] = None
    for rp in RE_PRICE:
        m = rp.search(text)
        if m:
            digits = re.sub(r"[^\d]", "", m.group(1))
            if digits:
                price_val = int(digits)
                break

    # Артикул (строго)
    sku = None

    # 1) Сначала пробуем поискать по явным CSS-узлам
    for kind, params in SKU_PATTERNS:
        if kind == "css":
            node = soup.find(attrs=params)
            if node:
                s = norm(node.get_text())
                # Внутри узла тоже мог быть "Артикул: XXX" — дёрнем теми же регэкспами
                for rp in RE_SKU:
                    m = rp.search(s)
                    if m:
                        sku = norm(m.group(1))
                        break
                # Или сам узел — чистое значение
                if not sku and s:
                    # Если там просто "105140"
                    if re.fullmatch(r"[A-Za-zА-Яа-я0-9\-._/]+", s):
                        sku = s
            if sku:
                break

    # 2) Если не нашли — общий текстовой поиск
    if not sku:
        for rp in RE_SKU:
            m = rp.search(text)
            if m:
                sku = norm(m.group(1))
                break

    if not sku:
        print(f"[skip] SKU не найден (строго) | {url}")
        return None

    # Фото: пробуем взять главное из карточки, иначе thumb->full
    img_main = soup.find("img", id=re.compile(r"^main_image_\d+$")) or soup.find("img", attrs={"itemprop": "image"})
    if img_main and img_main.get("src"):
        pic = img_main["src"]
        pic = pic if pic.startswith("http") else (BASE_URL + pic)
    else:
        # Фоллбек: заменяем thumb_ → full_
        pic = thumb_fallback.replace("/thumb_", "/full_")

    return {
        "name": name or "",      # Название всегда ставим
        "price": price_val,      # Цена может быть None
        "sku": sku,              # Обязателен — иначе None и товар выкидывается
        "url": url,
        "picture": pic,
    }

# ------------------ Сборка YML ------------------
ROOT_CAT_ID = "9300000"

def hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)

def cat_id_for(name: str) -> str:
    return str(9300001 + (hash_int(name.lower()) % 400000))

def offer_id_from_url(url: str) -> str:
    """
    Стабильный id: 'copyline:' + слаг из URL + короткий хеш.
    """
    slug = url.rstrip("/").rsplit("/", 1)[-1].replace(".html", "")
    h = hashlib.md5(url.encode("utf-8")).hexdigest()[:8]
    return f"copyline:{slug}:{h}"

def build_yml(items: List[Dict[str, Any]], category_name: str) -> bytes:
    """
    Формируем компактный YML:
    - name
    - vendorCode (sku)
    - price (если есть)
    - currencyId=KZT
    - categoryId (для категории)
    - url
    - picture
    """
    cats: Dict[str, str] = {}
    if category_name.strip():
        cats[category_name] = cat_id_for(category_name)

    root = Element("yml_catalog")
    shop = SubElement(root, "shop")
    SubElement(shop, "name").text = "copyline"
    curr = SubElement(shop, "currencies")
    SubElement(curr, "currency", {"id": "KZT", "rate": "1"})

    xml_cats = SubElement(shop, "categories")
    SubElement(xml_cats, "category", {"id": ROOT_CAT_ID}).text = "Copyline"
    for nm, cid in cats.items():
        SubElement(xml_cats, "category", {"id": cid, "parentId": ROOT_CAT_ID}).text = nm

    offers = SubElement(shop, "offers")
    for it in items:
        oid = offer_id_from_url(it["url"])
        o = SubElement(offers, "offer", {"id": oid, "available": "true", "in_stock": "true"})
        SubElement(o, "name").text = it["name"]
        if it.get("price") is not None:
            SubElement(o, "price").text = str(it["price"])
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cats.get(category_name, ROOT_CAT_ID)
        SubElement(o, "url").text = it["url"]
        SubElement(o, "picture").text = it["picture"]
        SubElement(o, "vendorCode").text = it["sku"]
        # Мин. набор складских флагов
        for tag in ("quantity_in_stock", "stock_quantity", "quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

# ------------------ MAIN ------------------
def main():
    if not CATEGORY_URL:
        print("ERROR: CATEGORY_URL не задан", file=sys.stderr)
        sys.exit(1)

    ensure_dir_for(OUT_FILE)

    # 1) Собираем товары из одной категории
    products, cat_name = parse_category_products(CATEGORY_URL)

    # 2) По каждому товару — идём в карточку и тянем поля
    results: List[Dict[str, Any]] = []
    for i, p in enumerate(products, 1):
        url   = p["url"]
        thumb = p["thumb"]
        try:
            sleep()
            info = parse_product(url, thumb)
            if info:
                results.append(info)
                print(f"[ok] {i:03d}/{len(products)} | SKU={info['sku']} | {url}")
            else:
                print(f"[skip] {i:03d}/{len(products)} | SKU=- | {url}")
        except Exception as e:
            print(f"[err] {i:03d}/{len(products)} | {url} | {e}")

    # 3) Если ничего не собрали — не затираем файл и выходим с ошибкой
    if not results:
        print("Error: нет валидных товаров (без SKU) — выходим без записи.", file=sys.stderr)
        sys.exit(1)

    # 4) Пишем YML
    yml = build_yml(results, cat_name or "Copyline")
    with open(OUT_FILE, "wb") as f:
        f.write(yml)

    print(f"[done] {OUT_FILE}: items={len(results)} | category='{cat_name}'")

if __name__ == "__main__":
    main()
