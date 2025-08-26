# -*- coding: utf-8 -*-
"""
Сборщик одной категории Copyline (исправленный от «левых» товаров):
- На странице категории берём плитки по img[src*="thumb_"]
- По каждой карточке заходим внутрь и ТОЛЬКО если:
    a) хлебные крошки содержат название категории
    b) и/или URL товара содержит бренд из названия категории (последнее слово), напр. "-brother-"
  — тогда принимаем товар; иначе пропускаем как «посторонний».
- Вытягиваем: name, vendorCode (артикул), price, url, picture и пишем в docs/copyline.yml

Подробные комментарии оставлены к каждому блоку.
"""

from __future__ import annotations
import os, io, time, hashlib, re, sys
from typing import List, Dict, Any, Optional, Tuple
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ------------------ Параметры запуска через ENV ------------------
BASE_URL         = os.getenv("BASE_URL", "https://copyline.kz").rstrip("/")
CATEGORY_URL     = os.getenv("CATEGORY_URL", "").strip()
OUT_FILE         = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC              = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "700"))
PAGE_TIMEOUT_S   = int(os.getenv("PAGE_TIMEOUT_S", "30"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) scraper/1.0"}

# ------------------ Вспомогательные утилиты ------------------
def sleep():
    """Деликатная задержка между сетевыми запросами, чтобы не спамить сайт."""
    time.sleep(max(REQUEST_DELAY_MS, 0) / 1000.0)

def get(url: str) -> bytes:
    """
    GET с 3 попытками и базовой проверкой размера ответа.
    Возвращает bytes содержимого или кидает исключение на 3-й фейл.
    """
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=PAGE_TIMEOUT_S)
            r.raise_for_status()
            if len(r.content) < MIN_BYTES:
                raise RuntimeError(f"too small response: {len(r.content)} bytes")
            return r.content
        except Exception:
            if attempt == 2:
                raise
            sleep()

def soup_from(url: str) -> BeautifulSoup:
    """Загружает HTML и парсит его в BeautifulSoup."""
    body = get(url)
    return BeautifulSoup(body, "html.parser")

def norm(s: Optional[str]) -> str:
    """Нормализует строки: трим + схлопывание внутренних пробелов."""
    return re.sub(r"\s+", " ", (s or "").strip())

def ensure_dir_for(path: str):
    """Гарантируем наличие папки под итоговый файл."""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

# ------------------ Извлечение товаров со страницы категории ------------------
def parse_category_products(cat_url: str) -> Tuple[List[Dict[str, str]], str]:
    """
    Ищем карточки товаров по уникальному признаку: img с 'thumb_' в пути.
    Важно: на странице есть боковые/нижние блоки с чужими товарами,
    поэтому сейчас это только предварительный список — окончательно фильтруем
    внутри карточек по breadcrumb/бренду.
    Возвращаем: список {url, name(draft), thumb} и название категории.
    """
    soup = soup_from(cat_url)

    # Заголовок категории (используем для breadcrumb-валидации и для YML)
    cat_h1 = soup.find("h1")
    category_name = norm(cat_h1.get_text()) if cat_h1 else "Copyline"

    # Находим изображения товарных плиток в сетке категории
    imgs = soup.find_all("img", src=re.compile(r"/components/com_jshopping/files/img_products/thumb_", re.I))
    products: Dict[str, Dict[str, str]] = {}

    for img in imgs:
        # Пробуем найти ссылку на товар рядом/сверху
        a = img.find_parent("a", href=True)
        if not a or "/goods/" not in a.get("href", ""):
            # Иногда ссылка наименования отдельная
            maybe = img.find_parent().find_next("a", href=True)
            if maybe and "/goods/" in maybe["href"]:
                a = maybe

        if not a:
            continue

        href = a["href"]
        if "/goods/" not in href:
            continue

        # Абсолютный URL товара
        url = href if href.startswith("http") else (BASE_URL + href if href.startswith("/") else BASE_URL + "/" + href)
        # Черновое имя — из alt
        draft_name = norm(img.get("alt") or "")
        thumb_url = img.get("src") or ""
        thumb_url = thumb_url if thumb_url.startswith("http") else (BASE_URL + thumb_url)

        products.setdefault(url, {"url": url, "name": draft_name, "thumb": thumb_url})

    items = list(products.values())
    print(f"[info] Найдено кандидатов на странице категории: {len(items)}")
    return items, category_name

# ------------------ Поиск артикула/цены/фото в карточке ------------------
SKU_PATTERNS = [
    # Типичные контейнеры артикула в JoomShopping
    ("css", {"class": re.compile(r"(jshop_code|prod_code|product-code|sku)", re.I)}),
    ("css", {"id": re.compile(r"(code|sku|vendor|artikul)", re.I)}),
]

# Регэкспы заранее компилируем и НЕ прокидываем потом flags повторно
RE_SKU = [
    re.compile(r"(?:Артикул|Код\s*товара|Модель)\s*[:#№]?\s*([A-Za-zА-Яа-я0-9\-._/]+)", re.I),
    re.compile(r"\bSKU\s*[:#№]?\s*([A-Za-z0-9\-._/]+)", re.I),
]
RE_PRICE = [
    re.compile(r"(\d[\d\s]{0,12}\d)\s*₸", re.I),
    re.compile(r"Цена\s*[:\s]\s*(\d[\d\s]{0,12}\d)", re.I),
]

def product_belongs_to_category(soup: BeautifulSoup, product_url: str, category_name: str) -> bool:
    """
    Жёсткая фильтрация «чужих» товаров:
    1) breadcrumb содержит точное текстовое вхождение названия категории;
    2) доп. предохранитель — бренд (последнее слово из названия категории) встречается в URL (например '-brother-').
    Хватает выполнения ЛИБО пункта 1, ЛИБО пункта 2.
    """
    ok_breadcrumb = False
    ok_brand = False

    # 1) Breadcrumb / pathway
    # Ищем несколько типичных контейнеров навигации
    crumb = soup.find("ul", class_=re.compile(r"breadcrumb|pathway", re.I)) \
         or soup.find("div", class_=re.compile(r"breadcrumb|pathway", re.I))
    if crumb:
        crumb_text = norm(crumb.get_text(" ", strip=True))
        if category_name and category_name in crumb_text:
            ok_breadcrumb = True

    # 2) Бренд из названия категории (последнее слово, например 'Brother' из 'Тонер-картриджи Brother')
    brand = norm(category_name).split(" ")[-1].lower() if category_name else ""
    if brand and f"-{brand}-" in product_url.lower():
        ok_brand = True

    return ok_breadcrumb or ok_brand

def parse_product(url: str, thumb_fallback: str, category_name: str) -> Optional[Dict[str, Any]]:
    """
    Заходит в карточку товара и:
    - отбрасывает «чужие» товары (см. product_belongs_to_category)
    - достаёт name, price, sku, main picture
    - если sku не найден — товар бракуется
    """
    soup = soup_from(url)

    # Фильтр принадлежности товару текущей категории
    if not product_belongs_to_category(soup, url, category_name):
        print(f"[skip] ВНЕ категории | {url}")
        return None

    # Название товара
    h1 = soup.find("h1")
    name = norm(h1.get_text()) if h1 else ""

    # Общий текст страницы для поиска цены/артикула
    text = soup.get_text(" ", strip=True)

    # Цена (если есть в явном виде)
    price_val: Optional[int] = None
    for rp in RE_PRICE:
        m = rp.search(text)
        if m:
            digits = re.sub(r"[^\d]", "", m.group(1))
            if digits:
                price_val = int(digits)
                break

    # Артикул (строгий поиск по блокам, затем по тексту)
    sku = None

    # 1) Поиск по «явным» css-узлам
    for kind, params in SKU_PATTERNS:
        if kind == "css":
            node = soup.find(attrs=params)
            if node:
                s = norm(node.get_text())
                # Внутри может быть «Артикул: XXX» — отловим регэкспом
                for rp in RE_SKU:
                    m = rp.search(s)
                    if m:
                        sku = norm(m.group(1))
                        break
                # Или сам текст узла = чистый артикул
                if not sku and s and re.fullmatch(r"[A-Za-zА-Яа-я0-9\-._/]+", s):
                    sku = s
        if sku:
            break

    # 2) Резервный поиск по всему тексту
    if not sku:
        for rp in RE_SKU:
            m = rp.search(text)
            if m:
                sku = norm(m.group(1))
                break

    if not sku:
        print(f"[skip] SKU не найден | {url}")
        return None

    # Главное фото: img#main_image_* или itemprop="image", иначе thumb_->full_
    img_main = soup.find("img", id=re.compile(r"^main_image_\d+$")) or soup.find("img", attrs={"itemprop": "image"})
    if img_main and img_main.get("src"):
        pic = img_main["src"]
        pic = pic if pic.startswith("http") else (BASE_URL + pic)
    else:
        pic = thumb_fallback.replace("/thumb_", "/full_")

    return {
        "name": name or "",
        "price": price_val,
        "sku": sku,
        "url": url,
        "picture": pic,
    }

# ------------------ Формирование YML ------------------
ROOT_CAT_ID = "9300000"

def hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)

def cat_id_for(name: str) -> str:
    return str(9300001 + (hash_int(name.lower()) % 400000))

def offer_id_from_url(url: str) -> str:
    """Стабильный offer id на базе URL (slug + короткий md5)."""
    slug = url.rstrip("/").rsplit("/", 1)[-1].replace(".html", "")
    h = hashlib.md5(url.encode("utf-8")).hexdigest()[:8]
    return f"copyline:{slug}:{h}"

def build_yml(items: List[Dict[str, Any]], category_name: str) -> bytes:
    """
    Компактный YML с основными полями под Satu:
    name, vendorCode, price, currencyId, categoryId, url, picture + простые складские признаки.
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
        o = SubElement(offers, "offer", {
            "id": offer_id_from_url(it["url"]),
            "available": "true",
            "in_stock": "true"
        })
        SubElement(o, "name").text = it["name"]
        if it.get("price") is not None:
            SubElement(o, "price").text = str(it["price"])
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cats.get(category_name, ROOT_CAT_ID)
        SubElement(o, "url").text = it["url"]
        SubElement(o, "picture").text = it["picture"]
        SubElement(o, "vendorCode").text = it["sku"]
        for tag in ("quantity_in_stock", "stock_quantity", "quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

# ------------------ Точка входа ------------------
def main():
    if not CATEGORY_URL:
        print("ERROR: CATEGORY_URL не задан", file=sys.stderr)
        sys.exit(1)

    ensure_dir_for(OUT_FILE)

    # 1) Собираем кандидатов со страницы категории
    candidates, cat_name = parse_category_products(CATEGORY_URL)

    # 2) Идём в карточки и применяем фильтры принадлежности к категории
    results: List[Dict[str, Any]] = []
    for i, p in enumerate(candidates, 1):
        url   = p["url"]
        thumb = p["thumb"]
        try:
            sleep()
            info = parse_product(url, thumb, cat_name)
            if info:
                results.append(info)
                print(f"[ok] {i:03d}/{len(candidates)} | SKU={info['sku']} | {url}")
            else:
                print(f"[skip] {i:03d}/{len(candidates)} | {url}")
        except Exception as e:
            print(f"[err] {i:03d}/{len(candidates)} | {url} | {e}")

    if not results:
        print("Error: нет валидных товаров (ничего не относится к категории) — выходим без записи.", file=sys.stderr)
        sys.exit(1)

    # 3) Записываем YML
    yml = build_yml(results, cat_name or "Copyline")
    with open(OUT_FILE, "wb") as f:
        f.write(yml)

    print(f"[done] {OUT_FILE}: items={len(results)} | category='{cat_name}'")

if __name__ == "__main__":
    main()
