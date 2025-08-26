#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ЗАДАЧА
------
Собрать товары с категории Copyline (и со всех её страниц пагинации),
извлечь ссылки на миниатюры (thumb_*.jpg), преобразовать их в основные картинки
(full_*.jpg) и сформировать YML-файл в формате Yandex Market.

ПОЧЕМУ ТАК
----------
1) На страницах категории у Copyline превьюшки лежат как
   /components/com_jshopping/files/img_products/thumb_XXXX.jpg
   При этом у основной (большой) картинки то же имя, но с префиксом full_.
   => достаточно заменить 'thumb_' на 'full_'.

2) Если вдруг у конкретного товара на листинге стоит "пустая"/общая картинка,
   пытаемся добрать основную картинку из карточки товара (<img itemprop="image"...>).

3) Скрипт берёт параметры из переменных окружения, чтобы им было удобно
   управлять в GitHub Actions и локально.

ОСНОВНЫЕ ПАРАМЕТРЫ (env)
------------------------
CATEGORY_URL      — URL категории для парсинга (обязателен)
OUT_FILE          — путь к итоговому YML (по умолчанию: docs/copyline.yml)
OUTPUT_ENCODING   — кодировка YML (по умолчанию: windows-1251)
REQUEST_DELAY_MS  — пауза между запросами (мс), чтобы не злить сайт (по умолчанию: 600)
PAGE_TIMEOUT_S    — таймаут запроса страницы (сек) (по умолчанию: 30)
MAX_PAGES         — ограничение на кол-во страниц пагинации (по умолчанию: 50)
MIN_BYTES         — если HEAD/GET у full_*.jpg меньше этого порога — считаем картинку плохой (по умолчанию: 2500)

ВЫХОД
-----
docs/copyline.yml — YML со списком offers {name, url, picture, categoryId=Copyline:Brother}
"""

import os
import re
import io
import time
import hashlib
import html
import requests
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, ElementTree

# -------------------- Настройки по умолчанию --------------------
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36"
}

CATEGORY_URL      = os.getenv("CATEGORY_URL", "").strip()
OUT_FILE          = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC               = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
REQUEST_DELAY_MS  = int(os.getenv("REQUEST_DELAY_MS", "600"))
PAGE_TIMEOUT_S    = int(os.getenv("PAGE_TIMEOUT_S", "30"))
MAX_PAGES         = int(os.getenv("MAX_PAGES", "50"))
MIN_BYTES         = int(os.getenv("MIN_BYTES", "2500"))

# -------------------- Утилиты --------------------
def ensure_dir_for(path: str) -> None:
    """Создать директорию под файл, если её ещё нет."""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def norm(s: Optional[str]) -> str:
    """Обрезать пробелы и схлопнуть повторяющиеся."""
    return re.sub(r"\s+", " ", (s or "").strip())

def fetch(url: str) -> requests.Response:
    """HTTP GET с заголовком UA и таймаутом."""
    r = requests.get(url, headers=UA_HEADERS, timeout=PAGE_TIMEOUT_S)
    r.raise_for_status()
    return r

def head_ok(url: str, min_bytes: int) -> bool:
    """
    Быстрая проверка доступности/вменяемости картинки:
    пытаемся HEAD, если не получилось — пробуем GET с потоковой загрузкой
    и проверяем Content-Length или количество байт.
    """
    try:
        h = requests.head(url, headers=UA_HEADERS, timeout=PAGE_TIMEOUT_S, allow_redirects=True)
        if "content-length" in h.headers:
            try:
                sz = int(h.headers["content-length"])
                return sz >= min_bytes
            except Exception:
                pass
        # если длины нет — проверим маленьким GET
        g = requests.get(url, headers=UA_HEADERS, timeout=PAGE_TIMEOUT_S, stream=True)
        total = 0
        for chunk in g.iter_content(8192):
            total += len(chunk)
            if total >= min_bytes:
                return True
        return False
    except Exception:
        return False

def to_full_url(thumb_url: str) -> str:
    """Заменить 'thumb_' на 'full_' только в имени файла (последнем сегменте URL)."""
    # пример: .../img_products/thumb_TN-1075.jpg -> .../img_products/full_TN-1075.jpg
    return re.sub(r"/thumb_", "/full_", thumb_url)

def same_host(base: str, href: str) -> str:
    """Собрать абсолютный URL с учётом базового."""
    return urljoin(base, href)

def extract_category_title(soup: BeautifulSoup) -> str:
    """
    Достаём заголовок категории (обычно <h1>).
    Если не нашли — вернём 'Copyline: Category'.
    """
    h1 = soup.find("h1")
    if h1:
        return norm(h1.get_text())
    return "Copyline: Category"

# -------------------- Парсинг листинга --------------------
def parse_list_page(html_text: str, base_url: str) -> List[Dict[str, Any]]:
    """
    Извлекаем из страницы категории карточки товара:
    - name
    - product_url
    - thumb_url (если есть)
    """
    soup = BeautifulSoup(html_text, "lxml")
    items: List[Dict[str, Any]] = []

    # Карточки у JoomShopping могут верстаться по-разному.
    # Берём все ссылки на /goods/*.html и пытаемся рядом найти <img>.
    for a in soup.select('a[href*="/goods/"][href$=".html"]'):
        href = a.get("href") or ""
        name = norm(a.get_text()) or norm(a.get("title"))
        product_url = same_host(base_url, href)

        # ищем картинку в пределах карточки (родительские контейнеры)
        thumb_url = None
        # 1) картинка как ребёнок ссылки
        img = a.find("img")
        # 2) если нет — ищем картинку в ближайшем контейнере
        if not img:
            for div in (a.parent, a.find_parent("div")):
                if not div:
                    continue
                img = div.find("img")
                if img:
                    break

        if img:
            # у lazy-картинок src может быть пустым, а путь лежать в data-src/data-original
            for attr in ("src", "data-src", "data-original"):
                val = img.get(attr)
                if val and "thumb_" in val:
                    thumb_url = same_host(base_url, val)
                    break
            # fallback: если src есть, но без "thumb_", всё равно сохраним как есть
            if not thumb_url and img.get("src"):
                thumb_url = same_host(base_url, img.get("src"))

        if not name:
            # иногда текст в ссылке пустой, а имя в alt
            if img and img.get("alt"):
                name = norm(img.get("alt"))

        # Скипаем дубликаты по URL товара
        if not product_url or any(x["product_url"] == product_url for x in items):
            continue

        items.append({
            "name": name or "Товар Copyline",
            "product_url": product_url,
            "thumb_url": thumb_url,
        })

    return items

def find_next_page(html_text: str, base_url: str, seen: set) -> Optional[str]:
    """
    Пытаемся найти ссылку на следующую страницу пагинации.
    Ищем:
      - <a rel="next" ...>
      - ссылки с текстом "Следующая", "Next", ">"
      - активную пагинацию с числами (берём +1)
    Возвращаем абсолютный URL или None.
    """
    soup = BeautifulSoup(html_text, "lxml")

    # rel="next"
    a = soup.find("a", rel=lambda v: v and "next" in v.lower())
    if a and a.get("href"):
        url = same_host(base_url, a["href"])
        return None if url in seen else url

    # текстовые варианты
    for txt in ("Следующая", "Следующая >", "Next", "Вперед", ">"):
        a = soup.find("a", string=lambda s: s and txt in s)
        if a and a.get("href"):
            url = same_host(base_url, a["href"])
            return None if url in seen else url

    # Числовая пагинация: ищем текущую активную и берём следующий элемент
    # (делаем максимально мягкую эвристику)
    pag = soup.select("ul.pagination li, div.pagination li, .pagination a, .paginator a")
    # если нашли набор ссылок-страниц, попробуем определить текущую и следующую
    current_found = False
    for el in pag:
        # текущая (активная) может быть <span class="active"> или <li class="active"><span>n</span></li>
        if ("active" in (el.get("class") or [])) or (el.name in ("span",) and "active" in (el.parent.get("class") or [])):
            current_found = True
            continue
        if current_found and el.name == "a" and el.get("href"):
            url = same_host(base_url, el["href"])
            return None if url in seen else url

    return None

# -------------------- Парсинг карточки (на случай, если превью фейковое) --------------------
def parse_product_image_from_card(product_url: str) -> Optional[str]:
    """
    Открываем карточку, ищем основной <img itemprop="image" id="main_image_*" src="...">.
    Возвращаем абсолютную ссылку или None.
    """
    try:
        r = fetch(product_url)
        soup = BeautifulSoup(r.text, "lxml")
        # основные варианты
        img = soup.find("img", id=re.compile(r"^main_image_\d+")) \
              or soup.find("img", attrs={"itemprop": "image"}) \
              or soup.find("img", class_=re.compile(r"(product|main).*image", re.I))
        if img and img.get("src"):
            return same_host(product_url, img["src"])
        # fallback: любая крупная картинка внутри блока галереи
        gal = soup.select_one(".productfull .image_middle, .productfull .image_big, .product_image_middle, .product_image_big")
        if gal:
            img = gal.find("img")
            if img and img.get("src"):
                return same_host(product_url, img["src"])
    except Exception:
        return None
    return None

# -------------------- Генерация YML --------------------
ROOT_CAT_ID = "9300000"

def hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)

def cat_id_for(name: str) -> str:
    return str(9300001 + (hash_int(name.lower()) % 400000))

def build_yml(items: List[Dict[str, Any]], category_title: str) -> bytes:
    """
    Формируем минимальный корректный YML:
      - одна корневая категория "Copyline"
      - подкатегория = название категории с сайта (например, "Тонер-картриджи Brother")
      - offer: name, url, picture, currencyId=KZT, categoryId
    """
    # корневой <yml_catalog><shop>...
    root = Element("yml_catalog")
    shop = SubElement(root, "shop")
    SubElement(shop, "name").text = "copyline"
    curr = SubElement(shop, "currencies")
    SubElement(curr, "currency", {"id": "KZT", "rate": "1"})

    # категории
    xml_cats = SubElement(shop, "categories")
    SubElement(xml_cats, "category", {"id": ROOT_CAT_ID}).text = "Copyline"
    subcat_id = cat_id_for(category_title)
    SubElement(xml_cats, "category", {"id": subcat_id, "parentId": ROOT_CAT_ID}).text = category_title

    # офферы
    offers = SubElement(shop, "offers")
    used_ids = set()

    for it in items:
        # стабильный id — хэш от URL товара
        oid = "copyline:" + hashlib.md5(it["product_url"].encode("utf-8")).hexdigest()[:10]
        if oid in used_ids:
            # крайне маловероятно, но на всякий — добьем ещё 2 символа
            suf = hashlib.md5((oid + it["name"]).encode("utf-8")).hexdigest()[:2]
            oid = f"{oid}-{suf}"
        used_ids.add(oid)

        o = SubElement(offers, "offer", {"id": oid, "available": "true", "in_stock": "true"})
        SubElement(o, "name").text = it["name"]
        SubElement(o, "url").text = it["product_url"]
        if it.get("picture"):
            SubElement(o, "picture").text = it["picture"]
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = subcat_id

        # Кол-во — просто 1, чтобы маркет/агрегаторы не ругались
        for tag in ("quantity_in_stock", "stock_quantity", "quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

# -------------------- Основная логика --------------------
def main():
    if not CATEGORY_URL:
        raise SystemExit("ERROR: CATEGORY_URL не задан")

    ensure_dir_for(OUT_FILE)

    # Собираем все страницы пагинации
    base = "{uri.scheme}://{uri.netloc}/".format(uri=urlparse(CATEGORY_URL))
    seen_pages = set()
    page_url = CATEGORY_URL
    all_items: List[Dict[str, Any]] = []
    category_title = "Copyline: Category"

    for page_no in range(1, MAX_PAGES + 1):
        if not page_url or page_url in seen_pages:
            break
        seen_pages.add(page_url)

        r = fetch(page_url)
        html_text = r.text

        # заголовок категории с первой страницы
        if page_no == 1:
            category_title = extract_category_title(BeautifulSoup(html_text, "lxml"))

        # парсим карточки
        batch = parse_list_page(html_text, base)
        print(f"[page {page_no}] найдено карточек: {len(batch)}")
        all_items.extend(batch)

        # следующая страница?
        next_url = find_next_page(html_text, base, seen_pages)
        if not next_url:
            break

        time.sleep(REQUEST_DELAY_MS / 1000.0)
        page_url = next_url

    # Обогащаем картинками (thumb_ -> full_). Где нужно — фоллбек к карточке.
    enriched: List[Dict[str, Any]] = []
    for it in all_items:
        name = it["name"]
        product_url = it["product_url"]
        thumb = it.get("thumb_url")
        picture = None

        if thumb:
            candidate = to_full_url(thumb)
            # Если превью "общая" (например, thumb_black-toner.jpg) — лучше сразу идти в карточку
            fn = thumb.rsplit("/", 1)[-1].lower()
            is_generic = any(x in fn for x in ["black-toner", "noimage", "placeholder"])
            if not is_generic and head_ok(candidate, MIN_BYTES):
                picture = candidate

        # Фоллбек — из карточки товара
        if not picture:
            pic2 = parse_product_image_from_card(product_url)
            if pic2 and head_ok(pic2, MIN_BYTES):
                picture = pic2
            # крайний случай — оставим хотя бы thumb
            elif thumb and head_ok(thumb, 1000):
                picture = thumb

        enriched.append({
            "name": name,
            "product_url": product_url,
            "picture": picture,
        })

        # Между карточками тоже слегка притормаживаем
        time.sleep(REQUEST_DELAY_MS / 1000.0)

    # Формируем YML
    yml_bytes = build_yml(enriched, category_title)
    with open(OUT_FILE, "wb") as f:
        f.write(yml_bytes)

    have_pics = sum(1 for x in enriched if x.get("picture"))
    print(f"[OK] {OUT_FILE}: товаров={len(enriched)}, с картинкой={have_pics}, категория='{category_title}'")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e)
        raise
