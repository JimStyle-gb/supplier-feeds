# -*- coding: utf-8 -*-
"""
Скрипт парсит страницу(ы) категории copyline.kz (JoomShopping),
вытягивает карточки товаров, изображения 'thumb_*', превращает в 'full_*',
проверяет, что картинка существует, и собирает YML (Yandex Market) в docs/copyline.yml.

=== ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ===
- CATEGORY_URL       : URL категории (если пусто — дефолт: Brother тонер-картриджи)
- OUT_FILE           : путь, куда писать YML (по умолчанию: docs/copyline.yml)
- OUTPUT_ENCODING    : кодировка XML (по умолчанию windows-1251)
- REQUEST_DELAY_MS   : пауза между запросами, мс (по умолчанию 600)
- PAGE_TIMEOUT_S     : таймаут запроса страницы, сек (по умолчанию 25)
- MAX_PAGES          : максимум страниц пагинации (по умолчанию 40)
- MIN_BYTES          : минимальный вес jpg, чтобы считать её валидной (по умолчанию 2500)

=== ЛОГИКА ПО СКРЕЙПУ ===
1) Грузим категорию; собираем товары из листинга:
   - имя (из title/alt/подписи),
   - ссылка на товар (если есть),
   - картинка thumb_* из <img>, меняем 'thumb' -> 'full'.
2) HEAD/GET проверка 'full' (200 и размер >= MIN_BYTES). Если нет — пробуем исходный 'thumb'.
3) Складываем в YML:
   - <name>, <url> (если найден), <picture> (валидная), <price> (если распарсился), <categoryId>.
   - Валюта: KZT. Корневая категория: "Copyline", вложенная — по заголовку страницы.
4) Если не нашли ни одного товара — всё равно создаём валидный YML (пустой список offers),
   чтобы деплой на GitHub Pages не падал.

=== ПРИМЕЧАНИЯ ===
- Разметка JoomShopping может слегка меняться; в коде несколько "мягких" селекторов.
- Пагинация в JoomShopping бывает разной (параметр ?start= или номера страниц); берём "следующую" ссылку,
  пока не упремся или пока не превысим MAX_PAGES.
"""

from __future__ import annotations
import os, re, time, io, hashlib
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ------------------- настройки из окружения -------------------
BASE_CATEGORY = "https://copyline.kz/goods/toner-cartridges-brother.html"
CATEGORY_URL = os.getenv("CATEGORY_URL") or BASE_CATEGORY

OUT_FILE        = os.getenv("OUT_FILE")        or "docs/copyline.yml"
ENC             = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
REQUEST_DELAY   = int(os.getenv("REQUEST_DELAY_MS") or "600") / 1000.0
PAGE_TIMEOUT_S  = int(os.getenv("PAGE_TIMEOUT_S") or "25")
MAX_PAGES       = int(os.getenv("MAX_PAGES") or "40")
MIN_BYTES       = int(os.getenv("MIN_BYTES") or "2500")

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ------------------- утилиты -------------------
def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def is_http(url: str) -> bool:
    try:
        return url.startswith("http://") or url.startswith("https://")
    except:
        return False

def http_exists(url: str) -> bool:
    """Проверяем, что файл существует и не пустой: сначала HEAD, при сомнении — GET с небольшим таймаутом."""
    try:
        r = requests.head(url, headers=UA, timeout=PAGE_TIMEOUT_S, allow_redirects=True)
        if r.status_code == 200:
            clen = r.headers.get("Content-Length")
            if clen and clen.isdigit() and int(clen) >= MIN_BYTES:
                return True
        # Если HEAD не дал размер — проверим коротким GET (без выкачивания всего)
        r = requests.get(url, headers=UA, timeout=PAGE_TIMEOUT_S, stream=True)
        if r.status_code == 200:
            n = 0
            for chunk in r.iter_content(chunk_size=2048):
                if not chunk:
                    break
                n += len(chunk)
                if n >= MIN_BYTES:
                    return True
        return False
    except requests.RequestException:
        return False

def make_full_from_thumb(src: str) -> str:
    """
    В листинге у Copyline встречается thumb_*.
    Меняем 'thumb' → 'full' (сохраняя остальную часть имени).
    Примеры:
      thumb_TN-1075.jpg -> full_TN-1075.jpg
      /.../thumb_TN-1075.jpg -> /.../full_TN-1075.jpg
    """
    # только basename меняем
    return src.replace("/thumb_", "/full_").replace("/thumb", "/full")

def parse_price(text: str) -> Optional[int]:
    if not text:
        return None
    t = norm(text).replace("₸","").replace("тг","").replace(",",".")
    digits = re.sub(r"[^\d]", "", t)
    return int(digits) if digits else None

def hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)

def cat_id_for(name: str) -> str:
    return str(9300001 + (hash_int(name.lower()) % 400000))

def offer_id(name: str, href: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", norm(name).lower())
    h = hashlib.md5((norm(name).lower()+"|"+norm(href).lower()).encode("utf-8")).hexdigest()[:8]
    return f"copyline:{base}:{h}"

# ------------------- парсер категории -------------------
def find_next_page(soup: BeautifulSoup, current_url: str) -> Optional[str]:
    """
    Ищем ссылку пагинации "вперёд": rel=next, или по классам-пагинаторам.
    Возвращаем абсолютный URL или None.
    """
    # rel=next
    a = soup.find("a", rel=lambda v: v and "next" in v.lower())
    if a and a.get("href"):
        return urljoin(current_url, a["href"])

    # по тексту
    for a in soup.select("a"):
        txt = norm(a.get_text()).lower()
        if txt in {"следующая", "вперёд", "next", ">" } and a.get("href"):
            return urljoin(current_url, a["href"])

    # общепринятые блоки пагинации joomshopping
    pag = soup.select_one(".pagination, .pagenav, .jshop_pagination")
    if pag:
        a = pag.find("a", string=lambda t: t and ">" in t)
        if a and a.get("href"):
            return urljoin(current_url, a["href"])

    return None

def parse_listing_page(html: str, page_url: str) -> List[Dict[str, Any]]:
    """
    Достаём товары из листинга. Подстраиваемся под разметку:
    - Берём вокруг <img> ближайшие название/ссылку/цену.
    - Изображение обязательно; по нему строим 'full_'.
    """
    soup = BeautifulSoup(html, "lxml")

    # Заголовок категории (для подкатегории)
    h1 = soup.find("h1")
    cat_title = norm(h1.get_text()) if h1 else "Copyline"

    items: List[Dict[str, Any]] = []

    # Вариант 1: карточки товаров
    product_blocks = soup.select(".product, .jshop_list_product, .jshop_prod, .productitem")
    if not product_blocks:
        # fallback: любые картинки из каталога
        product_blocks = soup.select("div, li")

    for block in product_blocks:
        # ищем IMG
        img = block.find("img", src=True)
        if not img:
            continue
        src = img.get("src") or ""
        if "components/com_jshopping/files/img_products" not in src:
            continue

        # нормализуем URL
        img_url = urljoin(page_url, src)

        # строим 'full_' вместо 'thumb'
        full_url = make_full_from_thumb(img_url)

        # имя товара: alt/img title/ подпись/ссылка
        name = norm(img.get("alt") or img.get("title") or "")
        if not name:
            a_name = block.find("a")
            if a_name:
                name = norm(a_name.get_text())

        # ссылка на карточку (если есть)
        href = ""
        a = block.find("a", href=True)
        if a:
            href = urljoin(page_url, a["href"])

        # цена (если есть рядом)
        price_text = ""
        price_el = block.select_one(".price, .jshop_price, .product_price, .old_price")
        if price_el:
            price_text = norm(price_el.get_text())
        price = parse_price(price_text)

        if not name:
            # без имени — пропускаем
            continue

        items.append({
            "name": name,
            "href": href,
            "thumb_or_full": full_url,  # попробуем full сначала
            "fallback_thumb": img_url,  # а это исходный thumb
            "price": price,
            "category": cat_title or "Copyline",
        })

    return items

def scrape_category(category_url: str) -> List[Dict[str, Any]]:
    """
    Идём по пагинации категории и собираем все товары.
    """
    all_items: List[Dict[str, Any]] = []
    seen_pages = set()
    url = category_url
    pages = 0

    while url and pages < MAX_PAGES:
        if url in seen_pages:
            break
        seen_pages.add(url)

        r = requests.get(url, headers=UA, timeout=PAGE_TIMEOUT_S)
        r.raise_for_status()

        items = parse_listing_page(r.text, url)
        all_items.extend(items)

        nxt = find_next_page(BeautifulSoup(r.text, "lxml"), url)
        url = nxt
        pages += 1
        time.sleep(REQUEST_DELAY)

    return all_items

# ------------------- сборка YML -------------------
ROOT_CAT_ID = "9300000"

def build_yml(items: List[Dict[str, Any]]) -> bytes:
    # собираем набор подкатегорий
    cats: Dict[str, str] = {}
    for it in items:
        nm = it.get("category") or "Copyline"
        if nm.strip().lower() != "copyline":
            cats.setdefault(nm, cat_id_for(nm))

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

    used_ids = set()
    for it in items:
        name = it["name"]
        href = it.get("href","")
        price = it.get("price")
        cat_name = it.get("category") or "Copyline"
        cat_id = ROOT_CAT_ID if cat_name.strip().lower() == "copyline" else cats.get(cat_name, ROOT_CAT_ID)

        # картинка: пробуем full, если нет — thumb
        img_main = it["thumb_or_full"]
        if not http_exists(img_main):
            fallback = it["fallback_thumb"]
            if http_exists(fallback):
                img_main = fallback
            else:
                img_main = ""  # не ставим picture

        oid = offer_id(name, href)
        if oid in used_ids:
            # на всякий случай уникализируем
            extra = hashlib.md5((name + href).encode("utf-8")).hexdigest()[:6]
            i = 2
            while f"{oid}-{extra}-{i}" in used_ids:
                i += 1
            oid = f"{oid}-{extra}-{i}"
        used_ids.add(oid)

        o = SubElement(offers, "offer", {"id": oid, "available": "true", "in_stock": "true"})
        SubElement(o, "name").text = name
        if price is not None:
            SubElement(o, "price").text = str(price)
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cat_id
        if href:
            SubElement(o, "url").text = href
        if img_main:
            SubElement(o, "picture").text = img_main

        # минимальные "складские" теги — чтобы маркетплейс не ругался
        for tag in ("quantity_in_stock", "stock_quantity", "quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

# ------------------- main -------------------
def main():
    ensure_dir_for(OUT_FILE)
    print(f"[info] Category: {CATEGORY_URL}")
    items = scrape_category(CATEGORY_URL)
    print(f"[info] Parsed items: {len(items)}")

    yml = build_yml(items)
    with open(OUT_FILE, "wb") as f:
        f.write(yml)

    print(f"[ok] Wrote: {OUT_FILE} (offers={len(items)})")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e)
        raise
