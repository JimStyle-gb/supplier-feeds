# -*- coding: utf-8 -*-
"""
ЗАДАЧА
------
"Просто, но эффективно" собрать YML для Satu напрямую со страницы категории Copyline.
Берём:
- name (название с листинга)
- url (ссылка на карточку)
- price (если есть на листинге)
- picture (из <img> с листинга: thumb_* -> full_*)
- categoryId (одна категория из <h1> страницы)
- vendor, vendorCode, typePrefix, model, description — выводим логично из названия/картинки
  без захода в карточку товара (быстро и надёжно).

ПАРАМЕТРЫ (ENV)
---------------
CATEGORY_URL   : обязательный — URL категории (например: https://copyline.kz/goods/toner-cartridges-brother.html)
OUT_FILE       : путь к файлу YML (по умолчанию docs/copyline.yml)
OUTPUT_ENCODING: windows-1251 (по умолчанию), можно "utf-8" при отладке
REQUEST_DELAY_MS: задержка между запросами (по умолчанию 700)
PAGE_TIMEOUT_S : таймаут HTTP-запроса (по умолчанию 30)
MAX_PAGES      : максимальное число страниц пагинации (по умолчанию 50)
MIN_BYTES      : минимальный размер картинки (в байтах) для валидации по названию — тут не проверяем сетью, только длину URL

ВАЖНО
-----
- Не лезем в карточку товара: всё из листинга (стабильно и быстро).
- Картинку берём из <img src>, меняем `thumb_` -> `full_`.
- vendorCode / model пытаемся извлечь из имени файла картинки (TN-1075 и т.п.),
  если не получилось — пытаемся из названия.
- vendor определяем по ключевым словам: "Euro Print" → Euro Print, "OEM" → Brother,
  иначе для этой категории разумно vendor=Brother.
- typePrefix задаём “Тонер-картридж” (под нашу категорию). При другой категории можно поменять.

РЕЗУЛЬТАТ
---------
Файл docs/copyline.yml с валидной YML-структурой.
"""

from __future__ import annotations
import os
import re
import io
import time
import hashlib
import html
import sys
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, ElementTree

# =========================
# ПАРАМЕТРЫ И НАСТРОЙКИ
# =========================
BASE_URL        = "https://copyline.kz"
CATEGORY_URL    = os.getenv("CATEGORY_URL")  # ОБЯЗАТЕЛЕН
OUT_FILE        = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC             = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
REQUEST_DELAY_S = (int(os.getenv("REQUEST_DELAY_MS", "700")) / 1000.0)
PAGE_TIMEOUT_S  = int(os.getenv("PAGE_TIMEOUT_S", "30"))
MAX_PAGES       = int(os.getenv("MAX_PAGES", "50"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "2500"))  # здесь используем как "просто фильтр" длины URL

UA_HEADERS = {
    # достаточно правдоподобный UA
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

ROOT_CAT_ID = "9300000"  # корневая категория в нашем YML


# =========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =========================
def norm(s: Optional[str]) -> str:
    """Приводим строки к аккуратному виду."""
    return re.sub(r"\s+", " ", (s or "").strip())

def ensure_dir_for(path: str):
    """Гарантируем, что каталог под файл существует."""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def fetch(url: str) -> str:
    """GET HTML с таймаутом и задержкой между запросами."""
    time.sleep(REQUEST_DELAY_S)
    r = requests.get(url, headers=UA_HEADERS, timeout=PAGE_TIMEOUT_S)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or 'utf-8'
    return r.text

def absolutize_url(href: str) -> str:
    """Делаем абсолютный URL (если относительный)."""
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"{BASE_URL}{href}"
    return f"{BASE_URL}/{href.lstrip('./')}"

def file_stem_from_img_url(img_url: str) -> str:
    """
    Достаём "стем" файла из URL (без каталога и расширения).
    Прим.: https://.../thumb_TN-1075.jpg -> thumb_TN-1075
    """
    fn = img_url.split("?")[0].split("/")[-1]
    stem = re.sub(r"\.[A-Za-z0-9]+$", "", fn)
    return stem

def guess_vendorcode_from_img(img_url: str) -> Optional[str]:
    """
    Извлекаем код модели из имени файла картинки.
    thumb_TN-1075.jpg -> TN-1075
    full_TN-2375-oem.jpg -> TN-2375 (счистим -oem/-ep хвосты).
    """
    stem = file_stem_from_img_url(img_url)
    stem = stem.replace("thumb_", "").replace("full_", "")
    # уберём хвосты типа -ep, -oem и подобные
    stem = re.sub(r"-(ep|oem)$", "", stem, flags=re.IGNORECASE)

    # Находим шаблон вида TN-1075, DR-1075, CF283A и т.п.
    m = re.search(r"[A-Za-z]{1,5}-?[0-9]{2,5}[A-Za-z0-9\-]*", stem)
    if m:
        code = m.group(0).upper()
        # нормализуем дефис между буквенной и цифровой частью, если нужно
        code = re.sub(r"([A-Za-z]+)(\d)", r"\1-\2", code)
        return code
    return None

def guess_vendorcode_from_name(name: str) -> Optional[str]:
    """
    Если по картинке не получилось, пробуем вытащить из названия.
    """
    s = norm(name)
    m = re.search(r"[A-Za-z]{1,5}-?\d{2,5}[A-Za-z0-9\-]*", s)
    if m:
        code = m.group(0).upper()
        code = re.sub(r"([A-Za-z]+)(\d)", r"\1-\2", code)
        return code
    return None

def vendor_from_name(name: str, default_vendor: str = "Brother") -> str:
    """
    Определяем бренд:
    - если в названии есть "Euro Print" → Euro Print
    - если "OEM" → Brother
    - иначе → default_vendor (для этой категории — Brother)
    """
    low = name.lower()
    if "euro print" in low:
        return "Euro Print"
    if "oem" in low:
        return "Brother"
    return default_vendor

def to_full_picture_url(img_src: str) -> Optional[str]:
    """
    Из <img src> с листинга делаем URL "full_*".
    Если там уже full_, возвращаем как есть.
    Если нет thumb_, всё равно вернём абсолютный URL (лучше такая картинка, чем никакая).
    """
    if not img_src:
        return None
    url = absolutize_url(img_src)
    url = url.replace("thumb_", "full_")
    return url

def price_from_text(text: str) -> Optional[int]:
    """Достаём число из текстовой цены."""
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None

def cat_id_for(name: str) -> str:
    """Детерминированный ID категории по названию (как в прежних скриптах)."""
    h = int(hashlib.md5(name.encode("utf-8")).hexdigest()[:6], 16)
    return str(9300001 + (h % 400000))

def offer_id_from(name: str, url: str) -> str:
    """Стабильный offer id: из названия+url (без артикула, т.к. его может не быть)."""
    base = re.sub(r"[^a-z0-9]+", "-", norm(name).lower())
    h = hashlib.md5((norm(name).lower()+"|"+url.lower()).encode("utf-8")).hexdigest()[:8]
    return f"copyline:{base}:{h}"


# =========================
# ПАРСИНГ ЛИСТИНГА КАТЕГОРИИ
# =========================
def parse_category(category_url: str) -> Dict[str, Any]:
    """
    Скачиваем страницу категории, вытаскиваем:
    - заголовок категории (H1)
    - список товаров: name, url, price, picture(full_*), vendor, vendorCode, model, typePrefix, description
    Пагинация: пытаемся находить ссылку на "следующую" страницу и идти дальше.
    """
    items: List[Dict[str, Any]] = []
    visited = set()

    # Название категории попробуем взять с первой страницы (H1)
    category_name = "Copyline"

    url = category_url
    pages = 0

    while url and url not in visited and pages < MAX_PAGES:
        visited.add(url)
        pages += 1

        html_text = fetch(url)
        soup = BeautifulSoup(html_text, "html.parser")

        # Заголовок категории:
        h1 = soup.find(["h1", "h2"], string=True)
        if h1:
            category_name = norm(h1.get_text()) or category_name

        # Ищем карточки товаров:
        # Простая и надёжная эвристика: все <a> ведущие на /goods/*.html
        product_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/goods/" in href and href.endswith(".html"):
                # Внутри ближнего контейнера найдём картинку и цену
                product_links.append(a)

        seen_urls = set()

        for a in product_links:
            prod_url = absolutize_url(a["href"])
            if prod_url in seen_urls:
                continue
            seen_urls.add(prod_url)

            name = norm(a.get_text())
            if not name:
                # если у <a> пустой текст, попробуем текст внутри карточки
                name = norm(a.get("title") or "")

            # найдём контейнер с картинкой/ценой: пойдём вверх до блока, а затем поиск вниз
            container = a
            for _ in range(4):  # ограничим подъём по дереву
                if container and container.parent:
                    container = container.parent
                else:
                    break

            # картинка
            img_tag = None
            if container:
                img_tag = container.find("img")
            if not img_tag:
                # запасной вариант — попробуем найти картинку рядом
                img_tag = a.find("img")

            img_src = img_tag.get("src") if img_tag else None
            picture = to_full_picture_url(img_src) if img_src else None

            # цена
            price = None
            price_candidates = []
            if container:
                price_candidates += container.find_all(text=re.compile(r"\d"))
            # из найденного текста выберем тот, где больше цифр и есть "тг"/валюта/пробелы
            best = None
            best_digits = 0
            for t in price_candidates:
                s = norm(t)
                digits = re.sub(r"[^\d]", "", s)
                if len(digits) > best_digits:
                    best_digits = len(digits)
                    best = s
            price = price_from_text(best or "")

            # поля Satu: vendor/vendorCode/model/typePrefix/description
            vendor_code = guess_vendorcode_from_img(picture or "") or guess_vendorcode_from_name(name) or ""
            vendor = vendor_from_name(name, default_vendor="Brother")  # под эту категорию
            type_prefix = "Тонер-картридж"
            model = vendor_code or ""  # обычно совпадает
            description = f"{name}. Подходит для принтеров Brother. (Автоген.)"

            items.append({
                "name": name,
                "url": prod_url,
                "price": price,
                "picture": picture,
                "vendor": vendor,
                "vendorCode": vendor_code,
                "typePrefix": type_prefix,
                "model": model,
                "description": description,
            })

        # ПАГИНАЦИЯ: ищем ссылку на следующую страницу
        next_url = None

        # 1) rel="next"
        link_next = soup.find("a", attrs={"rel": "next"})
        if link_next and link_next.get("href"):
            next_url = absolutize_url(link_next["href"])

        # 2) по тексту "Следующая", "»", ">", "Next"
        if not next_url:
            for a in soup.find_all("a", href=True):
                txt = norm(a.get_text()).lower()
                if txt in {"следующая", "далее", "»", ">>", "next", "вперед", "вперёд"}:
                    next_url = absolutize_url(a["href"])
                    break

        # 3) по классу пагинации
        if not next_url:
            pag = soup.find(class_=re.compile(r"pagin|jshop"))
            if pag:
                # часто последняя активная ссылка - это "следующая"
                candidates = pag.find_all("a", href=True)
                for a in candidates:
                    if "next" in (a.get("rel") or []) or "next" in (a.get("class") or []):
                        next_url = absolutize_url(a["href"])
                        break

        url = next_url

    return {
        "category_name": category_name or "Copyline",
        "items": items
    }


# =========================
# СБОРКА YML
# =========================
def build_yml(category_name: str, items: List[Dict[str, Any]]) -> bytes:
    """
    Собираем валидный YML. Добавим company/url в <shop>, как любит Satu.
    """
    ensure_dir_for(OUT_FILE)

    # категории
    cats_map = {category_name: cat_id_for(category_name)}

    root = Element("yml_catalog")
    shop = SubElement(root, "shop")

    # Обязательные и полезные поля магазина
    SubElement(shop, "name").text = "copyline"
    SubElement(shop, "company").text = "copyline"
    SubElement(shop, "url").text = BASE_URL

    # Валюта
    curr = SubElement(shop, "currencies")
    SubElement(curr, "currency", {"id": "KZT", "rate": "1"})

    # Категории
    xml_cats = SubElement(shop, "categories")
    SubElement(xml_cats, "category", {"id": ROOT_CAT_ID}).text = "Copyline"
    SubElement(xml_cats, "category", {"id": cats_map[category_name], "parentId": ROOT_CAT_ID}).text = category_name

    # Офферы
    offers = SubElement(shop, "offers")

    used_ids = set()

    for it in items:
        name = it.get("name") or ""
        url = it.get("url") or ""
        price = it.get("price")
        picture = it.get("picture")

        oid = offer_id_from(name, url)
        if oid in used_ids:
            # подстрахуемся от дублей
            i = 2
            base = oid
            while f"{base}-{i}" in used_ids:
                i += 1
            oid = f"{base}-{i}"
        used_ids.add(oid)

        o = SubElement(offers, "offer", {"id": oid, "available": "true", "in_stock": "true"})
        SubElement(o, "url").text = url

        if price is not None:
            SubElement(o, "price").text = str(price)
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cats_map[category_name]

        SubElement(o, "name").text = name
        if it.get("vendor"):
            SubElement(o, "vendor").text = it["vendor"]
        if it.get("vendorCode"):
            SubElement(o, "vendorCode").text = it["vendorCode"]
        if it.get("typePrefix"):
            SubElement(o, "typePrefix").text = it["typePrefix"]
        if it.get("model"):
            SubElement(o, "model").text = it["model"]
        if it.get("description"):
            SubElement(o, "description").text = it["description"]

        if picture:
            SubElement(o, "picture").text = picture

        # складские маркеры (чтобы фиды не ругались)
        for tag in ("quantity_in_stock", "stock_quantity", "quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()


# =========================
# MAIN
# =========================
def main():
    if not CATEGORY_URL:
        print("ERROR: CATEGORY_URL не задан", file=sys.stderr)
        sys.exit(1)

    parsed = parse_category(CATEGORY_URL)
    yml_bytes = build_yml(parsed["category_name"], parsed["items"])

    with open(OUT_FILE, "wb") as f:
        f.write(yml_bytes)

    print(f"[OK] {OUT_FILE}: category='{parsed['category_name']}' items={len(parsed['items'])}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
