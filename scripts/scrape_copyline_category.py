#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Простой парсер категории copyline.kz с сохранением в YML.
Задача: собрать товары ТОЛЬКО с текущей категории, для каждого
сходить в карточку и вытащить описание, артикул, цену, картинку.

Ключевые принципы упрощения:
- Только requests + BeautifulSoup со встроенным 'html.parser' (без lxml).
- Жёстко ограничиваем парсинг товарами внутри контейнера списка товаров,
  чтобы не ловить посторонние ссылки (как раньше 135 URL из меню/сайдбара).
- Описание берём строго из карточки по набору типичных селекторов JoomShopping.
- Минимум «магии»: аккуратные регулярки, много проверок, безопасные фолбэки.
"""

import os
import re
import time
import html
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


# ----------------------- Конфигурация из ENV -----------------------
BASE_URL         = os.getenv("BASE_URL", "https://copyline.kz").strip()
CATEGORY_URL     = os.getenv("CATEGORY_URL", "").strip()
OUT_FILE         = os.getenv("OUT_FILE", "docs/copyline.yml").strip()
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "700"))
PAGE_TIMEOUT_S   = int(os.getenv("PAGE_TIMEOUT_S", "30"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))

HEADERS = {
    # Базовый User-Agent, чтобы сайт не думал, что это что-то «подозрительное»
    "User-Agent": "Mozilla/5.0 (compatible; copyline-scraper/1.0; +https://example.org)",
}
# ------------------------------------------------------------------


# ======================== УТИЛИТЫ HTTP/HTML ========================
def make_soup(html_text: str) -> BeautifulSoup:
    """
    Универсальная обёртка над BeautifulSoup с встроенным парсером.
    Никаких внешних зависимостей (lxml) — меньше падений в CI.
    """
    return BeautifulSoup(html_text, "html.parser")


def get(url: str) -> Optional[str]:
    """
    HTTP GET с таймаутом, минимумом байт и задержкой между запросами.
    Возвращает текст страницы либо None при проблемах.
    """
    try:
        r = requests.get(url, headers=HEADERS, timeout=PAGE_TIMEOUT_S)
        if r.status_code == 200 and len(r.content) >= MIN_BYTES:
            return r.text
        return None
    finally:
        # Вежливая пауза, чтобы не долбить сайт
        time.sleep(REQUEST_DELAY_MS / 1000.0)


def absolutize(url_or_path: str, base: str = BASE_URL) -> str:
    """
    Приводим относительные ссылки/картинки к абсолютным URL.
    """
    return urljoin(base, url_or_path)
# ==================================================================


# ===================== ПАРСИНГ ДАННЫХ ТОВАРА ======================
PRICE_CLEAN_RE = re.compile(r"[^\d,\.]+", re.ASCII)   # чистим всё, кроме цифр и разделителей
COMMA_RE       = re.compile(r",")                    # заменяем запятые на точки (если вдруг)


def parse_price(raw: str) -> Optional[int]:
    """
    Из строки вида '2 273 тг' делаем целое число 2273.
    Возвращает None, если что-то совсем странное.
    """
    if not raw:
        return None
    s = PRICE_CLEAN_RE.sub("", raw).strip()
    if not s:
        return None
    s = COMMA_RE.sub(".", s)
    try:
        # В подавляющем большинстве цен на copyline — целые тензге.
        val = float(s)
        return int(round(val))
    except ValueError:
        return None


def extract_sku_from_product(soup: BeautifulSoup) -> Optional[str]:
    """
    Ищем артикул в карточке товара.
    Варианты:
      - текст 'Артикул:' рядом с кодом,
      - метка 'vendorcode' / 'vendor_code' в таблицах характеристик.
    Возвращает строку кода или None.
    """
    text = soup.get_text(" ", strip=True)

    # 1) Самый частый шаблон: 'Артикул: 123456'
    m = re.search(r"Артикул\s*[:\-]?\s*([A-Za-z0-9\-_]+)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    # 2) Иногда код лежит в таблицах характеристик (ключ-значение)
    # Ищем тр в таблицах, где один из тд содержит 'Артикул'
    for tr in soup.select("table tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.select("td,th")]
        if any("артикул" in c.lower() for c in cells):
            # берем соседнюю ячейку, где сам код
            for c in cells:
                if "артикул" not in c.lower():
                    # пытаемся вычленить код
                    mm = re.search(r"([A-Za-z0-9\-_]+)", c)
                    if mm:
                        return mm.group(1)

    return None


def extract_description_from_product(soup: BeautifulSoup) -> str:
    """
    Ищем описание в карточке:
      - типовые блоки JoomShopping: .jshop_prod_description / .prod_description
      - fallback: itemprop="description", #description, .product_description
      - последний шанс: <meta name="description">
    Возвращаем чистый текст (без HTML), лишние пробелы схлопнуты.
    Если ничего нет — вернём пустую строку.
    """
    candidates = [
        ".jshop_prod_description",
        ".prod_description",
        "[itemprop='description']",
        "#description",
        ".product_description",
        ".tab-content #description",
        ".descr",
    ]
    for sel in candidates:
        node = soup.select_one(sel)
        if node:
            txt = node.get_text(" ", strip=True)
            if len(txt) >= 10:  # отсечём мусор из пары символов
                return txt

    # <meta name="description">
    meta = soup.select_one("meta[name='description']")
    if meta and meta.get("content"):
        txt = meta["content"].strip()
        if len(txt) >= 10:
            return txt

    return ""


def extract_product_cards(html_cat: str, base_url: str) -> List[Dict]:
    """
    Из HTML категории достаём ТОЛЬКО карточки товаров.
    Здесь важно не тащить ссылки из меню/подвала/сайдбара — только список товаров.
    Возвращает список словарей с минимальными данными из списка (name/url/img/price_str).
    """
    soup = make_soup(html_cat)

    # 1) Ищем контейнер списка товаров JoomShopping
    list_container = soup.select_one(".jshop_list_product") or soup.select_one(".jshop_products")
    if not list_container:
        # Фолбэк: на всякий случай ищем по менее специфичным признакам
        list_container = soup

    cards = []

    # 2) Каждая карточка может называться .product или .jshop_product — покрываем оба случая
    for card in list_container.select(".product, .jshop_product"):
        # имя + ссылка на карточку
        a = card.select_one(".name a[href], a[href*='/goods/']")
        if not a:
            continue
        url = absolutize(a.get("href", ""), base_url)
        name = a.get_text(" ", strip=True)

        # цена (ищем любой блок с 'price' в классе)
        price_node = None
        for node in card.select("[class*='price']"):
            # отсекаем возможные «старая цена»/«скидка» если попадётся
            if node.get_text(strip=True):
                price_node = node
                break
        price_str = price_node.get_text(" ", strip=True) if price_node else ""

        # картинка
        img = None
        img_node = card.select_one("img[src]")
        if img_node and img_node.get("src"):
            img = absolutize(img_node["src"], base_url)

        cards.append({
            "name": name,
            "url": url,
            "img": img,
            "price_str": price_str,
        })

    # 3) Фильтр от дубликатов и мусора (по URL)
    seen = set()
    clean = []
    for c in cards:
        u = c["url"]
        if "/goods/" not in u or not u.endswith(".html"):
            continue
        if u in seen:
            continue
        seen.add(u)
        clean.append(c)

    return clean


def enrich_with_product_page(card: Dict) -> Dict:
    """
    Для каждой карточки идём в товар, докидываем:
      - vendorCode (артикул)
      - description (описание)
      - при необходимости поправляем картинку (если не было на листинге)
      - актуализируем цену (если на карточке точнее; но по умолчанию берём из листинга)
    """
    html_prod = get(card["url"])
    if not html_prod:
        card["vendorCode"] = None
        card["description"] = ""
        return card

    soup = make_soup(html_prod)
    card["vendorCode"]  = extract_sku_from_product(soup)
    card["description"] = extract_description_from_product(soup)

    # Если на листинге не нашли картинку — пробуем основной img на карточке
    if not card.get("img"):
        main_img = soup.select_one("img#main_image, img#main_image_*, img[itemprop='image'], .image img")
        if main_img and main_img.get("src"):
            card["img"] = absolutize(main_img["src"])

    # Попытка взять цену точнее из карточки (не обязательно)
    price_node = soup.select_one(".prod_price, .price, [class*='price']")
    if price_node:
        p = parse_price(price_node.get_text(" ", strip=True))
        if p:
            card["price"] = p

    return card
# ==================================================================


# ========================== ЗАПИСЬ В YML ==========================
def cdata(text: str) -> str:
    """
    Безопасно оборачиваем в CDATA. Если внутри есть ']]>', по-простому экранируем.
    """
    if not text:
        return ""
    safe = text.replace("]]>", "]]&gt;")
    return f"<![CDATA[{safe}]]>"


def xml(text: str) -> str:
    """
    Экранирование спецсимволов для обычных XML-узлов.
    """
    return html.escape(text or "", quote=True)


def write_yml(category_name: str, items: List[Dict], out_path: str, encoding: str = "windows-1251") -> None:
    """
    Пишем YML с базовой структурой (как в alstyle.yml), добавлен description.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding=encoding, newline="") as f:
        f.write(f"<?xml version='1.0' encoding='{encoding}'?>\n")
        f.write("<yml_catalog><shop>")
        f.write("<name>copyline</name>")
        f.write("<currencies><currency id=\"KZT\" rate=\"1\" /></currencies>")
        f.write("<categories>")
        f.write("<category id=\"9300000\">Copyline</category>")
        f.write(f"<category id=\"9402524\" parentId=\"9300000\">{xml(category_name)}</category>")
        f.write("</categories>")
        f.write("<offers>")

        for it in items:
            # id делаем стабильным и читабельным: slug из URL + хеш от URL
            path = urlparse(it["url"]).path.strip("/").replace("/", "-").replace(".html", "")
            # маленький хеш на всякий случай
            h = hex(abs(hash(it["url"])) & 0xFFFFFFFF)[2:]
            offer_id = f"copyline:{path}:{h}"

            name = it.get("name") or ""
            price = it.get("price")
            if price is None:
                price = parse_price(it.get("price_str", "")) or 0

            f.write(f"<offer id=\"{xml(offer_id)}\" available=\"true\" in_stock=\"true\">")
            f.write(f"<name>{xml(name)}</name>")
            f.write(f"<price>{price}</price>")
            f.write("<currencyId>KZT</currencyId>")
            f.write("<categoryId>9402524</categoryId>")
            f.write(f"<url>{xml(it['url'])}</url>")
            if it.get("img"):
                f.write(f"<picture>{xml(it['img'])}</picture>")
            if it.get("vendorCode"):
                f.write(f"<vendorCode>{xml(it['vendorCode'])}</vendorCode>")

            # Описание кладём в CDATA, чтобы не бояться спецсимволов
            desc = it.get("description", "").strip()
            if desc:
                f.write(f"<description>{cdata(desc)}</description>")

            # Стандартизированные поля остатков (как раньше)
            f.write("<quantity_in_stock>1</quantity_in_stock>")
            f.write("<stock_quantity>1</stock_quantity>")
            f.write("<quantity>1</quantity>")

            f.write("</offer>")

        f.write("</offers></shop></yml_catalog>")
# ==================================================================


# ============================== MAIN ==============================
def main():
    # 1) Валидация входных параметров
    if not CATEGORY_URL:
        raise SystemExit("ERROR: CATEGORY_URL не задан")

    # 2) Качаем страницу категории
    html_cat = get(CATEGORY_URL)
    if not html_cat:
        raise SystemExit(f"ERROR: не удалось загрузить CATEGORY_URL: {CATEGORY_URL}")

    # 3) Достаём карточки (ТОЛЬКО товары, не меню)
    cards = extract_product_cards(html_cat, BASE_URL)
    if not cards:
        raise SystemExit("ERROR: не удалось найти товары на странице категории")

    # 4) Для каждой — заход в карточку за артикулом и описанием
    enriched: List[Dict] = []
    for idx, c in enumerate(cards, start=1):
        try:
            c2 = enrich_with_product_page(c)
            enriched.append(c2)
            print(f"[ok] {idx:03d}/{len(cards)} | SKU={c2.get('vendorCode') or '-'} | {c2['url']}")
        except Exception as e:
            print(f"[err] {idx:03d}/{len(cards)} | {c['url']} | {e}")

    # 5) Название категории берём из заголовка страницы
    soup_cat = make_soup(html_cat)
    cat_name = ""
    h1 = soup_cat.select_one("h1, .jshop h1, .content h1")
    if h1:
        cat_name = h1.get_text(" ", strip=True)
    if not cat_name:
        cat_name = "Категория"

    # 6) Пишем YML
    write_yml(cat_name, enriched, OUT_FILE, OUTPUT_ENCODING)
    print(f"[done] Сохранено: {OUT_FILE} (товаров: {len(enriched)})")


if __name__ == "__main__":
    main()
