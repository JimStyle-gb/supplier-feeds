#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрейп фото по списку артикулов и запись <picture> в docs/copyline.yml.

Как работает:
1) Берём артикуляры из docs/copyline_articles.txt (по одному в строке; пустые/начинающиеся с # — игнор).
2) Для каждого артикула пробуем открыть страницу поиска copyline.kz по нескольким известным эндпоинтам.
3) Со страницы результатов забираем ссылку на карточку товара (/goods/… или /product/… .html) и заходим в неё.
4) В карточке ищем главное фото:
   - <img itemprop="image" ... src="..."> — приоритетно
   - <img id="main_image..."> — запасной путь
   - любые <img> со ссылкой на /components/com_jshopping/files/img_products/ (берём первую не-миниатюру)
   При этом читаем src, а если его нет — data-src / data-original.
5) Грузим docs/copyline.yml, находим <offer> по совпадению <vendorCode> == артикулу и проставляем/обновляем <picture>.
6) Сохраняем YML в windows-1251, печатаем сводку.

Все задержки/ретраи управляются через env.
"""

import os, re, sys, time, random, urllib.parse
from typing import List, Optional, Tuple
import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

# ---------- Константы/настройки ----------
BASE_URL = "https://copyline.kz"

# Файлы/пути из ENV (с дефолтами)
YML_PATH         = os.getenv("YML_PATH", "docs/copyline.yml")
ARTICLES_FILE    = os.getenv("ARTICLES_FILE", "docs/copyline_articles.txt")
ENC              = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

# Сетевые настройки из ENV
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "700"))     # задержка между запросами
MAX_RETRIES      = int(os.getenv("MAX_RETRIES", "3"))            # попыток на один HTTP
BACKOFF_MAX_MS   = int(os.getenv("BACKOFF_MAX_MS", "12000"))     # максимум бэкоффа на ретраи
TIMEOUT_SEC      = 40                                            # таймаут каждого запроса

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "ru,en;q=0.8",
}

# Поисковые эндпоинты Joomla/Jshopping — пробуем последовательно
SEARCH_URLS = [
    # самый простой вариант, часто работает
    f"{BASE_URL}/?search={{q}}",
    # альтернативы Joomla
    f"{BASE_URL}/search?searchword={{q}}",
    f"{BASE_URL}/index.php?option=com_jshopping&controller=search&task=view&search={{q}}",
    f"{BASE_URL}/index.php?option=com_jshopping&controller=search&task=view&search_name={{q}}",
    f"{BASE_URL}/index.php?option=com_jshopping&controller=search&task=view&setsearchfrompage=1&search={{q}}",
]

# ---------- Утилиты ----------
def pause():
    """Вежливая пауза между запросами (с небольшим джиттером)."""
    ms = REQUEST_DELAY_MS + random.randint(0, 150)
    time.sleep(ms / 1000.0)

def http_get(url: str) -> Optional[str]:
    """GET с ретраями и экспоненциальным бэкоффом. Возвращает текст HTML или None."""
    delay = 0.8
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=TIMEOUT_SEC, allow_redirects=True)
            if r.status_code == 200 and r.text:
                return r.text
        except Exception:
            pass
        # бэкофф
        time.sleep(min(delay, BACKOFF_MAX_MS/1000.0))
        delay *= 2.0
    return None

def absolutize(href: str) -> str:
    """Абсолютная ссылка относительно BASE_URL."""
    return urllib.parse.urljoin(BASE_URL, href)

def find_product_links_in_search(html: str) -> List[str]:
    """
    Со страницы поиска вытянуть ссылки на карточки товаров.
    Ищем <a href=".../goods/...html"> и <a href=".../product/...html">.
    """
    out = []
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href: 
            continue
        # только карточки
        if ("/goods/" in href or "/product/" in href) and href.endswith(".html"):
            out.append(absolutize(href))
    # уникализируем, сохраняя порядок
    seen, uniq = set(), []
    for u in out:
        if u not in seen:
            uniq.append(u); seen.add(u)
    return uniq

def extract_main_image_from_product(html: str) -> Optional[str]:
    """
    Из HTML карточки товара достать ссылку на главное фото.
    Приоритеты:
      1) <img itemprop="image" ...>
      2) <img id="main_image...">
      3) любые <img> ведущие в /components/com_jshopping/files/img_products/ (не миниатюры)
    """
    soup = BeautifulSoup(html, "lxml")

    # 1) itemprop="image"
    img = soup.find("img", attrs={"itemprop": "image"})
    if img:
        for attr in ("src", "data-src", "data-original"):
            val = (img.get(attr) or "").strip()
            if val:
                return absolutize(val)

    # 2) id="main_image..."
    img = soup.find("img", id=re.compile(r"^main_image", re.I))
    if img:
        for attr in ("src", "data-src", "data-original"):
            val = (img.get(attr) or "").strip()
            if val:
                return absolutize(val)

    # 3) любые картинки из каталога продуктов
    for im in soup.find_all("img"):
        src = (im.get("src") or im.get("data-src") or im.get("data-original") or "").strip()
        if not src:
            continue
        src_low = src.lower()
        if "/components/com_jshopping/files/img_products/" in src_low:
            # стараемся избегать миниатюр
            if "/thumb_" in src_low or "/mini_" in src_low:
                # если ничего лучше не найдём — вернём потом
                pass
            return absolutize(src)

    return None

def search_product_page_urls(article: str) -> List[str]:
    """
    Пробуем все поисковые эндпоинты, собираем ссылки на карточки.
    Возвращаем список URL карточек (по убыванию приоритета).
    """
    urls: List[str] = []
    for tpl in SEARCH_URLS:
        q = urllib.parse.quote_plus(article)
        url = tpl.format(q=q)
        html = http_get(url)
        pause()
        if not html:
            continue
        links = find_product_links_in_search(html)
        if links:
            # как только в каком-то эндпоинте нашлись ссылки — используем их первыми
            urls.extend(links)
            break
    # уникализируем
    seen, uniq = set(), []
    for u in urls:
        if u not in seen:
            uniq.append(u); seen.add(u)
    return uniq

def read_articles(path: str) -> List[str]:
    """Прочитать артикулы из файла (по одному в строке)."""
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            out.append(s)
    return out

def load_yml_tree(path: str) -> ET.ElementTree:
    """Загрузить YML как ElementTree (кодировка windows-1251 поддерживается автоматически)."""
    return ET.parse(path)

def offers_by_vendorcode(tree: ET.ElementTree) -> dict:
    """
    Построить словарь vendorCode -> offer_element.
    Если у оффера нет vendorCode — он пропускается.
    """
    root = tree.getroot()
    offers_map = {}
    for offer in root.findall(".//offer"):
        vc = offer.findtext("vendorCode")
        if vc:
            offers_map[vc.strip()] = offer
    return offers_map

def set_offer_picture(offer_el: ET.Element, picture_url: str):
    """Создать/обновить тег <picture> в оффере."""
    pic = offer_el.find("picture")
    if pic is None:
        pic = ET.SubElement(offer_el, "picture")
    pic.text = picture_url

# ---------- Главная логика ----------
def main():
    # 0) Проверки
    if not os.path.isfile(ARTICLES_FILE):
        print(f"ERROR: файл со списком артикулов не найден: {ARTICLES_FILE}", file=sys.stderr)
        sys.exit(1)
    if not os.path.isfile(YML_PATH):
        print(f"ERROR: YML не найден: {YML_PATH}", file=sys.stderr)
        sys.exit(1)

    articles = read_articles(ARTICLES_FILE)
    if not articles:
        print("Список артикулов пуст, выходим.")
        return

    tree = load_yml_tree(YML_PATH)
    by_vc = offers_by_vendorcode(tree)

    found_cnt = 0
    updated_cnt = 0

    for art in articles:
        art = art.strip()
        if not art:
            continue

        # есть ли оффер с таким vendorCode?
        offer = by_vc.get(art)
        if offer is None:
            print(f"[skip] {art}: в YML нет оффера с таким <vendorCode>")
            continue

        # ищем карточку(и) товара через поиск
        product_urls = search_product_page_urls(art)
        if not product_urls:
            print(f"[skip] {art}: карточка не найдена в поиске")
            continue

        photo_url: Optional[str] = None
        # пробуем по очереди все найденные карточки, пока не добудем фото
        for purl in product_urls:
            html = http_get(purl)
            pause()
            if not html:
                continue
            photo_url = extract_main_image_from_product(html)
            if photo_url:
                break

        if not photo_url:
            print(f"[skip] {art}: фото не найдено в карточке")
            continue

        # записываем <picture> в оффер
        prev = offer.findtext("picture") or ""
        if prev.strip() != photo_url:
            set_offer_picture(offer, photo_url)
            updated_cnt += 1

        found_cnt += 1
        print(f"[ok]   {art} -> {photo_url}")

    if updated_cnt > 0:
        # сохраним YML с исходной кодировкой
        tree.write(YML_PATH, encoding=ENC, xml_declaration=True)
        print(f"Готово: найдено ссылок={found_cnt}, обновлено офферов={updated_cnt}.")
    else:
        print("Нет ни одной найденной ссылки на фото, выходим без изменений.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
