# -*- coding: utf-8 -*-
"""
Скрипт парсит заданную категорию copyline.kz и формирует YML с товарами.
Основные принципы:
- Не генерируем пагинацию искусственно. Идём только по реальным ссылкам next/rel=next.
- Берём товары только из основного контейнера списка категории (фильтр по /goods/*.html).
- Игнорируем боковые/нижние модули с "популярными/похожими".
- Фото: берём <img> превью (thumb_*.jpg) и превращаем в full_*.jpg.
- В YML включаем только товары, у которых есть имя и валидная цена > 0.
- Код снабжён подробными комментариями по блокам (как ты просил).
"""

from __future__ import annotations
import os, re, io, time, random, hashlib
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup, Tag
from xml.etree.ElementTree import Element, SubElement, ElementTree

# =======================
# ПАРАМЕТРЫ / ОКРУЖЕНИЕ
# =======================
BASE_URL         = os.getenv("BASE_URL", "https://copyline.kz").rstrip("/")
CATEGORY_URL     = os.getenv("CATEGORY_URL", "").strip()  # ОБЯЗАТЕЛЬНО!
OUT_FILE         = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC              = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "700"))   # пауза между запросами
PAGE_TIMEOUT_S   = int(os.getenv("PAGE_TIMEOUT_S", "30"))      # timeout на страницу
MAX_PAGES        = int(os.getenv("MAX_PAGES", "200"))          # страховка от бескон. цикла
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))         # фильтр для "битых" миниатюр

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36"
}

# =======================
# УТИЛИТЫ
# =======================
def norm(s: Optional[str]) -> str:
    """Нормализация строки: обрезаем и схлопываем пробелы."""
    return re.sub(r"\s+", " ", (s or "").strip())

def ensure_dir_for(path: str):
    """Создаём директорию для выходного файла при необходимости."""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def sleep_jitter(ms: int):
    """Пауза между запросами с небольшим джиттером, чтобы не спамить сервер."""
    base = max(ms, 0) / 1000.0
    time.sleep(base + random.uniform(0, 0.25))

def get_soup(url: str) -> BeautifulSoup:
    """
    Загружаем страницу и отдаём BS4-soup.
    Ошибки пробрасываем — пусть Action падает явно, если сайт недоступен.
    """
    r = requests.get(url, headers=UA_HEADERS, timeout=PAGE_TIMEOUT_S)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def parse_price(text: str) -> Optional[float]:
    """
    Достаём число из строки цены.
    Примеры входа: '2 273 ₸', '2,273 тг', '2273', '2 273,00 ₸'
    Возвращаем float или None, если цифр нет.
    """
    if not text:
        return None
    # убираем пробелы-разделители тысяч и валюту
    t = norm(text).replace("₸", "").replace("тг", "").replace("ТГ", "")
    # десятичную запятую превращаем в точку
    t = t.replace(",", ".")
    # оставляем цифры и одну точку
    m = re.findall(r"\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        val = float(m[0])
        return val if val > 0 else None
    except:
        return None

def slugify(s: str) -> str:
    """Грубая транслитерация/слаг для id — только латиница, цифры и дефисы."""
    s = norm(s).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")

def full_from_thumb(src: str) -> str:
    """
    thumb_* → full_* для основных картинок copyline.
    Если паттерн не подходит — вернём как есть.
    """
    if not src:
        return src
    # типичный случай: .../thumb_TN-1075.jpg → .../full_TN-1075.jpg
    return src.replace("/thumb_", "/full_").replace("thumb_", "full_")

def looks_like_placeholder(url: str) -> bool:
    """
    Отсекаем очевидные заглушки, чтобы не засорять фид.
    """
    if not url:
        return True
    name = os.path.basename(urlparse(url).path).lower()
    # частая заглушка на сайте:
    if "thumb_black-toner" in name or "noimage" in name:
        return True
    return False

def find_main_grid(soup: BeautifulSoup) -> Tag | None:
    """
    Пытаемся найти основной контейнер со списком товаров категории.
    Возвращаем корневой Tag для поиска карточек.
    """
    # На сайтах на JoomShopping часто встречается подобная структура.
    candidates = [
        ".jshop_list_product",     # частый контейнер списка товаров
        ".list_product",           # fallback
        "div#comjshop",            # компонент магазина
        "div.jshop",               # общий контейнер магазина
        "div.component",           # ещё один общий контейнер
        "main",                    # на всякий
        "div.content",             # общий content
    ]
    for sel in candidates:
        tag = soup.select_one(sel)
        if tag:
            return tag
    return soup  # совсем крайний случай: ищем во всём документе

def extract_cards(root: Tag) -> List[Tag]:
    """
    Возвращаем список карточек товаров в пределах основного контейнера.
    Стараемся не захватывать боковые модули.
    Критерий: внутри карточки должен быть <a href="/goods/....html">.
    """
    cards = []
    # возможные классы карточек
    possible = root.select(
        ".product, .jshop_product, .productitem, li.product, .prod, .block_product"
    )
    seen = set()
    for c in possible:
        a = c.select_one('a[href*="/goods/"][href$=".html"]')
        if not a:
            continue
        href = a.get("href", "")
        href_abs = urljoin(BASE_URL + "/", href)
        if href_abs in seen:
            continue
        seen.add(href_abs)
        cards.append(c)

    # Fallback: если ничего не нашли по карточкам — соберём ссылки прямо из root
    if not cards:
        links = root.select('a[href*="/goods/"][href$=".html"]')
        # сгруппируем по href, чтобы не дублировать
        by_href = {}
        for a in links:
            href_abs = urljoin(BASE_URL + "/", a.get("href", ""))
            by_href.setdefault(href_abs, a)
        # завернём каждую ссылку в "виртуальную" карточку = её родителя
        cards = [v.parent or root for v in by_href.values()]

    return cards

def card_to_item(card: Tag) -> Optional[Dict[str, Any]]:
    """
    Парсим одну карточку: name, price, url, picture.
    Возвращаем None, если обязательных данных нет (имя/цена/ссылка).
    """
    a = card.select_one('a[href*="/goods/"][href$=".html"]')
    if not a:
        return None
    url = urljoin(BASE_URL + "/", a.get("href", ""))

    # Имя: текст ссылки или заголовка в карточке
    name = norm(a.get("title")) or norm(a.get_text())
    if not name:
        # иногда имя лежит в отдельном заголовке
        h = card.select_one("h3, h4, .name, .product_name")
        name = norm(h.get_text()) if h else ""
    if not name:
        return None

    # Цена: пытаемся из популярных классов
    price_text = ""
    for sel in [".price", ".jshop_price", ".product_price", ".price_value", ".jshop_price_value"]:
        t = card.select_one(sel)
        if t:
            price_text = norm(t.get_text())
            if price_text:
                break
    price = parse_price(price_text)
    if price is None:
        # иногда цена как атрибут, либо в другом месте — пробуем взять любую цифру с валютой рядом
        text = norm(card.get_text(" "))
        # берём "наибольшую" по числу знак-цифр подстроку (грубая эвристика)
        numbers = re.findall(r"\d[\d\s.,]*", text)
        numbers = sorted(numbers, key=lambda s: len(s), reverse=True)
        for cand in numbers:
            price = parse_price(cand)
            if price:
                break
    if price is None or price <= 0:
        return None  # без валидной цены не включаем позицию — это важно для SATU

    # Картинка: <img> внутри карточки/ссылки
    img = card.select_one("img")
    picture = ""
    if img:
        src = img.get("data-src") or img.get("src") or ""
        src = urljoin(BASE_URL + "/", src)
        if src:
            # конвертим превью → full
            pic_full = full_from_thumb(src)
            picture = pic_full if not looks_like_placeholder(pic_full) else ""
    # картинка опциональна; если её нет — пропустим picture

    return {
        "name": name,
        "price": round(float(price), 2),
        "url": url,
        "picture": picture,
    }

def find_next_link(soup: BeautifulSoup, current_url: str) -> Optional[str]:
    """
    Ищем реальную ссылку на следующую страницу.
    Приоритет:
      1) <link rel="next" href="...">
      2) Внутренняя пагинация: ul.pagination / div.pagination — кнопка "Следующая/Вперёд/»"
    Если ничего нет — возвращаем None.
    """
    # 1) rel="next" в <head>
    ln = soup.select_one('link[rel="next"]')
    if ln and ln.get("href"):
        return urljoin(current_url, ln["href"])

    # 2) Ссылки в блоке пагинации
    pag = soup.select_one("ul.pagination, div.pagination, nav.pagination")
    if pag:
        # попытка найти явную "next"
        a = pag.select_one('a[rel="next"], a.next, a[title*="След"], a[title*="Впер"], a:contains("След"), a:contains("Впер"), a:contains("»")')
        if a and a.get("href"):
            return urljoin(current_url, a["href"])

        # как fallback — последняя активная страница + 1
        # (но без синтеза цифр — только если у последнего "a" есть href)
        links = [x for x in pag.select("a[href]") if x.get("href")]
        # ищем ту, у которой текст выглядит как стрелка или больше текущего номера
        for cand in links[::-1]:
            txt = norm(cand.get_text()).lower()
            if "след" in txt or "вперед" in txt or txt.endswith("»"):
                return urljoin(current_url, cand["href"])

    return None

# =======================
# СБОРКА YML
# =======================
ROOT_CAT_ID = "9300000"

def hash_int(s): 
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)

def cat_id_for(name: str) -> str:
    """Детерминированный id категории-потомка от имени."""
    return str(9300001 + (hash_int(name.lower()) % 400000))

def offer_id(it: Dict[str, Any]) -> str:
    """Стабильный id оффера на основе имени; добавляем хеш против коллизий."""
    base = slugify(it.get("name", ""))
    h = hashlib.md5((it.get("name","") + "|" + it.get("url","")).encode("utf-8")).hexdigest()[:8]
    return f"copyline:{base}:{h}"

def build_yml(category_name: str, items: List[Dict[str, Any]]) -> bytes:
    """Формируем YML-XML в байтах (ENC из env)."""
    # Корневые узлы
    root = Element("yml_catalog")
    shop = SubElement(root, "shop")
    SubElement(shop, "name").text = "copyline"
    curr = SubElement(shop, "currencies")
    SubElement(curr, "currency", {"id":"KZT", "rate":"1"})

    # Категории: корневая + текущая категория
    xml_cats = SubElement(shop, "categories")
    SubElement(xml_cats, "category", {"id": ROOT_CAT_ID}).text = "Copyline"
    cat_name = norm(category_name) or "Категория"
    cat_id = cat_id_for(cat_name)
    SubElement(xml_cats, "category", {"id": cat_id, "parentId": ROOT_CAT_ID}).text = cat_name

    # Офферы
    offers = SubElement(shop, "offers")
    used = set()
    for it in items:
        oid = offer_id(it)
        if oid in used:
            # доп. защита от коллизий
            extra = hashlib.md5((it.get("url","")+it.get("name","")).encode("utf-8")).hexdigest()[:6]
            k = 2
            while f"{oid}-{extra}-{k}" in used:
                k += 1
            oid = f"{oid}-{extra}-{k}"
        used.add(oid)

        o = SubElement(offers, "offer", {"id": oid, "available":"true", "in_stock":"true"})
        SubElement(o, "name").text = it["name"]
        SubElement(o, "price").text = f'{it["price"]:.2f}'.rstrip("0").rstrip(".")
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cat_id
        SubElement(o, "url").text = it["url"]

        pic = it.get("picture") or ""
        if pic:
            SubElement(o, "picture").text = pic

        # Склад — чтобы CMS не ругалась на пустые теги
        for tag in ("quantity_in_stock","stock_quantity","quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

# =======================
# MAIN
# =======================
def main():
    if not CATEGORY_URL:
        raise SystemExit("ERROR: CATEGORY_URL не задан")

    ensure_dir_for(OUT_FILE)

    seen_urls = set()
    all_items: List[Dict[str, Any]] = []

    page_url = CATEGORY_URL
    pages_done = 0
    category_name = ""

    while page_url and pages_done < MAX_PAGES:
        pages_done += 1
        print(f"[page {pages_done}] GET {page_url}")
        soup = get_soup(page_url)

        # Название категории (хлебные крошки/заголовок)
        if not category_name:
            h1 = soup.select_one("h1") or soup.select_one(".category_name, .page-title, .cat-name")
            category_name = norm(h1.get_text()) if h1 else "Copyline категория"

        root = find_main_grid(soup)
        cards = extract_cards(root)

        page_items = []
        for c in cards:
            item = card_to_item(c)
            if not item:
                continue
            if item["url"] in seen_urls:
                continue
            # Допфильтр: URL должен быть под /goods/
            if "/goods/" not in urlparse(item["url"]).path:
                continue
            seen_urls.add(item["url"])
            page_items.append(item)

        print(f"[page {pages_done}] items found here: {len(page_items)}")
        all_items.extend(page_items)

        # Ищем реальный next
        next_url = find_next_link(soup, page_url)
        if next_url and next_url != page_url:
            page_url = next_url
            sleep_jitter(REQUEST_DELAY_MS)
            continue

        # нет next — заканчиваем
        break

    # Итог
    print(f"[total] unique items: {len(all_items)}")

    if not all_items:
        raise SystemExit("ERROR: не найдено ни одной позиции. Проверь CATEGORY_URL или селекторы.")

    yml = build_yml(category_name, all_items)
    with open(OUT_FILE, "wb") as f:
        f.write(yml)

    print(f"[OK] wrote YML → {OUT_FILE} (encoding={ENC})")

if __name__ == "__main__":
    main()
