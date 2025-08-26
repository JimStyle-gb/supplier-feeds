# -*- coding: utf-8 -*-
"""
Парсер категории copyline.kz → YML для SATU.
— Идём только по реальной пагинации (rel="next"/кнопка "Следующая").
— Карточки берём из основной сетки категории (ссылки вида /goods/*.html).
— Для каждого товара заходим в карточку и тянем description, vendorCode (Артикул),
  приоритетное основное изображение и, при необходимости, корректируем картинку
  thumb_ → full_. Также определяем vendor (бренд) из имени.
— В YML пишем: name, price, currencyId, categoryId, url, picture, vendorCode, vendor, description,
  и складные теги (quantity_*), чтобы CMS не ругалась.
"""

from __future__ import annotations
import os, re, io, time, random, hashlib
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup, Tag
from xml.etree.ElementTree import Element, SubElement, ElementTree

# =======================
# НАСТРОЙКИ / ОКРУЖЕНИЕ
# =======================
BASE_URL         = os.getenv("BASE_URL", "https://copyline.kz").rstrip("/")
CATEGORY_URL     = os.getenv("CATEGORY_URL", "").strip()  # ОБЯЗАТЕЛЬНО!
OUT_FILE         = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC              = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "700"))   # пауза между запросами
PAGE_TIMEOUT_S   = int(os.getenv("PAGE_TIMEOUT_S", "30"))      # timeout на запрос
MAX_PAGES        = int(os.getenv("MAX_PAGES", "200"))          # защита от циклов
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))         # фильтр "битых" миниатюр

UA_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36")
}

# известные бренды (для <vendor>)
KNOWN_BRANDS = [
    "Brother","HP","Hewlett Packard","Canon","Kyocera","Samsung","Xerox","Ricoh",
    "Konica Minolta","Pantum","Sharp","Epson","Lexmark","OKI","Dell","Toshiba"
]

# =======================
# УТИЛИТЫ
# =======================
def norm(s: Optional[str]) -> str:
    """Схлопываем пробелы и обрезаем края."""
    return re.sub(r"\s+", " ", (s or "").strip())

def ensure_dir_for(path: str):
    """Создаём директорию под выходной файл."""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def sleep_jitter(ms: int):
    """Пауза между запросами с небольшим джиттером."""
    time.sleep(max(ms, 0) / 1000.0 + random.uniform(0, 0.2))

def get_soup(url: str) -> BeautifulSoup:
    """GET страница → BS4 soup, ошибки пробрасываем."""
    r = requests.get(url, headers=UA_HEADERS, timeout=PAGE_TIMEOUT_S)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def parse_price(text: str) -> Optional[float]:
    """
    Корректный разбор цены.
    Примеры: '2 273 ₸', '2,273 тг', '2 273,00', '2273', '2 273.50'
    """
    if not text:
        return None
    t = norm(text)
    # вырезаем валюту/слова и НЕ нуллим пробелы внутри числа
    t = (t.replace("₸", "").replace("тг", "").replace("ТГ", "")
           .replace("\u00a0", " "))  # неразрывные пробелы -> обычные
    t = t.replace(",", ".")         # десятичную запятую -> точка
    t = t.replace(" ", "")          # <--- ВАЖНО: убираем пробелы-тысячники
    m = re.search(r"\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        val = float(m.group(0))
        return val if val > 0 else None
    except:
        return None

def slugify(s: str) -> str:
    """Грубо нормализуем под id оффера (латиница/цифры/дефис)."""
    s = norm(s).lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")

def full_from_thumb(src: str) -> str:
    """thumb_* → full_* (если встречается)."""
    if not src:
        return src
    return src.replace("/thumb_", "/full_").replace("thumb_", "full_")

def looks_like_placeholder(url: str) -> bool:
    """Фильтруем очевидные заглушки."""
    if not url:
        return True
    name = os.path.basename(urlparse(url).path).lower()
    if "thumb_black-toner" in name or "noimage" in name:
        return True
    return False

def find_main_grid(soup: BeautifulSoup) -> Tag | None:
    """Ищем контейнер основной сетки товаров."""
    for sel in [
        ".jshop_list_product",
        ".list_product",
        "div#comjshop",
        "div.jshop",
        "div.component",
        "main",
        "div.content",
    ]:
        tag = soup.select_one(sel)
        if tag:
            return tag
    return soup

def extract_cards(root: Tag) -> List[Tag]:
    """
    Собираем карточки товаров. Критерий — наличие ссылки на /goods/*.html.
    Это отсекает боковые "похожие/популярные".
    """
    cards = []
    possible = root.select(".product, .jshop_product, .productitem, li.product, .prod, .block_product")
    seen = set()
    for c in possible:
        a = c.select_one('a[href*="/goods/"][href$=".html"]')
        if not a:
            continue
        href_abs = urljoin(BASE_URL + "/", a.get("href", ""))
        if href_abs in seen:
            continue
        seen.add(href_abs)
        cards.append(c)

    if not cards:
        # fallback — минимум по ссылкам
        by_href = {}
        for a in root.select('a[href*="/goods/"][href$=".html"]'):
            href_abs = urljoin(BASE_URL + "/", a.get("href", ""))
            by_href.setdefault(href_abs, a)
        cards = [v.parent or root for v in by_href.values()]
    return cards

def detect_vendor(name: str) -> Optional[str]:
    """Пытаемся вычленить бренд из имени по списку известных брендов."""
    low = name.lower()
    for b in KNOWN_BRANDS:
        if b.lower() in low:
            return b
    return None

def card_to_basic_item(card: Tag) -> Optional[Dict[str, Any]]:
    """
    Достаём базовые поля из карточки категории: name, price, url, picture (thumb→full).
    Возвращаем None, если нет обязательного (name/price/url).
    """
    a = card.select_one('a[href*="/goods/"][href$=".html"]')
    if not a:
        return None
    url = urljoin(BASE_URL + "/", a.get("href", ""))

    name = norm(a.get("title")) or norm(a.get_text())
    if not name:
        h = card.select_one("h3, h4, .name, .product_name")
        name = norm(h.get_text()) if h else ""
    if not name:
        return None

    price_text = ""
    for sel in [".price", ".jshop_price", ".product_price", ".price_value", ".jshop_price_value"]:
        t = card.select_one(sel)
        if t:
            price_text = norm(t.get_text())
            if price_text:
                break
    price = parse_price(price_text)
    if price is None:
        return None

    pic = ""
    img = card.select_one("img")
    if img:
        src = img.get("data-src") or img.get("src") or ""
        src = urljoin(BASE_URL + "/", src)
        if src:
            cand = full_from_thumb(src)
            if not looks_like_placeholder(cand):
                pic = cand

    return {
        "name": name,
        "price": round(float(price), 2),
        "url": url,
        "picture": pic,
    }

def fetch_product_details(url: str) -> Dict[str, str]:
    """
    Заходим в карточку товара и тянем:
      - описание (description)
      - артикул (vendorCode)
      - основную картинку (main image)
    Всё опционально — возвращаем только найденное.
    """
    details: Dict[str, str] = {}
    soup = get_soup(url)

    # описание
    desc_node = (
        soup.select_one('[itemprop="description"]')
        or soup.select_one(".jshop_prod_description")
        or soup.select_one(".product_description")
        or soup.select_one("#description")
        or soup.select_one(".description")
    )
    if desc_node:
        txt = norm(desc_node.get_text("\n"))
        if txt:
            details["description"] = txt

    # артикул (SKU)
    sku_node = (
        soup.select_one('[itemprop="sku"]')
        or soup.select_one(".prod_code")
        or soup.select_one(".jshop_code")
        or soup.find(string=re.compile(r"Артикул", re.I))
    )
    if sku_node:
        if isinstance(sku_node, Tag):
            code = norm(sku_node.get_text())
        else:
            # текстовая нода "Артикул: 12345"
            line = norm(str(sku_node))
            m = re.search(r"Артикул[^0-9A-Za-zА-Яа-я]*([0-9A-Za-z\-_.]+)", line, re.I)
            code = m.group(1) if m else ""
        if code:
            details["vendorCode"] = code

    # основная картинка из карточки
    main_img = (
        soup.select_one('img#main_image')
        or soup.select_one('img[itemprop="image"]')
        or soup.select_one('#product_full img')
        or soup.select_one('.productfull img')
    )
    if main_img:
        src = main_img.get("data-src") or main_img.get("src")
        if src:
            src = urljoin(BASE_URL + "/", src)
            if not looks_like_placeholder(src):
                details["picture"] = src

    return details

def find_next_link(soup: BeautifulSoup, current_url: str) -> Optional[str]:
    """Ищем ссылку на следующую страницу (rel=next или кнопка пагинации)."""
    ln = soup.select_one('link[rel="next"]')
    if ln and ln.get("href"):
        return urljoin(current_url, ln["href"])

    pag = soup.select_one("ul.pagination, div.pagination, nav.pagination")
    if pag:
        # явная кнопка "следующая"
        a = pag.select_one('a[rel="next"], a.next, a[title*="След"], a:contains("След"), a:contains("»")')
        if a and a.get("href"):
            return urljoin(current_url, a["href"])
        links = [x for x in pag.select("a[href]") if x.get("href")]
        for cand in links[::-1]:
            txt = norm(cand.get_text()).lower()
            if "след" in txt or txt.endswith("»"):
                return urljoin(current_url, cand["href"])
    return None

# =======================
# СБОРКА YML
# =======================
ROOT_CAT_ID = "9300000"

def hash_int(s): 
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)

def cat_id_for(name: str) -> str:
    return str(9300001 + (hash_int(name.lower()) % 400000))

def offer_id(it: Dict[str, Any]) -> str:
    base = slugify(it.get("name", ""))
    h = hashlib.md5((it.get("name","") + "|" + it.get("url","")).encode("utf-8")).hexdigest()[:8]
    return f"copyline:{base}:{h}"

def build_yml(category_name: str, items: List[Dict[str, Any]]) -> bytes:
    root = Element("yml_catalog")
    shop = SubElement(root, "shop")
    SubElement(shop, "name").text = "copyline"
    curr = SubElement(shop, "currencies")
    SubElement(curr, "currency", {"id":"KZT", "rate":"1"})

    xml_cats = SubElement(shop, "categories")
    SubElement(xml_cats, "category", {"id": ROOT_CAT_ID}).text = "Copyline"
    cat_name = norm(category_name) or "Категория"
    cat_id = cat_id_for(cat_name)
    SubElement(xml_cats, "category", {"id": cat_id, "parentId": ROOT_CAT_ID}).text = cat_name

    offers = SubElement(shop, "offers")
    used = set()
    for it in items:
        oid = offer_id(it)
        if oid in used:
            extra = hashlib.md5((it.get("url","")+it.get("name","")).encode("utf-8")).hexdigest()[:6]
            k = 2
            while f"{oid}-{extra}-{k}" in used:
                k += 1
            oid = f"{oid}-{extra}-{k}"
        used.add(oid)

        o = SubElement(offers, "offer", {"id": oid, "available":"true", "in_stock":"true"})
        SubElement(o, "name").text = it["name"]

        # цена — строго > 0
        price_str = f'{it["price"]:.2f}'.rstrip("0").rstrip(".")
        SubElement(o, "price").text = price_str
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cat_id
        SubElement(o, "url").text = it["url"]

        if it.get("picture"):
            SubElement(o, "picture").text = it["picture"]

        # добавляем расширенные поля, если есть
        if it.get("vendorCode"):
            SubElement(o, "vendorCode").text = it["vendorCode"]
        if it.get("vendor"):
            SubElement(o, "vendor").text = it["vendor"]
        if it.get("description"):
            SubElement(o, "description").text = it["description"]

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

        if not category_name:
            h1 = soup.select_one("h1") or soup.select_one(".category_name, .page-title, .cat-name")
            category_name = norm(h1.get_text()) if h1 else "Copyline категория"

        root = find_main_grid(soup)
        cards = extract_cards(root)

        page_items = []
        for c in cards:
            basic = card_to_basic_item(c)
            if not basic:
                continue
            if basic["url"] in seen_urls:
                continue
            if "/goods/" not in urlparse(basic["url"]).path:
                continue

            # детально заходим в карточку (описание, артикул, картинка)
            try:
                sleep_jitter(REQUEST_DELAY_MS)  # щадим сайт
                det = fetch_product_details(basic["url"])
                basic.update(det)
            except Exception as e:
                print(f"[warn] details failed for {basic['url']}: {e}")

            # бренд по имени
            vend = detect_vendor(basic["name"])
            if vend and not basic.get("vendor"):
                basic["vendor"] = vend

            seen_urls.add(basic["url"])
            page_items.append(basic)

        print(f"[page {pages_done}] items found here: {len(page_items)}")
        all_items.extend(page_items)

        next_url = find_next_link(soup, page_url)
        if next_url and next_url != page_url:
            page_url = next_url
            sleep_jitter(REQUEST_DELAY_MS)
            continue
        break

    print(f"[total] unique items: {len(all_items)}")
    if not all_items:
        raise SystemExit("ERROR: не найдено ни одной позиции. Проверь CATEGORY_URL/верстку страницы.")

    # выкидываем потенциально кривые позиции (без имени/цены)
    all_items = [x for x in all_items if x.get("name") and x.get("price") and x["price"] > 0]

    yml = build_yml(category_name, all_items)
    with open(OUT_FILE, "wb") as f:
        f.write(yml)

    print(f"[OK] wrote YML → {OUT_FILE} (encoding={ENC})")

if __name__ == "__main__":
    main()
