# scripts/scrape_copyline_category.py
# -*- coding: utf-8 -*-
"""
Назначение:
  Спарсить конкретную категорию copyline.kz:
    - собрать ссылки на товары ТОЛЬКО из плиток этой категории;
    - зайти в каждую карточку и вытащить: name, price, vendorCode (артикул),
      picture, description;
    - сформировать YML (XML) как в alstyle.yml, но для Copyline.

Как запускать (в CI или локально):
  CATEGORY_URL="https://copyline.kz/goods/toner-cartridges-brother.html" \
  OUT_FILE="docs/copyline.yml" \
  OUTPUT_ENCODING="windows-1251" \
  REQUEST_DELAY_MS=700 PAGE_TIMEOUT_S=30 MAX_PAGES=200 MIN_BYTES=1500 \
  python scripts/scrape_copyline_category.py
"""

import os
import re
import time
import html
import hashlib
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from xml.sax.saxutils import escape as xml_escape

# --------------------------- Конфиг из переменных окружения ------------------
BASE_URL = "https://copyline.kz"
CATEGORY_URL = os.getenv("CATEGORY_URL", "").strip()
OUT_FILE = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "700"))
PAGE_TIMEOUT_S = int(os.getenv("PAGE_TIMEOUT_S", "30"))
MIN_BYTES = int(os.getenv("MIN_BYTES", "1500"))

# Лимит длины описания (Satu спокойно переваривает до нескольких тысяч символов)
DESC_MAX_LEN = 4000

# --------------------- Сессия requests с ретраями и таймаутом ----------------
def make_session() -> requests.Session:
    """
    Создаёт requests-сессию с:
      - автоматическими ретраями на временные ошибки;
      - разумными таймаутами по умолчанию.
    """
    sess = requests.Session()
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; CopylineScraper/1.0)"
    })
    return sess

session = make_session()

def get(url: str) -> Optional[str]:
    """
    GET-запрос с таймаутом, проверкой минимального размера ответа и паузой между запросами.
    Возвращает текст страницы или None.
    """
    try:
        resp = session.get(url, timeout=PAGE_TIMEOUT_S)
        if resp.status_code != 200:
            return None
        content = resp.content
        if content is None or len(content) < MIN_BYTES:
            return None
        # copyline.kz отдаёт HTML как UTF-8; доверимся requests.text (учитывает headers/charset)
        text = resp.text
        return text
    except requests.RequestException:
        return None
    finally:
        # пауза между запросами, чтобы не спамить
        time.sleep(REQUEST_DELAY_MS / 1000.0)

# ------------------------------- Вспомогалки ---------------------------------
def clean_text(s: str) -> str:
    """
    Универсальная зачистка пробелов/переводов строк.
    """
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s, flags=re.S)
    return s.strip()

def to_abs_url(src: str, base: str = BASE_URL) -> str:
    """
    Превращает относительную ссылку/картинку в абсолютную.
    """
    return urljoin(base, src)

def hash_id(s: str) -> str:
    """
    Хеш для стабильного offer id (добавка к slug).
    """
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:8]

# --------------------- Парсинг: плитки товаров в категории -------------------
def extract_product_tiles(doc: BeautifulSoup) -> List[Dict]:
    """
    Ищем ТОЛЬКО товары с плиток категории.
    Надёжная эвристика для copyline.kz: миниатюры лежат под
    /components/com_jshopping/files/img_products/
    Берём <img ...> и их родительские <a href="/goods/...html">.

    Возвращаем список словарей c полями:
      - url (страница товара)
      - name (если нашли на плитке)
      - price (если нашли на плитке; число или None)
      - picture (миниатюра с плитки; абсолютная ссылка)
    """
    tiles = []
    seen = set()

    # 1) Берём все IMG миниатюр товаров в центральной области
    for img in doc.select('img[src*="/components/com_jshopping/files/img_products/"]'):
        a = img.find_parent("a", href=True)
        if not a:
            continue
        href = a["href"]
        if not href or "/goods/" not in href:
            continue
        prod_url = to_abs_url(href)

        if prod_url in seen:
            continue
        seen.add(prod_url)

        picture = to_abs_url(img.get("src", ""))

        # Пытаемся вытянуть имя/цену из той же плитки (соседние элементы)
        name = None
        price = None

        # Имя часто бывает в alt/ title картинки или внутри подписи плитки
        name_candidates = [
            img.get("alt"),
            img.get("title"),
        ]
        if not name:
            # Часто название рядом/ниже в ссылке или div с классом name/title
            title_node = a.find_next(string=True)
            if title_node and isinstance(title_node, str):
                name_candidates.append(title_node)

        # Чистим кандидатов и берём первый осмысленный
        for c in name_candidates:
            if c:
                c_clean = clean_text(str(c))
                if len(c_clean) > 3:
                    name = c_clean
                    break

        # Цена — ищем ближайший текст, похожий на число
        # Пробуем найти в пределах карточки-родителя
        card = a
        for _ in range(3):
            if card and card.parent:
                card = card.parent
        if card:
            price_text = card.get_text(separator=" ", strip=True)
            m = re.search(r"(\d[\d\s]{1,12})(?:\s*(?:₸|KZT|тг|тенге))?", price_text)
            if m:
                digits = re.sub(r"\D", "", m.group(1))
                if digits:
                    try:
                        price = int(digits)
                    except ValueError:
                        price = None

        tiles.append({
            "url": prod_url,
            "name": name,
            "price": price,
            "picture": picture,
        })

    return tiles

# --------------------- Парсинг карточки: артикул/описание --------------------
# Готовим регулярки один раз. ВАЖНО: не передавать потом дополнительные flags — иначе будет ошибка.
RE_ART_LABEL = re.compile(r"\bАртикул\b", re.I)
RE_ART_INLINE = re.compile(r"Артикул\s*[:№]?\s*([A-Za-zА-Яа-я0-9\-\_/\. ]{2,40})", re.I)

def extract_vendor_code(soup: BeautifulSoup) -> Optional[str]:
    """
    Строгий поиск артикула внутри карточки:
      1) находим узел, где есть слово "Артикул" — берём соседний/следующий текст;
      2) fallback: ищем по всей карточке по inline-выражению "Артикул: ...".
    """
    # Вариант 1: таблицы характеристик, списки, div-лейблы
    # Ищем любой узел, который текстом содержит "Артикул"
    for node in soup.find_all(string=RE_ART_LABEL):
        # Пытаемся достать значение из ближайших соседей
        parent = node.parent
        if parent:
            # Случай: "Артикул" в <td>, значение в соседнем <td>
            if parent.name == "td":
                sib = parent.find_next_sibling("td")
                if sib:
                    val = clean_text(sib.get_text(" ", strip=True))
                    if 2 <= len(val) <= 40:
                        return val
            # Случай: "Артикул: 12345" прямо в одном узле
            inline = clean_text(parent.get_text(" ", strip=True))
            m = RE_ART_INLINE.search(inline)
            if m:
                val = clean_text(m.group(1))
                if 2 <= len(val) <= 40:
                    return val

            # Случай: label + value в соседних span/div
            for sib in parent.find_all_next(limit=2):
                txt = clean_text(sib.get_text(" ", strip=True))
                if txt and len(txt) <= 40 and not RE_ART_LABEL.search(txt):
                    # отсечём очевидный мусор
                    if re.search(r"[0-9A-Za-zА-Яа-я]", txt):
                        return txt

    # Вариант 2: прямой inline-поиск по всей странице
    all_text = soup.get_text(" ", strip=True)
    m = RE_ART_INLINE.search(all_text)
    if m:
        return clean_text(m.group(1))

    return None

def extract_description(soup: BeautifulSoup) -> Optional[str]:
    """
    Достаём описание карточки товара:
      - приоритет: itemprop="description"
      - далее: div с классами/id, содержащими 'description'
      - fallback: og:description, meta name=description
    Возвращаем ЧИСТЫЙ текст (без HTML), обрезанный по лимиту.
    """
    # 1) itemprop="description"
    tag = soup.find(attrs={"itemprop": "description"})
    if tag:
        txt = clean_text(tag.get_text(" ", strip=True))
        if txt and len(txt) > 10:
            return txt[:DESC_MAX_LEN]

    # 2) любые блоки с "description" в id/class
    cand_blocks = []
    cand_blocks += soup.select('[id*="descr"], [class*="descr"], [id*="description"], [class*="description"]')
    for block in cand_blocks:
        txt = clean_text(block.get_text(" ", strip=True))
        if txt and len(txt) > 10:
            return txt[:DESC_MAX_LEN]

    # 3) meta og:description / meta name=description
    for sel in [
        'meta[property="og:description"]',
        'meta[name="description"]',
    ]:
        meta = soup.select_one(sel)
        if meta and meta.get("content"):
            txt = clean_text(meta["content"])
            if txt and len(txt) > 10:
                return txt[:DESC_MAX_LEN]

    return None

# -------------------------- Парсинг карточки товара --------------------------
def scrape_product(url: str, fallback_name: Optional[str], fallback_price: Optional[int], fallback_picture: Optional[str]) -> Optional[Dict]:
    """
    Заходит в карточку товара и собирает финальные поля.
    Если каких-то данных нет в карточке — используем fallback из плитки.
    """
    html_text = get(url)
    if not html_text:
        return None
    soup = BeautifulSoup(html_text, "lxml")

    # name
    name = None
    # Часто заголовок в <h1>, иногда в .product_title и т.п.
    h1 = soup.find(["h1", "h2"])
    if h1:
        name = clean_text(h1.get_text(" ", strip=True))
    if not name:
        # Падение к <title>, но чистим бренд/хвосты
        t = soup.title.string if soup.title and soup.title.string else ""
        name = clean_text(t.split("|")[0])
    if not name:
        name = fallback_name or ""

    # price
    price = None
    # Ищем число поближе к слову "Цена" или валюте
    price_text = soup.get_text(" ", strip=True)
    m = re.search(r"Цена[^0-9]{0,10}(\d[\d\s]{1,12})", price_text, flags=re.I)
    if not m:
        m = re.search(r"(\d[\d\s]{1,12})(?:\s*(?:₸|KZT|тг|тенге))", price_text, flags=re.I)
    if m:
        digits = re.sub(r"\D", "", m.group(1))
        if digits:
            try:
                price = int(digits)
            except ValueError:
                price = None
    if price is None:
        price = fallback_price

    # picture (если есть основная в карточке — берём её; иначе — fallback с плитки)
    picture = fallback_picture
    main_img = soup.find("img", attrs={"itemprop": "image"})
    if main_img and main_img.get("src"):
        picture = to_abs_url(main_img["src"])

    # vendorCode (артикул) — строго из карточки
    vendor = extract_vendor_code(soup)

    # description — строго из карточки, с fallback на meta
    description = extract_description(soup)

    return {
        "url": url,
        "name": name,
        "price": price,
        "picture": picture,
        "vendorCode": vendor,
        "description": description,
    }

# ------------------------------- Запись YML -----------------------------------
def to_offer_id(slug: str, url: str) -> str:
    """
    Генерируем детерминированный offer id: copyline:<slug>:<hash8>
    """
    return f"copyline:{slug}:{hash_id(url)}"

def slug_from_url(url: str) -> str:
    """
    Строим "человеческий" slug из URL товара.
    """
    path = urlparse(url).path
    base = path.rstrip("/").split("/")[-1].replace(".html", "")
    return base or "item"

def xml_field(tag: str, value: Optional[str]) -> str:
    """
    Безопасно пишет XML-элемент (пропускает пустые значения).
    """
    if value is None:
        return ""
    return f"<{tag}>{xml_escape(value)}</{tag}>"

def write_yml(category_name: str, category_id: str, items: List[Dict], out_path: str, encoding: str):
    """
    Формирует и пишет XML в файл.
    Поля на выходе в каждом <offer>:
      - id, available/in_stock
      - name
      - price
      - currencyId
      - categoryId
      - url
      - picture
      - vendorCode
      - description
      - quantity_in_stock, stock_quantity, quantity (фиктивные 1 для валидности)
    """
    head = (
        f"<?xml version='1.0' encoding='{encoding}'?>\n"
        "<yml_catalog><shop>"
        "<name>copyline</name>"
        "<currencies><currency id=\"KZT\" rate=\"1\" /></currencies>"
        "<categories>"
        "<category id=\"9300000\">Copyline</category>"
        f"<category id=\"{xml_escape(category_id)}\" parentId=\"9300000\">{xml_escape(category_name)}</category>"
        "</categories><offers>"
    )

    body_parts = []
    for it in items:
        # Обязательные поля и валидация
        name = clean_text(it.get("name") or "")
        if not name:
            # Без имени оффер невалиден — пропускаем
            continue
        price = it.get("price")
        try:
            price_val = int(price) if price is not None else None
        except Exception:
            price_val = None

        # Заполняем оффер
        slug = slug_from_url(it["url"])
        offer_id = to_offer_id(slug, it["url"])

        offer_xml = []
        offer_xml.append(f"<offer id=\"{xml_escape(offer_id)}\" available=\"true\" in_stock=\"true\">")
        offer_xml.append(xml_field("name", name))
        if price_val is not None and price_val >= 1:
            offer_xml.append(xml_field("price", str(price_val)))
        else:
            # Если цены нет — Satu ругается; пока просто не пишем <price>
            pass
        offer_xml.append(xml_field("currencyId", "KZT"))
        offer_xml.append(xml_field("categoryId", category_id))
        offer_xml.append(xml_field("url", it["url"]))
        if it.get("picture"):
            offer_xml.append(xml_field("picture", it["picture"]))
        if it.get("vendorCode"):
            offer_xml.append(xml_field("vendorCode", it["vendorCode"]))
        if it.get("description"):
            # Экранируем, убираем слишком длинные хвосты
            desc = it["description"][:DESC_MAX_LEN]
            offer_xml.append(xml_field("description", desc))

        # фиктивные остатки (как в ваших примерах)
        offer_xml.append(xml_field("quantity_in_stock", "1"))
        offer_xml.append(xml_field("stock_quantity", "1"))
        offer_xml.append(xml_field("quantity", "1"))
        offer_xml.append("</offer>")
        body_parts.append("".join(offer_xml))

    tail = "</offers></shop></yml_catalog>"

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding=encoding, errors="ignore") as f:
        f.write(head + "".join(body_parts) + tail)

# --------------------------------- main() ------------------------------------
def main():
    # 0) Проверяем обязательный параметр
    if not CATEGORY_URL:
        raise SystemExit("ERROR: CATEGORY_URL не задан")

    # 1) Грузим страницу категории
    html_cat = get(CATEGORY_URL)
    if not html_cat:
        raise SystemExit(f"ERROR: Не удалось открыть категорию: {CATEGORY_URL}")

    soup_cat = BeautifulSoup(html_cat, "lxml")

    # Название категории — для YML
    cat_name = "Категория"
    h1 = soup_cat.find(["h1", "h2"])
    if h1:
        cat_name = clean_text(h1.get_text(" ", strip=True)) or cat_name

    # Простая эвристика id для категории (можете зашить свой постоянный)
    category_id = "9402524"

    # 2) Извлекаем плитки товаров (ТОЛЬКО товары)
    tiles = extract_product_tiles(soup_cat)

    # Убираем дубликаты по URL
    uniq = {}
    for t in tiles:
        uniq[t["url"]] = t
    tiles = list(uniq.values())

    print(f"[info] Найдено товаров в категории (по плиткам): {len(tiles)}")

    # 3) Скрапим карточки
    results: List[Dict] = []
    bad_vendor_urls: List[str] = []

    for idx, t in enumerate(tiles, 1):
        data = scrape_product(
            url=t["url"],
            fallback_name=t.get("name"),
            fallback_price=t.get("price"),
            fallback_picture=t.get("picture"),
        )
        if not data:
            print(f"[err] {idx:03d}/{len(tiles)} | {t['url']} | не удалось получить карточку")
            continue

        # SKU строго обязателен: если нет — фиксируем для отчёта, но НЕ валим весь процесс
        sku = data.get("vendorCode")
        if not sku:
            bad_vendor_urls.append(t["url"])
        else:
            print(f"[ok]  {idx:03d}/{len(tiles)} | SKU={sku} | {t['url']}")
        results.append(data)

    if bad_vendor_urls:
        # Просто предупреждаем, но файл всё равно пишем
        print("\n[warn] Не найден артикул в карточках (строгий режим):")
        for u in bad_vendor_urls:
            print("  -", u)

    # 4) Пишем YML
    write_yml(
        category_name=cat_name,
        category_id=category_id,
        items=results,
        out_path=OUT_FILE,
        encoding=OUTPUT_ENCODING,
    )
    print(f"\n[done] Записано: {OUT_FILE} (encoding={OUTPUT_ENCODING})")

if __name__ == "__main__":
    main()
