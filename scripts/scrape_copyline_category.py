# scripts/scrape_copyline_category.py
# -*- coding: utf-8 -*-
"""
ЗАДАЧА
------
Собрать товары из одной категории Copyline.kz и выгрузить YML для Сату.
Все ключевые поля (в т.ч. АРТИКУЛ) берём ИСКЛЮЧИТЕЛЬНО из карточек товаров.

КАК РАБОТАЕТ
------------
1) Берём URL категории из переменной окружения CATEGORY_URL.
2) На странице категории собираем ссылки карточек (a[href*="/goods/"]).
   Никаких цен/артикулов с категории не тянем.
3) Для КАЖДОЙ карточки:
   - Название (H1 / .product_name / просто <h1>)
   - Цена (любой элемент с классом, содержащим "price") -> вытаскиваем цифры
   - Фото (img[itemprop="image"] или #main_image_*). Если ссылка содержит
     /thumb_*.jpg — меняем на /full_*.jpg
   - Описание (itemprop="description" / .product_description / .description), текст
   - АРТИКУЛ (СТРОГО в карточке!):
       a) itemprop="sku" (content/text)
       b) типовые селекторы: .prod_code, .product_code, .jshop_code, #product_code
       c) табличные/меточные пары "Артикул", "Код товара", "Модель", "SKU",
          "Код продукта" и т.п. — берём значение из соседней ячейки/спана
   Если артикул не найден — помечаем как ошибку и, по умолчанию, выходим с кодом 1.
4) Собираем YML (root Copyline + категория по заголовку страницы) в кодировке
   windows-1251 и сохраняем в OUT_FILE (по умолчанию: docs/copyline.yml).

ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ (env)
--------------------------
CATEGORY_URL      (обязательно) ссылка на страницу категории
OUT_FILE          путь к итоговому YML (по умолчанию: docs/copyline.yml)
OUTPUT_ENCODING   кодировка файла (по умолчанию: windows-1251)
REQUEST_DELAY_MS  задержка между карточками (по умолчанию: 700)
PAGE_TIMEOUT_S    таймаут HTTP запроса (по умолчанию: 30)
MAX_RETRIES       ретраи для карточки (по умолчанию: 2)
STRICT_VENDORCODE 1|0 — если 1, падать, если для какого-то товара не найден артикул (по умолчанию: 1)

ЗАВИСИМОСТИ
-----------
pip install requests beautifulsoup4

ПРИМЕР ЗАПУСКА
--------------
CATEGORY_URL="https://copyline.kz/goods/toner-cartridges-brother.html" \
OUT_FILE="docs/copyline.yml" \
python scripts/scrape_copyline_category.py
"""
from __future__ import annotations
import os, re, io, time, sys, hashlib
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ---------- Настройки и утилиты ----------
UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

BASE_URL       = "https://copyline.kz"
CATEGORY_URL   = os.getenv("CATEGORY_URL", "").strip()
OUT_FILE       = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC            = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
REQ_DELAY      = int(os.getenv("REQUEST_DELAY_MS", "700")) / 1000.0
TIMEOUT        = int(os.getenv("PAGE_TIMEOUT_S", "30"))
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "2"))
STRICT_VCODE   = int(os.getenv("STRICT_VENDORCODE", "1"))  # 1 = падать если нет артикула

def die(msg: str):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def get(url: str) -> requests.Response:
    """HTTP GET с таймаутом и заголовками."""
    r = requests.get(url, headers=UA_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r

def abs_url(href: str) -> str:
    """Делаем абсолютный URL для относительных ссылок."""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if not href.startswith("/"):
        href = "/" + href
    return BASE_URL.rstrip("/") + href

def norm_text(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def only_digits(s: str) -> Optional[int]:
    ds = re.sub(r"[^\d]", "", s or "")
    return int(ds) if ds else None

# ---------- Парсинг категории: только ссылки карточек ----------
def collect_product_urls(category_url: str) -> List[str]:
    """
    Забираем ВСЕ ссылки вида /goods/*.html со страницы категории.
    Пагинации на этой категории может и не быть — скрипт берёт только то, что есть.
    """
    html = get(category_url).text
    soup = BeautifulSoup(html, "html.parser")

    seen, urls = set(), []
    # Опорный фильтр: любые <a> где href содержит '/goods/' и заканчивается на '.html'
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/goods/" in href and href.endswith(".html"):
            url = abs_url(href)
            if url not in seen:
                seen.add(url)
                urls.append(url)

    if not urls:
        print("[warn] На странице категории не найдено ссылок на товары.")
    print(f"[info] Найдено товаров в категории: {len(urls)}")
    return urls

# ---------- Вспомогательные выдёргиватели полей из карточки ----------
SKU_LABEL_RE = re.compile(r"(артикул|код\s*товара|код продукта|модель|sku)", re.I)

SKU_VALUE_RE = re.compile(r"^[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9._\-/]{0,49}$")

def extract_name(soup: BeautifulSoup) -> str:
    # типичные места с названием
    for sel in ["h1.product_name", "div.jshop_prod_name", "h1"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            return norm_text(el.get_text())
    # запасной вариант: <title>
    if soup.title and soup.title.string:
        return norm_text(soup.title.string)
    return ""

def extract_price(soup: BeautifulSoup) -> Optional[int]:
    # Ищем любой блок с классом, содержащим 'price' (jshop_price / price)
    for el in soup.select('[class*="price"]'):
        txt = norm_text(el.get_text())
        val = only_digits(txt)
        if val:
            return val
    # иногда цена в meta itemprop="price"
    meta = soup.select_one('[itemprop="price"]')
    if meta:
        content = meta.get("content") or meta.get("value") or meta.get_text()
        val = only_digits(content or "")
        if val:
            return val
    return None

def extract_picture(soup: BeautifulSoup) -> Optional[str]:
    # основное фото товара (как в примере с itemprop="image" или id="main_image_XXXX")
    el = soup.select_one('img[itemprop="image"]') or soup.select_one('img[id^="main_image_"]')
    if el and el.get("src"):
        src = abs_url(el["src"])
        # если миниатюра, меняем на full_*.jpg
        src = src.replace("/thumb_", "/full_")
        return src
    # запасной вариант — первая картинка в блоке карточки
    el = soup.select_one("div.product img, div.image img, .product-image img")
    if el and el.get("src"):
        return abs_url(el["src"]).replace("/thumb_", "/full_")
    return None

def _clean_sku_candidate(s: str) -> str:
    # Убираем двоеточия и саму метку, пробелы нормализуем
    s = norm_text(s)
    s = re.sub(SKU_LABEL_RE, "", s, flags=re.I)
    s = s.strip(" :\u00A0")
    return s

def extract_sku(soup: BeautifulSoup) -> Optional[str]:
    """
    СТРОГИЙ поиск артикула внутри карточки.
    Порядок:
      1) itemprop="sku"
      2) типовые селекторы с кодом
      3) табличные/меточные пары с подписями (Артикул/Код товара/…)
    """
    # 1) microdata
    el = soup.select_one('[itemprop="sku"]')
    if el:
        content = el.get("content") or el.get_text()
        cand = _clean_sku_candidate(content or "")
        if SKU_VALUE_RE.fullmatch(cand or ""):
            return cand

    # 2) популярные селекторы с кодом
    for sel in [".prod_code", ".product_code", ".jshop_code", "#product_code", ".product-code", ".code"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            cand = _clean_sku_candidate(el.get_text())
            if SKU_VALUE_RE.fullmatch(cand or ""):
                return cand

    # 3) лейбл + значение (в таблице/спанах)
    #   Ищем элементы с текстом-меткой и пытаемся забрать соседнее значение
    labels = soup.find_all(string=SKU_LABEL_RE)
    for lab in labels:
        # a) если это <td>Артикул</td><td>VALUE</td>
        if lab.parent.name == "td":
            td = lab.parent
            nxt = td.find_next_sibling("td")
            txt = norm_text(nxt.get_text() if nxt else "")
            cand = _clean_sku_candidate(txt)
            if SKU_VALUE_RE.fullmatch(cand or ""):
                return cand
        # b) если это <span>Артикул:</span><span>VALUE</span> / соседний элемент
        par = lab.parent
        if par and par.next_sibling:
            txt = ""
            # сосед может быть NavigableString или тегом
            sib = par.next_sibling
            if hasattr(sib, "get_text"):
                txt = norm_text(sib.get_text())
            else:
                txt = norm_text(str(sib))
            cand = _clean_sku_candidate(txt)
            if SKU_VALUE_RE.fullmatch(cand or ""):
                return cand
        # c) часто значение внутри родителя, но в другом дочернем
        container = lab.find_parent()
        if container:
            # ищем вторую ячейку в ряду
            tds = container.find_all("td")
            if len(tds) >= 2:
                cand = _clean_sku_candidate(tds[1].get_text())
                if SKU_VALUE_RE.fullmatch(cand or ""):
                    return cand

    return None  # НИЧЕГО не нашли в карточке (строгий режим — это ошибка)

def extract_description(soup: BeautifulSoup) -> str:
    for sel in ['[itemprop="description"]', '.product_description', '.description']:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            return norm_text(el.get_text())[:2000]  # ограничим размер
    return ""

# ---------- Парсинг одной карточки с ретраями ----------
def parse_product(url: str) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            html = get(url).text
            soup = BeautifulSoup(html, "html.parser")
            name = extract_name(soup)
            price = extract_price(soup)
            picture = extract_picture(soup)
            sku = extract_sku(soup)  # СТРОГО из карточки
            descr = extract_description(soup)

            return {
                "url": url,
                "name": name,
                "price": price,
                "picture": picture,
                "vendorCode": sku,
                "description": descr,
            }
        except Exception as e:
            last_err = e
            time.sleep(min(2**attempt * 0.5, 6.0))
    # если все попытки упали — пробрасываем последнюю ошибку
    raise last_err

# ---------- Построение YML ----------
ROOT_CAT_ID = "9300000"

def hash_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6], 16)

def cat_id_for(name: str) -> str:
    return str(9400000 + (hash_int(name.lower()) % 500000))

def offer_id_from(it: Dict[str, Any]) -> str:
    """
    Для стабильности используем артикул:
      copyline:{vendorCode_lower}
    Если в какой-то причине артикула нет — хэш по URL.
    """
    sku = (it.get("vendorCode") or "").strip()
    if sku:
        safe = re.sub(r"[^a-z0-9._\-]+", "-", sku.lower())
        return f"copyline:{safe}"
    # fallback (не должно случаться в строгом режиме)
    h = hashlib.md5((it.get("url", "")).encode("utf-8")).hexdigest()[:8]
    return f"copyline:url-{h}"

def build_yml(items: List[Dict[str, Any]], category_name: str) -> bytes:
    cats = {}
    if category_name and category_name.strip().lower() != "copyline":
        cats[category_name] = cat_id_for(category_name)

    root = Element("yml_catalog"); shop = SubElement(root, "shop")
    SubElement(shop, "name").text = "copyline"
    curr = SubElement(shop, "currencies"); SubElement(curr, "currency", {"id": "KZT", "rate": "1"})

    xml_cats = SubElement(shop, "categories")
    SubElement(xml_cats, "category", {"id": ROOT_CAT_ID}).text = "Copyline"
    for nm, cid in cats.items():
        SubElement(xml_cats, "category", {"id": cid, "parentId": ROOT_CAT_ID}).text = nm

    offers = SubElement(shop, "offers")
    used = set()
    for it in items:
        oid = offer_id_from(it)
        if oid in used:
            # крайне маловероятно, но на всякий случай
            i = 2
            while f"{oid}-{i}" in used:
                i += 1
            oid = f"{oid}-{i}"
        used.add(oid)

        cid = ROOT_CAT_ID if not category_name or category_name.strip().lower() == "copyline" \
              else list(cats.values())[0]

        o = SubElement(offers, "offer", {"id": oid, "available": "true", "in_stock": "true"})
        SubElement(o, "name").text = it.get("name") or ""
        # цена обязательна для Сату (>= 0.01). Если не нашли — ставим 0 и Сату сообщит об ошибке.
        if it.get("price") is not None:
            SubElement(o, "price").text = str(it["price"])
        else:
            SubElement(o, "price").text = "0"
        SubElement(o, "currencyId").text = "KZT"
        SubElement(o, "categoryId").text = cid
        SubElement(o, "url").text = it.get("url") or ""
        if it.get("picture"):
            SubElement(o, "picture").text = it["picture"]
        if it.get("vendorCode"):
            SubElement(o, "vendorCode").text = it["vendorCode"]
        if it.get("description"):
            SubElement(o, "description").text = it["description"]

        # Заполняем складские флаги (консервативно: 1)
        for tag in ("quantity_in_stock", "stock_quantity", "quantity"):
            SubElement(o, tag).text = "1"

    buf = io.BytesIO()
    ElementTree(root).write(buf, encoding=ENC, xml_declaration=True)
    return buf.getvalue()

# ---------- MAIN ----------
def main():
    if not CATEGORY_URL:
        die("CATEGORY_URL не задан")

    # Название категории для YML
    cat_html = get(CATEGORY_URL).text
    cat_soup = BeautifulSoup(cat_html, "html.parser")
    cat_name = ""
    h = cat_soup.select_one("h1") or cat_soup.select_one("h2")
    if h: cat_name = norm_text(h.get_text()) or "Copyline"

    # Ссылки карточек
    urls = collect_product_urls(CATEGORY_URL)
    if not urls:
        die("В категории не найдено товаров (ссылок вида /goods/*.html).")

    # Обходим карточки, собираем поля
    items, missing_sku = [], []
    for i, url in enumerate(urls, 1):
        if i > 1:
            time.sleep(REQ_DELAY)
        try:
            it = parse_product(url)
            print(f"[ok] {i:03d}/{len(urls)} | SKU={it.get('vendorCode') or '-'} | {url}")
            if not it.get("vendorCode"):
                missing_sku.append(url)
            items.append(it)
        except Exception as e:
            print(f"[err] {i:03d}/{len(urls)} | {url} | {e}", file=sys.stderr)

    # Строгая политика по артикулам
    if missing_sku:
        print("\n[ERROR] Не найден артикул в карточках (строгий режим):")
        for u in missing_sku:
            print("  -", u)
        if STRICT_VCODE:
            sys.exit(1)

    # Пишем YML
    ensure_dir_for(OUT_FILE)
    yml_bytes = build_yml(items, category_name=cat_name or "Copyline")
    with open(OUT_FILE, "wb") as f:
        f.write(yml_bytes)

    print(f"\n[OK] YML сохранён: {OUT_FILE} | товаров: {len(items)} | категория: {cat_name or 'Copyline'}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        die(str(e))
