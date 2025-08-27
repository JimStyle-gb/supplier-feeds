# -*- coding: utf-8 -*-
"""
build_copyline.py — единый сборщик copyline.kz «по-умному».

Что делает:
• Обходит сайт (BFS) от нескольких стартовых точек, собирает только карточки /goods/*.html,
  учитывает пагинацию (rel=next, page=, PAGEN_, /page/).
• Фильтрует товары по ключевым словам из docs/copyline_keywords.txt:
  drum, девелопер, драм, кабель сетевой, картридж, термоблок, термоэлемент, тонер-картридж.
• Товар учитывается только если у него есть: Title, SKU (Артикул), Price > 0.
• Фото берётся из <img id="main_image_*"> или og:image и нормализуется в URL с префиксом full_
  (если было thumb_ → заменяем на full_; если префикса не было — добавляем).
• Название чистится от хвостов вида "(Артикул XXX)" и ограничивается 110 символами (правило Satu).
• SKU (vendorCode) оставляем в <vendorCode>; если состоит только из цифр — добавляем префикс 'C'.
• Генерирует docs/copyline.yml (XML/YML) в Windows-1251, валюта KZT, с секцией <categories>:
  — Корень id=9300000 "Copyline"
  — Подкатегории по классу товара: Тонер-картриджи, Драм-юниты, Девелоперы, Термоблоки, Термоэлементы, Сетевые кабели.

Зависимости: requests, beautifulsoup4
"""

from __future__ import annotations

import os
import re
import io
import time
import html
import hashlib
import random
from typing import List, Dict, Tuple, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------- Конфиг из ENV ----------------------
BASE_URL         = os.getenv("BASE_URL", "https://copyline.kz").rstrip("/")
KEYWORDS_FILE    = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE         = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251")
TIMEOUT_S        = int(os.getenv("TIMEOUT_S", "30"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "700"))
MAX_VISIT_PAGES  = int(os.getenv("MAX_VISIT_PAGES", "2000"))
MAX_PRODUCTS     = int(os.getenv("MAX_PRODUCTS", "20000"))

UA = {"User-Agent": "Mozilla/5.0 (compatible; CopylineFeed/1.0)"}

ROOT_CAT_ID   = 9300000
ROOT_CAT_NAME = "Copyline"

# ---------------------- Утилиты ----------------------
def jitter_sleep(ms: int) -> None:
    """Небольшая пауза с джиттером, чтобы не долбить сайт как бот."""
    base = ms / 1000.0
    time.sleep(max(0.0, base + random.uniform(-0.15, 0.15) * base))

def http_get(url: str) -> Optional[bytes]:
    """Безопасный GET: таймаут, минимальный размер, базовый UA."""
    try:
        r = requests.get(url, headers=UA, timeout=TIMEOUT_S)
        if r.status_code != 200:
            print(f"[warn] GET {url} -> {r.status_code}")
            return None
        b = r.content
        if len(b) < MIN_BYTES:
            print(f"[warn] tiny response ({len(b)} bytes): {url}")
            return None
        return b
    except Exception as e:
        print(f"[err] GET {url} -> {e}")
        return None

def soup_of(b: bytes) -> BeautifulSoup:
    """Парсим HTML встроенным парсером — без lxml, чтобы меньше зависимостей."""
    return BeautifulSoup(b, "html.parser")

def norm_text(s: str) -> str:
    """Схлопываем пробелы/энтимнити и чистим текст."""
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def sanitize_title(s: str) -> str:
    """
    Чистим служебные хвосты "(Артикул ...)/(SKU ...)/(Код ...)" в конце строки.
    И ограничиваем до 110 символов под правило Satu.
    """
    if not s:
        return ""
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:110].rstrip()

def normalize_vendor_code(v: Optional[str]) -> Optional[str]:
    """Возвращаем SKU, добавляя префикс 'C' для чисто цифровых значений."""
    if not v:
        return None
    v = str(v).strip()
    if not v or v.lower() == "nan":
        return None
    if re.fullmatch(r"\d+", v):
        return f"C{v}"
    return v

def make_offer_id(vendor_code: Optional[str], title: str, url: str) -> str:
    """Стабильный offer id: приоритет — vendorCode; иначе хеш (название+url)."""
    return vendor_code or ("C" + hashlib.md5((title + "|" + url).encode("utf-8")).hexdigest()[:16])

def to_abs(base: str, href: str) -> str:
    """Нормализация относительных ссылок в абсолютные."""
    try:
        return urljoin(base, href)
    except Exception:
        return href

def normalize_img_to_full(url: Optional[str]) -> Optional[str]:
    """
    Нормализуем URL изображения к виду с 'full_' в имени файла.
    • // -> https://
    • / -> BASE_URL + /
    • thumb_*.jpg -> full_*.jpg
    • foo.jpg -> full_foo.jpg
    """
    if not url:
        return None
    u = url.strip()
    if u.startswith("//"):
        u = "https:" + u
    elif u.startswith("/"):
        u = BASE_URL + u
    m = re.match(r"^(https?://[^/]+)(/.*/)([^/]+)$", u)
    if not m:
        return u
    host, path, fname = m.groups()
    if fname.startswith("full_"):
        return u
    if fname.startswith("thumb_"):
        fname = "full_" + fname[len("thumb_"):]
    else:
        fname = "full_" + fname
    return f"{host}{path}{fname}"

# ---------------------- Ключевые слова и классификация ----------------------
def load_keywords() -> List[str]:
    """Читаем ключевые слова из файла; если файла нет/пуст — дефолт из ТЗ."""
    kws: List[str] = []
    if os.path.isfile(KEYWORDS_FILE):
        with io.open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip().lower()
                if not s or s.startswith("#"):
                    continue
                kws.append(s)
    if not kws:
        kws = ["drum", "девелопер", "драм", "кабель сетевой", "картридж", "термоблок", "термоэлемент", "тонер-картридж"]
    return kws

def title_has_keyword(title: str, kws: List[str]) -> bool:
    """Фильтр по ключам: сравниваем без пробелов/дефисов, с учётом рус./англ. форм."""
    t = title.lower().replace("ё", "е")
    t = re.sub(r"[\s\-]+", "", t)
    for kw in kws:
        k = kw.lower().replace("ё", "е")
        k = re.sub(r"[\s\-]+", "", k)
        if k and k in t:
            return True
    return False

def classify_category(title: str) -> Tuple[int, str]:
    """Определяем подкатегорию по названию товара (простые эвристики)."""
    tl = title.lower()
    # порядок важен: более специфичные — раньше
    if any(w in tl for w in ["драм", "drum"]):
        return stable_cat_id("Драм-юниты"), "Драм-юниты"
    if "девелопер" in tl:
        return stable_cat_id("Девелоперы"), "Девелоперы"
    if "термоэлемент" in tl:
        return stable_cat_id("Термоэлементы"), "Термоэлементы"
    if "термоблок" in tl or "печка" in tl or "fuser" in tl:
        return stable_cat_id("Термоблоки"), "Термоблоки"
    if "кабель" in tl and "сет" in tl:  # «кабель сетевой», «сетевой кабель», «кабель для сети»
        return stable_cat_id("Сетевые кабели"), "Сетевые кабели"
    # по умолчанию — тонер/картриджи
    return stable_cat_id("Тонер-картриджи"), "Тонер-картриджи"

def stable_cat_id(name: str, prefix: int = 9400000) -> int:
    """Делаем стабильный id категории из имени (md5->int с предсказуемым префиксом)."""
    h = hashlib.md5(name.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

# ---------------------- Парсинг карточки товара ----------------------
def parse_product(url: str) -> Optional[Dict]:
    """Разбираем карточку товара. Возвращаем dict или None, если это не валидный товар."""
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(url)
    if not b:
        return None
    s = soup_of(b)

    # Название
    title = ""
    h1 = s.find("h1")
    if h1 and h1.get_text(strip=True):
        title = norm_text(h1.get_text(" ", strip=True))
    if not title and s.title and s.title.get_text(strip=True):
        title = norm_text(s.title.get_text(strip=True))
    title = sanitize_title(title)
    if not title:
        return None

    # SKU / Артикул
    sku = None
    # itemprop=sku
    skuel = s.find(attrs={"itemprop": "sku"})
    if skuel:
        val = norm_text(skuel.get_text(" ", strip=True))
        if val:
            sku = val
    # текстовые паттерны
    if not sku:
        txt = s.get_text(" ", strip=True)
        m = re.search(r"(?:Артикул|SKU|Код)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})", txt, flags=re.I)
        if m:
            sku = m.group(1)
    sku = normalize_vendor_code(sku)
    if not sku:
        return None

    # Цена
    price = None
    mp = s.find("meta", attrs={"itemprop": "price"})
    if mp and mp.get("content"):
        try:
            price = float(str(mp["content"]).replace(",", "."))
        except Exception:
            price = None
    if price is None:
        # Подстраховка: ищем число в блоках с классом, содержащим 'price', либо в тексте
        cand = s.find(True, class_=lambda c: c and "price" in c)
        t = cand.get_text(" ", strip=True) if cand else s.get_text(" ", strip=True)
        m = re.search(r"(\d[\d\s.,]*)", t)
        if m:
            num = m.group(1).replace(" ", "").replace(",", ".")
            try:
                price = float(num)
            except Exception:
                price = None
    if not price or price <= 0:
        return None

    # Фото
    img = None
    imgel = s.find("img", id=re.compile(r"^main_image_", re.I))
    if imgel and (imgel.get("src") or imgel.get("data-src")):
        img = imgel.get("src") or imgel.get("data-src")
    if not img:
        ogi = s.find("meta", attrs={"property": "og:image"})
        if ogi and ogi.get("content"):
            img = ogi["content"].strip()
    picture = normalize_img_to_full(img) if img else None

    # Короткое описание — cp1251-safe (без эмодзи и экзотики)
    desc = re.sub(r"[^\x00-\x7F\u0400-\u04FF]+", " ", title)

    return {
        "url": url,
        "title": title,
        "vendorCode": sku,
        "price": float(f"{price:.2f}"),
        "picture": picture,
        "description": desc,
    }

# ---------------------- Сбор ссылок по сайту ----------------------
PRODUCT_RE = re.compile(r"/goods/[^/]+\.html$")
INDEX_SEEDS = [
    "/goods.html",                         # общий каталог
    "/goods/toner-cartridges-brother.html",# живая категория
    "/",                                   # корень
]

def discover_links_from_page(page_url: str) -> Tuple[List[str], List[str]]:
    """
    Возвращает (product_links, next_pages) из одной страницы:
    • product_links — абсолютные ссылки на /goods/*.html
    • next_pages    — кандидаты на продолжение обхода (rel=next, page=, PAGEN_, /page/)
    """
    b = http_get(page_url)
    if not b:
        return [], []
    s = soup_of(b)

    # Товары
    prods: Set[str] = set()
    for a in s.find_all("a", href=True):
        href = a["href"].strip()
        absu = to_abs(page_url, href)
        if "copyline.kz" not in absu:
            continue
        if PRODUCT_RE.search(absu) and not absu.endswith("/goods.html"):
            prods.add(absu)

    # Пагинация и навигация
    nexts: Set[str] = set()
    ln = s.find("link", attrs={"rel": "next"})
    if ln and ln.get("href"):
        nexts.add(to_abs(page_url, ln["href"]))
    for a in s.find_all("a", href=True):
        h = a["href"]
        if any(p in h for p in ["page=", "PAGEN_", "/page/"]):
            nexts.add(to_abs(page_url, h))

    return list(prods), list(nexts)

def crawl_site_for_products() -> List[str]:
    """
    BFS-обход сайта от стартовых страниц. Собираем /goods/*.html,
    учитываем пагинацию. Ограничиваемся MAX_VISIT_PAGES и MAX_PRODUCTS.
    """
    queue: List[str]   = [to_abs(BASE_URL, p) for p in INDEX_SEEDS]
    visited: Set[str]  = set()
    products: List[str]= []

    while queue and len(visited) < MAX_VISIT_PAGES and len(products) < MAX_PRODUCTS:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        jitter_sleep(REQUEST_DELAY_MS)

        prods, nexts = discover_links_from_page(url)
        for p in prods:
            if p not in products:
                products.append(p)
        for n in nexts:
            if n not in visited and n not in queue:
                queue.append(n)

    print(f"[scan] visited pages: {len(visited)}, products found: {len(products)}")
    return products

# ---------------------- Генерация YML ----------------------
def build_yml(categories: List[Tuple[int, str]], items: List[Tuple[int, Dict]]) -> str:
    """
    Собираем XML/YML-строку:
    • <categories> с корнем Copyline и нашими подкатегориями
    • <offers> со стандартными полями для Satu
    """
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append("<name>copyline</name>")
    out.append("<currencies><currency id=\"KZT\" rate=\"1\" /></currencies>")

    # Категории
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{html.escape(ROOT_CAT_NAME)}</category>")
    for cid, cname in categories:
        out.append(f"<category id=\"{cid}\" parentId=\"{ROOT_CAT_ID}\">{html.escape(cname)}</category>")
    out.append("</categories>")

    # Офферы
    out.append("<offers>")
    for cid, it in items:
        name_xml = html.escape(it["title"])
        url_xml  = html.escape(it["url"])
        sku_xml  = html.escape(it["vendorCode"])
        pic_xml  = html.escape(it["picture"] or "")
        price_val = it["price"]
        price_txt = str(int(price_val)) if float(price_val).is_integer() else f"{price_val}"

        oid = html.escape(make_offer_id(it["vendorCode"], it["title"], it["url"]))
        out.append(f"<offer id=\"{oid}\" available=\"true\" in_stock=\"true\">")
        out.append(f"<name>{name_xml}</name>")
        out.append(f"<price>{price_txt}</price>")
        out.append(f"<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        out.append(f"<url>{url_xml}</url>")
        if pic_xml:
            out.append(f"<picture>{pic_xml}</picture>")
        out.append(f"<vendorCode>{sku_xml}</vendorCode>")
        # Короткий безопасный description
        desc = re.sub(r"[^\x00-\x7F\u0400-\u04FF]+", " ", it.get("description") or it["title"])
        out.append(f"<description>{html.escape(desc)}</description>")
        # Базовые складские поля
        out.append("<quantity_in_stock>1</quantity_in_stock>")
        out.append("<stock_quantity>1</stock_quantity>")
        out.append("<quantity>1</quantity>")
        out.append("</offer>")
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------------------- MAIN ----------------------
def main() -> None:
    kws = load_keywords()
    print(f"[conf] BASE_URL={BASE_URL}, keywords={kws}")

    # 1) Сканируем сайт и собираем URL карточек
    product_urls = crawl_site_for_products()
    print(f"[urls] total candidates: {len(product_urls)}")

    # 2) Парсим карточки, фильтруем по ключам, классифицируем по категориям
    category_defs: List[Tuple[int, str]] = []
    seen_cat: Set[int] = set()
    items: List[Tuple[int, Dict]] = []
    seen_oids: Set[str] = set()

    for i, purl in enumerate(product_urls, 1):
        prod = parse_product(purl)
        if not prod:
            continue
        if not title_has_keyword(prod["title"], kws):
            continue

        cid, cname = classify_category(prod["title"])
        if cid not in seen_cat:
            category_defs.append((cid, cname))
            seen_cat.add(cid)

        oid = make_offer_id(prod["vendorCode"], prod["title"], prod["url"])
        if oid in seen_oids:
            continue
        seen_oids.add(oid)

        items.append((cid, prod))

        if i % 50 == 0:
            print(f"[parse] processed {i}/{len(product_urls)}")

    print(f"[stat] kept items: {len(items)}, categories: {len(category_defs)}")

    # 3) Пишем YML (Windows-1251)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml_text = build_yml(category_defs, items)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml_text)
    print(f"[done] written: {OUT_FILE}")

if __name__ == "__main__":
    main()
