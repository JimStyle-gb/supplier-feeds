# -*- coding: utf-8 -*-
"""
Мониторинг copyline.kz с фильтрацией товаров по КЛЮЧЕВЫМ СЛОВАМ и сбором фото,
как в текущем коде (нормализация к 'full_' в URL, без ре-хоста).

Особенности:
- Два режима работы:
    1) MODE=SITE_KEYWORDS (по умолчанию): мониторим весь сайт.
       Источники URL:
         • sitemap.xml / sitemap_index.xml (если есть)
         • резервно — обход сайта (site walk) от BASE_URL с ограничениями
       Берём только /goods/*.html, парсим карточку и фильтруем по ключевым словам в названии.
    2) MODE=CATEGORIES: обратная совместимость — берём категории из CATS_FILE и обходим их с пагинацией.
- Фильтры качества:
    • Товар учитывается только если есть: Название, SKU (Артикул), Цена > 0.
    • Название очищаем от хвостов "(Артикул ...)" и ограничиваем до 110 символов.
- Фото: ищем <img id="main_image_*"> или itemprop="image", затем нормализуем ссылку в 'full_'.
- YML: содержит <categories>:
    • Корень id=9300000 "Copyline"
    • Подкатегории: классификация по ключевым словам (драм → "Драм-юниты", картридж/тонер → "Тонер-картриджи")
      либо, в режиме CATEGORIES, по H1 категории.
- Кодировка: windows-1251.

ENV:
  MODE                — "SITE_KEYWORDS" (default) или "CATEGORIES"
  BASE_URL            — "https://copyline.kz"
  KEYWORDS_FILE       — путь к файлу ключевых слов (по одному на строку)
  CATS_FILE           — файл со списком URL категорий (для режима CATEGORIES)
  OUT_FILE            — путь к итоговому docs/copyline.yml
  OUTPUT_ENCODING     — "windows-1251"
  TIMEOUT_S           — таймаут HTTP
  MIN_BYTES           — минимальный размер ответа
  REQUEST_DELAY_MS    — задержка между запросами (с джиттером)
  MAX_SITEMAP_URLS    — лимит URL из sitemap(ов)
  MAX_SITEWALK_URLS   — лимит URL в обходе сайта
"""

from __future__ import annotations
import os
import re
import io
import time
import html
import math
import hashlib
import random
import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# -------------------- Конфиг --------------------
MODE               = os.getenv("MODE", "SITE_KEYWORDS").upper().strip()
BASE_URL           = os.getenv("BASE_URL", "https://copyline.kz").rstrip("/")
KEYWORDS_FILE      = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
CATS_FILE          = os.getenv("CATS_FILE", "docs/copyline_categories.txt")
OUT_FILE           = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING    = os.getenv("OUTPUT_ENCODING", "windows-1251")
TIMEOUT_S          = int(os.getenv("TIMEOUT_S", "30"))
MIN_BYTES          = int(os.getenv("MIN_BYTES", "1500"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "700"))
MAX_SITEMAP_URLS   = int(os.getenv("MAX_SITEMAP_URLS", "5000"))
MAX_SITEWALK_URLS  = int(os.getenv("MAX_SITEWALK_URLS", "2000"))

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

ROOT_CAT_ID   = 9300000
ROOT_CAT_NAME = "Copyline"

# -------------------- Вспомогалки --------------------
def jitter_sleep(ms: int) -> None:
    base = ms / 1000.0
    time.sleep(max(0.0, base + random.uniform(-0.15, 0.15) * base))

def http_get(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT_S)
        if r.status_code != 200:
            print(f"[warn] GET {url} -> {r.status_code}")
            return None
        b = r.content
        if len(b) < MIN_BYTES:
            print(f"[warn] too small response {len(b)} bytes: {url}")
            return None
        return b
    except Exception as e:
        print(f"[err] GET {url} -> {e}")
        return None

def make_soup(html_bytes: bytes) -> BeautifulSoup:
    return BeautifulSoup(html_bytes, "html.parser")

def normalize_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def sanitize_title(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:110].rstrip()

def normalize_vendor_code(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    v = str(v).strip()
    if not v or v.lower() == "nan":
        return None
    if re.fullmatch(r"\d+", v):
        return f"C{v}"
    return v

def make_offer_id(vendor_code: Optional[str], title: str, url: str) -> str:
    if vendor_code:
        return vendor_code
    return "C" + hashlib.md5((title + "|" + url).encode("utf-8")).hexdigest()[:16]

def to_abs(base: str, href: str) -> str:
    try:
        return urljoin(base, href)
    except Exception:
        return href

def normalize_img_to_full(url: str) -> str:
    """Нормализуем ссылку к виду с 'full_' в имени файла."""
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = BASE_URL + url
    m = re.match(r"^(https?://[^/]+)(/.*/)([^/]+)$", url)
    if not m:
        return url
    host, path, fname = m.groups()
    if fname.startswith("full_"):
        return url
    if fname.startswith("thumb_"):
        fname = "full_" + fname[len("thumb_"):]
    else:
        fname = "full_" + fname
    return f"{host}{path}{fname}"

def parse_price_from_soup(soup: BeautifulSoup) -> Optional[float]:
    # meta itemprop=price
    mp = soup.find("meta", attrs={"itemprop": "price"})
    if mp and mp.get("content"):
        try:
            return float(str(mp["content"]).replace(",", "."))
        except Exception:
            pass
    # классы с 'price'
    cand = soup.find(True, class_=lambda c: c and "price" in c)
    if cand and cand.get_text(strip=True):
        t = cand.get_text(" ", strip=True)
    else:
        t = soup.get_text(" ", strip=True)
    m = re.search(r"(\d[\d\s.,]*)", t)
    if not m:
        return None
    num = m.group(1).replace(" ", "").replace(",", ".")
    try:
        return float(num)
    except Exception:
        return None

def detect_keywords() -> List[str]:
    kws: List[str] = []
    if os.path.isfile(KEYWORDS_FILE):
        with io.open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip().lower()
                if not s or s.startswith("#"):
                    continue
                kws.append(s)
    if not kws:
        kws = ["картридж", "тонер", "тонер-картридж", "драм", "drum", "барабан"]
    return kws

def text_has_keyword(title: str, kws: List[str]) -> bool:
    # упрощённый "фаззи": сравниваем по нормализованным подстрокам без дефисов/лишних пробелов
    t = title.lower().replace("ё", "е")
    t = re.sub(r"[\s\-]+", "", t)
    for kw in kws:
        k = kw.lower().replace("ё", "е")
        k = re.sub(r"[\s\-]+", "", k)
        if k and k in t:
            return True
    return False

def classify_category_by_title(title: str) -> Tuple[int, str]:
    tl = title.lower()
    if any(w in tl for w in ["драм", "drum", "барабан"]):
        return stable_cat_id("Драм-юниты"), "Драм-юниты"
    # всё остальное по умолчанию — картриджи
    return stable_cat_id("Тонер-картриджи"), "Тонер-картриджи"

def stable_cat_id(name: str, prefix: int = 9400000) -> int:
    h = hashlib.md5(name.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

# -------------------- Источники URL (SITE_KEYWORDS) --------------------
def fetch_sitemap_urls(base: str) -> List[str]:
    """Пытаемся собрать все <loc> из sitemap(ов)."""
    candidates = [
        f"{base}/sitemap.xml",
        f"{base}/sitemap_index.xml",
        f"{base}/sitemap-index.xml",
        f"{base}/sitemap1.xml",
        f"{base}/sitemap-products.xml",
    ]
    urls: List[str] = []
    seen: Set[str] = set()

    def parse_sitemap(xml_bytes: bytes):
        try:
            root = ET.fromstring(xml_bytes)
        except Exception:
            return []
        locs: List[str] = []
        for el in root.iter():
            tag = el.tag.lower()
            if tag.endswith("loc") and el.text:
                locs.append(el.text.strip())
        return locs

    queue = []
    for u in candidates:
        b = http_get(u)
        if b:
            queue.append(u)
            # сразу добавим ссылки
            for loc in parse_sitemap(b):
                if loc not in seen:
                    seen.add(loc)
                    urls.append(loc)

    # если sitemap_index присутствует — там могут быть и другие sitemaps
    # простая эвристика: уже собранные urls могут быть xml — снова парсим
    extra: List[str] = []
    for loc in list(urls):
        if loc.lower().endswith(".xml") and len(urls) < MAX_SITEMAP_URLS:
            b = http_get(loc)
            if b:
                for loc2 in parse_sitemap(b):
                    if loc2 not in seen:
                        seen.add(loc2)
                        extra.append(loc2)
    urls += extra

    # фильтруем только /goods/*.html
    product_urls = [u for u in urls if re.search(r"/goods/[^?#]+\.html$", u)]
    # лимит
    if len(product_urls) > MAX_SITEMAP_URLS:
        product_urls = product_urls[:MAX_SITEMAP_URLS]
    return list(dict.fromkeys(product_urls))

def site_walk_products(base: str) -> List[str]:
    """Резервный обход: собираем ссылки /goods/*.html, начиная с BASE_URL, ограниченно."""
    to_visit = [base]
    seen_pages: Set[str] = set()
    found: List[str] = []
    while to_visit and len(seen_pages) < MAX_SITEWALK_URLS:
        url = to_visit.pop(0)
        if url in seen_pages:
            continue
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(url)
        seen_pages.add(url)
        if not b:
            continue
        soup = make_soup(b)
        # собираем товары
        for a in soup.find_all("a", href=True):
            href = a["href"]
            absu = to_abs(url, href)
            if "copyline.kz" not in absu:
                continue
            if re.search(r"/goods/[^?#]+\.html$", absu):
                found.append(absu)
            # базовая навигация
            if len(to_visit) + len(seen_pages) < MAX_SITEWALK_URLS:
                if re.search(r"/goods/|/catalog|/category|/collections|/jshop|/page=", absu) and absu not in seen_pages:
                    to_visit.append(absu)
    return list(dict.fromkeys(found))

# -------------------- Источники URL (CATEGORIES) --------------------
def read_categories() -> List[str]:
    urls: List[str] = []
    if os.path.isfile(CATS_FILE):
        with io.open(CATS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                if s.startswith("/"):
                    s = BASE_URL + s
                urls.append(s)
    if not urls:
        urls = [f"{BASE_URL}/goods/toner-cartridges-brother.html"]
    return urls

def discover_product_links_in_category(cat_url: str, max_pages: int = 40) -> List[str]:
    """Улучшенная пагинация: учитываем rel=next, 'page=', 'PAGEN', '/page/'."""
    found: List[str] = []
    visited: Set[str] = set()
    queue: List[str] = [cat_url]

    def extract_links(html_bytes: bytes, base: str) -> Tuple[List[str], List[str]]:
        soup = make_soup(html_bytes)
        product_links: Set[str] = set()
        next_pages: Set[str] = set()

        # товары
        for a in soup.find_all("a", href=True):
            href = a["href"]
            absu = to_abs(base, href)
            if re.search(r"/goods/[^?#]+\.html$", absu):
                product_links.add(absu)

        # rel=next
        rel_next = soup.find("link", attrs={"rel": "next"})
        if rel_next and rel_next.get("href"):
            next_pages.add(to_abs(base, rel_next["href"]))

        # любые ссылки с паттернами пагинации
        for a in soup.find_all("a", href=True):
            h = a["href"]
            if any(p in h for p in ["page=", "PAGEN_", "/page/"]):
                next_pages.add(to_abs(base, h))

        return list(product_links), list(next_pages)

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(url)
        visited.add(url)
        if not b:
            continue
        prods, nxts = extract_links(b, url)
        found += prods
        for n in nxts:
            if n not in visited and n not in queue:
                queue.append(n)

    return list(dict.fromkeys(found))

# -------------------- Парсинг карточки --------------------
def parse_product(url: str) -> Optional[Dict]:
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(url)
    if not b:
        return None
    soup = make_soup(b)

    # Название
    title = ""
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        title = normalize_text(h1.get_text(" ", strip=True))
    if not title and soup.title and soup.title.get_text(strip=True):
        title = normalize_text(soup.title.get_text(strip=True))
    title = sanitize_title(title)
    if not title:
        return None

    # SKU
    sku = None
    # itemprop=sku
    skuel = soup.find(attrs={"itemprop": "sku"})
    if skuel:
        val = normalize_text(skuel.get_text(" ", strip=True))
        if val:
            sku = val
    # по меткам
    if not sku:
        txt = soup.get_text(" ", strip=True)
        m = re.search(r"(?:Артикул|SKU|Код)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})", txt, flags=re.I)
        if m:
            sku = m.group(1)
    sku = normalize_vendor_code(sku)
    if not sku:
        return None

    # Цена
    price = parse_price_from_soup(soup)
    if not price or price <= 0:
        return None

    # Картинка
    img = None
    imgel = soup.find("img", id=re.compile(r"^main_image_", re.I))
    if imgel and (imgel.get("src") or imgel.get("data-src")):
        img = imgel.get("src") or imgel.get("data-src")
    if not img:
        ogi = soup.find("meta", attrs={"property": "og:image"})
        if ogi and ogi.get("content"):
            img = ogi["content"].strip()
    picture = normalize_img_to_full(img) if img else None

    # Короткое описание
    desc = title

    return {
        "url": url,
        "title": title,
        "vendorCode": sku,
        "price": float(f"{price:.2f}"),
        "picture": picture,
        "description": desc,
    }

# -------------------- Сбор URL и фильтрация --------------------
def collect_product_urls_site_keywords() -> List[str]:
    urls = fetch_sitemap_urls(BASE_URL)
    print(f"[site] sitemap product urls: {len(urls)}")
    if not urls:
        urls = site_walk_products(BASE_URL)
        print(f"[site] fallback site-walk product urls: {len(urls)}")
    return urls

def collect_product_urls_from_categories() -> Tuple[List[Tuple[int,str]], List[str]]:
    cats = read_categories()
    all_cat_defs: List[Tuple[int, str]] = []
    all_prod_urls: List[str] = []
    for cu in cats:
        b = http_get(cu)
        if not b:
            continue
        soup = make_soup(b)
        # имя категории
        cname = "Категория"
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            cname = normalize_text(h1.get_text(" ", strip=True))
        cid = stable_cat_id(cname)
        all_cat_defs.append((cid, cname))
        # товары с улучшенной пагинацией
        urls = discover_product_links_in_category(cu)
        all_prod_urls.extend(urls)
    return all_cat_defs, list(dict.fromkeys(all_prod_urls))

# -------------------- YML --------------------
def build_yml(categories: List[Tuple[int, str]], items: List[Tuple[int, Dict]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>copyline</name>")
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
        # минимальный description (без emoji, cp1251-safe)
        desc = re.sub(r"[^\x00-\x7F\u0400-\u04FF]+", " ", it.get("description") or it["title"])
        out.append(f"<description>{html.escape(desc)}</description>")
        out.append("<quantity_in_stock>1</quantity_in_stock>")
        out.append("<stock_quantity>1</stock_quantity>")
        out.append("<quantity>1</quantity>")
        out.append("</offer>")
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# -------------------- MAIN --------------------
def main() -> None:
    keywords = detect_keywords()
    print(f"[conf] MODE={MODE}, BASE_URL={BASE_URL}")
    print(f"[conf] keywords={keywords}")

    category_defs: List[Tuple[int, str]] = []
    product_urls: List[str] = []

    if MODE == "SITE_KEYWORDS":
        product_urls = collect_product_urls_site_keywords()
        # категории будут от классификатора по названию
    else:
        # обратная совместимость: режим категорий
        category_defs, product_urls = collect_product_urls_from_categories()

    print(f"[urls] total product urls: {len(product_urls)}")

    # Парсим карточки и фильтруем
    items_filtered: List[Tuple[int, Dict]] = []
    seen_oids: Set[str] = set()
    for i, purl in enumerate(product_urls, 1):
        prod = parse_product(purl)
        if not prod:
            continue
        if MODE == "SITE_KEYWORDS":
            # фильтр по ключевым словам
            if not text_has_keyword(prod["title"], keywords):
                continue
            # классификация категории по названию
            cid, cname = classify_category_by_title(prod["title"])
            if (cid, cname) not in category_defs:
                category_defs.append((cid, cname))
        else:
            # в режиме категорий — cid передаётся позже из списка
            pass

        oid = make_offer_id(prod["vendorCode"], prod["title"], prod["url"])
        if oid in seen_oids:
            continue
        seen_oids.add(oid)

        # подставляем cid: в SITE_KEYWORDS он уже вычислен, в CATEGORIES — позже при сборке
        if MODE == "SITE_KEYWORDS":
            items_filtered.append((classify_category_by_title(prod["title"])[0], prod))
        else:
            # временно 0 — распределим после
            items_filtered.append((0, prod))

        if i % 50 == 0:
            print(f"[parse] processed {i}/{len(product_urls)}")

    # если режим CATEGORIES — нужно раздать cid по товарам,
    # но у нас нет связи url->категория. Упростим: все товары кладём в первую категорию из файла,
    # чтобы не терять совместимость. При желании можно улучшить разбор breadcrumbs.
    if MODE != "SITE_KEYWORDS" and category_defs:
        cid0 = category_defs[0][0]
        items_filtered = [(cid0, it) for _, it in items_filtered]

    print(f"[stat] kept items: {len(items_filtered)}, categories: {len(category_defs)}")

    xml_text = build_yml(category_defs, items_filtered)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml_text)
    print(f"[done] written: {OUT_FILE}")

if __name__ == "__main__":
    main()
