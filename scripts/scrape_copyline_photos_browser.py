#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper фото для copyline.kz на базе Playwright (хедлесс-браузер).

Ключевые идеи:
- Ищем товар через встроенный поиск сайта, кликаем по результатам,
  открываем страницу карточки, ждём ленивую загрузку.
- Жёстко верифицируем, что это именно наш артикул: проверяем текст страницы
  (span[itemprop=sku], подписи «Артикул», вхождение кода).
- Достаём правильную картинку из карточки товара приоритетом:
  1) img[itemprop="image"] @src
  2) img[id^="main_image_"] @src
  3) link[rel="image_src"] @href
  4) meta[property="og:image"] @content
  5) a#zoom_main_image / a.zoom @href
- Отбрасываем заведомые заглушки (thumb_black-toner.jpg, noimage и т.п.),
  а также слишком "лёгкие" картинки (опционально HEAD-проверка Content-Length).
- Результат пишем ОТДЕЛЬНО от основного YML:
  * docs/copyline_photos.yml         — пары <vendorCode> + <picture>
  * docs/copyline_photo_report.csv   — детальный отчёт (для контроля)
  * docs/copyline_not_found.txt      — коды без валидной фотографии

Как запускать локально:
    export ARTICLES_FILE=docs/copyline_articles.txt
    python scripts/scrape_copyline_photos_browser.py

В GitHub Actions переменные берутся из env (см. workflow).
"""

import os
import re
import csv
import time
import random
import contextlib
from dataclasses import dataclass
from urllib.parse import urljoin

from xml.etree import ElementTree as ET
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


# =========================
# Конфигурация через ENV
# =========================

BASE_URL         = os.getenv("BASE_URL", "https://copyline.kz").rstrip("/")
ARTICLES_FILE    = os.getenv("ARTICLES_FILE", "").strip()           # txt: по 1 артикулу в строке
YML_PATH         = os.getenv("YML_PATH", "docs/copyline.yml").strip()  # альтернативный источник артикулов
OUT_YML          = os.getenv("OUT_YML", "docs/copyline_photos.yml").strip()
REPORT_CSV       = os.getenv("REPORT_CSV", "docs/copyline_photo_report.csv").strip()
NOT_FOUND_TXT    = os.getenv("NOT_FOUND_TXT", "docs/copyline_not_found.txt").strip()

# Тайминги и повторы
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "900"))   # базовая пауза между товарами
PAGE_TIMEOUT_MS  = int(os.getenv("PAGE_TIMEOUT_MS", "30000"))  # таймаут на загрузку страниц
MAX_RETRIES      = int(os.getenv("MAX_RETRIES", "3"))          # повторы при сбоях
BACKOFF_MAX_MS   = int(os.getenv("BACKOFF_MAX_MS", "15000"))   # макс. бэкоф

# Фильтр "мусорных" фото
MIN_BYTES        = int(os.getenv("MIN_BYTES", "2500"))         # отсев по Content-Length (опционально)
DISCARD_PATTERNS = [
    "thumb_black-toner.jpg",
    "noimage", "placeholder", "blank", "nophoto",
]

# Кодировка для итогового YML (под ваш пайплайн)
OUTPUT_ENCODING  = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()


# =========================
# Вспомогательные структуры
# =========================

@dataclass
class FoundPhoto:
    article: str
    product_url: str
    image_url: str
    note: str = ""


# =========================
# Загрузка списка артикулов
# =========================

def load_articles() -> list[str]:
    """
    Загружаем артикула одним из способов:
    1) Если указали ARTICLES_FILE — читаем простой txt (по 1 артикулу в строке).
    2) Иначе парсим существующий XML YML (docs/copyline.yml) и берём значения <vendorCode>.
    """
    if ARTICLES_FILE and os.path.isfile(ARTICLES_FILE):
        arts = []
        with open(ARTICLES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                a = re.sub(r"\s+", "", line)
                if a:
                    arts.append(a)
        print(f"[init] ARTICLES_FILE: loaded {len(arts)} codes from {ARTICLES_FILE}")
        return arts

    # Парсим основной YML
    arts = []
    if os.path.isfile(YML_PATH):
        try:
            tree = ET.parse(YML_PATH)
            root = tree.getroot()
            for vc in root.findall(".//vendorCode"):
                val = (vc.text or "").strip()
                if val:
                    arts.append(val)
            arts = sorted(set(arts))
            print(f"[init] YML_PATH: extracted {len(arts)} vendorCode from {YML_PATH}")
            return arts
        except Exception as e:
            print(f"[warn] failed to parse YML_PATH {YML_PATH}: {e}")

    print("[fatal] no articles source found. Provide ARTICLES_FILE or valid YML_PATH.")
    return []


# =========================
# Утилиты
# =========================

def jitter_sleep(ms_base: int, spread: float = 0.35):
    """
    Пауза с джиттером — помогает не выглядеть ботом.
    """
    ms = ms_base * random.uniform(1.0 - spread, 1.0 + spread)
    time.sleep(ms / 1000.0)

def is_discard_image_url(url: str) -> bool:
    """
    Отсев явных заглушек по имени/пути.
    """
    u = url.lower()
    return any(pat in u for pat in DISCARD_PATTERNS)

def ensure_dir_for(path: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def head_size_ok(context, url: str) -> bool:
    """
    HEAD-проверка размера картинки через Playwright APIRequestContext.
    Если сервер не отдаёт Content-Length — не блокируем, просто считаем OK.
    """
    try:
        resp = context.request.head(url, timeout=PAGE_TIMEOUT_MS)
        if not resp.ok:
            return True  # не наказываем — возьмём по содержимому ссылки
        cl = resp.headers.get("content-length") or resp.headers.get("Content-Length")
        if cl and cl.isdigit():
            return int(cl) >= MIN_BYTES
        return True
    except Exception:
        return True


# =========================
# Поиск и парсинг страницы
# =========================

SEARCH_INPUT_SELECTORS = [
    'input[name="search"]',
    'input[type="search"]',
    'input[name="searchword"]',
    'input[name="q"]',
    'input[placeholder*="Поиск" i]',
    'input[placeholder*="Search" i]',
    '#search input',
    'input[name="keyword"]',
]

SEARCH_URL_CANDIDATES = [
    # fallback-страницы поиска (для Joomla/JoomShopping это часто работает)
    "/search?searchword={q}",
    "/index.php?option=com_jshopping&controller=search&task=view&search={q}",
    "/?search={q}",
]

RESULT_LINK_SELECTORS = [
    # ссылки на карточки товара на страницах списка
    'a.product_link',
    '.product_name a',
    '.jshop_list_product a[href*="/goods/"]',
    'a[href*="/goods/"]',
    # общий fallback
    'a[href*="goods"]',
]

IMAGE_SELECTORS = [
    'img[itemprop="image"]',
    'img[id^="main_image_"]',
]

FALLBACK_IMAGE_SELECTORS = [
    'link[rel="image_src"]',
    'meta[property="og:image"]',
    'a#zoom_main_image',
    'a.zoom[href*="img_products"]',
]


def try_open_search(page, code: str) -> None:
    """
    Пытаемся выполнить поиск:
    1) Грузим главную, ищем поле поиска по ряду селекторов, вводим код, жмём Enter.
    2) Если не получилось — открываем одну из шаблонных поисковых URL с параметром.
    """
    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)

    for sel in SEARCH_INPUT_SELECTORS:
        with contextlib.suppress(PWTimeout, Exception):
            el = page.locator(sel).first
            if el and el.is_visible(timeout=1000):
                el.fill("")                     # очистить на всякий
                el.type(code, delay=40)         # немного «человеческой» задержки печати
                el.press("Enter")
                page.wait_for_load_state("networkidle", timeout=PAGE_TIMEOUT_MS)
                return

    # Fallback: прямые URL поиска
    for tpl in SEARCH_URL_CANDIDATES:
        url = BASE_URL + tpl.format(q=code)
        with contextlib.suppress(PWTimeout, Exception):
            page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
            return

    # если мы здесь — значит, не удалось даже открыть поисковую страницу
    raise RuntimeError("Не удалось открыть страницу поиска")


def collect_candidate_links(page) -> list[str]:
    """
    Собираем список ссылок на карточки товара с текущей страницы.
    """
    links = set()
    for sel in RESULT_LINK_SELECTORS:
        with contextlib.suppress(Exception):
            for a in page.locator(sel).all():
                href = a.get_attribute("href") or ""
                if not href:
                    continue
                if not href.startswith("http"):
                    href = urljoin(BASE_URL + "/", href)
                links.add(href)
    return list(links)


def page_contains_article(page, code: str) -> bool:
    """
    Проверяем, что на странице карточки есть нужный артикул:
    - span[itemprop=sku]
    - текст «Артикул/Код товара»
    - простое вхождение кода в текст страницы
    """
    code_clean = code.strip()
    # 1) microdata sku
    try:
        sku = page.locator('span[itemprop="sku"]').first
        if sku and sku.is_visible(timeout=500):
            text = (sku.text_content() or "").strip()
            if text == code_clean or code_clean in text:
                return True
    except Exception:
        pass

    # 2) подписи
    try:
        html = page.content()
        if re.search(r"(артикул|код\s*товара)[^<]{0,30}" + re.escape(code_clean), html, re.IGNORECASE):
            return True
    except Exception:
        pass

    # 3) полное содержимое
    try:
        text = (page.inner_text("body") or "")
        if code_clean in text:
            return True
    except Exception:
        pass

    return False


def extract_image_url(page) -> str | None:
    """
    Достаём URL основной картинки из карточки товара.
    Приоритет: IMG@src -> link[rel=image_src] -> meta og:image -> a.zoom@href.
    """
    # базовые IMG
    for sel in IMAGE_SELECTORS:
        with contextlib.suppress(Exception):
            img = page.locator(sel).first
            if img and img.is_visible(timeout=1000):
                # Иногда src может подмениться после скролла — проскроллим в зону видимости
                img.scroll_into_view_if_needed(timeout=2000)
                src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                if src:
                    return src

    # Fallback 1: <link rel="image_src" href="...">
    with contextlib.suppress(Exception):
        link = page.locator('link[rel="image_src"]').first
        if link:
            href = link.get_attribute("href") or ""
            if href:
                return href

    # Fallback 2: <meta property="og:image" content="...">
    with contextlib.suppress(Exception):
        meta = page.locator('meta[property="og:image"]').first
        if meta:
            cont = meta.get_attribute("content") or ""
            if cont:
                return cont

    # Fallback 3: a.zoom / a#zoom_main_image
    for sel in FALLBACK_IMAGE_SELECTORS:
        with contextlib.suppress(Exception):
            a = page.locator(sel).first
            if a:
                href = a.get_attribute("href") or ""
                if href:
                    return href

    return None


def normalize_image_url(url: str) -> str:
    """
    Превращаем относительный URL в абсолютный.
    """
    if not url:
        return url
    if not url.startswith("http"):
        return urljoin(BASE_URL + "/", url)
    return url


def find_photo_for_code(context, page, code: str) -> FoundPhoto | None:
    """
    Полный цикл для одного артикула:
    - поиск
    - выбор карточки
    - верификация артикула
    - извлечение фото
    - фильтрация мусора
    """
    # Шаг 1: открыть поиск по коду
    try_open_search(page, code)

    # Если поиск сразу кинул в карточку — попробуем тут же
    candidates = [page.url]
    # Иначе соберём ссылки с текущей страницы (выдача/категория)
    candidates += [u for u in collect_candidate_links(page) if u not in candidates]

    # Перебираем кандидатов до успешной верификации артикула
    for link in candidates:
        with contextlib.suppress(PWTimeout, Exception):
            page.goto(link, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
            # Дождаться «успокоения» сети (ленивая подгрузка)
            page.wait_for_load_state("networkidle", timeout=PAGE_TIMEOUT_MS)

            # Немного прокрутки — помогает триггернуть lazy-изображения
            page.evaluate("window.scrollTo(0, 0)")
            jitter_sleep(300)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.25)")
            jitter_sleep(300)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.6)")
            jitter_sleep(300)

            if not page_contains_article(page, code):
                continue  # не тот товар

            # Есть совпадение артикула — достаём картинку
            raw = extract_image_url(page)
            if not raw:
                continue
            img = normalize_image_url(raw)

            # Отсев очевидных заглушек
            if is_discard_image_url(img):
                return FoundPhoto(code, page.url, img, "discarded_placeholder")

            # HEAD-проверка (если сервер отдает Content-Length)
            if not head_size_ok(context, img):
                return FoundPhoto(code, page.url, img, f"too_small(<{MIN_BYTES} bytes)")

            return FoundPhoto(code, page.url, img, "ok")

    return None


# =========================
# Сериализация результатов
# =========================

def write_results_yml(rows: list[FoundPhoto], out_path: str):
    """
    Пишем лёгкий YML, который потом можно «влить» в основной фид.
    Формат:
      <yml_catalog><shop><offers>
        <offer>
          <vendorCode>...</vendorCode>
          <picture>...</picture>
        </offer>
      </offers></shop></yml_catalog>
    """
    ensure_dir_for(out_path)

    root = ET.Element("yml_catalog")
    shop = ET.SubElement(root, "shop")
    offers = ET.SubElement(shop, "offers")

    for r in rows:
        off = ET.SubElement(offers, "offer")
        ET.SubElement(off, "vendorCode").text = r.article
        ET.SubElement(off, "picture").text = r.image_url

    tree = ET.ElementTree(root)
    with open(out_path, "wb") as f:
        tree.write(f, encoding=OUTPUT_ENCODING, xml_declaration=True)


def write_report_csv(rows: list[FoundPhoto], path: str):
    """
    Детальный CSV-отчёт: артикул, url карточки, url картинки, примечание.
    """
    ensure_dir_for(path)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["article", "product_url", "image_url", "note"])
        for r in rows:
            w.writerow([r.article, r.product_url, r.image_url, r.note])


def write_not_found_txt(missed: list[str], path: str):
    """
    Список артикулов без валидного фото.
    """
    ensure_dir_for(path)
    with open(path, "w", encoding="utf-8") as f:
        for a in missed:
            f.write(f"{a}\n")


# =========================
# MAIN
# =========================

def main():
    articles = load_articles()
    if not articles:
        raise SystemExit(1)

    found: list[FoundPhoto] = []
    missed: list[str] = []

    with sync_playwright() as p:
        # Chromium наиболее совместим с «современными» сайтами
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/123.0 Safari/537.36",
            java_script_enabled=True,
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()
        page.set_default_timeout(PAGE_TIMEOUT_MS)

        for idx, code in enumerate(articles, 1):
            code = str(code).strip()
            if not code:
                continue

            ok: FoundPhoto | None = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    ok = find_photo_for_code(context, page, code)
                    if ok:
                        break
                except Exception as e:
                    print(f"[{idx}/{len(articles)}] {code}: error attempt {attempt}: {e}")

                # экспоненциальный бэкоф между попытками
                backoff = min(BACKOFF_MAX_MS, 800 * (2 ** (attempt - 1)))
                jitter_sleep(backoff)

            if ok and ok.note.startswith("ok"):
                print(f"[ok]   {code} -> {ok.image_url}")
                found.append(ok)
            elif ok:
                # картинка есть, но пометили как «сомнительную» (заглушка/мелкая)
                print(f"[skip] {code} -> {ok.image_url} ({ok.note})")
                missed.append(code)
            else:
                print(f"[miss] {code}: картинка не найдена")
                missed.append(code)

            # межтоварная пауза с джиттером
            jitter_sleep(REQUEST_DELAY_MS)

        browser.close()

    # Запись результатов
    if found:
        write_results_yml(found, OUT_YML)
        write_report_csv(found, REPORT_CSV)
        print(f"[done] photos: {len(found)} | yml: {OUT_YML} | report: {REPORT_CSV}")
    else:
        print("[done] не найдено ни одной валидной картинки")

    if missed:
        write_not_found_txt(missed, NOT_FOUND_TXT)
        print(f"[info] not found list: {NOT_FOUND_TXT} (count={len(missed)})")


if __name__ == "__main__":
    main()
