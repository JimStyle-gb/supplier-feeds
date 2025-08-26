# -*- coding: utf-8 -*-
"""
ВАРИАНТ B — ШАГ 1: ОБЪЕЗД КАТЕГОРИЙ И ПОСТРОЕНИЕ ИНДЕКСА
Собираем карту: URL карточки товара -> главная фотка (src из <img itemprop="image">) + H1.
Потом шаг 2 (enrich) будет искать URL карточки по артикулу и доставать ссылку на фото из этого индекса
(не открывая карточку повторно).

Что делает скрипт:
1) Берёт список URL категорий из файла (по умолчанию docs/copyline_category_urls.txt).
2) Идёт по каждой категории, собирает ссылки на карточки (a[href*="/goods/"] c .html).
3) Для каждой карточки заходит ОДИН РАЗ, достаёт главную фотку:
   - <img itemprop="image"> (src | data-src | data-original)  ИЛИ  <noscript> с <img> внутри.
   - фильтрует заглушки: noimage, placeholder, black-toner, thumb_ и т.п.
4) Сохраняет индекс в JSON: docs/copyline_photo_index.json
   Формат: {
     "generated_at": 1712345678,
     "products": {
       "https://copyline.kz/goods/....html": {
         "image": "https://copyline.kz/components/com_jshopping/files/img_products/....jpg",
         "title": "H1 карточки",
         "updated": 1712345678
       }
     }
   }

Переменные окружения:
- URLS_FILE            (default: docs/copyline_category_urls.txt) — список категорий для обхода
- INDEX_PATH           (default: docs/copyline_photo_index.json) — куда сохранять индекс
- REQUEST_DELAY_MS     (default: 700) — пауза между запросами (мс)
- BACKOFF_MAX_MS       (default: 12000) — макс. бэкофф при 429/403/5xx (мс)
- FLUSH_EVERY_N        (default: 25) — как часто флешить индекс на диск (по кол-ву карточек)
"""

from __future__ import annotations
import os, re, time, json, hashlib
from typing import Optional, Dict, Any, List, Tuple
import requests
from bs4 import BeautifulSoup

BASE = "https://copyline.kz"
UA_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

URLS_FILE      = os.getenv("URLS_FILE", "docs/copyline_category_urls.txt")
INDEX_PATH     = os.getenv("INDEX_PATH", "docs/copyline_photo_index.json")
REQUEST_DELAY  = max(100, int(os.getenv("REQUEST_DELAY_MS", "700"))) / 1000.0
BACKOFF_MAX    = max(1000, int(os.getenv("BACKOFF_MAX_MS", "12000"))) / 1000.0
FLUSH_EVERY_N  = max(1, int(os.getenv("FLUSH_EVERY_N", "25")))

# -------------------- УТИЛЫ --------------------
def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def absolutize(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return requests.compat.urljoin(BASE, url)

def load_lines(path: str) -> List[str]:
    """Читает список URL категорий (игнорит пустые и начинающиеся с '#')."""
    out = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                out.append(s)
    except FileNotFoundError:
        pass
    return out

def save_json(obj: Any, path: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def fetch(session: requests.Session, url: str) -> requests.Response:
    """GET с вежливыми задержками и экспоненциальным бэкоффом на 429/403/5xx."""
    delay = REQUEST_DELAY
    tries = 0
    while True:
        tries += 1
        r = session.get(url, headers=UA_HEADERS, timeout=60)
        # нормальные ответы
        if r.status_code < 400:
            return r
        # мягкие отказы / пределы — ждём и пробуем снова
        if r.status_code in (429, 403, 502, 503, 504):
            time.sleep(min(delay, BACKOFF_MAX))
            delay = min(delay * 2, BACKOFF_MAX)
            continue
        # жёсткая ошибка
        r.raise_for_status()

# -------------------- ПАРСИНГ --------------------
def extract_product_links(html: str) -> List[str]:
    """Собираем ссылки на карточки из HTML категории."""
    soup = BeautifulSoup(html, "lxml")
    hrefs = set()

    # Явные карточки
    for a in soup.select('a[href*="/goods/"]'):
        href = a.get("href") or ""
        if href.endswith(".html"):
            hrefs.add(absolutize(href))

    # На всякий случай все <a>, фильтруем
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/goods/" in href and href.endswith(".html"):
            hrefs.add(absolutize(href))

    return sorted(hrefs)

BAD_IMG_RE = re.compile(r"(noimage|placeholder|black[-_]?toner|thumb_)", re.I)

def pick_main_image_from_page(html: str) -> Tuple[Optional[str], str]:
    """
    Возвращает (absolute_image_url | None, h1_text).
    Ищем строго главную картинку карточки:
    - <img itemprop="image"> (src|data-src|data-original)
    - <noscript> с <img> внутри
    Фильтруем заглушки.
    """
    soup = BeautifulSoup(html, "lxml")

    # H1 (для возможного последующего матчинга по тексту)
    h1 = soup.find("h1")
    h1_text = norm(h1.get_text() if h1 else "")

    # 1) Прямо itemprop=image
    cand = soup.find("img", attrs={"itemprop": "image"})
    if cand:
        for attr in ("src", "data-src", "data-original"):
            val = (cand.get(attr) or "").strip()
            if val:
                url = absolutize(val)
                if not BAD_IMG_RE.search(url):
                    return url, h1_text

    # 2) Иногда крутят lazy-load через noscript
    for noscr in soup.find_all("noscript"):
        inner = BeautifulSoup(noscr.string or "", "lxml")
        ii = inner.find("img")
        if ii:
            val = (ii.get("src") or "").strip()
            if val:
                url = absolutize(val)
                if not BAD_IMG_RE.search(url):
                    return url, h1_text

    return None, h1_text

# -------------------- MAIN --------------------
def main():
    os.makedirs(os.path.dirname(INDEX_PATH) or ".", exist_ok=True)

    # Загружаем старый индекс (дополняем, а не затираем).
    index = {"generated_at": int(time.time()), "products": {}}
    if os.path.exists(INDEX_PATH):
        try:
            with open(INDEX_PATH, "r", encoding="utf-8") as f:
                old = json.load(f)
                if isinstance(old, dict) and "products" in old:
                    index["products"] = old.get("products", {})
        except Exception:
            pass

    urls = load_lines(URLS_FILE)
    if not urls:
        print(f"[WARN] {URLS_FILE} пуст — добавь ссылки категорий.")
        save_json(index, INDEX_PATH)
        return

    session = requests.Session()
    total_found = 0
    touched = 0

    for cat in urls:
        try:
            cat_html = fetch(session, cat).text
        except Exception as e:
            print(f"[ERR] категория недоступна: {cat} | {e}")
            continue

        links = extract_product_links(cat_html)
        print(f"[CAT] {cat} → товаров: {len(links)}")

        for purl in links:
            if purl in index["products"]:
                # уже есть — не трогаем (минимум запросов)
                continue

            try:
                ph = fetch(session, purl).text
            except Exception as e:
                print(f"[ERR] карточка недоступна: {purl} | {e}")
                continue

            img, h1 = pick_main_image_from_page(ph)
            index["products"][purl] = {
                "image": img,
                "title": h1,
                "updated": int(time.time()),
            }
            touched += 1
            if img:
                total_found += 1

            # периодически сохраняем прогресс
            if touched % FLUSH_EVERY_N == 0:
                save_json(index, INDEX_PATH)
                print(f"[FLUSH] сохранён прогресс: {touched} карточек, фото найдено: {total_found}")

            time.sleep(REQUEST_DELAY)

    index["generated_at"] = int(time.time())
    save_json(index, INDEX_PATH)
    print(f"[DONE] Всего обработано карточек: {touched}, фото найдено: {total_found}")
    print(f"[OUT]  {INDEX_PATH}")

if __name__ == "__main__":
    main()
