# -*- coding: utf-8 -*-
"""
ВАРИАНТ B — ШАГ 2: ДОБАВЛЕНИЕ <picture> В YML ПО ИНДЕКСУ
Берём готовый docs/copyline.yml (из build_copyline.py), пробегаем по офферам:
- читаем <vendorCode> как исходный артикул (без префиксов)
- находим URL карточки через ПОИСК на сайте (несколько вариантов ссылок)
- по найденному URL берём фотку из индекса docs/copyline_photo_index.json
- проставляем <picture> (если фотка валидна)
Скрипт НЕ открывает карточку товара — все фото берёт из индекса.

Переменные окружения:
- YML_PATH         (default: docs/copyline.yml) — входной YML
- OUT_FILE         (default: docs/copyline.yml) — куда писать результат (можно тот же файл)
- INDEX_PATH       (default: docs/copyline_photo_index.json) — индекс из шага 1
- OUTPUT_ENCODING  (default: windows-1251)
- REQUEST_DELAY_MS (default: 700) — пауза между запросами (мс)
- BACKOFF_MAX_MS   (default: 12000)
- MAX_RETRIES      (default: 3)
"""

from __future__ import annotations
import os, re, io, time
from typing import Optional, Dict, Any
import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET

BASE = "https://copyline.kz"
UA_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

YML_PATH        = os.getenv("YML_PATH", "docs/copyline.yml")
OUT_FILE        = os.getenv("OUT_FILE", "docs/copyline.yml")
INDEX_PATH      = os.getenv("INDEX_PATH", "docs/copyline_photo_index.json")
ENC             = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

REQUEST_DELAY   = max(100, int(os.getenv("REQUEST_DELAY_MS", "700"))) / 1000.0
BACKOFF_MAX     = max(1000, int(os.getenv("BACKOFF_MAX_MS", "12000"))) / 1000.0
MAX_RETRIES     = max(1, int(os.getenv("MAX_RETRIES", "3")))

BAD_IMG_RE = re.compile(r"(noimage|placeholder|black[-_]?toner|thumb_)", re.I)

# --- HTTP utils ---
def fetch(session: requests.Session, url: str) -> requests.Response:
    delay = REQUEST_DELAY
    tries = 0
    while True:
        tries += 1
        r = session.get(url, headers=UA_HEADERS, timeout=60, allow_redirects=True)
        if r.status_code < 400:
            return r
        if r.status_code in (429, 403, 502, 503, 504):
            time.sleep(min(delay, BACKOFF_MAX))
            delay = min(delay * 2, BACKOFF_MAX)
            if tries < MAX_RETRIES:
                continue
        r.raise_for_status()

def absolutize(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return requests.compat.urljoin(BASE, url)

# --- Поиск карточки по артикулу ---
SEARCH_CANDIDATES = [
    # Классический com_search
    "/index.php?option=com_search&searchword={q}",
    "/?option=com_search&searchword={q}",
    "/search?searchword={q}",
    "/component/search/?searchword={q}",
]

def find_product_url_by_code(session: requests.Session, code: str) -> Optional[str]:
    """
    Пробуем несколько URL поиска. Цель — получить ссылку на /goods/...html.
    Если страница сразу редиректит на карточку — возьмём r.url.
    Иначе распарсим выдачу и найдём первую подходящую ссылку.
    """
    code = code.strip()
    if not code:
        return None

    for pattern in SEARCH_CANDIDATES:
        url = BASE + pattern.format(q=requests.utils.quote(code))
        try:
            r = fetch(session, url)
        except Exception:
            continue

        # Если нас сразу перекинуло на карточку — отлично
        if "/goods/" in r.url and r.url.endswith(".html"):
            return r.url

        # Иначе парсим выдачу и ищем ссылку на карточку
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/goods/" in href and href.endswith(".html"):
                return absolutize(href)

        # Иногда выдача спрятана в <div class="search-results"> — уже покрыто find_all("a")

        time.sleep(REQUEST_DELAY)

    return None

# --- YML helpers ---
def read_xml(path: str) -> ET.ElementTree:
    with open(path, "rb") as f:
        data = f.read()
    return ET.ElementTree(ET.fromstring(data))

def write_xml(tree: ET.ElementTree, path: str, encoding: str):
    # Пишем строго с объявлением xml и нужной кодировкой (windows-1251 для Satu)
    buf = io.BytesIO()
    tree.write(buf, encoding=encoding, xml_declaration=True)
    with open(path, "wb") as f:
        f.write(buf.getvalue())

# --- MAIN ---
def main():
    # Загружаем индекс
    try:
        import json
        with open(INDEX_PATH, "r", encoding="utf-8") as f:
            index = json.load(f)
        url2img: Dict[str, Dict[str, Any]] = index.get("products", {})
    except Exception:
        print(f"[ERR] Не удалось прочитать индекс: {INDEX_PATH}")
        url2img = {}

    if not url2img:
        print("[WARN] Индекс пуст — сначала запусти build_copyline_index.py")
        return

    # Загружаем YML
    tree = read_xml(YML_PATH)
    root = tree.getroot()
    shop = root.find("shop")
    offers = shop.find("offers") if shop is not None else None
    if offers is None:
        print("[ERR] В YML нет секции <offers>")
        return

    session = requests.Session()
    updated = 0
    checked = 0

    for offer in offers.findall("offer"):
        checked += 1

        # Берём оригинальный артикул (без префиксов) из <vendorCode>
        vc = offer.findtext("vendorCode", "").strip()
        if not vc:
            continue

        # Если уже есть <picture> — пропускаем (не перезаписываем)
        if offer.find("picture") is not None:
            continue

        purl = find_product_url_by_code(session, vc)
        if not purl:
            print(f"[skip] {vc}: карточка не найдена через поиск")
            continue

        entry = url2img.get(purl)
        img = (entry or {}).get("image")

        # фильтруем мусор/заглушки
        if img and not BAD_IMG_RE.search(img):
            pic = ET.Element("picture")
            pic.text = img
            offer.append(pic)
            updated += 1
            print(f"[ok]   {vc} → {img}")
        else:
            print(f"[skip] {vc}: в индексе нет валидной фотки ({purl})")

        time.sleep(REQUEST_DELAY)

    # Если ничего не поменяли — не трогаем файл
    if updated == 0:
        print("Нет ни одной новой фотографии — выходим без изменений.")
        return

    write_xml(tree, OUT_FILE, ENC)
    print(f"[DONE] Обновлено офферов: {updated} / проверено: {checked}")
    print(f"[OUT]  {OUT_FILE}")

if __name__ == "__main__":
    main()
