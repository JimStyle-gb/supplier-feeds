# scripts/scrape_copyline_photos_by_list.py
# -*- coding: utf-8 -*-
"""
Назначение:
  - Пройти по списку артикулов (docs/copyline_articles.txt или ARTICLES_FILE из env),
  - Для каждого артикула найти карточку товара на https://copyline.kz через поиск,
  - Открыть карточку и вытащить ссылку на главное изображение из:
        <img itemprop="image" ... src="https://copyline.kz/.../img_products/....jpg">
  - Открыть docs/copyline.yml (YML каталога), найти <offer> по <vendorCode> и
    добавить/обновить <picture> на найденный src.
  - Сохранить обновлённый docs/copyline.yml в указанной кодировке.

Примечания:
  - Мы НЕ сохраняем картинки в репозиторий — только ссылки (это то, что просил заказчик).
  - Поиск делаем «по-вежливому»: задержки между запросами + ретраи с backoff.
  - Любые ошибки по отдельному артикулу логируем и идём дальше.
"""

from __future__ import annotations
import os, re, time, io, sys, urllib.parse, random
from typing import List, Optional, Dict
import requests
from bs4 import BeautifulSoup
from xml.etree.ElementTree import ElementTree, Element, SubElement

# ---------- Конфигурация через ENV с дефолтами ----------
BASE = "https://copyline.kz"

YML_PATH        = os.getenv("YML_PATH", "docs/copyline.yml")
ARTICLES_FILE   = os.getenv("ARTICLES_FILE", "docs/copyline_articles.txt")
ENC             = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "600"))   # базовая задержка между запросами
MAX_RETRIES      = int(os.getenv("MAX_RETRIES", "3"))          # ретраи на один HTTP шаг
BACKOFF_MAX_MS   = int(os.getenv("BACKOFF_MAX_MS", "12000"))   # максимум backoff

UA_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ---------- Утилиты ----------
def norm(s: Optional[str]) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def sleep_polite(mult: float = 1.0) -> None:
    """Маленькая «человеческая» задержка между запросами."""
    base = REQUEST_DELAY_MS / 1000.0
    jitter = random.uniform(0.2, 0.5) * base
    time.sleep(base * mult + jitter)

def fetch(url: str, *, allow_redirects: bool = True) -> requests.Response:
    """
    GET с ретраями и экспоненциальным бэкоффом.
    """
    last_err = None
    delay = REQUEST_DELAY_MS / 1000.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=UA_HEADERS, timeout=60, allow_redirects=allow_redirects)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if attempt >= MAX_RETRIES:
                break
            # экспоненциальный backoff (с ограничением)
            sleep = min(delay * (2 ** (attempt - 1)), BACKOFF_MAX_MS / 1000.0)
            time.sleep(sleep)
    raise RuntimeError(f"HTTP failed for {url}: {last_err}")

def absolutize(href: str) -> str:
    """Любой относительный путь превращаем в абсолютный URL сайта."""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urllib.parse.urljoin(BASE, href)

def read_articles_list(path: str) -> List[str]:
    """
    Загружаем артикулы из файла (по одному на строку).
    Пустые/комментарии (#...) пропускаем. Убираем дубликаты, порядок сохраняем.
    """
    seen = set()
    out: List[str] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = norm(line)
                if not s or s.startswith("#"):
                    continue
                if s not in seen:
                    out.append(s)
                    seen.add(s)
    except FileNotFoundError:
        pass
    return out

# ---------- Поиск карточки по артикулу ----------
SEARCH_ENDPOINTS = [
    # JoomShopping поиск по keyword
    "/index.php?option=com_jshopping&controller=search&task=view&keyword={q}",
    # Иногда работает общий поиск
    "/index.php?searchword={q}&searchphrase=all&option=com_search",
    # Простой короткий вариант
    "/search?keyword={q}",
]

def find_product_url_by_article(article: str) -> Optional[str]:
    """
    Пробуем несколько поисковых URL. Из страницы результатов берём первую ссылку,
    ведущую на карточку товара (/goods/ или /product/ ... .html).
    Возвращаем абсолютный URL карточки или None.
    """
    q = urllib.parse.quote(article)
    for tpl in SEARCH_ENDPOINTS:
        url = absolutize(tpl.format(q=q))
        try:
            resp = fetch(url)
            html = resp.text
        except Exception:
            continue

        soup = BeautifulSoup(html, "lxml")

        # Приоритет: явные ссылки на карточки
        for a in soup.select('a[href*="/goods/"], a[href*="/product/"]'):
            href = a.get("href") or ""
            if href.endswith(".html"):
                return absolutize(href)

        # Резерв: любые ссылки вида *.html с ключом goods/product
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ("/goods/" in href or "/product/" in href) and href.endswith(".html"):
                return absolutize(href)

        sleep_polite(0.6)
    return None

# ---------- Вытаскиваем главное фото из карточки ----------
def extract_main_image_url(product_url: str) -> Optional[str]:
    """
    Открываем карточку и ищем <img itemprop="image" ... src="...">.
    Берём значение src и делаем абсолютным. Если не нашли — None.
    """
    try:
        r = fetch(product_url)
    except Exception:
        return None

    soup = BeautifulSoup(r.text, "lxml")

    # 1) Самый надёжный способ — itemprop="image"
    img = soup.find("img", attrs={"itemprop": "image"})
    if img and img.get("src"):
        return absolutize(img["src"])

    # 2) Резерв — main_image_* по id
    img = soup.find("img", id=re.compile(r"^main_image_\d+"))
    if img and img.get("src"):
        return absolutize(img["src"])

    # 3) Резерв — первая картинка из блока товара
    img = soup.select_one('div.productfull img[src]')
    if img and img.get("src"):
        return absolutize(img["src"])

    return None

# ---------- Работа с YML (XML) ----------
def load_yml(path: str) -> ElementTree:
    """
    Загружаем существующий docs/copyline.yml. Если файла нет — создаём минимальный.
    """
    if not os.path.exists(path):
        # Минимальный каркас YML (если вдруг запускаем отдельно)
        root = Element("yml_catalog"); shop = SubElement(root, "shop")
        SubElement(shop, "name").text = "copyline"
        curr = SubElement(shop, "currencies"); SubElement(curr, "currency", {"id":"KZT","rate":"1"})
        SubElement(shop, "categories")  # пустые
        SubElement(shop, "offers")      # пустые
        return ElementTree(root)

    with open(path, "rb") as f:
        data = f.read()
    # В ElementTree.fromstring — класс-метод, поэтому создаём новый объект
    root = ElementTree(Element("stub"))
    root._setroot(ElementTree.fromstring(data))  # type: ignore[attr-defined]
    return root

def ensure_picture_child(offer_el: Element) -> Element:
    """
    Возвращает (существующий или новый) тег <picture> внутри <offer>.
    """
    pic = offer_el.find("picture")
    if pic is None:
        pic = SubElement(offer_el, "picture")
    return pic

def update_yml_pictures(tree: ElementTree, pictures: Dict[str, str]) -> int:
    """
    Вставляем/обновляем <picture> для офферов, где <vendorCode> совпадает со словарём pictures.
    Возвращаем количество обновлённых позиций.
    """
    root = tree.getroot()
    shop = root.find("shop")
    if shop is None:
        return 0
    offers = shop.find("offers")
    if offers is None:
        return 0

    updated = 0
    for offer in offers.findall("offer"):
        vc = offer.findtext("vendorCode")
        if not vc:
            continue
        if vc in pictures:
            pic_el = ensure_picture_child(offer)
            new_url = pictures[vc]
            # Пропускаем если уже одинаково
            if (pic_el.text or "").strip() != new_url:
                pic_el.text = new_url
                updated += 1
    return updated

def save_yml(tree: ElementTree, path: str, enc: str) -> None:
    """
    Сохраняем XML с нужной кодировкой и декларацией.
    """
    buf = io.BytesIO()
    tree.write(buf, encoding=enc, xml_declaration=True)
    with open(path, "wb") as f:
        f.write(buf.getvalue())

# ---------- MAIN ----------
def main():
    # 1) Загружаем список артикулов
    articles = read_articles_list(ARTICLES_FILE)
    if not articles:
        print(f"ERROR: список артикулов пуст: {ARTICLES_FILE}", file=sys.stderr)
        sys.exit(1)

    # 2) Находим URL карточек и вытаскиваем ссылки на фото
    found_pics: Dict[str, str] = {}
    for art in articles:
        try:
            # Поиск карточки
            url = find_product_url_by_article(art)
            if not url:
                print(f"[skip] {art}: карточка не найдена")
                sleep_polite()
                continue

            # Главная картинка
            img_url = extract_main_image_url(url)
            if not img_url:
                print(f"[skip] {art}: фото не найдено в карточке")
                sleep_polite()
                continue

            found_pics[art] = img_url
            print(f"[ok]   {art} -> {img_url}")

        except Exception as e:
            print(f"[err]  {art}: {e}", file=sys.stderr)
        finally:
            sleep_polite()

    if not found_pics:
        print("Нет ни одной найденной ссылки на фото, выходим без изменений.")
        return

    # 3) Загружаем YML и обновляем <picture> по vendorCode == артикулу
    tree = load_yml(YML_PATH)
    updated_cnt = update_yml_pictures(tree, found_pics)
    save_yml(tree, YML_PATH, ENC)

    print(f"Готово. Обновлено <picture>: {updated_cnt} из {len(found_pics)} найденных.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
