# -*- coding: utf-8 -*-
"""
VTT (b2b.vtt.ru) → Satu YML (минимально необходимое):
- Логинимся POST /validateLogin (X-CSRF-TOKEN из <meta name="csrf-token">).
- Из каталога собираем ссылки на товары.
- В КАРТОЧКЕ парсим: Название (h1), Артикул (dt:contains("Артикул:") + dd), Цена (span.price_main > b).
- Валюта: ЖЁСТКО KZT (тенге). Позиции без цены/артикула — пропускаем.
- Категории не строим (кладём всё в один корень).
- Без debug-файлов.
"""

from __future__ import annotations
import os, re, time, html
from typing import List, Dict, Any, Optional, Tuple, Set
from urllib.parse import urljoin
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ------------ ENV ------------
BASE_URL         = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL        = os.getenv("START_URL", f"{BASE_URL}/catalog/")

OUT_FILE         = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN        = os.getenv("VTT_LOGIN", "")
VTT_PASSWORD     = os.getenv("VTT_PASSWORD", "")

HTTP_TIMEOUT     = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "180"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "700"))
MAX_PAGES        = int(os.getenv("MAX_PAGES", "800"))
MAX_CRAWL_MIN    = int(os.getenv("MAX_CRAWL_MINUTES", "50"))

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"

SUPPLIER_NAME    = "vtt"
CURRENCY_ID      = "KZT"   # только тенге
ROOT_CAT_ID      = 9600000
ROOT_CAT_NAME    = "VTT"

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-b2b-YML/1.2)"}

# ------------ net utils ------------
def jitter_sleep(ms: int) -> None:
    if ms > 0: time.sleep(ms / 1000.0)

def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    if DISABLE_SSL_VERIFY:
        requests.packages.urllib3.disable_warnings()  # type: ignore
        s.verify = False
        print("[ssl] verification disabled by env")
    return s

def get_bytes(session: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return None
        b = r.content
        if b is None or len(b) < MIN_BYTES: return None
        return b
    except Exception:
        return None

def soup_html(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "html.parser")

def _txt(el) -> str:
    return (el.get_text(" ", strip=True) if el else "").strip()

# ------------ login ------------
def fetch_csrf(session: requests.Session) -> Optional[str]:
    # логин-форма висит на корне /
    b = get_bytes(session, f"{BASE_URL}/")
    if not b: return None
    s = soup_html(b)
    meta = s.find("meta", attrs={"name": "csrf-token"})
    return meta.get("content").strip() if meta and meta.get("content") else None

def login(session: requests.Session, login: str, password: str) -> bool:
    if not login or not password:
        print("Error: no VTT_LOGIN/VTT_PASSWORD provided")
        return False

    csrf = fetch_csrf(session)
    if not csrf:
        print("Error: cannot get CSRF token")
        return False

    headers = {"X-CSRF-TOKEN": csrf, "X-Requested-With": "XMLHttpRequest"}
    data = {"login": login, "password": password}
    try:
        r = session.post(f"{BASE_URL}/validateLogin", headers=headers, data=data, timeout=HTTP_TIMEOUT)
        j = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
    except Exception:
        print("Error: login request failed")
        return False

    ok = bool(j.get("result"))
    if ok:
        loc = j.get("location") or START_URL
        try:
            session.get(loc, timeout=HTTP_TIMEOUT)
        except Exception:
            pass
        print("[login] success")
        return True

    print("Error: login failed")
    return False

# ------------ listing parsing ------------
def collect_links_from_page(s: BeautifulSoup) -> List[str]:
    """
    На странице каталога блоки с названием — <div class="cutoff-off">
    Внутри — две ссылки: первая "камера" (btn_naked), вторая — ссылка на товар.
    """
    urls: List[str] = []
    for box in s.select("div.cutoff-off"):
        # берём <a> со ссылкой на товар, исключив иконку-камеру
        for a in box.select('a[href]'):
            classes = a.get("class") or []
            if "btn_naked" in classes:
                continue
            href = a.get("href") or ""
            if "/catalog/" in href:
                urls.append(href.strip())
                break
    # уникализируем с сохранением порядка
    seen: Set[str] = set()
    uniq: List[str] = []
    for u in urls:
        absu = urljoin(START_URL, u)
        if absu not in seen:
            seen.add(absu); uniq.append(absu)
    return uniq

def discover_all_product_links(session: requests.Session) -> List[str]:
    """
    Пробуем простую пагинацию ?page=N.
    Останавливаемся, когда подряд встретили 3 пустые страницы или истёк лимит.
    """
    product_urls: List[str] = []
    empty_streak = 0
    started = datetime.utcnow()
    for i in range(1, MAX_PAGES + 1):
        if datetime.utcnow() - started > timedelta(minutes=MAX_CRAWL_MIN):
            break
        page_url = START_URL if i == 1 else f"{START_URL}?page={i}"
        jitter_sleep(REQUEST_DELAY_MS)
        b = get_bytes(session, page_url)
        if not b:
            empty_streak += 1
            if empty_streak >= 3: break
            continue
        s = soup_html(b)
        links = collect_links_from_page(s)
        if not links:
            empty_streak += 1
            if empty_streak >= 3: break
            continue
        empty_streak = 0
        for u in links:
            if u not in product_urls:
                product_urls.append(u)
        # иногда каталог короткий — если на странице мало ссылок, всё равно идём дальше до трёх пустых подряд
    return product_urls

# ------------ product parsing ------------
def parse_product(session: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    jitter_sleep(REQUEST_DELAY_MS)
    b = get_bytes(session, url)
    if not b: return None
    s = soup_html(b)

    # title
    title = _txt(s.select_one("h1"))
    if not title:
        return None

    # sku (только из карточки)
    sku = None
    for dt in s.select("dt"):
        if "артикул" in _txt(dt).lower():
            dd = dt.find_next_sibling("dd")
            sku = _txt(dd)
            break
    if not sku:
        return None

    # price (только из карточки)
    price_wrap = s.select_one("span.price_main") or s.select_one("span.price")
    if not price_wrap: 
        return None
    val_el = price_wrap.find("b") or price_wrap
    raw = _txt(val_el)
    if not raw:
        return None
    raw = raw.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    m = re.search(r"\d+(?:\.\d+)?", raw)
    if not m:
        return None
    price = float(m.group(0))

    # картинка — по возможности
    pic = None
    og = s.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        pic = og["content"].strip()

    return {
        "title": title,
        "vendorCode": sku,
        "price": price,
        "currency": CURRENCY_ID,  # всегда KZT
        "url": url,
        "picture": pic,
    }

# ------------ YML ------------
def yml_escape(s: str) -> str:
    return html.escape(s or "")

def build_yml(offers: List[Dict[str, Any]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SUPPLIER_NAME)}</name>")
    out.append(f"<currencies><currency id=\"{CURRENCY_ID}\" rate=\"1\" /></currencies>")
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    out.append("</categories>")
    out.append("<offers>")
    for it in offers:
        price = it["price"]
        # без лишних нулей
        price_txt = str(int(price)) if float(price).is_integer() else f"{price}"
        out.append(f"<offer id=\"{yml_escape(it['vendorCode'])}\" available=\"true\" in_stock=\"true\">")
        out.append(f"<name>{yml_escape(it['title'])}</name>")
        out.append(f"<vendor>{yml_escape(SUPPLIER_NAME)}</vendor>")
        out.append(f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>")
        out.append(f"<price>{price_txt}</price>")
        out.append(f"<currencyId>{CURRENCY_ID}</currencyId>")
        out.append(f"<categoryId>{ROOT_CAT_ID}</categoryId>")
        if it.get("url"):     out.append(f"<url>{yml_escape(it['url'])}</url>")
        if it.get("picture"): out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        # простой desc
        out.append(f"<description>{yml_escape(it['title'])}</description>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ------------ MAIN ------------
def main() -> int:
    sess = new_session()
    if not login(sess, VTT_LOGIN, VTT_PASSWORD):
        return 2

    started = datetime.utcnow()
    links = discover_all_product_links(sess)
    print(f"[discover] product urls: {len(links)}")

    offers: List[Dict[str, Any]] = []
    for u in links:
        if datetime.utcnow() - started > timedelta(minutes=MAX_CRAWL_MIN):
            break
        item = parse_product(sess, u)
        if not item:
            continue
        offers.append(item)

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(offers)
    # cp1251 может не знать некоторые символы — игнорируем редкие
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)} -> {OUT_FILE}")
    return 0 if offers else 2

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("Error:", e)
        sys.exit(2)
