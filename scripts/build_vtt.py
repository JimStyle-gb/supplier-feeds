# -*- coding: utf-8 -*-
"""
B2B VTT → YML (KZT only, без отладочных файлов):
- Логин через /validateLogin с X-CSRF-TOKEN из <meta>.
- Список карточек берем с каталога: <div class="cutoff-off">…<a href=...>Название</a>.
- Фильтр по ключевым словам из docs/vtt_keywords.txt (если файл пустой — берём всё).
- Карточка:
    title: .page_title
    vendorCode (артикул): dl.description_row dt:*Артикул* + dd
    price KZT: .price .price_main b
    breadcrumbs: .breadcrumb
- Валюта всегда KZT (знак ₸ в текст не пишем, чтобы не ломать cp1251).
"""

from __future__ import annotations
import os, re, io, time, html, hashlib
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ---------- ENV ----------
BASE_URL         = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL        = os.getenv("START_URL", f"{BASE_URL}/catalog/")
OUT_FILE         = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN        = os.getenv("VTT_LOGIN", "")
VTT_PASSWORD     = os.getenv("VTT_PASSWORD", "")

HTTP_TIMEOUT     = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "160"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "500"))

MAX_PAGES        = int(os.getenv("MAX_PAGES", "800"))
MAX_PRODUCTS     = int(os.getenv("MAX_PRODUCTS", "5000"))
MAX_WORKERS      = int(os.getenv("MAX_WORKERS", "6"))
MAX_CRAWL_MIN    = int(os.getenv("MAX_CRAWL_MINUTES", "55"))

DISABLE_SSL      = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"

KEYWORDS_FILE    = "docs/vtt_keywords.txt"
SUPPLIER_NAME    = "vtt"
CURRENCY         = "KZT"
ROOT_CAT_ID      = 9600000
ROOT_CAT_NAME    = "VTT"

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-B2B/1.4)"}

# ---------- utils ----------
def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms/1000.0))

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    if DISABLE_SSL:
        # отключаем проверку SSL, если у них криво настроен цепочка сертификатов
        s.verify = False
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
    return s

def get_soup(r: requests.Response) -> Optional[BeautifulSoup]:
    if r.status_code != 200:
        return None
    if len(r.content) < MIN_BYTES:
        # На некоторых страницах контент минимален, но всё равно HTML.
        # Смягчаем это правило: пропустим такие, но крайне редко.
        pass
    return BeautifulSoup(r.text, "html.parser")

def yml_escape(s: str) -> str:
    return html.escape(s or "")

def sha1(s: str) -> str:
    import hashlib as _h
    return _h.sha1((s or "").encode("utf-8", "ignore")).hexdigest()

def load_keywords(path: str) -> List[str]:
    kws: List[str] = []
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                t = line.strip()
                if t and not t.startswith("#"):
                    kws.append(t)
    return kws

def title_matches_keywords(title: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    tl = title.lower()
    for kw in keywords:
        if kw.lower() in tl:
            return True
    return False

# ---------- auth ----------
def login(session: requests.Session) -> bool:
    # 1) получить csrf
    jitter_sleep(REQUEST_DELAY_MS)
    r = session.get(BASE_URL + "/", timeout=HTTP_TIMEOUT)
    s = get_soup(r)
    if not s:
        return False
    meta = s.find("meta", attrs={"name": "csrf-token"})
    csrf = meta["content"] if meta and meta.has_attr("content") else ""

    # 2) отправить форму
    payload = {"login": VTT_LOGIN, "password": VTT_PASSWORD, "remember": "1"}
    headers = {}
    if csrf:
        headers["X-CSRF-TOKEN"] = csrf

    jitter_sleep(REQUEST_DELAY_MS)
    rp = session.post(BASE_URL + "/validateLogin",
                      data=payload, headers=headers, timeout=HTTP_TIMEOUT)
    # сервер отвечает JSONом вида {"result":true,"location":"/catalog"}.
    # проверим, что каталог теперь доступен
    jitter_sleep(REQUEST_DELAY_MS)
    rc = session.get(START_URL, timeout=HTTP_TIMEOUT)
    if rc.status_code != 200:
        return False
    # простая эвристика: на странице каталога не должно быть формы логина
    return "Вход для клиентов" not in rc.text

# ---------- discovery ----------
PRODUCT_HREF_RE = re.compile(r"^https?://[^/]+/catalog/[^?]+", re.I)

def discover_product_urls(session: requests.Session, keywords: List[str]) -> List[str]:
    """
    Берём ссылки из блоков <div class="cutoff-off">…<a href="...">Название</a>
    плюс дособираем с пагинации (?page=2,3,…) если она есть.
    """
    seen_pages: Set[str] = set()
    to_visit: List[str] = [START_URL]
    urls: List[str] = []

    while to_visit and len(seen_pages) < MAX_PAGES:
        page = to_visit.pop(0)
        if page in seen_pages:
            continue
        seen_pages.add(page)

        jitter_sleep(REQUEST_DELAY_MS)
        r = session.get(page, timeout=HTTP_TIMEOUT)
        s = get_soup(r)
        if not s:
            continue

        # 1) карточки из блока cutoff-off
        for box in s.select("div.cutoff-off"):
            # второй <a> — это ссылка на товар; первый — кнопка камеры
            anchors = [a for a in box.find_all("a", href=True)]
            for a in anchors:
                href = a["href"].strip()
                txt = a.get_text(" ", strip=True)
                if not href or not txt or "btn_pic" in (a.get("class") or []):
                    continue
                # абсолютный урл
                absu = urljoin(page, href)
                if not PRODUCT_HREF_RE.search(absu):
                    continue
                # фильтр по ключевым словам уже на этапе сбора
                if not title_matches_keywords(txt, keywords):
                    continue
                urls.append(absu)

        # 2) пагинация: ищем ?page= или ссылки с «Следующая»
        pagers = []
        for a in s.select('a[href*="page="], a.page-link, a[rel="next"]'):
            href = a.get("href")
            if not href:
                continue
            absu = urljoin(page, href)
            if absu not in seen_pages and absu not in to_visit:
                pagers.append(absu)
        # подмешаем вперед (BFS)
        for u in pagers:
            if len(seen_pages) + len(to_visit) >= MAX_PAGES:
                break
            to_visit.append(u)

        if len(urls) >= MAX_PRODUCTS:
            break

    # уникализируем и обрезаем лимитом
    uniq = list(dict.fromkeys(urls))
    if len(uniq) > MAX_PRODUCTS:
        uniq = uniq[:MAX_PRODUCTS]
    return uniq

# ---------- product parsing ----------
def parse_price_kzt(s: BeautifulSoup) -> Optional[float]:
    # Ищем блок <div class="price"> … <span class="price_main"><b>9621.12</b> <ins>T</ins></span>
    wrap = s.select_one(".price .price_main")
    if not wrap:
        wrap = s.select_one(".price_main")
    if not wrap:
        return None
    b = wrap.find("b")
    if b:
        txt = b.get_text("", strip=True)
    else:
        txt = wrap.get_text(" ", strip=True)

    # вытаскиваем число
    t = txt.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None

def parse_vendor_code(s: BeautifulSoup) -> Optional[str]:
    # dl.description_row > dt:contains("Артикул") + dd
    for dt in s.select("dl.description_row dt"):
        label = dt.get_text(" ", strip=True).lower()
        if "артикул" in label:
            dd = dt.find_next_sibling("dd")
            if dd:
                v = dd.get_text(" ", strip=True)
                v = re.sub(r"\s+", "", v)
                return v
    return None

def parse_title(s: BeautifulSoup) -> Optional[str]:
    el = s.select_one(".page_title")
    if el:
        t = el.get_text(" ", strip=True)
        return t[:200].rstrip()
    # запасной вариант — заголовок окна
    if s.title:
        return s.title.get_text(" ", strip=True)[:200].rstrip()
    return None

def parse_breadcrumbs(s: BeautifulSoup) -> List[str]:
    names: List[str] = []
    for bc in s.select(".breadcrumb, .breadcrumbs, ul.breadcrumb, [class*='breadcrumb']"):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if t and t.lower() not in ("главная", "home"):
                names.append(t)
        if names:
            break
    # уберем дубли
    return [n for i, n in enumerate(names) if n and n.strip() and n not in names[:i]]

def stable_cat_id(text: str, prefix: int = 9700000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories(paths: List[List[str]]) -> Tuple[List[Tuple[int, str, int]], Dict[Tuple[str, ...], int]]:
    cat_map: Dict[Tuple[str, ...], int] = {}
    out: List[Tuple[int, str, int]] = []
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        parent = ROOT_CAT_ID
        cur: List[str] = []
        for name in clean:
            cur.append(name)
            key = tuple(cur)
            if key in cat_map:
                parent = cat_map[key]
                continue
            cid = stable_cat_id(" / ".join(cur))
            cat_map[key] = cid
            out.append((cid, name, parent))
            parent = cid
    return out, cat_map

def parse_product(session: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    jitter_sleep(REQUEST_DELAY_MS)
    r = session.get(url, timeout=HTTP_TIMEOUT)
    s = get_soup(r)
    if not s:
        return None

    title = parse_title(s)
    if not title:
        return None

    vendor_code = parse_vendor_code(s)  # берём артикул ТОЛЬКО из карточки
    if not vendor_code:
        return None

    price = parse_price_kzt(s)  # KZT из .price_main
    if price is None or price <= 0:
        return None

    crumbs = parse_breadcrumbs(s)

    return {
        "title": title,
        "vendorCode": vendor_code,
        "price": price,
        "breadcrumbs": crumbs,
        "url": url,
    }

# ---------- YML ----------
def build_yml(categories: List[Tuple[int, str, int]], offers: List[Tuple[int, Dict[str, Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SUPPLIER_NAME)}</name>")
    out.append('<currencies><currency id="KZT" rate="1" /></currencies>')

    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{yml_escape(name)}</category>")
    out.append("</categories>")

    out.append("<offers>")
    for cid, it in offers:
        price_txt = str(int(it["price"])) if float(it["price"]).is_integer() else f'{it["price"]}'
        out += [
            f"<offer id=\"{yml_escape(it['vendorCode'])}\" available=\"true\" in_stock=\"true\">",
            f"<name>{yml_escape(it['title'])}</name>",
            f"<vendor>{yml_escape(SUPPLIER_NAME)}</vendor>",
            f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>",
            f"<price>{price_txt}</price>",
            "<currencyId>KZT</currencyId>",
            f"<categoryId>{cid}</categoryId>",
            f"<url>{yml_escape(it['url'])}</url>",
            f"<description>{yml_escape(it['title'])}</description>",
            "<quantity_in_stock>1</quantity_in_stock>",
            "<stock_quantity>1</stock_quantity>",
            "<quantity>1</quantity>",
            "</offer>",
        ]
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------- MAIN ----------
def main() -> int:
    start_ts = datetime.utcnow()
    deadline = start_ts + timedelta(minutes=MAX_CRAWL_MIN)

    session = make_session()
    if not (VTT_LOGIN and VTT_PASSWORD):
        print("Error: credentials are empty (VTT_LOGIN/VTT_PASSWORD).")
        return 2

    if not login(session):
        print("Error: login failed")
        return 2

    keywords = load_keywords(KEYWORDS_FILE)
    product_urls = discover_product_urls(session, keywords)
    print(f"[discover] product urls: {len(product_urls)}")

    if not product_urls:
        # пустая выдача — пишем пустой YML, но с корневой категорией
        xml = build_yml([], [])
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(xml)
        print(f"[done] items: 0 -> {OUT_FILE}")
        return 2

    items: List[Dict[str, Any]] = []

    def worker(u: str) -> Optional[Dict[str, Any]]:
        if datetime.utcnow() > deadline:
            return None
        try:
            return parse_product(session, u)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(worker, u): u for u in product_urls}
        for fut in as_completed(futures):
            rec = fut.result()
            if rec:
                items.append(rec)

    if not items:
        xml = build_yml([], [])
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(xml)
        print(f"[done] items: 0 -> {OUT_FILE}")
        return 2

    # категории по крошкам
    paths = [it.get("breadcrumbs", []) for it in items if it.get("breadcrumbs")]
    cats, path_map = build_categories(paths)

    offers: List[Tuple[int, Dict[str, Any]]] = []
    for it in items:
        # ищем максимально длинный путь, который есть в карте
        cid = ROOT_CAT_ID
        crumbs = it.get("breadcrumbs") or []
        key = tuple([c for c in crumbs if c and c.strip()])
        while key and key not in path_map:
            key = key[:-1]
        if key and key in path_map:
            cid = path_map[key]
        offers.append((cid, it))

    xml = build_yml(cats, offers)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, cats: {len(cats)} -> {OUT_FILE}")
    return 0


if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("Error:", e)
        sys.exit(2)
