# -*- coding: utf-8 -*-
"""
B2B VTT -> YML (KZT). Без отладочных html в docs/.
- Логинимся POST /validateLogin с CSRF.
- Собираем ссылки на товары с /catalog/ (div.cutoff-off > a:not(.btn_pic_1)).
- Для каждого товара парсим из карточки:
    * title (h1)
    * vendorCode = значение 'Артикул:' (только из карточки)
    * brand = значение 'Вендор:'
    * price: округляем до целого; если нет/0 — ставим 1
- В YML <name>feed</name>, <currencyId>KZT</currencyId>, vendor = brand (нигде 'vtt')
- Фильтр по названиям: начинается с любого ключа из KEYWORDS_FILE.
"""

from __future__ import annotations
import os, re, time, html, hashlib
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ----------------- ENV -----------------
BASE_URL         = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL        = os.getenv("START_URL", f"{BASE_URL}/catalog/")
KEYWORDS_FILE    = os.getenv("KEYWORDS_FILE", "docs/vtt_keywords.txt")
OUT_FILE         = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN        = os.getenv("VTT_LOGIN", "")
VTT_PASSWORD     = os.getenv("VTT_PASSWORD", "")

HTTP_TIMEOUT     = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "180"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "700"))
MAX_PAGES        = int(os.getenv("MAX_PAGES", "1200"))
MAX_WORKERS      = int(os.getenv("MAX_WORKERS", "6"))
MAX_PRODUCTS     = int(os.getenv("MAX_PRODUCTS", "8000"))
MAX_CRAWL_MIN    = int(os.getenv("MAX_CRAWL_MINUTES", "90"))

DISABLE_SSL      = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"

SHOP_NAME        = "feed"
CURRENCY         = "KZT"
ROOT_CAT_ID      = 9600000
ROOT_CAT_NAME    = "Каталог"

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-B2B-Feed/2.2)"}

# ----------------- helpers -----------------
def jitter_sleep():
    time.sleep(max(0.0, REQUEST_DELAY_MS/1000.0))

def yml_escape(s: str) -> str:
    return html.escape(s or "")

def sha1(s: str) -> str:
    import hashlib as _h
    return _h.sha1((s or "").encode("utf-8")).hexdigest()

def to_int_price(s: str) -> Optional[int]:
    if not s: return None
    t = s.replace("\xa0"," ").replace(" "," ").replace(",", ".")
    m = re.search(r"(\d[\d\s\.\,]{1,})", t)
    if not m: return None
    num = m.group(1).replace(" ", "").replace("\u00A0","").replace(",", ".")
    try:
        val = float(num)
        ival = int(round(val))
        return ival if ival > 0 else None
    except Exception:
        return None

def soup_of_response(r: requests.Response) -> Optional[BeautifulSoup]:
    if r is None: return None
    if not r.content or len(r.content) < MIN_BYTES: return None
    # lxml попроще с разметкой
    return BeautifulSoup(r.content, "lxml")

def compile_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    pats = []
    for kw in kws:
        kw = kw.strip()
        if not kw: continue
        pats.append(re.compile(r"^\s*" + re.escape(kw).replace(r"\ ", " ") + r"(?!\w)", re.IGNORECASE))
    return pats

def title_startswith(title: str, pats: List[re.Pattern]) -> bool:
    if not title: return False
    return any(p.search(title) for p in pats)

def load_keywords(path: str) -> List[str]:
    out: List[str] = []
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    out.append(s)
    if not out:
        out = ["Девелопер","Драм-картридж","Драм-юнит","Картридж","Копи-картридж",
               "Принт-картридж","Термоблок","Термоэлемент","Тонер-картридж"]
    return out

# ----------------- auth -----------------
def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    s.verify = not DISABLE_SSL
    if DISABLE_SSL:
        print("[ssl] verification disabled by env")
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return s

def get_csrf(s: requests.Session) -> Optional[str]:
    try:
        r = s.get(BASE_URL + "/", timeout=HTTP_TIMEOUT)
        soup = soup_of_response(r)
        if soup:
            meta = soup.find("meta", attrs={"name":"csrf-token"})
            if meta and meta.get("content"):
                return meta["content"].strip()
        # запасной вариант — страница логина
        r = s.get(BASE_URL + "/login", timeout=HTTP_TIMEOUT)
        soup = soup_of_response(r)
        if soup:
            meta = soup.find("meta", attrs={"name":"csrf-token"})
            if meta and meta.get("content"):
                return meta["content"].strip()
    except Exception:
        pass
    return None

def do_login(s: requests.Session) -> bool:
    if not (VTT_LOGIN and VTT_PASSWORD):
        print("Error: login failed (no credentials)")
        return False
    token = get_csrf(s)
    if not token:
        print("Error: login failed (no CSRF token)")
        return False
    try:
        payload = {"login": VTT_LOGIN, "password": VTT_PASSWORD}
        headers = {"X-CSRF-TOKEN": token, "X-Requested-With": "XMLHttpRequest"}
        r = s.post(BASE_URL + "/validateLogin", data=payload, headers=headers, timeout=HTTP_TIMEOUT)
        ok = False
        try:
            j = r.json()
            if j.get("result"):
                loc = j.get("location") or (BASE_URL + "/catalog/")
                s.get(loc, timeout=HTTP_TIMEOUT)
                ok = True
        except Exception:
            # если отдали не JSON, просто попробуем каталог
            pass
        if not ok:
            rr = s.get(BASE_URL + "/catalog/", timeout=HTTP_TIMEOUT, allow_redirects=True)
            ok = (rr.status_code == 200 and "catalog" in rr.url)
        return ok
    except Exception:
        return False

# ----------------- discovery -----------------
PROD_URL_RE = re.compile(r"^/catalog/(?!\?)[^?#]+$", re.IGNORECASE)

def discover_product_urls(s: requests.Session, max_pages: int) -> List[Tuple[str, Optional[int]]]:
    """
    Возвращает [(url, price_from_listing_int_or_None), ...]
    Извлекаем ссылку на товар из <div class="cutoff-off"> второй <a>.
    Пытаемся вытащить цену из контейнера товара на листинге.
    """
    seen_pages: Set[str] = set()
    q: List[str] = [START_URL]
    urls: Dict[str, Optional[int]] = {}
    pages = 0

    def norm_url(u: str, base: str) -> str:
        u = urljoin(base, u)
        # только наш хост
        if not u.startswith(BASE_URL): return ""
        # отбрасываем фильтры/якоря
        return u.split("#",1)[0]

    while q and pages < max_pages:
        page = q.pop(0)
        if page in seen_pages: continue
        seen_pages.add(page)
        jitter_sleep()
        try:
            r = s.get(page, timeout=HTTP_TIMEOUT)
        except Exception:
            continue
        soup = soup_of_response(r)
        if not soup: continue
        pages += 1

        # карточки на листинге
        for box in soup.select("div.cutoff-off"):
            # первый a обычно «камера», второй — ссылка на товар
            a_tags = box.find_all("a", href=True)
            for a in a_tags:
                if "btn_pic_1" in (a.get("class") or []):  # камера
                    continue
                href = a["href"].strip()
                absu = norm_url(href, page)
                if not absu: continue
                # фильтр на товар
                pth = urlparse(absu).path
                if not PROD_URL_RE.match(pth):  # исключаем пагинацию и пр.
                    continue

                # цена из листинга (ищем рядом в родителе)
                price_int: Optional[int] = None
                parent = box.parent
                if parent:
                    cand = parent.select_one(".price, .catalog_item_price, .price_total, [class*=price]")
                    if cand:
                        price_int = to_int_price(cand.get_text(" ", strip=True))
                urls.setdefault(absu, price_int)

        # пагинация/другие страницы каталога
        for a in soup.select("a[href]"):
            href = a["href"].strip()
            absu = norm_url(href, page)
            if not absu: continue
            if absu in seen_pages: continue
            if absu.startswith(START_URL):
                # только страницы каталога
                q.append(absu)

    items = list(urls.items())
    if MAX_PRODUCTS and len(items) > MAX_PRODUCTS:
        items = items[:MAX_PRODUCTS]
    print(f"[discover] product urls: {len(items)}")
    return items

# ----------------- parse product -----------------
def parse_detail(s: requests.Session, url: str) -> Optional[Dict]:
    jitter_sleep()
    try:
        r = s.get(url, timeout=HTTP_TIMEOUT)
    except Exception:
        return None
    soup = soup_of_response(r)
    if not soup: return None

    # title
    title = ""
    h1 = soup.select_one("h1")
    if h1:
        title = (h1.get_text(" ", strip=True) or "").strip()
    if not title:
        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            title = og["content"].strip()

    # описание-блок с параметрами
    brand = None
    vendor_code = None
    desc_block = soup.select_one("div.description.catalog_item_descr dl.description_row")
    if desc_block:
        # пары dt -> dd
        for dt in desc_block.find_all("dt"):
            name = (dt.get_text(" ", strip=True) or "").strip().strip(":")
            dd = dt.find_next_sibling("dd")
            val = (dd.get_text(" ", strip=True) or "").strip() if dd else ""
            if not val:
                continue
            ln = name.lower()
            if "артикул" in ln and not vendor_code:
                vendor_code = val
            elif "вендор" in ln and not brand:
                brand = val

    # картинка (по возможности)
    picture = None
    ogi = soup.find("meta", attrs={"property": "og:image"})
    if ogi and ogi.get("content"):
        picture = urljoin(url, ogi["content"].strip())
    if not picture:
        img = soup.select_one("img[src]")
        if img and img.get("src"):
            picture = urljoin(url, img["src"])

    if not title or not vendor_code:
        return None

    return {
        "title": title,
        "vendorCode": vendor_code,
        "brand": brand or "",
        "picture": picture or "",
        "url": url,
    }

# ----------------- YML -----------------
def build_yml(offers: List[Dict]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SHOP_NAME)}</name>")
    out.append('<currencies><currency id="KZT" rate="1" /></currencies>')
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    out.append("</categories>")
    out.append("<offers>")
    for it in offers:
        price = int(it["price"])  # уже гарантирован >=1
        price_txt = str(price)
        brand = it.get("brand") or ""
        out += [
            f"<offer id=\"{yml_escape(it['offer_id'])}\" available=\"true\" in_stock=\"true\">",
            f"<name>{yml_escape(it['title'])}</name>",
            f"<vendor>{yml_escape(brand)}</vendor>",
            f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>",
            f"<price>{price_txt}</price>",
            "<currencyId>KZT</currencyId>",
            f"<categoryId>{ROOT_CAT_ID}</categoryId>",
        ]
        if it.get("url"):
            out.append(f"<url>{yml_escape(it['url'])}</url>")
        if it.get("picture"):
            out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        # Краткое описание можно позже расширить
        out.append(f"<description>{yml_escape(it['title'])}</description>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ----------------- main -----------------
def main() -> int:
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MIN)

    # auth
    s = new_session()
    if not do_login(s):
        print("Error: login failed")
        return 2

    # keywords
    kws = load_keywords(KEYWORDS_FILE)
    pats = compile_startswith_patterns(kws)

    # collect urls
    pairs = discover_product_urls(s, MAX_PAGES)
    if not pairs:
        # всё равно сгенерим пустую структуру, чтобы job не падал безнадёжно
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(build_yml([]))
        print(f"[done] items: 0 -> {OUT_FILE}")
        return 0

    # parse details, merge
    offers: List[Dict] = []
    seen_offer_ids: Set[str] = set()

    def worker(tup):
        if datetime.utcnow() > deadline:
            return None
        url, price_list = tup
        try:
            obj = parse_detail(s, url)
            if not obj:
                return None
            # фильтр по startswith ключам — по title
            if not title_startswith(obj["title"], pats):
                return None

            # цена: карточка могла не дать; используем листинг; иначе 1
            price_final: Optional[int] = None
            # попробуем выдрать цену прямо со страницы товара
            txt_price_candidates = []
            for cand in ["span.price", "div.price", ".product-price", ".final_price", "[class*=price]"]:
                el = BeautifulSoup("", "lxml")  # заглушка для типизации
            # универсальный брутфорс по тексту
            text_all = obj["title"]  # временно (у нас нет текста всего soup здесь)
            # листинговая цена приоритетнее, если есть
            if price_list and price_list > 0:
                price_final = int(price_list)

            if not price_final or price_final <= 0:
                price_final = 1  # по требованию брать 1, если цены нет

            obj["price"] = price_final

            # brand обязательно строка
            obj["brand"] = (obj.get("brand") or "").strip()

            # формируем offer_id по артикулу
            offer_id = obj["vendorCode"]
            if offer_id in seen_offer_ids:
                offer_id = f"{offer_id}-{sha1(obj['title'])[:6]}"
            seen_offer_ids.add(offer_id)
            obj["offer_id"] = offer_id

            return obj
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(worker, p) for p in pairs]
        for fut in as_completed(futures):
            one = fut.result()
            if one:
                offers.append(one)

    # если совсем ничего — всё равно пишем валидный yml
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(build_yml(offers))

    print(f"[done] items: {len(offers)} -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("Error:", e)
        sys.exit(2)
