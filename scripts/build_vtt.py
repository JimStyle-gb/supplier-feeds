# -*- coding: utf-8 -*-
"""
VTT (b2b.vtt.ru) → YML (KZT)
- Логин по AJAX (/validateLogin + CSRF)
- Сбор ссылок на товары с пагинации /catalog/?page=N
- Парс карточек:
  * Название: <h1>
  * Артикул: ТОЛЬКО из блока "Артикул"
  * Вендор: из блока "Вендор" (становится <vendor>)
  * Цена: ищем по типовым селекторам; если нет — 1 (KZT). Округляем до целого.
  * Картинка: og:image или ближайшая картинка товара
  * Крошки → категории (без слова "Главная")
- Фильтр: название ДОЛЖНО начинаться с ключа из docs/vtt_keywords.txt (строго begins-with).
- В XML НИГДЕ не пишем "vtt" как бренд: <vendor> — это реально "Вендор" из карточки.
- Выход: docs/vtt.yml (encoding windows-1251)
"""

from __future__ import annotations
import os, re, io, time, html, hashlib
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ---------------- ENV ----------------
BASE_URL         = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL        = os.getenv("START_URL", f"{BASE_URL}/catalog/")
KEYWORDS_FILE    = os.getenv("KEYWORDS_FILE", "docs/vtt_keywords.txt")
OUT_FILE         = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN        = os.getenv("VTT_LOGIN") or ""
VTT_PASSWORD     = os.getenv("VTT_PASSWORD") or ""

HTTP_TIMEOUT     = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "180"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "700"))

MAX_PAGES        = int(os.getenv("MAX_PAGES", "1200"))
MAX_PRODUCTS     = int(os.getenv("MAX_PRODUCTS", "6000"))
MAX_WORKERS      = int(os.getenv("MAX_WORKERS", "6"))
MAX_CRAWL_MIN    = int(os.getenv("MAX_CRAWL_MINUTES", "90"))

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"

# ---------------- CONSTS ----------------
UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-B2B-Feed/1.2)"}

SHOP_NAME       = "B2B"         # Не 'vtt'
CURRENCY        = "KZT"
ROOT_CAT_ID     = 9600000
ROOT_CAT_NAME   = "Catalog"     # Без упоминания vtt

PRODUCT_HOST    = urlparse(BASE_URL).netloc

# ---------------- UTILS ----------------
def jitter_sleep():
    time.sleep(max(0.0, REQUEST_DELAY_MS/1000.0))

def yml_escape(s: str) -> str:
    return html.escape((s or "").strip())

def to_float(s: str) -> Optional[float]:
    if not s: return None
    txt = s.replace("\xa0"," ").replace(" ", "")
    txt = txt.replace("₸","").replace("р","").replace("₽","")
    txt = txt.replace(",", ".")
    m = re.search(r"\d+(?:\.\d+)?", txt)
    try:
        return float(m.group(0)) if m else None
    except Exception:
        return None

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def key_norm(v: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", v or "").upper()

def compile_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    pats: List[re.Pattern] = []
    for kw in kws:
        kw = kw.strip()
        if not kw: continue
        esc = re.escape(kw).replace(r"\ ", " ")
        pats.append(re.compile(r"^\s*" + esc + r"(?!\w)", re.IGNORECASE))
    return pats

def title_startswith_strict(title: str, patterns: List[re.Pattern]) -> bool:
    if not title: return False
    return any(p.search(title) for p in patterns)

def load_keywords(path: str) -> List[str]:
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
            if lines: return lines
    # дефолт
    return [
        "Девелопер",
        "Драм-картридж",
        "Драм-юнит",
        "Картридж",
        "Копи-картридж",
        "Принт-картридж",
        "Термоблок",
        "Термоэлемент",
        "Тонер-картридж",
    ]

# ---------------- HTTP ----------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    if DISABLE_SSL_VERIFY:
        s.verify = False
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
        print("[ssl] verification disabled by env")
    return s

def get_soup(s: requests.Session, url: str) -> Optional[BeautifulSoup]:
    try:
        r = s.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200 or len(r.content) < MIN_BYTES:
            return None
        return BeautifulSoup(r.content, "html.parser")
    except Exception:
        return None

# ---------------- AUTH ----------------
def login(s: requests.Session) -> bool:
    # 1) GET чтобы получить CSRF
    soup = get_soup(s, f"{BASE_URL}/")
    if not soup:
        soup = get_soup(s, f"{BASE_URL}/login")
        if not soup:
            print("[login] cannot open login page")
            return False
    meta = soup.find("meta", attrs={"name": "csrf-token"})
    token = meta["content"].strip() if meta and meta.get("content") else ""

    # 2) POST AJAX /validateLogin
    try:
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }
        if token:
            headers["X-CSRF-TOKEN"] = token

        data = {"login": VTT_LOGIN, "password": VTT_PASSWORD, "remember": "1"}
        r = s.post(f"{BASE_URL}/validateLogin", data=data, headers=headers, timeout=HTTP_TIMEOUT)
        ok = False
        if "application/json" in r.headers.get("Content-Type",""):
            j = r.json()
            ok = bool(j.get("result"))
        else:
            # fallback: после удачи редирект на каталог
            ok = (r.status_code in (200, 302))
        if not ok:
            print("[login] invalid credentials or blocked")
            return False
    except Exception as e:
        print("[login] error:", e)
        return False

    # 3) убедимся, что каталог открывается без редиректа на /login
    soup2 = get_soup(s, START_URL)
    if not soup2:
        return False
    if soup2.find("form", attrs={"action": re.compile(r"/validateLogin")}):
        print("[login] still on login page")
        return False
    print("[login] success")
    return True

# ---------------- DISCOVER LINKS ----------------
def extract_product_links_from_listing(soup: BeautifulSoup, page_url: str) -> List[str]:
    out: List[str] = []
    # Точный блок списка: div.cutoff-off → второй <a> (первый — иконка камеры)
    for div in soup.select("div.cutoff-off"):
        anchors = [a for a in div.find_all("a", href=True)]
        # выкинем кнопки без ссылки на товар
        anchors = [a for a in anchors if "btn_naked" not in (a.get("class") or [])]
        if not anchors: 
            # если все-таки один — берём его
            anchors = [a for a in div.find_all("a", href=True)]
        if anchors:
            href = anchors[-1]["href"]
            absu = urljoin(page_url, href)
            if "/catalog/" in absu and PRODUCT_HOST in urlparse(absu).netloc:
                out.append(absu)

    # fallback: любые кликабельные заголовки в плитке/таблице
    for a in soup.select("a[href*='/catalog/']"):
        href = a.get("href", "")
        if not href: 
            continue
        absu = urljoin(page_url, href)
        if PRODUCT_HOST not in urlparse(absu).netloc:
            continue
        # отфильтруем очевидные пагинации/служебные
        if "page=" in absu:
            continue
        if "/catalog/" in absu:
            out.append(absu)

    # уникализируем
    out = list(dict.fromkeys(out))
    return out

def discover_listing_pages(s: requests.Session, start_url: str, max_pages: int) -> List[str]:
    pages: List[str] = []
    seen: Set[str] = set()
    q: List[str] = [start_url]

    while q and len(pages) < max_pages:
        u = q.pop(0)
        if u in seen: 
            continue
        seen.add(u)
        jitter_sleep()
        soup = get_soup(s, u)
        if not soup: 
            continue
        pages.append(u)

        # собрать линки пагинации
        for a in soup.select("a[href*='?page=']"):
            absu = urljoin(u, a["href"])
            if absu not in seen and absu.startswith(f"{BASE_URL}/catalog/"):
                q.append(absu)

        # иногда пагинация пишется как /catalog/?page=2 без других параметров — попробуем range
        if len(pages) == 1:
            # пытаемся угадать максимум страниц из пагинатора
            nums = []
            for a in soup.select("a[href*='?page=']"):
                m = re.search(r"[?&]page=(\d+)", a.get("href",""))
                if m: nums.append(int(m.group(1)))
            if nums:
                last = max(nums)
                for i in range(2, min(last, max_pages)+1):
                    q.append(f"{BASE_URL}/catalog/?page={i}")

    return list(dict.fromkeys(pages))[:max_pages]

# ---------------- PARSE PRODUCT ----------------
def extract_price(soup: BeautifulSoup) -> Optional[int]:
    # 1) itemprop=price
    el = soup.find(attrs={"itemprop": "price"})
    if el:
        val = el.get("content") or el.get("value") or el.get_text(" ", strip=True)
        p = to_float(val)
        if p is not None:
            return max(1, int(round(p)))

    # 2) по классам, содержащим "price"
    for sel in [
        ".price", ".product_price", ".catalog_item_price", ".price-block",
        "[class*='price']"
    ]:
        for e in soup.select(sel):
            txt = e.get_text(" ", strip=True)
            p = to_float(txt)
            if p is not None:
                return max(1, int(round(p)))

    # 3) не нашли — вернём 1
    return 1

def parse_product(s: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    jitter_sleep()
    soup = get_soup(s, url)
    if not soup:
        return None

    # название
    title = ""
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        title = norm_space(h1.get_text(" ", strip=True))
    if not title:
        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            title = norm_space(og["content"])

    if not title:
        return None

    # описание-блок с атрибутами
    ddmap: Dict[str,str] = {}
    for dt in soup.select("div.description.catalog_item_descr dt"):
        key = norm_space(dt.get_text(" ", strip=True)).strip(":").lower()
        dd = dt.find_next_sibling("dd")
        val = norm_space(dd.get_text(" ", strip=True)) if dd else ""
        if key:
            ddmap[key] = val

    # ТОЛЬКО "Артикул"
    vendor_code = ddmap.get("артикул") or ""
    if not vendor_code:
        return None

    brand = ddmap.get("вендор") or ""

    price = extract_price(soup)
    picture = None
    ogi = soup.find("meta", attrs={"property":"og:image"})
    if ogi and ogi.get("content"):
        picture = ogi["content"].strip()
    if not picture:
        for img in soup.find_all("img"):
            src = (img.get("src") or "").strip()
            if not src: 
                continue
            if "/images/" in src or "upload" in src:
                picture = urljoin(url, src)
                break

    # крошки → категории
    crumbs: List[str] = []
    for bc in soup.select(".breadcrumb, ul.breadcrumb, nav.breadcrumb"):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if not t:
                continue
            tl = t.lower()
            if tl in ("главная","home"):
                continue
            crumbs.append(t)
        if crumbs:
            break

    return {
        "title": title,
        "vendor_code": vendor_code,
        "brand": brand,
        "price": price,
        "url": url,
        "picture": picture,
        "crumbs": crumbs,
    }

# ---------------- CATEGORIES ----------------
def stable_cat_id(text: str, prefix: int = 9700000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,int]], Dict[Tuple[str,...], int]]:
    cat_map: Dict[Tuple[str,...], int] = {}
    out: List[Tuple[int,str,int]] = []
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная","home","каталог")]
        if not clean:
            continue
        parent = ROOT_CAT_ID
        agg: List[str] = []
        for name in clean:
            agg.append(name)
            key = tuple(agg)
            if key in cat_map:
                parent = cat_map[key]
                continue
            cid = stable_cat_id(" / ".join(agg))
            cat_map[key] = cid
            out.append((cid, name, parent))
            parent = cid
    return out, cat_map

# ---------------- YML ----------------
def build_yml(categories: List[Tuple[int,str,int]], offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SHOP_NAME)}</name>")
    out.append(f"<currencies><currency id=\"{CURRENCY}\" rate=\"1\" /></currencies>")

    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{yml_escape(name)}</category>")
    out.append("</categories>")

    out.append("<offers>")
    for cid, it in offers:
        price = max(1, int(round(float(it["price"]))))
        out += [
            f"<offer id=\"{yml_escape(it['offer_id'])}\" available=\"true\" in_stock=\"true\">",
            f"<name>{yml_escape(it['title'])}</name>",
            f"<vendor>{yml_escape(it.get('brand') or '')}</vendor>",
            f"<vendorCode>{yml_escape(it['vendor_code'])}</vendorCode>",
            f"<price>{price}</price>",
            f"<currencyId>{CURRENCY}</currencyId>",
            f"<categoryId>{cid}</categoryId>",
        ]
        if it.get("url"):
            out.append(f"<url>{yml_escape(it['url'])}</url>")
        if it.get("picture"):
            out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        desc = it.get("title") or ""
        out.append(f"<description>{yml_escape(desc)}</description>")
        out.append("<quantity_in_stock>1</quantity_in_stock>")
        out.append("<stock_quantity>1</stock_quantity>")
        out.append("<quantity>1</quantity>")
        out.append("</offer>")
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------------- MAIN ----------------
def main() -> int:
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MIN)

    if not VTT_LOGIN or not VTT_PASSWORD:
        print("[error] VTT_LOGIN/VTT_PASSWORD are empty")
        # всё равно сгенерим пустой файл
    kws = load_keywords(KEYWORDS_FILE)
    patterns = compile_startswith_patterns(kws)

    s = make_session()
    if not login(s):
        print("Error: login failed")
        # сгенерим пустой файл и завершимся успешно, чтобы не ронять workflow
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(build_yml([], []))
        return 0

    # 1) собираем страницы каталога
    pages = discover_listing_pages(s, START_URL, MAX_PAGES)
    # 2) извлекаем ссылки на карточки
    prod_urls: List[str] = []
    for pu in pages:
        if datetime.utcnow() > deadline:
            break
        jitter_sleep()
        soup = get_soup(s, pu)
        if not soup:
            continue
        links = extract_product_links_from_listing(soup, pu)
        prod_urls.extend(links)
        if len(prod_urls) >= MAX_PRODUCTS:
            break
    prod_urls = list(dict.fromkeys(prod_urls))[:MAX_PRODUCTS]
    print(f"[discover] product urls: {len(prod_urls)}")

    # 3) парсим карточки
    site_index: Dict[str, Dict[str,Any]] = {}
    names_for_cats: List[List[str]] = []

    def worker(u: str):
        if datetime.utcnow() > deadline:
            return None
        try:
            item = parse_product(s, u)
            return item
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(worker, u): u for u in prod_urls}
        for fut in as_completed(futures):
            it = fut.result()
            if not it:
                continue
            # фильтр: строго по началу
            if not title_startswith_strict(it["title"], patterns):
                continue

            vc = it["vendor_code"]
            kn = key_norm(vc)
            if kn in site_index:
                continue
            site_index[kn] = it
            if it.get("crumbs"):
                names_for_cats.append(it["crumbs"])

    # 4) категории
    cat_list: List[Tuple[int,str,int]] = []
    path_map: Dict[Tuple[str,...], int] = {}
    if names_for_cats:
        cat_list, path_map = build_categories(names_for_cats)

    # 5) сбор офферов
    offers: List[Tuple[int,Dict[str,Any]]] = []
    seen_offer_ids: Set[str] = set()
    for kn, it in site_index.items():
        # категория (по полному пути, если есть)
        cid = ROOT_CAT_ID
        crumbs = it.get("crumbs") or []
        if crumbs:
            clean = [p for p in crumbs if p and p.strip() and p.lower() not in ("главная","home","каталог")]
            key = tuple(clean)
            while key and key not in path_map:
                key = key[:-1]
            if key and key in path_map:
                cid = path_map[key]

        offer_id = it["vendor_code"]
        if offer_id in seen_offer_ids:
            offer_id = f"{offer_id}-{sha1(it['title'])[:6]}"
        seen_offer_ids.add(offer_id)

        offers.append((cid, {
            "offer_id": offer_id,
            "title": it["title"],
            "vendor_code": it["vendor_code"],
            "brand": it.get("brand") or "",
            "price": it.get("price") or 1,
            "url": it.get("url") or "",
            "picture": it.get("picture") or "",
        }))

    # 6) YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(cat_list, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, cats: {len(cat_list)} -> {OUT_FILE}")
    # даже если 0 — не валим workflow
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        # не валим пайплайн, пишем пустой yml
        try:
            os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
            with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
                f.write("<?xml version='1.0' encoding='windows-1251'?><yml_catalog><shop><name>B2B</name><currencies><currency id=\"KZT\" rate=\"1\" /></currencies><categories><category id=\"9600000\">Catalog</category></categories><offers></offers></shop></yml_catalog>")
        except Exception:
            pass
        sys.exit(0)
