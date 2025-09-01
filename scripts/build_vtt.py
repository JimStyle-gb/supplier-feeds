# -*- coding: utf-8 -*-
"""
VTT B2B → YML (без ручного участия):
- Логин по логину/паролю (env VTT_LOGIN/VTT_PASSWORD).
- Обход каталога, сбор карточек прямо со страниц списка (если нет страниц товара).
- Категории формируются по хлебным крошкам/заголовку.
- Отладочные файлы сохраняются в docs/ чтобы быстро подстроить селекторы при необходимости.

Зависимости: requests, beautifulsoup4, lxml
"""

from __future__ import annotations
import os, re, io, time, html, hashlib, json
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ---------------- ENV ----------------
BASE_URL        = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL       = os.getenv("START_URL", f"{BASE_URL}/catalog/")
OUT_FILE        = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN       = os.getenv("VTT_LOGIN", "")
VTT_PASSWORD    = os.getenv("VTT_PASSWORD", "")

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"
ALLOW_SSL_FALLBACK = os.getenv("ALLOW_SSL_FALLBACK", "1") == "1"

HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS= int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "800"))

MAX_WORKERS     = int(os.getenv("MAX_WORKERS", "6"))
MAX_CRAWL_MIN   = int(os.getenv("MAX_CRAWL_MINUTES", "60"))
MAX_PAGES       = int(os.getenv("MAX_PAGES", "800"))

SUPPLIER_NAME   = "vtt"
CURRENCY        = "RUB"

ROOT_CAT_ID     = 9600000
ROOT_CAT_NAME   = "VTT"

DEBUG_ROOT_HTML = "docs/vtt_debug_root.html"
DEBUG_LINKS_TXT = "docs/vtt_links.txt"

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-B2B-Scraper/1.2)"}

# ---------------- utils ----------------
def dlog(msg: str):
    print(msg, flush=True)
    try:
        with io.open("docs/vtt_debug_log.txt", "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms/1000.0))

def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    if DISABLE_SSL_VERIFY:
        dlog("[ssl] verification disabled by env")
        s.verify = False
        try:
            requests.packages.urllib3.disable_warnings()  # type: ignore
        except Exception:
            pass
    return s

def http_get(s: requests.Session, url: str, allow_fallback: bool = True) -> Optional[requests.Response]:
    try:
        r = s.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: 
            dlog(f"[http] GET status {r.status_code} {url}")
            return None
        if MIN_BYTES and len(r.content) < MIN_BYTES:
            dlog(f"[http] GET too small ({len(r.content)} bytes) {url}")
            return None
        return r
    except requests.exceptions.SSLError as e:
        dlog(f"[http] GET SSL fail {url}: {e}")
        if allow_fallback and ALLOW_SSL_FALLBACK:
            try:
                r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT, verify=False)
                if r.status_code == 200 and len(r.content) >= MIN_BYTES:
                    return r
            except Exception as e2:
                dlog(f"[http] Fallback verify=False failed: {e2}")
        return None
    except Exception as e:
        dlog(f"[http] GET fail {url}: {e}")
        return None

def soup_html(resp: requests.Response) -> BeautifulSoup:
    # Если это XML (иногда пагинации/виджеты), используем lxml-xml, иначе html.parser
    ctype = resp.headers.get("Content-Type", "")
    if "xml" in ctype:
        return BeautifulSoup(resp.content, "xml")
    return BeautifulSoup(resp.content, "html.parser")

def save_file(path: str, data: bytes | str):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        mode = "wb" if isinstance(data, (bytes, bytearray)) else "w"
        with open(path, mode) as f:
            f.write(data)
    except Exception as e:
        dlog(f"[debug] save fail {path}: {e}")

def yml_escape(s: str) -> str:
    return html.escape((s or "").strip())

def to_number(x: Any) -> Optional[float]:
    if x is None: return None
    s = str(x).replace("\xa0"," ").replace("\u202f"," ").replace(" ", "")
    s = s.replace("руб", "").replace("₽", "").replace(",", ".")
    if not re.search(r"\d", s): return None
    try:
        return float(s)
    except Exception:
        m = re.search(r"(\d+(?:\.\d+)?)", s)
        return float(m.group(1)) if m else None

def sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

# ---------------- login ----------------
def extract_csrf_token(soup: BeautifulSoup) -> Optional[str]:
    m = soup.find("meta", attrs={"name":"csrf-token"})
    if m and m.get("content"):
        return m["content"]
    inp = soup.find("input", attrs={"name":"_token"})
    if inp and inp.get("value"):
        return inp.get("value")
    return None

def looks_logged_in(soup: BeautifulSoup) -> bool:
    txt = soup.get_text(" ", strip=True).lower()
    # простые индикаторы: "выход", "профиль", "корзина" при наличии имени
    return ("выход" in txt or "выйти" in txt or "профиль" in txt) and ("вход" not in txt)

def try_login(sess: requests.Session) -> bool:
    # открываем любую страницу, чтобы получить csrf/сессию
    jitter_sleep(REQUEST_DELAY_MS)
    r = http_get(sess, START_URL)
    if not r:
        dlog("Error: Стартовая страница недоступна (проверьте URL).")
        return False
    save_file(DEBUG_ROOT_HTML, r.content)
    s = soup_html(r)

    if looks_logged_in(s):
        dlog("[login] already logged in")
        return True

    csrf = extract_csrf_token(s)

    # кандидаты эндпоинтов логина
    candidates = [
        f"{BASE_URL}/login",
        f"{BASE_URL}/auth/login",
        f"{BASE_URL}/signin",
        f"{BASE_URL}/account/login",
        f"{BASE_URL}/user/login",
        f"{BASE_URL}/authorization",
        f"{BASE_URL}/login_check",
    ]

    payload_variants = []
    # разные имена полей встречаются на B2B
    for user_field in ("email","login","username"):
        payload_variants.append({user_field: VTT_LOGIN, "password": VTT_PASSWORD, "_token": csrf or ""})
        payload_variants.append({user_field: VTT_LOGIN, "password": VTT_PASSWORD})
    payload_variants.append({"email": VTT_LOGIN, "pass": VTT_PASSWORD, "_token": csrf or ""})

    for url in candidates:
        for data in payload_variants:
            try:
                jitter_sleep(REQUEST_DELAY_MS)
                rr = sess.post(url, data=data, timeout=HTTP_TIMEOUT, allow_redirects=True)
                if rr.status_code in (200, 302):
                    jitter_sleep(REQUEST_DELAY_MS)
                    chk = http_get(sess, START_URL)
                    if chk:
                        ss = soup_html(chk)
                        save_file(DEBUG_ROOT_HTML, chk.content)  # обновим
                        if looks_logged_in(ss):
                            dlog("[login] success")
                            return True
            except Exception as e:
                dlog(f"[login] post fail {url}: {e}")

    # Последний шанс: иногда сайт авторизует через редирект на ту же форму с cookies remember
    jitter_sleep(REQUEST_DELAY_MS)
    chk = http_get(sess, START_URL)
    if chk:
        ss = soup_html(chk)
        if looks_logged_in(ss):
            dlog("[login] success (post-check)")
            return True

    dlog("Error: login failed")
    return False

# ---------------- discovery ----------------
PAGE_LINK_PAT = re.compile(r"/catalog/[^?#]*", re.I)

def is_same_host(u: str) -> bool:
    try:
        return urlparse(u).netloc in ("", urlparse(BASE_URL).netloc)
    except Exception:
        return False

def collect_catalog_pages(sess: requests.Session, start_url: str, deadline: datetime) -> List[str]:
    """Собираем ссылки разделов/страниц каталога (включая пагинацию)."""
    pages: List[str] = []
    seen: Set[str] = set()
    queue: List[str] = [start_url]
    iterations = 0

    # копим текст всех ссылок для дебага
    all_links: List[str] = []

    while queue and len(pages) < MAX_PAGES and datetime.utcnow() < deadline:
        url = queue.pop(0)
        if url in seen: 
            continue
        seen.add(url)

        jitter_sleep(REQUEST_DELAY_MS)
        r = http_get(sess, url)
        if not r:
            save_file(f"docs/vtt_fail_{len(seen):04d}.html", f"FAIL {url}")
            continue

        if iterations == 0:
            save_file(DEBUG_ROOT_HTML, r.content)

        s = soup_html(r)
        pages.append(url)

        # собираем ссылки на продукты сразу из листинга (если он без карточек)
        # всё равно пойдём по pagination
        for a in s.find_all("a", href=True):
            href = a["href"].strip()
            absu = urljoin(url, href)
            if not is_same_host(absu):
                continue
            all_links.append(absu)
            if PAGE_LINK_PAT.search(absu):
                # пагинация: ссылки с "page=" добираем
                # а также "catalog/..." без указания товара
                if absu not in seen and absu not in queue and len(queue) + len(pages) < MAX_PAGES:
                    queue.append(absu)

        # ищем явную пагинацию через rel="next"
        ln = s.find("link", attrs={"rel": "next"})
        if ln and ln.get("href"):
            nxt = urljoin(url, ln["href"])
            if nxt not in seen and nxt not in queue:
                queue.append(nxt)

        iterations += 1

    # сохраним все найденные ссылки для быстрой ручной проверки
    try:
        with io.open(DEBUG_LINKS_TXT, "w", encoding="utf-8") as f:
            for u in sorted(set(all_links)):
                f.write(u + "\n")
    except Exception:
        pass

    dlog(f"[discover] pages: {len(pages)}")
    return pages

# ---------------- parse listing ----------------
def extract_products_from_listing(soup: BeautifulSoup, page_url: str) -> List[Dict[str,Any]]:
    """
    Универсальная выжимка карточек прямо со страницы списка.
    Ищем:
      - JSON-LD Product/ItemList
      - data-атрибуты карточек
      - общие CSS для заголовка/цены/картинки/артикула
    Возвращаем как минимум: title, price, vendorCode?, picture?, url?
    """
    items: List[Dict[str,Any]] = []

    # 1) JSON-LD (если есть)
    for sc in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        try:
            data = json.loads(sc.string or "")
        except Exception:
            try:
                data = json.loads(sc.get_text())
            except Exception:
                continue
        # ItemList / Product
        candidates = []
        if isinstance(data, list):
            candidates = data
        elif isinstance(data, dict):
            candidates = [data]
        else:
            candidates = []

        for d in candidates:
            t = (d.get("@type") or "").lower()
            if t == "itemlist" and isinstance(d.get("itemListElement"), list):
                for it in d["itemListElement"]:
                    prod = it.get("item") if isinstance(it, dict) else None
                    if isinstance(prod, dict) and (prod.get("@type","").lower()=="product"):
                        title = (prod.get("name") or "").strip()
                        url = urljoin(page_url, (prod.get("url") or "").strip())
                        img = (prod.get("image") or "").strip()
                        price = None
                        of = prod.get("offers") or {}
                        if isinstance(of, dict):
                            price = to_number(of.get("price"))
                        elif isinstance(of, list) and of:
                            price = to_number(of[0].get("price"))
                        if title and price:
                            items.append({"title": title, "price": price, "url": url, "picture": img})
            if t == "product":
                title = (d.get("name") or "").strip()
                url = urljoin(page_url, (d.get("url") or "").strip())
                img = (d.get("image") or "").strip()
                price = None
                of = d.get("offers") or {}
                if isinstance(of, dict):
                    price = to_number(of.get("price"))
                elif isinstance(of, list) and of:
                    price = to_number(of[0].get("price"))
                if title and price:
                    items.append({"title": title, "price": price, "url": url, "picture": img})

    # 2) Heuristic: карточки товаров
    cards = []
    cards += soup.select('[data-product-id]')
    cards += soup.select('[class*="product-card"], [class*="product__item"], [class*="catalog__item"], [class*="goods-item"]')
    cards = list(dict.fromkeys(cards))  # уник

    for c in cards:
        # title
        title = None
        te = c.select_one('[itemprop="name"], .product-title, .product__title, .goods-title, a[href*="/catalog/"]')
        if te:
            title = te.get_text(" ", strip=True)
        if not title:
            title = c.get_text(" ", strip=True)
            title = re.sub(r"\s{2,}", " ", title)
            # обрежем хвосты кнопок
            title = re.sub(r"(в корзину.*)$", "", title, flags=re.I)

        # price
        price = None
        pe = c.select_one('[itemprop="price"], .price, .product-price, [class*="price__value"]')
        if pe:
            price = to_number(pe.get("content") or pe.get_text(" ", strip=True))
        if price is None:
            price = to_number(c.get_text(" ", strip=True))

        # vendorCode
        vcode = None
        for lab in ("артикул", "код", "sku", "код товара"):
            node = c.find(string=lambda t: t and lab in t.lower())
            if node:
                m = re.search(r"([A-Za-zА-Яа-я0-9\-\._/]{2,})", (node.parent.get_text(" ", strip=True) if node.parent else str(node)))
                if m:
                    vcode = m.group(1)
                    break
        if not vcode:
            # иногда sku в data-атрибутах
            for att in ("data-sku","data-code","data-art","data-article"):
                if c.has_attr(att) and c.get(att):
                    vcode = str(c.get(att)).strip()
                    break

        # picture
        img = None
        imgel = c.select_one('img[src], img[data-src]')
        if imgel:
            img = imgel.get("src") or imgel.get("data-src")
            if img and img.startswith("//"): img = "https:" + img
            img = urljoin(page_url, img)

        # product url (если есть)
        u = None
        ae = c.select_one('a[href*="/catalog/"]')
        if ae and ae.get("href"):
            u = urljoin(page_url, ae["href"])

        if title and price:
            items.append({"title": title, "price": float(f"{price:.2f}"), "vendorCode": vcode or "", "picture": img or "", "url": u or page_url})

    # 3) fallback: ищем типичные таблицы
    if not items:
        rows = soup.select("table tr")
        for tr in rows:
            tds = tr.find_all(["td","th"])
            if len(tds) < 3: 
                continue
            rowtxt = " ".join(td.get_text(" ", strip=True) for td in tds)
            if not re.search(r"\d", rowtxt): 
                continue
            title = tds[0].get_text(" ", strip=True)
            price = None
            vcode = None
            img = None
            url = None
            # price in any col
            for td in tds:
                price = price or to_number(td.get_text(" ", strip=True))
                if not vcode:
                    m = re.search(r"(?:Артикул|Код|SKU)\s*[:#]?\s*([A-Za-zА-Яа-я0-9\-\._/]{2,})", td.get_text(" ", strip=True), flags=re.I)
                    if m: vcode = m.group(1)
                if not img:
                    im = td.find("img")
                    if im and (im.get("src") or im.get("data-src")):
                        img = im.get("src") or im.get("data-src")
                        img = urljoin(page_url, img)
                if not url:
                    a = td.find("a", href=True)
                    if a: url = urljoin(page_url, a["href"])
            if title and price:
                items.append({"title": title, "price": float(f"{price:.2f}"), "vendorCode": vcode or "", "picture": img or "", "url": url or page_url})

    return items

def extract_breadcrumbs(soup: BeautifulSoup) -> List[str]:
    names: List[str] = []
    for bc in soup.select('nav[aria-label="breadcrumb"], ul.breadcrumb, .breadcrumbs, .breadcrumb, [class*="breadcrumb"]'):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if not t: 
                continue
            tl = t.lower()
            if tl in ("главная","home"): 
                continue
            names.append(t.strip())
        if names:
            break
    return [n for n in names if n]

def stable_cat_id(text: str, prefix: int = 9700000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories_from_paths(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
    cat_map: Dict[Tuple[str,...], int] = {}
    out_list: List[Tuple[int,str,Optional[int]]] = []
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная","home","каталог")]
        if not clean: 
            continue
        parent_id = ROOT_CAT_ID
        prefix: List[str] = []
        for name in clean:
            prefix.append(name)
            key = tuple(prefix)
            if key in cat_map:
                parent_id = cat_map[key]; continue
            cid = stable_cat_id(" / ".join(prefix))
            cat_map[key] = cid
            out_list.append((cid, name, parent_id))
            parent_id = cid
    return out_list, cat_map

# ---------------- YML ----------------
def build_yml(categories: List[Tuple[int,str,Optional[int]]], offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SUPPLIER_NAME)}</name>")
    out.append('<currencies><currency id="RUB" rate="1" /></currencies>')

    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        parent = parent if parent else ROOT_CAT_ID
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{yml_escape(name)}</category>")
    out.append("</categories>")

    out.append("<offers>")
    for cid, it in offers:
        price = it["price"]
        price_txt = str(int(price)) if float(price).is_integer() else f"{price}"
        out += [
            f"<offer id=\"{yml_escape(it['offer_id'])}\" available=\"true\" in_stock=\"true\">",
            f"<name>{yml_escape(it['title'])}</name>",
            f"<vendor>{yml_escape(it.get('brand') or SUPPLIER_NAME)}</vendor>",
            f"<vendorCode>{yml_escape(it.get('vendorCode') or '')}</vendorCode>",
            f"<price>{price_txt}</price>",
            "<currencyId>RUB</currencyId>",
            f"<categoryId>{cid}</categoryId>",
        ]
        if it.get("url"): out.append(f"<url>{yml_escape(it['url'])}</url>")
        if it.get("picture"): out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        desc = it.get("description") or it["title"]
        out.append(f"<description>{yml_escape(desc)}</description>")
        out += ["<quantity_in_stock>1</quantity_in_stock>", "<stock_quantity>1</stock_quantity>", "<quantity>1</quantity>", "</offer>"]
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------------- MAIN ----------------
def main() -> int:
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MIN)

    sess = new_session()
    if not VTT_LOGIN or not VTT_PASSWORD:
        dlog("Error: VTT_LOGIN/VTT_PASSWORD are empty (set repository secrets).")
        return 2

    if not try_login(sess):
        return 2

    # discovery
    pages = collect_catalog_pages(sess, START_URL, deadline)
    if not pages:
        dlog("Error: каталог пуст или недоступен.")
        # всё равно создадим пустой yml
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(build_yml([], []))
        return 2

    # parse listings
    all_items: List[Dict[str,Any]] = []
    all_paths: List[List[str]] = []

    for i, url in enumerate(pages, 1):
        if datetime.utcnow() > deadline:
            dlog("[time] deadline reached, stopping parse")
            break
        jitter_sleep(REQUEST_DELAY_MS)
        r = http_get(sess, url)
        if not r:
            save_file(f"docs/vtt_fail_{i:04d}.html", f"FAIL {url}")
            continue
        save_file(f"docs/vtt_page_{i:04d}.html", r.content)
        s = soup_html(r)

        items = extract_products_from_listing(s, url)
        if items:
            all_items.extend(items)

        bc = extract_breadcrumbs(s)
        if bc:
            all_paths.append(bc)
        else:
            # иногда хотя бы H1 пригодится
            h = s.find("h1")
            if h and h.get_text(strip=True):
                all_paths.append([h.get_text(strip=True)])

    # build cats
    cat_list, path_id_map = build_categories_from_paths(all_paths)
    dlog(f"[cats] built: {len(cat_list)}")

    # dedupe by (title, vendorCode)
    seen: Set[Tuple[str,str]] = set()
    offers: List[Tuple[int,Dict[str,Any]]] = []
    for it in all_items:
        title = (it.get("title") or "").strip()
        if not title:
            continue
        vcode = (it.get("vendorCode") or "").strip()
        price = it.get("price")
        if price is None or price <= 0:
            continue

        key = (title.lower(), vcode.lower())
        if key in seen:
            continue
        seen.add(key)

        # choose category by last seen crumbs intersection, иначе root
        cid = ROOT_CAT_ID
        if path_id_map:
            # ничего умнее без реальных крошек не придумаем — просто корень
            pass

        offer_id = (vcode or title)[:80]
        offer = {
            "offer_id": offer_id,
            "title": title,
            "price": float(f"{float(price):.2f}"),
            "vendorCode": vcode,
            "brand": SUPPLIER_NAME,
            "url": it.get("url") or START_URL,
            "picture": it.get("picture") or "",
            "description": it.get("description") or title,
        }
        offers.append((cid, offer))

    # write YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(cat_list, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    dlog(f"[done] items: {len(offers)}, cats: {len(cat_list)} -> {OUT_FILE}")
    return 0 if offers else 2


if __name__ == "__main__":
    import sys, hashlib
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e, flush=True)
        sys.exit(2)
