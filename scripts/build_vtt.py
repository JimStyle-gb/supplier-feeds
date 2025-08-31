# -*- coding: utf-8 -*-
"""
b2b.vtt.ru → YML (картриджи; логин/пароль; без ручных куки).
Добавлен авто-фолбэк при SSL ошибке (ALLOW_SSL_FALLBACK=1).

ENV:
  BASE_URL, START_URL, OUT_FILE, OUTPUT_ENCODING
  VTT_LOGIN, VTT_PASSWORD
  DISABLE_SSL_VERIFY, ALLOW_SSL_FALLBACK
  HTTP_TIMEOUT, REQUEST_DELAY_MS, MIN_BYTES
  MAX_WORKERS, MAX_CRAWL_MINUTES, MAX_PAGES
"""

from __future__ import annotations
import os, re, time, html, hashlib
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from requests.exceptions import SSLError, RequestException
from bs4 import BeautifulSoup

# ---------- ENV ----------
BASE_URL        = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL       = os.getenv("START_URL", f"{BASE_URL}/catalog/")
OUT_FILE        = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN       = os.getenv("VTT_LOGIN", "").strip()
VTT_PASSWORD    = os.getenv("VTT_PASSWORD", "").strip()

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0").lower() in ("1","true","yes")
ALLOW_SSL_FALLBACK = os.getenv("ALLOW_SSL_FALLBACK", "1").lower() in ("1","true","yes")

HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS= int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "800"))
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", "6"))
MAX_CRAWL_MIN   = int(os.getenv("MAX_CRAWL_MINUTES", "45"))
MAX_PAGES       = int(os.getenv("MAX_PAGES", "800"))

SUPPLIER_NAME   = "vtt"
CURRENCY        = "RUB"
ROOT_CAT_ID     = 9600000
ROOT_CAT_NAME   = "VTT"

UA = {
    "User-Agent": "Mozilla/5.0 (compatible; VTT-B2B-Login/1.1)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru,en;q=0.8",
}

session = requests.Session()
session.headers.update(UA)
session.verify = not DISABLE_SSL_VERIFY
if DISABLE_SSL_VERIFY:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print("[ssl] verification disabled by env")

# ---------- UTILS ----------
def _ssl_retry_toggle():
    """Отключает verify и предупреждения для повторной попытки."""
    import urllib3
    session.verify = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print("[ssl] fallback: retrying with verify=False")

def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms/1000.0))

def http_get(url: str) -> Optional[requests.Response]:
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code != 200: return None
        if r.content is None or len(r.content) < MIN_BYTES: return None
        return r
    except SSLError as e:
        print(f"[http] GET SSL fail {url}: {e}")
        if session.verify and ALLOW_SSL_FALLBACK:
            _ssl_retry_toggle()
            try:
                r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
                if r.status_code != 200: return None
                if r.content is None or len(r.content) < MIN_BYTES: return None
                return r
            except Exception as e2:
                print(f"[http] GET fail after fallback {url}: {e2}")
                return None
        return None
    except RequestException as e:
        print(f"[http] GET fail {url}: {e}")
        return None

def http_post(url: str, data: Dict[str, Any], headers: Dict[str,str]|None=None) -> Optional[requests.Response]:
    try:
        r = session.post(url, data=data, headers=headers or {}, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code not in (200, 302): return None
        return r
    except SSLError as e:
        print(f"[http] POST SSL fail {url}: {e}")
        if session.verify and ALLOW_SSL_FALLBACK:
            _ssl_retry_toggle()
            try:
                r = session.post(url, data=data, headers=headers or {}, timeout=HTTP_TIMEOUT, allow_redirects=True)
                if r.status_code not in (200, 302): return None
                return r
            except Exception as e2:
                print(f"[http] POST fail after fallback {url}: {e2}")
                return None
        return None
    except RequestException as e:
        print(f"[http] POST fail {url}: {e}")
        return None

def soup_of(r: requests.Response) -> BeautifulSoup:
    return BeautifulSoup(r.content, "html.parser")

def yml_escape(s: str) -> str:
    return html.escape(s or "")

def sha1(s: str) -> str:
    import hashlib as _h
    return _h.sha1(s.encode("utf-8")).hexdigest()

def to_number(x: Any) -> Optional[float]:
    if x is None: return None
    s = str(x).replace("\xa0"," ").strip().replace(" ", "").replace(",", ".")
    if not re.search(r"\d", s): return None
    try: return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        return float(m.group(0)) if m else None

# ---------- LOGIN ----------
LOGIN_PATHS = ["/login", "/auth/login", "/signin", "/account/login", "/user/login"]

def is_login_page(s: BeautifulSoup) -> bool:
    if s.find("input", {"type": "password"}): return True
    txt = s.get_text(" ", strip=True).lower()
    return ("вход" in txt and "пароль" in txt) or "логин" in txt

def detect_login_form(s: BeautifulSoup) -> Optional[Tuple[str, Dict[str,str]]]:
    for form in s.find_all("form"):
        if not form.find("input", {"type": "password"}):
            continue
        action = form.get("action") or ""
        fields = {"user": None, "pass": None, "csrf": None}
        names = [inp.get("name") for inp in form.find_all("input") if inp.get("name")]
        for cand in ["email", "login", "username", "user", "phone"]:
            if cand in names:
                fields["user"] = cand; break
        for cand in ["password", "passwd", "pass"]:
            if cand in names:
                fields["pass"] = cand; break
        for inp in form.find_all("input", {"type": "hidden"}):
            n = (inp.get("name") or "").lower()
            if "csrf" in n or n in ("_token", "csrf_token", "csrfmiddlewaretoken"):
                fields["csrf"] = inp.get("name"); break
        return action, fields
    return None

def find_csrf_meta(s: BeautifulSoup) -> Optional[str]:
    m = s.find("meta", attrs={"name": re.compile(r"csrf", re.I)})
    if m and m.get("content"):
        return m["content"].strip()
    return None

def login_vtt() -> bool:
    r = http_get(START_URL)
    if r:
        s = soup_of(r)
        if not is_login_page(s):
            print("[login] not required")
            return True
        login_page = r
    else:
        login_page = None
        for p in LOGIN_PATHS:
            rr = http_get(urljoin(BASE_URL, p))
            if rr:
                login_page = rr
                break
        if not login_page:
            print("[login] cannot open login page")
            return False

    s = soup_of(login_page)
    form = detect_login_form(s)
    if not form:
        print("[login] form not found")
        return False

    action_rel, fields = form
    action = urljoin(BASE_URL, action_rel)
    user_field, pass_field, csrf_field = fields["user"], fields["pass"], fields["csrf"]
    if not (user_field and pass_field):
        print("[login] username/password fields not detected")
        return False

    payload = {user_field: VTT_LOGIN, pass_field: VTT_PASSWORD}
    token = None
    if csrf_field:
        hidden = s.find("input", {"name": csrf_field})
        if hidden and hidden.get("value"):
            token = hidden["value"].strip()
            payload[csrf_field] = token
    if not token:
        token = find_csrf_meta(s)
    headers = {}
    if token:
        headers["X-CSRF-TOKEN"] = token
        headers["X-XSRF-TOKEN"] = token

    pr = http_post(action, payload, headers=headers)
    if not pr:
        print("[login] POST failed")
        return False

    test = http_get(START_URL)
    if not test:
        print("[login] after POST, catalog not accessible")
        return False
    if is_login_page(soup_of(test)):
        print("[login] still on login page -> wrong creds or extra step required")
        return False

    print("[login] success")
    return True

# ---------- DISCOVER ONLY CARTRIDGE CATEGORIES ----------
CAT_HINTS = ["картридж", "тонер", "cartridge", "toner"]

def same_host(u: str) -> bool:
    try: return urlparse(u).hostname == urlparse(BASE_URL).hostname
    except Exception: return False

def looks_like_catalog(u: str) -> bool:
    return "/catalog" in u

def is_cartridge_anchor(text: str, href: str) -> bool:
    t = (text or "").lower(); h = (href or "").lower()
    return any(k in t for k in CAT_HINTS) or any(k in h for k in CAT_HINTS)

def discover_cartridge_pages(start_url: str, deadline: datetime) -> List[str]:
    seen: Set[str] = set(); queue: List[str] = [start_url]; out: List[str] = []; pages = 0
    while queue and pages < MAX_PAGES and datetime.utcnow() < deadline:
        u = queue.pop(0)
        if u in seen: continue
        seen.add(u)
        jitter_sleep(REQUEST_DELAY_MS)
        r = http_get(u)
        if not r: continue
        s = soup_of(r)
        if is_login_page(s): break
        if is_cartridge_anchor(s.title.string if s.title else "", u):
            out.append(u)
        for a in s.find_all("a", href=True):
            absu = urljoin(u, a["href"])
            if not same_host(absu): continue
            if not looks_like_catalog(absu): continue
            if is_cartridge_anchor(a.get_text(" ", strip=True), absu):
                if "#" in absu: absu = absu.split("#", 1)[0]
                if absu not in seen and absu not in queue:
                    queue.append(absu)
        pages += 1
    return list(dict.fromkeys(out))

# ---------- PRODUCT PARSING ----------
def is_product_page(s: BeautifulSoup) -> bool:
    if s.find(attrs={"itemprop": "sku"}): return True
    if s.find("meta", attrs={"property": "og:type", "content": "product"}): return True
    txt = s.get_text(" ", strip=True).lower()
    return any(x in txt for x in ["артикул", "sku", "код товара", "характеристики", "в корзину", "купить"])

def extract_title(s: BeautifulSoup) -> Optional[str]:
    h1 = s.find("h1")
    if h1 and h1.get_text(strip=True): return h1.get_text(" ", strip=True)
    og = s.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"): return og["content"].strip()
    return (s.title.string.strip() if s.title and s.title.string else None)

def extract_sku(s: BeautifulSoup) -> Optional[str]:
    sk = s.find(attrs={"itemprop": "sku"})
    if sk:
        val = sk.get_text(" ", strip=True)
        if val: return val
    txt = s.get_text(" ", strip=True)
    m = re.search(r"(?:Артикул|SKU|Код товара|Код)\s*[:#]?\s*([A-Za-zА-Яа-я0-9\-\._/]{2,})", txt, flags=re.I)
    if m: return m.group(1)
    cand = s.find(attrs={"data-sku": True})
    if cand: return cand.get("data-sku")
    return None

def to_price_from_soup(s: BeautifulSoup) -> Optional[float]:
    pe = s.find(attrs={"itemprop": "price"})
    if pe:
        raw = pe.get("content") or pe.get_text(" ", strip=True)
        p = to_number(raw)
        if p is not None: return p
    for cls in ["price", "current-price", "product-price", "price-value"]:
        for el in s.select(f".{cls}"):
            val = el.get("content") or el.get_text(" ", strip=True)
            p = to_number(val)
            if p is not None: return p
    txt = s.get_text(" ", strip=True)
    m = re.search(r"(\d[\d\s\.,]{2,})\s*(?:₽|руб)\b", txt, flags=re.I)
    if m: return to_number(m.group(1))
    m = re.search(r"цена[^0-9]*([\d\s\.,]{2,})", txt, flags=re.I)
    if m: return to_number(m.group(1))
    return None

def extract_picture(s: BeautifulSoup, base: str) -> Optional[str]:
    og = s.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"): return urljoin(base, og["content"].strip())
    im = s.find("img", attrs={"id": re.compile(r"(main|product)", re.I)})
    if im and (im.get("src") or im.get("data-src")): return urljoin(base, im.get("src") or im.get("data-src"))
    for img in s.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if any(k in src for k in ["product", "catalog", "images", "photo"]):
            return urljoin(base, src)
    return None

def extract_description(s: BeautifulSoup) -> Optional[str]:
    for sel in ['[itemprop="description"]', ".product-description", ".description", "#description"]:
        el = s.select_one(sel)
        if el and el.get_text(strip=True): return el.get_text(" ", strip=True)
    for blk in s.select("article, .content, .product, .card, #content"):
        txt = blk.get_text(" ", strip=True)
        if txt and len(txt) > 120: return txt
    return None

def extract_breadcrumbs(s: BeautifulSoup) -> List[str]:
    out: List[str] = []
    for bc in s.select('ul.breadcrumb, .breadcrumbs, .breadcrumb, .pathway, [class*="breadcrumb"], [class*="pathway"]'):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if not t: continue
            tl = t.lower()
            if tl in ("главная","home"): continue
            out.append(t.strip())
        if out: break
    return out

def collect_product_links(cat_url: str, deadline: datetime) -> List[str]:
    urls: List[str] = []
    page = cat_url
    pages = 0
    seen: Set[str] = set()
    while page and pages < 30 and datetime.utcnow() < deadline:
        if page in seen: break
        seen.add(page)
        jitter_sleep(REQUEST_DELAY_MS)
        r = http_get(page)
        if not r: break
        s = soup_of(r)
        for a in s.find_all("a", href=True):
            href = a["href"]
            absu = urljoin(page, href)
            if "/product" in absu or re.search(r"/catalog/.+/.+\.html?$", absu) or re.search(r"/\d{5,}", absu):
                urls.append(absu)
        nxt = s.find("link", rel=lambda v: v and "next" in v.lower())
        if nxt and nxt.get("href"):
            page = urljoin(page, nxt["href"])
        else:
            a_next = None
            for a in s.find_all("a", href=True):
                t = (a.get_text(" ", strip=True) or "").lower()
                if t in ("следующая","вперёд","вперед","next",">"):
                    a_next = a; break
            page = urljoin(page, a_next["href"]) if a_next else None
        pages += 1
    return list(dict.fromkeys(urls))

# ---------- YML ----------
def stable_cat_id(text: str, prefix: int = 9700000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories_from_paths(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
    cat_map: Dict[Tuple[str,...], int] = {}
    out: List[Tuple[int,str,Optional[int]]] = []
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная","home","каталог")]
        if not clean: continue
        parent = ROOT_CAT_ID
        prefix: List[str] = []
        for name in clean:
            prefix.append(name)
            key = tuple(prefix)
            if key in cat_map:
                parent = cat_map[key]; continue
            cid = stable_cat_id(" / ".join(prefix))
            cat_map[key] = cid
            out.append((cid, name, parent))
            parent = cid
    return out, cat_map

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
            f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>",
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

# ---------- MAIN ----------
def main() -> int:
    if not (VTT_LOGIN and VTT_PASSWORD):
        print("Error: set VTT_LOGIN/VTT_PASSWORD")
        return 2

    if not login_vtt():
        print("Error: login failed")
        return 2

    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MIN)

    cat_pages = discover_cartridge_pages(START_URL, deadline)
    if not cat_pages:
        print("Error: cartridge sections not found")
        os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
        enc = "cp1251" if OUTPUT_ENCODING.lower() in ("windows-1251","cp1251") else OUTPUT_ENCODING
        with open(OUT_FILE, "w", encoding=enc, errors="ignore") as f:
            f.write(build_yml([], []))
        return 0

    print(f"[discover] cartridge pages: {len(cat_pages)}")

    product_urls: List[str] = []
    for cu in cat_pages:
        product_urls.extend(collect_product_links(cu, deadline))
    product_urls = list(dict.fromkeys(product_urls))
    print(f"[collect] product urls: {len(product_urls)}")

    def worker(u: str):
        if datetime.utcnow() > deadline: return None
        jitter_sleep(REQUEST_DELAY_MS)
        r = http_get(u)
        if not r: return None
        s = soup_of(r)
        if not is_product_page(s): return None

        title = (extract_title(s) or "").strip()
        price = to_price_from_soup(s)
        sku   = (extract_sku(s) or "").strip()
        pic   = extract_picture(s, u)
        desc  = extract_description(s) or ""
        crumbs= extract_breadcrumbs(s)

        if not title or price is None or not sku:
            return None

        return {
            "url": u,
            "title": title,
            "price": float(f"{price:.2f}"),
            "vendorCode": sku,
            "picture": pic,
            "description": desc,
            "crumbs": crumbs,
        }

    items: List[Dict[str,Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(worker, u): u for u in product_urls}
        for fut in as_completed(futures):
            rec = fut.result()
            if rec:
                items.append(rec)
                if len(items) % 50 == 0:
                    print(f"[parse] {len(items)}")

    print(f"[parse] total: {len(items)}")

    paths = [p["crumbs"] for p in items if p.get("crumbs")]
    cat_list, path_id_map = build_categories_from_paths(paths)

    offers: List[Tuple[int,Dict[str,Any]]] = []
    seen_ids: Set[str] = set()
    for it in items:
        cid = ROOT_CAT_ID
        crumbs = it.get("crumbs") or []
        if crumbs:
            clean = [p.strip() for p in crumbs if p and p.strip()]
            clean = [p for p in clean if p.lower() not in ("главная","home","каталог")]
            key = tuple(clean)
            while key and key not in path_id_map:
                key = key[:-1]
            if key and key in path_id_map:
                cid = path_id_map[key]

        offer_id = it["vendorCode"]
        if offer_id in seen_ids:
            offer_id = f"{offer_id}-{sha1(it['title'])[:6]}"
        seen_ids.add(offer_id)

        offers.append((cid, {
            "offer_id":   offer_id,
            "title":      it["title"],
            "price":      it["price"],
            "vendorCode": it["vendorCode"],
            "brand":      SUPPLIER_NAME,
            "url":        it["url"],
            "picture":    it.get("picture"),
            "description": it.get("description") or it["title"],
        }))

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    enc = "cp1251" if OUTPUT_ENCODING.lower() in ("windows-1251","cp1251") else OUTPUT_ENCODING
    with open(OUT_FILE, "w", encoding=enc, errors="ignore") as f:
        f.write(build_yml(cat_list, offers))

    print(f"[done] items: {len(offers)}, cats: {len(cat_list)} -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        sys.exit(2)
