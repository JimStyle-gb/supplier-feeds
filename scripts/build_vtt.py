# -*- coding: utf-8 -*-
"""
b2b.vtt.ru → YML
- Логин по форме (user/pass).
- BFS обход каталога /catalog/... (wide по умолчанию).
- Парсинг карточек: JSON-LD + расширенные селекторы.
- Отладка: docs/vtt_debug_root.html, docs/vtt_debug_links.txt, docs/vtt_debug_log.txt,
           docs/vtt_fail_001.html..docs/vtt_fail_010.html (первые 10 нераспознанных).
"""

from __future__ import annotations
import os, re, time, html, hashlib, io, json, warnings
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from requests.exceptions import SSLError, RequestException
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning

# глушим шумные ворнинги
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
warnings.filterwarnings("ignore", message="Some characters could not be decoded")

# ---------- ENV ----------
BASE_URL        = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL       = os.getenv("START_URL", f"{BASE_URL}/catalog/")
OUT_FILE        = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN       = os.getenv("VTT_LOGIN", "").strip()
VTT_PASSWORD    = os.getenv("VTT_PASSWORD", "").strip()

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "1").lower() in ("1","true","yes")
ALLOW_SSL_FALLBACK = os.getenv("ALLOW_SSL_FALLBACK", "1").lower() in ("1","true","yes")

HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS= int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "800"))
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", "6"))
MAX_CRAWL_MIN   = int(os.getenv("MAX_CRAWL_MINUTES", "45"))
MAX_PAGES       = int(os.getenv("MAX_PAGES", "900"))
CRAWL_MODE      = os.getenv("CRAWL_MODE", "wide").lower()  # wide | cartridges

SUPPLIER_NAME   = "vtt"
CURRENCY        = "RUB"
ROOT_CAT_ID     = 9600000
ROOT_CAT_NAME   = "VTT"

UA = {
    "User-Agent": "Mozilla/5.0 (compatible; VTT-B2B/1.4)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru,en;q=0.8",
}

# ---------- Session ----------
session = requests.Session()
session.headers.update(UA)
session.verify = not DISABLE_SSL_VERIFY
if DISABLE_SSL_VERIFY:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print("[ssl] verification disabled by env")

# ---------- Debug ----------
DEBUG_DIR = os.path.dirname(OUT_FILE) or "docs"
os.makedirs(DEBUG_DIR, exist_ok=True)
DEBUG_LOG = os.path.join(DEBUG_DIR, "vtt_debug_log.txt")

def dlog(msg: str):
    print(msg)
    try:
        with io.open(DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def save_file(name: str, content: bytes | str):
    path = os.path.join(DEBUG_DIR, name)
    try:
        mode = "wb" if isinstance(content, (bytes, bytearray)) else "w"
        with open(path, mode) as f:
            f.write(content)
        dlog(f"[debug] saved {path}")
    except Exception as e:
        dlog(f"[debug] save failed {path}: {e}")

# ---------- HTTP ----------
def _ssl_retry_toggle():
    import urllib3
    session.verify = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    dlog("[ssl] fallback: verify=False")

def http_get(url: str) -> Optional[requests.Response]:
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            dlog(f"[http] GET {url} -> {r.status_code}")
            return None
        if not r.content or len(r.content) < MIN_BYTES:
            dlog(f"[http] GET too small: {url} ({len(r.content) if r.content else 0} bytes)")
            return None
        return r
    except SSLError as e:
        dlog(f"[http] GET SSL fail {url}: {e}")
        if session.verify and ALLOW_SSL_FALLBACK:
            _ssl_retry_toggle()
            try:
                r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
                if r.status_code != 200 or not r.content or len(r.content) < MIN_BYTES:
                    return None
                return r
            except Exception as e2:
                dlog(f"[http] GET fail after fallback {url}: {e2}")
                return None
        return None
    except RequestException as e:
        dlog(f"[http] GET fail {url}: {e}")
        return None

def http_post(url: str, data: Dict[str, Any], headers: Dict[str,str]|None=None) -> Optional[requests.Response]:
    try:
        r = session.post(url, data=data, headers=headers or {}, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code not in (200, 302):
            dlog(f"[http] POST {url} -> {r.status_code}")
            return None
        return r
    except SSLError as e:
        dlog(f"[http] POST SSL fail {url}: {e}")
        if session.verify and ALLOW_SSL_FALLBACK:
            _ssl_retry_toggle()
            try:
                r = session.post(url, data=data, headers=headers or {}, timeout=HTTP_TIMEOUT, allow_redirects=True)
                if r.status_code not in (200, 302):
                    return None
                return r
            except Exception as e2:
                dlog(f"[http] POST fail after fallback {url}: {e2}")
                return None
        return None
    except RequestException as e:
        dlog(f"[http] POST fail {url}: {e}")
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

# ---------- Login ----------
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
    if not (VTT_LOGIN and VTT_PASSWORD):
        dlog("Error: set VTT_LOGIN/VTT_PASSWORD")
        return False

    r = http_get(START_URL)
    if r:
        save_file("vtt_debug_root.html", r.content)
        s = soup_of(r)
        if not is_login_page(s):
            dlog("[login] not required")
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
            dlog("[login] cannot open login page")
            return False

    s = soup_of(login_page)
    form = detect_login_form(s)
    if not form:
        dlog("[login] form not found")
        return False

    action_rel, fields = form
    action = urljoin(BASE_URL, action_rel)
    user_field, pass_field, csrf_field = fields["user"], fields["pass"], fields["csrf"]
    if not (user_field and pass_field):
        dlog("[login] username/password fields not detected")
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
        dlog("[login] POST failed")
        return False

    test = http_get(START_URL)
    if test:
        save_file("vtt_debug_root.html", test.content)
    if not test:
        dlog("[login] after POST, catalog not accessible")
        return False
    if is_login_page(soup_of(test)):
        dlog("[login] still on login page -> wrong creds or extra step required")
        return False

    dlog("[login] success")
    return True

# ---------- Crawl ----------
def same_host(u: str) -> bool:
    try: return urlparse(u).hostname == urlparse(BASE_URL).hostname
    except Exception: return False

def looks_like_catalog(u: str) -> bool:
    return "/catalog" in u

CARTRIDGE_HINTS = ["картридж", "тонер", "cartridge", "toner"]

def is_cartridge_anchor(text: str, href: str) -> bool:
    t = (text or "").lower(); h = (href or "").lower()
    return any(k in t for k in CARTRIDGE_HINTS) or any(k in h for k in CARTRIDGE_HINTS)

def discover_pages(start_url: str, deadline: datetime) -> List[str]:
    seen: Set[str] = set(); queue: List[str] = [start_url]; out: List[str] = []; pages = 0
    links_dump: List[str] = []
    while queue and pages < MAX_PAGES and datetime.utcnow() < deadline:
        u = queue.pop(0)
        if u in seen: continue
        seen.add(u)
        time.sleep(max(0.0, REQUEST_DELAY_MS/1000.0))
        r = http_get(u)
        if not r: 
            continue
        s = soup_of(r)
        title_txt = (s.title.string.strip() if s.title and s.title.string else "")
        links_dump.append(f"[{pages}] {u} :: {title_txt}")

        # сохраняем страницу в обход
        if CRAWL_MODE == "cartridges":
            if is_cartridge_anchor(title_txt, u):
                out.append(u)
        else:
            out.append(u)

        # ссылки глубже
        for a in s.find_all("a", href=True):
            absu = urljoin(u, a["href"])
            if not same_host(absu): continue
            if not looks_like_catalog(absu): continue
            if "#" in absu: absu = absu.split("#", 1)[0]
            if CRAWL_MODE == "cartridges":
                if not is_cartridge_anchor(a.get_text(" ", strip=True), absu):
                    continue
            if absu not in seen and absu not in queue:
                queue.append(absu)
        pages += 1

    save_file("vtt_debug_links.txt", "\n".join(links_dump))
    return list(dict.fromkeys(out))

def collect_product_links(page_url: str, deadline: datetime) -> List[str]:
    urls: List[str] = []; seen: Set[str] = set()
    if datetime.utcnow() > deadline: return urls
    r = http_get(page_url)
    if not r: return urls
    s = soup_of(r)

    # Пытаемся попасть в карточки: разные паттерны
    for a in s.find_all("a", href=True):
        href = a["href"]; absu = urljoin(page_url, href)
        if absu in seen: continue
        if re.search(r"/product", absu) \
           or re.search(r"/catalog/[^/?#]+/[^/?#]+/?$", absu) \
           or re.search(r"/catalog/.+/\d{4,}/?$", absu):
            urls.append(absu); seen.add(absu)

    # Простейшая пагинация
    nxt = s.find("link", rel=lambda v: v and "next" in v.lower())
    if nxt and nxt.get("href"):
        urls.extend(collect_product_links(urljoin(page_url, nxt["href"]), deadline))

    return urls

# ---------- Product parsing ----------
def is_product_page(s: BeautifulSoup) -> bool:
    if s.find(attrs={"itemprop": "sku"}): return True
    if s.find("meta", attrs={"property": "og:type", "content": "product"}): return True
    if s.find("script", attrs={"type": "application/ld+json"}): return True
    txt = s.get_text(" ", strip=True).lower()
    return any(x in txt for x in ["артикул", "sku", "код товара", "в корзину", "купить"])

def jsonld_product(s: BeautifulSoup) -> Optional[Dict[str, Any]]:
    # берём первый Product в JSON-LD
    for tag in s.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.get_text(strip=True))
        except Exception:
            continue
        def find_prod(obj):
            if isinstance(obj, dict):
                t = obj.get("@type") or obj.get("type")
                if isinstance(t, list):
                    t = next((x for x in t if isinstance(x, str)), None)
                if isinstance(t, str) and "product" in t.lower():
                    return obj
                for v in obj.values():
                    found = find_prod(v)
                    if found: return found
            elif isinstance(obj, list):
                for it in obj:
                    found = find_prod(it)
                    if found: return found
            return None
        prod = find_prod(data)
        if prod: return prod
    return None

def extract_title(s: BeautifulSoup) -> Optional[str]:
    h1 = s.find("h1")
    if h1 and h1.get_text(strip=True): return h1.get_text(" ", strip=True)
    og = s.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"): return og["content"].strip()
    return (s.title.string.strip() if s.title and s.title.string else None)

def extract_sku(s: BeautifulSoup) -> Optional[str]:
    # JSON-LD
    prod = jsonld_product(s)
    if prod:
        sku = prod.get("sku") or prod.get("mpn") or prod.get("productID")
        if isinstance(sku, (int, float)): sku = str(sku)
        if sku: return str(sku).strip()

    sk = s.find(attrs={"itemprop": "sku"})
    if sk:
        val = sk.get_text(" ", strip=True) or sk.get("content")
        if val: return val

    # частые подписи
    for lab in ["артикул", "код товара", "код", "sku", "mpn"]:
        node = s.find(string=lambda t: t and lab in t.lower())
        if node:
            txt = node.parent.get_text(" ", strip=True) if node.parent else str(node)
            m = re.search(r"([A-Za-zА-Яа-я0-9\-\._/]{2,})", txt)
            if m: return m.group(1)

    txt = s.get_text(" ", strip=True)
    m = re.search(r"(?:Артикул|SKU|Код товара|Код)\s*[:#]?\s*([A-Za-zА-Яа-я0-9\-\._/]{2,})", txt, flags=re.I)
    if m: return m.group(1)
    cand = s.find(attrs={"data-sku": True})
    if cand: return cand.get("data-sku")
    return None

def to_price_from_soup(s: BeautifulSoup) -> Optional[float]:
    # JSON-LD
    prod = jsonld_product(s)
    if prod:
        offers = prod.get("offers")
        if isinstance(offers, list):
            offers = offers[0] if offers else None
        if isinstance(offers, dict):
            p = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
            if p is not None:
                pp = to_number(p)
                if pp is not None: return pp

    pe = s.find(attrs={"itemprop": "price"})
    if pe:
        raw = pe.get("content") or pe.get_text(" ", strip=True)
        p = to_number(raw)
        if p is not None: return p

    # разные классы цены
    for cls in ["price", "current-price", "product-price", "price-value",
                "product__price", "price__current", "card-price", "c-price"]:
        for el in s.select(f".{cls}"):
            val = el.get("content") or el.get_text(" ", strip=True)
            p = to_number(val)
            if p is not None: return p

    # по всему тексту
    txt = s.get_text(" ", strip=True)
    m = re.search(r"(\d[\d\s\.,]{2,})\s*(?:₽|руб)\b", txt, flags=re.I)
    if m: return to_number(m.group(1))
    m = re.search(r"цена[^0-9]*([\d\s\.,]{2,})", txt, flags=re.I)
    if m: return to_number(m.group(1))
    return None

def extract_picture(s: BeautifulSoup, base: str) -> Optional[str]:
    # JSON-LD
    prod = jsonld_product(s)
    if prod:
        img = prod.get("image")
        if isinstance(img, list) and img: img = img[0]
        if isinstance(img, str) and img.strip():
            return urljoin(base, img.strip())

    og = s.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"): return urljoin(base, og["content"].strip())
    im = s.find("img", attrs={"id": re.compile(r"(main|product)", re.I)})
    if im and (im.get("src") or im.get("data-src")):
        return urljoin(base, im.get("src") or im.get("data-src"))
    for img in s.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if any(k in src for k in ["product", "catalog", "images", "photo", "goods"]):
            return urljoin(base, src)
    return None

def extract_description(s: BeautifulSoup) -> Optional[str]:
    # JSON-LD
    prod = jsonld_product(s)
    if prod:
        desc = prod.get("description")
        if isinstance(desc, str) and desc.strip():
            return desc.strip()

    for sel in ['[itemprop="description"]', ".product-description", ".description", "#description", ".product__description"]:
        el = s.select_one(sel)
        if el and el.get_text(strip=True): return el.get_text(" ", strip=True)
    for blk in s.select("article, .content, .product, .card, #content, .tabs"):
        txt = blk.get_text(" ", strip=True)
        if txt and len(txt) > 120: return txt
    return None

def extract_breadcrumbs(s: BeautifulSoup) -> List[str]:
    out: List[str] = []
    for bc in s.select('ul.breadcrumb, nav.breadcrumbs, .breadcrumbs, .breadcrumb, .pathway, [class*="breadcrumb"], [class*="pathway"]'):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if not t: continue
            tl = t.lower()
            if tl in ("главная","home"): continue
            out.append(t.strip())
        if out: break
    return out

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
        out += ["<quantity_in_stock>1</quantity_in_stock>",
                "<stock_quantity>1</stock_quantity>",
                "<quantity>1</quantity>", "</offer>"]
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------- MAIN ----------
def main() -> int:
    # обнулим лог
    try:
        with io.open(DEBUG_LOG, "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass

    if not login_vtt():
        dlog("Error: login failed")
        _write_empty()
        return 0

    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MIN)

    pages = discover_pages(START_URL, deadline)
    dlog(f"[discover] pages: {len(pages)}")
    if not pages:
        _write_empty()
        return 0

    # собираем ссылки карточек
    prod_urls: List[str] = []
    for u in pages:
        if datetime.utcnow() > deadline: break
        prod_urls.extend(collect_product_links(u, deadline))
    prod_urls = list(dict.fromkeys(prod_urls))
    dlog(f"[collect] product urls: {len(prod_urls)}")

    fail_saved = 0
    FAIL_SAVE_LIMIT = 10

    def worker(u: str):
        nonlocal fail_saved
        if datetime.utcnow() > deadline: return None
        time.sleep(max(0.0, REQUEST_DELAY_MS/1000.0))
        r = http_get(u)
        if not r: return None
        s = soup_of(r)
        if not is_product_page(s):
            if fail_saved < FAIL_SAVE_LIMIT:
                fname = f"vtt_fail_{fail_saved+1:03d}.html"
                save_file(fname, r.content)
                fail_saved += 1
            return None

        title = (extract_title(s) or "").strip()
        price = to_price_from_soup(s)
        sku   = (extract_sku(s) or "").strip()
        pic   = extract_picture(s, u)
        desc  = extract_description(s) or ""
        crumbs= extract_breadcrumbs(s)

        # если JSON-LD дал только часть — дособираем
        if not title:
            prod = jsonld_product(s)
            if prod:
                title = prod.get("name") or title
                if not pic:
                    img = prod.get("image")
                    if isinstance(img, list) and img: img = img[0]
                    if isinstance(img, str): pic = urljoin(u, img)

        if not title or price is None or not sku:
            if fail_saved < FAIL_SAVE_LIMIT:
                fname = f"vtt_fail_{fail_saved+1:03d}.html"
                save_file(fname, r.content)
                fail_saved += 1
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
        futures = {ex.submit(worker, u): u for u in prod_urls}
        for fut in as_completed(futures):
            rec = fut.result()
            if rec:
                items.append(rec)
                if len(items) % 50 == 0:
                    dlog(f"[parse] {len(items)}")

    dlog(f"[parse] total: {len(items)}")

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

    _write_yml(cat_list, offers)
    dlog(f"[done] items: {len(offers)}, cats: {len(cat_list)} -> {OUT_FILE}")
    return 0

def _write_empty():
    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    enc = "cp1251" if OUTPUT_ENCODING.lower() in ("windows-1251","cp1251") else OUTPUT_ENCODING
    with open(OUT_FILE, "w", encoding=enc, errors="ignore") as f:
        f.write(build_yml([], []))
    dlog("[write] empty yml")

def _write_yml(categories, offers):
    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    enc = "cp1251" if OUTPUT_ENCODING.lower() in ("windows-1251","cp1251") else OUTPUT_ENCODING
    with open(OUT_FILE, "w", encoding=enc, errors="ignore") as f:
        f.write(build_yml(categories, offers))

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        dlog(f"[fatal] {e}")
        _write_empty()
        sys.exit(0)
