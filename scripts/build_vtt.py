# -*- coding: utf-8 -*-
"""
B2B VTT → YML: авто-логин + универсальный обход каталога и парсинг карточек.
Если сайт рендерит карточки на сервере (SSR), скрипт соберёт товары и сформирует YML.
Сильные стороны:
- Логин: по cookies ИЛИ логин/пароль (csrf, _token, XSRF-TOKEN).
- Обход: BFS по /catalog/, ограничение по страницам/времени.
- Детектор карточек: sku/цена/кнопка/JSON-LD/schema.org/Product/ог-type=product.
- Парсер: h1/og:title, itemprop=price/sku, "Цена"/"Артикул", og:image, breadcrumbs.
- Отладка: сохраняет ключевые страницы в docs/.

deps: requests, beautifulsoup4, lxml
"""

from __future__ import annotations
import os, io, re, time, html, hashlib, sys
from typing import Optional, Dict, Any, List, Tuple, Set
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup

# ----------------- ENV -----------------
BASE_URL        = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL       = os.getenv("START_URL", f"{BASE_URL}/catalog/")
OUT_FILE        = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN       = os.getenv("VTT_LOGIN", "").strip()
VTT_PASSWORD    = os.getenv("VTT_PASSWORD", "").strip()
VTT_COOKIES     = os.getenv("VTT_COOKIES", "").strip()

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"
ALLOW_SSL_FALLBACK = os.getenv("ALLOW_SSL_FALLBACK", "1") == "1"
HTTP_TIMEOUT        = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS    = int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES           = int(os.getenv("MIN_BYTES", "800"))

MAX_PAGES           = int(os.getenv("MAX_PAGES", "600"))
MAX_CRAWL_MINUTES   = int(os.getenv("MAX_CRAWL_MINUTES", "60"))

DEBUG_ROOT  = "docs"
DEBUG_LOG   = os.path.join(DEBUG_ROOT, "vtt_debug_log.txt")

# ----------------- helpers -----------------
def ensure_dirs():
    os.makedirs(DEBUG_ROOT, exist_ok=True)

def dlog(msg: str):
    print(msg, flush=True)
    try:
        with io.open(DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def jitter_sleep(ms: int):
    time.sleep(max(0.0, ms/1000.0))

def save_debug(name: str, content: bytes | str):
    path = os.path.join(DEBUG_ROOT, name)
    try:
        if isinstance(content, (bytes, bytearray)):
            with open(path, "wb") as f:
                f.write(content)
        else:
            with open(path, "w", encoding="utf-8", errors="ignore") as f:
                f.write(content)
        dlog(f"[debug] saved {path}")
    except Exception as e:
        dlog(f"[debug] save failed {name}: {e}")

def make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru,en;q=0.9",
        "Connection": "keep-alive",
    })
    if DISABLE_SSL_VERIFY:
        dlog("[ssl] verification disabled by env")
        sess.verify = False
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
    return sess

def get_soup(html_bytes: bytes) -> BeautifulSoup:
    return BeautifulSoup(html_bytes, "lxml")

def http_get(sess: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = sess.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code != 200 or len(r.content) < MIN_BYTES:
            dlog(f"[http] GET bad status/len {url} -> {r.status_code}, {len(r.content)}")
            save_debug("vtt_fail_get.html", r.content)
            return None
        return r.content
    except requests.exceptions.SSLError as e:
        dlog(f"[http] GET SSL error {url}: {e}")
        if ALLOW_SSL_FALLBACK:
            try:
                r = sess.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True, verify=False)
                if r.status_code == 200 and len(r.content) >= MIN_BYTES:
                    return r.content
            except Exception as e2:
                dlog(f"[http] fallback fail {e2}")
        return None
    except Exception as e:
        dlog(f"[http] GET fail {url}: {e}")
        return None

def inject_cookie_string(sess: requests.Session, cookie_string: str):
    for part in cookie_string.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            sess.cookies.set(k.strip(), v.strip(), domain=urlparse(BASE_URL).hostname)

def extract_csrf(soup: BeautifulSoup) -> Optional[str]:
    m = soup.find("meta", attrs={"name": re.compile(r"csrf-token", re.I)})
    if m and m.get("content"):
        return m["content"].strip()
    inp = soup.find("input", attrs={"name": "_token"})
    if inp and inp.get("value"):
        return inp["value"].strip()
    return None

def guess_login_form(soup: BeautifulSoup) -> Dict[str, Any]:
    forms = soup.find_all("form")
    for frm in forms:
        txt = frm.get_text(" ", strip=True).lower()
        if any(w in txt for w in ["вход", "логин", "email", "пароль", "sign in", "login"]):
            action = frm.get("action") or "/login"
            fields = {}
            for inp in frm.find_all(["input", "button"]):
                name = inp.get("name")
                if not name:
                    continue
                val = inp.get("value") or ""
                fields[name] = val
            return {"action": action, "fields": fields}
    return {"action": "/login", "fields": {}}

def login_vtt(sess: requests.Session) -> bool:
    if VTT_COOKIES:
        inject_cookie_string(sess, VTT_COOKIES)
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, START_URL)
        if b:
            save_debug("vtt_debug_root_after_cookie.html", b)
            low = b.lower()
            if b"logout" in low or b"\xd0\xb2\xd1\x8b\xd0\xb9\xd1\x82\xd0\xb8" in low:
                dlog("[login] cookies ok (found logout)")
                return True
            return True
        dlog("[login] cookies injected, but catalog not reachable")

    login_pages = [
        f"{BASE_URL}/login",
        f"{BASE_URL}/signin",
        f"{BASE_URL}/auth/login",
        f"{BASE_URL}/account/login",
        f"{BASE_URL}/user/login",
        f"{BASE_URL}/",
    ]
    first_html = None
    first_url  = None

    for u in login_pages:
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, u)
        if not b:
            continue
        soup = get_soup(b)
        if soup.find("input", attrs={"type": "password"}) or soup.find(string=re.compile("пароль", re.I)):
            first_html = b
            first_url = u
            break
        if first_html is None:
            first_html = b
            first_url = u

    if not first_html:
        dlog("[login] cannot open login page")
        return False

    save_debug("vtt_debug_login_get.html", first_html)
    soup = get_soup(first_html)

    csrf = extract_csrf(soup)
    form  = guess_login_form(soup)
    action = form["action"]
    if not action.startswith("http"):
        action = urljoin(first_url, action)

    payload_base = {}
    if csrf:
        payload_base["_token"] = csrf

    login_keys = ["email", "login", "username"]
    pass_keys  = ["password", "passwd", "pass"]

    for lk in login_keys:
        for pk in pass_keys:
            payload = dict(payload_base)
            payload.update({ lk: VTT_LOGIN, pk: VTT_PASSWORD, "remember": "1" })
            headers = {
                "Origin": BASE_URL,
                "Referer": first_url,
                "Content-Type": "application/x-www-form-urlencoded",
            }
            xsrf = sess.cookies.get("XSRF-TOKEN")
            if xsrf:
                headers["X-XSRF-TOKEN"] = xsrf
            try:
                jitter_sleep(REQUEST_DELAY_MS)
                r = sess.post(action, data=payload, headers=headers,
                              timeout=HTTP_TIMEOUT, allow_redirects=True)
            except requests.exceptions.SSLError as e:
                dlog(f"[login] SSL post error: {e}")
                if ALLOW_SSL_FALLBACK:
                    r = sess.post(action, data=payload, headers=headers,
                                  timeout=HTTP_TIMEOUT, allow_redirects=True, verify=False)
                else:
                    continue

            save_debug("vtt_debug_login_post.html", r.content)

            if r.status_code in (200, 302):
                jitter_sleep(REQUEST_DELAY_MS)
                b2 = http_get(sess, START_URL)
                if b2:
                    save_debug("vtt_debug_root_after_login.html", b2)
                    low = b2.lower()
                    if b"logout" in low or b"\xd0\xb2\xd1\x8b\xd0\xb9\xd1\x82\xd0\xb8" in low:
                        dlog("[login] success (found logout)")
                        return True
                    dlog("[login] success (catalog reachable)")
                    return True
    return False

# ----------------- crawl -----------------
def same_host(u: str) -> bool:
    try:
        return urlparse(u).netloc == urlparse(BASE_URL).netloc
    except Exception:
        return False

def normalize_link(base: str, href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("javascript:") or href.startswith("mailto:"):
        return None
    absu = urljoin(base, href)
    absu, _ = urldefrag(absu)
    if not same_host(absu):
        return None
    # отсечём медиа/файлы
    if re.search(r"\.(jpg|jpeg|png|gif|svg|webp|pdf|docx?|xlsx?)$", absu, re.I):
        return None
    return absu

def is_catalog_like(u: str) -> bool:
    up = urlparse(u)
    return "/catalog" in up.path

def is_product_like(u: str) -> bool:
    p = urlparse(u).path.lower()
    # эвристика "карточка"
    return bool(re.search(r"/product|/products|/catalog/.+/\d{3,}", p))

def contains_any(soup: BeautifulSoup, patterns: List[re.Pattern]) -> bool:
    txt = soup.get_text(" ", strip=True)
    for p in patterns:
        if p.search(txt):
            return True
    return False

RX_PRICE = re.compile(r"(?:цена|₽|руб\.?|RUB)", re.I)
RX_ART   = re.compile(r"(?:артикул|SKU)", re.I)
RX_BUY   = re.compile(r"(?:в корзину|купить|заказать)", re.I)

def page_looks_like_product(soup: BeautifulSoup, url: str, raw: bytes) -> bool:
    score = 0
    if soup.find(attrs={"itemprop": "sku"}): score += 1
    if soup.find(attrs={"itemprop": "price"}): score += 1
    if soup.find("meta", attrs={"property": "og:type", "content": "product"}): score += 1
    if contains_any(soup, [RX_PRICE]): score += 1
    if contains_any(soup, [RX_ART]):   score += 1
    if contains_any(soup, [RX_BUY]):   score += 1
    if is_product_like(url):           score += 1
    return score >= 2

def discover_and_collect(sess: requests.Session, start_url: str) -> Tuple[List[str], List[str]]:
    queue: List[str] = [start_url]
    seen: Set[str] = set()
    product_urls: List[str] = []
    listing_samples: List[str] = []

    t0 = time.time()
    pages = 0

    while queue and pages < MAX_PAGES and (time.time() - t0) < MAX_CRAWL_MINUTES*60:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, url)
        if not b:
            continue
        soup = get_soup(b)

        if pages == 0:
            save_debug("vtt_debug_root.html", b)
        if is_catalog_like(url) and len(listing_samples) < 5:
            fn = f"vtt_page_listing_{len(listing_samples)+1}.html"
            save_debug(fn, b)
            listing_samples.append(fn)

        if page_looks_like_product(soup, url, b):
            product_urls.append(url)
        else:
            # тянем ссылки глубже только из каталога
            if is_catalog_like(url):
                for a in soup.find_all("a", href=True):
                    nu = normalize_link(url, a["href"])
                    if not nu:
                        continue
                    if "/logout" in nu or "/login" in nu:
                        continue
                    # приоритет карточек и вложенных разделов каталога
                    if is_product_like(nu) or is_catalog_like(nu):
                        queue.append(nu)

        pages += 1

    product_urls = list(dict.fromkeys(product_urls))
    dlog(f"[discover] pages: {pages}, product urls: {len(product_urls)}")
    return product_urls, list(seen)

# ----------------- parse product -----------------
def text(s: Optional[str]) -> str:
    return (s or "").strip()

def first_text(soup: BeautifulSoup, selectors: List[str]) -> Optional[str]:
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            t = el.get_text(" ", strip=True)
            if t:
                return t
    return None

def find_price(soup: BeautifulSoup) -> Optional[float]:
    # itemprop/ld+json
    m = soup.find(attrs={"itemprop": "price"})
    if m:
        v = m.get("content") or m.get("value") or m.get_text(" ", strip=True)
        if v:
            return to_price(v)

    # текстовые эвристики
    txt = soup.get_text(" ", strip=True)
    # ищем числа рядом с "Цена" или ₽/руб
    cand = re.findall(r"(?:цена[^0-9]{0,20}|)(\d[\d\s.,]{2,})(?:\s*(?:₽|руб\.?|RUB))", txt, flags=re.I)
    if not cand:
        cand = re.findall(r"(\d[\d\s.,]{2,})\s*(?:₽|руб\.?|RUB)", txt, flags=re.I)
    for c in cand:
        pr = to_price(c)
        if pr and pr > 0:
            return pr

    return None

def to_price(s: str) -> Optional[float]:
    s = s.replace("\xa0", " ")
    s = re.sub(r"[^\d,\. ]", "", s)
    s = s.replace(" ", "")
    # десятичная запятая/точка
    if s.count(",") == 1 and s.count(".") == 0:
        s = s.replace(",", ".")
    try:
        v = float(s)
        if v > 0:
            return float(f"{v:.2f}")
    except Exception:
        pass
    return None

def find_sku(soup: BeautifulSoup) -> Optional[str]:
    m = soup.find(attrs={"itemprop": "sku"})
    if m:
        val = m.get("content") or m.get_text(" ", strip=True)
        if val:
            return val.strip()
    # подпись "Артикул"
    node = soup.find(string=re.compile(r"Артикул|SKU", re.I))
    if node:
        txt = node.parent.get_text(" ", strip=True) if node.parent else str(node)
        m = re.search(r"([A-Za-z0-9\-\._/]{2,})", txt)
        if m:
            return m.group(1)
    return None

def find_title(soup: BeautifulSoup) -> Optional[str]:
    t = first_text(soup, ["h1", "h1.product-title", "[itemprop=name]", "meta[property='og:title']"])
    if t:
        return t
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()
    return None

def find_image(soup: BeautifulSoup, base: str) -> Optional[str]:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return urljoin(base, og["content"].strip())
    m = soup.find("img", attrs={"itemprop": "image"})
    if m and (m.get("src") or m.get("data-src")):
        return urljoin(base, m.get("src") or m.get("data-src"))
    # первая крупная картинка
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src")
        if not src: 
            continue
        if any(k in src.lower() for k in ["product", "catalog", "goods", "images"]):
            return urljoin(base, src)
    return None

def find_breadcrumbs(soup: BeautifulSoup) -> List[str]:
    out: List[str] = []
    for bc in soup.select(".breadcrumb, .breadcrumbs, ul.breadcrumb, nav.breadcrumb"):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if t and t.lower() not in ("главная", "home"):
                out.append(t)
        if out:
            break
    return out

def stable_cat_id(text: str, prefix: int = 9601000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories(paths: List[List[str]]) -> Tuple[List[Tuple[int, str, int]], Dict[Tuple[str, ...], int]]:
    cat_map: Dict[Tuple[str, ...], int] = {}
    out: List[Tuple[int, str, int]] = []
    ROOT_ID = 9600000
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная", "home", "каталог")]
        pid = ROOT_ID
        prefix: List[str] = []
        for name in clean:
            prefix.append(name)
            key = tuple(prefix)
            if key in cat_map:
                pid = cat_map[key]
                continue
            cid = stable_cat_id(" / ".join(prefix))
            cat_map[key] = cid
            out.append((cid, name, pid))
            pid = cid
    return out, cat_map

def parse_product(sess: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(sess, url)
    if not b:
        return None
    soup = get_soup(b)
    if not page_looks_like_product(soup, url, b):
        return None

    title = find_title(soup)
    price = find_price(soup)
    if not title or price is None:
        return None

    sku = find_sku(soup) or ""
    pic = find_image(soup, url) or ""
    desc = first_text(soup, ["[itemprop=description]", ".product-description", ".tab-content .description", "#description"]) or ""
    crumbs = find_breadcrumbs(soup)

    return {
        "url": url,
        "name": title.strip()[:200],
        "price": price,
        "sku": sku,
        "picture": pic,
        "description": desc,
        "breadcrumbs": crumbs,
    }

# ----------------- YML -----------------
def yml_escape(s: str) -> str:
    return html.escape(s or "")

def write_yml(categories: List[Tuple[int, str, int]], offers: List[Dict[str, Any]]):
    out = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append("<name>vtt</name>")
    out.append("<currencies><currency id=\"RUB\" rate=\"1\" /></currencies>")
    out.append("<categories>")
    out.append("<category id=\"9600000\">VTT</category>")
    for cid, name, parent in categories:
        if not parent:
            parent = 9600000
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{yml_escape(name)}</category>")
    out.append("</categories>")
    out.append("<offers>")
    for o in offers:
        out.append(f"<offer id=\"{yml_escape(o['id'])}\" available=\"true\" in_stock=\"true\">")
        out.append(f"<name>{yml_escape(o['name'])}</name>")
        out.append(f"<vendor>{yml_escape(o.get('vendor','VTT'))}</vendor>")
        out.append(f"<vendorCode>{yml_escape(o.get('vendorCode',''))}</vendorCode>")
        out.append(f"<price>{o.get('price','0')}</price>")
        out.append("<currencyId>RUB</currencyId>")
        out.append(f"<categoryId>{o.get('categoryId',9600000)}</categoryId>")
        if o.get("url"): out.append(f"<url>{yml_escape(o['url'])}</url>")
        if o.get("picture"): out.append(f"<picture>{yml_escape(o['picture'])}</picture>")
        out.append(f"<description>{yml_escape(o.get('description',''))}</description>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write("\n".join(out))
    dlog(f"[done] items: {len(offers)}, cats: {len(categories)} -> {OUT_FILE}")

# ----------------- MAIN -----------------
from datetime import datetime, timedelta

def main() -> int:
    ensure_dirs()
    sess = make_session()

    # 1) ЛОГИН
    if not login_vtt(sess):
        dlog("Error: login failed")
        write_yml([], [])
        return 2

    # 2) Обход каталога
    prod_urls, seen_pages = discover_and_collect(sess, START_URL)

    # 3) Парс карточек
    offers_raw: List[Dict[str, Any]] = []
    saved_samples = 0

    for i, u in enumerate(prod_urls):
        p = parse_product(sess, u)
        if not p:
            continue
        if saved_samples < 5:
            save_debug(f"vtt_page_product_{saved_samples+1}.html", http_get(sess, u) or b"")
            saved_samples += 1
        offers_raw.append(p)

    # 4) Категории по крошкам
    paths = [o["breadcrumbs"] for o in offers_raw if o.get("breadcrumbs")]
    cats, path_map = build_categories(paths)

    # 5) Сбор offers
    offers: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()
    for o in offers_raw:
        cid = 9600000
        if o.get("breadcrumbs"):
            clean = [x for x in o["breadcrumbs"] if x and x.strip() and x.lower() not in ("главная","home","каталог")]
            # найдём самый глубокий известный путь
            # (собирали их в build_categories)
            acc = []
            best = None
            for name in clean:
                acc.append(name)
                t = tuple(acc)
                if t in path_map:
                    best = path_map[t]
            if best:
                cid = best

        offer_id = (o.get("sku") or hashlib.md5(o["url"].encode("utf-8")).hexdigest()[:10]).strip()
        if offer_id in seen_ids:
            offer_id = f"{offer_id}-{hashlib.sha1(o['url'].encode('utf-8')).hexdigest()[:6]}"
        seen_ids.add(offer_id)

        offers.append({
            "id": offer_id,
            "name": o["name"],
            "vendor": "VTT",
            "vendorCode": o.get("sku",""),
            "price": o["price"],
            "categoryId": cid,
            "url": o["url"],
            "picture": o.get("picture",""),
            "description": o.get("description",""),
        })

    write_yml([(cid, name, parent) for cid, name, parent in cats], offers)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        dlog(f"[fatal] {e}")
        try:
            write_yml([], [])
        except Exception:
            pass
        sys.exit(2)
