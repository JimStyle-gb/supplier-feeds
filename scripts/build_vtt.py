# -*- coding: utf-8 -*-
"""
VTT B2B → Satu YML
- Авто-логин через Playwright (логин/пароль из env), cookies переиспользуются.
- Краул категорий и карточек через requests; если видим страницу входа/пусто — фолбэк в Playwright.
- Фильтр по KEYWORDS_FILE (по началу названия). Если файл пуст — берём все позиции.
- Сбор: title, vendorCode (sku), price, url, картинка, описание, хлебные крошки → yml.

Требования: requests, beautifulsoup4, lxml, playwright (и chromium установлен через `playwright install`).
"""

from __future__ import annotations
import os, re, io, time, html, json, hashlib, traceback
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ---------------- ENV ----------------
BASE_URL           = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL          = os.getenv("START_URL", f"{BASE_URL}/catalog")
KEYWORDS_FILE      = os.getenv("KEYWORDS_FILE", "docs/vtt_keywords.txt")
OUT_FILE           = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING    = os.getenv("OUTPUT_ENCODING", "windows-1251")

LOGIN              = os.getenv("B2B_VTT_LOGIN", "")
PASSWORD           = os.getenv("B2B_VTT_PASSWORD", "")

HTTP_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES          = int(os.getenv("MIN_BYTES", "900"))

MAX_WORKERS        = int(os.getenv("MAX_WORKERS", "6"))
MAX_CRAWL_MINUTES  = int(os.getenv("MAX_CRAWL_MINUTES", "120"))
MAX_CATEGORY_PAGES = int(os.getenv("MAX_CATEGORY_PAGES", "2000"))

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"
CURRENCY           = os.getenv("CURRENCY", "RUB").strip() or "RUB"

SUPPLIER_NAME      = "VTT"

ROOT_CAT_ID        = 9600000
ROOT_CAT_NAME      = "VTT"

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-B2B-Scraper/1.0)"}

# ---------------- Utils ----------------
def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms/1000.0))

def yml_escape(s: str) -> str:
    return html.escape(s or "")

def to_number(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).replace("\xa0", " ").strip()
    s = s.replace(" ", "").replace(",", ".")
    if not re.search(r"\d", s):
        return None
    try:
        return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        return float(m.group(0)) if m else None

def abs_url(base: str, href: str) -> str:
    if not href:
        return base
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return BASE_URL + href
    return urljoin(base, href)

def key_norm(v: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", v or "").upper()

def sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def sanitize_title(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:200].rstrip()

def load_keywords(path: str) -> List[str]:
    kws: List[str] = []
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    kws.append(s)
    return kws

def compile_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    pats: List[re.Pattern] = []
    for kw in kws:
        esc = re.escape(kw).replace(r"\ ", " ")
        pats.append(re.compile(r"^\s*" + esc + r"(?!\w)", re.I))
    return pats

def title_startswith(title: str, patterns: List[re.Pattern]) -> bool:
    if not patterns:
        return True  # нет ключей = берем все
    if not title:
        return False
    return any(p.search(title) for p in patterns)

# ---------------- Playwright login ----------------
class BrowserCtx:
    def __init__(self, context, page):
        self.context = context
        self.page = page

def playwright_login() -> Tuple[Optional[BrowserCtx], Optional[str]]:
    """
    Открываем Chromium, идем на START_URL, логинимся, возвращаем cookie header.
    """
    if not LOGIN or not PASSWORD:
        print("[auth] LOGIN/PASSWORD не заданы — логин пропущен.")
        return None, None

    try:
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=UA["User-Agent"], ignore_https_errors=DISABLE_SSL_VERIFY)
        page = context.new_page()

        # 1) на каталог
        page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)

        # 2) если есть форма — логинимся
        def has_login_form() -> bool:
            sel_any = [
                'input[type="password"]',
                'form[action*="login"]',
                'form[action*="signin"]',
                'form[action*="auth"]',
                'text="Вход"', 'text="Авторизация"', 'text="Логин"',
            ]
            for s in sel_any:
                if page.locator(s).first.count() > 0:
                    return True
            # урл явно про логин?
            u = page.url.lower()
            return any(x in u for x in ("/login", "/signin", "/auth"))
        if has_login_form():
            # попытки найти поля
            login_selectors = [
                'input[name="login"]', 'input[name="email"]', 'input[name="username"]',
                'input[type="email"]', 'input[type="text"]'
            ]
            pass_selectors = ['input[name="password"]', 'input[type="password"]']

            filled_login = False
            for s in login_selectors:
                el = page.locator(s).first
                if el.count() > 0:
                    el.fill(LOGIN, timeout=10000)
                    filled_login = True
                    break
            if not filled_login:
                # попробуем просто первый input[type=text]
                el = page.locator('input[type="text"]').first
                if el.count() > 0:
                    el.fill(LOGIN, timeout=10000)

            filled_pass = False
            for s in pass_selectors:
                el = page.locator(s).first
                if el.count() > 0:
                    el.fill(PASSWORD, timeout=10000)
                    filled_pass = True
                    break

            # кнопки «Войти»
            submit_selectors = [
                'button[type="submit"]',
                'button:has-text("Войти")', 'button:has-text("Login")',
                'input[type="submit"]'
            ]
            clicked = False
            for s in submit_selectors:
                el = page.locator(s).first
                if el.count() > 0:
                    el.click(timeout=10000)
                    clicked = True
                    break
            if not clicked:
                # энтер в пасс
                page.keyboard.press("Enter")

            # ждем навигацию / прогруз
            page.wait_for_timeout(1500)
            page.wait_for_load_state("networkidle", timeout=30000)

            # если остались на логине — пробуем альтернативную кнопку
            if has_login_form():
                alt = page.get_by_role("button", name=re.compile("Войти|Login|Sign in", re.I)).first
                if alt.count() > 0:
                    alt.click(timeout=8000)
                    page.wait_for_load_state("networkidle", timeout=30000)

        # 3) проверка, что мы в каталоге
        # если редиректит на логин — считаем неуспех
        cur = page.url.lower()
        if any(x in cur for x in ("/login", "/signin", "/auth")):
            print("[auth] Похоже, остались на странице входа.")
            context.close(); browser.close(); pw.stop()
            return None, None

        # 4) собираем cookie header
        cookies = context.cookies()
        cookie_header = "; ".join([f"{c['name']}={c['value']}" for c in cookies if 'name' in c and 'value' in c])
        print(f"[auth] cookies count: {len(cookies)}")

        return BrowserCtx(context=context, page=page), cookie_header
    except Exception as e:
        print("[auth] Ошибка логина:", e)
        traceback.print_exc()
        try:
            pw.stop()
        except Exception:
            pass
        return None, None

# ---------------- HTTP fetching (requests + fallback to Playwright) ----------------
class Fetcher:
    def __init__(self, cookie_header: Optional[str], browser: Optional[BrowserCtx]):
        self.sess = requests.Session()
        self.sess.headers.update(UA)
        if cookie_header:
            self.sess.headers["Cookie"] = cookie_header
        self.verify = not DISABLE_SSL_VERIFY
        self.browser = browser

    def get(self, url: str) -> Optional[bytes]:
        """Пытаемся requests; если неудачно — Playwright page.content()."""
        try:
            r = self.sess.get(url, timeout=HTTP_TIMEOUT, verify=self.verify, allow_redirects=True)
            if r.status_code == 200 and len(r.content) >= MIN_BYTES:
                # простая эвристика «на странице входа есть password»
                if b"type=\"password\"" not in r.content and b"type='password'" not in r.content:
                    return r.content
        except Exception as e:
            print(f"[http] fail {url}: {e}")

        # fallback -> Playwright
        if self.browser:
            try:
                self.browser.page.goto(url, wait_until="domcontentloaded", timeout=60000)
                self.browser.page.wait_for_timeout(300)
                html = self.browser.page.content()
                if html and len(html.encode("utf-8", "ignore")) >= MIN_BYTES:
                    # если снова логин — не отдаём
                    u = self.browser.page.url.lower()
                    if not any(x in u for x in ("/login", "/signin", "/auth")):
                        return html.encode("utf-8", "ignore")
            except Exception as e:
                print(f"[playwright] fail {url}: {e}")
        return None

# ---------------- Parsers ----------------
PRODUCT_RE = re.compile(r"/(product|goods|item|sku|catalog/[^/]+/[^/]+)\.?\w*$", re.I)

def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "lxml")

def find_category_links(base_url: str, s: BeautifulSoup) -> List[str]:
    out: List[str] = []
    for a in s.find_all("a", href=True):
        href = a["href"]
        u = abs_url(base_url, href)
        if not u.startswith(BASE_URL):
            continue
        # ссылки на разделы каталога
        if "/catalog" in u and not PRODUCT_RE.search(u):
            out.append(u.split("#")[0])
    # Уникализируем, оставляем только того же хоста
    uniq = []
    seen = set()
    for u in out:
        key = (urlparse(u).path, urlparse(u).query)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(u)
    return uniq

def find_product_links(base_url: str, s: BeautifulSoup) -> List[str]:
    out: Set[str] = set()
    for a in s.find_all("a", href=True):
        href = a["href"].strip()
        u = abs_url(base_url, href)
        if not u.startswith(BASE_URL):
            continue
        if PRODUCT_RE.search(u):
            out.add(u.split("#")[0])
    # эвристика на карточки через data-атрибуты
    for a in s.select('a[href][data-product-id], .product a[href], .card a[href]'):
        u = abs_url(base_url, a.get("href"))
        if u.startswith(BASE_URL):
            out.add(u.split("#")[0])
    return list(out)

def category_next_url(s: BeautifulSoup, page_url: str) -> Optional[str]:
    ln = s.find("link", attrs={"rel": "next"})
    if ln and ln.get("href"):
        return abs_url(page_url, ln["href"])
    # текстовые кнопки
    for a in s.find_all("a", href=True):
        txt = (a.get_text(" ", strip=True) or "").lower()
        if txt in ("следующая", "вперед", "вперёд", "next", "далее", ">"):
            return abs_url(page_url, a["href"])
    # param page=
    if "page=" in page_url:
        try:
            import urllib.parse as up
            pr = urlparse(page_url)
            q = dict([kv.split("=") for kv in pr.query.split("&") if "=" in kv])
            p = int(q.get("page", "1")) + 1
            q["page"] = str(p)
            newq = "&".join([f"{k}={v}" for k, v in q.items()])
            return pr._replace(query=newq).geturl()
        except Exception:
            pass
    return None

def parse_price_block(s: BeautifulSoup) -> Optional[float]:
    # Common price selectors
    cand = s.select_one('[itemprop="price"], .price, .product-price, .card-price, [class*="price__value"]')
    if cand:
        p = to_number(cand.get("content") or cand.get_text(" ", strip=True))
        if p is not None:
            return p
    # search any money-like number
    txt = s.get_text(" ", strip=True)
    m = re.search(r"(\d[\d\s.,]{2,})\s*(?:₽|руб|руб\.|RUB)?", txt, flags=re.I)
    if m:
        return to_number(m.group(1))
    return None

def parse_sku(s: BeautifulSoup) -> Optional[str]:
    # microdata
    skuel = s.find(attrs={"itemprop": "sku"})
    if skuel:
        t = (skuel.get_text(" ", strip=True) or "").strip()
        if t:
            return t
    # labels
    labels = ["артикул", "sku", "код", "модель", "part", "pn", "p/n"]
    for lab in labels:
        node = s.find(string=lambda t: t and lab in t.lower())
        if node:
            val = (node.parent.get_text(" ", strip=True) if node.parent else str(node)).strip()
            m = re.search(r"([A-Za-z0-9\-_.\/]{2,})", val)
            if m:
                return m.group(1)
    # JSON-LD
    for script in s.find_all("script", type=lambda v: v and "ld+json" in v):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                if data.get("@type") == "Product":
                    sku = data.get("sku") or data.get("mpn")
                    if sku:
                        return str(sku)
        except Exception:
            pass
    # fallback: any token like model
    txt = s.get_text(" ", strip=True)
    m = re.search(r"(?:Артикул|SKU|Код)\s*[:#]?\s*([A-Za-z0-9\-_.\/]{2,})", txt, flags=re.I)
    if m:
        return m.group(1)
    return None

def parse_title(s: BeautifulSoup) -> Optional[str]:
    for sel in ['h1', 'h1[itemprop="name"]', '[itemprop="name"]', '.product-title', '.card-title']:
        el = s.select_one(sel)
        if el and el.get_text(strip=True):
            return sanitize_title(el.get_text(" ", strip=True))
    title = s.title.string if s.title else None
    if title:
        return sanitize_title(title)
    return None

def parse_description(s: BeautifulSoup) -> Optional[str]:
    sels = [
        '[itemprop="description"]', '.product-description', '.desc', '#description',
        '.tab-content .description', '.tabs .description', '.product__description'
    ]
    for sel in sels:
        el = s.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)
    # JSON-LD
    for script in s.find_all("script", type=lambda v: v and "ld+json" in v):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                d = data.get("description")
                if d:
                    return str(d)
        except Exception:
            pass
    return None

def parse_picture(s: BeautifulSoup, page_url: str) -> Optional[str]:
    cand = s.find("img", {"id": re.compile(r"^main", re.I)})
    if cand and (cand.get("src") or cand.get("data-src")):
        return abs_url(page_url, cand.get("src") or cand.get("data-src"))
    og = s.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return abs_url(page_url, og["content"])
    for img in s.find_all("img"):
        src = img.get("src") or img.get("data-src")
        if not src:
            continue
        if any(k in src for k in ("product", "goods", "catalog", "/img/", "/images/")):
            return abs_url(page_url, src)
    return None

def parse_breadcrumbs(s: BeautifulSoup) -> List[str]:
    names: List[str] = []
    for bc in s.select('ul.breadcrumb, .breadcrumbs, .breadcrumb, .pathway, [class*="breadcrumb"], [class*="pathway"]'):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if t and t.lower() not in ("главная","home","каталог"):
                names.append(t.strip())
        if names:
            break
    return names

# ---------------- YML ----------------
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

def build_yml(categories: List[Tuple[int,str,Optional[int]]], offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SUPPLIER_NAME.lower())}</name>")
    out.append(f'<currencies><currency id="{CURRENCY}" rate="1" /></currencies>')

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
            f"<currencyId>{CURRENCY}</currencyId>",
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

# ---------------- Crawl logic ----------------
def main() -> int:
    started = datetime.utcnow()
    deadline = started + timedelta(minutes=MAX_CRAWL_MINUTES)

    # 0) логин
    browser_ctx, cookie_header = playwright_login()
    fetcher = Fetcher(cookie_header=cookie_header, browser=browser_ctx)

    # 1) стартовая
    b = fetcher.get(START_URL)
    if not b:
        raise RuntimeError("Стартовая страница недоступна после логина.")
    s = soup_of(b)

    # собираем верхние разделы
    cats_lvl1 = find_category_links(START_URL, s)
    if not cats_lvl1:
        print("[warn] Не нашли верхних разделов, попробуем всё же пройтись по странице как по листингу.")
        cats_lvl1 = [START_URL]

    print(f"[cats] top-level: {len(cats_lvl1)}")

    # 2) BFS по категориям + сбор product urls
    product_urls: Set[str] = set()
    seen_cats: Set[str] = set()
    queue: List[str] = list(dict.fromkeys(cats_lvl1))
    pages_scanned = 0

    while queue and datetime.utcnow() < deadline and pages_scanned < MAX_CATEGORY_PAGES:
        cat = queue.pop(0)
        if cat in seen_cats:
            continue
        seen_cats.add(cat)

        jitter_sleep(REQUEST_DELAY_MS)
        b = fetcher.get(cat)
        if not b:
            continue
        s = soup_of(b)

        # подкатегории
        subcats = find_category_links(cat, s)
        for sc in subcats:
            if sc not in seen_cats:
                queue.append(sc)

        # товары на странице + пагинация
        urls_here = find_product_links(cat, s)
        product_urls.update(urls_here)

        nxt = category_next_url(s, cat)
        if nxt and nxt not in seen_cats:
            queue.append(nxt)

        pages_scanned += 1
        if pages_scanned % 50 == 0:
            print(f"[scan] categories/pages visited: {pages_scanned}, products seen: {len(product_urls)}")

    print(f"[crawl] products collected: {len(product_urls)}")

    # 3) ключи (startswith)
    kws = load_keywords(KEYWORDS_FILE)
    patterns = compile_startswith_patterns(kws) if kws else []

    # 4) парсим карточки
    def parse_product(u: str) -> Optional[Dict[str, Any]]:
        if datetime.utcnow() > deadline:
            return None
        try:
            jitter_sleep(REQUEST_DELAY_MS)
            b = fetcher.get(u)
            if not b:
                return None
            ss = soup_of(b)

            title = parse_title(ss)
            if not title:
                return None
            title = sanitize_title(title)
            if not title_startswith(title, patterns):
                return None

            price = parse_price_block(ss)
            if price is None or price <= 0:
                return None

            sku = parse_sku(ss)
            if not sku:
                # без SKU сату не любит — сгенерируем, но лучше иметь реальный
                sku = "SKU-" + sha1(u)[:10]

            pic = parse_picture(ss, u)
            desc = parse_description(ss) or title
            crumbs = parse_breadcrumbs(ss)

            brand = None
            m = re.search(r"(?:Бренд|Производитель)\s*[:#]?\s*([A-Za-zА-Яа-я0-9\- ]+)", ss.get_text(" ", strip=True), flags=re.I)
            if m:
                brand = m.group(1).strip()

            return {
                "url": u,
                "title": title,
                "price": float(f"{price:.2f}"),
                "vendorCode": sku,
                "picture": pic,
                "description": desc,
                "crumbs": crumbs,
                "brand": brand or SUPPLIER_NAME
            }
        except Exception as e:
            print(f"[product] fail {u}: {e}")
            return None

    items: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = { ex.submit(parse_product, u): u for u in list(product_urls) }
        for fut in as_completed(futures):
            it = fut.result()
            if it:
                items.append(it)
            if len(items) % 50 == 0 and len(items) > 0:
                print(f"[prod] parsed: {len(items)} / {len(product_urls)}")

    print(f"[prod] total parsed: {len(items)}")

    # 5) категории из крошек
    paths = [it.get("crumbs") for it in items if it.get("crumbs")]
    cat_list, path_id_map = build_categories_from_paths(paths)

    # 6) маппинг items → cat id
    offers: List[Tuple[int, Dict[str, Any]]] = []
    seen_offer_ids: Set[str] = set()
    for it in items:
        cid = ROOT_CAT_ID
        crumbs = it.get("crumbs") or []
        clean = [p.strip() for p in crumbs if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная","home","каталог")]
        key = tuple(clean)
        while key and key not in path_id_map:
            key = key[:-1]
        if key and key in path_id_map:
            cid = path_id_map[key]

        offer_id = it["vendorCode"]
        if offer_id in seen_offer_ids:
            offer_id = f"{offer_id}-{sha1(it['title'])[:6]}"
        seen_offer_ids.add(offer_id)

        offers.append((cid, {
            "offer_id":   offer_id,
            "title":      it["title"],
            "price":      it["price"],
            "vendorCode": it["vendorCode"],
            "brand":      it.get("brand") or SUPPLIER_NAME,
            "url":        it.get("url"),
            "picture":    it.get("picture"),
            "description": it.get("description") or it["title"],
        }))

    # 7) YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(cat_list, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    # 8) закрываем браузер
    try:
        if browser_ctx:
            browser_ctx.context.close()
            browser_ctx.page.context.browser.close()
    except Exception:
        pass

    print(f"[done] items: {len(offers)}, categories: {len(cat_list)} -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        sys.exit(2)
