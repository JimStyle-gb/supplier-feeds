# -*- coding: utf-8 -*-
"""
VTT (b2b.vtt.ru) → YML

Особенности:
- Авторизация через готовые куки (секрет VTT_COOKIES). Поддержка XSRF-TOKEN.
- Устойчивый краулер каталога (категории → товары → карточки).
- Фильтр startswith по KEYWORDS_FILE (если пуст — берём все товары).
- Широкий набор селекторов для имени/цены/SKU/картинки/крошек.
- Бережный rate-limit и бюджет по времени/страницам.

ENV:
  BASE_URL, START_URL, KEYWORDS_FILE, OUT_FILE, OUTPUT_ENCODING
  VTT_COOKIES
  HTTP_TIMEOUT, REQUEST_DELAY_MS, MIN_BYTES
  MAX_WORKERS, MAX_CRAWL_MINUTES, MAX_CATEGORY_PAGES
  DISABLE_SSL_VERIFY (0/1)
"""

from __future__ import annotations
import os, re, io, time, html, hashlib
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ---------- ENV ----------
BASE_URL         = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL        = os.getenv("START_URL", f"{BASE_URL}/catalog/")
KEYWORDS_FILE    = os.getenv("KEYWORDS_FILE", "docs/vtt_keywords.txt")
OUT_FILE         = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_COOKIES      = os.getenv("VTT_COOKIES", "").strip()

HTTP_TIMEOUT     = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "900"))

MAX_WORKERS        = int(os.getenv("MAX_WORKERS", "6"))
MAX_CRAWL_MINUTES  = int(os.getenv("MAX_CRAWL_MINUTES", "90"))
MAX_CATEGORY_PAGES = int(os.getenv("MAX_CATEGORY_PAGES", "1500"))

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0").strip() == "1"

SUPPLIER_NAME = "VTT"
CURRENCY      = "RUB"

ROOT_CAT_ID   = 9600000
ROOT_CAT_NAME = "VTT"

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) VTTFeedBot/1.2",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}

# ---------- helpers ----------
def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms / 1000.0))

def yml_escape(s: str) -> str:
    return html.escape((s or "").strip())

def sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def to_number(s: Any) -> Optional[float]:
    if s is None: return None
    t = str(s).replace("\xa0", " ").strip()
    t = t.replace(" ", "").replace(",", ".")
    if not re.search(r"\d", t): return None
    try:
        return float(t)
    except Exception:
        m = re.search(r"([\d.]+)", t)
        return float(m.group(1)) if m else None

def sanitize_title(s: str) -> str:
    s = re.sub(r"\s{2,}", " ", (s or "").strip())
    return s[:200].rstrip()

def key_norm(v: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", v or "").upper()

# ---------- session ----------
def build_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(UA)
    if VTT_COOKIES:
        sess.headers["Cookie"] = VTT_COOKIES
        m = re.search(r"XSRF-TOKEN=([^;]+)", VTT_COOKIES)
        if m:
            try:
                sess.headers["X-XSRF-TOKEN"] = unquote(m.group(1))
            except Exception:
                sess.headers["X-XSRF-TOKEN"] = m.group(1)
        print("[auth] cookies injected.")
    else:
        print("[warn] VTT_COOKIES is empty.")
    if DISABLE_SSL_VERIFY:
        sess.verify = False
        print("[ssl] Verification disabled (DISABLE_SSL_VERIFY=1).")
    return sess

def http_get(sess: requests.Session, url: str) -> Optional[bytes]:
    for attempt in range(3):
        try:
            r = sess.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code != 200:
                jitter_sleep(200)
                continue
            b = r.content
            if len(b) < MIN_BYTES:
                jitter_sleep(200)
                continue
            # простая проверка на страницу логина
            txt = b.decode(errors="ignore").lower()
            if ("type=\"password\"" in txt or "name=\"password\"" in txt) and "/login" in txt:
                print("[auth] login page detected while GET", url)
                return None
            return b
        except Exception as e:
            if attempt == 2:
                print(f"[http] fail {url}: {e}")
                return None
            jitter_sleep(300)
    return None

def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "html.parser")

# ---------- keywords (startswith) ----------
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
    out = []
    for kw in kws:
        esc = re.escape(kw).replace(r"\ ", " ")
        out.append(re.compile(r"^\s*" + esc + r"(?!\w)", re.IGNORECASE))
    return out

def title_startswith(title: str, patterns: List[re.Pattern]) -> bool:
    if not patterns:
        return True  # нет ключей — берём всё
    return any(p.search(title or "") for p in patterns)

# ---------- category & product discovery ----------
CAT_ALLOW = ("/catalog/", )

def is_catalog_link(href: str) -> bool:
    if not href: return False
    u = href.strip()
    if u.startswith("//"): u = "https:" + u
    if u.startswith("/"):  u = BASE_URL + u
    if not u.startswith(BASE_URL): return False
    return any(p in u for p in CAT_ALLOW)

PRODUCT_PATTERNS = [
    re.compile(r"/catalog/[^/]+/[^/?#]+/?$"),           # /catalog/<group>/<slug>
    re.compile(r"/product/[^/?#]+/?$"),                 # /product/<slug>
    re.compile(r"/catalog/[^/?#]+\.html$"),             # иногда .html
]

def looks_like_product_url(u: str) -> bool:
    for rx in PRODUCT_PATTERNS:
        if rx.search(u):
            return True
    return False

def find_next_page(s: BeautifulSoup, base_url: str) -> Optional[str]:
    ln = s.find("link", href=True, rel=lambda v: v and "next" in (v if isinstance(v, list) else [v]))
    if ln:
        return urljoin(base_url, ln["href"])
    for a in s.find_all("a", href=True):
        t = (a.get_text(" ", strip=True) or "").lower()
        if t in ("следующая", "вперед", "вперёд", "next", ">"):
            return urljoin(base_url, a["href"])
    return None

def collect_category_urls(sess: requests.Session, start_url: str) -> List[str]:
    b = http_get(sess, start_url)
    if not b:
        raise RuntimeError("Стартовая страница недоступна (проверьте VTT_COOKIES и START_URL).")
    s = soup_of(b)

    urls: List[str] = []
    seen: Set[str] = set()

    # базовая выборка ссылок каталога
    for a in s.find_all("a", href=True):
        absu = urljoin(start_url, a["href"])
        if is_catalog_link(absu):
            if absu not in seen:
                seen.add(absu); urls.append(absu)

    # подстраховка: иногда корень каталога = собственная страница
    if start_url not in seen:
        urls.append(start_url)

    urls = list(dict.fromkeys(urls))
    if not urls:
        raise RuntimeError("Не нашли разделов каталога (скорее всего, cookies устарели или доступа нет).")

    print(f"[cats] found on start: {len(urls)}")
    return urls

def collect_product_urls_from_category(sess: requests.Session, cat_url: str, limit_pages: int) -> List[str]:
    page = cat_url
    seen_pages: Set[str] = set()
    urls: List[str] = []
    pages_done = 0

    while page and pages_done < limit_pages:
        if page in seen_pages: break
        seen_pages.add(page)

        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, page)
        if not b: break
        s = soup_of(b)

        # 1) карточки по типичным контейнерам
        for card in s.select('[data-product-id], .product, .catalog__item, .product-card, .product_item, .product-card__info'):
            a = card.find("a", href=True)
            if not a: continue
            u = urljoin(page, a["href"])
            if looks_like_product_url(u):
                urls.append(u)

        # 2) все ссылки /catalog/... как запасной вариант
        for a in s.find_all("a", href=True):
            u = urljoin(page, a["href"])
            if looks_like_product_url(u):
                urls.append(u)

        # пагинация
        page = find_next_page(s, page)
        pages_done += 1

    return list(dict.fromkeys(urls))

# ---------- product parse ----------
def extract_breadcrumbs(s: BeautifulSoup) -> List[str]:
    names: List[str] = []
    for bc in s.select('nav.breadcrumbs, ul.breadcrumb, .breadcrumbs, .breadcrumb, [class*="breadcrumb"]'):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if not t: continue
            tl = t.lower()
            if tl in ("главная", "home", "каталог"): continue
            names.append(t.strip())
        if names: break
    return names

def extract_text_by_labels(s: BeautifulSoup, labels: List[str]) -> Optional[str]:
    txt = s.get_text(" ", strip=True)
    m = re.search(r"(?:%s)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})" %
                  "|".join(labels), txt, flags=re.IGNORECASE)
    return m.group(1) if m else None

def parse_product_page(sess: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(sess, url)
    if not b: return None
    s = soup_of(b)

    # title
    title = None
    for sel in [
        "h1[itemprop=name]", "h1.product-title", "h1.title", "h1",
        ".product__title", ".card__title", "[itemprop='name']",
        'meta[property="og:title"]',
    ]:
        el = s.select_one(sel)
        if el:
            title = (el.get("content") or el.get_text(" ", strip=True) if hasattr(el, "get_text") else None) or el.get("content")
            if title: break
    title = sanitize_title(title or "")

    # price
    price = None
    candidates = []
    for sel in [
        '[itemprop="price"]', '.price__current', '.product-price__current', '.product-price', '.price', 'meta[itemprop="price"]',
        'meta[property="product:price:amount"]', 'meta[property="og:price:amount"]'
    ]:
        for el in s.select(sel):
            v = el.get("content") if el.has_attr("content") else el.get_text(" ", strip=True)
            if v: candidates.append(v)
    for v in candidates:
        p = to_number(v)
        if p and p > 0:
            price = p; break

    # sku
    sku = None
    # частые варианты
    for lab in ["Артикул", "SKU", "Код товара", "Код", "Модель"]:
        node = s.find(string=lambda t: t and lab.lower() in t.lower())
        if node:
            val = (node.parent.get_text(" ", strip=True) if node.parent else str(node)).strip()
            m = re.search(r"([A-Za-z0-9\-\._/]{2,})", val)
            if m: sku = m.group(1); break
    if not sku:
        sku = extract_text_by_labels(s, ["Артикул", "SKU", "Код товара", "Код", "Модель"])

    # image
    picture = None
    og = s.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        picture = urljoin(url, og["content"])
    if not picture:
        imgel = s.find("img", attrs={"itemprop": "image"}) or s.find("img", class_=re.compile(r"product", re.I))
        if imgel and (imgel.get("src") or imgel.get("data-src")):
            picture = urljoin(url, imgel.get("src") or imgel.get("data-src"))

    # description
    desc = None
    for sel in [
        '[itemprop="description"]', ".product-description", ".description", "#description",
        ".product__description", ".tabs__content .description"
    ]:
        el = s.select_one(sel)
        if el and el.get_text(strip=True):
            desc = el.get_text(" ", strip=True); break
    if not desc:
        txt = s.get_text(" ", strip=True)
        desc = txt if len(txt) > 80 else title

    # breadcrumbs
    crumbs = extract_breadcrumbs(s)

    # sanity
    if not title or not sku or not price or not picture:
        return None

    return {
        "url": url,
        "title": title,
        "price": float(price),
        "sku": sku,
        "picture": picture,
        "description": desc or title,
        "crumbs": crumbs,
    }

# ---------- categories build ----------
def stable_cat_id(text: str, prefix: int = 9700000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories_from_paths(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
    cat_map: Dict[Tuple[str,...], int] = {}
    out_list: List[Tuple[int,str,Optional[int]]] = []
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная","home","каталог")]
        if not clean: continue
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

# ---------- YML ----------
def build_yml(categories: List[Tuple[int,str,Optional[int]]], offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SUPPLIER_NAME.lower())}</name>")
    out.append(f'<currencies><currency id="{CURRENCY}" rate="1" /></currencies>')

    out.append("<categories>")
    out.append(f'<category id="{ROOT_CAT_ID}">{yml_escape(ROOT_CAT_NAME)}</category>')
    for cid, name, parent in categories:
        parent = parent if parent else ROOT_CAT_ID
        out.append(f'<category id="{cid}" parentId="{parent}">{yml_escape(name)}</category>')
    out.append("</categories>")

    out.append("<offers>")
    for cid, it in offers:
        price_txt = str(int(it["price"])) if float(it["price"]).is_integer() else f'{it["price"]}'
        out += [
            f'<offer id="{yml_escape(it["offer_id"])}" available="true" in_stock="true">',
            f'<name>{yml_escape(it["title"])}</name>',
            f'<vendor>{yml_escape(SUPPLIER_NAME)}</vendor>',
            f'<vendorCode>{yml_escape(it["vendorCode"])}</vendorCode>',
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

# ---------- MAIN ----------
def main() -> int:
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MINUTES)
    sess = build_session()

    # 1) собираем категории со стартовой
    try:
        categories_roots = collect_category_urls(sess, START_URL)
    except Exception as e:
        print("Error:", e)
        return 2

    # 2) из категорий собираем ссылки на товары (с пагинацией)
    total_cat_pages_budget = MAX_CATEGORY_PAGES
    per_cat_budget = max(1, total_cat_pages_budget // max(1, len(categories_roots)))

    product_urls: List[str] = []
    for cu in categories_roots:
        if datetime.utcnow() > deadline: break
        urls = collect_product_urls_from_category(sess, cu, per_cat_budget)
        product_urls.extend(urls)

    product_urls = list(dict.fromkeys(product_urls))
    print(f"[crawl] total product urls: {len(product_urls)}")
    if not product_urls:
        print("[error] Не нашли карточек товаров. Возможно, куки устарели или селекторы отличаются.")
        write_empty_yml()
        return 2

    # 3) парсим карточки
    items: List[Dict[str, Any]] = []
    def worker(u: str):
        if datetime.utcnow() > deadline: return None
        try:
            return parse_product_page(sess, u)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(worker, u): u for u in product_urls}
        for fut in as_completed(futures):
            if datetime.utcnow() > deadline: break
            rec = fut.result()
            if not rec: continue
            items.append(rec)
            if len(items) % 50 == 0:
                print(f"[parse] items parsed: {len(items)}")

    if not items:
        print("[error] Парсинг карточек не дал результатов.")
        write_empty_yml()
        return 2

    # 4) фильтр по ключам (startswith)
    kw_list = load_keywords(KEYWORDS_FILE)
    pats = compile_startswith_patterns(kw_list)
    filtered = [x for x in items if title_startswith(x["title"], pats)]
    if not filtered:
        print("[warn] После фильтра по startswith нет позиций — сохраняем пустой YML (проверь KEYWORDS_FILE).")
        write_empty_yml()
        return 0

    # 5) строим категории из крошек
    all_paths = [x.get("crumbs") for x in filtered if x.get("crumbs")]
    cat_list, path_id_map = build_categories_from_paths(all_paths)
    print(f"[cats] built: {len(cat_list)}")

    # 6) готовим офферы
    offers: List[Tuple[int, Dict[str, Any]]] = []
    seen_offer_ids: Set[str] = set()

    for it in filtered:
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

        offer_id = it["sku"]
        if offer_id in seen_offer_ids:
            offer_id = f'{it["sku"]}-{sha1(it["title"])[:6]}'
        seen_offer_ids.add(offer_id)

        offers.append((cid, {
            "offer_id":   offer_id,
            "title":      it["title"],
            "price":      it["price"],
            "vendorCode": it["sku"],
            "url":        it["url"],
            "picture":    it["picture"],
            "description": it["description"],
        }))

    # 7) сохраняем YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(cat_list, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, categories: {len(cat_list)} -> {OUT_FILE}")
    return 0

def write_empty_yml():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    empty = build_yml([], [])
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(empty)

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        write_empty_yml()
        sys.exit(2)
