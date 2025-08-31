# -*- coding: utf-8 -*-
"""
VTT (b2b.vtt.ru) → YML для Satu.
Особенности:
- Если файл ключей пустой или содержит '*' — берём ВСЕ товары (без фильтра по названиям).
- Если ключи заданы — фильтруем по НАЧАЛУ названия (startswith) и, при нуле результатов,
  автоматически ослабляем до 'contains' (с логом-предупреждением).
- Авторизация: через cookie-строку (env VTT_COOKIES). Если нет — пробуем публично.
- Категории: собираем с /catalog, затем пагинация; карточки ищем по общим селекторам.
- Описание/фото/sku: несколько надёжных стратегий (og:image, itemprop, очевидные классы).
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
BASE_URL        = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL       = os.getenv("START_URL", f"{BASE_URL}/catalog").rstrip("/")
KEYWORDS_FILE   = os.getenv("KEYWORDS_FILE", "docs/vtt_keywords.txt")
OUT_FILE        = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_COOKIES     = os.getenv("VTT_COOKIES", "").strip()

HTTP_TIMEOUT     = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "400"))

MAX_WORKERS       = int(os.getenv("MAX_WORKERS", "6"))
MAX_CRAWL_MINUTES = int(os.getenv("MAX_CRAWL_MINUTES", "90"))
MAX_CATEGORY_PAGES= int(os.getenv("MAX_CATEGORY_PAGES", "1500"))

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"

SUPPLIER_NAME = "vtt"
CURRENCY      = "RUB"
ROOT_CAT_ID   = 9600000
ROOT_CAT_NAME = "VTT"

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VTT-FeedBot/2.0; +https://github.com/JimStyle-gb/supplier-feeds)"
}

if DISABLE_SSL_VERIFY:
    print("[ssl] Verification disabled (DISABLE_SSL_VERIFY=1).")

# ---------- utils ----------
def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms/1000.0))

def normalize_url(u: str, base: str) -> str:
    if not u: return ""
    if u.startswith("//"): u = ("https:" if base.startswith("https") else "http:") + u
    if u.startswith("/"):  u = urljoin(base, u)
    return u

def yml_escape(s: str) -> str: return html.escape(s or "")

def sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def to_number(x: Any) -> Optional[float]:
    if x is None: return None
    s = str(x).replace("\xa0"," ").replace(" ", "").replace(",", ".").strip()
    if not re.search(r"\d", s): return None
    try: return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        return float(m.group(0)) if m else None

def sanitize_title(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:200].rstrip()

def stable_cat_id(text: str, prefix: int = 9700000) -> int:
    import hashlib
    h = hashlib.md5((text or "").encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

# ---------- keywords ----------
def load_keywords(path: str) -> List[str]:
    kws: List[str] = []
    try:
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    kws.append(s)
    except Exception:
        pass
    # '*' означает "без фильтра, взять всё"
    if not kws or any(k.strip() == "*" for k in kws):
        print("[kws] empty or '*' → mode=ALL (no title filter).")
        return []
    print(f"[kws] {len(kws)} keyword(s) loaded.")
    return kws

def compile_patterns_startswith(kws: List[str]) -> List[re.Pattern]:
    return [re.compile(r"^\s*" + re.escape(kw).replace(r"\ ", " ") + r"(?!\w)", re.I) for kw in kws]

def compile_patterns_contains(kws: List[str]) -> List[re.Pattern]:
    return [re.compile(re.escape(kw).replace(r"\ ", " "), re.I) for kw in kws]

# ---------- HTTP/session ----------
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA_HEADERS)

    if VTT_COOKIES:
        # Примем строку "key=value; key2=value2; ..."
        for pair in re.split(r";\s*", VTT_COOKIES):
            if not pair or "=" not in pair: continue
            k, v = pair.split("=", 1)
            s.cookies.set(k.strip(), v.strip(), domain=urlparse(BASE_URL).hostname)
        print("[auth] cookies injected from VTT_COOKIES.")
    return s

def http_get(session: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT, verify=not DISABLE_SSL_VERIFY)
        if r.status_code != 200:
            print(f"[http] {r.status_code} {url}")
            return None
        b = r.content
        if len(b) < MIN_BYTES:
            print(f"[http] too small ({len(b)} bytes) {url}")
            return None
        return b
    except Exception as e:
        print(f"[http] fail {url}: {e}")
        return None

def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "lxml")

# ---------- catalog discovery ----------
def discover_category_links(session: requests.Session, start_url: str) -> List[str]:
    b = http_get(session, start_url)
    if not b:
        print("Error: Стартовая страница недоступна (проверьте VTT_COOKIES и URL).")
        return []

    s = soup_of(b)

    # На закрытых порталах часто отдают страницу входа
    if s.find(string=re.compile(r"вход|логин|login", re.I)) and not VTT_COOKIES:
        print("[auth] login page detected while listing categories (no cookies).")
        return []

    links: List[str] = []
    for a in s.select('a[href]'):
        href = a.get("href") or ""
        text = (a.get_text(" ", strip=True) or "").lower()
        u = normalize_url(href, start_url)
        # Берём только то, что выглядит как разделы каталога
        if urlparse(u).netloc != urlparse(BASE_URL).netloc:
            continue
        if "/catalog" not in u:
            continue
        if any(x in u for x in ["?PAGEN_", "sort=", "#"]):
            # не фильтруем слишком сильно — пагинацию будем брать на уровне категорий
            pass
        links.append(u)

    # Уникализируем и оставляем только «корневые/подразделы», а не карточки
    uniq = []
    seen = set()
    for u in links:
        if u in seen: continue
        seen.add(u)
        uniq.append(u)

    print(f"[cats] found on start: {len(uniq)}")
    return uniq

PRODUCT_HINTS = ("add-to-cart", "buy", "В корзину", "product", "товар", "артикул", "sku")

def detect_product_links(session: requests.Session, cat_url: str, limit_pages: int) -> List[str]:
    urls: List[str] = []
    seen_pages: Set[str] = set()
    page = cat_url
    pages = 0

    def next_link(s: BeautifulSoup, base: str) -> Optional[str]:
        # rel=next, «следующая», пагинация по классам
        el = s.find("link", rel="next")
        if el and el.get("href"):
            return normalize_url(el["href"], base)
        for a in s.select("a[href]"):
            t = (a.get_text(" ", strip=True) or "").lower()
            if t in ("следующая", "вперёд", "next", ">") and a.get("href"):
                return normalize_url(a["href"], base)
        return None

    while page and pages < limit_pages and page not in seen_pages:
        seen_pages.add(page)
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(session, page)
        if not b: break
        s = soup_of(b)

        # эвристика: карточки чаще всего ведут глубже, а в ссылке/окружении есть «товарные» признаки
        for a in s.select("a[href]"):
            href = a.get("href") or ""
            u = normalize_url(href, page)
            if urlparse(u).netloc != urlparse(BASE_URL).netloc:
                continue
            # отфильтруем явные не карточки
            if "/catalog" not in u:
                continue
            # часто товары заканчиваются на /<slug>/, а категории — длинные списки, но точной границы нет
            text = a.get_text(" ", strip=True) or ""
            near = (a.parent.get_text(" ", strip=True) if a.parent else "") or ""
            blob = (text + " " + near).lower()
            looks_like_product = any(h.lower() in blob for h in PRODUCT_HINTS)
            # ещё подсказка: короткие пути часто — детальные страницы
            path = urlparse(u).path.rstrip("/")
            depth = len([p for p in path.split("/") if p])
            if looks_like_product or depth >= 3:
                urls.append(u)

        page = next_link(s, page)
        pages += 1

    urls = list(dict.fromkeys(urls))
    print(f"[crawl] {cat_url} → product urls: {len(urls)} (pages walked: {pages})")
    return urls

# ---------- product parsing ----------
def extract_text(s: BeautifulSoup, selectors: List[str]) -> Optional[str]:
    for sel in selectors:
        el = s.select_one(sel)
        if el:
            t = el.get_text(" ", strip=True)
            if t: return t
    return None

def extract_picture(s: BeautifulSoup, base: str) -> Optional[str]:
    # 1) og:image
    og = s.find("meta", property="og:image")
    if og and og.get("content"):
        return normalize_url(og["content"], base)
    # 2) любые img в «галлерее»
    for sel in ["img.product__image", ".product-gallery img", ".swiper img", "img"]:
        for im in s.select(sel):
            src = im.get("src") or im.get("data-src") or ""
            if src:
                return normalize_url(src, base)
    return None

def parse_product(session: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(session, url)
    if not b: return None
    s = soup_of(b)

    title = extract_text(s, ["h1", "[itemprop='name']", ".product-title", ".product__title"]) or ""
    title = sanitize_title(title)

    # price
    price: Optional[float] = None
    # JSON-LD ловим, если есть
    for el in s.find_all("script", type="application/ld+json"):
        try:
            import json
            data = json.loads(el.string or "")
            if isinstance(data, dict):
                offers = data.get("offers")
                if isinstance(offers, dict) and offers.get("price"):
                    price = to_number(offers["price"])
                    break
        except Exception:
            pass
    if price is None:
        # явные селекторы
        t = extract_text(s, [".price", ".product-price", "[itemprop='price']", ".card-price"])
        price = to_number(t)

    # sku / vendor code
    sku = extract_text(s, [".sku", ".product-article", "[itemprop='sku']", ".product__sku", ".vendor-code"])
    if not sku:
        m = re.search(r"(?:Артикул|SKU|Код)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})", s.get_text(" ", strip=True), re.I)
        if m: sku = m.group(1)

    picture = extract_picture(s, url)
    desc = extract_text(s, ["[itemprop='description']", ".product-description", ".product__description", "#description"]) or ""

    if not title or not price or not picture:
        return None

    # хлебные крошки для категорий
    crumbs: List[str] = []
    for bc in s.select(".breadcrumbs, .breadcrumb, nav[aria-label*='breadcrumb']"):
        for a in bc.select("a"):
            t = a.get_text(" ", strip=True)
            if t and t.lower() not in ("главная","home","каталог"):
                crumbs.append(t)
        if crumbs:
            break

    return {
        "url": url,
        "title": title,
        "price": float(f"{price:.2f}"),
        "vendorCode": sku or "",
        "picture": picture,
        "description": desc or title,
        "crumbs": crumbs,
    }

# ---------- YML ----------
def build_categories(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
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
        out.append(f"<category id=\"{cid}\" parentId=\"{parent or ROOT_CAT_ID}\">{yml_escape(name)}</category>")
    out.append("</categories>")
    out.append("<offers>")
    for cid, it in offers:
        price_txt = str(int(it["price"])) if float(it["price"]).is_integer() else f"{it['price']}"
        out += [
            f"<offer id=\"{yml_escape(it.get('vendorCode') or sha1(it['title']))}\" available=\"true\" in_stock=\"true\">",
            f"<name>{yml_escape(it['title'])}</name>",
            f"<vendor>{yml_escape(SUPPLIER_NAME)}</vendor>",
            f"<vendorCode>{yml_escape(it.get('vendorCode') or '')}</vendorCode>",
            f"<price>{price_txt}</price>",
            "<currencyId>RUB</currencyId>",
            f"<categoryId>{cid}</categoryId>",
            f"<url>{yml_escape(it['url'])}</url>",
            f"<picture>{yml_escape(it['picture'])}</picture>",
            f"<description>{yml_escape(it.get('description') or it['title'])}</description>",
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
    session = build_session()
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MINUTES)

    # 0) ключи
    kws = load_keywords(KEYWORDS_FILE)
    filter_mode = "ALL" if not kws else "STARTSWITH"
    pats_sw = compile_patterns_startswith(kws) if kws else []
    pats_ct = compile_patterns_contains(kws) if kws else []

    # 1) категории
    cats = discover_category_links(session, START_URL)
    if not cats:
        print("Error: Не нашли разделов каталога.")
        write_empty_yml()
        return 2

    # 2) собираем ссылки карточек
    product_urls: List[str] = []
    per_cat = max(1, MAX_CATEGORY_PAGES // max(1, len(cats)))
    for cu in cats:
        product_urls += detect_product_links(session, cu, per_cat)
    product_urls = list(dict.fromkeys(product_urls))
    print(f"[crawl] total product urls: {len(product_urls)}")

    if not product_urls:
        print("Error: Ноль карточек после обхода категорий.")
        write_empty_yml()
        return 2

    # 3) парсим карточки
    items: List[Dict[str,Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(parse_product, session, u): u for u in product_urls}
        for fut in as_completed(futures):
            if datetime.utcnow() > deadline:
                print("[time] crawl budget exceeded.")
                break
            rec = fut.result()
            if rec: items.append(rec)

    print(f"[parse] parsed items: {len(items)}")

    if not items:
        print("Error: Не удалось распарсить ни одной карточки (возможно, нужна авторизация/cookies).")
        write_empty_yml()
        return 2

    # 4) фильтрация по ключам (если заданы)
    def ok_title(title: str) -> bool:
        if filter_mode == "ALL": return True
        if any(p.search(title) for p in pats_sw): return True
        return False

    filtered = [it for it in items if ok_title(it["title"])]
    print(f"[filter:{filter_mode}] kept: {len(filtered)} / {len(items)}")

    # fallback: если по startswith получился 0 — попробуем contains
    if kws and not filtered:
        print("[filter] startswith=0 → fallback to CONTAINS.")
        filtered = [it for it in items if any(p.search(it["title"]) for p in pats_ct)]
        print(f"[filter:CONTAINS] kept: {len(filtered)} / {len(items)}")

    if not filtered:
        print("[warn] после фильтра нет товаров → выгружаем пустой YML с категориями.")
        write_empty_yml()
        return 2

    # 5) категории по крошкам
    paths = [it["crumbs"] for it in filtered if it.get("crumbs")]
    cat_list, path_id_map = build_categories(paths)
    print(f"[cats] built: {len(cat_list)}")

    # 6) назначение categoryId
    offers: List[Tuple[int, Dict[str,Any]]] = []
    for it in filtered:
        cid = ROOT_CAT_ID
        if it.get("crumbs"):
            clean = [p for p in it["crumbs"] if p and p.strip() and p.lower() not in ("главная","home","каталог")]
            key = tuple(clean)
            while key and key not in path_id_map:
                key = key[:-1]
            if key:
                cid = path_id_map[key]
        offers.append((cid, it))

    # 7) YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(cat_list, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)
    print(f"[done] items: {len(offers)}, categories: {len(cat_list)} -> {OUT_FILE}")
    return 0

def write_empty_yml():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    empty = """<?xml version='1.0' encoding='windows-1251'?>
<yml_catalog><shop>
<name>vtt</name>
<currencies><currency id="RUB" rate="1" /></currencies>
<categories>
<category id="9600000">VTT</category>
</categories>
<offers>
</offers>
</shop></yml_catalog>
"""
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
