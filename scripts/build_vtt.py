# -*- coding: utf-8 -*-
"""
VTT (b2bold.vtt.ru) → Satu YML
- Авторизация через cookies (секрет VTT_COOKIES).
- С канонического /catalog/ находим релевантные разделы, пагинацию, собираем ссылки на товары.
- Для каждой карточки вытаскиваем: title, price, vendorCode (артикул), полное описание, фото (og:image/галерея), хлебные крошки.
- Фильтрация по ключам: название ДОЛЖНО начинаться с одного из ключевых слов (после опционального бренда из ALLOW_PREFIX_BRANDS).
- Генерируем YML (windows-1251).
"""

from __future__ import annotations
import os, re, io, time, html, hashlib
from typing import Any, Dict, List, Optional, Tuple, Set, Iterable
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ---------- ENV ----------
BASE_URL           = os.getenv("BASE_URL", "https://b2bold.vtt.ru").rstrip("/")
START_URL          = os.getenv("START_URL", f"{BASE_URL}/catalog/")
KEYWORDS_FILE      = os.getenv("KEYWORDS_FILE", "docs/vtt_keywords.txt")
OUT_FILE           = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING    = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_COOKIES        = os.getenv("VTT_COOKIES", "").strip()

HTTP_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES          = int(os.getenv("MIN_BYTES", "900"))

MAX_WORKERS        = int(os.getenv("MAX_WORKERS", "6"))
MAX_CRAWL_MINUTES  = int(os.getenv("MAX_CRAWL_MINUTES", "90"))
MAX_CATEGORY_PAGES = int(os.getenv("MAX_CATEGORY_PAGES", "1500"))

ALLOW_PREFIX_BRANDS = [b.strip().lower() for b in os.getenv("ALLOW_PREFIX_BRANDS","").split(",") if b.strip()]

SUPPLIER_NAME      = "VTT"
CURRENCY           = "RUB"   # на сайте рубли; при необходимости замените
ROOT_CAT_ID        = 9600000
ROOT_CAT_NAME      = "VTT"

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; VTT-Catalog-Feed/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ---------- helpers ----------
def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms/1000.0))

def parse_cookie_string(cookie_str: str) -> Dict[str, str]:
    jar: Dict[str, str] = {}
    for part in cookie_str.split(";"):
        part = part.strip()
        if not part or "=" not in part: continue
        k, v = part.split("=", 1)
        k = k.strip(); v = v.strip()
        if k and v: jar[k] = v
    return jar

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA_HEADERS)
    if VTT_COOKIES:
        jar = parse_cookie_string(VTT_COOKIES)
        for k, v in jar.items():
            s.cookies.set(k, v, domain=urlparse(BASE_URL).hostname)
    return s

def http_get(session: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code != 200: return None
        b = r.content
        if len(b) < MIN_BYTES:   return None
        return b
    except Exception:
        return None

def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "lxml")

def yml_escape(s: str) -> str:
    return html.escape(s or "")

def sha1(s: str) -> str:
    import hashlib as _h
    return _h.sha1(s.encode("utf-8")).hexdigest()

def sanitize_spaces(s: str) -> str:
    return re.sub(r"\s{2,}", " ", (s or "").strip())

def to_number(txt: Any) -> Optional[float]:
    if txt is None: return None
    s = sanitize_spaces(str(txt)).replace("\xa0"," ").replace(" ", "")
    s = s.replace(",", ".")
    if not re.search(r"\d", s): return None
    try:
        return float(re.findall(r"[0-9]+(?:\.[0-9]+)?", s)[0])
    except Exception:
        return None

# ---------- keywords ----------
def load_keywords(path: str) -> List[str]:
    kws: List[str] = []
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    kws.append(s)
    if not kws:
        # без "кабель сетевой", по твоему требованию
        kws = ["drum","девелопер","драм","картридж","термоблок","термоэлемент","тонер-картридж"]
    return kws

def compile_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    pats: List[re.Pattern] = []
    for kw in kws:
        esc = re.escape(kw).replace(r"\ ", " ")
        pats.append(re.compile(r"^\s*" + esc + r"(?!\w)", re.I))
    return pats

def title_startswith_strict(title: str, pats: List[re.Pattern]) -> bool:
    return any(p.search(title) for p in pats)

def title_startswith_with_brand_allow(title: str, pats: List[re.Pattern]) -> bool:
    t = title.strip()
    if title_startswith_strict(t, pats):
        return True
    # допускаем один бренд-префикс
    tl = t.lower()
    for b in ALLOW_PREFIX_BRANDS:
        if tl.startswith(b + " "):
            rest = t[len(b):].lstrip()
            return title_startswith_strict(rest, pats)
    return False

# ---------- site parsing ----------
PRODUCT_HINTS = ("product", "/p/", "/sku/", "/item/", "/goods/", "/catalog/")  # last is wide; доп. проверка по DOM
CATEGORY_HINTS = ("catalog", "category", "catalogue", "section")

def looks_like_product_url(u: str) -> bool:
    lu = u.lower()
    # часто товары в /catalog/<slug>/, поэтому проверяем DOM позже
    if "/product" in lu or "/p/" in lu or "/item/" in lu or "/goods/" in lu:
        return True
    return False

def looks_like_category_url(u: str) -> bool:
    lu = u.lower()
    if "/catalog" in lu or "/catalogue" in lu or "/category" in lu or "/sections" in lu:
        return True
    return False

def is_valid_internal(u: str) -> bool:
    if not u.startswith("http"):
        return True
    host = urlparse(u).hostname or ""
    return host.endswith(urlparse(BASE_URL).hostname or "")

def extract_price(s: BeautifulSoup) -> Optional[float]:
    # разные варианты цен
    cand = []
    for sel in [
        ".price", ".product-price", ".product__price", ".sku-price", "[class*='price']",
        "[itemprop='price']",
        "meta[itemprop='price']",
    ]:
        for el in s.select(sel):
            if el.name == "meta" and el.get("content"):
                cand.append(el["content"])
            else:
                txt = el.get_text(" ", strip=True)
                if txt: cand.append(txt)
    # уникализируем, берём первое нормальное число
    seen = set()
    for x in cand:
        x = x.strip()
        if not x or x in seen: continue
        seen.add(x)
        val = to_number(x)
        if val and val > 0:
            return val
    return None

def extract_sku(s: BeautifulSoup) -> Optional[str]:
    # типичные подписи
    texts = s.get_text(" ", strip=True)
    m = re.search(r"(?:Артикул|SKU|Код)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})", texts, flags=re.I)
    if m:
        return m.group(1).strip()
    # мета/атрибуты
    el = s.find(attrs={"itemprop": "sku"})
    if el:
        v = el.get_text(" ", strip=True) or el.get("content") or ""
        if v.strip():
            return v.strip()
    # таблицы характеристик
    for th in s.find_all(["th","td"]):
        t = (th.get_text(" ", strip=True) or "").lower()
        if t in ("артикул", "sku", "код"):
            td = th.find_next("td")
            if td:
                v = td.get_text(" ", strip=True)
                if v:
                    return v
    return None

def extract_title(s: BeautifulSoup) -> Optional[str]:
    for sel in ["h1", "h1[itemprop='name']", "[itemprop='name']"]:
        el = s.select_one(sel)
        if el:
            t = el.get_text(" ", strip=True)
            if t: return t
    og = s.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()
    return None

def extract_description(s: BeautifulSoup) -> Optional[str]:
    for sel in [
        "[itemprop='description']",
        ".product-description", ".desc", ".tab-content .description", "#description",
        ".tabs .description", ".product__description", ".content"
    ]:
        el = s.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)
    # fallback: большой текст из области товара
    for box in s.select(".product, .product-page, .product__wrapper, #content"):
        t = box.get_text(" ", strip=True)
        if t and len(t) > 60:
            return t
    return None

def extract_image(s: BeautifulSoup, base: str) -> Optional[str]:
    og = s.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        u = og["content"].strip()
        if u:
            return urljoin(base, u)
    # из галереи
    for sel in ["img.product-image", ".product-gallery img", "img[data-src]", "img[src]"]:
        for img in s.select(sel):
            u = img.get("src") or img.get("data-src") or ""
            if not u: continue
            if any(k in u.lower() for k in ["/upload", "/images", "/img", "/media"]):
                return urljoin(base, u)
    return None

def extract_breadcrumbs(s: BeautifulSoup) -> List[str]:
    names: List[str] = []
    for bc in s.select("ul.breadcrumbs, .breadcrumbs, .breadcrumb, nav.breadcrumbs, [class*='breadcrumb']"):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if t and t.lower() not in ("главная", "home"):
                names.append(t)
        if names:
            break
    return [sanitize_spaces(x) for x in names if x]

def is_product_dom(s: BeautifulSoup) -> bool:
    # Наличие h1, цены и кнопки купить часто встречается на карточке
    if not extract_title(s):
        return False
    if extract_price(s) is None:
        # некоторые карточки без цены (по запросу) — допустим
        pass
    # ищем признаки кнопок
    btn = s.find(lambda tag: tag.name in ("button","a") and tag.get_text(strip=True) and any(w in tag.get_text(strip=True).lower() for w in ["в корзину","купить","заказать"]))
    # карточка часто имеет SKU/описание
    if extract_sku(s) or extract_description(s) or btn:
        return True
    return False

def category_next_url(s: BeautifulSoup, page_url: str) -> Optional[str]:
    ln = s.find("link", attrs={"rel": "next"})
    if ln and ln.get("href"):
        return urljoin(page_url, ln["href"])
    a = s.find("a", class_=lambda c: c and "next" in c.lower())
    if a and a.get("href"):
        return urljoin(page_url, a["href"])
    for a in s.find_all("a", href=True):
        tt = (a.get_text(" ", strip=True) or "").lower()
        if tt in ("следующая", "вперед", "вперёд", "next", "»", ">"):
            return urljoin(page_url, a["href"])
    return None

def collect_categories(session: requests.Session, start_url: str, limit_pages: int) -> List[str]:
    """Собираем ссылки разделов каталога (включая вложенные), без тестов на продуктовые страницы."""
    q = [start_url]
    seen: Set[str] = set()
    cats: List[str] = []
    pages = 0

    while q and pages < limit_pages:
        u = q.pop(0)
        if u in seen: continue
        seen.add(u)
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(session, u)
        if not b: 
            continue
        s = soup_of(b)
        pages += 1
        cats.append(u)

        # пагинация внутри раздела
        nxt = category_next_url(s, u)
        if nxt and nxt not in seen:
            q.append(nxt)

        # ссылки на вложенные категории
        for a in s.find_all("a", href=True):
            href = a["href"].strip()
            absu = urljoin(u, href)
            if not is_valid_internal(absu):
                continue
            if any(x in absu.lower() for x in ["/login","/auth","/cart","/order","/compare","/favorites","?PAGEN_","?sort="]):
                # сортировки и пагинации тоже можно, но мы их соберём через category_next_url
                pass
            # берём ссылки похожие на категории
            if looks_like_category_url(absu):
                # отсекаем явные карточки
                if looks_like_product_url(absu):
                    continue
                if absu not in seen and absu not in q and absu.startswith(f"{BASE_URL}/"):
                    q.append(absu)

        if pages >= limit_pages:
            break

    # уникализируем, сохраняя порядок
    return list(dict.fromkeys(cats))

def collect_product_urls_from_category(session: requests.Session, cat_url: str, limit_pages: int) -> List[str]:
    """На странице раздела собираем ссылки на товары; гоняем пагинацию."""
    urls: List[str] = []
    page = cat_url
    pages_done = 0
    seen_pages: Set[str] = set()

    while page and pages_done < limit_pages:
        if page in seen_pages:
            break
        seen_pages.add(page)

        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(session, page)
        if not b:
            break
        s = soup_of(b)

        # типичные ссылки карточек внутри списка
        for a in s.find_all("a", href=True):
            href = a["href"].strip()
            absu = urljoin(page, href)
            if not is_valid_internal(absu): 
                continue
            # фильтр на явные карточки
            if looks_like_product_url(absu):
                urls.append(absu)
                continue
            # fallback: по тексту
            txt = a.get_text(" ", strip=True) or ""
            if txt and len(txt) > 5 and any(w in txt.lower() for w in ["купить","подробнее","товар","описание"]):
                urls.append(absu)

        # пагинация
        page = category_next_url(s, page)
        pages_done += 1

    return list(dict.fromkeys(urls))

def parse_product_page(session: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(session, url)
    if not b: return None
    s = soup_of(b)
    if not is_product_dom(s):
        # на всякий случай — если это не карточка
        return None

    title = sanitize_spaces(extract_title(s) or "")
    if not title:
        return None
    price = extract_price(s)  # может быть None — пропустим потом
    sku   = extract_sku(s) or ""
    pic   = extract_image(s, url) or ""
    desc  = sanitize_spaces(extract_description(s) or title)
    crumbs = extract_breadcrumbs(s)

    return {
        "url": url,
        "title": title,
        "price": price,
        "vendorCode": sku,
        "picture": pic,
        "description": desc,
        "crumbs": crumbs,
    }

# ---------- categories build ----------
def stable_cat_id(text: str, prefix: int = 9610000) -> int:
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
    out.append(f"<currencies><currency id=\"{CURRENCY}\" rate=\"1\" /></currencies>")

    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        parent = parent if parent else ROOT_CAT_ID
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{yml_escape(name)}</category>")
    out.append("</categories>")

    out.append("<offers>")
    for cid, it in offers:
        price = it.get("price")
        if price is None:
            # на VTT может быть «по запросу» — пропускаем такие, чтобы не падал импорт
            continue
        price_txt = str(int(price)) if float(price).is_integer() else f"{price}"
        out += [
            f"<offer id=\"{yml_escape(it['offer_id'])}\" available=\"true\" in_stock=\"true\">",
            f"<name>{yml_escape(it['title'])}</name>",
            f"<vendor>{yml_escape(it.get('brand') or SUPPLIER_NAME)}</vendor>",
            f"<vendorCode>{yml_escape(it.get('vendorCode') or '')}</vendorCode>",
            f"<price>{price_txt}</price>",
            f"<currencyId>{CURRENCY}</currencyId>",
            f"<categoryId>{cid}</categoryId>",
        ]
        if it.get("url"):     out.append(f"<url>{yml_escape(it['url'])}</url>")
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
    session = make_session()

    # проверка доступа
    root_bytes = http_get(session, START_URL)
    if not root_bytes:
        raise RuntimeError("Не нашли разделов каталога.")

    # ключи
    keywords = load_keywords(KEYWORDS_FILE)
    pats = compile_startswith_patterns(keywords)

    # 1) собираем разделы
    cats = collect_categories(session, START_URL, limit_pages=MAX_CATEGORY_PAGES)
    if not cats:
        raise RuntimeError("Не нашли разделов каталога.")

    print(f"[cats] discovered: {len(cats)}")

    # 2) из разделов вытаскиваем ссылки на карточки
    per_cat_budget = max(1, MAX_CATEGORY_PAGES // max(1, len(cats)))
    prod_urls: List[str] = []
    for cu in cats:
        urls = collect_product_urls_from_category(session, cu, per_cat_budget)
        prod_urls.extend(urls)
    prod_urls = list(dict.fromkeys(prod_urls))
    print(f"[crawl] product URLs: {len(prod_urls)}")

    # 3) парсим карточки (параллельно)
    def worker(u: str) -> Optional[Dict[str, Any]]:
        if datetime.utcnow() > deadline:
            return None
        try:
            return parse_product_page(session, u)
        except Exception:
            return None

    parsed: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = { ex.submit(worker, u): u for u in prod_urls }
        for fut in as_completed(futures):
            if datetime.utcnow() > deadline:
                break
            rec = fut.result()
            if rec:
                parsed.append(rec)

    print(f"[parsed] products parsed: {len(parsed)}")

    if not parsed:
        # создадим пустой фид, чтобы шаг коммита не падал
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(build_yml([], []))
        print("[warn] Нет доступных карточек (возможно, cookies устарели).")
        return 2

    # 4) фильтр по startswith (с допуском бренда)
    filtered: List[Dict[str, Any]] = []
    for it in parsed:
        title = it["title"]
        if title_startswith_with_brand_allow(title, pats):
            filtered.append(it)

    print(f"[filter] passed by startswith: {len(filtered)} / {len(parsed)}")

    if not filtered:
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(build_yml([], []))
        print("[warn] После фильтрации нет позиций (проверьте ключевые слова).")
        return 2

    # 5) категории по крошкам
    all_paths = [r.get("crumbs") for r in filtered if r.get("crumbs")]
    cat_list, path_id_map = build_categories_from_paths(all_paths)
    print(f"[cats] built: {len(cat_list)}")

    # 6) в офферы (уникальный offer_id)
    offers: List[Tuple[int, Dict[str, Any]]] = []
    used_ids: Set[str] = set()
    for it in filtered:
        # цена обязательна для импорта
        if it.get("price") is None:
            continue
        # подберём категорию
        cid = ROOT_CAT_ID
        crumbs = it.get("crumbs") or []
        clean = [p.strip() for p in crumbs if p and p.strip()]
        if clean:
            key = tuple([p for p in clean if p.lower() not in ("главная","home","каталог")])
            while key and key not in path_id_map:
                key = key[:-1]
            if key and key in path_id_map:
                cid = path_id_map[key]

        # offer_id
        base_id = it.get("vendorCode") or sha1(it["url"])[:10]
        offer_id = base_id
        if offer_id in used_ids:
            offer_id = f"{base_id}-{sha1(it['title'])[:6]}"
        used_ids.add(offer_id)

        offers.append((cid, {
            "offer_id": offer_id,
            "title": it["title"][:200].rstrip(),
            "price": it["price"],
            "vendorCode": it.get("vendorCode") or "",
            "brand": SUPPLIER_NAME,
            "url": it.get("url") or "",
            "picture": it.get("picture") or "",
            "description": it.get("description") or it["title"],
        }))

    # 7) YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(cat_list, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, categories: {len(cat_list)} -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("Error: ", e)
        sys.exit(2)
