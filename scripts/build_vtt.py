# -*- coding: utf-8 -*-
"""
B2B VTT (Bitrix) → Satu YML
- Логин через форму Bitrix (или готовые куки), затем краулинг каталога.
- Строгий фильтр: название товара ДОЛЖНО НАЧИНАТЬСЯ с одного из ключевых слов.
- Тянем фото (крупная версия), полное описание, цену, SKU и хлебные крошки → строим дерево категорий.
- Только товары с фото.

ENV:
  BASE_URL, START_URL, KEYWORDS_FILE, OUT_FILE, OUTPUT_ENCODING
  VTT_LOGIN, VTT_PASSWORD, VTT_COOKIES
  HTTP_TIMEOUT, REQUEST_DELAY_MS, MIN_BYTES, MAX_CRAWL_MINUTES, MAX_CATEGORY_PAGES, MAX_WORKERS
  ALLOW_PREFIX_BRANDS (через запятую; "" — строго без префиксов бренда)
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
BASE_URL        = os.getenv("BASE_URL", "https://b2bold.vtt.ru").rstrip("/")
START_URL       = os.getenv("START_URL", f"{BASE_URL}/catalog/")
KEYWORDS_FILE   = os.getenv("KEYWORDS_FILE", "docs/vtt_keywords.txt")
OUT_FILE        = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN       = os.getenv("VTT_LOGIN") or ""
VTT_PASSWORD    = os.getenv("VTT_PASSWORD") or ""
VTT_COOKIES     = os.getenv("VTT_COOKIES") or ""

HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS= int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "900"))
MAX_CRAWL_MIN   = int(os.getenv("MAX_CRAWL_MINUTES", "90"))
MAX_CAT_PAGES   = int(os.getenv("MAX_CATEGORY_PAGES", "1500"))
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", "6"))

ALLOW_PREFIX_BRANDS = [b.strip().lower() for b in os.getenv("ALLOW_PREFIX_BRANDS", "").split(",") if b.strip()]

SUPPLIER_NAME   = "VTT"
CURRENCY        = "KZT"
ROOT_CAT_ID     = 9301000
ROOT_CAT_NAME   = "VTT"

UA = {
    "User-Agent": "Mozilla/5.0 (compatible; VTT-B2B-Crawler/1.0; +feed)"
}

def jitter_sleep(ms:int)->None:
    time.sleep(max(0.0, ms/1000.0))

def http_get(s: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = s.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200: return None
        b = r.content
        if len(b) < MIN_BYTES:   return None
        return b
    except Exception:
        return None

def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "lxml")

def yml_escape(s: str) -> str:
    return html.escape(s or "", quote=False)

def sha1(s: str) -> str:
    import hashlib as _h
    return _h.sha1(s.encode("utf-8")).hexdigest()

def key_norm(v: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", v or "").upper()

def sanitize_title(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:200].rstrip()

def to_number(x: Any) -> Optional[float]:
    if x is None: return None
    s = str(x).replace("\xa0"," ").strip()
    s = s.replace(" ", "").replace(",", ".")
    if not re.search(r"\d", s): return None
    try:
        return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        return float(m.group(0)) if m else None

# ---------- keywords ----------
def load_keywords(path: str) -> List[str]:
    ks: List[str] = []
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    ks.append(s)
    if not ks:
        ks = ["drum","девелопер","драм","картридж","термоблок","термоэлемент","тонер-картридж"]
    return ks

def compile_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    pats: List[re.Pattern] = []
    for kw in kws:
        esc = re.escape(kw).replace(r"\ ", " ")
        pats.append(re.compile(r"^\s*" + esc + r"(?!\w)", re.I))
        # вариант с разрешённым бренд-префиксом (если ALLOW_PREFIX_BRANDS задан)
        if ALLOW_PREFIX_BRANDS:
            brands = "|".join([re.escape(b) for b in ALLOW_PREFIX_BRANDS])
            pats.append(re.compile(r"^\s*(?:" + brands + r")\s+" + esc + r"(?!\w)", re.I))
    return pats

def title_startswith_strict(title: str, patterns: List[re.Pattern]) -> bool:
    if not title: return False
    return any(p.search(title) for p in patterns)

# ---------- auth ----------
def apply_cookies_string(sess: requests.Session, cookie_str: str, base: str) -> None:
    if not cookie_str: return
    parts = [p.strip() for p in cookie_str.split(";") if "=" in p]
    domain = urlparse(base).hostname or "b2bold.vtt.ru"
    for p in parts:
        name, value = p.split("=", 1)
        sess.cookies.set(name.strip(), value.strip(), domain=domain, path="/")

def try_login(sess: requests.Session) -> bool:
    """
    Попытка логина через Bitrix-форму:
    - открываем /auth/ (или /), ищем форму с полем password
    - собираем hidden поля, вставляем логин/пароль, отправляем POST на action
    - проверяем, что исчезла форма или появилась ссылка logout
    """
    # 0) если есть готовые куки — сразу применим
    if VTT_COOKIES.strip():
        apply_cookies_string(sess, VTT_COOKIES, BASE_URL)

    # тестовая загрузка каталога
    b = http_get(sess, START_URL)
    if b:
        s = soup_of(b)
        # если мы уже видим карточки, возможно, авторизация не нужна
        if s.find("a", href=re.compile(r"/\bcatalog\b/", re.I)) or s.find_all("img"):
            return True

    # если нет логина/пароля — дальше не пробуем
    if not (VTT_LOGIN and VTT_PASSWORD):
        return bool(b)

    # 1) пытаемся найти страницу логина
    for login_url in [f"{BASE_URL}/auth/", f"{BASE_URL}/login/", f"{BASE_URL}/?login=yes", BASE_URL + "/"]:
        bb = http_get(sess, login_url)
        if not bb: continue
        ss = soup_of(bb)
        form = None
        for f in ss.find_all("form"):
            if f.find("input", {"type": "password"}):
                form = f; break
        if not form:
            continue

        action = form.get("action") or login_url
        action = urljoin(login_url, action)
        payload = {}
        for inp in form.find_all("input"):
            name = inp.get("name")
            if not name: continue
            val = inp.get("value", "")
            payload[name] = val

        # типичные поля Bitrix
        # USER_LOGIN / USER_PASSWORD / AUTH_FORM=Y / TYPE=AUTH / Login=Войти
        # Подменим логин/пароль по известным именам, иначе найдём по типу
        if "USER_LOGIN" in payload:
            payload["USER_LOGIN"] = VTT_LOGIN
        else:
            # найдём любое текстовое поле
            t = form.find("input", {"type": "text"}) or form.find("input", {"type": "email"}) or form.find("input", {"name": re.compile(r"login|email|user", re.I)})
            if t and t.get("name"):
                payload[t.get("name")] = VTT_LOGIN

        if "USER_PASSWORD" in payload:
            payload["USER_PASSWORD"] = VTT_PASSWORD
        else:
            p = form.find("input", {"type": "password"})
            if p and p.get("name"):
                payload[p.get("name")] = VTT_PASSWORD

        if "AUTH_FORM" in payload and not payload["AUTH_FORM"]:
            payload["AUTH_FORM"] = "Y"
        if "TYPE" in payload and not payload["TYPE"]:
            payload["TYPE"] = "AUTH"
        # кнопка
        if "Login" in payload and not payload["Login"]:
            payload["Login"] = "Y"

        try:
            r = sess.post(action, data=payload, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code in (200, 302):
                # повторно проверим каталог
                bb2 = http_get(sess, START_URL)
                if bb2:
                    s2 = soup_of(bb2)
                    # признак успеха — нет явной формы логина и есть контент
                    if not s2.find("input", {"type": "password"}):
                        return True
        except Exception:
            pass

    # последний шанс — были только куки
    return bool(http_get(sess, START_URL))

# ---------- product page parsing ----------
def normalize_img(url: str) -> str:
    if not url: return url
    u = url.strip()
    if u.startswith("//"): u = "https:" + u
    if u.startswith("/"):  u = BASE_URL + u
    # пытаемся получить «крупную» версию (типовые суффиксы)
    u = re.sub(r"(/resize/[^/]+/)", "/", u)
    u = re.sub(r"(\?width=\d+&height=\d+)$", "", u)
    return u

def extract_full_description(s: BeautifulSoup) -> Optional[str]:
    sels = [
        '[itemprop="description"]', '.product-description', '.detail__description',
        '#tab-description', '#description', '.tabs .description', '.content', '.text'
    ]
    for sel in sels:
        el = s.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)
    # fallback: большой блок контента
    for c in s.select('.product, .product-detail, #content, .detail'):
        txt = c.get_text(" ", strip=True)
        if txt and len(txt) > 80:
            return txt
    return None

def extract_breadcrumbs(s: BeautifulSoup) -> List[str]:
    names: List[str] = []
    for bc in s.select('ul.breadcrumb, .breadcrumbs, .breadcrumb, .pathway, [class*="breadcrumb"], [class*="pathway"]'):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if not t: continue
            tl = t.lower()
            if tl in ("главная","home"): continue
            names.append(t.strip())
        if names: break
    return [n for n in names if n]

def parse_product_page(sess: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(sess, url)
    if not b: return None
    s = soup_of(b)

    # title
    title = ""
    for sel in ["h1", ".product-title", "[itemprop='name']", "h1[itemprop='name']"]:
        el = s.select_one(sel)
        if el and el.get_text(strip=True):
            title = sanitize_title(el.get_text(" ", strip=True))
            break
    if not title:
        return None

    # sku
    sku = None
    skuel = s.find(attrs={"itemprop": "sku"})
    if skuel:
        val = (skuel.get_text(" ", strip=True) or "").strip()
        if val: sku = val
    if not sku:
        for lab in ["артикул", "код товара", "sku", "код"]:
            node = s.find(string=lambda t: t and lab in t.lower())
            if node:
                txt = (node.parent.get_text(" ", strip=True) if node.parent else str(node))
                m = re.search(r"([A-Za-z0-9\-\._/]{2,})", txt)
                if m: sku = m.group(1); break
    # price
    price = None
    # itemprop price
    p = s.select_one('[itemprop="price"]')
    if p and (p.get("content") or p.get_text(strip=True)):
        price = to_number(p.get("content") or p.get_text(strip=True))
    if price is None:
        # частые классы
        for sel in [".price", ".product-price", ".detail-price", ".current-price", ".product-item-detail-price-current"]:
            el = s.select_one(sel)
            if el:
                price = to_number(el.get_text(" ", strip=True))
                if price: break
    if price is None:
        # в скриптах/JSON
        for sc in s.find_all("script"):
            txt = sc.string or sc.get_text()
            if not txt: continue
            m = re.search(r'"price"\s*:\s*"?(?P<p>[\d\.,\s]+)"?', txt, flags=re.I)
            if m:
                price = to_number(m.group("p"))
                if price: break

    # picture
    pic = None
    cand = []
    for sel in [
        'img[data-entity="image"]', 'a[data-fancybox] img', '.product-gallery img', '.swiper img',
        'img.product-photo', '.detail_picture img', 'img#bigpic', 'meta[property="og:image"]'
    ]:
        for el in s.select(sel):
            src = el.get("src") or el.get("data-src") or el.get("content")
            if src and len(src) > 5:
                cand.append(src)
    if not cand:
        # любые картинки
        for im in s.find_all("img"):
            src = im.get("src") or im.get("data-src")
            if src and len(src) > 5:
                cand.append(src)
    if cand:
        pic = normalize_img(urljoin(url, cand[0]))
    if not pic:
        return None

    desc = extract_full_description(s) or ""
    crumbs = extract_breadcrumbs(s)

    # vendorCode: sku или из названия (fallback)
    vendor_code = sku or ""
    if not vendor_code:
        m = re.search(r"[A-ZА-Я0-9]{2,}(?:[-/][A-ZА-Я0-9]{2,})?", title, flags=re.I)
        if m: vendor_code = m.group(0)

    return {
        "title": title,
        "sku": vendor_code,
        "price": price,
        "picture": pic,
        "url": url,
        "description": desc,
        "crumbs": crumbs,
    }

# ---------- catalog crawl ----------
PRODUCT_URL_RE = re.compile(r"/catalog/[^?#]+/?$", re.I)

def is_product_url(u: str) -> bool:
    # Тонкая грань между разделом и товаром в Bitrix: чаще товар — конечная страница без пагинации
    # Позже всё равно фильтруем по наличию title/sku на странице.
    # Здесь лишь базовая эвристика.
    return bool(PRODUCT_URL_RE.search(u)) and not u.rstrip("/").endswith("/catalog")

def find_next_page(s: BeautifulSoup, page_url: str) -> Optional[str]:
    ln = s.find("link", attrs={"rel": "next"})
    if ln and ln.get("href"):
        return urljoin(page_url, ln["href"])
    for a in s.find_all("a", href=True):
        txt = (a.get_text(" ", strip=True) or "").lower()
        if txt in ("следующая", "вперед", "вперёд", "next", ">"):
            return urljoin(page_url, a["href"])
        # Bitrix PAGEN_1
        href = a["href"]
        if "PAGEN_" in href and any(t in txt for t in ("след", "next", ">", "далее")):
            return urljoin(page_url, href)
    return None

def collect_product_urls(sess: requests.Session, cat_url: str, limit_pages: int) -> List[str]:
    urls: List[str] = []
    seen_pages: Set[str] = set()
    page = cat_url
    pages_done = 0
    while page and pages_done < limit_pages:
        if page in seen_pages: break
        seen_pages.add(page)
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, page)
        if not b: break
        s = soup_of(b)
        for a in s.find_all("a", href=True):
            href = a["href"]
            absu = urljoin(page, href)
            if urlparse(absu).netloc and urlparse(absu).netloc != urlparse(BASE_URL).netloc:
                continue
            # игнорируем якори/пустое
            if absu.endswith("#") or absu == page:
                continue
            # кандидаты карточек
            if "/catalog/" in absu and not absu.rstrip("/").endswith("/catalog"):
                urls.append(absu)
        page = find_next_page(s, page)
        pages_done += 1
    return list(dict.fromkeys(urls))

def discover_category_urls(sess: requests.Session) -> List[str]:
    seeds = [START_URL, f"{BASE_URL}/catalog/"]
    urls: List[str] = []
    seen: Set[str] = set()
    for u in seeds:
        b = http_get(sess, u)
        if not b: continue
        s = soup_of(b)
        for a in s.find_all("a", href=True):
            href = a["href"]
            absu = urljoin(u, href)
            if urlparse(absu).netloc != urlparse(BASE_URL).netloc:
                continue
            tl = (a.get_text(" ", strip=True) or "").lower()
            # разделы каталога: ссылки внутри /catalog/, исключая явные карточки (дальше всё равно проверим)
            if "/catalog/" in absu:
                if absu not in seen:
                    seen.add(absu); urls.append(absu)
    # отфильтруем мусор
    urls = [u for u in urls if "/catalog/" in u]
    # чуть подсушим список (часто много дублей на главной)
    return list(dict.fromkeys(urls))[:300]  # стартовые до 300 разделов

# ---------- categories tree ----------
def stable_cat_id(text: str, prefix: int = 9401000) -> int:
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
    out.append('<currencies><currency id="KZT" rate="1" /></currencies>')

    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        parent = parent if parent else ROOT_CAT_ID
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{yml_escape(name)}</category>")
    out.append("</categories>")

    out.append("<offers>")
    for cid, it in offers:
        price = it.get("price")
        price_txt = ""
        if price is not None:
            price_txt = str(int(price)) if float(price).is_integer() else f"{price}"
        out += [
            f"<offer id=\"{yml_escape(it['offer_id'])}\" available=\"true\" in_stock=\"true\">",
            f"<name>{yml_escape(it['title'])}</name>",
            f"<vendor>{yml_escape(it.get('brand') or SUPPLIER_NAME)}</vendor>",
            f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>",
        ]
        if price_txt:
            out += [f"<price>{price_txt}</price>", "<currencyId>KZT</currencyId>"]
        out += [f"<categoryId>{cid}</categoryId>"]
        if it.get("url"): out.append(f"<url>{yml_escape(it['url'])}</url>")
        if it.get("picture"): out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        desc = it.get("description") or it["title"]
        out.append(f"<description>{yml_escape(desc)}</description>")
        out += ["<quantity_in_stock>1</quantity_in_stock>", "<stock_quantity>1</stock_quantity>", "<quantity>1</quantity>", "</offer>"]
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------- main ----------
def main() -> int:
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MIN)
    sess = requests.Session()
    sess.headers.update(UA)

    if not try_login(sess):
        print("[warn] Не удалось явно авторизоваться. Пытаемся парсить публично доступный каталог.")

    # 1) Собираем разделы
    cat_urls = discover_category_urls(sess)
    if not cat_urls:
        print("[error] Не нашли разделов каталога.")
        _flush([], [])
        return 2
    print(f"[cats] discovered: {len(cat_urls)}")

    # 2) Собираем URL карточек из разделов
    pages_budget = max(1, MAX_CAT_PAGES // max(1, len(cat_urls)))
    prod_urls: List[str] = []
    for cu in cat_urls:
        prod_urls.extend(collect_product_urls(sess, cu, pages_budget))
    prod_urls = list(dict.fromkeys(u for u in prod_urls if "/catalog/" in u))
    print(f"[crawl] product url candidates: {len(prod_urls)}")

    # 3) Парсим карточки (параллельно)
    def worker(u: str):
        if datetime.utcnow() > deadline: return None
        try:
            data = parse_product_page(sess, u)
            return data
        except Exception:
            return None

    items: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for fut in as_completed({ex.submit(worker, u): u for u in prod_urls}):
            if datetime.utcnow() > deadline: break
            data = fut.result()
            if not data: continue
            items.append(data)
            if len(items) % 50 == 0:
                print(f"[parse] parsed: {len(items)}")

    if not items:
        print("[error] Ни одной карточки не разобрали.")
        _flush([], [])
        return 2
    print(f"[parse] total parsed items: {len(items)}")

    # 4) Фильтр по строгому началу названия
    kws = load_keywords(KEYWORDS_FILE)
    pats = compile_startswith_patterns(kws)
    filtered = [it for it in items if title_startswith_strict(it["title"], pats)]
    print(f"[filter] matched by startswith: {len(filtered)} / {len(items)}")

    # 5) Только с фото и с vendorCode (sku)
    filtered = [it for it in filtered if it.get("picture") and it.get("sku")]
    if not filtered:
        print("[error] После фильтрации не осталось позиций.")
        _flush([], [])
        return 2

    # 6) Категории по крошкам
    all_paths = [it.get("crumbs") for it in filtered if it.get("crumbs")]
    cat_list, path_id_map = build_categories_from_paths(all_paths)
    print(f"[cats] built: {len(cat_list)}")

    # 7) Сбор офферов
    offers: List[Tuple[int, Dict[str, Any]]] = []
    seen_offer_ids: Set[str] = set()
    for it in filtered:
        crumbs = it.get("crumbs") or []
        cid = ROOT_CAT_ID
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
            offer_id = f"{it['sku']}-{sha1(it['title'])[:6]}"
        seen_offer_ids.add(offer_id)
        offers.append((cid, {
            "offer_id":   offer_id,
            "title":      it["title"],
            "price":      it.get("price") if it.get("price") is not None else 0,
            "vendorCode": it["sku"],
            "brand":      SUPPLIER_NAME,
            "url":        it["url"],
            "picture":    it["picture"],
            "description": it.get("description") or it["title"],
        }))

    # 8) YML
    xml = build_yml(cat_list, offers)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, categories: {len(cat_list)} -> {OUT_FILE}")
    return 0

def _flush(categories, offers):
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(build_yml(categories, offers))

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        sys.exit(2)
