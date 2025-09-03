# -*- coding: utf-8 -*-
"""
VTT (b2b.vtt.ru) → YML (KZT)
- Логин через /validateLogin (только username/password).
- Сбор ссылок из /catalog/?page=1..MAX_PAGES (без сохранения дебага).
- Разбор карточки:
    * Название: .page_title (fallback: <title> / h1)
    * Пары dt/dd в .description.catalog_item_descr:
        - Артикул → vendorCode (обязательно)
        - Вендор  → <vendor> (бренд)
        - Категория/Подкатегория → дерево категорий
    * Цена (тенге): .price_main → округлить к int; если нет — 1
    * Картинка: из background-url/ссылок на /images/*.jpg|png (первая)
- Фильтр: строгое startswith по KEYWORDS_FILE (если файла нет — создаётся во workflow).
- В YML:
    vendor = бренд из «Вендор», currencyId = KZT, цена ≥ 1.
"""

from __future__ import annotations
import os, re, io, time, html, hashlib
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# -------- ENV --------
BASE_URL        = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL       = os.getenv("START_URL", f"{BASE_URL}/catalog/")
OUT_FILE        = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN       = os.getenv("VTT_LOGIN", "").strip()
VTT_PASSWORD    = os.getenv("VTT_PASSWORD", "").strip()

KEYWORDS_FILE   = os.getenv("KEYWORDS_FILE", "docs/vtt_keywords.txt")

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"
ALLOW_SSL_FALLBACK = os.getenv("ALLOW_SSL_FALLBACK", "0") == "1"

HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS= int(os.getenv("REQUEST_DELAY_MS", "120"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "600"))

MAX_PAGES       = int(os.getenv("MAX_PAGES", "800"))
MAX_CRAWL_MIN   = int(os.getenv("MAX_CRAWL_MINUTES", "60"))
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", "6"))

ROOT_CAT_ID     = 9600000
ROOT_CAT_NAME   = "VTT"
CURRENCY        = "KZT"

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-Feed/1.0)"}

def jitter_sleep(): time.sleep(max(0.0, REQUEST_DELAY_MS/1000.0))
def yml_escape(s: str) -> str: return html.escape(s or "")
def sha1(s: str) -> str: return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()
def abs_url(u: str) -> str: return urljoin(BASE_URL + "/", (u or "").strip())
def to_float(s: str) -> Optional[float]:
    if not s: return None
    t = s.replace("\xa0"," ").replace(" ","").replace(",",".")
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    try: return float(m.group(1)) if m else None
    except Exception: return None

# -------- keywords (strict startswith) --------
def load_keywords(path: str) -> List[str]:
    kws: List[str] = []
    try:
        if os.path.isfile(path):
            with io.open(path, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s and not s.startswith("#"):
                        kws.append(s)
    except Exception:
        pass
    if not kws:
        kws = ["Девелопер","Драм-картридж","Драм-юнит","Картридж","Копи-картридж","Принт-картридж","Термоблок","Термоэлемент","Тонер-картридж"]
    return kws

def compile_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    return [re.compile(r"^\s*" + re.escape(kw) + r"(?!\w)", re.IGNORECASE) for kw in kws]

def title_startswith(title: str, pats: List[re.Pattern]) -> bool:
    if not title: return False
    return any(p.search(title) for p in pats)

# -------- session / http --------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    s.verify = not DISABLE_SSL_VERIFY
    return s

def http_get(s: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = s.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code != 200: return None
        b = r.content
        if len(b) < MIN_BYTES:  # страницы логина короче?
            return b if b else None
        return b
    except requests.exceptions.SSLError:
        if ALLOW_SSL_FALLBACK:
            try:
                r = s.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True, verify=False)
                if r.status_code == 200:
                    return r.content
            except Exception:
                return None
        return None
    except Exception:
        return None

def soup_of(b: bytes, xml: bool=False) -> BeautifulSoup:
    return BeautifulSoup(b or b"", "html.parser")

# -------- login --------
def login_vtt(s: requests.Session) -> bool:
    # получаем csrf
    b = http_get(s, BASE_URL + "/")
    if not b: return False
    soup = soup_of(b)
    csrf = None
    m = soup.find("meta", attrs={"name":"csrf-token"})
    if m and m.get("content"): csrf = m["content"].strip()

    # пробуем POST /validateLogin (без csrf тоже часто работает)
    data = {"login": VTT_LOGIN, "password": VTT_PASSWORD, "remember": "1"}
    headers = {}
    if csrf: headers["X-CSRF-TOKEN"] = csrf

    try:
        r = s.post(BASE_URL + "/validateLogin", data=data, headers=headers, timeout=HTTP_TIMEOUT)
        # иногда возвращают JSON, иногда редирект
        if r.status_code not in (200, 302): return False
    except requests.exceptions.SSLError:
        if not ALLOW_SSL_FALLBACK: return False
        try:
            r = s.post(BASE_URL + "/validateLogin", data=data, headers=headers, timeout=HTTP_TIMEOUT, verify=False)
            if r.status_code not in (200, 302): return False
        except Exception:
            return False
    except Exception:
        return False

    # проверяем доступность каталога
    b = http_get(s, START_URL)
    return bool(b)

# -------- discover links --------
A_PROD = re.compile(r"^https?://[^/]+/catalog/[^/?#]+")

def discover_product_urls(s: requests.Session, max_pages: int) -> List[str]:
    urls: List[str] = []
    seen: Set[str] = set()
    start = datetime.utcnow()
    for i in range(1, max_pages+1):
        if datetime.utcnow() - start > timedelta(minutes=MAX_CRAWL_MIN): break
        u = f"{START_URL}?page={i}"
        jitter_sleep()
        b = http_get(s, u)
        if not b: break
        soup = soup_of(b)
        # карточки на листинге: div.cutoff-off > a (вторая ссылка — собственно товар)
        for box in soup.select("div.cutoff-off"):
            # берём все <a> внутри, кроме кнопки камеры
            links = [a for a in box.find_all("a", href=True) if "btn_pic_1" not in (a.get("class") or [])]
            for a in links:
                href = a["href"].strip()
                if href and "/catalog/" in href:
                    absu = abs_url(href)
                    if A_PROD.match(absu) and absu not in seen:
                        seen.add(absu); urls.append(absu)
        # если на странице почти ничего — дальше смысла меньше
        if i > 3 and len(urls) == 0:
            break
    return urls

# -------- parse product page --------
def parse_price_kzt(soup: BeautifulSoup) -> Optional[int]:
    # VTT показывает $ и тенге: .price_usd и .price_main
    el = soup.select_one(".price_main")
    if el:
        raw = el.get_text(" ", strip=True)
        val = to_float(raw)
        if val is not None:
            return int(round(val))
    # запасной путь: искать число перед Т/т/₸
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"(\d[\d\s.,]*)\s*[₸Тт]\b", txt)
    if m:
        val = to_float(m.group(1))
        if val is not None:
            return int(round(val))
    return None

def first_image_url(soup: BeautifulSoup) -> Optional[str]:
    # 1) og:image
    og = soup.find("meta", attrs={"property":"og:image"})
    if og and og.get("content"):
        return abs_url(og["content"])
    # 2) любые /images/*.jpg|png в атрибутах
    for tag in soup.find_all(True):
        for attr in ("src","data-src","href","data-img","content","style"):
            v = tag.get(attr)
            if not v or not isinstance(v, str): continue
            if "background-image" in attr or v.startswith("background-image"):
                m = re.search(r"url\(['\"]?([^'\")]+)", v)
                if m and (".jpg" in m.group(1).lower() or ".png" in m.group(1).lower()):
                    return abs_url(m.group(1))
            if "/images/" in v and (v.lower().endswith(".jpg") or v.lower().endswith(".png")):
                return abs_url(v)
    return None

def parse_pairs(soup: BeautifulSoup) -> Dict[str,str]:
    out: Dict[str,str] = {}
    box = soup.select_one("div.description.catalog_item_descr")
    if not box: return out
    dts = box.find_all("dt")
    dds = box.find_all("dd")
    for dt, dd in zip(dts, dds):
        k = (dt.get_text(" ", strip=True) or "").strip().strip(":")
        v = (dd.get_text(" ", strip=True) or "").strip()
        if k:
            out[k] = v
    return out

def parse_product(s: requests.Session, url: str, pats: List[re.Pattern]) -> Optional[Dict[str,Any]]:
    jitter_sleep()
    b = http_get(s, url)
    if not b: return None
    soup = soup_of(b)

    title = None
    el = soup.select_one(".page_title")
    if el and el.get_text(strip=True): title = el.get_text(" ", strip=True)
    if not title and soup.title: title = soup.title.get_text(" ", strip=True)
    if not title:
        h1 = soup.find("h1")
        if h1: title = h1.get_text(" ", strip=True)
    if not title: return None

    if not title_startswith(title, pats):
        return None

    pairs = parse_pairs(soup)
    vendor_code = pairs.get("Артикул") or pairs.get("Партс-номер") or ""
    vendor_code = vendor_code.strip()
    if not vendor_code:
        return None  # артикул обязателен

    brand = pairs.get("Вендор", "").strip()

    price_int = parse_price_kzt(soup)
    if price_int is None or price_int <= 0:
        price_int = 1

    picture = first_image_url(soup)

    cat  = pairs.get("Категория", "").strip()
    scat = pairs.get("Подкатегория", "").strip()
    cats = [c for c in [cat, scat] if c]

    return {
        "title": title,
        "vendorCode": vendor_code,
        "brand": brand or "",
        "price": int(price_int),
        "picture": picture,
        "url": url,
        "cats": cats,
    }

# -------- categories --------
def stable_cat_id(text: str, prefix: int = 9620000) -> int:
    h = hashlib.md5((text or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
    cat_map: Dict[Tuple[str,...], int] = {}
    out_list: List[Tuple[int,str,Optional[int]]] = []
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        if not clean: continue
        parent = ROOT_CAT_ID
        acc: List[str] = []
        for name in clean:
            acc.append(name)
            key = tuple(acc)
            if key in cat_map:
                parent = cat_map[key]; continue
            cid = stable_cat_id(" / ".join(acc))
            cat_map[key] = cid
            out_list.append((cid, name, parent))
            parent = cid
    return out_list, cat_map

# -------- YML --------
def build_yml(categories: List[Tuple[int,str,Optional[int]]], offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append("<name>vtt</name>")
    out.append('<currencies><currency id="KZT" rate="1" /></currencies>')
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        parent = parent if parent else ROOT_CAT_ID
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{yml_escape(name)}</category>")
    out.append("</categories>")
    out.append("<offers>")
    for cid, it in offers:
        out.append(f"<offer id=\"{yml_escape(it['vendorCode'])}\" available=\"true\" in_stock=\"true\">")
        out.append(f"<name>{yml_escape(it['title'])}</name>")
        out.append(f"<vendor>{yml_escape(it.get('brand') or '')}</vendor>")
        out.append(f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>")
        out.append(f"<price>{int(it['price'])}</price>")
        out.append("<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        if it.get("url"): out.append(f"<url>{yml_escape(it['url'])}</url>")
        if it.get("picture"): out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        # описание опустим, чтобы не таскать лишнее
        out.append("<quantity_in_stock>1</quantity_in_stock>")
        out.append("<stock_quantity>1</stock_quantity>")
        out.append("<quantity>1</quantity>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# -------- MAIN --------
def main() -> int:
    start_ts = datetime.utcnow()
    s = make_session()
    if DISABLE_SSL_VERIFY:
        print("[ssl] verification disabled by env")

    if not VTT_LOGIN or not VTT_PASSWORD:
        print("[warn] Empty login/password; cannot proceed.")
        # всё равно создадим пустой yml
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with io.open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(build_yml([], []))
        return 0

    if not login_vtt(s):
        print("Error: login failed")
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with io.open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(build_yml([], []))
        return 0

    kw_list = load_keywords(KEYWORDS_FILE)
    pats = compile_startswith_patterns(kw_list)

    urls = discover_product_urls(s, MAX_PAGES)
    print(f"[discover] product urls: {len(urls)}")

    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MIN)
    offers: List[Tuple[int,Dict[str,Any]]] = []
    seen_offer_ids: Set[str] = set()
    all_paths: List[List[str]] = []

    def worker(u: str) -> Optional[Dict[str,Any]]:
        if datetime.utcnow() > deadline: return None
        try:
            return parse_product(s, u, pats)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(worker, u) for u in urls]
        for fut in as_completed(futs):
            it = fut.result()
            if not it: continue
            offer_id = it["vendorCode"]
            if offer_id in seen_offer_ids:
                offer_id = f"{offer_id}-{sha1(it['title'])[:6]}"
            seen_offer_ids.add(offer_id)

            cats = it.get("cats") or []
            all_paths.append(cats)

            # временно categoryId поставим ROOT, пересчитаем после build_categories
            offers.append((ROOT_CAT_ID, {
                "vendorCode": offer_id,
                "title": it["title"],
                "brand": it.get("brand") or "",
                "price": int(it["price"]) if it.get("price") else 1,
                "url": it.get("url"),
                "picture": it.get("picture"),
            }))

    # категории
    categories, path_map = build_categories(all_paths)
    # подменим categoryId на самую глубокую подходящую
    for i, (cid, it) in enumerate(offers):
        cats = all_paths[i] if i < len(all_paths) else []
        key = tuple([c for c in cats if c])
        if key in path_map:
            offers[i] = (path_map[key], it)
        else:
            offers[i] = (ROOT_CAT_ID, it)

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(categories, offers)
    with io.open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, cats: {len(categories)} -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        try:
            os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
            with io.open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
                f.write("<?xml version='1.0' encoding='windows-1251'?>\n<yml_catalog><shop><name>vtt</name><currencies><currency id=\"KZT\" rate=\"1\" /></currencies><categories><category id=\"9600000\">VTT</category></categories><offers></offers></shop></yml_catalog>")
        except Exception:
            pass
        sys.exit(0)
