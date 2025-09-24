# scripts/build_vtt.py
# -*- coding: utf-8 -*-
"""
VTT (b2b.vtt.ru) -> YML (KZT) c очисткой:
- Исключаем из вывода: <categories>, <categoryId>, <currencyId>, <quantity>, <stock_quantity>, <quantity_in_stock>, <url>.
- У каждого <offer> ровно один <available>true</available> (без атрибутов available/in_stock).
- Остальное поведение как прежде (логин, обход категорий из файла, парсинг карточек).
"""

from __future__ import annotations
import os, re, io, time, html, hashlib
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
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

CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "docs/categories_vtt.txt")

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

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-Feed/2.0)"}

def jitter_sleep() -> None:
    time.sleep(max(0.0, REQUEST_DELAY_MS/1000.0))

def yml_escape(s: str) -> str:
    return html.escape(s or "")

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()

def abs_url(u: str) -> str:
    return urljoin(BASE_URL + "/", (u or "").strip())

def to_float(s: str) -> Optional[float]:
    if not s:
        return None
    t = s.replace("\xa0"," ").replace(" ","").replace(",",".")
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    try:
        return float(m.group(1)) if m else None
    except Exception:
        return None

# -------- session / http --------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    s.verify = not DISABLE_SSL_VERIFY
    return s

def http_get(s: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = s.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            return None
        b = r.content
        if len(b) < MIN_BYTES:
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

def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b or b"", "html.parser")

# -------- login --------
def login_vtt(s: requests.Session) -> bool:
    b = http_get(s, BASE_URL + "/")
    if not b:
        return False
    soup = soup_of(b)
    csrf = None
    m = soup.find("meta", attrs={"name":"csrf-token"})
    if m and m.get("content"):
        csrf = m["content"].strip()

    data = {"login": VTT_LOGIN, "password": VTT_PASSWORD, "remember": "1"}
    headers: Dict[str, str] = {}
    if csrf:
        headers["X-CSRF-TOKEN"] = csrf

    try:
        r = s.post(BASE_URL + "/validateLogin", data=data, headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code not in (200, 302):
            return False
    except requests.exceptions.SSLError:
        if not ALLOW_SSL_FALLBACK:
            return False
        try:
            r = s.post(BASE_URL + "/validateLogin", data=data, headers=headers, timeout=HTTP_TIMEOUT, verify=False)
            if r.status_code not in (200, 302):
                return False
        except Exception:
            return False
    except Exception:
        return False

    return bool(http_get(s, START_URL))

# -------- categories from file --------
def load_categories(path: str) -> List[str]:
    out: List[str] = []
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                u = (line or "").strip()
                if u and not u.startswith("#"):
                    out.append(u)
    return out

def add_or_replace_page_param(u: str, page: int) -> str:
    pr = urlparse(u)
    q = dict(parse_qsl(pr.query, keep_blank_values=True))
    q["page"] = str(page)
    new_q = urlencode(q, doseq=True)
    return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))

# -------- discover links per category --------
A_PROD = re.compile(r"^https?://[^/]+/catalog/[^/?#]+")

def collect_product_urls_from_category(s: requests.Session, cat_url: str, max_pages: int, deadline: datetime) -> List[str]:
    urls: List[str] = []
    seen: Set[str] = set()
    for i in range(1, max_pages+1):
        if datetime.utcnow() > deadline:
            break
        page_url = add_or_replace_page_param(cat_url, i)
        jitter_sleep()
        b = http_get(s, page_url)
        if not b:
            break
        soup = soup_of(b)
        found_here = 0
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue
            classes = a.get("class") or []
            if isinstance(classes, list) and any("btn_pic" in c for c in classes):
                continue
            absu = abs_url(href)
            if A_PROD.match(absu) and absu not in seen:
                seen.add(absu)
                urls.append(absu)
                found_here += 1
        if found_here == 0:
            break
    return urls

# -------- parse product page --------
def parse_price_kzt(soup: BeautifulSoup) -> Optional[int]:
    el = soup.select_one(".price_main")
    if el:
        val = to_float(el.get_text(" ", strip=True))
        if val is not None:
            return int(round(val))
    txt = soup.get_text(" ", strip=True)
    m = re.search(r"(\d[\d\s.,]*)\s*[₸Тт]\b", txt)
    if m:
        val = to_float(m.group(1))
        if val is not None:
            return int(round(val))
    return None

def first_image_url(soup: BeautifulSoup) -> Optional[str]:
    og = soup.find("meta", attrs={"property":"og:image"})
    if og and og.get("content"):
        return abs_url(og["content"])
    for tag in soup.find_all(True):
        style = tag.get("style")
        if isinstance(style, str) and "background-image" in style:
            m = re.search(r"url\(['\"]?([^'\"\)]+)", style)
            if m and (".jpg" in m.group(1).lower() or ".png" in m.group(1).lower()):
                return abs_url(m.group(1))
        for attr in ("src","data-src","href","data-img","content"):
            v = tag.get(attr)
            if not v or not isinstance(v, str):
                continue
            vl = v.lower()
            if "/images/" in vl and (vl.endswith(".jpg") or vl.endswith(".png")):
                return abs_url(v)
    return None

def parse_pairs(soup: BeautifulSoup) -> Dict[str,str]:
    out: Dict[str,str] = {}
    box = soup.select_one("div.description.catalog_item_descr")
    if not box:
        return out
    dts = box.find_all("dt")
    dds = box.find_all("dd")
    for dt, dd in zip(dts, dds):
        k = (dt.get_text(" ", strip=True) or "").strip().strip(":")
        v = (dd.get_text(" ", strip=True) or "").strip()
        if k:
            out[k] = v
    return out

def extract_description_meta(soup: BeautifulSoup) -> str:
    tag = soup.find("meta", attrs={"name": "description"})
    if tag and tag.get("content"):
        return re.sub(r"\s+", " ", tag["content"].strip())
    tag = soup.find("meta", attrs={"property": "og:description"})
    if tag and tag.get("content"):
        return re.sub(r"\s+", " ", tag["content"].strip())
    return ""

def parse_product(s: requests.Session, url: str) -> Optional[Dict[str,Any]]:
    jitter_sleep()
    b = http_get(s, url)
    if not b:
        return None
    soup = soup_of(b)

    title = None
    el = soup.select_one(".page_title")
    if el and el.get_text(strip=True):
        title = el.get_text(" ", strip=True)
    if not title and soup.title:
        title = soup.title.get_text(" ", strip=True)
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(" ", strip=True)
    if not title:
        return None

    pairs = parse_pairs(soup)
    vendor_code = (pairs.get("Артикул") or pairs.get("Партс-номер") or "").strip()
    if not vendor_code:
        return None  # артикул обязателен

    brand = (pairs.get("Вендор") or "").strip()

    price_int = parse_price_kzt(soup)
    if price_int is None or price_int <= 0:
        price_int = 1

    picture = first_image_url(soup)

    cat  = (pairs.get("Категория") or "").strip()
    scat = (pairs.get("Подкатегория") or "").strip()
    cats = [c for c in [cat, scat] if c]

    description = extract_description_meta(soup)

    return {
        "title": title,
        "vendorCode": vendor_code,
        "brand": brand or "",
        "price": int(price_int),
        "picture": picture,
        "url": url,
        "cats": cats,
        "description": description,
    }

# -------- categories tree (оставляем логику, но в выводе не используем) --------
def stable_cat_id(text: str, prefix: int = 9620000) -> int:
    h = hashlib.md5((text or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
    cat_map: Dict[Tuple[str,...], int] = {}
    out_list: List[Tuple[int,str,Optional[int]]] = []
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        if not clean:
            continue
        parent = ROOT_CAT_ID
        acc: List[str] = []
        for name in clean:
            acc.append(name)
            key = tuple(acc)
            if key in cat_map:
                parent = cat_map[key]
                continue
            cid = stable_cat_id(" / ".join(acc))
            cat_map[key] = cid
            out_list.append((cid, name, parent))
            parent = cid
    return out_list, cat_map

# -------- YML (очищенный вывод) --------
def build_yml(categories: List[Tuple[int,str,Optional[int]]], offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    # По задаче: <categories> и <categoryId> вообще не выводим.
    # <currencies>/<name> можно оставить, но <currencyId> внутри offer — не выводим.
    out.append("<offers>")
    for _cid, it in offers:
        # НЕ ставим атрибуты available/in_stock у offer
        out.append(f"<offer id=\"{yml_escape(it['vendorCode'])}\">")
        out.append(f"<name>{yml_escape(it['title'])}</name>")
        if it.get("brand"):
            out.append(f"<vendor>{yml_escape(it['brand'])}</vendor>")
        out.append(f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>")
        out.append(f"<price>{int(it['price'])}</price>")
        # НЕ выводим <currencyId>
        # НЕ выводим <categoryId>
        # НЕ выводим <url>
        if it.get("picture"):
            out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        if it.get("description"):
            out.append(f"<description>{yml_escape(it['description'])}</description>")
        # ЕДИНСТВЕННЫЙ тег наличия:
        out.append("<available>true</available>")
        # НЕ выводим <quantity_in_stock>, <stock_quantity>, <quantity>
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    xml = "\n".join(out)
    # Для читабельности: пустая строка между офферами
    xml = re.sub(r"(</offer>)\n(<offer\b)", r"\1\n\n\2", xml)
    return xml

# -------- MAIN --------
def main() -> int:
    s = make_session()
    if DISABLE_SSL_VERIFY:
        print("[ssl] verification disabled by env")

    if not VTT_LOGIN or not VTT_PASSWORD:
        print("[warn] Empty login/password; cannot proceed.")
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(build_yml([], []))
        return 0

    if not login_vtt(s):
        print("Error: login failed")
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(build_yml([], []))
        return 0

    categories_urls = load_categories(CATEGORIES_FILE)
    if not categories_urls:
        print("[error] categories file is empty.")
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(build_yml([], []))
        return 0

    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MIN)

    # собираем ссылки товаров со всех категорий
    prod_urls: List[str] = []
    seen: Set[str] = set()
    for cu in categories_urls:
        if datetime.utcnow() > deadline:
            break
        urls = collect_product_urls_from_category(s, cu, MAX_PAGES, deadline)
        for u in urls:
            if u not in seen:
                seen.add(u)
                prod_urls.append(u)

    print(f"[discover] product urls: {len(prod_urls)}")

    # парсим карточки
    offers: List[Tuple[int,Dict[str,Any]]] = []
    seen_offer_ids: Set[str] = set()
    all_paths: List[List[str]] = []

    def worker(u: str) -> Optional[Dict[str,Any]]:
        if datetime.utcnow() > deadline:
            return None
        try:
            return parse_product(s, u)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(worker, u) for u in prod_urls]
        for fut in as_completed(futs):
            it = fut.result()
            if not it:
                continue
            offer_id = it["vendorCode"]
            if offer_id in seen_offer_ids:
                offer_id = f"{offer_id}-{sha1(it['title'])[:6]}"
            seen_offer_ids.add(offer_id)

            cats = it.get("cats") or []
            all_paths.append(cats)

            offers.append((ROOT_CAT_ID, {
                "vendorCode": offer_id,
                "title": it["title"],
                "brand": it.get("brand") or "",
                "price": int(it["price"]) if it.get("price") else 1,
                # "url": it.get("url"),  # НЕ используем
                "picture": it.get("picture"),
                "description": it.get("description") or "",
            }))

    # Строим дерево категорий — но в build_yml оно не выводится
    categories, _path_map = build_categories(all_paths)

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(categories, offers)
    with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)} -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        try:
            os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
            with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
                f.write("<?xml version='1.0' encoding='windows-1251'?>\n<yml_catalog><shop><offers></offers></shop></yml_catalog>")
        except Exception:
            pass
        sys.exit(0)
