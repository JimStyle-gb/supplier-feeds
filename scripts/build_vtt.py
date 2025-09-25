# scripts/build_vtt.py
# -*- coding: utf-8 -*-
"""
VTT (b2b.vtt.ru) -> YML (KZT) в стиле alstyle/akcent.

ЦЕНА:
- Берём из <span class="price_main"><b>11121.48</b>...</span>
- Если цены нет или < 100 -> price = 100 (никаких правил/округлений)
- Иначе: применяем PRICING_RULES (процент + фикс) и поднимаем до ...900

ВЫВОД:
- Без <categories>, <categoryId>, <url>, любых *quantity*
- <available>true</available> для всех
- Порядок полей в <offer>: name, vendor, vendorCode, price, currencyId, picture, description, available
- Префикс vendorCode = VT + <Артикул> (нормализованный)
- Кодировка вывода: windows-1251
"""

from __future__ import annotations
import os, re, io, time, html, hashlib, math
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import requests
from bs4 import BeautifulSoup

# ---------------- ПАРАМЕТРЫ ОКРУЖЕНИЯ ----------------
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

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-Feed/akcent-pricing-1.2)"}

# ---------------- ЦЕНООБРАЗОВАНИЕ (как у akcent) ----------------
PriceRule = Tuple[int, int, float, int]  # (min_incl, max_incl, pct, add_kzt)
PRICING_RULES: List[PriceRule] = [
    (   101,    10000, 4.0,  3000),
    ( 10001,    25000, 4.0,  4000),
    ( 25001,    50000, 4.0,  5000),
    ( 50001,    75000, 4.0,  7000),
    ( 75001,   100000, 4.0, 10000),
    (100001,   150000, 4.0, 12000),
    (150001,   200000, 4.0, 15000),
    (200001,   300000, 4.0, 20000),
    (300001,   400000, 4.0, 25000),
    (400001,   500000, 4.0, 30000),
    (500001,   750000, 4.0, 40000),
    (750001,  1000000, 4.0, 50000),
    (1000001, 1500000, 4.0, 70000),
    (1500001, 2000000, 4.0, 90000),
    (2000001,100000000,4.0,100000),
]

def _round_up_tail_to_900(n: int) -> int:
    """
    Округление ВВЕРХ до ближайшего значения с окончанием ...900.
    Примеры: 10789 -> 10900; 238958 -> 239900.
    """
    thousands = (n + 999) // 1000  # ceil до следующей 1000
    return thousands * 1000 - 100   # ...900

def compute_price_from_supplier(base_price: Optional[int]) -> int:
    """
    Итоговая цена по правилам:
    - Если base_price отсутствует или < 100 -> вернуть ровно 100 (и больше ничего не делать)
    - Иначе -> применяем первую подходящую ступень PRICING_RULES и поднимаем до ...900
    """
    if base_price is None or base_price < 100:
        return 100

    # применяем таблицу
    for lo, hi, pct, add in PRICING_RULES:
        if lo <= base_price <= hi:
            raw = base_price * (1.0 + pct/100.0) + add
            return _round_up_tail_to_900(int(math.ceil(raw)))

    # страховка (если цена вне всех диапазонов)
    raw = base_price * (1.0 + PRICING_RULES[-1][2]/100.0) + PRICING_RULES[-1][3]
    return _round_up_tail_to_900(int(math.ceil(raw)))

# ---------------- УТИЛИТЫ ----------------
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

# ---------------- HTTP ----------------
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

# ---------------- АВТОРИЗАЦИЯ ----------------
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

# ---------------- КАТЕГОРИИ ----------------
def load_categories(path: str) -> List[str]:
    urls: List[str] = []
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                u = (line or "").strip()
                if u and not u.startswith("#"):
                    urls.append(u)
    return urls

def add_or_replace_page_param(u: str, page: int) -> str:
    pr = urlparse(u)
    q = dict(parse_qsl(pr.query, keep_blank_values=True))
    q["page"] = str(page)
    return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, urlencode(q, doseq=True), pr.fragment))

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

# ---------------- ПАРСИНГ КОНТЕНТА ----------------
def parse_supplier_price_from_soup(soup: BeautifulSoup) -> Optional[int]:
    """
    Строго берём цену из <span class="price_main"><b>11121.48</b>...</span>.
    Дробную часть отбрасываем. Возвращаем int KZT или None, если не нашли.
    """
    btag = soup.select_one("span.price_main > b")
    if not btag:
        return None
    raw = btag.get_text("", strip=True)
    if not raw:
        return None
    # убираем пробелы/nbsp и приводим запятую к точке
    norm = raw.replace("\u00A0", "").replace(" ", "").replace(",", ".")
    try:
        val = Decimal(norm)
    except InvalidOperation:
        return None
    return int(val)  # для положительных чисел это "floor" до целого тенге

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

def first_image_url(soup: BeautifulSoup) -> Optional[str]:
    og = soup.find("meta", attrs={"property":"og:image"})
    if og and og.get("content"):
        return abs_url(og["content"])
    for tag in soup.find_all(True):
        for attr in ("src","data-src","href","data-img","content","data-large"):
            v = tag.get(attr)
            if not v or not isinstance(v, str):
                continue
            vl = v.lower()
            if (".jpg" in vl or ".jpeg" in vl or ".png" in vl) and "/images/" in vl:
                return abs_url(v)
    return None

def parse_product(s: requests.Session, url: str) -> Optional[Dict[str,Any]]:
    jitter_sleep()
    b = http_get(s, url)
    if not b:
        return None
    soup = soup_of(b)

    # название
    title_el = (soup.select_one(".page_title") or soup.title or soup.find("h1"))
    title = title_el.get_text(" ", strip=True) if title_el else None
    if not title:
        return None

    # характеристики: ищем "Артикул", "Вендор"
    pairs = parse_pairs(soup)
    article = (pairs.get("Артикул") or pairs.get("Партс-номер") or "").strip()
    if not article:
        return None

    brand = (pairs.get("Вендор") or "").strip()

    # цена поставщика
    dealer_price = parse_supplier_price_from_soup(soup)  # int KZT или None
    final_price = compute_price_from_supplier(dealer_price)  # применяем правила/100

    # картинка и краткое описание
    picture = first_image_url(soup)
    meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    descr = meta.get("content").strip() if (meta and meta.get("content")) else ""

    return {
        "id": article,
        "vendorCode": "VT" + re.sub(r"[^\w\-]+", "", article),
        "title": title,
        "brand": brand or "",
        "price": int(final_price),
        "picture": picture,
        "description": re.sub(r"\s+", " ", descr),
    }

# ---------------- FEED_META + YML ----------------
def almaty_now_str() -> str:
    return (datetime.utcnow() + timedelta(hours=5)).strftime("%Y-%m-%d %H:%M:%S +05")

def build_feed_meta(source: str, stats: Dict[str,int]) -> str:
    pad = 24
    lines: List[str] = []
    def kv(k, v): lines.append(f"{k.ljust(pad)} = {v}")
    kv("supplier",          "VTT (VT)")
    kv("source",            source)
    kv("offers_discovered", str(stats.get("discovered", 0)))
    kv("offers_parsed",     str(stats.get("parsed", 0)))
    kv("offers_written",    str(stats.get("written", 0)))
    kv("pricing",           "akcent: rules + tail ...900; missing/<100 -> 100")
    kv("currency",          "KZT")
    kv("built_UTC",         datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"))
    kv("built_Asia/Almaty", f"{almaty_now_str()} | Время сборки (Алматы)-->")
    return "<!--FEED_META\n" + "\n".join(lines) + "\n-->"

def build_yml(offers: List[Dict[str,Any]], source: str, discovered: int, parsed: int) -> str:
    date_attr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append(f"<yml_catalog date=\"{date_attr}\">")
    out.append(build_feed_meta(source, {"discovered": discovered, "parsed": parsed, "written": len(offers)}))
    out.append("<shop>")
    out.append("  <offers>")
    for it in offers:
        out.append(f"    <offer id=\"{yml_escape(it['id'])}\">")
        out.append(f"      <name>{yml_escape(it['title'])}</name>")
        if it.get("brand"):
            out.append(f"      <vendor>{yml_escape(it['brand'])}</vendor>")
        out.append(f"      <vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>")
        out.append(f"      <price>{int(it['price'])}</price>")
        out.append("      <currencyId>KZT</currencyId>")
        if it.get("picture"):
            out.append(f"      <picture>{yml_escape(it['picture'])}</picture>")
        if it.get("description"):
            out.append(f"      <description>{yml_escape(it['description'])}</description>")
        out.append("      <available>true</available>")
        out.append("    </offer>\n")
    out.append("  </offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------------- ОСНОВНОЙ КОНТУР ----------------
def collect_all_product_urls(s: requests.Session, cats: List[str], deadline: datetime) -> List[str]:
    prod_urls: List[str] = []
    seen: Set[str] = set()
    for cu in cats:
        if datetime.utcnow() > deadline:
            break
        for u in collect_product_urls_from_category(s, cu, MAX_PAGES, deadline):
            if u not in seen:
                seen.add(u)
                prod_urls.append(u)
    return prod_urls

def main() -> int:
    s = make_session()
    if DISABLE_SSL_VERIFY:
        print("[ssl] verification disabled by env")

    if not VTT_LOGIN or not VTT_PASSWORD:
        print("[warn] Empty login/password; writing empty feed.")
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(build_yml([], START_URL, 0, 0))
        return 0

    if not login_vtt(s):
        print("Error: login failed; writing empty feed.")
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(build_yml([], START_URL, 0, 0))
        return 0

    cats = load_categories(CATEGORIES_FILE)
    if not cats:
        print("[error] categories file is empty; writing empty feed.")
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(build_yml([], START_URL, 0, 0))
        return 0

    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MIN)

    # сбор ссылок товаров
    prod_urls = collect_all_product_urls(s, cats, deadline)
    discovered = len(prod_urls)
    print(f"[discover] product urls: {discovered}")

    # парсинг карточек
    offers: List[Dict[str,Any]] = []
    seen_ids: Set[str] = set()
    parsed = 0

    def worker(u: str) -> Optional[Dict[str,Any]]:
        if datetime.utcnow() > deadline:
            return None
        try:
            return parse_product(s, u)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for fut in as_completed([ex.submit(worker, u) for u in prod_urls]):
            it = fut.result()
            parsed += 1
            if not it:
                continue
            oid = it["id"]
            if oid in seen_ids:
                oid = f"{oid}-{sha1(it['title'])[:6]}"
                it["id"] = oid
                it["vendorCode"] = "VT" + re.sub(r"[^\w\-]+", "", oid)
            seen_ids.add(oid)
            offers.append(it)

    # запись файла
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
        f.write(build_yml(offers, START_URL, discovered, parsed))
    print(f"[done] items: {len(offers)} -> {OUT_FILE}")
    return 0

# ---------------- ТОЧКА ВХОДА ----------------
if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        try:
            os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
            with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
                f.write(build_yml([], START_URL, 0, 0))
        except Exception:
            pass
        sys.exit(0)
