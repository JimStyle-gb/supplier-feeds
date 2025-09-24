# scripts/build_vtt.py
# -*- coding: utf-8 -*-
"""
VTT (b2b.vtt.ru) -> YML (KZT) под общий шаблон

Что делает:
- Удаляет из вывода: <categories>, <categoryId>, <quantity>, <stock_quantity>, <quantity_in_stock>, <url>.
- В каждом <offer>: один <available>true</available>.
- <currencyId>KZT</currencyId> — внутри каждого оффера.
- <vendorCode> — с префиксом VT (например, артикул '12345' -> 'VT12345'); id оффера = артикул (без префикса).
- Добавлен блок FEED_META (как у alstyle/akcent).
- Ценовые правила (как у других поставщиков): %
  наценка, фиксированная прибавка, минимальная цена, округление.

ENV (можно не трогать — стоят адекватные дефолты):
  BASE_URL, START_URL, OUT_FILE, OUTPUT_ENCODING
  VTT_LOGIN, VTT_PASSWORD
  CATEGORIES_FILE
  HTTP_TIMEOUT, REQUEST_DELAY_MS, MIN_BYTES
  MAX_PAGES, MAX_CRAWL_MINUTES, MAX_WORKERS
  DISABLE_SSL_VERIFY=0/1, ALLOW_SSL_FALLBACK=0/1

ЦЕНОВЫЕ ПРАВИЛА (ENV):
  PRICE_MARKUP_PCT   — наценка в %, целое/float. По умолчанию "0".
  PRICE_ADD_KZT      — прибавка в тенге (до округления). По умолчанию "0".
  PRICE_MIN_KZT      — нижний порог (минимальная цена). По умолчанию "1".
  PRICE_ROUND_STEP   — шаг округления (например 10 или 100). По умолчанию "10".
  PRICE_ROUND_MODE   — режим округления: "up" (вверх) или "nearest" (к ближайшему). По умолчанию "nearest".
"""

from __future__ import annotations
import os, re, io, time, html, hashlib, math
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

# ЦЕНОВЫЕ ПРАВИЛА
PRICE_MARKUP_PCT = float(os.getenv("PRICE_MARKUP_PCT", "0"))     # наценка %, например 7.5
PRICE_ADD_KZT    = float(os.getenv("PRICE_ADD_KZT", "0"))        # фикс. прибавка
PRICE_MIN_KZT    = float(os.getenv("PRICE_MIN_KZT", "1"))        # минимальная цена
PRICE_ROUND_STEP = int(os.getenv("PRICE_ROUND_STEP", "10"))      # шаг округления (10/100)
PRICE_ROUND_MODE = os.getenv("PRICE_ROUND_MODE", "nearest").strip().lower()  # "nearest" или "up"

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-Feed/2.1)"}

def jitter_sleep() -> None:
    """Небольшая пауза между запросами, чтобы не душить сайт."""
    time.sleep(max(0.0, REQUEST_DELAY_MS/1000.0))

def yml_escape(s: str) -> str:
    """Экранирование XML-спецсимволов."""
    return html.escape(s or "")

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()

def abs_url(u: str) -> str:
    return urljoin(BASE_URL + "/", (u or "").strip())

def to_float(s: str) -> Optional[float]:
    """Парсинг числа из '12 345,67' / '12,345.67' и т.п."""
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
    """Логин в b2b.vtt.ru. Возвращает True при успехе."""
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
    """Список URL категорий для обхода."""
    out: List[str] = []
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                u = (line or "").strip()
                if u and not u.startswith("#"):
                    out.append(u)
    return out

def add_or_replace_page_param(u: str, page: int) -> str:
    """Подменяем/добавляем ?page=N в URL."""
    pr = urlparse(u)
    q = dict(parse_qsl(pr.query, keep_blank_values=True))
    q["page"] = str(page)
    new_q = urlencode(q, doseq=True)
    return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))

# -------- discover links per category --------
A_PROD = re.compile(r"^https?://[^/]+/catalog/[^/?#]+")

def collect_product_urls_from_category(s: requests.Session, cat_url: str, max_pages: int, deadline: datetime) -> List[str]:
    """Собираем все ссылки на карточки товара в категории (педжинация)."""
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
    """Цена в тенге с карточки товара."""
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
    """URL главной картинки товара."""
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
    """Чтение пар характеристик (dt/dd) из блока описания."""
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
    """Короткое описание из meta."""
    tag = soup.find("meta", attrs={"name": "description"})
    if tag and tag.get("content"):
        return re.sub(r"\s+", " ", tag["content"].strip())
    tag = soup.find("meta", attrs={"property": "og:description"})
    if tag and tag.get("content"):
        return re.sub(r"\s+", " ", tag["content"].strip())
    return ""

def normalize_vendor_code(raw: str) -> str:
    """Нормализация артикула для vendorCode (без пробелов/мусора)."""
    s = re.sub(r"\s+", "", raw or "")
    s = re.sub(r"[^\w\-]+", "", s)
    return s

def parse_product(s: requests.Session, url: str) -> Optional[Dict[str,Any]]:
    """Парсинг карточки товара VTT."""
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
    article = (pairs.get("Артикул") or pairs.get("Партс-номер") or "").strip()
    if not article:
        return None  # артикул обязателен

    brand = (pairs.get("Вендор") or "").strip()

    price_src = parse_price_kzt(soup)
    if price_src is None or price_src <= 0:
        price_src = 1

    picture = first_image_url(soup)
    description = extract_description_meta(soup)

    return {
        "id": article,                                            # id оффера = артикул поставщика
        "vendorCode": "VT" + normalize_vendor_code(article),      # префикс VT
        "title": title,
        "brand": brand or "",
        "price_src": int(price_src),                              # базовая цена до правил
        "picture": picture,
        "description": description,
    }

# -------- pricing --------
def apply_price_rules(src_price: int) -> int:
    """Ценовые правила: наценка %, прибавка, минимум, округление."""
    p = float(src_price)
    if PRICE_MARKUP_PCT:
        p *= (1.0 + PRICE_MARKUP_PCT/100.0)
    if PRICE_ADD_KZT:
        p += PRICE_ADD_KZT
    if PRICE_MIN_KZT:
        p = max(p, PRICE_MIN_KZT)
    step = max(1, int(PRICE_ROUND_STEP or 1))
    if PRICE_ROUND_MODE == "up":
        p = step * math.ceil(p / step)
    else:  # nearest
        p = step * round(p / step)
    return int(p)

# -------- FEED_META --------
def almaty_now_str() -> str:
    """Время Алматы (+05) строкой."""
    now_utc = datetime.utcnow()
    return (now_utc + timedelta(hours=5)).strftime("%Y-%m-%d %H:%M:%S +05")

def build_feed_meta(source: str, stats: Dict[str,int], pricing: Dict[str,str]) -> str:
    """Формирует выровненный блок комментария FEED_META в стиле alstyle/akcent."""
    lines = []
    pad = 24  # ширина ключа до '='
    def kv(k, v): lines.append(f"{k.ljust(pad)} = {v}")
    kv("supplier",               "VTT (VT)")
    kv("source",                 source)
    kv("offers_discovered",      str(stats.get("discovered", 0)))
    kv("offers_parsed",          str(stats.get("parsed", 0)))
    kv("offers_written",         str(stats.get("written", 0)))
    kv("prices_updated",         str(stats.get("priced", 0)))
    kv("removed_tags",           "categories, categoryId, quantity*, url")
    kv("currency",               "KZT")
    kv("price_rules",            f"+{pricing['pct']}% ; +{pricing['add']}₸ ; min={pricing['min']}₸ ; round {pricing['mode']} {pricing['step']}")
    kv("built_UTC",              datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"))
    kv("built_Asia/Almaty",      f"{almaty_now_str()} | Время сборки (Алматы)-->")
    # Собираем в многострочный комментарий
    return "\n<!--FEED_META\n" + "\n".join(lines) + "\n"

# -------- YML (очищенный вывод) --------
def build_yml(offers: List[Dict[str,Any]], source: str, priced_count: int, discovered: int, parsed: int) -> str:
    """Сборка финального YML."""
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")

    # FEED_META (как у alstyle/akcent) — комментарий перед <shop>
    meta = build_feed_meta(
        source=source,
        stats={"discovered": discovered, "parsed": parsed, "written": len(offers), "priced": priced_count},
        pricing={
            "pct": str(PRICE_MARKUP_PCT).rstrip("0").rstrip(".") if "." in str(PRICE_MARKUP_PCT) else str(int(PRICE_MARKUP_PCT)),
            "add": str(int(PRICE_ADD_KZT)) if PRICE_ADD_KZT == int(PRICE_ADD_KZT) else str(PRICE_ADD_KZT),
            "min": str(int(PRICE_MIN_KZT)) if PRICE_MIN_KZT == int(PRICE_MIN_KZT) else str(PRICE_MIN_KZT),
            "step": str(PRICE_ROUND_STEP),
            "mode": PRICE_ROUND_MODE,
        }
    )
    out.append(meta + "<shop>")

    # По задаче: <categories>/<categoryId> не выводим
    out.append("<offers>")
    for it in offers:
        out.append(f"<offer id=\"{yml_escape(it['id'])}\">")
        out.append(f"<name>{yml_escape(it['title'])}</name>")
        if it.get("brand"):
            out.append(f"<vendor>{yml_escape(it['brand'])}</vendor>")
        out.append(f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>")
        out.append(f"<price>{int(it['price'])}</price>")
        out.append("<currencyId>KZT</currencyId>")
        if it.get("picture"):
            out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        if it.get("description"):
            # одно-строчное описание без лишних пробелов
            out.append(f"<description>{yml_escape(re.sub(r'\\s+', ' ', it['description']).strip())}</description>")
        out.append("<available>true</available>")
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
    source = START_URL

    if not VTT_LOGIN or not VTT_PASSWORD:
        # Пишем пустую структуру с FEED_META (чтобы пайплайн не падал)
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        xml = build_yml([], source, priced_count=0, discovered=0, parsed=0)
        with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(xml)
        print("[warn] Empty login/password; wrote empty feed.")
        return 0

    if not login_vtt(s):
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        xml = build_yml([], source, priced_count=0, discovered=0, parsed=0)
        with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(xml)
        print("Error: login failed, wrote empty feed.")
        return 0

    categories_urls = load_categories(CATEGORIES_FILE)
    if not categories_urls:
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        xml = build_yml([], source, priced_count=0, discovered=0, parsed=0)
        with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(xml)
        print("[error] categories file is empty; wrote empty feed.")
        return 0

    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MIN)

    # 1) собираем ссылки товаров
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

    discovered = len(prod_urls)
    print(f"[discover] product urls: {discovered}")

    # 2) парсим карточки
    parsed = 0
    priced_count = 0
    offers: List[Dict[str,Any]] = []
    seen_ids: Set[str] = set()

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
            parsed += 1
            # применяем ценовые правила
            src_p = it["price_src"]
            new_p = apply_price_rules(src_p)
            if new_p != src_p:
                priced_count += 1
            it["price"] = new_p

            oid = it["id"]
            if oid in seen_ids:
                oid = f"{oid}-{sha1(it['title'])[:6]}"
                it["id"] = oid
                it["vendorCode"] = "VT" + normalize_vendor_code(oid)
            seen_ids.add(oid)
            offers.append(it)

    # 3) сборка YML + FEED_META
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(offers, source, priced_count=priced_count, discovered=discovered, parsed=parsed)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
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
            with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
                f.write("<?xml version='1.0' encoding='windows-1251'?>\n<yml_catalog><!--FEED_META\nsupplier                = VTT (VT)\nsource                  = (error)\noffers_discovered       = 0\noffers_parsed           = 0\noffers_written          = 0\nprices_updated          = 0\nremoved_tags            = categories, categoryId, quantity*, url\ncurrency                = KZT\nprice_rules             = +0% ; +0₸ ; min=1₸ ; round nearest 10\nbuilt_UTC               = %s UTC\nbuilt_Asia/Almaty       = %s | Время сборки (Алматы)-->\n<shop><offers></offers></shop></yml_catalog>" % (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), (datetime.utcnow()+timedelta(hours=5)).strftime("%Y-%m-%d %H:%M:%S +05")))
        except Exception:
            pass
        sys.exit(0)
