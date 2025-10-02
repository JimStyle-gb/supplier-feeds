# scripts/build_vtt.py
# -*- coding: utf-8 -*-
"""
VTT (b2b.vtt.ru) -> YML (KZT)

ЦЕНА:
- Берём из <span class="price_main"><b>...</b></span>
- Если нет или < 100 → 100
- Иначе применяем PRICING_RULES (процент + фикс) и ОКРУГЛЯЕМ ВВЕРХ до ...900

ВЫВОД:
- Строгий порядок тегов в <offer>:
  <vendorCode><name><price><picture><vendor><currencyId><available><description>
- id = vendorCode = "VT" + нормализованный артикул
- <available>true</available> всем
- FEED_META как в feed.txt (Поле | значение) + ближайшее окно 1/10/20 в 05:00 (Алматы)
- Кодировка выхода: windows-1251
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

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0").lower() in {"1","true","yes"}
ALLOW_SSL_FALLBACK = os.getenv("ALLOW_SSL_FALLBACK", "0").lower() in {"1","true","yes"}

HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS= int(os.getenv("REQUEST_DELAY_MS", "120"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "600"))

MAX_PAGES       = int(os.getenv("MAX_PAGES", "800"))
MAX_CRAWL_MIN   = int(os.getenv("MAX_CRAWL_MINUTES", "60"))
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", "6"))

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-Feed/1.0)"}

# ---------------- ЦЕНООБРАЗОВАНИЕ ----------------
PriceRule = Tuple[int, int, float, int]
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

def _round_up_tail_900(n: int) -> int:
    """Округление ВВЕРХ до ближайшего значения заканчивающегося на ...900."""
    thousands = (n + 999) // 1000
    return thousands * 1000 - 100

def compute_price_from_supplier(base_price: Optional[int]) -> int:
    if base_price is None or base_price < 100:
        return 100
    for lo, hi, pct, add in PRICING_RULES:
        if lo <= base_price <= hi:
            raw = base_price * (1.0 + pct/100.0) + add
            return _round_up_tail_900(int(math.ceil(raw)))
    raw = base_price * (1.0 + PRICING_RULES[-1][2]/100.0) + PRICING_RULES[-1][3]
    return _round_up_tail_900(int(math.ceil(raw)))

# ---------------- УТИЛИТЫ ----------------
def jitter_sleep() -> None:
    time.sleep(max(0.0, REQUEST_DELAY_MS/1000.0))

def yml_escape(s: str) -> str:
    return html.escape((s or "").strip())

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()

def abs_url(u: str) -> str:
    return urljoin(BASE_URL + "/", (u or "").strip())

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

# ---------------- HTTP ----------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    s.verify = not DISABLE_SSL_VERIFY
    if VTT_LOGIN or VTT_PASSWORD:
        s.auth = (VTT_LOGIN, VTT_PASSWORD)
    return s

def http_get(s: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = s.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            return None
        b = r.content
        if b and len(b) < MIN_BYTES:
            return b
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

# ---------------- LOGIN ----------------
def login_vtt(s: requests.Session) -> bool:
    # Главная + попытка POST /validateLogin (если нужна)
    _ = http_get(s, BASE_URL + "/")
    data = {"login": VTT_LOGIN, "password": VTT_PASSWORD, "remember": "1"}
    try:
        r = s.post(BASE_URL + "/validateLogin", data=data, timeout=HTTP_TIMEOUT, allow_redirects=True, verify=s.verify)
        if r.status_code not in (200, 302):
            return False
    except Exception:
        return False
    # Проверим доступ к каталогу
    return http_get(s, START_URL) is not None

# ---------------- КАТАЛОГ ----------------
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

def collect_product_urls_from_category(s: requests.Session, cat_url: str, max_pages: int, deadline: float) -> List[str]:
    urls: List[str] = []
    seen: Set[str] = set()
    for i in range(1, max_pages+1):
        if time.time() > deadline:
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
            if "btn_pic" in (a.get("class") or []):
                continue
            absu = abs_url(href)
            if A_PROD.match(absu) and absu not in seen:
                seen.add(absu)
                urls.append(absu); found_here += 1
        if found_here == 0:
            break
    return urls

# ---------------- ПАРСИНГ ТОВАРА ----------------
def parse_supplier_price_from_soup(soup: BeautifulSoup) -> Optional[int]:
    """
    Цена поставщика с карточки: <span class="price_main"><b>11121.48</b>...</span>
    """
    btag = soup.select_one("span.price_main > b")
    if not btag:
        return None
    raw = (btag.get_text() or "").strip()
    if not raw:
        return None
    norm = raw.replace("\u00A0", "").replace(" ", "").replace(",", ".")
    try:
        val = Decimal(norm)
    except InvalidOperation:
        return None
    return int(val) if val > 0 else None

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

def normalize_vendor(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return v
    key = re.sub(r"[^\w]+", "", v, flags=re.IGNORECASE).lower()
    alias = {
        "kyoceramita": "Kyocera",
        "samsungbyhp": "Samsung",
    }
    return alias.get(key, v)

def parse_product(s: requests.Session, url: str) -> Optional[Dict[str,Any]]:
    jitter_sleep()
    b = http_get(s, url)
    if not b:
        return None
    soup = soup_of(b)

    # name
    title_el = (soup.select_one(".page_title") or soup.title or soup.find("h1"))
    title = (title_el.get_text(" ", strip=True) if title_el else "").strip()
    if not title:
        return None

    # артикул / бренд
    pairs = parse_pairs(soup)
    article = (pairs.get("Артикул") or pairs.get("Партс-номер") or "").strip()
    if not article:
        return None
    vendor = normalize_vendor((pairs.get("Вендор") or "").strip())

    # цены
    dealer_price = parse_supplier_price_from_soup(soup)
    final_price  = compute_price_from_supplier(dealer_price)

    # картинка и описание
    picture = first_image_url(soup) or ""
    meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    descr = (meta.get("content").strip() if (meta and meta.get("content")) else "")

    # id = vendorCode = VT + нормализованный артикул
    article_clean = re.sub(r"[^\w\-]+", "", article)
    vendor_code = f"VT{article_clean}"

    return {
        "id": vendor_code,
        "vendorCode": vendor_code,
        "title": title,
        "vendor": vendor,
        "price": int(final_price),
        "picture": picture,
        "description": re.sub(r"\s+", " ", descr).strip(),
        "dealer_price": dealer_price,
    }

# ---------------- FEED_META (Поле | значение) ----------------
def _alm_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5)  # Asia/Almaty ~ UTC+5

def _next_1_10_20_at_05() -> datetime:
    now = _alm_now()
    cands = []
    for d in (1, 10, 20):
        try:
            cands.append(now.replace(day=d, hour=5, minute=0, second=0, microsecond=0))
        except ValueError:
            pass
    future = [t for t in cands if t > now]
    if future:
        return min(future)
    # ближайшее 1-е следующего месяца 05:00
    if now.month == 12:
        return now.replace(year=now.year+1, month=1, day=1, hour=5, minute=0, second=0, microsecond=0)
    return (now.replace(day=1, hour=5, minute=0, second=0, microsecond=0) + timedelta(days=32)).replace(day=1)

def _fmt_alm(dt: datetime) -> str:
    return dt.strftime("%d:%m:%Y - %H:%M:%S")

def render_feed_meta_comment(source: str, offers_total: int, offers_written: int) -> str:
    rows = [
        ("Поставщик", "vtt"),
        ("URL поставщика", source),
        ("Время сборки (Алматы)", _fmt_alm(_alm_now())),
        ("Ближайшее время сборки (Алматы)", _fmt_alm(_next_1_10_20_at_05())),
        ("Сколько товаров у поставщика до фильтра", str(offers_total)),
        ("Сколько товаров у поставщика после фильтра", str(offers_written)),
        ("Сколько товаров есть в наличии (true)", str(offers_written)),
        ("Сколько товаров нет в наличии (false)", "0"),
    ]
    key_w = max(len(k) for k,_ in rows)
    lines = ["<!--FEED_META"]
    for i,(k,v) in enumerate(rows):
        end = " -->" if i == len(rows)-1 else ""
        lines.append(f"{k.ljust(key_w)} | {v}{end}")
    return "\n".join(lines)

# ---------------- СБОР YML ----------------
def build_yml(offers: List[Dict[str,Any]], source: str, offers_total: int) -> str:
    date_attr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append(f"<yml_catalog date=\"{date_attr}\">")
    out.append(render_feed_meta_comment(source, offers_total, len(offers)))
    out.append("<shop>")
    out.append("  <offers>")
    for it in offers:
        out.append(f"    <offer id=\"{yml_escape(it['id'])}\">")
        # ----- СТРОГИЙ ПОРЯДОК ТЕГОВ -----
        out.append(f"      <vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>")
        out.append(f"      <name>{yml_escape(it['title'])}</name>")
        out.append(f"      <price>{int(it['price'])}</price>")
        if it.get("picture"):
            out.append(f"      <picture>{yml_escape(it['picture'])}</picture>")
        if it.get("vendor"):
            out.append(f"      <vendor>{yml_escape(it['vendor'])}</vendor>")
        out.append("      <currencyId>KZT</currencyId>")
        out.append("      <available>true</available>")
        if it.get("description"):
            out.append(f"      <description>{yml_escape(it['description'])}</description>")
        # ---------------------------------
        out.append("    </offer>\n")
    out.append("  </offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------------- ОСНОВНОЙ КОНТУР ----------------
def collect_all_product_urls(s: requests.Session, cats: List[str], deadline_ts: float) -> List[str]:
    urls: List[str] = []
    seen: Set[str] = set()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(collect_product_urls_from_category, s, cu, MAX_PAGES, deadline_ts) for cu in cats]
        for f in as_completed(futs):
            try:
                for u in f.result():
                    if u not in seen:
                        seen.add(u); urls.append(u)
            except Exception:
                pass
    return urls

def main() -> int:
    s = make_session()

    if not VTT_LOGIN or not VTT_PASSWORD:
        os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
        with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(build_yml([], START_URL, 0))
        print("[warn] VTT_LOGIN/PASSWORD not set; wrote empty feed.")
        return 0

    if not login_vtt(s):
        os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
        with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(build_yml([], START_URL, 0))
        print("[error] login failed; wrote empty feed.")
        return 0

    cats = load_categories(CATEGORIES_FILE)
    if not cats:
        os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
        with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
            f.write(build_yml([], START_URL, 0))
        print("[error] categories file is empty; wrote empty feed.")
        return 0

    deadline_ts = time.time() + MAX_CRAWL_MIN*60
    prod_urls = collect_all_product_urls(s, cats, deadline_ts)
    offers_total = len(prod_urls)
    print(f"[discover] product urls: {offers_total}")

    offers: List[Dict[str,Any]] = []
    seen_ids: Set[str] = set()

    def worker(u: str) -> Optional[Dict[str,Any]]:
        if time.time() > deadline_ts:
            return None
        try:
            return parse_product(s, u)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for fut in as_completed([ex.submit(worker, u) for u in prod_urls]):
            it = fut.result()
            if not it:
                continue
            oid = it["id"]
            if oid in seen_ids:
                oid = f"{oid}-{sha1(it['title'])[:6]}"
                it["id"] = oid
            seen_ids.add(oid)
            offers.append(it)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
        f.write(build_yml(offers, START_URL, offers_total))

    print(f"[done] items: {len(offers)} -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        try:
            os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
            with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
                f.write(build_yml([], START_URL, 0))
        except Exception:
            pass
        sys.exit(0)
