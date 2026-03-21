# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/source.py

VTT source layer.

Роль:
- логин / session / retry
- category-first crawl только по одобренным category URLs
- product index
- raw parse карточки товара

Без supplier business-логики:
- без OfferOut
- без clean params
- без compat merge
"""

from __future__ import annotations

import os
import random
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


DEFAULT_CATEGORY_URLS: list[str] = [
    "https://b2b.vtt.ru/catalog/?category=DRM_CRT",
    "https://b2b.vtt.ru/catalog/?category=DRM_UNIT",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_ORIG",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_COPY",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_PRINT",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_TNR",
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_PRNTHD",
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_Refill",
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_ORIG",
    "https://b2b.vtt.ru/catalog/?category=CARTMAT_CART",
    "https://b2b.vtt.ru/catalog/?category=TNR_WASTETON",
    "https://b2b.vtt.ru/catalog/?category=DEV_DEV",
    "https://b2b.vtt.ru/catalog/?category=TNR_REFILL",
    "https://b2b.vtt.ru/catalog/?category=INK_COMMON",
    "https://b2b.vtt.ru/catalog/?category=PARTSPRINT_DEVUN",
]

PRODUCT_PATH_RE = re.compile(r"^/catalog/[^/?#]+/?$", re.I)
PRICE_RUB_JS_RE = re.compile(r"\blet\s+priceRUB\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*;", re.I)
PRICE_USD_JS_RE = re.compile(r"\blet\s+priceUSD\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*;", re.I)
SKU_JS_RE = re.compile(r"\blet\s+sku\s*=\s*['\"]([^'\"]+)['\"]\s*;", re.I)
PRICE_HTML_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[ \u00A0]?\d{3})+|\d+)(?:[.,]\d{1,2})?")
BAD_IMAGE_RE = re.compile(r"(favicon|yandex|counter|watch/|pixel|metrika|doubleclick)", re.I)
LOGISTIC_KEY_RE = re.compile(
    r"(местн|москва|склад|до новой поставки|в упаковке, штук|штук$|дней$)",
    re.I,
)
CODE_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9\-./]{2,}\b")


def log(msg: str) -> None:
    print(msg, flush=True)


@dataclass(frozen=True)
class VttSourceCfg:
    base_url: str
    start_url: str
    categories: list[str]
    login: str
    password: str
    max_listing_pages: int
    max_workers: int
    max_crawl_minutes: float
    delay_ms: int
    verify: object
    softfail: bool


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name, "") or "").strip() or str(default))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name, "") or "").strip() or str(default))
    except Exception:
        return default


def cfg_from_env() -> VttSourceCfg:
    base = (os.getenv("VTT_BASE_URL", "https://b2b.vtt.ru") or "").strip().rstrip("/")
    start = (os.getenv("VTT_START_URL", f"{base}/catalog/") or "").strip()
    cats_raw = (os.getenv("VTT_CATEGORIES", "") or "").strip()
    categories = [c.strip() for c in cats_raw.split(",") if c.strip()] if cats_raw else list(DEFAULT_CATEGORY_URLS)

    ssl_verify = _env_bool("VTT_SSL_VERIFY", True)
    ca_bundle = (os.getenv("VTT_CA_BUNDLE", "") or "").strip()
    verify: object = ca_bundle if ca_bundle else ssl_verify

    return VttSourceCfg(
        base_url=base,
        start_url=start,
        categories=categories,
        login=(os.getenv("VTT_LOGIN", "") or "").strip(),
        password=(os.getenv("VTT_PASSWORD", "") or "").strip(),
        max_listing_pages=_env_int("VTT_MAX_LISTING_PAGES", 5000),
        max_workers=_env_int("VTT_MAX_WORKERS", 10),
        max_crawl_minutes=_env_float("VTT_MAX_CRAWL_MINUTES", 60.0),
        delay_ms=_env_int("VTT_REQUEST_DELAY_MS", 40),
        verify=verify,
        softfail=_env_bool("VTT_SOFTFAIL", True),
    )


def _sleep_ms(ms: int) -> None:
    if ms <= 0:
        return
    time.sleep((ms / 1000.0) * random.uniform(0.75, 1.35))


def make_session(cfg: VttSourceCfg) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            "Accept-Language": "ru,en;q=0.8",
        }
    )
    return s


def clone_session_with_cookies(src: requests.Session, cfg: VttSourceCfg) -> requests.Session:
    s2 = make_session(cfg)
    try:
        s2.cookies.update(src.cookies)
    except Exception:
        pass
    return s2


def _request(
    s: requests.Session,
    cfg: VttSourceCfg,
    method: str,
    url: str,
    *,
    timeout: int = 30,
    data: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> requests.Response | None:
    tries = 6
    for i in range(tries):
        try:
            r = s.request(
                method=method,
                url=url,
                data=data,
                headers=headers,
                timeout=timeout,
                verify=cfg.verify,
                allow_redirects=True,
            )
            if r.status_code in (500, 502, 503, 504):
                raise requests.HTTPError(str(r.status_code))
            return r
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last = i == tries - 1
            log(f"[vtt:http] {method} {url} fail: {e}{' (last)' if last else ''}")
            if last:
                return None
            time.sleep(min(10.0, 0.6 * (2 ** i)) + random.uniform(0.0, 0.4))
    return None


def _get_text(s: requests.Session, cfg: VttSourceCfg, url: str, *, timeout: int = 30) -> str | None:
    r = _request(s, cfg, "GET", url, timeout=timeout)
    if not r or r.status_code != 200:
        return None
    _sleep_ms(cfg.delay_ms)
    return r.text


def _post(
    s: requests.Session,
    cfg: VttSourceCfg,
    url: str,
    *,
    data: dict[str, Any],
    headers: dict[str, str] | None = None,
    timeout: int = 30,
) -> requests.Response | None:
    r = _request(s, cfg, "POST", url, timeout=timeout, data=data, headers=headers)
    _sleep_ms(cfg.delay_ms)
    return r


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html or "", "html.parser")


def _extract_csrf_token(html: str) -> str:
    sp = _soup(html)
    m = sp.find("meta", attrs={"name": "csrf-token"})
    return ((m.get("content") if m else "") or "").strip()


def _normalize_listing_url(url: str) -> str:
    pu = urlparse(url)
    q = parse_qs(pu.query)
    items: list[tuple[str, str]] = []
    for key in sorted(q):
        for value in sorted(q[key]):
            items.append((key, value))
    return urlunparse((pu.scheme, pu.netloc, pu.path, "", urlencode(items, doseq=True), ""))


def _abs_url(cfg: VttSourceCfg, href: str) -> str:
    return urljoin(cfg.base_url + "/", href.strip())


def login(s: requests.Session, cfg: VttSourceCfg) -> bool:
    if not cfg.login or not cfg.password:
        log("[WARN] VTT_LOGIN/VTT_PASSWORD пустые")
        return False

    home_html = _get_text(s, cfg, cfg.base_url + "/")
    if not home_html:
        return False

    csrf = _extract_csrf_token(home_html)
    headers = {"Referer": cfg.base_url + "/"}
    if csrf:
        headers["X-CSRF-TOKEN"] = csrf

    resp = _post(
        s,
        cfg,
        cfg.base_url + "/validateLogin",
        data={"login": cfg.login, "password": cfg.password},
        headers=headers,
        timeout=35,
    )
    if not resp or resp.status_code not in (200, 204):
        return False

    probe_html = _get_text(s, cfg, cfg.start_url)
    if not probe_html:
        return False
    low = probe_html.lower()
    return ("/catalog" in cfg.start_url.lower()) and ("вход для клиентов" not in low)


def category_code(category_url: str) -> str:
    q = parse_qs(urlparse(category_url).query)
    return (q.get("category", [""]) or [""])[0].strip()


def _listing_page_links(page_url: str, html: str, allowed_category_codes: set[str]) -> tuple[list[str], list[str]]:
    sp = _soup(html)
    listing_urls: list[str] = []
    product_urls: list[str] = []
    seen_listing: set[str] = set()
    seen_product: set[str] = set()

    current_q = parse_qs(urlparse(page_url).query)
    current_categories = {x.strip() for x in current_q.get("category", []) if x.strip()}

    for a in sp.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue

        abs_url = urljoin(page_url, href)
        pu = urlparse(abs_url)

        if PRODUCT_PATH_RE.match(pu.path) and not pu.query and not pu.fragment:
            if abs_url not in seen_product:
                seen_product.add(abs_url)
                product_urls.append(abs_url)
            continue

        if pu.path.lower().startswith("/catalog"):
            q = parse_qs(pu.query)
            cat_vals = {x.strip() for x in q.get("category", []) if x.strip()}
            if cat_vals and cat_vals.issubset(allowed_category_codes):
                norm = _normalize_listing_url(abs_url)
                if norm not in seen_listing:
                    seen_listing.add(norm)
                    listing_urls.append(norm)

    # подстраховка: если верстка не отдала page-link, сами крутим ?page=N через текущую категорию
    if current_categories:
        cur_page = 1
        q = parse_qs(urlparse(page_url).query)
        try:
            cur_page = int((q.get("page", ["1"]) or ["1"])[0])
        except Exception:
            cur_page = 1
        next_page_url = _normalize_listing_url(_set_q(page_url, "page", str(cur_page + 1)))
        if next_page_url not in seen_listing:
            listing_urls.append(next_page_url)

    return listing_urls, product_urls


def _set_q(url: str, key: str, value: str) -> str:
    pu = urlparse(url)
    q = parse_qs(pu.query)
    q[key] = [value]
    return urlunparse(pu._replace(query=urlencode(q, doseq=True)))


def collect_product_index(
    s: requests.Session,
    cfg: VttSourceCfg,
    category_urls: list[str] | None,
    deadline_utc: datetime,
) -> list[dict[str, Any]]:
    urls = category_urls if category_urls else list(cfg.categories)
    allowed_codes = {category_code(x) for x in urls if category_code(x)}
    pending = [_normalize_listing_url(u) for u in urls]
    seen_listing: set[str] = set()
    product_to_categories: dict[str, set[str]] = defaultdict(set)
    listing_pages_total = 0

    while pending and listing_pages_total < max(1, cfg.max_listing_pages):
        if datetime.utcnow() >= deadline_utc:
            break
        current = pending.pop(0)
        if current in seen_listing:
            continue
        seen_listing.add(current)

        html = _get_text(s, cfg, current)
        if not html:
            continue
        listing_pages_total += 1

        # category tag from current page url
        cur_code = category_code(current)
        next_listings, product_urls = _listing_page_links(current, html, allowed_codes)

        for purl in product_urls:
            if cur_code:
                product_to_categories[purl].add(cur_code)

        for nxt in next_listings:
            if nxt not in seen_listing and nxt not in pending:
                # only same approved category codes
                q = parse_qs(urlparse(nxt).query)
                codes = {x.strip() for x in q.get("category", []) if x.strip()}
                if not codes or codes.issubset(allowed_codes):
                    pending.append(nxt)

    out: list[dict[str, Any]] = []
    for url, codes in sorted(product_to_categories.items(), key=lambda kv: kv[0]):
        out.append(
            {
                "url": url,
                "category_code": sorted(list(codes))[0] if codes else "",
                "source_categories": sorted(list(codes)),
                "title": "",
            }
        )

    log(f"[vtt:site] listing_pages={listing_pages_total} product_urls={len(out)} categories={len(urls)}")
    return out


def _text(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip() if el else ""


def _extract_title(sp: BeautifulSoup) -> str:
    return _text(sp.find("h1") or sp.select_one(".page_title") or sp.title)


def _extract_meta_desc(sp: BeautifulSoup) -> str:
    meta = sp.find("meta", attrs={"name": "description"}) or sp.find("meta", attrs={"property": "og:description"})
    out = (meta.get("content") if meta else "") or ""
    return re.sub(r"\s+", " ", out).strip()


def _extract_desc_body(sp: BeautifulSoup) -> str:
    for sel in ("div.catalog_item_descr", "div.description", "article", "div.catalog_item"):
        el = sp.select_one(sel)
        if el:
            txt = _text(el)
            if txt and len(txt) >= 50:
                return txt
    return ""


def _extract_pairs(sp: BeautifulSoup) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    public_pairs: list[tuple[str, str]] = []
    stock_pairs: list[tuple[str, str]] = []
    for box in sp.select("div.description.catalog_item_descr dl.description_row"):
        dts = box.find_all("dt")
        dds = box.find_all("dd")
        for dt, dd in zip(dts, dds):
            k = _text(dt).strip(":")
            v = _text(dd)
            if not k or not v:
                continue
            if LOGISTIC_KEY_RE.search(k):
                stock_pairs.append((k, v))
            else:
                public_pairs.append((k, v))
    return public_pairs, stock_pairs


def _parse_price_num(text: str) -> int | None:
    s = (text or "").replace("\u00a0", " ").strip()
    s = re.sub(r"[^0-9.,\s]+", "", s)
    s = re.sub(r"\s+", "", s)
    if not s:
        return None
    if "." in s and "," in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")
    else:
        if "," in s and "." not in s:
            if s.count(",") == 1 and len(s.split(",")[1]) <= 2:
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
    try:
        return int(round(float(s)))
    except Exception:
        return None


def _extract_price_rub(html: str, sp: BeautifulSoup) -> int | None:
    m = PRICE_RUB_JS_RE.search(html or "")
    if m:
        return _parse_price_num(m.group(1))
    el = sp.select_one("span.price_main b") or sp.select_one(".price_main b") or sp.select_one(".price_main")
    if el:
        p = _parse_price_num(_text(el))
        if p is not None:
            return p
    for script in sp.find_all("script"):
        txt = script.get_text(" ", strip=True)
        if "priceRUB" in txt:
            m = PRICE_RUB_JS_RE.search(txt)
            if m:
                return _parse_price_num(m.group(1))
    body = _text(sp)
    if "Цена" in body:
        m = re.search(r"\bЦена\b[^\d]{0,20}([0-9][0-9\s.,]{2,})", body, re.I)
        if m:
            return _parse_price_num(m.group(1))
    return None


def _extract_js_sku(html: str, pairs: list[tuple[str, str]]) -> str:
    m = SKU_JS_RE.search(html or "")
    if m:
        return m.group(1).strip()
    for key in ("Артикул", "SKU"):
        for k, v in pairs:
            if k == key and v:
                return v.strip()
    return ""


def _extract_price_usd(html: str) -> str:
    m = PRICE_USD_JS_RE.search(html or "")
    return m.group(1).strip() if m else ""


def _extract_pictures(cfg: VttSourceCfg, sp: BeautifulSoup, limit: int = 8) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def push(val: str) -> None:
        url = _abs_url(cfg, val)
        if not url or BAD_IMAGE_RE.search(url):
            return
        if not re.search(r"\.(jpg|jpeg|png|webp|gif|bmp|tif|tiff)(\?|#|$)", url, re.I):
            return
        if url not in seen:
            seen.add(url)
            out.append(url)

    for a in sp.select("div.catalog_item_pic a[href], a.glightbox[href]"):
        href = (a.get("href") or "").strip()
        if href:
            push(href)

    for img in sp.find_all("img"):
        for attr in ("data-src", "data-original", "src", "srcset"):
            val = img.get(attr)
            if not val:
                continue
            val = str(val).split(",")[0].strip().split(" ")[0].strip()
            if val:
                push(val)

    if not out:
        out = ["https://placehold.co/800x800/png?text=No+Photo"]
    return out[:limit]


def _extract_title_codes(title: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for code in CODE_TOKEN_RE.findall(title or ""):
        c = code.strip(".-/")
        if len(c) < 3:
            continue
        if not re.search(r"\d", c):
            continue
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def parse_product_page(
    s: requests.Session,
    cfg: VttSourceCfg,
    url: str,
    *,
    category_code: str = "",
    source_categories: list[str] | None = None,
) -> dict[str, Any] | None:
    html = _get_text(s, cfg, url)
    if not html:
        return None

    sp = _soup(html)
    title = _extract_title(sp)
    if not title:
        return None

    params_pairs, stock_pairs = _extract_pairs(sp)
    sku = _extract_js_sku(html, params_pairs)
    raw_price_rub = _extract_price_rub(html, sp)

    return {
        "url": url,
        "category_code": category_code,
        "source_categories": list(source_categories or ([category_code] if category_code else [])),
        "available": True,  # по проектному правилу VT всегда true
        "name": title,
        "vendor": next((v for k, v in params_pairs if k.lower() == "вендор"), "").strip(),
        "sku": sku,
        "title_codes": _extract_title_codes(title),
        "price_rub_raw": raw_price_rub,
        "price_usd_raw": _extract_price_usd(html),
        "pictures": _extract_pictures(cfg, sp),
        "description_meta": _extract_meta_desc(sp),
        "description_body": _extract_desc_body(sp),
        "params": params_pairs,
        "stock_pairs": stock_pairs,
    }


def parse_product_page_from_index(
    s: requests.Session,
    cfg: VttSourceCfg,
    index_item: dict[str, Any],
) -> dict[str, Any] | None:
    url = str(index_item.get("url") or "").strip()
    code = str(index_item.get("category_code") or "").strip()
    source_categories = list(index_item.get("source_categories") or ([] if not code else [code]))
    if not url:
        return None
    return parse_product_page(s, cfg, url, category_code=code, source_categories=source_categories)
