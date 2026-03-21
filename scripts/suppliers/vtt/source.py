# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/source.py

VTT source layer.
v5:
- category-first discovery only;
- lighter and faster listing parsing by regex href scan;
- product page parsing keeps the same result logic but avoids heavy generic work;
- pulls name / sku / priceRUB / pictures / params / description blocks for builder.
"""

from __future__ import annotations

import html as ihtml
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from html import unescape
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

DEFAULT_CATEGORY_CODES: list[str] = [
    "DRM_CRT",
    "DRM_UNIT",
    "CARTLAS_ORIG",
    "CARTLAS_COPY",
    "CARTLAS_PRINT",
    "CARTLAS_TNR",
    "CARTINJ_PRNTHD",
    "CARTINJ_Refill",
    "CARTINJ_ORIG",
    "CARTMAT_CART",
    "TNR_WASTETON",
    "DEV_DEV",
    "TNR_REFILL",
    "INK_COMMON",
    "PARTSPRINT_DEVUN",
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_HREF_RE = re.compile(r'''href=["']([^"']+)["']''', re.I)
_META_CSRF_RE = re.compile(r'''<meta[^>]+name=["']csrf-token["'][^>]+content=["']([^"']+)["']''', re.I)
_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.I | re.S)
_META_DESC_RE = re.compile(r'''<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["']''', re.I)
_SKU_RE = re.compile(r'''let\s+sku\s*=\s*["']([^"']+)["']''', re.I)
_PRICE_RUB_RE = re.compile(r'''let\s+priceRUB\s*=\s*([0-9]+(?:\.[0-9]+)?)''', re.I)
_PRICE_MAIN_RE = re.compile(r'''price_main[^>]*>\s*<b>([^<]+)</b>''', re.I | re.S)
_IMAGE_RE = re.compile(r'''(?:src|href|data-src|data-original|srcset)=["']([^"']+\.(?:jpg|jpeg|png|webp|gif)(?:\?[^"']*)?)["']''', re.I)
_CODE_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9\-./]{2,}\b")
_BAD_IMAGE_RE = re.compile(r"(favicon|yandex|counter|watch/|pixel|metrika|doubleclick)", re.I)

def _product_path_re(path: str) -> bool:
    return bool(re.match(r"^/catalog/[^/?#]+/?$", path or "", re.I))

@dataclass(slots=True)
class VTTConfig:
    base_url: str
    start_url: str
    login_url: str
    login: str
    password: str
    timeout_s: int = 40
    request_delay_ms: int = 10
    max_listing_pages: int = 5000
    max_workers: int = 14
    max_crawl_minutes: float = 90.0
    softfail: bool = False
    categories: list[str] = field(default_factory=lambda: list(DEFAULT_CATEGORY_CODES))

def log(msg: str) -> None:
    print(msg, flush=True)

def _sleep_ms(ms: int) -> None:
    if ms > 0:
        time.sleep(ms / 1000.0)

def _norm_ws(text: str) -> str:
    return " ".join(str(text or "").replace("\xa0", " ").split()).strip()

def _html_text(fragment: str) -> str:
    return _norm_ws(BeautifulSoup(fragment or "", "html.parser").get_text(" ", strip=True))

def _canon_vendor(vendor: str) -> str:
    v = _norm_ws(vendor)
    low = v.casefold()
    mapping = {
        "kyocera-mita": "Kyocera",
        "kyocera mita": "Kyocera",
        "konica-minolta": "Konica Minolta",
        "konica minolta": "Konica Minolta",
        "hewlett-packard": "HP",
        "hewlett packard": "HP",
    }
    return mapping.get(low, v)

def _normalize_listing_url(url: str) -> str:
    p = urlparse(url)
    qs = parse_qs(p.query)
    items: list[tuple[str, str]] = []
    for key in sorted(qs):
        for value in sorted(qs[key]):
            items.append((key, value))
    return urlunparse((p.scheme, p.netloc, p.path, "", urlencode(items, doseq=True), ""))

def _mk_category_url(base_url: str, code: str) -> str:
    return urljoin(base_url, f"/catalog/?category={code}")

def _safe_int_from_text(text: str) -> int:
    s = _norm_ws(text).replace(" ", "").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return 0
    try:
        return int(round(float(m.group(1))))
    except Exception:
        return 0

def make_session(cfg: VTTConfig) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": UA,
            "Accept-Language": "ru,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return s

def clone_session_with_cookies(master: requests.Session, cfg: VTTConfig) -> requests.Session:
    s = make_session(cfg)
    s.cookies.update(master.cookies)
    return s

def cfg_from_env() -> VTTConfig:
    base_url = (os.getenv("VTT_BASE_URL") or "https://b2b.vtt.ru/").strip()
    base_url = base_url.rstrip("/") + "/"
    cats = [x.strip() for x in (os.getenv("VTT_CATEGORY_CODES") or "").split(",") if x.strip()]
    if not cats:
        cats = list(DEFAULT_CATEGORY_CODES)
    return VTTConfig(
        base_url=base_url,
        start_url=urljoin(base_url, "/catalog/"),
        login_url=urljoin(base_url, "/validateLogin"),
        login=(os.getenv("VTT_LOGIN") or "").strip(),
        password=(os.getenv("VTT_PASSWORD") or "").strip(),
        timeout_s=int((os.getenv("VTT_TIMEOUT_S") or "40").strip() or "40"),
        request_delay_ms=int((os.getenv("VTT_REQUEST_DELAY_MS") or "10").strip() or "10"),
        max_listing_pages=int((os.getenv("VTT_MAX_LISTING_PAGES") or "5000").strip() or "5000"),
        max_workers=int((os.getenv("VTT_MAX_WORKERS") or "14").strip() or "14"),
        max_crawl_minutes=float((os.getenv("VTT_MAX_CRAWL_MINUTES") or "90").strip() or "90"),
        softfail=(os.getenv("VTT_SOFTFAIL") or "false").strip().lower() == "true",
        categories=cats,
    )

def _get(sess: requests.Session, cfg: VTTConfig, url: str) -> requests.Response:
    _sleep_ms(cfg.request_delay_ms)
    resp = sess.get(url, timeout=cfg.timeout_s, allow_redirects=True)
    resp.raise_for_status()
    return resp

def _post(sess: requests.Session, cfg: VTTConfig, url: str, **kwargs) -> requests.Response:
    _sleep_ms(cfg.request_delay_ms)
    resp = sess.post(url, timeout=cfg.timeout_s, allow_redirects=True, **kwargs)
    resp.raise_for_status()
    return resp

def login(sess: requests.Session, cfg: VTTConfig) -> bool:
    if not cfg.login or not cfg.password:
        return False
    home = _get(sess, cfg, urljoin(cfg.base_url, "/"))
    html = home.text or ""
    m = _META_CSRF_RE.search(html)
    token = m.group(1).strip() if m else ""
    headers = {"Referer": urljoin(cfg.base_url, "/")}
    if token:
        headers["X-CSRF-TOKEN"] = token
    _post(sess, cfg, cfg.login_url, data={"login": cfg.login, "password": cfg.password}, headers=headers)
    try:
        chk = _get(sess, cfg, cfg.start_url)
        body = chk.text or ""
        return ("/login" not in chk.url.lower()) and ("Вход для клиентов" not in body)
    except Exception:
        return False

def collect_product_index(sess: requests.Session, cfg: VTTConfig, categories: list[str], deadline: datetime) -> list[dict[str, Any]]:
    queue = [_normalize_listing_url(_mk_category_url(cfg.base_url, code)) for code in categories]
    seen_listings: set[str] = set()
    product_to_categories: dict[str, set[str]] = {}
    while queue and len(seen_listings) < cfg.max_listing_pages and datetime.utcnow() < deadline:
        url = queue.pop(0)
        if url in seen_listings:
            continue
        seen_listings.add(url)
        try:
            resp = _get(sess, cfg, url)
            html = resp.text or ""
        except Exception as exc:
            log(f"[VTT] listing error: {url} :: {exc}")
            continue
        current_cats = {x.strip() for x in parse_qs(urlparse(resp.url).query).get("category", []) if x.strip()}
        for href in _HREF_RE.findall(html):
            href = unescape((href or "").strip())
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            abs_url = urljoin(resp.url, href)
            p = urlparse(abs_url)
            if p.netloc != urlparse(cfg.base_url).netloc:
                continue
            if p.path.lower().startswith("/catalog/") and not p.query and _product_path_re(p.path):
                rec = product_to_categories.setdefault(abs_url, set())
                rec.update(current_cats)
                continue
            if p.path.lower().startswith("/catalog"):
                qs = parse_qs(p.query)
                cat_values = {x.strip() for x in qs.get("category", []) if x.strip()}
                if cat_values and cat_values.issubset(set(categories)):
                    norm = _normalize_listing_url(abs_url)
                    if norm not in seen_listings and norm not in queue:
                        queue.append(norm)
    return [{"url": url, "source_categories": sorted(list(cats))} for url, cats in sorted(product_to_categories.items(), key=lambda kv: kv[0])]

def _extract_title(html: str) -> str:
    m = _H1_RE.search(html)
    if m:
        return _html_text(m.group(1))
    m = _TITLE_RE.search(html)
    return _html_text(m.group(1)) if m else ""

def _extract_meta_desc(html: str) -> str:
    m = _META_DESC_RE.search(html)
    return _norm_ws(ihtml.unescape(m.group(1))) if m else ""

def _extract_price_rub(html: str) -> int:
    m = _PRICE_RUB_RE.search(html)
    if m:
        try:
            return int(round(float(m.group(1))))
        except Exception:
            pass
    m = _PRICE_MAIN_RE.search(html)
    return _safe_int_from_text(m.group(1)) if m else 0

def _extract_sku(html: str) -> str:
    m = _SKU_RE.search(html)
    return _norm_ws(m.group(1)) if m else ""

def _extract_images_from_html(page_url: str, html: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in _IMAGE_RE.findall(html or ""):
        url = urljoin(page_url, raw.strip())
        if _BAD_IMAGE_RE.search(url):
            continue
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out

def _extract_params_and_desc(html: str) -> tuple[list[tuple[str, str]], str]:
    soup = BeautifulSoup(html, "html.parser")
    params: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for box in soup.select("div.description.catalog_item_descr, div.description"):
        dts = box.find_all("dt")
        dds = box.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = _norm_ws(dt.get_text(" ", strip=True)).strip(":")
            val = _norm_ws(dd.get_text(" ", strip=True))
            if key and val and (key, val) not in seen:
                seen.add((key, val))
                params.append((key, val))
    if not params:
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                key = _norm_ws(cells[0].get_text(" ", strip=True)).strip(":")
                val = _norm_ws(cells[1].get_text(" ", strip=True))
                if key and val and (key, val) not in seen:
                    seen.add((key, val))
                    params.append((key, val))
    desc_parts: list[str] = []
    for sel in [
        "div.description.catalog_item_descr",
        "div.description",
        "div[itemprop='description']",
        ".product-description",
        ".tab-description",
    ]:
        box = soup.select_one(sel)
        if box:
            txt = _norm_ws(box.get_text(" ", strip=True))
            if txt:
                desc_parts.append(txt)
                break
    return params, "\n".join(desc_parts)

def _extract_title_codes(title: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for code in _CODE_TOKEN_RE.findall(title or ""):
        code = code.strip(".-/")
        if len(code) < 3:
            continue
        if not re.search(r"\d", code):
            continue
        if code not in seen:
            seen.add(code)
            out.append(code)
    return out

def _extract_vendor_from_title(title: str) -> str:
    upper = f" {title.upper()} "
    for vendor in (
        "HP", "CANON", "XEROX", "BROTHER", "KYOCERA", "SAMSUNG", "EPSON", "RICOH",
        "KONICA MINOLTA", "PANTUM", "LEXMARK", "OKI", "SHARP", "PANASONIC",
        "TOSHIBA", "DEVELOP", "GESTETNER", "RISO",
    ):
        if f" {vendor} " in upper:
            return _canon_vendor(vendor.title() if vendor != "HP" else vendor)
    return ""

def parse_product_page_from_index(sess: requests.Session, cfg: VTTConfig, item: dict[str, Any]) -> dict[str, Any] | None:
    url = _norm_ws(item.get("url"))
    if not url:
        return None
    resp = _get(sess, cfg, url)
    html = resp.text or ""
    title = _extract_title(html)
    if not title:
        return None
    params, desc_body = _extract_params_and_desc(html)
    return {
        "url": resp.url,
        "name": title,
        "vendor": _extract_vendor_from_title(title),
        "sku": _extract_sku(html),
        "price_rub_raw": _extract_price_rub(html),
        "pictures": _extract_images_from_html(resp.url, html),
        "params": params,
        "description_meta": _extract_meta_desc(html),
        "description_body": desc_body,
        "title_codes": _extract_title_codes(title),
        "source_categories": list(item.get("source_categories") or []),
        "category_code": ",".join(item.get("source_categories") or []),
    }
