# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/source.py

VTT source layer.
v17:
- same category-first coverage;
- keeps stronger HTTP pooling / keep-alive reuse;
- listing prefix check is advisory only;
- intentionally reverts source/index behavior to the best modular coverage state seen so far;
- keeps titles in index for diagnostics.
"""

from __future__ import annotations

import html as ihtml
import os
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from html import unescape
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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

# Ранний фильтр по смысловому началу названия на листинге.
DEFAULT_ALLOWED_TITLE_PREFIXES: list[str] = [
    "Drum",
    "Девелопер",
    "Драм-картридж",
    "Драм-юнит",
    "Драм-юниты",
    "Драм юнит",
    "Кабель сетевой",
    "Картридж",
    "Картриджи",
    "Термоблок",
    "Тонер-картридж",
    "Тонер-катридж",
    "Чернила",
    "Печатающая головка",
    "Копи-картридж",
    "Принт-картридж",
    "Контейнер",
    "Блок",
    "Бункер",
    "Носитель",
    "Фотобарабан",
    "Барабан",
    "Тонер",
    "Комплект",
    "Набор",
    "Заправочный комплект",
    "Модуль фоторецептора",
    "Фотопроводниковый блок",
    "Бокс сбора тонера",
    "Рефил",
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_HREF_RE = re.compile(r"""href=["']([^"']+)["']""", re.I)
_ANCHOR_RE = re.compile(r"""<a\b[^>]*href=["']([^"']+)["'][^>]*>(.*?)</a>""", re.I | re.S)
_META_CSRF_RE = re.compile(r"""<meta[^>]+name=["']csrf-token["'][^>]+content=["']([^"']+)["']""", re.I)
_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.I | re.S)
_META_DESC_RE = re.compile(r"""<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["']""", re.I)
_SKU_RE = re.compile(r"""let\s+sku\s*=\s*["']([^"']+)["']""", re.I)
_PRICE_RUB_RE = re.compile(r"""let\s+priceRUB\s*=\s*([0-9]+(?:\.[0-9]+)?)""", re.I)
_PRICE_MAIN_RE = re.compile(r"""price_main[^>]*>\s*<b>([^<]+)</b>""", re.I | re.S)
_IMAGE_RE = re.compile(
    r"""(?:src|href|data-src|data-original|srcset)=["']([^"']+\.(?:jpg|jpeg|png|webp|gif)(?:\?[^"']*)?)["']""",
    re.I,
)
_CODE_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9\-./]{2,}\b")
_BAD_IMAGE_RE = re.compile(r"(favicon|yandex|counter|watch/|pixel|metrika|doubleclick)", re.I)
_DESC_BLOCK_RE = re.compile(
    r"""<div[^>]+class=["'][^"']*(?:description|catalog_item_descr)[^"']*["'][^>]*>(.*?)</div>""",
    re.I | re.S,
)
_DT_DD_RE = re.compile(r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>", re.I | re.S)
_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.I | re.S)
_CELL_RE = re.compile(r"<(?:th|td)[^>]*>(.*?)</(?:th|td)>", re.I | re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_TITLE_LEAD_CODE_RE = re.compile(
    r"""^(?:[A-Z0-9][A-Z0-9\-./]{2,}(?:\s*,\s*[A-Z0-9][A-Z0-9\-./]{2,})*\s+)+""",
    re.I,
)
_ORIGINAL_MARK_RE = re.compile(r"""(?<!\w)\((?:O|О|OEM)\)(?!\w)|\bоригинал(?:ьн(?:ый|ая|ое|ые))?\b""", re.I)
_LEAD_MARK_RE = re.compile(r"""^(?:\((?:E|LE)\)|LE\b|E\b)\s*""", re.I)


def _product_path_re(path: str) -> bool:
    return bool(re.match(r"^/catalog/[^/?#]+/?$", path or "", re.I))


@dataclass(slots=True)
class VTTConfig:
    base_url: str
    start_url: str
    login_url: str
    login: str
    password: str
    timeout_s: int = 35
    listing_request_delay_ms: int = 6
    product_request_delay_ms: int = 0
    max_listing_pages: int = 5000
    max_workers: int = 20
    max_crawl_minutes: float = 90.0
    softfail: bool = False
    categories: list[str] = field(default_factory=lambda: list(DEFAULT_CATEGORY_CODES))
    allowed_title_prefixes: list[str] = field(default_factory=lambda: list(DEFAULT_ALLOWED_TITLE_PREFIXES))


def log(msg: str) -> None:
    print(msg, flush=True)


def _sleep_ms(ms: int) -> None:
    if ms > 0:
        time.sleep(ms / 1000.0)


def _norm_ws(text: str) -> str:
    return " ".join(str(text or "").replace("\xa0", " ").split()).strip()


def _html_text_fast(fragment: str) -> str:
    if not fragment:
        return ""
    text = _TAG_RE.sub(" ", fragment)
    text = ihtml.unescape(text)
    return _norm_ws(text)


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


def _build_retry() -> Retry:
    return Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )


def _configure_session(session: requests.Session, cfg: VTTConfig) -> requests.Session:
    adapter = HTTPAdapter(
        pool_connections=max(32, cfg.max_workers * 2),
        pool_maxsize=max(64, cfg.max_workers * 4),
        max_retries=_build_retry(),
    )
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": UA,
            "Accept-Language": "ru,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def make_session(cfg: VTTConfig) -> requests.Session:
    return _configure_session(requests.Session(), cfg)


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
    prefixes = [x.strip() for x in (os.getenv("VTT_ALLOWED_TITLE_PREFIXES") or "").split(",") if x.strip()]
    if not prefixes:
        prefixes = list(DEFAULT_ALLOWED_TITLE_PREFIXES)
    return VTTConfig(
        base_url=base_url,
        start_url=urljoin(base_url, "/catalog/"),
        login_url=urljoin(base_url, "/validateLogin"),
        login=(os.getenv("VTT_LOGIN") or "").strip(),
        password=(os.getenv("VTT_PASSWORD") or "").strip(),
        timeout_s=int((os.getenv("VTT_TIMEOUT_S") or "35").strip() or "35"),
        listing_request_delay_ms=int((os.getenv("VTT_LISTING_REQUEST_DELAY_MS") or os.getenv("VTT_REQUEST_DELAY_MS") or "6").strip() or "6"),
        product_request_delay_ms=int((os.getenv("VTT_PRODUCT_REQUEST_DELAY_MS") or "0").strip() or "0"),
        max_listing_pages=int((os.getenv("VTT_MAX_LISTING_PAGES") or "5000").strip() or "5000"),
        max_workers=int((os.getenv("VTT_MAX_WORKERS") or "20").strip() or "20"),
        max_crawl_minutes=float((os.getenv("VTT_MAX_CRAWL_MINUTES") or "90").strip() or "90"),
        softfail=(os.getenv("VTT_SOFTFAIL") or "false").strip().lower() == "true",
        categories=cats,
        allowed_title_prefixes=prefixes,
    )


def _get(sess: requests.Session, cfg: VTTConfig, url: str, *, delay_ms: int) -> requests.Response:
    _sleep_ms(delay_ms)
    resp = sess.get(url, timeout=cfg.timeout_s, allow_redirects=True)
    resp.raise_for_status()
    return resp


def _post(sess: requests.Session, cfg: VTTConfig, url: str, *, delay_ms: int, **kwargs) -> requests.Response:
    _sleep_ms(delay_ms)
    resp = sess.post(url, timeout=cfg.timeout_s, allow_redirects=True, **kwargs)
    resp.raise_for_status()
    return resp


def login(sess: requests.Session, cfg: VTTConfig) -> bool:
    if not cfg.login or not cfg.password:
        return False
    home = _get(sess, cfg, urljoin(cfg.base_url, "/"), delay_ms=cfg.listing_request_delay_ms)
    html = home.text or ""
    m = _META_CSRF_RE.search(html)
    token = m.group(1).strip() if m else ""
    headers = {"Referer": urljoin(cfg.base_url, "/")}
    if token:
        headers["X-CSRF-TOKEN"] = token
    _post(
        sess,
        cfg,
        cfg.login_url,
        delay_ms=cfg.listing_request_delay_ms,
        data={"login": cfg.login, "password": cfg.password},
        headers=headers,
    )
    try:
        chk = _get(sess, cfg, cfg.start_url, delay_ms=cfg.listing_request_delay_ms)
        body = chk.text or ""
        return ("/login" not in chk.url.lower()) and ("Вход для клиентов" not in body)
    except Exception:
        return False


def _normalize_listing_title(title: str) -> str:
    title = _norm_ws(title)
    title = _ORIGINAL_MARK_RE.sub("", title)
    title = _TITLE_LEAD_CODE_RE.sub("", title)
    while True:
        new_title = _LEAD_MARK_RE.sub("", title).strip(" ,.-")
        if new_title == title:
            break
        title = new_title
    title = _norm_ws(title).strip(" ,.-")
    return title


def _title_matches_allowed(title: str, prefixes: list[str]) -> bool:
    if not prefixes:
        return True
    if not title:
        return True  # не режем товар, если не смогли достать заголовок с листинга
    low = title.casefold()
    compact = low.replace("-", " ")
    for prefix in prefixes:
        p = prefix.casefold()
        pp = p.replace("-", " ")
        if low.startswith(p) or compact.startswith(pp):
            return True
    return False


def collect_product_index(sess: requests.Session, cfg: VTTConfig, categories: list[str], deadline: datetime) -> list[dict[str, Any]]:
    allowed_categories = set(categories)
    allowed_prefixes = list(cfg.allowed_title_prefixes)
    base_netloc = urlparse(cfg.base_url).netloc
    queue = deque(_normalize_listing_url(_mk_category_url(cfg.base_url, code)) for code in categories)
    seen_listings: set[str] = set()
    product_candidates: dict[str, dict[str, Any]] = {}
    early_rejected = 0

    while queue and len(seen_listings) < cfg.max_listing_pages and datetime.utcnow() < deadline:
        url = queue.popleft()
        if url in seen_listings:
            continue
        seen_listings.add(url)
        try:
            resp = _get(sess, cfg, url, delay_ms=cfg.listing_request_delay_ms)
            html = resp.text or ""
        except Exception as exc:
            log(f"[VTT] listing error: {url} :: {exc}")
            continue

        current_cats = {x.strip() for x in parse_qs(urlparse(resp.url).query).get("category", []) if x.strip()}

        for href, inner in _ANCHOR_RE.findall(html):
            href = unescape((href or "").strip())
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            abs_url = urljoin(resp.url, href)
            p = urlparse(abs_url)
            if p.netloc != base_netloc:
                continue

            if p.path.lower().startswith("/catalog/") and not p.query and _product_path_re(p.path):
                rec = product_candidates.setdefault(
                    abs_url,
                    {"source_categories": set(), "titles": set()},
                )
                rec["source_categories"].update(current_cats)
                title_text = _normalize_listing_title(_html_text_fast(inner))
                if title_text:
                    rec["titles"].add(title_text)
                continue

            if p.path.lower().startswith("/catalog"):
                qs = parse_qs(p.query)
                cat_values = {x.strip() for x in qs.get("category", []) if x.strip()}
                if cat_values and cat_values.issubset(allowed_categories):
                    norm = _normalize_listing_url(abs_url)
                    if norm not in seen_listings:
                        queue.append(norm)

        # fallback on bare href scan for product links without anchor text
        for href in _HREF_RE.findall(html):
            href = unescape((href or "").strip())
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            abs_url = urljoin(resp.url, href)
            p = urlparse(abs_url)
            if p.netloc != base_netloc:
                continue
            if p.path.lower().startswith("/catalog/") and not p.query and _product_path_re(p.path):
                rec = product_candidates.setdefault(
                    abs_url,
                    {"source_categories": set(), "titles": set()},
                )
                rec["source_categories"].update(current_cats)

    out: list[dict[str, Any]] = []
    soft_prefix_mismatch = 0
    for url, meta in sorted(product_candidates.items(), key=lambda kv: kv[0]):
        titles = sorted(meta.get("titles") or [])
        if titles and not any(_title_matches_allowed(title, allowed_prefixes) for title in titles):
            soft_prefix_mismatch += 1
        out.append(
            {
                "url": url,
                "source_categories": sorted(list(meta.get("source_categories") or [])),
                "listing_titles": titles,
            }
        )

    log(f"[VTT] listing_index total={len(product_candidates)} kept={len(out)} early_rejected={early_rejected} soft_prefix_mismatch={soft_prefix_mismatch}")
    return out


def _extract_title(html: str) -> str:
    m = _H1_RE.search(html)
    if m:
        return _html_text_fast(m.group(1))
    m = _TITLE_RE.search(html)
    return _html_text_fast(m.group(1)) if m else ""


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


def _extract_params_and_desc_fast(html: str) -> tuple[list[tuple[str, str]], str]:
    params: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for key_html, val_html in _DT_DD_RE.findall(html or ""):
        key = _html_text_fast(key_html).strip(":")
        val = _html_text_fast(val_html)
        if key and val and (key, val) not in seen:
            seen.add((key, val))
            params.append((key, val))

    if not params:
        for tr_html in _TR_RE.findall(html or ""):
            cells = _CELL_RE.findall(tr_html)
            if len(cells) < 2:
                continue
            key = _html_text_fast(cells[0]).strip(":")
            val = _html_text_fast(cells[1])
            if key and val and (key, val) not in seen:
                seen.add((key, val))
                params.append((key, val))

    desc = ""
    m = _DESC_BLOCK_RE.search(html or "")
    if m:
        desc = _html_text_fast(m.group(1))
    return params, desc


def _extract_params_and_desc(html: str) -> tuple[list[tuple[str, str]], str]:
    params, desc = _extract_params_and_desc_fast(html)
    if params or desc:
        return params, desc

    soup = BeautifulSoup(html or "", "lxml")
    params = []
    seen: set[tuple[str, str]] = set()

    for box in soup.select("div.description.catalog_item_descr, div.description"):
        dts = box.find_all("dt")
        dds = box.find_all("dd")
        if dts and dds:
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

    if not desc:
        m = _DESC_BLOCK_RE.search(html or "")
        if m:
            desc = _html_text_fast(m.group(1))
    return params, desc


def _extract_title_codes(title: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for code in _CODE_TOKEN_RE.findall(title or ""):
        code = code.strip(".-/")
        if len(code) < 3 or not re.search(r"\d", code):
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
    resp = _get(sess, cfg, url, delay_ms=cfg.product_request_delay_ms)
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
        "listing_titles": list(item.get("listing_titles") or []),
    }
