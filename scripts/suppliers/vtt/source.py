# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/source.py

VTT source layer.

Роль файла:
- только login/session/crawl/product-page parsing;
- без supplier-business логики по витрине;
- без owner-логики по builder/final;
- ассортиментная политика берётся из filtering.py / config, а не живёт здесь.

v18:
- source больше не держит собственные DEFAULT_CATEGORY_CODES / DEFAULT_ALLOWED_TITLE_PREFIXES;
- cfg_from_env() читает ассортиментные входы через filtering.py;
- сохраняет backward-safe API для build_vtt.py:
  cfg_from_env, make_session, clone_session_with_cookies, login,
  collect_product_index, parse_product_page_from_index.
"""

from __future__ import annotations

import os
import re
import time
from collections import deque
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .models import VTTConfig
from .normalize import canon_vendor, norm_ws

# Канонический extractor после Патча 1.
try:  # pragma: no cover - backward-safe bridge
    from .params import (
        extract_images_from_html,
        extract_meta_desc,
        extract_params_and_desc,
        extract_price_rub,
        extract_sku,
        extract_title,
        extract_title_codes,
    )
except Exception:  # pragma: no cover
    from .params_page import (  # type: ignore
        extract_images_from_html,
        extract_meta_desc,
        extract_params_and_desc,
        extract_price_rub,
        extract_sku,
        extract_title,
        extract_title_codes,
    )

from .filtering import (
    DEFAULT_ALLOWED_TITLE_PREFIXES,
    DEFAULT_CATEGORY_CODES,
    mk_category_url,
    normalize_listing_title,
    normalize_listing_url,
    product_path_re,
)

try:  # pragma: no cover - новый filtering.py
    from .filtering import resolve_filter_inputs, title_matches_allowed
except Exception:  # pragma: no cover - совместимость со старым filtering.py
    def resolve_filter_inputs(
        *,
        cfg_path: str | Path | None = None,
        env_category_codes: str | None = None,
        env_allowed_prefixes: str | None = None,
    ) -> tuple[list[str], list[str]]:
        cats = [x.strip() for x in (env_category_codes or "").split(",") if x.strip()]
        if not cats:
            cats = list(DEFAULT_CATEGORY_CODES)
        prefixes = [x.strip() for x in (env_allowed_prefixes or "").split(",") if x.strip()]
        if not prefixes:
            prefixes = list(DEFAULT_ALLOWED_TITLE_PREFIXES)
        return cats, prefixes

    def title_matches_allowed(title: str, allowed_prefixes: list[str]) -> bool:
        title_n = normalize_listing_title(title)
        if not title_n:
            return False
        for prefix in allowed_prefixes:
            p = norm_ws(prefix)
            if p and title_n.casefold().startswith(p.casefold()):
                return True
        return False


UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_HREF_RE = re.compile(r'''href=["\']([^"\']+)["\']''', re.I)
_ANCHOR_RE = re.compile(r'''<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>''', re.I | re.S)
_META_CSRF_RE = re.compile(r'''<meta[^>]+name=["\']csrf-token["\'][^>]+content=["\']([^"\']+)["\']''', re.I)
_VENDOR_TOKEN_RE = re.compile(r"\b(?:HP|CANON|XEROX|BROTHER|KYOCERA|SAMSUNG|EPSON|RICOH|KONICA\s+MINOLTA|PANTUM|LEXMARK|OKI|SHARP|PANASONIC|TOSHIBA|DEVELOP|GESTETNER|RISO)\b", re.I)


def log(msg: str) -> None:
    print(msg, flush=True)


def _sleep_ms(ms: int) -> None:
    if ms > 0:
        time.sleep(ms / 1000.0)


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _configure_session(sess: requests.Session, cfg: VTTConfig) -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update(
        {
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.5",
            "Connection": "keep-alive",
        }
    )
    return sess


def make_session(cfg: VTTConfig) -> requests.Session:
    return _configure_session(requests.Session(), cfg)


def clone_session_with_cookies(master: requests.Session, cfg: VTTConfig) -> requests.Session:
    sess = make_session(cfg)
    sess.cookies.update(master.cookies)
    return sess


def cfg_from_env() -> VTTConfig:
    base_url = (os.getenv("VTT_BASE_URL") or "https://b2b.vtt.ru/").strip()
    base_url = base_url.rstrip("/") + "/"

    cfg_path = os.getenv("VTT_FILTER_CFG") or "scripts/suppliers/vtt/config/filter.yml"
    categories, allowed_prefixes = resolve_filter_inputs(
        cfg_path=cfg_path,
        env_category_codes=os.getenv("VTT_CATEGORY_CODES"),
        env_allowed_prefixes=os.getenv("VTT_ALLOWED_TITLE_PREFIXES"),
    )

    return VTTConfig(
        base_url=base_url,
        start_url=urljoin(base_url, "/catalog/"),
        login_url=urljoin(base_url, "/validateLogin"),
        login=(os.getenv("VTT_LOGIN") or "").strip(),
        password=(os.getenv("VTT_PASSWORD") or "").strip(),
        timeout_s=_safe_int(os.getenv("VTT_TIMEOUT_S") or "35", 35),
        listing_request_delay_ms=_safe_int(
            os.getenv("VTT_LISTING_REQUEST_DELAY_MS") or os.getenv("VTT_REQUEST_DELAY_MS") or "6",
            6,
        ),
        product_request_delay_ms=_safe_int(os.getenv("VTT_PRODUCT_REQUEST_DELAY_MS") or "0", 0),
        max_listing_pages=_safe_int(os.getenv("VTT_MAX_LISTING_PAGES") or "5000", 5000),
        max_workers=_safe_int(os.getenv("VTT_MAX_WORKERS") or "20", 20),
        max_crawl_minutes=_safe_float(os.getenv("VTT_MAX_CRAWL_MINUTES") or "90", 90.0),
        softfail=(os.getenv("VTT_SOFTFAIL") or "false").strip().lower() == "true",
        categories=list(categories),
        allowed_title_prefixes=list(allowed_prefixes),
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
        headers=headers,
        data={"login": cfg.login, "password": cfg.password},
    )

    probe = _get(sess, cfg, cfg.start_url, delay_ms=cfg.listing_request_delay_ms)
    return "/catalog" in (probe.url or "")


def _extract_vendor_from_title(title: str) -> str:
    m = _VENDOR_TOKEN_RE.search(title or "")
    if not m:
        return ""
    return canon_vendor(norm_ws(m.group(0)))


def collect_product_index(
    sess: requests.Session,
    cfg: VTTConfig,
    categories: list[str],
    deadline: datetime,
) -> list[dict[str, Any]]:
    allowed_categories = {x.strip() for x in categories if x and x.strip()}
    allowed_prefixes = list(cfg.allowed_title_prefixes or [])
    base_netloc = urlparse(cfg.base_url).netloc
    queue = deque(normalize_listing_url(mk_category_url(cfg.base_url, code)) for code in categories if code)
    seen_listings: set[str] = set()
    product_candidates: dict[str, dict[str, Any]] = {}

    while queue and len(seen_listings) < int(cfg.max_listing_pages) and datetime.utcnow() < deadline:
        url = queue.popleft()
        if not url or url in seen_listings:
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
            parsed = urlparse(abs_url)
            if parsed.netloc != base_netloc:
                continue

            if parsed.path.lower().startswith("/catalog/") and not parsed.query and product_path_re(parsed.path):
                rec = product_candidates.setdefault(abs_url, {"source_categories": set(), "listing_titles": set()})
                rec["source_categories"].update(current_cats)
                title_text = normalize_listing_title(inner)
                if title_text:
                    rec["listing_titles"].add(title_text)
                continue

            if parsed.path.lower().startswith("/catalog"):
                qs = parse_qs(parsed.query)
                cat_values = {x.strip() for x in qs.get("category", []) if x.strip()}
                if cat_values and cat_values.issubset(allowed_categories):
                    norm = normalize_listing_url(abs_url)
                    if norm not in seen_listings:
                        queue.append(norm)

        # fallback: ссылки без anchor-text
        for href in _HREF_RE.findall(html):
            href = unescape((href or "").strip())
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            abs_url = urljoin(resp.url, href)
            parsed = urlparse(abs_url)
            if parsed.netloc != base_netloc:
                continue
            if parsed.path.lower().startswith("/catalog/") and not parsed.query and product_path_re(parsed.path):
                rec = product_candidates.setdefault(abs_url, {"source_categories": set(), "listing_titles": set()})
                rec["source_categories"].update(current_cats)

    out: list[dict[str, Any]] = []
    soft_prefix_mismatch = 0

    for url, meta in sorted(product_candidates.items(), key=lambda kv: kv[0]):
        titles = sorted(meta.get("listing_titles") or [])
        if titles and not any(title_matches_allowed(title, allowed_prefixes) for title in titles):
            soft_prefix_mismatch += 1
        out.append(
            {
                "url": url,
                "source_categories": sorted(list(meta.get("source_categories") or [])),
                "listing_titles": titles,
            }
        )

    log(
        f"[VTT] index: listings={len(seen_listings)} products={len(out)} "
        f"soft_prefix_mismatch={soft_prefix_mismatch}"
    )
    return out


def parse_product_page_from_index(
    sess: requests.Session,
    cfg: VTTConfig,
    item: dict[str, Any],
) -> dict[str, Any] | None:
    url = norm_ws(item.get("url"))
    if not url:
        return None

    resp = _get(sess, cfg, url, delay_ms=cfg.product_request_delay_ms)
    html = resp.text or ""

    title = extract_title(html)
    if not title:
        return None

    params, desc_body = extract_params_and_desc(html)
    source_categories = [norm_ws(x) for x in (item.get("source_categories") or []) if norm_ws(x)]
    listing_titles = [norm_ws(x) for x in (item.get("listing_titles") or []) if norm_ws(x)]

    return {
        "url": resp.url,
        "name": title,
        "vendor": _extract_vendor_from_title(title),
        "sku": extract_sku(html),
        "price_rub_raw": extract_price_rub(html),
        "pictures": extract_images_from_html(resp.url, html),
        "params": params,
        "description_meta": extract_meta_desc(html),
        "description_body": desc_body,
        "title_codes": extract_title_codes(title),
        "source_categories": source_categories,
        "category_code": ",".join(source_categories),
        "listing_titles": listing_titles,
    }


__all__ = [
    "VTTConfig",
    "cfg_from_env",
    "make_session",
    "clone_session_with_cookies",
    "login",
    "collect_product_index",
    "parse_product_page_from_index",
    "log",
]
