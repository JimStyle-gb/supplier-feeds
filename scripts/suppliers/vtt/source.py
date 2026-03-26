# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/source.py

VTT source layer.
v10:
- same category-first coverage;
- stronger HTTP pooling / keep-alive reuse;
- listing filter moved to filtering.py;
- html param/desc parsing moved to params_page.py;
- pictures cleanup moved to pictures.py;
- keeps existing build_vtt.py contract unchanged.
"""

from __future__ import annotations

import os
import time
from collections import deque
from datetime import datetime
from html import unescape
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .filtering import (
    DEFAULT_ALLOWED_TITLE_PREFIXES,
    DEFAULT_CATEGORY_CODES,
    categories_from_cfg,
    load_filter_config,
    mk_category_url,
    normalize_listing_title,
    normalize_listing_url,
    prefixes_from_cfg,
    product_path_re,
    title_matches_allowed,
)
from .models import ParsedProductPage, ProductIndexItem, VTTConfig
from .normalize import canon_vendor, norm_ws
from .params_page import (
    extract_images_from_html,
    extract_meta_desc,
    extract_params_and_desc,
    extract_price_rub,
    extract_sku,
    extract_title,
    extract_title_codes,
    extract_vendor_from_title,
    html_text_fast,
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

HREF_RE = __import__("re").compile(r"""href=["']([^"']+)["']""", __import__("re").I)
ANCHOR_RE = __import__("re").compile(r"""<a\b[^>]*href=["']([^"']+)["'][^>]*>(.*?)</a>""", __import__("re").I | __import__("re").S)
META_CSRF_RE = __import__("re").compile(r"""<meta[^>]+name=["']csrf-token["'][^>]+content=["']([^"']+)["']""", __import__("re").I)


def log(msg: str) -> None:
    print(msg, flush=True)


def _sleep_ms(ms: int) -> None:
    if ms > 0:
        time.sleep(ms / 1000.0)


def _repo_root() -> str:
    return str(__import__("pathlib").Path(__file__).resolve().parents[3])


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

    filter_cfg = load_filter_config(
        __import__("pathlib").Path(__file__).resolve().parent / "config" / "filter.yml"
    )

    cats = [x.strip() for x in (os.getenv("VTT_CATEGORY_CODES") or "").split(",") if x.strip()]
    if not cats:
        cats = categories_from_cfg(filter_cfg) or list(DEFAULT_CATEGORY_CODES)

    prefixes = [x.strip() for x in (os.getenv("VTT_ALLOWED_TITLE_PREFIXES") or "").split(",") if x.strip()]
    if not prefixes:
        prefixes = prefixes_from_cfg(filter_cfg) or list(DEFAULT_ALLOWED_TITLE_PREFIXES)

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
    m = META_CSRF_RE.search(html)
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


def collect_product_index(sess: requests.Session, cfg: VTTConfig, categories: list[str], deadline: datetime) -> list[dict[str, Any]]:
    allowed_categories = set(categories)
    allowed_prefixes = list(cfg.allowed_title_prefixes)
    base_netloc = urlparse(cfg.base_url).netloc
    queue = deque(normalize_listing_url(mk_category_url(cfg.base_url, code)) for code in categories)
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

        for href, inner in ANCHOR_RE.findall(html):
            href = unescape((href or "").strip())
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            abs_url = urljoin(resp.url, href)
            p = urlparse(abs_url)
            if p.netloc != base_netloc:
                continue

            if p.path.lower().startswith("/catalog/") and not p.query and product_path_re(p.path):
                rec = product_candidates.setdefault(abs_url, {"source_categories": set(), "titles": set()})
                rec["source_categories"].update(current_cats)
                title_text = normalize_listing_title(html_text_fast(inner))
                if title_text:
                    rec["titles"].add(title_text)
                continue

            if p.path.lower().startswith("/catalog"):
                qs = parse_qs(p.query)
                cat_values = {x.strip() for x in qs.get("category", []) if x.strip()}
                if cat_values and cat_values.issubset(allowed_categories):
                    norm = normalize_listing_url(abs_url)
                    if norm not in seen_listings:
                        queue.append(norm)

        for href in HREF_RE.findall(html):
            href = unescape((href or "").strip())
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            abs_url = urljoin(resp.url, href)
            p = urlparse(abs_url)
            if p.netloc != base_netloc:
                continue
            if p.path.lower().startswith("/catalog/") and not p.query and product_path_re(p.path):
                rec = product_candidates.setdefault(abs_url, {"source_categories": set(), "titles": set()})
                rec["source_categories"].update(current_cats)

    out: list[dict[str, Any]] = []
    for url, meta in sorted(product_candidates.items(), key=lambda kv: kv[0]):
        titles = sorted(meta.get("titles") or [])
        if titles and not any(title_matches_allowed(title, allowed_prefixes) for title in titles):
            early_rejected += 1
            continue
        item = ProductIndexItem(
            url=url,
            source_categories=sorted(list(meta.get("source_categories") or [])),
            listing_titles=titles,
        )
        out.append(
            {
                "url": item.url,
                "source_categories": item.source_categories,
                "listing_titles": item.listing_titles,
            }
        )

    log(f"[VTT] listing_index total={len(product_candidates)} kept={len(out)} early_rejected={early_rejected}")
    return out


def parse_product_page_from_index(sess: requests.Session, cfg: VTTConfig, item: dict[str, Any]) -> dict[str, Any] | None:
    url = norm_ws(item.get("url"))
    if not url:
        return None
    resp = _get(sess, cfg, url, delay_ms=cfg.product_request_delay_ms)
    html = resp.text or ""
    title = extract_title(html)
    if not title:
        return None
    params, desc_body = extract_params_and_desc(html)
    parsed = ParsedProductPage(
        url=resp.url,
        name=title,
        vendor=extract_vendor_from_title(title),
        sku=extract_sku(html),
        price_rub_raw=extract_price_rub(html),
        pictures=extract_images_from_html(resp.url, html),
        params=params,
        description_meta=extract_meta_desc(html),
        description_body=desc_body,
        title_codes=extract_title_codes(title),
        source_categories=list(item.get("source_categories") or []),
        category_code=",".join(item.get("source_categories") or []),
        listing_titles=list(item.get("listing_titles") or []),
    )
    return {
        "url": parsed.url,
        "name": parsed.name,
        "vendor": parsed.vendor,
        "sku": parsed.sku,
        "price_rub_raw": parsed.price_rub_raw,
        "pictures": parsed.pictures,
        "params": parsed.params,
        "description_meta": parsed.description_meta,
        "description_body": parsed.description_body,
        "title_codes": parsed.title_codes,
        "source_categories": parsed.source_categories,
        "category_code": parsed.category_code,
        "listing_titles": parsed.listing_titles,
    }
