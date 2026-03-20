# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/source.py

VTT source-layer.
Только transport / login / crawl / raw page parse.
Без business-логики supplier-layer:
- без фильтра ассортимента,
- без нормализации params,
- без compat,
- без сборки OfferOut.
"""

from __future__ import annotations

import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

# Старый рабочий regex ссылок товара.
_PRODUCT_HREF_RE = re.compile(r"^/catalog/[^?]+/?$")

# Совместимость со старым debug-слоем.
log = print


@dataclass(frozen=True)
class VttSourceCfg:
    """Конфиг source-слоя VTT."""
    base_url: str
    start_url: str
    login: str
    password: str
    max_pages: int
    max_workers: int
    max_crawl_minutes: float
    delay_ms: int
    verify: object  # bool | path to CA bundle
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
    """Читает env для source-слоя VTT."""
    base = (os.getenv("VTT_BASE_URL", "https://b2b.vtt.ru") or "").strip().rstrip("/")
    start = (os.getenv("VTT_START_URL", f"{base}/catalog/") or "").strip()

    ssl_verify = _env_bool("VTT_SSL_VERIFY", True)
    ca_bundle = (os.getenv("VTT_CA_BUNDLE", "") or "").strip()
    verify: object = ca_bundle if ca_bundle else ssl_verify

    return VttSourceCfg(
        base_url=base,
        start_url=start,
        login=(os.getenv("VTT_LOGIN", "") or "").strip(),
        password=(os.getenv("VTT_PASSWORD", "") or "").strip(),
        max_pages=_env_int("VTT_MAX_PAGES", 200),
        max_workers=_env_int("VTT_MAX_WORKERS", 10),
        max_crawl_minutes=_env_float("VTT_MAX_CRAWL_MINUTES", 18.0),
        delay_ms=_env_int("VTT_REQUEST_DELAY_MS", 80),
        verify=verify,
        softfail=_env_bool("VTT_SOFTFAIL", True),
    )


def _sleep_ms(ms: int) -> None:
    if ms <= 0:
        return
    time.sleep((ms / 1000.0) * random.uniform(0.75, 1.35))


def make_session(cfg: VttSourceCfg) -> requests.Session:
    """Создаёт requests session."""
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120 Safari/537.36"
            ),
            "Accept-Language": "ru,en;q=0.8",
        }
    )
    return s


def clone_session_with_cookies(src: requests.Session, cfg: VttSourceCfg) -> requests.Session:
    """Клонирует сессию для параллельного парса карточек."""
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
    timeout: int = 25,
    data: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> requests.Response | None:
    """GET/POST с retry/backoff."""
    tries = 7
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
                raise requests.HTTPError(f"{r.status_code}")
            return r
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last = (i == tries - 1)
            log(f"[vtt:http] {method} {url} fail: {e}{' (last)' if last else ''}", flush=True)
            if last:
                return None
            time.sleep(min(12.0, 0.6 * (2 ** i)) + random.uniform(0.0, 0.6))
    return None


def _get_bytes(s: requests.Session, cfg: VttSourceCfg, url: str, *, timeout: int = 25) -> bytes | None:
    r = _request(s, cfg, "GET", url, timeout=timeout)
    if not r or r.status_code != 200:
        return None
    _sleep_ms(cfg.delay_ms)
    return r.content


def _post_ok(
    s: requests.Session,
    cfg: VttSourceCfg,
    url: str,
    *,
    data: dict[str, Any],
    headers: dict[str, str] | None = None,
    timeout: int = 25,
) -> bool:
    r = _request(s, cfg, "POST", url, timeout=timeout, data=data, headers=headers)
    ok = bool(r and r.status_code in (200, 204))
    _sleep_ms(cfg.delay_ms)
    return ok


def _abs_url(cfg: VttSourceCfg, href: str) -> str:
    u = (href or "").strip()
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if not u.startswith("/"):
        u = "/" + u
    return cfg.base_url + u


def _set_q(url: str, key: str, value: str) -> str:
    pu = urlparse(url)
    q = parse_qs(pu.query)
    q[key] = [value]
    return urlunparse(pu._replace(query=urlencode(q, doseq=True)))


def _soup(html_bytes: bytes) -> BeautifulSoup:
    return BeautifulSoup(html_bytes, "html.parser")


def _extract_csrf_token(html_bytes: bytes) -> str:
    sp = _soup(html_bytes)
    m = sp.find("meta", attrs={"name": "csrf-token"})
    return ((m.get("content") if m else "") or "").strip()


def login(s: requests.Session, cfg: VttSourceCfg) -> bool:
    """Логин на b2b.vtt.ru."""
    if not cfg.login or not cfg.password:
        log("[WARN] VTT_LOGIN/VTT_PASSWORD пустые", flush=True)
        return False

    home = _get_bytes(s, cfg, cfg.base_url + "/")
    if not home:
        return False

    csrf = _extract_csrf_token(home)
    headers = {"Referer": cfg.base_url + "/"}
    if csrf:
        headers["X-CSRF-Token"] = csrf

    login_url = cfg.base_url + "/site/login"
    payload = {
        "LoginForm[login]": cfg.login,
        "LoginForm[password]": cfg.password,
    }
    ok = _post_ok(s, cfg, login_url, data=payload, headers=headers, timeout=30)
    if not ok:
        return False

    probe = _get_bytes(s, cfg, cfg.start_url)
    if not probe:
        return False
    return True


def category_code(category_url: str) -> str:
    """Вытаскивает code из ?category=..."""
    q = parse_qs(urlparse(category_url).query)
    return (q.get("category", [""]) or [""])[0].strip()


def collect_links_in_category(
    s: requests.Session,
    cfg: VttSourceCfg,
    category_url: str,
    deadline_utc: datetime,
) -> list[str]:
    """Собирает ссылки товаров из одной категории."""
    found: list[str] = []
    seen: set[str] = set()

    for page in range(1, max(1, cfg.max_pages) + 1):
        if datetime.utcnow() >= deadline_utc:
            break

        page_url = _set_q(category_url, "page", str(page))
        b = _get_bytes(s, cfg, page_url)
        if not b:
            break

        sp = _soup(b)
        links: list[str] = []

        for a in sp.find_all("a", href=True):
            cls = a.get("class") or []
            if isinstance(cls, list) and "btn_pic" in cls:
                continue

            href = str(a["href"]).strip()
            if not href or href.startswith("#"):
                continue

            if href.startswith("http://") or href.startswith("https://"):
                if cfg.base_url not in href:
                    continue
                href = urlparse(href).path

            if not _PRODUCT_HREF_RE.match(href):
                continue

            u = _abs_url(cfg, href)
            if u in seen:
                continue
            seen.add(u)
            links.append(u)

        if not links:
            break

        found.extend(links)

    return found


def collect_product_index(
    s: requests.Session,
    cfg: VttSourceCfg,
    category_urls: list[str],
    deadline_utc: datetime,
) -> list[dict[str, str]]:
    """
    Собирает сырой product-index.
    Здесь без filtering.py: source только приносит индекс.
    """
    out: list[dict[str, str]] = []
    seen: set[str] = set()

    for cu in category_urls:
        if datetime.utcnow() >= deadline_utc:
            break

        code = category_code(cu)
        links = collect_links_in_category(s, cfg, cu, deadline_utc)
        log(f"[vtt:site] category={code or '?'} links={len(links)}", flush=True)

        for u in links:
            if u in seen:
                continue
            seen.add(u)
            out.append(
                {
                    "url": u,
                    "category_code": code,
                    "title": "",
                }
            )

    return out


def _parse_price_int(text: str) -> int | None:
    """Понимает '2 449.22', '2 449,22', '2449', '2 449'."""
    if not text:
        return None
    s = str(text).replace("\u00a0", " ").replace("&nbsp;", " ")
    s = s.strip()
    s = re.sub(r"[^0-9.,\s]+", "", s)
    s = re.sub(r"\s+", "", s)
    if not s:
        return None

    if "." in s and "," in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "")
            s = s.replace(",", ".")
    else:
        if "," in s and "." not in s:
            if s.count(",") == 1 and len(s.split(",")[1]) == 2:
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        if "." in s and "," not in s:
            if not (s.count(".") == 1 and len(s.split(".")[1]) == 2):
                s = s.replace(".", "")

    try:
        if "." in s:
            s = s.split(".", 1)[0]
        s = s.lstrip("0") or "0"
        return int(s)
    except Exception:
        return None


def extract_pairs(sp: BeautifulSoup) -> dict[str, str]:
    """Сырые пары dt/dd из карточки."""
    out: dict[str, str] = {}
    box = sp.select_one("div.description.catalog_item_descr")
    if not box:
        return out
    dts = box.find_all("dt")
    dds = box.find_all("dd")
    for dt, dd in zip(dts, dds):
        k = (dt.get_text(" ", strip=True) or "").strip().strip(":")
        v = (dd.get_text(" ", strip=True) or "").strip()
        if k and v:
            out[k] = v
    return out


def extract_price(sp: BeautifulSoup) -> int | None:
    """Сырой supplier price."""
    for sel in (
        "span.price_main b",
        "span.price_main",
        "span.price_value",
        "div.price b",
        "div.price",
        "[itemprop=price]",
    ):
        el = sp.select_one(sel)
        if el and el.get_text(strip=True):
            p = _parse_price_int(el.get_text(" ", strip=True))
            if p:
                return p

    for tag, attrs in (
        ("meta", {"property": "product:price:amount"}),
        ("meta", {"itemprop": "price"}),
        ("meta", {"name": "price"}),
    ):
        meta = sp.find(tag, attrs=attrs)
        if meta and meta.get("content"):
            p = _parse_price_int(str(meta.get("content")))
            if p:
                return p

    for script in sp.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(script.get_text(strip=True) or "{}")
        except Exception:
            continue

        def _walk(x: Any) -> int | None:
            if isinstance(x, dict):
                if "offers" in x and isinstance(x["offers"], dict):
                    price = x["offers"].get("price") or x["offers"].get("lowPrice")
                    if price:
                        return _parse_price_int(str(price))
                if "price" in x:
                    return _parse_price_int(str(x["price"]))
                for v in x.values():
                    r = _walk(v)
                    if r is not None:
                        return r
            elif isinstance(x, list):
                for v in x:
                    r = _walk(v)
                    if r is not None:
                        return r
            return None

        p = _walk(data)
        if p is not None:
            return p

    txt = sp.get_text(" ", strip=True)
    m = re.search(r"\bЦена\b[^\d]{0,20}([0-9][0-9\s]{2,})\s*(?:тг|₸)?", txt, flags=re.I)
    if m:
        p = _parse_price_int(m.group(1))
        if p:
            return p

    return None


def extract_title(sp: BeautifulSoup) -> str:
    el = sp.select_one(".page_title") or sp.title or sp.find("h1")
    txt = el.get_text(" ", strip=True) if el else ""
    return (txt or "").strip()


def extract_meta_desc(sp: BeautifulSoup) -> str:
    meta = sp.find("meta", attrs={"name": "description"}) or sp.find(
        "meta", attrs={"property": "og:description"}
    )
    out = (meta.get("content") if meta else "") or ""
    return re.sub(r"\s+", " ", out).strip()


def extract_body_text(sp: BeautifulSoup) -> str:
    """Человеческий текст карточки без params-table."""
    for sel in (
        "div.catalog_item_descr > div",
        "div.catalog_item_descr",
        "div.catalog_item",
        "article",
    ):
        el = sp.select_one(sel)
        if not el:
            continue
        txt = el.get_text(" ", strip=True)
        txt = re.sub(r"\s+", " ", txt).strip()
        if txt and len(txt) >= 60:
            return txt
    return ""


def extract_pictures(cfg: VttSourceCfg, sp: BeautifulSoup, limit: int = 8) -> list[str]:
    """Сырые картинки карточки, без supplier-очистки business-слоя."""
    bad_host_snips = (
        "mc.yandex.ru",
        "metrika.yandex",
        "google-analytics.com",
        "googletagmanager.com",
        "doubleclick.net",
    )
    bad_path_snips = ("/watch/", "pixel", "counter", "collect", "favicon")
    img_ext_re = re.compile(r"\.(jpg|jpeg|png|webp|gif|bmp|tif|tiff)(\?|#|$)", re.I)
    allowed_path_snips = ("/upload/", "/images/", "/img/", "/image/", "/files/", "/components/")

    def _is_good_img(u: str) -> bool:
        lu = (u or "").strip().lower()
        if not lu:
            return False
        if lu.startswith("data:"):
            return False
        if any(x in lu for x in bad_host_snips):
            return False
        if any(x in lu for x in bad_path_snips):
            return False
        if img_ext_re.search(lu):
            return any(x in lu for x in allowed_path_snips)
        return any(x in lu for x in allowed_path_snips)

    def _push(out: list[str], url: str) -> None:
        url = (url or "").strip()
        if not url:
            return
        abs_u = _abs_url(cfg, url)
        if _is_good_img(abs_u) and abs_u not in out:
            out.append(abs_u)

    out: list[str] = []

    for a in sp.select(
        "div.catalog_item_pic a.glightbox[href], "
        "div.carousel-item a.glightbox[href], "
        "a.glightbox[data-gallery][href]"
    ):
        href = a.get("href") or ""
        _push(out, href)
        if len(out) >= limit:
            return out

    meta = sp.find("meta", attrs={"property": "og:image"})
    if meta and meta.get("content"):
        _push(out, meta.get("content"))

    for img in sp.find_all("img"):
        for attr in ("src", "data-src", "data-lazy", "data-original", "srcset", "data-srcset"):
            val = img.get(attr)
            if not val:
                continue
            if "srcset" in attr:
                first = str(val).split(",")[0].strip().split(" ")[0].strip()
                _push(out, first)
            else:
                _push(out, str(val))

    for a in sp.find_all("a"):
        href = a.get("href")
        if href and img_ext_re.search(str(href).lower()):
            _push(out, str(href))

    return out[:limit]


def parse_product_page(
    s: requests.Session,
    cfg: VttSourceCfg,
    url: str,
    *,
    category_code: str = "",
) -> dict[str, Any] | None:
    """
    Парсит карточку VTT в сырой supplier-item.
    Без нормализации params / compat / OfferOut.
    """
    b = _get_bytes(s, cfg, url)
    if not b:
        return None
    sp = _soup(b)

    name = extract_title(sp)
    if not name:
        return None

    pairs = extract_pairs(sp)
    pics = extract_pictures(cfg, sp)
    meta_desc = extract_meta_desc(sp)
    body_txt = extract_body_text(sp)

    params: list[tuple[str, str]] = []
    for k, v in pairs.items():
        kk = str(k or "").strip()
        vv = str(v or "").strip()
        if kk and vv:
            params.append((kk, vv))

    return {
        "url": url,
        "category_code": category_code,
        "available": True,
        "name": name,
        "vendor": (pairs.get("Вендор") or "").strip(),
        "article": (pairs.get("Артикул") or pairs.get("Партс-номер") or "").strip(),
        "supplier_price": extract_price(sp),
        "pictures": pics,
        "description_meta": meta_desc,
        "description_body": body_txt,
        "params": params,
    }


def parse_product_page_from_index(
    s: requests.Session,
    cfg: VttSourceCfg,
    index_item: dict[str, Any],
) -> dict[str, Any] | None:
    """Удобный wrapper для item из product-index."""
    url = str(index_item.get("url") or "").strip()
    code = str(index_item.get("category_code") or "").strip()
    if not url:
        return None
    return parse_product_page(s, cfg, url, category_code=code)
