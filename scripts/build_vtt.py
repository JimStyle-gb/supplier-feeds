# -*- coding: utf-8 -*-
"""
CS adapter: VTT (b2b.vtt.ru)

Правило CS: адаптер только собирает данные (name/price/pictures/params/native_desc) и отдаёт в cs.core.
Всё остальное (описание/WhatsApp/Характеристики/keywords/цены/формат) делает core.

ENV (GitHub Actions secrets/env):
- VTT_LOGIN, VTT_PASSWORD

Опционально:
- VTT_BASE_URL              (default https://b2b.vtt.ru)
- VTT_START_URL             (default https://b2b.vtt.ru/catalog/)
- VTT_CATEGORIES            (csv of category urls; if empty uses встроенный список)
- VTT_MAX_PAGES             (default 200)
- VTT_MAX_WORKERS           (default 10)
- VTT_MAX_CRAWL_MINUTES     (default 18)
- VTT_REQUEST_DELAY_MS      (default 80)
- VTT_SSL_VERIFY            (1/0; default 1)
- VTT_CA_BUNDLE             (path to CA bundle; if set uses as verify)
- VTT_SOFTFAIL              (1/0; default 1)  # при 503/таймаутах не портим docs/vtt.yml, завершаем job успешно
"""

from __future__ import annotations

import os
import re
import sys
import sys
import time
import random
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from cs.core import (
    OfferOut,
    compute_price,
    clean_params,
    ensure_footer_spacing,
    make_feed_meta,
    make_footer,
    make_header,
    now_almaty,
    safe_int,
    norm_ws,
    write_if_changed,
    validate_cs_yml,
)

SUPPLIER = "VTT"
OID_PREFIX = "VT"
OUT_FILE = "docs/vtt.yml"

_DEFAULT_CATEGORIES: list[str] = [
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_COMPAT",
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_ORIG",
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_PRNTHD",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_COMPAT",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_COPY",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_ORIG",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_PRINT",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_TNR",
    "https://b2b.vtt.ru/catalog/?category=CARTMAT_CART",
    "https://b2b.vtt.ru/catalog/?category=DEV_DEV",
    "https://b2b.vtt.ru/catalog/?category=DRM_CRT",
    "https://b2b.vtt.ru/catalog/?category=DRM_UNIT",
    "https://b2b.vtt.ru/catalog/?category=PARTSPRINT_THERBLC",
    "https://b2b.vtt.ru/catalog/?category=PARTSPRINT_THERELT",
]

_PRODUCT_HREF_RE = re.compile(r"^/catalog/[^?]+/?$")


def _log(msg: str) -> None:
    print(msg, flush=True)


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



def _next_run_dom(now: datetime, hour: int, doms: list[int]) -> datetime:
    # Следующий запуск по дням месяца (в Алматы), например [1,10,20] в 05:00
    allowed = sorted({int(d) for d in doms if 1 <= int(d) <= 31})
    y, m = now.year, now.month
    for _ in range(0, 24):  # до 2 лет вперёд — более чем достаточно
        for d in allowed:
            try:
                cand = datetime(y, m, d, hour, 0, 0)
            except ValueError:
                continue
            if cand > now:
                return cand
        # следующий месяц
        m += 1
        if m == 13:
            m = 1
            y += 1
    return now + timedelta(days=31)

@dataclass(frozen=True)
class _Cfg:
    base_url: str
    start_url: str
    categories: list[str]
    login: str
    password: str
    max_pages: int
    max_workers: int
    max_crawl_minutes: float
    delay_ms: int
    verify: object  # bool|str (requests)
    softfail: bool


def _cfg() -> _Cfg:
    base = (os.getenv("VTT_BASE_URL", "https://b2b.vtt.ru") or "").strip().rstrip("/")
    start = (os.getenv("VTT_START_URL", f"{base}/catalog/") or "").strip()
    cats_raw = (os.getenv("VTT_CATEGORIES", "") or "").strip()
    cats = [c.strip() for c in cats_raw.split(",") if c.strip()] if cats_raw else list(_DEFAULT_CATEGORIES)

    login = (os.getenv("VTT_LOGIN", "") or "").strip()
    password = (os.getenv("VTT_PASSWORD", "") or "").strip()

    ssl_verify = _env_bool("VTT_SSL_VERIFY", True)
    ca_bundle = (os.getenv("VTT_CA_BUNDLE", "") or "").strip()
    verify: object = ca_bundle if ca_bundle else ssl_verify

    return _Cfg(
        base_url=base,
        start_url=start,
        categories=cats,
        login=login,
        password=password,
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


def _make_session(cfg: _Cfg) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Accept-Language": "ru,en;q=0.8",
        }
    )
    # requests verify передадим на каждый запрос, чтобы было прозрачно
    return s


def _request(
    s: requests.Session,
    cfg: _Cfg,
    method: str,
    url: str,
    *,
    timeout: int = 25,
    data: dict | None = None,
    headers: dict | None = None,
) -> requests.Response | None:
    # шаблонный retry/backoff (5xx/таймауты)
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
            _log(f"[http] {method} {url} fail: {e}{' (last)' if last else ''}")
            if last:
                return None
            time.sleep(min(12.0, 0.6 * (2**i)) + random.uniform(0.0, 0.6))
    return None


def _get_bytes(s: requests.Session, cfg: _Cfg, url: str, *, timeout: int = 25) -> bytes | None:
    r = _request(s, cfg, "GET", url, timeout=timeout)
    if not r or r.status_code != 200:
        return None
    _sleep_ms(cfg.delay_ms)
    return r.content


def _post_ok(
    s: requests.Session,
    cfg: _Cfg,
    url: str,
    *,
    data: dict,
    headers: dict | None = None,
    timeout: int = 25,
) -> bool:
    r = _request(s, cfg, "POST", url, timeout=timeout, data=data, headers=headers)
    ok = bool(r and r.status_code in (200, 204))
    _sleep_ms(cfg.delay_ms)
    return ok


def _abs_url(cfg: _Cfg, href: str) -> str:
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


def _login(s: requests.Session, cfg: _Cfg) -> bool:
    if not cfg.login or not cfg.password:
        _log("[WARN] VTT_LOGIN/VTT_PASSWORD пустые")
        return False

    home = _get_bytes(s, cfg, cfg.base_url + "/")
    if not home:
        return False

    csrf = _extract_csrf_token(home)
    headers = {"Referer": cfg.base_url + "/"}
    if csrf:
        headers["X-CSRF-TOKEN"] = csrf

    ok = _post_ok(
        s,
        cfg,
        cfg.base_url + "/validateLogin",
        data={"login": cfg.login, "password": cfg.password},
        headers=headers,
    )
    if not ok:
        return False

    cat = _get_bytes(s, cfg, cfg.start_url)
    return bool(cat)


def _category_code(category_url: str) -> str:
    q = parse_qs(urlparse(category_url).query)
    return (q.get("category", [""]) or [""])[0].strip()


def _collect_links_in_category(s: requests.Session, cfg: _Cfg, category_url: str, deadline_utc: datetime) -> list[str]:
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


def _collect_all_links(s: requests.Session, cfg: _Cfg, deadline_utc: datetime) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for cu in cfg.categories:
        if datetime.utcnow() >= deadline_utc:
            break
        code = _category_code(cu)
        links = _collect_links_in_category(s, cfg, cu, deadline_utc)
        _log(f"[site] category={code or '?'} links={len(links)}")
        for u in links:
            out.append((u, code))
    return out


def _parse_int(text: str) -> int | None:
    if not text:
        return None
    s = re.sub(r"[^0-9]+", "", text)
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _parse_price_int(text: str) -> int | None:
    """Парс цены: понимает '2 449.22', '2 449,22', '2449', '2 449'. Возвращает целую часть."""
    if not text:
        return None
    s = str(text).replace("\u00a0", " ").replace("&nbsp;", " ")
    s = s.strip()
    # оставляем цифры, разделители и пробелы
    s = re.sub(r"[^0-9.,\s]+", "", s)
    s = re.sub(r"\s+", "", s)
    if not s:
        return None

    # нормализуем: если есть и '.' и ',', считаем последнюю как десятичную
    if "." in s and "," in s:
        # десятичный разделитель — тот, что ближе к концу
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "")
            s = s.replace(",", ".")
    else:
        # только запятая
        if "," in s and "." not in s:
            # если 1 запятая и после неё ровно 2 цифры — это десятичные
            if s.count(",") == 1 and len(s.split(",")[1]) == 2:
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
        # только точка
        if "." in s and "," not in s:
            if s.count(".") == 1 and len(s.split(".")[1]) == 2:
                pass
            else:
                s = s.replace(".", "")

    try:
        # берём целую часть
        if "." in s:
            s = s.split(".", 1)[0]
        s = s.lstrip("0") or "0"
        return int(s)
    except Exception:
        return None


def _extract_pairs(sp: BeautifulSoup) -> dict[str, str]:
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


def _extract_price(sp: BeautifulSoup) -> int | None:
    # основной селектор
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

    # мета-цена (если есть)
    for meta_sel in (
        ("meta", {"property": "product:price:amount"}),
        ("meta", {"itemprop": "price"}),
        ("meta", {"name": "price"}),
    ):
        meta = sp.find(meta_sel[0], attrs=meta_sel[1])
        if meta and meta.get("content"):
            p = _parse_price_int(str(meta.get("content")))
            if p:
                return p

    # json-ld
    for script in sp.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(script.get_text(strip=True) or "{}")
        except Exception:
            continue

        def _walk(x):
            if isinstance(x, dict):
                if "offers" in x and isinstance(x["offers"], dict):
                    price = x["offers"].get("price") or x["offers"].get("lowPrice")
                    if price:
                        return _parse_int(str(price))
                if "price" in x:
                    return _parse_int(str(x["price"]))
                for v in x.values():
                    r = _walk(v)
                    if r:
                        return r
            elif isinstance(x, list):
                for v in x:
                    r = _walk(v)
                    if r:
                        return r
            return None

        p = _walk(data)
        if p:
            return p

    # мягкий fallback по тексту рядом со словом "Цена"
    txt = sp.get_text(" ", strip=True)
    m = re.search(r"\bЦена\b[^\d]{0,20}([0-9][0-9\s]{2,})\s*(?:тг|₸)?", txt, flags=re.I)
    if m:
        p = _parse_price_int(m.group(1))
        if p:
            return p

    return None

def _extract_title(sp: BeautifulSoup) -> str:
    el = sp.select_one(".page_title") or sp.title or sp.find("h1")
    txt = el.get_text(" ", strip=True) if el else ""
    return (txt or "").strip()


def _extract_meta_desc(sp: BeautifulSoup) -> str:
    meta = sp.find("meta", attrs={"name": "description"}) or sp.find("meta", attrs={"property": "og:description"})
    out = (meta.get("content") if meta else "") or ""
    return re.sub(r"\s+", " ", out).strip()


def _extract_body_text(sp: BeautifulSoup) -> str:
    # пробуем взять человеческий текст карточки (не таблицу dt/dd)
    for sel in ("div.catalog_item_descr > div", "div.catalog_item_descr", "div.catalog_item", "article"):
        el = sp.select_one(sel)
        if not el:
            continue
        txt = el.get_text(" ", strip=True)
        txt = re.sub(r"\s+", " ", txt).strip()
        if txt and len(txt) >= 60:
            return txt
    return ""


def _extract_pictures(cfg: _Cfg, sp: BeautifulSoup, limit: int = 8) -> list[str]:
    # реальные картинки, без пикселей/счётчиков (yandex metrika и т.п.)
    BAD_HOST_SNIPS = (
        "mc.yandex.ru",
        "metrika.yandex",
        "google-analytics.com",
        "googletagmanager.com",
        "doubleclick.net",
    )
    BAD_PATH_SNIPS = (
        "/watch/",
        "pixel",
        "counter",
        "collect",
        "favicon",
    )
    IMG_EXT_RE = re.compile(r"\.(jpg|jpeg|png|webp|gif|bmp|tif|tiff)(\?|#|$)", re.I)
    ALLOWED_PATH_SNIPS = (
        "/upload/",
        "/images/",
        "/img/",
        "/image/",
        "/files/",
        "/components/",
    )

    def _is_good_img(u: str) -> bool:
        lu = (u or "").strip().lower()
        if not lu:
            return False
        if lu.startswith("data:"):
            return False
        if any(x in lu for x in BAD_HOST_SNIPS):
            return False
        if any(x in lu for x in BAD_PATH_SNIPS):
            return False
        if IMG_EXT_RE.search(lu):
            # берём только из типичных папок (чтобы не ловить favicon/пиксели)
            if any(x in lu for x in ALLOWED_PATH_SNIPS):
                return True
            return False
        # иногда без расширения, но в типичных папках
        if any(x in lu for x in ALLOWED_PATH_SNIPS):
            return True
        return False

    def _push(out: list[str], url: str):
        url = (url or "").strip()
        if not url:
            return
        abs_url = _abs_url(cfg, url)
        if _is_good_img(abs_url) and abs_url not in out:
            out.append(abs_url)

    out: list[str] = []

    # галерея товара (самый точный источник)
    for a in sp.select("div.catalog_item_pic a.glightbox[href], div.carousel-item a.glightbox[href], a.glightbox[data-gallery][href]"):
        href = a.get("href") or ""
        _push(out, href)
        if len(out) >= limit:
            return out

    # og:image
    meta = sp.find("meta", attrs={"property": "og:image"})
    if meta and meta.get("content"):
        _push(out, meta.get("content"))

    # img src / lazy attrs
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

    # <a href="...jpg">
    for a in sp.find_all("a"):
        href = a.get("href")
        if href and IMG_EXT_RE.search(str(href).lower()):
            _push(out, str(href))

    if not out:
        out = ["https://placehold.co/800x800/png?text=No+Photo"]

    return out[:limit]

def _ru_to_lat_ascii(s: str) -> str:
    # минимально: русские "Х" иногда попадают в артикулах -> сделаем стабильнее
    table = str.maketrans(
        {
            "А": "A",
            "В": "B",
            "Е": "E",
            "К": "K",
            "М": "M",
            "Н": "H",
            "О": "O",
            "Р": "P",
            "С": "C",
            "Т": "T",
            "Х": "X",
            "а": "a",
            "е": "e",
            "о": "o",
            "р": "p",
            "с": "c",
            "х": "x",
        }
    )
    return (s or "").translate(table)


def _clean_article(article: str) -> str:
    s = _ru_to_lat_ascii((article or "").strip())
    return re.sub(r"[^A-Za-z0-9_-]+", "", s)


def _normalize_vendor(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    key = re.sub(r"[^a-z0-9]+", "", v.lower())
    alias = {
        "hewlettpackard": "HP",
        "hp": "HP",
        "kyocera": "Kyocera",
        "canon": "Canon",
        "xerox": "Xerox",
        "brother": "Brother",
        "samsung": "Samsung",
        "epson": "Epson",
        "ricoh": "Ricoh",
        "konica": "Konica Minolta",
    }
    return alias.get(key, v)


def _clone_session_with_cookies(src: requests.Session, cfg: _Cfg) -> requests.Session:
    s2 = _make_session(cfg)
    try:
        s2.cookies.update(src.cookies)
    except Exception:
        pass
    return s2


def _parse_product(s: requests.Session, cfg: _Cfg, url: str, cat_code: str) -> OfferOut | None:
    b = _get_bytes(s, cfg, url)
    if not b:
        return None
    sp = _soup(b)

    name = norm_ws(_extract_title(sp))
    if not name:
        return None

    pairs = _extract_pairs(sp)
    article = (pairs.get("Артикул") or pairs.get("Партс-номер") or "").strip()
    if not article:
        return None

    article_clean = _clean_article(article)
    if not article_clean:
        return None

    oid = OID_PREFIX + article_clean

    vendor = _normalize_vendor((pairs.get("Вендор") or "").strip())

    supplier_price = _extract_price(sp)
    price = compute_price(safe_int(supplier_price))

    pics = _extract_pictures(cfg, sp)

    # params: служебное выкидываем, остальное отдаём core
    drop = {"артикул", "партс-номер", "вендор", "цена", "стоимость", "категория", "подкатегория", "штрих-код", "штрихкод", "ean", "barcode"}
    params: list[tuple[str, str]] = []
    for k, v in pairs.items():
        kk = norm_ws(k)
        vv = norm_ws(v)
        if not kk or not vv:
            continue
        if kk.casefold() in drop:
            continue
        params.append((kk, vv))

    params = clean_params(params)

    # native_desc: максимум источников (core сам доведёт под CS-шаблон)
    meta_desc = _extract_meta_desc(sp)
    body_txt = _extract_body_text(sp)
    native_desc = meta_desc
    if body_txt and body_txt not in (native_desc or ""):
        native_desc = (native_desc + "\n" + body_txt).strip() if native_desc else body_txt

    return OfferOut(
        oid=oid,
        available=True,
        name=name,
        price=int(price),
        pictures=pics,
        vendor=vendor,
        params=params,
        native_desc=native_desc,
        # categoryId добавишь позже в master yml; здесь оставляем пусто/как в core
    )


def _build_offers(s: requests.Session, cfg: _Cfg, deadline_utc: datetime) -> tuple[list[OfferOut], int]:
    links = _collect_all_links(s, cfg, deadline_utc)
    _log(f"[site] urls={len(links)} workers={cfg.max_workers}")

    offers: list[OfferOut] = []
    seen: set[str] = set()
    dup = 0

    if not links:
        return offers, dup

    with ThreadPoolExecutor(max_workers=max(1, cfg.max_workers)) as ex:
        futs = []
        for url, code in links:
            if datetime.utcnow() >= deadline_utc:
                break
            sess = _clone_session_with_cookies(s, cfg)
            futs.append(ex.submit(_parse_product, sess, cfg, url, code))

        for fut in as_completed(futs):
            o = fut.result()
            if not o:
                continue
            if o.oid in seen:
                dup += 1
                continue
            seen.add(o.oid)
            offers.append(o)

    offers.sort(key=lambda x: x.oid)
    return offers, dup


def main() -> int:
    cfg = _cfg()
    now = now_almaty()
    deadline = datetime.utcnow() + timedelta(minutes=cfg.max_crawl_minutes)

    s = _make_session(cfg)

    if not _login(s, cfg):
        msg = "VTT: авторизация не прошла (проверь VTT_LOGIN/VTT_PASSWORD). Если в логах 503/5xx — проблема на стороне сайта."
        if cfg.softfail:
            _log("[SOFTFAIL] " + msg)
            return 0
        raise RuntimeError(msg)

    offers, dup = _build_offers(s, cfg, deadline)

    if not offers:
        msg = "VTT: 0 offers (скорее всего сайт недоступен/503 или изменили верстку)."
        if cfg.softfail:
            _log("[SOFTFAIL] " + msg)
            return 0
        raise RuntimeError(msg)

    # VTT по расписанию 1/10/20 (05:00 Алматы).
    next_run = _next_run_dom(now, 5, [1, 10, 20])

    feed_meta = make_feed_meta(
        supplier=SUPPLIER,
        supplier_url=cfg.start_url,
        build_time=now,
        next_run=next_run,
        before=len(offers),
        after=len(offers),
        in_true=len(offers),
        in_false=0,
    )

    header = make_header(now, encoding="utf-8")
    footer = make_footer()

    offers_xml = "\n\n".join(o.to_xml(currency_id="KZT", public_vendor="CS") for o in offers)
    full = header + feed_meta + "\n\n" + offers_xml + ("\n" if offers_xml else "") + footer
    full = ensure_footer_spacing(full)

    # CS-валидация (не пишем мусор)
    validate_cs_yml(full)

    changed = write_if_changed(OUT_FILE, full, encoding="utf-8")
    _log(f"[done] offers={len(offers)} dup_skipped={dup} changed={changed} out={OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
