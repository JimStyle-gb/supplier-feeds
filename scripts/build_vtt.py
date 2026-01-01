#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CS adapter: VTT (b2b.vtt.ru)

Цель: вытащить товары у VTT, а дальше отдать их в CS Core, чтобы:
- описание/характеристики/keywords/WhatsApp/цены/формат были едины для всех поставщиков
- OID был стабильным (чтобы в будущих коммитах НЕ создавались новые товары)

Окружение (GitHub Actions secrets/env):
- VTT_LOGIN, VTT_PASSWORD   (логин/пароль b2b.vtt.ru)
Опционально:
- VTT_BASE_URL              (по умолчанию https://b2b.vtt.ru)
- VTT_START_URL             (по умолчанию https://b2b.vtt.ru/catalog/)
- VTT_MAX_PAGES             (лимит страниц внутри категории, по умолчанию 200)
- VTT_MAX_WORKERS           (параллельные запросы карточек, по умолчанию 10)
- VTT_MAX_CRAWL_MINUTES     (тайм‑лимит на обход, по умолчанию 18)
- VTT_REQUEST_DELAY_MS      (пауза между запросами, по умолчанию 80)
- VTT_SSL_VERIFY            (1/0; по умолчанию 1)
- VTT_CA_BUNDLE             (путь к CA bundle; если задан, используется как verify)
"""

from __future__ import annotations

import os
import re
import sys
import time
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import requests
from bs4 import BeautifulSoup

from cs.core import (
    OfferOut,
    compute_price,
    clean_params,
    now_almaty,
    next_run_at_hour,
    make_header,
    make_footer,
    make_feed_meta,
    ensure_footer_spacing,
    norm_ws,
    safe_int,
    write_if_changed,
)

SUPPLIER = "VTT"
OUT_FILE = "docs/vtt.yml"

BASE_URL = (os.getenv("VTT_BASE_URL", "https://b2b.vtt.ru") or "").strip().rstrip("/")
START_URL = (os.getenv("VTT_START_URL", f"{BASE_URL}/catalog/") or "").strip()

# Ссылки категорий (взято из последнего рабочего скрипта)
CATEGORIES: list[str] = [
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
    "https://b2b.vtt.ru/catalog/?category=PARTSPRINT_THERELT"
]

LOGIN = (os.getenv("VTT_LOGIN", "") or "").strip()
PASSWORD = (os.getenv("VTT_PASSWORD", "") or "").strip()

MAX_PAGES = int(os.getenv("VTT_MAX_PAGES", "200"))
MAX_WORKERS = int(os.getenv("VTT_MAX_WORKERS", "10"))
MAX_CRAWL_MINUTES = float(os.getenv("VTT_MAX_CRAWL_MINUTES", "18"))
REQUEST_DELAY_MS = int(os.getenv("VTT_REQUEST_DELAY_MS", "80"))

SSL_VERIFY = (os.getenv("VTT_SSL_VERIFY", "1") or "1").strip().lower() not in ("0", "false", "no")
CA_BUNDLE = (os.getenv("VTT_CA_BUNDLE", "") or "").strip()

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

# Стабильный префикс OID для VTT
OID_PREFIX = "VT"


def log(msg: str, *, flush: bool = True) -> None:
    print(msg, file=sys.stderr, flush=flush)


def _verify_value():
    if CA_BUNDLE:
        return CA_BUNDLE
    return SSL_VERIFY


def jitter_sleep(ms: int) -> None:
    if ms <= 0:
        return
    base = ms / 1000.0
    time.sleep(base + random.uniform(0, base * 0.35))


def make_session() -> requests.Session:
    s = requests.Session()
if not VTT_SSL_VERIFY:
    s.verify = False
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    })
    return s


def abs_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if u.startswith("//"):
        return "https:" + u
    if not u.startswith("/"):
        u = "/" + u
    return BASE_URL + u


def set_query_param(url: str, key: str, value: str) -> str:
    pr = urlparse(url)
    q = parse_qs(pr.query, keep_blank_values=True)
    q[key] = [value]
    return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, urlencode(q, doseq=True), pr.fragment))


def soup_of(html_bytes: bytes) -> BeautifulSoup:
    return BeautifulSoup(html_bytes or b"", "html.parser")


def http_get(s: requests.Session, url: str) -> bytes | None:
    last_err: Exception | None = None
    for _ in range(3):
        try:
            r = s.get(url, timeout=25, verify=_verify_value())
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {r.status_code}")
                jitter_sleep(800)
                continue
            r.raise_for_status()
            return r.content
        except Exception as e:
            last_err = e
            jitter_sleep(800)
    log(f"[http] GET fail: {url} | {last_err}")
    return None


def http_post(s: requests.Session, url: str, data: dict[str, str], headers: dict[str, str]) -> bool:
    last_err: Exception | None = None
    for _ in range(3):
        try:
            r = s.post(url, data=data, headers=headers, timeout=25, verify=_verify_value())
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {r.status_code}")
                jitter_sleep(800)
                continue
            r.raise_for_status()
            return True
        except Exception as e:
            last_err = e
            jitter_sleep(800)
    log(f"[http] POST fail: {url} | {last_err}")
    return False


def extract_csrf_token(html_bytes: bytes) -> str:
    soup = soup_of(html_bytes)
    m = soup.find("meta", attrs={"name": "csrf-token"})
    v = m.get("content") if m else ""
    return (v or "").strip()


def log_in(s: requests.Session) -> bool:
    if not LOGIN or not PASSWORD:
        log("[WARN] VTT_LOGIN/VTT_PASSWORD пустые")
        return False

    home = http_get(s, BASE_URL + "/")
    if not home:
        return False

    csrf = extract_csrf_token(home)
    headers = {"Referer": BASE_URL + "/"}
    if csrf:
        headers["X-CSRF-TOKEN"] = csrf

    ok = http_post(
        s,
        BASE_URL + "/validateLogin",
        data={"login": LOGIN, "password": PASSWORD},
        headers=headers,
    )
    if not ok:
        return False

    cat = http_get(s, START_URL)
    return bool(cat)


_PRODUCT_HREF_RE = re.compile(r"^/catalog/[^?]+/?$")


def collect_product_links(s: requests.Session, category_url: str, deadline: datetime) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()

    for page in range(1, MAX_PAGES + 1):
        if datetime.utcnow() >= deadline:
            break
        page_url = set_query_param(category_url, "page", str(page))
        b = http_get(s, page_url)
        if not b:
            break
        soup = soup_of(b)

        links: list[str] = []
        for a in soup.find_all("a", href=True):
            cls = a.get("class") or []
            if isinstance(cls, list) and "btn_pic" in cls:
                continue

            href = str(a["href"]).strip()
            if not href or href.startswith("#"):
                continue
            if href.startswith("http://") or href.startswith("https://"):
                if BASE_URL not in href:
                    continue
                pu = urlparse(href)
                href = pu.path

            if not _PRODUCT_HREF_RE.match(href):
                continue

            u = abs_url(href)
            if u in seen:
                continue
            seen.add(u)
            links.append(u)

        if not links:
            break

        found.extend(links)
        jitter_sleep(REQUEST_DELAY_MS)

    return found


def _category_code(category_url: str) -> str:
    q = parse_qs(urlparse(category_url).query)
    return (q.get("category", [""]) or [""])[0].strip()


def collect_all_products_links(s: requests.Session, deadline: datetime) -> list[tuple[str, str]]:
    all_links: list[tuple[str, str]] = []
    for cu in CATEGORIES:
        if datetime.utcnow() >= deadline:
            break
        code = _category_code(cu)
        links = collect_product_links(s, cu, deadline)
        for u in links:
            all_links.append((u, code))
        log(f"[site] category={code or '?'} links={len(links)}")
    return all_links


def parse_int_from_text(s: str) -> int | None:
    if not s:
        return None
    s2 = re.sub(r"[^0-9]+", "", s)
    if not s2:
        return None
    try:
        return int(s2)
    except Exception:
        return None


def parse_pairs(soup: BeautifulSoup) -> dict[str, str]:
    out: dict[str, str] = {}
    box = soup.select_one("div.description.catalog_item_descr")
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


def parse_supplier_price(soup: BeautifulSoup) -> int | None:
    b = soup.select_one("span.price_main b")
    if not b:
        return None
    return parse_int_from_text(b.get_text(" ", strip=True))


def get_title(soup: BeautifulSoup) -> str:
    el = soup.select_one(".page_title") or soup.title or soup.find("h1")
    txt = el.get_text(" ", strip=True) if el else ""
    return (txt or "").strip()


def get_meta_description(soup: BeautifulSoup) -> str:
    meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    out = (meta.get("content") if meta else "") or ""
    return re.sub(r"\s+", " ", out).strip()


def get_body_text(soup: BeautifulSoup) -> str:
    # Пробуем взять текст карточки товара; если селекторы не совпали — вернём пусто
    for sel in ("div.catalog_item_descr", "div.description", "div.catalog_item", "article"):
        el = soup.select_one(sel)
        if not el:
            continue
        txt = el.get_text(" ", strip=True)
        txt = re.sub(r"\s+", " ", txt).strip()
        if txt and len(txt) >= 40:
            return txt
    return ""


def collect_pictures(soup: BeautifulSoup, limit: int = 8) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    # og:image в приоритете
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        u = abs_url(str(og["content"]))
        if u and u not in seen:
            seen.add(u)
            out.append(u)

    # любые похожие на картинки ссылки/атрибуты
    for tag in soup.find_all(True):
        for attr in ("src", "data-src", "href", "data-img", "content", "data-large"):
            v = tag.get(attr)
            if not v or not isinstance(v, str):
                continue
            vl = v.lower()
            if (".jpg" in vl or ".jpeg" in vl or ".png" in vl or ".webp" in vl) and ("/images/" in vl or "/upload/" in vl):
                u = abs_url(v)
                if u and u not in seen:
                    seen.add(u)
                    out.append(u)
                    if len(out) >= limit:
                        return out
    return out


# Мини‑транслит, чтобы OID был стабилен, даже если артикул внезапно в кириллице
_CYR_TO_LAT = {
    "А":"A","Б":"B","В":"B","Г":"G","Д":"D","Е":"E","Ё":"E","Ж":"ZH","З":"Z","И":"I","Й":"Y","К":"K","Л":"L","М":"M","Н":"N","О":"O","П":"P","Р":"R","С":"S","Т":"T","У":"U","Ф":"F","Х":"H","Ц":"TS","Ч":"CH","Ш":"SH","Щ":"SCH","Ъ":"","Ы":"Y","Ь":"","Э":"E","Ю":"YU","Я":"YA",
    "а":"a","б":"b","в":"b","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ъ":"","ы":"y","ь":"","э":"e","ю":"yu","я":"ya",
}


def _ru_to_lat_ascii(s: str) -> str:
    if not s:
        return ""
    return "".join(_CYR_TO_LAT.get(ch, ch) for ch in s)


def clean_article(article: str) -> str:
    s = (article or "").strip()
    s = _ru_to_lat_ascii(s)
    return re.sub(r"[^A-Za-z0-9_-]+", "", s)


def normalize_vendor(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    key = re.sub(r"[^a-z0-9]+", "", v.lower())
    alias = {
        "hewlettpackard": "HP",
        "hp": "HP",
        "kyoсera": "Kyocera",
        "kyocera": "Kyocera",
        "canon": "Canon",
        "xerox": "Xerox",
        "brother": "Brother",
        "samsung": "Samsung",
        "samsungbyhp": "Samsung",
        "epson": "Epson",
        "ricoh": "Ricoh",
        "konica": "Konica Minolta",
        "minolta": "Konica Minolta",
    }
    return alias.get(key, v)


def parse_product(s: requests.Session, url: str, cat_code: str) -> OfferOut | None:
    b = http_get(s, url)
    if not b:
        return None
    soup = soup_of(b)

    name = norm_ws(get_title(soup))
    if not name:
        return None

    pairs = parse_pairs(soup)
    article = (pairs.get("Артикул") or pairs.get("Партс-номер") or "").strip()
    if not article:
        return None

    vendor = normalize_vendor((pairs.get("Вендор") or "").strip())

    supplier_price = parse_supplier_price(soup)
    price = compute_price(safe_int(supplier_price))

    pics = collect_pictures(soup)

    article_clean = clean_article(article)
    if not article_clean:
        return None

    oid = OID_PREFIX + article_clean

    # params: оставляем полезное, убираем служебное
    drop_keys = {"артикул", "партс-номер", "вендор", "цена", "стоимость"}
    params: list[tuple[str, str]] = []
    for k, v in pairs.items():
        kk = norm_ws(k)
        vv = norm_ws(v)
        if not kk or not vv:
            continue
        if kk.casefold() in drop_keys:
            continue
        params.append((kk, vv))

    params = clean_params(params)

    # native_desc: максимум источников (core сам нормализует/обогащает)
    meta_desc = get_meta_description(soup)
    body_txt = get_body_text(soup)
    native_desc = meta_desc
    if body_txt and body_txt not in native_desc:
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
    )


def _copy_cookies(src: requests.Session) -> requests.Session:
    s2 = make_session()
    try:
        s2.cookies.update(src.cookies)
    except Exception:
        pass
    return s2


def next_run_dom(now_local: datetime, hour: int, allowed_dom: list[int]) -> datetime:
    # ближайшая дата из allowed_dom в 05:00 (Алматы)
    candidates: list[datetime] = []
    for add_months in (0, 1, 2):
        y = now_local.year
        m = now_local.month + add_months
        while m > 12:
            y += 1
            m -= 12
        for d in allowed_dom:
            try:
                dt = datetime(y, m, d, hour, 0, 0)
            except Exception:
                continue
            if dt >= now_local:
                candidates.append(dt)
        if candidates:
            break
    return min(candidates) if candidates else next_run_at_hour(now_local, hour)


def main() -> int:
    now = now_almaty()
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MINUTES)

    s = make_session()
    if not log_in(s):
        raise RuntimeError("VTT: не удалось авторизоваться (проверь VTT_LOGIN/VTT_PASSWORD).")

    links = collect_all_products_links(s, deadline)
    log(f"[site] urls={len(links)} workers={MAX_WORKERS}")

    offers: list[OfferOut] = []
    seen: set[str] = set()
    dup = 0

    if links:
        with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS)) as ex:
            futs = []
            for url, code in links:
                if datetime.utcnow() >= deadline:
                    break
                # отдельная session на поток (с теми же cookies)
                sess = _copy_cookies(s)
                futs.append(ex.submit(parse_product, sess, url, code))
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

    # next run (VTT по расписанию 1/10/20 в 05:00 Алматы)
    next_run = next_run_dom(now, 5, [1, 10, 20])

    feed_meta = make_feed_meta(
        supplier=SUPPLIER,
        supplier_url=START_URL,
        build_time=now,
        next_run=next_run,
        before=len(offers),
        after=len(offers),
        in_true=len(offers),
        in_false=0,
    )

    header = make_header(now, encoding="utf-8")
    footer = make_footer()

    offers_xml = "\n\n".join([o.to_xml() for o in offers])
    full = header + feed_meta + "\n" + offers_xml + ("\n" if offers_xml else "") + footer
    full = ensure_footer_spacing(full)

    changed = write_if_changed(OUT_FILE, full, encoding="utf-8")
    log(f"[done] offers={len(offers)} dup_skipped={dup} changed={changed} out={OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
