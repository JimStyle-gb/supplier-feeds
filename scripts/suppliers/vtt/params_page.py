# -*- coding: utf-8 -*-
"""VTT params_page layer — price-fix based on supplier HTML.

Основано на реальном supplier file:
- основная цена на VTT живёт в видимом KZT-блоке:
  span.price_main > b.price-cart-amount__rub
- hidden input name="amount" содержит не KZT-цену и не должен использоваться
- цена должна парситься только из видимых KZT-блоков / meta / json-ld,
  без ложного fallback на hidden amount
"""

from __future__ import annotations

import json
import re
from bs4 import BeautifulSoup

from suppliers.vtt.source import abs_url, soup_from_bytes, get_bytes

_RE_IMG_EXT = re.compile(r'\.(jpg|jpeg|png|webp|gif|bmp|tif|tiff)(\?|#|$)', re.I)


def parse_int(text: str) -> int | None:
    if not text:
        return None
    s = re.sub(r"[^0-9]+", "", text)
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def parse_price_int(text: str) -> int | None:
    if not text:
        return None
    s = str(text).replace("\u00a0", " ").replace("&nbsp;", " ").strip()
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
            # для VTT "10 336.64" надо взять 10336
            # decimal-хвост просто отбрасываем позже
            pass

    try:
        if "." in s:
            s = s.split(".", 1)[0]
        s = s.lstrip("0") or "0"
        return int(s)
    except Exception:
        return None


def extract_pairs(sp: BeautifulSoup) -> dict[str, str]:
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


def _price_from_visible_kzt_block(sp: BeautifulSoup) -> int | None:
    # Реальный supplier pattern из сохранённого товара:
    # <span class="price_main"><b class="price-cart-amount__rub">10 336.64</b> T</span>
    selectors = (
        "span.price_main b.price-cart-amount__rub",
        "div.item_data_price span.price_main b.price-cart-amount__rub",
        "span.price_main b",
        "span.price_main",
        "div.item_data_price span.price_main",
        ".catalog_item_right .item_data_price .price_main",
    )
    for sel in selectors:
        el = sp.select_one(sel)
        if not el:
            continue
        txt = el.get_text(" ", strip=True)
        p = parse_price_int(txt)
        if p and p > 0:
            return p
    return None


def _price_from_meta(sp: BeautifulSoup) -> int | None:
    for meta_sel in (
        ("meta", {"property": "product:price:amount"}),
        ("meta", {"itemprop": "price"}),
        ("meta", {"name": "price"}),
    ):
        meta = sp.find(meta_sel[0], attrs=meta_sel[1])
        if meta and meta.get("content"):
            p = parse_price_int(str(meta.get("content")))
            if p and p > 0:
                return p
    return None


def _price_from_json_ld(sp: BeautifulSoup) -> int | None:
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
                        return parse_price_int(str(price))
                if "price" in x:
                    return parse_price_int(str(x["price"]))
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
        if p and p > 0:
            return p
    return None


def _price_from_text(sp: BeautifulSoup) -> int | None:
    txt = sp.get_text(" ", strip=True)
    m = re.search(r"\bЦена\b[^\d]{0,20}([0-9][0-9\s.,]{2,})\s*(?:тг|₸|t|тенге)?", txt, flags=re.I)
    if m:
        p = parse_price_int(m.group(1))
        if p and p > 0:
            return p
    return None


def extract_price(sp: BeautifulSoup) -> int | None:
    # 1) supplier truth: видимый KZT-блок
    p = _price_from_visible_kzt_block(sp)
    if p:
        return p

    # 2) structured meta
    p = _price_from_meta(sp)
    if p:
        return p

    # 3) json-ld
    p = _price_from_json_ld(sp)
    if p:
        return p

    # 4) мягкий текстовый fallback
    p = _price_from_text(sp)
    if p:
        return p

    # ВАЖНО: hidden input name="amount" специально НЕ парсим.
    return None


def extract_title(sp: BeautifulSoup) -> str:
    el = sp.select_one(".page_title") or sp.title or sp.find("h1")
    txt = el.get_text(" ", strip=True) if el else ""
    return (txt or "").strip()


def extract_meta_desc(sp: BeautifulSoup) -> str:
    meta = sp.find("meta", attrs={"name": "description"}) or sp.find("meta", attrs={"property": "og:description"})
    out = (meta.get("content") if meta else "") or ""
    return re.sub(r"\s+", " ", out).strip()


def extract_body_text(sp: BeautifulSoup) -> str:
    for sel in ("div.catalog_item_descr > div", "div.catalog_item_descr", "div.catalog_item", "article"):
        el = sp.select_one(sel)
        if not el:
            continue
        txt = el.get_text(" ", strip=True)
        txt = re.sub(r"\s+", " ", txt).strip()
        if txt and len(txt) >= 60:
            return txt
    return ""


def extract_pictures(cfg, sp: BeautifulSoup, limit: int = 8) -> list[str]:
    BAD_HOST_SNIPS = (
        "mc.yandex.ru",
        "metrika.yandex",
        "google-analytics.com",
        "googletagmanager.com",
        "doubleclick.net",
    )
    BAD_PATH_SNIPS = ("watch", "pixel", "counter", "collect", "favicon")
    ALLOWED_PATH_SNIPS = ("/upload/", "/images/", "/img/", "/image/", "/files/", "/components/")

    def _is_good_img(u: str) -> bool:
        lu = (u or "").strip().lower()
        if not lu or lu.startswith("data:"):
            return False
        if any(x in lu for x in BAD_HOST_SNIPS):
            return False
        if any(x in lu for x in BAD_PATH_SNIPS):
            return False
        if _RE_IMG_EXT.search(lu):
            return any(x in lu for x in ALLOWED_PATH_SNIPS)
        return any(x in lu for x in ALLOWED_PATH_SNIPS)

    def _push(out: list[str], url: str):
        url = (url or "").strip()
        if not url:
            return
        absu = abs_url(cfg, url)
        if _is_good_img(absu) and absu not in out:
            out.append(absu)

    out: list[str] = []

    for a in sp.select("div.catalog_item_pic a.glightbox[href], div.carousel-item a.glightbox[href], a.glightbox[data-gallery][href]"):
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
        if href and _RE_IMG_EXT.search(str(href).lower()):
            _push(out, str(href))

    if not out:
        out = ["https://placehold.co/800x800/png?text=No+Photo"]

    return out[:limit]
