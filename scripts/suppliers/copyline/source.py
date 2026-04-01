# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/source.py
CopyLine source layer.

Задача модуля:
- скачать sitemap поставщика;
- собрать product URLs;
- распарсить карточку товара в сырой page-payload;
- сохранить provenance сырья без ранней semantic-свёртки.

В этом модуле НЕТ supplier-business логики:
- нет фильтра по ассортименту;
- нет нормализации vendor/model;
- нет CS-обогащения;
- нет выбора «какой источник параметров главнее».

Важно:
- source.py может делать low-level parsing HTML;
- source.py не должен рано схлопывать происхождение фактов;
- для backward-safe этапа сохраняем legacy-ключи desc/params,
  но дополнительно отдаём raw_desc/raw_desc_pairs/raw_table_params.
"""

from __future__ import annotations

import os
import random
import re
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = (os.getenv("COPYLINE_BASE_URL", "https://copyline.kz") or "https://copyline.kz").rstrip("/")
SITEMAP_URL_DEFAULT = f"{BASE_URL}/site-map.html?id=1&view=html"
SITEMAP_URL = os.getenv("COPYLINE_SITEMAP_URL", SITEMAP_URL_DEFAULT)
SITEMAP_XML_URL = os.getenv("COPYLINE_SITEMAP_XML_URL", f"{BASE_URL}/sitemap.xml")
HTTP_TIMEOUT = float(os.getenv("COPYLINE_HTTP_TIMEOUT", os.getenv("HTTP_TIMEOUT", "30")) or "30")
REQUEST_DELAY_MS = int(os.getenv("COPYLINE_REQUEST_DELAY_MS", os.getenv("REQUEST_DELAY_MS", "60")) or "60")

UA = {
    "User-Agent": os.getenv(
        "COPYLINE_HTTP_UA",
        os.getenv(
            "HTTP_UA",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ),
    ),
    "Accept": "*/*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.5",
    "Connection": "keep-alive",
}


def safe_str(x: Any) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def title_clean(s: str) -> str:
    """Подчистить title карточки/ссылки."""
    s = (s or "").strip()
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    return re.sub(r"\s{2,}", " ", s).strip()[:240]


def parse_price_tenge(text: str) -> int:
    """Вытащить цену в тг из текста вроде '7 051 тг.'"""
    if not text:
        return 0
    s = str(text)
    m = re.search(r"(\d[\d\s]{1,15})\s*(?:тг|тенге|₸)", s, flags=re.I)
    if not m:
        return 0
    num = re.sub(r"\s+", "", m.group(1))
    try:
        return int(num)
    except Exception:
        return 0


def parse_price_digits(text: str) -> int:
    """Вытащить число из блока цены без привязки к 'тг'."""
    if not text:
        return 0
    s = str(text)
    m = re.search(r"(\d[\d\s]{0,15})(?:[\.,]\d{1,2})", s)
    if m:
        num = re.sub(r"\s+", "", m.group(1))
        try:
            return int(num)
        except Exception:
            return 0
    num = re.sub(r"[^0-9]+", "", s)
    if not num:
        return 0
    try:
        return int(num)
    except Exception:
        return 0


def _sleep_jitter(ms: int) -> None:
    """Небольшая пауза между HTTP-запросами."""
    d = max(0.0, ms / 1000.0)
    time.sleep(d * (1.0 + random.uniform(-0.15, 0.15)))


def http_get(url: str, tries: int = 3, min_bytes: int = 0) -> Optional[bytes]:
    """Скачать URL с простым retry."""
    delay = max(0.1, REQUEST_DELAY_MS / 1000.0)
    last_error: str = ""
    for _ in range(max(1, tries)):
        try:
            resp = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
            if resp.status_code == 200 and len(resp.content) >= min_bytes:
                return resp.content
            last_error = f"http {resp.status_code} size={len(resp.content)}"
        except Exception as exc:
            last_error = repr(exc)
        _sleep_jitter(int(delay * 1000))
        delay *= 1.6
    return None


def soup_of(data: bytes | str) -> BeautifulSoup:
    """Сделать BeautifulSoup из bytes/str."""
    if isinstance(data, bytes):
        return BeautifulSoup(data, "lxml")
    return BeautifulSoup(data or "", "lxml")


def extract_kv_pairs_from_text(text: str) -> List[tuple[str, str]]:
    """Мягкий парсер строк вида 'Ключ: значение'."""
    out: List[tuple[str, str]] = []
    for ln in (text or "").splitlines():
        ln = ln.strip().strip("•-–—")
        if not ln or ":" not in ln:
            continue
        key, value = ln.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key and value and len(key) <= 80 and len(value) <= 240:
            out.append((key, value))
    return out


def _dedupe_pairs(items: List[tuple[str, str]]) -> List[tuple[str, str]]:
    """Дедуп param-пары без потери их канала происхождения до merge-этапа."""
    out: List[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for key, value in items:
        k = safe_str(key)
        v = safe_str(value)
        if not k or not v:
            continue
        sig = (k.casefold(), v.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((k, v))
    return out


def _extract_table_pairs(table) -> List[tuple[str, str]]:
    """Вытащить сырые пары из HTML-таблицы без semantic-решений."""
    out: List[tuple[str, str]] = []
    if not table:
        return out
    for tr in table.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) < 2:
            continue
        key = safe_str(tds[0].get_text(" ", strip=True))
        value = safe_str(tds[1].get_text(" ", strip=True))
        if key and value and len(key) <= 80 and len(value) <= 240:
            out.append((key, value))
    return _dedupe_pairs(out)


def _abs_url(href: str) -> str:
    """Нормализовать URL относительно BASE_URL."""
    href = safe_str(href)
    if not href:
        return ""
    return urljoin(BASE_URL + "/", href)


def parse_sitemap_html_products(html_bytes: bytes) -> List[Dict[str, str]]:
    """Прочитать HTML-sitemap и вернуть все product links без фильтрации."""
    s = soup_of(html_bytes)
    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    for a in s.find_all("a"):
        href = safe_str(a.get("href"))
        title = title_clean(safe_str(a.get_text(" ", strip=True)))
        if not href or not title:
            continue
        url = _abs_url(href)
        if "/goods/" not in url or not re.search(r"\.html(?:\?|$)", url, flags=re.I):
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append({"url": url, "title": title})
    return out


def parse_sitemap_xml_products(xml_bytes: bytes) -> List[Dict[str, str]]:
    """Fallback: прочитать sitemap.xml и вернуть product links без фильтрации."""
    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return out

    for node in root.iter():
        tag = node.tag.rsplit("}", 1)[-1].lower()
        if tag != "loc":
            continue
        url = safe_str(node.text)
        if not url or "/goods/" not in url or not re.search(r"\.html(?:\?|$)", url, flags=re.I):
            continue
        if url in seen:
            continue
        seen.add(url)
        title = title_clean(url.rsplit("/", 1)[-1].rsplit(".", 1)[0].replace("-", " "))
        out.append({"url": url, "title": title})
    return out


def fetch_product_index() -> List[Dict[str, str]]:
    """Скачать sitemap и вернуть список product URLs."""
    html_bytes = http_get(SITEMAP_URL, tries=3, min_bytes=20_000)
    if html_bytes:
        products = parse_sitemap_html_products(html_bytes)
        if products:
            return products

    xml_bytes = http_get(SITEMAP_XML_URL, tries=3, min_bytes=5_000)
    if not xml_bytes:
        raise RuntimeError("CopyLine: не удалось скачать sitemap (HTML) и sitemap.xml.")
    return parse_sitemap_xml_products(xml_bytes)


def _parse_price_from_page(s: BeautifulSoup) -> int:
    """Найти цену на карточке товара."""
    main = s.select_one(".productfull") or s

    price_selectors = [
        "#block_price",
        ".prod_price",
        '[itemprop="price"]',
    ]
    for sel in price_selectors:
        el = main.select_one(sel)
        if not el:
            continue
        text = safe_str(el.get_text(" ", strip=True))
        price = parse_price_tenge(text) or parse_price_digits(text)
        if price > 0:
            return price
        content = safe_str(el.get("content"))
        price = parse_price_digits(content)
        if price > 0:
            return price

    meta_price = (
        s.find("meta", attrs={"property": "product:price:amount"})
        or s.find("meta", attrs={"itemprop": "price"})
    )
    if meta_price:
        price = parse_price_digits(safe_str(meta_price.get("content")))
        if price > 0:
            return price

    text = safe_str(main.get_text(" ", strip=True))
    return parse_price_tenge(text) or parse_price_digits(text)


def _parse_available_from_page(s: BeautifulSoup) -> bool:
    """Определить наличие товара по тексту страницы."""
    txt = safe_str(s.get_text(" ", strip=True)).lower()
    if "нет в наличии" in txt or "отсутств" in txt:
        return False
    return True


def _extract_picture_candidates(s: BeautifulSoup) -> List[str]:
    """Вытащить все возможные URL картинок со страницы."""
    cand: List[str] = []

    a_full = s.select_one('a.lightbox[id^="main_image_full_"]')
    if a_full and a_full.get("href"):
        cand.append(safe_str(a_full["href"]))

    ogi = s.find("meta", attrs={"property": "og:image"})
    if ogi and ogi.get("content"):
        cand.append(safe_str(ogi["content"]))

    lnk = s.find("link", attrs={"rel": "image_src"})
    if lnk and lnk.get("href"):
        cand.append(safe_str(lnk["href"]))

    img_main = s.select_one('img[id^="main_image_"]') or s.find("img", attrs={"itemprop": "image"})
    if img_main:
        for attr in ("data-src", "data-original", "data-lazy", "src", "srcset"):
            val = safe_str(img_main.get(attr))
            if val:
                cand.append(val)
                break

    for img in s.find_all("img"):
        for attr in ("data-src", "data-original", "data-lazy", "src", "srcset"):
            val = safe_str(img.get(attr))
            if not val or "thumb_" in val:
                continue
            if any(k in val for k in ("img_products", "jshopping", "/products/", "/img/")) or re.search(
                r"\.(?:jpg|jpeg|png|webp)(?:\?|$)", val, flags=re.I
            ):
                cand.append(val)
                break

    for a in s.find_all("a"):
        href = safe_str(a.get("href"))
        if not href or "thumb_" in href:
            continue
        if ("img_products" in href) or re.search(r"\.(?:jpg|jpeg|png|webp)(?:\?|$)", href, flags=re.I):
            cand.append(href)

    out: List[str] = []
    seen: set[str] = set()
    for raw in cand:
        url = _abs_url(raw).replace("&amp;", "&")
        if not url or url.startswith("data:"):
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def parse_product_page(url: str) -> Optional[Dict[str, Any]]:
    """Распарсить карточку товара в сырой payload с сохранением provenance."""
    data = http_get(url, tries=3)
    if not data:
        return None

    s = soup_of(data)

    sku = ""
    sku_el = s.find(attrs={"itemprop": "sku"})
    if sku_el:
        sku = safe_str(sku_el.get_text(" ", strip=True))
    if not sku:
        pc = s.find(id="product_code")
        if pc:
            sku = safe_str(pc.get_text(" ", strip=True))
    if not sku:
        txt = s.get_text(" ", strip=True)
        m = re.search(r"(?:Артикул|SKU|Код товара|Код)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})", txt, flags=re.I)
        if m:
            sku = m.group(1)
    if not sku:
        return None

    h = s.find(["h1", "h2"], attrs={"itemprop": "name"}) or s.find("h1") or s.find("h2")
    title = title_clean(safe_str(h.get_text(" ", strip=True) if h else ""))

    raw_desc = ""
    raw_desc_pairs: List[tuple[str, str]] = []
    block = (
        s.select_one('div[itemprop="description"].jshop_prod_description')
        or s.select_one("div.jshop_prod_description")
        or s.select_one('[itemprop="description"]')
    )
    if block:
        raw_desc = block.get_text("\n", strip=True)
        raw_desc_pairs = _dedupe_pairs(extract_kv_pairs_from_text(raw_desc))

    table = s.find("table")
    raw_table_params = _extract_table_pairs(table)

    # Backward-safe legacy merge: старые builder/extractor слои пока ещё ждут единый params.
    # Но теперь вместе с ним мы сохраняем отдельные каналы сырья.
    legacy_params = _dedupe_pairs([*raw_desc_pairs, *raw_table_params])

    pictures = _extract_picture_candidates(s)
    price_raw = _parse_price_from_page(s)
    available = _parse_available_from_page(s)

    return {
        "sku": sku,
        "url": url,
        "title": title,
        # Legacy keys
        "desc": raw_desc,
        "params": legacy_params,
        # Provenance-preserving keys
        "raw_desc": raw_desc,
        "raw_desc_pairs": raw_desc_pairs,
        "raw_table_params": raw_table_params,
        "pics": pictures,
        "pic": pictures[0] if pictures else "",
        "price_raw": price_raw,
        "available": available,
    }
