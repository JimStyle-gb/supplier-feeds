# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/params_page.py
"""

from __future__ import annotations

import html as ihtml
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .normalize import canon_vendor, norm_ws
from .pictures import clean_picture_urls

TAG_RE = re.compile(r"<[^>]+>")
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.I | re.S)
META_DESC_RE = re.compile(r"""<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["']""", re.I)
SKU_RE = re.compile(r"""let\s+sku\s*=\s*["']([^"']+)["']""", re.I)
PRICE_RUB_RE = re.compile(r"""let\s+priceRUB\s*=\s*([0-9]+(?:\.[0-9]+)?)""", re.I)
PRICE_MAIN_RE = re.compile(r"""price_main[^>]*>\s*<b>([^<]+)</b>""", re.I | re.S)
IMAGE_RE = re.compile(
    r"""(?:src|href|data-src|data-original|srcset)=["']([^"']+\.(?:jpg|jpeg|png|webp|gif)(?:\?[^"']*)?)["']""",
    re.I,
)
DESC_BLOCK_RE = re.compile(
    r"""<div[^>]+class=["'][^"']*(?:description|catalog_item_descr)[^"']*["'][^>]*>(.*?)</div>""",
    re.I | re.S,
)
DT_DD_RE = re.compile(r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>", re.I | re.S)
TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.I | re.S)
CELL_RE = re.compile(r"<(?:th|td)[^>]*>(.*?)</(?:th|td)>", re.I | re.S)
CODE_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9\-./]{2,}\b")


def html_text_fast(fragment: str) -> str:
    if not fragment:
        return ""
    text = TAG_RE.sub(" ", fragment)
    text = ihtml.unescape(text)
    return norm_ws(text)


def safe_int_from_text(text: str) -> int:
    s = norm_ws(text).replace(" ", "").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return 0
    try:
        return int(round(float(m.group(1))))
    except Exception:
        return 0


def extract_title(html: str) -> str:
    m = H1_RE.search(html)
    if m:
        return html_text_fast(m.group(1))
    m = TITLE_RE.search(html)
    return html_text_fast(m.group(1)) if m else ""


def extract_meta_desc(html: str) -> str:
    m = META_DESC_RE.search(html)
    return norm_ws(ihtml.unescape(m.group(1))) if m else ""


def extract_price_rub(html: str) -> int:
    m = PRICE_RUB_RE.search(html)
    if m:
        try:
            return int(round(float(m.group(1))))
        except Exception:
            pass
    m = PRICE_MAIN_RE.search(html)
    return safe_int_from_text(m.group(1)) if m else 0


def extract_sku(html: str) -> str:
    m = SKU_RE.search(html)
    return norm_ws(m.group(1)) if m else ""


def extract_images_from_html(page_url: str, html: str) -> list[str]:
    urls: list[str] = []
    for raw in IMAGE_RE.findall(html or ""):
        urls.append(urljoin(page_url, raw.strip()))
    return clean_picture_urls(urls)


def extract_params_and_desc_fast(html: str) -> tuple[list[tuple[str, str]], str]:
    params: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for key_html, val_html in DT_DD_RE.findall(html or ""):
        key = html_text_fast(key_html).strip(":")
        val = html_text_fast(val_html)
        if key and val and (key, val) not in seen:
            seen.add((key, val))
            params.append((key, val))

    if not params:
        for tr_html in TR_RE.findall(html or ""):
            cells = CELL_RE.findall(tr_html)
            if len(cells) < 2:
                continue
            key = html_text_fast(cells[0]).strip(":")
            val = html_text_fast(cells[1])
            if key and val and (key, val) not in seen:
                seen.add((key, val))
                params.append((key, val))

    desc = ""
    m = DESC_BLOCK_RE.search(html or "")
    if m:
        desc = html_text_fast(m.group(1))
    return params, desc


def extract_params_and_desc(html: str) -> tuple[list[tuple[str, str]], str]:
    params, desc = extract_params_and_desc_fast(html)
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
                key = norm_ws(dt.get_text(" ", strip=True)).strip(":")
                val = norm_ws(dd.get_text(" ", strip=True))
                if key and val and (key, val) not in seen:
                    seen.add((key, val))
                    params.append((key, val))

    if not params:
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                key = norm_ws(cells[0].get_text(" ", strip=True)).strip(":")
                val = norm_ws(cells[1].get_text(" ", strip=True))
                if key and val and (key, val) not in seen:
                    seen.add((key, val))
                    params.append((key, val))

    if not desc:
        m = DESC_BLOCK_RE.search(html or "")
        if m:
            desc = html_text_fast(m.group(1))
    return params, desc


def extract_title_codes(title: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for code in CODE_TOKEN_RE.findall(title or ""):
        code = code.strip(".-/")
        if len(code) < 3 or not re.search(r"\d", code):
            continue
        if code not in seen:
            seen.add(code)
            out.append(code)
    return out


def extract_vendor_from_title(title: str) -> str:
    upper = f" {title.upper()} "
    for vendor in (
        "HP", "CANON", "XEROX", "BROTHER", "KYOCERA", "SAMSUNG", "EPSON", "RICOH",
        "KONICA MINOLTA", "PANTUM", "LEXMARK", "OKI", "SHARP", "PANASONIC",
        "TOSHIBA", "DEVELOP", "GESTETNER", "RISO",
    ):
        if f" {vendor} " in upper:
            return canon_vendor(vendor.title() if vendor != "HP" else vendor)
    return ""
