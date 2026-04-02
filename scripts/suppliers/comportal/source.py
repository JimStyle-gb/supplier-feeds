# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/source.py
ComPortal source layer.

Роль:
- читать source YML поставщика;
- вернуть raw categories + raw offers;
- ничего не нормализовать и не фильтровать.

Поддержка источника:
- COMPORTAL_SOURCE_FILE
- COMPORTAL_SOURCE_URL
- COMPORTAL_LOGIN / COMPORTAL_PASSWORD (Basic Auth)
- COMPORTAL_HTTP_COOKIE
- COMPORTAL_HTTP_AUTHORIZATION
"""

from __future__ import annotations

import base64
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List
from urllib.request import Request, urlopen


DEFAULT_SOURCE_URL = "https://www.comportal.kz/auth/documents/prices/yml-catalog.php"
DEFAULT_TIMEOUT = 60.0


def safe_str(x: Any) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def env(name: str, default: str = "") -> str:
    """Удобный getenv с trim."""
    return os.getenv(name, default).strip()


def parse_price_int(text: str) -> int:
    """Вытащить int-цену из source price."""
    s = norm_spaces(text)
    if not s:
        return 0
    digits = re.sub(r"[^0-9]+", "", s)
    if not digits:
        return 0
    try:
        return int(digits)
    except Exception:
        return 0


def _basic_auth_header(login: str, password: str) -> str:
    """Собрать Basic Authorization."""
    token = f"{login}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(token).decode("ascii")


def _build_headers() -> Dict[str, str]:
    """HTTP headers для ComPortal."""
    headers = {
        "User-Agent": env(
            "COMPORTAL_HTTP_UA",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ),
        "Accept": "*/*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.5",
        "Connection": "close",
        "Referer": env("COMPORTAL_HTTP_REFERER", "https://www.comportal.kz/"),
    }

    cookie = env("COMPORTAL_HTTP_COOKIE")
    if cookie:
        headers["Cookie"] = cookie

    authorization = env("COMPORTAL_HTTP_AUTHORIZATION")
    if authorization:
        headers["Authorization"] = authorization
    else:
        login = env("COMPORTAL_LOGIN")
        password = env("COMPORTAL_PASSWORD")
        if login and password:
            headers["Authorization"] = _basic_auth_header(login, password)

    return headers


def _load_source_bytes() -> bytes:
    """Прочитать source YML: сначала локальный файл, потом URL."""
    source_file = env("COMPORTAL_SOURCE_FILE")
    if source_file:
        path = Path(source_file)
        data = path.read_bytes()
        if not data.strip():
            raise RuntimeError(f"ComPortal source file is empty: {source_file}")
        return data

    source_url = env("COMPORTAL_SOURCE_URL", DEFAULT_SOURCE_URL)
    timeout = float(env("COMPORTAL_HTTP_TIMEOUT", str(DEFAULT_TIMEOUT)) or str(DEFAULT_TIMEOUT))

    req = Request(source_url, headers=_build_headers())
    with urlopen(req, timeout=timeout) as resp:
        data = resp.read()

    if not data or not data.strip():
        raise RuntimeError(
            "ComPortal source returned empty body. "
            "Use COMPORTAL_LOGIN/COMPORTAL_PASSWORD or "
            "COMPORTAL_HTTP_COOKIE / COMPORTAL_HTTP_AUTHORIZATION, "
            "or set COMPORTAL_SOURCE_FILE."
        )
    return data


def _parse_xml(data: bytes) -> ET.Element:
    """Распарсить XML с понятной ошибкой."""
    raw = data.lstrip()

    if not raw:
        raise RuntimeError("ComPortal source body is empty after trim.")

    low = raw[:300].lower()
    if low.startswith(b"<html") or b"<!doctype html" in low:
        raise RuntimeError(
            "ComPortal source returned HTML instead of XML. "
            "Most likely auth failed or site returned a session page."
        )

    try:
        return ET.fromstring(raw)
    except ET.ParseError as exc:
        preview = raw[:300].decode("utf-8", errors="ignore")
        raise RuntimeError(
            "ComPortal XML parse failed. "
            f"Preview: {preview!r}. Error: {exc}"
        ) from exc


def build_category_index(root: ET.Element) -> Dict[str, Dict[str, str]]:
    """Построить индекс категорий с path."""
    idx: Dict[str, Dict[str, str]] = {}

    for cat in root.findall(".//categories/category"):
        cid = safe_str(cat.get("id"))
        if not cid:
            continue
        idx[cid] = {
            "id": cid,
            "name": norm_spaces(safe_str(cat.text)),
            "parent_id": safe_str(cat.get("parentId")),
        }

    def make_path(cid: str) -> str:
        seen: set[str] = set()
        names: List[str] = []
        cur = cid
        while cur and cur not in seen and cur in idx:
            seen.add(cur)
            rec = idx[cur]
            if rec.get("name"):
                names.append(rec["name"])
            cur = rec.get("parent_id", "")
        names.reverse()
        return " > ".join([x for x in names if x])

    for cid, rec in idx.items():
        rec["path"] = make_path(cid)

    return idx


def _collect_pictures(offer_el: ET.Element) -> List[str]:
    """Собрать picture[] без дублей."""
    out: List[str] = []
    seen: set[str] = set()

    for pic_el in offer_el.findall("./picture"):
        pic = norm_spaces(safe_str(pic_el.text))
        if not pic or pic in seen:
            continue
        seen.add(pic)
        out.append(pic)

    return out


def _collect_params(offer_el: ET.Element) -> List[Dict[str, str]]:
    """Собрать raw param[] как есть."""
    out: List[Dict[str, str]] = []

    for p in offer_el.findall("./param"):
        name = norm_spaces(safe_str(p.get("name")))
        value = norm_spaces(safe_str(p.text))
        if not name or not value:
            continue
        out.append({"name": name, "value": value})

    return out


def parse_offer(offer_el: ET.Element, category_index: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    """Превратить offer XML в raw offer payload без semantic-логики."""
    raw_id = norm_spaces(safe_str(offer_el.get("id")))
    raw_available = safe_str(offer_el.get("available")).lower() == "true"

    raw_url = norm_spaces(safe_str(offer_el.findtext("./url")))
    raw_name = norm_spaces(safe_str(offer_el.findtext("./name")))
    raw_vendor = norm_spaces(safe_str(offer_el.findtext("./vendor")))
    raw_vendor_code = norm_spaces(safe_str(offer_el.findtext("./vendorCode")))
    raw_category_id = norm_spaces(safe_str(offer_el.findtext("./categoryId")))
    raw_price_text = norm_spaces(safe_str(offer_el.findtext("./price")))
    raw_currency = norm_spaces(safe_str(offer_el.findtext("./currencyId")))
    raw_delivery = norm_spaces(safe_str(offer_el.findtext("./delivery")))
    raw_active = norm_spaces(safe_str(offer_el.findtext("./active")))

    pics = _collect_pictures(offer_el)
    params = _collect_params(offer_el)

    category_name = ""
    category_path = ""
    if raw_category_id and raw_category_id in category_index:
        category_name = category_index[raw_category_id].get("name", "")
        category_path = category_index[raw_category_id].get("path", "")

    return {
        # raw contract
        "id": raw_id,
        "sku": raw_vendor_code or raw_id,
        "url": raw_url,
        "title": raw_name,
        "name": raw_name,
        "vendor": raw_vendor,
        "vendorCode": raw_vendor_code,
        "categoryId": raw_category_id,
        "currencyId": raw_currency or "KZT",
        "available": raw_available,
        "price_raw": str(parse_price_int(raw_price_text)),
        "pic": pics[0] if pics else "",
        "pics": pics,
        "params": params,
        "desc": "",

        # provenance
        "raw_id": raw_id,
        "raw_name": raw_name,
        "raw_vendor": raw_vendor,
        "raw_vendorCode": raw_vendor_code,
        "raw_categoryId": raw_category_id,
        "raw_category_name": category_name,
        "raw_category_path": category_path,
        "raw_price_text": raw_price_text,
        "raw_currencyId": raw_currency or "KZT",
        "raw_delivery": raw_delivery,
        "raw_active": raw_active,
        "raw_picture": pics[0] if pics else "",
        "raw_pictures": pics,
        "raw_params": params,
        "raw_url": raw_url,
    }


def fetch_catalog_payload() -> Dict[str, Any]:
    """Прочитать весь каталог и вернуть raw categories + raw offers."""
    data = _load_source_bytes()
    root = _parse_xml(data)
    category_index = build_category_index(root)

    offers: List[Dict[str, Any]] = []
    for offer_el in root.findall(".//offers/offer"):
        offers.append(parse_offer(offer_el, category_index))

    source_file = env("COMPORTAL_SOURCE_FILE")
    source_url = source_file or env("COMPORTAL_SOURCE_URL", DEFAULT_SOURCE_URL)

    return {
        "source_url": source_url,
        "category_index": category_index,
        "offers": offers,
    }


def fetch_products() -> List[Dict[str, Any]]:
    """Backward-safe helper: вернуть только offers."""
    return fetch_catalog_payload()["offers"]


def fetch_categories() -> Dict[str, Dict[str, str]]:
    """Backward-safe helper: вернуть только categories."""
    return fetch_catalog_payload()["category_index"]


__all__ = [
    "fetch_catalog_payload",
    "fetch_products",
    "fetch_categories",
    "build_category_index",
    "parse_offer",
]
