# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/source.py
ComPortal source layer.

Задача модуля:
- прочитать исходный YML поставщика (локальный файл или URL);
- сохранить сырой payload офферов без ранней semantic-свёртки;
- сохранить category provenance для category-first фильтра.

В этом модуле НЕТ:
- фильтра по ассортименту;
- CS-нормализации name/vendor/model;
- supplier-business логики;
- удаления сервисных param;
- выбора "какие param важнее".
"""

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen

DEFAULT_SOURCE_URL = "https://www.comportal.kz/auth/documents/prices/yml-catalog.php"
SOURCE_URL = os.getenv("COMPORTAL_SOURCE_URL", DEFAULT_SOURCE_URL)
SOURCE_FILE = os.getenv("COMPORTAL_SOURCE_FILE", "").strip()
HTTP_TIMEOUT = float(os.getenv("COMPORTAL_HTTP_TIMEOUT", os.getenv("HTTP_TIMEOUT", "60")) or "60")
HTTP_UA = os.getenv(
    "COMPORTAL_HTTP_UA",
    os.getenv(
        "HTTP_UA",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ),
)


def safe_str(x: Any) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def parse_price_int(text: str) -> int:
    """Вытащить целую цену из source-строки."""
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


def load_source_bytes() -> bytes:
    """Прочитать исходный YML: сначала локальный файл, иначе URL."""
    if SOURCE_FILE:
        with open(SOURCE_FILE, "rb") as fh:
            return fh.read()

    req = Request(
        SOURCE_URL,
        headers={
            "User-Agent": HTTP_UA,
            "Accept": "*/*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.5",
            "Connection": "close",
        },
    )
    with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return resp.read()


def parse_xml(data: bytes) -> ET.Element:
    """Распарсить XML."""
    return ET.fromstring(data)


def build_category_index(root: ET.Element) -> Dict[str, Dict[str, str]]:
    """Построить словарь категорий с parent-chain."""
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
    """Собрать все picture."""
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
    """Собрать raw param как есть."""
    out: List[Dict[str, str]] = []
    for p in offer_el.findall("./param"):
        name = norm_spaces(safe_str(p.get("name")))
        value = norm_spaces(safe_str(p.text))
        if not name or not value:
            continue
        out.append({"name": name, "value": value})
    return out


def parse_offer(
    offer_el: ET.Element,
    category_index: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    """Превратить offer XML в сырой page-payload без semantic-логики."""
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

    # Legacy/backward-safe поля + сырой provenance.
    payload: Dict[str, Any] = {
        # identity
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

        # raw provenance
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
    return payload


def fetch_catalog_payload() -> Dict[str, Any]:
    """Прочитать весь каталог и вернуть categories + raw offers."""
    data = load_source_bytes()
    root = parse_xml(data)
    category_index = build_category_index(root)

    offers: List[Dict[str, Any]] = []
    for offer_el in root.findall(".//offers/offer"):
        offers.append(parse_offer(offer_el, category_index))

    return {
        "source_url": SOURCE_FILE or SOURCE_URL,
        "category_index": category_index,
        "offers": offers,
    }


def fetch_products() -> List[Dict[str, Any]]:
    """Backward-safe helper: вернуть только список raw offers."""
    return fetch_catalog_payload()["offers"]


def fetch_categories() -> Dict[str, Dict[str, str]]:
    """Вернуть индекс категорий поставщика."""
    return fetch_catalog_payload()["category_index"]


__all__ = [
    "fetch_catalog_payload",
    "fetch_products",
    "fetch_categories",
    "build_category_index",
    "parse_offer",
]
