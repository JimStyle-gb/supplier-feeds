# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/normalize.py
ComPortal basic normalization layer.

Задача модуля:
- нормализовать name;
- канонизировать vendor;
- поднять model;
- подготовить базовые поля для builder/extractor.

В модуле НЕТ:
- ассортиментного фильтра;
- удаления param-мусора;
- построения description;
- ценовой логики;
- выбора final param order.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


VENDOR_MAP = {
    "HP EUROPE": "HP",
    "HP INC.": "HP",
    "HEWLETT PACKARD": "HP",
    "HEWLETT-PACKARD": "HP",
    "HPE": "HPE",
    "HP ENTERPRISE": "HPE",
    "HP ENTERPRISE/HPE": "HPE",
    "CANON": "Canon",
    "EPSON": "Epson",
    "XEROX": "Xerox",
    "BROTHER": "Brother",
    "KYOCERA": "Kyocera",
    "PANTUM": "Pantum",
    "RICOH": "Ricoh",
    "APC": "APC",
    "DELL": "Dell",
    "LENOVO": "Lenovo",
    "ASUS": "ASUS",
    "ACER": "Acer",
    "MSI": "MSI",
    "LG": "LG",
    "SAMSUNG": "Samsung",
    "IIYAMA": "iiyama",
    "GIGABYTE": "Gigabyte",
    "HIKVISION": "Hikvision",
    "MICROSOFT": "Microsoft",
    "KASPERSKY": "Kaspersky",
    "DR.WEB": "Dr.Web",
    "DR. WEB": "Dr.Web",
}


def safe_str(x: Any) -> str:
    """Безопасно привести к строке."""
    return str(x).strip() if x is not None else ""


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def params_to_map(params: List[Dict[str, str]]) -> Dict[str, str]:
    """Собрать map param name -> value."""
    out: Dict[str, str] = {}
    for p in params or []:
        name = norm_spaces(safe_str(p.get("name")))
        value = norm_spaces(safe_str(p.get("value")))
        if not name or not value:
            continue
        out[name] = value
    return out


def clean_name(raw_name: str) -> str:
    """Почистить source name."""
    s = norm_spaces(raw_name)
    s = re.sub(r"^[\-\–\—•\s]+", "", s)
    s = re.sub(r"\s+\(([^()]*)\)\s*$", lambda m: f" ({m.group(1).strip()})", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def canonical_vendor(raw_vendor: str, params_map: Optional[Dict[str, str]] = None, raw_name: str = "") -> str:
    """Канонизировать vendor."""
    params_map = params_map or {}
    candidates = [
        norm_spaces(raw_vendor),
        norm_spaces(params_map.get("Бренд", "")),
    ]

    name_first = norm_spaces(raw_name).split(" ", 1)[0] if raw_name else ""
    if name_first:
        candidates.append(name_first)

    for cand in candidates:
        if not cand:
            continue
        key = cand.upper()
        if key in VENDOR_MAP:
            return VENDOR_MAP[key]
        if key.startswith("HP EUROPE"):
            return "HP"
        if key.startswith("HEWLETT PACKARD"):
            return "HP"
        if key.startswith("HP "):
            return "HP"
        if key.startswith("CANON"):
            return "Canon"
        if key.startswith("EPSON"):
            return "Epson"
        if key.startswith("XEROX"):
            return "Xerox"
        if key.startswith("BROTHER"):
            return "Brother"
        if key.startswith("KYOCERA"):
            return "Kyocera"
        if key.startswith("PANTUM"):
            return "Pantum"
        if key.startswith("RICOH"):
            return "Ricoh"
        if key.startswith("APC"):
            return "APC"
        if key.startswith("DELL"):
            return "Dell"
        if key.startswith("LENOVO"):
            return "Lenovo"
        if key.startswith("ASUS"):
            return "ASUS"
        if key.startswith("ACER"):
            return "Acer"
        if key.startswith("MSI"):
            return "MSI"
        if key.startswith("LG"):
            return "LG"
        if key.startswith("SAMSUNG"):
            return "Samsung"
        if key.startswith("IIYAMA"):
            return "iiyama"
        if key.startswith("GIGABYTE"):
            return "Gigabyte"
        if key.startswith("HIKVISION"):
            return "Hikvision"
        if key.startswith("MICROSOFT"):
            return "Microsoft"

        return cand

    return ""


def extract_model(raw_name: str, params_map: Optional[Dict[str, str]] = None) -> str:
    """Поднять модель из param или name."""
    params_map = params_map or {}

    for key in ("Модель", "Партномер", "Артикул", "Номер"):
        value = norm_spaces(params_map.get(key, ""))
        if value:
            return value

    name = norm_spaces(raw_name)

    m = re.search(r"\(([A-Za-z0-9\-\#\/\.]+)\)\s*$", name)
    if m:
        return m.group(1)

    m = re.search(r"\b([A-Z]{1,6}[A-Z0-9\-/]{2,})\b", name)
    if m:
        return m.group(1)

    return ""


def normalize_basics(raw_offer: Dict[str, Any]) -> Dict[str, Any]:
    """Вернуть базово нормализованный offer payload."""
    params = raw_offer.get("raw_params") or raw_offer.get("params") or []
    params_map = params_to_map(params)

    raw_name = norm_spaces(raw_offer.get("raw_name") or raw_offer.get("title") or raw_offer.get("name"))
    raw_vendor = norm_spaces(raw_offer.get("raw_vendor") or raw_offer.get("vendor"))
    raw_vendor_code = norm_spaces(raw_offer.get("raw_vendorCode") or raw_offer.get("vendorCode"))

    name = clean_name(raw_name)
    vendor = canonical_vendor(raw_vendor, params_map=params_map, raw_name=name)
    model = extract_model(name, params_map=params_map)

    payload = dict(raw_offer)
    payload.update(
        {
            "title": name,
            "name": name,
            "vendor": vendor,
            "vendorCode": raw_vendor_code,
            "model": model,
            "params_map": params_map,
        }
    )
    return payload


__all__ = [
    "VENDOR_MAP",
    "params_to_map",
    "clean_name",
    "canonical_vendor",
    "extract_model",
    "normalize_basics",
]
