# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/normalize.py
ComPortal basic normalization layer.

Что исправлено:
- vendor fallback больше не цепляет generic-слова типа МФП/Принтер/Ноутбук;
- сначала ищется нормальный бренд:
  1) source vendor
  2) param "Бренд"
  3) узнаваемый бренд в name
- model/name остаются в зоне normalize, без business-логики builder.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


VENDOR_MAP = {
    "HP EUROPE": "HP",
    "HP INC.": "HP",
    "HEWLETT PACKARD": "HP",
    "HEWLETT-PACKARD": "HP",
    "HP": "HP",
    "HPE": "HPE",
    "HP ENTERPRISE": "HPE",
    "HEWLETT PACKARD ENTERPRISE": "HPE",
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
    "VIEWSONIC": "ViewSonic",
    "BENQ": "BenQ",
    "AOC": "AOC",
    "HUAWEI": "Huawei",
    "TP-LINK": "TP-Link",
    "TPLINK": "TP-Link",
    "D-LINK": "D-Link",
    "DLINK": "D-Link",
    "CISCO": "Cisco",
    "ZYXEL": "Zyxel",
}

# Упорядоченный список для поиска бренда внутри name.
NAME_BRAND_PATTERNS = [
    (r"\bHP\s+Europe\b", "HP"),
    (r"\bHPE\b", "HPE"),
    (r"\bHP\b", "HP"),
    (r"\bCanon\b", "Canon"),
    (r"\bEpson\b", "Epson"),
    (r"\bXerox\b", "Xerox"),
    (r"\bBrother\b", "Brother"),
    (r"\bKyocera\b", "Kyocera"),
    (r"\bPantum\b", "Pantum"),
    (r"\bRicoh\b", "Ricoh"),
    (r"\bAPC\b", "APC"),
    (r"\bDell\b", "Dell"),
    (r"\bLenovo\b", "Lenovo"),
    (r"\bASUS\b", "ASUS"),
    (r"\bAcer\b", "Acer"),
    (r"\bMSI\b", "MSI"),
    (r"\bLG\b", "LG"),
    (r"\bSamsung\b", "Samsung"),
    (r"\biiyama\b", "iiyama"),
    (r"\bGigabyte\b", "Gigabyte"),
    (r"\bHikvision\b", "Hikvision"),
    (r"\bViewSonic\b", "ViewSonic"),
    (r"\bBenQ\b", "BenQ"),
    (r"\bAOC\b", "AOC"),
    (r"\bHuawei\b", "Huawei"),
    (r"\bTP-?Link\b", "TP-Link"),
    (r"\bD-?Link\b", "D-Link"),
    (r"\bCisco\b", "Cisco"),
    (r"\bZyxel\b", "Zyxel"),
    (r"\bMicrosoft\b", "Microsoft"),
]

GENERIC_VENDOR_WORDS = {
    "МФП",
    "МФУ",
    "ПРИНТЕР",
    "НОУТБУК",
    "МОНИТОР",
    "ИБП",
    "СКАНЕР",
    "ПРОЕКТОР",
    "КАРТРИДЖ",
    "ТОНЕР",
    "БАТАРЕЯ",
    "АККУМУЛЯТОР",
    "СТАБИЛИЗАТОР",
    "МОНОБЛОК",
    "СЕРВЕР",
    "КОММУТАТОР",
    "МАРШРУТИЗАТОР",
    "ДИСПЛЕЙ",
    "ПЛОТТЕР",
    "РАБОЧАЯ",
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


def _canonical_vendor_token(token: str) -> str:
    """Канонизировать один vendor-token."""
    cand = norm_spaces(token)
    if not cand:
        return ""

    key = cand.upper()
    if key in GENERIC_VENDOR_WORDS:
        return ""

    if key in VENDOR_MAP:
        return VENDOR_MAP[key]

    if key.startswith("HP EUROPE"):
        return "HP"
    if key.startswith("HEWLETT PACKARD ENTERPRISE"):
        return "HPE"
    if key.startswith("HEWLETT PACKARD"):
        return "HP"
    if key.startswith("HP ENTERPRISE"):
        return "HPE"

    for prefix, target in (
        ("CANON", "Canon"),
        ("EPSON", "Epson"),
        ("XEROX", "Xerox"),
        ("BROTHER", "Brother"),
        ("KYOCERA", "Kyocera"),
        ("PANTUM", "Pantum"),
        ("RICOH", "Ricoh"),
        ("APC", "APC"),
        ("DELL", "Dell"),
        ("LENOVO", "Lenovo"),
        ("ASUS", "ASUS"),
        ("ACER", "Acer"),
        ("MSI", "MSI"),
        ("LG", "LG"),
        ("SAMSUNG", "Samsung"),
        ("IIYAMA", "iiyama"),
        ("GIGABYTE", "Gigabyte"),
        ("HIKVISION", "Hikvision"),
        ("MICROSOFT", "Microsoft"),
        ("VIEWSONIC", "ViewSonic"),
        ("BENQ", "BenQ"),
        ("AOC", "AOC"),
        ("HUAWEI", "Huawei"),
        ("TP-LINK", "TP-Link"),
        ("TPLINK", "TP-Link"),
        ("D-LINK", "D-Link"),
        ("DLINK", "D-Link"),
        ("CISCO", "Cisco"),
        ("ZYXEL", "Zyxel"),
    ):
        if key.startswith(prefix):
            return target

    return cand


def _extract_vendor_from_name(raw_name: str) -> str:
    """Найти бренд внутри name."""
    name = norm_spaces(raw_name)
    if not name:
        return ""

    for pattern, vendor in NAME_BRAND_PATTERNS:
        if re.search(pattern, name, flags=re.IGNORECASE):
            return vendor

    return ""


def canonical_vendor(raw_vendor: str, params_map: Optional[Dict[str, str]] = None, raw_name: str = "") -> str:
    """Канонизировать vendor."""
    params_map = params_map or {}

    # 1) source vendor
    vendor = _canonical_vendor_token(raw_vendor)
    if vendor:
        return vendor

    # 2) param "Бренд"
    vendor = _canonical_vendor_token(params_map.get("Бренд", ""))
    if vendor:
        return vendor

    # 3) бренд внутри name
    vendor = _extract_vendor_from_name(raw_name)
    if vendor:
        return vendor

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
    "NAME_BRAND_PATTERNS",
    "GENERIC_VENDOR_WORDS",
    "params_to_map",
    "clean_name",
    "canonical_vendor",
    "extract_model",
    "normalize_basics",
]
