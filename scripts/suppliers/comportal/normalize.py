# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/normalize.py

Базовая supplier-нормализация полей ComPortal.
Роль такая же, как у готовых поставщиков:
- name / vendor / model / availability / price-in;
- без picture-policy;
- без большого extraction-комбайна;
- без builder/core-логики.
"""

from __future__ import annotations

import re

from cs.util import norm_ws, safe_int
from suppliers.comportal.models import ParamItem


_VENDOR_CANON_MAP = {
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

_NAME_VENDOR_PATTERNS: list[tuple[str, str]] = [
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

_GENERIC_VENDOR_WORDS = {
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


def _param_map(params: list[ParamItem]) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in params or []:
        name = norm_ws(p.name)
        value = norm_ws(p.value)
        if name and value:
            out[name] = value
    return out


def normalize_name(name: str) -> str:
    s = norm_ws(name)
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def build_offer_oid(raw_vendor_code: str, raw_id: str, *, prefix: str) -> str:
    base = norm_ws(raw_vendor_code) or norm_ws(raw_id)
    if not base:
        return ""
    base = re.sub(r"[^A-Za-z0-9]+", "", base)
    if not base:
        return ""
    if base.upper().startswith(prefix.upper()):
        return base
    return f"{prefix}{base}"


def normalize_available(available_attr: str, available_tag: str, active: str) -> bool:
    av_attr = (available_attr or "").strip().lower()
    if av_attr in ("true", "1", "yes"):
        return True
    if av_attr in ("false", "0", "no"):
        return False

    av_tag = (available_tag or "").strip().lower()
    if av_tag in ("true", "1", "yes"):
        return True
    if av_tag in ("false", "0", "no"):
        return False

    act = (active or "").strip().upper()
    if act == "Y":
        return True
    if act == "N":
        return False

    return False


def _canonical_vendor_token(vendor: str) -> str:
    s = norm_ws(vendor)
    if not s:
        return ""

    up = s.upper()
    if up in _GENERIC_VENDOR_WORDS:
        return ""

    if up in _VENDOR_CANON_MAP:
        return _VENDOR_CANON_MAP[up]

    if up.startswith("HP EUROPE"):
        return "HP"
    if up.startswith("HEWLETT PACKARD ENTERPRISE"):
        return "HPE"
    if up.startswith("HEWLETT PACKARD"):
        return "HP"
    if up.startswith("HP ENTERPRISE"):
        return "HPE"

    return s


def _infer_vendor_from_name(name: str) -> str:
    s = norm_ws(name)
    if not s:
        return ""
    for pattern, vendor in _NAME_VENDOR_PATTERNS:
        if re.search(pattern, s, flags=re.IGNORECASE):
            return vendor
    return ""


def normalize_vendor(
    vendor: str,
    *,
    name: str,
    params: list[ParamItem],
    vendor_blacklist: set[str],
    fallback_vendor: str = "",
) -> str:
    s = _canonical_vendor_token(vendor)
    if s and s.casefold() in vendor_blacklist:
        s = ""
    if s:
        return s

    pmap = _param_map(params)
    s = _canonical_vendor_token(pmap.get("Бренд", ""))
    if s and s.casefold() in vendor_blacklist:
        s = ""
    if s:
        return s

    s = _infer_vendor_from_name(name)
    if s and s.casefold() in vendor_blacklist:
        s = ""
    if s:
        return s

    return norm_ws(fallback_vendor)


def normalize_model(name: str, params: list[ParamItem]) -> str:
    pmap = _param_map(params)

    for key in ("Модель", "Партномер", "Артикул", "Номер"):
        val = norm_ws(pmap.get(key, ""))
        if val:
            return val

    s = normalize_name(name)
    m = re.search(r"\(([A-Za-z0-9#/\-\.]+)\)\s*$", s)
    if m:
        return norm_ws(m.group(1))

    m = re.search(r"\b([A-Z]{1,6}[A-Z0-9/\-]{2,})\b", s)
    if m:
        return norm_ws(m.group(1))

    return ""


def normalize_price_in(price_text: str) -> int | None:
    return safe_int(price_text)
