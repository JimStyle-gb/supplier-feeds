# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/builder.py
CopyLine builder layer.

Задача:
- собрать уже чистый raw OfferOut из supplier-layer модулей;
- не оставлять core дожимать supplier-specific смысл.
"""

from __future__ import annotations

import re
from typing import Sequence, Tuple

from cs.core import OfferOut, compute_price
from suppliers.copyline.compat import reconcile_copyline_params
from suppliers.copyline.desc_clean import clean_description
from suppliers.copyline.desc_extract import extract_desc_params
from suppliers.copyline.normalize import normalize_source_basics
from suppliers.copyline.params_page import extract_page_params
from suppliers.copyline.pictures import full_only_if_present, prefer_full_product_pictures


_CODE_FALLBACK_RX = re.compile(
    r"\b(?:CF\d{3,4}[A-Z]|CE\d{3,4}[A-Z]|CB\d{3,4}[A-Z]|Q\d{4}[A-Z]|W\d{4}[A-Z0-9]{1,4}|"
    r"113R\d{5}|108R\d{5}|106R\d{5}|006R\d{5}|"
    r"TK-?\d{3,5}[A-Z0-9]*|MLT-[A-Z]\d{3,5}[A-Z0-9/]*|CLT-[A-Z]\d{3,5}[A-Z]?|"
    r"KX-FA\d+[A-Z]?|KX-FAT\d+[A-Z]?|"
    r"C13T\d{5,8}[A-Z0-9]*|C12C\d{5,8}[A-Z0-9]*|C33S\d{5,8}[A-Z0-9]*|"
    r"C-?EXV\d+[A-Z]*|DR-\d+[A-Z0-9-]*|TN-\d+[A-Z0-9-]*)\b",
    re.I,
)

_VENDOR_FROM_COMPAT = (
    "HP",
    "Canon",
    "Xerox",
    "Samsung",
    "Panasonic",
    "Kyocera",
    "Brother",
    "Epson",
    "Ricoh",
    "RISO",
)

_WEAK_VALUES = {"-", "—", "нет", "n/a", "null"}



def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""



def _mk_oid(sku: str) -> str:
    sku = safe_str(sku)
    sku = re.sub(r"[^A-Za-z0-9\-\._/]", "", sku)
    return "CL" + sku



def _merge_params(*blocks: Sequence[Tuple[str, str]]) -> list[Tuple[str, str]]:
    out: list[Tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for block in blocks:
        for k, v in block or []:
            key = safe_str(k)
            val = safe_str(v)
            if not key or not val:
                continue
            sig = (key.casefold(), val.casefold())
            if sig in seen:
                continue
            seen.add(sig)
            out.append((key, val))
    return out



def _is_numeric_model(value: str) -> bool:
    return bool(re.fullmatch(r"\d+", safe_str(value)))



def _first_code_from_params(params: Sequence[Tuple[str, str]]) -> str:
    for key, value in params:
        if safe_str(key) != "Коды расходников":
            continue
        m = _CODE_FALLBACK_RX.search(safe_str(value).upper())
        if m:
            return m.group(0).upper().replace(" ", "")
    return ""



def _infer_vendor_from_compat(params: Sequence[Tuple[str, str]]) -> str:
    for key, value in params:
        if safe_str(key) != "Совместимость":
            continue
        compat = safe_str(value)
        for vendor in _VENDOR_FROM_COMPAT:
            if re.search(rf"\b{re.escape(vendor)}\b", compat, flags=re.I):
                return vendor
    return ""



def _drop_weak_params(params: Sequence[Tuple[str, str]]) -> list[Tuple[str, str]]:
    out: list[Tuple[str, str]] = []
    for key, value in params:
        v = safe_str(value)
        if not v or v.casefold() in {x.casefold() for x in _WEAK_VALUES}:
            continue
        out.append((safe_str(key), v))
    return out



def build_offer_from_page(page: dict, *, fallback_title: str = "") -> OfferOut | None:
    sku = safe_str(page.get("sku"))
    if not sku:
        return None

    source_title = safe_str(page.get("title") or fallback_title)
    if not source_title:
        return None

    page_desc = safe_str(page.get("desc"))
    page_params_raw = list(page.get("params") or [])

    basics = normalize_source_basics(
        title=source_title,
        sku=sku,
        description_text=page_desc,
        params=page_params_raw,
    )
    title = safe_str(basics.get("title") or source_title)
    vendor = safe_str(basics.get("vendor"))
    model = safe_str(basics.get("model"))

    cleaned_desc = clean_description(safe_str(basics.get("description") or page_desc))
    page_params = extract_page_params(title=title, description=cleaned_desc, page_params=page_params_raw)
    desc_params = extract_desc_params(title=title, description=cleaned_desc, existing_params=page_params)

    params = _merge_params(page_params, desc_params)
    if model:
        params = _merge_params(params, [("Модель", model)])

    if any(safe_str(k) == "Модель" and _is_numeric_model(v) for k, v in params):
        code_model = _first_code_from_params(params)
        new_params: list[Tuple[str, str]] = []
        for key, value in params:
            if safe_str(key) != "Модель":
                new_params.append((key, value))
                continue
            if not _is_numeric_model(value):
                new_params.append((key, value))
                continue
            if code_model:
                new_params.append(("Модель", code_model))
        params = new_params

    if not vendor:
        vendor = _infer_vendor_from_compat(params)

    if vendor and any(safe_str(k) == "Тип" and safe_str(v) in {"Картридж", "Тонер-картридж", "Драм-картридж", "Девелопер", "Чернила"} for k, v in params):
        params = _merge_params(params, [("Для бренда", vendor)])

    params = _drop_weak_params(params)
    params = reconcile_copyline_params(params)

    pictures = prefer_full_product_pictures(page.get("pics") or [])
    pictures = full_only_if_present(pictures)

    raw_price = int(page.get("price_raw") or 0)
    price = compute_price(raw_price)
    available = bool(page.get("available", True))

    return OfferOut(
        oid=_mk_oid(sku),
        available=available,
        name=title,
        price=price,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=(cleaned_desc or title),
    )
