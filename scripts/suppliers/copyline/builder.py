# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/builder.py
CopyLine builder layer.

Что делает:
- собирает raw OfferOut из уже распарсенного page-payload;
- объединяет page params и desc params;
- делает только supplier-side cleanup/reconcile;
- не считает shared pricing внутри supplier-layer.

Важно:
- по правилу проекта для CopyLine available всегда должно быть true;
- это supplier-specific правило, поэтому оно живёт здесь, а не в shared core.
"""

from __future__ import annotations

import re
from typing import Sequence, Tuple

from cs.core import OfferOut
from suppliers.copyline.compat import reconcile_copyline_params
from suppliers.copyline.desc_clean import clean_description
from suppliers.copyline.desc_extract import extract_desc_params
from suppliers.copyline.normalize import normalize_source_basics
from suppliers.copyline.params_page import extract_page_params
from suppliers.copyline.pictures import full_only_if_present, prefer_full_product_pictures


BRAND_HINTS: tuple[tuple[str, str], ...] = (
    (r"\bKonica[- ]?Minolta\b", "Konica-Minolta"),
    (r"\bToshiba\b", "Toshiba"),
    (r"\bRicoh\b", "Ricoh"),
    (r"\bRICOH\b", "Ricoh"),
    (r"\bPanasonic\b", "Panasonic"),
    (r"\bКАТЮША\b", "КАТЮША"),
    (r"\bKATYUSHA\b", "КАТЮША"),
    (r"\bXerox\b", "Xerox"),
    (r"\bCanon\b", "Canon"),
    (r"\bSamsung\b", "Samsung"),
    (r"\bKyocera\b", "Kyocera"),
    (r"\bBrother\b", "Brother"),
    (r"\bEpson\b", "Epson"),
    (r"\bLexmark\b", "Lexmark"),
    (r"\bRISO\b", "RISO"),
    (r"\bHP\b", "HP"),
)

CODE_SCORE_PATTERNS: tuple[tuple[re.Pattern[str], int], ...] = (
    (re.compile(r"^(?:CF|CE|CB|CC|Q|W)\d", re.I), 100),
    (re.compile(r"^(?:106R|006R|108R|113R|013R)\d", re.I), 100),
    (re.compile(r"^016\d{6}$", re.I), 95),
    (re.compile(r"^(?:MLT-|CLT-|TK-|KX-FA|KX-FAT|C-?EXV|DR-|TN-|C13T|C12C|C33S|T-)", re.I), 95),
    (re.compile(r"^ML-D\d", re.I), 90),
    (re.compile(r"^ML-\d{4,5}[A-Z]\d?$", re.I), 85),
)


# ----------------------------- basic helpers -----------------------------

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
        for key, value in block or []:
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


def _is_numeric_model(value: str) -> bool:
    return bool(re.fullmatch(r"\d+", safe_str(value)))


def _is_allowed_numeric_code(value: str) -> bool:
    return bool(re.fullmatch(r"016\d{6}", safe_str(value)))


def _code_score(code: str) -> int:
    token = safe_str(code)
    for rx, score in CODE_SCORE_PATTERNS:
        if rx.search(token):
            return score
    if _is_allowed_numeric_code(token):
        return 95
    return 10


def _first_code_from_params(params: Sequence[Tuple[str, str]]) -> str:
    best_code = ""
    best_score = -1
    for key, value in params:
        if safe_str(key) != "Коды расходников":
            continue
        parts = [x.strip() for x in re.split(r"\s*,\s*", safe_str(value)) if x.strip()]
        for part in parts:
            score = _code_score(part)
            if score > best_score:
                best_score = score
                best_code = part
    return best_code


def _infer_vendor_from_text(text: str) -> str:
    hay = safe_str(text)
    if not hay:
        return ""
    for pattern, vendor in BRAND_HINTS:
        if re.search(pattern, hay, flags=re.I):
            return vendor
    return ""


def _infer_vendor_from_compat(params: Sequence[Tuple[str, str]]) -> str:
    compat = ""
    for key, value in params:
        if safe_str(key) == "Совместимость":
            compat = safe_str(value)
            break
    return _infer_vendor_from_text(compat)


def _drop_weak_params(params: Sequence[Tuple[str, str]]) -> list[Tuple[str, str]]:
    bad_values = {"-", "—", "нет", "n/a", "null"}
    out: list[Tuple[str, str]] = []
    for key, value in params:
        k = safe_str(key)
        v = safe_str(value)
        if not k or not v:
            continue
        if v.casefold() in bad_values:
            continue
        out.append((k, v))
    return out


def _has_consumable_type(params: Sequence[Tuple[str, str]]) -> bool:
    consumable_types = {"Картридж", "Тонер-картридж", "Драм-картридж", "Девелопер", "Чернила"}
    return any(safe_str(key) == "Тип" and safe_str(value) in consumable_types for key, value in params)


# ----------------------------- resolve helpers -----------------------------

def _resolve_page_basics(page: dict, *, fallback_title: str) -> tuple[str, str, str, str, str, list[tuple[str, str]]]:
    sku = safe_str(page.get("sku"))
    source_title = safe_str(page.get("title") or fallback_title)
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
    return sku, title, vendor, model, cleaned_desc, page_params_raw


def _repair_model_param(params: Sequence[Tuple[str, str]], model: str) -> list[Tuple[str, str]]:
    merged = _merge_params(params, [("Модель", model)]) if model else list(params)

    current_model = ""
    for key, value in merged:
        if safe_str(key) == "Модель":
            current_model = safe_str(value)
            break

    if _is_numeric_model(current_model) and not _is_allowed_numeric_code(current_model):
        first_code = _first_code_from_params(merged)
        out: list[Tuple[str, str]] = []
        for key, value in merged:
            if safe_str(key) != "Модель":
                out.append((key, value))
        if first_code:
            out.append(("Модель", first_code))
        return out

    if not current_model:
        first_code = _first_code_from_params(merged)
        if first_code:
            return _merge_params(merged, [("Модель", first_code)])

    return merged


def _resolve_vendor(title: str, vendor: str, params: Sequence[Tuple[str, str]]) -> str:
    resolved = safe_str(vendor)
    if not resolved:
        resolved = _infer_vendor_from_compat(params)
    if not resolved:
        resolved = _infer_vendor_from_text(title)
    return resolved


def _finalize_params(params: Sequence[Tuple[str, str]], vendor: str) -> list[Tuple[str, str]]:
    merged = list(params)
    if vendor and _has_consumable_type(merged):
        merged = _merge_params(merged, [("Для бренда", vendor)])
    merged = _drop_weak_params(merged)
    merged = reconcile_copyline_params(merged)
    return merged


def _build_pictures(page: dict) -> list[str]:
    pictures = prefer_full_product_pictures(page.get("pics") or [])
    return full_only_if_present(pictures)


def _resolve_available(_: dict) -> bool:
    # ВАЖНО: по текущему правилу проекта CopyLine всегда должен выходить available=true.
    # Это supplier-policy, поэтому фиксируем здесь, а не в shared core.
    return True


# ----------------------------- main builder -----------------------------

def build_offer_from_page(page: dict, *, fallback_title: str = "") -> OfferOut | None:
    sku, title, vendor, model, cleaned_desc, page_params_raw = _resolve_page_basics(
        page,
        fallback_title=fallback_title,
    )
    if not sku or not title:
        return None

    page_params = extract_page_params(title=title, description=cleaned_desc, page_params=page_params_raw)
    desc_params = extract_desc_params(title=title, description=cleaned_desc, existing_params=page_params)

    params = _merge_params(page_params, desc_params)
    params = _repair_model_param(params, model)
    vendor = _resolve_vendor(title, vendor, params)
    params = _finalize_params(params, vendor)

    pictures = _build_pictures(page)
    raw_price = int(page.get("price_raw") or 0)
    available = _resolve_available(page)

    return OfferOut(
        oid=_mk_oid(sku),
        available=available,
        name=title,
        price=raw_price,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=(cleaned_desc or title),
    )
