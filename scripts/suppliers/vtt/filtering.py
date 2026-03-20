# -*- coding: utf-8 -*-
"""Path: scripts/suppliers/vtt/filtering.py"""

from __future__ import annotations

from dataclasses import dataclass


DEFAULT_ALLOWED_PREFIXES: list[str] = [
    "Drum",
    "Девелопер",
    "Драм-картридж",
    "Драм-юниты",
    "Кабель сетевой",
    "Картридж",
    "Картриджи",
    "Термоблок",
    "Тонер-картридж",
    "Чернила",
]

DEFAULT_ALLOWED_CATEGORY_CODES: list[str] = [
    "CARTINJ_COMPAT",
    "CARTINJ_ORIG",
    "CARTINJ_PRNTHD",
    "CARTLAS_COMPAT",
    "CARTLAS_COPY",
    "CARTLAS_ORIG",
    "CARTLAS_PRINT",
    "CARTLAS_TNR",
    "CARTMAT_CART",
    "DEV_DEV",
    "DRM_CRT",
    "DRM_UNIT",
    "PARTSPRINT_THERBLC",
    "PARTSPRINT_THERELT",
]


@dataclass(slots=True)
class PrefixFilterResult:
    allowed: bool
    matched_prefix: str | None


def title_passes_prefix_filter(
    title: str,
    allowed_prefixes: list[str] | None = None,
) -> PrefixFilterResult:
    allowed_prefixes = allowed_prefixes or DEFAULT_ALLOWED_PREFIXES
    name = (title or "").strip()
    for prefix in allowed_prefixes:
        if name.startswith(prefix):
            return PrefixFilterResult(True, prefix)
    return PrefixFilterResult(False, None)


def category_allowed(
    category_code: str | None,
    allowed_codes: list[str] | None = None,
) -> bool:
    allowed_codes = allowed_codes or DEFAULT_ALLOWED_CATEGORY_CODES
    return (category_code or "").strip().upper() in {x.upper() for x in allowed_codes}


def build_filter_summary(items: list[dict[str, object]]) -> dict[str, object]:
    total = len(items)
    prefix_ok = sum(1 for x in items if x.get("passes_prefix"))
    cat_ok = sum(1 for x in items if x.get("passes_category"))
    both_ok = sum(1 for x in items if x.get("passes_prefix") and x.get("passes_category"))
    return {
        "total_items": total,
        "prefix_ok": prefix_ok,
        "category_ok": cat_ok,
        "both_ok": both_ok,
        "allowed_prefixes": DEFAULT_ALLOWED_PREFIXES,
        "allowed_category_codes": DEFAULT_ALLOWED_CATEGORY_CODES,
    }
