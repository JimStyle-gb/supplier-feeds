# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/filtering.py

VTT temporary filtering.
v2:
- основной gate только по prefix в начале названия;
- категории не режут товары, а сохраняются только как диагностика;
- нормализует пробелы/тире перед startswith.
"""

from __future__ import annotations

import re
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


_WS_RE = re.compile(r"\s+")
_DASH_RE = re.compile(r"[\u2010\u2011\u2012\u2013\u2014\u2212]+")


@dataclass(slots=True)
class PrefixFilterResult:
    allowed: bool
    matched_prefix: str | None
    normalized_title: str


def normalize_title_for_prefix(text: str) -> str:
    value = (text or "").replace("\xa0", " ").strip()
    value = _DASH_RE.sub("-", value)
    value = _WS_RE.sub(" ", value)
    return value


def title_passes_prefix_filter(
    title: str,
    allowed_prefixes: list[str] | None = None,
) -> PrefixFilterResult:
    allowed_prefixes = allowed_prefixes or DEFAULT_ALLOWED_PREFIXES
    normalized = normalize_title_for_prefix(title)
    for prefix in allowed_prefixes:
        if normalized.startswith(normalize_title_for_prefix(prefix)):
            return PrefixFilterResult(True, prefix, normalized)
    return PrefixFilterResult(False, None, normalized)


def build_filter_summary(items: list[dict[str, object]]) -> dict[str, object]:
    total = len(items)
    prefix_ok = sum(1 for x in items if x.get("passes_prefix"))
    confident = sum(1 for x in items if x.get("product_confident"))
    with_price = sum(1 for x in items if x.get("price_candidates"))
    with_images = sum(1 for x in items if (x.get("images_count") or 0) > 0)

    categories_seen: dict[str, int] = {}
    for item in items:
        for code in (item.get("category_codes_found") or []):
            categories_seen[str(code)] = categories_seen.get(str(code), 0) + 1

    return {
        "total_items": total,
        "prefix_ok": prefix_ok,
        "product_confident": confident,
        "with_price_candidates": with_price,
        "with_images": with_images,
        "allowed_prefixes": DEFAULT_ALLOWED_PREFIXES,
        "categories_seen_top20": dict(sorted(categories_seen.items(), key=lambda kv: (-kv[1], kv[0]))[:20]),
        "mode": "prefix_first_category_diagnostics_only",
    }
