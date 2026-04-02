# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/diagnostics.py
ComPortal diagnostics helpers.

Роль:
- собрать простую supplier-диагностику до/после фильтра;
- посчитать базовые показатели для build summary и отладки.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


def safe_str(x: Any) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def summarize_source_offers(offers: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Базовая статистика по сырым офферам."""
    items = list(offers)
    total = len(items)
    available_true = sum(1 for x in items if bool(x.get("available", False)))
    available_false = total - available_true

    with_picture = sum(1 for x in items if safe_str(x.get("raw_picture") or x.get("pic")))
    without_picture = total - with_picture

    with_vendor = sum(1 for x in items if safe_str(x.get("raw_vendor") or x.get("vendor")))
    without_vendor = total - with_vendor

    with_vendor_code = sum(1 for x in items if safe_str(x.get("raw_vendorCode") or x.get("vendorCode")))
    without_vendor_code = total - with_vendor_code

    with_category = sum(1 for x in items if safe_str(x.get("raw_categoryId") or x.get("categoryId")))
    without_category = total - with_category

    return {
        "total": total,
        "available_true": available_true,
        "available_false": available_false,
        "with_picture": with_picture,
        "without_picture": without_picture,
        "with_vendor": with_vendor,
        "without_vendor": without_vendor,
        "with_vendor_code": with_vendor_code,
        "without_vendor_code": without_vendor_code,
        "with_category": with_category,
        "without_category": without_category,
    }


def summarize_built_offers(offers: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Базовая статистика по built raw offers."""
    items = list(offers)
    total = len(items)
    available_true = sum(1 for x in items if bool(x.get("available", False)))
    available_false = total - available_true

    with_picture = sum(1 for x in items if safe_str(x.get("picture")))
    without_picture = total - with_picture

    with_vendor = sum(1 for x in items if safe_str(x.get("vendor")))
    without_vendor = total - with_vendor

    with_vendor_code = sum(1 for x in items if safe_str(x.get("vendorCode")))
    without_vendor_code = total - with_vendor_code

    with_native_desc = sum(1 for x in items if safe_str(x.get("native_desc")))
    without_native_desc = total - with_native_desc

    return {
        "total": total,
        "available_true": available_true,
        "available_false": available_false,
        "with_picture": with_picture,
        "without_picture": without_picture,
        "with_vendor": with_vendor,
        "without_vendor": without_vendor,
        "with_vendor_code": with_vendor_code,
        "without_vendor_code": without_vendor_code,
        "with_native_desc": with_native_desc,
        "without_native_desc": without_native_desc,
    }


def top_source_categories(
    offers: Iterable[Dict[str, Any]],
    *,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Топ категорий по количеству офферов."""
    counts: Dict[str, Dict[str, Any]] = {}

    for x in offers:
        cid = safe_str(x.get("raw_categoryId") or x.get("categoryId"))
        name = safe_str(x.get("raw_category_name"))
        path = safe_str(x.get("raw_category_path"))
        if not cid:
            cid = "__empty__"

        rec = counts.setdefault(
            cid,
            {
                "id": cid,
                "name": name,
                "path": path,
                "count": 0,
            },
        )
        rec["count"] += 1

    rows = sorted(counts.values(), key=lambda r: (-int(r["count"]), str(r["id"])))
    return rows[:limit]


__all__ = [
    "summarize_source_offers",
    "summarize_built_offers",
    "top_source_categories",
]
