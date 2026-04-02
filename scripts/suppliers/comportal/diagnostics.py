# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/diagnostics.py

Диагностика и watch-report для ComPortal.
Роль как у готовых поставщиков:
- сводки по source/raw;
- watch helpers;
- лёгкие operational-отчёты.
"""

from __future__ import annotations

from pathlib import Path

from cs.core import OfferOut
from suppliers.comportal.models import BuildStats, SourceOffer


def build_watch_source_map(
    source_offers: list[SourceOffer],
    *,
    prefix: str,
    watch_ids: set[str],
) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for src in source_offers:
        oid = src.vendor_code if src.vendor_code.upper().startswith(prefix.upper()) else f"{prefix}{src.vendor_code}" if src.vendor_code else ""
        if oid in watch_ids:
            out[oid] = {"categoryId": src.category_id, "name": src.name}
    return out


def make_watch_messages(
    *,
    watch_ids: set[str],
    watch_source: dict[str, dict[str, str]],
    watch_out: set[str],
) -> list[str]:
    msgs: list[str] = []
    for oid in sorted(watch_ids):
        src = watch_source.get(oid)
        if src and oid in watch_out:
            msgs.append(f"OK {oid}: in feed | {src.get('categoryId', '')} | {src.get('name', '')}")
        elif src and oid not in watch_out:
            msgs.append(f"MISS {oid}: filtered out | {src.get('categoryId', '')} | {src.get('name', '')}")
        else:
            msgs.append(f"MISS {oid}: not found in source")
    return msgs


def write_watch_report(path: str | Path, lines: list[str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(lines).strip() + ("\n" if lines else ""), encoding="utf-8")


def summarize_offer_outs(offers: list[OfferOut]) -> dict[str, int]:
    with_picture = 0
    without_picture = 0
    with_vendor = 0
    without_vendor = 0
    with_native_desc = 0
    without_native_desc = 0
    available_true = 0
    available_false = 0

    for off in offers:
        if off.pictures:
            with_picture += 1
        else:
            without_picture += 1

        if (off.vendor or "").strip():
            with_vendor += 1
        else:
            without_vendor += 1

        if (off.native_desc or "").strip():
            with_native_desc += 1
        else:
            without_native_desc += 1

        if bool(off.available):
            available_true += 1
        else:
            available_false += 1

    return {
        "total": len(offers),
        "with_picture": with_picture,
        "without_picture": without_picture,
        "with_vendor": with_vendor,
        "without_vendor": without_vendor,
        "with_native_desc": with_native_desc,
        "without_native_desc": without_native_desc,
        "available_true": available_true,
        "available_false": available_false,
    }


def summarize_source_offers(source_offers: list[SourceOffer]) -> dict[str, int]:
    with_picture = 0
    without_picture = 0
    with_vendor = 0
    without_vendor = 0

    for src in source_offers:
        if src.picture_urls:
            with_picture += 1
        else:
            without_picture += 1

        if (src.vendor or "").strip():
            with_vendor += 1
        else:
            without_vendor += 1

    return {
        "total": len(source_offers),
        "with_picture": with_picture,
        "without_picture": without_picture,
        "with_vendor": with_vendor,
        "without_vendor": without_vendor,
    }


def summarize_build_stats(stats: BuildStats) -> dict[str, int]:
    return {
        "before": int(stats.before),
        "after": int(stats.after),
        "filtered_out": int(stats.filtered_out),
        "missing_picture_count": int(stats.missing_picture_count),
        "placeholder_picture_count": int(stats.placeholder_picture_count),
        "empty_vendor_count": int(stats.empty_vendor_count),
    }
