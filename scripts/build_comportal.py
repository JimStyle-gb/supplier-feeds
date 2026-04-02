# -*- coding: utf-8 -*-
"""
Path: scripts/build_comportal.py
ComPortal adapter under CS-template.

Что делает:
- читает YML ComPortal;
- применяет category-first whitelist filter;
- собирает clean raw offers;
- пишет raw/final feed через shared core;
- запускает diagnostics;
- запускает quality gate;
- печатает build summary, включая critical_preview из QG.
"""

from __future__ import annotations

import os
from typing import Any

from cs.core import OfferOut, get_public_vendor, write_cs_feed, write_cs_feed_raw
from cs.meta import next_run_at_hour, now_almaty

from suppliers.comportal.builder import build_offers
from suppliers.comportal.diagnostics import (
    summarize_built_offers,
    summarize_source_offers,
    top_source_categories,
)
from suppliers.comportal.filtering import filter_offers
from suppliers.comportal.quality_gate import run_quality_gate
from suppliers.comportal.source import fetch_catalog_payload


BUILD_COMPORTAL_VERSION = "build_comportal_v8_qg_preview"

SUPPLIER_NAME_DEFAULT = "ComPortal"
SUPPLIER_URL_DEFAULT = os.getenv(
    "SUPPLIER_URL",
    "https://www.comportal.kz/auth/documents/prices/yml-catalog.php",
)
OUT_FILE_DEFAULT = os.getenv("OUT_FILE", "docs/comportal.yml")
RAW_OUT_FILE_DEFAULT = os.getenv("RAW_OUT_FILE", "docs/raw/comportal.yml")
OUTPUT_ENCODING_DEFAULT = (os.getenv("OUTPUT_ENCODING", "utf-8") or "utf-8").strip() or "utf-8"

COMPORTAL_NEXT_RUN_HOUR = int(os.getenv("COMPORTAL_NEXT_RUN_HOUR", "4") or "4")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_offer_out(raw_offer: dict[str, Any]) -> OfferOut:
    """Преобразовать supplier raw-offer в cs.core.OfferOut."""
    params = []
    for p in raw_offer.get("params") or []:
        name = str(p.get("name") or "").strip()
        value = str(p.get("value") or "").strip()
        if not name or not value:
            continue
        params.append((name, value))

    pictures = list(raw_offer.get("pictures") or [])
    if not pictures and raw_offer.get("picture"):
        pictures = [str(raw_offer.get("picture"))]

    return OfferOut(
        oid=str(raw_offer.get("id") or "").strip(),
        available=bool(raw_offer.get("available", True)),
        name=str(raw_offer.get("name") or "").strip(),
        price=_safe_int(raw_offer.get("price"), 0),
        pictures=[str(x).strip() for x in pictures if str(x).strip()],
        vendor=str(raw_offer.get("vendor") or "").strip(),
        params=params,
        native_desc=str(raw_offer.get("native_desc") or "").strip(),
    )


def _print_summary(
    *,
    before: int,
    filter_report: dict[str, Any],
    source_diag: dict[str, Any],
    built_diag: dict[str, Any],
    top_categories: list[dict[str, Any]],
    qg: dict[str, Any],
    out_file: str,
    raw_out_file: str,
) -> None:
    print("=" * 72)
    print("[ComPortal] build summary")
    print("=" * 72)
    print(f"version: {BUILD_COMPORTAL_VERSION}")
    print(f"before: {before}")
    print(f"after:  {built_diag.get('total', 0)}")
    print(f"raw_out_file: {raw_out_file}")
    print(f"out_file: {out_file}")
    print("-" * 72)
    print("filter_report:")
    print(f"  mode: {filter_report.get('mode')}")
    print(f"  before: {filter_report.get('before')}")
    print(f"  after: {filter_report.get('after')}")
    print(f"  rejected_total: {filter_report.get('rejected_total')}")
    print(f"  allowed_category_count: {filter_report.get('allowed_category_count')}")
    print("-" * 72)
    print("source_diag:")
    for key in (
        "total",
        "available_true",
        "available_false",
        "with_picture",
        "without_picture",
        "with_vendor",
        "without_vendor",
        "with_vendor_code",
        "without_vendor_code",
        "with_category",
        "without_category",
    ):
        print(f"  {key}: {source_diag.get(key)}")
    print("-" * 72)
    print("built_diag:")
    for key in (
        "total",
        "available_true",
        "available_false",
        "with_picture",
        "without_picture",
        "with_vendor",
        "without_vendor",
        "with_vendor_code",
        "without_vendor_code",
        "with_native_desc",
        "without_native_desc",
    ):
        print(f"  {key}: {built_diag.get(key)}")
    print("-" * 72)
    print("top_categories:")
    for row in top_categories[:10]:
        print(f"  {row.get('id')}: {row.get('path') or row.get('name')} -> {row.get('count')}")
    print("-" * 72)
    print(f"quality_gate_ok: {qg.get('ok')}")
    print(f"quality_gate_report: {qg.get('report_path')}")
    print(f"quality_gate_critical_count: {qg.get('critical_count')}")
    print(f"quality_gate_cosmetic_count: {qg.get('cosmetic_count')}")

    critical_preview = qg.get("critical_preview") or []
    if critical_preview:
        print("quality_gate_critical_preview:")
        for item in critical_preview:
            print(f"  - {item}")

    cosmetic_preview = qg.get("cosmetic_preview") or []
    if cosmetic_preview:
        print("quality_gate_cosmetic_preview:")
        for item in cosmetic_preview:
            print(f"  - {item}")

    print("=" * 72)


def main() -> int:
    supplier_name = SUPPLIER_NAME_DEFAULT
    supplier_url = SUPPLIER_URL_DEFAULT
    out_file = OUT_FILE_DEFAULT
    raw_out_file = RAW_OUT_FILE_DEFAULT
    output_encoding = OUTPUT_ENCODING_DEFAULT

    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour=COMPORTAL_NEXT_RUN_HOUR)

    catalog = fetch_catalog_payload()
    source_offers = catalog["offers"]
    category_index = catalog["category_index"]

    before = len(source_offers)
    source_diag = summarize_source_offers(source_offers)
    top_categories = top_source_categories(source_offers, limit=20)

    filtered = filter_offers(source_offers, category_index)
    filtered_offers = filtered["offers"]
    filter_report = filtered["report"]

    built_raw_offers = build_offers(filtered_offers)
    built_diag = summarize_built_offers(built_raw_offers)
    out_offers = [_to_offer_out(x) for x in built_raw_offers]

    write_cs_feed_raw(
        out_offers,
        supplier=supplier_name,
        supplier_url=supplier_url,
        out_file=raw_out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=output_encoding,
    )

    public_vendor = get_public_vendor(supplier_name)
    write_cs_feed(
        out_offers,
        supplier=supplier_name,
        supplier_url=supplier_url,
        out_file=out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=output_encoding,
        public_vendor=public_vendor,
    )

    qg = run_quality_gate(feed_path=raw_out_file)

    _print_summary(
        before=before,
        filter_report=filter_report,
        source_diag=source_diag,
        built_diag=built_diag,
        top_categories=top_categories,
        qg=qg,
        out_file=out_file,
        raw_out_file=raw_out_file,
    )

    if not qg.get("ok", False):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
