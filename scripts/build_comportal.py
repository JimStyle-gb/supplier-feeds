# -*- coding: utf-8 -*-
"""
Path: scripts/build_comportal.py
ComPortal adapter under CS-template.

Что делает:
- читает YML ComPortal;
- применяет category-first whitelist filter;
- собирает clean raw offers;
- пишет raw/final feed через shared core;
- печатает build summary.

Важно:
- ComPortal здесь идёт как param-first supplier;
- префикс offer/vendorCode = CP;
- quality gate на этом шаге пока optional: если модуль ещё не создан,
  сборка не падает.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable, List

from cs.core import OfferOut, get_public_vendor, write_cs_feed, write_cs_feed_raw
from cs.meta import next_run_at_hour, now_almaty

from suppliers.comportal.builder import build_offers
from suppliers.comportal.filtering import filter_offers
from suppliers.comportal.source import fetch_catalog_payload


BUILD_COMPORTAL_VERSION = "build_comportal_v6_source_filter_normalize_params_builder"

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


def _maybe_run_quality_gate(raw_out_file: str) -> dict[str, Any]:
    """
    Optional quality gate.
    Если модуля ещё нет — пропускаем без падения сборки.
    """
    try:
        from suppliers.comportal.quality_gate import run_quality_gate  # type: ignore
    except Exception:
        return {
            "ok": None,
            "skipped": True,
            "reason": "quality_gate module not created yet",
            "report_path": "",
        }

    try:
        return run_quality_gate(feed_path=raw_out_file)
    except TypeError:
        # backward-safe fallback на случай другой сигнатуры
        return run_quality_gate(raw_out_file)
    except Exception as exc:
        return {
            "ok": False,
            "skipped": False,
            "reason": f"quality_gate error: {exc}",
            "report_path": "",
        }


def _print_summary(
    *,
    before: int,
    built_raw_offers: list[dict[str, Any]],
    filter_report: dict[str, Any],
    qg: dict[str, Any],
    out_file: str,
    raw_out_file: str,
) -> None:
    after = len(built_raw_offers)
    in_true = sum(1 for offer in built_raw_offers if offer.get("available"))
    in_false = after - in_true

    print("=" * 72)
    print("[ComPortal] build summary")
    print("=" * 72)
    print(f"version: {BUILD_COMPORTAL_VERSION}")
    print(f"before: {before}")
    print(f"after:  {after}")
    print(f"raw_out_file: {raw_out_file}")
    print(f"out_file: {out_file}")
    print("-" * 72)
    print("filter_report:")
    for key, value in filter_report.items():
        print(f"  {key}: {value}")
    print("-" * 72)
    print(f"quality_gate_ok:   {qg.get('ok')}")
    print(f"quality_gate_skip: {qg.get('skipped')}")
    if qg.get("reason"):
        print(f"quality_gate_note: {qg.get('reason')}")
    if qg.get("report_path"):
        print(f"quality_gate_report: {qg.get('report_path')}")
    print(f"availability_true:  {in_true}")
    print(f"availability_false: {in_false}")
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

    filtered = filter_offers(source_offers, category_index)
    filtered_offers = filtered["offers"]
    filter_report = filtered["report"]

    built_raw_offers = build_offers(filtered_offers)
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

    qg = _maybe_run_quality_gate(raw_out_file)

    _print_summary(
        before=before,
        built_raw_offers=built_raw_offers,
        filter_report=filter_report,
        qg=qg,
        out_file=out_file,
        raw_out_file=raw_out_file,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
