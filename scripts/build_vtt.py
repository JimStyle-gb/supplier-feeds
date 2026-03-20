# -*- coding: utf-8 -*-
"""
Path: scripts/build_vtt.py
VTT — CS orchestrator.

Задача файла:
- получить source VTT;
- распарсить сырой список товаров;
- собрать OfferOut через supplier-layer;
- записать raw и final фиды;
- запустить quality gate, если модуль уже добавлен.
"""

from __future__ import annotations

import os
from typing import Any

from cs.core import (
    get_public_vendor,
    next_run_dom_at_hour,
    now_almaty,
    write_cs_feed,
    write_cs_feed_raw,
)
from suppliers.vtt.builder import build_offer_from_raw
from suppliers.vtt.source import fetch_vtt_source, parse_vtt_source


SUPPLIER_NAME = "VTT"
SUPPLIER_URL_DEFAULT = (os.getenv("SUPPLIER_URL", "https://b2b.vtt.ru/catalog/") or "https://b2b.vtt.ru/catalog/").strip()
OUT_FILE = os.getenv("OUT_FILE", "docs/vtt.yml")
RAW_OUT_FILE = os.getenv("RAW_OUT_FILE", "docs/raw/vtt.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "utf-8") or "utf-8").strip() or "utf-8"
VTT_POLICY_YML = os.getenv("VTT_POLICY_YML", "scripts/suppliers/vtt/config/policy.yml")
VTT_QG_BASELINE = os.getenv("VTT_QG_BASELINE", "scripts/suppliers/vtt/config/quality_gate_baseline.yml")
VTT_QG_REPORT = os.getenv("VTT_QG_REPORT", "docs/raw/vtt_quality_gate.txt")
BUILD_VTT_VERSION = "build_vtt_v001_supplier_layer_start"


def _param_priority() -> tuple[str, ...]:
    """Приоритет параметров для final-feed."""
    return (
        "Тип",
        "Для бренда",
        "Коды расходников",
        "Партномер",
        "Совместимость",
        "Технология печати",
        "Цвет",
        "Количество страниц (5% заполнение)",
        "Ресурс",
        "Модель",
    )


def _run_quality_gate() -> dict[str, Any]:
    """Пробует запустить VTT quality gate; если файла ещё нет — мягко пропускает."""
    try:
        from suppliers.vtt.quality import run_quality_gate  # type: ignore
    except ModuleNotFoundError:
        return {
            "ok": True,
            "report_path": VTT_QG_REPORT,
            "skipped": True,
            "reason": "suppliers.vtt.quality not found yet",
        }
    except Exception as e:  # pragma: no cover
        return {
            "ok": False,
            "report_path": VTT_QG_REPORT,
            "skipped": False,
            "reason": f"quality import error: {e}",
        }

    try:
        return run_quality_gate(
            feed_path=RAW_OUT_FILE,
            policy_path=VTT_POLICY_YML,
            baseline_path=VTT_QG_BASELINE,
            report_path=VTT_QG_REPORT,
        )
    except FileNotFoundError:
        return {
            "ok": True,
            "report_path": VTT_QG_REPORT,
            "skipped": True,
            "reason": "quality config files not found yet",
        }


def _print_summary(*, before: int, after: int, qg: dict[str, Any], in_true: int, in_false: int) -> None:
    """Печатает итоговый summary сборки."""
    print("=" * 72)
    print("[VTT] build summary")
    print("=" * 72)
    print(f"version: {BUILD_VTT_VERSION}")
    print(f"before: {before}")
    print(f"after:  {after}")
    print(f"raw_out_file: {RAW_OUT_FILE}")
    print(f"out_file: {OUT_FILE}")
    print("-" * 72)
    print(f"quality_gate_ok:     {qg.get('ok')}")
    print(f"quality_gate_report: {qg.get('report_path')}")
    if qg.get("skipped"):
        print(f"quality_gate_skipped: {qg.get('reason')}")
    print(f"availability_true:   {in_true}")
    print(f"availability_false:  {in_false}")
    print("=" * 72)


def main() -> int:
    """Главный пайплайн VTT."""
    build_time = now_almaty().replace(tzinfo=None)
    next_run = next_run_dom_at_hour(build_time, 5, (1, 10, 20))

    source_bytes = fetch_vtt_source()
    raw_items = parse_vtt_source(source_bytes)
    before = len(raw_items)

    out_offers = []
    seen_oids: set[str] = set()

    for raw in raw_items:
        offer = build_offer_from_raw(raw)
        if not offer:
            continue
        if offer.oid in seen_oids:
            continue
        seen_oids.add(offer.oid)
        out_offers.append(offer)

    out_offers.sort(key=lambda o: o.oid)

    write_cs_feed_raw(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL_DEFAULT,
        out_file=RAW_OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
    )

    public_vendor = get_public_vendor(SUPPLIER_NAME)
    write_cs_feed(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL_DEFAULT,
        out_file=OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        public_vendor=public_vendor,
        param_priority=_param_priority(),
    )

    qg = _run_quality_gate()
    after = len(out_offers)
    in_true = sum(1 for o in out_offers if o.available)
    in_false = after - in_true

    _print_summary(before=before, after=after, qg=qg, in_true=in_true, in_false=in_false)

    if not qg.get("ok", True):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
