"""CopyLine diagnostics helpers."""

from __future__ import annotations

from typing import Any


def _print_filter_report(filter_report: dict[str, Any]) -> None:
    print("filter_report:")
    for key, value in filter_report.items():
        print(f"  {key}: {value}")


def print_build_summary(
    *,
    version: str,
    before: int,
    out_offers: list[Any],
    filter_report: dict[str, Any],
    qg: dict[str, Any],
    out_file: str,
    raw_out_file: str,
) -> None:
    """Печать итогового summary по сборке."""
    after = len(out_offers)
    in_true = sum(1 for offer in out_offers if getattr(offer, "available", False))
    in_false = after - in_true

    print("=" * 72)
    print("[CopyLine] build summary")
    print("=" * 72)
    print(f"version: {version}")
    print(f"before: {before}")
    print(f"after:  {after}")
    print(f"raw_out_file: {raw_out_file}")
    print(f"out_file: {out_file}")
    print("-" * 72)
    _print_filter_report(filter_report)
    print("-" * 72)
    print(f"quality_gate_ok:   {qg.get('ok')}")
    print(f"quality_gate_report: {qg.get('report_path') or qg.get('report_file')}")
    print(f"availability_true:  {in_true}")
    print(f"availability_false: {in_false}")
    print("=" * 72)
