# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/diagnostics.py

VTT diagnostics layer.

Что делает:
- печатает стабильный build summary для orchestrator;
- держит summary-логику вне build_vtt.py;
- не содержит supplier-business логики.

Важно:
- это только operational tooling;
- build_vtt.py должен вызывать print_build_summary(...),
  а не держать свой локальный _print_summary().
"""

from __future__ import annotations

from typing import Any


_SUMMARY_WIDTH = 72


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    s = str(value or "").strip().casefold()
    return s in {"1", "true", "yes", "y", "on"}


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _get_qg_attr(qg: Any, name: str, default: Any = None) -> Any:
    if qg is None:
        return default
    if isinstance(qg, dict):
        return qg.get(name, default)
    return getattr(qg, name, default)


def print_build_summary(
    *,
    version: str,
    before: int,
    after: int,
    raw_out_file: str,
    out_file: str,
    qg: Any,
    availability_true: int,
    availability_false: int,
) -> None:
    """Печатает стабильный summary прогона VTT."""
    print("=" * _SUMMARY_WIDTH)
    print("[VTT] build summary")
    print("=" * _SUMMARY_WIDTH)
    print(f"version: {version}")
    print(f"before: {before}")
    print(f"after:  {after}")
    print(f"raw_out_file: {raw_out_file}")
    print(f"out_file: {out_file}")
    print("-" * _SUMMARY_WIDTH)
    print(f"quality_gate_ok:       {_safe_bool(_get_qg_attr(qg, 'ok', True))}")
    print(f"quality_gate_report:   {_safe_text(_get_qg_attr(qg, 'report_path', _get_qg_attr(qg, 'report_file', '')))}")
    print(f"quality_gate_critical: {_safe_int(_get_qg_attr(qg, 'critical_count', 0))}")
    print(f"quality_gate_cosmetic: {_safe_int(_get_qg_attr(qg, 'cosmetic_count', 0))}")
    print(f"availability_true:     {_safe_int(availability_true)}")
    print(f"availability_false:    {_safe_int(availability_false)}")
    print("=" * _SUMMARY_WIDTH)
