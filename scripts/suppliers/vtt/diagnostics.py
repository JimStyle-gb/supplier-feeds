# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/diagnostics.py
"""

from __future__ import annotations


def print_build_summary(
    *,
    version: str,
    before: int,
    after: int,
    raw_out_file: str,
    out_file: str,
    qg,
    availability_true: int,
    availability_false: int,
) -> None:
    print("=" * 72)
    print("[VTT] build summary")
    print("=" * 72)
    print("version:", version)
    print(f"before: {before}")
    print(f"after:  {after}")
    print(f"raw_out_file: {raw_out_file}")
    print(f"out_file: {out_file}")
    print("-" * 72)
    print(f"quality_gate_ok:       {qg.ok}")
    print(f"quality_gate_report:   {qg.report_path}")
    print(f"quality_gate_critical: {qg.critical_count}")
    print(f"quality_gate_cosmetic: {qg.cosmetic_count}")
    print(f"availability_true:     {availability_true}")
    print(f"availability_false:    {availability_false}")
    print("=" * 72)
