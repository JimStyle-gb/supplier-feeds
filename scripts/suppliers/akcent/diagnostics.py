# -*- coding: utf-8 -*-
"""
AkCent diagnostics layer.

Что делает:
- печатает короткую и понятную сводку сборки
- не влияет на данные
- нужен только для удобного отчёта в CI / local run
"""

from __future__ import annotations

from typing import Any


def _fmt_map(data: dict[str, Any] | None, indent: int = 2) -> list[str]:
    if not data:
        return []
    lines: list[str] = []
    pad = " " * indent

    for key in sorted(data.keys()):
        value = data[key]
        if isinstance(value, dict):
            lines.append(f"{pad}{key}:")
            lines.extend(_fmt_map(value, indent=indent + 2))
        else:
            lines.append(f"{pad}{key}: {value}")
    return lines


def print_build_summary(
    *,
    supplier: str,
    version: str,
    before: int,
    after: int,
    filter_report: dict[str, Any] | None,
    build_report: dict[str, Any] | None,
    out_file: str,
    raw_out_file: str,
) -> None:
    print("=" * 72)
    print(f"[{supplier}] build summary")
    print("=" * 72)
    print(f"version: {version}")
    print(f"before: {before}")
    print(f"after: {after}")
    print(f"raw_out_file: {raw_out_file}")
    print(f"out_file: {out_file}")

    if filter_report:
        print("-" * 72)
        print("filter_report:")
        for line in _fmt_map(filter_report, indent=2):
            print(line)

    if build_report:
        print("-" * 72)
        print("build_report:")
        for line in _fmt_map(build_report, indent=2):
            print(line)

    print("=" * 72)
