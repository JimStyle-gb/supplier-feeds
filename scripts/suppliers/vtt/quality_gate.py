# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/quality_gate.py

VTT quality gate.
Минимальный gate для supplier raw:
- feed существует
- есть offer
- нет пустых price/vendorCode/id
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class QualityGateResult:
    ok: bool
    report_path: str
    critical_count: int
    cosmetic_count: int


def run_quality_gate(*, feed_path: str, report_path: str) -> QualityGateResult:
    p = Path(feed_path)
    report = Path(report_path)
    errors: list[str] = []
    warnings: list[str] = []

    if not p.exists():
        errors.append("feed_missing")
    else:
        text = p.read_text(encoding="utf-8", errors="ignore")
        offers = len(re.findall(r"<offer\b", text))
        if offers <= 0:
            errors.append("offers_zero")
        if re.search(r"<price>\s*</price>", text):
            errors.append("empty_price_tag")
        if re.search(r"<offer[^>]*id=\"\s*\"", text):
            errors.append("empty_offer_id")
        if re.search(r"<vendorCode>\s*</vendorCode>", text):
            errors.append("empty_vendorcode")
        if "placehold.co/800x800/png?text=No+Photo" in text:
            warnings.append("placeholder_pictures_present")

    lines: list[str] = []
    lines.append("VTT quality gate")
    lines.append("=" * 72)
    lines.append(f"feed_path: {feed_path}")
    lines.append(f"critical_count: {len(errors)}")
    lines.append(f"cosmetic_count: {len(warnings)}")
    if errors:
        lines.append("-" * 72)
        lines.append("critical:")
        lines.extend(f"  - {x}" for x in errors)
    if warnings:
        lines.append("-" * 72)
        lines.append("cosmetic:")
        lines.extend(f"  - {x}" for x in warnings)

    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    return QualityGateResult(
        ok=(len(errors) == 0),
        report_path=str(report),
        critical_count=len(errors),
        cosmetic_count=len(warnings),
    )
