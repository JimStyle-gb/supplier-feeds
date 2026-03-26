# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/quality_gate.py

VTT quality gate.
Минимальный gate для supplier raw:
- feed существует
- есть offer
- нет пустых price/vendorCode/id
- отчёт всегда явно обновляется на каждом прогоне
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path


@dataclass(frozen=True)
class QualityGateResult:
    ok: bool
    report_path: str
    critical_count: int
    cosmetic_count: int


def _now_almaty_str() -> str:
    almaty = timezone(timedelta(hours=5))
    return datetime.now(almaty).strftime("%Y-%m-%d %H:%M:%S")


def run_quality_gate(*, feed_path: str, report_path: str) -> QualityGateResult:
    p = Path(feed_path)
    report = Path(report_path)
    errors: list[str] = []
    warnings: list[str] = []
    offers = 0
    feed_size_bytes = 0
    feed_sha1 = ""

    if not p.exists():
        errors.append("feed_missing")
    else:
        text = p.read_text(encoding="utf-8", errors="ignore")
        feed_size_bytes = p.stat().st_size
        feed_sha1 = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:12]
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
    lines.append(f"generated_at_almaty: {_now_almaty_str()}")
    lines.append(f"feed_path: {feed_path}")
    lines.append(f"offers: {offers}")
    lines.append(f"feed_size_bytes: {feed_size_bytes}")
    lines.append(f"feed_sha1_12: {feed_sha1}")
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
