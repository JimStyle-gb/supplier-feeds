# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/quality_gate.py

ComPortal quality gate:
- critical issues валят сборку;
- cosmetic issues считаются отдельно;
- baseline может использоваться только для отчёта;
- supplier-specific ошибки должны ловиться здесь, а не core.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import re
import xml.etree.ElementTree as ET

import yaml

_GENERIC_VENDOR_WORDS = {
    "МФП",
    "МФУ",
    "ПРИНТЕР",
    "НОУТБУК",
    "МОНИТОР",
    "ИБП",
    "СКАНЕР",
    "ПРОЕКТОР",
    "КАРТРИДЖ",
    "ТОНЕР",
    "БАТАРЕЯ",
    "АККУМУЛЯТОР",
    "СТАБИЛИЗАТОР",
}


@dataclass(slots=True)
class GateIssue:
    code: str
    oid: str
    detail: str
    severity: str  # critical | cosmetic


def _safe(text: str | None) -> str:
    return (text or "").strip()


def _normalize_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _load_baseline(path: str | Path) -> set[tuple[str, str]]:
    p = Path(path)
    if not p.exists():
        return set()
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return set()
    issues = data.get("known_cosmetic") or []
    out: set[tuple[str, str]] = set()
    for item in issues:
        if not isinstance(item, dict):
            continue
        out.add((str(item.get("code") or "").strip(), str(item.get("oid") or "").strip()))
    return out


def _write_report(
    *,
    report_file: str | Path,
    baseline_file: str | Path,
    critical: list[GateIssue],
    cosmetic: list[GateIssue],
    known_cosmetic: list[GateIssue],
    new_cosmetic: list[GateIssue],
) -> None:
    p = Path(report_file)
    p.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("# Итог проверки quality gate")
    lines.append("QUALITY_GATE: " + ("PASS" if not critical else "FAIL"))
    lines.append("# PASS = можно выпускать | FAIL = есть блокирующие проблемы")
    lines.append("enforce: true")
    lines.append("# true = quality gate реально валит сборку")
    lines.append(f"report_file: {p.as_posix()}")
    lines.append("# Куда записан этот отчёт")
    lines.append(f"baseline_file: {Path(baseline_file).as_posix()}")
    lines.append("# Базовый файл для сравнения известных cosmetic-проблем")
    lines.append("freeze_current_as_baseline: no")
    lines.append("# yes = текущие cosmetic-хвосты сохранены как baseline-снимок")
    lines.append(f"critical_count: {len(critical)}")
    lines.append("# Сколько найдено критичных проблем")
    lines.append(f"cosmetic_total_count: {len(cosmetic)}")
    lines.append("# Общее число некритичных проблем")
    lines.append(f"cosmetic_offer_count: {len({i.oid for i in cosmetic})}")
    lines.append("# В скольких товарах есть cosmetic-проблемы")
    lines.append(f"known_cosmetic_count: {len(known_cosmetic)}")
    lines.append("# Сколько cosmetic-проблем уже известны по baseline")
    lines.append(f"known_cosmetic_offer_count: {len({i.oid for i in known_cosmetic})}")
    lines.append("# В скольких товарах были известные cosmetic-проблемы")
    lines.append(f"new_cosmetic_count: {len(new_cosmetic)}")
    lines.append("# Сколько cosmetic-проблем новые относительно baseline")
    lines.append(f"new_cosmetic_offer_count: {len({i.oid for i in new_cosmetic})}")
    lines.append("# В скольких товарах есть новые cosmetic-проблемы")
    lines.append("")

    lines.append("CRITICAL:")
    if critical:
        for i in critical:
            lines.append(f"- [{i.code}] {i.oid}: {i.detail}")
    else:
        lines.append("# Ошибок в этой секции нет")
    lines.append("")

    lines.append("COSMETIC TOTAL:")
    if cosmetic:
        for i in cosmetic:
            lines.append(f"- [{i.code}] {i.oid}: {i.detail}")
    else:
        lines.append("# Ошибок в этой секции нет")
    lines.append("")

    lines.append("KNOWN COSMETIC:")
    if known_cosmetic:
        for i in known_cosmetic:
            lines.append(f"- [{i.code}] {i.oid}: {i.detail}")
    else:
        lines.append("# Ошибок в этой секции нет")
    lines.append("")

    lines.append("NEW COSMETIC:")
    if new_cosmetic:
        for i in new_cosmetic:
            lines.append(f"- [{i.code}] {i.oid}: {i.detail}")
    else:
        lines.append("# Ошибок в этой секции нет")
    lines.append("")

    p.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _load_schema(path: str | Path) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def _iter_offers(feed_path: str | Path):
    root = ET.parse(feed_path).getroot()
    return root.findall(".//offer")


def run_quality_gate(
    *,
    feed_path: str | Path,
    schema_path: str | Path,
    enforce: bool = True,
) -> dict[str, object]:
    schema = _load_schema(schema_path)
    qg = schema.get("quality_gate") or {}

    baseline_file = qg.get("baseline_file") or "scripts/suppliers/comportal/config/quality_baseline.yml"
    report_file = qg.get("report_file") or "docs/raw/comportal_quality_gate.txt"

    known_keys = _load_baseline(baseline_file)

    critical: list[GateIssue] = []
    cosmetic: list[GateIssue] = []

    seen_ids: set[str] = set()

    for offer in _iter_offers(feed_path):
        oid = _safe(offer.get("id"))
        vendor_code = _safe(offer.findtext("vendorCode"))
        name = _safe(offer.findtext("name"))
        vendor = _safe(offer.findtext("vendor"))
        desc = _normalize_text(_safe(offer.findtext("description")))
        price = _safe(offer.findtext("price"))
        picture = _safe(offer.findtext("picture"))
        params = offer.findall("param")

        if not oid:
            critical.append(GateIssue("missing_id", "", "offer without id", "critical"))
            continue

        if oid in seen_ids:
            critical.append(GateIssue("duplicate_id", oid, "duplicate offer id", "critical"))
        seen_ids.add(oid)

        if not vendor_code:
            critical.append(GateIssue("missing_vendorcode", oid, "empty vendorCode", "critical"))

        if not name:
            critical.append(GateIssue("missing_name", oid, "empty name", "critical"))

        if not vendor:
            critical.append(GateIssue("missing_vendor", oid, "empty vendor", "critical"))
        elif vendor.upper() in _GENERIC_VENDOR_WORDS:
            critical.append(GateIssue("generic_vendor", oid, f"generic vendor '{vendor}'", "critical"))

        if not price or not price.isdigit():
            critical.append(GateIssue("bad_price", oid, f"invalid price '{price}'", "critical"))

        if not desc:
            cosmetic.append(GateIssue("empty_desc", oid, "empty supplier native_desc in raw", "cosmetic"))

        if not picture:
            cosmetic.append(GateIssue("missing_picture", oid, "missing picture in raw", "cosmetic"))

        if not params:
            cosmetic.append(GateIssue("missing_params", oid, "no params in raw", "cosmetic"))

    known_cosmetic: list[GateIssue] = []
    new_cosmetic: list[GateIssue] = []
    for issue in cosmetic:
        if (issue.code, issue.oid) in known_keys:
            known_cosmetic.append(issue)
        else:
            new_cosmetic.append(issue)

    _write_report(
        report_file=report_file,
        baseline_file=baseline_file,
        critical=critical,
        cosmetic=cosmetic,
        known_cosmetic=known_cosmetic,
        new_cosmetic=new_cosmetic,
    )

    ok = not critical
    if enforce and critical:
        ok = False

    return {
        "ok": ok,
        "critical_count": len(critical),
        "cosmetic_total_count": len(cosmetic),
        "known_cosmetic_count": len(known_cosmetic),
        "new_cosmetic_count": len(new_cosmetic),
        "critical_preview": [f"[{i.code}] {i.oid}: {i.detail}" for i in critical[:10]],
        "report_file": str(report_file),
        "baseline_file": str(baseline_file),
    }
