# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/quality_gate.py

ComPortal quality gate.

v3:
- стиль отчёта приведён к канону AkCent / AlStyle / CopyLine;
- baseline default path -> quality_gate_baseline.yml;
- секции строго: CRITICAL / COSMETIC TOTAL / NEW COSMETIC / KNOWN COSMETIC;
- добавлены max_cosmetic_offers / max_cosmetic_issues в шапку;
- логика known/new cosmetic через baseline сохранена.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import re
import xml.etree.ElementTree as ET

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore

QUALITY_BASELINE_DEFAULT = "scripts/suppliers/comportal/config/quality_gate_baseline.yml"
QUALITY_REPORT_DEFAULT = "docs/raw/comportal_quality_gate.txt"
PLACEHOLDER_URL = "https://placehold.co/800x800/png?text=No+Photo"
_WS_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class QualityIssue:
    severity: str
    rule: str
    oid: str
    name: str
    details: str


def _norm_ws(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _read_yaml(path: str | None) -> dict[str, Any]:
    if not path or yaml is None:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _write_yaml(path: str | None, data: dict[str, Any]) -> None:
    if not path or yaml is None:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _make_issue(severity: str, rule: str, oid: str, name: str, details: str) -> QualityIssue:
    return QualityIssue(
        severity=severity,
        rule=rule,
        oid=_norm_ws(oid),
        name=_norm_ws(name),
        details=_norm_ws(details),
    )


def _detect_issues(feed_path: str, schema_path: str | None = None) -> list[QualityIssue]:
    xml_text = Path(feed_path).read_text(encoding="utf-8", errors="ignore")
    root = ET.fromstring(xml_text)

    schema = _read_yaml(schema_path)
    blacklist = {str(x).strip().casefold() for x in (schema.get("vendor_blacklist_casefold") or []) if str(x).strip()}
    placeholder = str(schema.get("placeholder_picture") or PLACEHOLDER_URL).strip() or PLACEHOLDER_URL

    issues: list[QualityIssue] = []

    for offer in root.findall(".//offer"):
        oid = _norm_ws(offer.get("id") or "")
        name = _norm_ws(offer.findtext("name") or "")
        vendor = _norm_ws(offer.findtext("vendor") or "")
        desc_html = offer.findtext("description") or ""

        if not vendor:
            issues.append(_make_issue("critical", "empty_vendor", oid, name, ""))

        if vendor and vendor.casefold() in blacklist:
            issues.append(_make_issue("critical", "supplier_vendor_leak", oid, name, vendor))

        price_text = _norm_ws(offer.findtext("price") or "")
        if not price_text:
            issues.append(_make_issue("critical", "empty_price", oid, name, ""))

        for pic in offer.findall("picture"):
            url = _norm_ws("".join(pic.itertext()))
            if url == placeholder:
                issues.append(_make_issue("cosmetic", "placeholder_picture", oid, name, url))

        if "oaicite" in desc_html or "contentReference" in desc_html:
            issues.append(_make_issue("critical", "desc_oaicite_leak", oid, name, "oaicite/contentReference"))

    deduped: dict[tuple[str, str, str, str], QualityIssue] = {}
    for issue in issues:
        deduped[(issue.severity, issue.rule, issue.oid, issue.details)] = issue
    return sorted(deduped.values(), key=lambda x: (x.severity, x.rule, x.oid, x.details))


def _load_baseline(path: str | None) -> dict[str, set[str]]:
    data = _read_yaml(path)
    raw = data.get("accepted_cosmetic") or {}
    out: dict[str, set[str]] = {}
    for rule, ids in raw.items():
        out[str(rule)] = {str(x).strip() for x in (ids or []) if str(x).strip()}
    return out


def _make_baseline_payload(cosmetic: list[QualityIssue]) -> dict[str, Any]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for issue in cosmetic:
        grouped[issue.rule].append(issue.oid)
    return {
        "schema_version": 1,
        "accepted_cosmetic": {rule: sorted(set(oids)) for rule, oids in sorted(grouped.items())},
    }


def _section(lines: list[str], title: str, issues: list[QualityIssue]) -> None:
    lines.append("")
    lines.append(f"{title}:")
    if not issues:
        lines.append("# Ошибок в этой секции нет")
        return
    for issue in issues:
        if issue.details:
            lines.append(f"  - {issue.oid} | {issue.rule} | {issue.details}")
        else:
            lines.append(f"  - {issue.oid} | {issue.rule}")


def _write_report(
    path: str,
    *,
    passed: bool,
    enforce: bool,
    baseline_file: str,
    freeze_current_as_baseline: bool,
    critical: list[QualityIssue],
    cosmetic: list[QualityIssue],
    known_cosmetic: list[QualityIssue],
    new_cosmetic: list[QualityIssue],
    max_cosmetic_offers: int,
    max_cosmetic_issues: int,
) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    cosmetic_offer_count = len({x.oid for x in cosmetic})
    known_offer_count = len({x.oid for x in known_cosmetic})
    new_offer_count = len({x.oid for x in new_cosmetic})

    lines: list[str] = []
    lines.append("# Итог проверки quality gate")
    lines.append(f"QUALITY_GATE: {'PASS' if passed else 'FAIL'}")
    lines.append("# PASS = можно выпускать | FAIL = есть блокирующие проблемы")
    lines.append(f"enforce: {'true' if enforce else 'false'}")
    lines.append("# true = quality gate реально валит сборку")
    lines.append(f"report_file: {path}")
    lines.append("# Куда записан этот отчёт")
    lines.append(f"baseline_file: {baseline_file}")
    lines.append("# Базовый файл для сравнения известных cosmetic-проблем")
    lines.append(f"freeze_current_as_baseline: {'yes' if freeze_current_as_baseline else 'no'}")
    lines.append("# yes = текущие cosmetic-хвосты сохранены как baseline-снимок")
    lines.append(f"critical_count: {len(critical)}")
    lines.append("# Сколько найдено критичных проблем")
    lines.append(f"cosmetic_total_count: {len(cosmetic)}")
    lines.append("# Общее число некритичных проблем")
    lines.append(f"cosmetic_offer_count: {cosmetic_offer_count}")
    lines.append("# В скольких товарах есть cosmetic-проблемы")
    lines.append(f"known_cosmetic_count: {len(known_cosmetic)}")
    lines.append("# Сколько cosmetic-проблем уже известны по baseline")
    lines.append(f"known_cosmetic_offer_count: {known_offer_count}")
    lines.append("# В скольких товарах есть уже известные cosmetic-проблемы")
    lines.append(f"new_cosmetic_count: {len(new_cosmetic)}")
    lines.append("# Сколько найдено новых cosmetic-проблем")
    lines.append(f"new_cosmetic_offer_count: {new_offer_count}")
    lines.append("# В скольких товарах появились новые cosmetic-проблемы")
    lines.append(f"max_cosmetic_offers: {int(max_cosmetic_offers)}")
    lines.append("# Допустимый максимум товаров с cosmetic-проблемами")
    lines.append(f"max_cosmetic_issues: {int(max_cosmetic_issues)}")
    lines.append("# Допустимый максимум cosmetic-проблем всего")

    _section(lines, "CRITICAL", critical)
    _section(lines, "COSMETIC TOTAL", cosmetic)
    _section(lines, "NEW COSMETIC", new_cosmetic)
    _section(lines, "KNOWN COSMETIC", known_cosmetic)

    p.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def run_quality_gate(
    *,
    feed_path: str,
    schema_path: str | None = None,
    enforce: bool = True,
    baseline_path: str | None = None,
    report_path: str | None = None,
    max_new_cosmetic_offers: int = 5,
    max_new_cosmetic_issues: int = 5,
    freeze_current_as_baseline: bool = False,
) -> dict[str, object]:
    baseline_path = str(baseline_path or QUALITY_BASELINE_DEFAULT)
    report_path = str(report_path or QUALITY_REPORT_DEFAULT)

    issues = _detect_issues(feed_path, schema_path=schema_path)
    critical = [x for x in issues if x.severity == "critical"]
    cosmetic = [x for x in issues if x.severity == "cosmetic"]

    if freeze_current_as_baseline:
        _write_yaml(baseline_path, _make_baseline_payload(cosmetic))

    baseline = _load_baseline(baseline_path)

    known_cosmetic: list[QualityIssue] = []
    new_cosmetic: list[QualityIssue] = []
    for issue in cosmetic:
        if issue.oid in baseline.get(issue.rule, set()):
            known_cosmetic.append(issue)
        else:
            new_cosmetic.append(issue)

    new_offer_count = len({x.oid for x in new_cosmetic})
    passed = (
        len(critical) == 0
        and new_offer_count <= int(max_new_cosmetic_offers)
        and len(new_cosmetic) <= int(max_new_cosmetic_issues)
    )
    ok = True if not enforce else passed

    _write_report(
        report_path,
        passed=passed,
        enforce=enforce,
        baseline_file=baseline_path,
        freeze_current_as_baseline=freeze_current_as_baseline,
        critical=critical,
        cosmetic=cosmetic,
        known_cosmetic=known_cosmetic,
        new_cosmetic=new_cosmetic,
        max_cosmetic_offers=int(max_new_cosmetic_offers),
        max_cosmetic_issues=int(max_new_cosmetic_issues),
    )

    return {
        "ok": ok,
        "report_file": report_path,
        "baseline_file": baseline_path,
        "critical_count": len(critical),
        "cosmetic_total_count": len(cosmetic),
        "known_cosmetic_count": len(known_cosmetic),
        "new_cosmetic_count": len(new_cosmetic),
        "critical_preview": [
            f"{x.oid} | {x.rule} | {x.details}".strip(" |")
            for x in critical[:20]
        ],
    }
