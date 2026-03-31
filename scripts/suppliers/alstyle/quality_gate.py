# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/quality_gate.py

AlStyle quality gate:
- critical issues всегда валят сборку;
- cosmetic issues НЕ исключаются из подсчёта;
- baseline используется только для отчёта:
  какие cosmetic уже известны, а какие новые;
- сборка проходит, пока ОБЩЕЕ количество cosmetic
  не превышает порог;
- freeze_current_as_baseline сохраняет текущее состояние
  как справочный baseline, но не выключает будущий контроль;
- ship_title_prefix полностью убран как лишнее и неуниверсальное правило.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from html import unescape
from pathlib import Path
import re
import xml.etree.ElementTree as ET

import yaml

from cs.qg_report import write_quality_gate_report


_COMPAT_LABEL_LEAK_RE = re.compile(
    r"(?iu)\b(?:Характеристики|Модель|Совместимые\s+модели|Технология\s+печати|Цвет(?:\s+печати)?)\b"
)
_BAD_POWER_KEY_RE = re.compile(r"(?iu)^Мощность\s*\((?:bt|bт|вt)\)$")
_XEROX_FAMILY_RE = re.compile(
    r"(?iu)\b(?:VersaLink|AltaLink|Versant|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser)\b"
)
_WS_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class QualityIssue:
    severity: str
    rule: str
    oid: str
    name: str
    details: str


def _norm_ws(s: str) -> str:
    s2 = unescape(s or "")
    s2 = s2.replace("\u00a0", " ").strip()
    s2 = _WS_RE.sub(" ", s2).strip()
    return s2


def _read_yaml(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def _write_yaml(path: str, data: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _offer_params(offer_el: ET.Element) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    for p in offer_el.findall("param"):
        k = _norm_ws(p.get("name") or "")
        v = _norm_ws("".join(p.itertext()))
        if k and v:
            out[k].append(v)
    return dict(out)


def _detect_issues(feed_path: str) -> list[QualityIssue]:
    xml_text = Path(feed_path).read_text(encoding="utf-8", errors="ignore")
    root = ET.fromstring(xml_text)

    issues: list[QualityIssue] = []

    for offer in root.findall(".//offer"):
        oid = _norm_ws(offer.get("id") or "")
        name = _norm_ws(offer.findtext("name") or "")
        desc_html = offer.findtext("description") or ""
        params = _offer_params(offer)

        compat_values = params.get("Совместимость", [])
        for compat in compat_values:
            if _COMPAT_LABEL_LEAK_RE.search(compat):
                issues.append(
                    QualityIssue(
                        severity="critical",
                        rule="compat_label_leak",
                        oid=oid,
                        name=name,
                        details=compat[:200],
                    )
                )

            families = {x.casefold() for x in _XEROX_FAMILY_RE.findall(compat)}
            if len(families) >= 3 and len(compat) >= 180:
                issues.append(
                    QualityIssue(
                        severity="cosmetic",
                        rule="heavy_xerox_compat",
                        oid=oid,
                        name=name,
                        details=compat[:200],
                    )
                )

        for key in params:
            if _BAD_POWER_KEY_RE.match(key):
                issues.append(
                    QualityIssue(
                        severity="critical",
                        rule="bad_power_key",
                        oid=oid,
                        name=name,
                        details=key,
                    )
                )

        if "oaicite" in desc_html or "contentReference" in desc_html:
            issues.append(
                QualityIssue(
                    severity="critical",
                    rule="desc_oaicite_leak",
                    oid=oid,
                    name=name,
                    details="oaicite/contentReference",
                )
            )

    deduped: dict[tuple[str, str, str], QualityIssue] = {}
    for issue in issues:
        deduped[(issue.severity, issue.rule, issue.oid)] = issue

    return sorted(deduped.values(), key=lambda x: (x.severity, x.rule, x.oid))


def _load_cosmetic_baseline(baseline_path: str) -> dict[str, set[str]]:
    data = _read_yaml(baseline_path)
    raw = data.get("accepted_cosmetic") or {}
    out: dict[str, set[str]] = {}
    for rule, oids in raw.items():
        out[str(rule)] = {str(x).strip() for x in (oids or []) if str(x).strip()}
    return out


def _make_baseline_payload(cosmetic: list[QualityIssue]) -> dict:
    grouped: dict[str, list[str]] = defaultdict(list)
    for issue in cosmetic:
        grouped[issue.rule].append(issue.oid)

    payload = {
        "schema_version": 1,
        "accepted_cosmetic": {},
    }
    for rule in sorted(grouped):
        payload["accepted_cosmetic"][rule] = sorted(set(grouped[rule]))
    return payload


def _write_report(
    path: str,
    *,
    critical: list[QualityIssue],
    cosmetic: list[QualityIssue],
    known_cosmetic: list[QualityIssue],
    new_cosmetic: list[QualityIssue],
    accepted_cosmetic: dict[str, set[str]],
    max_cosmetic_offers: int,
    max_cosmetic_issues: int,
    passed: bool,
    baseline_path: str,
    frozen: bool,
    enforce: bool,
) -> None:
    """Единый writer отчёта."""
    write_quality_gate_report(
        path,
        passed=passed,
        enforce=enforce,
        baseline_path=baseline_path,
        freeze_current_as_baseline=frozen,
        critical=critical,
        cosmetic=cosmetic,
        known_cosmetic=known_cosmetic,
        new_cosmetic=new_cosmetic,
        max_cosmetic_offers=max_cosmetic_offers,
        max_cosmetic_issues=max_cosmetic_issues,
        accepted_cosmetic=accepted_cosmetic,
    )


def run_quality_gate(
    *,
    feed_path: str,
    baseline_path: str,
    report_path: str,
    max_new_cosmetic_offers: int = 5,
    max_new_cosmetic_issues: int = 5,
    enforce: bool = True,
    freeze_current_as_baseline: bool = False,
) -> tuple[bool, str]:
    """
    ВАЖНО:
    Для совместимости с уже существующим build_alstyle.py
    сохраняем старые имена аргументов:
      max_new_cosmetic_offers / max_new_cosmetic_issues

    Но теперь они трактуются как:
      max_cosmetic_offers / max_cosmetic_issues

    То есть baseline НЕ исключает issues из подсчёта.
    Baseline нужен только для справки в отчёте.
    """

    issues = _detect_issues(feed_path)
    critical = [x for x in issues if x.severity == "critical"]
    cosmetic = [x for x in issues if x.severity == "cosmetic"]

    if freeze_current_as_baseline:
        payload = _make_baseline_payload(cosmetic)
        _write_yaml(baseline_path, payload)
        accepted_cosmetic = _load_cosmetic_baseline(baseline_path)
        _write_report(
            report_path,
            critical=critical,
            cosmetic=cosmetic,
            known_cosmetic=cosmetic,
            new_cosmetic=[],
            accepted_cosmetic=accepted_cosmetic,
            max_cosmetic_offers=int(max_new_cosmetic_offers),
            max_cosmetic_issues=int(max_new_cosmetic_issues),
            passed=(len(critical) == 0),
            baseline_path=baseline_path,
            frozen=True,
            enforce=enforce,
        )
        return (len(critical) == 0), (
            f"[quality_gate] BASELINE_FROZEN | "
            f"critical={len(critical)} | "
            f"cosmetic_total={len(cosmetic)} | "
            f"cosmetic_offers={len({x.oid for x in cosmetic})} | "
            f"baseline={baseline_path}"
        )

    accepted_cosmetic = _load_cosmetic_baseline(baseline_path)

    known_cosmetic = [
        x for x in cosmetic
        if x.oid in accepted_cosmetic.get(x.rule, set())
    ]
    new_cosmetic = [
        x for x in cosmetic
        if x.oid not in accepted_cosmetic.get(x.rule, set())
    ]

    cosmetic_offer_count = len({x.oid for x in cosmetic})
    cosmetic_issue_count = len(cosmetic)

    passed = (
        len(critical) == 0
        and cosmetic_offer_count <= int(max_new_cosmetic_offers)
        and cosmetic_issue_count <= int(max_new_cosmetic_issues)
    )

    _write_report(
        report_path,
        critical=critical,
        cosmetic=cosmetic,
        known_cosmetic=known_cosmetic,
        new_cosmetic=new_cosmetic,
        accepted_cosmetic=accepted_cosmetic,
        max_cosmetic_offers=int(max_new_cosmetic_offers),
        max_cosmetic_issues=int(max_new_cosmetic_issues),
        passed=(passed or not enforce),
        baseline_path=baseline_path,
        frozen=False,
        enforce=enforce,
    )

    summary = (
        f"[quality_gate] {'PASS' if (passed or not enforce) else 'FAIL'} | "
        f"critical={len(critical)} | "
        f"cosmetic_total={cosmetic_issue_count} | "
        f"cosmetic_offers={cosmetic_offer_count} | "
        f"known_cosmetic={len(known_cosmetic)} | "
        f"new_cosmetic={len(new_cosmetic)} | "
        f"baseline={baseline_path} | report={report_path}"
    )

    if not enforce:
        return True, summary + " | enforce=no"

    return passed, summary
