# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/quality_gate.py

AlStyle quality gate:
- critical issues всегда валят сборку;
- cosmetic issues НЕ исключаются из подсчёта;
- baseline используется только для отчёта:
  какие cosmetic уже известны, а какие новые;
- сборка проходит, пока ОБЩЕЕ количество cosmetic
  не превышает порог.
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
_MARKETPLACE_RE = re.compile(r"(?iu)\bмаркетплейс")
_TECH_BODY_LEAK_RE = re.compile(
    r"(?iu)(?:<br>|^|\s)(?:Характеристики)(?:<br>|\s|:|…|\.\.\.)"
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
                issues.append(QualityIssue("critical", "compat_label_leak", oid, name, compat[:200]))

            families = {x.casefold() for x in _XEROX_FAMILY_RE.findall(compat)}
            if len(families) >= 3 and len(compat) >= 180:
                issues.append(QualityIssue("cosmetic", "heavy_xerox_compat", oid, name, compat[:200]))

        for key in params:
            if _BAD_POWER_KEY_RE.match(key):
                issues.append(QualityIssue("critical", "bad_power_key", oid, name, key))
            if _MARKETPLACE_RE.search(key):
                issues.append(QualityIssue("critical", "marketplace_param_leak", oid, name, key))

        if "oaicite" in desc_html or "contentReference" in desc_html:
            issues.append(QualityIssue("critical", "desc_oaicite_leak", oid, name, "oaicite/contentReference"))

        if _MARKETPLACE_RE.search(unescape(desc_html)):
            issues.append(QualityIssue("critical", "marketplace_text_in_description", oid, name, "marketplace text in final description"))

        if _TECH_BODY_LEAK_RE.search(unescape(desc_html)):
            issues.append(QualityIssue("cosmetic", "tech_block_leak_in_body", oid, name, "В обычный body протёк блок 'Характеристики'"))

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

    payload = {"schema_version": 1, "accepted_cosmetic": {}}
    for rule in sorted(grouped):
        payload["accepted_cosmetic"][rule] = sorted(set(grouped[rule]))
    return payload


def run_quality_gate(
    feed_path: str,
    *,
    baseline_path: str,
    report_path: str,
    max_new_cosmetic_offers: int = 5,
    max_new_cosmetic_issues: int = 5,
    freeze_current_as_baseline: bool = False,
) -> tuple[bool, str]:
    issues = _detect_issues(feed_path)

    critical = [x for x in issues if x.severity == "critical"]
    cosmetic = [x for x in issues if x.severity == "cosmetic"]

    accepted_cosmetic = _load_cosmetic_baseline(baseline_path)

    known_cosmetic: list[QualityIssue] = []
    new_cosmetic: list[QualityIssue] = []

    for issue in cosmetic:
        accepted = accepted_cosmetic.get(issue.rule, set())
        if issue.oid in accepted:
            known_cosmetic.append(issue)
        else:
            new_cosmetic.append(issue)

    if freeze_current_as_baseline:
        _write_yaml(baseline_path, _make_baseline_payload(cosmetic))
        accepted_cosmetic = _load_cosmetic_baseline(baseline_path)
        known_cosmetic = list(cosmetic)
        new_cosmetic = []

    cosmetic_offer_count = len({x.oid for x in cosmetic})
    passed = (
        not critical
        and len(cosmetic) <= int(max_new_cosmetic_issues)
        and cosmetic_offer_count <= int(max_new_cosmetic_offers)
    )

    write_quality_gate_report(
        report_path,
        supplier="alstyle",
        passed=passed,
        enforce=True,
        baseline_file=baseline_path,
        freeze_current_as_baseline=freeze_current_as_baseline,
        critical=critical,
        cosmetic=cosmetic,
        known_cosmetic=known_cosmetic,
        new_cosmetic=new_cosmetic,
        max_cosmetic_offers=int(max_new_cosmetic_offers),
        max_cosmetic_issues=int(max_new_cosmetic_issues),
    )

    return passed, report_path
