# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/quality_gate.py

VTT quality gate.

Patch focus:
- сохранить строгий контроль по реальным ошибкам VTT;
- НЕ валить сборку на supplier-specific image tail
  (`placeholder_picture`), потому что для VTT это допустимая особенность;
- всё остальное продолжать проверять строго.

Правило:
- critical всегда валят сборку;
- cosmetic `placeholder_picture` попадает в отчёт,
  но ИСКЛЮЧАЕТСЯ из enforce-лимитов;
- остальные cosmetic продолжают считаться в enforce-лимитах.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha1
from html import unescape
from pathlib import Path
from zoneinfo import ZoneInfo
import re
import xml.etree.ElementTree as ET

import yaml


PLACEHOLDER_URL = "https://placehold.co/800x800/png?text=No+Photo"
_DECIMAL_K_RE = re.compile(r"^\d+(?:[.,]\d+)+K$", re.I)
_WS_RE = re.compile(r"\s+")
_RULES_EXCLUDED_FROM_ENFORCE = {"placeholder_picture"}


@dataclass(frozen=True)
class QualityIssue:
    severity: str
    rule: str
    oid: str
    name: str
    details: str


def _now_almaty_str() -> str:
    return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S")


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


def _make_issue(severity: str, rule: str, oid: str, name: str, details: str) -> QualityIssue:
    return QualityIssue(
        severity=severity,
        rule=rule,
        oid=_norm_ws(oid),
        name=_norm_ws(name),
        details=_norm_ws(details),
    )


def _detect_issues(feed_path: str) -> tuple[list[QualityIssue], int, int]:
    xml_path = Path(feed_path)
    xml_text = xml_path.read_text(encoding="utf-8", errors="ignore")
    root = ET.fromstring(xml_text)

    issues: list[QualityIssue] = []
    offers = root.findall(".//offer")
    offer_count = len(offers)

    for offer in offers:
        oid = _norm_ws(offer.get("id") or "")
        name = _norm_ws(offer.findtext("name") or "")
        vendor = _norm_ws(offer.findtext("vendor") or "")
        desc_html = offer.findtext("description") or ""
        params = _offer_params(offer)

        if not vendor:
            issues.append(_make_issue("critical", "empty_vendor", oid, name, ""))

        for pic in offer.findall("picture"):
            url = _norm_ws("".join(pic.itertext()))
            if url == PLACEHOLDER_URL:
                issues.append(_make_issue("cosmetic", "placeholder_picture", oid, name, url))

        for resource in params.get("Ресурс", []):
            if _DECIMAL_K_RE.match(resource):
                issues.append(_make_issue("cosmetic", "decimal_k_resource", oid, name, resource))

        if "oaicite" in desc_html or "contentReference" in desc_html:
            issues.append(_make_issue("critical", "desc_oaicite_leak", oid, name, "oaicite/contentReference"))

    deduped: dict[tuple[str, str, str, str], QualityIssue] = {}
    for issue in issues:
        deduped[(issue.severity, issue.rule, issue.oid, issue.details)] = issue

    return sorted(deduped.values(), key=lambda x: (x.severity, x.rule, x.oid, x.details)), offer_count, len(xml_text.encode("utf-8"))


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


def _preview_lines(items: list[QualityIssue], limit: int = 50) -> list[str]:
    out: list[str] = []
    for issue in items[:limit]:
        if issue.details:
            out.append(f"  - {issue.oid} | {issue.rule} | {issue.details}")
        else:
            out.append(f"  - {issue.oid} | {issue.rule}")
    return out


def _rule_count_lines(items: list[QualityIssue]) -> list[str]:
    counts = Counter(x.rule for x in items)
    return [f"  - {rule}: {counts[rule]}" for rule in sorted(counts)]


def _write_report(
    path: str,
    *,
    feed_path: str,
    offer_count: int,
    feed_size_bytes: int,
    critical: list[QualityIssue],
    cosmetic: list[QualityIssue],
    enforced_cosmetic: list[QualityIssue],
) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    sha12 = sha1(Path(feed_path).read_bytes()).hexdigest()[:12]

    lines: list[str] = []
    lines.append("VTT quality gate")
    lines.append("========================================================================")
    lines.append(f"generated_at_almaty: {_now_almaty_str()}")
    lines.append(f"feed_path: {feed_path}")
    lines.append(f"offers: {offer_count}")
    lines.append(f"feed_size_bytes: {feed_size_bytes}")
    lines.append(f"feed_sha1_12: {sha12}")
    lines.append(f"critical_count: {len(critical)}")
    lines.append(f"cosmetic_count: {len(cosmetic)}")
    lines.append("------------------------------------------------------------------------")

    if critical:
        lines.append("critical_preview:")
        lines.extend(_preview_lines(critical))
        lines.append("critical_by_rule:")
        lines.extend(_rule_count_lines(critical))
        lines.append("------------------------------------------------------------------------")

    if cosmetic:
        lines.append("cosmetic_preview:")
        lines.extend(_preview_lines(cosmetic))
        lines.append("cosmetic_by_rule:")
        lines.extend(_rule_count_lines(cosmetic))

    if _RULES_EXCLUDED_FROM_ENFORCE:
        lines.append("------------------------------------------------------------------------")
        lines.append("excluded_from_enforce_rules:")
        for rule in sorted(_RULES_EXCLUDED_FROM_ENFORCE):
            count = sum(1 for x in cosmetic if x.rule == rule)
            lines.append(f"  - {rule}: {count}")

    lines.append("------------------------------------------------------------------------")
    lines.append(f"enforced_cosmetic_count: {len(enforced_cosmetic)}")
    lines.append(f"enforced_cosmetic_offer_count: {len({x.oid for x in enforced_cosmetic})}")

    p.write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    issues, offer_count, feed_size_bytes = _detect_issues(feed_path)
    critical = [x for x in issues if x.severity == "critical"]
    cosmetic = [x for x in issues if x.severity == "cosmetic"]

    if freeze_current_as_baseline:
        payload = _make_baseline_payload(cosmetic)
        _write_yaml(baseline_path, payload)

    _ = _load_cosmetic_baseline(baseline_path)  # backward-safe read, report-only legacy compatibility

    enforced_cosmetic = [x for x in cosmetic if x.rule not in _RULES_EXCLUDED_FROM_ENFORCE]
    enforced_offer_count = len({x.oid for x in enforced_cosmetic})
    enforced_issue_count = len(enforced_cosmetic)

    passed = (
        len(critical) == 0
        and enforced_offer_count <= int(max_new_cosmetic_offers)
        and enforced_issue_count <= int(max_new_cosmetic_issues)
    )

    _write_report(
        report_path,
        feed_path=feed_path,
        offer_count=offer_count,
        feed_size_bytes=feed_size_bytes,
        critical=critical,
        cosmetic=cosmetic,
        enforced_cosmetic=enforced_cosmetic,
    )

    summary = (
        f"[quality_gate] {'PASS' if (passed or not enforce) else 'FAIL'} | "
        f"critical={len(critical)} | "
        f"cosmetic_total={len(cosmetic)} | "
        f"enforced_cosmetic={enforced_issue_count} | "
        f"enforced_cosmetic_offers={enforced_offer_count} | "
        f"excluded_rules={','.join(sorted(_RULES_EXCLUDED_FROM_ENFORCE)) or '-'} | "
        f"report={report_path}"
    )

    if not enforce:
        return True, summary + " | enforce=no"

    return passed, summary
