# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/quality_gate.py

CopyLine quality gate:
- critical issues всегда валят сборку;
- cosmetic issues считаются по offer_count и issue_count;
- baseline используется только для отчёта;
- gate проверяет прежде всего raw, потому что raw_must_be_clean=true.
- v10: добавлены финальные CopyLine-правила под multi-code и broken compat normalization.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from html import unescape
from pathlib import Path
import re
import xml.etree.ElementTree as ET

import yaml

_WS_RE = re.compile(r"\s+")
_DESC_HEADER_RE = re.compile(r"(?iu)^\s*(?:Технические\s+характеристики|Характеристики|Основные\s+характеристики)\s*:?")
_COMPAT_FAMILY_RE = re.compile(r"(?iu)\b(?:LaserJet|Color\s+LaserJet|WorkForce|SureColor|EcoTank|Kyocera|Brother|Pantum|Xerox)\b")
_PLACEHOLDER_RE = re.compile(r"(?i)placehold\.co/800x800/png\?text=No\+Photo")

_TITLE_CODE_RX = re.compile(r"""(?ix)
    \b(?:
        CF\d{3,4}[A-Z]?|CE\d{3,4}[A-Z]?|CB\d{3,4}[A-Z]?|CC\d{3,4}[A-Z]?|Q\d{4}[A-Z]?|W\d{4}[A-Z0-9]{1,4}|
        106R\d{5}|006R\d{5}|108R\d{5}|113R\d{5}|013R\d{5}|016\d{6}|
        TK-?\d{3,5}[A-Z0-9]*|MLT-[A-Z]\d{3,5}[A-Z0-9/]*|CLT-[A-Z]\d{3,5}[A-Z]?|
        ML-D\d+[A-Z]?|ML-\d{4,5}[A-Z]\d?|T-\d{3,6}[A-Z]?|KX-FA\d+[A-Z]?|KX-FAT\d+[A-Z]?|
        C-?EXV\d+[A-Z]*|DR-\d+[A-Z0-9-]*|TN-\d+[A-Z0-9-]*|
        C13T\d{5,8}[A-Z0-9]*|C12C\d{5,8}[A-Z0-9]*|C33S\d{5,8}[A-Z0-9]*|
        50F\d[0-9A-Z]{2,4}|55B\d[0-9A-Z]{2,4}|56F\d[0-9A-Z]{2,4}|0?71H
    )\b
""")
_MULTI_CANON_TAIL_RX = re.compile(r"(?i)\bCanon\s+\d{3,4}[A-Z]?(?:\s*/\s*\d{3,4}[A-Z]?)*\b")
_BROKEN_COMPAT_RX = re.compile(r"(?i)\b([A-Za-z]+)\s+\1\b")


def _title_codes(name: str) -> list[str]:
    text = _norm_ws(name)
    if not text:
        return []
    out = []
    seen = set()
    for m in _TITLE_CODE_RX.finditer(text):
        token = _norm_ws(m.group(0)).upper().replace(' ', '')
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    for m in _MULTI_CANON_TAIL_RX.finditer(text):
        parts = re.split(r"\s*/\s*", re.sub(r"(?i)^canon\s+", "", _norm_ws(m.group(0))))
        for part in parts:
            token = f"Canon {_norm_ws(part)}"
            if token and token not in seen:
                seen.add(token)
                out.append(token)
    return out



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
    p.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _offer_params(offer_el: ET.Element) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    for p in offer_el.findall("param"):
        k = _norm_ws(p.get("name") or "")
        v = _norm_ws(p.text or "")
        if k and v:
            out[k].append(v)
    return out


def _param_first(params: dict[str, list[str]], key: str) -> str:
    vals = params.get(key) or []
    return vals[0] if vals else ""


def _offer_sig(issue: QualityIssue) -> str:
    return f"{issue.rule}|{issue.oid}|{issue.details}"


def collect_quality_issues(feed_path: str) -> list[QualityIssue]:
    xml_text = Path(feed_path).read_text(encoding="utf-8", errors="ignore")
    root = ET.fromstring(xml_text)
    issues: list[QualityIssue] = []

    for offer in root.findall(".//offer"):
        oid = _norm_ws(offer.get("id") or "")
        name = _norm_ws((offer.findtext("name") or ""))
        price = _norm_ws((offer.findtext("price") or ""))
        pic = _norm_ws((offer.findtext("picture") or ""))
        vendor = _norm_ws((offer.findtext("vendor") or ""))
        desc = _norm_ws((offer.findtext("description") or ""))
        params = _offer_params(offer)
        typ = _param_first(params, "Тип")
        model = _param_first(params, "Модель")
        compat = _param_first(params, "Совместимость")
        codes = _param_first(params, "Коды расходников")

        if not oid or not name:
            issues.append(QualityIssue("critical", "missing_identity", oid or "?", name or "?", "offer without id/name"))
            continue

        if not price or not re.fullmatch(r"\d+(?:\.\d+)?", price) or float(price) <= 0:
            issues.append(QualityIssue("critical", "invalid_price", oid, name, f"price={price!r}"))

        if not pic:
            issues.append(QualityIssue("critical", "missing_picture", oid, name, "picture missing"))
        elif _PLACEHOLDER_RE.search(pic):
            issues.append(QualityIssue("cosmetic", "placeholder_picture_only", oid, name, pic))

        if not desc:
            issues.append(QualityIssue("cosmetic", "empty_description", oid, name, "description empty"))
        elif _DESC_HEADER_RE.search(desc):
            issues.append(QualityIssue("cosmetic", "desc_header_leak", oid, name, desc[:120]))

        if not vendor:
            issues.append(QualityIssue("cosmetic", "empty_vendor", oid, name, "vendor empty"))

        if model and name and _norm_ws(model).casefold() == _norm_ws(name).casefold():
            issues.append(QualityIssue("cosmetic", "model_equals_name", oid, name, model[:120]))

        is_consumable = bool(re.match(r"(?iu)^(?:Картридж|Тонер-картридж|Драм-картридж|Drum|Чернила|Девелопер)", name) or typ in {"Картридж", "Тонер-картридж", "Драм-картридж", "Чернила", "Девелопер"})
        if is_consumable:
            title_codes = _title_codes(name)
            if not compat and not _COMPAT_FAMILY_RE.search(desc):
                issues.append(QualityIssue("cosmetic", "missing_compat", oid, name, "compat missing"))
            if not codes:
                issues.append(QualityIssue("cosmetic", "missing_codes", oid, name, "codes missing"))
            if title_codes and not codes:
                issues.append(QualityIssue("cosmetic", "code_present_in_title_but_missing_in_params", oid, name, ", ".join(title_codes[:6])))
            if _MULTI_CANON_TAIL_RX.search(name):
                canon_title = [x for x in title_codes if x.lower().startswith("canon ")]
                canon_codes = [x.strip() for x in re.split(r",\s*", codes) if x.strip().lower().startswith("canon ")]
                if canon_title and len(canon_codes) < len(canon_title):
                    issues.append(QualityIssue("cosmetic", "multi_code_parsing_incomplete", oid, name, f"title={', '.join(canon_title)} | params={codes or '-'}"))
            if compat and _BROKEN_COMPAT_RX.search(compat):
                issues.append(QualityIssue("cosmetic", "compat_normalization_broken", oid, name, compat[:160]))

    return issues


def run_quality_gate(*, feed_path: str, policy_path: str, baseline_path: str | None = None, report_path: str | None = None) -> dict:
    policy = _read_yaml(policy_path)
    qcfg = (policy.get("quality_gate") or {}) if isinstance(policy, dict) else {}
    enforce = bool(qcfg.get("enforce", True))
    max_cosmetic_offers = int(qcfg.get("max_cosmetic_offers", 5) or 5)
    max_cosmetic_issues = int(qcfg.get("max_cosmetic_issues", 5) or 5)
    if not baseline_path:
        baseline_path = qcfg.get("baseline_path") or ""
    if not report_path:
        report_path = qcfg.get("report_path") or ""

    issues = collect_quality_issues(feed_path)
    critical = [x for x in issues if x.severity == "critical"]
    cosmetic = [x for x in issues if x.severity == "cosmetic"]

    baseline = _read_yaml(baseline_path) if baseline_path else {}
    known = set((baseline.get("known_cosmetic") or [])) if isinstance(baseline, dict) else set()
    current_cosmetic = [_offer_sig(x) for x in cosmetic]
    new_cosmetic = [sig for sig in current_cosmetic if sig not in known]

    cosmetic_offer_count = len({x.oid for x in cosmetic})
    cosmetic_issue_count = len(cosmetic)
    critical_count = len(critical)

    passed = True
    if critical_count > 0:
        passed = False
    if cosmetic_offer_count > max_cosmetic_offers or cosmetic_issue_count > max_cosmetic_issues:
        passed = False
    if not enforce:
        passed = True

    if report_path:
        p = Path(report_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        lines.append(f"QUALITY_GATE: {'PASS' if passed else 'FAIL'}")
        lines.append(f"critical_count: {critical_count}")
        lines.append(f"cosmetic_total_count: {cosmetic_issue_count}")
        lines.append(f"cosmetic_offer_count: {cosmetic_offer_count}")
        lines.append(f"max_cosmetic_offers: {max_cosmetic_offers}")
        lines.append(f"max_cosmetic_issues: {max_cosmetic_issues}")
        lines.append("")
        if critical:
            lines.append("CRITICAL:")
            for x in critical:
                lines.append(f"- {x.oid} | {x.rule} | {x.details}")
            lines.append("")
        if cosmetic:
            lines.append("COSMETIC:")
            for x in cosmetic:
                lines.append(f"- {x.oid} | {x.rule} | {x.details}")
            lines.append("")
        lines.append(f"known_cosmetic: {len(known)}")
        lines.append(f"new_cosmetic: {len(new_cosmetic)}")
        p.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    return {
        "ok": passed,
        "critical_count": critical_count,
        "cosmetic_total_count": cosmetic_issue_count,
        "cosmetic_offer_count": cosmetic_offer_count,
        "new_cosmetic_count": len(new_cosmetic),
        "max_cosmetic_offers": max_cosmetic_offers,
        "max_cosmetic_issues": max_cosmetic_issues,
        "enforce": enforce,
        "report_path": report_path or "",
    }
