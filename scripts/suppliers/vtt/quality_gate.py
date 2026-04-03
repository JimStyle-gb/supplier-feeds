# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/quality_gate.py

VTT quality gate.

Что делает:
- пишет supplier-side отчёт по raw feed;
- остаётся backward-safe для текущего build_vtt.py;
- уже принимает будущие template-аргументы (enforce/baseline/max_cosmetic_*),
  даже если пока не использует baseline как у frozen suppliers;
- ловит не только пустой feed, но и базовые витринные/raw-хвосты VTT.

Важно:
- quality gate проверяет supplier raw, а не лечит его;
- cosmetic пока не валят сборку сами по себе;
- ok=False только если есть critical issues.
"""

from __future__ import annotations

import hashlib
import re
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


_PLACEHOLDER = "https://placehold.co/800x800/png?text=No+Photo"
_DECIMAL_K_RE = re.compile(r"\b\d+[\.,]\d+\s*[KК]\b", re.I)
_FOR_TITLE_RE = re.compile(r"(?iu)\bдля\b")
_MULTI_WS_RE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class GateIssue:
    oid: str
    rule: str
    details: str = ""


@dataclass(frozen=True, slots=True)
class QualityGateResult:
    ok: bool
    report_path: str
    critical_count: int
    cosmetic_count: int


def _now_almaty_str() -> str:
    almaty = timezone(timedelta(hours=5))
    return datetime.now(almaty).strftime("%Y-%m-%d %H:%M:%S")


def _norm_ws(value: str) -> str:
    return _MULTI_WS_RE.sub(" ", str(value or "")).strip()


def _text(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return _norm_ws("".join(el.itertext()))


def _iter_offers(root: ET.Element) -> list[ET.Element]:
    return list(root.findall(".//offer"))


def _first_picture(offer_el: ET.Element) -> str:
    pic = offer_el.find("picture")
    return _text(pic)


def _param_map(offer_el: ET.Element) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in offer_el.findall("param"):
        name = _norm_ws(p.attrib.get("name", ""))
        value = _text(p)
        if name and value and name not in out:
            out[name] = value
    return out


def _short(details: str, limit: int = 180) -> str:
    s = _norm_ws(details)
    if len(s) <= limit:
        return s
    return s[: limit - 3] + "..."


def _issue_line(issue: GateIssue) -> str:
    parts = [issue.oid or "-", issue.rule]
    if issue.details:
        parts.append(_short(issue.details))
    return " | ".join(parts)


def _append_issue(bucket: list[GateIssue], oid: str, rule: str, details: str = "") -> None:
    bucket.append(GateIssue(oid=oid or "", rule=rule, details=details or ""))


def run_quality_gate(
    *,
    feed_path: str,
    report_path: str,
    enforce: bool | None = None,
    baseline_path: str | None = None,
    max_cosmetic_offers: int | None = None,
    max_cosmetic_issues: int | None = None,
    freeze_current_as_baseline: bool = False,
) -> QualityGateResult:
    """Проверить VTT raw feed и записать supplier-side отчёт.

    Backward-safe:
    - текущий build_vtt.py вызывает только feed_path/report_path;
    - template build может позже передавать enforce/baseline/max_cosmetic_*.
    """
    _ = enforce
    _ = baseline_path
    _ = max_cosmetic_offers
    _ = max_cosmetic_issues
    _ = freeze_current_as_baseline

    p = Path(feed_path)
    report = Path(report_path)

    critical: list[GateIssue] = []
    cosmetic: list[GateIssue] = []
    offers_count = 0
    feed_size_bytes = 0
    feed_sha1 = ""

    if not p.exists():
        _append_issue(critical, "", "feed_missing", str(p))
        text = ""
        root = None
    else:
        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:  # pragma: no cover
            text = ""
            _append_issue(critical, "", "feed_read_error", str(exc))
            root = None
        else:
            feed_size_bytes = p.stat().st_size
            feed_sha1 = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:12]
            try:
                root = ET.fromstring(text)
            except Exception as exc:
                root = None
                _append_issue(critical, "", "xml_parse_error", str(exc))

    if root is not None:
        offers = _iter_offers(root)
        offers_count = len(offers)
        if offers_count <= 0:
            _append_issue(critical, "", "offers_zero")

        for offer_el in offers:
            oid = _norm_ws(offer_el.attrib.get("id", ""))
            vendor_code = _text(offer_el.find("vendorCode"))
            name = _text(offer_el.find("name"))
            price = _text(offer_el.find("price"))
            vendor = _text(offer_el.find("vendor"))
            picture = _first_picture(offer_el)
            description = _text(offer_el.find("description"))
            params = _param_map(offer_el)

            if not oid:
                _append_issue(critical, oid, "empty_offer_id")
            if not vendor_code:
                _append_issue(critical, oid, "empty_vendorcode")
            if not name:
                _append_issue(critical, oid, "empty_name")
            if not price:
                _append_issue(critical, oid, "empty_price")
            else:
                try:
                    if int(str(price).replace(" ", "")) <= 0:
                        _append_issue(critical, oid, "nonpositive_price", price)
                except Exception:
                    _append_issue(critical, oid, "invalid_price", price)
            if not vendor:
                _append_issue(critical, oid, "empty_vendor")
            if not picture:
                _append_issue(critical, oid, "missing_picture")
            elif _PLACEHOLDER in picture:
                _append_issue(cosmetic, oid, "placeholder_picture", picture)

            resource = params.get("Ресурс") or ""
            if resource and _DECIMAL_K_RE.search(resource):
                _append_issue(cosmetic, oid, "decimal_k_resource", resource)

            compat = params.get("Совместимость") or ""
            if _FOR_TITLE_RE.search(name) and not compat:
                _append_issue(cosmetic, oid, "compat_missing_for_title", name)

            if not description:
                _append_issue(cosmetic, oid, "empty_native_desc")

    critical_by_rule = Counter(x.rule for x in critical)
    cosmetic_by_rule = Counter(x.rule for x in cosmetic)

    lines: list[str] = []
    lines.append("VTT quality gate")
    lines.append("=" * 72)
    lines.append(f"generated_at_almaty: {_now_almaty_str()}")
    lines.append(f"feed_path: {feed_path}")
    lines.append(f"offers: {offers_count}")
    lines.append(f"feed_size_bytes: {feed_size_bytes}")
    lines.append(f"feed_sha1_12: {feed_sha1}")
    lines.append(f"critical_count: {len(critical)}")
    lines.append(f"cosmetic_count: {len(cosmetic)}")

    if critical:
        lines.append("-" * 72)
        lines.append("critical_preview:")
        for issue in critical[:50]:
            lines.append(f"  - {_issue_line(issue)}")
        lines.append("critical_by_rule:")
        for rule, cnt in sorted(critical_by_rule.items()):
            lines.append(f"  - {rule}: {cnt}")

    if cosmetic:
        lines.append("-" * 72)
        lines.append("cosmetic_preview:")
        for issue in cosmetic[:50]:
            lines.append(f"  - {_issue_line(issue)}")
        lines.append("cosmetic_by_rule:")
        for rule, cnt in sorted(cosmetic_by_rule.items()):
            lines.append(f"  - {rule}: {cnt}")

    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    return QualityGateResult(
        ok=(len(critical) == 0),
        report_path=str(report),
        critical_count=len(critical),
        cosmetic_count=len(cosmetic),
    )
