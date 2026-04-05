# -*- coding: utf-8 -*-
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
try:
    import yaml
except Exception:
    yaml = None

PLACEHOLDER_URL = "https://placehold.co/800x800/png?text=No+Photo"
QUALITY_BASELINE_DEFAULT = "scripts/suppliers/vtt/config/quality_gate_baseline.yml"
QUALITY_REPORT_DEFAULT = "docs/raw/vtt_quality_gate.txt"
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

@dataclass(frozen=True)
class QualityGateResult:
    ok: bool
    report_path: str
    critical_count: int
    cosmetic_count: int

def _now_almaty_str() -> str:
    return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S")

def _norm_ws(s: str) -> str:
    s2 = unescape(s or "")
    s2 = s2.replace("\u00a0", " ").strip()
    return _WS_RE.sub(" ", s2).strip()

def _read_yaml(path: str | None) -> dict:
    if not path:
        return {}
    p = Path(path)
    if not p.exists() or yaml is None:
        return {}
    try:
        return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}

def _write_yaml(path: str | None, data: dict) -> None:
    if not path or yaml is None:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")

def _offer_params(offer_el: ET.Element) -> dict[str, list[str]]:
    out = defaultdict(list)
    for p in offer_el.findall("param"):
        k = _norm_ws(p.get("name") or "")
        v = _norm_ws("".join(p.itertext()))
        if k and v:
            out[k].append(v)
    return dict(out)

def _make_issue(severity: str, rule: str, oid: str, name: str, details: str) -> QualityIssue:
    return QualityIssue(severity=severity, rule=rule, oid=_norm_ws(oid), name=_norm_ws(name), details=_norm_ws(details))

def _detect_issues(feed_path: str):
    xml_path = Path(feed_path)
    xml_text = xml_path.read_text(encoding="utf-8", errors="ignore")
    root = ET.fromstring(xml_text)
    issues = []
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
    deduped = {}
    for issue in issues:
        deduped[(issue.severity, issue.rule, issue.oid, issue.details)] = issue
    return sorted(deduped.values(), key=lambda x: (x.severity, x.rule, x.oid, x.details)), offer_count, len(xml_text.encode("utf-8"))

def _load_cosmetic_baseline(baseline_path: str | None):
    data = _read_yaml(baseline_path)
    raw = data.get("accepted_cosmetic") or {}
    return {str(rule): {str(x).strip() for x in (oids or []) if str(x).strip()} for rule, oids in raw.items()}

def _make_baseline_payload(cosmetic):
    grouped = defaultdict(list)
    for issue in cosmetic:
        grouped[issue.rule].append(issue.oid)
    payload = {"schema_version": 1, "accepted_cosmetic": {}}
    for rule in sorted(grouped):
        payload["accepted_cosmetic"][rule] = sorted(set(grouped[rule]))
    return payload

def _preview_lines(items, limit=50):
    out = []
    for issue in items[:limit]:
        out.append(f"  - {issue.oid} | {issue.rule}" + (f" | {issue.details}" if issue.details else ""))
    return out

def _rule_count_lines(items):
    counts = Counter(x.rule for x in items)
    return [f"  - {rule}: {counts[rule]}" for rule in sorted(counts)]

def _section(lines, title, items):
    lines.append("")
    lines.append(title + ":")
    if items:
        lines.extend(_preview_lines(items))
    else:
        lines.append("# Ошибок в этой секции нет")

def _write_report(path: str, *, feed_path: str, offer_count: int, feed_size_bytes: int, critical, cosmetic, known_cosmetic, new_cosmetic, enforced_new_cosmetic, baseline_path: str, freeze_current_as_baseline: bool, enforce: bool, max_cosmetic_offers: int, max_cosmetic_issues: int):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    sha12 = sha1(Path(feed_path).read_bytes()).hexdigest()[:12]
    cosmetic_offer_count = len({x.oid for x in cosmetic})
    known_offer_count = len({x.oid for x in known_cosmetic})
    new_offer_count = len({x.oid for x in new_cosmetic})
    passed = len(critical) == 0 and len({x.oid for x in enforced_new_cosmetic}) <= max_cosmetic_offers and len(enforced_new_cosmetic) <= max_cosmetic_issues
    lines = [
        "# Итог проверки quality gate",
        f"QUALITY_GATE: {'PASS' if passed else 'FAIL'}",
        "# PASS = можно выпускать | FAIL = есть блокирующие проблемы",
        f"enforce: {'true' if enforce else 'false'}",
        "# true = quality gate реально валит сборку",
        f"report_file: {path}",
        "# Куда записан этот отчёт",
        f"baseline_file: {baseline_path}",
        "# Базовый файл для сравнения известных cosmetic-проблем",
        f"freeze_current_as_baseline: {'yes' if freeze_current_as_baseline else 'no'}",
        "# yes = текущие cosmetic-хвосты сохранены как baseline-снимок",
        f"critical_count: {len(critical)}",
        "# Сколько найдено критичных проблем",
        f"cosmetic_total_count: {len(cosmetic)}",
        "# Общее число некритичных проблем",
        f"cosmetic_offer_count: {cosmetic_offer_count}",
        "# В скольких товарах есть cosmetic-проблемы",
        f"known_cosmetic_count: {len(known_cosmetic)}",
        "# Сколько cosmetic-проблем уже известны по baseline",
        f"known_cosmetic_offer_count: {known_offer_count}",
        "# В скольких товарах есть уже известные cosmetic-проблемы",
        f"new_cosmetic_count: {len(new_cosmetic)}",
        "# Сколько найдено новых cosmetic-проблем",
        f"new_cosmetic_offer_count: {new_offer_count}",
        "# В скольких товарах появились новые cosmetic-проблемы",
        f"max_cosmetic_offers: {int(max_cosmetic_offers)}",
        "# Допустимый максимум товаров с cosmetic-проблемами",
        f"max_cosmetic_issues: {int(max_cosmetic_issues)}",
        "# Допустимый максимум cosmetic-проблем всего",
    ]
    _section(lines, "CRITICAL", critical)
    _section(lines, "COSMETIC TOTAL", cosmetic)
    _section(lines, "NEW COSMETIC", new_cosmetic)
    _section(lines, "KNOWN COSMETIC", known_cosmetic)
    lines += ["", "DEBUG VTT:", f"generated_at_almaty: {_now_almaty_str()}", f"feed_path: {feed_path}", f"offers: {offer_count}", f"feed_size_bytes: {feed_size_bytes}", f"feed_sha1_12: {sha12}", "excluded_from_enforce_rules:"]
    for rule in sorted(_RULES_EXCLUDED_FROM_ENFORCE):
        lines.append(f"  - {rule}: {sum(1 for x in cosmetic if x.rule == rule)}")
    lines += [f"enforced_cosmetic_count: {len(enforced_new_cosmetic)}", f"enforced_cosmetic_offer_count: {len({x.oid for x in enforced_new_cosmetic})}", "", "COSMETIC TOTAL BY RULE:"]
    lines.extend(_rule_count_lines(cosmetic) if cosmetic else ["# Ошибок в этой секции нет"])
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")

def run_quality_gate(*, feed_path: str, report_path: str | None = None, baseline_path: str | None = None, max_new_cosmetic_offers: int = 5, max_new_cosmetic_issues: int = 5, enforce: bool = True, freeze_current_as_baseline: bool = False) -> QualityGateResult:
    report_path = str(report_path or QUALITY_REPORT_DEFAULT)
    baseline_path = str(baseline_path or QUALITY_BASELINE_DEFAULT)
    issues, offer_count, feed_size_bytes = _detect_issues(feed_path)
    critical = [x for x in issues if x.severity == "critical"]
    cosmetic = [x for x in issues if x.severity == "cosmetic"]
    if freeze_current_as_baseline:
        _write_yaml(baseline_path, _make_baseline_payload(cosmetic))
    baseline = _load_cosmetic_baseline(baseline_path)
    known_cosmetic, new_cosmetic = [], []
    for issue in cosmetic:
        (known_cosmetic if issue.oid in baseline.get(issue.rule, set()) else new_cosmetic).append(issue)
    enforced_new_cosmetic = [x for x in new_cosmetic if x.rule not in _RULES_EXCLUDED_FROM_ENFORCE]
    passed = len(critical) == 0 and len({x.oid for x in enforced_new_cosmetic}) <= int(max_new_cosmetic_offers) and len(enforced_new_cosmetic) <= int(max_new_cosmetic_issues)
    _write_report(report_path, feed_path=feed_path, offer_count=offer_count, feed_size_bytes=feed_size_bytes, critical=critical, cosmetic=cosmetic, known_cosmetic=known_cosmetic, new_cosmetic=new_cosmetic, enforced_new_cosmetic=enforced_new_cosmetic, baseline_path=baseline_path, freeze_current_as_baseline=freeze_current_as_baseline, enforce=enforce, max_cosmetic_offers=int(max_new_cosmetic_offers), max_cosmetic_issues=int(max_new_cosmetic_issues))
    ok = True if not enforce else passed
    return QualityGateResult(ok=ok, report_path=report_path, critical_count=len(critical), cosmetic_count=len(cosmetic))
