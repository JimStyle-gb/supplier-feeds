# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/quality_gate.py

Final quality gate for CopyLine freeze-finalize stage.
Проверяет raw-feed и ловит уже финальные остаточные классы:
- неполный mixed-brand / multi-code tail;
- код есть в title, но не поднялся в params;
- сломанная нормализация compat;
- пустой vendor при очевидном бренде;
- неожиданные cable-поля у не-кабелей.
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
_COMPAT_FAMILY_RE = re.compile(r"(?iu)\b(?:LaserJet|Color\s+LaserJet|WorkForce|SureColor|EcoTank|Kyocera|Brother|Pantum|Xerox|Canon|Samsung|Toshiba|Ricoh|Panasonic|Konica-Minolta)\b")
_PLACEHOLDER_RE = re.compile(r"(?i)placehold\.co/800x800/png\?text=No\+Photo")
_CONSUMABLE_NAME_RE = re.compile(r"(?iu)^(?:Картридж|Тонер-картридж|Драм-картридж|Drum|Чернила|Девелопер)")
_CABLE_KEYS = {"Тип кабеля", "Количество пар", "Толщина проводников", "Категория", "Материал изоляции", "Бухта"}
_BRAND_PREFIX_RE = re.compile(r"(?iu)^(?:HP|HEWLETT\s*PACKARD|CANON|XEROX|SAMSUNG|TOSHIBA|RICOH|PANASONIC|KONICA-?MINOLTA|BROTHER|KYOCERA)\s+")

_TITLE_CODE_RX = re.compile(
    r"\b(?:"
    r"CF\d{3,4}[A-Z]?|CE\d{3,4}[A-Z]?|CB\d{3,4}[A-Z]?|CC\d{3,4}[A-Z]?|Q\d{4}[A-Z]?|W\d{4}[A-Z0-9]{1,4}|"
    r"106R\d{5}|006R\d{5}|108R\d{5}|113R\d{5}|013R\d{5}|016\d{6}|"
    r"TK-?\d{3,5}[A-Z0-9]*|MLT-[A-Z]\d{3,5}[A-Z0-9/]*|CLT-[A-Z]\d{3,5}[A-Z]?|"
    r"ML-D\d+[A-Z]?|ML-\d{4,5}[A-Z]\d?|T-\d{3,6}[A-Z]?|KX-FA\d+[A-Z]?|KX-FAT\d+[A-Z]?|"
    r"C-?EXV\d+[A-Z]*|GPR-\d+[A-Z]*|NPG-\d+[A-Z]*|FX-10|EP-27|E-30|PC-?\d+[A-Z0-9-]*|TL-?\d+[A-Z0-9-]*|DL-?\d+[A-Z0-9-]*|"
    r"DR-\d+[A-Z0-9-]*|TN-\d+[A-Z0-9-]*|"
    r"C13T\d{5,8}[A-Z0-9]*|C13S\d{6,8}[A-Z0-9]*|C12C\d{5,8}[A-Z0-9]*|C33S\d{5,8}[A-Z0-9]*|"
    r"50F\d[0-9A-Z]{2,4}|51B[0-9A-Z]{4,5}|52D[0-9A-Z]{4,5}|55B\d[0-9A-Z]{2,4}|56F\d[0-9A-Z]{2,4}|60F[0-9A-Z]{4,5}|0?71H|737|728|725|719|712|713|T06|T08|T13|SP\d{3,5}[A-Z]{1,3}|101R\d{5}|842\d{3,6}"
    r")\b",
    re.I,
)
_CANON_NUMERIC_TAIL_RX = re.compile(r"\bCanon\s+((?:\d{2,4}[A-Z]?)(?:\s*/\s*\d{2,4}[A-Z]?){0,6})\b", re.I)
_CANON_ALPHA_TAIL_RX = re.compile(r"\bCanon\s+([A-Z]{1,8}-?[A-Z0-9]{1,12})\b", re.I)
_COMPAT_BROKEN_RX = re.compile(r"(?iu)(?:WorkCentre\s+WorkCentre|Phaser\s+Phaser|LaserJet\s+LaserJet|imageRUNNER\s+imageRUNNER|E-Studio\s+E-Studio)")


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


def _norm_code(s: str) -> str:
    s = _norm_ws(s).upper()
    s = re.sub(r"\s*[-–—]\s*", "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _brandless_code(s: str) -> str:
    return _norm_code(_BRAND_PREFIX_RE.sub("", _norm_ws(s)))


def _read_yaml(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


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


def _split_codes(value: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"\s*,\s*", _norm_ws(value)):
        if not part:
            continue
        norm = _norm_code(part)
        if norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def _extract_expected_title_codes(name: str) -> list[str]:
    text = _norm_ws(name)
    out: list[str] = []
    seen: set[str] = set()

    for m in _TITLE_CODE_RX.finditer(text):
        token = _norm_code(m.group(0))
        if re.fullmatch(r"(?:712|713|719|725|728|737)", token, re.I):
            continue
        if token not in seen:
            seen.add(token)
            out.append(token)

    for m in _CANON_NUMERIC_TAIL_RX.finditer(text):
        for part in re.split(r"\s*/\s*", _norm_ws(m.group(1))):
            token = f"CANON {part.upper()}"
            if token not in seen:
                seen.add(token)
                out.append(token)

    for m in _CANON_ALPHA_TAIL_RX.finditer(text):
        token = f"CANON {_norm_code(m.group(1))}"
        if token not in seen:
            seen.add(token)
            out.append(token)

    return out


def _obvious_brand(name: str, compat: str) -> str:
    hay = f"{_norm_ws(name)} | {_norm_ws(compat)}"
    brands = ["HP", "Canon", "Xerox", "Samsung", "Toshiba", "Ricoh", "Panasonic", "Konica-Minolta", "Brother", "Kyocera"]
    for b in brands:
        if re.search(rf"(?iu)\b{re.escape(b)}\b", hay):
            return b
    return ""


def _expected_code_is_covered(expected: str, code_list: list[str], vendor: str, name: str) -> bool:
    exp = _norm_code(expected)
    brandless = _brandless_code(exp)
    codes_exact = {_norm_code(x) for x in code_list}
    codes_brandless = {_brandless_code(x) for x in code_list}
    if exp in codes_exact or brandless in codes_brandless:
        return True

    if vendor.casefold() == "canon" and brandless and brandless in codes_brandless:
        return True

    title = _norm_ws(name)
    if "/" not in title and brandless and brandless in codes_brandless:
        return True

    return False



def _ink_can_skip_codes(name: str, typ: str, desc: str) -> bool:
    """Разрешаем не требовать codes только для generic/universal ink без явного кода."""
    if _norm_ws(typ) != "Чернила":
        return False

    hay = f"{_norm_ws(name)} | {_norm_ws(desc)}"
    # Если в title/body уже есть явный code-like token, codes всё равно обязательны.
    if _TITLE_CODE_RX.search(hay) or _CANON_NUMERIC_TAIL_RX.search(hay) or _CANON_ALPHA_TAIL_RX.search(hay):
        return False

    generic_re = re.compile(
        r"(?iu)\b(?:universal|универсал(?:ьн(?:ые|ое|ый))?|комплект|set|набор|ink\s*kit|100\s*мл|1\s*л|500\s*мл|250\s*мл|чернила\s+для)\b"
    )
    return bool(generic_re.search(hay))


def _ink_can_skip_compat(name: str, typ: str, desc: str) -> bool:
    """Разрешаем не требовать compat только для generic/universal ink без явного кода и списка устройств."""
    if _norm_ws(typ) != "Чернила":
        return False

    hay = f"{_norm_ws(name)} | {_norm_ws(desc)}"
    # Если есть явный код расходки, compat всё равно ожидаем.
    if _TITLE_CODE_RX.search(hay) or _CANON_NUMERIC_TAIL_RX.search(hay) or _CANON_ALPHA_TAIL_RX.search(hay):
        return False

    generic_re = re.compile(
        r"(?iu)\b(?:universal|универсал(?:ьн(?:ые|ое|ый))?|комплект|set|набор|ink\s*kit|100\s*мл|1\s*л|500\s*мл|250\s*мл|чернила\s+для)\b"
    )
    compat_hint_re = re.compile(
        r"(?iu)\b(?:for\s+[A-Z0-9][A-Z0-9/ -]*|для\s+(?:принтеров?|МФУ|устройств?|аппаратов?)|совместим\s+с|используется\s+в|Epson\s+L\d|EcoTank|LaserJet|WorkForce)\b"
    )
    return bool(generic_re.search(hay)) and not compat_hint_re.search(hay)

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
        code_list = _split_codes(codes)
        expected_title_codes = _extract_expected_title_codes(name)

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

        obvious_brand = _obvious_brand(name, compat)
        if not vendor:
            issues.append(QualityIssue("cosmetic", "empty_vendor", oid, name, "vendor empty"))
            if obvious_brand:
                issues.append(QualityIssue("cosmetic", "vendor_empty_but_brand_obvious", oid, name, obvious_brand))

        if model and name and _norm_ws(model).casefold() == _norm_ws(name).casefold():
            issues.append(QualityIssue("cosmetic", "model_equals_name", oid, name, model[:120]))

        is_consumable = bool(_CONSUMABLE_NAME_RE.match(name) or typ in {"Картридж", "Тонер-картридж", "Драм-картридж", "Чернила", "Девелопер"})
        if is_consumable:
            if not compat and not _COMPAT_FAMILY_RE.search(desc) and not _ink_can_skip_compat(name, typ, desc):
                issues.append(QualityIssue("cosmetic", "missing_compat", oid, name, "compat missing"))
            if not codes and not _ink_can_skip_codes(name, typ, desc):
                issues.append(QualityIssue("cosmetic", "missing_codes", oid, name, "codes missing"))

            if expected_title_codes and not code_list:
                issues.append(QualityIssue("cosmetic", "code_present_in_title_but_missing_in_params", oid, name, ", ".join(expected_title_codes[:6])))

            if expected_title_codes and code_list:
                missing = [x for x in expected_title_codes if not _expected_code_is_covered(x, code_list, vendor, name)]
                if missing:
                    canon_expected = [x for x in expected_title_codes if x.startswith("CANON ")]
                    canon_missing = [x for x in missing if x.startswith("CANON ")]
                    if canon_expected and canon_missing and "/" in name:
                        issues.append(QualityIssue("cosmetic", "mixed_brand_tail_incomplete", oid, name, ", ".join(canon_missing[:6])))
                    else:
                        issues.append(QualityIssue("cosmetic", "multi_code_parsing_incomplete", oid, name, ", ".join(missing[:6])))

            if _COMPAT_BROKEN_RX.search(compat):
                issues.append(QualityIssue("cosmetic", "compat_normalization_broken", oid, name, compat[:160]))

        if typ != "Кабель сетевой":
            unexpected_cable = [k for k in _CABLE_KEYS if _param_first(params, k)]
            if unexpected_cable:
                issues.append(QualityIssue("cosmetic", "unexpected_cable_param_on_non_cable", oid, name, ", ".join(unexpected_cable)))

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

    threshold_ok = True
    if critical_count > 0:
        threshold_ok = False
    if cosmetic_offer_count > max_cosmetic_offers or cosmetic_issue_count > max_cosmetic_issues:
        threshold_ok = False

    effective_ok = threshold_ok if enforce else True

    if report_path:
        p = Path(report_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        lines.append(f"QUALITY_GATE_THRESHOLD: {'PASS' if threshold_ok else 'FAIL'}")
        lines.append(f"QUALITY_GATE_EFFECTIVE: {'PASS' if effective_ok else 'FAIL'}")
        lines.append(f"enforce: {str(enforce).lower()}")
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
        "ok": effective_ok,
        "threshold_ok": threshold_ok,
        "critical_count": critical_count,
        "cosmetic_total_count": cosmetic_issue_count,
        "cosmetic_offer_count": cosmetic_offer_count,
        "new_cosmetic_count": len(new_cosmetic),
        "max_cosmetic_offers": max_cosmetic_offers,
        "max_cosmetic_issues": max_cosmetic_issues,
        "enforce": enforce,
        "report_path": report_path or "",
    }
