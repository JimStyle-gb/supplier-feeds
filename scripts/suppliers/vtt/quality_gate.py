# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/quality_gate.py

VTT quality gate.

Задача:
- проверить, что RAW уже чистый;
- отделить critical от cosmetic;
- уважать supplier policy/baseline;
- записать текстовый отчёт.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable
import xml.etree.ElementTree as ET

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore


DEFAULT_POLICY_PATH = Path(__file__).resolve().parent / "config" / "policy.yml"
DEFAULT_BASELINE_PATH = Path(__file__).resolve().parent / "config" / "quality_gate_baseline.yml"
DEFAULT_REPORT_PATH = Path("docs/raw/vtt_quality_gate.txt")


@dataclass(frozen=True)
class QualityGateResult:
    ok: bool
    critical_count: int
    cosmetic_count: int
    critical: tuple[str, ...]
    cosmetic: tuple[str, ...]
    report_path: str


def _safe_text(x: object) -> str:
    return str(x).strip() if x is not None else ""


def _cf(text: str) -> str:
    return _safe_text(text).casefold().replace("ё", "е")


def _read_yaml(path: str | Path | None) -> dict:
    p = Path(path) if path else None
    if not p or not p.exists() or yaml is None:
        return {}
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_policy(path: str | Path | None) -> dict:
    data = _read_yaml(path or DEFAULT_POLICY_PATH)
    qg = data.get("quality_gate") or {}
    if not isinstance(qg, dict):
        qg = {}
    return {
        "max_critical": int(qg.get("max_critical", 0)),
        "max_cosmetic": int(qg.get("max_cosmetic", 5)),
        "report_path": _safe_text(qg.get("report_path")) or str(DEFAULT_REPORT_PATH),
    }


def _load_baseline(path: str | Path | None) -> dict:
    data = _read_yaml(path or DEFAULT_BASELINE_PATH)
    if not isinstance(data, dict):
        return {"critical": [], "cosmetic": []}
    return {
        "critical": [str(x) for x in (data.get("critical") or [])],
        "cosmetic": [str(x) for x in (data.get("cosmetic") or [])],
    }


def _iter_offers(feed_path: str | Path) -> list[ET.Element]:
    root = ET.parse(str(feed_path)).getroot()
    offers = root.findall(".//offer")
    return list(offers)


def _param_map(offer: ET.Element) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for p in offer.findall("./param"):
        name = _safe_text(p.get("name"))
        val = _safe_text(p.text)
        if not name or not val:
            continue
        out.setdefault(name, []).append(val)
    return out


def _param_first(params: dict[str, list[str]], key: str) -> str:
    for k, vals in params.items():
        if _cf(k) == _cf(key) and vals:
            return _safe_text(vals[0])
    return ""


def _all_param_keys(params: dict[str, list[str]]) -> list[str]:
    return list(params.keys())


def _all_param_values(params: dict[str, list[str]]) -> list[str]:
    out: list[str] = []
    for vals in params.values():
        out.extend([_safe_text(v) for v in vals if _safe_text(v)])
    return out


def _has_bad_service_leak(texts: Iterable[str]) -> str:
    joined = "\n".join([_safe_text(x) for x in texts if _safe_text(x)])
    if not joined:
        return ""

    checks = (
        ("Аналоги", r"\bаналог(?:и)?\b"),
        ("OEM-номер", r"\boem(?:-номер| номер)?\b"),
        ("Каталожный номер", r"\bкат(?:аложный)?\.?\s*номер\b"),
        ("Штрихкод", r"\bштрих-?\s*код\b|\bean\b|\bbarcode\b"),
        ("Партс-номер", r"\bпартс-?номер\b"),
        ("Артикул", r"\bартикул\b"),
    )
    for label, rx in checks:
        if re.search(rx, joined, flags=re.I):
            return label
    return ""


def _looks_like_placeholder(url: str) -> bool:
    v = _safe_text(url).lower()
    if not v:
        return False
    return any(x in v for x in ("no_photo", "nophoto", "noimage", "no-image", "placeholder"))


def _looks_like_untranslated_color(value: str) -> bool:
    s = _cf(value).replace(" ", "")
    bad = {
        "black", "bk", "cyan", "magenta", "yellow", "grey", "gray",
        "mattblack", "matteblack", "photoblack", "photo black".replace(" ", ""),
        "color", "colour",
    }
    return s in bad


def _looks_like_bad_compat(value: str) -> bool:
    s = _safe_text(value)
    if not s:
        return False
    if len(s) < 4:
        return True
    # service tails in compat
    if re.search(r"\b(купить|цена|в наличии|под заказ|доставка|ресурс|цвет)\b", s, flags=re.I):
        return True
    return False


def _is_mostly_service_desc(desc: str) -> bool:
    s = _safe_text(desc)
    if not s:
        return False
    hits = 0
    for rx in (
        r"\bкупить\b",
        r"\bцена\b",
        r"\bв наличии\b",
        r"\bпод заказ\b",
        r"\bдоставка\b",
        r"\bсамовывоз\b",
        r"\bзаказать\b",
    ):
        if re.search(rx, s, flags=re.I):
            hits += 1
    return hits >= 2


def _normalize_issue(issue: str) -> str:
    return re.sub(r"\s+", " ", _safe_text(issue)).strip()


def _apply_baseline(issues: Iterable[str], baseline_list: Iterable[str]) -> list[str]:
    out: list[str] = []
    patterns = [str(x) for x in baseline_list if _safe_text(x)]
    for issue in issues:
        msg = _normalize_issue(issue)
        muted = False
        for pat in patterns:
            try:
                if re.search(pat, msg, flags=re.I):
                    muted = True
                    break
            except re.error:
                if pat.casefold() in msg.casefold():
                    muted = True
                    break
        if not muted:
            out.append(msg)
    return out


def _check_offer(offer: ET.Element) -> tuple[list[str], list[str]]:
    critical: list[str] = []
    cosmetic: list[str] = []

    oid = _safe_text(offer.get("id"))
    available = _safe_text(offer.get("available"))
    vendor_code = _safe_text(offer.findtext("./vendorCode"))
    name = _safe_text(offer.findtext("./name"))
    price = _safe_text(offer.findtext("./price"))
    vendor = _safe_text(offer.findtext("./vendor"))
    desc = _safe_text(offer.findtext("./description"))
    pics = [_safe_text(x.text) for x in offer.findall("./picture") if _safe_text(x.text)]

    params = _param_map(offer)
    partnumber = _param_first(params, "Партномер")
    compat = _param_first(params, "Совместимость")
    color = _param_first(params, "Цвет")
    codes = _param_first(params, "Коды расходников")

    # critical
    if not oid:
        critical.append("offer без id")
    if not vendor_code:
        critical.append(f"{oid or '?'}: пустой vendorCode")
    elif oid and vendor_code != oid:
        critical.append(f"{oid}: vendorCode != offer/@id")
    if available and available.lower() != "true":
        critical.append(f"{oid}: available != true")
    if not name:
        critical.append(f"{oid}: пустой name")
    if not price:
        critical.append(f"{oid}: пустой price")
    else:
        try:
            p = int(price)
            if p <= 0:
                critical.append(f"{oid}: price <= 0")
        except Exception:
            critical.append(f"{oid}: битый price")
    if not pics:
        critical.append(f"{oid}: нет picture")
    elif any(_looks_like_placeholder(x) for x in pics[1:]):
        critical.append(f"{oid}: placeholder не должен идти после реальных фото")

    # supplier-clean RAW checks
    leak = _has_bad_service_leak([name, desc, *(_all_param_keys(params)), *(_all_param_values(params))])
    if leak:
        critical.append(f"{oid}: утечка служебного поля '{leak}' в RAW")

    # cosmetic
    if not vendor:
        cosmetic.append(f"{oid}: пустой vendor")
    if not partnumber:
        cosmetic.append(f"{oid}: нет Партномер")
    if not compat:
        cosmetic.append(f"{oid}: нет Совместимость")
    elif _looks_like_bad_compat(compat):
        cosmetic.append(f"{oid}: подозрительная Совместимость")
    if color and _looks_like_untranslated_color(color):
        cosmetic.append(f"{oid}: неканонизированный Цвет='{color}'")
    if codes and len(codes) > 250:
        cosmetic.append(f"{oid}: слишком длинные Коды расходников")
    if desc:
        if _is_mostly_service_desc(desc):
            cosmetic.append(f"{oid}: service/SEO description")
        if len(desc) < 30:
            cosmetic.append(f"{oid}: слишком короткий description")
    else:
        cosmetic.append(f"{oid}: пустой description")
    if len(name) > 240:
        cosmetic.append(f"{oid}: слишком длинный name")
    if len(pics) > 8:
        cosmetic.append(f"{oid}: слишком много picture ({len(pics)})")

    return critical, cosmetic


def _check_duplicates(offers: list[ET.Element]) -> tuple[list[str], list[str]]:
    critical: list[str] = []
    cosmetic: list[str] = []

    seen_ids: dict[str, int] = {}
    for offer in offers:
        oid = _safe_text(offer.get("id"))
        if not oid:
            continue
        seen_ids[oid] = seen_ids.get(oid, 0) + 1

    dups = [oid for oid, cnt in seen_ids.items() if cnt > 1]
    for oid in dups:
        critical.append(f"{oid}: duplicate offer id")

    return critical, cosmetic


def _build_report(
    *,
    feed_path: str,
    critical: list[str],
    cosmetic: list[str],
    ok: bool,
) -> str:
    lines: list[str] = []
    lines.append("VTT QUALITY GATE REPORT")
    lines.append("=" * 72)
    lines.append(f"feed_path: {feed_path}")
    lines.append(f"ok: {'yes' if ok else 'no'}")
    lines.append(f"critical_count: {len(critical)}")
    lines.append(f"cosmetic_count: {len(cosmetic)}")
    lines.append("-" * 72)

    lines.append("CRITICAL:")
    if critical:
        for msg in critical:
            lines.append(f"- {msg}")
    else:
        lines.append("- none")

    lines.append("-" * 72)
    lines.append("COSMETIC:")
    if cosmetic:
        for msg in cosmetic:
            lines.append(f"- {msg}")
    else:
        lines.append("- none")

    lines.append("")
    return "\n".join(lines)


def run_quality_gate(
    *,
    feed_path: str | Path,
    policy_path: str | Path | None = None,
    baseline_path: str | Path | None = None,
    report_path: str | Path | None = None,
) -> QualityGateResult:
    """
    Главная функция quality gate.
    """
    feed_path = str(feed_path)
    policy = _load_policy(policy_path)
    baseline = _load_baseline(baseline_path)

    crit: list[str] = []
    cos: list[str] = []

    offers = _iter_offers(feed_path)

    c1, z1 = _check_duplicates(offers)
    crit.extend(c1)
    cos.extend(z1)

    for offer in offers:
        c, z = _check_offer(offer)
        crit.extend(c)
        cos.extend(z)

    crit = _apply_baseline(crit, baseline.get("critical") or [])
    cos = _apply_baseline(cos, baseline.get("cosmetic") or [])

    crit = sorted(set(crit))
    cos = sorted(set(cos))

    max_critical = int(policy.get("max_critical", 0))
    max_cosmetic = int(policy.get("max_cosmetic", 5))

    ok = (len(crit) <= max_critical) and (len(cos) <= max_cosmetic)

    final_report_path = Path(
        str(report_path or policy.get("report_path") or DEFAULT_REPORT_PATH)
    )
    final_report_path.parent.mkdir(parents=True, exist_ok=True)
    final_report_path.write_text(
        _build_report(
            feed_path=feed_path,
            critical=crit,
            cosmetic=cos,
            ok=ok,
        ),
        encoding="utf-8",
    )

    return QualityGateResult(
        ok=ok,
        critical_count=len(crit),
        cosmetic_count=len(cos),
        critical=tuple(crit),
        cosmetic=tuple(cos),
        report_path=str(final_report_path),
    )
