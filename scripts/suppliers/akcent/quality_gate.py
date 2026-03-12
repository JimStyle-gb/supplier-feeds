# -*- coding: utf-8 -*-
"""
AkCent supplier-side quality gate.

Что делает:
- проверяет уже собранный финальный feed
- делит проблемы на critical / cosmetic
- валит сборку только по правилу stop-rule

Правило прохождения по умолчанию:
- critical_count == 0
- cosmetic_offer_count <= 5
- cosmetic_issue_count <= 5

Важно:
- baseline не выключает контроль
- cosmetic не игнорируются
- supplier-specific логика остаётся внутри supplier-layer
"""

from __future__ import annotations

import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from typing import Any

import yaml


_BAD_VENDOR_VALUES = {
    "",
    "мфу",
    "интерактивная",
    "интерактивные",
    "интерактивный",
    "ноутбук",
    "ноутбуки",
    "монитор",
    "мониторы",
    "принтер",
    "принтеры",
    "сканер",
    "сканеры",
    "компьютер",
    "компьютеры",
    "планшет",
    "планшеты",
    "телевизор",
    "телевизоры",
    "сервер",
    "серверы",
    "pc",
    "пк",
}

_COMPAT_LABEL_RE = re.compile(
    r"(?i)\b(?:совместимые?\s+модели|поддерживаемые?\s+модели(?:\s+принтеров)?|"
    r"поддерживаемые?\s+продукты|совместимость)\b"
)
_CODES_LABEL_RE = re.compile(
    r"(?i)\b(?:коды?\s+расходников|код(?:ы)?\s+картриджа|расходный\s+материал|"
    r"расходные\s+материалы)\b"
)
_MODEL_LABEL_RE = re.compile(r"(?i)^\s*(?:модель|model)\s*[:\-]")
_BAD_POWER_KEY_RE = re.compile(r"(?i)\bмощность\s*\((?:bt|w)\)")
_OAICITE_RE = re.compile(r"(?i)(?:oaicite|contentreference|turn\d+\w+\d+|【\d+†)")
_XML_DECL_RE = re.compile(r"^\s*<\?xml\b", re.I)


def _config_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "config")


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        obj = yaml.safe_load(f) or {}
    if not isinstance(obj, dict):
        raise ValueError(f"Bad YAML root in {path}: expected mapping")
    return obj


def _load_policy_cfg() -> dict[str, Any]:
    return _load_yaml(os.path.join(_config_dir(), "policy.yml"))


def _norm_space(s: str) -> str:
    return " ".join((s or "").strip().split())


def _ci(s: str) -> str:
    return _norm_space(s).casefold()


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def _parse_xml(path: str) -> ET.Element:
    text = _read_text(path)
    text = text.lstrip("\ufeff").strip()

    if not text:
        raise ValueError(f"Empty XML file: {path}")

    if not _XML_DECL_RE.search(text):
        # XML declaration не обязательна, но пусть будет мягкая обработка
        pass

    try:
        return ET.fromstring(text)
    except ET.ParseError as e:
        raise ValueError(f"XML parse error in {path}: {e}") from e


def _iter_offer_elements(root: ET.Element) -> list[ET.Element]:
    offers = root.findall(".//offer")
    return list(offers)


def _offer_id(offer_el: ET.Element) -> str:
    return _norm_space(offer_el.attrib.get("id") or "")


def _offer_available(offer_el: ET.Element) -> str:
    return _norm_space(offer_el.attrib.get("available") or "")


def _find_text(offer_el: ET.Element, tag: str) -> str:
    el = offer_el.find(tag)
    return _norm_space(el.text if el is not None else "")


def _find_all_texts(offer_el: ET.Element, tag: str) -> list[str]:
    out: list[str] = []
    for el in offer_el.findall(tag):
        val = _norm_space(el.text or "")
        if val:
            out.append(val)
    return out


def _find_params(offer_el: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for p in offer_el.findall("param"):
        name = _norm_space(p.attrib.get("name") or "")
        value = _norm_space(p.text or "")
        out.append((name, value))
    return out


def _price_ok(price_text: str) -> bool:
    if not price_text:
        return False
    try:
        return float(price_text.replace(",", ".")) > 0
    except Exception:
        return False


def _quality_cfg(policy_cfg: dict[str, Any]) -> dict[str, Any]:
    q = policy_cfg.get("quality_gate") or {}
    if not isinstance(q, dict):
        q = {}
    return q


def _max_cosmetic_offers(policy_cfg: dict[str, Any]) -> int:
    q = _quality_cfg(policy_cfg)
    try:
        return int(
            q.get(
                "max_cosmetic_offers",
                policy_cfg.get("max_cosmetic_offers", 5),
            )
        )
    except Exception:
        return 5


def _max_cosmetic_issues(policy_cfg: dict[str, Any]) -> int:
    q = _quality_cfg(policy_cfg)
    try:
        return int(
            q.get(
                "max_cosmetic_issues",
                policy_cfg.get("max_cosmetic_issues", 5),
            )
        )
    except Exception:
        return 5


def _check_global_critical(offers: list[ET.Element]) -> list[tuple[str, str, str]]:
    issues: list[tuple[str, str, str]] = []

    if not offers:
        issues.append(("GLOBAL", "no_offers", "В итоговом feed нет ни одного offer"))
        return issues

    seen: set[str] = set()
    duplicates: set[str] = set()

    for offer_el in offers:
        oid = _offer_id(offer_el)
        if not oid:
            issues.append(("GLOBAL", "missing_offer_id", "Найден offer без id"))
            continue
        if oid in seen:
            duplicates.add(oid)
        seen.add(oid)

    for oid in sorted(duplicates):
        issues.append((oid, "duplicate_offer_id", "Дублируется offer/@id"))

    return issues


def _check_offer_critical(offer_el: ET.Element) -> list[tuple[str, str, str]]:
    oid = _offer_id(offer_el) or "UNKNOWN"
    issues: list[tuple[str, str, str]] = []

    name = _find_text(offer_el, "name")
    price = _find_text(offer_el, "price")
    pictures = _find_all_texts(offer_el, "picture")

    if not name:
        issues.append((oid, "empty_name", "Пустой <name>"))

    if not _price_ok(price):
        issues.append((oid, "bad_price", f"Некорректный <price>: {price or '<empty>'}"))

    if not pictures:
        issues.append((oid, "no_picture", "Нет ни одного <picture>"))

    return issues


def _check_offer_cosmetic(offer_el: ET.Element) -> list[tuple[str, str, str]]:
    oid = _offer_id(offer_el) or "UNKNOWN"
    issues: list[tuple[str, str, str]] = []

    name = _find_text(offer_el, "name")
    desc = _find_text(offer_el, "description")
    vendor = _find_text(offer_el, "vendor")
    available = _offer_available(offer_el)
    params = _find_params(offer_el)

    if _ci(vendor) in _BAD_VENDOR_VALUES:
        issues.append((oid, "bad_vendor_value", f"Подозрительный vendor: {vendor or '<empty>'}"))

    if available not in {"true", "false"}:
        issues.append((oid, "bad_available_attr", f"Подозрительный available: {available or '<empty>'}"))

    if _OAICITE_RE.search(name) or _OAICITE_RE.search(desc):
        issues.append((oid, "desc_oaicite_leak", "В name/description найден след от цитат/oaicite"))

    pictures = _find_all_texts(offer_el, "picture")
    if pictures == ["https://placehold.co/800x800/png?text=No+Photo"]:
        issues.append((oid, "placeholder_picture", "У товара только placeholder-картинка"))

    seen_param_names: set[str] = set()
    for param_name, param_value in params:
        low_name = _ci(param_name)

        if not param_name:
            issues.append((oid, "empty_param_name", "Найден <param> без name"))
            continue
        if not param_value:
            issues.append((oid, "empty_param_value", f"Пустой value у param: {param_name}"))
            continue

        if low_name in seen_param_names:
            issues.append((oid, "duplicate_param_key", f"Повторяется ключ param: {param_name}"))
        else:
            seen_param_names.add(low_name)

        if _BAD_POWER_KEY_RE.search(param_name):
            issues.append((oid, "bad_power_key", f"Ненормализованный ключ мощности: {param_name}"))

        if low_name == "совместимость" and _COMPAT_LABEL_RE.search(param_value):
            issues.append((oid, "compat_label_leak", "В значении Совместимость остался label/text-leak"))

        if low_name == "коды расходников" and _CODES_LABEL_RE.search(param_value):
            issues.append((oid, "codes_label_leak", "В значении Коды расходников остался label/text-leak"))

        if low_name == "модель" and _MODEL_LABEL_RE.search(param_value):
            issues.append((oid, "model_label_leak", "В значении Модель остался label/text-leak"))

        if low_name == "мощность (bt)":
            issues.append((oid, "bad_power_key", "Ключ Мощность (Bt) должен быть нормализован"))

    return issues


def _print_issue_block(title: str, issues: list[tuple[str, str, str]], limit: int = 30) -> None:
    print(title)
    if not issues:
        print("  - none")
        return

    for oid, code, msg in issues[:limit]:
        print(f"  - [{oid}] {code}: {msg}")

    if len(issues) > limit:
        print(f"  ... and {len(issues) - limit} more")


def run_quality_gate(
    *,
    out_file: str,
    raw_out_file: str,
    supplier: str,
    version: str,
) -> None:
    policy_cfg = _load_policy_cfg()
    max_cosmetic_offers = _max_cosmetic_offers(policy_cfg)
    max_cosmetic_issues = _max_cosmetic_issues(policy_cfg)

    if not os.path.isfile(out_file):
        raise SystemExit(f"[{supplier}] quality gate failed: out_file not found: {out_file}")

    try:
        root = _parse_xml(out_file)
    except Exception as e:
        raise SystemExit(f"[{supplier}] quality gate failed: {e}") from e

    offers = _iter_offer_elements(root)

    critical_issues: list[tuple[str, str, str]] = []
    cosmetic_issues: list[tuple[str, str, str]] = []

    critical_issues.extend(_check_global_critical(offers))

    for offer_el in offers:
        critical_issues.extend(_check_offer_critical(offer_el))
        cosmetic_issues.extend(_check_offer_cosmetic(offer_el))

    cosmetic_offer_ids = {oid for oid, _, _ in cosmetic_issues if oid != "GLOBAL"}

    critical_counter = Counter(code for _, code, _ in critical_issues)
    cosmetic_counter = Counter(code for _, code, _ in cosmetic_issues)

    print("=" * 72)
    print(f"[{supplier}] quality gate")
    print("=" * 72)
    print(f"version: {version}")
    print(f"out_file: {out_file}")
    print(f"raw_out_file: {raw_out_file}")
    print(f"offers_total: {len(offers)}")
    print(f"critical_count: {len(critical_issues)}")
    print(f"cosmetic_issue_count: {len(cosmetic_issues)}")
    print(f"cosmetic_offer_count: {len(cosmetic_offer_ids)}")
    print(f"max_cosmetic_issues: {max_cosmetic_issues}")
    print(f"max_cosmetic_offers: {max_cosmetic_offers}")

    print("-" * 72)
    print("critical_breakdown:")
    if critical_counter:
        for code in sorted(critical_counter):
            print(f"  {code}: {critical_counter[code]}")
    else:
        print("  none")

    print("-" * 72)
    print("cosmetic_breakdown:")
    if cosmetic_counter:
        for code in sorted(cosmetic_counter):
            print(f"  {code}: {cosmetic_counter[code]}")
    else:
        print("  none")

    print("-" * 72)
    _print_issue_block("critical_issues:", critical_issues, limit=30)

    print("-" * 72)
    _print_issue_block("cosmetic_issues:", cosmetic_issues, limit=30)

    passed = (
        len(critical_issues) == 0
        and len(cosmetic_offer_ids) <= max_cosmetic_offers
        and len(cosmetic_issues) <= max_cosmetic_issues
    )

    print("-" * 72)
    print(f"result: {'PASS' if passed else 'FAIL'}")
    print("=" * 72)

    if not passed:
        raise SystemExit(1)


def main(argv: list[str] | None = None) -> None:
    argv = argv or sys.argv[1:]
    if len(argv) < 4:
        raise SystemExit(
            "Usage: python -m suppliers.akcent.quality_gate <out_file> <raw_out_file> <supplier> <version>"
        )

    out_file, raw_out_file, supplier, version = argv[:4]
    run_quality_gate(
        out_file=out_file,
        raw_out_file=raw_out_file,
        supplier=supplier,
        version=version,
    )


if __name__ == "__main__":
    main()
