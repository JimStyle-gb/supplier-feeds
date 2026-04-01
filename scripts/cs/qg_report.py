# -*- coding: utf-8 -*-
"""
Path: scripts/cs/qg_report.py

Единый writer для docs/raw/<supplier>_quality_gate.txt.

Задача:
- сделать одинаковый формат отчёта для всех поставщиков;
- рядом с rule-кодом дать короткий русский комментарий;
- не менять supplier-specific логику поиска ошибок;
- отвечать только за единый текстовый вывод.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping


_RULE_COMMENTS_RU: dict[str, str] = {
    "invalid_price": "Цена невалидна после финальной сборки",
    "banned_param_key": "В финал просочился запрещённый параметр",
    "desc_oaicite_leak": "В описание попала служебная метка",
    "compat_label_leak": "В значение совместимости протёк чужой label/заголовок",
    "desc_header_leak": "В описание протёк supplier-заголовок",
    "suspicious_vendor": "Vendor выглядит подозрительно и требует проверки",
    "bad_power_key": "Ключ мощности записан в мусорном формате",
    "heavy_xerox_compat": "Слишком длинная и тяжёлая цепочка Xerox-совместимости",
    "missing_identity": "У товара отсутствует обязательная идентичность",
    "missing_picture": "У товара нет картинки",
    "placeholder_picture_only": "Вместо картинки стоит заглушка",
    "empty_description": "Описание пустое",
    "empty_vendor": "Vendor пустой",
    "vendor_empty_but_brand_obvious": "Бренд очевиден, но vendor не заполнен",
    "model_equals_name": "Модель полностью дублирует название",
    "missing_compat": "Не поднялась совместимость",
    "missing_codes": "Не поднялись коды расходников",
    "code_present_in_title_but_missing_in_params": "Код есть в названии, но не поднялся в параметры",
    "mixed_brand_tail_incomplete": "Mixed-brand / mixed-code хвост разобран не полностью",
    "multi_code_parsing_incomplete": "Не все ожидаемые коды корректно разобрались",
    "compat_normalization_broken": "Совместимость нормализована с ошибкой",
    "unexpected_cable_param_on_non_cable": "Кабельный параметр попал в некабельный товар",
    "marketplace_param_leak": "В параметры просочились служебные поля маркетплейса",
    "marketplace_text_in_description": "В описание просочился служебный текст маркетплейса",
}


def _safe_str(value: Any) -> str:
    return str(value or "").strip()


def _offer_count(items: list[dict[str, str]]) -> int:
    return len({x["oid"] for x in items if x["oid"]})


def _issue_to_row(issue: Any) -> dict[str, str]:
    return {
        "severity": _safe_str(getattr(issue, "severity", "")),
        "rule": _safe_str(getattr(issue, "rule", "")),
        "oid": _safe_str(getattr(issue, "oid", "")),
        "name": _safe_str(getattr(issue, "name", "")),
        "details": _safe_str(getattr(issue, "details", "")),
    }


def _rule_comment(rule: str) -> str:
    return _RULE_COMMENTS_RU.get(_safe_str(rule), "Требует ручной проверки")


def _append_issue_section(lines: list[str], title: str, issues: list[dict[str, str]]) -> None:
    lines.append(title)
    if not issues:
        lines.append("# Ошибок в этой секции нет")
        lines.append("")
        return

    for item in issues:
        lines.append(
            f"- {item['oid']} | {item['rule']} | {_rule_comment(item['rule'])} | {item['name']} | {item['details']}"
        )
    lines.append("")


def _append_baseline_snapshot(lines: list[str], accepted_cosmetic: Mapping[str, Iterable[str]] | None) -> None:
    if not accepted_cosmetic:
        return

    lines.append("BASELINE COSMETIC SNAPSHOT:")
    lines.append("# Справочный снимок известных cosmetic-хвостов из baseline")
    for rule in sorted(accepted_cosmetic):
        oids = sorted({_safe_str(x) for x in (accepted_cosmetic.get(rule) or []) if _safe_str(x)})
        if not oids:
            continue
        lines.append(f"- {rule}: {len(oids)} offer(s)")
        for oid in oids[:50]:
            lines.append(f"  - {oid}")
        if len(oids) > 50:
            lines.append(f"  - ... +{len(oids) - 50}")
    lines.append("")


def write_quality_gate_report(
    path: str,
    *,
    passed: bool,
    enforce: bool,
    baseline_path: str = "",
    freeze_current_as_baseline: bool = False,
    critical: Iterable[Any] = (),
    cosmetic: Iterable[Any] = (),
    known_cosmetic: Iterable[Any] = (),
    new_cosmetic: Iterable[Any] = (),
    max_cosmetic_offers: int = 5,
    max_cosmetic_issues: int = 5,
    accepted_cosmetic: Mapping[str, Iterable[str]] | None = None,
) -> None:
    """Записывает единый quality gate report для любого поставщика."""
    report_path = Path(path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    critical_rows = [_issue_to_row(x) for x in critical]
    cosmetic_rows = [_issue_to_row(x) for x in cosmetic]
    known_rows = [_issue_to_row(x) for x in known_cosmetic]
    new_rows = [_issue_to_row(x) for x in new_cosmetic]

    lines: list[str] = []
    lines.append("# Итог проверки quality gate")
    lines.append(f"QUALITY_GATE: {'PASS' if passed else 'FAIL'}")
    lines.append("# PASS = можно выпускать | FAIL = есть блокирующие проблемы")
    lines.append(f"enforce: {str(bool(enforce)).lower()}")
    lines.append("# true = quality gate реально валит сборку")
    lines.append(f"report_file: {path}")
    lines.append("# Куда записан этот отчёт")
    lines.append(f"baseline_file: {baseline_path}")
    lines.append("# Базовый файл для сравнения известных cosmetic-проблем")
    lines.append(f"freeze_current_as_baseline: {'yes' if freeze_current_as_baseline else 'no'}")
    lines.append("# yes = текущие cosmetic-хвосты сохранены как baseline-снимок")
    lines.append(f"critical_count: {len(critical_rows)}")
    lines.append("# Сколько найдено критичных проблем")
    lines.append(f"cosmetic_total_count: {len(cosmetic_rows)}")
    lines.append("# Общее число некритичных проблем")
    lines.append(f"cosmetic_offer_count: {_offer_count(cosmetic_rows)}")
    lines.append("# В скольких товарах есть cosmetic-проблемы")
    lines.append(f"known_cosmetic_count: {len(known_rows)}")
    lines.append("# Сколько cosmetic-проблем уже известны по baseline")
    lines.append(f"known_cosmetic_offer_count: {_offer_count(known_rows)}")
    lines.append("# В скольких товарах есть уже известные cosmetic-проблемы")
    lines.append(f"new_cosmetic_count: {len(new_rows)}")
    lines.append("# Сколько найдено новых cosmetic-проблем")
    lines.append(f"new_cosmetic_offer_count: {_offer_count(new_rows)}")
    lines.append("# В скольких товарах появились новые cosmetic-проблемы")
    lines.append(f"max_cosmetic_offers: {int(max_cosmetic_offers)}")
    lines.append("# Допустимый максимум товаров с cosmetic-проблемами")
    lines.append(f"max_cosmetic_issues: {int(max_cosmetic_issues)}")
    lines.append("# Допустимый максимум cosmetic-проблем всего")
    lines.append("")

    _append_baseline_snapshot(lines, accepted_cosmetic)
    _append_issue_section(lines, "CRITICAL:", critical_rows)
    _append_issue_section(lines, "COSMETIC TOTAL:", cosmetic_rows)
    _append_issue_section(lines, "NEW COSMETIC VS BASELINE:", new_rows)
    _append_issue_section(lines, "KNOWN COSMETIC FROM BASELINE:", known_rows)

    report_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
