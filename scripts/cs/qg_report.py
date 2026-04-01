# -*- coding: utf-8 -*-
"""
CS quality gate report writer.
Единый writer для всех docs/raw/<supplier>_quality_gate.txt
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable


def _ru_rule_comment(rule: str) -> str:
    comments = {
        "invalid_price": "Цена невалидна после финальной сборки",
        "banned_param_key": "В финал просочился запрещённый параметр",
        "desc_oaicite_leak": "В описание попала служебная метка",
        "compat_label_leak": "В совместимость протекли служебные label-блоки",
        "bad_power_key": "В параметрах остался мусорный ключ мощности",
        "heavy_xerox_compat": "Слишком длинная цепочка совместимости Xerox",
        "marketplace_param_leak": "В финал/гейт просочился служебный marketplace-параметр",
        "marketplace_text_in_description": "В описание попал служебный marketplace-текст",
        "tech_block_leak_in_body": "В обычный body протёк техблок 'Характеристики'",
    }
    return comments.get(rule, "Требует ручной проверки")


def _issue_line(issue) -> str:
    details = (issue.details or "").replace("\n", " ").strip()
    if len(details) > 240:
        details = details[:237] + "..."
    return f"{issue.oid} | {issue.rule} | {_ru_rule_comment(issue.rule)} | {details}"


def _section(lines: list[str], title: str, issues: Iterable) -> None:
    lines.append("")
    lines.append(f"{title}:")
    items = list(issues)
    if not items:
        lines.append("# Ошибок в этой секции нет")
        return
    for issue in items:
        lines.append(_issue_line(issue))


def write_quality_gate_report(
    path: str,
    *,
    supplier: str,
    passed: bool,
    enforce: bool,
    baseline_file: str,
    freeze_current_as_baseline: bool,
    critical: list,
    cosmetic: list,
    known_cosmetic: list,
    new_cosmetic: list,
    max_cosmetic_offers: int,
    max_cosmetic_issues: int,
) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    cosmetic_offer_count = len({x.oid for x in cosmetic})
    known_offer_count = len({x.oid for x in known_cosmetic})
    new_offer_count = len({x.oid for x in new_cosmetic})

    lines: list[str] = []
    lines.append("# Итог проверки quality gate")
    lines.append(f"QUALITY_GATE: {'PASS' if passed else 'FAIL'}")
    lines.append("# PASS = можно выпускать | FAIL = есть блокирующие проблемы")
    lines.append(f"enforce: {'true' if enforce else 'false'}")
    lines.append("# true = quality gate реально валит сборку")
    lines.append(f"report_file: {path}")
    lines.append("# Куда записан этот отчёт")
    lines.append(f"baseline_file: {baseline_file}")
    lines.append("# Базовый файл для сравнения известных cosmetic-проблем")
    lines.append(f"freeze_current_as_baseline: {'yes' if freeze_current_as_baseline else 'no'}")
    lines.append("# yes = текущие cosmetic-хвосты сохранены как baseline-снимок")
    lines.append(f"critical_count: {len(critical)}")
    lines.append("# Сколько найдено критичных проблем")
    lines.append(f"cosmetic_total_count: {len(cosmetic)}")
    lines.append("# Общее число некритичных проблем")
    lines.append(f"cosmetic_offer_count: {cosmetic_offer_count}")
    lines.append("# В скольких товарах есть cosmetic-проблемы")
    lines.append(f"known_cosmetic_count: {len(known_cosmetic)}")
    lines.append("# Сколько cosmetic-проблем уже известны по baseline")
    lines.append(f"known_cosmetic_offer_count: {known_offer_count}")
    lines.append("# В скольких товарах есть уже известные cosmetic-проблемы")
    lines.append(f"new_cosmetic_count: {len(new_cosmetic)}")
    lines.append("# Сколько найдено новых cosmetic-проблем")
    lines.append(f"new_cosmetic_offer_count: {new_offer_count}")
    lines.append("# В скольких товарах появились новые cosmetic-проблемы")
    lines.append(f"max_cosmetic_offers: {int(max_cosmetic_offers)}")
    lines.append("# Допустимый максимум товаров с cosmetic-проблемами")
    lines.append(f"max_cosmetic_issues: {int(max_cosmetic_issues)}")
    lines.append("# Допустимый максимум cosmetic-проблем всего")

    _section(lines, "CRITICAL", critical)
    _section(lines, "COSMETIC TOTAL", cosmetic)
    _section(lines, "NEW COSMETIC VS BASELINE", new_cosmetic)
    _section(lines, "KNOWN COSMETIC FROM BASELINE", known_cosmetic)

    p.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
