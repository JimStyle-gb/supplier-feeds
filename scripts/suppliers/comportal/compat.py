# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/compat.py
ComPortal compat / codes cleanup helpers.

Роль:
- точечная supplier-specific cleanup логика;
- не генерировать совместимость;
- только безопасная нормализация кодов/моделей, если они уже есть.

Важно:
- core не должен генерировать compat;
- модуль не должен угадывать совместимость.
"""

from __future__ import annotations

import re
from typing import Dict, List


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def normalize_codes_value(value: str) -> str:
    """Нормализовать формат кодов/part numbers без угадываний."""
    v = norm_spaces(value)
    if not v:
        return ""

    # Разделители типа "A/B" или "A, B" к единому виду с запятой+пробелом.
    v = re.sub(r"\s*/\s*", " / ", v)
    v = re.sub(r"\s*,\s*", ", ", v)
    v = re.sub(r"\s{2,}", " ", v)
    return v.strip()


def apply_compat_cleanup(params: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Применить точечную cleanup-логику к params.
    Никакой генерации совместимости, только cleanup уже существующих значений.
    """
    out: List[Dict[str, str]] = []

    for p in params or []:
        name = norm_spaces(p.get("name", ""))
        value = norm_spaces(p.get("value", ""))

        if not name or not value:
            continue

        if name in {"Коды", "Модель", "Партномер", "Номер"}:
            value = normalize_codes_value(value)

        out.append({"name": name, "value": value})

    return out


__all__ = [
    "normalize_codes_value",
    "apply_compat_cleanup",
]
