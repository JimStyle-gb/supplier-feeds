# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/compat.py

ComPortal compat / codes cleanup helpers.

Роль как у других поставщиков:
- supplier-side cleanup для кодов / совместимости / модельных серий;
- никакой генерации совместимости;
- только безопасная нормализация уже существующих значений.
"""

from __future__ import annotations

import re
from typing import Iterable

from cs.util import norm_ws
from suppliers.comportal.models import ParamItem


_MULTI_WS_RE = re.compile(r"\s{2,}")
_COMMA_WS_RE = re.compile(r"\s*,\s*")
_SLASH_WS_RE = re.compile(r"\s*/\s*")
_SEMICOLON_WS_RE = re.compile(r"\s*;\s*")


def _normalize_codes_value(value: str) -> str:
    s = norm_ws(value)
    if not s:
        return ""
    s = _SLASH_WS_RE.sub(" / ", s)
    s = _COMMA_WS_RE.sub(", ", s)
    s = _SEMICOLON_WS_RE.sub("; ", s)
    s = _MULTI_WS_RE.sub(" ", s)
    return s.strip(" ,;/")


def _normalize_compat_value(value: str) -> str:
    s = norm_ws(value)
    if not s:
        return ""
    s = _SLASH_WS_RE.sub(" / ", s)
    s = _COMMA_WS_RE.sub(", ", s)
    s = _SEMICOLON_WS_RE.sub("; ", s)
    s = _MULTI_WS_RE.sub(" ", s)
    return s.strip(" ,;/")


def apply_compat_cleanup(params: Iterable[ParamItem]) -> list[ParamItem]:
    """
    Применить безопасную cleanup-логику к supplier params.
    Совместимость не генерируем — только чистим уже найденное.
    """
    out: list[ParamItem] = []

    for p in params or []:
        name = norm_ws(p.name)
        value = norm_ws(p.value)
        if not name or not value:
            continue

        ncf = name.casefold()
        if ncf in {"коды", "модель", "партномер", "номер"}:
            value = _normalize_codes_value(value)
        elif ncf == "совместимость":
            value = _normalize_compat_value(value)

        if not value:
            continue

        out.append(ParamItem(name=name, value=value, source=p.source))

    return out


__all__ = [
    "apply_compat_cleanup",
]
