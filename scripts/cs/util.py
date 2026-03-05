# -*- coding: utf-8 -*-
"""
CS Util — мелкие общие утилиты.

Этап 8: вынос из cs/core.py в отдельный модуль.
Файл сделан САМОДОСТАТОЧНЫМ: содержит fix_mixed_cyr_lat(), чтобы не зависеть от cs/keywords.py
и не ловить NameError при частичных заменах файлов.

Содержит:
- fix_mixed_cyr_lat()
- norm_ws()
- safe_int()
- _truncate_text()
"""

from __future__ import annotations

import re
from typing import Any


# ----------------------------- text helpers -----------------------------

_RE_WS = re.compile(r"\s+")
_RE_INT = re.compile(r"-?\d+")

# Визуально похожие латинские -> кириллические (только внутри смешанных слов)
_MIX_MAP = str.maketrans({
    "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н", "K": "К", "M": "М", "O": "О", "P": "Р", "T": "Т", "X": "Х", "Y": "У",
    "a": "а", "c": "с", "e": "е", "h": "н", "k": "к", "m": "м", "o": "о", "p": "р", "t": "т", "x": "х", "y": "у",
})
_RE_CYR = re.compile(r"[А-Яа-яЁё]")
_RE_LAT = re.compile(r"[A-Za-z]")


def fix_mixed_cyr_lat(s: str) -> str:
    """Чинит смешение кириллицы/латиницы в одном слове (Pабота → Работа)."""
    if not s:
        return s

    def _fix_word(w: str) -> str:
        if _RE_CYR.search(w) and _RE_LAT.search(w):
            return w.translate(_MIX_MAP)
        return w

    parts = re.split(r"(\s+)", s)
    return "".join(_fix_word(p) if i % 2 == 0 else p for i, p in enumerate(parts))


def norm_ws(s: str) -> str:
    """Нормализует пробелы и правит смешанную кир/лат."""
    s2 = (s or "").replace("\u00a0", " ").strip()
    s2 = _RE_WS.sub(" ", s2).strip()
    return fix_mixed_cyr_lat(s2)


def safe_int(s: Any) -> int | None:
    """Безопасно парсит int из строки (берёт первое целое)."""
    if s is None:
        return None
    ss = str(s).strip()
    m = _RE_INT.search(ss.replace(" ", ""))
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


def _truncate_text(s: str, max_len: int) -> str:
    """Обрезает текст до max_len, аккуратно (без обрыва HTML, только plain)."""
    if not s:
        return ""
    max_len = int(max_len or 0)
    if max_len <= 0:
        return s
    if len(s) <= max_len:
        return s
    return s[:max_len].rstrip()
