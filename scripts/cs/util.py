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

# Визуально похожие латинские -> кириллические и обратно (только внутри смешанных токенов)
_LAT_TO_CYR = {
    "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н", "K": "К", "M": "М", "O": "О", "P": "Р", "T": "Т", "X": "Х", "Y": "У",
    "a": "а", "c": "с", "e": "е", "h": "н", "k": "к", "m": "м", "o": "о", "p": "р", "t": "т", "x": "х", "y": "у",
}
_CYR_TO_LAT = {
    "А": "A", "В": "B", "С": "C", "Е": "E", "Н": "H", "К": "K", "М": "M", "О": "O", "Р": "P", "Т": "T", "Х": "X", "У": "Y",
    "а": "a", "с": "c", "е": "e", "н": "h", "к": "k", "м": "m", "о": "o", "р": "p", "т": "t", "х": "x", "у": "y",
}
_RE_CYR = re.compile(r"[А-Яа-яЁё]")
_RE_LAT = re.compile(r"[A-Za-z]")
_RE_MIXED_TOKEN = re.compile(r"[A-Za-zА-Яа-яЁё]{2,}")


def fix_mixed_cyr_lat(s: str) -> str:
    """Чинит смешение кириллицы/латиницы в одном токене.

    Примеры:
    - Pабота -> Работа
    - LЕD -> LED
    - LСD -> LCD
    - SNМР -> SNMP
    - рlеnuм -> plenum
    """
    if not s:
        return s

    def _fix_token(m: re.Match[str]) -> str:
        tok = m.group(0)
        has_cyr = bool(_RE_CYR.search(tok))
        has_lat = bool(_RE_LAT.search(tok))
        if not (has_cyr and has_lat):
            return tok

        lat_cnt = sum(("A" <= ch <= "Z") or ("a" <= ch <= "z") for ch in tok)
        cyr_cnt = sum(bool(_RE_CYR.match(ch)) for ch in tok)

        # Если токен больше похож на латинский техно-термин/аббревиатуру — приводим к LAT.
        # Иначе считаем, что это кириллическое слово с латинскими вкраплениями.
        to_lat = lat_cnt >= cyr_cnt
        if to_lat:
            return "".join(_CYR_TO_LAT.get(ch, ch) for ch in tok)
        return "".join(_LAT_TO_CYR.get(ch, ch) for ch in tok)

    return _RE_MIXED_TOKEN.sub(_fix_token, s)


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


def _truncate_text(s: str, max_len: int, *, suffix: str = "") -> str:
    """Обрезает текст до max_len, аккуратно (plain), опционально с suffix."""
    if not s:
        return ""
    max_len = int(max_len or 0)
    if max_len <= 0:
        return s

    suffix = suffix or ""
    if len(s) <= max_len:
        return s

    if suffix:
        if max_len <= len(suffix):
            return suffix[:max_len].rstrip()
        cut = max_len - len(suffix)
        base = s[:cut].rstrip()
        return (base + suffix).rstrip()

    return s[:max_len].rstrip()
