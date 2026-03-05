# -*- coding: utf-8 -*-
"""
CS Keywords — общий сборщик <keywords>.

Файл вынесен из cs/core.py (Этап 1: утончение core).
Важно: НЕ зависит от cs/core.py (чтобы не ловить циклические импорты).

Правила:
- дедуп токенов
- если есть "доставка по Казахстану" — убираем отдельный "доставка"
- лимит по длине CS_KEYWORDS_MAX_LEN (по умолчанию 380), сначала уходят города (они в хвосте)
"""

from __future__ import annotations

import os
import re


CS_KEYWORDS_MAX_LEN = int((os.getenv("CS_KEYWORDS_MAX_LEN", "380") or "380").strip() or "380")

# Города Казахстана — хвост для локального поиска внутри маркетплейса
CS_KEYWORDS_CITIES = (
    "Казахстан",
    "Алматы",
    "Астана",
    "Шымкент",
    "Караганда",
    "Актобе",
    "Павлодар",
    "Костанай",
    "Атырау",
    "Актау",
    "Усть-Каменогорск",
    "Семей",
    "Тараз",
)

# Общие коммерческие фразы
CS_KEYWORDS_PHRASES = (
    "доставка",
    "доставка по Казахстану",
    "отправка в регионы",
)

# Визуально похожие латинские -> кириллические (только внутри смешанных слов)
_MIX_MAP = str.maketrans({
    "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н", "K": "К", "M": "М", "O": "О", "P": "Р", "T": "Т", "X": "Х", "Y": "У",
    "a": "а", "c": "с", "e": "е", "h": "н", "k": "к", "m": "м", "o": "о", "p": "р", "t": "т", "x": "х", "y": "у",
})

_RE_CYR = re.compile(r"[А-Яа-яЁё]")
_RE_LAT = re.compile(r"[A-Za-z]")
_RE_WS = re.compile(r"\s+")


def fix_mixed_cyr_lat(s: str) -> str:
    """Чинит смешение кириллицы/латиницы в одном слове (Pабота → Работа)."""
    if not s:
        return s
    def _fix_word(w: str) -> str:
        if _RE_CYR.search(w) and _RE_LAT.search(w):
            return w.translate(_MIX_MAP)
        return w
    # правим по словам, сохраняя разделители пробелами
    parts = re.split(r"(\s+)", s)
    return "".join(_fix_word(p) if i % 2 == 0 else p for i, p in enumerate(parts))


def norm_ws(s: str) -> str:
    s2 = (s or "").replace("\u00a0", " ").strip()
    s2 = _RE_WS.sub(" ", s2).strip()
    return fix_mixed_cyr_lat(s2)


def _dedup_keep_order(items: list[str]) -> list[str]:
    """Дедупликация со стабильным порядком (без сортировки)."""
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if not x:
            continue
        k = x.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def build_keywords(
    vendor: str | None,
    offer_name: str,
    extra: list[str] | None = None,
    **_kwargs,
) -> str:
    parts: list[str] = []
    # В keywords запятая — это разделитель токенов, поэтому убираем запятые из vendor/name
    vendor = (vendor or "").replace(",", " ") or None
    offer_name = (offer_name or "").replace(",", " ")
    if vendor:
        parts.append(norm_ws(vendor))
    if offer_name:
        parts.append(norm_ws(offer_name))

    if extra:
        for x in extra:
            x = norm_ws(x)
            if x:
                parts.append(x)

    parts.extend(CS_KEYWORDS_PHRASES)
    parts.extend(CS_KEYWORDS_CITIES)

    parts = _dedup_keep_order([norm_ws(p) for p in parts if norm_ws(p)])

    # анти-дубль: если есть "доставка по Казахстану" — убираем отдельный токен "доставка"
    low = [p.casefold() for p in parts]
    if "доставка по казахстану" in low and "доставка" in low:
        parts = [p for p in parts if p.casefold() != "доставка"]

    # лимит длины: сначала уходят города (они добавлены в конец)
    max_len = int(CS_KEYWORDS_MAX_LEN or 380)
    joined = ", ".join(parts)
    while len(joined) > max_len and len(parts) > 2:
        parts.pop()
        joined = ", ".join(parts)

    return joined
