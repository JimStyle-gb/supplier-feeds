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

# Базовые regex-хелперы
_RE_WS = re.compile(r"\s+")

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
