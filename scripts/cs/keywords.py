# -*- coding: utf-8 -*-
"""
CS Keywords — общий сборщик <keywords>.
Этап 1 рефакторинга: вынос keywords из cs/core.py без изменения логики.
"""

from __future__ import annotations

import os
import re

CS_KEYWORDS_MAX_LEN = int((os.getenv("CS_KEYWORDS_MAX_LEN", "380") or "480").strip() or "480")

def norm_ws(s: str) -> str:
    s2 = (s or "").replace("\u00a0", " ").strip()
    s2 = re.sub(r"\s+", " ", s2)
    s2 = fix_mixed_cyr_lat(s2)
    return s2.strip()

def _dedup_keep_order(items: list[str]) -> list[str]:
    """CS: дедупликация со стабильным порядком (без сортировки)."""
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

CS_KEYWORDS_PHRASES = (
    "доставка",
    "доставка по Казахстану",
    "отправка в регионы",
)

def build_keywords(
    vendor: str | None,
    offer_name: str,
    extra: list[str] | None = None,
    **_kwargs,
) -> str:
    # CS: keywords нужны в основном для внутреннего поиска/фильтров; Google meta-keywords не использует,
    # Yandex учитывает слабо. Но для Satu/маркетплейса и внутреннего поиска — полезно.
    # Правила:
    # - без дублей
    # - "доставка" убираем, если есть "доставка по Казахстану"
    # - лимит по длине (CS_KEYWORDS_MAX_LEN, по умолчанию 380)
    parts: list[str] = []
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
