# -*- coding: utf-8 -*-
"""
CS Pricing — общий расчёт цены.

Этап 4: вынос из cs/core.py в отдельный модуль, без изменения логики.
Важно: модуль НЕ импортирует cs/core.py (чтобы не ловить циклические импорты).

Правила (как раньше):
- если price_in отсутствует/<=100 → 100
- если >= 9_000_000 → 100
- наценка 4% + tier adder
- хвост всегда "...900"
"""

from __future__ import annotations

import os
import re

def safe_int(v) -> int | None:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        s = s.replace(" ", "").replace("\u00a0", "")
        # иногда цена приходит как "12 345.00"
        s = s.split(".")[0]
        return int(s)
    except Exception:
        return None


# Парсит множество id из env (например "1,10,20") или из fallback списка

CS_PRICE_TIERS = [
    (101, 10_000, 3_000),
    (10_001, 25_000, 4_000),
    (25_001, 50_000, 5_000),
    (50_001, 75_000, 7_000),
    (75_001, 100_000, 10_000),
    (100_001, 150_000, 12_000),
    (150_001, 200_000, 15_000),
    (200_001, 300_000, 20_000),
    (300_001, 500_000, 25_000),
    (500_001, 750_000, 30_000),
    (750_001, 1_000_000, 35_000),
    (1_000_001, 1_500_000, 40_000),
    (1_500_001, 2_000_000, 45_000),
]


def compute_price(price_in: int | None) -> int:
    p = safe_int(price_in)
    if p is None or p <= 100:
        return 100
    if p >= 9_000_000:
        return 100

    tiers = CS_PRICE_TIERS
    add = 60_000
    for lo, hi, a in tiers:
        if lo <= p <= hi:
            add = a
            break

    raw = int(p * 1.04 + add)

    # "хвост 900" (всегда заканчиваем на 900)
    out = (raw // 1000) * 1000 + 900

    if out >= 9_000_000:
        return 100
    if out <= 100:
        return 100
    return out


# Убирает мусорные параметры, пустые значения и дубли (применять всегда!)

# Параметры "вес/габариты/объем" полезны покупателю, но у некоторых поставщиков бывают мусорные значения.
# Валидируем мягко: оставляем только "похожие на правду".
_DIM_WORDS = ("габарит", "размер", "длина", "ширина", "высота")
_VOL_WORDS = ("объем", "объём", "volume")
_WGT_WORDS = ("вес", "масса", "weight")

_RE_NUM = re.compile(r"(\d+(?:[\.,]\d+)?)")
_RE_DIM_SEP = re.compile(r"[xх×\*]", re.I)
