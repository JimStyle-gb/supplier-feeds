# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/nvprint/filtering.py
NVPrint filtering layer — фильтр ассортимента по префиксу названия.
"""

from __future__ import annotations

import os
import re


RE_WS = re.compile(r"\s+")

LAT2CYR = {
    "A": "А", "a": "а",
    "B": "В", "b": "в",
    "C": "С", "c": "с",
    "E": "Е", "e": "е",
    "H": "Н", "h": "н",
    "K": "К", "k": "к",
    "M": "М", "m": "м",
    "O": "О", "o": "о",
    "P": "Р", "p": "р",
    "T": "Т", "t": "т",
    "X": "Х", "x": "х",
    "Y": "У", "y": "у",
}

NVPRINT_INCLUDE_PREFIXES_CF = [
    "блок фотобарабана",
    "картридж",
    "печатающая головка",
    "струйный картридж",
    "тонер-картридж",
    "тонер картридж",
    "тонер-туба",
    "тонер туба",
]



def fix_mixed_ru(s: str) -> str:
    """Починить латиницу внутри русских слов."""
    if not s:
        return ""
    out = []
    n = len(s)
    for i, ch in enumerate(s):
        rep = ch
        if ch in LAT2CYR and i + 1 < n:
            nxt = s[i + 1]
            if "\u0400" <= nxt <= "\u04FF":
                rep = LAT2CYR[ch]
        out.append(rep)
    return "".join(out)



def name_for_filter(name: str) -> str:
    """Нормализовать имя для фильтра."""
    s = (name or "").strip()
    s = fix_mixed_ru(s)
    s = s.casefold()
    s = RE_WS.sub(" ", s)
    return s



def include_by_name(name: str) -> bool:
    """Проверить, входит ли товар в ассортимент."""
    cf = name_for_filter(name)
    if not cf:
        return False

    extra = (os.environ.get("NVPRINT_INCLUDE_PREFIXES") or "").strip()
    prefixes = list(NVPRINT_INCLUDE_PREFIXES_CF)
    if extra:
        for x in extra.split(","):
            x = x.strip().casefold()
            if x and x not in prefixes:
                prefixes.append(x)

    for p in prefixes:
        if p and cf.startswith(p):
            return True
    return False
