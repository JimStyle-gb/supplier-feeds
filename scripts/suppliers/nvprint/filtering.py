# -*- coding: utf-8 -*-
"""NVPrint filtering layer: safe step1 split."""

from __future__ import annotations

import re

_RE_WS = re.compile(r"\s+")

_LAT2CYR = {
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


def fix_mixed_ru(s: str) -> str:
    """Латиница -> кириллица только в русских словах."""
    if not s:
        return ""
    out = []
    n = len(s)
    for i, ch in enumerate(s):
        rep = ch
        if ch in _LAT2CYR and i + 1 < n:
            nxt = s[i + 1]
            if "\u0400" <= nxt <= "\u04FF":
                rep = _LAT2CYR[ch]
        out.append(rep)
    return "".join(out)


def name_for_filter(name: str) -> str:
    s = (name or "").strip()
    s = fix_mixed_ru(s)
    s = s.casefold()
    s = _RE_WS.sub(" ", s)
    return s


def include_by_name(name: str, base_prefixes: list[str], extra_env: str = "") -> bool:
    cf = name_for_filter(name)
    if not cf:
        return False

    prefixes = list(base_prefixes)
    extra = (extra_env or "").strip()
    if extra:
        for x in extra.split(","):
            x = x.strip().casefold()
            if x and x not in prefixes:
                prefixes.append(x)

    return any(p and cf.startswith(p) for p in prefixes)
