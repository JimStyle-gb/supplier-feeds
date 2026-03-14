# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/desc_extract.py
CopyLine description-extract layer.

Задача:
- поднимать недостающие supplier-полезные params из body-description;
- не дублировать page params, а only-fill-missing.
"""

from __future__ import annotations

import re
from typing import List, Sequence, Tuple


CODE_RX = re.compile(
    r"\b(?:CF\d{3,4}[A-Z]|CE\d{3,4}[A-Z]|CB\d{3,4}[A-Z]|Q\d{4}[A-Z]|W\d{4}[A-Z0-9]{1,4}|106R\d{5}|006R\d{5}|TK-?\d{3,5}[A-Z0-9]*|MLT-[A-Z]\d{3,5}[A-Z0-9/]*|C-?EXV\d+[A-Z]*|DR-\d+[A-Z0-9-]*|TN-\d+[A-Z0-9-]*)\b",
    re.I,
)

DEVICE_LINE_RX = re.compile(
    r"(?:используется\s+в\s+принтерах\s+серий|используется\s+в\s+принтерах|для\s+принтеров\s+серий|для\s+принтеров)\s+(.+?)(?:\.|$)",
    re.I | re.S,
)


TECH_PAIR_HEADERS = {
    "технология печати": "Технология печати",
    "цвет печати": "Цвет",
    "цвет": "Цвет",
    "количество страниц (5% заполнение)": "Количество страниц (5% заполнение)",
    "количество страниц": "Количество страниц (5% заполнение)",
    "ресурс": "Ресурс",
    "совместимость": "Совместимость",
}



def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""



def _dedupe(items: Sequence[Tuple[str, str]]) -> list[Tuple[str, str]]:
    out: list[Tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for k, v in items:
        k2 = safe_str(k)
        v2 = safe_str(v)
        if not k2 or not v2:
            continue
        sig = (k2.casefold(), v2.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((k2, v2))
    return out



def _extract_compat(description: str) -> str:
    d = safe_str(description)
    if not d:
        return ""
    m = DEVICE_LINE_RX.search(d)
    if not m:
        return ""
    val = re.sub(r"\s+", " ", m.group(1)).strip(" ,.;")
    return val[:400]



def _extract_codes(text: str) -> str:
    found: list[str] = []
    seen: set[str] = set()
    for m in CODE_RX.finditer(text):
        code = m.group(0).upper().replace(" ", "")
        if code in seen:
            continue
        seen.add(code)
        found.append(code)
    return ", ".join(found[:6])



def _extract_line_pairs(description: str) -> list[Tuple[str, str]]:
    lines = [safe_str(x) for x in re.split(r"\n+", description) if safe_str(x)]
    out: list[Tuple[str, str]] = []
    for i in range(len(lines) - 1):
        k = lines[i].casefold()
        v = lines[i + 1]
        norm_key = TECH_PAIR_HEADERS.get(k, "")
        if not norm_key:
            continue
        if len(v) > 240:
            continue
        out.append((norm_key, v))
    return out



def extract_desc_params(*, title: str, description: str, existing_params: Sequence[Tuple[str, str]] | None = None) -> List[Tuple[str, str]]:
    """Поднять missing params из body-description."""
    existing_params = existing_params or []
    existing_keys = {safe_str(k).casefold() for k, _ in existing_params if safe_str(k)}
    out: list[Tuple[str, str]] = []

    for k, v in _extract_line_pairs(description):
        if k.casefold() in existing_keys:
            continue
        out.append((k, v))

    compat = _extract_compat(description)
    if compat and "совместимость" not in existing_keys:
        out.append(("Совместимость", compat))

    codes = _extract_codes(" ".join([safe_str(title), safe_str(description)]))
    if codes and "коды расходников" not in existing_keys:
        out.append(("Коды расходников", codes))

    return _dedupe(out)
