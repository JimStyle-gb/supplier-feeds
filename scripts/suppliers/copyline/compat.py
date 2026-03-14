# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/compat.py
CopyLine compat/reconcile layer.

Задача:
- нормализовать codes/compat lists;
- дочистить supplier-params до аккуратного raw.
"""

from __future__ import annotations

import re
from typing import List, Sequence, Tuple


VENDOR_FAMILIES = (
    "HP Color LaserJet",
    "HP LaserJet",
    "HP LaserJet Pro",
    "HP Color LaserJet Pro",
    "Canon i-SENSYS",
    "Canon imageRUNNER",
    "Canon imageCLASS",
    "Kyocera ECOSYS",
    "Brother HL",
    "Brother DCP",
    "Brother MFC",
)



def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""



def _dedupe_list_text(value: str, sep: str = ", ") -> str:
    raw = re.split(r"\s*,\s*|\s*/\s*", safe_str(value))
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        val = re.sub(r"\s+", " ", item).strip(" ,.;/")
        if not val:
            continue
        sig = val.casefold()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(val)
    return sep.join(out)



def normalize_codes(value: str) -> str:
    return _dedupe_list_text(value, sep=", ")



def normalize_compatibility(value: str) -> str:
    s = safe_str(value)
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s)
    # Если уже идёт список через запятую — дедупим и оставляем.
    if "," in s:
        return _dedupe_list_text(s, sep=", ")[:500]

    # Частый CopyLine-кейс: первое семейство + список моделей без повторения префикса.
    for fam in VENDOR_FAMILIES:
        if s.startswith(fam + " "):
            tail = s[len(fam):].strip()
            items = [x.strip() for x in tail.split(",") if x.strip()]
            if not items:
                return s[:500]
            rebuilt: list[str] = []
            for item in items:
                if item.startswith(fam):
                    rebuilt.append(item)
                else:
                    rebuilt.append(f"{fam} {item}")
            return _dedupe_list_text(", ".join(rebuilt), sep=", ")[:500]
    return s[:500]



def reconcile_copyline_params(params: Sequence[Tuple[str, str]]) -> List[Tuple[str, str]]:
    out: list[Tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for k, v in params:
        key = safe_str(k)
        val = safe_str(v)
        if not key or not val:
            continue
        if key == "Коды расходников":
            val = normalize_codes(val)
        elif key == "Совместимость":
            val = normalize_compatibility(val)
        sig = (key.casefold(), val.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((key, val))
    return out
