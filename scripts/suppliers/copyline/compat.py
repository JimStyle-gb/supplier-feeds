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
    "Canon iR",
    "Canon iR ADVANCE",
    "Canon imageRUNNER",
    "Canon imageRUNNER ADVANCE",
    "Canon imageCLASS",
    "Kyocera ECOSYS",
    "Brother HL",
    "Brother DCP",
    "Brother MFC",
    "Xerox Phaser",
    "Xerox WorkCentre",
    "Xerox Color Phaser",
    "Samsung CLP",
    "Samsung CLX",
    "Samsung Xpress",
    "Panasonic KX",
    "RISO",
    "Ricoh SP",
    "RICOH Aficio",
)



def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""



def _normalize_spaces(value: str) -> str:
    value = safe_str(value)
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()



def _normalize_list_separators(value: str) -> str:
    s = _normalize_spaces(value)
    s = s.replace(";", ",")
    s = s.replace("|", ",")
    s = re.sub(r",{2,}", ",", s)
    s = re.sub(r"\s*,\s*", ", ", s)
    return s.strip(" ,")



def _dedupe_list(items: Sequence[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        val = _normalize_spaces(item).strip(" ,.;/")
        if not val:
            continue
        sig = val.casefold()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(val)
    return out



def _expand_family_shorthand(value: str) -> str:
    s = _normalize_spaces(value)
    if not s:
        return ""

    for fam in VENDOR_FAMILIES:
        prefix = fam + " "
        if not s.startswith(prefix):
            continue
        tail = s[len(prefix):].strip()
        if not tail:
            return s

        if "/" in tail and "," not in tail:
            nums = [x.strip() for x in tail.split("/") if x.strip()]
            if len(nums) > 1 and all(re.fullmatch(r"[A-Z0-9-]+", x) for x in nums):
                return ", ".join([f"{fam} {x}" for x in nums])

        if "," in tail:
            parts = [x.strip() for x in tail.split(",") if x.strip()]
            rebuilt: list[str] = []
            for part in parts:
                if part.startswith(fam):
                    rebuilt.append(part)
                elif re.match(r"^(?:WC|WorkCentre|Phaser|CLP|CLX|Xpress|iR|imageRUNNER)\b", part, flags=re.I):
                    brand = fam.split()[0]
                    rebuilt.append(f"{brand} {part}")
                else:
                    rebuilt.append(f"{fam} {part}")
            return ", ".join(rebuilt)

    if re.search(r"\bPhaser\s+\d+\s*/\s*WC\s*\d+\b", s, flags=re.I):
        m = re.search(r"Phaser\s+(\d+)\s*/\s*WC\s*(\d+)", s, flags=re.I)
        if m:
            return f"Xerox Phaser {m.group(1)}, Xerox WorkCentre {m.group(2)}"

    return s



def normalize_codes(value: str) -> str:
    s = _normalize_list_separators(value)
    if not s:
        return ""
    items = [x.strip() for x in re.split(r"\s*,\s*", s) if x.strip()]
    return ", ".join(_dedupe_list(items))



def normalize_compatibility(value: str) -> str:
    s = _normalize_spaces(value)
    if not s:
        return ""
    s = _expand_family_shorthand(s)
    s = _normalize_list_separators(s)

    parts = [x.strip() for x in re.split(r"\s*,\s*", s) if x.strip()]
    rebuilt: list[str] = []
    last_family = ""
    for part in parts:
        piece = _normalize_spaces(part)
        if not piece:
            continue
        matched_family = next((fam for fam in VENDOR_FAMILIES if piece.startswith(fam + " ") or piece == fam), "")
        if matched_family:
            last_family = matched_family
            rebuilt.append(piece)
            continue
        if last_family and re.fullmatch(r"[A-Z0-9-]+(?:\s+[A-Z0-9-]+)*", piece):
            rebuilt.append(f"{last_family} {piece}")
            continue
        rebuilt.append(piece)

    return ", ".join(_dedupe_list(rebuilt))[:500]



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
