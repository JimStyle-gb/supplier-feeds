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



def _normalize_list_separators(value: str) -> str:
    s = safe_str(value)
    s = s.replace("|", ",")
    s = s.replace(";", ",")
    s = re.sub(r"\s+,\s+", ", ", s)
    s = re.sub(r",{2,}", ",", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip(" ,")



def _dedupe_list_text(value: str, sep: str = ", ") -> str:
    raw = [x for x in re.split(r"\s*,\s*", _normalize_list_separators(value)) if x]
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



def _expand_family_shorthand(value: str) -> str:
    s = safe_str(value)
    if not s:
        return ""

    # Xerox Phaser 5500/5550 -> Xerox Phaser 5500, Xerox Phaser 5550
    s = re.sub(
        r"\b(Xerox\s+Phaser)\s+(\d{3,5})\s*/\s*(\d{3,5})\b",
        lambda m: f"{m.group(1)} {m.group(2)}, {m.group(1)} {m.group(3)}",
        s,
        flags=re.I,
    )

    # Xerox Phaser 6510 / WC 6515 -> Xerox Phaser 6510, Xerox WorkCentre 6515
    s = re.sub(
        r"\b(Xerox\s+Phaser)\s+(\d{3,5})\s*/\s*WC\s*(\d{3,5})\b",
        lambda m: f"{m.group(1)} {m.group(2)}, Xerox WorkCentre {m.group(3)}",
        s,
        flags=re.I,
    )

    # Canon iR C2020/C2025/C2030...
    canon_rx = re.compile(
        r"\b(Canon\s+iR(?:\s+ADVANCE)?)\s+([A-Z]?\d{3,5}(?:\s*/\s*[A-Z]?\d{3,5}){1,})\b",
        re.I,
    )

    def _canon_repl(m: re.Match[str]) -> str:
        fam = m.group(1)
        parts = [x.strip() for x in re.split(r"\s*/\s*", m.group(2)) if x.strip()]
        return ", ".join(f"{fam} {part}" for part in parts)

    s = canon_rx.sub(_canon_repl, s)
    return s



def normalize_codes(value: str) -> str:
    value = _normalize_list_separators(value)
    return _dedupe_list_text(value, sep=", ")



def normalize_compatibility(value: str) -> str:
    s = safe_str(value)
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s)
    s = _expand_family_shorthand(s)
    s = _normalize_list_separators(s)

    if "," in s:
        parts = [x.strip() for x in re.split(r"\s*,\s*", s) if x.strip()]
        if parts:
            first = parts[0]
            family = ""
            for fam in VENDOR_FAMILIES:
                if first.lower().startswith(fam.lower() + " "):
                    family = fam
                    break
            rebuilt: list[str] = []
            for item in parts:
                if family and not any(item.lower().startswith(f.lower() + " ") for f in VENDOR_FAMILIES):
                    if item.upper().startswith("WC "):
                        rebuilt.append("Xerox WorkCentre " + item[3:].strip())
                    else:
                        rebuilt.append(f"{family} {item}")
                else:
                    rebuilt.append(item)
            return _dedupe_list_text(", ".join(rebuilt), sep=", ")[:500]

    for fam in VENDOR_FAMILIES:
        if s.lower().startswith(fam.lower() + " "):
            tail = s[len(fam):].strip()
            items = [x.strip() for x in re.split(r"\s*/\s*|\s*,\s*", tail) if x.strip()]
            if not items:
                return s[:500]
            rebuilt: list[str] = []
            for item in items:
                if item.lower().startswith(tuple(f.lower() for f in VENDOR_FAMILIES)):
                    rebuilt.append(item)
                elif item.upper().startswith("WC "):
                    rebuilt.append("Xerox WorkCentre " + item[3:].strip())
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
