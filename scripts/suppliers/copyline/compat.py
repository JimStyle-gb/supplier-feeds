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

STOP_HEADERS_RX = re.compile(
    r"(?:^|\b)(?:Производитель|Размер(?:\s+упаковки)?|Вес(?:\s+в\s+упаковке)?|Технические\s+характеристики|"
    r"Основные\s+характеристики|Характеристики|Артикул|Код\s+товара|Ресурс|Количество\s+страниц|"
    r"Цвет(?:\s+печати)?|Технология\s+печати|Тип\s+кабеля|Количество\s+пар|Толщина\s+проводников|"
    r"Категория|Назначение|Материал\s+изоляции|Бухта)\b",
    re.I,
)


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def _normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", safe_str(value)).strip()


def _normalize_list_separators(value: str) -> str:
    s = _normalize_spaces(value)
    s = s.replace("|", ",")
    s = s.replace(";", ",")
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r",{2,}", ",", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip(" ,")


def _strip_compat_leadins(value: str) -> str:
    s = _normalize_spaces(value)
    if not s:
        return ""
    stop = STOP_HEADERS_RX.search(s)
    if stop:
        s = s[: stop.start()].strip()
    s = re.split(r"(?:\.|\n\n)", s, maxsplit=1)[0]
    s = re.sub(
        r"^(?:в\s+)?(?:многофункциональных|лазерных|струйных)?\s*"
        r"(?:принтерах|мфу|устройствах|аппаратах)\s+",
        "",
        s,
        flags=re.I,
    )
    s = s.strip(" ,.;:-")
    return s


def _dedupe_list_text(value: str, sep: str = ", ") -> str:
    raw = [x for x in re.split(r"\s*,\s*", _normalize_list_separators(value)) if x]
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        val = _normalize_spaces(item).strip(" ,.;/")
        if not val:
            continue
        sig = val.casefold()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(val)
    return sep.join(out)


def _looks_like_family(item: str) -> bool:
    low = item.casefold()
    return any(low.startswith(f.casefold() + " ") for f in VENDOR_FAMILIES)


def _canon_expand(family: str, tail: str) -> str:
    parts = [x.strip() for x in re.split(r"\s*/\s*", tail) if x.strip()]
    if len(parts) <= 1:
        return f"{family} {tail}"
    return ", ".join(f"{family} {part}" for part in parts)


def _expand_family_shorthand(value: str) -> str:
    s = _normalize_spaces(value)
    if not s:
        return ""

    s = re.sub(
        r"\b(Xerox\s+Phaser)\s+(\d{3,5})\s*/\s*(\d{3,5})\b",
        lambda m: f"{m.group(1)} {m.group(2)}, {m.group(1)} {m.group(3)}",
        s,
        flags=re.I,
    )

    s = re.sub(
        r"\b(Xerox\s+Phaser)\s+(\d{3,5})\s*/\s*WC\s*(\d{3,5})\b",
        lambda m: f"{m.group(1)} {m.group(2)}, Xerox WorkCentre {m.group(3)}",
        s,
        flags=re.I,
    )

    s = re.sub(
        r"\b(Xerox\s+Phaser)\s+(\d{3,5})\s*,\s*(\d{3,5})\b",
        lambda m: f"{m.group(1)} {m.group(2)}, {m.group(1)} {m.group(3)}",
        s,
        flags=re.I,
    )

    s = re.sub(
        r"\b(Xerox\s+Phaser)\s+(\d{3,5})\s*,\s*WC\s*(\d{3,5})\b",
        lambda m: f"{m.group(1)} {m.group(2)}, Xerox WorkCentre {m.group(3)}",
        s,
        flags=re.I,
    )

    canon_rx = re.compile(
        r"\b(Canon\s+iR(?:\s+ADVANCE)?)\s+([A-Z]?\d{3,5}(?:\s*/\s*[A-Z]?\d{3,5}){1,})\b",
        re.I,
    )
    s = canon_rx.sub(lambda m: _canon_expand(m.group(1), m.group(2)), s)
    return s


def normalize_codes(value: str) -> str:
    value = _normalize_list_separators(value)
    return _dedupe_list_text(value, sep=", ")


def normalize_compatibility(value: str) -> str:
    s = _strip_compat_leadins(value)
    if not s:
        return ""
    s = _expand_family_shorthand(s)
    s = _normalize_list_separators(s)

    if "," in s:
        parts = [x.strip() for x in re.split(r"\s*,\s*", s) if x.strip()]
        if parts:
            family = ""
            first = parts[0]
            for fam in VENDOR_FAMILIES:
                if first.casefold().startswith(fam.casefold() + " "):
                    family = fam
                    break
            rebuilt: list[str] = []
            for item in parts:
                item = _strip_compat_leadins(item)
                if not item:
                    continue
                if item.upper().startswith("WC "):
                    rebuilt.append("Xerox WorkCentre " + item[3:].strip())
                elif family and not _looks_like_family(item):
                    rebuilt.append(f"{family} {item}")
                else:
                    rebuilt.append(item)
            return _dedupe_list_text(", ".join(rebuilt), sep=", ")[:500]

    for fam in VENDOR_FAMILIES:
        if s.casefold().startswith(fam.casefold() + " "):
            tail = s[len(fam):].strip()
            items = [x.strip() for x in re.split(r"\s*/\s*|\s*,\s*", tail) if x.strip()]
            if not items:
                return s[:500]
            rebuilt: list[str] = []
            for item in items:
                item = _strip_compat_leadins(item)
                if not item:
                    continue
                if _looks_like_family(item):
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
