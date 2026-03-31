# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/desc_extract.py
CopyLine description-extract layer.

Задача:
- only-fill-missing слой поверх params_page extractor;
- поднимать missing params из body-description;
- не держать второй дублирующий regex/extractor-комбайн;
- не тянуть device-list в Коды расходников.
"""

from __future__ import annotations

import re
from typing import List, Sequence, Tuple

from suppliers.copyline.params_page import (
    CABLE_CATEGORY_RX,
    CABLE_DIM_RX,
    CABLE_MATERIAL_RX,
    CABLE_SPOOL_RX,
    CABLE_TYPE_RX,
    _extract_codes,
    _extract_compat_from_desc,
    _norm_spaces,
    _trim_compat_tail,
    safe_str,
)

TECH_PAIR_HEADERS = {
    "технология печати": "Технология печати",
    "цвет печати": "Цвет",
    "цвет": "Цвет",
    "количество страниц (5% заполнение)": "Количество страниц (5% заполнение)",
    "количество страниц": "Количество страниц (5% заполнение)",
    "ресурс": "Ресурс",
    "совместимость": "Совместимость",
    "тип кабеля": "Тип кабеля",
    "количество пар": "Количество пар",
    "толщина проводников": "Толщина проводников",
    "категория": "Категория",
    "материал изоляции": "Материал изоляции",
    "бухта": "Бухта",
}

CABLE_KEYS = {
    "Тип кабеля",
    "Количество пар",
    "Толщина проводников",
    "Категория",
    "Материал изоляции",
    "Бухта",
}

CABLE_CONTEXT_RX = re.compile(r"(?:кабель\s+сетевой|витая\s+пара)", re.I)
TITLE_CABLE_RX = re.compile(r"^кабель\s+сетевой", re.I)

_COLOR_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?iu)\bч[её]рн(?:ый|ая|ое|ого|ому|ым|ом)?\b|\bblack\b"), "Чёрный"),
    (re.compile(r"(?iu)\bпурпурн(?:ый|ая|ое|ого|ому|ым|ом)?\b|\bmagenta\b"), "Пурпурный"),
    (re.compile(r"(?iu)\bжелт(?:ый|ая|ое|ого|ому|ым|ом)?\b|\byellow\b"), "Желтый"),
    (re.compile(r"(?iu)\bголуб(?:ой|ая|ое|ого|ому|ым|ом)?\b|\bcyan\b"), "Голубой"),
    (re.compile(r"(?iu)\bсин(?:ий|яя|ее|его|ему|им|ем)\b|\bblue\b"), "Синий"),
)


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


def _is_cable_context(title: str, text: str) -> bool:
    title = safe_str(title)
    text = safe_str(text)
    return bool(TITLE_CABLE_RX.search(title) or CABLE_CONTEXT_RX.search(text))


def _extract_inline_pair(line: str, *, is_cable: bool) -> tuple[str, str] | None:
    for sep in (":", " - "):
        if sep not in line:
            continue
        left, right = line.split(sep, 1)
        key = TECH_PAIR_HEADERS.get(safe_str(left).casefold(), "")
        value = _norm_spaces(right)
        if not key or not value or len(value) > 240:
            continue
        if key in CABLE_KEYS and not is_cable:
            continue
        return key, value
    return None


def _extract_cable_params_from_text(text: str, *, is_cable: bool) -> list[Tuple[str, str]]:
    if not is_cable:
        return []
    text = _norm_spaces(text)
    out: list[Tuple[str, str]] = []

    m = CABLE_TYPE_RX.search(text)
    if m:
        out.append(("Тип кабеля", m.group(1).upper()))

    m = CABLE_CATEGORY_RX.search(text)
    if m:
        out.append(("Категория", f"Cat.{m.group(1)}"))

    m = CABLE_DIM_RX.search(text)
    if m:
        out.append(("Количество пар", m.group(1)))
        out.append(("Толщина проводников", m.group(2).replace('.', ',')))

    m = CABLE_MATERIAL_RX.search(text)
    if m:
        out.append(("Материал изоляции", m.group(1).upper()))

    m = CABLE_SPOOL_RX.search(text)
    if m:
        out.append(("Бухта", f"{m.group(1)} м/б"))

    return out


def _extract_line_pairs(description: str, *, title: str) -> list[Tuple[str, str]]:
    lines = [safe_str(x) for x in re.split(r"\n+", description) if safe_str(x)]
    out: list[Tuple[str, str]] = []
    joined = " ".join(lines)
    is_cable = _is_cable_context(title, joined)

    for line in lines:
        pair = _extract_inline_pair(line, is_cable=is_cable)
        if pair:
            out.append(pair)

    for i in range(len(lines) - 1):
        k = lines[i].casefold()
        v = _norm_spaces(lines[i + 1])
        norm_key = TECH_PAIR_HEADERS.get(k, "")
        if not norm_key:
            continue
        if norm_key in CABLE_KEYS and not is_cable:
            continue
        if len(v) > 240:
            continue
        out.append((norm_key, v))

    out.extend(_extract_cable_params_from_text(joined, is_cable=is_cable))
    return out


def _extract_color_from_text(title: str, description: str) -> str:
    text = _norm_spaces(f"{safe_str(title)} {safe_str(description)}")
    if not text:
        return ""
    for rx, value in _COLOR_PATTERNS:
        if rx.search(text):
            return value
    return ""


def extract_desc_params(*, title: str, description: str, existing_params: Sequence[Tuple[str, str]] | None = None) -> List[Tuple[str, str]]:
    """Поднять missing params из body-description."""
    existing_params = existing_params or []
    existing_keys = {safe_str(k).casefold() for k, _ in existing_params if safe_str(k)}
    out: list[Tuple[str, str]] = []

    for k, v in _extract_line_pairs(description, title=title):
        if k.casefold() in existing_keys:
            continue
        out.append((k, v))

    color = _extract_color_from_text(title, description)
    if color and "цвет" not in existing_keys:
        out.append(("Цвет", color))

    compat = _extract_compat_from_desc(description)
    if not compat and re.search(r"(?:Panasonic|INTEGRAL)", title + " " + description, re.I):
        m = re.search(r"(?:для|used in|совместим(?:ость)? с)\s+((?:Panasonic|INTEGRAL)[^.;\n]{3,180})", _norm_spaces(description), re.I)
        if m:
            compat = _trim_compat_tail(m.group(1))
    if compat and "совместимость" not in existing_keys:
        out.append(("Совместимость", compat))

    codes = _extract_codes(title, description)
    if codes and "коды расходников" not in existing_keys:
        out.append(("Коды расходников", codes))

    return _dedupe(out)
