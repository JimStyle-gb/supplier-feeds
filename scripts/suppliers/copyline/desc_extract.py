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
    r"\b(?:CF\d{3,4}[A-Z]|CE\d{3,4}[A-Z]|CB\d{3,4}[A-Z]|Q\d{4}[A-Z]|W\d{4}[A-Z0-9]{1,4}|"
    r"113R\d{5}|108R\d{5}|106R\d{5}|006R\d{5}|"
    r"TK-?\d{3,5}[A-Z0-9]*|MLT-[A-Z]\d{3,5}[A-Z0-9/]*|CLT-[A-Z]\d{3,5}[A-Z]?|"
    r"KX-FA\d+[A-Z]?|KX-FAT\d+[A-Z]?|"
    r"C13T\d{5,8}[A-Z0-9]*|C12C\d{5,8}[A-Z0-9]*|C33S\d{5,8}[A-Z0-9]*|"
    r"C-?EXV\d+[A-Z]*|DR-\d+[A-Z0-9-]*|TN-\d+[A-Z0-9-]*)\b",
    re.I,
)

COMPAT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?:используется\s+в\s+принтерах\s+серий|используется\s+в\s+принтерах)\s+", re.I),
    re.compile(r"(?:для\s+принтеров\s+серий|для\s+принтеров)\s+", re.I),
    re.compile(r"применяется\s+в\s+мфу\s+", re.I),
    re.compile(r"применяется\s+в\s+", re.I),
    re.compile(r"совместимость\s+с\s+устройствами\s*:?\s*", re.I),
    re.compile(r"совместим\s+с\s+", re.I),
    re.compile(r"подходит\s+для\s+", re.I),
    re.compile(r"используется\s+с\s+", re.I),
    re.compile(r"для\s+устройств\s+", re.I),
    re.compile(r"для\s+аппаратов\s+", re.I),
)

_TECH_STOP_RX = re.compile(
    r"(?:\n\n+|(?:^|\n)(?:технические\s+характеристики|характеристика|основные\s+характеристики|характеристики|ресурс|цвет|технология\s+печати)\b)",
    re.I,
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
    "назначение": "Назначение",
    "материал изоляции": "Материал изоляции",
    "бухта": "Бухта",
}



def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""



def _normalize_code_token(s: str) -> str:
    s = safe_str(s).upper()
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s*[-–—]\s*", "-", s)
    s = re.sub(r"\b(113R|108R|106R|006R|C13T|C12C|C33S)\s+(\d{5,8}[A-Z0-9]*)\b", r"\1\2", s)
    s = re.sub(r"\b(CLT|MLT|TK|TN|DR|KX)\s*-\s*", r"\1-", s)
    s = re.sub(r"\bKX\s+(FA|FAT)(\d+[A-Z]?)\b", r"KX-\1\2", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()



def _normalize_code_search_text(text: str) -> str:
    return _normalize_code_token(text)



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
    d = re.sub(r"\s+", " ", d).strip()
    for rx in COMPAT_PATTERNS:
        m = rx.search(d)
        if not m:
            continue
        tail = d[m.end():]
        stop = _TECH_STOP_RX.search(tail)
        if stop:
            tail = tail[: stop.start()]
        tail = re.split(r"(?<=[.!?])\s", tail, maxsplit=1)[0]
        val = re.sub(r"\s+", " ", tail).strip(" ,.;")
        if val:
            return val[:400]
    return ""



def _extract_codes(text: str) -> str:
    hay = _normalize_code_search_text(text)
    found: list[str] = []
    seen: set[str] = set()
    for m in CODE_RX.finditer(hay):
        code = _normalize_code_token(m.group(0))
        if not code or code.isdigit() or len(code) < 4:
            continue
        if code in seen:
            continue
        seen.add(code)
        found.append(code)
    return ", ".join(found[:6])



def _extract_line_pairs(description: str) -> list[Tuple[str, str]]:
    out: list[Tuple[str, str]] = []
    text = safe_str(description)
    if not text:
        return out

    lines = [safe_str(x) for x in re.split(r"\n+", text) if safe_str(x)]

    for line in lines:
        m = re.match(r"^([^:]{1,80})\s*:\s*(.+)$", line)
        if not m:
            m = re.match(r"^([^\-]{1,80})\s+-\s+(.+)$", line)
        if not m:
            continue
        key = safe_str(m.group(1)).casefold()
        val = safe_str(m.group(2))
        norm_key = TECH_PAIR_HEADERS.get(key, "")
        if not norm_key or not val or len(val) > 240:
            continue
        out.append((norm_key, val))

    for i in range(len(lines) - 1):
        k = lines[i].casefold()
        v = lines[i + 1]
        norm_key = TECH_PAIR_HEADERS.get(k, "")
        if not norm_key or not v or len(v) > 240:
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
