# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/params_page.py
CopyLine page-params layer.

Задача:
- нормализовать page params из HTML-таблиц/описания;
- поднять самые полезные supplier-specific поля до raw:
  Технология печати, Цвет, Ресурс, Коды расходников, Совместимость.

Это ещё не финальный compat-layer, а только page/source-of-truth stage.
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

_CABLE_KEYS = {
    "Тип кабеля",
    "Количество пар",
    "Толщина проводников",
    "Категория",
    "Назначение",
    "Материал изоляции",
    "Бухта",
}


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""



def _title_kind(title: str) -> str:
    t = safe_str(title).lower()
    if not t:
        return ""
    if t.startswith("тонер-картридж") or t.startswith("тонер картридж"):
        return "Тонер-картридж"
    if t.startswith("картридж"):
        return "Картридж"
    if t.startswith("кабель сетевой"):
        return "Кабель сетевой"
    if t.startswith("термоблок"):
        return "Термоблок"
    if t.startswith("термоэлемент"):
        return "Термоэлемент"
    if t.startswith("девелопер") or t.startswith("developer"):
        return "Девелопер"
    if t.startswith("драм") or t.startswith("drum"):
        return "Драм-картридж"
    if t.startswith("чернила"):
        return "Чернила"
    return ""


KEY_MAP = {
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



def _norm_color(val: str) -> str:
    s = safe_str(val)
    repl = {
        "black": "Чёрный",
        "yellow": "Желтый",
        "magenta": "Пурпурный",
        "cyan": "Голубой",
    }
    if not s:
        return ""
    low = s.casefold()
    for k, v in repl.items():
        if low == k:
            return v
    return s[:120]



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
    s = _normalize_code_token(text)
    s = re.sub(r"\s+", " ", s)
    return s.strip()



def _dedupe_params(items: Sequence[Tuple[str, str]]) -> List[Tuple[str, str]]:
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



def _extract_codes(title: str, description: str) -> str:
    text = _normalize_code_search_text(" ".join([safe_str(title), safe_str(description)]))
    found: list[str] = []
    seen: set[str] = set()
    for m in CODE_RX.finditer(text):
        val = _normalize_code_token(m.group(0))
        if not val or val.isdigit() or len(val) < 4:
            continue
        if val in seen:
            continue
        seen.add(val)
        found.append(val)
    return ", ".join(found[:6])



def _extract_compat_from_desc(description: str) -> str:
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
            return val[:320]
    return ""



def extract_page_params(
    *,
    title: str,
    description: str,
    page_params: Sequence[Tuple[str, str]] | None = None,
) -> List[Tuple[str, str]]:
    """Нормализовать page params и поднять supplier-полезные значения."""
    page_params = page_params or []
    out: list[Tuple[str, str]] = []

    kind = _title_kind(title)
    if kind:
        out.append(("Тип", kind))

    for key, value in page_params:
        k = safe_str(key).casefold()
        v = safe_str(value)
        if not k or not v:
            continue
        norm_key = KEY_MAP.get(k, "")
        if not norm_key:
            continue
        if norm_key == "Цвет":
            v = _norm_color(v)
        elif kind == "Кабель сетевой" and norm_key in _CABLE_KEYS:
            v = v.strip()
        out.append((norm_key, v))

    compat = _extract_compat_from_desc(description)
    if compat:
        out.append(("Совместимость", compat))

    codes = _extract_codes(title, description)
    if codes:
        out.append(("Коды расходников", codes))

    title_low = safe_str(title).lower()
    if "yellow" in title_low and not any(k == "Цвет" for k, _ in out):
        out.append(("Цвет", "Желтый"))
    if "magenta" in title_low and not any(k == "Цвет" for k, _ in out):
        out.append(("Цвет", "Пурпурный"))
    if "black" in title_low and not any(k == "Цвет" for k, _ in out):
        out.append(("Цвет", "Чёрный"))

    return _dedupe_params(out)
