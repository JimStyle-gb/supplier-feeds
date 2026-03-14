# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/desc_clean.py
CopyLine description-clean layer.

Задача:
- убрать сырые supplier-заголовки;
- отделить narrative-описание от техблока;
- оставить raw-body уже аккуратным до core.
"""

from __future__ import annotations

import re
from typing import Iterable


TECH_HEADERS = (
    "Технические характеристики",
    "Основные характеристики",
    "Характеристики",
    "Описание",
)


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""



def _norm_spaces(s: str) -> str:
    s = safe_str(s).replace("\xa0", " ")
    s = re.sub(r"\r\n?", "\n", s)
    s = re.sub(r"[ \t\f\v]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()



def _cut_tech_tail(s: str) -> str:
    if not s:
        return ""
    best = None
    for header in TECH_HEADERS:
        m = re.search(rf"(?:^|\n)\s*{re.escape(header)}\s*:?")
        if m:
            pos = m.start()
            if best is None or pos < best:
                best = pos
    if best is None:
        return s
    return s[:best].strip()



def _drop_empty_lines(lines: Iterable[str]) -> list[str]:
    out: list[str] = []
    prev_blank = False
    for raw in lines:
        ln = safe_str(raw)
        if not ln:
            if not prev_blank and out:
                out.append("")
            prev_blank = True
            continue
        prev_blank = False
        out.append(ln)
    while out and not out[-1]:
        out.pop()
    return out



def clean_description(text: str) -> str:
    """Оставить аккуратный supplier-body без техшапок."""
    s = _norm_spaces(text)
    if not s:
        return ""

    # Частый CopyLine-кейс: narrative + техблок после заголовка.
    s = _cut_tech_tail(s)

    # Убираем одиночные техшапки в начале.
    for header in TECH_HEADERS:
        s = re.sub(rf"^\s*{re.escape(header)}\s*:?[ \t]*", "", s, flags=re.I)

    # Сжимаем в 1–2 абзаца.
    lines = _drop_empty_lines(s.splitlines())
    s = "\n".join(lines)

    # Небольшая косметика.
    s = re.sub(r"\s+([,.;:])", r"\1", s)
    s = re.sub(r"([,.;:])(\S)", r"\1 \2", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()
