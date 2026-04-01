# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/desc_clean.py
CopyLine display-description clean layer.

Роль модуля:
- готовить ТОЛЬКО display-body для raw/native_desc и final;
- убирать supplier-мусор и техшапки;
- не участвовать в truth extraction;
- не резать текст под extractor-логику.

Важно:
- главный extractor работает раньше и не должен зависеть от narrative-cleaning;
- этот модуль обслуживает только витринный текст;
- функция clean_description() намеренно оставлена как backward-safe public API.
"""

from __future__ import annotations

import re
from typing import Iterable


DISPLAY_CUT_HEADERS: tuple[str, ...] = (
    "Технические характеристики",
    "Основные характеристики",
    "Характеристики",
    "Описание",
)

DISPLAY_NOISE_LINES: tuple[re.Pattern[str], ...] = (
    re.compile(r"^подробн(?:ое|ые)?\s+описани(?:е|я)\s*$", re.I),
    re.compile(r"^характеристик(?:а|и)\s*$", re.I),
    re.compile(r"^основные\s+характеристики\s*$", re.I),
    re.compile(r"^технические\s+характеристики\s*$", re.I),
)


def safe_str(x: object) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""



def _norm_spaces(text: str) -> str:
    """Нормализовать пробелы/переводы строк без semantic-решений."""
    s = safe_str(text)
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\r\n?", "\n", s)
    s = re.sub(r"[ \t\f\v]+", " ", s)
    s = re.sub(r" *\n *", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()



def _cut_display_tail(text: str) -> str:
    """Обрезать явный теххвост для витринного narrative."""
    s = _norm_spaces(text)
    if not s:
        return ""
    best: int | None = None
    for header in DISPLAY_CUT_HEADERS:
        m = re.search(rf"(?:^|\n)\s*{re.escape(header)}\s*:?", s, flags=re.I)
        if m:
            pos = m.start()
            if best is None or pos < best:
                best = pos
    if best is None:
        return s
    return s[:best].strip()



def _drop_known_header_prefixes(text: str) -> str:
    """Убрать одиночные техшапки в начале текста."""
    s = _norm_spaces(text)
    if not s:
        return ""
    for header in DISPLAY_CUT_HEADERS:
        s = re.sub(rf"^\s*{re.escape(header)}\s*:?\s*", "", s, flags=re.I)
    return s.strip()



def _drop_noise_lines(lines: Iterable[str]) -> list[str]:
    """Убрать пустые и служебные строки, сохраняя абзацы."""
    out: list[str] = []
    prev_blank = False
    for raw in lines:
        ln = safe_str(raw)
        if not ln:
            if out and not prev_blank:
                out.append("")
            prev_blank = True
            continue
        if any(rx.fullmatch(ln) for rx in DISPLAY_NOISE_LINES):
            continue
        prev_blank = False
        out.append(ln)
    while out and not out[-1]:
        out.pop()
    return out



def _cleanup_punctuation(text: str) -> str:
    """Мягкая косметика narrative без изменения смысла."""
    s = safe_str(text)
    if not s:
        return ""
    s = re.sub(r"\s+([,.;:])", r"\1", s)
    s = re.sub(r"([,.;:])(\S)", r"\1 \2", s)
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    s = re.sub(r"\s{2,}", " ", s)
    s = re.sub(r" ?\n ?", "\n", s)
    return s.strip()



def clean_description(text: str) -> str:
    """
    Подготовить display-body для native_desc/final.

    Эта функция НЕ должна использоваться как подготовка текста для главного extractor-а.
    """
    s = _norm_spaces(text)
    if not s:
        return ""

    s = _cut_display_tail(s)
    s = _drop_known_header_prefixes(s)
    lines = _drop_noise_lines(s.splitlines())
    s = "\n".join(lines)
    s = _cleanup_punctuation(s)
    return s.strip()
