# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/desc_clean.py

AlStyle description cleaning.
Только narrative-cleaning, без desc->params extraction.

v115:
- сохраняет границы строк для multiline extraction;
- мягко разрезает плотные one-line тех-описания на label-friendly строки;
- чище дочищает Xerox/Canon narrative-хвосты;
- не схлопывает extraction-текст обратно в одну строку.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from html import unescape

from cs.util import norm_ws


_CODE_SERIES_RE = re.compile(
    r"(?<![\w/])(?:(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,}(?:\s*/\s*(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,})+)"
)
_SKU_TOKEN_RE = re.compile(r"\b[A-Z]{1,6}-\d{2,6}[A-Z]{0,4}\b|\b[A-Z]{2,}[A-Z0-9-]{4,}\b")
_CSS_SERVICE_LINE_RE = re.compile(
    r"(?iu)(?:^|\s)(?:body\s*\{|font-family\s*:|display\s*:|margin\s*:|padding\s*:|border\s*:|color\s*:|background\s*:|"
    r"\.?chip\s*\{|\.?badge\s*\{|\.?spec\s*\{|h[1-6]\s*\{)"
)

_LABEL_BREAK_PATTERNS = [
    r"Основные\s+характеристики",
    r"Технические\s+характеристики",
    r"Производитель",
    r"Модель",
    r"Аналог\s+модели",
    r"Совместимые\s+модели",
    r"Совместимость",
    r"Устройства",
    r"Устройство",
    r"Для\s+принтеров",
    r"Технология\s+печати",
    r"Цвет\s+печати",
    r"Цвет",
    r"Ресурс\s+картриджа,\s*[cс]тр\.",
    r"Ресурс\s+картриджа",
    r"Ресурс",
    r"Количество\s+страниц",
    r"Кол-во\s+страниц\s+при\s+5%\s+заполнении\s+А4",
    r"Емкость\s+лотка",
    r"Ёмкость\s+лотка",
    r"Емкость",
    r"Ёмкость",
    r"Объем\s+картриджа,\s*мл",
    r"Объём\s+картриджа,\s*мл",
    r"Степлирование",
    r"Дополнительные\s+опции",
    r"Применение",
    r"Количество\s+в\s+упаковке",
    r"Колличество\s+в\s+упаковке",
]
_LABEL_BREAK_RE = re.compile(
    r"(?<!^)(?<!\n)(?=\b(?:" + "|".join(_LABEL_BREAK_PATTERNS) + r")\b)",
    re.IGNORECASE,
)
_BRAND_GLUE_RE = re.compile(
    r"(?<=[A-Za-zА-Яа-я0-9])(?=(?:CANON|Canon|Xerox|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark)\s+"
    r"(?:PIXMA|WorkCentre|WorkCenter|VersaLink|AltaLink|Phaser|ColorQube|CopyCentre|imageRUNNER|i-SENSYS|ECOSYS|LaserJet|DeskJet|OfficeJet)\b)"
)


def dedupe_code_series_text(text: str) -> str:
    s = norm_ws(text)
    if not s:
        return ""

    def repl(m: re.Match[str]) -> str:
        raw = m.group(0)
        parts = [norm_ws(x) for x in re.split(r"\s*/\s*", raw) if norm_ws(x)]
        out: list[str] = []
        seen: set[str] = set()
        for p in parts:
            sig = p.casefold()
            if sig in seen:
_sanitize_desc_quality_text = sanitize_desc_quality_text
