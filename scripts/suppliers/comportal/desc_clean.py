# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/desc_clean.py

ComPortal description cleaning.
Роль как у других поставщиков:
- только narrative-cleaning;
- без desc->params extraction;
- без picture/vendor/builder логики.

У ComPortal source-description почти всегда пустой или слабый,
поэтому модуль лёгкий и нужен в основном для безопасной санитарной очистки.
"""

from __future__ import annotations

import re

from cs.util import norm_ws


_EMPTY_LINE_RE = re.compile(r"\n{3,}")
_TRAILING_PUNCT_LINE_RE = re.compile(r"^[\s\.,;:!\-–—]+$")
_MULTI_SPACE_RE = re.compile(r"[ \t]{2,}")
_SPACE_BEFORE_PUNCT_RE = re.compile(r"\s+([,.;:!?])")
_BRACKET_WS_OPEN_RE = re.compile(r"\(\s+")
_BRACKET_WS_CLOSE_RE = re.compile(r"\s+\)")
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _clean_line(line: str) -> str:
    s = line.replace("\xa0", " ")
    s = _HTML_TAG_RE.sub(" ", s)
    s = _SPACE_BEFORE_PUNCT_RE.sub(r"\1", s)
    s = _BRACKET_WS_OPEN_RE.sub("(", s)
    s = _BRACKET_WS_CLOSE_RE.sub(")", s)
    s = _MULTI_SPACE_RE.sub(" ", s)
    s = norm_ws(s)
    if _TRAILING_PUNCT_LINE_RE.fullmatch(s or ""):
        return ""
    return s


def sanitize_native_desc(text: str, *, title: str = "") -> str:
    """
    Почистить supplier narrative text.
    Ничего не извлекает — только мягко очищает.
    """
    raw = (text or "").replace("\r", "\n")
    lines = [_clean_line(x) for x in raw.splitlines()]
    lines = [x for x in lines if x]

    # Если description случайно начинается с полного title — убираем дубль первой строки.
    ttl = norm_ws(title)
    if ttl and lines:
        first = norm_ws(lines[0])
        if first.casefold() == ttl.casefold():
            lines = lines[1:]

    out = "\n".join(lines).strip()
    out = _EMPTY_LINE_RE.sub("\n\n", out)
    return out.strip()


__all__ = [
    "sanitize_native_desc",
]
