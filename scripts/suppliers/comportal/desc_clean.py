# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/desc_clean.py
ComPortal description cleaning helpers.

Роль:
- слегка чистить supplier-side narrative части;
- не строить final description;
- не быть вторым extractor.

Для ComPortal source-description почти пустой, поэтому модуль тонкий.
"""

from __future__ import annotations

import re


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def clean_native_text(text: str) -> str:
    """Почистить supplier narrative text."""
    s = text or ""
    s = s.replace("\r", "\n")
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = re.sub(r"\s+\.", ".", s)
    s = re.sub(r"\s+,", ",", s)
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    lines = [norm_spaces(line) for line in s.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines).strip()


def clean_title_for_desc(title: str) -> str:
    """Подготовить title для native_desc."""
    return norm_spaces(title)


__all__ = [
    "clean_native_text",
    "clean_title_for_desc",
]
