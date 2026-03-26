# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/pictures.py
"""

from __future__ import annotations

import re

from .normalize import safe_str

BAD_IMAGE_RE = re.compile(r"(favicon|yandex|counter|watch/|pixel|metrika|doubleclick|logo)", re.I)
PLACEHOLDER = "https://placehold.co/800x800/png?text=No+Photo"


def clean_picture_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in urls or []:
        url = safe_str(raw)
        if not url or BAD_IMAGE_RE.search(url):
            continue
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out
