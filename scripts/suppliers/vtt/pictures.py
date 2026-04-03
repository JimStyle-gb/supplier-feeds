# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/pictures.py

VTT pictures layer under CS-template.

Роль файла:
- чистить и дедуплицировать picture URLs;
- вырезать явный non-product image noise;
- возвращать placeholder, если реальных картинок не осталось.

Важно:
- supplier-layer сам должен вернуть clean raw pictures;
- placeholder не должен быть жёстко привязан к файлу, если его передали из config;
- модуль оставляет backward-safe API clean_picture_urls(...).
"""

from __future__ import annotations

import re
from typing import Sequence


# Backward-safe дефолт, если builder/source не передали placeholder из config.
PLACEHOLDER = "https://placehold.co/800x800/png?text=No+Photo"

# Явный non-product image noise.
BAD_IMAGE_RE = re.compile(
    r"(?:favicon|yandex|counter|watch/|pixel|metrika|doubleclick|logo)",
    re.I,
)
_HTTP_RE = re.compile(r"^https?://", re.I)
_MULTI_SPACE_RE = re.compile(r"\s+")


def safe_str(value: object) -> str:
    """Безопасно привести значение к строке."""
    return str(value).strip() if value is not None else ""


def _normalize_url(url: str) -> str:
    """Минимальная нормализация URL без supplier-magic."""
    s = safe_str(url)
    if not s:
        return ""
    s = _MULTI_SPACE_RE.sub(" ", s).strip()
    s = s.replace(" ", "%20")
    return s


def _looks_like_product_picture(url: str) -> bool:
    """Отсечь явный мусор и оставить только http/https картинки."""
    if not url:
        return False
    if not _HTTP_RE.match(url):
        return False
    if BAD_IMAGE_RE.search(url):
        return False
    return True


def clean_picture_urls(
    urls: Sequence[str] | None,
    *,
    placeholder_picture: str | None = None,
) -> list[str]:
    """
    Вернуть clean list picture URLs.
    Backward-safe:
    - старые вызовы clean_picture_urls(urls) продолжат работать;
    - новый канонический путь может передавать placeholder_picture из policy/schema.
    """
    out: list[str] = []
    seen: set[str] = set()

    for raw in urls or []:
        url = _normalize_url(raw)
        if not _looks_like_product_picture(url):
            continue
        sig = url.casefold()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(url)

    if out:
        return out

    placeholder = _normalize_url(placeholder_picture or PLACEHOLDER)
    return [placeholder] if placeholder else [PLACEHOLDER]


def collect_picture_urls(
    urls: Sequence[str] | None,
    *,
    placeholder_picture: str | None = None,
) -> list[str]:
    """
    Канонический alias под общий supplier-template.
    Оставлен для унификации с другими поставщиками.
    """
    return clean_picture_urls(urls, placeholder_picture=placeholder_picture)


__all__ = [
    "PLACEHOLDER",
    "BAD_IMAGE_RE",
    "safe_str",
    "clean_picture_urls",
    "collect_picture_urls",
]
