# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/pictures.py

VTT pictures layer.

Задача:
- принять сырой список pictures из source.py;
- убрать мусор/дубли/битые ссылки;
- оставить только реальные product pictures;
- если реальных фото нет — поставить project placeholder.
"""

from __future__ import annotations

import re
from typing import Iterable, List


PLACEHOLDER_URL = "https://placehold.co/800x800/png?text=No+Photo"

_BAD_HOST_SNIPS = (
    "mc.yandex.ru",
    "metrika.yandex",
    "google-analytics.com",
    "googletagmanager.com",
    "doubleclick.net",
)

_BAD_PATH_SNIPS = (
    "/watch/",
    "pixel",
    "counter",
    "collect",
    "favicon",
)

_IMG_EXT_RE = re.compile(r"\.(jpg|jpeg|png|webp|gif|bmp|tif|tiff)(\?|#|$)", re.I)

_ALLOWED_PATH_SNIPS = (
    "/upload/",
    "/images/",
    "/img/",
    "/image/",
    "/files/",
    "/components/",
)

_BAD_FILENAME_SNIPS = (
    "no_photo",
    "nophoto",
    "noimage",
    "no-image",
    "placeholder",
)


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def _normalize_url(url: str) -> str:
    """Лёгкая нормализация url."""
    val = safe_str(url).replace("&amp;", "&").replace("\\", "/")
    return val


def _looks_like_placeholder(url: str) -> bool:
    """Определить supplier placeholder/заглушку."""
    val = _normalize_url(url).lower()
    if not val:
        return False
    if "placehold.co/800x800/png?text=no+photo" in val:
        return True
    return any(x in val for x in _BAD_FILENAME_SNIPS)


def _is_good_picture(url: str) -> bool:
    """Оставляем только реальные картинки товара."""
    val = _normalize_url(url).lower()
    if not val:
        return False
    if val.startswith("data:"):
        return False
    if any(x in val for x in _BAD_HOST_SNIPS):
        return False
    if any(x in val for x in _BAD_PATH_SNIPS):
        return False
    if _looks_like_placeholder(val):
        return False

    if _IMG_EXT_RE.search(val):
        return any(x in val for x in _ALLOWED_PATH_SNIPS)

    # иногда без расширения, но из типичных папок
    return any(x in val for x in _ALLOWED_PATH_SNIPS)


def dedupe_pictures(pictures: Iterable[str]) -> List[str]:
    """Убрать пустые и дубли, сохранить порядок."""
    out: list[str] = []
    seen: set[str] = set()

    for raw in pictures:
        url = _normalize_url(raw)
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(url)

    return out


def clean_vtt_pictures(pictures: Iterable[str], limit: int = 8) -> List[str]:
    """
    Supplier-clean список pictures для VTT RAW.

    Правила:
    - только реальные картинки;
    - без дублей;
    - placeholder только если реальных фото нет;
    - limit максимум 8.
    """
    cleaned: list[str] = []
    seen: set[str] = set()

    for raw in pictures:
        url = _normalize_url(raw)
        if not url:
            continue
        if not _is_good_picture(url):
            continue
        if url in seen:
            continue
        seen.add(url)
        cleaned.append(url)
        if len(cleaned) >= max(1, limit):
            break

    if cleaned:
        return cleaned

    return [PLACEHOLDER_URL]


def ensure_picture_list(pictures: Iterable[str], limit: int = 8) -> List[str]:
    """Публичный helper для builder."""
    return clean_vtt_pictures(pictures, limit=limit)
