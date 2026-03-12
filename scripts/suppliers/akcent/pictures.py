# -*- coding: utf-8 -*-
"""
AkCent pictures layer.

Что делает:
- нормализует список картинок
- убирает дубли / пустые / кривые ссылки
- подставляет placeholder, если фото нет

Важно:
- supplier-layer готовит нормальный raw
- core потом уже просто использует готовые pictures
"""

from __future__ import annotations

import re
from typing import Any

from suppliers.akcent.normalize import NormalizedOffer

PLACEHOLDER_IMAGE = "https://placehold.co/800x800/png?text=No+Photo"

_BAD_IMAGE_EXACT = {
    "",
    "https://nvprint.ru/promo/photo/nophoto.jpg",
    "http://nvprint.ru/promo/photo/nophoto.jpg",
}

_BAD_IMAGE_PARTS = [
    "nophoto.jpg",
    "no_photo",
    "no-photo",
    "/placeholder/",
    "placehold.it",
]

_WS_RE = re.compile(r"\s+")


def _norm_space(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _cleanup_url(url: str) -> str:
    url = _norm_space(url)
    if not url:
        return ""

    # Частые мусорные пробелы
    url = url.replace(" ", "%20")

    # Схему и хвосты приводим к читабельному виду
    url = url.replace("http://", "https://", 1)

    # Убираем очевидные хвосты после URL
    for sep in ["\n", "\r", "\t"]:
        if sep in url:
            url = url.split(sep, 1)[0].strip()

    return url


def _looks_like_image_url(url: str) -> bool:
    u = url.lower()
    if not (u.startswith("https://") or u.startswith("http://")):
        return False

    if any(part in u for part in _BAD_IMAGE_PARTS):
        return False

    if u in _BAD_IMAGE_EXACT:
        return False

    # Разрешаем обычные image url и url без расширения
    if re.search(r"\.(jpg|jpeg|png|webp|gif)(\?.*)?$", u):
        return True
    if "/images/" in u or "/image/" in u or "/upload/" in u or "/uploads/" in u:
        return True
    if "?" in u and ("image" in u or "img" in u or "photo" in u):
        return True

    # Вообще AkCent часто даёт нормальные прямые URL — не режем слишком агрессивно
    return True


def _dedupe_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for raw in values:
        val = _cleanup_url(raw)
        if not val:
            continue
        key = val.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(val)

    return out


def normalize_pictures(pictures: list[str]) -> list[str]:
    cleaned = _dedupe_keep_order(pictures)
    good = [p for p in cleaned if _looks_like_image_url(p)]

    if not good:
        return [PLACEHOLDER_IMAGE]

    return good


def apply_pictures(offer: NormalizedOffer) -> tuple[list[str], dict[str, Any]]:
    before = len(offer.pictures)
    pictures = normalize_pictures(offer.pictures)
    after = len(pictures)

    report: dict[str, Any] = {
        "before": before,
        "after": after,
        "used_placeholder": pictures == [PLACEHOLDER_IMAGE],
    }
    return pictures, report


def apply_pictures_bulk(offers: list[NormalizedOffer]) -> tuple[dict[str, list[str]], dict[str, Any]]:
    mapping: dict[str, list[str]] = {}
    placeholder_count = 0
    total_before = 0
    total_after = 0

    for offer in offers:
        pictures, rep = apply_pictures(offer)
        mapping[offer.oid] = pictures
        total_before += int(rep["before"])
        total_after += int(rep["after"])
        if rep["used_placeholder"]:
            placeholder_count += 1

    report: dict[str, Any] = {
        "offers": len(offers),
        "pictures_before": total_before,
        "pictures_after": total_after,
        "placeholder_count": placeholder_count,
    }
    return mapping, report
