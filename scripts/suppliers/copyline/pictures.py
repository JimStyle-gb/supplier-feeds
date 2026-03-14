# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/pictures.py
CopyLine pictures layer.

Задача:
- оставить только реальные product pictures из JShopping img_products;
- убрать мусор/дубли;
- приоритет full_*.
"""

from __future__ import annotations

from typing import Iterable, List


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def _is_product_picture(url: str) -> bool:
    """CopyLine: реальными считаем только img_products URLs."""
    val = safe_str(url).replace("\\", "/")
    return "/components/com_jshopping/files/img_products/" in val


def _is_full_picture(url: str) -> bool:
    """Определить full_* картинку."""
    val = safe_str(url)
    base = val.rsplit("/", 1)[-1]
    return base.startswith("full_") or "/full_" in val


def prefer_full_product_pictures(pictures: Iterable[str]) -> List[str]:
    """Оставить только реальные фото товара; full_* поставить в приоритет."""
    cleaned: list[str] = []
    seen: set[str] = set()

    for raw in pictures:
        url = safe_str(raw).replace("&amp;", "&")
        if not url or url.startswith("data:"):
            continue
        if not _is_product_picture(url):
            continue
        if url in seen:
            continue
        seen.add(url)
        cleaned.append(url)

    if not cleaned:
        return []

    fulls = [u for u in cleaned if _is_full_picture(u)]
    other = [u for u in cleaned if not _is_full_picture(u)]
    return fulls if fulls else other


def full_only_if_present(pictures: Iterable[str]) -> List[str]:
    """Если среди уже очищенных картинок есть full_ — оставить только их."""
    pics = [safe_str(x) for x in pictures if safe_str(x)]
    if not pics:
        return []
    fulls = [u for u in pics if _is_full_picture(u)]
    return fulls if fulls else pics
