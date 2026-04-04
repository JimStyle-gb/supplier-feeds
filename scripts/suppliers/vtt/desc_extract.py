# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/desc_extract.py

Only-fill-missing extraction from title/description.

Patch focus:
- убрать массовый cosmetic-класс decimal_k_resource;
- не менять supplier-specific narrative logic;
- сохранить backward-safe API extract_resource / build_native_description.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Sequence

from .desc_clean import clean_native_description
from .normalize import RES_RE, format_resource_value, infer_color_from_title, norm_ws


def _normalize_resource_value(value: str) -> str:
    """
    Канонизировать ресурс supplier-layer.

    Правило для K:
    - если уже целое значение -> оставить N K;
    - если дробное значение -> перевести в целый K:
      * 0 < x < 1  -> 1K
      * x >= 1     -> округление HALF_UP до целого
    Это убирает хвосты вида 13,7K / 8.3K / 0,68K / 4.425K.
    """
    raw = norm_ws(value)
    if not raw:
        return ""

    # Сначала используем общую мягкую нормализацию.
    normalized = format_resource_value(raw)
    if not normalized:
        return ""

    low = normalized.casefold()
    if low.endswith("k"):
        number = normalized[:-1].strip().replace(",", ".")
        try:
            dec = Decimal(number)
        except InvalidOperation:
            return normalized

        if dec <= 0:
            return ""

        if dec < 1:
            return "1K"

        int_dec = dec.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return f"{int(int_dec)}K"

    return normalized


def extract_resource(title: str, params: Sequence[tuple[str, str]], desc: str) -> str:
    # 1) Предпочитаем supplier params, но всё равно канонизируем.
    for key, value in params:
        if str(key).strip().casefold() == "ресурс" and norm_ws(value):
            fixed = _normalize_resource_value(str(value))
            if fixed:
                return fixed

    # 2) Затем fallback из title/desc.
    hay = " | ".join([title, desc])
    m = RES_RE.search(hay)
    if not m:
        return ""

    num = m.group(1)
    unit = m.group(2)

    if unit.casefold() in {"k", "к"}:
        return _normalize_resource_value(f"{num}K")
    if unit.casefold() in {"ml", "мл"}:
        return _normalize_resource_value(f"{num} мл")
    if unit.casefold() in {"l", "л"}:
        return _normalize_resource_value(f"{num} л")
    return ""


def extract_missing_from_desc(*, title: str, desc: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    color = infer_color_from_title(title)
    if color:
        out.append(("Цвет", color))
    cleaned = clean_native_description(desc)
    if cleaned and cleaned.casefold() != norm_ws(title).casefold():
        pass
    return out


def build_native_description(
    *,
    title: str,
    type_name: str,
    part_number: str,
    compat: str,
    resource: str,
    color: str,
    is_original: bool,
    desc_body: str,
) -> str:
    parts: list[str] = []
    if type_name:
        parts.append(f"Тип: {type_name}")
    if part_number:
        parts.append(f"Партномер: {part_number}")
    if compat:
        parts.append(f"Совместимость: {compat}")
    if resource:
        parts.append(f"Ресурс: {resource}")
    if color:
        parts.append(f"Цвет: {color}")
    if is_original:
        parts.append("Оригинальность: Оригинал")
    head = "; ".join(parts)
    body = clean_native_description(desc_body)
    if body and body.casefold() != norm_ws(title).casefold():
        return f"{head}. {body}" if head else body
    return head or title


__all__ = [
    "extract_resource",
    "extract_missing_from_desc",
    "build_native_description",
]
