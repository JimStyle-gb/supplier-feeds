# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/desc_extract.py

Only-fill-missing extraction from title/description.
"""

from __future__ import annotations

from typing import Sequence

from .desc_clean import clean_native_description
from .normalize import RES_RE, format_resource_value, infer_color_from_title, norm_ws


def extract_resource(title: str, params: Sequence[tuple[str, str]], desc: str) -> str:
    for key, value in params:
        if str(key).strip().casefold() == "ресурс" and norm_ws(value):
            return format_resource_value(norm_ws(value))
    hay = " | ".join([title, desc])
    m = RES_RE.search(hay)
    if not m:
        return ""
    unit = m.group(2)
    if unit.casefold() in {"k", "к"}:
        return f"{m.group(1)}K"
    if unit.casefold() in {"ml", "мл"}:
        return f"{m.group(1)} мл"
    if unit.casefold() in {"l", "л"}:
        return f"{m.group(1)} л"
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
