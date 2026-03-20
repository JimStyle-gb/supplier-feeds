# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/desc_extract.py

VTT description extract layer.

Задача:
- очень мягко поднять missing params из supplier narrative description;
- работать только в режиме only_fill_missing;
- не перетирать source/page params;
- не строить compat и не заниматься агрессивной магией.
"""

from __future__ import annotations

import re
from typing import Iterable, Sequence

from .normalize import norm_spaces


_COLOR_MAP = {
    "black": "Черный",
    "bk": "Черный",
    "cyan": "Голубой",
    "magenta": "Пурпурный",
    "yellow": "Желтый",
    "grey": "Серый",
    "gray": "Серый",
    "blue": "Синий",
    "red": "Красный",
    "green": "Зеленый",
    "mattblack": "Матовый черный",
    "matteblack": "Матовый черный",
    "photoblack": "Фото-черный",
    "photo black": "Фото-черный",
    "color": "Цветной",
    "colour": "Цветной",
}

_VENDOR_LIST = (
    "HP",
    "Canon",
    "Xerox",
    "Kyocera",
    "Brother",
    "Epson",
    "Pantum",
    "Ricoh",
    "Lexmark",
    "Samsung",
    "OKI",
    "RISO",
    "Panasonic",
    "Toshiba",
    "Sharp",
    "Konica Minolta",
    "Develop",
)

_TYPE_RULES = (
    (r"\bтермоблок\b", "Термоблок"),
    (r"\bтермолент[аы]?\b", "Термолента"),
    (r"\bфотобарабан\b", "Фотобарабан"),
    (r"\bдрам(?:-?юнит|картридж)?\b", "Фотобарабан"),
    (r"\bдевелоп(?:ер)?\b", "Девелопер"),
    (r"\bчернил[ао]?\b|\bink\b", "Чернила"),
    (r"\bпечатающ(?:ая|ие)\s+голов", "Печатающая головка"),
    (r"\bтонер-?картридж\b", "Тонер-картридж"),
    (r"\bкартридж\b", "Картридж"),
)


def _cf(text: str) -> str:
    return norm_spaces(text).casefold().replace("ё", "е")


def _existing_key_map(params: Sequence[tuple[str, str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in params:
        kk = _cf(k)
        vv = norm_spaces(v)
        if kk and vv and kk not in out:
            out[kk] = vv
    return out


def _has_param(params: Sequence[tuple[str, str]], key: str) -> bool:
    key_cf = _cf(key)
    return any(_cf(k) == key_cf and norm_spaces(v) for k, v in params)


def _infer_brand(text: str) -> str:
    src = norm_spaces(text)
    if not src:
        return ""
    for vendor in _VENDOR_LIST:
        if re.search(rf"\b{re.escape(vendor)}\b", src, flags=re.I):
            return vendor
    m = re.search(
        r"(?:^|\b)(?:для|for)\s+"
        r"(HP|Canon|Xerox|Kyocera|Brother|Epson|Pantum|Ricoh|Lexmark|Samsung|OKI|RISO|Panasonic|Toshiba|Sharp)\b",
        src,
        flags=re.I,
    )
    return norm_spaces(m.group(1)) if m else ""


def _infer_type(text: str) -> str:
    src = _cf(text)
    if not src:
        return ""
    for rx, value in _TYPE_RULES:
        if re.search(rx, src, flags=re.I):
            return value
    return ""


def _infer_color(text: str) -> str:
    src = _cf(text).replace(" ", "")
    if not src:
        return ""
    for raw, value in _COLOR_MAP.items():
        key = raw.replace(" ", "")
        if key and key in src:
            return value
    return ""


def _infer_resource(text: str) -> str:
    src = norm_spaces(text)
    if not src:
        return ""

    # 26К / 32K
    m = re.search(r"\b(\d{1,3})\s*([КK])\b", src, flags=re.I)
    if m:
        return f"{m.group(1)}К"

    # 7000 стр / 7 000 страниц
    m = re.search(r"\b(\d[\d\s]{2,6})\s*(?:стр|страниц[а-я]*)\b", src, flags=re.I)
    if m:
        num = re.sub(r"\s+", "", m.group(1))
        return f"{num} стр"

    return ""


def _infer_volume(text: str) -> str:
    src = norm_spaces(text)
    if not src:
        return ""
    m = re.search(r"\b(\d{1,4})\s*(?:мл|ml)\b", src, flags=re.I)
    if not m:
        return ""
    return f"{m.group(1)} мл"


def _append_missing(params: list[tuple[str, str]], key: str, value: str) -> None:
    if not value:
        return
    if _has_param(params, key):
        return
    params.append((key, norm_spaces(value)))


def extract_missing_params_from_desc(
    *,
    title: str = "",
    native_desc: str = "",
    current_params: Sequence[tuple[str, str]] | None = None,
) -> list[tuple[str, str]]:
    """
    Возвращает только missing params, которые можно мягко и уверенно
    поднять из description/title.
    """
    current_params = list(current_params or [])
    hay = "\n".join([x for x in (norm_spaces(title), norm_spaces(native_desc)) if x]).strip()
    if not hay:
        return []

    out: list[tuple[str, str]] = []

    if not _has_param(current_params, "Для бренда"):
        brand = _infer_brand(hay)
        if brand:
            out.append(("Для бренда", brand))

    if not _has_param(current_params, "Тип"):
        typ = _infer_type(hay)
        if typ:
            out.append(("Тип", typ))

    if not _has_param(current_params, "Цвет"):
        color = _infer_color(hay)
        if color:
            out.append(("Цвет", color))

    if not _has_param(current_params, "Ресурс"):
        resource = _infer_resource(hay)
        if resource:
            out.append(("Ресурс", resource))

    if not _has_param(current_params, "Объем"):
        volume = _infer_volume(hay)
        if volume:
            out.append(("Объем", volume))

    return out


def merge_missing_params_from_desc(
    *,
    title: str = "",
    native_desc: str = "",
    current_params: Sequence[tuple[str, str]] | None = None,
) -> list[tuple[str, str]]:
    """
    Возвращает current_params + missing params from desc.
    Ничего не перетирает.
    """
    base = list(current_params or [])
    add = extract_missing_params_from_desc(
        title=title,
        native_desc=native_desc,
        current_params=base,
    )
    for k, v in add:
        _append_missing(base, k, v)
    return base
