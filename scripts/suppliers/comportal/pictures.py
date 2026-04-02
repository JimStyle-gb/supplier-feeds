# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/pictures.py
ComPortal picture policy.

Роль:
- выбрать картинки товара;
- убрать дубли;
- вернуть placeholder, если фото нет.

В модуле НЕТ:
- source reading;
- filter logic;
- normalize vendor/name;
- params extraction.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

PLACEHOLDER_PICTURE = "https://placehold.co/800x800/png?text=No+Photo"


def safe_str(x: Any) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def is_valid_picture(url: str) -> bool:
    """Проверить, что картинка выглядит валидной."""
    u = norm_spaces(url)
    if not u:
        return False
    if not (u.startswith("http://") or u.startswith("https://")):
        return False
    return True


def collect_pictures(raw_offer: Dict[str, Any]) -> List[str]:
    """Собрать pictures без дублей."""
    pics = raw_offer.get("raw_pictures") or raw_offer.get("pics") or []
    out: List[str] = []
    seen: set[str] = set()

    single = norm_spaces(raw_offer.get("raw_picture") or raw_offer.get("pic"))
    if is_valid_picture(single) and single not in seen:
        seen.add(single)
        out.append(single)

    for pic in pics:
        value = norm_spaces(safe_str(pic))
        if not is_valid_picture(value):
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)

    return out


def pick_pictures(raw_offer: Dict[str, Any]) -> List[str]:
    """Вернуть итоговый список картинок или placeholder."""
    out = collect_pictures(raw_offer)
    if out:
        return out
    return [PLACEHOLDER_PICTURE]


def pick_main_picture(raw_offer: Dict[str, Any]) -> str:
    """Вернуть главную картинку товара."""
    return pick_pictures(raw_offer)[0]


__all__ = [
    "PLACEHOLDER_PICTURE",
    "collect_pictures",
    "pick_pictures",
    "pick_main_picture",
]
