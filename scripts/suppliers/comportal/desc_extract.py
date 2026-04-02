# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/desc_extract.py
ComPortal description fill-missing helpers.

Роль:
- только fill-missing слой;
- не дублировать params_xml.py;
- не быть главным extractor.

Для ComPortal source-description слабый, поэтому модуль минимальный.
"""

from __future__ import annotations

import re
from typing import Dict, List


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _param_map(params: List[Dict[str, str]]) -> Dict[str, str]:
    """Собрать map param name -> value."""
    out: Dict[str, str] = {}
    for p in params or []:
        name = norm_spaces(p.get("name", ""))
        value = norm_spaces(p.get("value", ""))
        if name and value:
            out[name] = value
    return out


def fill_missing_from_title(raw_offer: Dict[str, object], clean_params: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Точечно дозаполнить параметры из title, если params_xml.py их не дал.
    Только безопасные fill-missing правила.
    """
    title = norm_spaces(str(raw_offer.get("name") or raw_offer.get("title") or ""))
    pmap = _param_map(clean_params)
    out = list(clean_params)

    # Коды из хвостовых скобок: "(CF236A#B19)" и т.п.
    if "Коды" not in pmap:
        m = re.search(r"\(([A-Za-z0-9#\-/\.]+)\)\s*$", title)
        if m:
            out.append({"name": "Коды", "value": m.group(1)})

    # Модель из скобок, если её ещё нет.
    pmap = _param_map(out)
    if "Модель" not in pmap:
        m = re.search(r"\(([A-Za-z0-9#\-/\.]+)\)\s*$", title)
        if m:
            out.append({"name": "Модель", "value": m.group(1)})

    return out


__all__ = [
    "fill_missing_from_title",
]
