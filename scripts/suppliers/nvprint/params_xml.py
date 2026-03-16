# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/nvprint/params_xml.py
NVPrint params_xml layer — цена и сырые params из XML.
"""

from __future__ import annotations

import re
from xml.etree import ElementTree as ET

from suppliers.nvprint.normalize import (
    DROP_PARAM_NAMES_CF,
    cleanup_param_value_nvprint,
    rename_param_key_nvprint,
)
from suppliers.nvprint.source import get_text, iter_children, local



def parse_num(text: str) -> float | None:
    """Распарсить число из текста."""
    t = (text or "").strip()
    if not t:
        return None
    t = t.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(\.\d+)?", t)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None



def extract_price(item: ET.Element) -> int | None:
    """Извлечь supplier price."""
    prefer_keys = {
        "purchase_price", "base_price", "price",
        "цена", "цена_кзт", "ценаказахстан", "ценаkzt", "pricekzt",
        "ценасндс", "ценабезндс",
    }

    for ch in iter_children(item):
        k = local(ch.tag).casefold()
        if k in prefer_keys:
            n = parse_num(get_text(ch))
            if n is not None:
                return int(n)

    found: list[int] = []
    for el in item.iter():
        k = local(el.tag).casefold()
        if "цена" in k or k in prefer_keys:
            n = parse_num(get_text(el))
            if n is not None and n > 0:
                found.append(int(n))

    return min(found) if found else None



def collect_params(item: ET.Element, fix_mixed_ru_func) -> list[tuple[str, str]]:
    """Собрать supplier params 1:1 без смены поведения."""
    out: list[tuple[str, str]] = []

    for p in item.findall("param"):
        k = (p.get("name") or "").strip()
        v = get_text(p)
        if not k or not v:
            continue
        if k.casefold() in DROP_PARAM_NAMES_CF:
            continue
        if k.casefold() in ("вес", "высота", "длина", "ширина", "ресурс") and v.strip() in ("0", "0.0", "0,0", "0,00", "0.00"):
            continue
        if k.casefold() == "гарантия" and v.strip().casefold() in ("0", "0 мес", "0 месяцев", "0мес"):
            continue
        k = rename_param_key_nvprint(k)
        v = cleanup_param_value_nvprint(k.replace(" ", ""), v, fix_mixed_ru_func) if k in ("Тип печати", "Цвет печати", "Совместимость с моделями") else cleanup_param_value_nvprint(k, v, fix_mixed_ru_func)
        orig_k = k
        k = rename_param_key_nvprint(k)
        v = cleanup_param_value_nvprint(orig_k, v, fix_mixed_ru_func)
        out.append((k, v))

    if out:
        return out

    skip_keys = {
        "код", "артикул", "guid", "номенклатура", "номенклатуракратко", "наименование",
        "цена", "ценасндс", "ценабезндс", "цена_кзт", "price", "new_reman", "разделпрайса", "ссылканакартинку",
    }

    for ch in iter_children(item):
        k = local(ch.tag).strip()
        cf = k.casefold()
        v = get_text(ch)
        if not v:
            continue
        if cf in skip_keys or cf in DROP_PARAM_NAMES_CF:
            continue
        if cf in ("вес", "высота", "длина", "ширина", "ресурс") and v.strip() in ("0", "0.0", "0,0", "0,00", "0.00"):
            continue
        if cf == "гарантия" and v.strip().casefold() in ("0", "0 мес", "0 месяцев", "0мес"):
            continue
        k = rename_param_key_nvprint(k)
        v = cleanup_param_value_nvprint(k.replace(" ", ""), v, fix_mixed_ru_func) if k in ("Тип печати", "Цвет печати", "Совместимость с моделями") else cleanup_param_value_nvprint(k, v, fix_mixed_ru_func)
        out.append((k, v))

    return out
