# -*- coding: utf-8 -*-
"""
NVPrint XML params layer — step2 safe split.

Пока без смены общей логики: переносим извлечение цены,
param cleanup и native_desc из текущего монолита.
"""

from __future__ import annotations

import re
from xml.etree import ElementTree as ET

from suppliers.nvprint.normalize import (
    rename_param_key_nvprint,
    cleanup_param_value_nvprint,
)
from suppliers.nvprint.source import (
    local,
    get_text,
    pick_first_text,
    iter_children,
)

RE_DESC_HAS_CS = re.compile(r"<!--\s*WhatsApp\s*-->|<!--\s*Описание\s*-->|<h3>\s*Характеристики\s*</h3>", re.I)

DROP_PARAM_NAMES_CF = {
    "артикул",
    "остаток",
    "наличие",
    "в наличии",
    "сопутствующие товары",
    "sku",
    "код",
    "guid",
    "ссылканакартинку",
    "вес",
    "высота",
    "длина",
    "ширина",
    "объем",
    "объём",
    "разделкаталога",
    "разделмодели",
}


def parse_num(text: str) -> float | None:
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

    if not found:
        return None
    return min(found)


def collect_params(item: ET.Element) -> list[tuple[str, str]]:
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
        v = cleanup_param_value_nvprint(k, v)
        out.append((k, v))

    if out:
        return out

    skip_keys = {
        "код", "артикул", "guid",
        "номенклатура", "номенклатуракратко", "наименование",
        "цена", "ценасндс", "ценабезндс", "цена_кзт", "price",
        "new_reman", "разделпрайса",
        "ссылканакартинку",
    }

    for ch in iter_children(item):
        k = local(ch.tag).strip()
        cf = k.casefold()
        v = get_text(ch)
        if not v:
            continue
        if cf in skip_keys:
            continue
        if cf in DROP_PARAM_NAMES_CF:
            continue
        if cf in ("вес", "высота", "длина", "ширина", "ресурс") and v.strip() in ("0", "0.0", "0,0", "0,00", "0.00"):
            continue
        if cf == "гарантия" and v.strip().casefold() in ("0", "0 мес", "0 месяцев", "0мес"):
            continue

        k = rename_param_key_nvprint(k)
        v = cleanup_param_value_nvprint(k, v)
        out.append((k, v))

    return out


def native_desc(item: ET.Element) -> str:
    d = pick_first_text(item, ("description", "Описание"))
    if not d:
        return ""
    if RE_DESC_HAS_CS.search(d):
        return ""
    return d
