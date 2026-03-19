# -*- coding: utf-8 -*-
"""
NVPrint params_xml — clean wave1.

Основано на реальном source XML:
- цена truth-source = УсловияПродаж/Договор/Цена
- compat можно добрать из <Принтеры>/<Принтер>
- supplier description часто пустой, поэтому нужен короткий technical raw-desc
"""

from __future__ import annotations

import re
from xml.etree import ElementTree as ET

from suppliers.nvprint.source import (
    get_text,
    iter_children,
    local,
    pick_first_text,
)

RE_DESC_HAS_CS = re.compile(r"<!--\s*WhatsApp\s*-->|<!--\s*Описание\s*-->|<h3>\s*Характеристики\s*</h3>", re.I)
RE_WS = re.compile(r"\s+")

DROP_PARAM_NAMES_CF = {
    "артикул", "остаток", "наличие", "в наличии", "сопутствующие товары",
    "sku", "код", "guid", "ссылканакартинку",
    "вес", "высота", "длина", "ширина", "объем", "объём",
    "разделкаталога", "разделмодели",
}

COLOR_MAP = {
    "пурпурный": "Magenta",
    "магента": "Magenta",
    "черный": "Black",
    "чёрный": "Black",
    "желтый": "Yellow",
    "жёлтый": "Yellow",
    "голубой": "Cyan",
    "циан": "Cyan",
    "color": "Color",
    "black": "Black",
    "cyan": "Cyan",
    "magenta": "Magenta",
    "yellow": "Yellow",
}

KEY_MAP = {
    "ТипПечати": "Тип печати",
    "ЦветПечати": "Цвет печати",
    "СовместимостьСМоделями": "Совместимость с моделями",
}

def _rename_key(k: str) -> str:
    k = (k or "").strip()
    return KEY_MAP.get(k, k)

def _cleanup_value(k: str, v: str) -> str:
    kk = (k or "").strip().casefold()
    vv = RE_WS.sub(" ", (v or "").strip())
    if not vv:
        return ""
    if kk in ("цвет печати", "цветпечати", "цвет"):
        return COLOR_MAP.get(vv.casefold(), vv)
    if kk in ("совместимость с моделями", "совместимостьсмоделями", "совместимость"):
        vv = re.sub(r"\bWorkcentr(e)?\b", "WorkCentre", vv, flags=re.I)
        vv = re.sub(r"\s*/\s*", "/ ", vv)
        vv = re.sub(r"\s+", " ", vv).strip()
    return vv

def _drop_zeroish_param(key_cf: str, value: str) -> bool:
    v = (value or "").strip()
    if key_cf in ("вес", "высота", "длина", "ширина", "ресурс") and v in ("0", "0.0", "0,0", "0,00", "0.00"):
        return True
    if key_cf == "гарантия" and v.casefold() in ("0", "0 мес", "0 месяцев", "0мес"):
        return True
    return False

def _collect_printers_compat(item: ET.Element) -> str:
    vals: list[str] = []
    printers = item.find("Принтеры")
    if printers is None:
        return ""
    for p in printers.findall("Принтер"):
        v = RE_WS.sub(" ", get_text(p))
        if v:
            vals.append(v)
    if not vals:
        return ""
    seen = set()
    out = []
    for v in vals:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return "/ ".join(out)

def collect_params(item: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    def _push(k: str, v: str) -> None:
        k2 = _rename_key(k)
        v2 = _cleanup_value(k2, v)
        if not k2 or not v2:
            return
        out.append((k2, v2))

    # прямые child-теги — основной truth-source для NVPrint
    skip_keys = {
        "код", "артикул", "guid",
        "номенклатура", "номенклатуракратко", "наименование",
        "ссылканакартинку",
        "условияпродаж", "принтеры",
        "new_reman", "разделпрайса",
        "цена", "наличие",
    }

    for ch in iter_children(item):
        k = local(ch.tag).strip()
        cf = k.casefold()
        v = get_text(ch)
        if not v:
            continue
        if cf in skip_keys or cf in DROP_PARAM_NAMES_CF:
            continue
        if _drop_zeroish_param(cf, v):
            continue
        _push(k, v)

    # если supplier не дал совместимость — поднимаем её из блока Принтеры
    if not any((k or "").casefold() == "совместимость с моделями" and v for k, v in out):
        compat = _collect_printers_compat(item)
        if compat:
            out.append(("Совместимость с моделями", compat))

    return out

def _first_param(params: list[tuple[str, str]], names: tuple[str, ...]) -> str:
    want = {n.casefold() for n in names}
    for k, v in params:
        if (k or "").casefold() in want and v:
            return v
    return ""

def _build_fallback_desc(item: ET.Element) -> str:
    name = pick_first_text(item, ("Номенклатура", "НоменклатураКратко", "name", "title", "Наименование"))
    name = RE_WS.sub(" ", (name or "").strip())
    params = collect_params(item)

    ptype = _first_param(params, ("Тип печати", "Тип"))
    color = _first_param(params, ("Цвет печати", "Цвет"))
    compat = _first_param(params, ("Совместимость с моделями", "Совместимость"))
    resource = _first_param(params, ("Ресурс",))
    barcode = _first_param(params, ("ШтрихКод",))

    bits = []
    if name:
        bits.append(name)
    if ptype:
        bits.append(f"Тип печати: {ptype}.")
    if color:
        bits.append(f"Цвет: {color}.")
    if resource:
        bits.append(f"Ресурс: {resource}.")
    if compat:
        bits.append(f"Совместимость: {compat}.")
    if barcode:
        bits.append(f"Штрихкод: {barcode}.")

    return " ".join(bits).strip()

def native_desc(item: ET.Element) -> str:
    d = pick_first_text(item, ("description", "Описание"))
    if d:
        d = d.strip()
        if d and not RE_DESC_HAS_CS.search(d):
            return d
    return _build_fallback_desc(item)
