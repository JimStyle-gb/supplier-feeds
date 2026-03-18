# -*- coding: utf-8 -*-
"""
NVPrint XML params layer — step5.

Цели:
- усилить извлечение цены;
- сохранить уже рабочий fallback native_desc;
- не трогать shared cs/*.
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
RE_WS = re.compile(r"\s+")

DROP_PARAM_NAMES_CF = {
    "артикул", "остаток", "наличие", "в наличии", "сопутствующие товары",
    "sku", "код", "guid", "ссылканакартинку",
    "вес", "высота", "длина", "ширина", "объем", "объём",
    "разделкаталога", "разделмодели",
}

PRICE_KEY_PRIORITY = [
    "purchase_price", "purchaseprice",
    "base_price", "baseprice",
    "supplier_price", "supplierprice",
    "dealerprice", "dealer_price",
    "optprice", "opt_price",
    "закупочнаяцена", "закупочная_цена",
    "ценапоставщика", "цена_поставщика",
    "ценабезндс", "ценасндс", "цена_с_ндс",
    "цена", "цена_кзт", "ценаказахстан", "ценаkzt",
    "pricekzt", "price",
]

PRICE_KEY_PARTS = ("price", "цена", "стоимость", "amount", "sum")
PRICE_BAD_KEY_PARTS = ("старая", "old", "retail", "рознич", "recommended", "рекомендуем", "rrp", "discount", "скид", "sale")


def _norm_key(s: str) -> str:
    s = (s or "").strip().casefold()
    return s.replace(" ", "").replace("-", "").replace("_", "")


def parse_num(text: str) -> float | None:
    t = (text or "").strip()
    if not t:
        return None
    t = t.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _iter_candidate_price_pairs(item: ET.Element) -> list[tuple[str, int]]:
    found: list[tuple[str, int]] = []

    for raw_key, raw_val in item.attrib.items():
        key = _norm_key(raw_key)
        val = parse_num(raw_val)
        if val is not None and val > 0:
            found.append((key, int(val)))

    for ch in iter_children(item):
        key = _norm_key(local(ch.tag))
        val = parse_num(get_text(ch))
        if val is not None and val > 0:
            found.append((key, int(val)))

    for p in item.findall("param"):
        key = _norm_key((p.get("name") or "").strip())
        val = parse_num(get_text(p))
        if val is not None and val > 0:
            found.append((key, int(val)))

    return found


def extract_price(item: ET.Element) -> int | None:
    found = _iter_candidate_price_pairs(item)
    if not found:
        return None

    by_key: dict[str, list[int]] = {}
    for key, val in found:
        by_key.setdefault(key, []).append(val)

    # 1) Сначала строго по приоритетным ключам.
    for key in PRICE_KEY_PRIORITY:
        nk = _norm_key(key)
        vals = by_key.get(nk) or []
        if not vals:
            continue
        strong = [v for v in vals if v > 100]
        if strong:
            return max(strong)
        return max(vals)

    # 2) Потом — по price/цена-подобным ключам, исключая retail/old/discount.
    fuzzy_vals: list[int] = []
    for key, val in found:
        if any(bad in key for bad in PRICE_BAD_KEY_PARTS):
            continue
        if any(part in key for part in PRICE_KEY_PARTS):
            fuzzy_vals.append(val)

    if fuzzy_vals:
        strong = [v for v in fuzzy_vals if v > 100]
        if strong:
            return max(strong)
        return max(fuzzy_vals)

    # 3) Последний мягкий фолбэк.
    strong = [v for _, v in found if v > 100]
    if strong:
        return max(strong)
    return max(v for _, v in found)


def _drop_zeroish_param(key_cf: str, value: str) -> bool:
    v = (value or "").strip()
    if key_cf in ("вес", "высота", "длина", "ширина", "ресурс") and v in ("0", "0.0", "0,0", "0,00", "0.00"):
        return True
    if key_cf == "гарантия" and v.casefold() in ("0", "0 мес", "0 месяцев", "0мес"):
        return True
    return False


def collect_params(item: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    def _push(k: str, v: str) -> None:
        k = rename_param_key_nvprint(k)
        v = cleanup_param_value_nvprint(k, v)
        if not k or not v:
            return
        out.append((k, v))

    for p in item.findall("param"):
        k = (p.get("name") or "").strip()
        v = get_text(p)
        if not k or not v:
            continue
        k_cf = k.casefold()
        if k_cf in DROP_PARAM_NAMES_CF:
            continue
        if _drop_zeroish_param(k_cf, v):
            continue
        _push(k, v)

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
        if cf in skip_keys or cf in DROP_PARAM_NAMES_CF:
            continue
        if _drop_zeroish_param(cf, v):
            continue
        _push(k, v)

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
    price = extract_price(item)

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
    if price and price > 100:
        bits.append(f"Базовая цена поставщика: {price}.")
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
