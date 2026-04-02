# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/builder.py
ComPortal builder layer.

Задача модуля:
- взять raw offer после source/filter/normalize;
- собрать clean raw offer под CS;
- сформировать стабильный CP-prefixed id/vendorCode;
- собрать минимальный native_desc из name + clean params.

В модуле НЕТ:
- source reading;
- category filtering;
- workflow/build summary;
- final writer/core логики.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List

from .normalize import normalize_basics
from .params_catalog import extract_clean_params

SUPPLIER_PREFIX = "CP"


def safe_str(x: Any) -> str:
    """Безопасно привести к строке."""
    return str(x).strip() if x is not None else ""


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def to_int(x: Any) -> int:
    """Безопасно привести цену к int."""
    s = norm_spaces(safe_str(x))
    if not s:
        return 0
    digits = re.sub(r"[^0-9]+", "", s)
    if not digits:
        return 0
    try:
        return int(digits)
    except Exception:
        return 0


def make_cp_code(raw_offer: Dict[str, Any]) -> str:
    """Стабильный vendorCode / id с префиксом CP."""
    vendor_code = norm_spaces(raw_offer.get("raw_vendorCode") or raw_offer.get("vendorCode"))
    raw_id = norm_spaces(raw_offer.get("raw_id") or raw_offer.get("id"))

    base = vendor_code or raw_id
    base = re.sub(r"[^A-Za-z0-9]+", "", base)
    if not base:
        base = "000000"

    if base.upper().startswith(SUPPLIER_PREFIX):
        return base
    return f"{SUPPLIER_PREFIX}{base}"


def pick_pictures(raw_offer: Dict[str, Any]) -> List[str]:
    """Собрать список картинок без дублей."""
    pics = raw_offer.get("raw_pictures") or raw_offer.get("pics") or []
    out: List[str] = []
    seen: set[str] = set()

    for pic in pics:
        value = norm_spaces(safe_str(pic))
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)

    single = norm_spaces(raw_offer.get("raw_picture") or raw_offer.get("pic"))
    if single and single not in seen:
        out.insert(0, single)

    return out


def _param_map(params: List[Dict[str, str]]) -> Dict[str, str]:
    """Собрать map param name -> value."""
    out: Dict[str, str] = {}
    for p in params:
        name = norm_spaces(safe_str(p.get("name")))
        value = norm_spaces(safe_str(p.get("value")))
        if not name or not value:
            continue
        out[name] = value
    return out


def build_native_desc(name: str, clean_params: List[Dict[str, str]], raw_offer: Dict[str, Any]) -> str:
    """Минимальное supplier-side описание для raw YML."""
    pmap = _param_map(clean_params)

    parts: List[str] = [name]

    vendor = norm_spaces(pmap.get("Для бренда", ""))
    model = norm_spaces(pmap.get("Модель", ""))
    ptype = norm_spaces(pmap.get("Тип", ""))
    tech = norm_spaces(pmap.get("Технология печати", ""))
    color = norm_spaces(pmap.get("Цвет", ""))
    resource = norm_spaces(pmap.get("Ресурс", ""))
    extra = norm_spaces(pmap.get("Дополнительная информация", ""))
    usage = norm_spaces(pmap.get("Применение", ""))

    sentence_bits: List[str] = []
    if ptype:
        sentence_bits.append(ptype.lower())
    if vendor:
        sentence_bits.append(vendor)
    if model and model.upper() not in name.upper():
        sentence_bits.append(model)
    if tech:
        sentence_bits.append(f"с технологией печати {tech.lower()}")
    if color:
        sentence_bits.append(f"цвет {color.lower()}")
    if resource:
        sentence_bits.append(f"ресурс {resource}")
    if usage:
        sentence_bits.append(usage)
    if extra:
        sentence_bits.append(extra)

    if sentence_bits:
        parts.append("Характеристики: " + ", ".join(sentence_bits) + ".")

    category_path = norm_spaces(raw_offer.get("raw_category_path", ""))
    if category_path:
        parts.append(f"Категория поставщика: {category_path}.")

    return "\n".join([x for x in parts if x]).strip()


def build_offer(raw_offer: Dict[str, Any]) -> Dict[str, Any]:
    """Собрать clean raw offer под CS."""
    normalized = normalize_basics(raw_offer)
    clean_params, _ = extract_clean_params(normalized)

    cp_code = make_cp_code(normalized)
    pics = pick_pictures(normalized)
    name = norm_spaces(normalized.get("name") or normalized.get("title") or "")
    vendor = norm_spaces(normalized.get("vendor", ""))
    price = to_int(normalized.get("price_raw") or normalized.get("raw_price_text"))

    offer: Dict[str, Any] = {
        "id": cp_code,
        "vendorCode": cp_code,
        "name": name,
        "price": price,
        "picture": pics[0] if pics else "",
        "pictures": pics,
        "vendor": vendor,
        "currencyId": norm_spaces(normalized.get("raw_currencyId") or normalized.get("currencyId") or "KZT") or "KZT",
        "available": bool(normalized.get("available", True)),
        "categoryId": "",
        "params": clean_params,
        "native_desc": build_native_desc(name, clean_params, normalized),
        "url": norm_spaces(normalized.get("raw_url") or normalized.get("url")),
        "source_category_id": norm_spaces(normalized.get("raw_categoryId") or normalized.get("categoryId")),
        "source_category_name": norm_spaces(normalized.get("raw_category_name")),
        "source_category_path": norm_spaces(normalized.get("raw_category_path")),
        "model": norm_spaces(normalized.get("model")),
    }
    return offer


def build_offers(raw_offers: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Построить список clean raw offers."""
    out: List[Dict[str, Any]] = []
    for raw_offer in raw_offers:
        built = build_offer(raw_offer)
        if not built.get("name"):
            continue
        if not built.get("vendorCode"):
            continue
        out.append(built)
    return out


__all__ = [
    "SUPPLIER_PREFIX",
    "build_offer",
    "build_offers",
    "make_cp_code",
    "build_native_desc",
]
