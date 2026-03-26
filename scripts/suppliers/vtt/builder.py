# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/builder.py

VTT builder layer.
v17:
- same v22 logic baseline preserved;
- responsibility split across normalize / compat / desc_clean / desc_extract / pictures modules;
- keeps RAW as supplier-clean result before shared core.
"""

from __future__ import annotations

import re
from typing import Sequence

from cs.core import OfferOut, compute_price

from .compat import CODE_SOURCE_KEYS, collect_codes, extract_compat, extract_part_number
from .desc_extract import build_native_description, extract_resource
from .normalize import (
    CATEGORY_TYPE_MAP,
    TECH_BY_CATEGORY,
    append_original_suffix,
    clean_title,
    guess_vendor,
    infer_color_from_title,
    infer_tech,
    infer_type,
    is_original,
    make_oid,
    norm_color,
    norm_ws,
    safe_str,
)
from .pictures import PLACEHOLDER, clean_picture_urls


SKIP_PARAM_KEYS = {
    "Артикул",
    "Штрих-код",
    "Вендор",
    "Категория",
    "Подкатегория",
    "В упаковке, штук",
    "Местный склад, штук",
    "Местный, до новой поставки, дней",
    "Склад Москва, штук",
    "Москва, до новой поставки, дней",
    "Категория VTT",
}

ALT_PART_TAIL_RE = re.compile(r"(?:[,\s]+(?:№\s*)?(?:[A-Z]+\d|\d+[A-Z])[A-Z0-9-]{1,}/?)+$", re.I)
DUPLICATE_LEAD_RE = re.compile(r"^([A-Z0-9][A-Z0-9\-./]{2,})\s*,\s*\1\b", re.I)


def _merge_params(
    raw: dict,
    vendor: str,
    type_name: str,
    tech: str,
    part_number: str,
    codes: list[str],
    title: str,
    compat: str,
    resource: str,
) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    color_found = ""

    def add(k: str, v: str) -> None:
        key = norm_ws(k)
        val = norm_ws(v)
        if not key or not val:
            return
        sig = (key.casefold(), val.casefold())
        if sig in seen:
            return
        seen.add(sig)
        out.append((key, val))

    raw_params = [(safe_str(k), safe_str(v)) for (k, v) in (raw.get("params") or [])]

    if type_name:
        add("Тип", type_name)
    if tech:
        add("Технология печати", tech)
    if vendor and type_name and any(
        x in type_name.casefold()
        for x in ("картридж", "драм", "девелопер", "чернила", "тонер", "головка", "блок", "барабан", "контейнер", "носитель")
    ):
        add("Для бренда", vendor)

    for key, value in raw_params:
        if key in SKIP_PARAM_KEYS or key in CODE_SOURCE_KEYS:
            continue
        if key == "Цвет":
            value = norm_color(value)
            color_found = value or color_found
        if key.casefold() == "ресурс":
            resource = resource or norm_ws(value)
            continue
        if key in {"Модель", "Партномер"}:
            continue
        add(key, value)

    if part_number:
        add("Партномер", part_number)
    if compat:
        add("Совместимость", compat)
    if resource:
        add("Ресурс", resource)
    if codes:
        add("Коды расходников", ", ".join(codes))

    if not color_found:
        inferred_color = infer_color_from_title(title)
        if inferred_color:
            add("Цвет", inferred_color)

    return out


def build_offer_from_raw(raw: dict, *, id_prefix: str = "VT") -> OfferOut | None:
    original_flag = is_original(safe_str(raw.get("name")), safe_str(raw.get("description_body")), safe_str(raw.get("description_meta")))
    clean_title_value = clean_title(norm_ws(raw.get("name")))
    title = append_original_suffix(clean_title_value, original_flag)
    if not title:
        return None

    sku = safe_str(raw.get("sku"))
    source_categories = list(raw.get("source_categories") or ([] if not safe_str(raw.get("category_code")) else [safe_str(raw.get("category_code"))]))
    vendor = guess_vendor(safe_str(raw.get("vendor")), clean_title_value, raw.get("params") or [])
    type_name = infer_type(source_categories, clean_title_value)
    tech = infer_tech(source_categories, type_name, clean_title_value)
    part_number = extract_part_number(raw, raw.get("params") or [], clean_title_value)

    if part_number:
        title_no_suffix = re.sub(r"\s*\(оригинал\)$", "", title, flags=re.I).strip(" ,")
        title_no_suffix = re.sub(rf"(?:,?\s*{re.escape(part_number)})+$", "", title_no_suffix, flags=re.I).strip(" ,")
        if sku:
            title_no_suffix = re.sub(rf"(?:,?\s*{re.escape(sku)})+$", "", title_no_suffix, flags=re.I).strip(" ,")
        title_no_suffix = re.sub(r"(?:,?\s*\d+(?:[.,]\s*\d+)?\s*[KКkк])\s*$", "", title_no_suffix, flags=re.I).strip(" ,")
        title_no_suffix = re.sub(
            r"(?:,?\s*(?:black|photo\s*black|photoblack|matte\s*black|matt\s*black|cyan|yellow|magenta|grey|gray|red|blue|color|colour|"
            r"bk|c|m|y|cl|ml|lc|lm|"
            r"черн(?:ый|ая|ое)?|чёрн(?:ый|ая|ое)?|голуб(?:ой|ая|ое)?|син(?:ий|яя|ее)?|цветн(?:ой|ая|ое)?|желт(?:ый|ая|ое)?|жёлт(?:ый|ая|ое)?|"
            r"пурпурн(?:ый|ая|ое)?|малинов(?:ый|ая|ое)?|сер(?:ый|ая|ое)?|красн(?:ый|ая|ое)?))\s*$",
            "",
            title_no_suffix,
            flags=re.I,
        ).strip(" ,")
        title_no_suffix = ALT_PART_TAIL_RE.sub("", title_no_suffix).strip(" ,/")
        title_no_suffix = re.sub(r"(?:,?\s*\d+(?:[.,]\s*\d+)?\s*[KКkк])\s*$", "", title_no_suffix, flags=re.I).strip(" ,")
        title_no_suffix = re.sub(
            r"(?:,?\s*(?:black|photo\s*black|photoblack|matte\s*black|matt\s*black|cyan|yellow|magenta|grey|gray|red|blue|color|colour|"
            r"bk|c|m|y|cl|ml|lc|lm|"
            r"черн(?:ый|ая|ое)?|чёрн(?:ый|ая|ое)?|голуб(?:ой|ая|ое)?|син(?:ий|яя|ее)?|цветн(?:ой|ая|ое)?|желт(?:ый|ая|ое)?|жёлт(?:ый|ая|ое)?|"
            r"пурпурн(?:ый|ая|ое)?|малинов(?:ый|ая|ое)?|сер(?:ый|ая|ое)?|красн(?:ый|ая|ое)?))\s*$",
            "",
            title_no_suffix,
            flags=re.I,
        ).strip(" ,")
        title_no_suffix = re.sub(r"(?:,?\s*[0-9])\s*$", "", title_no_suffix).strip(" ,")
        title_no_suffix = re.sub(r"(?:,?\s*(?:bk|c|m|y|cl|ml|lc|lm))\s*$", "", title_no_suffix, flags=re.I).strip(" ,")
        title_no_suffix = DUPLICATE_LEAD_RE.sub(r"\1", title_no_suffix).strip(" ,")
        title = append_original_suffix(norm_ws(title_no_suffix), original_flag)

    compat = extract_compat(clean_title_value, vendor, raw.get("params") or [], safe_str(raw.get("description_body")), part_number, sku)
    resource = extract_resource(clean_title_value, raw.get("params") or [], safe_str(raw.get("description_body")))
    codes = collect_codes(raw, raw.get("params") or [], resource, part_number, compat)
    params = _merge_params(raw, vendor, type_name, tech, part_number, codes, clean_title_value, compat, resource)

    raw_price = int(raw.get("price_rub_raw") or 0)
    price = compute_price(raw_price)

    pictures = clean_picture_urls([safe_str(x) for x in (raw.get("pictures") or []) if safe_str(x)])
    if not pictures:
        pictures = [PLACEHOLDER]

    color = ""
    for k, v in params:
        if k == "Цвет" and not color:
            color = norm_color(v)

    desc = build_native_description(
        title=title,
        type_name=type_name,
        part_number=part_number,
        compat=compat,
        resource=resource,
        color=color,
        is_original=original_flag,
        desc_body=safe_str(raw.get("description_body") or raw.get("description_meta")),
    )

    oid = make_oid(sku, clean_title_value)
    if id_prefix and not oid.startswith(id_prefix):
        oid = id_prefix + oid.lstrip()

    return OfferOut(
        oid=oid,
        available=True,
        name=title,
        price=price,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=desc,
    )
