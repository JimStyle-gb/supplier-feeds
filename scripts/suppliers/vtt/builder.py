# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/builder.py

VTT builder layer.
v25:
- restores v22-safe title cleanup;
- explicitly repairs Xerox WC 7525/.../7835 title row from compat;
- removes remaining Canon 052H and HP 651 title tails after compat cleanup;
- fixes Hi-Black 727 titles and color override from explicit title color;
- uses OEM-like display part number for Hi-Black/internal numeric cases;
- keeps description in sync with display part number used in params;
- keeps title short for SEO: type + family/code + compatibility + originality;
- leaves compat-specific logic to compat.py.
"""

from __future__ import annotations

import re

from cs.core import OfferOut, compute_price

from .compat import ALT_PART_TAIL_RE, CODE_SOURCE_KEYS, collect_codes, derive_display_part_number, derive_hiblack_color, extract_compat, extract_part_number
from .desc_extract import build_native_description, extract_resource
from .normalize import (
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
    "Артикул", "Штрих-код", "Вендор", "Категория", "Подкатегория",
    "В упаковке, штук", "Местный склад, штук", "Местный, до новой поставки, дней",
    "Склад Москва, штук", "Москва, до новой поставки, дней", "Категория VTT",
}
DUPLICATE_LEAD_RE = re.compile(r"^([A-Z0-9][A-Z0-9\-./]{2,})\s*,\s*\1\b", re.I)
_TITLE_COLOR_TAIL_RE = re.compile(
    r"(?:,?\s*(?:black|photo\s*black|photoblack|matte\s*black|matt\s*black|cyan|yellow|magenta|grey|gray|red|blue|color|colour|"
    r"bk|c|m|y|cl|ml|lc|lm|"
    r"черн(?:ый|ая|ое)?|чёрн(?:ый|ая|ое)?|голуб(?:ой|ая|ое)?|син(?:ий|яя|ее)?|цветн(?:ой|ая|ое)?|желт(?:ый|ая|ое)?|жёлт(?:ый|ая|ое)?|"
    r"пурпурн(?:ый|ая|ое)?|малинов(?:ый|ая|ое)?|сер(?:ый|ая|ое)?|красн(?:ый|ая|ое)?))\s*$",
    re.I,
)

def _merge_params(raw: dict, vendor: str, type_name: str, tech: str, part_number: str, display_part_number: str, codes: list[str], title: str, compat: str, resource: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    color_found = ""

    def add(k: str, v: str) -> None:
        key = norm_ws(k); val = norm_ws(v)
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
        x in type_name.casefold() for x in ("картридж", "драм", "девелопер", "чернила", "тонер", "головка", "блок", "барабан", "контейнер", "носитель")
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

    if display_part_number:
        add("Партномер", display_part_number)
    if compat:
        add("Совместимость", compat)
    if resource:
        add("Ресурс", resource)
    if codes:
        add("Коды расходников", ", ".join(codes))

    inferred_color = infer_color_from_title(title)
    hiblack_color = derive_hiblack_color(title=title, raw_part_number=part_number)
    final_color = hiblack_color or inferred_color

    if final_color:
        replaced = False
        if color_found and final_color != color_found and ("Hi-Black" in title or any(x in title for x in ("Cyan", "Magenta", "Yellow", "Grey", "Gray", "Photoblack", "Mattblack"))):
            out2: list[tuple[str, str]] = []
            for k, v in out:
                if k == "Цвет" and not replaced:
                    out2.append(("Цвет", final_color))
                    replaced = True
                else:
                    out2.append((k, v))
            out = out2
            color_found = final_color
        elif not color_found:
            add("Цвет", final_color)

    return out

def _strip_tail_noise(title_no_suffix: str) -> str:
    changed = True
    t = title_no_suffix
    while changed and t:
        before = t
        t = re.sub(r"\(\s*уцен[^)]*\)\s*$", "", t, flags=re.I).strip(" ,")
        t = re.sub(r"(?:,?\s*\d+(?:[.,]\s*\d+)?\s*[KКkк])\s*$", "", t, flags=re.I).strip(" ,")
        t = re.sub(r"(?:,?\s*\d+(?:[.,]\s*\d+)?\s*(?:мл|ml|л|l))\s*$", "", t, flags=re.I).strip(" ,")
        t = _TITLE_COLOR_TAIL_RE.sub("", t).strip(" ,")
        t = ALT_PART_TAIL_RE.sub("", t).strip(" ,/")
        t = re.sub(r"(?:,\s*|\s+)(?:0|1|2|3|4|5|6|7|8|9)\s*$", "", t).strip(" ,")
        t = re.sub(r"(?:,?\s*0[.,]\s*[36]\s*[KКkк])\s*$", "", t, flags=re.I).strip(" ,")
        t = re.sub(r"(?:,\s*|\s+)(?:bk|c|m|y|cl|ml|lc|lm)\s*$", "", t, flags=re.I).strip(" ,")
        changed = t != before
    return t

def _repair_known_titles(title_no_suffix: str, compat: str) -> str:
    t = norm_ws(title_no_suffix)
    comp = norm_ws(compat)

    if t.startswith("Тонер-картридж Xerox для WC ") and comp.startswith("Xerox WC "):
        row = comp[len("Xerox "):]
        if "/7835" in row:
            return "Тонер-картридж Xerox для WC 7525/7530/7535/7545/7556/7830/7835"
        return f"Тонер-картридж Xerox для {row}"

    if t.startswith("Тонер-картридж Xerox Color C60/"):
        return "Тонер-картридж Xerox Color C60/C70"

    if t.startswith("Тонер-картридж Xerox DC S"):
        return "Тонер-картридж Xerox DC SC2020"

    if t.startswith("Картридж 052H для Canon MF421dw/MF426dw/MF428x/MF429x"):
        return "Картридж 052H для Canon MF421dw/MF426dw/MF428x/MF429x"

    if t.startswith("Картридж 651 для HP DJ 5645"):
        return "Картридж 651 для HP DJ 5645"

    if "Hi-Black" in t and "HP DJ T920/T1500" in t:
        m = re.search(r"Hi-Black\s*\(([^)]+)\)", t)
        oem = m.group(1).strip() if m else ""
        if oem:
            return f"Картридж Hi-Black 727 для HP DJ T920/T1500 {oem}"
        return "Картридж Hi-Black 727 для HP DJ T920/T1500"

    if "Hi-Black 46" in t and "HP DJ 2020/2520" in t:
        m = re.search(r"\b(CZ63[78]AE)\b", t, re.I)
        oem = m.group(1).upper() if m else ""
        if oem:
            return f"Картридж Hi-Black 46 для HP DJ 2020/2520 {oem}"
        return "Картридж Hi-Black 46 для HP DJ 2020/2520"

    if "Hi-Black" in t and "HP OJ Pro 6230/6830" in t:
        m = re.search(r"\b(C2P\d{2}AE)\b", t, re.I)
        oem = m.group(1).upper() if m else ""
        if oem:
            return f"Картридж Hi-Black для HP OJ Pro 6230/6830 {oem}"
        return "Картридж Hi-Black для HP OJ Pro 6230/6830"

    return t

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

    title_no_suffix = re.sub(r"\s*\(оригинал\)$", "", title, flags=re.I).strip(" ,")
    if part_number:
        title_no_suffix = re.sub(rf"(?:,?\s*{re.escape(part_number)})+$", "", title_no_suffix, flags=re.I).strip(" ,")
    if sku:
        title_no_suffix = re.sub(rf"(?:,?\s*{re.escape(sku)})+$", "", title_no_suffix, flags=re.I).strip(" ,")
    title_no_suffix = _strip_tail_noise(title_no_suffix)

    compat = extract_compat(clean_title_value, vendor, raw.get("params") or [], safe_str(raw.get("description_body")), part_number, sku)
    title_no_suffix = _repair_known_titles(title_no_suffix, compat)
    title = append_original_suffix(norm_ws(title_no_suffix), original_flag)

    resource = extract_resource(clean_title_value, raw.get("params") or [], safe_str(raw.get("description_body")))
    codes = collect_codes(raw, raw.get("params") or [], resource, part_number, compat)
    display_part_number = derive_display_part_number(title=title, raw_part_number=part_number, codes=codes)
    params = _merge_params(raw, vendor, type_name, tech, part_number, display_part_number, codes, clean_title_value, compat, resource)

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
        part_number=(display_part_number or part_number),
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
