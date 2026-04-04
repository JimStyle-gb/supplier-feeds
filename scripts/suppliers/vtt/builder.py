# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/builder.py

VTT builder layer.

Patch focus v3:
- finish current critical class `empty_vendor`;
- keep existing VTT-specific title/compat logic intact;
- strengthen vendor inference for the last CET FujiFilm families
  (FUJIFILM Apeos / ApeosPort).

Важно:
- pricing intentionally не меняем;
- compat/title repair intentionally не меняем;
- это точечный raw-quality fix после structural template alignment.
"""

from __future__ import annotations

import re

from cs.core import OfferOut, compute_price

from .compat import (
    ALT_PART_TAIL_RE,
    CODE_SOURCE_KEYS,
    collect_codes,
    derive_display_part_number,
    derive_hiblack_color,
    extract_compat,
    extract_part_number,
)
from .desc_extract import build_native_description, extract_resource
from .normalize import (
    append_original_suffix,
    build_offer_oid,
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
from .pictures import PLACEHOLDER, collect_picture_urls


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

_TITLE_COLOR_TAIL_RE = re.compile(
    r"(?:,?\s*(?:black|photo\s*black|photoblack|matte\s*black|matt\s*black|cyan|yellow|magenta|grey|gray|red|blue|color|colour|"
    r"bk|c|m|y|cl|ml|lc|lm|"
    r"черн(?:ый|ая|ое)?|чёрн(?:ый|ая|ое)?|голуб(?:ой|ая|ое)?|син(?:ий|яя|ее)?|цветн(?:ой|ая|ое)?|желт(?:ый|ая|ое)?|жёлт(?:ый|ая|ое)?|"
    r"пурпурн(?:ый|ая|ое)?|малинов(?:ый|ая|ое)?|сер(?:ый|ая|ое)?|красн(?:ый|ая|ое)?))\s*$",
    re.I,
)

_VENDOR_ALIASES = (
    ("HP", "HP"),
    ("HPE", "HPE"),
    ("Canon", "Canon"),
    ("Xerox", "Xerox"),
    ("Kyocera", "Kyocera"),
    ("Brother", "Brother"),
    ("Epson", "Epson"),
    ("Ricoh", "Ricoh"),
    ("Samsung", "Samsung"),
    ("Lexmark", "Lexmark"),
    ("Pantum", "Pantum"),
    ("Sharp", "Sharp"),
    ("Panasonic", "Panasonic"),
    ("Toshiba", "Toshiba"),
    ("Develop", "Develop"),
    ("Gestetner", "Gestetner"),
    ("RISO", "RISO"),
    ("Avision", "Avision"),
    ("DELI", "Deli"),
    ("Deli", "Deli"),
    ("OKI", "OKI"),
    ("Oki", "OKI"),
    ("Olivetti", "Olivetti"),
    ("Triumph-Adler", "Triumph-Adler"),
    ("FUJIFILM", "FUJIFILM"),
    ("FujiFilm", "FUJIFILM"),
    ("Fujifilm", "FUJIFILM"),
    ("Huawei", "Huawei"),
    ("Катюша", "Катюша"),
    ("F+ imaging", "F+ imaging"),
    ("F+", "F+ imaging"),
    ("Konica Minolta", "Konica Minolta"),
    ("Minolta", "Konica Minolta"),
)

_DEVICE_VENDOR_HINTS = (
    (re.compile(r"(?iu)\b(?:LaserJet|DeskJet|DesignJet|OfficeJet|OJ\s+Pro|Color\s+LaserJet)\b"), "HP"),
    (re.compile(r"(?iu)\b(?:PIXMA|imageRUNNER|imagePRESS|i-SENSYS|LBP|MF\d|TM-\d|PRO-\d)\b"), "Canon"),
    (re.compile(r"(?iu)\b(?:VersaLink|AltaLink|WorkCentre|Phaser|ColorQube|DocuColor|Versant)\b"), "Xerox"),
    (re.compile(r"(?iu)\b(?:ECOSYS|TASKalfa|FS-\d|M\d{4}dn|P\d{4}dn)\b"), "Kyocera"),
    (re.compile(r"(?iu)\b(?:DCP|MFC|HL-\d|TN-\d|DR-\d)\b"), "Brother"),
    (re.compile(r"(?iu)\b(?:SCX|CLP|CLX|ML-\d|SL-[A-Z0-9-]+)\b"), "Samsung"),
    (re.compile(r"(?iu)\b(?:L\d{3,4}|XP-\d|WF-\d|Expression|Stylus)\b"), "Epson"),
    (re.compile(r"(?iu)\b(?:ApeosPort|Apeos)\b"), "FUJIFILM"),
    (re.compile(r"(?iu)\bAvision\b"), "Avision"),
    (re.compile(r"(?iu)\bDELI\b"), "Deli"),
    (re.compile(r"(?iu)\bDeli\b"), "Deli"),
    (re.compile(r"(?iu)\bOlivetti\b"), "Olivetti"),
    (re.compile(r"(?iu)\bTriumph-?Adler\b"), "Triumph-Adler"),
    (re.compile(r"(?iu)\bHuawei\b"), "Huawei"),
    (re.compile(r"(?iu)\bКатюша\b"), "Катюша"),
    (re.compile(r"(?iu)\bF\+\s*imaging\b"), "F+ imaging"),
    (re.compile(r"(?iu)\bPixLab\b"), "Huawei"),
    (re.compile(r"(?iu)\bBizhub\b"), "Konica Minolta"),
)

_FOR_BRAND_PATTERNS = (
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+HP\b"), "HP"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+HPE\b"), "HPE"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Canon\b"), "Canon"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Xerox\b"), "Xerox"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Kyocera\b"), "Kyocera"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Brother\b"), "Brother"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Epson\b"), "Epson"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Ricoh\b"), "Ricoh"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Samsung\b"), "Samsung"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Lexmark\b"), "Lexmark"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Pantum\b"), "Pantum"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Sharp\b"), "Sharp"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Panasonic\b"), "Panasonic"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Toshiba\b"), "Toshiba"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Develop\b"), "Develop"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Gestetner\b"), "Gestetner"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+RISO\b"), "RISO"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Avision\b"), "Avision"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+DELI\b"), "Deli"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Deli\b"), "Deli"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Olivetti\b"), "Olivetti"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Triumph-?Adler\b"), "Triumph-Adler"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+FUJIFILM\b"), "FUJIFILM"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+FujiFilm\b"), "FUJIFILM"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Fujifilm\b"), "FUJIFILM"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Huawei\b"), "Huawei"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Катюша\b"), "Катюша"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+F\+\s*imaging\b"), "F+ imaging"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Konica\s+Minolta\b"), "Konica Minolta"),
    (re.compile(r"(?iu)(?:^|\b)(?:для|for)\s+Minolta\b"), "Konica Minolta"),
)


def _canonical_vendor(value: str) -> str:
    s = norm_ws(value)
    if not s:
        return ""
    for raw, canon in _VENDOR_ALIASES:
        if s.casefold() == raw.casefold():
            return canon
    return s


def _vendor_from_texts(*texts: str) -> str:
    hay = "\n".join([norm_ws(x) for x in texts if norm_ws(x)])
    if not hay:
        return ""

    for rx, vendor in _FOR_BRAND_PATTERNS:
        if rx.search(hay):
            return vendor

    low = hay.casefold()
    for raw, canon in _VENDOR_ALIASES:
        if raw.casefold() in low:
            return canon

    for rx, vendor in _DEVICE_VENDOR_HINTS:
        if rx.search(hay):
            return vendor

    return ""


def _resolve_vendor(*, raw_vendor: str, title: str, params: list[tuple[str, str]], compat: str, description_text: str, codes: list[str], part_number: str, display_part_number: str) -> str:
    vendor = _canonical_vendor(guess_vendor(raw_vendor, title, params))
    if vendor:
        return vendor

    for key, value in params or []:
        key_n = norm_ws(key).casefold()
        val_n = _canonical_vendor(value)
        if not val_n:
            continue
        if key_n in {"для бренда", "бренд", "марка", "vendor", "brand", "производитель"}:
            return val_n

    vendor = _vendor_from_texts(
        compat,
        description_text,
        title,
        display_part_number,
        part_number,
        ", ".join(codes),
        *[f"{k}: {v}" for k, v in params],
    )
    if vendor:
        return vendor

    return ""


def _merge_params(raw: dict, vendor: str, type_name: str, tech: str, part_number: str, display_part_number: str, codes: list[str], title: str, compat: str, resource: str) -> list[tuple[str, str]]:
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
    if vendor and type_name and any(x in type_name.casefold() for x in ("картридж", "драм", "девелопер", "чернила", "тонер", "головка", "блок", "барабан", "контейнер", "носитель")):
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


def build_offer_from_raw(raw: dict, *, id_prefix: str = "VT", placeholder_picture: str | None = None) -> OfferOut | None:
    original_flag = is_original(safe_str(raw.get("name")), safe_str(raw.get("description_body")), safe_str(raw.get("description_meta")))

    clean_title_value = clean_title(norm_ws(raw.get("name")))
    title = append_original_suffix(clean_title_value, original_flag)
    if not title:
        return None

    sku = safe_str(raw.get("sku"))
    raw_params = raw.get("params") or []
    source_categories = list(raw.get("source_categories") or ([] if not safe_str(raw.get("category_code")) else [safe_str(raw.get("category_code"))]))

    vendor_pre = _canonical_vendor(guess_vendor(safe_str(raw.get("vendor")), clean_title_value, raw_params))
    type_name = infer_type(source_categories, clean_title_value)
    tech = infer_tech(source_categories, type_name, clean_title_value)
    part_number = extract_part_number(raw, raw_params, clean_title_value)

    title_no_suffix = re.sub(r"\s*\(оригинал\)$", "", title, flags=re.I).strip(" ,")
    if part_number:
        title_no_suffix = re.sub(rf"(?:,?\s*{re.escape(part_number)})+$", "", title_no_suffix, flags=re.I).strip(" ,")
    if sku:
        title_no_suffix = re.sub(rf"(?:,?\s*{re.escape(sku)})+$", "", title_no_suffix, flags=re.I).strip(" ,")
    title_no_suffix = _strip_tail_noise(title_no_suffix)

    compat = extract_compat(clean_title_value, vendor_pre, raw_params, safe_str(raw.get("description_body")), part_number, sku)
    title_no_suffix = _repair_known_titles(title_no_suffix, compat)
    title = append_original_suffix(norm_ws(title_no_suffix), original_flag)

    resource = extract_resource(clean_title_value, raw_params, safe_str(raw.get("description_body")))
    codes = collect_codes(raw, raw_params, resource, part_number, compat)
    display_part_number = derive_display_part_number(title=title, raw_part_number=part_number, codes=codes)

    vendor = _resolve_vendor(
        raw_vendor=safe_str(raw.get("vendor")),
        title=title,
        params=raw_params,
        compat=compat,
        description_text=safe_str(raw.get("description_body") or raw.get("description_meta")),
        codes=codes,
        part_number=part_number,
        display_part_number=display_part_number,
    )

    params = _merge_params(raw, vendor, type_name, tech, part_number, display_part_number, codes, clean_title_value, compat, resource)

    raw_price = int(raw.get("price_rub_raw") or 0)
    price = compute_price(raw_price)

    pictures = collect_picture_urls([safe_str(x) for x in (raw.get("pictures") or []) if safe_str(x)], placeholder_picture=(placeholder_picture or PLACEHOLDER))

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

    oid = build_offer_oid(raw_vendor_code=sku, raw_id=make_oid(sku, clean_title_value), prefix=id_prefix)
    if not oid:
        return None

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


__all__ = [
    "SKIP_PARAM_KEYS",
    "build_offer_from_raw",
]
