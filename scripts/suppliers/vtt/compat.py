# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/compat.py
"""

from __future__ import annotations

import re
from typing import Sequence

from .normalize import ORIGINAL_MARK_RE, first_code, norm_ws, safe_str

CODE_SOURCE_KEYS = {
    "Каталожный номер",
    "OEM-номер",
    "Партс-номер",
    "Партномер",
    "Аналоги",
}

ORIG_PACK_RE = re.compile(r"(?:\(?\s*ориг\.?\s*фасовк[а-я]*\s*\)?|\(?\s*original\s*pack(?:ing)?\s*\)?)", re.I)
ALT_PART_TAIL_RE = re.compile(r"(?:[,\s]+(?:№\s*)?(?:[A-Z]+\d|\d+[A-Z])[A-Z0-9-]{1,}/?)+$", re.I)

_COLOR_OR_TRAIL_RE = re.compile(
    r"(?:,?\s*(?:black|photo\s*black|photoblack|matte\s*black|matt\s*black|"
    r"cyan|yellow|magenta|grey|gray|red|blue|light\s*cyan|light\s*magenta|"
    r"bk|c|m|y|cl|ml|lc|lm|color|colour|"
    r"черн(?:ый|ая|ое)?|чёрн(?:ый|ая|ое)?|"
    r"голуб(?:ой|ая|ое)?|син(?:ий|яя|ее)?|цветн(?:ой|ая|ое)?|"
    r"желт(?:ый|ая|ое)?|жёлт(?:ый|ая|ое)?|"
    r"пурпурн(?:ый|ая|ое)?|малинов(?:ый|ая|ое)?|"
    r"сер(?:ый|ая|ое)?|красн(?:ый|ая|ое)?))\s*$",
    re.I,
)


def cleanup_compat(value: str, vendor: str, part_number: str = "", sku: str = "") -> str:
    compat = norm_ws(value).strip(" ,.;/")
    if not compat:
        return ""

    compat = ORIGINAL_MARK_RE.sub("", compat)
    compat = ORIG_PACK_RE.sub("", compat).strip(" ,.;/")

    changed = True
    while changed and compat:
        before = compat

        if part_number:
            compat = re.sub(rf"(?<!\w){re.escape(part_number)}(?!\w)", "", compat, flags=re.I).strip(" ,.;/")
        if sku:
            compat = re.sub(rf"(?<!\w){re.escape(sku)}(?!\w)", "", compat, flags=re.I).strip(" ,.;/")

        compat = re.sub(r"(?:,?\s*\d+(?:[.,]\s*\d+)?\s*(?:мл|ml|л|l))\s*$", "", compat, flags=re.I).strip(" ,.;/")
        compat = re.sub(r"(?:,?\s*\d+(?:[.,]\s*\d+)?\s*[KКkк])\s*$", "", compat, flags=re.I).strip(" ,.;/")
        compat = _COLOR_OR_TRAIL_RE.sub("", compat).strip(" ,.;/")
        compat = re.sub(r"(?:,|\s)+(?:№\s*)?(?:[A-Z]+\d|\d+[A-Z])[A-Z0-9-]{1,}/?\s*$", "", compat).strip(" ,.;/")
        compat = re.sub(r"(?:,?\s*[0-9])\s*$", "", compat).strip(" ,.;/")
        compat = re.sub(r"(?:,?\s*(?:bk|c|m|y|cl|ml|lc|lm))\s*$", "", compat, flags=re.I).strip(" ,.;/")
        compat = re.sub(r"\s*,\s*", ", ", compat)
        compat = re.sub(r"\s*/\s*", "/", compat)
        compat = re.sub(r"\s{2,}", " ", compat).strip(" ,.;/")
        changed = compat != before

    if vendor and compat and not compat.upper().startswith(vendor.upper()):
        compat = f"{vendor} {compat}"
    return norm_ws(compat)


def extract_part_number(raw: dict, params: Sequence[tuple[str, str]], title: str) -> str:
    for key, value in params:
        if safe_str(key) in ("Партномер", "Партс-номер", "Каталожный номер", "OEM-номер") and norm_ws(value):
            return norm_ws(value)
    sku = safe_str(raw.get("sku"))
    if sku:
        return sku
    return first_code(title)


def extract_compat(title: str, vendor: str, params: Sequence[tuple[str, str]], desc: str, part_number: str, sku: str = "") -> str:
    for key, value in params:
        k = safe_str(key).casefold()
        if any(x in k for x in ("совмест", "для устройств", "для принтеров", "подходит")):
            val = cleanup_compat(safe_str(value), vendor, part_number, sku)
            if val:
                return val

    clean_title = norm_ws(title)
    m = re.search(r"\bдля\s+(.+)$", clean_title, re.I)
    if m:
        tail = norm_ws(m.group(1))
        tail = cleanup_compat(tail, vendor, part_number, sku)
        if tail:
            return tail

    if desc:
        m = re.search(r"(?:совместим(?:ость|ые)?|подходит для|для принтеров|для устройств)\s*[:\-]?\s*([^.;\n]+)", desc, re.I)
        if m:
            compat = cleanup_compat(m.group(1), vendor, part_number, sku)
            if compat:
                return compat
    return ""


def should_keep_code(code: str, resource: str = "") -> bool:
    code = code.strip(".-/")
    if len(code) < 3:
        return False
    if not re.search(r"\d", code):
        return False
    if "/" in code:
        return False
    if re.fullmatch(r"\d+(?:[.,]\d+)?", code):
        return False
    if re.fullmatch(r"\d+(?:[.,]\d+)?[kкmlл]+", code, re.I):
        return False
    if resource:
        res_norm = resource.replace(" ", "").replace("мл", "ml").replace("л", "l").casefold()
        code_norm = code.replace(" ", "").casefold()
        if code_norm == res_norm:
            return False
    return True


def collect_codes(raw: dict, params: Sequence[tuple[str, str]], resource: str, part_number: str, compat: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    compat_low = norm_ws(compat).casefold()
    raw_title = norm_ws(raw.get("name"))
    title_tail = ""
    m = re.search(r"\bдля\s+(.+)$", raw_title, re.I)
    if m:
        title_tail = norm_ws(m.group(1)).casefold()

    title_start_code = ""
    m = re.search(
        r"^(?:Тонер-картридж|Картридж|Копи-картридж|Принт-картридж|Драм-картридж|Драм-юнит|Девелопер|Чернила|Печатающая головка|Контейнер|Барабан|Фотобарабан)\s+([A-Z0-9][A-Z0-9\-./]{1,})\b",
        raw_title,
        re.I,
    )
    if m:
        title_start_code = norm_ws(m.group(1)).strip(".-/")

    def add(val: str, *, from_title_codes: bool = False) -> None:
        for part in re.split(r"\s*,\s*", safe_str(val)):
            code = part.strip().strip(".-/")
            if not should_keep_code(code, resource):
                continue
            if part_number and code.casefold() == part_number.casefold():
                continue
            if compat_low and re.search(rf"(?<!\w){re.escape(code.casefold())}(?!\w)", compat_low):
                continue
            if from_title_codes:
                if title_tail and re.search(rf"(?<!\w){re.escape(code.casefold())}(?!\w)", title_tail):
                    continue
                if title_start_code and code.casefold() != title_start_code.casefold():
                    continue
                if not title_start_code:
                    continue
            if code not in seen:
                seen.add(code)
                out.append(code)

    sku = safe_str(raw.get("sku"))
    if sku:
        add(sku)
    for key, value in params:
        if safe_str(key) in CODE_SOURCE_KEYS:
            add(safe_str(value))
    for code in raw.get("title_codes") or []:
        add(safe_str(code), from_title_codes=True)
    return out
