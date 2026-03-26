# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/normalize.py

VTT normalization helpers.
"""

from __future__ import annotations

import re
from typing import Sequence


CATEGORY_TYPE_MAP: dict[str, str] = {
    "DRM_CRT": "Драм-картридж",
    "DRM_UNIT": "Драм-юнит",
    "CARTLAS_ORIG": "Картридж",
    "CARTLAS_COPY": "Копи-картридж",
    "CARTLAS_PRINT": "Принт-картридж",
    "CARTLAS_TNR": "Тонер-картридж",
    "CARTINJ_PRNTHD": "Печатающая головка",
    "CARTINJ_Refill": "Чернила",
    "CARTINJ_ORIG": "Картридж",
    "CARTMAT_CART": "Картридж",
    "TNR_WASTETON": "Контейнер для отработанного тонера",
    "DEV_DEV": "Девелопер",
    "TNR_REFILL": "Тонер",
    "INK_COMMON": "Чернила",
    "PARTSPRINT_DEVUN": "Блок проявки",
}

TECH_BY_CATEGORY: dict[str, str] = {
    "DRM_CRT": "Лазерная",
    "DRM_UNIT": "Лазерная",
    "CARTLAS_ORIG": "Лазерная",
    "CARTLAS_COPY": "Лазерная",
    "CARTLAS_PRINT": "Лазерная",
    "CARTLAS_TNR": "Лазерная",
    "CARTINJ_PRNTHD": "Струйная",
    "CARTINJ_Refill": "Струйная",
    "CARTINJ_ORIG": "Струйная",
    "CARTMAT_CART": "Матричная",
    "TNR_WASTETON": "Лазерная",
    "DEV_DEV": "Лазерная",
    "TNR_REFILL": "Лазерная",
    "INK_COMMON": "Струйная",
    "PARTSPRINT_DEVUN": "Лазерная",
}

VENDOR_HINTS = (
    "HP", "Canon", "Xerox", "Brother", "Kyocera", "Samsung", "Epson", "Ricoh",
    "Konica Minolta", "Pantum", "Lexmark", "Oki", "Sharp", "Panasonic",
    "Toshiba", "Develop", "Gestetner", "RISO",
)

CODE_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9\-./]{2,}\b")
TITLE_TAIL_RE = re.compile(r"\s*,?\s*(?:купить|цена|в\s+компании\s+втт|в\s+компании\s+vtt).*$", re.I)
ORIGINAL_MARK_RE = re.compile(r"(?<!\w)\((?:O|О|OEM)\)(?!\w)|\bоригинал(?:ьн(?:ый|ая|ое|ые))?\b", re.I)
DUPLICATE_LEAD_RE = re.compile(r"^([A-Z0-9][A-Z0-9\-./]{2,})\s*,\s*\1\b", re.I)
RES_RE = re.compile(r"(?<!\d)(\d+(?:[.,]\d+)?)\s*([kк]|ml|мл|l|л)\b", re.I)


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def norm_ws(text: str) -> str:
    return " ".join(safe_str(text).replace("\xa0", " ").split()).strip()


def canon_vendor(vendor: str) -> str:
    v = norm_ws(vendor)
    low = v.casefold()
    mapping = {
        "kyocera-mita": "Kyocera",
        "kyocera mita": "Kyocera",
        "konica-minolta": "Konica Minolta",
        "konica minolta": "Konica Minolta",
        "hewlett-packard": "HP",
        "hewlett packard": "HP",
    }
    return mapping.get(low, v)


def first_code(text: str) -> str:
    for code in CODE_TOKEN_RE.findall(text or ""):
        code = code.strip(".-/")
        if len(code) >= 3 and re.search(r"\d", code):
            return code
    return ""


def is_original(*parts: str) -> bool:
    return any(ORIGINAL_MARK_RE.search(safe_str(x)) for x in parts if safe_str(x))


def clean_title(title: str) -> str:
    title = norm_ws(title)
    title = TITLE_TAIL_RE.sub("", title).strip(" ,.-")
    title = ORIGINAL_MARK_RE.sub("", title)
    title = norm_ws(title).strip(" ,.-")
    while True:
        new_title = DUPLICATE_LEAD_RE.sub(r"\1", title).strip(" ,")
        if new_title == title:
            break
        title = new_title
    m = re.match(r"^(.+?)(?:,\s*([A-Z0-9][A-Z0-9\-./]{2,}))$", title, re.I)
    if m:
        head, tail = m.group(1), m.group(2)
        if first_code(head).casefold() == tail.casefold():
            title = head
    return norm_ws(title)


def append_original_suffix(title: str, original: bool) -> str:
    title = norm_ws(title)
    if not original:
        return title
    if "оригинал" in title.casefold():
        return title
    return f"{title} (оригинал)"


def make_oid(sku: str, title: str) -> str:
    base = safe_str(sku) or first_code(title) or re.sub(r"[^A-Za-z0-9]+", "", title)[:28]
    base = re.sub(r"[^A-Za-z0-9._/-]+", "", base)
    return "VT" + base


def guess_vendor(raw_vendor: str, title: str, params: Sequence[tuple[str, str]]) -> str:
    vendor = canon_vendor(raw_vendor)
    if vendor:
        return vendor
    for k, v in params:
        key = safe_str(k).lower()
        val = canon_vendor(norm_ws(v))
        if any(x in key for x in ("бренд", "vendor", "марка", "производ")) and val:
            return val
    upper = f" {title.upper()} "
    for candidate in VENDOR_HINTS:
        if f" {candidate.upper()} " in upper:
            return canon_vendor(candidate)
    return ""


def format_resource_value(value: str) -> str:
    val = norm_ws(value).replace(" ", "")
    m = re.fullmatch(r"(\d+)(?:[.,](\d+))?", val)
    if m:
        whole = int(m.group(1))
        frac = m.group(2) or ""
        number = float(f"{m.group(1)}.{frac}") if frac else float(whole)
        if number >= 1000:
            k = number / 1000.0
            if abs(k - round(k)) < 1e-9:
                return f"{int(round(k))}K"
            s = f"{k:.1f}".replace(".", ",").rstrip("0").rstrip(",")
            return f"{s}K"
        return val

    m = re.fullmatch(r"(\d+(?:[.,]\d+)?)\s*([kк]|ml|мл|l|л)", norm_ws(value), re.I)
    if not m:
        return norm_ws(value)
    num, unit = m.group(1), m.group(2)
    if unit.casefold() in {"k", "к"}:
        return f"{num}K"
    if unit.casefold() in {"ml", "мл"}:
        return f"{num} мл"
    if unit.casefold() in {"l", "л"}:
        return f"{num} л"
    return norm_ws(value)


def infer_type_by_title(title: str) -> str:
    low = title.casefold()
    checks = [
        ("тонер-картридж", "Тонер-картридж"),
        ("копи-картридж", "Копи-картридж"),
        ("принт-картридж", "Принт-картридж"),
        ("драм-картридж", "Драм-картридж"),
        ("драм-юниты", "Драм-юнит"),
        ("драм-юнит", "Драм-юнит"),
        ("контейнер для отработанного тонера", "Контейнер для отработанного тонера"),
        ("контейнер", "Контейнер для отработанного тонера"),
        ("блок проявки", "Блок проявки"),
        ("бункер", "Бункер отработанного тонера"),
        ("фотобарабан", "Фотобарабан"),
        ("барабан", "Барабан"),
        ("девелопер", "Девелопер"),
        ("печатающая головка", "Печатающая головка"),
        ("головка печатающая", "Печатающая головка"),
        ("головка", "Печатающая головка"),
        ("чернила", "Чернила"),
        ("тонер", "Тонер"),
        ("носитель", "Носитель девелопера"),
        ("картриджи", "Картридж"),
        ("картридж", "Картридж"),
    ]
    for needle, normalized in checks:
        if low.startswith(needle) or f" {needle}" in low:
            return normalized
    return ""


def infer_type(category_codes: Sequence[str], title: str) -> str:
    title_type = infer_type_by_title(title)
    if title_type:
        return title_type
    for code in category_codes:
        val = CATEGORY_TYPE_MAP.get(safe_str(code))
        if val:
            return val
    return ""


def infer_tech(category_codes: Sequence[str], type_name: str, title: str) -> str:
    for code in category_codes:
        val = TECH_BY_CATEGORY.get(safe_str(code))
        if val:
            return val
    low = f"{type_name} {title}".casefold()
    if "стру" in low or "чернил" in low or "головк" in low:
        return "Струйная"
    if "матрич" in low:
        return "Матричная"
    if any(x in low for x in ("картридж", "драм", "девелопер", "тонер", "барабан", "фотобарабан", "блок проявки")):
        return "Лазерная"
    return ""


def norm_color(value: str) -> str:
    val = norm_ws(value)
    low = val.casefold().replace("-", " ").replace("_", " ")
    mapping = {
        "black": "Черный",
        "photo black": "Черный",
        "photoblack": "Черный",
        "matte black": "Черный",
        "matt black": "Черный",
        "matteblack": "Черный",
        "mattblack": "Черный",
        "черный": "Черный",
        "чёрный": "Черный",
        "bk": "Черный",
        "cyan": "Голубой",
        "синий": "Голубой",
        "голубой": "Голубой",
        "c": "Голубой",
        "yellow": "Желтый",
        "желтый": "Желтый",
        "жёлтый": "Желтый",
        "y": "Желтый",
        "magenta": "Пурпурный",
        "малиновый": "Пурпурный",
        "пурпурный": "Пурпурный",
        "m": "Пурпурный",
        "grey": "Серый",
        "gray": "Серый",
        "серый": "Серый",
        "red": "Красный",
        "красный": "Красный",
        "color": "Цветной",
        "colour": "Цветной",
        "цветной": "Цветной",
    }
    return mapping.get(low, val[:1].upper() + val[1:] if val else val)


def infer_color_from_title(title: str) -> str:
    low = title.casefold().replace("-", " ")
    checks = [
        ("photo black", "Черный"),
        ("photoblack", "Черный"),
        ("matte black", "Черный"),
        ("matt black", "Черный"),
        ("matteblack", "Черный"),
        ("mattblack", "Черный"),
        (" black ", "Черный"),
        ("чёрный", "Черный"),
        ("черный", "Черный"),
        (" bk", "Черный"),
        (" cyan", "Голубой"),
        ("синий", "Голубой"),
        ("голубой", "Голубой"),
        (" c,", "Голубой"),
        (" yellow", "Желтый"),
        ("жёлтый", "Желтый"),
        ("желтый", "Желтый"),
        (" y,", "Желтый"),
        (" magenta", "Пурпурный"),
        ("малиновый", "Пурпурный"),
        ("пурпурный", "Пурпурный"),
        (" m,", "Пурпурный"),
        (" grey", "Серый"),
        (" gray", "Серый"),
        ("серый", "Серый"),
        (" red", "Красный"),
        ("красный", "Красный"),
        (" color", "Цветной"),
        (" colour", "Цветной"),
        (" цветной", "Цветной"),
    ]
    for needle, value in checks:
        if needle in low:
            return value
    return ""
