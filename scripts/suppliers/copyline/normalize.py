# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/normalize.py
CopyLine normalize layer.

Задача:
- нормализовать title/vendor/model и description-basics;
- не держать supplier extractor-комбайн внутри normalize;
- использовать extractor-patterns из params_page там, где это возможно.
"""

from __future__ import annotations

import re
from typing import Sequence, Tuple

from suppliers.copyline.params_page import CODE_RX

VENDOR_PRIORITY: list[str] = [
    "HP",
    "Canon",
    "Xerox",
    "Kyocera",
    "Brother",
    "Epson",
    "Pantum",
    "Ricoh",
    "Konica-Minolta",
    "Lexmark",
    "Samsung",
    "OKI",
    "RISO",
    "RIPO",
    "Panasonic",
    "Toshiba",
]

_DESC_CUT_HEADERS = (
    "Технические характеристики",
    "Характеристика",
    "Основные характеристики",
    "Характеристики",
)

_CONSUMABLE_TITLE_PREFIXES = (
    "картридж",
    "тонер-картридж",
    "тонер картридж",
    "драм-картридж",
    "драм картридж",
    "drum",
    "drum unit",
    "чернила",
    "девелопер",
    "developer",
    "термоблок",
    "термоэлемент",
)



def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def _norm_spaces(s: str) -> str:
    s = safe_str(s)
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\s*\n\s*", "\n", s)
    return s.strip()


def _normalize_code_token(s: str) -> str:
    s = safe_str(s).upper()
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s*-\s*", "-", s)
    s = re.sub(r"\s+", "", s)
    return s


def _looks_numeric_sku(s: str) -> bool:
    return bool(re.fullmatch(r"\d+", safe_str(s)))


def _is_allowed_numeric_code(s: str) -> bool:
    return bool(re.fullmatch(r"016\d{6}", _normalize_code_token(s)))


def _looks_consumable_title(title: str) -> bool:
    t = safe_str(title).lower()
    return any(t.startswith(prefix) for prefix in _CONSUMABLE_TITLE_PREFIXES)



_TITLE_COLOR_MAP = {
    "yellow": "Желтый",
    "magenta": "Пурпурный",
    "black": "Чёрный",
    "cyan": "Голубой",
}


def _localize_title_color_tokens(title: str) -> str:
    s = _norm_spaces(title)
    for en, ru in _TITLE_COLOR_MAP.items():
        s = re.sub(rf"(?<![A-Za-zА-Яа-яЁё]){en}(?![A-Za-zА-Яа-яЁё])", ru, s, flags=re.I)
    return s


def normalize_title(title: str) -> str:
    s = _localize_title_color_tokens(title)
    s = re.sub(r"\s{2,}", " ", s)
    return s[:240]


def _first_vendor_from_text(texts: Sequence[str]) -> str:
    hay = "\n".join([safe_str(x) for x in texts if safe_str(x)])
    if not hay:
        return ""

    m = re.search(
        r"(?:^|\b)(?:для|for)\s+(HP|Canon|Xerox|Kyocera|Brother|Epson|Pantum|Ricoh|Lexmark|Samsung|OKI|RISO|Panasonic|Toshiba)\b",
        hay,
        flags=re.I,
    )
    if m:
        val = m.group(1)
        if val.upper() == "HP":
            return "HP"
        if val.upper() == "OKI":
            return "OKI"
        return val.capitalize()

    for vendor in VENDOR_PRIORITY:
        if re.search(rf"\b{re.escape(vendor)}\b", hay, flags=re.I):
            return vendor
    return ""


def detect_vendor(*, title: str = "", description: str = "", params: Sequence[Tuple[str, str]] | None = None) -> str:
    params = params or []
    param_texts: list[str] = []
    for k, v in params:
        k2 = safe_str(k)
        v2 = safe_str(v)
        if not k2 or not v2:
            continue
        if k2.casefold() in {"производитель", "vendor", "brand", "для бренда"}:
            direct = _first_vendor_from_text([v2])
            if direct:
                return direct
        param_texts.append(v2)
    return _first_vendor_from_text([title, description, *param_texts])


def _search_code(text: str) -> str:
    hay = _norm_spaces(text)
    if not hay:
        return ""
    hay = re.sub(r"\b(113R|108R|106R|006R|013R|016|C13T|C12C|C33S)\s+(\d{4,8}[A-Z0-9]*)\b", r"\1\2", hay, flags=re.I)
    hay = re.sub(r"\b(CLT|MLT|KX|TK|TN|DR|T|C)\s*-\s*([A-Z0-9]{2,})\b", r"\1-\2", hay, flags=re.I)
    m = CODE_RX.search(hay)
    if m:
        return _normalize_code_token(m.group(0))
    return ""


def detect_model(*, title: str = "", description: str = "", sku: str = "") -> str:
    model = _search_code(title)
    if model:
        return model

    head = re.split(r"(?:используется\s+в|для\s+принтеров|совместимость\s+с\s+устройствами|применяется\s+в)", description, maxsplit=1, flags=re.I)[0]
    model = _search_code(head)
    if model:
        return model

    s = _normalize_code_token(sku)
    if not s:
        return ""
    if _looks_numeric_sku(s) and not _is_allowed_numeric_code(s):
        return ""
    if _looks_consumable_title(title):
        for rx in CODE_PATTERNS:
            if rx.fullmatch(s):
                return s
        return ""
    if re.fullmatch(r"[A-Z0-9._/-]{3,40}", s) and (not _looks_numeric_sku(s) or _is_allowed_numeric_code(s)):
        return s
    return ""


def clean_description(text: str) -> str:
    s = _norm_spaces(text)
    if not s:
        return ""
    for header in _DESC_CUT_HEADERS:
        m = re.search(rf"(^|\n){re.escape(header)}\s*:?", s, flags=re.I)
        if m:
            s = s[: m.start()].strip()
            break
    lines = [x.strip(" -•") for x in s.split("\n") if x.strip(" -•")]
    if not lines:
        return ""
    out = " ".join(lines)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out[:1200]


def normalize_source_basics(
    *,
    title: str,
    sku: str,
    description_text: str,
    params: Sequence[Tuple[str, str]] | None = None,
) -> dict:
    norm_title = normalize_title(title)
    clean_desc = clean_description(description_text)
    vendor = detect_vendor(title=norm_title, description=clean_desc or description_text, params=params)
    model = detect_model(title=norm_title, description=description_text, sku=sku)
    return {
        "title": norm_title,
        "vendor": vendor,
        "model": model,
        "description": clean_desc,
    }
