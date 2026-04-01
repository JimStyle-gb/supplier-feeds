# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/normalize.py
CopyLine normalize layer.

Роль модуля:
- только базовая supplier-нормализация;
- нормализует title;
- мягко определяет vendor и model;
- готовит very-light extraction body без narrative-cleaning.

Важно:
- здесь НЕТ owner-логики по display description;
- здесь НЕТ срезания теххвоста/"Характеристики"/"Технические характеристики";
- здесь НЕТ supplier-side semantic merge params.

Идея:
- text-for-data и text-for-display должны быть разведены;
- normalize.py готовит только basics + extraction-body;
- финальный narrative должен собираться позже в desc_clean.py.
"""

from __future__ import annotations

import re
from typing import Sequence, Tuple

VENDOR_PRIORITY: list[str] = [
    "HP",
    "Canon",
    "Xerox",
    "Kyocera",
    "Brother",
    "Epson",
    "Pantum",
    "Ricoh",
    "Konica Minolta",
    "Lexmark",
    "Samsung",
    "OKI",
    "RISO",
    "RIPO",
    "Panasonic",
    "Toshiba",
]

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

_TITLE_COLOR_MAP = {
    "yellow": "Желтый",
    "magenta": "Пурпурный",
    "black": "Чёрный",
    "cyan": "Голубой",
}

CODE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bCF\d{3,4}[A-Z]?\b", re.I),
    re.compile(r"\bCE\d{3,4}[A-Z]?\b", re.I),
    re.compile(r"\bCB\d{3,4}[A-Z]?\b", re.I),
    re.compile(r"\bCC\d{3,4}[A-Z]?\b", re.I),
    re.compile(r"\bQ\d{4}[A-Z]?\b", re.I),
    re.compile(r"\bW\d{4}[A-Z0-9]{1,4}\b", re.I),
    re.compile(r"\bMLT-[A-Z]\d{3,5}[A-Z0-9/]*\b", re.I),
    re.compile(r"\bCLT-[A-Z]\d{3,5}[A-Z]?\b", re.I),
    re.compile(r"\bTK-?\d{3,5}[A-Z0-9]*\b", re.I),
    re.compile(r"\b106R\d{5}\b", re.I),
    re.compile(r"\b006R\d{5}\b", re.I),
    re.compile(r"\b108R\d{5}\b", re.I),
    re.compile(r"\b113R\d{5}\b", re.I),
    re.compile(r"\b013R\d{5}\b", re.I),
    re.compile(r"\b016\d{6}\b", re.I),
    re.compile(r"\bML-D\d+[A-Z]?\b", re.I),
    re.compile(r"\bML-\d{4,5}[A-Z]\d?\b", re.I),
    re.compile(r"\bKX-FA\d+[A-Z]?\b", re.I),
    re.compile(r"\bKX-FAT\d+[A-Z]?\b", re.I),
    re.compile(r"\bT-\d{3,6}[A-Z]?\b", re.I),
    re.compile(r"\bC13T\d{5,8}[A-Z0-9]*\b", re.I),
    re.compile(r"\bC12C\d{5,8}[A-Z0-9]*\b", re.I),
    re.compile(r"\bC33S\d{5,8}[A-Z0-9]*\b", re.I),
    re.compile(r"\bC-?EXV\d+[A-Z]*\b", re.I),
    re.compile(r"\bUAE-\d+[A-Z0-9-]*\b", re.I),
    re.compile(r"\bDR-\d+[A-Z0-9-]*\b", re.I),
    re.compile(r"\bTN-\d+[A-Z0-9-]*\b", re.I),
    re.compile(r"\b(?:50F\d[0-9A-Z]{2,4}|55B\d[0-9A-Z]{2,4}|56F\d[0-9A-Z]{2,4})\b", re.I),
    re.compile(r"\b0?71H\b", re.I),
]


def safe_str(x: object) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""



def _norm_spaces(s: str) -> str:
    """Мягкая нормализация пробелов и переводов строк без narrative-cleaning."""
    s = safe_str(s)
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\r\n?", "\n", s)
    s = re.sub(r"[ \t\f\v]+", " ", s)
    s = re.sub(r" *\n *", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
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



def _localize_title_color_tokens(title: str) -> str:
    s = _norm_spaces(title)
    for en, ru in _TITLE_COLOR_MAP.items():
        s = re.sub(rf"(?<![A-Za-zА-Яа-яЁё]){en}(?![A-Za-zА-Яа-яЁё])", ru, s, flags=re.I)
    return s



def normalize_title(title: str) -> str:
    """Нормализовать title без supplier-side смысловой правки."""
    s = _localize_title_color_tokens(title)
    s = re.sub(r"\s{2,}", " ", s)
    return s[:240]



def _drop_title_echo_from_desc(title: str, description: str) -> str:
    """Убрать только очевидный дубль заголовка в первой строке extraction-body."""
    desc = _norm_spaces(description)
    if not desc:
        return ""
    lines = [ln.strip() for ln in desc.split("\n")]
    if not lines:
        return desc

    first = safe_str(lines[0])
    norm_title = normalize_title(title)
    if first and norm_title and first.casefold() == norm_title.casefold():
        lines = lines[1:]
    return "\n".join([ln for ln in lines if safe_str(ln)]).strip()



def build_extract_description(*, title: str, description_text: str) -> str:
    """
    Подготовить body для extraction.

    Здесь НЕЛЬЗЯ:
    - срезать теххвост;
    - выкидывать секции "Характеристики" / "Описание";
    - схлопывать всё в короткий narrative.
    """
    s = _drop_title_echo_from_desc(title, description_text)
    s = _norm_spaces(s)
    if not s:
        return ""
    return s[:6000]



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
    """Мягко определить vendor по title/description/params."""
    params = params or []
    param_texts: list[str] = []
    for key, value in params:
        key2 = safe_str(key)
        value2 = safe_str(value)
        if not key2 or not value2:
            continue
        if key2.casefold() in {"производитель", "vendor", "brand", "для бренда"}:
            direct = _first_vendor_from_text([value2])
            if direct:
                return direct
        param_texts.append(value2)
    return _first_vendor_from_text([title, description, *param_texts])



def _search_code(text: str) -> str:
    hay = _norm_spaces(text)
    if not hay:
        return ""
    hay = re.sub(r"\b(113R|108R|106R|006R|013R|016|C13T|C12C|C33S)\s+(\d{4,8}[A-Z0-9]*)\b", r"\1\2", hay, flags=re.I)
    hay = re.sub(r"\b(CLT|MLT|KX|TK|TN|DR|T|C)\s*-\s*([A-Z0-9]{2,})\b", r"\1-\2", hay, flags=re.I)
    for rx in CODE_PATTERNS:
        match = rx.search(hay)
        if match:
            return _normalize_code_token(match.group(0))
    return ""



def _description_head_for_model(description: str) -> str:
    """Взять только head текста для поиска модели без device-хвостов."""
    s = _norm_spaces(description)
    if not s:
        return ""
    head = re.split(
        r"(?:используется\s+в|для\s+принтеров|совместимость\s+с\s+устройствами|применяется\s+в)",
        s,
        maxsplit=1,
        flags=re.I,
    )[0]
    return head.strip()



def detect_model(*, title: str = "", description: str = "", sku: str = "") -> str:
    """Определить model/code по title → description head → sku."""
    model = _search_code(title)
    if model:
        return model

    model = _search_code(_description_head_for_model(description))
    if model:
        return model

    sku_norm = _normalize_code_token(sku)
    if not sku_norm:
        return ""
    if _looks_numeric_sku(sku_norm) and not _is_allowed_numeric_code(sku_norm):
        return ""
    if _looks_consumable_title(title):
        for rx in CODE_PATTERNS:
            if rx.fullmatch(sku_norm):
                return sku_norm
        return ""
    if re.fullmatch(r"[A-Z0-9._/-]{3,40}", sku_norm) and (not _looks_numeric_sku(sku_norm) or _is_allowed_numeric_code(sku_norm)):
        return sku_norm
    return ""



def normalize_source_basics(
    *,
    title: str,
    sku: str,
    description_text: str,
    params: Sequence[Tuple[str, str]] | None = None,
) -> dict:
    """
    Вернуть базовую нормализацию для builder.

    Backward-safe:
    - сохраняем ключ `description`, но теперь это extraction-body,
      а не narrative-cleaned display body;
    - дополнительно явно отдаём `extract_desc`.
    """
    norm_title = normalize_title(title)
    extract_desc = build_extract_description(title=norm_title, description_text=description_text)
    vendor = detect_vendor(title=norm_title, description=extract_desc or description_text, params=params)
    model = detect_model(title=norm_title, description=extract_desc or description_text, sku=sku)
    return {
        "title": norm_title,
        "vendor": vendor,
        "model": model,
        "extract_desc": extract_desc,
        "description": extract_desc,
    }
