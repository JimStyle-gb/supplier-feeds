# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/normalize.py
CopyLine normalize layer.

Задача:
- привести title/description к аккуратному raw-виду;
- попытаться определить vendor на supplier-layer;
- вытащить короткую модель из title/description.

Важно:
- здесь нет CS-логики и нет final-обёртки;
- это только supplier-side normalizer.
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
]

_DESC_CUT_HEADERS = (
    "Технические характеристики",
    "Характеристика",
    "Основные характеристики",
    "Характеристики",
)

_CONSUMABLE_TITLE_RX = re.compile(
    r"^(?:картридж|тонер[- ]картридж|драм[- ]картридж|drum\b|чернила\b|девелопер\b)",
    re.I,
)

CODE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bCF\d{3,4}[A-Z]\b", re.I),
    re.compile(r"\bCE\d{3,4}[A-Z]\b", re.I),
    re.compile(r"\bCB\d{3,4}[A-Z]\b", re.I),
    re.compile(r"\bQ\d{4}[A-Z]\b", re.I),
    re.compile(r"\bW\d{4}[A-Z0-9]{1,4}\b", re.I),
    re.compile(r"\bMLT-[A-Z]\d{3,5}[A-Z0-9/]*\b", re.I),
    re.compile(r"\bCLT-[A-Z]\d{3,5}[A-Z]?\b", re.I),
    re.compile(r"\bTK-?\d{3,5}[A-Z0-9]*\b", re.I),
    re.compile(r"\b113R\d{5}\b", re.I),
    re.compile(r"\b108R\d{5}\b", re.I),
    re.compile(r"\b106R\d{5}\b", re.I),
    re.compile(r"\b006R\d{5}\b", re.I),
    re.compile(r"\bKX-FA\d+[A-Z]?\b", re.I),
    re.compile(r"\bKX-FAT\d+[A-Z]?\b", re.I),
    re.compile(r"\bC13T\d{5,8}[A-Z0-9]*\b", re.I),
    re.compile(r"\bC12C\d{5,8}[A-Z0-9]*\b", re.I),
    re.compile(r"\bC33S\d{5,8}[A-Z0-9]*\b", re.I),
    re.compile(r"\bC-?EXV\d+[A-Z]*\b", re.I),
    re.compile(r"\bUAE-\d+[A-Z0-9-]*\b", re.I),
    re.compile(r"\bDR-\d+[A-Z0-9-]*\b", re.I),
    re.compile(r"\bTN-\d+[A-Z0-9-]*\b", re.I),
]



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
    s = re.sub(r"\s*[-–—]\s*", "-", s)
    s = re.sub(r"\b(113R|108R|106R|006R|C13T|C12C|C33S)\s+(\d{5,8}[A-Z0-9]*)\b", r"\1\2", s)
    s = re.sub(r"\b(CLT|MLT|TK|TN|DR|KX)\s*-\s*", r"\1-", s)
    s = re.sub(r"\bKX\s+(FA|FAT)(\d+[A-Z]?)\b", r"KX-\1\2", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()



def _looks_numeric_sku(s: str) -> bool:
    return bool(re.fullmatch(r"\d+", safe_str(s)))



def normalize_title(title: str) -> str:
    s = _norm_spaces(title)
    s = re.sub(r"\s{2,}", " ", s)
    return s[:240]



def _first_vendor_from_text(texts: Sequence[str]) -> str:
    hay = "\n".join([safe_str(x) for x in texts if safe_str(x)])
    if not hay:
        return ""

    m = re.search(
        r"(?:^|\b)(?:для|for)\s+(HP|Canon|Xerox|Kyocera|Brother|Epson|Pantum|Ricoh|Lexmark|Samsung|OKI|RISO|Panasonic)\b",
        hay,
        flags=re.I,
    )
    if m:
        val = m.group(1)
        upper = val.upper()
        if upper in {"HP", "OKI", "RISO"}:
            return upper
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



def _find_code(text: str) -> str:
    hay = _normalize_code_token(text)
    if not hay:
        return ""
    for rx in CODE_PATTERNS:
        m = rx.search(hay)
        if m:
            return _normalize_code_token(m.group(0))
    return ""



def detect_model(*, title: str = "", description: str = "", sku: str = "") -> str:
    code_from_title = _find_code(title)
    if code_from_title:
        return code_from_title

    code_from_desc = _find_code(description)
    if code_from_desc:
        return code_from_desc

    s = _normalize_code_token(sku)
    if not s:
        return ""
    if _looks_numeric_sku(s):
        return ""
    if _CONSUMABLE_TITLE_RX.search(safe_str(title)) and _looks_numeric_sku(s):
        return ""
    if re.fullmatch(r"[A-Z0-9._/-]{3,40}", s):
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
    """Собрать нормализованный supplier-basics для builder."""
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
