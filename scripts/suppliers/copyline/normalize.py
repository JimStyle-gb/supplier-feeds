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
from typing import Iterable, List, Sequence, Tuple

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
]

_DESC_CUT_HEADERS = (
    "Технические характеристики",
    "Характеристика",
    "Основные характеристики",
    "Характеристики",
)


CODE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bCF\d{3,4}[A-Z]\b", re.I),
    re.compile(r"\bCE\d{3,4}[A-Z]\b", re.I),
    re.compile(r"\bCB\d{3,4}[A-Z]\b", re.I),
    re.compile(r"\bQ\d{4}[A-Z]\b", re.I),
    re.compile(r"\bW\d{4}[A-Z0-9]{1,4}\b", re.I),
    re.compile(r"\bMLT-[A-Z]\d{3,5}[A-Z0-9/]*\b", re.I),
    re.compile(r"\bTK-?\d{3,5}[A-Z0-9]*\b", re.I),
    re.compile(r"\b106R\d{5}\b", re.I),
    re.compile(r"\b006R\d{5}\b", re.I),
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


def normalize_title(title: str) -> str:
    s = _norm_spaces(title)
    s = re.sub(r"\s{2,}", " ", s)
    return s[:240]



def _first_vendor_from_text(texts: Sequence[str]) -> str:
    hay = "\n".join([safe_str(x) for x in texts if safe_str(x)])
    if not hay:
        return ""

    # 1) прямые конструкции 'для HP', 'для Canon', ...
    m = re.search(
        r"(?:^|\b)(?:для|for)\s+(HP|Canon|Xerox|Kyocera|Brother|Epson|Pantum|Ricoh|Lexmark|Samsung|OKI|RISO)\b",
        hay,
        flags=re.I,
    )
    if m:
        val = m.group(1)
        return "OKI" if val.upper() == "OKI" else val.capitalize() if val.lower() not in {"hp"} else "HP"

    # 2) приоритетные бренды как standalone tokens
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



def detect_model(*, title: str = "", description: str = "", sku: str = "") -> str:
    # 1) пытаемся взять кодовую модель из title/desc
    hay = "\n".join([safe_str(title), safe_str(description)])
    for rx in CODE_PATTERNS:
        m = rx.search(hay)
        if m:
            return m.group(0).upper().replace(" ", "")

    # 2) SKU как fallback
    s = safe_str(sku)
    if s and re.fullmatch(r"[A-Za-z0-9._/-]{3,40}", s):
        return s
    return ""



def clean_description(text: str) -> str:
    s = _norm_spaces(text)
    if not s:
        return ""
    # оставляем только supplier-body до технических заголовков
    for header in _DESC_CUT_HEADERS:
        m = re.search(rf"(^|\n){re.escape(header)}\s*:?", s, flags=re.I)
        if m:
            s = s[: m.start()].strip()
            break
    # склеить короткий supplier-body в 1–2 предложения
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
