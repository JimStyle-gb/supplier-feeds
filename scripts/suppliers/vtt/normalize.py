# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/normalize.py

VTT normalize layer.

Задача:
- базовая санитарная нормализация сырого item из source.py;
- чистка title / vendor / article / description basis;
- без params-cleanup, compat и OfferOut.
"""

from __future__ import annotations

import re
from typing import Iterable


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
    "Panasonic",
    "Toshiba",
    "Sharp",
    "Develop",
    "Minolta",
]

_VENDOR_ALIASES: dict[str, str] = {
    "hewlettpackard": "HP",
    "hp": "HP",
    "canon": "Canon",
    "xerox": "Xerox",
    "kyocera": "Kyocera",
    "brother": "Brother",
    "epson": "Epson",
    "pantum": "Pantum",
    "ricoh": "Ricoh",
    "lexmark": "Lexmark",
    "samsung": "Samsung",
    "oki": "OKI",
    "riso": "RISO",
    "panasonic": "Panasonic",
    "toshiba": "Toshiba",
    "sharp": "Sharp",
    "konicaminolta": "Konica Minolta",
    "minolta": "Konica Minolta",
    "konica": "Konica Minolta",
    "develop": "Develop",
}

_SERVICE_TAILS = (
    "купить",
    "цена",
    "в наличии",
    "на складе",
    "заказать",
)

_RU_TO_LAT_TABLE = str.maketrans(
    {
        "А": "A",
        "В": "B",
        "Е": "E",
        "К": "K",
        "М": "M",
        "Н": "H",
        "О": "O",
        "Р": "P",
        "С": "C",
        "Т": "T",
        "Х": "X",
        "а": "a",
        "е": "e",
        "о": "o",
        "р": "p",
        "с": "c",
        "х": "x",
    }
)


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def norm_spaces(text: str) -> str:
    s = safe_str(text)
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("&nbsp;", " ")
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\s*\n\s*", "\n", s)
    return s.strip()


def ru_to_lat_ascii(text: str) -> str:
    """Стабилизирует артикула с кириллическими псевдо-латинскими буквами."""
    return safe_str(text).translate(_RU_TO_LAT_TABLE)


def clean_article(article: str) -> str:
    """
    Нормализация article / партномера для oid-basis.
    Оставляем только безопасные символы.
    """
    s = ru_to_lat_ascii(article)
    s = s.replace("—", "-").replace("–", "-")
    s = re.sub(r"\s*[/|]+\s*", "-", s)
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^A-Za-z0-9._-]+", "", s)
    s = re.sub(r"-{2,}", "-", s)
    s = re.sub(r"_{2,}", "_", s)
    s = s.strip("._-")
    return s[:80]


def build_oid(article: str, *, id_prefix: str = "VT") -> str:
    base = clean_article(article)
    return f"{id_prefix}{base}" if base else ""


def _keyify_vendor(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", safe_str(text).lower())


def normalize_vendor(vendor: str) -> str:
    """Приводит vendor к канону проекта."""
    raw = norm_spaces(vendor)
    if not raw:
        return ""
    key = _keyify_vendor(raw)
    if not key:
        return ""
    return _VENDOR_ALIASES.get(key, raw)


def vendor_from_texts(*texts: str) -> str:
    """Пытается вытащить бренд из title/desc, если source vendor пустой."""
    hay = "\n".join([norm_spaces(x) for x in texts if norm_spaces(x)])
    if not hay:
        return ""

    m = re.search(
        r"(?:^|\b)(?:для|for)\s+(HP|Canon|Xerox|Kyocera|Brother|Epson|Pantum|Ricoh|Lexmark|Samsung|OKI|RISO|Panasonic|Toshiba|Sharp)\b",
        hay,
        flags=re.I,
    )
    if m:
        return normalize_vendor(m.group(1))

    for vendor in VENDOR_PRIORITY:
        if re.search(rf"\b{re.escape(vendor)}\b", hay, flags=re.I):
            return vendor
    return ""


def normalize_title(title: str) -> str:
    """
    Лёгкая чистка title без supplier-magic.
    Ничего не вырезаем агрессивно, только санитария.
    """
    s = norm_spaces(title)
    if not s:
        return ""

    s = s.replace(" ,", ",").replace(" .", ".")
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    s = re.sub(r"\[\s+", "[", s)
    s = re.sub(r"\s+\]", "]", s)
    s = re.sub(r"\s{2,}", " ", s)
    s = re.sub(r"\s*([,/;:])\s*", r"\1 ", s)
    s = re.sub(r"\s{2,}", " ", s)

    # лёгкий срез типичного SEO-хвоста
    low = s.lower()
    for tail in _SERVICE_TAILS:
        pos = low.find(" " + tail)
        if pos > 0:
            s = s[:pos].strip()
            break

    return s[:240]


def merge_native_desc(meta_desc: str, body_desc: str, title: str = "") -> str:
    """
    Собирает supplier native_desc до входа в общий description builder.
    Если description == title, не тащим пустой мусор.
    """
    meta = norm_spaces(meta_desc)
    body = norm_spaces(body_desc)
    ttl = norm_spaces(title)

    parts: list[str] = []
    for chunk in (meta, body):
        if not chunk:
            continue
        if ttl and chunk.casefold() == ttl.casefold():
            continue
        if chunk not in parts:
            parts.append(chunk)

    out = "\n".join(parts).strip()
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out[:4000]


def normalize_raw_item(raw: dict, *, id_prefix: str = "VT") -> dict:
    """
    Базовая нормализация сырого VTT item.
    Не трогает params/pictures/compat.
    """
    title = normalize_title(raw.get("name") or "")
    article = clean_article(raw.get("article") or "")
    vendor_src = normalize_vendor(raw.get("vendor") or "")
    if not vendor_src:
        vendor_src = vendor_from_texts(
            title,
            raw.get("description_meta") or "",
            raw.get("description_body") or "",
        )

    native_desc = merge_native_desc(
        raw.get("description_meta") or "",
        raw.get("description_body") or "",
        title=title,
    )

    out = dict(raw)
    out["name"] = title
    out["article"] = article
    out["oid"] = build_oid(article, id_prefix=id_prefix)
    out["vendor"] = vendor_src
    out["native_desc"] = native_desc
    return out


def normalize_many(items: Iterable[dict], *, id_prefix: str = "VT") -> list[dict]:
    """Удобный helper для отладки."""
    return [normalize_raw_item(x, id_prefix=id_prefix) for x in items]
