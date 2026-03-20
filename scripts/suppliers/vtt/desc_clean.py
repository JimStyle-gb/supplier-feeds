# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/desc_clean.py

VTT description clean layer.

Задача:
- принять сырой supplier text из source/normalize;
- убрать очевидный мусор до RAW;
- не строить финальный HTML (это делает общий CS description builder);
- вернуть чистый native_desc для builder.py.
"""

from __future__ import annotations

import re


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def norm_spaces(text: str) -> str:
    s = safe_str(text)
    if not s:
        return ""
    s = s.replace("\xa0", " ").replace("&nbsp;", " ")
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\s*\n\s*", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


_SERVICE_PATTERNS = [
    re.compile(r"\bкупить\b", re.I),
    re.compile(r"\bцена\b", re.I),
    re.compile(r"\bв наличии\b", re.I),
    re.compile(r"\bпод заказ\b", re.I),
    re.compile(r"\bдоставка\b", re.I),
    re.compile(r"\bсамовывоз\b", re.I),
    re.compile(r"\bзаказать\b", re.I),
]

_NOISE_LINE_PATTERNS = [
    re.compile(r"^\s*артикул\s*[:\-]\s*", re.I),
    re.compile(r"^\s*партс-?номер\s*[:\-]\s*", re.I),
    re.compile(r"^\s*вендор\s*[:\-]\s*", re.I),
    re.compile(r"^\s*цена\s*[:\-]\s*", re.I),
    re.compile(r"^\s*штрих-?код\s*[:\-]\s*", re.I),
    re.compile(r"^\s*ean\s*[:\-]\s*", re.I),
    re.compile(r"^\s*barcode\s*[:\-]\s*", re.I),
]

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_PUNCT_RE = re.compile(r"([,;:])(?:\s*\1)+")
_MULTI_SPACE_RE = re.compile(r"\s{2,}")
_MODEL_BLOB_RE = re.compile(
    r"\b(?:HP|Canon|Xerox|Kyocera|Brother|Epson|Pantum|Ricoh|Samsung|Lexmark|OKI)\b"
    r"(?:[\s,/;-]+[A-Za-z0-9][A-Za-z0-9./_-]*){5,}",
    re.I,
)


def strip_html(text: str) -> str:
    """Грубое снятие html-тегов."""
    s = safe_str(text)
    if not s:
        return ""
    s = s.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    s = s.replace("</p>", "\n").replace("</div>", "\n").replace("</li>", "\n")
    s = _HTML_TAG_RE.sub(" ", s)
    s = s.replace("&nbsp;", " ").replace("&amp;", "&")
    return norm_spaces(s)


def is_same_as_title(text: str, title: str) -> bool:
    """Почти полное совпадение description и title."""
    a = norm_spaces(text).casefold().replace("ё", "е")
    b = norm_spaces(title).casefold().replace("ё", "е")
    if not a or not b:
        return False
    if a == b:
        return True
    if a.startswith(b) and len(a) - len(b) <= 12:
        return True
    return False


def looks_like_service_text(text: str) -> bool:
    """Явный SEO/commerce мусор вместо описания."""
    s = norm_spaces(text)
    if not s:
        return False
    hits = sum(1 for rx in _SERVICE_PATTERNS if rx.search(s))
    return hits >= 2


def _drop_noise_lines(text: str) -> str:
    lines = [x.strip() for x in re.split(r"[\n\r]+", text) if x.strip()]
    out: list[str] = []
    for line in lines:
        if any(rx.search(line) for rx in _NOISE_LINE_PATTERNS):
            continue
        out.append(line)
    return "\n".join(out).strip()


def _trim_model_blob(text: str) -> str:
    """
    Режет слишком длинные model/compat хвосты в narrative.
    Совместимость должна жить в params/compat, а не в native_desc.
    """
    s = norm_spaces(text)
    if not s:
        return ""
    return _MODEL_BLOB_RE.sub("", s).strip(" ,;:-")


def _clean_text(text: str) -> str:
    s = strip_html(text)
    if not s:
        return ""

    s = _drop_noise_lines(s)
    s = _trim_model_blob(s)

    s = s.replace(" ,", ",").replace(" .", ".")
    s = _MULTI_PUNCT_RE.sub(r"\1", s)
    s = _MULTI_SPACE_RE.sub(" ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = s.strip(" ,;:-")
    return s.strip()


def merge_desc_sources(meta_desc: str, body_desc: str) -> str:
    """Склеивает meta/body без дублей."""
    meta = _clean_text(meta_desc)
    body = _clean_text(body_desc)

    parts: list[str] = []
    for chunk in (meta, body):
        if not chunk:
            continue
        if chunk not in parts:
            parts.append(chunk)

    out = "\n\n".join(parts).strip()
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out


def clean_vtt_native_desc(
    *,
    title: str,
    meta_desc: str = "",
    body_desc: str = "",
    native_desc: str = "",
) -> str:
    """
    Возвращает supplier-clean native_desc для RAW.

    Приоритет:
    1) если уже есть native_desc из normalize — дочищаем его;
    2) иначе склеиваем meta/body;
    3) если текст по сути дублирует title или является service-мусором — глушим.
    """
    title_clean = norm_spaces(title)

    base = _clean_text(native_desc)
    if not base:
        base = merge_desc_sources(meta_desc, body_desc)

    if not base:
        return ""

    if title_clean and is_same_as_title(base, title_clean):
        return ""

    if looks_like_service_text(base):
        return ""

    # Очень короткие однотипные хвосты не тащим.
    if len(base) < 30 and title_clean:
        return ""

    return base[:4000]
