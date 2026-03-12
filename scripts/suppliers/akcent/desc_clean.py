# -*- coding: utf-8 -*-
"""
AkCent description clean layer.

Что делает:
- чистит supplier description
- сохраняет полезный текст
- подготавливает описание для desc_extract.py
- не генерирует новые факты, а только приводит текст в стабильный вид
"""

from __future__ import annotations

import html
import re
from typing import Any

from suppliers.akcent.normalize import NormalizedOffer


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_BR_RE = re.compile(r"(?i)<\s*br\s*/?\s*>")
_P_RE = re.compile(r"(?i)</\s*p\s*>")
_UL_END_RE = re.compile(r"(?i)</\s*(ul|ol)\s*>")
_LI_RE = re.compile(r"(?i)<\s*li[^>]*>")
_ENTITY_WS_RE = re.compile(r"[\xa0\u200b\ufeff]+")
_MULTI_NL_RE = re.compile(r"\n{3,}")
_MULTI_WS_RE = re.compile(r"[ \t]{2,}")
_TAB_SPLIT_RE = re.compile(r"\t+")
_BULLET_PREFIX_RE = re.compile(r"^\s*[•·●▪▫■\-–—]+\s*")
_LABEL_VALUE_INLINE_RE = re.compile(
    r"^\s*([A-Za-zА-Яа-яЁё0-9 /+\-().,%\"№]{2,80})\s*[:\-]\s*(.{1,1000})\s*$"
)

# Явные мусорные куски, которые часто не нужны в clean-text
_NOISE_LINE_PARTS = [
    "подробное описание на сайте производителя",
    "уточняйте у менеджера",
    "цена может отличаться",
    "изображение может отличаться",
    "характеристики могут быть изменены",
    "характеристики товара могут быть изменены",
    "комплектация может отличаться",
    "внешний вид товара может отличаться",
]


def _norm_space(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = _ENTITY_WS_RE.sub(" ", s)
    s = _MULTI_WS_RE.sub(" ", s)
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n[ \t]+", "\n", s)
    s = _MULTI_NL_RE.sub("\n\n", s)
    return s.strip()


def _html_to_text(raw: str) -> str:
    s = raw or ""
    s = html.unescape(s)

    # Сначала переводим структурные HTML-теги в переносы
    s = _BR_RE.sub("\n", s)
    s = _P_RE.sub("\n", s)
    s = _UL_END_RE.sub("\n", s)
    s = _LI_RE.sub("\n• ", s)

    # Потом режем остальные теги
    s = _HTML_TAG_RE.sub(" ", s)
    return _norm_space(s)


def _split_tab_lines(line: str) -> list[str]:
    parts = [x.strip(" ;|") for x in _TAB_SPLIT_RE.split(line) if x.strip(" ;|")]
    if len(parts) <= 1:
        return [line.strip()]

    out: list[str] = []
    if len(parts) % 2 == 0:
        for i in range(0, len(parts), 2):
            k = parts[i].strip()
            v = parts[i + 1].strip()
            if k and v:
                out.append(f"{k}: {v}")
            elif k:
                out.append(k)
    else:
        out.extend(parts)

    return [x for x in out if x]


def _cleanup_line(line: str) -> str:
    line = html.unescape(line or "")
    line = line.replace("\xa0", " ").replace("\ufeff", " ").replace("\u200b", " ")
    line = line.strip()

    # Убираем bullet-prefix в начале, но не ломаем смысл
    line = _BULLET_PREFIX_RE.sub("", line).strip()

    # Частые некрасивые хвосты
    line = line.strip(" ;|")
    line = re.sub(r"\s{2,}", " ", line)

    return line


def _looks_like_noise(line: str) -> bool:
    lc = line.casefold()
    if not lc:
        return True

    for part in _NOISE_LINE_PARTS:
        if part in lc:
            return True

    return False


def _normalize_inline_label_value(line: str) -> str:
    m = _LABEL_VALUE_INLINE_RE.match(line)
    if not m:
        return line

    left = m.group(1).strip(" :;-")
    right = m.group(2).strip(" :;-")
    if not left or not right:
        return line

    # Отсекаем слишком narrative-строки, чтобы не превращать абзацы в фейковые params
    if len(left.split()) > 8:
        return line

    return f"{left}: {right}"


def _dedupe_keep_order(lines: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for line in lines:
        key = re.sub(r"\s+", " ", line.strip()).casefold()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(line)

    return out


def clean_description_text(offer: NormalizedOffer) -> tuple[str, dict[str, Any]]:
    raw = offer.description or ""
    text = _html_to_text(raw)

    if not text:
        return "", {
            "raw_len": len(raw),
            "clean_len": 0,
            "lines_before": 0,
            "lines_after": 0,
        }

    raw_lines = [x for x in text.split("\n")]
    lines_out: list[str] = []

    for raw_line in raw_lines:
        raw_line = raw_line.strip()
        if not raw_line:
            lines_out.append("")
            continue

        for part in _split_tab_lines(raw_line):
            line = _cleanup_line(part)
            line = _normalize_inline_label_value(line)

            if not line:
                continue
            if _looks_like_noise(line):
                continue

            lines_out.append(line)

    # Схлопываем пустые блоки
    compact: list[str] = []
    prev_blank = True
    for line in lines_out:
        if not line.strip():
            if not prev_blank:
                compact.append("")
            prev_blank = True
            continue
        compact.append(line)
        prev_blank = False

    compact = _dedupe_keep_order(compact)
    cleaned = "\n".join(compact).strip()
    cleaned = _norm_space(cleaned)

    report: dict[str, Any] = {
        "raw_len": len(raw),
        "clean_len": len(cleaned),
        "lines_before": len([x for x in raw_lines if x.strip()]),
        "lines_after": len([x for x in cleaned.split("\n") if x.strip()]),
    }
    return cleaned, report


def clean_description_bulk(
    offers: list[NormalizedOffer],
) -> tuple[dict[str, str], dict[str, Any]]:
    mapping: dict[str, str] = {}
    total_raw_len = 0
    total_clean_len = 0
    lines_before = 0
    lines_after = 0

    for offer in offers:
        cleaned, rep = clean_description_text(offer)
        mapping[offer.oid] = cleaned

        total_raw_len += int(rep["raw_len"])
        total_clean_len += int(rep["clean_len"])
        lines_before += int(rep["lines_before"])
        lines_after += int(rep["lines_after"])

    report: dict[str, Any] = {
        "offers": len(offers),
        "raw_len_total": total_raw_len,
        "clean_len_total": total_clean_len,
        "lines_before_total": lines_before,
        "lines_after_total": lines_after,
    }
    return mapping, report
