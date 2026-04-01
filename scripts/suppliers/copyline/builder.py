# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/builder.py
CopyLine builder layer.

Что изменено в этой версии:
- builder больше не кормит extractor narrative-cleaned текстом;
- для extraction используется отдельный very-light `extract_desc`;
- для показа используется отдельный `display_desc`;
- поддержаны новые source-каналы:
  - raw_desc
  - raw_desc_pairs
  - raw_table_params
- сохранена backward-safe совместимость со старым payload:
  - desc
  - params

Главная идея:
- text-for-data и text-for-display больше не смешиваются;
- главный extractor работает по `extract_desc`, а не по `cleaned_desc`.
"""

from __future__ import annotations

import re
from typing import Iterable, Sequence, Tuple

from cs.core import OfferOut
from suppliers.copyline.compat import reconcile_copyline_params
from suppliers.copyline.desc_clean import clean_description
from suppliers.copyline.desc_extract import extract_desc_params
from suppliers.copyline.normalize import normalize_source_basics
from suppliers.copyline.params_page import extract_page_params
from suppliers.copyline.pictures import full_only_if_present, prefer_full_product_pictures


BRAND_HINTS: tuple[tuple[str, str], ...] = (
    (r"\bKonica[- ]?Minolta\b", "Konica-Minolta"),
    (r"\bToshiba\b", "Toshiba"),
    (r"\bRicoh\b", "Ricoh"),
    (r"\bRICOH\b", "Ricoh"),
    (r"\bPanasonic\b", "Panasonic"),
    (r"\bКАТЮША\b", "КАТЮША"),
    (r"\bKATYUSHA\b", "КАТЮША"),
    (r"\bXerox\b", "Xerox"),
    (r"\bCanon\b", "Canon"),
    (r"\bSamsung\b", "Samsung"),
    (r"\bKyocera\b", "Kyocera"),
    (r"\bBrother\b", "Brother"),
    (r"\bEpson\b", "Epson"),
    (r"\bLexmark\b", "Lexmark"),
    (r"\bRISO\b", "RISO"),
    (r"\bHP\b", "HP"),
)

CODE_SCORE_PATTERNS: tuple[tuple[re.Pattern[str], int], ...] = (
    (re.compile(r"^(?:CF|CE|CB|CC|Q|W)\d", re.I), 100),
    (re.compile(r"^(?:106R|006R|108R|113R|013R)\d", re.I), 100),
    (re.compile(r"^016\d{6}$", re.I), 95),
    (re.compile(r"^(?:MLT-|CLT-|TK-|KX-FA|KX-FAT|C-?EXV|DR-|TN-|C13T|C12C|C33S|T-)", re.I), 95),
    (re.compile(r"^ML-D\d", re.I), 90),
    (re.compile(r"^ML-\d{4,5}[A-Z]\d?$", re.I), 85),
)


# ----------------------------- basic helpers -----------------------------

def safe_str(x: object) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def _norm_spaces(text: str) -> str:
    """Лёгкая нормализация текста без narrative-cleaning."""
    s = safe_str(text).replace("\xa0", " ")
    s = re.sub(r"\r\n?", "\n", s)
    s = re.sub(r"[ \t\f\v]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _mk_oid(sku: str) -> str:
    """Стабильный OID по supplier SKU."""
    sku = safe_str(sku)
    sku = re.sub(r"[^A-Za-z0-9\-\._/]", "", sku)
    return "CL" + sku


def _merge_params(*blocks: Sequence[Tuple[str, str]]) -> list[Tuple[str, str]]:
    """Мягко склеить param-блоки без дублей."""
    out: list[Tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for block in blocks:
        for key, value in block or []:
            k = safe_str(key)
            v = safe_str(value)
            if not k or not v:
                continue
            sig = (k.casefold(), v.casefold())
            if sig in seen:
                continue
            seen.add(sig)
            out.append((k, v))
    return out


def _coerce_pairs(items: Iterable[object]) -> list[Tuple[str, str]]:
    """Нормализовать список сырых pair-элементов к (key, value)."""
    out: list[Tuple[str, str]] = []
    for item in items or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            key = safe_str(item[0])
            value = safe_str(item[1])
        elif isinstance(item, dict):
            key = safe_str(item.get("key") or item.get("name"))
            value = safe_str(item.get("value") or item.get("val"))
        else:
            continue
        if key and value:
            out.append((key, value))
    return out


def _build_extract_desc(raw_desc: str) -> str:
    """
    Подготовить text-for-data.

    ВАЖНО:
    - это НЕ narrative-cleaning;
    - здесь нельзя рано резать теххвост и секции;
    - задача только сделать body пригодным для extraction.
    """
    s = _norm_spaces(raw_desc)
    if not s:
        return ""

    # Убираем только совсем шумные повторяющиеся строки-заполнители.
    lines: list[str] = []
    prev = ""
    for raw in s.splitlines():
        line = _norm_spaces(raw)
        if not line:
            lines.append("")
            prev = ""
            continue
        if line.casefold() == prev.casefold():
            continue
        lines.append(line)
        prev = line

    s = "\n".join(lines)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _is_numeric_model(value: str) -> bool:
    return bool(re.fullmatch(r"\d+", safe_str(value)))


def _is_allowed_numeric_code(value: str) -> bool:
    return bool(re.fullmatch(r"016\d{6}", safe_str(value)))


def _code_score(code: str) -> int:
    token = safe_str(code)
    for rx, score in CODE_SCORE_PATTERNS:
        if rx.search(token):
            return score
    if _is_allowed_numeric_code(token):
        return 95
    return 10


def _first_code_from_params(params: Sequence[Tuple[str, str]]) -> str:
    """Взять лучший код из уже собранных params."""
    best_code = ""
    best_score = -1
    for key, value in params:
        if safe_str(key) != "Коды расходников":
            continue
        parts = [x.strip() for x in re.split(r"\s*,\s*", safe_str(value)) if x.strip()]
        for part in parts:
            score = _code_score(part)
            if score > best_score:
                best_score = score
                best_code = part
    return best_code


def _infer_vendor_from_text(text: str) -> str:
    """Грубый vendor-hint из текста."""
    hay = safe_str(text)
    if not hay:
        return ""
    for pattern, vendor in BRAND_HINTS:
        if re.search(pattern, hay, flags=re.I):
            return vendor
    return ""


def _infer_vendor_from_compat(params: Sequence[Tuple[str, str]]) -> str:
    """Попытаться понять vendor по полю совместимости."""
    compat = ""
    for key, value in params:
        if safe_str(key) == "Совместимость":
            compat = safe_str(value)
            break
    return _infer_vendor_from_text(compat)


def _drop_weak_params(params: Sequence[Tuple[str, str]]) -> list[Tuple[str, str]]:
    """Отфильтровать совсем слабые значения."""
    bad_values = {"-", "—", "нет", "n/a", "null"}
    out: list[Tuple[str, str]] = []
    for key, value in params:
        k = safe_str(key)
        v = safe_str(value)
        if not k or not v:
            continue
        if v.casefold() in bad_values:
            continue
        out.append((k, v))
    return out


def _has_consumable_type(params: Sequence[Tuple[str, str]]) -> bool:
    """Понять, является ли товар расходником."""
    consumable_types = {"Картридж", "Тонер-картридж", "Драм-картридж", "Девелопер", "Чернила"}
    return any(safe_str(key) == "Тип" and safe_str(value) in consumable_types for key, value in params)


# ----------------------------- resolve helpers -----------------------------

def _resolve_source_channels(page: dict) -> tuple[str, list[Tuple[str, str]], list[Tuple[str, str]], list[Tuple[str, str]]]:
    """
    Собрать source-каналы с backward-safe совместимостью.

    Возвращает:
    - raw_desc
    - raw_desc_pairs
    - raw_table_params
    - legacy_params
    """
    raw_desc = safe_str(page.get("raw_desc") or page.get("desc"))
    raw_desc_pairs = _coerce_pairs(page.get("raw_desc_pairs") or [])
    raw_table_params = _coerce_pairs(page.get("raw_table_params") or [])
    legacy_params = _coerce_pairs(page.get("params") or [])
    return raw_desc, raw_desc_pairs, raw_table_params, legacy_params


def _resolve_page_basics(page: dict, *, fallback_title: str) -> tuple[str, str, str, str, str, str, list[Tuple[str, str]]]:
    """
    Подготовить basics и развести text-for-data / text-for-display.

    Возвращает:
    - sku
    - title
    - vendor
    - model
    - extract_desc
    - display_desc
    - page_params_input
    """
    sku = safe_str(page.get("sku"))
    source_title = safe_str(page.get("title") or fallback_title)

    raw_desc, raw_desc_pairs, raw_table_params, legacy_params = _resolve_source_channels(page)

    # Главный extractor должен видеть более полный текст, а не уже narrative-cleaned body.
    extract_desc = _build_extract_desc(raw_desc)

    # normalize.py пока ещё backward-safe и сам умеет clean_description внутри.
    # Мы используем из него только basics, а не его `description`.
    basics = normalize_source_basics(
        title=source_title,
        sku=sku,
        description_text=extract_desc or raw_desc,
        params=raw_table_params or raw_desc_pairs or legacy_params,
    )
    title = safe_str(basics.get("title") or source_title)
    vendor = safe_str(basics.get("vendor"))
    model = safe_str(basics.get("model"))

    # display_desc — отдельный слой только для показа.
    display_desc = clean_description(raw_desc)

    # Для текущего контракта params_page ещё нельзя передать provenance отдельно,
    # поэтому аккуратно собираем input здесь, а не в source.py.
    page_params_input = _merge_params(raw_table_params, raw_desc_pairs, legacy_params)

    return sku, title, vendor, model, extract_desc, display_desc, page_params_input


def _repair_model_param(params: Sequence[Tuple[str, str]], model: str) -> list[Tuple[str, str]]:
    """Подстраховать `Модель` лучшим кодом, если там слабое значение."""
    merged = _merge_params(params, [("Модель", model)]) if model else list(params)

    current_model = ""
    for key, value in merged:
        if safe_str(key) == "Модель":
            current_model = safe_str(value)
            break

    if _is_numeric_model(current_model) and not _is_allowed_numeric_code(current_model):
        first_code = _first_code_from_params(merged)
        out: list[Tuple[str, str]] = []
        for key, value in merged:
            if safe_str(key) != "Модель":
                out.append((key, value))
        if first_code:
            out.append(("Модель", first_code))
        return out

    if not current_model:
        first_code = _first_code_from_params(merged)
        if first_code:
            return _merge_params(merged, [("Модель", first_code)])

    return merged


def _resolve_vendor(title: str, vendor: str, params: Sequence[Tuple[str, str]]) -> str:
    """Финально определить vendor по basics → compat → title."""
    resolved = safe_str(vendor)
    if not resolved:
        resolved = _infer_vendor_from_compat(params)
    if not resolved:
        resolved = _infer_vendor_from_text(title)
    return resolved


def _finalize_params(params: Sequence[Tuple[str, str]], vendor: str) -> list[Tuple[str, str]]:
    """Финальный supplier-side cleanup params."""
    merged = list(params)
    if vendor and _has_consumable_type(merged):
        merged = _merge_params(merged, [("Для бренда", vendor)])
    merged = _drop_weak_params(merged)
    merged = reconcile_copyline_params(merged)
    return merged


def _build_pictures(page: dict) -> list[str]:
    """Подготовить supplier pictures."""
    pictures = prefer_full_product_pictures(page.get("pics") or [])
    return full_only_if_present(pictures)


def _resolve_available(_: dict) -> bool:
    """По текущему правилу проекта CopyLine всегда available=true."""
    return True


# ----------------------------- main builder -----------------------------

def build_offer_from_page(page: dict, *, fallback_title: str = "") -> OfferOut | None:
    """Собрать raw OfferOut из page-payload."""
    sku, title, vendor, model, extract_desc, display_desc, page_params_input = _resolve_page_basics(
        page,
        fallback_title=fallback_title,
    )
    if not sku or not title:
        return None

    # Главный extractor должен работать по text-for-data.
    page_params = extract_page_params(
        title=title,
        description=extract_desc,
        page_params=page_params_input,
    )

    # Fill-missing слой тоже работает по text-for-data, а не по display narrative.
    desc_params = extract_desc_params(
        title=title,
        description=extract_desc,
        existing_params=page_params,
    )

    params = _merge_params(page_params, desc_params)
    params = _repair_model_param(params, model)
    vendor = _resolve_vendor(title, vendor, params)
    params = _finalize_params(params, vendor)

    pictures = _build_pictures(page)
    raw_price = int(page.get("price_raw") or 0)
    available = _resolve_available(page)

    return OfferOut(
        oid=_mk_oid(sku),
        available=available,
        name=title,
        price=raw_price,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=(display_desc or title),
    )
