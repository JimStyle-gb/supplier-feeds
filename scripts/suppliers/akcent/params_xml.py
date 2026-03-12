# -*- coding: utf-8 -*-
"""
AkCent XML params layer.

Что делает:
- чистит только родные supplier XML params
- не генерирует искусственные compat/codes
- не тащит supplier-логику в core
"""

from __future__ import annotations

import html
import re
from collections import Counter
from typing import Any

from suppliers.akcent.normalize import NormalizedOffer


_WS_RE = re.compile(r"\s+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SEP_RE = re.compile(r"\s*(?:[•·●▪▫■]+|[|]+|;{2,}|,{2,})\s*")
_RANGE_DASH_RE = re.compile(r"\s*[-–—]+\s*")

# Мусорные техполя, которые не нужны как merchant params
_BANNED_PARAM_NAMES = {
    "id",
    "offer id",
    "offer_id",
    "товар id",
    "id товара",
    "код товара",
    "внутренний код",
    "url",
    "ссылка",
    "линк",
    "link",
    "цена",
    "price",
    "цена закупа",
    "закупочная цена",
    "stock",
    "остаток",
    "остатки",
    "наличие",
    "в наличии",
    "доступность",
    "категория",
    "category",
    "categoryid",
    "category id",
    "picture",
    "image",
    "фото",
    "картинка",
    "изображение",
}

# Нормализация ключей
_PARAM_NAME_MAP = {
    "мощность (bt)": "Мощность (Вт)",
    "мощность(bt)": "Мощность (Вт)",
    "мощность (w)": "Мощность (Вт)",
    "мощность(w)": "Мощность (Вт)",
    "power, w": "Мощность (Вт)",
    "power (w)": "Мощность (Вт)",
    "power(w)": "Мощность (Вт)",
    "мощность": "Мощность",
    "диагональ экрана": "Диагональ экрана",
    "тип матрицы": "Тип матрицы",
    "разрешение экрана": "Разрешение экрана",
    "время отклика": "Время отклика",
    "яркость": "Яркость",
    "контрастность": "Контрастность",
    "интерфейсы": "Интерфейсы",
    "интерфейс": "Интерфейсы",
    "гарантия": "Гарантия",
    "модель": "Модель",
    "бренд": "Бренд",
    "производитель": "Производитель",
    "совместимость": "Совместимость",
    "коды расходников": "Коды расходников",
}

# Значения-пустышки
_BAD_VALUES = {
    "",
    "-",
    "—",
    "--",
    "---",
    ".",
    "..",
    "...",
    "n/a",
    "na",
    "null",
    "none",
    "нет данных",
    "не указано",
    "not specified",
}


def _norm_space(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _ci(s: str) -> str:
    return _norm_space(s).casefold()


def _strip_html(s: str) -> str:
    s = html.unescape(s or "")
    s = _HTML_TAG_RE.sub(" ", s)
    return _norm_space(s)


def _clean_name(name: str) -> str:
    name = _strip_html(name)
    name = name.replace(" ", " ").replace("\ufeff", " ")
    name = _norm_space(name).strip(" :;|,-")
    if not name:
        return ""

    key = _ci(name)
    if key in _PARAM_NAME_MAP:
        return _PARAM_NAME_MAP[key]

    # Единицы и частые опечатки
    name = re.sub(r"\(\s*bt\s*\)", "(Вт)", name, flags=re.IGNORECASE)
    name = re.sub(r"\(\s*w\s*\)", "(Вт)", name, flags=re.IGNORECASE)
    name = re.sub(r"\bbt\b", "Вт", name, flags=re.IGNORECASE)

    # Аккуратный title-like вид
    if name.isupper() and len(name) > 3:
        name = name.capitalize()

    return name


def _clean_value(value: str) -> str:
    value = html.unescape(value or "")
    value = value.replace("\xa0", " ").replace("\ufeff", " ")
    value = value.replace("\r", "\n")
    value = _HTML_TAG_RE.sub(" ", value)
    value = _MULTI_SEP_RE.sub("; ", value)
    value = re.sub(r"[ \t]*\n[ \t]*", "; ", value)
    value = _RANGE_DASH_RE.sub(" - ", value)
    value = _norm_space(value).strip(" ;|,")
    return value


def _normalize_booleanish(name: str, value: str) -> str:
    v = _ci(value)
    if v in {"true", "yes", "y", "1"}:
        return "Да"
    if v in {"false", "no", "n", "0"}:
        return "Нет"

    # Для warranty часто лучше Да/Нет
    if _ci(name) == "гарантия":
        if "нет" in v:
            return "Нет"
        if any(x in v for x in {"да", "есть", "имеется"}):
            return "Да"

    return value


def _normalize_units(name: str, value: str) -> str:
    # Power / watts
    if _ci(name) == _ci("Мощность (Вт)"):
        value = re.sub(r"\b(bt|w)\b", "Вт", value, flags=re.IGNORECASE)
        value = re.sub(r"\s*вт\b", " Вт", value, flags=re.IGNORECASE)
        value = _norm_space(value)

    # Inches
    if "диагональ" in _ci(name):
        value = re.sub(r"\b(inch|inches|in)\b", '"', value, flags=re.IGNORECASE)
        value = re.sub(r"\s*дюйм(а|ов)?\b", '"', value, flags=re.IGNORECASE)
        value = _norm_space(value)

    return value


def _value_is_bad(name: str, value: str, offer: NormalizedOffer) -> bool:
    if _ci(name) in _BANNED_PARAM_NAMES:
        return True

    if _ci(value) in _BAD_VALUES:
        return True

    if _ci(name) == _ci(value):
        return True

    # Пустые техдубли поля товара
    if _ci(name) == "модель" and _ci(value) == _ci(offer.model):
        return False  # native модель оставляем
    if _ci(name) == "бренд" and offer.vendor and _ci(value) == _ci(offer.vendor):
        return False

    # Откровенно шумные односимвольные значения
    if len(value.strip()) == 1 and value.strip() not in {'"', "0", "1"}:
        return True

    return False


def _dedupe_keep_order(params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for name, value in params:
        key = (_ci(name), _ci(value))
        if key in seen:
            continue
        seen.add(key)
        out.append((name, value))

    return out


def clean_xml_params(offer: NormalizedOffer) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    cleaned: list[tuple[str, str]] = []
    removed = Counter()

    for raw_name, raw_value in offer.xml_params:
        name = _clean_name(raw_name)
        value = _clean_value(raw_value)

        if not name:
            removed["empty_name"] += 1
            continue
        if not value:
            removed["empty_value"] += 1
            continue

        value = _normalize_booleanish(name, value)
        value = _normalize_units(name, value)

        if _value_is_bad(name, value, offer):
            removed["noise_or_banned"] += 1
            continue

        cleaned.append((name, value))

    before = len(cleaned)
    cleaned = _dedupe_keep_order(cleaned)
    removed["duplicates"] += max(0, before - len(cleaned))

    report: dict[str, Any] = {
        "before": len(offer.xml_params),
        "after": len(cleaned),
        "removed_total": len(offer.xml_params) - len(cleaned),
        "removed_breakdown": dict(sorted(removed.items())),
    }
    return cleaned, report


def clean_xml_params_bulk(
    offers: list[NormalizedOffer],
) -> tuple[dict[str, list[tuple[str, str]]], dict[str, Any]]:
    mapping: dict[str, list[tuple[str, str]]] = {}
    removed_total = 0
    total_before = 0
    total_after = 0
    removed = Counter()

    for offer in offers:
        params, rep = clean_xml_params(offer)
        mapping[offer.oid] = params

        total_before += int(rep["before"])
        total_after += int(rep["after"])
        removed_total += int(rep["removed_total"])
        for k, v in (rep.get("removed_breakdown") or {}).items():
            removed[k] += int(v)

    report: dict[str, Any] = {
        "offers": len(offers),
        "params_before": total_before,
        "params_after": total_after,
        "removed_total": removed_total,
        "removed_breakdown": dict(sorted(removed.items())),
    }
    return mapping, report
