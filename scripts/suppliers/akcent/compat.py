# -*- coding: utf-8 -*-
"""
AkCent compat/model cleanup layer.

Что делает:
- чистит только уже существующие supplier данные
- нормализует Модель / Совместимость / Коды расходников
- не генерирует auto compat / auto codes из текста
- не тащит supplier-specific эвристики в core
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any


_WS_RE = re.compile(r"\s+")
_MULTI_SEP_RE = re.compile(r"\s*(?:;|,|\||/|\n|\r|\t)+\s*")
_RANGE_DASH_RE = re.compile(r"\s*[-–—]+\s*")
_COMPAT_LABEL_RE = re.compile(
    r"(?i)\b(?:совместимые?\s+модели|поддерживаемые?\s+модели(?:\s+принтеров)?|поддерживаемые?\s+продукты|совместимость)\b\s*:?"
)
_CODES_LABEL_RE = re.compile(
    r"(?i)\b(?:коды?\s+расходников|код(?:ы)?\s+картриджа|расходный\s+материал|расходные\s+материалы)\b\s*:?"
)
_MODEL_LABEL_RE = re.compile(r"(?i)\b(?:модель|model)\b\s*:?\s*")
_NARRATIVE_TAIL_RE = re.compile(
    r"(?i)\b(?:цвет|ресурс|гарантия|комплектация|примечание|важно|наличие\s+чипа|chip|с\s+чипом|без\s+чипа)\b\s*[:\-].*$"
)

_COMPAT_KEYS = {
    "совместимость",
    "поддерживаемые модели",
    "поддерживаемые модели принтеров",
    "поддерживаемые продукты",
    "совместимые модели",
}
_CODES_KEYS = {
    "коды расходников",
    "код картриджа",
    "коды картриджа",
    "код расходника",
    "коды расходников/картриджей",
}
_MODEL_KEYS = {
    "модель",
    "model",
}

_CODE_TOKEN_RE = re.compile(
    r"\b(?:C13T\d{4,6}[A-Z]?|W\d{4}[A-Z]?|CF\d{3,4}[A-Z]?|CE\d{3,4}[A-Z]?|CB\d{3,4}[A-Z]?|Q\d{4}[A-Z]?|"
    r"TN[- ]?\d+[A-Z]*|DR[- ]?\d+[A-Z]*|MLT[- ]?[A-Z]?\d+[A-Z]*|106R\d{5}|006R\d{5}|"
    r"[A-Z]{1,4}-\d{2,6}[A-Z]*|[A-Z]{1,3}\d{2,6}[A-Z]{0,3})\b",
    re.IGNORECASE,
)


def _norm_space(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _ci(s: str) -> str:
    return _norm_space(s).casefold()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for v in values:
        vv = _norm_space(v)
        if not vv:
            continue
        key = vv.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(vv)

    return out


def _smart_split(value: str) -> list[str]:
    raw = _norm_space(value)
    if not raw:
        return []

    # Если есть явные разделители — делим.
    if any(x in raw for x in [";", "|", "\n", "\r", "\t"]):
        return [x.strip(" ;,|") for x in _MULTI_SEP_RE.split(raw) if x.strip(" ;,|")]

    # Запятые для compat обычно полезно делить, если строка длинная.
    if raw.count(",") >= 2:
        return [x.strip(" ;,|") for x in re.split(r"\s*,\s*", raw) if x.strip(" ;,|")]

    return [raw]


def _clean_model_text(value: str) -> str:
    value = _norm_space(value)
    value = _MODEL_LABEL_RE.sub("", value)
    value = _RANGE_DASH_RE.sub(" - ", value)
    value = value.strip(" ;,|")
    return _norm_space(value)


def _clean_compat_item(item: str) -> str:
    item = _norm_space(item)
    if not item:
        return ""

    item = _COMPAT_LABEL_RE.sub("", item)
    item = _MODEL_LABEL_RE.sub("", item)
    item = _NARRATIVE_TAIL_RE.sub("", item)
    item = _RANGE_DASH_RE.sub(" - ", item)
    item = item.strip(" ;,|")
    item = _norm_space(item)

    # мусорные заголовки без значения
    low = _ci(item)
    if low in {"совместимость", "совместимые модели", "поддерживаемые модели", "модель"}:
        return ""

    return item


def _clean_code_token(token: str) -> str:
    token = _norm_space(token)
    token = _CODES_LABEL_RE.sub("", token)
    token = token.strip(" ;,|")
    token = token.replace(" ", "")
    token = token.upper()
    return token


def _cleanup_compat_value(value: str) -> str:
    parts = [_clean_compat_item(x) for x in _smart_split(value)]
    parts = [x for x in parts if x]
    parts = _dedupe_keep_order(parts)
    return "; ".join(parts)


def _cleanup_codes_value(value: str) -> str:
    raw = _norm_space(value)
    if not raw:
        return ""

    # Если видим явные кодовые токены — берём их в приоритете
    tokens = [_clean_code_token(x.group(0)) for x in _CODE_TOKEN_RE.finditer(raw)]
    tokens = [x for x in tokens if x]
    tokens = _dedupe_keep_order(tokens)
    if tokens:
        return "; ".join(tokens)

    # Иначе аккуратная fallback-чистка существующего текста
    parts = []
    for x in _smart_split(raw):
        xx = _clean_code_token(x)
        if xx:
            parts.append(xx)

    parts = _dedupe_keep_order(parts)
    return "; ".join(parts)


def _merge_param_values(values: list[str], key_name: str) -> str:
    if not values:
        return ""

    if _ci(key_name) == "коды расходников":
        merged_parts: list[str] = []
        for v in values:
            merged_parts.extend([x.strip() for x in v.split(";") if x.strip()])
        merged_parts = _dedupe_keep_order(merged_parts)
        return "; ".join(merged_parts)

    if _ci(key_name) == "совместимость":
        merged_parts = []
        for v in values:
            merged_parts.extend([x.strip() for x in v.split(";") if x.strip()])
        merged_parts = _dedupe_keep_order(merged_parts)
        return "; ".join(merged_parts)

    # Модель и прочее — берём первое непустое
    for v in values:
        vv = _norm_space(v)
        if vv:
            return vv
    return ""


def reconcile_compat_related_params(
    params: list[tuple[str, str]],
) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    """
    Нормализует уже существующие params:
    - Модель
    - Совместимость
    - Коды расходников

    Ничего не создаёт из воздуха.
    """
    grouped: dict[str, list[str]] = {}
    passthrough: list[tuple[str, str]] = []
    counters = Counter()

    for raw_name, raw_value in params:
        name = _norm_space(raw_name)
        value = _norm_space(raw_value)

        if not name or not value:
            counters["empty_skip"] += 1
            continue

        key = _ci(name)

        if key in _MODEL_KEYS:
            cleaned = _clean_model_text(value)
            if cleaned:
                grouped.setdefault("Модель", []).append(cleaned)
                counters["model_seen"] += 1
            else:
                counters["model_drop"] += 1
            continue

        if key in _COMPAT_KEYS:
            cleaned = _cleanup_compat_value(value)
            if cleaned:
                grouped.setdefault("Совместимость", []).append(cleaned)
                counters["compat_seen"] += 1
            else:
                counters["compat_drop"] += 1
            continue

        if key in _CODES_KEYS:
            cleaned = _cleanup_codes_value(value)
            if cleaned:
                grouped.setdefault("Коды расходников", []).append(cleaned)
                counters["codes_seen"] += 1
            else:
                counters["codes_drop"] += 1
            continue

        passthrough.append((name, value))

    normalized_special: list[tuple[str, str]] = []
    for canon_name in ("Модель", "Совместимость", "Коды расходников"):
        merged = _merge_param_values(grouped.get(canon_name, []), canon_name)
        if merged:
            normalized_special.append((canon_name, merged))

    # Итог: сохраняем порядок обычных params, а спец-поля добавляем один раз в конец,
    # чтобы не плодить дубли и не таскать мусорные варианты ключей.
    result = passthrough + normalized_special

    report: dict[str, Any] = {
        "before": len(params),
        "after": len(result),
        "special_counts": {
            "Модель": len(grouped.get("Модель", [])),
            "Совместимость": len(grouped.get("Совместимость", [])),
            "Коды расходников": len(grouped.get("Коды расходников", [])),
        },
        "counters": dict(sorted(counters.items())),
    }
    return result, report
