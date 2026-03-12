# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/compat.py
AkCent supplier layer — cleanup совместимости / кодов / device-target для узкого потока.

v1:
- чистит только supplier-side поля расходки;
- не тащит supplier-specific логику в core;
- не угадывает compat/codes для нерасходки;
- умеет аккуратно дочищать Коды / Совместимость / Для устройства;
- умеет очень консервативно добирать код из name/model только для consumable.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence, Tuple

# -----------------------------
# Базовые regex
# -----------------------------

_WS_RE = re.compile(r"\s+")
_TAG_RE = re.compile(r"<[^>]+>")
_SPLIT_RE = re.compile(r"\s*(?:[,;|]|/\s(?=[A-Za-zА-Яа-я0-9]))\s*")
_BRACKET_RE = re.compile(r"[\[\]{}()]")

# Коды, которые реально встречаются в текущем потоке AkCent.
# Держим regex умеренно широким, но не агрессивным.
_CODE_RE = re.compile(
    r"\b(?:"
    r"C13T\d{4,6}[A-Z]?"
    r"|T\d{2,4}[A-Z]{0,2}\d{0,2}"
    r"|CF\d{2,4}[A-Z]?"
    r"|CE\d{2,4}[A-Z]?"
    r"|CB\d{2,4}[A-Z]?"
    r"|CC\d{2,4}[A-Z]?"
    r"|CLT-[A-Z0-9]{3,8}"
    r"|TN-?[A-Z0-9]{2,8}"
    r"|DR-?[A-Z0-9]{2,8}"
    r"|MLT-[A-Z0-9]{3,8}"
    r"|W\d{4}[A-Z]?"
    r"|B\d{3,5}[A-Z]?"
    r")\b",
    re.IGNORECASE,
)

# Для расходки по принтерам допускаем более мягкое выделение Epson-серий из начала name.
_EPSON_START_RE = re.compile(r"^(C13T\d{4,6}[A-Z]?)\b", re.IGNORECASE)

# Маркеры мусора в compat/device.
_NOISE_PARTS = (
    "оригинальный",
    "original",
    "совместимый",
    "compatible",
    "картридж",
    "чернила",
    "экономичный набор",
    "емкость для отработанных чернил",
    "ёмкость для отработанных чернил",
    "для принтера",
    "для мфу",
    "для устройства",
    "поддерживаемые модели",
    "поддерживаемые продукты",
    "supported models",
    "supported products",
)

_CONSUMABLE_PREFIXES = (
    "C13T55",
    "Ёмкость для отработанных чернил",
    "Емкость для отработанных чернил",
    "Картридж",
    "Чернила",
    "Экономичный набор",
)


# -----------------------------
# Низкоуровневые helper-ы
# -----------------------------


def _norm_spaces(text: str) -> str:
    return _WS_RE.sub(" ", (text or "").strip())



def _plain(text: str) -> str:
    text = _TAG_RE.sub(" ", text or "")
    text = _BRACKET_RE.sub(" ", text)
    return _norm_spaces(text)



def _cf(text: str) -> str:
    return _plain(text).casefold().replace("ё", "е")



def _dedupe_keep_order(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        x = _norm_spaces(item)
        if not x:
            continue
        key = x.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out



def _split_tokens(value: str) -> List[str]:
    raw = _plain(value)
    if not raw:
        return []
    parts = _SPLIT_RE.split(raw)
    out: List[str] = []
    for part in parts:
        x = _norm_spaces(part.strip(" .:-"))
        if not x:
            continue
        out.append(x)
    return _dedupe_keep_order(out)



def _is_consumable_name(name: str) -> bool:
    src = _norm_spaces(name)
    return any(src.startswith(p) for p in _CONSUMABLE_PREFIXES)



def _looks_like_device_phrase(text: str) -> bool:
    s = _cf(text)
    if not s:
        return False
    return any(mark in s for mark in (
        "epson", "hp", "canon", "brother", "xerox", "pantum", "samsung",
        "ecotank", "workforce", "expression", "surecolor", "laserjet",
        "pixma", "imageclass", "dcp", "mfc", "l3150", "l3250", "l8050",
    ))



def _clean_noise_prefixes(text: str) -> str:
    x = _norm_spaces(text)
    if not x:
        return ""
    y = x
    for marker in _NOISE_PARTS:
        y = re.sub(re.escape(marker), " ", y, flags=re.IGNORECASE)
    y = re.sub(r"^(?:для|подходит для|совместим с|совместимость|compatibility)\s+", "", y, flags=re.IGNORECASE)
    y = re.sub(r"\b(?:модель|модели|серия|series)\b\s*:?", " ", y, flags=re.IGNORECASE)
    y = _norm_spaces(y.strip(" ,;:-"))
    return y



def _titleish_device(text: str) -> str:
    x = _norm_spaces(text)
    if not x:
        return ""
    words = []
    for token in x.split():
        if re.fullmatch(r"[A-Z0-9-]{2,}", token):
            words.append(token)
        elif re.search(r"\d", token):
            # модели с цифрами не трогаем
            words.append(token)
        else:
            words.append(token[:1].upper() + token[1:])
    return _norm_spaces(" ".join(words))


# -----------------------------
# Коды
# -----------------------------


def extract_codes_from_text(*texts: Optional[str]) -> List[str]:
    found: List[str] = []
    for text in texts:
        if not text:
            continue
        for m in _CODE_RE.finditer(text):
            found.append(m.group(0).upper())
    return _dedupe_keep_order(found)



def extract_primary_code_from_name(name: str) -> str:
    src = _norm_spaces(name)
    if not src:
        return ""
    m = _EPSON_START_RE.search(src)
    if m:
        return m.group(1).upper()
    codes = extract_codes_from_text(src)
    return codes[0] if codes else ""



def clean_codes_value(value: str) -> str:
    codes = extract_codes_from_text(value)
    return ", ".join(codes)


# -----------------------------
# Совместимость / Для устройства
# -----------------------------


def _split_compat_chunks(value: str) -> List[str]:
    text = _clean_noise_prefixes(value)
    if not text:
        return []

    # Сначала грубо режем списки
    rough = _split_tokens(text)
    out: List[str] = []
    for item in rough:
        item = re.sub(r"\b(?:и|and)\b", " ", item, flags=re.IGNORECASE)
        item = _norm_spaces(item.strip(" ,;:-"))
        if not item:
            continue
        # Оставляем только то, что похоже на устройство/серию
        if _looks_like_device_phrase(item) or re.search(r"[A-Za-zА-Яа-я]+\d{2,}", item):
            out.append(_titleish_device(item))
    return _dedupe_keep_order(out)



def clean_compat_value(value: str) -> str:
    items = _split_compat_chunks(value)
    return ", ".join(items)



def clean_device_value(value: str) -> str:
    items = _split_compat_chunks(value)
    if not items:
        x = _titleish_device(_clean_noise_prefixes(value))
        return x
    return ", ".join(items)


# -----------------------------
# Сборка supplier-side cleanup
# -----------------------------


def _get_param(params: Sequence[Tuple[str, str]], key: str) -> str:
    key_cf = key.casefold()
    for k, v in params:
        if (k or "").casefold() == key_cf and (v or "").strip():
            return _norm_spaces(v)
    return ""



def _set_param(params: Sequence[Tuple[str, str]], key: str, value: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    inserted = False
    key_cf = key.casefold()
    clean_value = _norm_spaces(value)
    for k, v in params:
        if (k or "").casefold() == key_cf:
            if clean_value and not inserted:
                out.append((key, clean_value))
                inserted = True
            continue
        out.append((k, v))
    if clean_value and not inserted:
        out.append((key, clean_value))
    return out



def _drop_param(params: Sequence[Tuple[str, str]], key: str) -> List[Tuple[str, str]]:
    key_cf = key.casefold()
    return [(k, v) for k, v in params if (k or "").casefold() != key_cf]



def reconcile_consumable_params(
    params: Sequence[Tuple[str, str]],
    *,
    name: str = "",
    model: str = "",
    kind: str = "",
) -> List[Tuple[str, str]]:
    """Чистит params только для consumable-группы.

    Ничего не делает для других kind — это важно для безопасности.
    """
    if kind and kind != "consumable":
        return list(params)
    if not kind and not _is_consumable_name(name):
        return list(params)

    out = list(params)

    # 1) Коды
    raw_codes = _get_param(out, "Коды")
    clean_codes = clean_codes_value(raw_codes)
    if not clean_codes:
        clean_codes = extract_primary_code_from_name(name)
    if not clean_codes:
        clean_codes = clean_codes_value(model)
    if clean_codes:
        out = _set_param(out, "Коды", clean_codes)

    # 2) Совместимость
    raw_compat = _get_param(out, "Совместимость")
    clean_compat = clean_compat_value(raw_compat)
    if clean_compat:
        out = _set_param(out, "Совместимость", clean_compat)
    elif raw_compat:
        out = _drop_param(out, "Совместимость")

    # 3) Для устройства
    raw_device = _get_param(out, "Для устройства")
    clean_device = clean_device_value(raw_device)
    if clean_device:
        out = _set_param(out, "Для устройства", clean_device)
    elif raw_device:
        out = _drop_param(out, "Для устройства")

    # 4) Если есть Совместимость, но нет Для устройства — аккуратно дублируем укороченно
    final_device = _get_param(out, "Для устройства")
    final_compat = _get_param(out, "Совместимость")
    if final_compat and not final_device:
        # Держим device короче — максимум первые 3 модели
        parts = _split_compat_chunks(final_compat)
        if parts:
            out = _set_param(out, "Для устройства", ", ".join(parts[:3]))

    # 5) Пустые/шумные значения вычищаем
    for key in ("Коды", "Совместимость", "Для устройства"):
        value = _get_param(out, key)
        if not value or value in {"-", "—", "..."}:
            out = _drop_param(out, key)

    return out



def reconcile_params(
    params: Sequence[Tuple[str, str]],
    *,
    name: str = "",
    model: str = "",
    kind: str = "",
) -> List[Tuple[str, str]]:
    """Общая точка входа для builder.py."""
    return reconcile_consumable_params(params, name=name, model=model, kind=kind)


__all__ = [
    "extract_codes_from_text",
    "extract_primary_code_from_name",
    "clean_codes_value",
    "clean_compat_value",
    "clean_device_value",
    "reconcile_consumable_params",
    "reconcile_params",
]
