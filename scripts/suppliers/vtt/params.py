# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/params.py
VTT supplier layer — нормализация params.

Задача файла:
- почистить сырой список param;
- удалить служебный мусор VTT;
- собрать один канонический Партномер;
- при необходимости добавить Коды расходников;
- аккуратно нормализовать Тип / Цвет / Ресурс.
"""

from __future__ import annotations

from typing import Iterable, Sequence
import re


# Ключи VTT / supplier-layer
_SERVICE_PARAM_NAMES = {
    "аналоги",
    "аналог",
    "штрихкод",
    "штрих-код",
    "штрих код",
    "barcode",
}

_OEM_PARAM_NAMES = {
    "oem-номер",
    "oem номер",
    "oem",
    "oem номер детали",
    "oem номер/part number",
}

_CATALOG_PARAM_NAMES = {
    "каталожный номер",
    "кат. номер",
    "каталожный №",
    "кат. №",
}

_PARTNUMBER_PARAM_NAMES = {
    "партномер",
    "partnumber",
    "part number",
    "part no",
    "pn",
}

_COLOR_PARAM_NAMES = {
    "цвет",
    "color",
    "colour",
}

_RESOURCE_PARAM_NAMES = {
    "ресурс",
    "yield",
}

_TYPE_PARAM_NAMES = {
    "тип",
    "type",
}

_COLOR_MAP = {
    "bk": "Черный",
    "black": "Черный",
    "черный": "Черный",
    "чёрный": "Черный",
    "cyan": "Голубой",
    "blue": "Синий",
    "синий": "Синий",
    "magenta": "Пурпурный",
    "purple": "Пурпурный",
    "пурпурный": "Пурпурный",
    "yellow": "Желтый",
    "yellowe": "Желтый",
    "желтый": "Желтый",
    "жёлтый": "Желтый",
    "grey": "Серый",
    "gray": "Серый",
    "серый": "Серый",
    "mattblack": "Матовый черный",
    "matteblack": "Матовый черный",
    "matte black": "Матовый черный",
    "matt black": "Матовый черный",
    "матовый черный": "Матовый черный",
    "photoblack": "Фото-черный",
    "photo black": "Фото-черный",
    "фото черный": "Фото-черный",
    "фото-черный": "Фото-черный",
    "color": "Цветной",
    "colour": "Цветной",
    "цветной": "Цветной",
}

_TYPE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?i)^\s*тонер-?картридж\b"), "Тонер-картридж"),
    (re.compile(r"(?i)^\s*картридж\b"), "Картридж"),
    (re.compile(r"(?i)^\s*драм-?юнит\b"), "Драм-юнит"),
    (re.compile(r"(?i)^\s*фотобарабан\b"), "Фотобарабан"),
    (re.compile(r"(?i)^\s*барабан\b"), "Барабан"),
    (re.compile(r"(?i)^\s*девелопер\b"), "Девелопер"),
    (re.compile(r"(?i)^\s*тонер\b"), "Тонер"),
    (re.compile(r"(?i)^\s*чернила\b"), "Чернила"),
    (re.compile(r"(?i)^\s*лента\b"), "Лента"),
    (re.compile(r"(?i)^\s*ролик\b"), "Ролик"),
]

_RE_MULTI_SP = re.compile(r"\s+")
_RE_CODEISH = re.compile(r"\b[0-9A-ZА-ЯЁ№][0-9A-ZА-ЯЁ№\-]{3,}\b")
_RE_PN_IN_BRACKETS = re.compile(r"\(([0-9A-ZА-ЯЁ№][0-9A-ZА-ЯЁ№\-]{3,})\)", flags=re.IGNORECASE)
_RE_RESOURCE = re.compile(r"\b\d+(?:[\.,]\d+)?\s*[KК]\b|\b\d+(?:[\.,]\d+)?\s*(?:стр|стр\.|pages?)\b", flags=re.IGNORECASE)


def normalize_vtt_params(params: list[tuple[str, str]], name: str = "") -> list[tuple[str, str]]:
    """Главный вход: чистит сырой список params VTT."""
    cleaned = _clean_param_pairs(params)
    cleaned = _drop_service_params(cleaned)

    partnumber = extract_main_partnumber(cleaned, name=name)

    out = _strip_source_partnumber_params(cleaned)
    out = _normalize_type_param(out, name)
    out = _normalize_color_param(out, name)
    out = _normalize_resource_param(out, name)
    out = _append_partnumber(out, partnumber)

    codes = _extract_consumable_codes(out, name=name, pn=partnumber)
    out = _append_consumable_codes(out, codes)

    return _dedupe_and_sort_vtt_params(out)


def extract_main_partnumber(params: Sequence[tuple[str, str]], name: str = "") -> str:
    """Возвращает один канонический партномер для builder.py."""
    return _pick_main_partnumber(_extract_partnumber_candidates(params, name=name))


def get_param_value(params: Sequence[tuple[str, str]], key: str) -> str:
    """Читает первое значение param по имени."""
    key_cf = _norm_key(key)
    for k, v in params or []:
        if _norm_key(k) == key_cf:
            return norm_ws(v)
    return ""


def norm_ws(s: str) -> str:
    """Нормализует пробелы."""
    return _RE_MULTI_SP.sub(" ", (s or "").replace("\u00a0", " ")).strip()


def _norm_key(s: str) -> str:
    """Нормализует ключ param для сравнений."""
    return norm_ws(s).casefold().replace("ё", "е")


def _clean_param_pairs(params: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    """Чистит пустые и кривые пары."""
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for k0, v0 in params or []:
        k = norm_ws(k0)
        v = norm_ws(v0)
        if not k or not v:
            continue
        key = (_norm_key(k), v.casefold())
        if key in seen:
            continue
        seen.add(key)
        out.append((k, v))
    return out


def _drop_service_params(params: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    """Удаляет служебные VTT params."""
    out: list[tuple[str, str]] = []
    for k, v in params or []:
        k_cf = _norm_key(k)
        if k_cf in _SERVICE_PARAM_NAMES:
            continue
        out.append((k, v))
    return out


def _extract_partnumber_candidates(params: Sequence[tuple[str, str]], name: str = "") -> list[str]:
    """Собирает кандидаты партномера из params и имени."""
    out: list[str] = []

    for k, v in params or []:
        k_cf = _norm_key(k)
        vv = norm_ws(v)
        if not vv:
            continue
        if k_cf in _OEM_PARAM_NAMES or k_cf in _PARTNUMBER_PARAM_NAMES or k_cf in _CATALOG_PARAM_NAMES:
            out.extend(_expand_code_candidates(vv))

    if name:
        for token in _RE_PN_IN_BRACKETS.findall(name or ""):
            out.extend(_expand_code_candidates(token))

        for token in _find_codeish_tokens(name):
            out.extend(_expand_code_candidates(token))

    return _dedupe_keep_order([x for x in out if _looks_like_partnumber(x)])


def _pick_main_partnumber(candidates: list[str]) -> str:
    """Выбирает один канонический партномер."""
    for cand in candidates:
        if _looks_like_partnumber(cand):
            return cand
    return ""


def _strip_source_partnumber_params(params: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    """Удаляет исходные дублирующие ключи партномера."""
    out: list[tuple[str, str]] = []
    for k, v in params or []:
        k_cf = _norm_key(k)
        if k_cf in _OEM_PARAM_NAMES or k_cf in _PARTNUMBER_PARAM_NAMES or k_cf in _CATALOG_PARAM_NAMES:
            continue
        out.append((k, v))
    return out


def _append_partnumber(params: Sequence[tuple[str, str]], pn: str) -> list[tuple[str, str]]:
    """Добавляет канонический Партномер."""
    out = list(params or [])
    if pn:
        out.append(("Партномер", pn))
    return out


def _extract_consumable_codes(params: Sequence[tuple[str, str]], *, name: str = "", pn: str = "") -> list[str]:
    """Ищет коды расходников консервативно, без фантазии."""
    out: list[str] = []

    raw_codes = get_param_value(params, "Коды расходников")
    if raw_codes:
        out.extend(_split_codes(raw_codes))

    if pn and _looks_like_consumable_code(pn):
        out.extend(_expand_code_candidates(pn))

    for token in _find_codeish_tokens(name):
        if _looks_like_consumable_code(token):
            out.extend(_expand_code_candidates(token))

    return _dedupe_keep_order([x for x in out if _looks_like_consumable_code(x)])


def _append_consumable_codes(params: Sequence[tuple[str, str]], codes: Sequence[str]) -> list[tuple[str, str]]:
    """Добавляет Коды расходников."""
    if not codes:
        return list(params or [])

    out: list[tuple[str, str]] = []
    has_codes = False
    for k, v in params or []:
        if _norm_key(k) == _norm_key("Коды расходников"):
            has_codes = True
            out.append(("Коды расходников", ", ".join(codes)))
            continue
        out.append((k, v))

    if not has_codes:
        out.append(("Коды расходников", ", ".join(codes)))
    return out


def _normalize_type_param(params: Sequence[tuple[str, str]], name: str) -> list[tuple[str, str]]:
    """Нормализует Тип из params или имени."""
    out: list[tuple[str, str]] = []
    has_type = False

    for k, v in params or []:
        if _norm_key(k) in _TYPE_PARAM_NAMES:
            vv = _normalize_type_value(v or name)
            if vv:
                out.append(("Тип", vv))
                has_type = True
            continue
        out.append((k, v))

    if not has_type:
        vv = _normalize_type_value(name)
        if vv:
            out.append(("Тип", vv))
    return out


def _normalize_color_param(params: Sequence[tuple[str, str]], name: str) -> list[tuple[str, str]]:
    """Нормализует Цвет из params или имени."""
    out: list[tuple[str, str]] = []
    color_value = ""

    for k, v in params or []:
        if _norm_key(k) in _COLOR_PARAM_NAMES:
            if not color_value:
                color_value = _normalize_color_value(v)
            continue
        out.append((k, v))

    if not color_value:
        color_value = _extract_color_from_text(name)

    if color_value:
        out.append(("Цвет", color_value))
    return out


def _normalize_resource_param(params: Sequence[tuple[str, str]], name: str) -> list[tuple[str, str]]:
    """Нормализует Ресурс из params или имени."""
    out: list[tuple[str, str]] = []
    resource_value = ""

    for k, v in params or []:
        if _norm_key(k) in _RESOURCE_PARAM_NAMES:
            if not resource_value:
                resource_value = _normalize_resource_value(v)
            continue
        out.append((k, v))

    if not resource_value:
        resource_value = _extract_resource_from_text(name)

    if resource_value:
        out.append(("Ресурс", resource_value))
    return out


def _normalize_type_value(text: str) -> str:
    """Канонизирует значение Тип."""
    s = norm_ws(text)
    if not s:
        return ""
    for rx, canon in _TYPE_PATTERNS:
        if rx.search(s):
            return canon
    return ""


def _normalize_color_value(text: str) -> str:
    """Канонизирует значение Цвет."""
    s = norm_ws(text).strip(" ,.;:")
    if not s:
        return ""
    s_cf = s.casefold().replace("ё", "е")
    if s_cf in _COLOR_MAP:
        return _COLOR_MAP[s_cf]

    for raw, canon in _COLOR_MAP.items():
        if re.search(rf"(?<![0-9A-Za-zА-Яа-яЁё]){re.escape(raw)}(?![0-9A-Za-zА-Яа-яЁё])", s_cf, flags=re.IGNORECASE):
            return canon
    return s[:1].upper() + s[1:] if s else ""


def _extract_color_from_text(text: str) -> str:
    """Ищет цвет в имени товара."""
    s_cf = norm_ws(text).casefold().replace("ё", "е")
    if not s_cf:
        return ""

    ordered_keys = sorted(_COLOR_MAP.keys(), key=len, reverse=True)
    for raw in ordered_keys:
        if re.search(rf"(?<![0-9A-Za-zА-Яа-яЁё]){re.escape(raw)}(?![0-9A-Za-zА-Яа-яЁё])", s_cf, flags=re.IGNORECASE):
            return _COLOR_MAP[raw]
    return ""


def _normalize_resource_value(text: str) -> str:
    """Канонизирует значение Ресурс."""
    s = norm_ws(text)
    if not s:
        return ""
    m = _RE_RESOURCE.search(s)
    if not m:
        return ""
    val = norm_ws(m.group(0)).replace(" к", "K").replace(" К", "K")
    val = re.sub(r"\s+", "", val)
    val = val.replace("стр.", "стр").replace("pages", "стр")
    return val


def _extract_resource_from_text(text: str) -> str:
    """Ищет ресурс в имени товара."""
    return _normalize_resource_value(text)


def _dedupe_and_sort_vtt_params(params: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    """Финальная лёгкая дедупликация без жёсткой сортировки."""
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for k, v in params or []:
        kk = norm_ws(k)
        vv = norm_ws(v)
        if not kk or not vv:
            continue
        key = (_norm_key(kk), vv.casefold())
        if key in seen:
            continue
        seen.add(key)
        out.append((kk, vv))
    return out


def _find_codeish_tokens(text: str) -> list[str]:
    """Находит кодоподобные токены в тексте."""
    out: list[str] = []
    for token in _RE_CODEISH.findall((text or "").upper()):
        t = norm_ws(token).strip(".,;:()[]{}")
        if not t:
            continue
        out.append(t)
    return _dedupe_keep_order(out)


def _expand_code_candidates(text: str) -> list[str]:
    """Разбивает строку на отдельные кодоподобные токены."""
    parts = _split_codes(text)
    if not parts:
        parts = [norm_ws(text)]
    return _dedupe_keep_order([x for x in parts if x])


def _split_codes(text: str) -> list[str]:
    """Бережно разбивает строку кодов по разделителям."""
    raw = norm_ws(text)
    if not raw:
        return []
    items = re.split(r"\s*[,;/|]+\s*", raw)
    out: list[str] = []
    for item in items:
        token = norm_ws(item).strip(".,;:()[]{}")
        if not token:
            continue
        out.append(token)
    return _dedupe_keep_order(out)


def _looks_like_partnumber(token: str) -> bool:
    """Проверяет, похож ли токен на партномер."""
    t = norm_ws(token).upper()
    if len(t) < 4:
        return False
    if not re.search(r"[A-ZА-ЯЁ]", t):
        return False
    if not re.search(r"\d", t):
        return False
    if "/" in t:
        return False
    return True


def _looks_like_consumable_code(token: str) -> bool:
    """Проверяет, похож ли токен на код расходки."""
    t = norm_ws(token).upper()
    if not _looks_like_partnumber(t):
        return False
    if re.fullmatch(r"T\d{3,5}", t):
        return False
    if re.fullmatch(r"M\d{3,5}", t):
        return False
    return True


def _dedupe_keep_order(items: Iterable[str]) -> list[str]:
    """Дедуп со стабильным порядком."""
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        key = norm_ws(item).casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(norm_ws(item))
    return out
