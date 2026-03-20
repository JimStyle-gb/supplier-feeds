# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/compat.py

VTT compat layer.

Задача:
- аккуратно дособрать/дочистить Совместимость;
- нормализовать Коды расходников;
- не фантазировать совместимость, а брать только то,
  что явно читается из title/native_desc/params;
- вернуть supplier-clean params до RAW.
"""

from __future__ import annotations

import re
from typing import Iterable, Sequence

from .normalize import norm_spaces


_VENDOR_LIST = (
    "HP",
    "Canon",
    "Xerox",
    "Kyocera",
    "Brother",
    "Epson",
    "Pantum",
    "Ricoh",
    "Lexmark",
    "Samsung",
    "OKI",
    "RISO",
    "Panasonic",
    "Toshiba",
    "Sharp",
    "Konica Minolta",
    "Develop",
)

_COMPAT_KEYS = (
    "Совместимость",
    "Модель",
)

_CODE_RX = re.compile(
    r"\b(?:"
    r"[A-Z]{1,4}-?[A-Z0-9]{2,}(?:/[A-Z0-9-]{2,})+|"   # HB-Q5949A/Q7553A
    r"[A-Z]{1,4}\d{2,}[A-Z0-9-]{0,6}|"                # Q7553A / CE285A / TK1140 / B3P19A
    r"(?:006R|106R)\d{5}|"                            # Xerox
    r"W\d{4}[A-Z0-9]{0,4}"                            # W1106A / W1335A
    r")\b",
    re.I,
)

_DEVICE_WORDS = (
    "LaserJet",
    "DeskJet",
    "OfficeJet",
    "DesignJet",
    "WorkCentre",
    "Phaser",
    "EcoSys",
    "Aficio",
    "i-SENSYS",
    "imageRUNNER",
    "imageCLASS",
    "MFP",
    "SP",
    "BIZHUB",
)

_TRIM_TAIL_RX = re.compile(
    r"(?:"
    r"\bкупить\b.*|"
    r"\bцена\b.*|"
    r"\bв наличии\b.*|"
    r"\bпод заказ\b.*|"
    r"\bдоставка\b.*|"
    r"\bзаказать\b.*|"
    r"\bресурс\b.*|"
    r"\bцвет\b.*|"
    r"\bпартномер\b.*|"
    r"\bкод(?:ы)?\s+расходников\b.*"
    r")$",
    re.I,
)


def _cf(text: str) -> str:
    return norm_spaces(text).casefold().replace("ё", "е")


def _dedupe_pairs(params: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for k, v in params:
        kk = norm_spaces(k)
        vv = norm_spaces(v)
        if not kk or not vv:
            continue
        sig = (_cf(kk), _cf(vv))
        if sig in seen:
            continue
        seen.add(sig)
        out.append((kk, vv))
    return out


def _get_first(params: Sequence[tuple[str, str]], key: str) -> str:
    key_cf = _cf(key)
    for k, v in params:
        if _cf(k) == key_cf and norm_spaces(v):
            return norm_spaces(v)
    return ""


def _drop_keys(params: Sequence[tuple[str, str]], keys: Sequence[str]) -> list[tuple[str, str]]:
    drop_cf = {_cf(x) for x in keys}
    out: list[tuple[str, str]] = []
    for k, v in params:
        if _cf(k) in drop_cf:
            continue
        out.append((k, v))
    return out


def _append_once(params: list[tuple[str, str]], key: str, value: str) -> list[tuple[str, str]]:
    val = norm_spaces(value)
    if not val:
        return params
    key_cf = _cf(key)
    for i, (k, v) in enumerate(params):
        if _cf(k) == key_cf:
            # Если значение уже есть — оставляем, иначе заменяем на более чистое.
            if _cf(v) == _cf(val):
                return params
            params[i] = (key, val)
            return params
    params.append((key, val))
    return params


def _normalize_piece(piece: str) -> str:
    s = norm_spaces(piece)
    if not s:
        return ""
    s = s.replace(" ,", ",").replace(" .", ".")
    s = re.sub(r"\s*;\s*", "; ", s)
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s*/\s*", "/", s)
    s = re.sub(r"\s*-\s*", "-", s)
    s = re.sub(r"\s{2,}", " ", s)
    s = s.strip(" ,;:-/")
    return s


def _clean_compat_tail(text: str) -> str:
    s = _normalize_piece(text)
    if not s:
        return ""
    s = _TRIM_TAIL_RX.sub("", s).strip(" ,;:-/")
    return s


def _split_compat_chunks(value: str) -> list[str]:
    src = _clean_compat_tail(value)
    if not src:
        return []
    # Сначала грубый split по ; и переносам.
    raw_parts = re.split(r"[;\n\r]+", src)
    out: list[str] = []
    for raw in raw_parts:
        part = _normalize_piece(raw)
        if not part:
            continue
        out.append(part)
    return out


def _looks_like_compat_value(value: str) -> bool:
    s = _cf(value)
    if not s:
        return False
    if any(_cf(vendor) in s for vendor in _VENDOR_LIST):
        return True
    if any(word.casefold() in s.casefold() for word in _DEVICE_WORDS):
        return True
    if "/" in s and re.search(r"[a-zа-я0-9]", s, flags=re.I):
        return True
    return False


def _extract_vendor_model_phrases(text: str) -> list[str]:
    src = norm_spaces(text)
    if not src:
        return []

    out: list[str] = []
    seen: set[str] = set()

    # 1) "для HP LJ Pro M304/404n/404dn"
    rx_for = re.compile(
        r"(?:^|\b)(?:для|for)\s+"
        r"(HP|Canon|Xerox|Kyocera|Brother|Epson|Pantum|Ricoh|Lexmark|Samsung|OKI|RISO|Panasonic|Toshiba|Sharp|Konica Minolta|Develop)\b"
        r"([A-Za-z0-9][A-Za-z0-9\s./,_+\-]{2,140})",
        re.I,
    )

    # 2) "HP LJ Pro M304/404n/404dn" прямо в title
    rx_plain = re.compile(
        r"\b(HP|Canon|Xerox|Kyocera|Brother|Epson|Pantum|Ricoh|Lexmark|Samsung|OKI|RISO|Panasonic|Toshiba|Sharp|Konica Minolta|Develop)\b"
        r"([A-Za-z0-9][A-Za-z0-9\s./,_+\-]{2,140})",
        re.I,
    )

    for rx in (rx_for, rx_plain):
        for m in rx.finditer(src):
            vendor = _normalize_piece(m.group(1))
            tail = _clean_compat_tail(m.group(2))
            if not vendor or not tail:
                continue
            # Отрубаем явно товарные хвосты.
            tail = re.split(r"\b(?:картридж|тонер|чернила|фотобарабан|драм|девелопер|термоблок|термолента)\b", tail, flags=re.I)[0]
            tail = _clean_compat_tail(tail)
            if len(tail) < 3:
                continue
            if not re.search(r"[A-Za-zА-Яа-я0-9]", tail):
                continue
            value = f"{vendor} {tail}".strip()
            value = _normalize_piece(value)
            if not _looks_like_compat_value(value):
                continue
            sig = _cf(value)
            if sig in seen:
                continue
            seen.add(sig)
            out.append(value)

    return out


def _normalize_compat_value(value: str) -> str:
    parts = _split_compat_chunks(value)
    if not parts:
        return ""

    out: list[str] = []
    seen: set[str] = set()

    for part in parts:
        if not _looks_like_compat_value(part):
            continue
        sig = _cf(part)
        if sig in seen:
            continue
        seen.add(sig)
        out.append(part)

    # Если нашли несколько brand-model кусков — соединяем через "; "
    return "; ".join(out).strip()


def _extract_existing_compat(params: Sequence[tuple[str, str]]) -> str:
    parts: list[str] = []
    for key in _COMPAT_KEYS:
        val = _get_first(params, key)
        if val:
            parts.append(val)
    return _normalize_compat_value("; ".join(parts))


def _extract_codes_from_texts(*texts: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for text in texts:
        src = norm_spaces(text)
        if not src:
            continue
        for m in _CODE_RX.finditer(src):
            code = _normalize_piece(m.group(0)).upper()
            if not code:
                continue
            sig = code.casefold()
            if sig in seen:
                continue
            seen.add(sig)
            out.append(code)

    return out


def _merge_codes(existing: str, title: str, native_desc: str, partnumber: str) -> str:
    codes: list[str] = []
    seen: set[str] = set()

    for code in _extract_codes_from_texts(existing, title, native_desc, partnumber):
        sig = code.casefold()
        if sig in seen:
            continue
        seen.add(sig)
        codes.append(code)

    return ", ".join(codes[:12]).strip()


def reconcile_vtt_compat(
    params: Sequence[tuple[str, str]],
    *,
    title: str = "",
    native_desc: str = "",
) -> list[tuple[str, str]]:
    """
    Главная функция compat-layer.
    Возвращает supplier-clean params до RAW.
    """
    base = _dedupe_pairs(params)

    partnumber = _get_first(base, "Партномер")
    existing_compat = _extract_existing_compat(base)

    # 1) Совместимость: сначала существующая, потом source-derived.
    compat_candidates: list[str] = []
    if existing_compat:
        compat_candidates.append(existing_compat)

    compat_candidates.extend(_extract_vendor_model_phrases(title))
    compat_candidates.extend(_extract_vendor_model_phrases(native_desc))

    compat_value = _normalize_compat_value("; ".join([x for x in compat_candidates if x]))

    # 2) Коды расходников: добираем и нормализуем.
    existing_codes = _get_first(base, "Коды расходников")
    codes_value = _merge_codes(existing_codes, title, native_desc, partnumber)

    # 3) Убираем сырые compat keys и возвращаем канон.
    out = _drop_keys(base, ("Совместимость",))

    if compat_value:
        out = _append_once(out, "Совместимость", compat_value)

    if codes_value:
        out = _append_once(out, "Коды расходников", codes_value)

    return _dedupe_pairs(out)
