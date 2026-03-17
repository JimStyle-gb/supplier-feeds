# -*- coding: utf-8 -*-
"""
NVPrint normalize layer — step2 safe split.

Пока без смены поведения: переносим из текущего монолита
очистку name/vendor и нормализацию param keys/values.
"""

from __future__ import annotations

import re

from cs.core import norm_ws

_CYR2LAT = {
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X", "У": "Y",
    "а": "a", "в": "b", "е": "e", "к": "k", "м": "m", "н": "h", "о": "o", "р": "p", "с": "c", "т": "t", "х": "x", "у": "y",
}

_RE_TOKEN = re.compile(r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-._/]+")
_RE_DBL_SLASH = re.compile(r"//+")
_RE_NV_SPACE = re.compile(r"\bNV-\s+")
_RE_WS = re.compile(r"\s+")
_RE_SPACE_BEFORE_RP = re.compile(r"\s+\)")
_RE_SLASH_BEFORE_LETTER = re.compile(r"/(?!\s)(?=[A-Za-zА-Яа-я])")
_RE_SHT_MISSING_SPACE = re.compile(r"\((\d+)шт\)", re.I)
_RE_NUM_SHT_WORD = re.compile(r"\b(\d+)шт\b", re.I)
_RE_WORKCENTRE = re.compile(r"\bWorkcentr(e)?\b", re.I)

_STOP_BRAND_CF = {
    "лазерных", "струйных", "принтеров", "мфу", "копиров", "копировальных", "плоттеров",
    "принтера", "устройств", "устройства", "печати", "всех",
}

_COLOR_MAP = {
    "пурпурный": "Magenta",
    "магента": "Magenta",
    "черный": "Black",
    "чёрный": "Black",
    "желтый": "Yellow",
    "жёлтый": "Yellow",
    "голубой": "Cyan",
    "циан": "Cyan",
    "цветной": "Color",
    "color": "Color",
    "black": "Black",
    "cyan": "Cyan",
    "magenta": "Magenta",
    "yellow": "Yellow",
    "red": "Red",
}

_PARAM_KEY_MAP_NVPRINT = {
    "ТипПечати": "Тип печати",
    "ЦветПечати": "Цвет печати",
    "СовместимостьСМоделями": "Совместимость с моделями",
}

_RE_BRAND_AFTER_DLYA = re.compile(r"\bдля\s+([A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-._]{1,40})", re.I)
_RE_BRAND_AFTER_FOR = re.compile(r"\bfor\s+([A-Za-z0-9][A-Za-z0-9\-._]{1,40})", re.I)


def fix_confusables_to_latin_in_latin_tokens(s: str) -> str:
    """Кириллица -> латиница только внутри латинских/цифровых токенов."""
    if not s:
        return ""
    out = []
    last = 0
    for m in _RE_TOKEN.finditer(s):
        out.append(s[last:m.start()])
        tok = m.group(0)
        has_lat = any(("A" <= ch <= "Z") or ("a" <= ch <= "z") for ch in tok)
        if has_lat:
            tok = "".join(_CYR2LAT.get(ch, ch) for ch in tok)
        out.append(tok)
        last = m.end()
    out.append(s[last:])
    return "".join(out)


def drop_unmatched_rparens(s: str) -> str:
    if not s:
        return ""
    out = []
    bal = 0
    for ch in s:
        if ch == "(":
            bal += 1
            out.append(ch)
        elif ch == ")":
            if bal > 0:
                bal -= 1
                out.append(ch)
            else:
                continue
        else:
            out.append(ch)
    return "".join(out)


def normalize_name_prefix(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    if s.casefold().startswith("тонер картридж"):
        s = "Тонер-картридж" + s[len("Тонер картридж"):]
    if s.casefold().startswith("тонер туба"):
        s = "Тонер-туба" + s[len("Тонер туба"):]
    return s


def normalize_vendor(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    has_lat = any(("A" <= ch <= "Z") or ("a" <= ch <= "z") for ch in v)
    has_cyr = any(("А" <= ch <= "я") or (ch in "Ёё") for ch in v)
    if has_lat and has_cyr:
        table = str.maketrans({
            "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X", "У": "Y",
            "а": "a", "в": "b", "е": "e", "к": "k", "м": "m", "н": "h", "о": "o", "р": "p", "с": "c", "т": "t", "х": "x", "у": "y",
        })
        v = v.translate(table)
    return v.strip()


def derive_vendor_from_name(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    m = _RE_BRAND_AFTER_DLYA.search(s)
    if m:
        return normalize_vendor(m.group(1))
    m = _RE_BRAND_AFTER_FOR.search(s)
    if m:
        return normalize_vendor(m.group(1))
    return ""


def cleanup_name_nvprint(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    # сначала латиница -> кириллица в русских словах
    from suppliers.nvprint.filtering import fix_mixed_ru
    s = fix_mixed_ru(s)
    # затем кириллица -> латиница в кодах/брендах
    s = fix_confusables_to_latin_in_latin_tokens(s)
    s = _RE_NV_SPACE.sub("NV-", s)
    s = _RE_DBL_SLASH.sub("/", s)
    s = _RE_SPACE_BEFORE_RP.sub(")", s)
    s = _RE_SHT_MISSING_SPACE.sub(r"(\1 шт)", s)
    s = _RE_NUM_SHT_WORD.sub(r"\1 шт", s)
    s = _RE_SLASH_BEFORE_LETTER.sub("/ ", s)
    s = _RE_WORKCENTRE.sub("WorkCentre", s)
    s = drop_unmatched_rparens(s)
    s = norm_ws(s)
    s = normalize_name_prefix(s)
    s = re.sub(r"^Тонер\s+картридж\b", "Тонер-картридж", s, flags=re.I)
    s = _RE_WS.sub(" ", s).strip()
    return s


def rename_param_key_nvprint(k: str) -> str:
    k = (k or "").strip()
    if not k:
        return ""
    return _PARAM_KEY_MAP_NVPRINT.get(k, k)


def cleanup_param_value_nvprint(k: str, v: str) -> str:
    kk = (k or "").strip()
    vv = (v or "").strip()
    if not kk or not vv:
        return vv
    cf = kk.casefold()
    if cf in ("цветпечати", "цвет печати"):
        vv_cf = vv.casefold().strip()
        return _COLOR_MAP.get(vv_cf, vv.strip())
    if cf in ("совместимостьсмоделями", "совместимость с моделями", "модель"):
        from suppliers.nvprint.filtering import fix_mixed_ru
        vv = fix_confusables_to_latin_in_latin_tokens(fix_mixed_ru(vv))
        vv = _RE_DBL_SLASH.sub("/", vv)
        vv = _RE_SLASH_BEFORE_LETTER.sub("/ ", vv)
        vv = _RE_SPACE_BEFORE_RP.sub(")", vv)
        vv = _RE_WORKCENTRE.sub("WorkCentre", vv)
        vv = drop_unmatched_rparens(vv)
        vv = _RE_WS.sub(" ", vv).strip()
        return vv
    return vv


def cleanup_vendor_nvprint(vendor: str, name: str) -> str:
    v = normalize_vendor(vendor or "")
    if v.casefold() in {"остальное", "прочее", "прочие", "другое", "другие", "other"}:
        v = ""
    if not v:
        v = derive_vendor_from_name(name)
    if v and v.casefold() in _STOP_BRAND_CF:
        v = ""
    if not v and "nvp" in (name or "").casefold():
        v = "NVP"
    return v.strip()
