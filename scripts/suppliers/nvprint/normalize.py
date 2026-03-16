# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/nvprint/normalize.py
NVPrint normalize layer — name/vendor/oid и мелкая очистка.
"""

from __future__ import annotations

import re
from xml.etree import ElementTree as ET

from scripts.suppliers.nvprint.source import get_text, pick_first_text


RE_DESC_HAS_CS = re.compile(r"<!--\s*WhatsApp\s*-->|<!--\s*Описание\s*-->|<h3>\s*Характеристики\s*</h3>", re.I)
RE_TOKEN = re.compile(r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-._/]+")
RE_DBL_SLASH = re.compile(r"//+")
RE_NV_SPACE = re.compile(r"\bNV-\s+")
RE_WS = re.compile(r"\s+")
RE_SPACE_BEFORE_RP = re.compile(r"\s+\)")
RE_SLASH_BEFORE_LETTER = re.compile(r"/(?!\s)(?=[A-Za-zА-Яа-я])")
RE_SHT_MISSING_SPACE = re.compile(r"\((\d+)шт\)", re.I)
RE_NUM_SHT_WORD = re.compile(r"\b(\d+)шт\b", re.I)
RE_WORKCENTRE = re.compile(r"\bWorkcentr(e)?\b", re.I)
RE_BRAND_AFTER_DLYA = re.compile(r"\bдля\s+([A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-._]{1,40})", re.I)
RE_BRAND_AFTER_FOR = re.compile(r"\bfor\s+([A-Za-z0-9][A-Za-z0-9\-._]{1,40})", re.I)

CYR2LAT = {
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X", "У": "Y",
    "а": "a", "в": "b", "е": "e", "к": "k", "м": "m", "н": "h", "о": "o", "р": "p", "с": "c", "т": "t", "х": "x", "у": "y",
}

COLOR_MAP = {
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

PARAM_KEY_MAP_NVPRINT = {
    "ТипПечати": "Тип печати",
    "ЦветПечати": "Цвет печати",
    "СовместимостьСМоделями": "Совместимость с моделями",
}

STOP_BRAND_CF = {
    "лазерных", "струйных", "принтеров", "мфу", "копиров", "копировальных", "плоттеров",
    "принтера", "устройств", "устройства", "печати", "всех",
}

DROP_PARAM_NAMES_CF = {
    "артикул", "остаток", "наличие", "в наличии", "сопутствующие товары", "sku", "код", "guid",
    "ссылканакартинку", "вес", "высота", "длина", "ширина", "объем", "объём", "разделкаталога", "разделмодели",
}



def make_oid(item: ET.Element, name: str) -> str | None:
    """Собрать NP oid."""
    raw = (
        pick_first_text(item, ("vendorCode", "article", "Артикул", "sku", "code", "Код", "Guid"))
        or (item.get("id") or "").strip()
    )
    if not raw:
        return None
    out = []
    for ch in raw.strip():
        if re.fullmatch(r"[A-Za-z0-9_.-]", ch):
            out.append(ch)
        else:
            out.append("_")
    oid = "".join(out)
    if not oid.startswith("NP"):
        oid = "NP" + oid
    return oid



def fix_confusables_to_latin_in_latin_tokens(s: str) -> str:
    """Починить кириллицу внутри латинских токенов."""
    if not s:
        return ""
    out = []
    last = 0
    for m in RE_TOKEN.finditer(s):
        out.append(s[last:m.start()])
        tok = m.group(0)
        has_lat = any(("A" <= ch <= "Z") or ("a" <= ch <= "z") for ch in tok)
        if has_lat:
            tok = "".join(CYR2LAT.get(ch, ch) for ch in tok)
        out.append(tok)
        last = m.end()
    out.append(s[last:])
    return "".join(out)



def drop_unmatched_rparens(s: str) -> str:
    """Убрать лишние закрывающие скобки."""
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
            out.append(ch)
    return "".join(out)



def normalize_name_prefix(name: str) -> str:
    """Привести префиксы к одному виду."""
    s = (name or "").strip()
    if not s:
        return ""
    if s.casefold().startswith("тонер картридж"):
        s = "Тонер-картридж" + s[len("Тонер картридж"):]
    if s.casefold().startswith("тонер туба"):
        s = "Тонер-туба" + s[len("Тонер туба"):]
    return s



def cleanup_name_nvprint(name: str, fix_mixed_ru_func) -> str:
    """Почистить имя товара."""
    s = (name or "").strip()
    if not s:
        return ""
    s = fix_mixed_ru_func(s)
    s = fix_confusables_to_latin_in_latin_tokens(s)
    s = RE_NV_SPACE.sub("NV-", s)
    s = RE_DBL_SLASH.sub("/", s)
    s = RE_SPACE_BEFORE_RP.sub(")", s)
    s = RE_SHT_MISSING_SPACE.sub(r"(\1 шт)", s)
    s = RE_NUM_SHT_WORD.sub(r"\1 шт", s)
    s = RE_SLASH_BEFORE_LETTER.sub("/ ", s)
    s = RE_WORKCENTRE.sub("WorkCentre", s)
    s = drop_unmatched_rparens(s)
    s = re.sub(r"\s+", " ", s).strip()
    s = normalize_name_prefix(s)
    s = re.sub(r"^Тонер\s+картридж\b", "Тонер-картридж", s, flags=re.I)
    s = RE_WS.sub(" ", s).strip()
    return s



def normalize_vendor(v: str) -> str:
    """Почистить vendor."""
    v = (v or "").strip()
    if not v:
        return ""
    has_lat = any(("A" <= ch <= "Z") or ("a" <= ch <= "z") for ch in v)
    has_cyr = any(("А" <= ch <= "я") or (ch in "Ёё") for ch in v)
    if has_lat and has_cyr:
        table = str.maketrans(CYR2LAT)
        v = v.translate(table)
    return v.strip()



def derive_vendor_from_name(name: str) -> str:
    """Попробовать взять бренд из названия."""
    s = (name or "").strip()
    if not s:
        return ""
    m = RE_BRAND_AFTER_DLYA.search(s)
    if m:
        return normalize_vendor(m.group(1))
    m = RE_BRAND_AFTER_FOR.search(s)
    if m:
        return normalize_vendor(m.group(1))
    return ""



def cleanup_vendor_nvprint(vendor: str, name: str) -> str:
    """Нормализовать vendor NVPrint."""
    v = normalize_vendor(vendor or "")
    if v.casefold() in {"остальное", "прочее", "прочие", "другое", "другие", "other"}:
        v = ""
    if not v:
        v = derive_vendor_from_name(name)
    if v and v.casefold() in STOP_BRAND_CF:
        v = ""
    if not v and "nvp" in (name or "").casefold():
        v = "NVP"
    return v.strip()



def rename_param_key_nvprint(k: str) -> str:
    """Переименовать supplier-ключ."""
    k = (k or "").strip()
    if not k:
        return ""
    return PARAM_KEY_MAP_NVPRINT.get(k, k)



def cleanup_param_value_nvprint(k: str, v: str, fix_mixed_ru_func) -> str:
    """Почистить supplier-значение param."""
    kk = (k or "").strip()
    vv = (v or "").strip()
    if not kk or not vv:
        return vv
    cf = kk.casefold()
    if cf in ("цветпечати", "цвет печати"):
        return COLOR_MAP.get(vv.casefold().strip(), vv.strip())
    if cf in ("совместимостьсмоделями", "совместимость с моделями", "модель"):
        vv = fix_confusables_to_latin_in_latin_tokens(fix_mixed_ru_func(vv))
        vv = RE_DBL_SLASH.sub("/", vv)
        vv = RE_SLASH_BEFORE_LETTER.sub("/ ", vv)
        vv = RE_SPACE_BEFORE_RP.sub(")", vv)
        vv = RE_WORKCENTRE.sub("WorkCentre", vv)
        vv = drop_unmatched_rparens(vv)
        vv = RE_WS.sub(" ", vv).strip()
        return vv
    return vv



def native_desc(item: ET.Element) -> str:
    """Взять supplier-description, если он не похож на CS."""
    d = pick_first_text(item, ("description", "Описание"))
    if not d:
        return ""
    if RE_DESC_HAS_CS.search(d):
        return ""
    return d



def supplier_name_from_item(item: ET.Element) -> str:
    """Взять сырой name из item."""
    return (
        get_text(item.find("Номенклатура"))
        or get_text(item.find("НоменклатураКратко"))
        or pick_first_text(item, ("name", "title", "Наименование"))
    )
