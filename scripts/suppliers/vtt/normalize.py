# -*- coding: utf-8 -*-
"""VTT normalize layer — wave2.

Задача: перенести VTT-specific смысл из core в supplier-layer:
- vendor normalization
- type/color/resource extraction
- part/codes/compat extraction
- сбор clean raw params
"""

from __future__ import annotations

import re
from cs.core import norm_ws, clean_params

VENDOR_ALIAS = {
    "hewlettpackard": "HP",
    "hp": "HP",
    "kyocera": "Kyocera",
    "canon": "Canon",
    "xerox": "Xerox",
    "brother": "Brother",
    "samsung": "Samsung",
    "epson": "Epson",
    "ricoh": "Ricoh",
    "konicaminolta": "Konica Minolta",
    "konicaminolta": "Konica Minolta",
}

COLOR_MAP = {
    "black": "Черный",
    "mattblack": "Черный",
    "matteblack": "Черный",
    "photoblack": "Черный",
    "cyan": "Голубой",
    "magenta": "Пурпурный",
    "yellow": "Желтый",
    "red": "Красный",
    "blue": "Синий",
    "grey": "Серый",
    "gray": "Серый",
    "white": "Белый",
    "green": "Зеленый",
    "violet": "Фиолетовый",
    "черный": "Черный",
    "чёрный": "Черный",
    "голубой": "Голубой",
    "циан": "Голубой",
    "пурпурный": "Пурпурный",
    "маджента": "Пурпурный",
    "желтый": "Желтый",
    "жёлтый": "Желтый",
}

_RE_CODE = re.compile(r'\b(?:[A-Z]{1,4}\d[A-Z0-9-]{2,}|\d{2,}[A-Z-][A-Z0-9-]*|[A-Z]{1,3}-\d{2,}[A-Z0-9-]*|\d{5,}[A-Z]?)\b')
_RE_FOR = re.compile(r'\bдля\s+(.+?)(?:,|\s+\d+\s*К\b|\s+\(O\)|\s+[A-Z0-9-]{4,}|$)', re.I)
_RE_RES = re.compile(r'(\d+(?:[.,]\d+)?)\s*К\b', re.I)

def normalize_vendor(v: str) -> str:
    v = norm_ws(v or "")
    if not v:
        return ""
    key = re.sub(r'[^a-z0-9]+', '', v.lower())
    return VENDOR_ALIAS.get(key, v)

def infer_vendor(name: str, vendor: str = "") -> str:
    v = normalize_vendor(vendor)
    if v:
        return v
    s = (name or "").lower()
    for raw, norm in (
        ("hp", "HP"),
        ("hewlett-packard", "HP"),
        ("kyocera", "Kyocera"),
        ("canon", "Canon"),
        ("xerox", "Xerox"),
        ("brother", "Brother"),
        ("samsung", "Samsung"),
        ("epson", "Epson"),
        ("ricoh", "Ricoh"),
        ("konica", "Konica Minolta"),
    ):
        if raw in s:
            return norm
    return ""

def infer_type(name: str) -> str:
    s = (name or "").lower()
    if "тонер-картридж" in s:
        return "Тонер-картридж"
    if "картридж" in s:
        return "Картридж"
    if "блок фотобарабана" in s or "фотобарабан" in s:
        return "Блок фотобарабана"
    if "термоблок" in s:
        return "Термоблок"
    if "драм" in s:
        return "Драм-картридж"
    return ""

def infer_color(name: str, pairs: dict[str, str]) -> str:
    # 1) explicit pairs
    for k in ("Цвет", "Color"):
        if pairs.get(k):
            v = pairs[k].strip()
            return COLOR_MAP.get(v.lower(), v)
    # 2) from name tail
    lower = (name or "").lower()
    for raw, norm in COLOR_MAP.items():
        if re.search(rf'(?:^|[,\s/]){re.escape(raw)}(?:$|[,\s/])', lower):
            return norm
    return ""

def infer_resource(name: str, pairs: dict[str, str]) -> str:
    for k in ("Ресурс", "Resource"):
        if pairs.get(k):
            return norm_ws(pairs[k])
    m = _RE_RES.search(name or "")
    return m.group(1).replace('.', ',') + "K" if m else ""

def extract_codes(name: str, pairs: dict[str, str]) -> list[str]:
    raw = []
    for k in ("OEM-номер", "Каталожный номер", "Партномер", "Артикул", "Партс-номер"):
        if pairs.get(k):
            raw.extend(re.split(r'[/,;]\s*', pairs[k]))
    raw.extend(_RE_CODE.findall(name or ""))
    out = []
    seen = set()
    for x in raw:
        x = norm_ws(x).strip(" ,;/")
        if not x:
            continue
        if x.lower() in {"oem", "dj", "wc", "dc"}:
            continue
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def infer_partnumber(codes: list[str], pairs: dict[str, str]) -> str:
    for k in ("OEM-номер", "Каталожный номер", "Партномер", "Партс-номер", "Артикул"):
        if pairs.get(k):
            v = norm_ws(pairs[k])
            if v:
                return v
    return codes[0] if codes else ""

def infer_compat(name: str, pairs: dict[str, str], vendor: str) -> str:
    for k in ("Совместимость", "Совместимость с моделями"):
        if pairs.get(k):
            return norm_ws(pairs[k])
    m = _RE_FOR.search(name or "")
    if not m:
        return ""
    tail = norm_ws(m.group(1))
    if not tail:
        return ""
    # prepend vendor if tail starts with series only
    if vendor and not tail.lower().startswith(vendor.lower()):
        return f"{vendor} {tail}"
    return tail

def normalize_name(name: str) -> str:
    s = norm_ws(name or "")
    s = s.replace("Mattblack", "MattBlack").replace("photoblack", "PhotoBlack")
    s = re.sub(r'\s+/\s+', '/', s)
    return s

def build_clean_params(name: str, vendor: str, pairs: dict[str, str]) -> list[tuple[str, str]]:
    vendor = infer_vendor(name, vendor)
    ptype = infer_type(name)
    color = infer_color(name, pairs)
    codes = extract_codes(name, pairs)
    part = infer_partnumber(codes, pairs)
    compat = infer_compat(name, pairs, vendor)
    resource = infer_resource(name, pairs)

    params: list[tuple[str, str]] = []
    if part:
        params.append(("Партномер", part))
    if codes:
        params.append(("Коды расходников", ", ".join(codes)))
    if ptype:
        params.append(("Тип", ptype))
    if color:
        params.append(("Цвет", color))
    if compat:
        params.append(("Совместимость", compat))
    if resource:
        params.append(("Ресурс", resource))

    # Сохраняем полезные дополнительные поля, но без supplier-мусора
    keep_extra = {
        "Объем": "Объем",
        "Объём": "Объем",
        "Формат": "Формат",
        "Размер": "Размер",
    }
    for k, v in pairs.items():
        kk = norm_ws(k)
        vv = norm_ws(v)
        if not kk or not vv:
            continue
        if kk in {"Артикул", "Партс-номер", "Вендор", "Цена", "Стоимость", "Категория", "Подкатегория", "Штрих-код", "Штрихкод", "EAN", "Barcode", "OEM-номер", "Каталожный номер", "Аналоги"}:
            continue
        if kk in keep_extra:
            params.append((keep_extra[kk], vv))

    return clean_params(params)
