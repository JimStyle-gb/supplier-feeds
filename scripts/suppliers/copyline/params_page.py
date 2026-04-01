# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/params_page.py
CopyLine page-params layer.

Задача:
- быть главным extractor-модулем для page/body parsing;
- принимать раздельные сырьевые каналы (table params / desc pairs / text-body);
- нормализовать page params из HTML-таблиц/описания;
- поднять supplier-specific поля до raw;
- не тянуть device-list в Коды расходников;
- корректно поднимать single-tail и multi-tail вида Canon 725 / Canon 712/713/725.

Важно:
- модуль backward-safe: умеет работать и со старым page_params, и с новыми
  raw_desc_pairs/raw_table_params;
- семантическое слияние каналов выполняется здесь, а не в source.py.
"""

from __future__ import annotations

import re
from typing import List, Sequence, Tuple


CODE_RX = re.compile(
    r"\b(?:"
    r"CF\d{3,4}[A-Z]?|CE\d{3,4}[A-Z]?|CB\d{3,4}[A-Z]?|CC\d{3,4}[A-Z]?|Q\d{4}[A-Z]?|W\d{4}[A-Z0-9]{1,4}|"
    r"106R\d{5}|006R\d{5}|108R\d{5}|113R\d{5}|013R\d{5}|016\d{6}|NPG-\d+[A-Z]?|GPR-\d+[A-Z]?|EP-\d+[A-Z]?|E-\d+[A-Z]?|FX-\d+[A-Z]?|T\d{2}[A-Z]?|S-\d{4,5}[A-Z]?|DQ-[A-Z0-9-]+|FQ-[A-Z0-9-]+|"
    r"TK-?\d{3,5}[A-Z0-9]*|MLT-[A-Z]\d{3,5}[A-Z0-9/]*|CLT-[A-Z]\d{3,5}[A-Z]?|"
    r"ML-D\d+[A-Z]?|ML-\d{4,5}[A-Z]\d?|SCX-D\d+[A-Z]?|T-\d{3,6}[A-Z]?|KX-FA\d+[A-Z0-9]{0,2}|KX-FAT\d+[A-Z0-9]{0,2}|KX-FAD\d+[A-Z0-9]{0,2}|"
    r"C-?EXV\d+[A-Z]*|DR-\d+[A-Z0-9-]*|TN-\d+[A-Z0-9-]*|PC-?\d+[A-Z0-9-]*|TL-?\d+[A-Z0-9-]*|DL-?\d+[A-Z0-9-]*|"
    r"C13T\d{5,8}[A-Z0-9]*|C13S\d{6,8}[A-Z0-9]*|C12C\d{5,8}[A-Z0-9]*|C33S\d{5,8}[A-Z0-9]*|"
    r"50F\d[0-9A-Z]{2,4}|51B[0-9A-Z]{4,5}|52D[0-9A-Z]{4,5}|55B\d[0-9A-Z]{2,4}|56F\d[0-9A-Z]{2,4}|60F[0-9A-Z]{4,5}|0?71H|C\d{4}[A-Z]|CZ\d{3}[A-Z]?|SP\d{3,5}[A-Z]{1,3}|SP\s?C\d{3,5}[A-Z]?|SPC\d{3,5}[A-Z]?|101R\d{5}|CZ\s?\d{3}[A-Z]?|T\d{5,8}[A-Z]?|842\d{3,6}|DK-?\d{3,5}|DR\d{2,5}|408059|MP\d{3,5}[A-Z]?|X\d{3,6}[A-Z0-9]{1,4}|DV-\d+[KCMY]?|D-\d{4,5}|\d{4}-\d{3}|TK-\d{1,4}/\d{2,4}"
    r")\b",
    re.I,
)

COMPAT_PATTERNS = [
    re.compile(r"совместимость\s+с\s+устройствами\s*:?\s*(.+)", re.I | re.S),
    re.compile(r"используется\s+в\s+многофункциональных\s+аппаратах\s+серий\s+(.+)", re.I | re.S),
    re.compile(r"используется\s+в\s+многофункциональных\s+аппаратах\s+(.+)", re.I | re.S),
    re.compile(r"используется\s+в\s+многофункциональных\s+устройствах\s+серий\s+(.+)", re.I | re.S),
    re.compile(r"используется\s+в\s+многофункциональных\s+устройствах\s+(.+)", re.I | re.S),
    re.compile(r"используется\s+в\s+факсимильных\s+аппаратах\s+(.+)", re.I | re.S),
    re.compile(r"используется\s+в\s+факсах\s+(.+)", re.I | re.S),
    re.compile(r"используется\s+в\s+аппаратах\s+(.+)", re.I | re.S),
    re.compile(r"используется\s+в\s+принтерах\s+серий\s+(.+)", re.I | re.S),
    re.compile(r"используется\s+в\s+принтерах\s+(.+)", re.I | re.S),
    re.compile(r"для\s+принтеров\s+серий\s+(.+)", re.I | re.S),
    re.compile(r"для\s+принтеров\s+(.+)", re.I | re.S),
    re.compile(r"применяется\s+в\s+многофункциональных\s+принтерах\s+(.+)", re.I | re.S),
    re.compile(r"применяется\s+в\s+многофункциональных\s+устройствах\s+(.+)", re.I | re.S),
    re.compile(r"применяется\s+в\s+многофункциональных\s+аппаратах\s+(.+)", re.I | re.S),
    re.compile(r"применяется\s+в\s+МФУ\s+(.+)", re.I | re.S),
    re.compile(r"применяется\s+в\s+(.+)", re.I | re.S),
    re.compile(r"совместим\s+с\s+(.+)", re.I | re.S),
    re.compile(r"подходит\s+для\s+(.+)", re.I | re.S),
    re.compile(r"используется\s+с\s+(.+)", re.I | re.S),
    re.compile(r"для\s+устройств\s+(.+)", re.I | re.S),
    re.compile(r"для\s+аппаратов\s+(.+)", re.I | re.S),
]

STOP_HEADERS_RX = re.compile(
    r"(?:^|\b)(?:Производитель|Размер(?:\s+упаковки)?|Вес(?:\s+в\s+упаковке)?|Технические\s+характеристики|"
    r"Основные\s+характеристики|Характеристики|Артикул|Код\s+товара|Ресурс|Количество\s+страниц|"
    r"Цвет(?:\s+печати)?|Технология\s+печати|Тип\s+кабеля|Количество\s+пар|Толщина\s+проводников|"
    r"Категория|Материал\s+изоляции|Бухта)\b",
    re.I,
)

COMPAT_GUARD_RX = re.compile(
    r"(?:совместимость\s+с\s+устройствами|используется\s+в|для\s+принтеров|для\s+устройств|"
    r"для\s+аппаратов|применяется\s+в|подходит\s+для|совместим\s+с)",
    re.I,
)

CABLE_PARAM_KEYS = {
    "Тип кабеля",
    "Количество пар",
    "Толщина проводников",
    "Категория",
    "Материал изоляции",
    "Бухта",
}

CABLE_TYPE_RX = re.compile(r"\b(UTP|FTP|STP|SFTP|F/UTP|U/UTP|F/FTP|U/FTP)\b", re.I)
CABLE_CATEGORY_RX = re.compile(r"\bCat\.?\s*(5e|6a|6|7|7a|8)\b", re.I)
CABLE_DIM_RX = re.compile(r"\b(\d+)x\d+x\d+/([0-9]+(?:[.,][0-9]+)?)\b", re.I)
CABLE_MATERIAL_RX = re.compile(r"\b(LSZH|PVC|PE)\b", re.I)
CABLE_SPOOL_RX = re.compile(r"\b(\d+)\s*м/б\b", re.I)


CODE_PREFIX_WEIGHTS = (
    (re.compile(r"^(?:CF|CE|CB|CC|Q|W)\d", re.I), 100),
    (re.compile(r"^(?:106R|006R|108R|113R|013R)\d", re.I), 100),
    (re.compile(r"^016\d{6}$", re.I), 95),
    (re.compile(r"^Canon\s+[A-Z]{1,5}-?[A-Z0-9]{1,8}$", re.I), 93),
    (re.compile(r"^Canon\s+\d{3,4}$", re.I), 92),
    (re.compile(r"^(?:MLT-|CLT-|TK-|KX-FA|KX-FAT|C-?EXV|DR-|TN-|C13T|C12C|C33S|NPG-|GPR-|EP-|E-|FX-)", re.I), 95),
    (re.compile(r"^T\d{2}[A-Z]?$", re.I), 95),
    (re.compile(r"^ML-D\d", re.I), 90),
    (re.compile(r"^(?:ML-\d{4,5}[A-Z]\d?|SP\d{3,5}[A-Z]{1,3}|SCX-D\d+[A-Z]?)$", re.I), 85),
    (re.compile(r"^(?:50F|51B|52D|55B|56F|60F)\w+$", re.I), 90),
    (re.compile(r"^0?71H$", re.I), 90),
)

DEVICE_ONLY_RX = re.compile(
    r"^(?:ML-\d{4,5}|SCX-\d{4,5}|SF-?\d{3,5}|WC\s?\d{4}|P\d{4}|LBP-?\d{4}|KX-FL\d{3,4}|KX-FLM\d{3,4})$",
    re.I,
)

CONSUMABLE_TITLE_RX = re.compile(
    r"^(?:картридж|тонер-картридж|тонер\s+картридж|драм-картридж|драм\s+картридж|drum|чернила|девелопер|термоблок|термоэлемент)",
    re.I,
)


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def _title_kind(title: str) -> str:
    t = safe_str(title).lower()
    if not t:
        return ""
    if t.startswith("тонер-картридж") or t.startswith("тонер картридж"):
        return "Тонер-картридж"
    if t.startswith("картридж"):
        return "Картридж"
    if t.startswith("кабель сетевой"):
        return "Кабель сетевой"
    if t.startswith("термоблок"):
        return "Термоблок"
    if t.startswith("термоэлемент"):
        return "Термоэлемент"
    if t.startswith("девелопер") or t.startswith("developer"):
        return "Девелопер"
    if t.startswith("драм") or t.startswith("drum"):
        return "Драм-картридж"
    if t.startswith("чернила"):
        return "Чернила"
    return ""


KEY_MAP = {
    "технология печати": "Технология печати",
    "цвет печати": "Цвет",
    "цвет": "Цвет",
    "количество страниц (5% заполнение)": "Количество страниц (5% заполнение)",
    "количество страниц": "Количество страниц (5% заполнение)",
    "ресурс": "Ресурс",
    "совместимость": "Совместимость",
    "тип кабеля": "Тип кабеля",
    "количество пар": "Количество пар",
    "толщина проводников": "Толщина проводников",
    "категория": "Категория",
    "материал изоляции": "Материал изоляции",
    "бухта": "Бухта",
}


def _norm_spaces(s: str) -> str:
    s = safe_str(s).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _normalize_code_token(s: str) -> str:
    s = safe_str(s).upper()
    s = re.sub(r"\s*-\s*", "-", s)
    s = re.sub(r"\s+", "", s)
    return s


def _normalize_code_search_text(text: str) -> str:
    text = safe_str(text).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\b(113R|108R|106R|006R|013R|016|C13T|C12C|C33S)\s+(\d{4,8}[A-Z0-9]*)\b", r"\1\2", text, flags=re.I)
    text = re.sub(r"\b(CLT|MLT|ML|KX|TK|TN|DR|DL|TL|PC|T|C|NPG|GPR|EP|E|FX|DQ|FQ|S)\s*-\s*([A-Z0-9]{1,})\b", r"\1-\2", text, flags=re.I)
    return text.strip()


def _norm_color(val: str) -> str:
    s = safe_str(val)
    repl = {
        "black": "Чёрный",
        "yellow": "Желтый",
        "magenta": "Пурпурный",
        "cyan": "Голубой",
    }
    if not s:
        return ""
    low = s.casefold()
    for k, v in repl.items():
        if low == k:
            return v
    return s[:120]


def _dedupe_params(items: Sequence[Tuple[str, str]]) -> List[Tuple[str, str]]:
    out: list[Tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for k, v in items:
        k2 = safe_str(k)
        v2 = safe_str(v)
        if not k2 or not v2:
            continue
        sig = (k2.casefold(), v2.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((k2, v2))
    return out


def _is_consumable_title(title: str) -> bool:
    return bool(CONSUMABLE_TITLE_RX.search(safe_str(title)))


def _is_allowed_numeric_code(code: str) -> bool:
    code = _normalize_code_token(code)
    return bool(re.fullmatch(r"016\d{6}", code))


def _looks_device_series(code: str) -> bool:
    code = _normalize_code_token(code)
    if DEVICE_ONLY_RX.fullmatch(code):
        return True
    if re.fullmatch(r"\d{3}", code):
        return True
    return False


def _code_weight(code: str) -> int:
    raw = _norm_spaces(code)
    if re.fullmatch(r"Canon\s+\d{3,4}[A-Z]?", raw, re.I):
        return 92
    norm = _normalize_code_token(raw)
    if re.fullmatch(r"CANON\d{3,4}[A-Z]?", norm, re.I):
        return 92
    for rx, weight in CODE_PREFIX_WEIGHTS:
        if rx.search(norm):
            return weight
    if _is_allowed_numeric_code(norm):
        return 95
    return 10


def _extract_title_canon_numeric_codes(title: str) -> list[str]:
    title = _norm_spaces(title)
    out: list[str] = []
    seen: set[str] = set()
    patterns = [
        re.compile(r"\bCanon\s+((?:\d{3,4}[A-Z]?)(?:\s*/\s*\d{3,4}[A-Z]?){0,5})\b", re.I),
        re.compile(r"(?:^|[/(,])\s*Canon\s+((?:\d{3,4}[A-Z]?)(?:\s*/\s*\d{3,4}[A-Z]?){0,5})\b", re.I),
    ]
    for rx in patterns:
        for m in rx.finditer(title):
            for part in re.split(r"\s*/\s*", safe_str(m.group(1))):
                token = _normalize_code_token(part)
                if not token:
                    continue
                branded = f"Canon {token}"
                if branded in seen:
                    continue
                seen.add(branded)
                out.append(branded)
    return out




def _extract_title_canon_family_codes(title: str) -> list[str]:
    title = _norm_spaces(title)
    out: list[str] = []
    seen: set[str] = set()
    family_token = r"(?:C-?EXV\d+[A-Z]*|NPG-\d+[A-Z]?|GPR-\d+[A-Z]?|EP-\d+[A-Z]?|E-\d+[A-Z]?|FX-\d+[A-Z]?|T\d{2}[A-Z]?)"
    patterns = [
        re.compile(rf"\bCanon\s+(({family_token})(?:\s*/\s*{family_token}){{0,5}})\b", re.I),
        re.compile(rf"(?:^|[/(,])\s*Canon\s+(({family_token})(?:\s*/\s*{family_token}){{0,5}})\b", re.I),
    ]
    for rx in patterns:
        for m in rx.finditer(title):
            for part in re.split(r"\s*/\s*", safe_str(m.group(1))):
                token = _normalize_code_token(part)
                if not token:
                    continue
                if token in seen:
                    continue
                seen.add(token)
                out.append(token)
    return out

def _extract_xerox_developer_title_codes(title: str) -> list[str]:
    title = _norm_spaces(title)
    if not title or not re.search(r"\bДевелопер\b", title, re.I) or not re.search(r"\bXerox\b", title, re.I):
        return []
    out: list[str] = []
    seen: set[str] = set()
    patterns = [
        re.compile(r"\bXerox\s+(DC\s*\d{3}(?:\s*/\s*\d{3})+)\b", re.I),
        re.compile(r"\bXerox\s+(WC\s*\d{4}(?:\s*/\s*\d{4})*)\b", re.I),
        re.compile(r"\bXerox\s+(Phaser\s*\d{4}(?:\s*/\s*\d{4})+)\b", re.I),
    ]
    for rx in patterns:
        for m in rx.finditer(title):
            token = _norm_spaces(m.group(1))
            token = re.sub(r"\s*/\s*", "/", token)
            if token and token not in seen:
                seen.add(token)
                out.append(token)
    return out


def _extract_title_bare_family_codes(title: str) -> list[str]:
    title = _norm_spaces(title)
    out: list[str] = []
    seen: set[str] = set()

    bare_patterns = [
        re.compile(r"\bC\d{4}[A-Z]\b", re.I),
        re.compile(r"\bC13T[0-9A-Z]{5,10}\b", re.I),
        re.compile(r"\bCZ\d{3}[A-Z]?\b", re.I),
        re.compile(r"\bSP\d{3,5}[A-Z]?\b", re.I),
        re.compile(r"\bSP\s?C\d{3,5}[A-Z]?\b", re.I),
        re.compile(r"\bSPC\d{3,5}[A-Z]?\b", re.I),
        re.compile(r"\b101R\d{5}\b", re.I),
        re.compile(r"\bCZ\s?\d{3}\b", re.I),
        re.compile(r"\bS-\d{4,5}[A-Z]?\b", re.I),
        re.compile(r"\bDQ-[A-Z0-9-]+\b", re.I),
        re.compile(r"\bFQ-[A-Z0-9-]+\b", re.I),
        re.compile(r"\bT\d{5,8}[A-Z]?\b", re.I),
        re.compile(r"\b842\d{3,6}\b", re.I),
        re.compile(r"\b408059\b", re.I),
        re.compile(r"\bMP\d{3,5}[A-Z]?\b", re.I),
        re.compile(r"\bX\d{3,6}[A-Z0-9]{1,4}\b", re.I),
        re.compile(r"\bDV-\d+[KCMY]?\b", re.I),
        re.compile(r"\bD-\d{4,5}\b", re.I),
        re.compile(r"\b\d{4}-\d{3}\b", re.I),
        re.compile(r"\bTK-\d{1,4}/\d{2,4}\b", re.I),
        re.compile(r"\bDK-?\d{3,5}\b", re.I),
        re.compile(r"\bDR\d{2,5}\b", re.I),
    ]
    for rx in bare_patterns:
        for m in rx.finditer(title):
            token = _normalize_code_token(m.group(0))
            if token and token not in seen:
                seen.add(token)
                out.append(token)

    for m in re.finditer(r"\bMP\s*C(\d{4})(?:\s*/\s*C?(\d{4}))*", title, re.I):
        first = safe_str(m.group(1))
        if first:
            token = f"MP C{first}"
            if token not in seen:
                seen.add(token)
                out.append(token)
        tail = title[m.start(): m.end()]
        for part in re.findall(r"\bC?(\d{4})\b", tail, re.I):
            token = f"MP C{safe_str(part)}"
            if token not in seen:
                seen.add(token)
                out.append(token)

    return out


def _extract_ink_title_compat(title: str) -> str:
    title = _norm_spaces(title)
    if not title:
        return ""
    m = re.search(r"\bfor\s+([A-Z]?\d{3,5}(?:\s*/\s*[A-Z]?\d{3,5}){1,8})\b", title, re.I)
    if not m:
        return ""
    brand = ""
    if re.search(r"\bEpson\b", title, re.I):
        brand = "Epson"
    elif re.search(r"\bRISO\b", title, re.I):
        brand = "RISO"
    parts = [safe_str(x) for x in re.split(r"\s*/\s*", safe_str(m.group(1))) if safe_str(x)]
    out = []
    for part in parts:
        token = _normalize_code_token(part)
        if not token or len(token) < 3:
            continue
        if brand:
            out.append(f"{brand} {token}")
        else:
            out.append(token)
    return ", ".join(out[:8])


def _extract_riso_title_compat(title: str) -> str:
    title = _norm_spaces(title)
    if not title or not re.search(r"\bRISO\b", title, re.I):
        return ""
    m = re.search(r"\bfor\s+([A-Z]?\d{2,5}(?:\s*/\s*[A-Z]?\d{2,5}){1,8})\b", title, re.I)
    if m:
        parts = [safe_str(x) for x in re.split(r"\s*/\s*", safe_str(m.group(1))) if safe_str(x)]
        out = []
        for part in parts:
            token = _normalize_code_token(part)
            if token and len(token) >= 3:
                out.append(f"RISO {token}")
        return ", ".join(out[:8])

    m = re.search(r"\bRISO\s+((?:RP|RZ\s*/\s*RV|CZ\s*\d{2,4}))\b", title, re.I)
    if not m:
        return ""
    token = _norm_spaces(m.group(1)).upper()
    if "/" in token:
        return ", ".join([f"RISO {safe_str(x).upper()}" for x in re.split(r"\s*/\s*", token) if safe_str(x)])
    return f"RISO {token}"


def _extract_epson_desc_compat(title: str, description: str) -> str:
    blob = _norm_spaces(f"{safe_str(title)} {safe_str(description)}")
    if not re.search(r"\bEpson\b", blob, re.I):
        return ""
    m = re.search(r"(?:для|for)\s+((?:L?\d{4,5})(?:\s*/\s*L?\d{4,5}){1,8})", blob, re.I)
    if not m:
        return ""
    parts = [safe_str(x) for x in re.split(r"\s*/\s*", m.group(1)) if safe_str(x)]
    out = []
    for part in parts:
        part = part.upper()
        if not part.startswith("L"):
            part = f"L{part}"
        out.append(f"Epson {part}")
    return ", ".join(out[:8])


def _extract_panasonic_integral_compat(description: str) -> str:
    d = _norm_spaces(description)
    if not d:
        return ""
    m = re.search(r"(?:для|used in|совместим(?:ость)? с)\s+((?:Panasonic|INTEGRAL)[^.;\n]{3,180})", d, re.I)
    if not m:
        return ""
    return _trim_compat_tail(m.group(1))


def _split_title_body_parts(title: str) -> tuple[str, str]:
    title = _norm_spaces(title)
    if not title:
        return "", ""
    m = re.search(r"\b(?:для\s+принтеров|для\s+МФУ|для\s+устройств|совместимость\s+с)\b", title, flags=re.I)
    if not m:
        return title, ""
    return title[: m.start()].strip(" ,;/"), title[m.start():].strip()


def _extract_single_brand_numeric_tail(title: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    text = _normalize_code_search_text(title)
    for m in re.finditer(r"(?:^|/)\s*(Canon)\s+(\d{3,4}[A-Z]?)\b", text, re.I):
        brand = _norm_spaces(m.group(1))
        token = _normalize_code_token(m.group(2))
        if not token:
            continue
        branded = f"{brand.title()} {token}"
        if branded not in seen:
            seen.add(branded)
            out.append(branded)
    return out




def _extract_title_brand_alpha_tail(title: str) -> list[str]:
    title = _norm_spaces(title)
    out: list[str] = []
    seen: set[str] = set()

    for token in _extract_title_canon_family_codes(title):
        if token not in seen:
            seen.add(token)
            out.append(token)

    family_token = r"(?:C-?EXV\d+[A-Z]*|NPG-\d+[A-Z]?|GPR-\d+[A-Z]?|EP-\d+[A-Z]?|E-\d+[A-Z]?|FX-\d+[A-Z]?|T\d{2}[A-Z]?)"
    branded_tail_rx = re.compile(
        rf"(?:^|[/(,])\s*(Canon)\s+(({family_token})(?:\s*/\s*{family_token}){{0,5}})\b",
        re.I,
    )
    for m in branded_tail_rx.finditer(title):
        for part in re.split(r"\s*/\s*", safe_str(m.group(2))):
            token = _normalize_code_token(part)
            if not token or re.fullmatch(r"\d{3,4}[A-Z]?", token, re.I):
                continue
            if token not in seen:
                seen.add(token)
                out.append(token)
    return out

def _extract_title_multicode_tail(title: str) -> list[str]:
    title = _norm_spaces(title)
    out: list[str] = []
    seen: set[str] = set()

    for token in _extract_title_canon_family_codes(title):
        if token not in seen:
            seen.add(token)
            out.append(token)

    branded_tail_rx = re.compile(
        r"(?:^|/|[,(])\s*(Canon|Toshiba|Ricoh|Panasonic)\s+((?:[A-Z]?\d{3,6}[A-Z]?)(?:\s*/\s*[A-Z]?\d{3,6}[A-Z]?){0,5})\b",
        re.I,
    )
    for m in branded_tail_rx.finditer(title):
        brand = safe_str(m.group(1)).title()
        parts = [safe_str(x) for x in re.split(r"\s*/\s*", safe_str(m.group(2))) if safe_str(x)]
        for part in parts:
            token = _normalize_code_token(part)
            if not token:
                continue
            if token.isdigit():
                if brand.casefold() != "canon":
                    continue
                token = f"Canon {token}"
            elif brand.casefold() == "canon" and re.fullmatch(r"\d{3,4}[A-Z]?", token, re.I):
                token = f"Canon {token}"
            if token not in seen:
                seen.add(token)
                out.append(token)

    for token in _extract_title_canon_numeric_codes(title):
        branded = f"Canon {token}" if re.fullmatch(r"\d{3,4}[A-Z]?", token, re.I) else token
        if branded not in seen:
            seen.add(branded)
            out.append(branded)
    return out

def _strip_compat_zone(text: str) -> str:
    text = _norm_spaces(text)
    if not text:
        return ""
    m = COMPAT_GUARD_RX.search(text)
    if m:
        return text[: m.start()].strip()
    return text


def _collect_codes_from_text(text: str, *, allow_numeric: bool) -> list[str]:
    text = _normalize_code_search_text(text)
    found: list[str] = []
    seen: set[str] = set()
    for m in CODE_RX.finditer(text):
        val = _normalize_code_token(m.group(0))
        if not val or len(val) < 3 or val in seen:
            continue
        if val.isdigit() and not (allow_numeric and _is_allowed_numeric_code(val)):
            continue
        if _looks_device_series(val):
            continue
        seen.add(val)
        found.append(val)
    return found


def _pick_best_codes(codes: Sequence[str], *, limit: int = 8) -> list[str]:
    ordered = sorted(codes, key=lambda c: (-_code_weight(c), codes.index(c)))
    out: list[str] = []
    seen: set[str] = set()
    for code in ordered:
        raw = _norm_spaces(code)
        if not raw:
            continue
        norm = _normalize_code_token(raw) if not raw.lower().startswith("canon ") else f"Canon {_normalize_code_token(raw.split(None, 1)[1])}"
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
        if len(out) >= limit:
            break
    return out


def _extract_codes(title: str, description: str) -> str:
    title = safe_str(title)
    description = safe_str(description)

    title_head, _title_tail = _split_title_body_parts(title)
    title_codes = _collect_codes_from_text(title_head or title, allow_numeric=True)
    title_codes.extend(_extract_title_bare_family_codes(title))
    title_codes.extend(_extract_xerox_developer_title_codes(title))
    title_codes.extend(_extract_title_multicode_tail(title))
    title_codes.extend(_extract_title_brand_alpha_tail(title))
    title_codes.extend(_extract_single_brand_numeric_tail(title))

    desc_head = _strip_compat_zone(description)
    desc_codes = _collect_codes_from_text(desc_head, allow_numeric=_is_consumable_title(title))

    strong_title_codes = [
        c
        for c in title_codes
        if _code_weight(c) >= 80
        or _is_allowed_numeric_code(c)
        or bool(re.fullmatch(r"Canon\s+[A-Z]{1,5}-?[A-Z0-9]{1,8}", _norm_spaces(c), re.I))
    ]
    codes = strong_title_codes or title_codes
    if not strong_title_codes:
        codes.extend(desc_codes)

    if not codes:
        return ""

    best = _pick_best_codes(codes)
    return ", ".join(best)


def _trim_compat_tail(value: str) -> str:
    value = _norm_spaces(value)
    if not value:
        return ""
    stop = STOP_HEADERS_RX.search(value)
    if stop:
        value = value[: stop.start()].strip()
    value = re.split(r"(?:\.|\n\n)", value, maxsplit=1)[0]
    value = re.sub(
        r"^(?:в\s+)?(?:многофункциональных|лазерных|струйных|факсимильных)?\s*"
        r"(?:принтерах|мфу|устройствах|аппаратах|факсах)\s+",
        "",
        value,
        flags=re.I,
    )
    value = value.strip(" ,.;:-")
    return value[:320]


def _extract_compat_from_desc(description: str) -> str:
    d = safe_str(description)
    if not d:
        return ""
    d = _norm_spaces(d)
    for rx in COMPAT_PATTERNS:
        m = rx.search(d)
        if not m:
            continue
        val = _trim_compat_tail(m.group(1))
        if val:
            return val
    return ""


def _extract_cable_params_from_text(title: str, description: str) -> list[Tuple[str, str]]:
    text = _norm_spaces(f"{safe_str(title)} {safe_str(description)}")
    out: list[Tuple[str, str]] = []

    m = CABLE_TYPE_RX.search(text)
    if m:
        out.append(("Тип кабеля", m.group(1).upper()))

    m = CABLE_CATEGORY_RX.search(text)
    if m:
        out.append(("Категория", f"Cat.{m.group(1)}"))

    m = CABLE_DIM_RX.search(text)
    if m:
        out.append(("Количество пар", m.group(1)))
        out.append(("Толщина проводников", m.group(2).replace('.', ',')))

    m = CABLE_MATERIAL_RX.search(text)
    if m:
        out.append(("Материал изоляции", m.group(1).upper()))

    m = CABLE_SPOOL_RX.search(text)
    if m:
        out.append(("Бухта", f"{m.group(1)} м/б"))

    return out


def _normalize_param_block(block: Sequence[Tuple[str, str]] | None) -> list[Tuple[str, str]]:
    out: list[Tuple[str, str]] = []
    for key, value in block or []:
        k = safe_str(key)
        v = safe_str(value)
        if not k or not v:
            continue
        out.append((k, v))
    return out


def _merge_raw_param_channels(
    *,
    page_params: Sequence[Tuple[str, str]] | None = None,
    raw_desc_pairs: Sequence[Tuple[str, str]] | None = None,
    raw_table_params: Sequence[Tuple[str, str]] | None = None,
) -> list[Tuple[str, str]]:
    """Собрать сырьевые param-каналы в одном месте без потери backward-safe совместимости.

    Приоритет каналов задаётся порядком:
    1) raw_table_params — более структурный источник;
    2) raw_desc_pairs — пары, поднятые из body;
    3) page_params — legacy-канал, если builder/source ещё не разведены полностью.
    """
    merged: list[Tuple[str, str]] = []
    merged.extend(_normalize_param_block(raw_table_params))
    merged.extend(_normalize_param_block(raw_desc_pairs))
    merged.extend(_normalize_param_block(page_params))
    return merged


def extract_page_params(
    *,
    title: str,
    description: str = "",
    extract_desc: str | None = None,
    page_params: Sequence[Tuple[str, str]] | None = None,
    raw_desc_pairs: Sequence[Tuple[str, str]] | None = None,
    raw_table_params: Sequence[Tuple[str, str]] | None = None,
) -> List[Tuple[str, str]]:
    """Нормализовать page params и поднять supplier-полезные значения.

    Поддерживает два режима: 
    - legacy: title + description + page_params;
    - новый: title + extract_desc + raw_desc_pairs + raw_table_params.
    """
    text_body = safe_str(extract_desc) or safe_str(description)
    merged_page_params = _merge_raw_param_channels(
        page_params=page_params,
        raw_desc_pairs=raw_desc_pairs,
        raw_table_params=raw_table_params,
    )

    out: list[Tuple[str, str]] = []

    kind = _title_kind(title)
    if kind:
        out.append(("Тип", kind))

    for key, value in merged_page_params:
        k = safe_str(key).casefold()
        v = safe_str(value)
        if not k or not v:
            continue
        norm_key = KEY_MAP.get(k, "")
        if not norm_key:
            continue
        if norm_key == "Цвет":
            v = _norm_color(v)
        elif kind == "Кабель сетевой" and norm_key in CABLE_PARAM_KEYS:
            v = _norm_spaces(v)
        out.append((norm_key, v))

    if kind == "Кабель сетевой":
        out.extend(_extract_cable_params_from_text(title, text_body))

    compat = _extract_compat_from_desc(text_body)
    if not compat and kind == "Чернила":
        compat = _extract_ink_title_compat(title) or _extract_riso_title_compat(title) or _extract_epson_desc_compat(title, text_body)
    if not compat:
        compat = _extract_panasonic_integral_compat(text_body)
    if compat:
        out.append(("Совместимость", compat))

    codes = _extract_codes(title, text_body)
    if codes:
        out.append(("Коды расходников", codes))

    title_low = safe_str(title).lower()
    if "yellow" in title_low and not any(k == "Цвет" for k, _ in out):
        out.append(("Цвет", "Желтый"))
    if "magenta" in title_low and not any(k == "Цвет" for k, _ in out):
        out.append(("Цвет", "Пурпурный"))
    if "black" in title_low and not any(k == "Цвет" for k, _ in out):
        out.append(("Цвет", "Чёрный"))

    return _dedupe_params(out)


# Public aliases for fill-missing layer / future cleanup
trim_compat_tail = _trim_compat_tail
extract_compat_from_text = _extract_compat_from_desc
extract_codes_from_text = _extract_codes
norm_spaces = _norm_spaces


__all__ = [
    "CODE_RX",
    "COMPAT_PATTERNS",
    "STOP_HEADERS_RX",
    "COMPAT_GUARD_RX",
    "CABLE_TYPE_RX",
    "CABLE_CATEGORY_RX",
    "CABLE_DIM_RX",
    "CABLE_MATERIAL_RX",
    "CABLE_SPOOL_RX",
    "safe_str",
    "norm_spaces",
    "trim_compat_tail",
    "extract_compat_from_text",
    "extract_codes_from_text",
    # backward-safe private exports until desc_extract is narrowed
    "_norm_spaces",
    "_trim_compat_tail",
    "_extract_compat_from_desc",
    "_extract_codes",
    "extract_page_params",
]
