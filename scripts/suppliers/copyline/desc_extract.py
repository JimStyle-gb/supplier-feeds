# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/desc_extract.py
CopyLine description-extract layer.

Задача:
- поднимать missing params из body-description;
- only-fill-missing;
- не тянуть device-list в Коды расходников.
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
    r"Категория|Назначение|Материал\s+изоляции|Бухта)\b",
    re.I,
)

COMPAT_GUARD_RX = re.compile(
    r"(?:совместимость\s+с\s+устройствами|используется\s+в|для\s+принтеров|для\s+устройств|"
    r"для\s+аппаратов|применяется\s+в|подходит\s+для|совместим\s+с)",
    re.I,
)

TECH_PAIR_HEADERS = {
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
    "назначение": "Назначение",
    "материал изоляции": "Материал изоляции",
    "бухта": "Бухта",
}

CABLE_KEYS = {
    "Тип кабеля",
    "Количество пар",
    "Толщина проводников",
    "Категория",
    "Назначение",
    "Материал изоляции",
    "Бухта",
}

CABLE_TYPE_RX = re.compile(r"\b(UTP|FTP|STP|SFTP|F/UTP|U/UTP|F/FTP|U/FTP)\b", re.I)
CABLE_CATEGORY_RX = re.compile(r"\bCat\.?\s*(5e|6a|6|7|7a|8)\b", re.I)
CABLE_DIM_RX = re.compile(r"\b(\d+)x\d+x\d+/([0-9]+(?:[.,][0-9]+)?)\b", re.I)
CABLE_MATERIAL_RX = re.compile(r"\b(LSZH|PVC|PE)\b", re.I)
CABLE_SPOOL_RX = re.compile(r"\b(\d+)\s*м/б\b", re.I)
CABLE_CONTEXT_RX = re.compile(r"(?:кабель\s+сетевой|витая\s+пара)", re.I)
TITLE_CABLE_RX = re.compile(r"^кабель\s+сетевой", re.I)

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


def _dedupe(items: Sequence[Tuple[str, str]]) -> list[Tuple[str, str]]:
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


def _is_cable_context(title: str, text: str) -> bool:
    title = safe_str(title)
    text = safe_str(text)
    if TITLE_CABLE_RX.search(title):
        return True
    if CABLE_CONTEXT_RX.search(text):
        return True
    return False


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

def _split_title_body_parts(title: str) -> tuple[str, str]:
    title = _norm_spaces(title)
    if not title:
        return "", ""
    m = re.search(r"\b(?:для\s+принтеров|для\s+МФУ|для\s+устройств|совместимость\s+с)\b", title, flags=re.I)
    if not m:
        return title, ""
    return title[: m.start()].strip(" ,;/"), title[m.start():].strip()


def _extract_riso_title_compat(title: str) -> str:
    title = _norm_spaces(title)
    if not title or not re.search(r"\bRISO\b", title, re.I):
        return ""
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

    for token in _extract_title_canon_numeric_codes(title):
        if token not in seen:
            seen.add(token)
            out.append(token)

    branded_tail_rx = re.compile(
        r"(?:^|/|[,(])\s*(Canon|Toshiba|Ricoh|Panasonic)\s+((?:[A-Z]?\d{3,6}[A-Z]?)(?:\s*/\s*[A-Z]?\d{3,6}[A-Z]?){0,5})\b",
        re.I,
    )
    for m in branded_tail_rx.finditer(title):
        brand = safe_str(m.group(1)).title()
        for part in re.split(r"\s*/\s*", safe_str(m.group(2))):
            token = _normalize_code_token(part)
            if not token:
                continue
            if token.isdigit() and brand.casefold() != "canon":
                continue
            if brand.casefold() == "canon" and re.fullmatch(r"\d{3,4}[A-Z]?", token, re.I):
                token = f"Canon {token}"
            if token not in seen:
                seen.add(token)
                out.append(token)
    return out

def _strip_compat_zone(text: str) -> str:
    text = _norm_spaces(text)
    if not text:
        return ""
    m = COMPAT_GUARD_RX.search(text)
    if m:
        return text[: m.start()].strip()
    return text


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
    return value[:400]


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


def _extract_compat(description: str) -> str:
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


def _extract_codes_from_text(text: str, *, allow_numeric: bool) -> list[str]:
    text = _normalize_code_search_text(text)
    found: list[str] = []
    seen: set[str] = set()
    for m in CODE_RX.finditer(text):
        code = _normalize_code_token(m.group(0))
        if not code or len(code) < 3 or code in seen:
            continue
        if code.isdigit() and not (allow_numeric and _is_allowed_numeric_code(code)):
            continue
        if _looks_device_series(code):
            continue
        seen.add(code)
        found.append(code)
    return found


def _pick_best_codes(codes: Sequence[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for code in codes:
        raw = _norm_spaces(code)
        if not raw:
            continue
        norm = _normalize_code_token(raw) if not raw.lower().startswith("canon ") else f"Canon {_normalize_code_token(raw.split(None, 1)[1])}"
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
        if len(out) >= 8:
            break
    return out


def _extract_codes(title: str, description: str) -> str:
    title = safe_str(title)
    description = safe_str(description)
    title_head, _title_tail = _split_title_body_parts(title)
    title_codes = _extract_codes_from_text(title_head or title, allow_numeric=True)
    title_codes.extend(_extract_title_bare_family_codes(title))
    title_codes.extend(_extract_xerox_developer_title_codes(title))
    title_codes.extend(_extract_title_multicode_tail(title))
    title_codes.extend(_extract_title_brand_alpha_tail(title))
    title_codes.extend(_extract_single_brand_numeric_tail(title))
    desc_head = _strip_compat_zone(description)
    desc_codes = _extract_codes_from_text(desc_head, allow_numeric=_is_consumable_title(title))

    strong_title_codes = [
        c
        for c in title_codes
        if c
        and (not c.isdigit() or _is_allowed_numeric_code(c))
        and not _looks_device_series(c if not safe_str(c).lower().startswith("canon ") else safe_str(c).split(None, 1)[1])
    ]
    codes = strong_title_codes or title_codes
    if not strong_title_codes:
        codes = desc_codes
    best = _pick_best_codes(codes)
    return ", ".join(best)


def _extract_inline_pair(line: str, *, is_cable: bool) -> tuple[str, str] | None:
    for sep in (":", " - "):
        if sep not in line:
            continue
        left, right = line.split(sep, 1)
        key = TECH_PAIR_HEADERS.get(safe_str(left).casefold(), "")
        value = _norm_spaces(right)
        if not key or not value or len(value) > 240:
            continue
        if key in CABLE_KEYS and not is_cable:
            continue
        return key, value
    return None


def _extract_cable_params_from_text(text: str, *, is_cable: bool) -> list[Tuple[str, str]]:
    if not is_cable:
        return []
    text = _norm_spaces(text)
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

    if "витая пара" in text.casefold():
        out.append(("Назначение", "Витая пара"))
    return out


def _extract_line_pairs(description: str, *, title: str) -> list[Tuple[str, str]]:
    lines = [safe_str(x) for x in re.split(r"\n+", description) if safe_str(x)]
    out: list[Tuple[str, str]] = []
    joined = " ".join(lines)
    is_cable = _is_cable_context(title, joined)

    for line in lines:
        pair = _extract_inline_pair(line, is_cable=is_cable)
        if pair:
            out.append(pair)

    for i in range(len(lines) - 1):
        k = lines[i].casefold()
        v = _norm_spaces(lines[i + 1])
        norm_key = TECH_PAIR_HEADERS.get(k, "")
        if not norm_key:
            continue
        if norm_key in CABLE_KEYS and not is_cable:
            continue
        if len(v) > 240:
            continue
        out.append((norm_key, v))

    out.extend(_extract_cable_params_from_text(joined, is_cable=is_cable))
    return out


def extract_desc_params(*, title: str, description: str, existing_params: Sequence[Tuple[str, str]] | None = None) -> List[Tuple[str, str]]:
    """Поднять missing params из body-description."""
    existing_params = existing_params or []
    existing_keys = {safe_str(k).casefold() for k, _ in existing_params if safe_str(k)}
    out: list[Tuple[str, str]] = []

    for k, v in _extract_line_pairs(description, title=title):
        if k.casefold() in existing_keys:
            continue
        out.append((k, v))

    compat = _extract_compat(description)
    if not compat and re.search(r"(?:Panasonic|INTEGRAL)", title + " " + description, re.I):
        m = re.search(r"(?:для|used in|совместим(?:ость)? с)\s+((?:Panasonic|INTEGRAL)[^.;\n]{3,180})", _norm_spaces(description), re.I)
        if m:
            compat = _trim_compat_tail(m.group(1))
    if compat and "совместимость" not in existing_keys:
        out.append(("Совместимость", compat))

    codes = _extract_codes(title, description)
    if codes and "коды расходников" not in existing_keys:
        out.append(("Коды расходников", codes))

    return _dedupe(out)
