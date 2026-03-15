# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/params_page.py
CopyLine page-params layer.

Задача:
- нормализовать page params из HTML-таблиц/описания;
- поднять supplier-specific поля до raw;
- не тянуть device-list в Коды расходников.
"""

from __future__ import annotations

import re
from typing import List, Sequence, Tuple


CODE_RX = re.compile(
    r"\b(?:"
    r"CF\d{3,4}[A-Z]?|CE\d{3,4}[A-Z]?|CB\d{3,4}[A-Z]?|CC\d{3,4}[A-Z]?|Q\d{4}[A-Z]?|W\d{4}[A-Z0-9]{1,4}|"
    r"106R\d{5}|006R\d{5}|108R\d{5}|113R\d{5}|013R\d{5}|016\d{6}|"
    r"TK-?\d{3,5}[A-Z0-9]*|MLT-[A-Z]\d{3,5}[A-Z0-9/]*|CLT-[A-Z]\d{3,5}[A-Z]?|"
    r"ML-D\d+[A-Z]?|ML-\d{4,5}[A-Z]\d?|T-\d{3,6}[A-Z]?|KX-FA\d+[A-Z]?|KX-FAT\d+[A-Z]?|"
    r"C-?EXV\d+[A-Z]*|DR-\d+[A-Z0-9-]*|TN-\d+[A-Z0-9-]*|"
    r"C13T\d{5,8}[A-Z0-9]*|C12C\d{5,8}[A-Z0-9]*|C33S\d{5,8}[A-Z0-9]*|"
    r"50F\d[0-9A-Z]{2,4}|55B\d[0-9A-Z]{2,4}|56F\d[0-9A-Z]{2,4}|0?71H"
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

CABLE_PARAM_KEYS = {
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


CODE_PREFIX_WEIGHTS = (
    (re.compile(r"^(?:CF|CE|CB|CC|Q|W)\d", re.I), 100),
    (re.compile(r"^(?:106R|006R|108R|113R|013R)\d", re.I), 100),
    (re.compile(r"^016\d{6}$", re.I), 95),
    (re.compile(r"^(?:MLT-|CLT-|TK-|KX-FA|KX-FAT|C-?EXV|DR-|TN-|C13T|C12C|C33S|T-)", re.I), 95),
    (re.compile(r"^ML-D\d", re.I), 90),
    (re.compile(r"^ML-\d{4,5}[A-Z]\d?$", re.I), 85),
    (re.compile(r"^(?:50F|55B|56F)\w+$", re.I), 90),
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
    "назначение": "Назначение",
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
    text = re.sub(r"\b(CLT|MLT|ML|KX|TK|TN|DR|T|C)\s*-\s*([A-Z0-9]{2,})\b", r"\1-\2", text, flags=re.I)
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
    code = _normalize_code_token(code)
    for rx, weight in CODE_PREFIX_WEIGHTS:
        if rx.search(code):
            return weight
    if _is_allowed_numeric_code(code):
        return 95
    return 10


def _extract_title_canon_numeric_codes(title: str) -> list[str]:
    title = _norm_spaces(title)
    out: list[str] = []
    seen: set[str] = set()
    for m in re.finditer(r"\bCanon\s+((?:\d{3}[A-Z]?)(?:\s*/\s*\d{3}[A-Z]?)+)\b", title, flags=re.I):
        for part in re.split(r"\s*/\s*", safe_str(m.group(1))):
            token = _normalize_code_token(part)
            if token and token not in seen:
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


def _pick_best_codes(codes: Sequence[str], *, limit: int = 6) -> list[str]:
    ordered = sorted(codes, key=lambda c: (-_code_weight(c), codes.index(c)))
    out: list[str] = []
    seen: set[str] = set()
    for code in ordered:
        norm = _normalize_code_token(code)
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
    title_codes = _collect_codes_from_text(title, allow_numeric=True)
    title_codes.extend(_extract_title_canon_numeric_codes(title))

    desc_head = _strip_compat_zone(description)
    desc_codes = _collect_codes_from_text(desc_head, allow_numeric=_is_consumable_title(title))

    # Если в title уже есть сильный код расходки, не тянем device-list из description.
    strong_title_codes = [c for c in title_codes if _code_weight(c) >= 80]
    codes = strong_title_codes or title_codes
    if not strong_title_codes:
        codes.extend(desc_codes)
    elif not codes:
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

    if "витая пара" in text.casefold():
        out.append(("Назначение", "Витая пара"))
    return out


def extract_page_params(
    *,
    title: str,
    description: str,
    page_params: Sequence[Tuple[str, str]] | None = None,
) -> List[Tuple[str, str]]:
    """Нормализовать page params и поднять supplier-полезные значения."""
    page_params = page_params or []
    out: list[Tuple[str, str]] = []

    kind = _title_kind(title)
    if kind:
        out.append(("Тип", kind))

    for key, value in page_params:
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
        out.extend(_extract_cable_params_from_text(title, description))

    compat = _extract_compat_from_desc(description)
    if compat:
        out.append(("Совместимость", compat))

    codes = _extract_codes(title, description)
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
