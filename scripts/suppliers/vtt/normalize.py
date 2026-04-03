# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/normalize.py

VTT normalize layer under CS-template.

Роль файла:
- только базовая supplier-нормализация;
- helper-ы для title / vendor / type / tech / resource / color;
- без source-crawl логики;
- без compat repair-логики;
- без final description/render.

Важно:
- VTT-specific совместимость и part-number cleanup живут в compat.py;
- extraction из HTML-страницы живёт в params.py;
- builder использует этот модуль как базовый normalization-layer.

Файл intentionally backward-safe:
- сохраняет старые public helper-ы, которые уже импортируют builder.py / compat.py /
  desc_extract.py / params_page.py / filtering.py / pictures.py;
- дополнительно даёт более канонические alias-функции под общий supplier-template.
"""

from __future__ import annotations

import re
from typing import Sequence

from cs.util import safe_int as _safe_int_shared


# ----------------------------- constants / regex -----------------------------

CATEGORY_TYPE_MAP: dict[str, str] = {
    "DRM_CRT": "Драм-картридж",
    "DRM_UNIT": "Драм-юнит",
    "CARTLAS_ORIG": "Картридж",
    "CARTLAS_COPY": "Копи-картридж",
    "CARTLAS_PRINT": "Принт-картридж",
    "CARTLAS_TNR": "Тонер-картридж",
    "CARTINJ_PRNTHD": "Печатающая головка",
    "CARTINJ_Refill": "Чернила",
    "CARTINJ_ORIG": "Картридж",
    "CARTMAT_CART": "Картридж",
    "TNR_WASTETON": "Контейнер",
    "DEV_DEV": "Девелопер",
    "TNR_REFILL": "Тонер",
    "INK_COMMON": "Чернила",
    "PARTSPRINT_DEVUN": "Блок",
}

TECH_BY_CATEGORY: dict[str, str] = {
    "DRM_CRT": "Лазерная",
    "DRM_UNIT": "Лазерная",
    "CARTLAS_ORIG": "Лазерная",
    "CARTLAS_COPY": "Лазерная",
    "CARTLAS_PRINT": "Лазерная",
    "CARTLAS_TNR": "Лазерная",
    "CARTINJ_PRNTHD": "Струйная",
    "CARTINJ_Refill": "Струйная",
    "CARTINJ_ORIG": "Струйная",
    "CARTMAT_CART": "Матричная",
    "TNR_WASTETON": "Лазерная",
    "DEV_DEV": "Лазерная",
    "TNR_REFILL": "Лазерная",
    "INK_COMMON": "Струйная",
    "PARTSPRINT_DEVUN": "Лазерная",
}

VENDOR_HINTS = (
    "HP", "Canon", "Xerox", "Brother", "Kyocera", "Samsung", "Epson", "Ricoh",
    "Konica Minolta", "Pantum", "Lexmark", "OKI", "Sharp", "Panasonic",
    "Toshiba", "Develop", "Gestetner", "RISO",
)

_CONSUMABLE_TITLE_PREFIXES = (
    "картридж",
    "картриджи",
    "тонер-картридж",
    "копи-картридж",
    "принт-картридж",
    "драм-картридж",
    "драм-юнит",
    "драм-юниты",
    "девелопер",
    "чернила",
    "печатающая головка",
    "контейнер",
    "барабан",
    "фотобарабан",
    "термоблок",
    "кабель сетевой",
    "тонер",
    "комплект",
    "набор",
    "рефил",
    "блок",
)

_TITLE_COLOR_MAP = {
    "yellow": "Желтый",
    "magenta": "Пурпурный",
    "black": "Чёрный",
    "cyan": "Голубой",
    "photo black": "Чёрный",
    "matte black": "Чёрный",
}

_CODE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bCF\d{3,4}[A-Z]?\b", re.I),
    re.compile(r"\bCE\d{3,4}[A-Z]?\b", re.I),
    re.compile(r"\bCB\d{3,4}[A-Z]?\b", re.I),
    re.compile(r"\bCC\d{3,4}[A-Z]?\b", re.I),
    re.compile(r"\bQ\d{4}[A-Z]?\b", re.I),
    re.compile(r"\bW\d{4}[A-Z0-9]{1,4}\b", re.I),
    re.compile(r"\bMLT-[A-Z]\d{3,5}[A-Z0-9/]*\b", re.I),
    re.compile(r"\bCLT-[A-Z]\d{3,5}[A-Z]?\b", re.I),
    re.compile(r"\bTK-?\d{3,5}[A-Z0-9]*\b", re.I),
    re.compile(r"\b106R\d{5}\b", re.I),
    re.compile(r"\b006R\d{5}\b", re.I),
    re.compile(r"\b108R\d{5}\b", re.I),
    re.compile(r"\b113R\d{5}\b", re.I),
    re.compile(r"\b013R\d{5}\b", re.I),
    re.compile(r"\b016\d{6}\b", re.I),
    re.compile(r"\bML-D\d+[A-Z]?\b", re.I),
    re.compile(r"\bML-\d{4,5}[A-Z]\d?\b", re.I),
    re.compile(r"\bKX-FA\d+[A-Z]?\b", re.I),
    re.compile(r"\bKX-FAT\d+[A-Z]?\b", re.I),
    re.compile(r"\bT-\d{3,6}[A-Z]?\b", re.I),
    re.compile(r"\bC13T\d{5,8}[A-Z0-9]*\b", re.I),
    re.compile(r"\bC12C\d{5,8}[A-Z0-9]*\b", re.I),
    re.compile(r"\bC33S\d{5,8}[A-Z0-9]*\b", re.I),
    re.compile(r"\bC-?EXV\d+[A-Z]*\b", re.I),
    re.compile(r"\bUAE-\d+[A-Z0-9-]*\b", re.I),
    re.compile(r"\bDR-\d+[A-Z0-9-]*\b", re.I),
    re.compile(r"\bTN-\d+[A-Z0-9-]*\b", re.I),
    re.compile(r"\b50F\d[0-9A-Z]{2,4}\b", re.I),
    re.compile(r"\b55B\d[0-9A-Z]{2,4}\b", re.I),
    re.compile(r"\b56F\d[0-9A-Z]{2,4}\b", re.I),
    re.compile(r"\b0?71H\b", re.I),
]

CODE_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9\-./]{2,}\b")
TITLE_TAIL_RE = re.compile(r"\s*,?\s*(?:купить|цена|в\s+компании\s+втт|в\s+компании\s+vtt).*$", re.I)
ORIGINAL_MARK_RE = re.compile(r"(?<!\w)\((?:O|О|OEM)\)(?!\w)|\bоригинал(?:ьн(?:ый|ая|ое|ые))?\b", re.I)
DUPLICATE_LEAD_RE = re.compile(r"^([A-Z0-9][A-Z0-9\-./]{2,})\s*,\s*\1\b", re.I)
RES_RE = re.compile(r"(?<!\d)(\d+(?:[.,]\d+)?)\s*([kк]|ml|мл|l|л)\b", re.I)
_MULTI_SPACE_RE = re.compile(r"\s+")
_VENDOR_SUFFIX_RE = re.compile(r"(?iu)\b(?:gmbh|co\.?ltd|inc\.?|corp\.?|limited|ltd\.?)\b")
_BOOL_TRUE = {"true", "1", "yes", "y", "да", "in stock", "available"}
_BOOL_FALSE = {"false", "0", "no", "n", "нет", "out of stock", "unavailable"}


# ----------------------------- basic helpers -----------------------------

def safe_str(x: object) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def norm_ws(text: str) -> str:
    """Мягкая нормализация пробелов."""
    return " ".join(safe_str(text).replace("\xa0", " ").split()).strip()


def _norm_spaces(text: str) -> str:
    """Мягкая нормализация пробелов и переводов строк без narrative-cleaning."""
    s = safe_str(text)
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\r\n?", "\n", s)
    s = re.sub(r"[ \t\f\v]+", " ", s)
    s = re.sub(r" *\n *", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _normalize_code_token(s: str) -> str:
    s = safe_str(s).upper()
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s*-\s*", "-", s)
    s = re.sub(r"\s+", "", s)
    return s


def _looks_numeric_sku(s: str) -> bool:
    return bool(re.fullmatch(r"\d+", safe_str(s)))


def _is_allowed_numeric_code(s: str) -> bool:
    return bool(re.fullmatch(r"016\d{6}", _normalize_code_token(s)))


def _looks_consumable_title(title: str) -> bool:
    t = safe_str(title).lower()
    return any(t.startswith(prefix) for prefix in _CONSUMABLE_TITLE_PREFIXES)


# ----------------------------- vendor / title -----------------------------

def canon_vendor(vendor: str) -> str:
    """Привести supplier/vendor token к канону."""
    v = norm_ws(vendor)
    if not v:
        return ""
    v = _VENDOR_SUFFIX_RE.sub("", v).strip(" ,.;")
    low = v.casefold()
    mapping = {
        "kyocera-mita": "Kyocera",
        "kyocera mita": "Kyocera",
        "konica-minolta": "Konica Minolta",
        "konica minolta": "Konica Minolta",
        "hewlett-packard": "HP",
        "hewlett packard": "HP",
        "hewlett packard enterprise": "HPE",
        "hp enterprise": "HPE",
    }
    return mapping.get(low, v)


def _first_vendor_from_text(texts: Sequence[str]) -> str:
    hay = "\n".join([safe_str(x) for x in texts if safe_str(x)])
    if not hay:
        return ""

    m = re.search(
        r"(?:^|\b)(?:для|for)\s+(HP|Canon|Xerox|Kyocera|Brother|Epson|Pantum|Ricoh|Lexmark|Samsung|OKI|RISO|Panasonic|Toshiba)\b",
        hay,
        flags=re.I,
    )
    if m:
        val = m.group(1)
        if val.upper() == "HP":
            return "HP"
        if val.upper() == "OKI":
            return "OKI"
        return canon_vendor(val)

    for vendor in VENDOR_HINTS:
        if re.search(rf"\b{re.escape(vendor)}\b", hay, flags=re.I):
            return canon_vendor(vendor)
    return ""


def guess_vendor(raw_vendor: str, title: str, params: Sequence[tuple[str, str]]) -> str:
    """Backward-safe vendor guess для builder."""
    vendor = canon_vendor(raw_vendor)
    if vendor:
        return vendor
    for k, v in params or []:
        key = safe_str(k).lower()
        val = canon_vendor(norm_ws(v))
        if any(x in key for x in ("бренд", "vendor", "марка", "производ", "для бренда")) and val:
            return val
    return _first_vendor_from_text([title])


def detect_vendor(*, title: str = "", description: str = "", params: Sequence[tuple[str, str]] | None = None) -> str:
    """Канонический alias для мягкого определения vendor."""
    params = params or []
    param_texts: list[str] = []
    for key, value in params:
        key2 = safe_str(key)
        value2 = safe_str(value)
        if not key2 or not value2:
            continue
        if key2.casefold() in {"производитель", "vendor", "brand", "для бренда"}:
            direct = _first_vendor_from_text([value2])
            if direct:
                return direct
        param_texts.append(value2)
    return _first_vendor_from_text([title, description, *param_texts])


def _localize_title_color_tokens(title: str) -> str:
    s = _norm_spaces(title)
    for en, ru in _TITLE_COLOR_MAP.items():
        s = re.sub(rf"(?<![A-Za-zА-Яа-яЁё]){en}(?![A-Za-zА-Яа-яЁё])", ru, s, flags=re.I)
    return s


def normalize_title(title: str) -> str:
    """Нормализовать title без supplier-side смысловой правки."""
    s = _localize_title_color_tokens(title)
    s = re.sub(r"\s{2,}", " ", s)
    return s[:240]


def clean_title(title: str) -> str:
    """Backward-safe cleanup title для builder."""
    title = normalize_title(title)
    title = TITLE_TAIL_RE.sub("", title).strip(" ,.-")
    title = ORIGINAL_MARK_RE.sub("", title)
    title = norm_ws(title).strip(" ,.-")
    while True:
        new_title = DUPLICATE_LEAD_RE.sub(r"\1", title).strip(" ,")
        if new_title == title:
            break
        title = new_title
    m = re.match(r"^(.+?)(?:,\s*([A-Z0-9][A-Z0-9\-./]{2,}))$", title, re.I)
    if m:
        head, tail = m.group(1), m.group(2)
        if first_code(head).casefold() == tail.casefold():
            title = head
    return norm_ws(title)


def normalize_name(name: str) -> str:
    """Канонический alias под общий supplier-template."""
    return normalize_title(name)


def append_original_suffix(title: str, original: bool) -> str:
    title = norm_ws(title)
    if not original:
        return title
    if "оригинал" in title.casefold():
        return title
    return f"{title} (оригинал)"


# ----------------------------- code / model / oid -----------------------------

def first_code(text: str) -> str:
    for code in CODE_TOKEN_RE.findall(text or ""):
        code = code.strip(".-/")
        if len(code) >= 3 and re.search(r"\d", code):
            return code
    return ""


def _search_code(text: str) -> str:
    hay = _norm_spaces(text)
    if not hay:
        return ""
    hay = re.sub(r"\b(113R|108R|106R|006R|013R|016|C13T|C12C|C33S)\s+(\d{4,8}[A-Z0-9]*)\b", r"\1\2", hay, flags=re.I)
    hay = re.sub(r"\b(CLT|MLT|KX|TK|TN|DR|T|C)\s*-\s*([A-Z0-9]{2,})\b", r"\1-\2", hay, flags=re.I)
    for rx in _CODE_PATTERNS:
        match = rx.search(hay)
        if match:
            return _normalize_code_token(match.group(0))
    return ""


def detect_model(*, title: str = "", description: str = "", sku: str = "") -> str:
    """Определить model/code по title → description head → sku."""
    model = _search_code(title)
    if model:
        return model

    head = _norm_spaces(description).split("\n", 1)[0] if _norm_spaces(description) else ""
    model = _search_code(head)
    if model:
        return model

    sku_norm = _normalize_code_token(sku)
    if not sku_norm:
        return ""
    if _looks_numeric_sku(sku_norm) and not _is_allowed_numeric_code(sku_norm):
        return ""
    if _looks_consumable_title(title):
        for rx in _CODE_PATTERNS:
            if rx.fullmatch(sku_norm):
                return sku_norm
        return ""
    if re.fullmatch(r"[A-Z0-9._/-]{3,40}", sku_norm) and (
        not _looks_numeric_sku(sku_norm) or _is_allowed_numeric_code(sku_norm)
    ):
        return sku_norm
    return ""


def make_oid(sku: str, title: str) -> str:
    base = safe_str(sku) or first_code(title) or re.sub(r"[^A-Za-z0-9]+", "", title)[:28]
    base = re.sub(r"[^A-Za-z0-9._/-]+", "", base)
    return "VT" + base


def build_offer_oid(raw_vendor_code: str, raw_id: str, *, prefix: str) -> str:
    """Канонический alias под общий supplier-template."""
    base = norm_ws(raw_vendor_code) or norm_ws(raw_id)
    if not base:
        return ""
    base = re.sub(r"[^A-Za-z0-9._/-]+", "", base)
    if not base:
        return ""
    if base.upper().startswith(prefix.upper()):
        return base
    return f"{prefix}{base}"


# ----------------------------- resource / color / original -----------------------------

def is_original(*parts: str) -> bool:
    return any(ORIGINAL_MARK_RE.search(safe_str(x)) for x in parts if safe_str(x))


def format_resource_value(value: str) -> str:
    s = norm_ws(value)
    if not s:
        return ""
    m = RES_RE.search(s)
    if m:
        num = m.group(1).replace(",", ".")
        unit = m.group(2).casefold()
        if unit in {"k", "к"}:
            # убираем .0, но сохраняем 0.6K / 2.4K и т.п.
            if num.endswith(".0"):
                num = num[:-2]
            return f"{num}K".replace(".", ",") if num.startswith("0.") else f"{num}K"
        if unit in {"ml", "мл"}:
            if num.endswith(".0"):
                num = num[:-2]
            return f"{num} мл".replace(".", ",")
        if unit in {"l", "л"}:
            if num.endswith(".0"):
                num = num[:-2]
            return f"{num} л".replace(".", ",")
    return s


def infer_color_from_title(title: str) -> str:
    t = safe_str(title).lower()
    if re.search(r"\b(black|ч[её]рн\w*)\b", t):
        return "Чёрный"
    if re.search(r"\b(yellow|ж[её]лт\w*)\b", t):
        return "Желтый"
    if re.search(r"\b(magenta|пурпурн\w*|малинов\w*)\b", t):
        return "Пурпурный"
    if re.search(r"\b(cyan|голуб\w*|син\w*)\b", t):
        return "Голубой"
    if re.search(r"\b(gray|grey|сер\w*)\b", t):
        return "Серый"
    if re.search(r"\b(red|красн\w*)\b", t):
        return "Красный"
    return ""


def norm_color(value: str) -> str:
    """Нормализовать цвет supplier-layer без тяжёлой магии."""
    return infer_color_from_title(value) or norm_ws(value)


# ----------------------------- type / tech -----------------------------

def infer_type(source_categories: Sequence[str], title: str) -> str:
    for code in source_categories or []:
        if code in CATEGORY_TYPE_MAP:
            return CATEGORY_TYPE_MAP[code]

    t = safe_str(title).lower()
    for prefix in _CONSUMABLE_TITLE_PREFIXES:
        if t.startswith(prefix):
            # аккуратно canonicalize вариации
            if prefix in {"драм-юниты", "драм юнит"}:
                return "Драм-юнит"
            if prefix == "контейнер":
                return "Контейнер"
            if prefix == "рефил":
                return "Заправочный комплект"
            return prefix.capitalize() if prefix.isascii() else prefix[:1].upper() + prefix[1:]
    return ""


def infer_tech(source_categories: Sequence[str], type_name: str, title: str) -> str:
    for code in source_categories or []:
        if code in TECH_BY_CATEGORY:
            return TECH_BY_CATEGORY[code]

    t = " ".join([safe_str(type_name), safe_str(title)]).lower()
    if any(x in t for x in ("чернил", "струй", "ink", "printhead", "печатающая головка")):
        return "Струйная"
    if any(x in t for x in ("матрич",)):
        return "Матричная"
    return "Лазерная" if t else ""


# ----------------------------- extraction-body helpers -----------------------------

def _drop_title_echo_from_desc(title: str, description: str) -> str:
    desc = _norm_spaces(description)
    if not desc:
        return ""
    lines = [ln.strip() for ln in desc.split("\n")]
    if not lines:
        return desc
    first = safe_str(lines[0])
    norm_title = normalize_title(title)
    if first and norm_title and first.casefold() == norm_title.casefold():
        lines = lines[1:]
    return "\n".join([ln for ln in lines if safe_str(ln)]).strip()


def build_extract_description(*, title: str, description_text: str) -> str:
    s = _drop_title_echo_from_desc(title, description_text)
    s = _norm_spaces(s)
    if not s:
        return ""
    return s[:6000]


def normalize_source_basics(
    *,
    title: str,
    sku: str,
    description_text: str,
    params: Sequence[tuple[str, str]] | None = None,
) -> dict:
    """Вернуть базовую нормализацию для builder/source."""
    norm_title = normalize_title(title)
    extract_desc = build_extract_description(title=norm_title, description_text=description_text)
    vendor = detect_vendor(title=norm_title, description=extract_desc or description_text, params=params)
    model = detect_model(title=norm_title, description=extract_desc or description_text, sku=sku)
    return {
        "title": norm_title,
        "vendor": vendor,
        "model": model,
        "extract_desc": extract_desc,
        "description": extract_desc,
    }


# ----------------------------- common-template aliases -----------------------------

def normalize_vendor(
    vendor: str,
    *,
    name: str = "",
    params: Sequence[tuple[str, str]] | None = None,
    description_text: str = "",
    vendor_blacklist: set[str] | None = None,
    fallback_vendor: str = "",
) -> str:
    """Канонический alias под общий supplier-template."""
    vendor_blacklist = vendor_blacklist or set()
    direct = canon_vendor(vendor)
    if direct and direct.casefold() not in vendor_blacklist:
        return direct

    guessed = detect_vendor(title=name, description=description_text, params=params or [])
    if guessed and guessed.casefold() not in vendor_blacklist:
        return guessed

    fallback = canon_vendor(fallback_vendor)
    if fallback and fallback.casefold() not in vendor_blacklist:
        return fallback
    return ""


def normalize_model(name: str, params: Sequence[tuple[str, str]] | None = None, *, description_text: str = "", sku: str = "") -> str:
    """Канонический alias под общий supplier-template."""
    return detect_model(title=name, description=description_text, sku=sku)


def normalize_price_in(price_text: str = "", *, fallback_text: str = "") -> int | None:
    """Мягкая нормализация входной цены."""
    price_in = _safe_int_shared(price_text)
    if price_in is not None:
        return price_in
    return _safe_int_shared(fallback_text)


def normalize_available(available_attr: str = "", available_tag: str = "", active: str = "") -> bool:
    """Общий bool-normalizer; для VTT policy всё равно always_true_available=true."""
    for raw in (available_attr, available_tag, active):
        s = norm_ws(raw).casefold()
        if s in _BOOL_TRUE:
            return True
        if s in _BOOL_FALSE:
            return False
    return False


__all__ = [
    "CATEGORY_TYPE_MAP",
    "TECH_BY_CATEGORY",
    "VENDOR_HINTS",
    "CODE_TOKEN_RE",
    "TITLE_TAIL_RE",
    "ORIGINAL_MARK_RE",
    "DUPLICATE_LEAD_RE",
    "RES_RE",
    "safe_str",
    "norm_ws",
    "canon_vendor",
    "first_code",
    "is_original",
    "clean_title",
    "normalize_title",
    "normalize_name",
    "append_original_suffix",
    "make_oid",
    "build_offer_oid",
    "guess_vendor",
    "detect_vendor",
    "detect_model",
    "normalize_vendor",
    "normalize_model",
    "normalize_price_in",
    "normalize_available",
    "infer_type",
    "infer_tech",
    "infer_color_from_title",
    "norm_color",
    "format_resource_value",
    "build_extract_description",
    "normalize_source_basics",
]
