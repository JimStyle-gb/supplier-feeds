# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/builder.py

VTT builder layer.
v11:
- keeps RAW params clean before core;
- removes duplicate leading/trailing codes from title;
- treats (O)/(О)/OEM as original marker;
- appends "(оригинал)" to name once;
- removes duplicated trailing part numbers from title;
- cleans compatibility from original marker, part number, color/resource/volume tails and trailing device-code noise;
- keeps only useful product params in output;
- avoids putting device model codes from compatibility into "Коды расходников";
- normalizes resource values like 10000 -> 10K;
- keeps compatibility model rows like T920/T1500 and MF421dw/... without over-cutting;
- removes trailing supplier SKU from title where it is just an internal tail;
- fixes duplicate raw "Ресурс" param;
- keeps Canon/HP device model rows in compatibility without collapsing to vendor only;
- cleans Epson/InkTec compatibility tails like color / volume / orig.fasovka;
- removes trailing alt-partnumber tails from title where they are just supplier noise;
- does not change price/photo logic.
"""

from __future__ import annotations

import re
from typing import Sequence

from cs.core import OfferOut, compute_price


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
    "TNR_WASTETON": "Контейнер для отработанного тонера",
    "DEV_DEV": "Девелопер",
    "TNR_REFILL": "Тонер",
    "INK_COMMON": "Чернила",
    "PARTSPRINT_DEVUN": "Блок проявки",
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

SKIP_PARAM_KEYS = {
    "Артикул",
    "Штрих-код",
    "Вендор",
    "Категория",
    "Подкатегория",
    "В упаковке, штук",
    "Местный склад, штук",
    "Местный, до новой поставки, дней",
    "Склад Москва, штук",
    "Москва, до новой поставки, дней",
    "Категория VTT",
}

CODE_SOURCE_KEYS = {
    "Каталожный номер",
    "OEM-номер",
    "Партс-номер",
    "Партномер",
    "Аналоги",
}

VENDOR_HINTS = (
    "HP", "Canon", "Xerox", "Brother", "Kyocera", "Samsung", "Epson", "Ricoh",
    "Konica Minolta", "Pantum", "Lexmark", "Oki", "Sharp", "Panasonic",
    "Toshiba", "Develop", "Gestetner", "RISO",
)

CODE_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9\-./]{2,}\b")
RES_RE = re.compile(r"(?<!\d)(\d+(?:[.,]\d+)?)\s*([kк]|ml|мл|l|л)\b", re.I)
TITLE_TAIL_RE = re.compile(r"\s*,?\s*(?:купить|цена|в\s+компании\s+втт|в\s+компании\s+vtt).*$", re.I)
SERVICE_DESC_RE = re.compile(
    r"(?:^|[.;,\n ])(?:Артикул|Штрих-?код|Вендор|Категория|Подкатегория|В упаковке, штук|"
    r"Местный склад, штук|Местный, до новой поставки, дней|Склад Москва, штук|"
    r"Москва, до новой поставки, дней)\s*[:\-][^.;\n]*",
    re.I,
)
ORIGINAL_MARK_RE = re.compile(r"(?<!\w)\((?:O|О|OEM)\)(?!\w)|\bоригинал(?:ьн(?:ый|ая|ое|ые))?\b", re.I)
TRAIL_PART_RE = re.compile(r"(?:,?\s*[A-Z0-9][A-Z0-9\-./]{2,})+$", re.I)
COLOR_TAIL_RE = re.compile(
    r"(?:,?\s*(?:black|photo\s*black|photoblack|matte\s*black|matt\s*black|"
    r"cyan|yellow|magenta|grey|gray|red|blue|"
    r"черн(?:ый|ая|ое)?|чёрн(?:ый|ая|ое)?|"
    r"голуб(?:ой|ая|ое)?|син(?:ий|яя|ее)?|"
    r"желт(?:ый|ая|ое)?|жёлт(?:ый|ая|ое)?|"
    r"пурпурн(?:ый|ая|ое)?|малинов(?:ый|ая|ое)?|"
    r"сер(?:ый|ая|ое)?|красн(?:ый|ая|ое)?))+$",
    re.I,
)
VOLUME_TAIL_RE = re.compile(r"(?:,?\s*\d+(?:[.,]\d+)?\s*(?:мл|ml|л|l))+$", re.I)
RESOURCE_TAIL_RE = re.compile(r"(?:,?\s*\d+(?:[.,]\d+)?\s*[KКkк])+$", re.I)
DUPLICATE_LEAD_RE = re.compile(r"^([A-Z0-9][A-Z0-9\-./]{2,})\s*,\s*\1\b", re.I)
TRAIL_DUP_PART_RE = re.compile(r"(?:,?\s*[A-Z0-9][A-Z0-9\-./]{2,})+$", re.I)


def _s(x: object) -> str:
    return str(x).strip() if x is not None else ""


def _norm_ws(text: str) -> str:
    return " ".join(_s(text).replace("\xa0", " ").split()).strip()


def _canon_vendor(vendor: str) -> str:
    v = _norm_ws(vendor)
    low = v.casefold()
    mapping = {
        "kyocera-mita": "Kyocera",
        "kyocera mita": "Kyocera",
        "konica-minolta": "Konica Minolta",
        "konica minolta": "Konica Minolta",
        "hewlett-packard": "HP",
        "hewlett packard": "HP",
    }
    return mapping.get(low, v)


def _first_code(text: str) -> str:
    for code in CODE_TOKEN_RE.findall(text or ""):
        code = code.strip(".-/")
        if len(code) >= 3 and re.search(r"\d", code):
            return code
    return ""


def _is_original(*parts: str) -> bool:
    return any(ORIGINAL_MARK_RE.search(_s(x)) for x in parts if _s(x))



def _clean_title(title: str) -> str:
    title = _norm_ws(title)
    title = TITLE_TAIL_RE.sub("", title).strip(" ,.-")
    title = ORIGINAL_MARK_RE.sub("", title)
    title = _norm_ws(title).strip(" ,.-")
    while True:
        new_title = DUPLICATE_LEAD_RE.sub(r"\1", title).strip(" ,")
        if new_title == title:
            break
        title = new_title
    m = re.match(r"^(.+?)(?:,\s*([A-Z0-9][A-Z0-9\-./]{2,}))$", title, re.I)
    if m:
        head, tail = m.group(1), m.group(2)
        if _first_code(head).casefold() == tail.casefold():
            title = head
    return _norm_ws(title)

def _append_original_suffix(title: str, is_original: bool) -> str:
    title = _norm_ws(title)
    if not is_original:
        return title
    if "оригинал" in title.casefold():
        return title
    return f"{title} (оригинал)"


def _mk_oid(sku: str, title: str) -> str:
    base = _s(sku) or _first_code(title) or re.sub(r"[^A-Za-z0-9]+", "", title)[:28]
    base = re.sub(r"[^A-Za-z0-9._/-]+", "", base)
    return "VT" + base


def _guess_vendor(raw_vendor: str, title: str, params: Sequence[tuple[str, str]]) -> str:
    vendor = _canon_vendor(raw_vendor)
    if vendor:
        return vendor
    for k, v in params:
        key = _s(k).lower()
        val = _canon_vendor(_norm_ws(v))
        if any(x in key for x in ("бренд", "vendor", "марка", "производ")) and val:
            return val
    upper = f" {title.upper()} "
    for vendor in VENDOR_HINTS:
        if f" {vendor.upper()} " in upper:
            return _canon_vendor(vendor)
    return ""


def _format_resource_value(value: str) -> str:
    val = _norm_ws(value).replace(" ", "")
    m = re.fullmatch(r"(\d+)(?:[.,](\d+))?", val)
    if m:
        whole = int(m.group(1))
        frac = m.group(2) or ""
        number = float(f"{m.group(1)}.{frac}") if frac else float(whole)
        if number >= 1000:
            k = number / 1000.0
            if abs(k - round(k)) < 1e-9:
                return f"{int(round(k))}K"
            s = f"{k:.1f}".replace(".", ",").rstrip("0").rstrip(",")
            return f"{s}K"
        return val

    m = re.fullmatch(r"(\d+(?:[.,]\d+)?)\s*([kк]|ml|мл|l|л)", _norm_ws(value), re.I)
    if not m:
        return _norm_ws(value)
    num = m.group(1)
    unit = m.group(2)
    if unit.casefold() in {"k", "к"}:
        return f"{num}K"
    if unit.casefold() in {"ml", "мл"}:
        return f"{num} мл"
    if unit.casefold() in {"l", "л"}:
        return f"{num} л"
    return _norm_ws(value)


def _extract_resource(title: str, params: Sequence[tuple[str, str]], desc: str) -> str:
    for key, value in params:
        if _s(key).casefold() == "ресурс" and _norm_ws(value):
            return _format_resource_value(_norm_ws(value))
    hay = " | ".join([title, desc])
    m = RES_RE.search(hay)
    if not m:
        return ""
    unit = m.group(2)
    if unit.casefold() in {"k", "к"}:
        return f"{m.group(1)}K"
    if unit.casefold() in {"ml", "мл"}:
        return f"{m.group(1)} мл"
    if unit.casefold() in {"l", "л"}:
        return f"{m.group(1)} л"
    return ""

def _cleanup_compat(value: str, vendor: str, part_number: str = "", sku: str = "") -> str:
    compat = _norm_ws(value).strip(" ,.;/")
    if not compat:
        return ""

    compat = ORIGINAL_MARK_RE.sub("", compat)
    compat = ORIG_PACK_RE.sub("", compat).strip(" ,.;/")

    changed = True
    while changed and compat:
        before = compat

        if part_number:
            compat = re.sub(rf"(?<!\w){re.escape(part_number)}(?!\w)", "", compat, flags=re.I).strip(" ,.;/")
        if sku:
            compat = re.sub(rf"(?<!\w){re.escape(sku)}(?!\w)", "", compat, flags=re.I).strip(" ,.;/")

        # Объем/ресурс с пробелами после запятой тоже режем.
        compat = re.sub(r"(?:,?\s*\d+(?:[.,]\s*\d+)?\s*(?:мл|ml|л|l))\s*$", "", compat, flags=re.I).strip(" ,.;/")
        compat = re.sub(r"(?:,?\s*\d+(?:[.,]\s*\d+)?\s*[KКkк])\s*$", "", compat, flags=re.I).strip(" ,.;/")

        # Цветовые хвосты, включая короткие обозначения для чернил.
        compat = re.sub(
            r"(?:,?\s*(?:black|photo\s*black|photoblack|matte\s*black|matt\s*black|"
            r"cyan|yellow|magenta|grey|gray|red|blue|light\s*cyan|light\s*magenta|"
            r"bk|c|m|y|cl|ml|lc|lm|"
            r"черн(?:ый|ая|ое)?|чёрн(?:ый|ая|ое)?|"
            r"голуб(?:ой|ая|ое)?|син(?:ий|яя|ее)?|"
            r"желт(?:ый|ая|ое)?|жёлт(?:ый|ая|ое)?|"
            r"пурпурн(?:ый|ая|ое)?|малинов(?:ый|ая|ое)?|"
            r"сер(?:ый|ая|ое)?|красн(?:ый|ая|ое)?))\s*$",
            "",
            compat,
            flags=re.I,
        ).strip(" ,.;/")

        # Убираем только хвостовой part-like код в верхнем регистре/цифрах.
        # Device-модели Canon/HP/Epson с нижним регистром не трогаем.
        compat = re.sub(r"(?:,|\s)+[A-Z0-9-]*\d[A-Z0-9-]*\s*$", "", compat).strip(" ,.;/")
        compat = ALT_PART_TAIL_RE.sub("", compat).strip(" ,.;/")

        compat = re.sub(r"\s*,\s*", ", ", compat)
        compat = re.sub(r"\s*/\s*", "/", compat)
        compat = re.sub(r"\s{2,}", " ", compat).strip(" ,.;/")
        changed = compat != before

    if vendor and compat and not compat.upper().startswith(vendor.upper()):
        compat = f"{vendor} {compat}"
    return _norm_ws(compat)



def _extract_part_number(raw: dict, params: Sequence[tuple[str, str]], title: str) -> str:
    for key, value in params:
        if _s(key) in ("Партномер", "Партс-номер", "Каталожный номер", "OEM-номер") and _norm_ws(value):
            return _norm_ws(value)
    sku = _s(raw.get("sku"))
    if sku:
        return sku
    return _first_code(title)


def _extract_compat(title: str, vendor: str, params: Sequence[tuple[str, str]], desc: str, part_number: str, sku: str = "") -> str:
    # 1) Явный supplier-param приоритетнее.
    for key, value in params:
        k = _s(key).casefold()
        if any(x in k for x in ("совмест", "для устройств", "для принтеров", "подходит")):
            val = _cleanup_compat(_s(value), vendor, part_number, sku)
            if val:
                return val

    # 2) Из названия берем только участок после "для".
    clean_title = _norm_ws(title)
    m = re.search(r"\bдля\s+(.+)$", clean_title, re.I)
    if m:
        tail = _norm_ws(m.group(1))
        tail = _cleanup_compat(tail, vendor, part_number, sku)
        if tail:
            return tail

    # 3) Fallback по описанию.
    if desc:
        m = re.search(r"(?:совместим(?:ость|ые)?|подходит для|для принтеров|для устройств)\s*[:\-]?\s*([^.;\n]+)", desc, re.I)
        if m:
            compat = _cleanup_compat(m.group(1), vendor, part_number, sku)
            if compat:
                return compat
    return ""



def _should_keep_code(code: str, resource: str = "") -> bool:
    code = code.strip(".-/")
    if len(code) < 3:
        return False
    if not re.search(r"\d", code):
        return False
    if "/" in code:
        return False
    if re.fullmatch(r"\d+(?:[.,]\d+)?", code):
        return False
    if re.fullmatch(r"\d+(?:[.,]\d+)?[kкmlл]+", code, re.I):
        return False
    if resource:
        res_norm = resource.replace(" ", "").replace("мл", "ml").replace("л", "l").casefold()
        code_norm = code.replace(" ", "").casefold()
        if code_norm == res_norm:
            return False
    return True



def _collect_codes(raw: dict, params: Sequence[tuple[str, str]], resource: str, part_number: str, compat: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    compat_low = _norm_ws(compat).casefold()

    def add(val: str) -> None:
        for part in re.split(r"\s*,\s*", _s(val)):
            code = part.strip().strip(".-/")
            if not _should_keep_code(code, resource):
                continue
            if part_number and code.casefold() == part_number.casefold():
                continue
            if compat_low and re.search(rf"(?<!\w){re.escape(code.casefold())}(?!\w)", compat_low):
                continue
            if code not in seen:
                seen.add(code)
                out.append(code)

    sku = _s(raw.get("sku"))
    if sku:
        add(sku)
    for key, value in params:
        if _s(key) in CODE_SOURCE_KEYS:
            add(_s(value))
    for code in raw.get("title_codes") or []:
        add(_s(code))
    return out

def _infer_type_by_title(title: str) -> str:
    low = title.casefold()
    checks = [
        ("тонер-картридж", "Тонер-картридж"),
        ("копи-картридж", "Копи-картридж"),
        ("принт-картридж", "Принт-картридж"),
        ("драм-картридж", "Драм-картридж"),
        ("драм-юниты", "Драм-юнит"),
        ("драм-юнит", "Драм-юнит"),
        ("контейнер для отработанного тонера", "Контейнер для отработанного тонера"),
        ("контейнер", "Контейнер для отработанного тонера"),
        ("блок проявки", "Блок проявки"),
        ("бункер", "Бункер отработанного тонера"),
        ("фотобарабан", "Фотобарабан"),
        ("барабан", "Барабан"),
        ("девелопер", "Девелопер"),
        ("печатающая головка", "Печатающая головка"),
        ("головка печатающая", "Печатающая головка"),
        ("головка", "Печатающая головка"),
        ("чернила", "Чернила"),
        ("тонер", "Тонер"),
        ("носитель", "Носитель девелопера"),
        ("картриджи", "Картридж"),
        ("картридж", "Картридж"),
    ]
    for needle, normalized in checks:
        if low.startswith(needle) or f" {needle}" in low:
            return normalized
    return ""


def _infer_type(category_codes: Sequence[str], title: str) -> str:
    title_type = _infer_type_by_title(title)
    if title_type:
        return title_type
    for code in category_codes:
        t = CATEGORY_TYPE_MAP.get(_s(code))
        if t:
            return t
    return ""


def _infer_tech(category_codes: Sequence[str], type_name: str, title: str) -> str:
    for code in category_codes:
        t = TECH_BY_CATEGORY.get(_s(code))
        if t:
            return t
    low = f"{type_name} {title}".casefold()
    if "стру" in low or "чернил" in low or "головк" in low:
        return "Струйная"
    if "матрич" in low:
        return "Матричная"
    if any(x in low for x in ("картридж", "драм", "девелопер", "тонер", "барабан", "фотобарабан", "блок проявки")):
        return "Лазерная"
    return ""


def _norm_color(value: str) -> str:
    val = _norm_ws(value)
    low = val.casefold().replace("-", " ").replace("_", " ")
    mapping = {
        "black": "Черный",
        "photo black": "Черный",
        "photoblack": "Черный",
        "matte black": "Черный",
        "matt black": "Черный",
        "matteblack": "Черный",
        "mattblack": "Черный",
        "черный": "Черный",
        "чёрный": "Черный",
        "bk": "Черный",
        "cyan": "Голубой",
        "синий": "Голубой",
        "голубой": "Голубой",
        "c": "Голубой",
        "yellow": "Желтый",
        "желтый": "Желтый",
        "жёлтый": "Желтый",
        "y": "Желтый",
        "magenta": "Пурпурный",
        "малиновый": "Пурпурный",
        "пурпурный": "Пурпурный",
        "m": "Пурпурный",
        "grey": "Серый",
        "gray": "Серый",
        "серый": "Серый",
        "red": "Красный",
        "красный": "Красный",
    }
    return mapping.get(low, val[:1].upper() + val[1:] if val else val)


def _infer_color_from_title(title: str) -> str:
    low = title.casefold().replace("-", " ")
    checks = [
        ("photo black", "Черный"),
        ("photoblack", "Черный"),
        ("matte black", "Черный"),
        ("matt black", "Черный"),
        ("matteblack", "Черный"),
        ("mattblack", "Черный"),
        (" black ", "Черный"),
        ("чёрный", "Черный"),
        ("черный", "Черный"),
        (" bk", "Черный"),
        (" cyan", "Голубой"),
        ("синий", "Голубой"),
        ("голубой", "Голубой"),
        (" c,", "Голубой"),
        (" yellow", "Желтый"),
        ("жёлтый", "Желтый"),
        ("желтый", "Желтый"),
        (" y,", "Желтый"),
        (" magenta", "Пурпурный"),
        ("малиновый", "Пурпурный"),
        ("пурпурный", "Пурпурный"),
        (" m,", "Пурпурный"),
        (" grey", "Серый"),
        (" gray", "Серый"),
        ("серый", "Серый"),
        (" red", "Красный"),
        ("красный", "Красный"),
    ]
    for needle, value in checks:
        if needle in low:
            return value
    return ""


def _clean_desc_body(desc_body: str) -> str:
    body = _norm_ws(desc_body)
    if not body:
        return ""
    body = SERVICE_DESC_RE.sub(" ", body)
    body = ORIGINAL_MARK_RE.sub("", body)
    body = re.sub(r"\s{2,}", " ", body).strip(" ,.;")
    return _norm_ws(body)


def _build_native_desc(title: str, type_name: str, part_number: str, compat: str, resource: str, color: str, is_original: bool, desc_body: str) -> str:
    parts: list[str] = []
    if type_name:
        parts.append(f"Тип: {type_name}")
    if part_number:
        parts.append(f"Партномер: {part_number}")
    if compat:
        parts.append(f"Совместимость: {compat}")
    if resource:
        parts.append(f"Ресурс: {resource}")
    if color:
        parts.append(f"Цвет: {color}")
    if is_original:
        parts.append("Оригинальность: Оригинал")
    head = "; ".join(parts)
    body = _clean_desc_body(desc_body)
    if body and body.casefold() != title.casefold():
        return f"{head}. {body}" if head else body
    return head or title


def _merge_params(raw: dict, vendor: str, type_name: str, tech: str, part_number: str, codes: list[str], title: str, compat: str, resource: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    color_found = ""

    def add(k: str, v: str) -> None:
        key = _norm_ws(k)
        val = _norm_ws(v)
        if not key or not val:
            return
        sig = (key.casefold(), val.casefold())
        if sig in seen:
            return
        seen.add(sig)
        out.append((key, val))

    raw_params = [(_s(k), _s(v)) for (k, v) in (raw.get("params") or [])]

    if type_name:
        add("Тип", type_name)
    if tech:
        add("Технология печати", tech)
    if vendor and type_name and any(x in type_name.casefold() for x in ("картридж", "драм", "девелопер", "чернила", "тонер", "головка", "блок", "барабан", "контейнер", "носитель")):
        add("Для бренда", vendor)

    for key, value in raw_params:
        if key in SKIP_PARAM_KEYS or key in CODE_SOURCE_KEYS:
            continue
        if key == "Цвет":
            value = _norm_color(value)
            color_found = value or color_found
        if key.casefold() == "ресурс":
            resource = resource or _format_resource_value(_norm_ws(value))
            continue
        if key in {"Модель", "Партномер"}:
            continue
        add(key, value)

    if part_number:
        add("Партномер", part_number)
    if compat:
        add("Совместимость", compat)
    if resource:
        add("Ресурс", resource)
    if codes:
        add("Коды расходников", ", ".join(codes))

    if not color_found:
        inferred_color = _infer_color_from_title(title)
        if inferred_color:
            add("Цвет", inferred_color)

    return out



def build_offer_from_raw(raw: dict, *, id_prefix: str = "VT") -> OfferOut | None:
    original_flag = _is_original(_s(raw.get("name")), _s(raw.get("description_body")), _s(raw.get("description_meta")))
    clean_title = _clean_title(_norm_ws(raw.get("name")))
    title = _append_original_suffix(clean_title, original_flag)
    if not title:
        return None

    sku = _s(raw.get("sku"))
    source_categories = list(raw.get("source_categories") or ([] if not _s(raw.get("category_code")) else [_s(raw.get("category_code"))]))
    vendor = _guess_vendor(_s(raw.get("vendor")), clean_title, raw.get("params") or [])
    type_name = _infer_type(source_categories, clean_title)
    tech = _infer_tech(source_categories, type_name, clean_title)
    part_number = _extract_part_number(raw, raw.get("params") or [], clean_title)

    if part_number:
        title_no_suffix = re.sub(r"\s*\(оригинал\)$", "", title, flags=re.I).strip(" ,")
        title_no_suffix = re.sub(rf"(?:,?\s*{re.escape(part_number)})+$", "", title_no_suffix, flags=re.I).strip(" ,")
        if sku:
            title_no_suffix = re.sub(rf"(?:,?\s*{re.escape(sku)})+$", "", title_no_suffix, flags=re.I).strip(" ,")
        title_no_suffix = ALT_PART_TAIL_RE.sub("", title_no_suffix).strip(" ,")
        title_no_suffix = DUPLICATE_LEAD_RE.sub(r"\1", title_no_suffix).strip(" ,")
        title = _append_original_suffix(_norm_ws(title_no_suffix), original_flag)

    compat = _extract_compat(clean_title, vendor, raw.get("params") or [], _s(raw.get("description_body")), part_number, sku)
    resource = _extract_resource(clean_title, raw.get("params") or [], _s(raw.get("description_body")))
    codes = _collect_codes(raw, raw.get("params") or [], resource, part_number, compat)
    params = _merge_params(raw, vendor, type_name, tech, part_number, codes, clean_title, compat, resource)

    raw_price = int(raw.get("price_rub_raw") or 0)
    price = compute_price(raw_price)

    pictures = [_s(x) for x in (raw.get("pictures") or []) if _s(x)]
    if not pictures:
        pictures = ["https://placehold.co/800x800/png?text=No+Photo"]

    color = ""
    for k, v in params:
        if k == "Цвет" and not color:
            color = _norm_color(v)

    desc = _build_native_desc(
        title=title,
        type_name=type_name,
        part_number=part_number,
        compat=compat,
        resource=resource,
        color=color,
        is_original=original_flag,
        desc_body=_s(raw.get("description_body") or raw.get("description_meta")),
    )

    oid = _mk_oid(sku, clean_title)
    if id_prefix and not oid.startswith(id_prefix):
        oid = id_prefix + oid.lstrip()

    return OfferOut(
        oid=oid,
        available=True,
        name=title,
        price=price,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=desc,
    )

