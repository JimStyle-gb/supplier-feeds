# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/builder.py

VTT builder layer.
v4:
- cleans supplier SEO tails from title before RAW/core;
- removes logistics/internal params from feed;
- keeps type inference title-first;
- keeps price/photo logic untouched.
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
RES_IN_TITLE_RE = re.compile(r"(?<!\d)(\d+(?:[.,]\d+)?)\s*([kк]|ml|мл|l|л)\b", re.I)
TITLE_TAIL_RE = re.compile(
    r"\s*,?\s*(?:купить|цена|в\s+компании\s+втт|в\s+компании\s+vtt).*$",
    re.I,
)


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
    }
    return mapping.get(low, v)


def _clean_title(title: str) -> str:
    title = _norm_ws(title)
    title = TITLE_TAIL_RE.sub("", title).strip(" ,.-")
    return _norm_ws(title)


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


def _first_code(text: str) -> str:
    for code in CODE_TOKEN_RE.findall(text or ""):
        code = code.strip(".-/")
        if len(code) >= 3 and re.search(r"\d", code):
            return code
    return ""


def _extract_resource(title: str, params: Sequence[tuple[str, str]], desc: str) -> str:
    for key, value in params:
        if _s(key).casefold() == "ресурс" and _norm_ws(value):
            return _norm_ws(value)
    hay = " | ".join([title, desc])
    m = RES_IN_TITLE_RE.search(hay)
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


def _extract_compat(title: str, vendor: str, params: Sequence[tuple[str, str]], desc: str) -> str:
    for key, value in params:
        k = _s(key).casefold()
        if any(x in k for x in ("совмест", "для устройств", "для принтеров", "подходит")):
            val = _norm_ws(value)
            if val:
                return val
    m = re.search(
        r"\bдля\s+(.+?)(?:(?:,\s*(?:\d+(?:[.,]\d+)?\s*[kк]|black|cyan|magenta|yellow|grey|gray|red|blue|photo ?black|matt?e?black|ч[её]рн|ж[её]лт|син|голуб|малинов|пурпур|сер|крас))|$)",
        _norm_ws(title),
        re.I,
    )
    if not m:
        return ""
    compat = _norm_ws(m.group(1).strip(" ,"))
    if vendor and compat and not compat.upper().startswith(vendor.upper()):
        compat = f"{vendor} {compat}"
    return compat


def _should_keep_code(code: str) -> bool:
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
    return True


def _collect_codes(raw: dict, params: Sequence[tuple[str, str]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def add(val: str) -> None:
        for part in re.split(r"\s*,\s*", _s(val)):
            code = part.strip().strip(".-/")
            if not _should_keep_code(code):
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
    for prefix, normalized in checks:
        if low.startswith(prefix):
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


def _build_native_desc(title: str, type_name: str, model: str, compat: str, resource: str, color: str, desc_body: str) -> str:
    parts: list[str] = []
    if type_name:
        parts.append(f"Тип: {type_name}")
    if model:
        parts.append(f"Модель: {model}")
    if compat:
        parts.append(f"Совместимость: {compat}")
    if resource:
        parts.append(f"Ресурс: {resource}")
    if color:
        parts.append(f"Цвет: {color}")
    head = "; ".join(parts)
    body = _norm_ws(desc_body)
    if body and body.casefold() != title.casefold():
        return f"{head}. {body}" if head else body
    return head or title


def _merge_params(raw: dict, vendor: str, type_name: str, tech: str, codes: list[str], title: str, compat: str, resource: str) -> list[tuple[str, str]]:
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
            resource = resource or _norm_ws(value)
        add(key, value)

    if compat:
        add("Совместимость", compat)
    if resource:
        add("Ресурс", resource)
    if codes:
        add("Коды расходников", ", ".join(codes))

    model = ""
    for key, value in raw_params:
        if key in ("Каталожный номер", "OEM-номер", "Партс-номер", "Партномер") and value:
            model = _norm_ws(value)
            break
    if not model and codes:
        model = codes[0]
    if model:
        add("Модель", model)

    if not color_found:
        inferred_color = _infer_color_from_title(title)
        if inferred_color:
            add("Цвет", inferred_color)

    src_cats = [c for c in (raw.get("source_categories") or []) if _s(c)]
    if src_cats:
        add("Категория VTT", ", ".join(src_cats))

    return out


def build_offer_from_raw(raw: dict, *, id_prefix: str = "VT") -> OfferOut | None:
    title = _clean_title(_norm_ws(raw.get("name")))
    if not title:
        return None

    sku = _s(raw.get("sku"))
    source_categories = list(raw.get("source_categories") or ([] if not _s(raw.get("category_code")) else [_s(raw.get("category_code"))]))
    vendor = _guess_vendor(_s(raw.get("vendor")), title, raw.get("params") or [])
    type_name = _infer_type(source_categories, title)
    tech = _infer_tech(source_categories, type_name, title)
    compat = _extract_compat(title, vendor, raw.get("params") or [], _s(raw.get("description_body")))
    resource = _extract_resource(title, raw.get("params") or [], _s(raw.get("description_body")))
    codes = _collect_codes(raw, raw.get("params") or [])
    params = _merge_params(raw, vendor, type_name, tech, codes, title, compat, resource)

    raw_price = int(raw.get("price_rub_raw") or 0)
    price = compute_price(raw_price)

    pictures = [_s(x) for x in (raw.get("pictures") or []) if _s(x)]
    if not pictures:
        pictures = ["https://placehold.co/800x800/png?text=No+Photo"]

    model = ""
    color = ""
    for k, v in params:
        if k == "Модель" and not model:
            model = _norm_ws(v)
        if k == "Цвет" and not color:
            color = _norm_color(v)

    desc = _build_native_desc(
        title=title,
        type_name=type_name,
        model=model,
        compat=compat,
        resource=resource,
        color=color,
        desc_body=_s(raw.get("description_body") or raw.get("description_meta")),
    )

    oid = _mk_oid(sku, title)
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
