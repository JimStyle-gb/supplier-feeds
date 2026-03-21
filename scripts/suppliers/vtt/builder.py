# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/builder.py

VTT builder layer.

Роль:
- нормализовать raw VTT карточку
- собрать clean RAW OfferOut
- не тащить shared-логику в supplier adapter
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
}
CODE_SOURCE_KEYS = {
    "Каталожный номер",
    "OEM-номер",
    "Партс-номер",
    "Партномер",
    "Аналоги",
}
COLOR_MAP = {
    "black": "Черный",
    "cyan": "Голубой",
    "yellow": "Желтый",
    "magenta": "Пурпурный",
    "photo black": "Черный",
    "matte black": "Черный",
}
VENDOR_HINTS = (
    "HP", "Canon", "Xerox", "Brother", "Kyocera", "Samsung", "Epson", "Ricoh",
    "Konica Minolta", "Pantum", "Lexmark", "Oki", "Sharp", "Panasonic",
    "Toshiba", "Develop", "Gestetner", "RISO",
)
CODE_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9\-./]{2,}\b")


def _s(x: object) -> str:
    return str(x).strip() if x is not None else ""


def _norm_ws(text: str) -> str:
    return " ".join(_s(text).replace("\xa0", " ").split()).strip()


def _mk_oid(sku: str, title: str) -> str:
    base = _s(sku) or _first_code(title) or re.sub(r"[^A-Za-z0-9]+", "", title)[:28]
    base = re.sub(r"[^A-Za-z0-9._/-]+", "", base)
    return "VT" + base


def _guess_vendor(raw_vendor: str, title: str, params: Sequence[tuple[str, str]]) -> str:
    vendor = _norm_ws(raw_vendor)
    if vendor:
        return vendor
    for k, v in params:
        key = _s(k).lower()
        val = _norm_ws(v)
        if any(x in key for x in ("бренд", "vendor", "марка", "производ")) and val:
            return val
    upper = f" {title.upper()} "
    for vendor in VENDOR_HINTS:
        if f" {vendor.upper()} " in upper:
            return vendor
    return ""


def _first_code(text: str) -> str:
    for code in CODE_TOKEN_RE.findall(text or ""):
        code = code.strip(".-/")
        if len(code) >= 3 and re.search(r"\d", code):
            return code
    return ""


def _collect_codes(raw: dict, params: Sequence[tuple[str, str]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def add(val: str) -> None:
        for part in re.split(r"\s*,\s*", _s(val)):
            code = part.strip().strip(".-/")
            if len(code) < 3:
                continue
            if not re.search(r"\d", code):
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


def _infer_type(category_codes: Sequence[str], title: str) -> str:
    for code in category_codes:
        t = CATEGORY_TYPE_MAP.get(_s(code))
        if t:
            return t
    low = title.casefold()
    if low.startswith("тонер-картридж"):
        return "Тонер-картридж"
    if low.startswith("картридж"):
        return "Картридж"
    if low.startswith("чернила"):
        return "Чернила"
    if low.startswith("девелопер"):
        return "Девелопер"
    if low.startswith("драм-юнит") or low.startswith("драм-юниты"):
        return "Драм-юнит"
    if low.startswith("драм-картридж"):
        return "Драм-картридж"
    return ""


def _infer_tech(category_codes: Sequence[str], type_name: str) -> str:
    for code in category_codes:
        t = TECH_BY_CATEGORY.get(_s(code))
        if t:
            return t
    low = type_name.casefold()
    if "стру" in low or "чернил" in low or "головк" in low:
        return "Струйная"
    if "матрич" in low:
        return "Матричная"
    if "картридж" in low or "драм" in low or "девелопер" in low or "тонер" in low:
        return "Лазерная"
    return ""


def _norm_color(value: str) -> str:
    val = _norm_ws(value)
    low = val.casefold()
    return COLOR_MAP.get(low, val)


def _clean_desc(raw: dict, title: str) -> str:
    parts: list[str] = []
    meta = _norm_ws(raw.get("description_meta"))
    body = _norm_ws(raw.get("description_body"))

    if meta and meta.casefold() != title.casefold():
        parts.append(meta)

    if body:
        # убираем шумовые логистические хвосты
        body = re.sub(r"\b(?:Местный склад|Склад Москва|В упаковке, штук|до новой поставки)[^|]{0,200}", " ", body, flags=re.I)
        body = re.sub(r"\s{2,}", " ", body).strip()
        if body and body.casefold() != title.casefold():
            parts.append(body)

    desc = "\n".join(x for x in parts if x)
    return desc or title


def _merge_params(raw: dict, vendor: str, type_name: str, tech: str, codes: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

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

    raw_params = [( _s(k), _s(v) ) for (k, v) in (raw.get("params") or [])]

    if type_name:
        add("Тип", type_name)
    if tech:
        add("Технология печати", tech)
    if vendor and type_name and any(x in type_name.casefold() for x in ("картридж", "драм", "девелопер", "чернила", "тонер", "головка", "блок")):
        add("Для бренда", vendor)

    for key, value in raw_params:
        if key in SKIP_PARAM_KEYS:
            continue
        if key in CODE_SOURCE_KEYS:
            continue
        if key == "Цвет":
            value = _norm_color(value)
        add(key, value)

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

    # category diagnostics держим как supplier raw param
    src_cats = [c for c in (raw.get("source_categories") or []) if _s(c)]
    if src_cats:
        add("Категория VTT", ", ".join(src_cats))

    return out


def build_offer_from_raw(raw: dict, *, id_prefix: str = "VT") -> OfferOut | None:
    title = _norm_ws(raw.get("name"))
    if not title:
        return None

    sku = _s(raw.get("sku"))
    source_categories = list(raw.get("source_categories") or ([] if not _s(raw.get("category_code")) else [_s(raw.get("category_code"))]))
    vendor = _guess_vendor(_s(raw.get("vendor")), title, raw.get("params") or [])
    type_name = _infer_type(source_categories, title)
    tech = _infer_tech(source_categories, type_name)
    codes = _collect_codes(raw, raw.get("params") or [])
    params = _merge_params(raw, vendor, type_name, tech, codes)

    raw_price = int(raw.get("price_rub_raw") or 0)
    price = compute_price(raw_price)

    pictures = [ _s(x) for x in (raw.get("pictures") or []) if _s(x) ]
    if not pictures:
        pictures = ["https://placehold.co/800x800/png?text=No+Photo"]

    desc = _clean_desc(raw, title)
    oid = _mk_oid(sku, title)
    if id_prefix and not oid.startswith(id_prefix):
        oid = id_prefix + oid.lstrip()

    return OfferOut(
        oid=oid,
        available=True,  # по проектному правилу VT всегда true
        name=title,
        price=price,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=desc,
    )
