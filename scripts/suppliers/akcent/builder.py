# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/builder.py

AkCent supplier layer — сборка raw OfferOut.

Что делает:
- принимает уже отфильтрованные source-offers AkCent;
- нормализует базовые поля (oid / vendor / model / available / price_in / warranty);
- определяет kind строго по prefix-based схеме AkCent;
- собирает родные XML params;
- чистит description и аккуратно добирает недостающие desc params;
- применяет supplier-side cleanup для расходки (codes / compat / device);
- собирает чистый raw OfferOut для core.

Важно:
- core остаётся общим и не получает AkCent-specific логики;
- builder работает adapter-first;
- логика построена backward-safe: умеет брать данные и из dataclass-объектов, и из dict, и из offer_el.
"""

from __future__ import annotations

from collections import Counter
import re
import xml.etree.ElementTree as ET
from typing import Any, Iterable

from cs.core import OfferOut
from cs.util import norm_ws
from suppliers.akcent.compat import (
    extract_codes_from_text as compat_extract_codes_from_text,
    extract_consumable_device_candidate as compat_extract_consumable_device_candidate,
    extract_direct_epson_device_list as compat_extract_direct_epson_device_list,
    extract_explicit_epson_devices as compat_extract_explicit_epson_devices,
    extract_models_from_text as compat_extract_models_from_text,
    looks_generic_device_value as compat_looks_generic_device_value,
    normalize_consumable_device_params as compat_normalize_consumable_device_params,
    normalize_epson_device_list as compat_normalize_epson_device_list,
    pick_name_primary_code as compat_pick_name_primary_code,
    reconcile_params,
    should_force_consumable_model as compat_should_force_consumable_model,
)
from suppliers.akcent.desc_clean import clean_description_text, strip_name_prefix_from_desc as desc_strip_name_prefix_from_desc
from suppliers.akcent.desc_extract import extract_desc_params
from suppliers.akcent.normalize import (
    finalize_consumable_name as norm_finalize_consumable_name,
    finalize_waste_tank_name as norm_finalize_waste_tank_name,
    normalize_consumable_name as norm_normalize_consumable_name,
    normalize_source_basics,
)
from suppliers.akcent.params_xml import collect_xml_params, detect_kind_by_name, resolve_allowed_keys
from suppliers.akcent.pictures import collect_picture_urls


_RE_WS = re.compile(r"\s+")


_RE_DROP_CONSUMABLE_DESC_LINE = re.compile(
    r"(?iu)\b(?:поддерживаемые\s+модели(?:\s+принтеров|\s+устройств|\s+техники)?|"
    r"совместимые\s+модели(?:\s+техники)?|совместимые\s+продукты(?:\s+для)?|для)\s*:"
)



_RE_INLINE_SUPPLIER_HEADER = re.compile(
    r"(?iu)^(?:основные\s+преимущества|общие\s+характеристики|общие\s+характерстики)\s*:\s*"
)


def _param_value(params: list[tuple[str, str]], key: str) -> str:
    key_cf = _clean_text(key).casefold().replace("ё", "е")
    for k, v in params or []:
        k_cf = _clean_text(k).casefold().replace("ё", "е")
        if k_cf == key_cf:
            return _clean_text(v)
    return ""


def _original_consumable_prefix(subject: str) -> str:
    low = _clean_text(subject).casefold().replace("ё", "е")
    if "емкость" in low or "ёмкость" in low:
        return "Оригинальная"
    if low == "чернила":
        return "Оригинальные"
    return "Оригинальный"


def _color_phrase(color_value: str) -> str:
    color = _clean_text(color_value)
    if not color:
        return ""
    low = color.casefold().replace("ё", "е")
    if low.endswith(("ый", "ий", "ой")):
        return f"{color[:-2]}ого цвета"
    if low.endswith("ая"):
        return f"{color[:-2]}ой цвета"
    if low.endswith("ое"):
        return f"{color[:-2]}ого цвета"
    return f"{color} цвета"


def _build_consumable_short_desc(params: list[tuple[str, str]]) -> str:
    type_value = _clean_text(_param_value(params, "Тип") or "Расходный материал")
    brand_value = _clean_text(
        _param_value(params, "Для бренда")
        or _param_value(params, "Бренд")
        or _param_value(params, "Производитель")
    )
    model_value = _clean_text(_param_value(params, "Модель"))
    codes_value = _clean_text(_param_value(params, "Коды"))
    color_value = _clean_text(_param_value(params, "Цвет"))
    resource_value = _clean_text(_param_value(params, "Ресурс"))
    device_value = _clean_text(_param_value(params, "Для устройства") or _param_value(params, "Совместимость"))

    subject = type_value or "Расходный материал"
    prefix = brand_value or ""

    code_hint = ""
    if model_value and codes_value and model_value in codes_value:
        code_hint = model_value
    elif model_value:
        code_hint = model_value
    elif codes_value:
        code_hint = codes_value.split('/')[0].strip()

    parts = []
    original_prefix = _original_consumable_prefix(subject)
    if prefix and code_hint:
        parts.append(f"{original_prefix} {subject.lower()} {prefix} {code_hint}")
    elif prefix:
        parts.append(f"{original_prefix} {subject.lower()} {prefix}")
    elif code_hint:
        parts.append(f"{subject} {code_hint}")
    else:
        parts.append(subject)

    color_phrase = _color_phrase(color_value)
    if color_phrase:
        parts[-1] += f" {color_phrase}"

    if device_value:
        parts[-1] += f" для {device_value}"

    if resource_value:
        parts[-1] += f". Ресурс: {resource_value}"

    return _clean_text(parts[-1]).strip(". ") + "."


def _normalize_consumable_device_value(value: str) -> str:
    src = _clean_text(value)
    if not src:
        return ""
    src = re.sub(r"(?iu)\bMAINTENANCE\s+BOX\b", "Maintenance Box", src)
    src = re.sub(r"(?iu)\bULTRACHROME\b", "UltraChrome", src)
    parts = re.split(r"(?iu)\s*(?:;|,|\n|/)\s*", src)
    cleaned = [norm_ws(x) for x in parts]
    cleaned = [x for x in cleaned if x]
    out: list[str] = []
    seen: set[str] = set()
    for item in cleaned:
        key = item.casefold().replace("ё", "е")
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return " / ".join(out)


def _drop_consumable_device_narrative(clean_desc: str, params: list[tuple[str, str]], *, kind: str) -> str:
    text = _clean_text(clean_desc)
    if kind != "consumable" or not text:
        return text
    device_value = _normalize_consumable_device_value(_param_value(params, "Для устройства") or _param_value(params, "Совместимость"))
    if not device_value:
        return text
    text = re.sub(r"(?iu)^\s*(?:поддерживаемые|совместимые)\s+модели\s*:?\s*", "", text)
    text = text.strip(" .;,-")
    if text.casefold().replace("ё", "е") == device_value.casefold().replace("ё", "е"):
        return ""
    return text


def _looks_wrong_ink_color_text(text: str, params: list[tuple[str, str]]) -> bool:
    color = _cf(_param_value(params, "Цвет"))
    typ = _cf(_param_value(params, "Тип"))
    low = _cf(text)
    if "чернила" not in typ and "картридж" not in typ:
        return False
    if not color or not low:
        return False

    def has_any(markers: list[str]) -> bool:
        return any(m in low for m in markers)

    if has_any(["черн", "чёрн"]) and not any(x in color for x in ["черн", "чёрн"]):
        return True
    if has_any(["желт", "жёлт"]) and not any(x in color for x in ["желт", "жёлт"]):
        return True
    if has_any(["пурпур", "magenta"]) and not any(x in color for x in ["пурпур", "magenta"]):
        return True
    if has_any(["cyan", "циан", "голуб"]) and not any(x in color for x in ["cyan", "циан", "голуб"]):
        return True
    return False


def _soften_consumable_body(clean_desc: str, params: list[tuple[str, str]], *, kind: str) -> str:
    text = _drop_consumable_device_narrative(clean_desc, params, kind=kind)
    text = _clean_text(text)
    if not text:
        return text
    if kind != "consumable":
        return text
    text = _RE_INLINE_SUPPLIER_HEADER.sub(" ", text)
    text = re.sub(r"(?iu)\s*[;|]\s*", ". ", text)
    text = _clean_text(text)
    if not text:
        return _build_consumable_short_desc(params).strip()
    low = text.casefold().replace("ё", "е")
    if any(mark in low for mark in [
        'вид струй', 'назначение', 'цвет печати', 'поддерживаемые модели',
        'совместимые модели', 'совместимые продукты', 'ресурс '
    ]):
        return _build_consumable_short_desc(params).strip()
    if re.fullmatch(r'(?iu)(?:емкость|ёмкость)\s+для\s+отработанных\s+чернил(?:\s+[A-Z0-9-]+)?', text):
        return _build_consumable_short_desc(params).strip()
    if re.fullmatch(r'(?iu)чернила(?:\s+[A-Z0-9-]+)?', text):
        return _build_consumable_short_desc(params).strip()
    if _looks_wrong_ink_color_text(text, params):
        return _build_consumable_short_desc(params).strip()
    return text


def _tail_after_model(name: str, model: str) -> str:
    s = _clean_text(name)
    m = _clean_text(model)
    if not s or not m:
        return ""
    pat = re.compile(r"(?iu)^.*?\b" + re.escape(m) + r"\b")
    tail = pat.sub("", s, count=1).strip(" ,;-–—")
    tail = _clean_text(tail)
    tail = re.sub(r"(?iu)\bMAINTENANCE\s+BOX\b", "Maintenance Box", tail)
    tail = re.sub(r"(?u)\bТ(?=\d)", "T", tail)
    return tail


def _waste_tank_generic_second_sentence() -> str:
    return "Контейнер предназначен для сбора отработанных чернил и заменяется после уведомления принтера."


def _device_sentence_from_params(params: list[tuple[str, str]]) -> str:
    device_value = _normalize_consumable_device_value(
        _param_value(params, "Для устройства") or _param_value(params, "Совместимость")
    )
    if not device_value:
        return ""
    return _clean_text(f"Подходит для устройств: {device_value}.")


def _finalize_waste_tank_desc(desc: str, name: str, params: list[tuple[str, str]]) -> str:
    text = _clean_text(desc)
    typ = _param_value(params, "Тип")
    brand = _param_value(params, "Для бренда")
    model = _param_value(params, "Модель")

    if _clean_text(typ).casefold().replace("ё", "е") != _clean_text("Ёмкость для отработанных чернил").casefold().replace("ё", "е"):
        return text

    tail = _tail_after_model(name, model)
    base = _clean_text(f"Оригинальная ёмкость для отработанных чернил {brand} {model}")
    lead = _clean_text(f"{base} для {tail}.") if tail else _clean_text(base + ".")
    device_sentence = _device_sentence_from_params(params)
    generic_second = _waste_tank_generic_second_sentence()

    text = re.sub(r"(?iu)^технические\s+характеристики\s*", "", text).strip(" .;,-")
    text = re.sub(r"(?iu)^описание\s*[:.-]?\s*", "", text).strip(" .;,-")
    text = re.sub(r"(?iu)^емкость\s+для\s+отработанных\s+чернил\s+для\s*:?\s*", "", text).strip(" .;,-")
    text = re.sub(r"(?iu)^сменная\s+емкость\s+для\s+отработанных\s+чернил\.?\s*", "", text).strip(" .;,-")
    text = re.sub(r"(?iu)\bсменная\s+емкость\s+для\s+отработанных\s+чернил\.?\s*", "", text).strip(" .;,-")
    text = re.sub(
        r"(?iu)^информация\s+о\s+необходимости\s+замены\s+появится\s+на\s+панели\s+управлени[ея]\s+принтера\.?\s*",
        generic_second,
        text,
    ).strip(" .;,-")
    text = re.sub(
        r"(?iu)\bинформация\s+о\s+необходимости\s+замены\s+появится\s+на\s+панели\s+управлени[ея]\s+принтера\.?\s*",
        " " + generic_second,
        text,
    ).strip()

    generic_patterns = [
        r"(?iu)^(?:сменная\s+)?(?:емкость|ёмкость)\s+для\s+отработанных\s+чернил\.?$",
        r"(?iu)^оригинальная\s+(?:емкость|ёмкость)\s+для\s+отработанных\s+чернил(?:\s+[A-Z0-9-]+)?\.?$",
    ]

    if (not text) or any(re.fullmatch(p, text) for p in generic_patterns):
        parts = [lead, generic_second]
        if device_sentence and tail and len(device_sentence) > 80:
            parts.insert(1, device_sentence)
        return _clean_text(" ".join(parts))

    low = text.casefold().replace("ё", "е")
    base_low = base.casefold().replace("ё", "е")
    text_low = text.casefold().replace("ё", "е")
    generic_second_low = generic_second.casefold().replace("ё", "е")
    tail_low = _clean_text(tail).casefold().replace("ё", "е")
    device_low = _clean_text(device_sentence).casefold().replace("ё", "е")

    if text_low.rstrip(".") == base_low.rstrip("."):
        parts = [lead, generic_second]
        if device_sentence and tail and len(device_sentence) > 80:
            parts.insert(1, device_sentence)
        return _clean_text(" ".join(parts))

    if device_sentence and (
        "surecolor" in low
        or "workforce" in low
        or "sc-" in low
        or "wf-" in low
        or "et-" in low
        or "для:" in low
    ):
        return _clean_text(f"{lead} {device_sentence} {generic_second}")

    if len(text) < 180:
        if text and not text.endswith("."):
            text += "."
        if generic_second_low in text.casefold().replace("ё", "е"):
            return _clean_text(f"{lead} {generic_second}")
        if tail_low and tail_low in text_low:
            return _clean_text(f"{lead} {generic_second}")
        if device_low and device_low in text_low:
            return _clean_text(f"{lead} {generic_second}")
        if text.casefold().replace("ё", "е").startswith(base_low):
            return _clean_text(f"{lead} {generic_second}")
        return _clean_text(f"{lead} {text}")

    if text and not text.endswith("."):
        text += "."
    return text

def _infer_consumable_type(name: str, desc: str, current_type: str) -> str:
    low = _cf(" ".join([name, desc, current_type]))
    name_cf = _cf(name)
    if "емкость для отработанных чернил" in low or "ёмкость для отработанных чернил" in low:
        return "Ёмкость для отработанных чернил"
    if "экономичный набор" in low:
        return "Экономичный набор"
    if "картридж" in low or "singlepack" in name_cf or "cartridge" in low:
        return "Картридж"
    if "чернил" in low or name_cf.startswith("чернила"):
        return "Чернила"
    return _clean_text(current_type)


def _normalize_print_type_value(value: str) -> str:
    low = _cf(value)
    mapping = {
        "струйный": "Струйная",
        "лазерный": "Лазерная",
        "матричный": "Матричная",
        "сублимационный": "Сублимационная",
        "термосублимационный": "Термосублимационная",
    }
    return mapping.get(low, _clean_text(value))


def _set_single_param(params: list[tuple[str, str]], key: str, value: str) -> list[tuple[str, str]]:
    kcf = _cf(key)
    v = _clean_text(value)
    out: list[tuple[str, str]] = []
    placed = False
    for k, old in params:
        if _cf(k) == kcf:
            if not placed and v:
                out.append((key, v))
                placed = True
            continue
        out.append((k, old))
    if not placed and v:
        out.append((key, v))
    return out


def _repair_consumable_params(params: list[tuple[str, str]], *, name: str, desc: str, kind: str) -> list[tuple[str, str]]:
    if kind != "consumable":
        return list(params or [])

    out = list(params or [])
    current_type = _first_value(out, "Тип")
    inferred_type = _infer_consumable_type(name, desc, current_type)

    if _cf(current_type) in {"струйный", "лазерный", "матричный", "сублимационный", "термосублимационный"}:
        out = _set_single_param(out, "Тип печати", _normalize_print_type_value(current_type))
        out = _set_single_param(out, "Тип", inferred_type or current_type)
    elif inferred_type and current_type and ("фабрика печати" in _cf(current_type) or "чернила" in _cf(current_type) or "epson" in _cf(current_type)):
        out = _set_single_param(out, "Тип", inferred_type)
    elif inferred_type and not current_type:
        out = _set_single_param(out, "Тип", inferred_type)

    # normalize final consumable type labels
    norm_type = _clean_text(_first_value(out, "Тип"))
    if _cf(' '.join([norm_type, name, desc])).find('картридж') >= 0 or 'singlepack' in _cf(name):
        out = _set_single_param(out, "Тип", "Картридж")
    elif norm_type and ("фабрика печати" in _cf(norm_type) or _cf(norm_type) == "чернила"):
        out = _set_single_param(out, "Тип", "Чернила")

    current_device = _first_value(out, "Для устройства") or _first_value(out, "Совместимость")
    final_type = _clean_text(_first_value(out, "Тип"))
    is_waste_tank = "емкость для отработанных чернил" in _cf(final_type or name)

    # Для waste tank не синтезируем "Для устройства" из хвостов/desc:
    # это дало регрессии вроде "Epson EcoTank Maintenance".
    if not is_waste_tank:
        better_device = compat_extract_consumable_device_candidate(name, desc)
        if not better_device:
            better_device = compat_extract_direct_epson_device_list(" ".join([desc or "", name or ""]))
        if not better_device:
            better_device = compat_extract_explicit_epson_devices(" ".join([desc or "", name or ""]))
        if not better_device:
            better_device = compat_extract_models_from_text(" ".join([name or "", desc or ""]))
        better_device = compat_normalize_epson_device_list(better_device)
        if better_device and (compat_looks_generic_device_value(current_device) or len(better_device) >= len(current_device)):
            out = _set_single_param(out, "Для устройства", better_device)

    model = _first_value(out, "Модель")
    name_primary = compat_pick_name_primary_code(name)
    if name_primary and compat_should_force_consumable_model(model, name_primary, name):
        out = _set_single_param(out, "Модель", name_primary)
        model = name_primary

    code_src = " / ".join([
        _first_value(out, "Коды"),
        name_primary or "",
        name or "",
        model or "",
        desc or "",
    ])
    codes = compat_extract_codes_from_text(code_src)
    if name_primary and name_primary not in codes:
        codes.insert(0, name_primary)
    if codes:
        primary = codes[0]
        if compat_should_force_consumable_model(_first_value(out, "Модель"), primary, name):
            out = _set_single_param(out, "Модель", primary)
        # Для AkCent держим только основной supplier/item code.
        out = _set_single_param(out, "Коды", primary)

    # Для waste tank не создаём "Для устройства" автоматически из тела/хвостов.
    # Для остальных consumable допускаем только осторожный fallback.
    if not is_waste_tank and not _has_key(out, "Для устройства"):
        models = compat_normalize_epson_device_list(
            compat_extract_direct_epson_device_list(desc)
            or compat_extract_explicit_epson_devices(desc)
            or compat_extract_models_from_text(desc)
        )
        if models and not compat_looks_generic_device_value(models):
            out = _set_single_param(out, "Для устройства", models)

    if not is_waste_tank and not _has_key(out, "Для устройства"):
        desc_models = compat_normalize_epson_device_list(
            compat_extract_direct_epson_device_list(desc or name)
            or compat_extract_explicit_epson_devices(desc or name)
            or compat_extract_models_from_text(desc or name)
        )
        if desc_models and not compat_looks_generic_device_value(desc_models):
            out = _set_single_param(out, "Для устройства", desc_models)

    return out

def _clean_text(value: Any) -> str:
    return norm_ws(str(value or ""))


def _cf(value: Any) -> str:
    return _clean_text(value).casefold().replace("ё", "е")


def _get_field(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj.get(name)
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


def _get_offer_el(src: Any) -> ET.Element | None:
    el = _get_field(src, "offer_el", "el", "xml_offer")
    return el if isinstance(el, ET.Element) else None

def _read_price_triplet(src: Any) -> tuple[str, str, str]:
    dealer = _clean_text(
        _get_field(
            src,
            "dealer_price_text",
            "dealer_text",
            "dealer_price",
            "purchase_price_text",
            "purchase_price",
        )
    )
    price = _clean_text(
        _get_field(
            src,
            "price_text",
            "price",
            "price_kzt",
        )
    )
    rrp = _clean_text(
        _get_field(
            src,
            "rrp_text",
            "rrp",
            "retail_price_text",
            "rrp_price",
        )
    )

    offer_el = _get_offer_el(src)
    prices_el = offer_el.find("prices") if offer_el is not None else None
    if prices_el is not None:
        for price_el in prices_el.findall("price"):
            value = _clean_text("".join(price_el.itertext()))
            ptype = _cf(price_el.get("type"))
            if not value:
                continue
            if not dealer and ("дилер" in ptype or "dealer" in ptype):
                dealer = value
                continue
            if not rrp and ptype == "rrp":
                rrp = value
                continue
            if not price:
                price = value

    if offer_el is not None and not price:
        price = _clean_text(offer_el.findtext("price"))

    return dealer, price, rrp


def _read_warranty_values(src: Any) -> list[str]:
    values: list[str] = []
    for raw in (
        _get_field(src, "manufacturer_warranty", "manufacturer_warranty_text"),
        _get_field(src, "warranty", "warranty_text"),
    ):
        s = _clean_text(raw)
        if s:
            values.append(s)

    params_attr = _get_field(src, "params")
    if isinstance(params_attr, (list, tuple)):
        for item in params_attr:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                key = _clean_text(item[0])
                val = _clean_text(item[1])
                if key.casefold() == "гарантия" and val:
                    values.append(val)

    offer_el = _get_offer_el(src)
    if offer_el is not None:
        for p in offer_el.findall("Param"):
            key = _clean_text(p.get("name"))
            val = _clean_text("".join(p.itertext()))
            if key.casefold() == "гарантия" and val:
                values.append(val)

    # дедуп без потери порядка
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _append_unique_param(out: list[tuple[str, str]], seen: set[tuple[str, str]], key: str, value: str) -> None:
    k = _clean_text(key)
    v = _clean_text(value)
    if not k or not v:
        return
    item = (k, v)
    if item in seen:
        return
    seen.add(item)
    out.append(item)


def _first_value(params: Iterable[tuple[str, str]], key: str) -> str:
    key_cf = _cf(key)
    for k, v in params:
        if _cf(k) == key_cf and _clean_text(v):
            return _clean_text(v)
    return ""


def _has_key(params: Iterable[tuple[str, str]], key: str) -> bool:
    return bool(_first_value(params, key))


def _merge_params(
    xml_params: list[tuple[str, str]],
    desc_params: list[tuple[str, str]],
    *,
    allow_keys: set[str],
) -> list[tuple[str, str]]:
    """
    Базовый merge:
    - XML params считаем первичным источником;
    - desc params добираем только если такого ключа ещё нет;
    - порядок сохраняем стабильным.
    """
    out: list[tuple[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    seen_keys: set[str] = set()

    for key, value in xml_params or []:
        k = _clean_text(key)
        v = _clean_text(value)
        if not k or not v:
            continue
        if allow_keys and k not in allow_keys:
            continue
        _append_unique_param(out, seen_pairs, k, v)
        seen_keys.add(_cf(k))

    for key, value in desc_params or []:
        k = _clean_text(key)
        v = _clean_text(value)
        if not k or not v:
            continue
        if allow_keys and k not in allow_keys:
            continue
        if _cf(k) in seen_keys:
            continue
        _append_unique_param(out, seen_pairs, k, v)
        seen_keys.add(_cf(k))

    return out


def _ensure_default_params(
    params: list[tuple[str, str]],
    *,
    kind: str,
    model: str,
    warranty: str,
    vendor: str,
    allow_keys: set[str],
) -> list[tuple[str, str]]:
    out = list(params or [])
    seen: set[tuple[str, str]] = set(out)

    if model and (not allow_keys or "Модель" in allow_keys) and not _has_key(out, "Модель"):
        _append_unique_param(out, seen, "Модель", model)

    if warranty and (not allow_keys or "Гарантия" in allow_keys) and not _has_key(out, "Гарантия"):
        _append_unique_param(out, seen, "Гарантия", warranty)

    if kind == "consumable" and vendor and (not allow_keys or "Для бренда" in allow_keys) and not _has_key(out, "Для бренда"):
        _append_unique_param(out, seen, "Для бренда", vendor)

    return out




def _dedupe_type_params(params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """
    Чистит конфликтующие дубли по ключу "Тип".

    Логика:
    - если есть два значения "Тип", где одно содержится в другом
      ("Проектор" / "Проектор универсальный",
       "Экран" / "Экран настенный",
       "Картридж" / "Картридж EPSON"),
      оставляем более конкретное / длинное;
    - несвязанные значения не склеиваем;
    - порядок остальных параметров не трогаем.
    """
    if not params:
        return []

    type_values: list[str] = []
    for key, value in params:
        if _cf(key) == "тип":
            v = _clean_text(value)
            if v:
                type_values.append(v)

    if len(type_values) <= 1:
        return list(params)

    keep_values: set[str] = set(type_values)

    def _norm_type(v: str) -> str:
        return _RE_WS.sub(" ", _cf(v)).strip()

    norm_map = {v: _norm_type(v) for v in type_values}

    for left in type_values:
        nl = norm_map[left]
        if not nl:
            continue
        for right in type_values:
            if left == right:
                continue
            nr = norm_map[right]
            if not nr:
                continue
            if nl == nr:
                # если по смыслу одинаковые, оставляем более длинный / конкретный
                if len(right) > len(left):
                    keep_values.discard(left)
                continue
            if nl in nr and len(nr) > len(nl):
                keep_values.discard(left)
                continue

    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for key, value in params:
        k = _clean_text(key)
        v = _clean_text(value)
        if not k or not v:
            continue
        if _cf(k) == "тип" and v not in keep_values:
            continue
        item = (k, v)
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _filter_allowed(params: list[tuple[str, str]], allow_keys: set[str]) -> list[tuple[str, str]]:
    if not allow_keys:
        return params
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for key, value in params or []:
        k = _clean_text(key)
        v = _clean_text(value)
        if not k or not v:
            continue
        if k not in allow_keys:
            continue
        _append_unique_param(out, seen, k, v)
    return out


def _render_extra_info(extra_info: list[tuple[str, str]], *, limit: int = 12) -> str:
    items: list[str] = []
    for key, value in extra_info[: max(0, int(limit))]:
        k = _clean_text(key)
        v = _clean_text(value)
        if not k or not v:
            continue
        items.append(f"{k}: {v}")
    if not items:
        return ""
    return "Дополнительно:\n" + "\n".join(items)

def _merge_native_desc(clean_desc: str, extra_info: list[tuple[str, str]]) -> str:
    base = _clean_text(clean_desc)
    extra_block = _render_extra_info(extra_info)
    if base and extra_block:
        return f"{base}\n\n{extra_block}"
    return base or extra_block


def _build_single_offer(
    src: Any,
    *,
    schema_cfg: dict[str, Any],
    placeholder_picture: str,
    id_prefix: str,
    vendor_blacklist: set[str],
) -> tuple[OfferOut | None, dict[str, Any]]:
    raw_name = _clean_text(_get_field(src, "name"))
    kind = detect_kind_by_name(raw_name, schema_cfg)
    raw_name = norm_normalize_consumable_name(raw_name, kind=kind)
    allow_keys = resolve_allowed_keys(schema_cfg, kind)

    dealer_text, price_text, rrp_text = _read_price_triplet(src)
    warranty_values = _read_warranty_values(src)

    basics = normalize_source_basics(
        raw_id=_clean_text(_get_field(src, "raw_id", "id")),
        offer_id=_clean_text(_get_field(src, "offer_id", "Offer_ID")),
        article=_clean_text(_get_field(src, "article", "vendor_code")),
        name=raw_name,
        model=_clean_text(_get_field(src, "model")),
        vendor=_clean_text(_get_field(src, "vendor")),
        description_text=_clean_text(_get_field(src, "description", "desc")),
        dealer_text=dealer_text,
        price_text=price_text,
        rrp_text=rrp_text,
        available_attr=_clean_text(_get_field(src, "available_attr", "available")),
        available_tag=_clean_text(_get_field(src, "available_tag", "delivery")),
        stock_text=_clean_text(_get_field(src, "stock_text", "Stock", "stock")),
        warranty_values=warranty_values,
        vendor_blacklist=vendor_blacklist,
        id_prefix=id_prefix,
    )

    oid = _clean_text(basics.get("oid"))
    name = _clean_text(basics.get("name"))
    model = _clean_text(basics.get("model"))
    vendor = _clean_text(basics.get("vendor"))
    warranty = _clean_text(basics.get("warranty"))
    price_in = basics.get("price_in")
    available = bool(basics.get("available"))

    if not oid or not name:
        return None, {
            "built": False,
            "reason": "missing_identity",
            "kind": kind,
            "oid": oid,
            "name": name,
        }

    xml_params, extra_info, xml_report = collect_xml_params(
        src,
        schema_cfg=schema_cfg,
        kind=kind,
        unknown_to_extra_info=True,
    )

    description_raw = _clean_text(_get_field(src, "description", "desc"))
    cleaned_desc = clean_description_text(
        description_raw,
        name=name,
        kind=kind,
        vendor=vendor,
        model=model,
    )
    desc_params, desc_report = extract_desc_params(
        cleaned_desc,
        name=name,
        kind=kind,
        vendor=vendor,
        model=model,
        schema_cfg=schema_cfg,
    )

    merged_params = _merge_params(xml_params, desc_params, allow_keys=allow_keys)
    merged_params = _ensure_default_params(
        merged_params,
        kind=kind,
        model=model,
        warranty=warranty,
        vendor=vendor,
        allow_keys=allow_keys,
    )
    merged_params = reconcile_params(
        merged_params,
        name=name,
        model=model,
        kind=kind,
    )
    merged_params = compat_normalize_consumable_device_params(merged_params, kind=kind)
    merged_params = _repair_consumable_params(merged_params, name=name, desc=cleaned_desc, kind=kind)
    merged_params = _dedupe_type_params(merged_params)
    merged_params = _filter_allowed(merged_params, allow_keys)

    pictures = collect_picture_urls(src, placeholder_picture=placeholder_picture)
    cleaned_desc = _soften_consumable_body(cleaned_desc, merged_params, kind=kind)
    if kind == "consumable":
        cleaned_desc = desc_strip_name_prefix_from_desc(cleaned_desc, name)
        short_desc = _build_consumable_short_desc(merged_params).strip()
        low_desc = _cf(cleaned_desc)
        if not cleaned_desc:
            cleaned_desc = short_desc
        elif len(cleaned_desc) < 24:
            cleaned_desc = short_desc
        elif re.fullmatch(r"(?iu)(?:сменная\s+)?(?:емкость|ёмкость)\s+для\s+отработанных\s+чернил\.?$", cleaned_desc):
            cleaned_desc = short_desc
    native_desc = _merge_native_desc(cleaned_desc, extra_info)
    raw_price = price_in if isinstance(price_in, int) else 0

    if kind == "consumable":
        name = norm_finalize_consumable_name(name, merged_params)
        name = norm_finalize_waste_tank_name(name, merged_params)
        cleaned_desc = _finalize_waste_tank_desc(cleaned_desc, name, merged_params)
        native_desc = _merge_native_desc(cleaned_desc, extra_info)

    offer = OfferOut(
        oid=oid,
        available=available,
        name=name,
        price=raw_price,
        pictures=pictures,
        vendor=vendor,
        params=merged_params,
        native_desc=native_desc,
    )

    info = {
        "built": True,
        "kind": kind,
        "oid": oid,
        "name": name,
        "price_in": price_in,
        "price": raw_price,
        "available": available,
        "params_count": len(merged_params),
        "extra_info_count": len(extra_info),
        "desc_params_count": len(desc_params),
        "xml_params_count": len(xml_params),
        "xml_report": xml_report,
        "desc_report": desc_report,
        "used_placeholder_picture": bool(pictures and len(pictures) == 1 and pictures[0] == placeholder_picture),
    }
    return offer, info


def build_offers(
    filtered_offers: list[Any],
    *,
    schema_cfg: dict[str, Any] | None = None,
    policy_cfg: dict[str, Any] | None = None,
    placeholder_picture: str = "https://placehold.co/800x800/png?text=No+Photo",
    id_prefix: str = "AC",
    vendor_blacklist: set[str] | None = None,
) -> tuple[list[OfferOut], dict[str, Any]]:
    """
    Главная сборка AkCent raw offers.

    Возвращает:
    - список OfferOut;
    - подробный report для orchestrator/diagnostics.
    """
    schema_cfg = dict(schema_cfg or {})
    policy_cfg = dict(policy_cfg or {})
    vendor_blacklist = set(vendor_blacklist or set())

    if not placeholder_picture:
        placeholder_picture = _clean_text(
            (policy_cfg.get("placeholder_picture") if isinstance(policy_cfg, dict) else "")
            or "https://placehold.co/800x800/png?text=No+Photo"
        )

    built: list[OfferOut] = []
    kind_hits: Counter[str] = Counter()
    fail_reasons: Counter[str] = Counter()
    used_placeholder_picture = 0
    rows: list[dict[str, Any]] = []

    for src in filtered_offers or []:
        offer, info = _build_single_offer(
            src,
            schema_cfg=schema_cfg,
            placeholder_picture=placeholder_picture,
            id_prefix=id_prefix,
            vendor_blacklist=vendor_blacklist,
        )
        rows.append(info)
        if not offer:
            fail_reasons[str(info.get("reason") or "build_failed")] += 1
            continue
        built.append(offer)
        kind_hits[str(info.get("kind") or "unknown")] += 1
        if bool(info.get("used_placeholder_picture")):
            used_placeholder_picture += 1

    report: dict[str, Any] = {
        "before": len(filtered_offers or []),
        "after": len(built),
        "dropped_total": max(0, len(filtered_offers or []) - len(built)),
        "kinds": dict(sorted(kind_hits.items())),
        "fail_reasons": dict(sorted(fail_reasons.items())),
        "placeholder_picture_count": used_placeholder_picture,
        "rows_preview": rows[:50],
    }
    return built, report
