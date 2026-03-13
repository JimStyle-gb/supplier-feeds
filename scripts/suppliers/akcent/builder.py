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
from cs.pricing import compute_price
from cs.util import norm_ws
from suppliers.akcent.compat import clean_device_value, reconcile_params
from suppliers.akcent.desc_clean import clean_description_text
from suppliers.akcent.desc_extract import extract_desc_params
from suppliers.akcent.normalize import normalize_source_basics
from suppliers.akcent.params_xml import collect_xml_params, detect_kind_by_name, resolve_allowed_keys

try:
    from suppliers.akcent.pictures import collect_picture_urls as _collect_picture_urls  # type: ignore
except Exception:
    _collect_picture_urls = None


_RE_WS = re.compile(r"\s+")


_RE_DROP_CONSUMABLE_DESC_LINE = re.compile(
    r"(?iu)\b(?:поддерживаемые\s+модели(?:\s+принтеров|\s+устройств|\s+техники)?|"
    r"совместимые\s+модели(?:\s+техники)?|совместимые\s+продукты(?:\s+для)?|для)\s*:"
)

_RE_DEVICE_MODEL = re.compile(
    r"(?iu)"
    r"(?:(SureColor|WorkForce\s+Pro|WorkForce|EcoTank|Stylus\s+Pro|Expression|PIXMA|LaserJet)\s+)?"
    r"("
    r"(?:SC-[A-Z0-9-]+|WF-[A-Z0-9-]+|ET-\d+[A-Z0-9-]*|"
    r"L\d{4,5}[A-Z0-9-]*|T\d{4,5}[A-Z0-9-]*(?:\s*w/\s*o\s*stand)?|"
    r"P\d{4,5}[A-Z0-9-]*|B\d{4,5}[A-Z0-9-]*|C\d{4,5}[A-Z0-9-]*|"
    r"M\d{4,5}[A-Z0-9-]*|DCP-[A-Z0-9-]+|MFC-[A-Z0-9-]+)"
    r")"
)


def _dedupe_text_items(items: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        x = _clean_text(item)
        if not x:
            continue
        key = _cf(x)
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out


def _normalize_consumable_device_value(value: str) -> str:
    src = _clean_text(value)
    if not src:
        return ""

    src = re.sub(r"(?iu)\bw/\s*o\s*stand\b", "", src)
    src = re.sub(r"(?iu)\bsurecolor\b", "SureColor", src)
    src = re.sub(r"(?iu)\bworkforce\s+pro\b", "WorkForce Pro", src)
    src = re.sub(r"(?iu)\bworkforce\b", "WorkForce", src)
    src = re.sub(r"(?iu)\becotank\b", "EcoTank", src)
    src = re.sub(r"(?iu)\bstylus\s+pro\b", "Stylus Pro", src)
    src = re.sub(r"(?iu)\bexpression\b", "Expression", src)
    src = re.sub(r"(?iu)\bpixma\b", "PIXMA", src)
    src = re.sub(r"(?iu)\blaserjet\b", "LaserJet", src)
    src = re.sub(r"(?iu)\b([A-Z]{1,3})-\s+([A-Z0-9])", r"\1-\2", src)

    chunks: list[str] = []
    last_family = ""
    for m in _RE_DEVICE_MODEL.finditer(src):
        family = _clean_text(m.group(1))
        model = _clean_text(m.group(2))
        if not model:
            continue
        model = re.sub(r"(?iu)\bw/\s*o\s*stand\b", "", model)
        model = _clean_text(model)
        if family:
            last_family = family
        elif last_family and re.match(r"(?iu)^(?:P|T|WF|ET|L|SC-|B|C|M)\d", model):
            family = last_family
        item = f"{family} {model}".strip() if family else model
        chunks.append(item)

    if chunks:
        cleaned = _dedupe_text_items(chunks)
        return " / ".join(cleaned)

    fallback = _clean_text(clean_device_value(src))
    fallback = re.sub(r"(?iu)\bw/\s*o\s*stand\b", "", fallback)
    parts = [x for x in re.split(r"\s*(?:,|/)\s*", fallback) if _clean_text(x)]
    cleaned = _dedupe_text_items(parts)
    return " / ".join(cleaned) if cleaned else ""


def _normalize_consumable_device_params(params: list[tuple[str, str]], *, kind: str) -> list[tuple[str, str]]:
    if kind != "consumable" or not params:
        return list(params or [])

    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for key, value in params:
        k = _clean_text(key)
        v = _clean_text(value)
        if not k or not v:
            continue
        if _cf(k) in {"для устройства", "совместимость"}:
            v2 = _normalize_consumable_device_value(v)
            if v2:
                _append_unique_param(out, seen, k, v2)
            continue
        _append_unique_param(out, seen, k, v)

    return out


_RE_CONSUMABLE_MODEL_TAIL = re.compile(
    r"(?iu)(?:поддерживаемые\s+модели(?:\s+принтеров|\s+устройств|\s+техники)?|совместимые\s+модели(?:\s+техники)?|совместимые\s+продукты(?:\s+для)?)\s*:?[ \t]*(.+)$"
)
_RE_L_SERIES_PACK = re.compile(r"(?iu)\bL(\d{3,5}(?:/\d{3,5})+)\b")
_RE_CODE_TOKEN = re.compile(r"(?iu)C(?:11|12|13|33)[A-Z0-9]{5,10}|T[0-9A-Z]{5,10}")
_RE_WORKFORCE_MODEL = re.compile(r"(?iu)(?:Epson\s+)?WorkForce\s+[A-Z0-9-]+")
_RE_SURECOLOR_MODEL = re.compile(r"(?iu)(?:Epson\s+)?SureColor\s+SC-[A-Z0-9-]+")
_RE_ECOTANK_MODEL = re.compile(r"(?iu)(?:Epson\s+)?EcoTank\s+[A-Z0-9-]+")
_RE_STYLUS_MODEL = re.compile(r"(?iu)(?:Epson\s+)?Stylus(?:\s+Pro)?\s+[A-Z0-9-]+")
_RE_GENERIC_EPS_MODEL = re.compile(r"(?iu)(?:WF|SC|ET|L)-?[A-Z0-9]{2,}")


def _title_eps_family(value: str) -> str:
    v = _clean_text(value)
    if not v:
        return ""
    v = re.sub(r"(?iu)epson", "Epson", v)
    v = re.sub(r"(?iu)surecolor", "SureColor", v)
    v = re.sub(r"(?iu)workforce", "WorkForce", v)
    v = re.sub(r"(?iu)ecotank", "EcoTank", v)
    v = re.sub(r"(?iu)stylus", "Stylus", v)
    v = re.sub(r"(?iu)w/?o\s*stand", "", v)
    return _clean_text(v)


def _extract_models_from_text(text: str) -> str:
    src = _clean_text(text)
    if not src:
        return ""

    items: list[str] = []
    for rx in (_RE_SURECOLOR_MODEL, _RE_WORKFORCE_MODEL, _RE_ECOTANK_MODEL, _RE_STYLUS_MODEL):
        items.extend([_title_eps_family(m.group(0)) for m in rx.finditer(src)])

    if not items:
        for m in _RE_GENERIC_EPS_MODEL.finditer(src):
            token = _clean_text(m.group(0)).upper().replace('SC ', 'SC-').replace('WF ', 'WF-').replace('ET ', 'ET-').replace('L ', 'L')
            token = token.replace('SC- ', 'SC-').replace('WF- ', 'WF-').replace('ET- ', 'ET-')
            if token.startswith('SC-'):
                items.append(f"Epson SureColor {token}")
            elif token.startswith('WF-'):
                items.append(f"Epson WorkForce {token}")
            elif token.startswith('ET-'):
                items.append(f"Epson EcoTank {token}")
            elif token.startswith('L') and len(token) > 1 and token[1:].isdigit():
                items.append(f"Epson {token}")

    return " / ".join(_dedupe_text_items([x for x in items if x]))


def _extract_consumable_device_candidate(name: str, desc: str) -> str:
    text = _clean_text(desc)
    for line in text.split("\n"):
        line = _clean_text(line)
        if not line:
            continue
        m = _RE_CONSUMABLE_MODEL_TAIL.search(line)
        if m:
            cand = _normalize_consumable_device_value(m.group(1))
            models = _extract_models_from_text(cand)
            if models:
                return models
            if cand:
                return cand

    models = _extract_models_from_text(text)
    if models:
        return models

    # fallback for Epson L-series inks from name like L800/1800/810/850
    m2 = _RE_L_SERIES_PACK.search(_clean_text(name))
    if m2:
        nums = [x for x in m2.group(1).split('/') if _clean_text(x)]
        items = [f"Epson L{n}" for n in nums]
        return " / ".join(_dedupe_text_items(items))

    return ""


def _looks_generic_device_value(value: str) -> bool:
    low = _cf(value)
    if not low:
        return True
    if any(x in low for x in ["широкоформатный принтер", "принтер", "мфу", "фотопечать", "устройств epson"]):
        return True
    return not bool(_RE_DEVICE_MODEL.search(value))


_RE_PRIMARY_CONSUMABLE_CODE = re.compile(r"(?iu)\bC(?:11|12|13|33)[A-Z0-9]{5,10}\b")
_RE_SECONDARY_T_CODE = re.compile(r"(?iu)\bT[0-9A-Z]{5,10}\b")


def _pick_name_primary_code(name: str) -> str:
    m = _RE_PRIMARY_CONSUMABLE_CODE.search(_clean_text(name))
    return _clean_text(m.group(0)).upper() if m else ""


def _pick_secondary_t_code(name: str, desc: str, primary: str) -> str:
    joined = " / ".join([_clean_text(name), _clean_text(desc)])
    for m in _RE_SECONDARY_T_CODE.finditer(joined):
        code = _clean_text(m.group(0)).upper()
        if code and code != primary:
            return code
    return ""


def _should_force_consumable_model(current_model: str, primary_code: str, name: str) -> bool:
    cur = _clean_text(current_model).upper()
    if not primary_code:
        return False
    if not cur:
        return True
    if cur == primary_code:
        return False
    if ' ' in _clean_text(current_model):
        return True
    if cur.startswith('C11') or cur.startswith('C12') or cur.startswith('C13') or cur.startswith('C33'):
        return True
    if cur in _clean_text(name).upper() and cur != primary_code:
        return True
    return False


def _normalize_epson_device_list(value: str) -> str:
    src = _clean_text(value)
    if not src:
        return ""
    src = re.sub(r"(?iu)\bSC-\s+", "SC-", src)
    src = re.sub(r"(?iu)\bWF-\s+", "WF-", src)
    src = re.sub(r"(?iu)\bET-\s+", "ET-", src)
    src = re.sub(r"(?iu)(?<!Epson\s)\bSureColor\b", "Epson SureColor", src)
    src = re.sub(r"(?iu)(?<!Epson\s)\bWorkForce\b", "Epson WorkForce", src)
    src = re.sub(r"(?iu)(?<!Epson\s)\bEcoTank\b", "Epson EcoTank", src)
    src = re.sub(r"(?iu)(?<!Epson\s)\bStylus(?:\s+Pro)?\b", lambda m: 'Epson ' + _clean_text(m.group(0)), src)
    # keep only model-like fragments when possible
    models = _extract_models_from_text(src)
    return models or src


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
    better_device = _extract_consumable_device_candidate(name, desc)
    if not better_device:
        better_device = _extract_models_from_text(" ".join([name or "", desc or ""]))
    better_device = _normalize_epson_device_list(better_device)
    if better_device and (_looks_generic_device_value(current_device) or len(better_device) >= len(current_device)):
        out = _set_single_param(out, "Для устройства", better_device)

    model = _first_value(out, "Модель")
    name_primary = _pick_name_primary_code(name)
    if name_primary and _should_force_consumable_model(model, name_primary, name):
        out = _set_single_param(out, "Модель", name_primary)
        model = name_primary

    code_src = " / ".join([
        _first_value(out, "Коды"),
        name_primary or "",
        name or "",
        model or "",
        desc or "",
    ])
    codes: list[str] = []
    for m in _RE_CODE_TOKEN.finditer(code_src):
        c = _clean_text(m.group(0)).upper()
        if c and c not in codes:
            codes.append(c)
    if name_primary and name_primary not in codes:
        codes.insert(0, name_primary)
    if codes:
        primary = codes[0]
        if _should_force_consumable_model(_first_value(out, "Модель"), primary, name):
            out = _set_single_param(out, "Модель", primary)
        secondary_t = _pick_secondary_t_code(name, desc, primary)
        if secondary_t:
            out = _set_single_param(out, "Коды", f"{primary} / {secondary_t}")
        elif len(codes) > 1:
            compact = [c for c in codes[1:3] if c != primary]
            if compact:
                out = _set_single_param(out, "Коды", " / ".join([primary] + compact[:1]))
            else:
                out = _set_single_param(out, "Коды", primary)
        else:
            out = _set_single_param(out, "Коды", primary)

    # some descriptions are just pure model lists; preserve them as device list
    if not _has_key(out, "Для устройства"):
        models = _normalize_epson_device_list(_extract_models_from_text(desc))
        if models:
            out = _set_single_param(out, "Для устройства", models)

    return out


def _build_consumable_short_desc(params: list[tuple[str, str]]) -> str:
    type_value = _first_value(params, "Тип") or "Расходный материал"
    brand_value = (
        _first_value(params, "Для бренда")
        or _first_value(params, "Бренд")
        or _first_value(params, "Производитель")
    )
    type_value = _clean_text(type_value)
    brand_value = _clean_text(brand_value)

    if type_value and brand_value:
        return (
            f"{type_value} для устройств {brand_value}. "
            "Основные совместимые модели указаны в характеристике «Для устройства»."
        )

    if type_value:
        return f"{type_value}. Основные совместимые модели указаны в характеристике «Для устройства»."

    return "Основные совместимые модели указаны в характеристике «Для устройства»."


def _drop_consumable_device_narrative(clean_desc: str, params: list[tuple[str, str]], *, kind: str) -> str:
    if kind != "consumable":
        return _clean_text(clean_desc)

    device_value = _first_value(params, "Для устройства") or _first_value(params, "Совместимость")
    if not device_value:
        return _clean_text(clean_desc)

    lines = [_clean_text(x) for x in str(clean_desc or "").split("\n")]
    kept: list[str] = []

    for line in lines:
        if not line:
            continue
        low = _cf(line)
        if _RE_DROP_CONSUMABLE_DESC_LINE.search(line) and any(
            mark in low for mark in ("epson", "surecolor", "workforce", "ecotank", "stylus", "pixma", "laserjet")
        ):
            continue
        kept.append(line)

    result = "\n".join(kept).strip()
    if result:
        return result

    return _build_consumable_short_desc(params).strip()


_RE_INLINE_SUPPLIER_HEADER = re.compile(
    r"(?iu)\b(?:технические\s+характеристики|основные\s+характеристики|общие\s+характеристики|общие\s+характерстики)\b\s*:?"
)


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

    return text


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


def _iter_picture_urls(src: Any) -> list[str]:
    direct = _get_field(src, "picture_urls", "pictures", "picture_list")
    out: list[str] = []
    seen: set[str] = set()

    if isinstance(direct, (list, tuple)):
        for raw in direct:
            s = _clean_text(raw).replace(" ", "%20")
            if not s:
                continue
            key = s.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(s)

    for raw in (
        _get_field(src, "picture_url"),
        _get_field(src, "picture"),
        _get_field(src, "image"),
    ):
        s = _clean_text(raw).replace(" ", "%20")
        if not s:
            continue
        key = s.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)

    offer_el = _get_offer_el(src)
    if offer_el is not None:
        for pic_el in offer_el.findall("picture"):
            s = _clean_text("".join(pic_el.itertext())).replace(" ", "%20")
            if not s:
                continue
            key = s.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(s)

    return out


def _collect_pictures(urls: list[str], *, placeholder_picture: str) -> list[str]:
    if _collect_picture_urls is not None:
        return _collect_picture_urls(urls, placeholder_picture=placeholder_picture)

    out: list[str] = []
    seen: set[str] = set()
    for raw in urls or []:
        s = _clean_text(raw).replace(" ", "%20")
        if not s:
            continue
        key = s.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    if not out and placeholder_picture:
        out = [placeholder_picture]
    return out


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
    merged_params = _normalize_consumable_device_params(merged_params, kind=kind)
    merged_params = _repair_consumable_params(merged_params, name=name, desc=cleaned_desc, kind=kind)
    merged_params = _dedupe_type_params(merged_params)
    merged_params = _filter_allowed(merged_params, allow_keys)

    pictures = _collect_pictures(_iter_picture_urls(src), placeholder_picture=placeholder_picture)
    cleaned_desc = _soften_consumable_body(cleaned_desc, merged_params, kind=kind)
    native_desc = _merge_native_desc(cleaned_desc, extra_info)
    final_price = compute_price(price_in if isinstance(price_in, int) else None)

    offer = OfferOut(
        oid=oid,
        available=available,
        name=name,
        price=final_price,
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
        "price": final_price,
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
