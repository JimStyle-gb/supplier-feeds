# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/builder.py

AlStyle supplier layer — сборка raw offer.

v108:
- убран лишний повторный проход align/dedupe после sanitize_native_desc();
- добавлен безопасный fallback Модель из name для картриджных Canon-паттернов типа PG-510 / CL-511 / CLI-65;
- добавлен selective override: чистые desc params могут заменить только грязные XML значения
  для Совместимость / Цвет / Технология / Ресурс.
"""

from __future__ import annotations

import re

from cs.core import OfferOut
from cs.pricing import compute_price
from cs.util import norm_ws
from suppliers.alstyle.desc_clean import sanitize_native_desc
from suppliers.alstyle.desc_extract import extract_desc_spec_pairs
from suppliers.alstyle.models import SourceOffer
from suppliers.alstyle.normalize import (
    build_offer_oid,
    normalize_available,
    normalize_name,
    normalize_price_in,
    normalize_vendor,
)
from suppliers.alstyle.params_xml import collect_xml_params
from suppliers.alstyle.pictures import collect_picture_urls


_NAME_MODEL_RE = re.compile(
    r"\b(?:PG|CL|CLI|BCI|GI|PFI|CF|CE|CB|CC|CH|BH)-[A-Z0-9]{2,10}\b",
    re.IGNORECASE,
)


_SAFE_DESC_OVERRIDE_KEYS = {"Совместимость", "Цвет", "Технология", "Ресурс"}
_DIRTY_COMPAT_RE = re.compile(
    r"(?iu)\b(?:Гарантированн(?:ый|ого)\s+об(?:ъ|ь)ем\s+отпечатков|"
    r"при\s+5%\s+заполнении|формата\s+A4|только\s+для\s+продажи\s+на\s+территории|"
    r"Форматы\s+бумаги|Плотность|Емкость|Ёмкость|Скорость\s+печати|Интерфейс|Процессор|Память)\b"
)
_DIRTY_COLOR_RE = re.compile(
    r"(?iu)\b(?:Тип\s+чернил|Ресурс(?:\s+картриджа)?|Количество\s+страниц|Секция\s+аппарата|"
    r"Совместимость|Устройства|Количество\s+цветов|серия|Vivobook|Vector|Gaming|игров)\b"
)
_DIRTY_TECH_RE = re.compile(
    r"(?iu)\b(?:Количество\s+цветов|Тип\s+чернил|Ресурс(?:\s+картриджа)?|Совместимость|"
    r"Устройства|Об(?:ъ|ь)ем\s+картриджа|Секция\s+аппарата|серия)\b"
)
_CLEAN_TECH_RE = re.compile(
    r"(?iu)^(?:Лазерная(?:\s+монохромная|\s+цветная)?|Светодиодная(?:\s+монохромная|\s+цветная)?|"
    r"Струйная|Термоструйная|Матричная|Термосублимационная)$"
)
_CLEAN_RESOURCE_RE = re.compile(r"(?iu)^\d[\d\s.,]*(?:\s*(?:стр\.?|страниц|pages|copies))?$")


def _is_dirty_value(key: str, value: str) -> bool:
    k = norm_ws(key)
    v = norm_ws(value)
    if not k or not v:
        return True

    if k == "Совместимость":
        if _DIRTY_COMPAT_RE.search(v):
            return True
        if "/" not in v and "," not in v and len(v.split()) > 10:
            return True
        if re.search(r"(?iu)Canon\s+imagePRESS(?:\s+Lite)?\s+[^/]+\s+Canon\s+imageRUNNER", v):
            return True
        return False

    if k == "Цвет":
        if _DIRTY_COLOR_RE.search(v):
            return True
        if len(v.split()) > 4:
            return True
        return False

    if k == "Технология":
        if _DIRTY_TECH_RE.search(v):
            return True
        if not _CLEAN_TECH_RE.fullmatch(v):
            return True
        return False

    if k == "Ресурс":
        if len(v) > 40:
            return True
        if not _CLEAN_RESOURCE_RE.fullmatch(v):
            return True
        return False

    return False


def _prefer_desc_value(key: str, xml_val: str, desc_val: str) -> bool:
    if key not in _SAFE_DESC_OVERRIDE_KEYS:
        return False
    if not desc_val:
        return False

    xml_dirty = _is_dirty_value(key, xml_val)
    desc_dirty = _is_dirty_value(key, desc_val)
    if desc_dirty:
        return False
    if xml_dirty:
        return True

    if key == "Ресурс" and len(desc_val) < len(xml_val):
        return True
    return False


def merge_params(
    xml_params: list[tuple[str, str]],
    desc_params: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    XML params по умолчанию приоритетнее.
    Description-derived params только дополняют,
    но могут точечно заменить грязные XML значения
    для безопасного набора ключей.
    """
    out: list[tuple[str, str]] = []
    seen_pair: set[tuple[str, str]] = set()
    index_by_key: dict[str, int] = {}

    for k, v in xml_params:
        k2 = norm_ws(k)
        v2 = norm_ws(v)
        if not k2 or not v2:
            continue
        sig = (k2.casefold(), v2.casefold())
        if sig in seen_pair:
            continue
        index_by_key.setdefault(k2.casefold(), len(out))
        out.append((k2, v2))
        seen_pair.add(sig)

    for k, v in desc_params:
        k2 = norm_ws(k)
        v2 = norm_ws(v)
        if not k2 or not v2:
            continue

        key_cf = k2.casefold()
        sig = (key_cf, v2.casefold())
        if sig in seen_pair:
            continue

        if key_cf in index_by_key:
            idx = index_by_key[key_cf]
            xml_k, xml_v = out[idx]
            if _prefer_desc_value(xml_k, xml_v, v2):
                seen_pair.discard((xml_k.casefold(), xml_v.casefold()))
                out[idx] = (xml_k, v2)
                seen_pair.add((xml_k.casefold(), v2.casefold()))
            continue

        out.append((k2, v2))
        index_by_key[key_cf] = len(out) - 1
        seen_pair.add(sig)

    return out


def _has_param(params: list[tuple[str, str]], key: str) -> bool:
    kcf = norm_ws(key).casefold()
    return any(norm_ws(k).casefold() == kcf and norm_ws(v) for k, v in params)


def _infer_model_from_name(name: str) -> str:
    n = norm_ws(name)
    if not n:
        return ""
    hits = [m.group(0).upper() for m in _NAME_MODEL_RE.finditer(n)]
    if not hits:
        return ""
    return hits[-1]


def build_offer(
    src: SourceOffer,
    *,
    schema_cfg: dict,
    vendor_blacklist: set[str],
    placeholder_picture: str,
    id_prefix: str = "AS",
) -> tuple[OfferOut | None, bool]:
    raw_id = norm_ws(src.raw_id)
    name = normalize_name(src.name)
    if not raw_id or not name:
        return None, False

    oid = build_offer_oid(raw_id, prefix=id_prefix)
    available = normalize_available(src.available_attr, src.available_tag)
    pictures = collect_picture_urls(src.picture_urls, placeholder_picture=placeholder_picture)
    vendor = normalize_vendor(src.vendor, vendor_blacklist=vendor_blacklist)

    desc_src = sanitize_native_desc(src.description or "", name=name)

    xml_params = collect_xml_params(src.offer_el, schema_cfg) if src.offer_el is not None else []
    desc_params = extract_desc_spec_pairs(desc_src, schema_cfg)
    params = merge_params(xml_params, desc_params)

    if not _has_param(params, "Модель"):
        inferred_model = _infer_model_from_name(name)
        if inferred_model:
            params.append(("Модель", inferred_model))

    price_in = normalize_price_in(src.purchase_price_text, src.price_text)
    price = compute_price(price_in)

    offer = OfferOut(
        oid=oid,
        available=available,
        name=name,
        price=price,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=desc_src,
    )
    return offer, available


def build_offers(
    source_offers: list[SourceOffer],
    *,
    schema_cfg: dict,
    vendor_blacklist: set[str],
    placeholder_picture: str,
    id_prefix: str = "AS",
) -> tuple[list[OfferOut], int, int]:
    out: list[OfferOut] = []
    in_true = 0
    in_false = 0

    for src in source_offers:
        offer, available = build_offer(
            src,
            schema_cfg=schema_cfg,
            vendor_blacklist=vendor_blacklist,
            placeholder_picture=placeholder_picture,
            id_prefix=id_prefix,
        )
        if offer is None:
            continue
        if available:
            in_true += 1
        else:
            in_false += 1
        out.append(offer)

    out.sort(key=lambda x: x.oid)
    return out, in_true, in_false
