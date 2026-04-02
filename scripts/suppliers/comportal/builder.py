# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/builder.py

ComPortal supplier layer — сборка raw offer.

Единая роль файла как у готовых поставщиков:
- взять SourceOffer;
- прогнать supplier-side cleanup/extraction;
- собрать чистый raw OfferOut;
- supplier-specific ошибки чинятся здесь, а не в core.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from cs.core import OfferOut
from cs.util import norm_ws
from suppliers.comportal.compat import apply_compat_cleanup
from suppliers.comportal.desc_clean import sanitize_native_desc
from suppliers.comportal.desc_extract import extract_desc_fill_params
from suppliers.comportal.models import BuildStats, ParamItem, SourceOffer
from suppliers.comportal.normalize import (
    build_offer_oid,
    normalize_available,
    normalize_model,
    normalize_name,
    normalize_price_in,
    normalize_vendor,
)
from suppliers.comportal.params_xml import build_params_from_xml
from suppliers.comportal.pictures import collect_picture_urls


def _param_map(params: list[ParamItem]) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in params or []:
        name = norm_ws(p.name)
        value = norm_ws(p.value)
        if name and value and name.casefold() not in {k.casefold() for k in out}:
            out[name] = value
    return out


def _set_if_missing(params: list[ParamItem], *, name: str, value: str, source: str) -> list[ParamItem]:
    if not norm_ws(name) or not norm_ws(value):
        return params
    pmap = _param_map(params)
    if any(k.casefold() == name.casefold() for k in pmap):
        return params
    return list(params) + [ParamItem(name=norm_ws(name), value=norm_ws(value), source=source)]


def _ensure_base_params(
    *,
    source_offer: SourceOffer,
    params: list[ParamItem],
    vendor: str,
    model: str,
) -> list[ParamItem]:
    out = list(params)
    pmap = _param_map(out)

    if vendor and "Для бренда" not in pmap:
        out.append(ParamItem(name="Для бренда", value=vendor, source="normalize"))

    if model and "Модель" not in pmap:
        out.append(ParamItem(name="Модель", value=model, source="normalize"))

    # Коды часто удобно брать из хвостовых скобок / vendorCode.
    if "Коды" not in pmap:
        if model:
            out.append(ParamItem(name="Коды", value=model, source="normalize"))
        elif source_offer.vendor_code:
            out.append(ParamItem(name="Коды", value=norm_ws(source_offer.vendor_code), source="source"))

    return out


def _build_native_desc(
    *,
    clean_name: str,
    source_offer: SourceOffer,
    params: list[ParamItem],
) -> str:
    """
    Supplier-side narrative для raw.

    Для ComPortal source-description обычно слабый, поэтому:
    - если supplier body есть и чистый — берём его;
    - иначе собираем короткий служебно-полезный narrative из params.
    """
    native = sanitize_native_desc(source_offer.description or "", title=clean_name)
    if native:
        return native

    pmap = _param_map(params)
    bits: list[str] = []

    ptype = norm_ws(pmap.get("Тип", ""))
    if ptype:
        bits.append(ptype)

    for key in (
        "Функция",
        "Формат печати",
        "Разрешение",
        "Скорость печати ч/б",
        "Скорость печати цветной",
        "Диагональ",
        "Максимальное разрешение",
        "Тип матрицы",
        "Частота обновления",
        "Время отклика",
        "Модель процессора",
        "Серия процессора",
        "Оперативная память",
        "Объем жесткого диска",
        "Тип жесткого диска",
        "Операционная система",
        "Версия операционной системы",
        "Марка чипсета видеокарты",
        "Модель чипсета видеокарты",
        "Мощность (VA)",
        "Мощность (W)",
        "Форм-фактор",
        "Стабилизатор (AVR)",
        "Типовая продолжительность работы при 100% нагрузке, мин",
        "Выходные соединения",
        "Порты",
        "Беспроводная связь",
        "Беспроводные интерфейсы",
        "Цвет",
        "Технология печати",
        "Ресурс",
        "Объём",
        "Номер",
        "Применение",
        "Дополнительная информация",
        "Гарантия",
    ):
        val = norm_ws(pmap.get(key, ""))
        if val:
            bits.append(f"{key}: {val}")

    body = ". ".join(bits[:8]).strip()
    if body:
        if not body.endswith("."):
            body += "."
        return body

    if source_offer.category_path:
        return f"Категория поставщика: {norm_ws(source_offer.category_path)}."

    return ""


def build_offer_out(
    source_offer: SourceOffer,
    *,
    schema: dict[str, Any],
    policy: dict[str, Any],
) -> OfferOut | None:
    """Собрать один raw OfferOut."""
    prefix = norm_ws(schema.get("id_prefix") or schema.get("supplier_prefix") or "CP")
    placeholder_picture = norm_ws(schema.get("placeholder_picture") or "")
    vendor_blacklist = {str(x).casefold() for x in (schema.get("vendor_blacklist_casefold") or [])}

    fallback_vendor = norm_ws(
        (((policy.get("vendor_policy") or {}).get("neutral_fallback_vendor")) or "")
    )

    clean_name = normalize_name(source_offer.name)
    clean_vendor = normalize_vendor(
        source_offer.vendor,
        name=clean_name,
        params=source_offer.params,
        vendor_blacklist=vendor_blacklist,
        fallback_vendor=fallback_vendor,
    )
    clean_model = normalize_model(clean_name, source_offer.params)

    params = build_params_from_xml(source_offer, schema)
    params = extract_desc_fill_params(
        title=clean_name,
        desc_text=source_offer.description,
        existing_params=params,
    )
    params = _ensure_base_params(
        source_offer=source_offer,
        params=params,
        vendor=clean_vendor,
        model=clean_model,
    )
    params = apply_compat_cleanup(params)

    oid = build_offer_oid(source_offer.vendor_code, source_offer.raw_id, prefix=prefix)
    if not oid:
        return None

    pictures = collect_picture_urls(source_offer.picture_urls, placeholder_picture=placeholder_picture)
    available = normalize_available(source_offer.available_attr, source_offer.available_tag, source_offer.active)
    price_in = normalize_price_in(source_offer.price_text)

    native_desc = _build_native_desc(
        clean_name=clean_name,
        source_offer=source_offer,
        params=params,
    )

    out = OfferOut(
        oid=oid,
        available=available,
        name=clean_name,
        price=price_in,
        pictures=pictures,
        vendor=clean_vendor,
        params=[(norm_ws(p.name), norm_ws(p.value)) for p in params if norm_ws(p.name) and norm_ws(p.value)],
        native_desc=native_desc,
    )
    return out


def build_offers(
    source_offers: list[SourceOffer],
    *,
    schema: dict[str, Any],
    policy: dict[str, Any],
) -> tuple[list[OfferOut], BuildStats]:
    """Собрать список raw OfferOut и supplier stats."""
    out: list[OfferOut] = []
    stats = BuildStats(before=len(source_offers), after=0)

    placeholder_picture = norm_ws(schema.get("placeholder_picture") or "")
    for src in source_offers:
        offer = build_offer_out(src, schema=schema, policy=policy)
        if offer is None:
            stats.filtered_out += 1
            continue

        if not src.picture_urls:
            stats.missing_picture_count += 1
        if offer.pictures and placeholder_picture and offer.pictures[0] == placeholder_picture:
            stats.placeholder_picture_count += 1
        if not norm_ws(offer.vendor):
            stats.empty_vendor_count += 1

        out.append(offer)

    stats.after = len(out)
    return out, stats
