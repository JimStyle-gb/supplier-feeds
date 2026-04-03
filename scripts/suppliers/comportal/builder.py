# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/builder.py

ComPortal supplier layer — сборка raw offer.

Финальная микро-полировка:
- safe mutual enrichment сохранён;
- guard для "Модель" сохранён;
- type-aware prune слабых техпараметров сохранён;
- добавлена канонизация кривых supplier-ключей перед prune.

Что это чинит:
- "Емкость 1- го лотка" -> "емкость 1-го лотка"
- "Емкость 2- го лотка" -> "емкость 2-го лотка"
- "Емкость 3- го лотка" -> "емкость 3-го лотка"

После этого такие кривые вариации должны нормально отрезаться.
"""

from __future__ import annotations

from typing import Any
import re

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


_RECONCILE_KEYS = {
    "коды",
    "модель",
    "ресурс",
    "гарантия",
    "цвет",
}

_GLOBAL_DROP_PARAM_NAMES = {
    "серия",
}

_PRINT_DEVICE_DROP_PARAM_NAMES = {
    "объем памяти",
    "количество лотков",
    "емкость 1-го лотка",
    "емкость 2-го лотка",
    "емкость 3-го лотка",
}


def _param_map(params: list[ParamItem]) -> dict[str, str]:
    out: dict[str, str] = {}
    seen: set[str] = set()
    for p in params or []:
        name = norm_ws(p.name)
        value = norm_ws(p.value)
        if not name or not value:
            continue
        ncf = name.casefold()
        if ncf in seen:
            continue
        out[name] = value
        seen.add(ncf)
    return out


def _drop_param_casefold(params: list[ParamItem], name_to_drop: str) -> list[ParamItem]:
    target = norm_ws(name_to_drop).casefold()
    return [p for p in params if norm_ws(p.name).casefold() != target]


def _join_nonempty(parts: list[str], sep: str = ". ") -> str:
    vals = [norm_ws(x) for x in parts if norm_ws(x)]
    return sep.join(vals).strip()


def _append_param_line(bits: list[str], label: str, value: str) -> None:
    v = norm_ws(value)
    if v:
        bits.append(f"{label}: {v}")


def _finalize_desc(text: str) -> str:
    t = norm_ws(text)
    if t and not t.endswith("."):
        t += "."
    return t


def _param_value_score(name: str, value: str) -> int:
    ncf = norm_ws(name).casefold()
    v = norm_ws(value)
    if not v:
        return 0

    score = 0
    if len(v) >= 3:
        score += 1
    if any(ch.isdigit() for ch in v):
        score += 1
    if "#" in v or "/" in v:
        score += 1
    if len(v) >= 8:
        score += 1

    if ncf == "гарантия":
        if "мес" in v.casefold():
            score += 4
    elif ncf == "ресурс":
        if any(ch.isdigit() for ch in v):
            score += 3
        if "стр" in v.casefold():
            score += 1
    elif ncf in {"коды", "модель"}:
        if len(v) >= 5:
            score += 3
        if "#" in v or "/" in v or "-" in v:
            score += 2
    elif ncf == "цвет":
        if v.casefold() in {
            "чёрный", "черный", "жёлтый", "желтый", "голубой",
            "пурпурный", "серый", "белый", "синий", "красный", "зелёный", "зеленый",
        }:
            score += 3

    return score


def _model_reconcile_should_keep_old(old_value: str, new_value: str) -> bool:
    old_v = norm_ws(old_value)
    new_v = norm_ws(new_value)
    if not old_v or not new_v:
        return False

    old_cf = old_v.casefold()
    new_cf = new_v.casefold()

    if old_cf == new_cf:
        return True

    if new_cf.startswith(old_cf) and any(sep in new_v for sep in ("#", "/")):
        return True

    for sep in ("#", "/"):
        if sep in new_v:
            head = norm_ws(new_v.split(sep, 1)[0])
            if head.casefold() == old_cf:
                return True

    return False


def _merge_desc_enrichment(xml_params: list[ParamItem], desc_params: list[ParamItem]) -> list[ParamItem]:
    out = list(xml_params)
    index: dict[str, int] = {}

    for i, p in enumerate(out):
        ncf = norm_ws(p.name).casefold()
        if ncf and ncf not in index:
            index[ncf] = i

    for p in desc_params or []:
        name = norm_ws(p.name)
        value = norm_ws(p.value)
        if not name or not value:
            continue

        ncf = name.casefold()

        if ncf not in _RECONCILE_KEYS:
            if ncf not in index:
                out.append(ParamItem(name=name, value=value, source=p.source))
                index[ncf] = len(out) - 1
            continue

        if ncf not in index:
            out.append(ParamItem(name=name, value=value, source=p.source))
            index[ncf] = len(out) - 1
            continue

        old_idx = index[ncf]
        old_param = out[old_idx]

        if ncf == "модель" and _model_reconcile_should_keep_old(old_param.value, value):
            continue

        old_score = _param_value_score(old_param.name, old_param.value)
        new_score = _param_value_score(name, value)

        if new_score > old_score:
            out[old_idx] = ParamItem(name=old_param.name, value=value, source=p.source)

    return out


def _enrich_sparse_device_desc(bits: list[str], pmap: dict[str, str]) -> list[str]:
    strong_payload_count = max(0, len(bits) - 1)
    if strong_payload_count >= 3:
        return bits

    for key in ("Для бренда", "Модель", "Коды", "Гарантия"):
        val = norm_ws(pmap.get(key, ""))
        if not val:
            continue
        probe = f"{key}: {val}"
        if probe not in bits:
            bits.append(probe)

    return bits


def _desc_for_printing_device(pmap: dict[str, str]) -> str:
    bits: list[str] = []
    ptype = norm_ws(pmap.get("Тип", ""))
    if ptype:
        bits.append(ptype)
    _append_param_line(bits, "Формат печати", pmap.get("Формат печати", ""))
    _append_param_line(bits, "Разрешение", pmap.get("Разрешение", ""))
    _append_param_line(bits, "Скорость печати ч/б", pmap.get("Скорость печати ч/б", ""))
    _append_param_line(bits, "Скорость печати цветной", pmap.get("Скорость печати цветной", ""))
    _append_param_line(bits, "Порты", pmap.get("Порты", ""))
    _append_param_line(bits, "Технология печати", pmap.get("Технология печати", ""))
    _append_param_line(bits, "Гарантия", pmap.get("Гарантия", ""))
    bits = _enrich_sparse_device_desc(bits, pmap)
    return _finalize_desc(_join_nonempty(bits))


def _desc_for_monitor(pmap: dict[str, str]) -> str:
    bits: list[str] = ["Монитор"]
    _append_param_line(bits, "Диагональ", pmap.get("Диагональ", ""))
    _append_param_line(bits, "Максимальное разрешение", pmap.get("Максимальное разрешение", ""))
    _append_param_line(bits, "Тип матрицы", pmap.get("Тип матрицы", ""))
    _append_param_line(bits, "Частота обновления", pmap.get("Частота обновления", ""))
    _append_param_line(bits, "Время отклика", pmap.get("Время отклика", ""))
    _append_param_line(bits, "Порты", pmap.get("Порты", ""))
    _append_param_line(bits, "Гарантия", pmap.get("Гарантия", ""))
    bits = _enrich_sparse_device_desc(bits, pmap)
    return _finalize_desc(_join_nonempty(bits))


def _desc_for_computer(pmap: dict[str, str]) -> str:
    bits: list[str] = []
    ptype = norm_ws(pmap.get("Тип", ""))
    if ptype:
        bits.append(ptype)
    cpu = _join_nonempty([pmap.get("Серия процессора", ""), pmap.get("Модель процессора", "")], sep=" ")
    _append_param_line(bits, "Процессор", cpu)
    _append_param_line(bits, "Оперативная память", pmap.get("Оперативная память", ""))
    storage = _join_nonempty([pmap.get("Объем жесткого диска", ""), pmap.get("Тип жесткого диска", "")], sep=" ")
    _append_param_line(bits, "Накопитель", storage)
    _append_param_line(bits, "Диагональ", pmap.get("Диагональ", ""))
    _append_param_line(bits, "Максимальное разрешение", pmap.get("Максимальное разрешение", ""))
    os_name = _join_nonempty([pmap.get("Операционная система", ""), pmap.get("Версия операционной системы", "")], sep=" ")
    _append_param_line(bits, "ОС", os_name)
    gpu = _join_nonempty([pmap.get("Марка чипсета видеокарты", ""), pmap.get("Модель чипсета видеокарты", "")], sep=" ")
    _append_param_line(bits, "Видеокарта", gpu)
    _append_param_line(bits, "Гарантия", pmap.get("Гарантия", ""))
    bits = _enrich_sparse_device_desc(bits, pmap)
    return _finalize_desc(_join_nonempty(bits))


def _desc_for_power(pmap: dict[str, str]) -> str:
    bits: list[str] = []
    ptype = norm_ws(pmap.get("Тип", ""))
    if ptype:
        bits.append(ptype)
    power_pair = _join_nonempty(
        [
            f"{norm_ws(pmap.get('Мощность (VA)', ''))} VA" if norm_ws(pmap.get("Мощность (VA)", "")) else "",
            f"{norm_ws(pmap.get('Мощность (W)', ''))} W" if norm_ws(pmap.get("Мощность (W)", "")) else "",
        ],
        sep=" / ",
    )
    _append_param_line(bits, "Мощность", power_pair)
    _append_param_line(bits, "Форм-фактор", pmap.get("Форм-фактор", ""))
    _append_param_line(bits, "Стабилизатор (AVR)", pmap.get("Стабилизатор (AVR)", ""))
    _append_param_line(bits, "Время работы при 100% нагрузке, мин", pmap.get("Типовая продолжительность работы при 100% нагрузке, мин", ""))
    _append_param_line(bits, "Выходные соединения", pmap.get("Выходные соединения", ""))
    _append_param_line(bits, "Гарантия", pmap.get("Гарантия", ""))
    bits = _enrich_sparse_device_desc(bits, pmap)
    return _finalize_desc(_join_nonempty(bits))


def _desc_for_consumable(pmap: dict[str, str]) -> str:
    bits: list[str] = []
    ptype = norm_ws(pmap.get("Тип", ""))
    if ptype:
        bits.append(ptype)
    _append_param_line(bits, "Цвет", pmap.get("Цвет", ""))
    _append_param_line(bits, "Технология печати", pmap.get("Технология печати", ""))
    _append_param_line(bits, "Ресурс", pmap.get("Ресурс", ""))
    _append_param_line(bits, "Объём", pmap.get("Объём", ""))
    _append_param_line(bits, "Номер", pmap.get("Номер", ""))
    _append_param_line(bits, "Применение", pmap.get("Применение", ""))
    return _finalize_desc(_join_nonempty(bits))


def _canonical_param_name_for_prune(name: str) -> str:
    """
    Канонизация supplier-key только для prune-сравнения.
    """
    s = norm_ws(name).casefold()
    s = re.sub(r"\b(\d+)\s*-\s*го\b", r"\1-го", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _prune_low_value_params(params: list[ParamItem]) -> list[ParamItem]:
    """
    Точечная чистка слабополезных raw params.
    """
    pmap = _param_map(params)
    ptype = norm_ws(pmap.get("Тип", "")).casefold()

    drop_names = set(_GLOBAL_DROP_PARAM_NAMES)
    if ptype in {"мфу", "принтер", "сканер", "проектор", "широкоформатный принтер"}:
        drop_names |= set(_PRINT_DEVICE_DROP_PARAM_NAMES)

    out: list[ParamItem] = []
    for p in params or []:
        probe = _canonical_param_name_for_prune(p.name)
        if probe in drop_names:
            continue
        out.append(p)
    return out


def _build_native_desc(*, clean_name: str, source_offer: SourceOffer, params: list[ParamItem]) -> str:
    native = sanitize_native_desc(source_offer.description or "", title=clean_name)
    if native:
        return native
    pmap = _param_map(params)
    ptype = norm_ws(pmap.get("Тип", "")).casefold()

    if ptype in {"мфу", "принтер", "сканер", "проектор", "широкоформатный принтер"}:
        text = _desc_for_printing_device(pmap)
        if text:
            return text
    if ptype == "монитор":
        text = _desc_for_monitor(pmap)
        if text:
            return text
    if ptype in {"ноутбук", "моноблок", "настольный пк", "рабочая станция"}:
        text = _desc_for_computer(pmap)
        if text:
            return text
    if ptype in {"ибп", "стабилизатор", "батарея"}:
        text = _desc_for_power(pmap)
        if text:
            return text
    if ptype in {"картридж", "тонер", "расходный материал"}:
        text = _desc_for_consumable(pmap)
        if text:
            return text

    bits: list[str] = []
    if norm_ws(pmap.get("Тип", "")):
        bits.append(norm_ws(pmap.get("Тип", "")))
    for key in ("Для бренда", "Коды", "Модель", "Цвет", "Технология печати", "Ресурс", "Гарантия"):
        _append_param_line(bits, key, pmap.get(key, ""))
    body = _join_nonempty(bits)
    if body:
        return _finalize_desc(body)
    if source_offer.category_path:
        return f"Категория поставщика: {norm_ws(source_offer.category_path)}."
    return ""


def _ensure_base_params(*, source_offer: SourceOffer, params: list[ParamItem], vendor: str, model: str) -> list[ParamItem]:
    out = list(params)
    pmap = _param_map(out)
    if vendor and "Для бренда" not in pmap:
        out.append(ParamItem(name="Для бренда", value=vendor, source="normalize"))
    if model and "Модель" not in pmap:
        out.append(ParamItem(name="Модель", value=model, source="normalize"))
    if "Коды" not in pmap:
        if model:
            out.append(ParamItem(name="Коды", value=model, source="normalize"))
        elif source_offer.vendor_code:
            out.append(ParamItem(name="Коды", value=norm_ws(source_offer.vendor_code), source="source"))
    pmap = _param_map(out)
    if vendor and pmap.get("Для бренда") and pmap.get("Бренд"):
        out = _drop_param_casefold(out, "Бренд")
    return out


def build_offer_out(source_offer: SourceOffer, *, schema: dict[str, Any], policy: dict[str, Any]) -> OfferOut | None:
    prefix = norm_ws(schema.get("id_prefix") or schema.get("supplier_prefix") or "CP")
    placeholder_picture = norm_ws(schema.get("placeholder_picture") or "")
    vendor_blacklist = {str(x).casefold() for x in (schema.get("vendor_blacklist_casefold") or [])}
    fallback_vendor = norm_ws((((policy.get("vendor_policy") or {}).get("neutral_fallback_vendor")) or ""))

    clean_name = normalize_name(source_offer.name)
    clean_vendor = normalize_vendor(
        source_offer.vendor,
        name=clean_name,
        params=source_offer.params,
        description_text=source_offer.description,
        vendor_blacklist=vendor_blacklist,
        fallback_vendor=fallback_vendor,
    )
    clean_model = normalize_model(clean_name, source_offer.params)

    xml_params = build_params_from_xml(source_offer, schema)
    desc_hint_params = extract_desc_fill_params(
        title=clean_name,
        desc_text=source_offer.description,
        existing_params=[],
    )
    params = _merge_desc_enrichment(xml_params, desc_hint_params)
    params = _ensure_base_params(source_offer=source_offer, params=params, vendor=clean_vendor, model=clean_model)
    params = apply_compat_cleanup(params)
    params = _prune_low_value_params(params)

    oid = build_offer_oid(source_offer.vendor_code, source_offer.raw_id, prefix=prefix)
    if not oid:
        return None

    pictures = collect_picture_urls(source_offer.picture_urls, placeholder_picture=placeholder_picture)
    available = normalize_available(source_offer.available_attr, source_offer.available_tag, source_offer.active)
    price_in = normalize_price_in(source_offer.price_text)
    native_desc = _build_native_desc(clean_name=clean_name, source_offer=source_offer, params=params)

    return OfferOut(
        oid=oid,
        available=available,
        name=clean_name,
        price=price_in,
        pictures=pictures,
        vendor=clean_vendor,
        params=[(norm_ws(p.name), norm_ws(p.value)) for p in params if norm_ws(p.name) and norm_ws(p.value)],
        native_desc=native_desc,
    )


def build_offers(source_offers: list[SourceOffer], *, schema: dict[str, Any], policy: dict[str, Any]) -> tuple[list[OfferOut], BuildStats]:
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
