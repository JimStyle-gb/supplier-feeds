# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/builder.py

VTT builder layer.

Задача:
- принять сырой item из source.py;
- прогнать через supplier-layers до ИДЕАЛЬНОГО RAW;
- собрать OfferOut;
- не тащить в builder всю логику слоями одной кучей.
"""

from __future__ import annotations

from typing import Iterable, Sequence

from cs.core import OfferOut, compute_price, safe_int

from suppliers.vtt.compat import reconcile_vtt_compat
from suppliers.vtt.desc_clean import clean_vtt_native_desc
from suppliers.vtt.desc_extract import merge_missing_params_from_desc
from suppliers.vtt.normalize import normalize_raw_item, norm_spaces
from suppliers.vtt.params_page import normalize_vtt_page_params
from suppliers.vtt.pictures import ensure_picture_list


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def _dedupe_pairs(params: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for k, v in params:
        kk = norm_spaces(k)
        vv = norm_spaces(v)
        if not kk or not vv:
            continue
        sig = (kk.casefold(), vv.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((kk, vv))

    return out


def _merge_params(*blocks: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for block in blocks:
        if not block:
            continue
        out.extend(block)
    return _dedupe_pairs(out)


def _safe_price(supplier_price: object) -> int:
    """
    Общая проектная цена.
    Если supplier price битая/пустая — проектный фолбэк всё равно 100.
    """
    return int(compute_price(safe_int(supplier_price)))


def _is_valid_raw_item(raw: dict) -> bool:
    """
    Мягкая валидация сырого item до builder-логики.
    Не режем лишнего, только совсем пустой мусор.
    """
    if not isinstance(raw, dict):
        return False

    title = safe_str(raw.get("name"))
    if not title:
        return False

    # Для VTT article обязателен — это источник стабильного oid.
    article = safe_str(raw.get("article"))
    if not article:
        return False

    return True


def build_offer_from_raw(raw: dict, *, id_prefix: str = "VT") -> OfferOut | None:
    """
    Главная функция builder-layer.
    Возвращает supplier-clean OfferOut для RAW.
    """
    if not _is_valid_raw_item(raw):
        return None

    # 1) Базовая normalize-фаза
    item = normalize_raw_item(raw, id_prefix=id_prefix)

    oid = safe_str(item.get("oid"))
    name = norm_spaces(item.get("name"))
    vendor = norm_spaces(item.get("vendor"))
    article = safe_str(item.get("article"))
    category_code = safe_str(item.get("category_code"))

    if not oid or not name or not article:
        return None

    # 2) Pictures
    pictures = ensure_picture_list(item.get("pictures") or [], limit=8)

    # 3) Native description до RAW
    native_desc = clean_vtt_native_desc(
        title=name,
        meta_desc=item.get("description_meta") or "",
        body_desc=item.get("description_body") or "",
        native_desc=item.get("native_desc") or "",
    )

    # 4) Params from page/source -> schema cleanup -> canonical params
    params_page = normalize_vtt_page_params(
        item.get("params") or [],
        title=name,
        category_code=category_code,
        native_desc=native_desc,
    )

    # 5) only_fill_missing из description
    params_desc = merge_missing_params_from_desc(
        title=name,
        native_desc=native_desc,
        current_params=params_page,
    )

    # 6) compat/codes reconcile
    params_final = reconcile_vtt_compat(
        params_desc,
        title=name,
        native_desc=native_desc,
    )

    params_final = _dedupe_pairs(params_final)

    # 7) Цена
    price = _safe_price(item.get("supplier_price"))

    return OfferOut(
        oid=oid,
        available=bool(item.get("available", True)),
        name=name,
        price=price,
        pictures=pictures,
        vendor=vendor,
        params=params_final,
        native_desc=native_desc,
    )
