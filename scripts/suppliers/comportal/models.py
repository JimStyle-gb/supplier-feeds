# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/models.py
Typed payload contracts for ComPortal supplier-layer.
"""

from __future__ import annotations

from typing import TypedDict, List, Dict


class RawParam(TypedDict, total=False):
    """Сырой param из source YML."""
    name: str
    value: str


class CategoryRecord(TypedDict, total=False):
    """Категория поставщика."""
    id: str
    name: str
    parent_id: str
    path: str


class RawOffer(TypedDict, total=False):
    """Сырой offer после source.py."""
    id: str
    sku: str
    url: str
    title: str
    name: str
    vendor: str
    vendorCode: str
    categoryId: str
    currencyId: str
    available: bool
    price_raw: str
    pic: str
    pics: List[str]
    params: List[RawParam]
    desc: str

    raw_id: str
    raw_name: str
    raw_vendor: str
    raw_vendorCode: str
    raw_categoryId: str
    raw_category_name: str
    raw_category_path: str
    raw_price_text: str
    raw_currencyId: str
    raw_delivery: str
    raw_active: str
    raw_picture: str
    raw_pictures: List[str]
    raw_params: List[RawParam]
    raw_url: str


class CatalogPayload(TypedDict, total=False):
    """Полный source payload каталога."""
    source_url: str
    category_index: Dict[str, CategoryRecord]
    offers: List[RawOffer]


class FilterReport(TypedDict, total=False):
    """Отчёт category-first фильтра."""
    mode: str
    before: int
    after: int
    rejected_total: int
    allowed_category_count: int
    allowed_category_ids: List[str]
    excluded_root_ids: List[str]
    allowed_categories_report: List[Dict[str, object]]
    rejected_categories_report: List[Dict[str, object]]


class BuiltOffer(TypedDict, total=False):
    """Clean raw offer перед преобразованием в OfferOut."""
    id: str
    vendorCode: str
    name: str
    price: int
    picture: str
    pictures: List[str]
    vendor: str
    currencyId: str
    available: bool
    categoryId: str
    params: List[RawParam]
    native_desc: str
    url: str
    source_category_id: str
    source_category_name: str
    source_category_path: str
    model: str


__all__ = [
    "RawParam",
    "CategoryRecord",
    "RawOffer",
    "CatalogPayload",
    "FilterReport",
    "BuiltOffer",
]
