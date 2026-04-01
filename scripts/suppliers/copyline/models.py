"""Typed payload contracts for CopyLine supplier-layer."""

from __future__ import annotations

from typing import TypedDict


class ProductIndexItem(TypedDict, total=False):
    """Короткая карточка товара из списка поставщика."""
    sku: str
    url: str
    title: str
    price_raw: str
    available: bool
    pic: str
    pics: list[str]


class RawDescPair(TypedDict, total=False):
    """Сырая пара ключ-значение, поднятая из описания."""
    name: str
    value: str


class RawTableParam(TypedDict, total=False):
    """Сырой параметр, поднятый из таблицы товара."""
    name: str
    value: str


class ProductPagePayload(TypedDict, total=False):
    """Полный сырой payload страницы товара до semantic extraction."""
    sku: str
    url: str
    title: str
    raw_desc: str
    raw_desc_pairs: list[RawDescPair]
    raw_table_params: list[RawTableParam]
    desc: str
    params: list[dict[str, str]]
    pic: str
    pics: list[str]
    price_raw: str
    available: bool


class FilterReport(TypedDict, total=False):
    """Отчёт ассортиментного фильтра."""
    mode: str
    before: int
    after: int
    rejected_total: int
    allowed_prefix_count: int
    allowed_prefixes: list[str]


__all__ = [
    "ProductIndexItem",
    "RawDescPair",
    "RawTableParam",
    "ProductPagePayload",
    "FilterReport",
]
