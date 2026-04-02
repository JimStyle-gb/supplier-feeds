# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/models.py

Внутренние модели supplier layer для ComPortal.
Архитектурно — тот же тип слоя, что и у готовых поставщиков:
- dataclass-модели для source/builder/diagnostics;
- без бизнес-логики.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ParamItem:
    """Один source param."""
    name: str
    value: str
    source: str = "xml"


@dataclass(slots=True)
class CategoryRecord:
    """Одна source category."""
    category_id: str
    name: str
    parent_id: str = ""
    path: str = ""
    root_id: str = ""


@dataclass(slots=True)
class SourceOffer:
    """Один сырой offer из source YML ComPortal."""
    raw_id: str
    vendor_code: str
    category_id: str
    category_name: str
    category_path: str
    category_root_id: str
    name: str
    available_attr: str
    available_tag: str
    vendor: str
    description: str
    price_text: str
    currency_id: str
    url: str
    active: str
    delivery: str
    picture_urls: list[str] = field(default_factory=list)
    params: list[ParamItem] = field(default_factory=list)
    offer_el: Any | None = None


@dataclass(slots=True)
class BuildStats:
    """Базовая supplier-статистика сборки."""
    before: int = 0
    after: int = 0
    filtered_out: int = 0
    missing_picture_count: int = 0
    placeholder_picture_count: int = 0
    empty_vendor_count: int = 0


__all__ = [
    "ParamItem",
    "CategoryRecord",
    "SourceOffer",
    "BuildStats",
]
