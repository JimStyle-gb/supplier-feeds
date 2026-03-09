# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/models.py

Внутренние модели supplier layer для AlStyle.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ParamItem:
    name: str
    value: str
    source: str = "xml"


@dataclass(slots=True)
class SourceOffer:
    raw_id: str
    category_id: str
    name: str
    available_attr: str
    available_tag: str
    vendor: str
    description: str
    purchase_price_text: str
    price_text: str
    picture_urls: list[str] = field(default_factory=list)
    offer_el: Any | None = None


@dataclass(slots=True)
class BuildStats:
    before: int = 0
    after: int = 0
    filtered_out: int = 0
    missing_picture_count: int = 0
    placeholder_picture_count: int = 0
    watch_hits: int = 0
