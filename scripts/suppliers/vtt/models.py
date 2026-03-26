# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/models.py

VTT supplier layer — internal data models.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class VTTConfig:
    base_url: str
    start_url: str
    login_url: str
    login: str
    password: str
    timeout_s: int = 35
    listing_request_delay_ms: int = 6
    product_request_delay_ms: int = 0
    max_listing_pages: int = 5000
    max_workers: int = 20
    max_crawl_minutes: float = 90.0
    softfail: bool = False
    categories: list[str] = field(default_factory=list)
    allowed_title_prefixes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ProductIndexItem:
    url: str
    source_categories: list[str] = field(default_factory=list)
    listing_titles: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ParsedProductPage:
    url: str
    name: str
    vendor: str = ""
    sku: str = ""
    price_rub_raw: int = 0
    pictures: list[str] = field(default_factory=list)
    params: list[tuple[str, str]] = field(default_factory=list)
    description_meta: str = ""
    description_body: str = ""
    title_codes: list[str] = field(default_factory=list)
    source_categories: list[str] = field(default_factory=list)
    category_code: str = ""
    listing_titles: list[str] = field(default_factory=list)
