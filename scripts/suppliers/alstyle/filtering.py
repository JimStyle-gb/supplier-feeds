# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/filtering.py

Только фильтрация ассортимента AlStyle.
"""

from __future__ import annotations

import re

from suppliers.alstyle.models import SourceOffer


def parse_id_set(env_value: str | None, fallback: set[str]) -> set[str]:
    if not env_value:
        return set(fallback)
    s = env_value.strip()
    if not s:
        return set(fallback)
    parts = re.split(r"[\s,;]+", s)
    out = {p.strip() for p in parts if p and p.strip()}
    return out or set(fallback)


def offer_passes_filter(source_offer: SourceOffer, include_ids: set[str]) -> bool:
    if not include_ids:
        return True
    return bool(source_offer.category_id and source_offer.category_id in include_ids)


def filter_source_offers(offers: list[SourceOffer], include_ids: set[str]) -> list[SourceOffer]:
    return [src for src in offers if offer_passes_filter(src, include_ids)]
