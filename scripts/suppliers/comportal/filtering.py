# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/filtering.py

Только фильтрация ассортимента ComPortal.
Единая роль файла как у других поставщиков, но supplier-логика своя:
- include по category ids;
- exclude по root ids (Акции / Уцененные).
"""

from __future__ import annotations

import re

from suppliers.comportal.models import SourceOffer


def parse_id_set(env_value: str | None, fallback: set[str]) -> set[str]:
    """Прочитать set ids из env или вернуть fallback."""
    if not env_value:
        return set(fallback)
    s = env_value.strip()
    if not s:
        return set(fallback)
    parts = re.split(r"[\s,;]+", s)
    out = {p.strip() for p in parts if p and p.strip()}
    return out or set(fallback)


def offer_passes_filter(
    source_offer: SourceOffer,
    include_ids: set[str],
    excluded_root_ids: set[str],
) -> bool:
    """Проверить, проходит ли offer фильтр."""
    if not source_offer.category_id:
        return False
    if include_ids and source_offer.category_id not in include_ids:
        return False
    if excluded_root_ids and source_offer.category_root_id in excluded_root_ids:
        return False
    return True


def filter_source_offers(
    offers: list[SourceOffer],
    include_ids: set[str],
    excluded_root_ids: set[str],
) -> list[SourceOffer]:
    """Отфильтровать source offers."""
    return [
        src
        for src in offers
        if offer_passes_filter(src, include_ids, excluded_root_ids)
    ]
