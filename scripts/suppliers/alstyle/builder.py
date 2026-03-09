# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/builder.py

AlStyle supplier layer — сборка raw offer (stage 6).

Что делает:
- собирает один OfferOut из SourceOffer;
- держит последовательность supplier-stage pipeline в одном месте;
- XML params имеют приоритет над desc-derived params;
- build_alstyle.py остаётся только orchestrator'ом.
"""

from __future__ import annotations

from cs.core import OfferOut
from cs.pricing import compute_price
from cs.util import norm_ws
from suppliers.alstyle.desc_clean import (
    _align_desc_model_from_name,
    _dedupe_desc_leading_title,
    _sanitize_native_desc,
)
from suppliers.alstyle.desc_extract import extract_desc_spec_pairs
from suppliers.alstyle.models import SourceOffer
from suppliers.alstyle.normalize import (
    build_offer_oid,
    normalize_available,
    normalize_name,
    normalize_price_in,
    normalize_vendor,
)
from suppliers.alstyle.params_xml import collect_xml_params
from suppliers.alstyle.pictures import collect_picture_urls



def merge_params(
    xml_params: list[tuple[str, str]],
    desc_params: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    XML params всегда приоритетнее.
    Description-derived params только дополняют.
    """
    out: list[tuple[str, str]] = []
    seen_key: set[str] = set()
    seen_pair: set[tuple[str, str]] = set()

    for k, v in xml_params:
        k2 = norm_ws(k)
        v2 = norm_ws(v)
        if not k2 or not v2:
            continue
        out.append((k2, v2))
        seen_key.add(k2.casefold())
        seen_pair.add((k2.casefold(), v2.casefold()))

    for k, v in desc_params:
        k2 = norm_ws(k)
        v2 = norm_ws(v)
        if not k2 or not v2:
            continue
        if k2.casefold() in seen_key:
            continue
        sig = (k2.casefold(), v2.casefold())
        if sig in seen_pair:
            continue
        out.append((k2, v2))
        seen_key.add(k2.casefold())
        seen_pair.add(sig)

    return out



def build_offer(
    src: SourceOffer,
    *,
    schema_cfg: dict,
    vendor_blacklist: set[str],
    placeholder_picture: str,
    id_prefix: str = "AS",
) -> tuple[OfferOut | None, bool]:
    raw_id = norm_ws(src.raw_id)
    name = normalize_name(src.name)
    if not raw_id or not name:
        return None, False

    oid = build_offer_oid(raw_id, prefix=id_prefix)
    available = normalize_available(src.available_attr, src.available_tag)
    pictures = collect_picture_urls(src.picture_urls, placeholder_picture=placeholder_picture)
    vendor = normalize_vendor(src.vendor, vendor_blacklist=vendor_blacklist)

    desc_src = _sanitize_native_desc(src.description or "")
    desc_src = _align_desc_model_from_name(name, desc_src)
    desc_src = _dedupe_desc_leading_title(name, desc_src)

    xml_params = collect_xml_params(src.offer_el, schema_cfg) if src.offer_el is not None else []
    desc_params = extract_desc_spec_pairs(desc_src, schema_cfg)
    params = merge_params(xml_params, desc_params)

    price_in = normalize_price_in(src.purchase_price_text, src.price_text)
    price = compute_price(price_in)

    offer = OfferOut(
        oid=oid,
        available=available,
        name=name,
        price=price,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=desc_src,
    )
    return offer, available



def build_offers(
    source_offers: list[SourceOffer],
    *,
    schema_cfg: dict,
    vendor_blacklist: set[str],
    placeholder_picture: str,
    id_prefix: str = "AS",
) -> tuple[list[OfferOut], int, int]:
    out: list[OfferOut] = []
    in_true = 0
    in_false = 0

    for src in source_offers:
        offer, available = build_offer(
            src,
            schema_cfg=schema_cfg,
            vendor_blacklist=vendor_blacklist,
            placeholder_picture=placeholder_picture,
            id_prefix=id_prefix,
        )
        if offer is None:
            continue
        if available:
            in_true += 1
        else:
            in_false += 1
        out.append(offer)

    out.sort(key=lambda x: x.oid)
    return out, in_true, in_false
