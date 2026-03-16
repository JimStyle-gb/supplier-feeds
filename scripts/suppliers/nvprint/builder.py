# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/nvprint/builder.py
NVPrint builder layer — собрать OfferOut из сырого item.
"""

from __future__ import annotations

from xml.etree import ElementTree as ET

from cs.core import OfferOut, compute_price
from suppliers.nvprint.filtering import include_by_name
from suppliers.nvprint.normalize import (
    cleanup_name_nvprint,
    cleanup_vendor_nvprint,
    make_oid,
    native_desc,
    supplier_name_from_item,
)
from suppliers.nvprint.params_xml import collect_params, extract_price
from suppliers.nvprint.pictures import collect_pictures
from suppliers.nvprint.source import pick_first_text



def build_offer_from_item(item: ET.Element, fix_mixed_ru_func) -> OfferOut | None:
    """Собрать один OfferOut без смены поведения монолита."""
    raw_name = supplier_name_from_item(item)
    name = cleanup_name_nvprint(raw_name, fix_mixed_ru_func)
    if not name:
        return None
    if not include_by_name(name):
        return None

    oid = make_oid(item, name)
    if not oid:
        return None

    available = True
    pin = extract_price(item)
    price = compute_price(pin)
    pics = collect_pictures(item)

    vendor = pick_first_text(item, ("vendor", "brand", "Brand", "Производитель"))
    if not vendor:
        vendor = pick_first_text(item, ("РазделМодели",))
    if not vendor:
        vendor = pick_first_text(item, ("РазделПрайса",))
    vendor = cleanup_vendor_nvprint(vendor, name)

    params = collect_params(item, fix_mixed_ru_func)
    desc = native_desc(item)

    return OfferOut(
        oid=oid,
        name=name,
        price=price,
        available=available,
        pictures=pics,
        vendor=vendor,
        params=params,
        native_desc=desc,
    )
