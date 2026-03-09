# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/normalize.py

Базовая supplier-нормализация полей AlStyle.
"""

from __future__ import annotations

import re

from cs.util import norm_ws, safe_int


def normalize_name(name: str) -> str:
    s = norm_ws(name)
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    return s


def build_offer_oid(raw_id: str, *, prefix: str) -> str:
    rid = norm_ws(raw_id)
    if not rid:
        return ""
    if rid.upper().startswith(prefix.upper()):
        return rid
    return f"{prefix}{rid}"


def normalize_available(available_attr: str, available_tag: str) -> bool:
    av_attr = (available_attr or "").strip().lower()
    if av_attr in ("true", "1", "yes"):
        return True
    if av_attr in ("false", "0", "no"):
        return False
    return (available_tag or "").strip().lower() in ("true", "1", "yes")


def normalize_vendor(vendor: str, *, vendor_blacklist: set[str]) -> str:
    s = norm_ws(vendor)
    if s and s.casefold() in vendor_blacklist:
        return ""
    return s


def normalize_price_in(purchase_price_text: str, price_text: str) -> int | None:
    price_in = safe_int(purchase_price_text)
    if price_in is None:
        price_in = safe_int(price_text)
    return price_in
