#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ===============================
#  build_alstyle.py (v12, simplified without changing behavior)
#  Purpose: download Al-Style feed, filter & clean offers,
#  add AS prefix, rename purchase_price->price, reorder tags, save as cp1251.
# ===============================

from __future__ import annotations

import sys
import time
import re
import xml.etree.ElementTree as ET
from typing import Optional, Tuple
import pathlib
import requests
from requests.auth import HTTPBasicAuth

# ------------------------ Config ------------------------
SUPPLIER_URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"

# Credentials embedded by user request
USERNAME = "info@complex-solutions.kz"
PASSWORD = "Aa123456"

# Supplier categoryId list (CSV in one line)
ALLOWED_CATEGORY_IDS_CSV = (
    "3540,3541,3542,3543,3544,3545,3566,3567,3569,3570,3580,3688,3708,3721,3722,"
    "4889,4890,4895,5017,5075,5649,5710,5711,5712,5713,21279,21281,21291,21356,"
    "21367,21368,21369,21370,21371,21372,21451,21498,21500,21501,21572,21573,21574,"
    "21575,21576,21578,21580,21581,21583,21584,21585,21586,21588,21591,21640,21664,"
    "21665,21666,21698"
)
ALLOWED_CATEGORY_IDS = {x.strip() for x in ALLOWED_CATEGORY_IDS_CSV.split(",") if x.strip()}

# Param names to delete (normalized compare, case-insensitive)
PARAMS_TO_DROP = {
    "артикул",
    "благотворительность",
    "код тн вэд",
    "код товара kaspi",
    "новинка",
    "снижена цена",
    "штрихкод",
    "штрих-код",
    "назначение",
    "объем",
    "объём",
}

# Offer-level tags to remove (do NOT remove <price>)
STRIP_OFFER_TAGS = {"url", "quantity", "quantity_in_stock"}

# Desired order inside <offer>
OFFER_TAG_ORDER = ["categoryId", "vendorCode", "name", "price", "picture", "vendor", "currencyId", "description", "param"]

# Network
TIMEOUT_S = 45
RETRY = 2
SLEEP_BETWEEN_RETRY = 2
HEADERS = {"User-Agent": "AlStyleFeedBot/1.0 (+github-actions; python-requests)"}

# Output
OUT_FILE = pathlib.Path("docs/alstyle.yml")
OUTPUT_ENCODING = "windows-1251"


# ------------------------ Utils ------------------------
def _ensure_dirs(path: pathlib.Path) -> None:
    """Ensure destination directories exist."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _offers(root: ET.Element) -> Optional[ET.Element]:
    """Return <offers> node or None."""
    shop = root.find("./shop")
    return None if shop is None else shop.find("offers")


def _fetch(url: str) -> Optional[bytes]:
    """Download feed: first without auth, then with BasicAuth. Return bytes or None."""
    for attempt in range(1, RETRY + 2):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S)
            if r.status_code == 200 and r.content:
                return r.content
        except requests.RequestException:
            pass
        if attempt <= RETRY:
            time.sleep(SLEEP_BETWEEN_RETRY)

    auth = HTTPBasicAuth(USERNAME, PASSWORD)
    for attempt in range(1, RETRY + 2):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S, auth=auth)
            if r.status_code == 200 and r.content:
                return r.content
        except requests.RequestException:
            pass
        if attempt <= RETRY:
            time.sleep(SLEEP_BETWEEN_RETRY)
    return None


def _write_windows_1251(path: pathlib.Path, xml_unicode: str) -> None:
    """Write XML with cp1251 header; out-of-range chars -> numeric entities."""
    decl = '<?xml version="1.0" encoding="windows-1251"?>\n'
    data = (decl + xml_unicode).encode(OUTPUT_ENCODING, errors="xmlcharrefreplace")
    with open(path, "wb") as f:
        f.write(data)


def _norm_param_name(name: str) -> str:
    """Normalize param name: lowercase, collapse spaces, strip trailing , . ; :"""
    s = (name or "").replace("\u00A0", " ").strip().lower()
    s = re.sub(r"[,.;:]+$", "", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


# ------------------------ Steps ------------------------
def step_filter_by_category(root: ET.Element) -> tuple[int, int, int]:
    """Drop <offer> where <categoryId> not in ALLOWED_CATEGORY_IDS. Return total, kept, dropped."""
    offers = _offers(root)
    if offers is None:
        return (0, 0, 0)

    total = kept = dropped = 0
    for offer in list(offers):
        total += 1
        cat_el = offer.find("categoryId")
        cat = (cat_el.text or "").strip() if cat_el is not None else ""
        if cat.isdigit():
            cat = str(int(cat))  # "021" -> "21"
        if cat in ALLOWED_CATEGORY_IDS:
            kept += 1
        else:
            offers.remove(offer)
            dropped += 1
    return (total, kept, dropped)


_TRUE_WORDS = {"true", "1", "yes", "y", "да", "есть", "в наличии", "наличие", "есть в наличии"}
_FALSE_WORDS = {"false", "0", "no", "n", "нет", "отсутствует", "нет в наличии", "под заказ", "ожидается"}

def _to_bool_text(v: str) -> str:
    """Normalize string to 'true'/'false' for offer@available (simple heuristic)."""
    s = (v or "").strip().lower().replace(":", " ").replace("\u00A0", " ")
    if s in _TRUE_WORDS or "true" in s or "да" in s:
        return "true"
    if s in _FALSE_WORDS or "false" in s or "нет" in s or "под заказ" in s:
        return "false"
    return "false"  # safe default


def step_migrate_available(root: ET.Element) -> tuple[int, int, int, int]:
    """Move <available> into offer@available and remove the tag. Return seen, set, overridden, removed."""
    offers = _offers(root)
    if offers is None:
        return (0, 0, 0, 0)

    seen = set_cnt = overridden = removed = 0
    for offer in list(offers):
        seen += 1
        av_el = offer.find("available")
        av_text = (av_el.text or "").strip() if av_el is not None else None
        if av_text is not None:
            new_val = _to_bool_text(av_text)
            if offer.get("available") and offer.get("available") != new_val:
                overridden += 1
            elif not offer.get("available"):
                set_cnt += 1
            offer.set("available", new_val)
            offer.remove(av_el)
            removed += 1
    return (seen, set_cnt, overridden, removed)


def step_prune_shop_prefix(root: ET.Element) -> int:
    """Remove all children of <shop> that appear before <offers>. Return count removed."""
    shop = root.find("./shop")
    if shop is None:
        return 0
    offers = shop.find("offers")
    if offers is None:
        return 0
    removed = 0
    for child in list(shop):
        if child is offers:
            break
        shop.remove(child)
        removed += 1
    return removed


def step_strip_offer_fields(root: ET.Element) -> int:
    """Delete unwanted tags from each <offer>. Return count of removed tags."""
    offers = _offers(root)
    if offers is None:
        return 0
    removed = 0
    for offer in list(offers):
        for el in list(offer):
            if el.tag in STRIP_OFFER_TAGS:
                offer.remove(el)
                removed += 1
    return removed


def step_strip_params_by_name(root: ET.Element) -> int:
    """Delete <param name="..."> when normalized name is in PARAMS_TO_DROP. Return count removed."""
    offers = _offers(root)
    if offers is None:
        return 0
    bad = {_norm_param_name(x) for x in PARAMS_TO_DROP}
    removed = 0
    for offer in list(offers):
        for p in list(offer.findall("param")):
            nm = p.attrib.get("name") or ""
            if _norm_param_name(nm) in bad:
                offer.remove(p)
                removed += 1
    return removed


def step_prefix_vendorcode_and_sync_id(root: ET.Element, prefix: str = "AS") -> tuple[int, int]:
    """Prefix <vendorCode> with AS (always), and set offer@id to the same value. Return (offers_updated, changed_count)."""
    offers = _offers(root)
    if offers is None:
        return (0, 0)
    offers_upd = vc_changed = 0
    for offer in list(offers):
        vc_el = offer.find("vendorCode")
        if vc_el is None:
            continue
        old = (vc_el.text or "").strip()
        new = f"{prefix}{old}"
        if new != old:
            vc_el.text = new
            vc_changed += 1
        offer.set("id", new)
        offers_upd += 1
    return (offers_upd, vc_changed)


def step_rename_purchase_price(root: ET.Element) -> int:
    """Rename <purchase_price> to <price>. If <price> existed, remove it first, then create from purchase_price."""
    offers = _offers(root)
    if offers is None:
        return 0
    converted = 0
    for offer in list(offers):
        pp_list = offer.findall("purchase_price")
        if not pp_list:
            continue
        # remove all existing <price> to avoid duplicates
        for old_price in offer.findall("price"):
            offer.remove(old_price)
        # replace each <purchase_price> with <price>
        for pp in pp_list:
            new_price = ET.Element("price")
            new_price.text = pp.text or ""
            for k, v in pp.attrib.items():
                new_price.set(k, v)
            offer.append(new_price)
            offer.remove(pp)
            converted += 1
    return converted


def step_reorder_offer_children(root: ET.Element) -> int:
    """Reorder children of each <offer> according to OFFER_TAG_ORDER. Others go to the end with original order."""
    offers = _offers(root)
    if offers is None:
        return 0
    processed = 0
    order = OFFER_TAG_ORDER
    for offer in list(offers):
        children = list(offer)
        if not children:
            continue
        buckets = {}
        for ch in children:
            buckets.setdefault(ch.tag, []).append(ch)
        new_children = []
        for t in order:
            if t in buckets:
                new_children.extend(buckets.pop(t))
        for ch in children:
            lst = buckets.get(ch.tag)
            if lst:
                new_children.append(lst.pop(0))
                if not lst:
                    buckets.pop(ch.tag, None)
        for ch in list(offer):
            offer.remove(ch)
        offer.extend(new_children)
        processed += 1
    return processed


# ------------------------ Main ------------------------
def main() -> int:
    print(">> Downloading supplier feed...")
    raw = _fetch(SUPPLIER_URL)
    if not raw:
        print("!! Failed to download supplier feed.", file=sys.stderr)
        return 2

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print(f"!! XML parse error: {e}", file=sys.stderr)
        return 3

    if root.tag.lower() != "yml_catalog":
        print("!! Root tag is not <yml_catalog>.", file=sys.stderr)
        return 4

    total, kept, dropped = step_filter_by_category(root)
    print(f">> Offers total: {total}, kept: {kept}, dropped: {dropped}")

    seen, set_cnt, overr_cnt, removed_av = step_migrate_available(root)
    print(f">> Available migrated: seen={seen}, set={set_cnt}, overridden={overr_cnt}, tags_removed={removed_av}")

    pruned = step_prune_shop_prefix(root)
    print(f">> Shop prefix pruned: removed_nodes={pruned}")

    stripped = step_strip_offer_fields(root)
    print(f">> Offer fields stripped: removed_tags_total={stripped}")

    params_removed = step_strip_params_by_name(root)
    print(f">> Params removed by name: {params_removed}")

    offers_upd, vcodes_chg = step_prefix_vendorcode_and_sync_id(root, prefix="AS")
    print(f">> VendorCode prefixed and id synced: offers={offers_upd}, vendorCodes_changed={vcodes_chg}")

    converted = step_rename_purchase_price(root)
    print(f">> purchase_price->price converted: {converted}")

    reordered = step_reorder_offer_children(root)
    print(f">> Offers reordered: {reordered}")

    xml_unicode = ET.tostring(root, encoding="unicode")
    _ensure_dirs(OUT_FILE)
    _write_windows_1251(OUT_FILE, xml_unicode)
    print(f">> Written: {OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
