#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
scripts/build_alstyle.py — generator of docs/alstyle.yml from Al-Style supplier feed.

Processing steps (in order):
  1) Category filter by <categoryId> (supplier IDs).
  2) Move <available> value into offer@available attribute and remove <available> tag.
  3) Prune everything under <shop> before <offers> (keep <offers> and what follows).
  4) Remove from each <offer> the tags: <price>, <url>, <quantity>, <quantity_in_stock>.

Output: docs/alstyle.yml encoded as Windows-1251. Keeps overall structure except listed edits.
'''
from __future__ import annotations

import pathlib
import sys
import time
import xml.etree.ElementTree as ET

import requests
from requests.auth import HTTPBasicAuth

# ------------------------ Config ------------------------
SUPPLIER_URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"

# Hard-coded credentials (per user request)
USERNAME = "info@complex-solutions.kz"
PASSWORD = "Aa123456"

# Supplier category IDs as one CSV string
ALLOWED_CATEGORY_IDS_CSV = "3540,3541,3542,3543,3544,3545,3566,3567,3569,3570,3580,3688,3708,3721,3722,4889,4890,4895,5017,5075,5649,5710,5711,5712,5713,21279,21281,21291,21356,21367,21368,21369,21370,21371,21372,21451,21498,21500,21501,21572,21573,21574,21575,21576,21578,21580,21581,21583,21584,21585,21586,21588,21591,21640,21664,21665,21666,21698"
ALLOWED_CATEGORY_IDS = {x.strip() for x in ALLOWED_CATEGORY_IDS_CSV.split(",") if x.strip()}

# Output file and encoding
OUT_FILE = pathlib.Path("docs/alstyle.yml")
OUTPUT_ENCODING = "windows-1251"

# Network
TIMEOUT_S = 45
RETRY = 2
SLEEP_BETWEEN_RETRY = 2
HEADERS = {"User-Agent": "AlStyleFeedBot/1.0 (+github-actions; python-requests)"}

# ------------------------ Helpers ------------------------
def _ensure_dirs(path: pathlib.Path) -> None:
    """Create target dirs if missing."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _fetch(url: str) -> bytes | None:
    """Fetch feed: try w/o auth, then with Basic Auth."""
    # 1) No auth
    for attempt in range(1, RETRY + 2):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S)
            if r.status_code == 200 and r.content:
                return r.content
        except requests.RequestException:
            pass
        if attempt <= RETRY:
            time.sleep(SLEEP_BETWEEN_RETRY)

    # 2) Basic Auth
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


def _write_windows_1251(path: pathlib.Path, xml_body_unicode: str) -> None:
    """Write XML with windows-1251 declaration; out-of-range chars use numeric entities."""
    decl = '<?xml version="1.0" encoding="windows-1251"?>\n'
    data = (decl + xml_body_unicode).encode(OUTPUT_ENCODING, errors="xmlcharrefreplace")
    with open(path, "wb") as f:
        f.write(data)


# ------------------------ Category filter ------------------------
def _filter_offers_inplace(root: ET.Element) -> tuple[int, int, int]:
    """Remove offers whose <categoryId> is not in ALLOWED_CATEGORY_IDS. Return (total, kept, dropped)."""
    shop = root.find("./shop")
    if shop is None:
        return (0, 0, 0)

    offers_el = shop.find("offers")
    if offers_el is None:
        return (0, 0, 0)

    total = 0
    kept = 0
    dropped = 0

    for offer in list(offers_el):
        total += 1
        cat_el = offer.find("categoryId")
        cat_text = (cat_el.text or "").strip() if cat_el is not None else ""

        # normalize numeric strings (e.g., "021" -> "21")
        if cat_text.isdigit():
            cat_text = str(int(cat_text))

        if cat_text in ALLOWED_CATEGORY_IDS:
            kept += 1
        else:
            offers_el.remove(offer)
            dropped += 1

    return (total, kept, dropped)


# ------------------------ available -> attribute ------------------------
_TRUE_WORDS = {"true", "1", "yes", "y", "да", "есть", "в наличии", "наличие", "есть в наличии"}
_FALSE_WORDS = {"false", "0", "no", "n", "нет", "отсутствует", "нет в наличии", "под заказ", "ожидается"}

def _to_bool_text(v: str) -> str:
    s = (v or "").strip().lower()
    s = s.replace(":", " ").replace("\u00a0", " ").strip()
    if s in _TRUE_WORDS:
        return "true"
    if s in _FALSE_WORDS:
        return "false"
    if "true" in s or "да" in s:
        return "true"
    if "false" in s or "нет" in s or "под заказ" in s:
        return "false"
    return "false"


def _migrate_available_inplace(root: ET.Element) -> tuple[int, int, int, int]:
    """Move <available> into offer@available and drop the tag. Return (seen, set, overridden, removed)."""
    shop = root.find("./shop")
    if shop is None:
        return (0, 0, 0, 0)

    offers_el = shop.find("offers")
    if offers_el is None:
        return (0, 0, 0, 0)

    offers_seen = 0
    attrs_set = 0
    attrs_overridden = 0
    tags_removed = 0

    for offer in list(offers_el):
        offers_seen += 1
        av_el = offer.find("available")
        av_text = (av_el.text or "").strip() if av_el is not None else None
        new_attr = _to_bool_text(av_text) if av_text is not None else None

        if new_attr is not None:
            if "available" in offer.attrib:
                if offer.attrib.get("available") != new_attr:
                    attrs_overridden += 1
                offer.set("available", new_attr)
            else:
                offer.set("available", new_attr)
                attrs_set += 1

        if av_el is not None:
            offer.remove(av_el)
            tags_removed += 1

    return (offers_seen, attrs_set, attrs_overridden, tags_removed)


# ------------------------ Prune <shop> before <offers> ------------------------
def _prune_shop_before_offers(root: ET.Element) -> int:
    """Remove all children of <shop> before <offers>. Return count removed."""
    shop = root.find("./shop")
    if shop is None:
        return 0
    offers_el = shop.find("offers")
    if offers_el is None:
        return 0

    removed = 0
    for child in list(shop):
        if child is offers_el:
            break
        shop.remove(child)
        removed += 1
    return removed


# ------------------------ Strip tags from each offer ------------------------
STRIP_OFFER_TAGS = {"price", "url", "quantity", "quantity_in_stock"}

def _strip_offer_fields_inplace(root: ET.Element) -> int:
    """Remove specified child tags from each <offer>. Return total removed."""
    shop = root.find("./shop")
    if shop is None:
        return 0
    offers_el = shop.find("offers")
    if offers_el is None:
        return 0

    removed = 0
    for offer in list(offers_el):
        to_remove = [el for el in list(offer) if el.tag in STRIP_OFFER_TAGS]
        for el in to_remove:
            offer.remove(el)
            removed += 1
    return removed


# ------------------------ Main ------------------------
def main() -> int:
    print(">> Fetching supplier feed...")
    raw = _fetch(SUPPLIER_URL)
    if not raw:
        print("!! Failed to fetch supplier feed.", file=sys.stderr)
        return 2

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print(f"!! XML parse error: {e}", file=sys.stderr)
        return 3

    if root.tag.lower() != "yml_catalog":
        print("!! Root tag is not <yml_catalog>.", file=sys.stderr)
        return 4

    total, kept, dropped = _filter_offers_inplace(root)
    print(f">> Offers total: {total}, kept: {kept}, dropped: {dropped}")

    seen, set_cnt, overr_cnt, removed_av = _migrate_available_inplace(root)
    print(f">> Available migrated: seen={seen}, set={set_cnt}, overridden={overr_cnt}, tags_removed={removed_av}")

    pruned = _prune_shop_before_offers(root)
    print(f">> Shop prefix pruned: removed_nodes={pruned}")

    stripped = _strip_offer_fields_inplace(root)
    print(f">> Offer fields stripped: removed_tags_total={stripped}")

    xml_unicode = ET.tostring(root, encoding="unicode")
    _ensure_dirs(OUT_FILE)
    decl = '<?xml version="1.0" encoding="windows-1251"?>\n'
    data = (decl + xml_unicode).encode(OUTPUT_ENCODING, errors="xmlcharrefreplace")
    with open(OUT_FILE, "wb") as f:
        f.write(data)
    print(f">> Written: {OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
