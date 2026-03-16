# -*- coding: utf-8 -*-
"""
Path: scripts/build_nvprint.py
NVPrint thin orchestrator v1 — supplier package split.
"""

from __future__ import annotations

import os
import sys

from cs.core import (
    get_public_vendor,
    next_run_dom_at_hour,
    now_almaty,
    write_cs_feed,
    write_cs_feed_raw,
)
from suppliers.nvprint.builder import build_offer_from_item
from suppliers.nvprint.filtering import fix_mixed_ru
from suppliers.nvprint.source import load_items


OUT_FILE = "docs/nvprint.yml"
OUTPUT_ENCODING = "utf-8"
BUILD_NVPRINT_VERSION = "build_nvprint_v1_supplier_package_split"



def main() -> int:
    """Главная сборка NVPrint."""
    url = (os.environ.get("NVPRINT_XML_URL") or "").strip()
    if not url:
        raise RuntimeError("NVPRINT_XML_URL пустой. Укажи URL в workflow env.")

    now = now_almaty()
    now_naive = now.replace(tzinfo=None)
    try:
        hour = int((os.environ.get("SCHEDULE_HOUR_ALMATY", "4") or "4").strip())
    except Exception:
        hour = 4
    next_run = next_run_dom_at_hour(now_naive, hour, (1, 10, 20))
    strict = (os.environ.get("NVPRINT_STRICT") or "").strip().lower() in ("1", "true", "yes")

    try:
        _, _, items = load_items(url, strict=strict)
    except RuntimeError as e:
        if str(e) == "SOFT_EXIT":
            return 0
        raise

    out_offers = []
    filtered_out = 0
    in_true = 0
    in_false = 0

    for item in items:
        offer = build_offer_from_item(item, fix_mixed_ru)
        if offer is None:
            filtered_out += 1
            continue
        if offer.available:
            in_true += 1
        else:
            in_false += 1
        out_offers.append(offer)

    out_offers.sort(key=lambda o: o.oid)
    public_vendor = get_public_vendor("NVPrint")

    write_cs_feed_raw(
        out_offers,
        supplier="NVPrint",
        supplier_url=url,
        out_file="docs/raw/nvprint.yml",
        build_time=now,
        next_run=next_run,
        before=len(items),
        encoding=OUTPUT_ENCODING,
        currency_id="KZT",
    )

    changed = write_cs_feed(
        out_offers,
        supplier="NVPrint",
        supplier_url=url,
        out_file=OUT_FILE,
        build_time=now,
        next_run=next_run,
        before=len(items),
        encoding=OUTPUT_ENCODING,
        public_vendor=public_vendor,
        currency_id="KZT",
        param_priority=None,
    )

    print(
        f"[build_nvprint] OK | version={BUILD_NVPRINT_VERSION} | offers_in={len(items)} | "
        f"offers_out={len(out_offers)} | filtered_out={filtered_out} | in_true={in_true} | "
        f"in_false={in_false} | changed={'yes' if changed else 'no'} | file={OUT_FILE}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
