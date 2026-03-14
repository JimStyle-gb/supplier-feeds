# -*- coding: utf-8 -*-
"""
Path: scripts/build_copyline.py
CopyLine adapter stage-4.

Вынесено в supplier-layer:
- source.py
- filtering.py
- pictures.py
- normalize.py
- params_page.py
- desc_clean.py
- desc_extract.py
- compat.py
- builder.py
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List

from cs.core import get_public_vendor, next_run_dom_at_hour, now_almaty, write_cs_feed, write_cs_feed_raw
from suppliers.copyline.builder import build_offer_from_page
from suppliers.copyline.filtering import filter_product_index, load_filter_config
from suppliers.copyline.source import fetch_product_index, parse_product_page

SUPPLIER_NAME = "CopyLine"
SUPPLIER_URL_DEFAULT = os.getenv("SUPPLIER_URL", "https://copyline.kz/goods.html")
OUT_FILE = os.getenv("OUT_FILE", "docs/copyline.yml")
RAW_OUT_FILE = os.getenv("RAW_OUT_FILE", "docs/raw/copyline.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "utf-8") or "utf-8").strip() or "utf-8"
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6") or "6")
MAX_CRAWL_MINUTES = int(os.getenv("MAX_CRAWL_MINUTES", "60") or "60")
COPYLINE_FILTER_YML = os.getenv("COPYLINE_FILTER_YML", "scripts/suppliers/copyline/config/filter.yml")


def main() -> int:
    build_time = now_almaty()
    next_run = next_run_dom_at_hour(build_time, 3, (1, 10, 20))

    index = fetch_product_index()
    before = len(index)

    filter_cfg = load_filter_config(COPYLINE_FILTER_YML)
    filtered_index, filter_report = filter_product_index(index, include_prefixes=filter_cfg.get("include_prefixes") or [])

    out_offers: List = []
    seen_oids: set[str] = set()
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MINUTES)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(parse_product_page, item["url"]): item for item in filtered_index}
        for fut in as_completed(futures):
            if datetime.utcnow() > deadline:
                break
            page = fut.result()
            if not page:
                continue
            offer = build_offer_from_page(page, fallback_title=(futures[fut].get("title") or ""))
            if not offer:
                continue
            if offer.oid in seen_oids:
                continue
            seen_oids.add(offer.oid)
            out_offers.append(offer)

    out_offers.sort(key=lambda o: o.oid)

    write_cs_feed_raw(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL_DEFAULT,
        out_file=RAW_OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
    )

    public_vendor = get_public_vendor(SUPPLIER_NAME)
    write_cs_feed(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL_DEFAULT,
        out_file=OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        public_vendor=public_vendor,
        param_priority=(
            "Тип",
            "Для бренда",
            "Коды расходников",
            "Совместимость",
            "Технология печати",
            "Цвет",
            "Количество страниц (5% заполнение)",
            "Ресурс",
            "Модель",
        ),
    )

    after = len(out_offers)
    in_true = sum(1 for o in out_offers if o.available)
    in_false = after - in_true

    print("=" * 72)
    print("[CopyLine] build summary")
    print("=" * 72)
    print("version: build_copyline_v4_split_desc_compat_builder")
    print(f"before: {before}")
    print(f"after:  {after}")
    print(f"raw_out_file: {RAW_OUT_FILE}")
    print(f"out_file: {OUT_FILE}")
    print("-" * 72)
    print("filter_report:")
    for k, v in filter_report.items():
        print(f"  {k}: {v}")
    print("-" * 72)
    print(f"availability_true:  {in_true}")
    print(f"availability_false: {in_false}")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
