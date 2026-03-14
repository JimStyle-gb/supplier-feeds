# -*- coding: utf-8 -*-
"""
Path: scripts/build_copyline.py
CopyLine adapter stage-2.

Что уже вынесено в supplier-layer:
- source.py
- filtering.py
- pictures.py
- normalize.py
- params_page.py

Следующие этапы:
- desc_clean.py
- desc_extract.py
- compat.py
- builder.py
- quality_gate.py
"""

from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Sequence, Tuple

from cs.core import (
    OfferOut,
    compute_price,
    get_public_vendor,
    next_run_dom_at_hour,
    now_almaty,
    write_cs_feed,
    write_cs_feed_raw,
)
from suppliers.copyline.filtering import filter_product_index, load_filter_config
from suppliers.copyline.normalize import normalize_source_basics
from suppliers.copyline.params_page import extract_page_params
from suppliers.copyline.pictures import full_only_if_present, prefer_full_product_pictures
from suppliers.copyline.source import fetch_product_index, parse_product_page

SUPPLIER_NAME = "CopyLine"
SUPPLIER_URL_DEFAULT = os.getenv("SUPPLIER_URL", "https://copyline.kz/goods.html")
OUT_FILE = os.getenv("OUT_FILE", "docs/copyline.yml")
RAW_OUT_FILE = os.getenv("RAW_OUT_FILE", "docs/raw/copyline.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "utf-8") or "utf-8").strip() or "utf-8"
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6") or "6")
MAX_CRAWL_MINUTES = int(os.getenv("MAX_CRAWL_MINUTES", "60") or "60")
COPYLINE_FILTER_YML = os.getenv("COPYLINE_FILTER_YML", "scripts/suppliers/copyline/config/filter.yml")



def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""



def _mk_oid(sku: str) -> str:
    sku = safe_str(sku)
    sku = re.sub(r"[^A-Za-z0-9\-\._/]", "", sku)
    return "CL" + sku



def _merge_params(existing: Sequence[Tuple[str, str]], add: Sequence[Tuple[str, str]]) -> list[Tuple[str, str]]:
    out: list[Tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def push(key: str, value: str) -> None:
        k = safe_str(key)
        v = safe_str(value)
        if not k or not v:
            return
        if k.isdigit():
            return
        sig = (k.casefold(), v.casefold())
        if sig in seen:
            return
        seen.add(sig)
        out.append((k, v))

    for k, v in existing or []:
        push(k, v)
    for k, v in add or []:
        push(k, v)
    return out



def main() -> int:
    build_time = now_almaty()
    next_run = next_run_dom_at_hour(build_time, 3, (1, 10, 20))

    index = fetch_product_index()
    before = len(index)

    filter_cfg = load_filter_config(COPYLINE_FILTER_YML)
    filtered_index, filter_report = filter_product_index(index, include_prefixes=filter_cfg.get("include_prefixes") or [])

    out_offers: List[OfferOut] = []
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

            sku = safe_str(page.get("sku"))
            if not sku:
                continue
            oid = _mk_oid(sku)
            if oid in seen_oids:
                continue
            seen_oids.add(oid)

            source_title = safe_str(page.get("title") or futures[fut].get("title"))
            if not source_title:
                continue

            page_desc = safe_str(page.get("desc"))
            page_params_raw = list(page.get("params") or [])

            basics = normalize_source_basics(
                title=source_title,
                sku=sku,
                description_text=page_desc,
                params=page_params_raw,
            )
            title = safe_str(basics.get("title") or source_title)
            vendor = safe_str(basics.get("vendor"))
            native_desc = safe_str(basics.get("description") or page_desc) or title
            model = safe_str(basics.get("model"))

            params = extract_page_params(title=title, description=page_desc, page_params=page_params_raw)
            if model:
                params = _merge_params(params, [("Модель", model)])
            if vendor and any(safe_str(k) == "Тип" and safe_str(v) in {"Картридж", "Тонер-картридж", "Драм-картридж", "Девелопер", "Чернила"} for k, v in params):
                params = _merge_params(params, [("Для бренда", vendor)])

            pictures = prefer_full_product_pictures(page.get("pics") or [])
            pictures = full_only_if_present(pictures)

            raw_price = int(page.get("price_raw") or 0)
            price = compute_price(raw_price)
            available = bool(page.get("available", True))

            out_offers.append(
                OfferOut(
                    oid=oid,
                    available=available,
                    name=title,
                    price=price,
                    pictures=pictures,
                    vendor=vendor,
                    params=params,
                    native_desc=native_desc,
                )
            )

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
    print("version: build_copyline_v3_split_source_filter_pictures_normalize_params")
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
