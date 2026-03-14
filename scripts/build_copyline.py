# -*- coding: utf-8 -*-
"""
Path: scripts/build_copyline.py
CopyLine adapter stage-1.

Что уже вынесено в supplier-layer:
- source.py
- filtering.py
- pictures.py

Пока это ещё не финальная alstyle-архитектура.
Следующими этапами нужно вынести normalize/params/desc/builder/quality_gate.
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


def _derive_kind(title: str) -> str:
    """Минимальный supplier-kind по префиксу title."""
    t = safe_str(title).lower()
    if not t:
        return ""
    if t.startswith("тонер-картридж") or t.startswith("тонер картридж"):
        return "Тонер-картридж"
    if t.startswith("картридж"):
        return "Картридж"
    if t.startswith("кабель сетевой"):
        return "Кабель сетевой"
    if t.startswith("термоблок"):
        return "Термоблок"
    if t.startswith("термоэлемент"):
        return "Термоэлемент"
    if t.startswith("девелопер") or t.startswith("developer"):
        return "Девелопер"
    if t.startswith("драм") or t.startswith("drum"):
        return "Драм-картридж"
    if t.startswith("чернила"):
        return "Чернила"
    return ""


def _merge_params(existing: Sequence[Tuple[str, str]], add: Sequence[Tuple[str, str]]) -> list[Tuple[str, str]]:
    """Склеить params без дублей."""
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

            name = safe_str(page.get("title") or futures[fut].get("title"))
            if not name:
                continue

            params = list(page.get("params") or [])
            kind = _derive_kind(name)
            if kind:
                params = _merge_params(params, [("Тип", kind)])

            pictures = prefer_full_product_pictures(page.get("pics") or [])
            pictures = full_only_if_present(pictures)

            raw_price = int(page.get("price_raw") or 0)
            price = compute_price(raw_price)

            native_desc = safe_str(page.get("desc")) or name
            available = bool(page.get("available", True))

            out_offers.append(
                OfferOut(
                    oid=oid,
                    available=available,
                    name=name,
                    price=price,
                    pictures=pictures,
                    vendor="",  # vendor пока ещё будет дожиматься ядром; позже перенесём в normalize.py.
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
        param_priority=("Тип", "Технология печати", "Цвет", "Коды расходников", "Совместимость", "Ресурс"),
    )

    after = len(out_offers)
    in_true = sum(1 for o in out_offers if o.available)
    in_false = after - in_true

    print("=" * 72)
    print("[CopyLine] build summary")
    print("=" * 72)
    print(f"version: build_copyline_v2_stage1_split_source_filter_pictures")
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
