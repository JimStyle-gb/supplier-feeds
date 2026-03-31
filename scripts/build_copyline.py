# -*- coding: utf-8 -*-
"""
Path: scripts/build_copyline.py
CopyLine adapter under CS-template.

Что делает:
- читает supplier config;
- загружает индекс товаров;
- фильтрует ассортимент;
- собирает raw offers из page-payload;
- пишет raw/final feed;
- запускает supplier-side quality gate.
"""

from __future__ import annotations

import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, List

import yaml

from cs.core import get_public_vendor, next_run_dom_at_hour, now_almaty, write_cs_feed, write_cs_feed_raw
from suppliers.copyline.builder import build_offer_from_page
from suppliers.copyline.filtering import filter_product_index, load_filter_config
from suppliers.copyline.source import fetch_product_index, parse_product_page
from suppliers.copyline.quality_gate import run_quality_gate

SUPPLIER_NAME = "CopyLine"
SUPPLIER_URL_DEFAULT = os.getenv("SUPPLIER_URL", "https://copyline.kz/goods.html")
OUT_FILE = os.getenv("OUT_FILE", "docs/copyline.yml")
RAW_OUT_FILE = os.getenv("RAW_OUT_FILE", "docs/raw/copyline.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "utf-8") or "utf-8").strip() or "utf-8"
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6") or "6")
MAX_CRAWL_MINUTES = int(os.getenv("MAX_CRAWL_MINUTES", "60") or "60")
COPYLINE_FILTER_YML = os.getenv("COPYLINE_FILTER_YML", "scripts/suppliers/copyline/config/filter.yml")
COPYLINE_POLICY_YML = os.getenv("COPYLINE_POLICY_YML", "scripts/suppliers/copyline/config/policy.yml")
COPYLINE_QG_BASELINE = os.getenv("COPYLINE_QG_BASELINE", "scripts/suppliers/copyline/config/quality_gate_baseline.yml")
COPYLINE_QG_REPORT = os.getenv("COPYLINE_QG_REPORT", "docs/raw/copyline_quality_gate.txt")
BUILD_COPYLINE_VERSION = "build_copyline_v8_roles_cleanup"


def _read_yaml(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _load_policy_param_priority(path: str) -> tuple[str, ...]:
    data = _read_yaml(path)
    raw = data.get("param_priority") or []
    return tuple(str(x).strip() for x in raw if str(x).strip())


def _build_offers(filtered_index: list[dict]) -> list:
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
            if not offer or offer.oid in seen_oids:
                continue
            seen_oids.add(offer.oid)
            out_offers.append(offer)

    out_offers.sort(key=lambda o: o.oid)
    return out_offers


def _print_summary(*, before: int, out_offers: list, filter_report: dict, qg: dict) -> None:
    after = len(out_offers)
    in_true = sum(1 for o in out_offers if o.available)
    in_false = after - in_true

    print("=" * 72)
    print("[CopyLine] build summary")
    print("=" * 72)
    print(f"version: {BUILD_COPYLINE_VERSION}")
    print(f"before: {before}")
    print(f"after:  {after}")
    print(f"raw_out_file: {RAW_OUT_FILE}")
    print(f"out_file: {OUT_FILE}")
    print("-" * 72)
    print("filter_report:")
    for k, v in filter_report.items():
        print(f"  {k}: {v}")
    print("-" * 72)
    print(f"quality_gate_ok:   {qg.get('ok')}")
    print(f"quality_gate_report: {qg.get('report_path')}")
    print(f"availability_true:  {in_true}")
    print(f"availability_false: {in_false}")
    print("=" * 72)



def main() -> int:
    build_time = now_almaty().replace(tzinfo=None)
    next_run = next_run_dom_at_hour(build_time, 3, (1, 10, 20))

    index = fetch_product_index()
    before = len(index)

    filter_cfg = load_filter_config(COPYLINE_FILTER_YML)
    filtered_index, filter_report = filter_product_index(
        index,
        include_prefixes=filter_cfg.get("include_prefixes") or [],
    )

    out_offers = _build_offers(filtered_index)

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
        param_priority=_load_policy_param_priority(COPYLINE_POLICY_YML),
    )

    qg = run_quality_gate(
        feed_path=RAW_OUT_FILE,
        policy_path=COPYLINE_POLICY_YML,
        baseline_path=COPYLINE_QG_BASELINE,
        report_path=COPYLINE_QG_REPORT,
    )

    _print_summary(before=before, out_offers=out_offers, filter_report=filter_report, qg=qg)
    if not qg.get("ok", True):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
