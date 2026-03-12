# -*- coding: utf-8 -*-
"""
Тонкий orchestrator для AkCent по новой supplier-layer архитектуре.

Логика:
- source.py        -> читает XML поставщика
- filtering.py     -> фильтрует ассортимент
- builder.py       -> собирает идеальный raw OfferOut
- quality_gate.py  -> проверяет финальный результат
- cs/core.py       -> только общие вещи

Важно:
- supplier-specific логика не тащится в core
- build-файл не монолитный
"""

from __future__ import annotations

import os
from typing import Any

import yaml

from cs.core import (
    get_public_vendor,
    next_run_at_hour,
    now_almaty,
    write_cs_feed,
    write_cs_feed_raw,
)
from suppliers.akcent.builder import build_offers
from suppliers.akcent.diagnostics import print_build_summary
from suppliers.akcent.filtering import filter_source_offers
from suppliers.akcent.quality_gate import run_quality_gate
from suppliers.akcent.source import fetch_source_root, iter_source_offers

SUPPLIER_NAME = "AkCent"
SUPPLIER_URL = "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"

OUT_FILE = "docs/akcent.yml"
RAW_OUT_FILE = "docs/raw/akcent.yml"
OUTPUT_ENCODING = "utf-8"

BUILD_AKCENT_VERSION = "build_akcent_v64_supplier_package"


def _config_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "suppliers", "akcent", "config")


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        obj = yaml.safe_load(f) or {}
    if not isinstance(obj, dict):
        raise ValueError(f"Bad YAML root in {path}: expected mapping")
    return obj


def load_policy_config() -> dict[str, Any]:
    return _load_yaml(os.path.join(_config_dir(), "policy.yml"))


def _schedule_hour_from_policy(policy_cfg: dict[str, Any]) -> int:
    try:
        return int(policy_cfg.get("next_run_hour_local", 2) or 2)
    except Exception:
        return 2


def build() -> None:
    # config
    policy_cfg = load_policy_config()
    schedule_hour = _schedule_hour_from_policy(policy_cfg)

    # source
    root = fetch_source_root(SUPPLIER_URL)
    source_offers = list(iter_source_offers(root))
    before = len(source_offers)

    # filtering
    filtered_offers, filter_report = filter_source_offers(source_offers)

    # builder -> ideal raw OfferOut
    out_offers, build_report = build_offers(filtered_offers)

    # timestamps
    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour=schedule_hour)

    # raw feed
    write_cs_feed_raw(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL,
        out_file=RAW_OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
    )

    # final feed
    write_cs_feed(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL,
        out_file=OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        public_vendor=get_public_vendor(SUPPLIER_NAME),
    )

    # supplier-side diagnostics
    print_build_summary(
        supplier=SUPPLIER_NAME,
        version=BUILD_AKCENT_VERSION,
        before=before,
        after=len(out_offers),
        filter_report=filter_report,
        build_report=build_report,
        out_file=OUT_FILE,
        raw_out_file=RAW_OUT_FILE,
    )

    # supplier-side quality gate
    run_quality_gate(
        out_file=OUT_FILE,
        raw_out_file=RAW_OUT_FILE,
        supplier=SUPPLIER_NAME,
        version=BUILD_AKCENT_VERSION,
    )


def main() -> None:
    build()


if __name__ == "__main__":
    main()
