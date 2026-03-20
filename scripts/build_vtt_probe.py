# -*- coding: utf-8 -*-
"""Path: scripts/build_vtt_probe.py

VTT temporary full-inventory probe launcher.

v5:
- extracts full visible supplier inventory first;
- does NOT filter goods by business prefixes at extraction stage;
- writes inventory maps and summaries for later manual assortment decision.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from suppliers.vtt.client import VTTClient
from suppliers.vtt.probe import ProbeConfig, run_vtt_probe

BUILD_VTT_VERSION = "build_vtt_probe_v5"

BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "docs" / "debug" / "vtt_probe"


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"Missing required env: {name}")
    return value


def _safe_json_write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    base_url = (os.getenv("VTT_BASE_URL") or "https://b2b.vtt.ru/").strip()
    login = _require_env("VTT_LOGIN")
    password = _require_env("VTT_PASSWORD")
    max_pages = int((os.getenv("VTT_PROBE_MAX_PAGES") or "2500").strip() or "2500")
    delay_seconds = float((os.getenv("VTT_PROBE_DELAY") or "0.35").strip() or "0.35")
    max_product_html = int((os.getenv("VTT_PROBE_MAX_PRODUCT_HTML") or "120").strip() or "120")
    max_candidate_fetch = int((os.getenv("VTT_PROBE_MAX_PRODUCT_FETCH") or "10000").strip() or "10000")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    client = VTTClient(
        base_url=base_url,
        login=login,
        password=password,
        delay_seconds=delay_seconds,
    )

    try:
        summary = run_vtt_probe(
            client,
            ProbeConfig(
                out_dir=OUT_DIR,
                max_pages=max_pages,
                max_product_pages_to_save=max_product_html,
                max_candidate_fetch=max_candidate_fetch,
            ),
        )
    except Exception as exc:  # noqa: BLE001
        summary = {
            "ok": False,
            "stage": "crash",
            "out_dir": str(OUT_DIR),
            "message": str(exc),
            "version": BUILD_VTT_VERSION,
        }
        _safe_json_write(OUT_DIR / "run_summary.json", summary)
        print("=" * 72)
        print("[VTT] probe summary")
        print("=" * 72)
        print("version:", BUILD_VTT_VERSION)
        print("base_url:", base_url)
        print("out_dir:", OUT_DIR)
        print("ok:", False)
        print("stage:", "crash")
        print("run_summary:", OUT_DIR / "run_summary.json")
        print("=" * 72)
        print(f"[VTT] probe failed hard: {exc}", file=sys.stderr)
        return 1

    summary["version"] = BUILD_VTT_VERSION
    summary_path = OUT_DIR / "run_summary.json"
    _safe_json_write(summary_path, summary)

    print("=" * 72)
    print("[VTT] probe summary")
    print("=" * 72)
    print("version:", BUILD_VTT_VERSION)
    print("base_url:", base_url)
    print("out_dir:", OUT_DIR)
    print("ok:", summary.get("ok"))
    print("stage:", summary.get("stage"))
    print("pages_total:", summary.get("pages_total"))
    print("candidate_product_urls:", summary.get("candidate_product_urls"))
    print("inventory_items_fetched:", summary.get("inventory_items_fetched"))
    print("product_confident_items:", summary.get("product_confident_items"))
    print("with_price:", summary.get("with_price"))
    print("with_images:", summary.get("with_images"))
    print("with_params:", summary.get("with_params"))
    print("with_description:", summary.get("with_description"))
    print("with_codes:", summary.get("with_codes"))
    print("with_compat:", summary.get("with_compat"))
    print("-" * 72)
    print("run_summary:", summary_path)
    print("inventory_summary:", OUT_DIR / "inventory_summary.json")
    print("field_coverage:", OUT_DIR / "field_coverage.json")
    print("title_prefix_1word_top200:", OUT_DIR / "title_prefix_1word_top200.json")
    print("title_prefix_2word_top200:", OUT_DIR / "title_prefix_2word_top200.json")
    print("category_stats:", OUT_DIR / "category_stats.json")
    print("brand_stats:", OUT_DIR / "brand_stats.json")
    print("=" * 72)

    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
