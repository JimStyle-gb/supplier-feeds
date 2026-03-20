# -*- coding: utf-8 -*-
"""Path: scripts/build_vtt_probe.py

VTT temporary probe launcher.

v2:
- не валит workflow, если логин не прошёл, чтобы artifact всегда загружался;
- печатает пути к login_report.json и run_summary.json;
- оставляет ненулевой код только при настоящем крэше скрипта.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from suppliers.vtt.client import VTTClient
from suppliers.vtt.probe import ProbeConfig, run_vtt_probe


BUILD_VTT_VERSION = "build_vtt_probe_v2"

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
    max_pages = int((os.getenv("VTT_PROBE_MAX_PAGES") or "800").strip() or "800")
    delay_seconds = float((os.getenv("VTT_PROBE_DELAY") or "0.35").strip() or "0.35")
    max_product_html = int((os.getenv("VTT_PROBE_MAX_PRODUCT_HTML") or "50").strip() or "50")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    client = VTTClient(
        base_url=base_url,
        login=login,
        password=password,
        delay_seconds=delay_seconds,
    )

    summary: dict[str, object]
    try:
        summary = run_vtt_probe(
            client,
            ProbeConfig(
                out_dir=OUT_DIR,
                max_pages=max_pages,
                max_product_pages_to_save=max_product_html,
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

    login_report_path = OUT_DIR / "login_report.json"

    print("=" * 72)
    print("[VTT] probe summary")
    print("=" * 72)
    print("version:", BUILD_VTT_VERSION)
    print("base_url:", base_url)
    print("out_dir:", OUT_DIR)
    print("ok:", summary.get("ok"))
    print("stage:", summary.get("stage", "done"))
    print("pages_total:", summary.get("pages_total"))
    print("product_like_pages:", summary.get("product_like_pages"))
    print("category_like_pages:", summary.get("category_like_pages"))
    print("sample_products:", summary.get("sample_products"))
    print("discovered_endpoints:", summary.get("discovered_endpoints"))
    print("-" * 72)
    print("login_report:", login_report_path)
    print("run_summary:", summary_path)
    print("=" * 72)

    # Специально НЕ валим workflow на auth-fail, чтобы artifact успел загрузиться.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
