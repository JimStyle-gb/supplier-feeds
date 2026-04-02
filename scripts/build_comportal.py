# -*- coding: utf-8 -*-
"""
Path: scripts/build_comportal.py

ComPortal adapter (CP) — thin orchestrator under CS-template.

Что делает:
- грузит supplier config: filter / schema / policy;
- читает исходный YML поставщика;
- прогоняет source -> filtering -> builder;
- пишет raw feed;
- пишет final feed;
- пишет watch-report;
- запускает supplier-side quality gate.

Важно:
- supplier-specific логика остаётся только в suppliers/comportal/*;
- build_comportal.py не должен знать regex-логику ComPortal;
- orchestrator остаётся тонким и шаблонным относительно других поставщиков.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from cs.core import write_cs_feed, write_cs_feed_raw
from cs.meta import next_run_at_hour, now_almaty

from suppliers.comportal.builder import build_offers
from suppliers.comportal.diagnostics import (
    build_watch_source_map,
    make_watch_messages,
    summarize_build_stats,
    summarize_offer_outs,
    summarize_source_offers,
    write_watch_report,
)
from suppliers.comportal.filtering import filter_source_offers, parse_id_set
from suppliers.comportal.quality_gate import run_quality_gate
from suppliers.comportal.source import load_source_bundle


BUILD_COMPORTAL_VERSION = "build_comportal_v2_template_orchestrator"

COMPORTAL_URL_DEFAULT = "https://www.comportal.kz/auth/documents/prices/yml-catalog.php"
COMPORTAL_OUT_DEFAULT = "docs/comportal.yml"
COMPORTAL_RAW_OUT_DEFAULT = "docs/raw/comportal.yml"
COMPORTAL_ID_PREFIX = "CP"
COMPORTAL_WATCH_OIDS: set[str] = set()

CFG_DIR_DEFAULT = "scripts/suppliers/comportal/config"
FILTER_FILE_DEFAULT = "filter.yml"
SCHEMA_FILE_DEFAULT = "schema.yml"
POLICY_FILE_DEFAULT = "policy.yml"

WATCH_REPORT_DEFAULT = "docs/raw/comportal_watch.txt"
QUALITY_BASELINE_DEFAULT = "scripts/suppliers/comportal/config/quality_baseline.yml"
QUALITY_REPORT_DEFAULT = "docs/raw/comportal_quality_gate.txt"
PLACEHOLDER_DEFAULT = "https://placehold.co/800x800/png?text=No+Photo"


# ----------------------------- config helpers -----------------------------


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _load_supplier_config(cfg_dir: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    filter_cfg = _read_yaml(cfg_dir / FILTER_FILE_DEFAULT)
    schema_cfg = _read_yaml(cfg_dir / SCHEMA_FILE_DEFAULT)
    policy_cfg = _read_yaml(cfg_dir / POLICY_FILE_DEFAULT)
    return filter_cfg, schema_cfg, policy_cfg


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _resolve_hour(policy_cfg: dict[str, Any], schema_cfg: dict[str, Any]) -> int:
    return _safe_int(
        policy_cfg.get("schedule_hour_almaty")
        or policy_cfg.get("next_run_hour_local")
        or schema_cfg.get("next_run_hour_local"),
        4,
    )


def _resolve_placeholder(schema_cfg: dict[str, Any]) -> str:
    return str(schema_cfg.get("placeholder_picture") or PLACEHOLDER_DEFAULT).strip() or PLACEHOLDER_DEFAULT


def _resolve_vendor_blacklist(schema_cfg: dict[str, Any]) -> set[str]:
    return {str(x).strip().casefold() for x in (schema_cfg.get("vendor_blacklist_casefold") or []) if str(x).strip()}


def _resolve_quality_gate(schema_cfg: dict[str, Any]) -> dict[str, Any]:
    qg = dict(schema_cfg.get("quality_gate") or {})
    if "enabled" not in qg:
        qg["enabled"] = True
    if "enforce" not in qg:
        qg["enforce"] = True
    if not qg.get("baseline_file"):
        qg["baseline_file"] = QUALITY_BASELINE_DEFAULT
    if not qg.get("report_file"):
        qg["report_file"] = QUALITY_REPORT_DEFAULT
    return qg


def _resolve_allowed_category_ids(filter_cfg: dict[str, Any]) -> set[str]:
    fallback_ids = {str(x) for x in (filter_cfg.get("allowed_category_ids") or filter_cfg.get("category_ids") or [])}
    return parse_id_set(os.getenv("COMPORTAL_CATEGORY_IDS"), fallback_ids)


def _resolve_excluded_root_ids(filter_cfg: dict[str, Any]) -> set[str]:
    fallback_ids = {str(x) for x in (filter_cfg.get("excluded_root_ids") or [])}
    return parse_id_set(os.getenv("COMPORTAL_EXCLUDED_ROOT_IDS"), fallback_ids)


def _resolve_watch_ids() -> set[str]:
    raw = os.getenv("COMPORTAL_WATCH_OIDS", "").strip()
    if not raw:
        return set(COMPORTAL_WATCH_OIDS)
    parts = [x.strip() for x in raw.replace(";", ",").split(",")]
    return {x for x in parts if x}


def _run_quality_gate(*, raw_out_file: str, cfg_dir: Path, qg: dict[str, Any]) -> dict[str, object]:
    if not qg.get("enabled", True):
        return {
            "ok": True,
            "critical_count": 0,
            "cosmetic_total_count": 0,
            "known_cosmetic_count": 0,
            "new_cosmetic_count": 0,
            "critical_preview": [],
            "report_file": str(qg.get("report_file") or QUALITY_REPORT_DEFAULT),
            "baseline_file": str(qg.get("baseline_file") or QUALITY_BASELINE_DEFAULT),
        }

    schema_path = cfg_dir / SCHEMA_FILE_DEFAULT
    return run_quality_gate(
        feed_path=raw_out_file,
        schema_path=schema_path,
        enforce=bool(qg.get("enforce", True)),
    )


# ----------------------------- main -----------------------------


def main() -> int:
    cfg_dir = Path(os.getenv("COMPORTAL_CFG_DIR", CFG_DIR_DEFAULT))
    filter_cfg, schema_cfg, policy_cfg = _load_supplier_config(cfg_dir)

    url = os.getenv("COMPORTAL_SOURCE_URL", COMPORTAL_URL_DEFAULT).strip() or COMPORTAL_URL_DEFAULT
    out_file = os.getenv("COMPORTAL_OUT_FILE", COMPORTAL_OUT_DEFAULT).strip() or COMPORTAL_OUT_DEFAULT
    raw_out_file = os.getenv("COMPORTAL_RAW_OUT_FILE", COMPORTAL_RAW_OUT_DEFAULT).strip() or COMPORTAL_RAW_OUT_DEFAULT
    watch_report = os.getenv("COMPORTAL_WATCH_REPORT", WATCH_REPORT_DEFAULT).strip() or WATCH_REPORT_DEFAULT

    login = os.getenv("COMPORTAL_LOGIN", "").strip() or None
    password = os.getenv("COMPORTAL_PASSWORD", "").strip() or None
    timeout = _safe_int(os.getenv("COMPORTAL_TIMEOUT", "120"), 120)

    supplier_name = str(policy_cfg.get("supplier") or schema_cfg.get("supplier") or "ComPortal").strip() or "ComPortal"
    hour = _resolve_hour(policy_cfg, schema_cfg)
    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour=hour)

    placeholder_picture = _resolve_placeholder(schema_cfg)
    vendor_blacklist = _resolve_vendor_blacklist(schema_cfg)
    qg = _resolve_quality_gate(schema_cfg)

    # builder/schema compatibility helpers
    if "placeholder_picture" not in schema_cfg:
        schema_cfg["placeholder_picture"] = placeholder_picture
    if "vendor_blacklist_casefold" not in schema_cfg:
        schema_cfg["vendor_blacklist_casefold"] = sorted(vendor_blacklist)

    allowed_category_ids = _resolve_allowed_category_ids(filter_cfg)
    excluded_root_ids = _resolve_excluded_root_ids(filter_cfg)
    watch_ids = _resolve_watch_ids()

    category_index, source_offers = load_source_bundle(
        url=url,
        timeout=timeout,
        login=login,
        password=password,
    )
    before = len(source_offers)

    filtered_offers = filter_source_offers(
        source_offers,
        allowed_category_ids,
        excluded_root_ids,
    )

    watch_source = build_watch_source_map(
        source_offers,
        prefix=COMPORTAL_ID_PREFIX,
        watch_ids=watch_ids,
    )

    out_offers, build_stats = build_offers(
        filtered_offers,
        schema=schema_cfg,
        policy=policy_cfg,
    )
    after = len(out_offers)
    watch_out = {offer.oid for offer in out_offers}

    watch_messages = make_watch_messages(
        watch_ids=watch_ids,
        watch_source=watch_source,
        watch_out=watch_out,
    )
    write_watch_report(watch_report, watch_messages)

    write_cs_feed_raw(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=raw_out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding="utf-8",
        currency_id=str(schema_cfg.get("currency") or "KZT"),
    )

    changed = write_cs_feed(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding="utf-8",
        public_vendor=os.getenv("PUBLIC_VENDOR", "CS").strip() or "CS",
        currency_id=str(schema_cfg.get("currency") or "KZT"),
    )

    qg_result = _run_quality_gate(raw_out_file=raw_out_file, cfg_dir=cfg_dir, qg=qg)

    src_summary = summarize_source_offers(source_offers)
    out_summary = summarize_offer_outs(out_offers)
    build_summary = summarize_build_stats(build_stats)

    print(
        f"[build_comportal] OK | version={BUILD_COMPORTAL_VERSION} | "
        f"offers_in={before} | offers_out={after} | "
        f"in_true={out_summary.get('available_true', 0)} | "
        f"in_false={out_summary.get('available_false', 0)} | "
        f"changed={'yes' if changed else 'no'} | file={out_file}"
    )
    print(
        f"[build_comportal] source: with_vendor={src_summary.get('with_vendor', 0)} "
        f"without_vendor={src_summary.get('without_vendor', 0)} "
        f"with_picture={src_summary.get('with_picture', 0)} "
        f"without_picture={src_summary.get('without_picture', 0)}"
    )
    print(
        f"[build_comportal] build: filtered_out={build_summary.get('filtered_out', 0)} "
        f"placeholder_pictures={build_summary.get('placeholder_picture_count', 0)} "
        f"empty_vendor={build_summary.get('empty_vendor_count', 0)}"
    )
    print(
        f"[build_comportal] qg: ok={'yes' if qg_result.get('ok') else 'no'} | "
        f"critical={qg_result.get('critical_count', 0)} | "
        f"cosmetic_total={qg_result.get('cosmetic_total_count', 0)} | "
        f"report={qg_result.get('report_file', QUALITY_REPORT_DEFAULT)}"
    )

    if qg_result.get("critical_preview"):
        print("[build_comportal] qg critical preview:")
        for line in qg_result.get("critical_preview", []):
            print(f"  - {line}")

    if not qg_result.get("ok", True):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
