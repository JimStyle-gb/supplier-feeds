# -*- coding: utf-8 -*-
"""
Path: scripts/build_alstyle.py

AlStyle adapter (AS) — thin orchestrator under CS-template.

Что делает:
- грузит supplier config: filter / schema / policy;
- читает исходный XML поставщика;
- прогоняет source -> filtering -> builder;
- пишет raw feed;
- пишет final feed;
- пишет watch-report;
- запускает supplier-side quality gate.

Важно:
- supplier-specific логика остаётся только в suppliers/alstyle/*;
- build_alstyle.py не должен знать regex-логику AlStyle;
- orchestrator остаётся тонким и шаблонным относительно других поставщиков.

v109:
- default baseline path приведён к quality_gate_baseline.yml.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from cs.core import write_cs_feed, write_cs_feed_raw
from cs.meta import next_run_at_hour, now_almaty

from suppliers.alstyle.builder import build_offers
from suppliers.alstyle.diagnostics import (
    build_watch_source_map,
    make_watch_messages,
    write_watch_report,
)
from suppliers.alstyle.filtering import filter_source_offers, parse_id_set
from suppliers.alstyle.quality_gate import run_quality_gate
from suppliers.alstyle.source import load_source_offers


BUILD_ALSTYLE_VERSION = "build_alstyle_v109_qg_baseline_canonical"

ALSTYLE_URL_DEFAULT = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
ALSTYLE_OUT_DEFAULT = "docs/alstyle.yml"
ALSTYLE_RAW_OUT_DEFAULT = "docs/raw/alstyle.yml"
ALSTYLE_ID_PREFIX = "AS"
ALSTYLE_WATCH_OIDS = {"AS257478"}

CFG_DIR_DEFAULT = "scripts/suppliers/alstyle/config"
FILTER_FILE_DEFAULT = "filter.yml"
SCHEMA_FILE_DEFAULT = "schema.yml"
POLICY_FILE_DEFAULT = "policy.yml"

WATCH_REPORT_DEFAULT = "docs/raw/alstyle_watch.txt"
QUALITY_BASELINE_DEFAULT = "scripts/suppliers/alstyle/config/quality_gate_baseline.yml"
QUALITY_REPORT_DEFAULT = "docs/raw/alstyle_quality_gate.txt"
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


def _env_truthy(name: str) -> bool:
    val = os.getenv(name, "").strip().casefold()
    return val in {"1", "true", "yes", "y", "on"}


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _resolve_hour(policy_cfg: dict[str, Any]) -> int:
    return _safe_int(
        policy_cfg.get("schedule_hour_almaty")
        or policy_cfg.get("next_run_hour_local"),
        2,
    )


def _resolve_placeholder(policy_cfg: dict[str, Any]) -> str:
    return (
        os.getenv("PLACEHOLDER_PICTURE")
        or policy_cfg.get("placeholder_picture")
        or PLACEHOLDER_DEFAULT
    )


def _resolve_vendor_blacklist(policy_cfg: dict[str, Any]) -> set[str]:
    raw = policy_cfg.get("vendor_blacklist_casefold") or ["alstyle"]
    return {str(x).casefold() for x in raw if str(x).strip()}


def _resolve_quality_gate(policy_cfg: dict[str, Any]) -> dict[str, Any]:
    qg_cfg = policy_cfg.get("quality_gate") or {}

    return {
        "enabled": bool(qg_cfg.get("enabled", True)),
        "enforce": bool(qg_cfg.get("enforce", True)),
        "baseline_path": (
            os.getenv("ALSTYLE_QUALITY_BASELINE")
            or qg_cfg.get("baseline_file")
            or QUALITY_BASELINE_DEFAULT
        ),
        "report_path": (
            os.getenv("ALSTYLE_QUALITY_REPORT")
            or qg_cfg.get("report_file")
            or QUALITY_REPORT_DEFAULT
        ),
        "max_cosmetic_offers": _safe_int(
            os.getenv(
                "ALSTYLE_QUALITY_MAX_COSMETIC_OFFERS",
                os.getenv(
                    "ALSTYLE_QUALITY_MAX_NEW_COSMETIC_OFFERS",
                    str(qg_cfg.get("max_new_cosmetic_offers", 5)),
                ),
            ),
            5,
        ),
        "max_cosmetic_issues": _safe_int(
            os.getenv(
                "ALSTYLE_QUALITY_MAX_COSMETIC_ISSUES",
                os.getenv(
                    "ALSTYLE_QUALITY_MAX_NEW_COSMETIC_ISSUES",
                    str(qg_cfg.get("max_new_cosmetic_issues", 5)),
                ),
            ),
            5,
        ),
        "freeze_current_as_baseline": bool(qg_cfg.get("freeze_current_as_baseline", False))
        or _env_truthy("ALSTYLE_QUALITY_FREEZE_BASELINE"),
    }


def _run_quality_gate(*, out_file: str, qg: dict[str, Any]) -> None:
    if not qg.get("enabled", True):
        return

    ok, summary = run_quality_gate(
        feed_path=out_file,
        baseline_path=str(qg.get("baseline_path") or QUALITY_BASELINE_DEFAULT),
        report_path=str(qg.get("report_path") or QUALITY_REPORT_DEFAULT),
        max_new_cosmetic_offers=_safe_int(qg.get("max_cosmetic_offers"), 5),
        max_new_cosmetic_issues=_safe_int(qg.get("max_cosmetic_issues"), 5),
        enforce=bool(qg.get("enforce", True)),
        freeze_current_as_baseline=bool(qg.get("freeze_current_as_baseline", False)),
    )
    print(summary)
    if not ok:
        raise SystemExit(1)


# ----------------------------- main -----------------------------


def main() -> int:
    url = os.getenv("ALSTYLE_URL", ALSTYLE_URL_DEFAULT)
    out_file = os.getenv("ALSTYLE_OUT", ALSTYLE_OUT_DEFAULT)
    raw_out_file = os.getenv("ALSTYLE_RAW_OUT", ALSTYLE_RAW_OUT_DEFAULT)
    watch_report = os.getenv("ALSTYLE_WATCH_REPORT", WATCH_REPORT_DEFAULT)

    cfg_dir = Path(os.getenv("ALSTYLE_CFG_DIR", CFG_DIR_DEFAULT))
    filter_cfg, schema_cfg, policy_cfg = _load_supplier_config(cfg_dir)

    timeout = _safe_int(os.getenv("ALSTYLE_TIMEOUT", "120"), 120)
    login = os.getenv("ALSTYLE_LOGIN", "").strip()
    password = os.getenv("ALSTYLE_PASSWORD", "").strip()

    supplier_name = str(policy_cfg.get("supplier") or "AlStyle").strip() or "AlStyle"
    hour = _resolve_hour(policy_cfg)
    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour=hour)

    placeholder_picture = _resolve_placeholder(policy_cfg)
    vendor_blacklist = _resolve_vendor_blacklist(policy_cfg)
    qg = _resolve_quality_gate(policy_cfg)

    fallback_ids = {str(x) for x in (filter_cfg.get("category_ids") or [])}
    allowed_category_ids = parse_id_set(os.getenv("ALSTYLE_CATEGORY_IDS"), fallback_ids)

    source_offers = load_source_offers(
        url=url,
        timeout=timeout,
        login=login,
        password=password,
    )
    before = len(source_offers)
    filtered_offers = filter_source_offers(source_offers, allowed_category_ids)

    watch_source = build_watch_source_map(
        source_offers,
        prefix=ALSTYLE_ID_PREFIX,
        watch_ids=ALSTYLE_WATCH_OIDS,
    )

    out_offers, in_true, in_false = build_offers(
        filtered_offers,
        schema_cfg=schema_cfg,
        vendor_blacklist=vendor_blacklist,
        placeholder_picture=placeholder_picture,
        id_prefix=ALSTYLE_ID_PREFIX,
    )
    after = len(out_offers)
    watch_out = {offer.oid for offer in out_offers}

    watch_messages = make_watch_messages(
        watch_ids=ALSTYLE_WATCH_OIDS,
        watch_source=watch_source,
        watch_out=watch_out,
        allowed=allowed_category_ids,
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
        currency_id="KZT",
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
        currency_id="KZT",
    )

    _run_quality_gate(out_file=out_file, qg=qg)

    print(
        f"[build_alstyle] OK | version={BUILD_ALSTYLE_VERSION} | "
        f"offers_in={before} | offers_out={after} | "
        f"in_true={in_true} | in_false={in_false} | "
        f"changed={'yes' if changed else 'no'} | file={out_file}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
