# -*- coding: utf-8 -*-
"""
Path: scripts/build_akcent.py

AkCent adapter (AC) — thin orchestrator under CS-template.

Что делает:
- грузит supplier config: filter / schema / policy;
- читает исходный XML поставщика;
- прогоняет source -> filtering -> builder;
- пишет raw feed;
- пишет final feed;
- печатает diagnostics summary;
- запускает supplier-side quality gate.

Важно:
- supplier-specific логика остаётся только в suppliers/akcent/*;
- build_akcent.py не знает regexp-логики AkCent;
- orchestrator остаётся thin и backward-safe.
"""

from __future__ import annotations

import inspect
import os
from pathlib import Path
from typing import Any

import yaml

from cs.core import get_public_vendor, write_cs_feed, write_cs_feed_raw

try:
    from cs.meta import next_run_at_hour, now_almaty
except Exception:
    from cs.core import next_run_at_hour, now_almaty  # type: ignore

from suppliers.akcent.builder import build_offers
from suppliers.akcent.diagnostics import print_build_summary
from suppliers.akcent.filtering import filter_source_offers
from suppliers.akcent.quality_gate import run_quality_gate
from suppliers.akcent.source import fetch_source_root, iter_source_offers


BUILD_AKCENT_VERSION = "build_akcent_v66_supplier_layer_bootstrap"

AKCENT_URL_DEFAULT = "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"
AKCENT_OUT_DEFAULT = "docs/akcent.yml"
AKCENT_RAW_OUT_DEFAULT = "docs/raw/akcent.yml"

CFG_DIR_DEFAULT = "scripts/suppliers/akcent/config"
FILTER_FILE_DEFAULT = "filter.yml"
SCHEMA_FILE_DEFAULT = "schema.yml"
POLICY_FILE_DEFAULT = "policy.yml"

QUALITY_BASELINE_DEFAULT = "scripts/suppliers/akcent/config/quality_baseline.yml"
QUALITY_REPORT_DEFAULT = "docs/raw/akcent_quality_gate.txt"
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



def _filter_prefixes_from_cfg(filter_cfg: dict[str, Any]) -> list[str]:
    include_rules = filter_cfg.get("include_rules") or {}
    raw = (
        include_rules.get("name_prefixes")
        or filter_cfg.get("name_prefixes")
        or include_rules.get("allow_name_prefixes")
        or filter_cfg.get("allow_name_prefixes")
        or []
    )

    out: list[str] = []
    for item in raw:
        s = str(item or "").strip()
        if s:
            out.append(s)
    return out


# ----------------------------- call adapters -----------------------------


def _call_filter(
    source_offers: list[Any],
    *,
    filter_cfg: dict[str, Any],
) -> tuple[list[Any], dict[str, Any]]:
    """Backward-safe вызов filtering.py."""
    sig = inspect.signature(filter_source_offers)
    kwargs: dict[str, Any] = {}

    if "filter_cfg" in sig.parameters:
        kwargs["filter_cfg"] = filter_cfg
    if "prefixes" in sig.parameters:
        kwargs["prefixes"] = _filter_prefixes_from_cfg(filter_cfg)
    if "allowed_prefixes" in sig.parameters:
        kwargs["allowed_prefixes"] = _filter_prefixes_from_cfg(filter_cfg)
    if "mode" in sig.parameters:
        kwargs["mode"] = str(filter_cfg.get("mode") or "include")

    result = filter_source_offers(source_offers, **kwargs)

    if isinstance(result, tuple) and len(result) == 2:
        filtered, report = result
        return list(filtered or []), dict(report or {})

    filtered = list(result or [])
    report = {
        "before": len(source_offers),
        "after": len(filtered),
        "rejected_total": max(0, len(source_offers) - len(filtered)),
    }
    return filtered, report



def _call_builder(
    filtered_offers: list[Any],
    *,
    schema_cfg: dict[str, Any],
    policy_cfg: dict[str, Any],
) -> tuple[list[Any], dict[str, Any]]:
    """Backward-safe вызов builder.py."""
    sig = inspect.signature(build_offers)
    kwargs: dict[str, Any] = {}

    if "schema_cfg" in sig.parameters:
        kwargs["schema_cfg"] = schema_cfg
    if "policy_cfg" in sig.parameters:
        kwargs["policy_cfg"] = policy_cfg
    if "placeholder_picture" in sig.parameters:
        kwargs["placeholder_picture"] = (
            os.getenv("PLACEHOLDER_PICTURE")
            or policy_cfg.get("placeholder_picture")
            or PLACEHOLDER_DEFAULT
        )
    if "id_prefix" in sig.parameters:
        kwargs["id_prefix"] = str(policy_cfg.get("id_prefix") or "AC").strip() or "AC"
    if "vendor_blacklist" in sig.parameters:
        kwargs["vendor_blacklist"] = {
            str(x).casefold()
            for x in (policy_cfg.get("vendor_blacklist_casefold") or [])
            if str(x).strip()
        }

    result = build_offers(filtered_offers, **kwargs)

    if isinstance(result, tuple) and len(result) == 2:
        offers, report = result
        return list(offers or []), dict(report or {})

    offers = list(result or [])
    report = {
        "before": len(filtered_offers),
        "after": len(offers),
    }
    return offers, report



def _run_quality_gate(*, out_file: str, raw_out_file: str, policy_cfg: dict[str, Any]) -> None:
    """Backward-safe вызов quality_gate.py."""
    qg_cfg = policy_cfg.get("quality_gate") or {}
    if not bool(qg_cfg.get("enabled", True)):
        return

    sig = inspect.signature(run_quality_gate)
    params = set(sig.parameters.keys())

    if "feed_path" in params:
        baseline_path = (
            os.getenv("AKCENT_QUALITY_BASELINE")
            or qg_cfg.get("baseline_file")
            or QUALITY_BASELINE_DEFAULT
        )
        report_path = (
            os.getenv("AKCENT_QUALITY_REPORT")
            or qg_cfg.get("report_file")
            or QUALITY_REPORT_DEFAULT
        )
        enforce = bool(qg_cfg.get("enforce", True))
        freeze_current = bool(qg_cfg.get("freeze_current_as_baseline", False)) or _env_truthy(
            "AKCENT_QUALITY_FREEZE_BASELINE"
        )
        max_cosmetic_offers = _safe_int(
            os.getenv(
                "AKCENT_QUALITY_MAX_COSMETIC_OFFERS",
                os.getenv(
                    "AKCENT_QUALITY_MAX_NEW_COSMETIC_OFFERS",
                    str(qg_cfg.get("max_new_cosmetic_offers", 5)),
                ),
            ),
            5,
        )
        max_cosmetic_issues = _safe_int(
            os.getenv(
                "AKCENT_QUALITY_MAX_COSMETIC_ISSUES",
                os.getenv(
                    "AKCENT_QUALITY_MAX_NEW_COSMETIC_ISSUES",
                    str(qg_cfg.get("max_new_cosmetic_issues", 5)),
                ),
            ),
            5,
        )

        ok, summary = run_quality_gate(
            feed_path=out_file,
            baseline_path=baseline_path,
            report_path=report_path,
            max_new_cosmetic_offers=max_cosmetic_offers,
            max_new_cosmetic_issues=max_cosmetic_issues,
            enforce=enforce,
            freeze_current_as_baseline=freeze_current,
        )
        print(summary)
        if not ok:
            raise SystemExit(1)
        return

    run_quality_gate(
        out_file=out_file,
        raw_out_file=raw_out_file,
        supplier=str(policy_cfg.get("supplier") or "AkCent").strip() or "AkCent",
        version=BUILD_AKCENT_VERSION,
    )


# ----------------------------- main -----------------------------


def main() -> int:
    url = os.getenv("AKCENT_URL", AKCENT_URL_DEFAULT)
    out_file = os.getenv("AKCENT_OUT", os.getenv("AKCENT_OUT_FILE", AKCENT_OUT_DEFAULT))
    raw_out_file = os.getenv(
        "AKCENT_RAW_OUT",
        os.getenv("AKCENT_RAW_OUT_FILE", AKCENT_RAW_OUT_DEFAULT),
    )

    cfg_dir = Path(os.getenv("AKCENT_CFG_DIR", CFG_DIR_DEFAULT))
    filter_cfg, schema_cfg, policy_cfg = _load_supplier_config(cfg_dir)

    supplier_name = str(policy_cfg.get("supplier") or "AkCent").strip() or "AkCent"
    encoding = str(policy_cfg.get("output_encoding") or "utf-8").strip() or "utf-8"
    schedule_hour = _safe_int(
        policy_cfg.get("schedule_hour_almaty") or policy_cfg.get("next_run_hour_local"),
        2,
    )

    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour=schedule_hour)

    root = fetch_source_root(url)
    source_offers = list(iter_source_offers(root))
    before = len(source_offers)

    filtered_offers, filter_report = _call_filter(source_offers, filter_cfg=filter_cfg)
    out_offers, build_report = _call_builder(
        filtered_offers,
        schema_cfg=schema_cfg,
        policy_cfg=policy_cfg,
    )
    after = len(out_offers)

    write_cs_feed_raw(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=raw_out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
    )

    write_cs_feed(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        public_vendor=get_public_vendor(supplier_name),
    )

    print_build_summary(
        supplier=supplier_name,
        version=BUILD_AKCENT_VERSION,
        before=before,
        after=after,
        filter_report=filter_report,
        build_report=build_report,
        out_file=out_file,
        raw_out_file=raw_out_file,
    )

    _run_quality_gate(
        out_file=out_file,
        raw_out_file=raw_out_file,
        policy_cfg=policy_cfg,
    )

    print(
        f"[build_akcent] OK | version={BUILD_AKCENT_VERSION} | "
        f"offers_in={before} | offers_out={after} | file={out_file}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
