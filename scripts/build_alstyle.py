# -*- coding: utf-8 -*-
"""
Path: scripts/build_alstyle.py

AlStyle adapter (AS) — CS-шаблон (config-driven).

Этап 6 рефакторинга:
- сборка одного supplier offer вынесена в scripts/suppliers/alstyle/builder.py;
- build_alstyle.py теперь почти полностью orchestrator;
- source/filter/normalize/pictures/params_xml/desc_clean/compat/desc_extract уже живут отдельно.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from cs.core import write_cs_feed, write_cs_feed_raw
from cs.meta import now_almaty, next_run_at_hour
from suppliers.alstyle.builder import build_offers
from suppliers.alstyle.diagnostics import build_watch_source_map, make_watch_messages, write_watch_report
from suppliers.alstyle.filtering import filter_source_offers, parse_id_set
from suppliers.alstyle.source import load_source_offers


BUILD_ALSTYLE_VERSION = "build_alstyle_v105_stage6_builder_split"

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



def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}



def _load_supplier_config(cfg_dir: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    filter_cfg = _read_yaml(cfg_dir / FILTER_FILE_DEFAULT)
    schema_cfg = _read_yaml(cfg_dir / SCHEMA_FILE_DEFAULT)
    policy_cfg = _read_yaml(cfg_dir / POLICY_FILE_DEFAULT)
    return filter_cfg, schema_cfg, policy_cfg



def main() -> int:
    url = os.getenv("ALSTYLE_URL", ALSTYLE_URL_DEFAULT)
    out_file = os.getenv("ALSTYLE_OUT", ALSTYLE_OUT_DEFAULT)
    raw_out = os.getenv("ALSTYLE_RAW_OUT", ALSTYLE_RAW_OUT_DEFAULT)
    watch_report = os.getenv("ALSTYLE_WATCH_REPORT", WATCH_REPORT_DEFAULT)

    cfg_dir = Path(os.getenv("ALSTYLE_CFG_DIR", CFG_DIR_DEFAULT))
    filter_cfg, schema_cfg, policy_cfg = _load_supplier_config(cfg_dir)

    timeout = int(os.getenv("ALSTYLE_TIMEOUT", "120"))
    login = os.getenv("ALSTYLE_LOGIN", "").strip()
    password = os.getenv("ALSTYLE_PASSWORD", "").strip()

    hour = int(policy_cfg.get("schedule_hour_almaty") or 1)
    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour=hour)

    placeholder_picture = (
        os.getenv("PLACEHOLDER_PICTURE")
        or policy_cfg.get("placeholder_picture")
        or "https://placehold.co/800x800/png?text=No+Photo"
    )
    supplier_name = (policy_cfg.get("supplier") or "AlStyle").strip()
    vendor_blacklist = {
        str(x).casefold()
        for x in (policy_cfg.get("vendor_blacklist_casefold") or ["alstyle"])
    }

    fallback_ids = {str(x) for x in (filter_cfg.get("category_ids") or [])}
    allowed = parse_id_set(os.getenv("ALSTYLE_CATEGORY_IDS"), fallback_ids)

    source_offers = load_source_offers(url=url, timeout=timeout, login=login, password=password)
    before = len(source_offers)
    filtered_offers = filter_source_offers(source_offers, allowed)

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
    watch_out = {x.oid for x in out_offers}

    watch_messages = make_watch_messages(
        watch_ids=ALSTYLE_WATCH_OIDS,
        watch_source=watch_source,
        watch_out=watch_out,
        allowed=allowed,
    )
    write_watch_report(watch_report, watch_messages)

    write_cs_feed_raw(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=raw_out,
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

    print(
        f"[build_alstyle] OK | version={BUILD_ALSTYLE_VERSION} | "
        f"offers_in={before} | offers_out={after} | "
        f"in_true={in_true} | in_false={in_false} | "
        f"changed={'yes' if changed else 'no'} | file={out_file}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
