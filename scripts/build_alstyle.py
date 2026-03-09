# -*- coding: utf-8 -*-
"""
AlStyle adapter (AS) — CS-шаблон (config-driven).

Этап 5 рефакторинга:
- вынесен desc->params слой в scripts/suppliers/alstyle/desc_extract.py
- build_alstyle.py остаётся orchestrator'ом и точкой сборки raw/final
- desc_clean / params_xml / compat уже живут отдельно
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from cs.core import OfferOut, write_cs_feed, write_cs_feed_raw
from cs.meta import now_almaty, next_run_at_hour
from cs.pricing import compute_price
from cs.util import norm_ws, safe_int

from scripts.suppliers.alstyle.source import load_source_offers
from scripts.suppliers.alstyle.filtering import parse_allowed_category_ids, filter_source_offers
from scripts.suppliers.alstyle.normalize import (
    make_offer_oid,
    normalize_available,
    normalize_name,
    normalize_price_in,
    normalize_vendor,
)
from scripts.suppliers.alstyle.pictures import collect_picture_urls
from scripts.suppliers.alstyle.params_xml import collect_xml_params
from scripts.suppliers.alstyle.desc_clean import sanitize_native_desc
from scripts.suppliers.alstyle.desc_extract import extract_desc_spec_pairs
from scripts.suppliers.alstyle.diagnostics import (
    build_watch_source,
    collect_watch_messages,
    write_watch_report,
)

BUILD_ALSTYLE_VERSION = "build_alstyle_v104_stage5_desc_extract_split"

ALSTYLE_URL_DEFAULT = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
ALSTYLE_OUT_DEFAULT = "docs/alstyle.yml"
ALSTYLE_RAW_OUT_DEFAULT = "docs/raw/alstyle.yml"
ALSTYLE_ID_PREFIX = "AS"

CFG_DIR_DEFAULT = "scripts/suppliers/alstyle/config"
FILTER_FILE_DEFAULT = "filter.yml"
SCHEMA_FILE_DEFAULT = "schema.yml"
POLICY_FILE_DEFAULT = "policy.yml"
WATCH_REPORT_DEFAULT = "docs/raw/alstyle_watch.txt"

ALSTYLE_WATCH_OIDS = {"AS257478"}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _load_supplier_config(cfg_dir: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    filter_cfg = _read_yaml(cfg_dir / FILTER_FILE_DEFAULT)
    schema_cfg = _read_yaml(cfg_dir / SCHEMA_FILE_DEFAULT)
    policy_cfg = _read_yaml(cfg_dir / POLICY_FILE_DEFAULT)
    return filter_cfg, schema_cfg, policy_cfg


def _merge_params(
    xml_params: list[tuple[str, str]],
    desc_params: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    XML params всегда приоритетнее.
    Description-derived params только дополняют.
    """
    out: list[tuple[str, str]] = []
    seen_key = set()
    seen_pair = set()

    for k, v in xml_params:
        k2 = norm_ws(k)
        v2 = norm_ws(v)
        if not k2 or not v2:
            continue
        out.append((k2, v2))
        seen_key.add(k2.casefold())
        seen_pair.add((k2.casefold(), v2.casefold()))

    for k, v in desc_params:
        k2 = norm_ws(k)
        v2 = norm_ws(v)
        if not k2 or not v2:
            continue
        if k2.casefold() in seen_key:
            continue
        sig = (k2.casefold(), v2.casefold())
        if sig in seen_pair:
            continue
        out.append((k2, v2))
        seen_key.add(k2.casefold())
        seen_pair.add(sig)

    return out


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
    allowed = parse_allowed_category_ids(os.getenv("ALSTYLE_CATEGORY_IDS"), fallback_ids)

    source_offers = load_source_offers(url=url, timeout=timeout, login=login, password=password)
    before = len(source_offers)
    filtered_offers = filter_source_offers(source_offers, allowed)
    watch_source = build_watch_source(source_offers, ALSTYLE_WATCH_OIDS, id_prefix=ALSTYLE_ID_PREFIX)

    out_offers: list[OfferOut] = []
    watch_out: set[str] = set()
    in_true = 0
    in_false = 0

    for src in filtered_offers:
        raw_id = norm_ws(src.get("id") or src.get("vendorCode"))
        name = normalize_name(src.get("name") or "")
        if not raw_id or not name:
            continue

        oid = make_offer_oid(raw_id, prefix=ALSTYLE_ID_PREFIX)
        available = normalize_available(src)
        if available:
            in_true += 1
        else:
            in_false += 1

        pictures = collect_picture_urls(src, placeholder_picture=placeholder_picture)
        vendor = normalize_vendor(src.get("vendor") or "", vendor_blacklist=vendor_blacklist)

        desc_src = sanitize_native_desc(src.get("description") or "", name=name)
        xml_params = collect_xml_params(src, schema_cfg)
        desc_params = extract_desc_spec_pairs(desc_src, schema_cfg)
        params = _merge_params(xml_params, desc_params)

        price_in = normalize_price_in(src)
        price = compute_price(price_in)

        watch_out.add(oid)
        out_offers.append(
            OfferOut(
                oid=oid,
                available=available,
                name=name,
                price=price,
                pictures=pictures,
                vendor=vendor,
                params=params,
                native_desc=desc_src,
            )
        )

    out_offers.sort(key=lambda x: x.oid)
    after = len(out_offers)

    watch_messages = collect_watch_messages(
        watch_ids=ALSTYLE_WATCH_OIDS,
        watch_source=watch_source,
        watch_out=watch_out,
        allowed=allowed,
        id_prefix=ALSTYLE_ID_PREFIX,
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
