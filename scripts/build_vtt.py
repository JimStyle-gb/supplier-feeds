# -*- coding: utf-8 -*-
"""
Path: scripts/build_vtt.py

VTT adapter stage-next.

Структура:
- source.py        -> login / crawl / raw page parse
- filtering.py     -> category + item filter до парсинга карточек
- pictures.py      -> чистка картинок
- normalize.py     -> базовая нормализация item
- params_page.py   -> page params -> clean RAW params
- desc_clean.py    -> supplier native_desc cleanup
- desc_extract.py  -> only_fill_missing
- compat.py        -> compat/codes reconcile
- builder.py       -> сборка OfferOut
- quality_gate.py  -> проверка RAW

Правило:
RAW должен быть уже чистым supplier-result.
Core делает только общие shared-правки.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Sequence

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore

from cs.core import get_public_vendor, next_run_dom_at_hour, now_almaty, write_cs_feed, write_cs_feed_raw
from suppliers.vtt.builder import build_offer_from_raw
from suppliers.vtt.filtering import filter_product_index, load_filter_config
from suppliers.vtt.quality_gate import run_quality_gate
from suppliers.vtt.source import (
    cfg_from_env,
    clone_session_with_cookies,
    collect_product_index,
    login,
    log,
    make_session,
    parse_product_page_from_index,
)

SUPPLIER_NAME = "VTT"
OUT_FILE = os.getenv("OUT_FILE", "docs/vtt.yml")
RAW_OUT_FILE = os.getenv("RAW_OUT_FILE", "docs/raw/vtt.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "utf-8") or "utf-8").strip() or "utf-8"

VTT_FILTER_YML = os.getenv("VTT_FILTER_YML", "scripts/suppliers/vtt/config/filter.yml")
VTT_POLICY_YML = os.getenv("VTT_POLICY_YML", "scripts/suppliers/vtt/config/policy.yml")
VTT_QG_REPORT = os.getenv("VTT_QG_REPORT", "docs/raw/vtt_quality_gate.txt")


def _safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def _load_policy(path: str | Path | None = None) -> dict:
    p = Path(path or VTT_POLICY_YML)
    if not p.exists() or yaml is None:
        return {}
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _param_priority() -> Sequence[str]:
    data = _load_policy(VTT_POLICY_YML)
    raw = data.get("param_priority") or []
    out = [str(x).strip() for x in raw if str(x).strip()]
    if out:
        return tuple(out)
    return (
        "Тип",
        "Для бренда",
        "Коды расходников",
        "Партномер",
        "Совместимость",
        "Технология печати",
        "Цвет",
        "Ресурс",
        "Объем",
        "Модель",
    )


def _print_summary(
    *,
    before: int,
    after_filter: int,
    after: int,
    raw_out_file: str,
    out_file: str,
    filter_report: dict,
    qg,
    availability_true: int,
    availability_false: int,
) -> None:
    print("=" * 72)
    print("[VTT] build summary")
    print("=" * 72)
    print("version: build_vtt_v2_supplier_layer_raw_clean")
    print(f"before: {before}")
    print(f"after_filter: {after_filter}")
    print(f"after:  {after}")
    print(f"raw_out_file: {raw_out_file}")
    print(f"out_file: {out_file}")
    print("-" * 72)
    print("filter_report:")
    for k, v in filter_report.items():
        print(f"  {k}: {v}")
    print("-" * 72)
    print(f"quality_gate_ok:    {qg.ok}")
    print(f"quality_gate_report:{qg.report_path}")
    print(f"quality_gate_critical: {qg.critical_count}")
    print(f"quality_gate_cosmetic: {qg.cosmetic_count}")
    print(f"availability_true:  {availability_true}")
    print(f"availability_false: {availability_false}")
    print("=" * 72)


def main() -> int:
    cfg = cfg_from_env()
    build_time = now_almaty().replace(tzinfo=None)
    next_run = next_run_dom_at_hour(build_time, 5, (1, 10, 20))
    deadline = datetime.utcnow() + timedelta(minutes=max(1.0, float(cfg.max_crawl_minutes)))

    filter_cfg = load_filter_config(VTT_FILTER_YML)

    sess = make_session(cfg)
    if not login(sess, cfg):
        msg = "VTT: авторизация не прошла (проверь VTT_LOGIN/VTT_PASSWORD или доступность сайта)."
        if cfg.softfail:
            log("[SOFTFAIL] " + msg)
            return 0
        raise RuntimeError(msg)

    category_urls = list(getattr(cfg, "categories", None) or [])
    if not category_urls:
        category_urls = [
            f"{cfg.base_url}/catalog/?category={code}"
            for code in (filter_cfg.get("allowed_category_codes") or [])
            if _safe_str(code)
        ]
    if not category_urls:
        msg = "VTT: пустой список category urls для source/index."
        if cfg.softfail:
            log("[SOFTFAIL] " + msg)
            return 0
        raise RuntimeError(msg)

    index = collect_product_index(sess, cfg, category_urls, deadline)
    before = len(index)

    filtered_index, filter_report = filter_product_index(
        index,
        allowed_category_codes=filter_cfg.get("allowed_category_codes") or [],
        include_title_prefixes=filter_cfg.get("include_title_prefixes") or [],
        enforce_title_prefixes=bool(filter_cfg.get("enforce_title_prefixes", False)),
        require_catalog_url=bool(filter_cfg.get("require_catalog_url", True)),
    )
    after_filter = len(filtered_index)

    out_offers: list = []
    seen_oids: set[str] = set()

    if filtered_index:
        with ThreadPoolExecutor(max_workers=max(1, int(cfg.max_workers))) as pool:
            futures = []
            for item in filtered_index:
                if datetime.utcnow() >= deadline:
                    break
                child = clone_session_with_cookies(sess, cfg)
                futures.append(pool.submit(parse_product_page_from_index, child, cfg, item))

            for fut in as_completed(futures):
                if datetime.utcnow() >= deadline:
                    break
                raw = fut.result()
                if not raw:
                    continue
                offer = build_offer_from_raw(raw, id_prefix="VT")
                if not offer:
                    continue
                if offer.oid in seen_oids:
                    continue
                seen_oids.add(offer.oid)
                out_offers.append(offer)

    out_offers.sort(key=lambda o: o.oid)
    after = len(out_offers)

    if not out_offers:
        msg = "VTT: 0 offers после filtering/builder (скорее всего сайт недоступен или изменили верстку)."
        if cfg.softfail:
            log("[SOFTFAIL] " + msg)
            return 0
        raise RuntimeError(msg)

    write_cs_feed_raw(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=cfg.start_url,
        out_file=RAW_OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        currency_id="KZT",
    )

    public_vendor = get_public_vendor(SUPPLIER_NAME)
    write_cs_feed(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=cfg.start_url,
        out_file=OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        public_vendor=public_vendor,
        currency_id="KZT",
        param_priority=_param_priority(),
    )

    qg = run_quality_gate(
        feed_path=RAW_OUT_FILE,
        policy_path=VTT_POLICY_YML,
        report_path=VTT_QG_REPORT,
    )

    in_true = sum(1 for o in out_offers if o.available)
    in_false = after - in_true

    _print_summary(
        before=before,
        after_filter=after_filter,
        after=after,
        raw_out_file=RAW_OUT_FILE,
        out_file=OUT_FILE,
        filter_report=filter_report,
        qg=qg,
        availability_true=in_true,
        availability_false=in_false,
    )

    if not qg.ok:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
