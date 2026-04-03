# -*- coding: utf-8 -*-
"""
Path: scripts/build_vtt.py

VTT adapter (VT) — thin orchestrator under CS-template.

Что делает:
- грузит supplier config: filter / schema / policy;
- поддерживает mode=index / shard_index / merge / full;
- использует source.py только как login/session/crawl/product parsing слой;
- пишет raw feed;
- пишет final feed;
- запускает supplier-side quality gate;
- печатает diagnostics summary.

Важно:
- supplier-specific логика остаётся только в suppliers/vtt/*;
- orchestrator шаблонный по форме, но сохраняет VTT-specific sharding/index flow;
- ассортиментые правила берутся из filter.yml / filtering.py, а не из hardcode build-файла.
"""

from __future__ import annotations

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from cs.core import OfferOut, get_public_vendor, next_run_dom_at_hour, now_almaty, write_cs_feed, write_cs_feed_raw

from suppliers.vtt.builder import build_offer_from_raw
try:  # pragma: no cover - следующая чистка уберёт fallback
    from suppliers.vtt.diagnostics import print_build_summary as diagnostics_print_build_summary
except Exception:  # pragma: no cover
    diagnostics_print_build_summary = None  # type: ignore
from suppliers.vtt.filtering import categories_from_cfg, prefixes_from_cfg
from suppliers.vtt.quality_gate import run_quality_gate
from suppliers.vtt.source import (
    cfg_from_env,
    clone_session_with_cookies,
    collect_product_index,
    log,
    login,
    make_session,
    parse_product_page_from_index,
)


BUILD_VTT_VERSION = "build_vtt_v10_template_orchestrator_phase1"

SUPPLIER_NAME_DEFAULT = "VTT"
VTT_URL_DEFAULT = "https://b2b.vtt.ru/catalog/"
OUT_FILE_DEFAULT = "docs/vtt.yml"
RAW_OUT_FILE_DEFAULT = "docs/raw/vtt.yml"
OUTPUT_ENCODING_DEFAULT = "utf-8"
VTT_QG_REPORT_DEFAULT = "docs/raw/vtt_quality_gate.txt"
VTT_ID_PREFIX_DEFAULT = "VT"

CFG_DIR_DEFAULT = "scripts/suppliers/vtt/config"
FILTER_FILE_DEFAULT = "filter.yml"
SCHEMA_FILE_DEFAULT = "schema.yml"
POLICY_FILE_DEFAULT = "policy.yml"

ROOT = Path(__file__).resolve().parents[1]
SHARDS_DIR = ROOT / "docs" / "debug" / "vtt_shards"
INDEX_FILE = SHARDS_DIR / "index.json"


# ----------------------------- config helpers -----------------------------


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}



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



def _load_param_priority(policy_cfg: dict[str, Any], schema_cfg: dict[str, Any]) -> tuple[str, ...]:
    raw = (
        policy_cfg.get("param_priority")
        or schema_cfg.get("param_priority")
        or (
            "Тип",
            "Для бренда",
            "Партномер",
            "Коды расходников",
            "Совместимость",
            "Технология печати",
            "Цвет",
            "Ресурс",
            "Объем",
        )
    )
    out: list[str] = []
    for item in raw:
        s = str(item or "").strip()
        if s:
            out.append(s)
    return tuple(out)



def _resolve_hour(policy_cfg: dict[str, Any], schema_cfg: dict[str, Any]) -> int:
    return _safe_int(
        policy_cfg.get("schedule_hour_almaty")
        or policy_cfg.get("next_run_hour_local")
        or schema_cfg.get("next_run_hour_local"),
        5,
    )



def _resolve_dom_list(policy_cfg: dict[str, Any], schema_cfg: dict[str, Any]) -> tuple[int, ...]:
    raw = (
        policy_cfg.get("schedule_days_of_month")
        or schema_cfg.get("schedule_days_of_month")
        or [1, 10, 20]
    )
    out: list[int] = []
    for item in raw:
        try:
            out.append(int(item))
        except Exception:
            continue
    return tuple(out or [1, 10, 20])



def _resolve_supplier_name(policy_cfg: dict[str, Any], schema_cfg: dict[str, Any]) -> str:
    return str(policy_cfg.get("supplier") or schema_cfg.get("supplier") or SUPPLIER_NAME_DEFAULT).strip() or SUPPLIER_NAME_DEFAULT



def _resolve_id_prefix(policy_cfg: dict[str, Any], schema_cfg: dict[str, Any]) -> str:
    return str(policy_cfg.get("id_prefix") or schema_cfg.get("id_prefix") or VTT_ID_PREFIX_DEFAULT).strip() or VTT_ID_PREFIX_DEFAULT



def _resolve_output_encoding(policy_cfg: dict[str, Any], schema_cfg: dict[str, Any]) -> str:
    return str(
        policy_cfg.get("output_encoding")
        or schema_cfg.get("encoding")
        or OUTPUT_ENCODING_DEFAULT
    ).strip() or OUTPUT_ENCODING_DEFAULT



def _resolve_quality_gate(policy_cfg: dict[str, Any], schema_cfg: dict[str, Any]) -> dict[str, Any]:
    qg = dict(policy_cfg.get("quality_gate") or schema_cfg.get("quality_gate") or {})
    if "enabled" not in qg:
        qg["enabled"] = True
    if not qg.get("report_path") and not qg.get("report_file"):
        qg["report_path"] = VTT_QG_REPORT_DEFAULT
    return qg



def _resolve_paths() -> tuple[str, str]:
    out_file = os.getenv("VTT_OUT_FILE", os.getenv("OUT_FILE", OUT_FILE_DEFAULT)).strip() or OUT_FILE_DEFAULT
    raw_out_file = os.getenv("VTT_RAW_OUT_FILE", os.getenv("RAW_OUT_FILE", RAW_OUT_FILE_DEFAULT)).strip() or RAW_OUT_FILE_DEFAULT
    return out_file, raw_out_file



def _prepare_source_env(cfg_dir: Path, filter_cfg: dict[str, Any]) -> None:
    """
    Источник правды по filter-входам — YAML config.
    Source cfg_from_env() должен читать те же значения, даже если env извне пустой.
    """
    os.environ.setdefault("VTT_FILTER_CFG", str(cfg_dir / FILTER_FILE_DEFAULT))

    if not (os.getenv("VTT_CATEGORY_CODES") or "").strip():
        os.environ["VTT_CATEGORY_CODES"] = ",".join(categories_from_cfg(filter_cfg))

    if not (os.getenv("VTT_ALLOWED_TITLE_PREFIXES") or "").strip():
        os.environ["VTT_ALLOWED_TITLE_PREFIXES"] = ",".join(prefixes_from_cfg(filter_cfg))


# ----------------------------- diagnostics helpers -----------------------------


def _fallback_print_summary(
    *,
    version: str,
    before: int,
    after: int,
    raw_out_file: str,
    out_file: str,
    qg,
    availability_true: int,
    availability_false: int,
) -> None:
    print("=" * 72)
    print("[VTT] build summary")
    print("=" * 72)
    print("version:", version)
    print(f"before: {before}")
    print(f"after:  {after}")
    print(f"raw_out_file: {raw_out_file}")
    print(f"out_file: {out_file}")
    print("-" * 72)
    print(f"quality_gate_ok:       {qg.ok}")
    print(f"quality_gate_report:   {qg.report_path}")
    print(f"quality_gate_critical: {qg.critical_count}")
    print(f"quality_gate_cosmetic: {qg.cosmetic_count}")
    print(f"availability_true:     {availability_true}")
    print(f"availability_false:    {availability_false}")
    print("=" * 72)



def _print_summary(
    *,
    version: str,
    before: int,
    after: int,
    raw_out_file: str,
    out_file: str,
    qg,
    availability_true: int,
    availability_false: int,
) -> None:
    if diagnostics_print_build_summary is not None:
        diagnostics_print_build_summary(
            version=version,
            before=before,
            after=after,
            raw_out_file=raw_out_file,
            out_file=out_file,
            qg=qg,
            availability_true=availability_true,
            availability_false=availability_false,
        )
        return
    _fallback_print_summary(
        version=version,
        before=before,
        after=after,
        raw_out_file=raw_out_file,
        out_file=out_file,
        qg=qg,
        availability_true=availability_true,
        availability_false=availability_false,
    )


# ----------------------------- shard helpers -----------------------------


def _offer_to_dict(offer: OfferOut) -> dict[str, Any]:
    return {
        "oid": str(offer.oid),
        "available": bool(offer.available),
        "name": str(offer.name or ""),
        "price": int(offer.price or 0),
        "pictures": [str(x) for x in (offer.pictures or [])],
        "vendor": str(offer.vendor or ""),
        "params": [[str(k), str(v)] for k, v in (offer.params or [])],
        "native_desc": str(offer.native_desc or ""),
    }



def _dict_to_offer(row: dict[str, Any]) -> OfferOut:
    return OfferOut(
        oid=str(row["oid"]),
        available=bool(row.get("available", True)),
        name=str(row.get("name", "")),
        price=int(row.get("price", 0)),
        pictures=[str(x) for x in (row.get("pictures") or [])],
        vendor=str(row.get("vendor", "")),
        params=[(str(k), str(v)) for k, v in (row.get("params") or [])],
        native_desc=str(row.get("native_desc", "")),
    )



def _safe_write_json(path: Path, payload: dict[str, Any] | list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# ----------------------------- source/build helpers -----------------------------


def _login_or_raise(cfg):
    sess = make_session(cfg)
    if not login(sess, cfg):
        msg = "VTT: авторизация не прошла (проверь VTT_LOGIN/VTT_PASSWORD или доступность сайта)."
        if getattr(cfg, "softfail", False):
            log("[SOFTFAIL] " + msg)
            return None
        raise RuntimeError(msg)
    return sess



def _collect_index(cfg) -> list[dict[str, Any]]:
    deadline = datetime.utcnow() + timedelta(minutes=max(1.0, float(cfg.max_crawl_minutes)))
    sess = _login_or_raise(cfg)
    if sess is None:
        return []
    return collect_product_index(sess, cfg, list(cfg.categories), deadline)



def _build_offers_for_index(cfg, index: list[dict[str, Any]], *, id_prefix: str) -> list[OfferOut]:
    deadline = datetime.utcnow() + timedelta(minutes=max(1.0, float(cfg.max_crawl_minutes)))
    sess = _login_or_raise(cfg)
    if sess is None:
        return []

    out_offers: list[OfferOut] = []
    seen_oids: set[str] = set()

    if index:
        thread_state = threading.local()

        def parse_worker(item: dict[str, Any]):
            worker_sess = getattr(thread_state, "sess", None)
            if worker_sess is None:
                worker_sess = clone_session_with_cookies(sess, cfg)
                thread_state.sess = worker_sess
            return parse_product_page_from_index(worker_sess, cfg, item)

        with ThreadPoolExecutor(max_workers=max(1, int(cfg.max_workers))) as pool:
            futures = []
            for item in index:
                if datetime.utcnow() >= deadline:
                    break
                futures.append(pool.submit(parse_worker, item))

            for fut in as_completed(futures):
                if datetime.utcnow() >= deadline:
                    break
                try:
                    raw = fut.result()
                except Exception as exc:
                    log(f"[VTT] product parse error: {exc}")
                    continue
                if not raw:
                    continue
                offer = build_offer_from_raw(raw, id_prefix=id_prefix)
                if not offer:
                    continue
                if offer.oid in seen_oids:
                    continue
                seen_oids.add(offer.oid)
                out_offers.append(offer)

    out_offers.sort(key=lambda o: o.oid)
    return out_offers



def _run_quality_gate(*, raw_out_file: str, qg_cfg: dict[str, Any]):
    if not bool(qg_cfg.get("enabled", True)):
        class _QG:
            ok = True
            report_path = str(qg_cfg.get("report_path") or qg_cfg.get("report_file") or VTT_QG_REPORT_DEFAULT)
            critical_count = 0
            cosmetic_count = 0
        return _QG()

    report_path = str(qg_cfg.get("report_path") or qg_cfg.get("report_file") or VTT_QG_REPORT_DEFAULT)
    return run_quality_gate(feed_path=raw_out_file, report_path=report_path)



def _write_feeds(
    *,
    offers: list[OfferOut],
    supplier_name: str,
    supplier_url: str,
    out_file: str,
    raw_out_file: str,
    build_time,
    next_run,
    before: int,
    encoding: str,
    param_priority: tuple[str, ...],
) -> None:
    write_cs_feed_raw(
        offers,
        supplier=supplier_name,
        supplier_url=supplier_url,
        out_file=raw_out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        currency_id="KZT",
    )

    write_cs_feed(
        offers,
        supplier=supplier_name,
        supplier_url=supplier_url,
        out_file=out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        public_vendor=get_public_vendor(supplier_name),
        currency_id="KZT",
        param_priority=param_priority,
    )


# ----------------------------- mode handlers -----------------------------


def _run_index(cfg_dir: Path, filter_cfg: dict[str, Any], schema_cfg: dict[str, Any], policy_cfg: dict[str, Any]) -> int:
    _prepare_source_env(cfg_dir, filter_cfg)
    cfg = cfg_from_env()
    index = _collect_index(cfg)
    payload = {
        "categories": list(cfg.categories),
        "total": len(index),
        "index": index,
    }
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    _safe_write_json(INDEX_FILE, payload)
    _safe_write_json(
        SHARDS_DIR / "index_summary.json",
        {"total": len(index), "categories": list(cfg.categories)},
    )

    print("=" * 72)
    print("[VTT] index summary")
    print("=" * 72)
    print("version:", BUILD_VTT_VERSION)
    print("index_file:", INDEX_FILE)
    print("total:", len(index))
    print("categories:", ",".join(cfg.categories))
    print("=" * 72)
    return 0



def _run_shard_index(cfg_dir: Path, filter_cfg: dict[str, Any], schema_cfg: dict[str, Any], policy_cfg: dict[str, Any]) -> int:
    _prepare_source_env(cfg_dir, filter_cfg)
    cfg = cfg_from_env()

    shard_name = (os.getenv("VTT_SHARD_NAME") or "shard").strip() or "shard"
    shard_total = max(1, int((os.getenv("VTT_SHARD_TOTAL") or "5").strip() or "5"))
    shard_no = int((os.getenv("VTT_SHARD_NO") or "0").strip() or "0")
    id_prefix = _resolve_id_prefix(policy_cfg, schema_cfg)

    if not INDEX_FILE.exists():
        raise RuntimeError(f"VTT index file not found: {INDEX_FILE}")

    payload = json.loads(INDEX_FILE.read_text(encoding="utf-8"))
    full_index = list(payload.get("index") or [])
    before = len(full_index)
    shard_index = [item for i, item in enumerate(full_index) if i % shard_total == shard_no]

    offers = _build_offers_for_index(cfg, shard_index, id_prefix=id_prefix)

    shard_payload = {
        "shard_name": shard_name,
        "shard_no": shard_no,
        "shard_total": shard_total,
        "before": before,
        "shard_input": len(shard_index),
        "after": len(offers),
        "offers": [_offer_to_dict(x) for x in offers],
    }
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    _safe_write_json(SHARDS_DIR / f"{shard_name}.json", shard_payload)
    _safe_write_json(
        SHARDS_DIR / f"{shard_name}_summary.json",
        {
            "shard_name": shard_name,
            "shard_no": shard_no,
            "shard_total": shard_total,
            "before": before,
            "shard_input": len(shard_index),
            "after": len(offers),
        },
    )

    print("=" * 72)
    print("[VTT] shard summary")
    print("=" * 72)
    print("version:", BUILD_VTT_VERSION)
    print("shard_name:", shard_name)
    print("shard_no:", shard_no)
    print("shard_total:", shard_total)
    print("before:", before)
    print("shard_input:", len(shard_index))
    print("after:", len(offers))
    print("json:", SHARDS_DIR / f"{shard_name}.json")
    print("=" * 72)
    return 0



def _load_shards() -> tuple[list[OfferOut], int]:
    offers: list[OfferOut] = []
    seen_oids: set[str] = set()

    shard_files = sorted(SHARDS_DIR.glob("*.json"))
    shard_files = [
        x for x in shard_files
        if not x.name.endswith("_summary.json")
        and x.name not in {"merge_summary.json", "index.json"}
    ]
    if not shard_files:
        raise RuntimeError("No VTT shard JSON files found for merge.")

    before = 0
    if INDEX_FILE.exists():
        try:
            payload = json.loads(INDEX_FILE.read_text(encoding="utf-8"))
            before = int(payload.get("total") or 0)
        except Exception:
            before = 0

    for path in shard_files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for row in payload.get("offers", []):
            offer = _dict_to_offer(row)
            if offer.oid in seen_oids:
                continue
            seen_oids.add(offer.oid)
            offers.append(offer)

    offers.sort(key=lambda x: x.oid)
    return offers, before



def _run_merge(cfg_dir: Path, filter_cfg: dict[str, Any], schema_cfg: dict[str, Any], policy_cfg: dict[str, Any]) -> int:
    _prepare_source_env(cfg_dir, filter_cfg)
    cfg = cfg_from_env()

    supplier_name = _resolve_supplier_name(policy_cfg, schema_cfg)
    out_file, raw_out_file = _resolve_paths()
    output_encoding = _resolve_output_encoding(policy_cfg, schema_cfg)
    hour = _resolve_hour(policy_cfg, schema_cfg)
    dom = _resolve_dom_list(policy_cfg, schema_cfg)
    qg_cfg = _resolve_quality_gate(policy_cfg, schema_cfg)
    param_priority = _load_param_priority(policy_cfg, schema_cfg)

    build_time = now_almaty().replace(tzinfo=None)
    next_run = next_run_dom_at_hour(build_time, hour, dom)

    offers, before = _load_shards()
    if not offers:
        raise RuntimeError("VTT merge: 0 offers after shard merge.")

    _write_feeds(
        offers=offers,
        supplier_name=supplier_name,
        supplier_url=cfg.start_url,
        out_file=out_file,
        raw_out_file=raw_out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=output_encoding,
        param_priority=param_priority,
    )

    qg = _run_quality_gate(raw_out_file=raw_out_file, qg_cfg=qg_cfg)
    availability_true = sum(1 for o in offers if o.available)
    availability_false = len(offers) - availability_true

    _safe_write_json(
        SHARDS_DIR / "merge_summary.json",
        {
            "before": before,
            "after": len(offers),
            "quality_gate_ok": bool(qg.ok),
            "quality_gate_critical": int(qg.critical_count),
            "quality_gate_cosmetic": int(qg.cosmetic_count),
        },
    )

    _print_summary(
        version=BUILD_VTT_VERSION,
        before=before,
        after=len(offers),
        raw_out_file=raw_out_file,
        out_file=out_file,
        qg=qg,
        availability_true=availability_true,
        availability_false=availability_false,
    )
    return 0 if qg.ok else 1



def _run_full(cfg_dir: Path, filter_cfg: dict[str, Any], schema_cfg: dict[str, Any], policy_cfg: dict[str, Any]) -> int:
    _prepare_source_env(cfg_dir, filter_cfg)
    cfg = cfg_from_env()

    supplier_name = _resolve_supplier_name(policy_cfg, schema_cfg)
    id_prefix = _resolve_id_prefix(policy_cfg, schema_cfg)
    out_file, raw_out_file = _resolve_paths()
    output_encoding = _resolve_output_encoding(policy_cfg, schema_cfg)
    hour = _resolve_hour(policy_cfg, schema_cfg)
    dom = _resolve_dom_list(policy_cfg, schema_cfg)
    qg_cfg = _resolve_quality_gate(policy_cfg, schema_cfg)
    param_priority = _load_param_priority(policy_cfg, schema_cfg)

    build_time = now_almaty().replace(tzinfo=None)
    next_run = next_run_dom_at_hour(build_time, hour, dom)

    full_index = _collect_index(cfg)
    before = len(full_index)
    offers = _build_offers_for_index(cfg, full_index, id_prefix=id_prefix)
    after = len(offers)

    if not offers:
        msg = "VTT: 0 offers после source/builder (скорее всего сайт недоступен или изменилась верстка)."
        if getattr(cfg, "softfail", False):
            log("[SOFTFAIL] " + msg)
            return 0
        raise RuntimeError(msg)

    _write_feeds(
        offers=offers,
        supplier_name=supplier_name,
        supplier_url=cfg.start_url,
        out_file=out_file,
        raw_out_file=raw_out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=output_encoding,
        param_priority=param_priority,
    )

    qg = _run_quality_gate(raw_out_file=raw_out_file, qg_cfg=qg_cfg)
    availability_true = sum(1 for o in offers if o.available)
    availability_false = after - availability_true

    _print_summary(
        version=BUILD_VTT_VERSION,
        before=before,
        after=after,
        raw_out_file=raw_out_file,
        out_file=out_file,
        qg=qg,
        availability_true=availability_true,
        availability_false=availability_false,
    )
    return 0 if qg.ok else 1


# ----------------------------- main -----------------------------


def main() -> int:
    cfg_dir = Path(os.getenv("VTT_CFG_DIR", CFG_DIR_DEFAULT))
    filter_cfg, schema_cfg, policy_cfg = _load_supplier_config(cfg_dir)

    mode = (os.getenv("VTT_BUILD_MODE") or "full").strip().lower()
    if mode == "index":
        return _run_index(cfg_dir, filter_cfg, schema_cfg, policy_cfg)
    if mode == "shard_index":
        return _run_shard_index(cfg_dir, filter_cfg, schema_cfg, policy_cfg)
    if mode == "merge":
        return _run_merge(cfg_dir, filter_cfg, schema_cfg, policy_cfg)
    return _run_full(cfg_dir, filter_cfg, schema_cfg, policy_cfg)


if __name__ == "__main__":
    raise SystemExit(main())
