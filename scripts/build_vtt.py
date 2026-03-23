# -*- coding: utf-8 -*-
"""
Path: scripts/build_vtt.py

VTT adapter.
v8:
- supports normal full build;
- supports shard build via VTT_BUILD_MODE=shard;
- supports shard merge via VTT_BUILD_MODE=merge;
- keeps supplier logic identical for item parsing/building;
- uses shard mode only to reduce wall-clock time.
"""

from __future__ import annotations

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

from cs.core import (
    OfferOut,
    get_public_vendor,
    next_run_dom_at_hour,
    now_almaty,
    write_cs_feed,
    write_cs_feed_raw,
)
from suppliers.vtt.builder import build_offer_from_raw
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
VTT_QG_REPORT = os.getenv("VTT_QG_REPORT", "docs/raw/vtt_quality_gate.txt")
ROOT = Path(__file__).resolve().parents[1]
SHARDS_DIR = ROOT / "docs" / "debug" / "vtt_shards"


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


def _offer_to_dict(offer: OfferOut) -> dict:
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


def _dict_to_offer(row: dict) -> OfferOut:
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


def _safe_write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_offers_for_cfg(cfg) -> tuple[list[OfferOut], int, list[str]]:
    deadline = datetime.utcnow() + timedelta(minutes=max(1.0, float(cfg.max_crawl_minutes)))

    sess = make_session(cfg)
    if not login(sess, cfg):
        msg = "VTT: авторизация не прошла (проверь VTT_LOGIN/VTT_PASSWORD или доступность сайта)."
        if cfg.softfail:
            log("[SOFTFAIL] " + msg)
            return [], 0, []
        raise RuntimeError(msg)

    index = collect_product_index(sess, cfg, list(cfg.categories), deadline)
    before = len(index)
    index_urls = [str(item.get("url") or "") for item in index if str(item.get("url") or "")]

    out_offers: list[OfferOut] = []
    seen_oids: set[str] = set()

    if index:
        thread_state = threading.local()

        def parse_worker(item: dict):
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
                offer = build_offer_from_raw(raw, id_prefix="VT")
                if not offer:
                    continue
                if offer.oid in seen_oids:
                    continue
                seen_oids.add(offer.oid)
                out_offers.append(offer)

    out_offers.sort(key=lambda o: o.oid)
    return out_offers, before, index_urls


def _run_shard() -> int:
    shard_name = (os.getenv("VTT_SHARD_NAME") or "shard").strip() or "shard"
    cfg = cfg_from_env()
    offers, before, index_urls = _build_offers_for_cfg(cfg)

    payload = {
        "shard_name": shard_name,
        "categories": list(cfg.categories),
        "before": before,
        "index_urls": index_urls,
        "after": len(offers),
        "offers": [_offer_to_dict(x) for x in offers],
    }
    SHARDS_DIR.mkdir(parents=True, exist_ok=True)
    _safe_write_json(SHARDS_DIR / f"{shard_name}.json", payload)
    _safe_write_json(
        SHARDS_DIR / f"{shard_name}_summary.json",
        {
            "shard_name": shard_name,
            "categories": list(cfg.categories),
            "before": before,
            "after": len(offers),
        },
    )

    print("=" * 72)
    print("[VTT] shard summary")
    print("=" * 72)
    print("shard_name:", shard_name)
    print("categories:", ",".join(cfg.categories))
    print("before:", before)
    print("after:", len(offers))
    print("json:", SHARDS_DIR / f"{shard_name}.json")
    print("=" * 72)
    return 0


def _load_shards() -> tuple[list[OfferOut], int]:
    offers: list[OfferOut] = []
    seen_oids: set[str] = set()
    all_index_urls: set[str] = set()

    shard_files = sorted(SHARDS_DIR.glob("*.json"))
    shard_files = [x for x in shard_files if not x.name.endswith("_summary.json") and x.name != "merge_summary.json"]
    if not shard_files:
        raise RuntimeError("No VTT shard JSON files found for merge.")

    for path in shard_files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for url in payload.get("index_urls", []):
            if url:
                all_index_urls.add(str(url))
        for row in payload.get("offers", []):
            offer = _dict_to_offer(row)
            if offer.oid in seen_oids:
                continue
            seen_oids.add(offer.oid)
            offers.append(offer)

    offers.sort(key=lambda x: x.oid)
    return offers, len(all_index_urls)


def _run_merge() -> int:
    cfg = cfg_from_env()
    build_time = now_almaty().replace(tzinfo=None)
    next_run = next_run_dom_at_hour(build_time, 5, (1, 10, 20))

    offers, before = _load_shards()
    if not offers:
        raise RuntimeError("VTT merge: 0 offers after shard merge.")

    write_cs_feed_raw(
        offers,
        supplier=SUPPLIER_NAME,
        supplier_url=cfg.start_url,
        out_file=RAW_OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        currency_id="KZT",
    )

    write_cs_feed(
        offers,
        supplier=SUPPLIER_NAME,
        supplier_url=cfg.start_url,
        out_file=OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        public_vendor=get_public_vendor(SUPPLIER_NAME),
        currency_id="KZT",
        param_priority=(
            "Тип",
            "Для бренда",
            "Партномер",
            "Коды расходников",
            "Совместимость",
            "Технология печати",
            "Цвет",
            "Ресурс",
            "Объем",
        ),
    )

    qg = run_quality_gate(feed_path=RAW_OUT_FILE, report_path=VTT_QG_REPORT)
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
        version="build_vtt_v8_merge_shards",
        before=before,
        after=len(offers),
        raw_out_file=RAW_OUT_FILE,
        out_file=OUT_FILE,
        qg=qg,
        availability_true=availability_true,
        availability_false=availability_false,
    )
    return 0 if qg.ok else 1


def _run_full() -> int:
    cfg = cfg_from_env()
    build_time = now_almaty().replace(tzinfo=None)
    next_run = next_run_dom_at_hour(build_time, 5, (1, 10, 20))

    out_offers, before, _ = _build_offers_for_cfg(cfg)
    after = len(out_offers)

    if not out_offers:
        msg = "VTT: 0 offers после source/builder (скорее всего сайт недоступен или изменилась верстка)."
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

    write_cs_feed(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=cfg.start_url,
        out_file=OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        public_vendor=get_public_vendor(SUPPLIER_NAME),
        currency_id="KZT",
        param_priority=(
            "Тип",
            "Для бренда",
            "Партномер",
            "Коды расходников",
            "Совместимость",
            "Технология печати",
            "Цвет",
            "Ресурс",
            "Объем",
        ),
    )

    qg = run_quality_gate(feed_path=RAW_OUT_FILE, report_path=VTT_QG_REPORT)
    availability_true = sum(1 for o in out_offers if o.available)
    availability_false = after - availability_true

    _print_summary(
        version="build_vtt_v8_full_or_sharded",
        before=before,
        after=after,
        raw_out_file=RAW_OUT_FILE,
        out_file=OUT_FILE,
        qg=qg,
        availability_true=availability_true,
        availability_false=availability_false,
    )
    return 0 if qg.ok else 1


def main() -> int:
    mode = (os.getenv("VTT_BUILD_MODE") or "full").strip().lower()
    if mode == "shard":
        return _run_shard()
    if mode == "merge":
        return _run_merge()
    return _run_full()


if __name__ == "__main__":
    raise SystemExit(main())
