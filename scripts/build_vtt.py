# -*- coding: utf-8 -*-
"""
Path: scripts/build_vtt.py

VTT adapter.
v6:
- same category-first logic;
- same supplier scope;
- faster page fetch with slightly higher worker count;
- no price/photo logic changes in this patch.
"""

from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from cs.core import get_public_vendor, next_run_dom_at_hour, now_almaty, write_cs_feed, write_cs_feed_raw
from suppliers.vtt.builder import build_offer_from_raw
from suppliers.vtt.quality_gate import run_quality_gate
from suppliers.vtt.source import cfg_from_env, clone_session_with_cookies, collect_product_index, login, log, make_session, parse_product_page_from_index

SUPPLIER_NAME = "VTT"
OUT_FILE = os.getenv("OUT_FILE", "docs/vtt.yml")
RAW_OUT_FILE = os.getenv("RAW_OUT_FILE", "docs/raw/vtt.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "utf-8") or "utf-8").strip() or "utf-8"
VTT_QG_REPORT = os.getenv("VTT_QG_REPORT", "docs/raw/vtt_quality_gate.txt")

def _print_summary(*, before: int, after: int, raw_out_file: str, out_file: str, qg, availability_true: int, availability_false: int) -> None:
    print("=" * 72)
    print("[VTT] build summary")
    print("=" * 72)
    print("version: build_vtt_v6_source_fast_builder_clean")
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

def main() -> int:
    cfg = cfg_from_env()
    build_time = now_almaty().replace(tzinfo=None)
    next_run = next_run_dom_at_hour(build_time, 5, (1, 10, 20))
    deadline = datetime.utcnow() + timedelta(minutes=max(1.0, float(cfg.max_crawl_minutes)))

    sess = make_session(cfg)
    if not login(sess, cfg):
        msg = "VTT: авторизация не прошла (проверь VTT_LOGIN/VTT_PASSWORD или доступность сайта)."
        if cfg.softfail:
            log("[SOFTFAIL] " + msg)
            return 0
        raise RuntimeError(msg)

    index = collect_product_index(sess, cfg, list(cfg.categories), deadline)
    before = len(index)

    out_offers: list = []
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
        param_priority=(
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
            "Категория VTT",
        ),
    )

    qg = run_quality_gate(feed_path=RAW_OUT_FILE, report_path=VTT_QG_REPORT)
    availability_true = sum(1 for o in out_offers if o.available)
    availability_false = after - availability_true

    _print_summary(
        before=before,
        after=after,
        raw_out_file=RAW_OUT_FILE,
        out_file=OUT_FILE,
        qg=qg,
        availability_true=availability_true,
        availability_false=availability_false,
    )

    return 0 if qg.ok else 1

if __name__ == "__main__":
    raise SystemExit(main())
