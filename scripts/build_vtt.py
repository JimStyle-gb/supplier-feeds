# -*- coding: utf-8 -*-
"""
CS adapter: VTT (wave2 thin orchestrator)

Wave2:
- normalize.py
- builder.py
source/filtering/params_page остаются из wave1.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from cs.core import (
    get_public_vendor,
    next_run_dom_at_hour,
    now_almaty,
    write_cs_feed,
    write_cs_feed_raw,
)

from suppliers.vtt.source import (
    cfg_from_env,
    log,
    make_session,
    login,
)
from suppliers.vtt.filtering import collect_all_links
from suppliers.vtt.builder import build_offers

SUPPLIER = "VTT"
OUT_FILE = "docs/vtt.yml"


def main() -> int:
    cfg = cfg_from_env()
    now = now_almaty()
    now_naive = now.replace(tzinfo=None)
    deadline = datetime.utcnow() + timedelta(minutes=cfg.max_crawl_minutes)

    s = make_session(cfg)

    if not login(s, cfg):
        msg = "VTT: авторизация не прошла (проверь VTT_LOGIN/VTT_PASSWORD). Если в логах 503/5xx — проблема на стороне сайта."
        if cfg.softfail:
            log("[SOFTFAIL] " + msg)
            return 0
        raise RuntimeError(msg)

    links = collect_all_links(s, cfg, deadline)
    log(f"[site] urls={len(links)} workers={cfg.max_workers}")

    offers, dup = build_offers(s, cfg, links, deadline)

    if not offers:
        msg = "VTT: 0 offers (скорее всего сайт недоступен/503 или изменили верстку)."
        if cfg.softfail:
            log("[SOFTFAIL] " + msg)
            return 0
        raise RuntimeError(msg)

    next_run = next_run_dom_at_hour(now_naive, 5, (1, 10, 20))
    public_vendor = get_public_vendor(SUPPLIER)

    write_cs_feed_raw(
        offers,
        supplier=SUPPLIER,
        supplier_url=cfg.start_url,
        out_file="docs/raw/vtt.yml",
        build_time=now,
        next_run=next_run,
        before=len(offers),
        encoding="utf-8",
        currency_id="KZT",
    )

    changed = write_cs_feed(
        offers,
        supplier=SUPPLIER,
        supplier_url=cfg.start_url,
        out_file=OUT_FILE,
        build_time=now,
        next_run=next_run,
        before=len(offers),
        encoding="utf-8",
        public_vendor=public_vendor,
        currency_id="KZT",
        param_priority=None,
    )

    log(f"[done] offers={len(offers)} dup_skipped={dup} changed={changed} out={OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
