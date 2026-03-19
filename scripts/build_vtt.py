# -*- coding: utf-8 -*-
"""
CS adapter: VTT (wave1 safe split)

Шаг 1:
- выносим source.py
- выносим filtering.py
- выносим params_page.py
- остальную рабочую логику пока оставляем в build_vtt.py,
  чтобы не ломать поставщика.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from cs.core import (
    OfferOut,
    clean_params,
    compute_price,
    get_public_vendor,
    next_run_dom_at_hour,
    now_almaty,
    norm_ws,
    safe_int,
    write_cs_feed,
    write_cs_feed_raw,
)

from suppliers.vtt.source import (
    VttCfg,
    cfg_from_env,
    log,
    make_session,
    login,
    clone_session_with_cookies,
)
from suppliers.vtt.filtering import collect_all_links
from suppliers.vtt.params_page import (
    extract_pairs,
    extract_price,
    extract_title,
    extract_meta_desc,
    extract_body_text,
    extract_pictures,
    soup_from_bytes,
    get_bytes,
)

SUPPLIER = "VTT"
OID_PREFIX = "VT"
OUT_FILE = "docs/vtt.yml"


def _ru_to_lat_ascii(s: str) -> str:
    table = str.maketrans(
        {
            "А": "A",
            "В": "B",
            "Е": "E",
            "К": "K",
            "М": "M",
            "Н": "H",
            "О": "O",
            "Р": "P",
            "С": "C",
            "Т": "T",
            "Х": "X",
            "а": "a",
            "е": "e",
            "о": "o",
            "р": "p",
            "с": "c",
            "х": "x",
        }
    )
    return (s or "").translate(table)


def _clean_article(article: str) -> str:
    s = _ru_to_lat_ascii((article or "").strip())
    return re.sub(r"[^A-Za-z0-9_-]+", "", s)


def _normalize_vendor(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    key = re.sub(r"[^a-z0-9]+", "", v.lower())
    alias = {
        "hewlettpackard": "HP",
        "hp": "HP",
        "kyocera": "Kyocera",
        "canon": "Canon",
        "xerox": "Xerox",
        "brother": "Brother",
        "samsung": "Samsung",
        "epson": "Epson",
        "ricoh": "Ricoh",
        "konica": "Konica Minolta",
    }
    return alias.get(key, v)


def _parse_product(s, cfg: VttCfg, url: str, cat_code: str) -> OfferOut | None:
    b = get_bytes(s, cfg, url)
    if not b:
        return None
    sp = soup_from_bytes(b)

    name = norm_ws(extract_title(sp))
    if not name:
        return None

    pairs = extract_pairs(sp)
    article = (pairs.get("Артикул") or pairs.get("Партс-номер") or "").strip()
    if not article:
        return None

    article_clean = _clean_article(article)
    if not article_clean:
        return None

    oid = OID_PREFIX + article_clean
    vendor = _normalize_vendor((pairs.get("Вендор") or "").strip())

    supplier_price = extract_price(sp)
    price = compute_price(safe_int(supplier_price))

    pics = extract_pictures(cfg, sp)

    drop = {"артикул", "партс-номер", "вендор", "цена", "стоимость", "категория", "подкатегория", "штрих-код", "штрихкод", "ean", "barcode"}
    params: list[tuple[str, str]] = []
    for k, v in pairs.items():
        kk = norm_ws(k)
        vv = norm_ws(v)
        if not kk or not vv:
            continue
        if kk.casefold() in drop:
            continue
        params.append((kk, vv))

    params = clean_params(params)

    meta_desc = extract_meta_desc(sp)
    body_txt = extract_body_text(sp)
    native_desc = meta_desc
    if body_txt and body_txt not in (native_desc or ""):
        native_desc = (native_desc + "\n" + body_txt).strip() if native_desc else body_txt

    return OfferOut(
        oid=oid,
        available=True,
        name=name,
        price=int(price),
        pictures=pics,
        vendor=vendor,
        params=params,
        native_desc=native_desc,
    )


def _build_offers(s, cfg: VttCfg, deadline_utc: datetime) -> tuple[list[OfferOut], int]:
    links = collect_all_links(s, cfg, deadline_utc)
    log(f"[site] urls={len(links)} workers={cfg.max_workers}")

    offers: list[OfferOut] = []
    seen: set[str] = set()
    dup = 0

    if not links:
        return offers, dup

    with ThreadPoolExecutor(max_workers=max(1, cfg.max_workers)) as ex:
        futs = []
        for url, code in links:
            if datetime.utcnow() >= deadline_utc:
                break
            sess = clone_session_with_cookies(s, cfg)
            futs.append(ex.submit(_parse_product, sess, cfg, url, code))

        for fut in as_completed(futs):
            o = fut.result()
            if not o:
                continue
            if o.oid in seen:
                dup += 1
                continue
            seen.add(o.oid)
            offers.append(o)

    offers.sort(key=lambda x: x.oid)
    return offers, dup


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

    offers, dup = _build_offers(s, cfg, deadline)

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
