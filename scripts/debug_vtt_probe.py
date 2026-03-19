# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from xml.sax.saxutils import escape

from suppliers.vtt.source import cfg_from_env, log, make_session, login, clone_session_with_cookies
from suppliers.vtt.filtering import collect_all_links
from suppliers.vtt.params_page import (
    get_bytes,
    soup_from_bytes,
    extract_title,
    extract_pairs,
    extract_price,
    extract_meta_desc,
    extract_body_text,
)

OUT_YML = "docs/raw/vtt_missing_prices.yml"
OUT_SUMMARY = "docs/raw/vtt_missing_prices_summary.txt"
OID_PREFIX = "VT"


def _clean_article(article: str) -> str:
    table = str.maketrans({
        "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X",
        "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "х": "x",
    })
    s = (article or "").strip().translate(table)
    return re.sub(r"[^A-Za-z0-9_-]+", "", s)


def _pick_article(pairs: dict[str, str]) -> str:
    for k in ("Артикул", "Партс-номер", "OEM-номер", "Каталожный номер"):
        v = (pairs.get(k) or "").strip()
        if v:
            return v
    return ""


def _first_text(sp, selectors: tuple[str, ...]) -> str:
    for sel in selectors:
        el = sp.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            if txt:
                return txt
    return ""


def _get_hidden_amount(sp) -> str:
    inp = sp.select_one('input[name="amount"]')
    if inp and inp.get("value") is not None:
        return str(inp.get("value")).strip()
    return ""


def _get_visible_price_text(sp) -> str:
    return _first_text(
        sp,
        (
            "span.price_main b.price-cart-amount__rub",
            "div.item_data_price span.price_main b.price-cart-amount__rub",
            "span.price_main b",
            "span.price_main",
            "div.item_data_price span.price_main",
        ),
    )


def _get_add_to_cart_text(sp) -> str:
    return _first_text(
        sp,
        (
            ".buy_block .button_cart span",
            ".buy_block .button_cart",
            ".buy_block .button_buy span",
            ".buy_block .button_buy",
            "button.button_cart span",
            "button.button_cart",
        ),
    )


def _param_xml(name: str, value: str) -> str:
    if value is None:
        value = ""
    return f'    <param name="{escape(name)}">{escape(str(value))}</param>\n'


def _offer_xml(row: dict[str, str]) -> str:
    oid = escape(row["oid"])
    name = escape(row["title"] or row["article"] or row["oid"])
    out = []
    out.append(f'  <offer id="{oid}" available="true">\n')
    out.append(f"    <name>{name}</name>\n")
    out.append("    <price>0</price>\n")
    out.append(_param_xml("URL", row["url"]))
    out.append(_param_xml("CategoryCode", row["category"]))
    out.append(_param_xml("Article", row["article"]))
    out.append(_param_xml("OEM", row["oem"]))
    out.append(_param_xml("CatalogNo", row["catalog"]))
    out.append(_param_xml("VisiblePriceText", row["visible_price_text"]))
    out.append(_param_xml("HiddenAmount", row["hidden_amount"]))
    out.append(_param_xml("AddToCartText", row["add_to_cart"]))
    out.append(_param_xml("MetaDesc", row["meta_desc"]))
    out.append(_param_xml("BodyExcerpt", row["body_excerpt"]))
    out.append("  </offer>\n")
    return "".join(out)


def _probe_one(s, cfg, idx: int, url: str, cat_code: str):
    sess = clone_session_with_cookies(s, cfg)
    raw = get_bytes(sess, cfg, url)
    if not raw:
        return {"kind": "fetch_failed", "url": url, "category": cat_code or "", "idx": idx}

    sp = soup_from_bytes(raw)
    title = (extract_title(sp) or "").strip()
    pairs = extract_pairs(sp)
    article = _pick_article(pairs)
    oid = OID_PREFIX + _clean_article(article) if article else f"ROW{idx}"
    parsed_price = extract_price(sp)

    row = {
        "oid": oid,
        "url": url,
        "category": cat_code or "",
        "title": title,
        "article": article,
        "oem": (pairs.get("OEM-номер") or "").strip(),
        "catalog": (pairs.get("Каталожный номер") or "").strip(),
        "visible_price_text": _get_visible_price_text(sp),
        "hidden_amount": _get_hidden_amount(sp),
        "add_to_cart": _get_add_to_cart_text(sp),
        "meta_desc": extract_meta_desc(sp),
        "body_excerpt": (extract_body_text(sp) or "")[:500],
        "parsed_price": parsed_price or 0,
    }

    if parsed_price:
        return {"kind": "price_found", "row": row}
    return {"kind": "price_missing", "row": row}


def main() -> int:
    cfg = cfg_from_env()
    workers = int((os.getenv("VTT_PROBE_WORKERS", "") or "").strip() or max(4, cfg.max_workers))

    s = make_session(cfg)
    if not login(s, cfg):
        raise RuntimeError("VTT missing-prices probe: login failed")

    links = collect_all_links(s, cfg, datetime.utcnow().replace(year=2099))
    log(f"[missing-price-probe] links_before_parse={len(links)} workers={workers}")

    rows_missing = []
    checked = 0
    fetch_failed = 0
    price_found = 0

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = [
            ex.submit(_probe_one, s, cfg, idx, url, cat_code)
            for idx, (url, cat_code) in enumerate(links, 1)
        ]

        for fut in as_completed(futures):
            res = fut.result()
            kind = res["kind"]

            if kind == "fetch_failed":
                fetch_failed += 1
                checked += 1
                continue

            row = res["row"]
            checked += 1

            if kind == "price_found":
                price_found += 1
                continue

            rows_missing.append(row)
            log(
                f"[missing-price-probe] miss {len(rows_missing)} oid={row['oid']} "
                f"visible='{row['visible_price_text']}' hidden='{row['hidden_amount']}' url={row['url']}"
            )

    os.makedirs("docs/raw", exist_ok=True)

    yml = []
    yml.append('<?xml version="1.0" encoding="utf-8"?>\n')
    yml.append(f'<yml_catalog date="{datetime.utcnow().strftime("%Y-%m-%d %H:%M")}">\n')
    yml.append("  <shop>\n")
    yml.append("    <name>VTT Missing Prices</name>\n")
    yml.append("    <company>VTT Missing Prices</company>\n")
    yml.append("    <url>https://b2b.vtt.ru/</url>\n")
    yml.append('    <currencies><currency id="KZT" rate="1"/></currencies>\n')
    yml.append('    <categories><category id="1">MissingPrices</category></categories>\n')
    yml.append("    <offers>\n")
    for row in rows_missing:
        yml.append(_offer_xml(row))
    yml.append("    </offers>\n")
    yml.append("  </shop>\n")
    yml.append("</yml_catalog>\n")

    with open(OUT_YML, "w", encoding="utf-8", newline="\n") as f:
        f.write("".join(yml))

    with open(OUT_SUMMARY, "w", encoding="utf-8", newline="\n") as f:
        f.write(f"links_before_parse={len(links)}\n")
        f.write(f"checked_pages={checked}\n")
        f.write(f"price_found_offers={price_found}\n")
        f.write(f"missing_price_offers={len(rows_missing)}\n")
        f.write(f"fetch_failed_pages={fetch_failed}\n")

    log(
        f"[missing-price-probe] wrote missing={len(rows_missing)} "
        f"price_found={price_found} checked={checked} fetch_failed={fetch_failed} file={OUT_YML}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
