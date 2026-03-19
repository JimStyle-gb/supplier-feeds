# -*- coding: utf-8 -*-
"""
Path: scripts/debug_vtt_missing_prices.py

Ищет все VTT-товары, у которых текущий live-crawl НЕ находит цену.
Пишет:
- docs/raw/vtt_missing_prices.yml
- docs/raw/vtt_missing_prices_summary.txt

Логика:
- логинится как обычный VTT crawler
- собирает те же product links через suppliers.vtt.filtering.collect_all_links
- парсит страницы текущим suppliers.vtt.params_page.extract_price(...)
- если price не найден -> пишет offer в probe-YML

ENV:
- VTT_LOGIN
- VTT_PASSWORD

Опционально:
- VTT_PROBE_LIMIT=0      # 0 = без лимита
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
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


def main() -> int:
    cfg = cfg_from_env()
    deadline = datetime.utcnow() + timedelta(minutes=cfg.max_crawl_minutes)
    limit_raw = (os.getenv("VTT_PROBE_LIMIT", "0") or "0").strip()
    limit = int(limit_raw) if limit_raw.isdigit() else 0

    s = make_session(cfg)
    if not login(s, cfg):
        raise RuntimeError("VTT missing-prices probe: login failed")

    links = collect_all_links(s, cfg, deadline)
    log(f"[missing-price-probe] links_before_parse={len(links)}")

    rows = []
    checked = 0

    for idx, (url, cat_code) in enumerate(links, 1):
        if datetime.utcnow() >= deadline:
            break
        if limit and checked >= limit:
            break

        sess = clone_session_with_cookies(s, cfg)
        raw = get_bytes(sess, cfg, url)
        if not raw:
            continue

        checked += 1
        sp = soup_from_bytes(raw)
        title = (extract_title(sp) or "").strip()
        pairs = extract_pairs(sp)
        article = _pick_article(pairs)
        oid = OID_PREFIX + _clean_article(article) if article else f"ROW{idx}"
        parsed_price = extract_price(sp)

        if parsed_price:
            continue

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
        }
        rows.append(row)

        log(
            f"[missing-price-probe] miss {len(rows)} oid={oid} "
            f"visible='{row['visible_price_text']}' hidden='{row['hidden_amount']}' url={url}"
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
    for row in rows:
        yml.append(_offer_xml(row))
    yml.append("    </offers>\n")
    yml.append("  </shop>\n")
    yml.append("</yml_catalog>\n")

    with open(OUT_YML, "w", encoding="utf-8", newline="\n") as f:
        f.write("".join(yml))

    summary = []
    summary.append(f"links_before_parse={len(links)}\n")
    summary.append(f"checked_pages={checked}\n")
    summary.append(f"missing_price_offers={len(rows)}\n")
    with open(OUT_SUMMARY, "w", encoding="utf-8", newline="\n") as f:
        f.writelines(summary)

    log(f"[missing-price-probe] wrote missing={len(rows)} checked={checked} file={OUT_YML}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
