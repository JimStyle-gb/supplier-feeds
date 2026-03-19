# -*- coding: utf-8 -*-
"""
Path: scripts/debug_vtt_probe.py

Диагностический пробник VTT.
Ходит по тем же ссылкам, что и основной build_vtt.py, и пишет сырой probe-YML
для ручной сверки цен/страниц.

Что делает:
- логинится как обычный VTT crawler
- собирает те же product links через suppliers.vtt.filtering.collect_all_links
- парсит страницы через текущий suppliers.vtt.params_page
- пишет docs/raw/vtt_probe.yml

ENV:
- VTT_LOGIN
- VTT_PASSWORD

Опционально:
- VTT_PROBE_LIMIT=50              # сколько карточек пробить, default 50
- VTT_PROBE_MATCH=VT005402        # фильтр по oid/article/title/url substring
- VTT_PROBE_FULL=1                # если 1, не ограничивать limit
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

OUT_FILE = "docs/raw/vtt_probe.yml"
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


def _first(sel_sp, selectors: tuple[str, ...]) -> str:
    for sel in selectors:
        el = sel_sp.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            if txt:
                return txt
    return ""


def _get_hidden_amount(sp) -> str:
    inp = sp.select_one('input[name="amount"]')
    if inp and inp.get("value"):
        return str(inp.get("value")).strip()
    return ""


def _get_visible_price_text(sp) -> str:
    return _first(
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
    return _first(
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


def _iter_probe_rows(s, cfg, links: list[tuple[str, str]], deadline_utc: datetime, limit: int | None, match: str):
    out = []
    seen = set()
    match_l = (match or "").strip().lower()

    for idx, (url, cat_code) in enumerate(links, 1):
        if datetime.utcnow() >= deadline_utc:
            break
        if limit is not None and len(out) >= limit:
            break

        sess = clone_session_with_cookies(s, cfg)
        raw = get_bytes(sess, cfg, url)
        if not raw:
            continue

        sp = soup_from_bytes(raw)
        title = (extract_title(sp) or "").strip()
        pairs = extract_pairs(sp)
        article = _pick_article(pairs)
        oid = OID_PREFIX + _clean_article(article) if article else ""

        hay = " | ".join([
            oid,
            article,
            title,
            url,
            cat_code or "",
            pairs.get("OEM-номер", "") or "",
            pairs.get("Каталожный номер", "") or "",
        ]).lower()

        if match_l and match_l not in hay:
            continue

        if oid and oid in seen:
            continue
        if oid:
            seen.add(oid)

        visible_price_text = _get_visible_price_text(sp)
        hidden_amount = _get_hidden_amount(sp)
        parsed_price = extract_price(sp)
        meta_desc = extract_meta_desc(sp)
        body_txt = extract_body_text(sp)
        add_to_cart = _get_add_to_cart_text(sp)

        row = {
            "oid": oid or f"ROW{len(out)+1}",
            "url": url,
            "category": cat_code or "",
            "title": title,
            "article": article,
            "oem": (pairs.get("OEM-номер") or "").strip(),
            "catalog": (pairs.get("Каталожный номер") or "").strip(),
            "parsed_price": str(parsed_price or ""),
            "visible_price_text": visible_price_text,
            "hidden_amount": hidden_amount,
            "add_to_cart": add_to_cart,
            "meta_desc": meta_desc,
            "body_excerpt": (body_txt or "")[:500],
        }
        out.append(row)
        log(f"[probe] {len(out)} oid={row['oid']} parsed_price={row['parsed_price']} visible='{visible_price_text}' url={url}")

    return out


def _param_xml(name: str, value: str) -> str:
    if not value:
        return ""
    return f'    <param name="{escape(name)}">{escape(value)}</param>\n'


def _offer_xml(row: dict[str, str]) -> str:
    oid = escape(row["oid"])
    name = escape(row["title"] or row["article"] or row["oid"])
    price = escape(row["parsed_price"] or "0")
    body = []
    body.append(f'  <offer id="{oid}" available="true">\n')
    body.append(f"    <name>{name}</name>\n")
    body.append(f"    <price>{price}</price>\n")
    body.append(_param_xml("URL", row["url"]))
    body.append(_param_xml("CategoryCode", row["category"]))
    body.append(_param_xml("Article", row["article"]))
    body.append(_param_xml("OEM", row["oem"]))
    body.append(_param_xml("CatalogNo", row["catalog"]))
    body.append(_param_xml("VisiblePriceText", row["visible_price_text"]))
    body.append(_param_xml("HiddenAmount", row["hidden_amount"]))
    body.append(_param_xml("AddToCartText", row["add_to_cart"]))
    body.append(_param_xml("MetaDesc", row["meta_desc"]))
    body.append(_param_xml("BodyExcerpt", row["body_excerpt"]))
    body.append("  </offer>\n")
    return "".join(body)


def main() -> int:
    cfg = cfg_from_env()
    now = datetime.utcnow()
    deadline = now + timedelta(minutes=cfg.max_crawl_minutes)

    full = (os.getenv("VTT_PROBE_FULL", "") or "").strip() in ("1", "true", "True", "yes", "YES")
    limit = None if full else int((os.getenv("VTT_PROBE_LIMIT", "50") or "50").strip())
    match = (os.getenv("VTT_PROBE_MATCH", "") or "").strip()

    s = make_session(cfg)
    if not login(s, cfg):
        raise RuntimeError("VTT probe: login failed")

    links = collect_all_links(s, cfg, deadline)
    log(f"[probe] links_before_parse={len(links)}")

    rows = _iter_probe_rows(s, cfg, links, deadline, limit=limit, match=match)
    if not rows:
        raise RuntimeError("VTT probe: 0 rows")

    out = []
    out.append('<?xml version="1.0" encoding="utf-8"?>\n')
    out.append(f'<yml_catalog date="{datetime.utcnow().strftime("%Y-%m-%d %H:%M")}">\n')
    out.append("  <shop>\n")
    out.append("    <name>VTT Probe</name>\n")
    out.append("    <company>VTT Probe</company>\n")
    out.append("    <url>https://b2b.vtt.ru/</url>\n")
    out.append("    <currencies><currency id=\"KZT\" rate=\"1\"/></currencies>\n")
    out.append("    <categories><category id=\"1\">Probe</category></categories>\n")
    out.append("    <offers>\n")
    for row in rows:
        out.append(_offer_xml(row))
    out.append("    </offers>\n")
    out.append("  </shop>\n")
    out.append("</yml_catalog>\n")

    os.makedirs("docs/raw", exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8", newline="\n") as f:
        f.write("".join(out))

    log(f"[probe] wrote rows={len(rows)} file={OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
