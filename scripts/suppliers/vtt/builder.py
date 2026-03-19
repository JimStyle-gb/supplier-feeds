# -*- coding: utf-8 -*-
"""VTT builder layer — wave2.

Переносим parse/build в supplier-layer, чтобы raw уже был ближе к CS-шаблону.
"""

from __future__ import annotations

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from cs.core import OfferOut, compute_price, safe_int, norm_ws
from suppliers.vtt.source import clone_session_with_cookies
from suppliers.vtt.params_page import (
    get_bytes,
    soup_from_bytes,
    extract_title,
    extract_pairs,
    extract_price,
    extract_meta_desc,
    extract_body_text,
    extract_pictures,
)
from suppliers.vtt.normalize import normalize_name, infer_vendor, build_clean_params

OID_PREFIX = "VT"

def clean_article(article: str) -> str:
    import re
    s = (article or "").strip()
    s = s.translate(str.maketrans({
        "А":"A","В":"B","Е":"E","К":"K","М":"M","Н":"H","О":"O","Р":"P","С":"C","Т":"T","Х":"X",
        "а":"a","е":"e","о":"o","р":"p","с":"c","х":"x",
    }))
    return re.sub(r"[^A-Za-z0-9_-]+", "", s)

def build_native_desc(name: str, params: list[tuple[str, str]], meta_desc: str, body_txt: str) -> str:
    # если есть нормальный meta/body — используем их
    meta_desc = (meta_desc or "").strip()
    body_txt = (body_txt or "").strip()
    if meta_desc and body_txt and body_txt not in meta_desc:
        return (meta_desc + "\n" + body_txt).strip()
    if body_txt:
        return body_txt
    if meta_desc:
        return meta_desc

    # иначе строим короткий technical raw-desc
    d = {k: v for k, v in params}
    bits = [name]
    if d.get("Тип"):
        bits.append(f"Тип: {d['Тип']}.")
    if d.get("Совместимость"):
        bits.append(f"Совместимость: {d['Совместимость']}.")
    if d.get("Цвет"):
        bits.append(f"Цвет: {d['Цвет']}.")
    if d.get("Ресурс"):
        bits.append(f"Ресурс: {d['Ресурс']}.")
    return " ".join(x for x in bits if x).strip()

def build_offer_from_page(s, cfg, url: str, cat_code: str) -> OfferOut | None:
    b = get_bytes(s, cfg, url)
    if not b:
        return None
    sp = soup_from_bytes(b)

    name = normalize_name(norm_ws(extract_title(sp)))
    if not name:
        return None

    pairs = extract_pairs(sp)
    article = (pairs.get("Артикул") or pairs.get("Партс-номер") or pairs.get("OEM-номер") or pairs.get("Каталожный номер") or "").strip()
    if not article:
        return None

    article_clean = clean_article(article)
    if not article_clean:
        return None

    oid = OID_PREFIX + article_clean
    vendor = infer_vendor(name, (pairs.get("Вендор") or "").strip())

    supplier_price = extract_price(sp)
    price = compute_price(safe_int(supplier_price))

    pics = extract_pictures(cfg, sp)
    params = build_clean_params(name, vendor, pairs)

    meta_desc = extract_meta_desc(sp)
    body_txt = extract_body_text(sp)
    native_desc = build_native_desc(name, params, meta_desc, body_txt)

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

def build_offers(s, cfg, links: list[tuple[str, str]], deadline_utc: datetime) -> tuple[list[OfferOut], int]:
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
            futs.append(ex.submit(build_offer_from_page, sess, cfg, url, code))

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
