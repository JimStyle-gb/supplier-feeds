# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/source.py

AkCent supplier layer — source reader.

Задача модуля:
- скачать XML поставщика;
- распарсить offer-элементы без business-логики;
- вернуть чистые SourceOffer для supplier-layer.

Важно:
- тут нет фильтрации ассортимента;
- тут нет нормализации vendor/model/price;
- тут нет supplier-specific эвристик;
- тут нет picture-normalization;
- source.py только честно читает исходник.
"""

from __future__ import annotations

from typing import Any, Iterable
import xml.etree.ElementTree as ET

import requests

from cs.util import norm_ws
from suppliers.akcent.models import SourceOffer


DEFAULT_TIMEOUT = 90


# Текст дочернего элемента

def child_text(parent: ET.Element | None, tag: str) -> str:
    if parent is None:
        return ""
    el = parent.find(tag)
    if el is None:
        return ""
    return norm_ws(el.text or "")


# Все непустые тексты дочерних элементов

def iter_child_texts(parent: ET.Element | None, tag: str) -> Iterable[str]:
    if parent is None:
        return []
    out: list[str] = []
    for el in parent.findall(tag):
        val = norm_ws(el.text or "")
        if val:
            out.append(val)
    return out




# Родные Param поставщика как есть

def collect_raw_params(offer_el: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for p in offer_el.findall("Param"):
        key = norm_ws(p.get("name") or "")
        val = norm_ws("".join(p.itertext()))
        if not key or not val:
            continue
        out.append((key, val))
    return out


# Цены из блока prices

def collect_prices(offer_el: ET.Element) -> tuple[str, str, str]:
    dealer = ""
    rrp = ""
    fallback = ""

    prices_el = offer_el.find("prices")
    if prices_el is None:
        return dealer, rrp, fallback

    for price_el in prices_el.findall("price"):
        ptype = norm_ws(price_el.get("type") or "").casefold()
        value = norm_ws(price_el.text or "")
        if not value:
            continue

        if not fallback:
            fallback = value

        if "дилер" in ptype or "dealer" in ptype:
            if not dealer:
                dealer = value
            continue

        if ptype == "rrp" or "rrp" in ptype:
            if not rrp:
                rrp = value
            continue

    return dealer, rrp, fallback


# Один offer -> SourceOffer

def parse_offer(offer_el: ET.Element) -> SourceOffer:
    raw_id = norm_ws(offer_el.get("id") or "")
    offer_id = child_text(offer_el, "Offer_ID") or raw_id
    article = norm_ws(offer_el.get("article") or "")
    category_id = norm_ws((offer_el.find("categoryId").text if offer_el.find("categoryId") is not None and offer_el.find("categoryId").text is not None else ""))

    name = child_text(offer_el, "name")
    type_text = norm_ws(offer_el.get("type") or child_text(offer_el, "type"))
    available_attr = norm_ws(offer_el.get("available") or "")
    available_tag = child_text(offer_el, "available")

    vendor = child_text(offer_el, "vendor")
    model = child_text(offer_el, "model")
    description = child_text(offer_el, "description")
    manufacturer_warranty = child_text(offer_el, "manufacturer_warranty")
    stock_text = child_text(offer_el, "Stock")
    url = child_text(offer_el, "url")

    dealer_price_text, rrp_price_text, price_text = collect_prices(offer_el)

    return SourceOffer(
        raw_id=raw_id,
        offer_id=offer_id,
        article=article,
        category_id=category_id,
        name=name,
        type_text=type_text,
        available_attr=available_attr,
        available_tag=available_tag,
        vendor=vendor,
        model=model,
        description=description,
        manufacturer_warranty=manufacturer_warranty,
        stock_text=stock_text,
        dealer_price_text=dealer_price_text,
        rrp_price_text=rrp_price_text,
        price_text=price_text,
        url=url,
        picture_urls=[],
        raw_params=collect_raw_params(offer_el),
        offer_el=offer_el,
    )


# Скачать и распарсить XML

def fetch_source_root(url: str, *, timeout: int = DEFAULT_TIMEOUT) -> ET.Element:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    content = resp.content
    if not content:
        raise ValueError("AkCent source XML is empty")
    return ET.fromstring(content)


# Итератор по всем offer

def iter_source_offers(root: ET.Element) -> Iterable[SourceOffer]:
    for offer_el in root.findall(".//offer"):
        try:
            yield parse_offer(offer_el)
        except Exception:
            # supplier-layer не должен падать на одном кривом offer
            continue


# Удобный helper для локальных проверок

def read_source_offers(url: str, *, timeout: int = DEFAULT_TIMEOUT) -> list[SourceOffer]:
    root = fetch_source_root(url, timeout=timeout)
    return list(iter_source_offers(root))
