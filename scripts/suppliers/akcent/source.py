# -*- coding: utf-8 -*-
"""
AkCent supplier source layer.

Что делает:
- читает XML поставщика
- даёт единый SourceOffer для downstream-модулей
- ничего не "угадывает" и не чинит supplier-логику
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Iterator
from urllib.request import Request, urlopen


@dataclass(slots=True)
class SourceOffer:
    # Базовые поля исходника
    oid: str
    article: str
    type_name: str
    available: bool

    # Теги XML
    name: str
    url: str
    offer_id: str
    category_id: str
    vendor: str
    model: str
    description: str
    manufacturer_warranty: str
    stock_text: str

    # Коллекции
    pictures: list[str] = field(default_factory=list)
    xml_params: list[tuple[str, str]] = field(default_factory=list)
    prices: list[dict[str, str]] = field(default_factory=list)

    # Сырой узел для supplier-layer
    raw_offer: ET.Element | None = None


def _text(node: ET.Element | None) -> str:
    return (node.text or "").strip() if node is not None else ""


def _bool_attr(value: str | None) -> bool:
    return str(value or "").strip().lower() == "true"


def _read_bytes_from_url(url: str) -> bytes:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/xml,text/xml,*/*",
        },
    )
    with urlopen(req, timeout=120) as resp:
        return resp.read()


def _read_bytes(source: str) -> bytes:
    # Локальный файл
    if os.path.isfile(source):
        with open(source, "rb") as f:
            return f.read()

    # URL
    if source.startswith("http://") or source.startswith("https://"):
        return _read_bytes_from_url(source)

    raise FileNotFoundError(f"AkCent source not found: {source}")


def fetch_source_root(source: str) -> ET.Element:
    data = _read_bytes(source)
    root = ET.fromstring(data)

    # Нормальный кейс AkCent: yml_catalog/shop/offers/offer
    if root.find("./shop/offers") is not None:
        return root

    # Иногда могут дать уже shop
    if root.tag == "shop" and root.find("./offers") is not None:
        wrapper = ET.Element("yml_catalog")
        wrapper.append(root)
        return wrapper

    raise ValueError("AkCent XML format is not recognized")


def _extract_pictures(offer_el: ET.Element) -> list[str]:
    pics: list[str] = []
    for pic in offer_el.findall("picture"):
        val = _text(pic)
        if val and val not in pics:
            pics.append(val)
    return pics


def _extract_params(offer_el: ET.Element) -> list[tuple[str, str]]:
    params: list[tuple[str, str]] = []
    for p in offer_el.findall("Param"):
        name = (p.attrib.get("name") or "").strip()
        value = _text(p)
        if name:
            params.append((name, value))
    return params


def _extract_prices(offer_el: ET.Element) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    prices_el = offer_el.find("prices")
    if prices_el is None:
        return rows

    for p in prices_el.findall("price"):
        rows.append(
            {
                "type": (p.attrib.get("type") or "").strip(),
                "currencyId": (p.attrib.get("currencyId") or "").strip(),
                "value": _text(p),
            }
        )
    return rows


def _iter_offer_elements(root: ET.Element) -> Iterator[ET.Element]:
    shop = root.find("./shop")
    if shop is None:
        return
    offers = shop.find("./offers")
    if offers is None:
        return
    for offer_el in offers.findall("offer"):
        yield offer_el


def iter_source_offers(root: ET.Element) -> Iterator[SourceOffer]:
    for offer_el in _iter_offer_elements(root):
        yield SourceOffer(
            oid=(offer_el.attrib.get("id") or "").strip(),
            article=(offer_el.attrib.get("article") or "").strip(),
            type_name=(offer_el.attrib.get("type") or "").strip(),
            available=_bool_attr(offer_el.attrib.get("available")),
            name=_text(offer_el.find("name")),
            url=_text(offer_el.find("url")),
            offer_id=_text(offer_el.find("Offer_ID")),
            category_id=_text(offer_el.find("categoryId")),
            vendor=_text(offer_el.find("vendor")),
            model=_text(offer_el.find("model")),
            description=_text(offer_el.find("description")),
            manufacturer_warranty=_text(offer_el.find("manufacturer_warranty")),
            stock_text=_text(offer_el.find("Stock")),
            pictures=_extract_pictures(offer_el),
            xml_params=_extract_params(offer_el),
            prices=_extract_prices(offer_el),
            raw_offer=offer_el,
        )
