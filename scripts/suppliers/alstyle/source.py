# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/source.py

Только источник AlStyle: скачать XML, распарсить и собрать SourceOffer.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

import requests

from cs.util import norm_ws
from suppliers.alstyle.models import SourceOffer


def fetch_xml_text(url: str, *, timeout: int = 120, login: str | None = None, password: str | None = None) -> str:
    auth = (login, password) if (login and password) else None
    r = requests.get(url, timeout=timeout, auth=auth)
    r.raise_for_status()
    return r.text


def get_text(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def parse_xml_root(xml_text: str) -> ET.Element:
    return ET.fromstring(xml_text)


def iter_offer_elements(root: ET.Element):
    return root.findall(".//offer")


def extract_source_offer(offer_el: ET.Element) -> SourceOffer:
    return SourceOffer(
        raw_id=norm_ws(offer_el.get("id") or get_text(offer_el.find("vendorCode"))),
        category_id=norm_ws(get_text(offer_el.find("categoryId"))),
        name=norm_ws(get_text(offer_el.find("name"))),
        available_attr=(offer_el.get("available") or "").strip(),
        available_tag=norm_ws(get_text(offer_el.find("available"))),
        vendor=norm_ws(get_text(offer_el.find("vendor"))),
        description=get_text(offer_el.find("description")),
        purchase_price_text=get_text(offer_el.find("purchase_price")),
        price_text=get_text(offer_el.find("price")),
        picture_urls=[norm_ws(get_text(p)) for p in offer_el.findall("picture") if norm_ws(get_text(p))],
        offer_el=offer_el,
    )


def load_source_offers(*, url: str, timeout: int = 120, login: str | None = None, password: str | None = None) -> list[SourceOffer]:
    xml_text = fetch_xml_text(url, timeout=timeout, login=login, password=password)
    root = parse_xml_root(xml_text)
    return [extract_source_offer(el) for el in iter_offer_elements(root)]
