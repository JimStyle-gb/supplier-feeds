# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/source.py

Только источник ComPortal:
- скачать YML;
- распарсить categories;
- собрать SourceOffer.

Без normalize/filter/builder-логики.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

import requests

from cs.util import norm_ws
from suppliers.comportal.models import CategoryRecord, ParamItem, SourceOffer


def _preview_text(text: str, limit: int = 240) -> str:
    s = (text or "").replace("\r", " ").replace("\n", " ").strip()
    if len(s) > limit:
        return s[:limit] + "..."
    return s


def fetch_xml_text(
    url: str,
    *,
    timeout: int = 120,
    login: str | None = None,
    password: str | None = None,
) -> str:
    """Скачать source YML и сразу отловить пустой/не-XML ответ."""
    auth = (login, password) if (login and password) else None
    r = requests.get(url, timeout=timeout, auth=auth)
    r.raise_for_status()

    text = (r.text or "").lstrip("\ufeff").strip()
    if not text:
        raise RuntimeError(
            "ComPortal source вернул пустой body. "
            "Проверь COMPORTAL_LOGIN/COMPORTAL_PASSWORD, доступность source URL "
            "и не отдал ли поставщик пустой ответ."
        )

    low = text[:400].lower()
    if "<html" in low or "<!doctype html" in low:
        raise RuntimeError(
            "ComPortal source вернул HTML вместо YML/XML. "
            "Скорее всего не прошла авторизация или поставщик отдал страницу логина/ошибки. "
            f"URL={url} | preview={_preview_text(text)}"
        )

    return text


def get_text(el: ET.Element | None) -> str:
    """Безопасно вытащить text из XML-узла."""
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def parse_xml_root(xml_text: str) -> ET.Element:
    """Распарсить XML root с понятной ошибкой."""
    text = (xml_text or "").lstrip("\ufeff").strip()
    if not text:
        raise RuntimeError("ComPortal source XML пустой после скачивания.")

    try:
        return ET.fromstring(text)
    except ET.ParseError as e:
        raise RuntimeError(
            "ComPortal source не распарсился как XML. "
            f"ParseError: {e}. Preview: {_preview_text(text)}"
        ) from e


def iter_offer_elements(root: ET.Element):
    """Итератор source offer nodes."""
    return root.findall(".//offer")


def build_category_index(root: ET.Element) -> dict[str, CategoryRecord]:
    """Построить индекс source categories."""
    out: dict[str, CategoryRecord] = {}

    for cat in root.findall(".//categories/category"):
        cid = norm_ws(cat.get("id") or "")
        if not cid:
            continue
        out[cid] = CategoryRecord(
            category_id=cid,
            name=norm_ws(get_text(cat)),
            parent_id=norm_ws(cat.get("parentId") or ""),
        )

    for cid, rec in out.items():
        chain: list[str] = []
        cur = cid
        seen: set[str] = set()
        root_id = cid

        while cur and cur not in seen and cur in out:
            seen.add(cur)
            cur_rec = out[cur]
            if cur_rec.name:
                chain.append(cur_rec.name)
            root_id = cur
            cur = cur_rec.parent_id

        chain.reverse()
        rec.path = " > ".join([x for x in chain if x])
        rec.root_id = root_id

    return out


def collect_params(offer_el: ET.Element) -> list[ParamItem]:
    """Собрать source params."""
    out: list[ParamItem] = []

    for p in offer_el.findall("param"):
        name = norm_ws(p.get("name") or "")
        value = norm_ws(get_text(p))
        if not name or not value:
            continue
        out.append(ParamItem(name=name, value=value, source="xml"))

    return out


def extract_source_offer(
    offer_el: ET.Element,
    *,
    category_index: dict[str, CategoryRecord],
) -> SourceOffer:
    """Собрать один SourceOffer."""
    category_id = norm_ws(get_text(offer_el.find("categoryId")))
    cat = category_index.get(category_id)

    return SourceOffer(
        raw_id=norm_ws(offer_el.get("id") or ""),
        vendor_code=norm_ws(get_text(offer_el.find("vendorCode"))),
        category_id=category_id,
        category_name=cat.name if cat else "",
        category_path=cat.path if cat else "",
        category_root_id=cat.root_id if cat else "",
        name=norm_ws(get_text(offer_el.find("name"))),
        available_attr=(offer_el.get("available") or "").strip(),
        available_tag=norm_ws(get_text(offer_el.find("available"))),
        vendor=norm_ws(get_text(offer_el.find("vendor"))),
        description=get_text(offer_el.find("description")),
        price_text=get_text(offer_el.find("price")),
        currency_id=norm_ws(get_text(offer_el.find("currencyId"))),
        url=norm_ws(get_text(offer_el.find("url"))),
        active=norm_ws(get_text(offer_el.find("active"))),
        delivery=norm_ws(get_text(offer_el.find("delivery"))),
        picture_urls=[
            norm_ws(get_text(p))
            for p in offer_el.findall("picture")
            if norm_ws(get_text(p))
        ],
        params=collect_params(offer_el),
        offer_el=offer_el,
    )


def load_source_bundle(
    *,
    url: str,
    timeout: int = 120,
    login: str | None = None,
    password: str | None = None,
) -> tuple[dict[str, CategoryRecord], list[SourceOffer]]:
    """Загрузить categories + offers."""
    xml_text = fetch_xml_text(url, timeout=timeout, login=login, password=password)
    root = parse_xml_root(xml_text)
    category_index = build_category_index(root)
    offers = [
        extract_source_offer(el, category_index=category_index)
        for el in iter_offer_elements(root)
    ]
    return category_index, offers
