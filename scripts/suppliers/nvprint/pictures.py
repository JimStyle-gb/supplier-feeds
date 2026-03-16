# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/nvprint/pictures.py
NVPrint pictures layer — картинки из XML.
"""

from __future__ import annotations

from xml.etree import ElementTree as ET

from suppliers.nvprint.source import get_text, local, pick_first_text



def collect_pictures(item: ET.Element) -> list[str]:
    """Собрать картинки из XML."""
    pics: list[str] = []
    for el in item.iter():
        if local(el.tag).casefold() != "picture":
            continue
        u = get_text(el)
        if not u:
            continue
        u = u.strip()
        if u.startswith("//"):
            u = "https:" + u
        if u.startswith("/"):
            u = "https://nvprint.ru" + u
        pics.append(u)

    if not pics:
        u = pick_first_text(
            item,
            (
                "СсылкаНаКартинку",
                "СсылкаНаКартинку1",
                "СсылкаНаКартинку2",
                "СсылкаНаКартинк",
                "Картинка",
                "Фото",
                "Image",
                "Picture",
            ),
        ).strip()
        if u:
            if u.startswith("//"):
                u = "https:" + u
            if u.startswith("http://"):
                u = "https://" + u[len("http://"):]
            pics = [u]

    seen = set()
    out: list[str] = []
    for u in pics:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out
