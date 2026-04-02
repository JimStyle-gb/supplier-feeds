# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/pictures.py

Только картинки ComPortal.
Роль:
- очистить urls;
- dedupe;
- вернуть placeholder если ничего не осталось.
"""

from __future__ import annotations

from cs.util import norm_ws


def collect_picture_urls(urls: list[str], *, placeholder_picture: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for raw in urls or []:
        u = norm_ws(raw).replace(" ", "%20")
        if not u:
            continue
        sig = u.casefold()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(u)

    if not out:
        out = [placeholder_picture]

    return out
