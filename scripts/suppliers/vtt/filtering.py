# -*- coding: utf-8 -*-
"""VTT filtering layer — category-driven link collection."""

from __future__ import annotations

from datetime import datetime
from urllib.parse import parse_qs, urlparse
import re

from suppliers.vtt.source import VttCfg, get_bytes, soup_from_bytes, abs_url, set_q, log

_PRODUCT_HREF_RE = re.compile(r"^/catalog/[^?]+/?$")


def category_code(category_url: str) -> str:
    q = parse_qs(urlparse(category_url).query)
    return (q.get("category", [""]) or [""])[0].strip()


def collect_links_in_category(s, cfg: VttCfg, category_url: str, deadline_utc: datetime) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()

    for page in range(1, max(1, cfg.max_pages) + 1):
        if datetime.utcnow() >= deadline_utc:
            break

        page_url = set_q(category_url, "page", str(page))
        b = get_bytes(s, cfg, page_url)
        if not b:
            break

        sp = soup_from_bytes(b)
        links: list[str] = []

        for a in sp.find_all("a", href=True):
            cls = a.get("class") or []
            if isinstance(cls, list) and "btn_pic" in cls:
                continue

            href = str(a["href"]).strip()
            if not href or href.startswith("#"):
                continue

            if href.startswith("http://") or href.startswith("https://"):
                if cfg.base_url not in href:
                    continue
                href = urlparse(href).path

            if not _PRODUCT_HREF_RE.match(href):
                continue

            u = abs_url(cfg, href)
            if u in seen:
                continue
            seen.add(u)
            links.append(u)

        if not links:
            break

        found.extend(links)

    return found


def collect_all_links(s, cfg: VttCfg, deadline_utc: datetime) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for cu in cfg.categories:
        if datetime.utcnow() >= deadline_utc:
            break
        code = category_code(cu)
        links = collect_links_in_category(s, cfg, cu, deadline_utc)
        log(f"[site] category={code or '?'} links={len(links)}")
        for u in links:
            out.append((u, code))
    return out
