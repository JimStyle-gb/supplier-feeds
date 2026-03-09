# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/diagnostics.py

Диагностика и watch-report для AlStyle.
"""

from __future__ import annotations

from pathlib import Path

from suppliers.alstyle.models import SourceOffer


def build_watch_source_map(source_offers: list[SourceOffer], *, prefix: str, watch_ids: set[str]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for src in source_offers:
        oid = src.raw_id if src.raw_id.upper().startswith(prefix.upper()) else f"{prefix}{src.raw_id}" if src.raw_id else ""
        if oid in watch_ids:
            out[oid] = {"categoryId": src.category_id, "name": src.name}
    return out


def make_watch_messages(*, watch_ids: set[str], watch_source: dict[str, dict[str, str]], watch_out: set[str], allowed: set[str]) -> list[str]:
    messages: list[str] = []
    for wid in sorted(watch_ids):
        if wid not in watch_source:
            messages.append(f"[build_alstyle] ROOT_CAUSE: watched offer not present in supplier XML: {wid}")
            continue
        if wid not in watch_out:
            info = watch_source[wid]
            cat = info.get("categoryId", "")
            reason = "filtered_by_category" if (allowed and (not cat or cat not in allowed)) else "skipped_after_parse"
            messages.append(
                f"[build_alstyle] ROOT_CAUSE: watched offer missing in output: {wid}; reason={reason}; "
                f"categoryId={cat!r}; name={info.get('name', '')!r}"
            )
            continue
        info = watch_source[wid]
        messages.append(
            f"[build_alstyle] WATCH_OK: {wid}; categoryId={info.get('categoryId', '')!r}; name={info.get('name', '')!r}"
        )
    return messages


def write_watch_report(path_str: str, messages: list[str]) -> None:
    if not path_str:
        return
    try:
        path = Path(path_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(messages) + ("\n" if messages else ""), encoding="utf-8")
    except Exception as e:
        print(f"[build_alstyle] WARN: failed to write watch report {path_str!r}: {e}")
