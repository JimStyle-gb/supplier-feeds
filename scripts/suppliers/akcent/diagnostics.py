# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/diagnostics.py

AkCent supplier layer — build diagnostics / summary.

Что делает:
- печатает стабильный build summary для orchestrator;
- красиво форматирует filter/build report без шума;
- оставляет простые watch-helpers на будущее.

Важно:
- это operational tooling, а не бизнес-логика;
- ничего не фильтрует и не меняет в offers;
- нужен для удобного разбора прогонов AkCent.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable


_SUMMARY_WIDTH = 72


# -----------------------------
# Базовые helper'ы
# -----------------------------

def _clean_text(value: Any) -> str:
    return str(value or "").strip()



def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default



def _sort_key(value: Any) -> tuple[int, str]:
    s = _clean_text(value)
    return (0, s.casefold()) if s else (1, "")



def _fmt_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)



def _fmt_inline_map(data: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for key in sorted(data.keys(), key=_sort_key):
        val = data.get(key)
        if isinstance(val, dict):
            lines.append(f"  {key}:")
            for sub_key in sorted(val.keys(), key=_sort_key):
                lines.append(f"    {sub_key}: {_fmt_scalar(val.get(sub_key))}")
            continue
        if isinstance(val, (list, tuple, set)):
            seq = [_clean_text(x) for x in val if _clean_text(x)]
            if not seq:
                lines.append(f"  {key}: []")
            else:
                lines.append(f"  {key}:")
                for item in seq:
                    lines.append(f"    - {item}")
            continue
        lines.append(f"  {key}: {_fmt_scalar(val)}")
    return lines



def _extract_top_counts(data: Any, *, top_n: int = 12) -> list[tuple[str, int]]:
    if not isinstance(data, dict):
        return []
    items: list[tuple[str, int]] = []
    for key, value in data.items():
        try:
            count = int(value)
        except Exception:
            continue
        name = _clean_text(key)
        if not name:
            continue
        items.append((name, count))
    items.sort(key=lambda x: (-x[1], x[0].casefold()))
    return items[:top_n]


# -----------------------------
# Summary print
# -----------------------------

def print_build_summary(
    *,
    supplier: str,
    version: str,
    before: int,
    after: int,
    filter_report: dict[str, Any] | None,
    build_report: dict[str, Any] | None,
    out_file: str,
    raw_out_file: str,
) -> None:
    """Печатает стабильный summary прогона AkCent."""

    supplier_name = _clean_text(supplier) or "AkCent"
    filter_report = dict(filter_report or {})
    build_report = dict(build_report or {})

    print("=" * _SUMMARY_WIDTH)
    print(f"[{supplier_name}] build summary")
    print("=" * _SUMMARY_WIDTH)
    print(f"version: {version}")
    print(f"before: {before}")
    print(f"after: {after}")
    print(f"raw_out_file: {raw_out_file}")
    print(f"out_file: {out_file}")
    print("-" * _SUMMARY_WIDTH)

    if filter_report:
        print("filter_report:")
        for line in _fmt_inline_map(filter_report):
            print(line)
        print("-" * _SUMMARY_WIDTH)

    if build_report:
        print("build_report:")
        for line in _fmt_inline_map(build_report):
            print(line)
        print("-" * _SUMMARY_WIDTH)

    top_fail = _extract_top_counts(build_report.get("fail_reasons"), top_n=10)
    if top_fail:
        print("top_fail_reasons:")
        for name, count in top_fail:
            print(f"  - {name}: {count}")
        print("-" * _SUMMARY_WIDTH)

    top_kinds = _extract_top_counts(build_report.get("kinds"), top_n=20)
    if top_kinds:
        print("kinds:")
        for name, count in top_kinds:
            print(f"  - {name}: {count}")
        print("-" * _SUMMARY_WIDTH)


# -----------------------------
# Watch helpers (на будущее)
# -----------------------------

def build_watch_source_map(
    source_offers: Iterable[Any],
    *,
    prefix: str = "AC",
    watch_articles: set[str] | None = None,
    watch_prefixes: tuple[str, ...] | list[str] | None = None,
) -> dict[str, dict[str, str]]:
    """
    Собирает мини-карту интересных source-offers:
    - по article;
    - по name-prefix.
    """
    watch_articles_cf = {_clean_text(x).casefold() for x in (watch_articles or set()) if _clean_text(x)}
    watch_prefixes_cf = tuple(_clean_text(x).casefold().replace("ё", "е") for x in (watch_prefixes or []) if _clean_text(x))
    prefix = _clean_text(prefix) or "AC"

    out: dict[str, dict[str, str]] = {}
    for src in source_offers:
        name = _clean_text(getattr(src, "name", None) if hasattr(src, "name") else (src.get("name") if isinstance(src, dict) else ""))
        article = _clean_text(
            getattr(src, "article", None) if hasattr(src, "article") else (src.get("article") if isinstance(src, dict) else "")
        )
        raw_id = _clean_text(
            getattr(src, "raw_id", None)
            if hasattr(src, "raw_id")
            else (src.get("raw_id") if isinstance(src, dict) else (src.get("id") if isinstance(src, dict) else ""))
        )
        category_id = _clean_text(
            getattr(src, "category_id", None)
            if hasattr(src, "category_id")
            else (src.get("category_id") if isinstance(src, dict) else "")
        )

        name_cf = name.casefold().replace("ё", "е")
        article_cf = article.casefold()
        matched = False
        if article_cf and article_cf in watch_articles_cf:
            matched = True
        if not matched and watch_prefixes_cf and any(name_cf.startswith(p) for p in watch_prefixes_cf):
            matched = True
        if not matched:
            continue

        oid = raw_id
        if oid and prefix and not oid.upper().startswith(prefix.upper()):
            oid = f"{prefix}{oid}"
        if not oid:
            continue
        out[oid] = {
            "article": article,
            "categoryId": category_id,
            "name": name,
        }
    return out



def make_watch_messages(
    *,
    watch_source: dict[str, dict[str, str]],
    watch_out: set[str] | None = None,
) -> list[str]:
    """Строит простые watch-сообщения по найденным / потерянным товарам."""
    watch_out = set(watch_out or set())
    messages: list[str] = []
    for oid in sorted(watch_source.keys(), key=lambda x: x.casefold()):
        info = watch_source.get(oid) or {}
        if oid in watch_out:
            messages.append(
                f"[build_akcent] WATCH_OK: {oid}; article={info.get('article', '')!r}; "
                f"categoryId={info.get('categoryId', '')!r}; name={info.get('name', '')!r}"
            )
            continue
        messages.append(
            f"[build_akcent] WATCH_MISSING: {oid}; article={info.get('article', '')!r}; "
            f"categoryId={info.get('categoryId', '')!r}; name={info.get('name', '')!r}"
        )
    return messages



def write_watch_report(path_str: str, messages: list[str]) -> None:
    """Пишет watch-report в txt, если путь задан."""
    path_str = _clean_text(path_str)
    if not path_str:
        return
    try:
        path = Path(path_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(messages) + ("\n" if messages else ""), encoding="utf-8")
    except Exception as e:
        print(f"[build_akcent] WARN: failed to write watch report {path_str!r}: {e}")
