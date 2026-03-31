# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/copyline/filtering.py
CopyLine filtering layer.

Фильтрация ассортимента по префиксам названия.
Модуль отвечает только за include-filter и отчёт по фильтрации.
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import Dict, Iterable, List, Sequence, Tuple

import yaml

DEFAULT_INCLUDE_PREFIXES: list[str] = [
    "Drum",
    "Девелопер",
    "Драм-картридж",
    "Драм-юниты",
    "Кабель сетевой",
    "Картридж",
    "Картриджи",
    "Термоблок",
    "Тонер-картридж",
    "Чернила",
]


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def compile_startswith_patterns(prefixes: Sequence[str]) -> list[re.Pattern[str]]:
    """Скомпилировать строгие regex по префиксам названия."""
    out: list[re.Pattern[str]] = []
    for raw in prefixes:
        val = safe_str(raw)
        if not val:
            continue
        out.append(re.compile(r"^\s*" + re.escape(val).replace(r"\ ", " ") + r"(?!\w)", re.I))
    return out


def title_allowed(title: str, patterns: Sequence[re.Pattern[str]]) -> bool:
    """Разрешён ли title по фильтру префиксов."""
    title = safe_str(title)
    return bool(title) and any(p.search(title) for p in patterns)


def load_filter_config(path: str | None = None) -> dict:
    """Прочитать filter.yml; если файла нет — взять defaults."""
    if not path:
        return {"mode": "include", "include_prefixes": list(DEFAULT_INCLUDE_PREFIXES)}

    p = Path(path)
    if not p.exists():
        return {"mode": "include", "include_prefixes": list(DEFAULT_INCLUDE_PREFIXES)}

    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        data = {}

    prefixes = data.get("include_prefixes") or data.get("prefixes") or DEFAULT_INCLUDE_PREFIXES
    return {
        "mode": safe_str(data.get("mode") or "include").lower() or "include",
        "include_prefixes": [safe_str(x) for x in prefixes if safe_str(x)],
    }


def filter_product_index(
    products: Iterable[dict],
    *,
    include_prefixes: Sequence[str] | None = None,
) -> Tuple[List[dict], Dict[str, object]]:
    """Отфильтровать индекс товаров по title-prefix."""
    prefixes = list(include_prefixes or DEFAULT_INCLUDE_PREFIXES)
    patterns = compile_startswith_patterns(prefixes)

    before = 0
    kept: list[dict] = []
    rejected_total = 0
    kept_by_prefix: dict[str, int] = {p: 0 for p in prefixes}

    for product in products:
        before += 1
        title = safe_str(product.get("title"))
        if not title_allowed(title, patterns):
            rejected_total += 1
            continue
        kept.append(product)
        low = title.lower()
        for p in prefixes:
            pl = p.lower()
            if low == pl or low.startswith(pl + " ") or low.startswith(pl + "-"):
                kept_by_prefix[p] = kept_by_prefix.get(p, 0) + 1
                break

    report: Dict[str, object] = {
        "mode": "include",
        "before": before,
        "after": len(kept),
        "rejected_total": rejected_total,
        "allowed_prefix_count": len(prefixes),
        "allowed_prefixes": prefixes,
        "kept_by_prefix": {k: v for k, v in kept_by_prefix.items() if v > 0},
        "reject_reasons": {"name_prefix_not_allowed": rejected_total},
    }
    return kept, report
