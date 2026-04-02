# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/filtering.py
ComPortal category-first filtering.

Роль:
- читать config/filter.yml;
- фильтровать только по category ids / excluded roots;
- вернуть filtered offers + report.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

import yaml


CONFIG_PATH = Path(__file__).resolve().parent / "config" / "filter.yml"


def safe_str(x: Any) -> str:
    """Безопасно привести к строке."""
    return str(x).strip() if x is not None else ""


def _load_filter_config(config_path: Path = CONFIG_PATH) -> Dict[str, Any]:
    """Прочитать YAML-конфиг фильтра."""
    if not config_path.exists():
        raise FileNotFoundError(f"ComPortal filter config not found: {config_path}")
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"ComPortal filter config must be a dict: {config_path}")
    return data


def load_allowed_category_ids(config_path: Path = CONFIG_PATH) -> Set[str]:
    """Вернуть set разрешённых category ids."""
    cfg = _load_filter_config(config_path)
    values = cfg.get("allowed_category_ids") or []
    return {safe_str(x) for x in values if safe_str(x)}


def load_excluded_root_ids(config_path: Path = CONFIG_PATH) -> Set[str]:
    """Вернуть set запрещённых root ids."""
    cfg = _load_filter_config(config_path)
    values = cfg.get("excluded_root_ids") or []
    return {safe_str(x) for x in values if safe_str(x)}


def _category_record(category_index: Dict[str, Dict[str, str]], cid: str) -> Dict[str, str]:
    """Вернуть запись категории или пустую заглушку."""
    return category_index.get(cid, {"id": cid, "name": "", "parent_id": "", "path": ""})


def _root_category_id(category_index: Dict[str, Dict[str, str]], cid: str) -> str:
    """Найти верхний root id для categoryId."""
    cur = safe_str(cid)
    seen: set[str] = set()
    last = cur

    while cur and cur not in seen and cur in category_index:
        seen.add(cur)
        last = cur
        parent_id = safe_str(category_index[cur].get("parent_id"))
        if not parent_id:
            return cur
        cur = parent_id

    return last


def is_allowed_offer(
    offer: Dict[str, Any],
    category_index: Dict[str, Dict[str, str]],
    *,
    allowed_category_ids: Set[str],
    excluded_root_ids: Set[str],
) -> bool:
    """Проверить, проходит ли offer фильтр."""
    cid = safe_str(offer.get("raw_categoryId") or offer.get("categoryId"))
    if not cid:
        return False

    if cid not in allowed_category_ids:
        return False

    root_id = _root_category_id(category_index, cid)
    if root_id in excluded_root_ids:
        return False

    return True


def filter_offers(
    offers: Iterable[Dict[str, Any]],
    category_index: Dict[str, Dict[str, str]],
    *,
    config_path: Path = CONFIG_PATH,
) -> Dict[str, Any]:
    """Отфильтровать offers по config/filter.yml."""
    cfg = _load_filter_config(config_path)
    allowed_category_ids = load_allowed_category_ids(config_path)
    excluded_root_ids = load_excluded_root_ids(config_path)

    source_list = list(offers)
    kept: List[Dict[str, Any]] = []
    rejected_total = 0

    kept_category_counts: Dict[str, int] = {}
    rejected_category_counts: Dict[str, int] = {}

    for offer in source_list:
        cid = safe_str(offer.get("raw_categoryId") or offer.get("categoryId"))

        if is_allowed_offer(
            offer,
            category_index,
            allowed_category_ids=allowed_category_ids,
            excluded_root_ids=excluded_root_ids,
        ):
            kept.append(offer)
            if cid:
                kept_category_counts[cid] = kept_category_counts.get(cid, 0) + 1
        else:
            rejected_total += 1
            if cid:
                rejected_category_counts[cid] = rejected_category_counts.get(cid, 0) + 1

    allowed_categories_report: List[Dict[str, Any]] = []
    for cid in sorted(allowed_category_ids):
        rec = _category_record(category_index, cid)
        allowed_categories_report.append(
            {
                "id": cid,
                "name": safe_str(rec.get("name")),
                "path": safe_str(rec.get("path")),
                "kept_count": kept_category_counts.get(cid, 0),
            }
        )

    rejected_limit = int((cfg.get("report") or {}).get("top_rejected_limit") or 20)
    rejected_categories_report: List[Dict[str, Any]] = []
    for cid, cnt in sorted(rejected_category_counts.items(), key=lambda x: (-x[1], x[0])):
        rec = _category_record(category_index, cid)
        rejected_categories_report.append(
            {
                "id": cid,
                "name": safe_str(rec.get("name")),
                "path": safe_str(rec.get("path")),
                "rejected_count": cnt,
            }
        )

    report = {
        "mode": safe_str(cfg.get("mode") or "include"),
        "before": len(source_list),
        "after": len(kept),
        "rejected_total": rejected_total,
        "allowed_category_count": len(allowed_category_ids),
        "allowed_category_ids": sorted(allowed_category_ids),
        "excluded_root_ids": sorted(excluded_root_ids),
        "allowed_categories_report": allowed_categories_report if (cfg.get("report") or {}).get("show_allowed_category_report", True) else [],
        "rejected_categories_report": rejected_categories_report[:rejected_limit] if (cfg.get("report") or {}).get("show_rejected_category_report", True) else [],
    }

    return {
        "offers": kept,
        "report": report,
    }


__all__ = [
    "CONFIG_PATH",
    "load_allowed_category_ids",
    "load_excluded_root_ids",
    "is_allowed_offer",
    "filter_offers",
]
