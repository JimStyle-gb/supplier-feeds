# -*- coding: utf-8 -*-
"""
AkCent supplier filtering layer.

Что делает:
- отбирает ассортимент по config/filter.yml
- не меняет данные товара
- возвращает filtered offers + отчёт
"""

from __future__ import annotations

import os
from collections import Counter
from typing import Any

import yaml

from suppliers.akcent.source import SourceOffer


def _config_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "config")


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        obj = yaml.safe_load(f) or {}
    if not isinstance(obj, dict):
        raise ValueError(f"Bad YAML root in {path}: expected mapping")
    return obj


def _load_filter_cfg() -> dict[str, Any]:
    return _load_yaml(os.path.join(_config_dir(), "filter.yml"))


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _to_set(values: Any) -> set[str]:
    if not isinstance(values, list):
        return set()
    out: set[str] = set()
    for v in values:
        vv = _norm(str(v or ""))
        if vv:
            out.add(vv)
    return out


def _contains_any(text: str, needles: set[str]) -> bool:
    hay = _norm(text)
    if not hay or not needles:
        return False
    return any(n in hay for n in needles)


def _eq_any(text: str, variants: set[str]) -> bool:
    return _norm(text) in variants if variants else False


def _category_allowed(offer: SourceOffer, cfg: dict[str, Any]) -> bool:
    category = _norm(offer.category_id)
    include_categories = _to_set(cfg.get("include_category_ids"))
    exclude_categories = _to_set(cfg.get("exclude_category_ids"))

    if include_categories and category not in include_categories:
        return False
    if exclude_categories and category in exclude_categories:
        return False
    return True


def _vendor_allowed(offer: SourceOffer, cfg: dict[str, Any]) -> bool:
    vendor = _norm(offer.vendor)
    name = _norm(offer.name)
    model = _norm(offer.model)
    text = f"{vendor} {name} {model}"

    include_vendors = _to_set(cfg.get("include_vendors"))
    exclude_vendors = _to_set(cfg.get("exclude_vendors"))
    exclude_vendor_keywords = _to_set(cfg.get("exclude_vendor_keywords"))

    if include_vendors and not (_eq_any(vendor, include_vendors) or _contains_any(text, include_vendors)):
        return False
    if exclude_vendors and (_eq_any(vendor, exclude_vendors) or _contains_any(text, exclude_vendors)):
        return False
    if exclude_vendor_keywords and _contains_any(text, exclude_vendor_keywords):
        return False
    return True


def _text_allowed(offer: SourceOffer, cfg: dict[str, Any]) -> bool:
    text = " ".join(
        [
            offer.name or "",
            offer.vendor or "",
            offer.model or "",
            offer.description or "",
            " ".join(v for _, v in offer.xml_params),
        ]
    )

    include_keywords = _to_set(cfg.get("include_keywords"))
    exclude_keywords = _to_set(cfg.get("exclude_keywords"))

    if include_keywords and not _contains_any(text, include_keywords):
        return False
    if exclude_keywords and _contains_any(text, exclude_keywords):
        return False
    return True


def _basic_allowed(offer: SourceOffer, cfg: dict[str, Any]) -> bool:
    if not (offer.oid or "").strip():
        return False

    require_name = bool(cfg.get("require_name", True))
    if require_name and not (offer.name or "").strip():
        return False

    require_price = bool(cfg.get("require_price", False))
    if require_price and not offer.prices:
        return False

    return True


def _reject_reason(offer: SourceOffer, cfg: dict[str, Any]) -> str:
    if not _basic_allowed(offer, cfg):
        return "basic_reject"
    if not _category_allowed(offer, cfg):
        return "category_reject"
    if not _vendor_allowed(offer, cfg):
        return "vendor_reject"
    if not _text_allowed(offer, cfg):
        return "keyword_reject"
    return ""


def filter_source_offers(source_offers: list[SourceOffer]) -> tuple[list[SourceOffer], dict[str, Any]]:
    cfg = _load_filter_cfg()

    kept: list[SourceOffer] = []
    rejected = Counter()

    for offer in source_offers:
        reason = _reject_reason(offer, cfg)
        if reason:
            rejected[reason] += 1
            continue
        kept.append(offer)

    report: dict[str, Any] = {
        "before": len(source_offers),
        "after": len(kept),
        "rejected_total": len(source_offers) - len(kept),
        "rejected_breakdown": dict(sorted(rejected.items())),
    }
    return kept, report
