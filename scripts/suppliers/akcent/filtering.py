# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/filtering.py

AkCent supplier filtering layer.

Логика:
- общий шаблон как у AlStyle: отдельный supplier filter module;
- индивидуальная логика AkCent: фильтр по config/filter.yml;
- основной include-критерий: name_prefixes;
- доп. исключения: drop_articles / drop_rules;
- никаких supplier-specific эвристик в core.
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


def _norm_ws(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").strip().split())


def _cf(s: str) -> str:
    return _norm_ws(s).casefold()


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _text_for_rules(src: SourceOffer) -> str:
    parts = [
        src.name or "",
        src.vendor or "",
        src.model or "",
        src.type_name or "",
        src.article or "",
        src.description or "",
        " ".join(v for _, v in (src.xml_params or [])),
    ]
    return _cf(" ".join(parts))


def _get_prefixes(cfg: dict[str, Any]) -> list[str]:
    include_rules = cfg.get("include_rules") or {}

    raw = (
        include_rules.get("name_prefixes")
        or cfg.get("name_prefixes")
        or include_rules.get("allow_name_prefixes")
        or cfg.get("allow_name_prefixes")
        or []
    )

    out: list[str] = []
    seen: set[str] = set()

    for x in _as_list(raw):
        s = _norm_ws(str(x or ""))
        if not s:
            continue
        key = s.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)

    return out


def _get_mode(cfg: dict[str, Any], mode_override: str | None = None) -> str:
    raw = _norm_ws(mode_override or str(cfg.get("mode") or "include")).casefold()
    return raw if raw in {"include", "exclude"} else "include"


def _get_drop_articles(cfg: dict[str, Any]) -> set[str]:
    exclude_rules = cfg.get("exclude_rules") or {}
    raw = (
        exclude_rules.get("articles")
        or cfg.get("drop_articles")
        or []
    )

    out: set[str] = set()
    for x in _as_list(raw):
        s = _cf(str(x or ""))
        if s:
            out.add(s)
    return out


def _get_drop_rules(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    exclude_rules = cfg.get("exclude_rules") or {}
    raw = (
        exclude_rules.get("rules")
        or cfg.get("drop_rules")
        or []
    )

    out: list[dict[str, Any]] = []
    for item in _as_list(raw):
        if isinstance(item, dict):
            out.append(item)
    return out


def _name_matches_prefixes(name: str, prefixes: list[str]) -> bool:
    if not prefixes:
        return True

    n = _cf(name)
    if not n:
        return False

    for p in prefixes:
        if n.startswith(_cf(p)):
            return True
    return False


def _match_any_of(text_cf: str, tokens: list[str]) -> bool:
    for token in tokens:
        t = _cf(token)
        if t and t in text_cf:
            return True
    return False


def _match_all_of(text_cf: str, tokens: list[str]) -> bool:
    prepared = [_cf(x) for x in tokens if _cf(x)]
    if not prepared:
        return False
    return all(t in text_cf for t in prepared)


def _rule_matches(text_cf: str, rule: dict[str, Any]) -> bool:
    """
    Поддерживаемые rule-shapes:
    - {"all_groups": [{"any_of": [...]}, {"any_of": [...]}]}
    - {"any_of": [...]}
    - {"all_of": [...]}

    Это достаточно для текущего AkCent filter.yml и безопасно для расширения.
    """
    if not rule:
        return False

    if "all_groups" in rule:
        groups = _as_list(rule.get("all_groups"))
        if not groups:
            return False

        for group in groups:
            if not isinstance(group, dict):
                return False

            if "any_of" in group:
                if not _match_any_of(text_cf, [str(x) for x in _as_list(group.get("any_of"))]):
                    return False
                continue

            if "all_of" in group:
                if not _match_all_of(text_cf, [str(x) for x in _as_list(group.get("all_of"))]):
                    return False
                continue

            return False

        return True

    if "any_of" in rule:
        return _match_any_of(text_cf, [str(x) for x in _as_list(rule.get("any_of"))])

    if "all_of" in rule:
        return _match_all_of(text_cf, [str(x) for x in _as_list(rule.get("all_of"))])

    return False


def _offer_passes_include_mode(src: SourceOffer, prefixes: list[str]) -> bool:
    return _name_matches_prefixes(src.name or "", prefixes)


def _offer_passes_exclude_mode(src: SourceOffer, prefixes: list[str]) -> bool:
    if not prefixes:
        return True
    return not _name_matches_prefixes(src.name or "", prefixes)


def _reject_reason(
    src: SourceOffer,
    *,
    prefixes: list[str],
    mode: str,
    drop_articles: set[str],
    drop_rules: list[dict[str, Any]],
) -> str:
    article_cf = _cf(src.article)
    text_cf = _text_for_rules(src)

    if article_cf and article_cf in drop_articles:
        return "drop_article"

    for rule in drop_rules:
        if _rule_matches(text_cf, rule):
            return "drop_rule"

    if mode == "exclude":
        if not _offer_passes_exclude_mode(src, prefixes):
            return "prefix_excluded"
        return ""

    # default = include
    if not _offer_passes_include_mode(src, prefixes):
        return "prefix_not_allowed"

    return ""


def filter_source_offers(
    source_offers: list[SourceOffer],
    filter_cfg: dict[str, Any] | None = None,
    prefixes: list[str] | None = None,
    allowed_prefixes: list[str] | None = None,
    mode: str | None = None,
) -> tuple[list[SourceOffer], dict[str, Any]]:
    """
    Backward-safe API:
    - можно звать просто filter_source_offers(source_offers)
    - можно передавать filter_cfg
    - можно передавать prefixes / allowed_prefixes / mode из orchestrator

    Возвращает:
    - filtered offers
    - отчёт
    """
    cfg = dict(filter_cfg or _load_filter_cfg())

    resolved_prefixes = list(prefixes or allowed_prefixes or _get_prefixes(cfg))
    resolved_mode = _get_mode(cfg, mode_override=mode)
    drop_articles = _get_drop_articles(cfg)
    drop_rules = _get_drop_rules(cfg)

    kept: list[SourceOffer] = []
    rejected = Counter()

    for src in source_offers:
        reason = _reject_reason(
            src,
            prefixes=resolved_prefixes,
            mode=resolved_mode,
            drop_articles=drop_articles,
            drop_rules=drop_rules,
        )
        if reason:
            rejected[reason] += 1
            continue
        kept.append(src)

    report: dict[str, Any] = {
        "before": len(source_offers),
        "after": len(kept),
        "rejected_total": len(source_offers) - len(kept),
        "rejected_breakdown": dict(sorted(rejected.items())),
        "mode": resolved_mode,
        "prefix_count": len(resolved_prefixes),
        "drop_articles_count": len(drop_articles),
        "drop_rules_count": len(drop_rules),
    }
    return kept, report
