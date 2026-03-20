# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/filtering.py
VTT filtering layer.

Задача слоя:
- держать whitelist ассортимента вне build_vtt.py;
- отфильтровывать индекс товаров ДО парсинга карточек;
- формировать нормальный filter_report для RAW.

Для VTT основной фильтр сейчас двухуровневый:
1) category whitelist — это главный и обязательный слой;
2) title-prefix filter — мягкий/дополнительный, включается только если в index есть title.
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import Dict, Iterable, List, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

import yaml

DEFAULT_ALLOWED_CATEGORY_CODES: list[str] = [
    "CARTINJ_COMPAT",
    "CARTINJ_ORIG",
    "CARTINJ_PRNTHD",
    "CARTLAS_COMPAT",
    "CARTLAS_COPY",
    "CARTLAS_ORIG",
    "CARTLAS_PRINT",
    "CARTLAS_TNR",
    "CARTMAT_CART",
    "DEV_DEV",
    "DRM_CRT",
    "DRM_UNIT",
    "PARTSPRINT_THERBLC",
    "PARTSPRINT_THERELT",
]

# Мягкий title-filter для index-этапа. Не должен резать по живому.
DEFAULT_INCLUDE_TITLE_PREFIXES: list[str] = [
    "Картридж",
    "Тонер-картридж",
    "Тонер-катридж",
    "Принт-картридж",
    "Копи-картридж",
    "Драм-юнит",
    "Драм-картридж",
    "Фотобарабан",
    "Барабан",
    "Девелопер",
    "Термоблок",
    "Термоэлемент",
    "Нагревательный",
    "Блок",
    "Контейнер",
    "Комплект",
    "Набор",
    "Носитель",
    "Печатающая",
    "Тонер",
]

_PRODUCT_PATH_RE = re.compile(r"^/catalog/[^?#]+/?$", re.I)


def safe_str(x: object) -> str:
    return str(x).strip() if x is not None else ""


def category_code_from_url(url: str) -> str:
    """Вытащить ?category=... из category url."""
    q = parse_qs(urlparse(safe_str(url)).query)
    return safe_str((q.get("category") or [""])[0])


def compile_startswith_patterns(prefixes: Sequence[str]) -> list[re.Pattern[str]]:
    """Скомпилировать строгие regex по префиксам title."""
    out: list[re.Pattern[str]] = []
    for raw in prefixes:
        val = safe_str(raw)
        if not val:
            continue
        out.append(re.compile(r"^\s*" + re.escape(val).replace(r"\ ", " ") + r"(?!\w)", re.I))
    return out


def title_allowed(title: str, patterns: Sequence[re.Pattern[str]]) -> bool:
    """Разрешён ли title по prefix-filter."""
    title = safe_str(title)
    return bool(title) and any(p.search(title) for p in patterns)


def load_filter_config(path: str | None = None) -> dict:
    """Прочитать filter.yml; если файла нет — взять безопасные defaults."""
    defaults = {
        "mode": "include",
        "allowed_category_codes": list(DEFAULT_ALLOWED_CATEGORY_CODES),
        "include_title_prefixes": list(DEFAULT_INCLUDE_TITLE_PREFIXES),
        "enforce_title_prefixes": False,
        "require_catalog_url": True,
    }
    if not path:
        return defaults

    p = Path(path)
    if not p.exists():
        return defaults

    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return defaults

    category_codes = data.get("allowed_category_codes") or data.get("category_codes") or defaults["allowed_category_codes"]
    prefixes = data.get("include_title_prefixes") or data.get("title_prefixes") or defaults["include_title_prefixes"]

    return {
        "mode": safe_str(data.get("mode") or defaults["mode"]).lower() or defaults["mode"],
        "allowed_category_codes": [safe_str(x) for x in category_codes if safe_str(x)],
        "include_title_prefixes": [safe_str(x) for x in prefixes if safe_str(x)],
        "enforce_title_prefixes": bool(data.get("enforce_title_prefixes", defaults["enforce_title_prefixes"])),
        "require_catalog_url": bool(data.get("require_catalog_url", defaults["require_catalog_url"])),
    }


def filter_product_index(
    products: Iterable[dict],
    *,
    allowed_category_codes: Sequence[str] | None = None,
    include_title_prefixes: Sequence[str] | None = None,
    enforce_title_prefixes: bool = False,
    require_catalog_url: bool = True,
) -> Tuple[List[dict], Dict[str, object]]:
    """Отфильтровать index VTT до парсинга карточек.

    Ожидаемый item:
    {
        "url": "https://.../catalog/...",
        "cat_code": "CARTLAS_ORIG",
        "title": "..."   # optional
    }
    """
    allowed_codes = [safe_str(x) for x in (allowed_category_codes or DEFAULT_ALLOWED_CATEGORY_CODES) if safe_str(x)]
    code_set = {x.upper() for x in allowed_codes}
    prefixes = [safe_str(x) for x in (include_title_prefixes or DEFAULT_INCLUDE_TITLE_PREFIXES) if safe_str(x)]
    patterns = compile_startswith_patterns(prefixes)

    before = 0
    kept: list[dict] = []
    rejected_total = 0
    reject_reasons: dict[str, int] = {
        "missing_url": 0,
        "non_catalog_url": 0,
        "missing_category_code": 0,
        "category_not_allowed": 0,
        "title_prefix_not_allowed": 0,
    }
    kept_by_category: dict[str, int] = {}

    for product in products:
        before += 1
        url = safe_str(product.get("url"))
        title = safe_str(product.get("title"))
        cat_code = safe_str(product.get("cat_code") or product.get("category_code"))

        if not url:
            rejected_total += 1
            reject_reasons["missing_url"] += 1
            continue

        parsed = urlparse(url)
        path = parsed.path or ""
        if require_catalog_url and not _PRODUCT_PATH_RE.match(path):
            rejected_total += 1
            reject_reasons["non_catalog_url"] += 1
            continue

        if not cat_code:
            rejected_total += 1
            reject_reasons["missing_category_code"] += 1
            continue

        if code_set and cat_code.upper() not in code_set:
            rejected_total += 1
            reject_reasons["category_not_allowed"] += 1
            continue

        # VTT index пока может не нести title. Тогда не режем товар по prefix-filter.
        if enforce_title_prefixes and title and patterns and not title_allowed(title, patterns):
            rejected_total += 1
            reject_reasons["title_prefix_not_allowed"] += 1
            continue

        kept.append(product)
        kept_by_category[cat_code] = kept_by_category.get(cat_code, 0) + 1

    report: Dict[str, object] = {
        "mode": "include",
        "before": before,
        "after": len(kept),
        "rejected_total": rejected_total,
        "allowed_category_count": len(allowed_codes),
        "allowed_category_codes": allowed_codes,
        "title_prefix_filter_enabled": bool(enforce_title_prefixes),
        "allowed_title_prefix_count": len(prefixes),
        "allowed_title_prefixes": prefixes,
        "kept_by_category": kept_by_category,
        "reject_reasons": {k: v for k, v in reject_reasons.items() if v > 0},
    }
    return kept, report
