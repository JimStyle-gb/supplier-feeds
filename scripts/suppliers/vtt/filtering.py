# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/filtering.py

VTT filtering layer.

Каноническая роль файла:
- хранить supplier filter-policy;
- читать filter.yml;
- давать source/build единый source of truth по category codes и allowed title prefixes;
- держать URL/title helper-ы для listing crawl;
- не тащить login/source/builder логику.

Важно:
- именно filtering.py, а не source.py, должен быть центром ассортиментной политики VTT;
- backward-safe helper-ы сохранены, чтобы поэтапная чистка не ломала текущий build.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore

from .normalize import norm_ws


DEFAULT_CATEGORY_CODES: list[str] = [
    "DRM_CRT",
    "DRM_UNIT",
    "CARTLAS_ORIG",
    "CARTLAS_COPY",
    "CARTLAS_PRINT",
    "CARTLAS_TNR",
    "CARTINJ_PRNTHD",
    "CARTINJ_Refill",
    "CARTINJ_ORIG",
    "CARTMAT_CART",
    "TNR_WASTETON",
    "DEV_DEV",
    "TNR_REFILL",
    "INK_COMMON",
    "PARTSPRINT_DEVUN",
]

DEFAULT_ALLOWED_TITLE_PREFIXES: list[str] = [
    "Drum",
    "Девелопер",
    "Драм-картридж",
    "Драм-юнит",
    "Драм-юниты",
    "Драм юнит",
    "Кабель сетевой",
    "Картридж",
    "Картриджи",
    "Термоблок",
    "Тонер-картридж",
    "Тонер-катридж",
    "Чернила",
    "Печатающая головка",
    "Копи-картридж",
    "Принт-картридж",
    "Контейнер",
    "Блок",
    "Бункер",
    "Носитель",
    "Фотобарабан",
    "Барабан",
    "Тонер",
    "Комплект",
    "Набор",
    "Заправочный комплект",
    "Модуль фоторецептора",
    "Фотопроводниковый блок",
    "Бокс сбора тонера",
    "Рефил",
]

TITLE_LEAD_CODE_RE = re.compile(
    r"""^(?:[A-Z0-9][A-Z0-9\-./]{2,}(?:\s*,\s*[A-Z0-9][A-Z0-9\-./]{2,})*\s+)+""",
    re.I,
)
ORIGINAL_MARK_RE = re.compile(
    r"""(?<!\w)\((?:O|О|OEM)\)(?!\w)|\bоригинал(?:ьн(?:ый|ая|ое|ые))?\b""",
    re.I,
)
LEAD_MARK_RE = re.compile(r"""^(?:\((?:E|LE)\)|LE\b|E\b)\s*""", re.I)


def _clean_list(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        s = norm_ws(item)
        if not s:
            continue
        sig = s.casefold()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(s)
    return out


def parse_id_list(raw: str | None, fallback: list[str]) -> list[str]:
    """Прочитать список кодов из env или вернуть fallback."""
    if not raw:
        return list(fallback)
    parts = re.split(r"[\s,;]+", raw.strip())
    out = _clean_list(parts)
    return out or list(fallback)


def product_path_re(path: str) -> bool:
    return bool(re.match(r"^/catalog/[^/?#]+/?$", path or "", re.I))


def normalize_listing_url(url: str) -> str:
    p = urlparse(url)
    qs = parse_qs(p.query)
    items: list[tuple[str, str]] = []
    for key in sorted(qs):
        for value in sorted(qs[key]):
            items.append((key, value))
    return urlunparse((p.scheme, p.netloc, p.path, "", urlencode(items, doseq=True), ""))


def mk_category_url(base_url: str, code: str) -> str:
    return urljoin(base_url, f"/catalog/?category={code}")


def normalize_listing_title(title: str) -> str:
    title = norm_ws(title)
    title = ORIGINAL_MARK_RE.sub("", title)
    title = TITLE_LEAD_CODE_RE.sub("", title)
    while True:
        new_title = LEAD_MARK_RE.sub("", title).strip(" ,.-")
        if new_title == title:
            break
        title = new_title
    return norm_ws(title).strip(" ,.-")


def title_matches_allowed(title: str, prefixes: list[str]) -> bool:
    if not prefixes:
        return True
    if not title:
        return True
    low = title.casefold()
    compact = low.replace("-", " ")
    for prefix in prefixes:
        p = prefix.casefold()
        pp = p.replace("-", " ")
        if low.startswith(p) or compact.startswith(pp):
            return True
    return False


def load_filter_config(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if yaml is None or not p.exists():
        return {}
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def categories_from_cfg(cfg: dict[str, Any]) -> list[str]:
    vals = cfg.get("category_codes")
    if isinstance(vals, list):
        out = _clean_list(vals)
        if out:
            return out
    return list(DEFAULT_CATEGORY_CODES)


def prefixes_from_cfg(cfg: dict[str, Any]) -> list[str]:
    vals = cfg.get("allowed_title_prefixes")
    if isinstance(vals, list):
        out = _clean_list(vals)
        if out:
            return out

    # backward-safe support for older names
    vals = cfg.get("include_prefixes")
    if isinstance(vals, list):
        out = _clean_list(vals)
        if out:
            return out

    return list(DEFAULT_ALLOWED_TITLE_PREFIXES)


def resolve_filter_inputs(
    *,
    filter_cfg: dict[str, Any] | None = None,
    category_codes_env: str | None = None,
    allowed_prefixes_env: str | None = None,
) -> tuple[list[str], list[str]]:
    """
    Единый helper для source/build:
    - category codes идут из filter.yml, env только override;
    - allowed title prefixes идут из filter.yml, env только override.
    """
    cfg = filter_cfg or {}
    categories = categories_from_cfg(cfg)
    prefixes = prefixes_from_cfg(cfg)
    categories = parse_id_list(category_codes_env, categories)
    prefixes = parse_id_list(allowed_prefixes_env, prefixes)
    return categories, prefixes


__all__ = [
    "DEFAULT_ALLOWED_TITLE_PREFIXES",
    "DEFAULT_CATEGORY_CODES",
    "categories_from_cfg",
    "load_filter_config",
    "mk_category_url",
    "normalize_listing_title",
    "normalize_listing_url",
    "parse_id_list",
    "prefixes_from_cfg",
    "product_path_re",
    "resolve_filter_inputs",
    "title_matches_allowed",
]
