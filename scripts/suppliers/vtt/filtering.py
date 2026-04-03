# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/filtering.py

VTT filtering layer under CS-template.

Роль файла:
- держать только ассортиментную политику VTT;
- category scope + allowed title prefixes;
- helper-ы для listing/url фильтра;
- backward-safe API для текущих source.py / build_vtt.py.

Важно:
- source.py не должен дублировать ассортиментные defaults;
- filter.yml остаётся source of truth;
- сохранены старые функции:
  mk_category_url, normalize_listing_url, product_path_re,
  normalize_listing_title, title_matches_allowed,
  categories_from_cfg, prefixes_from_cfg, resolve_filter_inputs.
- resolve_filter_inputs(...) поддерживает все старые имена kwargs:
  cfg_path / filter_cfg / categories_env / prefixes_env /
  category_codes_env / allowed_title_prefixes_env /
  env_category_codes / env_allowed_title_prefixes /
  env_categories / env_prefixes.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore


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
_MULTI_WS_RE = re.compile(r"\s+")


def norm_ws(text: str) -> str:
    return " ".join(str(text or "").replace("\xa0", " ").split()).strip()


def safe_str(value: object) -> str:
    return str(value).strip() if value is not None else ""


def _norm_title_prefix(text: str) -> str:
    s = safe_str(text)
    if not s:
        return ""
    s = s.replace("Ё", "Е").replace("ё", "е")
    s = _MULTI_WS_RE.sub(" ", s)
    return s.strip()


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


def build_listing_url(base_url: str, category_code: str, page_no: int = 1) -> str:
    base = safe_str(base_url).rstrip("/")
    cat = safe_str(category_code)
    if not base or not cat:
        return base
    if page_no <= 1:
        return mk_category_url(base, cat)
    return f"{mk_category_url(base, cat)}&PAGEN_1={int(page_no)}"


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


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    if yaml is None:
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def load_filter_cfg(cfg_path: str | Path | None) -> dict[str, Any]:
    if not cfg_path:
        return {}
    return _read_yaml(Path(cfg_path))


def load_filter_config(path: str | Path) -> dict[str, Any]:
    return load_filter_cfg(path)


def _as_list(raw: Any) -> list[str]:
    out: list[str] = []
    for item in raw or []:
        s = safe_str(item)
        if s:
            out.append(s)
    return out


def _split_env_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    parts = re.split(r"[\s,;|]+", str(raw).strip())
    return [p.strip() for p in parts if p and p.strip()]


def categories_from_cfg(cfg: dict[str, Any] | None) -> list[str]:
    vals = (cfg or {}).get("category_codes")
    if isinstance(vals, list):
        out = [norm_ws(x) for x in vals if norm_ws(x)]
        if out:
            return out
    return list(DEFAULT_CATEGORY_CODES)


def prefixes_from_cfg(cfg: dict[str, Any] | None) -> list[str]:
    vals = (cfg or {}).get("allowed_title_prefixes")
    if isinstance(vals, list):
        out = [norm_ws(x) for x in vals if norm_ws(x)]
        if out:
            return out
    vals = (cfg or {}).get("include_prefixes")
    if isinstance(vals, list):
        out = [norm_ws(x) for x in vals if norm_ws(x)]
        if out:
            return out
    return list(DEFAULT_ALLOWED_TITLE_PREFIXES)


def title_allowed(title: str, allowed_prefixes: list[str] | tuple[str, ...] | set[str] | None) -> bool:
    prefixes = [safe_str(x) for x in (allowed_prefixes or []) if safe_str(x)]
    if not prefixes:
        return True

    t = _norm_title_prefix(title)
    if not t:
        return False
    t_cf = t.casefold()

    for prefix in prefixes:
        p = _norm_title_prefix(prefix)
        if not p:
            continue
        if t_cf.startswith(p.casefold()):
            return True
    return False


def title_matches_allowed(title: str, prefixes: list[str]) -> bool:
    return title_allowed(title, prefixes)


def url_allowed(url: str, category_codes: list[str] | tuple[str, ...] | set[str] | None) -> bool:
    codes = [safe_str(x) for x in (category_codes or []) if safe_str(x)]
    if not codes:
        return True

    low = safe_str(url).lower()
    if not low:
        return False

    for code in codes:
        if code.lower() in low:
            return True
    return False


def filter_index_items(
    items: list[dict[str, Any]],
    *,
    category_codes: list[str] | None = None,
    allowed_title_prefixes: list[str] | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in items or []:
        title = safe_str(item.get("title"))
        url = safe_str(item.get("url"))
        if category_codes and not url_allowed(url, category_codes):
            continue
        if allowed_title_prefixes and not title_allowed(title, allowed_title_prefixes):
            continue
        out.append(item)
    return out


def resolve_filter_inputs(
    *,
    cfg_path: str | Path | None = None,
    filter_cfg: dict[str, Any] | None = None,
    categories_env: str | None = None,
    prefixes_env: str | None = None,
    category_codes_env: str | None = None,
    allowed_title_prefixes_env: str | None = None,
    env_category_codes: str | None = None,
    env_allowed_title_prefixes: str | None = None,
    env_categories: str | None = None,
    env_prefixes: str | None = None,
) -> tuple[list[str], list[str]]:
    cfg: dict[str, Any] = {}
    if cfg_path:
        cfg.update(load_filter_cfg(cfg_path))
    if filter_cfg:
        cfg.update(filter_cfg)

    categories = _split_env_list(
        category_codes_env or categories_env or env_category_codes or env_categories
    )
    if not categories:
        categories = categories_from_cfg(cfg)

    prefixes = _split_env_list(
        allowed_title_prefixes_env or prefixes_env or env_allowed_title_prefixes or env_prefixes
    )
    if not prefixes:
        prefixes = prefixes_from_cfg(cfg)

    return categories, prefixes


__all__ = [
    "DEFAULT_CATEGORY_CODES",
    "DEFAULT_ALLOWED_TITLE_PREFIXES",
    "safe_str",
    "norm_ws",
    "product_path_re",
    "normalize_listing_url",
    "mk_category_url",
    "build_listing_url",
    "normalize_listing_title",
    "load_filter_cfg",
    "load_filter_config",
    "categories_from_cfg",
    "prefixes_from_cfg",
    "resolve_filter_inputs",
    "title_allowed",
    "title_matches_allowed",
    "url_allowed",
    "filter_index_items",
]
