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
- resolve_filter_inputs(...) специально совместим
  со старыми вызовами через cfg_path=... и/или filter_cfg=....
- сохранены aliases categories_from_cfg / prefixes_from_cfg
  для текущего build_vtt.py.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

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
    """Backward-safe alias для build_vtt.py."""
    return _as_list((cfg or {}).get("category_codes")) or list(DEFAULT_CATEGORY_CODES)


def prefixes_from_cfg(cfg: dict[str, Any] | None) -> list[str]:
    """Backward-safe alias для build_vtt.py."""
    return _as_list((cfg or {}).get("allowed_title_prefixes")) or list(DEFAULT_ALLOWED_TITLE_PREFIXES)


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
) -> tuple[list[str], list[str]]:
    """
    Backward-safe resolver.

    Поддерживает старые вызовы:
      resolve_filter_inputs(cfg_path=...)
      resolve_filter_inputs(filter_cfg=...)
      resolve_filter_inputs(cfg_path=..., filter_cfg=...)
    и новые env-override варианты.
    """
    cfg: dict[str, Any] = {}
    if cfg_path:
        cfg.update(load_filter_cfg(cfg_path))
    if filter_cfg:
        cfg.update(filter_cfg)

    categories = _split_env_list(category_codes_env or categories_env)
    if not categories:
        categories = categories_from_cfg(cfg)

    prefixes = _split_env_list(allowed_title_prefixes_env or prefixes_env)
    if not prefixes:
        prefixes = prefixes_from_cfg(cfg)

    return categories, prefixes


def build_listing_url(base_url: str, category_code: str, page_no: int = 1) -> str:
    base = safe_str(base_url).rstrip("/")
    cat = safe_str(category_code)
    if not base or not cat:
        return base
    if page_no <= 1:
        return f"{base}/catalog/{cat}/"
    return f"{base}/catalog/{cat}/?PAGEN_1={int(page_no)}"


__all__ = [
    "DEFAULT_CATEGORY_CODES",
    "DEFAULT_ALLOWED_TITLE_PREFIXES",
    "safe_str",
    "norm_ws",
    "load_filter_cfg",
    "categories_from_cfg",
    "prefixes_from_cfg",
    "resolve_filter_inputs",
    "title_allowed",
    "url_allowed",
    "filter_index_items",
    "build_listing_url",
]
