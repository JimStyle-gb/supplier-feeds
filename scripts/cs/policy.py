# -*- coding: utf-8 -*-
"""
Path: scripts/cs/policy.py

CS Policy — shared loader supplier-specific core rules.

Идея шаблона:
- shared-механизм живет в scripts/cs/*
- supplier-specific policy живет только в scripts/suppliers/<supplier>/config/policy.yml
- core.py читает только этот модуль и не знает VT/AS/AC/CL вручную

v17:
- loader читает supplier policy из supplier-layer config/policy.yml;
- поддерживает both nested core_rules and legacy top-level keys;
- сохраняет backward-safe defaults, если policy.yml отсутствует.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore


@dataclass(frozen=True)
class SupplierPolicy:
    code: str
    always_true_available: bool = False
    drop_desc_specs_pairs: bool = False

    # Флаги shared-core, которые могут быть выключены supplier policy.
    enable_enrich_from_desc: bool = True
    enable_enrich_from_name_desc: bool = True
    enable_auto_compat: bool = True
    enable_apply_color_from_name: bool = True
    enable_split_params_for_chars: bool = True
    enable_clean_params: bool = True


def _supplier_code_from_oid(oid: str) -> str:
    oid_u = (oid or "").upper()
    return oid_u[:2] if len(oid_u) >= 2 else oid_u


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _policy_path_by_code(code: str) -> Path:
    supplier_map = {
        "AS": "alstyle",
        "AC": "akcent",
        "CL": "copyline",
        "NP": "nvprint",
        "VT": "vtt",
    }
    folder = supplier_map.get((code or "").upper(), "")
    if not folder:
        return Path("")
    return _repo_root() / "scripts" / "suppliers" / folder / "config" / "policy.yml"


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path or not path.exists() or yaml is None:
        return {}
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _bool_from_cfg(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    s = str(value).strip().casefold()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _fallback_policy(code: str) -> SupplierPolicy:
    code = (code or "").upper()

    if code == "AS":
        return SupplierPolicy(
            "AS",
            always_true_available=False,
            drop_desc_specs_pairs=True,
            enable_enrich_from_desc=False,
            enable_enrich_from_name_desc=False,
            enable_auto_compat=False,
            enable_apply_color_from_name=False,
            enable_split_params_for_chars=False,
            enable_clean_params=False,
        )
    if code == "AC":
        return SupplierPolicy(
            "AC",
            always_true_available=False,
            drop_desc_specs_pairs=True,
            enable_enrich_from_desc=False,
            enable_enrich_from_name_desc=False,
            enable_auto_compat=False,
            enable_apply_color_from_name=False,
            enable_split_params_for_chars=False,
            enable_clean_params=False,
        )
    if code == "CL":
        return SupplierPolicy("CL", always_true_available=True, drop_desc_specs_pairs=False)
    if code == "NP":
        return SupplierPolicy("NP", always_true_available=True, drop_desc_specs_pairs=False)
    if code == "VT":
        return SupplierPolicy("VT", always_true_available=True, drop_desc_specs_pairs=False)
    return SupplierPolicy("*", always_true_available=False, drop_desc_specs_pairs=False)


def _policy_from_cfg(code: str, cfg: dict[str, Any]) -> SupplierPolicy:
    base = _fallback_policy(code)
    core_rules = cfg.get("core_rules") if isinstance(cfg.get("core_rules"), dict) else {}

    def pick(key: str, default: bool) -> bool:
        if key in core_rules:
            return _bool_from_cfg(core_rules.get(key), default)
        return _bool_from_cfg(cfg.get(key), default)

    return SupplierPolicy(
        code=code,
        always_true_available=_bool_from_cfg(cfg.get("always_true_available"), base.always_true_available),
        drop_desc_specs_pairs=pick("drop_desc_specs_pairs", base.drop_desc_specs_pairs),
        enable_enrich_from_desc=pick("enable_enrich_from_desc", base.enable_enrich_from_desc),
        enable_enrich_from_name_desc=pick("enable_enrich_from_name_desc", base.enable_enrich_from_name_desc),
        enable_auto_compat=pick("enable_auto_compat", base.enable_auto_compat),
        enable_apply_color_from_name=pick("enable_apply_color_from_name", base.enable_apply_color_from_name),
        enable_split_params_for_chars=pick("enable_split_params_for_chars", base.enable_split_params_for_chars),
        enable_clean_params=pick("enable_clean_params", base.enable_clean_params),
    )


def get_supplier_policy(oid: str) -> SupplierPolicy:
    code = _supplier_code_from_oid(oid)
    path = _policy_path_by_code(code)
    cfg = _read_yaml(path) if path else {}
    if cfg:
        return _policy_from_cfg(code, cfg)
    return _fallback_policy(code)
