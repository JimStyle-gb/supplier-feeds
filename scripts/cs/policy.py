# -*- coding: utf-8 -*-
"""
Path: scripts/cs/policy.py

CS Policy — shared common defaults only.

Идея шаблона:
- core делает только общие одинаковые действия для всех поставщиков;
- supplier-specific policy не живет в core и не определяется по OID/коду поставщика;
- если какому-то поставщику нужны особые правила, это обязанность RAW / supplier-layer.

v18:
- убрана supplier-aware логика из shared policy;
- сохранен backward-safe API (SupplierPolicy / get_supplier_policy / _supplier_code_from_oid),
  чтобы не ломать существующие импорты при поэтапной чистке core.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SupplierPolicy:
    code: str = "*"
    always_true_available: bool = False
    drop_desc_specs_pairs: bool = False

    # Все supplier-repair флаги в shared core по умолчанию выключены.
    enable_enrich_from_desc: bool = False
    enable_enrich_from_name_desc: bool = False
    enable_auto_compat: bool = False
    enable_apply_color_from_name: bool = False
    enable_split_params_for_chars: bool = False
    enable_clean_params: bool = False


def _supplier_code_from_oid(oid: str) -> str:
    oid_u = (oid or "").upper()
    return oid_u[:2] if len(oid_u) >= 2 else oid_u


def get_supplier_policy(oid: str) -> SupplierPolicy:
    # Shared core больше не определяет поведение по поставщику.
    # Возвращаем единый общий policy-объект только для backward compatibility.
    return SupplierPolicy(code="*")
