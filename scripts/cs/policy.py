# -*- coding: utf-8 -*-
"""
CS Policy — политики поставщиков (точечные флаги поведения core).

Этап 6: вынос из cs/core.py в отдельный модуль.
Цель: чтобы индивидуальные особенности поставщиков жили в одном месте и не ломали других.

Важно: модуль НЕ импортирует cs/core.py (без циклических импортов).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SupplierPolicy:
    code: str
    always_true_available: bool = False
    drop_desc_specs_pairs: bool = False  # AS: не переносим пары specs из native_desc в params

    # Модули "умного" обогащения. По умолчанию включены для всех, но для некоторых поставщиков
    # (например AkCent) лучше отключать и делать эти правки в адаптере, чтобы не ломать других.
    enable_enrich_from_desc: bool = True
    enable_enrich_from_name_desc: bool = True
    enable_auto_compat: bool = True
    enable_apply_color_from_name: bool = True
    enable_split_params_for_chars: bool = True  # вынос "параметры-фразы" в notes (можно отключать для AC)
    enable_clean_params: bool = True  # для AC можно отключать (params уже чистит адаптер)


def _supplier_code_from_oid(oid: str) -> str:
    oid_u = (oid or "").upper()
    return oid_u[:2] if len(oid_u) >= 2 else oid_u


_POLICIES: dict[str, SupplierPolicy] = {
    "AS": SupplierPolicy("AS", always_true_available=False, drop_desc_specs_pairs=True),
    "AC": SupplierPolicy(
        "AC",
        always_true_available=False,
        drop_desc_specs_pairs=True,
        enable_enrich_from_desc=False,
        enable_enrich_from_name_desc=False,
        enable_auto_compat=False,
        enable_apply_color_from_name=False,
        enable_split_params_for_chars=False,
        enable_clean_params=False,
    ),
    "CL": SupplierPolicy("CL", always_true_available=True, drop_desc_specs_pairs=False),
    "NP": SupplierPolicy("NP", always_true_available=True, drop_desc_specs_pairs=False),
    "VT": SupplierPolicy("VT", always_true_available=True, drop_desc_specs_pairs=False),
    "*": SupplierPolicy("*", always_true_available=False, drop_desc_specs_pairs=False),
}


def get_supplier_policy(oid: str) -> SupplierPolicy:
    return _POLICIES.get(_supplier_code_from_oid(oid), _POLICIES["*"])
