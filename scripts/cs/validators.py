# -*- coding: utf-8 -*-
"""
Path: scripts/cs/validators.py

CS Validators — проверки готового CS-фида.

Что изменено в этой версии:
- убраны локальные дубли norm_ws/safe_int;
- используем единые shared-хелперы из cs.util;
- убран жёсткий хардкод старого списка поставщиков (AC/AS/CL/NP);
- hash-like id теперь определяется по общей форме, без привязки к конкретному поставщику;
- структура файла выровнена и снабжена короткими русскими комментариями.
"""

from __future__ import annotations

import re

from .util import norm_ws, safe_int


# Общие regex валидатора
_RE_HASH_LIKE_OID = re.compile(r"^[A-Z]{2}H[0-9A-F]{10}$")


def validate_cs_yml(xml: str, *, param_drop_default_cf: set[str]) -> None:
    """Проверить уже собранный CS-фид и бросить ValueError при ошибках."""
    errors: list[str] = []

    # -----------------------------
    # Глобальные запреты
    # -----------------------------
    if "<available>" in xml:
        errors.append('Найден тег <available> (должен быть только available="true/false" в <offer>).')

    if re.search(r"\bShuko\b", xml, flags=re.I):
        errors.append("Найдено слово 'Shuko' (нужно 'Schuko').")

    drop_names = {norm_ws(x).casefold() for x in (param_drop_default_cf or set()) if norm_ws(x)}

    # -----------------------------
    # Состояние текущего offer
    # -----------------------------
    in_offer = False
    offer_id = ""
    has_picture = False
    vendor_code = ""
    keywords = ""
    price_ok = True

    ids_seen: set[str] = set()
    dup_ids: list[str] = []
    hash_like_ids: list[str] = []
    bad_no_pic: list[str] = []
    bad_vendorcode: list[str] = []
    bad_keywords: list[str] = []
    bad_params: list[str] = []
    bad_price: list[str] = []

    # -----------------------------
    # Построчный разбор готового XML
    # -----------------------------
    for line in xml.splitlines():
        s = line.strip()

        if s.startswith("<offer ") and 'id="' in s:
            in_offer = True
            has_picture = False
            vendor_code = ""
            keywords = ""
            price_ok = True

            m = re.search(r'id="([^"]+)"', s)
            offer_id = m.group(1) if m else ""
            if offer_id:
                if offer_id in ids_seen:
                    dup_ids.append(offer_id)
                ids_seen.add(offer_id)
                # Это не критическая ошибка, а сигнал о подозрительно hash-похожем id.
                if _RE_HASH_LIKE_OID.fullmatch(offer_id):
                    hash_like_ids.append(offer_id)
            continue

        if not in_offer:
            continue

        if s.startswith("<picture>") and s.endswith("</picture>"):
            has_picture = True
            continue

        if s.startswith("<vendorCode>") and s.endswith("</vendorCode>"):
            vendor_code = re.sub(r"^<vendorCode>|</vendorCode>$", "", s).strip()
            continue

        if s.startswith("<keywords>") and s.endswith("</keywords>"):
            keywords = re.sub(r"^<keywords>|</keywords>$", "", s).strip()
            continue

        if s.startswith("<price>") and s.endswith("</price>"):
            price_val = re.sub(r"^<price>|</price>$", "", s).strip()
            price_num = safe_int(price_val)
            price_ok = price_num is not None and price_num >= 100
            continue

        if s.startswith("<param ") and 'name="' in s:
            m_name = re.search(r'name="([^"]+)"', s)
            param_name = norm_ws(m_name.group(1) if m_name else "")
            if param_name and param_name.casefold() in drop_names:
                bad_params.append(f"{offer_id}: запрещённый param '{param_name}'")
            continue

        if s == "</offer>":
            if offer_id:
                if not has_picture:
                    bad_no_pic.append(offer_id)
                if not vendor_code or vendor_code != offer_id:
                    bad_vendorcode.append(offer_id)
                if not keywords:
                    bad_keywords.append(offer_id)
                if not price_ok:
                    bad_price.append(offer_id)

            # Сбрасываем состояние offer
            in_offer = False
            offer_id = ""
            has_picture = False
            vendor_code = ""
            keywords = ""
            price_ok = True
            continue

    # -----------------------------
    # Сборка финальных ошибок
    # -----------------------------
    if dup_ids:
        errors.append("Дублирующиеся offer id: " + ", ".join(dup_ids[:20]))
    if hash_like_ids:
        errors.append("Подозрительные hash-like offer id: " + ", ".join(hash_like_ids[:20]))
    if bad_no_pic:
        errors.append("Офферы без picture: " + ", ".join(bad_no_pic[:20]))
    if bad_vendorcode:
        errors.append("vendorCode отсутствует или не совпадает с offer id: " + ", ".join(bad_vendorcode[:20]))
    if bad_keywords:
        errors.append("Офферы без keywords: " + ", ".join(bad_keywords[:20]))
    if bad_params:
        errors.append("В финал просочились запрещённые params: " + "; ".join(bad_params[:20]))
    if bad_price:
        errors.append("Офферы с невалидной price: " + ", ".join(bad_price[:20]))

    if errors:
        raise ValueError("\n".join(errors))
