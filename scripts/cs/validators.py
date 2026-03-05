# -*- coding: utf-8 -*-
"""
CS Validators — проверки готового CS-фида.

Этап 7: вынос в отдельный модуль, без изменения логики.
Важно: модуль НЕ импортирует cs/core.py (без циклических импортов).
"""

from __future__ import annotations

import re


def validate_cs_yml(xml: str, *, param_drop_default_cf: set[str]) -> None:
    errors: list[str] = []

    # Глобальные запреты
    if "<available>" in xml:
        errors.append("Найден тег <available> (должен быть только available=\"true/false\" в <offer>).")

    # Shuko не должно встречаться вообще
    if re.search(r"\bShuko\b", xml, flags=re.I):
        errors.append("Найдено слово 'Shuko' (нужно 'Schuko').")

    # Служебные параметры не должны просачиваться
    drop_names = param_drop_default_cf

    # Прогон по офферам
    in_offer = False
    offer_id = ""
    has_picture = False
    vendor_code = ""
    keywords = ""
    price_ok = True
    ids_seen: set[str] = set()
    hash_like_ids: list[str] = []
    _RE_HASH_OID = re.compile(r"^(AC|AS|CL|NP)H[0-9A-F]{10}$")  # VT может иметь OEM-коды вида VTH... — не считаем это hash-id

    bad_no_pic: list[str] = []
    bad_vendorcode: list[str] = []
    bad_keywords: list[str] = []
    bad_params: list[str] = []
    bad_price: list[str] = []
    dup_ids: list[str] = []

    # Для keywords может быть много текста — берём по строке (рендер у нас одно-строчный)
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
                if _RE_HASH_OID.match(offer_id):
                    hash_like_ids.append(offer_id)
            continue

        if not in_offer:
            continue

        if "<picture>" in s:
            has_picture = True

        if s.startswith("<vendorCode>"):
            vendor_code = re.sub(r"</?vendorCode>", "", s).strip()

        if s.startswith("<keywords>"):
            kw = re.sub(r"</?keywords>", "", s).strip()
            keywords = kw

        if s.startswith("<price>"):
            pr = re.sub(r"</?price>", "", s).strip()
            pi = safe_int(pr)
            if pi is None or pi < 100:
                price_ok = False

        # param проверки
        if s.startswith("<param ") and 'name="' in s:
            mname = re.search(r'name="([^"]+)"', s)
            pname = mname.group(1) if mname else ""
            pname_n = norm_ws(pname)
            pname_key = pname_n.casefold()

            # служебные/запрещённые
            if pname_key in drop_names:
                bad_params.append(f"{offer_id}: запрещённый param '{pname_n}'")

            # Bт не должно быть
            if re.search(r"Bт", pname_n):
                bad_params.append(f"{offer_id}: param содержит 'Bт' -> '{pname_n}'")

            # значение
            # <param name="X">VALUE</param>
            mv = re.search(r'">(.+)</param>$', s)
            pval = mv.group(1) if mv else ""
            pval_n = norm_ws(pval)
            vv_compact = pval_n.replace(" ", "")
            if re.fullmatch(r"[-–—.]+", vv_compact) or vv_compact in {"..", "..."}:
                bad_params.append(f"{offer_id}: пустышка в param '{pname_n}'='{pval_n}'")
            if "..." in vv_compact and not re.search(r"\d+\s*\.\.\.\s*\d+", vv_compact):
                if vv_compact.endswith("...") or re.search(r"[A-Za-zА-Яа-яЁё]\.\.\.", vv_compact):
                    bad_params.append(f"{offer_id}: обрезанное значение param '{pname_n}'='{pval_n}'")

        if s == "</offer>":
            # проверка на картинку
            if not has_picture:
                bad_no_pic.append(offer_id)

            # vendorCode должен совпадать с id
            if offer_id and vendor_code and vendor_code != offer_id:
                bad_vendorcode.append(offer_id)

            # keywords: должны быть через запятые
            if keywords:
                if "," not in keywords:
                    bad_keywords.append(offer_id)
            else:
                bad_keywords.append(offer_id)

            if not price_ok:
                bad_price.append(offer_id)

            in_offer = False
            offer_id = ""
            continue

    # Сводка ошибок
    if dup_ids:
        errors.append(f"Дубликаты offer id: {', '.join(dup_ids[:10])}" + ("..." if len(dup_ids) > 10 else ""))
    if hash_like_ids:
        errors.append(
            "Найдены hash-похожие offer id (похоже на md5/хеш). Это запрещено: "
            + ", ".join(hash_like_ids[:10])
            + ("..." if len(hash_like_ids) > 10 else "")
        )


    if bad_no_pic:
        errors.append(f"Есть offer без <picture>: {', '.join(bad_no_pic[:10])}" + ("..." if len(bad_no_pic) > 10 else ""))

    if bad_vendorcode:
        errors.append(f"vendorCode != offer/@id: {', '.join(bad_vendorcode[:10])}" + ("..." if len(bad_vendorcode) > 10 else ""))

    if bad_keywords:
        errors.append(f"keywords без запятых/пустые: {', '.join(bad_keywords[:10])}" + ("..." if len(bad_keywords) > 10 else ""))

    if bad_price:
        errors.append(f"price < 100 или невалидный: {', '.join(bad_price[:10])}" + ("..." if len(bad_price) > 10 else ""))

    if bad_params:
        # показываем первые 15 строк, чтобы лог был читаемый
        head = "\n".join(bad_params[:15])
        tail = "..." if len(bad_params) > 15 else ""
        errors.append("Проблемные params:\n" + head + ("\n" + tail if tail else ""))

    if errors:
        raise ValueError("CS-валидация не пройдена:\n- " + "\n- ".join(errors))



# ----------------------------- Backward-compatible wrappers -----------------------------
# (core стал тоньше: реализация вынесена в scripts/cs/description.py)
