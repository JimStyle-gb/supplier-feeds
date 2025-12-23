# -*- coding: utf-8 -*-
"""
AlStyle adapter — сборщик по шаблону CS (использует scripts/cs/core.py).
Важно: здесь только "индивидуальная часть" поставщика: скачивание, парсинг, фильтр категорий.
"""

from __future__ import annotations
import os
from xml.etree import ElementTree as ET

import requests

from cs.core import (
    CURRENCY_ID_DEFAULT,
    OUTPUT_ENCODING_DEFAULT,
    OfferOut,
    compute_price,
    ensure_footer_spacing,
    make_feed_meta,
    make_footer,
    make_header,
    now_almaty,
    next_run_at_hour,
    norm_ws,
    parse_id_set,
    safe_int,
    stable_id,
    write_if_changed,
    validate_cs_yml
)

# Конфиг поставщика AlStyle
ALSTYLE_SUPPLIER = "AlStyle"
ALSTYLE_URL_DEFAULT = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
ALSTYLE_OUT_DEFAULT = "docs/alstyle.yml"
ALSTYLE_ID_PREFIX = "AS"

# Категории (источник правды: хардкод; можно переопределить ENV=ALSTYLE_CATEGORY_IDS)
ALSTYLE_ALLOWED_CATEGORY_IDS_FALLBACK = {
    "3540", "3541", "3542", "3543", "3544", "3545", "3566", "3567", "3569", "3570",
    "3580", "3688", "3708", "3721", "3722", "4889", "4890", "4895", "5017", "5075",
    "5649", "5710", "5711", "5712", "5713", "21279", "21281", "21291", "21356",
    "21367", "21368", "21369", "21370", "21371", "21372", "21451", "21498", "21500",
    "21572", "21573", "21574", "21575", "21576", "21578", "21580", "21581", "21583",
    "21584", "21585", "21586", "21588", "21591", "21640", "21664", "21665", "21666",
    "21698",
}

# Приоритет характеристик (сначала важные, потом остальное по алфавиту)
ALSTYLE_PARAM_PRIORITY = [
    "Бренд",
    "Модель",
    "Артикул",
    "Тип",
    "Совместимость",
    "Цвет",
    "Размер",
    "Материал",
    "Гарантия",
]


# Скачивает XML поставщика (с опциональным basic-auth)
def _fetch_xml(url: str, timeout: int, login: str | None, password: str | None) -> bytes:
    auth = (login, password) if (login and password) else None
    r = requests.get(url, timeout=timeout, auth=auth)
    r.raise_for_status()
    return r.content


# Берет текст из тега
def _t(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text


# Собирает список картинок
def _collect_pictures(offer_el: ET.Element) -> list[str]:
    pics: list[str] = []
    seen: set[str] = set()
    for p in offer_el.findall("picture"):
        u = norm_ws(_t(p))
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        pics.append(u)
    return pics


# Собирает характеристики param
def _collect_params(offer_el: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for p in offer_el.findall("param"):
        k = norm_ws(p.get("name") or "")
        v = norm_ws(_t(p))
        if not k or not v:
            continue
        out.append((k, v))
    return out


def main() -> int:
    url = os.getenv("ALSTYLE_URL", ALSTYLE_URL_DEFAULT).strip()
    out_file = os.getenv("OUT_FILE", ALSTYLE_OUT_DEFAULT).strip()
    encoding = os.getenv("OUTPUT_ENCODING", OUTPUT_ENCODING_DEFAULT).strip() or OUTPUT_ENCODING_DEFAULT

    # публичный vendor (никогда НЕ supplier_name)
    public_vendor = os.getenv("PUBLIC_VENDOR", "CS").strip() or "CS"

    # schedule hour только для FEED_META (workflow сам решает, когда запускать)
    hour = int(os.getenv("SCHEDULE_HOUR_ALMATY", "1"))
    timeout = int(os.getenv("HTTP_TIMEOUT", "90"))

    login = os.getenv("ALSTYLE_LOGIN")
    password = os.getenv("ALSTYLE_PASSWORD")

    allowed = parse_id_set(os.getenv("ALSTYLE_CATEGORY_IDS"), ALSTYLE_ALLOWED_CATEGORY_IDS_FALLBACK)

    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour)

    raw = _fetch_xml(url, timeout=timeout, login=login, password=password)
    root = ET.fromstring(raw)

    offers_in = root.findall(".//offer")
    before = len(offers_in)

    out_offers: list[OfferOut] = []
    in_true = 0
    in_false = 0

    for o in offers_in:
        cat = norm_ws(_t(o.find("categoryId")))
        # include-режим: если список категорий задан — пропускаем только те, что входят в allowed
        if allowed and (not cat or cat not in allowed):
            continue

        raw_id = norm_ws(o.get("id") or _t(o.find("vendorCode")))
        name = norm_ws(_t(o.find("name")))
        if not name:
            # если вообще нет названия — пропустим
            continue

        # stable id если нет id
        if not raw_id:
            raw_id = stable_id(ALSTYLE_ID_PREFIX, name)

        # vendorCode/id: AS + id (если не начинается на AS)
        oid = raw_id if raw_id.upper().startswith(ALSTYLE_ID_PREFIX) else f"{ALSTYLE_ID_PREFIX}{raw_id}"

        # available: атрибут offer@available (если нет — попробуем <available>)
        av_attr = (o.get("available") or "").strip().lower()
        if av_attr in ("true", "1", "yes"):
            available = True
        elif av_attr in ("false", "0", "no"):
            available = False
        else:
            av_tag = _t(o.find("available")).strip().lower()
            available = av_tag in ("true", "1", "yes")

        if available:
            in_true += 1
        else:
            in_false += 1

        pics = _collect_pictures(o)
        params = _collect_params(o)

        vendor_src = norm_ws(_t(o.find("vendor")))
        desc_src = _t(o.find("description"))  # может быть CDATA — ET вернет как text
        if desc_src is None:
            desc_src = ""

        # цена: сначала purchase_price, потом price
        price_in = safe_int(_t(o.find("purchase_price")))
        if price_in is None:
            price_in = safe_int(_t(o.find("price")))

        price = compute_price(price_in)



        # vendor: не раскрываем имя поставщика; если vendor_src совпал с поставщиком — считаем пустым
        vendor_src_norm = norm_ws(vendor_src)
        if vendor_src_norm.casefold() == ALSTYLE_SUPPLIER.casefold():
            vendor_src_norm = ""
        out_offers.append(
            OfferOut(
                oid=oid,
                available=available,
                name=name,
                price=price,
                pictures=pics,
                vendor=vendor_src_norm,
                params=params,
                native_desc=desc_src,
            )
        )

    after = len(out_offers)

    feed_meta = make_feed_meta(
        ALSTYLE_SUPPLIER,
        url,
        build_time,
        next_run,
        before=before,
        after=after,
        in_true=in_true,
        in_false=in_false,
    )

    header = make_header(build_time, encoding=encoding)
    footer = make_footer()

    offers_xml = "\n\n".join(
        [off.to_xml(currency_id=CURRENCY_ID_DEFAULT, public_vendor=public_vendor, param_priority=ALSTYLE_PARAM_PRIORITY) for off in out_offers]
    )

    full = header + "\n" + feed_meta + "\n\n" + offers_xml + "\n" + footer
    full = ensure_footer_spacing(full)

    # Страховочная валидация (если что-то сломалось — падаем сборкой)
    validate_cs_yml(full)

    changed = write_if_changed(out_file, full, encoding=encoding)

    print(
        f"[build_alstyle] OK | offers_in={before} | offers_out={after} | in_true={in_true} | in_false={in_false} | "
        f"changed={'yes' if changed else 'no'} | file={out_file}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
