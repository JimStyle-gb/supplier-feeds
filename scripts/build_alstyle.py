# -*- coding: utf-8 -*-
"""
AlStyle adapter (AS) — CS-шаблон.

Цель: адаптер отдаёт ИДЕАЛЬНЫЙ raw (чистые params без мусора, стабильные id/vendorCode, pictures с placeholder),
а cs/core.py делает только 100% общие вещи (keywords/description/FEED_META/writer).

Важно:
- фильтр товаров: include по categoryId (строго по списку ALSTYLE_CATEGORY_IDS или fallback)
- никаких эвристик по совместимости/кодам тут не делаем (AlStyle отдаёт свои params — мы их только чистим)
"""

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET

import requests

from cs.core import OfferOut, write_cs_feed, write_cs_feed_raw
from cs.meta import now_almaty, next_run_at_hour
from cs.pricing import compute_price
from cs.util import norm_ws, safe_int


BUILD_ALSTYLE_VERSION = "build_alstyle_v57_strict_raw_no_core_guess"

ALSTYLE_SUPPLIER = "AlStyle"
ALSTYLE_URL_DEFAULT = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
ALSTYLE_OUT_DEFAULT = "docs/alstyle.yml"
ALSTYLE_RAW_OUT = "docs/raw/alstyle.yml"
ALSTYLE_ID_PREFIX = "AS"

PLACEHOLDER_PICTURE = "https://placehold.co/800x800/png?text=No+Photo"

# Категории (include-режим). Можно переопределить ENV=ALSTYLE_CATEGORY_IDS (через запятую/пробел/перенос).
ALSTYLE_ALLOWED_CATEGORY_IDS_FALLBACK = {
    "3540", "3541", "3542", "3543", "3544", "3545",
    "3566", "3567", "3569", "3570", "3580", "3688", "3708", "3721", "3722",
    "4889", "4890", "4895", "5017", "5075", "5649",
    "5710", "5711", "5712", "5713",
    "21279", "21281", "21291", "21356", "21451",
    "21572", "21573", "21574", "21575", "21576", "21578", "21580",
    "21498", "21500",
    "21640", "21664", "21665", "21666", "21698",
    "21367", "21368", "21369", "21370", "21371", "21372",
}

# Приоритет сортировки характеристик (если core сортирует, тут задаём список)
ALSTYLE_PARAM_PRIORITY = [
    "Бренд",
    "Модель",
    "Тип",
    "Совместимость",
    "Цвет",
    "Размер",
    "Материал",
    "Гарантия",
]

# Drop-лист мусорных ключей (строго то, что реально мешает)
_DROP_KEYS_CF = {
    "артикул",
    "штрихкод",
    "код тн вэд",
    "код товара kaspi",
    "код товара kaspi.kz",
    "благотворительность",
    "снижена цена",
    "новинка",
    "короткое наименование (каз)",
    "полное наименование (каз)",
}

_RE_HAS_LETTER = re.compile(r"[A-Za-zА-Яа-яЁё]")
_RE_WS = re.compile(r"\s+")


def _parse_id_set(env: str | None, fallback: set[str]) -> set[str]:
    if not env:
        return set(fallback)
    s = env.strip()
    if not s:
        return set(fallback)
    parts = re.split(r"[\s,;]+", s)
    out = {p.strip() for p in parts if p and p.strip()}
    return out or set(fallback)


def _fetch_xml(url: str, *, timeout: int, login: str | None, password: str | None) -> str:
    auth = (login, password) if (login and password) else None
    r = requests.get(url, timeout=timeout, auth=auth)
    r.raise_for_status()
    return r.text


def _t(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def _collect_pictures(offer_el: ET.Element) -> list[str]:
    pics: list[str] = []
    for p in offer_el.findall("picture"):
        u = norm_ws(_t(p))
        if u:
            pics.append(u)
    if not pics:
        pics = [PLACEHOLDER_PICTURE]
    return pics


def _key_quality_ok(k: str) -> bool:
    # Запрещено:
    # - без букв
    # - слишком длинный ключ
    # - ключ-предложение (слишком много слов)
    kk = norm_ws(k)
    if not kk:
        return False
    if not _RE_HAS_LETTER.search(kk):
        return False
    if len(kk) > 60:
        return False
    if len(kk.split()) > 9:
        return False
    return True


def _normalize_key(k: str) -> str:
    kk = norm_ws(k)
    # исправление латинской B в (Bт)
    kk = kk.replace("Bт", "Вт").replace("BТ", "Вт")
    # единообразие мощности
    if kk == "Мощность (Bт)":
        kk = "Мощность (Вт)"
    return kk


def _should_drop_param(k: str, v: str) -> bool:
    kcf = norm_ws(k).casefold()
    vcf = norm_ws(v).casefold()

    if not _key_quality_ok(k):
        return True

    if kcf in _DROP_KEYS_CF:
        return True

    # частные мусорные булевы
    if kcf == "назначение" and vcf in ("да", "есть"):
        return True
    if kcf == "безопасность" and vcf == "есть":
        return True

    # гарантия "нет" — выкидываем, но нормальную гарантию оставляем
    if kcf == "гарантия" and vcf in ("нет", "no", "-"):
        return True

    return False


def _collect_params(offer_el: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for p in offer_el.findall("param"):
        k0 = p.get("name") or ""
        v0 = _t(p)

        k = _normalize_key(k0)
        v = norm_ws(v0)

        if not k or not v:
            continue
        if _should_drop_param(k, v):
            continue

        key = (k.casefold(), v.casefold())
        if key in seen:
            continue
        seen.add(key)
        out.append((k, v))

    # Если в параметрах есть Бренд/Модель и т.п. — хорошо.
    # Тип: добавляем строго и безопасно, только если его нет.
    keys_cf = {k.casefold() for k, _ in out}
    if "тип" not in keys_cf:
        # Берём первое слово названия (только если похоже на тип)
        name = norm_ws(_t(offer_el.find("name")))
        first = name.split()[0] if name else ""
        if first and len(first) <= 20:
            out.append(("Тип", first))

    return out


def main() -> int:
    url = os.getenv("ALSTYLE_URL", ALSTYLE_URL_DEFAULT).strip()
    out_file = os.getenv("OUT_FILE", ALSTYLE_OUT_DEFAULT).strip()
    encoding = (os.getenv("OUTPUT_ENCODING") or "utf-8").strip() or "utf-8"

    hour = int(os.getenv("SCHEDULE_HOUR_ALMATY", "1"))
    timeout = int(os.getenv("HTTP_TIMEOUT", "90"))

    login = os.getenv("ALSTYLE_LOGIN")
    password = os.getenv("ALSTYLE_PASSWORD")

    allowed = _parse_id_set(os.getenv("ALSTYLE_CATEGORY_IDS"), ALSTYLE_ALLOWED_CATEGORY_IDS_FALLBACK)

    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour)

    xml_text = _fetch_xml(url, timeout=timeout, login=login, password=password)
    root = ET.fromstring(xml_text)

    offers_in = root.findall(".//offer")
    before = len(offers_in)

    out_offers: list[OfferOut] = []
    in_true = 0
    in_false = 0

    for o in offers_in:
        cat = norm_ws(_t(o.find("categoryId")))
        if allowed and (not cat or cat not in allowed):
            continue

        raw_id = norm_ws(o.get("id") or _t(o.find("vendorCode")))
        name = norm_ws(_t(o.find("name")))
        if not name or not raw_id:
            continue

        oid = raw_id if raw_id.upper().startswith(ALSTYLE_ID_PREFIX) else f"{ALSTYLE_ID_PREFIX}{raw_id}"

        # available: offer@available -> <available>
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
        desc_src = _t(o.find("description")) or ""

        # цена: purchase_price -> price
        price_in = safe_int(_t(o.find("purchase_price")))
        if price_in is None:
            price_in = safe_int(_t(o.find("price")))

        price = compute_price(price_in)

        # не раскрываем имя поставщика как vendor
        if vendor_src.casefold() == ALSTYLE_SUPPLIER.casefold():
            vendor_src = ""

        out_offers.append(
            OfferOut(
                oid=oid,
                available=available,
                name=name,
                price=price,
                pictures=pics,
                vendor=vendor_src,
                params=params,
                native_desc=desc_src,
            )
        )

    after = len(out_offers)

    # стабильный порядок
    out_offers.sort(key=lambda x: x.oid)

    write_cs_feed_raw(
        out_offers,
        supplier=ALSTYLE_SUPPLIER,
        supplier_url=url,
        out_file=ALSTYLE_RAW_OUT,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        currency_id="KZT",
    )

    changed = write_cs_feed(
        out_offers,
        supplier=ALSTYLE_SUPPLIER,
        supplier_url=url,
        out_file=out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        public_vendor=os.getenv("PUBLIC_VENDOR", "CS").strip() or "CS",
        currency_id="KZT",
        param_priority=ALSTYLE_PARAM_PRIORITY,
    )

    print(
        f"[build_alstyle] OK | version={BUILD_ALSTYLE_VERSION} | offers_in={before} | offers_out={after} | "
        f"in_true={in_true} | in_false={in_false} | changed={'yes' if changed else 'no'} | file={out_file}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
