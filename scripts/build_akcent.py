from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET

import requests

from cs.core import (
    OfferOut,
    clean_params,
    compute_price,
    ensure_footer_spacing,
    make_feed_meta,
    make_footer,
    make_header,
    next_run_at_hour,
    now_almaty,
    safe_int,
    write_if_changed,
    validate_cs_yml,
)

SUPPLIER_NAME = "AkCent"
SUPPLIER_URL = "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"
OUT_FILE = "docs/akcent.yml"
OUTPUT_ENCODING = "utf-8"
SCHEDULE_HOUR_ALMATY = 2

AKCENT_NAME_PREFIXES: list[str] = [
    "C13T55",
    "Ёмкость для отработанных чернил",
    "Интерактивная доска",
    "Интерактивная панель",
    "Интерактивный дисплей",
    "Картридж",
    "Ламинатор",
    "Монитор",
    "МФУ",
    "Переплетчик",
    "Пленка для ламинирования",
    "Плоттер",
    "Принтер",
    "Проектор",
    "Сканер",
    "Чернила",
    "Шредер",
    "Экономичный набор",
    "Экран",
]

# Префиксы в casefold (для нечувствительности к регистру)
AKCENT_NAME_PREFIXES_CF = tuple((p or "").casefold() for p in AKCENT_NAME_PREFIXES)

# Параметры AkCent, которые не являются характеристиками (только для этого поставщика)
AKCENT_PARAM_DROP = {"Сопутствующие товары"}

# Иногда поставщик кладёт страну в vendor/Производитель — такие значения лучше не использовать как бренд
COUNTRY_VENDOR_BLACKLIST_CF = {
    "китай", "china",
    "россия", "russia",
    "казахстан", "kazakhstan",
    "турция", "turkey",
    "сша", "usa", "united states",
    "германия", "germany",
    "япония", "japan",
    "корея", "korea",
    "великобритания", "uk", "united kingdom",
    "франция", "france",
    "италия", "italy",
    "испания", "spain",
    "польша", "poland",
    "тайвань", "taiwan",
    "таиланд", "thailand",
    "вьетнам", "vietnam",
    "индия", "india",
}


def _clean_vendor(v: str) -> str:
    # vendor = бренд; если туда прилетает страна/общие слова — убираем, чтобы не портить бренд.
    s = (v or "").strip()
    if not s:
        return ""
    cf = s.casefold()
    # чистим "made in ..." и явные страны
    if "made in" in cf or cf in COUNTRY_VENDOR_BLACKLIST_CF:
        return ""
    return s


# Приоритет характеристик (как в AlStyle: сначала важное, потом остальное по алфавиту)
AKCENT_PARAM_PRIORITY = [
    "Бренд",
    "Производитель",
    "Модель",
    "Артикул",
    "Тип",
    "Назначение",
    "Совместимость",
    "Цвет",
    "Размер",
    "Материал",
    "Гарантия",
    "Интерфейс",
    "Подключение",
    "Разрешение",
    "Мощность",
    "Напряжение",
]

# Нормализуем URL (если вдруг пришёл без схемы)
def _normalize_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return u
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return "https://" + u.lstrip("/")

# Проверяем, что название товара начинается с одного из заданных префиксов
def _passes_name_prefixes(name: str) -> bool:
    s = (name or "").lstrip()
    if not s:
        return False
    s_cf = s.casefold()
    for pref_cf in AKCENT_NAME_PREFIXES_CF:
        if pref_cf and s_cf.startswith(pref_cf):
            return True
    return False


# Генерирует стабильный CS-oid для AkCent (offer id == vendorCode)
# Основной ключ: AC + offer@article (в XML он есть; в id оставляем только ASCII)
# Важно: если в article есть символы вроде "*", кодируем их как _2A, чтобы не ловить коллизии.
def _make_oid(offer: ET.Element, name: str) -> str | None:
    art = (offer.get("article") or "").strip()
    if art:
        out: list[str] = []
        for ch in art:
            if re.fullmatch(r"[A-Za-z0-9_.-]", ch):
                out.append(ch)
            else:
                out.append(f"_{ord(ch):02X}")
        part = "".join(out)
        if part:
            return "AC" + part    # fallback (на случай если поставщик поломает article)
    # ВАЖНО: никаких хэшей от имени — только стабильный id из исходных атрибутов.
    sid = (offer.get("id") or "").strip()
    if sid:
        out: list[str] = []
        for ch in sid:
            if re.fullmatch(r"[A-Za-z0-9_.-]", ch):
                out.append(ch)
            else:
                out.append(f"_{ord(ch):02X}")
        part = "".join(out)
        if part:
            return "AC" + part

    return None
# Берём текст узла (без None)
def _get_text(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()

# Собираем картинки
def _collect_pictures(offer: ET.Element) -> list[str]:
    pics: list[str] = []
    for p in offer.findall("picture"):
        t = _normalize_url(_get_text(p))
        if t:
            pics.append(t)
    # уникализация (сохраняем порядок)
    out: list[str] = []
    seen: set[str] = set()
    for u in pics:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

# Собираем параметры (param/Param)
def _collect_params(offer: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for p in offer.findall("param"):
        k = (p.get("name") or "").strip()
        v = _get_text(p)
        if k and v:
                # Мусор от поставщика: Гарантия=0 (убираем)
                if k.casefold() == 'гарантия' and v.strip().casefold() in ('0', '0 мес', '0 месяцев', '0мес'):
                    continue
                out.append((k, v))
    for p in offer.findall("Param"):
        k = (p.get("name") or p.get("Name") or "").strip()
        v = _get_text(p)
        if k and v:
                # Мусор от поставщика: Гарантия=0 (убираем)
                if k.casefold() == 'гарантия' and v.strip().casefold() in ('0', '0 мес', '0 месяцев', '0мес'):
                    continue
                out.append((k, v))
    return out

# Достаём vendor (если пусто — CS Core сам определит бренд по имени/парам/описанию)
def _extract_vendor(offer: ET.Element, params: list[tuple[str, str]]) -> str:
    v = _clean_vendor(_get_text(offer.find("vendor")))
    if v:
        return v
    for k, val in params:
        if k.casefold() in ("производитель", "бренд", "brand", "manufacturer"):
            v2 = _clean_vendor(val)
            if v2:
                return v2
    return ""

# Достаём описание
def _extract_desc(offer: ET.Element) -> str:
    return _get_text(offer.find("description"))

# Достаём исходную цену:
# AkCent кладёт цены в <prices><price type="Цена дилерского портала KZT">41727</price> ...</prices>
def _extract_price_in(offer: ET.Element) -> int:
    prices = offer.find("prices")
    if prices is not None:
        best_any: int | None = None
        best_rrp: int | None = None
        for pe in prices.findall("price"):
            t = (pe.get("type") or "").casefold()
            cur = (pe.get("currencyId") or "").strip().upper()
            v = safe_int(_get_text(pe))
            if not v:
                continue
            if cur and cur != "KZT":
                continue

            # 1) приоритет — дилерская цена
            if "дилер" in t or "dealer" in t:
                return int(v)

            # 2) RRP как запасной приоритет
            if "rrp" in t:
                best_rrp = int(v)

            if best_any is None:
                best_any = int(v)

        if best_rrp is not None:
            return best_rrp
        if best_any is not None:
            return best_any

    # запасные варианты (на случай другого формата)
    p1 = safe_int(_get_text(offer.find("purchase_price")))
    if p1:
        return int(p1)
    p2 = safe_int(_get_text(offer.find("price")))
    return int(p2 or 0)

# Достаём доступность (если нет атрибута — считаем true)
def _extract_available(offer: ET.Element) -> bool:
    a = (offer.get("available") or "").strip().lower()
    if not a:
        return True
    return a in ("1", "true", "yes", "y", "да")

# Вытаскиваем offers из XML
def _extract_offers(root: ET.Element) -> list[ET.Element]:
    offers_node = root.find(".//offers")
    if offers_node is None:
        return []
    return list(offers_node.findall("offer"))

# main
def main() -> int:
    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, SCHEDULE_HOUR_ALMATY)

    r = requests.get(_normalize_url(SUPPLIER_URL), timeout=90)
    r.raise_for_status()
    root = ET.fromstring(r.content)

    offers_in = _extract_offers(root)
    before = len(offers_in)

    out_offers: list[OfferOut] = []

    price_missing = 0

    for offer in offers_in:
        name = _get_text(offer.find("name"))
        if not name or not _passes_name_prefixes(name):
            continue

        oid = _make_oid(offer, name)
        if not oid:
            continue
        if not oid:
            continue

        available = _extract_available(offer)
        pics = _collect_pictures(offer)
        params_raw = _collect_params(offer)
        params = clean_params(params_raw, drop=AKCENT_PARAM_DROP)

        price_in = _extract_price_in(offer)
        if not price_in or int(price_in) < 1:
            price_missing += 1
        price = compute_price(price_in)

        vendor = _extract_vendor(offer, params)
        native_desc = _extract_desc(offer)

        out_offers.append(
            OfferOut(
                oid=oid,
                available=available,
                name=name,
                price=price,
                pictures=pics,
                vendor=vendor,
                params=params,
                native_desc=native_desc,
            )
        )

    after = len(out_offers)
    in_true = sum(1 for o in out_offers if o.available)
    in_false = after - in_true

    header = make_header(build_time, encoding=OUTPUT_ENCODING)
    meta = make_feed_meta(
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL,
        build_time=build_time,
        next_run=next_run,
        before=before,
        after=after,
        in_true=in_true,
        in_false=in_false,
    )

    public_vendor = (os.getenv("PUBLIC_VENDOR") or os.getenv("CS_PUBLIC_VENDOR") or "CS").strip() or "CS"

    # Стабильный порядок офферов (меньше лишних диффов между коммитами)
    out_offers.sort(key=lambda x: x.oid)

    offers_xml = "\n\n".join(
        o.to_xml(currency_id="KZT", public_vendor=public_vendor, param_priority=AKCENT_PARAM_PRIORITY) for o in out_offers
    )

    full = header + meta + "\n\n" + offers_xml + "\n\n" + make_footer()
    full = ensure_footer_spacing(full)
    validate_cs_yml(full)

    changed = write_if_changed(OUT_FILE, full, encoding=OUTPUT_ENCODING)
    print(f"[akcent] before={before} after={after} price_missing={price_missing} changed={changed}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())

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
        # если нет стабильного id/vendorCode — пропускаем (никаких хэшей)
        if not raw_id:
            continue

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

# -*- coding: utf-8 -*-
"""
CopyLine adapter — сборщик по шаблону CS (использует scripts/cs/core.py).
Задача адаптера: забрать данные поставщика (sitemap + сайт) и отдать в CS ядро список OfferOut.
"""

from __future__ import annotations

import io
import os
import re
import time
import random
import hashlib
from datetime import datetime, timedelta

# Логи (можно выключить: VERBOSE=0)
def _pick_copyline_best_picture(pictures: list[str]) -> list[str]:
    """CopyLine: оставить только реальные фото товара.
    Берём только картинки из img_products, чистим мусор, сохраняем порядок (full_* сначала).
    """
    if not pictures:
        return [PLACEHOLDER_PIC]

    cleaned: list[str] = []
    seen: set[str] = set()

    for p in pictures:
        if not p:
            continue
        p = str(p).strip()
        if not p:
            continue

        # только реальные фото товаров (без логотипов/иконок/печатных и т.п.)
        if "/components/com_jshopping/files/img_products/" not in p.replace("\\", "/"):
            continue

        # нормализуем HTML-экранку
        p = p.replace("&amp;", "&")

        if p in seen:
            continue
        seen.add(p)
        cleaned.append(p)

    if not cleaned:
        return [PLACEHOLDER_PIC]

    def is_full(u: str) -> bool:
        base = u.split("/")[-1]
        return base.startswith("full_") or "/full_" in u

    fulls = [u for u in cleaned if is_full(u)]
    normals = [u for u in cleaned if not is_full(u)]

    # если на странице есть только обычное фото — его и оставим
    return (fulls + normals) if (fulls + normals) else [PLACEHOLDER_PIC]
def _pick_copyline_picture(pics: list[str]) -> list[str]:
    """# CopyLine: одна картинка на товар — full_ если есть, иначе обычная. Только img_products."""
    if not pics:
        return []

    def norm(u: str) -> str:
        u = (u or "").strip()
        u = u.split("#", 1)[0]
        return u

    candidates: list[str] = []
    for u in pics:
        u = norm(u)
        if not u:
            continue
        if "components/com_jshopping/files/img_products/" not in u:
            continue
        if "/img_products/thumb_" in u:
            continue
        candidates.append(u)

    if not candidates:
        return []

    # full_ приоритет
    for u in candidates:
        base = u.rsplit("/", 1)[-1]
        if base.startswith("full_"):
            return [u]

    return [candidates[0]]

VERBOSE = os.environ.get("VERBOSE", "0") in ("1","true","True","yes","YES")

def log(*args, **kwargs) -> None:
    # Печать логов (в Actions удобно оставлять краткие метки)
    # Поддерживаем kwargs типа flush/end/sep, чтобы не ловить TypeError.
    if VERBOSE:
        if "flush" not in kwargs:
            kwargs["flush"] = True
        print(*args, **kwargs)

import requests
from bs4 import BeautifulSoup
try:
    from openpyxl import load_workbook
except Exception:
    load_workbook = None  # sitemap-режим работает без openpyxl

from cs.core import (
    CURRENCY_ID_DEFAULT,
    OfferOut,
    compute_price,
    ensure_footer_spacing,
    make_feed_meta,
    make_footer,
    make_header,
    now_almaty,
    validate_cs_yml,
    write_if_changed,
)

# -----------------------------
# Настройки
