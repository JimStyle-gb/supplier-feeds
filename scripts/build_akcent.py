# build_akcent.py
# Адаптер AkCent под CS Core: скачать XML -> фильтр по префиксам -> собрать OfferOut -> отрендерить CS YML

from __future__ import annotations

import os
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
    validate_cs_yml,
    write_if_changed,
)

SUPPLIER_NAME = "AkCent"
SUPPLIER_URL = "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"
OUT_FILE = "docs/akcent.yml"
OUTPUT_ENCODING = "utf-8"
SCHEDULE_HOUR_ALMATY = 2

AKCENT_NAME_PREFIXES: List[str] = [
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

# Проверяем, что название товара начинается с одного из заданных префиксов
def _passes_name_prefixes(name: str) -> bool:
    s = (name or "").lstrip()
    for pref in AKCENT_NAME_PREFIXES:
        if s.startswith(pref):
            return True
    return False

# Берём текст узла (без None)
def _get_text(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()

# Собираем картинки
def _collect_pictures(offer: ET.Element) -> list[str]:
    pics: list[str] = []
    for p in offer.findall("picture"):
        t = _get_text(p)
        if t:
            pics.append(t)
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
            out.append((k, v))
    for p in offer.findall("Param"):
        k = (p.get("Name") or p.get("name") or "").strip()
        v = _get_text(p)
        if k and v:
            out.append((k, v))
    return out

# Достаём vendor из vendor/params (если пусто — CS Core сам определит по имени/описанию)
def _extract_vendor(offer: ET.Element, params: list[tuple[str, str]]) -> str:
    v = _get_text(offer.find("vendor"))
    if v:
        return v
    for k, val in params:
        if k.casefold() in ("производитель", "бренд", "brand", "manufacturer"):
            return val
    return ""

# Достаём описание
def _extract_desc(offer: ET.Element) -> str:
    return _get_text(offer.find("description"))

# Достаём исходную цену (purchase_price -> price)
def _extract_price_in(offer: ET.Element) -> int:
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

    r = requests.get(SUPPLIER_URL, timeout=90)
    r.raise_for_status()
    root = ET.fromstring(r.content)

    offers_in = _extract_offers(root)
    before = len(offers_in)

    out_offers: list[OfferOut] = []
    for offer in offers_in:
        oid = (offer.get("id") or "").strip()
        if not oid:
            continue

        name = _get_text(offer.find("name"))
        if not name or not _passes_name_prefixes(name):
            continue

        available = _extract_available(offer)
        pics = _collect_pictures(offer)
        params_raw = _collect_params(offer)
        params = clean_params(params_raw)

        price_in = _extract_price_in(offer)
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
    offers_xml = "\n\n".join(
        o.to_xml(currency_id="KZT", public_vendor=(os.getenv("CS_PUBLIC_VENDOR") or "CS")) for o in out_offers
    )
    full = (
        header
        + f"<yml_catalog date=\"{build_time.strftime('%Y-%m-%d %H:%M')}\">\n"
        + "<shop><offers>\n\n"
        + meta
        + "\n\n"
        + offers_xml
        + "\n\n</offers>\n</shop>\n</yml_catalog>\n"
    )
    full = ensure_footer_spacing(full)
    validate_cs_yml(full)

    changed = write_if_changed(OUT_FILE, full, encoding=OUTPUT_ENCODING)
    print(f"[akcent] before={before} after={after} changed={changed}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
