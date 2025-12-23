# build_akcent.py
# Адаптер AkCent под CS Core (скачать -> отфильтровать -> собрать OfferOut -> отрендерить как CS)

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple

import requests

from cs.core import (
    OfferOut,
    build_keywords,
    clean_params,
    compute_price,
    compute_stats,
    ensure_footer_spacing,
    make_feed_meta,
    make_footer,
    make_header,
    now_almaty,
    pick_vendor,
    validate_cs_yml,
    write_if_changed,
    picture_placeholder,
)

SUPPLIER_NAME = "AkCent"
SUPPLIER_URL = "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"
OUT_FILE = "docs/akcent.yml"
ENCODING = "utf-8"

# Префиксы (include) — как в твоём текущем AkCent
AKCENT_NAME_PREFIXES = [
    "EPSON",
    "CANON",
    "HP",
    "XEROX",
    "KYOCERA",
    "RICOH",
    "BROTHER",
    "SINDOH",
    "PANTUM",
    "TOSHIBA",
    "KONICA",
    "SHARP",
    "OKI",
    "DELL",
    "LENOVO",
    "ASUS",
    "ACER",
    "VIEWSONIC",
    "BENQ",
    "OPTOMA",
]

# Нормализация vendor, чтобы не светить поставщика и чистить мусор
_VENDOR_CLEAN = [
    (re.compile(r"\bAk\s*Cent\b", flags=re.I), ""),
    (re.compile(r"\bak-cent\b", flags=re.I), ""),
    (re.compile(r"\bproj\b\.?$", flags=re.I), ""),  # Epson Proj -> Epson
]
_VENDOR_MAP = {
    "epson proj": "Epson",
    "epson projector": "Epson",
    "viewsonic proj": "ViewSonic",
    "viewsonic projector": "ViewSonic",
}

# Вытаскиваем текст (без None)
def _t(parent: ET.Element, tag: str) -> str:
    el = parent.find(tag)
    if el is None or el.text is None:
        return ""
    return el.text.strip()

# Вытаскиваем все картинки (если нет — пусто, заглушку поставит core)
def _collect_pictures(offer: ET.Element) -> List[str]:
    pics: List[str] = []
    for p in offer.findall(".//picture"):
        if p.text:
            u = p.text.strip()
            if u:
                pics.append(u)
    # Иногда у поставщиков бывает image/img
    for p in offer.findall(".//image"):
        if p.text:
            u = p.text.strip()
            if u:
                pics.append(u)
    for p in offer.findall(".//img"):
        if p.text:
            u = p.text.strip()
            if u:
                pics.append(u)
    # уникализируем, сохраняя порядок
    out: List[str] = []
    seen = set()
    for u in pics:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

# Собираем параметры из <param name="...">value</param>
def _collect_params(offer: ET.Element) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for p in offer.findall(".//param"):
        name = (p.get("name") or "").strip()
        val = (p.text or "").strip()
        if not name:
            continue
        if not val:
            continue
        out.append((name, val))
    return out

# Нормализуем vendor (не должен раскрывать поставщика)
def _normalize_vendor(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return ""
    for rx, rep in _VENDOR_CLEAN:
        s = rx.sub(rep, s).strip()
    if not s:
        return ""
    key = re.sub(r"\s+", " ", s).casefold()
    if key in _VENDOR_MAP:
        return _VENDOR_MAP[key]
    return s

# Берём "сырой" vendor из XML/params
def _extract_vendor(offer: ET.Element, params: List[Tuple[str, str]]) -> str:
    # прямой vendor тег
    v = _t(offer, "vendor")
    if v:
        return _normalize_vendor(v)
    # param Производитель
    for k, val in params:
        if k.strip().casefold() in ("производитель", "бренд", "brand", "manufacturer"):
            return _normalize_vendor(val)
    return ""

# Берём цену (сырой price) из XML/params
def _extract_price_raw(offer: ET.Element, params: List[Tuple[str, str]]) -> int:
    # пробуем <price>
    p = _t(offer, "price")
    if p:
        try:
            return int(float(p.replace(" ", "").replace("\u00a0", "").replace(",", ".")))
        except Exception:
            pass
    # param "Цена дилерского портала" (как в твоём AkCent)
    for k, val in params:
        if k.strip().casefold() in ("цена дилерского портала", "цена", "price"):
            s = val.replace(" ", "").replace("\u00a0", "")
            s = re.sub(r"[^0-9,\.]", "", s)
            s = s.replace(",", ".")
            try:
                return int(float(s))
            except Exception:
                continue
    return 0

# Include-фильтр по имени (как у тебя)
def _name_allowed(name: str) -> bool:
    n = (name or "").strip()
    if not n:
        return False
    up = n.upper()
    return any(up.startswith(pfx) for pfx in AKCENT_NAME_PREFIXES)

# main
def main() -> int:
    # Время сборки всегда берём из core (оно учитывает CS_FORCE_BUILD_TIME_ALMATY)
    build_time = now_almaty()

    r = requests.get(SUPPLIER_URL, timeout=90)
    r.raise_for_status()
    xml_bytes = r.content

    root = ET.fromstring(xml_bytes)

    offers_in = root.findall(".//offer")
    before = len(offers_in)

    out_offers: List[OfferOut] = []

    for off in offers_in:
        oid = (off.get("id") or "").strip() or _t(off, "id")
        if not oid:
            continue

        name = _t(off, "name")
        if not _name_allowed(name):
            continue

        params_raw = _collect_params(off)
        price_raw = _extract_price_raw(off, params_raw)
        price = compute_price(price_raw)

        pics = _collect_pictures(off)

        vendor_src = _extract_vendor(off, params_raw)

        # keywords: только из name + хвост городов/шаблон в core
        keywords = build_keywords(name)

        # чистим params через core (там же выкинутся служебные и мусорные)
        params_clean = clean_params(params_raw)

        out_offers.append(
            OfferOut(
                offer_id=str(oid),
                vendor_code=str(oid),
                name=name,
                price=price,
                pictures=pics,
                vendor=vendor_src,
                currency_id="KZT",
                available=True,  # если у AkCent нет корректного склада — можно всегда true; при желании вытащим позже
                native_desc=_t(off, "description"),
                params=params_clean,
                keywords=keywords,
            )
        )

    stats = compute_stats(out_offers, before=before)

    public_vendor = (os.getenv("CS_PUBLIC_VENDOR", "CS") or "CS").strip()

    offers_xml = "\n\n".join(
        off.to_xml(currency_id="KZT", public_vendor=public_vendor) for off in out_offers
    )

    header = make_header(build_time, encoding=ENCODING)
    meta = make_feed_meta(
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL,
        build_time=build_time,
        next_run_almaty="2025-12-24 02:00:00",
        stats=stats,
    )
    footer = make_footer()

    full = header + meta + "<shop>\n<offers>\n\n" + offers_xml + "\n\n</offers>\n</shop>\n" + footer
    full = ensure_footer_spacing(full)

    validate_cs_yml(full)

    changed = write_if_changed(OUT_FILE, full, encoding=ENCODING)
    print(f"[akcent] offers_in={before} offers_out={len(out_offers)} changed={changed}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
