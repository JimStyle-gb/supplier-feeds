#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlStyle feed builder v121_cp1251_fix

Строит docs/alstyle.yml из XML поставщика AlStyle.
Кодировка вывода: windows-1251, безопасная (xmlcharrefreplace для неподдерживаемых символов).
"""

from __future__ import annotations

import sys
import math
import datetime as dt
from pathlib import Path
from typing import Optional, List, Dict, Set
import re
import xml.etree.ElementTree as ET

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

SUPPLIER_NAME = "AlStyle"
SUPPLIER_URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
ENCODING_SUPPLIER = "utf-8"
ENCODING_OUT = "windows-1251"

DEFAULT_OUTPUT = Path("docs/alstyle.yml")
DEFAULT_CATEGORIES = Path("docs/alstyle_categories.txt")

VENDOR_PREFIX = "AS"
DEFAULT_CURRENCY = "KZT"
TIMEZONE_OFFSET_HOURS = 5  # Алматы

PARAM_BLACKLIST = {
    "Артикул",
    "Штрихкод",
    "Код товара Kaspi",
    "Код ТН ВЭД",
    "Объём",
    "Снижена цена",
}

PARAM_PRIORITY = [
    "Бренд",
    "Модель",
    "Серия",
    "Тип",
    "Назначение",
    "Цвет",
    "Мощность",
    "Напряжение",
    "Ёмкость батареи",
]

WHATSAPP_BLOCK = """\n<!-- WhatsApp -->
<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;"><p style="text-align:center; margin:0 0 12px;"><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,.08);">&#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!</a></p><div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;"><h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3><ul style="margin:0; padding-left:18px;"><li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li><li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li></ul><hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" /><h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3><ul style="margin:0; padding-left:18px;"><li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li><li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li><li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li><li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li></ul></div></div>\n"""


def _now_almaty() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=TIMEZONE_OFFSET_HOURS)


def _read_text(path: Path, encoding: str) -> str:
    return path.read_text(encoding=encoding, errors="replace")


def _make_encoding_safe(text: str, encoding: str) -> str:
    return text.encode(encoding, errors="xmlcharrefreplace").decode(encoding)


def _write_text(path: Path, data: str, encoding: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe = _make_encoding_safe(data, encoding)
    with path.open("w", encoding=encoding, newline="\n") as f:
        f.write(safe)


def _download_xml(url: str) -> str:
    if requests is None:
        raise RuntimeError("Модуль requests недоступен, не могу скачать XML поставщика")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.text


def _load_categories(path: Path) -> Optional[Set[str]]:
    if not path.is_file():
        return None
    raw = path.read_text(encoding="utf-8", errors="ignore")
    result: Set[str] = set()
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        result.add(s)
    return result or None


def _strip_doctype(xml_text: str) -> str:
    return re.sub(r"<!DOCTYPE[^>]*>", "", xml_text, flags=re.IGNORECASE | re.DOTALL)


def _parse_float(value: str) -> float:
    value = (value or "").strip().replace(" ", "").replace(",", ".")
    if not value:
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def _calc_price(purchase_raw: str, supplier_raw: str) -> int:
    purchase = _parse_float(purchase_raw)
    supplier_price = _parse_float(supplier_raw)

    if purchase <= 0 and supplier_price > 0:
        purchase = supplier_price
    if purchase <= 0:
        return 100
    if purchase >= 9_000_000:
        return 100

    base = 1.04
    add = 0.0

    if purchase < 1000:
        add = 8.0
    elif purchase < 3000:
        add = 3.0
    elif purchase < 5000:
        add = 1.0
    elif purchase < 8000:
        add = 0.55
    elif purchase < 12000:
        add = 0.40
    elif purchase < 20000:
        add = 0.30
    elif purchase < 30000:
        add = 0.22
    elif purchase < 50000:
        add = 0.16
    elif purchase < 80000:
        add = 0.14
    elif purchase < 120000:
        add = 0.13
    elif purchase < 200000:
        add = 0.12
    elif purchase < 300000:
        add = 0.11
    elif purchase < 500000:
        add = 0.10
    elif purchase < 800000:
        add = 0.09
    elif purchase < 1_200_000:
        add = 0.08
    else:
        add = 0.07

    coeff = base + add
    raw_price = purchase * coeff

    candidates = [raw_price, purchase * 1.04]
    if supplier_price > 0:
        candidates.append(supplier_price * 0.9)

    raw_price = max(candidates)

    rounded = math.ceil(raw_price / 1000.0) * 1000 - 100
    if rounded < 100:
        rounded = 100
    return int(rounded)


def _parse_available(value: str) -> bool:
    v = (value or "").strip().lower()
    if v in {"1", "true", "yes", "да"}:
        return True
    if v in {"0", "false", "no", "нет"}:
        return False
    return False


def _collect_params(src_offer: ET.Element) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []

    for param in src_offer.findall("param"):
        name = (param.get("name") or "").strip()
        value = (param.text or "").strip()
        if not name or not value:
            continue
        if name in PARAM_BLACKLIST:
            continue

        key_lower = name.lower()
        if any(p["name"].lower() == key_lower for p in items):
            continue

        items.append({"name": name, "value": value})

    def sort_key(item: Dict[str, str]) -> tuple:
        name = item["name"]
        try:
            idx = PARAM_PRIORITY.index(name)
        except ValueError:
            idx = 10**6
        return (idx, name)

    items.sort(key=sort_key)
    return items


_re_ws = re.compile(r"\s+", re.U)


def _normalize_description_text(text: str) -> str:
    if not text:
        return ""
    lines = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        lines.append(s)
    if not lines:
        return ""
    joined = " ".join(lines)
    joined = _re_ws.sub(" ", joined)
    return joined.strip()


def _build_description_html(name: str, original_desc: str, params_block: List[Dict[str, str]]) -> str:
    parts: List[str] = []
    parts.append("<description>")
    parts.append("")
    parts.append(WHATSAPP_BLOCK.strip("\n"))
    parts.append("")
    parts.append("<!-- Описание -->")

    norm = _normalize_description_text(original_desc)
    if norm:
        parts.append(f"<h3>{name}</h3><p>{norm}</p>")
    else:
        parts.append(f"<h3>{name}</h3>")

    if params_block:
        parts.append("<h3>Характеристики</h3><ul>")
        for item in params_block:
            pname = item["name"]
            pvalue = item["value"]
            parts.append(f"<li><strong>{pname}:</strong> {pvalue}</li>")
        parts.append("</ul>")

    parts.append("")
    parts.append("</description>")
    return "\n".join(parts)


def _build_feed_meta(build_time: dt.datetime, stats: Dict[str, int], next_build: dt.datetime) -> str:
    lines = [
        "<!--FEED_META",
        f"Поставщик                                  | {SUPPLIER_NAME}",
        f"URL поставщика                             | {SUPPLIER_URL}",
        f"Время сборки (Алматы)                      | {build_time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Ближайшая сборка (Алматы)                  | {next_build.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Сколько товаров у поставщика до фильтра    | {stats['total_before']}",
        f"Сколько товаров у поставщика после фильтра | {stats['after_filter']}",
        f"Сколько товаров есть в наличии (true)      | {stats['available_true']}",
        f"Сколько товаров нет в наличии (false)      | {stats['available_false']}",
        "-->",
    ]
    return "\n".join(lines)


def _convert_offer(src_offer: ET.Element, allowed_categories: Optional[Set[str]], stats: Dict[str, int]) -> Optional[str]:
    stats["total_before"] += 1

    def g(tag: str) -> str:
        return (src_offer.findtext(tag) or "").strip()

    category_id = g("categoryId")
    if allowed_categories and category_id and category_id not in allowed_categories:
        return None

    article = (src_offer.get("id") or "").strip()
    vendor_code_raw = g("vendorCode")
    base_code = vendor_code_raw or article
    if not base_code:
        return None

    vendor_code = f"{VENDOR_PREFIX}{base_code}"
    offer_id = vendor_code

    purchase_raw = g("purchase_price")
    supplier_price_raw = g("price")
    price_int = _calc_price(purchase_raw, supplier_price_raw)

    currency_id = g("currencyId") or DEFAULT_CURRENCY

    available_raw = g("available")
    is_avail = _parse_available(available_raw)

    stats["after_filter"] += 1
    if is_avail:
        stats["available_true"] += 1
    else:
        stats["available_false"] += 1

    vendor = g("vendor")
    name = g("name")
    original_desc = g("description")

    pictures: List[str] = []
    for pic in src_offer.findall("picture"):
        url = (pic.text or "").strip()
        if url and url not in pictures:
            pictures.append(url)

    params_block = _collect_params(src_offer)
    desc_html = _build_description_html(name=name, original_desc=original_desc, params_block=params_block)

    lines: List[str] = []
    avail_str = "true" if is_avail else "false"
    lines.append(f'<offer id="{offer_id}" available="{avail_str}">')
    lines.append(f"<categoryId>{category_id}</categoryId>")
    lines.append(f"<vendorCode>{vendor_code}</vendorCode>")
    lines.append(f"<name>{name}</name>")
    lines.append(f"<price>{price_int}</price>")
    for u in pictures:
        lines.append(f"<picture>{u}</picture>")
    if vendor:
        lines.append(f"<vendor>{vendor}</vendor>")
    lines.append(f"<currencyId>{currency_id}</currencyId>")
    lines.append(desc_html)
    for p in params_block:
        lines.append(f'<param name="{p["name"]}">{p["value"]}</param>')
    lines.append("</offer>")

    return "\n".join(lines)


def build_alstyle(source_xml: Optional[Path] = None, output_path: Path = DEFAULT_OUTPUT, categories_path: Path = DEFAULT_CATEGORIES) -> None:
    if source_xml is None:
        xml_text = _download_xml(SUPPLIER_URL)
    else:
        xml_text = _read_text(source_xml, ENCODING_SUPPLIER)

    xml_text = _strip_doctype(xml_text)

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise RuntimeError(f"Ошибка парсинга XML поставщика: {e}") from e

    shop = root.find("shop")
    offers_container = None
    if shop is not None:
        offers_container = shop.find("offers")
    if offers_container is None:
        offers_container = root.find("shop/offers")
    if offers_container is None:
        raise RuntimeError("Не найден блок <offers> в XML поставщика")

    all_offers = list(offers_container.findall("offer"))

    allowed_categories = _load_categories(categories_path)

    stats: Dict[str, int] = {
        "total_before": 0,
        "after_filter": 0,
        "available_true": 0,
        "available_false": 0,
    }

    converted_offers: List[str] = []
    for src_offer in all_offers:
        converted = _convert_offer(src_offer, allowed_categories, stats)
        if converted:
            converted_offers.append(converted)

    build_time = _now_almaty()
    next_build = (build_time + dt.timedelta(days=1)).replace(hour=1, minute=0, second=0, microsecond=0)

    feed_meta = _build_feed_meta(build_time, stats, next_build)

    lines: List[str] = []
    lines.append(f'<?xml version="1.0" encoding="{ENCODING_OUT}"?><!DOCTYPE yml_catalog SYSTEM "shops.dtd">')
    lines.append(f'<yml_catalog date="{build_time.strftime("%Y-%m-%d %H:%M")}">')
    lines.append("<shop><offers>")
    lines.append("")
    lines.append(feed_meta)
    lines.append("")

    for idx, offer_text in enumerate(converted_offers):
        lines.append(offer_text)
        if idx != len(converted_offers) - 1:
            lines.append("")

    lines.append("")
    lines.append("</offers>")
    lines.append("</shop>")
    lines.append("</yml_catalog>")

    final_text = "\n".join(lines)
    _write_text(output_path, final_text, ENCODING_OUT)


def main(argv: Optional[List[str]] = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    source_xml: Optional[Path] = None
    output_path: Path = DEFAULT_OUTPUT
    categories_path: Path = DEFAULT_CATEGORIES

    if len(argv) >= 1 and argv[0]:
        source_xml = Path(argv[0])
    if len(argv) >= 2 and argv[1]:
        output_path = Path(argv[1])
    if len(argv) >= 3 and argv[2]:
        categories_path = Path(argv[2])

    build_alstyle(source_xml=source_xml, output_path=output_path, categories_path=categories_path)


if __name__ == "__main__":
    main()
