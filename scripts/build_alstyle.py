#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_alstyle_120_simplified.py
Переписанный с нуля пайплайн для AlStyle.
"""

from __future__ import annotations

import os
import sys
import math
import datetime as dt
from pathlib import Path
import xml.etree.ElementTree as ET

# Константы пайплайна
SUPPLIER_URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
OUTPUT_REL_PATH = Path("docs") / "alstyle.yml"
CATEGORIES_REL_PATH = Path("docs") / "alstyle_categories.txt"
ENCODING_OUT = "windows-1251"
VENDOR_PREFIX = "AS"

# Готовый HTML-блок WhatsApp / оплата / доставка
WHATSAPP_BLOCK = """
<!-- WhatsApp -->
<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;"><p style="text-align:center; margin:0 0 12px;"><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,.08);">&#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!</a></p><div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;"><h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3><ul style="margin:0; padding-left:18px;"><li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li><li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li></ul><hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" /><h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3><ul style="margin:0; padding-left:18px;"><li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li><li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li><li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li><li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li></ul></div></div>
"""

# Параметры, которые точно не нужны покупателю
PARAM_BLACKLIST = {
    "Артикул",
    "Штрихкод",
    "Благотворительность",
    "Код ТН ВЭД",
    "Код товара Kaspi",
    "Объём",
    "Снижена цена",
}

# Приоритет сортировки характеристик (сначала важные, потом остальные по алфавиту)
PARAM_PRIORITY = [
    "Бренд",
    "Производитель",
    "Серия",
    "Модель",
    "Тип",
    "Назначение",
    "Совместимость",
    "Цвет",
    "Формат печати",
    "Ресурс",
    "Ёмкость батареи",
    "Мощность (Bт)",
    "Интерфейсы",
    "Разъёмы",
    "Диагональ экрана",
    "Разрешение",
]


class Stats:
    """Простая статистика для FEED_META."""

    def __init__(self) -> None:
        self.total_before = 0
        self.total_after = 0
        self.available_true = 0
        self.available_false = 0

    def mark_after(self, available: bool) -> None:
        self.total_after += 1
        if available:
            self.available_true += 1
        else:
            self.available_false += 1


def _now_almaty() -> dt.datetime:
    """Возвращает текущее время в Алматы (UTC+5)."""
    return dt.datetime.utcnow() + dt.timedelta(hours=5)


def _read_text(path: Path, encoding: str = "utf-8") -> str:
    with path.open("r", encoding=encoding) as f:
        return f.read()


def _write_text(path: Path, data: str, encoding: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=encoding, newline="\n") as f:
        f.write(data)


def _strip_doctype(xml_text: str) -> str:
    """Убираем <!DOCTYPE ...>, чтобы ET не ругался."""
    return xml_text.replace('<!DOCTYPE yml_catalog SYSTEM "shops.dtd">', "")


def _parse_supplier_xml(xml_text: str) -> ET.Element:
    xml_clean = _strip_doctype(xml_text)
    root = ET.fromstring(xml_clean)
    if root.tag != "yml_catalog":
        raise RuntimeError("Ожидался корень <yml_catalog>")
    return root


def _load_categories(path: Path) -> set[str]:
    """Читаем список categoryId (один id на строку)."""
    if not path.exists():
        # Если файла нет — не фильтруем по категориям
        return set()
    allowed: set[str] = set()
    for line in _read_text(path).splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        allowed.add(line)
    return allowed


def _get_child_text(elem: ET.Element, tag: str) -> str | None:
    child = elem.find(tag)
    if child is None or child.text is None:
        return None
    text = child.text.strip()
    return text or None


def _bool_from_text(text: str | None) -> bool:
    if text is None:
        return False
    return text.strip().lower() in {"1", "true", "yes", "y"}


def _calc_price(purchase: float, supplier_price: float) -> int:
    """Расчёт новой цены: базовая наценка + ступени + окончание 900."""
    if purchase >= 9_000_000 or purchase <= 0:
        return 100

    # Базовый коэффициент (4%)
    coeff = 1.04

    # Дополнительные ступени по закупочной цене
    if purchase < 1_000:
        coeff += 8.0
    elif purchase < 3_000:
        coeff += 3.0
    elif purchase < 5_000:
        coeff += 1.0
    elif purchase < 8_000:
        coeff += 0.55
    elif purchase < 12_000:
        coeff += 0.4
    elif purchase < 20_000:
        coeff += 0.3
    elif purchase < 30_000:
        coeff += 0.22
    elif purchase < 50_000:
        coeff += 0.16
    elif purchase < 80_000:
        coeff += 0.14
    elif purchase < 120_000:
        coeff += 0.13
    elif purchase < 200_000:
        coeff += 0.12
    elif purchase < 300_000:
        coeff += 0.11
    elif purchase < 500_000:
        coeff += 0.10
    elif purchase < 800_000:
        coeff += 0.09
    elif purchase < 1_200_000:
        coeff += 0.08
    else:
        coeff += 0.07

    raw = purchase * coeff

    # Подстраховка: не ниже закупочной и не ниже цены поставщика * 0.9
    raw = max(raw, purchase * 1.04, supplier_price * 0.9)

    # Хвост 900: округляем вверх до тысячи и отнимаем 100
    rounded = int(math.ceil(raw / 1000.0) * 1000 - 100)
    return max(100, rounded)


def _param_sort_key(name: str) -> tuple[int, str]:
    try:
        idx = PARAM_PRIORITY.index(name)
    except ValueError:
        idx = len(PARAM_PRIORITY)
    return (idx, name)


def _collect_params(offer: ET.Element) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for p in offer.findall("param"):
        name = (p.get("name") or "").strip()
        if not name:
            continue
        if name in PARAM_BLACKLIST:
            continue
        value = (p.text or "").strip()
        if not value:
            continue
        result.append((name, value))

    # Удаляем дубликаты (по name, оставляем первое значение)
    seen: set[str] = set()
    unique: list[tuple[str, str]] = []
    for name, value in result:
        if name in seen:
            continue
        seen.add(name)
        unique.append((name, value))

    unique.sort(key=lambda nv: _param_sort_key(nv[0]))
    return unique


def _normalize_description_text(text: str | None) -> str:
    if not text:
        return ""
    # Сглаживаем лишние пробелы и переносы
    lines = [line.strip() for line in text.splitlines()]
    compact = " ".join(ln for ln in lines if ln)
    return compact


def _build_description(name: str, raw_desc: str | None, params: list[tuple[str, str]]) -> str:
    """Формируем `<description>`: WhatsApp + Описание + Характеристики."""
    desc_text = _normalize_description_text(raw_desc)

    parts: list[str] = []
    parts.append("<description>")
    parts.append("")  # пустая строка после <description>

    # Блок WhatsApp / оплата / доставка — как готовый HTML
    if WHATSAPP_BLOCK:
        parts.append(WHATSAPP_BLOCK)
        parts.append("")  # пустая строка

    # Блок описания
    parts.append("<!-- Описание -->")
    if desc_text:
        parts.append(f"<h3>{name}</h3><p>{desc_text}</p>")
    else:
        parts.append(f"<h3>{name}</h3>")

    # Блок характеристик
    if params:
        parts.append("<h3>Характеристики</h3><ul>")
        for pname, val in params:
            parts.append(f"<li><strong>{pname}:</strong> {val}</li>")
        parts.append("</ul>")

    parts.append("")  # пустая строка перед </description>
    parts.append("</description>")
    return "\n".join(parts)


def _transform_offer(offer: ET.Element, allowed_categories: set[str], stats: Stats) -> str | None:
    stats.total_before += 1

    category_id = _get_child_text(offer, "categoryId") or "0"
    if allowed_categories and category_id not in allowed_categories:
        return None

    vendor_raw = _get_child_text(offer, "vendorCode") or _get_child_text(offer, "id") or "0"
    vendor_raw = vendor_raw.strip()
    vendor_code = f"{VENDOR_PREFIX}{vendor_raw}"

    name = _get_child_text(offer, "name") or vendor_code

    available_tag = _bool_from_text(_get_child_text(offer, "available"))
    available = available_tag

    price_supplier = float(_get_child_text(offer, "price") or "0")
    purchase = float(_get_child_text(offer, "purchase_price") or "0")
    new_price = _calc_price(purchase, price_supplier)

    # Картинки
    pictures = [
        (p.text or "").strip()
        for p in offer.findall("picture")
        if (p.text or "").strip()
    ]

    vendor = _get_child_text(offer, "vendor") or ""
    currency = _get_child_text(offer, "currencyId") or "KZT"

    # Характеристики
    params = _collect_params(offer)

    # Описание
    raw_desc = _get_child_text(offer, "description")
    description_html = _build_description(name, raw_desc, params)

    stats.mark_after(available)

    lines: list[str] = []
    lines.append(f'<offer id="{vendor_code}" available="{"true" if available else "false"}">')
    lines.append(f"<categoryId>{category_id}</categoryId>")
    lines.append(f"<vendorCode>{vendor_code}</vendorCode>")
    lines.append(f"<name>{name}</name>")
    lines.append(f"<price>{int(new_price)}</price>")
    for url in pictures:
        lines.append(f"<picture>{url}</picture>")
    if vendor:
        lines.append(f"<vendor>{vendor}</vendor>")
    lines.append(f"<currencyId>{currency}</currencyId>")
    lines.append(description_html)
    for pname, val in params:
        lines.append(f'<param name="{pname}">{val}</param>')
    lines.append("</offer>")
    return "\n".join(lines)


def _build_feed_meta(stats: Stats, total_before: int, now_local: dt.datetime) -> str:
    """Строим блок FEED_META (как многострочный комментарий)."""
    next_build = (now_local + dt.timedelta(days=1)).replace(
        hour=1, minute=0, second=0, microsecond=0
    )

    lines = [
        "<!--FEED_META",
        f"Поставщик                                  | AlStyle",
        f"URL поставщика                             | {SUPPLIER_URL}",
        f"Время сборки (Алматы)                      | {now_local.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Ближайшая сборка (Алматы)                  | {next_build.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Сколько товаров у поставщика до фильтра    | {total_before}",
        f"Сколько товаров у поставщика после фильтра | {stats.total_after}",
        f"Сколько товаров есть в наличии (true)      | {stats.available_true}",
        f"Сколько товаров нет в наличии (false)      | {stats.available_false}",
        "-->",
        "",
    ]
    return "\n".join(lines)


def build_alstyle(
    source_xml: str,
    output_path: Path,
    categories_path: Path,
) -> None:
    """Главная функция: принимает текст XML и пишет готовый alstyle.yml."""
    root = _parse_supplier_xml(source_xml)
    shop = root.find("shop")
    if shop is None:
        raise RuntimeError("Не найден тег <shop> у поставщика")

    offers_parent = shop.find("offers")
    if offers_parent is None:
        raise RuntimeError("Не найден блок <offers> у поставщика")

    offers_src = list(offers_parent.findall("offer"))

    allowed_categories = _load_categories(categories_path)

    stats = Stats()
    offer_blocks: list[str] = []

    for off in offers_src:
        block = _transform_offer(off, allowed_categories, stats)
        if block is None:
            continue
        offer_blocks.append(block)

    now_local = _now_almaty()
    yml_date = now_local.strftime("%Y-%m-%d %H:%M")

    feed_meta = _build_feed_meta(stats, total_before=len(offers_src), now_local=now_local)

    parts: list[str] = []
    parts.append('<?xml version="1.0" encoding="windows-1251"?><!DOCTYPE yml_catalog SYSTEM "shops.dtd">')
    parts.append(f'<yml_catalog date="{yml_date}">')
    parts.append("<shop><offers>")
    parts.append("")  # пустая строка
    parts.append(feed_meta)

    for block in offer_blocks:
        parts.append(block)
        parts.append("")  # пустая строка между офферами

    parts.append("</offers>")
    parts.append("</shop>")
    parts.append("</yml_catalog>")
    final_text = "\n".join(parts) + "\n"

    _write_text(output_path, final_text, ENCODING_OUT)


def _download_source_from_url(url: str) -> str:
    """Простой загрузчик XML по URL (можно выключить в GitHub Actions)."""
    try:
        import requests  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Модуль requests не установлен, не могу скачать XML") from exc

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


def main(argv: list[str] | None = None) -> None:
    """CLI: python scripts/build_alstyle_120_simplified.py [source.xml]."""
    argv = list(sys.argv[1:] if argv is None else argv)

    repo_root = Path(__file__).resolve().parents[1]
    output_path = repo_root / OUTPUT_REL_PATH
    categories_path = repo_root / CATEGORIES_REL_PATH

    if argv:
        # Если указан путь до файла — читаем его.
        source_path = Path(argv[0])
        source_xml = _read_text(source_path, encoding="utf-8")
    else:
        # Иначе пробуем скачать с сайта поставщика.
        source_xml = _download_source_from_url(SUPPLIER_URL)

    build_alstyle(source_xml=source_xml, output_path=output_path, categories_path=categories_path)


if __name__ == "__main__":  # pragma: no cover
    main()
