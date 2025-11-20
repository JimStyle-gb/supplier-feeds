#!/usr/bin/env python3
"""Простой сборщик YML для поставщика Akcent (v2)."""

from __future__ import annotations

import html
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


SUPPLIER_URL = os.getenv(
    "AKCENT_SOURCE_URL",
    "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml",
)

OUTPUT_PATH = os.getenv("AKCENT_OUTPUT_PATH", "docs/akcent.yml")

# Разрешённые префиксы в начале <name>
_ALLOWED_PREFIXES = [
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

# Простейшая карта переименования параметров
PARAM_TITLE_MAP: dict[str, str] = {
    "Разрешение печати,dpi": "Разрешение печати, dpi",
    "Разрешение сканера,dpi": "Разрешение сканера, dpi",
    "Уровень шума (норм./эконом.) Дб": "Уровень шума (норм./эконом.), дБ",
    "Яркость (ANSI) лмн": "Яркость (ANSI), лм",
    "Проекционный коэффицент (Throw ratio)": "Проекционный коэффициент (throw ratio)",
}

# Служебные бренды, которые не хотим видеть как vendor
_BRAND_BLOCKLIST = (
    "alstyle",
    "al-style",
    "copyline",
    "vtt",
    "akcent",
    "ak-cent",
    "китай",
)

# Бренды, которые чаще всего встречаются у Akcent — используем для подстановки vendor,
# если в исходном фиде он пустой.
_KNOWN_BRANDS = (
    "Epson",
    "Fellowes",
    "HyperX",
    "Mr.Pixel",
    "Philips",
    "SBID",
    "Smart",
    "ViewSonic",
    "Vivitek",
    "Zebra",
)

# Фиксированный блок WhatsApp + доставка/оплата (одна строка)
WHATSAPP_BLOCK = (
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; "
    "line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; "
    "padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,.08);\">"
    "💬 НАПИСАТЬ В WHATSAPP</a></p>"
    "<ul style='margin:0; padding:0 0 0 18px;'>"
    "<li>Оплата: наличными, картой, переводом, по счету для юр. лиц</li>"
    "<li>Доставка по Алматы: курьером до двери</li>"
    "<li>Доставка по Казахстану: транспортными компаниями и почтой</li>"
    "</ul></div>"
)
@dataclass
class OfferData:
    id: str
    available: str
    category_id: str
    vendor_code: str
    name: str
    price: int
    pictures: list[str]
    vendor: str
    description_html: str
    params: list[tuple[str, str]]


def _decode_bytes(data: bytes) -> str:
    """Попробовать угадать кодировку."""
    # сначала UTF-8 с BOM/без, потом windows-1251
    for enc in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def _name_allowed(name: str) -> bool:
    """Фильтр по началу name."""
    n = name.strip()
    for prefix in _ALLOWED_PREFIXES:
        if n.startswith(prefix):
            return True
    return False


def _normalize_brand_name(raw: str) -> str:
    """Немного привести бренд к виду для vendor/Производитель."""
    t = raw.strip()
    if not t:
        return ""

    low = t.lower()
    for bad in _BRAND_BLOCKLIST:
        if low == bad:
            return ""

    # Убираем типичные хвосты
    t = re.sub(r"\s*proj$", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*projector$", "", t, flags=re.IGNORECASE)
    return t.strip()


def _apply_price_rules(raw_price: int) -> int:
    """Применить наценку 4% + фиксированный диапазон и хвост 900.

    Если итоговая цена >= 9 000 000 — вернуть 100.
    """
    base = int(raw_price)
    if base <= 0:
        return base

    tiers = [
        (101, 10_000, 3_000),
        (10_001, 25_000, 4_000),
        (25_001, 50_000, 5_000),
        (50_001, 75_000, 7_000),
        (75_001, 100_000, 10_000),
        (100_001, 150_000, 12_000),
        (150_001, 200_000, 15_000),
        (200_001, 300_000, 20_000),
        (300_001, 400_000, 25_000),
        (400_001, 500_000, 30_000),
        (500_001, 750_000, 40_000),
        (750_001, 1_000_000, 50_000),
        (1_000_001, 1_500_000, 70_000),
        (1_500_001, 2_000_000, 90_000),
        (2_000_001, 100_000_000, 100_000),
    ]

    bonus = 0
    for lo, hi, add in tiers:
        if lo <= base <= hi:
            bonus = add
            break

    if bonus == 0:
        return base

    # 4% + фиксированный бонус
    value = base * 1.04 + bonus

    # Хвост 900 + округление вверх
    thousands = int(value) // 1000
    price = thousands * 1000 + 900
    if price < value:
        price += 1000

    # Если стало слишком дорого — ставим 100
    if price >= 9_000_000:
        return 100

    return price

def _extract_params(block: str) -> tuple[list[tuple[str, str]], list[str]]:
    """Достать пары (name, value) из Param и список сопутствующих устройств."""
    params: list[tuple[str, str]] = []
    compat: list[str] = []

    for m in re.finditer(r'<Param\s+name="([^"]*)">(.*?)</Param>', block, flags=re.DOTALL | re.IGNORECASE):
        name = html.unescape(m.group(1) or "").strip()
        value = html.unescape(m.group(2) or "").strip()

        if not name:
            continue

        # Сопутствующие товары — только в совместимые устройства, из Param убираем
        if name == "Сопутствующие товары":
            v = value.strip()
            if v and v.lower() not in {"нет", "none", "n/a"}:
                compat.append(v)
            continue

        # Явный мусор/служебные параметры — полностью выкидываем
        if name in {
            "Наименование производителя",
            "Совместимые продукты",
            "Оригинальное разрешение",
        }:
            continue

        # Нормализуем производителя, чтобы не тянуть «китай», хвосты и т.п.
        if name == "Производитель":
            norm_val = _normalize_brand_name(value)
            if not norm_val:
                continue
            value = norm_val

        # Параметры типа "Тип", "Вид", "Для бренда" не несут пользы для фильтров Сату — пропускаем
        if name in {"Тип", "Вид", "Для бренда"}:
            continue

        # Немного чистки заголовков
        norm_name = PARAM_TITLE_MAP.get(name, name)
        params.append((norm_name, value))

    return params, compat

def _build_description(name: str, raw_desc: str, params: list[tuple[str, str]], compat: list[str]) -> str:
    """Собрать HTML <description>."""
    name_html = html.escape(name.strip())
    desc_text = (raw_desc or "").strip()
    desc_text = html.unescape(desc_text)
    desc_text = re.sub(r"\s+", " ", desc_text)

    if not desc_text:
        desc_text = f"{name_html} — качественное решение для повседневной работы и учебы."

    # Ограничим длину описания, чтобы не раздувать карточку
    max_len = 900
    if len(desc_text) > max_len:
        cut = desc_text.rfind(".", 0, max_len)
        if cut == -1:
            cut = max_len
        desc_text = desc_text[:cut].rstrip()

    inner: list[str] = []

    inner.append("")
    inner.append("<!-- WhatsApp -->")
    inner.append(WHATSAPP_BLOCK)
    inner.append("")
    inner.append("<!-- Описание -->")
    inner.append(f"<h3>{name_html}</h3><p>{html.escape(desc_text)}</p>")

    # Характеристики
    if params:
        inner.append("<h3>Характеристики</h3>")
        li: list[str] = []
        for pname, pvalue in params:
            if not pvalue.strip():
                continue
            li.append(f"<li><strong>{html.escape(pname)}:</strong> {html.escape(pvalue)}</li>")
        if li:
            inner.append("<ul>" + "".join(li) + "</ul>")

    # Совместимые устройства
    if compat:
        inner.append("<h3>Совместимые устройства</h3>")
        li2 = [f"<li>{html.escape(v)}</li>" for v in compat[:10]]
        inner.append("<ul>" + "".join(li2) + "</ul>")

    # Оборачиваем переносами, как у alstyle/akcent
    html_block = "\n".join(inner)
    return f"\n\n{html_block}\n\n"


def _guess_brand(name: str, raw_desc: str, body: str) -> str:
    """Попробовать угадать бренд по Param/имени/описанию."""
    # 1) Явные параметры про производителя
    for pattern in (
        r'<Param\s+name="Производитель">(.*?)</Param>',
        r'<Param\s+name="Наименование производителя">(.*?)</Param>',
        r'<Param\s+name="Для бренда">(.*?)</Param>',
    ):
        m = re.search(pattern, body, flags=re.DOTALL | re.IGNORECASE)
        if m:
            brand = _normalize_brand_name(m.group(1))
            if brand:
                return brand

    # 2) Пытаемся найти бренд по известным названиям в name/description
    text = f"{name} {html.unescape(raw_desc or '')}"
    low = text.lower()
    for b in _KNOWN_BRANDS:
        if b.lower() in low:
            return b

    # Частный случай для интерактивных панелей SBID-...
    if "SBID-" in text:
        return "SBID"

    return ""

def _parse_offer(block: str) -> OfferData | None:
    """Разобрать один исходный <offer> в структуру OfferData или вернуть None, если выкидываем."""
    # Заголовок offer
    m_head = re.match(r"<offer\b([^>]*)>(.*)</offer>", block, flags=re.DOTALL | re.IGNORECASE)
    if not m_head:
        return None

    header_attrs = m_head.group(1)
    body = m_head.group(2)

    # name
    m_name = re.search(r"<name>(.*?)</name>", body, flags=re.DOTALL | re.IGNORECASE)
    name = html.unescape(m_name.group(1).strip()) if m_name else ""
    if not name:
        return None

    if not _name_allowed(name):
        return None

    # article / старый id
    m_article = re.search(r'\barticle="([^"]*)"', header_attrs)
    article = (m_article.group(1).strip() if m_article else "")

    if not article:
        m_old_id = re.search(r'\bid="([^"]*)"', header_attrs)
        if m_old_id:
            article = m_old_id.group(1).strip()

    if not article:
        return None

    new_id = "AK" + article

    # available
    m_av = re.search(r'\bavailable="([^"]*)"', header_attrs)
    available = (m_av.group(1).strip().lower() if m_av else "true")
    available = "true" if available in {"true", "1", "yes"} else "false"

    # categoryId
    m_cat = re.search(r"<categoryId[^>]*>(.*?)</categoryId>", body, flags=re.DOTALL | re.IGNORECASE)
    cat_id = html.unescape(m_cat.group(1).strip()) if m_cat else ""

    # raw description (понадобится и для vendor, и для финального <description>)
    m_desc = re.search(r"<description>(.*?)</description>", body, flags=re.DOTALL | re.IGNORECASE)
    raw_desc = html.unescape(m_desc.group(1)) if m_desc else ""

    # vendor: сначала берём из тега, затем, если пусто/мусор, пробуем угадать по Param/имени/описанию
    m_vendor = re.search(r"<vendor>(.*?)</vendor>", body, flags=re.DOTALL | re.IGNORECASE)
    vendor = html.unescape(m_vendor.group(1).strip()) if m_vendor else ""
    vendor = _normalize_brand_name(vendor)
    if not vendor:
        vendor = _guess_brand(name, raw_desc, body)

    # картинки
    pictures: list[str] = []
    for m in re.finditer(r"<picture>(.*?)</picture>", body, flags=re.DOTALL | re.IGNORECASE):
        url = html.unescape(m.group(1).strip())
        if url:
            pictures.append(url)

    # цена: берём "Цена дилерского портала KZT"
    raw_price_val = None
    m_price = re.search(
        r'<price[^>]*type="Цена дилерского портала KZT"[^>]*>(.*?)</price>',
        body,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if m_price:
        value = re.sub(r"\s", "", m_price.group(1))
        if value.isdigit():
            raw_price_val = int(value)

    if raw_price_val is None or raw_price_val <= 0:
        # Без закупочной цены смысла в оффере нет
        return None

    price = _apply_price_rules(raw_price_val)

    # Параметры и сопутствующие товары
    params, compat = _extract_params(body)

    # Описание
    desc_html = _build_description(name, raw_desc, params, compat)

    return OfferData(
        id=new_id,
        available=available,
        category_id=cat_id,
        vendor_code=new_id,
        name=name,
        price=price,
        pictures=pictures,
        vendor=vendor,
        description_html=desc_html,
        params=params,
    )

def _download_raw_text() -> str:
    """Скачать исходный XML от поставщика."""
    print(f"[akcent] Скачиваем фид: {SUPPLIER_URL}")
    resp = requests.get(SUPPLIER_URL, timeout=60)
    resp.raise_for_status()
    text = _decode_bytes(resp.content)
    return text


def _build_yml(offers: list[OfferData], total_raw: int) -> str:
    """Собрать финальный YML как строку."""
    # Время по Алматы (UTC+5)
    tz_almaty = timezone(timedelta(hours=5))
    now = datetime.now(tz=tz_almaty)
    today_str = now.strftime("%Y-%m-%d %H:%M")
    meta_now = now.strftime("%Y-%m-%d %H:%M:%S")

    # Следующая сборка в 01:00 завтрашнего дня
    next_run = (now + timedelta(days=1)).replace(hour=1, minute=0, second=0, microsecond=0)
    meta_next = next_run.strftime("%Y-%m-%d %H:%M:%S")

    total_filtered = len(offers)
    avail_true = sum(1 for o in offers if o.available == "true")
    avail_false = total_filtered - avail_true

    header_lines = [
        '<?xml version="1.0" encoding="windows-1251"?>',
        '<!DOCTYPE yml_catalog SYSTEM "shops.dtd">',
        f'<yml_catalog date="{today_str}">',
        "<shop><offers>",
        "",
        "<!--FEED_META",
        "Поставщик                                  | AkCent",
        f"URL поставщика                             | {SUPPLIER_URL}",
        f"Время сборки (Алматы)                      | {meta_now}",
        f"Ближайшая сборка (Алматы)                  | {meta_next}",
        f"Сколько товаров у поставщика до фильтра    | {total_raw}",
        f"Сколько товаров у поставщика после фильтра | {total_filtered}",
        f"Сколько товаров есть в наличии (true)      | {avail_true}",
        f"Сколько товаров нет в наличии (false)      | {avail_false}",
        "-->",
        "",
    ]

    parts: list[str] = []

    for off in offers:
        lines: list[str] = []
        lines.append(f'<offer id="{off.id}" available="{off.available}">')
        lines.append(f"<categoryId>{html.escape(off.category_id)}</categoryId>")
        lines.append(f"<vendorCode>{html.escape(off.vendor_code)}</vendorCode>")
        lines.append(f"<name>{html.escape(off.name)}</name>")
        lines.append(f"<price>{off.price}</price>")
        for pic in off.pictures:
            lines.append(f"<picture>{html.escape(pic)}</picture>")
        if off.vendor:
            lines.append(f"<vendor>{html.escape(off.vendor)}</vendor>")
        lines.append("<currencyId>KZT</currencyId>")
        lines.append("<description>")
        lines.append(off.description_html)
        lines.append("</description>")
        for pname, pvalue in off.params:
            lines.append(f'<param name="{html.escape(pname)}">{html.escape(pvalue)}</param>')
        lines.append("</offer>")
        parts.append("\n".join(lines))

    body = "\n\n".join(parts)

    footer_lines = [
        "",
        "</offers></shop>",
        "</yml_catalog>",
    ]

    full = "\n".join(header_lines) + "\n" + body + "\n" + "\n".join(footer_lines)
    return full


def build_akcent_yml(output_path: str | Path = OUTPUT_PATH) -> None:
    """Главная точка входа: скачать, пересобрать, сохранить."""
    raw_text = _download_raw_text()

    # Находим все исходные <offer>...</offer>
    blocks = re.findall(r"<offer\b[^>]*>.*?</offer>", raw_text, flags=re.DOTALL | re.IGNORECASE)
    total_raw = len(blocks)
    print(f"[akcent] Найдено офферов у поставщика: {total_raw}")

    offers: list[OfferData] = []

    for block in blocks:
        data = _parse_offer(block)
        if data is None:
            continue
        offers.append(data)

    print(f"[akcent] В фид попало офферов: {len(offers)}")

    yml_text = _build_yml(offers, total_raw)

    # Записываем в Windows-1251
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_bytes = yml_text.encode("cp1251", errors="ignore")
    out_path.write_bytes(out_bytes)
    print(f"[akcent] Готовый YML сохранён в {out_path}")


def main(argv: list[str] | None = None) -> int:
    _ = argv or sys.argv[1:]
    try:
        build_akcent_yml()
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"[akcent] Ошибка: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
