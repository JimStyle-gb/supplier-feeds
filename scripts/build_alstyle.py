#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlStyle feed builder v127_desc_enrich_norm_res

Строит docs/alstyle.yml из XML поставщика AlStyle.

Особенности:
- Кодировка вывода windows-1251 с xmlcharrefreplace (без UnicodeEncodeError).
- Вшитый список categoryId (без файла docs/alstyle_categories.txt).
- Экранит спецсимволы в текстах/атрибутах (&, <, >, ").
- Блок WhatsApp взят из эталонного YML (21.11) без изменений.
- Описание:
    * умная обрезка до ~1000 символов по предложениям;
    * вставка <br /> внутри <p> для длинных описаний;
    * вся HTML-часть после <!-- Описание --> в одну строку.
- Блок характеристик и <param> формируются:
    * из тегов <param> поставщика (после фильтра и сортировки);
    * дополняются характеристиками, распознанными из родного описания
      (Производитель, Устройство, Цвет печати, Тип чернил, Совместимость и т.п.);
    * для параметров "Ресурс"/"Ресурс картриджа" оставляем только значения, где есть цифры.
- Значения параметров нормализуются (убираем лишние переносы строк/пробелы),
  чтобы и <param>, и список <li> были в одну строку.
"""

from __future__ import annotations

import sys
import math
import datetime as dt
from pathlib import Path
from typing import Optional, List, Dict, Set
import re
import xml.etree.ElementTree as ET
import html as html_module

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

SUPPLIER_NAME = "AlStyle"
SUPPLIER_URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
ENCODING_SUPPLIER = "utf-8"
ENCODING_OUT = "windows-1251"

DEFAULT_OUTPUT = Path("docs/alstyle.yml")

VENDOR_PREFIX = "AS"
DEFAULT_CURRENCY = "KZT"
TIMEZONE_OFFSET_HOURS = 5  # Алматы

# Параметры, которые не нужны покупателю / SEO
PARAM_BLACKLIST = {
    "артикул",
    "штрихкод",
    "штрих-код",
    "код товара kaspi",
    "код тн вэд",
    "объем",
    "объём",
    "объём, л",
    "объём, мл",
    "объем, л",
    "объем, мл",
    "снижена цена",
    "благотворительность",
    "назначение",
    "новинка",
}

# Приоритет важнейших параметров в блоке характеристик
PARAM_PRIORITY = [
    "Бренд",
    "Производитель",
    "Модель",
    "Серия",
    "Тип",
    "Тип чернил",
    "Технология печати",
    "Устройство",
    "Совместимость",
    "Цвет",
    "Цвет печати",
    "Диагональ экрана",
    "Яркость",
    "Объем картриджа, мл",
    "Объём картриджа, мл",
    "Объем, л",
    "Объем, мл",
    "Ёмкость батареи",
    "Память",
    "Вес",
    "Размеры",
]

# Подсказки для вычленения характеристик из текста описания
DESC_PARAM_HINTS = [
    "Производитель",
    "Устройство",
    "Цвет печати",
    "Тип чернил",
    "Технология печати",
    "Объем картриджа, мл",
    "Объём картриджа, мл",
    "Объем картриджа",
    "Объём картриджа",
    "Совместимость",
    "Ресурс картриджа",
    "Ресурс",
    "Объем, л",
    "Объем, мл",
    "Объём, л",
    "Объём, мл",
]

ALLOWED_CATEGORY_IDS: Set[str] = {
    "3540",
    "3541",
    "3542",
    "3543",
    "3544",
    "3545",
    "3566",
    "3567",
    "3569",
    "3570",
    "3580",
    "3688",
    "3708",
    "3721",
    "3722",
    "4889",
    "4890",
    "4895",
    "5017",
    "5075",
    "5649",
    "5710",
    "5711",
    "5712",
    "5713",
    "21279",
    "21281",
    "21291",
    "21356",
    "21367",
    "21368",
    "21369",
    "21370",
    "21371",
    "21372",
    "21451",
    "21498",
    "21500",
    "21572",
    "21573",
    "21574",
    "21575",
    "21576",
    "21578",
    "21580",
    "21581",
    "21583",
    "21584",
    "21585",
    "21586",
    "21588",
    "21591",
    "21640",
    "21664",
    "21665",
    "21666",
    "21698",
}

WHATSAPP_BLOCK = """<!-- WhatsApp -->
<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;"><p style="text-align:center; margin:0 0 12px;"><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,.08);">&#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!</a></p><div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;"><h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3><ul style="margin:0; padding-left:18px;"><li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li><li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li></ul><hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" /><h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3><ul style="margin:0; padding-left:18px;"><li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li><li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li><li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li><li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li></ul></div></div>
"""


def _now_almaty() -> dt.datetime:
    """Текущее время в Алматы (UTC+5)."""
    return dt.datetime.utcnow() + dt.timedelta(hours=TIMEZONE_OFFSET_HOURS)


def _read_text(path: Path, encoding: str) -> str:
    return path.read_text(encoding=encoding, errors="replace")


def _make_encoding_safe(text: str, encoding: str) -> str:
    """Делает строку безопасной для записи в encoding (xmlcharrefreplace)."""
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
    """Формула наценки с хвостом 900 и страховкой от слишком низкой цены."""
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

    rounded = int(math.ceil(raw_price / 1000.0) * 1000 - 100)
    if rounded < 100:
        rounded = 100
    return rounded


def _parse_available(value: str) -> bool:
    v = (value or "").strip().lower()
    if v in {"1", "true", "yes", "да"}:
        return True
    if v in {"0", "false", "no", "нет"}:
        return False
    return False


def _xml_escape_text(s: str) -> str:
    if not s:
        return ""
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )


def _xml_escape_attr(s: str) -> str:
    if not s:
        return ""
    return (
        s.replace("&", "&amp;")
         .replace('"', "&quot;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )


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


def _plain_from_html(html_text: str) -> str:
    """Преобразует HTML описания в обычный текст: убирает теги, декодирует сущности."""
    if not html_text:
        return ""
    txt = html_module.unescape(html_text)
    txt = re.sub(r"(?i)<br\s*/?>", " ", txt)
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = txt.replace("\u00A0", " ")
    txt = _re_ws.sub(" ", txt)
    return txt.strip()


GOAL = 1000
GOAL_LOW = 900
MAX_HARD = 1200


def _build_desc_text(plain: str) -> str:
    """Умная обрезка plain-текста до ~1000 символов по предложениям."""
    if len(plain) <= GOAL:
        return plain

    parts = re.split(r"(?<=[\.!?])\s+|;\s+", plain)
    parts = [p.strip() for p in parts if p.strip()]
    if not parts:
        return plain[:GOAL]

    selected: List[str] = []
    total = 0

    selected.append(parts[0])
    total = len(parts[0])

    for p in parts[1:]:
        add = (1 if total else 0) + len(p)
        if total + add > MAX_HARD:
            break
        selected.append(p)
        total += add
        if total >= GOAL_LOW:
            break

    if total < GOAL_LOW:
        for p in parts[len(selected):]:
            add = (1 if total else 0) + len(p)
            if total + add > MAX_HARD:
                break
            selected.append(p)
            total += add
            if total >= GOAL_LOW:
                break

    return " ".join(selected).strip()


def _split_for_br(text: str, max_chunk_len: int = 220, max_br: int = 3) -> List[str]:
    """Делит текст на части для <br />, стараясь резать по предложениям."""
    text = text.strip()
    if len(text) <= max_chunk_len:
        return [text]

    parts = re.split(r"(?<=[\.!?])\s+|;\s+", text)
    parts = [p.strip() for p in parts if p.strip()]
    if not parts:
        return [text]

    lines: List[str] = []
    cur = ""

    for s in parts:
        cand = cur + (" " if cur else "") + s
        if cur and len(cand) > max_chunk_len and len(lines) < max_br:
            lines.append(cur)
            cur = s
        else:
            cur = cand

    if cur:
        lines.append(cur)

    if len(lines) > max_br + 1:
        head = lines[:max_br]
        tail = " ".join(lines[max_br:])
        lines = head + [tail]

    return lines


def _make_br_paragraph(text: str) -> str:
    """Формирует <p>...</p> с разумными <br /> внутри."""
    if not text:
        return "<p></p>"
    trimmed = _build_desc_text(text)
    lines = _split_for_br(trimmed, max_chunk_len=220, max_br=3)
    html_lines = [_xml_escape_text(x) for x in lines]
    if len(html_lines) == 1:
        return "<p>" + html_lines[0] + "</p>"
    return "<p>" + "<br />".join(html_lines) + "</p>"


def _normalize_param_value(value: str) -> str:
    """Нормализует значение параметра: убирает лишние пробелы/переносы строк."""
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _sort_params(params: List[Dict[str, str]]) -> List[Dict[str, str]]:
    def _pkey(item: Dict[str, str]) -> tuple:
        name = item["name"]
        try:
            idx = PARAM_PRIORITY.index(name)
        except ValueError:
            idx = 10**6
        return (idx, name.lower())

    return sorted(params, key=_pkey)


def _collect_params_from_xml(src_offer: ET.Element) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []

    for param in src_offer.findall("param"):
        name = (param.get("name") or "").strip()
        value = (param.text or "").strip()
        if not name or not value:
            continue
        key_clean = name.strip().strip(":")
        key_lower = key_clean.lower()
        if key_lower in PARAM_BLACKLIST:
            continue
        if any(p["name"].lower() == key_lower for p in items):
            continue
        norm_val = _normalize_param_value(value)
        if not norm_val:
            continue
        # для параметров "Ресурс"/"Ресурс картриджа" оставляем только значения с цифрами
        if key_lower in {"ресурс", "ресурс картриджа"} and not any(ch.isdigit() for ch in norm_val):
            continue
        items.append({"name": key_clean, "value": norm_val})

    return items


def _extract_params_from_desc(desc_html: str, existing_names_lower: Set[str]) -> List[Dict[str, str]]:
    """Вытаскивает пары ключ-значение из родного описания по словам-подсказкам."""
    plain = _plain_from_html(desc_html)
    if not plain:
        return []
    tokens = plain.split()
    if not tokens:
        return []

    hint_tokens = [(h, h.split()) for h in DESC_PARAM_HINTS]

    extra: List[Dict[str, str]] = []
    n = len(tokens)
    i = 0

    while i < n:
        match_name = None
        match_len = 0

        for name, htoks in hint_tokens:
            L = len(htoks)
            if L == 0 or i + L > n:
                continue
            if tokens[i:i+L] == htoks:
                if L > match_len:
                    match_name = name
                    match_len = L

        if not match_name:
            i += 1
            continue

        key_clean = match_name.strip()
        key_lower = key_clean.lower()
        if key_lower in PARAM_BLACKLIST or key_lower in existing_names_lower:
            i += match_len
            continue

        j = i + match_len
        val_tokens: List[str] = []
        k = j
        while k < n:
            next_match = False
            for name2, htoks2 in hint_tokens:
                L2 = len(htoks2)
                if L2 == 0 or k + L2 > n:
                    continue
                if tokens[k:k+L2] == htoks2:
                    next_match = True
                    break
            if next_match:
                break

            tok = tokens[k]
            val_tokens.append(tok)
            if any(ch in tok for ch in [".", "!", "?"]) and len(val_tokens) > 2:
                k += 1
                break
            k += 1

        raw_value = " ".join(val_tokens).strip(" .,:;")
        norm_value = _normalize_param_value(raw_value)
        # для "Ресурс"/"Ресурс картриджа" сохраняем только, если в значении есть цифры
        if key_lower in {"ресурс", "ресурс картриджа"} and (not norm_value or not any(ch.isdigit() for ch in norm_value)):
            i = k
            continue
        if norm_value:
            extra.append({"name": key_clean, "value": norm_value})
            existing_names_lower.add(key_lower)

        i = k

    return extra


def _build_description_html(name: str, original_desc: str, params_block: List[Dict[str, str]]) -> str:
    parts: List[str] = []
    parts.append("<description>")
    parts.append("")
    parts.append(WHATSAPP_BLOCK.strip("\n"))
    parts.append("")
    parts.append("<!-- Описание -->")

    norm_plain = _normalize_description_text(original_desc)
    if not norm_plain:
        name_html = _xml_escape_text(name)
        tail = "<h3>" + name_html + "</h3>"
    else:
        desc_plain = _plain_from_html(original_desc)
        if not desc_plain:
            desc_plain = norm_plain
        desc_html_p = _make_br_paragraph(desc_plain)
        name_html = _xml_escape_text(name)
        tail = "<h3>" + name_html + "</h3>" + desc_html_p

    if params_block:
        tail += "<h3>Характеристики</h3><ul>"
        for item in params_block:
            pname = _xml_escape_text(item["name"])
            pvalue = _xml_escape_text(item["value"])
            tail += "<li><strong>" + pname + ":</strong> " + pvalue + "</li>"
        tail += "</ul>"

    parts.append(tail)
    parts.append("")
    parts.append("</description>")
    return "\n".join(parts)


def _build_feed_meta(build_time: dt.datetime, stats: Dict[str, int], next_build: dt.datetime) -> str:
    lines = [
        "<!--FEED_META",
        "Поставщик                                  | " + SUPPLIER_NAME,
        "URL поставщика                             | " + SUPPLIER_URL,
        "Время сборки (Алматы)                      | " + build_time.strftime("%Y-%m-%d %H:%M:%S"),
        "Ближайшая сборка (Алматы)                  | " + next_build.strftime("%Y-%m-%d %H:%M:%S"),
        "Сколько товаров у поставщика до фильтра    | " + str(stats["total_before"]),
        "Сколько товаров у поставщика после фильтра | " + str(stats["after_filter"]),
        "Сколько товаров есть в наличии (true)      | " + str(stats["available_true"]),
        "Сколько товаров нет в наличии (false)      | " + str(stats["available_false"]),
        "-->",
    ]
    return "\n".join(lines)


def _convert_offer(src_offer: ET.Element, stats: Dict[str, int]) -> Optional[str]:
    stats["total_before"] += 1

    def g(tag: str) -> str:
        return (src_offer.findtext(tag) or "").strip()

    category_id = g("categoryId")
    if category_id and category_id not in ALLOWED_CATEGORY_IDS:
        return None

    article = (src_offer.get("id") or "").strip()
    vendor_code_raw = g("vendorCode")
    base_code = vendor_code_raw or article
    if not base_code:
        return None

    vendor_code = VENDOR_PREFIX + base_code
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

    params_block = _collect_params_from_xml(src_offer)
    existing_names_lower: Set[str] = set(p["name"].lower() for p in params_block)
    extra_params = _extract_params_from_desc(original_desc, existing_names_lower)
    if extra_params:
        params_block.extend(extra_params)
    if params_block:
        params_block = _sort_params(params_block)

    desc_html = _build_description_html(name=name, original_desc=original_desc, params_block=params_block)

    lines: List[str] = []
    avail_str = "true" if is_avail else "false"
    lines.append('<offer id="' + _xml_escape_attr(offer_id) + '" available="' + avail_str + '">')
    lines.append("<categoryId>" + category_id + "</categoryId>")
    lines.append("<vendorCode>" + _xml_escape_text(vendor_code) + "</vendorCode>")
    lines.append("<name>" + _xml_escape_text(name) + "</name>")
    lines.append("<price>" + str(price_int) + "</price>")
    for u in pictures:
        lines.append("<picture>" + _xml_escape_text(u) + "</picture>")
    if vendor:
        lines.append("<vendor>" + _xml_escape_text(vendor) + "</vendor>")
    lines.append("<currencyId>" + currency_id + "</currencyId>")
    lines.append(desc_html)
    for p in params_block:
        pname_attr = _xml_escape_attr(p["name"])
        pvalue_text = _xml_escape_text(p["value"])
        lines.append('<param name="' + pname_attr + '">' + pvalue_text + "</param>")
    lines.append("</offer>")

    return "\n".join(lines)


def build_alstyle(source_xml: Optional[Path] = None, output_path: Path = DEFAULT_OUTPUT) -> None:
    if source_xml is None:
        xml_text = _download_xml(SUPPLIER_URL)
    else:
        xml_text = _read_text(source_xml, ENCODING_SUPPLIER)

    xml_text = _strip_doctype(xml_text)

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise RuntimeError("Ошибка парсинга XML поставщика: %s" % e) from e

    shop = root.find("shop")
    offers_container = None
    if shop is not None:
        offers_container = shop.find("offers")
    if offers_container is None:
        offers_container = root.find("shop/offers")
    if offers_container is None:
        raise RuntimeError("Не найден блок <offers> в XML поставщика")

    all_offers = list(offers_container.findall("offer"))

    stats: Dict[str, int] = {
        "total_before": 0,
        "after_filter": 0,
        "available_true": 0,
        "available_false": 0,
    }

    converted_offers: List[str] = []
    for src_offer in all_offers:
        converted = _convert_offer(src_offer, stats)
        if converted:
            converted_offers.append(converted)

    build_time = _now_almaty()
    next_build = (build_time + dt.timedelta(days=1)).replace(hour=1, minute=0, second=0, microsecond=0)

    feed_meta = _build_feed_meta(build_time, stats, next_build)

    lines: List[str] = []
    lines.append('<?xml version="1.0" encoding="' + ENCODING_OUT + '"?><!DOCTYPE yml_catalog SYSTEM "shops.dtd">')
    lines.append('<yml_catalog date="' + build_time.strftime("%Y-%m-%d %H:%M") + '">')
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

    if len(argv) >= 1 and argv[0]:
        source_xml = Path(argv[0])
    if len(argv) >= 2 and argv[1]:
        output_path = Path(argv[1])

    build_alstyle(source_xml=source_xml, output_path=output_path)


if __name__ == "__main__":
    main()
