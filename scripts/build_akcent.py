#!/usr/bin/env python3
"""Сборщик YML для поставщика Akcent.

Логика пайплайна:
1. Скачиваем исходный XML/YML файл поставщика.
2. Вырезаем всё содержимое между <shop> и <offers>, оставляя сами теги.
3. Оставляем только те <offer>, у которых <name> начинается с наших ключевых слов.
4. Удаляем служебные теги (url, url/ , Offer_ID, delivery, local_delivery_cost, model,
   manufacturer_warranty, Stock, prices/RRP).
5. Приводим каждый <offer> к нужному виду:
   - в <offer> оставляем только атрибуты id и available;
   - id формируем как "AK" + article (или старый id, если article пустой);
   - внутри создаём <vendorCode> с тем же значением, что и id;
   - <categoryId type="..."> превращаем в <categoryId>значение</categoryId>,
     при отсутствии значения делаем <categoryId></categoryId>;
   - в каждом оффере добавляем <currencyId>KZT</currencyId>;
   - если <vendor/> пустой или служебный, пытаемся найти бренд в Param/name/description;
   - цену берём из <price type="Цена дилерского портала KZT" ...>, пересчитываем
     по правилам (4% + диапазон, хвост 900, >= 9 000 000 -> 100) и записываем
     как <price>XXX</price> без атрибутов;
   - все Param name="Сопутствующие товары" убираем из характеристик и в конец
     description добавляем текстовый блок
     "Сопутствующие товары и совместимые устройства:" со списком;
   - выкидываем из Param мусорные:
       * Наименование производителя
       * Оригинальное разрешение
       * Сопутствующие товары
       * Совместимые продукты.
6. Нормализуем разметку: убираем лишние отступы и пустые строки внутри <offer>,
   аккуратно расставляем разрывы:
   <shop><offers>\n\n<offer ...>\n<categoryId>...\n...\n</offer>\n\n</offers>
7. Сохраняем результат в docs/akcent.yml (UTF-8).
"""

from __future__ import annotations

import html
import os
import re
import sys
from pathlib import Path

import requests


WHATSAPP_BLOCK = """<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;">
  <p style="text-align:center; margin:0 0 12px;">
    <a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0"
       style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,.08);">
      💬 НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!
    </a>
  </p>

  <div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;">
    <h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3>
    <ul style="margin:0; padding-left:18px;">
      <li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li>
      <li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li>
    </ul>

    <hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" />

    <h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3>
    <ul style="margin:0; padding-left:18px;">
      <li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li>
      <li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li>
      <li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>
      <li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li>
    </ul>
  </div>
</div>"""


# Ключевые префиксы для начала тега <name>
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

_ALLOWED_PREFIXES_UPPER = [p.upper() for p in _ALLOWED_PREFIXES]

# Не допускаем, чтобы эти значения попадали в <vendor>
_BRAND_BLOCKLIST = (
    "alstyle",
    "al-style",
    "copyline",
    "vtt",
    "akcent",
    "ak-cent",
)

# Подборка типичных брендов в этой номенклатуре
_KNOWN_BRANDS = [
    "Epson",
    "Philips",
    "Fellowes",
    "Brother",
    "Canon",
    "HP",
    "Kyocera",
    "Ricoh",
    "Sharp",
    "Panasonic",
    "BenQ",
    "ViewSonic",
    "AOC",
    "Dell",
    "Lenovo",
    "Asus",
    "Acer",
    "Samsung",
    "Logitech",
    "Poly",
    "Defender",
    "OKI",
    "Xerox",
    "Lexmark",
    "Vivitek",  # важно для DX273
]


def _decode_bytes(raw: bytes) -> str:
    """Аккуратно декодировать байты в строку (UTF-8 / CP1251)."""
    for enc in ("utf-8", "cp1251"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _strip_shop_header(text: str) -> str:
    """Удалить всё между <shop> и <offers>, оставив сами теги."""
    shop_tag = "<shop>"
    offers_tag = "<offers>"

    idx_shop = text.find(shop_tag)
    if idx_shop == -1:
        return text

    idx_offers = text.find(offers_tag, idx_shop)
    if idx_offers == -1:
        return text

    idx_after_shop = idx_shop + len(shop_tag)
    return text[:idx_after_shop] + "\n" + text[idx_offers:]


def _name_allowed(name_text: str) -> bool:
    """Проверить, начинается ли name с одного из разрешённых префиксов."""
    t = html.unescape(name_text).strip()
    upper = t.upper()
    return any(upper.startswith(prefix) for prefix in _ALLOWED_PREFIXES_UPPER)


def _filter_offers_by_name(text: str) -> str:
    """Оставить только те <offer>, у которых <name> начинается с нужных слов."""
    pattern = re.compile(r"(<offer\b[^>]*>.*?</offer>)", re.DOTALL | re.IGNORECASE)

    parts: list[str] = []
    last_end = 0
    kept = 0
    skipped = 0

    for match in pattern.finditer(text):
        parts.append(text[last_end:match.start()])

        block = match.group(1)
        name_match = re.search(r"<name>(.*?)</name>", block, re.DOTALL | re.IGNORECASE)

        if not name_match:
            skipped += 1
        else:
            name_text = name_match.group(1)
            if _name_allowed(name_text):
                parts.append(block)
                kept += 1
            else:
                skipped += 1

        last_end = match.end()

    parts.append(text[last_end:])

    result = "".join(parts)
    print(f"[akcent] Фильтр по name: оставлено {kept}, выкинуто {skipped} офферов.")
    return result


def _clean_tags(text: str) -> str:
    """Удалить служебные теги и блоки (url, Offer_ID, delivery, RRP и т.п.)
    и сразу «подтянуть» остальные теги вверх (убрать пустые строки).
    """
    simple_patterns = [
        r"<url>.*?</url>",
        r"<url\s*/>",
        r"<Offer_ID>.*?</Offer_ID>",
        r"<delivery>.*?</delivery>",
        r"<local_delivery_cost>.*?</local_delivery_cost>",
        r"<model>.*?</model>",
        r"<Stock>.*?</Stock>",
    ]
    for pat in simple_patterns:
        text = re.sub(pat, "", text, flags=re.DOTALL | re.IGNORECASE)

    # Удаляем любые manufacturer_warranty (этот тег нам не нужен в итоговом YML)
    text = re.sub(
        r"<manufacturer_warranty>.*?</manufacturer_warranty>",
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    text = re.sub(
        r"<manufacturer_warranty\s*/>",
        "",
        text,
        flags=re.IGNORECASE,
    )

    # Удаляем RRP-цену
    text = re.sub(
        r'<price[^>]*type=["\']RRP["\'][^>]*>.*?</price>',
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )

    # Удаляем обёртку <prices>
    text = re.sub(r"</?prices>", "", text, flags=re.IGNORECASE)

    # Убираем пустые строки
    lines = text.splitlines()
    non_empty = [ln for ln in lines if ln.strip()]
    text = "\n".join(non_empty)

    return text


def _normalize_brand_name(raw: str) -> str:
    """Очистить название бренда и отфильтровать служебные значения."""
    s = html.unescape(raw or "").strip()
    if not s:
        return ""
    s = s.replace("®", "").replace("™", "").strip(" ,.;")
    lower = s.lower()
    if any(bad in lower for bad in _BRAND_BLOCKLIST):
        return ""
    return s


def _extract_brand_from_block(body: str) -> str:
    """Попробовать вытащить бренд из Param/имени/описания."""
    # Специальные параметры про производителя
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

    # Пытаемся найти бренд в name/description
    name_text = ""
    desc_text = ""

    m = re.search(r"<name>(.*?)</name>", body, flags=re.DOTALL | re.IGNORECASE)
    if m:
        name_text = html.unescape(m.group(1))

    m = re.search(r"<description>(.*?)</description>", body, flags=re.DOTALL | re.IGNORECASE)
    if m:
        desc_text = html.unescape(m.group(1))

    haystack = f"{name_text}\n{desc_text}"

    for brand in _KNOWN_BRANDS:
        if re.search(r"\b" + re.escape(brand) + r"\b", haystack, flags=re.IGNORECASE):
            norm = _normalize_brand_name(brand)
            if norm:
                return norm

    # Частный случай для интерактивных панелей SBID-...
    if "SBID-" in name_text:
        return "SBID"

    return ""


def _fill_empty_vendor(body: str) -> str:
    """Заполнить пустой <vendor/>, если возможно, не трогая нормальные бренды."""

    def _has_good_vendor(s: str) -> bool:
        m = re.search(r"<vendor>(.*?)</vendor>", s, flags=re.DOTALL | re.IGNORECASE)
        if not m:
            return False
        val = html.unescape(m.group(1)).strip()
        if not val:
            return False
        lower = val.lower()
        if any(bad in lower for bad in _BRAND_BLOCKLIST):
            return False
        return True

    # Если уже есть нормальный бренд — ничего не делаем
    if _has_good_vendor(body):
        return body

    brand = _extract_brand_from_block(body)
    if not brand:
        return body

    def repl_empty(match: re.Match) -> str:
        indent = match.group(1) or ""
        return f"{indent}<vendor>{brand}</vendor>"

    # <vendor/>
    new_body = re.sub(
        r"(\s*)<vendor\s*/>",
        repl_empty,
        body,
        count=1,
        flags=re.IGNORECASE,
    )
    if new_body != body:
        return new_body

    # <vendor>   </vendor>
    new_body2 = re.sub(
        r"(\s*)<vendor>\s*</vendor>",
        repl_empty,
        body,
        count=1,
        flags=re.IGNORECASE,
    )
    if new_body2 != body:
        return new_body2

    # Если внутри vendor что-то из блок-листа — заменяем на найденный бренд
    def repl_blocked(match: re.Match) -> str:
        indent = match.group(1) or ""
        val = html.unescape(match.group(2) or "").strip()
        if any(bad in val.lower() for bad in _BRAND_BLOCKLIST):
            return f"{indent}<vendor>{brand}</vendor>"
        return match.group(0)

    new_body3 = re.sub(
        r"(\s*)<vendor>(.*?)</vendor>",
        repl_blocked,
        body,
        count=1,
        flags=re.DOTALL | re.IGNORECASE,
    )
    return new_body3


def _apply_price_rules(base: int) -> int:
    """Применить наценку 4% + фиксированный диапазон и хвост 900.

    Если итоговая цена >= 9 000 000 — вернуть 100.
    """
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


def _move_related_products_to_description(body: str) -> str:
    """Перенести Param name="Сопутствующие товары" из характеристик в конец description."""
    pattern = re.compile(
        r'<Param\s+name="Сопутствующие товары">(.*?)</Param>',
        re.DOTALL | re.IGNORECASE,
    )
    matches = pattern.findall(body)
    if not matches:
        return body

    items: list[str] = []
    for raw_val in matches:
        text = html.unescape(raw_val).strip()
        if not text:
            continue
        text = re.sub(r"\s+", " ", text)
        if text not in items:
            items.append(text)

    # Удаляем все такие Param из тела
    body = pattern.sub("", body)

    if not items:
        return body

    block_lines = ["Сопутствующие товары и совместимые устройства:"]
    for item in items:
        block_lines.append(f"- {item}")
    block_text = "\n".join(block_lines)

    # Вставляем блок в конец description
    desc_pattern = re.compile(
        r"(<description>)(.*?)(</description>)",
        re.DOTALL | re.IGNORECASE,
    )
    m = desc_pattern.search(body)
    if m:
        prefix, inner, suffix = m.groups()
        inner_clean = inner.rstrip()
        if inner_clean:
            new_inner = inner_clean + "\n\n" + block_text
        else:
            new_inner = block_text
        new_desc = prefix + new_inner + suffix
        body = body[: m.start()] + new_desc + body[m.end() :]
        return body

    # Если description не было вообще — создаём
    body = body.rstrip() + "\n<description>" + block_text + "</description>\n"
    return body


def _filter_params(body: str) -> str:
    """Выкинуть из Param заведомо мусорные/служебные параметры."""

    def repl(match: re.Match) -> str:
        name = html.unescape(match.group(1) or "").strip()
        value = html.unescape(match.group(2) or "").strip()

        if not name:
            return match.group(0)

        # Полностью выкидываем параметры, не нужные покупателю/SEO
        if name in {
            "Наименование производителя",
            "Сопутствующие товары",
            "Совместимые продукты",
            "Объем",
            "Количество игл",
            "Вид",
        }:
            return ""

        if name == "Оригинальное разрешение":
            # У поставщика тут обычно просто "Оригинальное" — смысла нет
            return ""

        # Чистим заведомо бесполезные значения "Тип"
        if name == "Тип":
            v = value.strip().lower()
            if v in {
                "шредеры офисные",
                "ёмкость для отработанных чернил",
                "емкость для отработанных чернил",
            } or "картридж epson" in v or "фабрика печати" in v:
                return ""

        # "Для бренда" = Epson дублирует vendor/производителя — выкидываем
        if name == "Для бренда":
            if value.strip().lower() == "epson":
                return ""

        return match.group(0)

    return re.sub(
        r'<Param\s+name="([^"]*)">(.*?)</Param>',
        repl,
        body,
        flags=re.DOTALL,
    )




def _build_description_akcent(body: str) -> str:
    """Собрать HTML-описание Akcent: WhatsApp-блок, текст и характеристики."""

    def _parse_params(block: str) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for m in re.finditer(r'<Param\s+name="([^"]*)">(.*?)</Param>', block, flags=re.DOTALL):
            name = html.unescape(m.group(1) or "").strip()
            val = html.unescape(m.group(2) or "").strip()
            if not name or not val:
                continue
            val = re.sub(r"\s+", " ", val)
            out.append((name, val))
        return out

    def _extract_compat(desc: str) -> tuple[str, list[str]]:
        lines = [ln.rstrip() for ln in desc.splitlines()]
        new_lines: list[str] = []
        compat: list[str] = []
        i = 0
        while i < len(lines):
            raw = lines[i]
            line = raw.strip()
            if "Сопутствующие товары и совместимые устройства" in line:
                i += 1
                while i < len(lines):
                    l = lines[i].strip()
                    if not l:
                        i += 1
                        break
                    if l.startswith(("-", "•")):
                        compat.append(l.lstrip("-• ").strip())
                    else:
                        compat.append(l)
                    i += 1
            else:
                new_lines.append(raw)
                i += 1
        main = "\n".join(new_lines).strip()
        return main, compat

    def _shorten(text_: str, max_len: int = 700) -> str:
        text_ = re.sub(r"\s+", " ", text_).strip()
        if len(text_) <= max_len:
            return text_
        cut = text_.rfind(".", 0, max_len)
        if cut == -1:
            cut = max_len
        return text_[:cut].rstrip()

    def _classify(name: str, params_map: dict[str, str]) -> str:
        n = name.lower()
        t = (params_map.get("Тип") or params_map.get("Тип устройства") or "").lower()
        if any(w in n for w in ("картридж", "чернил", "емкость для отработанных чернил", "ёмкость для отработанных чернил")) or "картридж" in t:
            return "consumable"
        if any(w in n for w in ("принтер", "мфу", "многофункцион")) or "принтер" in t or "мфу" in t:
            return "printer"
        if "проектор" in n or "projector" in n or "proj" in (params_map.get("Производитель") or "").lower():
            return "projector"
        if "шредер" in n or "уничтожитель" in n:
            return "shredder"
        return "other"

    def _build_fallback_paragraph(name: str, vendor: str, params_map: dict[str, str], compat: list[str]) -> str:
        n = name.strip()
        v = vendor.strip()
        cat = _classify(n or "", params_map)
        parts: list[str] = []
        base = n or params_map.get("Тип") or ""
        if not base:
            return ""
        if cat == "consumable":
            color = params_map.get("Цвет печати") or params_map.get("Цвет чернил")
            res = params_map.get("Ресурс") or params_map.get("Объём") or params_map.get("Объем")
            sent = f"{base} — расходный материал"
            if v:
                sent += f" {v}"
            sent += " для устройств соответствующей серии."
            if color:
                sent += f" Обеспечивает печать в цвете {color.lower()}."
            if res:
                sent += f" Ресурс: {res}."
            parts.append(sent)
        elif cat == "printer":
            fmt = params_map.get("Формат") or params_map.get("Формат печати")
            tech = params_map.get("Технология печати") or params_map.get("Тип печати")
            sent = f"{base} подходит для печати документов и материалов в офисе или дома."
            if fmt:
                sent += f" Поддерживает печать до формата {fmt}."
            if tech:
                sent += f" Используется технология печати: {tech.lower()}."
            parts.append(sent)
        elif cat == "projector":
            bright = params_map.get("Яркость,ANSI люмен") or params_map.get("Яркость, ANSI люмен") or params_map.get("Яркость")
            sent = f"{base} предназначен для презентаций и демонстрации контента в переговорных комнатах, учебных классах или небольших залах."
            if bright:
                sent += f" Яркость до {bright} обеспечивает чёткое изображение."
            parts.append(sent)
        elif cat == "shredder":
            level = params_map.get("Уровень секретности") or params_map.get("Уровень секретности DIN")
            sent = f"{base} используется для безопасного уничтожения документов в офисе."
            if level:
                sent += f" Уровень секретности: {level}."
            parts.append(sent)
        else:
            sent = f"{base} — решение для повседневной работы и задач в офисе или дома."
            parts.append(sent)
        if compat:
            few = ", ".join(compat[:3])
            parts.append(f"Подходит для использования с моделями: {few}.")
        return " ".join(parts).strip()

    # Вытаскиваем name, vendor и исходный description
    name_match = re.search(r"<name>(.*?)</name>", body, flags=re.DOTALL | re.IGNORECASE)
    name_text = html.unescape(name_match.group(1).strip()) if name_match else ""

    vendor_match = re.search(r"<vendor>(.*?)</vendor>", body, flags=re.DOTALL | re.IGNORECASE)
    vendor_text = html.unescape(vendor_match.group(1).strip()) if vendor_match else ""

    desc_match = re.search(r"<description>(.*?)</description>", body, flags=re.DOTALL | re.IGNORECASE)
    raw_desc = html.unescape(desc_match.group(1)) if desc_match else ""
    raw_desc = raw_desc.replace("\r\n", "\n")

    main_text, compat_items = _extract_compat(raw_desc)
    params = _parse_params(body)
    params_map: dict[str, str] = {}
    for k, v in params:
        if k not in params_map:
            params_map[k] = v

    main_text = main_text.strip()
    if main_text:
        main_text = _shorten(main_text)
    if len(main_text) < 80:
        main_text = _build_fallback_paragraph(name_text, vendor_text, params_map, compat_items)

    parts: list[str] = []
    parts.append(WHATSAPP_BLOCK)

    if name_text:
        parts.append(f"\n<h3>{html.escape(name_text)}</h3>")

    if main_text:
        # один или два абзаца максимум
        paras = re.split(r"\n{2,}", main_text)
        for p in paras:
            p = p.strip()
            if not p:
                continue
            parts.append(f"<p>{html.escape(p)}</p>")

    if params:
        parts.append("\n<h3>Характеристики</h3>")
        parts.append("<ul>")
        for k, v in params:
            parts.append(f"<li><strong>{html.escape(k)}:</strong> {html.escape(v)}</li>")
        parts.append("</ul>")

    if compat_items:
        parts.append("\n<h3>Совместимые устройства</h3>")
        parts.append("<ul>")
        for item in compat_items[:10]:
            parts.append(f"<li>{html.escape(item)}</li>")
        parts.append("</ul>")

    new_inner = "\n".join(parts)

    if desc_match:
        start, end = desc_match.span(1)
        body = body[:start] + new_inner + body[end:]
    else:
        body = body.rstrip() + "\n<description>" + new_inner + "</description>\n"

    return body

def _transform_offers(text: str) -> str:
    """Привести <offer> к нужному виду."""

    def _process_offer(match: re.Match) -> str:
        header = match.group(1)
        body = match.group(2)
        footer = match.group(3)

        # Берём article, если есть, иначе старый id
        article_match = re.search(r'\barticle="([^"]*)"', header)
        art = (article_match.group(1).strip() if article_match else "").strip()

        if not art:
            id_match = re.search(r'\bid="([^"]*)"', header)
            if id_match:
                art = id_match.group(1).strip()

        new_id = f"AK{art}" if art else ""
        avail_match = re.search(r'\bavailable="([^"]*)"', header)
        available = avail_match.group(1).strip() if avail_match else "true"

        # Новый заголовок оффера
        new_header = f'<offer id="{new_id}" available="{available}">\n'

        # Вытаскиваем categoryId
        cat_val = ""
        cat_val_match = re.search(
            r"<categoryId[^>]*>(.*?)</categoryId>",
            body,
            re.DOTALL | re.IGNORECASE,
        )
        if cat_val_match:
            cat_val = cat_val_match.group(1).strip()

        # Удаляем любые старые categoryId
        body = re.sub(
            r"<categoryId[^>]*>.*?</categoryId>",
            "",
            body,
            flags=re.DOTALL | re.IGNORECASE,
        )
        body = re.sub(r"<categoryId[^>]*/>", "", body, flags=re.IGNORECASE)

        body = body.lstrip()

        # Строгий порядок первых трёх тегов
        prefix = (
            f"<categoryId>{cat_val}</categoryId>\n"
            f"<vendorCode>{new_id}</vendorCode>\n"
            "<currencyId>KZT</currencyId>\n"
        )
        body = prefix + body

        # Бренд
        body = _fill_empty_vendor(body)

        # Пересчёт цены
        def _reprice(match_price: re.Match) -> str:
            base_str = match_price.group(1)
            try:
                base = int(base_str)
            except ValueError:
                return match_price.group(0)
            new_price = _apply_price_rules(base)
            return f"<price>{new_price}</price>"

        body = re.sub(
            r'<price[^>]*type=["\']Цена дилерского портала KZT["\'][^>]*>(\d+)</price>',
            _reprice,
            body,
            flags=re.IGNORECASE,
        )

        # Сопутствующие товары → в описание
        body = _move_related_products_to_description(body)

        # Фильтрация мусорных Param
        body = _filter_params(body)

        # Перестраиваем description под Akcent
        body = _build_description_akcent(body)

        return new_header + body + footer

    pattern = re.compile(r"(<offer\b[^>]*>)(.*?)(</offer>)", re.DOTALL | re.IGNORECASE)
    new_text, count = pattern.subn(_process_offer, text)
    print(f"[akcent] Трансформация offer: обработано {count} офферов.")
    return new_text


def _normalize_layout(text: str) -> str:
    """Привести разметку к ровному виду и расставить разрывы."""
    # Убираем начальные пробелы у строк
    lines = text.splitlines()
    text = "\n".join(line.lstrip(" \t") for line in lines)

    # <shop><offers> + пустая строка + первый offer
    text = re.sub(
        r"<shop>\s*<offers>\s*<offer",
        "<shop><offers>\n\n<offer",
        text,
        count=1,
    )

    # Перенос после заголовка offer перед categoryId
    text = re.sub(
        r"(<offer\b[^>]*>)\s*<categoryId>",
        r"\1\n<categoryId>",
        text,
        flags=re.IGNORECASE,
    )

    # Пустая строка между офферами
    text = re.sub(r"</offer>\s*<offer", "</offer>\n\n<offer", text)
    # Пустая строка перед </offers>
    text = re.sub(r"</offer>\s*</offers>", "</offer>\n\n</offers>", text)

    # Убираем пустые строки ВНУТРИ offer
    lines = text.splitlines()
    out_lines: list[str] = []
    inside_offer = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("<offer "):
            inside_offer = True
            out_lines.append(line)
            continue

        if stripped == "</offer>":
            inside_offer = False
            out_lines.append(line)
            continue

        if inside_offer and not stripped:
            # пропускаем пустые строки внутри <offer>...</offer>
            continue

        out_lines.append(line)

    return "\n".join(out_lines)


def download_akcent_feed(source_url: str, out_path: Path) -> None:
    """Скачать файл поставщика, обработать и сохранить на диск."""
    print(f"[akcent] Скачиваем файл: {source_url}")
    resp = requests.get(source_url, timeout=60)
    resp.raise_for_status()

    text = _decode_bytes(resp.content)
    print(f"[akcent] Получено байт: {len(resp.content)}")

    text = _strip_shop_header(text)
    text = _filter_offers_by_name(text)
    text = _clean_tags(text)
    text = _transform_offers(text)
    text = _normalize_layout(text)

    out_bytes = text.encode("utf-8")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(out_bytes)
    print(f"[akcent] Записано байт: {len(out_bytes)} в {out_path}")


def main() -> int:
    """Точка входа скрипта."""
    source_url = os.getenv(
        "AKCENT_URL",
        "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml",
    )
    out_file = Path(os.getenv("OUT_FILE", "docs/akcent.yml"))

    try:
        download_akcent_feed(source_url, out_file)
    except Exception as exc:  # noqa: BLE001
        print(f"[akcent] Ошибка при скачивании: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
