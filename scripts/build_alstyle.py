#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_alstyle.py — сборка фида AlStyle под эталонную структуру (AlStyle как референс).

v122 (2025-12-11):
- Удалено: чтение categoryId из файла категорий (все связано с путём/файлом)
- Добавлено: вшитый список categoryId (include) + опциональный override через env ALSTYLE_CATEGORY_IDS
- Изменено расписание: ежедневно в 01:00 (Алматы) + ручной запуск в любое время
"""

from __future__ import annotations

import os
import re
import sys
import math
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import xml.etree.ElementTree as ET

try:
    import requests  # type: ignore
except Exception:
    requests = None


# --- Константы ---
SUPPLIER_NAME = "AlStyle"
SUPPLIER_URL_DEFAULT = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
OUT_DEFAULT = "docs/alstyle.yml"
CURRENCY_ID = "KZT"

# AlStyle — 1/10/20 числа месяца в 01:00 по Алматы
SCHEDULE_HOUR_ALMATY = 1
SCHEDULE_DAYS_MONTH = tuple(range(1, 32))  # ежедневный режим: все дни месяца
ALMATY_UTC_OFFSET = 5  # Алматы: UTC+5 (без DST)

# Вшитый include-фильтр по categoryId (строки). Если пусто — фильтр выключен.
# Можно переопределить через env: ALSTYLE_CATEGORY_IDS (через запятую или перевод строки)
ALSTYLE_ALLOWED_CATEGORY_IDS: set[str] = {
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
# Городской хвост keywords (как в эталонном AlStyle)
ALSTYLE_CITY_TAIL = "Казахстан, Алматы, Астана, Шымкент, Караганда, Актобе, Павлодар, Атырау, Тараз, Костанай, Кызылорда, Петропавловск, Талдыкорган, Актау"

# Блок WhatsApp (эталон)
AL_WA_BLOCK = (
    '<!-- WhatsApp -->\n'
    '<div style="font-family: Cambria, \'Times New Roman\', serif; line-height:1.5; color:#222; font-size:15px;">'
    '<p style="text-align:center; margin:0 0 12px;">'
    '<a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" '
    'style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; '
    'border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);">'
    '&#128172; Написать в WhatsApp</a></p></div>'
)

# Линия 2px (эталон)
AL_HR_2PX = '<hr style="border:none; border-top:2px solid #E7D6B7; margin:12px 0;" />'

# Оплата/доставка (эталон)
AL_PAY_BLOCK = (
    '<!-- Оплата и доставка -->\n'
    '<div style="font-family: Cambria, \'Times New Roman\', serif; line-height:1.5; color:#222; font-size:15px;">'
    '<div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;">'
    '<h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3>'
    '<ul style="margin:0; padding-left:18px;">'
    '<li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li>'
    '<li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li>'
    '</ul>'
    '<hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" />'
    '<h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3>'
    '<ul style="margin:0; padding-left:18px;">'
    '<li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li>'
    '<li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5 000 тг. | 3–7 рабочих дней</em></li>'
    '<li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>'
    '<li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li>'
    '</ul>'
    '</div></div>'
)

# Приоритет характеристик (как в ваших унифицированных скриптах)
PARAM_PRIO = [
    "Производитель", "Бренд", "Вендор", "Vendor", "Brand",
    "Модель", "Артикул", "Код производителя", "Совместимость",
    "Тип", "Тип печати", "Цвет", "Ресурс", "Объем", "Ёмкость", "Гарантия", "Страна происхождения"
]
PARAM_PRIO_INDEX = {k.lower(): i for i, k in enumerate(PARAM_PRIO)}


# Ненужные служебные параметры (убираем из <param> и блока характеристик)
PARAM_DROP_NAMES = {
    "Благотворительность",
    "Новинка",
    "Снижена цена",
    "Назначение",
    "Штрихкод",
    "Код товара Kaspi",
    "Код ТН ВЭД",
    "Объём",
}
PARAM_DROP_LC = {x.lower() for x in PARAM_DROP_NAMES}



# --- Время ---
def _now_almaty() -> datetime:
    # Время "как в Actions": utcnow + 5
    return datetime.utcnow().replace(microsecond=0) + timedelta(hours=ALMATY_UTC_OFFSET)


def _next_scheduled_run(build_time: datetime, hour: int, days: tuple[int, ...]) -> datetime:
    # Ежедневное расписание: ближайшая сборка всегда в 01:00 (Алматы)
    # Если текущее время до указанного часа — сегодня, иначе завтра.
    cand = build_time.replace(hour=hour, minute=0, second=0)
    if cand <= build_time:
        cand = cand + timedelta(days=1)
    return cand


def _get_allowed_categories() -> set[str]:
    raw = (os.getenv("ALSTYLE_CATEGORY_IDS") or "").strip()
    if raw:
        parts = re.split(r"[\s,;]+", raw)
        return {p.strip() for p in parts if p.strip()}
    return set(ALSTYLE_ALLOWED_CATEGORY_IDS)


# --- Утилиты ---
def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _bool_str(v: bool) -> str:
    return "true" if v else "false"


def _parse_bool(s: Optional[str]) -> bool:
    if s is None:
        return False
    t = s.strip().lower()
    return t in {"1", "true", "yes", "y", "да"}


def _safe_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    t = re.sub(r"[^\d]", "", s)
    if not t:
        return None
    try:
        return int(t)
    except Exception:
        return None


def _tail_900(price: int) -> int:
    # Приводим к окончанию **900** (как правило "хвост 900")
    if price <= 100:
        return 100
    v = int(math.ceil(price / 1000.0) * 1000 - 100)
    return max(100, v)


def _apply_price_rule(supplier_price: Optional[int]) -> int:
    # Правило цены: 4% + ступени, хвост 900, >= 9 000 000 -> 100, отсутствует -> 100
    if supplier_price is None or supplier_price <= 100:
        return 100
    if supplier_price >= 9_000_000:
        return 100

    p = float(supplier_price) * 1.04

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
        (750_001, 1_000_000, 60_000),
        (1_000_001, 1_500_000, 80_000),
        (1_500_001, 2_000_000, 100_000),
        (2_000_001, 3_000_000, 150_000),
        (3_000_001, 4_000_000, 200_000),
        (4_000_001, 9_000_000, 250_000),
    ]

    add = 0
    for lo, hi, a in tiers:
        if lo <= supplier_price <= hi:
            add = a
            break

    out = int(round(p + add))
    out = _tail_900(out)

    if out >= 9_000_000:
        return 100
    return out



def _sort_params(items: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    # Сортировка параметров: приоритеты, затем по алфавиту
    def sk(it: Tuple[str, str]) -> Tuple[int, str]:
        k = it[0].strip()
        return (PARAM_PRIO_INDEX.get(k.lower(), 10_000), k.lower())
    return sorted(items, key=sk)


def _fix_text_common(s: str) -> str:
    # Мини-правки грамматики/опечаток без "переписывания" смысла
    s = s or ""
    s = s.replace("Shuko", "Schuko")
    s = s.replace("Cтоечные", "Стоечные").replace("Cтоечный", "Стоечный")
    s = s.replace("Линейно-Интерактивный", "Линейно-интерактивный")
    s = re.sub(r"\b(\d+)\s*-\s*х\b", r"\1-х", s)
    return s


# --- SEO: keywords ---
_RUS2LAT = {
    "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m",
    "н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch",
    "ъ":"","ы":"y","ь":"","э":"e","ю":"yu","я":"ya"
}


def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            continue
        if "а" <= ch <= "я" or ch == "ё":
            out.append(_RUS2LAT.get(ch, ""))
        else:
            out.append("-")
    x = "".join(out)
    x = re.sub(r"[^a-z0-9\-]+", "-", x)
    x = re.sub(r"-{2,}", "-", x).strip("-")
    return x


def _build_keywords(vendor: str, name: str) -> str:
    # Простая схема как у вас: бренд + имя + токены + слаги + хвост городов
    vendor = _norm_spaces(vendor)
    name = _norm_spaces(name)

    parts: List[str] = []
    if vendor:
        parts.append(vendor)
    if name:
        parts.append(name)

    # Токены (рус/лат/цифры)
    toks = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", name)
    toks = [t for t in toks if len(t) >= 2]
    for t in toks[:18]:
        parts.append(t)

    if vendor and name:
        # бренд + модель (первый "код" или слово с цифрой)
        m = re.search(r"\b[A-Za-zА-Яа-яЁё]*\d[\w\-]*\b", name)
        if m:
            parts.append(f"{vendor} {m.group(0)}")

    # Слаги
    base_slug = _slugify(name)
    if base_slug:
        parts.append(base_slug)
        if vendor:
            parts.append(f"{base_slug}-{_slugify(vendor)}".strip("-"))

    # Удаляем дубли и городской хвост, потом добавляем хвост как в AlStyle
    drop = {x.strip() for x in ALSTYLE_CITY_TAIL.split(",")}
    seen = set()
    cleaned: List[str] = []
    for p in parts:
        p = _norm_spaces(p).strip(" ,")
        if not p or p in drop:
            continue
        k = p.lower()
        if k in seen:
            continue
        seen.add(k)
        cleaned.append(p)

    return ", ".join(cleaned + [ALSTYLE_CITY_TAIL])


# --- Описание ---
def _html_escape_min(s: str) -> str:
    # Для CDATA достаточно нормализовать опасные & вне сущностей
    return s


def _native_desc_to_p(desc: str, name: str) -> str:
    d = (desc or "").strip()
    d = _fix_text_common(d)
    if not d:
        return f"<p>{name}</p>"
    # если есть теги <p> — оставляем как есть
    if re.search(r"<\s*p\b", d, flags=re.I):
        return d
    # переносы превращаем в <br>
    d = d.replace("\r\n", "\n").replace("\r", "\n")
    d = re.sub(r"\n{2,}", "\n", d).strip()
    if "\n" in d:
        d = "<br>".join([_norm_spaces(x) for x in d.split("\n") if _norm_spaces(x)])
    return f"<p>{d}</p>"


def _build_chars_block(params: List[Tuple[str, str]]) -> str:
    if not params:
        return "<h3>Характеристики</h3><ul><li><strong>Гарантия:</strong> 0</li></ul>"

    items = []
    for k, v in _sort_params(params):
        k2 = _norm_spaces(k)
        v2 = _norm_spaces(v)
        if not k2 or not v2:
            continue
        items.append(f"<li><strong>{k2}:</strong> {v2}</li>")

    if not items:
        items = ["<li><strong>Гарантия:</strong> 0</li>"]
    return "<h3>Характеристики</h3><ul>" + "".join(items) + "</ul>"


def _build_description(name: str, native_desc: str, params: List[Tuple[str, str]]) -> str:
    # Тройное обогащение description / Характеристики / param:
    # 1) из исходного описания вытягиваем пары "Ключ: значение"
    # 2) добавляем их в params (если таких ключей ещё нет)
    # 3) убираем эти строки из текста описания, чтобы не было дублей
    # 4) блок "Характеристики" и <param> строятся из одного и того же объединённого набора

    name = _norm_spaces(name)
    desc = native_desc or ""

    extra_pairs: List[Tuple[str, str]] = []

    # Если в описании уже есть HTML-абзацы (<p>), не пытаемся вырезать блок "Характеристики:"
    # — считаем, что это уже подготовленный HTML от поставщика.
    if desc and not re.search(r"<\s*p\b", desc, flags=re.I):
        # Нормализуем переводы строк
        tmp = desc.replace("\r\n", "\n").replace("\r", "\n")
        tmp = re.sub(r"\n{2,}", "\n", tmp)
        raw_lines = tmp.split("\n")

        cleaned_lines: List[str] = []
        heading_keys = {
            "характеристики",
            "основные характеристики",
            "основные характеристики и преимущества",
            "особенности",
            "особенности и преимущества",
            "преимущества",
            "условия гарантии",
            "примечание",
            "внимание",
        }

        for raw_line in raw_lines:
            line = raw_line.strip()
            if not line:
                # Пустую строку оставляем — она помогает разделять абзацы
                cleaned_lines.append(raw_line)
                continue

            # Убираем маркеры списков только для распознавания, но не для сохранения текста
            cand = re.sub(r"^[\-\•\*\u2013\u2014]\s*", "", line)

            if ":" in cand:
                key_part, val_part = cand.split(":", 1)
                key = key_part.strip()
                val = val_part.strip()
                lk = key.lower()

                # Явные "служебные" заголовки без значения — просто выкидываем
                if not val and lk in heading_keys:
                    continue

                # Ограничение для ключей характеристик:
                # - не более 2 слов
                # - не более 40 символов
                # Иначе считаем, что это не параметр, а обычный текст и оставляем строку в описании.
                if key:
                    # считаем слова по пробелам
                    word_count = len(re.split(r"\s+", key))
                    if word_count > 2 or len(key) > 40:
                        cleaned_lines.append(raw_line)
                        continue

                # Классический случай "Ключ: значение" — считаем характеристикой
                if key and val and (lk not in PARAM_DROP_LC) and (lk not in heading_keys):
                    extra_pairs.append((key, val))
                    # строку с характеристикой в основном описании больше не показываем
                    continue

            # Всё остальное оставляем как есть (маркетинговый текст, заголовки, и т.п.)
            cleaned_lines.append(raw_line)

        cleaned_text = "\n".join(cleaned_lines).strip()
    else:
        # HTML-режим или пустое описание — не трогаем исходный текст
        cleaned_text = desc

    # Объединяем исходные params и найденные в описании характеристики.
    if extra_pairs:
        combined: List[Tuple[str, str]] = []
        seen = set()

        # 1) сначала берём всё, что уже есть в params
        for k, v in params:
            k2 = _norm_spaces(k)
            v2 = _norm_spaces(v)
            if not k2 or not v2:
                continue
            lk = k2.lower()
            if lk in PARAM_DROP_LC:
                continue
            if lk in seen:
                continue
            seen.add(lk)
            combined.append((k2, v2))

        # 2) добавляем новые пары из описания, если таких ключей ещё нет
        for key, val in extra_pairs:
            k2 = _norm_spaces(key)
            v2 = _norm_spaces(val)
            if not k2 or not v2:
                continue
            lk = k2.lower()
            if lk in PARAM_DROP_LC or lk in seen:
                continue
            seen.add(lk)
            combined.append((k2, v2))

        # Мутируем params "на месте", чтобы OfferOut.to_xml увидел уже обогащённый набор
        params.clear()
        params.extend(combined)

    # Превращаем очищенный текст в HTML, как и раньше
    native_html = _native_desc_to_p(cleaned_text, name)
    chars = _build_chars_block(params)

    # CDATA — как в эталоне: перевод строки сразу после <![CDATA[
    cdata = (
        "\n"
        + AL_WA_BLOCK
        + "\n" + AL_HR_2PX
        + "\n<!-- Описание -->\n"
        + f"<h3>{name}</h3>"
        + native_html
        + "\n" + chars
        + "\n" + AL_PAY_BLOCK
        + "\n"
    )
    return f"<description><![CDATA[{cdata}]]></description>"



# --- Модель оффера ---
@dataclass
class OfferOut:
    oid: str
    available: bool
    name: str
    price: int
    vendor: str
    pictures: List[str]
    params: List[Tuple[str, str]]
    native_desc: str

    def to_xml(self) -> str:
        # Порядок тегов строго фиксируем
        lines: List[str] = []
        lines.append(f'<offer id="{self.oid}" available="{_bool_str(self.available)}">')
        lines.append("<categoryId></categoryId>")
        lines.append(f"<vendorCode>{self.oid}</vendorCode>")
        lines.append(f"<name>{self.name}</name>")
        lines.append(f"<price>{self.price}</price>")
        for pic in self.pictures:
            lines.append(f"<picture>{pic}</picture>")
        if self.vendor:
            lines.append(f"<vendor>{self.vendor}</vendor>")
        lines.append(f"<currencyId>{CURRENCY_ID}</currencyId>")
        lines.append(_build_description(self.name, self.native_desc, self.params))
        for k, v in _sort_params(self.params):
            k2 = _norm_spaces(k)
            v2 = _norm_spaces(v)
            if not k2 or not v2:
                continue
            lines.append(f'<param name="{k2}">{v2}</param>')
        lines.append(f"<keywords>{_build_keywords(self.vendor, self.name)}</keywords>")
        lines.append("</offer>")
        return "\n".join(lines)


# --- Парсинг входного фида ---
def _get_text(node: Optional[ET.Element]) -> str:
    if node is None:
        return ""
    return (node.text or "").strip()


def _collect_params(offer: ET.Element) -> List[Tuple[str, str]]:
    params: List[Tuple[str, str]] = []
    seen = set()
    for p in offer.findall("param"):
        k = (p.get("name") or "").strip()
        v = (p.text or "").strip()
        if not k:
            continue
        lk = k.lower()
        if lk in PARAM_DROP_LC:
            continue
        if lk in seen:
            continue
        seen.add(lk)
        if v:
            params.append((k, _fix_text_common(v)))
    return params


def _collect_pictures(offer: ET.Element) -> List[str]:
    pics = []
    for p in offer.findall("picture"):
        u = _norm_spaces(_get_text(p))
        if u:
            pics.append(u)
    # убрать дубли, сохранить порядок
    out = []
    seen = set()
    for u in pics:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _pick_native_desc(offer: ET.Element) -> str:
    # Для AlStyle обычно <description> уже есть
    d = offer.find("description")
    if d is not None:
        # description может быть в CDATA — ElementTree отдаст как text
        return (d.text or "").strip()
    return ""


def _build_offer_out(offer: ET.Element) -> OfferOut:
    # Достаём id/артикул
    raw_id = offer.get("id") or _get_text(offer.find("vendorCode")) or ""
    raw_id = _norm_spaces(raw_id)

    if not raw_id:
        # fallback — стабильный id по имени
        raw_id = hashlib.md5(_get_text(offer.find("name")).encode("utf-8", errors="ignore")).hexdigest()[:10]

    oid = raw_id if raw_id.upper().startswith("AS") else f"AS{raw_id}"

    name = _fix_text_common(_norm_spaces(_get_text(offer.find("name"))))
    vendor = _fix_text_common(_norm_spaces(_get_text(offer.find("vendor"))))

    # availability: атрибут или тег <available>
    av_attr = offer.get("available")
    av_tag = _get_text(offer.find("available"))
    available = _parse_bool(av_attr) or _parse_bool(av_tag)

    pics = _collect_pictures(offer)
    params = _collect_params(offer)
    native_desc = _pick_native_desc(offer)

    # цена: сначала purchase_price, затем price
    src_price = _safe_int(_get_text(offer.find("purchase_price"))) or _safe_int(_get_text(offer.find("price")))
    out_price = _apply_price_rule(src_price)

    return OfferOut(
        oid=oid,
        available=available,
        name=name,
        price=out_price,
        vendor=vendor,
        pictures=pics,
        params=params,
        native_desc=native_desc,
    )


def _extract_offers(root: ET.Element) -> List[ET.Element]:
    # Ищем offers/offer в любом месте
    offers_node = root.find(".//offers")
    if offers_node is None:
        return []
    return list(offers_node.findall("offer"))


# --- Сборка результата ---
def _make_feed_meta(
    build_time: datetime,
    next_run: datetime,
    cnt_before: int,
    cnt_after: int,
    cnt_true: int,
    cnt_false: int,
    supplier_url: str,
) -> str:
    def row(label: str, value: str) -> str:
        return f"{label:<42} | {value}"

    lines = []
    lines.append("<!--FEED_META")
    lines.append(row("Поставщик", SUPPLIER_NAME))
    lines.append(row("URL поставщика", supplier_url))
    lines.append(row("Время сборки (Алматы)", build_time.strftime("%Y-%m-%d %H:%M:%S")))
    lines.append(row("Ближайшая сборка (Алматы)", next_run.strftime("%Y-%m-%d %H:%M:%S")))
    lines.append(row("Сколько товаров у поставщика до фильтра", str(cnt_before)))
    lines.append(row("Сколько товаров у поставщика после фильтра", str(cnt_after)))
    lines.append(row("Сколько товаров есть в наличии (true)", str(cnt_true)))
    lines.append(row("Сколько товаров нет в наличии (false)", str(cnt_false)))
    lines.append("-->")
    return "\n".join(lines)


def _ensure_footer_spacing(s: str) -> str:
    # Правила пробелов: после <shop><offers> — пустая строка, перед </offers> — пустая строка
    s = re.sub(r"(<shop><offers>\n)(\n*)", r"\1\n", s, count=1)
    s = re.sub(r"(</offer>\n)(</offers>)", r"\1\n\2", s, count=1)
    s = s.rstrip() + "\n"
    return s


def _atomic_write_if_changed(path: str, data: str, encoding: str = "windows-1251") -> bool:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    try:
        new_bytes = data.encode(encoding, errors="strict")
    except UnicodeEncodeError:
        # Приводим текст к Windows-1251 (без падения сборки)
        repl = {
            "\u00a0": " ",  # NBSP
            "\u202f": " ",  # NNBSP
            "\u2013": "-",
            "\u2014": "-",
            "\u2212": "-",
            "\u2018": "'",
            "\u2019": "'",
            "\u201c": '"',
            "\u201d": '"',
            "\u00e4": "a", "\u00c4": "A",
            "\u00f6": "o", "\u00d6": "O",
            "\u00fc": "u", "\u00dc": "U",
            "\u00df": "ss",
            "\u00e9": "e", "\u00e8": "e", "\u00ea": "e", "\u00eb": "e",
            "\u00e1": "a", "\u00e0": "a", "\u00e2": "a", "\u00e3": "a",
            "\u00f3": "o", "\u00f2": "o", "\u00f4": "o", "\u00f5": "o",
            "\u00fa": "u", "\u00f9": "u", "\u00fb": "u",
            "\u00ed": "i", "\u00ec": "i", "\u00ee": "i",
            "\u00e7": "c",
            "\u00f1": "n",
            "\u00e5": "a",
            "\u00f8": "o",
            "\u00e6": "ae",
        }
        for k, v in repl.items():
            data = data.replace(k, v)
        data = data.encode(encoding, errors="ignore").decode(encoding)
        new_bytes = data.encode(encoding, errors="strict")

    if p.exists():
        old_bytes = p.read_bytes()
        if old_bytes == new_bytes:
            return False

    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_bytes(new_bytes)
    tmp.replace(p)
    return True


# --- Загрузка ---
def _fetch(url: str) -> bytes:
    if requests is None:
        raise RuntimeError("requests не установлен. В GitHub Actions он обычно есть.")
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    return r.content


def _should_run_now(build_time: datetime) -> bool:
    # Условия записи YML:
    # 1) расписание: ежедневно в 01:00 (Алматы)
    # 2) ручной запуск: в любое время (push / workflow_dispatch / и т.п.)
    if os.getenv("FORCE_YML_REFRESH", "").strip().lower() in {"1", "true", "yes"}:
        return True
    ev = (os.getenv("GITHUB_EVENT_NAME") or "").strip().lower()
    if ev == "schedule":
        return build_time.hour == SCHEDULE_HOUR_ALMATY
    return True
    ev = (os.getenv("GITHUB_EVENT_NAME") or "").strip().lower()
    if ev == "schedule":
        return (build_time.day in SCHEDULE_DAYS_MONTH) and (build_time.hour == SCHEDULE_HOUR_ALMATY)
    return True


def main() -> int:
    url = os.getenv("ALSTYLE_URL", SUPPLIER_URL_DEFAULT).strip() or SUPPLIER_URL_DEFAULT
    out_path = os.getenv("OUT", OUT_DEFAULT).strip() or OUT_DEFAULT

    build_time = _now_almaty()
    if not _should_run_now(build_time):
        print(f"[alstyle] skip: event=schedule, now={build_time.strftime('%Y-%m-%d %H:%M:%S')} (Алматы); ждём ежедневный запуск в час {SCHEDULE_HOUR_ALMATY}:00.")
        return 0
    allowed_cats = _get_allowed_categories()
    print(f"[alstyle] Скачиваем фид: {url}")

    raw = _fetch(url)
    root = ET.fromstring(raw)

    in_offers = _extract_offers(root)
    cnt_before = len(in_offers)

    out_offers: List[OfferOut] = []
    for o in in_offers:
        # фильтр по categoryId (include)
        cat = _get_text(o.find("categoryId"))
        if allowed_cats and cat not in allowed_cats:
            continue
        out_offers.append(_build_offer_out(o))

    cnt_after = len(out_offers)
    cnt_true = sum(1 for x in out_offers if x.available)
    cnt_false = cnt_after - cnt_true

    next_run = _next_scheduled_run(build_time, SCHEDULE_HOUR_ALMATY, SCHEDULE_DAYS_MONTH)

    feed_meta = _make_feed_meta(
        build_time=build_time,
        next_run=next_run,
        cnt_before=cnt_before,
        cnt_after=cnt_after,
        cnt_true=cnt_true,
        cnt_false=cnt_false,
        supplier_url=url,
    )

    # Сборка тела
    header = [
        '<?xml version="1.0" encoding="windows-1251"?>',
        f'<yml_catalog date="{build_time.strftime("%Y-%m-%d %H:%M")}">',
        "<shop><offers>",
        "",
        feed_meta,
        "",
    ]

    body_lines: List[str] = []
    for off in out_offers:
        body_lines.append(off.to_xml())
        body_lines.append("")  # пустая строка между офферами

    footer = ["</offers>", "</shop>", "</yml_catalog>"]

    out = "\n".join(header + body_lines + footer)
    out = _ensure_footer_spacing(out)

    changed = _atomic_write_if_changed(out_path, out, encoding="windows-1251")
    print(f"[alstyle] Найдено офферов у поставщика: {cnt_before}")
    print(f"[alstyle] В фид попало офферов: {cnt_after}")
    print(f"[alstyle] В наличии true: {cnt_true}; false: {cnt_false}")
    print(f"[alstyle] Записано: {out_path}; changed={'yes' if changed else 'no'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
