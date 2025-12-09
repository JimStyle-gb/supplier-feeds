#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_alstyle.py — сборка фида AlStyle под эталонную структуру (AlStyle как референс).

v119 (2025-12-09):
- Исправлено: скрипт по умолчанию НЕ пропускает сборку на событии push (теперь всегда пересобирает). Для старого поведения можно задать ALSTYLE_SKIP_PUSH=1.
- Добавлено: автоматическая санитаризация текста (кириллица/латиница-двойники, единицы Вт, IEC/С13, дефисы вида "4 - х", корректировка "3/4 ... разъёма", типовая опечатка "и тех устройства которым").
- Исправлено: _slugify() — сначала транслитерация кириллицы, затем фильтрация (slug-ключи стали стабильнее).
- Добавлено: защита CDATA от ']]>' и удаление запрещённых управляющих символов XML 1.0.

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
CATEGORIES_FILE_DEFAULT = "docs/alstyle_categories.txt"
CURRENCY_ID = "KZT"

# AlStyle — ежедневно в 01:00 по Алматы
SCHEDULE_HOUR_ALMATY = 1
ALMATY_UTC_OFFSET = 5  # Алматы: UTC+5 (без DST)

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


# --- Время ---
def _now_almaty() -> datetime:
    # Время "как в Actions": utcnow + 5
    return datetime.utcnow().replace(microsecond=0) + timedelta(hours=ALMATY_UTC_OFFSET)


def _next_daily_run(build_time: datetime, hour: int) -> datetime:
    # Следующая ближайшая сборка (всегда в будущем)
    cand = build_time.replace(hour=hour, minute=0, second=0)
    if cand <= build_time:
        cand = cand + timedelta(days=1)
    return cand


# --- Утилиты ---
def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


# --- Санитаризация текста (авто-исправления в новых товарах) ---
_LAT2CYR = str.maketrans({
    "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н", "K": "К", "M": "М", "O": "О", "P": "Р", "T": "Т", "X": "Х",
    "a": "а", "b": "в", "c": "с", "e": "е", "h": "н", "k": "к", "m": "м", "o": "о", "p": "р", "t": "т", "x": "х",
})
_CYR2LAT = str.maketrans({
    "А": "A", "В": "B", "С": "C", "Е": "E", "Н": "H", "К": "K", "М": "M", "О": "O", "Р": "P", "Т": "T", "Х": "X",
    "а": "a", "в": "b", "с": "c", "е": "e", "н": "h", "к": "k", "м": "m", "о": "o", "р": "p", "т": "t", "х": "x",
})


def _strip_xml_ctrl(s: str) -> str:
    # Удаляем запрещённые управляющие символы XML 1.0
    return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", s or "")


def _fix_mixed_scripts(s: str) -> str:
    # 1) Латиница-двойники внутри русских слов -> кириллица
    #    пример: Cтильный, Bт, Cтоечные
    x = s or ""
    x = re.sub(r"(?<=\b)[ABCEHKMOPTX](?=[А-Яа-яЁё])", lambda m: m.group(0).translate(_LAT2CYR), x)
    x = re.sub(r"(?<=[А-Яа-яЁё])[ABCEHKMOPTX](?=[А-Яа-яЁё])", lambda m: m.group(0).translate(_LAT2CYR), x)

    # 2) Кириллица-двойники внутри кодов/стандартов -> латиница
    #    пример: IEС -> IEC, С13 -> C13
    x = re.sub(r"(?<=[A-Za-z0-9])[АВЕСНКМОРТХ](?=[A-Za-z0-9])", lambda m: m.group(0).translate(_CYR2LAT), x)
    x = re.sub(r"(?<=[A-Za-z])[АВЕСНКМОРТХ](?=[^А-Яа-яЁё]|$)", lambda m: m.group(0).translate(_CYR2LAT), x)
    x = re.sub(r"(^|[^А-Яа-яЁё])([АВЕСНКМОРТХ])(?=\d)", lambda m: m.group(1) + m.group(2).translate(_CYR2LAT), x)

    return x


def _fix_numeric_hyphens(s: str) -> str:
    x = s or ""
    # 4 - х -> 4-х
    x = re.sub(r"\b(\d+)\s*-\s*х\b", r"\1-х", x, flags=re.I)
    # 4-х парный -> 4-парный
    x = re.sub(r"\b(\d+)-х\s+парн(ый|ая|ое|ые)\b", r"\1-парн\2", x, flags=re.I)
    return x


def _fix_ru_inflections(s: str) -> str:
    # Корректировка только для "разъём(а/ов)" по числу
    def repl(m: re.Match) -> str:
        n = int(m.group(1))
        # 11-14 -> разъёмов
        if 11 <= (n % 100) <= 14:
            form = "разъёмов"
        else:
            last = n % 10
            if last == 1:
                form = "разъём"
            elif last in (2, 3, 4):
                form = "разъёма"
            else:
                form = "разъёмов"
        return f"{m.group(1)} {m.group(2)} {form}"

    return re.sub(r"\b(\d+)\s+(выходн(?:ых|ые))\s+разъёмов\b", repl, s or "", flags=re.I)


def _fix_typo_phrases(s: str) -> str:
    x = s or ""
    # типовая опечатка из описаний
    x = re.sub(r"\bи\s+тех\s+устройства\s+которым\b", "и тех устройств, которым", x, flags=re.I)
    return x


def _safe_cdata_payload(s: str) -> str:
    # Защита CDATA от ']]>'
    return (s or "").replace("]]>", "]]]]><![CDATA[>")


def _sanitize_text(s: str, *, keep_ws: bool = False) -> str:
    x = _strip_xml_ctrl(s or "")
    x = x.replace("\ufeff", "")
    x = x.replace("\r\n", "\n").replace("\r", "\n")
    x = _fix_mixed_scripts(x)
    x = _fix_numeric_hyphens(x)
    x = _fix_ru_inflections(x)
    x = _fix_typo_phrases(x)
    if not keep_ws:
        x = _norm_spaces(x)
    return x



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


def _read_categories(path: str) -> set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    out = set()
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.add(s)
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
    vendor = _sanitize_text(vendor)
    name = _sanitize_text(name)

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
    name = _sanitize_text(name)
    native_desc = _sanitize_text(native_desc, keep_ws=True)
    native_html = _native_desc_to_p(native_desc, name)
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
    payload = _safe_cdata_payload(cdata)
    return f"<description><![CDATA[{payload}]]></description>"


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
        name = _sanitize_text(self.name)
        vendor = _sanitize_text(self.vendor)
        native_desc = _sanitize_text(self.native_desc, keep_ws=True)
        params = [(_sanitize_text(k), _sanitize_text(v)) for (k, v) in (self.params or [])]
        lines: List[str] = []
        lines.append(f'<offer id="{self.oid}" available="{_bool_str(self.available)}">')
        lines.append("<categoryId></categoryId>")
        lines.append(f"<vendorCode>{self.oid}</vendorCode>")
        lines.append(f"<name>{name}</name>")
        lines.append(f"<price>{self.price}</price>")
        for pic in self.pictures:
            lines.append(f"<picture>{pic}</picture>")
        if vendor:
            lines.append(f"<vendor>{vendor}</vendor>")
        lines.append(f"<currencyId>{CURRENCY_ID}</currencyId>")
        lines.append(_build_description(name, native_desc, params))
        for k, v in _sort_params(params):
            k2 = _sanitize_text(k)
            v2 = _sanitize_text(v)
            if not k2 or not v2:
                continue
            lines.append(f'<param name="{k2}">{v2}</param>')
        lines.append(f"<keywords>{_build_keywords(vendor, name)}</keywords>")
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
    # Запуск по push:
    # - по умолчанию пересобираем ВСЕГДА (чтобы время сборки и правки сразу попадали в файл)
    # - для старого поведения можно задать ALSTYLE_SKIP_PUSH=1 (тогда ограничение 01:00 вернётся)
    if os.getenv("FORCE_YML_REFRESH", "").strip().lower() in {"1", "true", "yes"}:
        return True
    ev = (os.getenv("GITHUB_EVENT_NAME") or "").strip().lower()
    if ev == "push":
        if os.getenv("ALSTYLE_SKIP_PUSH", "").strip().lower() in {"1", "true", "yes"}:
            return build_time.hour == SCHEDULE_HOUR_ALMATY
        return True
    return True



def main() -> int:
    url = os.getenv("ALSTYLE_URL", SUPPLIER_URL_DEFAULT).strip() or SUPPLIER_URL_DEFAULT
    out_path = os.getenv("OUT", OUT_DEFAULT).strip() or OUT_DEFAULT
    cat_path = os.getenv("CATEGORIES_FILE", CATEGORIES_FILE_DEFAULT).strip() or CATEGORIES_FILE_DEFAULT

    build_time = _now_almaty()
    if not _should_run_now(build_time):
        print(f"[alstyle] skip: event=push and hour={build_time.hour}, ожидается {SCHEDULE_HOUR_ALMATY} (Алматы).")
        return 0

    allowed_cats = _read_categories(cat_path)
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

    next_run = _next_daily_run(build_time, SCHEDULE_HOUR_ALMATY)

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
