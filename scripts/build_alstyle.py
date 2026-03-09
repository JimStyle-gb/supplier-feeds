# -*- coding: utf-8 -*-
"""
AlStyle adapter (AS) — CS-шаблон (config-driven).

Адаптер делает ИДЕАЛЬНЫЙ raw:
- фильтр товаров по categoryId (include) из config/filter.yml
- schema чистит params (drop/aliases/normalizers), без гаданий по совместимости/кодам
- стабильный id/vendorCode с префиксом AS
- pictures: если нет — placeholder
- vendor не должен содержать имя поставщика

Core делает только общее (keywords/description/FEED_META/writer). Для AS в scripts/cs/policy.py
должно быть отключено вмешательство core в params (enable_clean_params=False и т.п.).
"""

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from html import unescape
from pathlib import Path
from typing import Any

import requests
import yaml

from cs.core import OfferOut, write_cs_feed, write_cs_feed_raw
from cs.meta import now_almaty, next_run_at_hour
from cs.pricing import compute_price
from cs.util import norm_ws, safe_int


BUILD_ALSTYLE_VERSION = "build_alstyle_v94_selective_desc_block_desc_cleanup"

ALSTYLE_URL_DEFAULT = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
ALSTYLE_OUT_DEFAULT = "docs/alstyle.yml"
ALSTYLE_RAW_OUT_DEFAULT = "docs/raw/alstyle.yml"
ALSTYLE_ID_PREFIX = "AS"
# Исторически выпавшие офферы не дорисовываем, только диагностируем причину пропажи.
ALSTYLE_WATCH_OIDS = {"AS257478"}

CFG_DIR_DEFAULT = "scripts/suppliers/alstyle/config"
FILTER_FILE_DEFAULT = "filter.yml"
SCHEMA_FILE_DEFAULT = "schema.yml"
POLICY_FILE_DEFAULT = "policy.yml"  # опционально
WATCH_REPORT_DEFAULT = "docs/raw/alstyle_watch.txt"


_RE_HAS_LETTER = re.compile(r"[A-Za-zА-Яа-яЁё]")
_RE_LETTER_SLASH_LETTER = re.compile(r"([A-Za-zА-Яа-яЁё])\s*/\s*([A-Za-zА-Яа-яЁё])")


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _t(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def _parse_id_set(env: str | None, fallback: set[str]) -> set[str]:
    if not env:
        return set(fallback)
    s = env.strip()
    if not s:
        return set(fallback)
    parts = re.split(r"[\s,;]+", s)
    out = {p.strip() for p in parts if p and p.strip()}
    return out or set(fallback)


def _key_quality_ok(k: str, *, require_letter: bool, max_len: int, max_words: int) -> bool:
    kk = norm_ws(k)
    if not kk:
        return False
    if require_letter and not _RE_HAS_LETTER.search(kk):
        return False
    if max_len and len(kk) > int(max_len):
        return False
    if max_words and len(kk.split()) > int(max_words):
        return False
    return True


def _normalize_warranty_to_months(v: str) -> str:
    vv = norm_ws(v)
    if not vv:
        return ""
    low = vv.casefold()
    if low in ("нет", "no", "-", "—"):
        return ""
    m = re.search(r"(\d{1,2})\s*(год|года|лет)\b", low)
    if m:
        n = int(m.group(1))
        return f"{n*12} мес"
    if re.fullmatch(r"\d{1,3}", low):
        return f"{int(low)} мес"
    m = re.search(r"\b(\d{1,3})\b", low)
    if m and ("мес" in low or "month" in low):
        return f"{int(m.group(1))} мес"
    return vv


def _apply_value_normalizers(key: str, val: str, schema: dict[str, Any]) -> str:
    v = norm_ws(val)
    if not v:
        return ""
    vn = (schema.get("value_normalizers") or {})
    ops = vn.get(key) or vn.get(key.casefold()) or []
    for op in ops:
        if op == "warranty_months":
            v = _normalize_warranty_to_months(v)
        elif op == "trim_ws":
            v = norm_ws(v)
    # Нормализация: 'слово/Word' -> 'слово Word' только там, где slash не несёт смысл модели/совместимости
    kcf = norm_ws(key).casefold()
    if kcf not in {"совместимость", "модель", "аналог модели"}:
        v = _RE_LETTER_SLASH_LETTER.sub(r"\1 \2", v)
    v = _sanitize_param_value(key, v)
    if not v:
        return ""
    # Безопасная техно-нормализация только после основной очистки значений.
    # Не трогаем модель/совместимость, чтобы не ломать vendor/model tokens и слэши.
    if kcf not in {"совместимость", "модель", "аналог модели"}:
        v = _normalize_tech_value(v)
        v = re.sub(r"(?<=\d),\s+(?=\d)", ",", v)
        v = re.sub(r"(?iu)\b(\d),(\d{1,3})\s+(мм|см|м|кг|г|Вт|Гц|мс|дюйм(?:а|ов)?|дюйма|дюймов|ГБ|ТБ)\b", r"\1,\2 \3", v)
        v = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+кд\s*(?:/\s*м²|м2)\b", r"\1 кд/м²", v)
        v = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гбит\s*/?\s*с\b", r"\1 Гбит/с", v)
        v = re.sub(r"(?iu)\b(\d+)\s*[xх×]\s*(\d+)\s*Вт\b", r"\1 × \2 Вт", v)
    return v


def _collect_pictures(offer_el: ET.Element, placeholder: str) -> list[str]:
    pics: list[str] = []
    for p in offer_el.findall("picture"):
        u = norm_ws(_t(p))
        if u:
            pics.append(u)
    if not pics:
        pics = [placeholder]
    return pics


def _collect_params(offer_el: ET.Element, schema: dict[str, Any]) -> list[tuple[str, str]]:
    drop = {str(x).casefold() for x in (schema.get("drop_keys_casefold") or [])}
    aliases = {str(k).casefold(): str(v) for k, v in (schema.get("aliases_casefold") or {}).items()}
    rules = schema.get("key_rules") or {}
    require_letter = bool(rules.get("require_letter", True))
    max_len = int(rules.get("max_len", 60))
    max_words = int(rules.get("max_words", 9))

    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for p in offer_el.findall("param"):
        k0 = p.get("name") or ""
        v0 = _t(p)

        k = norm_ws(k0)
        v = norm_ws(v0)
        if not k or not v:
            continue

        kcf = k.casefold()
        if kcf in aliases:
            k = aliases[kcf]

        if not _key_quality_ok(k, require_letter=require_letter, max_len=max_len, max_words=max_words):
            continue

        # hard-drop: в финальном baseline AlStyle коды НКТ не нужны
        if k.casefold() in drop or k.casefold() in ("код нкт",):
            continue

        if k.casefold() == "назначение" and v.casefold() in ("да", "есть"):
            continue
        if k.casefold() == "безопасность" and v.casefold() == "есть":
            continue

        v2 = _apply_value_normalizers(k, v, schema)
        if not v2:
            continue

        sig = (k.casefold(), v2.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((k, v2))

    return out


_DESC_SPEC_START_RE = re.compile(r"(?im)^\s*(Характеристики|Основные характеристики|Технические характеристики)\s*:?\s*$")
_DESC_SPEC_STOP_RE = re.compile(
    r"(?im)^\s*(Преимущества|Комплектация|Условия гарантии|Гарантия|Примечание|Примечания|Особенности|Описание|EUROPRINT)\s*:?\s*$"
)
_DESC_SPEC_LINE_RE = re.compile(
    r"(?im)^\s*"
    r"(Модель|Аналог модели|Совместимость|Совместимые модели|Устройства|Для принтеров|"
    r"Технология печати|Цвет|Цвет печати|Ресурс|Ресурс картриджа|Ресурс картриджа, cтр\.|"
    r"Количество страниц|Кол-во страниц при 5% заполнении А4|Емкость|Ёмкость|Емкость лотка|Ёмкость лотка|"
    r"Степлирование|Дополнительные опции|Применение|Количество в упаковке|Колличество в упаковке)"
    r"\s*(?::|\t+|\s{2,}|[-–—])\s*(.+?)\s*$"
)
_DESC_COMPAT_LINE_RE = re.compile(r"(?im)^\s*Совместим(?:а|о|ы)?\s+с\s+(.+?)\s*$")
_PROJECTOR_RICH_LINE_RE = re.compile(
    r"(?im)^\s*"
    r"(Технология касания|Технология|Разрешение|Яркость|Контраст|Источник света|Световой источник|Оптика|"
    r"Методы установки|Способы установки|Размер экрана|Дистанция|Коэффициент проекции|"
    r"Форматы сторон|Смарт-?система|Беспроводной дисплей|Проводное зеркалирование|"
    r"Интерфейсы|Акустика|Питание|Габариты проектора|Вес проектора|Габариты упаковки|"
    r"Вес упаковки|Языки интерфейса|Комплектация|Беспроводные модули|Беспроводные интерфейсы|"
    r"Беспроводные подключения|Беспроводные возможности)"
    r"\s*(?::|[-–—])?\s*(.+?)\s*$"
)
_MONITOR_RICH_LINE_RE = re.compile(
    r"(?im)^\s*"
    r"(Управление|Пользовательские настройки|Встроенные колонки|Защита замком|HDMI|DisplayPort|USB-?хаб|"
    r"Разъ[её]м для наушников|Поддержка HDCP|Языки меню OSD)"
    r"\s*(?::|[-–—])\s*(.+?)\s*$"
)
_DESC_COMPAT_LABEL_ONLY_RE = re.compile(r"(?im)^\s*(Совместимость|Совместимые модели|Устройства)\s*:?\s*$")
_DESC_TECH_PRINT_LABEL_ONLY_RE = re.compile(r"(?im)^\s*Технология\s+печати\s*:?\s*$")
_DESC_COMPAT_SENTENCE_RE = re.compile(
    r"(?is)\bСовместим(?:а|о|ы)?\s+с\s+(.{6,220}?)(?:(?:[.!?](?:\s|$))|\n|$)"
)
_DESC_FOR_DEVICES_SENTENCE_RE = re.compile(
    r"(?is)\bдля\s+(?:устройств|принтеров(?:\s+и\s+МФУ)?|МФУ|аппаратов)\s+(.{6,220}?)(?:(?:[.!?](?:\s|$))|\n|$)"
)
_COMPAT_BRAND_HINT_RE = re.compile(
    r"(?i)\b(Xerox|Canon|HP|Hewlett|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki|"
    r"VersaLink|AltaLink|WorkCentre|DocuCentre|imageRUNNER|i-SENSYS|ECOSYS|bizhub)\b"
)
_COMPAT_MODEL_HINT_RE = re.compile(
    r"(?i)(?:\b[A-Z]{1,8}-?\d{2,5}[A-Z]{0,3}x?\b|\b\d{3,5}[A-Z]{0,3}i?\b|/\s*[A-Z]?\d{2,5}[A-Z]{0,3}x?\b)"
)
_DESC_SPEC_KEY_MAP = {
    "модель": "Модель",
    "аналог модели": "Аналог модели",
    "совместимость": "Совместимость",
    "совместимые модели": "Совместимость",
    "устройства": "Совместимость",
    "для принтеров": "Совместимость",
    "цвет": "Цвет",
    "цвет печати": "Цвет",
    "ресурс": "Ресурс",
    "ресурс картриджа": "Ресурс",
    "ресурс картриджа, cтр.": "Ресурс",
    "количество страниц": "Ресурс",
    "кол-во страниц при 5% заполнении а4": "Ресурс",
    "емкость": "Ёмкость",
    "ёмкость": "Ёмкость",
    "емкость лотка": "Ёмкость",
    "ёмкость лотка": "Ёмкость",
    "степлирование": "Степлирование",
    "дополнительные опции": "Дополнительные опции",
    "применение": "Применение",
    "количество в упаковке": "Количество в упаковке",
    "колличество в упаковке": "Количество в упаковке",

    "технология касания": "Технология касания",
    "технология": "Технология",
    "технология печати": "Технология",
    "разрешение": "Разрешение",
    "яркость": "Яркость",
    "контраст": "Контраст",
    "источник света": "Источник света",
    "световой источник": "Источник света",
    "оптика": "Оптика",
    "методы установки": "Методы установки",
    "способы установки": "Методы установки",
    "размер экрана": "Размер экрана",
    "дистанция": "Дистанция",
    "коэффициент проекции": "Коэффициент проекции",
    "форматы сторон": "Форматы сторон",
    "смарт-система": "Смарт-система",
    "беспроводной дисплей": "Беспроводной дисплей",
    "проводное зеркалирование": "Проводное зеркалирование",
    "интерфейсы": "Интерфейсы",
    "акустика": "Акустика",
    "питание": "Питание",
    "габариты проектора": "Габариты проектора",
    "вес проектора": "Вес проектора",
    "габариты упаковки": "Габариты упаковки",
    "вес упаковки": "Вес упаковки",
    "языки интерфейса": "Языки интерфейса",
    "комплектация": "Комплектация",
    "беспроводные модули": "Беспроводные модули",
    "беспроводные интерфейсы": "Беспроводные интерфейсы",
    "беспроводные подключения": "Беспроводные подключения",
    "беспроводные возможности": "Беспроводные возможности",
    "управление": "Управление",
    "пользовательские настройки": "Пользовательские настройки",
    "встроенные колонки": "Встроенные колонки",
    "защита замком": "Защита замком",
    "hdmi": "HDMI",
    "displayport": "DisplayPort",
    "usb-хаб": "USB-хаб",
    "usb хаб": "USB-хаб",
    "разъём для наушников": "Разъём для наушников",
    "поддержка hdcp": "Поддержка HDCP",
    "языки меню osd": "Языки меню OSD",
}

_SAFE_DESC_PARAM_KEYS = {
    "Модель",
    "Аналог модели",
    "Совместимость",
    "Технология",
    "Цвет",
    "Ресурс",
    "Ёмкость",
    "Степлирование",
    "Дополнительные опции",
    "Применение",
    "Количество в упаковке",
}


def _clean_desc_text(s: str) -> str:
    t = s or ""
    t = t.replace("\r", "\n")
    t = re.sub(r"(?i)<\s*br\s*/?>", "\n", t)
    t = re.sub(r"(?i)</\s*p\s*>", "\n", t)
    t = re.sub(r"(?i)<\s*p[^>]*>", "", t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace("\xa0", " ")
    t = unescape(t)
    t = _sanitize_desc_quality_text(t)
    return t


def _is_heading_only_value(val: str) -> bool:
    v = norm_ws(val).strip()
    if not v:
        return False
    # Служебные заголовки rich-block: "/ РАЗЪЕМЫ / УПРАВЛЕНИЕ:" и подобные
    if re.fullmatch(r"[/\\sA-ZА-ЯЁ0-9()_.:+-]{3,}", v) and ":" in v:
        return True
    if re.fullmatch(r"(?:[/\\s]*[A-ZА-ЯЁ][A-ZА-ЯЁ0-9()_.+-]*[/\\s]*){2,}:?", v):
        return True
    if re.match(r"^/\s*[A-ZА-ЯЁ]", v) and ":" in v:
        return True
    return False


def _fix_common_broken_words(s: str) -> str:
    t = s or ""
    if not t:
        return ""

    exact_repl = [
        (r"(?iu)\bос\s+новное\b", "основное"),
        (r"(?iu)\bос\s+новной\b", "основной"),
        (r"(?iu)\bос\s+новные\b", "основные"),
        (r"(?iu)\bос\s+новного\b", "основного"),
        (r"(?iu)\bос\s+новным\b", "основным"),
        (r"(?iu)\bос\s+нове\b", "основе"),
        (r"(?iu)\bос\s+нова\b", "основа"),
        (r"(?iu)\bос\s+новываясь\b", "основываясь"),
        (r"(?iu)\bос\s+новании\b", "основании"),
        (r"(?iu)\bос\s+нован\b", "основан"),
        (r"(?iu)\bос\s+ью\b", "осью"),
        (r"(?iu)\bос\s+ыпался\b", "осыпался"),
        (r"(?iu)\bос\s+ып\b", "осып"),
        (r"(?iu)\bос\s+нащена\b", "оснащена"),
        (r"(?iu)\bос\s+нащен\b", "оснащен"),
        (r"(?iu)\bОс\s+обенности\b", "Особенности"),
        (r"(?iu)\bОС\s+ОБЕННОСТИ\s+И\s+ПРЕИМУЩЕСТВА\b", "Особенности и преимущества"),
        (r"(?iu)\bОС\s+ОБЕННОСТИ\b", "Особенности"),
        (r"(?iu)\bос\s+обенности\b", "особенности"),
        (r"(?iu)\bос\s+обенно\b", "особенно"),
        (r"(?iu)\bКонтраст\s+ность\b", "Контрастность"),
        (r"(?iu)\bконтраст\s+ность\b", "контрастность"),
        (r"(?iu)\bяркость\s+ю\b", "яркостью"),
        (r"(?iu)\bразрешение\s+м\b", "разрешением"),
        (r"(?iu)\bв\s+случаи\b", "в случае"),
        (r"(?iu)\bКолличество\b", "Количество"),
        (r"(?iu)\bпроеци=ирования\b", "проецирования"),
        (r"(?iu)\bпроеци=рует\b", "проецирует"),
    ]
    for pat, rep in exact_repl:
        t = re.sub(pat, rep, t)

    stem_repl = [
        (r"(?iu)\bос\s+уществ", "осуществ"),
        (r"(?iu)\bос\s+вещ", "освещ"),
        (r"(?iu)\bос\s+тав", "остав"),
        (r"(?iu)\bос\s+тат", "остат"),
        (r"(?iu)\bос\s+тан", "остан"),
        (r"(?iu)\bос\s+вобожд", "освобожд"),
        (r"(?iu)\bос\s+вобод", "освобод"),
        (r"(?iu)\bос\s+леп", "ослеп"),
        (r"(?iu)\bос\s+лаб", "ослаб"),
    ]
    for pat, rep in stem_repl:
        t = re.sub(pat, rep, t)

    t = re.sub(
        r"([A-ZА-ЯЁ0-9][A-Za-zА-Яа-яЁё0-9/.-]{1,})\s+(?=(Canon|Xerox|HP|Hewlett|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki)\b)",
        r"\1, ",
        t,
    )
    t = re.sub(
        r"([A-Z0-9][A-Za-z0-9/-]{2,})(?=(Протяжный сканер|Сканер|Canon|Xerox|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki)\b)",
        r"\1, ",
        t,
    )
    t = re.sub(r"(?<=\d),\s+(?=\d)", ",", t)
    t = re.sub(r"\s*,\s*", ", ", t)
    return t



def _sanitize_desc_quality_text(s: str) -> str:
    t = s or ""
    if not t:
        return ""

    # Частые смешанные лат/кир техно-токены от поставщика.
    repl = [
        (r"(?iu)\b[LЛ][CС][DD]\b", "LCD"),
        (r"(?iu)\b[LЛ][EЕ][DD]\b", "LED"),
        (r"(?iu)\b[SЅ][NN][MМ][PР]\b", "SNMP"),
        (r"(?iu)\b[HН][DD][MМ][IІ]\b", "HDMI"),
        (r"(?iu)\b[Ff][RrГг][Oо0][Nп][Tт]\b", "Front"),
        (r"(?iu)\bc[иi]c[tт]e[mм]a\b", "система"),
        (r"(?iu)\bд[иi][cс]пл[eе]й\b", "дисплей"),
    ]
    for pat, rep in repl:
        t = re.sub(pat, rep, t)

    t = _fix_common_broken_words(t)

    # Повторяющиеся supplier-prose правки для AlStyle-кабелей/сетевых товаров.
    t = re.sub(r"(?iu)\bВ\s+отличии\s+от\b", "В отличие от", t)
    t = re.sub(r"(?iu)\bпри\s+прокладки\b", "при прокладке", t)
    t = re.sub(r"(?iu)\b(\d+)\s*-\s*х\b", r"\1-х", t)
    t = re.sub(r"(?iu)\b(\d+)\s*-\s*ех\b", r"\1-ех", t)
    t = re.sub(r"(?iu)([а-яё\)])\.Также\b", r"\1. Также", t)
    t = re.sub(r"(?iu)\bМГц\s+\.", "МГц.", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+(\d+(?:,\d+)?)\s*(м|мм|см|дюйм(?:ов|а)?)\b", r"\1–\2 \3", t)
    t = re.sub(r"(?iu)\bAuyo[- ]?фокус\b", "Автофокус", t)
    t = re.sub(r"(?iu)\b(\d)\.(\d+)\s*(мм|см|м|кг|г|Вт|Гц|мс|ГБ|ТБ)\b", r"\1,\2 \3", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+(Мбит/с|Гбит/с)\.\s+или\s+(\d+(?:,\d+)?)\s+(Мбит/с|Гбит/с)\.", r"\1 \2 или \3 \4", t)
    t = re.sub(r"(?iu)\b(Кабель\s+сетевой\s+SHIP\s+[A-Z0-9-]+)\s+Это\b", r"\1. Это", t)
    t = re.sub(r"(?iu)\b((?:\d+(?:,\d+)?)\s*мм)\.(?=\s|$)", r"\1", t)
    t = re.sub(r"(?iu)\b((?:\d+(?:,\d+)?)\s*МГц)\.(?=\s|$)", r"\1", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+(Мбит/с|Гбит/с)\.\s+или\s+(\d+(?:,\d+)?)\s+(Мбит/с|Гбит/с)\b", r"\1 \2 или \3 \4", t)
    t = re.sub(r"(?<=[A-Za-zА-Яа-яЁё0-9%])\.(?=[А-ЯЁA-Z])", ". ", t)

    # Нормальные пробелы после типовых spec-лейблов внутри native description.
    # Важно: добавляем пробел только когда после лейбла идёт LAT/цифра.
    # Так не ломаем обычные русские формы вроде "разрешением"/"яркостью".
    t = re.sub(
        r"(?iu)\b("
        r"(?:ОС(?=[A-Z0-9]))|Источник света|Световой источник|Оптика|Методы установки|Способы установки|Размер экрана|"
        r"Дистанция|Коэффициент проекции|Форматы сторон|Смарт-?система|Беспроводной дисплей|"
        r"Проводное зеркалирование|Интерфейсы|Акустика|Питание|Габариты проектора|Вес проектора|"
        r"Габариты упаковки|Вес упаковки|Языки интерфейса|Комплектация|Беспроводные модули|"
        r"Беспроводные интерфейсы|Беспроводные подключения|Беспроводные возможности"
        r")(?=[A-Za-zА-Яа-яЁё0-9])",
        r"\1 ",
        t,
    )

    # Для этих лейблов пробел вставляем только в безопасных случаях, чтобы не ломать
    # обычные русские формы вроде "яркостью", "контрастность", "разрешением".
    t = re.sub(r"(?iu)\bТехнология(?=(?:касания\b|печати\b|[A-Z0-9]))", "Технология ", t)
    t = re.sub(r"(?iu)\bРазрешение(?=\d)", "Разрешение ", t)
    t = re.sub(r"(?iu)\bЯркость(?=\d)", "Яркость ", t)
    t = re.sub(r"(?iu)\bКонтраст(?=\d)", "Контраст ", t)

    # Страховка для уже сломанных форм, если такое пришло от прошлой чистки.
    t = re.sub(r"(?iu)\bразрешение\s+м(?=\s+\d)", "разрешением", t)
    t = re.sub(r"(?iu)\bяркость\s+ю(?=\s+\d)", "яркостью", t)

    # Локальные quality-правки.
    t = re.sub(r"(?iu)\bplenum\s+полост", "plenum-полост", t)
    t = re.sub(r"(?im)^\s*\.\s*$", "", t)
    t = re.sub(r"(?im)^\s*Ап\s*$", "", t)
    t = re.sub(r"(?iu)Проводное\s+зеркалированиепо\b", "Проводное зеркалирование по", t)
    t = re.sub(r"(?iu)Смарт-?система(?=[A-Za-zА-Яа-яЁё0-9])", "Смарт-система ", t)
    t = re.sub(r"(?iu)^\s*ОСОБЕННОСТИ\s+И\s+ПРЕИМУЩЕСТВА:?\s*$", "Особенности и преимущества", t, flags=re.M)
    t = re.sub(r"(?iu)^\s*ИНТЕРФЕЙСЫ\s*/\s*РАЗЪ[ЕЁ]МЫ\s*/\s*УПРАВЛЕНИЕ:?\s*$", "Интерфейсы / разъёмы / управление", t, flags=re.M)
    t = re.sub(r"(?iu)^\s*АКСЕССУАРЫ:?\s*$", "Аксессуары", t, flags=re.M)
    t = re.sub(r"(?iu)^\s*ПОРТЫ\s+И\s+ПОДКЛЮЧЕНИЕ:?\s*$", "Порты и подключение", t, flags=re.M)
    t = re.sub(r"(?iu)^\s*ЗАДНЯЯ\s+ПАНЕЛЬ:?\s*$", "Задняя панель", t, flags=re.M)
    t = re.sub(r"(?iu)^\s*ПЕРЕДНЯЯ\s+ПАНЕЛЬ:?\s*$", "Передняя панель", t, flags=re.M)
    t = re.sub(r"(?iu)\bос\s+новные\s+характеристики\b", "Основные характеристики", t)
    t = re.sub(r"(?iu)\bос\s+новные\b", "основные", t)
    t = re.sub(r"(?iu)\bос\s+новной\b", "основной", t)
    t = re.sub(r"(?iu)\bос\s+нова\b", "основа", t)
    t = re.sub(r"(?iu)\bос\s+нове\b", "основе", t)
    t = re.sub(r"(?iu)\bос\s+новании\b", "основании", t)
    t = re.sub(r"(?<=\d),\s+(?=\d)", ",", t)
    t = re.sub(r"(?iu)\b(\d{3,4})\s+(\d{3,4})(?=(?:\s*@|\s*(?:пикс|dpi|px|Гц|кд(?:/м²|\s*м2)?|\)|$)))", r"\1×\2", t)
    t = re.sub(r"(?iu)\b(\d+)\s{1,}(\d+)\s*Вт\b", r"\1 × \2 Вт", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гбит\s+с\b", r"\1 Гбит/с", t)
    t = re.sub(r"(?iu)\bкд\s*м2\b", "кд/м²", t)
    t = re.sub(r"(?iu)\b(\d+)\s+порта?\s+(\d+)\s+Type-([AC])\b", r"\1 порта: \2 × Type-\3", t)
    t = re.sub(r"(?iu)\b(\d+)\s+Type-([AC])\b", r"\1 × Type-\2", t)
    t = re.sub(r"(?iu)\b(\d),\s+(\d{1,3})\s+(мм|см|м|кг|г|Вт|Гц|мс|ГБ|ТБ)\b", r"\1,\2 \3", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Мбит\s+сек\b", r"\1 Мбит/с", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гбит\s+сек\b", r"\1 Гбит/с", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Мегабит/сек\b", r"\1 Мбит/с", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гигабит/сек\b", r"\1 Гбит/с", t)
    t = re.sub(r"(?iu)\bЕсть\s+разъ[её]м\s+для\s+наушников\.?", "Разъём для наушников: есть.", t)
    t = re.sub(r"(?iu)\bПоддержка\s+HDCP\.?", "Поддержка HDCP: есть.", t)
    t = re.sub(r"(?im)^\s*[.#]?[A-Za-z][A-Za-z0-9_-]*\s*\{[^{}]+\}\s*$", "", t)
    t = re.sub(r"(?iu)\bСовместимость:\s*Для\s*,\s*", "Совместимость: ", t)
    t = re.sub(r"(?iu)\bдополнтельно\b", "дополнительно", t)
    t = re.sub(r"(?iu)\bопцонально\b", "опционально", t)
    t = re.sub(r"(?iu)\bсистемой\s+управления\s+питание\s*м\b", "системой управления питанием", t)
    t = re.sub(r"!{2,}", "!", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _sanitize_native_desc(s: str) -> str:
    t = s or ""
    if not t:
        return ""
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    # Двойные HTML-энтити от поставщика вроде &amp;gt; / &gt; не должны попадать в raw/final.
    t = t.replace("&amp;gt;", "&gt;")
    t = unescape(t)
    # Убираем служебные хвосты-строки, состоящие только из '>' или '&gt;'.
    t = re.sub(r"(?im)^\s*>+\s*$", "", t)
    t = re.sub(r"(?im)^\s*&gt;\s*$", "", t)
    # CSS/служебные строки поставщика не должны попадать в raw/final.
    t = re.sub(r"(?im)^\s*[.#]?[A-Za-z][A-Za-z0-9_-]*\s*\{[^{}]+\}\s*$", "", t)
    # Косметика для raw: лишние пробелы внутри скобок.
    t = re.sub(r"\(\s+", "(", t)
    t = re.sub(r"\s+\)", ")", t)
    t = _sanitize_desc_quality_text(t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _canon_desc_spec_key(k: str) -> str:
    kk = norm_ws(k).casefold()
    return _DESC_SPEC_KEY_MAP.get(kk, norm_ws(k))


def _normalize_tech_value(s: str) -> str:
    t = norm_ws(s)
    if not t:
        return ""
    # Безопасная техно-нормализация только для значений, не для всего description.
    t = re.sub(r"(?<=\d),\s+(?=\d)", ",", t)
    t = re.sub(r"(?iu)\b(\d{3,4})\s*[xх*×]\s*(\d{3,4})(?=(?:\s*@|\s*(?:пикс|dpi|px|Гц|кд(?:/м²|\s*м2)?|$)))", r"\1×\2", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+(\d+(?:,\d+)?)\s*(м|мм|см|дюйм(?:ов|а)?)\b", r"\1–\2 \3", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гбит\s+с\b", r"\1 Гбит/с", t)
    t = re.sub(r"(?iu)\bAuyo[- ]?фокус\b", "Автофокус", t)
    t = re.sub(r"(?iu)\b(\d)\.(\d+)\s*(мм|см|м|кг|г|Вт|Гц|мс|ГБ|ТБ)\b", r"\1,\2 \3", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+(Мбит/с|Гбит/с)\.\s+или\s+(\d+(?:,\d+)?)\s+(Мбит/с|Гбит/с)\.", r"\1 \2 или \3 \4", t)
    t = re.sub(r"(?iu)\b(Кабель\s+сетевой\s+SHIP\s+[A-Z0-9-]+)\s+Это\b", r"\1. Это", t)
    t = re.sub(r"(?iu)\b((?:\d+(?:,\d+)?)\s*мм)\.(?=\s|$)", r"\1", t)
    t = re.sub(r"(?iu)\b((?:\d+(?:,\d+)?)\s*МГц)\.(?=\s|$)", r"\1", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+(Мбит/с|Гбит/с)\.\s+или\s+(\d+(?:,\d+)?)\s+(Мбит/с|Гбит/с)\b", r"\1 \2 или \3 \4", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Мбит\s+сек\b", r"\1 Мбит/с", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гбит\s+сек\b", r"\1 Гбит/с", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Мегабит/сек\b", r"\1 Мбит/с", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гигабит/сек\b", r"\1 Гбит/с", t)
    t = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+кд\s*(?:/\s*м²|м2)\b", r"\1 кд/м²", t)
    t = re.sub(r"(?iu)\b(\d+)\s*[xх×]\s*(\d+)\s*Вт\b", r"\1 × \2 Вт", t)
    t = re.sub(r"(?iu)\b(\d+)\s+Type-([AC])\b", r"\1 × Type-\2", t)
    t = re.sub(r"(?iu)\b(\d+)\s+порта?\s+(\d+)\s*×?\s*Type-([AC])\b", r"\1 порта: \2 × Type-\3", t)
    t = re.sub(r"(?iu)\b(\d),\s+(\d{1,3})\s+(мм|см|м|кг|г|Вт|Гц|мс|ГБ|ТБ)\b", r"\1,\2 \3", t)
    return t


def _drop_broken_canon_compat_tail(s: str) -> str:
    t = norm_ws(s)
    if not t:
        return ""

    parts = [p.strip() for p in re.split(r"\s*,\s*", t) if p.strip()]
    if not parts:
        return ""

    last = parts[-1]
    broken_last_patterns = [
        r"(?iu)^Canon\s+imageRUNNE$",
        r"(?iu)^Canon\s+imageRUNNER\s+ADV$",
        r"(?iu)^Canon\s+imageRUNNER\s+ADVANCE$",
        r"(?iu)^Canon\s+imagePROGRAF\s+\d{2,4}Can$",
        r"(?iu)^Canon\s+imageFORMULA\s+[A-Z0-9-]*Can$",
        r"(?iu)^Canon\s+imageCLASS\s+[A-Z0-9-]*Can$",
    ]
    if any(re.match(p, last) for p in broken_last_patterns):
        parts.pop()

    # Если source был без запятых и хвост остался в конце целой строки.
    out = ", ".join(parts)
    out = re.sub(r"(?iu),?\s*Canon\s+imageRUNNE\s*$", "", out).strip(" ,;.-")
    out = re.sub(r"(?iu),?\s*Canon\s+imageRUNNER\s+ADV(?:ANCE)?\s*$", "", out).strip(" ,;.-")
    out = re.sub(r"(?iu),?\s*Canon\s+imagePROGRAF\s+\d{2,4}Can\s*$", "", out).strip(" ,;.-")
    return norm_ws(out)



def _dedupe_slash_tail_models(s: str) -> str:
    t = norm_ws(s)
    if not t or "/" not in t:
        return t

    m = re.match(r"^(.*?\s)([^\s,]+(?:/[^\s,]+)+)$", t)
    if not m:
        return t

    prefix = m.group(1)
    tail = m.group(2)
    parts = [x.strip() for x in tail.split('/') if x.strip()]
    if len(parts) < 2:
        return t

    uniq: list[str] = []
    seen: set[str] = set()
    for part in parts:
        key = part.casefold()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(part)

    return prefix + '/'.join(uniq)



def _sanitize_param_value(key: str, val: str) -> str:
    v = norm_ws(val)
    if not v:
        return ""

    v = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Мбит\s+сек\b", r"\1 Мбит/с", v)
    v = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гбит\s+сек\b", r"\1 Гбит/с", v)
    v = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Мегабит/сек\b", r"\1 Мбит/с", v)
    v = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гигабит/сек\b", r"\1 Гбит/с", v)

    # Обрезаем случайно склеенные секции
    v = re.split(
        r"(?i)\b(Преимущества|Комплектация|Условия гарантии|Примечание|Примечания|Особенности|Описание)\b",
        v,
        maxsplit=1,
    )[0].strip(" ;,.-")
    if not v:
        return ""

    kcf = norm_ws(key).casefold()
    v = _fix_common_broken_words(v)
    if kcf == "технология":
        v = re.sub(r"(?iu)^печати\s*:\s*", "", v)
    if kcf == "контраст":
        v = re.sub(r"(?iu)^ность\s*:\s*", "", v)

    if _is_heading_only_value(v):
        return ""

    if kcf == "совместимость":
        v = re.sub(r"(?i)^совместим(?:а|о|ы)?\s+с\s+", "", v).strip()
        v = re.sub(r"(?i)^для\s*,\s*", "", v).strip()
        v = re.sub(r"(?i)^для\s+совместимых\s+(?:устройств|принтеров(?:\s+и\s+мфу)?|мфу|аппаратов)\s+", "", v).strip()
        v = re.sub(r"(?i)^для\s+(?:устройств|принтеров(?:\s+и\s+мфу)?|мфу|аппаратов)\s+", "", v).strip()
        v = re.sub(r"(?i)^для\s*,\s*", "", v).strip()
        v = re.sub(r"(?i)^устройства?\s*,?\s*", "", v).strip()
        v = re.sub(
            r"([A-ZА-ЯЁ0-9][A-Za-zА-Яа-яЁё0-9/.-]{1,})\s+(?=(Canon|Xerox|HP|Hewlett|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki)\b)",
            r"\1, ",
            v,
        )
        v = re.sub(
            r"([A-Z0-9][A-Za-z0-9/-]{2,})(?=(Протяжный сканер|Сканер|Canon|Xerox|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki)\b)",
            r"\1, ",
            v,
        )
        v = re.sub(r"\s*/\s*", "/", v)
        v = re.sub(r"(?:,?\s*(?:&gt;|&amp;gt;|>))+\s*$", "", v).strip(" ,;.-")
        # Убираем явный битый хвост, пришедший уже обрезанным от поставщика.
        # Ничего не угадываем и не дописываем — فقط отбрасываем последний мусорный кусок.
        v = _drop_broken_canon_compat_tail(v)
        # Страховка для старого паттерна вроде "... 610Can" на конце.
        v = re.sub(r"(?iu)(\d)(?:Can|Xer|Eps|Bro|Ric|Pan|Lex|Kon|Min|Oki|Kyo|Hew)$", r"\1", v)
        v = re.sub(r"\s*,\s*", ", ", v)
        v = _dedupe_slash_tail_models(v)
        v = norm_ws(v)

    if kcf == "ёмкость":
        v = re.sub(r"(?i)^[её]мкость(?:\s+лотка)?\s*[-:–—]\s*", "", v).strip()

    if kcf in {"встроенные колонки", "hdmi", "displayport", "usb-хаб", "управление", "пользовательские настройки", "разъём для наушников", "поддержка hdcp", "языки меню osd", "интерфейсы"}:
        v = re.sub(r"(?<=\d),\s+(?=\d)", ",", v)
        v = re.sub(r"(?iu)\b(\d+)\s+(\d+)\s*Вт\b", r"\1 × \2 Вт", v)
        v = _normalize_tech_value(v)
        if _is_heading_only_value(v):
            return ""

    if kcf == "разъём для наушников":
        if re.search(r"(?iu)\bесть\b", v):
            v = "есть"
        else:
            v = re.sub(r"(?iu)^есть\.?$", "есть", v)
    if kcf == "поддержка hdcp":
        if re.search(r"(?iu)\bесть\b", v):
            v = "есть"
        else:
            v = re.sub(r"(?iu)^есть\.?$", "есть", v)

    if kcf == "ресурс":
        # Убираем обрезанные хвосты из source, чтобы не тащить мусор в final
        v = re.sub(r"(?i)\.\s*Ресурс указан в соответствии.*$", "", v).strip(" ;,.-")
        v = re.sub(r"(?i)Ресурс указан в соответствии.*$", "", v).strip(" ;,.-")
        v = re.sub(r"(?i)\.\s*ISO\s*/?\s*IEC\s*\d{4,6}\.?\s*[A-Za-zА-Яа-яЁё]?$", "", v).strip(" ;,.-")
        v = re.sub(r"(?<=[A-Za-zА-Яа-яЁё])\s+[A-Za-zА-Яа-яЁё]$", "", v)

    return norm_ws(v)


def _iter_desc_lines(text: str) -> list[str]:
    out: list[str] = []
    for raw in text.splitlines():
        ln = raw.strip(" 	-–—•")
        if not norm_ws(ln):
            continue
        if ln in {">", "&gt;", "&amp;gt;"}:
            continue
        out.append(ln)
    return out


def _is_label_only_line(raw: str) -> bool:
    ln = norm_ws(raw)
    if not ln:
        return False
    if _DESC_COMPAT_LABEL_ONLY_RE.match(ln):
        return True
    if _DESC_SPEC_START_RE.match(ln) or _DESC_SPEC_STOP_RE.match(ln):
        return True
    return False


def _join_compat_lines(lines: list[str]) -> str:
    parts: list[str] = []
    for raw in lines:
        ln = norm_ws(raw)
        if not ln:
            continue
        if _DESC_COMPAT_LABEL_ONLY_RE.match(ln):
            continue
        parts.append(ln)
    if not parts:
        return ""
    s = ", ".join(parts)
    s = re.sub(
        r"([A-ZА-ЯЁ0-9][A-Za-zА-Яа-яЁё0-9/.-]{1,})\s+(?=(Canon|Xerox|HP|Hewlett|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki)\b)",
        r"\1, ",
        s,
    )
    s = re.sub(
        r"([A-Z0-9][A-Za-z0-9/-]{2,})(?=(Протяжный сканер|Сканер|Canon|Xerox|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki)\b)",
        r"\1, ",
        s,
    )
    return norm_ws(s)


def _extract_multiline_compat_pairs(lines: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    i = 0
    n = len(lines)
    while i < n:
        cur = norm_ws(lines[i])
        if not cur:
            i += 1
            continue
        if not _DESC_COMPAT_LABEL_ONLY_RE.match(cur):
            i += 1
            continue

        j = i + 1
        buf: list[str] = []
        while j < n:
            nxt = norm_ws(lines[j])
            if not nxt:
                break
            if _DESC_SPEC_STOP_RE.match(nxt) or _DESC_SPEC_START_RE.match(nxt):
                break
            parsed = _parse_desc_spec_line(nxt)
            if parsed and parsed[0] != "Совместимость":
                break
            if _DESC_COMPAT_LABEL_ONLY_RE.match(nxt):
                j += 1
                continue
            if re.match(r"(?i)^(Производитель|Устройство|Секция аппарата|Технология печати|Гарантия)\s*$", nxt):
                break
            buf.append(nxt)
            j += 1

        cand = _join_compat_lines(buf)
        if cand and _looks_like_compatibility_value(cand):
            out.append(("Совместимость", cand))
        i = max(j, i + 1)
    return out


def _extract_multiline_tech_print_pairs(lines: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    i = 0
    n = len(lines)
    while i < n:
        cur = norm_ws(lines[i])
        if not cur:
            i += 1
            continue
        if not _DESC_TECH_PRINT_LABEL_ONLY_RE.match(cur):
            i += 1
            continue

        j = i + 1
        while j < n and not norm_ws(lines[j]):
            j += 1
        if j >= n:
            break

        nxt = norm_ws(lines[j])
        if (
            nxt
            and not _DESC_SPEC_START_RE.match(nxt)
            and not _DESC_SPEC_STOP_RE.match(nxt)
            and not _DESC_COMPAT_LABEL_ONLY_RE.match(nxt)
            and not _DESC_TECH_PRINT_LABEL_ONLY_RE.match(nxt)
            and not _is_heading_only_value(nxt)
        ):
            out.append(("Технология", nxt))
        i = max(j + 1, i + 1)
    return out


def _parse_desc_spec_line(raw: str) -> tuple[str, str] | None:
    ln = norm_ws(raw)
    if not ln:
        return None
    if re.fullmatch(r"(?iu)(Интерфейсы\s*/\s*разъ[её]мы\s*/\s*управление|Аксессуары|Порты\s+и\s+подключение|Задняя\s+панель|Передняя\s+панель):?", ln):
        return None

    m = _DESC_SPEC_LINE_RE.match(raw)
    if not m:
        compact = re.sub(r"\t+", "  ", raw)
        compact = re.sub(r"\s{3,}", "  ", compact)
        m = _DESC_SPEC_LINE_RE.match(compact)
    if m:
        return (_canon_desc_spec_key(m.group(1)), norm_ws(m.group(2)))

    m = _DESC_COMPAT_LINE_RE.match(raw)
    if m:
        return ("Совместимость", norm_ws(m.group(1)))

    if _DESC_TECH_PRINT_LABEL_ONLY_RE.match(ln):
        return None

    m = _PROJECTOR_RICH_LINE_RE.match(raw)
    if m:
        key = _canon_desc_spec_key(m.group(1))
        val = norm_ws(m.group(2))
        if key == "Технология" and val.casefold() == "печати":
            return None
        if _is_heading_only_value(val):
            return None
        return (key, val)

    m = _MONITOR_RICH_LINE_RE.match(raw)
    if m:
        val = norm_ws(m.group(2))
        if _is_heading_only_value(val):
            return None
        return (_canon_desc_spec_key(m.group(1)), val)

    return None



def _looks_like_compatibility_value(val: str) -> bool:
    v = norm_ws(val)
    if not v or len(v) < 6:
        return False
    if not _COMPAT_BRAND_HINT_RE.search(v):
        return False
    if not _COMPAT_MODEL_HINT_RE.search(v):
        return False
    return True


def _extract_sentence_compat_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    for rx in (_DESC_COMPAT_SENTENCE_RE, _DESC_FOR_DEVICES_SENTENCE_RE):
        for m in rx.finditer(text):
            cand = norm_ws(m.group(1))
            if not cand:
                continue
            cand = re.split(
                r"(?i)\b(Преимущества|Комплектация|Условия гарантии|Примечание|Примечания|Особенности|Описание)\b",
                cand,
                maxsplit=1,
            )[0].strip(" ;,.-")
            if not _looks_like_compatibility_value(cand):
                continue
            out.append(("Совместимость", cand))

    return out


def _validate_desc_pair(key: str, val: str, schema: dict[str, Any]) -> tuple[str, str] | None:
    if not key or not val:
        return None

    drop = {str(x).casefold() for x in (schema.get("drop_keys_casefold") or [])}
    rules = schema.get("key_rules") or {}
    require_letter = bool(rules.get("require_letter", True))
    max_len = int(rules.get("max_len", 60))
    max_words = int(rules.get("max_words", 9))

    if key.casefold() in drop or key.casefold() in ("код нкт",):
        return None
    if key not in _SAFE_DESC_PARAM_KEYS:
        return None
    if not _key_quality_ok(key, require_letter=require_letter, max_len=max_len, max_words=max_words):
        return None

    val2 = _apply_value_normalizers(key, val, schema)
    if not val2:
        return None

    return (key, val2)


def _extract_desc_spec_pairs(desc_src: str, schema: dict[str, Any]) -> list[tuple[str, str]]:
    text = _clean_desc_text(desc_src)
    if not text.strip():
        return []

    # Для AlStyle берём только строгий блок характеристик и только безопасные ключи.
    # Не парсим весь description целиком и не выдёргиваем совместимость из обычных фраз,
    # чтобы не ломать проекторы/мониторы и не нарушать mutate_params=false.
    m = _DESC_SPEC_START_RE.search(text)
    if not m:
        return []

    block = text[m.end():]
    stop = _DESC_SPEC_STOP_RE.search(block)
    if stop:
        block = block[:stop.start()]

    block_lines = _iter_desc_lines(block)
    candidates: list[tuple[str, str]] = []
    for ln in block_lines:
        pair = _parse_desc_spec_line(ln)
        if pair:
            candidates.append(pair)
    candidates.extend(_extract_multiline_tech_print_pairs(block_lines))
    candidates.extend(_extract_multiline_compat_pairs(block_lines))

    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for key, val in candidates:
        checked = _validate_desc_pair(key, val, schema)
        if not checked:
            continue
        sig = (checked[0].casefold(), checked[1].casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append(checked)

    return out


def _merge_params(base_params: list[tuple[str, str]], extra_params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = list(base_params)
    seen = {(norm_ws(k).casefold(), norm_ws(v).casefold()) for k, v in base_params}
    seen_keys = {norm_ws(k).casefold() for k, _ in base_params}

    for k, v in extra_params:
        kcf = norm_ws(k).casefold()
        sig = (kcf, norm_ws(v).casefold())
        if sig in seen:
            continue
        if kcf in seen_keys:
            continue
        out.append((k, v))
        seen.add(sig)
        seen_keys.add(kcf)

    return out


def _fetch_xml(url: str, *, timeout: int, login: str | None, password: str | None) -> str:
    auth = (login, password) if (login and password) else None
    r = requests.get(url, timeout=timeout, auth=auth)
    r.raise_for_status()
    return r.text


def main() -> int:
    url = (os.getenv("ALSTYLE_URL") or ALSTYLE_URL_DEFAULT).strip()
    out_file = (os.getenv("OUT_FILE") or ALSTYLE_OUT_DEFAULT).strip()
    raw_out = (os.getenv("RAW_OUT_FILE") or ALSTYLE_RAW_OUT_DEFAULT).strip()
    watch_report = (os.getenv("WATCH_REPORT_FILE") or WATCH_REPORT_DEFAULT).strip()
    encoding = (os.getenv("OUTPUT_ENCODING") or "utf-8").strip() or "utf-8"

    env_hour = (os.getenv("SCHEDULE_HOUR_ALMATY") or "").strip()  # legacy env, будет сравнение после чтения policy.yml
    timeout = int(os.getenv("HTTP_TIMEOUT", "90"))

    login = os.getenv("ALSTYLE_LOGIN")
    password = os.getenv("ALSTYLE_PASSWORD")

    cfg_dir = Path(os.getenv("ALSTYLE_CFG_DIR", CFG_DIR_DEFAULT))
    filter_cfg = _read_yaml(cfg_dir / (os.getenv("ALSTYLE_FILTER_FILE") or FILTER_FILE_DEFAULT))
    schema_cfg = _read_yaml(cfg_dir / (os.getenv("ALSTYLE_SCHEMA_FILE") or SCHEMA_FILE_DEFAULT))
    policy_cfg = _read_yaml(cfg_dir / (os.getenv("ALSTYLE_POLICY_FILE") or POLICY_FILE_DEFAULT))

    # schedule hour: источник истины — policy.yml
    hour = int((policy_cfg.get("schedule_hour_almaty") or 1))
    if env_hour:
        try:
            eh = int(env_hour)
            if eh != hour:
                print(f"[build_alstyle] WARN: ignoring SCHEDULE_HOUR_ALMATY={eh}; policy.yml schedule_hour_almaty={hour}")
        except Exception:
            print(f"[build_alstyle] WARN: bad SCHEDULE_HOUR_ALMATY={env_hour!r}; using policy.yml schedule_hour_almaty={hour}")

    placeholder_picture = (
        os.getenv("PLACEHOLDER_PICTURE")
        or policy_cfg.get("placeholder_picture")
        or "https://placehold.co/800x800/png?text=No+Photo"
    )

    fallback_ids = {str(x) for x in (filter_cfg.get("category_ids") or [])}
    allowed = _parse_id_set(os.getenv("ALSTYLE_CATEGORY_IDS"), fallback_ids)

    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour=hour)

    xml_text = _fetch_xml(url, timeout=timeout, login=login, password=password)
    root = ET.fromstring(xml_text)

    offers_in = root.findall(".//offer")
    before = len(offers_in)

    out_offers: list[OfferOut] = []
    in_true = 0
    in_false = 0

    supplier_name = (policy_cfg.get("supplier") or "AlStyle").strip()
    vendor_blacklist = {str(x).casefold() for x in (policy_cfg.get("vendor_blacklist_casefold") or ["alstyle"])}
    watch_source: dict[str, dict[str, str]] = {}
    watch_out: set[str] = set()

    for o in offers_in:
        cat = norm_ws(_t(o.find("categoryId")))
        raw_id = norm_ws(o.get("id") or _t(o.find("vendorCode")))
        name = norm_ws(_t(o.find("name")))
        oid_probe = raw_id if raw_id.upper().startswith(ALSTYLE_ID_PREFIX) else f"{ALSTYLE_ID_PREFIX}{raw_id}" if raw_id else ""
        if oid_probe in ALSTYLE_WATCH_OIDS:
            watch_source[oid_probe] = {"categoryId": cat, "name": name}
        if allowed and (not cat or cat not in allowed):
            continue
        name = re.sub(r"\(\s+", "(", name)
        name = re.sub(r"\s+\)", ")", name)
        if not name or not raw_id:
            continue

        oid = raw_id if raw_id.upper().startswith(ALSTYLE_ID_PREFIX) else f"{ALSTYLE_ID_PREFIX}{raw_id}"

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

        pics = _collect_pictures(o, placeholder_picture)

        params = _collect_params(o, schema_cfg)

        vendor_src = norm_ws(_t(o.find("vendor")))
        if vendor_src and vendor_src.casefold() in vendor_blacklist:
            vendor_src = ""

        desc_src = _sanitize_native_desc(_t(o.find("description")) or "")
        params = _merge_params(params, _extract_desc_spec_pairs(desc_src, schema_cfg))

        price_in = safe_int(_t(o.find("purchase_price")))
        if price_in is None:
            price_in = safe_int(_t(o.find("price")))
        price = compute_price(price_in)

        watch_out.add(oid)
        out_offers.append(
            OfferOut(
                oid=oid,
                available=available,
                name=name,
                price=price,
                pictures=pics,
                vendor=vendor_src,
                params=params,
                native_desc=desc_src,
            )
        )

    after = len(out_offers)
    watch_messages: list[str] = []
    for wid in sorted(ALSTYLE_WATCH_OIDS):
        if wid not in watch_source:
            msg = f"[build_alstyle] ROOT_CAUSE: watched offer not present in supplier XML: {wid}"
            print(msg)
            watch_messages.append(msg)
        elif wid not in watch_out:
            info = watch_source[wid]
            cat = info.get('categoryId', '')
            reason = "filtered_by_category" if (allowed and (not cat or cat not in allowed)) else "skipped_after_parse"
            msg = (
                f"[build_alstyle] ROOT_CAUSE: watched offer missing in output: {wid}; reason={reason}; "
                f"categoryId={cat!r}; name={info.get('name', '')!r}"
            )
            print(msg)
            watch_messages.append(msg)
        else:
            info = watch_source[wid]
            msg = (
                f"[build_alstyle] WATCH_OK: {wid}; categoryId={info.get('categoryId', '')!r}; "
                f"name={info.get('name', '')!r}"
            )
            print(msg)
            watch_messages.append(msg)

    try:
        if watch_report:
            rp = Path(watch_report)
            rp.parent.mkdir(parents=True, exist_ok=True)
            rp.write_text("\n".join(watch_messages) + ("\n" if watch_messages else ""), encoding="utf-8")
    except Exception as e:
        print(f"[build_alstyle] WARN: failed to write watch report {watch_report!r}: {e}")

    out_offers.sort(key=lambda x: x.oid)

    write_cs_feed_raw(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=raw_out,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        currency_id="KZT",
    )

    changed = write_cs_feed(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        public_vendor=os.getenv("PUBLIC_VENDOR", "CS").strip() or "CS",
        currency_id="KZT",
    )

    print(
        f"[build_alstyle] OK | version={BUILD_ALSTYLE_VERSION} | offers_in={before} | offers_out={after} | "
        f"in_true={in_true} | in_false={in_false} | changed={'yes' if changed else 'no'} | file={out_file}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
