# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/params_xml.py

XML params pipeline для AlStyle.
Только cleanup родных XML <param>.

v123:
- усиливает post-clean для Совместимость;
- вырезает мусорные хвосты типа '&gt;' и dangling brand tail;
- отбрасывает ложные значения Ёмкость/Ёмкость лотка вида
  'для подачи бумаги', 'для бумаги', 'для документов';
- сохраняет текущую модель selective-clean без изменения core.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any

from cs.util import norm_ws
from suppliers.alstyle.compat import clean_compatibility_text, dedupe_code_series_text, sanitize_param_value


_RE_HAS_LETTER = re.compile(r"[A-Za-zА-Яа-яЁё]")
_RE_LETTER_SLASH_LETTER = re.compile(r"([A-Za-zА-Яа-яЁё])\s*/\s*([A-Za-zА-Яа-яЁё])")

_COLOR_STOP_RE = re.compile(
    r"(?iu)(?:\b(?:Тип\s+чернил|Ресурс(?:\s+картриджа)?|Количество\s+страниц|Секция\s+аппарата|"
    r"Совместимость|Устройства|Количество\s+цветов|серия)\b|,\s*серия\b|,\s*(?:Vivobook|Vector|Gaming|Go)\b)"
)
_TECH_VALUE_RE = re.compile(
    r"(?iu)\b("
    r"Лазерная(?:\s+монохромная|\s+цветная)?|"
    r"Светодиодная(?:\s+монохромная|\s+цветная)?|"
    r"Струйная|Термоструйная|Матричная|Термосублимационная"
    r")\b"
)
_TECH_STOP_RE = re.compile(
    r"(?iu)\b(?:Количество\s+цветов|Тип\s+чернил|Ресурс(?:\s+картриджа)?|Совместимость|"
    r"Устройства|Об(?:ъ|ь)ем\s+картриджа|Секция\s+аппарата|серия)\b"
)
_RESOURCE_VALUE_RE = re.compile(
    r"(?iu)\b\d[\d\s.,]*(?:\s*(?:стандартн(?:ых|ые)?\s+страниц(?:ы)?(?:\s+в\s+среднем)?|стр\.?|страниц|copies|pages))\b"
)
_RESOURCE_NUMBER_ONLY_RE = re.compile(r"(?iu)^\d[\d\s.,]*(?:\s*(?:стр\.?|страниц))?$")
_MODEL_GARBAGE_RE = re.compile(
    r"(?iu)\b(?:зависит\s+от\s+конфигурации|модель\s+зависит\s+от\s+конфигурации|определяется\s+конфигурацией)\b"
)
_CAPACITY_GARBAGE_RE = re.compile(
    r"(?iu)^(?:для\s+подачи\s+бумаги|для\s+бумаги|для\s+документов|для\s+оригиналов|бумаги|документов|оригиналов)$"
)
_CAPACITY_VALID_RE = re.compile(
    r"(?iu)(?:\b\d[\d\s.,]*\s*(?:лист(?:а|ов)?|стр\.?|страниц|л)\b|\b\d[\d\s.,]*\b)"
)
_COMPAT_LEADING_NOISE_RE = re.compile(
    r"(?iu)^(?:Модель\s+[A-Z0-9-]+\s+|Совместимые\s+модели\s+|Устройства\s+|Устройство\s+)"
)
_TRAILING_HTML_GARBAGE_RE = re.compile(r"(?iu)(?:&gt;|&amp;gt;|&lt;|&amp;lt;|>)+\s*$")
_DANGLING_BRAND_TAIL_RE = re.compile(
    r"(?iu)(?:\s*/\s*|\s+)(Canon|Xerox|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark)\s*$"
)
_DANGLING_CONNECTOR_RE = re.compile(r"(?iu)(?:\s*/\s*|\s*,\s*|-+\s*)$")


def key_quality_ok(k: str, *, require_letter: bool, max_len: int, max_words: int) -> bool:
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


def normalize_warranty_to_months(v: str) -> str:
    vv = norm_ws(v)
    if not vv:
        return ""
    low = vv.casefold()
    if low in ("нет", "no", "-", "—"):
        return ""
    m = re.search(r"(\d{1,2})\s*(год|года|лет)\b", low)
    if m:
        return f"{int(m.group(1)) * 12} мес"
    if re.fullmatch(r"\d{1,3}", low):
        return f"{int(low)} мес"
    m = re.search(r"\b(\d{1,3})\b", low)
    if m and ("мес" in low or "month" in low):
        return f"{int(m.group(1))} мес"
    return vv


def normalize_tech_value(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = re.sub(r"(?iu)\bUSB\s+C\b", "USB-C", s)
    s = re.sub(r"(?iu)\bWi\s*Fi\b", "Wi-Fi", s)
    s = re.sub(r"(?iu)\bBluetooth\s*([0-9.]+)\b", r"Bluetooth \1", s)
    s = re.sub(r"(?iu)\bFull\s*HD\b", "Full HD", s)
    s = re.sub(r"(?iu)\bANSI\s*люмен\b", "ANSI люмен", s)
    return norm_ws(s)


def _strip_trailing_compat_garbage(s: str) -> str:
    out = norm_ws(s)
    if not out:
        return ""
    prev = None
    while prev != out:
        prev = out
        out = _TRAILING_HTML_GARBAGE_RE.sub("", out).strip()
        out = _DANGLING_BRAND_TAIL_RE.sub("", out).strip()
        out = _DANGLING_CONNECTOR_RE.sub("", out).strip()
    return norm_ws(out.strip(" ;,.-"))


def _normalize_color_word(word: str) -> str:
    low = norm_ws(word).casefold().replace("ё", "е")
    mapping = {
        "матовый черн": "Матовый чёрный",
        "фоточерн": "Фоточёрный",
        "черн": "Чёрный",
        "бел": "Белый",
        "сер": "Серый",
        "син": "Синий",
        "голуб": "Голубой",
        "красн": "Красный",
        "малинов": "Малиновый",
        "пурпурн": "Пурпурный",
        "желт": "Жёлтый",
        "зелен": "Зелёный",
        "оранжев": "Оранжевый",
        "фиолетов": "Фиолетовый",
        "коричнев": "Коричневый",
        "розов": "Розовый",
        "бежев": "Бежевый",
        "прозрачн": "Прозрачный",
        "серебрист": "Серебристый",
        "золотист": "Золотистый",
    }
    for pref, clean in mapping.items():
        if low.startswith(pref):
            return clean
    return norm_ws(word)


def _post_clean_color_xml_value(v: str) -> str:
    s = norm_ws(v).strip(" ;,.-")
    if not s:
        return ""
    m = _COLOR_STOP_RE.search(s)
    if m and m.start() >= 1:
        s = s[:m.start()].strip(" ;,.-")
    if not s:
        return ""
    parts = [norm_ws(x) for x in re.split(r"\s*[,/;]\s*", s) if norm_ws(x)]
    if parts and len(parts) <= 3 and all(
        re.fullmatch(r"(?iu)(?:матовый\s+черн(?:ый|ая|ое|ые)?|фоточерн(?:ый|ая|ое|ые)?|[А-Яа-яЁё-]+)", p)
        for p in parts
    ):
        return ", ".join(_normalize_color_word(p) for p in parts)
    if len(s.split()) > 4:
        return ""
    return s


def _post_clean_technology_xml_value(v: str) -> str:
    s = norm_ws(v).strip(" ;,.-")
    if not s:
        return ""
    m = _TECH_STOP_RE.search(s)
    if m and m.start() >= 1:
        s = s[:m.start()].strip(" ;,.-")
    low = s.casefold().replace("ё", "е")
    if low in {"стр", "струйная печать"}:
        return "Струйная"
    m = _TECH_VALUE_RE.search(s)
    if m:
        return norm_ws(m.group(1))
    return ""


def _post_clean_resource_xml_value(v: str) -> str:
    s = norm_ws(v).strip(" ;,.-")
    if not s:
        return ""
    m = _RESOURCE_VALUE_RE.search(s)
    if m:
        return norm_ws(m.group(0))
    if _RESOURCE_NUMBER_ONLY_RE.fullmatch(s):
        return s
    return ""


def _post_clean_compat_xml_value(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""

    s = _COMPAT_LEADING_NOISE_RE.sub("", s)
    s = clean_compatibility_text(s)
    s = dedupe_code_series_text(s)
    s = _strip_trailing_compat_garbage(s)

    return norm_ws(s.strip(" ;,.-"))


def _post_clean_model_xml_value(v: str) -> str:
    s = dedupe_code_series_text(norm_ws(v).strip(" ;,.-"))
    if not s:
        return ""
    if _MODEL_GARBAGE_RE.search(s):
        return ""
    if s.endswith(")") and not re.search(r"\([A-Za-z0-9/-]{2,}\)$", s):
        s = s.rstrip(") ")
    return norm_ws(s)


def _post_clean_capacity_xml_value(v: str) -> str:
    s = norm_ws(v).strip(" ;,.-")
    if not s:
        return ""
    if _CAPACITY_GARBAGE_RE.fullmatch(s):
        return ""
    if not _CAPACITY_VALID_RE.search(s):
        return ""
    return s


def _post_clean_xml_value(key: str, val: str) -> str:
    kcf = norm_ws(key).casefold()
    if kcf == "цвет":
        return _post_clean_color_xml_value(val)
    if kcf == "технология":
        return _post_clean_technology_xml_value(val)
    if kcf == "ресурс":
        return _post_clean_resource_xml_value(val)
    if kcf == "совместимость":
        return _post_clean_compat_xml_value(val)
    if kcf == "модель":
        return _post_clean_model_xml_value(val)
    if kcf in {"ёмкость", "емкость", "ёмкость лотка", "емкость лотка"}:
        return _post_clean_capacity_xml_value(val)
    return norm_ws(val)


def apply_value_normalizers(key: str, val: str, schema: dict[str, Any]) -> str:
    v = norm_ws(val)
    if not v:
        return ""
    vn = schema.get("value_normalizers") or {}
    ops = vn.get(key) or vn.get(key.casefold()) or []

    for op in ops:
        if op == "warranty_months":
            v = normalize_warranty_to_months(v)
        elif op == "trim_ws":
            v = norm_ws(v)

    kcf = norm_ws(key).casefold()
    if kcf not in {"совместимость", "модель", "аналог модели"}:
        v = _RE_LETTER_SLASH_LETTER.sub(r"\1 \2", v)

    v = sanitize_param_value(key, v)
    if not v:
        return ""

    v = _post_clean_xml_value(key, v)
    if not v:
        return ""

    if kcf not in {"совместимость", "модель", "аналог модели"}:
        v = normalize_tech_value(v)
        v = re.sub(r"(?<=\d),\s+(?=\d)", ",", v)
        v = re.sub(
            r"(?iu)\b(\d),(\d{1,3})\s+(мм|см|м|кг|г|Вт|Гц|мс|дюйм(?:а|ов)?|дюйма|дюймов|ГБ|ТБ)\b",
            r"\1,\2 \3",
            v,
        )
        v = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+кд\s*(?:/\s*м²|м2)\b", r"\1 кд/м²", v)
        v = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гбит\s*/?\s*с\b", r"\1 Гбит/с", v)
        v = re.sub(r"(?iu)\b(\d+)\s*[xх×]\s*(\d+)\s*Вт\b", r"\1 × \2 Вт", v)
    return norm_ws(v)


def collect_xml_params(offer_el: ET.Element, schema: dict[str, Any]) -> list[tuple[str, str]]:
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
        v0 = "".join(p.itertext()).strip()

        k = norm_ws(k0)
        v = norm_ws(v0)
        if not k or not v:
            continue

        kcf = k.casefold()
        if kcf in aliases:
            k = aliases[kcf]

        if not key_quality_ok(k, require_letter=require_letter, max_len=max_len, max_words=max_words):
            continue

        if k.casefold() in drop or k.casefold() == "код нкт":
            continue
        if k.casefold() == "назначение" and v.casefold() in {"да", "есть"}:
            continue
        if k.casefold() == "безопасность" and v.casefold() == "есть":
            continue

        v2 = apply_value_normalizers(k, v, schema)
        if not v2:
            continue

        sig = (k.casefold(), v2.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((k, v2))

    return out


# Backward-compatible aliases for already split stages.
_key_quality_ok = key_quality_ok
_normalize_warranty_to_months = normalize_warranty_to_months
_apply_value_normalizers = apply_value_normalizers
