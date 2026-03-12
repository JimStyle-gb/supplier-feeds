# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/params_xml.py

AkCent supplier layer — cleanup родных XML <Param>.

Что делает:
- читает только supplier XML params;
- применяет aliases / discard / banned / key_rules из schema.yml;
- определяет kind-aware набор разрешённых ключей;
- нормализует значения по самым частым кейсам AkCent;
- лишние, но полезные ключи уводит в extra_info;
- возвращает params + extra_info + report.

Важно:
- модуль не гадает compat/codes из description;
- не лезет в core;
- не пытается делать глобальную supplier-магию вне XML params.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any, Iterable

from cs.util import fix_mixed_cyr_lat, norm_ws, safe_int


# ----------------------------- regex / const -----------------------------

_RE_HAS_LETTER = re.compile(r"[A-Za-zА-Яа-яЁё]")
_RE_CODE_TOKEN = re.compile(r"\b(?:C1[23][A-Z0-9]{6,10}|T\d{2}[A-Z]\d{2}[A-Z]?|SBID-[A-Z0-9\-]+|[A-Z]{1,4}\d{2,6}[A-Z\-]{0,6})\b")
_RE_RESOLUTION = re.compile(r"(?iu)^(\d[\d\s]{2,5})\s*[xх×]\s*(\d[\d\s]{2,5})$")
_RE_INCH = re.compile(r'(?iu)^\s*(\d{1,3}(?:[.,]\d+)?)\s*(?:"|дюйм(?:ов|а)?|inch|in)\s*$')
_RE_MM = re.compile(r"(?iu)^\s*(\d{1,4}(?:[.,]\d+)?)\s*мм\s*$")
_RE_CM = re.compile(r"(?iu)^\s*(\d{1,4}(?:[.,]\d+)?)\s*см\s*$")
_RE_MCM = re.compile(r"(?iu)^\s*(\d{1,4}(?:[.,]\d+)?)\s*мкм\s*$")
_RE_DB = re.compile(r"(?iu)^\s*(\d{1,3}(?:[.,]\d+)?)\s*дб\s*$")
_RE_WATT = re.compile(r"(?iu)^\s*(\d{1,5}(?:[.,]\d+)?)\s*в[тt]\s*$")
_RE_KG = re.compile(r"(?iu)^\s*(\d{1,4}(?:[.,]\d+)?)\s*кг\s*$")
_RE_GRM2 = re.compile(r"(?iu)^\s*(\d{1,4}(?:[.,]\d+)?)\s*г\s*/\s*м(?:2|²)\s*$")
_RE_BOOL_YES = re.compile(r"(?iu)^(?:да|yes|true|есть|supported|поддерживается)$")
_RE_BOOL_NO = re.compile(r"(?iu)^(?:нет|no|false|отсутствует|не поддерживается)$")
_RE_MONTHS = re.compile(r"(?iu)(\d{1,3})\s*(?:мес|месяц|месяца|месяцев)")
_RE_YEARS = re.compile(r"(?iu)(\d{1,2})\s*(?:год|года|лет|yr|year|years)")
_RE_PLAIN_NUM = re.compile(r"^\d{1,3}$")
_RE_NO_WARRANTY = re.compile(r"(?iu)^(?:нет|no|none|0|0\s*мес(?:\.|яцев)?|n/a)$")
_RE_SPLIT_STARS = re.compile(r"\s*\*\s*")
_RE_SPLIT_LIST = re.compile(r"\s*[,;|/]\s*")
_RE_WS = re.compile(r"\s+")
_RE_BRACKETS = re.compile(r"\(\s+|\s+\)")
_RE_HTML_TAG = re.compile(r"<[^>]+>")

_BOOL_KEYS = {
    "двусторонняя печать",
    "жк дисплей",
    "автоподатчик",
    "печать фото",
    "3d",
    "интерактивный",
}

_INTERFACE_KEYS = {
    "интерфейсы",
    "usb",
    "wi-fi",
    "wifi",
    "hdmi",
    "vga",
    "s-video",
    "displayport",
    "dvi-d",
    "ethernet",
    "hdbaset",
}

_CODE_KEYS = {"коды", "совместимые продукты"}
_COMPAT_KEYS = {"совместимость", "для устройства"}
_TYPE_KEYS = {"тип", "тип печати", "тип матрицы", "тип управления", "тип чернил", "тип расходных материалов"}
_COLOR_KEYS = {"цвет", "цвета"}

_COLOR_MAP = {
    "черный": "Чёрный",
    "черн": "Чёрный",
    "bk": "Чёрный",
    "матовый черный": "Матовый чёрный",
    "матовый черн": "Матовый чёрный",
    "фото черный": "Фоточёрный",
    "фоточерный": "Фоточёрный",
    "cyan": "Голубой",
    "голубой": "Голубой",
    "c": "Голубой",
    "magenta": "Пурпурный",
    "пурпурный": "Пурпурный",
    "m": "Пурпурный",
    "yellow": "Жёлтый",
    "желтый": "Жёлтый",
    "желтыйй": "Жёлтый",
    "y": "Жёлтый",
    "white": "Белый",
    "белый": "Белый",
    "grey": "Серый",
    "gray": "Серый",
    "серый": "Серый",
    "red": "Красный",
    "красный": "Красный",
    "green": "Зелёный",
    "зеленый": "Зелёный",
    "blue": "Синий",
    "синий": "Синий",
}

_VALUE_TITLE_CASE_KEYS = {
    "тип печати",
    "тип",
    "цветность",
    "область применения",
    "тип чернил",
    "тип расходных материалов",
}


# ----------------------------- small helpers -----------------------------


def _clean_text(value: Any) -> str:
    s = str(value or "").replace("\xa0", " ")
    s = _RE_HTML_TAG.sub(" ", s)
    s = fix_mixed_cyr_lat(s)
    s = norm_ws(s)
    s = _RE_BRACKETS.sub(lambda m: "(" if m.group(0).startswith("(") else ")", s)
    s = _RE_WS.sub(" ", s).strip(" ;,.-")
    return s


# Короткий доступ к полю объекта/dict.
def _get_field(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj.get(name)
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


# XML itertext с fallback.
def _text_of(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return _clean_text("".join(el.itertext()))


# Casefold-set из списка.
def _cf_set(values: Iterable[Any]) -> set[str]:
    return {_clean_text(v).casefold() for v in values if _clean_text(v)}


# Resolve aliases backward-safe.
def _resolve_aliases(schema_cfg: dict[str, Any]) -> dict[str, str]:
    raw = dict(schema_cfg.get("aliases") or {})
    out: dict[str, str] = {}
    for src, dst in raw.items():
        s = _clean_text(src)
        d = _clean_text(dst)
        if s and d:
            out[s.casefold()] = d
    return out


# Resolve default+kind allowlist.
def resolve_allowed_keys(schema_cfg: dict[str, Any], kind: str = "") -> set[str]:
    allow_by_kind = schema_cfg.get("allow_by_kind") or {}
    keys = list(allow_by_kind.get("default") or [])
    if kind:
        keys.extend(list(allow_by_kind.get(kind) or []))
    return {_clean_text(x) for x in keys if _clean_text(x)}


# Определяем kind по началу name.
def detect_kind_by_name(name: str, schema_cfg: dict[str, Any]) -> str:
    text = _clean_text(name)
    text_cf = text.casefold().replace("ё", "е")
    for kind, prefixes in (schema_cfg.get("kind_by_name_prefix") or {}).items():
        for prefix in prefixes or []:
            pref = _clean_text(prefix)
            if pref and text_cf.startswith(pref.casefold().replace("ё", "е")):
                return str(kind)
    return ""


# Общая проверка качества ключа.
def key_quality_ok(key: str, schema_cfg: dict[str, Any]) -> bool:
    rules = schema_cfg.get("key_rules") or {}
    k = _clean_text(key)
    if not k:
        return False

    banned = _cf_set(rules.get("banned_exact") or [])
    if k.casefold() in banned:
        return False

    if bool(rules.get("must_contain_letter", True)) and not _RE_HAS_LETTER.search(k):
        return False

    max_len = int(rules.get("max_len") or 0)
    if max_len > 0 and len(k) > max_len:
        return False

    max_words = int(rules.get("max_words") or 0)
    if max_words > 0 and len(k.split()) > max_words:
        return False

    return True


# Нормализация ключа: cleanup + alias.
def normalize_param_key(raw_key: Any, schema_cfg: dict[str, Any]) -> str:
    key = _clean_text(raw_key)
    if not key:
        return ""
    aliases = _resolve_aliases(schema_cfg)
    key = aliases.get(key.casefold(), key)
    key = key.replace("г/м2", "г/м²")
    key = key.replace("  ", " ")
    return _clean_text(key)


# Нормализация цвета.
def _normalize_color_list(value: str) -> str:
    parts = [p for p in _RE_SPLIT_STARS.split(value) if p] if "*" in value else [p for p in _RE_SPLIT_LIST.split(value) if p]
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        p = _clean_text(part).casefold().replace("ё", "е")
        if not p:
            continue
        canon = _COLOR_MAP.get(p, "")
        if not canon:
            canon = p.capitalize().replace("черный", "Чёрный").replace("желтый", "Жёлтый")
        if canon.casefold() in seen:
            continue
        seen.add(canon.casefold())
        out.append(canon)
    return ", ".join(out)


# Нормализация интерфейсов.
def _normalize_interfaces(value: str) -> str:
    raw_parts = _RE_SPLIT_STARS.split(value) if "*" in value else _RE_SPLIT_LIST.split(value)
    mapping = {
        "wifi": "Wi-Fi",
        "wi fi": "Wi-Fi",
        "wi-fi": "Wi-Fi",
        "usb": "USB",
        "hdmi": "HDMI",
        "vga": "VGA",
        "s-video": "S-Video",
        "svideo": "S-Video",
        "displayport": "DisplayPort",
        "dvi-d": "DVI-D",
        "ethernet": "Ethernet",
        "ethernet/fast ethernet": "Ethernet/Fast Ethernet",
        "fast ethernet": "Fast Ethernet",
        "rj-45": "RJ-45",
        "hdbaset": "HDBaseT",
    }
    out: list[str] = []
    seen: set[str] = set()
    for part in raw_parts:
        p = _clean_text(part)
        if not p:
            continue
        p_cf = p.casefold()
        canon = mapping.get(p_cf, p)
        canon = canon.replace("WiFi", "Wi-Fi").replace("Wifi", "Wi-Fi")
        canon = canon.replace("Usb", "USB")
        canon = canon.replace("Hdmi", "HDMI")
        canon = canon.replace("Vga", "VGA")
        canon = canon.replace("Displayport", "DisplayPort")
        canon = canon.replace("Dvi-D", "DVI-D")
        canon = canon.replace("Hdbaset", "HDBaseT")
        if canon.casefold() in seen:
            continue
        seen.add(canon.casefold())
        out.append(canon)
    return ", ".join(out)


# Нормализация кодов/артикулов.
def _normalize_codes(value: str) -> str:
    parts = _RE_SPLIT_STARS.split(value) if "*" in value else _RE_SPLIT_LIST.split(value)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        p = _clean_text(part)
        if not p:
            continue
        if len(p) <= 1:
            continue
        if not _RE_CODE_TOKEN.search(p):
            # для AkCent оставляем и короткие модельные токены, если там есть цифры и буквы
            if not (re.search(r"[A-Za-zА-Яа-яЁё]", p) and re.search(r"\d", p)):
                continue
        p = p.replace(" ", "")
        if p.casefold() in seen:
            continue
        seen.add(p.casefold())
        out.append(p)
    return ", ".join(out)


# Нормализация совместимости / для устройства.
def _normalize_compat(value: str) -> str:
    s = _clean_text(value)
    if not s:
        return ""
    s = s.replace("*", ", ")
    s = s.replace("/ /", "/")
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip(" ;,.-")


# Нормализация гарантии в месяцы.
def _normalize_warranty(value: str) -> str:
    s = _clean_text(value)
    if not s or _RE_NO_WARRANTY.match(s):
        return ""
    m = _RE_YEARS.search(s)
    if m:
        return f"{int(m.group(1)) * 12} мес."
    m = _RE_MONTHS.search(s)
    if m:
        return f"{int(m.group(1))} мес."
    if _RE_PLAIN_NUM.fullmatch(s):
        return f"{int(s)} мес."
    return s


# Нормализация простых yes/no параметров.
def _normalize_bool(value: str) -> str:
    s = _clean_text(value)
    if not s:
        return ""
    if _RE_BOOL_YES.match(s):
        return "Да"
    if _RE_BOOL_NO.match(s):
        return "Нет"
    return s


# Нормализация размеров/весов/единиц.
def _normalize_unitish(value: str) -> str:
    s = _clean_text(value)
    if not s:
        return ""
    m = _RE_RESOLUTION.match(s)
    if m:
        return f"{m.group(1).replace(' ', '')} x {m.group(2).replace(' ', '')}"
    for rx, unit in ((_RE_INCH, '"'), (_RE_MM, " мм"), (_RE_CM, " см"), (_RE_MCM, " мкм"), (_RE_DB, " дБ"), (_RE_WATT, " Вт"), (_RE_KG, " кг"), (_RE_GRM2, " г/м²")):
        m = rx.match(s)
        if m:
            num = m.group(1).replace(",", ".")
            return f"{num}{unit}"
    return s


# Умеренная чистка значения по ключу.
def normalize_param_value(key: str, raw_value: Any, schema_cfg: dict[str, Any]) -> str:
    value = _clean_text(raw_value)
    if not value:
        return ""

    kcf = key.casefold()

    if kcf == "гарантия":
        return _normalize_warranty(value)

    if kcf in _BOOL_KEYS:
        return _normalize_bool(value)

    if kcf in _INTERFACE_KEYS or kcf == "интерфейсы":
        return _normalize_interfaces(value)

    if kcf in _CODE_KEYS:
        return _normalize_codes(value)

    if kcf in _COMPAT_KEYS:
        return _normalize_compat(value)

    if kcf in _COLOR_KEYS:
        return _normalize_color_list(value)

    if kcf in _TYPE_KEYS:
        low = value.casefold().replace("ё", "е")
        if low == "струйная":
            return "Струйная"
        if low == "лазерная":
            return "Лазерная"
        return value[0].upper() + value[1:] if value else value

    if kcf in {"диагональ", "диагональ (см)", "яркость", "контрастность", "контрастность (динамическая)", "разрешение", "разрешение печати, dpi", "разрешение сканера, dpi", "максимальное разрешение, dpi", "интерполяционное разрешение, dpi", "скорость печати (a4)", "минимальная плотность бумаги, г/м²", "максимальная плотность бумаги, г/м²", "время отклика", "частота обновления", "размер", "ширина", "высота", "габариты", "вес", "вес (в упак.)", "вес (без упак.)"}:
        return _normalize_unitish(value)

    # fallback value_normalizers из schema, если появятся позже.
    vns = schema_cfg.get("value_normalizers") or {}
    ops = list(vns.get(key) or []) + list(vns.get(key.casefold()) or []) + list(vns.get("*") or [])
    out = value
    for op in ops:
        op_name = _clean_text(op).casefold()
        if op_name in {"warranty_months", "warranty"}:
            out = _normalize_warranty(out)
        elif op_name in {"interfaces", "split_interfaces"}:
            out = _normalize_interfaces(out)
        elif op_name in {"codes", "code_list"}:
            out = _normalize_codes(out)
        elif op_name in {"compat", "compatibility"}:
            out = _normalize_compat(out)
        elif op_name in {"unitish", "numeric_units"}:
            out = _normalize_unitish(out)
        elif op_name in {"bool", "boolean"}:
            out = _normalize_bool(out)
        elif op_name in {"colors", "color_list"}:
            out = _normalize_color_list(out)
        out = _clean_text(out)
        if not out:
            break

    if kcf in {x.casefold() for x in _VALUE_TITLE_CASE_KEYS}:
        low = out.casefold()
        if low == "струйная":
            return "Струйная"
        if low == "лазерная":
            return "Лазерная"

    return out


# Извлекаем сырые пары Param name/value из source backward-safe.
def iter_source_param_pairs(src: Any) -> list[tuple[str, str]]:
    # 1) Если уже есть готовый params list.
    params = _get_field(src, "params", "xml_params")
    if isinstance(params, list):
        out: list[tuple[str, str]] = []
        for item in params:
            if isinstance(item, tuple) and len(item) >= 2:
                out.append((_clean_text(item[0]), _clean_text(item[1])))
            elif isinstance(item, dict):
                out.append((_clean_text(item.get("name")), _clean_text(item.get("value"))))
            elif hasattr(item, "name") and hasattr(item, "value"):
                out.append((_clean_text(getattr(item, "name")), _clean_text(getattr(item, "value"))))
        return [(k, v) for k, v in out if k and v]

    # 2) Если есть offer_el / raw element.
    offer_el = _get_field(src, "offer_el", "raw_offer_el", "el")
    if isinstance(offer_el, ET.Element):
        out = []
        for prm in offer_el.findall("Param"):
            name = _clean_text(prm.get("name"))
            value = _text_of(prm)
            if name and value:
                out.append((name, value))
        return out

    return []


# Dedup params по ключу+значению с сохранением порядка.
def _append_unique(pairs: list[tuple[str, str]], key: str, value: str, seen: set[tuple[str, str]]) -> None:
    item = (key, value)
    if item in seen:
        return
    seen.add(item)
    pairs.append(item)


# Главная функция XML params pipeline.
def extract_xml_params(
    src: Any,
    *,
    schema_cfg: dict[str, Any] | None = None,
    kind: str = "",
    unknown_to_extra_info: bool = True,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]], dict[str, Any]]:
    schema = dict(schema_cfg or {})
    kind_name = kind or detect_kind_by_name(str(_get_field(src, "name") or ""), schema)

    discard_exact = _cf_set(schema.get("discard_exact") or [])
    allow_keys = resolve_allowed_keys(schema, kind_name)

    kept: list[tuple[str, str]] = []
    extra_info: list[tuple[str, str]] = []
    seen_kept: set[tuple[str, str]] = set()
    seen_extra: set[tuple[str, str]] = set()

    stats = {
        "source_param_count": 0,
        "kept_param_count": 0,
        "extra_info_count": 0,
        "dropped_empty": 0,
        "dropped_discard_key": 0,
        "dropped_bad_key": 0,
        "dropped_bad_value": 0,
        "dropped_not_allowed": 0,
        "kind": kind_name,
    }

    for raw_key, raw_val in iter_source_param_pairs(src):
        stats["source_param_count"] += 1

        key = normalize_param_key(raw_key, schema)
        if not key:
            stats["dropped_bad_key"] += 1
            continue

        if key.casefold() in discard_exact:
            stats["dropped_discard_key"] += 1
            continue

        if not key_quality_ok(key, schema):
            stats["dropped_bad_key"] += 1
            continue

        value = normalize_param_value(key, raw_val, schema)
        if not value:
            stats["dropped_bad_value"] += 1
            continue

        if allow_keys and key not in allow_keys:
            if unknown_to_extra_info:
                _append_unique(extra_info, key, value, seen_extra)
                stats["extra_info_count"] = len(extra_info)
            else:
                stats["dropped_not_allowed"] += 1
            continue

        _append_unique(kept, key, value, seen_kept)
        stats["kept_param_count"] = len(kept)

    return kept, extra_info, stats


# Alias под будущий builder, если захочется более явное имя.
def collect_xml_params(
    src: Any,
    *,
    schema_cfg: dict[str, Any] | None = None,
    kind: str = "",
    unknown_to_extra_info: bool = True,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]], dict[str, Any]]:
    return extract_xml_params(
        src,
        schema_cfg=schema_cfg,
        kind=kind,
        unknown_to_extra_info=unknown_to_extra_info,
    )
