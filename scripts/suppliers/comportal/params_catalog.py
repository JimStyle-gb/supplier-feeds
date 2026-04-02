# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/params_catalog.py
ComPortal main param-first extractor.

Задача модуля:
- взять raw param[] из source;
- убрать service-мусор;
- нормализовать ключи/значения;
- собрать основной supplier-param set для raw offer.

В модуле НЕТ:
- category filter;
- final description builder;
- ценовой логики;
- порядка XML writer core;
- вычисления финального offer id (префикс CP будет в builder).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple


DROP_PARAM_NAMES = {
    "Цена по запросу (для сортировки)",
    "Акция (для сортировки)",
    "Бренд",
}

PARAM_RENAME_MAP = {
    "Тип печати": "Технология печати",
    "Чернильный": "Струйный",
}


def safe_str(x: Any) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def norm_value(name: str, value: str) -> str:
    """Нормализовать значение param."""
    v = norm_spaces(value)
    if not v:
        return ""

    if name == "Технология печати":
        up = v.upper()
        if "ЛАЗЕР" in up:
            return "Лазерная"
        if "СТРУЙ" in up or "ЧЕРНИЛ" in up:
            return "Струйная"

    if name == "Цвет":
        repl = {
            "Черный": "Чёрный",
            "Желтый": "Жёлтый",
        }
        return repl.get(v, v)

    return v


def norm_param_name(name: str) -> str:
    """Нормализовать имя param."""
    n = norm_spaces(name)
    if not n:
        return ""

    if n in DROP_PARAM_NAMES:
        return ""

    if n == "Бренд":
        return ""

    if n == "Модель":
        return "Модель"
    if n == "Номер":
        return "Номер"
    if n == "Партномер":
        return "Партномер"
    if n == "Артикул":
        return "Артикул"
    if n == "Цвет":
        return "Цвет"
    if n == "Ресурс":
        return "Ресурс"
    if n == "Объем":
        return "Объём"
    if n == "Тип печати":
        return "Технология печати"
    if n == "Гарантия":
        return "Гарантия"
    if n == "Дополнительная информация":
        return "Дополнительная информация"
    if n == "Применение":
        return "Применение"
    if n == "Версия":
        return "Версия"
    if n == "Языковая версия":
        return "Языковая версия"

    return n


def category_leaf_name(raw_offer: Dict[str, Any]) -> str:
    """Имя листовой source-категории."""
    return norm_spaces(raw_offer.get("raw_category_name") or "")


def infer_type(raw_offer: Dict[str, Any], params_map: Dict[str, str]) -> str:
    """Определить товарный тип для raw offer."""
    leaf = category_leaf_name(raw_offer).lower()
    title = norm_spaces(raw_offer.get("name") or raw_offer.get("title") or "").lower()

    if "лазерные монохромные мфу" in leaf:
        return "МФУ"
    if "лазерные цветные мфу" in leaf:
        return "МФУ"
    if "струйные мфу" in leaf:
        return "МФУ"
    if "лазерные монохромные принтеры" in leaf:
        return "Принтер"
    if "лазерные цветные принтеры" in leaf:
        return "Принтер"
    if "струйные принтеры" in leaf:
        return "Принтер"
    if "широкоформатные принтеры" in leaf:
        return "Широкоформатный принтер"
    if "сканеры" in leaf:
        return "Сканер"
    if "ноутбуки" in leaf:
        return "Ноутбук"
    if "мониторы" in leaf:
        return "Монитор"
    if "моноблоки" in leaf:
        return "Моноблок"
    if "настольные пк" in leaf:
        return "Настольный ПК"
    if "рабочие станции" in leaf:
        return "Рабочая станция"
    if "проекторы" in leaf:
        return "Проектор"
    if "картриджи для лазерных устройств" in leaf:
        return "Картридж"
    if "картриджи для струйных устройств" in leaf:
        return "Картридж"
    if "картриджи для широкоформатных устройств" in leaf:
        return "Картридж"
    if "тонеры" in leaf:
        return "Тонер"
    if "прочие расходные материалы" in leaf:
        return "Расходный материал"
    if "батареи. аккумуляторы" in leaf:
        return "Батарея"
    if leaf == "ибп":
        return "ИБП"
    if leaf == "стабилизаторы":
        return "Стабилизатор"

    if title.startswith("картридж"):
        return "Картридж"
    if title.startswith("тонер"):
        return "Тонер"
    if title.startswith("ноутбук"):
        return "Ноутбук"
    if title.startswith("монитор"):
        return "Монитор"
    if title.startswith("проектор"):
        return "Проектор"

    return ""


def extract_codes(raw_offer: Dict[str, Any], params_map: Dict[str, str]) -> str:
    """Поднять код/модель расходки."""
    for key in ("Модель", "Партномер", "Артикул", "Номер"):
        v = norm_spaces(params_map.get(key, ""))
        if v:
            return v

    title = norm_spaces(raw_offer.get("name") or raw_offer.get("title") or "")
    m = re.search(r"\(([A-Za-z0-9#\-/\.]+)\)\s*$", title)
    if m:
        return m.group(1)
    return ""


def extract_clean_params(raw_offer: Dict[str, Any]) -> Tuple[List[Dict[str, str]], Dict[str, str]]:
    """Собрать основной очищенный набор param для CS raw."""
    raw_params = raw_offer.get("raw_params") or raw_offer.get("params") or []
    params_map: Dict[str, str] = {}

    for p in raw_params:
        raw_name = norm_spaces(safe_str(p.get("name")))
        raw_value = norm_spaces(safe_str(p.get("value")))
        name = norm_param_name(raw_name)
        if not name:
            continue
        value = norm_value(name, raw_value)
        if not value:
            continue
        params_map[name] = value

    vendor = norm_spaces(raw_offer.get("vendor") or "")
    model = norm_spaces(raw_offer.get("model") or params_map.get("Модель", ""))
    type_label = infer_type(raw_offer, params_map)
    codes = extract_codes(raw_offer, params_map)

    out_map: Dict[str, str] = {}

    if type_label:
        out_map["Тип"] = type_label
    if vendor:
        out_map["Для бренда"] = vendor
    if codes:
        out_map["Коды"] = codes
    if model:
        out_map["Модель"] = model

    for key in (
        "Цвет",
        "Технология печати",
        "Ресурс",
        "Объём",
        "Гарантия",
        "Дополнительная информация",
        "Применение",
        "Версия",
        "Языковая версия",
    ):
        value = norm_spaces(params_map.get(key, ""))
        if value:
            out_map[key] = value

    # Пропустить дубли model/codes.
    if out_map.get("Модель") and out_map.get("Коды") == out_map.get("Модель"):
        pass

    ordered_names = [
        "Тип",
        "Для бренда",
        "Коды",
        "Модель",
        "Цвет",
        "Технология печати",
        "Ресурс",
        "Объём",
        "Гарантия",
        "Дополнительная информация",
        "Применение",
        "Версия",
        "Языковая версия",
    ]

    clean_params: List[Dict[str, str]] = []
    for name in ordered_names:
        value = norm_spaces(out_map.get(name, ""))
        if value:
            clean_params.append({"name": name, "value": value})

    # Добавить остальные незапрещённые param в хвост.
    used = set(ordered_names) | {"Бренд"}
    for name, value in params_map.items():
        if name in used:
            continue
        if name in DROP_PARAM_NAMES:
            continue
        if not value:
            continue
        clean_params.append({"name": name, "value": value})

    return clean_params, out_map


__all__ = [
    "DROP_PARAM_NAMES",
    "PARAM_RENAME_MAP",
    "extract_clean_params",
    "infer_type",
    "extract_codes",
]
