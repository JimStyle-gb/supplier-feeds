# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/params_xml.py
ComPortal main param-first extractor.

Роль:
- взять raw param[] из source;
- убрать supplier/service мусор;
- нормализовать ключи и значения;
- собрать основной supplier-param set для raw offer.

В модуле НЕТ:
- category filter;
- picture policy;
- description builder;
- core pricing/writer logic.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml


CONFIG_POLICY_PATH = Path(__file__).resolve().parent / "config" / "policy.yml"


def safe_str(x: Any) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _load_policy(config_path: Path = CONFIG_POLICY_PATH) -> Dict[str, Any]:
    """Прочитать supplier policy."""
    if not config_path.exists():
        return {}
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    return data if isinstance(data, dict) else {}


def load_drop_param_names(config_path: Path = CONFIG_POLICY_PATH) -> set[str]:
    """Вернуть set param names, которые нужно дропать."""
    policy = _load_policy(config_path)
    values = (((policy.get("param_policy") or {}).get("drop_param_names")) or [])
    return {norm_spaces(str(x)) for x in values if norm_spaces(str(x))}


def norm_param_name(name: str) -> str:
    """Нормализовать имя param."""
    n = norm_spaces(name)
    if not n:
        return ""

    mapping = {
        "Тип печати": "Технология печати",
        "Объем": "Объём",
        "Беспроводная связь": "Беспроводные интерфейсы",
        "Разъемы/порты": "Порты",
        "Формфактор": "Форм-фактор",
    }
    return mapping.get(n, n)


def norm_param_value(name: str, value: str) -> str:
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


def params_to_map(params: List[Dict[str, str]]) -> Dict[str, str]:
    """Собрать map param name -> value."""
    out: Dict[str, str] = {}
    for p in params or []:
        name = norm_spaces(safe_str(p.get("name")))
        value = norm_spaces(safe_str(p.get("value")))
        if name and value:
            out[name] = value
    return out


def category_leaf_name(raw_offer: Dict[str, Any]) -> str:
    """Имя листовой source-категории."""
    return norm_spaces(raw_offer.get("raw_category_name") or "")


def infer_type(raw_offer: Dict[str, Any], pmap: Dict[str, str]) -> str:
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
    if "батареи" in leaf or "аккумуляторы" in leaf:
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


def extract_codes(raw_offer: Dict[str, Any], pmap: Dict[str, str]) -> str:
    """Поднять коды/модель товара."""
    for key in ("Модель", "Партномер", "Артикул", "Номер"):
        value = norm_spaces(pmap.get(key, ""))
        if value:
            return value

    title = norm_spaces(raw_offer.get("name") or raw_offer.get("title") or "")
    m = re.search(r"\(([A-Za-z0-9#\-/\.]+)\)\s*$", title)
    if m:
        return m.group(1)

    return ""


def _collect_clean_source_params(raw_offer: Dict[str, Any]) -> Dict[str, str]:
    """Собрать очищенные supplier params из raw_params."""
    drop_names = load_drop_param_names()
    raw_params = raw_offer.get("raw_params") or raw_offer.get("params") or []

    out: Dict[str, str] = {}
    for p in raw_params:
        raw_name = norm_spaces(safe_str(p.get("name")))
        raw_value = norm_spaces(safe_str(p.get("value")))

        if not raw_name or not raw_value:
            continue
        if raw_name in drop_names:
            continue

        name = norm_param_name(raw_name)
        if not name:
            continue

        value = norm_param_value(name, raw_value)
        if not value:
            continue

        out[name] = value

    return out


def extract_clean_params(raw_offer: Dict[str, Any]) -> Tuple[List[Dict[str, str]], Dict[str, str]]:
    """Собрать основной supplier-param set для CS raw."""
    pmap = _collect_clean_source_params(raw_offer)

    vendor = norm_spaces(raw_offer.get("vendor") or "")
    model = norm_spaces(raw_offer.get("model") or pmap.get("Модель", ""))
    ptype = infer_type(raw_offer, pmap)
    codes = extract_codes(raw_offer, pmap)

    out_map: Dict[str, str] = {}

    if ptype:
        out_map["Тип"] = ptype
    if vendor:
        out_map["Для бренда"] = vendor
    if codes:
        out_map["Коды"] = codes
    if model:
        out_map["Модель"] = model

    # Часто полезные поля для техники / ИБП / расходки.
    for key in (
        "Функция",
        "Формат печати",
        "Разрешение",
        "Скорость печати ч/б",
        "Скорость печати цветной",
        "Диагональ",
        "Максимальное разрешение",
        "Тип матрицы",
        "Частота обновления",
        "Время отклика",
        "Модель процессора",
        "Серия процессора",
        "Оперативная память",
        "Объем жесткого диска",
        "Тип жесткого диска",
        "Операционная система",
        "Версия операционной системы",
        "Марка чипсета видеокарты",
        "Модель чипсета видеокарты",
        "Мощность (VA)",
        "Мощность (W)",
        "Форм-фактор",
        "Стабилизатор (AVR)",
        "Типовая продолжительность работы при 100% нагрузке, мин",
        "Выходные соединения",
        "Порты",
        "Беспроводные интерфейсы",
        "Цвет",
        "Технология печати",
        "Ресурс",
        "Объём",
        "Номер",
        "Гарантия",
        "Применение",
        "Дополнительная информация",
        "Версия",
        "Языковая версия",
    ):
        value = norm_spaces(pmap.get(key, ""))
        if value:
            out_map[key] = value

    if out_map.get("Модель") and out_map.get("Коды") == out_map.get("Модель"):
        pass

    ordered_names = [
        "Тип",
        "Для бренда",
        "Коды",
        "Модель",
        "Функция",
        "Формат печати",
        "Разрешение",
        "Скорость печати ч/б",
        "Скорость печати цветной",
        "Диагональ",
        "Максимальное разрешение",
        "Тип матрицы",
        "Частота обновления",
        "Время отклика",
        "Модель процессора",
        "Серия процессора",
        "Оперативная память",
        "Объем жесткого диска",
        "Тип жесткого диска",
        "Операционная система",
        "Версия операционной системы",
        "Марка чипсета видеокарты",
        "Модель чипсета видеокарты",
        "Мощность (VA)",
        "Мощность (W)",
        "Форм-фактор",
        "Стабилизатор (AVR)",
        "Типовая продолжительность работы при 100% нагрузке, мин",
        "Выходные соединения",
        "Порты",
        "Беспроводные интерфейсы",
        "Цвет",
        "Технология печати",
        "Ресурс",
        "Объём",
        "Номер",
        "Гарантия",
        "Применение",
        "Дополнительная информация",
        "Версия",
        "Языковая версия",
    ]

    clean_params: List[Dict[str, str]] = []
    used = set()

    for name in ordered_names:
        value = norm_spaces(out_map.get(name, ""))
        if value:
            clean_params.append({"name": name, "value": value})
            used.add(name)

    for name, value in pmap.items():
        if name in used:
            continue
        if not value:
            continue
        clean_params.append({"name": name, "value": value})

    return clean_params, out_map


__all__ = [
    "CONFIG_POLICY_PATH",
    "load_drop_param_names",
    "params_to_map",
    "category_leaf_name",
    "infer_type",
    "extract_codes",
    "extract_clean_params",
]
