# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/builder.py
ComPortal builder layer.

Роль:
- взять raw offer после source/filter/normalize;
- собрать clean raw offer под CS;
- сформировать стабильный CP-prefixed id/vendorCode;
- собрать минимально информативный native_desc из name + params + category provenance.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List

from .compat import apply_compat_cleanup
from .desc_clean import clean_native_text, clean_title_for_desc
from .desc_extract import fill_missing_from_title
from .normalize import normalize_basics
from .params_xml import extract_clean_params
from .pictures import pick_main_picture, pick_pictures

SUPPLIER_PREFIX = "CP"


def safe_str(x: Any) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def norm_spaces(s: str) -> str:
    """Сжать пробелы и NBSP."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def to_int(x: Any) -> int:
    """Безопасно привести цену к int."""
    s = norm_spaces(safe_str(x))
    if not s:
        return 0
    digits = re.sub(r"[^0-9]+", "", s)
    if not digits:
        return 0
    try:
        return int(digits)
    except Exception:
        return 0


def make_cp_code(raw_offer: Dict[str, Any]) -> str:
    """Стабильный vendorCode / id с префиксом CP."""
    vendor_code = norm_spaces(raw_offer.get("raw_vendorCode") or raw_offer.get("vendorCode"))
    raw_id = norm_spaces(raw_offer.get("raw_id") or raw_offer.get("id"))

    base = vendor_code or raw_id
    base = re.sub(r"[^A-Za-z0-9]+", "", base)
    if not base:
        base = "000000"

    if base.upper().startswith(SUPPLIER_PREFIX):
        return base
    return f"{SUPPLIER_PREFIX}{base}"


def _param_map(params: List[Dict[str, str]]) -> Dict[str, str]:
    """Собрать map param name -> value."""
    out: Dict[str, str] = {}
    for p in params:
        name = norm_spaces(safe_str(p.get("name")))
        value = norm_spaces(safe_str(p.get("value")))
        if name and value:
            out[name] = value
    return out


def _join_nonempty(parts: List[str], sep: str = ", ") -> str:
    """Склеить непустые куски."""
    return sep.join([norm_spaces(p) for p in parts if norm_spaces(p)])


def _desc_for_printing_device(pmap: Dict[str, str]) -> str:
    """Нарратив для печатной техники."""
    bits: List[str] = []
    if pmap.get("Функция"):
        bits.append(pmap["Функция"])
    if pmap.get("Формат печати"):
        bits.append(f"формат {pmap['Формат печати']}")
    if pmap.get("Разрешение"):
        bits.append(f"разрешение {pmap['Разрешение']}")
    if pmap.get("Скорость печати ч/б"):
        bits.append(f"скорость ч/б {pmap['Скорость печати ч/б']} стр/мин")
    if pmap.get("Скорость печати цветной"):
        bits.append(f"скорость цветной печати {pmap['Скорость печати цветной']} стр/мин")
    if pmap.get("Порты"):
        bits.append(f"интерфейсы {pmap['Порты']}")
    return _join_nonempty(bits)


def _desc_for_monitor(pmap: Dict[str, str]) -> str:
    """Нарратив для монитора."""
    bits: List[str] = []
    if pmap.get("Диагональ"):
        bits.append(f"диагональ {pmap['Диагональ']}")
    if pmap.get("Максимальное разрешение"):
        bits.append(f"разрешение {pmap['Максимальное разрешение']}")
    if pmap.get("Тип матрицы"):
        bits.append(f"матрица {pmap['Тип матрицы']}")
    if pmap.get("Частота обновления"):
        bits.append(f"частота {pmap['Частота обновления']} Гц")
    if pmap.get("Время отклика"):
        bits.append(f"отклик {pmap['Время отклика']} мс")
    if pmap.get("Порты"):
        bits.append(f"интерфейсы {pmap['Порты']}")
    return _join_nonempty(bits)


def _desc_for_computer(pmap: Dict[str, str]) -> str:
    """Нарратив для ноутбука/ПК/моноблока/рабочей станции."""
    bits: List[str] = []
    cpu = _join_nonempty([pmap.get("Серия процессора", ""), pmap.get("Модель процессора", "")], " ")
    if cpu:
        bits.append(f"процессор {cpu}")
    if pmap.get("Оперативная память"):
        bits.append(f"оперативная память {pmap['Оперативная память']}")
    storage = _join_nonempty([pmap.get("Объем жесткого диска", ""), pmap.get("Тип жесткого диска", "")], " ")
    if storage:
        bits.append(f"накопитель {storage}")
    if pmap.get("Диагональ"):
        bits.append(f"диагональ {pmap['Диагональ']}")
    if pmap.get("Максимальное разрешение"):
        bits.append(f"разрешение {pmap['Максимальное разрешение']}")
    os_name = _join_nonempty([pmap.get("Операционная система", ""), pmap.get("Версия операционной системы", "")], " ")
    if os_name:
        bits.append(f"ОС {os_name}")
    gpu = _join_nonempty([pmap.get("Марка чипсета видеокарты", ""), pmap.get("Модель чипсета видеокарты", "")], " ")
    if gpu:
        bits.append(f"видеокарта {gpu}")
    return _join_nonempty(bits)


def _desc_for_power(pmap: Dict[str, str]) -> str:
    """Нарратив для ИБП/стабилизаторов/батарей."""
    bits: List[str] = []
    va = pmap.get("Мощность (VA)", "")
    w = pmap.get("Мощность (W)", "")
    if va or w:
        bits.append(_join_nonempty([f"{va} VA" if va else "", f"{w} W" if w else ""], " / "))
    if pmap.get("Форм-фактор"):
        bits.append(f"форм-фактор {pmap['Форм-фактор']}")
    if pmap.get("Стабилизатор (AVR)"):
        bits.append(f"AVR {pmap['Стабилизатор (AVR)']}")
    if pmap.get("Типовая продолжительность работы при 100% нагрузке, мин"):
        bits.append(
            "время работы при полной нагрузке "
            f"{pmap['Типовая продолжительность работы при 100% нагрузке, мин']} мин"
        )
    if pmap.get("Выходные соединения"):
        bits.append(f"выходы {pmap['Выходные соединения']}")
    return _join_nonempty(bits)


def _desc_for_consumable(pmap: Dict[str, str]) -> str:
    """Нарратив для расходки."""
    bits: List[str] = []
    if pmap.get("Технология печати"):
        bits.append(f"технология печати {pmap['Технология печати'].lower()}")
    if pmap.get("Цвет"):
        bits.append(f"цвет {pmap['Цвет'].lower()}")
    if pmap.get("Ресурс"):
        bits.append(f"ресурс {pmap['Ресурс']}")
    if pmap.get("Объём"):
        bits.append(f"объём {pmap['Объём']}")
    if pmap.get("Номер"):
        bits.append(f"номер {pmap['Номер']}")
    if pmap.get("Применение"):
        bits.append(pmap["Применение"])
    return _join_nonempty(bits)


def _make_highlights(pmap: Dict[str, str]) -> str:
    """Собрать основной narrative по типу товара."""
    ptype = norm_spaces(pmap.get("Тип", "")).lower()

    if ptype in {"мфу", "принтер", "сканер", "проектор", "широкоформатный принтер"}:
        return _desc_for_printing_device(pmap)
    if ptype == "монитор":
        return _desc_for_monitor(pmap)
    if ptype in {"ноутбук", "моноблок", "настольный пк", "рабочая станция"}:
        return _desc_for_computer(pmap)
    if ptype in {"ибп", "стабилизатор", "батарея"}:
        return _desc_for_power(pmap)
    if ptype in {"картридж", "тонер", "расходный материал"}:
        return _desc_for_consumable(pmap)

    bits: List[str] = []
    for key in (
        "Технология печати",
        "Цвет",
        "Ресурс",
        "Диагональ",
        "Максимальное разрешение",
        "Оперативная память",
        "Объем жесткого диска",
        "Мощность (VA)",
        "Мощность (W)",
        "Гарантия",
    ):
        if pmap.get(key):
            bits.append(f"{key.lower()} {pmap[key]}")
    return _join_nonempty(bits)


def build_native_desc(name: str, clean_params: List[Dict[str, str]], raw_offer: Dict[str, Any]) -> str:
    """Собрать supplier-side native_desc для raw YML."""
    pmap = _param_map(clean_params)
    parts: List[str] = [clean_title_for_desc(name)]

    highlights = _make_highlights(pmap)
    if highlights:
        parts.append("Характеристики: " + highlights + ".")

    category_path = norm_spaces(raw_offer.get("raw_category_path", ""))
    if category_path:
        parts.append(f"Категория поставщика: {category_path}.")

    return clean_native_text("\n".join([x for x in parts if x]).strip())


def build_offer(raw_offer: Dict[str, Any]) -> Dict[str, Any]:
    """Собрать один clean raw offer под CS."""
    normalized = normalize_basics(raw_offer)

    clean_params, _ = extract_clean_params(normalized)
    clean_params = fill_missing_from_title(normalized, clean_params)
    clean_params = apply_compat_cleanup(clean_params)

    cp_code = make_cp_code(normalized)
    pictures = pick_pictures(normalized)
    main_picture = pick_main_picture(normalized)

    name = norm_spaces(normalized.get("name") or normalized.get("title") or "")
    vendor = norm_spaces(normalized.get("vendor", ""))
    price = to_int(normalized.get("price_raw") or normalized.get("raw_price_text"))

    return {
        "id": cp_code,
        "vendorCode": cp_code,
        "name": name,
        "price": price,
        "picture": main_picture,
        "pictures": pictures,
        "vendor": vendor,
        "currencyId": norm_spaces(normalized.get("raw_currencyId") or normalized.get("currencyId") or "KZT") or "KZT",
        "available": bool(normalized.get("available", True)),
        "categoryId": "",
        "params": clean_params,
        "native_desc": build_native_desc(name, clean_params, normalized),
        "url": norm_spaces(normalized.get("raw_url") or normalized.get("url")),
        "source_category_id": norm_spaces(normalized.get("raw_categoryId") or normalized.get("categoryId")),
        "source_category_name": norm_spaces(normalized.get("raw_category_name")),
        "source_category_path": norm_spaces(normalized.get("raw_category_path")),
        "model": norm_spaces(normalized.get("model")),
    }


def build_offers(raw_offers: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Построить список clean raw offers."""
    out: List[Dict[str, Any]] = []
    for raw_offer in raw_offers:
        built = build_offer(raw_offer)
        if not built.get("name"):
            continue
        if not built.get("vendorCode"):
            continue
        out.append(built)
    return out


__all__ = [
    "SUPPLIER_PREFIX",
    "build_offer",
    "build_offers",
    "make_cp_code",
    "build_native_desc",
]
