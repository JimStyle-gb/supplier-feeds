# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/filtering.py
ComPortal category-first filtering.

Задача модуля:
- брать сырые офферы из source.py;
- пропускать только разрешённые categoryId;
- не пускать ветки Акции / Уцененные;
- вернуть удобный report для build summary.

В модуле НЕТ:
- нормализации name/vendor/model;
- semantic extraction param;
- правок description;
- ценовой логики.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Set

# Выбранный whitelist пользователя.
ALLOWED_CATEGORY_IDS: Set[str] = {
    "8052",  # Ноутбуки
    "8048",  # Мониторы
    "8030",  # Моноблоки
    "8031",  # Настольные ПК
    "8073",  # Проекторы
    "8032",  # Рабочие станции

    "8034",  # Лазерные монохромные МФУ
    "8037",  # Лазерные монохромные принтеры
    "8035",  # Лазерные цветные МФУ
    "8038",  # Лазерные цветные принтеры
    "8036",  # Сканеры
    "8039",  # Струйные МФУ
    "8074",  # Струйные принтеры
    "8040",  # Широкоформатные принтеры

    "8043",  # Картриджи для лазерных устройств
    "8044",  # Картриджи для струйных устройств
    "8046",  # Картриджи для широкоформатных устройств
    "8062",  # Прочие расходные материалы
    "8047",  # Тонеры

    "8065",  # Батареи. аккумуляторы
    "8063",  # ИБП
    "8064",  # Стабилизаторы
}

# Корни, которые намеренно не берём.
EXCLUDED_ROOT_IDS: Set[str] = {
    "8028",  # Акции
    "8029",  # Уцененные
}


def safe_str(x: Any) -> str:
    """Безопасно привести значение к строке."""
    return str(x).strip() if x is not None else ""


def _category_record(category_index: Dict[str, Dict[str, str]], cid: str) -> Dict[str, str]:
    """Вернуть запись категории или пустую заглушку."""
    return category_index.get(cid, {"id": cid, "name": "", "parent_id": "", "path": ""})


def _root_category_id(category_index: Dict[str, Dict[str, str]], cid: str) -> str:
    """Найти верхний root id для categoryId."""
    cur = safe_str(cid)
    seen: set[str] = set()
    last = cur
    while cur and cur not in seen and cur in category_index:
        seen.add(cur)
        last = cur
        parent_id = safe_str(category_index[cur].get("parent_id"))
        if not parent_id:
            return cur
        cur = parent_id
    return last


def is_allowed_offer(
    offer: Dict[str, Any],
    category_index: Dict[str, Dict[str, str]],
) -> bool:
    """Проверить, проходит ли оффер фильтр."""
    cid = safe_str(offer.get("raw_categoryId") or offer.get("categoryId"))
    if not cid:
        return False

    if cid not in ALLOWED_CATEGORY_IDS:
        return False

    root_id = _root_category_id(category_index, cid)
    if root_id in EXCLUDED_ROOT_IDS:
        return False

    return True


def filter_offers(
    offers: Iterable[Dict[str, Any]],
    category_index: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    """Отфильтровать офферы и вернуть filtered + report."""
    source_list = list(offers)
    kept: List[Dict[str, Any]] = []
    rejected_total = 0

    kept_category_counts: Dict[str, int] = {}
    rejected_category_counts: Dict[str, int] = {}

    for offer in source_list:
        cid = safe_str(offer.get("raw_categoryId") or offer.get("categoryId"))
        if is_allowed_offer(offer, category_index):
            kept.append(offer)
            if cid:
                kept_category_counts[cid] = kept_category_counts.get(cid, 0) + 1
        else:
            rejected_total += 1
            if cid:
                rejected_category_counts[cid] = rejected_category_counts.get(cid, 0) + 1

    allowed_categories_report: List[Dict[str, Any]] = []
    for cid in sorted(ALLOWED_CATEGORY_IDS):
        rec = _category_record(category_index, cid)
        allowed_categories_report.append(
            {
                "id": cid,
                "name": safe_str(rec.get("name")),
                "path": safe_str(rec.get("path")),
                "kept_count": kept_category_counts.get(cid, 0),
            }
        )

    rejected_categories_report: List[Dict[str, Any]] = []
    for cid, cnt in sorted(rejected_category_counts.items(), key=lambda x: (-x[1], x[0])):
        rec = _category_record(category_index, cid)
        rejected_categories_report.append(
            {
                "id": cid,
                "name": safe_str(rec.get("name")),
                "path": safe_str(rec.get("path")),
                "rejected_count": cnt,
            }
        )

    report = {
        "mode": "include",
        "before": len(source_list),
        "after": len(kept),
        "rejected_total": rejected_total,
        "allowed_category_count": len(ALLOWED_CATEGORY_IDS),
        "allowed_category_ids": sorted(ALLOWED_CATEGORY_IDS),
        "excluded_root_ids": sorted(EXCLUDED_ROOT_IDS),
        "allowed_categories_report": allowed_categories_report,
        "rejected_categories_report": rejected_categories_report[:20],
    }

    return {
        "offers": kept,
        "report": report,
    }


__all__ = [
    "ALLOWED_CATEGORY_IDS",
    "EXCLUDED_ROOT_IDS",
    "is_allowed_offer",
    "filter_offers",
]
