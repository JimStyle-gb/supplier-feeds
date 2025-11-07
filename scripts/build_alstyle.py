#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
scripts/build_alstyle.py — генератор docs/alstyle.yml из фида поставщика Al-Style.

Шаги обработки (в порядке выполнения):
  1) ФИЛЬТР по <categoryId> (используем список ID ПОСТАВЩИКА).
  2) МИГРАЦИЯ доступности: перенос <available> внутрь атрибута <offer available="true|false">,
     удаление самого тега <available>.
  3) ЧИСТКА shop-префикса: удалить всё, что находится внутри <shop> ДО тега <offers>.
  4) ЧИСТКА офферов: удалить из каждого <offer> теги <price>, <url>, <quantity>, <quantity_in_stock>.
  5) ВЕНДОРКОД/ID: добавить префикс "AS" к <vendorCode> (без дефиса) и записать то же значение
     в атрибут <offer id="...">.

Выход: docs/alstyle.yml в Windows-1251. Структура YML сохраняется за исключением описанных правок.
Внешние пояснения минимальны — подробные комментарии внутри кода.
'''
from __future__ import annotations

import pathlib
import sys
import time
import xml.etree.ElementTree as ET

import requests
from requests.auth import HTTPBasicAuth

# ------------------------ Конфигурация ------------------------
SUPPLIER_URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"

# Жёстко вшитые логин/пароль (по просьбе пользователя)
USERNAME = "info@complex-solutions.kz"
PASSWORD = "Aa123456"

# Список categoryId ПОСТАВЩИКА (ОДНА CSV-строка, компактно)
ALLOWED_CATEGORY_IDS_CSV = "3540,3541,3542,3543,3544,3545,3566,3567,3569,3570,3580,3688,3708,3721,3722,4889,4890,4895,5017,5075,5649,5710,5711,5712,5713,21279,21281,21291,21356,21367,21368,21369,21370,21371,21372,21451,21498,21500,21501,21572,21573,21574,21575,21576,21578,21580,21581,21583,21584,21585,21586,21588,21591,21640,21664,21665,21666,21698"
ALLOWED_CATEGORY_IDS = {x.strip() for x in ALLOWED_CATEGORY_IDS_CSV.split(",") if x.strip()}

# Выходной файл и кодировка
OUT_FILE = pathlib.Path("docs/alstyle.yml")
OUTPUT_ENCODING = "windows-1251"

# Сетевые параметры
TIMEOUT_S = 45
RETRY = 2
SLEEP_BETWEEN_RETRY = 2
HEADERS = {"User-Agent": "AlStyleFeedBot/1.0 (+github-actions; python-requests)"}

# ------------------------ Вспомогательные функции ------------------------
def _ensure_dirs(path: pathlib.Path) -> None:
    """Создать каталоги назначения, если их ещё нет."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _fetch(url: str) -> bytes | None:
    """Скачать фид: пробуем без авторизации, затем с Basic Auth; возвращаем байты либо None."""
    # 1) Без авторизации
    for attempt in range(1, RETRY + 2):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S)
            if r.status_code == 200 and r.content:
                return r.content
        except requests.RequestException:
            pass
        if attempt <= RETRY:
            time.sleep(SLEEP_BETWEEN_RETRY)

    # 2) Basic Auth
    auth = HTTPBasicAuth(USERNAME, PASSWORD)
    for attempt in range(1, RETRY + 2):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S, auth=auth)
            if r.status_code == 200 and r.content:
                return r.content
        except requests.RequestException:
            pass
        if attempt <= RETRY:
            time.sleep(SLEEP_BETWEEN_RETRY)

    return None


def _write_windows_1251(path: pathlib.Path, xml_body_unicode: str) -> None:
    """Записать XML с декларацией и Windows-1251; вне-диапазонные символы → числовые сущности."""
    decl = '<?xml version="1.0" encoding="windows-1251"?>\n'
    data = (decl + xml_body_unicode).encode(OUTPUT_ENCODING, errors="xmlcharrefreplace")
    with open(path, "wb") as f:
        f.write(data)


# ------------------------ Фильтрация по категориям ------------------------
def _filter_offers_inplace(root: ET.Element) -> tuple[int, int, int]:
    """Удалить все <offer>, у которых <categoryId> НЕ входит в ALLOWED_CATEGORY_IDS. Возвращает (total, kept, dropped)."""
    shop = root.find("./shop")
    if shop is None:
        return (0, 0, 0)

    offers_el = shop.find("offers")
    if offers_el is None:
        return (0, 0, 0)

    total = 0
    kept = 0
    dropped = 0

    for offer in list(offers_el):  # идём по копии, чтобы безопасно удалять
        total += 1
        cat_el = offer.find("categoryId")
        cat_text = (cat_el.text or "").strip() if cat_el is not None else ""

        # Нормализуем числовые строки (например, "021" -> "21") для надёжного сравнения
        if cat_text.isdigit():
            cat_text = str(int(cat_text))

        if cat_text in ALLOWED_CATEGORY_IDS:
            kept += 1
        else:
            offers_el.remove(offer)
            dropped += 1

    return (total, kept, dropped)


# ------------------------ Миграция available → атрибут ------------------------
_TRUE_WORDS = {"true", "1", "yes", "y", "да", "есть", "в наличии", "наличие", "есть в наличии"}
_FALSE_WORDS = {"false", "0", "no", "n", "нет", "отсутствует", "нет в наличии", "под заказ", "ожидается"}

def _to_bool_text(v: str) -> str:
    """Нормализовать строку к 'true'/'false' для offer@available."""
    s = (v or "").strip().lower()
    s = s.replace(":", " ").replace("\u00a0", " ").strip()
    if s in _TRUE_WORDS:  # явные маркеры «в наличии»
        return "true"
    if s in _FALSE_WORDS:  # явные маркеры «нет»
        return "false"
    if "true" in s or "да" in s:
        return "true"
    if "false" in s or "нет" in s or "под заказ" in s:
        return "false"
    return "false"  # по умолчанию осторожно считаем «нет»


def _migrate_available_inplace(root: ET.Element) -> tuple[int, int, int, int]:
    """Перенести <available> в атрибут offer@available и удалить тег.
    Возвращает: (offers_seen, attrs_set, attrs_overridden, tags_removed)."""
    shop = root.find("./shop")
    if shop is None:
        return (0, 0, 0, 0)

    offers_el = shop.find("offers")
    if offers_el is None:
        return (0, 0, 0, 0)

    offers_seen = 0
    attrs_set = 0
    attrs_overridden = 0
    tags_removed = 0

    for offer in list(offers_el):
        offers_seen += 1
        av_el = offer.find("available")
        av_text = (av_el.text or "").strip() if av_el is not None else None
        new_attr = _to_bool_text(av_text) if av_text is not None else None

        if new_attr is not None:
            if "available" in offer.attrib:
                if offer.attrib.get("available") != new_attr:
                    attrs_overridden += 1
                offer.set("available", new_attr)
            else:
                offer.set("available", new_attr)
                attrs_set += 1

        if av_el is not None:
            offer.remove(av_el)
            tags_removed += 1

    return (offers_seen, attrs_set, attrs_overridden, tags_removed)


# ------------------------ Чистка shop-префикса ------------------------
def _prune_shop_before_offers(root: ET.Element) -> int:
    """Удалить все дочерние элементы внутри <shop> ДО <offers>."""
    shop = root.find("./shop")
    if shop is None:
        return 0
    offers_el = shop.find("offers")
    if offers_el is None:
        return 0

    removed = 0
    for child in list(shop):
        if child is offers_el:
            break
        shop.remove(child)
        removed += 1
    return removed


# ------------------------ Чистка офферов от полей ------------------------
STRIP_OFFER_TAGS = {"price", "url", "quantity", "quantity_in_stock"}

def _strip_offer_fields_inplace(root: ET.Element) -> int:
    """Удалить из каждого <offer> перечисленные дочерние теги."""
    shop = root.find("./shop")
    if shop is None:
        return 0
    offers_el = shop.find("offers")
    if offers_el is None:
        return 0

    removed = 0
    for offer in list(offers_el):
        to_remove = [el for el in list(offer) if el.tag in STRIP_OFFER_TAGS]
        for el in to_remove:
            offer.remove(el)
            removed += 1
    return removed


# ------------------------ Префикс vendorCode и offer@id ------------------------
def _prefix_vendor_and_set_offer_id(root: ET.Element, prefix: str = "AS") -> tuple[int, int, int, int]:
    """
    Для каждого <offer>:
      - Берём <vendorCode>, добавляем префикс 'AS' в начало (без дефиса).
      - Записываем то же значение в атрибут offer@id.
    Возвращает: (offers_seen, vendor_updated, id_set, missing_vendor).
    """
    shop = root.find("./shop")
    if shop is None:
        return (0, 0, 0, 0)
    offers_el = shop.find("offers")
    if offers_el is None:
        return (0, 0, 0, 0)

    offers_seen = 0
    vendor_updated = 0
    id_set = 0
    missing_vendor = 0

    for offer in list(offers_el):
        offers_seen += 1
        v_el = offer.find("vendorCode")
        if v_el is None:
            missing_vendor += 1
            continue

        base = (v_el.text or "").strip()
        if not base:
            missing_vendor += 1
            continue

        # префикс без дефиса; внешние пробелы убираем
        new_code = f"{prefix}{base}"
        v_el.text = new_code
        vendor_updated += 1

        # выставляем атрибут id ровно в это значение
        if offer.get("id") != new_code:
            offer.set("id", new_code)
            id_set += 1

    return (offers_seen, vendor_updated, id_set, missing_vendor)


# ------------------------ Основной сценарий ------------------------
def main() -> int:
    print(">> Fetching supplier feed...")
    raw = _fetch(SUPPLIER_URL)
    if not raw:
        print("!! Не удалось скачать фид поставщика. Проверьте доступ/креды/URL.", file=sys.stderr)
        return 2

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print(f"!! Ошибка парсинга XML: {e}", file=sys.stderr)
        return 3

    if root.tag.lower() != "yml_catalog":
        print("!! Корневой тег не <yml_catalog>.", file=sys.stderr)
        return 4

    # 1) Фильтрация по категориям
    total, kept, dropped = _filter_offers_inplace(root)
    print(f">> Offers total: {total}, kept: {kept}, dropped: {dropped}")

    # 2) Перенос <available> в offer@available
    seen, set_cnt, overr_cnt, removed_av = _migrate_available_inplace(root)
    print(f">> Available migrated: seen={seen}, set={set_cnt}, overridden={overr_cnt}, tags_removed={removed_av}")

    # 3) Удалить всё в <shop> до <offers>
    pruned = _prune_shop_before_offers(root)
    print(f">> Shop prefix pruned: removed_nodes={pruned}")

    # 4) Удалить теги price/url/quantity/quantity_in_stock из офферов
    stripped = _strip_offer_fields_inplace(root)
    print(f">> Offer fields stripped: removed_tags_total={stripped}")

    # 5) Префикс vendorCode и offer@id
    seen2, vend_upd, id_set, miss_v = _prefix_vendor_and_set_offer_id(root, prefix="AS")
    print(f">> Vendor/id: offers_seen={seen2}, vendor_updated={vend_upd}, id_set={id_set}, missing_vendor={miss_v}")

    # Сохранение результата
    xml_unicode = ET.tostring(root, encoding="unicode")
    _ensure_dirs(OUT_FILE)
    _write_windows_1251(OUT_FILE, xml_unicode)
    print(f">> Written: {OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
