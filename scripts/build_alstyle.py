#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Генератор docs/alstyle.yml из поставщика Al-Style c фильтрацией по <categoryId>.
Требование: вшить список категорий прямо в код (через запятую) и оставить только офферы,
у которых <categoryId> входит в список.

ЛОГИН/ПАРОЛЬ ВШИТЫ ПО ПРОСЬБЕ ПОЛЬЗОВАТЕЛЯ.
Если понадобится безопасное хранение — перенести в GitHub Secrets.

Выход: docs/alstyle.yml в кодировке Windows-1251.
'''
from __future__ import annotations

import pathlib
import sys
import time
import xml.etree.ElementTree as ET
from typing import Iterable

import requests
from requests.auth import HTTPBasicAuth

# ------------------------ Конфигурация ------------------------
SUPPLIER_URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"

# Вшитые креды (по запросу пользователя)
USERNAME = "info@complex-solutions.kz"
PASSWORD = "Aa123456"

# Список categoryId, вшитый одной строкой (через запятую), чтобы не занимать много места в коде
ALLOWED_CATEGORY_IDS_CSV = (
    "3540,3541,3542,3543,3544,3545,3566,3567,3569,3570,3580,3688,3708,3721,3722,"
    "4889,4890,4895,5017,5075,5649,5710,5711,5712,5713,21279,21281,21291,21356,"
    "21367,21368,21369,21370,21371,21372,21451,21498,21500,21501,21572,21573,"
    "21574,21575,21576,21578,21580,21581,21583,21584,21585,21586,21588,21591,"
    "21640,21664,21665,21666,21698"
)

# Преобразуем CSV-строку в множество строк-идентификаторов (strip для надёжности)
ALLOWED_CATEGORY_IDS = {x.strip() for x in ALLOWED_CATEGORY_IDS_CSV.split(",") if x.strip()}

# Выходной файл и кодировка
OUT_FILE = pathlib.Path("docs/alstyle.yml")
OUTPUT_ENCODING = "windows-1251"

# Сетевые параметры
TIMEOUT_S = 45
RETRY = 2                # число повторов при неудаче
SLEEP_BETWEEN_RETRY = 2  # пауза между попытками
HEADERS = {"User-Agent": "AlStyleFeedBot/1.0 (+github-actions; python-requests)"}


# ------------------------ Утилиты ------------------------
def _ensure_dirs(path: pathlib.Path) -> None:
    """Создать родительские каталоги для файла, если их нет."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _fetch(url: str) -> bytes | None:
    """
    Скачивание фида.
    1) Пытаемся без авторизации.
    2) Если не получилось — с Basic Auth.
    Возвращаем байты XML при успехе, иначе None.
    """
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


def _xml_to_unicode(root: ET.Element) -> str:
    """
    Преобразовать XML-дерево в unicode-строку без декларации.
    Декларацию с encoding добавим вручную.
    """
    return ET.tostring(root, encoding="unicode")


def _write_windows_1251(path: pathlib.Path, xml_body_unicode: str) -> None:
    """
    Записать XML с декларацией и кодировкой Windows-1251.
    Символы вне cp1251 заменяются на числовые ссылки (xmlcharrefreplace).
    """
    decl = '<?xml version="1.0" encoding="windows-1251"?>\n'
    data = (decl + xml_body_unicode).encode(OUTPUT_ENCODING, errors="xmlcharrefreplace")
    with open(path, "wb") as f:
        f.write(data)


def _iter_offers(shop_el: ET.Element):
    """Итератор по <offer> внутри <shop>/<offers>."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return []
    return list(offers_el)  # возвращаем копию списка для безопасного удаления


def _filter_offers_inplace(root: ET.Element):
    """
    Удалить из дерева все <offer>, у которых <categoryId> не в ALLOWED_CATEGORY_IDS.
    Возвращает (total, kept, dropped).
    """
    shop = root.find("./shop")
    if shop is None:
        return (0, 0, 0)

    offers_el = shop.find("offers")
    if offers_el is None:
        return (0, 0, 0)

    total = 0
    kept = 0
    dropped = 0

    # Проходим по копии, чтобы безопасно удалять из исходного родителя
    for offer in list(offers_el):
        total += 1
        cat_el = offer.find("categoryId")
        cat_text = (cat_el.text or "").strip() if cat_el is not None else ""
        if cat_text in ALLOWED_CATEGORY_IDS:
            kept += 1
        else:
            offers_el.remove(offer)
            dropped += 1

    return (total, kept, dropped)


# ------------------------ Основной сценарий ------------------------
def main() -> int:
    print(">> Fetching supplier feed...")
    raw = _fetch(SUPPLIER_URL)
    if not raw:
        print("!! Не удалось скачать фид поставщика. Проверьте доступ/креды/URL.", file=sys.stderr)
        return 2

    try:
        # Парсим напрямую байты — ElementTree сам учтёт декларацию encoding
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print(f"!! Ошибка парсинга XML: {e}", file=sys.stderr)
        return 3

    if root.tag.lower() != "yml_catalog":
        print("!! Корневой тег не <yml_catalog>. Проверьте формат поставщика.", file=sys.stderr)
        return 4

    total, kept, dropped = _filter_offers_inplace(root)
    print(f">> Offers total: {total}, kept: {kept}, dropped: {dropped}")

    # Преобразуем обратно в текст и записываем Windows-1251
    xml_unicode = _xml_to_unicode(root)
    _ensure_dirs(OUT_FILE)
    _write_windows_1251(OUT_FILE, xml_unicode)

    print(f">> Written: {OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
