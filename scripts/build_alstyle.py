#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
scripts/build_alstyle.py — генератор docs/alstyle.yml из фида поставщика Al-Style
с фильтрацией по <categoryId> (список ID поставщика задан заказчиком).

Правила и поведение:
- Креды вшиты по просьбе пользователя (для поставщика с возможной Basic-Auth).
- Скачиваем фид, парсим как YML/XML (ElementTree).
- Удаляем <offer>, если его <categoryId> не входит в список ALLOWED_CATEGORY_IDS.
- Сохраняем результат в docs/alstyle.yml с декларацией и кодировкой Windows-1251.
- Внешних пояснений минимум; внутри — подробные комментарии.
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

# Жёстко вшитые логин/пароль (по запросу пользователя)
USERNAME = "info@complex-solutions.kz"
PASSWORD = "Aa123456"

# Список categoryId ПОСТАВЩИКА (ОДНА CSV-строка, как просили — компактно)
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
    """
    Скачивание фида. Порядок:
      1) Проба без авторизации (часто фид открыт).
      2) Если не получилось — повтор с HTTP Basic Auth.
    Возвращает сырые байты XML при успехе, иначе None.
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


def _write_windows_1251(path: pathlib.Path, xml_body_unicode: str) -> None:
    """
    Записать XML с декларацией и кодировкой Windows-1251.
    Символы вне диапазона cp1251 заменяем на числовые сущности (xmlcharrefreplace),
    чтобы сохранить валидность и читаемость.
    """
    decl = '<?xml version="1.0" encoding="windows-1251"?>\n'
    data = (decl + xml_body_unicode).encode(OUTPUT_ENCODING, errors="xmlcharrefreplace")
    with open(path, "wb") as f:
        f.write(data)


# ------------------------ Фильтрация ------------------------
def _filter_offers_inplace(root: ET.Element) -> tuple[int, int, int]:
    """
    Удаляет все <offer>, у которых <categoryId> НЕ входит в ALLOWED_CATEGORY_IDS.
    Возвращает статистику: (total, kept, dropped).
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

    # Итерируем поверх копии, чтобы безопасно удалять из исходного контейнера
    for offer in list(offers_el):
        total += 1
        cat_el = offer.find("categoryId")
        cat_text = (cat_el.text or "").strip() if cat_el is not None else ""

        # Нормализуем числовые строки (например, "021" -> "21")
        if cat_text.isdigit():
            cat_text = str(int(cat_text))

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
        # Парсим байты напрямую — ElementTree учитывает исходную XML-декларацию encoding
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        print(f"!! Ошибка парсинга XML: {e}", file=sys.stderr)
        return 3

    if root.tag.lower() != "yml_catalog":
        print("!! Корневой тег не <yml_catalog>.", file=sys.stderr)
        return 4

    total, kept, dropped = _filter_offers_inplace(root)
    print(f">> Offers total: {total}, kept: {kept}, dropped: {dropped}")

    # Преобразуем дерево обратно в текст (без декларации) и сохраняем в cp1251
    xml_unicode = ET.tostring(root, encoding="unicode")
    _ensure_dirs(OUT_FILE)
    _write_windows_1251(OUT_FILE, xml_unicode)
    print(f">> Written: {OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
