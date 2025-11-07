#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
scripts/build_alstyle.py — генератор docs/alstyle.yml из фида поставщика Al-Style
с двумя важными шагами обработки:
1) ФИЛЬТР по <categoryId> (используем ID ПОСТАВЩИКА из заданного списка).
2) МИГРАЦИЯ доступности: перенос значения <available>true/false</available> внутрь атрибута
   самого оффера <offer ... available="true|false"> и удаление дочернего тега <available>,
   как требует Satu.

Выход: docs/alstyle.yml в кодировке Windows-1251 с сохранением исходной структуры YML,
за исключением удаления лишних <offer> и переноса <available> в атрибут.
Внешних пояснений минимум — подробные комментарии внутри кода.
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
_TRUE_WORDS = {
    "true", "1", "yes", "y", "да", "есть", "в наличии", "наличие", "есть в наличии", "true/false:true"
}
_FALSE_WORDS = {
    "false", "0", "no", "n", "нет", "отсутствует", "нет в наличии", "под заказ", "ожидается", "true/false:false"
}

def _to_bool_text(v: str) -> str:
    """Нормализовать текст в 'true'/'false' для атрибута offer@available."""
    s = (v or "").strip().lower()
    # убираем лишние пробелы и двоеточия
    s = s.replace(":", " ").replace("\u00a0", " ").strip()
    # пробуем маппинг на известные формы
    if s in _TRUE_WORDS:
        return "true"
    if s in _FALSE_WORDS:
        return "false"
    # эвристика: любые 'true'/'false' внутри строки
    if "true" in s or "да" in s:
        return "true"
    if "false" in s or "нет" in s or "под заказ" in s:
        return "false"
    # по умолчанию — не доступен (безопасно для маркетплейса)
    return "false"


def _migrate_available_inplace(root: ET.Element) -> tuple[int, int, int, int]:
    """Перенести <available> внутрь атрибута offer@available и удалить сам тег.
    Возвращает кортеж: (offers_seen, attrs_set, attrs_overridden, tags_removed)."""
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

        # найдём дочерний <available>, если есть
        av_el = offer.find("available")
        av_text = None
        if av_el is not None:
            av_text = (av_el.text or "").strip()
        # вычислим целевое значение атрибута
        new_attr = _to_bool_text(av_text) if av_text is not None else None

        # если атрибут уже есть — решаем, переопределять ли его
        if new_attr is not None:
            if "available" in offer.attrib:
                # переопределяем значением из дочернего тега — так просил пользователь
                if offer.attrib.get("available") != new_attr:
                    attrs_overridden += 1
                offer.set("available", new_attr)
            else:
                offer.set("available", new_attr)
                attrs_set += 1

        # удалить дочерний тег <available>, если он был
        if av_el is not None:
            offer.remove(av_el)
            tags_removed += 1

    return (offers_seen, attrs_set, attrs_overridden, tags_removed)


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

    # 1) Фильтрация по категориям
    total, kept, dropped = _filter_offers_inplace(root)
    print(f">> Offers total: {total}, kept: {kept}, dropped: {dropped}")

    # 2) Перенос <available> в offer@available
    seen, set_cnt, overr_cnt, removed_cnt = _migrate_available_inplace(root)
    print(f">> Offers processed for available: seen={seen}, set={set_cnt}, overridden={overr_cnt}, tags_removed={removed_cnt}")

    # Преобразуем дерево обратно в текст (без декларации) и сохраняем в cp1251
    xml_unicode = ET.tostring(root, encoding="unicode")
    _ensure_dirs(OUT_FILE)
    _write_windows_1251(OUT_FILE, xml_unicode)
    print(f">> Written: {OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
