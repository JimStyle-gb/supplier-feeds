#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Генератор docs/alstyle.yml из поставщика Al-Style.
Требование пользователя: "скопировать все офферы из файла поставщика".
Реализация: скачиваем исходный YML (XML) целиком и пересохраняем в docs/alstyle.yml
с кодировкой Windows-1251, не меняя структуру и содержимое.

⚠️ ЛОГИН/ПАРОЛЬ ВШИТЫ ПО ПРОСЬБЕ ПОЛЬЗОВАТЕЛЯ.
Если позже понадобится безопасно хранить — переносим в GitHub Secrets.

Поведение:
1) Пытаемся скачать по прямой ссылке.
2) Если контент не получен, пробуем HTTP Basic Auth.
3) Если заголовок XML содержит encoding, переписываем его на windows-1251.
4) Записываем файл docs/alstyle.yml (создаём каталоги при необходимости).
"""

from __future__ import annotations
import os
import re
import sys
import time
import pathlib
import typing as t

import requests
from requests.auth import HTTPBasicAuth

# ---------- Настройки ----------
SUPPLIER_URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"

# Вшитые креды (по запросу пользователя)
USERNAME = "info@complex-solutions.kz"
PASSWORD = "Aa123456"

# Выход
OUT_FILE = pathlib.Path("docs/alstyle.yml")
OUTPUT_ENCODING = "windows-1251"

# Сетевые параметры
TIMEOUT_S = 45
RETRY = 2                # лёгкий ретрай на случай нестабильности
SLEEP_BETWEEN_RETRY = 2  # сек между ретраями
HEADERS = {
    "User-Agent": "AlStyleFeedBot/1.0 (+github-actions; python-requests)"
}


# ---------- Вспомогательные функции ----------
def _ensure_dirs(path: pathlib.Path) -> None:
    """Создать родительские каталоги для файла, если их нет."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _try_fetch(url: str) -> requests.Response | None:
    """
    Попытаться скачать URL без авторизации.
    Возвращает Response при 200 и непустом теле, иначе None.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S)
        if resp.status_code == 200 and resp.content and b"<yml_catalog" in resp.content:
            return resp
    except requests.RequestException:
        return None
    return None


def _try_fetch_basic(url: str, user: str, pwd: str) -> requests.Response | None:
    """
    Попытаться скачать URL с HTTP Basic Auth.
    Возвращает Response при 200 и непустом теле, иначе None.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S,
                            auth=HTTPBasicAuth(user, pwd))
        if resp.status_code == 200 and resp.content and b"<yml_catalog" in resp.content:
            return resp
    except requests.RequestException:
        return None
    return None


def _decode_best_effort(data: bytes) -> str:
    """
    Аккуратно декодировать байты в текст.
    Пробуем UTF-8, затем cp1251, затем iso-8859-1. В крайнем случае — 'latin-1'.
    """
    for enc in ("utf-8", "cp1251", "windows-1251", "iso-8859-1", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    # Последний шанс: декодируем игнорируя ошибки (чтобы ничего не потерять — ниже заменим спецсимволами)
    return data.decode("utf-8", errors="ignore")


def _force_xml_decl_encoding(xml_text: str, target_enc: str = "windows-1251") -> str:
    """
    Заменить/добавить XML декларацию на windows-1251.
    Сохраняем остальной контент 1:1.
    """
    xml_decl_re = re.compile(r'^<\?xml[^>]*\?>', flags=re.IGNORECASE | re.DOTALL)
    enc_re = re.compile(r'encoding\s*=\s*["\'][^"\']+["\']', flags=re.IGNORECASE)

    if xml_decl_re.search(xml_text):
        # Уже есть декларация — приводим encoding к нужному
        def _repl(m: re.Match) -> str:
            decl = m.group(0)
            if enc_re.search(decl):
                decl = enc_re.sub(f'encoding="{target_enc}"', decl)
            else:
                # Добавляем encoding, если вдруг его не было
                decl = decl[:-2] + f' encoding="{target_enc}"?>'
            return decl

        return xml_decl_re.sub(_repl, xml_text, count=1)
    else:
        # Добавляем декларацию в начало
        return f'<?xml version="1.0" encoding="{target_enc}"?>\n' + xml_text.lstrip()


def _encode_windows_1251(xml_text: str) -> bytes:
    """
    Закодировать текст в Windows-1251 максимально безопасно.
    Символы вне диапазона заменяем числовыми ссылками (xmlcharrefreplace),
    чтобы не потерять информацию и сохранить валидный XML.
    """
    return xml_text.encode(OUTPUT_ENCODING, errors="xmlcharrefreplace")


def _write_bytes(path: pathlib.Path, data: bytes) -> None:
    """Записать байты в файл."""
    with open(path, "wb") as f:
        f.write(data)


# ---------- Основная логика ----------
def main() -> int:
    print(">> Fetching supplier feed...")

    resp: requests.Response | None = None

    # 1) Пытаемся скачать напрямую
    for attempt in range(1, RETRY + 2):  # первая + ретраи
        resp = _try_fetch(SUPPLIER_URL)
        if resp:
            break
        if attempt <= RETRY:
            time.sleep(SLEEP_BETWEEN_RETRY)

    # 2) Если не вышло — пробуем Basic Auth
    if not resp:
        for attempt in range(1, RETRY + 2):
            resp = _try_fetch_basic(SUPPLIER_URL, USERNAME, PASSWORD)
            if resp:
                break
            if attempt <= RETRY:
                time.sleep(SLEEP_BETWEEN_RETRY)

    if not resp:
        print("!! Не удалось скачать фид поставщика. Проверьте доступ/креды/URL.", file=sys.stderr)
        return 2

    # Декодируем текст максимально бережно
    raw = resp.content
    text = _decode_best_effort(raw)

    # Быстрая sanity-проверка на наличие корня YML
    if "<yml_catalog" not in text:
        print("!! Ответ получен, но не похож на YML/XML Yandex Market (нет <yml_catalog>).", file=sys.stderr)
        return 3

    # Принудительно ставим encoding=windows-1251 в декларации
    text = _force_xml_decl_encoding(text, target_enc=OUTPUT_ENCODING)

    # Кодируем в CP1251 c заменой нерепрезентируемых символов на &#...;
    out_bytes = _encode_windows_1251(text)

    # Гарантируем наличие каталогов и записываем файл
    _ensure_dirs(OUT_FILE)
    _write_bytes(OUT_FILE, out_bytes)

    # Короткая сводка в лог
    size_kb = len(out_bytes) / 1024.0
    print(f">> Wrote {OUT_FILE} ({size_kb:.1f} KiB)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
