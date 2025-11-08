# -*- coding: utf-8 -*-
"""
build_alstyle.py — v1
Задача: скачать исходный фид AlStyle по HTTP Basic-авторизации и сохранить как docs/alstyle.yml (cp1251).
• Логин/пароль вшиты по запросу пользователя.
• Без трансформаций: сохраняем весь контент (все <offer> и остальное), только перекодируем при необходимости.
"""

import os
import sys
import re
import pathlib
import requests

# --- Константы ---------------------------------------------------------------
URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"  # источник
LOGIN = "info@complex-solutions.kz"  # по просьбе пользователя
PASSWORD = "Aa123456"                # по просьбе пользователя
OUT_PATH = pathlib.Path("docs/alstyle.yml")  # итоговый файл
ENC_OUT = "windows-1251"             # требование проекта

# --- Утилиты -----------------------------------------------------------------
def ensure_parent_dir(p: pathlib.Path) -> None:
    """Создать каталог для файла, если его нет."""
    p.parent.mkdir(parents=True, exist_ok=True)

def decode_best_effort(data: bytes, declared: str | None) -> str:
    """Разумно декодировать байты → str. Пытаемся заголовочную кодировку, затем популярные варианты."""
    tried = []
    for enc in filter(None, [declared, "utf-8", "windows-1251", "cp1251", "latin-1"]):
        if enc in tried:
            continue
        tried.append(enc)
        try:
            return data.decode(enc)
        except Exception:
            pass
    # Последняя попытка — 'utf-8' с заменой
    return data.decode("utf-8", errors="replace")

def encode_cp1251(text: str) -> bytes:
    """Строго кодируем в cp1251, заменяя неподдерживаемые символы безопасной подстановкой."""
    return text.encode(ENC_OUT, errors="replace")

# --- Основная логика ---------------------------------------------------------
def main() -> int:
    # 1) Скачиваем источник с базовой авторизацией
    try:
        resp = requests.get(URL, timeout=90, auth=(LOGIN, PASSWORD))
    except Exception as e:
        print(f"[ERROR] Не удалось скачать источник: {e}", file=sys.stderr)
        return 1

    if resp.status_code != 200:
        print(f"[ERROR] HTTP {resp.status_code}: сервер не вернул 200 OK", file=sys.stderr)
        return 1

    # 2) Декодируем разумно (учитываем resp.encoding, если есть)
    text = decode_best_effort(resp.content, getattr(resp, "encoding", None))

    # 3) Минимальная sanity-проверка: наличие корневых тегов
    # (не прерываем сборку, просто предупреждаем)
    if "<offer" not in text:
        print("[WARN] Похоже, в источнике не найдено ни одного <offer>. Проверьте логин/пароль/доступ.")

    # 4) Пишем файл как cp1251 (windows-1251)
    ensure_parent_dir(OUT_PATH)
    data_cp1251 = encode_cp1251(text)
    OUT_PATH.write_bytes(data_cp1251)

    # 5) Отчёт в stdout (для лога Actions)
    offers = len(re.findall(r"<offer\\b", text, flags=re.IGNORECASE))
    size_kb = len(data_cp1251) / 1024.0
    print(f"[OK] Сохранено: {OUT_PATH} | ~{size_kb:.1f} KB | офферов: {offers}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
