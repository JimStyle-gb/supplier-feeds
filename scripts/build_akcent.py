#!/usr/bin/env python3
"""Простой сборщик для поставщика Akcent.

Вариант v3:
- скачиваем исходный XML/YML файл поставщика;
- удаляем весь блок МЕЖДУ тегами <shop> и <offers> (оставляем сами теги);
- выравниваем все строки по левому краю (убираем ведущие пробелы и табы);
- сохраняем результат как docs/akcent.yml.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import requests


def _decode_text(raw_bytes: bytes) -> str:
    """Аккуратно декодировать байты в строку.

    Пытаемся UTF-8, потом Windows-1251, в крайнем случае — UTF-8 с игнором ошибок.
    """
    for enc in ("utf-8", "cp1251"):
        try:
            return raw_bytes.decode(enc)
        except UnicodeDecodeError:
            pass
    # Последний шанс — UTF-8 с игнором ошибок
    return raw_bytes.decode("utf-8", errors="ignore")


def _strip_shop_header(raw_bytes: bytes) -> str:
    """Удалить всё содержимое между <shop> и <offers>, оставив сами теги.

    Возвращаем текст (str). Если теги не найдены — возвращаем исходный текст.
    """
    text = _decode_text(raw_bytes)

    shop_tag = "<shop>"
    offers_tag = "<offers>"

    idx_shop = text.find(shop_tag)
    if idx_shop == -1:
        # Не нашли <shop> — возвращаем как есть
        return text

    idx_offers = text.find(offers_tag, idx_shop)
    if idx_offers == -1:
        # Не нашли <offers> после <shop> — возвращаем как есть
        return text

    # Позиция сразу после тега <shop>
    idx_after_shop = idx_shop + len(shop_tag)

    # Формируем новый текст:
    # всё до <shop> включительно + сразу блок, начиная с <offers>...
    new_text = text[:idx_after_shop] + "\n" + text[idx_offers:]
    return new_text


def _left_align(text: str) -> str:
    """Убрать все ведущие пробелы/табы у каждой строки.

    Это выравнивает весь XML/YML по левому краю.
    """
    lines = text.splitlines()
    stripped = [line.lstrip(" \t") for line in lines]
    return "\n".join(stripped)


def download_akcent_feed(source_url: str, out_path: Path) -> None:
    """Скачать файл поставщика, обработать и сохранить на диск."""
    print(f"[akcent] Скачиваем файл: {source_url}")
    resp = requests.get(source_url, timeout=60)
    resp.raise_for_status()

    original = resp.content
    print(f"[akcent] Получено байт: {len(original)}")


    # 1) режем блок между <shop> и <offers>
    text = _strip_shop_header(original)

    # 2) выравниваем по левому краю
    text = _left_align(text)

    out_bytes = text.encode("utf-8")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(out_bytes)
    print(f"[akcent] Записано байт: {len(out_bytes)} в {out_path}")


def main() -> int:
    """Точка входа: читаем переменные окружения и запускаем скачивание."""
    source_url = os.getenv(
        "AKCENT_URL",
        "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml",
    )
    out_file = Path(os.getenv("OUT_FILE", "docs/akcent.yml"))

    try:
        download_akcent_feed(source_url, out_file)
    except Exception as exc:  # noqa: BLE001
        print(f"[akcent] Ошибка при скачивании: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
