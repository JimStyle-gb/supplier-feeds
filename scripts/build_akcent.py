#!/usr/bin/env python3
"""Простой сборщик для поставщика Akcent.

Вариант v2:
- скачиваем исходный XML/YML файл поставщика;
- удаляем весь блок МЕЖДУ тегами <shop> и <offers> (оставляем сами теги);
- сохраняем результат как docs/akcent.yml.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import requests


def _strip_shop_header(raw_bytes: bytes) -> bytes:
    """Удалить всё содержимое между <shop> и <offers>, оставив сами теги.

    Если нужные теги не найдены или декодирование UTF-8 не удалось,
    возвращаем исходные байты без изменений.
    """
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        # Если по каким-то причинам файл не UTF-8 — не трогаем
        return raw_bytes

    shop_tag = "<shop>"
    offers_tag = "<offers>"

    idx_shop = text.find(shop_tag)
    if idx_shop == -1:
        # Не нашли <shop> — возвращаем как есть
        return raw_bytes

    idx_offers = text.find(offers_tag, idx_shop)
    if idx_offers == -1:
        # Не нашли <offers> после <shop> — возвращаем как есть
        return raw_bytes

    # Позиция сразу после тега <shop>
    idx_after_shop = idx_shop + len(shop_tag)

    # Формируем новый текст:
    # всё до <shop> включительно + сразу блок, начиная с <offers>...
    new_text = text[:idx_after_shop] + "\n" + text[idx_offers:]

    return new_text.encode("utf-8")


def download_akcent_feed(source_url: str, out_path: Path) -> None:
    """Скачать файл поставщика, вырезать блок shop‑header и сохранить на диск."""
    print(f"[akcent] Скачиваем файл: {source_url}")
    resp = requests.get(source_url, timeout=60)
    resp.raise_for_status()

    original = resp.content
    print(f"[akcent] Получено байт: {len(original)}")

    processed = _strip_shop_header(original)
    if processed != original:
        print("[akcent] Обрезан блок между <shop> и <offers>.")
    else:
        print("[akcent] Структура без изменений (теги не найдены или не UTF-8).")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(processed)
    print(f"[akcent] Записано байт: {len(processed)} в {out_path}")


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
