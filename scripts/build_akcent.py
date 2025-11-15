#!/usr/bin/env python3
"""
Простой сборщик для поставщика Akcent.

Сейчас он просто скачивает исходный XML/YML файл поставщика
и сохраняет его как docs/akcent.yml без каких‑либо изменений.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import requests


def download_akcent_feed(source_url: str, out_path: Path) -> None:
    """Скачать файл поставщика и сохранить на диск без изменений."""
    print(f"[akcent] Скачиваем файл: {source_url}")
    resp = requests.get(source_url, timeout=60)
    resp.raise_for_status()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(resp.content)
    print(f"[akcent] Записано байт: {len(resp.content)} в {out_path}")


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
