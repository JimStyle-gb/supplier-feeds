#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_akcent.py
Простой базовый скрипт для поставщика AkCent:
- скачивает исходный XML-файл поставщика;
- без изменений копирует его в docs/akcent.yml;
- на этом этапе НИКАКИХ преобразований не делает (только "зеркало" поставщика).
При необходимости дальше можно добавить наценки, префиксы, фильтры и т.п.
"""

import os
import sys
from pathlib import Path
from typing import Final

import requests


# URL исходного файла поставщика AkCent (можно переопределить через переменную окружения)
DEFAULT_SOURCE_URL: Final[str] = "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"
ENV_SOURCE_URL: Final[str] = "AKCENT_SOURCE_URL"

# Куда сохраняем итоговый файл в репозитории
OUTPUT_PATH: Final[Path] = Path("docs") / "akcent.yml"


def _get_source_url() -> str:
    """Берём URL из переменной окружения или используем дефолтный."""
    env_url = os.environ.get(ENV_SOURCE_URL, "").strip()
    return env_url or DEFAULT_SOURCE_URL


def _download_bytes(url: str) -> bytes:
    """Скачиваем сырой контент поставщика (как есть, без перекодировки)."""
    print(f"[akcent] Downloading from: {url}")
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[akcent] ERROR: failed to download source XML: {exc}", file=sys.stderr)
        raise
    print(f"[akcent] Downloaded {len(resp.content)} bytes")
    return resp.content


def _save_output(data: bytes) -> None:
    """Сохраняем результат в docs/akcent.yml (создавая папку docs при необходимости)."""
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_bytes(data)
    print(f"[akcent] Saved to {OUTPUT_PATH} ({len(data)} bytes)")


def main() -> int:
    """Точка входа: скачать XML и скопировать его как есть в docs/akcent.yml."""
    url = _get_source_url()
    data = _download_bytes(url)
    _save_output(data)
    print("[akcent] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
