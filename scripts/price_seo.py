#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
Простая копия: docs/price.yml -> docs/price_seo.yml
Никаких изменений содержимого. Полная побайтная идентичность.
"""

from pathlib import Path
import shutil
import sys

SRC = Path("docs/price.yml")
DST = Path("docs/price_seo.yml")

def main() -> int:
    if not SRC.exists():
        print(f"[seo] Исходный файл не найден: {SRC}", file=sys.stderr)
        return 1

    # создаём папку docs, если вдруг её нет
    DST.parent.mkdir(parents=True, exist_ok=True)

    # побайтная копия, чтобы не трогать кодировку/символы
    shutil.copyfile(SRC, DST)

    # чуть-чуть логов для Actions
    print(f"[seo] Скопировано без изменений:\n  {SRC} -> {DST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
