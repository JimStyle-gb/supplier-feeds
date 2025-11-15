#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Простой загрузчик фида AkCent.

Скрипт скачивает исходный XML/YML-файл поставщика по URL
и сохраняет его как есть (байт-в-байт) в docs/alstyle.yml.

Никаких преобразований структуры не делается: все <offer> и остальной
контент остаются в точности как у поставщика.
"""

import os
import sys
from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

# URL фида поставщика — можно переопределить через переменную окружения AKCENT_FEED_URL
FEED_URL = os.environ.get(
    "AKCENT_FEED_URL",
    "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml",
)

# Путь к выходному файлу в репозитории — можно переопределить через AKCENT_OUT_FILE
OUT_FILE = os.environ.get("AKCENT_OUT_FILE", "docs/alstyle.yml")

# Минимальный допустимый размер файла (в байтах), защита от пустых/обрезанных ответов
MIN_BYTES = int(os.environ.get("AKCENT_MIN_BYTES", "1000"))


def download_feed(url: str) -> bytes:
    """Скачать файл по HTTP(S) и вернуть сырые байты."""
    print(f"[akcent] Скачиваем фид: {url}")
    try:
        with urlopen(url, timeout=60) as resp:
            # Пытаемся прочитать весь ответ
            data = resp.read()
            status = getattr(resp, "status", None)
            if status not in (None, 200):
                print(f"[akcent] Ошибка: HTTP статус {status}", file=sys.stderr)
                sys.exit(1)
    except HTTPError as e:
        print(f"[akcent] HTTPError: {e}", file=sys.stderr)
        sys.exit(1)
    except URLError as e:
        print(f"[akcent] URLError: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[akcent] Неожиданная ошибка при скачивании: {e}", file=sys.stderr)
        sys.exit(1)

    size = len(data)
    print(f"[akcent] Получено байт: {size}")
    if size < MIN_BYTES:
        print(
            f"[akcent] Ошибка: файл слишком маленький (< {MIN_BYTES} байт), прерываем.",
            file=sys.stderr,
        )
        sys.exit(1)

    return data


def save_feed(path: str, data: bytes) -> None:
    """Сохранить байты в файл, создавая папку при необходимости."""
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(data)
    print(f"[akcent] Фид сохранён в: {out_path}")


def main() -> None:
    """Точка входа: скачать фид и сохранить его без изменений."""
    data = download_feed(FEED_URL)
    save_feed(OUT_FILE, data)


if __name__ == "__main__":
    main()
