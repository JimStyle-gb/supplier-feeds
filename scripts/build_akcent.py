#!/usr/bin/env python3
"""Сборщик YML для поставщика Akcent.

Логика максимально простая и линейная:
1. Скачиваем исходный XML/YML файл поставщика.
2. Вырезаем всё содержимое между <shop> и <offers>, оставляя сами теги.
3. Оставляем только те <offer>, у которых <name> начинается с наших ключевых слов.
4. Удаляем служебные теги (url, Offer_ID, delivery, local_delivery_cost, model,
   manufacturer_warranty, Stock, prices/RRP).
5. Нормализуем разметку: убираем лишние отступы и ставим аккуратные разрывы:
   <shop><offers>\n\n<offer...> ... </offer>\n\n</offers>
6. Сохраняем результат в docs/akcent.yml (UTF‑8).
"""

from __future__ import annotations

import html
import os
import re
import sys
from pathlib import Path

import requests


# Список разрешённых префиксов для <name> (начало строки)
_ALLOWED_PREFIXES = [
    "C13T55",
    "Ёмкость для отработанных чернил",
    "Интерактивная доска",
    "Интерактивная панель",
    "Интерактивный дисплей",
    "Картридж",
    "Ламинатор",
    "Монитор",
    "МФУ",
    "Переплетчик",
    "Пленка для ламинирования",
    "Плоттер",
    "Принтер",
    "Проектор",
    "Сканер",
    "Чернила",
    "Шредер",
    "Экономичный набор",
    "Экран",
]

# Такой же список, но в верхнем регистре — чтобы не считать .upper() каждый раз
_ALLOWED_PREFIXES_UPPER = [p.upper() for p in _ALLOWED_PREFIXES]


def _decode_bytes(raw: bytes) -> str:
    """Аккуратно декодировать байты в строку.

    Пробуем UTF‑8, затем Windows‑1251, в крайнем случае — UTF‑8 с игнором.
    """
    for enc in ("utf-8", "cp1251"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _strip_shop_header(text: str) -> str:
    """Удалить всё между <shop> и <offers>, оставив сами теги.

    Если пара тегов не найдена — возвращаем текст как есть.
    """
    shop_tag = "<shop>"
    offers_tag = "<offers>"

    idx_shop = text.find(shop_tag)
    if idx_shop == -1:
        return text

    idx_offers = text.find(offers_tag, idx_shop)
    if idx_offers == -1:
        return text

    idx_after_shop = idx_shop + len(shop_tag)
    # Оставляем <shop> и сразу приклеиваем <offers> и всё, что ниже
    return text[:idx_after_shop] + "\n" + text[idx_offers:]


def _name_allowed(name_text: str) -> bool:
    """Проверить, начинается ли имя с одного из разрешённых префиксов."""
    t = html.unescape(name_text).strip()
    upper = t.upper()
    return any(upper.startswith(prefix) for prefix in _ALLOWED_PREFIXES_UPPER)


def _filter_offers_by_name(text: str) -> str:
    """Оставить только те <offer>, где <name> начинается с наших ключей."""
    pattern = re.compile(r"(<offer\b[^>]*>.*?</offer>)", re.DOTALL | re.IGNORECASE)

    parts: list[str] = []
    last_end = 0
    kept = 0
    skipped = 0

    for match in pattern.finditer(text):
        # Фрагмент до текущего оффера оставляем как есть
        parts.append(text[last_end:match.start()])

        block = match.group(1)
        name_match = re.search(r"<name>(.*?)</name>", block, re.DOTALL | re.IGNORECASE)
        if not name_match:
            skipped += 1
        else:
            name_text = name_match.group(1)
            if _name_allowed(name_text):
                parts.append(block)
                kept += 1
            else:
                skipped += 1

        last_end = match.end()

    # Хвост после последнего оффера
    parts.append(text[last_end:])

    result = "".join(parts)
    print(f"[akcent] Фильтр по name: оставлено {kept}, выкинуто {skipped} офферов.")
    return result


def _clean_tags(text: str) -> str:
    """Удалить служебные теги и аккуратно «подтянуть» содержимое вверх."""
    # 1) Удаляем простые теги с содержимым
    text = re.sub(
        r"<(url|Offer_ID|delivery|local_delivery_cost|model|manufacturer_warranty|Stock)>.*?</\1>",
        "",
        text,
        flags=re.DOTALL,
    )

    # 2) Удаляем блок цены по RRP: <price type="RRP" ...>...</price>
    text = re.sub(
        r'<price[^>]*type=["\']RRP["\'][^>]*>.*?</price>',
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )

    # 3) Удаляем оболочку <prices> и </prices>, внутренние <price> остаются
    text = re.sub(r"</?prices>", "", text, flags=re.IGNORECASE)

    # 4) Схлопываем лишние пустые строки, которые появились после удаления тегов
    text = re.sub(r"\n\s*\n", "\n", text)

    return text


def _normalize_layout(text: str) -> str:
    """Выравнивание слева + аккуратные разрывы между блоками.

    На выходе хотим:
    <shop><offers>\n\n<offer ...>...</offer>\n\n<offer ...>...</offer>...\n\n</offers>
    """
    # 1) Убираем ведущие пробелы/табы у каждой строки
    lines = text.splitlines()
    text = "\n".join(line.lstrip(" \t") for line in lines)

    # 2) Нормализуем начало: <shop><offers>\n\n<offer...
    text = re.sub(
        r"<shop>\s*<offers>\s*<offer",
        "<shop><offers>\n\n<offer",
        text,
        count=1,
    )

    # 3) Между офферами делаем пустую строку
    text = re.sub(r"</offer>\s*<offer", "</offer>\n\n<offer", text)

    # 4) Между последним </offer> и </offers> делаем пустую строку
    text = re.sub(r"</offer>\s*</offers>", "</offer>\n\n</offers>", text)

    return text


def download_akcent_feed(source_url: str, out_path: Path) -> None:
    """Скачать файл поставщика, обработать и сохранить на диск."""
    print(f"[akcent] Скачиваем файл: {source_url}")
    resp = requests.get(source_url, timeout=60)
    resp.raise_for_status()

    raw = resp.content
    print(f"[akcent] Получено байт: {len(raw)}")

    # 1) Декодируем байты в текст
    text = _decode_bytes(raw)

    # 2) Режем блок между <shop> и <offers>
    text = _strip_shop_header(text)

    # 3) Фильтруем офферы по началу <name>
    text = _filter_offers_by_name(text)

    # 4) Удаляем служебные теги и лишние пустые строки
    text = _clean_tags(text)

    # 5) Выравниваем и нормализуем разметку
    text = _normalize_layout(text)

    out_bytes = text.encode("utf-8")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(out_bytes)
    print(f"[akcent] Записано байт: {len(out_bytes)} в {out_path}")


def main() -> int:
    """Точка входа скрипта."""
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
