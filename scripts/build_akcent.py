#!/usr/bin/env python3
"""Простой сборщик для поставщика Akcent.

Вариант: базовый v3 + фильтр по <name> + очистка тегов.
- скачиваем исходный XML/YML файл поставщика;
- удаляем весь блок МЕЖДУ тегами <shop> и <offers> (оставляем сами теги);
- оставляем только те <offer>, у которых <name> начинается с нужных слов;
- удаляем лишние теги (<url>, <Offer_ID>, <delivery>, <local_delivery_cost>,
  <model>, <manufacturer_warranty>, <Stock>, <prices>, </prices> и блок
  <price type="RRP" ... </price>);
- выравниваем все строки по левому краю (убираем ведущие пробелы и табы);
- сохраняем результат как docs/akcent.yml.
"""

from __future__ import annotations

import html
import os
import re
import sys
from pathlib import Path

import requests


# Разрешённые префиксы начала тега <name>
_ALLOWED_NAME_PREFIXES = [
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


def _decode_text(raw_bytes: bytes) -> str:
    """Декодировать байты в строку с попытками utf-8 / cp1251."""
    for enc in ("utf-8", "cp1251"):
        try:
            return raw_bytes.decode(enc)
        except UnicodeDecodeError:
            pass
    return raw_bytes.decode("utf-8", errors="ignore")


def _strip_shop_header(raw_bytes: bytes) -> str:
    """Удалить всё между <shop> и <offers>, оставив сами теги."""
    text = _decode_text(raw_bytes)

    shop_tag = "<shop>"
    offers_tag = "<offers>"

    idx_shop = text.find(shop_tag)
    if idx_shop == -1:
        return text

    idx_offers = text.find(offers_tag, idx_shop)
    if idx_offers == -1:
        return text

    idx_after_shop = idx_shop + len(shop_tag)
    new_text = text[:idx_after_shop] + "\n" + text[idx_offers:]
    return new_text


def _name_allowed(name_text: str) -> bool:
    """Проверить, начинается ли name с одного из разрешённых префиксов."""
    t = name_text.strip()
    upper = t.upper()
    for prefix in _ALLOWED_NAME_PREFIXES:
        if upper.startswith(prefix.upper()):
            return True
    return False


def _filter_offers_by_name(text: str) -> str:
    """Оставить только те <offer>, у которых <name> начинается с нужных слов."""
    pattern = re.compile(r"(<offer\b[^>]*>.*?</offer>)", re.DOTALL | re.IGNORECASE)
    parts: list[str] = []
    last_end = 0
    kept = 0
    skipped = 0

    for match in pattern.finditer(text):
        parts.append(text[last_end:match.start()])  # кусок до оффера

        block = match.group(1)
        name_match = re.search(r"<name>(.*?)</name>", block, re.DOTALL | re.IGNORECASE)
        if not name_match:
            skipped += 1
        else:
            raw_name = name_match.group(1)
            name_text = html.unescape(raw_name).strip()
            if _name_allowed(name_text):
                parts.append(block)
                kept += 1
            else:
                skipped += 1

        last_end = match.end()

    parts.append(text[last_end:])  # хвост после последнего оффера

    result = "".join(parts)
    print(f"[akcent] Фильтр по name: оставлено {kept}, выкинуто {skipped} офферов.")
    return result


def _clean_tags(text: str) -> str:
    """Удалить лишние теги и блоки (url, Offer_ID, delivery, RRP и т.п.)."""
    # Простые теги с содержимым
    simple_patterns = [
        r"<url>.*?</url>",
        r"<Offer_ID>.*?</Offer_ID>",
        r"<delivery>.*?</delivery>",
        r"<local_delivery_cost>.*?</local_delivery_cost>",
        r"<model>.*?</model>",
        r"<manufacturer_warranty>.*?</manufacturer_warranty>",
        r"<Stock>.*?</Stock>",
    ]
    for pat in simple_patterns:
        text = re.sub(pat, "", text, flags=re.DOTALL)

    # Блок цены по RRP: <price type="RRP" ...>...</price>
    text = re.sub(
        r'<price[^>]*type=["\']RRP["\'][^>]*>.*?</price>',
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )

    # Удаляем только оболочку <prices> и </prices>
    text = re.sub(r"</?prices>", "", text)

    return text


def _left_align(text: str) -> str:
    """Убрать ведущие пробелы/табы у каждой строки."""
    lines = text.splitlines()
    stripped = [line.lstrip(" \t") for line in lines]
    return "\n".join(stripped)


def _format_layout(text: str) -> str:
    """Сделать <shop><offers> и разрывы между офферами.

    В начале файла приводим последовательность к виду:
    <shop><offers>\n\n<offer...
    и добавляем пустую строку между каждым </offer> и следующим <offer>.
    """
    # 1) Только первое вхождение <shop>...<offers> приводим к <shop><offers>\n\n
    text = re.sub(r"<shop>\s*<offers>", "<shop><offers>\n\n", text, count=1)
    # 2) Между </offer> и следующими <offer> вставляем двойной перенос
    text = re.sub(r"</offer>\s*<offer", "</offer>\n\n<offer", text)
    return text


def download_akcent_feed(source_url: str, out_path: Path) -> None:
    """Скачать файл поставщика, обработать и сохранить на диск."""
    print(f"[akcent] Скачиваем файл: {source_url}")
    resp = requests.get(source_url, timeout=60)
    resp.raise_for_status()

    original = resp.content
    print(f"[akcent] Получено байт: {len(original)}")


    # 1) удаляем блок между <shop> и <offers>
    text = _strip_shop_header(original)

    # 2) фильтруем офферы по началу <name>
    text = _filter_offers_by_name(text)

    # 3) чистим ненужные теги
    text = _clean_tags(text)

    # 4) выравниваем по левому краю
    text = _left_align(text)

    # 5) приводим <shop><offers> + двойной перенос строки после них
    text = _format_layout(text)

    out_bytes = text.encode("utf-8")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(out_bytes)
    print(f"[akcent] Записано байт: {len(out_bytes)} в {out_path}")


def main() -> int:
    """Точка входа."""
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
