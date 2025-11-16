#!/usr/bin/env python3
"""Сборщик YML для поставщика Akcent.

Логика пайплайна:
1. Скачиваем исходный XML/YML файл поставщика.
2. Вырезаем всё содержимое между <shop> и <offers>, оставляя сами теги.
3. Оставляем только те <offer>, у которых <name> начинается с наших ключевых слов.
4. Удаляем служебные теги (url, Offer_ID, delivery, local_delivery_cost, model,
   manufacturer_warranty, Stock, prices/RRP).
5. Приводим каждый <offer> к нужному виду:
   - в <offer> оставляем только атрибуты id и available;
   - id формируем как "AK" + article;
   - внутри создаём <vendorCode> с тем же значением, что и id;
   - <categoryId type="..."> превращаем в <categoryId>значение</categoryId>,
     при отсутствии значения делаем <categoryId></categoryId>;
   - в каждом оффере добавляем <currencyId>KZT</currencyId>.
6. Нормализуем разметку: убираем лишние отступы и ставим аккуратные разрывы:
   <shop><offers>\n\n<offer...> ... </offer>\n\n</offers>
7. Сохраняем результат в docs/akcent.yml (UTF-8).
"""

from __future__ import annotations

import html
import os
import re
import sys
from pathlib import Path

import requests


# Ключевые префиксы для начала тега <name>
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

_ALLOWED_PREFIXES_UPPER = [p.upper() for p in _ALLOWED_PREFIXES]


def _decode_bytes(raw: bytes) -> str:
    """Аккуратно декодировать байты в строку (UTF-8 / CP1251)."""
    for enc in ("utf-8", "cp1251"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _strip_shop_header(text: str) -> str:
    """Удалить всё между <shop> и <offers>, оставив сами теги."""
    shop_tag = "<shop>"
    offers_tag = "<offers>"

    idx_shop = text.find(shop_tag)
    if idx_shop == -1:
        return text

    idx_offers = text.find(offers_tag, idx_shop)
    if idx_offers == -1:
        return text

    idx_after_shop = idx_shop + len(shop_tag)
    return text[:idx_after_shop] + "\n" + text[idx_offers:]


def _name_allowed(name_text: str) -> bool:
    """Проверить, начинается ли name с одного из разрешённых префиксов."""
    t = html.unescape(name_text).strip()
    upper = t.upper()
    return any(upper.startswith(prefix) for prefix in _ALLOWED_PREFIXES_UPPER)


def _filter_offers_by_name(text: str) -> str:
    """Оставить только те <offer>, у которых <name> начинается с нужных слов."""
    pattern = re.compile(r"(<offer\b[^>]*>.*?</offer>)", re.DOTALL | re.IGNORECASE)

    parts: list[str] = []
    last_end = 0
    kept = 0
    skipped = 0

    for match in pattern.finditer(text):
        # фрагмент до текущего оффера
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

    parts.append(text[last_end:])  # хвост после последнего оффера

    result = "".join(parts)
    print(f"[akcent] Фильтр по name: оставлено {kept}, выкинуто {skipped} офферов.")
    return result


def _clean_tags(text: str) -> str:
    """Удалить служебные теги и блоки (url, Offer_ID, delivery, RRP и т.п.)."""
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

    # Удаляем блок цены по RRP: <price type="RRP" ...>...</price>
    text = re.sub(
        r'<price[^>]*type=["\']RRP["\'][^>]*>.*?</price>',
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )

    # Удаляем только оболочку <prices> и </prices>
    text = re.sub(r"</?prices>", "", text)

    # Схлопываем лишние пустые строки
    text = re.sub(r"\n\s*\n+", "\n", text)

    return text


def _guess_vendor_from_text(raw: str) -> str:
    """Попробовать вытащить бренд из текста name/description.

    Ищем по простому allowlist брендов (регистр игнорируем).
    Если ничего не нашли, возвращаем пустую строку.
    """
    if not raw:
        return ""

    text = raw.lower()

    brand_map = {
        "epson": "Epson",
        "hp": "HP",
        "hewlett-packard": "HP",
        "canon": "Canon",
        "brother": "Brother",
        "xerox": "Xerox",
        "samsung": "Samsung",
        "ricoh": "Ricoh",
        "kyocera": "Kyocera",
        "pantum": "Pantum",
        "konica minolta": "Konica Minolta",
        "oki": "OKI",
        "sharp": "Sharp",
        "lexmark": "Lexmark",
        "dell": "Dell",
        "toshiba": "Toshiba",
        "nv print": "NV Print",
        "hi-black": "Hi-Black",
        "hi black": "Hi-Black",
        "fuji xerox": "Fuji Xerox",
    }

    for key, brand in brand_map.items():
        if key in text:
            return brand

    return ""


def _transform_offers(text: str) -> str:
    """Привести <offer> к нужному виду (id/available, vendorCode, categoryId, currencyId, vendor)."""

    def _process_offer(match: re.Match) -> str:
        header = match.group(1)
        body = match.group(2)
        footer = match.group(3)

        # 1) Достаём article и available из заголовка
        article_match = re.search(r'\barticle="([^"]*)"', header)
        art = (article_match.group(1).strip() if article_match else "").strip()

        # если article вдруг пустой, пробуем старый id как fallback
        if not art:
            id_match = re.search(r'\bid="([^"]*)"', header)
            if id_match:
                art = id_match.group(1).strip()

        new_id = f"AK{art}" if art else ""
        avail_match = re.search(r'\bavailable="([^"]*)"', header)
        available = avail_match.group(1).strip() if avail_match else "true"

        # 2) Новый заголовок: только id и available, с переводом строки после '>'
        new_header = f'<offer id="{new_id}" available="{available}">\n'

        # 3) Достаём значение categoryId, если оно было
        cat_val = ""
        cat_val_match = re.search(r"<categoryId[^>]*>(.*?)</categoryId>", body, re.DOTALL | re.IGNORECASE)
        if cat_val_match:
            cat_val = cat_val_match.group(1).strip()

        # 4) Удаляем все старые теги categoryId (и с содержимым, и самозакрывающиеся)
        body = re.sub(r"<categoryId[^>]*>.*?</categoryId>", "", body, flags=re.DOTALL | re.IGNORECASE)
        body = re.sub(r"<categoryId[^>]*/>", "", body, flags=re.IGNORECASE)

        # 5) После удаления старого categoryId убираем пустые строки/пробелы в начале тела
        body = body.lstrip()

        # 6) Добавляем новый блок categoryId + vendorCode + currencyId в начало тела
        prefix = (
            f"<categoryId>{cat_val}</categoryId>\n"
            f"<vendorCode>{new_id}</vendorCode>\n"
            "<currencyId>KZT</currencyId>\n"
        )
        body = prefix + body

        # 7) Если тег <vendor> пустой (<vendor/> или <vendor></vendor>), пробуем определить бренд
        #    по name/description
        # 7.1. Собираем текст для анализа
        name_match = re.search(r"<name>(.*?)</name>", body, re.DOTALL | re.IGNORECASE)
        desc_match = re.search(r"<description>(.*?)</description>", body, re.DOTALL | re.IGNORECASE)
        search_text_parts = []
        if name_match:
            search_text_parts.append(name_match.group(1))
        if desc_match:
            search_text_parts.append(desc_match.group(1))
        search_text = " ".join(search_text_parts).strip()

        # 7.2. Проверяем, пустой ли vendor
        has_selfclosing_vendor = re.search(r"<vendor\s*/>", body, re.IGNORECASE) is not None
        has_empty_vendor = re.search(r"<vendor[^>]*>\s*</vendor>", body, re.IGNORECASE) is not None

        if (has_selfclosing_vendor or has_empty_vendor) and search_text:
            guessed = _guess_vendor_from_text(search_text)
            if guessed:
                if has_selfclosing_vendor:
                    body = re.sub(
                        r"<vendor\s*/>",
                        f"<vendor>{guessed}</vendor>",
                        body,
                        flags=re.IGNORECASE,
                    )
                if has_empty_vendor:
                    body = re.sub(
                        r"<vendor[^>]*>\s*</vendor>",
                        f"<vendor>{guessed}</vendor>",
                        body,
                        flags=re.IGNORECASE,
                    )

        return new_header + body + footer

    pattern = re.compile(r"(<offer\b[^>]*>)(.*?)(</offer>)", re.DOTALL | re.IGNORECASE)
    new_text, count = pattern.subn(_process_offer, text)
    print(f"[akcent] Трансформация offer: обработано {count} офферов.")
    return new_text



def _normalize_layout(text: str) -> str:
    """Привести разметку к ровному виду и расставить разрывы.

    - выровнять всё по левому краю;
    - сделать начало: <shop><offers>\n\n<offer...;
    - поставить пустую строку между офферами;
    - поставить пустую строку перед </offers>;
    - убрать пустые строки ВНУТРИ каждого <offer>...</offer>.
    """

    # 1) Выравниваем по левому краю
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

    # 5) Убираем пустые строки внутри блоков <offer>...</offer>,
    #    чтобы после удаления тегов не оставались "дырки".
    lines = text.splitlines()
    out_lines: list[str] = []
    inside_offer = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("<offer "):
            inside_offer = True
            out_lines.append(line)
            continue

        if stripped == "</offer>":
            inside_offer = False
            out_lines.append(line)
            continue

        # Пустые строки внутри офферов пропускаем
        if inside_offer and not stripped:
            continue

        out_lines.append(line)

    return "\n".join(out_lines)



def download_akcent_feed(source_url: str, out_path: Path) -> None:
    """Скачать файл поставщика, обработать и сохранить на диск."""
    print(f"[akcent] Скачиваем файл: {source_url}")
    resp = requests.get(source_url, timeout=60)
    resp.raise_for_status()

    text = _decode_bytes(resp.content)
    print(f"[akcent] Получено байт: {len(resp.content)}")

    # 1) режем блок между <shop> и <offers>
    text = _strip_shop_header(text)

    # 2) фильтруем офферы по началу <name>
    text = _filter_offers_by_name(text)

    # 3) чистим ненужные теги
    text = _clean_tags(text)

    # 4) приводим офферы к нужному виду (id, vendorCode, categoryId, currencyId)
    text = _transform_offers(text)

    # 5) нормализуем разметку и разрывы
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
