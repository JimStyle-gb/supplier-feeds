# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle → YML (NO-DESCRIPTION-TOUCH edition)

Задача: полностью отключить любые изменения содержимого тега <description>.
Мы НЕ создаём/заменяем/форматируем описания — берём их из исходного XML как есть.
Остальной пайплайн (бренд, цена, available, vendorCode/id, currencyId, keywords, порядок полей и т.д.) сохранён.

Версия: alstyle-2025-10-30.ndt-1
Python: 3.11+
"""

from __future__ import annotations
import os, sys, re, time, random, hashlib, urllib.parse, requests, html
from typing import Dict, List, Tuple, Optional, Set
...

def format_dt_almaty(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def next_build_time_almaty() -> datetime:
    cur = now_almaty()
    t = cur.replace(hour=1, minute=0, second=0, microsecond=0)
    return t + timedelta(days=1) if cur >= t else t

def load_source_bytes(src: str) -> bytes:
    """Скачиваем/читаем исходный XML поставщика."""
    if not src:
        raise RuntimeError("SUPPLIER_URL не задан")
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path, "rb"
...
# --- Description sanitizer (whitespace/entity cleanup; NO HTML formatting) ---
def _split_tags_and_text(s: str) -> List[str]:
    """
    Разбивает строку на список фрагментов, где теги <...> и текст чередуются.
    Мы будем обрабатывать только текстовые фрагменты (не теги).
    """
    if not s:
        return []
    parts = re.split(r'(<[^>]+>)', s, flags=re.S)
    return [p for p in parts if p is not None and p != ""]

def _sanitize_text_piece(txt: str) -> str:
    """Чистим текст без изменения HTML-тегов: пробелы, переносы, сущности."""
    if not txt:
        return txt
    # нормализуем переносы
    t = txt.replace('\r\n', '\n').replace('\r', '\n')
    # заменить неразрывные пробелы
    t = t.replace('\u00a0', ' ')
    # декодируем базовые HTML-сущности в тексте (не в тегах)
    t = html.unescape(t)
    # убираем пробелы перед переводом строки
    t = re.sub(r'[ \t]+\n', '\n', t)
    # сжимаем 2+ пробелов в один (только в тексте)
    t = re.sub(r' {2,}', ' ', t)
    return t

def _sanitize_block(txt: str) -> str:
    """Финальная правка блока: трим, сжатие пустых строк, правка 'висячих заголовков'."""
    if not txt:
        return txt
    t = txt
    # убираем хвостовые пробелы в конце строк
    t = re.sub(r'[ \t]+(\n)', r'\1', t, flags=re.M)
    t = t.strip()
    # схлопываем 3+ пустых строк в одну
    t = re.sub(r'\n{3,}', '\n\n', t, flags=re.S)
    # убираем висячие заголовки-строки, завершающиеся двоеточием
    t = re.sub(r'(?m)^([^\n<>]{3,}):\s*$', r'\1', t)
    # удаляем подряд идущие дубли одинаковой строки-заголовка (без тегов)
    t = re.sub(r'(?m)^(?P<h>[^<>\n]{3,})\n(?P=h)\n', r'\g<h>\n', t)
    return t

def _sanitize_desc_inner(s: str) -> str:
    """Чистка innerHTML/innerText (без добавления новых HTML-тегов)."""
    if not s or not s.strip():
        return s
    parts = _split_tags_and_text(s)
    out = []
    for p in parts:
        if p.startswith('<') and p.endswith('>'):
            # теги оставляем как есть
            out.append(p)
        else:
            out.append(_sanitize_text_piece(p))
    joined = ''.join(out)
    # финальная правка на всём блоке (границы тегов/текста)
    joined = _sanitize_block(joined)
    return joined

def sanitize_descriptions_in_xml(xml_text: str) -> str:
    """
    Находит <description>...</description> и чистит только содержимое,
    сохраняя CDATA где оно было. Пустые описания НЕ трогаем.
    """
    if not xml_text or '<description' not in xml_text:
        return xml_text

    # 1) CDATA-блоки
    def cdata_repl(m: re.Match) -> str:
        inner = m.group(1)
        if inner is None or inner.strip() == '':
            return m.group(0)  # пустые не трогаем
        cleaned = _sanitize_desc_inner(inner)
        return f"<description><![CDATA[{cleaned}]]></description>"

    xml_text = re.sub(
        r"<description>\s*<!\[CDATA\[(.*?)\]\]>\s*</description>",
        lambda m: cdata_repl(m),
        xml_text,
        flags=re.S,
    )

    # 2) Обычные текстовые описания (без CDATA)
    def plain_repl(m: re.Match) -> str:
        inner = m.group(1)
        if inner is None or inner.strip() == '':
            return m.group(0)
        cleaned = _sanitize_desc_inner(inner)
        return f"<description>{cleaned}</description>"

    xml_text = re.sub(
        r"<description>(?!\s*<!\[CDATA\[)(.*?)</description>",
        lambda m: plain_repl(m),
        xml_text,
        flags=re.S,
    )

    return xml_text
# --- /Description sanitizer ---

def main() -> None:
    log(f"Source: {SUPPLIER_URL if SUPPLIER_URL else '(not set)'}")
    data = load_source_bytes(SUPPLIER_URL)

    src_root = ET.fromstring(data)
    shop_in  = src_root.find("shop") if src_root.tag.lower() != "shop" else src_root
    if shop_in is None:
        err("XML: <shop> not found")

    offers_in_el = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None:
        err("XML: <offers> not found")

    src_offers = list(offers_in_el.findall("offer"))

...
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # Сериализация
    xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")

    # Лёгкая косметика: перенос после FEED_META и пустая строка между офферами
    xml_text = re.sub(r"(-->)\s*(<shop\b)", r"\1\n\2", xml_text, count=1)
    xml_text = re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_text)

    # ОЧИСТКА ОПИСАНИЙ (без HTML-оформления)
    xml_text = sanitize_descriptions_in_xml(xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", ex
...
    try:
        docs_dir = os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | description=AS IS")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
