# scripts/build_price.py
# -*- coding: utf-8 -*-
"""
Сборка единого прайса docs/price.yml из 5 источников с выводом ровно как в «должно быть».

Формат:
- Шапка:
    <?xml ...>
    <yml_catalog date="...">

    <!--FEED_META
    Поставщик | Price
    ...
    Разбивка по источникам | AlStyle:..., AkCent:..., CopyLine:..., NVPrint:..., VTT:...-->
- Затем блоки FEED_META каждого поставщика, между блоками одна пустая строка.
- Затем <shop><offers> с офферами; между </offer> и следующим <offer> ровно одна пустая строка.

Прочее:
- Дедуп по <vendorCode> (берём первый).
- id не трогаем (берём как есть из поставщиков).
- Кодировка I/O: windows-1251.
"""

from __future__ import annotations
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone

# --- Пути и источники ---
ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
OUT  = DOCS / "price.yml"

SOURCES = [
    ("AlStyle",  DOCS / "alstyle.yml",  "alstyle"),
    ("AkCent",   DOCS / "akcent.yml",   "akcent"),
    ("CopyLine", DOCS / "copyline.yml", "copyline"),
    ("NVPrint",  DOCS / "nvprint.yml",  "nvprint"),
    ("VTT",      DOCS / "vtt.yml",      "vtt"),
]

ENC = "cp1251"

# --- Регексы ---
RX_OFFER       = re.compile(r"<offer\b.*?</offer>", re.I | re.S)
RX_FEED_META   = re.compile(r"<!--\s*FEED_META\s*(.*?)\s*-->", re.I | re.S)
RX_VENDORCODE  = re.compile(r"<vendorCode>\s*([^<\s]+)\s*</vendorCode>", re.I)
RX_AVAILABLE   = re.compile(r"<available>\s*(true|false)\s*</available>", re.I)
RX_SUPPLIER_LN = re.compile(r"^(Поставщик\s*\|)(.*)$", re.M)

# --- Время Алматы (UTC+6, как в твоих примерах) ---
ALMATY_TZ = timezone(timedelta(hours=6))

def now_almaty() -> datetime:
    return datetime.now(ALMATY_TZ)

def fmt_meta_dt(dt: datetime) -> str:
    return dt.strftime("%d:%m:%Y - %H:%M:%S")

def fmt_yml_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")

# --- IO ---
def rtext(p: Path) -> str:
    return p.read_text(encoding=ENC, errors="replace")

def wtext(p: Path, txt: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(txt, encoding=ENC, errors="replace")

# --- Парсинг исходников ---
def extract_offers(xml: str) -> list[str]:
    return RX_OFFER.findall(xml)

def extract_feed_meta_inner(xml: str) -> str:
    """
    Возвращает «внутренность» первого блока FEED_META (без <!--FEED_META / -->).
    Удаляет вложенные <!-- и -->, лишние пробелы по краям.
    """
    m = RX_FEED_META.search(xml)
    if not m:
        return ""
    inner = m.group(1)
    inner = inner.replace("<!--", "").replace("-->", "")
    return inner.strip()

def normalize_supplier_name_in_meta(inner: str, display_name: str) -> str:
    """
    Внутри блока FEED_META заменяет строку 'Поставщик | ...' на нужное display_name.
    Сохраняет остальные строки как есть.
    """
    def _repl(m: re.Match) -> str:
        return f"{m.group(1)} {display_name}"
    return RX_SUPPLIER_LN.sub(_repl, inner, count=1)

def dedupe_by_vendorcode(offers: list[str]) -> tuple[list[str], int]:
    seen = set()
    out  = []
    for off in offers:
        m = RX_VENDORCODE.search(off)
        code = (m.group(1).strip() if m else None)
        if not code:
            out.append(off)
            continue
        if code in seen:
            continue
        seen.add(code)
        out.append(off)
    return out, len(seen)

def count_availability(offers: list[str]) -> tuple[int,int]:
    t = f = 0
    for off in offers:
        m = RX_AVAILABLE.search(off)
        if m:
            if m.group(1).lower() == "true":
                t += 1
            else:
                f += 1
    return t, f

# --- Сборка ---
def main() -> None:
    sources_data: list[tuple[str, list[str], str]] = []  # (DisplayName, offers[], FEED_META inner)
    per_source_count: dict[str,int] = {}
    total_in = 0

    # читаем все источники (сохраняем порядок)
    for display, path, key in SOURCES:
        if not path.exists():
            per_source_count[display] = 0
            continue
        txt = rtext(path)
        offers = extract_offers(txt)
        inner  = extract_feed_meta_inner(txt)
        if inner:
            inner = normalize_supplier_name_in_meta(inner, display)
        sources_data.append((display, offers, inner))
        per_source_count[display] = len(offers)
        total_in += len(offers)

    # объединяем офферы и дедупим
    merged_offers: list[str] = []
    for display, offers, _ in sources_data:
        merged_offers.extend(offers)

    merged_offers, _unique = dedupe_by_vendorcode(merged_offers)
    total_after = len(merged_offers)
    avail_true, avail_false = count_availability(merged_offers)

    # разбивка как в примере «должно быть» (строго по SOURCES порядку)
    breakdown = ", ".join(f"{display}:{per_source_count.get(display,0)}" for display,_,_ in SOURCES)

    # --- Шапка XML ---
    now = now_almaty()
    xml_lines = []
    xml_lines.append("<?xml version='1.0' encoding='windows-1251'?>")
    xml_lines.append(f"<yml_catalog date=\"{fmt_yml_dt(now)}\">")
    xml_lines.append("")  # пустая строка как в «должно быть»

    # --- Общий FEED_META (Price) — закрывающий '-->' сразу после строки ---
    xml_lines.append("<!--FEED_META")
    xml_lines.append("Поставщик                                  | Price")
    xml_lines.append(f"Время сборки (Алматы)                      | {fmt_meta_dt(now)}")
    xml_lines.append(f"Сколько товаров у поставщика до фильтра    | {total_in}")
    xml_lines.append(f"Сколько товаров у поставщика после фильтра | {total_after}")
    xml_lines.append(f"Сколько товаров есть в наличии (true)      | {avail_true}")
    xml_lines.append(f"Сколько товаров нет в наличии (false)      | {avail_false}")
    xml_lines.append(f"Дубликатов по vendorCode отброшено         | 0")
    xml_lines.append(f"Разбивка по источникам                     | {breakdown}-->")
    xml_lines.append("")  # пустая строка между FEED_META-блоками

    # --- Блоки FEED_META поставщиков (как есть, но с нормализованным 'Поставщик | ...') ---
    for display, _offers, inner in sources_data:
        if not inner:
            continue
        # inner уже без оболочки; закроем так же «в строку»
        xml_lines.append("<!--FEED_META")
        xml_lines.append(inner)
        # убрать возможные лишние пробелы в конце последней строки перед '-->'
        if xml_lines[-1].endswith(" "):
            xml_lines[-1] = xml_lines[-1].rstrip()
        xml_lines[-1] = xml_lines[-1]  # последняя строка тела остаётся последней строкой блока
        xml_lines.append("-->")
        xml_lines.append("")  # пустая строка-разделитель

    # --- Начало shop/offers ---
    xml_lines.append("  <shop>")
    xml_lines.append("    <offers>")

    # офферы: между ними ровно одна пустая строка; все строки внутри оффера отступаем на 6 пробелов
    # (берём оффер как есть из источника)
    if merged_offers:
        joined = "\n\n".join(merged_offers).rstrip() + "\n"
        # смещение на 6 пробелов
        indented = "\n".join(("      " + ln if ln else "") for ln in joined.splitlines())
        # сохранить пустые строки между офферами:
        indented = indented.replace("\n      \n", "\n\n")
        xml_lines.append(indented.rstrip())
    # закрытия
    xml_lines.append("    </offers>")
    xml_lines.append("  </shop>")
    xml_lines.append("</yml_catalog>")

    # запись
    wtext(OUT, "\n".join(xml_lines) + "\n")

    # на всякий — .nojekyll
    try:
        (DOCS / ".nojekyll").write_bytes(b"")
    except Exception:
        pass

    print(f"[price] Wrote {OUT.relative_to(ROOT)} | before={total_in} after={total_after} true={avail_true} false={avail_false}")

if __name__ == "__main__":
    main()
