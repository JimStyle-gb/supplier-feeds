#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VTT post-process (v59)

Фиксы (точечно, без XML-переформатирования):
1) Устойчивое чтение docs/vtt.yml:
   - windows-1251 -> utf-8 -> utf-8(errors="replace")
   - удаляем UTF-8 BOM, если есть
2) Всегда сохраняем обратно в windows-1251:
   - неподдерживаемые символы -> XML-сущности (&#...;) через xmlcharrefreplace
3) Шапка файла:
   - убираем пустые строки/пробелы перед <?xml ...?>
   - убираем пустую строку между <?xml ...?> и <yml_catalog ...>
4) FEED_META: если строка "Время сборки (Алматы)" повреждена (например "P25-12-07 ..."),
   то восстанавливаем её как у остальных поставщиков
   + синхронизируем "Время сборки" с <yml_catalog date="..."> (чтобы как у других совпадало по минутам)
5) Описание (CDATA): выравниваем как у большинства:
   - ровно 2 перевода строки в начале и в конце CDATA (для каждого offer)
6) WhatsApp-кнопка: чтобы блок был как у остальных —
   - добавляем "&#128172; " перед текстом "НАЖМИТЕ..." если его нет
7) WhatsApp rgba: rgba(0,0,0,.08) -> rgba(0,0,0,0.08)
8) <picture> заглушка, если picture отсутствует в offer (вставка сразу после </price>)
9) Форс-diff для коммита (чтобы не было "No changes to commit") ТОЛЬКО когда нужно:
   - workflow_dispatch: да
   - FORCE_YML_REFRESH=1: да
   - push: только если сейчас в Алматы hour == SCHEDULE_HOUR_ALMATY
   - schedule: нет

Входной файл: OUT_FILE или docs/vtt.yml
Если файла нет — code=2.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import os
import re
import sys
from pathlib import Path

PLACEHOLDER_PICTURE_URL = (
    "https://images.satu.kz/227774166_w1280_h1280_cid41038_pid120085106-4f006b4f.jpg?fresh=1"
)

RE_OFFER_BLOCK = re.compile(r"(<offer\b[^>]*>)(.*?)(</offer>)", re.DOTALL)
RE_RGBA_BAD = re.compile(r"rgba\(0,0,0,\.08\)")
RE_PRICE_LINE = re.compile(r"(\n[ \t]*)<price>")

RE_FEED_META_BLOCK = re.compile(r"<!--FEED_META\n(.*?)\n-->", re.DOTALL)

RE_BUILD_TIME_LINE = re.compile(
    r"(Время сборки\s*\(Алматы\)\s*\|\s*)(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
    re.IGNORECASE,
)
RE_YML_CATALOG_DATE = re.compile(r'(<yml_catalog\b[^>]*\bdate=")([^"]*)(")', re.IGNORECASE)

RE_DESC_CDATA = re.compile(r"(<description><!\[CDATA\[)(.*?)(\]\]></description>)", re.DOTALL)
RE_WA_BTN_NO_EMOJI = re.compile(r'(">\s*)(НАЖМИТЕ,\s*ЧТОБЫ\s*НАПИС)', re.IGNORECASE)


# Текущее время Алматы (UTC+5)
def _now_almaty_dt() -> datetime:
    return datetime.utcnow() + timedelta(hours=5)


def _now_almaty_str() -> str:
    return _now_almaty_dt().strftime("%Y-%m-%d %H:%M:%S")


def _now_almaty_str_min() -> str:
    return _now_almaty_dt().strftime("%Y-%m-%d %H:%M")


# Нужно ли форсить обновление YML (чтобы появился diff для коммита)
def _should_force_refresh() -> bool:
    v = os.environ.get("FORCE_YML_REFRESH", "").strip().lower()
    if v in ("1", "true", "yes", "y"):
        return True

    ev = os.environ.get("GITHUB_EVENT_NAME", "").strip().lower()
    if ev == "workflow_dispatch":
        return True

    if ev == "push":
        try:
            want_h = int((os.environ.get("SCHEDULE_HOUR_ALMATY", "0") or "0").strip())
        except Exception:
            want_h = 0
        return _now_almaty_dt().hour == want_h

    return False


# Читает файл устойчиво (cp1251 -> utf8 -> utf8 replace), убирает BOM
def _read_text(path: Path) -> tuple[str, str, int]:
    data = path.read_bytes()
    bom = 0
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
        bom = 1

    try:
        return data.decode("windows-1251"), "windows-1251", bom
    except UnicodeDecodeError:
        pass

    try:
        return data.decode("utf-8"), "utf-8", bom
    except UnicodeDecodeError:
        return data.decode("utf-8", errors="replace"), "utf-8(replace)", bom


# Пишет файл строго windows-1251, неподдерживаемые символы -> XML-сущности
def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("windows-1251", errors="xmlcharrefreplace"))


# Чинит верх файла: убирает пустые строки до <?xml> и после него
def _normalize_header(src: str) -> tuple[str, int]:
    s = src.lstrip("\ufeff")
    changed = 0

    s2 = s.lstrip(" \t\r\n")
    if s2 != s:
        changed = 1
        s = s2

    lines = s.splitlines(True)
    if not lines:
        return s, changed

    first = lines[0]
    if first.lstrip().startswith("<?xml"):
        first = first.rstrip("\r\n") + "\n"
        i = 1
        while i < len(lines) and lines[i].strip() == "":
            i += 1
            changed = 1
        return first + "".join(lines[i:]), changed

    return s, changed


# Парсит дату из кривой строки типа "P25-12-07 05:27:12" -> "2025-12-07 05:27:12"
def _parse_weird_dt(line: str) -> str | None:
    s = line.strip()

    m = re.fullmatch(r"\D*(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\D*", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)} {m.group(4)}:{m.group(5)}:{m.group(6)}"

    m = re.fullmatch(r"\D*(\d{2})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\D*", s)
    if m:
        yy = int(m.group(1))
        return f"20{yy:02d}-{m.group(2)}-{m.group(3)} {m.group(4)}:{m.group(5)}:{m.group(6)}"

    return None


# Берём yml_catalog date (минуты)
def _get_yml_date(src: str) -> str | None:
    m = RE_YML_CATALOG_DATE.search(src)
    if not m:
        return None
    return m.group(2)


# Восстанавливает строку "Время сборки (Алматы)" в FEED_META, если она повреждена/отсутствует
def _fix_feed_meta_build_time(src: str) -> tuple[str, int]:
    m = RE_FEED_META_BLOCK.search(src)
    if not m:
        return src, 0

    body = m.group(1)
    lines = body.splitlines()

    for ln in lines:
        if "Время сборки" in ln:
            return src, 0

    url_idx = None
    next_idx = None
    for i, ln in enumerate(lines):
        if url_idx is None and ln.startswith("URL поставщика"):
            url_idx = i
        if next_idx is None and ln.startswith("Ближайшая сборка"):
            next_idx = i

    lo = (url_idx + 1) if url_idx is not None else 0
    hi = next_idx if next_idx is not None else len(lines)

    cand_i = None
    dt = None
    for i in range(lo, hi):
        dt2 = _parse_weird_dt(lines[i])
        if dt2:
            cand_i = i
            dt = dt2
            break

    if cand_i is None:
        return src, 0

    lines[cand_i] = f"Время сборки (Алматы)                      | {dt}"
    new_body = "\n".join(lines)
    new_block = "<!--FEED_META\n" + new_body + "\n-->"
    out = src[: m.start()] + new_block + src[m.end() :]
    return out, 1


# Синхронизирует "Время сборки" с yml_catalog date (чтобы совпадало по минутам)
def _sync_feed_meta_time_with_yml_date(src: str) -> tuple[str, int]:
    y = _get_yml_date(src)
    if not y:
        return src, 0

    # y обычно "YYYY-MM-DD HH:MM"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}", y):
        y_full = y + ":00"
    else:
        # если уже с секундами
        y_full = y

    m = RE_FEED_META_BLOCK.search(src)
    if not m:
        return src, 0

    body = m.group(1)
    lines = body.splitlines()

    changed = 0
    for i, ln in enumerate(lines):
        mm = RE_BUILD_TIME_LINE.search(ln)
        if mm:
            lines[i] = "Время сборки (Алматы)                      | " + y_full
            changed = 1
            break

    if not changed:
        return src, 0

    new_body = "\n".join(lines)
    new_block = "<!--FEED_META\n" + new_body + "\n-->"
    out = src[: m.start()] + new_block + src[m.end() :]
    return out, 1


# Форс-обновление времени (обновляем FEED_META и date="..." у yml_catalog)
def _bump_build_time_if_needed(src: str) -> tuple[str, int, int]:
    if not _should_force_refresh():
        return src, 0, 0

    now_s = _now_almaty_str()
    now_min = _now_almaty_str_min()

    out, n_meta = RE_BUILD_TIME_LINE.subn(rf"\1{now_s}", src, count=1)
    out2, n_date = RE_YML_CATALOG_DATE.subn(lambda mm: mm.group(1) + now_min + mm.group(3), out, count=1)
    return out2, n_meta, n_date


# Нормализуем CDATA: ровно 2 \n в начале и в конце
def _normalize_description_cdata_2nl(src: str) -> tuple[str, int]:
    fixed = 0

    def repl(m: re.Match) -> str:
        nonlocal fixed
        head, body, tail = m.group(1), m.group(2), m.group(3)
        b = body.replace("\r\n", "\n")
        core = b.lstrip("\n").rstrip("\n")
        out = "\n\n" + core + "\n\n"
        if out != b:
            fixed += 1
        return head + out + tail

    out = RE_DESC_CDATA.sub(repl, src)
    return out, fixed


# Добавляем эмодзи-энтити в кнопку WhatsApp, если нет
def _ensure_wa_emoji(src: str) -> tuple[str, int]:
    # только если есть кнопка без эмодзи
    out, n = RE_WA_BTN_NO_EMOJI.subn(r"\1&#128172; \2", src)
    return out, n


# Вставляет <picture> заглушку в offer, если picture отсутствует
def _inject_picture_if_missing(offer_body: str) -> tuple[str, int]:
    if "<picture>" in offer_body:
        return offer_body, 0

    idx = offer_body.find("</price>")
    if idx == -1:
        return offer_body, 0

    m = RE_PRICE_LINE.search(offer_body)
    indent = m.group(1) if m else "\n"

    insert = f"{indent}<picture>{PLACEHOLDER_PICTURE_URL}</picture>"
    new_body = offer_body[: idx + len("</price>")] + insert + offer_body[idx + len("</price>") :]
    return new_body, 1


# Применяет точечные правки
def _process_text(src: str) -> tuple[str, dict]:
    stats = {
        "offers_scanned": 0,
        "offers_pictures_added": 0,
        "rgba_fixed": 0,
        "header_fixed": 0,
        "feed_meta_time_fixed": 0,
        "feed_meta_time_synced": 0,
        "desc_cdata_fixed": 0,
        "wa_emoji_added": 0,
        "build_time_bumped_meta": 0,
        "build_time_bumped_date": 0,
    }

    s, hf = _normalize_header(src)
    stats["header_fixed"] = hf

    s, fm = _fix_feed_meta_build_time(s)
    stats["feed_meta_time_fixed"] = fm

    # сначала синхронизация с yml_catalog (как у других)
    s, sync = _sync_feed_meta_time_with_yml_date(s)
    stats["feed_meta_time_synced"] = sync

    s, n_meta, n_date = _bump_build_time_if_needed(s)
    stats["build_time_bumped_meta"] = n_meta
    stats["build_time_bumped_date"] = n_date

    # если bump был — снова синхронизация (чтобы было 1:1)
    if n_date or n_meta:
        s, sync2 = _sync_feed_meta_time_with_yml_date(s)
        stats["feed_meta_time_synced"] = stats["feed_meta_time_synced"] or sync2

    s, n_cdata = _normalize_description_cdata_2nl(s)
    stats["desc_cdata_fixed"] = n_cdata

    s, n_wa = _ensure_wa_emoji(s)
    stats["wa_emoji_added"] = n_wa

    s, n_rgba = RE_RGBA_BAD.subn("rgba(0,0,0,0.08)", s)
    stats["rgba_fixed"] = n_rgba

    def repl(mo: re.Match) -> str:
        head, body, tail = mo.group(1), mo.group(2), mo.group(3)
        stats["offers_scanned"] += 1
        body2, added = _inject_picture_if_missing(body)
        stats["offers_pictures_added"] += added
        return head + body2 + tail

    out = RE_OFFER_BLOCK.sub(repl, s)
    return out, stats


# Определяет входной файл по OUT_FILE или docs/vtt.yml
def _resolve_infile(cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path)
    env_path = os.environ.get("OUT_FILE", "").strip()
    if env_path:
        return Path(env_path)
    return Path("docs/vtt.yml")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", default=None, help="Путь к vtt.yml (по умолчанию OUT_FILE или docs/vtt.yml)")
    args = ap.parse_args(argv)

    path = _resolve_infile(args.infile)
    if not path.exists():
        print(f"[postprocess_vtt] ERROR: file not found: {path}", file=sys.stderr)
        return 2

    src, enc, bom = _read_text(path)
    if enc != "windows-1251" or bom:
        info = []
        if enc != "windows-1251":
            info.append(f"encoding={enc}")
        if bom:
            info.append("bom=stripped")
        print(f"[postprocess_vtt] WARN: input {'; '.join(info)} (сохраним как windows-1251)", file=sys.stderr)

    out, stats = _process_text(src)

    if out != src or enc != "windows-1251" or bom:
        _write_text(path, out)

    bumped = stats["build_time_bumped_meta"] + stats["build_time_bumped_date"]
    print(
        "[postprocess_vtt] OK | "
        f"offers={stats['offers_scanned']} | "
        f"pictures_added={stats['offers_pictures_added']} | "
        f"rgba_fixed={stats['rgba_fixed']} | "
        f"header_fixed={stats['header_fixed']} | "
        f"feed_meta_time_fixed={stats['feed_meta_time_fixed']} | "
        f"feed_meta_time_synced={stats['feed_meta_time_synced']} | "
        f"desc_cdata_fixed={stats['desc_cdata_fixed']} | "
        f"wa_emoji_added={stats['wa_emoji_added']} | "
        f"build_time_bumped={bumped} (meta={stats['build_time_bumped_meta']}, date={stats['build_time_bumped_date']}) | "
        f"file={path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
