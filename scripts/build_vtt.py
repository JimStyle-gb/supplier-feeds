#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VTT post-process (v58)

Фиксы (точечно, без XML-переформатирования):
1) Устойчивое чтение docs/vtt.yml:
   - windows-1251 -> utf-8 -> utf-8(errors="replace")
   - удаляем UTF-8 BOM, если есть
2) Всегда сохраняем обратно в windows-1251:
   - неподдерживаемые символы -> XML-сущности (&#...;) через xmlcharrefreplace
3) Шапка файла:
   - убираем пустые строки/пробелы перед <?xml ...?>
   - убираем пустую строку между <?xml ...?> и <yml_catalog ...>
4) FEED_META: если строка "Время сборки (Алматы)" повреждена (например "P25-12-07 05:27:12"),
   то восстанавливаем её как у остальных поставщиков:
   "Время сборки (Алматы)                      | 2025-12-07 05:27:12"
5) WhatsApp rgba: rgba(0,0,0,.08) -> rgba(0,0,0,0.08)
6) <picture> заглушка, если picture отсутствует в offer (вставка сразу после </price>)
7) Форс-diff для коммита (чтобы не было "No changes to commit") ТОЛЬКО когда нужно:
   - workflow_dispatch: да
   - FORCE_YML_REFRESH=1: да
   - push: только если сейчас в Алматы hour == SCHEDULE_HOUR_ALMATY
   - schedule: нет

ВАЖНО: двойные переносы строк внутри CDATA НЕ трогаем (оставляем как есть).

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


# Текущее время Алматы (UTC+5)
def _now_almaty_dt() -> datetime:
    return datetime.utcnow() + timedelta(hours=5)


def _now_almaty_str() -> str:
    return _now_almaty_dt().strftime("%Y-%m-%d %H:%M:%S")


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

    # 2025-12-07 05:27:12
    m = re.fullmatch(r"\D*(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\D*", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)} {m.group(4)}:{m.group(5)}:{m.group(6)}"

    # 25-12-07 05:27:12
    m = re.fullmatch(r"\D*(\d{2})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\D*", s)
    if m:
        yy = int(m.group(1))
        return f"20{yy:02d}-{m.group(2)}-{m.group(3)} {m.group(4)}:{m.group(5)}:{m.group(6)}"

    return None


# Восстанавливает строку "Время сборки (Алматы)" в FEED_META, если она повреждена/отсутствует
def _fix_feed_meta_build_time(src: str) -> tuple[str, int]:
    m = RE_FEED_META_BLOCK.search(src)
    if not m:
        return src, 0

    body = m.group(1)
    lines = body.splitlines()

    # Если уже есть строка "Время сборки" — ничего не делаем
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

    # Как у остальных поставщиков: фиксированная колонка
    lines[cand_i] = f"Время сборки (Алматы)                      | {dt}"
    new_body = "\n".join(lines)

    new_block = "<!--FEED_META\n" + new_body + "\n-->"
    out = src[: m.start()] + new_block + src[m.end() :]
    return out, 1


# Форс-обновление времени (обновляем FEED_META и date="..." у yml_catalog)
def _bump_build_time_if_needed(src: str) -> tuple[str, int, int]:
    if not _should_force_refresh():
        return src, 0, 0

    now_s = _now_almaty_str()

    out, n_meta = RE_BUILD_TIME_LINE.subn(rf"\1{now_s}", src, count=1)

    def _date_repl(mm: re.Match) -> str:
        old = mm.group(2)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}", old):
            new = now_s[:16]
        else:
            new = now_s
        return mm.group(1) + new + mm.group(3)

    out2, n_date = RE_YML_CATALOG_DATE.subn(_date_repl, out, count=1)
    return out2, n_meta, n_date


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
        "feed_meta_build_time_fixed": 0,
        "build_time_bumped_meta": 0,
        "build_time_bumped_date": 0,
    }

    src_h, header_fixed = _normalize_header(src)
    stats["header_fixed"] = header_fixed

    src_m, fm_fixed = _fix_feed_meta_build_time(src_h)
    stats["feed_meta_build_time_fixed"] = fm_fixed

    src0, n_meta, n_date = _bump_build_time_if_needed(src_m)
    stats["build_time_bumped_meta"] = n_meta
    stats["build_time_bumped_date"] = n_date

    src1, n_rgba = RE_RGBA_BAD.subn("rgba(0,0,0,0.08)", src0)
    stats["rgba_fixed"] = n_rgba

    def repl(mo: re.Match) -> str:
        head, body, tail = mo.group(1), mo.group(2), mo.group(3)
        stats["offers_scanned"] += 1
        body2, added = _inject_picture_if_missing(body)
        stats["offers_pictures_added"] += added
        return head + body2 + tail

    out = RE_OFFER_BLOCK.sub(repl, src1)
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
        f"feed_meta_time_fixed={stats['feed_meta_build_time_fixed']} | "
        f"build_time_bumped={bumped} (meta={stats['build_time_bumped_meta']}, date={stats['build_time_bumped_date']}) | "
        f"file={path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
