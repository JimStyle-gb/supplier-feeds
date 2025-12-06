#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlStyle post-process (v121)

Фиксы и унификация (точечно, без переформатирования XML):
1) Устойчивое чтение docs/alstyle.yml:
   - windows-1251 -> utf-8 -> utf-8(errors="replace")
   - удаляем UTF-8 BOM, если есть
2) Всегда сохраняем обратно в windows-1251:
   - неподдерживаемые символы -> XML-сущности (&#...;) через xmlcharrefreplace
3) Шапка файла:
   - убираем пустые строки/пробелы перед <?xml ...?>
   - убираем пустую строку между <?xml ...?> и <yml_catalog ...>
4) WhatsApp rgba: rgba(0,0,0,.08) -> rgba(0,0,0,0.08)
5) <picture> заглушка, если picture отсутствует в offer (вставка сразу после </price>)
6) Форс-diff для коммита (чтобы не было "No changes to commit") ТОЛЬКО когда нужно:
   - workflow_dispatch: да
   - FORCE_YML_REFRESH=1: да
   - push: только если сейчас в Алматы hour == SCHEDULE_HOUR_ALMATY
   - schedule: нет

Входной файл: OUT_FILE или docs/alstyle.yml
Если файла нет — выходим с code=2 (ничего не “создаём”).
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

    # 1) срезаем всё пустое/пробельное в самом начале до первого непустого
    s2 = s.lstrip(" \t\r\n")
    if s2 != s:
        changed = 1
        s = s2

    # 2) убираем пустые строки сразу после <?xml ...?>
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


# Форс-обновление времени (обновляем и FEED_META, и date="..." у yml_catalog)
def _bump_build_time_if_needed(src: str) -> tuple[str, int, int]:
    if not _should_force_refresh():
        return src, 0, 0

    now_s = _now_almaty_str()

    # 1) FEED_META всегда с секундами
    out, n_meta = RE_BUILD_TIME_LINE.subn(rf"\1{now_s}", src, count=1)

    # 2) date="..." сохраняем формат: если было без секунд — пишем без секунд
    def _date_repl(m: re.Match) -> str:
        old = m.group(2)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}", old):
            new = now_s[:16]
        else:
            new = now_s
        return m.group(1) + new + m.group(3)

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
    indent = m.group(1) if m else "\n            "

    insert = f"{indent}<picture>{PLACEHOLDER_PICTURE_URL}</picture>"
    return offer_body[: idx + len("</price>")] + insert + offer_body[idx + len("</price>") :], 1


# Применяет точечные правки
def _process_text(src: str) -> tuple[str, dict]:
    stats = {
        "offers_scanned": 0,
        "offers_pictures_added": 0,
        "rgba_fixed": 0,
        "header_fixed": 0,
        "build_time_bumped_meta": 0,
        "build_time_bumped_date": 0,
    }

    # 0) шапка
    src_h, header_fixed = _normalize_header(src)
    stats["header_fixed"] = header_fixed

    # 0.5) форс-время (если надо)
    src0, n_meta, n_date = _bump_build_time_if_needed(src_h)
    stats["build_time_bumped_meta"] = n_meta
    stats["build_time_bumped_date"] = n_date

    # 1) WhatsApp rgba
    src2, n = RE_RGBA_BAD.subn("rgba(0,0,0,0.08)", src0)
    stats["rgba_fixed"] = n

    # 2) picture заглушка по offer-блокам
    def repl(m: re.Match) -> str:
        head, body, tail = m.group(1), m.group(2), m.group(3)
        stats["offers_scanned"] += 1
        body2, added = _inject_picture_if_missing(body)
        stats["offers_pictures_added"] += added
        return head + body2 + tail

    out = RE_OFFER_BLOCK.sub(repl, src2)
    return out, stats


# Определяет входной файл по OUT_FILE или docs/alstyle.yml
def _resolve_infile(cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path)
    env_path = os.environ.get("OUT_FILE", "").strip()
    if env_path:
        return Path(env_path)
    return Path("docs/alstyle.yml")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", default=None, help="Путь к alstyle.yml (по умолчанию OUT_FILE или docs/alstyle.yml)")
    args = ap.parse_args(argv)

    path = _resolve_infile(args.infile)
    if not path.exists():
        print(f"[postprocess_alstyle] ERROR: file not found: {path}", file=sys.stderr)
        return 2

    src, enc, bom = _read_text(path)
    if enc != "windows-1251" or bom:
        info = []
        if enc != "windows-1251":
            info.append(f"encoding={enc}")
        if bom:
            info.append("bom=stripped")
        print(f"[postprocess_alstyle] WARN: input {'; '.join(info)} (сохраним как windows-1251)", file=sys.stderr)

    out, stats = _process_text(src)

    # Переписываем, если:
    # - были правки
    # - или вход был не cp1251 / был BOM (нормализуем)
    if out != src or enc != "windows-1251" or bom:
        _write_text(path, out)

    bumped = stats["build_time_bumped_meta"] + stats["build_time_bumped_date"]
    print(
        "[postprocess_alstyle] OK | "
        f"offers={stats['offers_scanned']} | "
        f"pictures_added={stats['offers_pictures_added']} | "
        f"rgba_fixed={stats['rgba_fixed']} | "
        f"header_fixed={stats['header_fixed']} | "
        f"build_time_bumped={bumped} (meta={stats['build_time_bumped_meta']}, date={stats['build_time_bumped_date']}) | "
        f"file={path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
