#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlStyle post-process (v119)
Fix: UnicodeDecodeError при чтении docs/alstyle.yml после восстановления через GitHub (файл часто получается UTF-8).

1) Чтение: пробуем windows-1251, если не получилось — пробуем utf-8, иначе читаем с заменой.
2) Запись: всегда сохраняем в windows-1251, неподдерживаемые символы -> XML-сущности (&#...;), чтобы не падать.
3) Опционально форсим “diff” для коммита (чтобы не было "No changes to commit"):
   - workflow_dispatch: да
   - FORCE_YML_REFRESH=1: да
   - push: только если сейчас в Алматы hour == SCHEDULE_HOUR_ALMATY
   (schedule не форсим)
4) Унификация WhatsApp rgba + picture-заглушка, как в v118.

Важно: скрипт делает ТОЛЬКО точечные правки по тексту файла (без XML-переформатирования).
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


# Нужно ли форсить обновление YML (ставим diff для коммита)
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


# Читает файл: сначала windows-1251, потом utf-8, иначе с заменой
def _read_text(path: Path) -> tuple[str, str]:
    data = path.read_bytes()

    try:
        return data.decode("windows-1251"), "windows-1251"
    except UnicodeDecodeError:
        pass

    try:
        return data.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        txt = data.decode("utf-8", errors="replace")
        return txt, "utf-8(replace)"


# Пишет файл в windows-1251, неподдерживаемые символы -> XML-сущности
def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = text.encode("windows-1251", errors="xmlcharrefreplace")
    path.write_bytes(data)


# Форс-обновление времени: сначала “Время сборки…”, иначе date="..." у <yml_catalog>
def _bump_build_time_if_needed(src: str) -> tuple[str, int, int]:
    if not _should_force_refresh():
        return src, 0, 0

    now_s = _now_almaty_str()

    out, n_meta = RE_BUILD_TIME_LINE.subn(rf"\1{now_s}", src, count=1)
    if n_meta:
        return out, n_meta, 0

    out2, n_date = RE_YML_CATALOG_DATE.subn(rf'\1{now_s}\3', out, count=1)
    return out2, 0, n_date


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
    new_body = offer_body[: idx + len("</price>")] + insert + offer_body[idx + len("</price>") :]
    return new_body, 1


# Применяет точечные правки
def _process_text(src: str) -> tuple[str, dict]:
    stats = {
        "offers_scanned": 0,
        "offers_pictures_added": 0,
        "rgba_fixed": 0,
        "build_time_bumped_meta": 0,
        "build_time_bumped_date": 0,
    }

    src0, n_meta, n_date = _bump_build_time_if_needed(src)
    stats["build_time_bumped_meta"] = n_meta
    stats["build_time_bumped_date"] = n_date

    src2, n = RE_RGBA_BAD.subn("rgba(0,0,0,0.08)", src0)
    stats["rgba_fixed"] = n

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

    src, enc = _read_text(path)
    if enc != "windows-1251":
        print(f"[postprocess_alstyle] WARN: input encoding={enc} (будет сохранено как windows-1251)", file=sys.stderr)

    out, stats = _process_text(src)

    # Всегда переписываем, если:
    # - были правки
    # - или вход был не cp1251 (нормализуем)
    if out != src or enc != "windows-1251":
        _write_text(path, out)

    bumped = stats["build_time_bumped_meta"] + stats["build_time_bumped_date"]
    print(
        "[postprocess_alstyle] OK | "
        f"offers={stats['offers_scanned']} | "
        f"pictures_added={stats['offers_pictures_added']} | "
        f"rgba_fixed={stats['rgba_fixed']} | "
        f"build_time_bumped={bumped} (meta={stats['build_time_bumped_meta']}, date={stats['build_time_bumped_date']}) | "
        f"file={path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
