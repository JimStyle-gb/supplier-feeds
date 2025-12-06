#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AlStyle post-process (v124)
1) Унификация WhatsApp-блока (rgba(0,0,0,.08) -> rgba(0,0,0,0.08))
2) Добавление picture-заглушки, если <picture> отсутствует в offer
3) Форс-обновление YML для коммита (чтобы не было "No changes to commit"):
   - workflow_dispatch: всегда
   - push: только если сейчас в Алматы hour == SCHEDULE_HOUR_ALMATY
   - schedule: не форсим (чтобы не плодить пустые коммиты)
   - FORCE_YML_REFRESH=1: всегда
4) Если docs/alstyle.yml отсутствует — создаём минимальный валидный yml_catalog (пустые offers),
   чтобы пайплайн не падал. Это спасает после случайного удаления файла.

Как форсим:
- сначала пытаемся обновить строку "Время сборки (Алматы) | ..."
- если такой строки нет/не совпала — обновляем date="..." у <yml_catalog ...>

Важно: только точечные правки по тексту (без XML-переформатирования).
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

RE_YML_CATALOG_DATE = re.compile(
    r'(<yml_catalog\b[^>]*\bdate=")([^"]*)(")',
    re.IGNORECASE,
)


# Читает текст файла в windows-1251
def _read_text(path: Path) -> str:
    return path.read_text(encoding="windows-1251")


# Пишет текст файла в windows-1251
def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="windows-1251")


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
            want_h = int(os.environ.get("SCHEDULE_HOUR_ALMATY", "0").strip() or "0")
        except Exception:
            want_h = 0
        now_h = _now_almaty_dt().hour
        return now_h == want_h

    return False


# Создаёт минимальный yml_catalog, если файла нет (пустые offers)
def _ensure_file_exists(path: Path) -> int:
    if path.exists():
        return 0
    now_s = _now_almaty_str()
    stub = (
        '<?xml version="1.0" encoding="windows-1251"?>\n'
        f'<yml_catalog date="{now_s}">\n'
        "  <shop>\n"
        "    <offers>\n"
        "    </offers>\n"
        "  </shop>\n"
        "</yml_catalog>\n"
    )
    _write_text(path, stub)
    return 1


# Форс-обновление времени в мета-строке или date="..." у yml_catalog
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

    created = _ensure_file_exists(path)

    src = _read_text(path)
    out, stats = _process_text(src)

    if out != src:
        _write_text(path, out)

    bumped = stats["build_time_bumped_meta"] + stats["build_time_bumped_date"]
    print(
        "[postprocess_alstyle] OK | "
        f"offers={stats['offers_scanned']} | "
        f"pictures_added={stats['offers_pictures_added']} | "
        f"rgba_fixed={stats['rgba_fixed']} | "
        f"build_time_bumped={bumped} (meta={stats['build_time_bumped_meta']}, date={stats['build_time_bumped_date']}) | "
        f"created={created} | "
        f"file={path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
