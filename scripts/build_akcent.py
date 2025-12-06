#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AkCent post-process (v53)
1) Унификация WhatsApp-блока (на случай если где-то попадётся rgba(0,0,0,.08))
2) Добавление picture-заглушки, если <picture> отсутствует в offer

Важно: скрипт делает ТОЛЬКО точечные правки по тексту файла (без XML-переформатирования).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

PLACEHOLDER_PICTURE_URL = (
    "https://images.satu.kz/227774166_w1280_h1280_cid41038_pid120085106-4f006b4f.jpg?fresh=1"
)

RE_OFFER_BLOCK = re.compile(r"(<offer\\b[^>]*>)(.*?)(</offer>)", re.DOTALL)
RE_RGBA_BAD = re.compile(r"rgba\\(0,0,0,\\.08\\)")
RE_PRICE_LINE = re.compile(r"(\\n[ \\t]*)<price>")


# Читает файл в windows-1251
def _read_text(path: Path) -> str:
    return path.read_text(encoding="windows-1251")


# Пишет файл в windows-1251
def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="windows-1251")


# Вставляет <picture> заглушку в offer, если picture отсутствует
def _inject_picture_if_missing(offer_body: str) -> tuple[str, int]:
    if "<picture>" in offer_body:
        return offer_body, 0

    idx = offer_body.find("</price>")
    if idx == -1:
        return offer_body, 0

    m = RE_PRICE_LINE.search(offer_body)
    indent = m.group(1) if m else "\\n            "

    insert = f"{indent}<picture>{PLACEHOLDER_PICTURE_URL}</picture>"
    new_body = offer_body[: idx + len("</price>")] + insert + offer_body[idx + len("</price>") :]
    return new_body, 1


# Применяет правки без изменения общего форматирования
def _process_text(src: str) -> tuple[str, dict]:
    stats = {"offers_scanned": 0, "offers_pictures_added": 0, "rgba_fixed": 0}

    src2, n = RE_RGBA_BAD.subn("rgba(0,0,0,0.08)", src)
    stats["rgba_fixed"] = n

    def repl(m: re.Match) -> str:
        head, body, tail = m.group(1), m.group(2), m.group(3)
        stats["offers_scanned"] += 1
        body2, added = _inject_picture_if_missing(body)
        stats["offers_pictures_added"] += added
        return head + body2 + tail

    out = RE_OFFER_BLOCK.sub(repl, src2)
    return out, stats


# Определяет входной файл по OUT_FILE или docs/akcent.yml
def _resolve_infile(cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path)
    env_path = os.environ.get("OUT_FILE", "").strip()
    if env_path:
        return Path(env_path)
    return Path("docs/akcent.yml")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", default=None, help="Путь к akcent.yml (по умолчанию OUT_FILE или docs/akcent.yml)")
    args = ap.parse_args(argv)

    path = _resolve_infile(args.infile)
    if not path.exists():
        print(f"[postprocess_akcent] ERROR: file not found: {path}", file=sys.stderr)
        return 2

    src = _read_text(path)
    out, stats = _process_text(src)

    if out != src:
        _write_text(path, out)

    print(
        "[postprocess_akcent] OK | "
        f"offers={stats['offers_scanned']} | "
        f"pictures_added={stats['offers_pictures_added']} | "
        f"rgba_fixed={stats['rgba_fixed']} | "
        f"file={path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
