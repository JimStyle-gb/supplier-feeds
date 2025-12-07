#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NVPrint post-process (v58)

Точечные правки (без XML-переформатирования):
1) Устойчивое чтение docs/nvprint.yml:
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

Входной файл: OUT_FILE или docs/nvprint.yml
Если файла нет — code=2 (ничего не создаём).
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

RE_FEED_META_BLOCK = re.compile(r"<!--FEED_META.*?-->", re.DOTALL)
RE_BROKEN_TIME_P = re.compile(r"^\s*P(\d{2})-(\d{2})-(\d{2})\s+(\d{2}:\d{2}:\d{2})\s*$")
RE_NEXT_RUN_LINE = re.compile(
    r"(Ближайшая сборка\s*\(Алматы\)\s*\|\s*)(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
    re.IGNORECASE,
)


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


# Чинит FEED_META:
# - строку времени вида "P25-12-07 07:53:29" -> "Время сборки (Алматы) | 2025-12-07 07:53:29"
# - "Ближайшая сборка (Алматы)" если она оказалась в прошлом (по SCHEDULE_DOM и SCHEDULE_HOUR_ALMATY)
def _fix_feed_meta(src: str) -> tuple[str, int]:
    m = RE_FEED_META_BLOCK.search(src)
    if not m:
        return src, 0

    block = m.group(0)
    lines = block.splitlines()

    # Позиция колонки "|" (чтобы выровнять как в других строках FEED_META)
    pipe_pos = None
    for ln in lines:
        if "|" in ln:
            pipe_pos = ln.index("|")
            break
    if pipe_pos is None:
        pipe_pos = 42

    # Достаём время сборки (после фикса) — нужно для проверки "Ближайшая сборка"
    build_dt: datetime | None = None

    changed = 0
    out_lines: list[str] = []
    for ln in lines:
        mm = RE_BROKEN_TIME_P.match(ln)
        if mm:
            yy, mo, dd, t = mm.group(1), mm.group(2), mm.group(3), mm.group(4)
            ts = f"20{yy}-{mo}-{dd} {t}"
            left = "Время сборки (Алматы)"
            out_lines.append(left.ljust(pipe_pos) + "| " + ts)
            changed = 1
            try:
                build_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            except Exception:
                build_dt = None
            continue

        out_lines.append(ln)

        if build_dt is None and "Время сборки" in ln and "|" in ln:
            try:
                ts = ln.split("|", 1)[1].strip()
                build_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            except Exception:
                build_dt = None

    # Если время сборки не нашли, пробуем взять из <yml_catalog date="...">
    if build_dt is None:
        ym = RE_YML_CATALOG_DATE.search(src)
        if ym:
            raw = (ym.group(2) or "").strip()
            try:
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}", raw):
                    build_dt = datetime.strptime(raw + ":00", "%Y-%m-%d %H:%M:%S")
                else:
                    build_dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except Exception:
                build_dt = None

    # Фикс "Ближайшая сборка" только если она оказалась <= времени сборки (то есть в прошлом)
    if build_dt is not None:
        for i, ln in enumerate(out_lines):
            mm = RE_NEXT_RUN_LINE.search(ln)
            if not mm:
                continue
            try:
                nxt = datetime.strptime(mm.group(2), "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            if nxt <= build_dt:
                new_dt = _calc_next_run(build_dt)
                if new_dt is None:
                    continue
                left = "Ближайшая сборка (Алматы)"
                out_lines[i] = left.ljust(pipe_pos) + "| " + new_dt.strftime("%Y-%m-%d %H:%M:%S")
                changed = 1
            break

    if not changed:
        return src, 0

    new_block = "\n".join(out_lines)
    out = src[: m.start()] + new_block + src[m.end() :]
    return out, 1


# Считает ближайшую сборку в Алматы по переменным окружения:
# - SCHEDULE_HOUR_ALMATY (час)
# - SCHEDULE_DOM (например "1,10,20"; если пусто — ежедневно)
def _calc_next_run(build_dt: datetime) -> datetime | None:
    try:
        hour = int((os.environ.get("SCHEDULE_HOUR_ALMATY", "") or "").strip())
    except Exception:
        return None

    dom_raw = (os.environ.get("SCHEDULE_DOM", "") or "").strip()
    doms: list[int] = []
    if dom_raw:
        for x in dom_raw.split(","):
            x = x.strip()
            if not x:
                continue
            try:
                v = int(x)
                if 1 <= v <= 31:
                    doms.append(v)
            except Exception:
                continue
        doms = sorted(set(doms))

    # Ежедневно
    if not doms:
        cand = build_dt.replace(hour=hour, minute=0, second=0, microsecond=0)
        if cand <= build_dt:
            cand = cand + timedelta(days=1)
        return cand

    # По дням месяца (1/10/20 и т.п.)
    y = build_dt.year
    m = build_dt.month

    best: datetime | None = None
    for step in range(0, 3):  # хватит пары месяцев вперёд
        mm = m + step
        yy = y + (mm - 1) // 12
        mm = ((mm - 1) % 12) + 1
        for d in doms:
            try:
                cand = datetime(yy, mm, d, hour, 0, 0)
            except Exception:
                continue
            if cand <= build_dt:
                continue
            if best is None or cand < best:
                best = cand
    return best

    return s, changed


# Форс-обновление времени (обновляем и FEED_META, и date="..." у yml_catalog)
def _bump_build_time_if_needed(src: str) -> tuple[str, int, int]:
    if not _should_force_refresh():
        return src, 0, 0

    now_s = _now_almaty_str()

    out, n_meta = RE_BUILD_TIME_LINE.subn(rf"\1{now_s}", src, count=1)

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
        "feed_meta_fixed": 0,
        "build_time_bumped_meta": 0,
        "build_time_bumped_date": 0,
    }

    src_h, header_fixed = _normalize_header(src)
    stats["header_fixed"] = header_fixed

    src_m, meta_fixed = _fix_feed_meta(src_h)
    stats["feed_meta_fixed"] = meta_fixed

    src0, n_meta, n_date = _bump_build_time_if_needed(src_m)
    stats["build_time_bumped_meta"] = n_meta
    stats["build_time_bumped_date"] = n_date

    src1, n_rgba = RE_RGBA_BAD.subn("rgba(0,0,0,0.08)", src0)
    stats["rgba_fixed"] = n_rgba

    def repl(m: re.Match) -> str:
        head, body, tail = m.group(1), m.group(2), m.group(3)
        stats["offers_scanned"] += 1
        body2, added = _inject_picture_if_missing(body)
        stats["offers_pictures_added"] += added
        return head + body2 + tail

    out = RE_OFFER_BLOCK.sub(repl, src1)
    return out, stats


# Определяет входной файл по OUT_FILE или docs/nvprint.yml
def _resolve_infile(cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path)
    env_path = os.environ.get("OUT_FILE", "").strip()
    if env_path:
        return Path(env_path)
    return Path("docs/nvprint.yml")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", default=None, help="Путь к nvprint.yml (по умолчанию OUT_FILE или docs/nvprint.yml)")
    args = ap.parse_args(argv)

    path = _resolve_infile(args.infile)
    if not path.exists():
        print(f"[postprocess_nvprint] ERROR: file not found: {path}", file=sys.stderr)
        return 2

    src, enc, bom = _read_text(path)
    if enc != "windows-1251" or bom:
        info = []
        if enc != "windows-1251":
            info.append(f"encoding={enc}")
        if bom:
            info.append("bom=stripped")
        print(f"[postprocess_nvprint] WARN: input {'; '.join(info)} (сохраним как windows-1251)", file=sys.stderr)

    out, stats = _process_text(src)

    if out != src or enc != "windows-1251" or bom:
        _write_text(path, out)

    bumped = stats["build_time_bumped_meta"] + stats["build_time_bumped_date"]
    print(
        "[postprocess_nvprint] OK | "
        f"offers={stats['offers_scanned']} | "
        f"pictures_added={stats['offers_pictures_added']} | "
        f"rgba_fixed={stats['rgba_fixed']} | "
        f"header_fixed={stats['header_fixed']} | "
        f"feed_meta_fixed={stats['feed_meta_fixed']} | "
        f"build_time_bumped={bumped} (meta={stats['build_time_bumped_meta']}, date={stats['build_time_bumped_date']}) | "
        f"file={path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
