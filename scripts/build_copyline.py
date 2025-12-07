#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CopyLine post-process (v19)

Точечные правки (без XML-переформатирования):
1) Устойчивое чтение docs/copyline.yml:
   - windows-1251 -> utf-8 -> utf-8(errors="replace")
   - удаляем UTF-8 BOM, если есть
2) Всегда сохраняем обратно в windows-1251:
   - неподдерживаемые символы -> XML-сущности (&#...;) через xmlcharrefreplace
3) Шапка файла:
   - убираем пустые строки/пробелы перед <?xml ...?>
   - убираем пустую строку между <?xml ...?> и <yml_catalog ...>
4) WhatsApp rgba: rgba(0,0,0,.08) -> rgba(0,0,0,0.08)
5) <picture> заглушка, если picture отсутствует в offer (вставка сразу после </price>)
6) Если у offer нет <param>, добавляем минимум 1 параметр:
   <param name="Совместимость">...</param>
   + в <description> добавляем:
     <p><strong>Совместимость:</strong> ...</p>
     <h3>Характеристики</h3><ul><li><strong>Совместимость:</strong> ...
   (делаем только если в описании ещё нет блока "Характеристики")
7) Нормализация CDATA в <description> как у остальных:
   - ровно 2 перевода строки в начале и в конце CDATA (убирает хвосты \n\n\n)
8) Форс-diff для коммита (чтобы не было "No changes to commit") ТОЛЬКО когда нужно:
   - workflow_dispatch: да
   - FORCE_YML_REFRESH=1: да
   - push: только если сейчас в Алматы hour == SCHEDULE_HOUR_ALMATY
   - schedule: нет

Входной файл: OUT_FILE или docs/copyline.yml
Если файла нет — code=2 (ничего не создаём).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import html as _html
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

RE_NAME = re.compile(r"<name>(.*?)</name>", re.DOTALL)
RE_DESC_CDATA_ONLY = re.compile(r"<description><!\[CDATA\[(.*?)\]\]></description>", re.DOTALL)
RE_DESC_CDATA_WRAP = re.compile(r"(<description><!\[CDATA\[)(.*?)(\]\]></description>)", re.DOTALL)
RE_DESC_MARK = re.compile(r"<!--\s*Описание\s*-->", re.IGNORECASE)
RE_HAS_CHAR = re.compile(r"\bХарактеристики\b", re.IGNORECASE)

RE_INSERT_PARAM_BEFORE_KEYWORDS = re.compile(r"(</description>\n)([ \t]*)(<keywords>)", re.DOTALL)

RE_BUILD_TIME_LINE = re.compile(
    r"(Время сборки\s*\(Алматы\)\s*\|\s*)(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
    re.IGNORECASE,
)
RE_YML_CATALOG_DATE = re.compile(r'(<yml_catalog\b[^>]*\bdate=")([^"]*)(")', re.IGNORECASE)

TYPE_PREFIX = re.compile(
    r"^(Картридж|Тонер|Драм|Фотобарабан|Девелопер|Чип|Лента|Ролик|Печатающая головка|Заправка)\b[:\- ]*",
    re.IGNORECASE,
)


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


# Форс-обновление времени (обновляем и FEED_META, и date="..." у yml_catalog)
def _bump_build_time_if_needed(src: str) -> tuple[str, int, int]:
    if not _should_force_refresh():
        return src, 0, 0

    now_s = _now_almaty_str()
    now_min = _now_almaty_str_min()

    out, n_meta = RE_BUILD_TIME_LINE.subn(rf"\1{now_s}", src, count=1)
    out2, n_date = RE_YML_CATALOG_DATE.subn(lambda m: m.group(1) + now_min + m.group(3), out, count=1)
    return out2, n_meta, n_date


# XML-экранирование текста для <param> значений
def _xml_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


# Чуть “человеческое” значение для совместимости (из name)
def _compat_from_name(name: str) -> str:
    s = " ".join(name.split()).strip()
    s = TYPE_PREFIX.sub("", s).strip()
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


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


# Добавляет блок характеристик и параграф "Совместимость" в CDATA, если характеристик нет
def _inject_desc_compat(inner: str, compat_html: str) -> tuple[str, int]:
    if RE_HAS_CHAR.search(inner):
        return inner, 0

    m = RE_DESC_MARK.search(inner)
    add = (
        f"\n<p><strong>Совместимость:</strong> {compat_html}</p>"
        f"\n<h3>Характеристики</h3><ul><li><strong>Совместимость:</strong> {compat_html}</li></ul>\n"
    )

    if not m:
        return inner + add, 1

    pos = m.end()
    return inner[:pos] + add + inner[pos:], 1


# Если <param> нет — добавляем совместимость и в description, и в param
def _ensure_min_param(offer_body: str) -> tuple[str, int, int]:
    if "<param" in offer_body:
        return offer_body, 0, 0

    nm = RE_NAME.search(offer_body)
    name = nm.group(1).strip() if nm else ""
    if not name:
        return offer_body, 0, 0

    compat = _compat_from_name(name)
    if not compat:
        return offer_body, 0, 0

    desc_added = 0
    descm = RE_DESC_CDATA_ONLY.search(offer_body)
    if descm:
        inner = descm.group(1)
        compat_html = _html.escape(compat, quote=False)
        inner2, desc_added = _inject_desc_compat(inner, compat_html)
        if desc_added:
            new_desc = f"<description><![CDATA[{inner2}]]></description>"
            offer_body = offer_body[: descm.start()] + new_desc + offer_body[descm.end() :]

    param_xml = _xml_escape(compat)
    insert_line = f'<param name="Совместимость">{param_xml}</param>\n'

    def _repl(m: re.Match) -> str:
        after_desc = m.group(1)
        indent = m.group(2)
        kw = m.group(3)
        return f"{after_desc}{indent}{insert_line}{indent}{kw}"

    new_body, n = RE_INSERT_PARAM_BEFORE_KEYWORDS.subn(_repl, offer_body, count=1)
    if n == 0:
        new_body = offer_body + "\n" + insert_line

    return new_body, 1, desc_added


# Нормализация CDATA: ровно 2 \n в начале и в конце (убирает хвост \n\n\n)
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

    out = RE_DESC_CDATA_WRAP.sub(repl, src)
    return out, fixed


# Применяет точечные правки без изменения общего форматирования
def _process_text(src: str) -> tuple[str, dict]:
    stats = {
        "offers_scanned": 0,
        "offers_pictures_added": 0,
        "offers_params_added": 0,
        "offers_desc_fixed": 0,
        "desc_cdata_fixed": 0,
        "rgba_fixed": 0,
        "header_fixed": 0,
        "build_time_bumped_meta": 0,
        "build_time_bumped_date": 0,
    }

    src_h, header_fixed = _normalize_header(src)
    stats["header_fixed"] = header_fixed

    src0, n_meta, n_date = _bump_build_time_if_needed(src_h)
    stats["build_time_bumped_meta"] = n_meta
    stats["build_time_bumped_date"] = n_date

    src1, n_rgba = RE_RGBA_BAD.subn("rgba(0,0,0,0.08)", src0)
    stats["rgba_fixed"] = n_rgba

    def repl(m: re.Match) -> str:
        head, body, tail = m.group(1), m.group(2), m.group(3)

        stats["offers_scanned"] += 1

        body2, pic_added = _inject_picture_if_missing(body)
        stats["offers_pictures_added"] += pic_added

        body3, param_added, desc_added = _ensure_min_param(body2)
        stats["offers_params_added"] += param_added
        stats["offers_desc_fixed"] += desc_added

        return head + body3 + tail

    out = RE_OFFER_BLOCK.sub(repl, src1)

    out2, n_cdata = _normalize_description_cdata_2nl(out)
    stats["desc_cdata_fixed"] = n_cdata

    return out2, stats


# Определяет входной файл по OUT_FILE или docs/copyline.yml
def _resolve_infile(cli_path: str | None) -> Path:
    if cli_path:
        return Path(cli_path)
    env_path = os.environ.get("OUT_FILE", "").strip()
    if env_path:
        return Path(env_path)
    return Path("docs/copyline.yml")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", default=None, help="Путь к copyline.yml (по умолчанию OUT_FILE или docs/copyline.yml)")
    args = ap.parse_args(argv)

    path = _resolve_infile(args.infile)
    if not path.exists():
        print(f"[postprocess_copyline] ERROR: file not found: {path}", file=sys.stderr)
        return 2

    src, enc, bom = _read_text(path)
    if enc != "windows-1251" or bom:
        info = []
        if enc != "windows-1251":
            info.append(f"encoding={enc}")
        if bom:
            info.append("bom=stripped")
        print(f"[postprocess_copyline] WARN: input {'; '.join(info)} (сохраним как windows-1251)", file=sys.stderr)

    out, stats = _process_text(src)

    if out != src or enc != "windows-1251" or bom:
        _write_text(path, out)

    bumped = stats["build_time_bumped_meta"] + stats["build_time_bumped_date"]
    print(
        "[postprocess_copyline] OK | "
        f"offers={stats['offers_scanned']} | "
        f"pictures_added={stats['offers_pictures_added']} | "
        f"params_added={stats['offers_params_added']} | "
        f"desc_fixed={stats['offers_desc_fixed']} | "
        f"desc_cdata_fixed={stats['desc_cdata_fixed']} | "
        f"rgba_fixed={stats['rgba_fixed']} | "
        f"header_fixed={stats['header_fixed']} | "
        f"build_time_bumped={bumped} (meta={stats['build_time_bumped_meta']}, date={stats['build_time_bumped_date']}) | "
        f"file={path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
