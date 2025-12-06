#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CopyLine post-process (v17)
1) Унификация WhatsApp-блока (rgba(0,0,0,.08) -> rgba(0,0,0,0.08))
2) Добавление picture-заглушки, если <picture> отсутствует в offer
3) Если у offer нет <param>, добавляем минимум 1 параметр:
   <param name="Совместимость">...</param>
   + в <description> добавляем блок:
     <p><strong>Совместимость:</strong> ...</p>
     <h3>Характеристики</h3><ul><li><strong>Совместимость:</strong> ...

Важно: скрипт делает ТОЛЬКО точечные правки по тексту файла (без XML-переформатирования).

Fix v17: убраны ошибочные экранирования (SyntaxError на вставке param + корректные regex/backref).
"""

from __future__ import annotations

import argparse
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
RE_DESC_CDATA = re.compile(
    r"<description><!\[CDATA\[(.*?)\]\]></description>",
    re.DOTALL | re.IGNORECASE,
)

RE_WHATSAPP_BLOCK = re.compile(
    r"<!--\s*WhatsApp\s*-->.*?<!--\s*Описание\s*-->",
    re.DOTALL | re.IGNORECASE,
)
RE_DESC_MARK = re.compile(r"<!--\s*Описание\s*-->", re.IGNORECASE)
RE_HAS_CHAR = re.compile(r"\bХарактеристики\b", re.IGNORECASE)

# group1: конец description+перенос, group2: отступ перед <keywords>, group3: <keywords...
RE_INSERT_PARAM_BEFORE_KEYWORDS = re.compile(
    r"(\]\]></description>\s*\n)([ \t]*)(<keywords\b)",
    re.IGNORECASE,
)

TYPE_PREFIX = re.compile(
    r"^(девелопер|драм[- ]картридж|термоблок|термоэлемент|drum\s+unit|drum)\s+",
    re.IGNORECASE,
)


# Читает файл в windows-1251
def _read_text(path: Path) -> str:
    return path.read_text(encoding="windows-1251")


# Пишет файл в windows-1251
def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="windows-1251")


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

    # если явно есть "для/for" — берём хвост после этого
    m = re.search(r"\bдля\s+(.+)$", s, flags=re.IGNORECASE)
    if m:
        s = m.group(1).strip()
    else:
        m = re.search(r"\bfor\s+(.+)$", s, flags=re.IGNORECASE)
        if m:
            s = m.group(1).strip()

    # убрать цвета (как правило это не совместимость)
    s = re.sub(r"\s+(black|cyan|magenta|yellow)\b.*$", "", s, flags=re.IGNORECASE)

    # убрать вес/фасовку
    s = re.sub(r"\s+foil\s+bags\b.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+\d+\s*(?:г|гр|g|кг|kg)\b.*$", "", s, flags=re.IGNORECASE)

    s = " ".join(s.split()).strip()
    if len(s) > 180:
        s = s[:180].rstrip()
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

    # найдём маркер Описание
    m = RE_DESC_MARK.search(inner)
    if not m:
        # если вдруг маркера нет — аккуратно добавим в конец перед закрытием CDATA
        add = (
            f"\n<p><strong>Совместимость:</strong> {compat_html}</p>"
            f"\n<h3>Характеристики</h3><ul><li><strong>Совместимость:</strong> {compat_html}</li></ul>\n"
        )
        return inner + add, 1

    start = m.end()
    tail = inner[start:]

    # вставим после последнего </p> в описании
    lp = tail.lower().rfind("</p>")
    if lp != -1:
        insert_pos = start + lp + len("</p>")
    else:
        insert_pos = start

    add = (
        f"\n<p><strong>Совместимость:</strong> {compat_html}</p>"
        f"\n<h3>Характеристики</h3><ul><li><strong>Совместимость:</strong> {compat_html}</li></ul>\n"
    )
    return inner[:insert_pos] + add + inner[insert_pos:], 1


# Если нет param — добавляет param "Совместимость" + чинит description
def _inject_param_and_desc_if_missing(offer_body: str) -> tuple[str, int, int]:
    if "<param " in offer_body:
        return offer_body, 0, 0

    m_name = RE_NAME.search(offer_body)
    name = m_name.group(1).strip() if m_name else ""

    compat = _compat_from_name(name)
    if not compat:
        return offer_body, 0, 0

    # 1) description (CDATA) — добавим блок, если нет характеристик
    descm = RE_DESC_CDATA.search(offer_body)
    desc_added = 0
    if descm:
        inner = descm.group(1)

        # совместимость для HTML внутри CDATA — экранируем <>&
        compat_html = _html.escape(compat, quote=False)

        inner2, desc_added = _inject_desc_compat(inner, compat_html)
        if desc_added:
            new_desc = f"<description><![CDATA[{inner2}]]></description>"
            offer_body = offer_body[: descm.start()] + new_desc + offer_body[descm.end() :]

    # 2) param — вставим сразу после </description> перед <keywords>
    param_xml = _xml_escape(compat)
    insert_line = f'<param name="Совместимость">{param_xml}</param>\n'

    def _repl(m: re.Match) -> str:
        after_desc = m.group(1)
        indent = m.group(2)
        kw = m.group(3)
        return f"{after_desc}{indent}{insert_line}{indent}{kw}"

    new_body, n = RE_INSERT_PARAM_BEFORE_KEYWORDS.subn(_repl, offer_body, count=1)
    if n == 0:
        # если вдруг keywords нет — просто добавим в конец offer-body
        new_body = offer_body + "\n" + insert_line

    return new_body, 1, desc_added


# Применяет точечные правки без изменения общего форматирования
def _process_text(src: str) -> tuple[str, dict]:
    stats = {
        "offers_scanned": 0,
        "offers_pictures_added": 0,
        "offers_params_added": 0,
        "offers_desc_fixed": 0,
        "rgba_fixed": 0,
    }

    # 1) WhatsApp rgba
    src2, n = RE_RGBA_BAD.subn("rgba(0,0,0,0.08)", src)
    stats["rgba_fixed"] = n

    # 2-3) по offer-блокам
    def repl(m: re.Match) -> str:
        head, body, tail = m.group(1), m.group(2), m.group(3)
        stats["offers_scanned"] += 1

        body2, added_pic = _inject_picture_if_missing(body)
        stats["offers_pictures_added"] += added_pic

        body3, added_param, desc_fixed = _inject_param_and_desc_if_missing(body2)
        stats["offers_params_added"] += added_param
        stats["offers_desc_fixed"] += desc_fixed

        return head + body3 + tail

    out = RE_OFFER_BLOCK.sub(repl, src2)
    return out, stats


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
    ap.add_argument(
        "--in",
        dest="infile",
        default=None,
        help="Путь к copyline.yml (по умолчанию OUT_FILE или docs/copyline.yml)",
    )
    args = ap.parse_args(argv)

    path = _resolve_infile(args.infile)
    if not path.exists():
        print(f"[postprocess_copyline] ERROR: file not found: {path}", file=sys.stderr)
        return 2

    src = _read_text(path)
    out, stats = _process_text(src)

    if out != src:
        _write_text(path, out)

    print(
        "[postprocess_copyline] OK | "
        f"offers={stats['offers_scanned']} | "
        f"pictures_added={stats['offers_pictures_added']} | "
        f"params_added={stats['offers_params_added']} | "
        f"desc_fixed={stats['offers_desc_fixed']} | "
        f"rgba_fixed={stats['rgba_fixed']} | "
        f"file={path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
