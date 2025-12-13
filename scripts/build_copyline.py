#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CopyLine post-process (v20)

Точечные правки (без XML-переформатирования):
1) Устойчивое чтение docs/copyline.yml:
   - windows-1251 -> utf-8 -> utf-8(errors="replace")
   - удаляем UTF-8 BOM, если есть
 2) Всегда сохраняем обратно в utf-8:
    - без xmlcharrefreplace; все символы пишем как есть (UTF-8)
3) Шапка файла:
   - убираем пустые строки/пробелы перед <?xml ...?>
   - убираем пустую строку между <?xml ...?> и <yml_catalog ...>
4) FEED_META:
   - фиксит повреждённую строку времени вида "P25-12-07 07:54:11"
     и превращает в:
     "Время сборки (Алматы)                      | 2025-12-07 07:54:11"
   - если строки времени вообще нет — вставляет после "URL поставщика"
   - приводит строку "Поставщик" к: "Поставщик                                  | CopyLine"
   - исправляет "Ближайшая сборка (Алматы)" так, чтобы это была будущая дата
     (считаем по env: SCHEDULE_DOM и SCHEDULE_HOUR_ALMATY, UTC+5)
5) WhatsApp rgba: rgba(0,0,0,.08) -> rgba(0,0,0,0.08)
6) <picture> заглушка, если picture отсутствует в offer (вставка сразу после </price>)
7) Если у offer нет <param>, добавляем минимум 1 параметр:
   <param name="Совместимость">...</param>
   + в <description> добавляем:
     <p><strong>Совместимость:</strong> ...</p>
     <h3>Характеристики</h3><ul><li><strong>Совместимость:</strong> ...
   (делаем только если в описании ещё нет блока "Характеристики")
8) Нормализация CDATA в <description> как у остальных:
   - ровно 2 перевода строки в начале и в конце CDATA (убирает хвосты 


)
9) Форс-diff для коммита (чтобы не было "No changes to commit") ТОЛЬКО когда нужно:
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


# Блоки шаблона CS для описания (WhatsApp + разделитель + Оплата/Доставка)
CS_WA_BLOCK = """<!-- WhatsApp -->
<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;"><p style="text-align:center; margin:0 0 12px;"><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);">&#128172; Написать в WhatsApp</a></p></div>"""
CS_HR_2PX = """<hr style="border:none; border-top:2px solid #E7D6B7; margin:12px 0;" />"""
CS_PAY_BLOCK = """<!-- Оплата и доставка -->
<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;"><div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;"><h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3><ul style="margin:0; padding-left:18px;"><li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li><li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li></ul><hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" /><h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3><ul style="margin:0; padding-left:18px;"><li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li><li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5 000 тг. | 3–7 рабочих дней</em></li><li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li><li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li></ul></div></div>"""


# Дефолты расписания для CopyLine (Алматы, UTC+5)
DEFAULT_SCHEDULE_DOM = "1,10,20"
DEFAULT_SCHEDULE_HOUR_ALMATY = 3


RE_OFFER_BLOCK = re.compile(r"(<offer\b[^>]*>)(.*?)(</offer>)", re.DOTALL)
RE_RGBA_BAD = re.compile(r"rgba\(0,0,0,\.08\)")
RE_PRICE_LINE = re.compile(r"(\n[ \t]*)<price>")

RE_NAME = re.compile(r"<name>(.*?)</name>", re.DOTALL)
RE_DESC_CDATA_ONLY = re.compile(r"<description><!\[CDATA\[(.*?)\]\]></description>", re.DOTALL)
RE_DESC_CDATA_WRAP = re.compile(r"(<description><!\[CDATA\[)(.*?)(\]\]></description>)", re.DOTALL)
RE_DESC_MARK = re.compile(r"<!--\s*Описание\s*-->", re.IGNORECASE)
RE_HAS_CHAR = re.compile(r"\bХарактеристики\b", re.IGNORECASE)
RE_COMP_PARA = re.compile(r"(?is)\n<p>\s*<strong>\s*Совместимость\s*:\s*</strong>.*?</p>\s*")
RE_COMP_LI = re.compile(r"(?is)<li>\s*<strong>\s*Совместимость\s*:\s*</strong>")
RE_CHAR_HDR = re.compile(r"(?is)<h3>\s*Характеристики\s*</h3>")


RE_INSERT_PARAM_BEFORE_KEYWORDS = re.compile(r"(</description>\n)([ \t]*)(<keywords>)", re.DOTALL)

# Толерантный FEED_META: допускаем \r\n и пробелы после -->
RE_FEED_META_BLOCK = re.compile(r"(<!--FEED_META\r?\n)(.*?)(\r?\n-->)([ \t]*)", re.DOTALL)

RE_NEXT_RUN_LINE = re.compile(
    r"(Ближайшая сборка\s*\(Алматы\)\s*\|\s*)(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
    re.IGNORECASE,
)

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
            want_h = int((os.environ.get("SCHEDULE_HOUR_ALMATY", str(DEFAULT_SCHEDULE_HOUR_ALMATY)) or "0").strip())
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


# Пишет файл в UTF-8
def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


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
        # Нормализуем encoding в заголовке до utf-8
        new_first = re.sub(r'encoding="[^"]+"', 'encoding="utf-8"', first)
        if new_first != first:
            first = new_first
            changed = 1
        first = first.rstrip("\r\n") + "\n"
        i = 1
        while i < len(lines) and lines[i].strip() == "":
            i += 1
            changed = 1
        return first + "".join(lines[i:]), changed

    return s, changed


# Форс-обновление времени (обновляем и FEED_META, и date="..." у yml_catalog)


# Парсит "P25-12-07 07:54:11" / "25-12-07 07:54:11" / "2025-12-07 07:54:11" -> "2025-12-07 07:54:11"
def _parse_weird_dt(line: str) -> str | None:
    s = (line or "").strip()

    m4 = re.search(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", s)
    if m4:
        return m4.group(1)

    m2 = re.search(r"(\d{2})-(\d{2})-(\d{2})\s+(\d{2}:\d{2}:\d{2})", s)
    if m2:
        yy, mm, dd, tt = m2.group(1), m2.group(2), m2.group(3), m2.group(4)
        try:
            y = int(yy)
            if y < 70:
                y += 2000
            else:
                y += 1900
            return f"{y:04d}-{mm}-{dd} {tt}"
        except Exception:
            return None

    return None


# Достаёт date="..." из <yml_catalog ...>
def _get_yml_date(src: str) -> str | None:
    m = RE_YML_CATALOG_DATE.search(src or "")
    if not m:
        return None
    return (m.group(2) or "").strip() or None


# Преобразует date="..." в datetime (Алматы)
def _get_yml_date_dt(src: str) -> datetime | None:
    s = _get_yml_date(src)
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


# Берёт дату/время сборки из FEED_META (если есть)
def _get_feed_meta_build_dt(src: str) -> datetime | None:
    m = RE_FEED_META_BLOCK.search(src or "")
    if not m:
        return None
    body = m.group(2)
    for ln in body.splitlines():
        mm = RE_BUILD_TIME_LINE.search(ln)
        if mm:
            try:
                return datetime.strptime(mm.group(2), "%Y-%m-%d %H:%M:%S")
            except Exception:
                return None
    return None


# Считает следующую сборку (строго в будущем) по env:
# - SCHEDULE_DOM="1,10,20" (дни месяца)
# - SCHEDULE_HOUR_ALMATY="3" (час)
def _compute_next_run_dt(base_dt: datetime) -> datetime:
    try:
        hour = int((os.environ.get("SCHEDULE_HOUR_ALMATY", str(DEFAULT_SCHEDULE_HOUR_ALMATY)) or "0").strip())
    except Exception:
        hour = 0
    hour = max(0, min(23, hour))

    dom_raw = (os.environ.get("SCHEDULE_DOM", DEFAULT_SCHEDULE_DOM) or "").strip()
    dom_list: list[int] = []
    if dom_raw:
        for part in dom_raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                d = int(part)
                if 1 <= d <= 31:
                    dom_list.append(d)
            except Exception:
                continue
        dom_list = sorted(set(dom_list))

    def mk(dt: datetime) -> datetime:
        return dt.replace(hour=hour, minute=0, second=0, microsecond=0)

    # Если расписание по дням месяца
    if dom_list:
        cur = base_dt
        # проверим текущий месяц и следующий, чтобы точно найти
        for month_shift in range(0, 14):
            # сдвиг месяца вручную (без dateutil)
            y = cur.year
            mo = cur.month + month_shift
            y += (mo - 1) // 12
            mo = ((mo - 1) % 12) + 1

            # список дней для месяца
            for d in dom_list:
                try:
                    cand = datetime(y, mo, d, hour, 0, 0)
                except Exception:
                    continue
                if cand > base_dt:
                    return cand
        # fallback
        return mk(base_dt + timedelta(days=1))

    # Ежедневно в hour
    today = mk(base_dt)
    if today > base_dt:
        return today
    return mk(base_dt + timedelta(days=1))


# Приводит строку "Поставщик" в FEED_META к нужному имени
def _ensure_feed_meta_supplier(src: str, supplier: str = "CopyLine") -> tuple[str, int]:
    m = RE_FEED_META_BLOCK.search(src or "")
    if not m:
        return src, 0

    head, body, tail, tail_ws = m.group(1), m.group(2), m.group(3), m.group(4)
    lines = body.splitlines()
    changed = 0

    for i, ln in enumerate(lines):
        if ln.strip().startswith("Поставщик"):
            want = f"Поставщик                                  | {supplier}"
            if ln != want:
                lines[i] = want
                changed = 1
            break

    if not changed:
        return src, 0

    new_block = head + "\n".join(lines) + tail + (tail_ws or "")
    out = src[: m.start()] + new_block + src[m.end() :]
    return out, 1


# Восстанавливает "Время сборки (Алматы) | ..." в FEED_META
def _ensure_feed_meta_build_time(src: str) -> tuple[str, int]:
    m = RE_FEED_META_BLOCK.search(src or "")
    if not m:
        return src, 0

    head, body, tail, tail_ws = m.group(1), m.group(2), m.group(3), m.group(4)
    lines = body.splitlines()
    changed = 0

    # уже есть нормальная строка
    for ln in lines:
        if RE_BUILD_TIME_LINE.search(ln):
            return src, 0

    # пытаемся починить строку, если она есть но повреждена
    for i, ln in enumerate(lines):
        if "Время сборки" in ln:
            dt = _parse_weird_dt(ln)
            if dt:
                lines[i] = f"Время сборки (Алматы)                      | {dt}"
                changed = 1
            break

    # ищем "P25-..." между URL и "Ближайшая сборка"
    if not changed:
        url_i = None
        next_i = None
        for i, ln in enumerate(lines):
            if ln.strip().startswith("URL поставщика"):
                url_i = i
            if "Ближайшая сборка" in ln:
                next_i = i
                break
        if url_i is not None:
            lo = url_i + 1
            hi = next_i if next_i is not None else len(lines)
            for j in range(lo, hi):
                dt = _parse_weird_dt(lines[j])
                if dt:
                    lines[j] = f"Время сборки (Алматы)                      | {dt}"
                    changed = 1
                    break

    # если не нашли — вставляем после URL поставщика
    if not changed:
        yml_dt = _get_yml_date(src)
        if yml_dt and re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}", yml_dt):
            dt = yml_dt + ":00"
        elif yml_dt and re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", yml_dt):
            dt = yml_dt
        else:
            dt = _now_almaty_str()

        insert_at = None
        for i, ln in enumerate(lines):
            if ln.strip().startswith("URL поставщика"):
                insert_at = i + 1
                break
        if insert_at is None:
            insert_at = 0
        lines.insert(insert_at, f"Время сборки (Алматы)                      | {dt}")
        changed = 1

    if not changed:
        return src, 0

    new_block = head + "\n".join(lines) + tail + (tail_ws or "")
    out = src[: m.start()] + new_block + src[m.end() :]
    return out, 1


# Обновляет/вставляет "Ближайшая сборка (Алматы) | ..." в FEED_META
def _ensure_feed_meta_next_run(src: str) -> tuple[str, int]:
    m = RE_FEED_META_BLOCK.search(src or "")
    if not m:
        return src, 0

    head, body, tail, tail_ws = m.group(1), m.group(2), m.group(3), m.group(4)
    lines = body.splitlines()

    base = _get_feed_meta_build_dt(src) or _get_yml_date_dt(src) or _now_almaty_dt()
    next_dt = _compute_next_run_dt(base)
    next_s = next_dt.strftime("%Y-%m-%d %H:%M:%S")
    want_line = f"Ближайшая сборка (Алматы)                  | {next_s}"

    found = False
    changed = 0

    for i, ln in enumerate(lines):
        if "Ближайшая сборка" in ln:
            found = True
            if ln != want_line:
                lines[i] = want_line
                changed = 1
            else:
                return src, 0
            break

    if not found:
        # вставим после "Время сборки"
        ins = None
        for i, ln in enumerate(lines):
            if "Время сборки" in ln:
                ins = i + 1
                break
        if ins is None:
            for i, ln in enumerate(lines):
                if ln.strip().startswith("URL поставщика"):
                    ins = i + 1
                    break
        if ins is None:
            ins = 0
        lines.insert(ins, want_line)
        changed = 1

    if not changed:
        return src, 0

    new_block = head + "\n".join(lines) + tail + (tail_ws or "")
    out = src[: m.start()] + new_block + src[m.end() :]
    return out, 1
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

# Убирает дубли "Совместимость" в CDATA: если есть в блоке "Характеристики",
# то удаляем отдельный <p><strong>Совместимость:</strong> ...</p> до блока.
def _dedupe_desc_compat(inner: str) -> tuple[str, int]:
    m = RE_CHAR_HDR.search(inner)
    if not m:
        return inner, 0

    prefix = inner[: m.start()]
    suffix = inner[m.start() :]

    if not RE_COMP_LI.search(suffix):
        return inner, 0

    prefix2, n = RE_COMP_PARA.subn("", prefix)
    if n == 0:
        return inner, 0

    return prefix2 + suffix, n


def _dedupe_offer_desc_compat(offer_body: str) -> tuple[str, int]:
    m = RE_DESC_CDATA_ONLY.search(offer_body)
    if not m:
        return offer_body, 0

    inner = m.group(1)
    inner2, n = _dedupe_desc_compat(inner)
    if n == 0:
        return offer_body, 0

    new_desc = f"<description><![CDATA[{inner2}]]></description>"
    out = offer_body[: m.start()] + new_desc + offer_body[m.end() :]
    return out, n


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
def _rebuild_description_inner(inner: str) -> str:
    """Перестраивает HTML описания в шаблон CS: WhatsApp + hr + Описание + Характеристики + Оплата/Доставка."""
    body = inner.strip()

    marker = "<!-- Описание -->"
    if marker in body:
        _, core = body.split(marker, 1)
    else:
        core = body

    core = core.strip()

    h3_char = "<h3>Характеристики</h3>"
    if h3_char in core:
        desc_part, char_part = core.split(h3_char, 1)
        char_part = h3_char + char_part
    else:
        desc_part, char_part = core, ""

    desc_part = re.sub(
        r"(?is)\s*<p>\s*<strong>\s*Совместимость\s*:\s*</strong>.*?</p>\s*",
        "",
        desc_part,
    ).strip()

    if not desc_part and not char_part:
        core_html = ""
    else:
        pieces: list[str] = []
        pieces.append(marker)
        if desc_part:
            pieces.append(desc_part.strip())
        if char_part:
            pieces.append(char_part.strip())
        core_html = "\n".join(pieces)

    parts: list[str] = []
    parts.append(CS_WA_BLOCK)
    parts.append(CS_HR_2PX)
    if core_html:
        parts.append(core_html)
    parts.append(CS_PAY_BLOCK)
    return "\n".join(parts)


def _rebuild_offer_description(offer_body: str) -> tuple[str, int]:
    m = RE_DESC_CDATA_WRAP.search(offer_body)
    if not m:
        return offer_body, 0

    prefix, inner, suffix = m.group(1), m.group(2), m.group(3)
    new_inner = _rebuild_description_inner(inner)
    if new_inner == inner:
        return offer_body, 0

    new_desc = prefix + "\n" + new_inner + "\n" + suffix
    out = offer_body[: m.start()] + new_desc + offer_body[m.end() :]
    return out, 1


# Применяет точечные правки без изменения общего форматирования
def _process_text(src: str) -> tuple[str, dict]:
    stats = {
        "offers_scanned": 0,
        "offers_pictures_added": 0,
        "offers_params_added": 0,
        "offers_desc_fixed": 0,
        "offers_desc_deduped": 0,
        "desc_cdata_fixed": 0,
        "rgba_fixed": 0,
        "header_fixed": 0,
        "feed_meta_supplier_fixed": 0,
        "feed_meta_time_fixed": 0,
        "feed_meta_next_run_fixed": 0,
        "build_time_bumped_meta": 0,
        "build_time_bumped_date": 0,
    }

    src_h, header_fixed = _normalize_header(src)
    stats["header_fixed"] = header_fixed

    src_h, n_sup = _ensure_feed_meta_supplier(src_h)
    stats["feed_meta_supplier_fixed"] = n_sup

    src_h, n_bt = _ensure_feed_meta_build_time(src_h)
    stats["feed_meta_time_fixed"] = n_bt

    src_h, n_nr0 = _ensure_feed_meta_next_run(src_h)
    stats["feed_meta_next_run_fixed"] = n_nr0

    src0, n_meta, n_date = _bump_build_time_if_needed(src_h)

    src0, n_nr1 = _ensure_feed_meta_next_run(src0)
    stats["feed_meta_next_run_fixed"] += n_nr1

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

        body4, rebuilt = _rebuild_offer_description(body3)
        stats["offers_desc_fixed"] += rebuilt

        body5, n_ded = _dedupe_offer_desc_compat(body4)
        if n_ded:
            stats["offers_desc_deduped"] += 1

        return head + body5 + tail

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
    if enc != "utf-8" or bom:
        info = []
        if enc != "utf-8":
            info.append(f"encoding={enc}")
        if bom:
            info.append("bom=stripped")
        print(f"[postprocess_copyline] WARN: input {'; '.join(info)} (сохраним как utf-8)", file=sys.stderr)

    out, stats = _process_text(src)

    if out != src or enc != "utf-8" or bom:
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
