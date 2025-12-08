#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_akcent.py (v60 style-unified)

Финальный пост-процессор AkCent под эталон AlStyle:
1) FEED_META: yml_catalog date и "Ближайшая сборка" должны быть логичны.
2) <description>: одинаковая структура у всех:
   WhatsApp-блок (как AlStyle) -> HR 2px -> <!-- Описание --> -> родное описание -> Характеристики -> Оплата/Доставка (как AlStyle).
3) <keywords>: хвост городов строго как в AlStyle (список + порядок).
"""

import os
import re
from pathlib import Path
from datetime import datetime, timedelta


OUT_DEFAULT = "docs/akcent.yml"

# Хвост городов строго как в AlStyle
ALSTYLE_CITY_TAIL = (
    "Казахстан, Алматы, Астана, Шымкент, Караганда, Актобе, Павлодар, Атырау, Тараз, "
    "Костанай, Кызылорда, Петропавловск, Талдыкорган, Актау"
)

# Эталонный WhatsApp-блок (ВНИМАНИЕ: только triple-quoted, иначе будет SyntaxError)
AL_WA_BLOCK = """<!-- WhatsApp -->
<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;"><p style="text-align:center; margin:0 0 12px;"><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);">&#128172; Написать в WhatsApp</a></p></div>"""

# Эталонный HR (как AlStyle)
AL_HR_2PX = '<hr style="border:none; border-top:2px solid #E7D6B7; margin:12px 0;" />'

# Эталонный блок "Оплата и доставка" (как AlStyle)
AL_PAY_BLOCK = """<!-- Оплата и доставка -->
<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;"><div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;"><h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3><ul style="margin:0; padding-left:18px;"><li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li><li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li></ul><hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" /><h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3><ul style="margin:0; padding-left:18px;"><li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li><li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5 000 тг. | 3–7 рабочих дней</em></li><li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li><li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li></ul></div></div>"""


RE_OFFER = re.compile(r"(?s)(<offer\b[^>]*>)(.*?)(</offer>)")
RE_NAME = re.compile(r"(?s)<name>(.*?)</name>")
RE_DESC = re.compile(r"(?s)<description><!\[CDATA\[(.*?)\]\]></description>")
RE_PARAM = re.compile(r'(?s)<param\s+name="([^"]+)">(.*?)</param>')
RE_KEYWORDS = re.compile(r"(?s)<keywords>(.*?)</keywords>")


def _read_text(path: str) -> str:
    """Читает файл устойчиво."""
    data = Path(path).read_bytes()
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    for enc in ("windows-1251", "utf-8"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            pass
    return data.decode("utf-8", errors="replace")


def _write_text(path: str, text: str) -> None:
    """Пишет файл в windows-1251."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_bytes(text.encode("windows-1251", errors="xmlcharrefreplace"))


def _parse_build_time(src: str):
    """Достаёт время сборки из FEED_META."""
    m = re.search(
        r"Время сборки \(Алматы\)\s*\|\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})",
        src,
    )
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _next_daily(dt: datetime, hour: int) -> datetime:
    """Следующий запуск ежедневно в hour:00."""
    cand = dt.replace(hour=hour, minute=0, second=0, microsecond=0)
    if cand <= dt:
        cand += timedelta(days=1)
    return cand


def _fix_text_common(html: str) -> str:
    """Правит типовые опечатки/типографику."""
    s = html
    s = re.sub(r"\bинсталяционн", "инсталляционн", s, flags=re.I)
    s = re.sub(r",(?=[А-Яа-яЁё])", ", ", s)  # запятая+пробел перед кириллицей
    s = re.sub(r"(\d+,\d+)\s{2,}(\d+,\d+)\s*м\b", r"\1 x \2 м", s)
    s = re.sub(r"(\d+,\d+)\s*м\b", r"\1 м", s)
    return s


def _extract_native_desc(cdata: str, name: str) -> str:
    """Берёт родное описание из CDATA (после <!-- Описание -->), без блоков характеристик/оплаты."""
    s = cdata.replace("\r\n", "\n").strip()

    m = re.search(r"(?s)<!--\s*Описание\s*-->\s*(.*)$", s, flags=re.I)
    if m:
        s = m.group(1).strip()

    s = re.split(
        r"(?is)<h3>\s*Характеристики\s*</h3>|<!--\s*Оплата\s+и\s+доставка\s*-->",
        s,
        maxsplit=1,
    )[0].strip()

    s = re.sub(r"(?is)^\s*<h3>.*?</h3>\s*", "", s, count=1).strip()

    if not s:
        return f"<p>{name}</p>"

    if "<p" not in s.lower():
        s = f"<p>{s}</p>"

    return s


def _build_chars_from_params(offer_body: str) -> str:
    """Строит блок характеристик из <param> с единым порядком."""
    params = []
    seen = set()

    for k, v in RE_PARAM.findall(offer_body):
        k2 = k.strip()
        if not k2:
            continue
        lk = k2.lower()
        if lk in seen:
            continue
        seen.add(lk)
        params.append((k2, v.strip()))

    if not params:
        return "<h3>Характеристики</h3><ul><li><strong>Гарантия:</strong> 0</li></ul>"

    prio = [
        "Производитель",
        "Бренд",
        "Вендор",
        "Vendor",
        "Brand",
        "Модель",
        "Артикул",
        "Код производителя",
        "Совместимость",
        "Тип",
        "Тип печати",
        "Цвет",
        "Ресурс",
        "Объем",
        "Ёмкость",
        "Гарантия",
        "Страна происхождения",
    ]
    prio_index = {k.lower(): i for i, k in enumerate(prio)}

    def sk(item):
        k = item[0]
        return (prio_index.get(k.lower(), 10_000), k.lower())

    params.sort(key=sk)

    items = []
    for k, v in params:
        if not v:
            continue
        items.append(f"<li><strong>{k}:</strong> {v}</li>")

    if not items:
        items = ["<li><strong>Гарантия:</strong> 0</li>"]

    return "<h3>Характеристики</h3><ul>" + "".join(items) + "</ul>"


def _normalize_keywords(body: str) -> str:
    """Хвост городов в <keywords> как в AlStyle."""
    drop = {x.strip() for x in ALSTYLE_CITY_TAIL.split(",")}

    def repl(m: re.Match) -> str:
        raw = m.group(1)
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        kept = [p for p in parts if p not in drop]
        base = ", ".join(kept).strip(" ,")
        if base:
            return f"<keywords>{base}, {ALSTYLE_CITY_TAIL}</keywords>"
        return f"<keywords>{ALSTYLE_CITY_TAIL}</keywords>"

    return RE_KEYWORDS.sub(repl, body)


def _normalize_desc_tag(offer_body: str) -> str:
    """Собирает <description> в эталонной структуре AlStyle."""
    nm = RE_NAME.search(offer_body)
    name = nm.group(1).strip() if nm else ""

    dm = RE_DESC.search(offer_body)
    cdata = dm.group(1) if dm else ""

    native = _fix_text_common(_extract_native_desc(cdata, name))
    chars = _fix_text_common(_build_chars_from_params(offer_body))

    new_cdata = (
        "\n"
        + AL_WA_BLOCK.strip()
        + "\n"
        + AL_HR_2PX
        + "\n<!-- Описание -->\n"
        + f"<h3>{name}</h3>"
        + native
        + "\n"
        + chars
        + "\n"
        + AL_PAY_BLOCK.strip()
        + "\n"
    )

    return RE_DESC.sub(
        lambda _m: f"<description><![CDATA[{new_cdata}]]></description>",
        offer_body,
        count=1,
    )


def fix_akcent(src: str) -> str:
    """Главная доводка: даты + описания + keywords + форматирование."""
    s = src.lstrip("\ufeff").lstrip()

    # Убираем пустую строку между XML-декларацией и yml_catalog
    s = re.sub(
        r"(?s)\A\s*(<\?xml[^>]*\?>)\s*\n\s*(<yml_catalog\b)",
        r"\1\n\2",
        s,
        count=1,
    )

    bt = _parse_build_time(s)
    if bt:
        # yml_catalog date = время сборки (минуты)
        s = re.sub(
            r'(<yml_catalog\s+date=")[^"]+(")',
            r"\1" + bt.strftime("%Y-%m-%d %H:%M") + r"\2",
            s,
            count=1,
        )
        # Ближайшая сборка = следующий запуск (AkCent ежедневно 02:00)
        nxt = _next_daily(bt, 2)
        s = re.sub(
            r"(Ближайшая сборка \(Алматы\)\s*\|\s*)\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
            r"\1" + nxt.strftime("%Y-%m-%d %H:%M:%S"),
            s,
            count=1,
        )

    def offer_repl(m: re.Match) -> str:
        head, body, tail = m.group(1), m.group(2), m.group(3)
        body = _normalize_desc_tag(body)
        body = _normalize_keywords(body)
        return head + body + tail

    s = RE_OFFER.sub(offer_repl, s)

    # Форматирование как AlStyle: после <shop><offers> — двойная пустая строка,
    # и перед </offers> — двойная пустая строка
    s = re.sub(r"(<shop><offers>\n)(?!\n)", r"\1\n", s, count=1)
    s = re.sub(r"(</offer>\n)(</offers>)", r"\1\n\2", s, count=1)

    return s


def main() -> int:
    """Точка входа."""
    outfile = os.environ.get("OUT_FILE", "").strip() or OUT_DEFAULT
    src = _read_text(outfile)
    out = fix_akcent(src)
    _write_text(outfile, out)
    print(f"[akcent] patched: {outfile}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
