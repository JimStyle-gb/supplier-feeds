#!/usr/bin/env python3
# AlStyle post-process (v17)
# - CDATA формат под эталон (строго по строкам)
# - Линия-разделитель чуть толще (border-top:2px)
# - WhatsApp: текст кнопки "Написать в WhatsApp"
# - SEO: кнопка сверху -> родное описание/характеристики -> блок оплаты/доставки
# - Доставка: 5000 тг. -> 5 000 тг.
# - Keywords: убрать города (Темиртау, Экибастуз, Орал, Оскемен, Кокшетау, Семей)
# - Безопасная запись в windows-1251 (чтобы workflow/commit не падал)

import os
import re
import sys
from pathlib import Path


_CITIES_DROP = {"Темиртау", "Экибастуз", "Орал", "Оскемен", "Кокшетау", "Семей"}

# Линия-разделитель между кнопкой и описанием (чуть толще)
_SEP_LINE = '<hr style="border:none; border-top:2px solid #E7D6B7; margin:12px 0;" />'


def _read_text_safely(path: str) -> str:
    'Читаем файл максимально устойчиво (cp1251 -> utf-8 -> utf-8 replace), убираем BOM.'
    data = Path(path).read_bytes()

    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]

    for enc in ("windows-1251", "utf-8"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            pass

    return data.decode("utf-8", errors="replace")


def _write_cp1251_safe(path: str, text: str) -> None:
    'Пишем строго в windows-1251, но не падаем на символах вне кодировки.'
    if not text.endswith("\n"):
        text += "\n"
    Path(path).write_bytes(text.encode("windows-1251", errors="xmlcharrefreplace"))


def _ensure_cdata_edges(cdata: str) -> str:
    'Ровно 1 перенос сразу после <![CDATA[ и 1 перенос перед ]]>.'
    cdata = cdata.replace("\r\n", "\n")
    cdata = cdata.lstrip("\n")
    cdata = "\n" + cdata
    if not cdata.endswith("\n"):
        cdata += "\n"
    return cdata


def _update_whatsapp_button_text(html: str) -> str:
    'Меняем текст кнопки, не трогая стили/ссылку.'
    html = html.replace("НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!", "Написать в WhatsApp")
    html = html.replace("НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ В WHATSAPP!", "Написать в WhatsApp")
    html = re.sub(
        r"(?s)(&#128172;\s*)([^<]{1,160}?)(WHATSAPP!?)(\s*)</a>",
        r"\1Написать в WhatsApp</a>",
        html,
        count=1,
    )
    return html


def _format_delivery_numbers(html: str) -> str:
    '5000 тг. -> 5 000 тг. (только в контексте доставки до 5 кг).'
    return re.sub(
        r"(до\s*5\s*кг\s*[—-]\s*)5000(\s*тг\.?)(?!\d)",
        r"\g<1>5 000\g<2>",
        html,
    )


def _normalize_block_spacing(text: str) -> str:
    'Убираем лишние пустые строки между блоками (оставляем 1 перенос).'
    text = text.replace("\r\n", "\n")
    text = re.sub(r"(</div>)\n\n+(%s)" % re.escape(_SEP_LINE), r"\1\n\2", text)
    text = re.sub(r"(%s)\n\n+(<!--\s*Описание\s*-->)" % re.escape(_SEP_LINE), r"\1\n\2", text)
    text = re.sub(r"(</ul>)\n\n+(<!--\s*Оплата\s+и\s+доставка\s*-->)", r"\1\n\2", text)
    return text


def _reorder_desc_blocks(cdata: str) -> str:
    'Кнопка сверху -> описание/характеристики -> оплата/доставка + вставить HR.'
    if "<!-- WhatsApp -->" not in cdata or "<!-- Описание -->" not in cdata:
        return cdata

    m = re.search(
        r"(?s)<!--\s*WhatsApp\s*-->\s*(?P<wa>.*?)\s*<!--\s*Описание\s*-->\s*(?P<body>.*)$",
        cdata,
    )
    if not m:
        return cdata

    wa = m.group("wa").strip()
    body = m.group("body").strip()

    m2 = re.match(r"(?s)^(?P<open><div\b[^>]*>)(?P<inner>.*)(?P<close></div>)\s*$", wa)
    if not m2:
        return cdata

    open_div = m2.group("open")
    inner = m2.group("inner")
    close_div = m2.group("close")

    p_end = inner.find("</p>")
    if p_end == -1:
        return cdata
    p_end += len("</p>")

    btn_inner = inner[:p_end].strip()
    rest_inner = inner[p_end:].strip()

    btn_block = f"{open_div}{btn_inner}{close_div}"
    pay_block = f"{open_div}{rest_inner}{close_div}" if rest_inner else ""

    parts = []
    parts.append("<!-- WhatsApp -->")
    parts.append(btn_block)
    parts.append(_SEP_LINE)
    parts.append("<!-- Описание -->")
    parts.append(body)
    if pay_block:
        parts.append("<!-- Оплата и доставка -->")
        parts.append(pay_block)

    return _normalize_block_spacing("\n".join(parts))


def _process_description_blocks(text: str) -> str:
    'Правки внутри <description><![CDATA[...]]></description>.'
    rx = re.compile(r"(?s)(<description><!\[CDATA\[)(.*?)(\]\]></description>)")

    def repl(m: re.Match) -> str:
        head, cdata, tail = m.group(1), m.group(2), m.group(3)

        cdata = cdata.replace("\r\n", "\n")
        cdata = _update_whatsapp_button_text(cdata)
        cdata = _format_delivery_numbers(cdata)
        cdata = _reorder_desc_blocks(cdata)
        cdata = _ensure_cdata_edges(cdata)

        return f"{head}{cdata}{tail}"

    return rx.sub(repl, text)


def _trim_keywords(text: str) -> str:
    'Убираем города из <keywords> у всех офферов.'
    rx = re.compile(r"(?s)<keywords>(.*?)</keywords>")

    def repl(m: re.Match) -> str:
        raw = m.group(1)
        if not any(city in raw for city in _CITIES_DROP) and "Кокшетау Семей" not in raw:
            return m.group(0)

        parts = [p.strip() for p in raw.split(",")]
        filtered = []
        for p in parts:
            if not p:
                continue
            if p in _CITIES_DROP:
                continue
            if p == "Кокшетау Семей":
                continue
            filtered.append(p)

        return f"<keywords>{', '.join(filtered)}</keywords>"

    return rx.sub(repl, text)


def main() -> int:
    out_file = os.getenv("OUT_FILE", "docs/alstyle.yml").strip() or "docs/alstyle.yml"

    if not Path(out_file).exists():
        print(f"[alstyle post] OUT_FILE not found: {out_file}", file=sys.stderr)
        return 2

    text = _read_text_safely(out_file)

    text = _process_description_blocks(text)
    text = _trim_keywords(text)

    _write_cp1251_safe(out_file, text)

    print(f"[alstyle post] ok: cdata/seo/keywords + windows-1251 safe -> {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
