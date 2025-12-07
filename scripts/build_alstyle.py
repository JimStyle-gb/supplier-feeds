#!/usr/bin/env python3
# AlStyle post-process (v14)
# - Исправление формата CDATA под эталон:
#   <description><![CDATA[
#   <!-- WhatsApp -->
#   ...
#   <!-- Описание -->
#   ...
#   <!-- Оплата и доставка -->
#   ...
#   ]]></description>
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


def _read_text_safely(path: str) -> str:
    "Читаем файл максимально устойчиво (cp1251 -> utf-8 -> utf-8 replace), убираем BOM."
    data = Path(path).read_bytes()

    # BOM (на всякий случай)
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]

    for enc in ("windows-1251", "utf-8"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            pass

    return data.decode("utf-8", errors="replace")


def _write_cp1251_safe(path: str, text: str) -> None:
    "Пишем строго в windows-1251, но не падаем на символах вне кодировки."
    if not text.endswith("\n"):
        text += "\n"

    data = text.encode("windows-1251", errors="xmlcharrefreplace")
    Path(path).write_bytes(data)


def _normalize_cdata_prefix(cdata: str) -> str:
    "Всегда делаем ровно 1 перенос строки сразу после <![CDATA[."
    cdata = cdata.replace("\r\n", "\n")
    cdata = cdata.lstrip("\n")
    return "\n" + cdata


def _update_whatsapp_button_text(html: str) -> str:
    "Меняем текст кнопки, не трогая стили/ссылку."
    html = html.replace("НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!", "Написать в WhatsApp")
    html = html.replace("НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ В WHATSAPP!", "Написать в WhatsApp")
    # Подстраховка: если внутри <a> есть &#128172; + что-то про whatsapp
    html = re.sub(
        r"(?s)(&#128172;\s*)([^<]{1,140}?)(WHATSAPP!?)(\s*)</a>",
        r"\1Написать в WhatsApp</a>",
        html,
        count=1,
    )
    return html


def _format_delivery_numbers(html: str) -> str:
    "5000 тг. -> 5 000 тг. (только в контексте доставки до 5 кг)."
    return re.sub(
        r"(до\s*5\s*кг\s*[—-]\s*)5000(\s*тг\.?)(?!\d)",
        r"\g<1>5 000\g<2>",
        html,
    )


def _reorder_desc_blocks(cdata: str) -> str:
    "Кнопка сверху -> описание/характеристики -> блок оплаты/доставки."
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

    # WA блок должен быть оберткой <div ...> ... </div>
    m2 = re.match(r"(?s)^(?P<open><div\b[^>]*>)(?P<inner>.*)(?P<close></div>)\s*$", wa)
    if not m2:
        return cdata

    open_div = m2.group("open")
    inner = m2.group("inner")
    close_div = m2.group("close")

    # Кнопка — это первый <p>..</p> внутри WA блока
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
    parts.append("<!-- Описание -->")
    parts.append(body)
    if pay_block:
        parts.append("<!-- Оплата и доставка -->")
        parts.append(pay_block)

    out = "\n".join(parts)

    # В конце CDATA оставляем 1 перенос для красивого закрытия ]]>
    if not out.endswith("\n"):
        out += "\n"
    return out


def _process_description_blocks(text: str) -> str:
    "Правки внутри <description><![CDATA[...]]></description>."
    rx = re.compile(r"(?s)(<description><!\[CDATA\[)(.*?)(\]\]></description>)")

    def repl(m: re.Match) -> str:
        head, cdata, tail = m.group(1), m.group(2), m.group(3)

        cdata = _normalize_cdata_prefix(cdata)
        cdata = _update_whatsapp_button_text(cdata)
        cdata = _format_delivery_numbers(cdata)
        cdata = _reorder_desc_blocks(cdata)

        return f"{head}{cdata}{tail}"

    return rx.sub(repl, text)


def _trim_keywords(text: str) -> str:
    "Убираем города из <keywords> у всех офферов."
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

        new_raw = ", ".join(filtered)
        return f"<keywords>{new_raw}</keywords>"

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
