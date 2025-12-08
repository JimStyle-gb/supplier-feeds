#!/usr/bin/env python3
# AlStyle post-process (v23)
# - Линия между кнопкой и описанием ОБЯЗАТЕЛЬНО есть и всегда "жирность 2" (border-top:2px)
# - Убирает дубль HR (1px/2px) в верхней зоне: оставляет ровно один HR 2px
# - Убирает дубли комментария "<!-- Оплата и доставка -->" (оставляет 1 раз)
# - Если блок оплаты/доставки попал внутрь "Описание" — вырезаем и переносим вниз
# - WhatsApp: текст кнопки "Написать в WhatsApp"
# - Доставка: 5000 тг. -> 5 000 тг.
# - Keywords: убрать города (Темиртау, Экибастуз, Орал, Оскемен, Кокшетау, Семей)
# - Безопасная запись в windows-1251 (чтобы workflow/commit не падал)

import os
import re
import sys
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo


_CITIES_DROP = {"Темиртау", "Экибастуз", "Орал", "Оскемен", "Кокшетау", "Семей"}

# Одна линия-разделитель (жирность 2)
_HR_2PX = '<hr style="border:none; border-top:2px solid #E7D6B7; margin:12px 0;" />'

# Любой HR (только для чистки "верхней зоны" вне блока оплаты/доставки)
_RX_ANY_HR = re.compile(r"(?is)<hr\b[^>]*>")

# Коммент "Оплата и доставка"
_RX_PAY_COMMENT = re.compile(r"(?is)<!--\s*Оплата\s+и\s+доставка\s*-->\s*")

# Кнопка WhatsApp (первый div с кнопкой)
_RX_WA_BTN = re.compile(
    r'(?s)(<div\s+style="font-family:\s*Cambria,\s*\x27Times New Roman\x27,\s*serif;\s*line-height:1\.5;\s*color:#222;\s*font-size:15px;">'
    r'\s*<p\b[^>]*>\s*<a\b[^>]*>.*?</a>\s*</p>\s*</div>)'
)

# Блок оплаты/доставки (фон/рамка) — внутри него есть свой HR 1px, его НЕ трогаем
_RX_PAYBLOCK = re.compile(
    r'(?s)(<div\s+style="font-family:\s*Cambria,\s*\x27Times New Roman\x27,\s*serif;\s*line-height:1\.5;\s*color:#222;\s*font-size:15px;">\s*'
    r'<div\s+style="background:#FFF6E5;\s*border:1px\s+solid\s+#F1E2C6;.*?</div>\s*</div>)'
)


# Время сборки (Алматы) — ДОЛЖНО быть временем раннера, а не датой поставщика
_RX_FEED_META_BUILD_TIME = re.compile(r"(?m)^(Время сборки \(Алматы\)\s*\|\s*)(.+)$")


def _almaty_now_str() -> str:
    'Берём ALMATY_NOW из workflow (если задан), иначе считаем локально (Asia/Almaty).'
    env = (os.getenv("ALMATY_NOW", "") or "").strip()
    if env:
        # допускаем "YYYY-MM-DD HH:MM" или "YYYY-MM-DD HH:MM:SS"
        m = re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$", env)
        if m:
            return env if len(env) > 16 else (env + ":00")
    return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S")


def _fix_feed_meta_build_time(text: str) -> str:
    'Обновляем только значение в строке FEED_META, вид/структуру не меняем.'
    now_s = _almaty_now_str()

    def repl(m: re.Match) -> str:
        return f"{m.group(1)}{now_s}"

    return _RX_FEED_META_BUILD_TIME.sub(repl, text, count=1)



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


def _compact_blank_lines(s: str) -> str:
    'Сжимаем только 3+ переносов, чтобы не ломать нужные переносы.'
    s = s.replace("\r\n", "\n")
    return re.sub(r"\n{3,}", "\n\n", s)


def _update_whatsapp_button_text(html: str) -> str:
    'Меняем текст кнопки, не трогая стили/ссылку.'
    html = html.replace("НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!", "Написать в WhatsApp")
    html = html.replace("НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ В WHATSAPP!", "Написать в WhatsApp")
    html = re.sub(
        r"(?s)(&#128172;\s*)([^<]{1,180}?)(WHATSAPP!?)(\s*)</a>",
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


def _split_payment_from_body(body: str) -> tuple[str, str]:
    'Если внутри body есть блок оплаты/доставки — вырезаем его.'
    m = _RX_PAYBLOCK.search(body)
    if not m:
        return body.strip(), ""
    pay = m.group(1).strip()
    body2 = (body[: m.start()] + body[m.end() :]).strip()
    body2 = _compact_blank_lines(body2)
    return body2, pay


def _extract_btn_and_pay_from_wa(wa: str) -> tuple[str, str]:
    'Достаём кнопку и (если есть) оплату/доставку из WhatsApp-зоны, не ломая внутренний HR оплаты.'
    wa = wa.replace("\r\n", "\n")
    wa = _RX_PAY_COMMENT.sub("", wa).strip()

    m_btn = _RX_WA_BTN.search(wa)
    if not m_btn:
        return wa.strip(), ""

    btn = m_btn.group(1).strip()
    rest = (wa[: m_btn.start()] + wa[m_btn.end() :]).strip()

    # Сначала ищем блок оплаты/доставки как есть (с внутренним HR)
    m_pay = _RX_PAYBLOCK.search(rest)
    if m_pay:
        pay = m_pay.group(1).strip()
        return btn, pay

    # Если оплаты нет — просто кнопка
    return btn, ""


def _cleanup_top_hrs(text: str) -> str:
    'Убираем ВСЕ HR сразу после кнопки (старые 1px/2px), оставим один правильный при сборке.'
    text = text.replace("\r\n", "\n")
    # после </div> в верхней зоне до <!-- Описание -->
    text = re.sub(
        r"(?s)(</div>)\s*(?:<hr\b[^>]*>\s*)+(?=<!--\s*Описание\s*-->)",
        r"\1\n",
        text,
    )
    return text


def _dedupe_pay_comments(text: str) -> str:
    'Оставляем только один "<!-- Оплата и доставка -->" перед блоком оплаты.'
    text = text.replace("\r\n", "\n")
    # Схлопнуть повторения подряд
    text = re.sub(r"(?s)(<!--\s*Оплата\s+и\s+доставка\s*-->\s*){2,}", "<!-- Оплата и доставка -->\n", text)
    return text


def _reorder_desc_blocks(cdata: str) -> str:
    'Сборка под эталон: один HR 2px, один коммент оплаты.'
    if "<!-- WhatsApp -->" not in cdata or "<!-- Описание -->" not in cdata:
        return cdata

    cdata = _cleanup_top_hrs(cdata)
    cdata = _dedupe_pay_comments(cdata)
    # На всякий случай уберём комменты оплаты внутри текста (потом вставим один правильный)
    cdata = _RX_PAY_COMMENT.sub("", cdata)

    m = re.search(
        r"(?s)<!--\s*WhatsApp\s*-->\s*(?P<wa>.*?)\s*<!--\s*Описание\s*-->\s*(?P<body>.*)$",
        cdata,
    )
    if not m:
        return cdata

    wa = m.group("wa")
    body = m.group("body")

    btn_block, pay_from_wa = _extract_btn_and_pay_from_wa(wa)

    body2, pay_from_body = _split_payment_from_body(body)

    pay_block = pay_from_wa or pay_from_body
    if pay_block:
        pay_block = _RX_PAY_COMMENT.sub("", pay_block).strip()

    parts = []
    parts.append("<!-- WhatsApp -->")
    parts.append(btn_block.strip())
    parts.append(_HR_2PX)  # ОБЯЗАТЕЛЬНО
    parts.append("<!-- Описание -->")
    parts.append(body2.strip())

    if pay_block:
        parts.append("<!-- Оплата и доставка -->")
        parts.append(pay_block.strip())

    out = "\n".join(parts)
    out = _compact_blank_lines(out)

    # Убрать пустую строку перед "<!-- Оплата и доставка -->" (сразу после </ul>)
    out = re.sub(r"(</ul>)\n\n+(<!--\s*Оплата\s+и\s+доставка\s*-->)", r"\1\n\2", out)

    # Финально: никаких дублей коммента оплаты
    out = _dedupe_pay_comments(out)

    # Финально: если вдруг где-то вылезли лишние HR между кнопкой и описанием — убрать, и оставить один
    out = re.sub(
        r"(?s)(</div>)\n(?:<hr\b[^>]*>\s*)+(?=<!--\s*Описание\s*-->)",
        r"\1\n" + _HR_2PX + "\n",
        out,
    )

    return out


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

    text = _fix_feed_meta_build_time(text)
    text = _process_description_blocks(text)
    text = _trim_keywords(text)

    _write_cp1251_safe(out_file, text)

    print(f"[alstyle post] ok: hr2px + dedupe comments + keywords + windows-1251 safe -> {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
