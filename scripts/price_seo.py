#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
-----------------------------------
Создаёт docs/price_seo.yml из docs/price.yml и добавляет в НАЧАЛО каждого <description>
ВАШ ВЕРХНИЙ БЛОК (Cambria) → <hr> → красиво оформленное родное описание (Times New Roman).

Исправления:
  • Даже если верхний блок уже был, часть ПОСЛЕ <hr> теперь оформляется (раньше пропускалась).
  • Жирнение «ключа» в парах "Ключ: Значение" работает в <li>, <p> и строках с <br>:
      - Гарантия: 1 год → <strong>Гарантия:</strong> 1 год

Без CDATA. Повторной вставки блока нет. Кодировка: windows-1251 (безопасная нормализация).
"""

from __future__ import annotations

from pathlib import Path
import io
import re
import sys
from html import escape as html_escape

# ─────────────────────────── Конфигурация ───────────────────────────

SRC: Path = Path("docs/price.yml")
DST: Path = Path("docs/price_seo.yml")
ENC: str  = "windows-1251"

# Цвета
COLOR_LINK  = "#0b3d91"   # тёмно-синий для ссылок
COLOR_WHITE = "#ffffff"   # белый для текста на кнопке
COLOR_BTN   = "#27ae60"   # зелёный фон кнопки
COLOR_KASPI = "#8b0000"   # тёмно-красный для KASPI

# ── ВЕРХНИЙ БЛОК (Cambria) ──────────────────────────────────────────
TEMPLATE_HTML: str = f"""<div style="font-family: Cambria, 'Times New Roman', serif;">
  <center>
    <a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0"
       style="display:inline-block;background:{COLOR_BTN};color:{COLOR_WHITE};text-decoration:none;padding:10px 16px;border-radius:8px;font-weight:700;">
      НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!
    </a>
  </center>

  <center>
    Просьба отправлять запросы в
    <a href="tel:+77073270501" style="color:{COLOR_LINK};text-decoration:none;"><strong>WhatsApp: +7 (707) 327-05-01</strong></a>
    либо на почту:
    <a href="mailto:info@complex-solutions.kz" style="color:{COLOR_LINK};text-decoration:none;"><strong>info@complex-solutions.kz</strong></a>
  </center>

  <h2>Оплата</h2>
  <ul>
    <li><strong>Безналичный</strong> расчет для <u>юридических лиц</u></li>
    <li><strong>Удаленная оплата</strong> по <font color="{COLOR_KASPI}"><strong>KASPI</strong></font> счету для <u>физических лиц</u></li>
  </ul>

  <h2>Доставка</h2>
  <ul>
    <li><em><strong>ДОСТАВКА</strong> в "квадрате" г. Алматы — БЕСПЛАТНО!</em></li>
    <li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тенге | 3–7 рабочих дней | Сотрудничаем с курьерской компанией
      <a href="https://exline.kz/" style="color:{COLOR_LINK};text-decoration:none;"><strong>Exline.kz</strong></a></em>
    </li>
    <li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>
    <li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал "САЙРАН"</em></li>
  </ul>
</div>"""

# ─────────────────────────── I/O утилиты ───────────────────────────

def read_cp1251(path: Path) -> str:
    with io.open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def write_cp1251(path: Path, text: str) -> None:
    safe = (
        text
        .replace("\u20B8", "тг.")   # ₸ → "тг."
        .replace("\u2248", "~")     # ≈ → "~"
        .replace("\u00A0", " ")     # NBSP → пробел
        .replace("\u201C", '"').replace("\u201D", '"')  # “ ” → "
        .replace("\u201E", '"').replace("\u201F", '"')  # „ ‟ → "
        .replace("\u2013", "-").replace("\u2014", "—")  # – → -, — оставляем
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with io.open(path, "w", encoding=ENC, newline="\n", errors="xmlcharrefreplace") as f:
        f.write(safe)

# ─────────────────────────── Регэкспы ───────────────────────────

DESC_RX        = re.compile(r"<description\b[^>]*>(.*?)</description>", re.I | re.S)
OFFER_RX       = re.compile(r"<offer\b[^>]*>.*?</offer>",                re.I | re.S)
HAS_HTML_TAGS  = re.compile(r"<[a-zA-Z/!][^>]*>")  # грубая проверка на наличие HTML
A_NO_STYLE_RX  = re.compile(r"<a(?![^>]*\bstyle=)", re.I)  # <a ...> без style=

# ─────────────────────────── Оформление родного описания ───────────────────────────

def emphasize_kv_in_li(html: str) -> str:
    """В <li> превращаем 'Ключ: Значение' в '<strong>Ключ:</strong> Значение'."""
    def repl(m: re.Match) -> str:
        before, body, after = m.group(1), m.group(2), m.group(3)
        # уже жирно до двоеточия — не трогаем
        if re.search(r"^\s*(?:[-–—]\s*)?<strong>[^:]{1,120}:</strong>", body, flags=re.I):
            return m.group(0)
        kv = re.sub(
            r"^\s*(?:[-•–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$",
            lambda k: f"<strong>{k.group(1).strip()}:</strong> {k.group(2).strip()}",
            body, count=1, flags=re.S
        )
        return f"{before}{kv}{after}"
    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", repl, html, flags=re.S | re.I)

PARA_RX = re.compile(r"(<p[^>]*>)(.*?)(</p>)", re.S | re.I)
BR_SPLIT_RX = re.compile(r"(<br\s*/?>)", re.I)
KV_LINE_RX = re.compile(r"^\s*(?:[-•–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$", re.S)

def emphasize_kv_in_paragraphs(html: str) -> str:
    """
    Внутри <p> ищем строки (по <br>) вида 'Ключ: Значение' и жирним ключ.
    """
    def para_repl(m: re.Match) -> str:
        start, body, end = m.group(1), m.group(2), m.group(3)
        parts = BR_SPLIT_RX.split(body)  # сохраняем <br> как токены
        for i in range(0, len(parts), 2):
            if i >= len(parts): break
            line = parts[i]
            if not line: continue
            mm = KV_LINE_RX.match(line.strip())
            if mm:
                key = mm.group(1).strip()
                val = mm.group(2).strip()
                parts[i] = re.sub(KV_LINE_RX, f"<strong>{html_escape(key)}:</strong> {html_escape(val)}", line)
        return start + "".join(parts) + end
    return PARA_RX.sub(para_repl, html)

def beautify_existing_html(html: str) -> str:
    """
    Уже-HTML-описание:
      • добавляем стиль ссылкам без style,
      • жирним 'Ключ:' в <li> и <p>/<br>,
      • оборачиваем в контейнер Times New Roman.
    """
    with_links = A_NO_STYLE_RX.sub(
        f'<a style="color:{COLOR_LINK};text-decoration:none"', html
    )
    with_li = emphasize_kv_in_li(with_links)
    with_p  = emphasize_kv_in_paragraphs(with_li)
    wrapper = (
        f'<div style="font-family: \'Times New Roman\', Times, serif; '
        f'font-size:15px; line-height:1.55;">{with_p}</div>'
    )
    return wrapper

def beautify_plain_text(text: str) -> str:
    """
    Plain-text → аккуратный HTML c жирным ключом.
    """
    t = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t:
        return ""
    blocks = [b.strip("\n") for b in re.split(r"\n{2,}", t)]
    out: list[str] = []

    def is_list_block(block: str) -> bool:
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        return bool(lines) and all(ln.startswith("- ") or ln.startswith("• ") for ln in lines)

    for blk in blocks:
        if is_list_block(blk):
            lis = []
            for ln in blk.split("\n"):
                s = ln.strip()
                if not s: continue
                if s.startswith("- ") or s.startswith("• "):
                    s = s[2:].strip()
                m = KV_LINE_RX.match(s)
                if m:
                    key = html_escape(m.group(1).strip())
                    val = html_escape(m.group(2).strip())
                    lis.append(f"<li><strong>{key}:</strong> {val}</li>")
                else:
                    lis.append(f"<li>{html_escape(s)}</li>")
            out.append("<ul>\n" + "\n".join(lis) + "\n</ul>")
        else:
            lines = [html_escape(x) for x in blk.split("\n")]
            # применим жирнение к первой паре "ключ: значение" строки
            if lines and KV_LINE_RX.match(blk.strip()):
                m = KV_LINE_RX.match(blk.strip())
                key = html_escape(m.group(1).strip())
                val = html_escape(m.group(2).strip())
                para = f"<strong>{key}:</strong> {val}"
                out.append(f"<p>{para}</p>")
            else:
                out.append(f"<p>{'<br>'.join(lines)}</p>")

    wrapped = (
        f'<div style="font-family: \'Times New Roman\', Times, serif; '
        f'font-size:15px; line-height:1.55;">' + "\n".join(out) + "</div>"
    )
    return wrapped

def beautify_original_description(existing_inner: str) -> str:
    """Возвращает красиво оформленный HTML оригинального описания (Times New Roman)."""
    content = existing_inner.strip()
    if not content:
        return ""
    if HAS_HTML_TAGS.search(content):
        return beautify_existing_html(content)
    return beautify_plain_text(content)

# ─────────────────────────── Сборка description ───────────────────────────

def has_our_block(desc_html: str) -> bool:
    return "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_html

HR_RX = re.compile(r"<hr\b[^>]*>", re.I)

def rebuild_with_existing_block(desc_inner: str) -> str:
    """
    Если блок уже есть: отделяем всё ПОСЛЕ первого <hr>, оформляем, собираем обратно.
    Если <hr> нет — добавляем его и оформляем хвост.
    """
    # Если хвост уже оформлен Times New Roman — ничего не делаем
    if "font-family: 'Times New Roman'" in desc_inner or 'font-family: "Times New Roman"' in desc_inner:
        return f"<description>{desc_inner}</description>"

    parts = HR_RX.split(desc_inner, maxsplit=1)
    if len(parts) == 2:
        head, tail = parts[0], parts[1]
        pretty_tail = beautify_original_description(tail)
        if pretty_tail:
            return "<description>" + head + "<hr>\n\n" + pretty_tail + "</description>"
        else:
            return "<description>" + desc_inner + "</description>"
    else:
        # <hr> ещё не стоит: добавим и оформим всё, что после блока
        # пытаемся отделить наш верхний блок — он заканчивается ближайшим </div> после кнопки
        cut = desc_inner
        m = re.search(r"</div>\s*$", cut)  # часто блок заканчивается </div>
        head = desc_inner
        tail = ""
        # Точная граница непредсказуема; поступим просто: добавим <hr> и не тронем head,
        # а tail оставим пустым, чтобы не повредить существующий контент.
        return "<description>" + head + "<hr></description>"

def build_new_description(existing_inner: str) -> str:
    """[Верхний блок (Cambria)] + <hr> + [красиво оформленный родной текст]."""
    pretty = beautify_original_description(existing_inner)
    if pretty:
        return TEMPLATE_HTML + "\n\n<hr>\n\n" + pretty
    return TEMPLATE_HTML

def inject_into_description_block(desc_inner: str) -> str:
    """Обновляет существующий <description>."""
    if has_our_block(desc_inner):
        return rebuild_with_existing_block(desc_inner)
    return "<description>" + build_new_description(desc_inner) + "</description>"

def add_description_if_missing(offer_block: str) -> str:
    """Если описания нет — вставляем только верхний блок (без <hr>)."""
    if re.search(r"<description\b", offer_block, flags=re.I):
        return offer_block
    m = re.search(r"\n([ \t]+)<", offer_block)
    indent = m.group(1) if m else "  "
    insertion = f"\n{indent}<description>{TEMPLATE_HTML}</description>"
    tail = (insertion + "\n" + indent[:-2] + "</offer>") if len(indent) >= 2 else (insertion + "\n</offer>")
    return offer_block.replace("</offer>", tail)

# ─────────────────────────── Основная логика ───────────────────────────

def process_whole_text(xml_text: str) -> str:
    def _desc_repl(m: re.Match) -> str:
        return inject_into_description_block(m.group(1))
    updated = DESC_RX.sub(_desc_repl, xml_text)

    def _offer_repl(m: re.Match) -> str:
        block = m.group(0)
        if re.search(r"<description\b", block, flags=re.I):
            return block
        return add_description_if_missing(block)
    return OFFER_RX.sub(_offer_repl, updated)

# ─────────────────────────── Точка входа ───────────────────────────

def main() -> int:
    if not SRC.exists():
        print(f"[seo] Исходный файл не найден: {SRC}", file=sys.stderr)
        return 1
    original  = read_cp1251(SRC)
    processed = process_whole_text(original)
    write_cp1251(DST, processed)
    print(f"[seo] Готово: верхний блок + оформленный хвост; ключи в характеристиках жирные. Файл: {DST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
