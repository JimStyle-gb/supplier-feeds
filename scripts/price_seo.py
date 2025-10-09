#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
-----------------------------------
Создаёт docs/price_seo.yml из docs/price.yml и добавляет в НАЧАЛО каждого <description>
ВАШ ВЕРХНИЙ БЛОК (Cambria) → <hr> → красиво оформленное родное описание (Times New Roman).

Плюс: авто-выделение «ключ: значение» в характеристиках (жирным только ключ до двоеточия).
Работает и для plain-text (делаем списки), и для уже-HTML (правим <li>).

Без CDATA. Повторной вставки нет. Кодировка выхода: windows-1251 (безопасная нормализация).
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
# (НЕ меняю твою структуру; только шрифт на Cambria)
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
    """Пишем в cp1251 с безопасной нормализацией."""
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
    """
    В <li> превращаем 'Ключ: Значение' в '<strong>Ключ:</strong> Значение'.
    Стараемся не трогать, если уже есть <strong> до двоеточия.
    """
    def repl(m: re.Match) -> str:
        before = m.group(1)  # <li ...>
        body   = m.group(2)  # содержимое
        after  = m.group(3)  # </li>

        # уже есть <strong> до двоеточия — не трогаем
        if re.search(r"^\s*(?:[-–—]\s*)?<strong>[^:]{1,120}:</strong>", body, flags=re.I):
            return m.group(0)

        # выделим первую пару Ключ: Значение
        kv = re.sub(
            r"^\s*(?:[-–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$",
            lambda k: f"<strong>{k.group(1).strip()}:</strong> {k.group(2).strip()}",
            body,
            count=1,
            flags=re.S,
        )
        return f"{before}{kv}{after}"

    # матч <li>...</li> (многострочный)
    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", repl, html, flags=re.S|re.I)

def beautify_existing_html(html: str) -> str:
    """
    Уже-HTML-описание:
      • добавляем стиль ссылкам без style (тёмно-синий, без подчёркивания),
      • в <li> делаем '<strong>Ключ:</strong> Значение',
      • оборачиваем в контейнер Times New Roman.
    """
    with_links = A_NO_STYLE_RX.sub(
        f'<a style="color:{COLOR_LINK};text-decoration:none"', html
    )
    with_kv = emphasize_kv_in_li(with_links)
    wrapper = (
        f'<div style="font-family: \'Times New Roman\', Times, serif; '
        f'font-size:15px; line-height:1.55;">{with_kv}</div>'
    )
    return wrapper

def beautify_plain_text(text: str) -> str:
    """
    Plain-text → аккуратный HTML:
      • пустые строки → параграфы,
      • строки с '-' или '• ' → <ul><li><strong>Ключ:</strong> Значение</li>…</ul> если похоже на 'ключ: значение',
      • иначе — простые <li>…</li>,
      • в абзацах одинарные переносы → <br>.
    """
    t = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t:
        return ""

    blocks = [b.strip("\n") for b in re.split(r"\n{2,}", t)]
    out: list[str] = []

    def is_list_block(block: str) -> bool:
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        return bool(lines) and all(ln.startswith("- ") or ln.startswith("• ") for ln in lines)

    KV_RX = re.compile(r"^\s*(?:[-–—]\s*)?([^:]{1,120}?)\s*:\s*(.+?)\s*$")

    for blk in blocks:
        if is_list_block(blk):
            lis = []
            for ln in blk.split("\n"):
                s = ln.strip()
                if not s:
                    continue
                # убираем маркер
                if s.startswith("- ") or s.startswith("• "):
                    s = s[2:].strip()

                # если похоже на "ключ: значение" — делаем жирный ключ
                m = KV_RX.match(s)
                if m:
                    key = html_escape(m.group(1).strip())
                    val = html_escape(m.group(2).strip())
                    lis.append(f"<li><strong>{key}:</strong> {val}</li>")
                else:
                    lis.append(f"<li>{html_escape(s)}</li>")
            out.append("<ul>\n" + "\n".join(lis) + "\n</ul>")
        else:
            # параграф: одинарные переносы -> <br>
            lines = [html_escape(x) for x in blk.split("\n")]
            para = "<br>".join(lines)
            out.append(f"<p>{para}</p>")

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
    """Проверяем, вставлялся ли уже верхний блок (по ключевой фразе)."""
    return "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_html

def build_new_description(existing_inner: str) -> str:
    """[Верхний блок (Cambria)] + <hr> + [родное описание (Times New Roman, красиво)]."""
    pretty = beautify_original_description(existing_inner)
    if pretty:
        return TEMPLATE_HTML + "\n\n<hr>\n\n" + pretty
    return TEMPLATE_HTML

def inject_into_description_block(desc_inner: str) -> str:
    """Обновляет существующий <description>."""
    if has_our_block(desc_inner):
        return f"<description>{desc_inner}</description>"
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
    # 1) обновляем все существующие <description>…</description>
    def _desc_repl(m: re.Match) -> str:
        return inject_into_description_block(m.group(1))
    updated = DESC_RX.sub(_desc_repl, xml_text)

    # 2) добавляем description тем офферам, где его не было
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
    print(f"[seo] Готово: верхний блок (Cambria) + красивое родное описание (Times New Roman). Файл: {DST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
