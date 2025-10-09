#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
-----------------------------------
Создаёт docs/price_seo.yml из docs/price.yml и добавляет в НАЧАЛО каждого <description>
ваш маркетинговый блок (НЕ МЕНЯЛ: зелёная кнопка, тёмно-синие ссылки без подчёркивания,
KASPI тёмно-красным, доставка 5000), затем <hr>, затем — красиво оформленное родное описание.

Красивое оформление родного описания:
  • Если там уже HTML — лишь оборачиваем в <div> со шрифтом Cambria и line-height, и добавляем стиль ссылкам без style.
  • Если это plain-text — превращаем в чистый HTML: параграфы, списки (- и •), <br> внутри абзацев, экранирование.

Без CDATA. Повторной вставки нет.
Кодировка вывода: windows-1251 с безопасной нормализацией.
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

# Цвета (сохраняем как в твоём блоке)
COLOR_LINK  = "#0b3d91"   # тёмно-синий для ссылок
COLOR_WHITE = "#ffffff"   # белый для текста на кнопке
COLOR_BTN   = "#27ae60"   # зелёный фон кнопки
COLOR_KASPI = "#8b0000"   # тёмно-красный для KASPI

# ── ТВОЙ ВЕРХНИЙ БЛОК: НЕ МЕНЯЛ ─────────────────────────────────────
TEMPLATE_HTML: str = f"""<div style="font-family: Arial, Helvetica, sans-serif;">
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
    """
    Пишем в cp1251:
      - заменяем неподдерживаемые символы на безопасные аналоги;
      - прочие редкие символы превращаем в &#NNNN; (xmlcharrefreplace).
    """
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

DESC_RX  = re.compile(r"<description\b[^>]*>(.*?)</description>", re.I | re.S)
OFFER_RX = re.compile(r"<offer\b[^>]*>.*?</offer>",                re.I | re.S)

HAS_HTML_TAGS_RX = re.compile(r"<[a-zA-Z/!][^>]*>")  # грубая проверка на наличие HTML-тегов
A_NO_STYLE_RX    = re.compile(r"<a(?![^>]*\bstyle=)", re.I)  # <a ...> без style=

# ─────────────────────────── Оформление родного описания ───────────────────────────

def beautify_existing_html(html: str) -> str:
    """
    Уже-HTML-описание: не трогаем структуру, только:
      • добавляем стиль ссылкам без style (тёмно-синий, без подчёркивания),
      • оборачиваем в див со шрифтом Cambria и line-height.
    """
    with_links = A_NO_STYLE_RX.sub(
        f'<a style="color:{COLOR_LINK};text-decoration:none"', html
    )
    wrapper = (
        f'<div style="font-family: Cambria, \'Times New Roman\', serif; '
        f'font-size:15px; line-height:1.55;">{with_links}</div>'
    )
    return wrapper

def beautify_plain_text(text: str) -> str:
    """
    Превращаем plain-text в приятный HTML:
      • блоки по пустым строкам → параграфы или списки,
      • строки, начинающиеся с "-" или "•" → <ul><li>…</li></ul>,
      • одинарные переносы внутри абзаца сохраняем как <br>,
      • всё экранируем.
    """
    t = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t:
        return ""

    blocks = [b.strip("\n") for b in re.split(r"\n{2,}", t)]
    out: list[str] = []

    def is_list_block(block: str) -> bool:
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        if not lines:
            return False
        return all(ln.startswith("- ") or ln.startswith("• ") for ln in lines)

    for blk in blocks:
        if is_list_block(blk):
            lis = []
            for ln in blk.split("\n"):
                s = ln.strip()
                if not s:
                    continue
                s = s[2:]  # убираем "- " или "• "
                lis.append(f"<li>{html_escape(s)}</li>")
            out.append("<ul>\n" + "\n".join(lis) + "\n</ul>")
        else:
            # параграф: одинарные переносы -> <br>
            lines = [html_escape(x) for x in blk.split("\n")]
            para = "<br>".join(lines)
            out.append(f"<p>{para}</p>")

    wrapped = (
        f'<div style="font-family: Cambria, \'Times New Roman\', serif; '
        f'font-size:15px; line-height:1.55;">' + "\n".join(out) + "</div>"
    )
    return wrapped

def beautify_original_description(existing_inner: str) -> str:
    """
    Выдаёт красиво оформленный HTML оригинального описания.
    """
    content = existing_inner.strip()
    if not content:
        return ""

    if HAS_HTML_TAGS_RX.search(content):
        return beautify_existing_html(content)
    else:
        return beautify_plain_text(content)

# ─────────────────────────── Сборка description ───────────────────────────

def has_our_block(desc_html: str) -> bool:
    """Проверяем, вставлялся ли уже наш верхний блок (по ключевой фразе)."""
    return "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_html

def build_new_description(existing_inner: str) -> str:
    """
    Конструируем новое <description>:
      [ваш верхний блок] + <hr> + [красиво оформленный родной текст] (если он был).
    """
    pretty = beautify_original_description(existing_inner)
    if pretty:
        return TEMPLATE_HTML + "\n\n<hr>\n\n" + pretty
    else:
        return TEMPLATE_HTML

def inject_into_description_block(desc_inner: str) -> str:
    """Обновляет существующий <description>."""
    if has_our_block(desc_inner):
        return f"<description>{desc_inner}</description>"
    return "<description>" + build_new_description(desc_inner) + "</description>"

def add_description_if_missing(offer_block: str) -> str:
    """
    Если <description> отсутствует — создаём его перед </offer> с твоим блоком (без <hr>).
    """
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
        inner = m.group(1)
        return inject_into_description_block(inner)

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

    print(f"[seo] Готово: блок добавлен, родное описание красиво оформлено (Cambria). Файл: {DST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
