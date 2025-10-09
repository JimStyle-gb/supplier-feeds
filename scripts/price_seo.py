#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
-----------------------------------
Создаёт docs/price_seo.yml из docs/price.yml и добавляет в НАЧАЛО каждого <description>
ваш маркетинговый блок: зелёная «кнопка» WhatsApp с округлёнными углами, контакты
(тёмно-синие ссылки без подчёркивания), затем <hr>, затем — родное описание.

Без CDATA. Повторной вставки нет (ищем фразу "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!").
Кодировка: windows-1251 с безопасной нормализацией.
"""

from __future__ import annotations

from pathlib import Path
import io
import re
import sys

# ─────────────────────────── Конфигурация ───────────────────────────

SRC: Path = Path("docs/price.yml")
DST: Path = Path("docs/price_seo.yml")
ENC: str  = "windows-1251"

# Цвета
COLOR_LINK  = "#0b3d91"   # тёмно-синий для ссылок
COLOR_WHITE = "#ffffff"   # белый для текста на кнопке
COLOR_BTN   = "#27ae60"   # зелёный фон кнопки
COLOR_KASPI = "#8b0000"   # тёмно-красный для KASPI

# Весь блок — в шрифте Arial/Helvetica; кнопка с border-radius; все ссылки без подчёркивания
TEMPLATE_HTML: str = f"""<div style="font-family: Cambria, Helvetica, sans-serif;">
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

# ─────────────────────────── Вспомогательные ───────────────────────────

def has_our_block(desc_html: str) -> bool:
    """Проверяем, вставлялся ли уже наш блок (по ключевой фразе)."""
    return "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_html

def build_new_description(existing_inner: str) -> str:
    """
    Формируем новое содержимое <description>:
      [ваш блок] + <hr> + [родной текст], если он был.
    """
    if existing_inner.strip():
        return TEMPLATE_HTML + "\n\n<hr>\n\n" + existing_inner.strip()
    else:
        return TEMPLATE_HTML  # если родного текста не было — делить нечего

def inject_into_description_block(desc_inner: str) -> str:
    """Обновляет существующий <description>."""
    if has_our_block(desc_inner):
        return f"<description>{desc_inner}</description>"
    return "<description>" + build_new_description(desc_inner) + "</description>"

def add_description_if_missing(offer_block: str) -> str:
    """
    Если <description> отсутствует — создаём его перед </offer> с вашим блоком.
    (<hr> не нужен, потому что нет «второй части».)
    """
    if re.search(r"<description\b", offer_block, flags=re.I):
        return offer_block
    # подхватываем отступ для красивого вида
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

def main() -> int:
    if not SRC.exists():
        print(f"[seo] Исходный файл не найден: {SRC}", file=sys.stderr)
        return 1

    original  = read_cp1251(SRC)
    processed = process_whole_text(original)
    write_cp1251(DST, processed)

    print(f"[seo] Готово: блок с округлой кнопкой и красивым шрифтом добавлен. Файл: {DST}")
    return 0

# ─────────────────────────── Точка входа ───────────────────────────

if __name__ == "__main__":
    raise SystemExit(main())
