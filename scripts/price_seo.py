#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
Копирует docs/price.yml -> docs/price_seo.yml и добавляет в НАЧАЛО каждого <description>
единый HTML-блок (через CDATA). Родной текст описания сохраняется и идёт сразу после.
Если у оффера нет <description>, он будет добавлен с этим блоком.

Особенности:
- Без XML-парсера (устойчив к «грязным» & и < в тексте).
- Защита от двойной вставки (ищем маркер "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!").
- Кодировка записи: windows-1251. Символ ₸ автоматически заменяется на "тг.".
- Между </offer> и <offer> исходное форматирование сохраняется.
"""

from pathlib import Path
import re
import shutil
import sys
import os
import io

SRC = Path("docs/price.yml")
DST = Path("docs/price_seo.yml")
ENC = "windows-1251"

# --- HTML-шаблон (без «умных» кавычек и без символа ₸) ---
TEMPLATE_HTML = """<p style="text-align:center;margin:0 0 12px;">
  <a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" target="_blank"><strong>НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!</strong></a>
</p>

<p style="text-align:center;margin:0 0 16px;">
  Просьба отправлять запросы в
  <a href="tel:+77073270501" target="_self"><strong>WhatsApp: +7 (707) 327-05-01</strong></a>
  либо на почту:
  <a href="mailto:info@complex-solutions.kz" target="_self"><strong>info@complex-solutions.kz</strong></a>
</p>

<h2 style="margin:12px 0 6px;">Оплата</h2>
<ul style="margin:0 0 12px 18px;">
  <li><strong>Безналичный</strong> расчет для <u>юридических лиц</u></li>
  <li><strong>Удаленная оплата</strong> по <strong style="color:#c0392b;">KASPI</strong> счету для <u>физических лиц</u></li>
</ul>

<h2 style="margin:12px 0 6px;">Доставка</h2>
<ul style="margin:0 0 12px 18px;">
  <li><em><strong>ДОСТАВКА</strong> в "квадрате" г. Алматы — БЕСПЛАТНО!</em></li>
  <li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 2500 тенге | 3–7 рабочих дней | Сотрудничаем с курьерской компанией <a href="https://exline.kz/" target="_blank"><strong>Exline.kz</strong></a></em></li>
  <li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>
  <li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал "САЙРАН"</em></li>
</ul>"""

# --- утилиты чтения/записи ---
def read_cp1251(path: Path) -> str:
    with io.open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def write_cp1251(path: Path, text: str) -> None:
    # заменяем символ тенге на "тг." (₸ не входит в cp1251)
    text = text.replace("\u20b8", "тг.")
    path.parent.mkdir(parents=True, exist_ok=True)
    with io.open(path, "w", encoding=ENC, newline="\n", errors="strict") as f:
        f.write(text)

# --- helpers ---
DESC_RX = re.compile(r"<description\b[^>]*>(.*?)</description>", re.S | re.I)
OFFER_RX = re.compile(r"<offer\b[^>]*>.*?</offer>", re.S | re.I)

def strip_cdata(s: str) -> str:
    s = s.strip()
    if s.startswith("<![CDATA[") and s.endswith("]]>"):
        return s[len("<![CDATA["):-len("]]>")]
    return s

def wrap_cdata(s: str) -> str:
    # не допускаем последовательность "]]>" внутри CDATA
    return "<![CDATA[" + s.replace("]]>", "]]&gt;") + "]]>"

def inject_into_description_block(desc_inner: str) -> str:
    """Вставляет шаблон в начало блока описания, если его там ещё нет."""
    inner_clean = strip_cdata(desc_inner)
    # Проверка на повторную вставку
    if "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in inner_clean:
        return f"<description>{desc_inner}</description>"
    # Собираем: шаблон + пустая строка + исходный текст (если был)
    if inner_clean.strip():
        combined = TEMPLATE_HTML + "\n\n" + inner_clean.strip()
    else:
        combined = TEMPLATE_HTML
    return "<description>" + wrap_cdata(combined) + "</description>"

def add_description_if_missing(offer_block: str) -> str:
    """Если в оффере нет description — вставляем его перед </offer> с шаблоном."""
    if re.search(r"<description\b", offer_block, flags=re.I):
        return offer_block
    # вычислим базовый отступ (по остальным тегам внутри оффера)
    m = re.search(r"\n([ \t]+)<", offer_block)
    indent = (m.group(1) if m else "  ")
    insertion = f"\n{indent}<description>{wrap_cdata(TEMPLATE_HTML)}</description>"
    return offer_block.replace("</offer>", insertion + "\n" + indent[:-2] + "</offer>" if len(indent) >= 2 else insertion + "\n</offer>")

def process_whole_text(xml_text: str) -> str:
    # 1) сначала обрабатываем все существующие <description>...</description>
    def _repl(m: re.Match) -> str:
        inner = m.group(1)
        return inject_into_description_block(inner)

    text_after_desc = DESC_RX.sub(_repl, xml_text)

    # 2) затем добавим description тем офферам, где его нет
    def _offer_repl(m: re.Match) -> str:
        block = m.group(0)
        if re.search(r"<description\b", block, flags=re.I):
            return block
        return add_description_if_missing(block)

    final_text = OFFER_RX.sub(_offer_repl, text_after_desc)
    return final_text

def main() -> int:
    if not SRC.exists():
        print(f"[seo] Исходный файл не найден: {SRC}", file=sys.stderr)
        return 1

    # побайтная копия на случай отладки (можно закомментировать)
    # shutil.copyfile(SRC, DST)

    original = read_cp1251(SRC)
    processed = process_whole_text(original)
    write_cp1251(DST, processed)

    print(f"[seo] Готово: добавлен шаблон в описания. Файл: {DST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
