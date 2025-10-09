#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
Копирует docs/price.yml -> docs/price_seo.yml и добавляет в НАЧАЛО каждого <description>
твой блок (с центровкой align="center"), затем горизонтальную линию <hr>, потом идёт родной текст.
Если у оффера нет <description>, он будет добавлен только с блоком (без <hr> — делить нечего).

Особенности:
- Без XML-парсера (устойчив к «грязным» & и < в тексте).
- Защита от повторной вставки (ищем фразу "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!").
- Кодировка: windows-1251. Символы вне cp1251 обрабатываем безопасно:
  ₸ -> "тг.", ≈ -> "~", NBSP -> " ", «умные» кавычки -> обычные, прочие — через xmlcharrefreplace.
"""

from pathlib import Path
import re
import sys
import io

SRC = Path("docs/price.yml")
DST = Path("docs/price_seo.yml")
ENC = "windows-1251"

# === ТВОЙ ТЕКСТ (слова/смысл без изменений), центрирование без CSS ===
TEMPLATE_HTML = """<p align="center"><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0"><strong>НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!</strong></a></p>

<p align="center">Просьба отправлять запросы в <a href="tel:+77073270501"><strong>WhatsApp: +7 (707) 327-05-01</strong></a> либо на почту: <a href="mailto:info@complex-solutions.kz"><strong>info@complex-solutions.kz</strong></a></p>

<h2>Оплата</h2>
<ul>
  <li><strong>Безналичный</strong> расчет для <u>юридических лиц</u></li>
  <li><strong>Удаленная оплата</strong> по <strong>KASPI</strong> счету для <u>физических лиц</u></li>
</ul>

<h2>Доставка</h2>
<ul>
  <li><em><strong>ДОСТАВКА</strong> в "квадрате" г. Алматы — БЕСПЛАТНО!</em></li>
  <li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 2500 тенге | 3–7 рабочих дней | Сотрудничаем с курьерской компанией <a href="https://exline.kz/"><strong>Exline.kz</strong></a></em></li>
  <li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>
  <li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал "САЙРАН"</em></li>
</ul>"""

# --- IO helpers ---
def read_cp1251(path: Path) -> str:
    with io.open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def write_cp1251(path: Path, text: str) -> None:
    # Нормализация неподдерживаемых символов под cp1251 (смысл не меняем)
    text = (text
            .replace("\u20b8", "тг.")   # ₸ -> "тг."
            .replace("\u2248", "~")     # ≈ -> "~"
            .replace("\u00A0", " ")     # NBSP -> space
            .replace("\u201C", '"').replace("\u201D", '"')  # “ ” -> "
            .replace("\u201E", '"').replace("\u201F", '"')
            .replace("\u2013", "-").replace("\u2014", "—")) # – -> -, — оставляем
    path.parent.mkdir(parents=True, exist_ok=True)
    # Редкие символы превратятся в &#NNNN; (без падения кодировки)
    with io.open(path, "w", encoding=ENC, newline="\n", errors="xmlcharrefreplace") as f:
        f.write(text)

# --- regex helpers ---
DESC_RX  = re.compile(r"<description\b[^>]*>(.*?)</description>", re.S | re.I)
OFFER_RX = re.compile(r"<offer\b[^>]*>.*?</offer>", re.S | re.I)

def strip_cdata(s: str) -> str:
    s = s.strip()
    if s.startswith("<![CDATA[") and s.endswith("]]>"):
        return s[len("<![CDATA["):-len("]]>")]
    return s

def wrap_cdata(s: str) -> str:
    # Не допускаем "]]>" внутри CDATA
    return "<![CDATA[" + s.replace("]]>", "]]&gt;") + "]]>"

def inject_into_description_block(desc_inner: str) -> str:
    """Вставляет шаблон в начало блока описания, если его там ещё нет.
       После блока добавляем разделяющую линию <hr>, и затем идёт родной текст.
    """
    inner_clean = strip_cdata(desc_inner)
    # защита от повторной вставки
    if "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in inner_clean:
        return f"<description>{desc_inner}</description>"

    # если есть существующий текст — вставляем <hr> между блоком и родным описанием
    if inner_clean.strip():
        combined = TEMPLATE_HTML + "\n\n<hr>\n\n" + inner_clean.strip()
    else:
        combined = TEMPLATE_HTML

    return "<description>" + wrap_cdata(combined) + "</description>"

def add_description_if_missing(offer_block: str) -> str:
    """Если в оффере нет description — вставляем его перед </offer> с твоим блоком."""
    if re.search(r"<description\b", offer_block, flags=re.I):
        return offer_block
    # вычислим базовый отступ (по остальным тегам внутри оффера)
    m = re.search(r"\n([ \t]+)<", offer_block)
    indent = (m.group(1) if m else "  ")
    insertion = f"\n{indent}<description>{wrap_cdata(TEMPLATE_HTML)}</description>"
    return offer_block.replace("</offer>",
                              (insertion + "\n" + indent[:-2] + "</offer>") if len(indent) >= 2
                              else (insertion + "\n</offer>"))

def process_whole_text(xml_text: str) -> str:
    # 1) обновляем существующие description
    def _repl(m: re.Match) -> str:
        inner = m.group(1)
        return inject_into_description_block(inner)
    text_after_desc = DESC_RX.sub(_repl, xml_text)
    # 2) добавляем для тех офферов, где description отсутствует
    def _offer_repl(m: re.Match) -> str:
        block = m.group(0)
        if re.search(r"<description\b", block, flags=re.I):
            return block
        return add_description_if_missing(block)
    return OFFER_RX.sub(_offer_repl, text_after_desc)

def main() -> int:
    if not SRC.exists():
        print(f"[seo] Исходный файл не найден: {SRC}", file=sys.stderr)
        return 1
    original = read_cp1251(SRC)
    processed = process_whole_text(original)
    write_cp1251(DST, processed)
    print(f"[seo] Готово: блок добавлен сверху, <hr> между блоком и родным описанием. Файл: {DST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
