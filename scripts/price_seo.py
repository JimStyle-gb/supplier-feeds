#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
Копирует docs/price.yml -> docs/price_seo.yml и добавляет в НАЧАЛО каждого <description>
единый «safe» HTML-блок (без инлайн-стилей). Родной текст описания сохраняется и идёт сразу после.
Если у оффера нет <description>, он будет добавлен с этим блоком.

Особенности:
- Без XML-парсера (устойчив к «грязным» & и < в тексте).
- Защита от двойной вставки (ищем маркер "Написать нам в WhatsApp").
- Кодировка: windows-1251. Символы вне cp1251 обрабатываем безопасно:
  ₸ -> "тг.", ≈ -> "~", прочие — через xmlcharrefreplace (&#NNNN;).
"""

from pathlib import Path
import re
import sys
import io

SRC = Path("docs/price.yml")
DST = Path("docs/price_seo.yml")
ENC = "windows-1251"

# --- «safe» HTML-шаблон для Satu (вставляется ВВЕРХУ каждого описания) ---
TEMPLATE_HTML = """<p><strong><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0">Написать нам в WhatsApp</a></strong></p>
<p>Быстрые ответы в WhatsApp: <a href="tel:+77073270501"><strong>+7 (707) 327-05-01</strong></a> или почта <a href="mailto:info@complex-solutions.kz"><strong>info@complex-solutions.kz</strong></a></p>
<p><strong>Оплата</strong></p>
<ul>
  <li>Безналичный расчёт для юр. лиц</li>
  <li>Оплата по Kaspi для физ. лиц</li>
</ul>
<p><strong>Доставка</strong></p>
<ul>
  <li>Алматы (“квадрат”) — бесплатно</li>
  <li>По Казахстану: до 5 кг ~ 2500 тг., 3–7 рабочих дней</li>
  <li>Любой перевозчик или отправка через автовокзал «Сайран»</li>
</ul>"""

# --- IO helpers ---
def read_cp1251(path: Path) -> str:
    with io.open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def write_cp1251(path: Path, text: str) -> None:
    # Нормализация неподдерживаемых символов под cp1251
    text = (text
            .replace("\u20b8", "тг.")   # ₸ -> тг.
            .replace("\u2248", "~")     # ≈ -> ~
            .replace("\u00A0", " "))    # nbsp -> space
    path.parent.mkdir(parents=True, exist_ok=True)
    # На всякий случай: редкие символы превратятся в &#NNNN;
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
    return "<![CDATA[" + s.replace("]]>", "]]&gt;") + "]]>"

def inject_into_description_block(desc_inner: str) -> str:
    """Вставляет шаблон в начало блока описания, если его там ещё нет."""
    inner_clean = strip_cdata(desc_inner)
    if "Написать нам в WhatsApp" in inner_clean:
        return f"<description>{desc_inner}</description>"
    combined = TEMPLATE_HTML + ("\n\n" + inner_clean.strip() if inner_clean.strip() else "")
    return "<description>" + wrap_cdata(combined) + "</description>"

def add_description_if_missing(offer_block: str) -> str:
    """Если в оффере нет description — вставляем его перед </offer> с шаблоном."""
    if re.search(r"<description\b", offer_block, flags=re.I):
        return offer_block
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
    print(f"[seo] Готово: добавлен «safe»-блок в описания. Файл: {DST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
