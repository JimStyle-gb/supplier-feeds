#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
-----------------------------------
Создаёт docs/price_seo.yml на основе docs/price.yml и добавляет в НАЧАЛО
каждого <description> ваш блок (2 строки по центру), затем <hr>, затем — родное описание.

Особенности:
  • Без XML-парсера (устойчив к «грязным» текстам с & и <).
  • Повторной вставки нет (узнаём по фразе "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!").
  • Кодировка вывода: windows-1251 с безопасной нормализацией.
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

# Ваш блок: первые две строки — строго по центру через <center>.
TEMPLATE_HTML: str = """<center><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0"><strong>НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!</strong></a></center>

<center>Просьба отправлять запросы в <a href="tel:+77073270501"><strong>WhatsApp: +7 (707) 327-05-01</strong></a> либо на почту: <a href="mailto:info@complex-solutions.kz"><strong>info@complex-solutions.kz</strong></a></center>

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
        .replace("\u00A0", " ")     # NBSP → обычный пробел
        .replace("\u201C", '"').replace("\u201D", '"')  # “ ” → "
        .replace("\u201E", '"').replace("\u201F", '"')  # „ ‟ → "
        .replace("\u2013", "-").replace("\u2014", "—")  # – → -, — оставляем
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with io.open(path, "w", encoding=ENC, newline="\n", errors="xmlcharrefreplace") as f:
        f.write(safe)

# ─────────────────────────── Регэкспы ───────────────────────────

DESC_RX   = re.compile(r"<description\b[^>]*>(.*?)</description>", re.I | re.S)
OFFER_RX  = re.compile(r"<offer\b[^>]*>.*?</offer>",                re.I | re.S)
CD_START  = "<![CDATA["
CD_END    = "]]>"

# ─────────────────────────── Вспомогательные ───────────────────────────

def strip_cdata(s: str) -> str:
    s = s.strip()
    if s.startswith(CD_START) and s.endswith(CD_END):
        return s[len(CD_START):-len(CD_END)]
    return s

def wrap_cdata(s: str) -> str:
    # Запрещаем "]]>" внутри CDATA
    return CD_START + s.replace("]]>", "]]&gt;") + CD_END

def inject_into_description_block(desc_inner: str) -> str:
    """
    Вставляет ваш блок в начало описания (если его ещё нет).
    После блока добавляет <hr>, затем — исходный текст.
    """
    inner_clean = strip_cdata(desc_inner)

    # Защита от дублирования
    if "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in inner_clean:
        return f"<description>{desc_inner}</description>"

    if inner_clean.strip():
        combined = TEMPLATE_HTML + "\n\n<hr>\n\n" + inner_clean.strip()
    else:
        combined = TEMPLATE_HTML

    return "<description>" + wrap_cdata(combined) + "</description>"

def add_description_if_missing(offer_block: str) -> str:
    """
    Если описания нет — создаём <description> с вашим блоком (без <hr>).
    """
    if re.search(r"<description\b", offer_block, flags=re.I):
        return offer_block

    # Определим отступ по ближайшему тегу внутри оффера (чтобы было красиво)
    m = re.search(r"\n([ \t]+)<", offer_block)
    indent = m.group(1) if m else "  "

    insertion = f"\n{indent}<description>{wrap_cdata(TEMPLATE_HTML)}</description>"
    tail = (insertion + "\n" + indent[:-2] + "</offer>") if len(indent) >= 2 else (insertion + "\n</offer>")
    return offer_block.replace("</offer>", tail)

# ─────────────────────────── Основная логика ───────────────────────────

def process_whole_text(xml_text: str) -> str:
    # 1) сначала обновляем все существующие <description>…</description>
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

    print(f"[seo] Готово: блок добавлен сверху, <hr> перед родным описанием. Файл: {DST}")
    return 0

# ─────────────────────────── Точка входа ───────────────────────────

if __name__ == "__main__":
    raise SystemExit(main())
