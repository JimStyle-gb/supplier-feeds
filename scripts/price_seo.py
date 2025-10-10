#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
-----------------------------------
Создаёт docs/price_seo.yml из docs/price.yml и добавляет в НАЧАЛО каждого <description>
ВАШ ВЕРХНИЙ БЛОК (Cambria) → <hr> → красиво оформленное родное описание (Times New Roman).

Особенности:
  • «Характеристики» сводятся к списку-столбцу.
  • Ключи до двоеточия — жирные: <strong>Гарантия:</strong> 1 год.
  • Если внутри одного <li> склеены несколько пар (' - Ключ: Значение - ...'),
    они автоматически «разрезаются» на несколько СОСЕДНИХ <li>.

Без CDATA. Повторной вставки блока нет. Кодировка выхода: windows-1251 (безопасная нормализация).
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

# ── ВЕРХНИЙ БЛОК (Cambria) — НЕ МЕНЯЕМ ─────────────────────────────
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

# Линия «ключ: значение»
KV_LINE_RX = re.compile(r"^\s*(?:[-•–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$", re.S)

# ─────────────────────────── Хелперы характеристик ───────────────────────────

def kv_to_li(text_line: str) -> str:
    """'- Гарантия: 1 год' -> '<li><strong>Гарантия:</strong> 1 год</li>'"""
    s = text_line.strip()
    if s.startswith(("- ", "• ", "– ", "— ")):
        s = s[2:].strip()
    m = KV_LINE_RX.match(s)
    if m:
        key = html_escape(m.group(1).strip())
        val = html_escape(m.group(2).strip())
        return f"<li><strong>{key}:</strong> {val}</li>"
    return f"<li>{html_escape(s)}</li>"

def make_ul_from_bullets(text_block: str) -> str:
    """Преобразует набор строк c '-'/'•' в <ul>…</ul>."""
    lines = [ln for ln in text_block.replace("\r\n","\n").replace("\r","\n").split("\n") if ln.strip()]
    items = [kv_to_li(ln) for ln in lines if ln.strip()]
    return "<ul>\n" + "\n".join(items) + "\n</ul>"

# ★ FIXED: разбиваем один LI с инлайновыми парами " - Ключ: Значение" на несколько СОСЕДНИХ <li>
def explode_inline_kv_pairs_in_li(html: str) -> str:
    """
    Находит <li>..</li>, где внутри несколько пар вида " - Ключ: Значение",
    и ЗАМЕНЯЕТ такой <li> на несколько СОСЕДНИХ <li>. Ключ жирный.
    Сохраняем существующую разметку (например, <strong>Ресурс:</strong> 34000).
    """
    def li_repl(m: re.Match) -> str:
        body = m.group(2)

        # Быстрый выход: нет потенциальных разделителей — не трогаем
        if not any(sep in body for sep in (" - ", " — ", " • ", " · ")):
            return m.group(0)

        # Нормализуем разделители к ' - ' для анализа
        norm = body.replace(" — ", " - ").replace(" • ", " - ").replace(" · ", " - ")

        # Если всего одно двоеточие — оставляем как есть (скорее одна пара)
        if norm.count(":") <= 1:
            return m.group(0)

        # Сплит по " - " только если фрагмент ПРАВДА похож на "Ключ: ..."
        parts = []
        buf = []
        tokens = norm.split(" - ")
        for i, tk in enumerate(tokens):
            if i == 0:
                buf.append(tk)
                continue
            if re.match(r"\s*[^:<>{}\n]{1,120}:\s*.", tk, flags=re.S):
                parts.append(" - ".join(buf))
                buf = [tk]
            else:
                buf.append(tk)
        parts.append(" - ".join(buf))

        # КАЖДЫЙ фрагмент -> ОТДЕЛЬНЫЙ <li> (без внешней обёртки!)
        lis = []
        for frag in parts:
            frag = frag.strip()
            # если во фрагменте уже есть HTML (например, <strong>Ключ:</strong> 34000) — не экранируем
            if "<" in frag or ">" in frag:
                lis.append(f"<li>{frag}</li>")
                continue
            # иначе пытаемся распознать "Ключ: Значение"
            m_kv = re.match(r"([^:<>{}\n]{1,120}?)\s*:\s*(.+)$", frag, flags=re.S)
            if m_kv:
                key = html_escape(m_kv.group(1).strip())
                val = html_escape(m_kv.group(2).strip())
                lis.append(f"<li><strong>{key}:</strong> {val}</li>")
            elif frag:
                lis.append(f"<li>{html_escape(frag)}</li>")

        # ВАЖНО: возвращаем ТОЛЬКО набор <li>…</li> БЕЗ исходного before/after <li>…</li>
        return "".join(lis)

    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", li_repl, html, flags=re.S | re.I)

# Делает жирным 'Ключ:' в <li> + вызывает сплит одиночных LI
def emphasize_kv_in_li(html: str) -> str:
    """
    Делает жирным 'Ключ:' в <li>. Сначала раскалывает одиночные <li> с
    инлайновыми парами ' - Ключ: Значение' на несколько <li>.
    """
    html2 = explode_inline_kv_pairs_in_li(html)

    def repl(m: re.Match) -> str:
        before, body, after = m.group(1), m.group(2), m.group(3)
        if re.search(r"^\s*(?:[-–—]\s*)?<strong>[^:]{1,120}:</strong>", body, flags=re.I):
            return m.group(0)
        kv = re.sub(
            r"^\s*(?:[-•–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$",
            lambda k: f"<strong>{k.group(1).strip()}:</strong> {k.group(2).strip()}",
            body, count=1, flags=re.S
        )
        return f"{before}{kv}{after}"
    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", repl, html2, flags=re.S | re.I)

# ─────────────────────────── Преобразования HTML-описаний ───────────────────────────

def transform_characteristics_paragraphs(html: str) -> str:
    """
    Ищем абзацы с «Характеристики:» и пулями внутри (через переносы или <br>),
    превращаем их в: <p><strong>Характеристики:</strong></p><ul>…</ul>.
    """
    def para_repl(m: re.Match) -> str:
        start, body, end = m.group(1), m.group(2), m.group(3)
        if not re.search(r"\bХарактеристик[аи]:", body, flags=re.I):
            return m.group(0)

        norm = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
        parts = re.split(r"(?i)Характеристик[аи]:", norm, maxsplit=1)
        if len(parts) != 2:
            return m.group(0)
        head, tail = parts[0], parts[1]

        bullet_lines = [ln for ln in tail.strip().split("\n") if ln.strip()]
        bullet_like = [ln for ln in bullet_lines if ln.lstrip().startswith(("-", "•", "–", "—"))]
        if len(bullet_like) < 1:
            return m.group(0)

        ul = make_ul_from_bullets("\n".join(bullet_lines))
        head_html = head.strip()
        out = ""
        if head_html:
            out += f"<p>{head_html}</p>\n"
        out += "<p><strong>Характеристики:</strong></p>\n" + ul
        return start + out + end

    PARA_RX = re.compile(r"(<p[^>]*>)(.*?)(</p>)", re.S | re.I)
    return PARA_RX.sub(para_repl, html)

def beautify_existing_html(html: str) -> str:
    """
    Уже-HTML-описание:
      • «Характеристики» → список,
      • ссылки без style → красим,
      • <li> — жирный ключ и сплит одиночных LI на много,
      • контейнер Times New Roman.
    """
    step1 = transform_characteristics_paragraphs(html)
    step2 = A_NO_STYLE_RX.sub(f'<a style="color:{COLOR_LINK};text-decoration:none"', step1)
    step3 = emphasize_kv_in_li(step2)
    wrapper = (
        f'<div style="font-family: \'Times New Roman\', Times, serif; '
        f'font-size:15px; line-height:1.55;">{step3}</div>'
    )
    return wrapper

def beautify_plain_text(text: str) -> str:
    """
    Plain-text → аккуратный HTML (с учётом «Характеристики» и KV).
    """
    t = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not t:
        return ""
    blocks = [b.strip("\n") for b in re.split(r"\n{2,}", t)]
    out: list[str] = []

    def is_list_block(block: str) -> bool:
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        return bool(lines) and all(ln.startswith(("- ", "• ", "– ", "— ")) for ln in lines)

    for blk in blocks:
        if re.search(r"(?i)\bХарактеристик[аи]:", blk):
            parts = re.split(r"(?i)Характеристик[аи]:", blk, maxsplit=1)
            head = parts[0].strip()
            tail = parts[1].strip() if len(parts) == 2 else ""
            if head:
                out.append(f"<p>{html_escape(head)}</p>")
            if tail:
                out.append("<p><strong>Характеристики:</strong></p>")
                out.append(make_ul_from_bullets(tail))
            continue

        if is_list_block(blk):
            lis = []
            for ln in blk.split("\n"):
                if not ln.strip():
                    continue
                lis.append(kv_to_li(ln))
            out.append("<ul>\n" + "\n".join(lis) + "\n</ul>")
        else:
            lines = [html_escape(x) for x in blk.split("\n")]
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
    Если блок уже есть: оформляем часть после первого <hr> (если он есть).
    Если <hr> нет — возвращаем как есть, чтобы не ломать старый контент.
    """
    parts = HR_RX.split(desc_inner, maxsplit=1)
    if len(parts) == 2:
        head, tail = parts[0], parts[1]
        pretty_tail = beautify_original_description(tail)
        if pretty_tail:
            return "<description>" + head + "<hr>\n\n" + pretty_tail + "</description>"
        return "<description>" + desc_inner + "</description>"
    else:
        return "<description>" + desc_inner + "</description>"

def build_new_description(existing_inner: str) -> str:
    pretty = beautify_original_description(existing_inner)
    if pretty:
        return TEMPLATE_HTML + "\n\n<hr>\n\n" + pretty
    return TEMPLATE_HTML

def inject_into_description_block(desc_inner: str) -> str:
    if has_our_block(desc_inner):
        return rebuild_with_existing_block(desc_inner)
    return "<description>" + build_new_description(desc_inner) + "</description>"

def add_description_if_missing(offer_block: str) -> str:
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
    print(f"[seo] Готово: характеристики в столбик, ключи жирные; блок/шрифты — как задано. Файл: {DST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
