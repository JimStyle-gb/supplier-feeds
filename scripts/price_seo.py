#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
-----------------------------------
Создаёт docs/price_seo.yml из docs/price.yml.
В <description> добавляет ваш верхний блок (Cambria) → <hr> → красиво оформленный хвост (Times).
«Характеристики» всегда идут столбцом, ключи до двоеточия — жирные.

НОВОЕ:
  • Финальный глобальный проход по ВСЕМ <li>: даже если внутри одного <li> склеены
    ' - Ключ: Значение - ...', он разрежет на несколько СОСЕДНИХ <li> и ожирнит ключи.
"""

from __future__ import annotations
from pathlib import Path
import io, re, sys
from html import escape as html_escape

SRC = Path("docs/price.yml")
DST = Path("docs/price_seo.yml")
ENC = "windows-1251"

COLOR_LINK  = "#0b3d91"
COLOR_WHITE = "#ffffff"
COLOR_BTN   = "#27ae60"
COLOR_KASPI = "#8b0000"

TEMPLATE_HTML = f"""<div style="font-family: Cambria, 'Times New Roman', serif;">
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

# ============ I/O ============

def rtext(path: Path) -> str:
    with io.open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def wtext(path: Path, text: str) -> None:
    safe = (text
            .replace("\u20B8", "тг.")
            .replace("\u2248", "~")
            .replace("\u00A0", " ")
            .replace("\u201C", '"').replace("\u201D", '"')
            .replace("\u201E", '"').replace("\u201F", '"')
            .replace("\u2013", "-").replace("\u2014", "—"))
    path.parent.mkdir(parents=True, exist_ok=True)
    with io.open(path, "w", encoding=ENC, newline="\n", errors="xmlcharrefreplace") as f:
        f.write(safe)

# ============ Регэкспы ============

DESC_RX       = re.compile(r"<description\b[^>]*>(.*?)</description>", re.I | re.S)
OFFER_RX      = re.compile(r"<offer\b[^>]*>.*?</offer>",              re.I | re.S)
HAS_HTML_TAGS = re.compile(r"<[a-zA-Z/!][^>]*>")
A_NO_STYLE_RX = re.compile(r"<a(?![^>]*\bstyle=)", re.I)
HR_RX         = re.compile(r"<hr\b[^>]*>", re.I)

# Ключ: Значение (без HTML-скобок в ключе)
KV_LINE_RX = re.compile(r"^\s*(?:[-•–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$", re.S)

# ============ Характеристики ============

def kv_to_li(line: str) -> str:
    s = line.strip()
    if s.startswith(("- ","• ","– ","— ")): s = s[2:].strip()
    m = KV_LINE_RX.match(s)
    if m:
        key = html_escape(m.group(1).strip())
        val = html_escape(m.group(2).strip())
        return f"<li><strong>{key}:</strong> {val}</li>"
    return f"<li>{html_escape(s)}</li>"

def make_ul(text_block: str) -> str:
    lines = [ln for ln in text_block.replace("\r\n","\n").replace("\r","\n").split("\n") if ln.strip()]
    return "<ul>\n" + "\n".join(kv_to_li(ln) for ln in lines) + "\n</ul>"

def transform_characteristics_paragraphs(html: str) -> str:
    # <p>...Характеристики: ...\n- ключ: значение ...</p> -> <p><strong>Характеристики:</strong></p><ul>...</ul>
    def para_repl(m: re.Match) -> str:
        start, body, end = m.group(1), m.group(2), m.group(3)
        if "Характеристик" not in body and "Характеристики" not in body: return m.group(0)
        norm = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
        parts = re.split(r"(?i)Характеристик[аи]:", norm, maxsplit=1)
        if len(parts) != 2: return m.group(0)
        head, tail = parts[0], parts[1]
        bullet_lines = [ln for ln in tail.strip().split("\n") if ln.strip()]
        bullet_like = [ln for ln in bullet_lines if ln.lstrip().startswith(("-", "•", "–", "—"))]
        if not bullet_like: return m.group(0)
        ul = make_ul("\n".join(bullet_lines))
        out = (f"<p>{head.strip()}</p>\n" if head.strip() else "") + "<p><strong>Характеристики:</strong></p>\n" + ul
        return start + out + end

    return re.sub(r"(<p[^>]*>)(.*?)(</p>)", para_repl, html, flags=re.S | re.I)

def explode_inline_li(html: str) -> str:
    """
    Мягкое разрезание: один <li> с ' - Ключ: Значение - ...' -> несколько соседних <li>.
    """
    def li_repl(m: re.Match) -> str:
        body = m.group(2)
        if not re.search(r"[ \u00A0][\-–—•·][ \u00A0]", body): return m.group(0)

        norm = (body.replace("\u00A0"," ")
                    .replace(" — "," - ").replace(" – "," - ")
                    .replace(" • "," - ").replace(" · "," - "))
        if norm.count(":") <= 1: return m.group(0)

        parts, buf = [], []
        for i, tk in enumerate(norm.split(" - ")):
            if i == 0:
                buf.append(tk); continue
            if re.match(r"\s*[^:<>{}\n]{1,120}:\s*.", tk, flags=re.S):
                parts.append(" - ".join(buf)); buf = [tk]
            else:
                buf.append(tk)
        parts.append(" - ".join(buf))

        lis = []
        for frag in parts:
            frag = frag.strip()
            if "<" in frag or ">" in frag:
                lis.append(f"<li>{frag}</li>")
                continue
            m_kv = re.match(r"([^:<>{}\n]{1,120}?)\s*:\s*(.+)$", frag, flags=re.S)
            if m_kv:
                key = html_escape(m_kv.group(1).strip())
                val = html_escape(m_kv.group(2).strip())
                lis.append(f"<li><strong>{key}:</strong> {val}</li>")
            elif frag:
                lis.append(f"<li>{html_escape(frag)}</li>")
        return "".join(lis)

    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", li_repl, html, flags=re.S | re.I)

def force_inline_li_breaks(html: str) -> str:
    """
    Жёсткий дожим внутри <li>: ' - Ключ: Значение' -> '</li><li><strong>Ключ:</strong> Значение'
    """
    def inner_fix(m: re.Match) -> str:
        before, body, after = m.group(1), m.group(2), m.group(3)
        x = (body.replace("\u00A0"," ")
                 .replace(" — "," - ").replace(" – "," - ")
                 .replace(" • "," - ").replace(" · "," - "))
        def sub_kv(mm: re.Match) -> str:
            key = html_escape(mm.group(1).strip())
            val = html_escape(mm.group(2).strip())
            return f"</li><li><strong>{key}:</strong> {val}"
        x2 = re.sub(r"\s-\s([^:<>{}\n]{1,120}?)\s*:\s*(.+?)(?=\s-\s[^:<>{}\n]{1,120}:\s|$)", sub_kv, x)
        return before + x2 + after
    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", inner_fix, html, flags=re.S | re.I)

def emphasize_kv_in_li(html: str) -> str:
    html2 = explode_inline_li(html)
    html3 = force_inline_li_breaks(html2)
    def repl(m: re.Match) -> str:
        before, body, after = m.group(1), m.group(2), m.group(3)
        if re.search(r"^\s*(?:[-–—]\s*)?<strong>[^:]{1,120}:</strong>", body, flags=re.I): return m.group(0)
        kv = re.sub(r"^\s*(?:[-•–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$",
                    lambda k: f"<strong>{k.group(1).strip()}:</strong> {k.group(2).strip()}",
                    body, count=1, flags=re.S)
        return f"{before}{kv}{after}"
    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", repl, html3, flags=re.S | re.I)

def beautify_existing_html(html: str) -> str:
    step1 = transform_characteristics_paragraphs(html)
    step2 = A_NO_STYLE_RX.sub(f'<a style="color:{COLOR_LINK};text-decoration:none"', step1)
    step3 = emphasize_kv_in_li(step2)
    return (f'<div style="font-family: \'Times New Roman\', Times, serif; '
            f'font-size:15px; line-height:1.55;">{step3}</div>')

def beautify_plain_text(text: str) -> str:
    t = text.replace("\r\n","\n").replace("\r","\n").strip()
    if not t: return ""
    blocks = [b.strip("\n") for b in re.split(r"\n{2,}", t)]
    out = []
    def is_list_block(block: str) -> bool:
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        return bool(lines) and all(ln.startswith(("- ","• ","– ","— ")) for ln in lines)
    for blk in blocks:
        if re.search(r"(?i)\bХарактеристик[аи]:", blk):
            parts = re.split(r"(?i)Характеристик[аи]:", blk, maxsplit=1)
            head = parts[0].strip()
            tail = parts[1].strip() if len(parts)==2 else ""
            if head: out.append(f"<p>{html_escape(head)}</p>")
            if tail:
                out.append("<p><strong>Характеристики:</strong></p>")
                out.append(make_ul(tail))
            continue
        if is_list_block(blk):
            lis = [kv_to_li(ln) for ln in blk.split("\n") if ln.strip()]
            out.append("<ul>\n" + "\n".join(lis) + "\n</ul>")
        else:
            lines = [html_escape(x) for x in blk.split("\n")]
            out.append(f"<p>{'<br>'.join(lines)}</p>")
    return (f'<div style="font-family: \'Times New Roman\', Times, serif; '
            f'font-size:15px; line-height:1.55;">' + "\n".join(out) + "</div>")

def beautify_original_description(inner: str) -> str:
    content = inner.strip()
    if not content: return ""
    if HAS_HTML_TAGS.search(content): return beautify_existing_html(content)
    return beautify_plain_text(content)

def has_our_block(desc_html: str) -> bool:
    return "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_html

def rebuild_with_existing_block(desc_inner: str) -> str:
    parts = HR_RX.split(desc_inner, maxsplit=1)
    if len(parts) != 2:
        return "<description>" + desc_inner + "</description>"
    head, tail = parts[0], parts[1]
    # если уже есть Times-контейнер — обрабатываем его содержимое без повторной обёртки
    m = re.search(r"(<div[^>]*font-family:\s*['\"]?Times New Roman['\"]?[^>]*>)(.*?)(</div>)",
                  tail, flags=re.I | re.S)
    if m:
        start_div, inner, end_div = m.group(1), m.group(2), m.group(3)
        processed = emphasize_kv_in_li(A_NO_STYLE_RX.sub(f'<a style="color:{COLOR_LINK};text-decoration:none"', transform_characteristics_paragraphs(inner)))
        new_tail = tail[:m.start()] + start_div + processed + end_div + tail[m.end():]
        return "<description>" + head + "<hr>\n\n" + new_tail + "</description>"
    pretty_tail = beautify_original_description(tail)
    return "<description>" + head + "<hr>\n\n" + pretty_tail + "</description>"

def build_new_description(existing_inner: str) -> str:
    pretty = beautify_original_description(existing_inner)
    return TEMPLATE_HTML + ("\n\n<hr>\n\n" + pretty if pretty else "")

def inject_into_description_block(desc_inner: str) -> str:
    if has_our_block(desc_inner): return rebuild_with_existing_block(desc_inner)
    return "<description>" + build_new_description(desc_inner) + "</description>"

def add_description_if_missing(offer_block: str) -> str:
    if re.search(r"<description\b", offer_block, flags=re.I): return offer_block
    m = re.search(r"\n([ \t]+)<", offer_block); indent = m.group(1) if m else "  "
    insertion = f"\n{indent}<description>{TEMPLATE_HTML}</description>"
    tail = (insertion + "\n" + indent[:-2] + "</offer>") if len(indent) >= 2 else (insertion + "\n</offer>")
    return offer_block.replace("</offer>", tail)

# ============ Финальный глобальный полиш по ВСЕМ <li> ============

SPLIT_HINT_RX = re.compile(r"\s[-–—•·]\s(?=[^:<>{}\n]{1,120}\s*:)")
KV_ANY_RX     = re.compile(r"^\s*([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$", re.S)

def global_li_polish(xml_text: str) -> str:
    """
    Находит ЛЮБОЙ <li>…</li>, где внутри несколько ' - Ключ: Значение',
    и ЗАМЕНЯЕТ на несколько соседних <li> с жирным ключом.
    """
    def li_global_repl(m: re.Match) -> str:
        body = m.group(2)
        # быстрый выход — нет подсказок на сплит
        if not SPLIT_HINT_RX.search(body): return m.group(0)

        # режем по ' - ' (и аналогам), но только там, где дальше ключ с двоеточием
        parts = SPLIT_HINT_RX.split(body)
        if len(parts) <= 1: return m.group(0)

        out_lis = []
        for frag in parts:
            frag = frag.strip()
            # если видим уже HTML (например, <strong>Ключ:</strong>) — оставляем как есть
            if "<" in frag and ">" in frag:
                out_lis.append(f"<li>{frag}</li>")
                continue
            mm = KV_ANY_RX.match(frag)
            if mm:
                key = html_escape(mm.group(1).strip())
                val = html_escape(mm.group(2).strip())
                out_lis.append(f"<li><strong>{key}:</strong> {val}</li>")
            elif frag:
                out_lis.append(f"<li>{html_escape(frag)}</li>")
        return "".join(out_lis)

    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", li_global_repl, xml_text, flags=re.S | re.I)

# ============ Основной конвейер ============

def process_text(xml_text: str) -> str:
    # 1) вставляем/обновляем description
    updated = DESC_RX.sub(lambda m: inject_into_description_block(m.group(1)), xml_text)
    # 2) добавляем description если его не было
    updated = OFFER_RX.sub(lambda m: (m.group(0) if re.search(r"<description\b", m.group(0), re.I)
                                      else add_description_if_missing(m.group(0))), updated)
    # 3) ГЛОБАЛЬНЫЙ полиш по всем <li> во всём документе
    polished = global_li_polish(updated)
    return polished

def main() -> int:
    if not SRC.exists():
        print(f"[seo] Исходный файл не найден: {SRC}", file=sys.stderr); return 1
    original  = rtext(SRC)
    processed = process_text(original)
    wtext(DST, processed)
    print(f"[seo] OK: {DST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
