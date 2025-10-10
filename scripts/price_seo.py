#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py — изменяет ТОЛЬКО содержимое <description> у каждого <offer>.
Не трогает порядок/набор остальных тегов.

Фиксы:
• «Совместимость»: извлекаем все модели из слэш-списков (учтены «похожие» кириллические буквы, как С↔C).
• «Тех. характеристики»: если несколько «Ключ: Значение» шли в одном <li> через " - ",
  разбиваем их на отдельные <li> и делаем ключи жирными.
"""

from __future__ import annotations
from pathlib import Path
import io, re, sys
from html import escape as html_escape

SRC = Path("docs/price.yml")
DST = Path("docs/price_seo.yml")
ENC = "windows-1251"

# ---- стили верхнего блока (оставляем без изменений) ----
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

# ---------- I/O ----------

def rtext(p: Path) -> str:
    with io.open(p, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def wtext(p: Path, s: str) -> None:
    safe = (s
        .replace("\u20B8", "тг.")   # ₸
        .replace("\u2248", "~")     # ≈
        .replace("\u00A0", " ")     # NBSP
        .replace("\u201C", '"').replace("\u201D", '"')
        .replace("\u201E", '"').replace("\u201F", '"')
        .replace("\u2013", "-").replace("\u2014", "—"))
    p.parent.mkdir(parents=True, exist_ok=True)
    with io.open(p, "w", encoding=ENC, newline="\n", errors="xmlcharrefreplace") as f:
        f.write(safe)

# ---------- RegExp (работаем ТОЛЬКО внутри <description>) ----------

OFFER_RX      = re.compile(r"<offer\b[^>]*>.*?</offer>",              re.I | re.S)
DESC_RX       = re.compile(r"<description\b[^>]*>(.*?)</description>", re.I | re.S)
NAME_RX       = re.compile(r"<name\b[^>]*>(.*?)</name>",              re.I | re.S)
VENDORCODE_RX = re.compile(r"<vendorCode\b[^>]*>(.*?)</vendorCode>",  re.I | re.S)
HR_RX         = re.compile(r"<hr\b[^>]*>", re.I)
HAS_HTML_TAGS = re.compile(r"<[a-zA-Z/!][^>]*>")

# ключ:значение (строка списка)
KV_LINE_RX = re.compile(r"^\s*(?:[-•–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$", re.S)
TECH_RX    = re.compile(r"(?i)техническ\w*\s+характеристик", re.U)
TRIM_TECH_TAIL_RX = re.compile(r"(?i)[\s,;:—–-]*техническ\w*\s*$")

def pick_char_heading(context: str) -> str:
    return "Технические характеристики" if TECH_RX.search(context or "") else "Характеристики"

def trim_trailing_tech_word(s: str) -> str:
    return TRIM_TECH_TAIL_RX.sub("", s or "").rstrip()

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\u00A0", " ")).strip()

# --- нормализация похожих букв (кириллица → латиница) для моделей ---
LOOKALIKE_MAP = str.maketrans({
    "А":"A","В":"B","С":"C","Е":"E","Н":"H","К":"K","М":"M","О":"O","Р":"P","Т":"T","Х":"X","У":"Y",
    "а":"a","в":"b","с":"c","е":"e","н":"h","к":"k","м":"m","о":"o","р":"p","т":"t","х":"x","у":"y",
})
def latinize(text: str) -> str:
    return (text or "").translate(LOOKALIKE_MAP)

# ---------- Оформление «характеристик» ----------

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

# разбить один <li> вида "… 2300 - Тип печати: Лазерная - Цвет: Black …" на много <li>
def split_inline_li(html: str) -> str:
    def repl(m: re.Match) -> str:
        before, body, after = m.group(1), m.group(2), m.group(3)
        # унифицируем разделители
        x = (body.replace("\u00A0"," ")
                 .replace(" — "," - ").replace(" – "," - ").replace(" − "," - "))
        # если нет хотя бы двух «ключ:значение» — оставляем как есть
        if x.count(":") < 2 or " - " not in x:
            return m.group(0)
        # делим на сегменты по " - "
        chunks = [c.strip() for c in x.split(" - ") if c.strip()]
        parts = []
        for ch in chunks:
            m_kv = KV_LINE_RX.match(ch)
            if m_kv:
                k = html_escape(m_kv.group(1).strip())
                v = html_escape(m_kv.group(2).strip())
                parts.append(f"<li><strong>{k}:</strong> {v}</li>")
            else:
                parts.append(f"<li>{html_escape(ch)}</li>")
        return before + "".join(parts) + after
    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", repl, html, flags=re.S | re.I)

def emphasize_first_kv_in_each_li(html: str) -> str:
    def repl(m: re.Match) -> str:
        b, body, a = m.group(1), m.group(2), m.group(3)
        if re.search(r"^\s*(?:[-–—]\s*)?<strong>[^:]{1,120}:</strong>", body): return m.group(0)
        m_kv = KV_LINE_RX.match(body.strip())
        if not m_kv: return m.group(0)
        key = html_escape(m_kv.group(1).strip())
        val = html_escape(m_kv.group(2).strip())
        return f"{b}<strong>{key}:</strong> {val}{a}"
    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", repl, html, flags=re.S | re.I)

def beautify_existing_html(html: str) -> str:
    # 1) перенос «Тех. характеристики … — пункты через <br>» в список
    def para_repl(m: re.Match) -> str:
        start, body, end = m.group(1), m.group(2), m.group(3)
        if not re.search(r"(?i)характеристик", body): return m.group(0)
        norm = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
        parts = re.split(r"(?i)характеристик[аи]:", norm, maxsplit=1)
        if len(parts) != 2: return m.group(0)
        head_raw, tail = parts[0], parts[1]
        head = trim_trailing_tech_word(head_raw.strip())
        lines = [ln for ln in tail.strip().split("\n") if ln.strip()]
        bullet_like = [ln for ln in lines if ln.lstrip().startswith(("-", "•", "–", "—"))]
        if not bullet_like: return m.group(0)
        label = pick_char_heading(body)
        ul = make_ul("\n".join(lines))
        return (start + (f"<p>{head}</p>\n" if head else "") +
                f"<p><strong>{label}:</strong></p>\n" + ul + end)
    html = re.sub(r"(<p[^>]*>)(.*?)(</p>)", para_repl, html, flags=re.S | re.I)

    # 2) у всех <a> добавим стили, если их нет
    html = re.sub(r"<a(?![^>]*\bstyle=)", f'<a style="color:{COLOR_LINK};text-decoration:none"', html, flags=re.I)

    # 3) разрезаем длинные <li> по " - " и делаем ключи жирными
    html = split_inline_li(html)
    html = emphasize_first_kv_in_each_li(html)

    # 4) обернём в Times-блок
    return (f'<div style="font-family: \'Times New Roman\', Times, serif; '
            f'font-size:15px; line-height:1.55;">{html}</div>')

def beautify_plain_text(text: str) -> str:
    t = text.replace("\r\n","\n").replace("\r","\n").strip()
    if not t: return ""
    blocks = [b.strip("\n") for b in re.split(r"\n{2,}", t)]
    out = []
    for blk in blocks:
        if re.search(r"(?i)\bхарактеристик", blk):
            parts = re.split(r"(?i)характеристик[аи]:", blk, maxsplit=1)
            head_raw = parts[0].strip()
            tail     = parts[1].strip() if len(parts)==2 else ""
            label    = pick_char_heading(blk)
            head     = trim_trailing_tech_word(head_raw)
            if head: out.append(f"<p>{html_escape(head)}</p>")
            if tail: out.append(f"<p><strong>{label}:</strong></p>"); out.append(make_ul(tail))
            continue
        lines = [html_escape(x) for x in blk.split("\n")]
        out.append(f"<p>{'<br>'.join(lines)}</p>")
    return (f'<div style="font-family: \'Times New Roman\', Times, serif; '
            f'font-size:15px; line-height:1.55;">' + "\n".join(out) + "</div>")

def beautify_original_description(inner: str) -> str:
    c = inner.strip()
    if not c: return ""
    if HAS_HTML_TAGS.search(c): return beautify_existing_html(c)
    return beautify_plain_text(c)

# ---------- Совместимость (включаем ВСЕ устройства из слэш-списков) ----------

# Находим бренд + «семейство» сразу из текста name+desc, сохраняя оригинальный регистр/дефисы
BRAND_RE = re.compile(r"(?i)\b(HP|Hewlett[ -]?Packard|Canon|Epson|Brother|Kyocera|Samsung|Ricoh|Xerox|Sharp|Lexmark|OKI|Panasonic|Konica(?:-| )?Minolta|Pantum)\b")
FAMILY_RE = re.compile(r"(?i)\b(Color|Laser|LaserJet|LJ|MFP|DeskJet|OfficeJet|PageWide|DesignJet|imageRUNNER|i-SENSYS|LBP|PIXMA|Stylus|WorkForce|EcoTank|SureColor|HL|DCP|MFC|FS|TASKalfa|ECOSYS|CLX|ML|SL|SP|Aficio|VersaLink|WorkCentre|Phaser|bizhub)\b")

# Расходники (не устройства) — фильтруем
CARTRIDGE_RE = re.compile(r"(?i)\b((?:CE|CF|CB|CC|Q)\d{2,4}[A-Z]|CRG[- ]?\d{2,4}|(?:TN|TK|TKC|TNP|TNR|DR|DK|DV|CN|AR|MX|JL)[- ]?\d{2,5}[A-Z]?)\b")

MODEL_TOKEN_RE = re.compile(r"\b([A-Z]{1,4}-?[A-Z]?\d{2,6}[A-Z]?(?:-[A-Z0-9]{1,4})?)\b", re.I)

def extract_brand_family(text: str) -> tuple[str,str]:
    t = normalize_space(latinize(text))
    mb = BRAND_RE.search(t)
    brand = mb.group(0) if mb else ""
    fam = ""
    if brand:
        after = t[mb.end():]
        mf = FAMILY_RE.search(after)
        fam = mf.group(0) if mf else ""
    return brand, fam

def expand_slashed(s: str) -> list[str]:
    # Делим по '/', восстанавливаем общий префикс первой части до цифры (например, "Pantum CM")
    toks = [t for t in s.split("/") if t]
    if len(toks) == 1:
        return [s]
    first = toks[0]
    m = re.match(r"^(.*?)[A-Za-z]*\d", first)
    prefix = normalize_space(m.group(1)) if m else ""
    out = []
    for tk in toks:
        tk = normalize_space(tk)
        tk = latinize(tk)
        if prefix and not tk.lower().startswith(prefix.lower()):
            out.append((prefix + tk).strip())
        else:
            out.append(tk)
    return out

def extract_device_models(name_text: str, desc_plain: str) -> list[str]:
    t = normalize_space(latinize(name_text + " " + desc_plain))
    # Разбиваем по запятым/точкам с запятой/«и»/вертикальным чертам
    parts = re.split(r"[,;|]|\s+\bи\b\s+", t, flags=re.I)
    cand = []
    for p in parts:
        if "/" in p:
            cand.extend(expand_slashed(p))
        else:
            cand.append(p)
    models = []
    for chunk in cand:
        for m in MODEL_TOKEN_RE.finditer(chunk):
            token = m.group(1)
            if not CARTRIDGE_RE.search(token):  # выкидываем TN-210K и т.п.
                models.append(token.upper())
    # Приклеим бренд/семейство, если токены «голые»
    brand, fam = extract_brand_family(name_text + " " + desc_plain)
    res = []
    for m in models:
        if BRAND_RE.match(m):  # уже содержит бренд
            res.append(m)
        else:
            prefix = " ".join(x for x in (brand, fam) if x)
            res.append((prefix + " " + m).strip() if prefix else m)
    # Дедуп/сортировка
    seen = set(); out = []
    for x in res:
        k = normalize_space(x)
        if k and k not in seen:
            seen.add(k); out.append(k)
    out.sort(key=lambda s: (s.split()[0].lower(), s.lower()))
    return out

def build_compat_block(name_text: str, tail_html: str) -> str:
    # Если уже есть блок «Совместимость» c >=2 пунктами — не трогаем
    m = re.search(r"(?is)<p[^>]*>\s*<strong>\s*совместим\w*:\s*</strong>\s*</p>\s*<ul>(.*?)</ul>", tail_html)
    if m and len(re.findall(r"<li\b", m.group(1), flags=re.I)) >= 2:
        return ""
    plain = normalize_space(re.sub(r"<br\s*/?>", " ", re.sub(r"<[^>]+>", " ", tail_html)))
    models = extract_device_models(name_text, plain)
    if len(models) < 2:
        return ""
    items = "\n".join(f"<li>{html_escape(x)}</li>" for x in models)
    return "<p><strong>Совместимость:</strong></p>\n<ul>\n" + items + "\n</ul>"

# ---------- Сборка <description> ----------

def has_our_block(desc_html: str) -> bool:
    return "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_html

def prettify_original(inner: str) -> str:
    c = inner.strip()
    if not c: return ""
    if HAS_HTML_TAGS.search(c): return beautify_existing_html(c)
    return beautify_plain_text(c)

def rebuild_with_existing(desc_inner: str, name_text: str) -> str:
    parts = HR_RX.split(desc_inner, maxsplit=1)
    if len(parts) != 2:
        # Только подчистим длинные <li> внутри уже существующего блока
        fixed = split_inline_li(desc_inner)
        fixed = emphasize_first_kv_in_each_li(fixed)
        return "<description>" + fixed + "</description>"
    head, tail = parts[0], parts[1]
    # улучшаем «низ» — разбиваем длинные <li>, жирним ключи
    tail_fixed = split_inline_li(tail)
    tail_fixed = emphasize_first_kv_in_each_li(tail_fixed)
    # добавляем совместимость (если нет)
    compat = build_compat_block(name_text, tail_fixed)
    if compat:
        tail_fixed = re.sub(r"(?is)</div>\s*$", "\n" + compat + "\n</div>", tail_fixed, count=1)
    return "<description>" + head + "<hr>\n\n" + tail_fixed + "</description>"

def build_new_desc(existing_inner: str, name_text: str) -> str:
    pretty = prettify_original(existing_inner)
    compat = build_compat_block(name_text, pretty) if pretty else ""
    if compat:
        pretty = re.sub(r"(?is)</div>\s*$", "\n" + compat + "\n</div>", pretty, count=1)
    return TEMPLATE_HTML + ("\n\n<hr>\n\n" + pretty if pretty else "")

def inject_description(desc_inner: str, name_text: str) -> str:
    if has_our_block(desc_inner):
        return rebuild_with_existing(desc_inner, name_text)
    return "<description>" + build_new_desc(desc_inner, name_text) + "</description>"

def add_desc_if_missing(offer_block: str) -> str:
    if re.search(r"<description\b", offer_block, flags=re.I): return offer_block
    m = re.search(r"\n([ \t]+)<", offer_block); indent = m.group(1) if m else "  "
    insertion = f"\n{indent}<description>{TEMPLATE_HTML}</description>"
    tail = (insertion + "\n" + indent[:-2] + "</offer>") if len(indent) >= 2 else (insertion + "\n</offer>")
    return offer_block.replace("</offer>", tail)

# ---------- Основной конвейер (прочие теги не трогаем) ----------

def process_offer(offer_block: str) -> str:
    # контекст для бренда/семейства берём из name
    name_text = ""
    mname = NAME_RX.search(offer_block)
    if mname and mname.group(1):
        name_text = normalize_space(re.sub(r"<[^>]+>", " ", mname.group(1)))

    def _desc_repl(m: re.Match) -> str:
        return inject_description(m.group(1), name_text)

    block = DESC_RX.sub(_desc_repl, offer_block)
    if not re.search(r"<description\b", block, flags=re.I):
        block = add_desc_if_missing(block)
    return block

def process_text(xml_text: str) -> str:
    return OFFER_RX.sub(lambda m: process_offer(m.group(0)), xml_text)

def main() -> int:
    if not SRC.exists():
        print(f"[seo] Исходный файл не найден: {SRC}", file=sys.stderr); return 1
    original  = rtext(SRC)
    processed = process_text(original)
    wtext(DST, processed)
    print(f"[seo] OK: {DST} — изменены только <description>")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
