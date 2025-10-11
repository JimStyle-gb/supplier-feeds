#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py — добавляет блок «Совместимость»
----------------------------------------------
Вход:  docs/price.yml
Выход: docs/price_seo.yml

Меняет только <description>:
• Сохраняет твой верхний блок (Cambria + WhatsApp-кнопка, Оплата/Доставка) и <hr>.
• «Технические характеристики» не трогает (если слитно — аккуратно раскладывает в <ul>).
• ДОБАВЛЯЕТ внизу блок «Совместимость:» списком устройств, извлекая модели из <name> и текста описания.
  - раскрывает списки со слешем (например, CM1100ADW/CM1100ADN/…);
  - приклеивает бренд/семейство (Pantum / HP LJ / Konica Minolta bizhub и т.п.) если модель «голая»;
  - отсеивает коды расходников (CE285A, TN-210K, DR-2300 и т.п.) и совпадение с vendorCode;
  - если устройств < 2 — блок не добавляется.
"""

from __future__ import annotations
from pathlib import Path
import io, re, sys
from html import escape as esc

SRC = Path("docs/price.yml")
DST = Path("docs/price_seo.yml")
ENC = "windows-1251"

# ---------- Верхний фиксированный блок ----------
COLOR_LINK  = "#0b3d91"
COLOR_WHITE = "#ffffff"
COLOR_BTN   = "#27ae60"
COLOR_KASPI = "#8b0000"

HEADER_HTML = f"""<div style="font-family: Cambria, 'Times New Roman', serif;">
  <center>
    <a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0"
       style="display:inline-block;background:{COLOR_BTN};color:{COLOR_WHITE};text-decoration:none;padding:10px 16px;border-radius:8px;font-weight:700;text-decoration:none;">
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
    safe = (s.replace("\u00A0"," ")
              .replace("\u20B8","тг.")   # ₸
              .replace("\u2248","~")     # ≈
              .replace("\u2013","-").replace("\u2014","—")
              .replace("\u201C",'"').replace("\u201D",'"')
              .replace("\u201E",'"').replace("\u201F",'"'))
    p.parent.mkdir(parents=True, exist_ok=True)
    with io.open(p, "w", encoding=ENC, newline="\n", errors="xmlcharrefreplace") as f:
        f.write(safe)

# ---------- Поиск внутри оффера ----------
OFFER_RX      = re.compile(r"<offer\b[^>]*>.*?</offer>",              re.I|re.S)
DESC_RX       = re.compile(r"<description\b[^>]*>(.*?)</description>", re.I|re.S)
NAME_RX       = re.compile(r"<name\b[^>]*>(.*?)</name>",               re.I|re.S)
VENDORCODE_RX = re.compile(r"<vendorCode\b[^>]*>(.*?)</vendorCode>",   re.I|re.S)
HR_RX         = re.compile(r"<hr\b[^>]*>", re.I)
HAS_HTML_TAGS = re.compile(r"<[a-zA-Z/!][^>]*>")

def normsp(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").replace("\u00A0"," ")).strip()

# ---------- «Технические характеристики»: раскладка (как у тебя) ----------
KV_LINE_RX = re.compile(r"^\s*(?:[-•–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$", re.S)
TECH_HDR_RX = re.compile(r"(?i)\bтехническ\w*\s+характеристик[аи]\s*:\s*")
TRIM_TECH_TAIL_RX = re.compile(r"(?i)[\s,;:—–-]*техническ\w*\s*$")

def kv_li(line: str) -> str:
    s = line.strip()
    if s.startswith(("- ","• ","– ","— ")): s = s[2:].strip()
    m = KV_LINE_RX.match(s)
    if m:
        key = esc(m.group(1).strip()); val = esc(m.group(2).strip())
        return f"<li><strong>{key}:</strong> {val}</li>"
    return f"<li>{esc(s)}</li>"

def make_ul(block_text: str) -> str:
    lines = [ln for ln in block_text.replace("\r\n","\n").replace("\r","\n").split("\n") if ln.strip()]
    return "<ul>\n" + "\n".join(kv_li(ln) for ln in lines) + "\n</ul>"

def pick_char_heading(context: str) -> str:
    return "Технические характеристики" if re.search(r"(?i)техническ\w*\s+характеристик", context or "") else "Характеристики"

def trim_trailing_tech_word(s: str) -> str:
    return TRIM_TECH_TAIL_RX.sub("", s or "").rstrip()

def transform_characteristics_paragraphs(html: str) -> str:
    def para_repl(m: re.Match) -> str:
        start, body, end = m.group(1), m.group(2), m.group(3)
        if not re.search(r"(?i)характеристик", body): return m.group(0)
        norm = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
        parts = re.split(r"(?i)характеристик[аи]:", norm, maxsplit=1)
        if len(parts) != 2: return m.group(0)
        head_raw, tail = parts[0], parts[1]
        head = trim_trailing_tech_word(head_raw.strip())
        bullet_lines = [ln for ln in tail.strip().split("\n") if ln.strip()]
        bullet_like  = [ln for ln in bullet_lines if ln.lstrip().startswith(("-", "•", "–", "—"))]
        if not bullet_like: return m.group(0)
        label = pick_char_heading(body)
        ul    = make_ul("\n".join(bullet_lines))
        out = (f"<p>{esc(head)}</p>\n" if head else "") + f"<p><strong>{label}:</strong></p>\n" + ul
        return start + out + end
    return re.sub(r"(<p[^>]*>)(.*?)(</p>)", para_repl, html, flags=re.S | re.I)

A_NO_STYLE_RX = re.compile(r"<a(?![^>]*\bstyle=)", re.I)

def emphasize_kv_in_li(html: str) -> str:
    def repl(m: re.Match) -> str:
        before, body, after = m.group(1), m.group(2), m.group(3)
        if re.search(r"^\s*(?:[-–—]\s*)?<strong>[^:]{1,120}:</strong>", body, flags=re.I): return m.group(0)
        kv = re.sub(r"^\s*(?:[-•–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$",
                    lambda k: f"<strong>{esc(k.group(1).strip())}:</strong> {esc(k.group(2).strip())}",
                    body, count=1, flags=re.S)
        return f"{before}{kv}{after}"
    return re.sub(r"(<li[^>]*>)(.*?)(</li>)", repl, html, flags=re.S | re.I)

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
    for blk in blocks:
        if re.search(r"(?i)\bхарактеристик", blk):
            parts = re.split(r"(?i)характеристик[аи]:", blk, maxsplit=1)
            head_raw = parts[0].strip()
            tail     = parts[1].strip() if len(parts)==2 else ""
            label    = pick_char_heading(blk)
            head     = trim_trailing_tech_word(head_raw)
            if head: out.append(f"<p>{esc(head)}</p>")
            if tail:
                out.append(f"<p><strong>{label}:</strong></p>")
                out.append(make_ul(tail))
        else:
            lines = [esc(x) for x in blk.split("\n")]
            out.append(f"<p>{'<br>'.join(lines)}</p>")
    return (f'<div style="font-family: \'Times New Roman\', Times, serif; '
            f'font-size:15px; line-height:1.55;">' + "\n".join(out) + "</div>")

def beautify_original_description(inner: str) -> str:
    c = inner.strip()
    if not c: return ""
    return beautify_existing_html(c) if HAS_HTML_TAGS.search(c) else beautify_plain_text(c)

# ---------- «Совместимость»: извлечение из name + description ----------
BRANDS = (
    "HP","Hewlett Packard","Canon","Epson","Brother","Kyocera","Samsung",
    "Ricoh","Xerox","Sharp","Lexmark","OKI","Panasonic","Konica Minolta","Pantum",
)
FAMILY_HINTS = (
    "Color","Laser","LaserJet","LJ","MFP","DeskJet","OfficeJet","PageWide","DesignJet",
    "imageRUNNER","i-SENSYS","LBP","PIXMA",
    "Stylus","WorkForce","EcoTank","SureColor",
    "HL","DCP","MFC",
    "FS","TASKalfa","ECOSYS",
    "CLX","ML","SL",
    "SP","Aficio",
    "VersaLink","WorkCentre","Phaser",
    "bizhub",
)
CARTRIDGE_PATTERNS = (
    r"(?:CE|CF|CB|CC|Q)[0-9]{2,4}[A-Z]",
    r"(?:CRG)[- ]?\d{2,4}",
    r"(?:TN|TK|TKC|TNP|TNR)[- ]?\d{2,5}[A-Z]?",
    r"(?:DR|DK|DV)[- ]?\d{2,5}[A-Z]?",
    r"(?:CN|AR|MX|JL)[- ]?\d{2,5}[A-Z]?",
)
CARTRIDGE_RE = re.compile(rf"(?i)\b({'|'.join(CARTRIDGE_PATTERNS)})\b")
MODEL_RE = re.compile(r"\b([A-Z]{1,4}-?[A-Z]?\d{2,6}[A-Z]?(?:-[A-Z0-9]{1,4})?)\b", re.I)
SEPS_RE  = re.compile(r"[,/;|]|(?:\s+\bи\b\s+)", re.I)

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\u00A0"," ")).strip()

def extract_brand_context(text: str) -> tuple[str,str]:
    t = normalize_space(text)
    brand = ""
    for b in BRANDS:
        m = re.search(rf"(?i)\b{re.escape(b)}\b", t)
        if m and (not brand or m.start() < t.lower().find(brand.lower())):
            brand = b
    family = ""
    if brand:
        after = t[t.lower().find(brand.lower()) + len(brand):].strip()
        m2 = re.search(rf"(?i)\b({'|'.join([re.escape(x) for x in FAMILY_HINTS])})\b", after)
        if m2: family = m2.group(0)
    return brand.strip(), family.strip()

def split_candidates(chunk: str) -> list[str]:
    return [normalize_space(p) for p in SEPS_RE.split(chunk) if normalize_space(p)]

def expand_slashed_models(s: str) -> list[str]:
    if "/" not in s: return [s]
    tokens = [t for t in s.split("/") if t]
    prefix = ""
    m2 = re.match(r"^(.*?)[A-Za-z]*\d.*$", tokens[0].strip())
    if m2: prefix = normalize_space(m2.group(1) or "")
    out = []
    for tk in tokens:
        tk = normalize_space(tk)
        if prefix and not re.match(r"(?i)^" + re.escape(prefix), tk):
            out.append((prefix + tk).strip())
        else:
            out.append(tk)
    return out

def extract_models_from_text(text: str) -> list[str]:
    t = normalize_space(text)
    raw = []
    for p in split_candidates(t):
        raw.extend(expand_slashed_models(p))
    models = []
    for item in raw:
        for m in MODEL_RE.finditer(item):
            models.append(m.group(1).upper())
    return models

def build_compatibility_block(name_text: str, desc_text_plain: str, vendor_code_hint: str) -> str:
    brand, family = extract_brand_context(name_text + " " + desc_text_plain)
    raw_models = extract_models_from_text(name_text) + extract_models_from_text(desc_text_plain)
    raw_models = [m for m in raw_models if (not vendor_code_hint or m.upper()!=vendor_code_hint.upper()) and not CARTRIDGE_RE.search(m)]
    # приклеиваем бренд/семейство к «голым» кодам; без family — пропускаем короткие
    models = []
    for m in raw_models:
        if re.match(r"(?i)^(HP|HEWLETT PACKARD|EPSON|CANON|BROTHER|KYOCERA|SAMSUNG|RICOH|XEROX|SHARP|LEXMARK|OKI|PANASONIC|KONICA|KONICA MINOLTA|PANTUM)\b", m):
            models.append(m); continue
        if family:
            prefix = " ".join(x for x in (brand, family) if x)
            models.append((prefix + " " + m).strip())
    # уникализация и сортировка
    seen=set(); out=[]
    for x in models:
        k=normalize_space(x)
        if k and k not in seen: seen.add(k); out.append(k)
    out.sort(key=lambda s: (re.split(r"\s+", s)[0].lower(), s.lower()))
    if len(out)<2: return ""
    lis="\n".join(f"<li>{esc(x)}</li>" for x in out)
    return "<p><strong>Совместимость:</strong></p>\n<ul>\n" + lis + "\n</ul>"

def has_compat_block(html: str) -> bool:
    return re.search(r"(?i)<strong>\s*совместим\w*\s*:</strong>", html) is not None

def extract_plain_text(html: str) -> str:
    txt = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    return normalize_space(txt)

def inject_compatibility(html_tail_times: str, name_text: str, vendor_code_hint: str) -> str:
    if has_compat_block(html_tail_times):
        lis = re.findall(r"(?is)<p[^>]*>\s*<strong>\s*совместим\w*\s*:</strong>\s*</p>\s*<ul>(.*?)</ul>", html_tail_times)
        if lis and len(re.findall(r"<li\b", lis[0], flags=re.I)) >= 2:
            return html_tail_times
    plain = extract_plain_text(html_tail_times)
    compat_block = build_compatibility_block(name_text, plain, vendor_code_hint)
    if not compat_block:
        return html_tail_times
    return re.sub(r"(?is)</div>\s*$", "\n" + compat_block + "\n</div>", html_tail_times, count=1) or (html_tail_times + "\n" + compat_block)

# ---------- Сборка DESCRIPTION ----------
def has_our_header(desc_html: str) -> bool:
    return "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_html

def beautify_tail_any(tail: str) -> str:
    if not HAS_HTML_TAGS.search(tail):
        return beautify_plain_text(tail)
    m = re.search(r"(<div[^>]*font-family:\s*['\"]?Times New Roman['\"]?[^>]*>)(.*?)(</div>)",
                  tail, flags=re.I|re.S)
    if m:
        start_div, inner, end_div = m.group(1), m.group(2), m.group(3)
        processed = emphasize_kv_in_li(A_NO_STYLE_RX.sub(f'<a style="color:{COLOR_LINK};text-decoration:none"',
                              transform_characteristics_paragraphs(inner)))
        return tail[:m.start()] + start_div + processed + end_div + tail[m.end():]
    return beautify_existing_html(tail)

def rebuild_with_existing_header(desc_inner: str, name_text: str, vendor_code_hint: str) -> str:
    parts = HR_RX.split(desc_inner, maxsplit=1)
    if len(parts) != 2:
        body = beautify_tail_any(desc_inner)
        body = inject_compatibility(body, name_text, vendor_code_hint)
        return "<description>" + body + "</description>"
    head, tail = parts[0], parts[1]
    tail_fixed = beautify_tail_any(tail)
    tail_fixed = inject_compatibility(tail_fixed, name_text, vendor_code_hint)
    return "<description>" + head + "<hr>\n\n" + tail_fixed + "</description>"

def build_new_description(existing_inner: str, name_text: str, vendor_code_hint: str) -> str:
    pretty = beautify_tail_any(existing_inner)
    pretty = inject_compatibility(pretty, name_text, vendor_code_hint)
    return HEADER_HTML + ("\n\n<hr>\n\n" + pretty if pretty else "")

def inject_into_description(desc_inner: str, name_text: str, vendor_code_hint: str) -> str:
    if has_our_header(desc_inner):
        return rebuild_with_existing_header(desc_inner, name_text, vendor_code_hint)
    return "<description>" + build_new_description(desc_inner, name_text, vendor_code_hint) + "</description>"

def add_description_if_missing(offer_xml: str) -> str:
    if re.search(r"<description\b", offer_xml, flags=re.I): return offer_xml
    m = re.search(r"\n([ \t]+)<", offer_xml); indent = m.group(1) if m else "  "
    ins = f"\n{indent}<description>{HEADER_HTML}</description>"
    tail = (ins + "\n" + indent[:-2] + "</offer>") if len(indent) >= 2 else (ins + "\n</offer>")
    return offer_xml.replace("</offer>", tail)

# ---------- Основной проход ----------
def process_offer(offer_xml: str) -> str:
    # контекст
    name_text = ""
    mname = NAME_RX.search(offer_xml)
    if mname and mname.group(1):
        name_text = normsp(re.sub(r"<[^>]+>", " ", mname.group(1)))
    vendor_hint = ""
    mvc = VENDORCODE_RX.search(offer_xml)
    if mvc and mvc.group(1):
        vendor_hint = normsp(re.sub(r"<[^>]+>", " ", mvc.group(1)))
    # описание
    def _desc_repl(m: re.Match) -> str:
        return inject_into_description(m.group(1), name_text, vendor_hint)
    updated = DESC_RX.sub(_desc_repl, offer_xml)
    if updated == offer_xml:
        updated = add_description_if_missing(offer_xml)
    return updated

def process_text(xml_text: str) -> str:
    return OFFER_RX.sub(lambda m: process_offer(m.group(0)), xml_text)

def main() -> int:
    if not SRC.exists():
        print(f"[seo] Исходный файл не найден: {SRC}", file=sys.stderr); return 1
    original  = rtext(SRC)
    processed = process_text(original)
    wtext(DST, processed)
    print(f"[seo] OK: {DST} — добавлен блок «Совместимость» (из name+description), остальное без изменений")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
