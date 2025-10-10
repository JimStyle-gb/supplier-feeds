#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py — работает ТОЛЬКО с <description>, прочие теги не трогает.
Фикс: нормализация кириллических «похожих» букв (С↔C, А↔A и т.п.) для корректного извлечения всех моделей.
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

# ---------------- I/O ----------------

def rtext(path: Path) -> str:
    with io.open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def wtext(path: Path, text: str) -> None:
    safe = (text
            .replace("\u20B8", "тг.")   # ₸
            .replace("\u2248", "~")     # ≈
            .replace("\u00A0", " ")     # NBSP
            .replace("\u201C", '"').replace("\u201D", '"')
            .replace("\u201E", '"').replace("\u201F", '"')
            .replace("\u2013", "-").replace("\u2014", "—"))
    path.parent.mkdir(parents=True, exist_ok=True)
    with io.open(path, "w", encoding=ENC, newline="\n", errors="xmlcharrefreplace") as f:
        f.write(safe)

# ------------- Служебные RegExp (ТОЛЬКО для description) -------------

DESC_RX       = re.compile(r"<description\b[^>]*>(.*?)</description>", re.I | re.S)
OFFER_RX      = re.compile(r"<offer\b[^>]*>.*?</offer>",              re.I | re.S)
NAME_RX       = re.compile(r"<name\b[^>]*>(.*?)</name>",              re.I | re.S)
VENDORCODE_RX = re.compile(r"<vendorCode\b[^>]*>(.*?)</vendorCode>",  re.I | re.S)
HAS_HTML_TAGS = re.compile(r"<[a-zA-Z/!][^>]*>")
A_NO_STYLE_RX = re.compile(r"<a(?![^>]*\bstyle=)", re.I)
HR_RX         = re.compile(r"<hr\b[^>]*>", re.I)

KV_LINE_RX    = re.compile(r"^\s*(?:[-•–—]\s*)?([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$", re.S)
TECH_RX       = re.compile(r"(?i)техническ\w*\s+характеристик", re.U)
TRIM_TECH_TAIL_RX = re.compile(r"(?i)[\s,;:—–-]*техническ\w*\s*$")

def pick_char_heading(context: str) -> str:
    return "Технические характеристики" if TECH_RX.search(context or "") else "Характеристики"

def trim_trailing_tech_word(s: str) -> str:
    return TRIM_TECH_TAIL_RX.sub("", s or "").rstrip()

# ----------- Нормализация «похожих» букв (кириллица → латиница) -----------

LOOKALIKE_MAP = str.maketrans({
    "А":"A","В":"B","С":"C","Е":"E","Н":"H","К":"K","М":"M","О":"O","Р":"P","Т":"T","Х":"X","У":"Y",
    "а":"a","в":"b","с":"c","е":"e","н":"h","к":"k","м":"m","о":"o","р":"p","т":"t","х":"x","у":"y",
})

def latinize_lookalikes(text: str) -> str:
    return (text or "").translate(LOOKALIKE_MAP)

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\u00A0"," ")).strip()

# ---------------- Оформление характеристик (внутри description) ----------------

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
    def para_repl(m: re.Match) -> str:
        start, body, end = m.group(1), m.group(2), m.group(3)
        if not re.search(r"(?i)характеристик", body): return m.group(0)
        norm = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
        parts = re.split(r"(?i)характеристик[аи]:", norm, maxsplit=1)
        if len(parts) != 2: return m.group(0)
        head_raw, tail = parts[0], parts[1]
        head = trim_trailing_tech_word(head_raw.strip())
        bullet_lines = [ln for ln in tail.strip().split("\n") if ln.strip()]
        bullet_like = [ln for ln in bullet_lines if ln.lstrip().startswith(("-", "•", "–", "—"))]
        if not bullet_like: return m.group(0)
        label = pick_char_heading(body)
        ul = make_ul("\n".join(bullet_lines))
        out = (f"<p>{head}</p>\n" if head else "") + f"<p><strong>{label}:</strong></p>\n" + ul
        return start + out + end
    return re.sub(r"(<p[^>]*>)(.*?)(</p>)", para_repl, html, flags=re.S | re.I)

SPLIT_HINT_RX = re.compile(r"\s[-–—•·]\s(?=[^:<>{}\n]{1,120}\s*:)")
KV_ANY_RX     = re.compile(r"^\s*([^:<>{}\n]{1,120}?)\s*:\s*(.+?)\s*$", re.S)

def explode_inline_li(html: str) -> str:
    def li_repl(m: re.Match) -> str:
        body = m.group(2)
        if not re.search(r"[ \u00A0][\-–—•·][ \u00A0]", body): return m.group(0)
        norm = (body.replace("\u00A0"," ")
                    .replace(" — "," - ").replace(" – "," - ")
                    .replace(" • "," - ").replace(" · "," - "))
        if norm.count(":") <= 1: return m.group(0)
        parts, buf = [], []
        for i, tk in enumerate(norm.split(" - ")):
            if i == 0: buf.append(tk); continue
            if re.match(r"\s*[^:<>{}\n]{1,120}:\s*.", tk, flags=re.S):
                parts.append(" - ".join(buf)); buf = [tk]
            else:
                buf.append(tk)
        parts.append(" - ".join(buf))
        lis = []
        for frag in parts:
            frag = frag.strip()
            if "<" in frag and ">" in frag:
                lis.append(f"<li>{frag}</li>"); continue
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
        if re.search(r"(?i)\bхарактеристик", blk):
            parts = re.split(r"(?i)характеристик[аи]:", blk, maxsplit=1)
            head_raw = parts[0].strip()
            tail     = parts[1].strip() if len(parts)==2 else ""
            label    = pick_char_heading(blk)
            head     = trim_trailing_tech_word(head_raw)
            if head: out.append(f"<p>{html_escape(head)}</p>")
            if tail:
                out.append(f"<p><strong>{label}:</strong></p>")
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

# ------------- Совместимость (только устройства), с нормализацией букв -------------

BRANDS = (
    "HP","Hewlett Packard","Canon","Epson","Brother","Kyocera","Samsung",
    "Ricoh","Xerox","Sharp","Lexmark","OKI","Panasonic","Konica Minolta","Konica-Minolta","Pantum",
)

FAMILY_HINTS = ("Color","Laser","LaserJet","LJ","MFP","DeskJet","OfficeJet","PageWide","DesignJet",
                "imageRUNNER","i-SENSYS","LBP","PIXMA","Stylus","WorkForce","EcoTank","SureColor",
                "HL","DCP","MFC","FS","TASKalfa","ECOSYS","CLX","ML","SL","SP","Aficio",
                "VersaLink","WorkCentre","Phaser","bizhub")

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

def extract_brand_context(text: str) -> tuple[str,str]:
    t = normalize_space(latinize_lookalikes(text))
    brand = ""; brand_pos = 10**9
    for b in BRANDS:
        m = re.search(rf"(?i)\b{re.escape(b)}\b", t)
        if m and m.start() < brand_pos:
            brand, brand_pos = m.group(0), m.start()
    family = ""
    if brand:
        after = t[brand_pos + len(brand):]
        best = 10**9
        for fam in FAMILY_HINTS:
            m2 = re.search(rf"(?i)\b{re.escape(fam)}\b", after)
            if m2 and m2.start() < best:
                family, best = m2.group(0), m2.start()
    return brand.strip(), family.strip()

def split_candidates(chunk: str) -> list[str]:
    return [normalize_space(p) for p in SEPS_RE.split(chunk) if normalize_space(p)]

def expand_slashed_models(s: str) -> list[str]:
    if "/" not in s: return [s]
    tokens = [t for t in s.split("/") if t]
    prefix = ""
    first = tokens[0].strip()
    m2 = re.match(r"^(.*?)[A-Za-z]*\d.*$", first)
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
    t = normalize_space(latinize_lookalikes(text))
    raw_parts = split_candidates(t)
    expanded = []
    for p in raw_parts:
        expanded.extend(expand_slashed_models(p))
    models = []
    for item in expanded:
        for m in MODEL_RE.finditer(item):
            models.append(m.group(1).upper())
    return models

def is_cartridge_code(s: str, vendor_code_hint: str = "") -> bool:
    if not s: return False
    if vendor_code_hint and s.upper() == vendor_code_hint.upper():
        return True
    return CARTRIDGE_RE.search(s) is not None

DEVICE_BRAND_PREFIX_RE = re.compile(
    r"(?i)^(HP|Hewlett Packard|Epson|Canon|Brother|Kyocera|Samsung|Ricoh|Xerox|Sharp|Lexmark|OKI|Panasonic|Konica|Konica-Minolta|Konica Minolta|Pantum)\b"
)

def attach_brand_family_only_devices(models: list[str], brand: str, family: str) -> list[str]:
    b = brand.strip(); fam = family.strip()
    res = []
    for m in models:
        if DEVICE_BRAND_PREFIX_RE.match(m):
            res.append(m); continue
        # если есть бренд — допускаем и без family (Pantum CM1100ADW …)
        if b:
            prefix = " ".join([x for x in (b, fam) if x])
            res.append((prefix + " " + m).strip())
        elif fam:
            res.append((fam + " " + m).strip())
        else:
            # короткий код без контекста — отбрасываем
            continue
    return res

def uniq_sorted(models: list[str]) -> list[str]:
    seen = set(); out = []
    for m in models:
        k = normalize_space(m)
        if k and k not in seen:
            seen.add(k); out.append(k)
    out.sort(key=lambda s: (re.split(r"\s+", s)[0].lower(), s.lower()))
    return out

def render_compat_ul(models: list[str]) -> str:
    items = "\n".join(f"<li>{html_escape(m)}</li>" for m in models)
    return "<ul>\n" + items + "\n</ul>"

def extract_plain_text(html: str) -> str:
    txt = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    return normalize_space(txt)

def build_compatibility_block(name_text: str, desc_text_plain: str, vendor_code_hint: str) -> str:
    brand, family = extract_brand_context(name_text + " " + desc_text_plain)
    raw_models = extract_models_from_text(name_text) + extract_models_from_text(desc_text_plain)
    raw_models = [m for m in raw_models if not is_cartridge_code(m, vendor_code_hint)]
    models = attach_brand_family_only_devices(raw_models, brand, family)
    models = [m for m in models if re.search(r"[A-Za-z]*\d", m)]
    models = uniq_sorted(models)
    if len(models) < 2:
        return ""
    return "<p><strong>Совместимость:</strong></p>\n" + render_compat_ul(models)

def has_compat_block(html: str) -> bool:
    return re.search(r"(?i)<strong>\s*совместим\w*\s*:</strong>", html) is not None

# ---------------- Сборка DESCRIPTION (прочие теги не трогаем) ----------------

def has_our_block(desc_html: str) -> bool:
    return "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_html

def beautify_original_description(inner: str) -> str:
    content = inner.strip()
    if not content: return ""
    if HAS_HTML_TAGS.search(content): return beautify_existing_html(content)
    return beautify_plain_text(content)

def inject_compatibility(html_tail_times: str, name_text: str, vendor_code_hint: str) -> str:
    if has_compat_block(html_tail_times):
        lis = re.findall(r"(?is)<p[^>]*>\s*<strong>\s*совместим\w*\s*:</strong>\s*</p>\s*<ul>(.*?)</ul>", html_tail_times)
        if lis:
            items = re.findall(r"<li\b", lis[0], flags=re.I)
            if len(items) >= 2:
                return html_tail_times
    plain = extract_plain_text(html_tail_times)
    compat_block = build_compatibility_block(name_text, plain, vendor_code_hint)
    if not compat_block:
        return html_tail_times
    return re.sub(r"(?is)</div>\s*$", "\n" + compat_block + "\n</div>", html_tail_times, count=1)

def rebuild_with_existing_block(desc_inner: str, name_text: str, vendor_code_hint: str) -> str:
    parts = HR_RX.split(desc_inner, maxsplit=1)
    if len(parts) != 2:
        return "<description>" + desc_inner + "</description>"
    head, tail = parts[0], parts[1]
    m = re.search(r"(<div[^>]*font-family:\s*['\"]?Times New Roman['\"]?[^>]*>)(.*?)(</div>)",
                  tail, flags=re.I | re.S)
    if m:
        start_div, inner, end_div = m.group(1), m.group(2), m.group(3)
        processed = emphasize_kv_in_li(
            A_NO_STYLE_RX.sub(f'<a style="color:{COLOR_LINK};text-decoration:none"', inner)
        )
        processed = inject_compatibility(processed, name_text, vendor_code_hint)
        new_tail = tail[:m.start()] + start_div + processed + end_div + tail[m.end():]
        return "<description>" + head + "<hr>\n\n" + new_tail + "</description>"
    pretty_tail = beautify_original_description(tail)
    pretty_tail = inject_compatibility(pretty_tail, name_text, vendor_code_hint)
    return "<description>" + head + "<hr>\n\n" + pretty_tail + "</description>"

def build_new_description(existing_inner: str, name_text: str, vendor_code_hint: str) -> str:
    pretty = beautify_original_description(existing_inner)
    pretty = inject_compatibility(pretty, name_text, vendor_code_hint)
    return TEMPLATE_HTML + ("\n\n<hr>\n\n" + pretty if pretty else "")

def inject_into_description_block(desc_inner: str, name_text: str, vendor_code_hint: str) -> str:
    if has_our_block(desc_inner):
        return rebuild_with_existing_block(desc_inner, name_text, vendor_code_hint)
    return "<description>" + build_new_description(desc_inner, name_text, vendor_code_hint) + "</description>"

def add_description_if_missing(offer_block: str) -> str:
    if re.search(r"<description\b", offer_block, flags=re.I): return offer_block
    m = re.search(r"\n([ \t]+)<", offer_block); indent = m.group(1) if m else "  "
    insertion = f"\n{indent}<description>{TEMPLATE_HTML}</description>"
    tail = (insertion + "\n" + indent[:-2] + "</offer>") if len(indent) >= 2 else (insertion + "\n</offer>")
    return offer_block.replace("</offer>", tail)

# ------------- Глобально НЕ меняем теги, только содержимое description -------------

def process_offer_block(offer_block: str) -> str:
    name_text = ""
    mname = NAME_RX.search(offer_block)
    if mname and mname.group(1):
        name_text = normalize_space(re.sub(r"<[^>]+>", " ", mname.group(1)))
    vendor_hint = ""
    mvc = VENDORCODE_RX.search(offer_block)
    if mvc and mvc.group(1):
        vendor_hint = normalize_space(re.sub(r"<[^>]+>", " ", mvc.group(1)))

    def _desc_repl(m: re.Match) -> str:
        return inject_into_description_block(m.group(1), name_text, vendor_hint)

    # правим только <description> внутри оффера
    block = DESC_RX.sub(_desc_repl, offer_block)
    if not re.search(r"<description\b", block, flags=re.I):
        block = add_description_if_missing(block)
    return block

def process_text(xml_text: str) -> str:
    # Меняем только контент <description>, остальное копируем как есть
    return OFFER_RX.sub(lambda m: process_offer_block(m.group(0)), xml_text)

def main() -> int:
    if not SRC.exists():
        print(f"[seo] Исходный файл не найден: {SRC}", file=sys.stderr); return 1
    original  = rtext(SRC)
    processed = process_text(original)
    wtext(DST, processed)
    print(f"[seo] OK: {DST} (описания обновлены, прочие теги не тронуты)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
