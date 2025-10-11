#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
————————
Меняет ТОЛЬКО <description> у каждого <offer>:

• Если верхний блок (WhatsApp/Оплата/Доставка) отсутствует — добавляет его и <hr>.
• «Технические характеристики» НЕ трогает вообще (сохраняет как есть).
• Добавляет/пересобирает «Совместимость» внизу описания по шаблону:
    <p><strong>Совместимость:</strong></p>
    <ul>
      <li>Бренд Семейство</li>
      ...
    </ul>
  Из слэш-цепочек и наборов моделей извлекаются «семейства» (CM1100, CP1100 и т.п.), с префиксом бренда.
  В итоге список краткий, как в твоём файле-примере.

Вход:  docs/price.yml
Выход: docs/price_seo.yml
"""

from __future__ import annotations
from pathlib import Path
import io, re, sys
from html import escape as esc

SRC = Path("docs/price.yml")
DST = Path("docs/price_seo.yml")
ENC = "windows-1251"

# ===== Верхний фиксированный блок =====
COLOR_LINK  = "#0b3d91"
COLOR_WHITE = "#ffffff"
COLOR_BTN   = "#27ae60"
COLOR_KASPI = "#8b0000"

HEADER_HTML = f"""<div style="font-family: Cambria, 'Times New Roman', serif;">
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
    <li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тенге | 3-7 рабочих дней | Сотрудничаем с курьерской компанией
      <a href="https://exline.kz/" style="color:{COLOR_LINK};text-decoration:none;"><strong>Exline.kz</strong></a></em>
    </li>
    <li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>
    <li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал "САЙРАН"</em></li>
  </ul>
</div>"""

# ===== I/O =====
def rtext(p: Path) -> str:
    with io.open(p, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def wtext(p: Path, s: str) -> None:
    # cp1251-safe
    safe = (s.replace("\u00A0"," ")
              .replace("\u20B8","тг.")   # ₸
              .replace("\u2248","~")     # ≈
              .replace("\u2013","-").replace("\u2014","—")
              .replace("\u201C",'"').replace("\u201D",'"')
              .replace("\u201E",'"').replace("\u201F",'"'))
    p.parent.mkdir(parents=True, exist_ok=True)
    with io.open(p, "w", encoding=ENC, newline="\n", errors="xmlcharrefreplace") as f:
        f.write(safe)

# ===== Регэкспы: работаем ТОЛЬКО внутри <description> =====
OFFER_RX = re.compile(r"<offer\b[^>]*>.*?</offer>", re.I|re.S)
DESC_RX  = re.compile(r"<description\b[^>]*>(.*?)</description>", re.I|re.S)
HR_RX    = re.compile(r"<hr\b[^>]*>", re.I)

def normsp(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").replace("\u00A0"," ")).strip()

# ===== Совместимость (семейства моделей) =====
# нормализация похожих букв (кириллица→латиница) для корректной CM/CP
LOOKALIKE = str.maketrans({
    "А":"A","В":"B","С":"C","Е":"E","Н":"H","К":"K","М":"M","О":"O","Р":"P","Т":"T","Х":"X","У":"Y",
    "а":"a","в":"b","с":"c","е":"e","н":"h","к":"k","м":"m","о":"o","р":"p","т":"t","х":"x","у":"y",
})
def latinize(s: str) -> str: return (s or "").translate(LOOKALIKE)

# Ключевые подписи, откуда берём исходные строки моделей
KV_SPLIT = re.compile(r"\s[-–—]\s")  # « - » между KV-парами
KEY_RX   = re.compile(r"^\s*([^:]{1,120}?)\s*:\s*(.+?)\s*$", re.S)
TARGET_KEYS = ("совместимость с моделями", "принтеры", "совместимость")

# Разделители внутри значений
SEPS_RE = re.compile(r"[;,]|\s+\bи\b\s+", re.I)

# Токен модели (включает семейство+вариант, типа CM1100ADW/CP1100DN)
MODEL_TOKEN_RE = re.compile(r"\b([A-Z]{1,4}-?[A-Z]?\d{2,6}[A-Z]?(?:-[A-Z0-9]{1,4})?)\b", re.I)

# Семейство = часть до первой буквы/суффикса после цифр (CM1100 из CM1100ADW; CP1100 из CP1100DN)
FAMILY_FROM_TOKEN = re.compile(r"^([A-Z]{1,4}-?[A-Z]?\d{2,6})", re.I)

def html_to_text(html: str) -> str:
    t = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace(" — ", " - ").replace(" – ", " - ")
    return normsp(t)

def parse_kv_pairs(flat_text: str) -> dict[str,str]:
    pairs = {}
    for chunk in KV_SPLIT.split(flat_text):
        m = KEY_RX.match(chunk.strip())
        if not m: continue
        key = normsp(m.group(1)).lower()
        val = normsp(m.group(2))
        pairs[key] = val
    return pairs

def expand_slashes(series: str) -> list[str]:
    # раскрываем A/B/C → ["A","B","C"] с восстановлением общего префикса (Pantum CM …)
    parts = [p for p in series.split("/") if p]
    if len(parts) == 1:
        return [series]
    first = normsp(latinize(parts[0]))
    m = re.match(r"^(.*?)[A-Za-z]*\d", first)
    prefix = normsp(m.group(1)) if m else ""
    out = [first]
    for p in parts[1:]:
        tk = normsp(latinize(p))
        out.append((prefix + tk).strip() if prefix and not tk.lower().startswith(prefix.lower()) else tk)
    return out

def split_series(value: str) -> list[str]:
    items = []
    for chunk in SEPS_RE.split(value):
        chunk = normsp(chunk)
        if not chunk: continue
        if "/" in chunk:
            items.extend(expand_slashes(chunk))
        else:
            items.append(latinize(chunk))
    return items

def families_from_value(value: str, brand_hint: str = "") -> list[str]:
    """Из строки со списком моделей собирает семейства (CM1100, CP1100 …) и приклеивает бренд, если задан."""
    fams = []
    for frag in split_series(value):
        for m in MODEL_TOKEN_RE.finditer(frag):
            token = m.group(1).upper()
            mf = FAMILY_FROM_TOKEN.match(token)
            if not mf:
                continue
            family = mf.group(1)  # CM1100ADW -> CM1100
            # попробуем вытащить слева бренд (если он присутствует в тексте фрагмента)
            before = frag[:m.start()].strip()
            brand = ""
            # простая эвристика бренда слева (первое слово с заглавной)
            mb = re.search(r"\b([A-Z][a-zA-Z]+)\b(?:\s+[A-Z][a-zA-Z]+)?\s*$", before)
            if mb:
                brand = mb.group(0).strip()
            if not brand and brand_hint:
                brand = brand_hint
            full = (brand + " " + family).strip() if brand else family
            fams.append(normsp(full))
    # дедуп с сохранением порядка
    seen, out = set(), []
    for s in fams:
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

BRAND_HINT_RX = re.compile(r"\b(HP|Hewlett[ -]?Packard|Canon|Epson|Brother|Kyocera|Samsung|Ricoh|Xerox|Sharp|Lexmark|OKI|Panasonic|Konica(?:-| )?Minolta|Pantum)\b", re.I)

def brand_hint_from_text(text: str) -> str:
    m = BRAND_HINT_RX.search(latinize(text))
    return m.group(0) if m else ""

def collect_compat_families(desc_inner_html: str) -> list[str]:
    # работаем с «низом» (после <hr>) если он есть
    parts = HR_RX.split(desc_inner_html, maxsplit=1)
    tail = parts[1] if len(parts) == 2 else desc_inner_html
    flat = html_to_text(tail)
    hint = brand_hint_from_text(flat)

    kv = parse_kv_pairs(flat)
    fams: list[str] = []
    for key, val in kv.items():
        if any(key.startswith(k) for k in TARGET_KEYS):
            fams.extend(families_from_value(val, hint))

    if len(fams) < 1:
        # fallback: возьмём просто из «Принтеры: …» если встречается
        m = re.search(r"(?i)принтеры\s*:\s*(.+)", flat)
        if m:
            fams.extend(families_from_value(m.group(1), hint))

    # короткий список семейств (как в твоём файле). Если получилось слишком много,
    # оставим уникальные и отсортируем стабильно по алфавиту.
    seen, out = set(), []
    for s in fams:
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def render_compat_ul(models: list[str]) -> str:
    if len(models) < 1:
        return ""
    items = "\n".join(f"<li>{esc(m)}</li>" for m in models)
    return "<p><strong>Совместимость:</strong></p>\n<ul>\n" + items + "\n</ul>"

def has_compat_block(html: str) -> bool:
    return re.search(r"(?i)<strong>\s*совместим\w*\s*:</strong>", html) is not None

# ===== Сборка частей =====
def ensure_header(desc_inner: str) -> str:
    if "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_inner:
        return desc_inner
    return HEADER_HTML + ("\n\n<hr>\n\n" + desc_inner if desc_inner.strip() else "")

def inject_compatibility(desc_inner_html: str) -> str:
    # если «Совместимость» уже есть и там >=1 пункт — оставим как есть (чтобы не ломать готовые карточки)
    if has_compat_block(desc_inner_html):
        m = re.search(r"(?is)<p[^>]*>\s*<strong>\s*совместим\w*:\s*</strong>\s*</p>\s*<ul>(.*?)</ul>", desc_inner_html)
        if m and len(re.findall(r"<li\b", m.group(1), flags=re.I)) >= 1:
            return "<description>" + desc_inner_html + "</description>"

    fams = collect_compat_families(desc_inner_html)
    compat_html = render_compat_ul(fams)
    if not compat_html:
        return "<description>" + desc_inner_html + "</description>"

    # вставляем «Совместимость» в самый низ (после <hr>, если есть)
    parts = HR_RX.split(desc_inner_html, maxsplit=1)
    if len(parts) == 2:
        head, tail = parts[0], parts[1]
        tail2 = re.sub(r"(?is)<p[^>]*>\s*<strong>\s*совместим\w*:\s*</strong>\s*</p>\s*<ul>.*?</ul>",
                       compat_html, tail, count=1)
        if tail2 == tail:
            tail2 = tail.rstrip() + ("\n" if not tail.rstrip().endswith("\n") else "") + compat_html
        return "<description>" + head + "<hr>\n\n" + tail2 + "</description>"
    else:
        body = desc_inner_html.rstrip() + ("\n" if not desc_inner_html.rstrip().endswith("\n") else "") + compat_html
        return "<description>" + body + "</description>"

# ===== Основной проход =====
def process_offer(offer_xml: str) -> str:
    def _desc_repl(m: re.Match) -> str:
        inner = ensure_header(m.group(1))
        # НЕ трогаем «Технические характеристики» вообще — берём как есть
        return inject_compatibility(inner)
    new_block = DESC_RX.sub(_desc_repl, offer_xml)
    if new_block == offer_xml:
        # не было <description> — создаём минимальный с «шапкой»
        m = re.search(r"\n([ \t]+)<", offer_xml)
        indent = m.group(1) if m else "  "
        ins = f"\n{indent}<description>{HEADER_HTML}</description>"
        tail = (ins + "\n" + indent[:-2] + "</offer>") if len(indent) >= 2 else (ins + "\n</offer>")
        new_block = offer_xml.replace("</offer>", tail)
    return new_block

def process_text(xml_text: str) -> str:
    return OFFER_RX.sub(lambda m: process_offer(m.group(0)), xml_text)

def main() -> int:
    if not SRC.exists():
        print(f"[seo] Исходный файл не найден: {SRC}", file=sys.stderr); return 1
    original  = rtext(SRC)
    processed = process_text(original)
    wtext(DST, processed)
    print(f"[seo] OK: {DST} — добавлен блок «Совместимость» (семейства), остальное без изменений")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
