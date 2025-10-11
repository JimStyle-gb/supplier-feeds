#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
————————
БЕРЁТ твой рабочий формат описания за основу, меняет ТОЛЬКО <description>:

1) Если верхнего блока (WhatsApp/Оплата/Доставка) нет — добавляет его и <hr>.
2) «Технические характеристики» НЕ трогает, если уже оформлены списком (как в твоём рабочем коде).
   (Если встречает слитную строку «… Технические характеристики: - Ключ: Значение - …», аккуратно
   превращает в <ul> с жирными ключами — это поведение оставлено без изменений.)
3) ДОБАВЛЯЕТ в самый низ блок «Совместимость:»
   • извлекает модели устройств (принтеры/МФУ/плоттеры) из родного описания и <name>;
   • раскрывает слэш-цепочки (CM1100ADW/CM1100ADN/…);
   • приклеивает бренд (Pantum/HP/Konica Minolta/…);
   • отсекает коды расходников (TN-…, DR-…, CE285A и т.п.);
   • выводит как:
       <p><strong>Совместимость:</strong></p>
       <ul><li>Pantum CM1100</li> …</ul>
4) Остальные теги/порядок внутри <offer> НЕ трогает.

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

# ===== Верхний фиксированный блок (как у тебя) =====
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

# ===== I/O (cp1251 safe) =====
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

# ===== Поиск внутри оффера/описания =====
OFFER_RX      = re.compile(r"<offer\b[^>]*>.*?</offer>",              re.I|re.S)
DESC_RX       = re.compile(r"<description\b[^>]*>(.*?)</description>", re.I|re.S)
NAME_RX       = re.compile(r"<name\b[^>]*>(.*?)</name>",               re.I|re.S)
HR_RX         = re.compile(r"<hr\b[^>]*>", re.I)

def normsp(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").replace("\u00A0"," ")).strip()

# ===== «ТЕХНИЧЕСКИЕ ХАРАКТЕРИСТИКИ» (оставляем как у тебя) =====
TECH_HDR_RX = re.compile(r"(?i)\bтехническ\w*\s+характеристик[аи]\s*:\s*")
DASH_SPLIT  = re.compile(r"\s[-–—]\s")
KV_RX       = re.compile(r"^\s*([^:]{1,120}?)\s*:\s*(.+?)\s*$", re.S)

def kv_li(line: str) -> str:
    m = KV_RX.match(line.strip())
    if m:
        key = esc(normsp(m.group(1)))
        val = esc(normsp(m.group(2)))
        return f"<li><strong>{key}:</strong> {val}</li>"
    return f"<li>{esc(normsp(line))}</li>"

def to_ul_from_dashed(text_after_header: str) -> str:
    parts = [p for p in DASH_SPLIT.split(text_after_header) if p.strip()]
    if len(parts) < 2:
        return ""
    items = "\n".join(kv_li(p) for p in parts)
    return "<ul>\n" + items + "\n</ul>"

def normalize_tech_block(html_tail: str) -> str:
    if re.search(r"(?is)<p[^>]*>\s*<strong>\s*техническ\w*\s+характеристик[аи]\s*:\s*</strong>\s*</p>\s*<ul>.*?</ul>", html_tail):
        return html_tail
    def para_repl(m: re.Match) -> str:
        start, body, end = m.group(1), m.group(2), m.group(3)
        if not TECH_HDR_RX.search(body): return m.group(0)
        after = re.split(TECH_HDR_RX, body, maxsplit=1)[-1].strip()
        ul = to_ul_from_dashed(after)
        if not ul: return m.group(0)
        return start + "<strong>Технические характеристики:</strong>" + end + "\n" + ul
    return re.sub(r"(<p[^>]*>)(.*?)(</p>)", para_repl, html_tail, flags=re.S|re.I)

# ===== «СОВМЕСТИМОСТЬ» =====
# нормализация похожих букв (кириллица→латиница) для корректной CM/CP
LOOKALIKE = str.maketrans({
    "А":"A","В":"B","С":"C","Е":"E","Н":"H","К":"K","М":"M","О":"O","Р":"P","Т":"T","Х":"X","У":"Y",
    "а":"a","в":"b","с":"c","е":"e","н":"h","к":"k","м":"m","о":"o","р":"p","т":"t","х":"x","у":"y",
})
def latinize(s: str) -> str: return (s or "").translate(LOOKALIKE)

# ключевые подписи, откуда берём список моделей
TARGET_KEYS = (
    "совместимость с моделями",
    "совместимость",
    "принтеры",
    "подходит к",
    "подходит для",
)

# разделители значений
SEPS_RE  = re.compile(r"[;,]|\s+\bи\b\s+", re.I)
MODEL_RE = re.compile(r"\b([A-Z]{1,4}-?[A-Z]?\d{2,6}[A-Z]?(?:-[A-Z0-9]{1,4})?)\b", re.I)

# паттерны КАРТРИДЖЕЙ/ДРАМОВ (исключаем из списка совместимости)
CARTRIDGE_PATTERNS = (
    r"(?:CE|CF|CB|CC|Q)[0-9]{2,4}[A-Z]",       # CE285A, CF226X ...
    r"(?:CRG)[- ]?\d{2,4}",                    # CRG-725
    r"(?:TN|TK|TKC|TNP|TNR)[- ]?\d{2,5}[A-Z]?",# TN-210K, TK-1170
    r"(?:DR|DK|DV)[- ]?\d{2,5}[A-Z]?",         # DR-2300, DK-1110
    r"(?:CN|AR|MX|JL)[- ]?\d{2,5}[A-Z]?",      # вендорные
)
CARTRIDGE_RE = re.compile(rf"(?i)\b({'|'.join(CARTRIDGE_PATTERNS)})\b")

BRANDS = (
    "HP","Hewlett Packard","Canon","Epson","Brother","Kyocera","Samsung",
    "Ricoh","Xerox","Sharp","Lexmark","OKI","Panasonic","Konica Minolta","Pantum"
)
BRAND_RX = re.compile(r"(?i)\b(" + "|".join([re.escape(b) for b in BRANDS]) + r")\b")

KV_SPLIT = re.compile(r"\s[-–—]\s")
KEY_RX   = re.compile(r"^\s*([^:]{1,120}?)\s*:\s*(.+?)\s*$", re.S)

def html_to_plain(html: str) -> str:
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

def split_series(value: str) -> list[str]:
    out = []
    for chunk in SEPS_RE.split(value):
        c = normsp(chunk)
        if c: out.append(latinize(c))
    return out

def expand_slashes(series: str) -> list[str]:
    if "/" not in series: return [series]
    parts = [p for p in series.split("/") if p]
    # восстановим общий префикс (Pantum CM…, bizhub C…, LJ M…)
    first = normsp(parts[0])
    m = re.match(r"^(.*?)[A-Za-z]*\d", first)
    prefix = normsp(m.group(1) or "") if m else ""
    out = [normsp(parts[0])]
    for p in parts[1:]:
        p = normsp(p)
        out.append((prefix + p).strip() if prefix and not p.lower().startswith(prefix.lower()) else p)
    return out

def extract_models_from_text(text: str) -> list[str]:
    models = []
    for chunk in split_series(text):
        for unit in expand_slashes(chunk):
            for m in MODEL_RE.finditer(unit):
                models.append(m.group(1).upper())
    return models

def extract_brand_hint(text: str) -> str:
    m = BRAND_RX.search(latinize(text))
    return (m.group(1) if m else "").strip()

def collect_compat_models(desc_inner_html: str, name_text: str) -> list[str]:
    # работаем по «низу» (после <hr>) если он есть
    parts = HR_RX.split(desc_inner_html, maxsplit=1)
    tail_html = parts[1] if len(parts) == 2 else desc_inner_html
    flat = html_to_plain(tail_html)

    brand = extract_brand_hint(name_text + " " + flat)

    # 1) KV-пары «Совместимость/Принтеры/Подходит …»
    kv = parse_kv_pairs(flat)
    raw = []
    for key, val in kv.items():
        if any(key.startswith(k) for k in TARGET_KEYS):
            raw.extend(extract_models_from_text(val))

    # 2) Дополнительно из <name>
    raw.extend(extract_models_from_text(name_text))

    # 3) Удаляем коды расходников (оставляем устройства)
    raw = [x for x in raw if not CARTRIDGE_RE.search(x)]

    # 4) Приклеиваем бренд
    def glue_brand(model: str) -> str:
        if BRAND_RX.search(model):  # бренд уже есть
            return model
        return (brand + " " + model).strip() if brand else model

    full = [glue_brand(m) for m in raw]

    # Дедуп + фильтр «слишком мало» (минимум 2)
    seen, out = set(), []
    for s in full:
        k = normsp(s)
        if k and k not in seen:
            seen.add(k); out.append(k)
    return out if len(out) >= 2 else []

def render_compat_block(models: list[str]) -> str:
    lis = "\n".join(f"<li>{esc(m)}</li>" for m in models)
    return "<p><strong>Совместимость:</strong></p>\n<ul>\n" + lis + "\n</ul>"

def has_compat_block(html: str) -> bool:
    return re.search(r"(?i)<strong>\s*совместим\w*:\s*</strong>", html) is not None

# ===== Сборка DESCRIPTION =====
def ensure_header(desc_inner: str) -> str:
    if "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_inner:
        return desc_inner
    return HEADER_HTML + ("\n\n<hr>\n\n" + desc_inner if desc_inner.strip() else "")

def rebuild_with_header_and_compat(desc_inner: str, name_text: str) -> str:
    parts = HR_RX.split(desc_inner, maxsplit=1)
    if len(parts) == 2:
        head, tail = parts[0], parts[1]
        # Характеристики — оставляем как у тебя (только если были «в линию», аккуратно разложим)
        tail = normalize_tech_block(tail)
        # Совместимость
        if not (has_compat_block(tail) and len(re.findall(r"(?is)<li\b", tail)) >= 2):
            models = collect_compat_models(desc_inner, name_text)
            if models:
                compat_html = render_compat_block(models)
                # добавим перед закрывающим </div> «низа», если он есть
                tail = re.sub(r"(?is)</div>\s*$", "\n" + compat_html + "\n</div>", tail, count=1) or (tail + "\n" + compat_html)
        return "<description>" + head + "<hr>\n\n" + tail + "</description>"
    else:
        body = normalize_tech_block(desc_inner)
        if not (has_compat_block(body) and len(re.findall(r"(?is)<li\b", body)) >= 2):
            models = collect_compat_models(desc_inner, name_text)
            if models:
                body = body.rstrip() + ("\n" if not body.rstrip().endswith("\n") else "") + render_compat_block(models)
        return "<description>" + body + "</description>"

def add_description_if_missing(offer_xml: str) -> str:
    if re.search(r"<description\b", offer_xml, flags=re.I): return offer_xml
    m = re.search(r"\n([ \t]+)<", offer_xml); indent = m.group(1) if m else "  "
    ins = f"\n{indent}<description>{HEADER_HTML}</description>"
    tail = (ins + "\n" + indent[:-2] + "</offer>") if len(indent) >= 2 else (ins + "\n</offer>")
    return offer_xml.replace("</offer>", tail)

# ===== Основной проход =====
def process_offer(offer_xml: str) -> str:
    name_text = ""
    mname = NAME_RX.search(offer_xml)
    if mname and mname.group(1):
        name_text = normsp(re.sub(r"<[^>]+>", " ", mname.group(1)))

    def _desc_repl(m: re.Match) -> str:
        inner = ensure_header(m.group(1))
        return rebuild_with_header_and_compat(inner, name_text)

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
    print(f"[seo] OK: {DST} — добавлен блок «Совместимость» (список устройств), остальное без изменений")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
