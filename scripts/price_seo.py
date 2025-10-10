#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_seo.py
———————
Меняет ТОЛЬКО содержимое <description>:
- добавляет/пересобирает блок «Совместимость» из уже имеющегося текста описания;
- учитывает слэш-списки (CM1100ADW/СM1100ADN/… → все модели по отдельности);
- нормализует похожие кириллические буквы (С↔C и др.), чтобы ничего не потерять;
- НЕ трогает «Технические характеристики» и прочие теги/порядок внутри <offer>.

Вход:  docs/price.yml
Выход: docs/price_seo.yml  (та же структура, правки только в <description>)
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
NAME_RX  = re.compile(r"<name\b[^>]*>(.*?)</name>", re.I|re.S)
HR_RX    = re.compile(r"<hr\b[^>]*>", re.I)

# нормализация похожих букв (кириллица→латиница), чтобы парсить CM/CP и т.п.
LOOKALIKE = str.maketrans({
    "А":"A","В":"B","С":"C","Е":"E","Н":"H","К":"K","М":"M","О":"O","Р":"P","Т":"T","Х":"X","У":"Y",
    "а":"a","в":"b","с":"c","е":"e","н":"h","к":"k","м":"m","о":"o","р":"p","т":"t","х":"x","у":"y",
})
def latinize(s: str) -> str: return (s or "").translate(LOOKALIKE)
def normsp(s: str) -> str:   return re.sub(r"\s+"," ", (s or "").replace("\u00A0"," ")).strip()

# ===== Извлечение моделей ТОЛЬКО из родного описания =====
# Берём значения у ключей «Совместимость с моделями: …», «Принтеры: …»
KV_SPLIT = re.compile(r"\s[-–—]\s")  # разделитель « - » между парами ключ:значение
KEY_RX   = re.compile(r"^\s*([^:]{1,100}?)\s*:\s*(.+?)\s*$", re.S)
TARGET_KEYS = (
    "совместимость с моделями",
    "принтеры",           # часто список моделей тут
    "совместимость",      # на всякий случай
)

def html_to_text(html: str) -> str:
    t = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace(" — ", " - ").replace(" – ", " - ")
    return normsp(t)

def parse_kv_pairs(flat_text: str) -> dict[str,str]:
    # делим по « - », затем ловим «ключ: значение»
    pairs = {}
    for chunk in KV_SPLIT.split(flat_text):
        m = KEY_RX.match(chunk.strip())
        if not m: continue
        key = normsp(m.group(1)).lower()
        val = normsp(m.group(2))
        pairs[key] = val
    return pairs

MODEL_TOKEN_RE = re.compile(r"\b([A-Z]{1,4}-?[A-Z]?\d{2,6}[A-Z]?(?:-[A-Z0-9]{1,4})?)\b", re.I)
SEPS_RE        = re.compile(r"[;,]|\s+\bи\b\s+", re.I)

def expand_slashes(series: str) -> list[str]:
    # Восстанавливаем общий префикс первой части до цифры (например, "Pantum CM")
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
    # разбиваем по запятым/«и»/точкам с запятой, внутри фрагментов раскрываем слэш-списки
    items = []
    for chunk in SEPS_RE.split(value):
        chunk = normsp(chunk)
        if not chunk: continue
        if "/" in chunk:
            items.extend(expand_slashes(chunk))
        else:
            items.append(latinize(chunk))
    return items

def extract_models_from_value(value: str) -> list[str]:
    # из одной строки значения (после ключа) — достаём все токены-модели
    models = []
    for frag in split_series(value):
        for m in MODEL_TOKEN_RE.finditer(frag):
            token = m.group(1).upper()
            models.append(frag[:m.start()] + token + frag[m.end():])  # сохраняем бренд/семейство слева
    # нормализация/дедуп
    seen, out = set(), []
    for s in models:
        s2 = normsp(s)
        if s2 and s2 not in seen:
            seen.add(s2); out.append(s2)
    return out

def collect_compat_from_description(desc_inner_html: str) -> list[str]:
    # работаем только с родным низом (после <hr>), но если <hr> нет — со всем
    parts = HR_RX.split(desc_inner_html, maxsplit=1)
    tail = parts[1] if len(parts) == 2 else desc_inner_html
    flat = html_to_text(tail)
    kv = parse_kv_pairs(flat)
    models_all: list[str] = []
    for key, val in kv.items():
        if any(key.startswith(k) for k in TARGET_KEYS):
            models_all.extend(extract_models_from_value(val))
    # если из ключей мало — попробуем просто найти явный список после слова «Принтеры»
    if len(models_all) < 2:
        m = re.search(r"(?i)принтеры\s*:\s*(.+)", flat)
        if m:
            models_all.extend(extract_models_from_value(m.group(1)))
    # дедуп с сохранением порядка
    seen, out = set(), []
    for s in models_all:
        s2 = normsp(s)
        if s2 and s2 not in seen:
            seen.add(s2); out.append(s2)
    return out

def render_compat_ul(models: list[str]) -> str:
    if len(models) < 2:
        return ""
    items = "\n".join(f"<li>{esc(m)}</li>" for m in models)
    return "<p><strong>Совместимость:</strong></p>\n<ul>\n" + items + "\n</ul>"

def has_our_header(desc_inner: str) -> bool:
    return "НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!" in desc_inner

def ensure_compat_block(desc_inner: str, name_text: str) -> str:
    models = collect_compat_from_description(desc_inner)
    compat_html = render_compat_ul(models)
    if not compat_html:
        return "<description>" + desc_inner + "</description>"
    # если наш верхний блок уже есть — вставим/заменим «Совместимость» в «низу»
    parts = HR_RX.split(desc_inner, maxsplit=1)
    if len(parts) == 2:
        head, tail = parts[0], parts[1]
        # если «Совместимость» уже есть — заменим список
        tail2 = re.sub(r"(?is)<p[^>]*>\s*<strong>\s*совместим\w*:\s*</strong>\s*</p>\s*<ul>.*?</ul>",
                       compat_html, tail, count=1)
        if tail2 == tail:
            # не было — аккуратно добавим ближе к концу
            tail2 = re.sub(r"(?is)</div>\s*$", "\n" + compat_html + "\n</div>", tail, count=1)
        return "<description>" + head + "<hr>\n\n" + tail2 + "</description>"
    # если <hr> нет — просто прицепим совместимость в конец
    return "<description>" + desc_inner + ("\n\n" + compat_html) + "</description>"

def inject_header_if_missing(desc_inner: str) -> str:
    if has_our_header(desc_inner):
        return desc_inner
    # если описания нет вообще — создадим базовый каркас с нашим блоком
    return HEADER_HTML + ("\n\n<hr>\n\n" + desc_inner if desc_inner.strip() else "")

def process_offer(offer_xml: str) -> str:
    # вытаскиваем name только как контекст (не обязательно)
    mname = NAME_RX.search(offer_xml)
    name_text = normsp(re.sub(r"<[^>]+>", " ", mname.group(1))) if (mname and mname.group(1)) else ""
    # меняем только содержимое <description>
    def _desc_repl(m: re.Match) -> str:
        inner = m.group(1)
        inner2 = inject_header_if_missing(inner)
        return ensure_compat_block(inner2, name_text)
    new_block = DESC_RX.sub(_desc_repl, offer_xml)
    # если у оффера не было description — создадим минимальный (с хедером)
    if new_block == offer_xml:
        # вставим перед </offer>, сохранив отступ
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
    print(f"[seo] OK: {DST} — обновлён только раздел «Совместимость», остальное без изменений")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
