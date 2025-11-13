
# coding: utf-8
# build_alstyle.py — v107
# Изменения v107: в CTA заменён эмодзи 💬 на HTML-энтити &#128172; и символ ₸ на &#8376; (чтобы корректно сохранялось в CP1251).

import os, re, html, sys, time, hashlib
from pathlib import Path
import requests

# === precompiled regexes (hot paths) ===
RX_OFFER_BLOCK = re.compile(r'(?is)<offer\b.*?</offer>')
RX_PARAM_BLOCK = re.compile(r'(?is)<\s*param\b[^>]*>.*?</\s*param\s*>')
RX_CATEGORY_ID = re.compile(r'(?is)<\s*categoryId\s*>\s*(\d+)\s*</\s*categoryId\s*>')
RX_PARAM_KV = re.compile(r'(?is)<\s*param\b[^>]*\bname\s*=\s*"([^"]+)"[^>]*>(.*?)</\s*param\s*>')

# --- spacing helper (always present) ---
def _ensure_footer_spacing(out_text: str) -> str:
    # Переносы внизу: 2 NL перед </offers>, перенос перед </shop> и </yml_catalog>.
    out_text = re.sub(r'</offer>[ \t]*(?:\r?\n){0,10}[ \t]*(?=</offers>)', '</offer>\n\n', out_text, count=1)
    out_text = re.sub(r'([^\n])[ \t]*</shop>', r'\1\n</shop>', out_text, count=1)
    out_text = re.sub(r'([^\n])[ \t]*</yml_catalog>', r'\1\n</yml_catalog>', out_text, count=1)
    return out_text

print('[VER] build_alstyle v107 (CTA entity) (tidy+kv+deny+whitespace) (helper present) (footer helper + fast count) (precompiled+price-swap+source_total fast) (FEED_META + 2NL last </offer> + guards) params-sorted + attr-order fix')

# --- Secrets via env (fallback оставлен для локалки) ---
LOGIN = os.getenv('ALSTYLE_LOGIN', 'info@complex-solutions.kz')
PASSWORD = os.getenv('ALSTYLE_PASSWORD', 'Aa123456')

# --- Константы для описаний и форматирования ---
GOAL = 1000       # целевая длина описания
GOAL_LOW = 900    # минимально приемлемая
MAX_HARD = 1200   # жёсткий потолок (по предложениям)
LMAX = 220        # макс длина строки для «умного» <br>
MAX_BR = 3        # максимум переносов

# --- Фильтр категорий поставщика (по <categoryId>) ---
ALLOW_CATS = set(map(str, [
  3540, 3541, 3542, 3543, 3544, 3545, 3566, 3567, 3569, 3570,
  3580, 3688, 3708, 3721, 3722, 4889, 4890, 4895, 5017, 5075,
  5649, 5710, 5711, 5712, 5713, 21279, 21281, 21291, 21356, 21367,
  21368, 21369, 21370, 21371, 21372, 21451, 21498, 21500, 21501,
  21572, 21573, 21574, 21575, 21576, 21578, 21580, 21581, 21583, 21584,
  21585, 21586, 21588, 21591, 21640, 21664, 21665, 21666, 21698
]))

# --- Чёрный список параметров ---
DENY_PARAMS = {s.lower() for s in [
  "Артикул", "Благотворительность", "Код ТН ВЭД", "Код товара Kaspi",
  "Новинка", "Снижена цена", "Штрихкод", "Штрих-код", "Назначение",
  "Объем", "Объём"
]}

# --- Стандартный CTA-блок (с HTML-сущностями для эмодзи и тенге) ---
CTA_HTML = '''<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;">
  <p style="text-align:center; margin:0 0 12px;">
    <a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0"
       style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,.08);">
      &#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!
    </a>
  </p>

  <div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;">
    <h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3>
    <ul style="margin:0; padding-left:18px;">
      <li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li>
      <li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li>
    </ul>

    <hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;">

    <h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3>
    <ul style="margin:0; padding-left:18px;">
      <li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li>
      <li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 &#8376; | 3–7 рабочих дней</em></li>
      <li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>
      <li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li>
    </ul>
  </div>
</div>'''

# --- Утилиты текста ---
_re_tag = re.compile(r'(?is)<[^>]+>')
def _clean_plain(txt: str) -> str:
    # HTML → текст
    for _ in range(2):
        nt = html.unescape(txt)
        if nt == txt: break
        txt = nt
    txt = txt.replace('\u00A0', ' ')
    txt = re.sub(r'[\u200B-\u200D\uFEFF]', '', txt)
    txt = re.sub(r'\r\n|\r|\n', ' ', txt)
    txt = _re_tag.sub(' ', txt)
    txt = re.sub(r'\s+', ' ', txt).strip()
    return txt

def _sentences(plain: str):
    parts = re.split(r'(?<=[\.\!\?])\s+|;\s+', plain)
    return [p.strip() for p in parts if p.strip()]

def _build_desc_text(plain: str) -> str:
    # Сжатие до ~1000 по предложениям
    if len(plain) <= GOAL:
        return plain
    parts = _sentences(plain)
    selected, total = [], 0
    if parts:
        selected.append(parts[0]); total = len(parts[0])
    for p in parts[1:]:
        add = (1 if total else 0) + len(p)
        if total + add > MAX_HARD: break
        selected.append(p); total += add
        if total >= GOAL_LOW: break
    if total < GOAL_LOW:
        for p in parts[len(selected):]:
            add = (1 if total else 0) + len(p)
            if total + add > MAX_HARD: break
            selected.append(p); total += add
            if total >= GOAL_LOW: break
    return ' '.join(selected).strip()

# --- Цена ---
def _price_adders(base: int) -> int:
    if 101 <= base <= 10_000: return 3_000
    elif 10_001 <= base <= 25_000: return 4_000
    elif 25_001 <= base <= 50_000: return 5_000
    elif 50_001 <= base <= 75_000: return 7_000
    elif 75_001 <= base <= 100_000: return 10_000
    elif 100_001 <= base <= 150_000: return 12_000
    elif 150_001 <= base <= 200_000: return 15_000
    elif 200_001 <= base <= 300_000: return 20_000
    elif 300_001 <= base <= 400_000: return 25_000
    elif 400_001 <= base <= 500_000: return 30_000
    elif 500_001 <= base <= 750_000: return 40_000
    elif 750_001 <= base <= 1_000_000: return 50_000
    elif 1_000_001 <= base <= 1_500_000: return 70_000
    elif 1_500_001 <= base <= 2,000,000: return 90_000
    elif 2_000_001 <= base <= 100_000_000: return 100_000
    else: return 0

def _retail_price_from_base(base: int) -> int:
    if base >= 9_000_000: return 100
    add = _price_adders(base)
    tmp = int(base * 1.04 + add + 0.9999)
    thousands = (tmp + 999) // 1000
    retail = thousands * 1000 - 100
    if retail % 1000 != 900:
        retail = (retail // 1000 + 1) * 1000 - 100
    return max(retail, 900)

# --- Параметры ---
def _collect_params(block: str):
    out = []
    for name, val in re.findall(r'(?is)<\s*param\b[^>]*\bname\s*=\s*"([^"]+)"[^>]*>(.*?)</\s*param\s*>', block):
        key = _clean_plain(name).strip(': ')
        if not key or key.lower() in DENY_PARAMS: 
            continue
        vv = _clean_plain(val)
        if not vv: continue
        key = key[:1].upper() + key[1:]
        out.append((key, vv))
    return out

PRIOR_KEYS = ['Диагональ экрана','Яркость','Операционная система','Объем встроенной памяти',
              'Память','Точек касания','Интерфейсы','Вес','Размеры']

def _sort_params(params):
    def _pkey(item):
        k = item[0]
        try: return (0, PRIOR_KEYS.index(k))
        except ValueError: return (1, k.lower())
    return sorted(params, key=_pkey)

# --- Перенос <available> в атрибут offer ---
def _move_available_attr(header: str, body: str):
    m = re.search(r'(?is)<\s*available\s*>\s*(true|false)\s*</\s*available\s*>', body)
    if not m: 
        return header, body
    avail = m.group(1)
    # удалить тег <available>…</available> из body
    body = re.sub(r'(?is)<\s*available\s*>.*?</\s*available\s*>', '', body, count=1)
    # если атрибут уже есть — обновим на месте
    if re.search(r'(?is)\bavailable\s*=\s*"(?:true|false)"', header):
        header = re.sub(r'(?is)\bavailable\s*=\s*"(?:true|false)"', f'available="{avail}"', header, count=1)
    else:
        # иначе добавим перед закрывающей '>' — так сохраняем исходный порядок id и прочих атрибутов
        header = re.sub(r'>\s*$', f' available="{avail}">', header, count=1)
    return header, body

# --- Удаление простых тегов ---
FORBIDDEN_TAGS = ('url','quantity','quantity_in_stock','purchase_price')
def _remove_simple_tags(body: str) -> str:
    for t in FORBIDDEN_TAGS:
        body = re.sub(rf'(?is)<\s*{t}\s*>.*?</\s*{t}\s*>', '', body)
    body = re.sub(r'[ \t]+\n', '\n', body)
    body = re.sub(r'\n{3,}', '\n\n', body)
    return body.strip()

# --- Fallback: создать <price> из <purchase_price> если <price> отсутствует ---
def _ensure_price_from_purchase(body: str) -> str:
    if re.search(r'(?is)<\s*price\s*>', body): 
        return body
    m = re.search(r'(?is)<\s*purchase_price\s*>\s*(.*?)\s*</\s*purchase_price\s*>', body)
    if not m: return body
    digits = re.sub(r'[^\d]', '', m.group(1))
    if not digits: return body
    tag = f'<price>{digits}</price>'
    m2 = re.search(r'(?is)<\s*currencyId\s*>', body)
    if m2: return body[:m2.start()] + tag + body[m2.start():]
    m3 = re.search(r'(?is)</\s*name\s*>', body)
    if m3: return body[:m3.end()] + tag + body[m3.end():]
    m4 = re.search(r'(?is)</\s*offer\s*>', body)
    if m4: return body[:m4.start()] + tag + body[m4.start():]
    return body

# --- Перестройка описания: добавляем CTA_HTML в начало без изменений ---
def _desc_postprocess_native_specs(offer_xml: str) -> str:
    # 1) достаём исходный <description>
    m = re.search(r'(?is)(<\s*description\b[^>]*>)(.*?)(</\s*description\s*>)', offer_xml)
    head, raw, tail = (m.group(1), m.group(2), m.group(3)) if m else ('<description>', '', '</description>')

    # 2) нормализуем «родной» текст
    plain_full = _clean_plain(raw)
    desc_text = _build_desc_text(plain_full)

    # 3) заголовок из <name>
    mname = re.search(r'(?is)<\s*name\s*>\s*(.*?)\s*</\s*name\s*>', offer_xml)
    name_h3 = ''
    if mname:
        nm = _clean_plain(mname.group(1))
        if nm: name_h3 = '<h3>' + html.escape(nm) + '</h3>'

    # 4) основной абзац (умный <br> только для длинных исходников)
    if len(plain_full) > GOAL:
        parts = _sentences(desc_text)
        lines, cur = [], ''
        for s in parts:
            cand = (cur + (' ' if cur else '') + s)
            if cur and len(cand) > LMAX and len(lines) < MAX_BR:
                lines.append(cur); cur = s
            else:
                cur = cand
        if cur: lines.append(cur)
        if len(lines) > MAX_BR + 1:
            head_lines = lines[:MAX_BR]
            tail_line = ' '.join(lines[MAX_BR:])
            lines = head_lines + [tail_line]
        desc_html = '<br>' + '<br>'.join(html.escape(x) for x in lines) if lines else ''
        if not desc_html:
            desc_html = html.escape(desc_text)
    else:
        desc_html = html.escape(desc_text)

    # 5) характеристики из <param>
    params = _collect_params(offer_xml)
    params = _sort_params(params)

    # 6) Сборка HTML: СНАЧАЛА CTA, затем name/описание/характеристики
    blocks = []
    blocks.append(CTA_HTML)  # << неизменяемый блок
    if name_h3: blocks.append(name_h3)
    blocks.append('<p>' + desc_html + '</p>')
    if params:
        blocks.append('<h3>Характеристики</h3>')
        ul = '<ul>' + ''.join(f'<li><strong>{html.escape(k)}:</strong> {html.escape(v)}</li>' for k,v in params) + '</ul>'
        blocks.append(ul)

    new_html = ''.join(blocks)
    if m:
        return offer_xml[:m.start(1)] + head + new_html + tail + offer_xml[m.end(3):]
    else:
        insert_at = re.search(r'(?is)</\s*currencyId\s*>', offer_xml)
        ins = insert_at.end() if insert_at else len(offer_xml)
        return offer_xml[:ins] + '<description>' + new_html + '</description>' + offer_xml[ins:]

# --- Сортировка тегов и сбор оффера ---
WANT_ORDER = ('categoryId','vendorCode','name','price','picture','vendor','currencyId','description','param')

def _swap_price_nodes(xml: str) -> str:
    # Меняет местами значения узлов <price> и <purchase_price> в пределах одного offer за 1 проход.
    xml = re.sub(r'(?is)<\s*price\s*>', '<_TMP_PRICE_>', xml)
    xml = re.sub(r'(?is)</\s*price\s*>', '</_TMP_PRICE_>', xml)
    xml = re.sub(r'(?is)<\s*purchase_price\s*>', '<_TMP_PPRICE_>', xml)
    xml = re.sub(r'(?is)</\s*purchase_price\s*>', '</_TMP_PPRICE_>', xml)
    xml = xml.replace('<_TMP_PRICE_>', '<purchase_price>')
    xml = xml.replace('</_TMP_PRICE_>', '</purchase_price>')
    xml = xml.replace('<_TMP_PPRICE_>', '<price>')
    xml = xml.replace('</_TMP_PPRICE_>', '</price>')
    return xml

def _rebuild_offer(offer_xml: str) -> str:
    m = re.match(r'(?is)^\s*(<offer\b[^>]*>)(.*)</offer>\s*$', offer_xml)
    if not m: return offer_xml.strip() + '\n\n'
    header, body = m.group(1), m.group(2)

    header, body = _move_available_attr(header, body)
    body = _ensure_price_from_purchase(body)

    # price ← purchase_price
    mp = re.search(r'(?is)<\s*purchase_price\s*>\s*(.*?)\s*</\s*purchase_price\s*>', body)
    if mp:
        val = mp.group(1)
        if re.search(r'(?is)<\s*price\s*>', body):
            body = re.sub(r'(?is)(<\s*price\s*>).*?(</\s*price\s*>)', r'\g<1>'+val+r'\g<2>', body, count=1)
        else:
            body = '<price>'+val+'</price>' + body

    body = _remove_simple_tags(body)

    # vendorCode + id
    mv = re.search(r'(?is)<\s*vendorCode\s*>\s*(.*?)\s*</\s*vendorCode\s*>', body)
    if mv:
        v = _clean_plain(mv.group(1))
    else:
        mi = re.search(r'(?is)\bid="([^"]+)"', header)
        v = mi.group(1) if mi else 'AS' + hashlib.md5(body.encode('utf-8')).hexdigest()[:8].upper()
        body = '<vendorCode>'+html.escape(v)+'</vendorCode>' + body
    if not v.startswith('AS'):
        v_new = 'AS' + v
        body = re.sub(r'(?is)(<\s*vendorCode\s*>\s*).*?(\s*</\s*vendorCode\s*>)', r'\g<1>'+html.escape(v_new)+r'\g<2>', body, count=1)
        v = v_new
    header = re.sub(r'(?is)\bid="[^"]*"', f'id="{v}"', header, count=1)
    # fix: убрать лишние пробелы в заголовке <offer ...>
    header = re.sub(r'\s{2,}', ' ', header)

    # цена с наценкой
    mprice = re.search(r'(?is)<\s*price\s*>\s*(.*?)\s*</\s*price\s*>', body)
    if mprice:
        digits = re.sub(r'[^\d]', '', mprice.group(1))
        base = int(digits) if digits else 0
        newp = _retail_price_from_base(base) if base else 0
        body = re.sub(r'(?is)(<\s*price\s*>\s*).*?(\s*</\s*price\s*>)', r'\g<1>'+str(newp)+r'\g<2>', body, count=1)

    full_offer = header + body + '</offer>'
    full_offer = _desc_postprocess_native_specs(full_offer)

    parts = {}
    for t in WANT_ORDER:
        parts[t] = re.findall(rf'(?is)<\s*{t}\b[^>]*>.*?</\s*{t}\s*>', full_offer)
        full_offer = re.sub(rf'(?is)<\s*{t}\b[^>]*>.*?</\s*{t}\s*>', '', full_offer)

    out_lines = []
    for t in ('categoryId','vendorCode','name','price'):
        out_lines += parts.get(t, [])
    for pic in parts.get('picture', []):
        out_lines.append(pic)
    for t in ('vendor','currencyId','description'):
        out_lines += parts.get(t, [])
    for prm in parts.get('param', []):
        mname = re.search(r'(?is)name\s*=\s*"([^"]+)"', prm or '')
        if mname and mname.group(1).strip().lower() in DENY_PARAMS:
            continue
        mname = re.search(r'(?is)<\s*param\b[^>]*\bname\s*=\s*"([^"]+)"', prm)
        if mname:
            nm = re.sub(r'[\s\-]+', ' ', mname.group(1).strip().lower()).replace('ё','е')
            if nm in DENY_PARAMS:
                continue
        out_lines.append(prm)

    out = header + '\n' + '\n'.join(x.strip() for x in out_lines if x.strip()) + '\n</offer>\n\n'
    return out

# --- Главный поток ---
def main() -> int:
    url = 'https://al-style.kz/upload/catalog_export/al_style_catalog.php'
    r = requests.get(url, auth=(LOGIN, PASSWORD), timeout=60)
    r.raise_for_status()
    src = r.content

    try:
        text = src.decode('windows-1251')
    except UnicodeDecodeError:
        text = src.decode('utf-8', errors='replace')

    m = re.search(r'(?is)^(.*?<offers\s*>)(.*?)(</\s*offers\s*>.*)$', text)
    if not m:
        m = re.search(r'(?is)(.*?<offers\s*>)(.*)(</\s*offers\s*>.*)', text)
        if not m:
            raise SystemExit('Не найден блок <offers>')
    head, offers_block, tail = m.group(1), m.group(2), m.group(3)

    head = re.sub(r'(?is)<shop\s*>.*?<offers\s*>', '<shop><offers>', head, count=1)

    offers = RX_OFFER_BLOCK.findall(offers_block)
    kept = []
    for off in offers:
        mcat = RX_CATEGORY_ID.search(off)
        if not mcat or mcat.group(1) not in ALLOW_CATS:
            continue
        kept.append(_rebuild_offer(off))

    new_offers = '\n\n'.join(x.strip() for x in kept)

    # FEED_META: многострочная шапка (Asia/Almaty)
    total = len(kept)
    avail_true = sum('available="true"' in k for k in kept)
    avail_false = sum('available="false"' in k for k in kept)
    source_total = text.lower().count('<offer')
    from datetime import datetime, timedelta
    try:
        from zoneinfo import ZoneInfo
        _tz = ZoneInfo('Asia/Almaty')
        _now_local = datetime.now(_tz)
    except Exception:
        _now_local = datetime.utcnow()
    _next = _now_local.replace(hour=1, minute=0, second=0, microsecond=0)
    if _now_local >= _next:
        _next = (_now_local + timedelta(days=1)).replace(hour=1, minute=0, second=0, microsecond=0)
    def _line(label: str, value) -> str:
        return f"{label:<42} | {value}"
    feed_meta = (
        "<!--FEED_META\n"
        f"{_line('Поставщик', 'AlStyle')}\n"
        f"{_line('URL поставщика', 'https://al-style.kz/upload/catalog_export/al_style_catalog.php')}\n"
        f"{_line('Время сборки (Алматы)', _now_local.strftime('%Y-%m-%d %H:%M:%S'))}\n"
        f"{_line('Ближайшая сборка (Алматы)', _next.strftime('%Y-%m-%d %H:%M:%S'))}\n"
        f"{_line('Сколько товаров у поставщика до фильтра', source_total)}\n"
        f"{_line('Сколько товаров у поставщика после фильтра', total)}\n"
        f"{_line('Сколько товаров есть в наличии (true)', avail_true)}\n"
        f"{_line('Сколько товаров нет в наличии (false)', avail_false)}\n"
        "-->\n\n"
    )
    out_text = head + '\n' + new_offers + '\n' + tail
    out_text = feed_meta + out_text
    out_text = _ensure_footer_spacing(out_text)

    out_text = re.sub(r'[ \t]+\n', '\n', out_text)
    out_text = re.sub(r'\n{3,}', '\n\n', out_text)
    out_text = out_text.replace('<shop><offers>', '<shop><offers>\n')

    Path('docs').mkdir(exist_ok=True)
    Path('docs/alstyle.yml').write_text(out_text, encoding='windows-1251', errors='replace')
    print('OK: docs/alstyle.yml, offers:', len(kept))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
