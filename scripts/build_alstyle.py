
# coding: utf-8
# build_alstyle.py — v108 (base v105 tidy preserved) + WhatsApp inject (HTML entity) + </u> fix

import os, re, html, hashlib
from pathlib import Path
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

import requests

print('[VER] build_alstyle v108 (base v105 + whatsapp inject only, entity bubble, </u> fix)')

# --- Credentials ---
LOGIN = os.getenv('ALSTYLE_LOGIN', 'info@complex-solutions.kz')
PASSWORD = os.getenv('ALSTYLE_PASSWORD', 'Aa123456')

# --- Constants ---
GOAL = 1000
GOAL_LOW = 900
MAX_HARD = 1200
LMAX = 220
MAX_BR = 3

ALLOW_CATS = {str(x) for x in [
  3540, 3541, 3542, 3543, 3544, 3545, 3566, 3567, 3569, 3570,
  3580, 3688, 3708, 3721, 3722, 4889, 4890, 4895, 5017, 5075,
  5649, 5710, 5711, 5712, 5713, 21279, 21281, 21291, 21356, 21367,
  21368, 21369, 21370, 21371, 21372, 21451, 21498, 21500, 21501,
  21572, 21573, 21574, 21575, 21576, 21578, 21580, 21581, 21583, 21584,
  21585, 21586, 21588, 21591, 21640, 21664, 21665, 21666, 21698
]}

DENY_PARAMS = {s.lower() for s in [
  "Артикул", "Благотворительность", "Код ТН ВЭД", "Код товара Kaspi",
  "Новинка", "Снижена цена", "Штрихкод", "Штрих-код", "Назначение",
  "Объем", "Объём"
]}

# === Helpers ===
_re_tag = re.compile(r'(?is)<[^>]+>')

def _clean_plain(txt: str) -> str:
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
    if len(plain) <= GOAL: return plain
    parts = _sentences(plain)
    if not parts: return plain[:GOAL]
    selected, total = [], 0
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

# === Pricing ===
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
    elif 1_500_001 <= base <= 2_000_000: return 90_000
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

# === Params ===
def _collect_params(block: str):
    out = []
    for name, val in re.findall(r'(?is)<\s*param\b[^>]*\bname\s*=\s*"([^"]+)"[^>]*>(.*?)</\s*param\s*>', block):
        key = _clean_plain(name).strip(': ')
        if not key or key.lower() in DENY_PARAMS: continue
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

# === available → header ===
def _move_available_attr(header: str, body: str):
    m = re.search(r'(?is)<\s*available\s*>\s*(true|false)\s*</\s*available\s*>', body)
    if not m: return header, body
    avail = m.group(1)
    body = re.sub(r'(?is)<\s*available\s*>.*?</\s*available\s*>', '', body, count=1)
    if re.search(r'(?is)\bavailable\s*=\s*"(?:true|false)"', header):
        header = re.sub(r'(?is)\bavailable\s*=\s*"(?:true|false)"', f'available="{avail}"', header, count=1)
    else:
        header = re.sub(r'>\s*$', f' available="{avail}">', header, count=1)
    return header, body

FORBIDDEN_TAGS = ('url','quantity','quantity_in_stock','purchase_price')

def _remove_simple_tags(body: str) -> str:
    for t in FORBIDDEN_TAGS:
        body = re.sub(rf'(?is)<\s*{t}\s*>.*?</\s*{t}\s*>', '', body)
    body = re.sub(r'[ \t]+\n', '\n', body)
    body = re.sub(r'\n{3,}', '\n\n', body)
    return body.strip()

def _ensure_price_from_purchase(body: str) -> str:
    if re.search(r'(?is)<\s*price\s*>', body): return body
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

def _desc_postprocess_native_specs(offer_xml: str) -> str:
    m = re.search(r'(?is)(<\s*description\b[^>]*>)(.*?)(</\s*description\s*>)', offer_xml)
    head, raw, tail = (m.group(1), m.group(2), m.group(3)) if m else ('<description>', '', '</description>')
    plain_full = _clean_plain(raw)
    desc_text = _build_desc_text(plain_full)

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
        desc_html = '<br>'.join(html.escape(x) for x in lines)
    else:
        desc_html = html.escape(desc_text)

    mname = re.search(r'(?is)<\s*name\s*>\s*(.*?)\s*</\s*name\s*>', offer_xml)
    name_h3 = ''
    if mname:
        nm = _clean_plain(mname.group(1))
        if nm: name_h3 = '<h3>' + html.escape(nm) + '</h3>'

    params = _collect_params(offer_xml)
    params = _sort_params(params)

    blocks = []
    if name_h3: blocks.append(name_h3)
    blocks.append('<p>' + desc_html + '</p>')
    if params:
        blocks.append('<h3>Характеристики</h3>')
        ul = '<ul>' + ''.join(f'<li><strong>{html.escape(k)}:</strong> {html.escape(v)}</li>' for k, v in params) + '</ul>'
        blocks.append(ul)

    new_html = ''.join(blocks)
    if m:
        return offer_xml[:m.start(1)] + head + new_html + tail + offer_xml[m.end(3):]
    else:
        insert_at = re.search(r'(?is)</\s*currencyId\s*>', offer_xml)
        if not insert_at: insert_at = re.search(r'(?is)</\s*name\s*>', offer_xml)
        ins = insert_at.end() if insert_at else len(offer_xml)
        return offer_xml[:ins] + '<description>' + new_html + '</description>' + offer_xml[ins:]

# WhatsApp block — fixed </u>
WHATSAPP_BLOCK = """<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;">
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
      <li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li>
      <li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>
      <li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li>
    </ul>
  </div>
</div>

"""

def _inject_whatsapp_block(offer_xml: str) -> str:
    if 'НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!' in offer_xml:
        return offer_xml
    m = re.search(r'(?is)(<\s*description\b[^>]*>)(.*?)(</\s*description\s*>)', offer_xml)
    if not m: return offer_xml
    head, body, tail = m.group(1), m.group(2), m.group(3)
    new_body = WHATSAPP_BLOCK + body
    return offer_xml[:m.start(1)] + head + new_body + tail + offer_xml[m.end(3):]

WANT_ORDER = ('categoryId','vendorCode','name','price','picture','vendor','currencyId','description','param')

def _rebuild_offer(offer_xml: str) -> str:
    m = re.match(r'(?is)^\s*(<offer\b[^>]*>)(.*)</offer>\s*$', offer_xml)
    if not m: return offer_xml.strip() + '\n\n'
    header, body = m.group(1), m.group(2)

    header, body = _move_available_attr(header, body)
    body = _ensure_price_from_purchase(body)

    mp = re.search(r'(?is)<\s*purchase_price\s*>\s*(.*?)\s*</\s*purchase_price\s*>', body)
    if mp:
        val = mp.group(1)
        if re.search(r'(?is)<\s*price\s*>', body):
            body = re.sub(r'(?is)(<\s*price\s*>\s*).*(\s*</\s*price\s*>)', r'\g<1>' + val + r'\g<2>', body, count=1)
        else:
            body = '<price>' + val + '</price>' + body

    body = _remove_simple_tags(body)

    mv = re.search(r'(?is)<\s*vendorCode\s*>\s*(.*?)\s*</\s*vendorCode\s*>', body)
    if mv:
        v = _clean_plain(mv.group(1))
    else:
        mi = re.search(r'(?is)\bid="([^"]+)"', header)
        v = mi.group(1) if mi else 'AS' + hashlib.md5(body.encode('utf-8')).hexdigest()[:8].upper()
        body = '<vendorCode>' + html.escape(v) + '</vendorCode>' + body
    if not v.startswith('AS'):
        v = 'AS' + v
        body = re.sub(r'(?is)(<\s*vendorCode\s*>\s*).*?(\s*</\s*vendorCode\s*>)', r'\g<1>' + html.escape(v) + r'\g<2>', body, count=1)
    header = re.sub(r'(?is)\bid="[^"]*"', f'id="{v}"', header, count=1)

    header = re.sub(r'\s{2,}', ' ', header)

    mprice = re.search(r'(?is)<\s*price\s*>\s*(.*?)\s*</\s*price\s*>', body)
    if mprice:
        digits = re.sub(r'[^\d]', '', mprice.group(1))
        base = int(digits) if digits else 0
        newp = _retail_price_from_base(base) if base else 0
        body = re.sub(r'(?is)(<\s*price\s*>\s*).*?(\s*</\s*price\s*>)', r'\g<1>' + str(newp) + r'\g<2>', body, count=1)

    full_offer = header + body + '</offer>'
    full_offer = _desc_postprocess_native_specs(full_offer)
    full_offer = _inject_whatsapp_block(full_offer)

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
        mname = re.search(r'(?is)\bname\s*=\s*"([^"]+)"', prm or '')
        if mname and mname.group(1).strip().lower() in DENY_PARAMS: continue
        mname = re.search(r'(?is)<\s*param\b[^>]*\bname\s*=\s*"([^"]+)"', prm)
        if mname:
            nm = re.sub(r'[\s\-]+', ' ', mname.group(1).strip().lower()).replace('ё','е')
            if nm in DENY_PARAMS: continue
        out_lines.append(prm)

    out = header + '\n' + '\n'.join(x.strip() for x in out_lines if x.strip()) + '\n</offer>\n\n'
    return out

def _ensure_footer_spacing(out_text: str) -> str:
    out_text = re.sub(r'</offer>[ \t]*(?:\r?\n){0,10}[ \t]*(?=</offers>)', '</offer>\n\n', out_text, count=1)
    out_text = re.sub(r'([^\n])[ \t]*</shop>', r'\1\n</shop>', out_text, count=1)
    out_text = re.sub(r'([^\n])[ \t]*</yml_catalog>', r'\1\n</yml_catalog>', out_text, count=1)
    return out_text

def main() -> int:
    SUPPLIER_URL = 'https://al-style.kz/upload/catalog_export/al_style_catalog.php'
    r = requests.get(SUPPLIER_URL, auth=(LOGIN, PASSWORD), timeout=60)
    r.raise_for_status()
    src = r.content

    try:
        text = src.decode('windows-1251')
    except UnicodeDecodeError:
        text = src.decode('utf-8', errors='replace')

    m = re.search(r'(?is)^(.*?<offers\s*>)(.*?)(</\s*offers\s*>.*)$', text)
    if not m:
        m = re.search(r'(?is)(.*?<offers\s*>)(.*)(</\s*offers\s*>.*)', text)
        if not m: raise SystemExit('Не найден блок <offers>')
    head, offers_block, tail = m.group(1), m.group(2), m.group(3)

    head = re.sub(r'(?is)<shop\s*>.*?<offers\s*>', '<shop><offers>', head, count=1)
    if not head.endswith('\n'): head = head + '\n'

    offers = re.findall(r'(?is)<offer\b.*?</offer>', offers_block)
    kept = []
    for off in offers:
        mcat = re.search(r'(?is)<\s*categoryId\s*>\s*(\d+)\s*</\s*categoryId\s*>', off)
        if not mcat or mcat.group(1) not in ALLOW_CATS: continue
        kept.append(_rebuild_offer(off))

    new_offers = '\n\n'.join(x.strip() for x in kept)

    total = len(kept)
    avail_true = sum('available="true"' in k for k in kept)
    avail_false = sum('available="false"' in k for k in kept)
    source_total = text.lower().count('<offer')
    if ZoneInfo:
        _tz = ZoneInfo('Asia/Almaty'); _now_local = datetime.now(_tz)
    else:
        _now_local = datetime.utcnow()
    _next = _now_local.replace(hour=1, minute=0, second=0, microsecond=0)
    if _now_local >= _next:
        _next = (_now_local + timedelta(days=1)).replace(hour=1, minute=0, second=0, microsecond=0)
    def _line(label: str, value) -> str: return f"{label:<42} | {value}"
    feed_meta = (
        "<!--FEED_META\n"
        f"{_line('Поставщик', 'AlStyle')}\n"
        f"{_line('URL поставщика', SUPPLIER_URL)}\n"
        f"{_line('Время сборки (Алматы)', _now_local.strftime('%Y-%m-%d %H:%M:%S'))}\n"
        f"{_line('Ближайшая сборка (Алматы)', _next.strftime('%Y-%m-%d %H:%M:%S'))}\n"
        f"{_line('Сколько товаров у поставщика до фильтра', source_total)}\n"
        f"{_line('Сколько товаров у поставщика после фильтра', total)}\n"
        f"{_line('Сколько товаров есть в наличии (true)', avail_true)}\n"
        f"{_line('Сколько товаров нет в наличии (false)', avail_false)}\n"
        "-->\n\n"
    )

    out_text = feed_meta + head + new_offers + '\n' + tail
    out_text = _ensure_footer_spacing(out_text)
    out_text = re.sub(r'[ \t]+\n', '\n', out_text)
    out_text = re.sub(r'\n{3,}', '\n\n', out_text)
    out_text = out_text.replace('<shop><offers>', '<shop><offers>\n')

    Path('docs').mkdir(exist_ok=True)
    out_text = _append_faq_reviews_after_desc(out_text)
    Path('docs/alstyle.yml').write_text(out_text, encoding='windows-1251', errors='replace')
    print('OK: docs/alstyle.yml, offers:', len(kept))
    return 0


# --- [APPENDIX] FAQ+Отзывы в конец <description> (вариант A, идемпотентно) ---
def _append_faq_reviews_after_desc(_text: str) -> str:
    """
    Динамически добавляет блоки «FAQ — Частые вопросы» и «Отзывы покупателей»
    в КОНЕЦ <description> каждого оффера на основе <name>, текста описания И <param>.
    НИЧЕГО больше в карточке не трогаем. Повторно не вставляем, если уже есть.
    """
    import re as _re, html as _html

    # --- Утилиты ---
    def _unhtml(s: str) -> str:
        s = _re.sub(r'(?is)<script.*?>.*?</script>', '', s)
        s = _re.sub(r'(?is)<style.*?>.*?</style>', '', s)
        s = _re.sub(r'(?is)<[^>]+>', ' ', s)
        s = _re.sub(r'\s+', ' ', s).strip()
        return _html.unescape(s)

    def _params_kv(offer_html: str):
        kv = {}
        for pm in _re.finditer(r'(?is)<param\b([^>]*)>(.*?)</param>', offer_html):
            attrs, val = pm.group(1), pm.group(2)
            nm = _re.search(r'name\s*=\s*"(.*?)"', attrs)
            if not nm:
                continue
            key = _html.unescape(nm.group(1)).strip().lower()
            val = _html.unescape(_unhtml(val)).strip()
            if not key or not val:
                continue
            # Храним первое значение, чтобы не плодить длинные списки
            kv.setdefault(key, val)
        return kv

    def _classify(ctx: str, kv: dict) -> str:
        ctx_lc = ctx.lower()
        # приоритет по параметрам
        if any(k in kv for k in ['картридж','тонер','ресурс','совместимость']): return 'cartridge'
        if any(k in kv for k in ['мощность, ва','мощность ва','va','тип ибп']):   return 'ups'
        if any(k in kv for k in ['диагональ','разрешение','частота','яркость']) and 'монитор' in (ctx_lc + ' ' + ' '.join(kv.keys())): 
            return 'monitor'
        if any(k in kv for k in ['процессор','оперативная память','озу','ssd','накопитель','операционная система']): 
            return 'laptop'
        if 'принтер' in ctx_lc or 'мфу' in ctx_lc: return 'printer'
        if 'монитор' in ctx_lc: return 'monitor'
        if 'картридж' in ctx_lc or 'toner' in ctx_lc or 'drum' in ctx_lc: return 'cartridge'
        if 'ups' in ctx_lc or 'ибп' in ctx_lc: return 'ups'
        return 'generic'

    # Извлечение ключевых фактов из параметров/текста
    def _facts(kind: str, plain_lc: str, kv: dict) -> dict:
        F = {}
        g = lambda *keys: next((kv[k] for k in keys if k in kv), '')
        if kind == 'cartridge':
            F['compat'] = g('совместимость','подходит для','совместимые модели')
            F['yield']  = g('ресурс','ресурс страниц','ресурс печати')
            F['color']  = g('цвет','цвет печати')
        elif kind == 'ups':
            F['va']     = g('мощность, ва','мощность ва','va')
            F['watt']   = g('мощность, вт','мощность вт','w')
            F['type']   = g('тип ибп','топология')
        elif kind == 'monitor':
            F['diag']   = g('диагональ','диагональ экрана')
            F['res']    = g('разрешение','разрешение экрана')
            F['hz']     = g('частота обновления','частота')
            F['bright'] = g('яркость')
            F['ports']  = g('интерфейсы','видеовыходы','разъёмы')
        elif kind == 'laptop':
            F['cpu']    = g('процессор')
            F['ram']    = g('оперативная память','озу')
            F['ssd']    = g('накопитель','ssd','объем ssd','объем накопителя')
            F['os']     = g('операционная система','ос')
        elif kind == 'printer':
            F['speed']  = g('скорость печати')
            F['wifi']   = g('wi-fi','wi‑fi','wifi','беспроводной интерфейс')
            F['duplex'] = g('двусторонняя печать','duplex')
        # fallback по тексту
        if not F.get('yield'):
            m = re.search(r'(\d{3,5})\s*(стр|страниц)', plain_lc)
            if m: F['yield'] = m.group(1)
        if not F.get('res'):
            m = re.search(r'(\d{3,4})\s*[x×х]\s*(\d{3,4})', plain_lc)
            if m: F['res'] = f"{m.group(1)}×{m.group(2)}"
        if not F.get('diag'):
            m = re.search(r'(\d{2,3})\s*(дюйм|\"|″)', plain_lc)
            if m: F['diag'] = m.group(1)
        return F

    def _faq_html(kind: str, model: str, F: dict) -> str:
        # Только FAQ — без блока «Ключевые особенности»
        if kind == 'cartridge':
            qs = [
                f"<strong>Подойдёт ли { _html.escape(model) } к моему принтеру?</strong><br>{ 'Проверьте совместимость: ' + _html.escape(F.get('compat','см. карточку')) if F.get('compat') else 'Сверьте список совместимости в карточке или напишите нам.' }",
                f"<strong>Какой ресурс печати?</strong><br>{ ('Около ' + _html.escape(F['yield']) + ' страниц.') if F.get('yield') else 'Ресурс зависит от покрытия страницы; ориентируйтесь на паспорт модели.' }",
                f"<strong>Цвет печати?</strong><br>{ (_html.escape(F['color']).capitalize()) if F.get('color') else 'Уточните в описании: чёрный или цветной картридж.' }",
            ]
        elif kind == 'ups':
            qs = [
                f"<strong>На сколько хватит ИБП?</strong><br>{ ('Зависит от нагрузки; при ' + _html.escape(F.get('watt','типовой')) + ' Вт — 5–15 минут.') if F.get('watt') else 'Обычно 5–15 минут при типовой нагрузке.' }",
                f"<strong>Какая мощность?</strong><br>{ (_html.escape(F['va']) + ' VA') if F.get('va') else 'Смотрите мощность в характеристиках.' }",
                f"<strong>Топология?</strong><br>{ _html.escape(F['type']) if F.get('type') else 'Чаще всего line‑interactive; см. карточку.' }",
            ]
        elif kind == 'monitor':
            qs = [
                f"<strong>Подходит для работы/учёбы?</strong><br>{ ('Да, ' + _html.escape(F.get('diag','')) + '″, ' + _html.escape(F.get('res',''))).strip(', ') or 'Смотрите диагональ и разрешение — подберите под свои задачи.' }",
                f"<strong>Какие порты?</strong><br>{ _html.escape(F['ports']) if F.get('ports') else 'HDMI / DisplayPort / USB‑C — проверьте список интерфейсов.' }",
                f"<strong>Частота обновления?</strong><br>{ (_html.escape(F['hz']) + ' Гц') if F.get('hz') else 'См. спецификацию (60/75/120/144 Гц и т.д.).' }",
            ]
        elif kind == 'laptop':
            qs = [
                f"<strong>Производительность?</strong><br>{ ('CPU: ' + _html.escape(F['cpu'])) if F.get('cpu') else 'Оцените CPU/GPU и объём памяти под ваши задачи.' }",
                f"<strong>Память и накопитель?</strong><br>{ ('ОЗУ: ' + _html.escape(F.get('ram','?')) + ', SSD: ' + _html.escape(F.get('ssd','?'))) if (F.get('ram') or F.get('ssd')) else 'Смотрите объёмы ОЗУ и SSD в карточке.' }",
                f"<strong>Какая ОС?</strong><br>{ _html.escape(F['os']) if F.get('os') else 'Windows / Linux / без ОС — уточняйте.' }",
            ]
        elif kind == 'printer':
            qs = [
                f"<strong>Скорость печати?</strong><br>{ (_html.escape(F['speed']) + ' стр/мин') if F.get('speed') else 'См. скорость печати в спецификации.' }",
                f"<strong>Есть Wi‑Fi?</strong><br>{ 'Да' if F.get('wifi') else 'Проверьте в параметрах — Wi‑Fi указан в модели с беспроводным модулем.' }",
                f"<strong>Двусторонняя печать?</strong><br>{ 'Автоматическая' if (F.get('duplex','').lower().startswith('авто')) else 'См. параметры Duplex (авто/ручной).' }",
            ]
        else:
            qs = [
                "<strong>Есть гарантия?</strong><br>Да, официальная гарантия производителя.",
                "<strong>Сроки доставки?</strong><br>По РК обычно 3–7 рабочих дней.",
                "<strong>Способы оплаты?</strong><br>Для юр. лиц — безнал, для физ. — KASPI.",
            ]
        return (
            '<div style="background:#F7FAFF;border:1px solid #DDE8FF;padding:12px 14px;margin:12px 0;">'
            '<h3 style="margin:0 0 10px;font-size:17px;">FAQ — Частые вопросы</h3>'
            '<ul style="margin:0;padding-left:18px;">' + ''.join(f'<li style="margin:0 0 8px;">{q}</li>' for q in qs) + '</ul>'
            '</div>'
        )

    def _reviews_html(kind: str, name: str) -> str:
        if kind == 'cartridge':
            r1 = f"{name} печатает чётко и без шлейфов; ресурс соответствует ожиданиям."
            r2 = "Подходит для офисных задач — стабильная подача тонера и чистая печать."
        elif kind == 'ups':
            r1 = "Хватает, чтобы спокойно сохранить документы и выключить ПК."
            r2 = "Тихая работа и адекватное время автономии для дома."
        elif kind == 'monitor':
            r1 = "Цвета ровные, глазам комфортно; засветов минимально."
            r2 = "Подключился без проблем, картинка стабильная, шрифты чёткие."
        elif kind == 'laptop':
            r1 = "Система работает быстро, шум в норме; удобно для работы и учёбы."
            r2 = "Аккумулятор держит достойно, корпус не люфтит."
        elif kind == 'printer':
            r1 = "Быстрая и качественная печать; настройка заняла минуты."
            r2 = "Wi‑Fi подключился сразу, duplex экономит бумагу."
        else:
            r1 = f"{name} соответствует описанию и ожиданиям по качеству."
            r2 = "Упаковка надёжная, доставка своевременная — рекомендуем."
        return (
            '<div style="background:#F8FFF5;border:1px solid #DDEFD2;padding:12px 14px;margin:12px 0;">'
            '<h3 style="margin:0 0 10px;font-size:17px;">Отзывы покупателей</h3>'
            '<div style="background:#ffffff;border:1px solid #E4F0DD;padding:10px 12px;border-radius:10px;box-shadow:0 1px 0 rgba(0,0,0,.04);margin:0 0 10px;">'
            '<div style="font-weight:700;">Асем, Алматы <span style="color:#888;font-weight:400;">— 2025-10-28</span></div>'
            '<div style="color:#f5a623;font-size:14px;margin:2px 0 6px;" aria-label="Оценка 5 из 5">&#9733;&#9733;&#9733;&#9733;&#9733;</div>'
            f'<p style="margin:0;">{_html.escape(r1)}</p></div>'
            '<div style="background:#ffffff;border:1px solid #E4F0DD;padding:10px 12px;border-radius:10px;box-shadow:0 1px 0 rgba(0,0,0,.04);margin:0 0 10px;">'
            '<div style="font-weight:700;">Ерлан, Астана <span style="color:#888;font-weight:400;">— 2025-11-02</span></div>'
            '<div style="color:#f5a623;font-size:14px;margin:2px 0 6px;" aria-label="Оценка 5 из 5">&#9733;&#9733;&#9733;&#9733;&#9733;</div>'
            f'<p style="margin:0;">{_html.escape(r2)}</p></div>'
            '</div>'
        )

    # --- Основной проход по офферам ---
    out, pos = [], 0
    for m_offer in _re.finditer(r'(?is)<offer\b.*?>.*?</offer>', _text):
        out.append(_text[pos:m_offer.start()])
        offer = m_offer.group(0)

        # Уже есть FAQ/Отзывы? — пропустить
        if _re.search(r'FAQ\s*—\s*Частые\s+вопросы|Отзывы\s+покупателей', offer, _re.I):
            out.append(offer); pos = m_offer.end(); continue

        # Вытаскиваем name / description
        m_name = _re.search(r'(?is)<name>(.*?)</name>', offer)
        name_html = m_name.group(1) if m_name else ''
        name = _html.unescape(name_html).strip()

        m_desc = _re.search(r'(?is)(<description[^>]*>)(.*?)(</description>)', offer)
        if not m_desc:
            out.append(offer); pos = m_offer.end(); continue
        d_head, d_body_html, d_tail = m_desc.group(1), m_desc.group(2), m_desc.group(3)

        # Параметры
        kv = _params_kv(offer)
        # Контекст для классификации и фактов
        plain_ctx = _unhtml(name_html + ' ' + d_body_html + ' ' + ' '.join(f"{k}: {v}" for k,v in kv.items())).lower()
        kind = _classify(plain_ctx, kv)
        facts = _facts(kind, plain_ctx, kv)

        faq_html = _faq_html(kind, name[:60], facts)
        reviews_html = _reviews_html(kind, name)

        # Вставляем ТОЛЬКО FAQ и Отзывы, без "Ключевых особенностей"
        inject = '\n' + faq_html + reviews_html
        new_desc = d_head + d_body_html + inject + d_tail
        offer = offer[:m_desc.start()] + new_desc + offer[m_desc.end():]

        out.append(offer)
        pos = m_offer.end()

    out.append(_text[pos:])
    return ''.join(out)
    def _repl(m):
        head, body, tail = m.group(1), m.group(2), m.group(3)
        if ("FAQ — Частые вопросы" in body) or ("Отзывы покупателей" in body):
            return head + body + tail
        return head + body + '\n' + _FAQ + tail
    return _p.sub(_repl, _text)
# --- [END APPENDIX] ---
if __name__ == '__main__':
    raise SystemExit(main())
