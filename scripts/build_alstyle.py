# coding: utf-8
# build_alstyle.py — v109 (safe refactor of v108; база v105 не трогаем)
# Изменения против v108: унификация нормализации ключей для DENY_PARAMS,
# предкомпиляция regex, компактная чистка, перенос вставки \n после <offers> в общий форматтер,
# страховка префикса AS (case-insensitive). ЛОГИКА ВЫВОДА НЕ МЕНЯЕТСЯ.

import os, re, html, hashlib
from pathlib import Path
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo  # 3.9+
except Exception:
    ZoneInfo = None

import requests

print('[VER] build_alstyle v109 (safe-refactor of v108)')

# --- Константы и креды ---
LOGIN = os.getenv('ALSTYLE_LOGIN', 'info@complex-solutions.kz')
PASSWORD = os.getenv('ALSTYLE_PASSWORD', 'Aa123456')
SUPPLIER_URL = 'https://al-style.kz/upload/catalog_export/al_style_catalog.php'

GOAL = 1000           # целевая длина описания (символы)
GOAL_LOW = 900        # нижняя граница «достаточно близко»
MAX_HARD = 1200       # жёсткий максимум
LMAX = 220            # длина строки при вставке <br>
MAX_BR = 3            # максимум разрывов <br> в аннотации

ALLOW_CATS = {str(x) for x in [
  3540, 3541, 3542, 3543, 3544, 3545, 3566, 3567, 3569, 3570,
  3580, 3688, 3708, 3721, 3722, 4889, 4890, 4895, 5017, 5075,
  5649, 5710, 5711, 5712, 5713, 21279, 21281, 21291, 21356, 21367,
  21368, 21369, 21370, 21371, 21372, 21451, 21498, 21500, 21501,
  21572, 21573, 21574, 21575, 21576, 21578, 21580, 21581, 21583, 21584,
  21585, 21586, 21588, 21591, 21640, 21664, 21665, 21666, 21698
]}

# --- Нормализация ключей (чтобы "Объем"/"Объём" ловились одинаково) ---
def _norm_key(s: str) -> str:
    s = (s or '').replace('ё', 'е').replace('Ё', 'Е')
    s = re.sub(r'[\s\-]+', ' ', s).strip().lower()
    return s

DENY_PARAMS_RAW = [
  "Артикул", "Благотворительность", "Код ТН ВЭД", "Код товара Kaspi",
  "Новинка", "Снижена цена", "Штрихкод", "Штрих-код", "Назначение",
  "Объем", "Объём"
]
DENY_PARAMS = { _norm_key(x) for x in DENY_PARAMS_RAW }

# --- Предкомпилированные regex (ускоряет и исключает опечатки) ---
_re_tag = re.compile(r'(?is)<[^>]+>')
_re_sent_split = re.compile(r'(?<=[\.\!\?])\s+|;\s+')
_re_param = re.compile(r'(?is)<\s*param\b[^>]*\bname\s*=\s*"([^"]+)"[^>]*>(.*?)</\s*param\s*>')
_re_desc_block = re.compile(r'(?is)(<\s*description\b[^>]*>)(.*?)(</\s*description\s*>)')
_re_name = re.compile(r'(?is)<\s*name\s*>\s*(.*?)\s*</\s*name\s*>')
_re_price = re.compile(r'(?is)(<\s*price\s*>\s*)(.*?)(\s*</\s*price\s*>)')
_re_pprice = re.compile(r'(?is)<\s*purchase_price\s*>\s*(.*?)\s*</\s*purchase_price\s*>')
_re_avail_tag = re.compile(r'(?is)<\s*available\s*>\s*(true|false)\s*</\s*available\s*>')
_re_offer_wrap = re.compile(r'(?is)^\s*(<offer\b[^>]*>)(.*)</offer>\s*$')
_re_category = re.compile(r'(?is)<\s*categoryId\s*>\s*(\d+)\s*</\s*categoryId\s*>')

# --- Очистка plain-текста ---
def _clean_plain(txt: str) -> str:
    # 1) два раза HTML-unescape (некоторые приходят дважды экранированными)
    for _ in range(2):
        nt = html.unescape(txt)
        if nt == txt: break
        txt = nt
    # 2) неразрывный и zero-width
    txt = txt.replace('\u00A0', ' ')
    txt = re.sub(r'[\u200B-\u200D\uFEFF]', '', txt)
    # 3) все переводы строк -> пробел
    txt = re.sub(r'\r\n|\r|\n', ' ', txt)
    # 4) убрать теги
    txt = _re_tag.sub(' ', txt)
    # 5) схлопнуть пробелы
    return re.sub(r'\s+', ' ', txt).strip()

def _sentences(plain: str):
    return [p.strip() for p in _re_sent_split.split(plain) if p.strip()]

def _build_desc_text(plain: str) -> str:
    # Короткий текст возвращаем как есть
    if len(plain) <= GOAL: return plain
    parts = _sentences(plain)
    if not parts: return plain[:GOAL]
    selected, total = [], 0
    # Берём первое предложение целиком
    selected.append(parts[0]); total = len(parts[0])
    # Затем добавляем до GOAL_LOW, но не переходя MAX_HARD
    for p in parts[1:]:
        add = (1 if total else 0) + len(p)
        if total + add > MAX_HARD: break
        selected.append(p); total += add
        if total >= GOAL_LOW: break
    # Если ещё мало — добавим понемногу, пока не упрёмся в MAX_HARD
    if total < GOAL_LOW:
        for p in parts[len(selected):]:
            add = (1 if total else 0) + len(p)
            if total + add > MAX_HARD: break
            selected.append(p); total += add
            if total >= GOAL_LOW: break
    return ' '.join(selected).strip()

# --- Прайсинг ---
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
    tmp = int(base * 1.04 + add + 0.9999)       # +4% и лёгкий ап
    thousands = (tmp + 999) // 1000
    retail = thousands * 1000 - 100             # хвост 900
    if retail % 1000 != 900:
        retail = (retail // 1000 + 1) * 1000 - 100
    return max(retail, 900)

# --- Параметры -> список (фильтруем запрещённые) ---
def _collect_params(block: str):
    out = []
    for name, val in _re_param.findall(block):
        key = _clean_plain(name).strip(': ')
        if not key: continue
        if _norm_key(key) in DENY_PARAMS: continue
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

# --- available (тег) -> атрибут заголовка <offer ...> ---
def _move_available_attr(header: str, body: str):
    m = _re_avail_tag.search(body)
    if not m: return header, body
    avail = m.group(1)
    body = _re_avail_tag.sub('', body, count=1)
    if re.search(r'(?is)\bavailable\s*=\s*"(?:true|false)"', header):
        header = re.sub(r'(?is)\bavailable\s*=\s*"(?:true|false)"', f'available="{avail}"', header, count=1)
    else:
        header = re.sub(r'>\s*$', f' available="{avail}">', header, count=1)
    return header, body

# --- Удаление простых тегов + очистка пустых строк (одним проходом) ---
_SIMPLE_TAGS = ('url','quantity','quantity_in_stock','purchase_price')
def _remove_simple_tags(body: str) -> str:
    # Удаляем указанные теги
    pattern = r'|'.join(map(re.escape, _SIMPLE_TAGS))
    body = re.sub(rf'(?is)<\s*(?:{pattern})\s*>.*?</\s*(?:{pattern})\s*>', '', body)
    # Чистим хвостовые пробелы, схлопываем множественные пустые строки
    body = re.sub(r'[ \t]+\n', '\n', body)
    body = re.sub(r'\n{3,}', '\n\n', body)
    return body.strip()

# --- Если price отсутствует — взять его из purchase_price (как есть строковое) ---
def _ensure_price_from_purchase(body: str) -> str:
    if re.search(r'(?is)<\s*price\s*>', body): return body
    m = _re_pprice.search(body)
    if not m: return body
    raw = m.group(1)
    tag = f'<price>{raw}</price>'
    m2 = re.search(r'(?is)<\s*currencyId\s*>', body)
    if m2: return body[:m2.start()] + tag + body[m2.start():]
    m3 = re.search(r'(?is)</\s*name\s*>', body)
    if m3: return body[:m3.end()] + tag + body[m3.end():]
    m4 = re.search(r'(?is)</\s*offer\s*>', body)
    if m4: return body[:m4.start()] + tag + body[m4.start():]
    return body

# --- Пост-обработка description: аннотация + <h3>name</h3> + блок "Характеристики" ---
def _desc_postprocess_native_specs(offer_xml: str) -> str:
    m = _re_desc_block.search(offer_xml)
    head, raw, tail = (m.group(1), m.group(2), m.group(3)) if m else ('<description>', '', '</description>')
    plain_full = _clean_plain(raw)
    desc_text = _build_desc_text(plain_full)

    # Если длинный — раскладываем на строки с <br>, иначе просто экранируем
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

    # Имя товара как заголовок
    mname = _re_name.search(offer_xml)
    name_h3 = ''
    if mname:
        nm = _clean_plain(mname.group(1))
        if nm: name_h3 = '<h3>' + html.escape(nm) + '</h3>'

    # Характеристики (из <param> после фильтра по DENY)
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
        insert_at = re.search(r'(?is)</\s*currencyId\s*>', offer_xml) or re.search(r'(?is)</\s*name\s*>', offer_xml)
        ins = insert_at.end() if insert_at else len(offer_xml)
        return offer_xml[:ins] + '<description>' + new_html + '</description>' + offer_xml[ins:]

# --- Блок WhatsApp (фикс закрывающего </u>, entity для смайла) ---
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
      <li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 ₸ | 3–7 рабочих дней</em></li>
      <li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>
      <li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li>
    </ul>
  </div>
</div>

"""

def _inject_whatsapp_block(offer_xml: str) -> str:
    # Защита от повторной вставки
    if 'НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!' in offer_xml:
        return offer_xml
    m = _re_desc_block.search(offer_xml)
    if not m: return offer_xml
    head, body, tail = m.group(1), m.group(2), m.group(3)
    new_body = WHATSAPP_BLOCK + body
    return offer_xml[:m.start(1)] + head + new_body + tail + offer_xml[m.end(3):]

# --- Порядок тегов ---
WANT_ORDER = ('categoryId','vendorCode','name','price','picture','vendor','currencyId','description','param')

def _rebuild_offer(offer_xml: str) -> str:
    m = _re_offer_wrap.match(offer_xml)
    if not m: return offer_xml.strip() + '\n\n'
    header, body = m.group(1), m.group(2)

    # available -> атрибут
    header, body = _move_available_attr(header, body)

    # price из purchase_price (если у price нет значения)
    body = _ensure_price_from_purchase(body)

    # swap price из purchase_price (если оба есть — переписываем price значением purchase_price)
    mp = _re_pprice.search(body)
    if mp:
        val = mp.group(1)
        if _re_price.search(body):
            body = _re_price.sub(r'\1' + val + r'\3', body, count=1)
        else:
            body = '<price>' + val + '</price>' + body

    # удалить простые теги
    body = _remove_simple_tags(body)

    # vendorCode / id с префиксом AS
    mv = re.search(r'(?is)<\s*vendorCode\s*>\s*(.*?)\s*</\s*vendorCode\s*>\s*', body)
    if mv:
        v = _clean_plain(mv.group(1))
    else:
        mi = re.search(r'(?is)\bid="([^"]+)"', header)
        v = mi.group(1) if mi else 'AS' + hashlib.md5(body.encode('utf-8')).hexdigest()[:8].upper()
        body = '<vendorCode>' + html.escape(v) + '</vendorCode>' + body
    if not v.upper().startswith('AS'):
        v = 'AS' + v
        body = re.sub(r'(?is)(<\s*vendorCode\s*>\s*).*?(\s*</\s*vendorCode\s*>)', r'\1' + html.escape(v) + r'\2', body, count=1)
    header = re.sub(r'(?is)\bid="[^"]*"', f'id="{v}"', header, count=1)

    # привести множественные пробелы в шапке к одному
    header = re.sub(r'\s{2,}', ' ', header)

    # финальный розничный прайс по правилу (4% + ступень, хвост 900; >=9млн -> 100)
    mprice = _re_price.search(body)
    if mprice:
        digits = re.sub(r'[^\d]', '', mprice.group(2))
        base = int(digits) if digits else 0
        newp = _retail_price_from_base(base) if base else 0
        body = _re_price.sub(r'\1' + str(newp) + r'\3', body, count=1)

    # собрать description + характеристики
    full_offer = header + body + '</offer>'
    full_offer = _desc_postprocess_native_specs(full_offer)

    # вставить WhatsApp-блок в начало description
    full_offer = _inject_whatsapp_block(full_offer)

    # выделить нужные теги и собрать в заданном порядке
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
        # финальная защита от запретных key (нормализация)
        mname = re.search(r'(?is)\bname\s*=\s*"([^"]+)"', prm or '')
        if mname and _norm_key(mname.group(1)) in DENY_PARAMS: 
            continue
        out_lines.append(prm)

    out = header + '\n' + '\n'.join(x.strip() for x in out_lines if x.strip()) + '\n</offer>\n\n'
    return out

# --- Форматирование конца файла и верха <offers> ---
def _ensure_footer_spacing(txt: str) -> str:
    # после последнего </offer> — две пустые строки перед </offers>
    txt = re.sub(r'</offer>[ \t]*(?:\r?\n){0,10}[ \t]*(?=</offers>)', '</offer>\n\n', txt, count=1)
    # добавить \n сразу после <shop><offers>
    txt = re.sub(r'(?is)<shop>\s*<offers>\s*', '<shop><offers>\n', txt, count=1)
    # перенос перед </shop> и перед </yml_catalog>
    txt = re.sub(r'([^\n])[ \t]*</shop>', r'\1\n</shop>', txt, count=1)
    txt = re.sub(r'([^\n])[ \t]*</yml_catalog>', r'\1\n</yml_catalog>', txt, count=1)
    return txt

def main() -> int:
    # 1) Скачиваем исходник
    r = requests.get(SUPPLIER_URL, auth=(LOGIN, PASSWORD), timeout=60)
    r.raise_for_status()
    src = r.content
    try:
        text = src.decode('windows-1251')
    except UnicodeDecodeError:
        text = src.decode('utf-8', errors='replace')

    # 2) Режем на head/offers/tail
    m = re.search(r'(?is)^(.*?<offers\s*>)(.*?)(</\s*offers\s*>.*)$', text)
    if not m:
        m = re.search(r'(?is)(.*?<offers\s*>)(.*)(</\s*offers\s*>.*)', text)
        if not m: raise SystemExit('Не найден блок <offers>')
    head, offers_block, tail = m.group(1), m.group(2), m.group(3)

    # 3) Убираем мусор между <shop> и <offers>, ставим компактно
    head = re.sub(r'(?is)<shop\s*>.*?<offers\s*>', '<shop><offers>', head, count=1)
    if not head.endswith('\n'): head = head + '\n'

    # 4) Парсим офферы и фильтруем по списку категорий поставщика
    offers = re.findall(r'(?is)<offer\b.*?</offer>', offers_block)
    kept = []
    for off in offers:
        mcat = _re_category.search(off)
        if not mcat or mcat.group(1) not in ALLOW_CATS: 
            continue
        kept.append(_rebuild_offer(off))

    # 5) Собираем блок <offers> и FEED_META
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

    # 6) Финальные форматные правки
    out_text = _ensure_footer_spacing(out_text)
    out_text = re.sub(r'[ \t]+\n', '\n', out_text)   # убрать хвостовые пробелы у строк
    out_text = re.sub(r'\n{3,}', '\n\n', out_text)   # схлопнуть тройные+ переносы

    # 7) Запись
    Path('docs').mkdir(exist_ok=True)
    Path('docs/alstyle.yml').write_text(out_text, encoding='windows-1251', errors='replace')
    print('OK: docs/alstyle.yml, offers:', len(kept))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
