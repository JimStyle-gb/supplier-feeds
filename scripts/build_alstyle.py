#!/usr/bin/env python3
# coding: utf-8
# build_alstyle.py — v68 feed_meta + params-sorted + attr-order fix
# База: v67 (ничего не трогаем), добавлен блок FEED_META в конец YML.

import os, re, html, sys, time, hashlib
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

# --- Константы поставщика ---
URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
AUTH = ("info@complex-solutions.kz", "Aa123456")
OUT = "docs/alstyle.yml"
ENC = "windows-1251"

# --- Разрешённые категории поставщика ---
ALLOW_CATS = {
  "3540","3541","3542","3543","3544","3545","3566","3567","3569","3570",
  "3580","3688","3708","3721","3722","4889","4890","4895","5017","5075",
  "5649","5710","5711","5712","5713","21279","21281","21291","21356","21367",
  "21368","21369","21370","21371","21372","21451","21498","21500","21501",
  "21572","21573","21574","21575","21576","21578","21580","21581","21583","21584",
  "21585","21586","21588","21591","21640","21664","21665","21666","21698"
}

# --- Чёрный список параметров ---
DENY_PARAMS = {s.lower() for s in [
  "Артикул","Благотворительность","Код ТН ВЭД","Код товара Kaspi",
  "Новинка","Снижена цена","Штрихкод","Штрих-код","Назначение",
  "Объем","Объём"
]}

# --- Приоритет параметров в блоке «Характеристики» ---
PRIOR = ['Диагональ экрана','Яркость','Операционная система','Объем встроенной памяти',
         'Память','Точек касания','Интерфейсы','Вес','Размеры']

# --- Утилиты ---
_re_offers_block = re.compile(r"(?is)(.*?<offers>)(.*?)(</offers>.*)", re.S)
_re_offer = re.compile(r"(?is)(<offer\b[^>]*>)(.*?)(</offer>)")
_re_cat = re.compile(r"(?is)<\s*categoryId\s*>\s*(\d+)\s*</\s*categoryId\s*>")
_re_tag = lambda t: re.compile(rf"(?is)<\s*{t}\s*>.*?</\s*{t}\s*>")
_re_price = re.compile(r"(?is)<\s*price\s*>\s*(\d+)\s*</\s*price\s*>")
_re_pprice = re.compile(r"(?is)<\s*purchase_price\s*>\s*(\d+)\s*</\s*purchase_price\s*>")
_re_vendorCode = re.compile(r"(?is)<\s*vendorCode\s*>\s*(.*?)\s*</\s*vendorCode\s*>")
_re_name = re.compile(r"(?is)<\s*name\s*>\s*(.*?)\s*</\s*name\s*>")
_re_available_tag = re.compile(r"(?is)<\s*available\s*>\s*(true|false)\s*</\s*available\s*>")

def _price_retail(base: int) -> int:
    """Розничная цена: +4% + абсолютные надбавки по диапазонам; хвост «900»; >=9,000,000 → 100"""
    if base >= 9_000_000: return 100
    add = 0
    if   101 <= base <= 10_000: add = 3_000
    elif 10_001 <= base <= 25_000: add = 4_000
    elif 25_001 <= base <= 50_000: add = 5_000
    elif 50_001 <= base <= 75_000: add = 7_000
    elif 75_001 <= base <= 100_000: add = 10_000
    elif 100_001 <= base <= 150_000: add = 12_000
    elif 150_001 <= base <= 200_000: add = 15_000
    elif 200_001 <= base <= 300_000: add = 20_000
    elif 300_001 <= base <= 400_000: add = 25_000
    elif 400_001 <= base <= 500_000: add = 30_000
    elif 500_001 <= base <= 750_000: add = 40_000
    elif 750_001 <= base <= 1_000_000: add = 50_000
    elif 1_000_001 <= base <= 1_500_000: add = 70_000
    elif 1_500_001 <= base <= 2_000_000: add = 90_000
    elif 2_000_001 <= base: add = 100_000
    retail = int((base * 1.04) + add + 0.9999)  # вверх
    retail = (retail // 1000) * 1000 + 900  # хвост 900
    return retail

def _move_available_attr(header: str, body: str):
    """Переносим <available>true/false</available> в атрибут available="..." в конце заголовка (сохраняя порядок)"""
    m = _re_available_tag.search(body)
    if not m: 
        return header, body
    val = m.group(1)
    body = _re_available_tag.sub("", body, count=1)
    if re.search(r'(?is)\bavailable\s*=\s*"(?:true|false)"', header):
        header = re.sub(r'(?is)\bavailable\s*=\s*"(?:true|false)"', f'available="{val}"', header, count=1)
    else:
        header = re.sub(r'>\s*$', f' available="{val}">', header, count=1)
    return header, body

def _strip_black_params(body: str) -> str:
    """Удаляем запрещённые <param name="...">...</param> (без пустых строк)"""
    def repl(m):
        name = m.group(1).strip().lower()
        return "" if name in DENY_PARAMS else m.group(0)
    body = re.sub(r'(?is)<\s*param\b[^>]*\bname\s*=\s*"([^"]+)"[^>]*>.*?</\s*param\s*>', repl, body)
    body = re.sub(r'[ \t]+\n', '\n', body)
    body = re.sub(r'\n{3,}', '\n\n', body)
    return body

def _fix_vendorcode_and_id(body: str, header: str):
    """Префикс AS и равенство id == vendorCode"""
    m = _re_vendorCode.search(body)
    vc = m.group(1).strip() if m else ""
    if not vc:
        n = _re_name.search(body)
        vc = ("AS" + hashlib.md5((n.group(1).strip() if n else "X").encode('utf-8')).hexdigest()[:6].upper())
        body = re.sub(r'(?is)</vendorCode>', '', body) if m else f"<vendorCode>{vc}</vendorCode>\n" + body
    if not vc.startswith("AS"):
        vc = "AS" + vc
        if m:
            body = _re_vendorCode.sub(f"<vendorCode>{vc}</vendorCode>", body, count=1)
        else:
            body = f"<vendorCode>{vc}</vendorCode>\n" + body
    header = re.sub(r'\bid="[^"]+"', f'id="{vc}"', header) if re.search(r'\bid="', header) else header.replace("<offer", f'<offer id="{vc}"', 1)
    return body, header, vc

def _swap_price_tags(body: str) -> str:
    """Меняем местами <price> и <purchase_price>, затем удаляем <purchase_price> и считаем розничную"""
    price = _re_price.search(body)
    pprice = _re_pprice.search(body)
    if price and pprice:
        b = body
        b = _re_price.sub(f"<price>{pprice.group(1)}</price>", b, count=1)
        b = _re_pprice.sub(f"<purchase_price>{price.group(1)}</purchase_price>", b, count=1)
        body = b
    price = _re_price.search(body)
    if price:
        base = int(price.group(1))
        retail = _price_retail(base)
        body = _re_price.sub(f"<price>{retail}</price>", body, count=1)
    body = _re_pprice.sub("", body)
    return body

def _clean_description_text(raw: str) -> str:
    """Чистим «родное» описание до плоского текста"""
    t = re.sub(r'(?is)<[^>]+>', ' ', raw)
    t = html.unescape(t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def _build_description(name: str, raw_desc: str, params_html: str) -> str:
    """<h3>{name}</h3> + <p>…≤1000…</p> + <h3>Характеристики</h3><ul>…</ul>; <br> — только для длинных"""
    plain = _clean_description_text(raw_desc)
    GOAL = 1000
    if len(plain) > GOAL:
        sents = re.split(r'(?<=[.!?])\s+', plain)
        acc, L = [], 0
        for s in sents:
            if L + len(s) > 1200: break
            acc.append(s); L += len(s) + 1
        text = ' '.join(acc).strip()
        text = re.sub(r'\s*\.\s*', '.<br>', text)  # мягкие разрывы только для длинных
    else:
        text = plain
    return f"<description><h3>{html.escape(name)}</h3><p>{text}</p>{params_html}</description>"

def _collect_params(body: str):
    """Собираем пары (name, value) из <param>"""
    pairs = re.findall(r'(?is)<\s*param\b[^>]*\bname\s*=\s*"([^"]+)"[^>]*>(.*?)</\s*param\s*>', body)
    result = []
    for k, v in pairs:
        k = re.sub(r'\s+', ' ', k).strip()
        v = re.sub(r'\s+', ' ', html.unescape(re.sub(r'(?is)<[^>]+>', ' ', v))).strip()
        if not k or not v: 
            continue
        result.append((k, v))
    return result

def _sort_params(items):
    """Приоритетные ключи сверху, затем остальное по алфавиту"""
    if not items: return []
    present = {k for k,_ in items}
    head = [k for k in PRIOR if k in present]
    tail = sorted([k for k,_ in items if k not in head], key=lambda s: s.lower())
    order = {k:i for i,k in enumerate(head + tail)}
    return sorted(items, key=lambda kv: order.get(kv[0], 10**6))

def _params_to_html(items):
    if not items: return ""
    lis = ''.join(f"<li><strong>{html.escape(k)}:</strong> {html.escape(v)}</li>" for k, v in items)
    return f"<h3>Характеристики</h3><ul>{lis}</ul>"

# --- FEED_META ---
def _append_feed_meta(text_out: str, *, supplier_url: str, total_before: int, total_after: int, avail_true: int, avail_false: int) -> str:
    """Добавляет HTML-комментарий FEED_META с пустыми строками до и после (в конец файла)."""
    try:
        now = datetime.now(ZoneInfo("Asia/Almaty"))
    except Exception:
        now = datetime.utcnow()
    target = now.replace(hour=1, minute=0, second=0, microsecond=0)
    if now >= target:
        target = target + timedelta(days=1)
    meta = (
        "<!--FEED_META\n"
        f"Поставщик                                  | AlStyle\n"
        f"URL поставщика                             | {supplier_url}\n"
        f"Время сборки (Алматы)                      | {now:%Y-%m-%d %H:%M:%S}\n"
        f"Ближайшая сборка (Алматы)                  | {target:%Y-%m-%d %H:%M:%S}\n"
        f"Сколько товаров у поставщика до фильтра    | {total_before}\n"
        f"Сколько товаров у поставщика после фильтра | {total_after}\n"
        f"Сколько товаров есть в наличии (true)      | {avail_true}\n"
        f"Сколько товаров нет в наличии (false)      | {avail_false}\n"
        "-->"
    )
    if not text_out.endswith("\n"):
        text_out += "\n"
    return text_out + "\n" + meta + "\n\n"

# --- Главный поток ---
def main() -> int:
    print('[VER] build_alstyle v68 feed_meta + params-sorted + attr-order fix')
    # 1) Скачиваем исходник
    r = requests.get(URL, auth=AUTH, timeout=60)
    r.raise_for_status()
    src = r.content
    try:
        text = src.decode(ENC)
    except UnicodeDecodeError:
        text = src.decode('utf-8', errors='replace')

    # 2) Блоки до/после <offers>
    m = _re_offers_block.match(text)
    if not m:
        print("ERR: offers block not found", file=sys.stderr); return 2
    head, offers_block, tail = m.group(1), m.group(2), m.group(3)

    # 3) Подсчёт до фильтра
    total_before = len(_re_offer.findall(offers_block))

    # 4) Обходим офферы
    kept = []
    for hdr, body, _ in _re_offer.findall(offers_block):
        # фильтр по categoryId
        cm = _re_cat.search(body)
        if not cm or cm.group(1) not in ALLOW_CATS:
            continue

        # перенос available
        hdr, body = _move_available_attr(hdr, body)

        # переименование тегов цен + розничная
        body = _swap_price_tags(body)

        # удаление служебных тегов
        for tg in ("url", "quantity", "quantity_in_stock", "available", "purchase_price"):
            body = _re_tag(tg).sub("", body)

        # чёрные параметры
        body = _strip_black_params(body)

        # vendorCode + id
        body, hdr, vc = _fix_vendorcode_and_id(body, hdr)

        # характеристики
        params = _collect_params(body)
        params = _sort_params(params)
        params_html = _params_to_html(params)

        # name + описание
        nm = (_re_name.search(body).group(1).strip() if _re_name.search(body) else vc)
        dm = re.search(r'(?is)<\s*description\s*>(.*?)</\s*description\s*>', body)
        raw_desc = dm.group(1) if dm else ""
        desc = _build_description(nm, raw_desc, params_html)

        # заменить/вставить <description>
        if dm:
            body = re.sub(r'(?is)<\s*description\s*>.*?</\s*description\s*>', desc, body, count=1)
        else:
            body = desc + "\n" + body

        # порядок тегов
        def _pick(tag, s):
            m = re.search(rf'(?is)<\s*{tag}\b.*?</\s*{tag}\s*>', s)
            return (m.group(0) if m else "")
        ordered = (
            _pick("categoryId", body) +
            _pick("vendorCode", body) +
            _pick("name", body) +
            _pick("price", body) +
            ''.join(re.findall(r'(?is)<\s*picture\b.*?</\s*picture\s*>', body)) +
            _pick("vendor", body) +
            _pick("currencyId", body) +
            _pick("description", body) +
            ''.join(re.findall(r'(?is)<\s*param\b[^>]*>.*?</\s*param\s*>', body))
        )
        kept.append(f"{hdr}{ordered}</offer>")

    # 5) Сборка результата
    new_offers = '\n'.join(x.strip() for x in kept)
    out_text = head + '\n' + new_offers + '\n' + tail

    # Нормализация
    out_text = re.sub(r'[ \t]+\n', '\n', out_text)
    out_text = re.sub(r'\n{3,}', '\n\n', out_text)
    out_text = out_text.replace('<shop><offers>', '<shop><offers>\n')

    # 6) FEED_META (по готовому списку)
    joined = ''.join(kept)
    avail_true = len(re.findall(r'(?is)<offer\b[^>]*\bavailable="true"', joined))
    avail_false = len(re.findall(r'(?is)<offer\b[^>]*\bavailable="false"', joined))
    out_text = _append_feed_meta(
        out_text,
        supplier_url=URL,
        total_before=total_before,
        total_after=len(kept),
        avail_true=avail_true,
        avail_false=avail_false
    )

    # 7) Запись
    Path('docs').mkdir(exist_ok=True)
    Path(OUT).write_text(out_text, encoding=ENC, errors='replace')
    print('OK:', OUT, 'offers:', len(kept))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
