# scripts/build_price.py
# -*- coding: utf-8 -*-
"""
Сборщик общего YML (Price) из 5 поставщиков.
- Правильный FEED_META (без лишних <!-- и -->)
- Сразу после общего FEED_META приклеиваются FEED_META всех поставщиков (как есть, по порядку)
- Сортировка тегов внутри <offer>:
    <vendorCode>
    <name>
    <price>
    <picture> (все подряд, если их несколько)
    <vendor>
    <currencyId>
    <available>
    <description>
- Дедупликация по vendorCode (первый победил).
"""

from __future__ import annotations
import os, re, sys, time
from datetime import datetime, timezone, timedelta
from xml.etree import ElementTree as ET

# ---------- настройки ----------
ENC               = os.getenv("OUTPUT_ENCODING", "windows-1251")
OUT_FILE_YML      = os.getenv("OUT_FILE_PRICE", "docs/price.yml")
SUPPLIER_FILES    = [
    ("alstyle",  "docs/alstyle.yml"),
    ("akcent",   "docs/akcent.yml"),
    ("copyline", "docs/copyline.yml"),
    ("nvprint",  "docs/nvprint.yml"),
    ("vtt",      "docs/vtt.yml"),
]

# ---------- время Алматы ----------
try:
    from zoneinfo import ZoneInfo  # py3.9+
    ALMATY_TZ = ZoneInfo("Asia/Almaty")
except Exception:
    ALMATY_TZ = None

def now_almaty_str() -> str:
    if ALMATY_TZ:
        return datetime.now(ALMATY_TZ).strftime("%d:%m:%Y - %H:%M:%S")
    return time.strftime("%d:%m:%Y - %H:%M:%S", time.localtime())

def next_build_time_almaty(hours: int = 4, days_list=(1,10,20)) -> str:
    """Чисто косметика для FEED_META в общем прайсе: ближайшая дата D из days_list, время HH:00:00."""
    if ALMATY_TZ:
        now = datetime.now(ALMATY_TZ)
    else:
        now = datetime.now()
    y, m = now.year, now.month
    candidates = []
    for d in sorted(days_list):
        dt = datetime(y, m, d, hours, 0, 0)
        if ALMATY_TZ: dt = dt.replace(tzinfo=ALMATY_TZ)
        if dt >= now: candidates.append(dt)
    if not candidates:
        # следующий месяц
        if m == 12:
            y2, m2 = y+1, 1
        else:
            y2, m2 = y, m+1
        for d in sorted(days_list):
            # если дня нет (напр. 31), пропускаем
            try:
                dt = datetime(y2, m2, d, hours, 0, 0)
                if ALMATY_TZ: dt = dt.replace(tzinfo=ALMATY_TZ)
                candidates.append(dt)
            except ValueError:
                pass
    dt = candidates[0]
    return dt.strftime("%d:%m:%Y - %H:%M:%S")

# ---------- утилиты ----------
def read_text(path: str) -> str:
    with open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def extract_feed_meta(txt: str) -> str:
    """
    Вытаскивает первый <!--FEED_META ... --> как есть (без двойных маркеров).
    Если встречается случай с лишним <!-- в начале или --> в конце, нормализуем.
    """
    # Находим самый первый блок комментария, где в теле есть 'FEED_META'
    for m in re.finditer(r"<!--(.*?)-->", txt, flags=re.S):
        body = m.group(1)
        if "FEED_META" in body:
            # вычистим возможные лишние маркеры из тела
            body_clean = body
            body_clean = body_clean.replace("<!--", "").replace("-->", "")
            body_clean = body_clean.strip()
            return f"<!--{body_clean}-->"
    return ""  # если у файла нет FEED_META

def parse_offers_from_file(path: str) -> list[ET.Element]:
    xml = ET.fromstring(read_text(path))
    shop = xml.find("shop") if xml.tag.lower() != "shop" else xml
    if shop is None: return []
    offers_el = shop.find("offers") or shop.find("Offers")
    if offers_el is None: return []
    return list(offers_el.findall("offer"))

ORDER = ["vendorCode","name","price","picture","vendor","currencyId","available","description"]

def reorder_offer_children(offer: ET.Element) -> None:
    # собираем по типам
    children = list(offer)
    # buckets
    by_tag = {k: [] for k in ORDER}
    others = []
    for ch in children:
        tag = ch.tag
        if tag == "picture":
            by_tag["picture"].append(ch)
        elif tag in by_tag:
            by_tag[tag] = [ch] if tag != "picture" else by_tag["picture"]
        else:
            others.append(ch)
    # очищаем
    for ch in children:
        offer.remove(ch)
    # кладём по порядку
    def add_one(el: ET.Element | None):
        if el is not None:
            offer.append(el)
    # vendorCode, name, price
    add_one(by_tag["vendorCode"][0] if by_tag["vendorCode"] else None)
    add_one(by_tag["name"][0]       if by_tag["name"]       else None)
    add_one(by_tag["price"][0]      if by_tag["price"]      else None)
    # все <picture>
    for p in by_tag["picture"]:
        offer.append(p)
    # vendor, currencyId, available, description
    add_one(by_tag["vendor"][0]     if by_tag["vendor"]     else None)
    add_one(by_tag["currencyId"][0] if by_tag["currencyId"] else None)
    add_one(by_tag["available"][0]  if by_tag["available"]  else None)
    add_one(by_tag["description"][0]if by_tag["description"]else None)
    # остальное (если что-то осталось необычное — в конец, чтобы ничего не потерять)
    for o in others:
        offer.append(o)

def get_text(el: ET.Element, tag: str) -> str:
    n = el.find(tag)
    return (n.text or "").strip() if n is not None and n.text else ""

# ---------- сборка ----------
def main() -> None:
    # создаём общий корень
    out_root = ET.Element("yml_catalog")
    out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")
    out_offers = ET.SubElement(out_shop, "offers")

    # статистика
    total_in = 0
    by_source_count: dict[str,int] = {}
    seen_vendorcodes: set[str] = set()
    duplicates = 0

    # собираем FEED_META поставщиков (текстом)
    suppliers_meta_comments: list[str] = []

    # пробегаемся по поставщикам
    for supplier_key, path in SUPPLIER_FILES:
        if not os.path.exists(path):
            continue
        txt = read_text(path)
        # заберём FEED_META как есть, но нормализованный
        fm = extract_feed_meta(txt)
        if fm:
            suppliers_meta_comments.append(fm)

        # парсим офферы
        offers = parse_offers_from_file(path)
        total_in += len(offers)
        kept = 0
        for o in offers:
            vc = get_text(o, "vendorCode")
            if not vc:
                # без кода — пропускаем
                continue
            if vc in seen_vendorcodes:
                duplicates += 1
                continue
            seen_vendorcodes.add(vc)
            # сортируем теги
            reorder_offer_children(o)
            out_offers.append(o)
            kept += 1
        by_source_count[supplier_key] = kept

    written = len(list(out_offers.findall("offer")))
    avail_true = sum(1 for o in out_offers.findall("offer") if get_text(o,"available").lower()=="true")
    avail_false = written - avail_true

    # ---------- общий FEED_META ----------
    merged_meta_lines = [
        "FEED_META",
        f"Поставщик                                  | merged",
        f"Время сборки (Алматы)                      | {now_almaty_str()}",
        f"Сколько товаров у поставщика до фильтра    | {total_in}",
        f"Сколько товаров у поставщика после фильтра | {written}",
        f"Сколько товаров есть в наличии (true)      | {avail_true}",
        f"Сколько товаров нет в наличии (false)      | {avail_false}",
        f"Дубликатов по vendorCode отброшено         | {duplicates}",
        "Разбивка по источникам                     | " + ", ".join(f"{k}:{by_source_count.get(k,0)}" for k,_ in SUPPLIER_FILES),
    ]
    merged_meta = "<!--" + "\n".join(merged_meta_lines) + " -->"

    # вставляем общий FEED_META как первый комментарий
    out_root.insert(0, ET.Comment("\n".join(merged_meta_lines)))

    # “красиво” отформатируем
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # сериализация
    xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")

    # аккуратно заменим автоматически вставленный комментарий на «ровный» вариант (он такой же, но это избавляет от сюрпризов)
    xml_text = re.sub(r"<!--\s*FEED_META.*?-->", merged_meta, xml_text, flags=re.S, count=1)

    # приклеим подряд FEED_META всех поставщиков сразу ПОСЛЕ общего FEED_META
    # найдём позицию первого ' -->' нашего общего блока
    m = re.search(r"(<!--FEED_META.*?-->)", xml_text, flags=re.S)
    if m and suppliers_meta_comments:
        tail = "\n" + "\n\n".join(suppliers_meta_comments) + "\n"
        xml_text = xml_text[:m.end()] + tail + xml_text[m.end():]

    # финальная косметика: убрать возможные лишние пустые строки
    xml_text = re.sub(r"\n{3,}", "\n\n", xml_text)

    # запись
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
        f.write(xml_text)

    print(f"Wrote: {OUT_FILE_YML} | total_in={total_in} | written={written} | dup={duplicates} | encoding={ENC}")

if __name__ == "__main__":
    main()
