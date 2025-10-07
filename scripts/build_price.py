# scripts/build_price.py
# -*- coding: utf-8 -*-
"""
Сборщик общего прайса docs/price.yml из 5 поставщиков.

FEED_META:
- Первый блок: Поставщик = Price
- Разбивка по источникам: AlStyle, AkCent, CopyLine, VTT, NVPrint
- Ровно одинарные маркеры комментария <!--FEED_META ... -->
- Между блоками — одна пустая строка
- Сразу после общего блока приклеиваются FEED_META всех поставщиков (как есть, но нормализованные)

Офферы:
- Дедуп по vendorCode (первый победил)
- Строгий порядок тегов:
  vendorCode, name, price, picture*, vendor, currencyId, available, description
- id = vendorCode
- Ровно один <currencyId>KZT</currencyId>
- available по умолчанию true
- Выход: windows-1251
"""

from __future__ import annotations
import os, re, time
from datetime import datetime, timedelta
from typing import List, Dict
from xml.etree import ElementTree as ET

# ---------- конфиг ----------
ENC          = os.getenv("OUTPUT_ENCODING", "windows-1251")
OUT_FILE     = os.getenv("OUT_FILE_PRICE", "docs/price.yml")

SOURCES: List[tuple[str, str, str]] = [
    # (display_name, key, path)
    ("AlStyle",  "alstyle",  "docs/alstyle.yml"),
    ("AkCent",   "akcent",   "docs/akcent.yml"),
    ("CopyLine", "copyline", "docs/copyline.yml"),
    ("NVPrint",  "nvprint",  "docs/nvprint.yml"),
    ("VTT",      "vtt",      "docs/vtt.yml"),
]

ORDER = ["vendorCode","name","price","picture","vendor","currencyId","available","description"]

# ---------- время Алматы ----------
def alm_now_str() -> str:
    # простая реализация UTC+5 без внешних зависимостей
    return (datetime.utcnow() + timedelta(hours=5)).strftime("%d:%m:%Y - %H:%M:%S")

# ---------- utils ----------
def read_text(path: str) -> str:
    with open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def extract_first_feed_meta_block(txt: str) -> str:
    """
    Вытаскивает ПЕРВЫЙ <!--FEED_META ... --> блок из текста, нормализуя лишние вложенные маркеры.
    Возвращает строку вида: <!--FEED_META\n...\n-->
    Если не найден — пустую строку.
    """
    # найдём первый HTML-комментарий, в котором встречается 'FEED_META'
    for m in re.finditer(r"<!--(.*?)-->", txt, flags=re.S):
        body = m.group(1)
        if "FEED_META" in body:
            # вычистим возможные вложенные маркеры
            body = body.replace("<!--", "").replace("-->", "")
            body = body.strip("\n\r\t ")
            # гарантируем, что первая строка начинается с 'FEED_META'
            if not body.lstrip().startswith("FEED_META"):
                # иногда встречается заголовок/комментарий до FEED_META — оставим только с 'FEED_META'
                idx = body.find("FEED_META")
                if idx >= 0:
                    body = body[idx:]
            # приводим к каноническому виду
            body_lines = [ln.rstrip() for ln in body.splitlines()]
            body_clean = "\n".join(body_lines).strip()
            return f"<!--FEED_META\n{body_clean}\n-->"
    return ""

def parse_offers(path: str) -> List[ET.Element]:
    xml_txt = read_text(path)
    root = ET.fromstring(xml_txt)
    shop = root.find("shop") if root.tag.lower() != "shop" else root
    if shop is None:
        return []
    offers = shop.find("offers") or shop.find("Offers")
    return list(offers.findall("offer")) if offers is not None else []

def get_text(el: ET.Element, tag: str) -> str:
    n = el.find(tag)
    return (n.text or "").strip() if (n is not None and n.text) else ""

def reorder_offer_children(offer: ET.Element) -> None:
    """
    Кладём дочерние теги оффера в строгом порядке.
    Если <picture> несколько — сохраняем все, подряд.
    Остальные неожиданные теги — в конец, чтобы ничего не потерять.
    """
    children = list(offer)
    buckets: Dict[str, List[ET.Element]] = {k: [] for k in ORDER}
    others: List[ET.Element] = []
    for ch in children:
        if ch.tag == "picture":
            buckets["picture"].append(ch)
        elif ch.tag in buckets and ch.tag != "picture":
            # берём первый встреченный из каждого ключевого
            if not buckets[ch.tag]:
                buckets[ch.tag].append(ch)
        else:
            others.append(ch)
    # очистка
    for ch in children:
        offer.remove(ch)
    # сборка
    def add_one(name: str):
        if buckets[name]:
            offer.append(buckets[name][0])
    add_one("vendorCode")
    add_one("name")
    add_one("price")
    for pic in buckets["picture"]:
        offer.append(pic)
    add_one("vendor")
    add_one("currencyId")
    add_one("available")
    add_one("description")
    for extra in others:
        offer.append(extra)

def ensure_currency_available_and_ids(offer: ET.Element) -> None:
    """Гарантируем: один <currencyId>KZT</currencyId>, есть <available>, id == vendorCode."""
    # currencyId
    cids = offer.findall("currencyId")
    if not cids:
        ET.SubElement(offer, "currencyId").text = "KZT"
    else:
        # оставить только первый
        for extra in cids[1:]:
            offer.remove(extra)
        cids[0].text = "KZT"
    # available
    av = offer.find("available")
    if av is None:
        av = ET.SubElement(offer, "available")
    txt = (av.text or "").strip().lower()
    av.text = "false" if txt == "false" else "true"
    # id = vendorCode
    vc = get_text(offer, "vendorCode")
    if vc:
        offer.attrib["id"] = vc

def make_meta_block(title: str, lines: List[str]) -> str:
    """Конструирует корректный FEED_META-блок без лишних маркеров и пробелов."""
    body = "\n".join(lines).rstrip()
    return f"<!--FEED_META\nПоставщик                                  | {title}\n{body}\n-->"

def main() -> None:
    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)

    # агрегаты
    seen_codes: set[str] = set()
    by_source_count: Dict[str, int] = {}
    supplier_meta_blocks: List[str] = []
    total_in = 0
    kept_total = 0
    avail_true = 0
    avail_false = 0

    # дерево результата
    root = ET.Element("yml_catalog")
    root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    # placeholder для FEED_META-секции (подменим после сериализации)
    root.insert(0, ET.Comment("FEED_META_PLACEHOLDER"))
    shop = ET.SubElement(root, "shop")
    offers_out = ET.SubElement(shop, "offers")

    # загрузка источников
    for display, key, path in SOURCES:
        if not os.path.isfile(path):
            by_source_count[display] = 0
            continue

        # FEED_META источника (нормализуем)
        txt = read_text(path)
        fm = extract_first_feed_meta_block(txt)
        if fm:
            # убедимся, что блок начинается строго с <!--FEED_META на первой позиции
            fm = re.sub(r'^\s*<!--FEED_META', '<!--FEED_META', fm)
            supplier_meta_blocks.append(fm)

        # офферы
        offers = parse_offers(path)
        total_in += len(offers)
        kept = 0
        for o in offers:
            vc = get_text(o, "vendorCode")
            if not vc:
                continue
            if vc in seen_codes:
                continue
            seen_codes.add(vc)

            # привести оффер к общим правилам
            ensure_currency_available_and_ids(o)
            reorder_offer_children(o)

            # учёт доступности
            if get_text(o, "available").lower() == "false":
                avail_false += 1
            else:
                avail_true += 1

            offers_out.append(o)
            kept += 1
            kept_total += 1

        by_source_count[display] = kept

    # ---------- общий FEED_META (Price) ----------
    # строки, КРОМЕ первой (потому что первая — "Поставщик | Price" вставляется в make_meta_block)
    merged_lines = [
        f"Время сборки (Алматы)                      | {alm_now_str()}",
        f"Сколько товаров у поставщика до фильтра    | {total_in}",
        f"Сколько товаров у поставщика после фильтра | {kept_total}",
        f"Сколько товаров есть в наличии (true)      | {avail_true}",
        f"Сколько товаров нет в наличии (false)      | {avail_false}",
        "Разбивка по источникам                     | " + ", ".join(
            f"{disp}:{by_source_count.get(disp, 0)}" for disp, _, _ in SOURCES
        ),
    ]
    meta_price = make_meta_block("Price", merged_lines)

    # сериализация и подмена placeholder на секцию из блоков
    try:
        ET.indent(root, space="  ")
    except Exception:
        pass

    xml_bytes = ET.tostring(root, encoding=ENC, xml_declaration=True)
    xml_text = xml_bytes.decode(ENC, errors="replace")

    # слепим секцию: общий блок + пустая строка + блоки поставщиков, разделённые пустыми строками
    blocks = [meta_price] + supplier_meta_blocks
    meta_section = "\n\n".join(blocks)

    # аккуратная подмена placeholder на секцию БЕЗ ведущих пробелов
    xml_text = re.sub(r"<!--FEED_META_PLACEHOLDER-->", meta_section, xml_text, count=1)

    # косметика — убрать тройные пустые строки, если вдруг появились
    xml_text = re.sub(r"\n{3,}", "\n\n", xml_text)

    with open(OUT_FILE, "w", encoding=ENC, newline="\n") as f:
        f.write(xml_text)

    # отдать pages как статик
    try:
        open("docs/.nojekyll", "wb").close()
    except Exception:
        pass

    print(f"Wrote: {OUT_FILE} | total_in={total_in} | written={kept_total} | true={avail_true} | false={avail_false} | enc={ENC}")

if __name__ == "__main__":
    main()
