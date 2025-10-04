# scripts/build_price.py
# -*- coding: utf-8 -*-
"""
Merge supplier feeds -> docs/price.yml (windows-1251)

Источники (если существуют): docs/alstyle.yml, docs/akcent.yml, docs/copyline.yml, docs/nvprint.yml, docs/vtt.yml
Оставляем порядок тегов в <offer>:
  <vendorCode><name><price><picture><vendor><currencyId><available><description>
Правила:
- id == vendorCode (как в исходниках)
- При дублях vendorCode: берём ПЕРВЫЙ (остальные считаем дубликатами)
- FEED_META: сводка по каждому источнику и итого
"""

from __future__ import annotations
import os, io, re, html
from typing import List, Dict, Any
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

OUTPUT = "docs/price.yml"
ENC    = "windows-1251"

SOURCES = [
    ("alstyle",  "docs/alstyle.yml"),
    ("akcent",   "docs/akcent.yml"),
    ("copyline", "docs/copyline.yml"),
    ("nvprint",  "docs/nvprint.yml"),
    ("vtt",      "docs/vtt.yml"),
]

def _alm_now():
    # Asia/Almaty ≈ UTC+5 без тонкой TZ
    return datetime.utcnow() + timedelta(hours=5)

def _fmt(dt: datetime) -> str:
    return dt.strftime("%d:%m:%Y - %H:%M:%S")

def yesc(s: str) -> str:
    return html.escape((s or "").strip())

def _get_text(el: ET.Element, tag: str) -> str:
    t = el.find(tag)
    return (t.text or "").strip() if (t is not None and t.text) else ""

def parse_offers(path: str) -> List[ET.Element]:
    with io.open(path, "r", encoding=ENC, errors="ignore") as f:
        xml = f.read()
    root = ET.fromstring(xml.encode(ENC, errors="ignore"))
    shop = root.find("shop") if root.tag.lower() != "shop" else root
    if shop is None: return []
    offers = shop.find("offers")
    return list(offers.findall("offer")) if offers is not None else []

ORDER = ["vendorCode","name","price","picture","vendor","currencyId","available","description"]

def reorder_offer_children(offer: ET.Element) -> None:
    """Приводим к строгому порядку тегов; остальные теги (если вдруг есть) — в конец."""
    children = list(offer)
    by_name: Dict[str, List[ET.Element]] = {}
    rest: List[ET.Element] = []
    for ch in children:
        nm = ch.tag
        if nm in ORDER:
            by_name.setdefault(nm, []).append(ch)
        else:
            rest.append(ch)
    # очищаем
    for ch in children:
        offer.remove(ch)
    # добавляем в порядке
    for nm in ORDER:
        for ch in by_name.get(nm, []):
            offer.append(ch)
    for ch in rest:
        offer.append(ch)

def render_feed_meta(stats: Dict[str,int], dups: int, total: int) -> str:
    rows = [
        ("Поставщик", "merged"),
        ("Время сборки (Алматы)", _fmt(_alm_now())),
        ("Сколько товаров у поставщика до фильтра", str(total + dups)),
        ("Сколько товаров у поставщика после фильтра", str(total)),
        ("Сколько товаров есть в наличии (true)", str(sum(stats.values()))),  # считаем все true
        ("Сколько товаров нет в наличии (false)", "0"),
        ("Дубликатов по vendorCode отброшено", str(dups)),
        ("Разбивка по источникам", ", ".join(f"{k}:{v}" for k,v in stats.items()) or "n/a"),
    ]
    key_w = max(len(k) for k,_ in rows)
    out = ["<!--FEED_META"]
    for i,(k,v) in enumerate(rows):
        end = " -->" if i == len(rows)-1 else ""
        out.append(f"{k.ljust(key_w)} | {v}{end}")
    return "\n".join(out)

def main() -> int:
    os.makedirs(os.path.dirname(OUTPUT) or ".", exist_ok=True)

    stats: Dict[str,int] = {}
    seen_codes: set[str] = set()
    merged: List[ET.Element] = []
    dups = 0
    total_candidates = 0

    for key, path in SOURCES:
        if not os.path.isfile(path):
            continue
        offers = parse_offers(path)
        cnt_before = 0
        for off in offers:
            cnt_before += 1
            total_candidates += 1
            vcode = _get_text(off, "vendorCode")
            if not vcode:
                # пропускаем странные офферы без vendorCode
                continue
            if vcode in seen_codes:
                dups += 1
                continue
            # нормализуем currencyId (ровно один)
            cids = off.findall("currencyId")
            if len(cids) == 0:
                ET.SubElement(off, "currencyId").text = "KZT"
            elif len(cids) > 1:
                # оставляем только первый
                first = cids[0]
                for extra in cids[1:]:
                    off.remove(extra)
                first.text = "KZT"
            else:
                cids[0].text = "KZT"
            # available по умолчанию true, если нет
            av = off.find("available")
            if av is None:
                av = ET.SubElement(off, "available")
            av.text = "true" if (av.text or "").strip().lower() != "false" else "false"
            # id = vendorCode (если вдруг не равно)
            off.attrib["id"] = vcode
            # порядок тегов
            reorder_offer_children(off)
            # добавляем
            merged.append(off)
            seen_codes.add(vcode)
        stats[key] = stats.get(key, 0) + (cnt_before - (total_candidates - len(merged) - dups))

    # соберём выходной YML
    root = ET.Element("yml_catalog"); root.set("date", datetime.utcnow().strftime("%Y-%m-%d %H:%M"))
    root.append(ET.Comment(render_feed_meta(stats, dups, len(merged))))
    shop = ET.SubElement(root, "shop")
    offers = ET.SubElement(shop, "offers")
    for off in merged:
        offers.append(off)

    # pretty
    try: ET.indent(root, space="  ")
    except Exception: pass

    xml_bytes = ET.tostring(root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")
    with io.open(OUTPUT, "w", encoding=ENC, newline="\n") as f:
        f.write(xml_text)

    # nojekyll, чтобы Pages отдал файл
    try:
        open("docs/.nojekyll", "wb").close()
    except Exception:
        pass

    print(f"Wrote: {OUTPUT} | offers={len(merged)}")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
