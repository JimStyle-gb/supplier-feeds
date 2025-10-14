# scripts/build_alstyle_same_skeleton.py
# -*- coding: utf-8 -*-
"""
Alstyle → YML (same visual skeleton as old 26) + PRICE RULES
Цель: выдать файл визуально как (26) и вернуть пересчёт цен, но код короче.
"""

from __future__ import annotations
import os, sys, re, time
from typing import List, Optional, Tuple
from xml.etree import ElementTree as ET
from datetime import datetime

# ====== НАСТРОЙКИ ======
SRC_URL_OR_FILE = os.getenv("SUPPLIER_URL", "alstyle_source.yml")     # URL или локальный путь
OUT_FILE        = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENCODING        = os.getenv("OUTPUT_ENCODING", "windows-1251")

# фиксированный порядок детей, как в (26)
ORDER = ["categoryId","vendorCode","name","price","picture","vendor","currencyId","description","param","keywords"]

# ценовые правила (как раньше)
# формат: (min_incl, max_incl, percent, add_abs)
PRICING_RULES = [
    (   101,    10000, 4.0,  3000),
    ( 10001,    25000, 4.0,  4000),
    ( 25001,    50000, 4.0,  5000),
    ( 50001,    75000, 4.0,  7000),
    ( 75001,   100000, 4.0, 10000),
    (100001,   150000, 4.0, 12000),
    (150001,   200000, 4.0, 15000),
    (200001,   300000, 4.0, 20000),
    (300001,   400000, 4.0, 25000),
    (400001,   500000, 4.0, 30000),
    (500001,   750000, 4.0, 40000),
    (750001,  1000000, 4.0, 50000),
    (1000001, 1500000, 4.0, 70000),
    (1500001, 2000000, 4.0, 90000),
    (2000001,100000000,4.0,100000),
]

# внутренние ценовые теги, которые в конечном YML не оставляем (как в 26)
INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice",
    "prices","Prices"  # контейнер с <price type="...">
)

# ====== УТИЛИТЫ ======
def log(msg: str): print(msg, flush=True)

def load_bytes(src: str) -> bytes:
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path, "rb") as f: return f.read()
    import requests
    r = requests.get(src, timeout=30)
    r.raise_for_status()
    return r.content

def bool_from_text(s: str) -> str:
    v = (s or "").strip().lower()
    true_set  = {"true","1","yes","да","есть","available","in stock"}
    false_set = {"false","0","no","нет","unavailable","out of stock","под заказ","ожидается","на заказ"}
    if v in true_set:  return "true"
    if v in false_set: return "false"
    return "false"

def ensure_available_attr(offer: ET.Element):
    """available только как атрибут; тег <available> удаляем."""
    if "available" not in offer.attrib:
        tag = offer.find("available")
        if tag is not None and tag.text:
            offer.attrib["available"] = bool_from_text(tag.text)
        else:
            offer.attrib["available"] = "false"
    # удалить тег
    for node in list(offer.findall("available")):
        offer.remove(node)

def ensure_category_first(offer: ET.Element):
    """Единственный <categoryId> и ПЕРВЫМ ребёнком. Значение сохраняем; если нет — '0'."""
    cats = list(offer.findall("categoryId"))
    text = None
    if cats:
        text = (cats[0].text or "").strip() if cats[0].text else None
        for c in cats: offer.remove(c)
    if not text: text = "0"
    cnew = ET.Element("categoryId"); cnew.text = text
    offer.insert(0, cnew)

def reorder_children_same_skeleton(offer: ET.Element):
    """Порядок как в (26). Незнакомые теги — в хвост, в исходном порядке."""
    children = list(offer)
    buckets = {k: [] for k in ORDER}
    others: List[ET.Element] = []
    for node in children:
        t = node.tag
        if t in ("picture","param"):
            buckets[t].append(node)
        elif t in buckets:
            buckets[t].append(node)
        else:
            others.append(node)
    new_children: List[ET.Element] = []
    for key in ORDER:
        new_children.extend(buckets[key])
    new_children.extend(others)
    if new_children != children:
        for n in children: offer.remove(n)
        for n in new_children: offer.append(n)

def ensure_currency_exists(offer: ET.Element, default_cur="KZT"):
    cur = offer.find("currencyId")
    if cur is None:
        cur = ET.SubElement(offer, "currencyId")
        cur.text = default_cur
    elif not (cur.text or "").strip():
        cur.text = default_cur

# ====== ЦЕНЫ ======
PRICE_DEALER_HINT  = re.compile(r"(дилер|dealer|опт|wholesale|b2b|закуп|purchase|оптов)", re.I)
PRICE_RRP_HINT     = re.compile(r"(rrp|ррц|розниц|retail|msrp)", re.I)

def _parse_price_number(raw: Optional[str]) -> Optional[float]:
    if not raw: return None
    s = (raw.replace("\xa0"," ").replace(" ","")
             .replace("KZT","").replace("kzt","").replace("₸","")
             .replace(",",".").strip())
    if not s: return None
    try:
        v = float(s)
        return v if v > 0 else None
    except Exception:
        return None

def pick_dealer_price(offer: ET.Element) -> Tuple[Optional[float], str]:
    """Ищем закупочную/оптовую цену: сначала внутри <prices><price type="...">,
       затем по прямым полям. Возвращаем (значение, источник)."""
    dealer_candidates=[]; rrp_candidates=[]
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            val=_parse_price_number(p.text or "")
            if val is None: continue
            t=(p.attrib.get("type") or "")
            if PRICE_DEALER_HINT.search(t): dealer_candidates.append(val)
            elif PRICE_RRP_HINT.search(t): rrp_candidates.append(val)
    if dealer_candidates: return (min(dealer_candidates), "prices_dealer")

    direct_fields = [
        "purchasePrice","purchase_price","wholesalePrice","wholesale_price",
        "opt_price","b2bPrice","b2b_price","supplier_price","supplierPrice",
        "min_price","minPrice"
    ]
    direct=[]
    for tag in direct_fields:
        el=offer.find(tag)
        if el is not None and el.text:
            v=_parse_price_number(el.text)
            if v is not None: direct.append(v)
    if direct: return (min(direct), "direct_field")
    if rrp_candidates: return (min(rrp_candidates), "rrp_fallback")
    return (None, "missing")

def _force_tail_900(n: float) -> int:
    i=int(n); k=max(i//1000,0); out=k*1000+900
    return out if out>=900 else 900

def compute_retail(dealer: float) -> Optional[int]:
    for lo, hi, pct, add in PRICING_RULES:
        if lo <= dealer <= hi:
            val = dealer*(1.0+pct/100.0) + add
            return _force_tail_900(val)
    return None

def apply_price_rules(offer: ET.Element):
    """Выставляем <price> по правилам. Если данных для расчёта нет — оставляем существующую цену.
       В конце чистим внутренние ценовые теги, как в 26."""
    dealer, src = pick_dealer_price(offer)
    new_price = None
    if dealer and dealer > 100:
        rp = compute_retail(dealer)
        if rp: new_price = str(int(rp))

    p = offer.find("price")
    if new_price is not None:
        if p is None: p = ET.SubElement(offer, "price")
        p.text = new_price
    # чистим внутренние
    for tag in INTERNAL_PRICE_TAGS:
        for node in list(offer.findall(tag)):
            offer.remove(node)

# ====== FEED_META ======
def render_feed_meta_comment(total_in: int, total_out: int) -> str:
    # Формат как в (26): "FEED_META" и время "%d:%m:%Y - %H:%M:%S"
    ts = datetime.now().strftime("%d:%m:%Y - %H:%M:%S")
    lines = [
        "FEED_META",
        f"Поставщик              | AlStyle",
        f"URL поставщика         | {SRC_URL_OR_FILE}",
        f"Время сборки (Алматы)  | {ts}",
        f"Сколько товаров вход   | {total_in}",
        f"Сколько товаров выход  | {total_out}",
    ]
    return "\n".join(lines)

# ====== MAIN ======
def main():
    raw = load_bytes(SRC_URL_OR_FILE)
    root_in = ET.fromstring(raw)

    shop_in = root_in.find("shop") if root_in.tag.lower() != "shop" else root_in
    if shop_in is None: raise SystemExit("XML: <shop> not found")
    offers_in = (shop_in.find("offers") or shop_in.find("Offers"))
    if offers_in is None: raise SystemExit("XML: <offers> not found")
    src_offers = list(offers_in.findall("offer"))

    out_root = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")
    out_offers = ET.SubElement(out_shop, "offers")

    for o in src_offers:
        offer = ET.fromstring(ET.tostring(o, encoding="utf-8"))  # deepcopy
        # 1) available → только атрибут
        ensure_available_attr(offer)
        # 2) categoryId → первым (значение сохраняем, по умолчанию 0)
        ensure_category_first(offer)
        # 3) currencyId → не трогаем, если есть; иначе KZT
        ensure_currency_exists(offer, default_cur="KZT")
        # 4) PRICE RULES
        apply_price_rules(offer)
        # 5) порядок детей — как в (26)
        reorder_children_same_skeleton(offer)
        out_offers.append(offer)

    # отступы как в (26) — два пробела
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # FEED_META в начале
    out_root.insert(0, ET.Comment(render_feed_meta_comment(len(src_offers), len(list(out_offers.findall("offer"))))))

    # запись: windows-1251, LF
    xml_bytes = ET.tostring(out_root, encoding=ENCODING, xml_declaration=True)
    xml_text = xml_bytes.decode(ENCODING, errors="replace").replace("\r\n","\n")
    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=ENCODING, newline="\n") as f:
        f.write(xml_text)

    log(f"Wrote: {OUT_FILE} (offers: {len(list(out_offers))})")

if __name__ == "__main__":
    main()
