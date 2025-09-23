# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint → Satu YML (минимальная правка структуры)
script_version = nvprint-2025-09-23.2

Что изменено по задаче:
1) Убраны полностью <categories> и любые <categoryId> у офферов.
2) У <offer> удаляем атрибуты available/in_stock и создаём РОВНО ОДИН дочерний <available>.
3) Не выводим теги остатков <quantity_in_stock> и <quantity>.

Остальная логика простая: читаем XML, достаём базовые поля и пишем YML.
Цены/бренды/префиксы и т.п. здесь не трогаем (по просьбе «пока всё»).
"""

from __future__ import annotations
import os, re, sys, html, time, random
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

import requests
import xml.etree.ElementTree as ET

# ===================== НАСТРОЙКИ (ENV) =====================

SUPPLIER_URL    = (os.getenv("NVPRINT_XML_URL") or os.getenv("SUPPLIER_URL") or "").strip()
OUT_FILE        = os.getenv("OUT_FILE", "docs/nvprint.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "windows-1251") or "windows-1251")
TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "45"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF_S = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))

NV_USER         = (os.getenv("NVPRINT_LOGIN") or "").strip()
NV_PASS         = (os.getenv("NVPRINT_PASSWORD") or "").strip()

UA = {"User-Agent": "supplier-feeds/nvprint-min 1.0"}

# ===================== УТИЛИТЫ =====================

def log(msg: str) -> None: print(msg, flush=True)
def warn(msg: str) -> None: print("WARN: "+msg, file=sys.stderr, flush=True)
def err(msg: str, code: int = 1) -> None: print("ERROR: "+msg, file=sys.stderr, flush=True); sys.exit(code)
def x(s: str) -> str: return html.escape((s or "").strip())

def parse_float(v: Optional[str]) -> Optional[float]:
    if not v: return None
    t = v.replace("\xa0"," ").replace(" ","").replace(",",".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m: return None
    try: return float(m.group(0))
    except Exception: return None

# ===================== HTTP =====================

def fetch_xml_bytes(url: str) -> bytes:
    if not url: err("NVPRINT_XML_URL не задан")
    sess = requests.Session()
    auth = (NV_USER, NV_PASS) if (NV_USER or NV_PASS) else None
    last = None
    for i in range(1, RETRIES+1):
        try:
            r = sess.get(url, timeout=TIMEOUT_S, headers=UA, auth=auth)
            r.raise_for_status()
            b = r.content
            if len(b) < MIN_BYTES: raise RuntimeError(f"too small ({len(b)} bytes)")
            return b
        except Exception as e:
            last = e
            if i < RETRIES:
                sl = RETRY_BACKOFF_S*i*(1.0+random.uniform(-0.2,0.2))
                warn(f"try {i}/{RETRIES} failed: {e}; sleep {sl:.1f}s")
                time.sleep(sl)
    err(f"fetch failed: {last}")

# ===================== ПАРСИНГ XML → ПОЛЯ =====================

def strip_ns(tag: str) -> str:
    return tag.split("}",1)[1] if "}" in tag else tag

# Эвристики для тегов
NAME_TAGS   = ["НоменклатураКратко","Номенклатура","name","title","наименование","FullName","НаименованиеТовара"]
SKU_TAGS    = ["Артикул","sku","vendorcode","Код","КодТовара","Code","Code1C"]
PRICE_TAGS  = ["ЦенаТенге","PriceKZT","price_kzt","Цена","price","Amount","Value"]
DESC_TAGS   = ["Описание","ПолноеОписание","Description","FullDescription"]
IMG_LIKE    = ["image","img","photo","picture","картин","изобр","фото"]
QTY_LIKE    = ["колич","остат","qty","quantity","stock","free","balance","amount","count"]
AVAIL_LIKE  = ["налич","avail","available","status","доступ"]

IMG_RE      = re.compile(r"https?://[^\s'\"<>]+?\.(?:jpg|jpeg|png|webp|gif)(?:\?[^\s'\"<>]*)?$", re.I)

def first_text(node: ET.Element, names: List[str]) -> Optional[str]:
    names_l = {n.lower() for n in names}
    for ch in node.iter():
        nm = strip_ns(ch.tag).lower()
        if nm in names_l:
            t = (ch.text or "").strip() if ch.text else ""
            if t: return t
    return None

def collect_images(node: ET.Element, limit: int = 6) -> List[str]:
    pics: List[str] = []
    for ch in node.iter():
        nm = strip_ns(ch.tag).lower()
        if any(k in nm for k in IMG_LIKE):
            if ch.text:
                pics += IMG_RE.findall(ch.text.strip())
            for v in (ch.attrib or {}).values():
                pics += IMG_RE.findall(str(v))
    # uniq
    seen=set(); out=[]
    for u in pics:
        if u not in seen:
            seen.add(u); out.append(u)
        if len(out) >= limit: break
    return out

POS_WORDS = ["есть","в наличии","in stock","instock","true","yes","да","доступ"]
NEG_WORDS = ["нет","отсутств","out of stock","false","no","под заказ","ожидается"]

def parse_availability(node: ET.Element) -> bool:
    qty = 0
    avail: Optional[bool] = None
    for ch in node.iter():
        nm = strip_ns(ch.tag).lower()
        if any(k in nm for k in QTY_LIKE):
            for val in [ch.text] + list((ch.attrib or {}).values()):
                n = parse_float(val if isinstance(val,str) else None)
                if n and n > 0: qty = max(qty, int(round(n)))
        if any(k in nm for k in AVAIL_LIKE):
            t = (ch.text or "").strip().lower() if ch.text else ""
            if any(w in t for w in POS_WORDS): avail = True
            elif any(w in t for w in NEG_WORDS) and avail is None: avail = False
    return True if (avail is True or qty > 0) else False

def guess_items(root: ET.Element) -> List[ET.Element]:
    cands = root.findall(".//Товар") + root.findall(".//item") + root.findall(".//product") + root.findall(".//row")
    if cands: return cands
    out=[]
    for node in root.iter():
        if first_text(node, NAME_TAGS) or first_text(node, SKU_TAGS):
            out.append(node)
    return out

def parse_item(node: ET.Element) -> Optional[Dict[str,Any]]:
    name = first_text(node, NAME_TAGS)
    if not name: return None
    sku  = first_text(node, SKU_TAGS) or ""
    price=None
    for t in PRICE_TAGS:
        price = parse_float(first_text(node, [t]))
        if price: break
    if not price or price <= 0:
        price = 1.0
    desc = first_text(node, DESC_TAGS) or name
    pics = collect_images(node)
    available = parse_availability(node)
    return {
        "name": name.strip(),
        "sku": sku.strip(),
        "price": float(price),
        "description": desc.strip(),
        "pictures": pics,
        "available": available,
    }

# ===================== СБОРКА YML (без categories/categoryId и без qty-тегов) =====================

def build_yml(offers: List[Dict[str,Any]]) -> str:
    root = ET.Element("yml_catalog"); root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(root, "shop")
    offers_el = ET.SubElement(shop, "offers")

    for it in offers:
        # никаких атрибутов available/in_stock у <offer> — только id
        offer = ET.SubElement(offers_el, "offer"); offer.set("id", it["id"])

        name_el = ET.SubElement(offer, "name"); name_el.text = it["name"]
        if it.get("vendor"):
            ven = ET.SubElement(offer, "vendor"); ven.text = it["vendor"]
        if it.get("vendorCode"):
            vc = ET.SubElement(offer, "vendorCode"); vc.text = it["vendorCode"]

        price = ET.SubElement(offer, "price"); price.text = str(int(round(it["price"])))
        cur = ET.SubElement(offer, "currencyId"); cur.text = "KZT"

        # Никаких <categoryId> и вообще нет раздела <categories> в этом файле!

        for u in (it.get("pictures") or []):
            pic = ET.SubElement(offer, "picture"); pic.text = u

        desc = ET.SubElement(offer, "description"); desc.text = it["description"]

        # РОВНО ОДИН тег <available>
        av = ET.SubElement(offer, "available"); av.text = "true" if it["available"] else "false"

        # Никаких <quantity_in_stock> или <quantity> — не выводим их совсем!

    # красивый отступ + пустая строка между офферами
    try: ET.indent(root, space="  ")
    except Exception: pass

    xml = ET.tostring(root, encoding=OUTPUT_ENCODING, xml_declaration=True).decode(OUTPUT_ENCODING, errors="replace")
    xml = re.sub(r"(</offer>)\n\s*(<offer\b)", r"\1\n\n    \2", xml)
    return xml

# ===================== MAIN =====================

def main() -> int:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    b = fetch_xml_bytes(SUPPLIER_URL)
    root = ET.fromstring(b)
    items = guess_items(root)
    log(f"[nvprint] items detected: {len(items)}")

    parsed: List[Dict[str,Any]] = []
    for i, node in enumerate(items, 1):
        it = parse_item(node)
        if not it: continue

        # Формируем id и vendorCode максимально из артикула, иначе из name
        base = it.get("sku") or it.get("name") or f"nv-{i}"
        oid  = re.sub(r"[^\w\-]+","-", base).strip("-") or f"nv-{i}"

        parsed.append({
            "id": oid,
            "name": it["name"],
            "vendor": None,                  # как есть; при необходимости добавим позже
            "vendorCode": it.get("sku") or "",
            "price": it["price"],
            "pictures": it["pictures"],
            "description": it["description"],
            "available": it["available"],
        })

    # Пишем YML без <categories>/<categoryId>, с единым <available> и без qty-тегов
    xml = build_yml(parsed)
    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml)

    log(f"Wrote: {OUT_FILE} | offers={len(parsed)} | encoding={OUTPUT_ENCODING}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        err(str(e), 2)
