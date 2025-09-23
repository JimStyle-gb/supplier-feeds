# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint → Satu YML (сохранение исходных id; vendorCode из id по правилу NP + (id без NV-))
script_version = nvprint-2025-09-23.6

Требования, реализованные здесь:
- id оффера НЕ меняем: берём ровно как в источнике.
- vendorCode формируем из id: NV-XXXX → NPXXXX (без дефиса).
- Всегда <available>true</available>; атрибуты available/in_stock и любые старые теги <available> удаляем.
- Полностью убираем <categories> и все <categoryId>.
- Удаляем <quantity_in_stock> и <quantity>.
- Фильтр: имя должно НАЧИНАТЬСЯ с любого слова из docs/nvprint_keywords.txt (авто-детект кодировки).
- Выводим в windows-1251; пустая строка между <offer> для читабельности.
"""

from __future__ import annotations
import os, re, sys, html, time, random
from typing import List, Optional
from datetime import datetime
import xml.etree.ElementTree as ET
import requests

# ---------------------------- ENV ----------------------------
SUPPLIER_URL    = (os.getenv("NVPRINT_XML_URL") or os.getenv("SUPPLIER_URL") or "").strip()
OUT_FILE        = os.getenv("OUT_FILE", "docs/nvprint.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "windows-1251") or "windows-1251")
TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "45"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF_S = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))
NV_USER         = (os.getenv("NVPRINT_LOGIN") or "").strip()
NV_PASS         = (os.getenv("NVPRINT_PASSWORD") or "").strip()
KEYWORDS_PATH   = os.getenv("NVPRINT_KEYWORDS_PATH", "docs/nvprint_keywords.txt")

UA = {"User-Agent": "supplier-feeds/nvprint 1.0"}

# ---------------------------- utils ----------------------------
def log(s: str) -> None: print(s, flush=True)
def warn(s: str) -> None: print("WARN: " + s, file=sys.stderr, flush=True)
def err(s: str, code: int = 1) -> None: print("ERROR: " + s, file=sys.stderr, flush=True); sys.exit(code)

def file_read_autoenc(path: str) -> str:
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read().replace("\ufeff","").replace("\x00","")
        except Exception:
            pass
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read().replace("\x00","")

def load_keywords(path: str) -> List[str]:
    if not os.path.isfile(path): return []
    data = file_read_autoenc(path)
    out=[]
    for ln in data.splitlines():
        s = ln.strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out

def compile_prefix_patterns(kws: List[str]) -> List[re.Pattern]:
    pats=[]
    for kw in kws:
        k = re.sub(r"\s+"," ", kw.strip())
        if not k: continue
        pats.append(re.compile(r"^\s*"+re.escape(k)+r"(?!\w)", re.I))
    return pats

def name_starts_with(name: str, pats: List[re.Pattern]) -> bool:
    if not pats: return True
    return any(p.search(name or "") for p in pats)

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

def parse_float(v: Optional[str]) -> Optional[float]:
    if not v: return None
    t = v.replace("\xa0"," ").replace(" ","").replace(",",".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m: return None
    try: return float(m.group(0))
    except Exception: return None

def strip_ns(tag: str) -> str:
    return tag.split("}",1)[1] if "}" in tag else tag

# ---------------------------- core ----------------------------
def process_tree(src_root: ET.Element, kw_pats: List[re.Pattern]) -> ET.Element:
    # создаём чистый каркас
    out_root = ET.Element("yml_catalog"); out_root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")
    out_offers = ET.SubElement(out_shop, "offers")

    # найдём offers в источнике
    shop_in = src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    offers_in = None
    if shop_in is not None:
        # Удаляем из выходного файла <categories> вообще (не копируем)
        offers_in = shop_in.find("offers") or shop_in.find("Offers")
    # если offers не нашли, пройдёмся эвристически
    items = list(offers_in.findall("offer")) if offers_in is not None else src_root.findall(".//offer")

    for off in items:
        # --- ID: берём РОВНО как в источнике ---
        src_id = (off.attrib.get("id") or "").strip()
        if not src_id:  # без id пропускаем
            continue

        # name (для фильтра)
        name_el = off.find("name")
        name = (name_el.text or "").strip() if name_el is not None and name_el.text else ""
        if not name:
            continue
        if kw_pats and not name_starts_with(name, kw_pats):
            continue

        # ---- vendorCode из id ----
        # NV-XXXX → NPXXXX (без дефиса), иначе NP + очищенный id
        if src_id.upper().startswith("NV-"):
            base = src_id[3:]  # без "NV-"
        else:
            base = src_id
        base = re.sub(r"[^A-Za-z0-9]+", "", base).upper()
        vendor_code = "NP" + base if base else "NPNA"

        # ---- собираем выходной offer ----
        new = ET.SubElement(out_offers, "offer"); new.set("id", src_id)

        # name
        ET.SubElement(new, "name").text = name

        # vendor (если был — переносим как есть)
        ven = off.find("vendor")
        if ven is not None and (ven.text or "").strip():
            ET.SubElement(new, "vendor").text = ven.text.strip()

        # vendorCode (перезаписываем по правилу)
        ET.SubElement(new, "vendorCode").text = vendor_code

        # price
        price_txt = None
        pnode = off.find("price")
        if pnode is not None and (pnode.text or "").strip():
            price_txt = pnode.text.strip()
        price_val = parse_float(price_txt) or 1.0
        ET.SubElement(new, "price").text = str(int(round(price_val)))
        ET.SubElement(new, "currencyId").text = "KZT"

        # picture (переносим все)
        for p in off.findall("picture"):
            url = (p.text or "").strip()
            if url:
                ET.SubElement(new, "picture").text = url

        # description (переносим как есть)
        desc = off.find("description")
        ET.SubElement(new, "description").text = (desc.text or name).strip() if desc is not None else name

        # available: всегда true (сначала удалим возможные атрибуты/теги)
        # (в исходнике эти атрибуты у "off", но мы их НЕ копируем; просто создаём 1 тег)
        ET.SubElement(new, "available").text = "true"

    # красивый отступ + пустая строка между офферами
    try: ET.indent(out_root, space="  ")
    except Exception: pass

    return out_root

# ---------------------------- main ----------------------------
def main() -> int:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    b = fetch_xml_bytes(SUPPLIER_URL)
    src_root = ET.fromstring(b)

    # читаем keywords и компилим паттерны "startswith"
    kws = load_keywords(KEYWORDS_PATH)
    kw_pats = compile_prefix_patterns(kws)

    out_root = process_tree(src_root, kw_pats)

    # сериализация
    xml = ET.tostring(out_root, encoding=OUTPUT_ENCODING, xml_declaration=True).decode(OUTPUT_ENCODING, errors="replace")

    # убираем вообще секцию <categories> если вдруг была (не создавали, но на всякий)
    xml = re.sub(r"\s*<categories>.*?</categories>\s*", "", xml, flags=re.S|re.I)
    # убираем любые <categoryId> (на случай если попадут в description из источника)
    xml = re.sub(r"\s*<categoryId\b[^>]*>.*?</categoryId>\s*", "", xml, flags=re.S|re.I)
    # удаляем quantity_in_stock/quantity, если случайно просочились из описаний
    xml = re.sub(r"\s*<quantity_in_stock\b[^>]*>.*?</quantity_in_stock>\s*", "", xml, flags=re.S|re.I)
    xml = re.sub(r"\s*<quantity\b[^>]*>.*?</quantity>\s*", "", xml, flags=re.S|re.I)

    # пустая строка между офферами (читабельность)
    xml = re.sub(r"(</offer>)\n\s*(<offer\b)", r"\1\n\n    \2", xml)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml)

    log(f"Wrote: {OUT_FILE} | encoding={OUTPUT_ENCODING} | keywords={len(kws)}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        err(str(e), 2)
