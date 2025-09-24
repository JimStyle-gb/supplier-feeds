# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint -> Satu YML
script_version = nvprint-2025-09-23.5

Что делает:
- Читает исходный XML/YML NVPrint.
- Если источник уже YML с <shop>/<offers>/<offer>, берём ИСХОДНЫЙ offer[@id] и только заменяем префикс NV- -> NP-.
- Если источник в другом формате, пытаемся распарсить товары эвристически (как прежде).
- Фильтр по keywords из docs/nvprint_keywords.txt (автодетект кодировки): оставляем товары, у которых name НАЧИНАЕТСЯ с любого ключевого слова.
- В выводе:
  - <categories> и <categoryId> отсутствуют.
  - У <offer> только id (с заменой NV- на NP-), один дочерний <available>true</available>.
  - <vendorCode> формируем из поставщицкого кода/атрикула/ид, заменяя NV- -> NP- и гарантируя префикс NP-.
  - Не выводим quantity/остатки. Валюта KZT.
"""

from __future__ import annotations
import os, re, sys, html, time, random
from typing import Any, Dict, List, Optional
from datetime import datetime

import requests
import xml.etree.ElementTree as ET

# ---------------------------- настройки ----------------------------

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

# ---------------------------- утилиты ----------------------------

def log(msg: str) -> None: print(msg, flush=True)
def warn(msg: str) -> None: print("WARN: "+msg, file=sys.stderr, flush=True)
def err(msg: str, code: int = 1) -> None: print("ERROR: "+msg, file=sys.stderr, flush=True); sys.exit(code)
def x(s: str) -> str: return html.escape((s or "").strip())

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
    if not path or not os.path.isfile(path): return []
    data = file_read_autoenc(path)
    out: List[str] = []
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

def starts_with_any(name: str, pats: List[re.Pattern]) -> bool:
    if not pats: return True
    return any(p.search(name or "") for p in pats)

def parse_float(v: Optional[str]) -> Optional[float]:
    if not v: return None
    t = v.replace("\xa0"," ").replace(" ","").replace(",",".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m: return None
    try: return float(m.group(0))
    except Exception: return None

# ---------------------------- сеть/вход ----------------------------

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

# ---------------------------- парсинг произвольного XML ----------------------------

def strip_ns(tag: str) -> str:
    return tag.split("}",1)[1] if "}" in tag else tag

NAME_TAGS   = ["НоменклатураКратко","Номенклатура","name","title","наименование","FullName","НаименованиеТовара"]
SKU_TAGS    = ["Артикул","sku","vendorcode","Код","КодТовара","Code","Code1C"]
PRICE_TAGS  = ["ЦенаТенге","PriceKZT","price_kzt","Цена","price","Amount","Value"]
DESC_TAGS   = ["Описание","ПолноеОписание","Description","FullDescription"]
IMG_LIKE    = ["image","img","photo","picture","картин","изобр","фото"]
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
            if ch.text: pics += IMG_RE.findall(ch.text.strip())
            for v in (ch.attrib or {}).values():
                pics += IMG_RE.findall(str(v))
    # uniq
    seen=set(); out=[]
    for u in pics:
        if u not in seen:
            seen.add(u); out.append(u)
        if len(out) >= limit: break
    return out

def guess_items(root: ET.Element) -> List[ET.Element]:
    cands = root.findall(".//Товар") + root.findall(".//item") + root.findall(".//product") + root.findall(".//row")
    if cands: return cands
    out=[]
    for node in root.iter():
        if first_text(node, NAME_TAGS) or first_text(node, SKU_TAGS):
            out.append(node)
    return out

# ---------------------------- нормализация префикса ----------------------------

NV_PREFIX_RX = re.compile(r"^NV-", re.I)

def to_np_prefix(s: str) -> str:
    if not s: return s
    return NV_PREFIX_RX.sub("NP-", s.strip())

def ensure_vendorcode_np(raw: str, fallback: str) -> str:
    base = (raw or "").strip() or (fallback or "").strip()
    base = to_np_prefix(base)
    # если после замены всё ещё нет NP- в начале (не было NV- у поставщика), добавим NP-
    if not base.upper().startswith("NP-"):
        base = "NP-" + re.sub(r"^\-+", "", base)
    # чистим странные символы, но оставляем дефисы
    base = re.sub(r"[^A-Za-z0-9\-]+", "", base).upper()
    return base or "NP-NA"

# ---------------------------- сборка YML ----------------------------

def build_yml(offers: List[Dict[str,Any]]) -> str:
    root = ET.Element("yml_catalog"); root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(root, "shop")
    offers_el = ET.SubElement(shop, "offers")

    for it in offers:
        offer = ET.SubElement(offers_el, "offer"); offer.set("id", it["id"])
        name_el = ET.SubElement(offer, "name"); name_el.text = it["name"]
        if it.get("vendor"):
            ven = ET.SubElement(offer, "vendor"); ven.text = it["vendor"]
        vc = ET.SubElement(offer, "vendorCode"); vc.text = it["vendorCode"]
        price_el = ET.SubElement(offer, "price"); price_el.text = str(int(round(it["price"])))
        cur = ET.SubElement(offer, "currencyId"); cur.text = "KZT"
        for u in (it.get("pictures") or []):
            p = ET.SubElement(offer, "picture"); p.text = u
        d = ET.SubElement(offer, "description"); d.text = it["description"]
        av = ET.SubElement(offer, "available"); av.text = "true"  # всегда true

    try: ET.indent(root, space="  ")
    except Exception: pass

    xml = ET.tostring(root, encoding=OUTPUT_ENCODING, xml_declaration=True).decode(OUTPUT_ENCODING, errors="replace")
    # пустая строка между офферами
    xml = re.sub(r"(</offer>)\n\s*(<offer\b)", r"\1\n\n    \2", xml)
    return xml

# ---------------------------- MAIN ----------------------------

def main() -> int:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    b = fetch_xml_bytes(SUPPLIER_URL)
    src = ET.fromstring(b)

    kws = load_keywords(KEYWORDS_PATH)
    pats = compile_prefix_patterns(kws)

    out_items: List[Dict[str,Any]] = []

    # --- путь 1: источник уже в YML-формате с <offers>/<offer> ---
    shop = src.find("shop")
    offers_el = shop.find("offers") if shop is not None else None
    if offers_el is not None:
        for off in offers_el.findall("offer"):
            # исходный id -> только заменить NV- на NP-
            src_id = off.attrib.get("id","").strip()
            if not src_id:  # без id смысла нет
                continue
            new_id = to_np_prefix(src_id)

            name = (off.findtext("name") or "").strip()
            if not name:  # без имени пропускаем
                continue
            if kws and not starts_with_any(name, pats):
                continue

            # vendorCode
            vc_src = (off.findtext("vendorCode") or off.attrib.get("article") or src_id).strip()
            vendorCode = ensure_vendorcode_np(vc_src, fallback=src_id)

            # price
            price_txt = (off.findtext("price") or "1").strip()
            price = parse_float(price_txt) or 1.0

            # description
            desc = (off.findtext("description") or name).strip()

            # pictures
            pics = [ (p.text or "").strip() for p in off.findall("picture") if (p.text or "").strip() ]

            # собираем минимальный оффер
            out_items.append({
                "id": new_id,                          # <-- строго сохранили исходный id с заменой NV- -> NP-
                "name": name,
                "vendor": (off.findtext("vendor") or "").strip() or None,
                "vendorCode": vendorCode,              # <-- всегда NP-...
                "price": price,
                "pictures": pics,
                "description": desc,
            })
    else:
        # --- путь 2: произвольный XML, эвристический разбор (как раньше) ---
        items = guess_items(src)
        for i, node in enumerate(items, 1):
            name = first_text(node, NAME_TAGS)
            if not name: continue
            if kws and not starts_with_any(name, pats): continue

            sku  = first_text(node, SKU_TAGS) or ""
            price=None
            for t in PRICE_TAGS:
                price = parse_float(first_text(node, [t]))
                if price: break
            if not price: price = 1.0

            desc = first_text(node, DESC_TAGS) or name
            pics = collect_images(node)

            base_id = sku or name or f"nv-{i}"
            src_id  = re.sub(r"[^\w\-]+","-", base_id).strip("-") or f"nv-{i}"
            new_id  = to_np_prefix(src_id)
            vendorCode = ensure_vendorcode_np(sku, fallback=src_id)

            out_items.append({
                "id": new_id,
                "name": name.strip(),
                "vendor": None,
                "vendorCode": vendorCode,
                "price": float(price),
                "pictures": pics,
                "description": desc.strip(),
            })

    # записываем YML (без categories/categoryId и без quantity-тегов, available=true)
    xml = build_yml(out_items)
    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml)

    log(f"Wrote: {OUT_FILE} | offers={len(out_items)} | encoding={OUTPUT_ENCODING} | keywords={len(kws)}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        err(str(e), 2)
