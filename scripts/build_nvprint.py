# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint → Satu YML (ровно 5 правок по ТЗ)
script_version = nvprint-2025-09-24.1

Выполняет:
1) Удаляет <categories> и все <categoryId>.
2) У <offer> удаляет атрибуты available/in_stock и добавляет один тег <available>.
3) Удаляет <quantity_in_stock> и <quantity>.
4) Читает docs/nvprint_keywords.txt с авто-детектом кодировки; фильтр: name НАЧИНАЕТСЯ с ключевого слова.
5) <available>true для всех.

Прочее:
- id оффера сохраняется как в источнике.
- Остальные теги копируются без изменений (кроме перечисленных вычеркнутых).
- Вывод кодируется в windows-1251 (можно поменять через OUTPUT_ENCODING).
"""

from __future__ import annotations
import os, re, sys, html, time, random
from typing import List
from datetime import datetime
import requests
import xml.etree.ElementTree as ET

# ================= ENV =================
SUPPLIER_URL    = (os.getenv("NVPRINT_XML_URL") or os.getenv("SUPPLIER_URL") or "").strip()
OUT_FILE        = os.getenv("OUT_FILE", "docs/nvprint.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "windows-1251") or "windows-1251")
TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "60"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF_S = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))

KEYWORDS_PATH   = os.getenv("NVPRINT_KEYWORDS_PATH", "docs/nvprint_keywords.txt")

UA = {"User-Agent": "supplier-feeds/nvprint/5fix"}

# =============== utils ===============
def log(s: str) -> None: print(s, flush=True)
def warn(s: str) -> None: print("WARN: "+s, file=sys.stderr, flush=True)
def err(s: str, code: int=1) -> None: print("ERROR: "+s, file=sys.stderr, flush=True); sys.exit(code)
def strip_ns(tag: str) -> str: return tag.split("}",1)[1] if "}" in tag else tag

def fetch_xml_bytes(url: str) -> bytes:
    if not url: err("NVPRINT_XML_URL не задан")
    last = None
    for i in range(1, RETRIES+1):
        try:
            r = requests.get(url, headers=UA, timeout=TIMEOUT_S)
            r.raise_for_status()
            b = r.content
            if len(b) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(b)} bytes)")
            return b
        except Exception as e:
            last = e
            if i < RETRIES:
                sl = RETRY_BACKOFF_S*i*(1.0+random.uniform(-0.2,0.2))
                warn(f"try {i}/{RETRIES} failed: {e}; sleep {sl:.1f}s")
                time.sleep(sl)
    err(f"fetch failed: {last}")

def file_read_autoenc(path: str) -> str:
    # авто-детект кодировки для nvprint_keywords.txt
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251","cp866","koi8-r"):
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
    out: List[str] = []
    for line in data.splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out

def compile_prefix_patterns(kws: List[str]) -> List[re.Pattern]:
    pats=[]
    for kw in kws:
        k = re.sub(r"\s+", " ", kw.strip())
        if not k: continue
        # якорим в начало имени
        pats.append(re.compile(r"^\s*"+re.escape(k)+r"(?!\w)", re.I))
    return pats

def name_starts_with(name: str, pats: List[re.Pattern]) -> bool:
    if not pats: return True
    return any(p.search(name or "") for p in pats)

# =============== core ===============
def collect_offers(src_root: ET.Element) -> List[ET.Element]:
    # Находим все узлы offer без учёта namespace/регистра
    offers=[]
    for node in src_root.iter():
        if strip_ns(node.tag).lower() == "offer":
            offers.append(node)
    return offers

def copy_allowed_children(src_offer: ET.Element, dst_offer: ET.Element) -> None:
    """
    Копируем все дочерние теги, КРОМЕ:
    - categoryId
    - quantity_in_stock
    - quantity
    - available (добавим свой позже)
    """
    SKIP = {"categoryid","quantity_in_stock","quantity","available"}
    for ch in list(src_offer):
        nm = strip_ns(ch.tag).lower()
        if nm in SKIP:
            continue
        # копируем тег как есть (включая вложенность)
        dst_offer.append(_deep_copy(ch))

def _deep_copy(el: ET.Element) -> ET.Element:
    new = ET.Element(strip_ns(el.tag))
    # копируем текст/атрибуты
    new.text = el.text
    for k, v in (el.attrib or {}).items():
        new.set(k, v)
    # копируем детей
    for c in list(el):
        new.append(_deep_copy(c))
    return new

def build_output(offers_nodes: List[ET.Element], kw_pats: List[re.Pattern]) -> ET.Element:
    out_root = ET.Element("yml_catalog"); out_root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(out_root, "shop")
    offers_out = ET.SubElement(shop, "offers")

    kept = 0
    for off in offers_nodes:
        # id обязателен
        src_id = (off.attrib.get("id") or "").strip()
        if not src_id:
            # иногда id может быть дочерним тегом — пробуем
            id_node = off.find("./id")
            if id_node is not None and (id_node.text or "").strip():
                src_id = id_node.text.strip()
        if not src_id:
            continue

        # фильтр по имени (startswith)
        name_node = off.find("./name")
        name_text = (name_node.text or "").strip() if (name_node is not None and name_node.text) else ""
        if kw_pats and not name_starts_with(name_text, kw_pats):
            continue

        # собираем новый <offer id="..."> БЕЗ атрибутов available/in_stock
        new_off = ET.SubElement(offers_out, "offer"); new_off.set("id", src_id)

        # копируем все разрешённые теги как есть
        copy_allowed_children(off, new_off)

        # добавляем наш единый <available>true</available>
        av = new_off.find("./available")
        if av is not None:
            new_off.remove(av)
        ET.SubElement(new_off, "available").text = "true"

        kept += 1

    # красивый отступ
    try: ET.indent(out_root, space="  ")
    except Exception: pass
    return out_root

# =============== main ===============
def main() -> int:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    xml_bytes = fetch_xml_bytes(SUPPLIER_URL)
    src_root = ET.fromstring(xml_bytes)

    # загружаем ключевые слова и компилим startswith-паттерны
    kws = load_keywords(KEYWORDS_PATH)
    kw_pats = compile_prefix_patterns(kws)
    log(f"keywords: {len(kws)}")

    # собираем офферы из источника
    offer_nodes = collect_offers(src_root)
    log(f"offers found (raw): {len(offer_nodes)}")

    out_root = build_output(offer_nodes, kw_pats)

    # сериализация
    xml = ET.tostring(out_root, encoding=OUTPUT_ENCODING, xml_declaration=True).decode(OUTPUT_ENCODING, errors="replace")

    # гарантийно убираем <categories> и любые <categoryId> (если вдруг подсосались из вложений)
    xml = re.sub(r"\s*<categories>.*?</categories>\s*", "", xml, flags=re.S|re.I)
    xml = re.sub(r"\s*<categoryId\b[^>]*>.*?</categoryId>\s*", "", xml, flags=re.S|re.I)

    # убираем остатки quantity* (если где-то глубоко встретились)
    xml = re.sub(r"\s*<quantity_in_stock\b[^>]*>.*?</quantity_in_stock>\s*", "", xml, flags=re.S|re.I)
    xml = re.sub(r"\s*<quantity\b[^>]*>.*?</quantity>\s*", "", xml, flags=re.S|re.I)

    # пустая строка между офферами для читабельности
    xml = re.sub(r"(</offer>)\n\s*(<offer\b)", r"\1\n\n    \2", xml)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml)
    log(f"Wrote: {OUT_FILE} | encoding={OUTPUT_ENCODING}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        err(str(e), 2)
