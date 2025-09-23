# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint → Satu YML (плоские <offers>)
script_version = nvprint-2025-09-23.1

Что делает:
- Скачивает XML API NVPrint (можно с basic-auth).
- Фильтрует товары: ИМЯ ДОЛЖНО НАЧИНАТЬСЯ с любого слова из docs/nvprint_keywords.txt.
- Цены: применяет те же правила наценки, что у akcent/alstyle (процент + фикс + «хвост 900»).
- Артикул: срезает у поставщика префикс NV- и ставит наш префикс NP в <vendorCode>.
- <available>: вычисляет и оставляет ровно один тег; атрибуты/дубликаты удаляет.
- <picture>: подтягивает все URL-картинки, если встречаются.
- Чистит «Артикул: …» из описания, удаляет служебные поля.
- Не пишет <url>, <categories>, <categoryId>.
- FEED_META выровнен по колонкам и закрывается `-->` прямо перед `<shop>`; между офферами — пустая строка.
"""

from __future__ import annotations
import os, re, sys, io, html, time, random, hashlib
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

import requests
import xml.etree.ElementTree as ET

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# ===================== НАСТРОЙКИ (ENV) =====================

SUPPLIER_NAME   = os.getenv("SUPPLIER_NAME", "nvprint")
SUPPLIER_URL    = (os.getenv("NVPRINT_XML_URL") or os.getenv("SUPPLIER_URL") or "").strip()
NV_USER         = (os.getenv("NVPRINT_LOGIN") or "").strip()
NV_PASS         = (os.getenv("NVPRINT_PASSWORD") or "").strip()

OUT_FILE        = os.getenv("OUT_FILE", "docs/nvprint.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "windows-1251") or "windows-1251")
TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "45"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF_S = float(os.getenv("RETRY_BACKOFF_S", "2.0"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))

# фильтр по ключевым словам (только prefix-режим!)
KEYWORDS_PATH   = os.getenv("NVPRINT_KEYWORDS_PATH", "docs/nvprint_keywords.txt")
KEYWORDS_MODE   = "prefix"   # фиксируем для NVPrint

# префиксы артикулов
SUPPL_PREFIX    = os.getenv("NVPRINT_SUPPLIER_PREFIX", "NV-")
OUR_PREFIX      = os.getenv("VENDORCODE_PREFIX", "NP")

# наличие
FORCE_AVAILABLE = os.getenv("NVPRINT_FORCE_AVAILABLE", "0") in {"1","true","yes"}

# ===================== УТИЛИТЫ =====================

def log(msg: str) -> None: print(msg, flush=True)
def warn(msg: str) -> None: print("WARN: "+msg, file=sys.stderr, flush=True)
def err(msg: str, code: int = 1) -> None: print("ERROR: "+msg, file=sys.stderr, flush=True); sys.exit(code)

def now_utc() -> str: return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
def now_almaty() -> str:
    if ZoneInfo: return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S +05")
    return time.strftime("%Y-%m-%d %H:%M:%S +05")

def x(s: str) -> str: return html.escape((s or "").strip())

def parse_float(v: Optional[str]) -> Optional[float]:
    if not v: return None
    t = v.replace("\xa0"," ").replace(" ","").replace(",",".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m: return None
    try: return float(m.group(0))
    except Exception: return None

def strip_supplier_prefix(code: str, supp_pref: str) -> str:
    s = (code or "").strip()
    if not s: return ""
    if supp_pref and s.upper().startswith(supp_pref.upper()):
        s = s[len(supp_pref):]
    # убираем повторные NV-, пробелы, тире
    s = re.sub(r"^(NV-)+", "", s, flags=re.I)
    s = re.sub(r"\s+", "", s)
    return s

def normalize_vendor_code(raw_code: str) -> str:
    core = strip_supplier_prefix(raw_code, SUPPL_PREFIX)
    core = re.sub(r"[^A-Za-z0-9\-]+","", core)
    return f"{OUR_PREFIX}{core}" if core else OUR_PREFIX

def file_read_autoenc(path: str) -> str:
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f: return f.read().replace("\ufeff","").replace("\x00","")
        except Exception: pass
    with open(path,"r",encoding="utf-8",errors="ignore") as f:
        return f.read().replace("\x00","")

def load_keywords(path: str) -> List[str]:
    if not os.path.isfile(path): return []
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
        pats.append(re.compile(r"^\s*"+re.escape(k)+r"(?!\w)", re.I))
    return pats

def name_starts_with(name: str, patterns: List[re.Pattern]) -> bool:
    if not patterns: return True
    return any(p.search(name or "") for p in patterns)

# ===================== HTTP =====================

UA = {"User-Agent": "supplier-feeds/nvprint 1.0"}

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
                sleep = RETRY_BACKOFF_S*i*(1.0+random.uniform(-0.2,0.2))
                warn(f"try {i}/{RETRIES} failed: {e}; sleep {sleep:.1f}s")
                time.sleep(sleep)
    err(f"fetch failed: {last}")

# ===================== ПАРСИНГ XML =====================

# эвристический набор тэгов
NAME_TAGS   = ["НоменклатураКратко","Номенклатура","name","title","наименование","FullName","НаименованиеТовара"]
SKU_TAGS    = ["Артикул","sku","vendorcode","Код","КодТовара","Code","Code1C"]
PRICE_TAGS  = ["ЦенаТенге","PriceKZT","price_kzt","Цена","price","Amount","Value"]
DESC_TAGS   = ["Описание","ПолноеОписание","Description","FullDescription"]
IMG_LIKE    = ["image","img","photo","picture","картин","изобр","фото"]
QTY_LIKE    = ["колич","остат","qty","quantity","stock","free","balance","amount","count"]
AVAIL_LIKE  = ["налич","avail","available","status","доступ"]
IMG_RE      = re.compile(r"https?://[^\s'\"<>]+?\.(?:jpg|jpeg|png|webp|gif)(?:\?[^\s'\"<>]*)?$", re.I)

def strip_ns(tag: str) -> str:
    return tag.split("}",1)[1] if "}" in tag else tag

def find_first_text(node: ET.Element, names: List[str]) -> Optional[str]:
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
                for m in IMG_RE.findall(ch.text.strip()):
                    pics.append(m)
            for v in (ch.attrib or {}).values():
                for m in IMG_RE.findall(str(v)):
                    pics.append(m)
    uniq=[]; seen=set()
    for u in pics:
        if u not in seen:
            seen.add(u); uniq.append(u)
        if len(uniq) >= limit: break
    return uniq

def parse_availability(node: ET.Element) -> Tuple[bool,int]:
    """
    Возвращает (available, qty_int)
    Приоритет: числовые поля (qty/остаток) > текстовые статусы
    """
    qty = 0
    avail_flag: Optional[bool] = None
    for ch in node.iter():
        nm = strip_ns(ch.tag).lower()
        if any(k in nm for k in QTY_LIKE):
            if ch.text:
                n = parse_float(ch.text)
                if n and n > 0:
                    qty = max(qty, int(round(n)))
        if any(k in nm for k in AVAIL_LIKE):
            txt = (ch.text or "").strip().lower()
            if any(w in txt for w in ["есть","в наличии","in stock","instock","true","yes","доступ"]):
                avail_flag = True
            elif any(w in txt for w in ["нет","отсутств","out of stock","false","no","под заказ","ожидается"]):
                if avail_flag is None:
                    avail_flag = False
    available = (qty > 0) if (avail_flag is None) else bool(avail_flag)
    if FORCE_AVAILABLE:
        available = True
        if qty <= 0: qty = 1
    return available, (qty if qty > 0 else (1 if available else 0))

ART_LINE_RE = re.compile(r"(^|\n)\s*[-–—]?\s*Артикул\s*:\s*[^\n]+(?=\n|$)", re.I)

def clean_description(s: str) -> str:
    if not s: return s
    s = ART_LINE_RE.sub(lambda m: ("" if m.group(1)=="" else m.group(1)), s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r"(\n\s*){3,}", "\n\n", s)
    return s.strip()

def guess_items(root: ET.Element) -> List[ET.Element]:
    # частые контейнеры
    cands = root.findall(".//Товар") + root.findall(".//item") + root.findall(".//product") + root.findall(".//row")
    if cands: return cands
    # fallback: любые узлы, у которых есть name или sku
    out=[]
    for node in root.iter():
        if find_first_text(node, NAME_TAGS) or find_first_text(node, SKU_TAGS):
            out.append(node)
    return out

def parse_item(node: ET.Element) -> Optional[Dict[str,Any]]:
    name = find_first_text(node, NAME_TAGS)
    if not name: return None
    sku  = find_first_text(node, SKU_TAGS) or ""

    price=None
    for t in PRICE_TAGS:
        v = find_first_text(node, [t])
        price = parse_float(v)
        if price: break
    if not price or price <= 0:
        return None

    desc = find_first_text(node, DESC_TAGS) or name
    pics = collect_images(node)
    available, qty = parse_availability(node)

    return {
        "name": name.strip(),
        "supplier_code": sku.strip(),
        "price_dealer": float(price),
        "description": desc.strip(),
        "pictures": pics,
        "available": available,
        "qty": qty,
    }

# ===================== ЦЕНООБРАЗОВАНИЕ =====================

class Rule:
    __slots__=("lo","hi","pct","add")
    def __init__(self, lo:int, hi:int, pct:float, add:int): self.lo, self.hi, self.pct, self.add = lo,hi,pct,add

PRICING_RULES: List[Rule] = [
    Rule(   101,    10000, 4.0,  3000),
    Rule( 10001,    25000, 4.0,  4000),
    Rule( 25001,    50000, 4.0,  5000),
    Rule( 50001,    75000, 4.0,  7000),
    Rule( 75001,   100000, 4.0, 10000),
    Rule(100001,   150000, 4.0, 12000),
    Rule(150001,   200000, 4.0, 15000),
    Rule(200001,   300000, 4.0, 20000),
    Rule(300001,   400000, 4.0, 25000),
    Rule(400001,   500000, 4.0, 30000),
    Rule(500001,   750000, 4.0, 40000),
    Rule(750001,  1000000, 4.0, 50000),
    Rule(1000001, 1500000, 4.0, 70000),
    Rule(1500001, 2000000, 4.0, 90000),
    Rule(2000001,100000000,4.0,100000),
]

def _force_tail_900(n: float) -> int:
    i = int(n)
    k = max(i // 1000, 0)
    out = k*1000 + 900
    return out if out >= 900 else 900

def retail_from_dealer(dealer: float) -> Optional[int]:
    for r in PRICING_RULES:
        if r.lo <= dealer <= r.hi:
            return _force_tail_900(dealer * (1 + r.pct/100.0) + r.add)
    return None

# ===================== FEED_META =====================

def render_feed_meta(pairs: List[Tuple[str,str,str]]) -> str:
    key_w = max(len(k) for k,_,_ in pairs)
    val_w = max(len(v) for _,v,_ in pairs)
    lines = ["<!--FEED_META"]
    for i,(k,v,c) in enumerate(pairs):
        tail = " -->" if i == len(pairs)-1 else ""
        lines.append(f"{k.ljust(key_w)} = {v.ljust(val_w)} | {c}{tail}")
    return "\n".join(lines)

# ===================== СБОРКА YML =====================

def build_yml(offers: List[Dict[str,Any]], feed_meta: str) -> str:
    root = ET.Element("yml_catalog"); root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(root, "shop")
    offers_el = ET.SubElement(shop, "offers")

    for it in offers:
        offer = ET.SubElement(offers_el, "offer"); offer.set("id", it["offer_id"])
        name_el = ET.SubElement(offer, "name"); name_el.text = it["title"]
        if it.get("vendor"):
            ven = ET.SubElement(offer, "vendor"); ven.text = it["vendor"]
        vc = ET.SubElement(offer, "vendorCode"); vc.text = it["vendorCode"]
        price = ET.SubElement(offer, "price"); price.text = str(int(it["price"]))
        cur = ET.SubElement(offer, "currencyId"); cur.text = "KZT"
        for u in (it.get("pictures") or []):
            pic = ET.SubElement(offer, "picture"); pic.text = u
        desc = ET.SubElement(offer, "description"); desc.text = it["description"]
        av = ET.SubElement(offer, "available"); av.text = "true" if it["available"] else "false"

    # красивый отступ + пустая строка между офферами
    try:
        ET.indent(root, space="  ")
    except Exception:
        pass
    xml = ET.tostring(root, encoding=OUTPUT_ENCODING, xml_declaration=True).decode(OUTPUT_ENCODING, errors="replace")
    # Вставляем FEED_META перед <shop>
    xml = xml.replace("<shop>", feed_meta + "\n  <shop>", 1)
    # Пустая строка между офферами
    xml = re.sub(r"(</offer>)\n\s*(<offer\b)", r"\1\n\n    \2", xml)
    return xml

# ===================== MAIN =====================

def main() -> int:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    b = fetch_xml_bytes(SUPPLIER_URL)
    root = ET.fromstring(b)

    items = guess_items(root)
    log(f"XML items detected: {len(items)}")

    # keywords (prefix)
    kws = load_keywords(KEYWORDS_PATH)
    pats = compile_prefix_patterns(kws)

    parsed: List[Dict[str,Any]] = []
    for node in items:
        it = parse_item(node)
        if not it: continue
        if kws and not name_starts_with(it["name"], pats):
            continue

        # цена → retail
        retail = retail_from_dealer(it["price_dealer"])
        if retail is None: continue

        # vendorCode с заменой NV- → NP
        base_code = it.get("supplier_code") or ""
        if not base_code:
            # пробуем вытащить из имени
            m = re.search(r"\b([A-Z0-9]{2,}(?:-[A-Z0-9]+)*)\b", it["name"], flags=re.I)
            base_code = m.group(1) if m else ""
        our_vcode = normalize_vendor_code(base_code)

        parsed.append({
            "offer_id":   our_vcode,                       # id = наш код
            "title":      it["name"],
            "vendor":     None,                            # NVPrint — не показываем; если надо — можно доопределить OEM
            "vendorCode": our_vcode,
            "price":      retail,
            "pictures":   it.get("pictures") or [],
            "description": clean_description(it.get("description") or it["name"]),
            "available":  bool(it.get("available")),
        })

    offers_written = len(parsed)

    feed = [
        ("script_version",    "nvprint-2025-09-23.1",            "Версия скрипта"),
        ("supplier",          SUPPLIER_NAME,                      "Метка поставщика"),
        ("source",            SUPPLIER_URL or "file",             "URL исходного XML"),
        ("offers_total",      str(len(items)),                    "Офферов у поставщика до очистки"),
        ("offers_written",    str(offers_written),                "Офферов записано (после очистки)"),
        ("keywords_mode",     KEYWORDS_MODE,                      "Режим фильтра ключевых слов"),
        ("keywords_loaded",   str(len(kws)),                      "Ключевых слов загружено"),
        ("force_available",   "1" if FORCE_AVAILABLE else "0",    "Принудительная доступность"),
        ("built_utc",         now_utc(),                          "Время сборки (UTC)"),
        ("built_Asia/Almaty", now_almaty(),                       "Время сборки (Алматы)"),
    ]
    meta_comment = render_feed_meta(feed)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    xml = build_yml(parsed, meta_comment)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml)

    log(f"Wrote: {OUT_FILE} | offers={offers_written} | encoding={OUTPUT_ENCODING}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        err(str(e), 2)
