# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint -> YML (KZT) без локального docs/nvprint_source.xml.

Правила:
- id = vendorCode = "NP" + <очищенный артикул>
- Порядок тегов в <offer>:
  <vendorCode>, <name>, <price>, <picture>, <vendor>, <currencyId>, <available>, <description>
- <available>true</available> всем, валюта одна: <currencyId>KZT</currencyId>
- FEED_META: как в feed.txt, «Ближайшее время сборки (Алматы)» = ближайшее 1/10/20 в 04:00 Asia/Almaty.
"""

from __future__ import annotations
import os, re, io, html, math, sys, time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import xml.etree.ElementTree as ET

try:
    import requests
except Exception:
    requests = None

# ---------------- КОНСТАНТЫ / ОКРУЖЕНИЕ ----------------
SUPPLIER_URL      = os.getenv("NVPRINT_XML_URL", "https://api.nvprint.ru/api/hs/getprice/398/881105302369/none/?format=xml&getallinfo=true")
OUT_FILE          = os.getenv("OUT_FILE", "docs/nvprint.yml")
OUTPUT_ENCODING   = os.getenv("OUTPUT_ENCODING", "windows-1251")
HTTP_TIMEOUT      = float(os.getenv("HTTP_TIMEOUT", "45"))
RETRIES           = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF_S   = float(os.getenv("RETRY_BACKOFF_S", "2"))

NV_LOGIN          = (os.getenv("NVPRINT_LOGIN") or os.getenv("NVPRINT_XML_USER") or "").strip()
NV_PASSWORD       = (os.getenv("NVPRINT_PASSWORD") or os.getenv("NVPRINT_XML_PASS") or "").strip()

KEYWORDS_FILE     = (os.getenv("NVPRINT_KEYWORDS_FILE") or "docs/nvprint_keywords.txt").strip()

# ---------------- УТИЛИТЫ ----------------
def yml_escape(s: str) -> str:
    return html.escape((s or "").strip())

def strip_ns(tag: str) -> str:
    if not tag: return tag
    if tag.startswith("{"):
        i = tag.rfind("}")
        if i != -1: return tag[i+1:]
    return tag

def parse_number(txt: Optional[str]) -> Optional[float]:
    if not txt: return None
    t = txt.strip().replace("\u00A0", "").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m: return None
    try: return float(Decimal(m.group(0)))
    except (InvalidOperation, ValueError): return None

def first_child_text(item: ET.Element, tag_names: List[str]) -> Optional[str]:
    names = {t.lower() for t in tag_names}
    for ch in item:
        if strip_ns(ch.tag).lower() in names:
            val = (ch.text or "").strip()
            if val: return val
    return None

def find_descendant(item: ET.Element, tag_names: List[str]) -> Optional[ET.Element]:
    names = {t.lower() for t in tag_names}
    for node in item.iter():
        if strip_ns(node.tag).lower() in names:
            return node
    return None

def find_descendant_text(item: ET.Element, tag_names: List[str]) -> Optional[str]:
    node = find_descendant(item, tag_names)
    if node is not None:
        txt = (node.text or "").strip()
        if txt: return txt
    return None

def read_source_bytes() -> bytes:
    if not SUPPLIER_URL: raise RuntimeError("SUPPLIER_URL пуст")
    if requests is None: raise RuntimeError("requests недоступен")
    auth = (NV_LOGIN, NV_PASSWORD) if (NV_LOGIN or NV_PASSWORD) else None
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(SUPPLIER_URL, timeout=HTTP_TIMEOUT, auth=auth)
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized: проверь NVPRINT_LOGIN/NVPRINT_PASSWORD в secrets")
            r.raise_for_status()
            b = r.content
            if not b: raise RuntimeError("Источник вернул пустой ответ")
            return b
        except Exception as e:
            last_err = e
            if attempt >= RETRIES or ("401" in str(e)): break
            time.sleep(RETRY_BACKOFF_S * attempt)
    raise RuntimeError(str(last_err) if last_err else "Не удалось скачать источник")

# ---------------- KEYWORDS ----------------
def read_text_with_encodings(path: str, encodings: List[str]) -> Optional[str]:
    if not os.path.isfile(path): return None
    for enc in encodings:
        try:
            with io.open(path, "r", encoding=enc) as f: return f.read()
        except Exception: continue
    try:
        with io.open(path, "rb") as f: raw = f.read()
        return raw.decode("latin-1", errors="ignore")
    except Exception: return None

def load_keywords(path: str) -> List[str]:
    txt = read_text_with_encodings(path, ["utf-8-sig","utf-8","utf-16","cp1251","koi8-r","iso-8859-5","cp866"])
    if not txt: return []
    kws: List[str] = []
    for line in txt.splitlines():
        ln = line.strip()
        if not ln or ln.startswith("#") or ln.startswith(";"): continue
        ln = re.sub(r"\s+", " ", ln).strip().lower()
        if ln: kws.append(ln)
    seen=set(); out=[]
    for k in kws:
        if k not in seen:
            seen.add(k); out.append(k)
    return out

def norm_for_match(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def name_starts_with_keywords(name_short: str, keywords: List[str]) -> bool:
    if not keywords: return True
    base = norm_for_match(name_short)
    for kw in keywords:
        if base.startswith(kw): return True
    return False

# ---------------- ЦЕНЫ ПО ДОГОВОРАМ ----------------
def _norm_contract(s: str) -> str:
    if not s: return ""
    tr = str.maketrans({
        "А":"A","В":"B","Е":"E","К":"K","М":"M","Н":"H","О":"O","Р":"P","С":"C","Т":"T","Х":"X","У":"Y",
        "а":"A","в":"B","е":"E","к":"K","м":"M","н":"H","о":"O","р":"P","с":"C","т":"T","х":"X","у":"Y",
        "Ё":"E","ё":"e",
    })
    u = s.translate(tr).upper()
    u = re.sub(r"[\s\-\_]+", "", u)
    return u  # TA000079, TA000079МСК

def _extract_price_from_contracts(item: ET.Element) -> Optional[float]:
    price_kz = price_msk = None
    for node in item.iter():
        if strip_ns(node.tag).lower() != "договор": continue
        num = (node.attrib.get("НомерДоговора") or node.attrib.get("Номердоговора") or "").strip()
        num_n = _norm_contract(num)
        if "000079" not in num_n: continue
        price_el = find_descendant(node, ["Цена","price","amount","value"])
        val = parse_number(price_el.text if price_el is not None else None)
        if val is None or val <= 0: continue
        if "MSK" in num_n or "МСК" in num_n: price_msk = val
        else: price_kz = val
    return price_kz if price_kz else price_msk

# ---------------- ПРАВИЛА ЦЕНООБРАЗОВАНИЯ ----------------
from typing import Tuple
PriceRule = Tuple[int,int,float,int]
PRICING_RULES: List[PriceRule] = [
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

def _round_up_tail_900(n: int) -> int:
    thousands = (n + 999) // 1000
    return thousands * 1000 - 100

def compute_price_from_supplier(base_price: Optional[int]) -> int:
    if base_price is None or base_price < 100: return 100
    for lo,hi,pct,add in PRICING_RULES:
        if lo<=base_price<=hi:
            raw = base_price * (1 + pct/100.0) + add
            return _round_up_tail_900(int(math.ceil(raw)))
    raw = base_price * (1 + PRICING_RULES[-1][2]/100.0) + PRICING_RULES[-1][3]
    return _round_up_tail_900(int(math.ceil(raw)))

# ---------------- СБОР ОПИСАНИЯ ----------------
def clean_article(raw: str) -> str:
    s = (raw or "").strip()
    s = re.sub(r"^\s*NV[\-\_\s]+", "", s, flags=re.IGNORECASE)
    s = s.replace(" ", "")
    return s

def make_ids_from_article(article: str) -> Tuple[str,str]:
    ac = clean_article(article)
    pref = "NP" + ac
    return pref, pref

def collect_printers(item: ET.Element) -> List[str]:
    printers=[]
    node = find_descendant(item, ["Принтеры"])
    if node is not None:
        for ch in node.iter():
            if strip_ns(ch.tag).lower()=="принтер":
                t=(ch.text or "").strip()
                if t: printers.append(t)
    seen=set(); out=[]
    for p in printers:
        if p not in seen:
            seen.add(p); out.append(p)
    return out

def build_description(item: ET.Element) -> str:
    parts=[]
    nom_full = find_descendant_text(item, ["Номенклатура"]) or ""
    nom_full = re.sub(r"\s+"," ",nom_full).strip()
    if nom_full: parts.append(nom_full)
    specs=[]
    resurs = find_descendant_text(item, ["Ресурс"])
    if resurs and resurs.strip() and resurs.strip()!="0": specs.append(f"- Ресурс: {resurs.strip()}")
    tip    = find_descendant_text(item, ["ТипПечати"])
    if tip: specs.append(f"- Тип печати: {tip.strip()}")
    color  = find_descendant_text(item, ["ЦветПечати"])
    if color: specs.append(f"- Цвет печати: {color.strip()}")
    compat = find_descendant_text(item, ["СовместимостьСМоделями"])
    if compat: specs.append(f"- Совместимость с моделями: {re.sub(r'\\s+',' ',compat).strip()}")
    weight = find_descendant_text(item, ["Вес"])
    if weight: specs.append(f"- Вес: {weight.strip()}")
    prn_list = collect_printers(item)
    if prn_list: specs.append(f"- Принтеры: {', '.join(prn_list)}")
    if specs:
        parts.append("Технические характеристики:")
        parts.extend(specs)
    return "\n".join(parts).strip()

# ---------------- ПАРСИНГ ТОВАРА ----------------
def parse_item(elem: ET.Element) -> Optional[Dict[str,Any]]:
    article = first_child_text(elem, ["Артикул","articul","sku","article","PartNumber"])
    if not article: return None
    name_short = find_descendant_text(elem, ["НоменклатураКратко"])
    if not name_short: return None
    name_short = re.sub(r"\s+"," ",name_short).strip()
    base = _extract_price_from_contracts(elem)
    base_int = 100 if (base is None or base <= 0) else int(math.ceil(base))
    final_price = compute_price_from_supplier(base_int)
    vendor = first_child_text(elem, ["Бренд","Производитель","Вендор","Brand","Vendor"]) or ""
    picture = (first_child_text(elem, ["СсылкаНаКартинку","Картинка","Изображение","Фото","Picture","Image","ФотоURL","PictureURL"]) or "").strip()
    description = build_description(elem)
    oid, vcode = make_ids_from_article(article)
    return {
        "id": oid, "vendorCode": vcode, "title": name_short, "price": final_price,
        "vendor": vendor, "picture": picture, "description": description,
    }

def guess_item_nodes(root: ET.Element) -> List[ET.Element]:
    items=[]; seen=set()
    for node in root.iter():
        if not find_descendant(node, ["Артикул","articul","sku","article","PartNumber"]): continue
        if not find_descendant(node, ["НоменклатураКратко"]): continue
        key=id(node)
        if key in seen: continue
        seen.add(key); items.append(node)
    return items

# ---------------- FEED_META (как в файле) ----------------
def _almaty_now() -> datetime:
    # Достаточно фиксированного смещения +5 часов (Asia/Almaty)
    return datetime.utcnow() + timedelta(hours=5)

def _next_build_1_10_20_at_04() -> datetime:
    now = _almaty_now()
    targets = [1,10,20]
    # кандидаты в этом месяце
    cands = [now.replace(day=d, hour=4, minute=0, second=0, microsecond=0) for d in targets if d <= 28]
    # корректировка для 30/31 дней — 1-е след. месяца покроем отдельно
    future = [t for t in cands if t > now]
    if future:
        return min(future)
    # иначе — 1-е число следующего месяца, 04:00
    if now.month == 12:
        return now.replace(year=now.year+1, month=1, day=1, hour=4, minute=0, second=0, microsecond=0)
    else:
        # безопасно: шаг к 1-му числу следующего месяца
        first_next = (now.replace(day=1, hour=4, minute=0, second=0, microsecond=0)
                      + timedelta(days=32))
        first_next = first_next.replace(day=1)
        return first_next

def _fmt_alm(dt: datetime) -> str:
    return dt.strftime("%d:%m:%Y - %H:%M:%S")

def render_feed_meta_comment(pairs: Dict[str, str]) -> str:
    now_alm  = _almaty_now()
    next_alm = _next_build_1_10_20_at_04()
    rows = [
        ("Поставщик", pairs.get("supplier","")),
        ("URL поставщика", pairs.get("source","")),
        ("Время сборки (Алматы)", _fmt_alm(now_alm)),
        ("Ближайшее время сборки (Алматы)", _fmt_alm(next_alm)),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total","0"))),
        ("Сколько товаров у поставщика после фильтра", str(pairs.get("offers_written","0"))),
        ("Сколько товаров есть в наличии (true)", str(pairs.get("available_true","0"))),
        ("Сколько товаров нет в наличии (false)", str(pairs.get("available_false","0"))),
    ]
    key_w = max(len(k) for k,_ in rows)
    lines = ["<!--FEED_META"]
    for i,(k,v) in enumerate(rows):
        end = " -->" if i == len(rows)-1 else ""
        lines.append(f"{k.ljust(key_w)} | {v}{end}")
    return "\n".join(lines)

# ---------------- СЕРИАЛИЗАЦИЯ ----------------
def build_yml(offers: List[Dict[str,Any]], source: str, offers_total: int) -> str:
    date_attr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    offers_written = len(offers)
    meta_pairs = {
        "supplier": "nvprint",
        "source": source,
        "offers_total": str(offers_total),
        "offers_written": str(offers_written),
        "available_true": str(offers_written),
        "available_false": "0",
    }
    out=[]
    out.append(f"<?xml version='1.0' encoding='{OUTPUT_ENCODING}'?>")
    out.append(f"<yml_catalog date=\"{date_attr}\">")
    out.append(render_feed_meta_comment(meta_pairs))
    out.append("<shop>")
    out.append("  <offers>")
    for it in offers:
        offer_id = it.get("vendorCode") or it.get("id")
        out.append(f"    <offer id=\"{yml_escape(offer_id)}\">")
        out.append(f"      <vendorCode>{yml_escape(offer_id)}</vendorCode>")
        out.append(f"      <name>{yml_escape(it['title'])}</name>")
        out.append(f"      <price>{int(it['price'])}</price>")
        if it.get("picture"): out.append(f"      <picture>{yml_escape(it['picture'])}</picture>")
        if it.get("vendor"):  out.append(f"      <vendor>{yml_escape(it['vendor'])}</vendor>")
        out.append("      <currencyId>KZT</currencyId>")
        out.append("      <available>true</available>")
        if it.get("description"):
            desc_clean = re.sub(r"\s+"," ", it["description"]).strip()
            out.append(f"      <description>{yml_escape(desc_clean)}</description>")
        out.append("    </offer>\n")
    out.append("  </offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------------- MAIN ----------------
def parse_xml_to_yml(xml_bytes: bytes, source_label: str) -> str:
    root = ET.fromstring(xml_bytes)
    keywords = load_keywords(KEYWORDS_FILE)
    nodes = guess_item_nodes(root)
    offers_total = len(nodes)
    offers=[]
    for node in nodes:
        name_short = find_descendant_text(node, ["НоменклатураКратко"]) or ""
        if not name_starts_with_keywords(name_short, keywords): continue
        it = parse_item(node)
        if it: offers.append(it)
    return build_yml(offers, source_label, offers_total)

def main() -> int:
    try:
        data = read_source_bytes()
        yml = parse_xml_to_yml(data, SUPPLIER_URL)
    except Exception as e:
        sys.stderr.write(f"ERROR: {e}\n")
        yml = build_yml([], SUPPLIER_URL, 0)
    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
        f.write(yml)
    print(f"Wrote: {OUT_FILE} | encoding={OUTPUT_ENCODING}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
