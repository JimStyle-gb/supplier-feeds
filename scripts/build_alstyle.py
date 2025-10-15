# scripts/build_alstyle_min.py
# -*- coding: utf-8 -*-
"""
AlStyle → YML for Satu (минимальная версия)
Цели:
- Простая поддержка и читаемость.
- Только обязательные преобразования под Satu.
- Без «магии», ничего лишнего.

Что делает:
• availability: переводит в атрибут offer[@available], тег <available> убирает; чистит qty/stock-теги.
• categoryId: ставит <categoryId>0</categoryId> самым первым внутри <offer> (потом подменишь 0 на id).
• price: считает розничную из «дилерской» по простым правилам, округляя на ...900. Внутренние прайс-теги удаляет.
• currencyId: принудительно KZT внутри каждого оффера.
• description: лёгкая чистка (символы/опечатки/пробелы), удаление только служебных строк («Артикул/Штрихкод/Новинка/Снижена цена/Благотворительность/Оригинальный код»).
• param: удаляет служебные, пустые/заглушки и дубликаты по имени. Осмысленные «Есть/Нет» оставляет.
• порядок детей оффера: categoryId → vendorCode → name → price → picture* → vendor → currencyId → description → (остальное как есть)
• FEED_META: краткая шапка-комментарий.

Переменные окружения:
SUPPLIER_URL         — источник XML (URL или путь к файлу)
OUT_FILE             — путь для вывода (по умолчанию docs/alstyle.yml)
OUTPUT_ENCODING      — кодировка вывода (windows-1251)
VENDORCODE_PREFIX    — префикс для vendorCode (по умолчанию AS). Не меняем, если уже задано.
CATEGORY_ID_DEFAULT  — значение для <categoryId> (по умолчанию 0)
"""

from __future__ import annotations
import os, re, time, html, urllib.parse
from typing import Optional, Tuple, List
from xml.etree import ElementTree as ET
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests

# ---------- настройки ----------
SUPPLIER_URL        = os.getenv("SUPPLIER_URL", "").strip()
OUT_FILE            = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC                 = os.getenv("OUTPUT_ENCODING", "windows-1251")
VENDORCODE_PREFIX   = os.getenv("VENDORCODE_PREFIX", "AS")
CATEGORY_ID_DEFAULT = os.getenv("CATEGORY_ID_DEFAULT", "0")
TIMEOUT_S = int(os.getenv("TIMEOUT_S", "30"))

# правила ценообразования (как обсуждали)
PRICING_RULES: List[Tuple[int,int,float,int]] = [
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
INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice",
)

# служебные названия параметров (вырезаем в <param> и из описаний)
UNWANTED_NAMES_RE = re.compile(
    r"^\s*(?:артикул(?:\s*/\s*штрихкод)?|оригинальн\w*\s*код|штрихкод|благотворительн\w*|новинк\w*|снижена\s*цена)\b",
    re.I
)

PLACEHOLDER_VALUES = {"", "-", "—", "–", ".", "..", "...", "n/a", "na", "none", "null", "нет данных", "не указано", "неизвестно"}

# --- утилиты ---
def _now_almaty_str():
    try:   return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    except: return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _fetch_bytes(url: str) -> bytes:
    if not url:
        raise RuntimeError("SUPPLIER_URL не задан")
    if "://" not in url:
        with open(url, "rb") as f: return f.read()
    r = requests.get(url, timeout=TIMEOUT_S, headers={"User-Agent":"alstyle-min/1.0"})
    r.raise_for_status()
    return r.content

def _text(el: ET.Element, tag: str) -> str:
    n = el.find(tag)
    return (n.text or "").strip() if (n is not None and n.text) else ""

def _remove_all(el: ET.Element, *tags: str) -> int:
    n=0
    for t in tags:
        for x in list(el.findall(t)):
            el.remove(x); n+=1
    return n

# --- цены ---
def _parse_price(raw: str) -> Optional[float]:
    if raw is None: return None
    s = raw.strip().replace("\xa0"," ").replace(" ","").replace("KZT","").replace("₸","").replace(",",".")
    if not s: return None
    try:
        v = float(s)
        return v if v > 0 else None
    except:
        return None

def _pick_dealer_price(offer: ET.Element) -> Optional[float]:
    # 1) из блоков <prices><price type="dealer|опт|b2b|...">
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            t = (p.attrib.get("type") or "").lower()
            if any(k in t for k in ("dealer","опт","wholesale","b2b","закуп","purchase")):
                v = _parse_price(p.text or "")
                if v: return v
    # 2) прямые поля
    for tag in INTERNAL_PRICE_TAGS:
        el = offer.find(tag)
        if el is not None and el.text:
            v = _parse_price(el.text)
            if v: return v
    # 3) fallback: rrp/retail не трогаем (не понижаем)
    return None

def _force_tail_900(n: float) -> int:
    i = int(n)
    k = max(i // 1000, 0)
    out = k * 1000 + 900
    return out if out >= 900 else 900

def _compute_retail(dealer: float) -> Optional[int]:
    for lo,hi,pct,add in PRICING_RULES:
        if lo <= dealer <= hi:
            val = dealer * (1.0 + pct/100.0) + add
            return _force_tail_900(val)
    return None

def apply_pricing(offer: ET.Element) -> None:
    dealer = _pick_dealer_price(offer)
    if dealer and dealer > 100:
        retail = _compute_retail(dealer)
        if retail:
            p = offer.find("price") or ET.SubElement(offer, "price")
            p.text = str(int(retail))
    # Чистим внутренние цены
    _remove_all(offer, "prices", "Prices")
    for tag in INTERNAL_PRICE_TAGS:
        _remove_all(offer, tag)

# --- доступность ---
TRUE_WORDS  = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS = {"false","0","no","n","нет","out of stock","unavailable","под заказ","ожидается","на заказ","нет в наличии"}

def _parse_bool(s: str) -> Optional[bool]:
    v = (s or "").strip().lower()
    if v in TRUE_WORDS: return True
    if v in FALSE_WORDS: return False
    return None

def normalize_available(offer: ET.Element) -> None:
    # приоритет: тег <available>…</available>, затем quantity/stock
    b = None
    av = offer.find("available")
    if av is not None and av.text:
        b = _parse_bool(av.text)
    if b is None:
        for tag in ("quantity_in_stock","quantity","stock","Stock"):
            node = offer.find(tag)
            if node is not None and node.text:
                try:
                    qty = int(re.sub(r"[^\d\-]","", node.text))
                    b = (qty > 0)
                    break
                except:
                    pass
    offer.attrib["available"] = "true" if b else "false"
    _remove_all(offer, "available", "quantity_in_stock","quantity","stock","Stock")

# --- categoryId первым ---
def ensure_category_first(offer: ET.Element) -> None:
    # удаляем все старые categoryId и вставляем наш в начало
    for node in list(offer.findall("categoryId")) + list(offer.findall("CategoryId")):
        offer.remove(node)
    cid = ET.Element("categoryId"); cid.text = CATEGORY_ID_DEFAULT
    offer.insert(0, cid)

# --- валюта ---
def ensure_currency_kzt(offer: ET.Element) -> None:
    _remove_all(offer, "currencyId")
    cur = ET.SubElement(offer, "currencyId"); cur.text = "KZT"

# --- vendorCode/id (префикс не навязываем, если уже есть нормальный код) ---
ART_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)

def _article_from_name(name: str) -> str:
    m = ART_RE.search(name or "")
    return (m.group(1) if m else "").upper()

def _article_from_url(url: str) -> str:
    try:
        path = urllib.parse.urlparse(url or "").path.rstrip("/")
        last = re.sub(r"\.(html?|php|aspx?)$","", path.split("/")[-1], flags=re.I)
        m = ART_RE.search(last)
        return (m.group(1) if m else last).upper()
    except:
        return ""

def ensure_vendorcode_and_id(offer: ET.Element) -> None:
    vc = offer.find("vendorCode")
    if vc is None:
        vc = ET.SubElement(offer, "vendorCode"); vc.text = ""
    if not (vc.text or "").strip():
        art = _article_from_name(_text(offer,"name")) or _article_from_url(_text(offer,"url")) or (offer.attrib.get("id") or "")
        vc.text = (art or "").upper()
    # префикс добавляем, если его ещё нет
    if VENDORCODE_PREFIX and not (vc.text or "").startswith(VENDORCODE_PREFIX):
        vc.text = f"{VENDORCODE_PREFIX}{vc.text or ''}"
    # id = vendorCode
    offer.attrib["id"] = vc.text or offer.attrib.get("id","")

# --- чистка param ---
def prune_params(offer: ET.Element) -> None:
    seen=set()
    for tag in ("param","Param"):
        for p in list(offer.findall(tag)):
            nm = (p.attrib.get("name") or "").strip()
            val = (p.text or "").strip()
            if UNWANTED_NAMES_RE.search(nm):
                offer.remove(p); continue
            if (val.strip().lower() in PLACEHOLDER_VALUES) or re.search(r"https?://|www\.", val, re.I):
                offer.remove(p); continue
            key = re.sub(r"\s+"," ", nm.lower())
            if key in seen:
                offer.remove(p); continue
            seen.add(key)

# --- чистка description ---
RE_HTML_TAG     = re.compile(r"<[^>]+>")
RE_SCHUKO       = re.compile(r"\bshuko\b", re.I)
RE_LATIN_WATT   = re.compile(r"\bBт\b|\bBТ\b")
RE_BAD_CASE     = re.compile(r"\bЛинейно-Интерактивный\b")
RE_X_DIM        = re.compile(r"(?<=\d)\s*[xX]\s*(?=\d)")
RE_UNIT_SPACE   = re.compile(r"(?<!\d)(\d+)(?=(В|Вт|Ач|А|Гц|мм|см|кг)\b)")
RE_HZ_SPACE     = re.compile(r"(\d(?:[.,]\d+)?)\s*(Гц)\b")
RE_PM_SPACE     = re.compile(r"±\s*(\d)")
RE_DBL_SPACES   = re.compile(r"[ \t]{2,}")

def clean_description_text(txt: str) -> str:
    if not (txt or "").strip(): return txt
    # html entities/невидимые
    txt = html.unescape(txt)
    txt = txt.replace("\uFEFF","").replace("\u200B","").replace("\u200C","").replace("\u200D","")
    txt = txt.replace("\u00A0"," ")
    # служебные строки удаляем
    lines = []
    for ln in (txt or "").splitlines():
        if UNWANTED_NAMES_RE.search(ln):  # только они
            continue
        lines.append(ln)
    txt = "\n".join(lines)
    # сырые html-теги, знаки
    txt = RE_HTML_TAG.sub("", txt)
    txt = re.sub(r"[®™©]", "", txt)
    # терминология/опечатки/формат
    txt = RE_SCHUKO.sub("Schuko", txt)
    txt = RE_LATIN_WATT.sub("Вт", txt)
    txt = RE_BAD_CASE.sub("Линейно-интерактивный", txt)
    txt = RE_X_DIM.sub(" × ", txt)
    txt = RE_UNIT_SPACE.sub(r"\1 ", txt)
    txt = RE_HZ_SPACE.sub(r"\1 \2", txt)
    txt = RE_PM_SPACE.sub(r"± \1", txt)
    txt = RE_DBL_SPACES.sub(" ", txt)
    # не удаляем строки «...: Есть/Нет/Да»
    return txt.strip()

def clean_description(offer: ET.Element) -> None:
    d = offer.find("description")
    if d is None: return
    raw = (d.text or "").strip()
    if not raw: return
    d.text = clean_description_text(raw)

# --- порядок детей ---
DESIRED_ORDER = ["categoryId","vendorCode","name","price","picture","vendor","currencyId","description"]

def reorder_children(offer: ET.Element) -> None:
    children = list(offer)
    if not children: return
    buckets = {k:[] for k in DESIRED_ORDER}; others=[]
    for node in children:
        if node.tag in buckets: buckets[node.tag].append(node)
        else: others.append(node)
    new_children=[]
    for k in DESIRED_ORDER: new_children.extend(buckets[k])
    new_children.extend(others)
    if new_children != children:
        for n in children: offer.remove(n)
        for n in new_children: offer.append(n)

# --- FEED_META ---
def _feed_meta_comment(total:int, written:int)->str:
    try:   now = datetime.now(ZoneInfo("Asia/Almaty"))
    except: now = datetime.now(timezone.utc)
    def fmt(dt): return dt.strftime("%d:%m:%Y - %H:%M:%S")
    rows = [
        "FEED_META",
        f"Источник                 | {SUPPLIER_URL or '(file)'}",
        f"Время сборки (Алматы)    | {fmt(now)}",
        f"Товаров всего             | {total}",
        f"Товаров записано          | {written}",
    ]
    return "\n".join(rows)

# --- main ---
def main():
    data = _fetch_bytes(SUPPLIER_URL)
    src_root = ET.fromstring(data)

    shop_in = src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: raise RuntimeError("<shop> not found")
    offers_in = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in is None: raise RuntimeError("<offers> not found")
    offers = list(offers_in.findall("offer"))

    # строим новый документ: просто копируем офферы и приводим по правилам
    out_root = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")
    out_offers = ET.SubElement(out_shop, "offers")

    for src in offers:
        off = ET.fromstring(ET.tostring(src, encoding="utf-8"))
        # обязательные шаги
        normalize_available(off)
        ensure_category_first(off)
        ensure_currency_kzt(off)
        apply_pricing(off)
        prune_params(off)
        clean_description(off)
        ensure_vendorcode_and_id(off)
        reorder_children(off)
        out_offers.append(off)

    # FEED_META комментарий
    meta = ET.Comment(_feed_meta_comment(len(offers), len(list(out_offers.findall("offer")))))
    out_root.insert(0, meta)

    # чуть отформатируем
    try: ET.indent(out_root, space="  ")
    except Exception: pass

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=ENC, newline="\n") as f:
        f.write(ET.tostring(out_root, encoding=ENC, xml_declaration=True).decode(ENC, errors="replace"))

    print(f"OK → {OUT_FILE} | offers={len(list(out_offers.findall('offer')))} | encoding={ENC}")

if __name__ == "__main__":
    main()
