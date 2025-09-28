# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint -> YML (KZT)

Цена:
  1) <Договор НомерДоговора="ТА-000079"> -> <Цена> (Казахстан)
  2) если нет, <Договор НомерДоговора="TA-000079Мск"> -> <Цена> (Москва)
  3) если нет нигде -> 100
  4) после этого применяем PRICING_RULES (наценка) и округляем вверх до ...900

Вывод (минимальный шаблон под Satu):
  - Без <categories>, <categoryId>, <url>, любых quantity-тегов
  - <available>true</available> всем
  - id = <Артикул> без префикса "NV-", vendorCode = "NP" + id
  - Кодировка файла: windows-1251
"""

from __future__ import annotations
import os, re, io, html, math, sys
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import xml.etree.ElementTree as ET

try:
    import requests
except Exception:
    requests = None

# ---------------- ENV ----------------
SUPPLIER_URL     = os.getenv("SUPPLIER_URL", "").strip()
OUT_FILE         = os.getenv("OUT_FILE", "docs/nvprint.yml")
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251")
HTTP_TIMEOUT     = float(os.getenv("HTTP_TIMEOUT", "45"))

# ---------------- UTILS ----------------
def yml_escape(s: str) -> str:
    return html.escape((s or "").strip())

def strip_ns(tag: str) -> str:
    if not tag:
        return tag
    if tag[0] == "{":
        i = tag.rfind("}")
        if i != -1:
            return tag[i+1:]
    return tag

def parse_number(txt: Optional[str]) -> Optional[float]:
    if not txt:
        return None
    t = txt.strip().replace("\u00A0", "").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        return float(Decimal(m.group(0)))
    except (InvalidOperation, ValueError):
        return None

def first_child_text(item: ET.Element, tag_names: List[str]) -> Optional[str]:
    low = [t.lower() for t in tag_names]
    for ch in item:
        if strip_ns(ch.tag).lower() in low:
            val = (ch.text or "").strip()
            if val:
                return val
    return None

def find_descendant(item: ET.Element, tag_names: List[str]) -> Optional[ET.Element]:
    low = [t.lower() for t in tag_names]
    for node in item.iter():
        if strip_ns(node.tag).lower() in low:
            return node
    return None

def read_source_bytes(src: str) -> bytes:
    if not src:
        raise RuntimeError("SUPPLIER_URL не задан")
    if os.path.isfile(src):
        with io.open(src, "rb") as f:
            b = f.read()
        if not b:
            raise RuntimeError("Пустой локальный файл источника")
        return b
    if requests is None:
        raise RuntimeError("requests недоступен для скачивания URL")
    r = requests.get(src, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    b = r.content
    if not b:
        raise RuntimeError("Источник вернул пустой ответ")
    return b

# ---------------- ЦЕНА ИЗ ДОГОВОРОВ ----------------
def _norm_contract(s: str) -> str:
    """Латинизируем похожие кириллические буквы, убираем пробелы/дефисы/подчёркивания, upper."""
    if not s:
        return ""
    tr = str.maketrans({
        "А":"A","В":"B","Е":"E","К":"K","М":"M","Н":"H","О":"O","Р":"P","С":"C","Т":"T","Х":"X","У":"Y",
        "а":"A","в":"B","е":"E","к":"K","м":"M","н":"H","о":"O","р":"P","с":"C","т":"T","х":"X","у":"Y",
        "Ё":"E","ё":"e",
    })
    u = s.translate(tr).upper()
    u = re.sub(r"[\s\-\_]+", "", u)
    return u  # примеры: TA000079, TA000079МСК

def _extract_price_from_contracts(item: ET.Element) -> Optional[float]:
    """
    Ищем <Договор><Цена> по приоритету:
      1) НомерДоговора содержит 000079 и НЕ содержит MSK/МСК (Казахстан)
      2) НомерДоговора содержит 000079 и содержит MSK/МСК (Москва)
    """
    price_kz: Optional[float] = None
    price_msk: Optional[float] = None

    for node in item.iter():
        if strip_ns(node.tag).lower() != "договор":
            continue
        num = (node.attrib.get("НомерДоговора") or node.attrib.get("Номердоговора") or "").strip()
        num_n = _norm_contract(num)
        if "000079" not in num_n:
            continue
        price_el = find_descendant(node, ["Цена", "price", "amount", "value"])
        val = parse_number(price_el.text if price_el is not None else None)
        if val is None or val <= 0:
            continue
        if "MSK" in num_n or "МСК" in num_n:
            price_msk = val
        else:
            price_kz = val

    if price_kz is not None and price_kz > 0:
        return price_kz
    if price_msk is not None and price_msk > 0:
        return price_msk
    return None

# ---------------- ЦЕНООБРАЗОВАНИЕ (как у других поставщиков) ----------------
from typing import Tuple
PriceRule = Tuple[int, int, float, int]
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
    """Округление вверх до ближайшего значения с окончанием ...900."""
    thousands = (n + 999) // 1000
    return thousands * 1000 - 100

def compute_price_from_supplier(base_price: Optional[int]) -> int:
    """Если base_price отсутствует или <100 -> 100. Иначе применяем правило и округляем вверх до ...900."""
    if base_price is None or base_price < 100:
        return 100
    for lo, hi, pct, add in PRICING_RULES:
        if lo <= base_price <= hi:
            raw = base_price * (1.0 + pct/100.0) + add
            return _round_up_tail_900(int(math.ceil(raw)))
    raw = base_price * (1.0 + PRICING_RULES[-1][2]/100.0) + PRICING_RULES[-1][3]
    return _round_up_tail_900(int(math.ceil(raw)))

# ---------------- ПАРСИНГ ТОВАРА ----------------
def clean_article(raw: str) -> str:
    """Удаляем ведущий NV-/NV_/NV и пробелы."""
    s = (raw or "").strip()
    s = re.sub(r"^\s*NV[\-\_\s]+", "", s, flags=re.IGNORECASE)
    s = s.replace(" ", "")
    return s

def make_ids_from_article(article: str) -> Tuple[str, str]:
    ac = clean_article(article)
    return ac, "NP" + ac

def parse_item(elem: ET.Element) -> Optional[Dict[str, Any]]:
    article = first_child_text(elem, ["Артикул","articul","sku","article","PartNumber"])
    if not article:
        return None
    name = first_child_text(elem, ["Наименование","Название","Name","Товар","Модель","full_name","title"])
    if not name:
        return None

    # 1) Цена с приоритетом КЗ -> МСК -> 100
    base = _extract_price_from_contracts(elem)
    if base is None or base <= 0:
        base_int = 100
    else:
        base_int = int(math.ceil(base))

    # 2) Применяем правила наценки и округления до ...900
    final_price = compute_price_from_supplier(base_int)

    vendor = first_child_text(elem, ["Бренд","Производитель","Вендор","Brand","Vendor"]) or ""
    picture = first_child_text(elem, ["Картинка","Изображение","Фото","Picture","Image","ФотоURL","PictureURL"]) or ""
    description = first_child_text(elem, ["Описание","Description","Текст","About"]) or ""

    oid, vcode = make_ids_from_article(article)
    return {
        "id": oid,
        "title": name,
        "vendor": vendor,
        "vendorCode": vcode,
        "price": final_price,
        "picture": picture,
        "description": description,
    }

def guess_item_nodes(root: ET.Element) -> List[ET.Element]:
    items: List[ET.Element] = []
    seen: set = set()
    for node in root.iter():
        art = find_descendant(node, ["Артикул","articul","sku","article","PartNumber"])
        if art is None:
            continue
        name = find_descendant(node, ["Наименование","Название","Name","Товар","Модель","full_name","title"])
        if name is None:
            continue
        key = id(node)
        if key in seen:
            continue
        seen.add(key)
        items.append(node)
    return items

# ---------------- FEED_META + YML ----------------
def almaty_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5)

def almaty_now_str() -> str:
    return almaty_now().strftime("%Y-%m-%d %H:%M:%S +05")

def build_feed_meta(source: str, offers_total: int, offers_written: int, prices_picked: int) -> str:
    pad = 28
    rows: List[str] = []
    def kv(k, v, cmt=""):
        if cmt:
            rows.append(f"{k.ljust(pad)} = {str(v):<60} | {cmt}")
        else:
            rows.append(f"{k.ljust(pad)} = {str(v)}")
    kv("supplier",         "nvprint",                         "Метка поставщика")
    kv("source",           source,                            "URL/файл источника")
    kv("offers_total",     offers_total,                      "Офферов в источнике (оценочно)")
    kv("offers_written",   offers_written,                    "Офферов записано")
    kv("prices_updated",   prices_picked,                     "Цены взяты из договоров (+ наценка)")
    kv("available_forced", offers_written,                    "Сколько офферов получили available=true")
    rows.append(f"{'built_Asia/Almaty'.ljust(pad)} = {almaty_now_str():<60} | Время сборки (Алматы)-->")
    return "<!--FEED_META\n" + "\n".join(rows) + "\n"

def build_yml(offers: List[Dict[str, Any]], source: str, offers_total: int, prices_picked: int) -> str:
    date_attr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append(f"<yml_catalog date=\"{date_attr}\">")
    out.append(build_feed_meta(source, offers_total, len(offers), prices_picked))
    out.append("<shop>")
    out.append("  <offers>")
    for it in offers:
        out.append(f"    <offer id=\"{yml_escape(it['id'])}\">")
        out.append(f"      <name>{yml_escape(it['title'])}</name>")
        if it.get("vendor"):
            out.append(f"      <vendor>{yml_escape(it['vendor'])}</vendor>")
        out.append(f"      <vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>")
        out.append(f"      <price>{int(it['price'])}</price>")
        out.append("      <currencyId>KZT</currencyId>")
        if it.get("picture"):
            out.append(f"      <picture>{yml_escape(it['picture'])}</picture>")
        if it.get("description"):
            desc = re.sub(r"\s+", " ", it["description"]).strip()
            out.append(f"      <description>{yml_escape(desc)}</description>")
        out.append("      <available>true</available>")
        out.append("    </offer>\n")
    out.append("  </offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------------- MAIN ----------------
def parse_xml_to_yml(xml_bytes: bytes, source_label: str) -> str:
    root = ET.fromstring(xml_bytes)
    nodes = guess_item_nodes(root)
    offers_total = len(nodes)

    offers: List[Dict[str, Any]] = []
    prices_picked = 0

    for node in nodes:
        it = parse_item(node)
        if not it:
            continue
        if it.get("price", 0) and it["price"] > 100:
            prices_picked += 1
        offers.append(it)

    return build_yml(offers, source_label, offers_total, prices_picked)

def main() -> int:
    try:
        data = read_source_bytes(SUPPLIER_URL)
        yml = parse_xml_to_yml(data, SUPPLIER_URL or "(local)")
    except Exception as e:
        yml = build_yml([], SUPPLIER_URL or "(unknown)", 0, 0)
        print(f"ERROR: {e}", file=sys.stderr)

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
        f.write(yml)
    print(f"Wrote: {OUT_FILE} | encoding={OUTPUT_ENCODING}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
