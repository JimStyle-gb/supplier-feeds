# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint -> YML (KZT) под единый шаблон.

Маппинг:
  <name>        = <НоменклатураКратко>
  <description> = <Номенклатура> + блок "Технические характеристики" из:
                  <Ресурс>, <ТипПечати>, <ЦветПечати>, <СовместимостьСМоделями>, <Вес>, <Принтеры>/<Принтер>

ID и код:
  id = vendorCode = "NP" + <Артикул_без_префикса NV-> (пробелы удаляем)

Цена:
  1) <Договор НомерДоговора="ТА-000079"> -> <Цена> (Казахстан приоритет)
  2) иначе <Договор НомерДоговора="TA-000079Мск"> -> <Цена> (Москва)
  3) иначе 100
  4) применяем PRICING_RULES (твои) и округляем вверх до ...900

Вывод:
  - только нужные теги (name, vendor, vendorCode, price, currencyId, picture, description, available)
  - без <categories>, <categoryId>, <url>, quantity-тегов
  - всем <available>true</available>
  - кодировка файла: windows-1251
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
SUPPLIER_URL    = "https://api.nvprint.ru/api/hs/getprice/398/881105302369/none/?format=xml&getallinfo=true"
OUT_FILE        = "docs/nvprint.yml"
OUTPUT_ENCODING = "windows-1251"
HTTP_TIMEOUT    = 45.0
RETRIES         = 4
RETRY_BACKOFF_S = 2.0

# Basic-Auth из секретов CI
NV_LOGIN        = (os.getenv("NVPRINT_LOGIN") or os.getenv("NVPRINT_XML_USER") or "").strip()
NV_PASSWORD     = (os.getenv("NVPRINT_PASSWORD") or os.getenv("NVPRINT_XML_PASS") or "").strip()

# ---------------- ВСПОМОГАТЕЛЬНЫЕ ----------------
def yml_escape(s: str) -> str:
    """Экранирование текста для YML/XML."""
    return html.escape((s or "").strip())

def strip_ns(tag: str) -> str:
    """Удалить XML namespace из имени тега."""
    if not tag:
        return tag
    if tag.startswith("{"):
        i = tag.rfind("}")
        if i != -1:
            return tag[i+1:]
    return tag

def parse_number(txt: Optional[str]) -> Optional[float]:
    """Извлечь число из строки, поддержать '6 914,28' и т.п."""
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
    """Взять .text первого дочернего тега из списка имен (без рекурсии)."""
    names = {t.lower() for t in tag_names}
    for ch in item:
        if strip_ns(ch.tag).lower() in names:
            val = (ch.text or "").strip()
            if val:
                return val
    return None

def find_descendant(item: ET.Element, tag_names: List[str]) -> Optional[ET.Element]:
    """Найти первый потомок (в глубину) с именем тега из списка."""
    names = {t.lower() for t in tag_names}
    for node in item.iter():
        if strip_ns(node.tag).lower() in names:
            return node
    return None

def find_descendant_text(item: ET.Element, tag_names: List[str]) -> Optional[str]:
    """Взять .text первого потомка (в глубину) с именем тега из списка."""
    node = find_descendant(item, tag_names)
    if node is not None:
        txt = (node.text or "").strip()
        if txt:
            return txt
    return None

def read_source_bytes() -> bytes:
    """Скачать исходный XML с авторизацией и ретраями."""
    if not SUPPLIER_URL:
        raise RuntimeError("SUPPLIER_URL пуст")
    if requests is None:
        raise RuntimeError("requests недоступен")

    auth = (NV_LOGIN, NV_PASSWORD) if (NV_LOGIN or NV_PASSWORD) else None
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(SUPPLIER_URL, timeout=HTTP_TIMEOUT, auth=auth)
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized: проверь NVPRINT_LOGIN/NVPRINT_PASSWORD в secrets")
            r.raise_for_status()
            b = r.content
            if not b:
                raise RuntimeError("Источник вернул пустой ответ")
            return b
        except Exception as e:
            last_err = e
            if attempt >= RETRIES or ("401" in str(e)):
                break
            time.sleep(RETRY_BACKOFF_S * attempt)
    raise RuntimeError(str(last_err) if last_err else "Не удалось скачать источник")

# ---------------- ЦЕНА ИЗ ДОГОВОРОВ ----------------
def _norm_contract(s: str) -> str:
    """Нормализуем номер договора: латинизируем похожие буквы, убираем пробелы/дефисы/подчёркивания, upper."""
    if not s:
        return ""
    tr = str.maketrans({
        "А":"A","В":"B","Е":"E","К":"K","М":"M","Н":"H","О":"O","Р":"P","С":"C","Т":"T","Х":"X","У":"Y",
        "а":"A","в":"B","е":"E","к":"K","м":"M","н":"H","о":"O","р":"P","с":"C","т":"T","х":"X","у":"Y",
        "Ё":"E","ё":"e",
    })
    u = s.translate(tr).upper()
    u = re.sub(r"[\s\-\_]+", "", u)
    return u  # TA000079, TA000079МСК

def _extract_price_from_contracts(item: ET.Element) -> Optional[float]:
    """Вернуть цену по приоритету: Казахстан (ТА-000079) → Москва (TA-000079Мск), иначе None."""
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

# ---------------- ПРАВИЛА ЦЕНООБРАЗОВАНИЯ ----------------
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
    """Обрезать префикс NV-/NV_/NV (игнор регистра), удалить пробелы внутри артикула."""
    s = (raw or "").strip()
    s = re.sub(r"^\s*NV[\-\_\s]+", "", s, flags=re.IGNORECASE)
    s = s.replace(" ", "")
    return s

def make_ids_from_article(article: str) -> Tuple[str, str]:
    """
    Возвращаем (id, vendorCode) с одинаковым NP-префиксом:
      id = "NP" + <очищенный_артикул>
      vendorCode = "NP" + <очищенный_артикул>
    """
    ac = clean_article(article)
    pref = "NP" + ac
    return pref, pref

def collect_printers(item: ET.Element) -> List[str]:
    """Собрать все значения из <Принтеры><Принтер>...</Принтер>..."""
    printers: List[str] = []
    node = find_descendant(item, ["Принтеры"])
    if node is not None:
        for ch in node.iter():
            if strip_ns(ch.tag).lower() == "принтер":
                t = (ch.text or "").strip()
                if t:
                    printers.append(t)
    # Убрать дубли, сохранить порядок
    seen = set(); uniq: List[str] = []
    for p in printers:
        if p not in seen:
            seen.add(p); uniq.append(p)
    return uniq

def build_description(item: ET.Element) -> str:
    """Собрать описание по ТЗ: Номенклатура + блок характеристик из заданных тегов."""
    parts: List[str] = []

    # 1) Первая строка — Номенклатура
    nom_full = find_descendant_text(item, ["Номенклатура"]) or ""
    nom_full = re.sub(r"\s+", " ", nom_full).strip()
    if nom_full:
        parts.append(nom_full)

    # 2) Технические характеристики (только непустые)
    specs: List[str] = []
    resurs = find_descendant_text(item, ["Ресурс"])
    if resurs and resurs.strip() and resurs.strip() != "0":
        specs.append(f"- Ресурс: {resurs.strip()}")

    tip = find_descendant_text(item, ["ТипПечати"])
    if tip:
        specs.append(f"- Тип печати: {tip.strip()}")

    color = find_descendant_text(item, ["ЦветПечати"])
    if color:
        specs.append(f"- Цвет печати: {color.strip()}")

    compat = find_descendant_text(item, ["СовместимостьСМоделями"])
    if compat:
        compat = re.sub(r"\s+", " ", compat).strip()
        specs.append(f"- Совместимость с моделями: {compat}")

    weight = find_descendant_text(item, ["Вес"])
    if weight:
        specs.append(f"- Вес: {weight.strip()}")

    prn_list = collect_printers(item)
    if prn_list:
        specs.append(f"- Принтеры: {', '.join(prn_list)}")

    if specs:
        parts.append("Технические характеристики:")
        parts.extend(specs)

    desc = "\n".join(parts).strip()
    return desc

def parse_item(elem: ET.Element) -> Optional[Dict[str, Any]]:
    """Распарсить один товар из XML NVPrint в словарь для YML."""
    # Обязательные поля
    article = first_child_text(elem, ["Артикул","articul","sku","article","PartNumber"])
    if not article:
        return None

    name = first_child_text(elem, ["НоменклатураКратко"])
    if not name:
        return None
    name = re.sub(r"\s+", " ", name).strip()

    # Цена: КЗ -> МСК -> 100, затем правила
    base = _extract_price_from_contracts(elem)
    base_int = 100 if (base is None or base <= 0) else int(math.ceil(base))
    final_price = compute_price_from_supplier(base_int)

    # Бренд (если есть)
    vendor = first_child_text(elem, ["Бренд","Производитель","Вендор","Brand","Vendor"]) or ""

    # Картинка (если есть)
    picture = (first_child_text(elem, ["СсылкаНаКартинку","Картинка","Изображение","Фото","Picture","Image","ФотоURL","PictureURL"]) or "").strip()

    # Описание
    description = build_description(elem)

    # ID и vendorCode
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
    """Эвристика: выбираем узлы, у которых есть и Артикул, и НоменклатураКратко."""
    items: List[ET.Element] = []
    seen: set = set()
    for node in root.iter():
        art = find_descendant(node, ["Артикул","articul","sku","article","PartNumber"])
        if art is None:
            continue
        nmk = find_descendant(node, ["НоменклатураКратко"])
        if nmk is None:
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
    """Собрать YML со структурой под Satu (минимум, красиво отформатировано)."""
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
            # заранее нормализуем, чтобы не было backslash в f-строке
            desc_clean = re.sub(r"\s+", " ", it["description"]).strip()
            out.append(f"      <description>{yml_escape(desc_clean)}</description>")
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
        data = read_source_bytes()
        yml = parse_xml_to_yml(data, SUPPLIER_URL)
    except Exception as e:
        # На ошибке всё равно пишем пустой корректный YML, чтобы CI не падал с пустым файлом
        yml = build_yml([], SUPPLIER_URL, 0, 0)
        print(f"ERROR: {e}", file=sys.stderr)

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
        f.write(yml)
    print(f"Wrote: {OUT_FILE} | encoding={OUTPUT_ENCODING}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
