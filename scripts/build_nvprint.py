# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint -> YML (KZT), общий шаблон как у других поставщиков.

Правила:
- Цена берётся из <Договор> по приоритету:
    1) НомерДоговора == "ТА-000079" (Казахстан)
    2) НомерДоговора == "TA-000079Мск" (Москва)
    3) если нет цены ни там, ни там -> 100
- id и vendorCode из <Артикул>:
    id = <Артикул> без ведущего "NV-" (и похожих вариантов)
    vendorCode = "NP" + id
- available = true всем
- Пишем только нужные теги: name, vendor (если есть), vendorCode, price, currencyId, picture (если есть), description (если есть), available
- Кодировка вывода: windows-1251
"""

from __future__ import annotations
import os, re, io, html, math
import sys
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

try:
    import requests
except Exception:
    requests = None  # на всякий случай, чтобы не падать при локальном чтении

# ---------------- ПАРАМЕТРЫ ----------------
SUPPLIER_URL     = os.getenv("SUPPLIER_URL", "").strip()
OUT_FILE         = os.getenv("OUT_FILE", "docs/nvprint.yml")
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251")

# ---------------- УТИЛИТЫ ----------------
def yml_escape(s: str) -> str:
    return html.escape((s or "").strip())

def parse_number(txt: Optional[str]) -> Optional[float]:
    if not txt:
        return None
    t = txt.strip().replace("\u00A0", "").replace(" ", "")
    # допускаем "12345,67" и "12345.67"
    t = t.replace(",", ".")
    # оставим только числа и точку
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        return float(Decimal(m.group(0)))
    except (InvalidOperation, ValueError):
        return None

def strip_ns(tag: str) -> str:
    # убираем namespace вида {ns}Tag
    if not tag:
        return tag
    if tag[0] == "{":
        i = tag.rfind("}")
        if i != -1:
            return tag[i+1:]
    return tag

def first_child_text(item: ET.Element, tag_names: List[str]) -> Optional[str]:
    # ищем первый дочерний тег из списка (без учёта namespace)
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
    # если src пуст — ошибка
    if not src:
        raise RuntimeError("SUPPLIER_URL не задан")
    # если это локальный путь к файлу:
    if os.path.isfile(src):
        with io.open(src, "rb") as f:
            data = f.read()
        if not data:
            raise RuntimeError("Пустой локальный файл источника")
        return data
    # иначе — URL
    if requests is None:
        raise RuntimeError("requests недоступен для скачивания URL")
    resp = requests.get(src, timeout=40)
    resp.raise_for_status()
    data = resp.content
    if not data:
        raise RuntimeError("Источник вернул пустой ответ")
    return data

# ---------------- ЦЕНА ИЗ ДОГОВОРОВ ----------------
def _norm_contract(s: str) -> str:
    """
    Нормализация номера договора: латинизируем похожие кириллические буквы,
    убираем пробелы/дефисы/подчёркивания, приводим к верхнему регистру.
    """
    if not s:
        return ""
    tr = str.maketrans({
        "А":"A","В":"B","Е":"E","К":"K","М":"M","Н":"H","О":"O","Р":"P","С":"C","Т":"T","Х":"X","У":"Y",
        "а":"A","в":"B","е":"E","к":"K","м":"M","н":"H","о":"O","р":"P","с":"C","т":"T","х":"X","у":"Y",
        "Ё":"E","ё":"e",
    })
    u = s.translate(tr).upper()
    u = re.sub(r"[\s\-\_]+", "", u)
    return u

def _extract_price_from_contracts(item: ET.Element) -> Optional[float]:
    """
    Ищем цену в дочерних узлах <Договор>:
      приоритет: ТА-000079 (КЗ) -> TA-000079Мск (MSK)
    Возвращаем float или None.
    """
    price_kz: Optional[float] = None
    price_msk: Optional[float] = None

    for node in item.iter():
        if strip_ns(node.tag).lower() != "договор":
            continue
        num = (node.attrib.get("НомерДоговора") or node.attrib.get("Номердоговора") or "").strip()
        num_n = _norm_contract(num)  # "TA000079" или "TA000079МСК" -> "TA000079MSK"

        # интересуют только варианты с 000079
        if "000079" not in num_n:
            continue

        # читаем <Цена> внутри этого <Договор>
        p_el = find_descendant(node, ["Цена", "price", "amount", "value"])
        p_val = parse_number(p_el.text if p_el is not None else None)
        if p_val is None or p_val <= 0:
            continue

        # если встречается "MSK" — это московский договор
        if "MSK" in num_n:
            price_msk = p_val
        else:
            # договор КЗ — без "MSK"
            price_kz = p_val

    if price_kz is not None and price_kz > 0:
        return price_kz
    if price_msk is not None and price_msk > 0:
        return price_msk
    return None

# ---------------- ПАРСИНГ ТОВАРА ----------------
def clean_article(raw: str) -> str:
    """
    Очищаем артикул: удаляем ведущий NV- (или NV_, NV ), любые пробелы.
    """
    s = (raw or "").strip()
    s = re.sub(r"^\s*NV[\-\_\s]+", "", s, flags=re.IGNORECASE)
    s = s.replace(" ", "")
    return s

def make_ids_from_article(article: str) -> Tuple[str, str]:
    """
    Возвращает (id, vendorCode) по правилу:
      id = article_clean
      vendorCode = "NP" + article_clean
    """
    ac = clean_article(article)
    return ac, "NP" + ac

def parse_item(elem: ET.Element) -> Optional[Dict[str, Any]]:
    # Обязательные поля: Артикул и Наименование
    article = first_child_text(elem, ["Артикул", "articul", "sku", "article", "PartNumber"])
    if not article:
        return None
    name = first_child_text(elem, ["Наименование", "Название", "Name", "Товар", "Модель"])
    if not name:
        return None

    # Цена из договоров
    price = _extract_price_from_contracts(elem)
    if price is None or price <= 0:
        price = 100.0
    price_int = int(math.ceil(price))  # NVPrint — без наценки/правил; округляем вверх до целого

    # Бренд / производитель (если есть)
    vendor = first_child_text(elem, ["Бренд", "Производитель", "Вендор", "БрендПроизводитель", "Brand", "Vendor"]) or ""

    # Картинка / описание (если есть)
    picture = first_child_text(elem, ["Картинка", "Изображение", "Фото", "Picture", "Image", "ФотоURL", "PictureURL"]) or ""
    description = first_child_text(elem, ["Описание", "Description", "Текст", "About"]) or ""

    # Идентификаторы
    oid, vcode = make_ids_from_article(article)

    return {
        "id": oid,
        "title": name,
        "vendor": vendor,
        "vendorCode": vcode,
        "price": price_int,
        "picture": picture,
        "description": description,
    }

def guess_item_nodes(root: ET.Element) -> List[ET.Element]:
    """
    Универсальный способ найти "корневые" узлы товаров:
    - ищем элементы, которые содержат дочерний тег <Артикул>
    - возвращаем их родителя, если сам элемент - это не контейнер верхнего уровня
    """
    items: List[ET.Element] = []
    seen: set = set()
    for node in root.iter():
        # узел содержит Артикул?
        art = find_descendant(node, ["Артикул", "articul", "sku", "article", "PartNumber"])
        if art is None:
            continue
        # считаем этот node товаром, если у него есть и Наименование внутри
        name = find_descendant(node, ["Наименование", "Название", "Name", "Товар", "Модель"])
        if name is None:
            continue
        # ключ для уникальности
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
    """
    Мини-мета как у других фидов.
    Закрываем комментарий на строке '(Алматы)-->' — без дополнительного '-->' ниже.
    """
    pad = 28
    rows: List[str] = []
    def kv(k, v, cmt=""):
        if cmt:
            rows.append(f"{k.ljust(pad)} = {str(v):<60} | {cmt}")
        else:
            rows.append(f"{k.ljust(pad)} = {str(v)}")

    kv("supplier",           "nvprint",                         "Метка поставщика")
    kv("source",             source,                            "URL/файл источника")
    kv("offers_total",       offers_total,                      "Офферов в источнике (оценочно)")
    kv("offers_written",     offers_written,                    "Офферов записано")
    kv("prices_updated",     prices_picked,                     "Цены взяты из договоров (КЗ/МСК)")
    kv("available_forced",   offers_written,                    "Сколько офферов получили available=true")
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
            # однословный нормал-айзер пробелов
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

    # Находим узлы товаров
    nodes = guess_item_nodes(root)
    offers_total = len(nodes)

    offers: List[Dict[str, Any]] = []
    prices_picked = 0

    for node in nodes:
        it = parse_item(node)
        if not it:
            continue
        if it.get("price", 0) > 100:
            prices_picked += 1
        offers.append(it)

    return build_yml(offers, source_label, offers_total, prices_picked)

def main() -> int:
    try:
        src = SUPPLIER_URL
        data = read_source_bytes(src)
        yml = parse_xml_to_yml(data, src if src else "(local)")
    except Exception as e:
        # при ошибке — пишем пустой валидный YML
        yml = build_yml([], SUPPLIER_URL or "(unknown)", 0, 0)
        print(f"ERROR: {e}", file=sys.stderr)

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
        f.write(yml)
    print(f"Wrote: {OUT_FILE} | encoding={OUTPUT_ENCODING}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
