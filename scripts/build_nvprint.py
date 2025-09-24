# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint -> Satu YML (упрощенный конвертер под общий шаблон)

Что делает:
- Тянет исходный XML у поставщика (URL из ENV: NVPRINT_XML_URL или SUPPLIER_URL).
- Парсит товары из узлов <Товары>/<Товар>.
- Берёт:
    name        = <НоменклатураКратко> или <Номенклатура>
    article     = <Артикул>  (ОБЯЗАТЕЛЬНО, иначе оффер пропускаем)
    price       = первое числовое <Цена> в блоке <УсловияПродаж>/<Договор>
    picture     = <СсылкаНаКартинку> (если пусто, не выводим <picture>)
    description = name  (минимально; можно расширить при необходимости)
- Идентификаторы:
    offer/@id   = article с отрезанным только ведущим "NV-" (например "NV-CF232A-SET2" -> "CF232A-SET2")
    vendorCode  = "NP" + offer_id  (например "NPCF232A-SET2")
- Фильтр:
    docs/nvprint_keywords.txt - автодетект кодировки; оставляем товар, если name начинается с одного из слов.
    Если файл не найден или пуст - ничего не фильтруем.
- Вывод:
    <categories> и <categoryId> отсутствуют.
    У КАЖДОГО товара ровно один <available>true</available>.
    Валюта KZT.
    Человекочитаемые пустые строки между <offer> для удобства.

ENV:
    NVPRINT_XML_URL или SUPPLIER_URL  - источник XML
    OUT_FILE                          - путь для вывода (по умолчанию docs/nvprint.yml)
    OUTPUT_ENCODING                   - кодировка файла (по умолчанию windows-1251)
"""

from __future__ import annotations
import os, re, sys, html, time, random
from typing import List, Dict, Any, Optional
from datetime import datetime

import requests
import xml.etree.ElementTree as ET

# ---------------------------- настройки ----------------------------

SUPPLIER_URL    = (os.getenv("NVPRINT_XML_URL") or os.getenv("SUPPLIER_URL") or "").strip()
OUT_FILE        = os.getenv("OUT_FILE", "docs/nvprint.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING") or "windows-1251").strip() or "windows-1251"
TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "45"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF_S = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))

KEYWORDS_PATH   = os.getenv("NVPRINT_KEYWORDS_PATH", "docs/nvprint_keywords.txt")

UA = {"User-Agent": "supplier-feeds/nvprint 1.0"}

# ---------------------------- лог и утилиты ----------------------------

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print("WARN: " + msg, file=sys.stderr, flush=True)

def die(msg: str, code: int = 1) -> None:
    print("ERROR: " + msg, file=sys.stderr, flush=True)
    sys.exit(code)

def x(s: Optional[str]) -> str:
    # Экранируем спецсимволы для XML
    return html.escape((s or "").strip())

def file_read_autoenc(path: str) -> str:
    # Чтение текстового файла с автоподбором кодировки
    encs = ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251","cp866")
    for enc in encs:
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read().replace("\ufeff","").replace("\x00","")
        except Exception:
            pass
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read().replace("\x00","")

def load_keywords(path: str) -> List[str]:
    # Загружаем ключевые слова для фильтра с обрезкой комментов и пустых строк
    if not path or not os.path.isfile(path):
        return []
    data = file_read_autoenc(path)
    out: List[str] = []
    for ln in data.splitlines():
        s = ln.strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out

def compile_prefix_patterns(kws: List[str]) -> List[re.Pattern]:
    # Готовим якорные шаблоны "начинается с слова"
    pats: List[re.Pattern] = []
    for kw in kws:
        k = re.sub(r"\s+", " ", kw.strip())
        if not k:
            continue
        pats.append(re.compile(r"^\s*" + re.escape(k) + r"(?!\w)", re.I))
    return pats

def starts_with_any(name: str, pats: List[re.Pattern]) -> bool:
    # Проверяем, начинается ли name с любого ключевого слова
    if not pats:
        return True
    return any(p.search(name or "") for p in pats)

def parse_float(txt: Optional[str]) -> Optional[float]:
    # Нормализация числа с запятой/пробелами
    if not txt:
        return None
    t = txt.replace("\xa0"," ").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

# ---------------------------- сеть ----------------------------

def fetch_xml(url: str) -> bytes:
    if not url:
        die("NVPRINT_XML_URL не задан")
    sess = requests.Session()
    last = None
    for i in range(1, RETRIES + 1):
        try:
            r = sess.get(url, headers=UA, timeout=TIMEOUT_S)
            r.raise_for_status()
            b = r.content
            if len(b) < MIN_BYTES:
                raise RuntimeError(f"слишком мало данных: {len(b)} байт")
            return b
        except Exception as e:
            last = e
            if i < RETRIES:
                sl = RETRY_BACKOFF_S * i * (1.0 + random.uniform(-0.2, 0.2))
                warn(f"попытка {i}/{RETRIES} не удалась: {e}; ждём {sl:.1f}s")
                time.sleep(sl)
    die(f"не удалось скачать источник: {last}")

# ---------------------------- парсинг NVPrint ----------------------------

def get_first_text(node: ET.Element, tag_name: str) -> Optional[str]:
    # Возвращает .text первого дочернего тега с точным именем без пространств имён
    for ch in node:
        nm = ch.tag.split("}", 1)[-1]
        if nm == tag_name:
            t = (ch.text or "").strip()
            if t:
                return t
    return None

def find_any_text(node: ET.Element, names: List[str]) -> Optional[str]:
    # Ищем первый из списка имён тегов
    for nm in names:
        t = get_first_text(node, nm)
        if t:
            return t
    return None

def extract_price(node: ET.Element) -> Optional[float]:
    # Цена - первое непустое числовое <Цена> внутри <УсловияПродаж>/<Договор>
    cond = get_first_child(node, "УсловияПродаж")
    if cond is None:
        return None
    for contract in cond:
        nm = contract.tag.split("}", 1)[-1]
        if nm != "Договор":
            continue
        price = get_first_text(contract, "Цена")
        val = parse_float(price)
        if val is not None:
            return val
    return None

def get_first_child(node: ET.Element, name: str) -> Optional[ET.Element]:
    for ch in node:
        if ch.tag.split("}", 1)[-1] == name:
            return ch
    return None

def normalize_offer_id_from_article(article: str) -> str:
    # Извлекаем offer_id из <Артикул>; срезаем только ведущий NV-
    s = (article or "").strip()
    s = re.sub(r"^\s*NV-", "", s, flags=re.I)  # только спереди
    # Уберем недопустимые символы в id (оставим буквы/цифры/дефисы/подчеркивания)
    s = re.sub(r"[^\w\-]+", "-", s).strip("-")
    return s or "NA"

def make_vendor_code_from_id(offer_id: str) -> str:
    # Формируем vendorCode: NP + offer_id (без дополнительного дефиса)
    base = (offer_id or "").strip()
    base = re.sub(r"^\-+", "", base)
    return "NP" + base

# ---------------------------- сборка YML ----------------------------

def build_yml(items: List[Dict[str, Any]], source: str) -> str:
    now_local = datetime.now().strftime("%Y-%m-%d %H:%M:%S +05")
    # Корневой узел
    root = ET.Element("yml_catalog")
    root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
    # Комментарий FEED_META
    feed_meta = (
        "\n"
        "<!--FEED_META\n"
        f"supplier                = NVPrint (NP)\n"
        f"source                  = {source}\n"
        f"built_Asia/Almaty       = {now_local} | Время сборки (Алматы)-->\n"
    )
    # Загружаем комментарий перед <shop>
    root_text_holder = ET.Comment(feed_meta)
    root.append(root_text_holder)

    shop = ET.SubElement(root, "shop")
    # Имя магазина можно опустить; для единообразия оставим пустым
    offers = ET.SubElement(shop, "offers")

    for it in items:
        off = ET.SubElement(offers, "offer")
        off.set("id", it["id"])

        nm = ET.SubElement(off, "name")
        nm.text = it["name"]

        vc = ET.SubElement(off, "vendorCode")
        vc.text = it["vendorCode"]

        pr = ET.SubElement(off, "price")
        pr.text = str(int(round(it["price"])))

        cur = ET.SubElement(off, "currencyId")
        cur.text = "KZT"

        if it.get("picture"):
            pic = ET.SubElement(off, "picture")
            pic.text = it["picture"]

        ds = ET.SubElement(off, "description")
        ds.text = it["description"]

        av = ET.SubElement(off, "available")
        av.text = "true"  # всегда true

    # Красивные отступы
    try:
        ET.indent(root, space="  ")
    except Exception:
        pass

    xml = ET.tostring(root, encoding=OUTPUT_ENCODING, xml_declaration=True).decode(OUTPUT_ENCODING, errors="replace")

    # Пустая строка между соседними <offer> для читабельности
    xml = re.sub(r"(</offer>)\n\s*(<offer\b)", r"\1\n\n    \2", xml)

    # Удаляем возможные пустые текстовые хвосты комментария
    xml = xml.replace("&lt;!--FEED_META", "<!--FEED_META").replace("--&gt;", "-->")
    return xml

# ---------------------------- MAIN ----------------------------

def main() -> int:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    raw = fetch_xml(SUPPLIER_URL)
    try:
        src = ET.fromstring(raw)
    except Exception as e:
        die(f"не могу распарсить XML: {e}")

    # Загружаем фильтр
    kws = load_keywords(KEYWORDS_PATH)
    pats = compile_prefix_patterns(kws)

    # Ищем список товаров
    goods_parent = src.find(".//Товары")
    if goods_parent is None:
        die("не найден блок <Товары>")

    out: List[Dict[str, Any]] = []
    for node in goods_parent.findall("Товар"):
        name = find_any_text(node, ["НоменклатураКратко", "Номенклатура"]) or ""
        if not name:
            continue
        if pats and not starts_with_any(name, pats):
            continue

        article = find_any_text(node, ["Артикул"]) or ""
        if not article:
            # Без артикула не сможем сформировать id по вашим правилам
            continue

        price = extract_price(node) or 1.0
        picture = find_any_text(node, ["СсылкаНаКартинку"]) or ""

        offer_id = normalize_offer_id_from_article(article)  # обрезали только ведущий NV-
        vendor_code = make_vendor_code_from_id(offer_id)     # NP + id

        out.append({
            "id": offer_id,
            "name": name,
            "vendorCode": vendor_code,
            "price": float(price),
            "picture": picture if picture else None,
            "description": name,  # минимально, без лишних полей
        })

    # Пишем YML
    xml = build_yml(out, SUPPLIER_URL or "(not set)")
    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml)

    log(f"Wrote: {OUT_FILE} | offers={len(out)} | encoding={OUTPUT_ENCODING} | keywords={len(kws)}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as e:
        die(str(e), 2)
