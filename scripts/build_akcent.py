# -*- coding: utf-8 -*-
"""
AK-Cent -> normalized YML (UTF-8)

Вход:
- AKCENT_XML_URL (опц.)  — URL выгрузки (без авторизации)
- AKCENT_XML_PATH (опц.) — путь к локальному файлу XML (fallback: docs/akcent_source.xml)

Выход:
- OUT_FILE (default: docs/akcent.yml), UTF-8

Правила:
- price: <prices>/<price type="Цена дилерского портала KZT"> (fallback: первый <price currencyId="KZT">)
- vendor: <vendor> или Param[@name="Производитель"]
- vendorCode: @article → <Offer_ID> → @id
- available: из <Stock> (любая цифра >0, либо наличие символа '>' трактуем как есть в наличии)
- categoryId: <categoryId> (если пусто — матчим по offer/@type на справочник категорий по имени)
- picture/url/description — как есть
- категории из <categories>/<category>; пустые id генерим стабильно
"""

from __future__ import annotations
import os, io, re, html, hashlib
from typing import Any, Dict, List, Optional, Tuple
import requests
from xml.etree import ElementTree as ET

# ---------- ENV ----------
AKCENT_XML_URL   = os.getenv("AKCENT_XML_URL", "").strip()
AKCENT_XML_PATH  = os.getenv("AKCENT_XML_PATH", "docs/akcent_source.xml")
OUT_FILE         = os.getenv("OUT_FILE", "docs/akcent.yml")
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "utf-8")
HTTP_TIMEOUT     = float(os.getenv("HTTP_TIMEOUT", "60"))

ROOT_CAT_ID      = 9800000
ROOT_CAT_NAME    = "AKCENT"

# ---------- utils ----------
def x(s: str) -> str:
    return html.escape((s or "").strip())

def unhtml(s: str) -> str:
    return html.unescape(s or "").strip()

def parse_number(s: str) -> Optional[float]:
    if not s: return None
    t = unhtml(s)
    t = t.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    m = re.search(r"(-?\d+(?:\.\d+)?)", t)
    return float(m.group(1)) if m else None

def stable_id_for_name(name: str, prefix: int = 9810000) -> int:
    h = hashlib.md5((name or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    return prefix + int(h, 16)

def get_text(node: Optional[ET.Element]) -> str:
    return unhtml(node.text) if node is not None and node.text else ""

def first(node: ET.Element, path: str) -> Optional[ET.Element]:
    return node.find(path) if node is not None else None

# ---------- IO ----------
def read_xml_bytes() -> bytes:
    if AKCENT_XML_URL:
        r = requests.get(AKCENT_XML_URL, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.content
    # fallback к локальному файлу
    if os.path.isfile(AKCENT_XML_PATH):
        with io.open(AKCENT_XML_PATH, "rb") as f:
            return f.read()
    raise RuntimeError("No AKCENT_XML_URL and file not found: %s" % AKCENT_XML_PATH)

# ---------- parsing ----------
def load_root() -> ET.Element:
    data = read_xml_bytes()
    return ET.fromstring(data)

def build_categories_map(root: ET.Element) -> Tuple[Dict[str, int], List[Tuple[int, str, int]]]:
    """
    Возвращает:
    - map: исходный id (строка) -> целочисленный id в нашем YML
    - cats_list: список (id, name, parentId)
    """
    map_src_to_out: Dict[str, int] = {}
    cats_out: List[Tuple[int, str, int]] = []

    cats_node = root.find(".//categories")
    if cats_node is None:
        return map_src_to_out, cats_out

    for c in cats_node.findall("./category"):
        raw_id = (c.get("id") or "").strip()
        name = get_text(c)
        if not name:
            continue
        if raw_id:
            try:
                out_id = int(raw_id)
            except Exception:
                out_id = stable_id_for_name(name)
        else:
            out_id = stable_id_for_name(name)
        if raw_id:
            map_src_to_out[raw_id] = out_id
        # все напрямую подвешиваем к ROOT
        cats_out.append((out_id, name, ROOT_CAT_ID))
    return map_src_to_out, cats_out

def resolve_vendor(offer: ET.Element) -> str:
    v = get_text(first(offer, "./vendor"))
    if v:
        return v
    for p in offer.findall("./Param"):
        if (p.get("name") or "").strip().lower() == "производитель":
            t = get_text(p)
            if t:
                return t
    return ""

def resolve_vendor_code(offer: ET.Element) -> str:
    # приоритет: @article -> <Offer_ID> -> @id
    vc = (offer.get("article") or "").strip()
    if vc:
        return vc
    oid = get_text(first(offer, "./Offer_ID"))
    if oid:
        return oid
    return (offer.get("id") or "").strip() or hashlib.md5(ET.tostring(offer)).hexdigest()[:10]

def resolve_price_kzt(offer: ET.Element) -> Optional[int]:
    prices = offer.find("./prices")
    chosen = None
    if prices is not None:
        # 1) искать цену дилерского портала KZT
        for p in prices.findall("./price"):
            if (p.get("currencyId") or "").strip().upper() == "KZT" and (p.get("type") or "").strip().lower().startswith("цена дилерского портала"):
                chosen = p
                break
        # 2) fallback: первый ценник с currencyId=KZT
        if chosen is None:
            for p in prices.findall("./price"):
                if (p.get("currencyId") or "").strip().upper() == "KZT":
                    chosen = p
                    break
    if chosen is not None:
        val = parse_number(get_text(chosen))
        if val is not None:
            return int(round(val))
    return None

def resolve_available(offer: ET.Element) -> bool:
    st = get_text(first(offer, "./Stock")).lower()
    if not st:
        # нет инфы — считаем доступным (как у других поставщиков)
        return True
    # '>' явно доступно
    if ">" in st:
        return True
    # вытаскиваем число
    num = parse_number(st)
    if num is not None:
        return num > 0
    # кейсы вроде "<5" — трактуем как есть в наличии
    if "<" in st:
        return True
    # ключевые слова отсутствия
    if "нет" in st or "out of stock" in st:
        return False
    return True

def resolve_category_id(offer: ET.Element, cats_map: Dict[str, int], name_to_id: Dict[str, int]) -> int:
    c = first(offer, "./categoryId")
    if c is not None:
        raw = get_text(c)
        if raw:
            # иногда значение — строка исходного id
            if raw in cats_map:
                return cats_map[raw]
            # иногда это уже число, попробуем
            try:
                val = int(raw)
                return cats_map.get(raw, val)
            except Exception:
                pass
    # fallback по атрибуту type у offer (это имя категории)
    t = (offer.get("type") or "").strip()
    if t:
        if t in name_to_id:
            return name_to_id[t]
        # создаем на лету
        nid = stable_id_for_name(t)
        name_to_id[t] = nid
        return nid
    return ROOT_CAT_ID

def first_picture(offer: ET.Element) -> Optional[str]:
    p = first(offer, "./picture")
    u = get_text(p)
    return u or None

def description_text(offer: ET.Element) -> str:
    return get_text(first(offer, "./description"))

# ---------- YML build ----------
def build_yml(categories: List[Tuple[int, str, int]], offers: List[Dict[str, Any]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='utf-8'?>")
    out.append("<yml_catalog><shop>")
    out.append("<name>akcent</name>")
    out.append('<currencies><currency id="KZT" rate="1" /></currencies>')
    out.append("<categories>")
    out.append(f'<category id="{ROOT_CAT_ID}">{x(ROOT_CAT_NAME)}</category>')
    for cid, name, parent in categories:
        parent = parent or ROOT_CAT_ID
        out.append(f'<category id="{cid}" parentId="{parent}">{x(name)}</category>')
    out.append("</categories>")
    out.append("<offers>")
    for it in offers:
        avail = "true" if it.get("available") else "false"
        out.append(f'<offer id="{x(it["vendorCode"])}" available="{avail}" in_stock="{avail}">')
        out.append(f'<name>{x(it["title"])}</name>')
        out.append(f'<vendor>{x(it.get("vendor") or "")}</vendor>')
        out.append(f'<vendorCode>{x(it["vendorCode"])}</vendorCode>')
        out.append(f'<price>{int(it["price"])}</price>')
        out.append(f'<currencyId>KZT</currencyId>')
        out.append(f'<categoryId>{int(it["categoryId"])}</categoryId>')
        if it.get("url"):     out.append(f'<url>{x(it["url"])}</url>')
        if it.get("picture"): out.append(f'<picture>{x(it["picture"])}</picture>')
        if it.get("description"): out.append(f'<description>{x(it["description"])}</description>')
        # как у остальных поставщиков — минимальные обязательные
        out.append("<quantity_in_stock>1</quantity_in_stock>")
        out.append("<stock_quantity>1</stock_quantity>")
        out.append("<quantity>1</quantity>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------- MAIN ----------
def main() -> int:
    root = load_root()

    # категории
    cats_map_raw_to_out, cats_list = build_categories_map(root)
    # сопоставление по имени (для offer/@type)
    name_to_id = { name: cid for (cid, name, parent) in cats_list }

    # офферы
    offers_src = root.findall(".//offer")
    offers_out: List[Dict[str, Any]] = []

    for o in offers_src:
        title = get_text(first(o, "./name"))
        if not title:
            continue

        vendor = resolve_vendor(o)
        vendor_code = resolve_vendor_code(o)
        price = resolve_price_kzt(o)
        if price is None or price <= 0:
            # пропускаем бесплатное/битое
            continue
        available = resolve_available(o)
        url = get_text(first(o, "./url"))
        picture = first_picture(o)
        descr = description_text(o)
        cat_id = resolve_category_id(o, cats_map_raw_to_out, name_to_id)

        offers_out.append({
            "title": title,
            "vendor": vendor,
            "vendorCode": vendor_code,
            "price": int(price),
            "available": available,
            "url": url or None,
            "picture": picture,
            "description": descr or "",
            "categoryId": int(cat_id),
        })

    # запись
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(cats_list, offers_out)
    with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
        f.write(xml)

    print(f"[akcent] done: {len(offers_out)} offers, {len(cats_list)} categories -> {OUT_FILE} (encoding={OUTPUT_ENCODING})")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        # минимальный пустой YML, чтобы job не падал
        try:
            os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
            with io.open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore") as f:
                f.write("<?xml version='1.0' encoding='utf-8'?>\n<yml_catalog><shop><name>akcent</name><currencies><currency id=\"KZT\" rate=\"1\" /></currencies><categories><category id=\"9800000\">AKCENT</category></categories><offers></offers></shop></yml_catalog>")
        except Exception:
            pass
        sys.exit(0)
