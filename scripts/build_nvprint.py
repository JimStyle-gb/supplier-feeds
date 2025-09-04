# -*- coding: utf-8 -*-
"""
NVPrint: XML API (getallinfo=true) -> YML (KZT)
- База: цены, остатки, разделы из XML.
- Фото/описание/характеристики: ПРЯМО из XML (если выданы с getallinfo=true).
- Обогащение с сайта nvprint.ru оставлено опциональным (по умолчанию выключено).

Настройки через ENV (важные):
  NVPRINT_XML_URL            — полный URL XML (включая getallinfo=true).
  NVPRINT_LOGIN/PASSWORD     — если API под BasicAuth.

  Кастом тэгов (через запятую): NVPRINT_PICS_TAGS, NVPRINT_DESC_TAGS, NVPRINT_PARAMS_BLOCK_TAGS,
  NVPRINT_PARAM_NAME_TAGS, NVPRINT_PARAM_VALUE_TAGS и т.д. (см. ниже).
"""

from __future__ import annotations
import os, re, sys, html, hashlib, time
from typing import Any, Dict, List, Optional, Tuple
import requests, xml.etree.ElementTree as ET
from datetime import datetime

# ---------- ENV: XML ----------
XML_URL      = os.getenv("NVPRINT_XML_URL", "").strip()
NV_LOGIN     = (os.getenv("NVPRINT_LOGIN") or os.getenv("NVPRINT_XML_USER") or "").strip()
NV_PASSWORD  = (os.getenv("NVPRINT_PASSWORD") or os.getenv("NVPRINT_XML_PASS") or "").strip()

OUT_FILE     = os.getenv("OUT_FILE", "docs/nvprint.yml")
ENCODING     = (os.getenv("OUTPUT_ENCODING") or "utf-8").lower()
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))
MAX_PICTURES = int(os.getenv("MAX_PICTURES", "10"))

# ---------- ENV: парсинг XML ----------
ITEM_XPATH   = (os.getenv("NVPRINT_ITEM_XPATH") or "").strip()  # например ".//Товар"
NAME_OVR     = os.getenv("NVPRINT_NAME_TAGS")
PRICEKZT_OVR = os.getenv("NVPRINT_PRICE_KZT_TAGS")
PRICEANY_OVR = os.getenv("NVPRINT_PRICE_TAGS")
SKU_OVR      = os.getenv("NVPRINT_SKU_TAGS")
VENDOR_OVR   = os.getenv("NVPRINT_VENDOR_TAGS")
QTY_OVR      = os.getenv("NVPRINT_QTY_TAGS")
DESC_OVR     = os.getenv("NVPRINT_DESC_TAGS")
URL_OVR      = os.getenv("NVPRINT_URL_TAGS")
CAT_OVR      = os.getenv("NVPRINT_CAT_TAGS")
SUBCAT_OVR   = os.getenv("NVPRINT_SUBCAT_TAGS")
PIC_OVR      = os.getenv("NVPRINT_PIC_TAGS")              # одиночные поля вида ImageURL
PICS_OVR     = os.getenv("NVPRINT_PICS_TAGS")             # множественные поля (галерея)
BARCODE_OVR  = os.getenv("NVPRINT_BARCODE_TAGS")
CATPATH_OVR  = os.getenv("NVPRINT_CAT_PATH_TAGS")

# Характеристики (если XML отдаёт пары имя/значение)
PARAMS_BLOCK_OVR = os.getenv("NVPRINT_PARAMS_BLOCK_TAGS")     # контейнеры, например "Характеристики,Specs,Attributes"
PARAM_NAME_OVR   = os.getenv("NVPRINT_PARAM_NAME_TAGS")       # "Имя,Name,Параметр"
PARAM_VALUE_OVR  = os.getenv("NVPRINT_PARAM_VALUE_TAGS")      # "Значение,Value,Знач"

# ---------- ENV: (опц.) обогащение с nvprint.ru — отключено по умолчанию ----------
ENRICH_SITE       = os.getenv("NVPRINT_ENRICH_FROM_SITE", "0") == "1"
ENRICH_LIMIT      = int(os.getenv("NVPRINT_ENRICH_LIMIT", "0"))      # 0 = выключено / все
ENRICH_DELAY_MS   = int(os.getenv("NVPRINT_ENRICH_DELAY_MS", "250"))

ROOT_CAT_ID   = 9400000
ROOT_CAT_NAME = "NVPrint"
UA = {"User-Agent": "Mozilla/5.0 (compatible; NVPrint-XML-Feed/2.1)"}

def x(s: str) -> str: return html.escape((s or "").strip())
def stable_cat_id(text: str, prefix: int = 9420000) -> int:
    h = hashlib.md5((text or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    return prefix + int(h, 16)

# ---------- HTTP ----------
def fetch_xml_bytes(url: str) -> bytes:
    if not url: raise RuntimeError("NVPRINT_XML_URL пуст.")
    auth = (NV_LOGIN, NV_PASSWORD) if (NV_LOGIN or NV_PASSWORD) else None
    r = requests.get(url, auth=auth, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    b = r.content
    # лог для дебага
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    try:
        with open("docs/nvprint_source.xml", "wb") as f:
            f.write(b[:15_000_000])
    except Exception:
        pass
    return b

# ---------- XML helpers ----------
def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def split_tags(s: Optional[str], defaults: List[str]) -> List[str]:
    if not s: return defaults
    parts = [p.strip() for p in re.split(r"[,|;]+", s) if p.strip()]
    return parts or defaults

def parse_number(s: Optional[str]) -> Optional[float]:
    if not s: return None
    t = s.replace("\xa0"," ").replace(" ","").replace(",",".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m: return None
    try: return float(m.group(0))
    except: return None

def first_desc_text(item: ET.Element, names: List[str]) -> Optional[str]:
    names_l = {n.lower() for n in names}
    for ch in item.iter():
        nm = strip_ns(ch.tag).lower()
        if nm in names_l:
            txt = (ch.text or "").strip() if ch.text else ""
            if txt: return txt
    return None

def all_desc_texts_like(item: ET.Element, substrs: List[str]) -> List[str]:
    subs = [s.lower() for s in substrs]
    out: List[str] = []
    for ch in item.iter():
        nm = strip_ns(ch.tag).lower()
        if any(s in nm for s in subs):
            txt = (ch.text or "").strip() if ch.text else ""
            if txt: out.append(txt)
    return out

# ---------- items guess ----------
def guess_items(root: ET.Element) -> List[ET.Element]:
    if ITEM_XPATH:
        items = root.findall(ITEM_XPATH)
        if items: return items
    cands = root.findall(".//Товар") + root.findall(".//item") + root.findall(".//product") + root.findall(".//row")
    if cands: return cands
    NAME_TAGS = split_tags(NAME_OVR, ["НоменклатураКратко","Номенклатура","full_name","name","title","наименование"])
    PRICE_ANY = split_tags(PRICEANY_OVR, ["Цена","price","amount","value","цена"])
    out: List[ET.Element] = []
    for node in root.iter():
        if first_desc_text(node, NAME_TAGS) and first_desc_text(node, PRICE_ANY):
            out.append(node)
    return out

# ---------- tag sets ----------
NAME_TAGS       = split_tags(NAME_OVR,      ["НоменклатураКратко","Номенклатура","full_name","name","title","наименование"])
VENDOR_TAGS     = split_tags(VENDOR_OVR,    ["brand","бренд","вендор","producer","manufacturer","производитель"])
SKU_TAGS        = split_tags(SKU_OVR,       ["Артикул","articul","sku","vendorcode","кодтовара","code","код"])
PRICE_KZT_TAGS  = split_tags(PRICEKZT_OVR,  ["ЦенаТенге","price_kzt","ценатенге","цена_kzt","kzt"])
PRICE_ANY_TAGS  = split_tags(PRICEANY_OVR,  ["Цена","price","amount","value","цена"])
URL_TAGS        = split_tags(URL_OVR,       ["url","link","ссылка"])
DESC_TAGS       = split_tags(DESC_OVR,      ["Описание","ПолноеОписание","Description","FullDescription","descr","short_description"])
CAT_TAGS        = split_tags(CAT_OVR,       ["РазделПрайса","category","категория","group","раздел"])
SUBCAT_TAGS     = split_tags(SUBCAT_OVR,    ["subcategory","подкатегория","subgroup","подраздел"])

# картинки: одиночные имена полей (типа ImageURL) + "подобные" имена
PIC_SINGLE_TAGS  = split_tags(PIC_OVR,      ["Image","ImageURL","Photo","Picture","Картинка","Изображение"])
PIC_LIKE         = ["image","img","photo","picture","картин","изобр","фото"]
PICS_LIST_TAGS   = split_tags(PICS_OVR,     ["Images","Pictures","Photos","Галерея","Картинки","Изображения"])

QTY_TAGS        = split_tags(QTY_OVR,       ["Наличие","quantity","qty","stock","остаток"])
BARCODE_TAGS    = split_tags(BARCODE_OVR,   ["Штрихкод","barcode","ean","ean13"])
CATPATH_TAGS    = split_tags(CATPATH_OVR,   ["category_path","full_path","path","путь"])

# характеристики
PARAMS_BLOCK_TAGS = split_tags(PARAMS_BLOCK_OVR, ["Характеристики","Specs","Attributes","Параметры","ПараметрыТовара"])
PARAM_NAME_TAGS   = split_tags(PARAM_NAME_OVR,   ["Имя","Name","Параметр","Показатель","Характеристика"])
PARAM_VALUE_TAGS  = split_tags(PARAM_VALUE_OVR,  ["Значение","Value","Величина","ПараметрЗначение"])

# общие регексы
IMG_RE = re.compile(r"https?://[^\s'\"<>]+?\.(?:jpg|jpeg|png|webp|gif)(?:\?[^\s'\"<>]*)?$", re.I)
SEP_RE = re.compile(r"\s*(?:>|/|\\|\||→|»|›|—|-)\s*")

# ---------- extract helpers ----------
def extract_category_path(item: ET.Element) -> List[str]:
    for t in CATPATH_TAGS:
        val = first_desc_text(item, [t])
        if val:
            parts = [p.strip() for p in SEP_RE.split(val) if p.strip()]
            if parts:
                return parts[:4]
    cat  = first_desc_text(item, CAT_TAGS) or ""
    scat = first_desc_text(item, SUBCAT_TAGS) or ""
    path = [p for p in [cat, scat] if p]
    if path:
        return path
    cand = all_desc_texts_like(item, ["category","категор","group","раздел"])
    seen = set(); clean = []
    for v in cand:
        vv = v.strip()
        if not vv or vv.lower() in seen:
            continue
        seen.add(vv.lower())
        if len(vv) < 2:
            continue
        clean.append(vv)
        if len(clean) >= 2:
            break
    return clean

def extract_pictures(item: ET.Element) -> List[str]:
    pics: List[str] = []
    # 1) явные одиночные поля
    for t in PIC_SINGLE_TAGS:
        txt = first_desc_text(item, [t])
        if txt:
            for m in IMG_RE.findall(txt):
                pics.append(m)
    # 2) контейнеры-галереи: пройдём по потомкам
    def walk_and_collect(el: ET.Element):
        nm = strip_ns(el.tag).lower()
        # если имя тега "похоже" на картинку — берём текст
        if any(k in nm for k in PIC_LIKE):
            if el.text:
                for m in IMG_RE.findall(el.text.strip()):
                    pics.append(m)
        # проверим атрибуты (на всякий)
        for _, v in (el.attrib or {}).items():
            for m in IMG_RE.findall(str(v)):
                pics.append(m)
        for ch in el:
            walk_and_collect(ch)
    for node in item:
        nn = strip_ns(node.tag).lower()
        if nn in [n.lower() for n in PICS_LIST_TAGS] or any(k in nn for k in PIC_LIKE):
            walk_and_collect(node)
    # 3) общий проход по всем узлам — вдруг где-то просто текстом лежат ссылки
    for ch in item.iter():
        if ch.text:
            for m in IMG_RE.findall(ch.text.strip()):
                pics.append(m)
    # уникализируем и ограничим
    uniq = []
    seen = set()
    for u in pics:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq[:MAX_PICTURES]

def extract_description(item: ET.Element) -> Optional[str]:
    txt = first_desc_text(item, DESC_TAGS)
    if txt and len(txt.strip()) >= 10:
        return txt.strip()
    return None

def extract_params(item: ET.Element) -> Dict[str, str]:
    params: Dict[str, str] = {}
    # Вариант 1: найти блоки-хранилища характеристик и внутри пары Имя/Значение
    blocks: List[ET.Element] = []
    for node in item.iter():
        nm = strip_ns(node.tag).lower()
        if nm in [b.lower() for b in PARAMS_BLOCK_TAGS] or "характер" in nm or "spec" in nm or "attrib" in nm:
            blocks.append(node)
    def add_pair(k: str, v: str):
        k = (k or "").strip(": ")
        v = (v or "").strip()
        if k and v and k not in params:
            params[k] = v
    for b in blocks:
        # пары Имя/Значение
        names: List[str]  = []
        values: List[str] = []
        for ch in b.iter():
            nm = strip_ns(ch.tag).lower()
            if nm in [p.lower() for p in PARAM_NAME_TAGS]:
                if ch.text: names.append(ch.text.strip())
            if nm in [p.lower() for p in PARAM_VALUE_TAGS]:
                if ch.text: values.append(ch.text.strip())
        for k, v in zip(names, values):
            add_pair(k, v)
        # на случай иного формата: "Параметр: Значение" одним тегом
        for ch in b.iter():
            if ch.text and ":" in ch.text and len(ch.text) < 200:
                k, v = ch.text.split(":", 1)
                add_pair(k, v)
    return params

# ---------- parse XML item ----------
def parse_xml_item(item: ET.Element) -> Optional[Dict[str, Any]]:
    name = first_desc_text(item, ["НоменклатураКратко"]) or first_desc_text(item, NAME_TAGS)
    if not name:
        return None

    vendor_code = first_desc_text(item, ["Артикул"]) or first_desc_text(item, SKU_TAGS) or ""
    vendor = first_desc_text(item, VENDOR_TAGS) or "NV Print"

    price = None
    for t in PRICE_KZT_TAGS:
        price = parse_number(first_desc_text(item, [t]))
        if price is not None:
            break
    if price is None:
        for t in PRICE_ANY_TAGS:
            price = parse_number(first_desc_text(item, [t]))
            if price is not None:
                break
    if price is None or price <= 0:
        return None

    # Кол-во/наличие
    qty = 0.0
    for t in QTY_TAGS:
        n = parse_number(first_desc_text(item, [t]))
        if n is not None:
            qty = max(qty, n)
    qty_int = int(round(qty)) if qty and qty > 0 else 0
    available = qty_int > 0

    # Категории
    path = extract_category_path(item)

    # Описание/картинки/параметры из XML
    desc = extract_description(item)
    if not desc:
        base = first_desc_text(item, ["Номенклатура"]) or name
        bits = [base]
        if vendor_code:
            bits.append(f"Артикул: {vendor_code}")
        desc = "; ".join(bits)

    pictures = extract_pictures(item)
    params = extract_params(item)

    url = first_desc_text(item, URL_TAGS) or ""  # если в XML есть

    return {
        "name": name,
        "vendor": vendor,
        "vendorCode": vendor_code,
        "price": price,
        "url": url,
        "pictures": pictures,
        "description": desc,
        "qty": qty_int,
        "path": path,
        "params": params,
        "available": available,
        "in_stock": available,
    }

# ---------- YML ----------
def build_yml(categories: List[Tuple[int,str,Optional[int]]],
              offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    enc_label = "utf-8" if ENCODING.startswith("utf") else "windows-1251"
    out: List[str] = []
    out.append(f"<?xml version='1.0' encoding='{enc_label}'?>")
    out.append(f"<yml_catalog date=\"{datetime.now().strftime('%Y-%m-%d %H:%M')}\">")
    out.append("<shop>")
    out.append("<name>nvprint</name>")
    out.append('<currencies><currency id="KZT" rate="1" /></currencies>')
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{x(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        parent = parent if parent else ROOT_CAT_ID
        out.append(f"<category id=\"{cid}\" parentId=\"{parent}\">{x(name)}</category>")
    out.append("</categories>")
    out.append("<offers>")
    for cid, it in offers:
        attrs = f' available="{"true" if it.get("available") else "false"}" in_stock="{"true" if it.get("in_stock") else "false"}"'
        out.append(f"<offer id=\"{x(it['id'])}\" {attrs}>")
        out.append(f"<name>{x(it['name'])}</name>")
        out.append(f"<vendor>{x(it.get('vendor') or 'NV Print')}</vendor>")
        if it.get("vendorCode"):
            out.append(f"<vendorCode>{x(it['vendorCode'])}</vendorCode>")
        out.append(f"<price>{int(round(float(it['price'])))}</price>")
        out.append("<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        if it.get("url"):
            out.append(f"<url>{x(it['url'])}</url>")
        for u in (it.get("pictures") or [])[:MAX_PICTURES]:
            out.append(f"<picture>{x(u)}</picture>")
        if it.get("description"):
            out.append(f"<description>{x(it['description'])}</description>")
        qty = int(it.get("qty") or 0)
        out.append(f"<quantity_in_stock>{qty}</quantity_in_stock>")
        out.append(f"<stock_quantity>{qty}</stock_quantity>")
        out.append(f"<quantity>{qty if qty>0 else 1}</quantity>")
        for k, v in (it.get("params") or {}).items():
            out.append(f"<param name=\"{x(k)}\">{x(v)}</param>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------- main ----------
def main() -> int:
    # 1) XML -> товары
    xml_bytes = fetch_xml_bytes(XML_URL)
    root = ET.fromstring(xml_bytes)
    items = guess_items(root)
    print(f"[nvprint] xml items: {len(items)}")

    parsed: List[Dict[str,Any]] = []
    for el in items:
        it = parse_xml_item(el)
        if it:
            parsed.append(it)

    # 2) первичные офферы и пути
    offers: List[Tuple[int, Dict[str,Any]]] = []
    paths: List[List[str]] = []
    for i, it in enumerate(parsed):
        offer_id_src = it.get("vendorCode") or it.get("name") or f"nv-{i+1}"
        oid = re.sub(r"[^\w\-]+", "-", offer_id_src).strip("-") or f"nv-{i+1}"
        paths.append(it.get("path") or [])
        offers.append((ROOT_CAT_ID, {
            "id": oid, "name": it["name"], "vendor": it.get("vendor") or "NV Print",
            "vendorCode": it.get("vendorCode") or "", "price": it["price"],
            "url": it.get("url") or "", "pictures": it.get("pictures") or [],
            "description": it.get("description") or "", "qty": int(it.get("qty") or 0),
            "available": it.get("available", False), "in_stock": it.get("in_stock", False),
            "params": it.get("params") or {},
        }))

    # 3) дерево категорий из путей
    cat_map: Dict[Tuple[str,...], int] = {}
    categories: List[Tuple[int,str,Optional[int]]] = []
    for path in paths:
        clean = [p for p in (path or []) if isinstance(p, str) and p.strip()]
        if not clean:
            continue
        parent = ROOT_CAT_ID; acc: List[str] = []
        for name in clean:
            acc.append(name.strip()); key = tuple(acc)
            if key in cat_map:
                parent = cat_map[key]
                continue
            cid = stable_cat_id(" / ".join(acc))
            cat_map[key] = cid
            categories.append((cid, name.strip(), parent))
            parent = cid

    def path_to_id(path: List[str]) -> int:
        key = tuple([p.strip() for p in (path or []) if p and p.strip()])
        return cat_map.get(key, ROOT_CAT_ID)

    offers = [(path_to_id(paths[i] if i < len(paths) else []), it) for i, (_, it) in enumerate(offers)]

    # 4) (опц.) обогащение сайтом — по умолчанию выключено
    if ENRICH_SITE and ENRICH_LIMIT != 0:
        print("[nvprint.ru] enrichment is enabled, but для getallinfo=true обычно не требуется.")

    # 5) запись YML
    xml = build_yml(categories, offers)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding=("utf-8" if ENCODING.startswith("utf") else "cp1251"), errors="ignore") as f:
        f.write(xml)

    print(f"[nvprint] done: {len(offers)} offers, {len(categories)} categories -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e, file=sys.stderr)
        try:
            os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
            with open(OUT_FILE, "w", encoding=("utf-8" if ENCODING.startswith("utf") else "cp1251"), errors="ignore") as f:
                f.write("<?xml version='1.0' encoding='utf-8'?>\n<yml_catalog><shop><name>nvprint</name><currencies><currency id=\"KZT\" rate=\"1\" /></currencies><categories><category id=\"9400000\">NVPrint</category></categories><offers></offers></shop></yml_catalog>")
        except Exception:
            pass
        sys.exit(0)
