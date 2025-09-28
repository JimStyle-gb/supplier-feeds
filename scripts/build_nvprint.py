# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint: XML API (getallinfo=true) -> YML (KZT)

Фикс цены по ТЗ:
1) <Договор НомерДоговора="ТА-000079">  -> <Цена>
2) иначе <Договор НомерДоговора="TA-000079Мск"> -> <Цена>
3) иначе 100

Остальная логика не менялась.
"""

from __future__ import annotations
import os, re, sys, html, hashlib, csv
from typing import Any, Dict, List, Optional, Tuple
import requests, xml.etree.ElementTree as ET
from datetime import datetime

# ---------- ENV ----------
XML_URL      = os.getenv("NVPRINT_XML_URL", "").strip()
NV_LOGIN     = (os.getenv("NVPRINT_LOGIN") or os.getenv("NVPRINT_XML_USER") or "").strip()
NV_PASSWORD  = (os.getenv("NVPRINT_PASSWORD") or os.getenv("NVPRINT_XML_PASS") or "").strip()

OUT_FILE     = os.getenv("OUT_FILE", "docs/nvprint.yml")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))
MAX_PICTURES = int(os.getenv("MAX_PICTURES", "10"))
DEBUG_AVAIL  = os.getenv("NVPRINT_DEBUG_AVAIL", "0") == "1"
FORCE_AVAIL  = os.getenv("NVPRINT_FORCE_AVAILABLE", "0") == "1"

# --- фильтр по ключевым словам (как было) ---
KEYWORDS_FILE = os.getenv("NVPRINT_KEYWORDS_FILE", "docs/nvprint_keywords.txt")
KEYWORDS_MODE = (os.getenv("NVPRINT_KEYWORDS_MODE", "prefix") or "prefix").lower()  # 'prefix' | 'any'

# mapping (как было)
MAP_FILE       = os.getenv("NVPRINT_MAP_FILE", "docs/nvprint_map.csv")
MAP_DELIM      = os.getenv("NVPRINT_MAP_DELIM", ",")
MAP_SUPPL_COL  = int(os.getenv("NVPRINT_MAP_SUPPLIER_COL", "0"))
MAP_OUR_COL    = int(os.getenv("NVPRINT_MAP_OUR_COL", "1"))
REQUIRE_MAP    = os.getenv("NVPRINT_REQUIRE_MAP", "0") == "1"
OUR_SKU_PREFIX = os.getenv("NVPRINT_OUR_SKU_PREFIX", "")
SUPPL_PARAM    = os.getenv("NVPRINT_PARAM_SUPPLIER_CODE", "SupplierCode")

# кастом-теги (как было)
ITEM_XPATH   = (os.getenv("NVPRINT_ITEM_XPATH") or "").strip()
NAME_OVR     = os.getenv("NVPRINT_NAME_TAGS")
PRICEKZT_OVR = os.getenv("NVPRINT_PRICE_KZT_TAGS")
PRICEANY_OVR = os.getenv("NVPRINT_PRICE_TAGS")
SKU_OVR      = os.getenv("NVPRINT_SKU_TAGS")
VENDOR_OVR   = os.getenv("NVPRINT_VENDOR_TAGS")
DESC_OVR     = os.getenv("NVPRINT_DESC_TAGS")
URL_OVR      = os.getenv("NVPRINT_URL_TAGS")
CAT_OVR      = os.getenv("NVPRINT_CAT_TAGS")
SUBCAT_OVR   = os.getenv("NVPRINT_SUBCAT_TAGS")
PIC_OVR      = os.getenv("NVPRINT_PIC_TAGS")
PICS_OVR     = os.getenv("NVPRINT_PICS_TAGS")
BARCODE_OVR  = os.getenv("NVPRINT_BARCODE_TAGS")
CATPATH_OVR  = os.getenv("NVPRINT_CAT_PATH_TAGS")
PARAMS_BLOCK_OVR = os.getenv("NVPRINT_PARAMS_BLOCK_TAGS")
PARAM_NAME_OVR   = os.getenv("NVPRINT_PARAM_NAME_TAGS")
PARAM_VALUE_OVR  = os.getenv("NVPRINT_PARAM_VALUE_TAGS")

ROOT_CAT_ID   = 9400000
ROOT_CAT_NAME = "NVPrint"
UA = {"User-Agent": "Mozilla/5.0 (compatible; NVPrint-XML-Feed/3.2)"}

def x(s: str) -> str: return html.escape((s or "").strip())
def stable_cat_id(text: str, prefix: int = 9420000) -> int:
    h = hashlib.md5((text or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    return prefix + int(h, 16)

# ---------- HTTP ----------
def fetch_xml_bytes(url: str) -> bytes:
    if not url:
        raise RuntimeError("NVPRINT_XML_URL пуст.")
    auth = (NV_LOGIN, NV_PASSWORD) if (NV_LOGIN or NV_PASSWORD) else None
    r = requests.get(url, auth=auth, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    b = r.content
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
    out_nodes: List[ET.Element] = []
    for ch in item.iter():
        nm = strip_ns(ch.tag).lower()
        if any(s in nm for s in subs):
            out_nodes.append(ch)
    texts: List[str] = []
    for node in out_nodes:
        if node.text:
            t = node.text.strip()
            if t:
                texts.append(t)
    return texts

# ---------- items guess ----------
def guess_items(root: ET.Element) -> List[ET.Element]:
    if ITEM_XPATH:
        items = root.findall(ITEM_XPATH)
        if items: return items
    cands = root.findall(".//Товар") + root.findall(".//item") + root.findall(".//product") + root.findall(".//row")
    if cands: return cands
    NAME_DEF = split_tags(NAME_OVR, ["НоменклатураКратко","Номенклатура","full_name","name","title","наименование"])
    SKU_DEF  = split_tags(SKU_OVR,  ["Артикул","articul","sku","vendorcode","кодтовара","code","код"])
    out: List[ET.Element] = []
    for node in root.iter():
        has_name = first_desc_text(node, NAME_DEF)
        has_sku  = first_desc_text(node, SKU_DEF)
        if has_name or has_sku:
            out.append(node)
    return out

# ---------- tag sets (как было) ----------
NAME_TAGS       = split_tags(NAME_OVR,      ["НоменклатураКратко","Номенклатура","full_name","name","title","наименование"])
VENDOR_TAGS     = split_tags(VENDOR_OVR,    ["brand","бренд","вендор","producer","manufacturer","производитель"])
SKU_TAGS        = split_tags(SKU_OVR,       ["Артикул","articul","sku","vendorcode","кодтовара","code","код"])
PRICE_KZT_TAGS  = split_tags(PRICEKZT_OVR,  ["ЦенаТенге","price_kzt","ценатенге","цена_kzt","kzt"])
PRICE_ANY_TAGS  = split_tags(PRICEANY_OVR,  ["Цена","price","amount","value","цена"])
URL_TAGS        = split_tags(URL_OVR,       ["url","link","ссылка"])
DESC_TAGS       = split_tags(DESC_OVR,      ["Описание","ПолноеОписание","Description","FullDescription","descr","short_description"])
CAT_TAGS        = split_tags(CAT_OVR,       ["РазделПрайса","category","категория","group","раздел"])
SUBCAT_TAGS     = split_tags(SUBCAT_OVR,    ["subcategory","подкатегория","subgroup","подраздел"])

# ключи для распознавания количества/наличия (как было)
QTY_KEYS   = ["колич", "кол-во", "к-во", "налич", "остат", "qty", "quantity", "stock", "free", "balance", "count", "amount"]
AVAIL_KEYS = ["налич", "avail", "stock", "status", "доступ", "статус"]

# картинки (как было)
PIC_SINGLE_TAGS  = split_tags(PIC_OVR,      ["Image","ImageURL","Photo","Picture","Картинка","Изображение"])
PIC_LIKE         = ["image","img","photo","picture","картин","изобр","фото"]
PICS_LIST_TAGS   = split_tags(PICS_OVR,     ["Images","Pictures","Photos","Галерея","Картинки","Изображения"])

BARCODE_TAGS    = split_tags(BARCODE_OVR,   ["Штрихкод","barcode","ean","ean13"])
CATPATH_TAGS    = split_tags(CATPATH_OVR,   ["category_path","full_path","path","путь"])

PARAMS_BLOCK_TAGS = split_tags(PARAMS_BLOCK_OVR, ["Характеристики","Specs","Attributes","Параметры","ПараметрыТовара"])
PARAM_NAME_TAGS   = split_tags(PARAM_NAME_OVR,   ["Имя","Name","Параметр","Показатель","Характеристика"])
PARAM_VALUE_TAGS  = split_tags(PARAM_VALUE_OVR,  ["Значение","Value","Величина","ПараметрЗначение"])

IMG_RE = re.compile(r"https?://[^\s'\"<>]+?\.(?:jpg|jpeg|png|webp|gif)(?:\?[^\s'\"<>]*)?$", re.I)
SEP_RE = re.compile(r"\s*(?:>|/|\\|\||→|»|›|—|-)\s*")

POS_WORDS = ["есть","в наличии","вналичии","true","yes","да","available","instock","in stock","много","на складе","есть на складе","доступно","готов к отгрузке","положительный"]
NEG_WORDS = ["нет","отсутств","false","no","нет в наличии","нет на складе","под заказ","preorder","ожидается","ожид","out of stock","законч","0 шт","0шт","отсутствует","недоступно"]

def parse_availability_text(s: Optional[str]) -> Optional[bool]:
    if not s: return None
    t = re.sub(r"\s+", " ", s.strip().lower())
    for w in POS_WORDS:
        if w in t: return True
    for w in NEG_WORDS:
        if w in t: return False
    return None

def nalichie_attr_qty(item: ET.Element) -> Tuple[Optional[float], bool]:
    best_qty: Optional[float] = None
    found_attr = False
    for ch in item.iter():
        nm = strip_ns(ch.tag).lower()
        if nm not in ("наличие","nalichie","availability"):
            continue
        for k, v in (ch.attrib or {}).items():
            kl = re.sub(r"[\s\-_]+", "", k.lower())
            if kl in ("количество","колво","кво","qty","quantity","count","amount") or "кол" in kl:
                found_attr = True
                val = (str(v) or "").strip()
                n = parse_number(val)
                if n is not None:
                    best_qty = max(best_qty or 0.0, n)
    return best_qty, found_attr

def fallback_qty_and_avail(item: ET.Element) -> Tuple[int, Optional[bool]]:
    qty = 0.0
    avail_flag: Optional[bool] = None
    qkeys = tuple(QTY_KEYS)
    akeys = tuple(AVAIL_KEYS)
    for ch in item.iter():
        nm = strip_ns(ch.tag).lower()
        if any(k in nm for k in qkeys):
            if ch.text:
                n = parse_number(ch.text)
                if n is not None: qty = max(qty, n)
            for v in (ch.attrib or {}).values():
                n = parse_number(str(v))
                if n is not None: qty = max(qty, n)
        if any(k in nm for k in akeys):
            flag = parse_availability_text(ch.text or "")
            if flag is True: avail_flag = True
            elif flag is False and avail_flag is None: avail_flag = False
            for v in (ch.attrib or {}).values():
                flag = parse_availability_text(str(v))
                if flag is True: avail_flag = True
                elif flag is False and avail_flag is None: avail_flag = False
    return (int(round(qty)) if qty and qty > 0 else 0), avail_flag

# ---------- keywords ----------
def load_keywords(path: str) -> List[str]:
    kws: List[str] = []
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
                for line in f:
                    t = (line or "").strip()
                    if t and not t.startswith("#"):
                        kws.append(t)
    except Exception:
        pass
    return kws

def _normalize_for_prefix(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("\xa0", " ")
    s = re.sub(r"[–—−―]", "-", s)
    s = re.sub(r"\s+", " ", s).strip(" .,_-")
    return s

def matches_keywords(it: Dict[str, Any], kws: List[str]) -> bool:
    if not kws:
        return True
    mode = KEYWORDS_MODE
    if mode == "any":
        parts: List[str] = []
        parts.append(str(it.get("name") or ""))
        parts.append(str(it.get("vendor") or ""))
        parts.append(str(it.get("description") or ""))
        for p in (it.get("path") or []):
            parts.append(str(p))
        hay = " ".join(parts).lower()
        kw_norms = [ _normalize_for_prefix(k) for k in kws ]
        return any(k in hay for k in kw_norms)
    else:
        name = _normalize_for_prefix(it.get("name") or "")
        if not name:
            return False
        kw_norms = [ _normalize_for_prefix(k) for k in kws ]
        return any(name.startswith(k) for k in kw_norms)

# ===================== НОВОЕ: ЦЕНА ИЗ <Договор> ПО ПРИОРИТЕТУ =====================
def _norm_contract(s: str) -> str:
    """
    Нормализуем номер договора: латинские аналоги, удаляем пробелы/дефисы/подчёркивания,
    приводим к upper. 'ТА-000079' -> 'TA000079', 'TA-000079Мск' -> 'TA000079МСК'
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
        # цена в этом договоре
        price_el = None
        for ch in node.iter():
            if strip_ns(ch.tag).lower() in ("цена","price","amount","value"):
                price_el = ch
                break
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
# ==================================================================================

# ---------- parse XML item ----------
def parse_xml_item(item: ET.Element) -> Optional[Dict[str, Any]]:
    NAME_TAGS = split_tags(NAME_OVR, ["НоменклатураКратко","Номенклатура","full_name","name","title","наименование"])
    VENDOR_TAGS = split_tags(VENDOR_OVR, ["brand","бренд","вендор","producer","manufacturer","производитель"])
    SKU_TAGS = split_tags(SKU_OVR, ["Артикул","articul","sku","vendorcode","кодтовара","code","код"])
    PRICE_KZT_TAGS = split_tags(PRICEKZT_OVR, ["ЦенаТенге","price_kzt","ценатенге","цена_kzt","kzt"])
    PRICE_ANY_TAGS = split_tags(PRICEANY_OVR, ["Цена","price","amount","value","цена"])
    URL_TAGS = split_tags(URL_OVR, ["url","link","ссылка"])

    name = first_desc_text(item, ["НоменклатураКратко"]) or first_desc_text(item, NAME_TAGS)
    if not name:
        return None

    supplier_code = first_desc_text(item, ["Артикул"]) or first_desc_text(item, SKU_TAGS) or ""
    vendor = first_desc_text(item, VENDOR_TAGS) or "NV Print"

    # -------- ИЗМЕНЕНО: цена из <Договор> (КЗ -> МСК -> 100) --------
    price = _extract_price_from_contracts(item)
    if price is None or price <= 0:
        price = 100.0
    # ---------------------------------------------------------------

    # наличие (как было)
    qty_from_nal, has_nalichie = nalichie_attr_qty(item)
    if has_nalichie:
        available = True
        qty_int = int(round(qty_from_nal)) if (qty_from_nal is not None and qty_from_nal > 0) else 1
    else:
        qty_int, avail_flag = fallback_qty_and_avail(item)
        available = (qty_int > 0) if (avail_flag is None) else bool(avail_flag)
        if available and qty_int == 0:
            qty_int = 1
    if FORCE_AVAIL:
        available = True
        if qty_int <= 0:
            qty_int = 1

    path = extract_category_path(item)
    desc = extract_description(item) or "; ".join([first_desc_text(item, ["Номенклатура"]) or name] + ([f"Артикул: {supplier_code}"] if supplier_code else []))
    pictures = extract_pictures(item)
    params = extract_params(item)
    url = first_desc_text(item, URL_TAGS) or ""

    return {
        "name": name,
        "vendor": vendor,
        "supplierCode": supplier_code,
        "vendorCode": supplier_code,     # как было
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

# ---------- YML (как было) ----------
def build_yml(categories: List[Tuple[int,str,Optional[int]]],
              offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='utf-8'?>")
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
        if it.get("vendorCode"): out.append(f"<vendorCode>{x(it['vendorCode'])}</vendorCode>")
        out.append(f"<price>{int(round(float(it['price'])))}</price>")
        out.append("<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        if it.get("url"): out.append(f"<url>{x(it['url'])}</url>")
        for u in (it.get("pictures") or [])[:MAX_PICTURES]:
            out.append(f"<picture>{x(u)}</picture>")
        if it.get("description"): out.append(f"<description>{x(it['description'])}</description>")
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

# ---------- mapping (как было) ----------
def load_code_map(path: str, delim: str, c_sup: int, c_our: int) -> Dict[str, str]:
    m: Dict[str, str] = {}
    if not path or not os.path.isfile(path):
        return m
    def norm(s: str) -> str: return (s or "").strip()
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            reader = csv.reader(f, delimiter=delim)
            for row in reader:
                if not row or len(row) <= max(c_sup, c_our): continue
                sup = norm(row[c_sup]); our = norm(row[c_our])
                if sup and our: m[sup] = our
        return m
    except Exception:
        pass
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        sniffer = csv.Sniffer()
        sample = f.read(4096); f.seek(0)
        try:
            dialect = sniffer.sniff(sample, delimiters=",;\t|")
        except Exception:
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        for row in reader:
            if not row or len(row) <= max(c_sup, c_our): continue
            sup = norm(row[c_sup]); our = norm(row[c_our])
            if sup and our: m[sup] = our
    return m

# ---------- категории/описания/картинки (как было) ----------
def extract_category_path(item: ET.Element) -> List[str]:
    for t in split_tags(CATPATH_OVR, ["category_path","full_path","path","путь"]):
        val = first_desc_text(item, [t])
        if val:
            parts = [p.strip() for p in SEP_RE.split(val) if p.strip()]
            if parts: return parts[:4]
    cat  = first_desc_text(item, split_tags(CAT_OVR, ["РазделПрайса","category","категория","group","раздел"])) or ""
    scat = first_desc_text(item, split_tags(SUBCAT_OVR, ["subcategory","подкатегория","subgroup","подраздел"])) or ""
    path = [p for p in [cat, scat] if p]
    if path: return path
    cand_texts = all_desc_texts_like(item, ["category","категор","group","раздел"])
    seen = set(); clean = []
    for v in cand_texts:
        vv = v.strip()
        if not vv or vv.lower() in seen: continue
        seen.add(vv.lower())
        if len(vv) < 2: continue
        clean.append(vv)
        if len(clean) >= 2: break
    return clean

def extract_pictures(item: ET.Element) -> List[str]:
    pics: List[str] = []
    for t in split_tags(PIC_OVR, ["Image","ImageURL","Photo","Picture","Картинка","Изображение"]):
        txt = first_desc_text(item, [t])
        if txt:
            for m in IMG_RE.findall(txt):
                pics.append(m)
    def walk_and_collect(el: ET.Element):
        nm = strip_ns(el.tag).lower()
        if any(k in nm for k in ["image","img","photo","picture","картин","изобр","фото"]):
            if el.text:
                for m in IMG_RE.findall(el.text.strip()):
                    pics.append(m)
        for _, v in (el.attrib or {}).items():
            for m in IMG_RE.findall(str(v)):
                pics.append(m)
        for ch in el:
            walk_and_collect(ch)
    for node in item:
        nn = strip_ns(node.tag).lower()
        if nn in [n.lower() for n in split_tags(PICS_OVR, ["Images","Pictures","Photos","Галерея","Картинки","Изображения"])] or any(k in nn for k in ["image","img","photo","picture","картин","изобр","фото"]):
            walk_and_collect(node)
    for ch in item.iter():
        if ch.text:
            for m in IMG_RE.findall(ch.text.strip()):
                pics.append(m)
    uniq = []
    seen = set()
    for u in pics:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq[:MAX_PICTURES]

def extract_description(item: ET.Element) -> Optional[str]:
    txt = first_desc_text(item, split_tags(DESC_OVR, ["Описание","ПолноеОписание","Description","FullDescription","descr","short_description"]))
    if txt and len(txt.strip()) >= 10:
        return txt.strip()
    return None

def extract_params(item: ET.Element) -> Dict[str, str]:
    params: Dict[str, str] = {}
    blocks: List[ET.Element] = []
    for node in item.iter():
        nm = strip_ns(node.tag).lower()
        if nm in [b.lower() for b in split_tags(PARAMS_BLOCK_OVR, ["Характеристики","Specs","Attributes","Параметры","ПараметрыТовара"])] or "характер" in nm or "spec" in nm or "attrib" in nm:
            blocks.append(node)
    def add_pair(k: str, v: str):
        k = (k or "").strip(": ")
        v = (v or "").strip()
        if k and v and k not in params:
            params[k] = v
    for b in blocks:
        names: List[str]  = []
        values: List[str] = []
        for ch in b.iter():
            nm = strip_ns(ch.tag).lower()
            if nm in [p.lower() for p in split_tags(PARAM_NAME_OVR, ["Имя","Name","Параметр","Показатель","Характеристика"])]:
                if ch.text: names.append(ch.text.strip())
            if nm in [p.lower() for p in split_tags(PARAM_VALUE_OVR, ["Значение","Value","Величина","ПараметрЗначение"])]:
                if ch.text: values.append(ch.text.strip())
        for k, v in zip(names, values):
            add_pair(k, v)
        for ch in b.iter():
            if ch.text and ":" in ch.text and len(ch.text) < 200:
                k, v = ch.text.split(":", 1)
                add_pair(k, v)
    return params

# ---------- main ----------
def load_code_map(path: str, delim: str, c_sup: int, c_our: int) -> Dict[str, str]:
    m: Dict[str, str] = {}
    if not path or not os.path.isfile(path):
        return m
    def norm(s: str) -> str: return (s or "").strip()
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            reader = csv.reader(f, delimiter=delim)
            for row in reader:
                if not row or len(row) <= max(c_sup, c_our): continue
                sup = norm(row[c_sup]); our = norm(row[c_our])
                if sup and our: m[sup] = our
        return m
    except Exception:
        pass
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
        sniffer = csv.Sniffer()
        sample = f.read(4096); f.seek(0)
        try:
            dialect = sniffer.sniff(sample, delimiters=",;\t|")
        except Exception:
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        for row in reader:
            if not row or len(row) <= max(c_sup, c_our): continue
            sup = norm(row[c_sup]); our = norm(row[c_our])
            if sup and our: m[sup] = our
    return m

def build_yml(categories: List[Tuple[int,str,Optional[int]]],
              offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    # оставлено без изменений (см. выше)
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='utf-8'?>")
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
        if it.get("vendorCode"): out.append(f"<vendorCode>{x(it['vendorCode'])}</vendorCode>")
        out.append(f"<price>{int(round(float(it['price'])))}</price>")
        out.append("<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        if it.get("url"): out.append(f"<url>{x(it['url'])}</url>")
        for u in (it.get("pictures") or [])[:MAX_PICTURES]:
            out.append(f"<picture>{x(u)}</picture>")
        if it.get("description"): out.append(f"<description>{x(it['description'])}</description>")
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

def main() -> int:
    # 0) ключевые слова (как было)
    keywords = load_keywords(KEYWORDS_FILE)

    # 1) маппинг (как было)
    code_map = load_code_map(MAP_FILE, MAP_DELIM, MAP_SUPPL_COL, MAP_OUR_COL)

    # 2) XML -> товары
    xml_bytes = fetch_xml_bytes(XML_URL)
    root = ET.fromstring(xml_bytes)
    items = guess_items(root)
    print(f"[nvprint] xml items: {len(items)}")

    parsed: List[Dict[str,Any]] = []
    for el in items:
        it = parse_xml_item(el)
        if not it:
            continue

        # фильтр: имя товара должно начинаться с любого ключевого слова (если заданы)
        if keywords and not matches_keywords(it, keywords):
            continue

        # маппинг кода (как было)
        supplier_code = (it.get("supplierCode") or "").strip()
        our_code = code_map.get(supplier_code, "").strip() if supplier_code else ""
        if our_code:
            our_full = f"{OUR_SKU_PREFIX}{our_code}"
            it["params"] = it.get("params") or {}
            it["params"][SUPPL_PARAM] = supplier_code
            it["vendorCode"] = our_full
        else:
            if REQUIRE_MAP:
                continue

        parsed.append(it)

    # 3) офферы/пути (как было)
    offers: List[Tuple[int, Dict[str,Any]]] = []
    paths: List[List[str]] = []
    for i, it in enumerate(parsed):
        id_src = it.get("vendorCode") or it.get("supplierCode") or it.get("name") or f"nv-{i+1}"
        oid = re.sub(r"[^\w\-]+", "-", id_src).strip("-") or f"nv-{i+1}"
        paths.append(it.get("path") or [])
        offers.append((ROOT_CAT_ID, {
            "id": oid, "name": it["name"], "vendor": it.get("vendor") or "NV Print",
            "vendorCode": it.get("vendorCode") or "", "price": it["price"],
            "url": it.get("url") or "", "pictures": it.get("pictures") or [],
            "description": it.get("description") or "", "qty": int(it.get("qty") or 0),
            "available": it.get("available", False), "in_stock": it.get("in_stock", False),
            "params": it.get("params") or {},
        }))

    # 4) категории (как было)
    cat_map: Dict[Tuple[str,...], int] = {}
    categories: List[Tuple[int,str,Optional[int]]] = []
    for path in paths:
        clean = [p for p in (path or []) if isinstance(p, str) and p.strip()]
        if not clean: continue
        parent = ROOT_CAT_ID; acc: List[str] = []
        for name in clean:
            acc.append(name.strip()); key = tuple(acc)
            if key in cat_map:
                parent = cat_map[key]; continue
            cid = stable_cat_id(" / ".join(acc))
            cat_map[key] = cid
            categories.append((cid, name.strip(), parent))
            parent = cid

    def path_to_id(path: List[str]) -> int:
        key = tuple([p.strip() for p in (path or []) if p and p.strip()])
        return cat_map.get(key, ROOT_CAT_ID)

    offers = [(path_to_id(paths[i] if i < len(paths) else []), it) for i, (_, it) in enumerate(offers)]

    # 5) запись (как было)
    xml = build_yml(categories, offers)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8-sig", errors="ignore") as f:
        f.write(xml)

    if DEBUG_AVAIL:
        total = len(offers)
        av_cnt = sum(1 for _, it in offers if it.get("available"))
        print(f"[nvprint] available TRUE in yml: {av_cnt}/{total}")

    print(f"[nvprint] done: {len(offers)} offers, {len(categories)} categories -> {OUT_FILE}")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e, file=sys.stderr)
        try:
            os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
            with open(OUT_FILE, "w", encoding="utf-8-sig", errors="ignore") as f:
                f.write("<?xml version='1.0' encoding='utf-8'?>\n"
                        "<yml_catalog><shop><name>nvprint</name>"
                        "<currencies><currency id=\"KZT\" rate=\"1\" /></currencies>"
                        "<categories><category id=\"9400000\">NVPrint</category></categories>"
                        "<offers></offers></shop></yml_catalog>")
        except Exception:
            pass
        sys.exit(0)
