# -*- coding: utf-8 -*-
"""
NVPrint (XML Basic Auth) → YML (Satu-совместимый)
- URL: NVPRINT_XML_URL (пример: https://api.nvprint.ru/api/hs/getprice/.../?format=xml)
- Basic Auth: NVPRINT_LOGIN / NVPRINT_PASSWORD (есть обратная совместимость с NVPRINT_XML_USER/PASS)
- Без пагинации. Цена берем KZT, если есть явное поле; иначе любой <price> как KZT.
- Остатки: quantity/stock/остаток → available/in_stock и *_quantity.
- Категории: category/subcategory → дерево под корнем "NVPrint".
"""

from __future__ import annotations
import os, re, sys, html, hashlib
from typing import Any, Dict, List, Optional, Tuple
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

# -------- ENV --------
XML_URL      = os.getenv("NVPRINT_XML_URL", "").strip()
# поддерживаем оба варианта имен:
NV_LOGIN     = (os.getenv("NVPRINT_LOGIN") or os.getenv("NVPRINT_XML_USER") or "").strip()
NV_PASSWORD  = (os.getenv("NVPRINT_PASSWORD") or os.getenv("NVPRINT_XML_PASS") or "").strip()

OUT_FILE     = os.getenv("OUT_FILE", "docs/nvprint.yml")
ENCODING     = (os.getenv("OUTPUT_ENCODING") or "utf-8").lower()
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))
MAX_PICTURES = int(os.getenv("MAX_PICTURES", "10"))

ROOT_CAT_ID   = 9400000
ROOT_CAT_NAME = "NVPrint"

UA = {"User-Agent": "Mozilla/5.0 (compatible; NVPrint-XML-Feed/1.1)"}

# -------- helpers --------
def x(s: str) -> str:
    return html.escape((s or "").strip())

def stable_cat_id(text: str, prefix: int = 9420000) -> int:
    h = hashlib.md5((text or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    return prefix + int(h, 16)

def fetch_xml_bytes(url: str) -> bytes:
    if not url:
        raise RuntimeError("NVPRINT_XML_URL пуст. Укажи ссылку на XML.")
    auth = (NV_LOGIN, NV_PASSWORD) if (NV_LOGIN or NV_PASSWORD) else None
    r = requests.get(url, auth=auth, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.content

def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def first_text(item: ET.Element, name_candidates: List[str]) -> Optional[str]:
    want = {n.lower() for n in name_candidates}
    for ch in list(item):
        nm = strip_ns(ch.tag).lower()
        if nm in want:
            txt = (ch.text or "").strip()
            if txt:
                return txt
    return None

def all_texts_like(item: ET.Element, substr_candidates: List[str]) -> List[str]:
    out = []
    subs = [s.lower() for s in substr_candidates]
    for ch in list(item):
        nm = strip_ns(ch.tag).lower()
        if any(s in nm for s in subs):
            val = (ch.text or "").strip()
            if val:
                out.append(val)
    return out

def parse_number(s: Optional[str]) -> Optional[float]:
    if not s: return None
    t = s.replace("\xa0"," ").replace(" ","").replace(",",".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m: return None
    try: return float(m.group(0))
    except: return None

def guess_items(root: ET.Element) -> List[ET.Element]:
    cands = root.findall(".//item") + root.findall(".//row") + root.findall(".//product")
    if cands: return cands
    out = []
    for node in root.iter():
        ch = list(node)
        if not ch: continue
        has_name = any(strip_ns(c.tag).lower() in {"name","наименование","full_name","fullname","title"} for c in ch)
        has_price = any("price" in strip_ns(c.tag).lower() or "цена" in strip_ns(c.tag).lower() for c in ch)
        if has_name and has_price:
            out.append(node)
    return out

# -------- mapping --------
NAME_TAGS       = ["full_name","fullname","name","наименование","title"]
VENDOR_TAGS     = ["brand","бренд","вендор","producer","manufacturer","производитель"]
SKU_TAGS        = ["articul","артикул","sku","code","код","vendorcode","кодтовара"]
PRICE_KZT_TAGS  = ["price_kzt","ценатенге","цена_kzt","kzt","pricekzt","price_kz","price_kaz"]
PRICE_ANY_TAGS  = ["price","цена","amount","value"]
URL_TAGS        = ["url","link","ссылка"]
DESC_TAGS       = ["description","описание","descr","short_description"]
CAT_TAGS        = ["category","категория","group","группа","section","раздел"]
SUBCAT_TAGS     = ["subcategory","подкатегория","subgroup","subsection","подраздел"]
PIC_LIKE        = ["image","img","picture","photo","фото","imageurl","image_url","photourl"]
QTY_TAGS        = ["quantity","qty","остаток","stock","amount","наличие","на_складе","store_amount"]
BARCODE_TAGS    = ["barcode","ean","штрихкод","ean13","ean-13"]

def parse_item(item: ET.Element) -> Optional[Dict[str, Any]]:
    name = first_text(item, NAME_TAGS)
    if not name: return None

    vendor = first_text(item, VENDOR_TAGS) or "NV Print"
    vendor_code = first_text(item, SKU_TAGS)

    price = None
    for t in PRICE_KZT_TAGS:
        v = first_text(item, [t])
        price = parse_number(v)
        if price is not None: break
    if price is None:
        for t in PRICE_ANY_TAGS:
            v = first_text(item, [t])
            price = parse_number(v)
            if price is not None: break
    if price is None or price <= 0: return None

    url = first_text(item, URL_TAGS) or ""

    pics = all_texts_like(item, PIC_LIKE)
    pics = [p for p in pics if re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", p, re.I)]
    pics = list(dict.fromkeys(pics))

    desc = first_text(item, DESC_TAGS)
    if not desc:
        bits = [name]
        if vendor_code: bits.append(f"Артикул: {vendor_code}")
        bc = first_text(item, BARCODE_TAGS)
        if bc: bits.append(f"Штрихкод: {bc}")
        desc = "; ".join(bits)

    qty = 0.0
    for t in QTY_TAGS:
        v = first_text(item, [t])
        n = parse_number(v)
        if n is not None:
            qty = max(qty, n)
    available = qty > 0
    in_stock = available
    qty_int = int(round(qty)) if qty and qty > 0 else 0

    cat  = first_text(item, CAT_TAGS) or ""
    scat = first_text(item, SUBCAT_TAGS) or ""
    path = [p for p in [cat, scat] if p]

    # собрать остальные простые поля в <param>
    mapped = set([*NAME_TAGS, *VENDOR_TAGS, *SKU_TAGS, *PRICE_KZT_TAGS, *PRICE_ANY_TAGS,
                  *URL_TAGS, *DESC_TAGS, *CAT_TAGS, *SUBCAT_TAGS, *BARCODE_TAGS])
    params: Dict[str,str] = {}
    for ch in list(item):
        key = strip_ns(ch.tag)
        lkey = key.lower()
        if lkey in mapped: continue
        txt = (ch.text or "").strip()
        if not txt or len(txt) > 500: continue
        if list(ch): continue
        params[key] = txt

    return {
        "name": name,
        "vendor": vendor,
        "vendorCode": vendor_code or "",
        "price": price,
        "url": url,
        "pictures": pics[:MAX_PICTURES],
        "description": desc,
        "qty": qty_int,
        "path": path,
        "params": params,
    }

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
        if it.get("vendorCode"): out.append(f"<vendorCode>{x(it['vendorCode'])}</vendorCode>")
        out.append(f"<price>{int(round(float(it['price'])))}</price>")
        out.append("<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        if it.get("url"): out.append(f"<url>{x(it['url'])}</url>")
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

def main() -> int:
    # 1) тянем XML
    xml_bytes = fetch_xml_bytes(XML_URL)
    # 2) парсим
    root = ET.fromstring(xml_bytes)
    # 3) находим товарные элементы
    items = guess_items(root)
    if not items:
        print("WARN: не нашли товарных элементов в XML (проверь структуру/логин/пароль)", file=sys.stderr)

    # 4) собираем офферы
    parsed: List[Dict[str,Any]] = []
    for el in items:
        data = parse_item(el)
        if data: parsed.append(data)

    offers: List[Tuple[int, Dict[str,Any]]] = []
    paths: List[List[str]] = []
    for i, it in enumerate(parsed):
        offer_id_src = it.get("vendorCode") or it.get("url") or it.get("name") or f"nv-{i+1}"
        oid = re.sub(r"[^\w\-]+", "-", offer_id_src).strip("-") or f"nv-{i+1}"
        available = (it.get("qty", 0) or 0) > 0
        paths.append(it.get("path") or [])
        offers.append((ROOT_CAT_ID, {
            "id": oid,
            "name": it["name"],
            "vendor": it.get("vendor") or "NV Print",
            "vendorCode": it.get("vendorCode") or "",
            "price": it["price"],
            "url": it.get("url") or "",
            "pictures": it.get("pictures") or [],
            "description": it.get("description") or "",
            "qty": int(it.get("qty") or 0),
            "available": available,
            "in_stock": available,
            "params": it.get("params") or {},
        }))

    # 5) дерево категорий
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

    offers_final: List[Tuple[int, Dict[str,Any]]] = []
    for i, (_, it) in enumerate(offers):
        offers_final.append((path_to_id(paths[i] if i < len(paths) else []), it))

    # 6) запись
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(categories, offers_final)
    with open(OUT_FILE, "w", encoding=("utf-8" if ENCODING.startswith("utf") else "cp1251"), errors="ignore") as f:
        f.write(xml)

    print(f"[nvprint-xml] done: {len(offers_final)} offers, {len(categories)} categories -> {OUT_FILE} (encoding={ENCODING})")
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
