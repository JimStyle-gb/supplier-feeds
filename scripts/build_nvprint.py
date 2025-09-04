# -*- coding: utf-8 -*-
"""
NVPrint (XML Basic Auth) → YML (Satu-совместимый)
- URL: NVPRINT_XML_URL
- Basic Auth: NVPRINT_LOGIN / NVPRINT_PASSWORD  (поддерживаются NVPRINT_XML_USER/PASS)
- Сохраняет сырой ответ в docs/nvprint_source.xml.
- Ищет товары на любой вложенности; можно задать XPath через NVPRINT_ITEM_XPATH (например: ".//Product").
- КАТЕГОРИИ:
    1) Если есть поле-путь (CategoryPath / full_path / КатегорияПуть) — разбираем разделителями ">", "/", "|", "→", "-".
    2) Если есть парные поля Category/Subcategory — берём их.
    3) Если ничего не найдено — эвристика: берём первые 1–2 поля, где имя тега содержит "category|категор|group|группа|section|раздел".
- Имена полей можно задать через ENV (см. переменные ниже).
"""

from __future__ import annotations
import os, re, sys, html, hashlib
from typing import Any, Dict, List, Optional, Tuple
import requests, xml.etree.ElementTree as ET
from datetime import datetime

# -------- ENV --------
XML_URL      = os.getenv("NVPRINT_XML_URL", "").strip()
NV_LOGIN     = (os.getenv("NVPRINT_LOGIN") or os.getenv("NVPRINT_XML_USER") or "").strip()
NV_PASSWORD  = (os.getenv("NVPRINT_PASSWORD") or os.getenv("NVPRINT_XML_PASS") or "").strip()

OUT_FILE     = os.getenv("OUT_FILE", "docs/nvprint.yml")
ENCODING     = (os.getenv("OUTPUT_ENCODING") or "utf-8").lower()
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))
MAX_PICTURES = int(os.getenv("MAX_PICTURES", "10"))

# КАСТОМНЫЕ ПЕРЕОПРЕДЕЛЕНИЯ (через запятую)
ITEM_XPATH   = (os.getenv("NVPRINT_ITEM_XPATH") or "").strip()  # пример: ".//Product"
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
PIC_OVR      = os.getenv("NVPRINT_PIC_TAGS")
BARCODE_OVR  = os.getenv("NVPRINT_BARCODE_TAGS")
CATPATH_OVR  = os.getenv("NVPRINT_CAT_PATH_TAGS")  # поле, где уже хранится полный путь категории

ROOT_CAT_ID   = 9400000
ROOT_CAT_NAME = "NVPrint"

UA = {"User-Agent": "Mozilla/5.0 (compatible; NVPrint-XML-Feed/1.4)"}

# -------- helpers --------
def x(s: str) -> str: return html.escape((s or "").strip())
def stable_cat_id(text: str, prefix: int = 9420000) -> int:
    h = hashlib.md5((text or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    return prefix + int(h, 16)

def fetch_xml_bytes(url: str) -> bytes:
    if not url: raise RuntimeError("NVPRINT_XML_URL пуст.")
    auth = (NV_LOGIN, NV_PASSWORD) if (NV_LOGIN or NV_PASSWORD) else None
    r = requests.get(url, auth=auth, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.content

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

def guess_items(root: ET.Element) -> List[ET.Element]:
    if ITEM_XPATH:
        items = root.findall(ITEM_XPATH)
        if items: return items
    cands = root.findall(".//item") + root.findall(".//row") + root.findall(".//product")
    if cands: return cands
    NAME_TAGS = split_tags(NAME_OVR, ["full_name","fullname","name","наименование","title"])
    PRICE_ANY_TAGS = split_tags(PRICEANY_OVR, ["price","цена","amount","value"])
    out: List[ET.Element] = []
    for node in root.iter():
        if first_desc_text(node, NAME_TAGS) and first_desc_text(node, PRICE_ANY_TAGS):
            out.append(node)
    return out

# -------- default tag sets (перекрываются ENV) --------
NAME_TAGS       = split_tags(NAME_OVR,      ["full_name","fullname","name","наименование","title"])
VENDOR_TAGS     = split_tags(VENDOR_OVR,    ["brand","бренд","вендор","producer","manufacturer","производитель"])
SKU_TAGS        = split_tags(SKU_OVR,       ["articul","артикул","sku","code","код","vendorcode","кодтовара"])
PRICE_KZT_TAGS  = split_tags(PRICEKZT_OVR,  ["price_kzt","ценатенге","цена_kzt","kzt","pricekzt","price_kz","price_kaz"])
PRICE_ANY_TAGS  = split_tags(PRICEANY_OVR,  ["price","цена","amount","value"])
URL_TAGS        = split_tags(URL_OVR,       ["url","link","ссылка"])
DESC_TAGS       = split_tags(DESC_OVR,      ["description","описание","descr","short_description"])
CAT_TAGS        = split_tags(CAT_OVR,       ["category","категория","group","группа","section","раздел"])
SUBCAT_TAGS     = split_tags(SUBCAT_OVR,    ["subcategory","подкатегория","subgroup","subsection","подраздел"])
PIC_LIKE        = split_tags(PIC_OVR,       ["image","img","picture","photo","фото","imageurl","image_url","photourl"])
QTY_TAGS        = split_tags(QTY_OVR,       ["quantity","qty","остаток","stock","amount","наличие","на_складе","store_amount"])
BARCODE_TAGS    = split_tags(BARCODE_OVR,   ["barcode","ean","штрихкод","ean13","ean-13"])
CATPATH_TAGS    = split_tags(CATPATH_OVR,   ["category_path","full_path","path","категорияпуть","путь","раздел_путь"])

# -------- categories extraction --------
SEP_RE = re.compile(r"\s*(?:>|/|\\|\||→|»|›|—|-)\s*")

def extract_category_path(item: ET.Element) -> List[str]:
    # 1) Поле-путь (разбиваем на части)
    for t in CATPATH_TAGS:
        val = first_desc_text(item, [t])
        if val:
            parts = [p.strip() for p in SEP_RE.split(val) if p.strip()]
            if parts: return parts[:4]  # ограничим глубину

    # 2) Раздельные поля
    cat  = first_desc_text(item, CAT_TAGS) or ""
    scat = first_desc_text(item, SUBCAT_TAGS) or ""
    path = [p for p in [cat, scat] if p]
    if path: return path

    # 3) Эвристика по совпадению имен тегов
    cand = all_desc_texts_like(item, ["category","категор","group","группа","section","раздел"])
    # чистим
    seen = set(); clean = []
    for v in cand:
        vv = v.strip()
        if not vv or vv.lower() in seen: continue
        seen.add(vv.lower())
        # отсечем слишком короткие (типа "A")
        if len(vv) < 2: continue
        clean.append(vv)
        if len(clean) >= 2: break
    return clean

# -------- parsing --------
def parse_item(item: ET.Element) -> Optional[Dict[str, Any]]:
    name = first_desc_text(item, NAME_TAGS)
    if not name: return None

    vendor = first_desc_text(item, VENDOR_TAGS) or "NV Print"
    vendor_code = first_desc_text(item, SKU_TAGS)

    price = None
    for t in PRICE_KZT_TAGS:
        price = parse_number(first_desc_text(item, [t]))
        if price is not None: break
    if price is None:
        for t in PRICE_ANY_TAGS:
            price = parse_number(first_desc_text(item, [t]))
            if price is not None: break
    if price is None or price <= 0: return None

    url = first_desc_text(item, URL_TAGS) or ""
    pics = all_desc_texts_like(item, PIC_LIKE)
    pics = [p for p in pics if re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", p, re.I)]
    pics = list(dict.fromkeys(pics))[:MAX_PICTURES]

    desc = first_desc_text(item, DESC_TAGS)
    if not desc:
        bits = [name]
        if vendor_code: bits.append(f"Артикул: {vendor_code}")
        bc = first_desc_text(item, BARCODE_TAGS)
        if bc: bits.append(f"Штрихкод: {bc}")
        desc = "; ".join(bits)

    qty = 0.0
    for t in QTY_TAGS:
        n = parse_number(first_desc_text(item, [t]))
        if n is not None: qty = max(qty, n)
    available = qty > 0
    in_stock = available
    qty_int = int(round(qty)) if qty and qty > 0 else 0

    path = extract_category_path(item)

    return {
        "name": name,
        "vendor": vendor,
        "vendorCode": vendor_code or "",
        "price": price,
        "url": url,
        "pictures": pics,
        "description": desc,
        "qty": qty_int,
        "path": path,
        "params": {},  # можно расширить при необходимости
    }

# -------- YML build --------
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
        for u in (it.get("pictures") or []):
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

# -------- main --------
def main() -> int:
    # XML → файл-дамп
    xml_bytes = fetch_xml_bytes(XML_URL)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    try:
        with open("docs/nvprint_source.xml","wb") as f:
            f.write(xml_bytes[:10_000_000])
    except Exception:
        pass

    root = ET.fromstring(xml_bytes)
    items = guess_items(root)
    print(f"[nvprint] guessed items: {len(items)}")

    parsed: List[Dict[str,Any]] = []
    for el in items:
        it = parse_item(el)
        if it: parsed.append(it)

    offers: List[Tuple[int, Dict[str,Any]]] = []
    paths: List[List[str]] = []
    for i, it in enumerate(parsed):
        offer_id_src = it.get("vendorCode") or it.get("url") or it.get("name") or f"nv-{i+1}"
        oid = re.sub(r"[^\w\-]+", "-", offer_id_src).strip("-") or f"nv-{i+1}"
        available = (it.get("qty", 0) or 0) > 0
        path = it.get("path") or []
        paths.append(path)
        offers.append((ROOT_CAT_ID, {
            "id": oid, "name": it["name"], "vendor": it.get("vendor") or "NV Print",
            "vendorCode": it.get("vendorCode") or "", "price": it["price"],
            "url": it.get("url") or "", "pictures": it.get("pictures") or [],
            "description": it.get("description") or "", "qty": int(it.get("qty") or 0),
            "available": available, "in_stock": available, "params": it.get("params") or {},
        }))

    # дерево категорий
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

    xml = build_yml(categories, offers_final)
    with open(OUT_FILE, "w", encoding=("utf-8" if ENCODING.startswith("utf") else "cp1251"), errors="ignore") as f:
        f.write(xml)

    print(f"[nvprint-xml] done: {len(offers_final)} offers, {len(categories)} categories -> {OUT_FILE} (encoding={ENCODING})")
    if not categories:
        print("INFO: категории не найдены в XML — товары отправлены в корень. "
              "Если нужны рубрики, укажи NVPRINT_CAT_PATH_TAGS или NVPRINT_CAT_TAGS/NVPRINT_SUBCAT_TAGS.", file=sys.stderr)
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
