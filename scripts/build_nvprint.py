# -*- coding: utf-8 -*-
"""
NVPrint: XML API -> YML (KZT) + обогащение с nvprint.ru по артикулу.

База (обязательная):
- NVPRINT_XML_URL: https://api.nvprint.ru/api/hs/getprice/398/881105302369/none/?format=xml
- BasicAuth: NVPRINT_LOGIN / NVPRINT_PASSWORD (если требуется поставщиком; для этой ссылки тоже BasicAuth)

Обогащение (опционально):
- NVPRINT_ENRICH_FROM_SITE=1 — включить поиск на https://nvprint.ru по артикулу
- NVPRINT_SITE_SEARCH_TEMPLATES — список шаблонов поиска (по умолчанию пробуем популярные варианты WP):
    "https://nvprint.ru/?s={art},https://nvprint.ru/search/?q={art}"
- Селекторы можно переопределить через ENV (см. ниже).
"""

from __future__ import annotations
import os, re, sys, html, hashlib, time
from typing import Any, Dict, List, Optional, Tuple
import requests, xml.etree.ElementTree as ET
from datetime import datetime
from bs4 import BeautifulSoup

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
PIC_OVR      = os.getenv("NVPRINT_PIC_TAGS")
BARCODE_OVR  = os.getenv("NVPRINT_BARCODE_TAGS")
CATPATH_OVR  = os.getenv("NVPRINT_CAT_PATH_TAGS")

# ---------- ENV: обогащение с nvprint.ru ----------
ENRICH_SITE       = os.getenv("NVPRINT_ENRICH_FROM_SITE", "0") == "1"
ENRICH_LIMIT      = int(os.getenv("NVPRINT_ENRICH_LIMIT", "300"))         # лимит на один запуск (0 = все)
ENRICH_DELAY_MS   = int(os.getenv("NVPRINT_ENRICH_DELAY_MS", "250"))      # задержка между запросами

SITE_SEARCH_TPL   = (os.getenv("NVPRINT_SITE_SEARCH_TEMPLATES")
                     or "https://nvprint.ru/?s={art},https://nvprint.ru/search/?q={art}")
SITE_SEARCH_TEMPLATES = [t.strip() for t in SITE_SEARCH_TPL.split(",") if "{art}" in t]

B2B_TIMEOUT       = float(os.getenv("NVPRINT_SITE_TIMEOUT", "25"))

# CSS селекторы для карточки nvprint.ru (можно переопределить ENV при необходимости)
SEL_PRODUCT_LINKS   = os.getenv("NVPRINT_SITE_SEL_SEARCH_LINKS", "a[href*='/product/']").strip()
SEL_TITLE           = os.getenv("NVPRINT_SITE_SEL_TITLE", ".page-title,h1,.product-title").strip()
SEL_DESC            = os.getenv("NVPRINT_SITE_SEL_DESC", ".product-description,meta[name='description']").strip()
SEL_OGIMG           = os.getenv("NVPRINT_SITE_SEL_OGIMG", "meta[property='og:image'],meta[name='og:image']").strip()
SEL_GALLERY         = os.getenv("NVPRINT_SITE_SEL_GALLERY", ".product-gallery img, img").strip()
SEL_BREADCRUMBS     = os.getenv("NVPRINT_SITE_SEL_BC", ".breadcrumbs li, nav.breadcrumbs li, .breadcrumb li").strip()
# пары характеристик (dt -> dd) или таблицы
SEL_SPECS_BLOCKS    = os.getenv("NVPRINT_SITE_SEL_SPECS", "dl, .specs, .characteristics, table").strip()

ROOT_CAT_ID   = 9400000
ROOT_CAT_NAME = "NVPrint"
UA = {"User-Agent": "Mozilla/5.0 (compatible; NVPrint-XML-Feed/2.0)"}

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
    # сохраним исходник для дебага
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    try:
        with open("docs/nvprint_source.xml", "wb") as f:
            f.write(b[:10_000_000])
    except Exception:
        pass
    return b

def http_get(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=UA, timeout=B2B_TIMEOUT, allow_redirects=True)
        if r.status_code != 200: return None
        return r.content
    except Exception:
        return None

def soup_of(b: Optional[bytes]) -> BeautifulSoup:
    return BeautifulSoup(b or b"", "html.parser")

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
    # типовые пути
    cands = root.findall(".//Товар") + root.findall(".//item") + root.findall(".//product") + root.findall(".//row")
    if cands: return cands
    # эвристика: есть имя и цена
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
DESC_TAGS       = split_tags(DESC_OVR,      ["Описание","description","descr","short_description"])
CAT_TAGS        = split_tags(CAT_OVR,       ["РазделПрайса","category","категория","group","раздел"])
SUBCAT_TAGS     = split_tags(SUBCAT_OVR,    ["subcategory","подкатегория","subgroup","подраздел"])
PIC_LIKE        = split_tags(PIC_OVR,       ["image","img","picture","photo","фото"])
QTY_TAGS        = split_tags(QTY_OVR,       ["Наличие","quantity","qty","stock","остаток"])
BARCODE_TAGS    = split_tags(BARCODE_OVR,   ["barcode","ean","штрихкод","ean13"])
CATPATH_TAGS    = split_tags(CATPATH_OVR,   ["category_path","full_path","path","путь"])

# ---------- категории ----------
SEP_RE = re.compile(r"\s*(?:>|/|\\|\||→|»|›|—|-)\s*")
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
    # fallback: любые поля, похожие на "категория"
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

    url = first_desc_text(item, URL_TAGS) or ""
    desc = first_desc_text(item, DESC_TAGS)
    if not desc:
        base = first_desc_text(item, ["Номенклатура"]) or name
        bits = [base]
        if vendor_code:
            bits.append(f"Артикул: {vendor_code}")
        desc = "; ".join(bits)

    qty = 0.0
    for t in QTY_TAGS:
        n = parse_number(first_desc_text(item, [t]))
        if n is not None:
            qty = max(qty, n)
    qty_int = int(round(qty)) if qty and qty > 0 else 0
    available = qty_int > 0

    path = extract_category_path(item)

    return {
        "name": name,
        "vendor": vendor,
        "vendorCode": vendor_code,
        "price": price,
        "url": url,
        "pictures": [],
        "description": desc,
        "qty": qty_int,
        "path": path,
        "params": {},
        "available": available,
        "in_stock": available,
    }

# ---------- nvprint.ru enrichment ----------
IMG_RE = re.compile(r"\.(?:jpg|jpeg|png|webp|gif)(?:\?.*)?$", re.I)

def parse_specs_params(s: BeautifulSoup) -> Dict[str, str]:
    params: Dict[str, str] = {}
    # dt/dd пары
    for dl in s.select("dl"):
        dts = dl.find_all("dt"); dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            k = (dt.get_text(" ", strip=True) or "").strip(": ")
            v = (dd.get_text(" ", strip=True) or "").strip()
            if k and v:
                params.setdefault(k, v)
    # таблицы 2-колоночные
    for table in s.select("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td","th"])
            if len(tds) == 2:
                k = (tds[0].get_text(" ", strip=True) or "").strip(": ")
                v = (tds[1].get_text(" ", strip=True) or "").strip()
                if k and v:
                    params.setdefault(k, v)
    return params

def enrich_from_nv_site(art: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not art:
        return out
    for tmpl in SITE_SEARCH_TEMPLATES:
        url = tmpl.format(art=art)
        time.sleep(max(0.0, ENRICH_DELAY_MS/1000.0))
        b = http_get(url)
        if not b:
            continue
        s = soup_of(b)

        # если страница уже карточка
        og_url = s.select_one("meta[property='og:url']")
        if og_url and og_url.get("content"):
            out.setdefault("url", og_url["content"].strip())

        # подхватим og:image
        og_img = s.select_one(SEL_OGIMG)
        if og_img and og_img.get("content"):
            out.setdefault("pictures", []).append(og_img["content"].strip())

        # соберём ссылки на /product/
        links = s.select(SEL_PRODUCT_LINKS) if SEL_PRODUCT_LINKS else []
        hrefs: List[str] = []
        for a in links:
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if href.startswith("//"):
                href = "https:" + href
            hrefs.append(href)

        # пройдём по нескольким кандидатам
        for href in hrefs[:5]:
            time.sleep(max(0.0, ENRICH_DELAY_MS/1000.0))
            pb = http_get(href)
            if not pb:
                continue
            ps = soup_of(pb)

            # заголовок
            t_el = ps.select_one(SEL_TITLE)
            if t_el:
                out["title"] = (t_el.get_text(" ", strip=True) or "").strip()

            # описание (meta или блок)
            d_el = ps.select_one(SEL_DESC)
            if d_el:
                if d_el.name == "meta":
                    out["description"] = (d_el.get("content") or "").strip()
                else:
                    out["description"] = (d_el.get_text(" ", strip=True) or "").strip()

            # фото: og:image + галерея
            og = ps.select_one(SEL_OGIMG)
            if og and og.get("content"):
                out.setdefault("pictures", []).append(og.get("content").strip())
            for img in ps.select(SEL_GALLERY) or []:
                u = (img.get("src") or img.get("data-src") or img.get("data-image") or "").strip()
                if u.startswith("//"):
                    u = "https:" + u
                if IMG_RE.search(u):
                    out.setdefault("pictures", []).append(u)

            # хлебные крошки → категории
            bnames: List[str] = []
            for bc in ps.select(SEL_BREADCRUMBS) or []:
                t = (bc.get_text(" ", strip=True) or "").strip()
                if t:
                    bnames.append(t)
            if len(bnames) >= 2:
                bnames = [t for t in bnames if t.lower() not in ("главная", "каталог", "home", "catalog")]
            if bnames:
                out["breadcrumbs"] = bnames[:4]

            # характеристики → params
            params = parse_specs_params(ps)
            if params:
                out["params"] = params

            out.setdefault("url", href)
            if out.get("title") or out.get("pictures") or out.get("description") or out.get("params"):
                # достаточно данных — выходим
                return out
    return out

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
        out.append(f"<vendor>{x(it.get("vendor") or "NV Print")}</vendor>")
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

    # 3) дерево категорий из XML-путей
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

    # 4) обогащение nvprint.ru
    if ENRICH_SITE and SITE_SEARCH_TEMPLATES:
        total = len(offers) if ENRICH_LIMIT <= 0 else min(ENRICH_LIMIT, len(offers))
        print(f"[nvprint.ru] enrich: {total} items (limit={ENRICH_LIMIT})")
        for idx in range(total):
            cid, it = offers[idx]
            art = it.get("vendorCode") or ""
            if not art:
                continue
            add = {}
            try:
                add = enrich_from_nv_site(art)
            except Exception:
                add = {}
            # применяем только то, чего нет
            if add.get("url") and not it.get("url"):
                it["url"] = add["url"]
            if add.get("pictures"):
                pics = list(dict.fromkeys((it.get("pictures") or []) + add["pictures"]))
                it["pictures"] = pics[:MAX_PICTURES]
            if add.get("description"):
                if not it.get("description") or len(it["description"]) < 40:
                    it["description"] = add["description"]
            if add.get("title"):
                # иногда на сайте название аккуратнее — можно заменить, если оно короче/чище
                if len(add["title"]) <= len(it["name"]) + 10:
                    it["name"] = add["title"]
            if add.get("params"):
                for k, v in add["params"].items():
                    it.setdefault("params", {}).setdefault(k, v)
            # хлебные крошки → категория точнее
            bc = add.get("breadcrumbs") or []
            if bc:
                parent = ROOT_CAT_ID; acc: List[str] = []
                for name in bc:
                    acc.append(name.strip()); key = tuple(acc)
                    if key not in cat_map:
                        cid_new = stable_cat_id(" / ".join(acc))
                        cat_map[key] = cid_new
                        categories.append((cid_new, name.strip(), parent))
                        parent = cid_new
                    else:
                        parent = cat_map[key]
                it_cid = cat_map[tuple(acc)]
                offers[idx] = (it_cid, it)
            # задержка между товарами
            time.sleep(max(0.0, ENRICH_DELAY_MS/1000.0))

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
