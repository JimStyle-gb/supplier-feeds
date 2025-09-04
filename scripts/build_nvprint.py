# -*- coding: utf-8 -*-
"""
NVPrint (XML Basic Auth) → YML (Satu-совместимый) + обогащение из B2B по артикулам.
- База: NVPRINT_XML_URL (Basic Auth NVPRINT_LOGIN/NVPRINT_PASSWORD; поддерживаются NVPRINT_XML_USER/PASS).
- Категории: из путей/парных тегов в XML (как в предыдущей версии).
- Обогащение (опционально): NVPRINT_ENRICH_FROM_B2B=1 — поиск по артикулу на B2B, подтягиваем url/picture/description/brand/breadcrumbs.
  * URL поиска задаются через NVPRINT_B2B_SEARCH_TEMPLATES (через запятую, шаблон {art} обязателен).
  * Селекторы можно переопределить через ENV (см. ниже).
"""

from __future__ import annotations
import os, re, sys, html, hashlib, time
from typing import Any, Dict, List, Optional, Tuple
import requests, xml.etree.ElementTree as ET
from datetime import datetime
from bs4 import BeautifulSoup

# -------- ENV (источник XML) --------
XML_URL      = os.getenv("NVPRINT_XML_URL", "").strip()
NV_LOGIN     = (os.getenv("NVPRINT_LOGIN") or os.getenv("NVPRINT_XML_USER") or "").strip()
NV_PASSWORD  = (os.getenv("NVPRINT_PASSWORD") or os.getenv("NVPRINT_XML_PASS") or "").strip()

OUT_FILE     = os.getenv("OUT_FILE", "docs/nvprint.yml")
ENCODING     = (os.getenv("OUTPUT_ENCODING") or "utf-8").lower()
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))
MAX_PICTURES = int(os.getenv("MAX_PICTURES", "10"))

# -------- ENV (категории/теги в XML) --------
ITEM_XPATH   = (os.getenv("NVPRINT_ITEM_XPATH") or "").strip()
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

# -------- ENV (обогащение из B2B) --------
ENRICH       = os.getenv("NVPRINT_ENRICH_FROM_B2B", "0") == "1"
ENRICH_LIMIT = int(os.getenv("NVPRINT_ENRICH_LIMIT", "200"))
ENRICH_DELAY = int(os.getenv("NVPRINT_ENRICH_DELAY_MS", "250"))  # мс
B2B_TIMEOUT  = float(os.getenv("NVPRINT_B2B_TIMEOUT", "25"))

# шаблоны поиска; через запятую; каждый должен содержать {art}
B2B_SEARCH_TEMPLATES = [t.strip() for t in (os.getenv("NVPRINT_B2B_SEARCH_TEMPLATES") or "").split(",") if "{art}" in t]

# опционально — если требуется логин на B2B
B2B_LOGIN    = os.getenv("NVPRINT_B2B_LOGIN", "").strip()
B2B_PASSWORD = os.getenv("NVPRINT_B2B_PASSWORD", "").strip()

# CSS-селекторы можно переопределить через ENV
SEL_LINKS         = os.getenv("NVPRINT_B2B_SEL_SEARCH_LINKS", "a[href*='/product'],a[href*='/catalog/'],a.product-link").strip()
SEL_TITLE         = os.getenv("NVPRINT_B2B_SEL_TITLE", ".page-title,h1,.product-title").strip()
SEL_CODE          = os.getenv("NVPRINT_B2B_SEL_CODE", "[data-code], .sku, .articul, .product-code").strip()
SEL_DESC          = os.getenv("NVPRINT_B2B_SEL_DESC", ".description,.product-description,meta[name='description']").strip()
SEL_OG_IMAGE_META = os.getenv("NVPRINT_B2B_SEL_OGIMG", "meta[property='og:image'], meta[name='og:image']").strip()
SEL_GALLERY       = os.getenv("NVPRINT_B2B_SEL_GALLERY", "img, [data-img], [data-image], .product-gallery img").strip()
SEL_BREADCRUMBS   = os.getenv("NVPRINT_B2B_SEL_BC", ".breadcrumbs li, nav.breadcrumbs li, .breadcrumb li").strip()
SEL_VENDOR        = os.getenv("NVPRINT_B2B_SEL_VENDOR", ".vendor,.brand,.producer,.manufacturer").strip()

ROOT_CAT_ID   = 9400000
ROOT_CAT_NAME = "NVPrint"

UA = {"User-Agent": "Mozilla/5.0 (compatible; NVPrint-XML-Feed/1.5)"}

def x(s: str) -> str: return html.escape((s or "").strip())
def stable_cat_id(text: str, prefix: int = 9420000) -> int:
    h = hashlib.md5((text or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    return prefix + int(h, 16)

def fetch_xml_bytes(url: str) -> bytes:
    if not url: raise RuntimeError("NVPRINT_XML_URL пуст.")
    auth = (NV_LOGIN, NV_PASSWORD) if (NV_LOGIN or NV_PASSWORD) else None
    r = requests.get(url, auth=auth, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status(); b = r.content
    # сохраним сырой источник
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    try:
        with open("docs/nvprint_source.xml","wb") as f: f.write(b[:10_000_000])
    except Exception: pass
    return b

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
    cands = root.findall(".//item") + root.findall(".//row") + root.findall(".//product") + root.findall(".//Товар")
    if cands: return cands
    NAME_TAGS = split_tags(NAME_OVR, ["full_name","fullname","name","наименование","title","НоменклатураКратко","Номенклатура"])
    PRICE_ANY_TAGS = split_tags(PRICEANY_OVR, ["price","цена","amount","value","Цена"])
    out: List[ET.Element] = []
    for node in root.iter():
        if first_desc_text(node, NAME_TAGS) and first_desc_text(node, PRICE_ANY_TAGS):
            out.append(node)
    return out

# -------- default tag sets (перекрываются ENV) --------
NAME_TAGS       = split_tags(NAME_OVR,      ["full_name","fullname","name","наименование","title","НоменклатураКратко","Номенклатура"])
VENDOR_TAGS     = split_tags(VENDOR_OVR,    ["brand","бренд","вендор","producer","manufacturer","производитель"])
SKU_TAGS        = split_tags(SKU_OVR,       ["articul","артикул","sku","code","код","vendorcode","кодтовара","Артикул"])
PRICE_KZT_TAGS  = split_tags(PRICEKZT_OVR,  ["price_kzt","ценатенге","цена_kzt","kzt","pricekzt","price_kz","price_kaz"])
PRICE_ANY_TAGS  = split_tags(PRICEANY_OVR,  ["price","цена","amount","value","Цена"])
URL_TAGS        = split_tags(URL_OVR,       ["url","link","ссылка"])
DESC_TAGS       = split_tags(DESC_OVR,      ["description","описание","descr","short_description","Описание"])
CAT_TAGS        = split_tags(CAT_OVR,       ["category","категория","group","группа","section","раздел","РазделПрайса"])
SUBCAT_TAGS     = split_tags(SUBCAT_OVR,    ["subcategory","подкатегория","subgroup","subsection","подраздел"])
PIC_LIKE        = split_tags(PIC_OVR,       ["image","img","picture","photo","фото","imageurl","image_url","photourl"])
QTY_TAGS        = split_tags(QTY_OVR,       ["quantity","qty","остаток","stock","amount","наличие","на_складе","store_amount","Наличие"])
BARCODE_TAGS    = split_tags(BARCODE_OVR,   ["barcode","ean","штрихкод","ean13","ean-13"])
CATPATH_TAGS    = split_tags(CATPATH_OVR,   ["category_path","full_path","path","категорияпуть","путь","раздел_путь"])

# -------- categories extraction --------
SEP_RE = re.compile(r"\s*(?:>|/|\\|\||→|»|›|—|-)\s*")

def extract_category_path(item: ET.Element) -> List[str]:
    for t in CATPATH_TAGS:
        val = first_desc_text(item, [t])
        if val:
            parts = [p.strip() for p in SEP_RE.split(val) if p.strip()]
            if parts: return parts[:4]
    cat  = first_desc_text(item, CAT_TAGS) or ""
    scat = first_desc_text(item, SUBCAT_TAGS) or ""
    path = [p for p in [cat, scat] if p]
    if path: return path
    cand = all_desc_texts_like(item, ["category","категор","group","группа","section","раздел"])
    seen = set(); clean = []
    for v in cand:
        vv = v.strip()
        if not vv or vv.lower() in seen: continue
        seen.add(vv.lower())
        if len(vv) < 2: continue
        clean.append(vv)
        if len(clean) >= 2: break
    return clean

# -------- parsing XML item --------
def parse_item(item: ET.Element) -> Optional[Dict[str, Any]]:
    # name: предпочитаем короткое
    name = first_desc_text(item, ["НоменклатураКратко"]) or first_desc_text(item, NAME_TAGS)
    if not name: return None
    vendor = first_desc_text(item, VENDOR_TAGS) or "NV Print"
    vendor_code = first_desc_text(item, ["Артикул"]) or first_desc_text(item, SKU_TAGS)

    price = None
    for t in PRICE_KZT_TAGS:
        price = parse_number(first_desc_text(item, [t]));  if price is not None: break
    if price is None:
        for t in PRICE_ANY_TAGS:
            price = parse_number(first_desc_text(item, [t]));  if price is not None: break
    if price is None or price <= 0: return None

    url = first_desc_text(item, URL_TAGS) or ""
    pics = []  # из XML обычно нет
    desc = first_desc_text(item, DESC_TAGS)
    if not desc:
        base = first_desc_text(item, ["Номенклатура"]) or name
        bits = [base]
        if vendor_code: bits.append(f"Артикул: {vendor_code}")
        desc = "; ".join(bits)

    qty = 0.0
    for t in QTY_TAGS:
        n = parse_number(first_desc_text(item, [t]))
        if n is not None: qty = max(qty, n)
    available = qty > 0
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
        "params": {},
    }

# -------- B2B enrichment --------
def b2b_login_if_needed(session: requests.Session) -> None:
    """
    Заглушка под возможный логин. Если портал требует логин — реализуй здесь.
    Примеры:
      - Basic Auth уже в домене — тогда не нужно.
      - Форма логина: session.post(LOGIN_URL, data={...}, headers=..., timeout=B2B_TIMEOUT)
      - CSRF: сначала GET, вытащить meta[name=csrf-token], затем POST.
    Сейчас ничего не делаем.
    """
    return

def soup_of(resp_bytes: bytes) -> BeautifulSoup:
    return BeautifulSoup(resp_bytes or b"", "html.parser")

def http_get(session: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = session.get(url, timeout=B2B_TIMEOUT, allow_redirects=True)
        if r.status_code != 200: return None
        return r.content
    except Exception:
        return None

IMG_RE = re.compile(r"\.(?:jpg|jpeg|png|webp)(?:\?.*)?$", re.I)

def enrich_one(session: requests.Session, art: str) -> Dict[str, Any]:
    """Возвращает dict с возможными полями: url, pictures(list), title, description, vendor, breadcrumbs(list)."""
    out: Dict[str, Any] = {}
    if not art: return out
    # последовательно пробуем шаблоны
    for tmpl in B2B_SEARCH_TEMPLATES or []:
        search_url = tmpl.format(art=art)
        time.sleep(max(0.0, ENRICH_DELAY/1000.0))
        b = http_get(session, search_url)
        if not b: continue
        s = soup_of(b)

        # 1) если есть og:url/og:image — иногда уже страница товара
        og_url = s.select_one("meta[property='og:url']")
        if og_url and og_url.get("content"): out.setdefault("url", og_url["content"].strip())
        og_img = s.select_one(SEL_OG_IMAGE_META)
        if og_img and og_img.get("content"):
            out.setdefault("pictures", []).append(og_img["content"].strip())

        # 2) собрать ссылки-кандидаты
        links = s.select(SEL_LINKS) if SEL_LINKS else []
        cand_hrefs: List[str] = []
        for a in links:
            href = a.get("href") or ""
            href = href.strip()
            if not href: continue
            if href.startswith("//"): href = "https:" + href
            if href.startswith("/"):  # попытка восстановить абсолютную (если домен совпадает)
                # домен берём из search_url
                m = re.match(r"^(https?://[^/]+)", search_url)
                if m: href = m.group(1) + href
            cand_hrefs.append(href)

        # 3) проходим по кандидатам и ищем карточку с совпадающим артикулом
        for url in cand_hrefs[:5]:  # не больше 5 кликов на результат
            time.sleep(max(0.0, ENRICH_DELAY/1000.0))
            pb = http_get(session, url)
            if not pb: continue
            ps = soup_of(pb)

            # сверяем артикул, если удаётся извлечь
            code_ok = False
            if SEL_CODE:
                code_el = ps.select_one(SEL_CODE)
                code_txt = ""
                if code_el:
                    code_txt = (code_el.get("content") or code_el.get_text(" ", strip=True) or "").strip()
                if code_txt:
                    norm = re.sub(r"\W+", "", code_txt).lower()
                    norm_art = re.sub(r"\W+", "", art).lower()
                    if norm and norm == norm_art:
                        code_ok = True
            # если не смогли проверить — допустим
            if not code_ok and SEL_CODE:
                pass

            # собираем данные
            title_el = ps.select_one(SEL_TITLE) if SEL_TITLE else None
            if title_el:
                out["title"] = (title_el.get_text(" ", strip=True) or "").strip()

            if SEL_DESC:
                d_el = ps.select_one(SEL_DESC)
                if d_el:
                    if d_el.name == "meta":
                        out["description"] = (d_el.get("content") or "").strip()
                    else:
                        out["description"] = (d_el.get_text(" ", strip=True) or "").strip()

            if SEL_VENDOR:
                v_el = ps.select_one(SEL_VENDOR)
                if v_el:
                    out["vendor"] = (v_el.get_text(" ", strip=True) or "").strip()

            # картинки: og:image + галерея
            og = ps.select_one(SEL_OG_IMAGE_META)
            if og and og.get("content"):
                out.setdefault("pictures", []).append(og["content"].strip())
            for img in ps.select(SEL_GALLERY) or []:
                u = img.get("src") or img.get("data-src") or img.get("data-image") or ""
                u = u.strip()
                if not u: continue
                if u.startswith("//"): u = "https:" + u
                if IMG_RE.search(u):
                    out.setdefault("pictures", []).append(u)

            # хлебные крошки → категории
            bnames: List[str] = []
            for bc in ps.select(SEL_BREADCRUMBS) or []:
                t = (bc.get_text(" ", strip=True) or "").strip()
                if t: bnames.append(t)
            # часто первые элементы — Домой/Каталог → обрежем их
            if len(bnames) >= 2:
                # эвристика: убрать "Главная" и "Каталог"
                bnames = [t for t in bnames if t and t.lower() not in ("главная","каталог","home","catalog")]
            if bnames:
                out["breadcrumbs"] = bnames[:4]

            out.setdefault("url", url)  # если не было og:url
            # если есть что-то осмысленное — хватит
            if out.get("title") or out.get("pictures") or out.get("description"):
                return out

    return out

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

# -------- main --------
def main() -> int:
    # 1) XML → парс
    xml_bytes = fetch_xml_bytes(XML_URL)
    root = ET.fromstring(xml_bytes)
    items = guess_items(root)
    print(f"[nvprint] guessed items: {len(items)}")

    # 2) baseline товары
    parsed: List[Dict[str,Any]] = []
    for el in items:
        it = parse_item(el)
        if it: parsed.append(it)

    # 3) Категории из XML
    paths: List[List[str]] = []
    offers: List[Tuple[int, Dict[str,Any]]] = []
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

    # 4) дерево категорий (из XML путей)
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

    # 5) ОБОГАЩЕНИЕ из B2B (опционально)
    if ENRICH and (B2B_SEARCH_TEMPLATES or B2B_LOGIN or B2B_PASSWORD):
        s = requests.Session(); s.headers.update(UA)
        try:
            b2b_login_if_needed(s)
        except Exception:
            pass
        total = len(offers) if ENRICH_LIMIT <= 0 else min(ENRICH_LIMIT, len(offers))
        print(f"[b2b] enrich enabled: {total} items (limit={ENRICH_LIMIT})")
        for idx in range(total):
            cid, it = offers[idx]
            art = it.get("vendorCode") or ""
            if not art: continue
            try:
                add = enrich_one(s, art)
            except Exception:
                add = {}
            # применяем только то, чего нет
            if add.get("url") and not it.get("url"): it["url"] = add["url"]
            if add.get("pictures"):
                pics = list(dict.fromkeys((it.get("pictures") or []) + add["pictures"]))
                it["pictures"] = pics[:MAX_PICTURES]
            if add.get("description"):
                # если базовое описание короткое/синтетическое — заменим
                if not it.get("description") or len(it["description"]) < 40:
                    it["description"] = add["description"]
            if add.get("vendor") and (not it.get("vendor") or it["vendor"] == "NV Print"):
                it["vendor"] = add["vendor"]
            # хлебные крошки → категория точнее
            bc = add.get("breadcrumbs") or []
            if bc:
                # построим/зарегистрируем путь
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
                # присвоим самый глубокий
                it_cid = cat_map[tuple(acc)]
                offers[idx] = (it_cid, it)

    # 6) запись
    xml = build_yml(categories, offers)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding=("utf-8" if ENCODING.startswith("utf") else "cp1251"), errors="ignore") as f:
        f.write(xml)

    print(f"[nvprint-xml] done: {len(offers)} offers, {len(categories)} categories -> {OUT_FILE} (encoding={ENCODING})")
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
