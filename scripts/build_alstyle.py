# scripts/build_alstyle.py
from __future__ import annotations
import os, io, sys, re
import requests
import xml.etree.ElementTree as ET

SUPPLIER_URL   = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php")
OUT_FILE       = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC            = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
CATS_FILE      = os.getenv("CATEGORIES_FILE", "docs/categories_alstyle.txt")
BASIC_USER     = os.getenv("BASIC_USER", "").strip()
BASIC_PASS     = os.getenv("BASIC_PASS", "").strip()

def load_filters(path: str):
    ids=set(); subs=[]; regs=[]
    try:
        with open(path,"r",encoding="utf-8") as f:
            for line in f:
                s=line.strip()
                if not s or s.startswith("#"): continue
                if s.isdigit():
                    ids.add(s)
                elif s.lower().startswith("re:"):
                    try: regs.append(re.compile(s[3:], re.I))
                    except: pass
                else:
                    subs.append(s.lower())
    except FileNotFoundError:
        pass
    return ids, subs, regs

def match_cat(name: str, ids_filter: set[str], subs, regs, cid: str):
    if not ids_filter and not subs and not regs:
        return True
    if cid in ids_filter: return True
    nm = (name or "").lower()
    if any(sub in nm for sub in subs): return True
    if any(r.search(nm) for r in regs): return True
    return False

def read_supplier_xml(url: str) -> tuple[ET.Element, bytes]:
    auth = (BASIC_USER, BASIC_PASS) if BASIC_USER and BASIC_PASS else None
    r = requests.get(url, auth=auth, timeout=180)
    r.raise_for_status()
    content = r.content
    root = ET.fromstring(content)  # пусть парсер сам определит кодировку из пролога
    return root, content

def write_empty(out_path: str):
    root = ET.Element("yml_catalog")
    shop = ET.SubElement(root, "shop")
    ET.SubElement(shop, "name").text = "al-style.kz"
    curr = ET.SubElement(shop, "currencies")
    ET.SubElement(curr, "currency", {"id":"KZT","rate":"1"})
    ET.SubElement(shop, "categories")
    ET.SubElement(shop, "offers")
    ET.ElementTree(root).write(out_path, encoding=ENC, xml_declaration=True)

def main():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

    # 1) загрузка и парсинг
    root_in, raw = read_supplier_xml(SUPPLIER_URL)

    # Ищем элементы максимально «широко», чтобы не промахнуться по путям
    shop_in = root_in.find(".//shop")
    cats_in = root_in.findall(".//categories/category")
    offs_in = root_in.findall(".//offers/offer")

    if shop_in is None or not cats_in or not offs_in:
        # Попробуем альтернативу: иногда категории лежат прямо под <shop>
        if shop_in is None:
            shop_in = root_in
        if not cats_in:
            cats_in = (shop_in.findall("./categories/category") or
                       root_in.findall("./shop/categories/category"))
        if not offs_in:
            offs_in = (shop_in.findall("./offers/offer") or
                       root_in.findall("./shop/offers/offer"))

    if not cats_in or not offs_in:
        # Не пишем «успешную пустышку», а явно сигналим текстом и пишем пустую шапку
        sys.stderr.write("WARN: не нашёл categories/offers в источнике. Проверь ссылку и доступ.\n")
        preview = raw[:400].decode("latin-1","ignore")
        sys.stderr.write("RAW PREVIEW:\n" + preview + "\n")
        write_empty(OUT_FILE)
        print(f"{OUT_FILE}: пусто (источник без categories/offers)"); 
        return

    # 2) фильтры категорий
    id_filter, subs, regs = load_filters(CATS_FILE)

    id2cat = {c.get("id"): c for c in cats_in}
    parent = {c.get("id"): c.get("parentId") for c in cats_in}

    # какие категории оставляем
    if not id_filter and not subs and not regs:
        used_offers = offs_in
        used_cat_ids = set(id2cat.keys())
    else:
        keep_rule=set()
        for c in cats_in:
            cid = c.get("id")
            name = (c.text or "").strip()
            if match_cat(name, id_filter, subs, regs, cid):
                keep_rule.add(cid)
        used_offers=[]; used_cat_ids=set()
        for o in offs_in:
            cid = (o.findtext("categoryId") or "").strip()
            if cid in keep_rule:
                used_offers.append(o); used_cat_ids.add(cid)
        # добавляем всех предков
        def add_ancestors(cid: str):
            cur = parent.get(cid); seen=set()
            while cur and cur not in seen:
                used_cat_ids.add(cur); seen.add(cur); cur = parent.get(cur)
        for cid in list(used_cat_ids):
            add_ancestors(cid)

    # 3) пишем выходной YML
    out_root = ET.Element("yml_catalog")
    out_shop = ET.SubElement(out_root, "shop")
    ET.SubElement(out_shop, "name").text = "al-style.kz"
    curr = ET.SubElement(out_shop, "currencies")
    ET.SubElement(curr, "currency", {"id":"KZT","rate":"1"})

    # категории (родители → дети)
    cats_out = ET.SubElement(out_shop, "categories")
    def level_of(cid: str) -> int:
        lv=0; cur = parent.get(cid)
        while cur:
            lv += 1; cur = parent.get(cur)
        return lv
    for cid in sorted(used_cat_ids, key=level_of):
        c = id2cat.get(cid)
        if not c: continue
        attrs = {"id": c.get("id")}
        if c.get("parentId"): attrs["parentId"] = c.get("parentId")
        el = ET.SubElement(cats_out, "category", attrs)
        el.text = (c.text or "").strip()

    # офферы (копируем поля как есть)
    offers_out = ET.SubElement(out_shop, "offers")
    for o in used_offers:
        new = ET.SubElement(offers_out, "offer", dict(o.attrib))
        for child in list(o):
            node = ET.SubElement(new, child.tag)
            node.text = (child.text or "").strip()

    ET.ElementTree(out_root).write(OUT_FILE, encoding=ENC, xml_declaration=True)
    print(f"Wrote {OUT_FILE}: offers={len(used_offers)}, cats={len(used_cat_ids)} (encoding={ENC})")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
