# scripts/build_alstyle.py
from __future__ import annotations
import os, io, sys, re
import xml.etree.ElementTree as ET
import requests
from collections import defaultdict

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

def read_supplier_xml(url: str) -> ET.Element:
    auth = (BASIC_USER, BASIC_PASS) if BASIC_USER and BASIC_PASS else None
    r = requests.get(url, auth=auth, timeout=120)
    r.raise_for_status()
    content = r.content  # оставляем байты – парсер сам поймёт исходную кодировку
    return ET.fromstring(content)

def build_parent_map(cats: list[ET.Element]) -> dict[str,str|None]:
    # <category id="..." parentId="...">Name</category>
    parent = {}
    for c in cats:
        cid = c.get("id")
        parent[cid] = c.get("parentId")
    return parent

def collect_needed_categories(cats, parent, ids_keep: set[str]) -> set[str]:
    # добавляем всех предков
    need=set(ids_keep)
    for cid in list(ids_keep):
        cur = parent.get(cid)
        seen=set()
        while cur and cur not in seen:
            need.add(cur)
            seen.add(cur)
            cur = parent.get(cur)
    return need

def main():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

    # 1) грузим исходный YML
    root_in = read_supplier_xml(SUPPLIER_URL)
    shop_in = root_in.find("./shop")
    if shop_in is None:
        print("ERROR: нет <shop> в источнике", file=sys.stderr); sys.exit(1)

    cats_in = list(shop_in.findall("./categories/category"))
    offs_in = list(shop_in.findall("./offers/offer"))

    if not cats_in or not offs_in:
        # даже если пусто — отдадим пустую шапку, как есть
        out_root = ET.Element("yml_catalog")
        out_shop = ET.SubElement(out_root, "shop")
        ET.SubElement(out_shop, "name").text = "al-style.kz"
        curr = ET.SubElement(out_shop, "currencies")
        ET.SubElement(curr, "currency", {"id":"KZT","rate":"1"})
        ET.SubElement(out_shop, "categories")
        ET.SubElement(out_shop, "offers")
        ET.ElementTree(out_root).write(OUT_FILE, encoding=ENC, xml_declaration=True)
        print(f"{OUT_FILE}: пусто (в источнике нет категорий/офферов)"); return

    # 2) фильтры категорий
    id_filter, subs, regs = load_filters(CATS_FILE)

    # карта категорий
    id2cat = {c.get("id"): c for c in cats_in}
    parent = build_parent_map(cats_in)

    # если фильтров нет – берём всё
    if not id_filter and not subs and not regs:
        used_cat_ids = set(id2cat.keys())
        used_offers  = offs_in
    else:
        # выберем категории по имени/ID
        keep_by_rule=set()
        for c in cats_in:
            cid = c.get("id")
            name = (c.text or "").strip()
            if match_cat(name, id_filter, subs, regs, cid):
                keep_by_rule.add(cid)

        # офферы, у которых categoryId ∈ keep_by_rule
        used_offers=[]
        used_cat_ids=set()
        for o in offs_in:
            cid = (o.findtext("categoryId") or "").strip()
            if cid in keep_by_rule:
                used_offers.append(o)
                used_cat_ids.add(cid)

        # добавим всех предков
        used_cat_ids = collect_needed_categories(cats_in, parent, used_cat_ids)

    # 3) строим выходной YML c правильным деревом
    out_root = ET.Element("yml_catalog")
    out_shop = ET.SubElement(out_root, "shop")
    ET.SubElement(out_shop, "name").text = "al-style.kz"
    curr = ET.SubElement(out_shop, "currencies")
    ET.SubElement(curr, "currency", {"id":"KZT","rate":"1"})

    cats_out = ET.SubElement(out_shop, "categories")
    # вывод категорий в топологическом порядке: сначала без parentId, потом с родителями
    def level_of(cid: str) -> int:
        lv=0; cur=parent.get(cid)
        while cur:
            lv+=1; cur=parent.get(cur)
        return lv
    for cid in sorted(used_cat_ids, key=lambda x: level_of(x)):
        c = id2cat.get(cid)
        if c is None: continue
        attrs = {"id": c.get("id")}
        if c.get("parentId"): attrs["parentId"]=c.get("parentId")
        el = ET.SubElement(cats_out, "category", attrs)
        el.text = (c.text or "").strip()

    offers_out = ET.SubElement(out_shop, "offers")
    for o in used_offers:
        # копируем offer как есть (ID, name, price, currencyId, categoryId, vendor, vendorCode, picture, description, и т.д.)
        new = ET.SubElement(offers_out, "offer", dict(o.attrib))
        for child in list(o):
            # переносим все поля без изменений
            ET.SubElement(new, child.tag).text = (child.text or "").strip()

    ET.ElementTree(out_root).write(OUT_FILE, encoding=ENC, xml_declaration=True)
    print(f"Wrote {OUT_FILE}: offers={len(used_offers)}, cats={len(used_cat_ids)} (encoding={ENC})")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
