# scripts/build_alstyle.py
# Надёжная сборка alstyle.yml из их YML: категории строятся ПО ФАКТУ офферов + предки.
# Кодировка выхода — CP1251. Фильтр категорий (не обязателен) — docs/categories_alstyle.txt.
from __future__ import annotations
import os, sys, re
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

def cat_matches(name: str, cid: str, ids_filter:set[str], subs, regs) -> bool:
    if not ids_filter and not subs and not regs: return True
    if cid in ids_filter: return True
    nm = (name or "").lower()
    if any(sub in nm for sub in subs): return True
    if any(r.search(nm) for r in regs): return True
    return False

def fetch_xml(url: str) -> ET.Element:
    auth = (BASIC_USER, BASIC_PASS) if BASIC_USER and BASIC_PASS else None
    r = requests.get(url, auth=auth, timeout=180)
    r.raise_for_status()
    return ET.fromstring(r.content)

def write_empty(path: str):
    root = ET.Element("yml_catalog")
    shop = ET.SubElement(root, "shop")
    ET.SubElement(shop, "name").text = "al-style.kz"
    curr = ET.SubElement(shop, "currencies")
    ET.SubElement(curr, "currency", {"id":"KZT","rate":"1"})
    ET.SubElement(shop, "categories")
    ET.SubElement(shop, "offers")
    ET.ElementTree(root).write(path, encoding=ENC, xml_declaration=True)

def build_parent_map(cats):
    return {c.get("id"): c.get("parentId") for c in cats}

def main():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

    root_in = fetch_xml(SUPPLIER_URL)
    cats_in = root_in.findall(".//categories/category")
    offers_in = root_in.findall(".//offers/offer")

    if not offers_in:
        sys.stderr.write("WARN: у источника нет <offers/offer>\n")
        write_empty(OUT_FILE); print(f"{OUT_FILE}: offers=0, cats=0"); return

    id2name = { c.get("id"): (c.text or "").strip() for c in cats_in } if cats_in else {}
    parent  = build_parent_map(cats_in) if cats_in else {}

    ids_filter, subs, regs = load_filters(CATS_FILE)

    # фильтрация офферов (если нужен фильтр)
    if not ids_filter and not subs and not regs:
        used_offers = offers_in
    else:
        keep_cat_ids = set()
        for cid, nm in id2name.items():
            if cat_matches(nm, cid, ids_filter, subs, regs):
                keep_cat_ids.add(cid)
        used_offers = []
        for o in offers_in:
            cid = (o.findtext("categoryId") or "").strip()
            if not cid: continue
            if not keep_cat_ids or cid in keep_cat_ids:
                used_offers.append(o)

    # набор категорий из фактических офферов + предки
    used_cat_ids = set()
    for o in used_offers:
        cid = (o.findtext("categoryId") or "").strip()
        if cid: used_cat_ids.add(cid)

    if parent:
        def add_ancestors(cid: str):
            seen=set(); cur=parent.get(cid)
            while cur and cur not in seen:
                used_cat_ids.add(cur); seen.add(cur); cur=parent.get(cur)
        for cid in list(used_cat_ids):
            add_ancestors(cid)

    # пишем выходной YML
    out_root = ET.Element("yml_catalog")
    out_shop = ET.SubElement(out_root, "shop")
    ET.SubElement(out_shop, "name").text = "al-style.kz"
    curr = ET.SubElement(out_shop, "currencies")
    ET.SubElement(curr, "currency", {"id":"KZT","rate":"1"})

    cats_out = ET.SubElement(out_shop, "categories")
    if used_cat_ids and id2name:
        def level_of(x:str)->int:
            lv=0; cur=parent.get(x)
            while cur:
                lv+=1; cur=parent.get(cur)
            return lv
        for cid in sorted(used_cat_ids, key=level_of):
            if cid not in id2name: 
                continue
            attrs={"id":cid}
            pid = parent.get(cid)
            if pid: attrs["parentId"]=pid
            el = ET.SubElement(cats_out, "category", attrs)
            el.text = id2name[cid]

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
