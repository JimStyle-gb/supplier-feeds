# scripts/build_alstyle.py
from __future__ import annotations
import os, sys, urllib.request, xml.etree.ElementTree as ET
from copy import deepcopy

SOURCE_URL   = os.getenv("SOURCE_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php")
OUT_FILE     = os.getenv("OUT_FILE",  "docs/alstyle.yml")
ENC          = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
CATS_FILE    = os.getenv("CATEGORIES_FILE", "docs/categories_alstyle.txt")

def ensure_files():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    if not os.path.exists(CATS_FILE):
        with open(CATS_FILE, "w", encoding="utf-8") as f:
            f.write("# по одному ID или имени категории в строке\n# пример: 5649\n")

def load_filters(path):
    ids, names = set(), set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"): continue
            if s.isdigit(): ids.add(s)
            else: names.add(s.lower())
    return ids, names

def fetch_bytes(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()

def parse_xml_bytes(data: bytes):
    try: return ET.fromstring(data)
    except Exception:
        for enc in ("cp1251","utf-8"):
            try: return ET.fromstring(data.decode(enc,"replace"))
            except Exception: pass
    return None

def main():
    ensure_files()
    keep_ids, keep_names = load_filters(CATS_FILE)

    data = fetch_bytes(SOURCE_URL)
    src  = parse_xml_bytes(data)
    if src is None:
        root=ET.Element("yml_catalog"); shop=ET.SubElement(root,"shop")
        ET.SubElement(shop,"name").text="alstyle"
        ET.SubElement(shop,"categories"); ET.SubElement(shop,"offers")
        ET.ElementTree(root).write(OUT_FILE, encoding=ENC, xml_declaration=True)
        return

    shop        = src.find(".//shop") or ET.Element("shop")
    orig_cats   = src.find(".//categories")
    orig_offers = src.find(".//offers")

    id2cat, parent, children, name_by_id = {}, {}, {}, {}
    if orig_cats is not None:
        for c in orig_cats.findall("category"):
            cid=(c.get("id") or "").strip()
            pid=(c.get("parentId") or "").strip()
            nm =(c.text or "").strip()
            if not cid: continue
            id2cat[cid]=c; parent[cid]=pid; name_by_id[cid]=nm
            children.setdefault(pid,[]).append(cid)

    def descendants(root_id: str) -> set[str]:
        seen={root_id}; stack=[root_id]
        while stack:
            x=stack.pop()
            for y in children.get(x,[]):
                if y not in seen:
                    seen.add(y); stack.append(y)
        return seen

    allowed=set()
    for rid in keep_ids:
        if rid in id2cat:
            allowed |= descendants(rid)
    if keep_names:
        lowmap={cid:(name_by_id.get(cid,"").lower()) for cid in id2cat}
        for needle in keep_names:
            for cid,nm in lowmap.items():
                if nm == needle:
                    allowed |= descendants(cid)

    root = ET.Element("yml_catalog")
    out_shop = ET.SubElement(root,"shop")
    ET.SubElement(out_shop,"name").text = shop.findtext("name") or "alstyle"

    orig_curr = shop.find("currencies") or src.find(".//currencies")
    out_curr = deepcopy(orig_curr) if orig_curr is not None else ET.Element("currencies")
    if orig_curr is None:
        ET.SubElement(out_curr,"currency",{"id":"KZT","rate":"1"})
    out_shop.append(out_curr)

    out_cats = ET.SubElement(out_shop,"categories")
    if orig_cats is not None and allowed:
        for cid,c in id2cat.items():
            if cid in allowed:
                newc = ET.Element("category", {"id": c.get("id")})
                pid  = c.get("parentId")
                if pid: newc.set("parentId", pid)
                newc.text = c.text or ""
                out_cats.append(newc)

    out_offers = ET.SubElement(out_shop,"offers")
    if orig_offers is not None and allowed:
        for off in orig_offers.findall("offer"):
            cat_el=off.find("categoryId")
            catid=(cat_el.text.strip() if (cat_el is not None and cat_el.text) else "")
            if catid and catid in allowed:
                out_offers.append(deepcopy(off))

    ET.ElementTree(root).write(OUT_FILE, encoding=ENC, xml_declaration=True)

if __name__=="__main__":
    try: main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
