# scripts/route_copyline_categories.py
from __future__ import annotations
import os, sys, re
import xml.etree.ElementTree as ET

INPUT  = os.getenv("COPYLINE_YML", "docs/copyline.yml")
ENC    = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

# Твои группы в Satu
CANON_COMPAT = "9456124"  # Совместимые Canon
CANON_ORIG   = "9456123"  # Оригинальные Canon
XEROX_COMPAT = "9456121"  # Совместимые Xerox

ORIG_WORDS = re.compile(r"\b(oem|original|genuine|оригинал\w*)\b", re.I)
IS_CART    = re.compile(r"(картридж|тонер|drum|драм|фото?барабан|developer|узел прояв|unit)", re.I)

def text(el, tag):
    n = el.find(tag)
    return (n.text or "").strip() if n is not None and n.text else ""

def set_text(el, tag, val):
    n = el.find(tag)
    if n is None:
        n = ET.SubElement(el, tag)
    n.text = val

def main():
    if not os.path.exists(INPUT):
        print(f"ERROR: нет файла {INPUT}", file=sys.stderr)
        sys.exit(1)

    tree = ET.parse(INPUT)
    root = tree.getroot()
    offers = root.findall(".//offers/offer")

    changed = 0
    for o in offers:
        name = text(o, "name")
        vendor = text(o, "vendor")
        cat_old = text(o, "categoryId")

        name_l = name.lower()
        vend_l = (vendor or "").lower()
        looks_cart = bool(IS_CART.search(name_l))

        # --- CANON ---
        if ("canon" in vend_l) or (" canon" in " " + name_l):
            if looks_cart:
                new_cat = CANON_ORIG if (ORIG_WORDS.search(name) or ORIG_WORDS.search(vendor)) else CANON_COMPAT
                if new_cat and new_cat != cat_old:
                    set_text(o, "categoryId", new_cat); changed += 1
                continue

        # --- XEROX ---
        if ("xerox" in vend_l) or (" xerox" in " " + name_l):
            if looks_cart:
                # по умолчанию совместимые; «оригинал» — оставляем как было
                if not (ORIG_WORDS.search(name) or ORIG_WORDS.search(vendor)):
                    if XEROX_COMPAT != cat_old:
                        set_text(o, "categoryId", XEROX_COMPAT); changed += 1
                continue

    tree.write(INPUT, encoding=ENC, xml_declaration=True)
    print(f"route_copyline_categories: offers={len(offers)}, changed={changed}, enc={ENC}")

if __name__ == "__main__":
    main()
