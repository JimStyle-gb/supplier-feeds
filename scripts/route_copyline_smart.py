# scripts/route_copyline_smart.py
from __future__ import annotations
import os, sys, re, csv
import xml.etree.ElementTree as ET

INPUT  = os.getenv("COPYLINE_YML", "docs/copyline.yml")
ENC    = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()
MAP_CSV = os.getenv("SATU_MAP_CSV", "docs/satu_map.csv")

# --- правила распознавания ---
RE_ORIG  = re.compile(r"\b(oem|original|genuine|оригинал\w*)\b", re.I)

RE_LASER = re.compile(r"(картридж|тонер|laser|лазерн)", re.I)
RE_DRUM  = re.compile(r"(drum|драм|фото?барабан)", re.I)
RE_DEV   = re.compile(r"(developer|прояв|девелоп)", re.I)
RE_INK   = re.compile(r"(струйн|ink|чернила|пигмент|dye)", re.I)
RE_FUSER = re.compile(r"(термоблок|fuser|печк|узел\s*печати|узел\s*закрепления|fixing|heat)", re.I)
RE_LAM   = re.compile(r"(ламинатор|laminator)", re.I)
RE_UTP   = re.compile(r"(витая\s*пара|utp|ftp|сетев(ой|ые)|ethernet\s*cable|кабель)", re.I)

BRANDS = [
    ("canon",   re.compile(r"\bcanon\b", re.I)),
    ("xerox",   re.compile(r"\bxerox\b", re.I)),
    ("hp",      re.compile(r"\b(hp|hewlett[\s\-]*packard)\b", re.I)),
    ("kyocera", re.compile(r"\bkyocera\b", re.I)),
    ("ricoh",   re.compile(r"\bricoh\b", re.I)),
    ("lexmark", re.compile(r"\blexmark\b", re.I)),
    ("pantum",  re.compile(r"\bpantum\b", re.I)),
    ("toshiba", re.compile(r"\btoshiba\b", re.I)),
]

def load_map_csv(path: str) -> dict[str,str]:
    if not os.path.exists(path):
        print(f"ERROR: нет файла маппинга {path}", file=sys.stderr)
        sys.exit(1)
    m = {}
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.reader(f):
            if not row or row[0].strip().startswith("#"): 
                continue
            if len(row) < 2: 
                continue
            key = row[0].strip()
            val = row[1].strip()
            if key and val:
                m[key] = val
    return m

def txt(el: ET.Element, tag: str) -> str:
    n = el.find(tag)
    return (n.text or "").strip() if n is not None and n.text else ""

def set_txt(el: ET.Element, tag: str, value: str):
    n = el.find(tag)
    if n is None:
        n = ET.SubElement(el, tag)
    n.text = value

def detect_brand(name_l: str, vendor_l: str) -> str | None:
    for key, rx in BRANDS:
        if rx.search(vendor_l) or rx.search(name_l):
            return key
    return None

def detect_key(name: str, vendor: str) -> str | None:
    """Вернём ключ категории (satu_key) либо None, если не распознано."""
    name_l  = (name or "").lower()
    vendor_l= (vendor or "").lower()

    brand = detect_brand(name_l, vendor_l)

    is_laserish = bool(RE_LASER.search(name_l) or RE_DRUM.search(name_l) or RE_DEV.search(name_l))
    is_ink      = bool(RE_INK.search(name_l))
    is_fuser    = bool(RE_FUSER.search(name_l))
    is_lam      = bool(RE_LAM.search(name_l))
    is_utp      = bool(RE_UTP.search(name_l))

    # Специальные ветки без бренда
    if is_fuser:
        return "fusers"
    if is_lam:
        return "laminators"
    if is_utp:
        return "utp_cables"

    # Брендовые правила
    if brand == "canon":
        if is_ink:
            return "canon_ink"
        if is_laserish:
            return "canon_oem" if (RE_ORIG.search(name or "") or RE_ORIG.search(vendor or "")) else "canon_compat"
        return None

    if brand == "xerox":
        if is_laserish:
            # по умолчанию — совместимые; если «оригинал» — отдельный ключ (может не быть в маппинге)
            return "xerox_oem" if (RE_ORIG.search(name or "") or RE_ORIG.search(vendor or "")) else "xerox_compat"
        return None

    if brand in {"hp","kyocera","ricoh","lexmark","pantum","toshiba"}:
        if is_laserish:
            return f"{brand}_laser"
        return None

    return None

def main():
    # 1) загрузка маппинга ключ -> текущий satu_id
    key2id = load_map_csv(MAP_CSV)

    # 2) парсим YML
    if not os.path.exists(INPUT):
        print(f"ERROR: нет входного файла {INPUT}", file=sys.stderr)
        sys.exit(1)

    tree = ET.parse(INPUT)
    root = tree.getroot()
    offers = root.findall(".//offers/offer")

    changed = 0
    for o in offers:
        name   = txt(o, "name")
        vendor = txt(o, "vendor")
        cat_id_old = txt(o, "categoryId")

        key = detect_key(name, vendor)
        if not key:
            continue  # не распознали — не трогаем

        satu_id = key2id.get(key, "").strip()
        if not satu_id:
            # ключ есть, но ID не задан — пропускаем и сигналим
            print(f"WARN: нет ID для ключа '{key}' (товар: {name})", file=sys.stderr)
            continue

        if satu_id != cat_id_old:
            set_txt(o, "categoryId", satu_id)
            changed += 1

    # 3) пишем обратно
    tree.write(INPUT, encoding=ENC, xml_declaration=True)
    print(f"route_copyline_smart: offers={len(offers)}, changed={changed}, enc={ENC}, map={MAP_CSV}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
