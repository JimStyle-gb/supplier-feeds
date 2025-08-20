# -*- coding: utf-8 -*-
"""
Генерит docs/copyline.yml из docs/copyline.xlsx (прайс Copyline),
фильтрует по docs/categories_copyline.txt (если файл есть),
ставит categoryId в ГРУППЫ САТУ (из списка, который ты дал),
и пытается найти 1 корректное фото по шаблону img_products/<КОД>.jpg.
Кодировка YML = windows-1251.
"""

import os, re, json, time, hashlib
from typing import Optional, Dict, Any, List
import requests
import pandas as pd
from xml.etree import ElementTree as ET

XLSX_PATH = "docs/copyline.xlsx"
OUTPUT_YML = "docs/copyline.yml"
FILTER_TXT = "docs/categories_copyline.txt"
IMG_CACHE = "docs/copyline_images.json"

COPYLINE_IMG_BASE = "https://copyline.kz/components/com_jshopping/files/img_products"

# ====== МАППИНГ КАТЕГОРИЙ САТУ (твои ID) ======
SATU_CAT = {
    # Картриджи
    "LASER_HP": 9457485,
    "LASER_SAMSUNG": 9457454,
    "LASER_XEROX_ORIG": 9457500,
    "LASER_XEROX_COMP": 9457495,
    "LASER_CANON_ORIG": 9457491,
    "LASER_CANON_COMP": 9457505,
    "LASER_RICOH": 9457483,
    "LASER_LEXMARK": 9457482,
    "LASER_KYOCERA": 9457480,
    "LASER_TOSHIBA": 9457476,
    "LASER_PANTUM": 9457456,
    "LASER_GENERIC": 9457435,  # на всякий

    # ЗИП общие
    "ZIP_ROOT": 9457440,
    "ZIP_SEP": 9457522,       # Сепараторы (тормозные площадки)
    "ZIP_DOCTOR": 9457523,    # Дозирующие лезвия
    "ZIP_WIPER": 9457524,     # Ракельные ножи
    "ZIP_PCR": 9457525,       # Валы заряда
    "ZIP_PICKUP": 9457526,    # Ролики захвата
    "ZIP_RUBBER": 9457527,    # Резиновые валы
    "ZIP_MAG_DEV": 9457528,   # Магнитные валы и валы проявки

    # ЗИП бренд-специфика (Samsung/Xerox/Pantum блоки)
    "ZIP_DOCTOR_SAMS": 9457529,
    "ZIP_RUBBER_XSP": 9457530,
    "ZIP_PICKUP_XSP": 9457531,
    "ZIP_SEP_XSP": 9457532,
    "ZIP_DEV_SAMS": 9457533,
    "ZIP_WIPER_SAMS": 9457534,
    "ZIP_PCR_SAMS": 9457535,
}

# ====== Утилиты ======
def load_cache() -> Dict[str, Any]:
    if os.path.exists(IMG_CACHE):
        try:
            with open(IMG_CACHE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache: Dict[str, Any]) -> None:
    try:
        with open(IMG_CACHE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def read_filters() -> List[str]:
    if not os.path.exists(FILTER_TXT):
        return []  # нет файла — значит не фильтруем
    lines = []
    with open(FILTER_TXT, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                lines.append(s.lower())
    return lines

def include_by_filters(name: str, filters: List[str]) -> bool:
    if not filters:
        return True
    nm = name.lower()
    for token in filters:
        if token in nm:
            return True
    return False

def sanitize_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    # Убираем лишние управляющие
    s = re.sub(r"[\x00-\x08\x0b-\x1f]+", " ", s)
    return s

def unique_id(art: str, name: str, seen: set) -> str:
    base = f"copyline:{art}".lower()
    uid = base
    if uid in seen:
        h = hashlib.md5((art + "|" + name).encode("utf-8")).hexdigest()[:6]
        uid = f"{base}-{h}"
    seen.add(uid)
    return uid

def parse_qty(s: Any) -> int:
    t = sanitize_text(s)
    if not t or t == "-":
        return 0
    m = re.search(r"(\d+)", t)
    if m:
        return int(m.group(1))
    return 0

def is_in_stock(s: Any) -> bool:
    return parse_qty(s) > 0

def detect_brand(name: str) -> Optional[str]:
    n = name.lower()
    for b in ["canon", "hp", "samsung", "xerox", "ricoh", "lexmark", "kyocera", "toshiba", "pantum"]:
        if b in n:
            return b
    return None

def is_original(name: str) -> bool:
    n = name.lower()
    return ("oem" in n) or ("ориг" in n) or ("original" in n)

def is_cartridge(name: str) -> bool:
    n = name.lower()
    return ("картридж" in n) or ("toner" in n) or ("тонер" in n)

def detect_zip_type(name: str) -> Optional[str]:
    n = name.lower()
    if "дозирующ" in n:
        return "DOCTOR"
    if "ракель" in n:
        return "WIPER"
    if "вал заряда" in n:
        return "PCR"
    if ("ролик" in n and "захват" in n) or "ролика захвата" in n:
        return "PICKUP"
    if "резиновый вал" in n or "прижимн" in n:
        return "RUBBER"
    if "вал проявки" in n or "подачи тонера" in n or "developer" in n:
        return "MAG_DEV"
    if "сепаратор" in n or "тормозная площадка" in n:
        return "SEP"
    return None

def map_category_id(name: str) -> int:
    brand = (detect_brand(name) or "").lower()
    if is_cartridge(name):
        if brand == "canon":
            return SATU_CAT["LASER_CANON_ORIG"] if is_original(name) else SATU_CAT["LASER_CANON_COMP"]
        if brand == "hp":
            return SATU_CAT["LASER_HP"]
        if brand == "samsung":
            return SATU_CAT["LASER_SAMSUNG"]
        if brand == "xerox":
            return SATU_CAT["LASER_XEROX_ORIG"] if is_original(name) else SATU_CAT["LASER_XEROX_COMP"]
        if brand == "ricoh":
            return SATU_CAT["LASER_RICOH"]
        if brand == "lexmark":
            return SATU_CAT["LASER_LEXMARK"]
        if brand == "kyocera":
            return SATU_CAT["LASER_KYOCERA"]
        if brand == "toshiba":
            return SATU_CAT["LASER_TOSHIBA"]
        if brand == "pantum":
            return SATU_CAT["LASER_PANTUM"]
        return SATU_CAT["LASER_GENERIC"]

    # ЗИП
    zt = detect_zip_type(name)
    if not zt:
        return SATU_CAT["ZIP_ROOT"]

    if (brand == "samsung") and zt == "DOCTOR":
        return SATU_CAT["ZIP_DOCTOR_SAMS"]
    if (brand == "samsung") and zt == "WIPER":
        return SATU_CAT["ZIP_WIPER_SAMS"]
    if (brand == "samsung") and zt == "PCR":
        return SATU_CAT["ZIP_PCR_SAMS"]
    if (brand == "samsung") and zt == "MAG_DEV":
        return SATU_CAT["ZIP_DEV_SAMS"]
    if (brand in ["samsung", "xerox", "pantum"]) and zt == "PICKUP":
        return SATU_CAT["ZIP_PICKUP_XSP"]
    if (brand in ["samsung", "xerox", "pantum"]) and zt == "RUBBER":
        return SATU_CAT["ZIP_RUBBER_XSP"]
    if (brand in ["samsung", "xerox", "pantum"]) and zt == "SEP":
        return SATU_CAT["ZIP_SEP_XSP"]

    return {
        "SEP": SATU_CAT["ZIP_SEP"],
        "DOCTOR": SATU_CAT["ZIP_DOCTOR"],
        "WIPER": SATU_CAT["ZIP_WIPER"],
        "PCR": SATU_CAT["ZIP_PCR"],
        "PICKUP": SATU_CAT["ZIP_PICKUP"],
        "RUBBER": SATU_CAT["ZIP_RUBBER"],
        "MAG_DEV": SATU_CAT["ZIP_MAG_DEV"],
    }.get(zt, SATU_CAT["ZIP_ROOT"])

def http_exists(url: str, timeout: float = 8.0) -> bool:
    try:
        r = requests.get(url, timeout=timeout, stream=True)
        ct = r.headers.get("Content-Type", "")
        return (r.status_code == 200) and ("image" in ct.lower())
    except Exception:
        return False

def guess_image_url(art: str, name: str) -> Optional[str]:
    # Кандидаты из артикула и "похожих" кодов в названии
    cands = set()

    art_clean = re.sub(r"[^A-Za-z0-9\-]+", "", art).upper()
    if art_clean:
        cands.add(art_clean)

    for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9\-]{2,}", name.upper()):
        if len(token) >= 3:
            cands.add(token)

    # Попытки расширений
    exts = [".jpg", ".JPG", ".jpeg", ".png"]
    for code in list(cands)[:8]:  # не безумствуем
        for ext in exts:
            url = f"{COPYLINE_IMG_BASE}/{code}{ext}"
            if http_exists(url):
                return url
    return None

def load_copyline_xlsx(xlsx_path: str) -> pd.DataFrame:
    # В этом XLSX заголовок в 2 строки, начинаются с 10-й (0-based: [9,10])
    df = pd.read_excel(xlsx_path, sheet_name="TDSheet", header=[9, 10])

    colmap = {
        ('Номенклатура', 'Unnamed: 0_level_1'): 'Номенклатура',
        ('1', 'Номенклатура.Артикул '): 'Артикул',
        ('Остаток', ' '): 'Остаток',
        ('ОПТ', 'Цена'): 'Цена',
        ('ОПТ', 'Ед.'): 'Ед.',
    }
    df.columns = [colmap.get(c, f'{c[0]}::{c[1]}') for c in df.columns]
    df = df[['Номенклатура', 'Артикул', 'Остаток', 'Цена', 'Ед.']].copy()

    # Валидные строки
    df = df[df['Артикул'].notna() & df['Цена'].notna()]

    # Типы
    df['Номенклатура'] = df['Номенклатура'].astype(str).str.strip()
    df['Артикул'] = df['Артикул'].astype(str).str.strip()
    df['Ед.'] = df['Ед.'].astype(str).str.strip()
    df['Цена'] = pd.to_numeric(df['Цена'], errors='coerce').fillna(0).astype(int)

    return df

def build_yml(rows: List[Dict[str, Any]]) -> bytes:
    root = ET.Element("yml_catalog")
    shop = ET.SubElement(root, "shop")
    ET.SubElement(shop, "name").text = "al-style.kz"
    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id="KZT", rate="1")
    # ВНИМАНИЕ: категории НЕ пишем, чтобы не плодить новые группы; только categoryId в офферах.
    offers = ET.SubElement(shop, "offers")

    for r in rows:
        o = ET.SubElement(offers, "offer", id=r["id"], available="true" if r["in_stock"] else "false", in_stock="true" if r["in_stock"] else "false")
        ET.SubElement(o, "name").text = r["name"]
        ET.SubElement(o, "price").text = str(r["price"])
        ET.SubElement(o, "currencyId").text = "KZT"
        ET.SubElement(o, "categoryId").text = str(r["categoryId"])
        ET.SubElement(o, "vendorCode").text = r["art"]
        # количество
        ET.SubElement(o, "quantity_in_stock").text = str(r["qty"])
        ET.SubElement(o, "stock_quantity").text = str(r["qty"])
        ET.SubElement(o, "quantity").text = str(r["qty"])
        # картинка
        if r.get("picture"):
            ET.SubElement(o, "picture").text = r["picture"]

    # сериализация в CP1251 (windows-1251) с xml декларацией
    xml_bytes = ET.tostring(root, encoding="windows-1251", xml_declaration=True)
    return xml_bytes

def main():
    if not os.path.exists(XLSX_PATH):
        raise SystemExit(f"ERROR: {XLSX_PATH} не найден.")

    filters = read_filters()  # если пусто — берём все
    cache = load_cache()
    df = load_copyline_xlsx(XLSX_PATH)

    seen = set()
    rows = []
    hit_img, miss_img = 0, 0

    for _, row in df.iterrows():
        name = sanitize_text(row["Номенклатура"])
        if not include_by_filters(name, filters):
            continue

        art = sanitize_text(row["Артикул"]) or "NA"
        price = int(row["Цена"]) if not pd.isna(row["Цена"]) else 0
        qty = parse_qty(row["Остаток"])
        instock = qty > 0
        cat_id = map_category_id(name)

        uid = unique_id(art, name, seen)

        # Картинка — кэш по артикулу
        pic = cache.get(art)
        if not pic:
            pic = guess_image_url(art, name)
            if pic:
                cache[art] = pic

        if pic:
            hit_img += 1
        else:
            miss_img += 1

        rows.append({
            "id": uid,
            "name": name,
            "art": art,
            "price": price,
            "qty": qty,
            "in_stock": instock,
            "categoryId": cat_id,
            "picture": pic
        })

    # Пишем кэш и YML
    save_cache(cache)
    yml_bytes = build_yml(rows)
    os.makedirs(os.path.dirname(OUTPUT_YML), exist_ok=True)
    with open(OUTPUT_YML, "wb") as f:
        f.write(yml_bytes)

    print(f"[copyline] rows: {len(rows)} | img ok: {hit_img} | img miss: {miss_img}")
    print(f"[copyline] output: {OUTPUT_YML}")

if __name__ == "__main__":
    main()
