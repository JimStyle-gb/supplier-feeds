# scripts/build_copyline.py
# -*- coding: utf-8 -*-
import os, re, io, sys
import pandas as pd
from xml.etree import ElementTree as ET

XLSX_PATH = os.path.join("docs", "copyline.xlsx")
KEEP_PATH = os.path.join("docs", "categories_copyline.txt")
OUT_PATH  = os.path.join("docs", "copyline.yml")

CURRENCY = "KZT"

BRAND_BUCKETS = {
    "CANON":    "CANON",
    "HP":       "HP",
    "HEWLETT":  "HP",
    "SAMSUNG":  "SAMSUNG",
    "XEROX":    "XEROX",
    "BROTHER":  "BROTHER",
    "EPSON":    "EPSON",
    "RICOH":    "RICOH",
    "KYOCERA":  "KYOCERA",
    "LEXMARK":  "LEXMARK",
    "PANTUM":   "PANTUM",
    "TOSHIBA":  "TOSHIBA",
}

def log(*a): print(*a, file=sys.stderr)

def normalize_header(v:str) -> str:
    v = str(v).strip()
    v = re.sub(r'\s+', ' ', v)
    return v.lower()

def find_header_row(df: pd.DataFrame) -> int:
    """
    Ищем строку, где одновременно встречаются что-то из:
    - 'номенклатура'
    - 'номенклатура.артикул' или 'артикул'
    - 'цена'
    Остаток желательно, но не обязателен.
    """
    for i in range(min(100, len(df))):
        row = [normalize_header(x) for x in df.iloc[i].tolist()]
        row_join = " | ".join(row)
        if ("номенклатура" in row_join) and (("номенклатура.артикул" in row_join) or ("артикул" in row_join)) and ("цена" in row_join):
            return i
    # запасной сценарий: ищем где встречается «номенклатура.артикул»
    for i in range(min(200, len(df))):
        row = [normalize_header(x) for x in df.iloc[i].tolist()]
        if any("номенклатура.артикул" in c or "артикул" in c for c in row):
            return i
    raise RuntimeError("Не удалось найти строку заголовков в XLSX (ищу Номенклатура / Номенклатура.Артикул / Цена).")

def pick_column(headers, candidates_contains):
    """
    headers: список нормализованных заголовков
    candidates_contains: список подстрок, любую из которых можно содержать
    Возвращает индекс столбца или None
    """
    for idx, h in enumerate(headers):
        for key in candidates_contains:
            if key in h:
                return idx
    return None

def load_copyline_xlsx(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"XLSX не найден: {path}")

    # Читаем БЕЗ заголовков, т.к. они могут быть не на первой строке
    raw = pd.read_excel(path, header=None, dtype=str, engine="openpyxl")
    log(f"[build_copyline] XLSX: {os.path.abspath(path)}")

    # Находим строку заголовков
    hrow = find_header_row(raw)
    headers_norm = [normalize_header(x) for x in raw.iloc[hrow].tolist()]
    # Данные ниже заголовка
    data = raw.iloc[hrow+1:].reset_index(drop=True).copy()
    # Подгоняем число столбцов
    ncols = len(headers_norm)
    data = data.iloc[:, :ncols]
    data.columns = headers_norm

    # Выбираем нужные столбцы
    i_name   = pick_column(headers_norm, ["номенклатура", "наименование", "товар", "название"])
    i_art    = pick_column(headers_norm, ["номенклатура.артикул", "артикул", "код"])
    i_price  = pick_column(headers_norm, ["цена", "опт"])
    i_stock  = pick_column(headers_norm, ["остаток", "наличие", "кол-во", "количество", "склад"])

    if i_name is None or i_art is None or i_price is None:
        raise RuntimeError(f"Не хватает колонок. Нашёл: name={i_name}, article={i_art}, price={i_price}, stock={i_stock}")

    df = pd.DataFrame({
        "name":  data.iloc[:, i_name],
        "article": data.iloc[:, i_art],
        "price_raw": data.iloc[:, i_price],
        "stock_raw": data.iloc[:, i_stock] if i_stock is not None else None,
    })

    # Чистим строки/пустые
    df["name"] = df["name"].astype(str).str.strip()
    df["article"] = df["article"].astype(str).str.strip()
    if "price_raw" in df:
        df["price_raw"] = df["price_raw"].astype(str).str.strip()

    # Убираем строки-разделители (нет артикула и цены)
    df = df[~(df["article"].isna() | (df["article"] == "nan") | (df["article"] == ""))]
    df = df[~(df["name"].isna() | (df["name"] == "nan") | (df["name"] == ""))]

    # Цена: убираем пробелы тысяч и запятую как десятичный
    def parse_price(x):
        s = str(x)
        if s in ("nan", "", None):
            return None
        s = s.replace(" ", "").replace("\u00A0", "").replace(",", ".")
        m = re.search(r"(\d+(\.\d{1,2})?)", s)
        if not m: return None
        try:
            return round(float(m.group(1)), 2)
        except:
            return None

    df["price"] = df["price_raw"].apply(parse_price)

    # Остаток → число и флаг in_stock
    def parse_stock(x):
        if x is None or str(x).lower() in ("nan", "", "-", "нет"):
            return 0
        s = str(x).strip()
        m = re.search(r"(\d+)", s)
        if m:
            return int(m.group(1))
        return 0

    if "stock_raw" in df and df["stock_raw"] is not None:
        df["stock_qty"] = df["stock_raw"].apply(parse_stock)
    else:
        df["stock_qty"] = 0

    df["in_stock"] = df["stock_qty"] > 0

    # Итог: только те, где есть цена (иначе Сату ругается)
    df = df[df["price"].notna()].reset_index(drop=True)
    return df

def load_keywords(path: str):
    if not os.path.exists(path):
        log(f"[build_copyline] KEEP file отсутствует: {os.path.abspath(path)} (возьмём все позиции)")
        return []
    with io.open(path, "r", encoding="utf-8") as f:
        kws = [ln.strip().lower() for ln in f if ln.strip()]
    log(f"[build_copyline] Loaded {len(kws)} keywords: {kws}")
    return kws

def filter_by_keywords(df: pd.DataFrame, kws):
    if not kws:
        return df
    mask = pd.Series(False, index=df.index)
    names = df["name"].str.lower()
    for kw in kws:
        mask = mask | names.str.contains(re.escape(kw), na=False)
    return df[mask].reset_index(drop=True)

def detect_brand_bucket(name: str) -> str:
    up = name.upper()
    for key, bucket in BRAND_BUCKETS.items():
        if key in up:
            return bucket
    return "OTHER"

def guess_image_url(name: str) -> str | None:
    """
    Пробуем вытащить «модель» из названия (DR-1075, TN-2075, 039, MLT-D101S и т.п.)
    и собираем прямую ссылку на фото Copyline:
    https://copyline.kz/components/com_jshopping/files/img_products/<MODEL>.jpg
    """
    cand = None
    tokens = re.findall(r"[A-Z0-9][A-Z0-9\-_/]{2,}", name.upper())
    # оставим токены где есть цифры (чаще это и есть модель)
    tokens = [t for t in tokens if re.search(r"\d", t)]
    if tokens:
        # берём самый «осмысленный» — самый длинный
        cand = max(tokens, key=len)
    if not cand:
        return None
    return f"https://copyline.kz/components/com_jshopping/files/img_products/{cand}.jpg"

def to_yml(df: pd.DataFrame) -> bytes:
    root = ET.Element("yml_catalog")
    shop = ET.SubElement(root, "shop")
    ET.SubElement(shop, "name").text = "copyline-xlsx"

    # currencies
    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id=CURRENCY, rate="1")

    # categories: корень + по брендам
    cats = ET.SubElement(shop, "categories")
    ROOT_ID = 9300000
    ET.SubElement(cats, "category", id=str(ROOT_ID)).text = "Copyline"

    brand_ids = {}
    next_id = 9500000
    for b in sorted(set(detect_brand_bucket(n) for n in df["name"])):
        bid = next_id; next_id += 1
        brand_ids[b] = bid
        ET.SubElement(cats, "category", id=str(bid), parentId=str(ROOT_ID)).text = b

    # offers
    offers = ET.SubElement(shop, "offers")
    for _, r in df.iterrows():
        oid = f"copyline:{r['article']}"
        offer = ET.SubElement(offers, "offer", id=oid,
                              available="true" if r["in_stock"] else "false",
                              in_stock="true" if r["in_stock"] else "false")
        ET.SubElement(offer, "name").text = str(r["name"])
        ET.SubElement(offer, "price").text = str(int(round(float(r["price"]))))
        ET.SubElement(offer, "currencyId").text = CURRENCY
        brand = detect_brand_bucket(str(r["name"]))
        ET.SubElement(offer, "categoryId").text = str(brand_ids.get(brand, ROOT_ID))
        ET.SubElement(offer, "vendorCode").text = str(r["article"])
        # Кол-во как информативные теги (Сату не обязательно их читает)
        ET.SubElement(offer, "quantity_in_stock").text = str(int(r["stock_qty"]))
        ET.SubElement(offer, "stock_quantity").text = str(int(r["stock_qty"]))
        ET.SubElement(offer, "quantity").text = str(int(r["stock_qty"]))
        # Картинка (если угадали по паттерну)
        pic = guess_image_url(str(r["name"]))
        if pic:
            ET.SubElement(offer, "picture").text = pic

    # Сериализация в CP1251 (windows-1251)
    xml_bytes = ET.tostring(root, encoding="windows-1251", xml_declaration=True)
    return xml_bytes

def main():
    if not os.path.exists(XLSX_PATH):
        raise SystemExit(f"ERROR: {XLSX_PATH} не найден.")
    kws = load_keywords(KEEP_PATH)
    df = load_copyline_xlsx(XLSX_PATH)
    df = filter_by_keywords(df, kws)
    xml_bytes = to_yml(df)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "wb") as f:
        f.write(xml_bytes)
    log(f"[build_copyline] OK → {os.path.abspath(OUT_PATH)} (items: {len(df)})")

if __name__ == "__main__":
    main()
