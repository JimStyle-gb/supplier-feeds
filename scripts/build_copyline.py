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
    "CANON": "CANON", "HP": "HP", "HEWLETT": "HP",
    "SAMSUNG": "SAMSUNG", "XEROX": "XEROX", "BROTHER": "BROTHER",
    "EPSON": "EPSON", "RICOH": "RICOH", "KYOCERA": "KYOCERA",
    "LEXMARK": "LEXMARK", "PANTUM": "PANTUM", "TOSHIBA": "TOSHIBA",
}

def log(*a): print(*a, file=sys.stderr)

def nrm(x:str) -> str:
    x = "" if x is None else str(x)
    x = x.replace("\u00A0"," ").strip()
    x = re.sub(r"\s+"," ",x)
    return x

def nrm_lc(x:str) -> str: return nrm(x).lower()

def find_header_row_and_build_headers(raw: pd.DataFrame):
    """
    Ищем 1-ю строку шапки, допускаем 2-строчную шапку (склейка через пробел).
    """
    # ищем первую строку, где есть что-то из ключевых слов
    key_hits = []
    for i in range(min(200, len(raw))):
        row = [nrm_lc(v) for v in raw.iloc[i].tolist()]
        joined = " | ".join(row)
        score = 0
        score += 1 if "номенклатура" in joined or "наименование" in joined or "товары" in joined else 0
        score += 1 if "артикул" in joined else 0
        score += 1 if "цена" in joined or "опт" in joined else 0
        if score >= 2:
            key_hits.append((i, score))
    if not key_hits:
        raise RuntimeError("Не нашёл строку шапки (жду 'Номенклатура/Артикул/Цена/ОПТ').")
    h0 = min(key_hits, key=lambda t: t[0])[0]

    # формируем заголовки: склеиваем строку h0 и (если есть) h0+1
    base = [nrm(raw.iloc[h0, j]) for j in range(raw.shape[1])]
    add  = [nrm(raw.iloc[h0+1, j]) if h0+1 < len(raw) else "" for j in range(raw.shape[1])]

    headers = []
    for a,b in zip(base, add):
        cell = " ".join([t for t in (a,b) if t]).strip()
        headers.append(nrm_lc(cell))
    # нормализации
    headers = [h.replace("  "," ") for h in headers]
    return h0, headers

def pick_column(headers, contains_list):
    for idx, h in enumerate(headers):
        for key in contains_list:
            if key in h:
                return idx
    return None

def parse_price(x):
    s = nrm(x).replace(" ", "").replace(",", ".")
    m = re.search(r"(\d+(\.\d{1,2})?)", s)
    if not m: return None
    try: return round(float(m.group(1)), 2)
    except: return None

def parse_stock(x):
    s = nrm_lc(x)
    if s in ("", "-", "нет", "nan"): return 0
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else 0

def load_copyline_xlsx(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"XLSX не найден: {path}")
    raw = pd.read_excel(path, header=None, dtype=str, engine="openpyxl")
    log(f"[build_copyline] XLSX: {os.path.abspath(path)}")

    h0, headers = find_header_row_and_build_headers(raw)
    data = raw.iloc[h0+2:].reset_index(drop=True).copy()

    # ровняем кол-во столбцов
    data = data.iloc[:, :len(headers)]
    data.columns = headers

    i_name  = pick_column(headers, ["номенклатура", "наименование", "товары", "название"])
    i_art   = pick_column(headers, ["номенклатура.артикул", "артикул", "код"])
    i_price = pick_column(headers, ["цена", "опт"])
    i_stock = pick_column(headers, ["остаток", "наличие", "кол-во", "количество", "склад"])

    if i_name is None or i_art is None or i_price is None:
        raise RuntimeError(f"Не хватает колонок. name={i_name}, article={i_art}, price={i_price}, stock={i_stock}")

    df = pd.DataFrame({
        "name":   data.iloc[:, i_name].astype(str).map(nrm),
        "article":data.iloc[:, i_art].astype(str).map(nrm),
        "price_raw": data.iloc[:, i_price].astype(str),
        "stock_raw": data.iloc[:, i_stock].astype(str) if i_stock is not None else "",
    })

    # фильтр пустых
    df = df[(df["name"]!="") & (df["article"]!="")]

    df["price"] = df["price_raw"].map(parse_price)
    df["stock_qty"] = df["stock_raw"].map(parse_stock) if "stock_raw" in df else 0
    df["in_stock"] = df["stock_qty"] > 0

    # нужна цена
    df = df[df["price"].notna()].reset_index(drop=True)
    log(f"[build_copyline] rows after parse: {len(df)}")
    return df

def load_keywords(path: str):
    if not os.path.exists(path):
        log("[build_copyline] KEEP отсутствует → берём все позиции")
        return []
    # читаем и в UTF-8, и fallback CP1251
    content = None
    for enc in ("utf-8", "cp1251"):
        try:
            with io.open(path, "r", encoding=enc) as f:
                content = f.read()
            break
        except Exception:
            continue
    if content is None:
        log("[build_copyline] Не удалось прочитать KEEP (utf-8/cp1251). Игнорирую фильтр.")
        return []
    kws = [ln.strip().lower() for ln in content.splitlines() if ln.strip()]
    log(f"[build_copyline] KEEP keywords: {kws}")
    return kws

def filter_by_keywords(df: pd.DataFrame, kws):
    if not kws: return df
    names = df["name"].str.lower().fillna("")
    mask = False
    for kw in kws:
        mask = mask | names.str.contains(re.escape(kw), na=False)
    out = df[mask].reset_index(drop=True)
    log(f"[build_copyline] after KEEP filter: {len(out)}")
    return out

def detect_brand_bucket(name: str) -> str:
    up = (name or "").upper()
    for key,bucket in BRAND_BUCKETS.items():
        if key in up: return bucket
    return "OTHER"

def guess_image_url(name: str) -> str | None:
    tokens = re.findall(r"[A-Z0-9][A-Z0-9\-_/]{2,}", (name or "").upper())
    tokens = [t for t in tokens if re.search(r"\d", t)]
    if not tokens: return None
    model = max(tokens, key=len)
    return f"https://copyline.kz/components/com_jshopping/files/img_products/{model}.jpg"

def to_yml(df: pd.DataFrame) -> bytes:
    root = ET.Element("yml_catalog")
    shop = ET.SubElement(root, "shop")
    ET.SubElement(shop, "name").text = "copyline-xlsx"

    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id=CURRENCY, rate="1")

    cats = ET.SubElement(shop, "categories")
    ROOT_ID = 9300000
    ET.SubElement(cats, "category", id=str(ROOT_ID)).text = "Copyline"
    brand_ids, next_id = {}, 9500000
    for b in sorted(set(detect_brand_bucket(n) for n in df["name"])):
        bid = next_id; next_id += 1
        brand_ids[b] = bid
        ET.SubElement(cats, "category", id=str(bid), parentId=str(ROOT_ID)).text = b

    offers = ET.SubElement(shop, "offers")
    for _, r in df.iterrows():
        oid = f"copyline:{r['article']}"
        o = ET.SubElement(offers, "offer", id=oid,
                          available="true" if r["in_stock"] else "false",
                          in_stock="true" if r["in_stock"] else "false")
        ET.SubElement(o, "name").text = str(r["name"])
        ET.SubElement(o, "price").text = str(int(round(float(r["price"]))))
        ET.SubElement(o, "currencyId").text = CURRENCY
        ET.SubElement(o, "categoryId").text = str(brand_ids.get(detect_brand_bucket(str(r["name"])), ROOT_ID))
        ET.SubElement(o, "vendorCode").text = str(r["article"])
        ET.SubElement(o, "quantity_in_stock").text = str(int(r["stock_qty"]))
        ET.SubElement(o, "stock_quantity").text = str(int(r["stock_qty"]))
        ET.SubElement(o, "quantity").text = str(int(r["stock_qty"]))
        pic = guess_image_url(str(r["name"]))
        if pic: ET.SubElement(o, "picture").text = pic

    return ET.tostring(root, encoding="windows-1251", xml_declaration=True)

def main():
    if not os.path.exists(XLSX_PATH):
        raise SystemExit(f"ERROR: {XLSX_PATH} не найден.")
    df = load_copyline_xlsx(XLSX_PATH)
    kws = load_keywords(KEEP_PATH)
    df_keep = filter_by_keywords(df, kws) if kws else df
    if kws and len(df_keep) == 0:
        log("[build_copyline] KEEP дал 0 записей → отключаю фильтр (беру всё, чтобы не был пустой YML).")
        df_keep = df
    xml_bytes = to_yml(df_keep)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "wb") as f:
        f.write(xml_bytes)
    log(f"[build_copyline] DONE → {os.path.abspath(OUT_PATH)} items={len(df_keep)}")

if __name__ == "__main__":
    main()
