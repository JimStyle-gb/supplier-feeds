#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, re, csv, json, html, time, pathlib
from pathlib import Path
import pandas as pd

# ---------------------------
# Константы путей
# ---------------------------
ROOT = Path(__file__).resolve().parents[1]
XLSX_PATH = ROOT / "docs" / "copyline.xlsx"
KEEP_FILE = ROOT / "docs" / "categories_copyline.txt"
OUT_YML = ROOT / "docs" / "copyline.yml"

# ---------------------------
# Утилиты
# ---------------------------
def read_text_safely(p: Path) -> str:
    if not p.exists():
        return ""
    data = p.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "cp1251", "windows-1251"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    # как fallback
    return data.decode(errors="ignore")

def load_keep_keywords(p: Path):
    raw = read_text_safely(p)
    words = []
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        words.append(s)
    return words

def norm_price(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    # "249 370,00" -> "249370.00" -> int
    s = s.replace(" ", "").replace("\u00A0","").replace(",", ".")
    try:
        val = float(s)
        return int(round(val))
    except Exception:
        return None

def parse_stock(x):
    # ">50", "<10", "1", "-", "Нет", ">" и т.д.
    if x is None or (isinstance(x,float) and pd.isna(x)):
        return 0
    s = str(x).strip()
    if not s or s == "-" or s.lower() in ("нет", "no", "none"):
        return 0
    s = s.replace(" ", "").replace("\u00A0","")
    # отрежем все нецифры
    m = re.search(r"(\d+)", s)
    if m:
        try:
            return int(m.group(1))
        except:
            return 0
    return 0

def as_cp1251(s: str) -> str:
    # В yml мы пишем bytes cp1251 — но текст готовим как str
    # (github pages отдаст в windows-1251)
    return s

# ---------------------------
# Роутинг в Satu
# ---------------------------
SATU = {
    # Samsung
    "laser_samsung": 9457454,
    # Canon
    "canon_oem":    9457491,  # Оригинальные Canon
    "canon_compat": 9457505,  # Совместимые Canon
}

OEM_MARKERS = re.compile(r"\b(OEM|Original|Оригинал|оригинал)\b", re.IGNORECASE)

def pick_category_id(name: str) -> int | None:
    n = name or ""
    n_low = n.lower()

    # Canon
    if "canon" in n_low:
        if OEM_MARKERS.search(n):
            return SATU["canon_oem"]
        return SATU["canon_compat"]

    # Samsung
    if "samsung" in n_low or "mlt-" in n_low or "scx-" in n_low:
        return SATU["laser_samsung"]

    # если бренд не распознан — можно вернуть None (товар пропустим)
    return None

# ---------------------------
# Фильтрация по списку keywords
# ---------------------------
def compile_keep_regex(words):
    if not words:
        return None
    parts = []
    for w in words:
        w = w.strip()
        if not w:
            continue
        parts.append(re.escape(w))
    if not parts:
        return None
    # ищем вхождение любого ключа (без учёта регистра)
    return re.compile("|".join(parts), re.IGNORECASE)

# ---------------------------
# Чтение XLSX (Copyline)
# ---------------------------
def load_copyline_xlsx(xlsx_path: Path) -> pd.DataFrame:
    # Прочтём первый лист, нормализуем имена колонок
    df = pd.read_excel(xlsx_path, header=None, dtype=str)
    # найдём строку заголовка
    header_row_idx = None
    for i in range(min(40, len(df))):
        row = " ".join([str(x) for x in df.iloc[i].tolist()])
        if "Номенклатура" in row and "Артикул" in row:
            header_row_idx = i
            break
    if header_row_idx is None:
        raise RuntimeError("Не нашёл строку заголовка с колонками 'Номенклатура' и 'Артикул'.")

    df = pd.read_excel(xlsx_path, header=header_row_idx)
    # нормализуем имена
    cols = {c: str(c).strip() for c in df.columns}
    df.rename(columns=cols, inplace=True)

    # Переименуем ключевые поля в стандарт
    # Возможные варианты: 'Номенклатура', 'Номенклатура.Артикул', 'Артикул', 'Остаток', 'Цена'
    def pick(colnames):
        for c in colnames:
            if c in df.columns:
                return c
        return None

    c_name   = pick(["Номенклатура", "Название", "Наименование"])
    c_art    = pick(["Номенклатура.Артикул", "Артикул", "Код"])
    c_stock  = pick(["Остаток", "Наличие"])
    c_price  = pick(["Цена", "ОПТ", "Цена, тг", "Цена тнг"])
    if not c_name or not c_art or not c_price:
        raise RuntimeError(f"Не хватает колонок. Нашёл: name={c_name}, article={c_art}, price={c_price}, stock={c_stock}")

    out = pd.DataFrame({
        "name":   df[c_name].astype(str).fillna(""),
        "article":df[c_art].astype(str).fillna(""),
        "stock":  df[c_stock] if c_stock in df else 0,
        "price":  df[c_price],
    })
    return out

# ---------------------------
# Генерация YML
# ---------------------------
def y(s):  # xml-escape
    return html.escape(str(s), quote=True)

def build_yml(rows: list[dict]) -> bytes:
    # Категории не публикуем (пусто), чтобы Satu не создавал группы
    parts = []
    parts.append("<?xml version='1.0' encoding='windows-1251'?>")
    parts.append("<yml_catalog><shop><name>al-style.kz</name><currencies><currency id=\"KZT\" rate=\"1\" /></currencies>")
    parts.append("<categories />")
    parts.append("<offers>")
    for r in rows:
        attrs = []
        attrs.append(f'id="copyline:{y(r["article"])}"')
        attrs.append(f'available="{str(r["available"]).lower()}"')
        attrs.append(f'in_stock="{str(r["in_stock"]).lower()}"')
        parts.append(f"<offer {' '.join(attrs)}>")
        parts.append(f"<name>{y(r['name'])}</name>")
        parts.append(f"<price>{y(r['price'])}</price>")
        parts.append("<currencyId>KZT</currencyId>")
        parts.append(f"<categoryId>{y(r['category_id'])}</categoryId>")
        parts.append(f"<vendorCode>{y(r['article'])}</vendorCode>")
        if r.get("picture"):
            parts.append(f"<picture>{y(r['picture'])}</picture>")
        parts.append(f"<quantity_in_stock>{y(r['qty'])}</quantity_in_stock>")
        parts.append(f"<stock_quantity>{y(r['qty'])}</stock_quantity>")
        parts.append(f"<quantity>{y(r['qty'])}</quantity>")
        parts.append("</offer>")
    parts.append("</offers></shop></yml_catalog>")
    text = "\n".join(parts)
    return text.encode("cp1251", errors="replace")

# ---------------------------
# main
# ---------------------------
def main():
    print(f"[build_copyline] XLSX: {XLSX_PATH}")
    if not XLSX_PATH.exists():
        print("ERROR: docs/copyline.xlsx не найден.", file=sys.stderr)
        sys.exit(1)

    # 1) ключи фильтра
    keep_words = load_keep_keywords(KEEP_FILE)
    print(f"[build_copyline] KEEP file: {KEEP_FILE}")
    print(f"[build_copyline] Loaded {len(keep_words)} keywords: {keep_words}")
    keep_re = compile_keep_regex(keep_words)

    # 2) загрузка прайса
    df = load_copyline_xlsx(XLSX_PATH)
    print(f"[build_copyline] Rows total: {len(df)}")

    kept = []
    kept_by_brand = {"canon":0, "samsung":0, "other":0}

    for _, row in df.iterrows():
        name = (row.get("name") or "").strip()
        article = (row.get("article") or "").strip()
        price = norm_price(row.get("price"))
        stock = parse_stock(row.get("stock"))

        if not name or not article or price is None:
            continue

        # 3) фильтр по KEEP
        hay = f"{name} {article}"
        if keep_re and not keep_re.search(hay):
            continue

        # 4) категоризация
        cat_id = pick_category_id(name)
        if cat_id is None:
            # нет правил под бренд — пропускаем
            continue

        # счётчики для отладки
        nl = name.lower()
        if "canon" in nl:
            kept_by_brand["canon"] += 1
        elif "samsung" in nl or "mlt-" in nl or "scx-" in nl:
            kept_by_brand["samsung"] += 1
        else:
            kept_by_brand["other"] += 1

        kept.append({
            "name": name,
            "article": article,
            "price": price,
            "qty": max(stock, 0),
            "available": stock > 0,
            "in_stock": stock > 0,
            "category_id": cat_id,
            "picture": None,  # (пока без картинок — вопрос был в фильтре)
        })

    print(f"[build_copyline] Kept rows: {len(kept)} (brand split: {kept_by_brand})")

    # 5) yml
    OUT_YML.parent.mkdir(parents=True, exist_ok=True)
    OUT_YML.write_bytes(build_yml(kept))
    print(f"[build_copyline] Wrote: {OUT_YML} ({OUT_YML.stat().st_size} bytes)")

if __name__ == "__main__":
    main()
