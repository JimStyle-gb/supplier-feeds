# -*- coding: utf-8 -*-
"""
build_copyline.py — генератор YML из XLSX Copyline с фильтром по ключевым словам
и подгрузкой главного фото с карточки товара (как в «похваленном» коде).

✓ Источник: XLSX_URL (env) — https://copyline.kz/files/price-CLA.xlsx
✓ Фильтр по ключам (docs/copyline_keywords.txt): drum, девелопер, драм, кабель сетевой, картридж, термоблок, термоэлемент, тонер-картридж
✓ Фото: с карточки товара — <img id="main_image_*"> → fallback og:image; нормализуем к виду full_*.jpg
✓ Название: чистим хвосты "(Артикул XXX)" / "(SKU ...)" / "(Код ...)", ограничиваем до 110 символов.
✓ SKU (vendorCode): обязателен; если только цифры — добавляем префикс 'C'.
✓ Цена > 0 — обязательна.
✓ Выход: docs/copyline.yml (Windows-1251), валюта KZT, с <categories> по классам товаров.

Зависимости: requests, beautifulsoup4, openpyxl
"""

from __future__ import annotations

import os
import re
import io
import time
import html
import json
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

# -------------------- ENV / Параметры --------------------
XLSX_URL        = os.getenv("XLSX_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE        = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")  # cp1251
KEYWORDS_FILE   = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT", "25"))

SUPPLIER_NAME   = os.getenv("SUPPLIER_NAME", "Copyline")
CURRENCY        = "KZT"

ROOT_CAT_ID     = 9300000
ROOT_CAT_NAME   = "Copyline"

UA = {"User-Agent": "Mozilla/5.0 (compatible; Copyline-XLSX/1.0)"}

# -------------------- Утилиты --------------------
def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def sanitize_title(s: str) -> str:
    """Убирает хвосты '(Артикул ...)/(SKU ...)/(Код ...)' и режет до 110 символов."""
    if not s:
        return ""
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:110].rstrip()

def normalize_vendor_code(v: Any) -> Optional[str]:
    """SKU обязателен; цифры → добавляем префикс 'C'."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    if re.fullmatch(r"\d+", s):
        return f"C{s}"
    return s

def to_number(x: Any) -> Optional[float]:
    """Пытаемся получить число из строки/ячейки."""
    if x is None:
        return None
    s = str(x).replace("\xa0", " ").strip().replace(" ", "").replace(",", ".")
    if not re.search(r"\d", s):
        return None
    try:
        return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        if m:
            try:
                return float(m.group(0))
            except Exception:
                return None
        return None

def load_keywords(path: str) -> List[str]:
    """Читаем ключевые слова (по одному на строку)."""
    kws: List[str] = []
    if os.path.isfile(path):
        with io.open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip().lower()
                if s and not s.startswith("#"):
                    kws.append(s)
    if not kws:
        kws = ["drum","девелопер","драм","кабель сетевой","картридж","термоблок","термоэлемент","тонер-картридж"]
    return kws

def title_has_keyword(title: str, kws: List[str]) -> bool:
    """Фильтр по ключам: сравниваем без пробелов/дефисов, с учётом рус./англ."""
    t = title.lower().replace("ё", "е")
    t = re.sub(r"[\s\-]+", "", t)
    for kw in kws:
        k = kw.lower().replace("ё", "е")
        k = re.sub(r"[\s\-]+", "", k)
        if k and k in t:
            return True
    return False

def normalize_img_to_full(url: Optional[str]) -> Optional[str]:
    """Нормализуем URL изображения к виду с 'full_' в имени файла (без скачивания)."""
    if not url:
        return None
    u = url.strip()
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        # без базового домена не знаем, оставляем как есть (обычно из product_url соберём абсолютный ниже)
        pass
    m = re.match(r"^(https?://[^/]+)(/.*/)([^/]+)$", u)
    if not m:
        return u
    host, path, fname = m.groups()
    if fname.startswith("full_"):
        return u
    if fname.startswith("thumb_"):
        fname = "full_" + fname[len("thumb_"):]
    else:
        fname = "full_" + fname
    return f"{host}{path}{fname}"

def abs_url(base: Optional[str], href: str) -> str:
    from urllib.parse import urljoin
    try:
        return urljoin(base or "", href)
    except Exception:
        return href

def extract_main_image(product_url: str) -> Optional[str]:
    """
    Тянем HTML карточки и возвращаем главный src:
      1) <img id="main_image_*"> (src / data-src)
      2) <meta property="og:image">
    Затем нормализуем к формату full_*.jpg.
    """
    try:
        r = requests.get(product_url, headers=UA, timeout=HTTP_TIMEOUT)
        if r.status_code != 200 or len(r.content) < 1500:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        img = soup.find("img", id=re.compile(r"^main_image_", re.I))
        src = None
        if img:
            src = img.get("src") or img.get("data-src")
        if not src:
            og = soup.find("meta", attrs={"property": "og:image"})
            if og and og.get("content"):
                src = og["content"].strip()
        if not src:
            return None
        src = abs_url(product_url, src)
        return normalize_img_to_full(src)
    except Exception:
        return None

# -------------------- Чтение XLSX --------------------
def fetch_xlsx_bytes(url: str) -> bytes:
    r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.content

def detect_headers(header_row: List[str]) -> Dict[str, int]:
    """
    Гибкий поиск столбцов. Покрываем типичные названия.
    Ищем первые совпадения и не перезатираем найденные.
    """
    idx: Dict[str, int] = {}
    patterns = {
        "vendor_code": r"^(артикул|sku|код|vendorcode)\b",
        "name":        r"^(наименование|название|товар|модель|model|name)\b",
        "price":       r"^(цена|price)\b",
        "currency":    r"^(валюта|currency)\b",
        "url":         r"^(url|ссылка|link)\b",
        "image":       r"^(фото|image|picture|картинка)\b",
        "category":    r"^(категори|group|группа)\b",
        "brand":       r"^(бренд|vendor|производитель|brand)\b",
        "description": r"^(описание|description|desc)\b",
    }
    for i, raw in enumerate(header_row):
        cell = str(raw or "").strip().lower()
        for key, patt in patterns.items():
            if re.search(patt, cell):
                idx.setdefault(key, i)
    return idx

# -------------------- Классификация категорий --------------------
def stable_cat_id(name: str, prefix: int = 9400000) -> int:
    h = hashlib.md5(name.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def classify_category(title: str) -> Tuple[int, str]:
    tl = title.lower()
    if any(w in tl for w in ["драм", "drum"]):
        return stable_cat_id("Драм-юниты"), "Драм-юниты"
    if "девелопер" in tl:
        return stable_cat_id("Девелоперы"), "Девелоперы"
    if "термоэлемент" in tl:
        return stable_cat_id("Термоэлементы"), "Термоэлементы"
    if "термоблок" in tl or "печка" in tl or "fuser" in tl:
        return stable_cat_id("Термоблоки"), "Термоблоки"
    if "кабель" in tl and "сет" in tl:
        return stable_cat_id("Сетевые кабели"), "Сетевые кабели"
    return stable_cat_id("Тонер-картриджи"), "Тонер-картриджи"

# -------------------- Генерация YML --------------------
def yml_escape(s: str) -> str:
    return html.escape(s or "")

def build_yml(categories: List[Tuple[int, str]], offers: List[Tuple[int, Dict[str, Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SUPPLIER_NAME.lower())}</name>")
    out.append('<currencies><currency id="KZT" rate="1" /></currencies>')

    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, cname in categories:
        out.append(f"<category id=\"{cid}\" parentId=\"{ROOT_CAT_ID}\">{yml_escape(cname)}</category>")
    out.append("</categories>")

    out.append("<offers>")
    for cid, it in offers:
        oid   = yml_escape(it["offer_id"])
        name  = yml_escape(it["title"])
        price = it["price"]
        price_txt = str(int(price)) if float(price).is_integer() else f"{price}"
        url   = yml_escape(it.get("url") or "")
        pic   = yml_escape(it.get("picture") or "")
        brand = yml_escape(it.get("brand") or SUPPLIER_NAME)
        vcode = yml_escape(it.get("vendorCode") or "")

        out.append(f'<offer id="{oid}" available="true" in_stock="true">')
        out.append(f"<name>{name}</name>")
        out.append(f"<vendor>{brand}</vendor>")
        if vcode:
            out.append(f"<vendorCode>{vcode}</vendorCode>")
        out.append(f"<price>{price_txt}</price>")
        out.append("<currencyId>KZT</currencyId>")
        out.append(f"<categoryId>{cid}</categoryId>")
        if url:
            out.append(f"<url>{url}</url>")
        if pic:
            out.append(f"<picture>{pic}</picture>")
        desc = it.get("description") or it["title"]
        # безопасно для cp1251: убираем экзотику
        desc = re.sub(r"[^\x00-\x7F\u0400-\u04FF]+", " ", desc)
        out.append(f"<description>{yml_escape(desc)}</description>")
        out.append("<quantity_in_stock>1</quantity_in_stock>")
        out.append("<stock_quantity>1</stock_quantity>")
        out.append("<quantity>1</quantity>")
        out.append("</offer>")
    out.append("</offers>")

    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# -------------------- Основная логика --------------------
def main() -> int:
    # 1) тянем XLSX
    xlsx_bytes = fetch_xlsx_bytes(XLSX_URL)
    wb = load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        print("[error] XLSX пуст.")
        return 2

    headers = [str(c or "").strip() for c in rows[0]]
    idx = detect_headers(headers)
    print("[info] headers:", json.dumps(idx, ensure_ascii=False))

    if "name" not in idx or "price" not in idx:
        print("[error] Нет обязательных столбцов 'Название'/'Цена'.")
        return 2

    keywords = load_keywords(KEYWORDS_FILE)
    print("[info] keywords:", keywords)

    categories: List[Tuple[int, str]] = []
    seen_cats: set[int] = set()
    offers: List[Tuple[int, Dict[str, Any]]] = []
    seen_offer_ids: set[str] = set()

    for r in rows[1:]:
        try:
            name_raw = str(r[idx["name"]] if idx.get("name") is not None and r[idx["name"]] is not None else "").strip()
            if not name_raw:
                continue
            title = sanitize_title(name_raw)

            # фильтр по ключам (по названию; при желании можно расширить на "category"/"description")
            if not title_has_keyword(title, keywords):
                continue

            price = to_number(r[idx["price"]] if idx.get("price") is not None else None)
            if price is None or price <= 0:
                continue

            vendor_code = normalize_vendor_code(r[idx["vendor_code"]]) if "vendor_code" in idx else None
            if not vendor_code:
                # без SKU не берём
                continue

            brand = str(r[idx["brand"]]).strip() if "brand" in idx and r[idx["brand"]] else ""
            url   = str(r[idx["url"]]).strip()   if "url"   in idx and r[idx["url"]]   else ""
            img   = str(r[idx["image"]]).strip() if "image" in idx and r[idx["image"]] else ""
            descr = str(r[idx["description"]]).strip() if "description" in idx and r[idx["description"]] else title

            # Фото: приоритет — страница товара; затем колонка image; всё нормализуем к full_*
            picture = None
            if url:
                picture = extract_main_image(url)
            if not picture and img and re.match(r"^https?://", img):
                picture = normalize_img_to_full(img)

            # Категория — по названию (эвристики)
            cid, cname = classify_category(title)
            if cid not in seen_cats:
                categories.append((cid, cname))
                seen_cats.add(cid)

            # offer id: приоритет — vendor_code; иначе стабильный хэш
            offer_id = vendor_code or ("C" + sha1(title)[:16])
            if offer_id in seen_offer_ids:
                continue
            seen_offer_ids.add(offer_id)

            offers.append((cid, {
                "offer_id":   offer_id,
                "title":      title,
                "price":      float(f"{price:.2f}"),
                "vendorCode": vendor_code,
                "brand":      brand or SUPPLIER_NAME,
                "url":        url,
                "picture":    picture,
                "description": descr or title,
            }))

        except Exception as e:
            # Пропускаем строку, если что-то пошло не так
            continue

    print(f"[stat] offers: {len(offers)}, categories: {len(categories)}")
    # 2) пишем YML
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    xml = build_yml(categories, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)
    print(f"[done] written: {OUT_FILE} ({OUTPUT_ENCODING})")
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e)
        sys.exit(2)
