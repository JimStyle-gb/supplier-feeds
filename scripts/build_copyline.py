# -*- coding: utf-8 -*-
"""
Генератор YML для Satu.kz из прайса Copyline + фото с сайта copyline.kz.

Что делает:
1) Скачивает XLSX, находит шапку (поддержка двустрочной 'Номенклатура / Номенклатура.Артикул' + 'ОПТ / Цена').
2) Берёт товары, фильтрует по ключам (docs/copyline_keywords.txt), цена > 0.
3) Собирает с сайта список карточек /goods/*.html (через sitemap; fallback — обход сайта),
   парсит артикул (SKU) и главное фото (<img id="main_image_*"> или og:image), нормализует к 'full_*'.
4) Мёрджит товары из XLSX с карточками по артикулу. Если фото/URL не найден — товар пропускается.
5) Пишет docs/copyline.yml (Windows-1251), валюта KZT, категории определяет по названию (тонер, драм и т.п.).

Зависимости: requests, beautifulsoup4, openpyxl
"""

from __future__ import annotations
import os, re, io, time, html, hashlib, random, xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

# -------- ENV / конфиг --------
BASE_URL           = "https://copyline.kz"
XLSX_URL           = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
KEYWORDS_FILE      = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE           = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING    = os.getenv("OUTPUT_ENCODING", "windows-1251")
HTTP_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES          = int(os.getenv("MIN_BYTES", "1200"))
MAX_SITEMAP_URLS   = int(os.getenv("MAX_SITEMAP_URLS", "8000"))
MAX_VISIT_PAGES    = int(os.getenv("MAX_VISIT_PAGES", "2000"))

SUPPLIER_NAME = "Copyline"
CURRENCY      = "KZT"

ROOT_CAT_ID   = 9300000
ROOT_CAT_NAME = "Copyline"

UA = {"User-Agent": "Mozilla/5.0 (compatible; Copyline-XLSX-Site/1.0)"}

# -------- общие утилиты --------
def jitter_sleep(ms: int) -> None:
    base = ms / 1000.0
    time.sleep(max(0.0, base + random.uniform(-0.15, 0.15) * base))

def http_get(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None
        b = r.content
        if len(b) < MIN_BYTES:
            return None
        return b
    except Exception:
        return None

def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "html.parser")

def yml_escape(s: str) -> str: return html.escape(s or "")

def sanitize_title(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:110].rstrip()

def to_number(x: Any) -> Optional[float]:
    if x is None: return None
    s = str(x).replace("\xa0", " ").strip().replace(" ", "").replace(",", ".")
    if not re.search(r"\d", s): return None
    try:
        return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        if m:
            try: return float(m.group(0))
            except: return None
        return None

def normalize_vendor_code(v: Any) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    if not s or s.lower() == "nan": return None
    return s

def add_c_prefix_if_digits(vcode: str) -> str:
    """Для YML: если артикул — только цифры, добавляем префикс 'C' (только в вывод)."""
    if re.fullmatch(r"\d+", vcode):
        return f"C{vcode}"
    return vcode

def key_for_match(v: str) -> str:
    """Ключ сопоставления SKU: убираем всё, кроме букв/цифр; в верхний регистр."""
    return re.sub(r"[^A-Za-z0-9]+", "", v or "").upper()

def sha1(s: str) -> str: return hashlib.sha1(s.encode("utf-8")).hexdigest()

# -------- ключевые слова / категории --------
def load_keywords(path: str) -> List[str]:
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
    t = title.lower().replace("ё", "е")
    t = re.sub(r"[\s\-]+", "", t)
    for kw in kws:
        k = kw.lower().replace("ё", "е")
        k = re.sub(r"[\s\-]+", "", k)
        if k and k in t:
            return True
    return False

def classify_category(title: str) -> Tuple[int, str]:
    tl = title.lower()
    if any(w in tl for w in ["драм","drum"]): return stable_cat_id("Драм-юниты"), "Драм-юниты"
    if "девелопер" in tl:                     return stable_cat_id("Девелоперы"), "Девелоперы"
    if "термоэлемент" in tl:                  return stable_cat_id("Термоэлементы"), "Термоэлементы"
    if "термоблок" in tl or "печка" in tl or "fuser" in tl:
                                               return stable_cat_id("Термоблоки"), "Термоблоки"
    if "кабель" in tl and "сет" in tl:        return stable_cat_id("Сетевые кабели"), "Сетевые кабели"
    return stable_cat_id("Тонер-картриджи"), "Тонер-картриджи"

def stable_cat_id(name: str, prefix: int = 9400000) -> int:
    h = hashlib.md5(name.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

# -------- XLSX parser (двухстрочная шапка) --------
def fetch_xlsx_bytes(url: str) -> bytes:
    r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.content

def detect_header_two_row(rows: List[List[Any]], scan_rows: int = 60) -> Tuple[int,int,Dict[str,int]]:
    """
    Возвращает (row0, row1, mapping) — индексы двух строк шапки и индексы колонок:
      mapping['name'], mapping['vendor_code'], mapping['price'].
    Грубая эвристика под файл Copyline: строка с "Номенклатура" + следующая со "Артикул"/"Цена".
    """
    def low(x): return str(x or "").strip().lower()
    for i in range(min(scan_rows, len(rows)-1)):
        row0 = [low(c) for c in rows[i]]
        row1 = [low(c) for c in rows[i+1]]
        if any("номенклатура" in c for c in row0):
            # кол-во колонок может отличаться, ищем по текстам
            name_col = next((j for j,c in enumerate(row0) if "номенклатура" in c), None)
            # во второй строке ищем 'артикул' и 'цена'
            vendor_col = next((j for j,c in enumerate(row1) if "артикул" in c), None)
            price_col  = next((j for j,c in enumerate(row1) if "цена" in c), None)
            if name_col is not None and (vendor_col is not None) and (price_col is not None):
                return i, i+1, {"name": name_col, "vendor_code": vendor_col, "price": price_col}
    # fallback: попробуем первую найденную «цена» во второй строке
    for i in range(min(scan_rows, len(rows)-1)):
        row0 = [low(c) for c in rows[i]]
        row1 = [low(c) for c in rows[i+1]]
        name_col = next((j for j,c in enumerate(row0) if any(k in c for k in ["номенклатура","наименован"])), None)
        price_col = next((j for j,c in enumerate(row1) if "цена" in c or "опт" in c), None)
        vendor_col = next((j for j,c in enumerate(row1) if "артикул" in c), None)
        if name_col is not None and price_col is not None and vendor_col is not None:
            return i, i+1, {"name": name_col, "vendor_code": vendor_col, "price": price_col}
    return -1, -1, {}

def extract_sku_from_name(name: str) -> Optional[str]:
    """Пытаемся выцепить SKU из названия: форматы типа DR-2300, CF226A и т.п."""
    t = name.upper()
    tokens = re.findall(r"[A-ZА-Я0-9]{2,}(?:[-/–][A-ZА-Я0-9]{2,})?", t)
    for tok in tokens:
        if re.search(r"[A-ZА-Я]", tok) and re.search(r"\d", tok):
            return tok.replace("–","-").replace("/","-")
    return None

# -------- сайт: сбор карточек и фото --------
PRODUCT_RE = re.compile(r"/goods/[^/]+\.html$")

def normalize_img_to_full(url: Optional[str]) -> Optional[str]:
    if not url: return None
    u = url.strip()
    if u.startswith("//"): u = "https:" + u
    if u.startswith("/"):  u = BASE_URL + u
    m = re.match(r"^(https?://[^/]+)(/.*/)([^/]+)$", u)
    if not m: return u
    host, path, fname = m.groups()
    if fname.startswith("full_"): return u
    if fname.startswith("thumb_"): fname = "full_" + fname[len("thumb_"):]
    else: fname = "full_" + fname
    return f"{host}{path}{fname}"

def parse_product_page(url: str) -> Optional[Tuple[str, str]]:
    """Возвращает (vendor_code, picture_url) или None."""
    jitter_sleep(REQUEST_DELAY_MS)
    b = http_get(url)
    if not b: return None
    s = soup_of(b)

    # SKU
    sku = None
    skuel = s.find(attrs={"itemprop": "sku"})
    if skuel:
        val = (skuel.get_text(" ", strip=True) or "").strip()
        if val: sku = val
    if not sku:
        txt = s.get_text(" ", strip=True)
        m = re.search(r"(?:Артикул|SKU|Код)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})", txt, flags=re.I)
        if m: sku = m.group(1)
    if not sku: return None

    # picture
    src = None
    imgel = s.find("img", id=re.compile(r"^main_image_", re.I))
    if imgel and (imgel.get("src") or imgel.get("data-src")):
        src = imgel.get("src") or imgel.get("data-src")
    if not src:
        ogi = s.find("meta", attrs={"property": "og:image"})
        if ogi and ogi.get("content"):
            src = ogi["content"].strip()
    if not src: return None
    pic = normalize_img
