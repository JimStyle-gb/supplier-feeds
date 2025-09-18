# -*- coding: utf-8 -*-
"""
Copyline → Satu YML (простой конвейер по твоим правилам)
script_version = copyline-2025-09-18.1

Шаги:
1) Читаем XLSX, собираем «двухстрочную» шапку (там есть «Номенклатура.Артикул»).
2) Оставляем только товары: есть артикул и цена > 0 (категории/подкатегории отпадают).
3) Фильтр: берём ТОЛЬКО те товары, чьи названия НАЧИНАЮТСЯ с любого слова из docs/copyline_keywords.txt.
4) По каждому артикулу ищем карточку на сайте copyline.kz, тянем фото/описание/характеристики/бренд.
5) В YML записываем: name, picture, vendor (бренд), description (включая «Технические характеристики»), vendorCode=CL+артикул, price (из XLSX), currencyId=KZT, available=true.
6) Вставляем FEED_META с русскими комментариями и аккуратным выравниванием.

Зависимости: openpyxl, requests, beautifulsoup4
"""

from __future__ import annotations
import os, re, io, time, html, json, hashlib, unicodedata
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone
from xml.etree import ElementTree as ET
from urllib.parse import urlencode, urljoin, quote

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

# ===================== НАСТРОЙКИ/ENV =====================

SCRIPT_VERSION = "copyline-2025-09-18.1"

BASE_URL        = os.getenv("BASE_URL", "https://copyline.kz")
XLSX_URL        = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
OUT_FILE        = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

KEYWORDS_FILE   = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")

HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT", "25"))
RETRIES         = int(os.getenv("RETRIES", "3"))
REQUEST_DELAY_S = float(os.getenv("REQUEST_DELAY_S", "0.2"))

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "CL")

# ===================== УТИЛИТЫ =====================

def log(msg: str) -> None:
    print(msg, flush=True)

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def now_almaty_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %z")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S +05")

def nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")

def one_line(s: str) -> str:
    """Схлопываем пробелы/переносы в одну строку и чистим мусорные «Артикул/Благотворительность»."""
    if not s: return ""
    s = nfkc(s).replace("\r", "\n").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"(?:^|\s)(Артикул|Благотворительность)\s*:\s*[^;.,]+[;.,]?\s*", " ", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip()

def normalize_name(s: str) -> str:
    s = nfkc(s).replace("\xa0"," ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_lower(s: str) -> str:
    s = nfkc(s).lower().replace("ё","е")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_float(val) -> Optional[float]:
    if val is None: return None
    s = str(val).replace("\xa0"," ").replace(" ","").replace(",",".")
    if not re.search(r"\d", s): return None
    try: v=float(s); return v if v>0 else None
    except: return None

def sha12(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]

# ===================== ЧТЕНИЕ КЛЮЧЕВЫХ СЛОВ =====================

def load_prefixes(path: str) -> List[str]:
    """Читаем keywords с авто-кодировкой. Пустые/комменты пропускаем."""
    if not os.path.exists(path): return []
    data = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path, "r", encoding=enc) as f:
                data = f.read()
            break
        except Exception:
            continue
    if data is None:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read()
    out=[]
    for line in data.splitlines():
        s=line.strip()
        if not s or s.startswith("#"): continue
        out.append(norm_lower(s))
    return out

def name_starts_with_any(name: str, prefixes: List[str]) -> bool:
    """Строгий префикс: название ДОЛЖНО НАЧИНАТЬСЯ с любого из ключей (после очистки ведущих знаков)."""
    if not prefixes: return True
    n = norm_lower(name)
    n = re.sub(r'^[\s\-\–\—•·|:/\\\[\]\(\)«»"“”„\']+', "", n)
    return any(n.startswith(p) for p in prefixes)

# ===================== XLSX: ШАПКА И ДАННЫЕ =====================

NAME_KEYS  = {"name","наименование","название","товар","наименование товара","полное наименование","товары"}
SKU_KEYS   = {"артикул","sku","код","номенклатура.артикул"}
PRICE_KEYS = {"цена","цена, тг","цена тг","цена (тг)","стоимость","dealer","закуп","b2b","price","опт","розница"}

def best_header_map(ws) -> Tuple[Dict[int,str], int]:
    """Находим наилучшую строку шапки. Пробуем одну строку r и r+r+1."""
    max_row = ws.max_row or 0
    def row_vals(r: int) -> List[str]:
        out=[]
        for c in range(1, min(ws.max_column or 0, 80) + 1):
            v = ws.cell(row=r, column=c).value
            out.append("" if v is None else str(v).strip())
        return out

    def map_from(vals: List[str]) -> Dict[int,str]:
        m={}
        for i,raw in enumerate(vals, start=1):
            k = norm_lower(raw)
            if not k: continue
            if k in NAME_KEYS: m[i]="name"
            elif k in SKU_KEYS: m[i]="sku"
            elif k in PRICE_KEYS: m[i]="price"
        return m

    best, best_row, best_score = {}, -1, -1
    for r in range(1, min(max_row, 80)+1):
        a = row_vals(r)
        m1 = map_from(a); s1=len(m1)
        if r+1 <= max_row:
            b = row_vals(r+1)
            merged = [(b[i] or a[i]) if i < min(len(a),len(b)) else (b[i] if i<len(b) else "") for i in range(max(len(a),len(b)))]
            m2 = map_from(merged); s2=len(m2)
        else:
            m2 = {}; s2=-1
        if s1>best_score and "name" in m1.values(): best, best_row, best_score = m1, r, s1
        if s2>best_score and "name" in m2.values(): best, best_row, best_score = m2, r, s2
        if best_score>=2 and "name" in best.values(): break
    return best, best_row

def iter_rows(ws, header_row_idx: int):
    for r in ws.iter_rows(min_row=header_row_idx+1, values_only=True):
        yield ["" if v is None else str(v).strip() for v in r]

def row_to_dict(vals: List[str], mapping: Dict[int,str]) -> Dict[str,str]:
    d={}
    for col_idx, field in mapping.items():
        if col_idx-1 < len(vals):
            v = vals[col_idx-1]
            if v != "": d[field]=v
    return d

def is_category_row(d: Dict[str,str]) -> bool:
    """Категория/подкатегория — нет артикулу или нет цены; либо одно крупное слово БЕЗ цифр."""
    name = d.get("name","").strip()
    if not d.get("sku") or parse_float(d.get("price")) is None:
        # это отфильтрует большинство «шапок»
        return True
    s = nfkc(name)
    if 2 < len(s) < 48 and not re.search(r"\d", s):
        letters = [ch for ch in s if ch.isalpha()]
        if letters:
            upp = sum(1 for ch in letters if ch.upper()==ch)
            if upp / max(len(letters),1) > 0.95:
                return True
    return False

# ===================== ПОИСК И РАЗБОР КАРТОЧКИ =====================

UA = {"User-Agent": "Mozilla/5.0 (compatible; Copyline-Scraper/1.0)"}

SEARCH_PATTERNS = [
    "/index.php?route=product/search&search={q}",          # OpenCart
    "/search/?search={q}",
    "/?search={q}",
    "/?s={q}",                                             # WP
    "/catalogsearch/result/?q={q}",                        # Magento
]

def http_get(url: str) -> Optional[bytes]:
    """GET с ретраями и паузой между попытками."""
    delay = REQUEST_DELAY_S
    last = None
    for _ in range(RETRIES):
        try:
            r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT, allow_redirects=True)
            if r.status_code == 200 and len(r.content) >= 300:
                return r.content
            last = f"HTTP {r.status_code}, {len(r.content)} bytes"
        except Exception as e:
            last = str(e)
        time.sleep(delay); delay *= 1.6
    log(f"WARN: GET fail {url} -> {last}")
    return None

def soup_of(b: Optional[bytes]) -> Optional[BeautifulSoup]:
    return BeautifulSoup(b, "html.parser") if b else None

def try_find_product_url_by_sku(sku: str) -> Optional[str]:
    """Ищем ссылку на товар по артикулу через несколько типовых страниц поиска."""
    q = quote(sku)
    for pat in SEARCH_PATTERNS:
        url = urljoin(BASE_URL, pat.format(q=q))
        s = soup_of(http_get(url))
        if not s: continue
        # кандидаты ссылок
        links = []
        for a in s.select("a[href]"):
            href = a.get("href","")
            txt  = a.get_text(" ", strip=True)
            if not href: continue
            if not re.search(r"/product|/catalog|/shop|/store|/goods|/item|/card|/index\.php\?route=product", href, flags=re.I):
                continue
            # проверка на совпадение SKU либо в тексте, либо в href
            if re.search(re.escape(sku), href, flags=re.I) or re.search(re.escape(sku), txt, flags=re.I):
                links.append(urljoin(BASE_URL, href))
        if links:
            return links[0]
    # Пробуем прямые варианты
    for path in [f"/product/{q}", f"/goods/{q}", f"/catalog/{q}", f"/item/{q}"]:
        url = urljoin(BASE_URL, path)
        b = http_get(url)
        if b and b.startswith(b"<!"):  # хоть какая-то HTML-страница
            return url
    return None

def parse_product_page(url: str) -> Tuple[Optional[str], Optional[str], Dict[str,str], Optional[str]]:
    """
    Возвращает: (title, description, specs_dict, picture_url)
    Всё максимально толерантно к вёрстке.
    """
    s = soup_of(http_get(url))
    if not s: return None, None, {}, None

    # Название
    title = None
    h1 = s.select_one("h1") or s.select_one("h1.product-title") or s.select_one("[itemprop='name']")
    if h1: title = h1.get_text(" ", strip=True)
    if not title:
        mt = s.select_one("meta[property='og:title']") or s.select_one("meta[name='title']")
        if mt: title = mt.get("content","")

    # Картинка
    pic = None
    og = s.select_one("meta[property='og:image']")
    if og: pic = og.get("content","")
    if not pic:
        img = s.select_one("img[itemprop='image'], img#zoom, .product-image img, .product__image img, .product-images img")
        if img: pic = img.get("src") or img.get("data-src") or ""
    if pic: pic = urljoin(BASE_URL, pic)

    # Описание
    desc = None
    for sel in [
        ".product-description", "#tab-description", "[itemprop='description']",
        ".content-description", ".description", ".product__description"
    ]:
        el = s.select_one(sel)
        if el:
            desc = el.get_text(" ", strip=True)
            break
    if not desc:
        md = s.select_one("meta[name='description']")
        if md: desc = md.get("content","")

    # Характеристики (таблица/список/def-list)
    specs: Dict[str,str] = {}
    # таблицы
    for tbl in s.select("table"):
        rows = tbl.select("tr")
        good = 0
        for tr in rows:
            th = tr.find("th"); td = tr.find("td")
            if not td: continue
            k = th.get_text(" ", strip=True) if th else ""
            v = td.get_text(" ", strip=True)
            if k and v and len(k) <= 60 and len(v) <= 400:
                specs[k] = v; good += 1
        if good >= 2: break
    # definition list
    if len(specs) < 2:
        for dl in s.select("dl"):
            dts = dl.select("dt"); dds = dl.select("dd")
            if len(dts) >= 2 and len(dds) >= 2:
                for dt,dd in zip(dts,dds):
                    k = dt.get_text(" ", strip=True); v = dd.get_text(" ", strip=True)
                    if k and v: specs[k]=v
                break
    # списки
    if len(specs) < 2:
        for ul in s.select("ul"):
            items = [li.get_text(" ", strip=True) for li in ul.select("li")]
            good_pairs = [i for i in items if ":" in i]
            for pair in good_pairs:
                k,v = pair.split(":",1)
                k=k.strip(); v=v.strip()
                if k and v: specs[k]=v
            if len(specs)>=2: break

    # чистим лишнее из спеки
    for bad in list(specs.keys()):
        if re.search(r"артикул|благотворительность", bad, flags=re.I):
            specs.pop(bad, None)

    return title, desc, specs, pic

def specs_to_text(specs: Dict[str,str]) -> str:
    if not specs: return ""
    parts = [f"{k}: {v}" for k,v in specs.items()]
    return "; ".join(parts)

# ===================== FEED_META =====================

def render_feed_meta(meta: Dict[str,str]) -> str:
    order = [
        "script_version","supplier","source",
        "offers_total","offers_after_keywords","offers_written",
        "built_utc","built_Asia/Almaty",
    ]
    comments = {
        "script_version": "Версия скрипта",
        "supplier": "Метка поставщика",
        "source": "URL исходного XLSX",
        "offers_total": "Товарных строк в XLSX (после отсечения категорий)",
        "offers_after_keywords": "Осталось после фильтра по словам (по началу названия)",
        "offers_written": "Офферов записано в YML",
        "built_utc": "Время сборки (UTC)",
        "built_Asia/Almaty": "Время сборки (Алматы)",
    }
    mk = max(len(k) for k in order)
    lefts = [f"{k.ljust(mk)} = {str(meta.get(k,'n/a'))}" for k in order]
    ml = max(len(x) for x in lefts)
    lines = ["FEED_META"]
    for left,k in zip(lefts, order):
        lines.append(f"{left.ljust(ml)}  | {comments[k]}")
    return "\n".join(lines)

# ===================== MAIN =====================

def main() -> None:
    log(f"Source: {XLSX_URL}")
    # 1) Скачиваем XLSX
    b = None; last = None; delay = 0.4
    for _ in range(RETRIES):
        try:
            r = requests.get(XLSX_URL, timeout=HTTP_TIMEOUT, headers=UA)
            if r.status_code == 200 and len(r.content) > 1500:
                b = r.content; break
            last = f"HTTP {r.status_code}, {len(r.content)} bytes"
        except Exception as e:
            last = str(e)
        time.sleep(delay); delay *= 1.7
    if not b:
        raise RuntimeError(f"Не удалось скачать XLSX: {last}")

    # 2) Открываем книгу, ищем лучшую шапку
    wb = load_workbook(io.BytesIO(b), data_only=True, read_only=True)
    best = (None, {}, -1, -1)
    for ws in wb.worksheets:
        mapping, row_idx = best_header_map(ws)
        score = len(mapping)
        if score > best[3]:
            best = (ws, mapping, row_idx, score)
    ws, mapping, header_row = best
    if not ws or not mapping or header_row <= 0:
        raise RuntimeError("Не нашёл шапку с колонкой названия.")

    # 3) Читаем строки → словари, нормализуем
    rows=[]
    for vals in iter_rows(ws, header_row):
        d = row_to_dict(vals, mapping)
        if not d.get("name"): continue
        d["name"] = normalize_name(d["name"])
        rows.append(d)

    # 4) Оставляем только товары (есть артикул + цена)
    goods = [d for d in rows if not is_category_row(d)]
    offers_total = len(goods)

    # 5) Фильтр по словам (по началу названия)
    prefixes = load_prefixes(KEYWORDS_FILE)
    goods = [d for d in goods if name_starts_with_any(d.get("name",""), prefixes)]
    after_keywords = len(goods)

    # 6) Для каждого артикула тянем карточку
    out_offers = []
    for d in goods:
        sku = (d.get("sku") or "").strip()
        price = parse_float(d.get("price"))
        if not sku or not price:  # защита
            continue

        url = try_find_product_url_by_sku(sku)
        title, desc, specs, pic = (None, None, {}, None)
        if url:
            title, desc, specs, pic = parse_product_page(url)

        name = title or d["name"]
        description = one_line(" ".join(filter(None, [
            desc or "",
            ("Технические характеристики: " + specs_to_text(specs)) if specs else ""
        ])))

        # Бренд возьмём из спеки, если есть
        vendor = ""
        for k in list(specs.keys()):
            if re.search(r"бренд|производитель", k, flags=re.I):
                vendor = specs[k]; break

        offer = {
            "id": sku,
            "name": name,
            "picture": pic or "",
            "vendor": vendor.strip(),
            "description": description,
            "vendorCode": f"{VENDORCODE_PREFIX}{sku}",
            "price": str(int(round(price))),
            "currencyId": "KZT",
            "available": "true",
        }
        out_offers.append(offer)
        time.sleep(REQUEST_DELAY_S)

    # 7) Сборка YML
    root = ET.Element("yml_catalog"); root.set("date", datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(root, "shop"); offers = ET.SubElement(shop, "offers")

    for o in out_offers:
        offer = ET.SubElement(offers, "offer", {"id": o["id"], "available": o["available"]})
        ET.SubElement(offer, "name").text = o["name"]
        if o["picture"]: ET.SubElement(offer, "picture").text = o["picture"]
        if o["vendor"]: ET.SubElement(offer, "vendor").text = o["vendor"]
        if o["description"]: ET.SubElement(offer, "description").text = o["description"]
        ET.SubElement(offer, "vendorCode").text = o["vendorCode"]
        ET.SubElement(offer, "price").text = o["price"]
        ET.SubElement(offer, "currencyId").text = o["currencyId"]

    try:
        ET.indent(root, space="  ")
    except Exception:
        pass

    meta = {
        "script_version": SCRIPT_VERSION,
        "supplier": "copyline",
        "source": XLSX_URL,
        "offers_total": str(offers_total),
        "offers_after_keywords": str(after_keywords),
        "offers_written": str(len(out_offers)),
        "built_utc": now_utc_str(),
        "built_Asia/Almaty": now_almaty_str(),
    }
    root.insert(0, ET.Comment(render_feed_meta(meta)))

    xml_bytes = ET.tostring(root, encoding=OUTPUT_ENCODING, xml_declaration=True)
    xml_text  = xml_bytes.decode(OUTPUT_ENCODING, errors="replace")
    # красивый перенос после комментария
    xml_text  = re.sub(r"(-->)\s*(<shop>)", r"\1\n  \2", xml_text)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, newline="\n") as f:
        f.write(xml_text)

    log(f"Wrote: {OUT_FILE} | offers={len(out_offers)} | encoding={OUTPUT_ENCODING}")

if __name__ == "__main__":
    main()
