# scripts/build_copyline.py
from __future__ import annotations
import os, re, io, sys, json, time, hashlib, urllib.parse
from collections import Counter, defaultdict

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from xml.etree.ElementTree import Element, SubElement, ElementTree

# ========= ПАРАМЕТРЫ =========
XLSX_URL   = os.getenv("XLSX_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE   = os.getenv("OUT_FILE",  "docs/copyline.yml")
ENC        = (os.getenv("OUTPUT_ENCODING") or "windows-1251").lower()

# Фильтр категорий из прайса (по словам в названии/категории/листе). Пусто = берём всё
CATS_FILE  = os.getenv("CATEGORIES_FILE", "docs/categories_copyline.txt")

# СПИСОК КАТЕГОРИЙ САЙТА ДЛЯ ОБХОДА (страницы вида /goods/...html)
URLS_FILE  = os.getenv("URLS_FILE", "docs/categories_copyline_urls.txt")

# Кэш индекса картинок (код -> картинка; имя -> картинка)
PHOTO_INDEX_FILE = os.getenv("PHOTO_INDEX_FILE", "docs/copyline_photo_index.json")

# Лимит и паузы при скачивании страниц
MAX_PAGES_PER_CAT = int(os.getenv("MAX_PAGES_PER_CAT", "200"))
REQUEST_DELAY     = float(os.getenv("REQUEST_DELAY", "0.4"))

UA_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
BASE = "https://copyline.kz"

# ========= УТИЛЫ =========
def norm(s: str|None) -> str:
    return re.sub(r"\s+"," ", (s or "").strip())

def fetch(url: str) -> requests.Response:
    r = requests.get(url, headers=UA_HEADERS, timeout=60)
    r.raise_for_status()
    return r

def fetch_bytes(url: str) -> bytes:
    return fetch(url).content

def ensure_files():
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

def load_lines(path: str) -> list[str]:
    arr=[]
    try:
        with open(path,"r",encoding="utf-8") as f:
            for line in f:
                s=line.strip()
                if not s or s.startswith("#"): continue
                arr.append(s)
    except FileNotFoundError:
        pass
    return arr

def load_patterns(path: str):
    subs, regs = [], []
    try:
        with open(path,"r",encoding="utf-8") as f:
            for line in f:
                s=line.strip()
                if not s or s.startswith("#"): continue
                if s.lower().startswith("re:"):
                    try: regs.append(re.compile(s[3:], re.I))
                    except: pass
                else:
                    subs.append(s.lower())
    except FileNotFoundError:
        pass
    return subs, regs

def pass_filters(cat: str, sheet: str, name: str, subs, regs) -> bool:
    if not subs and not regs: return True
    hay = (cat.lower(), sheet.lower(), name.lower())
    if any(sub in h for sub in subs for h in hay): return True
    if any(r.search(h) for r in regs for h in hay): return True
    return False

# ========= РАСПОЗНАВАНИЕ КОЛОНОК ИЗ XLSX =========
HEADER_HINTS = {
    "name":        ["номенклатура","наименование","название","товар","описание"],
    "article":     ["артикул","номенклатура.артикул","sku","код товара","код","модель","part number","pn","p/n"],
    "availability":["остаток","наличие","кол-во","количество","qty","stock","остатки"],
    "price":       ["цена","опт","опт. цена","розница","стоимость","цена, тг","цена тг","retail"],
    "unit":        ["ед.","ед","единица"],
    "category":    ["категория","раздел","группа","тип"],
}

def best_header(ws):
    def score(row_vals):
        low = [norm(x).lower() for x in row_vals]
        matched=set()
        for key,hints in HEADER_HINTS.items():
            for cell in low:
                if any(h in cell for h in hints):
                    matched.add(key); break
        return len(matched)

    rows=[]
    for row in ws.iter_rows(min_row=1, max_row=40, values_only=True):
        rows.append([norm("" if v is None else str(v)) for v in row])

    best_row, best_idx, best_sc = [], None, -1
    for i,r in enumerate(rows):
        sc=score(r)
        if sc>best_sc: best_row, best_idx, best_sc = r, i+1, sc

    for i in range(len(rows)-1):
        a,b=rows[i],rows[i+1]
        m=max(len(a),len(b)); merged=[]
        for j in range(m):
            x=a[j] if j<len(a) else ""
            y=b[j] if j<len(b) else ""
            merged.append(" ".join(t for t in (x,y) if t).strip())
        sc=score(merged)
        if sc>best_sc: best_row, best_idx, best_sc = merged, i+2, sc
    return best_row, (best_idx or 1)+1

def map_columns(headers):
    cols={}
    low=[h.lower() for h in headers]
    def find_any(keys):
        for i,cell in enumerate(low):
            if any(k in cell for k in keys): return i
        return None
    cols["name"]        = find_any(HEADER_HINTS["name"])
    cols["article"]     = find_any(HEADER_HINTS["article"])
    cols["availability"]= find_any(HEADER_HINTS["availability"])
    cols["price"]       = find_any(HEADER_HINTS["price"])
    cols["unit"]        = find_any(HEADER_HINTS["unit"])
    cols["category"]    = find_any(HEADER_HINTS["category"])
    return cols

def parse_price(v) -> int|None:
    if v is None: return None
    t = norm(str(v)).replace("₸","").replace("тг","")
    digits = re.sub(r"[^\d]","", t)
    return int(digits) if digits else None

def is_available(v) -> tuple[bool,str]:
    s = norm(str(v)).lower()
    if not s or s in {"-","—","н/д","нет"}: return False, "0"
    if s.isdigit(): q=int(s); return (q>0), str(q)
    if re.search(r">\s*\d+", s): return True, "10"
    if "есть" in s or "в наличии" in s or "да" in s: return True, "1"
    return True, "1"

# ========= ВЫДЕЛЕНИЕ «КОДОВ» ИЗ ТЕКСТА (TN-2075, DR-1075, CF283A, 039 и т.п.) =========
CODE_SLASH   = re.compile(r"\b([A-Z]{1,8}-)(\d{2,6})(?:/(\d{2,6}))+")
CODE_SIMPLE  = re.compile(r"\b([A-Z]{1,8}(?:-[A-Z]{1,3})?[-_ ]?\d{2,6}[A-Z]{0,3})\b")
CODE_NUMONLY = re.compile(r"\b(\d{2,6})\b")

def codes_from_text(text: str) -> list[str]:
    if not text: return []
    t = norm(text).upper()
    out=[]
    m = CODE_SLASH.search(t)
    if m:
        out.append((m.group(1)+m.group(2)).replace(" ","-"))
    for m in CODE_SIMPLE.finditer(t):
        out.append(re.sub(r"[ _]", "-", m.group(1)))
    # осторожно добавляем чисто числовой код (например, Canon 039)
    for m in CODE_NUMONLY.finditer(t):
        out.append(m.group(1))
    seen=set(); res=[]
    for c in out:
        if c not in seen:
            res.append(c); seen.add(c)
    return res

def absolutize(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"): return url
    return urllib.parse.urljoin(BASE, url)

# ========= ОБХОД КАТЕГОРИЙ COPYLINE И ПОСТРОЕНИЕ ИНДЕКСА КАРТИНОК =========
def read_photo_index() -> dict:
    if os.path.exists(PHOTO_INDEX_FILE):
        try:
            with open(PHOTO_INDEX_FILE,"r",encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_photo_index(data: dict):
    tmp = PHOTO_INDEX_FILE + ".tmp"
    with open(tmp,"w",encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, PHOTO_INDEX_FILE)

def extract_product_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    hrefs=set()
    # типичные списки товаров
    for a in soup.select('a[href*="/product/"], a[href*="/goods/"]'):
        href = a.get("href") or ""
        if href.endswith(".html"):
            hrefs.add(absolutize(href))
    # Иногда карточки без явного класса — собираем всё, но фильтруем по .html
    for a in soup.find_all("a", href=True):
        href=a["href"]
        if href.endswith(".html") and ("/goods/" in href or "/product/" in href):
            hrefs.add(absolutize(href))
    return list(hrefs)

def find_next_page_url(html: str, current_url: str) -> str|None:
    soup = BeautifulSoup(html, "lxml")
    # rel="next"
    a = soup.find("a", rel=lambda v: v and "next" in v.lower())
    if a and a.get("href"): return absolutize(a["href"])
    # текстовые варианты
    for sel in ["a.next", "a.pagination-next", "a.pagenav", "a[aria-label*=След]", "a:contains('След')"]:
        for a in soup.select(sel):
            if a and a.get("href"): return absolutize(a["href"])
    # эвристика: берем ссылку с текстом '>' или '»'
    for a in soup.find_all("a"):
        txt = norm(a.get_text())
        if txt in {">", "»", "Next", "Следующая", "Далее"} and a.get("href"):
            return absolutize(a["href"])
    return None

def parse_product_page(url: str) -> tuple[list[str], str|None, str]:
    """Возвращает (коды, картинка_src, название)"""
    try:
        html = fetch(url).text
    except Exception:
        return [], None, ""
    soup = BeautifulSoup(html, "lxml")
    title = soup.find("h1")
    title_txt = norm(title.get_text()) if title else ""

    img = soup.find("img", {"itemprop":"image"})
    pic = img.get("src") if img else None
    if pic:
        pic = absolutize(pic)

    # добираем коды также из alt/title картинки
    extras = []
    if img:
        if img.get("alt"): extras.append(img.get("alt"))
        if img.get("title"): extras.append(img.get("title"))
        # из имени файла
        fn = os.path.basename(urllib.parse.urlparse(pic or "").path)
        name_wo_ext = os.path.splitext(fn)[0]
        extras.append(name_wo_ext)

    codes = []
    for t in [title_txt] + extras:
        codes.extend(codes_from_text(t))
    # уникализируем, сохраняем порядок
    seen=set(); codes_u=[]
    for c in codes:
        if c not in seen:
            codes_u.append(c); seen.add(c)
    return codes_u, pic, title_txt

def build_photo_index(urls: list[str], index: dict) -> dict:
    """index: {"code2img":{}, "name2img":{}, "seen":{url:ts}}"""
    index.setdefault("code2img", {})
    index.setdefault("name2img", {})
    index.setdefault("seen", {})

    for cat_url in urls:
        try:
            html = fetch(cat_url).text
        except Exception:
            continue
        pages_seen=0
        next_url = cat_url
        while html and pages_seen < MAX_PAGES_PER_CAT:
            pages_seen += 1
            # ссылки на товары
            links = extract_product_links(html)
            for purl in links:
                if purl in index["seen"]:  # уже парсили
                    continue
                codes, pic, name = parse_product_page(purl)
                index["seen"][purl] = int(time.time())
                if pic:
                    if name:
                        key = norm(name).lower()
                        index["name2img"][key] = pic
                    for c in codes:
                        index["code2img"].setdefault(c, pic)
                time.sleep(REQUEST_DELAY)
            # следующая страница категории
            nx = find_next_page_url(html, next_url)
            if not nx: break
            try:
                html = fetch(nx).text
                next_url = nx
                time.sleep(REQUEST_DELAY)
            except Exception:
                break
        # сохраняем частично, чтобы не терять прогресс
        save_photo_index(index)
    return index

# ========= YML =========
ROOT_CAT_ID = "9300000"
def hash_int(s: str) -> int: return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:6],16)
def cat_id_for(name: str) -> str: return str(9300001 + (hash_int(name.lower()) % 400000))

def make_offer_id(it: dict) -> str:
    art = (it.get("article") or "").strip()
    if art: return f"copyline:{art}"
    base = re.sub(r"[^a-z0-9]+","-", (it.get("name","").lower()))
    h = hashlib.md5((it.get('name','').lower()+"|"+(it.get('category') or '').lower()).encode('utf-8')).hexdigest()[:8]
    return f"copyline:{base}:{h}"

def build_yml(items):
    cats={}
    for it in items:
        nm = it.get("category") or "Copyline"
        if nm not in cats: cats[nm]=cat_id_for(nm)

    root=Element("yml_catalog"); shop=SubElement(root,"shop")
    SubElement(shop,"name").text="copyline-xlsx"
    curr=SubElement(shop,"currencies"); SubElement(curr,"currency",{"id":"KZT","rate":"1"})

    xml_cats=SubElement(shop,"categories")
    SubElement(xml_cats,"category",{"id":ROOT_CAT_ID}).text="Copyline"
    for nm,cid in cats.items():
        SubElement(xml_cats,"category",{"id":cid,"parentId":ROOT_CAT_ID}).text=nm

    offers=SubElement(shop,"offers")
    used=set()
    for it in items:
        oid = make_offer_id(it)
        if oid in used:
            extra = hashlib.md5((it.get("name","")+str(it.get("price"))+(it.get("qty") or "")).encode("utf-8")).hexdigest()[:6]
            i=2
            while f"{oid}-{extra}-{i}" in used: i+=1
            oid = f"{oid}-{extra}-{i}"
        used.add(oid)

        cid = cats.get(it.get("category") or "Copyline", ROOT_CAT_ID)
        o = SubElement(offers,"offer",{
            "id": oid,
            "available":"true" if it["available"] else "false",
            "in_stock":"true" if it["available"] else "false",
        })
        SubElement(o,"name").text = it["name"]
        if it.get("price") is not None: SubElement(o,"price").text=str(it["price"])
        SubElement(o,"currencyId").text="KZT"
        SubElement(o,"categoryId").text=cid
        if it.get("vendor"): SubElement(o,"vendor").text=it["vendor"]
        if it.get("article"): SubElement(o,"vendorCode").text=it["article"]
        q = it.get("qty") or ("1" if it["available"] else "0")
        for tag in ("quantity_in_stock","stock_quantity","quantity"): SubElement(o,tag).text=q
        if it.get("picture"): SubElement(o,"picture").text = it["picture"]

    buf=io.BytesIO(); ElementTree(root).write(buf, encoding=ENC, xml_declaration=True); return buf.getvalue()

# ========= MAIN =========
def main():
    ensure_files()
    # 1) Загружаем прайс
    xls = fetch_bytes(XLSX_URL)
    wb = load_workbook(io.BytesIO(xls), read_only=True, data_only=True)

    subs, regs = load_patterns(CATS_FILE)

    raw=[]
    for ws in wb.worksheets:
        headers, start = best_header(ws)
        if not headers: continue
        cols = map_columns(headers)
        if cols.get("name") is None: continue

        current_cat=None
        for row in ws.iter_rows(min_row=start, values_only=True):
            row=list(row)
            def getc(i): return None if i is None or i>=len(row) else row[i]
            name    = norm(getc(cols.get("name")))
            article = norm(getc(cols.get("article")))
            price   = parse_price(getc(cols.get("price")))
            avail, qty = is_available(getc(cols.get("availability")))
            raw_cat = norm(getc(cols.get("category"))) if cols.get("category") is not None else ""

            # заголовок секции — считаем категорией
            if name and not article and price is None:
                if len(name)>3 and not re.search(r"^(цены|прайс|тенге|лист|итог)", name.lower()):
                    current_cat=name; continue
            if not name and not article: continue

            cat = raw_cat or current_cat or norm(ws.title)
            if not pass_filters(cat, ws.title, name or article, subs, regs): continue

            raw.append({
                "article": article,
                "name": name or article,
                "vendor": "",
                "category": cat,
                "price": price,
                "available": avail,
                "qty": qty,
            })

    # дедуп
    ded={}
    for it in raw:
        key = ("a", it["article"]) if it["article"] else ("n", it["name"].lower(), (it["category"] or "").lower())
        if key in ded:
            old=ded[key]
            better = it if (it["price"] and not old["price"]) or (it["available"] and not old["available"]) else old
            ded[key]=better
        else:
            ded[key]=it
    items=list(ded.values())

    # 2) Собираем фото с сайта: обходим список категорий и строим индекс code->img и name->img
    urls = load_lines(URLS_FILE)
    photo_index = read_photo_index()
    photo_index = build_photo_index(urls, photo_index)  # безопасно: догружает и кэширует

    code2img = photo_index.get("code2img", {})
    name2img = photo_index.get("name2img", {})

    # 3) Проставляем <picture> для каждого товара из прайса
    for it in items:
        pic = None
        # сначала пытаемся по коду из названия
        for c in codes_from_text(it.get("name","")):
            if c in code2img:
                pic = code2img[c]; break
        # потом по полному имени
        if not pic:
            key = norm(it.get("name","")).lower()
            pic = name2img.get(key)
        it["picture"] = pic

    # 4) Пишем YML
    yml = build_yml(items)
    with open(OUT_FILE,"wb") as f: f.write(yml)
    print(f"{OUT_FILE}: {len(items)} items | pictures filled for {sum(1 for i in items if i.get('picture'))}")
    print(f"Photo index: {PHOTO_INDEX_FILE} | categories crawled: {len(urls)}")

if __name__=="__main__":
    try: main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr); sys.exit(1)
