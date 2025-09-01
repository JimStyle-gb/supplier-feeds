# -*- coding: utf-8 -*-
"""
VTT b2b → полный YML:
- Логин через /validateLogin (CSRF из <meta name="csrf-token">)
- Пагинация каталога /catalog/?page=N
- С листинга собираем: название, URL, data-img (картинка), цена (если есть)
- С карточки дополняем: артикул (vendorCode), цена (если не было), картинка, описание, крошки
- Категории строим из хлебных крошек; валюта RUB

ENV:
  BASE_URL (https://b2b.vtt.ru)
  START_URL (https://b2b.vtt.ru/catalog/)
  VTT_LOGIN, VTT_PASSWORD
  DISABLE_SSL_VERIFY ("1"|"0")
  HTTP_TIMEOUT, REQUEST_DELAY_MS, MIN_BYTES, MAX_PAGES, MAX_CRAWL_MINUTES, MAX_WORKERS
  OUT_FILE, OUTPUT_ENCODING
"""
from __future__ import annotations
import os, re, time, html, hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# ---------- ENV ----------
BASE_URL            = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL           = os.getenv("START_URL", f"{BASE_URL}/catalog/")

OUT_FILE            = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING     = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN           = os.getenv("VTT_LOGIN", "")
VTT_PASSWORD        = os.getenv("VTT_PASSWORD", "")

DISABLE_SSL_VERIFY  = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"
HTTP_TIMEOUT        = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS    = int(os.getenv("REQUEST_DELAY_MS", "160"))
MIN_BYTES           = int(os.getenv("MIN_BYTES", "700"))

MAX_PAGES           = int(os.getenv("MAX_PAGES", "800"))
MAX_CRAWL_MINUTES   = int(os.getenv("MAX_CRAWL_MINUTES", "60"))
MAX_WORKERS         = int(os.getenv("MAX_WORKERS", "6"))

UA = {
    "User-Agent": "Mozilla/5.0 (compatible; VTT-FullFeed/2.0; +https://github.com/)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

ROOT_CAT_ID    = 9600000
ROOT_CAT_NAME  = "VTT"
CURRENCY       = "RUB"
SUPPLIER_NAME  = "vtt"

# ---------- UTILS ----------
def jitter_sleep(ms: int) -> None:
    if ms > 0:
        time.sleep(ms / 1000.0)

def soup_of(content: bytes) -> BeautifulSoup:
    return BeautifulSoup(content, "html.parser")

def good(resp: requests.Response) -> bool:
    return (resp is not None) and (resp.status_code == 200) and (len(resp.content) >= MIN_BYTES)

def is_login_page_bytes(b: bytes) -> bool:
    s = ""
    try:
        s = b.decode("utf-8", errors="ignore").lower()
    except Exception:
        return False
    return ("/validatelogin" in s) or ("вход для клиентов" in s)

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def yml_escape(s: str) -> str:
    return html.escape(s or "")

def to_number(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).replace("\xa0", " ").strip()
    s = s.replace(" ", "").replace(",", ".")
    if not re.search(r"\d", s):
        return None
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    try:
        return float(m.group(0)) if m else None
    except Exception:
        return None

def normalize_url(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return BASE_URL + u
    return u

# ---------- YML ----------
def build_yml(categories: List[Tuple[int,str,Optional[int]]],
              offers: List[Tuple[int,Dict[str,Any]]]) -> str:
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SUPPLIER_NAME)}</name>")
    out.append(f"<currencies><currency id=\"{CURRENCY}\" rate=\"1\" /></currencies>")
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    for cid, name, parent in categories:
        pid = parent if parent else ROOT_CAT_ID
        out.append(f"<category id=\"{cid}\" parentId=\"{pid}\">{yml_escape(name)}</category>")
    out.append("</categories>")
    out.append("<offers>")

    for cid, it in offers:
        price = it.get("price", 0.0) or 0.0
        price_txt = str(int(price)) if float(price).is_integer() else f"{price}"
        out += [
            f"<offer id=\"{yml_escape(it['offer_id'])}\" available=\"true\" in_stock=\"true\">",
            f"<name>{yml_escape(it['title'])}</name>",
            f"<vendor>{yml_escape(it.get('brand') or ROOT_CAT_NAME)}</vendor>",
            f"<vendorCode>{yml_escape(it.get('vendorCode') or '')}</vendorCode>",
            f"<price>{price_txt}</price>",
            f"<currencyId>{CURRENCY}</currencyId>",
            f"<categoryId>{cid}</categoryId>",
        ]
        if it.get("url"):     out.append(f"<url>{yml_escape(it['url'])}</url>")
        if it.get("picture"): out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        desc = it.get("description") or it["title"]
        out.append(f"<description>{yml_escape(desc)}</description>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ---------- HTTP ----------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    return s

def request_get(s: requests.Session, url: str) -> Optional[requests.Response]:
    try:
        return s.get(url, timeout=HTTP_TIMEOUT, verify=(not DISABLE_SSL_VERIFY))
    except requests.exceptions.SSLError:
        if not DISABLE_SSL_VERIFY:
            try:
                return s.get(url, timeout=HTTP_TIMEOUT, verify=False)
            except Exception:
                return None
        return None
    except Exception:
        return None

def request_post(s: requests.Session, url: str, data: dict, headers: dict) -> Optional[requests.Response]:
    try:
        return s.post(url, data=data, headers=headers, timeout=HTTP_TIMEOUT, verify=(not DISABLE_SSL_VERIFY))
    except requests.exceptions.SSLError:
        if not DISABLE_SSL_VERIFY:
            try:
                return s.post(url, data=data, headers=headers, timeout=HTTP_TIMEOUT, verify=False)
            except Exception:
                return None
        return None
    except Exception:
        return None

# ---------- LOGIN ----------
def extract_csrf_token(b: bytes) -> Optional[str]:
    s = soup_of(b)
    m = s.find("meta", attrs={"name": "csrf-token"})
    return (m.get("content").strip() if m and m.get("content") else None)

def login(s: requests.Session) -> bool:
    if not VTT_LOGIN or not VTT_PASSWORD:
        print("Error: VTT_LOGIN / VTT_PASSWORD not set.")
        return False

    r0 = request_get(s, f"{BASE_URL}/")
    if not (r0 and good(r0)):
        print("Error: cannot open base page")
        return False
    token = extract_csrf_token(r0.content)

    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/",
    }
    if token: headers["X-CSRF-TOKEN"] = token

    _ = request_post(s, f"{BASE_URL}/validateLogin",
                     data={"login": VTT_LOGIN, "password": VTT_PASSWORD},
                     headers=headers)

    r2 = request_get(s, START_URL)
    if not (r2 and good(r2)):
        print("Error: catalog not reachable after login")
        return False
    if is_login_page_bytes(r2.content):
        print("Error: still on login page (credentials?)")
        return False
    print("[login] success")
    return True

# ---------- LISTING PARSE ----------
TITLE_LINK_SEL = 'div.catalog_list_row div.cl_name .cutoff-off a[href*="/catalog/"]:not(.btn_naked)'
CAM_SEL        = 'div.catalog_list_row div.cl_name .cutoff-off a.btn_naked[data-img], .cutoff-off a[data-img]'
PRICE_CANDIDATES = ['.cl_price', '.price', '.product_price', '.price_value', '.price-current', '.cl_cost']

def extract_vendor_from_title(title: str) -> Optional[str]:
    t = (title or "").strip()
    # ищем «похожее на артикул» — буквы/цифры/дефисы длиной 5+
    tokens = re.findall(r"[A-Za-zА-Яа-я0-9]{2,}(?:[-/][A-Za-zА-Яа-я0-9]{2,})*", t)
    # чаще всего артикул в конце, пройдёмся с конца
    for tok in reversed(tokens):
        if re.search(r"\d", tok) and re.search(r"[A-Za-zА-Яа-я]", tok):
            return tok
        if len(tok) >= 6 and re.search(r"\d", tok):
            return tok
    return None

def extract_entries_from_list_page(b: bytes) -> List[Dict[str,Any]]:
    s = soup_of(b)
    out: List[Dict[str,Any]] = []
    for a in s.select(TITLE_LINK_SEL):
        href = a.get("href") or ""
        url  = urljoin(BASE_URL, href)
        title = (a.get("title") or a.get_text(" ", strip=True) or "").strip()
        pic = None
        cam = a.find_previous("a", attrs={"data-img": True})
        if not cam:
            # пробуем общий селектор
            cam = s.select_one(CAM_SEL)
        if cam and cam.get("data-img"):
            pic = normalize_url(cam.get("data-img"))

        # попробуем цену из строки товара
        price = None
        row = a.find_parent("div", class_="catalog_list_row")
        if row:
            for sel in PRICE_CANDIDATES:
                node = row.select_one(sel)
                if node and node.get_text(strip=True):
                    price = to_number(node.get_text(" ", strip=True))
                    if price is not None:
                        break

        vcode = extract_vendor_from_title(title)
        out.append({"url": url, "title": title, "picture": pic, "price": price, "vendorCode": vcode})
    return out

# ---------- PRODUCT PAGE PARSE ----------
def find_text_like(s: BeautifulSoup, *needles: str) -> Optional[str]:
    body = s.get_text(" ", strip=True)
    m = re.search(r"(?:%s)\s*[:#]?\s*([A-Za-zА-Яа-я0-9\-\._/]{2,})" %
                  "|".join([re.escape(x) for x in needles]), body, flags=re.I)
    return m.group(1) if m else None

def extract_breadcrumbs(s: BeautifulSoup) -> List[str]:
    names: List[str] = []
    for bc in s.select('ul.breadcrumb, .breadcrumb, .breadcrumbs, [class*="breadcrumb"]'):
        for a in bc.find_all("a"):
            t = a.get_text(" ", strip=True)
            if not t: continue
            tl = t.lower()
            if tl in ("главная", "home"): continue
            names.append(t.strip())
        if names: break
    return names

def parse_product_page(s: requests.Session, url: str,
                       list_hint: Dict[str,Any]) -> Optional[Dict[str,Any]]:
    jitter_sleep(REQUEST_DELAY_MS)
    r = request_get(s, url)
    if not (r and good(r)) or is_login_page_bytes(r.content):
        return None
    doc = soup_of(r.content)

    title = None
    for sel in ["h1", ".page_title", "title"]:
        n = doc.select_one(sel)
        if n and n.get_text(strip=True):
            title = n.get_text(" ", strip=True)
            break
    if not title:
        og = doc.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            title = og["content"].strip()
    if not title:
        title = list_hint.get("title")

    # цена
    price = list_hint.get("price")
    if price is None:
        for sel in ['.price', '.product_price', '.price_value', '[class*="price"]']:
            n = doc.select_one(sel)
            if n and n.get_text(strip=True):
                price = to_number(n.get_text(" ", strip=True))
                if price is not None:
                    break

    # артикул
    vcode = list_hint.get("vendorCode")
    if not vcode:
        # itemprop=sku или подписи «Артикул / Код товара»
        skun = doc.find(attrs={"itemprop": "sku"})
        if skun and skun.get_text(strip=True):
            vcode = skun.get_text(" ", strip=True)
        if not vcode:
            vcode = find_text_like(doc, "Артикул", "Код товара", "Код", "SKU")

    # картинка
    pic = list_hint.get("picture")
    if not pic:
        ogi = doc.find("meta", attrs={"property": "og:image"})
        if ogi and ogi.get("content"):
            pic = normalize_url(ogi["content"])
        else:
            imgel = doc.find("img")
            if imgel and (imgel.get("src") or imgel.get("data-src")):
                pic = normalize_url(imgel.get("src") or imgel.get("data-src"))

    # описание
    desc = None
    for sel in ['[itemprop="description"]',
                '.product_description', '.prod_description', '.productfull',
                '#description', '.tab-content .description', '.tabs .description']:
        n = doc.select_one(sel)
        if n and n.get_text(strip=True):
            desc = n.get_text(" ", strip=True)
            break
    if not desc:
        desc = title

    crumbs = extract_breadcrumbs(doc)

    return {
        "title": title or list_hint.get("title") or "",
        "price": price if (price is not None and price >= 0) else 0.0,
        "vendorCode": vcode or "",
        "url": url,
        "picture": pic or "",
        "description": desc or "",
        "crumbs": crumbs,
        "brand": SUPPLIER_NAME,
    }

# ---------- CATEGORIES ----------
def stable_cat_id(text: str, prefix: int = 9700000) -> int:
    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:6]
    return prefix + int(h, 16)

def build_categories_from_paths(paths: List[List[str]]) -> Tuple[List[Tuple[int,str,Optional[int]]], Dict[Tuple[str,...], int]]:
    cat_map: Dict[Tuple[str,...], int] = {}
    out_list: List[Tuple[int,str,Optional[int]]] = []
    for path in paths:
        clean = [p.strip() for p in path if p and p.strip()]
        clean = [p for p in clean if p.lower() not in ("главная", "home", "каталог")]
        if not clean: continue
        parent_id = ROOT_CAT_ID
        prefix: List[str] = []
        for name in clean:
            prefix.append(name)
            key = tuple(prefix)
            if key in cat_map:
                parent_id = cat_map[key]; continue
            cid = stable_cat_id(" / ".join(prefix))
            cat_map[key] = cid
            out_list.append((cid, name, parent_id))
            parent_id = cid
    return out_list, cat_map

# ---------- CRAWL ----------
def crawl_listing(s: requests.Session) -> List[Dict[str,Any]]:
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MINUTES)
    all_entries: List[Dict[str,Any]] = []
    empty_streak = 0

    for page in range(1, MAX_PAGES + 1):
        if datetime.utcnow() > deadline:
            print("[crawl] deadline reached")
            break
        url = START_URL if page == 1 else f"{START_URL}?page={page}"
        r = request_get(s, url)
        if not (r and good(r)):
            empty_streak += 1
            if empty_streak >= 3:
                break
            continue
        if is_login_page_bytes(r.content):
            print("[crawl] session expired")
            break

        found = extract_entries_from_list_page(r.content)
        if found:
            all_entries.extend(found)
            empty_streak = 0
        else:
            empty_streak += 1
            if empty_streak >= 3:
                break

        jitter_sleep(REQUEST_DELAY_MS)

    # уник по URL
    uniq: Dict[str, Dict[str,Any]] = {}
    for e in all_entries:
        u = e["url"]
        if u not in uniq:
            uniq[u] = e
    entries = list(uniq.values())
    print(f"[discover] listing entries: {len(entries)}")
    return entries

def enrich_products(s: requests.Session, entries: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    out: List[Dict[str,Any]] = []

    def worker(e: Dict[str,Any]) -> Optional[Dict[str,Any]]:
        try:
            parsed = parse_product_page(s, e["url"], e)
            return parsed
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(worker, e): e["url"] for e in entries}
        for fut in as_completed(futs):
            rec = fut.result()
            if rec:
                out.append(rec)
    print(f"[parse] product pages parsed: {len(out)}")
    return out

# ---------- MAIN ----------
def main() -> int:
    if DISABLE_SSL_VERIFY:
        print("[ssl] verification disabled by env")

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

    s = make_session()
    if not login(s):
        xml = build_yml([], [])
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(xml)
        return 2

    entries = crawl_listing(s)
    if not entries:
        xml = build_yml([], [])
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(xml)
        print(f"[done] items: 0, cats: 0 -> {OUT_FILE}")
        return 1

    products = enrich_products(s, entries)

    # категории по крошкам
    cat_paths = [p.get("crumbs") for p in products if p.get("crumbs")]
    cat_list, path_id_map = build_categories_from_paths(cat_paths)

    # сборка офферов
    offers: List[Tuple[int,Dict[str,Any]]] = []
    seen_offer_ids = set()
    for p in products:
        # категория — ближайшая собранная цепочка
        cid = ROOT_CAT_ID
        if p.get("crumbs"):
            key = tuple([x for x in p["crumbs"] if x and x.strip()])
            while key and key not in path_id_map:
                key = key[:-1]
            if key and key in path_id_map:
                cid = path_id_map[key]

        offer_id = sha1(p["url"] or p["title"])
        if offer_id in seen_offer_ids:
            offer_id = sha1(p["url"] + p["title"] + (p.get("vendorCode") or ""))
        seen_offer_ids.add(offer_id)

        offers.append((cid, {
            "offer_id":   offer_id,
            "title":      p["title"],
            "price":      p.get("price", 0.0) or 0.0,
            "vendorCode": p.get("vendorCode") or "",
            "brand":      p.get("brand") or SUPPLIER_NAME,
            "url":        p.get("url") or "",
            "picture":    p.get("picture") or "",
            "description":p.get("description") or p["title"],
        }))

    xml = build_yml(cat_list, offers)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)

    print(f"[done] items: {len(offers)}, cats: {len(cat_list)} -> {OUT_FILE}")
    return 0 if offers else 1

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("Error:", e)
        sys.exit(2)
