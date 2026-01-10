# -*- coding: utf-8 -*-
"""
CopyLine adapter — сборщик по шаблону CS (использует scripts/cs/core.py).
Задача адаптера: забрать данные поставщика (XLSX + сайт) и отдать в CS ядро список OfferOut.
"""

from __future__ import annotations

import io
import os
import re
import time
import random
from datetime import datetime, timedelta

# Логи (можно выключить: VERBOSE=0)
VERBOSE = (os.getenv("VERBOSE", "1") or "1").strip() not in ("0", "false", "no", "off")

def log(*args, **kwargs) -> None:
    # Печать логов (в Actions удобно оставлять краткие метки)
    # Поддерживаем kwargs типа flush/end/sep, чтобы не ловить TypeError.
    if VERBOSE:
        if "flush" not in kwargs:
            kwargs["flush"] = True
        print(*args, **kwargs)

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

from cs.core import (
    CURRENCY_ID_DEFAULT,
    OfferOut,
    compute_price,
    ensure_footer_spacing,
    make_feed_meta,
    make_footer,
    make_header,
    now_almaty,
    validate_cs_yml,
    write_if_changed,
)

# -----------------------------
# Настройки
# -----------------------------
SUPPLIER_NAME = "CopyLine"
SUPPLIER_URL_DEFAULT = "https://copyline.kz/goods.html"
BASE_URL = "https://copyline.kz"

XLSX_URL = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")

# Вариант C: фильтрация CopyLine по префиксам названия (строго с начала строки)
# Важно для стабильного ассортимента и чтобы не тянуть UPS/прочее из прайса.
COPYLINE_INCLUDE_PREFIXES = [
    "drum",
    "developer",
    "девелопер",
    "драм",
    "кабель сетевой",
    "картридж",
    "термоблок",
    "термоэлемент",
    "тонер-картридж",
    "тонер картридж",
]



OUT_FILE = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING", "utf-8") or "utf-8").strip() or "utf-8"
NO_CRAWL = (os.getenv("NO_CRAWL", "0") or "0").strip().lower() in ("1", "true", "yes", "y", "on")
MAX_CATEGORY_PAGES = int(os.getenv("MAX_CATEGORY_PAGES", "25") or "25")  # лимит страниц на категорию
MAX_CRAWL_MINUTES = int(os.getenv("MAX_CRAWL_MINUTES", "12") or "12")    # общий лимит времени обхода сайта
# Регулярка для карточек товара (не категорий)
PRODUCT_RE = re.compile(r"/goods/[^/]+\.html$")

# Параллелизм обхода сайта
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6") or "6")




VENDORCODE_PREFIX = (os.getenv("VENDORCODE_PREFIX") or "CL").strip()
PUBLIC_VENDOR = (os.getenv("PUBLIC_VENDOR") or SUPPLIER_NAME).strip() or SUPPLIER_NAME

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "60"))
# HTTP headers (нужно для requests.get; иначе некоторые ответы могут быть урезаны)
UA = {
    "User-Agent": os.getenv(
        "HTTP_UA",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ),
    "Accept": "*/*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.5",
    "Connection": "keep-alive",
}



def _sleep_jitter(ms: int) -> None:
    d = max(0.0, ms / 1000.0)
    time.sleep(d * (1.0 + random.uniform(-0.15, 0.15)))


def http_get(url: str, tries: int = 3, min_bytes: int = 0) -> Optional[bytes]:
    delay = max(0.1, REQUEST_DELAY_MS / 1000.0)
    last = None
    for _ in range(max(1, tries)):
        try:
            r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
            if r.status_code == 200 and (len(r.content) >= min_bytes):
                return r.content
            last = f"http {r.status_code} size={len(r.content)}"
        except Exception as e:
            last = repr(e)
        _sleep_jitter(int(delay * 1000))
        delay *= 1.6
    log(f"[http] fail: {url} | {last}")
    return None


def soup_of(b: bytes) -> BeautifulSoup:
    return BeautifulSoup(b, "html.parser")


def norm_ascii(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def title_clean(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    return re.sub(r"\s{2,}", " ", s).strip()[:200]


def safe_str(x: Any) -> str:
    return (str(x).strip() if x is not None else "")


def to_number(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    s = s.replace("\xa0", " ").replace(" ", "")
    s = s.replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def parse_stock_to_bool(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, (int, float)):
        return float(x) > 0
    s = str(x).strip()
    if not s:
        return False
    s_low = s.lower()
    if s_low in ("-", "нет", "0", "0.0"):
        return False
    # "<10", ">5", "есть", "1-2" — считаем как наличие
    if re.search(r"\d", s_low):
        return True
    if "есть" in s_low:
        return True
    return False


def oid_from_vendor_code_raw(raw: str) -> str:
    raw = (raw or "").strip()
    raw = raw.replace("–", "-").replace("/", "-").replace("\\", "-")
    raw = re.sub(r"\s+", "", raw)
    raw = re.sub(r"[^A-Za-z0-9_.-]+", "", raw)
    raw = raw.strip("-.")
    if not raw:
        return ""
    return f"{VENDORCODE_PREFIX}{raw}"


def compile_startswith_patterns(kws: Sequence[str]) -> List[re.Pattern]:
    # строго с начала строки, чтобы не тянуть мусорные позиции
    out: List[re.Pattern] = []
    for kw in kws:
        kw = kw.strip()
        if not kw:
            continue
        out.append(re.compile(r"^\s*" + re.escape(kw).replace(r"\ ", " ") + r"(?!\w)", re.I))
    return out


def title_startswith_strict(title: str, patterns: Sequence[re.Pattern]) -> bool:
    return bool(title) and any(p.search(title) for p in patterns)


def _is_allowed_prefix(title: str) -> bool:
    # Финальная проверка по префиксам (чтобы после обогащения с сайта не вылезало лишнее)
    if not title:
        return False
    pats = compile_startswith_patterns(COPYLINE_INCLUDE_PREFIXES)
    return title_startswith_strict(title_clean(title), pats)


# -----------------------------
# XLSX
# -----------------------------
def detect_header_two_row(rows: List[List[Any]], scan_rows: int = 60) -> Tuple[int, int, Dict[str, int]]:
    def low(x: Any) -> str:
        return safe_str(x).lower()

    for i in range(min(scan_rows, len(rows) - 1)):
        row0 = [low(c) for c in rows[i]]
        row1 = [low(c) for c in rows[i + 1]]

        if any("номенклатура" in c for c in row0):
            name_col = next((j for j, c in enumerate(row0) if "номенклатура" in c), None)
            vendor_col = next((j for j, c in enumerate(row1) if "артикул" in c), None)
            price_col = next((j for j, c in enumerate(row1) if "цена" in c or "опт" in c), None)
            unit_col = next((j for j, c in enumerate(row1) if c.strip().startswith("ед")), None)
            stock_col = (
                next((j for j, c in enumerate(row0) if "остаток" in c), None)
                or next((j for j, c in enumerate(row1) if "остаток" in c), None)
            )
            if name_col is not None and vendor_col is not None and price_col is not None:
                idx = {"name": name_col, "vendor_code": vendor_col, "price": price_col}
                if stock_col is not None:
                    idx["stock"] = stock_col
                return i, i + 1, idx

    return -1, -1, {}



def _derive_kind(title: str) -> str:
    t = (title or "").strip().lower()
    if not t:
        return ""
    if t.startswith("тонер-картридж") or t.startswith("тонер картридж"):
        return "Тонер-картридж"
    if t.startswith("картридж"):
        return "Картридж"
    if t.startswith("кабель сетевой"):
        return "Кабель сетевой"
    if t.startswith("термоблок"):
        return "Термоблок"
    if t.startswith("термоэлемент"):
        return "Термоэлемент"
    if t.startswith("девелопер") or t.startswith("developer"):
        return "Девелопер"
    if t.startswith("драм") or t.startswith("drum"):
        return "Драм-картридж"
    return ""

def _merge_params(existing: List[Tuple[str, str]], add: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    # Склеиваем параметры без дублей, и выкидываем мусорные ключи (например, "3").
    seen = set()
    out: List[Tuple[str, str]] = []

    def push(k: str, v: str) -> None:
        kk = (k or "").strip()
        vv = (v or "").strip()
        if not kk or not vv:
            return
        if kk.isdigit():
            return
        key = (kk.lower(), vv.lower())
        if key in seen:
            return
        seen.add(key)
        out.append((kk, vv))

    for k, v in (existing or []):
        push(k, v)
    for k, v in (add or []):
        push(k, v)

    return out


def parse_xlsx_items(xlsx_bytes: bytes) -> Tuple[int, List[Dict[str, Any]]]:
    wb = load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    sheet = max(wb.sheetnames, key=lambda n: wb[n].max_row * max(1, wb[n].max_column))
    ws = wb[sheet]
    rows = [[c for c in r] for r in ws.iter_rows(values_only=True)]
    log(f"[xls] sheet={sheet} rows={len(rows)}")

    row0, row1, idx = detect_header_two_row(rows)
    if row0 < 0:
        raise RuntimeError("Не удалось распознать шапку в XLSX.")

    data_start = row1 + 1
    name_col, vendor_col, price_col = idx["name"], idx["vendor_code"], idx["price"]
    stock_col = idx.get("stock")
    unit_col = idx.get("unit")
    kws = COPYLINE_INCLUDE_PREFIXES
    start_patterns = compile_startswith_patterns(kws)
    source_rows = sum(1 for r in rows[data_start:] if any(v is not None and str(v).strip() for v in r))

    out: List[Dict[str, Any]] = []
    for r in rows[data_start:]:
        name_raw = r[name_col]
        if not name_raw:
            continue
        title = title_clean(safe_str(name_raw))
        if not title_startswith_strict(title, start_patterns):
            continue

        dealer = to_number(r[price_col])
        if dealer is None or dealer <= 0:
            continue

        v_raw = r[vendor_col]
        vcode = safe_str(v_raw)
        if not vcode:
            # иногда артикул спрятан в названии
            m = re.search(r"[A-ZА-Я0-9]{2,}(?:[-/–][A-ZА-Я0-9]{2,})?", title.upper())
            if m:
                vcode = m.group(0).replace("–", "-").replace("/", "-")
        if not vcode:
            continue

        available = True
        if stock_col is not None and stock_col < len(r):
            available = parse_stock_to_bool(r[stock_col])

        out.append(
            {
                "title": title,
                "vendorCode_raw": vcode,
                "dealer_price": int(round(float(dealer))),
                "available": bool(available),
                "stock_raw": safe_str(r[stock_col]).strip() if (stock_col is not None and stock_col < len(r)) else "",
                "unit_raw": safe_str(r[unit_col]).strip() if (unit_col is not None and unit_col < len(r)) else "",
            }
        )

    log(f"[xls] source_rows={source_rows} filtered={len(out)}")
    return source_rows, out


# -----------------------------
# Сайт: индексация карточек (картинки + описание + характеристики)
# -----------------------------
def normalize_img_to_full(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if not u:
        return None
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return BASE_URL + u
    if u.startswith("http://"):
        return "https://" + u[len("http://") :]
    return u


def extract_kv_pairs_from_text(text: str) -> List[Tuple[str, str]]:
    # очень мягкий парсер "Ключ: значение" в тексте
    out: List[Tuple[str, str]] = []
    for ln in (text or "").splitlines():
        ln = ln.strip().strip("•-–—")
        if not ln:
            continue
        if ":" in ln:
            k, v = ln.split(":", 1)
            k = k.strip()
            v = v.strip()
            if k and v and len(k) <= 80 and len(v) <= 240:
                out.append((k, v))
    return out


def parse_product_page(url: str) -> Optional[Dict[str, Any]]:
    b = http_get(url, tries=3)
    if not b:
        return None
    s = soup_of(b)

    # SKU
    sku = ""
    skuel = s.find(attrs={"itemprop": "sku"})
    if skuel:
        sku = safe_str(skuel.get_text(" ", strip=True))

    # jshopping: Артикул часто лежит тут: <span id="product_code">101942</span>
    if not sku:
        pc = s.find(id="product_code")
        if pc:
            sku = safe_str(pc.get_text(" ", strip=True))
    if not sku:
        txt = s.get_text(" ", strip=True)
        m = re.search(r"(?:Артикул|SKU|Код товара|Код)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})", txt, flags=re.I)
        if m:
            sku = m.group(1)


    if not sku:
        return None

    # Title
    h = s.find(["h1", "h2"], attrs={"itemprop": "name"}) or s.find("h1") or s.find("h2")
    title = title_clean(safe_str(h.get_text(" ", strip=True) if h else ""))
    # Picture (на сайте почти всегда есть фото; вытаскиваем максимально надёжно)

    src = ""

    cand: list[str] = []


    # 0) основная картинка (как на странице): <a class="lightbox" id="main_image_full_..."> href="...full_*.jpg"

    a_full = s.select_one('a.lightbox[id^="main_image_full_"]')

    if a_full and a_full.get("href"):

        cand.append(safe_str(a_full["href"]))


    # 1) og:image (обычно ведёт на img_products/*.jpg)

    ogi = s.find("meta", attrs={"property": "og:image"})

    if ogi and ogi.get("content"):

        cand.append(safe_str(ogi["content"]))


    # 2) rel=image_src

    lnk = s.find("link", attrs={"rel": "image_src"})

    if lnk and lnk.get("href"):

        cand.append(safe_str(lnk["href"]))


    # 3) main_image_* / itemprop=image

    img_main = s.select_one('img[id^="main_image_"]') or s.find("img", attrs={"itemprop": "image"})

    if img_main:

        for a in ("data-src", "data-original", "data-lazy", "src", "srcset"):

            v = img_main.get(a)

            if v:

                cand.append(safe_str(v))

                break


    # 4) любые img на странице (отбираем только похожие на фото товара)

    for img in s.find_all("img"):

        for a in ("data-src", "data-original", "data-lazy", "src", "srcset"):

            t = safe_str(img.get(a))

            if not t:

                continue

            if "thumb_" in t:

                continue

            if any(k in t for k in ("img_products", "jshopping", "/products/", "/img/")) or re.search(r"\.(?:jpg|jpeg|png|webp)(?:\?|$)", t, flags=re.I):

                cand.append(t)

                break


    # 5) иногда большая картинка лежит в <a href="...full_...jpg">

    for a in s.find_all("a"):

        href = safe_str(a.get("href"))

        if not href:

            continue

        if "thumb_" in href:

            continue

        if ("img_products" in href) or re.search(r"\.(?:jpg|jpeg|png|webp)(?:\?|$)", href, flags=re.I):

            cand.append(href)


    # выберем лучшую: full_ > не-thumb > первая

    src = ""

    for t in cand:

        t = (t or "").strip()

        if not t or t.startswith("data:"):

            continue

        if "/full_" in t:

            src = t

            break

    if not src:

        for t in cand:

            t = (t or "").strip()

            if not t or t.startswith("data:") or "thumb_" in t:

                continue

            src = t

            break


    pic = normalize_img_to_full(src)

    if not pic:
        return None
    # Description + params
    desc_txt = ""
    params: List[Tuple[str, str]] = []

    block = (
        s.select_one('div[itemprop="description"].jshop_prod_description')
        or s.select_one("div.jshop_prod_description")
        or s.select_one('[itemprop="description"]')
    )
    if block:
        desc_txt = block.get_text("\n", strip=True)
        params.extend(extract_kv_pairs_from_text(desc_txt))

    # Table specs (если есть)
    table = s.find("table")
    if table:
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if len(tds) >= 2:
                k = safe_str(tds[0].get_text(" ", strip=True))
                v = safe_str(tds[1].get_text(" ", strip=True))
                if k and v and len(k) <= 80 and len(v) <= 240:
                    params.append((k, v))

    # Удалим дубли
    seen = set()
    params2: List[Tuple[str, str]] = []
    for k, v in params:
        kk = k.strip()
        vv = v.strip()
        if not kk or not vv:
            continue
        key = (kk.lower(), vv.lower())
        if key in seen:
            continue
        seen.add(key)
        params2.append((kk, vv))

    return {
        "sku": sku.strip(),
        "title": title,
        "desc": desc_txt.strip(),
        "pic": pic,
        "params": [(k, v) for (k, v) in params2 if not re.fullmatch(r"\d{1,4}", k.strip())],
        "url": url,
    }


def discover_relevant_category_urls() -> List[str]:
    # Берём ссылки из /goods.html и главной, фильтруем по словам в тексте ссылки или в URL.
    seeds = [f"{BASE_URL}/", f"{BASE_URL}/goods.html"]
    pages: List[Tuple[str, BeautifulSoup]] = []
    for u in seeds:
        b = http_get(u, tries=3)
        if b:
            pages.append((u, soup_of(b)))
    if not pages:
        return []

    kws = [k.strip() for k in COPYLINE_INCLUDE_PREFIXES if k.strip()]
    urls: List[str] = []
    seen = set()

    for base, s in pages:
        for a in s.find_all("a", href=True):
            txt = safe_str(a.get_text(" ", strip=True) or "")
            absu = requests.compat.urljoin(base, safe_str(a["href"]))
            if "copyline.kz" not in absu:
                continue
            if "/goods/" not in absu and not absu.endswith("/goods.html"):
                continue

            ok = False
            for kw in kws:
                if re.search(r"(?i)(?<!\w)" + re.escape(kw).replace(r"\ ", " ") + r"(?!\w)", txt):
                    ok = True
                    break

            if not ok:
                slug = absu.lower()
                if any(h in slug for h in [
                    "drum", "developer", "fuser", "toner", "cartridge",
                    "драм", "девелопер", "фьюзер", "термоблок", "термоэлемент", "cartridg",
                    "кабель", "cable",
                ]):
                    ok = True

            if ok and absu not in seen:
                seen.add(absu)
                urls.append(absu)

    return list(dict.fromkeys(urls))


def _category_next_url(s: BeautifulSoup, page_url: str) -> Optional[str]:
    ln = s.find("link", attrs={"rel": "next"})
    if ln and ln.get("href"):
        return requests.compat.urljoin(page_url, safe_str(ln["href"]))
    a = s.find("a", class_=lambda c: c and "next" in safe_str(c).lower())
    if a and a.get("href"):
        return requests.compat.urljoin(page_url, safe_str(a["href"]))
    for a in s.find_all("a", href=True):
        txt = safe_str(a.get_text(" ", strip=True) or "").lower()
        if txt in ("следующая", "вперед", "вперёд", "next", ">"):
            return requests.compat.urljoin(page_url, safe_str(a["href"]))
    return None


def collect_product_urls(category_url: str, limit_pages: int) -> List[str]:
    # Собирает ссылки на товары внутри категории, проходя пагинацию.
    urls: List[str] = []
    seen_pages = set()
    page = category_url
    pages_done = 0

    while page and pages_done < limit_pages:
        if page in seen_pages:
            break
        seen_pages.add(page)

        _sleep_jitter(REQUEST_DELAY_MS)
        b = http_get(page, tries=3)
        if not b:
            break
        s = soup_of(b)

        for a in s.find_all("a", href=True):
            absu = requests.compat.urljoin(page, safe_str(a["href"]))
            if PRODUCT_RE.search(absu):
                urls.append(absu)

        page = _category_next_url(s, page)
        pages_done += 1

    return list(dict.fromkeys(urls))


def build_site_index(want_keys: Optional[Set[str]] = None) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    if NO_CRAWL:
        log("[site] NO_CRAWL=1 -> skip site parsing")
        return {}

    cats = discover_relevant_category_urls()
    if not cats:
        log("[site] no category urls found")
        return {}

    pages_budget = MAX_CATEGORY_PAGES

    product_urls: List[str] = []
    for cu in cats:
        product_urls.extend(collect_product_urls(cu, pages_budget))
    product_urls = list(dict.fromkeys(product_urls))
    log(f"[site] categories={len(cats)} product_urls={len(product_urls)} pages_budget={pages_budget}")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MINUTES)

    site_index: Dict[str, Dict[str, Any]] = {}
    matched: Set[str] = set()

    with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS)) as ex:
        futures = {ex.submit(parse_product_page, u): u for u in product_urls}
        for fut in as_completed(futures):
            if datetime.utcnow() > deadline:
                break
            try:
                out = fut.result()
            except Exception:
                out = None
            if not out:
                continue

            sku = safe_str(out.get("sku")).strip()
            if not sku:
                continue

            variants = {sku, sku.replace("-", "")}
            if re.fullmatch(r"[Cc]\d+", sku):
                variants.add(sku[1:])
            if re.fullmatch(r"\d+", sku):
                variants.add("C" + sku)

            keys = [norm_ascii(v) for v in variants if norm_ascii(v)]
            if not keys:
                continue

            if want_keys:
                useful = [k for k in keys if k in want_keys and k not in matched]
                if not useful:
                    continue
                for k in useful:
                    matched.add(k)
                    site_index[k] = out
            else:
                for k in keys:
                    site_index[k] = out

    log(f"[site] indexed={len(site_index)} matched={len(matched) if want_keys else '-'}")
    return site_index, {}
def next_run_dom_1_10_20_at_hour(now_local: datetime, hour: int) -> datetime:
    # now_local — наивный datetime в Алматы
    y = now_local.year
    m = now_local.month

    def candidates_for_month(yy: int, mm: int) -> List[datetime]:
        return [datetime(yy, mm, d, hour, 0, 0) for d in (1, 10, 20)]

    cands = [dt for dt in candidates_for_month(y, m) if dt > now_local]
    if cands:
        return min(cands)

    # следующий месяц
    if m == 12:
        y2, m2 = y + 1, 1
    else:
        y2, m2 = y, m + 1
    return min(candidates_for_month(y2, m2))


# -----------------------------
# Main
# -----------------------------



def main() -> int:
    build_time = now_almaty()
    next_run = next_run_dom_1_10_20_at_hour(build_time, 3)

    xlsx_bytes = http_get(XLSX_URL, tries=3, min_bytes=10_000)
    if not xlsx_bytes:
        raise RuntimeError("Не удалось скачать XLSX.")

    before, items = parse_xlsx_items(xlsx_bytes)

    # хотим подтянуть картинки только для наших артикулов (как в старом скрипте)

    want_keys: Set[str] = set()

    for it in items:

        raw_v = safe_str(it.get("vendorCode_raw") or "").strip()

        if not raw_v:

            continue

        variants = {raw_v, raw_v.replace("-", "")}

        if re.fullmatch(r"[Cc]\d+", raw_v):

            variants.add(raw_v[1:])

        if re.fullmatch(r"\d+", raw_v):

            variants.add("C" + raw_v)

        for v in variants:

            want_keys.add(norm_ascii(v))


    site_sku_index, site_index = build_site_index(want_keys)

    # Собираем offers (важно: стабильные oid!)
    out_offers: List[OfferOut] = []
    seen_oids = set()

    for it in items:
        raw_v = safe_str(it.get("vendorCode_raw"))
        base_oid = oid_from_vendor_code_raw(raw_v)

        # найдём карточку на сайте (если есть)
        found = None
        candidates = {raw_v, raw_v.replace("-", "")} 
        raw_v0 = raw_v.lstrip("0")
        if raw_v0 and raw_v0 != raw_v:
            candidates.add(raw_v0)
            candidates.add(raw_v0.replace("-", "")) 
        raw_v0 = raw_v.lstrip("0")
        if raw_v0 and raw_v0 != raw_v:
            candidates.add(raw_v0)
            candidates.add(raw_v0.replace("-", ""))
        if re.fullmatch(r"[Cc]\d+", raw_v):
            candidates.add(raw_v[1:])
        if re.fullmatch(r"\d+", raw_v):
            candidates.add("C" + raw_v)

        for v in candidates:
            kn = norm_ascii(v)
            if kn in site_sku_index:
                found = site_sku_index[kn]
                break

        if not found:
            tk_full = norm_ascii(title_clean(it["title"]))
            if tk_full and tk_full in site_index:
                found = site_index[tk_full]
            else:
                tk30 = norm_ascii(title_clean(it["title"])[:30])
                if tk30 and tk30 in site_index:
                    found = site_index[tk30]

        name = it["title"]
        if not _is_allowed_prefix(name):
            continue
        native_desc = it["title"]
        pictures: List[str] = []
        params: List[Tuple[str, str]] = []
        if found:
            native_desc = safe_str(found.get("desc")) or native_desc
            if found.get("pic"):
                pictures = [safe_str(found.get("pic"))]
            params = list(found.get("params") or [])

        if not _is_allowed_prefix(name):
            continue

        # Минимальные характеристики из прайса (чтобы у всех товаров были params)
        kind = _derive_kind(name)
        p_min: List[Tuple[str, str]] = []
        if kind:
            p_min.append(("Тип", kind))
        unit_raw = safe_str(it.get("unit_raw") or "").strip()
        if unit_raw and unit_raw != "-":
            p_min.append(("Ед. изм.", unit_raw))
        params = _merge_params(params, p_min)

        price = compute_price(int(it.get("dealer_price") or 0))
        oid = base_oid
        if not oid:
            # нет стабильного артикула — пропускаем (никаких хэшей)
            continue
        if oid in seen_oids:
            # дубль артикула: пропускаем позицию (лучше потерять пару дублей, чем плодить новые id)
            continue
        seen_oids.add(oid)

        out_offers.append(
            OfferOut(
                oid=oid,
                available=bool(it.get("available", True)),
                name=name,
                price=price,
                pictures=pictures,
                vendor="",  # бренд будет выбран ядром; если не найдётся — упадём на PUBLIC_VENDOR
                params=params,
                native_desc=native_desc,
            )
        )

    after = len(out_offers)
    in_true = sum(1 for o in out_offers if o.available)
    in_false = after - in_true

    feed_meta = make_feed_meta(
        supplier=SUPPLIER_NAME,
        supplier_url=os.getenv("SUPPLIER_URL", SUPPLIER_URL_DEFAULT),
        build_time=build_time,
        next_run=next_run,
        before=before,
        after=after,
        in_true=in_true,
        in_false=in_false,
    )

    header = make_header(build_time, encoding=OUTPUT_ENCODING)
    footer = make_footer()

    offers_xml = "\n\n".join(
        [o.to_xml(currency_id=CURRENCY_ID_DEFAULT, public_vendor=PUBLIC_VENDOR) for o in out_offers]
    )

    full = header + "\n" + feed_meta + "\n\n" + offers_xml + "\n" + footer
    full = ensure_footer_spacing(full)
    validate_cs_yml(full)
    changed = write_if_changed(OUT_FILE, full, encoding=OUTPUT_ENCODING)

    log(
        f"[build_copyline] OK | offers_in={before} | offers_out={after} | in_true={in_true} | in_false={in_false} | "
        f"crawl={'no' if NO_CRAWL else 'yes'} | changed={'yes' if changed else 'no'} | file={OUT_FILE}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
