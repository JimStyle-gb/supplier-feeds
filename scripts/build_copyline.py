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
import hashlib
from typing import Any, Dict, List, Optional, Sequence, Tuple
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

from cs.core import (
    CURRENCY_ID_DEFAULT,
    OUTPUT_ENCODING_DEFAULT,
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
KEYWORDS_FILE = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")

OUT_FILE = os.getenv("OUT_FILE", "docs/copyline.yml")
OUTPUT_ENCODING = (os.getenv("OUTPUT_ENCODING") or OUTPUT_ENCODING_DEFAULT).strip() or "utf-8"

VENDORCODE_PREFIX = (os.getenv("VENDORCODE_PREFIX") or "CL").strip()
PUBLIC_VENDOR = (os.getenv("PUBLIC_VENDOR") or SUPPLIER_NAME).strip() or SUPPLIER_NAME

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
REQUEST_DELAY_MS = int(os.getenv("REQUEST_DELAY_MS", "60"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

# Если сайт будет мешать — можно временно отключить парсинг сайта (останутся товары с placeholder-картинкой и описанием из XLSX)
NO_CRAWL = (os.getenv("NO_CRAWL", "") or "").strip().lower() in ("1", "true", "yes", "y")

UA = {
    "User-Agent": os.getenv(
        "UA",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.6,en;q=0.4",
}

PRODUCT_RE = re.compile(r"/goods/[^/]+\.html$", re.I)


# -----------------------------
# Утилиты
# -----------------------------
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
    print(f"[http] fail: {url} | {last}", flush=True)
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
        # аварийный вариант (стабильный, но без исходного кода)
        h = hashlib.md5((raw or "empty").encode("utf-8", errors="ignore")).hexdigest()[:10].upper()
        return f"{VENDORCODE_PREFIX}{h}"
    return f"{VENDORCODE_PREFIX}{raw}"


def load_keywords(path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            kws = [ln.strip() for ln in f.readlines()]
        return [kw for kw in kws if kw and not kw.startswith("#")]
    except Exception:
        return []


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


def parse_xlsx_items(xlsx_bytes: bytes) -> Tuple[int, List[Dict[str, Any]]]:
    wb = load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    sheet = max(wb.sheetnames, key=lambda n: wb[n].max_row * max(1, wb[n].max_column))
    ws = wb[sheet]
    rows = [[c for c in r] for r in ws.iter_rows(values_only=True)]
    print(f"[xls] sheet={sheet} rows={len(rows)}", flush=True)

    row0, row1, idx = detect_header_two_row(rows)
    if row0 < 0:
        raise RuntimeError("Не удалось распознать шапку в XLSX.")

    data_start = row1 + 1
    name_col, vendor_col, price_col = idx["name"], idx["vendor_code"], idx["price"]
    stock_col = idx.get("stock")

    kws = load_keywords(KEYWORDS_FILE)
    start_patterns = compile_startswith_patterns(kws)

    source_rows = sum(1 for r in rows[data_start:] if any(v is not None and str(v).strip() for v in r))

    out: List[Dict[str, Any]] = []
    for r in rows[data_start:]:
        name_raw = r[name_col]
        if not name_raw:
            continue
        title = title_clean(safe_str(name_raw))
        if kws and not title_startswith_strict(title, start_patterns):
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
            }
        )

    print(f"[xls] source_rows={source_rows} filtered={len(out)}", flush=True)
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

    # Picture
    src = None
    imgel = s.find("img", id=re.compile(r"^main_image_", re.I))
    if imgel:
        src = imgel.get("src") or imgel.get("data-src")
    if not src:
        ogi = s.find("meta", attrs={"property": "og:image"})
        if ogi and ogi.get("content"):
            src = safe_str(ogi["content"])
    if not src:
        for img in s.find_all("img"):
            t = safe_str(img.get("src") or img.get("data-src"))
            if any(k in t for k in ("img_products", "/products/", "/img/")):
                src = t
                break
    pic = normalize_img_to_full(src)

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
        "params": params2,
        "url": url,
    }


def discover_relevant_category_urls() -> List[str]:
    # Берём ссылки из /goods.html и главной, фильтруем по ключевым словам в тексте ссылки или в URL
    seeds = [f"{BASE_URL}/", f"{BASE_URL}/goods.html"]
    pages: List[Tuple[str, BeautifulSoup]] = []
    for u in seeds:
        b = http_get(u)
        if b:
            pages.append((u, soup_of(b)))
    if not pages:
        return []

    kws = load_keywords(KEYWORDS_FILE)
    urls: List[str] = []
    seen = set()

    for base, s in pages:
        for a in s.find_all("a", href=True):
            txt = safe_str(a.get_text(" ", strip=True))
            absu = requests.compat.urljoin(base, a["href"])
            if "copyline.kz" not in absu:
                continue
            if "/goods/" not in absu and not absu.endswith("/goods.html"):
                continue

            ok = False
            if kws:
                ok = any(re.search(r"(?i)(?<!\w)" + re.escape(kw).replace(r"\ ", " ") + r"(?!\w)", txt) for kw in kws)
                if not ok:
                    slug = absu.lower()
                    ok = any(kw.lower().replace(" ", "") in slug.replace("-", "").replace("_", "") for kw in kws)
            else:
                ok = True

            if not ok:
                continue
            if absu in seen:
                continue
            seen.add(absu)
            urls.append(absu)

    return urls[:80]


def collect_product_urls(category_url: str) -> List[str]:
    b = http_get(category_url, tries=3)
    if not b:
        return []
    s = soup_of(b)

    urls: List[str] = []
    seen = set()

    for a in s.find_all("a", href=True):
        href = safe_str(a["href"])
        if not href:
            continue
        if not PRODUCT_RE.search(href):
            continue
        u = requests.compat.urljoin(category_url, href)
        if u in seen:
            continue
        seen.add(u)
        urls.append(u)

    return urls


def build_site_index() -> Dict[str, Dict[str, Any]]:
    if NO_CRAWL:
        print("[site] NO_CRAWL=1 -> skip site parsing", flush=True)
        return {}

    cat_urls = discover_relevant_category_urls()
    if not cat_urls:
        print("[site] no category urls found", flush=True)
        return {}

    # соберём product urls
    all_urls: List[str] = []
    for cu in cat_urls:
        all_urls.extend(collect_product_urls(cu))

    # уникальные
    seen = set()
    uniq: List[str] = []
    for u in all_urls:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)

    print(f"[site] categories={len(cat_urls)} products={len(uniq)}", flush=True)

    # параллельный парсинг карточек
    from concurrent.futures import ThreadPoolExecutor, as_completed

    idx: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS)) as ex:
        futs = [ex.submit(parse_product_page, u) for u in uniq]
        for fut in as_completed(futs):
            try:
                it = fut.result()
            except Exception:
                it = None
            if not it:
                continue

            sku = safe_str(it.get("sku"))
            if not sku:
                continue

            # ключи под разные варианты артикула
            variants = {sku, sku.replace("-", "")}
            if re.fullmatch(r"[Cc]\d+", sku):
                variants.add(sku[1:])
            if re.fullmatch(r"\d+", sku):
                variants.add("C" + sku)

            for v in variants:
                idx[norm_ascii(v)] = it

    print(f"[site] indexed={len(idx)}", flush=True)
    return idx


# -----------------------------
# Планировщик для FEED_META
# -----------------------------
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

    site_index = build_site_index()

    # Собираем offers (важно: стабильные oid!)
    out_offers: List[OfferOut] = []
    seen_oids = set()

    for it in items:
        raw_v = safe_str(it.get("vendorCode_raw"))
        base_oid = oid_from_vendor_code_raw(raw_v)

        # найдём карточку на сайте (если есть)
        found = None
        candidates = {raw_v, raw_v.replace("-", "")}
        if re.fullmatch(r"[Cc]\d+", raw_v):
            candidates.add(raw_v[1:])
        if re.fullmatch(r"\d+", raw_v):
            candidates.add("C" + raw_v)

        for v in candidates:
            kn = norm_ascii(v)
            if kn in site_index:
                found = site_index[kn]
                break

        name = it["title"]
        native_desc = it["title"]
        pictures: List[str] = []
        params: List[Tuple[str, str]] = []

        if found:
            if found.get("title"):
                name = title_clean(safe_str(found.get("title"))) or name
            native_desc = safe_str(found.get("desc")) or native_desc
            if found.get("pic"):
                pictures = [safe_str(found.get("pic"))]
            params = list(found.get("params") or [])

        price = compute_price(int(it.get("dealer_price") or 0))

        oid = base_oid
        if oid in seen_oids:
            # редкий случай: дубль артикулов — делаем СТАБИЛЬНЫЙ суффикс от URL или имени
            seed = safe_str(found.get("url") if found else "") or name
            suf = hashlib.md5(seed.encode("utf-8", errors="ignore")).hexdigest()[:6].upper()
            oid = f"{base_oid}-{suf}"
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

    print(
        f"[build_copyline] OK | offers_in={before} | offers_out={after} | in_true={in_true} | in_false={in_false} | "
        f"crawl={'no' if NO_CRAWL else 'yes'} | changed={'yes' if changed else 'no'} | file={OUT_FILE}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
