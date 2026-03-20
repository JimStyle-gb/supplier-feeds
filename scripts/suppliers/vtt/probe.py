# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/probe.py

VTT temporary full-inventory probe.
v5:
- больше не режет товары по prefix на стадии вытаскивания;
- после логина пытается собрать максимально полный список candidate product URLs;
- дочитывает все candidate product pages (лимит задается config);
- строит inventory-карту поставщика: title/category/brand/price/stock/images/params/desc/codes/compat;
- сохраняет сводки, чтобы потом решить, какие группы оставлять в финальном адаптере.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

from bs4 import BeautifulSoup


_PRODUCT_HREF_RE = re.compile(r"^/catalog/[^/?#]+/?$", re.I)
_PRICE_RE = re.compile(
    r"(?<!\d)(\d{1,3}(?:[ \u00A0]?\d{3})+|\d+)(?:[.,]\d{1,2})?\s*(?:₸|тг|тенге|kzt)?",
    re.I,
)
_CATEGORY_CODE_RE = re.compile(r"\b[A-Z_]{4,}\b")
_BAD_IMAGE_RE = re.compile(r"(favicon|yandex|counter|watch/|pixel|metrika|doubleclick)", re.I)
_SPLIT_RE = re.compile(r"[\s/|,;:()]+")

# search-seeds только для discovery, НЕ для final filtering
DEFAULT_DISCOVERY_WORDS: list[str] = [
    "Drum",
    "Девелопер",
    "Драм-картридж",
    "Драм-юниты",
    "Кабель",
    "Картридж",
    "Картриджи",
    "Термоблок",
    "Тонер-картридж",
    "Чернила",
    "Тонер",
    "Драм",
    "Термопленка",
    "Термоэлемент",
    "Печь",
    "Ролик",
    "Чип",
    "Головка",
    "Developer",
    "Ink",
    "Cartridge",
]


@dataclass(slots=True)
class ProbeConfig:
    out_dir: Path
    max_pages: int = 2500
    max_product_pages_to_save: int = 120
    max_candidate_fetch: int = 10000


def run_vtt_probe(client, config: ProbeConfig) -> dict[str, Any]:
    out_dir = config.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    login_report = client.login_and_verify()
    _write_json(out_dir / "login_report.json", login_report)
    if not login_report.get("ok"):
        return {
            "ok": False,
            "stage": "login",
            "out_dir": str(out_dir),
            "message": "Login failed. Check login_report.json.",
        }

    seed_urls = _build_seed_urls(login_report.get("catalog_url") or "/catalog/")
    _write_json(out_dir / "search_seed_urls.json", seed_urls)

    pages = client.crawl_same_host(
        start_urls=seed_urls,
        max_pages=config.max_pages,
        allow_paths=["/catalog"],
    )
    _write_json(out_dir / "crawl_pages.json", _strip_html_for_main_dump(pages))

    candidate_urls = _collect_candidate_product_urls(pages)
    _write_json(out_dir / "candidate_product_urls.json", candidate_urls)

    inventory_items = _fetch_and_analyze_candidates(client, candidate_urls, config.max_candidate_fetch)
    _write_json(out_dir / "inventory_items.json", inventory_items)
    _write_csv(out_dir / "inventory_items.csv", inventory_items)

    inventory_summary = _build_inventory_summary(inventory_items, len(candidate_urls), len(pages))
    _write_json(out_dir / "inventory_summary.json", inventory_summary)

    field_coverage = _build_field_coverage(inventory_items)
    _write_json(out_dir / "field_coverage.json", field_coverage)

    prefix_stats_1 = _build_prefix_stats(inventory_items, words_count=1)
    prefix_stats_2 = _build_prefix_stats(inventory_items, words_count=2)
    _write_json(out_dir / "title_prefix_1word_top200.json", prefix_stats_1)
    _write_json(out_dir / "title_prefix_2word_top200.json", prefix_stats_2)

    category_stats = _build_category_stats(inventory_items)
    _write_json(out_dir / "category_stats.json", category_stats)

    brand_stats = _build_brand_stats(inventory_items)
    _write_json(out_dir / "brand_stats.json", brand_stats)

    product_type_examples = _build_examples_by_prefix(inventory_items, words_count=1, per_group=12, top_groups=50)
    _write_json(out_dir / "sample_by_1word_prefix.json", product_type_examples)

    discovered_endpoints = _collect_discovered_endpoints(pages)
    _write_json(out_dir / "discovered_endpoints.json", discovered_endpoints)

    _write_product_html_samples(out_dir / "sample_html", inventory_items, config.max_product_pages_to_save)

    summary = {
        "ok": True,
        "out_dir": str(out_dir),
        "pages_total": len(pages),
        "candidate_product_urls": len(candidate_urls),
        "inventory_items_fetched": len(inventory_items),
        "product_confident_items": sum(1 for x in inventory_items if x.get("product_confident")),
        "with_price": sum(1 for x in inventory_items if x.get("price_text")),
        "with_images": sum(1 for x in inventory_items if (x.get("images_count") or 0) > 0),
        "with_params": sum(1 for x in inventory_items if (x.get("params_count") or 0) > 0),
        "with_description": sum(1 for x in inventory_items if x.get("description_present")),
        "with_codes": sum(1 for x in inventory_items if x.get("codes_present")),
        "with_compat": sum(1 for x in inventory_items if x.get("compat_present")),
        "discovered_endpoints": len(discovered_endpoints),
        "notes": [
            "Temporary full-inventory VTT probe.",
            "No business filtering is applied at extraction stage.",
            "Use inventory_summary/category_stats/brand_stats/title_prefix_* to decide which goods to keep later.",
        ],
    }
    _write_json(out_dir / "summary.json", summary)
    return summary


def _build_seed_urls(catalog_url: str) -> list[str]:
    seeds = [catalog_url, "/catalog", "/catalog/"]
    for word in DEFAULT_DISCOVERY_WORDS:
        seeds.append(f"/catalog?word={quote(word)}")
    seen: set[str] = set()
    out: list[str] = []
    for item in seeds:
        value = str(item).strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _collect_candidate_product_urls(pages: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for page in pages:
        for raw in [page.get("url")] + list(page.get("links") or []) + list(page.get("api_like_links") or []):
            url = str(raw or "").strip()
            if not url:
                continue
            parsed = urlparse(url)
            if parsed.query or parsed.fragment:
                continue
            if not _PRODUCT_HREF_RE.match(parsed.path):
                continue
            norm = parsed._replace(query="", fragment="").geturl()
            if norm not in seen:
                seen.add(norm)
                out.append(norm)
    return out


def _fetch_and_analyze_candidates(client, candidate_urls: list[str], max_candidate_fetch: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    limit = min(len(candidate_urls), max(1, max_candidate_fetch))
    for idx, url in enumerate(candidate_urls[:limit], start=1):
        try:
            resp = client.get(url, allow_redirects=True)
            html = resp.text or ""
            if not html:
                continue
            item = _analyze_product_page(resp.url, html)
            item["fetch_index"] = idx
            out.append(item)
        except Exception as exc:  # noqa: BLE001
            out.append(
                {
                    "url": url,
                    "fetch_index": idx,
                    "fetch_error": str(exc),
                    "product_confident": False,
                    "title": "",
                    "h1": "",
                    "normalized_title": "",
                    "first_word": "",
                    "first_two_words": "",
                    "brand_guess": "",
                    "price_text": "",
                    "stock_text": "",
                    "images_count": 0,
                    "params_count": 0,
                    "description_present": False,
                    "codes_present": False,
                    "compat_present": False,
                    "category_codes_found": [],
                    "breadcrumbs": [],
                    "html": "",
                }
            )
    return out


def _analyze_product_page(url: str, html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = _extract_title(soup)
    h1 = _extract_h1(soup)
    product_title = (h1 or title).strip()
    normalized_title = _normalize_text(product_title)
    breadcrumbs = _extract_breadcrumbs(soup)

    params = _extract_params(soup)
    params_count = len(params)

    images = _extract_images(soup, url)
    price_values = _extract_prices(" ".join(soup.get_text(" ", strip=True).split()))
    price_text = price_values[0] if price_values else ""
    stock_text = _extract_stock_text(soup)
    description_text = _extract_description_text(soup)
    description_present = bool(description_text)

    codes = _extract_codes(product_title, params, description_text)
    compat_text = _extract_compat_text(params, description_text)
    brand_guess = _guess_brand(product_title, params, breadcrumbs)

    text_full = " ".join(soup.get_text(" ", strip=True).split())
    has_buy = ("в корзину" in text_full.lower()) or ("куп" in text_full.lower())
    product_confident = bool(product_title and (price_text or params_count or images or has_buy))

    return {
        "url": url,
        "title": title,
        "h1": h1,
        "normalized_title": normalized_title,
        "first_word": _first_words(normalized_title, 1),
        "first_two_words": _first_words(normalized_title, 2),
        "breadcrumbs": breadcrumbs,
        "category_codes_found": _extract_category_codes(url, text_full, breadcrumbs),
        "brand_guess": brand_guess,
        "price_text": price_text,
        "stock_text": stock_text,
        "images": images[:20],
        "images_count": len(images),
        "params": params[:150],
        "params_count": params_count,
        "description_present": description_present,
        "description_snippet": description_text[:1500],
        "codes_present": bool(codes),
        "codes": codes[:50],
        "compat_present": bool(compat_text),
        "compat_snippet": compat_text[:1500],
        "product_confident": product_confident,
        "html": html,
    }


def _extract_title(soup: BeautifulSoup) -> str:
    el = soup.select_one(".page_title") or soup.find("title") or soup.find("h1")
    return " ".join(el.get_text(" ", strip=True).split()) if el else ""


def _extract_h1(soup: BeautifulSoup) -> str:
    el = soup.find("h1")
    return " ".join(el.get_text(" ", strip=True).split()) if el else ""


def _extract_breadcrumbs(soup: BeautifulSoup) -> list[str]:
    out: list[str] = []
    for a in soup.select("[class*=breadcrumb] a, nav[aria-label*=breadcrumb] a"):
        txt = _normalize_text(a.get_text(" ", strip=True))
        if txt:
            out.append(txt)
    return out


def _extract_params(soup: BeautifulSoup) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = _normalize_text(cells[0].get_text(" ", strip=True)).strip(":")
            value = _normalize_text(cells[1].get_text(" ", strip=True))
            if key and value and (key, value) not in seen:
                seen.add((key, value))
                out.append({"key": key, "value": value})

    box = soup.select_one("div.description.catalog_item_descr")
    if box:
        dts = box.find_all("dt")
        dds = box.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = _normalize_text(dt.get_text(" ", strip=True)).strip(":")
            value = _normalize_text(dd.get_text(" ", strip=True))
            if key and value and (key, value) not in seen:
                seen.add((key, value))
                out.append({"key": key, "value": value})

    return out


def _extract_images(soup: BeautifulSoup, page_url: str) -> list[str]:
    from urllib.parse import urljoin

    out: list[str] = []
    seen: set[str] = set()

    for tag in soup.find_all(["img", "source", "a"]):
        candidates = [
            tag.get("src"),
            tag.get("data-src"),
            tag.get("data-original"),
            tag.get("href"),
            tag.get("srcset"),
        ]
        for src in candidates:
            if not src:
                continue
            src = str(src).split(",")[0].strip().split(" ")[0].strip()
            if not src:
                continue
            abs_url = urljoin(page_url, src)
            if _BAD_IMAGE_RE.search(abs_url):
                continue
            if not re.search(r"\.(?:jpg|jpeg|png|webp|gif)(?:\?|$)", abs_url, re.I):
                continue
            if abs_url not in seen:
                seen.add(abs_url)
                out.append(abs_url)
    return out


def _extract_prices(text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for match in _PRICE_RE.findall(text or ""):
        cleaned = " ".join(str(match).split())
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


def _extract_stock_text(soup: BeautifulSoup) -> str:
    text = " ".join(soup.get_text(" ", strip=True).split())
    patterns = [
        r"(?:в наличии|под заказ|нет в наличии|ожидается)[^.;,\n]{0,80}",
        r"(?:остаток|наличие)[^.;,\n]{0,80}",
        r"(?:срок поставки)[^.;,\n]{0,80}",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            return _normalize_text(m.group(0))
    return ""


def _extract_description_text(soup: BeautifulSoup) -> str:
    selectors = [
        "div.description",
        "div.catalog_item_descr",
        "div[itemprop='description']",
        ".product-description",
        ".tab-description",
    ]
    for sel in selectors:
        box = soup.select_one(sel)
        if box:
            txt = _normalize_text(box.get_text(" ", strip=True))
            if txt:
                return txt
    return ""


def _extract_codes(title: str, params: list[dict[str, str]], description_text: str) -> list[str]:
    text_blocks = [title, description_text]
    text_blocks += [f"{x.get('key','')}: {x.get('value','')}" for x in params]
    text = " | ".join(text_blocks)
    found = re.findall(r"\b[A-Z0-9][A-Z0-9\-./]{2,}\b", text)
    out: list[str] = []
    seen: set[str] = set()
    for code in found:
        code = code.strip(".-/")
        if len(code) < 3:
            continue
        if not re.search(r"\d", code):
            continue
        if code not in seen:
            seen.add(code)
            out.append(code)
    return out


def _extract_compat_text(params: list[dict[str, str]], description_text: str) -> str:
    parts: list[str] = []
    for row in params:
        key = (row.get("key") or "").lower()
        value = row.get("value") or ""
        if any(word in key for word in ["совмест", "подходит", "для устройств", "для принтеров"]):
            parts.append(value)
    desc_hits = []
    for pattern in [
        r"(?:совместим(?:ость|ые)?|подходит для)[^.;]{0,500}",
        r"(?:для принтеров|для мфу|для устройств)[^.;]{0,500}",
    ]:
        for m in re.finditer(pattern, description_text or "", re.I):
            desc_hits.append(m.group(0))
    parts.extend(desc_hits)
    return _normalize_text(" | ".join(x for x in parts if x))


def _guess_brand(title: str, params: list[dict[str, str]], breadcrumbs: list[str]) -> str:
    for row in params:
        key = (row.get("key") or "").lower()
        value = _normalize_text(row.get("value") or "")
        if any(word in key for word in ["бренд", "brand", "производ", "vendor", "марка"]) and value:
            return value

    known = [
        "HP", "Canon", "Xerox", "Brother", "Kyocera", "Samsung", "Epson",
        "Ricoh", "Konica Minolta", "Pantum", "Lexmark", "Oki", "Sharp",
        "Panasonic", "Toshiba", "Develop", "Gestetner", "Riso",
    ]
    upper_title = f" {title.upper()} "
    for brand in known:
        if f" {brand.upper()} " in upper_title:
            return brand
    for crumb in breadcrumbs:
        for brand in known:
            if brand.lower() in crumb.lower():
                return brand
    return ""


def _extract_category_codes(url: str, text: str, breadcrumbs: list[str]) -> list[str]:
    found: list[str] = []
    seen: set[str] = set()
    query = parse_qs(urlparse(url).query)
    for value in query.get("category", []):
        val = (value or "").strip()
        if val and val not in seen:
            seen.add(val)
            found.append(val)
    for block in [text] + breadcrumbs:
        for code in _CATEGORY_CODE_RE.findall(block or ""):
            if code not in seen:
                seen.add(code)
                found.append(code)
    return found[:50]


def _build_inventory_summary(items: list[dict[str, Any]], candidate_total: int, pages_total: int) -> dict[str, Any]:
    confident = [x for x in items if x.get("product_confident")]
    return {
        "candidate_product_urls_total": candidate_total,
        "inventory_items_fetched": len(items),
        "pages_total": pages_total,
        "product_confident_items": len(confident),
        "with_price": sum(1 for x in items if x.get("price_text")),
        "with_images": sum(1 for x in items if (x.get("images_count") or 0) > 0),
        "with_params": sum(1 for x in items if (x.get("params_count") or 0) > 0),
        "with_description": sum(1 for x in items if x.get("description_present")),
        "with_codes": sum(1 for x in items if x.get("codes_present")),
        "with_compat": sum(1 for x in items if x.get("compat_present")),
        "with_brand_guess": sum(1 for x in items if x.get("brand_guess")),
        "fetch_errors": sum(1 for x in items if x.get("fetch_error")),
    }


def _build_field_coverage(items: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(items) or 1
    counts = {
        "title": sum(1 for x in items if x.get("normalized_title")),
        "brand_guess": sum(1 for x in items if x.get("brand_guess")),
        "price_text": sum(1 for x in items if x.get("price_text")),
        "stock_text": sum(1 for x in items if x.get("stock_text")),
        "images": sum(1 for x in items if (x.get("images_count") or 0) > 0),
        "params": sum(1 for x in items if (x.get("params_count") or 0) > 0),
        "description": sum(1 for x in items if x.get("description_present")),
        "codes": sum(1 for x in items if x.get("codes_present")),
        "compat": sum(1 for x in items if x.get("compat_present")),
        "breadcrumbs": sum(1 for x in items if x.get("breadcrumbs")),
        "category_codes_found": sum(1 for x in items if x.get("category_codes_found")),
    }
    return {
        "total_items": len(items),
        "counts": counts,
        "share_pct": {k: round(v * 100.0 / total, 2) for k, v in counts.items()},
    }


def _build_prefix_stats(items: list[dict[str, Any]], words_count: int) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for item in items:
        value = item.get("first_word") if words_count == 1 else item.get("first_two_words")
        value = _normalize_text(str(value or ""))
        if value:
            counter[value] += 1
    return dict(counter.most_common(200))


def _build_category_stats(items: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for item in items:
        for crumb in item.get("breadcrumbs") or []:
            value = _normalize_text(str(crumb))
            if value:
                counter[value] += 1
        for code in item.get("category_codes_found") or []:
            value = _normalize_text(str(code))
            if value:
                counter[value] += 1
    return dict(counter.most_common(300))


def _build_brand_stats(items: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for item in items:
        value = _normalize_text(str(item.get("brand_guess") or ""))
        if value:
            counter[value] += 1
    return dict(counter.most_common(200))


def _build_examples_by_prefix(
    items: list[dict[str, Any]],
    *,
    words_count: int,
    per_group: int,
    top_groups: int,
) -> dict[str, list[dict[str, Any]]]:
    groups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    stats = _build_prefix_stats(items, words_count=words_count)
    top_names = list(stats.keys())[:top_groups]
    allowed = set(top_names)

    for item in items:
        group = item.get("first_word") if words_count == 1 else item.get("first_two_words")
        group = _normalize_text(str(group or ""))
        if not group or group not in allowed:
            continue
        slim = {
            "url": item.get("url"),
            "title": item.get("normalized_title"),
            "brand_guess": item.get("brand_guess"),
            "price_text": item.get("price_text"),
            "stock_text": item.get("stock_text"),
            "images_count": item.get("images_count"),
            "params_count": item.get("params_count"),
            "codes_present": item.get("codes_present"),
            "compat_present": item.get("compat_present"),
            "breadcrumbs": item.get("breadcrumbs"),
        }
        if len(groups[group]) < per_group:
            groups[group].append(slim)

    out: dict[str, list[dict[str, Any]]] = {}
    for name in top_names:
        if name in groups:
            out[name] = groups[name]
    return out


def _first_words(text: str, words_count: int) -> str:
    tokens = [x for x in _SPLIT_RE.split(text) if x]
    return " ".join(tokens[:words_count]).strip()


def _normalize_text(text: str) -> str:
    return " ".join(str(text or "").replace("\xa0", " ").split()).strip()


def _write_product_html_samples(sample_dir: Path, items: list[dict[str, Any]], limit: int) -> None:
    sample_dir.mkdir(parents=True, exist_ok=True)
    for old in sample_dir.glob("*.html"):
        try:
            old.unlink()
        except Exception:
            pass

    chosen = sorted(
        [x for x in items if x.get("html") and x.get("product_confident")],
        key=lambda x: (
            -(1 if x.get("price_text") else 0),
            -(x.get("images_count") or 0),
            -(x.get("params_count") or 0),
            x.get("normalized_title") or "",
        ),
    )[: max(1, limit)]

    for idx, item in enumerate(chosen, start=1):
        html = item.get("html") or ""
        if html:
            (sample_dir / f"{idx:03d}.html").write_text(str(html), encoding="utf-8", errors="ignore")


def _collect_discovered_endpoints(pages: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for page in pages:
        for url in page.get("api_like_links") or []:
            value = str(url).strip()
            if value and value not in seen:
                seen.add(value)
                out.append(value)
    return out[:1500]


def _strip_html_for_main_dump(pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in pages:
        item = dict(page)
        item.pop("html", None)
        out.append(item)
    return out


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "fetch_index",
        "url",
        "normalized_title",
        "first_word",
        "first_two_words",
        "brand_guess",
        "price_text",
        "stock_text",
        "images_count",
        "params_count",
        "description_present",
        "codes_present",
        "compat_present",
        "product_confident",
        "breadcrumbs",
        "category_codes_found",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})
