# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/probe.py

VTT temporary probe.
v4:
- больше не пытается угадывать "product_like" по общему crawl;
- строит discovery через /catalog + /catalog?word=<prefix>;
- собирает candidate product URLs по старому рабочему паттерну: /catalog/<slug>;
- отдельно дочитывает candidate product pages и уже по ним фильтрует title startswith(prefix);
- категории сохраняет только как диагностику.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

from bs4 import BeautifulSoup

from suppliers.vtt.filtering import (
    DEFAULT_ALLOWED_PREFIXES,
    build_filter_summary,
    title_passes_prefix_filter,
)

_PRODUCT_HREF_RE = re.compile(r"^/catalog/[^/?#]+/?$", re.I)
_PRICE_RE = re.compile(
    r"(?<!\d)(\d{1,3}(?:[ \u00A0]?\d{3})+|\d+)(?:[.,]\d{1,2})?\s*(?:₸|тг|тенге|kzt)?",
    re.I,
)
_CATEGORY_CODE_RE = re.compile(r"\b[A-Z_]{4,}\b")
_BAD_IMAGE_RE = re.compile(r"(favicon|yandex|counter|watch/|pixel|metrika|doubleclick)", re.I)


@dataclass(slots=True)
class ProbeConfig:
    out_dir: Path
    max_pages: int = 1000
    max_product_pages_to_save: int = 50
    max_candidate_fetch: int = 500


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

    analyzed_products = _fetch_and_analyze_candidates(client, candidate_urls, config.max_candidate_fetch)
    _write_json(out_dir / "product_pages.json", analyzed_products)

    sample_products = _pick_sample_products(analyzed_products)
    _write_json(out_dir / "sample_products.json", sample_products)
    _write_csv(out_dir / "sample_products.csv", sample_products)
    _write_product_html_samples(out_dir / "sample_html", analyzed_products, config.max_product_pages_to_save)

    filter_summary = build_filter_summary(analyzed_products)
    _write_json(out_dir / "filter_summary.json", filter_summary)

    discovered_endpoints = _collect_discovered_endpoints(pages)
    _write_json(out_dir / "discovered_endpoints.json", discovered_endpoints)

    summary = {
        "ok": True,
        "out_dir": str(out_dir),
        "pages_total": len(pages),
        "candidate_product_urls": len(candidate_urls),
        "product_pages_fetched": len(analyzed_products),
        "prefix_matched_products": sum(1 for x in analyzed_products if x.get("passes_prefix")),
        "sample_products": len(sample_products),
        "discovered_endpoints": len(discovered_endpoints),
        "filter_summary": filter_summary,
        "notes": [
            "Temporary VTT probe in search-seed + candidate-product-url mode.",
            "Categories are diagnostics only and do not gate selection.",
            "After VTT structure is understood, delete probe files and create final adapter.",
        ],
    }
    _write_json(out_dir / "summary.json", summary)
    return summary


def _build_seed_urls(catalog_url: str) -> list[str]:
    seeds = [catalog_url, "/catalog", "/catalog/"]
    for prefix in DEFAULT_ALLOWED_PREFIXES:
        seeds.append(f"/catalog?word={quote(prefix)}")
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
    for url in candidate_urls[: max(1, max_candidate_fetch)]:
        try:
            resp = client.get(url, allow_redirects=True)
            html = resp.text or ""
            if not html:
                continue
            out.append(_analyze_product_page(resp.url, html))
        except Exception as exc:  # noqa: BLE001
            out.append(
                {
                    "url": url,
                    "fetch_error": str(exc),
                    "product_confident": False,
                    "passes_prefix": False,
                    "matched_prefix": None,
                    "title": "",
                    "h1": "",
                    "normalized_title": "",
                    "price_candidates": [],
                    "images": [],
                    "images_count": 0,
                    "tables": [],
                    "tables_count": 0,
                    "category_codes_found": [],
                    "text_snippet": "",
                    "html_path": "",
                }
            )
    return out


def _analyze_product_page(url: str, html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = _extract_title(soup)
    h1 = _extract_h1(soup)
    title_for_filter = h1 or title
    prefix_res = title_passes_prefix_filter(title_for_filter)
    text = " ".join(soup.get_text(" ", strip=True).split())

    tables = _extract_tables(soup)
    images = _extract_images(soup, url)
    price_candidates = _extract_prices(text)
    breadcrumb = _extract_breadcrumbs(soup)
    category_codes = _extract_category_codes(url, text, breadcrumb)

    has_buy = ("в корзину" in text.lower()) or ("куп" in text.lower())
    product_confident = bool(
        title_for_filter
        and (price_candidates or images or tables or has_buy)
    )

    return {
        "url": url,
        "title": title,
        "h1": h1,
        "normalized_title": prefix_res.normalized_title,
        "matched_prefix": prefix_res.matched_prefix,
        "passes_prefix": prefix_res.allowed,
        "product_confident": product_confident,
        "breadcrumbs": breadcrumb,
        "category_codes_found": category_codes,
        "images": images[:20],
        "images_count": len(images),
        "tables": tables[:50],
        "tables_count": len(tables),
        "price_candidates": price_candidates[:10],
        "text_snippet": text[:5000],
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
        txt = " ".join(a.get_text(" ", strip=True).split())
        if txt:
            out.append(txt)
    return out


def _extract_tables(soup: BeautifulSoup) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = " ".join(cells[0].get_text(" ", strip=True).split())
            value = " ".join(cells[1].get_text(" ", strip=True).split())
            if key and value:
                out.append({"key": key, "value": value})
    if out:
        return out

    # fallback: старый VTT часто держал dt/dd внутри description
    box = soup.select_one("div.description.catalog_item_descr")
    if box:
        dts = box.find_all("dt")
        dds = box.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = " ".join(dt.get_text(" ", strip=True).split()).strip(":")
            value = " ".join(dd.get_text(" ", strip=True).split())
            if key and value:
                out.append({"key": key, "value": value})
    return out


def _extract_images(soup: BeautifulSoup, page_url: str) -> list[str]:
    from urllib.parse import urljoin

    out: list[str] = []
    seen: set[str] = set()
    for tag in soup.find_all(["img", "source"]):
        src = tag.get("src") or tag.get("data-src") or tag.get("srcset")
        if not src:
            continue
        src = src.split(",")[0].strip().split(" ")[0].strip()
        if not src:
            continue
        abs_url = urljoin(page_url, src)
        if _BAD_IMAGE_RE.search(abs_url):
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
        if not cleaned:
            continue
        if cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


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


def _pick_sample_products(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # сначала prefix-match товары, потом просто confident products
    ranked = sorted(
        items,
        key=lambda x: (
            0 if x.get("passes_prefix") else 1,
            0 if x.get("product_confident") else 1,
            -(len(x.get("price_candidates") or [])),
            -(x.get("images_count") or 0),
            x.get("normalized_title") or "",
        ),
    )
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in ranked:
        url = str(item.get("url") or "")
        if not url or url in seen:
            continue
        seen.add(url)
        slim = dict(item)
        slim.pop("html", None)
        out.append(slim)
        if len(out) >= 120:
            break
    return out


def _write_product_html_samples(sample_dir: Path, analyzed_products: list[dict[str, Any]], limit: int) -> None:
    sample_dir.mkdir(parents=True, exist_ok=True)
    for old in sample_dir.glob("*.html"):
        try:
            old.unlink()
        except Exception:
            pass

    chosen = [
        x for x in analyzed_products
        if x.get("passes_prefix") and x.get("html")
    ]
    if not chosen:
        chosen = [x for x in analyzed_products if x.get("product_confident") and x.get("html")]

    for idx, item in enumerate(chosen[: max(1, limit)], start=1):
        html = item.get("html") or ""
        if not html:
            continue
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
    return out[:1000]


def _strip_html_for_main_dump(pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in pages:
        item = dict(page)
        if "html" in item:
            item.pop("html")
        out.append(item)
    return out


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "url",
        "title",
        "h1",
        "normalized_title",
        "matched_prefix",
        "passes_prefix",
        "product_confident",
        "images_count",
        "tables_count",
        "price_candidates",
        "category_codes_found",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})
