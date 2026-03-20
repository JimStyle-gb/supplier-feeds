# -*- coding: utf-8 -*-
"""Path: scripts/suppliers/vtt/probe.py"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from suppliers.vtt.filtering import (
    DEFAULT_ALLOWED_CATEGORY_CODES,
    build_filter_summary,
    category_allowed,
    title_passes_prefix_filter,
)


_PRICE_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[ \u00A0]?\d{3})+|\d+)(?:[.,]\d{1,2})?\s*(?:₽|руб|тенге|kzt)?", re.I)
_CATEGORY_RE = re.compile(r"\b(" + "|".join(map(re.escape, DEFAULT_ALLOWED_CATEGORY_CODES)) + r")\b", re.I)
_CODE_RE = re.compile(r"\b[A-Z0-9]{4,}[-/A-Z0-9]*\b")


@dataclass(slots=True)
class ProbeConfig:
    out_dir: Path
    max_pages: int = 800
    max_product_pages_to_save: int = 50


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
            "message": "Login failed. Check login form and probe output.",
        }

    start_urls = [
        client.base_url,
        login_report.get("landing_url") or client.base_url,
    ]
    pages = client.crawl_same_host(
        start_urls=start_urls,
        max_pages=config.max_pages,
    )

    _write_json(out_dir / "crawl_pages.json", _strip_html_for_main_dump(pages))

    analyzed = [_analyze_page(page) for page in pages if page.get("kind") != "error"]
    _write_json(out_dir / "pages_analyzed.json", analyzed)

    product_like = [x for x in analyzed if x.get("page_kind") == "product_like"]
    category_like = [x for x in analyzed if x.get("page_kind") == "category_like"]
    other_pages = [x for x in analyzed if x.get("page_kind") == "other"]

    _write_json(out_dir / "product_pages.json", product_like)
    _write_json(out_dir / "category_pages.json", category_like)
    _write_json(out_dir / "other_pages.json", other_pages)

    sample_products = _pick_sample_products(product_like)
    _write_json(out_dir / "sample_products.json", sample_products)

    _write_product_html_samples(out_dir / "sample_html", pages, sample_products, config.max_product_pages_to_save)
    _write_csv(out_dir / "sample_products.csv", sample_products)
    _write_json(out_dir / "filter_summary.json", build_filter_summary(sample_products))

    discovered_endpoints = _collect_discovered_endpoints(pages)
    _write_json(out_dir / "discovered_endpoints.json", discovered_endpoints)

    summary = {
        "ok": True,
        "out_dir": str(out_dir),
        "pages_total": len(pages),
        "pages_analyzed": len(analyzed),
        "product_like_pages": len(product_like),
        "category_like_pages": len(category_like),
        "other_pages": len(other_pages),
        "sample_products": len(sample_products),
        "discovered_endpoints": len(discovered_endpoints),
        "filter_summary": build_filter_summary(sample_products),
        "notes": [
            "This is a temporary probe layer.",
            "After structure is understood, delete probe files and create the final VTT adapter.",
        ],
    }
    _write_json(out_dir / "summary.json", summary)
    return summary


def _analyze_page(page: dict[str, Any]) -> dict[str, Any]:
    html = page.get("html") or ""
    url = str(page.get("url") or "")
    title = str(page.get("title") or "")
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    h1_text = " ".join(h1.get_text(" ", strip=True).split()) if h1 else ""

    meta_desc = ""
    meta_tag = soup.find("meta", attrs={"name": "description"})
    if meta_tag:
        meta_desc = (meta_tag.get("content") or "").strip()

    breadcrumb = [
        " ".join(a.get_text(" ", strip=True).split())
        for a in soup.select("[class*=breadcrumb] a, nav[aria-label*=breadcrumb] a")
        if a.get_text(strip=True)
    ]

    tables = _extract_tables(soup)
    images = _extract_images(soup, url)
    scripts = _extract_json_blobs(soup)

    text = " ".join(soup.get_text(" ", strip=True).split())
    price_candidates = _extract_prices(text)
    category_codes = sorted(set(m.upper() for m in _CATEGORY_RE.findall(text + " " + title)))
    code_candidates = _extract_code_candidates(title + " " + text)
    passes_prefix = title_passes_prefix_filter(h1_text or title).allowed
    passes_category = any(category_allowed(c) for c in category_codes)

    return {
        "url": url,
        "page_kind": page.get("kind") or "other",
        "title": title,
        "h1": h1_text,
        "meta_description": meta_desc,
        "breadcrumbs": breadcrumb,
        "category_codes_found": category_codes,
        "passes_category": passes_category,
        "passes_prefix": passes_prefix,
        "images": images[:20],
        "tables": tables[:10],
        "json_blobs": scripts[:20],
        "price_candidates": price_candidates[:10],
        "code_candidates": code_candidates[:30],
        "has_add_to_cart": ("в корзину" in text.lower()) or ("add to cart" in text.lower()),
        "text_snippet": text[:5000],
    }


def _extract_tables(soup: BeautifulSoup) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = " ".join(cells[0].get_text(" ", strip=True).split())
            value = " ".join(cells[1].get_text(" ", strip=True).split())
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
        if abs_url not in seen:
            seen.add(abs_url)
            out.append(abs_url)
    return out


def _extract_json_blobs(soup: BeautifulSoup) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for tag in soup.find_all("script"):
        raw = (tag.string or tag.get_text() or "").strip()
        if not raw:
            continue
        script_type = (tag.get("type") or "").strip().lower()
        if "json" in script_type:
            try:
                parsed = json.loads(raw)
                out.append({"type": script_type, "parsed": _safe_small_json(parsed)})
                continue
            except Exception:  # noqa: BLE001
                pass

        candidates = re.findall(r"(\{.*?\}|\[.*?\])", raw, re.S)
        for cand in candidates[:5]:
            if len(cand) < 2 or len(cand) > 30000:
                continue
            try:
                parsed = json.loads(cand)
                out.append({"type": "embedded_json", "parsed": _safe_small_json(parsed)})
                break
            except Exception:  # noqa: BLE001
                continue
    return out


def _extract_prices(text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for match in _PRICE_RE.findall(text):
        cleaned = " ".join(match.split())
        if len(cleaned) < 2:
            continue
        if cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


def _extract_code_candidates(text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for match in _CODE_RE.findall(text):
        token = match.strip(" -_/")
        if len(token) < 4:
            continue
        if token not in seen:
            seen.add(token)
            out.append(token)
    return out


def _pick_sample_products(product_like: list[dict[str, Any]]) -> list[dict[str, Any]]:
    picked: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    seen_categories: set[str] = set()

    for page in product_like:
        title = page.get("h1") or page.get("title") or ""
        prefix_res = title_passes_prefix_filter(str(title))
        cats = [c for c in page.get("category_codes_found") or [] if category_allowed(c)]
        if not (prefix_res.allowed or cats):
            continue
        for cat in cats:
            if cat in seen_categories:
                continue
            item = {
                "url": page["url"],
                "title": title,
                "matched_prefix": prefix_res.matched_prefix,
                "category_code": cat,
                "passes_prefix": prefix_res.allowed,
                "passes_category": True,
                "price_candidates": page.get("price_candidates") or [],
                "images_count": len(page.get("images") or []),
                "images": (page.get("images") or [])[:10],
                "tables": (page.get("tables") or [])[:30],
                "code_candidates": (page.get("code_candidates") or [])[:30],
                "breadcrumbs": page.get("breadcrumbs") or [],
                "text_snippet": page.get("text_snippet") or "",
            }
            picked.append(item)
            seen_categories.add(cat)
            seen_urls.add(page["url"])
            break

    for page in product_like:
        if page["url"] in seen_urls:
            continue
        title = page.get("h1") or page.get("title") or ""
        prefix_res = title_passes_prefix_filter(str(title))
        cats = [c for c in page.get("category_codes_found") or [] if category_allowed(c)]
        if not prefix_res.allowed and not cats:
            continue
        item = {
            "url": page["url"],
            "title": title,
            "matched_prefix": prefix_res.matched_prefix,
            "category_code": cats[0] if cats else None,
            "passes_prefix": prefix_res.allowed,
            "passes_category": bool(cats),
            "price_candidates": page.get("price_candidates") or [],
            "images_count": len(page.get("images") or []),
            "images": (page.get("images") or [])[:10],
            "tables": (page.get("tables") or [])[:30],
            "code_candidates": (page.get("code_candidates") or [])[:30],
            "breadcrumbs": page.get("breadcrumbs") or [],
            "text_snippet": page.get("text_snippet") or "",
        }
        picked.append(item)
        seen_urls.add(page["url"])
        if len(picked) >= max(25, len(DEFAULT_ALLOWED_CATEGORY_CODES)):
            break

    return picked


def _collect_discovered_endpoints(pages: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for page in pages:
        for url in page.get("api_like_links") or []:
            path = urlparse(url).path.lower()
            if not any(x in path for x in ["api", "json", "ajax", "catalog", "item", "product", "goods"]):
                continue
            if url not in seen:
                seen.add(url)
                out.append(url)
    return out[:500]


def _strip_html_for_main_dump(pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for page in pages:
        row = dict(page)
        row.pop("html", None)
        cleaned.append(row)
    return cleaned


def _write_product_html_samples(
    sample_dir: Path,
    pages: list[dict[str, Any]],
    sample_products: list[dict[str, Any]],
    max_count: int,
) -> None:
    sample_dir.mkdir(parents=True, exist_ok=True)
    wanted = {x["url"] for x in sample_products[:max_count]}
    idx = 1
    for page in pages:
        if page.get("url") not in wanted:
            continue
        html = page.get("html") or ""
        path = sample_dir / f"{idx:03d}.html"
        path.write_text(html, encoding="utf-8", errors="ignore")
        idx += 1


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    columns = [
        "url",
        "title",
        "matched_prefix",
        "category_code",
        "passes_prefix",
        "passes_category",
        "price_candidates",
        "images_count",
        "code_candidates",
        "breadcrumbs",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _csv_value(row.get(k)) for k in columns})


def _csv_value(value: Any) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return "" if value is None else str(value)


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_small_json(value: Any) -> Any:
    text = json.dumps(value, ensure_ascii=False)
    if len(text) > 8000:
        return {"_truncated": True, "_preview": text[:8000]}
    return value
