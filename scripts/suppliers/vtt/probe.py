# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/probe.py

VTT temporary probe.
v3:
- больше не использует category-gate;
- работает в prefix-first режиме;
- обходит только /catalog;
- старается отличать реальные товарные карточки от шума;
- категории сохраняет только как диагностику.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup

from suppliers.vtt.filtering import build_filter_summary, title_passes_prefix_filter


_PRICE_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[ \u00A0]?\d{3})+|\d+)(?:[.,]\d{1,2})?\s*(?:₽|руб|тенге|kzt)?", re.I)
_CODE_RE = re.compile(r"\b[A-Z0-9]{4,}[-/A-Z0-9]*\b")
_CATEGORY_CODE_RE = re.compile(r"\b[A-Z_]{4,}\b")


@dataclass(slots=True)
class ProbeConfig:
    out_dir: Path
    max_pages: int = 1000
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
            "message": "Login failed. Check login_report.json.",
        }

    pages = client.crawl_same_host(
        start_urls=[login_report.get("catalog_url") or "/catalog/"],
        max_pages=config.max_pages,
        allow_paths=["/catalog"],
    )
    _write_json(out_dir / "crawl_pages.json", _strip_html_for_main_dump(pages))

    analyzed = [_analyze_page(page) for page in pages if page.get("kind") != "error"]
    _write_json(out_dir / "pages_analyzed.json", analyzed)

    product_confident = [x for x in analyzed if x.get("product_confident")]
    listing_like = [x for x in analyzed if x.get("page_kind") == "listing_like"]
    catalog_other = [x for x in analyzed if not x.get("product_confident") and x.get("page_kind") != "listing_like"]

    _write_json(out_dir / "product_pages.json", product_confident)
    _write_json(out_dir / "category_pages.json", listing_like)
    _write_json(out_dir / "other_pages.json", catalog_other)

    sample_products = _pick_sample_products(product_confident)
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
        "product_like_pages": len(product_confident),
        "category_like_pages": len(listing_like),
        "other_pages": len(catalog_other),
        "sample_products": len(sample_products),
        "discovered_endpoints": len(discovered_endpoints),
        "filter_summary": build_filter_summary(sample_products),
        "notes": [
            "Temporary VTT probe in prefix-first mode.",
            "Categories are diagnostics only and do not gate selection.",
            "After VTT structure is understood, delete probe files and create final adapter.",
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
    title_for_filter = h1_text or title
    prefix_res = title_passes_prefix_filter(title_for_filter)

    breadcrumb = [
        " ".join(a.get_text(" ", strip=True).split())
        for a in soup.select("[class*=breadcrumb] a, nav[aria-label*=breadcrumb] a")
        if a.get_text(strip=True)
    ]

    tables = _extract_tables(soup)
    images = _extract_images(soup, url)
    text = " ".join(soup.get_text(" ", strip=True).split())
    price_candidates = _extract_prices(text)
    code_candidates = _extract_code_candidates(f"{title_for_filter} {text}")
    category_codes = _extract_category_codes(url, text, breadcrumb)

    has_add_to_cart = ("в корзину" in text.lower()) or ("add to cart" in text.lower())
    service_noise = _service_noise_score(url, text)
    product_score = 0
    if has_add_to_cart:
        product_score += 4
    if price_candidates:
        product_score += 2
    if tables:
        product_score += 2
    if images:
        product_score += 1
    if code_candidates:
        product_score += 1
    if "/catalog/" in url.lower():
        product_score += 1
    product_score -= service_noise

    page_kind = str(page.get("kind") or "other")
    if page_kind not in {"product_like", "listing_like", "catalog_like"}:
        page_kind = "catalog_like" if "/catalog" in url.lower() else "other"

    product_confident = product_score >= 5 and not _looks_like_listing(url, text, tables)

    return {
        "url": url,
        "page_kind": page_kind,
        "title": title,
        "h1": h1_text,
        "normalized_title": prefix_res.normalized_title,
        "matched_prefix": prefix_res.matched_prefix,
        "passes_prefix": prefix_res.allowed,
        "product_score": product_score,
        "product_confident": product_confident,
        "breadcrumbs": breadcrumb,
        "category_codes_found": category_codes,
        "images": images[:20],
        "tables": tables[:30],
        "price_candidates": price_candidates[:10],
        "code_candidates": code_candidates[:30],
        "has_add_to_cart": has_add_to_cart,
        "text_snippet": text[:5000],
    }


def _looks_like_listing(url: str, text: str, tables: list[dict[str, str]]) -> bool:
    lower = text.lower()
    if "категории товаров" in lower or "все товары каталога" in lower:
        return True
    if "показать еще" in lower or "показать ещё" in lower:
        return True
    if "фильтр" in lower and "сортировка" in lower:
        return True
    if len(tables) == 0 and lower.count("в корзину") > 5:
        return True
    query = parse_qs(urlparse(url).query)
    if "category" in query and not any(x in lower for x in ["артикул", "модель", "ресурс", "совместимость"]):
        return True
    return False


def _service_noise_score(url: str, text: str) -> int:
    lower = text.lower()
    score = 0
    noise_markers = [
        "документы",
        "претензии",
        "помощь",
        "аккаунт",
        "профиль",
        "резервы",
        "прогнозы",
        "extra-баллы",
        "редактировать список",
        "мои категории",
    ]
    for marker in noise_markers:
        if marker in lower:
            score += 2
    if re.search(r"/(help|account|documents|claims|forecast|reserve)", url, re.I):
        score += 4
    return score


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
        if "yandex.ru/watch" in abs_url:
            continue
        if abs_url not in seen:
            seen.add(abs_url)
            out.append(abs_url)
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


def _extract_category_codes(url: str, text: str, breadcrumbs: list[str]) -> list[str]:
    found: set[str] = set()
    query = parse_qs(urlparse(url).query)
    for value in query.get("category", []):
        if value:
            found.add(value.strip().upper())
    for token in _CATEGORY_CODE_RE.findall(" ".join(breadcrumbs) + " " + text[:2000]):
        if "_" in token and len(token) >= 6:
            found.add(token.strip().upper())
    return sorted(found)


def _pick_sample_products(product_pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    picked: list[dict[str, Any]] = []
    seen_url: set[str] = set()
    seen_prefix: set[str] = set()

    # сначала хотим хотя бы по одному товару на каждый prefix
    for page in sorted(product_pages, key=lambda x: (-int(x.get("product_score") or 0), x.get("url") or "")):
        if page["url"] in seen_url:
            continue
        if not page.get("passes_prefix"):
            continue
        prefix = str(page.get("matched_prefix") or "")
        if prefix in seen_prefix:
            continue
        picked.append(_sample_row(page))
        seen_url.add(page["url"])
        seen_prefix.add(prefix)

    # потом добиваем ещё товары только с prefix match
    for page in sorted(product_pages, key=lambda x: (-int(x.get("product_score") or 0), x.get("url") or "")):
        if page["url"] in seen_url:
            continue
        if not page.get("passes_prefix"):
            continue
        picked.append(_sample_row(page))
        seen_url.add(page["url"])
        if len(picked) >= 50:
            break

    return picked


def _sample_row(page: dict[str, Any]) -> dict[str, Any]:
    return {
        "url": page["url"],
        "title": page.get("h1") or page.get("title") or "",
        "normalized_title": page.get("normalized_title") or "",
        "matched_prefix": page.get("matched_prefix"),
        "passes_prefix": bool(page.get("passes_prefix")),
        "product_confident": bool(page.get("product_confident")),
        "product_score": int(page.get("product_score") or 0),
        "category_codes_found": page.get("category_codes_found") or [],
        "price_candidates": page.get("price_candidates") or [],
        "images_count": len(page.get("images") or []),
        "images": (page.get("images") or [])[:10],
        "tables": (page.get("tables") or [])[:30],
        "code_candidates": (page.get("code_candidates") or [])[:30],
        "breadcrumbs": page.get("breadcrumbs") or [],
        "text_snippet": page.get("text_snippet") or "",
    }


def _collect_discovered_endpoints(pages: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for page in pages:
        for url in page.get("api_like_links") or []:
            path = urlparse(url).path.lower()
            if not any(x in path for x in ["api", "json", "ajax", "catalog", "item", "product", "goods", "search"]):
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


def _write_product_html_samples(sample_dir: Path, pages: list[dict[str, Any]], sample_products: list[dict[str, Any]], max_count: int) -> None:
    sample_dir.mkdir(parents=True, exist_ok=True)
    wanted = {x["url"] for x in sample_products[:max_count]}
    idx = 1
    for page in pages:
        if page.get("url") not in wanted:
            continue
        html = page.get("html") or ""
        (sample_dir / f"{idx:03d}.html").write_text(html, encoding="utf-8", errors="ignore")
        idx += 1


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    columns = [
        "url",
        "title",
        "normalized_title",
        "matched_prefix",
        "passes_prefix",
        "product_confident",
        "product_score",
        "category_codes_found",
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
