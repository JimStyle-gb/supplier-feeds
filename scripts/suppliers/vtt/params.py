# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/params.py

Canonical VTT params layer under CS-template.

Назначение:
- держать главный extractor supplier-параметров;
- сохранять backward-safe public API старого params_page.py;
- отдавать source.py те же low-level helper-ы, что раньше:
  extract_title / extract_meta_desc / extract_price_rub / extract_sku /
  extract_images_from_html / extract_params_and_desc(_fast).

Важно:
- это фикс совместимости после ввода canonical params.py;
- логика intentionally оставлена максимально близкой к старому params_page.py,
  чтобы не ломать build_vtt.py и source.py.
"""

from __future__ import annotations

import html as ihtml
import re
from typing import List, Sequence, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .normalize import canon_vendor, norm_ws
from .pictures import clean_picture_urls


# ----------------------------- low-level HTML extractors -----------------------------

TAG_RE = re.compile(r"<[^>]+>")
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.I | re.S)
META_DESC_RE = re.compile(
    r"""<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["']""",
    re.I,
)
SKU_RE = re.compile(r"""let\s+sku\s*=\s*["']([^"']+)["']""", re.I)
PRICE_RUB_RE = re.compile(r"""let\s+priceRUB\s*=\s*([0-9]+(?:\.[0-9]+)?)""", re.I)
PRICE_MAIN_RE = re.compile(r"""price_main[^>]*>\s*<b>([^<]+)</b>""", re.I | re.S)
IMAGE_RE = re.compile(
    r"""(?:src|href|data-src|data-original|srcset)=["']([^"']+\.(?:jpg|jpeg|png|webp|gif)(?:\?[^"']*)?)["']""",
    re.I,
)
DESC_BLOCK_RE = re.compile(
    r"""<div[^>]+class=["'][^"']*(?:description|catalog_item_descr)[^"']*["'][^>]*>(.*?)</div>""",
    re.I | re.S,
)
DT_DD_RE = re.compile(r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>", re.I | re.S)
TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.I | re.S)
CELL_RE = re.compile(r"<(?:th|td)[^>]*>(.*?)</(?:th|td)>", re.I | re.S)
CODE_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9\-./]{2,}\b")


def html_text_fast(fragment: str) -> str:
    if not fragment:
        return ""
    text = TAG_RE.sub(" ", fragment)
    text = ihtml.unescape(text)
    return norm_ws(text)


def safe_int_from_text(text: str) -> int:
    s = norm_ws(text).replace(" ", "").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return 0
    try:
        return int(round(float(m.group(1))))
    except Exception:
        return 0


def extract_title(html: str) -> str:
    m = H1_RE.search(html)
    if m:
        return html_text_fast(m.group(1))
    m = TITLE_RE.search(html)
    return html_text_fast(m.group(1)) if m else ""


def extract_meta_desc(html: str) -> str:
    m = META_DESC_RE.search(html)
    return norm_ws(ihtml.unescape(m.group(1))) if m else ""


def extract_price_rub(html: str) -> int:
    m = PRICE_RUB_RE.search(html)
    if m:
        try:
            return int(round(float(m.group(1))))
        except Exception:
            pass
    m = PRICE_MAIN_RE.search(html)
    return safe_int_from_text(m.group(1)) if m else 0


def extract_sku(html: str) -> str:
    m = SKU_RE.search(html)
    return norm_ws(m.group(1)) if m else ""


def extract_images_from_html(page_url: str, html: str) -> list[str]:
    urls: list[str] = []
    for raw in IMAGE_RE.findall(html or ""):
        urls.append(urljoin(page_url, raw.strip()))
    return clean_picture_urls(urls)


def extract_params_and_desc_fast(html: str) -> tuple[list[tuple[str, str]], str]:
    params: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for key_html, val_html in DT_DD_RE.findall(html or ""):
        key = html_text_fast(key_html).strip(":")
        val = html_text_fast(val_html)
        if key and val and (key, val) not in seen:
            seen.add((key, val))
            params.append((key, val))

    if not params:
        for tr_html in TR_RE.findall(html or ""):
            cells = CELL_RE.findall(tr_html)
            if len(cells) < 2:
                continue
            key = html_text_fast(cells[0]).strip(":")
            val = html_text_fast(cells[1])
            if key and val and (key, val) not in seen:
                seen.add((key, val))
                params.append((key, val))

    desc = ""
    m = DESC_BLOCK_RE.search(html or "")
    if m:
        desc = html_text_fast(m.group(1))
    return params, desc


def extract_params_and_desc(html: str) -> tuple[list[tuple[str, str]], str]:
    params, desc = extract_params_and_desc_fast(html)
    if params or desc:
        return params, desc

    soup = BeautifulSoup(html or "", "lxml")
    params = []
    seen: set[tuple[str, str]] = set()

    for box in soup.select("div.description.catalog_item_descr, div.description"):
        dts = box.find_all("dt")
        dds = box.find_all("dd")
        if dts and dds:
            for dt, dd in zip(dts, dds):
                key = norm_ws(dt.get_text(" ", strip=True)).strip(":")
                val = norm_ws(dd.get_text(" ", strip=True))
                if key and val and (key, val) not in seen:
                    seen.add((key, val))
                    params.append((key, val))

    if not params:
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                key = norm_ws(cells[0].get_text(" ", strip=True)).strip(":")
                val = norm_ws(cells[1].get_text(" ", strip=True))
                if key and val and (key, val) not in seen:
                    seen.add((key, val))
                    params.append((key, val))

    if not desc:
        m = DESC_BLOCK_RE.search(html or "")
        if m:
            desc = html_text_fast(m.group(1))
    return params, desc


def extract_title_codes(title: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for code in CODE_TOKEN_RE.findall(title or ""):
        code = code.strip(".-/")
        if len(code) < 3 or not re.search(r"\d", code):
            continue
        if code not in seen:
            seen.add(code)
            out.append(code)
    return out


# ----------------------------- high-level params extractor -----------------------------

CODE_RX = re.compile(
    r"\b(?:"
    r"CF\d{3,4}[A-Z]?|CE\d{3,4}[A-Z]?|CB\d{3,4}[A-Z]?|CC\d{3,4}[A-Z]?|Q\d{4}[A-Z]?|W\d{4}[A-Z0-9]{1,4}|"
    r"MLT-[A-Z]\d{3,5}[A-Z0-9/]*|CLT-[A-Z]\d{3,5}[A-Z]?|TK-?\d{3,5}[A-Z0-9]*|"
    r"106R\d{5}|006R\d{5}|108R\d{5}|113R\d{5}|013R\d{5}|016\d{6}|"
    r"ML-D\d+[A-Z]?|ML-\d{4,5}[A-Z]\d?|KX-FA\d+[A-Z]?|KX-FAT\d+[A-Z]?|"
    r"C13T\d{5,8}[A-Z0-9]*|C12C\d{5,8}[A-Z0-9]*|C33S\d{5,8}[A-Z0-9]*|"
    r"C-?EXV\d+[A-Z]*|DR-\d+[A-Z0-9-]*|TN-\d+[A-Z0-9-]*|T-\d{3,6}[A-Z]?|"
    r"50F\d[0-9A-Z]{2,4}|55B\d[0-9A-Z]{2,4}|56F\d[0-9A-Z]{2,4}|0?71H|052H|041H?|651|727|934/?935"
    r")\b",
    re.I,
)

COMPAT_PATTERNS = [
    re.compile(r"(?iu)\b(?:для|used in|совместим(?:ость)? с)\s+([^.;\n]{4,220})"),
]

STOP_HEADERS_RX = re.compile(
    r"(?iu)\b(?:характеристики|описание|спецификация|технические характеристики|примечание|дополнительно)\b"
)
COMPAT_GUARD_RX = re.compile(r"(?iu)\b(?:ресурс|цвет|партномер|код|артикул|оригинальн)\b")

CABLE_TYPE_RX = re.compile(r"(?iu)\b(?:витая\s+пара|utp|ftp|stp|sftp)\b")
CABLE_CATEGORY_RX = re.compile(r"(?iu)\bcat\.?\s*(5e|6|6a|7)\b")
CABLE_DIM_RX = re.compile(r"(?iu)\b(\d+)\s*x\s*([0-9]+(?:[.,][0-9]+)?)\b")
CABLE_MATERIAL_RX = re.compile(r"(?iu)\b(?:cu|cca|copper|мед[ьи]|алюмини)\b")
CABLE_SPOOL_RX = re.compile(r"(?iu)\b(\d{2,4})\s*м(?:/б)?\b")

KEY_MAP = {
    "тип": "Тип",
    "для бренда": "Для бренда",
    "бренд": "Для бренда",
    "партномер": "Партномер",
    "каталожный номер": "Партномер",
    "oem-номер": "Партномер",
    "партс-номер": "Партномер",
    "совместимость": "Совместимость",
    "коды расходников": "Коды расходников",
    "технология печати": "Технология печати",
    "цвет": "Цвет",
    "ресурс": "Ресурс",
    "объем": "Объем",
    "объём": "Объем",
    "тип кабеля": "Тип кабеля",
    "категория": "Категория",
    "количество пар": "Количество пар",
    "толщина проводников": "Толщина проводников",
    "материал изоляции": "Материал изоляции",
    "бухта": "Бухта",
}

CABLE_PARAM_KEYS = {
    "Тип кабеля",
    "Категория",
    "Количество пар",
    "Толщина проводников",
    "Материал изоляции",
    "Бухта",
}


def safe_str(value: object) -> str:
    return str(value).strip() if value is not None else ""


def _norm_spaces(text: str) -> str:
    return " ".join(safe_str(text).replace("\xa0", " ").split()).strip()


def _dedupe_params(items: Sequence[Tuple[str, str]]) -> list[Tuple[str, str]]:
    out: list[Tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for key, value in items or []:
        k = safe_str(key)
        v = safe_str(value)
        if not k or not v:
            continue
        sig = (k.casefold(), v.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((k, v))
    return out


def _title_kind(title: str) -> str:
    t = safe_str(title).lower()
    mapping = [
        ("драм-картридж", "Драм-картридж"),
        ("драм-юнит", "Драм-юнит"),
        ("драм-юниты", "Драм-юнит"),
        ("драм юнит", "Драм-юнит"),
        ("тонер-картридж", "Тонер-картридж"),
        ("тонер-катридж", "Тонер-картридж"),
        ("копи-картридж", "Копи-картридж"),
        ("принт-картридж", "Принт-картридж"),
        ("картридж", "Картридж"),
        ("чернила", "Чернила"),
        ("печатающая головка", "Печатающая головка"),
        ("девелопер", "Девелопер"),
        ("кабель сетевой", "Кабель сетевой"),
        ("термоблок", "Термоблок"),
        ("контейнер", "Контейнер"),
        ("блок", "Блок"),
        ("бункер", "Бункер"),
        ("носитель", "Носитель"),
        ("фотобарабан", "Фотобарабан"),
        ("барабан", "Барабан"),
        ("тонер", "Тонер"),
        ("комплект", "Комплект"),
        ("набор", "Набор"),
        ("заправочный комплект", "Заправочный комплект"),
        ("рефил", "Рефил"),
    ]
    for prefix, value in mapping:
        if t.startswith(prefix):
            return value
    return ""


def _norm_color(value: str) -> str:
    s = _norm_spaces(value)
    low = s.casefold()
    if re.search(r"\b(black|ч[её]рн)", low):
        return "Чёрный"
    if re.search(r"\b(yellow|ж[её]лт)", low):
        return "Желтый"
    if re.search(r"\b(magenta|пурпурн|малинов)", low):
        return "Пурпурный"
    if re.search(r"\b(cyan|голуб|син)", low):
        return "Голубой"
    return s


def _trim_compat_tail(value: str) -> str:
    s = _norm_spaces(value).strip(" ;,.-")
    if not s:
        return ""
    s = STOP_HEADERS_RX.split(s, maxsplit=1)[0].strip(" ;,.-")
    while COMPAT_GUARD_RX.search(s) and any(x in s for x in (";", "|")):
        parts = re.split(r"[;|]+", s)
        if not parts:
            break
        s = _norm_spaces(parts[0]).strip(" ;,.-")
    return s


def _extract_compat_from_desc(text: str) -> str:
    s = _norm_spaces(text)
    if not s:
        return ""
    for rx in COMPAT_PATTERNS:
        m = rx.search(s)
        if m:
            val = _trim_compat_tail(m.group(1))
            if val and not COMPAT_GUARD_RX.search(val):
                return val
    return ""


def _extract_codes(title: str, text: str) -> str:
    found: list[str] = []
    seen: set[str] = set()
    hay = f"{safe_str(title)}\n{safe_str(text)}"
    for token in CODE_RX.findall(hay):
        code = _norm_spaces(token).upper()
        if not code or code in seen:
            continue
        seen.add(code)
        found.append(code)
    return ", ".join(found)


def _extract_cable_params_from_text(title: str, text: str) -> list[Tuple[str, str]]:
    joined = _norm_spaces(f"{title} {text}")
    if "кабель сетевой" not in joined.casefold():
        return []

    out: list[Tuple[str, str]] = []

    m = CABLE_TYPE_RX.search(joined)
    if m:
        out.append(("Тип кабеля", m.group(0).upper()))

    m = CABLE_CATEGORY_RX.search(joined)
    if m:
        out.append(("Категория", f"Cat.{m.group(1)}"))

    m = CABLE_DIM_RX.search(joined)
    if m:
        out.append(("Количество пар", m.group(1)))
        out.append(("Толщина проводников", m.group(2).replace(".", ",")))

    m = CABLE_MATERIAL_RX.search(joined)
    if m:
        out.append(("Материал изоляции", m.group(0).upper()))

    m = CABLE_SPOOL_RX.search(joined)
    if m:
        out.append(("Бухта", f"{m.group(1)} м/б"))

    return out


def _normalize_param_block(block: Sequence[Tuple[str, str]] | None) -> list[Tuple[str, str]]:
    out: list[Tuple[str, str]] = []
    for key, value in block or []:
        k = safe_str(key)
        v = safe_str(value)
        if not k or not v:
            continue
        out.append((k, v))
    return out


def _merge_raw_param_channels(
    *,
    page_params: Sequence[Tuple[str, str]] | None = None,
    raw_desc_pairs: Sequence[Tuple[str, str]] | None = None,
    raw_table_params: Sequence[Tuple[str, str]] | None = None,
) -> list[Tuple[str, str]]:
    merged: list[Tuple[str, str]] = []
    merged.extend(_normalize_param_block(raw_table_params))
    merged.extend(_normalize_param_block(raw_desc_pairs))
    merged.extend(_normalize_param_block(page_params))
    return merged


def extract_page_params(
    *,
    title: str,
    description: str = "",
    extract_desc: str | None = None,
    page_params: Sequence[Tuple[str, str]] | None = None,
    raw_desc_pairs: Sequence[Tuple[str, str]] | None = None,
    raw_table_params: Sequence[Tuple[str, str]] | None = None,
) -> List[Tuple[str, str]]:
    text_body = safe_str(extract_desc) or safe_str(description)
    merged_page_params = _merge_raw_param_channels(
        page_params=page_params,
        raw_desc_pairs=raw_desc_pairs,
        raw_table_params=raw_table_params,
    )

    out: list[Tuple[str, str]] = []

    kind = _title_kind(title)
    if kind:
        out.append(("Тип", kind))

    for key, value in merged_page_params:
        k = safe_str(key).casefold()
        v = safe_str(value)
        if not k or not v:
            continue
        norm_key = KEY_MAP.get(k, "")
        if not norm_key:
            continue
        if norm_key == "Цвет":
            v = _norm_color(v)
        elif kind == "Кабель сетевой" and norm_key in CABLE_PARAM_KEYS:
            v = _norm_spaces(v)
        out.append((norm_key, v))

    if kind == "Кабель сетевой":
        out.extend(_extract_cable_params_from_text(title, text_body))

    compat = _extract_compat_from_desc(text_body)
    if compat:
        out.append(("Совместимость", compat))

    codes = _extract_codes(title, text_body)
    if codes:
        out.append(("Коды расходников", codes))

    title_low = safe_str(title).lower()
    if "yellow" in title_low and not any(k == "Цвет" for k, _ in out):
        out.append(("Цвет", "Желтый"))
    if "magenta" in title_low and not any(k == "Цвет" for k, _ in out):
        out.append(("Цвет", "Пурпурный"))
    if "black" in title_low and not any(k == "Цвет" for k, _ in out):
        out.append(("Цвет", "Чёрный"))

    return _dedupe_params(out)


# Public aliases for fill-missing layer / future cleanup
trim_compat_tail = _trim_compat_tail
extract_compat_from_text = _extract_compat_from_desc
extract_codes_from_text = _extract_codes
norm_spaces = _norm_spaces


__all__ = [
    "TAG_RE",
    "TITLE_RE",
    "H1_RE",
    "META_DESC_RE",
    "SKU_RE",
    "PRICE_RUB_RE",
    "PRICE_MAIN_RE",
    "IMAGE_RE",
    "DESC_BLOCK_RE",
    "DT_DD_RE",
    "TR_RE",
    "CELL_RE",
    "CODE_TOKEN_RE",
    "CODE_RX",
    "COMPAT_PATTERNS",
    "STOP_HEADERS_RX",
    "COMPAT_GUARD_RX",
    "CABLE_TYPE_RX",
    "CABLE_CATEGORY_RX",
    "CABLE_DIM_RX",
    "CABLE_MATERIAL_RX",
    "CABLE_SPOOL_RX",
    "html_text_fast",
    "safe_int_from_text",
    "extract_title",
    "extract_meta_desc",
    "extract_price_rub",
    "extract_sku",
    "extract_images_from_html",
    "extract_params_and_desc_fast",
    "extract_params_and_desc",
    "extract_title_codes",
    "safe_str",
    "norm_spaces",
    "trim_compat_tail",
    "extract_compat_from_text",
    "extract_codes_from_text",
    "_norm_spaces",
    "_trim_compat_tail",
    "_extract_compat_from_desc",
    "_extract_codes",
    "extract_page_params",
]
