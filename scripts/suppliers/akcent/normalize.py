# -*- coding: utf-8 -*-
"""
AkCent supplier normalization layer.

Что делает:
- нормализует supplier-данные до стабильного вида
- выбирает vendor / availability / price_in
- не лезет в core и не строит финальный XML
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from typing import Any

from suppliers.akcent.source import SourceOffer


_WS_RE = re.compile(r"\s+")
_PRICE_NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_BAD_VENDOR_VALUES = {
    "",
    "brand",
    "vendor",
    "мфу",
    "интерактивная",
    "интерактивные",
    "интерактивный",
    "ноутбук",
    "ноутбуки",
    "монитор",
    "мониторы",
    "принтер",
    "принтеры",
    "сканер",
    "сканеры",
    "компьютер",
    "компьютеры",
    "планшет",
    "планшеты",
    "телевизор",
    "телевизоры",
    "сервер",
    "серверы",
    "пк",
    "pc",
}


@dataclass(slots=True)
class NormalizedOffer:
    # Идентификация
    oid: str
    article: str
    source_type: str

    # Базовые поля
    name: str
    vendor: str
    model: str
    url: str
    category_id: str

    # Статусы / цены
    available: bool
    price_in: float

    # Описание / мета
    description: str
    manufacturer_warranty: str
    stock_text: str

    # Коллекции
    pictures: list[str] = field(default_factory=list)
    xml_params: list[tuple[str, str]] = field(default_factory=list)
    prices: list[dict[str, str]] = field(default_factory=list)

    # Отладка
    raw_vendor: str = ""
    raw_name: str = ""
    raw_model: str = ""


def _norm_space(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = s.replace("\xa0", " ").replace("\ufeff", " ")
    return _norm_space(s)


def _strip_html(s: str) -> str:
    return _norm_space(_HTML_TAG_RE.sub(" ", html.unescape(s or "")))


def _ci(s: str) -> str:
    return _norm_space(s).casefold()


def _pick_oid(src: SourceOffer) -> str:
    for candidate in (src.oid, src.article, src.offer_id):
        val = _clean_text(candidate)
        if val:
            return val
    return ""


def _unique_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        vv = _clean_text(v)
        if not vv:
            continue
        key = vv.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(vv)
    return out


def _cleanup_name(name: str, vendor: str) -> str:
    name = _clean_text(name)

    # Если vendor продублирован в начале 2 раза — схлопываем
    if vendor:
        v = re.escape(vendor)
        name = re.sub(rf"^(?:{v}\s+)+{v}\b", vendor, name, flags=re.IGNORECASE)

    # Убираем мусорные табы/переносы, уже сведённые в _clean_text
    return name


def _cleanup_model(model: str) -> str:
    return _clean_text(model)


def _param_value(xml_params: list[tuple[str, str]], *names: str) -> str:
    wanted = {_ci(x) for x in names}
    for k, v in xml_params:
        if _ci(k) in wanted and _clean_text(v):
            return _clean_text(v)
    return ""


def _is_bad_vendor(vendor: str) -> bool:
    v = _ci(vendor)
    if not v:
        return True
    if v in _BAD_VENDOR_VALUES:
        return True
    if len(v) <= 1:
        return True
    if re.fullmatch(r"\d+", v):
        return True
    return False


def _title_case_brand(s: str) -> str:
    s = _clean_text(s)
    if not s:
        return ""

    upper_map = {
        "hp": "HP",
        "apc": "APC",
        "dell": "Dell",
        "lenovo": "Lenovo",
        "asus": "ASUS",
        "acer": "Acer",
        "msi": "MSI",
        "lg": "LG",
        "aoc": "AOC",
        "benq": "BenQ",
        "hikvision": "Hikvision",
        "dahua": "Dahua",
        "tp-link": "TP-Link",
        "canon": "Canon",
        "epson": "Epson",
        "xerox": "Xerox",
        "pantum": "Pantum",
        "brother": "Brother",
        "kyocera": "Kyocera",
        "ricoh": "Ricoh",
        "samsung": "Samsung",
        "lexmark": "Lexmark",
        "sharp": "Sharp",
        "konica minolta": "Konica Minolta",
        "smart": "SMART",
        "viewsonic": "ViewSonic",
    }

    key = s.casefold()
    if key in upper_map:
        return upper_map[key]

    # Оставляем multi-word бренды читабельными
    if " " in s:
        return " ".join(x[:1].upper() + x[1:] if x else "" for x in s.split())

    return s[:1].upper() + s[1:]


def _infer_vendor_from_text(text: str) -> str:
    text_cf = _ci(text)

    patterns: list[tuple[str, str]] = [
        (r"\bhp\b|hewlett[\s\-]*packard", "HP"),
        (r"\bcanon\b", "Canon"),
        (r"\bepson\b", "Epson"),
        (r"\bxerox\b", "Xerox"),
        (r"\bpantum\b", "Pantum"),
        (r"\bbrother\b", "Brother"),
        (r"\bkyocera\b", "Kyocera"),
        (r"\bricoh\b", "Ricoh"),
        (r"\blexmark\b", "Lexmark"),
        (r"\bsamsung\b", "Samsung"),
        (r"\bsharp\b", "Sharp"),
        (r"\bkonica\s+minolta\b", "Konica Minolta"),
        (r"\bsmart\b", "SMART"),
        (r"\bviewsonic\b", "ViewSonic"),
        (r"\bbenq\b", "BenQ"),
        (r"\bacer\b", "Acer"),
        (r"\basus\b", "ASUS"),
        (r"\blenovo\b", "Lenovo"),
        (r"\bdell\b", "Dell"),
        (r"\bmsi\b", "MSI"),
        (r"\blg\b", "LG"),
        (r"\baoc\b", "AOC"),
        (r"\bapc\b", "APC"),
        (r"\bhikvision\b", "Hikvision"),
        (r"\bdahua\b", "Dahua"),
        (r"\btp[\s\-]*link\b", "TP-Link"),
    ]

    for pattern, brand in patterns:
        if re.search(pattern, text_cf, flags=re.IGNORECASE):
            return brand

    return ""


def _choose_vendor(src: SourceOffer) -> str:
    # 1) Прямой vendor из XML
    vendor = _clean_text(src.vendor)
    if not _is_bad_vendor(vendor):
        return _title_case_brand(vendor)

    # 2) Явные params бренда / производителя
    param_vendor = _param_value(
        src.xml_params,
        "Бренд",
        "Производитель",
        "Vendor",
        "Brand",
        "Торговая марка",
    )
    if not _is_bad_vendor(param_vendor):
        return _title_case_brand(param_vendor)

    # 3) name + model + description
    inferred = _infer_vendor_from_text(
        " ".join(
            [
                src.name or "",
                src.model or "",
                _strip_html(src.description or ""),
            ]
        )
    )
    if inferred:
        return inferred

    return ""


def _parse_number(text: str) -> float:
    raw = _clean_text(text).replace(" ", "")
    match = _PRICE_NUM_RE.search(raw)
    if not match:
        return 0.0
    val = match.group(0).replace(",", ".")
    try:
        return float(val)
    except Exception:
        return 0.0


def _price_priority(price_type: str) -> int:
    t = _ci(price_type)
    priorities = [
        "розничная",
        "retail",
        "rrc",
        "base",
        "price",
        "цена продажи",
        "основная",
    ]
    for idx, token in enumerate(priorities):
        if token in t:
            return idx
    return 999


def _choose_price_in(prices: list[dict[str, str]]) -> float:
    candidates: list[tuple[int, float]] = []

    for row in prices:
        val = _parse_number(str(row.get("value") or ""))
        if val > 0:
            prio = _price_priority(str(row.get("type") or ""))
            candidates.append((prio, val))

    if not candidates:
        return 0.0

    candidates.sort(key=lambda x: (x[0], x[1]))
    return float(candidates[0][1])


def _stock_implies_available(stock_text: str) -> bool | None:
    text = _ci(stock_text)
    if not text:
        return None

    if any(x in text for x in ["нет", "отсутств", "под заказ", "ожидается", "ожидание"]):
        return False
    if any(x in text for x in ["есть", "в наличии", "доступ", "склад"]):
        return True

    num = _parse_number(stock_text)
    if num > 0:
        return True
    if num == 0 and re.search(r"\b0+\b", text):
        return False

    return None


def _choose_available(src: SourceOffer) -> bool:
    by_stock = _stock_implies_available(src.stock_text)
    if by_stock is not None:
        return by_stock
    return bool(src.available)


def normalize_offer(src: SourceOffer) -> NormalizedOffer:
    vendor = _choose_vendor(src)
    model = _cleanup_model(src.model)

    # Иногда model пустой, но есть одноимённый param
    if not model:
        model = _param_value(src.xml_params, "Модель", "Model")

    name = _cleanup_name(src.name, vendor)
    price_in = _choose_price_in(src.prices)
    available = _choose_available(src)

    return NormalizedOffer(
        oid=_pick_oid(src),
        article=_clean_text(src.article),
        source_type=_clean_text(src.type_name),
        name=name,
        vendor=vendor,
        model=model,
        url=_clean_text(src.url),
        category_id=_clean_text(src.category_id),
        available=available,
        price_in=price_in,
        description=(src.description or "").strip(),
        manufacturer_warranty=_clean_text(src.manufacturer_warranty),
        stock_text=_clean_text(src.stock_text),
        pictures=_unique_keep_order(src.pictures),
        xml_params=[(_clean_text(k), _clean_text(v)) for k, v in src.xml_params if _clean_text(k)],
        prices=src.prices[:],
        raw_vendor=_clean_text(src.vendor),
        raw_name=_clean_text(src.name),
        raw_model=_clean_text(src.model),
    )


def normalize_offers(source_offers: list[SourceOffer]) -> tuple[list[NormalizedOffer], dict[str, Any]]:
    out: list[NormalizedOffer] = []
    vendor_fallback_used = 0
    zero_price_count = 0
    unavailable_count = 0

    for src in source_offers:
        norm = normalize_offer(src)
        out.append(norm)

        if _is_bad_vendor(src.vendor) and norm.vendor:
            vendor_fallback_used += 1
        if norm.price_in <= 0:
            zero_price_count += 1
        if not norm.available:
            unavailable_count += 1

    report: dict[str, Any] = {
        "normalized": len(out),
        "vendor_fallback_used": vendor_fallback_used,
        "zero_price_count": zero_price_count,
        "unavailable_count": unavailable_count,
    }
    return out, report
