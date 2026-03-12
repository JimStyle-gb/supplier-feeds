# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/normalize.py

AkCent supplier normalization layer.

Общая шаблонная логика:
- supplier-layer сам нормализует id / name / vendor / available / price_in
- core не должен угадывать supplier-specific данные

Индивидуальное для AkCent:
- vendor часто пустой или слабый -> нужен fallback из name/description/params
- category/type не всегда надёжны
- цены имеют явный приоритет:
  1) "Цена дилерского портала KZT"
  2) RRP
  3) любой другой положительный price
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from typing import Any

from suppliers.akcent.source import SourceOffer


_WS_RE = re.compile(r"\s+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")
_LATIN_BRAND_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9.+\-/]{1,}")
_BRAND_SPLIT_RE = re.compile(r"[^A-Za-zА-Яа-яЁё0-9.+\-/]+")

# Явно слабые / мусорные "vendor" значения, которые нельзя считать брендом
_BAD_VENDOR_VALUES = {
    "",
    "brand",
    "vendor",
    "производитель",
    "бренд",
    "мфу",
    "ноутбук",
    "ноутбуки",
    "монитор",
    "мониторы",
    "компьютер",
    "компьютеры",
    "сервер",
    "серверы",
    "интерактивная",
    "интерактивный",
    "интерактивные",
    "панель",
    "панели",
    "гарнитура",
    "кабель",
    "патч-корд",
    "картридж",
    "принтер",
    "сканер",
    "аксессуар",
    "аксессуары",
    "товар",
    "device",
    "product",
}

# Частые бренды/вендоры, которые у AkCent встречаются в name/description
# Нужны как страховка, если supplier vendor пустой.
_KNOWN_BRANDS = {
    "a4tech": "A4Tech",
    "acer": "Acer",
    "aoc": "AOC",
    "apc": "APC",
    "apple": "Apple",
    "asus": "ASUS",
    "benq": "BenQ",
    "brother": "Brother",
    "cablexpert": "Cablexpert",
    "canon": "Canon",
    "cisco": "Cisco",
    "cooler master": "Cooler Master",
    "corsair": "Corsair",
    "dahua": "Dahua",
    "dell": "Dell",
    "defender": "Defender",
    "epson": "Epson",
    "fiya": "Fiya",
    "fujitsu": "Fujitsu",
    "gembird": "Gembird",
    "gigabyte": "Gigabyte",
    "hikvision": "Hikvision",
    "hpe": "HPE",
    "hp": "HP",
    "hewlett packard": "HP",
    "ibm": "IBM",
    "intel": "Intel",
    "jabra": "Jabra",
    "juniper": "Juniper",
    "kaspersky": "Kaspersky",
    "kingston": "Kingston",
    "kyocera": "Kyocera",
    "lenovo": "Lenovo",
    "lexmark": "Lexmark",
    "lg": "LG",
    "logitech": "Logitech",
    "microsoft": "Microsoft",
    "mikrotik": "MikroTik",
    "msi": "MSI",
    "olmio": "OLMIO",
    "pantum": "Pantum",
    "philips": "Philips",
    "plantronics": "Plantronics",
    "powercom": "Powercom",
    "qnap": "QNAP",
    "ricoh": "Ricoh",
    "ritmix": "Ritmix",
    "samsung": "Samsung",
    "sandisk": "SanDisk",
    "seagate": "Seagate",
    "sharp": "Sharp",
    "smart": "SMART",
    "sony": "Sony",
    "synology": "Synology",
    "tenda": "Tenda",
    "toshiba": "Toshiba",
    "tp-link": "TP-Link",
    "transcend": "Transcend",
    "ubiquiti": "Ubiquiti",
    "viewsonic": "ViewSonic",
    "wd": "WD",
    "western digital": "WD",
    "xerox": "Xerox",
    "zebra": "Zebra",
    "zyxel": "Zyxel",
}


@dataclass(slots=True)
class NormalizedOffer:
    # Идентификация
    raw_oid: str
    oid: str
    article: str
    offer_id: str
    source_type: str

    # Базовые поля
    name: str
    vendor: str
    model: str
    url: str
    category_id: str

    # Статусы / цены
    available: bool
    price_in: int | None

    # Описание / сервисные поля
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


def _norm_ws(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _ci(s: str) -> str:
    return _norm_ws(s).casefold()


def _strip_html(s: str) -> str:
    s = html.unescape(s or "")
    s = _HTML_TAG_RE.sub(" ", s)
    return _norm_ws(s)


def _clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = s.replace("\xa0", " ").replace("\ufeff", " ").replace("\u200b", " ")
    return _norm_ws(s)


def _normalize_name(name: str) -> str:
    s = _clean_text(name)
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip(" ;|")


def _normalize_model(model: str) -> str:
    s = _clean_text(model)
    s = re.sub(r"^\s*(?:модель|model)\s*[:\-]\s*", "", s, flags=re.IGNORECASE)
    return s.strip(" ;|")


def _dedupe_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for raw in values:
        val = _clean_text(raw)
        if not val:
            continue
        key = val.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(val)

    return out


def _clean_xml_params(values: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for raw_k, raw_v in values:
        k = _clean_text(raw_k)
        v = _clean_text(raw_v)
        if not k:
            continue
        key = (_ci(k), _ci(v))
        if key in seen:
            continue
        seen.add(key)
        out.append((k, v))
    return out


def _pick_raw_oid(src: SourceOffer) -> str:
    """
    Для AkCent id источника может быть:
    - article
    - offer/@id
    - Offer_ID
    """
    for candidate in (src.article, src.oid, src.offer_id):
        s = _clean_text(candidate)
        if s:
            return s
    return ""


def build_offer_oid(raw_id: str, *, prefix: str) -> str:
    rid = _clean_text(raw_id)
    if not rid:
        return ""
    if prefix and rid.upper().startswith(prefix.upper()):
        return rid
    return f"{prefix}{rid}" if prefix else rid


def _param_value(xml_params: list[tuple[str, str]], names: list[str]) -> str:
    wanted = {_ci(x) for x in names}
    for k, v in xml_params:
        if _ci(k) in wanted and _clean_text(v):
            return _clean_text(v)
    return ""


def _title_brand(s: str) -> str:
    s = _clean_text(s)
    if not s:
        return ""

    key = s.casefold()
    if key in _KNOWN_BRANDS:
        return _KNOWN_BRANDS[key]

    # Нормализация частых форм
    fixed = {
        "hp": "HP",
        "hpe": "HPE",
        "apc": "APC",
        "lg": "LG",
        "msi": "MSI",
        "wd": "WD",
        "qnap": "QNAP",
        "olmio": "OLMIO",
        "smart": "SMART",
        "tp-link": "TP-Link",
        "viewsonic": "ViewSonic",
        "aoc": "AOC",
    }
    if key in fixed:
        return fixed[key]

    if " " in s:
        parts = []
        for part in s.split():
            if not part:
                continue
            if part.isupper():
                parts.append(part)
            else:
                parts.append(part[:1].upper() + part[1:])
        return " ".join(parts)

    if len(s) <= 4 and re.fullmatch(r"[A-Za-z0-9\-]+", s):
        return s.upper()

    return s[:1].upper() + s[1:]


def _is_bad_vendor(vendor: str, vendor_blacklist: set[str]) -> bool:
    v = _ci(vendor)
    if not v:
        return True
    if v in _BAD_VENDOR_VALUES:
        return True
    if v in vendor_blacklist:
        return True
    if len(v) <= 1:
        return True
    if re.fullmatch(r"\d+", v):
        return True
    return False


def _tokens(text: str) -> list[str]:
    raw = _BRAND_SPLIT_RE.split(_strip_html(text))
    return [x for x in raw if x]


def _build_brand_lexicon(source_offers: list[SourceOffer], vendor_blacklist: set[str]) -> dict[str, str]:
    """
    Собираем supplier-side лексикон брендов из:
    - vendor
    - Param Производитель / Бренд
    - известного словаря брендов
    """
    found: dict[str, str] = {}

    # 1) supplier native vendor / params
    for src in source_offers:
        candidates = [
            _clean_text(src.vendor),
            _param_value(src.xml_params, ["Производитель", "Бренд", "Vendor", "Brand"]),
        ]
        for c in candidates:
            if _is_bad_vendor(c, vendor_blacklist):
                continue
            canon = _title_brand(c)
            if canon:
                found[canon.casefold()] = canon

    # 2) страховка известными брендами
    for _, canon in _KNOWN_BRANDS.items():
        found[canon.casefold()] = canon

    return found


def _infer_vendor_from_brand_lexicon(text: str, brand_lexicon: dict[str, str]) -> str:
    raw = _strip_html(text)
    if not raw:
        return ""

    low = raw.casefold()

    # Сначала multi-word бренды
    multi = [x for x in brand_lexicon.values() if " " in x or "-" in x]
    multi.sort(key=len, reverse=True)

    for brand in multi:
        b = brand.casefold()
        if b and b in low:
            return brand

    # Потом токены
    for tok in _tokens(raw):
        t = tok.casefold()
        if t in brand_lexicon:
            return brand_lexicon[t]

    return ""


def normalize_vendor(
    src: SourceOffer,
    *,
    vendor_blacklist: set[str],
    brand_lexicon: dict[str, str],
) -> str:
    # 1) прямой vendor
    vendor = _clean_text(src.vendor)
    if not _is_bad_vendor(vendor, vendor_blacklist):
        return _title_brand(vendor)

    # 2) params бренда / производителя
    param_vendor = _param_value(
        src.xml_params,
        ["Производитель", "Бренд", "Vendor", "Brand", "Торговая марка"],
    )
    if not _is_bad_vendor(param_vendor, vendor_blacklist):
        return _title_brand(param_vendor)

    # 3) name
    inferred = _infer_vendor_from_brand_lexicon(src.name or "", brand_lexicon)
    if inferred:
        return inferred

    # 4) model
    inferred = _infer_vendor_from_brand_lexicon(src.model or "", brand_lexicon)
    if inferred:
        return inferred

    # 5) description
    inferred = _infer_vendor_from_brand_lexicon(src.description or "", brand_lexicon)
    if inferred:
        return inferred

    return ""


def normalize_available(available_attr: str | bool, stock_text: str) -> bool:
    av_attr = str(available_attr or "").strip().lower()
    if av_attr in {"true", "1", "yes"}:
        return True
    if av_attr in {"false", "0", "no"}:
        return False

    stock = _clean_text(stock_text).casefold()
    if not stock:
        return False

    if "нет" in stock or "отсутств" in stock:
        return False
    if "под заказ" in stock or "ожида" in stock:
        return False
    if ">" in stock or "<" in stock:
        return True
    if "в наличии" in stock or "склад" in stock or "доступ" in stock:
        return True

    m = _NUM_RE.search(stock.replace(" ", ""))
    if not m:
        return False

    try:
        return float(m.group(0).replace(",", ".")) > 0
    except Exception:
        return False


def _parse_price_num(text: str) -> int | None:
    raw = _clean_text(text).replace(" ", "")
    if not raw:
        return None
    m = _NUM_RE.search(raw)
    if not m:
        return None
    try:
        val = float(m.group(0).replace(",", "."))
    except Exception:
        return None
    if val <= 0:
        return None
    return int(round(val))


def _price_priority(price_type: str) -> int:
    t = _ci(price_type)

    if "цена дилерского портала" in t and "kzt" in t:
        return 0
    if t == "rrp" or "rrp" in t:
        return 1
    if "рознич" in t:
        return 2
    if "price" in t or "цена" in t:
        return 3
    return 9


def normalize_price_in(prices: list[dict[str, str]]) -> int | None:
    candidates: list[tuple[int, int]] = []

    for row in prices:
        value = _parse_price_num(str(row.get("value") or ""))
        if value is None:
            continue
        ptype = str(row.get("type") or "")
        candidates.append((_price_priority(ptype), value))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], x[1]))
    return candidates[0][1]


def normalize_offer(
    src: SourceOffer,
    *,
    id_prefix: str,
    vendor_blacklist: set[str],
    brand_lexicon: dict[str, str],
) -> NormalizedOffer:
    raw_oid = _pick_raw_oid(src)
    oid = build_offer_oid(raw_oid, prefix=id_prefix)

    xml_params = _clean_xml_params(src.xml_params)
    vendor = normalize_vendor(
        src,
        vendor_blacklist=vendor_blacklist,
        brand_lexicon=brand_lexicon,
    )

    model = _normalize_model(src.model)
    if not model:
        model = _param_value(xml_params, ["Модель", "Model"])

    return NormalizedOffer(
        raw_oid=raw_oid,
        oid=oid,
        article=_clean_text(src.article),
        offer_id=_clean_text(src.offer_id),
        source_type=_clean_text(src.type_name),
        name=_normalize_name(src.name),
        vendor=vendor,
        model=model,
        url=_clean_text(src.url),
        category_id=_clean_text(src.category_id),
        available=normalize_available(src.available, src.stock_text),
        price_in=normalize_price_in(src.prices),
        description=(src.description or "").strip(),
        manufacturer_warranty=_clean_text(src.manufacturer_warranty),
        stock_text=_clean_text(src.stock_text),
        pictures=_dedupe_keep_order(src.pictures),
        xml_params=xml_params,
        prices=list(src.prices or []),
        raw_vendor=_clean_text(src.vendor),
        raw_name=_clean_text(src.name),
        raw_model=_clean_text(src.model),
    )


def normalize_offers(
    source_offers: list[SourceOffer],
    *,
    id_prefix: str = "AC",
    vendor_blacklist: set[str] | None = None,
) -> tuple[list[NormalizedOffer], dict[str, Any]]:
    vendor_blacklist = {str(x).strip().casefold() for x in (vendor_blacklist or set()) if str(x).strip()}
    brand_lexicon = _build_brand_lexicon(source_offers, vendor_blacklist)

    out: list[NormalizedOffer] = []
    vendor_fallback_used = 0
    empty_vendor_count = 0
    zero_price_count = 0
    unavailable_count = 0
    empty_oid_count = 0

    for src in source_offers:
        norm = normalize_offer(
            src,
            id_prefix=id_prefix,
            vendor_blacklist=vendor_blacklist,
            brand_lexicon=brand_lexicon,
        )
        out.append(norm)

        if not norm.oid:
            empty_oid_count += 1
        if not _clean_text(src.vendor) and norm.vendor:
            vendor_fallback_used += 1
        if not norm.vendor:
            empty_vendor_count += 1
        if not norm.available:
            unavailable_count += 1
        if norm.price_in is None or norm.price_in <= 0:
            zero_price_count += 1

    report: dict[str, Any] = {
        "normalized": len(out),
        "vendor_fallback_used": vendor_fallback_used,
        "empty_vendor_count": empty_vendor_count,
        "zero_price_count": zero_price_count,
        "unavailable_count": unavailable_count,
        "empty_oid_count": empty_oid_count,
        "brand_lexicon_size": len(brand_lexicon),
    }
    return out, report
