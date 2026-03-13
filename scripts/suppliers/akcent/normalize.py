# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/normalize.py

AkCent supplier-layer — базовая нормализация полей.

Что делает:
- нормализует name/model/vendor;
- строит стабильный oid с префиксом AC;
- выбирает входную цену из dealer -> price -> rrp;
- приводит available к bool;
- нормализует warranty;
- умеет мягко восстанавливать vendor и article/code для узкого потока AkCent.

Важно:
- здесь нет тяжёлой supplier-specific логики по params/compat/desc;
- только базовые поля до builder.py;
- заточено под текущий узкий ассортимент AkCent.
"""

from __future__ import annotations

import re
from typing import Iterable

from cs.util import norm_ws, safe_int


# ----------------------------- regex / const -----------------------------

_RE_WS = re.compile(r"\s+")
_RE_PARENS_SP = re.compile(r"\(\s+|\s+\)")
_RE_STOCK_NUM = re.compile(r"(-?\d+)")
_RE_WARRANTY_NUM = re.compile(r"(\d{1,3})")
_RE_CODE_TOKEN = re.compile(
    r"\b(?:C13T\d{5,6}[A-Z]?|C12C\d{6,9}|C11[A-Z0-9]{6,10}|V1[123]H[A-Z0-9]{5,10}|W\d{4}[A-Z]|[A-Z]{1,2}\d{3,4}[A-Z]|LS\d{3,5}[A-Z]{1,3}|SBID-[A-Z0-9\-]+|S\d{2,3}ABK\d{2})\b",
    re.IGNORECASE,
)
_RE_MONTHS = re.compile(r"(мес|месяц|месяцев)", re.IGNORECASE)
_RE_YEARS = re.compile(r"(год|года|лет|yr|year)", re.IGNORECASE)
_RE_NO_WARRANTY = re.compile(r"^(нет|no|none|0|0\s*мес)\b", re.IGNORECASE)
_RE_VENDOR_SUFFIX = re.compile(r"\b(?:proj|projector|display|mon|monitor)\b", re.IGNORECASE)

_ALLOWED_VENDOR_MAP = {
    "epson": "Epson",
    "viewsonic": "ViewSonic",
    "mr.pixel": "Mr.Pixel",
    "mr pixel": "Mr.Pixel",
    "smart": "SMART",
    "philips": "Philips",
    "fellowes": "Fellowes",
    "zebra": "Zebra",
    "idprt": "iDPRT",
    "iDPRT": "iDPRT",
}

_NAME_PREFIXES = (
    "C13T55",
    "Ёмкость для отработанных чернил",
    "Интерактивная доска",
    "Интерактивная панель",
    "Интерактивный дисплей",
    "Картридж",
    "Ламинатор",
    "Монитор",
    "МФУ",
    "Переплетчик",
    "Пленка для ламинирования",
    "Плоттер",
    "Принтер",
    "Проектор",
    "Сканер",
    "Чернила",
    "Шредер",
    "Экономичный набор",
    "Экран",
)


# ----------------------------- small helpers -----------------------------


def _clean_spaces(s: str) -> str:
    s = norm_ws(s)
    s = s.replace("\t", " ")
    s = _RE_WS.sub(" ", s).strip()
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    return s.strip(" -–—")


def _canon_vendor(v: str) -> str:
    s = _clean_spaces(v)
    if not s:
        return ""
    s = _RE_VENDOR_SUFFIX.sub("", s)
    s = _clean_spaces(s)
    key = s.casefold()
    return _ALLOWED_VENDOR_MAP.get(key, s)


def _extract_code_token(*parts: str) -> str:
    for part in parts:
        s = _clean_spaces(part)
        if not s:
            continue
        m = _RE_CODE_TOKEN.search(s)
        if m:
            return m.group(0).upper()
    return ""


def _extract_vendor_from_text(*parts: str) -> str:
    text = " ".join(_clean_spaces(x) for x in parts if _clean_spaces(x))
    if not text:
        return ""
    low = text.casefold()
    # длинные ключи сначала
    for raw, canon in sorted(_ALLOWED_VENDOR_MAP.items(), key=lambda kv: -len(kv[0])):
        if raw.casefold() in low:
            return canon
    return ""


def _strip_prefix_from_name(name: str) -> str:
    s = _clean_spaces(name)
    for prefix in _NAME_PREFIXES:
        if s.startswith(prefix):
            tail = _clean_spaces(s[len(prefix):])
            return tail or s
    return s


# ----------------------------- public API -----------------------------


def normalize_name(name: str) -> str:
    """Чистит имя без смысловых перестроек."""
    return _clean_spaces(name)


def _short_model_from_text(*parts: str) -> str:
    """Пытается достать короткую модель/код из текста."""
    code = _extract_code_token(*parts)
    if code:
        return code.upper()
    return ""


def normalize_model(model: str, *, name: str = "", description_text: str = "") -> str:
    """
    Нормализует модель.

    Для AkCent consumable-кейсов старается не тащить полное имя товара,
    а выделять короткий код модели, если он явно читается.
    """
    s = _clean_spaces(model)
    if s:
        short = _short_model_from_text(s, name, description_text)
        if short and len(s) > len(short) + 6:
            return short
        return s

    short = _short_model_from_text(name, description_text)
    if short:
        return short
    return normalize_name(name)


def normalize_vendor(
    vendor: str,
    *,
    name: str = "",
    model: str = "",
    description_text: str = "",
    vendor_blacklist: set[str] | None = None,
) -> str:
    """
    Нормализует vendor.

    Порядок:
    1) supplier vendor;
    2) fallback из name/model по известным брендам текущего узкого потока.
    """
    vendor_blacklist = vendor_blacklist or set()

    s = _canon_vendor(vendor)
    if s and s.casefold() not in vendor_blacklist:
        return s

    guessed = _extract_vendor_from_text(name, model, description_text)
    if guessed and guessed.casefold() not in vendor_blacklist:
        return guessed

    return ""


def normalize_price_in(
    dealer_text: str = "",
    price_text: str = "",
    rrp_text: str = "",
) -> int | None:
    """Берёт входную цену из dealer -> price -> rrp."""
    for value in (dealer_text, price_text, rrp_text):
        num = safe_int(value)
        if num is not None and num > 0:
            return num
    return None


def normalize_available(
    available_attr: str = "",
    stock_text: str = "",
    available_tag: str = "",
) -> bool:
    """
    Приводит наличие к bool.

    Приоритет:
    1) @available;
    2) текстовый available tag;
    3) Stock (>0, <5, <25, <100 и т.п.).
    """
    for raw in (available_attr, available_tag):
        s = _clean_spaces(raw).casefold()
        if s in {"true", "1", "yes", "y", "да"}:
            return True
        if s in {"false", "0", "no", "n", "нет"}:
            return False

    stock = _clean_spaces(stock_text)
    if not stock:
        return False

    # Строки вида <5 / <25 / >100 / 0
    m = _RE_STOCK_NUM.search(stock.replace(" ", ""))
    if not m:
        return False
    try:
        num = int(m.group(1))
    except Exception:
        return False
    return num > 0


def normalize_warranty(*values: str) -> str:
    """
    Приводит гарантию к виду 'N мес.' или ''.

    Примеры:
    - '12 месяцев' -> '12 мес.'
    - '2 года' -> '24 мес.'
    - '3' -> '3 мес.'
    - 'нет' -> ''
    """
    for raw in values:
        s = _clean_spaces(raw)
        if not s:
            continue
        if _RE_NO_WARRANTY.search(s):
            return ""

        m = _RE_WARRANTY_NUM.search(s)
        if not m:
            continue
        num = int(m.group(1))
        if num <= 0:
            return ""

        if _RE_YEARS.search(s):
            num *= 12
            return f"{num} мес."
        if _RE_MONTHS.search(s):
            return f"{num} мес."

        # AkCent часто отдаёт просто число; считаем, что это месяцы.
        return f"{num} мес."
    return ""


def build_offer_oid(
    raw_id: str = "",
    *,
    article: str = "",
    offer_id: str = "",
    name: str = "",
    model: str = "",
    prefix: str = "AC",
) -> str:
    """
    Собирает стабильный oid для AkCent.

    Приоритет источников:
    1) article
    2) code token из name/model
    3) offer_id
    4) raw_id
    """
    base = _clean_spaces(article)
    if not base:
        base = _extract_code_token(name, model)
    if not base:
        base = _clean_spaces(offer_id)
    if not base:
        base = _clean_spaces(raw_id)

    if not base:
        return ""

    base = re.sub(r"\s+", "", base)
    if base.upper().startswith(prefix.upper()):
        return base
    return f"{prefix}{base}"


def normalize_article(article: str = "", *, name: str = "", model: str = "") -> str:
    """Нормализует article; если article пуст — пытается достать код из name/model."""
    s = _clean_spaces(article)
    if s:
        return s
    return _extract_code_token(name, model)


def normalize_stock_text(stock_text: str) -> str:
    """Оставляет stock в компактном виде для диагностики."""
    return _clean_spaces(stock_text)


def normalize_source_basics(
    *,
    raw_id: str = "",
    offer_id: str = "",
    article: str = "",
    name: str = "",
    model: str = "",
    vendor: str = "",
    description_text: str = "",
    dealer_text: str = "",
    price_text: str = "",
    rrp_text: str = "",
    available_attr: str = "",
    available_tag: str = "",
    stock_text: str = "",
    warranty_values: Iterable[str] | None = None,
    vendor_blacklist: set[str] | None = None,
    id_prefix: str = "AC",
) -> dict[str, object]:
    """Удобный агрегатор для builder.py."""
    n_name = normalize_name(name)
    n_model = normalize_model(model, name=n_name, description_text=description_text)
    n_vendor = normalize_vendor(vendor, name=n_name, model=n_model, description_text=description_text, vendor_blacklist=vendor_blacklist)
    n_article = normalize_article(article, name=n_name, model=n_model)
    n_oid = build_offer_oid(
        raw_id,
        article=n_article,
        offer_id=offer_id,
        name=n_name,
        model=n_model,
        prefix=id_prefix,
    )
    n_available = normalize_available(available_attr, stock_text, available_tag)
    n_price_in = normalize_price_in(dealer_text, price_text, rrp_text)
    warranty_values = list(warranty_values or [])
    n_warranty = normalize_warranty(*warranty_values)

    return {
        "oid": n_oid,
        "article": n_article,
        "name": n_name,
        "model": n_model,
        "vendor": n_vendor,
        "available": n_available,
        "price_in": n_price_in,
        "warranty": n_warranty,
        "stock_text": normalize_stock_text(stock_text),
    }
