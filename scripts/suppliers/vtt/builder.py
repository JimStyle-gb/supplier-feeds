# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/builder.py
VTT supplier layer — сборка одного OfferOut.

Задача файла:
- взять один сырой VTT item из source.py;
- почистить name / vendor / pictures / native_desc;
- нормализовать params через params.py;
- аккуратно добавить совместимость;
- вернуть готовый OfferOut для core.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Sequence

from cs.core import OfferOut, normalize_pictures
from suppliers.vtt.params import extract_main_partnumber, get_param_value, normalize_vtt_params, norm_ws


_BRAND_CANON_MAP = {
    "hp": "HP",
    "hewlett packard": "HP",
    "canon": "Canon",
    "xerox": "Xerox",
    "epson": "Epson",
    "brother": "Brother",
    "kyocera": "Kyocera",
    "ricoh": "Ricoh",
    "samsung": "Samsung",
    "oki": "OKI",
    "lexmark": "Lexmark",
    "pantum": "Pantum",
    "sharp": "Sharp",
    "toshiba": "Toshiba",
    "konica minolta": "Konica Minolta",
    "panasonic": "Panasonic",
    "minolta": "Konica Minolta",
}

_RE_MULTI_SP = re.compile(r"\s+")
_RE_TAGS = re.compile(r"<[^>]+>")
_RE_CODEISH = re.compile(r"\b[0-9A-ZА-ЯЁ][0-9A-ZА-ЯЁ\-]{3,}\b")
_RE_RESOURCE = re.compile(r"\b\d+(?:[\.,]\d+)?\s*[KК]\b|\b\d+(?:[\.,]\d+)?\s*(?:стр|стр\.|pages?)\b", flags=re.IGNORECASE)
_RE_ML = re.compile(r"\b\d+(?:[\.,]\d+)?\s*мл\b", flags=re.IGNORECASE)
_RE_BRACKETS_O = re.compile(r"\(\s*O\s*\)", flags=re.IGNORECASE)
_RE_DUP_PUNCT = re.compile(r"\s*([,;])\s*")
_RE_FOR_COMPAT = re.compile(r"\bдля\s+(.+)$", flags=re.IGNORECASE)
_RE_BRAND_POS = re.compile(
    r"\b(HP|Canon|Xerox|Epson|Brother|Kyocera|Ricoh|Samsung|OKI|Lexmark|Pantum|Sharp|Toshiba|Konica\s+Minolta|Panasonic)\b",
    flags=re.IGNORECASE,
)


def build_offer_from_raw(raw: dict[str, Any]) -> OfferOut | None:
    """Собирает один OfferOut из сырого VTT item."""
    if not _is_valid_for_output(raw):
        return None

    raw_name = norm_ws(str((raw or {}).get("name", "") or ""))
    raw_desc = str((raw or {}).get("description", "") or "")
    raw_vendor = norm_ws(str((raw or {}).get("vendor", "") or ""))
    raw_price = (raw or {}).get("price")
    raw_id = norm_ws(str((raw or {}).get("id", "") or ""))
    raw_pictures = list((raw or {}).get("pictures") or [])
    raw_params = list((raw or {}).get("params") or [])
    available = bool((raw or {}).get("available", True))

    name = _normalize_name(raw_name)
    if not name:
        return None

    params = normalize_vtt_params(raw_params, name=name)
    partnumber = get_param_value(params, "Партномер") or extract_main_partnumber(raw_params, name=name)

    pictures = _normalize_pictures(raw_pictures)
    native_desc = _build_native_desc(raw_desc, name)
    params = _inject_compatibility(params, name=name, native_desc=native_desc, vendor=raw_vendor)
    vendor = _normalize_vendor(raw_vendor, name=name, params=params)
    oid = _build_oid(raw_id=raw_id, partnumber=partnumber, name=name)
    price = _safe_price(raw_price)

    return OfferOut(
        oid=oid,
        available=available,
        name=name,
        price=price,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=native_desc,
    )


def _is_valid_for_output(raw: dict[str, Any]) -> bool:
    """Мягкая валидация сырого товара."""
    if not isinstance(raw, dict):
        return False
    if not norm_ws(str(raw.get("name", "") or "")):
        return False
    return True


def _build_oid(*, raw_id: str, partnumber: str, name: str) -> str:
    """Строит стабильный oid с префиксом VT."""
    rid = _sanitize_oid_token(raw_id)
    if rid:
        return rid if rid.startswith("VT") else f"VT{rid}"

    pn = _sanitize_oid_token(partnumber)
    if pn:
        return pn if pn.startswith("VT") else f"VT{pn}"

    digest = hashlib.md5(norm_ws(name).encode("utf-8", errors="ignore")).hexdigest()[:12].upper()
    return f"VT{digest}"


def _sanitize_oid_token(text: str) -> str:
    """Оставляет безопасный токен для oid."""
    s = norm_ws(text).upper()
    if not s:
        return ""
    s = re.sub(r"[^0-9A-ZА-ЯЁ_-]+", "", s)
    return s[:64]


def _normalize_name(name: str) -> str:
    """Бережно чистит имя товара без агрессивной эвристики."""
    s = norm_ws(name)
    if not s:
        return ""
    s = _RE_BRACKETS_O.sub("(O)", s)
    s = s.replace(" ,", ",")
    s = s.replace(" ;", ";")
    s = s.replace(" / ", "/")
    s = s.replace("( ", "(").replace(" )", ")")
    s = _RE_DUP_PUNCT.sub(r"\1 ", s)
    s = _RE_MULTI_SP.sub(" ", s).strip(" ,.;")
    return s


def _normalize_vendor(vendor: str, *, name: str, params: Sequence[tuple[str, str]]) -> str:
    """Готовит vendor_src для core без названия поставщика."""
    v = norm_ws(vendor)
    if v and v.casefold() != "vtt":
        canon = _canon_brand(v)
        if canon:
            return canon
        return v

    joined = " ".join([name] + [f"{k} {val}" for k, val in (params or [])])
    found = _infer_brand(joined)
    return found or ""


def _canon_brand(text: str) -> str:
    """Канонизирует известные бренды."""
    s = norm_ws(text).casefold().replace("ё", "е")
    if not s:
        return ""
    for raw, canon in sorted(_BRAND_CANON_MAP.items(), key=lambda x: len(x[0]), reverse=True):
        if s == raw:
            return canon
    return ""


def _infer_brand(text: str) -> str:
    """Ищет очевидный бренд в имени/params."""
    hay = norm_ws(text)
    if not hay:
        return ""
    for raw, canon in sorted(_BRAND_CANON_MAP.items(), key=lambda x: len(x[0]), reverse=True):
        if re.search(rf"\b{re.escape(raw)}\b", hay, flags=re.IGNORECASE):
            return canon
    return ""


def _normalize_pictures(pictures: list[str]) -> list[str]:
    """Чистит и дедуплицирует pictures."""
    cleaned: list[str] = []
    for p in pictures or []:
        u = norm_ws(str(p or ""))
        if not u:
            continue
        cleaned.append(u)
    return normalize_pictures(cleaned)


def _build_native_desc(raw_desc: str, name: str) -> str:
    """Готовит минимально полезное native_desc."""
    d = raw_desc or ""
    d = d.replace("\xa0", " ")
    d = _RE_TAGS.sub(" ", d)
    d = _RE_MULTI_SP.sub(" ", d).strip()
    if not d:
        return ""

    name_cmp = _cmp_text(name)
    desc_cmp = _cmp_text(d)
    if not desc_cmp or desc_cmp == name_cmp:
        return ""
    if desc_cmp.startswith(name_cmp) and len(desc_cmp) <= len(name_cmp) + 24:
        return ""
    return d


def _cmp_text(text: str) -> str:
    """Упрощённое сравнение текстов без шума."""
    s = norm_ws(text).casefold().replace("ё", "е")
    s = _RE_TAGS.sub(" ", s)
    s = re.sub(r"[^0-9a-zа-я]+", " ", s, flags=re.IGNORECASE)
    return norm_ws(s)


def _inject_compatibility(
    params: Sequence[tuple[str, str]],
    *,
    name: str,
    native_desc: str,
    vendor: str = "",
) -> list[tuple[str, str]]:
    """Добавляет Совместимость только если она уверенно читается."""
    if get_param_value(params, "Совместимость"):
        return list(params or [])

    compat = _extract_compatibility(name=name, native_desc=native_desc, vendor=vendor)
    if not compat:
        return list(params or [])

    out = list(params or [])
    out.append(("Совместимость", compat))
    return out


def _extract_compatibility(*, name: str, native_desc: str, vendor: str = "") -> str:
    """Консервативно вытаскивает совместимость из name/desc."""
    for text in (name, native_desc):
        compat = _extract_compat_from_for_clause(text)
        if compat:
            return compat

    for text in (name, native_desc):
        compat = _extract_compat_from_brand_models(text, vendor=vendor)
        if compat:
            return compat

    return ""


def _extract_compat_from_for_clause(text: str) -> str:
    """Ищет хвост после 'для ...'."""
    s = norm_ws(text)
    if not s:
        return ""
    m = _RE_FOR_COMPAT.search(s)
    if not m:
        return ""
    tail = norm_ws(m.group(1))
    tail = _trim_compat_tail(tail)
    return _clean_compat_text(tail)


def _extract_compat_from_brand_models(text: str, *, vendor: str = "") -> str:
    """Ищет связку бренд + модели, если она явно есть в имени."""
    s = norm_ws(text)
    if not s:
        return ""

    m = _RE_BRAND_POS.search(s)
    if not m:
        return ""

    tail = s[m.start():]
    tail = _trim_compat_tail(tail)
    tail = _clean_compat_text(tail)
    if not tail:
        return ""

    brand = _canon_brand(vendor) or _canon_brand(m.group(1))
    if brand and not tail.lower().startswith(brand.lower()):
        tail = f"{brand} {tail}"
    return tail


def _trim_compat_tail(text: str) -> str:
    """Режет совместимость до первого явного технического хвоста."""
    s = norm_ws(text)
    if not s:
        return ""

    # Сначала обрезаем по запятой, чтобы не тащить цвет/объём.
    if "," in s:
        s = s.split(",", 1)[0].strip()

    # Убираем теххвосты по ресурсу, мл, партномеру, цвету.
    tokens = s.split()
    keep: list[str] = []
    for token in tokens:
        t = token.strip(" ,.;:()[]{}")
        if not t:
            continue
        if _looks_like_resource_token(t) or _looks_like_volume_token(t):
            break
        if _looks_like_color_token(t):
            break
        if _looks_like_code_token(t):
            break
        if t.casefold() in {"o", "(o)", "без", "чипа", "восстановленный"}:
            break
        keep.append(token)
    return norm_ws(" ".join(keep))


def _clean_compat_text(text: str) -> str:
    """Финально чистит строку совместимости."""
    s = norm_ws(text)
    if not s:
        return ""
    s = _RE_BRACKETS_O.sub("", s)
    s = s.strip(" ,.;:-")
    s = re.sub(r"\s*/\s*", "/", s)
    s = _RE_MULTI_SP.sub(" ", s).strip()
    if len(s) < 4:
        return ""
    return s


def _looks_like_code_token(token: str) -> bool:
    """Похоже ли на партномер/код."""
    t = norm_ws(token).upper()
    if len(t) < 4:
        return False
    if re.fullmatch(r"[A-ZА-ЯЁ0-9\-]{4,}", t) and re.search(r"\d", t) and re.search(r"[A-ZА-ЯЁ]", t):
        return True
    return False


def _looks_like_resource_token(token: str) -> bool:
    """Похоже ли на ресурс печати."""
    return bool(_RE_RESOURCE.search(token))


def _looks_like_volume_token(token: str) -> bool:
    """Похоже ли на объём в мл."""
    return bool(_RE_ML.search(token))


def _looks_like_color_token(token: str) -> bool:
    """Похоже ли на цветовой хвост."""
    t = norm_ws(token).casefold().replace("ё", "е")
    return t in {
        "black",
        "bk",
        "cyan",
        "magenta",
        "yellow",
        "grey",
        "gray",
        "photoblack",
        "mattblack",
        "matteblack",
        "color",
        "colour",
        "черный",
        "черный",
        "синий",
        "голубой",
        "пурпурный",
        "желтый",
        "серый",
        "цветной",
    }


def _safe_price(raw_price: Any) -> int:
    """Бережно приводит цену к int, при проблеме даёт фолбэк 100."""
    if isinstance(raw_price, (int, float)):
        val = int(raw_price)
        return val if val > 0 else 100

    s = norm_ws(str(raw_price or ""))
    if not s:
        return 100

    s = s.replace(" ", "")
    s = s.replace("\u00a0", "")
    s = s.replace(",", ".")

    m = re.search(r"\d+(?:\.\d+)?", s)
    if not m:
        return 100
    try:
        val = int(float(m.group(0)))
    except Exception:
        return 100
    return val if val > 0 else 100
