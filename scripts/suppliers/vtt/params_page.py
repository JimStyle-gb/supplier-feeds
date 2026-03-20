# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/params_page.py

VTT page-params layer.

Задача:
- принять сырые params из карточки VTT;
- убрать служебный мусор до RAW;
- привести ключи по aliases/schema;
- собрать один Партномер;
- мягко поднять Коды расходников / Тип / Цвет / Ресурс;
- не заниматься compat-reconcile (это отдельный compat.py).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Sequence

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore

from .normalize import norm_spaces, safe_str


DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parent / "config" / "schema.yml"

# Широкий, но безопасный паттерн под коды расходки.
_CODE_RX = re.compile(
    r"\b(?:"
    r"[A-Z]{1,4}-?[A-Z0-9]{2,}(?:/[A-Z0-9-]{2,})+|"  # HB-Q5949A/Q7553A
    r"[A-Z]{1,4}\d{2,}[A-Z0-9-]{0,6}|"               # Q7553A / CE285A / TK-1140 / B3P19A
    r"(?:006R|106R)\d{5}|"                           # Xerox
    r"\d{3,4}[A-Z]{1,3}|"                            # 725A / 728
    r"W\d{4}[A-Z0-9]{0,4}"                           # W1106A / W1335A
    r")\b",
    re.I,
)

_COLOR_MAP = {
    "black": "Черный",
    "bk": "Черный",
    "cyan": "Голубой",
    "magenta": "Пурпурный",
    "yellow": "Желтый",
    "grey": "Серый",
    "gray": "Серый",
    "blue": "Синий",
    "red": "Красный",
    "green": "Зеленый",
    "mattblack": "Матовый черный",
    "matteblack": "Матовый черный",
    "photoblack": "Фото-черный",
    "photo black": "Фото-черный",
    "color": "Цветной",
    "colour": "Цветной",
}


def _cf(text: str) -> str:
    return norm_spaces(text).casefold().replace("ё", "е")


def _dedupe_pairs(params: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for k, v in params:
        kk = norm_spaces(k)
        vv = norm_spaces(v)
        if not kk or not vv:
            continue
        sig = (_cf(kk), _cf(vv))
        if sig in seen:
            continue
        seen.add(sig)
        out.append((kk, vv))
    return out


def load_schema_config(path: str | Path | None = None) -> dict:
    """Читает schema.yml; если файла нет — работает на встроенных дефолтах."""
    p = Path(path) if path else DEFAULT_SCHEMA_PATH
    if yaml is None or not p.exists():
        return {
            "banned_keys": [],
            "discard_keys": [
                "Аналоги",
                "Аналог",
                "Штрихкод",
                "Штрих-код",
                "Штрих код",
                "EAN",
                "Barcode",
                "Артикул",
                "Партс-номер",
                "Вендор",
                "Цена",
                "Стоимость",
                "Категория",
                "Подкатегория",
            ],
            "aliases": {
                "OEM-номер": "Партномер",
                "OEM номер": "Партномер",
                "OEM": "Партномер",
                "OEM номер детали": "Партномер",
                "OEM номер/Part Number": "Партномер",
                "Каталожный номер": "Партномер",
                "Кат. номер": "Партномер",
                "Каталожный №": "Партномер",
                "Кат. №": "Партномер",
                "Part Number": "Партномер",
                "PartNumber": "Партномер",
                "PN": "Партномер",
                "Part No": "Партномер",
            },
            "allow_keys": {
                "consumable": ["Тип", "Для бренда", "Партномер", "Коды расходников", "Совместимость", "Технология печати", "Цвет", "Ресурс", "Модель"],
                "drum": ["Тип", "Для бренда", "Партномер", "Коды расходников", "Совместимость", "Цвет", "Ресурс", "Модель"],
                "developer": ["Тип", "Для бренда", "Партномер", "Коды расходников", "Совместимость", "Цвет", "Ресурс", "Модель"],
                "ink": ["Тип", "Для бренда", "Партномер", "Коды расходников", "Совместимость", "Цвет", "Объем", "Ресурс", "Модель"],
                "spare_part": ["Тип", "Для бренда", "Партномер", "Совместимость", "Модель"],
            },
        }
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        return {}
    return data


def _canon_key(key: str, aliases: dict[str, str]) -> str:
    raw = norm_spaces(key).strip(":")
    if not raw:
        return ""
    raw_cf = _cf(raw)
    for src, dst in aliases.items():
        if raw_cf == _cf(src):
            return norm_spaces(dst)
    return raw


def _classify_product(title: str, category_code: str) -> str:
    t = _cf(title)
    c = _cf(category_code)
    if any(x in c for x in ("therblc", "therelt", "partsprint")):
        return "spare_part"
    if "dev" in c or "девелоп" in t:
        return "developer"
    if "drm" in c or "фотобарабан" in t or "драм" in t:
        return "drum"
    if "ink" in t or "чернил" in t:
        return "ink"
    return "consumable"


def _normalize_value(value: str) -> str:
    s = norm_spaces(value)
    s = s.replace(" ,", ",").replace(" .", ".")
    s = re.sub(r"\s*;\s*", "; ", s)
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip(" ,;/")


def _drop_service_params(params: Iterable[tuple[str, str]], discard_keys: Sequence[str], banned_keys: Sequence[str]) -> list[tuple[str, str]]:
    drop_cf = {_cf(x) for x in discard_keys}
    ban_cf = {_cf(x) for x in banned_keys}
    out: list[tuple[str, str]] = []
    for k, v in params:
        kk = norm_spaces(k)
        vv = _normalize_value(v)
        if not kk or not vv:
            continue
        kcf = _cf(kk)
        if kcf in drop_cf or kcf in ban_cf:
            continue
        out.append((kk, vv))
    return out


def _extract_partnumber_candidates(params: Sequence[tuple[str, str]], title: str = "") -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    # Сначала явные ключи source.
    for k, v in params:
        kcf = _cf(k)
        if kcf in {
            "партномер",
            "partnumber",
            "part number",
            "pn",
            "part no",
            "oem-номер",
            "oem номер",
            "oem",
            "oem номер детали",
            "oem номер/part number",
            "каталожный номер",
            "кат. номер",
            "каталожный №".casefold().replace("ё", "е"),
            "кат. №".casefold().replace("ё", "е"),
        }:
            val = _normalize_value(v)
            if val and _cf(val) not in seen:
                seen.add(_cf(val))
                out.append(val)

    # Фолбэк из title: только явные длинные кодовые токены.
    for m in _CODE_RX.finditer(title or ""):
        code = _normalize_value(m.group(0)).strip(" ,;/")
        if len(re.sub(r"[^A-Z0-9]", "", code.upper())) < 5:
            continue
        if _cf(code) in seen:
            continue
        seen.add(_cf(code))
        out.append(code)

    return out


def _pick_main_partnumber(candidates: Sequence[str]) -> str:
    """Берём первый вменяемый кандидат без device-list мусора."""
    for raw in candidates:
        val = _normalize_value(raw)
        if not val:
            continue
        if len(val) > 120:
            continue
        if len(re.findall(r"[A-Za-z0-9]", val)) < 3:
            continue
        return val
    return ""


def _strip_source_partnumber_keys(params: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    drop = {
        "партномер",
        "partnumber",
        "part number",
        "pn",
        "part no",
        "oem-номер",
        "oem номер",
        "oem",
        "oem номер детали",
        "oem номер/part number",
        "каталожный номер",
        "кат. номер",
        "каталожный №".casefold().replace("ё", "е"),
        "кат. №".casefold().replace("ё", "е"),
    }
    out: list[tuple[str, str]] = []
    for k, v in params:
        if _cf(k) in drop:
            continue
        out.append((k, v))
    return out


def _extract_codes_from_texts(*texts: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for text in texts:
        src = norm_spaces(text)
        if not src:
            continue
        for m in _CODE_RX.finditer(src):
            code = _normalize_value(m.group(0)).strip(" ,;/").upper()
            if code.isdigit() and len(code) < 4:
                continue
            sig = code.casefold()
            if sig in seen:
                continue
            seen.add(sig)
            out.append(code)
    return out


def _looks_like_device_list(value: str) -> bool:
    s = _cf(value)
    device_words = (
        "laserjet",
        "workcentre",
        "phaser",
        "ecosys",
        "imageclass",
        "imagerunner",
        "i-sensys",
        "mfp",
        "принтер",
        "мфу",
    )
    return any(w in s for w in device_words)


def _append_consumable_codes(params: list[tuple[str, str]], title: str, pn: str, native_desc: str = "") -> list[tuple[str, str]]:
    codes = _extract_codes_from_texts(title, pn, native_desc)
    if not codes:
        return params
    joined = ", ".join(codes[:12])
    if _looks_like_device_list(joined):
        return params
    return params + [("Коды расходников", joined)]


def _normalize_type(title: str, category_code: str) -> str:
    t = _cf(title)
    c = _cf(category_code)
    if any(x in c for x in ("therblc", "thermobl", "термоблок")) or "термоблок" in t:
        return "Термоблок"
    if any(x in c for x in ("therelt", "термолент")) or "термолента" in t:
        return "Термолента"
    if "фотобарабан" in t or "драм" in t or "drm" in c:
        return "Фотобарабан"
    if "девелоп" in t or c.endswith("dev"):
        return "Девелопер"
    if "чернил" in t or "ink" in t:
        return "Чернила"
    if "printhead" in t or "печатающ" in t or "prnthd" in c:
        return "Печатающая головка"
    if "картридж" in t:
        if "тонер" in t:
            return "Тонер-картридж"
        return "Картридж"
    return ""


def _normalize_color(title: str, params: Sequence[tuple[str, str]]) -> str:
    for k, v in params:
        if _cf(k) != "цвет":
            continue
        vv = _cf(v).replace(" ", "")
        if vv in _COLOR_MAP:
            return _COLOR_MAP[vv]
        return _normalize_value(v)

    t = _cf(title).replace(" ", "")
    for src, dst in _COLOR_MAP.items():
        key = src.replace(" ", "")
        if key and key in t:
            return dst
    return ""


def _normalize_resource(title: str, params: Sequence[tuple[str, str]]) -> str:
    for k, v in params:
        if _cf(k) == "ресурс":
            return _normalize_value(v)

    t = norm_spaces(title)
    m = re.search(r"\b(\d{1,3})\s*([КK])\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}К"

    m = re.search(r"\b(\d[\d\s]{2,6})\s*(?:стр|страниц[а-я]*)\b", t, flags=re.I)
    if m:
        return f"{re.sub(r'\s+', '', m.group(1))} стр"
    return ""


def _infer_brand(title: str, params: Sequence[tuple[str, str]]) -> str:
    for k, v in params:
        if _cf(k) in {"для бренда", "бренд", "производитель"}:
            return _normalize_value(v)

    t = norm_spaces(title)
    for vendor in (
        "HP",
        "Canon",
        "Xerox",
        "Kyocera",
        "Brother",
        "Epson",
        "Pantum",
        "Ricoh",
        "Samsung",
        "Lexmark",
        "OKI",
        "Panasonic",
        "Sharp",
        "Toshiba",
        "RISO",
        "Konica Minolta",
    ):
        if re.search(rf"\b{re.escape(vendor)}\b", t, flags=re.I):
            return vendor
    return ""


def _filter_allowed_keys(params: Sequence[tuple[str, str]], product_class: str, allow_map: dict) -> list[tuple[str, str]]:
    allowed = allow_map.get(product_class) or []
    if not allowed:
        return list(params)
    allowed_cf = {_cf(x) for x in allowed}
    out: list[tuple[str, str]] = []
    for k, v in params:
        if _cf(k) not in allowed_cf:
            continue
        out.append((k, v))
    return out


def normalize_vtt_page_params(
    raw_params: Sequence[tuple[str, str]],
    *,
    title: str = "",
    category_code: str = "",
    native_desc: str = "",
    schema_path: str | Path | None = None,
) -> list[tuple[str, str]]:
    """
    Нормализует page params VTT до чистого RAW.
    """
    schema = load_schema_config(schema_path)
    aliases = schema.get("aliases") or {}
    discard_keys = schema.get("discard_keys") or []
    banned_keys = schema.get("banned_keys") or []
    allow_map = schema.get("allow_keys") or {}

    # 1) базовая чистка + aliases
    pre: list[tuple[str, str]] = []
    for k, v in raw_params or []:
        kk = _canon_key(k, aliases)
        vv = _normalize_value(v)
        if not kk or not vv:
            continue
        pre.append((kk, vv))

    # 2) выкидываем service мусор
    pre = _drop_service_params(pre, discard_keys, banned_keys)

    # 3) собираем единый Партномер
    pn = _pick_main_partnumber(_extract_partnumber_candidates(pre, title=title))
    pre = _strip_source_partnumber_keys(pre)
    if pn:
        pre.append(("Партномер", pn))

    # 4) мягкие supplier-derived params
    product_class = _classify_product(title, category_code)

    typ = _normalize_type(title, category_code)
    if typ:
        pre.append(("Тип", typ))

    brand = _infer_brand(title, pre)
    if brand:
        pre.append(("Для бренда", brand))

    color = _normalize_color(title, pre)
    if color:
        pre.append(("Цвет", color))

    resource = _normalize_resource(title, pre)
    if resource:
        pre.append(("Ресурс", resource))

    pre = _append_consumable_codes(pre, title=title, pn=pn, native_desc=native_desc)

    # 5) allow_keys по классу товара
    pre = _filter_allowed_keys(pre, product_class, allow_map)

    # 6) финальная дедупликация
    return _dedupe_pairs(pre)
