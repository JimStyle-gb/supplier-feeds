# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/params_xml.py

XML params pipeline для ComPortal.
Это главный extractor supplier-параметров:
- cleanup родных XML <param>;
- aliases / value normalizers;
- kind-aware cleanup;
- синтетический param "Тип" из category.

Без core-логики и без narrative-builder.
"""

from __future__ import annotations

import re
from typing import Any

from cs.util import norm_ws
from suppliers.comportal.models import ParamItem, SourceOffer


_RE_HAS_LETTER = re.compile(r"[A-Za-zА-Яа-яЁё]")
_RE_LETTER_SLASH_LETTER = re.compile(r"([A-Za-zА-Яа-яЁё])\s*/\s*([A-Za-zА-Яа-яЁё])")

_TECH_VALUE_RE = re.compile(
    r"(?iu)\b("
    r"Лазерн(?:ая|ый)|"
    r"Струйн(?:ая|ый)|"
    r"Чернильн(?:ый|ая)|"
    r"Светодиодн(?:ая|ый)|"
    r"Матричн(?:ая|ый)|"
    r"Термосублимационн(?:ая|ый)"
    r")\b"
)
_RESOURCE_NUMBER_ONLY_RE = re.compile(r"(?iu)^\d[\d\s.,]*(?:\s*(?:стр\.?|страниц))?$")


def key_quality_ok(k: str, *, require_letter: bool, max_len: int, max_words: int) -> bool:
    kk = norm_ws(k)
    if not kk:
        return False
    if require_letter and not _RE_HAS_LETTER.search(kk):
        return False
    if max_len and len(kk) > int(max_len):
        return False
    if max_words and len(kk.split()) > int(max_words):
        return False
    return True


def normalize_warranty_to_months(v: str) -> str:
    vv = norm_ws(v)
    if not vv:
        return ""
    low = vv.casefold()
    if low in ("нет", "no", "-", "—"):
        return ""
    m = re.search(r"(\d{1,2})\s*(год|года|лет)\b", low)
    if m:
        return f"{int(m.group(1)) * 12} мес"
    if re.fullmatch(r"\d{1,3}", low):
        return f"{int(low)} мес"
    m = re.search(r"\b(\d{1,3})\b", low)
    if m and ("мес" in low or "month" in low):
        return f"{int(m.group(1))} мес"
    return vv


def normalize_key_aliases(key: str, schema: dict[str, Any]) -> str:
    kk = norm_ws(key)
    aliases = schema.get("aliases_casefold") or {}
    repl = aliases.get(kk.casefold())
    return norm_ws(repl) if repl else kk


def _post_clean_color_value(v: str) -> str:
    s = norm_ws(v).strip(" ;,.-")
    if not s:
        return ""
    mapping = {
        "черн": "Чёрный",
        "желт": "Жёлтый",
        "голуб": "Голубой",
        "пурпур": "Пурпурный",
        "сер": "Серый",
        "бел": "Белый",
        "син": "Синий",
        "красн": "Красный",
        "зел": "Зелёный",
    }
    low = s.casefold().replace("ё", "е")
    for pref, clean in mapping.items():
        if low.startswith(pref):
            return clean
    return s


def _post_clean_technology_value(v: str) -> str:
    s = norm_ws(v).strip(" ;,.-")
    if not s:
        return ""
    low = s.casefold().replace("ё", "е")
    if low in {"чернильный", "чернильная", "струйный", "струйная"}:
        return "Струйная"
    if low in {"лазерный", "лазерная"}:
        return "Лазерная"
    m = _TECH_VALUE_RE.search(s)
    if not m:
        return s
    found = norm_ws(m.group(1))
    low = found.casefold().replace("ё", "е")
    if low.startswith("лазерн"):
        return "Лазерная"
    if low.startswith("струйн") or low.startswith("чернильн"):
        return "Струйная"
    return found


def _post_clean_resource_value(v: str) -> str:
    s = norm_ws(v).strip(" ;,.-")
    if not s:
        return ""
    if _RESOURCE_NUMBER_ONLY_RE.fullmatch(s):
        return s
    return s


def _post_clean_value(key: str, val: str) -> str:
    kcf = norm_ws(key).casefold()
    if kcf == "цвет":
        return _post_clean_color_value(val)
    if kcf in {"технология печати", "тип печати"}:
        return _post_clean_technology_value(val)
    if kcf == "ресурс":
        return _post_clean_resource_value(val)
    return norm_ws(val)


def apply_value_normalizers(key: str, val: str, schema: dict[str, Any]) -> str:
    v = norm_ws(val)
    if not v:
        return ""

    vn = schema.get("value_normalizers") or {}
    ops = vn.get(key) or vn.get(key.casefold()) or []

    for op in ops:
        if op == "warranty_months":
            v = normalize_warranty_to_months(v)
        elif op == "trim_ws":
            v = norm_ws(v)

    if norm_ws(key).casefold() not in {"совместимость", "модель", "аналог модели"}:
        v = _RE_LETTER_SLASH_LETTER.sub(r"\1 \2", v)

    v = _post_clean_value(key, v)
    return norm_ws(v)


def iter_source_params(source_offer: SourceOffer) -> list[ParamItem]:
    return list(source_offer.params or [])


def category_type_hint(source_offer: SourceOffer) -> str:
    leaf = norm_ws(source_offer.category_name).casefold()
    if leaf == "ноутбуки":
        return "Ноутбук"
    if leaf == "мониторы":
        return "Монитор"
    if leaf == "моноблоки":
        return "Моноблок"
    if leaf == "настольные пк":
        return "Настольный ПК"
    if leaf == "рабочие станции":
        return "Рабочая станция"
    if leaf == "проекторы":
        return "Проектор"
    if leaf in {"лазерные монохромные мфу", "лазерные цветные мфу", "струйные мфу"}:
        return "МФУ"
    if leaf in {"лазерные монохромные принтеры", "лазерные цветные принтеры", "струйные принтеры"}:
        return "Принтер"
    if leaf == "широкоформатные принтеры":
        return "Широкоформатный принтер"
    if leaf == "сканеры":
        return "Сканер"
    if leaf.startswith("картриджи"):
        return "Картридж"
    if leaf == "тонеры":
        return "Тонер"
    if leaf == "прочие расходные материалы":
        return "Расходный материал"
    if leaf == "батареи. аккумуляторы":
        return "Батарея"
    if leaf == "ибп":
        return "ИБП"
    if leaf == "стабилизаторы":
        return "Стабилизатор"
    return ""


def build_params_from_xml(source_offer: SourceOffer, schema: dict[str, Any]) -> list[ParamItem]:
    out: list[ParamItem] = []

    drop_keys_casefold = {str(x).casefold() for x in (schema.get("drop_keys_casefold") or [])}
    require_letter = bool((schema.get("key_rules") or {}).get("require_letter", True))
    max_len = int((schema.get("key_rules") or {}).get("max_len", 60) or 60)
    max_words = int((schema.get("key_rules") or {}).get("max_words", 9) or 9)

    seen_keys: set[str] = set()

    type_hint = category_type_hint(source_offer)
    if type_hint:
        out.append(ParamItem(name="Тип", value=type_hint, source="category"))
        seen_keys.add("тип")

    for param in iter_source_params(source_offer):
        raw_key = norm_ws(param.name)
        raw_val = norm_ws(param.value)
        if not raw_key or not raw_val:
            continue

        key = normalize_key_aliases(raw_key, schema)
        if not key:
            continue
        if key.casefold() in drop_keys_casefold:
            continue
        if not key_quality_ok(key, require_letter=require_letter, max_len=max_len, max_words=max_words):
            continue

        val = apply_value_normalizers(key, raw_val, schema)
        if not val:
            continue

        kcf = key.casefold()
        if kcf in seen_keys:
            continue

        out.append(ParamItem(name=key, value=val, source="xml"))
        seen_keys.add(kcf)

    return out
