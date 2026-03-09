# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/params_xml.py

XML params pipeline для AlStyle.

Этап 2 split:
- вынесены schema-cleanup и normalizers для родных XML param;
- поведение сохранено максимально близко к v100/v99;
- desc/compat логика пока ещё не переносится сюда.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any

from cs.util import norm_ws


_RE_HAS_LETTER = re.compile(r"[A-Za-zА-Яа-яЁё]")
_RE_LETTER_SLASH_LETTER = re.compile(r"([A-Za-zА-Яа-яЁё])\s*/\s*([A-Za-zА-Яа-яЁё])")
_CODE_SERIES_RE = re.compile(
    r"(?<![\w/])(?:(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,}(?:\s*/\s*(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,})+)"
)


def _key_quality_ok(k: str, *, require_letter: bool, max_len: int, max_words: int) -> bool:
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



def _normalize_warranty_to_months(v: str) -> str:
    vv = norm_ws(v)
    if not vv:
        return ""
    low = vv.casefold()
    if low in ("нет", "no", "-", "—"):
        return ""
    m = re.search(r"(\d{1,2})\s*(год|года|лет)\b", low)
    if m:
        n = int(m.group(1))
        return f"{n*12} мес"
    if re.fullmatch(r"\d{1,3}", low):
        return f"{int(low)} мес"
    m = re.search(r"\b(\d{1,3})\b", low)
    if m and ("мес" in low or "month" in low):
        return f"{int(m.group(1))} мес"
    return vv



def _dedupe_code_series_text(text: str) -> str:
    s = norm_ws(text)
    if not s:
        return ""

    def repl(m: re.Match[str]) -> str:
        raw = m.group(0)
        parts = [norm_ws(x) for x in re.split(r"\s*/\s*", raw) if norm_ws(x)]
        out: list[str] = []
        seen: set[str] = set()
        for p in parts:
            sig = p.casefold()
            if sig in seen:
                continue
            seen.add(sig)
            out.append(p)
        return " / ".join(out)

    return _CODE_SERIES_RE.sub(repl, s)



def _fix_common_broken_words(s: str) -> str:
    s = s or ""
    fixes = {
        "питание м": "питанием",
        "электропитание м": "электропитанием",
        "управление м": "управлением",
        "резервным питание м": "резервным питанием",
        "с системой управления питание м": "с системой управления питанием",
        "и питание м": "и питанием",
        "одним кабелем управляйте": "одним кабелем и управляйте",
        "дополнтельно": "дополнительно",
        "опцонально": "опционально",
        "!!!": "!",
    }
    for a, b in fixes.items():
        s = s.replace(a, b).replace(a.capitalize(), b.capitalize())
    return s



def _normalize_tech_value(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = re.sub(r"(?iu)\bUSB\s+C\b", "USB-C", s)
    s = re.sub(r"(?iu)\bWi\s*Fi\b", "Wi‑Fi", s)
    s = re.sub(r"(?iu)\bBluetooth\s*([0-9.]+)\b", r"Bluetooth \1", s)
    s = re.sub(r"(?iu)\bFull\s*HD\b", "Full HD", s)
    s = re.sub(r"(?iu)\bANSI\s*люмен\b", "ANSI люмен", s)
    return norm_ws(s)



def _drop_broken_canon_compat_tail(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = re.sub(r"(?iu)^CANON\s+PIXMA\s+", "Canon PIXMA ", s)
    return s



def _dedupe_slash_tail_models(v: str) -> str:
    parts = [norm_ws(x) for x in re.split(r"\s*/\s*", v or "") if norm_ws(x)]
    if len(parts) < 2:
        return norm_ws(v)
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        sig = p.casefold()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(p)
    return " / ".join(out)



def _split_glued_brand_models(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = re.sub(r"(?i)(Canon\s+PIXMA\s+[A-Za-z]*\d+[A-Za-z0-9-]*)(?=Canon\s+PIXMA)", r"\1 / ", s)
    s = re.sub(r"(?i)(Xerox\s+[A-Za-z-]*\d+[A-Za-z0-9/-]*)(?=Xerox\s+)", r"\1 / ", s)
    return norm_ws(s)



def _clean_compatibility_text(v: str) -> str:
    s = _drop_broken_canon_compat_tail(v)
    s = re.sub(r"(?iu)^Xerox\s+Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)^Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)^Для\s+принтеров\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)^Для\s+МФУ\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"\bWorkCenter\b", "WorkCentre", s, flags=re.I)
    s = _split_glued_brand_models(s)
    s = _dedupe_slash_tail_models(s)
    return norm_ws(s)



def _sanitize_param_value(key: str, val: str) -> str:
    v = norm_ws(val)
    if not v:
        return ""
    kcf = norm_ws(key).casefold()
    if kcf == "совместимость":
        v = _clean_compatibility_text(v)
    elif kcf in {"модель", "аналог модели"}:
        v = _dedupe_code_series_text(v)
    else:
        v = _fix_common_broken_words(v)
    return norm_ws(v)



def _apply_value_normalizers(key: str, val: str, schema: dict[str, Any]) -> str:
    v = norm_ws(val)
    if not v:
        return ""
    vn = (schema.get("value_normalizers") or {})
    ops = vn.get(key) or vn.get(key.casefold()) or []
    for op in ops:
        if op == "warranty_months":
            v = _normalize_warranty_to_months(v)
        elif op == "trim_ws":
            v = norm_ws(v)
    kcf = norm_ws(key).casefold()
    if kcf not in {"совместимость", "модель", "аналог модели"}:
        v = _RE_LETTER_SLASH_LETTER.sub(r"\1 \2", v)
    v = _sanitize_param_value(key, v)
    if not v:
        return ""
    if kcf not in {"совместимость", "модель", "аналог модели"}:
        v = _normalize_tech_value(v)
        v = re.sub(r"(?<=\d),\s+(?=\d)", ",", v)
        v = re.sub(r"(?iu)\b(\d),(\d{1,3})\s+(мм|см|м|кг|г|Вт|Гц|мс|дюйм(?:а|ов)?|дюйма|дюймов|ГБ|ТБ)\b", r"\1,\2 \3", v)
        v = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+кд\s*(?:/\s*м²|м2)\b", r"\1 кд/м²", v)
        v = re.sub(r"(?iu)\b(\d+(?:,\d+)?)\s+Гбит\s*/?\s*с\b", r"\1 Гбит/с", v)
        v = re.sub(r"(?iu)\b(\d+)\s*[xх×]\s*(\d+)\s*Вт\b", r"\1 × \2 Вт", v)
    return v



def collect_xml_params(offer_el: ET.Element, schema: dict[str, Any]) -> list[tuple[str, str]]:
    drop = {str(x).casefold() for x in (schema.get("drop_keys_casefold") or [])}
    aliases = {str(k).casefold(): str(v) for k, v in (schema.get("aliases_casefold") or {}).items()}
    rules = schema.get("key_rules") or {}
    require_letter = bool(rules.get("require_letter", True))
    max_len = int(rules.get("max_len", 60))
    max_words = int(rules.get("max_words", 9))

    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for p in offer_el.findall("param"):
        k0 = p.get("name") or ""
        v0 = "".join(p.itertext()).strip()

        k = norm_ws(k0)
        v = norm_ws(v0)
        if not k or not v:
            continue

        kcf = k.casefold()
        if kcf in aliases:
            k = aliases[kcf]

        if not _key_quality_ok(k, require_letter=require_letter, max_len=max_len, max_words=max_words):
            continue

        if k.casefold() in drop or k.casefold() in ("код нкт",):
            continue
        if k.casefold() == "назначение" and v.casefold() in ("да", "есть"):
            continue
        if k.casefold() == "безопасность" and v.casefold() == "есть":
            continue

        v2 = _apply_value_normalizers(k, v, schema)
        if not v2:
            continue

        sig = (k.casefold(), v2.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((k, v2))

    return out
