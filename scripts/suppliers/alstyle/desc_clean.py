# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/desc_clean.py

AlStyle supplier layer — narrative description cleaning (stage 3).

Что изменено в этой версии:
- из build_alstyle.py вынесен слой очистки native description;
- поведение сохранено максимально близким к v101;
- desc/compat extraction логика пока не менялась и будет переноситься отдельно.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from html import unescape

from cs.util import norm_ws


_CODE_SERIES_RE = re.compile(
    r"(?<![\w/])(?:(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,}(?:\s*/\s*(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,})+)"
)
_SKU_TOKEN_RE = re.compile(r"\b[A-Z]{2,}[A-Z0-9-]{4,}\b")
_CSS_SERVICE_LINE_RE = re.compile(
    r"(?iu)(?:^|\s)(?:body\s*\{|font-family\s*:|display\s*:|margin\s*:|padding\s*:|border\s*:|color\s*:|background\s*:|"
    r"\.?chip\s*\{|\.?badge\s*\{|\.?spec\s*\{|h[1-6]\s*\{)"
)


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


def _is_service_desc_line(line: str) -> bool:
    s = norm_ws(unescape(re.sub(r"<[^>]+>", " ", line or "")))
    if not s:
        return True
    low = s.casefold()
    if _CSS_SERVICE_LINE_RE.search(s):
        return True
    if low.startswith(("body {", "font-family:", "display:", "margin:", "padding:", "border:", "color:", "background:")):
        return True
    if re.fullmatch(r"(?i)(print\s*/\s*scan\s*/\s*copy|wi-?fi\s+wireless\s+printing|mi\s+home\s+app\s+support)", s):
        return True
    if re.fullmatch(r"(?i)(window\s+hello|windows\s+hello)", s):
        return True
    if re.fullmatch(r"(?i)[A-Z0-9][A-Z0-9\-+/ ]{1,18}\s*x\d+", s):
        return True
    if re.fullmatch(r"(?i)(hdmi|displayport|usb-?c|usb|rj45|lan|vga|audio)\s*x\d+", s):
        return True
    return False


def _norm_title_like_text(s: str) -> str:
    s = norm_ws(unescape(re.sub(r"<[^>]+>", " ", s or "")))
    s = re.sub(r"[()\[\],;:!?.«»\"'`]+", " ", s)
    return norm_ws(s).casefold()


def _is_title_like_duplicate(name: str, line: str) -> bool:
    a = _norm_title_like_text(name)
    b = _norm_title_like_text(line)
    if not a or not b:
        return False
    if a == b:
        return True
    if a in b or b in a:
        shorter = min(len(a), len(b))
        longer = max(len(a), len(b))
        if shorter >= max(12, int(longer * 0.7)):
            return True
    ratio = SequenceMatcher(None, a, b).ratio()
    return ratio >= 0.9


def _dedupe_desc_leading_title(name: str, desc: str) -> str:
    parts = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", unescape(desc or "")) if norm_ws(x)]
    while parts and _is_title_like_duplicate(name, parts[0]):
        parts.pop(0)
    return "\n".join(parts)


def _strip_desc_sections(desc: str) -> str:
    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", unescape(desc or ""))]
    out: list[str] = []
    skip = False
    skipped_any = False
    for ln in lines:
        if not ln:
            continue
        if re.match(r"(?iu)^(порты|что\s+в\s+коробке|комплектация)\s*:?$", ln):
            skip = True
            skipped_any = True
            continue
        if skip:
            if re.match(r"(?iu)^(описание|особенности|преимущества|характеристики|технические характеристики|гарантия)\s*:?$", ln):
                skip = False
            else:
                continue
        out.append(ln)
    cleaned = "\n".join(out)
    if skipped_any:
        before = len(norm_ws(unescape(desc or "")))
        after = len(norm_ws(cleaned))
        if before and after < max(40, int(before * 0.35)):
            return norm_ws(unescape(desc or ""))
    return cleaned


def _align_desc_model_from_name(name: str, desc: str) -> str:
    n = norm_ws(name)
    d = norm_ws(unescape(desc or ""))
    if not n or not d:
        return d
    m_name = _SKU_TOKEN_RE.search(n)
    if not m_name:
        return d
    sku_name = m_name.group(0)
    first_sent = re.split(r"(?<=[.!?])\s+|\n+", d, maxsplit=1)[0]
    m_desc = _SKU_TOKEN_RE.search(first_sent)
    if not m_desc:
        return d
    sku_desc = m_desc.group(0)
    if sku_desc == sku_name:
        return d
    if len(sku_desc) >= 6 and len(sku_name) >= 6 and SequenceMatcher(None, sku_desc, sku_name).ratio() >= 0.82:
        return d.replace(sku_desc, sku_name, 1)
    return d


def _clean_desc_text(s: str) -> str:
    s = unescape(s or "")
    s = re.sub(r"<\s*br\s*/?\s*>", "\n", s, flags=re.I)
    s = re.sub(r"</p\s*>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", s)]
    lines = [x for x in lines if x and not _is_service_desc_line(x)]
    return "\n".join(lines)


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


def _sanitize_desc_quality_text(desc: str) -> str:
    s = norm_ws(desc)
    if not s:
        return ""
    s = _fix_common_broken_words(s)
    s = re.sub(r"(?iu)Xerox\s+Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)Для\s+принтеров\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)Для\s+МФУ\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"\bWorkCenter\b", "WorkCentre", s, flags=re.I)
    s = _dedupe_code_series_text(s)
    return norm_ws(s)


def _sanitize_native_desc(desc: str) -> str:
    raw = _clean_desc_text(desc)
    if not raw:
        return ""
    before_sections = raw
    raw = _strip_desc_sections(raw)
    if len(norm_ws(raw)) < max(40, int(len(norm_ws(before_sections)) * 0.35)):
        raw = before_sections
    raw = _sanitize_desc_quality_text(raw)
    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", raw) if norm_ws(x)]
    while lines and (lines[0][:1] in {"(", ",", ";", ":"} or _is_service_desc_line(lines[0])):
        lines.pop(0)
    return "\n".join(lines)
