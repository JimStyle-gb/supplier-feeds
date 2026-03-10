# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/compat.py

AlStyle supplier layer — cleanup моделей / совместимости / кодовых серий.

v117:
- убран дубль бренда в начале совместимости: Xerox Xerox -> Xerox;
- дочищается Xerox Для Xerox ... и похожие хвосты;
- Canon PIXMA канонизируется по всей строке;
- WorkCenter -> WorkCentre;
- лучше режутся склеенные brand-model цепочки.
"""

from __future__ import annotations

import re

from cs.util import norm_ws
from suppliers.alstyle.desc_clean import fix_common_broken_words


_CODE_SERIES_RE = re.compile(
    r"(?<![\w/])(?:(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,}(?:\s*/\s*(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,})+)"
)
_BRAND_GLUE_PATTERNS = [
    (re.compile(r"(?i)(Canon\s+PIXMA\s+[A-Za-z]*\d+[A-Za-z0-9-]*)(?=\s*Canon\s+PIXMA)"), r"\1 / "),
    (re.compile(r"(?i)(Xerox\s+[A-Za-z-]*\d+[A-Za-z0-9/-]*)(?=\s*Xerox\s+)"), r"\1 / "),
    (re.compile(r"(?i)(WorkCentre\s+[A-Za-z-]*\d+[A-Za-z0-9/-]*)(?=\s*WorkCentre\s+)"), r"\1 / "),
]
_REPEATED_BRAND_RE = re.compile(
    r"(?iu)\b(Xerox|Canon|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark)\s+\1\b"
)


def dedupe_code_series_text(text: str) -> str:
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


def dedupe_slash_tail_models(v: str) -> str:
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


def split_glued_brand_models(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    for rx, repl in _BRAND_GLUE_PATTERNS:
        s = rx.sub(repl, s)
    s = re.sub(
        r"(?<=[A-Za-zА-Яа-я0-9])(?=(?:Canon|CANON|Xerox|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark)\s+"
        r"(?:PIXMA|WorkCentre|WorkCenter|VersaLink|AltaLink|Phaser|ColorQube|CopyCentre|imageRUNNER|i-SENSYS|ECOSYS|LaserJet|DeskJet|OfficeJet)\b)",
        " / ",
        s,
    )
    return norm_ws(s)


def _canonize_brand_case(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = re.sub(r"(?iu)\bCANON\s+PIXMA\b", "Canon PIXMA", s)
    s = re.sub(r"(?iu)\bCanon\s+Pixma\b", "Canon PIXMA", s)
    s = re.sub(r"(?iu)\bWorkCenter\b", "WorkCentre", s)
    return norm_ws(s)


def _dedupe_repeated_brand_prefixes(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    while True:
        nxt = _REPEATED_BRAND_RE.sub(lambda m: m.group(1), s)
        if nxt == s:
            break
        s = nxt
    return norm_ws(s)


def clean_compatibility_text(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = fix_common_broken_words(s)
    s = _canonize_brand_case(s)
    s = _dedupe_repeated_brand_prefixes(s)

    s = re.sub(r"(?iu)\bXerox\s+Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)\bXerox\s+Для\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)\bДля,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)\bДля\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)\bДля\s+принтеров\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)\bДля\s+МФУ\s+Xerox\s+", "Xerox ", s)

    s = split_glued_brand_models(s)
    s = _canonize_brand_case(s)
    s = _dedupe_repeated_brand_prefixes(s)

    s = re.sub(r"\s*,\s*/\s*", " / ", s)
    s = re.sub(r"\s*/\s*,\s*", " / ", s)
    s = re.sub(r"\s{2,}", " ", s)

    s = dedupe_slash_tail_models(s)
    s = _dedupe_repeated_brand_prefixes(s)
    return norm_ws(s.strip(" ;,.-"))


def sanitize_param_value(key: str, val: str) -> str:
    v = norm_ws(val)
    if not v:
        return ""
    kcf = norm_ws(key).casefold()
    if kcf == "совместимость":
        v = clean_compatibility_text(v)
    elif kcf in {"модель", "аналог модели"}:
        v = dedupe_code_series_text(fix_common_broken_words(v))
    else:
        v = fix_common_broken_words(v)
    return norm_ws(v)
