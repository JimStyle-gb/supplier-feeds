# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/compat.py

AlStyle supplier layer — cleanup моделей / совместимости / кодовых серий (stage 4).

Что изменено в этой версии:
- из build_alstyle.py и params_xml.py вынесен слой cleanup для `Модель` / `Аналог модели` / `Совместимость`;
- поведение сохранено максимально близко к v102/v101;
- desc-extract логика пока не переносится сюда.
"""

from __future__ import annotations

import re

from cs.util import norm_ws
from suppliers.alstyle.desc_clean import _fix_common_broken_words


_CODE_SERIES_RE = re.compile(
    r"(?<![\w/])(?:(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,}(?:\s*/\s*(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,})+)"
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
    s = re.sub(r"(?i)(Canon\s+PIXMA\s+[A-Za-z]*\d+[A-Za-z0-9-]*)(?=Canon\s+PIXMA)", r"\1 / ", s)
    s = re.sub(r"(?i)(Xerox\s+[A-Za-z-]*\d+[A-Za-z0-9/-]*)(?=Xerox\s+)", r"\1 / ", s)
    return norm_ws(s)



def drop_broken_canon_compat_tail(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = re.sub(r"(?iu)^CANON\s+PIXMA\s+", "Canon PIXMA ", s)
    return s



def clean_compatibility_text(v: str) -> str:
    s = drop_broken_canon_compat_tail(v)
    s = re.sub(r"(?iu)^Xerox\s+Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)^Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)^Для\s+принтеров\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)^Для\s+МФУ\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"\bWorkCenter\b", "WorkCentre", s, flags=re.I)
    s = split_glued_brand_models(s)
    s = dedupe_slash_tail_models(s)
    return norm_ws(s)



def sanitize_param_value(key: str, val: str) -> str:
    v = norm_ws(val)
    if not v:
        return ""
    kcf = norm_ws(key).casefold()
    if kcf == "совместимость":
        v = clean_compatibility_text(v)
    elif kcf in {"модель", "аналог модели"}:
        v = dedupe_code_series_text(v)
    else:
        v = _fix_common_broken_words(v)
    return norm_ws(v)
