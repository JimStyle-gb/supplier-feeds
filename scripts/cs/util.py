# -*- coding: utf-8 -*-
"""
CS Util — мелкие общие утилиты.

Этап 8: вынос из cs/core.py в отдельный модуль.
Важно: модуль НЕ импортирует cs/core.py (без циклических импортов).
"""

from __future__ import annotations

import re
from typing import Any


def norm_ws(s: str) -> str:
    s2 = (s or "").replace("\u00a0", " ").strip()
    s2 = re.sub(r"\s+", " ", s2)
    s2 = fix_mixed_cyr_lat(s2)
    return s2.strip()

def safe_int(v) -> int | None:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        s = s.replace(" ", "").replace("\u00a0", "")
        # иногда цена приходит как "12 345.00"
        s = s.split(".")[0]
        return int(s)
    except Exception:
        return None


# Парсит множество id из env (например "1,10,20") или из fallback списка

def _truncate_text(s: str, max_len: int, *, suffix: str = "") -> str:
    # CS: безопасно режем строку по границе слова/запятой
    s = norm_ws(s)
    if max_len <= 0:
        return ""
    if len(s) <= max_len:
        return s

    cut_len = max_len - len(suffix)
    if cut_len <= 0:
        return suffix[:max_len]

    chunk = s[:cut_len].rstrip()
    # режем по последней "хорошей" границе
    for sep in (",", " ", "/", ";"):
        j = chunk.rfind(sep)
        if j >= max(0, cut_len - 40):  # не уходим слишком далеко назад
            chunk = chunk[:j].rstrip(" ,/;")
            break

    chunk = chunk.rstrip(" ,/;")
    if suffix:
        return (chunk + suffix)[:max_len]
    return chunk
