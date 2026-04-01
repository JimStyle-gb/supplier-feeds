# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/desc_clean.py

AlStyle description cleaning.
Только narrative-cleaning, без desc->params extraction.

v126:
- жёстче режет техблок после явной шапки "Характеристики / Основные характеристики / Технические характеристики";
- умеет резать inline-кейс вида "Характеристики..." / "Характеристики ..." в конце narrative;
- не откатывает cut-back к исходному тексту, если техблок найден явно;
- сохраняет безопасные helper-функции для compat.py / builder.py / desc_extract.py;
- добавляет короткие русские комментарии в спорных местах.
"""

from __future__ import annotations

import re
from html import unescape

from cs.util import norm_ws

# ----------------------------- regex / const -----------------------------

_CODE_SERIES_RE = re.compile(
    r"(?<![\w/])(?:(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,}(?:\s*/\s*(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,})+)"
)
_OAICITE_RE = re.compile(r"(?is):{0,2}contentReference\[[^\]]*oaicite[^\]]*\](?:\{[^{}]*\})?")
_REPEATED_BRAND_RE = re.compile(
    r"(?iu)\b(Xerox|Canon|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark)\s+\1\b"
)
_PURE_GARBAGE_LINE_RE = re.compile(r"(?iu)^(?:>|&gt;|&amp;gt;|&lt;|&amp;lt;|\.|,|;|:)$")
_TECH_HEADER_LINE_RE = re.compile(
    r"(?iu)^(?:Характеристики|Основные\s+характеристики|Технические\s+характеристики)\s*:?\s*$"
)
_TECH_HEADER_INLINE_RE = re.compile(
    r"(?iu)(?:^|[\n\r]|[.!?]\s+)(Характеристики|Основные\s+характеристики|Технические\s+характеристики)\s*(?:[:.…]|$)"
)
_SPEC_LABEL_RE = re.compile(
    r"(?iu)^(?:"
    r"Модель|Аналог\s+модели|Совместимость|Совместимые\s+модели|Для\s+принтеров|Устройства?|"
    r"Технология\s+печати|Цвет(?:\s+печати)?|Ресурс(?:\s+картриджа)?|Количество\s+страниц|"
    r"Кол-во\s+страниц\s+при\s+5%\s+заполнении\s+А4|Емкость|Ёмкость|Емкость\s+лотка|Ёмкость\s+лотка|"
    r"Степлирование|Дополнительные\s+опции|Применение|Количество\s+в\s+упаковке|Колличество\s+в\s+упаковке|"
    r"Производитель|Объем\s+картриджа,\s*мл|Объём\s+картриджа,\s*мл"
    r")\b"
)
_LABEL_BREAK_PATTERNS = [
    r"Основные\s+характеристики",
    r"Технические\s+характеристики",
    r"Характеристики",
    r"Модель",
    r"Аналог\s+модели",
    r"Совместимые\s+модели",
    r"Совместимость",
    r"Устройства?",
    r"Для\s+принтеров",
    r"Технология\s+печати",
    r"Цвет\s+печати",
    r"Цвет",
    r"Ресурс\s+картриджа,\s*[cс]тр\.",
    r"Ресурс\s+картриджа",
    r"Ресурс",
    r"Количество\s+страниц",
    r"Кол-во\s+страниц\s+при\s+5%\s+заполнении\s+А4",
    r"Емкость\s+лотка",
    r"Ёмкость\s+лотка",
    r"Емкость",
    r"Ёмкость",
    r"Степлирование",
    r"Дополнительные\s+опции",
    r"Применение",
    r"Количество\s+в\s+упаковке",
    r"Колличество\s+в\s+упаковке",
]
_LABEL_BREAK_RE = re.compile(
    r"(?<!^)(?<!\n)(?=\b(?:" + "|".join(_LABEL_BREAK_PATTERNS) + r")\b)",
    re.IGNORECASE,
)

# ----------------------------- helpers -----------------------------

def dedupe_code_series_text(text: str) -> str:
    s = norm_ws(text)
    if not s:
        return ""

    def repl(m: re.Match[str]) -> str:
        parts = [norm_ws(x) for x in re.split(r"\s*/\s*", m.group(0)) if norm_ws(x)]
        out: list[str] = []
        seen: set[str] = set()
        for p in parts:
            key = p.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
        return " / ".join(out)

    return _CODE_SERIES_RE.sub(repl, s)


def is_service_desc_line(line: str) -> bool:
    s = norm_ws(unescape(re.sub(r"<[^>]+>", " ", line or "")))
    if not s:
        return True
    if _PURE_GARBAGE_LINE_RE.fullmatch(s):
        return True
    low = s.casefold()
    if low.startswith(("body {", "font-family:", "display:", "margin:", "padding:", "border:", "color:", "background:")):
        return True
    return False


def fix_common_broken_words(s: str) -> str:
    s = s or ""
    fixes = {
        "в случаи": "в случае",
        "В случаи": "В случае",
        "в отличии": "в отличие",
        "В отличии": "В отличие",
        "при прокладки": "при прокладке",
        "электропитания.Часто": "электропитания. Часто",
        "конфигурацией!Для": "конфигурацией! Для",
        "см. в Сопутствующие товары": "см. в сопутствующих товарах",
        "Колличество": "Количество",
        "WorkCenter": "WorkCentre",
    }
    out = s
    for bad, good in fixes.items():
        out = out.replace(bad, good)
    out = re.sub(r"\bCANON\s+PIXMA\b", "Canon PIXMA", out, flags=re.I)
    out = re.sub(r"\bCanon\s+Pixma\b", "Canon PIXMA", out, flags=re.I)
    out = re.sub(r"\bCanon\s+imageprograf\b", "Canon ImagePROGRAF", out, flags=re.I)
    out = re.sub(r"\bCANON\s+IMAGEPROGRAF\b", "Canon ImagePROGRAF", out, flags=re.I)
    out = re.sub(r"\bCanon\s+imagerunner\b", "Canon imageRUNNER", out, flags=re.I)
    out = re.sub(r"\bCANON\s+IMAGERUNNER\b", "Canon imageRUNNER", out, flags=re.I)
    return out


def _dedupe_repeated_brands(s: str) -> str:
    out = s or ""
    prev = None
    while out != prev:
        prev = out
        out = _REPEATED_BRAND_RE.sub(r"\1", out)
    return out


def _inject_label_breaks(s: str) -> str:
    out = s or ""
    out = _LABEL_BREAK_RE.sub("\n", out)
    out = re.sub(r"(?iu)(Характеристики)\s+([А-ЯA-Z])", r"\1\n\2", out)
    return out


def _preserve_clean_lines(lines: list[str]) -> str:
    out: list[str] = []
    prev_blank = False
    for raw in lines:
        ln = norm_ws(raw)
        if not ln:
            if not prev_blank and out:
                out.append("")
            prev_blank = True
            continue
        prev_blank = False
        if is_service_desc_line(ln):
            continue
        if _PURE_GARBAGE_LINE_RE.fullmatch(ln):
            continue
        out.append(ln)
    while out and not out[-1]:
        out.pop()
    return "\n".join(out).strip()


def _find_explicit_tech_start(lines: list[str]) -> int | None:
    for idx, ln in enumerate(lines):
        if _TECH_HEADER_LINE_RE.fullmatch(ln):
            return idx
    return None


def _looks_like_spec_block(lines: list[str], start_idx: int) -> bool:
    sample = [norm_ws(x) for x in lines[start_idx:start_idx + 6] if norm_ws(x)]
    if not sample:
        return False
    score = 0
    for ln in sample:
        if _SPEC_LABEL_RE.match(ln):
            score += 1
        if ":" in ln and _SPEC_LABEL_RE.match(ln.split(":", 1)[0]):
            score += 1
    return score >= 2


def _cut_explicit_tech_block(text: str) -> tuple[str, bool]:
    """Режем техблок, если нашли явную шапку характеристик."""
    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", text) if norm_ws(x)]
    if not lines:
        return "", False

    idx = _find_explicit_tech_start(lines)
    if idx is None:
        return text, False

    # До шапки должно оставаться хоть немного narrative.
    head = "\n".join(lines[:idx]).strip()
    if head and len(norm_ws(head)) >= 30:
        return head, True

    # Если narrative почти нет — отдаем пусто, а не техблок.
    return "", True


def _cut_inline_tech_tail(text: str) -> tuple[str, bool]:
    """
    Режем inline-кейс, когда после narrative внутри той же строки
    начинается "Характеристики ..." и дальше идут label/value пары.
    """
    s = text or ""
    m = _TECH_HEADER_INLINE_RE.search(s)
    if not m:
        return s, False

    tail = s[m.start():]
    tail_lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", _inject_label_breaks(tail)) if norm_ws(x)]
    if not _looks_like_spec_block(tail_lines, 0):
        return s, False

    head = s[:m.start()].strip()
    head = re.sub(r"[ ,;:\-–—]+$", "", head).strip()
    return head, True


def strip_desc_sections(desc: str) -> str:
    s = desc or ""
    cut1, used1 = _cut_explicit_tech_block(s)
    if used1:
        return cut1
    cut2, used2 = _cut_inline_tech_tail(s)
    if used2:
        return cut2
    return s


def dedupe_desc_leading_title(name: str, desc: str) -> str:
    title = norm_ws(name)
    text = norm_ws(desc)
    if not title or not text:
        return text
    pat = re.compile(r"(?iu)^" + re.escape(title) + r"(?:[\s\-–—:,.]+)?")
    stripped = pat.sub("", text, count=1).strip()
    return stripped or text


def _strip_name_prefix_from_first_line(name: str, desc: str) -> str:
    title = norm_ws(name)
    if not title or not desc:
        return desc
    lines = desc.split("\n")
    if not lines:
        return desc
    first = dedupe_desc_leading_title(title, lines[0])
    lines[0] = first
    return "\n".join(x for x in lines if norm_ws(x))


def align_desc_model_from_name(name: str, desc: str) -> str:
    # Безопасный no-op: интерфейс нужен builder.py.
    _ = name
    return desc or ""


def _drop_conflicting_named_blocks(name: str, desc: str) -> str:
    # На этом этапе оставляем безопасный no-op: не хотим агрессивно ломать narrative.
    _ = name
    return desc or ""

# ----------------------------- main pipeline -----------------------------

def clean_desc_text_for_extraction(desc: str) -> str:
    s = unescape(desc or "")
    s = _OAICITE_RE.sub(" ", s)
    s = re.sub(r"<\s*br\s*/?\s*>", "\n", s, flags=re.I)
    s = re.sub(r"</p\s*>", "\n", s, flags=re.I)
    s = re.sub(r"<\s*/?(?:div|li|ul|ol|table|tr|td|th|h[1-6])\b[^>]*>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = fix_common_broken_words(s)
    s = _inject_label_breaks(s)
    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", s)]
    return _preserve_clean_lines(lines)


def sanitize_desc_quality_text(desc: str) -> str:
    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", desc or "") if norm_ws(x)]
    out: list[str] = []
    for ln in lines:
        s = fix_common_broken_words(ln)
        s = _dedupe_repeated_brands(s)
        s = dedupe_code_series_text(s)
        s = norm_ws(s)
        if not s:
            continue
        if _TECH_HEADER_LINE_RE.fullmatch(s):
            continue
        if _PURE_GARBAGE_LINE_RE.fullmatch(s):
            continue
        # Кейс "Характеристики…" в конце narrative не оставляем.
        if re.fullmatch(r"(?iu)Характеристики(?:\.\.\.|…)?", s):
            continue
        out.append(s)
    return _preserve_clean_lines(out)


def sanitize_native_desc(desc: str, *, name: str = "") -> str:
    raw = clean_desc_text_for_extraction(desc)
    if not raw:
        return ""

    before_sections = raw
    raw = strip_desc_sections(raw)

    # ВАЖНО:
    # если техблок нашли явно — не откатываемся назад только потому,
    # что narrative стал короче. Иначе в body снова протекает "Характеристики".
    explicit_cut = raw != before_sections

    if (not explicit_cut) and len(norm_ws(raw)) < max(40, int(len(norm_ws(before_sections)) * 0.35)):
        raw = before_sections

    if name:
        raw = align_desc_model_from_name(name, raw)
        raw = dedupe_desc_leading_title(name, raw)
        raw = _strip_name_prefix_from_first_line(name, raw)
        raw = _drop_conflicting_named_blocks(name, raw)

    raw = sanitize_desc_quality_text(raw)

    if name:
        raw = _drop_conflicting_named_blocks(name, raw)

    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", raw) if norm_ws(x)]
    while lines and (lines[0][:1] in {"(", ",", ";", ":"} or is_service_desc_line(lines[0])):
        lines.pop(0)

    lines = [x for x in lines if x not in {".", ",", ";", ":"}]
    return "\n".join(lines).strip()


# Backward-compatible aliases for already split stages.
_is_service_desc_line = is_service_desc_line
_fix_common_broken_words = fix_common_broken_words
_dedupe_desc_leading_title = dedupe_desc_leading_title
_align_desc_model_from_name = align_desc_model_from_name
_sanitize_native_desc = sanitize_native_desc
_sanitize_desc_quality_text = sanitize_desc_quality_text
