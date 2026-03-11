# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/desc_clean.py

AlStyle description cleaning.
Только narrative-cleaning, без desc->params extraction.

v122:
- расширяет словарь typо/narrative-fixes (в случае / в отличие / при прокладке и т.п.);
- добавляет regex-дочистку 4-х / или 1 / 100-метровый и похожих хвостов;
- срезает прилипший title-prefix в первой строке native description;
- сохраняет границы строк для multiline extraction;
- мягко разрезает плотные one-line тех-описания на label-friendly строки;
- чище дочищает Xerox/Canon narrative-хвосты;
- не схлопывает extraction-текст обратно в одну строку;
- убирает дубли бренда в narrative;
- чинит Canon imagePROGRAF glue и обрезанный хвост "...610Can";
- режет хвосты совместимости в narrative:
  Цвет / Ресурс / Наличие чипа / Принт-картриджи / Комплект поставки;
- сохраняет уже сделанный фикс CopyCentre 245 / 255;
- вырезает конфликтные intro/warning blocks, если narrative относится к другому accessory/model token;
- удаляет мусорные одиночные строки '>' / '&gt;'.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from html import unescape

from cs.util import norm_ws


_CODE_SERIES_RE = re.compile(
    r"(?<![\w/])(?:(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,}(?:\s*/\s*(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,})+)"
)
_SKU_TOKEN_RE = re.compile(r"\b[A-Z]{1,6}-\d{2,6}[A-Z]{0,4}\b|\b[A-Z]{2,}[A-Z0-9-]{4,}\b")
_MODELISH_TOKEN_RE = re.compile(
    r"\b(?:[A-Z]{2,20}(?:-[A-Z0-9@]{1,20})+|[A-Z][A-Za-z]{1,24}(?:-[A-Z0-9@]{1,20})+)\b"
)
_CSS_SERVICE_LINE_RE = re.compile(
    r"(?iu)(?:^|\s)(?:body\s*\{|font-family\s*:|display\s*:|margin\s*:|padding\s*:|border\s*:|color\s*:|background\s*:|"
    r"\.?chip\s*\{|\.?badge\s*\{|\.?spec\s*\{|h[1-6]\s*\{)"
)
_REPEATED_BRAND_RE = re.compile(
    r"(?iu)\b(Xerox|Canon|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark)\s+\1\b"
)
_COMPAT_STOP_LABEL_RE = re.compile(
    r"(?iu)\b(?:"
    r"Цвет(?:\s+печати)?|"
    r"Ресурс(?:\s+картриджа| фотобарабана)?|"
    r"Количество\s+страниц|"
    r"Наличие\s+чипа|"
    r"Принт-?картриджи(?:\s+EUROPRINT)?|"
    r"Комплект\s+поставки|"
    r"Преимущества|Описание|Особенности|"
    r"Гарантия|"
    r"Условия\s+гарантии"
    r")\b"
)
_COMPAT_NOISE_PHRASE_RE = re.compile(
    r"(?iu)\b(?:"
    r"формата\s+A4\s+можно\s+аккуратно\s+разместить|"
    r"можно\s+аккуратно\s+разместить\s+на\s+рабочих\s+столах|"
    r"что\s+идеально\s+подходит\s+для\s+небольших\s+офисов"
    r")\b"
)
_COMPAT_NARRATIVE_HINT_RE = re.compile(
    r"(?iu)\b(?:Canon|Xerox)\b.*\b(?:"
    r"ImagePROGRAF|imageRUNNER|PIXMA|i-SENSYS|LBP|MF\d|"
    r"WorkCentre|WorkCenter|Versant|DocuColor|CopyCentre|ColorQube|Phaser"
    r")\b"
)

_LABEL_BREAK_PATTERNS = [
    r"Основные\s+характеристики",
    r"Технические\s+характеристики",
    r"Производитель",
    r"Модель",
    r"Аналог\s+модели",
    r"Совместимые\s+модели",
    r"Совместимость",
    r"Устройства",
    r"Устройство",
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
    r"Объем\s+картриджа,\s*мл",
    r"Объём\s+картриджа,\s*мл",
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
_OAICITE_RE = re.compile(r"(?is):{0,2}contentReference\[[^\]]*oaicite[^\]]*\](?:\{[^{}]*\})?")
_BRAND_GLUE_RE = re.compile(
    r"(?<=[A-Za-zА-Яа-я0-9])(?=(?:CANON|Canon|Xerox|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark)\s+"
    r"(?:PIXMA|ImagePROGRAF|imageRUNNER|WorkCentre|WorkCenter|VersaLink|AltaLink|Phaser|ColorQube|CopyCentre|imageRUNNER|i-SENSYS|ECOSYS|LaserJet|DeskJet|OfficeJet)\b)"
)
_SECTION_HEADING_RE = re.compile(
    r"(?iu)^(?:Характеристики|Основные\s+характеристики|Технические\s+характеристики|"
    r"Ключевые\s+особенности|Особенности|Описание|Преимущества|Комплектация|Условия\s+гарантии|Гарантия)\s*:?$"
)
_WARNING_HEAD_RE = re.compile(r"(?iu)^ВНИМАНИЕ!?$")
_CONFLICTING_TITLE_PREFIX_RE = re.compile(
    r"(?iu)^(?:Автоподатчик|Модуль|Плата|Комплект|Крышка|Устройство|Блок|Финишер|Степлер|Факс|"
    r"Тонер(?:-картридж)?|Картридж|Фотобарабан|Драм|Ролик)\b"
)
_ALLOWED_CONFLICT_CONTEXT_RE = re.compile(
    r"(?iu)\b(?:для\s+(?:устройств|принтеров|МФУ|аппаратов)|совместим|совместимость|серии|"
    r"подходит\s+для|используется\s+с|совместно\s+с|не\s+может\s+быть\s+установлен)\b"
)
_PURE_GARBAGE_LINE_RE = re.compile(r"(?iu)^(?:>|&gt;|&amp;gt;|&lt;|&amp;lt;)$")


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


def is_service_desc_line(line: str) -> bool:
    s = norm_ws(unescape(re.sub(r"<[^>]+>", " ", line or "")))
    if not s:
        return True
    if _PURE_GARBAGE_LINE_RE.fullmatch(s):
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
    if re.fullmatch(r"(?i)(hdmi|displayport|usb-?c|usb|rj45|lan|vga|audio)\s*x\d+", s):
        return True
    return False


def fix_common_broken_words(s: str) -> str:
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
        "в случаи": "в случае",
        "В случаи": "В случае",
        "в отличии": "в отличие",
        "В отличии": "В отличие",
        "при прокладки": "при прокладке",
        "при прокладки внутри помещения": "при прокладке внутри помещений",
        "при прокладки внутри помещений": "при прокладке внутри помещений",
        "!!!": "!",
    }
    for a, b in fixes.items():
        s = s.replace(a, b).replace(a.capitalize(), b.capitalize())

    s = re.sub(r"(?iu)\b(\d)\s*-\s*х\b", r"\1-х", s)
    s = re.sub(r"(?iu)\bили(?=\d)", "или ", s)
    s = re.sub(r"(?iu)\b(\d+)\s+метровый\b", r"\1-метровый", s)
    s = re.sub(r"(?iu)(?<=\d)(Гигабит/сек|Мегабит/сек)\b", r" \1", s)
    s = re.sub(r"(?iu)\.\s*Также\b", ". Также", s)
    return s


def norm_title_like_text(s: str) -> str:
    s = norm_ws(unescape(re.sub(r"<[^>]+>", " ", s or "")))
    s = re.sub(r"[()\[\],;:!?.«»\"'`]+", " ", s)
    return norm_ws(s).casefold()


def is_title_like_duplicate(name: str, line: str) -> bool:
    a = norm_title_like_text(name)
    b = norm_title_like_text(line)
    if not a or not b:
        return False
    if a == b:
        return True
    if a in b or b in a:
        shorter = min(len(a), len(b))
        longer = max(len(a), len(b))
        if shorter >= max(12, int(longer * 0.7)):
            return True
    return SequenceMatcher(None, a, b).ratio() >= 0.9


def _extract_modelish_tokens(text: str) -> set[str]:
    out: set[str] = set()
    for m in _MODELISH_TOKEN_RE.finditer(text or ""):
        tok = norm_ws(m.group(0)).upper().rstrip(".,;:)]}>")
        if tok:
            out.add(tok)
    return out


def _looks_like_conflicting_product_title_line(name_tokens: set[str], line: str) -> bool:
    s = norm_ws(line)
    if not s or len(s) > 180:
        return False
    line_tokens = _extract_modelish_tokens(s)
    if not line_tokens:
        return False
    if name_tokens & line_tokens:
        return False
    if not _CONFLICTING_TITLE_PREFIX_RE.match(s):
        return False
    if _ALLOWED_CONFLICT_CONTEXT_RE.search(s):
        return False
    return True


def _warning_block_has_conflict(name_tokens: set[str], lines: list[str], start_idx: int) -> bool:
    probe = []
    j = start_idx + 1
    while j < len(lines):
        ln = norm_ws(lines[j])
        if not ln:
            break
        if _SECTION_HEADING_RE.match(ln):
            break
        probe.append(ln)
        if len(probe) >= 5:
            break
        j += 1

    if not probe:
        return False

    for ln in probe:
        toks = _extract_modelish_tokens(ln)
        if not toks:
            continue
        if name_tokens & toks:
            return False
        if _CONFLICTING_TITLE_PREFIX_RE.match(ln) or "Canon" in ln or "Xerox" in ln:
            return True
    return False


def _drop_conflicting_named_blocks(name: str, desc: str) -> str:
    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", desc or "") if norm_ws(x)]
    if not lines:
        return ""

    name_tokens = _extract_modelish_tokens(name)
    if not name_tokens:
        return "\n".join(lines)

    while lines and _looks_like_conflicting_product_title_line(name_tokens, lines[0]):
        lines.pop(0)

    out: list[str] = []
    i = 0
    while i < len(lines):
        ln = lines[i]

        if _WARNING_HEAD_RE.match(ln) and _warning_block_has_conflict(name_tokens, lines, i):
            i += 1
            while i < len(lines):
                nxt = norm_ws(lines[i])
                if not nxt or _SECTION_HEADING_RE.match(nxt):
                    break
                i += 1
            continue

        if not out and _looks_like_conflicting_product_title_line(name_tokens, ln):
            i += 1
            continue

        out.append(ln)
        i += 1

    return "\n".join(out)


def dedupe_desc_leading_title(name: str, desc: str) -> str:
    parts = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", unescape(desc or "")) if norm_ws(x)]
    while parts and is_title_like_duplicate(name, parts[0]):
        parts.pop(0)
    return "\n".join(parts)


def strip_desc_sections(desc: str) -> str:
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


def align_desc_model_from_name(name: str, desc: str) -> str:
    n = norm_ws(name)
    raw = unescape(desc or "")
    if not n or not raw:
        return norm_ws(raw)
    m_name = _SKU_TOKEN_RE.search(n)
    if not m_name:
        return raw
    sku_name = m_name.group(0)

    lines = [x for x in re.split(r"(?:\r?\n)+", raw)]
    if not lines:
        return raw
    first_line = norm_ws(lines[0])
    if not first_line:
        return raw

    m_desc = _SKU_TOKEN_RE.search(first_line)
    if not m_desc:
        return raw
    sku_desc = m_desc.group(0)
    if sku_desc == sku_name:
        return raw
    if len(sku_desc) >= 6 and len(sku_name) >= 6 and SequenceMatcher(None, sku_desc, sku_name).ratio() >= 0.82:
        lines[0] = first_line.replace(sku_desc, sku_name, 1)
        return "\n".join(lines)
    return raw


def _strip_name_prefix_from_first_line(name: str, desc: str) -> str:
    n = norm_ws(name)
    raw = unescape(desc or "")
    if not n or not raw:
        return raw

    lines = [x for x in re.split(r"(?:\r?\n)+", raw)]
    if not lines:
        return raw

    first = norm_ws(lines[0])
    if not first:
        return raw

    n_cf = n.casefold()
    first_cf = first.casefold()

    if first_cf.startswith(n_cf):
        tail = norm_ws(first[len(n):]).lstrip(" -—:;,.")
        if tail and re.search(r"[A-Za-zА-Яа-яЁё]", tail):
            lines[0] = tail
            return "\n".join(lines)

    return raw


def _preserve_clean_lines(lines: list[str]) -> str:
    out: list[str] = []
    prev = ""
    for raw in lines:
        ln = norm_ws(raw)
        if not ln or is_service_desc_line(ln):
            continue
        if prev and prev.casefold() == ln.casefold():
            continue
        out.append(ln)
        prev = ln
    return "\n".join(out)


def _inject_label_breaks(text: str) -> str:
    s = text or ""
    if not s:
        return ""
    s = _BRAND_GLUE_RE.sub("\n", s)
    s = re.sub(r"(?iu)\b(Характеристики|Основные\s+характеристики|Технические\s+характеристики)\b\s*", r"\n\1\n", s)
    s = _LABEL_BREAK_RE.sub("\n", s)
    s = re.sub(r"(?iu)\b(Совместимость)\s+(Устройства|Устройство|Совместимые\s+модели|Для\s+принтеров)\b", r"\1\n\2", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _dedupe_repeated_brands(s: str) -> str:
    out = norm_ws(s)
    if not out:
        return ""
    while True:
        nxt = _REPEATED_BRAND_RE.sub(lambda m: m.group(1), out)
        if nxt == out:
            break
        out = nxt
    return norm_ws(out)


def _fix_known_xerox_compat_typos(s: str) -> str:
    out = s or ""
    out = re.sub(r"(?iu)\bCopyCentre\s+245\s*/\s*25\b", "CopyCentre 245 / 255", out)
    out = re.sub(r"(?iu)\bWorkCentre\s+7220i\s*/\s*7225i\b", "WorkCentre 7220i / 7225i", out)
    out = re.sub(r"(?iu)\bWorkCentre\s+5865i\s*/\s*5875i\s*/\s*5890i\b", "WorkCentre 5865i / 5875i / 5890i", out)
    return out


def _fix_known_canon_compat_typos(s: str) -> str:
    out = s or ""
    out = re.sub(r"(?iu)\b(Canon\s+ImagePROGRAF\s+\d+)(?=Canon\s+ImagePROGRAF\s+\d+\b)", r"\1 / ", out)
    out = re.sub(r"(?iu)\b(Canon\s+ImagePROGRAF\s+\d+)\s*Can\b", r"\1", out)
    out = re.sub(
        r"(?iu)\b(Canon\s+imageRUNNER\s+ADVANCE(?:\s+DX)?\s+[A-Za-z]*\d+[A-Za-z0-9-]*)(?=Canon\s+imageRUNNER\s+ADVANCE)",
        r"\1 / ",
        out,
    )
    return out


def _trim_compat_narrative_noise(s: str) -> str:
    out = norm_ws(s)
    if not out:
        return ""

    cut_positions: list[int] = []

    m = _COMPAT_STOP_LABEL_RE.search(out)
    if m and m.start() >= 8:
        cut_positions.append(m.start())

    m = _COMPAT_NOISE_PHRASE_RE.search(out)
    if m and m.start() >= 8:
        cut_positions.append(m.start())

    if cut_positions:
        out = out[: min(cut_positions)]

    return norm_ws(out.strip(" ;,.-"))


def _looks_like_compat_narrative_line(s: str) -> bool:
    if not s:
        return False
    if re.match(r"(?iu)^(Совместимость|Совместимые\s+модели|Устройства|Для\s+принтеров)\b", s):
        return True
    return bool(_COMPAT_NARRATIVE_HINT_RE.search(s))


def _clean_compat_narrative_line(s: str) -> str:
    out = fix_common_broken_words(s)
    out = _dedupe_repeated_brands(out)

    out = re.sub(r"(?iu)Совместимые\s+модели\s+Xerox\s+Для\s+Xerox\s+", "Совместимые модели Xerox ", out)
    out = re.sub(r"(?iu)Xerox\s+Для,\s+Xerox\s+", "Xerox ", out)
    out = re.sub(r"(?iu)Xerox\s+Для\s+Xerox\s+", "Xerox ", out)
    out = re.sub(r"(?iu)Для,\s+Xerox\s+", "Xerox ", out)
    out = re.sub(r"(?iu)Для\s+Xerox\s+", "Xerox ", out)
    out = re.sub(r"(?iu)Для\s+принтеров\s+Xerox\s+", "Xerox ", out)
    out = re.sub(r"(?iu)Для\s+МФУ\s+Xerox\s+", "Xerox ", out)

    out = re.sub(r"\bWorkCenter\b", "WorkCentre", out, flags=re.I)
    out = re.sub(r"(?iu)\bCANON\s+PIXMA\b", "Canon PIXMA", out)
    out = re.sub(r"(?iu)\bCanon\s+Pixma\b", "Canon PIXMA", out)
    out = re.sub(r"(?iu)\bCanon\s+imageprograf\b", "Canon ImagePROGRAF", out)
    out = re.sub(r"(?iu)\bCANON\s+IMAGEPROGRAF\b", "Canon ImagePROGRAF", out)
    out = re.sub(r"(?iu)\bCanon\s+imagerunner\b", "Canon imageRUNNER", out)
    out = re.sub(r"(?iu)\bCANON\s+IMAGERUNNER\b", "Canon imageRUNNER", out)

    out = _fix_known_xerox_compat_typos(out)
    out = _fix_known_canon_compat_typos(out)
    out = dedupe_code_series_text(out)
    out = _dedupe_repeated_brands(out)
    out = _trim_compat_narrative_noise(out)

    return norm_ws(out)


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
        if _looks_like_compat_narrative_line(ln):
            s = _clean_compat_narrative_line(ln)
        else:
            s = fix_common_broken_words(ln)
            s = re.sub(r"\bWorkCenter\b", "WorkCentre", s, flags=re.I)
            s = _dedupe_repeated_brands(s)
            s = _fix_known_canon_compat_typos(s)
            s = norm_ws(s)
        if s and not _PURE_GARBAGE_LINE_RE.fullmatch(s):
            out.append(s)
    return _preserve_clean_lines(out)


def sanitize_native_desc(desc: str, *, name: str = "") -> str:
    raw = clean_desc_text_for_extraction(desc)
    if not raw:
        return ""
    before_sections = raw
    raw = strip_desc_sections(raw)
    if len(norm_ws(raw)) < max(40, int(len(norm_ws(before_sections)) * 0.35)):
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
    return "\n".join(lines)


# Backward-compatible aliases for already split stages.
_is_service_desc_line = is_service_desc_line
_fix_common_broken_words = fix_common_broken_words
_dedupe_desc_leading_title = dedupe_desc_leading_title
_align_desc_model_from_name = align_desc_model_from_name
_sanitize_native_desc = sanitize_native_desc
_sanitize_desc_quality_text = sanitize_desc_quality_text
