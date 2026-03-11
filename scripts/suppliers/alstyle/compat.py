# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/compat.py

AlStyle supplier layer — cleanup моделей / совместимости / кодовых серий.

v130:
- сохраняет текущие Canon/Xerox cleanup-фиксы;
- усиливает cleanup Xerox family-lists даже без слов финишер/степлер;
- разворачивает сокращённые Xerox модели:
  B7125 / 30 / 35 -> B7125 / B7130 / B7135
  C7120 / 25 / 30 -> C7120 / C7125 / C7130;
- не ломает сложные Xerox family-цепочки глобальным slash-dedupe;
- сохраняет Canon ImagePROGRAF / imageRUNNER / imagePRESS glue-fixes;
- срезает мусорные хвосты типа &gt; и dangling '/ Canon' в конце совместимости.
"""

from __future__ import annotations

import re

from cs.util import norm_ws
from suppliers.alstyle.desc_clean import fix_common_broken_words


_CODE_SERIES_RE = re.compile(
    r"(?<![\w/])(?:(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,}(?:\s*/\s*(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,})+)"
)
_REPEATED_BRAND_RE = re.compile(
    r"(?iu)\b(Xerox|Canon|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark)\s+\1\b"
)

# Canon model patterns
_PIXMA_MODEL = r"[A-Za-z]*\d+[A-Za-z0-9-]*"
_IMAGEPROGRAF_MODEL = (
    r"(?:iPF\s+)?"
    r"(?:TX|GP|TM|TZ|PRO|PROGRAF|IPF)?"
    r"[\s-]*"
    r"[A-Za-z-]*\d+[A-Za-z0-9-]*"
    r"(?:\s+MFP\s+T\d{2}(?:-AiO)?)?"
)
_IMAGEPRESS_MODEL = r"[A-Za-z]*\d+[A-Za-z0-9-]*(?:\s+(?:I|II|III|IV|V|PRO))?"
_IR_ADV_MODEL = r"[A-Za-z]*\d+[A-Za-z0-9-]*(?:\s+(?:I|II|III|IV|V|PRO))?"
_IR_CLASSIC_MODEL = r"[A-Za-z]*\d+[A-Za-z0-9-]*(?:\s+(?:I|II|III|IV|V|PRO))?"
_ROMAN_ONLY = r"(?:I|II|III|IV|V)"

_HTML_ENTITY_GARBAGE_RE = re.compile(r"(?iu)(?:&gt;|&amp;gt;|&lt;|&amp;lt;|>)+\s*$")
_DANGLING_BRAND_TAIL_RE = re.compile(
    r"(?iu)(?:\s*/\s*|\s+)(Canon|Xerox|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark)\s*$"
)
_DANGLING_CONNECTOR_RE = re.compile(r"(?iu)(?:\s*/\s*|\s*,\s*|-+\s*)$")

_BRAND_GLUE_PATTERNS = [
    (re.compile(rf"(?i)(Canon\s+PIXMA\s+{_PIXMA_MODEL})(?=\s*Canon\s+PIXMA)"), r"\1 / "),
    (
        re.compile(rf"(?i)(Canon\s+ImagePROGRAF\s+{_IMAGEPROGRAF_MODEL})(?=\s*Canon\s+imagePROGRAF)"),
        r"\1 / ",
    ),
    (
        re.compile(rf"(?i)(Canon\s+ImagePROGRAF\s+{_IMAGEPROGRAF_MODEL})(?=\s*Canon\s+ImagePROGRAF)"),
        r"\1 / ",
    ),
    (
        re.compile(
            rf"(?i)(Canon\s+imageRUNNER\s+ADVANCE(?:\s+DX)?\s+{_IR_ADV_MODEL})(?=\s*Canon\s+imageRUNNER\s+ADVANCE)"
        ),
        r"\1 / ",
    ),
    (
        re.compile(
            rf"(?i)(Canon\s+imageRUNNER\s+{_IR_CLASSIC_MODEL})(?=\s*Canon\s+imageRUNNER\b)"
        ),
        r"\1 / ",
    ),
    (
        re.compile(
            rf"(?i)(Canon\s+imagePRESS(?:\s+Lite)?\s+{_IMAGEPRESS_MODEL})(?=\s*Canon\s+imagePRESS)"
        ),
        r"\1 / ",
    ),
    (
        re.compile(
            rf"(?i)(Canon\s+imagePRESS(?:\s+Lite)?\s+{_IMAGEPRESS_MODEL})(?=\s*Canon\s+imageRUNNER\s+ADVANCE)"
        ),
        r"\1 / ",
    ),
    (re.compile(rf"(?i)(Canon\s+i-SENSYS\s+{_PIXMA_MODEL})(?=\s*Canon\s+i-SENSYS)"), r"\1 / "),
    (re.compile(r"(?i)(Xerox\s+[A-Za-z-]*\d+[A-Za-z0-9/-]*)(?=\s*Xerox\s+)"), r"\1 / "),
    (re.compile(r"(?i)(WorkCentre\s+[A-Za-z-]*\d+[A-Za-z0-9/-]*)(?=\s*WorkCentre\s+)"), r"\1 / "),
]

_COMPAT_STOP_LABEL_RE = re.compile(
    r"(?iu)\b(?:"
    r"Цвет(?:\s+печати)?|"
    r"Ресурс(?:\s+картриджа| фотобарабана)?|"
    r"Количество\s+страниц|"
    r"Наличие\s+чипа|"
    r"Тип\s+чернил|"
    r"Количество\s+цветов|"
    r"Секция\s+аппарата|"
    r"Гарантированн(?:ый|ого)\s+об(?:ъ|ь)ем\s+отпечатков|"
    r"Форматы\s+бумаги|"
    r"Плотность|"
    r"Емкость|Ёмкость|"
    r"Скорость\s+печати|"
    r"Поддержка\s+двусторонней\s+печати|"
    r"Интерфейс|Процессор|Память|"
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
    r"что\s+идеально\s+подходит\s+для\s+небольших\s+офисов|"
    r"при\s+5%\s+заполнении|"
    r"только\s+для\s+продажи\s+на\s+территории|"
    r"для\s+быстрой\s+и\s+надежной\s+печати|"
    r"для\s+быстрой\s+и\s+над(?:е|ё)жной\s+печати|"
    r"позволяют\s+оптимизировать\s+рабочий\s+процесс"
    r")\b"
)
_LEADING_COMPAT_NOISE_RE = re.compile(
    r"(?iu)^(?:Комплект\s+поставки|Описание|Особенности|Преимущества)\s+"
)

_XEROX_ACCESSORY_PREFIX_RE = re.compile(
    r"(?iu)^.*?\bдля\s+(?=(?:Xerox\s+)?(?:VersaLink|AltaLink|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser)\b)"
)
_XEROX_FAMILY_HEAD_RE = re.compile(
    r"(?iu)^(?:Xerox\s+)?(VersaLink|AltaLink|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser)\s+(.+)$"
)
_XEROX_FAMILY_ANY_RE = re.compile(r"(?iu)\b(?:VersaLink|AltaLink|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser)\b")
_XEROX_DIGITAL_COPIER_RE = re.compile(r"(?iu)\bDigital\s+Copier\b")
_XEROX_COMMA_SPLIT_RE = re.compile(r"\s*,\s*")
_XEROX_SLASH_SPLIT_RE = re.compile(r"\s*/\s*")
_XEROX_SPACE_RE = re.compile(r"\s{2,}")
_XEROX_MODEL_FULL_RE = re.compile(r"(?iu)^([A-Z]+)?(\d{2,5})([A-Z]{0,4}(?:I|DNI|DN|DT)?)$")
_XEROX_MODEL_SHORT_RE = re.compile(r"(?iu)^(\d{2,4})([A-Z]{0,4}(?:I|DNI|DN|DT)?)$")


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
    s = norm_ws(v)
    if not s:
        return ""
    # Для сложных Xerox family-листов глобальный slash-dedupe разрушает структуру.
    if "," in s and _XEROX_FAMILY_ANY_RE.search(s):
        return s
    parts = [norm_ws(x) for x in re.split(r"\s*/\s*", s) if norm_ws(x)]
    if len(parts) < 2:
        return s
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        sig = p.casefold()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(p)
    return " / ".join(out)


def _strip_trailing_compat_garbage(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""

    prev = None
    while prev != s:
        prev = s
        s = _HTML_ENTITY_GARBAGE_RE.sub("", s).strip()
        s = _DANGLING_BRAND_TAIL_RE.sub("", s).strip()
        s = _DANGLING_CONNECTOR_RE.sub("", s).strip()

    return norm_ws(s.strip(" ;,.-"))


def split_glued_brand_models(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""

    for rx, repl in _BRAND_GLUE_PATTERNS:
        s = rx.sub(repl, s)

    s = re.sub(
        r"(?<=[A-Za-zА-Яа-я0-9])(?=(?:"
        r"Canon|CANON|Xerox|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark"
        r")\s+(?:"
        r"PIXMA|ImagePROGRAF|imagePROGRAF|imageRUNNER|imagePRESS|WorkCentre|WorkCenter|VersaLink|AltaLink|Phaser|ColorQube|CopyCentre|i-SENSYS|ECOSYS|LaserJet|DeskJet|OfficeJet"
        r")\b)",
        " / ",
        s,
    )

    s = re.sub(
        rf"(?iu)\b({_ROMAN_ONLY}|PRO)\s+(?=Canon\s+(?:imageRUNNER\s+ADVANCE|imageRUNNER|imagePRESS|ImagePROGRAF|imagePROGRAF|i-SENSYS)\b)",
        r"\1 / ",
        s,
    )

    return norm_ws(s)


def _canonize_brand_case(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = re.sub(r"(?iu)\bCANON\s+PIXMA\b", "Canon PIXMA", s)
    s = re.sub(r"(?iu)\bCanon\s+Pixma\b", "Canon PIXMA", s)
    s = re.sub(r"(?iu)\bCanon\s+imageprograf\b", "Canon ImagePROGRAF", s)
    s = re.sub(r"(?iu)\bCANON\s+IMAGEPROGRAF\b", "Canon ImagePROGRAF", s)
    s = re.sub(r"(?iu)\bCanon\s+imagerunner\b", "Canon imageRUNNER", s)
    s = re.sub(r"(?iu)\bCANON\s+IMAGERUNNER\b", "Canon imageRUNNER", s)
    s = re.sub(r"(?iu)\bCanon\s+imagepress\b", "Canon imagePRESS", s)
    s = re.sub(r"(?iu)\bCANON\s+IMAGEPRESS\b", "Canon imagePRESS", s)
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


def _prefix_missing_canon_brand(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""

    series_patterns = [
        rf"ImagePROGRAF\s+{_IMAGEPROGRAF_MODEL}",
        rf"imagePROGRAF\s+{_IMAGEPROGRAF_MODEL}",
        rf"imageRUNNER\s+ADVANCE(?:\s+DX)?\s+{_IR_ADV_MODEL}",
        rf"imageRUNNER\s+{_IR_CLASSIC_MODEL}",
        rf"imagePRESS(?:\s+Lite)?\s+{_IMAGEPRESS_MODEL}",
        rf"i-SENSYS\s+{_PIXMA_MODEL}",
    ]
    for pat in series_patterns:
        s = re.sub(rf"(?iu)\b({pat})\b", r"Canon \1", s)
        s = re.sub(r"(?iu)\bCanon\s+Canon\b", "Canon", s)

    return norm_ws(s)


def _fix_known_compat_typos(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""

    s = re.sub(
        rf"(?iu)\b(Canon\s+ImagePROGRAF\s+{_IMAGEPROGRAF_MODEL})(?=\s*Canon\s+imagePROGRAF\b)",
        r"\1 / ",
        s,
    )
    s = re.sub(
        rf"(?iu)\b(Canon\s+ImagePROGRAF\s+{_IMAGEPROGRAF_MODEL})(?=\s*Canon\s+ImagePROGRAF\b)",
        r"\1 / ",
        s,
    )
    s = re.sub(rf"(?iu)\b(Canon\s+ImagePROGRAF\s+{_IMAGEPROGRAF_MODEL})\s*Can\b", r"\1", s)
    s = re.sub(rf"(?iu)\b(Canon\s+ImagePROGRAF\s+{_IMAGEPROGRAF_MODEL})Canon\b", r"\1 / Canon", s)

    s = re.sub(
        rf"(?iu)\b(Canon\s+imageRUNNER\s+ADVANCE(?:\s+DX)?\s+{_IR_ADV_MODEL})(?=\s*Canon\s+imageRUNNER\s+ADVANCE)",
        r"\1 / ",
        s,
    )
    s = re.sub(
        rf"(?iu)\b(Canon\s+imageRUNNER\s+ADVANCE(?:\s+DX)?\s+{_IR_ADV_MODEL})Canon\b",
        r"\1 / Canon",
        s,
    )

    s = re.sub(
        rf"(?iu)\b(Canon\s+imageRUNNER\s+{_IR_CLASSIC_MODEL})(?=\s*Canon\s+imageRUNNER\b)",
        r"\1 / ",
        s,
    )
    s = re.sub(
        rf"(?iu)\b(Canon\s+imageRUNNER\s+{_IR_CLASSIC_MODEL})Canon\b",
        r"\1 / Canon",
        s,
    )

    s = re.sub(
        rf"(?iu)\b(Canon\s+imagePRESS(?:\s+Lite)?\s+{_IMAGEPRESS_MODEL})(?=\s*Canon\s+imagePRESS)",
        r"\1 / ",
        s,
    )
    s = re.sub(
        rf"(?iu)\b(Canon\s+imagePRESS(?:\s+Lite)?\s+{_IMAGEPRESS_MODEL})(?=\s*Canon\s+imageRUNNER\s+ADVANCE)",
        r"\1 / ",
        s,
    )
    s = re.sub(
        rf"(?iu)\b(Canon\s+imagePRESS(?:\s+Lite)?\s+{_IMAGEPRESS_MODEL})Canon\b",
        r"\1 / Canon",
        s,
    )

    s = re.sub(
        rf"(?iu)\b(Canon\s+i-SENSYS\s+{_PIXMA_MODEL})(?=\s*Canon\s+i-SENSYS)",
        r"\1 / ",
        s,
    )

    s = re.sub(r"(?iu)\bCopyCentre\s+245\s*/\s*25\b", "CopyCentre 245 / 255", s)

    return norm_ws(s)


def _normalize_xerox_family_name(family: str) -> str:
    fam = norm_ws(family)
    fam = re.sub(r"(?iu)^WorkCenter\b", "WorkCentre", fam)
    fam = re.sub(r"(?iu)\s+", " ", fam)
    return fam


def _expand_xerox_short_model(prev_model: str, token: str) -> str:
    prev = norm_ws(prev_model).upper()
    cur = norm_ws(token).upper()
    if not prev or not cur:
        return cur

    m_prev = _XEROX_MODEL_FULL_RE.fullmatch(prev)
    m_cur_short = _XEROX_MODEL_SHORT_RE.fullmatch(cur)
    if not m_prev or not m_cur_short:
        return cur

    pref_prev = norm_ws(m_prev.group(1) or "").upper()
    digits_prev = norm_ws(m_prev.group(2) or "")
    suffix_prev = norm_ws(m_prev.group(3) or "").upper()

    digits_cur = norm_ws(m_cur_short.group(1) or "")
    suffix_cur = norm_ws(m_cur_short.group(2) or "").upper()

    # Уже полная модель той же длины — просто добавим возможный префикс по текущему family-паттерну.
    if len(digits_cur) >= len(digits_prev):
        return f"{pref_prev}{digits_cur}{suffix_cur}" if pref_prev and not re.match(r"^[A-Z]+", cur) else cur

    # Для B7125 / 30 / 35 и C7120 / 25 / 30 меняем только хвост цифр.
    merged_digits = digits_prev[:-len(digits_cur)] + digits_cur
    merged_suffix = suffix_cur or suffix_prev
    return f"{pref_prev}{merged_digits}{merged_suffix}" if pref_prev else f"{merged_digits}{merged_suffix}"


def _extract_xerox_models(body: str) -> list[str]:
    s = norm_ws(_XEROX_DIGITAL_COPIER_RE.sub("", body or ""))
    if not s:
        return []

    raw_parts = [norm_ws(x) for x in _XEROX_SLASH_SPLIT_RE.split(s) if norm_ws(x)]
    if not raw_parts:
        return []

    out: list[str] = []
    seen: set[str] = set()
    prev_model = ""

    for raw in raw_parts:
        p = norm_ws(raw).strip(" ;,.-")
        if not p:
            continue

        m_full = _XEROX_MODEL_FULL_RE.fullmatch(p.upper())
        if m_full:
            pref = norm_ws(m_full.group(1) or "").upper()
            digits = norm_ws(m_full.group(2) or "")
            suffix = norm_ws(m_full.group(3) or "").upper()
            model = f"{pref}{digits}{suffix}" if pref else f"{digits}{suffix}"
        else:
            model = _expand_xerox_short_model(prev_model, p)

        sig = model.casefold()
        if sig in seen:
            prev_model = model
            continue
        seen.add(sig)
        out.append(model)
        prev_model = model

    return out


def _looks_like_xerox_family_list(v: str) -> bool:
    s = norm_ws(v)
    if not s or not _XEROX_FAMILY_ANY_RE.search(s):
        return False
    if re.search(r"(?iu)\b(?:финишер|буклетмейкер|степлер)\b", s):
        return True
    if re.search(r"(?iu)\b(?:VersaLink|AltaLink|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser)\b.*?,", s):
        return True
    if re.search(r"(?iu)/\s*\d{2,4}(?:[A-Z]{0,4}(?:I|DNI|DN|DT)?)?\b", s):
        return True
    if len(re.findall(r"(?iu)\b(?:VersaLink|AltaLink|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser)\b", s)) >= 2:
        return True
    return False


def _cleanup_xerox_finisher_compat(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""

    s = _XEROX_ACCESSORY_PREFIX_RE.sub("", s).strip()
    s = re.sub(r"(?iu)^для\s+", "", s).strip()

    raw_chunks = [norm_ws(x) for x in _XEROX_COMMA_SPLIT_RE.split(s) if norm_ws(x)]
    if not raw_chunks:
        return s

    family_to_models: dict[str, list[str]] = {}
    family_seen: dict[str, set[str]] = {}
    family_order: list[str] = []
    current_family = ""

    for chunk in raw_chunks:
        part = norm_ws(chunk).strip(" ;,.-")
        if not part:
            continue

        m = _XEROX_FAMILY_HEAD_RE.match(part)
        if m:
            current_family = _normalize_xerox_family_name(m.group(1))
            body = norm_ws(m.group(2))
        elif current_family:
            body = part
        else:
            continue

        if current_family not in family_to_models:
            family_to_models[current_family] = []
            family_seen[current_family] = set()
            family_order.append(current_family)

        models = _extract_xerox_models(body)
        if not models:
            continue

        for model in models:
            sig = model.casefold()
            if sig in family_seen[current_family]:
                continue
            family_seen[current_family].add(sig)
            family_to_models[current_family].append(model)

    out_chunks: list[str] = []
    for family in family_order:
        models = family_to_models.get(family) or []
        if not models:
            continue
        out_chunks.append(f"Xerox {family} " + " / ".join(models))

    if not out_chunks:
        return s

    return ", ".join(out_chunks)


def _trim_compat_noise_tail(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""

    while True:
        nxt = _LEADING_COMPAT_NOISE_RE.sub("", s).strip()
        if nxt == s:
            break
        s = nxt

    cut_positions: list[int] = []

    m = _COMPAT_STOP_LABEL_RE.search(s)
    if m and m.start() >= 8:
        cut_positions.append(m.start())

    m = _COMPAT_NOISE_PHRASE_RE.search(s)
    if m and m.start() >= 8:
        cut_positions.append(m.start())

    if cut_positions:
        s = s[: min(cut_positions)]

    s = _strip_trailing_compat_garbage(s)
    return norm_ws(s.strip(" ;,.-"))


def clean_compatibility_text(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""

    s = re.sub(r"(?iu)^Модель\s+[A-Z0-9-]+\s+", "", s)
    s = re.sub(r"(?iu)^Совместимые\s+модели\s+", "", s)
    s = re.sub(r"(?iu)^Устройства\s+", "", s)
    s = re.sub(r"(?iu)^Устройство\s+", "", s)

    s = fix_common_broken_words(s)
    s = _canonize_brand_case(s)
    s = split_glued_brand_models(s)
    s = _prefix_missing_canon_brand(s)
    s = _dedupe_repeated_brand_prefixes(s)
    s = _fix_known_compat_typos(s)

    s = re.sub(r"(?iu)\bXerox\s+Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)\bXerox\s+Для\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)\bДля,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)\bДля\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)\bДля\s+принтеров\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)\bДля\s+МФУ\s+Xerox\s+", "Xerox ", s)

    s = split_glued_brand_models(s)
    s = _canonize_brand_case(s)
    s = _prefix_missing_canon_brand(s)
    s = _dedupe_repeated_brand_prefixes(s)
    s = _fix_known_compat_typos(s)

    if _looks_like_xerox_family_list(s):
        s = _cleanup_xerox_finisher_compat(s)

    s = _trim_compat_noise_tail(s)

    s = re.sub(r"\s*,\s*/\s*", " / ", s)
    s = re.sub(r"\s*/\s*,\s*", " / ", s)
    s = _XEROX_SPACE_RE.sub(" ", s)

    s = dedupe_slash_tail_models(s)
    s = _dedupe_repeated_brand_prefixes(s)
    s = _trim_compat_noise_tail(s)
    s = _strip_trailing_compat_garbage(s)
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
