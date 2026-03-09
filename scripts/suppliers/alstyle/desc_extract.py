# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/desc_extract.py

AlStyle description -> params extraction.

Фикс v112:
- возвращает длинную Совместимость для кейсов типа AS113735;
- умеет поднимать multiline label/value блоки, включая:
  Совместимость -> Устройства -> ...
  Ресурс картриджа, cтр. -> 220
- не возвращает назад техлистовые "Совместимость" у интерактивных панелей.
"""

from __future__ import annotations

import re
from typing import Any

from cs.util import norm_ws
from suppliers.alstyle.desc_clean import clean_desc_text_for_extraction
from suppliers.alstyle.params_xml import key_quality_ok, apply_value_normalizers
from suppliers.alstyle.compat import (
    clean_compatibility_text,
    dedupe_code_series_text,
    split_glued_brand_models,
)

_DESC_SPEC_START_RE = re.compile(
    r"^\s*(Характеристики|Основные характеристики|Технические характеристики)\s*:?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_DESC_SPEC_STOP_RE = re.compile(
    r"^\s*(Преимущества|Комплектация|Условия гарантии|Гарантия|Примечание|Примечания|Особенности|Описание|EUROPRINT)\s*:?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_DESC_SPEC_LINE_RE = re.compile(
    r"^\s*"
    r"(Модель|Аналог модели|Совместимость|Совместимые модели|Устройства|Для принтеров|"
    r"Технология печати|Цвет|Цвет печати|Ресурс|Ресурс картриджа|Ресурс картриджа, cтр\.|Ресурс картриджа, стр\.|"
    r"Количество страниц|Кол-во страниц при 5% заполнении А4|Емкость|Ёмкость|Емкость лотка|Ёмкость лотка|"
    r"Степлирование|Дополнительные опции|Применение|Количество в упаковке|Колличество в упаковке|"
    r"Производитель|Устройство|Объем картриджа, мл|Объём картриджа, мл)"
    r"\s*(?::|\t+|\s{2,}|[-–—])\s*(.+?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_DESC_COMPAT_LINE_RE = re.compile(
    r"^\s*Совместим(?:а|о|ы)?\s+с\s+(.+?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_DESC_COMPAT_SENTENCE_RE = re.compile(
    r"\bСовместим(?:а|о|ы)?\s+с\s+(.{6,420}?)(?:(?:[.!?](?:\s|$))|\n|$)",
    re.IGNORECASE | re.DOTALL,
)
_DESC_FOR_DEVICES_SENTENCE_RE = re.compile(
    r"\bдля\s+(?:устройств|принтеров(?:\s+и\s+МФУ)?|МФУ|аппаратов)\s+(.{6,420}?)(?:(?:[.!?](?:\s|$))|\n|$)",
    re.IGNORECASE | re.DOTALL,
)
_DESC_TECH_PRINT_LABEL_ONLY_RE = re.compile(
    r"^\s*Технология\s+печати\s*:?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_DESC_CAPACITY_SENTENCE_RE = re.compile(
    r"\b(?:Емкость|Ёмкость)\s+лотка\s*[-:]\s*(.{2,120}?)(?:(?:[.!?](?:\s|$))|\n|$)",
    re.IGNORECASE | re.DOTALL,
)

_RESOURCE_INLINE_RE = re.compile(
    r"(?iu)\b(?:Ресурс\s+картриджа(?:,\s*[cс]тр\.)?|Ресурс|Количество\s+страниц|Кол-во\s+страниц\s+при\s+5%\s+заполнении\s+А4)\b"
    r"\s*(?::|[-–—])?\s*"
    r"(.{1,120})$"
)
_RESOURCE_VALUE_RE = re.compile(
    r"(?iu)\b\d[\d\s.,]*\s*(?:стандартн(?:ых|ые)?\s+страниц(?:ы)?(?:\s+в\s+среднем)?|стр\.?|страниц|copies|pages)\b"
)
_RESOURCE_NUMBER_ONLY_RE = re.compile(r"(?iu)^\d[\d\s.,]*\s*(?:стр\.?|страниц)?$")

_COMPAT_BRAND_HINT_RE = re.compile(
    r"\b(Xerox|Canon|HP|Hewlett|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki|"
    r"VersaLink|AltaLink|WorkCentre|WorkCenter|DocuCentre|imageRUNNER|i-SENSYS|ECOSYS|bizhub|PIXMA|Phaser|ColorQube|CopyCentre)\b",
    re.IGNORECASE,
)
_COMPAT_MODEL_TOKEN_RE = re.compile(
    r"\b(?:[A-Z]{1,8}-?\d{2,5}[A-Z]{0,3}x?|[A-Z]?\d{3,5}[A-Z]{0,3}i?)\b",
    re.IGNORECASE,
)
_COMPAT_REJECT_RE = re.compile(
    r"\b("
    r"Windows|Android|Mac\s*OS|Linux|Chrome|USB(?:-C| Type-C)?|HDMI|VGA|RJ45|RS232|OTG|TF\s*Card|"
    r"Line\s*Out|SPDIF|OPS(?:-slot| Slot)?|Wi-?Fi|Bluetooth|RAM|ROM|процессор|Cortex|дисплей|панель|"
    r"яркость|контрастность|угол\s+обзора|время\s+отклика|точность|позиционирования|аудио|динамики|"
    r"микрофоны|звуковое\s+давление|интерфейс(?:ы)?|подключение|передняя\s+панель|задняя\s+панель|"
    r"touch\s*out|usb\s*touch|hdmi\s+in|hdmi\s+out|dp\s+in|type-c|ops\s+slot|single\s+touch"
    r")\b",
    re.IGNORECASE,
)

_DESC_SPEC_KEY_MAP = {
    "модель": "Модель",
    "аналог модели": "Аналог модели",
    "совместимость": "Совместимость",
    "совместимые модели": "Совместимость",
    "устройства": "Совместимость",
    "устройство": "Совместимость",
    "для принтеров": "Совместимость",
    "цвет": "Цвет",
    "цвет печати": "Цвет",
    "ресурс": "Ресурс",
    "ресурс картриджа": "Ресурс",
    "ресурс картриджа, cтр.": "Ресурс",
    "ресурс картриджа, стр.": "Ресурс",
    "количество страниц": "Ресурс",
    "кол-во страниц при 5% заполнении а4": "Ресурс",
    "емкость": "Ёмкость",
    "ёмкость": "Ёмкость",
    "емкость лотка": "Ёмкость",
    "ёмкость лотка": "Ёмкость",
    "степлирование": "Степлирование",
    "дополнительные опции": "Дополнительные опции",
    "применение": "Применение",
    "количество в упаковке": "Количество в упаковке",
    "колличество в упаковке": "Количество в упаковке",
    "технология печати": "Технология",
}

_SAFE_DESC_PARAM_KEYS = {
    "Модель",
    "Аналог модели",
    "Совместимость",
    "Технология",
    "Цвет",
    "Ресурс",
    "Ёмкость",
    "Степлирование",
    "Дополнительные опции",
    "Применение",
    "Количество в упаковке",
}

_COMPACT_LABELS = [
    "Модель", "Аналог модели", "Совместимость", "Совместимые модели", "Устройства", "Устройство",
    "Для принтеров", "Производитель", "Технология печати", "Цвет печати", "Цвет",
    "Ресурс картриджа, cтр.", "Ресурс картриджа, стр.", "Ресурс картриджа", "Количество страниц", "Ресурс",
    "Емкость лотка", "Ёмкость лотка", "Емкость", "Ёмкость", "Объем картриджа, мл", "Объём картриджа, мл",
    "Степлирование", "Дополнительные опции", "Применение", "Количество в упаковке", "Колличество в упаковке",
]
_COMPACT_LABEL_RE = re.compile(
    r"\b(?:Характеристики|Основные характеристики|Технические характеристики)\b\s*:?\s*|"
    r"\b(" + "|".join(re.escape(x) for x in sorted(_COMPACT_LABELS, key=len, reverse=True)) + r")\b(?:\s*[:\-–—]\s*|\s+)",
    re.IGNORECASE,
)
_LABEL_ONLY_RE = re.compile(
    r"^(?:"
    + "|".join(re.escape(x) for x in sorted(_COMPACT_LABELS + ["Характеристики", "Основные характеристики", "Технические характеристики"], key=len, reverse=True))
    + r")\s*:?$",
    re.IGNORECASE,
)


def canon_desc_spec_key(k: str) -> str:
    kk = norm_ws(k).casefold()
    return _DESC_SPEC_KEY_MAP.get(kk, norm_ws(k))


def _compat_model_token_count(v: str) -> int:
    return len(_COMPAT_MODEL_TOKEN_RE.findall(v or ""))


def _normalize_compat_candidate(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = clean_compatibility_text(s)
    s = re.sub(r"(?iu)\bXerox\s+Для\s+Xerox\b", "Xerox", s)
    s = re.sub(r"(?iu)\bДля\s+Xerox\b", "Xerox", s)
    s = re.sub(r"(?iu)\bXerox\s+Для\b", "Xerox", s)
    s = re.sub(r"\s{2,}", " ", s)
    return norm_ws(s.strip(" ;,.-"))


def looks_like_compatibility_value(val: str) -> bool:
    v = _normalize_compat_candidate(val)
    if not v or len(v) < 6:
        return False
    if _COMPAT_REJECT_RE.search(v):
        return False

    has_brand = bool(_COMPAT_BRAND_HINT_RE.search(v))
    model_count = _compat_model_token_count(v)
    word_count = len(v.split())

    # обычные кейсы
    if len(v) <= 520 and word_count <= 90:
        if has_brand and model_count >= 1:
            return True
        if model_count >= 2 and ("/" in v or "," in v):
            return True

    # длинные compat для степлерных/буклетных картриджей и больших Xerox/Canon списков
    if len(v) <= 1800 and word_count <= 260:
        if has_brand and model_count >= 8:
            return True

    return False


def looks_like_resource_value(val: str) -> bool:
    v = norm_ws(val)
    if not v:
        return False
    if len(v) > 140:
        return False
    low = v.casefold()
    if "максимальное количество отпечатков" in low:
        return False
    if "зависит от" in low:
        return False
    if "можно произвести" in low:
        return False
    if _RESOURCE_VALUE_RE.search(v):
        return True
    if _RESOURCE_NUMBER_ONLY_RE.match(v):
        return True
    return False


def iter_desc_lines(block: str) -> list[str]:
    lines: list[str] = []
    for raw in (block or "").splitlines():
        ln = norm_ws(raw)
        if ln:
            lines.append(ln)
    return lines


def parse_desc_spec_line(raw: str) -> tuple[str, str] | None:
    ln = norm_ws(raw)
    if not ln:
        return None
    if re.fullmatch(
        r"(Интерфейсы\s*/\s*разъ[её]мы\s*/\s*управление|Аксессуары|Порты\s+и\s+подключение|Задняя\s+панель|Передняя\s+панель):?",
        ln,
        flags=re.IGNORECASE,
    ):
        return None

    m = _DESC_SPEC_LINE_RE.match(raw)
    if not m:
        compact = re.sub(r"\t+", "  ", raw)
        compact = re.sub(r"\s{3,}", "  ", compact)
        m = _DESC_SPEC_LINE_RE.match(compact)
    if m:
        return (canon_desc_spec_key(m.group(1)), norm_ws(m.group(2)))

    m = _DESC_COMPAT_LINE_RE.match(raw)
    if m:
        return ("Совместимость", norm_ws(m.group(1)))

    if _DESC_TECH_PRINT_LABEL_ONLY_RE.match(ln):
        return None

    return None


def split_inline_desc_pairs(line: str) -> list[str]:
    ln = norm_ws(line)
    if not ln:
        return []
    key_pat = (
        r"Модель|Аналог модели|Совместимость|Совместимые модели|Устройства|Устройство|Для принтеров|"
        r"Производитель|Цвет|Цвет печати|Ресурс|Ресурс картриджа(?:,\s*cтр\.)?|Количество страниц|"
        r"[ЕеЁё]мкость(?: лотка)?|Об[ъе]ем картриджа,\s*мл|Степлирование|Дополнительные опции|"
        r"Применение|Количество в упаковке|Колличество в упаковке"
    )
    rx = re.compile(rf"(?iu)(?=\b(?:{key_pat})\b\s*(?::|[-–—]))")
    parts = [norm_ws(x) for x in rx.split(ln) if norm_ws(x)]
    return parts if len(parts) > 1 else [ln]


def extract_compact_labeled_sequences(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for line in text.splitlines():
        ln = norm_ws(line)
        if not ln or len(ln) < 20 or len(ln) > 1600:
            continue
        matches = list(_COMPACT_LABEL_RE.finditer(ln))
        real_labels = [m for m in matches if m.group(1)]
        if len(real_labels) < 2:
            continue
        for i, m in enumerate(real_labels):
            label = m.group(1)
            start = m.end()
            end = real_labels[i + 1].start() if i + 1 < len(real_labels) else len(ln)
            value = norm_ws(ln[start:end]).strip(" ;,.-")
            if not value:
                continue
            out.append((canon_desc_spec_key(label), value))
    return out


def extract_multiline_label_value_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    lines = iter_desc_lines(text)
    i = 0
    while i < len(lines):
        line = lines[i]
        if not _LABEL_ONLY_RE.match(line):
            i += 1
            continue

        label_raw = re.sub(r"\s*:\s*$", "", line)
        label = canon_desc_spec_key(label_raw)

        # служебный старт блока
        if label_raw.casefold() in {"характеристики", "основные характеристики", "технические характеристики"}:
            i += 1
            continue

        j = i + 1

        # частный кейс: "Совместимость" -> "Устройства" -> значения
        if label == "Совместимость" and j < len(lines):
            next_cf = lines[j].casefold().rstrip(":")
            if next_cf in {"устройства", "устройство", "совместимые модели", "для принтеров"}:
                j += 1

        value_parts: list[str] = []
        while j < len(lines):
            nxt = lines[j]
            if _LABEL_ONLY_RE.match(nxt):
                break
            value_parts.append(nxt)
            j += 1

        value = norm_ws(" ".join(value_parts)).strip(" ;,.-")
        if value:
            if label == "Ресурс" and re.fullmatch(r"\d[\d\s.,]*", value):
                value = f"{value} стр."
            out.append((label, value))

        i = max(j, i + 1)

    return out


def extract_strict_kv_block(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    m = _DESC_SPEC_START_RE.search(text)
    if not m:
        return out

    block = text[m.end():]
    stop = _DESC_SPEC_STOP_RE.search(block)
    if stop:
        block = block[:stop.start()]

    block_lines = iter_desc_lines(block)
    for ln in block_lines:
        pair = parse_desc_spec_line(ln)
        if pair:
            out.append(pair)

    # multiline fallback inside strict block
    out.extend(extract_multiline_label_value_pairs(block))
    return out


def extract_short_inline_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    lines = [norm_ws(x) for x in text.splitlines() if norm_ws(x)]
    if len(lines) > 12:
        lines = lines[:12]

    for ln in lines:
        parts = split_inline_desc_pairs(ln)
        for part in parts:
            pair = parse_desc_spec_line(part)
            if pair:
                out.append(pair)

    out.extend(extract_compact_labeled_sequences("\n".join(lines)))
    out.extend(extract_multiline_label_value_pairs("\n".join(lines)))
    return out


def extract_sentence_compat_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    for rx in (_DESC_COMPAT_SENTENCE_RE, _DESC_FOR_DEVICES_SENTENCE_RE):
        for m in rx.finditer(text):
            cand = norm_ws(m.group(1))
            if not cand:
                continue
            cand = re.split(
                r"\b(Преимущества|Комплектация|Условия гарантии|Примечание|Примечания|Особенности|Описание)\b",
                cand,
                maxsplit=1,
                flags=re.IGNORECASE,
            )[0].strip(" ;,.-")
            cand = _normalize_compat_candidate(cand)
            if not looks_like_compatibility_value(cand):
                continue
            out.append(("Совместимость", cand))

    return out


def extract_sentence_capacity_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    for m in _DESC_CAPACITY_SENTENCE_RE.finditer(text):
        cand = norm_ws(m.group(1)).strip(" ;,.-")
        if not cand:
            continue
        out.append(("Ёмкость", cand))

    for line in text.splitlines():
        ln = norm_ws(line)
        if not ln:
            continue
        m = re.search(r"\bСовместим\s+с\s+(.+?)\s*$", ln, flags=re.IGNORECASE)
        if m:
            cand = _normalize_compat_candidate(m.group(1))
            if looks_like_compatibility_value(cand):
                out.append(("Совместимость", cand))
        m = re.search(r"\b(?:Емкость|Ёмкость)\s+лотка\s*[-:]\s*(.+?)\s*$", ln, flags=re.IGNORECASE)
        if m:
            cand = norm_ws(m.group(1))
            if cand:
                out.append(("Ёмкость", cand))

    return out


def extract_resource_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for line in text.splitlines():
        ln = norm_ws(line)
        if not ln:
            continue
        m = _RESOURCE_INLINE_RE.search(ln)
        if not m:
            continue
        cand = norm_ws(m.group(1)).strip(" ;,.-")
        if looks_like_resource_value(cand):
            out.append(("Ресурс", cand))
    return out


def validate_desc_pair(key: str, val: str, schema: dict[str, Any]) -> tuple[str, str] | None:
    if not key or not val:
        return None

    drop = {str(x).casefold() for x in (schema.get("drop_keys_casefold") or [])}
    rules = schema.get("key_rules") or {}
    require_letter = bool(rules.get("require_letter", True))
    max_len = int(rules.get("max_len", 60))
    max_words = int(rules.get("max_words", 9))

    if key.casefold() in drop or key.casefold() in ("код нкт",):
        return None
    if key not in _SAFE_DESC_PARAM_KEYS:
        return None
    if not key_quality_ok(key, require_letter=require_letter, max_len=max_len, max_words=max_words):
        return None

    val2 = apply_value_normalizers(key, val, schema)
    if not val2:
        return None

    if key == "Совместимость":
        val2 = _normalize_compat_candidate(val2)
        if not looks_like_compatibility_value(val2):
            return None
    elif key in {"Модель", "Аналог модели"}:
        val2 = dedupe_code_series_text(split_glued_brand_models(val2))
    elif key == "Ресурс":
        if not looks_like_resource_value(val2):
            return None
        # канонизируем короткое значение
        m = _RESOURCE_VALUE_RE.search(val2)
        if m:
            val2 = norm_ws(m.group(0))

    if not val2:
        return None
    return (key, val2)


def extract_desc_spec_pairs(desc_src: str, schema: dict[str, Any]) -> list[tuple[str, str]]:
    text = clean_desc_text_for_extraction(desc_src)
    if not text.strip():
        return []

    candidates: list[tuple[str, str]] = []
    candidates.extend(extract_resource_pairs(text))

    strict = extract_strict_kv_block(text)
    if strict:
        candidates.extend(strict)

    candidates.extend(extract_short_inline_pairs(text))
    candidates.extend(extract_sentence_compat_pairs(text))
    candidates.extend(extract_sentence_capacity_pairs(text))

    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    best_resource: tuple[str, str] | None = None

    for key, val in candidates:
        checked = validate_desc_pair(key, val, schema)
        if not checked:
            continue

        if checked[0] == "Ресурс":
            if best_resource is None or len(checked[1]) < len(best_resource[1]):
                best_resource = checked
            continue

        sig = (checked[0].casefold(), checked[1].casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append(checked)

    if best_resource is not None:
        out = [x for x in out if x[0] != "Ресурс"]
        out.append(best_resource)

    return out
