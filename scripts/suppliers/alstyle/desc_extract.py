# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/desc_extract.py

AlStyle description -> params extraction.

Фикс v108:
- ужесточена валидация `Совместимость`
- больше не пропускаем ОС/интерфейсы/порт-листы как compatibility
- режем слишком длинные и техлистовые значения
"""

from __future__ import annotations

import re
from typing import Any

from cs.util import norm_ws
from suppliers.alstyle.desc_clean import clean_desc_text_for_extraction
from suppliers.alstyle.params_xml import (
    key_quality_ok,
    apply_value_normalizers,
)
from suppliers.alstyle.compat import (
    clean_compatibility_text,
    dedupe_code_series_text,
    split_glued_brand_models,
)

_DESC_SPEC_START_RE = re.compile(
    r"(?im)^\s*(Характеристики|Основные характеристики|Технические характеристики)\s*:?\s*$"
)
_DESC_SPEC_STOP_RE = re.compile(
    r"(?im)^\s*(Преимущества|Комплектация|Условия гарантии|Гарантия|Примечание|Примечания|Особенности|Описание|EUROPRINT)\s*:?\s*$"
)
_DESC_SPEC_LINE_RE = re.compile(
    r"(?im)^\s*"
    r"(Модель|Аналог модели|Совместимость|Совместимые модели|Устройства|Для принтеров|"
    r"Технология печати|Цвет|Цвет печати|Ресурс|Ресурс картриджа|Ресурс картриджа, cтр\.|"
    r"Количество страниц|Кол-во страниц при 5% заполнении А4|Емкость|Ёмкость|Емкость лотка|Ёмкость лотка|"
    r"Степлирование|Дополнительные опции|Применение|Количество в упаковке|Колличество в упаковке)"
    r"\s*(?::|\t+|\s{2,}|[-–—])\s*(.+?)\s*$"
)
_DESC_COMPAT_LINE_RE = re.compile(r"(?im)^\s*Совместим(?:а|о|ы)?\s+с\s+(.+?)\s*$")
_DESC_COMPAT_SENTENCE_RE = re.compile(
    r"(?is)\bСовместим(?:а|о|ы)?\s+с\s+(.{6,220}?)(?:(?:[.!?](?:\s|$))|\n|$)"
)
_DESC_FOR_DEVICES_SENTENCE_RE = re.compile(
    r"(?is)\bдля\s+(?:устройств|принтеров(?:\s+и\s+МФУ)?|МФУ|аппаратов)\s+(.{6,220}?)(?:(?:[.!?](?:\s|$))|\n|$)"
)
_DESC_TECH_PRINT_LABEL_ONLY_RE = re.compile(r"(?im)^\s*Технология\s+печати\s*:?\s*$")
_DESC_CAPACITY_SENTENCE_RE = re.compile(
    r"(?is)\b(?:Емкость|Ёмкость)\s+лотка\s*[-:]\s*(.{2,120}?)(?:(?:[.!?](?:\s|$))|\n|$)"
)

_COMPAT_BRAND_HINT_RE = re.compile(
    r"(?i)\b(Xerox|Canon|HP|Hewlett|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki|"
    r"VersaLink|AltaLink|WorkCentre|WorkCenter|DocuCentre|imageRUNNER|i-SENSYS|ECOSYS|bizhub|PIXMA)\b"
)
_COMPAT_MODEL_TOKEN_RE = re.compile(
    r"(?i)\b(?:[A-Z]{1,8}-?\d{2,5}[A-Z]{0,3}x?|[A-Z]?\d{3,5}[A-Z]{0,3}i?)\b"
)
_COMPAT_REJECT_RE = re.compile(
    r"(?iu)\b("
    r"Windows|Android|Mac\s*OS|Linux|Chrome|USB(?:-C| Type-C)?|HDMI|VGA|RJ45|RS232|OTG|TF\s*Card|"
    r"Line\s*Out|SPDIF|OPS(?:-slot| Slot)?|Wi-?Fi|Bluetooth|RAM|ROM|процессор|Cortex|дисплей|панель|"
    r"яркость|контрастность|угол\s+обзора|время\s+отклика|точность|позиционирования|аудио|динамики|"
    r"микрофоны|звуковое\s+давление|интерфейс(?:ы)?|подключение|передняя\s+панель|задняя\s+панель|"
    r"touch\s*out|usb\s*touch|hdmi\s+in|hdmi\s+out|dp\s+in|type-c|ops\s+slot|single\s+touch"
    r")\b"
)

_DESC_SPEC_KEY_MAP = {
    "модель": "Модель",
    "аналог модели": "Аналог модели",
    "совместимость": "Совместимость",
    "совместимые модели": "Совместимость",
    "устройства": "Совместимость",
    "для принтеров": "Совместимость",
    "цвет": "Цвет",
    "цвет печати": "Цвет",
    "ресурс": "Ресурс",
    "ресурс картриджа": "Ресурс",
    "ресурс картриджа, cтр.": "Ресурс",
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


def canon_desc_spec_key(k: str) -> str:
    kk = norm_ws(k).casefold()
    return _DESC_SPEC_KEY_MAP.get(kk, norm_ws(k))


def _compat_model_token_count(v: str) -> int:
    return len(_COMPAT_MODEL_TOKEN_RE.findall(v or ""))


def looks_like_compatibility_value(val: str) -> bool:
    v = norm_ws(val)
    if not v or len(v) < 6:
        return False
    if len(v) > 160:
        return False
    if len(v.split()) > 18:
        return False
    if v.count(":") > 1:
        return False
    if _COMPAT_REJECT_RE.search(v):
        return False

    has_brand = bool(_COMPAT_BRAND_HINT_RE.search(v))
    model_count = _compat_model_token_count(v)

    if has_brand and model_count >= 1:
        return True
    if model_count >= 2 and ("/" in v or "," in v):
        return True
    return False


def iter_desc_lines(block: str) -> list[str]:
    lines = []
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
        r"(?iu)(Интерфейсы\s*/\s*разъ[её]мы\s*/\s*управление|Аксессуары|Порты\s+и\s+подключение|Задняя\s+панель|Передняя\s+панель):?",
        ln,
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
        r"Модель|Аналог модели|Совместимость|Совместимые модели|Устройства|Для принтеров|"
        r"Цвет|Цвет печати|Ресурс|Ресурс картриджа(?:,\s*cтр\.)?|Количество страниц|"
        r"[ЕеЁё]мкость(?: лотка)?|Степлирование|Дополнительные опции|Применение|Количество в упаковке"
    )
    rx = re.compile(rf"(?iu)(?=\b(?:{key_pat})\b\s*(?::|[-–—]))")
    parts = [norm_ws(x) for x in rx.split(ln) if norm_ws(x)]
    return parts if len(parts) > 1 else [ln]


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
    return out


def extract_short_inline_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    lines = [norm_ws(x) for x in text.splitlines() if norm_ws(x)]
    if len(lines) > 8:
        lines = lines[:8]

    for ln in lines:
        parts = split_inline_desc_pairs(ln)
        for part in parts:
            pair = parse_desc_spec_line(part)
            if pair:
                out.append(pair)
    return out


def extract_sentence_compat_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    for rx in (_DESC_COMPAT_SENTENCE_RE, _DESC_FOR_DEVICES_SENTENCE_RE):
        for m in rx.finditer(text):
            cand = norm_ws(m.group(1))
            if not cand:
                continue
            cand = re.split(
                r"(?i)\b(Преимущества|Комплектация|Условия гарантии|Примечание|Примечания|Особенности|Описание)\b",
                cand,
                maxsplit=1,
            )[0].strip(" ;,.-")
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
        m = re.search(r"(?iu)\bСовместим\s+с\s+(.+?)\s*$", ln)
        if m:
            cand = norm_ws(m.group(1))
            if looks_like_compatibility_value(cand):
                out.append(("Совместимость", cand))
        m = re.search(r"(?iu)\b(?:Емкость|Ёмкость)\s+лотка\s*[-:]\s*(.+?)\s*$", ln)
        if m:
            cand = norm_ws(m.group(1))
            if cand:
                out.append(("Ёмкость", cand))

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
        val2 = clean_compatibility_text(val2)
        if not looks_like_compatibility_value(val2):
            return None
    elif key in {"Модель", "Аналог модели"}:
        val2 = dedupe_code_series_text(split_glued_brand_models(val2))

    if not val2:
        return None
    return (key, val2)


def extract_desc_spec_pairs(desc_src: str, schema: dict[str, Any]) -> list[tuple[str, str]]:
    text = clean_desc_text_for_extraction(desc_src)
    if not text.strip():
        return []

    candidates: list[tuple[str, str]] = []

    strict = extract_strict_kv_block(text)
    if strict:
        candidates.extend(strict)
    else:
        candidates.extend(extract_short_inline_pairs(text))

    candidates.extend(extract_sentence_compat_pairs(text))
    candidates.extend(extract_sentence_capacity_pairs(text))

    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for key, val in candidates:
        checked = validate_desc_pair(key, val, schema)
        if not checked:
            continue
        sig = (checked[0].casefold(), checked[1].casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append(checked)

    return out
