# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/desc_extract.py

AkCent description extract layer.

Что делает:
- поднимает из cleaned description только ЯВНЫЕ spec-пары;
- понимает:
  1) strict "Ключ: значение"
  2) alternating key/value блоки
  3) compatibility block ("Поддерживаемые модели", "Совместимые продукты")
- не генерирует auto compat / auto codes из narrative текста;
- отдельно возвращает body_text без уже поднятых spec-линий.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

from suppliers.akcent.normalize import NormalizedOffer


_WS_RE = re.compile(r"\s+")
_KV_RE = re.compile(r"^\s*([^:]{1,120})\s*:\s*(.+?)\s*$")
_RANGE_DASH_RE = re.compile(r"\s*[-–—]+\s*")
_MULTI_SEP_RE = re.compile(r"\s*(?:;|\||•|·|●|▪|▫|■)\s*")
_SPLIT_ENUM_RE = re.compile(r"\s*;\s*")
_WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+")

# Заголовки/секции, которые не надо превращать в обычные body-lines
_HEADINGS = {
    "характеристики",
    "основные характеристики",
    "технические характеристики",
    "спецификация",
    "спецификации",
    "описание",
    "комплектация",
    "особенности",
}

# AkCent-specific alternate keys
# ВАЖНО: "Назначение" переводим в "Для устройства", чтобы не тащить запрещённый param "Назначение"
# в общий CS-core/validator.
_ALT_KEYS = {
    "вид": "Тип",
    "назначение": "Для устройства",
    "цвет печати": "Цвет",
    "поддерживаемые модели принтеров": "Совместимость",
    "поддерживаемые модели": "Совместимость",
    "поддерживаемые продукты": "Совместимость",
    "совместимые продукты": "Совместимость",
    "совместимые модели": "Совместимость",
    "ресурс": "Ресурс",
    "диагональ": "Диагональ",
    "разрешение": "Разрешение",
    "яркость": "Яркость",
    "контрастность": "Контрастность",
    "время отклика": "Время отклика",
    "гарантийный период": "Гарантия",
    "гарантия": "Гарантия",
}

# Ключи, которые не поднимаем из description в params
_BANNED_KEYS = {
    "описание",
    "подробное описание",
    "дополнительно",
    "особенности",
    "комментарий",
    "примечание",
    "важно",
    "внимание",
    "условия эксплуатации",
    "условия хранения",
    "информация",
    "артикул",
    "код товара",
    "id",
    "url",
    "ссылка",
    "наличие",
    "остаток",
}

# Narrative phrases: если value выглядит как рекламный абзац, не поднимаем как param
_NARRATIVE_PARTS = [
    "идеально подходит",
    "обеспечивает",
    "предназначен",
    "предназначена",
    "предназначены",
    "позволяет",
    "используется для",
    "подходит для",
    "отличается",
    "благодаря",
    "в комплекте",
    "в комплект поставки",
    "может использоваться",
    "рекомендуется",
    "высокое качество",
    "высококачественную печать",
]

# Явные compatibility headings
_COMPAT_HEADINGS_RE = re.compile(
    r"(?iu)^(?:"
    r"Поддерживаемые\s+модели(?:\s+принтеров)?|"
    r"Поддерживаемые\s+продукты|"
    r"Совместимые\s+продукты|"
    r"Совместимые\s+модели|"
    r"Совместимость"
    r")$"
)

# Бренд/модельный хинт для чистой совместимости
_COMPAT_BRAND_HINT_RE = re.compile(
    r"(?iu)\b(?:"
    r"Xerox|Canon|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Samsung|Sharp|Konica|SMART|ViewSonic|"
    r"VersaLink|AltaLink|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser|DocuColor|Versant|"
    r"PrimeLink|DocuCentre|SureColor|WorkForce|LaserJet|Color\s+LaserJet|PIXMA|imageRUNNER|imagePRESS|"
    r"SC-[A-Z0-9]+|WF-[A-Z0-9]+|M\d{3,4}"
    r")\b"
)

# Для strip body: короткие ключи без двоеточия
_SHORT_LABEL_RE = re.compile(r"^[A-Za-zА-Яа-яЁё0-9 /+\-().,%\"№]{2,90}$")


def _norm_ws(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _ci(s: str) -> str:
    return _norm_ws(s).casefold()


def _clean_value(value: str) -> str:
    s = _norm_ws(value).strip(" ;|")
    s = _RANGE_DASH_RE.sub(" - ", s)
    s = _MULTI_SEP_RE.sub("; ", s)
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s*;\s*", "; ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip(" ;|")


def _canonical_key(key: str) -> str:
    s = _norm_ws(key).strip(" :;|,-")
    if not s:
        return ""

    low = _ci(s)
    if low in _ALT_KEYS:
        return _ALT_KEYS[low]

    if low == "nfc":
        return "NFC"
    if low == "ean":
        return "EAN"

    # Частые нормализации
    s = re.sub(r"\(\s*bt\s*\)", "(Вт)", s, flags=re.I)
    s = re.sub(r"\(\s*w\s*\)", "(Вт)", s, flags=re.I)
    s = re.sub(r"\bbt\b", "Вт", s, flags=re.I)

    if s:
        return s[:1].upper() + s[1:]
    return ""


def _is_heading(line: str) -> bool:
    return _ci(line).strip(":") in _HEADINGS


def _word_count(s: str) -> int:
    return len(_WORD_RE.findall(s or ""))


def _looks_like_narrative(text: str) -> bool:
    low = _ci(text)
    if not low:
        return True
    if _word_count(low) > 18 and ":" not in low:
        return True
    return any(part in low for part in _NARRATIVE_PARTS)


def _bad_key(key: str) -> bool:
    low = _ci(key)
    if not low:
        return True
    if low in _BANNED_KEYS:
        return True
    if _word_count(low) > 8:
        return True
    if len(low) > 80:
        return True
    return False


def _bad_value(value: str) -> bool:
    low = _ci(value)
    if not low:
        return True
    if low in {"-", "—", "--", "...", "n/a", "na", "нет данных", "не указано"}:
        return True
    if len(low) == 1 and low not in {'"', "0", "1"}:
        return True
    return False


def _compat_looks_clean(value: str) -> bool:
    s = _clean_value(value)
    if not s:
        return False
    if ":" in s and _word_count(s.split(":", 1)[0]) <= 4:
        return False
    if len(s) > 350:
        return False
    return bool(_COMPAT_BRAND_HINT_RE.search(s))


def _split_lines(cleaned_desc: str) -> list[str]:
    lines = []
    for raw in (cleaned_desc or "").split("\n"):
        ln = _norm_ws(raw)
        if ln:
            lines.append(ln)
        else:
            lines.append("")
    return lines


def _maybe_pair_from_line(line: str) -> tuple[str, str] | None:
    m = _KV_RE.match(line)
    if not m:
        return None

    raw_key = m.group(1).strip()
    raw_value = m.group(2).strip()

    key = _canonical_key(raw_key)
    value = _clean_value(raw_value)

    if _bad_key(key) or _bad_value(value):
        return None

    # Если это явно narrative-значение — не поднимаем
    if _looks_like_narrative(value):
        if key != "Совместимость":
            return None
        if not _compat_looks_clean(value):
            return None

    return key, value


def _extract_colon_pairs(lines: list[str]) -> tuple[list[tuple[int, str, str]], set[int]]:
    out: list[tuple[int, str, str]] = []
    consumed: set[int] = set()

    for i, line in enumerate(lines):
        if not line:
            continue
        if _is_heading(line):
            continue

        pair = _maybe_pair_from_line(line)
        if not pair:
            continue

        key, value = pair
        out.append((i, key, value))
        consumed.add(i)

    return out, consumed


def _extract_alt_pairs(lines: list[str]) -> tuple[list[tuple[int, str, str]], set[int]]:
    """
    Ловит блоки вида:
    Вид
    струйный
    Назначение
    широкоформатный принтер
    """
    out: list[tuple[int, str, str]] = []
    consumed: set[int] = set()
    i = 0

    while i + 1 < len(lines):
        left = _norm_ws(lines[i]).strip(":")
        right = _clean_value(lines[i + 1])

        if not left or not right:
            i += 1
            continue

        if _is_heading(left):
            i += 1
            continue

        low_left = _ci(left)
        if low_left not in _ALT_KEYS:
            i += 1
            continue

        if ":" in right:
            i += 1
            continue

        key = _canonical_key(left)
        value = _clean_value(right)

        if _bad_key(key) or _bad_value(value):
            i += 1
            continue

        # Совместимость из alt-пары должна быть реально похожа на список моделей/брендов
        if key == "Совместимость" and not _compat_looks_clean(value):
            i += 1
            continue

        # narrative-строки не поднимаем
        if _looks_like_narrative(value) and key != "Совместимость":
            i += 1
            continue

        out.append((i, key, value))
        consumed.add(i)
        consumed.add(i + 1)
        i += 2

    return out, consumed


def _extract_compat_blocks(lines: list[str]) -> tuple[list[tuple[int, str, str]], set[int]]:
    """
    Ловит блоки вида:

    Поддерживаемые модели
    Epson A, Epson B, Epson C

    или

    Совместимые продукты
    Xerox ....
    Xerox ....
    """
    out: list[tuple[int, str, str]] = []
    consumed: set[int] = set()

    i = 0
    while i < len(lines):
        line = _norm_ws(lines[i]).strip(":")
        if not line:
            i += 1
            continue

        if not _COMPAT_HEADINGS_RE.match(line):
            i += 1
            continue

        values: list[str] = []
        j = i + 1

        while j < len(lines):
            cur = _norm_ws(lines[j])
            if not cur:
                j += 1
                continue

            # новый heading -> стоп
            if _is_heading(cur):
                break
            if _COMPAT_HEADINGS_RE.match(cur):
                break

            # если начался новый короткий label или strict kv -> стоп
            if ":" in cur and _maybe_pair_from_line(cur) is not None:
                break
            if _SHORT_LABEL_RE.fullmatch(cur) and _ci(cur) in _ALT_KEYS:
                break

            values.append(cur)
            j += 1

        merged = _clean_value("; ".join(values))
        if merged and _compat_looks_clean(merged):
            out.append((i, "Совместимость", merged))
            consumed.add(i)
            for idx in range(i + 1, j):
                consumed.add(idx)

        i = max(i + 1, j)

    return out, consumed


def _prefer_pair(existing: tuple[str, str] | None, new_key: str, new_value: str) -> tuple[str, str]:
    if existing is None:
        return new_key, new_value

    old_key, old_value = existing

    key_cf = _ci(new_key)

    # Для Совместимости берём более чистую и обычно более компактную версию
    if key_cf == "совместимость":
        old_clean = _compat_looks_clean(old_value)
        new_clean = _compat_looks_clean(new_value)

        if new_clean and not old_clean:
            return new_key, new_value
        if old_clean and not new_clean:
            return old_key, old_value

        if new_clean and old_clean:
            if len(new_value) < len(old_value):
                return new_key, new_value
            return old_key, old_value

    # Для коротких характеристик предпочитаем более короткое явное значение
    if key_cf in {"цвет", "тип", "ресурс", "технология", "гарантия"}:
        if len(new_value) < len(old_value):
            return new_key, new_value

    return old_key, old_value


def _dedupe_pairs(pairs: list[tuple[int, str, str]]) -> list[tuple[str, str]]:
    best: dict[str, tuple[str, str]] = {}
    order: list[str] = []

    for _, key, value in pairs:
        key_cf = _ci(key)
        prev = best.get(key_cf)
        chosen = _prefer_pair(prev, key, value)

        if key_cf not in best:
            order.append(key_cf)
        best[key_cf] = chosen

    return [best[k] for k in order if k in best]


def _strip_consumed_lines(lines: list[str], consumed: set[int]) -> str:
    out: list[str] = []
    prev_blank = True

    for i, line in enumerate(lines):
        if i in consumed:
            continue

        ln = _norm_ws(line)
        if not ln:
            if not prev_blank:
                out.append("")
            prev_blank = True
            continue

        if _is_heading(ln):
            continue

        out.append(ln)
        prev_blank = False

    return "\n".join(out).strip()


def extract_desc_parts(
    offer: NormalizedOffer,
    cleaned_desc: str,
) -> tuple[str, list[tuple[str, str]], dict[str, Any]]:
    """
    Возвращает:
    - body_text (описание без поднятых spec-пар)
    - desc_params
    - report
    """
    lines = _split_lines(cleaned_desc)

    colon_pairs, colon_consumed = _extract_colon_pairs(lines)
    alt_pairs, alt_consumed = _extract_alt_pairs(lines)
    compat_pairs, compat_consumed = _extract_compat_blocks(lines)

    all_pairs = colon_pairs + alt_pairs + compat_pairs
    params = _dedupe_pairs(all_pairs)

    consumed = set()
    consumed.update(colon_consumed)
    consumed.update(alt_consumed)
    consumed.update(compat_consumed)

    body_text = _strip_consumed_lines(lines, consumed)

    report: dict[str, Any] = {
        "lines_total": len([x for x in lines if _norm_ws(x)]),
        "params_from_colon": len(colon_pairs),
        "params_from_alternating": len(alt_pairs),
        "params_from_compat_blocks": len(compat_pairs),
        "params_total": len(params),
        "body_len": len(body_text),
    }
    return body_text, params, report


def extract_desc_spec_pairs(
    cleaned_desc: str,
    schema_cfg: dict[str, Any] | None = None,
) -> list[tuple[str, str]]:
    """
    Backward-safe helper в стиле AlStyle:
    возвращает только извлечённые spec-pairs без body_text.
    """
    dummy_offer = NormalizedOffer(
        raw_oid="",
        oid="",
        article="",
        offer_id="",
        source_type="",
        name="",
        vendor="",
        model="",
        url="",
        category_id="",
        available=False,
        price_in=None,
        description="",
        manufacturer_warranty="",
        stock_text="",
        pictures=[],
        xml_params=[],
        prices=[],
        raw_vendor="",
        raw_name="",
        raw_model="",
    )
    _, params, _ = extract_desc_parts(dummy_offer, cleaned_desc)
    return params


def extract_desc_bulk(
    offers: list[NormalizedOffer],
    cleaned_map: dict[str, str],
) -> tuple[dict[str, str], dict[str, list[tuple[str, str]]], dict[str, Any]]:
    body_map: dict[str, str] = {}
    params_map: dict[str, list[tuple[str, str]]] = {}

    total_body_len = 0
    total_params = 0
    counters = Counter()

    for offer in offers:
        cleaned_desc = cleaned_map.get(offer.oid) or cleaned_map.get(offer.raw_oid) or ""
        body_text, params, rep = extract_desc_parts(offer, cleaned_desc)

        body_map[offer.oid] = body_text
        params_map[offer.oid] = params

        total_body_len += len(body_text)
        total_params += len(params)
        counters["offers"] += 1
        counters["params_from_colon"] += int(rep["params_from_colon"])
        counters["params_from_alternating"] += int(rep["params_from_alternating"])
        counters["params_from_compat_blocks"] += int(rep["params_from_compat_blocks"])

    report: dict[str, Any] = {
        "offers": int(counters["offers"]),
        "params_total": total_params,
        "body_len_total": total_body_len,
        "params_from_colon": int(counters["params_from_colon"]),
        "params_from_alternating": int(counters["params_from_alternating"]),
        "params_from_compat_blocks": int(counters["params_from_compat_blocks"]),
    }
    return body_map, params_map, report
