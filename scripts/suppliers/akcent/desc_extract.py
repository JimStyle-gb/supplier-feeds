# -*- coding: utf-8 -*-
"""
AkCent description extract layer.

Что делает:
- из cleaned description вытаскивает только воспроизводимые params
- отдельно оставляет narrative/body text
- не генерирует выдуманные характеристики
- не создаёт auto compat / auto codes

Важно:
- брать только то, что явно есть в тексте
- длинные рекламные абзацы не превращать в params
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

from suppliers.akcent.normalize import NormalizedOffer


_WS_RE = re.compile(r"\s+")
_KV_RE = re.compile(
    r"^\s*([A-Za-zА-Яа-яЁё0-9 /+\-().,%\"№@&]{2,90})\s*:\s*(.{1,2000})\s*$"
)
_ALT_KEY_RE = re.compile(r"^[A-Za-zА-Яа-яЁё0-9 /+\-().,%\"№]{2,90}$")
_ONLY_UNITS_RE = re.compile(r"^(?:мм|см|м|г|кг|мс|вт|в|гц|дб|dpi|ppi|mah|tb|gb|mb)$", re.I)
_MODELISH_RE = re.compile(r"[A-ZА-Я0-9][A-ZА-Я0-9\-_/]{2,}")
_SPLIT_ENUM_RE = re.compile(r"\s*(?:;|\|)\s*")

# Заголовки, которые не должны попадать в body как обычный текст
_HEADINGS = {
    "характеристики",
    "основные характеристики",
    "технические характеристики",
    "спецификация",
    "спецификации",
    "описание",
    "комплектация",
    "особенности",
    "интерфейсы",
    "порты и интерфейсы",
}

# Частые alternate keys AkCent
_ALT_KEYS = {
    "вид": "Тип",
    "назначение": "Назначение",
    "цвет печати": "Цвет",
    "цвет": "Цвет",
    "поддерживаемые модели": "Совместимость",
    "поддерживаемые модели принтеров": "Совместимость",
    "поддерживаемые продукты": "Совместимость",
    "ресурс": "Ресурс",
    "диагональ": "Диагональ",
    "разрешение": "Разрешение",
    "яркость": "Яркость",
    "контрастность": "Контрастность",
    "время отклика": "Время отклика",
    "стилус": "Стилус",
    "число касаний": "Число касаний",
    "покрытие экрана": "Покрытие экрана",
    "соотношение сторон": "Соотношение сторон",
    "подключение": "Подключение",
    "энергопотребление": "Энергопотребление",
    "тип дисплея": "Тип дисплея",
    "технология распознавания": "Технология распознавания",
    "жесты": "Жесты",
    "звук": "Звук",
    "микрофоны": "Микрофоны",
    "индикатор состояния": "Индикатор состояния",
    "встроенный nfc считыватель": "Встроенный NFC считыватель",
    "минимальный размер объекта": "Минимальный размер объекта",
    "гарантия": "Гарантия",
}

# Ключи, которые точно не стоит тащить из desc в params
_BANNED_KEYS = {
    "описание",
    "подробное описание",
    "преимущества",
    "особенности модели",
    "особенности устройства",
    "дополнительно",
    "комментарий",
    "примечание",
    "важно",
    "внимание",
    "условия эксплуатации",
    "условия хранения",
    "информация",
    "для заказа",
    "артикул",
    "код товара",
    "id",
    "url",
    "ссылка",
}

# Фразы, характерные для narrative, а не для характеристик
_NARRATIVE_PARTS = [
    "идеально подходит",
    "обеспечивает",
    "предназначен",
    "позволяет",
    "используется для",
    "подходит для",
    "отличается",
    "благодаря",
    "в комплекте",
    "в комплект поставки",
    "может использоваться",
    "рекомендуется",
    "современный дизайн",
    "высокое качество",
]


def _norm_space(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _ci(s: str) -> str:
    return _norm_space(s).casefold()


def _is_heading(line: str) -> bool:
    return _ci(line).strip(":") in _HEADINGS


def _canonical_key(key: str) -> str:
    key = _norm_space(key).strip(" :;|,-")
    if not key:
        return ""

    low = _ci(key)
    if low in _ALT_KEYS:
        return _ALT_KEYS[low]

    # Аккуратные нормализации единиц/опечаток
    key = re.sub(r"\(\s*bt\s*\)", "(Вт)", key, flags=re.I)
    key = re.sub(r"\(\s*w\s*\)", "(Вт)", key, flags=re.I)
    key = re.sub(r"\bbt\b", "Вт", key, flags=re.I)

    if key.lower() == "nfc":
        return "NFC"

    return key[:1].upper() + key[1:] if key else ""


def _clean_value(value: str) -> str:
    value = _norm_space(value).strip(" ;|")
    value = value.replace(" ,", ",").replace(" .", ".")
    value = re.sub(r"\s*[-–—]\s*", " - ", value)
    value = _norm_space(value)
    return value


def _looks_like_narrative(line: str) -> bool:
    low = _ci(line)
    if not low:
        return True
    if len(low.split()) > 18 and ":" not in low:
        return True
    return any(part in low for part in _NARRATIVE_PARTS)


def _bad_key(key: str) -> bool:
    low = _ci(key)
    if not low:
        return True
    if low in _BANNED_KEYS:
        return True
    if len(low) < 2:
        return True
    if len(low.split()) > 8:
        return True
    if _ONLY_UNITS_RE.fullmatch(low):
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


def _split_enumeration_value(value: str) -> str:
    parts = [x.strip() for x in _SPLIT_ENUM_RE.split(value) if x.strip()]
    if len(parts) <= 1:
        return value
    # Оставляем через ; чтобы было стабильно
    return "; ".join(parts)


def _maybe_pair_from_line(line: str) -> tuple[str, str] | None:
    m = _KV_RE.match(line)
    if not m:
        return None

    key = _canonical_key(m.group(1))
    value = _clean_value(m.group(2))
    value = _split_enumeration_value(value)

    if _bad_key(key) or _bad_value(value):
        return None

    # Не превращаем явные narrative-абзацы в param
    if _looks_like_narrative(value) and not _MODELISH_RE.search(value):
        if len(value.split()) > 18:
            return None

    return key, value


def _extract_alternating_pairs(lines: list[str]) -> list[tuple[str, str]]:
    """
    Ловит блоки вида:
    Вид
    Интерактивная доска
    Назначение
    Для переговорных
    """
    out: list[tuple[str, str]] = []
    i = 0
    while i + 1 < len(lines):
        left = _norm_space(lines[i]).strip(":")
        right = _clean_value(lines[i + 1])

        if not left or not right:
            i += 1
            continue

        if _is_heading(left):
            i += 1
            continue

        # только компактный key, не narrative
        if not _ALT_KEY_RE.fullmatch(left):
            i += 1
            continue
        if len(left.split()) > 6:
            i += 1
            continue
        if ":" in right:
            i += 1
            continue
        if _looks_like_narrative(left):
            i += 1
            continue

        key = _canonical_key(left)
        value = _split_enumeration_value(right)

        if _bad_key(key) or _bad_value(value):
            i += 1
            continue

        out.append((key, value))
        i += 2

    return out


def _dedupe_pairs_keep_order(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for k, v in pairs:
        key = (_ci(k), _ci(v))
        if key in seen:
            continue
        seen.add(key)
        out.append((k, v))

    return out


def _strip_param_lines_from_body(lines: list[str], pairs: list[tuple[str, str]]) -> list[str]:
    pair_lines = {
        f"{_ci(k)}::: {_ci(v)}".replace("::: ", ":::"): None for k, v in pairs
    }
    # нормальный ключ для сопоставления с исходной line вида "key: value"
    pair_norms = {
        f"{_ci(k)}::: {_ci(v)}" for k, v in pairs
    }

    body: list[str] = []
    for line in lines:
        ln = _norm_space(line)
        if not ln:
            if body and body[-1] != "":
                body.append("")
            continue

        p = _maybe_pair_from_line(ln)
        if p:
            sig = f"{_ci(p[0])}::: {_ci(p[1])}"
            if sig in pair_norms:
                continue

        if _is_heading(ln):
            continue

        body.append(ln)

    # схлопываем пустые строки
    compact: list[str] = []
    prev_blank = True
    for line in body:
        if not line.strip():
            if not prev_blank:
                compact.append("")
            prev_blank = True
            continue
        compact.append(line)
        prev_blank = False

    return compact


def extract_desc_parts(
    offer: NormalizedOffer,
    cleaned_desc: str,
) -> tuple[str, list[tuple[str, str]], dict[str, Any]]:
    lines = [_norm_space(x) for x in (cleaned_desc or "").split("\n")]
    lines = [x for x in lines if x is not None]

    colon_pairs: list[tuple[str, str]] = []
    for line in lines:
        if not line or _is_heading(line):
            continue
        pair = _maybe_pair_from_line(line)
        if pair:
            colon_pairs.append(pair)

    alt_pairs = _extract_alternating_pairs([x for x in lines if x.strip()])

    params = _dedupe_pairs_keep_order(colon_pairs + alt_pairs)
    body_lines = _strip_param_lines_from_body(lines, params)
    body_text = "\n".join(body_lines).strip()

    report: dict[str, Any] = {
        "lines_total": len([x for x in lines if x.strip()]),
        "params_from_colon": len(colon_pairs),
        "params_from_alternating": len(alt_pairs),
        "params_total": len(params),
        "body_len": len(body_text),
    }
    return body_text, params, report


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
        cleaned_desc = cleaned_map.get(offer.oid, "")
        body_text, params, rep = extract_desc_parts(offer, cleaned_desc)

        body_map[offer.oid] = body_text
        params_map[offer.oid] = params

        total_body_len += len(body_text)
        total_params += len(params)
        counters["offers"] += 1
        counters["params_from_colon"] += int(rep["params_from_colon"])
        counters["params_from_alternating"] += int(rep["params_from_alternating"])

    report: dict[str, Any] = {
        "offers": int(counters["offers"]),
        "params_total": total_params,
        "body_len_total": total_body_len,
        "params_from_colon": int(counters["params_from_colon"]),
        "params_from_alternating": int(counters["params_from_alternating"]),
    }
    return body_map, params_map, report
