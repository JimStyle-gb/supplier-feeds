# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/desc_extract.py

AkCent supplier layer — extraction характеристик из supplier description.

Что делает:
- работает только после desc_clean.py;
- поднимает из description только безопасные пары key/value;
- сначала пытается читать явные key:value строки;
- потом читает compact/alt-pairs вида "Ключ" -> следующая строка;
- потом добирает часть тех-строк без двоеточия, если они начинаются с известного label;
- не гадает compat/codes из narrative free-text;
- уважает schema.yml: aliases / key_rules / allow_by_kind / normalizers.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from cs.util import fix_mixed_cyr_lat, norm_ws
from suppliers.akcent.desc_clean import clean_description_text, description_lines
from suppliers.akcent.params_xml import (
    detect_kind_by_name,
    key_quality_ok,
    normalize_param_key,
    normalize_param_value,
    resolve_allowed_keys,
)


_RE_WS = re.compile(r"\s+")
_RE_KV = re.compile(r"^\s*([^:]{1,80})\s*:\s*(.+?)\s*$")
_RE_COMPACT_SPLIT = re.compile(r"\s{2,}|\t+|\s+[\-–—]\s+")
_RE_HTML = re.compile(r"<[^>]+>")
_RE_ONLY_PUNCT = re.compile(r"^[\s\-–—:;,.|/\\•·*()\[\]]+$")
_RE_CODEY = re.compile(r"(?iu)\b(?:C13T\d{5,6}[A-Z]?|C12C\d{6,9}|C11[A-Z0-9]{6,10}|V1[123]H[A-Z0-9]{5,10}|W\d{4}[A-Z])\b")
_RE_WARRANTY_INLINE = re.compile(r"(?iu)^гарантия\s+(.*)$")
_RE_YN_TAIL = re.compile(r"(?iu)\b(?:да|нет|yes|no|true|false)\b(?:\s*\(.+\))?$")

# Стартовые label для строк без двоеточия.
_LABEL_PREFIX_MAP = {
    "тип": "Тип",
    "тип печати": "Тип печати",
    "тип матрицы": "Тип матрицы",
    "тип управления": "Тип управления",
    "тип чернил": "Тип чернил",
    "тип расходных материалов": "Тип расходных материалов",
    "для устройства": "Для устройства",
    "для бренда": "Для бренда",
    "цвет": "Цвет",
    "цвета": "Цвета",
    "цветность": "Цветность",
    "ресурс": "Ресурс",
    "объем": "Объем",
    "объём": "Объем",
    "коды": "Коды",
    "совместимость": "Совместимость",
    "гарантия": "Гарантия",
    "яркость": "Яркость",
    "цветовая яркость": "Цветовая яркость",
    "контрастность": "Контрастность",
    "разрешение": "Разрешение",
    "технология": "Технология",
    "источник света": "Источник света",
    "тип источника света": "Тип источника света",
    "срок службы лампы": "Срок службы лампы (норм./ эконом.) ч.",
    "срок службы источника света": "Срок службы источника света",
    "уровень шума": "Уровень шума (норм./эконом.) Дб",
    "проекционный коэффициент": "Проекционный коэффициент (Throw ratio)",
    "проекционный коэффицент": "Проекционный коэффициент (Throw ratio)",
    "проекционное отношение": "Проекционное отношение (мин)",
    "проекционное расстояние": "Проекционное расстояние",
    "диагональ": "Диагональ",
    "подсветка": "Подсветка",
    "углы обзора": "Углы обзора",
    "соотношение сторон": "Соотношение сторон",
    "время отклика": "Время отклика",
    "частота обновления": "Частота обновления",
    "глубина цвета": "Глубина цвета",
    "размер пикселя": "Размер пикселя",
    "интерфейсы": "Интерфейсы",
    "формат": "Формат",
    "двусторонняя печать": "Двусторонняя печать",
    "жк дисплей": "ЖК дисплей",
    "автоподатчик": "Автоподатчик",
    "разрешение печати": "Разрешение печати, dpi",
    "разрешение сканера": "Разрешение сканера, dpi",
    "скорость печати": "Скорость печати (A4)",
    "печать фото": "Печать фото",
    "область применения": "Область применения",
    "минимальная плотность бумаги": "Минимальная плотность бумаги, г/м²",
    "максимальная плотность бумаги": "Максимальная плотность бумаги, г/м²",
    "ширина печати": "Ширина печати, мм",
    "количество слотов для картриджей": "Количество слотов для картриджей",
    "тип резки": "Тип резки",
    "уровень секретности": "Уровень секретности",
    "уничтожение": "Уничтожение",
    "производительность уничтожителя": "Производительность уничтожителя",
    "емкость корзины": "Емкость корзины, л",
    "ёмкость корзины": "Емкость корзины, л",
    "размер": "Размер",
    "ширина": "Ширина",
    "высота": "Высота",
    "габариты": "Габариты",
    "размеры": "Размеры",
    "внешние размеры": "Внешние Размеры",
    "активная область": "Активная область",
    "метод ввода": "Метод ввода",
    "скорость отклика": "Скорость отклика",
    "вес": "Вес",
    "вес нетто": "Вес нетто",
    "вес брутто": "Вес брутто",
    "размер упаковки": "Размер упаковки",
}

# Точные и безопасные label-only строки, где value ожидается на следующей строке.
_LABEL_ONLY_MAP = {
    "тип": "Тип",
    "тип печати": "Тип печати",
    "цвет": "Цвет",
    "ресурс": "Ресурс",
    "объем": "Объем",
    "объём": "Объем",
    "коды": "Коды",
    "совместимость": "Совместимость",
    "гарантия": "Гарантия",
    "яркость": "Яркость",
    "контрастность": "Контрастность",
    "разрешение": "Разрешение",
    "интерфейсы": "Интерфейсы",
    "формат": "Формат",
    "диагональ": "Диагональ",
    "тип чернил": "Тип чернил",
    "тип расходных материалов": "Тип расходных материалов",
}

# Для narrow-flow AkCent совместимость/коды берём только из явных label-pairs.
_STRICT_TEXT_KEYS = {"Коды", "Совместимость", "Для устройства"}


def _clean_text(value: Any) -> str:
    s = str(value or "")
    s = _RE_HTML.sub(" ", s)
    s = s.replace("\xa0", " ")
    s = fix_mixed_cyr_lat(s)
    s = norm_ws(s)
    s = _RE_WS.sub(" ", s)
    return s.strip(" ;,.-")


def _cf(s: str) -> str:
    return _clean_text(s).casefold().replace("ё", "е")


def _append_unique(
    out: list[tuple[str, str]],
    seen: set[tuple[str, str]],
    key: str,
    value: str,
) -> None:
    k = _clean_text(key)
    v = _clean_text(value)
    if not k or not v:
        return
    sig = (_cf(k), _cf(v))
    if sig in seen:
        return
    seen.add(sig)
    out.append((k, v))


def _get_field(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj.get(name)
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


def _line_noise(line: str) -> bool:
    s = _clean_text(line)
    if not s:
        return True
    if _RE_ONLY_PUNCT.fullmatch(s):
        return True
    low = _cf(s)
    if low in {"описание", "характеристики", "основные характеристики", "технические характеристики", "комплектация"}:
        return True
    return False


def _allowed_keys(schema_cfg: dict[str, Any], kind: str) -> set[str]:
    return resolve_allowed_keys(schema_cfg, kind=kind)


def _normalize_pair(key: str, value: str, schema_cfg: dict[str, Any], kind: str) -> tuple[str, str] | None:
    k = normalize_param_key(key, schema_cfg)
    if not k or not key_quality_ok(k, schema_cfg):
        return None

    # Только allowed_by_kind/default.
    if k not in _allowed_keys(schema_cfg, kind):
        return None

    v = normalize_param_value(k, value, schema_cfg)
    if not v:
        return None

    # Для строгих ключей из текста никаких narrative guesses.
    if k in _STRICT_TEXT_KEYS:
        raw_v = _clean_text(value)
        if not raw_v:
            return None
        # Должно быть явно похоже на список/кодовую строку, а не narrative-предложение.
        if len(raw_v) > 220:
            return None
        if k == "Коды" and not _RE_CODEY.search(raw_v):
            return None
    return (k, v)


def _iter_explicit_kv_lines(lines: Iterable[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for line in lines:
        s = _clean_text(line)
        if _line_noise(s):
            continue
        m = _RE_KV.match(s)
        if m:
            out.append((_clean_text(m.group(1)), _clean_text(m.group(2))))
            continue

        # Compact format: "Ключ    значение" или "Ключ - значение"
        parts = [x for x in _RE_COMPACT_SPLIT.split(s, maxsplit=1) if _clean_text(x)]
        if len(parts) == 2:
            left, right = _clean_text(parts[0]), _clean_text(parts[1])
            if left and right and len(left) <= 60:
                out.append((left, right))
    return out


def _iter_label_only_pairs(lines: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for i, raw in enumerate(lines[:-1]):
        cur = _clean_text(raw)
        nxt = _clean_text(lines[i + 1])
        if _line_noise(cur) or _line_noise(nxt):
            continue
        key = _LABEL_ONLY_MAP.get(_cf(cur))
        if not key:
            continue
        if _cf(nxt) in _LABEL_ONLY_MAP:
            continue
        if len(nxt) > 240:
            continue
        out.append((key, nxt))
    return out


def _iter_label_prefix_lines(lines: Iterable[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for raw in lines:
        s = _clean_text(raw)
        if _line_noise(s):
            continue
        s_cf = _cf(s)
        for prefix_cf, key in sorted(_LABEL_PREFIX_MAP.items(), key=lambda x: len(x[0]), reverse=True):
            if not s_cf.startswith(prefix_cf + " "):
                continue
            value = s[len(s.split()[0]):].strip()  # fallback
            # Более аккуратно: режем реальный префикс по длине нормализованного текста.
            orig = s
            pref_words = len(prefix_cf.split())
            value = " ".join(orig.split()[pref_words:]).strip()
            if not value:
                break
            # Не тащим явный narrative tail на сотни символов.
            if len(value) > 220:
                break
            # Совместимость/коды не берём из префиксных narrative-строк.
            if key in _STRICT_TEXT_KEYS:
                break
            out.append((key, value))
            break

        # Спецкейс: "Гарантия 1 год".
        m = _RE_WARRANTY_INLINE.match(s)
        if m:
            val = _clean_text(m.group(1))
            if val:
                out.append(("Гарантия", val))
    return out


def _post_rank_pairs(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    # Приоритет: явные key:value > label-only > prefix-lines.
    # Здесь просто режем дубли по ключу+значению с сохранением порядка.
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for k, v in pairs:
        _append_unique(out, seen, k, v)
    return out


def extract_desc_params(
    description: str,
    *,
    name: str = "",
    kind: str = "",
    vendor: str = "",
    model: str = "",
    schema_cfg: dict[str, Any] | None = None,
) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    schema_cfg = dict(schema_cfg or {})
    kind = kind or detect_kind_by_name(name, schema_cfg)

    cleaned = clean_description_text(
        description,
        name=name,
        kind=kind,
        vendor=vendor,
        model=model,
    )
    lines = [x for x in description_lines(cleaned, name=name, kind=kind, vendor=vendor, model=model) if x]

    sources_cfg = (schema_cfg.get("sources") or {})
    use_alt_pairs = bool(((sources_cfg.get("desc_alt_pairs") or {}).get("enabled", True)))
    kv_cfg = sources_cfg.get("desc_kv_block") or {}
    use_kv_block = bool(kv_cfg.get("enabled", True))
    min_kv_lines = int(kv_cfg.get("min_kv_lines") or 5)

    raw_pairs: list[tuple[str, str]] = []

    explicit_kv = _iter_explicit_kv_lines(lines) if use_kv_block else []
    if explicit_kv and len(explicit_kv) >= min_kv_lines:
        raw_pairs.extend(explicit_kv)
    else:
        # Даже если kv-блок короткий, всё равно берём часть явных пар — но аккуратно.
        raw_pairs.extend(explicit_kv)

    if use_alt_pairs:
        raw_pairs.extend(_iter_label_only_pairs(lines))
        raw_pairs.extend(_iter_label_prefix_lines(lines))

    raw_pairs = _post_rank_pairs(raw_pairs)

    params: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    rejected: list[dict[str, str]] = []

    for raw_key, raw_val in raw_pairs:
        normalized = _normalize_pair(raw_key, raw_val, schema_cfg, kind)
        if not normalized:
            rejected.append({"key": _clean_text(raw_key), "value": _clean_text(raw_val), "reason": "not_allowed_or_bad"})
            continue
        key, val = normalized
        _append_unique(params, seen, key, val)

    report = {
        "kind": kind,
        "cleaned_lines": len(lines),
        "raw_pairs": len(raw_pairs),
        "accepted_pairs": len(params),
        "rejected_pairs": len(rejected),
        "rejected_preview": rejected[:20],
        "cleaned_description": cleaned,
    }
    return params, report


def collect_desc_params(
    src: Any,
    *,
    schema_cfg: dict[str, Any] | None = None,
) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    schema_cfg = dict(schema_cfg or {})

    name = _clean_text(_get_field(src, "name"))
    description = _clean_text(_get_field(src, "description", "desc"))
    vendor = _clean_text(_get_field(src, "vendor"))
    model = _clean_text(_get_field(src, "model"))
    kind = _clean_text(_get_field(src, "kind")) or detect_kind_by_name(name, schema_cfg)

    return extract_desc_params(
        description,
        name=name,
        kind=kind,
        vendor=vendor,
        model=model,
        schema_cfg=schema_cfg,
    )
