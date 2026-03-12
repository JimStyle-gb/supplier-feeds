# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/desc_extract.py

AkCent description -> params extraction.

v2:
- усиливает extraction для projector / interactive;
- лучше режет плотные техблоки без ':' на строки по известным labels;
- умеет поднимать пары вида `Aspect Ratio 16:9`, `Light source Laser`,
  `HDR support HDR10`, `Диагональ 75"`, `Покрытие экрана ...`, `Число касаний 20`;
- не угадывает codes/compat из narrative, кроме уже разрешённых XML/compat-слоёв;
- остаётся conservative: description только добирает недостающие характеристики.
"""

from __future__ import annotations

import re
from typing import Any

from cs.util import norm_ws
from suppliers.akcent.params_xml import (
    detect_kind_by_name,
    key_quality_ok,
    normalize_param_value,
)


_RE_HTML_TAG = re.compile(r"<[^>]+>")
_RE_WS = re.compile(r"\s+")
_RE_MULTI_NL = re.compile(r"\n{2,}")
_RE_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
_RE_SPLIT_DENSE = re.compile(r"\s*[,;|]\s*")
_RE_NUM_UNIT = re.compile(r"(?iu)\b\d[\d\s.,]*\s*(?:мс|Вт|кг|см|мм|дюйм(?:а|ов)?|\"|мкм|dpi|Гц|лм|люмен|:1|x\d+)?\b")

# Только безопасные ключи description-layer.
_SAFE_DESC_PARAM_KEYS = {
    "Тип",
    "Модель",
    "Гарантия",
    "Для устройства",
    "Для бренда",
    "Цвет",
    "Ресурс",
    "Объем",
    "Тип печати",
    "Разрешение",
    "Разрешение печати, dpi",
    "Разрешение сканера, dpi",
    "Интерфейсы",
    "Технология",
    "Источник света",
    "Тип источника света",
    "Срок службы источника света",
    "Срок службы лампы (норм./ эконом.) ч.",
    "Яркость",
    "Яркость (ANSI) лмн",
    "Яркость (ANSI LUMEN)",
    "Цветовая яркость",
    "Контрастность",
    "Контрастность (динамическая)",
    "Соотношение сторон",
    "Диагональ",
    "Диагональ (см)",
    "Размер",
    "Размер экрана",
    "Проекционное расстояние",
    "Проекционный коэффициент (Throw ratio)",
    "Проекционное отношение (мин)",
    "Проекционное отношение (макс)",
    "3D",
    "Интерактивный",
    "HDMI",
    "VGA",
    "S-Video",
    "DisplayPort",
    "DVI-D",
    "Ethernet",
    "USB",
    "Wi-Fi",
    "HDBaseT",
    "Вес",
    "Габариты",
    "Тип дисплея",
    "Покрытие экрана",
    "Число касаний",
    "Время отклика",
    "Частота обновления",
    "Тип матрицы",
    "Изогнутый экран",
    "VESA",
    "Звук",
    "Микрофоны",
    "NFC",
    "Энергопотребление",
    "Совместимость ПО с ОС",
    "Тип управления",
    "Стилус",
    "HDR",
    "Фокус",
}

_MANUAL_LABEL_ALIASES = {
    "Aspect Ratio": "Соотношение сторон",
    "Light source": "Источник света",
    "Laser Light source": "Срок службы источника света",
    "HDR support": "HDR",
    "Throw Ratio": "Проекционный коэффициент (Throw ratio)",
    "Projection Distance Wide/Tele": "Проекционное расстояние",
    "Projection Distance": "Проекционное расстояние",
    "Screen Size": "Диагональ",
    "Interfaces": "Интерфейсы",
    "Connectivity": "Интерфейсы",
    "Projection Lens Focus": "Фокус",
    "Type display": "Тип дисплея",
    "Тип дисплея": "Тип дисплея",
    "Покрытие экрана": "Покрытие экрана",
    "Диагональ": "Диагональ",
    "Яркость": "Яркость",
    "Число касаний": "Число касаний",
    "Технология распознавания": "Технология",
    "Стилус": "Стилус",
    "Звук": "Звук",
    "Микрофоны": "Микрофоны",
    "Встроенный NFC считыватель": "NFC",
    "Время отклика": "Время отклика",
    "Энергопотребление": "Энергопотребление",
    "Подключение": "Интерфейсы",
    "Тип матрицы": "Тип матрицы",
    "Изогнутый экран": "Изогнутый экран",
    "Крепление VESA": "VESA",
    "Совместимость с Mac®": "Совместимость ПО с ОС",
    "Совместимость с ПК": "Совместимость ПО с ОС",
    "Тип управления": "Тип управления",
}

_DROP_LABELS = {
    "описание",
    "характеристики",
    "технические характеристики",
    "основные характеристики",
    "общие параметры",
    "конструкция",
    "электропитание",
    "дополнительно",
    "оптический",
    "образ",
    "технологии",
    "connectivity",
    "advanced features",
    "упаковка",
    "упаковка, габариты, вес",
    "комплектация",
}

_BAD_INLINE_LABEL_RE = re.compile(
    r"(?iu)^(?:"
    r"общие\s+характер(?:истики|стики)|описание|характеристики|технические\s+характеристики|"
    r"комплектация|упаковка|дополнительно|образ|оптический|технологии|advanced\s+features|connectivity"
    r")$"
)


def _clean_text(s: str) -> str:
    s = str(s or "")
    if not s:
        return ""
    s = _RE_COMMENT.sub(" ", s)
    s = s.replace("\r", "\n").replace("\xa0", " ")
    s = _RE_HTML_TAG.sub("\n", s)
    s = s.replace("•", "\n").replace("·", "\n")
    s = s.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    lines = [norm_ws(x) for x in s.split("\n")]
    lines = [x for x in lines if x]
    return "\n".join(lines)


def _schema_aliases(schema_cfg: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    aliases = schema_cfg.get("aliases") or {}
    for k, v in aliases.items():
        ks = norm_ws(str(k))
        vs = norm_ws(str(v))
        if ks and vs:
            out[ks] = vs
    for k, v in _MANUAL_LABEL_ALIASES.items():
        out.setdefault(norm_ws(k), norm_ws(v))
    return out


def _allowed_keys(schema_cfg: dict[str, Any], kind: str) -> set[str]:
    allow = schema_cfg.get("allow_by_kind") or {}
    out = set()
    for x in (allow.get("default") or []):
        sx = norm_ws(str(x))
        if sx:
            out.add(sx)
    for x in (allow.get(kind) or []):
        sx = norm_ws(str(x))
        if sx:
            out.add(sx)
    return out


def _candidate_labels(schema_cfg: dict[str, Any], kind: str) -> dict[str, str]:
    allowed = _allowed_keys(schema_cfg, kind)
    aliases = _schema_aliases(schema_cfg)
    out: dict[str, str] = {}
    for k in allowed:
        out[k] = k
    for src, dst in aliases.items():
        if dst in allowed:
            out[src] = dst
    # безопасные extras для projector / interactive
    for src, dst in _MANUAL_LABEL_ALIASES.items():
        if dst in allowed:
            out.setdefault(src, dst)
    return out


def _looks_like_value(v: str) -> bool:
    v = norm_ws(v)
    if not v:
        return False
    if len(v) < 1:
        return False
    if len(v) > 500:
        return False
    return True


def _normalize_key(key: str, schema_cfg: dict[str, Any], kind: str) -> str:
    key = norm_ws(key)
    if not key:
        return ""
    aliases = _candidate_labels(schema_cfg, kind)
    if key in aliases:
        return aliases[key]
    low = key.casefold()
    for src, dst in aliases.items():
        if src.casefold() == low:
            return dst
    return key


def _prepare_dense_lines(text: str, schema_cfg: dict[str, Any], kind: str) -> list[str]:
    labels = _candidate_labels(schema_cfg, kind)
    if not labels:
        return [x for x in _clean_text(text).split("\n") if x]
    ordered = sorted(labels.keys(), key=len, reverse=True)

    s = _clean_text(text)
    # Вставляем перевод строки перед известным label, если он прилип к предыдущему значению.
    for lbl in ordered:
        pat = re.compile(rf"(?<!^)(?<!\n)(?=\b{re.escape(lbl)}\b(?:\s*[:：]|\s+))", re.IGNORECASE)
        s = pat.sub("\n", s)

    raw_lines: list[str] = []
    for line in s.split("\n"):
        line = norm_ws(line)
        if not line:
            continue
        parts = [norm_ws(x) for x in _RE_SPLIT_DENSE.split(line) if norm_ws(x)]
        raw_lines.extend(parts or [line])

    out: list[str] = []
    for line in raw_lines:
        if not line:
            continue
        if line.casefold() in _DROP_LABELS or _BAD_INLINE_LABEL_RE.match(line):
            continue
        out.append(line)
    return out


def _extract_line_pair(line: str, schema_cfg: dict[str, Any], kind: str) -> tuple[str, str] | None:
    line = norm_ws(line)
    if not line:
        return None

    # классический Ключ: значение
    m = re.match(r"^([^:]{1,120})\s*[:：]\s*(.+)$", line)
    if m:
        key = _normalize_key(m.group(1), schema_cfg, kind)
        val = norm_ws(m.group(2))
        if key and _looks_like_value(val):
            return (key, val)

    # line starts with known label and дальше просто value без ':'
    labels = _candidate_labels(schema_cfg, kind)
    ordered = sorted(labels.keys(), key=len, reverse=True)
    low = line.casefold()
    for src in ordered:
        src_low = src.casefold()
        if low == src_low:
            return None
        if low.startswith(src_low + " "):
            key = labels[src]
            val = norm_ws(line[len(src) :])
            if _looks_like_value(val):
                return (key, val)
    return None


def _dedupe_pairs(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for k, v in pairs:
        sig = (k.casefold(), v.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((k, v))
    return out


def validate_desc_pair(key: str, val: str, schema_cfg: dict[str, Any], kind: str) -> tuple[str, str] | None:
    key = norm_ws(key)
    val = norm_ws(val)
    if not key or not val:
        return None

    drop = {str(x).casefold() for x in (schema_cfg.get("discard_exact") or [])}
    banned = {str(x).casefold() for x in (schema_cfg.get("banned_exact") or [])}
    if key.casefold() in drop or key.casefold() in banned:
        return None
    if key not in _SAFE_DESC_PARAM_KEYS:
        return None

    if not key_quality_ok(key, schema_cfg):
        return None

    allowed = _allowed_keys(schema_cfg, kind)
    if allowed and key not in allowed:
        return None

    val2 = normalize_param_value(key, val, schema_cfg)
    val2 = norm_ws(val2)
    if not val2:
        return None

    # специальные подрезки
    if key in {"Интерфейсы", "Совместимость ПО с ОС"}:
        val2 = re.sub(r"\s{2,}", ", ", val2)
    if key in {"Диагональ", "Время отклика", "Энергопотребление", "Частота обновления"}:
        m = _RE_NUM_UNIT.search(val2)
        if m:
            val2 = norm_ws(m.group(0))
    if len(val2) > 300:
        return None
    return (key, val2)


def extract_desc_params(
    desc_src: str,
    *,
    name: str = "",
    kind: str = "",
    vendor: str = "",
    model: str = "",
    schema_cfg: dict[str, Any] | None = None,
) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    schema_cfg = schema_cfg or {}
    kind = norm_ws(kind) or detect_kind_by_name(name or model, schema_cfg)

    lines = _prepare_dense_lines(desc_src or "", schema_cfg, kind)
    raw_pairs: list[tuple[str, str]] = []
    for line in lines:
        pair = _extract_line_pair(line, schema_cfg, kind)
        if pair:
            raw_pairs.append(pair)

    accepted: list[tuple[str, str]] = []
    rejected: list[dict[str, str]] = []
    for key, val in raw_pairs:
        checked = validate_desc_pair(key, val, schema_cfg, kind)
        if checked is None:
            rejected.append({"key": key, "value": val, "reason": "not_allowed_or_bad"})
            continue
        accepted.append(checked)

    accepted = _dedupe_pairs(accepted)
    report = {
        "kind": kind,
        "cleaned_lines": len(lines),
        "raw_pairs": len(raw_pairs),
        "accepted_pairs": len(accepted),
        "rejected_pairs": len(rejected),
        "rejected_preview": rejected[:10],
        "cleaned_description": "\n".join(lines[:80]),
    }
    return accepted, report
