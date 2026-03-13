# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/desc_extract.py

AkCent description -> params extraction.

v4:
- больше не режет значения по запятой, из-за чего раньше ломались
  `Покрытие экрана`, `Число касаний`, `Покрытие экрана (антитрение, антиблик)`;
- лучше режет плотные projector / interactive техблоки по known labels;
- умеет обрезать значение по следующему label, чтобы хвосты не утекали в поле;
- убирает длинный narrative-префикс перед первым техблоком у interactive / projector / monitor;
- остаётся conservative: description только добирает missing params и не гадает compat/codes.
"""

from __future__ import annotations

import re
from typing import Any

from cs.util import norm_ws
from suppliers.akcent.params_xml import (
    detect_kind_by_name,
    key_quality_ok,
    normalize_param_value,
    resolve_allowed_keys,
)
from suppliers.akcent.compat import clean_device_value


_RE_HTML_TAG = re.compile(r"<[^>]+>")
_RE_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
_RE_MULTI_NL = re.compile(r"\n{2,}")
_RE_DROP_CHUNKS = re.compile(r"\s*(?:[;|•·]+)\s*")
_RE_PAREN_SP = re.compile(r"\(\s+|\s+\)")
_RE_LINE_PAIR = re.compile(r"^([^:]{1,120})\s*[:：]\s*(.+)$")
_RE_RANGE = re.compile(r"(?iu)\b\d{1,4}(?:[.,]\d+)?\s*[-–—]\s*\d{1,4}(?:[.,]\d+)?(?:\s*:\s*1)?\b")
_RE_DIMS = re.compile(r"(?iu)\b\d{1,4}(?:[.,]\d+)?\s*[xх×]\s*\d{1,4}(?:[.,]\d+)?(?:\s*[xх×]\s*\d{1,4}(?:[.,]\d+)?)?\b")

_SAFE_DESC_PARAM_KEYS = {
    "Тип", "Модель", "Гарантия", "Для устройства", "Для бренда", "Цвет", "Ресурс", "Объем",
    "Тип печати", "Разрешение", "Разрешение печати, dpi", "Разрешение сканера, dpi", "Интерфейсы",
    "Технология", "Источник света", "Тип источника света", "Срок службы источника света",
    "Срок службы лампы (норм./ эконом.) ч.", "Яркость", "Яркость (ANSI) лмн", "Яркость (ANSI LUMEN)",
    "Цветовая яркость", "Контрастность", "Контрастность (динамическая)", "Соотношение сторон",
    "Диагональ", "Диагональ (см)", "Размер", "Размер экрана", "Проекционное расстояние",
    "Проекционный коэффициент (Throw ratio)", "Проекционное отношение (мин)",
    "Проекционное отношение (макс)", "3D", "Интерактивный", "HDMI", "VGA", "S-Video",
    "DisplayPort", "DVI-D", "Ethernet", "USB", "Wi-Fi", "HDBaseT", "Вес", "Габариты",
    "Тип дисплея", "Покрытие экрана", "Число касаний", "Время отклика", "Частота обновления",
    "Тип матрицы", "Изогнутый экран", "VESA", "Звук", "Микрофоны", "NFC", "Энергопотребление",
    "Совместимость ПО с ОС", "Тип управления", "Стилус", "HDR", "Фокус",
}

_MANUAL_LABEL_ALIASES = {
    "Aspect Ratio": "Соотношение сторон",
    "Contrast Ratio": "Контрастность",
    "Light source": "Источник света",
    "Laser Light source": "Срок службы источника света",
    "HDR support": "HDR",
    "Throw Ratio": "Проекционный коэффициент (Throw ratio)",
    "Optical Throw Ratio": "Проекционный коэффициент (Throw ratio)",
    "Projection Distance Wide/Tele": "Проекционное расстояние",
    "Projection Distance": "Проекционное расстояние",
    "Screen Size": "Диагональ",
    "Interfaces": "Интерфейсы",
    "Connectivity": "Интерфейсы",
    "Projection Lens Focus": "Фокус",
    "Проекционная система": "Технология",
    "Тип дисплея": "Тип дисплея",
    "Покрытие экрана": "Покрытие экрана",
    "Диагональ": "Диагональ",
    "Яркость": "Яркость",
    "Цветной световой поток": "Цветовая яркость",
    "Выход белого света": "Яркость",
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
    "Оригинальное разрешение": "Разрешение",
}

_STOP_LABELS = {
    "Жесты", "Панель управления", "Индикатор состояния", "Минимальный размер объекта",
    "Входы (на задней стороне)", "Входы (на передней стороне)", "Размер упаковки", "Вес брутто",
    "Product dimensions", "Product weight", "General", "Other", "Advanced Features",
    "Advanced features", "Colour Modes", "Video Color Modes", "Lens Optical", "Offset",
    "Supported Temperature", "Supported Humidity", "Loudspeaker", "Room Type / Application",
    "Positioning", "Colour", "Other Warranty", "Комплектация", "Описание", "Характеристики",
    "Технические характеристики", "Основные характеристики", "Общие параметры", "Упаковка",
    "Упаковка, габариты, вес",
}

_DROP_LABELS_CF = {
    "описание", "характеристики", "технические характеристики", "основные характеристики",
    "общие параметры", "общие характеристики", "общие характерстики", "конструкция",
    "электропитание", "дополнительно", "упаковка", "упаковка, габариты, вес", "комплектация",
}


def _clean_text(value: Any) -> str:
    s = str(value or "")
    if not s:
        return ""
    s = _RE_COMMENT.sub(" ", s)
    s = s.replace("\r", "\n").replace("\xa0", " ")
    s = _RE_HTML_TAG.sub("\n", s)
    s = s.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    s = s.replace("•", "\n").replace("·", "\n")
    s = _RE_PAREN_SP.sub(lambda m: "(" if m.group(0).startswith("(") else ")", s)
    lines = [norm_ws(x) for x in s.split("\n")]
    lines = [x for x in lines if x]
    return "\n".join(lines)


def _schema_aliases(schema_cfg: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    raw = schema_cfg.get("aliases") or {}
    for src, dst in raw.items():
        s = norm_ws(str(src))
        d = norm_ws(str(dst))
        if s and d:
            out[s] = d
    for src, dst in _MANUAL_LABEL_ALIASES.items():
        out.setdefault(norm_ws(src), norm_ws(dst))
    return out


def _allowed_keys(schema_cfg: dict[str, Any], kind: str) -> set[str]:
    return {norm_ws(x) for x in resolve_allowed_keys(schema_cfg, kind) if norm_ws(x)}


def _candidate_labels(schema_cfg: dict[str, Any], kind: str) -> dict[str, str]:
    allowed = _allowed_keys(schema_cfg, kind)
    aliases = _schema_aliases(schema_cfg)
    out: dict[str, str] = {}
    for k in allowed:
        out[k] = k
    for src, dst in aliases.items():
        if dst in allowed:
            out[src] = dst
    return out


def _all_known_labels(schema_cfg: dict[str, Any], kind: str) -> list[str]:
    labels = set(_candidate_labels(schema_cfg, kind).keys())
    labels.update(_STOP_LABELS)
    labels.update(_MANUAL_LABEL_ALIASES.keys())
    return sorted((norm_ws(x) for x in labels if norm_ws(x)), key=len, reverse=True)


def _normalize_key(key: str, schema_cfg: dict[str, Any], kind: str) -> str:
    key = norm_ws(key)
    if not key:
        return ""
    aliases = _schema_aliases(schema_cfg)
    if key in aliases:
        return aliases[key]
    low = key.casefold()
    for src, dst in aliases.items():
        if src.casefold() == low:
            return dst
    return key


def _clip_to_first_tech_block(text: str, schema_cfg: dict[str, Any], kind: str) -> str:
    if kind not in {"interactive", "projector", "monitor", "lamination"}:
        return text
    first_idx = None
    for lbl in _all_known_labels(schema_cfg, kind):
        m = re.search(rf"(?iu)\b{re.escape(lbl)}\b", text)
        if not m:
            continue
        idx = m.start()
        if first_idx is None or idx < first_idx:
            first_idx = idx
    if first_idx is not None and first_idx > 140:
        return text[first_idx:]
    return text


def _inject_breaks(text: str, schema_cfg: dict[str, Any], kind: str) -> str:
    s = _clip_to_first_tech_block(_clean_text(text), schema_cfg, kind)
    for lbl in _all_known_labels(schema_cfg, kind):
        pat = re.compile(rf"(?<!^)(?<!\n)(?=\b{re.escape(lbl)}\b(?:\s*[:：]|\s+))", re.IGNORECASE)
        s = pat.sub("\n", s)
    return _RE_MULTI_NL.sub("\n", s)


def _prepare_lines(text: str, schema_cfg: dict[str, Any], kind: str) -> list[str]:
    s = _inject_breaks(text, schema_cfg, kind)
    lines: list[str] = []
    for raw in s.split("\n"):
        raw = norm_ws(raw)
        if not raw:
            continue
        for part in [norm_ws(x) for x in _RE_DROP_CHUNKS.split(raw) if norm_ws(x)] or [raw]:
            if part.casefold() in _DROP_LABELS_CF:
                continue
            lines.append(part)
    return lines


def _cut_at_next_label(value: str, schema_cfg: dict[str, Any], kind: str, current_key: str) -> str:
    v = norm_ws(value)
    if not v:
        return ""
    cut_at = None
    current_cf = current_key.casefold()
    for lbl in _all_known_labels(schema_cfg, kind):
        norm_lbl = _normalize_key(lbl, schema_cfg, kind)
        if norm_lbl and norm_lbl.casefold() == current_cf:
            continue
        m = re.search(rf"(?iu)(?<!^)\b{re.escape(lbl)}\b(?:\s*[:：]|\s+)", v)
        if not m:
            continue
        if cut_at is None or m.start() < cut_at:
            cut_at = m.start()
    if cut_at is not None:
        v = norm_ws(v[:cut_at])
    return v


def _extract_line_pair(line: str, schema_cfg: dict[str, Any], kind: str) -> tuple[str, str] | None:
    line = norm_ws(line)
    if not line:
        return None
    m = _RE_LINE_PAIR.match(line)
    if m:
        key = _normalize_key(m.group(1), schema_cfg, kind)
        val = _cut_at_next_label(m.group(2), schema_cfg, kind, key)
        if key and val:
            return key, val
    labels = _candidate_labels(schema_cfg, kind)
    for src in sorted(labels.keys(), key=len, reverse=True):
        src_low = src.casefold()
        low = line.casefold()
        if low == src_low:
            return None
        if low.startswith(src_low + " "):
            key = labels[src]
            val = _cut_at_next_label(norm_ws(line[len(src):]), schema_cfg, kind, key)
            if val:
                return key, val
    return None


def _cleanup_value_by_key(key: str, value: str) -> str:
    v = norm_ws(value)
    if not v:
        return ""
    if key == "Покрытие экрана":
        m = re.search(r"(?iu)^([^\n]+?\))", v)
        if m:
            return norm_ws(m.group(1))
    if key == "Число касаний":
        m = re.search(r"(?iu)^\d{1,3}(?:\s*\([^)]*\))?", v)
        if m:
            return norm_ws(m.group(0))
    if key == "Технология":
        return norm_ws(re.split(r"(?iu)\bЖесты\b|\bПанель управления\b|\bГарантия\b|\bРесурс лампы\b|\bРазрешение\b|\bЯркость\b", v, maxsplit=1)[0])
    if key == "Микрофоны":
        return norm_ws(re.split(r"(?iu)\bИндикатор состояния\b|\bВстроенный NFC считыватель\b", v, maxsplit=1)[0])
    if key == "NFC" and re.search(r"(?iu)\bесть\b", v):
        return "Есть"
    if key == "Диагональ":
        m = re.search(r'(?iu)\b\d{1,3}(?:[.,]\d+)?\s*(?:"|”|дюйм(?:ов|а)?|inch(?:es)?)\b(?:\s*[-–—]\s*\d{1,3}(?:[.,]\d+)?\s*(?:"|”|дюйм(?:ов|а)?|inch(?:es)?)\b)?', v)
        if m:
            return norm_ws(m.group(0))
    if key in {"Проекционный коэффициент (Throw ratio)", "Проекционное расстояние"}:
        m = re.search(r"(?iu)\b\d{1,3}(?:[.,]\d+)?\s*[-–—]\s*\d{1,3}(?:[.,]\d+)?\s*:\s*1\b", v)
        if m:
            return norm_ws(m.group(0))
        m = _RE_RANGE.search(v)
        if m:
            return norm_ws(m.group(0))
        m = re.search(r"(?iu)\b\d{1,3}(?:[.,]\d+)?\s*m\b(?:\s*[-–—]\s*\d{1,3}(?:[.,]\d+)?\s*m\b)?", v)
        if m:
            return norm_ws(m.group(0))
    if key == "Вес":
        m = re.search(r"(?iu)\b\d{1,4}(?:[.,]\d+)?\s*кг\b", v)
        if m:
            return norm_ws(m.group(0))
        return ""
    if key == "Энергопотребление":
        m = re.search(r"(?iu)\b\d{1,4}(?:[.,]\d+)?\s*Вт\b", v)
        if m:
            return norm_ws(m.group(0))
        return ""
    if key in {"Яркость", "Яркость (ANSI) лмн", "Цветовая яркость"}:
        m = re.search(r"(?iu)(?:≥\s*)?\d{1,5}(?:[.,]\d+)?\s*(?:cd/m²|ansi\s*lumen|ansi\s*lm|лмн|лм|люмен)\b", v)
        if m:
            return norm_ws(m.group(0).replace("ANSI LUMEN", "ANSI lm").replace("ANSI lumen", "ANSI lm"))
        m = re.search(r"(?iu)\b\d{1,5}(?:[.,]\d+)?\b", v)
        if m and key == "Яркость (ANSI) лмн":
            return norm_ws(m.group(0))
        return ""
    if key == "Разрешение":
        m = re.search(r"(?iu)\b(?:wxga|xga|full\s*hd|4k\s*uhd|uhd|hd|svga)\b(?:\s*\([^)]*\))?", v)
        if m:
            return norm_ws(m.group(0))
        m = re.search(r"(?iu)\b\d{3,5}\s*[xх×]\s*\d{3,5}\b", v)
        if m:
            return norm_ws(m.group(0))
        if v.casefold() in {"оригинальное", "native", "original"}:
            return ""
        return ""
    if key in {"HDMI", "USB", "VGA", "DisplayPort", "Ethernet", "Wi-Fi", "DVI-D", "S-Video", "HDBaseT"}:
        if v.casefold() in {"и", "или", "with", "and"}:
            return ""
        if key != "USB" and len(v) > 120:
            return ""
    if key == "Интерфейсы":
        if len(v) > 180:
            return ""
        parts = [norm_ws(x) for x in re.split(r"\s*,\s*", v) if norm_ws(x)]
        if parts:
            return ", ".join(dict.fromkeys(parts))
    if key == "Габариты":
        m = _RE_DIMS.search(v)
        if m:
            return norm_ws(m.group(0))
    if key == "Контрастность":
        m = re.search(r"(?iu)\b\d[\d .]{0,12}:\s*1\b", v)
        if m:
            return norm_ws(m.group(0))
    return v


def _prefer_duplicate_value(key: str, old: str, new: str) -> str:
    old = norm_ws(old)
    new = norm_ws(new)
    if not old:
        return new
    if not new:
        return old
    if key == "Разрешение":
        bad = {"оригинальное", "native", "original"}
        if old.casefold() in bad:
            return new
        if new.casefold() in bad:
            return old
        old_num = bool(re.search(r"(?iu)\b\d{3,5}\s*[xх×]\s*\d{3,5}\b", old))
        new_num = bool(re.search(r"(?iu)\b\d{3,5}\s*[xх×]\s*\d{3,5}\b", new))
        if old_num and not new_num:
            return old
        if new_num and not old_num:
            return new
    if key in {"Яркость", "Яркость (ANSI) лмн", "Цветовая яркость", "Контрастность", "Вес", "Проекционное расстояние", "Проекционный коэффициент (Throw ratio)"}:
        return new if len(new) > len(old) else old
    if key in {"HDMI", "USB", "VGA", "DisplayPort", "Ethernet", "Wi-Fi", "DVI-D", "S-Video", "HDBaseT"}:
        if old.casefold() in {"и", "или", "with", "and"}:
            return new
        if new.casefold() in {"и", "или", "with", "and"}:
            return old
    return old if len(old) >= len(new) else new


def _extract_consumable_device_pair(lines: list[str], schema_cfg: dict[str, Any], kind: str) -> tuple[str, str] | None:
    if kind != "consumable":
        return None
    patterns = (
        re.compile(r"(?iu)\bподдерживаемые\s+модели\s+(?:принтеров|устройств|техники)\s*:\s*(.+)$"),
        re.compile(r"(?iu)\bсовместимые\s+модели(?:\s+техники)?\s*:\s*(.+)$"),
        re.compile(r"(?iu)\bдля\s*:\s*(.+)$"),
    )
    for line in lines:
        text = norm_ws(line)
        if not text:
            continue
        low = text.casefold().replace("ё", "е")
        if "epson" not in low and "surecolor" not in low and "workforce" not in low and "stylus" not in low and "ecotank" not in low:
            continue
        for pat in patterns:
            m = pat.search(text)
            if not m:
                continue
            raw = norm_ws(m.group(1))
            clean = norm_ws(clean_device_value(raw))
            if clean and len(clean) <= 300:
                checked = validate_desc_pair("Для устройства", clean, schema_cfg, kind)
                if checked is not None:
                    return checked
    return None


def _dedupe_pairs(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    by_key: dict[str, int] = {}
    seen: set[tuple[str, str]] = set()
    for k, v in pairs:
        sig = (k.casefold(), v.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        key_cf = k.casefold()
        if key_cf in by_key:
            idx = by_key[key_cf]
            old_k, old_v = out[idx]
            best = _prefer_duplicate_value(k, old_v, v)
            out[idx] = (old_k, best)
            continue
        by_key[key_cf] = len(out)
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
    val2 = norm_ws(normalize_param_value(key, val, schema_cfg))
    val2 = _cleanup_value_by_key(key, val2)
    val2 = norm_ws(val2)
    if not val2 or len(val2) > 300:
        return None
    return key, val2


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
    lines = _prepare_lines(desc_src or "", schema_cfg, kind)
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

    have_device = any(k == "Для устройства" for k, _ in accepted)
    have_compat = any(k == "Совместимость" for k, _ in accepted)
    if kind == "consumable" and not have_device and not have_compat:
        extra_device = _extract_consumable_device_pair(lines, schema_cfg, kind)
        if extra_device is not None:
            accepted.append(extra_device)

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
