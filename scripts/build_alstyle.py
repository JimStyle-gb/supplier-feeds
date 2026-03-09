# -*- coding: utf-8 -*-
"""
Path: scripts/build_alstyle.py

AlStyle adapter (AS) — CS-шаблон (stage-based supplier split, stage 1).

Что изменено в этой версии:
- entrypoint стал тоньше;
- source/filter/normalize/pictures/diagnostics вынесены в scripts/suppliers/alstyle/;
- supplier-specific desc/params логика пока оставлена в этом файле без смены поведения,
  чтобы безопасно пройти первый этап переноса.
"""

from __future__ import annotations

import os
import re
from difflib import SequenceMatcher
from html import unescape
from pathlib import Path
from typing import Any

import yaml

from cs.core import OfferOut, write_cs_feed, write_cs_feed_raw
from cs.meta import now_almaty, next_run_at_hour
from cs.pricing import compute_price
from cs.util import norm_ws, safe_int
from suppliers.alstyle.diagnostics import build_watch_source_map, make_watch_messages, write_watch_report
from suppliers.alstyle.filtering import filter_source_offers, parse_id_set
from suppliers.alstyle.normalize import build_offer_oid, normalize_available, normalize_name, normalize_price_in, normalize_vendor
from suppliers.alstyle.params_xml import collect_xml_params
from suppliers.alstyle.pictures import collect_picture_urls
from suppliers.alstyle.source import load_source_offers


BUILD_ALSTYLE_VERSION = "build_alstyle_v101_stage2_params_xml_split"

ALSTYLE_URL_DEFAULT = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
ALSTYLE_OUT_DEFAULT = "docs/alstyle.yml"
ALSTYLE_RAW_OUT_DEFAULT = "docs/raw/alstyle.yml"
ALSTYLE_ID_PREFIX = "AS"
ALSTYLE_WATCH_OIDS = {"AS257478"}

CFG_DIR_DEFAULT = "scripts/suppliers/alstyle/config"
FILTER_FILE_DEFAULT = "filter.yml"
SCHEMA_FILE_DEFAULT = "schema.yml"
POLICY_FILE_DEFAULT = "policy.yml"
WATCH_REPORT_DEFAULT = "docs/raw/alstyle_watch.txt"


_CODE_SERIES_RE = re.compile(
    r"(?<![\w/])(?:(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,}(?:\s*/\s*(?=[A-Z0-9._-]*\d)[A-Z0-9._-]{3,})+)"
)
_SKU_TOKEN_RE = re.compile(r"\b[A-Z]{2,}[A-Z0-9-]{4,}\b")
_CSS_SERVICE_LINE_RE = re.compile(
    r"(?iu)(?:^|\s)(?:body\s*\{|font-family\s*:|display\s*:|margin\s*:|padding\s*:|border\s*:|color\s*:|background\s*:|"
    r"\.?chip\s*\{|\.?badge\s*\{|\.?spec\s*\{|h[1-6]\s*\{)"
)


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


_DESC_SPEC_START_RE = re.compile(r"(?im)^\s*(Характеристики|Основные характеристики|Технические характеристики)\s*:?\s*$")
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
_PROJECTOR_RICH_LINE_RE = re.compile(
    r"(?im)^\s*"
    r"(Технология касания|Технология|Разрешение|Яркость|Контраст|Источник света|Световой источник|Оптика|"
    r"Методы установки|Способы установки|Размер экрана|Дистанция|Коэффициент проекции|"
    r"Форматы сторон|Смарт-?система|Беспроводной дисплей|Проводное зеркалирование|"
    r"Интерфейсы|Акустика|Питание|Габариты проектора|Вес проектора|Габариты упаковки|"
    r"Вес упаковки|Языки интерфейса|Комплектация|Беспроводные модули|Беспроводные интерфейсы|"
    r"Беспроводные подключения|Беспроводные возможности)"
    r"\s*(?::|[-–—])?\s*(.+?)\s*$"
)
_MONITOR_RICH_LINE_RE = re.compile(
    r"(?im)^\s*"
    r"(Управление|Пользовательские настройки|Встроенные колонки|Защита замком|HDMI|DisplayPort|USB-?хаб|"
    r"Разъ[её]м для наушников|Поддержка HDCP|Языки меню OSD)"
    r"\s*(?::|[-–—])\s*(.+?)\s*$"
)
_DESC_COMPAT_LABEL_ONLY_RE = re.compile(r"(?im)^\s*(Совместимость|Совместимые модели|Устройства)\s*:?\s*$")
_DESC_TECH_PRINT_LABEL_ONLY_RE = re.compile(r"(?im)^\s*Технология\s+печати\s*:?\s*$")
_DESC_COMPAT_SENTENCE_RE = re.compile(r"(?is)\bСовместим(?:а|о|ы)?\s+с\s+(.{6,220}?)(?:(?:[.!?](?:\s|$))|\n|$)")
_DESC_FOR_DEVICES_SENTENCE_RE = re.compile(
    r"(?is)\bдля\s+(?:устройств|принтеров(?:\s+и\s+МФУ)?|МФУ|аппаратов)\s+(.{6,220}?)(?:(?:[.!?](?:\s|$))|\n|$)"
)
_COMPAT_BRAND_HINT_RE = re.compile(
    r"(?i)\b(Xerox|Canon|HP|Hewlett|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki|"
    r"VersaLink|AltaLink|WorkCentre|DocuCentre|imageRUNNER|i-SENSYS|ECOSYS|bizhub)\b"
)
_COMPAT_MODEL_HINT_RE = re.compile(
    r"(?i)(?:\b[A-Z]{1,8}-?\d{2,5}[A-Z]{0,3}x?\b|\b\d{3,5}[A-Z]{0,3}i?\b|/\s*[A-Z]?\d{2,5}[A-Z]{0,3}x?\b)"
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
    "технология касания": "Технология касания",
    "технология": "Технология",
    "технология печати": "Технология",
    "разрешение": "Разрешение",
    "яркость": "Яркость",
    "контраст": "Контраст",
    "источник света": "Источник света",
    "световой источник": "Источник света",
    "оптика": "Оптика",
    "методы установки": "Методы установки",
    "способы установки": "Методы установки",
    "размер экрана": "Размер экрана",
    "дистанция": "Дистанция",
    "коэффициент проекции": "Коэффициент проекции",
    "форматы сторон": "Форматы сторон",
    "смарт-система": "Смарт-система",
    "беспроводной дисплей": "Беспроводной дисплей",
    "проводное зеркалирование": "Проводное зеркалирование",
    "интерфейсы": "Интерфейсы",
    "акустика": "Акустика",
    "питание": "Питание",
    "габариты проектора": "Габариты проектора",
    "вес проектора": "Вес проектора",
    "габариты упаковки": "Габариты упаковки",
    "вес упаковки": "Вес упаковки",
    "языки интерфейса": "Языки интерфейса",
    "комплектация": "Комплектация",
    "беспроводные модули": "Беспроводные модули",
    "беспроводные интерфейсы": "Беспроводные интерфейсы",
    "беспроводные подключения": "Беспроводные подключения",
    "беспроводные возможности": "Беспроводные возможности",
    "управление": "Управление",
    "пользовательские настройки": "Пользовательские настройки",
    "встроенные колонки": "Встроенные колонки",
    "защита замком": "Защита замком",
    "hdmi": "HDMI",
    "displayport": "DisplayPort",
    "usb-хаб": "USB-хаб",
    "usb хаб": "USB-хаб",
    "разъём для наушников": "Разъём для наушников",
    "поддержка hdcp": "Поддержка HDCP",
    "языки меню osd": "Языки меню OSD",
}

_SAFE_DESC_PARAM_KEYS = {
    "Модель", "Аналог модели", "Совместимость", "Технология", "Цвет", "Ресурс", "Ёмкость",
    "Степлирование", "Дополнительные опции", "Применение", "Количество в упаковке",
}


def _dedupe_code_series_text(text: str) -> str:
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


def _is_service_desc_line(line: str) -> bool:
    s = norm_ws(unescape(re.sub(r"<[^>]+>", " ", line or "")))
    if not s:
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
    if re.fullmatch(r"(?i)[A-Z0-9][A-Z0-9\-+/ ]{1,18}\s*x\d+", s):
        return True
    if re.fullmatch(r"(?i)(hdmi|displayport|usb-?c|usb|rj45|lan|vga|audio)\s*x\d+", s):
        return True
    return False


def _norm_title_like_text(s: str) -> str:
    s = norm_ws(unescape(re.sub(r"<[^>]+>", " ", s or "")))
    s = re.sub(r"[()\[\],;:!?.«»\"'`]+", " ", s)
    return norm_ws(s).casefold()


def _is_title_like_duplicate(name: str, line: str) -> bool:
    a = _norm_title_like_text(name)
    b = _norm_title_like_text(line)
    if not a or not b:
        return False
    if a == b:
        return True
    if a in b or b in a:
        shorter = min(len(a), len(b))
        longer = max(len(a), len(b))
        if shorter >= max(12, int(longer * 0.7)):
            return True
    ratio = SequenceMatcher(None, a, b).ratio()
    return ratio >= 0.9


def _dedupe_desc_leading_title(name: str, desc: str) -> str:
    parts = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", unescape(desc or "")) if norm_ws(x)]
    while parts and _is_title_like_duplicate(name, parts[0]):
        parts.pop(0)
    return "\n".join(parts)


def _strip_desc_sections(desc: str) -> str:
    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", unescape(desc or ""))]
    out: list[str] = []
    skip = False
    skipped_any = False
    for ln in lines:
        if not ln:
            continue
        low = ln.casefold()
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


def _align_desc_model_from_name(name: str, desc: str) -> str:
    n = norm_ws(name)
    d = norm_ws(unescape(desc or ""))
    if not n or not d:
        return d
    m_name = _SKU_TOKEN_RE.search(n)
    if not m_name:
        return d
    sku_name = m_name.group(0)
    first_sent = re.split(r"(?<=[.!?])\s+|\n+", d, maxsplit=1)[0]
    m_desc = _SKU_TOKEN_RE.search(first_sent)
    if not m_desc:
        return d
    sku_desc = m_desc.group(0)
    if sku_desc == sku_name:
        return d
    if len(sku_desc) >= 6 and len(sku_name) >= 6 and SequenceMatcher(None, sku_desc, sku_name).ratio() >= 0.82:
        return d.replace(sku_desc, sku_name, 1)
    return d


def _clean_desc_text(s: str) -> str:
    s = unescape(s or "")
    s = re.sub(r"<\s*br\s*/?\s*>", "\n", s, flags=re.I)
    s = re.sub(r"</p\s*>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", s)]
    lines = [x for x in lines if x and not _is_service_desc_line(x)]
    return "\n".join(lines)


def _is_heading_only_value(v: str) -> bool:
    vv = norm_ws(v)
    if not vv:
        return True
    low = vv.casefold()
    if low in {"характеристики", "описание", "особенности", "преимущества", "комплектация", "гарантия"}:
        return True
    if re.fullmatch(r"(?iu)(порты|что\s+в\s+коробке|комплектация)\s*:?", vv):
        return True
    return False


def _fix_common_broken_words(s: str) -> str:
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
        "!!!": "!",
    }
    for a, b in fixes.items():
        s = s.replace(a, b).replace(a.capitalize(), b.capitalize())
    return s


def _sanitize_desc_quality_text(desc: str) -> str:
    s = norm_ws(desc)
    if not s:
        return ""
    s = _fix_common_broken_words(s)
    s = re.sub(r"(?iu)Xerox\s+Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)Для\s+принтеров\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)Для\s+МФУ\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"\bWorkCenter\b", "WorkCentre", s, flags=re.I)
    s = _dedupe_code_series_text(s)
    return norm_ws(s)


def _sanitize_native_desc(desc: str) -> str:
    raw = _clean_desc_text(desc)
    if not raw:
        return ""
    before_sections = raw
    raw = _strip_desc_sections(raw)
    if len(norm_ws(raw)) < max(40, int(len(norm_ws(before_sections)) * 0.35)):
        raw = before_sections
    raw = _sanitize_desc_quality_text(raw)
    lines = [norm_ws(x) for x in re.split(r"(?:\r?\n)+", raw) if norm_ws(x)]
    while lines and (lines[0][:1] in {"(", ",", ";", ":"} or _is_service_desc_line(lines[0])):
        lines.pop(0)
    return "\n".join(lines)


def _canon_desc_spec_key(k: str) -> str:
    return _DESC_SPEC_KEY_MAP.get(norm_ws(k).casefold(), norm_ws(k))


def _normalize_tech_value(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = re.sub(r"(?iu)\bUSB\s+C\b", "USB-C", s)
    s = re.sub(r"(?iu)\bWi\s*Fi\b", "Wi‑Fi", s)
    s = re.sub(r"(?iu)\bBluetooth\s*([0-9.]+)\b", r"Bluetooth \1", s)
    s = re.sub(r"(?iu)\bFull\s*HD\b", "Full HD", s)
    s = re.sub(r"(?iu)\bANSI\s*люмен\b", "ANSI люмен", s)
    return norm_ws(s)


def _drop_broken_canon_compat_tail(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = re.sub(r"(?iu)^CANON\s+PIXMA\s+", "Canon PIXMA ", s)
    return s


def _clean_compatibility_text(v: str) -> str:
    s = _drop_broken_canon_compat_tail(v)
    s = re.sub(r"(?iu)^Xerox\s+Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)^Для,\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)^Для\s+принтеров\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"(?iu)^Для\s+МФУ\s+Xerox\s+", "Xerox ", s)
    s = re.sub(r"\bWorkCenter\b", "WorkCentre", s, flags=re.I)
    s = _split_glued_brand_models(s)
    s = _dedupe_slash_tail_models(s)
    return norm_ws(s)


def _dedupe_slash_tail_models(v: str) -> str:
    parts = [norm_ws(x) for x in re.split(r"\s*/\s*", v or "") if norm_ws(x)]
    if len(parts) < 2:
        return norm_ws(v)
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        sig = p.casefold()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(p)
    return " / ".join(out)


def _split_glued_brand_models(v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    s = re.sub(r"(?i)(Canon\s+PIXMA\s+[A-Za-z]*\d+[A-Za-z0-9-]*)(?=Canon\s+PIXMA)", r"\1 / ", s)
    s = re.sub(r"(?i)(Xerox\s+[A-Za-z-]*\d+[A-Za-z0-9/-]*)(?=Xerox\s+)", r"\1 / ", s)
    return norm_ws(s)


def _split_inline_desc_pairs(desc: str) -> list[tuple[str, str]]:
    text = norm_ws(desc)
    if not text:
        return []
    keys = [
        "Модель", "Аналог модели", "Совместимость", "Совместимые модели", "Устройства",
        "Цвет", "Ресурс", "Ресурс картриджа", "Емкость", "Ёмкость", "Емкость лотка", "Ёмкость лотка",
        "Технология печати", "Количество в упаковке", "Колличество в упаковке",
    ]
    key_pat = r"(?:" + "|".join(re.escape(k) for k in keys) + r")"
    rx = re.compile(rf"(?iu)\b({key_pat})\s*:\s*(.+?)(?=(?:\s+\b{key_pat}\s*:)|$)")
    return [(norm_ws(m.group(1)), norm_ws(m.group(2))) for m in rx.finditer(text) if norm_ws(m.group(2))]


def _extract_simple_desc_pairs(desc: str) -> list[tuple[str, str]]:
    text = norm_ws(desc)
    if not text:
        return []
    pairs: list[tuple[str, str]] = []
    for ln in _iter_desc_lines(text)[:8]:
        for k, v in _split_inline_desc_pairs(ln):
            ck = _canon_desc_spec_key(k)
            if ck in _SAFE_DESC_PARAM_KEYS and not _is_heading_only_value(v):
                pairs.append((ck, v))
    return pairs


def _extract_sentence_capacity_pairs(desc: str) -> list[tuple[str, str]]:
    text = norm_ws(desc)
    if not text:
        return []
    out: list[tuple[str, str]] = []
    m = re.search(r"(?iu)\b(?:Емкость|Ёмкость)\s+лотка\s*[-–—:]\s*(.{3,120}?)(?:(?:[.!?](?:\s|$))|$)", text)
    if m:
        out.append(("Ёмкость", norm_ws(m.group(1))))
    m = re.search(r"(?iu)\bдо\s+\d+[+\d\s]*лист[ао]в?\s+А4\b", text)
    if m and not any(k == "Ёмкость" for k, _ in out):
        out.append(("Ёмкость", norm_ws(m.group(0))))
    return out


def _sanitize_param_value(key: str, val: str) -> str:
    v = norm_ws(val)
    if not v:
        return ""
    kcf = norm_ws(key).casefold()
    if kcf == "совместимость":
        v = _clean_compatibility_text(v)
    elif kcf in {"модель", "аналог модели"}:
        v = _dedupe_code_series_text(v)
    else:
        v = _fix_common_broken_words(v)
    return norm_ws(v)


def _iter_desc_lines(text: str) -> list[str]:
    return [norm_ws(x) for x in re.split(r"(?:\r?\n)+", text or "") if norm_ws(x)]


def _is_label_only_line(line: str, rx: re.Pattern[str]) -> bool:
    return bool(rx.match(norm_ws(line)))


def _join_compat_lines(lines: list[str]) -> str:
    out: list[str] = []
    for ln in lines:
        s = norm_ws(ln)
        if not s:
            continue
        if _is_service_desc_line(s):
            continue
        out.append(s)
    return norm_ws(" ".join(out))


def _extract_multiline_compat_pairs(lines: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for i, ln in enumerate(lines):
        if _is_label_only_line(ln, _DESC_COMPAT_LABEL_ONLY_RE):
            chunk = _join_compat_lines(lines[i + 1:i + 4])
            if chunk and _looks_like_compatibility_value(chunk):
                out.append(("Совместимость", chunk))
    return out


def _extract_multiline_tech_print_pairs(lines: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for i, ln in enumerate(lines):
        if _is_label_only_line(ln, _DESC_TECH_PRINT_LABEL_ONLY_RE):
            chunk = _join_compat_lines(lines[i + 1:i + 3])
            if chunk and len(chunk) <= 80:
                out.append(("Технология", chunk))
    return out


def _parse_desc_spec_line(line: str) -> tuple[str, str] | None:
    s = norm_ws(line)
    if not s:
        return None
    m = _DESC_SPEC_LINE_RE.match(s)
    if m:
        return _canon_desc_spec_key(m.group(1)), norm_ws(m.group(2))
    m = _DESC_COMPAT_LINE_RE.match(s)
    if m:
        return "Совместимость", norm_ws(m.group(1))
    m = _PROJECTOR_RICH_LINE_RE.match(s)
    if m:
        return _canon_desc_spec_key(m.group(1)), norm_ws(m.group(2))
    m = _MONITOR_RICH_LINE_RE.match(s)
    if m:
        return _canon_desc_spec_key(m.group(1)), norm_ws(m.group(2))
    return None


def _looks_like_compatibility_value(v: str) -> bool:
    s = norm_ws(v)
    if len(s) < 6:
        return False
    return bool(_COMPAT_BRAND_HINT_RE.search(s) or _COMPAT_MODEL_HINT_RE.search(s))


def _extract_sentence_compat_pairs(desc: str) -> list[tuple[str, str]]:
    text = norm_ws(desc)
    if not text:
        return []
    out: list[tuple[str, str]] = []
    for rx in (_DESC_COMPAT_SENTENCE_RE, _DESC_FOR_DEVICES_SENTENCE_RE):
        for m in rx.finditer(text):
            candidate = norm_ws(m.group(1))
            candidate = re.sub(r"(?iu)^(?:для\s+)?(?:принтеров(?:\s+и\s+МФУ)?|МФУ|устройств|аппаратов)\s+", "", candidate)
            candidate = candidate.strip(" .,:;-")
            if _looks_like_compatibility_value(candidate):
                out.append(("Совместимость", candidate))
    return out


def _validate_desc_pair(k: str, v: str, schema: dict[str, Any]) -> tuple[str, str] | None:
    kk = _canon_desc_spec_key(k)
    if kk not in _SAFE_DESC_PARAM_KEYS:
        return None
    vv = _apply_value_normalizers(kk, v, schema)
    if not vv or _is_heading_only_value(vv):
        return None
    return kk, vv


def _extract_desc_spec_pairs(desc: str, schema: dict[str, Any]) -> list[tuple[str, str]]:
    text = norm_ws(desc)
    if not text:
        return []

    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add_pair(k: str, v: str) -> None:
        pair = _validate_desc_pair(k, v, schema)
        if not pair:
            return
        sig = (pair[0].casefold(), pair[1].casefold())
        if sig in seen:
            return
        seen.add(sig)
        out.append(pair)

    lines = _iter_desc_lines(text)
    in_block = False
    block_lines: list[str] = []
    for ln in lines:
        if _DESC_SPEC_START_RE.match(ln):
            in_block = True
            continue
        if in_block and _DESC_SPEC_STOP_RE.match(ln):
            in_block = False
            continue
        if in_block:
            block_lines.append(ln)

    for ln in block_lines:
        pair = _parse_desc_spec_line(ln)
        if pair:
            add_pair(*pair)

    if not out:
        for k, v in _extract_simple_desc_pairs(text):
            add_pair(k, v)
        for k, v in _extract_multiline_compat_pairs(lines):
            add_pair(k, v)
        for k, v in _extract_multiline_tech_print_pairs(lines):
            add_pair(k, v)
        for k, v in _extract_sentence_compat_pairs(text):
            add_pair(k, v)
        for k, v in _extract_sentence_capacity_pairs(text):
            add_pair(k, v)

    return out


def _merge_params(base_params: list[tuple[str, str]], extra_params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = list(base_params)
    seen = {(norm_ws(k).casefold(), norm_ws(v).casefold()) for k, v in base_params}
    seen_keys = {norm_ws(k).casefold() for k, _ in base_params}
    for k, v in extra_params:
        kcf = norm_ws(k).casefold()
        sig = (kcf, norm_ws(v).casefold())
        if sig in seen or kcf in seen_keys:
            continue
        out.append((k, v))
        seen.add(sig)
        seen_keys.add(kcf)
    return out


def _load_supplier_config(cfg_dir: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    filter_cfg = _read_yaml(cfg_dir / (os.getenv("ALSTYLE_FILTER_FILE") or FILTER_FILE_DEFAULT))
    schema_cfg = _read_yaml(cfg_dir / (os.getenv("ALSTYLE_SCHEMA_FILE") or SCHEMA_FILE_DEFAULT))
    policy_cfg = _read_yaml(cfg_dir / (os.getenv("ALSTYLE_POLICY_FILE") or POLICY_FILE_DEFAULT))
    return filter_cfg, schema_cfg, policy_cfg


def main() -> int:
    url = (os.getenv("ALSTYLE_URL") or ALSTYLE_URL_DEFAULT).strip()
    out_file = (os.getenv("OUT_FILE") or ALSTYLE_OUT_DEFAULT).strip()
    raw_out = (os.getenv("RAW_OUT_FILE") or ALSTYLE_RAW_OUT_DEFAULT).strip()
    watch_report = (os.getenv("WATCH_REPORT_FILE") or WATCH_REPORT_DEFAULT).strip()
    encoding = (os.getenv("OUTPUT_ENCODING") or "utf-8").strip() or "utf-8"
    timeout = int(os.getenv("HTTP_TIMEOUT", "90"))
    login = os.getenv("ALSTYLE_LOGIN")
    password = os.getenv("ALSTYLE_PASSWORD")

    cfg_dir = Path(os.getenv("ALSTYLE_CFG_DIR", CFG_DIR_DEFAULT))
    filter_cfg, schema_cfg, policy_cfg = _load_supplier_config(cfg_dir)

    env_hour = (os.getenv("SCHEDULE_HOUR_ALMATY") or "").strip()
    hour = int((policy_cfg.get("schedule_hour_almaty") or 1))
    if env_hour:
        try:
            eh = int(env_hour)
            if eh != hour:
                print(f"[build_alstyle] WARN: ignoring SCHEDULE_HOUR_ALMATY={eh}; policy.yml schedule_hour_almaty={hour}")
        except Exception:
            print(f"[build_alstyle] WARN: bad SCHEDULE_HOUR_ALMATY={env_hour!r}; using policy.yml schedule_hour_almaty={hour}")

    placeholder_picture = (
        os.getenv("PLACEHOLDER_PICTURE")
        or policy_cfg.get("placeholder_picture")
        or "https://placehold.co/800x800/png?text=No+Photo"
    )
    supplier_name = (policy_cfg.get("supplier") or "AlStyle").strip()
    vendor_blacklist = {str(x).casefold() for x in (policy_cfg.get("vendor_blacklist_casefold") or ["alstyle"])}
    fallback_ids = {str(x) for x in (filter_cfg.get("category_ids") or [])}
    allowed = parse_id_set(os.getenv("ALSTYLE_CATEGORY_IDS"), fallback_ids)

    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour=hour)

    source_offers = load_source_offers(url=url, timeout=timeout, login=login, password=password)
    before = len(source_offers)
    watch_source = build_watch_source_map(source_offers, prefix=ALSTYLE_ID_PREFIX, watch_ids=ALSTYLE_WATCH_OIDS)
    filtered_offers = filter_source_offers(source_offers, allowed)

    out_offers: list[OfferOut] = []
    in_true = 0
    in_false = 0
    watch_out: set[str] = set()

    for src in filtered_offers:
        name = _dedupe_code_series_text(normalize_name(src.name))
        if not name or not src.raw_id:
            continue

        oid = build_offer_oid(src.raw_id, prefix=ALSTYLE_ID_PREFIX)
        available = normalize_available(src.available_attr, src.available_tag)
        if available:
            in_true += 1
        else:
            in_false += 1

        pics = collect_picture_urls(src.picture_urls, placeholder_picture=placeholder_picture)
        params = collect_xml_params(src.offer_el, schema_cfg)
        vendor_src = normalize_vendor(src.vendor, vendor_blacklist=vendor_blacklist)

        desc_src = _sanitize_native_desc(src.description or "")
        desc_src = _align_desc_model_from_name(name, desc_src)
        desc_src = _dedupe_desc_leading_title(name, desc_src)
        desc_src = _align_desc_model_from_name(name, desc_src)
        params = _merge_params(params, _extract_desc_spec_pairs(desc_src, schema_cfg))

        price_in = normalize_price_in(src.purchase_price_text, src.price_text)
        price = compute_price(price_in)

        watch_out.add(oid)
        out_offers.append(
            OfferOut(
                oid=oid,
                available=available,
                name=name,
                price=price,
                pictures=pics,
                vendor=vendor_src,
                params=params,
                native_desc=desc_src,
            )
        )

    after = len(out_offers)
    watch_messages = make_watch_messages(
        watch_ids=ALSTYLE_WATCH_OIDS,
        watch_source=watch_source,
        watch_out=watch_out,
        allowed=allowed,
    )
    for msg in watch_messages:
        print(msg)
    write_watch_report(watch_report, watch_messages)

    out_offers.sort(key=lambda x: x.oid)

    write_cs_feed_raw(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=raw_out,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        currency_id="KZT",
    )

    changed = write_cs_feed(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        public_vendor=os.getenv("PUBLIC_VENDOR", "CS").strip() or "CS",
        currency_id="KZT",
    )

    print(
        f"[build_alstyle] OK | version={BUILD_ALSTYLE_VERSION} | offers_in={before} | offers_out={after} | "
        f"in_true={in_true} | in_false={in_false} | changed={'yes' if changed else 'no'} | file={out_file}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
