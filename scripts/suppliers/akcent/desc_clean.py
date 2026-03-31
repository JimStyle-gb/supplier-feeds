# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/desc_clean.py

AkCent supplier layer — очистка supplier description.

Что делает:
- чистит HTML/служебный мусор;
- сохраняет границы строк для будущего desc_extract.py;
- режет дубли title/model/vendor в начале описания;
- убирает пустые и шумные строки;
- удаляет supplier-хвосты "Дополнительно" / "Сопутствующие товары";
- мягко разрезает плотные тех-строки на label-friendly блоки.

Важно:
- модуль НЕ строит финальное HTML-описание;
- модуль НЕ вытаскивает params сам по себе;
- задача только одна: сделать description безопасным и пригодным для extraction.
"""

from __future__ import annotations

import html
import re
from difflib import SequenceMatcher
from typing import Iterable

from cs.util import fix_mixed_cyr_lat, norm_ws


# ----------------------------- regex / const -----------------------------

_RE_HTML_COMMENT = re.compile(r"(?is)<!--.*?-->")
_RE_SCRIPT_STYLE = re.compile(r"(?is)<(script|style)\b[^>]*>.*?</\1>")
_RE_TAG_BR = re.compile(r"(?is)<\s*br\s*/?\s*>")
_RE_TAG_BLOCK = re.compile(r"(?is)</?\s*(?:p|div|section|article|tr|table|thead|tbody|tfoot|h[1-6]|ul|ol)\b[^>]*>")
_RE_TAG_LI_OPEN = re.compile(r"(?is)<\s*li\b[^>]*>")
_RE_TAG_LI_CLOSE = re.compile(r"(?is)</\s*li\s*>")
_RE_TAG_ANY = re.compile(r"(?is)<[^>]+>")
_RE_WS = re.compile(r"[ \t\x0b\x0c\r]+")
_RE_MULTI_NL = re.compile(r"\n{3,}")
_RE_LABEL_BREAK = re.compile(
    r"(?<!^)(?<!\n)(?=\b(?:"
    r"Тип|Назначение|Для устройства|Для бренда|Цвет|Ресурс|Объем|Объ[её]м|"
    r"Совместимость|Коды|Гарантия|Диагональ|Разрешение|Яркость|Контрастность|"
    r"Интерфейсы|Формат|Скорость|Время отклика|Тип печати|Тип расходных материалов|"
    r"Технология|Источник света|Уровень шума|Размер(?:ы)?|Вес(?: \(.*?\))?|"
    r"Соотношение сторон|Ширина|Высота|Активная область|Тип управления"
    r")\s*:)"
)
_RE_URL = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_RE_PHONE = re.compile(r"\+?\d[\d\s()\-]{7,}\d")
_RE_EMAIL = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_RE_ONLY_PUNCT = re.compile(r"^[\s\-–—:;,.|/\\•·*]+$")
_RE_CSS_GARBAGE = re.compile(r"(?iu)(?:^|\s)(?:font-family\s*:|display\s*:|margin\s*:|padding\s*:|border\s*:|color\s*:|background\s*:)")
_RE_OAICITE = re.compile(r"(?is):{0,2}contentReference\[[^\]]*oaicite[^\]]*\](?:\{[^{}]*\})?")
_RE_ARTICLE_LINE = re.compile(r"(?iu)^(?:артикул|код\s+товара|sku|offer_id|штрихкод)\s*:?\s*.+$")
_RE_VENDOR_LINE = re.compile(r"(?iu)^(?:производитель|brand|vendor)\s*:?\s*.+$")
_RE_MODEL_LINE = re.compile(r"(?iu)^(?:модель|model)\s*:?\s*.+$")
_RE_SECTION_HEADING = re.compile(
    r"(?iu)^(?:описание|характеристики|основные\s+характеристики|технические\s+характеристики|"
    r"комплектация|преимущества|особенности|назначение|условия\s+гарантии|гарантия)\s*:?$"
)
_RE_EXTRA_SECTION_LINE = re.compile(
    r"(?iu)^(?:дополнительно|сопутствующие\s+товары)\s*:?(?:\s*.+)?$"
)
_RE_BULLET_LEAD = re.compile(r"^[•·▪◦*\-–—]+\s*")

_RE_INLINE_SUPPLIER_HEADER = re.compile(
    r"(?iu)^(?:основные\s+преимущества|общие\s+характеристики|общие\s+характерстики)\s*:\s*"
)


# ----------------------------- small helpers -----------------------------


# Чистим plain-текст.
def _clean_text(value: object) -> str:
    s = str(value or "")
    s = html.unescape(s)
    s = s.replace("\u00a0", " ")
    s = fix_mixed_cyr_lat(s)
    s = _RE_WS.sub(" ", s)
    return s.strip(" \t\n\r;|")


# Достаём текст из HTML, сохраняя логические переносы.
def _html_to_text(value: str) -> str:
    s = str(value or "")
    if not s:
        return ""
    s = _RE_HTML_COMMENT.sub(" ", s)
    s = _RE_SCRIPT_STYLE.sub(" ", s)
    s = _RE_OAICITE.sub(" ", s)
    s = _RE_TAG_BR.sub("\n", s)
    s = _RE_TAG_LI_OPEN.sub("\n• ", s)
    s = _RE_TAG_LI_CLOSE.sub("\n", s)
    s = _RE_TAG_BLOCK.sub("\n", s)
    s = _RE_TAG_ANY.sub(" ", s)
    s = html.unescape(s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n[ \t]+", "\n", s)
    s = _RE_MULTI_NL.sub("\n\n", s)
    return s.strip()


# Нормализуем строку для сравнения с title.
def _title_like(s: str) -> str:
    s = _clean_text(s)
    s = re.sub(r"[()\[\],;:!?.«»\"'`]+", " ", s)
    return norm_ws(s).casefold().replace("ё", "е")


# Проверяем, что строка почти дублирует name.
def _is_title_duplicate(name: str, line: str) -> bool:
    a = _title_like(name)
    b = _title_like(line)
    if not a or not b:
        return False
    if a == b:
        return True
    if a in b or b in a:
        shorter = min(len(a), len(b))
        longer = max(len(a), len(b))
        if shorter >= max(12, int(longer * 0.7)):
            return True
    return SequenceMatcher(None, a, b).ratio() >= 0.90


# Считаем строку явным служебным мусором.
def _is_service_line(line: str) -> bool:
    s = _clean_text(line)
    if not s:
        return True
    if _RE_ONLY_PUNCT.fullmatch(s):
        return True
    low = s.casefold()
    if _RE_CSS_GARBAGE.search(s):
        return True
    if low in {"html", "body", "div", "span", "nbsp"}:
        return True
    if _RE_URL.fullmatch(s) or _RE_PHONE.fullmatch(s) or _RE_EMAIL.fullmatch(s):
        return True
    return False


# Разбиваем плотные тех-строки на блоки по типовым label.
def _split_dense_labels(text: str) -> str:
    s = str(text or "")
    if not s:
        return ""
    return _RE_LABEL_BREAK.sub("\n", s)


# Чистим отдельные строки.
def _cleanup_lines(lines: Iterable[str], *, name: str, vendor: str = "", model: str = "") -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    vendor_cf = _clean_text(vendor).casefold()
    model_cf = _clean_text(model).casefold()

    for raw in lines:
        line = _clean_text(raw)
        if not line:
            continue
        line = _RE_BULLET_LEAD.sub("", line)
        line = line.strip("-–—| ")
        line = _clean_text(line)
        if not line:
            continue
        if _is_service_line(line):
            continue
        if _RE_SECTION_HEADING.fullmatch(line):
            continue
        # Убираем supplier-блоки "Дополнительно" / "Сопутствующие товары" целиком.
        if _RE_EXTRA_SECTION_LINE.match(line):
            continue
        # Убираем supplier-шапки вида "Основные преимущества:" / "Общие характеристики:"
        # но сохраняем полезный текст, если он идёт в той же строке.
        line = _RE_INLINE_SUPPLIER_HEADER.sub("", line).strip()
        if not line:
            continue
        if _is_title_duplicate(name, line):
            continue
        # Убираем дубли vendor/model как отдельные строки в начале narrative.
        if vendor_cf and _title_like(line) == vendor_cf:
            continue
        if model_cf and _title_like(line) == model_cf:
            continue
        # Убираем чисто служебные supplier-линии.
        if _RE_ARTICLE_LINE.match(line):
            continue
        if _RE_VENDOR_LINE.match(line):
            continue
        if _RE_MODEL_LINE.match(line) and _is_title_duplicate(name, line.split(":", 1)[-1]):
            continue

        sig = line.casefold().replace("ё", "е")
        if sig in seen:
            continue
        seen.add(sig)
        out.append(line)
    return out


# Мягко режем очень длинные строки без потери ':' блоков.
def _soft_wrap_lines(lines: Iterable[str]) -> list[str]:
    out: list[str] = []
    for line in lines:
        s = _clean_text(line)
        if not s:
            continue
        if len(s) > 220 and ":" in s:
            parts = [norm_ws(x) for x in s.split(";") if norm_ws(x)]
            if len(parts) >= 2:
                out.extend(parts)
                continue
        out.append(s)
    return out


# Главная очистка description в multiline plain-text.


def _param_value(params: list[tuple[str, str]], key: str) -> str:
    key_cf = _clean_text(key).casefold().replace("ё", "е")
    for k, v in params or []:
        k_cf = _clean_text(k).casefold().replace("ё", "е")
        if k_cf == key_cf:
            return _clean_text(v)
    return ""


def _original_consumable_prefix(subject: str) -> str:
    low = _clean_text(subject).casefold().replace("ё", "е")
    if "емкость" in low or "ёмкость" in low:
        return "Оригинальная"
    if low == "чернила":
        return "Оригинальные"
    return "Оригинальный"


def _color_phrase(color_value: str) -> str:
    color = _clean_text(color_value)
    if not color:
        return ""
    low = color.casefold().replace("ё", "е")
    if low.endswith(("ый", "ий", "ой")):
        return f"{color[:-2]}ого цвета"
    if low.endswith("ая"):
        return f"{color[:-2]}ой цвета"
    if low.endswith("ое"):
        return f"{color[:-2]}ого цвета"
    return f"{color} цвета"


def _build_consumable_short_desc(params: list[tuple[str, str]]) -> str:
    type_value = _clean_text(_param_value(params, "Тип") or "Расходный материал")
    brand_value = _clean_text(
        _param_value(params, "Для бренда")
        or _param_value(params, "Бренд")
        or _param_value(params, "Производитель")
    )
    model_value = _clean_text(_param_value(params, "Модель"))
    codes_value = _clean_text(_param_value(params, "Коды"))
    color_value = _clean_text(_param_value(params, "Цвет"))
    resource_value = _clean_text(_param_value(params, "Ресурс"))
    device_value = _clean_text(_param_value(params, "Для устройства") or _param_value(params, "Совместимость"))

    subject = type_value or "Расходный материал"
    prefix = brand_value or ""

    code_hint = ""
    if model_value and codes_value and model_value in codes_value:
        code_hint = model_value
    elif model_value:
        code_hint = model_value
    elif codes_value:
        code_hint = codes_value.split('/')[0].strip()

    parts = []
    original_prefix = _original_consumable_prefix(subject)
    if prefix and code_hint:
        parts.append(f"{original_prefix} {subject.lower()} {prefix} {code_hint}")
    elif prefix:
        parts.append(f"{original_prefix} {subject.lower()} {prefix}")
    elif code_hint:
        parts.append(f"{subject} {code_hint}")
    else:
        parts.append(subject)

    color_phrase = _color_phrase(color_value)
    if color_phrase:
        parts[-1] += f" {color_phrase}"

    if device_value:
        parts[-1] += f" для {device_value}"

    if resource_value:
        parts[-1] += f". Ресурс: {resource_value}"

    return _clean_text(parts[-1]).strip(". ") + "."


def build_consumable_short_desc(params: list[tuple[str, str]]) -> str:
    return _build_consumable_short_desc(params)


def _normalize_epson_device_list(value: str) -> str:
    s = _clean_text(value)
    if not s:
        return ""
    s = re.sub(r"(?iu)\bMAINTENANCE\s+BOX\b", "Maintenance Box", s)
    s = re.sub(r"(?iu)\bULTRACHROME\b", "UltraChrome", s)
    s = re.sub(r"(?iu)\s*/\s*", " / ", s)
    s = norm_ws(s)
    return s


def _normalize_consumable_device_value(value: str) -> str:
    src = _clean_text(value)
    if not src:
        return ""
    parts = re.split(r"(?iu)\s*(?:;|,|\n|/)\s*", src)
    cleaned = [_normalize_epson_device_list(x) for x in parts]
    cleaned = [x for x in cleaned if x]
    out: list[str] = []
    seen: set[str] = set()
    for item in cleaned:
        key = item.casefold().replace("ё", "е")
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return " / ".join(out)


def _drop_consumable_device_narrative(clean_desc: str, params: list[tuple[str, str]], *, kind: str) -> str:
    text = _clean_text(clean_desc)
    if kind != "consumable" or not text:
        return text
    device_value = _normalize_consumable_device_value(_param_value(params, "Для устройства") or _param_value(params, "Совместимость"))
    if not device_value:
        return text

    text = re.sub(r"(?iu)^\s*(?:поддерживаемые|совместимые)\s+модели\s*:?\s*", "", text)
    text = text.strip(" .;,-")
    if text.casefold().replace("ё", "е") == device_value.casefold().replace("ё", "е"):
        return ""
    return text


def soften_consumable_body(clean_desc: str, params: list[tuple[str, str]], *, kind: str) -> str:
    text = _drop_consumable_device_narrative(clean_desc, params, kind=kind)
    text = _clean_text(text)
    if not text:
        return text

    if kind != "consumable":
        return text

    text = _RE_INLINE_SUPPLIER_HEADER.sub(" ", text)
    text = re.sub(r"(?iu)\s*[;|]\s*", ". ", text)
    text = _clean_text(text)

    if not text:
        return _build_consumable_short_desc(params).strip()

    low = text.casefold().replace("ё", "е")
    if any(mark in low for mark in [
        'вид струй', 'назначение', 'цвет печати', 'поддерживаемые модели',
        'совместимые модели', 'совместимые продукты', 'ресурс '
    ]):
        return _build_consumable_short_desc(params).strip()

    if re.fullmatch(r'(?iu)(?:емкость|ёмкость)\s+для\s+отработанных\s+чернил(?:\s+[A-Z0-9-]+)?', text):
        return _build_consumable_short_desc(params).strip()
    if re.fullmatch(r'(?iu)чернила(?:\s+[A-Z0-9-]+)?', text):
        return _build_consumable_short_desc(params).strip()

    return text


def strip_name_prefix_from_desc(desc: str, name: str) -> str:
    text = _clean_text(desc)
    title = _clean_text(name)
    if not text or not title:
        return text
    pat = re.compile(r"(?iu)^" + re.escape(title) + r"(?:[\s\-–—:,.]+)?")
    stripped = pat.sub("", text, count=1).strip()
    return stripped or text


def _tail_after_model(name: str, model: str) -> str:
    s = _clean_text(name)
    m = _clean_text(model)
    if not s or not m:
        return ""
    pat = re.compile(r"(?iu)^.*?\b" + re.escape(m) + r"\b")
    tail = pat.sub("", s, count=1).strip(" ,;-–—")
    tail = _clean_text(tail)
    tail = re.sub(r"(?iu)\bMAINTENANCE\s+BOX\b", "Maintenance Box", tail)
    tail = re.sub(r"(?u)\bТ(?=\d)", "T", tail)
    return tail






def _waste_tank_generic_second_sentence() -> str:
    return "Контейнер предназначен для сбора отработанных чернил и заменяется после уведомления принтера."

def _device_sentence_from_params(params: list[tuple[str, str]]) -> str:
    device_value = _normalize_consumable_device_value(
        _param_value(params, "Для устройства") or _param_value(params, "Совместимость")
    )
    if not device_value:
        return ""
    return _clean_text(f"Подходит для устройств: {device_value}.")

def _waste_tank_lead_sentence(name: str, params: list[tuple[str, str]]) -> str:
    typ = _param_value(params, "Тип")
    brand = _param_value(params, "Для бренда")
    model = _param_value(params, "Модель")

    if _clean_text(typ).casefold().replace("ё", "е") != _clean_text("Ёмкость для отработанных чернил").casefold().replace("ё", "е"):
        return ""

    base = f"Оригинальная ёмкость для отработанных чернил {brand} {model}".strip()
    tail = _tail_after_model(name, model)
    if tail:
        return _clean_text(f"{base} для {tail}.")
    return _clean_text(base + ".")


def finalize_waste_tank_desc(desc: str, name: str, params: list[tuple[str, str]]) -> str:
    text = _clean_text(desc)
    typ = _param_value(params, "Тип")
    brand = _param_value(params, "Для бренда")
    model = _param_value(params, "Модель")

    if _clean_text(typ).casefold().replace("ё", "е") != _clean_text("Ёмкость для отработанных чернил").casefold().replace("ё", "е"):
        return text

    tail = _tail_after_model(name, model)
    base = _clean_text(f"Оригинальная ёмкость для отработанных чернил {brand} {model}")
    lead = _clean_text(f"{base} для {tail}.") if tail else _clean_text(base + ".")
    device_sentence = _device_sentence_from_params(params)
    generic_second = _waste_tank_generic_second_sentence()

    text = re.sub(r"(?iu)^технические\s+характеристики\s*", "", text).strip(" .;,-")
    text = re.sub(r"(?iu)^описание\s*[:.-]?\s*", "", text).strip(" .;,-")
    text = re.sub(r"(?iu)^емкость\s+для\s+отработанных\s+чернил\s+для\s*:?\s*", "", text).strip(" .;,-")
    text = re.sub(r"(?iu)^сменная\s+емкость\s+для\s+отработанных\s+чернил\.?\s*", "", text).strip(" .;,-")
    text = re.sub(r"(?iu)\bсменная\s+емкость\s+для\s+отработанных\s+чернил\.?\s*", "", text).strip(" .;,-")
    text = re.sub(
        r"(?iu)^информация\s+о\s+необходимости\s+замены\s+появится\s+на\s+панели\s+управлени[ея]\s+принтера\.?\s*",
        generic_second,
        text,
    ).strip(" .;,-")
    text = re.sub(
        r"(?iu)\bинформация\s+о\s+необходимости\s+замены\s+появится\s+на\s+панели\s+управлени[ея]\s+принтера\.?\s*",
        " " + generic_second,
        text,
    ).strip()

    generic_patterns = [
        r"(?iu)^(?:сменная\s+)?(?:емкость|ёмкость)\s+для\s+отработанных\s+чернил\.?$",
        r"(?iu)^оригинальная\s+(?:емкость|ёмкость)\s+для\s+отработанных\s+чернил(?:\s+[A-Z0-9-]+)?\.?$",
    ]

    # Пустое/общее описание: единый сильный шаблон.
    if (not text) or any(re.fullmatch(p, text) for p in generic_patterns):
        parts = [lead, generic_second]
        # Device sentence добавляем только если он реально полезнее tail и не дублирует его.
        if device_sentence and tail and len(device_sentence) > 80:
            parts.insert(1, device_sentence)
        return _clean_text(" ".join(parts))

    low = text.casefold().replace("ё", "е")
    base_low = base.casefold().replace("ё", "е")
    text_low = text.casefold().replace("ё", "е")
    generic_second_low = generic_second.casefold().replace("ё", "е")
    tail_low = _clean_text(tail).casefold().replace("ё", "е")
    device_low = _clean_text(device_sentence).casefold().replace("ё", "е")

    # Слабая однофразная база без контекста.
    if text_low.rstrip(".") == base_low.rstrip("."):
        parts = [lead, generic_second]
        if device_sentence and tail and len(device_sentence) > 80:
            parts.insert(1, device_sentence)
        return _clean_text(" ".join(parts))

    # Если уже есть нормальный device-list в params, а text тащит второй сырой список —
    # оставляем lead + device_sentence + generic_second.
    if device_sentence and (
        "surecolor" in low
        or "workforce" in low
        or "sc-" in low
        or "wf-" in low
        or "et-" in low
        or "для:" in low
    ):
        return _clean_text(f"{lead} {device_sentence} {generic_second}")

    # Короткий service-style текст усиливаем, но без дублей.
    if len(text) < 180:
        if text and not text.endswith("."):
            text += "."
        if generic_second_low in text.casefold().replace("ё", "е"):
            return _clean_text(f"{lead} {generic_second}")
        if tail_low and tail_low in text_low:
            return _clean_text(f"{lead} {generic_second}")
        if device_low and device_low in text_low:
            return _clean_text(f"{lead} {generic_second}")
        if text.casefold().replace("ё", "е").startswith(base_low):
            return _clean_text(f"{lead} {generic_second}")
        return _clean_text(f"{lead} {text}")

    if text and not text.endswith("."):
        text += "."
    return text

def clean_description_text(
    description: str,
    *,
    name: str = "",
    kind: str = "",
    vendor: str = "",
    model: str = "",
) -> str:
    """
    Возвращает очищенный plain-text с сохранёнными переводами строк.

    kind пока используется только как резерв под будущие точечные правила.
    """
    raw = str(description or "")
    if not raw:
        return ""

    text = _html_to_text(raw)
    text = _split_dense_labels(text)
    text = text.replace("\t", " ")
    text = _RE_MULTI_NL.sub("\n\n", text)

    # Сохраняем multiline-структуру для desc_extract.
    lines = [_clean_text(x) for x in text.split("\n")]
    lines = _cleanup_lines(lines, name=name, vendor=vendor, model=model)
    lines = _soft_wrap_lines(lines)

    # Точечные safe-правки под AkCent narrow-flow.
    cleaned: list[str] = []
    for line in lines:
        s = line
        s = s.replace(" ,", ",").replace(" .", ".")
        s = s.replace(" ;", ";").replace(" :", ":")
        s = s.replace("..", ".")
        s = s.replace("( ", "(").replace(" )", ")")
        s = norm_ws(s)
        if not s:
            continue
        cleaned.append(s)

    result = "\n".join(cleaned).strip()
    result = _RE_MULTI_NL.sub("\n\n", result)
    return result


# Alias: краткое имя для builder.
def clean_description(
    description: str,
    *,
    name: str = "",
    kind: str = "",
    vendor: str = "",
    model: str = "",
) -> str:
    return clean_description_text(
        description,
        name=name,
        kind=kind,
        vendor=vendor,
        model=model,
    )


# Возвращает готовые строки для desc_extract.py.
def description_lines(
    description: str,
    *,
    name: str = "",
    kind: str = "",
    vendor: str = "",
    model: str = "",
) -> list[str]:
    text = clean_description_text(
        description,
        name=name,
        kind=kind,
        vendor=vendor,
        model=model,
    )
    return [norm_ws(x) for x in text.split("\n") if norm_ws(x)]
