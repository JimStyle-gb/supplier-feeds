# -*- coding: utf-8 -*-
"""
CS Core — общее ядро для всех поставщиков.

В этом файле лежит "эталон CS":
- правила цены (4% + надбавки + хвост 900, но если цена невалидна/<=100 → 100)
- единый WhatsApp блок, HR, Оплата/Доставка
- единая сборка description + Характеристики
- единый keywords + хвост городов
- стабилизация форматирования (переводы строк, футер)
"""

# core v036_policy_ac_split_vendor_guard: AC отключаем split_params_for_chars по policy; гарантия не считается фразой; защита от vendor=тип/код

from __future__ import annotations


from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo
import os
import hashlib
import re

# Числа для парсинга float/int (вес/объём/габариты и т.п.)
_RE_NUM = re.compile(r"(\d+(?:[\.,]\d+)?)")
_RE_DIM_SEP = re.compile(r"(?:[xх×*/]|\bto\b)")  # 10x20, 10×20, 10х20, 10/20, 10 to 20
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from .keywords import build_keywords, CS_KEYWORDS_MAX_LEN
from .description import build_description, build_chars_block
from .pricing import compute_price, CS_PRICE_TIERS
from .meta import now_almaty, next_run_at_hour
from .validators import validate_cs_yml
from .util import norm_ws, safe_int, _truncate_text
from .writer import (
    xml_escape_text,
    xml_escape_attr,
    bool_to_xml,
    xml_escape,
    make_header,
    make_footer,
    ensure_footer_spacing,
    make_feed_meta,
    build_cs_feed_xml,
    build_cs_feed_xml_raw,
    write_if_changed,
)

# Back-compat guard: адаптеры импортируют OfferOut из cs.core
# Если вы случайно удалили OfferOut — верните полный core.py.

# Fallback: если кто-то случайно удалит импорт, всё равно будет лимит
CS_KEYWORDS_MAX_LEN_FALLBACK = 380
def _dedup_keep_order(items: list[str]) -> list[str]:
    """CS: дедупликация со стабильным порядком (без сортировки)."""
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if not x:
            continue
        k = x.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def _cs_norm_url(u: str) -> str:
    # CS: нормализуем URL картинок (пробелы ломают загрузку)
    return (u or "").replace(" ", "%20").replace("\t", "%20")

# Регексы для fix_text (компилируем один раз)
_RE_SHUKO = re.compile(r"\bShuko\b", flags=re.IGNORECASE)
_RE_MULTI_NL = re.compile(r"\n{3,}")
_RE_MULTI_SP = re.compile(r"[ \u00a0]{2,}")
# Регексы: десятичная запятая внутри чисел (2,44 -> 2.44) — включается через env
_RE_DECIMAL_COMMA = re.compile(r"(?<=\d),(?=\d)")
_RE_MULTI_COMMA = re.compile(r"\s*,\s*,+")
# Регексы: мусорные имена параметров (цифры/числа/Normal) — включается через env
_RE_TRASH_PARAM_NAME_NUM = re.compile(r"^[0-9][0-9\s\.,]*$")

# Флаги поведения (по умолчанию выключены, чтобы не ломать AlStyle)
CS_FIX_KEYWORDS_DECIMAL_COMMA = (os.getenv("CS_FIX_KEYWORDS_DECIMAL_COMMA", "0") or "0").strip() == "1"
CS_DROP_TRASH_PARAM_NAMES = (os.getenv("CS_DROP_TRASH_PARAM_NAMES", "0") or "0").strip() == "1"
CS_FIX_KEYWORDS_MULTI_COMMA = (os.getenv("CS_FIX_KEYWORDS_MULTI_COMMA", "0") or "0").strip() == "1"
CS_DESC_ADD_BRIEF = False  # CS: не генерируем "Кратко"
CS_DESC_BRIEF_MIN_FIELDS = 0  # CS: не используется
# Дефолты (используются адаптерами)
OUTPUT_ENCODING_DEFAULT = "utf-8"
CURRENCY_ID_DEFAULT = "KZT"
ALMATY_TZ = "Asia/Almaty"

# --- CS: Совместимость (только безопасно и только где нужно) ---
_COMPAT_PARAM_NAME = "Совместимость"
_COMPAT_ALIAS_NAMES = {
    "совместимость с моделями",
    "совместимость с принтерами",
    "совместимые модели",
    "для принтеров",
    "для принтера",
    "принтер",
    "принтеры",
    "применение",
    "подходит для",
    "совместимость",
}

_PARTNUMBER_PARAM_NAMES = {
    "партномер",
    "partnumber",
    "part number",
    "part no",
    "pn",
    "код производителя",
    # VTT/другие: часто приходят так
    "oem-номер",
    "oem номер",
    "каталожный номер",
    "кат. номер",
    "каталожный №",
    "кат. №",
}

# Типы, где совместимость реально уместна (расходники)
_COMPAT_TYPE_HINTS = (
    "картридж",
    "тонер",
    "тонер-картридж",
    "драм",
    "драм-юнит",
    "драм-картридж",
    "фотобарабан",
    "барабан",
    "чернила",
    "печатающая головка",
    "девелопер",
    "термопленка",
)

def _cs_is_consumable(name_full: str, params: list[tuple[str, str]]) -> bool:
    """
    Core больше не определяет "это расходник или нет" по name/params.

    По правилу CS:
    - любые supplier-specific heuristics по расходникам живут в adapter/compat.py;
    - shared core не должен чинить raw и не должен ветвиться по логике "если это расходник".

    Функция оставлена только для backward-safe совместимости со старым кодом.
    """
    _ = name_full
    _ = params
    return False

def _cs_is_consumable_code_token(tok: str) -> bool:
    t = (tok or "").strip().strip(" ,;./()[]{}").upper()
    if not t:
        return False
    # чистые числа 6–9 знаков
    if re.fullmatch(r"\d{6,9}", t):
        return True
    # Xerox: 106R02773 / 113R00780 / 008R13041
    if re.fullmatch(r"\d{3}R\d{5}", t):
        return True
    # Epson: C13T00R140 / C13T66414A и т.п.
    if re.fullmatch(r"C\d{2}T[0-9A-Z]{5,8}", t):
        return True

    # Canon: C-EXV34 / NPG-59 / GPR-53
    if re.fullmatch(r"C-?EXV\d{1,3}", t.replace(" ", "").replace("-", "")):
        return True
    if re.fullmatch(r"(?:NPG|GPR)-?\d{1,3}", t.replace(" ", "")):
        return True

    # HP ink: 3ED77A / 1VK08A
    if re.fullmatch(r"\d[A-Z]{2}\d{2}[A-Z]{1,2}", t):
        return True
    # Canon OEM: 0287C001 / 0491C001AA и т.п.
    if re.fullmatch(r"\d{4}[A-Z]\d{3}[A-Z]{0,2}", t):
        return True
    # HP: CF283A / CE285A / W1106A и т.п.
    if re.fullmatch(r"(?:CF|CE|CB|CC|Q|W)\d{3,5}[A-Z]{0,3}", t):
        return True
    # Canon T-коды (T06/T07/...) — код расходника (важно: не путать с T3000/T5200 и т.п.)
    if re.fullmatch(r"T0\d", t):
        return True

    # ML-коды: ML-1710D3 / ML-1210D3 / ML-D1630A
    if re.fullmatch(r"ML-?\d{3,5}D\d{1,2}", t):
        return True
    if re.fullmatch(r"ML-?D\d{3,5}[A-Z]{0,2}", t):
        return True

    # HP/Canon: C7115A / C9730A / C8543X (но не C11... — это SKU техники)
    if re.fullmatch(r"C\d{4}[A-Z]{0,3}", t) and (not t.startswith("C11")):
        if re.fullmatch(r"C\d{4}", t):
            return False
        if re.fullmatch(r"C\d{4}(?:DN|DW|DWF|FDN|FDW|MFP)$", t):
            return False
        return True
    # Kyocera: TK-1150 / TK1150
    if re.fullmatch(r"TK-?\d{3,5}[A-Z]{0,3}", t):
        return True
    # Brother: TN-2375 / TN2375 / DR-2335 / DR2335
    if re.fullmatch(r"(?:TN|DR)-?\d{3,5}[A-Z]{0,3}", t):
        return True
    # Samsung: MLT-D111S / CLT-K404S
    if re.fullmatch(r"(?:MLT|CLT)-[A-Z]?\d{3,5}[A-Z]{0,3}", t):
        return True
    # Canon/HP short codes: 710H / 051H / 056H / 126A / 435A
    if re.fullmatch(r"\d{3,4}[AHX]", t):
        return True
    return False

def _cs_looks_like_consumable_code_list(s: str) -> bool:
    """
    Core не должен угадывать списки кодов расходников в тексте.
    Это обязанность supplier-layer.

    Оставлено как backward-safe no-op.
    """
    _ = s
    return False

def _cs_expand_consumable_code_ranges(s: str) -> str:
    """Раскрывает диапазоны кодов вида T0481–T0486 → T0481 T0482 ... T0486.
    Делает это только для безопасных коротких диапазонов (<=20).
    """
    if not s:
        return ""
    # унифицируем тире
    t = s.replace("–", "-").replace("—", "-")
    # Пример: T0481- T0486
    def _repl(m: re.Match) -> str:
        a = (m.group("a") or "").upper()
        b = (m.group("b") or "").upper()
        # отделяем буквенную и цифровую часть
        ma = re.match(r"^([A-Z]{1,3})(\d{3,6})$", a)
        mb = re.match(r"^([A-Z]{1,3})(\d{3,6})$", b)
        if not ma or not mb:
            return m.group(0)
        p1, n1 = ma.group(1), ma.group(2)
        p2, n2 = mb.group(1), mb.group(2)
        if p1 != p2:
            return m.group(0)
        if len(n1) != len(n2):
            return m.group(0)
        i1 = int(n1)
        i2 = int(n2)
        if i2 < i1:
            return m.group(0)
        if (i2 - i1) > 20:
            return m.group(0)
        width = len(n1)
        out = [f"{p1}{str(i).zfill(width)}" for i in range(i1, i2 + 1)]
        return " " + " ".join(out) + " "
    t = re.sub(r"(?i)\b(?P<a>[A-Z]{1,3}\d{3,6})\s*-\s*(?P<b>[A-Z]{1,3}\d{3,6})\b", _repl, t)
    return t

def _cs_expand_grouped_consumable_codes(s: str) -> str:
    if not s:
        return ""
    # CS: некоторые поставщики префиксуют коды (NV-/NVP-/EP- и т.п.) — убираем префикс только перед кодами
    s = re.sub(
        r"(?i)\b(?:NV|NVP|EP|EPR|EPC)-(?=(?:\d{3}R\d{5}|C\d{2}T|(?:CF|CE|CB|CC|Q|W)\d{3,5}|(?:CZ|CN)\d{3}|T\d{4,5}|TK-?\d{2,5}|(?:TN|DR)-?\d{2,5}|(?:MLT|CLT)-[A-Z]?\d{3,5}))",
        "",
        s,
    )
    s = _cs_expand_consumable_code_ranges(s)
    # CS: вариант вида "CF283A/285A" → "CF283A CF285A"
    def _repl2(m: re.Match) -> str:
        pfx = (m.group("pfx") or "").upper()
        n1 = m.group("n1") or ""
        s1 = (m.group("suf1") or "").upper()
        n2 = m.group("n2") or ""
        s2 = (m.group("suf2") or "").upper()
        return " " + f"{pfx}{n1}{s1} {pfx}{n2}{s2}" + " "
    s = re.sub(
        r"(?i)\b(?P<pfx>(?:CF|CE|CB|CC|Q|W))(?P<n1>\d{3,5})(?P<suf1>[A-Z]{1,3})/(?P<n2>\d{3,5})(?P<suf2>[A-Z]{1,3})\b",
        _repl2,
        s,
    )
    def _repl(m: re.Match) -> str:
        pfx = (m.group("pfx") or "").upper()
        body = m.group("body") or ""
        suf = (m.group("suf") or "").upper()
        nums = [x for x in body.split("/") if x]
        out = [f"{pfx}{n}{suf}" for n in nums]
        return " " + " ".join(out) + " "
    s = _RE_CODE_GROUPED_PREFIX.sub(_repl, s)
    s = _RE_CODE_GROUPED_GENERIC.sub(_repl, s)
    return s
# CS: извлекаем коды расходников (в исходном порядке) из текста. Никаких моделей техники сюда не пускаем.
def _cs_extract_consumable_codes_ordered(text: str, allow_short_3dig: bool = True) -> list[str]:
    """
    Core не извлекает коды расходников из текста.
    Это делает supplier-layer в raw.

    Оставлено как backward-safe no-op.
    """
    _ = text
    _ = allow_short_3dig
    return []

def _cs_strip_consumable_codes_from_text(text: str, allow_short_3dig: bool = True) -> str:
    """
    Core не чистит текст от кодов расходников.
    Если supplier-layer отдал raw с такими хвостами — это ошибка адаптера.

    Оставлено как backward-safe pass-through.
    """
    _ = allow_short_3dig
    return norm_ws(text)

def _cs_clean_compat_value(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return ""
    # HTML → текст
    s = _COMPAT_HTML_TAG_RE.sub(" ", s)
    s = norm_ws(s)

    # убираем маркетинг/служебку
    s = re.sub(r"(?i)\bбез\s+чипа\b", " ", s)

    s = re.sub(r"(?i)\b(?:новинка|распродажа|акция|хит|sale|new)\b", " ", s)

    # убираем префиксы/служебные слова, которые часто прилетают от поставщиков
    s = re.sub(r"(?i)\bприменени[ея]\s*[:\-]\s*", " ", s)
    s = re.sub(r"(?i)\bдля\s+принтер[а-я]*\b\s*[:\-]?\s*", " ", s)

    # если в строке есть явный хинт бренда/линейки — оставляем хвост с первой модели (режем "Применение: ...")
    m = _RE_COMPAT_DEVICE_HINT.search(s)
    if m and m.start() > 0:
        s = s[m.start():]

    # из совместимости вырезаем цвет/страницы/единицы — это НЕ модели устройств
    s = re.sub(r"(?i)\b(?:ч[её]рн\w*|голуб\w*|ж[её]лт\w*|желт\w*|магент\w*|пурпур\w*|сер\w*|цветн\w*|пигмент\w*)\b", " ", s)
    s = re.sub(r"(?i)\b(?:black|cyan|magenta|yellow|grey|gray)\b", " ", s)
    s = re.sub(r"(?i)\b(?:LC|LM|LK|MBK|PBK|Bk|C|M|Y|K)\b", " ", s)
    s = re.sub(r"(?i)\b\d+\s*(?:стр\.?|страниц\w*|pages?)\b", " ", s)

    s = norm_ws(s)

    # режем типовые "Ресурс: 1600 стр" / "yield 7.3K" и т.п.
    s = re.sub(r"(?i)\bресурс\b\s*[:\-]?\s*\d+(?:[.,]\d+)?\s*(?:k|к|стр\.?|страниц\w*|pages?)\b", " ", s)
    s = re.sub(r"(?i)\byield\b\s*[:\-]?\s*\d+(?:[.,]\d+)?\s*k\b", " ", s)

    # отдельный кейс вида 29млХ3шт (убираем полностью)
    s = re.sub(r"(?i)\b\d+\s*(?:мл|ml)\s*[xхXХ]\s*\d+\s*(?:шт|pcs|pieces)\b", " ", s)

    # чистим по фрагментам (важно: запятая-разделитель, но НЕ десятичная 2,4K)
    parts = re.split(r"[\n;]+|\s*(?:(?<!\d),|,(?!\d))\s*", s)
    cleaned: list[str] = []
    for p in parts:
        p = _clean_compat_fragment(p)
        p = norm_ws(p).strip(" ,;/:-")
        if not p:
            continue
        if not _is_valid_compat_fragment(p):
            continue
        cleaned.append(p)

    cleaned = _dedup_keep_order(cleaned)
    out = ", ".join(cleaned).strip()
    out = norm_ws(out)
    if not out:
        return ""
    # безопасность по длине (дальше всё равно тримится до 260 в clean_params)
    if len(out) > 600:
        return ""
    return out





def _cs_trim_compat_to_max(v: str, max_len: int = 260) -> str:
    """Обрезает совместимость безопасно, не разрезая модель на середине.
    Стараемся обрезать по последней запятой/точке с запятой/пробелу в пределах max_len,
    затем удаляем возможные обрывки вида '/P1' на конце.
    """
    s = (v or "").strip()
    if not s:
        return ""
    if len(s) <= max_len:
        return s

    cut = s[:max_len]
    # Предпочитаем резать по разделителю списка моделей
    pos = cut.rfind(", ")
    if pos >= 40:
        cut = cut[:pos]
    else:
        pos = cut.rfind("; ")
        if pos >= 40:
            cut = cut[:pos]
        else:
            pos = cut.rfind(" ")
            if pos >= 40:
                cut = cut[:pos]

    cut = cut.rstrip(" ,;/.-")
    # Удаляем короткий обрывок после '/', если он начинается с буквы и слишком короткий (например '/P1')
    cut = re.sub(r"/(?=[A-Za-zА-Яа-я])[A-Za-zА-Яа-я0-9]{1,2}$", "", cut).rstrip(" ,;/.-")
    # И короткий обрывок после запятой/пробела (например ', M1')
    cut = re.sub(r"(?:,|\s)+(?=[A-Za-zА-Яа-я])[A-Za-zА-Яа-я0-9]{1,2}$", "", cut).rstrip(" ,;/.-")
    return cut


def _cs_looks_like_device_models(s: str) -> bool:
    s0 = _cs_clean_compat_value(s)
    if not s0:
        return False

    # CS: если строка в основном состоит из кодов расходников — это НЕ модели устройств
    toks = [t for t in re.split(r"[\s,/;]+", s0) if t]
    code_cnt = sum(1 for t in toks if _cs_is_consumable_code_token(t))
    if code_cnt >= 2 and code_cnt >= max(2, int(len(toks) * 0.6)):
        return False

    # Нужны подсказки бренда/линейки (иначе часто ловим "ресурс/цвет/формат")
    if not _RE_COMPAT_DEVICE_HINT.search(s0):
        return False

    # И хотя бы 1 токен, похожий на модель устройства (не код расходника)
    tokens = _RE_MODEL_TOKEN.findall(s0)
    device_tokens = [t for t in tokens if (not _cs_is_consumable_code_token(t)) and (not re.fullmatch(r"\d{3}", t))]
    if len(device_tokens) >= 1:
        return True

    # CS: серии с пробелом/дефисом (например: Epson L 700 / WF 2810, Ricoh MP 2014, Canon LBP-312x, FC-2xx и т.п.)
    if re.search(r"(?i)\b(?:l|wf|mp|sp|mx|mf|lbp|fc|pc|i[-\s]?r|hl|dcp|mfc|scx|ml|clp|clx)\s*[-\s]*\d{1,5}(?:x{1,2})?[A-ZА-Я]{0,3}\b", s0):
        return True

    # CS: 3-значные модели устройств (WorkCentre 315/320 и т.п.) — допускаем только если есть серия/линейка
    if re.search(r"(?i)\b(?:workcentre|phaser|versalink|altalink|docucolor|color|taskalfa|ecosys|bizhub|laserjet|deskjet|designjet|officejet|pagewide|imagerunner|imageclass|aficio|\bmp\b|\bsp\b)\b", s0):
        if re.search(r"\b\d{3}\b", s0):
            return True

    # CS v031: Xerox серии, где указаны только 3-значные номера (205/210/215; 550/560/570 и т.п.)
    if re.search(r"(?i)\bxerox\b", s0):
        nums = re.findall(r"\b\d{3}\b", s0)
        if len(nums) >= 2:
            return True

    return False



def _cs_extract_compat_candidate(text: str) -> str:
    t = norm_ws(text).replace("\xa0", " ").strip()
    if not t:
        return ""

    brands = [
        "Xerox",
        "Canon",
        "HP",
        "Kyocera",
        "Ricoh",
        "Konica",
        "Minolta",
        "Epson",
        "Brother",
        "Samsung",
        "Pantum",
        "Oki",
        "Lexmark",
        "Sharp",
        "Toshiba",
    ]

    def _hint(prefix: str) -> str:
        p = (prefix or "")
        for b in brands:
            if re.search(rf"(?i)\b{re.escape(b)}\b", p):
                return b
        m = re.search(
            r"(?i)\b(laserjet|deskjet|designjet|workcentre|phaser|versalink|altalink|taskalfa|ecosys|bizhub)\b",
            p,
        )
        return (m.group(1) if m else "")

    patterns = [
        r"(?i)\bдля\s+([^\n\r]{3,240})",
        r"(?i)\bподходит\s+для\s+([^\n\r]{3,240})",
        r"(?i)\bиспользуется\s+в\s+(?:принтерах|мфу|устройствах)(?:\s+серий)?\s+([^\n\r]{3,240})",
        r"(?i)\bприменяется\s+в\s+(?:принтерах|мфу|устройствах)(?:\s+серий)?\s+([^\n\r]{3,240})",
        r"(?i)\bсовместим\w*\s+с\s+([^\n\r]{3,240})",
        r"(?i)\bсовместимость\b\s*(?:устройства?|модели)\s+([^\n\r]{3,240})",
        r"(?i)\bсовместимость\b\s*[:\-–—]?\s+([^\n\r]{3,240})",
        r"(?i)\bfor\s+([^\n\r]{3,240})",
    ]

    for pat in patterns:
        m = re.search(pat, t)
        if not m:
            continue
        cand = m.group(1)
        cand = re.split(
            r"(?:(?:\s*[\(\[\{])|(?:\s+[—-]\s+)|(?:\s*\.|\s*!|\s*\?))",
            cand,
            maxsplit=1,
        )[0]
        cand = _cs_clean_compat_value(cand)
        if not cand:
            continue

        hint = _hint(t[: m.start()])
        if hint and (not re.search(rf"(?i)\b{re.escape(hint)}\b", cand)):
            cand = _cs_clean_compat_value(f"{hint} {cand}")

        return cand

    return ""

def _cs_merge_compat_values(vals: list[str]) -> str:
    parts: list[str] = []
    for v in vals:
        v = _cs_clean_compat_value(v)
        if not v:
            continue
        # дробим по запятым/точкам с запятой/переводам строк
        for p in re.split(r"[\n;]+|\s*(?:(?<!\d),|,(?!\d))\s*", v):  # запятая-разделитель, но НЕ десятичная 2,4K
            p = _cs_clean_compat_value(p)
            if not p:
                continue
            parts.append(p)
    parts = _dedup_keep_order(parts)
    out = ", ".join(parts).strip()
    if len(out) > 600:
        return ""
    return out


def ensure_compatibility_param(params: list[tuple[str, str]], name_full: str, native_desc: str) -> None:
    """
    Adapter-first:
    - "Коды", "Совместимость", device-model heuristics и cleanup совместимости
      формируются только в supplier-layer;
    - shared core ничего не создаёт и не чинит по совместимости.

    Оставлено как backward-safe no-op.
    """
    _ = params
    _ = name_full
    _ = native_desc
    return

def normalize_offer_name(name: str) -> str:
    # CS: лёгкая типографика имени (без изменения смысла)
    s = norm_ws(name)
    # CS: орфография (частая опечатка)
    s = re.sub(r"(?i)\bmaintance\b", "Maintenance", s)
    if not s:
        return ""
    # "дляPantum" -> "для Pantum"
    s = re.sub(r"\bдля(?=[A-ZА-Я])", "для ", s)
    # "(аналогDL-5120)" -> "(аналог DL-5120)" (только если далее заглавная/цифра)
    s = re.sub(r"(?i)\bаналог(?=[A-ZА-Я0-9])", "аналог ", s)
    # двойной слэш в моделях
    s = s.replace("//", "/")
    # ",Color" -> ", Color"
    s = re.sub(r"(?i),\s*color\b", ", Color", s)
    # убрать пробелы перед знаками
    s = re.sub(r"\s+([,.;:!?])", r"\1", s)
    # CS: добавить пробел после запятой/точки с запятой (если дальше буква)
    s = re.sub(r",(?=[A-Za-zА-Яа-яЁё])", ", ", s)
    s = re.sub(r";(?=[A-Za-zА-Яа-яЁё])", "; ", s)
    # убрать лишние пробелы внутри скобок
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    # хвостовая запятая
    s = re.sub(r",\s*$", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    # CS: чинить смешение кириллицы/латиницы в имени
    s = fix_mixed_cyr_lat(s)
    return s


_RE_COLOR_TOKENS = [
    ("Голубой", re.compile(r"\bcyan\b", re.IGNORECASE)),
    ("Пурпурный", re.compile(r"\bmagenta\b", re.IGNORECASE)),
    ("Желтый", re.compile(r"\byellow\b", re.IGNORECASE)),
    ("Черный", re.compile(r"\bblack\b", re.IGNORECASE)),
    ("Серый", re.compile(r"\bgr(?:a|e)y\b", re.IGNORECASE)),
    # RU
    ("Голубой", re.compile(r"\bголуб(?:ой|ая|ое|ые|ого|ому|ым|ых)\b", re.IGNORECASE)),
    ("Пурпурный", re.compile(r"\bпурпурн(?:ый|ая|ое|ые|ого|ому|ым|ых)\b", re.IGNORECASE)),
    ("Пурпурный", re.compile(r"\bмаджент(?:а|овый|овая|овое|овые)\b", re.IGNORECASE)),
    ("Пурпурный", re.compile(r"\bмалин(?:овый|овая|овое|овые|ого|ому|ым|ых)\b", re.IGNORECASE)),
    ("Желтый", re.compile(r"\bжелт(?:ый|ая|ое|ые|ого|ому|ым|ых)\b", re.IGNORECASE)),
    ("Черный", re.compile(r"\bчерн(?:ый|ая|ое|ые|ого|ому|ым|ых)\b", re.IGNORECASE)),
    ("Серый", re.compile(r"\bсер(?:ый|ая|ое|ые|ого|ому|ым|ых)\b", re.IGNORECASE)),
]

_RE_HI_BLACK = re.compile(r"\bhi[-\s]?black\b", re.IGNORECASE)


def _compat_fragments(s: str) -> list[str]:
    # CS: разбиваем строку совместимости на фрагменты (стабильно)
    s = norm_ws(s)
    if not s:
        return []
    # унифицируем разделители
    s = s.replace(";", ",").replace("|", ",")
    parts = [norm_ws(p) for p in s.split(",")]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        if not p:
            continue
        # нормализуем пробелы вокруг слэшей, чтобы одинаковые списки схлопывались
        p = _COMPAT_SLASH_SPACES_RE.sub("/", p)
        p = _COMPAT_MULTI_SLASH_RE.sub("/", p)
        p = norm_ws(p).strip(" ,;/:-")
        if not p:
            continue
        key = p.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


def _get_param_value(params: list[tuple[str, str]], key_name: str) -> str:
    kn = key_name.casefold()
    for k, v in params:
        if norm_ws(k).casefold() == kn:
            return norm_ws(v)
    return ""


# CS: ключи, где может жить совместимость (в разных поставщиках)
_COMPAT_KEYS = ("Совместимость", "Совместимые модели", "Для", "Применение")

# CS: фильтрация мусора в совместимости (цвет/объём/служебные слова)
_COMPAT_UNIT_RE = re.compile(r"^\s*(?:\d+\s*(?:[*xх]\s*)\d+|\d+(?:[.,]\d+)?)\s*(?:мл|ml)\b", re.I)
_COMPAT_PARENS_UNIT_RE = re.compile(r"\(\s*(?:\d+\s*(?:[*xх]\s*)\d+|\d+(?:[.,]\d+)?)\s*(?:мл|ml)\s*\)", re.I)
# CS: вычищаем единицы/объём и служебные слова внутри фрагмента (если они встречаются вместе с моделью)
_COMPAT_UNIT_ANY_RE = re.compile(r"(?i)\b(?:\d+\s*(?:[*xх]\s*)\d+|\d+(?:[.,]\d+)?)\s*(?:мл|ml)\b")
_COMPAT_SKIP_ANY_RE = re.compile(r"(?i)\b(?:совместим\w*|compatible|original|оригинал)\b")
# CS: слова/форматы/ОС, которые не должны попадать в "Совместимость" (часто прилетают из ТТХ)
_COMPAT_PAPER_OS_WORD_RE = re.compile(
    r"(?i)\b(?:letter|legal|a[4-6]|b5|c6|dl|no\.\s*10|windows(?:\s*\d+)?|mac\s*os|linux|android|ios|"
    r"конверт\w*|дуплекс|формат|выбрать\s*формат|paper\s*tray\s*capacity)\b"
)
# CS: размеры/соотношения сторон — тоже мусор для совместимости
_COMPAT_DIM_TOKEN_RE = re.compile(r"(?i)\b\d+\s*[xх]\s*\d+\b|\b\d+\s*см\b|\b16:9\b")
# CS: шумовые слова, которые иногда попадают в совместимость (цвета/маркетинг/описания)
_COMPAT_NOISE_IN_COMPAT_RE = re.compile(
    r"(?i)\b(?:euro\s*print|отработанн\w*|чернил\w*|ink|pigment|dye|"
    r"cyan|magenta|yellow|black|grey|gray|matt\s*black|photo\s*black|photoblack|light\s*cyan|light\s*magenta)\b"
)


# CS: мусор в скобках (ресурс/комплект/обрезанные хвосты)
_COMPAT_PARENS_YIELD_PACK_RE = re.compile(r"(?i)\([^)]*(?:\b\d+\s*[kк]\b|\b\d+\s*шт\b|pcs|pieces|yield|страниц|стр\.?|ресурс|увелич)[^)]*\)")
_COMPAT_YIELD_ANY_RE = re.compile(r"(?i)\b\d+\s*[kк]\b")
_COMPAT_PACK_ANY_RE = re.compile(r"(?i)\b\d+\s*шт\b|\b\d+\s*pcs\b|\b\d+\s*pieces\b")
_COMPAT_SLASH_SPACES_RE = re.compile(r"\s*/\s*")
_COMPAT_MULTI_SLASH_RE = re.compile(r"/{2,}")
_COMPAT_HYPHEN_MODEL_RE = re.compile(r"(?<=\D)-(?=\d)")

_COMPAT_COLOR_ONLY_RE = re.compile(
    r"^\s*(?:cyan|magenta|yellow|black|grey|gray|matt\s*black|photo\s*black|photoblack|light\s*cyan|light\s*magenta|"
    r"ч[её]рн(?:ый|ая|ое|ые)?|син(?:ий|яя|ее|ие)?|голуб(?:ой|ая|ое|ые)?|желт(?:ый|ая|ое|ые)?|"
    r"пурпур(?:ный|ная|ное|ные)?|магент(?:а|ы)?|сер(?:ый|ая|ое|ые)?)\s*$",
    re.I,
)
_COMPAT_SKIP_WORD_RE = re.compile(r"^\s*(?:совместим\w*|compatible|original|оригинал)\s*$", re.I)
_COMPAT_NUM_ONLY_RE = re.compile(r"^\s*\d+(?:[.,]\d+)?\s*$")
_COMPAT_NO_CODE_RE = re.compile(r"^\s*(?:№|#)\s*\d{2,}\s*$")


def _clean_compat_fragment(f: str) -> str:
    # CS: чистим один фрагмент совместимости (безопасно)
    f = norm_ws(f)
    if not f:
        return ""

    # нормализуем слэши + модельные дефисы (KM-1620 -> KM 1620)
    f = _COMPAT_SLASH_SPACES_RE.sub("/", f)
    f = _COMPAT_MULTI_SLASH_RE.sub("/", f)
    f = _COMPAT_HYPHEN_MODEL_RE.sub(" ", f)

    # выкидываем цвет/объём/служебные слова
    f = _COMPAT_PARENS_UNIT_RE.sub("", f)
    f = _COMPAT_UNIT_ANY_RE.sub("", f)
    f = _COMPAT_SKIP_ANY_RE.sub("", f)

    # дополнительно: ресурс/комплект в совместимости — мусор
    if CS_COMPAT_CLEAN_YIELD_PACK:
        f = _COMPAT_PARENS_YIELD_PACK_RE.sub("", f)
        f = _COMPAT_YIELD_ANY_RE.sub("", f)
        f = _COMPAT_PACK_ANY_RE.sub("", f)

    # CS: форматы бумаги / ОС / размеры — не должны жить в "Совместимость"
    if CS_COMPAT_CLEAN_PAPER_OS_DIM:
        f = _COMPAT_PAPER_OS_WORD_RE.sub("", f)
        f = _COMPAT_DIM_TOKEN_RE.sub("", f)

    # CS: шумовые слова (цвета/маркетинг/описания) — тоже режем
    if CS_COMPAT_CLEAN_NOISE_WORDS:
        f = _COMPAT_NOISE_IN_COMPAT_RE.sub("", f)

    # если скобки сломаны (обрезан хвост) — режем с последней '('
    if f.count("(") != f.count(")"):
        last = f.rfind("(")
        if last != -1:
            f = f[:last]
        f = f.replace(")", "")

    f = norm_ws(f).strip(" ,;/:-")

    # в совместимости скобки не нужны — убираем остатки, чтобы не было "битых" хвостов
    if "(" in f or ")" in f:
        f = f.replace("(", " ").replace(")", " ")
        f = norm_ws(f).strip(" ,;/:-")

    # CS: иногда поставщик повторяет целый список дважды — режем повтор (часто у NVPrint)
    if CS_COMPAT_CLEAN_REPEAT_BLOCKS and len(f) >= 80:
        f_low = f.casefold()
        pfx = norm_ws(f[:60]).casefold()
        if len(pfx) >= 24:
            pos = f_low.find(pfx, len(pfx))
            if pos != -1:
                f = f[:pos]
                f = norm_ws(f).strip(" ,;/:-")

    # убираем дубли внутри "A/B/C" (частая грязь у поставщиков)
    if "/" in f:
        parts = [norm_ws(x) for x in f.split("/") if norm_ws(x)]
        out: list[str] = []
        seen: set[str] = set()
        for x in parts:
            if CS_COMPAT_CLEAN_PAPER_OS_DIM:
                x = _COMPAT_PAPER_OS_WORD_RE.sub("", x)
                x = _COMPAT_DIM_TOKEN_RE.sub("", x)
            if CS_COMPAT_CLEAN_NOISE_WORDS:
                x = _COMPAT_NOISE_IN_COMPAT_RE.sub("", x)
            x = norm_ws(x).strip(" ,;/:-")
            if not x:
                continue
            k = x.casefold()
            if k in seen:
                continue
            seen.add(k)
            out.append(x)
        f = "/".join(out)

    return f

def _is_valid_compat_fragment(f: str) -> bool:
    """CS: проверка, что фрагмент похож на совместимость (модель/список моделей), а не мусор."""
    f = norm_ws(f)
    if not f:
        return False

    # чисто цвет/служебные слова —
    if _COMPAT_COLOR_ONLY_RE.match(f) or _COMPAT_SKIP_WORD_RE.match(f):
        return False

    # чисто единицы/объём —
    if _COMPAT_UNIT_RE.match(f):
        return False

    # голые числа/номера —
    if _COMPAT_NUM_ONLY_RE.match(f) or _COMPAT_NO_CODE_RE.match(f):
        return False

    # форматы бумаги / ОС / размеры — не совместимость принтеров
    if CS_COMPAT_CLEAN_PAPER_OS_DIM:
        if _COMPAT_PAPER_OS_WORD_RE.search(f) or _COMPAT_DIM_TOKEN_RE.search(f):
            return False

    # должна быть цифра (модели почти всегда с цифрами)
    if not re.search(r"\d", f):
        return False

    # и буква (чтобы не ловить голые числа)
    if not re.search(r"[A-Za-zА-Яа-я]", f):
        return False

    # слишком коротко — почти наверняка мусор
    if len(f) < 4:
        return False

    return True

_COMPAT_MODEL_TOKEN_RE = re.compile(r"(?i)\b[A-ZА-Я]{1,6}\s*\d{2,5}[A-ZА-Я]?\b")
_COMPAT_TEXT_SPLIT_RE = re.compile(r"[\n\r\.\!\?]+")
_COMPAT_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _shorten_smart_name(name: str, params: list[tuple[str, str]], max_len: int) -> str:
    # CS: Универсально — делаем короткое имя без потери кода/смысла.
    # Полная совместимость остаётся в param "Совместимость" (обогащённая из name/desc/params).
    name = norm_ws(name)
    if len(name) <= max_len:
        return name

    compat_full = _get_param_value(params, "Совместимость")
    frags = _compat_fragments(compat_full)

    # Пытаемся выделить "префикс" до "для ..."
    low = name.casefold()
    pfx = name
    tail_sep = ""
    if " для " in low:
        i = low.find(" для ")
        pfx = norm_ws(name[:i])
        tail_sep = " для "
    elif "для " in low:
        # на случай если без пробелов
        i = low.find("для ")
        pfx = norm_ws(name[:i].rstrip())
        tail_sep = " для "

    # Если нет compat в params — берём хвост из name после "для"
    if not frags and tail_sep:
        tail = norm_ws(name[len(pfx) + len(tail_sep):])
        frags = _compat_fragments(tail)

    # Собираем короткую совместимость: уменьшаем число фрагментов, пока не влезет
    # Начинаем с 6, дальше 5..1
    max_items = 6
    while max_items >= 1:
        short = ", ".join(frags[:max_items]) if frags else ""
        if short:
            cand = f"{pfx}{tail_sep}{short} и др."
        else:
            cand = f"{pfx}"
        if len(cand) <= max_len:
            return cand
        max_items -= 1

    # Фоллбэк: просто режем по границе и добавляем "…"
    return _truncate_text(name, max_len, suffix=" и др.")




# Лимиты (по умолчанию):
# - <name> держим коротким и читаемым (150 по решению пользователя)
CS_NAME_MAX_LEN = int((os.getenv("CS_NAME_MAX_LEN", "150") or "150").strip() or "150")

CS_COMPAT_CLEAN_YIELD_PACK = (os.getenv("CS_COMPAT_CLEAN_YIELD_PACK", "1") or "1").strip().lower() not in ("0", "false", "no")
CS_COMPAT_CLEAN_PAPER_OS_DIM = (os.getenv("CS_COMPAT_CLEAN_PAPER_OS_DIM", "1") or "1").strip().lower() not in ("0", "false", "no")
CS_COMPAT_CLEAN_NOISE_WORDS = (os.getenv("CS_COMPAT_CLEAN_NOISE_WORDS", "1") or "1").strip().lower() not in ("0", "false", "no")
CS_COMPAT_CLEAN_REPEAT_BLOCKS = (os.getenv("CS_COMPAT_CLEAN_REPEAT_BLOCKS", "1") or "1").strip().lower() not in ("0", "false", "no")

# Заглушка картинки, если у оффера нет фото (можно переопределить env CS_PICTURE_PLACEHOLDER_URL)
CS_PICTURE_PLACEHOLDER_URL = (os.getenv("CS_PICTURE_PLACEHOLDER_URL") or "https://placehold.co/800x800/png?text=No+Photo").strip()

def enforce_name_policy(oid: str, name: str, params: list[tuple[str, str]]) -> str:
    # CS: глобальная политика имени — одинаково для всех поставщиков
    name = norm_ws(name)
    if not name:
        return ""
    if len(name) <= CS_NAME_MAX_LEN:
        return name

    # Универсальное "умное" укорочение
    return _shorten_smart_name(name, params, CS_NAME_MAX_LEN)



def extract_color_from_name(name: str) -> str:
    # CS: цвет берём строго из имени (без ложных совпадений на бренд Hi-Black)
    s = normalize_offer_name(name)
    if not s:
        return ""
    s = _RE_HI_BLACK.sub(" ", s)
    # CS: нормализуем 'ё' → 'е', чтобы ловить 'чёрный/жёлтый'
    s = s.replace("ё", "е").replace("Ё", "Е")
    # CS: явные маркеры "цветной"
    if re.search(r"(?i)\b(cmyk|cmy)\b", s) or re.search(r"(?i)\bцветн\w*\b", s):
        return "Цветной"
    # CS: если "Color" стоит В КОНЦЕ (вариант картриджа), а не в середине (Color LaserJet)
    if re.search(r"(?i)\bcolor\b\s*(?:\)|\]|\}|$)", s):
        return "Цветной"
    # CS: если "Color" идёт перед "для/for" (вариант картриджа), считаем цветной
    if re.search(r"(?i)\bcolor\b\s*(?:для|for)\b", s):
        return "Цветной"
    # CS: если явно указан составной цвет (черный+цвет / black+color) → Цветной
    if re.search(r"\b(черн\w*|black)\b\s*\+\s*\b(цвет(?:н\w*)?|colou?r)\b", s, re.IGNORECASE) or \
       re.search(r"\b(цвет(?:н\w*)?|colou?r)\b\s*\+\s*\b(черн\w*|black)\b", s, re.IGNORECASE):
        return "Цветной"
    found: list[str] = []
    for color, rx in _RE_COLOR_TOKENS:
        if rx.search(s):
            if color not in found:
                found.append(color)
    if not found:
        return ""
    if len(found) > 1:
        return "Цветной"
    return found[0]


def apply_color_from_name(params: Sequence[tuple[str, str]], name: str) -> list[tuple[str, str]]:
    # CS: если в имени явно указан цвет — перезаписываем param "Цвет"; если param отсутствует — добавляем
    color = extract_color_from_name(name)
    base_params = list(params or [])
    if not color:
        return base_params
    out: list[tuple[str, str]] = []
    found = False
    for k, v in base_params:
        kk = norm_ws(k)
        vv = sanitize_mixed_text(norm_ws(v))
        if kk.casefold().replace("ё", "е") == "цвет":
            out.append(("Цвет", color))
            found = True
        else:
            out.append((kk, vv))
    if not found:
        out.append(("Цвет", color))
    return out



def normalize_color_value(raw: str) -> str:
    # CS: нормализация значения цвета (из params/описания) → каноническая форма
    s = norm_ws(raw)
    if not s:
        return ""
    s2 = s.replace("ё", "е").replace("Ё", "Е").strip()
    low = s2.casefold()

    # CS: составные маркеры (цветной)
    if re.search(r"\b(cmyk|cmy)\b", low):
        return "Цветной"
    if re.search(r"\bcmy\s*\+\s*bk\b|\bbk\s*\+\s*cmy\b", low):
        return "Цветной"
    if re.search(r"\bbk[,/ ]*c[,/ ]*m[,/ ]*y\b|\bc[,/ ]*m[,/ ]*y[,/ ]*bk\b", low):
        return "Цветной"
    if re.search(r"\b(?:4|6)\s*color\b", low):
        return "Цветной"

    # CS: английские/аббревиатуры
    if low in {"black", "bk"}:
        return "Черный"
    if low in {"cyan"}:
        return "Голубой"
    if low in {"magenta"}:
        return "Пурпурный"
    if low in {"yellow"}:
        return "Желтый"
    if low in {"gray", "grey"}:
        return "Серый"
    if low in {"red"}:
        return "Красный"
    if low in {"light grey", "light gray", "lgy"}:
        return "Серый"
    if low in {"chroma optimize", "chroma optimizer", "chroma optimise", "chroma optimiser"}:
        return "Прозрачный"

    # CS: русские базовые
    mapping = {
        "черный": "Черный",
        "серый": "Серый",
        "желтый": "Желтый",
        "голубой": "Голубой",
        "пурпурный": "Пурпурный",
        "маджента": "Пурпурный",
        "цветной": "Цветной",
        "серебряный": "Серебряный",
        "белый": "Белый",
        "синий": "Синий",
        "красный": "Красный",
        "зеленый": "Зеленый",
        "прозрачный": "Прозрачный",
        "фиолетовый": "Фиолетовый",
        "золотой": "Золотой",
    }
    if low in mapping:
        return mapping[low]

    # CS: если пришло уже в правильном виде
    if s2 in {"Черный", "Серый", "Желтый", "Голубой", "Пурпурный", "Цветной", "Серебряный", "Белый", "Синий", "Красный", "Зеленый", "Прозрачный", "Фиолетовый", "Золотой"}:
        return s2

    # CS: если внутри есть явное слово цвета (без стемов вроде 'сер\w*')
    if re.search(r"\bчерн(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Черный"
    if re.search(r"\bсер(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Серый"
    if re.search(r"\bж[её]лт(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Желтый"
    if re.search(r"\bголуб(?:ой|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Голубой"
    if re.search(r"\bпурпур(?:ный|ная|ное|ные|ного|ному|ным|ными|ных)\b", low):
        return "Пурпурный"
    if re.search(r"\bкрасн(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Красный"
    if re.search(r"\bсин(?:ий|яя|ее|ие|его|ему|им|ими|их)\b", low):
        return "Синий"
    if re.search(r"\bзел[её]н(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Зеленый"
    if re.search(r"\bбел(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Белый"
    if re.search(r"\bсеребрян(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Серебряный"
    if re.search(r"\bпрозрачн(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Прозрачный"
    if re.search(r"\bфиолетов(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Фиолетовый"
    if re.search(r"\bзолот(?:ой|ая|ое|ые|ого|ому|ым|ыми|ых)\b", low):
        return "Золотой"

    return ""


_RE_SERVICE_KV = re.compile(
    r"^(?:артикул|каталожный номер|oem\s*-?номер|oem\s*номер|ш?трих\s*-?код|штрихкод|код товара|код производителя|аналоги|аналог)\s*[:\-].*$",
    re.IGNORECASE,
)


def strip_service_kv_lines(text: str) -> str:
    # CS: удаляем служебные строки "Ключ: значение" из текста описания
    raw = fix_text(text or "")
    if not raw:
        return ""
    lines = [ln.strip() for ln in raw.split("\n")]
    keep: list[str] = []
    for ln in lines:
        if not ln:
            continue
        if _RE_SERVICE_KV.match(ln):
            continue
        keep.append(ln)
    return "\n".join(keep).strip()





# Нормализация "смешанная кириллица/латиница" внутри слов.
# Правило:
# - если в буквенной последовательности есть и кириллица, и латиница,
#   то приводим её к ОДНОМУ алфавиту по большинству букв.
# - последовательности с цифрами не трогаем (модели/коды).
_CYR_TO_LAT = str.maketrans(
    {
        "А": "A",
        "В": "B",
        "Е": "E",
        "К": "K",
        "М": "M",
        "Н": "H",
        "О": "O",
        "Р": "P",
        "С": "C",
        "Т": "T",
        "Х": "X",
        "У": "Y",
        "а": "a",
        "е": "e",
        "о": "o",
        "р": "p",
        "с": "c",
        "х": "x",
        "у": "y",
        "к": "k",
        "м": "m",
        "т": "t",
        "в": "b",
        "н": "h",
        "И": "I",
        "и": "i",
}
)

_LAT_TO_CYR = str.maketrans(
    {
        "A": "А",
        "B": "В",
        "E": "Е",
        "K": "К",
        "M": "М",
        "H": "Н",
        "O": "О",
        "P": "Р",
        "C": "С",
        "T": "Т",
        "X": "Х",
        "Y": "У",
        "a": "а",
        "b": "в",
        "e": "е",
        "k": "к",
        "m": "м",
        "h": "н",
        "o": "о",
        "p": "р",
        "c": "с",
        "t": "т",
        "x": "х",
        "y": "у",
        "I": "И",
        "i": "и",
}
)

_RE_WORDLIKE = re.compile(r"[0-9A-Za-zА-Яа-яЁё][0-9A-Za-zА-Яа-яЁё._\-/+]*")
_RE_LETTER_SEQ = re.compile(r"[A-Za-zА-Яа-яЁё]+")


def fix_mixed_cyr_lat(s: str) -> str:
    # Adapter-first: не меняем буквы кир/лат в core (это может портить корректные коды/модели).
    return s or ""


    def _fix_letters(seq: str) -> str:
        if not seq:
            return seq
        if re.search(r"[A-Za-z]", seq) and re.search(r"[А-Яа-яЁё]", seq):
            cyr = len(re.findall(r"[А-Яа-яЁё]", seq))
            lat = len(re.findall(r"[A-Za-z]", seq))
            if cyr >= lat:
                return seq.translate(_LAT_TO_CYR)
            return seq.translate(_CYR_TO_LAT)
        return seq

    def _sub(m: re.Match[str]) -> str:
        w = m.group(0)
        # Для кодов/моделей (есть цифры) часто бывает 1-2 кириллических "двойника" в латинском коде: CB540А -> CB540A
        if re.search(r"\d", w) and re.search(r"[A-Za-z]", w) and re.search(r"[А-Яа-яЁё]", w):
            cyr = len(re.findall(r"[А-Яа-яЁё]", w))
            lat = len(re.findall(r"[A-Za-z]", w))
            # В кодах/моделях почти всегда доминирует латиница; чинить нужно даже если латинская буква одна (iВ4040 -> iB4040)
            if lat >= cyr:
                return w.translate(_CYR_TO_LAT)
            return w.translate(_LAT_TO_CYR)
        # Иначе — аккуратно чиним смешанные последовательности букв
        return _RE_LETTER_SEQ.sub(lambda mm: _fix_letters(mm.group(0)), w)

    return _RE_WORDLIKE.sub(_sub, t)

# Безопасное int из любого значения
# Нормализация смешанных LAT-CYR токенов с дефисом: "LED-индикаторы" -> "LED индикаторы"
_RE_MIXED_HYPHEN_LAT_CYR = re.compile(r"\b([A-Za-z]{2,}[A-Za-z0-9]*)[\-–—]([А-Яа-яЁё]{2,})\b")
_RE_MIXED_HYPHEN_CYR_LAT = re.compile(r"\b([А-Яа-яЁё]{2,})[\-–—]([A-Za-z]{2,}[A-Za-z0-9]*)\b")
_RE_MIXED_HYPHEN_A1_CYR = re.compile(r"\b([A-Za-z]\d{1,3})[\-–—]([А-Яа-яЁё]{2,})\b")

_RE_MIXED_SLASH_LAT_CYR = re.compile(r"([A-Za-z]{1,}[A-Za-z0-9]*)/([Ѐ-ӿ]{2,})")
_RE_MIXED_SLASH_CYR_LAT = re.compile(r"([Ѐ-ӿ]{2,})/([A-Za-z]{1,}[A-Za-z0-9]*)")

def normalize_mixed_hyphen(s: str) -> str:
    t = s or ""
    if not t:
        return t
    # Начиная с v061 не удаляем дефис между LAT/CYR:
    # LCD-дисплей, LED-индикаторы, SNMP-карты, Android-приставка и т.п.
    return t


_RE_MIXED_SLASH_LAT_CYR_RE_MIXED_SLASH_LAT_CYR = re.compile(r"([A-Za-z]{1,}[A-Za-z0-9]*)/([Ѐ-ӿ]{2,})")
_RE_MIXED_SLASH_CYR_LAT = re.compile(r"([Ѐ-ӿ]{2,})/([A-Za-z]{1,}[A-Za-z0-9]*)")

_RE_KEEP_LAT_CYR_SLASH = re.compile(r"[A-Z]{1,5}(?:\d{0,3}[A-Z]{0,3})?")

def _mixed_slash_repl_lat_cyr(m: re.Match[str]) -> str:
    left = m.group(1)
    right = m.group(2)
    return f"{left}/{right}"

def _mixed_slash_repl_cyr_lat(m: re.Match[str]) -> str:
    left = m.group(1)
    right = m.group(2)
    return f"{left}/{right}"

def normalize_mixed_slash(s: str) -> str:
    t = s or ""
    if not t:
        return t
    # Только кир/лат переходы: колодка/IEC, CD/банк, ЖК/USB, контактілер/EPO.
    # Лат/лат (RJ11/RJ45) и цифры/лат (4/IEC) не трогаем.
    for _ in range(3):  # на случай нескольких вхождений
        t2 = _RE_MIXED_SLASH_LAT_CYR.sub(_mixed_slash_repl_lat_cyr, t)
        t2 = _RE_MIXED_SLASH_CYR_LAT.sub(_mixed_slash_repl_cyr_lat, t2)
        if t2 == t:
            break
        t = t2
    return t

# Нормализация слэша между разными алфавитами (LAT <-> CYR), включая казахские буквы.
_CYR_CHAR_RE = re.compile(r"[\u0400-\u04FF]")
_LAT_CHAR_RE = re.compile(r"[A-Za-z]")

def _char_script(ch: str) -> str | None:
    if not ch:
        return None
    if _LAT_CHAR_RE.match(ch):
        return "LAT"
    if _CYR_CHAR_RE.match(ch):
        return "CYR"
    return None

def normalize_mixed_slash_scripts(s: str) -> str:
    t = s or ""
    if "/" not in t:
        return t
    chars = list(t)
    n = len(chars)

    def find_script_left(i: int) -> str | None:
        j = i - 1
        while j >= 0:
            ch = chars[j]
            sc = _char_script(ch)
            if sc:
                return sc
            # пропускаем цифры/точки/дефисы/пробелы
            j -= 1
        return None

    def find_script_right(i: int) -> str | None:
        j = i + 1
        while j < n:
            ch = chars[j]
            sc = _char_script(ch)
            if sc:
                return sc
            j += 1
        return None

    changed = False
    for i, ch in enumerate(chars):
        if ch != "/":
            continue
        ls = find_script_left(i)
        rs = find_script_right(i)
        if ls and rs and ls != rs:
            chars[i] = " "
            changed = True
    if not changed:
        return t
    out = "".join(chars)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out

def fix_jk_token(s: str) -> str:
    # "ЖK" -> "ЖК"
    return re.sub(r"Ж[КKk]", "ЖК", s or "")

def sanitize_mixed_text(s: str) -> str:
    t = fix_mixed_cyr_lat(s)
    # Каз/рус тексты: исправляем короткие смешанные токены (ЖK -> ЖК)
    t = t.replace("ЖK", "ЖК").replace("Жk", "ЖК")
    # Не ломаем техно-токены вида LCD-дисплей, SNMP-карты, Android-приставка.
    # Слэш тоже сохраняем: LX/Улучшенный, Xerox/Карты и т.п.
    return normalize_mixed_slash(t)

def parse_id_set(env_value: str | None, fallback: Iterable[int] | None = None) -> set[str]:
    out: set[str] = set()
    if env_value:
        for part in env_value.split(","):
            p = part.strip()
            if p:
                out.add(p)
    if not out and fallback:
        out = {str(int(x)) for x in fallback}
    return out


# Генератор стабильного id (если у поставщика нет id)

_WGT_WORDS = ("вес", "масса", "weight")
_VOL_WORDS = ("объем", "объём", "volume")
_DIM_WORDS = ("габарит", "размер", "длина", "ширина", "высота")

def _looks_like_weight(name: str) -> bool:
    nl = (name or "").casefold()
    return any(w in nl for w in _WGT_WORDS)

def _looks_like_volume(name: str) -> bool:
    nl = (name or "").casefold()
    return any(w in nl for w in _VOL_WORDS)

def _looks_like_dims(name: str) -> bool:
    nl = (name or "").casefold()
    return any(w in nl for w in _DIM_WORDS)

def _to_float(v: str) -> float | None:
    m = _RE_NUM.search(v or "")
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except Exception:
        return None

def _is_sane_weight(v: str) -> bool:
    x = _to_float(v)
    if x is None:
        return False
    vv = (v or "").casefold()
    # если явно граммы — переводим в кг
    if ("кг" not in vv) and (re.search(r"\bг\b|гр", vv) is not None):
        x = x / 1000.0
    return 0.001 <= x <= 2000.0

def _is_sane_volume(v: str) -> bool:
    x = _to_float(v)
    if x is None:
        return False
    return 0.001 <= x <= 5000.0

def _is_sane_dims(v: str) -> bool:
    vv = (v or "").casefold()
    nums = _RE_NUM.findall(vv)
    # минимум 2 числа + разделитель или единицы измерения
    if len(nums) >= 2 and (_RE_DIM_SEP.search(vv) or any(u in vv for u in ("мм", "см", "м", "cm", "mm"))):
        return True
    return False



# Эвристика: похоже ли значение "Совместимость" на список моделей/серий (а не на общее назначение "для дома")
def _looks_like_model_compat(v: str) -> bool:
    s = norm_ws(v)
    if not s:
        return False
    scf = s.casefold()

    # CS: "увеличения/использования ..." — это описание назначения, а не список моделей
    if scf.startswith(("увеличения ", "использования ")):
        return False

    # CS: ссылки/маркетинг в "Совместимость" — мусор
    if "http://" in scf or "https://" in scf or "www." in scf:
        return False
    if ("™" in s or "®" in s) and len(s) > 40:
        return False

    has_sep = bool(re.search(r"[,;/\\|]", s))
    tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9\-]+", s)
    word_count = len(re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", s))

    # ! ? … — всегда предложение; точка — только если после буквы (не "1.0" и не "1010.")
    has_sentence = bool(re.search(r"[!?…]", s)) or bool(re.search(r"(?<=[A-Za-zА-Яа-яЁё])\.(?:\s|$)", s))

    # бренды/линейки (часто встречающиеся в совместимости)
    brands = r"(xerox|hp|canon|epson|brother|samsung|kyocera|ricoh|konica|minolta|lexmark|oki|pantum|dell|sharp|olivetti|toshiba|triumph|adler|panasonic)"

    # токены моделей (буквы+цифры)
    model_tokens = 0
    for t in tokens:
        if re.search(r"\d", t) and re.search(r"[A-Za-zА-Яа-яЁё]", t):
            model_tokens += 1
        elif re.match(r"^[A-Z]{1,4}\d{2,6}[A-Z]{0,3}$", t):
            model_tokens += 1

    cyr_words = [t for t in tokens if re.search(r"[а-яё]", t.casefold())]
    series_hits = sum(
        1 for t in tokens
        if t.casefold() in {
            "laserjet", "deskjet", "officejet", "pixma", "ecotank", "workforce",
            "workcentre", "versalink", "taskalfa", "ecosys", "bizhub", "i-sensys", "lbp", "mfp", "phaser"
        }
    )

    # 1) Списки (коммы/слэши/точки с запятой) и цифры — почти всегда модели
    if has_sep and re.search(r"\d", s):
        return True

    # 2) Типовая форма "для принтеров ..."
    if scf.startswith("для ") and re.search(r"\d", s):
        return True

    # 3) Короткие коды/серии (без предложений) — ok
    if (not has_sentence) and len(s) <= 40 and re.search(r"\d", s) and word_count <= 10:
        return True

    # 4) Много моделей — ok
    if model_tokens >= 2:
        return True

    # 5) Коротко + бренд — ok (Sharp C-CUBE, Olivetti PR II, Xerox ...)
    if len(tokens) <= 6 and re.search(rf"\b{brands}\b", scf):
        return True

    # 6) Маркетинг/предложения: длинный русскоязычный текст без списков и с точкой после слова
    if has_sentence and (not has_sep) and word_count >= 7 and len(cyr_words) >= 3 and model_tokens <= 2:
        stop = {
            "и","в","во","на","с","со","к","по","при","для","от","это","даже","если","чтобы",
            "как","но","или","то","же","также","еще","уже","благодаря","обеспечивает","используя",
            "работы","дома","офиса","победы","плавного","максимальной","четкости","детализации"
        }
        stop_hits = sum(1 for t in cyr_words if t.casefold() in stop)
        ratio = stop_hits / max(1, len(cyr_words))
        if len(s) > 45 or ratio >= 0.2:
            return False

    # 7) 1 модель + бренд/линейка — ok
    if model_tokens >= 1 and (series_hits >= 1 or re.search(rf"\b{brands}\b", scf)):
        return True

    return False


def _cs_trim_float(v: str, max_decimals: int = 4) -> str:
    # CS: аккуратно укорачиваем длинные дроби (объём/вес/габариты) для читаемости
    s = (v or "").strip()
    if not s:
        return s
    if not re.fullmatch(r"-?\d+(?:\.\d+)?", s):
        return s
    if "." not in s:
        return s
    intp, frac = s.split(".", 1)
    if len(frac) <= max_decimals:
        return s
    try:
        d = Decimal(s)
        q = Decimal("1." + ("0" * max_decimals))
        d2 = d.quantize(q, rounding=ROUND_HALF_UP)
        out = format(d2, "f")
        # убираем хвостовые нули и точку
        out = out.rstrip("0").rstrip(".")
        return out
    except (InvalidOperation, ValueError):
        return s

def clean_params(
    params: Sequence[tuple[str, str]],
    *,
    drop: set[str] | None = None,
) -> list[tuple[str, str]]:
    drop_set = (PARAM_DROP_DEFAULT_CF if drop is None else {norm_ws(x).casefold() for x in drop})

    tech_model_candidates: list[str] = []  # из ключей вида 'Технические характеристики ...'

    # Нормализация/переименования ключей (унификация между поставщиками)
    rename_map = {
        "наименование производителя": "Модель",
        "модель производителя": "Модель",

        # Совместимость
        "совместимость": "Совместимость",
        "совместимость с моделями": "Совместимость",
        "совместимость с устройствами": "Совместимость",

        # заголовки из тех.спека — не должны становиться названием параметра
        "основные характеристики": "Особенности",
        "технические характеристики": "Особенности",
        "системные характеристики": "Система",
        "совместимость с принтерами": "Совместимость",
        "совместимые модели": "Совместимость",
        "совместимость моделей": "Совместимость",

        "ресурс, стр": "Ресурс",
        "цвет печати": "Цвет",

        "состав поставки": "Комплектация",
        "комплектация": "Комплектация",
        "комплект поставки": "Комплектация",
    }

    def _normalize_color(v: str) -> str:
        s = (v or "").strip()
        if not s:
            return s
        s_cf = s.casefold().replace("ё", "е")
        # простые каноны
        if "black" in s_cf or s_cf.startswith("черн"):
            return "черный"
        if "cyan" in s_cf or "голуб" in s_cf:
            return "голубой"
        if "magenta" in s_cf or "пурпур" in s_cf:
            return "пурпурный"
        if "yellow" in s_cf or "желт" in s_cf:
            return "желтый"
        if "blue" in s_cf or ("син" in s_cf and "голуб" not in s_cf):
            return "синий"
        return s.replace("ё", "е")

    def _norm_key(k: str) -> str:
        kk = norm_ws(k)
        # ':' в имени параметра — обычно разделитель вида 'ключ : единица'. Убираем.
        if ':' in kk:
            kk = norm_ws(re.sub(r"\s*:\s*", " ", kk))
        if not kk:
            return ""
        # Срезаем мусорные ведущие символы (например, невидимые emoji/вариации)
        kk = re.sub(r"^[^0-9A-Za-zА-Яа-яЁё]+", "", kk)
        # Убираем zero-width символы внутри имени параметра
        kk = re.sub(r"[​‌‍﻿⁠]", "", kk)
        # Типовые опечатки/кодировки
        kk = re.sub(r"\(\s*B\s*т\s*\)", "(Вт)", kk)
        kk = kk.replace("(Bт)", "(Вт)").replace("Bт", "Вт")
        kk = fix_mixed_cyr_lat(kk)
        base = kk.casefold()
        # Общая эвристика: любые ключи с "совместим..." сводим к "Совместимость" (чтобы не плодить дубли)
        if "совместим" in base:
            kk = "Совместимость"
        else:
            kk = rename_map.get(base, kk)
        return kk

    def _norm_val(key: str, v: str) -> str:
        vv = sanitize_mixed_text(norm_ws(v))
        if not vv:
            return ""
        vv = fix_mixed_cyr_lat(vv)
        # иногда после парсинга остаётся хвостовая пунктуация
        vv = vv.rstrip(" ,;")
        if key.casefold() == "цвет":
            vv = _normalize_color(vv)
        return vv

    # Сбор значений по ключам (для совместимости допускаем объединение)
    buckets: dict[str, list[str]] = {}
    display: dict[str, str] = {}
    order: list[str] = []

    for k, v in params or []:
        # colon-in-key: иногда поставщик пишет так: "Совместимость: HP ..." (значение попало в имя)
        raw_k = norm_ws(k)
        raw_v = norm_ws(v)
        raw_k = fix_mixed_cyr_lat(raw_k)
        raw_v = fix_mixed_cyr_lat(raw_v)
        # Артефакт тех.спеков: номера разделов/строк вида "2.09 ..." — это мусор, не превращаем в param
        if re.match(r"^\d+\.\d+\s", raw_k) or re.match(r"^\d+\.\s", raw_k):
            continue
        if ":" in raw_k:
            base, tail = raw_k.split(":", 1)
            base_cf = base.strip().casefold()
            tail = tail.strip()
            if base_cf.startswith("совместимость") and tail:
                if not raw_v:
                    raw_v = tail
                elif tail.casefold() not in raw_v.casefold():
                    raw_v = (raw_v + " " + tail).strip()
                raw_k = base.strip()
        k = raw_k
        v = raw_v

        # Артефакт парсинга: ключ "Кол" + значение "во ..." (разбитое "Кол-во ...")
        if norm_ws(k).casefold() == "кол" and v.startswith("во "):
            tail = norm_ws(v)[2:].strip()
            if ":" in tail:
                subk, subv = tail.split(":", 1)
                subk = norm_ws(subk)
                subv = norm_ws(subv)
                k = (f"Кол-во {subk}" if subk else "Кол-во")
                v = subv
            else:
                m2 = re.match(r"(.+?)\s+(\d+(?:[.,]\d+)?)\s*(.*)$", tail)
                if m2:
                    subj = norm_ws(m2.group(1))
                    num = m2.group(2)
                    rest = norm_ws(m2.group(3))
                    k = (f"Кол-во {subj}" if subj else "Кол-во")
                    v = (num + (" " + rest if rest else "")).strip()
                else:
                    k = "Кол-во"
                    v = tail

        
        # CS: эвристика против перевёрнутых пар (когда name выглядит как значение, а value как ключ)
        _KEYLIKE = {
            "совместимость","интерфейс","технология","сертификация","частоты","частота","разрешение",
            "габариты","размер","вес","материал","материал корпуса","питание","напряжение","мощность",
            "модель","бренд","марка","тип","формат","объем","объём","ресурс"
        }
        k_cf = norm_ws(k).casefold().replace("ё","е")
        v_cf = norm_ws(v).casefold().replace("ё","е")
        def _looks_like_value_name(x: str) -> bool:
            xx = (x or "").strip()
            if not xx:
                return False
            if len(xx) > 40:
                return False
            # числовые значения с единицами
            if re.match(r"^\d", xx) and re.search(r"(?i)\b(?:см|мм|м|кг|г|gb|гб|mhz|гц|мгц|вт|w|v|а|mah|мaч|мл|ml)\b", xx):
                return True
            # коды/артикулы (497K22640, CC364A и т.п.)
            if re.fullmatch(r"[A-Z0-9\-]{4,}", xx.upper()) and re.search(r"\d", xx):
                return True
            return False
        if _looks_like_value_name(k) and (v_cf in _KEYLIKE) and (k_cf not in _KEYLIKE):
            k, v = v, k
        kk = _norm_key(k)
        vv = _norm_val(kk, v)
        # перехват заголовков вида 'Технические характеристики ...' — в параметры не кладём,
        # но значение попробуем использовать как 'Модель' если её нет
        k_cf_raw = norm_ws(k).casefold()
        if k_cf_raw.startswith('технические характеристики'):
            if vv:
                tech_model_candidates.append(vv)
            continue
        # мусорный артефакт парсинга: 'Основные свойства' = 'Применение'
        if k_cf_raw == 'основные свойства' and (vv.casefold() in {'применение', 'назначение', 'тип'} or len(vv) <= 20):
            continue
        if not kk or not vv:
            continue

        # Мусор из таблиц: заголовок "Параметр=Значение"
        kk_cf = kk.casefold().replace("ё", "е")
        vv_cf = vv.casefold().replace("ё", "е")
        if kk_cf in {"параметр", "параметры"} and vv_cf in {"значение", "значения"}:
            continue

        # Маркетинговые дисклеймеры (обычно дублируются в <description>) — в params не нужны
        if ("соответствуют всем стандартам качества" in vv_cf) and (
            "картридж" in kk_cf or "драм" in kk_cf or "фотобарабан" in kk_cf
        ):
            continue

        # Мусорные имена параметров: цифры/числа/Normal (включается env, чтобы не ломать AlStyle)
        if CS_DROP_TRASH_PARAM_NAMES:
            if _RE_TRASH_PARAM_NAME_NUM.match(kk) or kk.casefold() == "normal":
                continue

        # Убираем нулевой мусор в значениях
        vv_compact = vv.strip()
        if re.fullmatch(r"[-–—.]+", vv_compact) or vv_compact in {"..", "..."}:
            continue
        # Обрезанные значения вида "Вось..." — выкидываем (кроме числовых диапазонов 10...20)
        if "..." in vv_compact and not re.search(r"\d+\s*\.\.\.\s*\d+", vv_compact):
            if vv_compact.endswith("...") or re.search(r"[A-Za-zА-Яа-яЁё]\.\.\.", vv_compact):
                continue

        if kk.casefold() in drop_set:
            continue

        # Мягкая валидация вес/габариты/объем
        if _looks_like_weight(kk) and not _is_sane_weight(vv):
            continue
        if _looks_like_volume(kk) and not _is_sane_volume(vv):
            continue
        if _looks_like_dims(kk) and not _is_sane_dims(vv):
            continue

        if _looks_like_weight(kk) or _looks_like_volume(kk) or _looks_like_dims(kk):
            vv = _cs_trim_float(vv, max_decimals=4)

        # Системные характеристики: 'CPU: ...' лучше вынести как отдельный параметр CPU
        if kk.casefold() == 'система' and vv.lower().startswith('cpu:'):
            kk = 'CPU'
            vv = norm_ws(vv.split(':', 1)[1])

        key_cf = kk.casefold()
        # CS: не плодим мусорные булевые параметры
        if key_cf in {"применение", "безопасность"} and vv.casefold() in {"да", "есть", "true", "yes"}:
            continue
        if key_cf not in buckets:
            buckets[key_cf] = []
            display[key_cf] = kk
            order.append(key_cf)

        # Совместимость — объединяем, но предварительно чистим (ресурс/цвет/коды расходников)
        if key_cf == "совместимость":
            vv = _cs_strip_consumable_codes_from_text(vv, allow_short_3dig=_cs_looks_like_consumable_code_list(vv))
            vv = _cs_clean_compat_value(vv)
            if not vv:
                continue
            if not _looks_like_model_compat(vv):
                continue
            # уникализация по lower
            have = {x.casefold() for x in buckets[key_cf]}
            if vv.casefold() not in have:
                buckets[key_cf].append(vv)
        else:
            if not buckets[key_cf]:
                buckets[key_cf].append(vv)

    # Пост-правила: AkCent часто даёт "Вид" == "Тип" — убираем дубль
    if "тип" in buckets and "вид" in buckets:
        tval = (buckets["тип"][0] if buckets["тип"] else "").casefold()
        vval = (buckets["вид"][0] if buckets["вид"] else "").casefold()
        if tval and vval and (tval == vval or tval in vval or vval in tval):
            buckets.pop("вид", None)
            display.pop("вид", None)
            order = [k for k in order if k != "вид"]

    # Если модели нет, но были заголовки 'Технические характеристики ...' с коротким кодом — используем как Модель
    if ('модель' not in buckets) and tech_model_candidates:
        cand = tech_model_candidates[0]
        # если есть производитель — добавим его в начало (если ещё не указан)
        prod = (buckets.get('производитель',[""])[0] if buckets.get('производитель') else "").strip()
        if prod and (prod.casefold() not in cand.casefold()):
            cand = f"{prod} {cand}".strip()
        buckets['модель'] = [cand]
        display['модель'] = 'Модель'
        order.insert(0, 'модель')


    out: list[tuple[str, str]] = []
    for kcf in order:
        vals = buckets.get(kcf) or []
        if not vals:
            continue
        name = display.get(kcf, kcf)
        if kcf == "совместимость":
            v = _cs_merge_compat_values(vals)
            if not v:
                continue
            if len(v) > 260:
                v = _cs_trim_compat_to_max(v, 260)
            if not v:
                continue
            out.append((name, v))
        else:
            out.append((name, vals[0]))

    return out

# --- AkCent: компактная "поддержка штрихкодов" (читаемо и полезно для SEO) ---
_AC_BARCODE_PARAM_1D_NAMES = {"1D", "1d"}
_AC_BARCODE_PARAM_2D_NAMES = {"2D", "2d", "Распознование кода", "Распознавание кода"}
_AC_BARCODE_PARAM_OUT = "Поддерживаемые штрихкоды"

def _ac_compact_barcode_support(params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    # Склеиваем "1D" + "Распознование/Распознавание кода" в один человекочитаемый параметр,
    # чтобы не было простыней в характеристиках.
    p1d = ""
    p2d = ""
    out = []
    for k, v in params:
        if k in _AC_BARCODE_PARAM_1D_NAMES and v and not p1d:
            p1d = v.strip()
            continue
        if k in _AC_BARCODE_PARAM_2D_NAMES and v and not p2d:
            p2d = v.strip()
            continue
        out.append((k, v))

    if not (p1d or p2d):
        return params

    # Выдёргиваем самые "поисковые" и понятные токены, ограничиваем длину.
    src = " ".join([p1d, p2d])
    # Нормализуем разделители
    src = re.sub(r"[;•]+", ",", src)
    src = re.sub(r"\s+", " ", src).strip()

    # Кандидаты (часто встречающиеся стандарты)
    want = [
        "EAN-13", "EAN-8", "UPC-A", "UPC-E", "UCC/EAN-128",
        "Code128", "Code 128", "Code39", "Code 39", "Codabar",
        "ITF", "Interleaved 2 из 5", "Matrix 2 из 5", "Standard 25",
        "QR", "QR-код", "Data Matrix", "PDF417", "Maxicode", "HanXin",
        "Aztec", "RSS-14", "RSS-Limited", "RSS-Expand", "RSS-Expanded",
    ]

    found = []
    low = src.lower()
    for t in want:
        if t.lower() in low:
            # Канонизируем написание
            canon = t.replace("QR-код", "QR").replace("Code 128", "Code128").replace("Code 39", "Code39")
            canon = canon.replace("RSS-Expand", "RSS-Expanded")
            found.append(canon)

    # Дедуп + ограничение
    seen = set()
    compact = []
    for t in found:
        if t not in seen:
            seen.add(t)
            compact.append(t)
        if len(compact) >= 10:
            break

    # Если ничего не нашли (редко), оставим короткую фразу без простыни.
    if not compact:
        value = "1D/2D (основные форматы), QR, Data Matrix и др."
    else:
        value = "1D/2D: " + ", ".join(compact) + " и др."

    # Не дублируем если уже есть подобный параметр
    if not any(k == _AC_BARCODE_PARAM_OUT for k, _ in out):
        out.append((_AC_BARCODE_PARAM_OUT, value))

    return out
def _ac_drop_barcode_params(params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    # Удаляем "1D/2D" и похожие параметры ТОЛЬКО по имени (ключу). Значения не анализируем.
    drop_names = {
        "1d", "2d",
        "распознование кода", "распознавание кода",
        "поддерживаемые штрихкоды",
    }
    out: list[tuple[str, str]] = []
    for k, v in params:
        k_cf = (k or "").strip().casefold()
        v_cf = (v or "").strip().casefold()
        if k_cf in drop_names:
            continue
        out.append((k, v))
    return out


def apply_supplier_param_rules(params: Sequence[tuple[str, str]], oid: str, name: str) -> list[tuple[str, str]]:
    """Точечные правила по поставщикам/категориям для <param> и блока характеристик.
    - Удаляем служебные params (Артикул добавляется/может приходить извне)
    - VTT: штрих-код/аналоги удаляем; OEM-номер -> Партномер; если OEM нет — Каталожный номер -> Партномер
    - CopyLine: для 'Кабель сетевой' удаляем 'Совместимость'
    """
    oid_u = (oid or "").upper()
    name_cf = (name or "").strip().casefold()
    is_vtt = oid_u.startswith("VT")
    is_copyline = oid_u.startswith("CL")


    # AkCent: переименовываем техничные ключи 1D/2D/Распознавание кода в человекочитаемые названия.
    # Значения (списки форматов) сохраняем как есть.
    if oid_u.startswith("AC"):
        renamed: list[tuple[str, str]] = []
        for k0, v0 in (params or []):
            kk0 = norm_ws(k0)
            vv0 = norm_ws(v0)
            k_cf0 = kk0.casefold()
            if k_cf0 == "1d":
                kk0 = "Поддерживаемые 1D-коды"
            elif k_cf0 == "2d" or k_cf0 in {"распознование кода", "распознавание кода"}:
                kk0 = "Поддерживаемые 2D-коды"
            renamed.append((kk0, vv0))
        params = renamed

    # VTT: если OEM не дали — не теряем Каталожный номер (переносим в Партномер)
    vtt_has_oem = False
    vtt_has_part = False
    if is_vtt:
        for k0, v0 in params or []:
            kk0 = norm_ws(k0)
            vv0 = norm_ws(v0)
            if not kk0 or not vv0:
                continue
            k0_cf = kk0.casefold().replace("ё", "е")
            if k0_cf in {"oem-номер", "oem номер", "oem", "oem номер детали", "oem номер/part number"}:
                vtt_has_oem = True
            if k0_cf in {"партномер", "partnumber", "part number", "pn", "part no"}:
                vtt_has_part = True

    out: list[tuple[str, str]] = []
    for k, v in params or []:
        kk = norm_ws(k)
        vv = sanitize_mixed_text(norm_ws(v))
        if not kk or not vv:
            continue
        k_cf = kk.casefold().replace("ё", "е")
        # глобально: не выводим служебный 'Артикул' как характеристику
        if k_cf == "артикул":
            continue

        if is_vtt:
            # VTT: чистим служебные параметры
            if k_cf in {"аналоги", "аналог", "штрихкод", "штрих-код", "штрих код"}:
                continue
            # VTT: OEM-номер -> Партномер
            if k_cf in {"oem-номер", "oem номер", "oem", "oem номер детали", "oem номер/part number"}:
                kk = "Партномер"
            # VTT: Каталожный номер оставляем ТОЛЬКО если OEM/Партномер не дали
            elif k_cf in {"каталожный номер", "кат. номер", "каталожный №", "кат. №"}:
                if vtt_has_oem or vtt_has_part:
                    continue
                kk = "Партномер"

        if is_copyline and name_cf.startswith("кабель сетевой") and (k_cf == "совместимость"):
            continue

        out.append((kk, vv))
    return out

# Сортирует параметры: сначала приоритетные, затем по алфавиту
def sort_params(params: Sequence[tuple[str, str]], priority: Sequence[str] | None = None) -> list[tuple[str, str]]:
    pr = [norm_ws(x) for x in (priority or []) if norm_ws(x)]
    pr_map = {p.casefold(): i for i, p in enumerate(pr)}

    def key(kv):
        k = norm_ws(kv[0])
        idx = pr_map.get(k.casefold(), 10_000)
        return (idx, k.casefold())

    return sorted(list(params), key=key)


# Пробует извлечь пары "Характеристика: значение" из HTML описания (если поставщик кладёт это в description)
def enrich_params_from_desc(params: list[tuple[str, str]], desc_html: str) -> None:
    if not desc_html:
        return

    # <li><strong>Ключ:</strong> Значение</li>
    for m in re.finditer(r"<li>\s*<strong>([^<:]{1,80}):</strong>\s*([^<]{1,200})</li>", desc_html, flags=re.I):
        k = norm_ws(m.group(1))
        v = norm_ws(m.group(2))
        if k and v:
            # CS: артефакт парсинга вида 'Кол: во ...' -> 'Кол-во ...'
            if k.casefold() == "кол" and v.lower().startswith("во"):
                m2 = re.match(r"(?i)^во\s+(.+)$", v)
                rest = norm_ws(m2.group(1)) if m2 else ""
                if rest:
                    if ":" in rest:
                        k2, v2 = rest.split(":", 1)
                        k2 = norm_ws(k2)
                        v2 = norm_ws(v2)
                        if k2 and v2:
                            params.append((f"Кол-во {k2}", v2))
                            continue
                    params.append(("Кол-во", rest))
                    continue
            params.append((k, v))


# Лёгкое обогащение характеристик из name/description (когда у поставщика params бедные)

def _extract_color_from_name(name: str) -> str:
    """CS: совместимость со старым именем функции (цвет из name)."""
    return extract_color_from_name(name)



def enrich_params_from_name_and_desc(params: list[tuple[str, str]], name: str, desc_text: str) -> None:
    name = name or ""
    desc_text = desc_text or ""
    keys_cf = {norm_ws(k).casefold() for k, _ in (params or []) if k}

    def _has(k: str) -> bool:
        return (k or "").casefold() in keys_cf

    hay = f"{name}\n{desc_text}"

    # Тип (первое слово) — только если нет
    if not _has("Тип"):
        first = (name.split() or [""])[0].strip()
        if first and len(first) <= 32 and not re.search(r"\d", first):
            params.append(("Тип", first))
            keys_cf.add("тип")
    # CS: Совместимость НЕ создаём и НЕ обогащаем автоматически.
    # Если поставщик не дал параметр совместимости — оставляем пусто (по просьбе пользователя).

    # Ресурс
    if not (_has("Ресурс") or _has("Ресурс, стр")):
        m = re.search(r"(?i)\b(\d[\d\s\.,]{0,10}\d|\d{2,7})\s*(?:стр|страниц\w*|pages?)\b", hay)
        if m:
            num = re.sub(r"[^\d]", "", m.group(1))
            if len(num) >= 2 and not re.fullmatch(r"0+", num):
                params.append(("Ресурс", num))
                keys_cf.add("ресурс")
    # Цвет
    # ВАЖНО: если цвет явно указан в НАЗВАНИИ — он приоритетнее параметров (исправляем конфликт).
    # CS: чистим мусорные значения ("сервисам", "сертифицированном", "серии" и т.п.) и нормализуем допустимые.
    _bad_color_re = re.compile(r"(?i)\b(сервис\w*|сертифиц\w*|сертификац\w*|сер(?:ии|ий|ия))\b")
    for i in range(len(params) - 1, -1, -1):
        k, v = params[i]
        if norm_ws(k).casefold() == "цвет":
            vv_raw = norm_ws(v)
            vv_cf = vv_raw.casefold().replace("ё", "е")
            if vv_cf in {"сердцевина", "по одному на каждый цвет", "комбинированный"} or _bad_color_re.search(vv_raw):
                del params[i]
                keys_cf.discard("цвет")
                continue
            vv_norm = normalize_color_value(vv_raw)
            if vv_norm:
                params[i] = ("Цвет", vv_norm)
            elif len(vv_raw) > 24 and " " in vv_raw:
                # CS: длинные фразы обычно не цвет
                del params[i]
                keys_cf.discard("цвет")

    color_from_name = _extract_color_from_name(name)
    if color_from_name:
        # обновляем существующий "Цвет" (если был) или добавляем
        updated = False
        for i, (k, v) in enumerate(list(params)):
            if norm_ws(k).casefold() == "цвет":
                if norm_ws(v).casefold() != norm_ws(color_from_name).casefold():
                    params[i] = ("Цвет", color_from_name)
                updated = True
                break
        if not updated:
            params.append(("Цвет", color_from_name))
        keys_cf.add("цвет")
    else:
        # Если цвета в названии нет — можно попробовать вытащить из имени+описания
        if not _has("Цвет"):
            m = re.search(
                r"(?i)\b("
                r"cmyk|cmy|cmy\s*\+\s*bk|bk\s*\+\s*cmy|bk[,/ ]*c[,/ ]*m[,/ ]*y|c[,/ ]*m[,/ ]*y[,/ ]*bk|"
                r"black|bk|cyan|magenta|yellow|gray|grey|red|light\s*grey|light\s*gray|lgy|chroma\s*optim(?:ize|iser|izer)|"
                r"черн(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)|"
                r"сер(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)|"
                r"ж[её]лт(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)|"
                r"голуб(?:ой|ая|ое|ые|ого|ому|ым|ыми|ых)|"
                r"пурпур(?:ный|ная|ное|ные|ного|ному|ным|ными|ных)|"
                r"син(?:ий|яя|ее|ие|его|ему|им|ими|их)|"
                r"красн(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)|"
                r"зел[её]н(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)|"
                r"бел(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)|"
                r"серебрян(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)|"
                r"прозрачн(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)|"
                r"фиолетов(?:ый|ая|ое|ые|ого|ому|ым|ыми|ых)|"
                r"золот(?:ой|ая|ое|ые|ого|ому|ым|ыми|ых)"
                r")\b",
                hay,
            )
            if m:
                canon = normalize_color_value(m.group(0))
                if canon:
                    params.append(("Цвет", canon))
                    keys_cf.add("цвет")


# Делает текст описания "без странностей" (убираем лишние пробелы)
def fix_text(s: str) -> str:
    # Нормализует переносы строк и убирает мусорные пробелы/табуляции на пустых строках
    t = (s or "").replace("\r\n", "\n").replace("\r", "\n").strip()

    # Убираем служебные/паспортные строки (CRC/Barcode/внутренние коды), чтобы не портить описание
    def _is_service_line(ln: str) -> bool:
        s2 = (ln or "").strip()
        if not s2:
            return False
        # типичные ключи паспорта/склада
        if re.search(r"(?i)^(CRC|Retail\s*Bar\s*Code|Retail\s*Barcode|Bar\s*Code|Barcode|EAN|GTIN|SKU)\b", s2):
            return (":" in s2) or ("\t" in s2)
        # русские служебные строки (VTT часто так пишет)
        if re.search(r"(?i)^(Артикул|Каталожн\w*\s*номер|Кат\.\s*номер|OEM(?:-номер)?|ОЕМ(?:-номер)?|Код\s*производител\w*|Код\s*товара|Штрих[-\s]?код)\b", s2):
            return (":" in s2) or ("\t" in s2)
        if re.search(r"(?i)^Дата\s*(ввода|вывода|введения|обновления)\b", s2):
            return (":" in s2) or ("\t" in s2)
        # строки вида "1.01 ...:" или "2.14 ...\t..."
        if re.match(r"^\d+\.\d+\b", s2) and ((":" in s2[:60]) or ("\t" in s2)):
            return True
        return False

    if t:
        t = "\n".join([ln for ln in t.split("\n") if not _is_service_line(ln)])

    # строки, которые состоят только из пробелов/табов, считаем пустыми
    if t:
        t = "\n".join("" if (ln.strip() == "") else ln for ln in t.split("\n"))

    # убираем тройные пустые строки
    t = _RE_MULTI_NL.sub("\n\n", t)

    # Нормализация частой опечатки (Shuko -> Schuko)
    t = _RE_SHUKO.sub("Schuko", t)
    t = fix_mixed_cyr_lat(t)
    return t






def _native_has_specs_text(d: str) -> bool:
    # Если в "родном" описании уже есть свой блок характеристик/спецификаций — НЕ дублируем CS-блок.
    # Важно: у части поставщиков характеристики приходят таблично (через "\t") или внутри одной строки
    # (например: "⚙️ Основные характеристики" или "Основные характеристики: ...").
    if not d:
        return False
    # 1) Любые табы почти всегда означают таблицу характеристик
    if "\t" in d:
        return True
    # 2) Технические/основные характеристики — ловим В ЛЮБОМ месте, а не только в начале строки
    if re.search(r"\b(Технические характеристики|Основные характеристики)\b", d, flags=re.IGNORECASE):
        return True
    # 3) Секция "Характеристики" как заголовок (часто у AlStyle)
    if re.search(r"(?:^|\n)\s*Характеристики\b", d, flags=re.IGNORECASE):
        # чтобы не ловить маркетинг, проверим что рядом есть признаки таблицы/списка
        if re.search(r"(?:^|\n)\s*(Артикул|Модель|Совместимые|Тип|Разрешение|Цвет)\b", d, flags=re.IGNORECASE):
            return True
    return False

def _looks_like_section_header(line: str) -> bool:
    # Заголовок секции внутри характеристик (без табов, не слишком длинный)
    if not line:
        return False
    if "\t" in line:
        return False
    s = line.strip()
    if len(s) < 3 or len(s) > 64:
        return False
    # часто секции — 1-3 слова, без точки в конце
    if s.endswith("."):
        return False
    return True


def _build_specs_html_from_text(d: str) -> str:
    # Превращает "текстовую кашу" характеристик (с \n и \t) в читабельный HTML.
    lines = [ln.strip() for ln in (d or "").split("\n")]
    lines = [ln for ln in lines if ln != ""]

    # Разделяем: до первого маркера и после него
    idx = None
    for i, ln in enumerate(lines):
        if re.search(r"^[^A-Za-zА-Яа-яЁё]*\s*(?:Технические характеристики|Основные характеристики|Характеристики)\b", ln, flags=re.IGNORECASE):
            idx = i
            break

    if idx is None:
        # fallback: иногда маркер встречается ВНУТРИ строки ("... Основные характеристики: ...").
        # Попробуем вставить перенос перед первым маркером и распарсить заново.
        if re.search(r"\b(Технические характеристики|Основные характеристики)\b", d or "", flags=re.IGNORECASE):
            d_mod = re.sub(r"(?i)\b(Технические характеристики|Основные характеристики)\b", r"\n\1", d, count=1)
            return _build_specs_html_from_text(d_mod)

        # Если явного заголовка нет, но есть табы — считаем это таблицей характеристик.
        if "\t" in (d or ""):
            idx = 0
        else:
            d2 = xml_escape_text(d).replace("\n", "<br>")
            return f"<p>{d2}</p>"

    pre = lines[:idx]
    rest = lines[idx:]

    out: list[str] = []
    if pre:
        out.append("<p>" + "<br>".join(xml_escape_text(x) for x in pre) + "</p>")

    ul_items: list[str] = []
    pending_key = ""
    pending_vals: list[str] = []

    def flush_pending() -> None:
        nonlocal pending_key, pending_vals, ul_items
        if pending_key:
            val = ", ".join(v for v in pending_vals if v)
            if val:
                ul_items.append(f"<li><strong>{xml_escape_text(pending_key)}</strong>: {xml_escape_text(val)}</li>")
            else:
                ul_items.append(f"<li><strong>{xml_escape_text(pending_key)}</strong></li>")
            pending_key = ""
            pending_vals = []

    def flush_ul() -> None:
        nonlocal ul_items
        if ul_items:
            out.append("<ul>" + "".join(ul_items) + "</ul>")
            ul_items = []

    for ln in rest:
        if re.search(r"^[^A-Za-zА-Яа-яЁё]*\s*(?:Технические характеристики|Основные характеристики|Характеристики)\b", ln, flags=re.IGNORECASE):
            flush_pending()
            flush_ul()
            # нормализуем заголовок
            out.append("<h4>Технические характеристики</h4>")
            continue

        if _looks_like_section_header(ln):
            # если следующее — табы или список, считаем заголовком секции
            flush_pending()
            flush_ul()
            out.append(f"<h4>{xml_escape_text(ln)}</h4>")
            continue

        if "\t" in ln:
            flush_pending()
            parts = [p.strip() for p in ln.split("\t") if p.strip() != ""]
            if len(parts) >= 2:
                key = parts[0]
                val = " ".join(parts[1:]).strip()
                pending_key = key
                pending_vals = ([val] if val else [])
            elif len(parts) == 1:
                ul_items.append(f"<li>{xml_escape_text(parts[0])}</li>")
            # если совсем пусто — просто пропускаем
            continue

        if pending_key:
            # значения, идущие после строки с ключом без значения (пример: "Применение\t" + "Для дома")
            pending_vals.append(ln)
            continue

        # обычный пункт списка (например, состав поставки)
        ul_items.append(f"<li>{xml_escape_text(ln)}</li>")

    flush_pending()
    flush_ul()

    return "".join(out)

_RE_SPECS_HDR_LINE = re.compile(r"^[^A-Za-zА-Яа-яЁё]*\s*(?:Технические характеристики|Основные характеристики|Характеристики)\b", re.IGNORECASE)
_RE_SPECS_HDR_ANY = re.compile(r"\b(Технические характеристики|Основные характеристики|Характеристики)\b", re.IGNORECASE)




def _htmlish_to_text(s: str) -> str:
    """Превращает HTML-подобный текст (с <br>, <p>, списками) в текст с \n.
    Нужно, чтобы корректно вытащить тех/осн характеристики из CopyLine и похожих источников.
    """
    raw = s or ""
    if not raw:
        return ""
    # основные разрывы строк
    raw = re.sub(r"(?i)<br\s*/?>", "\n", raw)
    raw = re.sub(r"(?i)</?(?:p|div|li|ul|ol|tr|td|th|table|h[1-6])[^>]*>", "\n", raw)
    # вычищаем остальные теги
    raw = re.sub(r"<[^>]+>", " ", raw)
    # html entities
    try:
        import html as _html
        raw = _html.unescape(raw)
    except Exception:
        pass
    raw = raw.replace("\xa0", " ")
    return raw

# Разбивает инлайновые списки тех/осн характеристик (AlStyle/часть AkCent).
# Пример: "Основные характеристики: - Диапазон ...- Скорость ..." -> строки "- ...".
def _split_inline_specs_bullets(rest: str) -> str:
    t = rest or ""
    if not t:
        return ""
    # "...характеристики:" -> заголовок на отдельной строке
    t = re.sub(
        r"(?i)\b(Технические характеристики|Основные характеристики|Характеристики)\s*:\s*",
        lambda m: m.group(1) + "\n",
        t,
    )
    # ".- Скорость" / "мкм.-Скорость" -> новая строка с буллетом
    t = re.sub(r"\.-\s*(?=[A-Za-zА-Яа-яЁё])", ".\n- ", t)
    # " ... - Время" -> новая строка с буллетом (не трогаем диапазоны 3-5)
    t = re.sub(r"\s+-\s+(?=[A-Za-zА-Яа-яЁё])", "\n- ", t)
    # нормализуем пустые строки
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t



def extract_specs_pairs_and_strip_desc(d: str) -> tuple[str, list[tuple[str, str]]]:
    """Единый CS-подход:
    - вырезаем блоки тех/осн характеристик из нативного описания
    - превращаем их в пары (k, v), чтобы затем вывести ОДИН CS-блок <h3>Характеристики</h3>

    Важно: если пары не удалось распарсить, НЕ режем описание (чтобы не потерять данные).

    Возвращает: (description_without_specs, extracted_pairs)
    """
    raw = d or ""
    # если прилетел HTML — переводим в текст для устойчивого парсинга
    if "<" in raw and ">" in raw:
        raw = _htmlish_to_text(raw)

    raw = fix_text(raw)
    if not raw:
        return "", []

    lines_raw = raw.split("\n")

    idx = None
    for i, ln in enumerate(lines_raw):
        if _RE_SPECS_HDR_LINE.search(ln or ""):
            idx = i
            break

    if idx is None:
        # fallback: если заголовок встретился внутри строки — вставим перенос и попробуем снова
        if _RE_SPECS_HDR_ANY.search(raw):
            d2 = _RE_SPECS_HDR_ANY.sub(lambda m: "\n" + m.group(0), raw, count=1)
            if d2 != raw:
                return extract_specs_pairs_and_strip_desc(d2)
        # если табы — почти всегда таблица характеристик
        if "\t" in raw:
            idx = 0
        else:
            return raw, []

    pre = "\n".join(lines_raw[:idx]).strip()
    rest = "\n".join(lines_raw[idx:]).strip()
    rest = _split_inline_specs_bullets(rest)

    pairs = _parse_specs_pairs_from_text(rest)

    # страховка: если вообще ничего не распарсили — не трогаем описание
    if not pairs:
        return raw, []

    return pre, pairs


def _parse_specs_pairs_from_text(text: str) -> list[tuple[str, str]]:
    """Парсит блок характеристик в пары key/value.

    Поддерживает:
    - табличный формат (\t)
    - "Ключ: значение"
    - CopyLine-формат: чередование строк "Ключ" / "Значение" после заголовка
    - секции типа "Состав поставки" -> Комплектация
    """
    lines = [ln.strip() for ln in (text or "").split("\n")]
    lines = [ln for ln in lines if ln]

    out: list[tuple[str, str]] = []

    pending_key = ""

    misc_items: list[str] = []

    pending_vals: list[str] = []

    section = ""
    section_items: list[str] = []
    section_is_list = False

    def flush_pending() -> None:
        nonlocal pending_key, pending_vals, out
        if not pending_key:
            return
        v = ", ".join([x for x in pending_vals if x]).strip()
        if v:
            out.append((pending_key, v))
        pending_key = ""
        pending_vals = []

    def flush_section() -> None:
        nonlocal section, section_items, out, section_is_list
        if section and section_items:
            v = ", ".join([x for x in section_items if x]).strip()
            if v:
                if len(v) > 350:
                    v = v[:350].rstrip(" ,")
                out.append((section, v))
        section = ""
        section_items = []
        section_is_list = False

    def _is_kv_key(s: str) -> bool:
        if not s:
            return False
        ss = s.strip()
        if len(ss) < 2 or len(ss) > 80:
            return False
        if ss.casefold() in {"да", "нет"}:
            return False
        if ss.endswith(":"):
            return True
        # должно быть хоть одно слово/буква
        if not re.search(r"[A-Za-zА-Яа-яЁё]", ss):
            return False
        # избегаем списка вида "USB-кабель" как ключа (обычно это item в Комплектации)
        return True

    def _is_kv_val(s: str) -> bool:
        if not s:
            return False
        ss = s.strip()
        if len(ss) < 1 or len(ss) > 250:
            return False
        # значение может быть "Да/Нет/4800x4800" и т.п.
        return True

    i = 0
    while i < len(lines):
        ln = lines[i]

        # bullet/list items (AlStyle often uses '- ...' after 'Основные характеристики:')
        orig_ln = ln
        is_bullet = False
        if ln and ln[0] in '-•—*':
            is_bullet = True
            ln = ln.lstrip('-•—*').strip()
            if not ln:
                i += 1
                continue


        # dash-separated пары внутри буллета: "Ключ — Значение" / "Ключ - Значение"
        md = re.match(r"^([^:]{1,80}?)\s+[–—-]\s+(.{1,250})$", ln)
        if md:
            k2 = md.group(1).strip().rstrip(':')
            v2 = md.group(2).strip()
            if k2 and v2 and _is_kv_key(k2) and _is_kv_val(v2):
                flush_pending()
                flush_section()
                if _RE_SPECS_HDR_ANY.search(k2):
                    i += 1
                    continue
                out.append((k2, v2))
                i += 1
                continue

        # заголовки тех/осн характеристик
        if _RE_SPECS_HDR_LINE.search(ln):
            flush_pending()
            flush_section()
            i += 1
            continue

        # заголовок секции
        # ВАЖНО: не путать ключи ("Цвет печати") с секциями.
        # Считаем секцией только если:
        #  - это "Состав поставки/Комплектация" (ожидаем список), или
        #  - следующая строка выглядит как табличная (с \t)
        if ("\t" not in ln) and _looks_like_section_header(ln):
            nxt = lines[i + 1] if (i + 1 < len(lines)) else ""
            is_list_hdr = bool(re.search(r"(?i)состав\s+поставки|комплектац", ln))
            if is_list_hdr or ("\t" in (nxt or "")):
                flush_pending()
                flush_section()
                section = ln.strip()
                section_is_list = is_list_hdr
                i += 1
                continue

        # табличный формат
        if "\t" in ln:
            flush_pending()
            parts_raw = [p.strip() for p in ln.split("\t")]
            parts_raw = [p for p in parts_raw if p != ""]
            if not parts_raw:
                i += 1
                continue
            key = parts_raw[0] if parts_raw else ""
            # если это заголовок секции — пропускаем
            if _RE_SPECS_HDR_ANY.search(key):
                i += 1
                continue
            vals = [x for x in parts_raw[1:] if x]
            val = " ".join(vals).strip()

            if key and val:
                out.append((key, val))
                i += 1
                continue
            if key and not val:
                pending_key = key
                pending_vals = []
                i += 1
                continue

            only = " ".join(parts_raw).strip()
            if only and section:
                section_items.append(only)
            i += 1
            continue

        # формат "Ключ: значение"
        m = re.match(r"^([^:]{1,80}):\s*(.{1,250})$", ln)
        if m:
            flush_pending()
            flush_section()
            key = m.group(1).strip()

            val = m.group(2).strip()

            # не тащим заголовки тех/осн характеристик как параметр

            if _RE_SPECS_HDR_ANY.search(key):

                i += 1

                continue

            out.append((key, val))
            i += 1
            continue

        # если ожидаем список (Комплектация) — не пытаемся делать пары
        if section and section_is_list:
            section_items.append(ln)
            i += 1
            continue

        # CopyLine-формат: чередование строк ключ/значение
        if i + 1 < len(lines):
            nxt = lines[i + 1]
            # не мешаемся, если следующая строка — заголовок или табличная
            if ("\t" not in ln) and ("\t" not in nxt) and (not _RE_SPECS_HDR_LINE.search(nxt)) and (not re.search(r"(?i)состав\s+поставки|комплектац", nxt)):
                k_cf = ln.strip().casefold()
                known_key = k_cf in {"тип", "вид", "цвет", "бренд", "марка", "модель", "артикул", "штрихкод", "совместимость", "технология", "сертификация", "разрешение", "интерфейс", "частота", "частоты", "скорость", "формат", "размер", "габариты", "вес", "объем", "объём", "емкость", "ёмкость", "ресурс", "гарантия", "питание", "напряжение", "мощность"}
                keyish = (
                    bool(re.search(r"\s", ln))
                    or bool(re.search(r"\d", ln))
                    or bool(re.search(r"[()/%×x]", ln))
                    or known_key
                    or len(ln) >= 15
                )
                if keyish and _is_kv_val(nxt):
                    # избегаем склейки простых списков в пары: пропускаем только если ключ = 1 слово
                    wcount = len(re.findall(r"[A-Za-zА-Яа-яЁё]+", ln))
                    if len(ln) <= 12 and len(nxt) <= 12 and (not re.search(r"\d", ln + nxt)) and wcount <= 1 and (not known_key):
                        pass
                    else:
                        flush_pending()
                        flush_section()
                        out.append((ln.rstrip(":").strip(), nxt.strip()))
                        i += 2
                        continue
        # режим "ключ без значения" (редко, но бывает)
        if pending_key:
            pending_vals.append(ln)
            i += 1
            continue

        # по умолчанию: если это bullet-строка без явной структуры и секции нет — копим в общий список
        if is_bullet and (not section) and ln:
            misc_items.append(ln)
            i += 1
            continue

        # по умолчанию: кладём как пункт секции (если секция есть)
        if section:
            section_items.append(ln)
        i += 1

    flush_pending()
    flush_section()

    # если остались буллеты без пар — не теряем: кладём одним параметром
    if misc_items:
        v = ', '.join([x for x in misc_items if x]).strip()
        if v:
            if len(v) > 350:
                v = v[:350].rstrip(' ,')
            out.append(('Особенности', v))

    return out




def _cmp_name_like_text(s: str) -> str:
    # Для сравнения "похоже ли это на название" (используем только в дедупе описаний).
    t = (s or "")
    # срезаем простые HTML-теги и HTML-энтити (иногда поставщик кладёт <p>Название</p>)
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"&[a-zA-Z]+;|&#\d+;|&#x[0-9a-fA-F]+;", " ", t)
    t = norm_ws(t)
    t = t.strip(" \t\r\n\"'«»„“”‘’`")
    t = re.sub(r"[\s\-–—:|·•,\.]+$", "", t).strip()
    t = re.sub(r"^[\s\-–—:|·•,\.]+", "", t).strip()
    return t.casefold()


def _dedupe_desc_leading_name(desc: str, name: str) -> str:
    # CS: убираем повтор названия в начале "родного" описания (заголовок <h3> выводим сами).
    d = (desc or "").strip()
    n = norm_ws(name).strip()
    if not d or not n:
        return d

    n_cmp = _cmp_name_like_text(n)

    lines = d.splitlines()
    idx = None
    for i, ln in enumerate(lines):
        if ln.strip():
            idx = i
            break
    if idx is None:
        return d

    first = lines[idx].lstrip()
    first_cmp = _cmp_name_like_text(first)

    # Случай: первая строка = "Название" (или "Название:" и т.п.) — убираем строку целиком.
    tail_cut = re.sub(r"[\s\-–—:|·•,\.]+$", "", first_cmp).strip()
    if tail_cut == n_cmp:
        lines[idx] = ""
        out = "\n".join(ln for ln in lines if ln.strip()).strip()
        if not out:
            # если было только название — описание оставляем пустым (останется <h3>).
            if _cmp_name_like_text(d) == n_cmp:
                return ""
            return d
        return out

    # Regex: название с гибкими пробелами + разделители (решает проблему разных пробелов в исходнике)
    tokens = [re.escape(t) for t in n.split()]
    if not tokens:
        return d
    name_pat = r"\s+".join(tokens)
    rx = re.compile(
        rf"^\s*[«\"\'„“”‘’`]*{name_pat}[»\"\'”’`]*\s*(?:[\-–—:|·•,\.]|\s)+",
        re.IGNORECASE,
    )
    m = rx.search(first)
    if not m:
        return d

    rest = first[m.end():].lstrip(" \t-–—:|·•,.")
    if not rest:
        lines[idx] = ""
    else:
        lines[idx] = rest

    out = "\n".join(ln for ln in lines if ln.strip()).strip()

    # Если после вырезания осталось пусто — это был только дубль названия.
    if not out:
        if _cmp_name_like_text(d) == n_cmp:
            return ""
        return d

    # Safety: не превращаем описание в пустоту, если текста по сути не было.
    if len(out) < 20 and len(d) <= len(n) + 15:
        # Исключение: если d был по сути только названием — разрешаем "пусто" (или очень короткий остаток).
        if _cmp_name_like_text(d) == n_cmp:
            return out
        return d
    return out


def _clip_desc_plain(desc: str, *, max_chars: int = 1200) -> str:
    # CS: обрезание слишком длинного текста описания (маркетинговые простыни),
    # чтобы карточка была читабельной и не дублировала характеристики.
    s = (desc or "").strip()
    if not s:
        return s
    max_chars = int(max_chars)
    if len(s) <= max_chars:
        return s

    min_cut = 260

    # 1) режем по абзацам/строкам
    cut = s.rfind("\n\n", 0, max_chars)
    if cut >= min_cut:
        out = s[:cut].strip()
    else:
        cut = s.rfind("\n", 0, max_chars)
        if cut >= min_cut:
            out = s[:cut].strip()
        else:
            out = ""

    # 2) если разрывов нет — режем по знакам препинания/разделителям
    if not out:
        seps = [". ", "! ", "? ", "… ", "; ", ": ", ", "]
        best = -1
        for sep in seps:
            pos = s.rfind(sep, 0, max_chars)
            if pos > best:
                best = pos
        if best >= min_cut:
            out = s[: best + 1].strip()
        else:
            out = s[:max_chars].strip()

    out = out.rstrip(" ,.;:-")
    if len(s) - len(out) >= 80 and not out.endswith("…"):
        out = out + "…"
    return out



def _build_desc_part(name: str, native_desc: str) -> str:
    # CS: возвращает ТОЛЬКО тело описания (<p>...</p>), без <h3> (заголовок строится выше шаблоном)
    d = fix_text(native_desc)
    if not d:
        return ""

    # Если в нативном описании есть технические/основные характеристики или табличные данные,
    # не дублируем это в описании (единый CS-блок характеристик будет ниже).
    if _native_has_specs_text(d):
        ls = d.split("\n")
        cut = None
        for i, ln in enumerate(ls):
            if "\t" in ln:
                cut = i
                break
            if re.search(r"(?i)\b(технические\s+характеристики|основные\s+характеристики|характеристики)\b", ln):
                cut = i
                break
        if cut is not None:
            d = "\n".join(ls[:cut]).strip()

    # CS: убираем повтор названия в начале и режем длинные простыни
    d = _dedupe_desc_leading_name(d, name)
    d = _clip_desc_plain(d, max_chars=int(os.getenv("CS_NATIVE_DESC_MAX_CHARS", "1200")))

    # Если после чистки осталось только название — не выводим пустой <p> с дублем.
    if _cmp_name_like_text(d) == _cmp_name_like_text(name):
        d = ""

    if not d:
        return ""

    d2 = xml_escape_text(d).replace("\n", "<br>")
    return f"<p>{d2}</p>"

def normalize_cdata_inner(inner: str) -> str:
    # Убираем мусорные пробелы/пустые строки внутри CDATA, без лишних ведущих/хвостовых переводов строк
    inner = (inner or "").strip()
    inner = _RE_MULTI_NL.sub("\n\n", inner)
    return inner

def normalize_pictures(pictures: Sequence[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for p in pictures or []:
        u = norm_ws(p)
        # CS: NVPrint отдаёт заглушку nophoto.jpg — приводим к общему placeholder
        if u.lower() in {"https://nvprint.ru/promo/photo/nophoto.jpg", "http://nvprint.ru/promo/photo/nophoto.jpg"}:
            u = CS_PICTURE_PLACEHOLDER_URL
        if not u:
            continue
        # если это просто домен без пути — это не картинка
        try:
            from urllib.parse import urlparse
            pr = urlparse(u)
            if pr.scheme in {"http", "https"} and pr.netloc and pr.path in {"", "/"}:
                continue
        except Exception:
            pass
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out



# Собирает keywords: бренд + полное имя + разбор имени на слова + города (в конце)


# Собирает keywords: бренд + имя + ключи по доставке + города (компактно, без "простыни")
# Важно: никаких "CS_CITY_TAIL" больше нет — keywords строятся только здесь.
# Похоже на "предложение" (инструкция/маркетинг) в имени параметра — переносим в notes, а не в характеристики.
# Дублирует часть эвристик выше, но даёт дополнительную страховку.
_RE_PARAM_SENTENCEY = re.compile(r"[.!?]|\b(?:вы|вам|вас|можете|пожалуйста|важно|внимание|доставка|оплата)\b", re.IGNORECASE)

def _is_sentence_like_param_name(k: str) -> bool:
    kk = norm_ws(k)
    if not kk:
        return False

    cf = kk.casefold()
    # исключение: это нормальные характеристики (оставляем в блоке "Характеристики")
    if (("рекомендуемая" in cf) or ("рекомендуемое" in cf)) and (("нагрузк" in cf) or ("количеств" in cf)):
        return False


    # исключение: гарантия — это характеристика (а не маркетинг/фраза)
    if cf.startswith("гаранти") and (len(kk) <= 25) and (len(kk.split()) <= 3):
        return False

    # 1) Явные фразы/инструкции/маркетинг — не характеристики
    if any(x in cf for x in (
        "вы можете купить",
        "в городах",
        "доставка",
        "оплата",
        "рекомендуем",
        "важно",
        "внимание",
        "обратите",
        "пожалуйста",
        "не обновля",
        "не обновлять",
        "маркир",
        "подлинност",
        "original",
        "оригинал",
        "упаковк",
        "предупрежден",
        "гаранти",
        "качества используемой бумаги",
        "заполняемость выводимых",
    )):
        return True

    # 2) Ключ со строчной буквы (обрывок/продолжение) — не характеристика
    first = kk[0]
    if first.isalpha() and first.islower():
        return True

    # 3) Слишком длинный "ключ-фраза"
    if len(kk) >= 65:
        return True
    words = kk.split()
    if len(words) >= 8:
        return True

    # 4) Похоже на предложение / обрывок
    if kk.endswith((".", "!", "?", ";")):
        return True
    if kk.endswith((",", ":")):
        return True
    if (kk.count(",") >= 1) and len(kk) >= 45:
        return True
    if _RE_PARAM_SENTENCEY.search(kk):
        return True

    return False


def split_params_for_chars(
    params_sorted: Sequence[tuple[str, str]],
) -> tuple[list[tuple[str, str]], list[str]]:
    """Отделяет 'параметры-фразы' (дисклеймеры/примечания) от реальных характеристик."""
    kept: list[tuple[str, str]] = []
    notes_raw: list[str] = []

    for k, v in (params_sorted or []):
        kk = norm_ws(k)
        vv = sanitize_mixed_text(norm_ws(v))
        if not kk or not vv:
            continue

        # кривые обрывки значений типа '...:' — это не характеристика
        if vv.endswith(":"):
            vv2 = vv.rstrip(": ")
            txt = f"{kk}: {vv2}" if vv2 else kk
            txt = norm_ws(txt)
            if len(txt) >= 18:
                notes_raw.append(txt)
            continue

        # пустые/неинформативные значения (UX/SEO мусор)
        vv_cf = vv.casefold()
        if (len(vv) <= 28) and any(x in vv_cf for x in ("поэтому рекомендуем", "рекомендуем", "советуем", "рекоменд")):
            # если нет цифр/единиц — выбрасываем (не переносим даже в примечание)
            if not re.search(r"\d", vv):
                continue

        if _is_sentence_like_param_name(kk):
            # обрывки/инструкции -> примечание, но слишком короткие куски выкидываем
            text = kk
            if vv and (vv.casefold() not in kk.casefold()):
                text = f"{kk}: {vv}"
            text = norm_ws(text)
            if len(text) < 18:
                continue
            notes_raw.append(text)
            continue

        kept.append((kk, vv))




    # uniq + limit
    notes: list[str] = []
    seen: set[str] = set()
    for x in notes_raw:
        x2 = norm_ws(x)
        if not x2:
            continue
        cf = x2.casefold()
        if cf in seen:
            continue
        seen.add(cf)
        notes.append(x2)

    return kept, notes[:2]


def _build_param_summary(params_sorted: Sequence[tuple[str, str]]) -> str:
    """
    Короткая фраза из существующих param, если родного описания нет.
    Ничего не выдумываем, берем только реальные значения.
    """
    # приоритетные поля (без габаритов/объемов и прочего шумного)
    pri = [
        "тип", "вид", "тип товара",
        "производитель", "бренд", "марка",
        "модель",
        "совместимость",
        "цвет",
        "ресурс",
        "формат",
        "интерфейс",
    ]
    blacklist = {
        "артикул", "штрихкод", "ean", "sku", "код",
        "вес", "габариты", "габариты (шхгхв)", "ширина", "высота", "длина", "объём", "объем",
    }
    # собираем последние значения по ключу
    buckets: dict[str, tuple[str, str]] = {}
    for k, v in params_sorted or []:
        kk = norm_ws(k).lower()
        vv = sanitize_mixed_text(norm_ws(v))
        if not kk or not vv:
            continue
        if kk in blacklist:
            continue
        # отсекаем "да/нет/есть" — в кратком абзаце это мусор
        vv_l = vv.strip().lower()
        if vv_l in {"да", "нет", "есть", "имеется", "-", "—"}:
            continue
        if len(vv) > 140:
            continue
        buckets[kk] = (k.strip(), vv.strip())

    picked: list[tuple[str, str]] = []
    for want in pri:
        if want in buckets:
            picked.append(buckets[want])
        if len(picked) >= 3:
            break

    # fallback: первые 2 адекватных
    if not picked:
        for _, (k, v) in buckets.items():
            picked.append((k, v))
            if len(picked) >= 2:
                break

    if not picked:
        return ""

    # "Тип: ...; Модель: ...; ..."
    return "; ".join(f"{k}: {v}" for k, v in picked).strip()

def _cs_chars_block_html(params: list[tuple[str, str]]) -> str:
    """HTML-блок характеристик. Если параметров нет — выводим заглушку."""
    if not params:
        return '<h3>Характеристики</h3><p>Характеристики уточняются.</p>'
    # стандартный список
    items = ''.join([f'<li><b>{xml_escape(k)}:</b> {xml_escape(v)}</li>' for k, v in params if k and v])
    if not items:
        return '<h3>Характеристики</h3><p>Характеристики уточняются.</p>'
    return f'<h3>Характеристики</h3><ul>{items}</ul>'


def get_public_vendor(supplier: str | None = None) -> str:
    raw = (os.getenv("CS_PUBLIC_VENDOR", "") or os.getenv("PUBLIC_VENDOR", "") or "CS").strip()
    raw = norm_ws(raw) or "CS"
    # страховка: не допускаем, чтобы public_vendor был названием поставщика
    bad = {"alstyle", "akcent", "copyline", "nvprint", "vtt"}
    if supplier and supplier.strip():
        bad.add(supplier.strip().casefold())
    if raw.casefold() in bad:
        return "CS"
    if any(b in raw.casefold() for b in bad):
        # если кто-то случайно подсунул строку с названием поставщика
        return "CS"
    return raw


# CS: вычисляет next_run для расписания "в дни месяца" (например 1/10/20) в заданный час (Алматы)
def next_run_dom_at_hour(now: datetime, hour: int, doms: Sequence[int]) -> datetime:
    hour = int(hour)
    doms_sorted = sorted({int(d) for d in doms if int(d) >= 1 and int(d) <= 31})
    if not doms_sorted:
        # fallback: завтра в тот же час
        base = (now + timedelta(days=1)).replace(minute=0, second=0, microsecond=0)
        return base.replace(hour=hour)

    def _last_day_of_month(y: int, m: int) -> int:
        # 28-е + 4 дня гарантированно перейдёт в следующий месяц
        first_next = datetime(y, m, 28) + timedelta(days=4)
        first_next = datetime(first_next.year, first_next.month, 1)
        return (first_next - timedelta(days=1)).day

    def _pick_in_month(y: int, m: int, after_dt: datetime | None) -> datetime | None:
        last = _last_day_of_month(y, m)
        for d in doms_sorted:
            if d > last:
                continue
            cand = datetime(y, m, d, hour, 0, 0)
            if after_dt is None or cand > after_dt:
                return cand
        return None

    # 1) в текущем месяце — следующий подходящий день
    cand = _pick_in_month(now.year, now.month, now)
    if cand:
        return cand

    # 2) в следующем месяце — самый ранний подходящий день
    y2, m2 = now.year, now.month + 1
    if m2 == 13:
        m2 = 1
        y2 += 1
    cand2 = _pick_in_month(y2, m2, None)
    if cand2:
        return cand2

    # 3) fallback: 1-е число следующего месяца
    return datetime(y2, m2, 1, hour, 0, 0)


# CS: собирает полный XML фида (header + FEED_META + offers + footer)
def write_cs_feed_raw(
    offers: Sequence["OfferOut"],
    *,
    supplier: str,
    supplier_url: str,
    out_file: str,
    build_time: datetime,
    next_run: datetime,
    before: int,
    encoding: str = OUTPUT_ENCODING_DEFAULT,
    currency_id: str = CURRENCY_ID_DEFAULT,
) -> bool:
    full = build_cs_feed_xml_raw(
        offers,
        supplier=supplier,
        supplier_url=supplier_url,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        currency_id=currency_id,
    )
    return write_if_changed(out_file, full, encoding=encoding)

# CS: пишет фид в файл (validate + write_if_changed)
def write_cs_feed(
    offers: Sequence["OfferOut"],
    *,
    supplier: str,
    supplier_url: str,
    out_file: str,
    build_time: datetime,
    next_run: datetime,
    before: int,
    encoding: str = OUTPUT_ENCODING_DEFAULT,
    public_vendor: str = "CS",
    currency_id: str = CURRENCY_ID_DEFAULT,
    param_priority: Sequence[str] | None = None,
) -> bool:
    full = build_cs_feed_xml(
        offers,
        supplier=supplier,
        supplier_url=supplier_url,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        public_vendor=public_vendor,
        currency_id=currency_id,
        param_priority=param_priority,
    )
    validate_cs_yml(full, param_drop_default_cf=PARAM_DROP_DEFAULT_CF)
    return write_if_changed(out_file, full, encoding=encoding)



# Пишет файл только если изменился (атомарно)
def normalize_vendor(v: str) -> str:
    # CS: нормализация vendor (убираем дубль 'Hewlett-Packard' -> 'HP' и т.п.)
    if not v:
        return ""
    v = str(v).strip()
    if not v:
        return ""
    v_cf = v.casefold().replace("ё", "е")
    # унификация SMART
    if v_cf == "smart":
        v = "SMART"
        v_cf = "smart"
    # частые алиасы/опечатки
    if v_cf.startswith("epson proj"):
        v = "Epson"
    elif v_cf.startswith("viewsonic proj"):
        v = "ViewSonic"
    elif v_cf.startswith("brothe"):
        v = "Brother"
    elif v_cf.startswith("europrint"):
        v = "Europrint"
    # унификация Konica Minolta
    if "konica" in v_cf and "minolta" in v_cf:
        v = "Konica Minolta"
    # унификация Kyocera-Mita
    if "kyocera" in v_cf and "mita" in v_cf:
        v = "Kyocera"
        v_cf = "kyocera"
    # нормализуем слэш-списки (HP/Canon)
    parts = [p.strip() for p in re.split(r"\s*/\s*", v) if p.strip()]
    norm_parts: list[str] = []
    for p in parts:
        low = p.lower().replace("‑", "-").replace("–", "-")
        if re.search(r"hewlett\s*-?\s*packard", low):
            norm_parts.append("HP")
        else:
            norm_parts.append(p)
    # склеиваем обратно
    out = "/".join(norm_parts)
    out = re.sub(r"\s{2,}", " ", out).strip()
    # CS: не смешиваем бренды через '/' (HP/Canon -> HP) — для vendor нужен один бренд
    if "/" in out:
        parts2 = [p.strip() for p in out.split("/") if p.strip()]
        if len(parts2) >= 2:
            # берём первый бренд, если список состоит из известных брендов
            canon_set = set(CS_BRANDS_MAP.values())
            if all(p in canon_set for p in parts2):
                out = parts2[0]
    return out

# Пытается определить бренд (vendor) по vendor_src / name / params / description (если пусто — public_vendor)

# CS: защита от ошибочного vendor (тип товара/префикс/код вместо бренда)
_BAD_VENDOR_WORDS = {
    "мфу", "принтер", "сканер", "плоттер", "шредер", "ламинатор", "переплетчик",
    "монитор", "экран", "проектор",
    "интерактивная", "интерактивный", "интерактивная панель", "интерактивный дисплей", "интерактивная доска",
    "экономичный", "экономичный набор",
    "картридж", "чернила", "тонер", "барабан", "чип",
    "пленка для ламинирования", "емкость для отработанных чернил",
}
_RE_VENDOR_CODELIKE = re.compile(r"^[A-ZА-ЯЁ]{1,3}\d")

def _is_bad_vendor_token(v: str) -> bool:
    vv = norm_ws(v)
    if not vv:
        return False
    cf = vv.casefold().replace("ё", "е")
    if cf in _BAD_VENDOR_WORDS:
        return True
    # коды/артикулы вида C13T55KD00, W1335A, V12H... не являются брендом
    if (" " not in vv) and (len(vv) <= 24) and _RE_VENDOR_CODELIKE.match(vv):
        return True
    return False

# Словарь брендов для pick_vendor (упорядочен, расширяем при необходимости)
CS_BRANDS_MAP = {
    "hp": "HP",
    "hewlett": "HP",
    "canon": "Canon",
    "epson": "Epson",
    "brother": "Brother",
    "samsung": "Samsung",
    "sv": "SVC",
    "svc": "SVC",
    "apc": "APC",
    "schneider": "Schneider Electric",
    "cyberpower": "CyberPower",
    "cyber-power": "CyberPower",
    "cyber power": "CyberPower",
    "smart": "SMART",
    "idprt": "IDPRT",
    "id-prt": "IDPRT",
    "id prt": "IDPRT",
    "asus": "ASUS",
    "lenovo": "Lenovo",
    "acer": "Acer",
    "dell": "Dell",
    "logitech": "Logitech",
    "xiaomi": "Xiaomi",

    "ripo": "RIPO",
    "xerox": "Xerox",
    "kyocera": "Kyocera",
    "ricoh": "Ricoh",
    "toshiba": "Toshiba",
    "integral": "INTEGRAL",
    "pantum": "Pantum",
    "oki": "OKI",
    "lexmark": "Lexmark",
    "konica": "Konica Minolta",
    "minolta": "Konica Minolta",
    "fujifilm": "FUJIFILM",
    "huawei": "Huawei",
    "deli": "Deli",
    "olivetti": "Olivetti",
    "panasonic": "Panasonic",
    "riso": "Riso",
    "avision": "Avision",
    "fellowes": "Fellowes",
    "viewsonic": "ViewSonic",
    "philips": "Philips",
    "zebra": "Zebra",
    "euro print": "Europrint",
    "designjet": "HP",
    "mr.pixel": "Mr.Pixel",
    "hyperx": "HyperX",
    "aoc": "AOC",
    "benq": "BenQ",
    "lg": "LG",
    "msi": "MSI",
    "gigabyte": "GIGABYTE",
    "tp-link": "TP-Link",
    "tplink": "TP-Link",
    "mikrotik": "MikroTik",
    "ubiquiti": "Ubiquiti",
    "d-link": "D-Link",
    "europrint": "Europrint",
    "brothe": "Brother",
}

def pick_vendor(
    vendor_src: str,
    name: str,
    params: Sequence[tuple[str, str]],
    desc_html: str,
    *,
    public_vendor: str = "CS",
) -> str:
    """
    Общий vendor-guard без supplier-rescue.

    Правило CS:
    - core НЕ ищет vendor в name / description / params;
    - core принимает vendor, который уже отдал адаптер;
    - если vendor пустой/мусорный — применяет только общий fallback public_vendor.

    Аргументы name/params/desc_html оставлены для back-compat сигнатуры,
    чтобы не ломать существующие вызовы и адаптеры.
    """
    _ = name
    _ = params
    _ = desc_html

    v = norm_ws(vendor_src)
    if v:
        v2 = normalize_vendor(v)
        if v2 and (not _is_bad_vendor_token(v2)):
            return v2

    return norm_ws(public_vendor)


@dataclass
class OfferOut:
    oid: str
    available: bool
    name: str
    price: int | None
    pictures: list[str]
    vendor: str
    params: list[tuple[str, str]]
    native_desc: str

    # Собирает XML offer (фиксированный порядок)
    def to_xml(
        self,
        *,
        currency_id: str = CURRENCY_ID_DEFAULT,
        public_vendor: str = "CS",
        param_priority: Sequence[str] | None = None,
    ) -> str:
        name_full = normalize_offer_name(self.name)
        name_full = sanitize_mixed_text(name_full)
        native_desc = fix_text(self.native_desc)
        # RAW должен уже отдавать идеальные params и чистое supplier-description.
        # Core НЕ переносит характеристики из description в params и не enrich'ит их из desc/name.
        native_desc = strip_service_kv_lines(native_desc)
        vendor = pick_vendor(self.vendor, name_full, self.params, native_desc, public_vendor=public_vendor)
        price_final = compute_price(self.price)

        # RAW обязан отдавать уже чистые и финальные supplier params.
        # Core не чистит, не нормализует и не перестраивает параметры под поставщика.
        params = [(sanitize_mixed_text(k), sanitize_mixed_text(v)) for (k, v) in (self.params or [])]
        params_sorted = sort_params(params, priority=list(param_priority or []))
        chars_html = _cs_chars_block_html(params_sorted)
        notes: list[str] = []

        # CS: лимитируем <name> (умно для NVPrint)
        name_short = enforce_name_policy(self.oid, name_full, params_sorted)
        name_short = sanitize_mixed_text(name_short)

        # CS: В описании сохраняем полное наименование (если оно было укорочено).
        # Если <name> был укорочен — в описании сохраняем полное наименование.
        name_for_desc = name_full if (name_short != name_full) else name_short
        name_for_desc = sanitize_mixed_text(name_for_desc)

        desc_cdata = build_description(name_for_desc, native_desc, params_sorted, notes=notes)
        desc_cdata = sanitize_mixed_text(desc_cdata)
        keywords = build_keywords(vendor, name_short)
        keywords = _truncate_text(keywords, int(CS_KEYWORDS_MAX_LEN or CS_KEYWORDS_MAX_LEN_FALLBACK))
        keywords = sanitize_mixed_text(keywords)

        # Core не знает поставщиков и не применяет supplier-specific санитайзеры.
        # Любая такая очистка должна происходить только в RAW / supplier-layer.

        pics_xml = ""
        pics = normalize_pictures(self.pictures or [])
        if not pics and CS_PICTURE_PLACEHOLDER_URL:
            pics = [CS_PICTURE_PLACEHOLDER_URL]
        for pp in pics:
            pics_xml += f"\n<picture>{xml_escape_text(_cs_norm_url(pp))}</picture>"

        params_xml = ""
        for k, v in params_sorted:
            kk = xml_escape_attr(norm_ws(k))
            vv = xml_escape_text(norm_ws(v))
            if not kk or not vv:
                continue
            params_xml += f"\n<param name=\"{kk}\">{vv}</param>"

        # Core не знает поставщиков и не меняет availability.
        # RAW обязан отдать уже правильное available для конкретного supplier-layer.
        avail_effective = bool(self.available)

        out = (
            f"<offer id=\"{xml_escape_attr(self.oid)}\" available=\"{bool_to_xml(bool(avail_effective))}\">\n"
            f"<categoryId></categoryId>\n"
            f"<vendorCode>{xml_escape_text(self.oid)}</vendorCode>\n"
            f"<name>{xml_escape_text(name_short)}</name>\n"
            f"<price>{int(price_final)}</price>"
            f"{pics_xml}\n"
            f"<vendor>{xml_escape_text(vendor)}</vendor>\n"
            f"<currencyId>{xml_escape_text(currency_id)}</currencyId>\n"
            f"<description><![CDATA[\n{desc_cdata}]]></description>"
            f"{params_xml}\n"
            f"<keywords>{xml_escape_text(keywords)}</keywords>\n"
            f"</offer>"
        )
        return out

# Собирает XML offer (СЫРОЙ: без enrich/clean/compat/keywords/описания-шаблона)
# Нужен только для диагностики: "что адаптер отдал в core".

    # Собирает XML offer в "сыром" виде (до core: без enrich/clean/compat/keywords/шаблона description).
    # Нужно только для диагностики: сравнить docs/raw/*.yml (вход core) и docs/*.yml (выход core).
    def to_xml_raw(
        self,
        *,
        currency_id: str = CURRENCY_ID_DEFAULT,
    ) -> str:
        oid = xml_escape_attr(self.oid)
        avail = bool_to_xml(bool(self.available))

        name = xml_escape_text(fix_text(norm_ws(self.name)))
        vendor = xml_escape_text(fix_text(norm_ws(self.vendor)))
        pi = safe_int(self.price)
        price = int(pi) if pi is not None else 0

        # native_desc сохраняем максимально как есть (только делаем безопасным для XML)
        native_desc = fix_text(self.native_desc or "").replace("]]>", "]]&gt;")

        pics_xml = ""
        for pp in (self.pictures or []):
            pp2 = (pp or "").strip()
            if not pp2:
                continue
            pics_xml += f"\n<picture>{xml_escape_text(_cs_norm_url(pp2))}</picture>"

        params_xml = ""
        for k, v in (self.params or []):
            kk = xml_escape_attr(norm_ws(k))
            # сырое: не выводим служебные/отладочные параметры
            if re.fullmatch(r"(?i)товаров:\s*\d{1,7}", kk or ""):
                continue
            vv = xml_escape_text(fix_text(norm_ws(v)))
            if not kk or not vv:
                continue
            params_xml += f"\n<param name=\"{kk}\">{vv}</param>"

        out = (
            f"<offer id=\"{oid}\" available=\"{avail}\">\n"
            f"<categoryId></categoryId>\n"
            f"<vendorCode>{xml_escape_text(self.oid)}</vendorCode>\n"
            f"<name>{name}</name>\n"
            f"<price>{price}</price>"
            f"{pics_xml}\n"
            f"<vendor>{vendor}</vendor>\n"
            f"<currencyId>{xml_escape_text(currency_id)}</currencyId>\n"
            f"<description><![CDATA[\n{native_desc}]]></description>"
            f"{params_xml}\n"
            f"</offer>"
        )
        return out


# Валидирует готовый CS-фид (страховка: если что-то сломалось — падаем сборкой)
def _cs_build_description(*args, **kwargs):
    return build_description(*args, **kwargs)

def _cs_build_chars_block(*args, **kwargs):
    return build_chars_block(*args, **kwargs)
