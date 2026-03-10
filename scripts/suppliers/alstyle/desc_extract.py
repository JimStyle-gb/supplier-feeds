# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/desc_extract.py

AlStyle description -> params extraction.

v122:
- чинит ложный парсинг "Цвет печати" -> ("Цвет", "печати");
- режет кабельные хвосты у цвета;
- режет хвосты цвета вида "Черный Ресурс картриджа, стр 3100" -> "Чёрный";
- режет хвосты цвета вида "Пурпурный Секция аппарата Фотопроводник" -> "Пурпурный";
- нормализует технологию: "Термоструйная Количество цветов 5" -> "Термоструйная";
- дочищает compatibility-кандидаты от "Комплект поставки / Описание / Особенности";
- не считает singular "Устройство" совместимостью;
- сохраняет длинную совместимость и multiline label/value кейсы.
"""

from __future__ import annotations

import re
from typing import Any

from cs.util import norm_ws
from suppliers.alstyle.compat import clean_compatibility_text, dedupe_code_series_text
from suppliers.alstyle.desc_clean import clean_desc_text_for_extraction
from suppliers.alstyle.params_xml import apply_value_normalizers, key_quality_ok


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
_DESC_COLOR_PRINT_LABEL_ONLY_RE = re.compile(
    r"^\s*Цвет\s+печати\s*:?\s*$",
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
    r"VersaLink|AltaLink|WorkCentre|WorkCenter|DocuCentre|imageRUNNER|ImagePROGRAF|imagePRESS|i-SENSYS|ECOSYS|bizhub|PIXMA|Phaser|ColorQube|CopyCentre)\b",
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
_LEADING_COMPAT_NOISE_RE = re.compile(
    r"(?iu)^(?:Комплект\s+поставки|Описание|Особенности|Преимущества)\s+"
)
_TECH_STOP_RE = re.compile(
    r"(?iu)\b(?:Количество\s+цветов|Цвет(?:\s+печати)?|Совместимость|Устройства|Ресурс|Ресурс\s+картриджа|Количество\s+страниц|Тип\s+чернил|Об(?:ъ|ь)ем\s+картриджа|Секция\s+аппарата|серия)\b"
)
_TECH_VALUE_RE = re.compile(
    r"(?iu)\b("
    r"Лазерная(?:\s+монохромная|\s+цветная)?|"
    r"Светодиодная(?:\s+монохромная|\s+цветная)?|"
    r"Термоструйная|"
    r"Струйная|"
    r"Матричная|"
    r"Термосублимационная"
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
    "Модель",
    "Аналог модели",
    "Совместимость",
    "Совместимые модели",
    "Устройства",
    "Устройство",
    "Для принтеров",
    "Производитель",
    "Технология печати",
    "Цвет печати",
    "Цвет",
    "Ресурс картриджа, cтр.",
    "Ресурс картриджа, стр.",
    "Ресурс картриджа",
    "Количество страниц",
    "Ресурс",
    "Емкость лотка",
    "Ёмкость лотка",
    "Емкость",
    "Ёмкость",
    "Объем картриджа, мл",
    "Объём картриджа, мл",
    "Степлирование",
    "Дополнительные опции",
    "Применение",
    "Количество в упаковке",
    "Колличество в упаковке",
]
_COMPACT_LABEL_RE = re.compile(
    r"\b(?:Характеристики|Основные характеристики|Технические характеристики)\b\s*:?\s*|"
    r"\b(" + "|".join(re.escape(x) for x in sorted(_COMPACT_LABELS, key=len, reverse=True)) + r")\b(?:\s*[:\-–—]\s*|\s+)",
    re.IGNORECASE,
)
_LABEL_ONLY_RE = re.compile(
    r"^(?:"
    + "|".join(
        re.escape(x)
        for x in sorted(
            _COMPACT_LABELS + ["Характеристики", "Основные характеристики", "Технические характеристики"],
            key=len,
            reverse=True,
        )
    )
    + r")\s*:?$",
    re.IGNORECASE,
)

_BAD_COLOR_VALUES = {
    "печати",
    "цвет печати",
    "печати.",
    "печати:",
}
_MODEL_GARBAGE_RE = re.compile(
    r"(?iu)\b(?:зависит\s+от\s+конфигурации|модель\s+зависит\s+от\s+конфигурации|определяется\s+конфигурацией)\b"
)
_COLOR_REJECT_RE = re.compile(r"(?iu)\b(?:серия|Vivobook|Vector|Gaming|игровой|игровая|дизайн|корпус)\b")
_COMPAT_SENTENCE_NOISE_SPLIT_RE = re.compile(
    r"(?iu)\b(?:Преимущества|Комплектация|Условия\s+гарантии|Примечание|Примечания|Особенности|Описание|"
    r"Гарантированн(?:ый|ого)\s+об(?:ъ|ь)ем\s+отпечатков|при\s+5%\s+заполнении|формата\s+A4|"
    r"только\s+для\s+продажи\s+на\s+территории)\b"
)

_COLOR_RESOURCE_STOP_RE = re.compile(
    r"(?iu)\b(?:Ресурс(?:\s+картриджа)?|Количество\s+страниц|стр\.?)\b"
)
_COLOR_TECH_TAIL_RE = re.compile(
    r"(?iu)\b("
    r"коробка|бухта|метр(?:а|ов)?|305\s*м|100\s*м|500\s*м|"
    r"для\s+групповой|внутри\s+помещений|внешняя\s+оболочка|полиолефин|"
    r"lszh|pvc|u/utp|f/utp|s/ftp|cat5e|cat6|cat6a|"
    r"кабель|кабеля|провод|lan|витая\s+пара|"
    r"секция\s+аппарата|фотопроводник|фотобарабан|блок\s+проявки|девелопер|drum\s+unit"
    r")\b"
)

_COLOR_WORD_RE = re.compile(
    r"(?iu)\b("
    r"ч[её]рн(?:ый|ая|ое|ые)?|"
    r"бел(?:ый|ая|ое|ые)?|"
    r"сер(?:ый|ая|ое|ые)?|"
    r"син(?:ий|яя|ее|ие)?|"
    r"голуб(?:ой|ая|ое|ые)?|"
    r"красн(?:ый|ая|ое|ые)?|"
    r"малинов(?:ый|ая|ое|ые)?|"
    r"пурпурн(?:ый|ая|ое|ые)?|"
    r"ж[её]лт(?:ый|ая|ое|ые)?|"
    r"зел[её]н(?:ый|ая|ое|ые)?|"
    r"оранжев(?:ый|ая|ое|ые)?|"
    r"фиолетов(?:ый|ая|ое|ые)?|"
    r"коричнев(?:ый|ая|ое|ые)?|"
    r"розов(?:ый|ая|ое|ые)?|"
    r"бежев(?:ый|ая|ое|ые)?|"
    r"прозрачн(?:ый|ая|ое|ые)?|"
    r"серебрист(?:ый|ая|ое|ые)?|"
    r"золотист(?:ый|ая|ое|ые)?|"
    r"многоцветн(?:ый|ая|ое|ые)?|"
    r"фоточерн(?:ый|ая|ое|ые)?|"
    r"матовый\s+черный|"
    r"светло-?пурпурн(?:ый|ая|ое|ые)?|"
    r"фотоголуб(?:ой|ая|ое|ые)?|"
    r"фотопурпурн(?:ый|ая|ое|ые)?|"
    r"фотосер(?:ый|ая|ое|ые)?"
    r")\b"
)
_COLOR_PREFIX_RE = re.compile(
    r"(?iu)^(?:цвет\s+печати|печати|цвет\s+внешней\s+оболочки|внешней\s+оболочки|цвет\s+оболочки|оболочки|цвет\s+корпуса|корпуса)\s+"
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

    s = re.sub(r"(?iu)^(Устройства|Устройство|Совместимые\s+модели|Для\s+принтеров)\s+", "", s)

    while True:
        nxt = _LEADING_COMPAT_NOISE_RE.sub("", s).strip()
        if nxt == s:
            break
        s = nxt

    s = clean_compatibility_text(s)

    while True:
        nxt = _LEADING_COMPAT_NOISE_RE.sub("", s).strip()
        if nxt == s:
            break
        s = nxt

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

    if len(v) <= 520 and word_count <= 90:
        if has_brand and model_count >= 1:
            return True
        if model_count >= 2 and ("/" in v or "," in v):
            return True

    if len(v) <= 1800 and word_count <= 260:
        if has_brand and model_count >= 2:
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


def _canon_color_word(word: str) -> str:
    low = norm_ws(word).casefold().replace("ё", "е")
    if low.startswith("черн"):
        return "Чёрный"
    if low.startswith("бел"):
        return "Белый"
    if low.startswith("сер"):
        return "Серый"
    if low.startswith("син"):
        return "Синий"
    if low.startswith("голуб"):
        return "Голубой"
    if low.startswith("красн"):
        return "Красный"
    if low.startswith("малинов"):
        return "Малиновый"
    if low.startswith("пурпурн"):
        return "Пурпурный"
    if low.startswith("желт"):
        return "Жёлтый"
    if low.startswith("зелен"):
        return "Зелёный"
    if low.startswith("оранжев"):
        return "Оранжевый"
    if low.startswith("фиолетов"):
        return "Фиолетовый"
    if low.startswith("коричнев"):
        return "Коричневый"
    if low.startswith("розов"):
        return "Розовый"
    if low.startswith("бежев"):
        return "Бежевый"
    if low.startswith("прозрачн"):
        return "Прозрачный"
    if low.startswith("серебрист"):
        return "Серебристый"
    if low.startswith("золотист"):
        return "Золотистый"
    if low.startswith("многоцветн"):
        return "Многоцветный"
    if low.startswith("фоточерн"):
        return "Фоточёрный"
    if low.startswith("матовый черн"):
        return "Матовый чёрный"
    if low.startswith("светло-пурпур"):
        return "Светло-пурпурный"
    if low.startswith("фотоголуб"):
        return "Фотоголубой"
    if low.startswith("фотопурпур"):
        return "Фотопурпурный"
    if low.startswith("фотосер"):
        return "Фотосерый"
    return word


def _normalize_color_candidate(val: str) -> str:
    s = norm_ws(val).strip(" ;,.-")
    if not s:
        return ""
    s = _COLOR_PREFIX_RE.sub("", s).strip(" ;,.-")
    if not s:
        return ""
    if s.casefold() in _BAD_COLOR_VALUES:
        return ""

    m = _COLOR_RESOURCE_STOP_RE.search(s)
    if m and m.start() >= 1:
        s = s[:m.start()].strip(" ;,.-")

    pure_parts = [norm_ws(x) for x in re.split(r"\s*[,/;]\s*", s) if norm_ws(x)]
    if pure_parts and all(_COLOR_WORD_RE.fullmatch(x) for x in pure_parts):
        return ", ".join(_canon_color_word(x) for x in pure_parts)

    if _COLOR_TECH_TAIL_RE.search(s):
        m = _COLOR_WORD_RE.search(s)
        if not m:
            return ""
        return _canon_color_word(m.group(1))

    if _COLOR_WORD_RE.fullmatch(s):
        return _canon_color_word(s)

    return s


def _normalize_technology_candidate(val: str) -> str:
    s = norm_ws(val).strip(" ;,.-")
    if not s:
        return ""
    m = _TECH_STOP_RE.search(s)
    if m and m.start() >= 1:
        s = s[:m.start()].strip(" ;,.-")
    m = _TECH_VALUE_RE.search(s)
    if m:
        return norm_ws(m.group(1))
    return s


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
        key = canon_desc_spec_key(m.group(1))
        val = norm_ws(m.group(2))

        if key == "Цвет" and val.casefold() in _BAD_COLOR_VALUES:
            return None

        return (key, val)

    m = _DESC_COMPAT_LINE_RE.match(raw)
    if m:
        return ("Совместимость", norm_ws(m.group(1)))

    if _DESC_TECH_PRINT_LABEL_ONLY_RE.match(ln):
        return None

    if _DESC_COLOR_PRINT_LABEL_ONLY_RE.match(ln):
        return None

    labels = sorted(_COMPACT_LABELS, key=len, reverse=True)
    for label_raw in labels:
        rx = re.match(rf"(?iu)^({re.escape(label_raw)})\s+(.+?)$", ln)
        if not rx:
            continue

        if norm_ws(label_raw).casefold() == "устройство":
            return None

        key = canon_desc_spec_key(rx.group(1))
        val = norm_ws(rx.group(2))

        if key == "Цвет" and val.casefold() in _BAD_COLOR_VALUES:
            return None

        return (key, val)

    return None


def _inline_label_pattern() -> str:
    labels = sorted(_COMPACT_LABELS, key=len, reverse=True)
    return "|".join(re.escape(x) for x in labels)


def split_inline_desc_pairs(line: str) -> list[str]:
    ln = norm_ws(line)
    if not ln:
        return []
    key_pat = _inline_label_pattern()
    rx = re.compile(rf"(?iu)(?=\b(?:{key_pat})\b\s*(?::|[-–—]|\s+))")
    parts = [norm_ws(x) for x in rx.split(ln) if norm_ws(x)]
    return parts if len(parts) > 1 else [ln]


def extract_compact_labeled_sequences(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    matches = list(_COMPACT_LABEL_RE.finditer(text))
    if not matches:
        return out

    for i, m in enumerate(matches):
        label = norm_ws(m.group(1) or "")
        if not label or label.casefold() in {
            "характеристики",
            "основные характеристики",
            "технические характеристики",
            "устройство",
        }:
            continue

        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        key = canon_desc_spec_key(label)
        value = norm_ws(text[start:end])
        value = re.sub(r"(?iu)^[:\-–—]\s*", "", value)
        value = value.strip(" ;,.-")

        if not value or _LABEL_ONLY_RE.match(value):
            continue

        if key == "Цвет" and value.casefold() in _BAD_COLOR_VALUES:
            continue

        out.append((key, value))

    return out


def extract_multiline_label_value_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    lines = [norm_ws(x) for x in text.splitlines() if norm_ws(x)]
    if not lines:
        return out

    labels_cf = {x.casefold() for x in _COMPACT_LABELS}
    i = 0
    while i < len(lines):
        ln = lines[i]
        ln_cf = ln.casefold().rstrip(":")
        if ln_cf in labels_cf and ln_cf != "устройство":
            key = canon_desc_spec_key(ln.rstrip(":"))
            vals: list[str] = []
            j = i + 1
            while j < len(lines):
                nxt = lines[j]
                nxt_cf = nxt.casefold().rstrip(":")
                if nxt_cf in labels_cf or _DESC_SPEC_STOP_RE.match(nxt):
                    break
                vals.append(nxt)
                j += 1
            value = norm_ws(" ".join(vals)).strip(" ;,.-")
            if value and not (key == "Цвет" and value.casefold() in _BAD_COLOR_VALUES):
                out.append((key, value))
            i = j
            continue
        i += 1
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

    joined = "\n".join(lines)
    out.extend(extract_compact_labeled_sequences(joined))
    out.extend(extract_multiline_label_value_pairs(joined))
    return out


def extract_sentence_compat_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    for rx in (_DESC_COMPAT_SENTENCE_RE, _DESC_FOR_DEVICES_SENTENCE_RE):
        for m in rx.finditer(text):
            cand = norm_ws(m.group(1))
            if not cand:
                continue
            cand = _COMPAT_SENTENCE_NOISE_SPLIT_RE.split(cand, maxsplit=1)[0].strip(" ;,.-")
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

    if key == "Цвет":
        val2 = _normalize_color_candidate(val2)
        if not val2 or val2.casefold() in _BAD_COLOR_VALUES:
            return None
        if _COLOR_REJECT_RE.search(val2) and not _COLOR_WORD_RE.search(val2):
            return None

    if key == "Технология":
        val2 = _normalize_technology_candidate(val2)

    if key == "Совместимость":
        val2 = _normalize_compat_candidate(val2)
        if not looks_like_compatibility_value(val2):
            return None
    elif key in {"Модель", "Аналог модели"}:
        val2 = dedupe_code_series_text(val2)
        if _MODEL_GARBAGE_RE.search(val2):
            return None
        if val2.endswith(")"):
            return None
    elif key == "Ресурс":
        if not looks_like_resource_value(val2):
            return None
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
    best_compat: tuple[str, str] | None = None

    for key, val in candidates:
        checked = validate_desc_pair(key, val, schema)
        if not checked:
            continue

        if checked[0] == "Ресурс":
            if best_resource is None or len(checked[1]) < len(best_resource[1]):
                best_resource = checked
            continue

        if checked[0] == "Совместимость":
            if best_compat is None or len(checked[1]) > len(best_compat[1]):
                best_compat = checked
            continue

        sig = (checked[0].casefold(), checked[1].casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append(checked)

    if best_compat is not None:
        out = [x for x in out if x[0] != "Совместимость"]
        out.append(best_compat)

    if best_resource is not None:
        out = [x for x in out if x[0] != "Ресурс"]
        out.append(best_resource)

    return out
