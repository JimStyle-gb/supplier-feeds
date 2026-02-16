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

from __future__ import annotations


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

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo
import os
import hashlib
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP



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

# Лимиты (по умолчанию):
# - <name> держим коротким и читаемым (150 по решению пользователя)
# - <keywords> по правилам YML обычно <= 1024
CS_NAME_MAX_LEN = int((os.getenv("CS_NAME_MAX_LEN", "150") or "150").strip() or "150")
CS_KEYWORDS_MAX_LEN = int((os.getenv("CS_KEYWORDS_MAX_LEN", "1024") or "1024").strip() or "1024")

CS_COMPAT_CLEAN_YIELD_PACK = (os.getenv("CS_COMPAT_CLEAN_YIELD_PACK", "1") or "1").strip().lower() not in ("0", "false", "no")
CS_COMPAT_CLEAN_PAPER_OS_DIM = (os.getenv("CS_COMPAT_CLEAN_PAPER_OS_DIM", "1") or "1").strip().lower() not in ("0", "false", "no")
CS_COMPAT_CLEAN_NOISE_WORDS = (os.getenv("CS_COMPAT_CLEAN_NOISE_WORDS", "1") or "1").strip().lower() not in ("0", "false", "no")
CS_COMPAT_CLEAN_REPEAT_BLOCKS = (os.getenv("CS_COMPAT_CLEAN_REPEAT_BLOCKS", "1") or "1").strip().lower() not in ("0", "false", "no")




# Заглушка картинки, если у оффера нет фото (можно переопределить env CS_PICTURE_PLACEHOLDER_URL)
CS_PICTURE_PLACEHOLDER_URL = (os.getenv("CS_PICTURE_PLACEHOLDER_URL") or "https://placehold.co/800x800/png?text=No+Photo").strip()
# Хвост городов (один и тот же для всех поставщиков)
CS_CITY_TAIL = (
    "Казахстан, Алматы, Нур-Султан, Астана, Шымкент, Караганда, Актобе, Тараз, Павлодар, Усть-Каменогорск, Усть Каменогорск, Оскемен, Семей, Уральск, Орал, Темиртау, Костанай, Кызылорда, Атырау, Актау, Кокшетау, Петропавловск, Талдыкорган, Туркестан"
)
# WhatsApp блок (единый)
CS_WA_BLOCK = (
    "<!-- WhatsApp -->\n"
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; "
    "border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);\">"
    "&#128172; Написать в WhatsApp</a></p></div>"
)

# WhatsApp div (без комментария, чтобы комментарии строились шаблоном)
CS_WA_DIV = (
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; "
    "border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);\">"
    "&#128172; Написать в WhatsApp</a></p></div>"
)

# Горизонтальная линия (2px)
CS_HR_2PX = "<hr style=\"border:none; border-top:2px solid #E7D6B7; margin:12px 0;\" />"

# Оплата/Доставка — КАНОНИЧЕСКИЙ текст (как в твоём эталоне)
CS_PAY_BLOCK = (
    "<!-- Оплата и доставка -->\n"
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<div style=\"background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;\">"
    "<h3 style=\"margin:0 0 8px; font-size:17px;\">Оплата</h3>"
    "<ul style=\"margin:0; padding-left:18px;\">"
    "<li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li>"
    "<li><strong>Удалённая оплата</strong> по <span style=\"color:#8b0000;\"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li>"
    "</ul>"
    "<hr style=\"border:none; border-top:1px solid #E7D6B7; margin:12px 0;\" />"
    "<h3 style=\"margin:0 0 8px; font-size:17px;\">Доставка по Алматы и Казахстану</h3>"
    "<ul style=\"margin:0; padding-left:18px;\">"
    "<li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li>"
    "<li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5 000 тг. | 3–7 рабочих дней</em></li>"
    "<li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>"
    "<li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li>"
    "</ul>"
    "</div></div>"
)

# Параметры, которые нужно выкидывать из <param> и из "Характеристик"
PARAM_DROP_DEFAULT = {
    "Штрихкод",
    "Артикул",
    "Новинка",
    "Снижена цена",
    "Благотворительность",
    "Код товара Kaspi",
    "Код ТН ВЭД",
    "Назначение",
}
# Кеш: служебные параметры в casefold (для clean_params/валидации)
PARAM_DROP_DEFAULT_CF = {str(x).strip().casefold() for x in PARAM_DROP_DEFAULT}


# Возвращает текущее время в Алматы
def now_almaty() -> datetime:
    forced = (os.getenv("CS_FORCE_BUILD_TIME_ALMATY", "") or "").strip()
    if forced:
        try:
            return datetime.strptime(forced, "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return datetime.now(ZoneInfo(ALMATY_TZ)).replace(tzinfo=None)


# Считает ближайший запуск на заданный час (Алматы) — для FEED_META
def next_run_at_hour(now_local: datetime, hour: int) -> datetime:
    hour = int(hour)
    candidate = now_local.replace(hour=hour, minute=0, second=0, microsecond=0)
    if candidate <= now_local:
        candidate = candidate + timedelta(days=1)
    return candidate


# Нормализует пробелы/переводы строк в строке
def norm_ws(s: str) -> str:
    s2 = (s or "").replace("\u00a0", " ").strip()
    s2 = re.sub(r"\s+", " ", s2)
    s2 = fix_mixed_cyr_lat(s2)
    return s2.strip()

def normalize_offer_name(name: str) -> str:
    # CS: лёгкая типографика имени (без изменения смысла)
    s = norm_ws(name)
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
    # убрать лишние пробелы внутри скобок
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    # хвостовая запятая
    s = re.sub(r",\s*$", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
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


def _truncate_text(s: str, max_len: int, *, suffix: str = "") -> str:
    # CS: безопасно режем строку по границе слова/запятой
    s = norm_ws(s)
    if max_len <= 0:
        return ""
    if len(s) <= max_len:
        return s

    cut_len = max_len - len(suffix)
    if cut_len <= 0:
        return suffix[:max_len]

    chunk = s[:cut_len].rstrip()
    # режем по последней "хорошей" границе
    for sep in (",", " ", "/", ";"):
        j = chunk.rfind(sep)
        if j >= max(0, cut_len - 40):  # не уходим слишком далеко назад
            chunk = chunk[:j].rstrip(" ,/;")
            break

    chunk = chunk.rstrip(" ,/;")
    if suffix:
        return (chunk + suffix)[:max_len]
    return chunk


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
    return _truncate_text(name, max_len, suffix="…")


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
        vv = norm_ws(v)
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
    }
)

_RE_WORDLIKE = re.compile(r"[0-9A-Za-zА-Яа-яЁё][0-9A-Za-zА-Яа-яЁё._\-/+]*")
_RE_LETTER_SEQ = re.compile(r"[A-Za-zА-Яа-яЁё]+")


def fix_mixed_cyr_lat(s: str) -> str:
    t = s or ""
    if not t:
        return t

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
            if lat >= 2 and cyr <= 2:
                return w.translate(_CYR_TO_LAT)
            if cyr >= 2 and lat <= 2:
                return w.translate(_LAT_TO_CYR)
            return w
        # Иначе — аккуратно чиним смешанные последовательности букв
        return _RE_LETTER_SEQ.sub(lambda mm: _fix_letters(mm.group(0)), w)

    return _RE_WORDLIKE.sub(_sub, t)

# Безопасное int из любого значения
def safe_int(v) -> int | None:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        s = s.replace(" ", "").replace("\u00a0", "")
        # иногда цена приходит как "12 345.00"
        s = s.split(".")[0]
        return int(s)
    except Exception:
        return None


# Парсит множество id из env (например "1,10,20") или из fallback списка
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
def xml_escape_text(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# XML escape для атрибутов
def xml_escape_attr(s: str) -> str:
    return xml_escape_text(s).replace('"', "&quot;")


# bool → "true/false"
def bool_to_xml(v: bool) -> str:
    return "true" if bool(v) else "false"


# Каноническое правило цены (4% + надбавки + хвост 900; невалидно/<=100 → 100; >=9,000,000 → 100)
# Тарифные пороги для compute_price (как в эталоне)
CS_PRICE_TIERS = [
    (101, 10_000, 3_000),
    (10_001, 25_000, 4_000),
    (25_001, 50_000, 5_000),
    (50_001, 75_000, 7_000),
    (75_001, 100_000, 10_000),
    (100_001, 150_000, 12_000),
    (150_001, 200_000, 15_000),
    (200_001, 300_000, 20_000),
    (300_001, 500_000, 25_000),
    (500_001, 750_000, 30_000),
    (750_001, 1_000_000, 35_000),
    (1_000_001, 1_500_000, 40_000),
    (1_500_001, 2_000_000, 45_000),
]

def compute_price(price_in: int | None) -> int:
    p = safe_int(price_in)
    if p is None or p <= 100:
        return 100
    if p >= 9_000_000:
        return 100

    tiers = CS_PRICE_TIERS
    add = 60_000
    for lo, hi, a in tiers:
        if lo <= p <= hi:
            add = a
            break

    raw = int(p * 1.04 + add)

    # "хвост 900" (всегда заканчиваем на 900)
    out = (raw // 1000) * 1000 + 900

    if out >= 9_000_000:
        return 100
    if out <= 100:
        return 100
    return out


# Убирает мусорные параметры, пустые значения и дубли (применять всегда!)

# Параметры "вес/габариты/объем" полезны покупателю, но у некоторых поставщиков бывают мусорные значения.
# Валидируем мягко: оставляем только "похожие на правду".
_DIM_WORDS = ("габарит", "размер", "длина", "ширина", "высота")
_VOL_WORDS = ("объем", "объём", "volume")
_WGT_WORDS = ("вес", "масса", "weight")

_RE_NUM = re.compile(r"(\d+(?:[\.,]\d+)?)")
_RE_DIM_SEP = re.compile(r"[xх×\*]", re.I)

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
        vv = norm_ws(v)
        if not vv:
            return ""
        vv = fix_mixed_cyr_lat(vv)
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

        # Совместимость — объединяем, но пропускаем мусор (маркетинг/предложения)
        if key_cf == "совместимость":
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
            v = ", ".join(vals)
            if len(v) > 260:
                v = v[:260].rstrip(" ,")
            out.append((name, v))
        else:
            out.append((name, vals[0]))

    return out

def apply_supplier_param_rules(params: Sequence[tuple[str, str]], oid: str, name: str) -> list[tuple[str, str]]:
    """Точечные правила по поставщикам/категориям для <param> и блока характеристик.
    - Удаляем служебные params (Артикул добавляется/может приходить извне)
    - VTT: удаляем Каталожный номер/Штрих-код/Аналоги, OEM-номер -> Партномер
    - CopyLine: для 'Кабель сетевой' удаляем 'Совместимость'
    """
    oid_u = (oid or "").upper()
    name_cf = (name or "").strip().casefold()
    is_vtt = oid_u.startswith("VT")
    is_copyline = oid_u.startswith("CL")
    out: list[tuple[str, str]] = []
    for k, v in params or []:
        kk = norm_ws(k)
        vv = norm_ws(v)
        if not kk or not vv:
            continue
        k_cf = kk.casefold().replace("ё", "е")
        # глобально: не выводим служебный 'Артикул' как характеристику
        if k_cf == "артикул":
            continue
        if is_vtt:
            # VTT: чистим служебные параметры
            if k_cf in {"каталожный номер", "аналоги", "аналог", "штрихкод", "штрих-код", "штрих код"}:
                continue
            # VTT: OEM-номер -> Партномер
            if k_cf in {"oem-номер", "oem номер", "oem", "oem номер детали", "oem номер/part number"}:
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
        md = re.match(r"^([^:]{1,80}?)\s*[–—-]\s*(.{1,250})$", ln)
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
                keyish = (
                    bool(re.search(r"\s", ln))
                    or bool(re.search(r"\d", ln))
                    or bool(re.search(r"[()/%×x]", ln))
                    or k_cf in {"тип", "вид", "цвет", "бренд", "марка", "модель", "разрешение", "интерфейс", "скорость", "формат", "размер", "вес", "объем", "объём", "емкость", "ёмкость", "ресурс", "гарантия", "питание"}
                    or len(ln) >= 15
                )
                if keyish and _is_kv_val(nxt):
                    # избегаем склейки простых списков в пары: пропускаем только если ключ = 1 слово
                    wcount = len(re.findall(r"[A-Za-zА-Яа-яЁё]+", ln))
                    if len(ln) <= 12 and len(nxt) <= 12 and (not re.search(r"\d", ln + nxt)) and wcount <= 1:
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
def build_keywords(
    vendor: str,
    name: str,
    *,
    city_tail: str | None = None,
    max_tokens: int = 18,
    extra: list[str] | None = None,
) -> str:
    vendor = norm_ws(vendor)
    name = norm_ws(name)

    def _kw_safe(s: str) -> str:
        # CS: чтобы <keywords> не "ломались" из-за запятых внутри имени
        s = str(s or "")
        s = s.replace(",", " ").replace(";", " ").replace("|", " ")
        return norm_ws(s).strip(" ,")

    # CS: лёгкая канонизация vendor для SEO/фильтров (без изменения name)
    def _canon_vendor(v: str) -> str:
        vv = norm_ws(v)
        if not vv:
            return ""
        vv_cf = vv.casefold().replace("ё", "е")
        vv_cf2 = vv_cf.replace(" ", "").replace("-", "")
        if vv_cf2 == "kyoceramita":
            return "Kyocera"
        return vv

    # Стоп-слова (мусорные токены)
    stop = {
        "и", "в", "на", "для", "с", "по", "от", "до", "к", "из", "при", "без",
        "шт", "pcs", "pc", "dr", "др",
    }

    # Маппинг одиночных букв (VTT/тонер/цвета)
    one_map = {
        "C": "голубой",
        "M": "пурпурный",
        "Y": "желтый",
        "K": "черный",
        "O": "оригинальный",
    }

    parts: list[str] = []
    v2 = _canon_vendor(vendor)
    if v2:
        parts.append(_kw_safe(v2))
    if name:
        parts.append(_kw_safe(name))

    # Разбор имени на слова (цифры/буквы, с дефисами)
    raw_tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+(?:-[A-Za-zА-Яа-яЁё0-9]+)*", name)
    used = 0
    for t in raw_tokens:
        if used >= max(0, int(max_tokens)):
            break
        tt = norm_ws(t)
        if not tt:
            continue

        low = tt.casefold().replace("ё", "е")

        # стоп-слова
        if low in stop:
            continue

        # одиночные символы: либо маппим, либо выбрасываем
        if len(tt) == 1:
            key = tt.upper()
            if key in one_map:
                tt = one_map[key]
                low = tt.casefold().replace("ё", "е")
            else:
                continue

        # одиночные цифры — мусор (2, 3), но нормальные числа (например 3020) оставляем
        if tt.isdigit() and len(tt) == 1:
            continue

        parts.append(tt)
        used += 1

    if extra:
        for x in extra:
            xx = norm_ws(str(x))
            if xx:
                parts.append(_kw_safe(xx))

    # Города добавляем единым хвостом (уже с запятыми). Если не передали — берём дефолт.
    ct = norm_ws(city_tail or CS_CITY_TAIL)
    if ct:
        parts.append(ct)

    # Уникализация (без учёта регистра)
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        if CS_FIX_KEYWORDS_DECIMAL_COMMA:
            p = _RE_DECIMAL_COMMA.sub(".", p)
        if CS_FIX_KEYWORDS_MULTI_COMMA:
            p = p.strip().strip(" ,")
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(p)

    return ", ".join(out)

_RE_PARAM_SENTENCEY = re.compile(
    r"(?i)\b(внимание|обратите|пожалуйста|важно|маркир|подлинност|original|оригинал|упаковк|предупрежден|рекомендуем|гаранти)\b"
)

def _is_sentence_like_param_name(k: str) -> bool:
    kk = norm_ws(k)
    if not kk:
        return False

    cf = kk.casefold()
    # исключение: это нормальные характеристики (оставляем в блоке "Характеристики")
    if (("рекомендуемая" in cf) or ("рекомендуемое" in cf)) and (("нагрузк" in cf) or ("количеств" in cf)):
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
        vv = norm_ws(v)
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


def build_chars_block(params_sorted: Sequence[tuple[str, str]]) -> str:
    items: list[str] = []
    for k, v in params_sorted or []:
        kk = xml_escape_text(norm_ws(k))
        vv = xml_escape_text(norm_ws(v))
        if not kk or not vv:
            continue
        items.append(f"<li><strong>{kk}:</strong> {vv}</li>")
    if not items:
        # CS: если характеристик нет — не выводим пустой блок
        return ""
    return "<h3>Характеристики</h3><ul>" + "".join(items) + "</ul>"

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
        vv = norm_ws(v)
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

def build_description(
    name: str,
    native_desc: str,
    params_sorted: Sequence[tuple[str, str]],
    *,
    notes: Sequence[str] | None = None,
    wa_block: str = CS_WA_DIV,
    hr_2px: str = CS_HR_2PX,
    pay_block: str = CS_PAY_BLOCK,
) -> str:
    n = norm_ws(name)
    n_esc = xml_escape_text(n)

    # Тело родного описания (без <h3>)
    desc_body = _build_desc_part(n, native_desc)

    # Если родного описания нет — берём короткий summary из параметров,
    # иначе (если и параметров нет) — короткий нейтральный фолбэк.
    if not desc_body:
        sm = _build_param_summary(params_sorted)
        if sm:
            desc_body = f"<p>{xml_escape_text(sm)}</p>"
        else:
            desc_body = "<p>Подробности уточняйте в WhatsApp.</p>"

    # Характеристики (если пусто — блок не выводим)
    chars = build_chars_block(params_sorted)

    # WA: страховка, если кто-то передал старый CS_WA_BLOCK с комментарием
    w = (wa_block or "").lstrip()
    if w.startswith("<!--"):
        w = re.sub(r"^<!--.*?-->\s*\n?", "", w, flags=re.S).strip()
    if not w:
        w = CS_WA_DIV

    parts: list[str] = []
    parts.append("<!-- Наименование товара -->")
    parts.append(f"<h3>{n_esc}</h3>")

    parts.append("<!-- WhatsApp -->")
    parts.append(hr_2px)
    parts.append(w)
    parts.append(hr_2px)

    parts.append("<!-- Описание -->")
    parts.append(desc_body)

    # Примечания (вынесены из "параметров-фраз", чтобы не засорять характеристики)
    if notes:
        nn: list[str] = []
        for x in (notes or [])[:2]:
            t = xml_escape_text(norm_ws(x))
            if t:
                # косметика: город и пунктуация
                t = t.replace("Нур: Султан", "Нур-Султан").replace("Нур : Султан", "Нур-Султан")
                t = re.sub(r"\s*:\s*", ": ", t)
                t = re.sub(r"(?:,\s*){2,}", ", ", t)
                t = re.sub(r":\s*:", ": ", t)
                t = re.sub(r"\s{2,}", " ", t).strip()
                # пробел после точки/воскл/вопрос/многоточия перед заглавной буквой
                t = re.sub(r"([.!?…])([A-ZА-ЯЁ])", r"\1 \2", t)
                # пробел между цифрой и кириллицей (>=1299Рекомендуемое -> >=1299 Рекомендуемое)
                t = re.sub(r"(\d)([А-Яа-яЁё])", r"\1 \2", t)
                if len(t) > 180:
                    t = t[:180].rstrip(" ,.;") + "…"
                nn.append(t)
        if nn:
            parts.append(f"<p><strong>Примечание:</strong> " + "<br>".join(nn) + "</p>")

    if chars:
        parts.append(chars)
    parts.append(pay_block)

    inner = "\n".join([p for p in parts if p is not None and str(p).strip() != ""])
    return normalize_cdata_inner(inner)

def make_feed_meta(
    supplier: str,
    supplier_url: str,
    build_time: datetime,
    next_run: datetime,
    *,
    before: int,
    after: int,
    in_true: int,
    in_false: int,
) -> str:
    lines = [
        "<!--FEED_META",
        f"Поставщик                                  | {supplier}",
        f"URL поставщика                             | {supplier_url}",
        f"Время сборки (Алматы)                      | {build_time:%Y-%m-%d %H:%M:%S}",
        f"Ближайшая сборка (Алматы)                  | {next_run:%Y-%m-%d %H:%M:%S}",
        f"Сколько товаров у поставщика до фильтра    | {before}",
        f"Сколько товаров у поставщика после фильтра | {after}",
        f"Сколько товаров есть в наличии (true)      | {in_true}",
        f"Сколько товаров нет в наличии (false)      | {in_false}",
        "-->",
    ]
    return "\n".join(lines)


# Верх файла (минимальный shop+offers; витрина будет в cs_price позже)
def make_header(build_time: datetime, *, encoding: str = OUTPUT_ENCODING_DEFAULT) -> str:
    return (
        f"<?xml version=\"1.0\" encoding=\"{encoding}\"?>\n"
        f"<yml_catalog date=\"{build_time:%Y-%m-%d %H:%M}\">\n"
        f"<shop><offers>\n"
    )


# Низ файла
def make_footer() -> str:
    return "</offers>\n</shop>\n</yml_catalog>\n"


# Гарантирует пустую строку после <offers> и перед </offers>
def ensure_footer_spacing(xml: str) -> str:
    xml = re.sub(r"(<offers>\n)(\n*)", r"\1\n", xml, count=1)
    xml = re.sub(r"(</offer>\n)(</offers>)", r"\1\n\2", xml)
    return xml


# CS: публичный vendor (нельзя светить названия поставщиков)
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
def build_cs_feed_xml(
    offers: Sequence["OfferOut"],
    *,
    supplier: str,
    supplier_url: str,
    build_time: datetime,
    next_run: datetime,
    before: int,
    encoding: str = OUTPUT_ENCODING_DEFAULT,
    public_vendor: str = "CS",
    currency_id: str = CURRENCY_ID_DEFAULT,
    param_priority: Sequence[str] | None = None,
) -> str:
    after = len(offers)
    in_true = sum(1 for o in offers if getattr(o, "available", False))
    in_false = after - in_true
    meta = make_feed_meta(
        supplier=supplier,
        supplier_url=supplier_url,
        build_time=build_time,
        next_run=next_run,
        before=before,
        after=after,
        in_true=in_true,
        in_false=in_false,
    )

    offers_xml = ""
    if offers:
        offers_xml = "\n\n".join(
            [
                o.to_xml(
                    currency_id=currency_id,
                    public_vendor=public_vendor,
                    param_priority=param_priority,
                )
                for o in offers
            ]
        )

    xml = make_header(build_time, encoding=encoding) + "\n" + meta + "\n\n" + offers_xml + "\n\n" + make_footer()
    return ensure_footer_spacing(xml)


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
    validate_cs_yml(full)
    return write_if_changed(out_file, full, encoding=encoding)



# Пишет файл только если изменился (атомарно)
def write_if_changed(path: str, data: str, *, encoding: str = OUTPUT_ENCODING_DEFAULT) -> bool:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    new_bytes = data.encode(encoding, errors="strict")

    if p.exists():
        old = p.read_bytes()
        if old == new_bytes:
            return False

    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_bytes(new_bytes)
    tmp.replace(p)
    return True


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
    "euro print": "Euro Print",
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
    "europrint": "Euro Print",
    "brothe": "Brother",
}


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
        v = "Euro Print"
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
def pick_vendor(
    vendor_src: str,
    name: str,
    params: Sequence[tuple[str, str]],
    desc_html: str,
    *,
    public_vendor: str = "CS",
) -> str:
    # CS: vendor выбираем единообразно для всех поставщиков:
    # 1) если vendor_src задан адаптером — используем его,
    # 2) иначе ищем бренд по имени товара,
    # 3) затем по описанию,
    # 4) затем по параметрам,
    # 5) иначе fallback (public_vendor), который НЕ должен быть названием поставщика.
    v = norm_ws(vendor_src)
    if v:
        return normalize_vendor(v)

    # CS: спец-правило для SMART интерактивных панелей (модельный префикс SBID-)
    if name and re.search(r"\bSBID-", name, flags=re.IGNORECASE):
        return "SMART"

    def _find_in(text: str) -> str:
        if not text:
            return ""
        hay = text.lower()
        best_canon = ""
        best_pos = 10**9
        for key, canon in CS_BRANDS_MAP.items():
            m = re.search(rf"\b{re.escape(key)}\b", hay)
            if m:
                pos = m.start()
                if pos < best_pos:
                    best_pos = pos
                    best_canon = canon
        return best_canon

    # 1) name
    cand = _find_in(name or "")
    if cand:
        return cand

    # 2) description (HTML)
    cand = _find_in(desc_html or "")
    if cand:
        return cand

    # 3) params
    if params:
        joined = " ".join([f"{k} {val}" for k, val in params])
        cand = _find_in(joined)
        if cand:
            return cand

    return norm_ws(public_vendor)
@dataclass
class OfferOut:
    oid: str
    available: bool
    name: str
    price: int
    pictures: list[str]
    vendor: str
    params: list[tuple[str, str]]
    native_desc: str

    # Собирает XML offer (фиксированный порядок)
    def to_xml(
        self,
        *,
        currency_id: str = CURRENCY_ID_DEFAULT,
        city_tail: str = CS_CITY_TAIL,
        public_vendor: str = "CS",
        param_priority: Sequence[str] | None = None,
    ) -> str:
        name_full = normalize_offer_name(self.name)
        native_desc = fix_text(self.native_desc)
        # Вытаскиваем тех/осн характеристики из нативного описания в params, чтобы не было дублей
        native_desc, _spec_pairs = extract_specs_pairs_and_strip_desc(native_desc)
        native_desc = strip_service_kv_lines(native_desc)
        vendor = pick_vendor(self.vendor, name_full, self.params, native_desc, public_vendor=public_vendor)

        # тройное обогащение: params + из описания
        params = list(self.params)
        if _spec_pairs:
            params.extend(_spec_pairs)
        enrich_params_from_desc(params, native_desc)
        enrich_params_from_name_and_desc(params, name_full, native_desc)
        # CS: Совместимость не обогащаем и не создаём автоматически (по просьбе пользователя).

        # чистим и сортируем (ВАЖНО: чистить всегда)
        params = clean_params(params)
        params = apply_supplier_param_rules(params, self.oid, name_full)
        params = apply_color_from_name(params, name_full)
        params_sorted = sort_params(params, priority=list(param_priority or []))

                # выносим "параметры-фразы" в примечания и оставляем чистые характеристики
        params_sorted, notes = split_params_for_chars(params_sorted)

        # CS: лимитируем <name> (умно для NVPrint)
        name_short = enforce_name_policy(self.oid, name_full, params_sorted)

        # CS: В описании сохраняем полное наименование (если оно было укорочено).
        # Если <name> был укорочен — в описании сохраняем полное наименование.
        name_for_desc = name_full if (name_short != name_full) else name_short

        desc_cdata = build_description(name_for_desc, native_desc, params_sorted, notes=notes)
        keywords = build_keywords(vendor, name_short, city_tail=city_tail)
        keywords = _truncate_text(keywords, CS_KEYWORDS_MAX_LEN)

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

        # Политика availability по поставщику:
        # - AlStyle (AS) и AkCent (AC): как у поставщика
        # - CopyLine (CL), NVPrint (NP), VTT (VT): всегда true
        oid_u = (self.oid or "").upper()
        avail_effective = bool(self.available)
        if oid_u.startswith(("CL", "NP", "VT")):
            avail_effective = True

        out = (
            f"<offer id=\"{xml_escape_attr(self.oid)}\" available=\"{bool_to_xml(bool(avail_effective))}\">\n"
            f"<categoryId></categoryId>\n"
            f"<vendorCode>{xml_escape_text(self.oid)}</vendorCode>\n"
            f"<name>{xml_escape_text(name_short)}</name>\n"
            f"<price>{int(self.price)}</price>"
            f"{pics_xml}\n"
            f"<vendor>{xml_escape_text(vendor)}</vendor>\n"
            f"<currencyId>{xml_escape_text(currency_id)}</currencyId>\n"
            f"<description><![CDATA[\n{desc_cdata}]]></description>"
            f"{params_xml}\n"
            f"<keywords>{xml_escape_text(keywords)}</keywords>\n"
            f"</offer>"
        )
        return out

# Валидирует готовый CS-фид (страховка: если что-то сломалось — падаем сборкой)
def validate_cs_yml(xml: str) -> None:
    errors: list[str] = []

    # Глобальные запреты
    if "<available>" in xml:
        errors.append("Найден тег <available> (должен быть только available=\"true/false\" в <offer>).")

    # Shuko не должно встречаться вообще
    if re.search(r"\bShuko\b", xml, flags=re.I):
        errors.append("Найдено слово 'Shuko' (нужно 'Schuko').")

    # Служебные параметры не должны просачиваться
    drop_names = PARAM_DROP_DEFAULT_CF

    # Прогон по офферам
    in_offer = False
    offer_id = ""
    has_picture = False
    vendor_code = ""
    keywords = ""
    price_ok = True
    ids_seen: set[str] = set()
    hash_like_ids: list[str] = []
    _RE_HASH_OID = re.compile(r"^(AC|AS|CL|NP)H[0-9A-F]{10}$")  # VT может иметь OEM-коды вида VTH... — не считаем это hash-id

    bad_no_pic: list[str] = []
    bad_vendorcode: list[str] = []
    bad_keywords: list[str] = []
    bad_params: list[str] = []
    bad_price: list[str] = []
    dup_ids: list[str] = []

    # Для keywords может быть много текста — берём по строке (рендер у нас одно-строчный)
    for line in xml.splitlines():
        s = line.strip()

        if s.startswith("<offer ") and 'id="' in s:
            in_offer = True
            has_picture = False
            vendor_code = ""
            keywords = ""
            price_ok = True

            m = re.search(r'id="([^"]+)"', s)
            offer_id = m.group(1) if m else ""
            if offer_id:
                if offer_id in ids_seen:
                    dup_ids.append(offer_id)
                ids_seen.add(offer_id)
                if _RE_HASH_OID.match(offer_id):
                    hash_like_ids.append(offer_id)
            continue

        if not in_offer:
            continue

        if "<picture>" in s:
            has_picture = True

        if s.startswith("<vendorCode>"):
            vendor_code = re.sub(r"</?vendorCode>", "", s).strip()

        if s.startswith("<keywords>"):
            kw = re.sub(r"</?keywords>", "", s).strip()
            keywords = kw

        if s.startswith("<price>"):
            pr = re.sub(r"</?price>", "", s).strip()
            pi = safe_int(pr)
            if pi is None or pi < 100:
                price_ok = False

        # param проверки
        if s.startswith("<param ") and 'name="' in s:
            mname = re.search(r'name="([^"]+)"', s)
            pname = mname.group(1) if mname else ""
            pname_n = norm_ws(pname)
            pname_key = pname_n.casefold()

            # служебные/запрещённые
            if pname_key in drop_names:
                bad_params.append(f"{offer_id}: запрещённый param '{pname_n}'")

            # Bт не должно быть
            if re.search(r"Bт", pname_n):
                bad_params.append(f"{offer_id}: param содержит 'Bт' -> '{pname_n}'")

            # значение
            # <param name="X">VALUE</param>
            mv = re.search(r'">(.+)</param>$', s)
            pval = mv.group(1) if mv else ""
            pval_n = norm_ws(pval)
            vv_compact = pval_n.replace(" ", "")
            if re.fullmatch(r"[-–—.]+", vv_compact) or vv_compact in {"..", "..."}:
                bad_params.append(f"{offer_id}: пустышка в param '{pname_n}'='{pval_n}'")
            if "..." in vv_compact and not re.search(r"\d+\s*\.\.\.\s*\d+", vv_compact):
                if vv_compact.endswith("...") or re.search(r"[A-Za-zА-Яа-яЁё]\.\.\.", vv_compact):
                    bad_params.append(f"{offer_id}: обрезанное значение param '{pname_n}'='{pval_n}'")

        if s == "</offer>":
            # проверка на картинку
            if not has_picture:
                bad_no_pic.append(offer_id)

            # vendorCode должен совпадать с id
            if offer_id and vendor_code and vendor_code != offer_id:
                bad_vendorcode.append(offer_id)

            # keywords: должны быть через запятые
            if keywords:
                if "," not in keywords:
                    bad_keywords.append(offer_id)
            else:
                bad_keywords.append(offer_id)

            if not price_ok:
                bad_price.append(offer_id)

            in_offer = False
            offer_id = ""
            continue

    # Сводка ошибок
    if dup_ids:
        errors.append(f"Дубликаты offer id: {', '.join(dup_ids[:10])}" + ("..." if len(dup_ids) > 10 else ""))
    if hash_like_ids:
        errors.append(
            "Найдены hash-похожие offer id (похоже на md5/хеш). Это запрещено: "
            + ", ".join(hash_like_ids[:10])
            + ("..." if len(hash_like_ids) > 10 else "")
        )


    if bad_no_pic:
        errors.append(f"Есть offer без <picture>: {', '.join(bad_no_pic[:10])}" + ("..." if len(bad_no_pic) > 10 else ""))

    if bad_vendorcode:
        errors.append(f"vendorCode != offer/@id: {', '.join(bad_vendorcode[:10])}" + ("..." if len(bad_vendorcode) > 10 else ""))

    if bad_keywords:
        errors.append(f"keywords без запятых/пустые: {', '.join(bad_keywords[:10])}" + ("..." if len(bad_keywords) > 10 else ""))

    if bad_price:
        errors.append(f"price < 100 или невалидный: {', '.join(bad_price[:10])}" + ("..." if len(bad_price) > 10 else ""))

    if bad_params:
        # показываем первые 15 строк, чтобы лог был читаемый
        head = "\n".join(bad_params[:15])
        tail = "..." if len(bad_params) > 15 else ""
        errors.append("Проблемные params:\n" + head + ("\n" + tail if tail else ""))

    if errors:
        raise ValueError("CS-валидация не пройдена:\n- " + "\n- ".join(errors))
