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
def stable_id(prefix: str, seed: str) -> str:
    h = hashlib.md5((seed or "").encode("utf-8", errors="ignore")).hexdigest()[:10]
    return f"{prefix}H{h.upper()}"


# XML escape для текста
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
    s = (v or '').strip()
    if not s:
        return False
    scf = s.casefold()
    # явные разделители списков/моделей
    if any(ch in s for ch in [',', ';', '/', '\\', '|']):
        return True
    # цифры/серии почти всегда означают модели
    if re.search(r"\d", s):
        return True
    # бренды и линейки (частые для расходников)
    if re.search(r"\b(hp|canon|epson|brother|samsung|xerox|kyocera|ricoh|lexmark|oki|panasonic|konica|minolta|pantum|dell)\b", scf):
        return True
    if re.search(r"\b(laserjet|deskjet|officejet|pixma|ecotank|workforce|bizhub)\b", scf):
        return True
    # короткие коды моделей (типа Q2612A, TN-1075, 12A)
    if re.search(r"\b[A-Z]{1,4}[- ]?\d{2,6}[A-Z]{0,2}\b", s):
        return True
    # если это явно "для дома/офиса" и т.п. — это НЕ совместимость
    if re.search(r"(?i)\bдля\s+(дома|офиса|защиты|работы|печати|обучения|школы|склада|магазина)\b", s):
        return False
    # по умолчанию: если строка длинная и без признаков моделей — считаем назначением
    return False
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

        # Совместимость — объединяем, остальное — берём первое значение
        if key_cf == "совместимость":
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
            params.append((k, v))

# Лёгкое обогащение характеристик из name/description (когда у поставщика params бедные)
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

    # Совместимость (простая эвристика по "для ...")
    if not (_has("Совместимость") or _has("Совместимые модели") or _has("Для") or _has("Применение")):
        m = re.search(r"(?i)\bдля\s+([^\n\r,;]{3,120})", hay)
        if m:
            val = norm_ws(m.group(1))
            if len(val) > 140:
                val = val[:140].rstrip(" ,")
            if val:
                params.append(("Совместимость", val))
                keys_cf.add("совместимость")

    # Ресурс
    if not (_has("Ресурс") or _has("Ресурс, стр")):
        m = re.search(r"(?i)\b(\d{2,5})\s*(?:стр|страниц\w*|pages?)\b", hay)
        if m:
            params.append(("Ресурс", m.group(1)))
            keys_cf.add("ресурс")

    # Цвет
    if not _has("Цвет"):
        m = re.search(r"(?i)\b(черн\w*|black|cyan|magenta|yellow|синий|голуб\w*|пурпур\w*|ж[её]лт\w*)\b", hay)
        if m:
            params.append(("Цвет", norm_ws(m.group(1))))
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
    n_esc = xml_escape_text(name)

    d = fix_text(native_desc)
    if not d:
        return f"<h3>{n_esc}</h3>"

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
        return f"<h3>{n_esc}</h3>"

    d2 = xml_escape_text(d).replace("\n", "<br>")
    return f"<h3>{n_esc}</h3><p>{d2}</p>"


# Делает аккуратный HTML внутри CDATA (добавляет \n в начале/конце)
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

    parts: list[str] = []
    if vendor:
        parts.append(vendor)
    if name:
        parts.append(name)

    # Разбор имени на слова (цифры/буквы, с дефисами)
    tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+(?:-[A-Za-zА-Яа-яЁё0-9]+)*", name)
    for t in tokens[: max(0, int(max_tokens))]:
        tt = norm_ws(t)
        if tt:
            parts.append(tt)

    if extra:
        for x in extra:
            xx = norm_ws(str(x))
            if xx:
                parts.append(xx)

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


def ensure_min_chars_params(
    params_sorted: Sequence[tuple[str, str]],
    oid: str,
    *,
    min_items: int = 3,
    priority: Sequence[str] | None = None,
) -> list[tuple[str, str]]:
    """Если характеристик слишком мало — добавляем безопасные пункты (без выдумывания фактов)."""
    ps = list(params_sorted or [])

    def _has_key(key: str) -> bool:
        kcf = key.casefold()
        return any(norm_ws(k).casefold() == kcf for k, _ in ps)

    if len(ps) < int(min_items):
        if not _has_key("Артикул"):
            ps.append(("Артикул", norm_ws(oid)))

    if len(ps) < int(min_items):
        if not _has_key("Код товара"):
            ps.append(("Код товара", norm_ws(oid)))

    return sort_params(ps, priority=list(priority or []))



# Собирает description (WhatsApp + HR + Описание + Характеристики + Оплата/Доставка)
# Единый CS-блок "Характеристики" (одного вида для всех поставщиков)
def build_chars_block(params_sorted: Sequence[tuple[str, str]]) -> str:
    items: list[str] = []
    for k, v in params_sorted or []:
        kk = xml_escape_text(norm_ws(k))
        vv = xml_escape_text(norm_ws(v))
        if not kk or not vv:
            continue
        items.append(f"<li><strong>{kk}:</strong> {vv}</li>")
    if not items:
        return "<h3>Характеристики</h3><ul></ul>"
    return "<h3>Характеристики</h3><ul>" + "".join(items) + "</ul>"



def _strip_native_tech_table(html: str) -> str:
    # CS: если "родное" описание похоже на длинную тех-таблицу, вырезаем техничку,
    # чтобы не дублировать наш блок "Характеристики".
    if not html:
        return html
    low = html.casefold()

    br_cnt = low.count("<br")
    tab_cnt = html.count("\t") + html.count("	")

    # режем только "простыню"
    if (len(html) < 1200) or (br_cnt < 18 and tab_cnt < 6):
        return html

    markers = [
        "технические характеристики",
        "основные характеристики",
        "интерфейсы",
        "характеристики устройства",
        "качество сканирования",
        "скорость сканирования",
        "дополнительная информация",
        "параметры питания",
        "состав поставки",
        "комплектация",
        "модель<br"
        "model<br"
        "характеристики<br"

    ]
    idxs = [low.find(m) for m in markers if low.find(m) != -1]
    cut_idx = min(idxs) if idxs else None

    # Частый кейс: Epson-стиль 'Параметр\tЗначение' без слов-маркеров
    if cut_idx is None and tab_cnt >= 8:
        ti = html.find("\t")
        if ti == -1:
            ti = html.find("	")
        if ti != -1:
            cut_idx = ti

    if cut_idx is None:
        return html

    # Стараемся отрезать с начала строки/блока
    line_start = max(html.rfind("<br", 0, cut_idx), html.rfind("</p>", 0, cut_idx))
    if line_start != -1 and line_start > 200:
        cut = html[:line_start]
    else:
        cut = html[:cut_idx]

    # Если срез получился слишком короткий, не ломаем
    if len(cut) < 200:
        return html

    cut = re.sub(r"(?:<br\s*/?>\s*){2,}$", "<br>", cut, flags=re.I)
    return cut.strip()

def _clip_long_native_desc(html: str, *, max_plain: int = 1200, max_br: int = 60) -> str:
    # CS: если "родное" описание слишком длинное (обычно маркетинговая простыня
    # или скопированная таблица характеристик), делаем читабельный обрез:
    # - режем по границам <br>, </p>, </li>, </hX>
    # - если это одна длинная "плита" без разрывов — берём первые max_plain символов текста.
    if not html:
        return html

    plain = re.sub(r"<[^>]+>", " ", html)
    plain = re.sub(r"\s+", " ", plain).strip()
    if len(plain) <= max_plain:
        return html

    # Разбивка по безопасным границам (не режем "внутри" тегов)
    chunks = re.split(r"(?i)(<br\s*/?>|</p\s*>|</li\s*>|</h[1-6]\s*>)", html)

    out: list[str] = []
    acc = 0
    brs = 0

    for ch in chunks:
        if not ch:
            continue

        if re.match(r"(?i)^(<br\s*/?>|</p\s*>|</li\s*>|</h[1-6]\s*>)$", ch.strip()):
            brs += 1
            out.append(ch)
            # Не даём раздуваться простынеством даже если текст "ползёт"
            if brs >= max_br and acc >= 500:
                break
            continue

        add_plain = re.sub(r"<[^>]+>", " ", ch)
        add_plain = re.sub(r"\s+", " ", add_plain).strip()
        if add_plain:
            if acc + len(add_plain) > max_plain and acc >= 250:
                break
            acc += len(add_plain)

        out.append(ch)

        if acc >= max_plain:
            break

    cut = "".join(out).strip()

    # Если разрывов не было (или мы не набрали ничего полезного) — fallback: режем по тексту
    if not cut or re.sub(r"<[^>]+>", " ", cut).strip() == "":
        t = plain[:max_plain].strip()
        t = t.rstrip(" ,.;:-")
        t = t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return f"<p>{t}</p>"

    cut = re.sub(r"(?:<br\s*/?>\s*){2,}$", "<br>", cut, flags=re.I)
    return cut.strip()

def build_description(
    name: str,
    native_desc: str,
    params_sorted: Sequence[tuple[str, str]],
    *,
    notes: Sequence[str] | None = None,
        wa_block: str = CS_WA_BLOCK,
    hr_2px: str = CS_HR_2PX,
    pay_block: str = CS_PAY_BLOCK,
) -> str:
    n = norm_ws(name)
    # Родное описание (обрезание/дедуп/удаление технички — внутри _build_desc_part)
    desc_part = _build_desc_part(n, native_desc)

    # Единый CS-блок характеристик всегда одного вида
    chars = build_chars_block(params_sorted)

    parts: list[str] = []
    parts.append(wa_block)
    parts.append(hr_2px)
    parts.append("<!-- Описание -->")
    parts.append(desc_part)

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
    parts.append(chars)
    parts.append(pay_block)

    inner = "\n".join(parts)
    return normalize_cdata_inner(inner)


# Делает FEED_META (фиксированный вид)
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
    "asus": "ASUS",
    "lenovo": "Lenovo",
    "acer": "Acer",
    "dell": "Dell",
    "logitech": "Logitech",
    "xiaomi": "Xiaomi",
}

# Пытается определить бренд (vendor) по vendor_src / name / params / description (если пусто — public_vendor)
def pick_vendor(
    vendor_src: str,
    name: str,
    params: Sequence[tuple[str, str]],
    desc_html: str,
    *,
    public_vendor: str = "CS",
) -> str:
    v = norm_ws(vendor_src)
    if v:
        return v

    hay = " ".join(
        [name or "", desc_html or ""]
        + [f"{k} {val}" for k, val in (params or [])]
    ).lower()

    for key, canon in CS_BRANDS_MAP.items():
        if re.search(rf"\b{re.escape(key)}\b", hay):
            return canon

    return norm_ws(public_vendor) or "CS"


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
        name = norm_ws(self.name)
        native_desc = fix_text(self.native_desc)
        # Вытаскиваем тех/осн характеристики из нативного описания в params, чтобы не было дублей
        native_desc, _spec_pairs = extract_specs_pairs_and_strip_desc(native_desc)
        vendor = pick_vendor(self.vendor, name, self.params, native_desc, public_vendor=public_vendor)

        # тройное обогащение: params + из описания
        params = list(self.params)
        if _spec_pairs:
            params.extend(_spec_pairs)
        enrich_params_from_desc(params, native_desc)
        enrich_params_from_name_and_desc(params, name, native_desc)

        # чистим и сортируем (ВАЖНО: чистить всегда)
        params = clean_params(params)
        params_sorted = sort_params(params, priority=list(param_priority or []))

                # выносим "параметры-фразы" в примечания и оставляем чистые характеристики
        params_sorted, notes = split_params_for_chars(params_sorted)
        # если характеристик мало — добавим безопасный пункт 'Артикул'
        params_sorted = ensure_min_chars_params(
            params_sorted,
            self.oid,
            priority=list(param_priority or []),
        )

        desc_cdata = build_description(name, native_desc, params_sorted, notes=notes)
        keywords = build_keywords(vendor, name, city_tail=city_tail)

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
            f"<name>{xml_escape_text(name)}</name>\n"
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
    _RE_HASH_OID = re.compile(r"^(AC|AS|CL|NP|VT)H[0-9A-F]{10}$")

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
            "Найдены hash-похожие offer id (похоже на stable_id/md5). Это запрещено: "
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
