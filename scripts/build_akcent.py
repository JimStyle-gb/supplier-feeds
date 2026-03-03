# -*- coding: utf-8 -*-
"""
AkCent adapter — сборщик по шаблону CS (использует scripts/cs/core.py).
Важно: здесь только "индивидуальная часть" поставщика: скачивание XML и сбор сырья -> OfferOut.
Все правила шаблона (описание/keywords/price/params/валидация) — в cs.core.
"""

from __future__ import annotations

import re
from xml.etree import ElementTree as ET

import requests

from cs.core import (
    OfferOut,
    clean_params,
    compute_price,
    get_public_vendor,
    next_run_at_hour,
    now_almaty,
    safe_int,
    write_cs_feed,
    write_cs_feed_raw,
)

SUPPLIER_NAME = "AkCent"
SUPPLIER_URL = "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"
OUT_FILE = "docs/akcent.yml"
OUTPUT_ENCODING = "utf-8"
SCHEDULE_HOUR_ALMATY = 2
# Версия скрипта (для отладки в GitHub Actions)
BUILD_AKCENT_VERSION = "build_akcent_v39_force_next_run_02"
AKCENT_NAME_PREFIXES: list[str] = [
    "C13T55",
    "Ёмкость для отработанных чернил",
    "Интерактивная доска",
    "Интерактивная панель",
    "Интерактивный дисплей",
    "Картридж",
    "Ламинатор",
    "Монитор",
    "МФУ",
    "Переплетчик",
    "Пленка для ламинирования",
    "Плоттер",
    "Принтер",
    "Проектор",
    "Сканер",
    "Чернила",
    "Шредер",
    "Экономичный набор",
    "Экран",
]

# Префиксы в casefold (для нечувствительности к регистру)
AKCENT_NAME_PREFIXES_CF = tuple((p or "").casefold() for p in AKCENT_NAME_PREFIXES)

# Параметры AkCent, которые не являются характеристиками (только для этого поставщика)
AKCENT_PARAM_DROP = {"Артикул", "Сопутствующие товары"}

# CS: исключаем "картриджи для фильтра/бутылки" Philips AWP (не наша категория)
AKCENT_DROP_ARTICLES = {"AWP201/10", "AWP286/10"}

# Иногда поставщик кладёт страну в vendor/Производитель — такие значения лучше не использовать как бренд
COUNTRY_VENDOR_BLACKLIST_CF = {
    "китай", "china",
    "россия", "russia",
    "казахстан", "kazakhstan",
    "турция", "turkey",
    "сша", "usa", "united states",
    "германия", "germany",
    "япония", "japan",
    "корея", "korea",
    "великобритания", "uk", "united kingdom",
    "франция", "france",
    "италия", "italy",
    "испания", "spain",
    "польша", "poland",
    "тайвань", "taiwan",
    "таиланд", "thailand",
    "вьетнам", "vietnam",
    "индия", "india",
}


def _clean_vendor(v: str) -> str:
    # vendor = бренд; если туда прилетает страна/общие слова — убираем, чтобы не портить бренд.
    s = (v or "").strip()
    if not s:
        return ""
    cf = s.casefold()

    # AkCent: иногда бренд приходит как 'Epson Proj' / 'ViewSonic proj' / '... projector'
    # Убираем хвост "proj"/"projector" и точки.
    s2 = re.sub(r"\s+(proj\.?|projector)\s*$", "", s, flags=re.IGNORECASE).strip()
    cf2 = s2.casefold()

    # Спец-кейс Epson (часто именно так и приходит)
    if cf in {"epson proj", "epson proj.", "epson projector"} or cf2 == "epson":
        return "Epson"

    # чистим "made in ..." и явные страны
    if "made in" in cf2 or cf2 in COUNTRY_VENDOR_BLACKLIST_CF:
        return ""

    return s2



# Приоритет характеристик (как в AlStyle: сначала важное, потом остальное по алфавиту)
AKCENT_PARAM_PRIORITY = [
    "Бренд",
    "Производитель",
    "Модель",
    "Артикул",
    "Тип",
    "Назначение",
    "Совместимость",
    "Коды",
    "Цвет",
    "Размер",
    "Материал",
    "Гарантия",
    "Интерфейс",
    "Подключение",
    "Разрешение",
    "Мощность",
    "Напряжение",
]

# Нормализуем URL (если вдруг пришёл без схемы)
def _normalize_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return u
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return "https://" + u.lstrip("/")

# Проверяем, что название товара начинается с одного из заданных префиксов
def _passes_name_prefixes(name: str) -> bool:
    s = (name or "").lstrip()
    if not s:
        return False
    s_cf = s.casefold()
    for pref_cf in AKCENT_NAME_PREFIXES_CF:
        if pref_cf and s_cf.startswith(pref_cf):
            return True
    return False


# Генерирует стабильный CS-oid для AkCent (offer id == vendorCode)
# Основной ключ: AC + offer@article (в XML он есть; в id оставляем только ASCII)
# Важно: если в article есть символы вроде "*", кодируем их как _2A, чтобы не ловить коллизии.
def _make_oid(offer: ET.Element, name: str) -> str | None:
    art = (offer.get("article") or "").strip()
    if art:
        out: list[str] = []
        for ch in art:
            if re.fullmatch(r"[A-Za-z0-9_.-]", ch):
                out.append(ch)
            else:
                out.append(f"_{ord(ch):02X}")
        part = "".join(out)
        if part:
            return "AC" + part    # fallback (на случай если поставщик поломает article)
    # ВАЖНО: никаких хэшей от имени — только стабильный id из исходных атрибутов.
    sid = (offer.get("id") or "").strip()
    if sid:
        out: list[str] = []
        for ch in sid:
            if re.fullmatch(r"[A-Za-z0-9_.-]", ch):
                out.append(ch)
            else:
                out.append(f"_{ord(ch):02X}")
        part = "".join(out)
        if part:
            return "AC" + part

    return None
# Берём текст узла (без None)
def _get_text(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()

# Собираем картинки
def _collect_pictures(offer: ET.Element) -> list[str]:
    pics: list[str] = []
    for p in offer.findall("picture"):
        t = _normalize_url(_get_text(p))
        if t:
            pics.append(t)
    # уникализация (сохраняем порядок)
    out: list[str] = []
    seen: set[str] = set()
    for u in pics:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

# Собираем параметры (param/Param)
def _collect_params(offer: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for p in offer.findall("param"):
        k = (p.get("name") or "").strip()
        v = _get_text(p)
        if k and v:
                # Мусор от поставщика: Гарантия=0 (убираем)
                if k.casefold() == 'гарантия' and v.strip().casefold() in ('0', '0 мес', '0 месяцев', '0мес'):
                    continue
                out.append((k, v))
    for p in offer.findall("Param"):
        k = (p.get("name") or p.get("Name") or "").strip()
        v = _get_text(p)
        if k and v:
                # Мусор от поставщика: Гарантия=0 (убираем)
                if k.casefold() == 'гарантия' and v.strip().casefold() in ('0', '0 мес', '0 месяцев', '0мес'):
                    continue
                out.append((k, v))
    return out

# Достаём vendor (если пусто — CS Core сам определит бренд по имени/парам/описанию)
def _extract_vendor(offer: ET.Element, params: list[tuple[str, str]], name: str = "", oid: str = "") -> str:
    v = _clean_vendor(_get_text(offer.find("vendor")))
    if v:
        return v
    for k, val in params:
        if k.casefold() in ("производитель", "бренд", "brand", "manufacturer"):
            v2 = _clean_vendor(val)
            if v2:
                return v2
    # фолбэк по oid/артикулу (если поставщик не дал vendor/производителя)
    oid_cf = (oid or "").casefold()
    if oid_cf:
        # Epson: большинство расходников AkCent кодируются как C13T... прямо в oid (например ACC13T00S64A)
        if "c13t" in oid_cf:
            return "Epson"
        # SMART: интерактивные панели/дисплеи часто идут SBID-... без явного бренда в названии
        if "sbid" in oid_cf:
            return "SMART"


    # фолбэк по имени (если поставщик не дал vendor/производителя в XML/params)
    n = (name or "").strip()
    if n:
        # порядок важен (самые специфичные выше)
        brand_map: list[tuple[re.Pattern, str]] = [
            (re.compile(r"\bMr\.?\s*Pixel\b", re.IGNORECASE), "Mr.Pixel"),
            (re.compile(r"\bView\s*Sonic\b", re.IGNORECASE), "ViewSonic"),
            (re.compile(r"\bSMART\b", re.IGNORECASE), "SMART"),
            (re.compile(r"\bSBID\b", re.IGNORECASE), "SMART"),
            (re.compile(r"\bIDPRT\b", re.IGNORECASE), "IDPRT"),
            (re.compile(r"\bFellowes\b", re.IGNORECASE), "Fellowes"),
            (re.compile(r"\bEpson\b", re.IGNORECASE), "Epson"),
            (re.compile(r"\bCanon\b", re.IGNORECASE), "Canon"),
            (re.compile(r"\bBrother\b", re.IGNORECASE), "Brother"),
            (re.compile(r"\bKyocera\b", re.IGNORECASE), "Kyocera"),
            (re.compile(r"\bRicoh\b", re.IGNORECASE), "Ricoh"),
            (re.compile(r"\bXerox\b", re.IGNORECASE), "Xerox"),
            (re.compile(r"\bLexmark\b", re.IGNORECASE), "Lexmark"),
            (re.compile(r"\bPantum\b", re.IGNORECASE), "Pantum"),
            (re.compile(r"\bSamsung\b", re.IGNORECASE), "Samsung"),
            (re.compile(r"\bToshiba\b", re.IGNORECASE), "Toshiba"),
            (re.compile(r"\bSharp\b", re.IGNORECASE), "Sharp"),
            (re.compile(r"\bOki\b", re.IGNORECASE), "OKI"),
            (re.compile(r"\bHP\b", re.IGNORECASE), "HP"),
        ]
        for rx, brand in brand_map:
            if rx.search(n):
                return brand
    return ""


_ASPECT_FIX_NAME_RE = re.compile(r"^\s*Соотношение\s+сторон\s+(\d{1,2})\s*$", re.IGNORECASE)
_ASPECT_FIX_VAL_RE = re.compile(r"^\s*(\d{1,2})\s*$")

def _ac_fix_aspect_ratio_params(params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Фиксит битый кейс: <param name="Соотношение сторон 16">9</param> -> Соотношение сторон=16:9."""
    out: list[tuple[str, str]] = []
    for k, v in (params or []):
        k0 = (k or "").strip()
        v0 = (v or "").strip()
        m = _ASPECT_FIX_NAME_RE.match(k0)
        if m:
            m2 = _ASPECT_FIX_VAL_RE.match(v0)
            if m2:
                out.append(("Соотношение сторон", f"{m.group(1)}:{m2.group(1)}"))
                continue
        out.append((k, v))
    return out


# Достаём описание

# --- AkCent: максимум поставщик-специфичных правок в адаптере (CS-ready raw) ---

_AC_TEXT_REPL = [
    # орфография/типографика
    (r"конфернец", "конференц"),
    (r"характерстик", "характеристик"),
    (r"пурпурнымичернилами", "пурпурными чернилами"),
    (r"полотна(\d{3,4}\*)", r"полотна \1"),
]
_AC_TEXT_REPL_RE = [(re.compile(p, flags=re.IGNORECASE), rep) for p, rep in _AC_TEXT_REPL]

def _ru_minutes(n: int) -> str:
    # 1 минуту, 2-4 минуты, 5-20 минут, 21 минуту...
    n_abs = abs(int(n))
    n_mod100 = n_abs % 100
    n_mod10 = n_abs % 10
    if 11 <= n_mod100 <= 14:
        return "минут"
    if n_mod10 == 1:
        return "минуту"
    if 2 <= n_mod10 <= 4:
        return "минуты"
    return "минут"

def _ac_fix_text(desc: str) -> str:
    t = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    for rx, rep in _AC_TEXT_REPL_RE:
        t = rx.sub(rep, t)

    # типовые орфо/опечатки (AkCent)
    t = re.sub(r"(?i)\bтраспортировк", "транспортировк", t)
    t = re.sub(r"(?i)\bв\s+хранение\b", "в хранении", t)
    t = re.sub(r"(?i)\bв\s+комплект\s+работы\s+входит\b", "В комплект входит", t)

    # грамматика минут (склонение)
    def _min_repl(mm: re.Match) -> str:
        n = int(mm.group(1))
        return f"через {n} {_ru_minutes(n)}"
    t = re.sub(r"(?i)\bчерез\s+(\d+)\s+минут(?:ы|у)?\b", _min_repl, t)

    # ламинатор: изъять документ
    t = re.sub(r"(?i)\bдокумент\s+в\s+ламинатор\b", "документ из ламинатора", t)

    # 3LСD (кириллическая С) -> 3LCD
    t = t.replace("3LСD", "3LCD").replace("3lсd", "3lcd")

    # вырезаем огромные табличные простыни из описания (они пойдут в параметры)
    t = re.sub(
        r"(?is)\n\s*Технические\s+характеристики\s+Параметр/\s*\n\s*Значение\s*\n.*$",
        "",
        t,
    )

    # лечим оборванный хвост (встречается в AkCent): 'Уничтожение CD ... (1,'
    t = re.sub(
        r"(?is)\bУничтожение\s+CD\s+или\s+Blu-?Ray\s+DVD\s*\(1,\s*(?=$|\n)",
        "",
        t,
    )

    return t.strip()

def _ac_norm_name(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return s
    # NBSP/узкие пробелы -> обычный пробел (иначе regex не ловит)
    s = s.replace("\u00A0", " ").replace("\u202F", " ")
    # пробел после ®
    s = re.sub(r"®\s*(?=[A-Za-z0-9])", "® ", s)
    # шредеры: 5лст/11 лтр -> 5 лист., 11 л
    s = re.sub(r"(?i)\b(\d+)\s*лст\.?\b", r"\1 лист.", s)
    s = re.sub(r"(?i)\b(\d+)\s*лтр\.?\b", r"\1 л", s)
    s = s.replace("лист..", "лист.")
    # размеры/десятичные: 2, 03 -> 2,03; X/× -> x
    s = re.sub(r"(\d),[ \t\u00A0\u202F]+(\d)", r"\1,\2", s)
    s = re.sub(r"[ \t\u00A0\u202F]+X[ \t\u00A0\u202F]+", " x ", s)
    s = s.replace("×", " x ")
    # запятые/пробелы
    s = re.sub(r",(\S)", r", \1", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()

def _ac_norm_country(v: str) -> str:
    t = (v or "").strip()
    if not t:
        return ""
    # приводим к запятым
    t = t.replace("/", ",").replace(";", ",")
    t = re.sub(r"\.\s*", ", ", t)
    t = re.sub(r"\s{2,}", " ", t)
    parts = [p.strip() for p in t.split(",") if p.strip()]
    normed = []
    for p in parts:
        p2 = p
        p2 = re.sub(r"(?i)\bжапония\b", "Япония", p2)
        p2 = re.sub(r"(?i)\bфилипин(ы|)\b", "Филиппины", p2)
        p2 = re.sub(r"(?i)\bфилиппин\b", "Филиппины", p2)
        p2 = re.sub(r"(?i)\bфилипины\b", "Филиппины", p2)
        p2 = p2[:1].upper() + p2[1:] if p2 else p2
        if p2 and p2 not in normed:
            normed.append(p2)
    return ", ".join(normed)

def _ac_extract_tab_specs_from_desc(desc: str) -> tuple[list[tuple[str, str]], str]:
    """Вытаскиваем табличные строки AkCent вида 'Ключ\\tЗначение' в params и вычищаем их из описания."""
    t = (desc or "").replace("\r\n", "\n").replace("\r", "\n")
    if "\t" not in t:
        return [], (t.strip())
    lines = t.split("\n")
    out: list[tuple[str, str]] = []
    keep: list[str] = []
    i = 0
    while i < len(lines):
        ln = lines[i]
        if "\t" in ln:
            left, right = ln.split("\t", 1)
            k = left.strip()
            v = right.strip()
            # если значение пустое, собираем следующие строки пока не встретим новую таб-пару или пустую строку
            if k and not v:
                vals = []
                j = i + 1
                while j < len(lines):
                    ln2 = lines[j]
                    if "\t" in ln2:
                        break
                    if ln2.strip() == "":
                        break
                    vals.append(ln2.strip())
                    j += 1
                v = ", ".join(dict.fromkeys(vals)) if vals else ""
                i = j - 1
            if k and v:
                out.append((k, v))
            # не добавляем эту строку в описание
        else:
            keep.append(ln)
        i += 1
    cleaned = "\n".join(keep).strip()
    return out, cleaned

_CODE_TOKEN_RE = re.compile(r"\bC13T\d{5,8}[A-Z]?\b"r"|\bC12C\d{6}\b"r"|\bC11[A-Z]{2}\d{5}[A-Z0-9]{0,2}\b"r"|\bV1[23]H[0-9A-Z]{6,12}\b"r"|\bC\d{2}C\d{5,6}\b"r"|\b(?:CE|CF|CC|CB|Q)\d{3,6}[A-Z]?\b"r"|\b106R\d{5}\b"r"|\b(?:TN|DR|TK)\s*-?\s*\d{3,5}[A-Z]?\b"r"|\bMLT\s*-?\s*[A-Z]?\d{3,4}[A-Z]?\b"r"|\bCRG\s*-?\s*\d{3,4}[A-Z]?\b"r"|\bW\d{4}[A-Z]\b"r"|\bT\d{2}[A-Z]?\b"r"|\b[A-Z]\d{2}[A-Z]\d{3,6}\b", re.IGNORECASE)
def _ac_extract_codes_from_fields(name: str, params: list[tuple[str, str]], desc: str) -> list[str]:
    text = " ".join([name or "", desc or ""] + [f"{k} {v}" for k, v in (params or [])])
    codes = []
    for m in _CODE_TOKEN_RE.finditer(text):
        c = m.group(0).upper()
        if c not in codes:
            codes.append(c)
    return codes

def _ac_extract_volume_ml(name: str, desc: str, params: list[tuple[str, str]]) -> str:
    text = " ".join([name or "", desc or ""] + [v for _, v in (params or [])])
    m = re.search(r"(?i)\b(\d{2,4})\s*(мл|ml)\b", text)
    if m:
        return f"{m.group(1)} мл"
    return ""


_LAT2CYR = str.maketrans({
    "A":"А","a":"а","B":"В","E":"Е","e":"е","K":"К","k":"к","M":"М","m":"м",
    "H":"Н","h":"н","O":"О","o":"о","P":"Р","p":"р","C":"С","c":"с",
    "T":"Т","t":"т","X":"Х","x":"х","Y":"У","y":"у"
})

def _ac_cyr_like(s: str) -> str:
    # приводим латинские "похожие" буквы к кириллице для устойчивых замен (только внутри AkCent)
    return (s or "").translate(_LAT2CYR)

def _ac_params_postfix(params: list[tuple[str, str]], name: str, desc: str) -> list[tuple[str, str]]:
    out = []
    # rename keys / values
    for k, v in params:
        kk = (k or "").strip()
        vv = (v or "").strip()
        if not kk or not vv:
            continue
        kcf = kk.casefold()
        # ключи
        if kcf == "проекционный коэффицент (throw ratio)" or kcf == "проекционный коэффицент":
            kk = "Проекционный коэффициент"
        elif kcf == "тип резки":
            kk = "Тип резки"
        # значения
        if kk.casefold() == "уничтожение":
            vv_norm = _ac_cyr_like(vv)
            vv_norm = re.sub(r"(?i)\bскобк[ыи]\b", "скобы", vv_norm)
            vv = vv_norm
        if kk.casefold() == "страна происхождения":
            vv = _ac_norm_country(vv)
        if kk.casefold().startswith("отдельная корзина") and vv.casefold() in {"н", "н.", "нету", "нет"}:
            vv = "нет"
        out.append((kk, vv))
    # Совместимость из табличных параметров вида "Epson L7160"="C11..." и т.п.
    compat = []
    cleaned2 = []
    for k, v in out:
        if re.match(r"(?i)^(epson|hp|canon|brother|xerox|panasonic|ricoh|kyocera)\b", k.strip()):
            # если значение похоже на код/артикул производителя, считаем это строкой совместимости
            if re.search(r"\bC\d{2,}\b", v) or re.search(r"\b[A-Z]{1,2}\d{3,}\b", v) or v.strip().endswith("-"):
                compat.append(k.strip())
                continue
        cleaned2.append((k, v))
    out = cleaned2
    if compat:
        compat_u = []
        for c in compat:
            if c not in compat_u:
                compat_u.append(c)
        out.append(("Совместимость", ", ".join(compat_u)))
    # Коды расходников (AkCent): оставляем только реальные коды расходников, а модели (T3000/T5200/...) убираем
    existing_vals: list[str] = []
    tmp: list[tuple[str, str]] = []
    for k, v in out:
        if k.casefold() == "коды расходников":
            if v:
                existing_vals.append(v)
        else:
            tmp.append((k, v))
    out = tmp

    def _codes_from_val(vv: str) -> list[str]:
        toks = re.split(r"[;,\s]+", (vv or ""))
        res: list[str] = []
        for t in toks:
            tt = t.strip().strip(".,:()[]{}<>")
            if not tt:
                continue
            uu = tt.upper()
            if uu.isdigit():
                continue
            # это модели устройств, не коды расходников (Epson SureColor T3000/T5200/T5700D и т.п.)
            if re.fullmatch(r"T\d{3,4}[A-Z]?", uu):
                continue
            # допускаем типовые коды (Epson C13T..., HP/Canon вида CE278A и т.п., а также T09/T11 и т.п.)
            if re.fullmatch(r"C13T\d{5,6}[A-Z]?", uu) or re.fullmatch(r"[A-Z]\d{2}[A-Z]\d{3,6}", uu) or re.fullmatch(r"W\d{4}[A-Z]", uu) or re.fullmatch(r"T\d{2}[A-Z]?", uu):
                if uu not in res:
                    res.append(uu)
        return res

    codes: list[str] = []
    for vv in existing_vals:
        for c in _codes_from_val(vv):
            if c not in codes:
                codes.append(c)

    # добираем коды из name/desc (только по безопасным паттернам)
    for c in _ac_extract_codes_from_fields(name, out, desc):
        cc = c.upper()
        if re.fullmatch(r"T\d{3,4}[A-Z]?", cc):
            continue
        if cc not in codes:
            codes.append(cc)

    if codes:
        out.append(("Коды расходников", ", ".join(codes)))
    # Ресурс (только для расходников)
    name_cf = (name or "").casefold()
    if any(w in name_cf for w in ["чернила", "картридж", "тонер", "драм", "drum", "ink", "toner", "cartridge"]):
        vol = _ac_extract_volume_ml(name, desc, out)
        if vol:
            out.append(("Ресурс", vol))
    return out


def _extract_desc(offer: ET.Element) -> str:
    return _get_text(offer.find("description"))

# Достаём исходную цену:
# AkCent кладёт цены в <prices><price type="Цена дилерского портала KZT">41727</price> ...</prices>
def _extract_price_in(offer: ET.Element) -> int:
    prices = offer.find("prices")
    if prices is not None:
        best_any: int | None = None
        best_rrp: int | None = None
        for pe in prices.findall("price"):
            t = (pe.get("type") or "").casefold()
            cur = (pe.get("currencyId") or "").strip().upper()
            v = safe_int(_get_text(pe))
            if not v:
                continue
            if cur and cur != "KZT":
                continue

            # 1) приоритет — дилерская цена
            if "дилер" in t or "dealer" in t:
                return int(v)

            # 2) RRP как запасной приоритет
            if "rrp" in t:
                best_rrp = int(v)

            if best_any is None:
                best_any = int(v)

        if best_rrp is not None:
            return best_rrp
        if best_any is not None:
            return best_any

    # запасные варианты (на случай другого формата)
    p1 = safe_int(_get_text(offer.find("purchase_price")))
    if p1:
        return int(p1)
    p2 = safe_int(_get_text(offer.find("price")))
    return int(p2 or 0)

# Достаём доступность (если нет атрибута — считаем true)
def _extract_available(offer: ET.Element) -> bool:
    a = (offer.get("available") or "").strip().lower()
    if not a:
        return True
    return a in ("1", "true", "yes", "y", "да")

# Вытаскиваем offers из XML
def _extract_offers(root: ET.Element) -> list[ET.Element]:
    offers_node = root.find(".//offers")
    if offers_node is None:
        return []
    return list(offers_node.findall("offer"))

def _next_run_almaty(build_time: str, hour: int) -> str:
    """Ближайшая сборка по Алматы: следующий запуск сегодня/завтра в hour:00:00.
    build_time — строка now_almaty() вида 'YYYY-MM-DD HH:MM:SS'.
    """
    try:
        from datetime import datetime, timedelta

        dt = datetime.strptime((build_time or "").strip(), "%Y-%m-%d %H:%M:%S")
        cand = dt.replace(hour=int(hour), minute=0, second=0, microsecond=0)
        if cand <= dt:
            cand = cand + timedelta(days=1)
        return cand.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # fallback на core-хелпер (если формат времени вдруг поменяется)
        return next_run_at_hour(build_time, int(hour))

# main
def main() -> int:
    print(f"[akcent] version={BUILD_AKCENT_VERSION}")
    build_time = now_almaty()
    next_run = _next_run_almaty(build_time, 2)
    r = requests.get(_normalize_url(SUPPLIER_URL), timeout=90)
    r.raise_for_status()
    root = ET.fromstring(r.content)

    offers_in = _extract_offers(root)
    before = len(offers_in)

    out_offers: list[OfferOut] = []

    price_missing = 0

    for offer in offers_in:
        name = _ac_norm_name(_get_text(offer.find("name")))
        if not name or not _passes_name_prefixes(name):
            continue

        # CS: выкидываем "картриджи для фильтра/бутылки" (Philips AWP) из ассортимента
        art_raw = (offer.get("article") or "").strip()
        if art_raw in AKCENT_DROP_ARTICLES:
            continue
        ncf = (name or "").casefold()
        if ("картридж" in ncf or "cartridge" in ncf) and ("фильтр" in ncf or "filter" in ncf or "бутылк" in ncf or "bottle" in ncf) and ("philips" in ncf or "awp" in ncf):
            continue

        oid = _make_oid(offer, name)
        if not oid:
            continue
        if not oid:
            continue

        available = _extract_available(offer)
        pics = _collect_pictures(offer)
        params_raw = _collect_params(offer)
        native_desc = _ac_fix_text(_extract_desc(offer))
        extra_params2, native_desc = _ac_extract_colon_specs_from_desc(native_desc)
        extra_params, native_desc = _ac_extract_tab_specs_from_desc(native_desc)
        if extra_params2:
            params_raw.extend(extra_params2)
        if extra_params:
            params_raw.extend(extra_params)
        params_raw = _ac_params_postfix(params_raw, name, native_desc)
        params = clean_params(params_raw, drop=AKCENT_PARAM_DROP)

        price_in = _extract_price_in(offer)
        if not price_in or int(price_in) < 1:
            price_missing += 1
        price = compute_price(price_in)
        vendor = _extract_vendor(offer, params, name, oid)

        params = _ac_enrich_codes_and_compat(oid, name, vendor, params, native_desc)
        params = _ac_fix_model_by_name(name, vendor, params)
        params = _ac_fix_aspect_ratio_params(params)
        params = clean_params(params, drop=AKCENT_PARAM_DROP)
        out_offers.append(
            OfferOut(
                oid=oid,
                available=available,
                name=name,
                price=price,
                pictures=pics,
                vendor=vendor,
                params=params,
                native_desc=native_desc,
            )
        )

    after = len(out_offers)
    in_true = sum(1 for o in out_offers if o.available)
    in_false = after - in_true

    public_vendor = get_public_vendor()

    # Стабильный порядок офферов (меньше лишних диффов между коммитами)
    out_offers.sort(key=lambda x: x.oid)

    write_cs_feed_raw(out_offers, supplier=SUPPLIER_NAME, supplier_url=SUPPLIER_URL, out_file="docs/raw/akcent.yml", build_time=build_time, next_run=next_run, before=before, encoding=OUTPUT_ENCODING, currency_id="KZT")

    changed = write_cs_feed(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL,
        out_file=OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        public_vendor=public_vendor,
        currency_id="KZT",
        param_priority=AKCENT_PARAM_PRIORITY,
    )

    print(f"[akcent] before={before} after={after} price_missing={price_missing} changed={changed}")

    return 0
def _ac_extract_colon_specs_from_desc(desc: str) -> tuple[list[tuple[str, str]], str]:
    """Извлекает характеристики из многострочного описания вида 'Ключ: Значение'."""
    if not desc:
        return [], desc

    lines = desc.splitlines()
    out_params: list[tuple[str, str]] = []
    out_lines: list[str] = []

    section_headers = {
        "общие параметры", "изображение", "интерфейсы", "корпус", "разъемы", "питание",
        "функции", "другое", "экран", "сеть", "память", "звук",
    }

    def is_good_key(k: str) -> bool:
        k = (k or "").strip()
        if not k or len(k) > 70:
            return False
        kcf = k.casefold()
        if kcf in section_headers:
            return False
        if "http" in kcf:
            return False
        return True

    def is_good_val(v: str) -> bool:
        v = (v or "").strip()
        if not v:
            return False
        if len(v) > 250:
            return False
        return True

    extracted = 0
    for ln in lines:
        s = (ln or "").strip()
        if not s:
            out_lines.append(ln)
            continue

        if ":" in s and not s.startswith("http"):
            k, v = s.split(":", 1)
            k = k.strip()
            v = v.strip()
            if is_good_key(k) and is_good_val(v):
                pair = (k, v)
                if pair not in out_params:
                    out_params.append(pair)
                extracted += 1
                continue

        out_lines.append(ln)

        if extracted >= 80:
            break

    cleaned = "\n".join(out_lines).strip()
    return out_params, cleaned


def _ac_norm_code_token(t: str) -> str:
    s = (t or "").strip().upper()
    if not s:
        return ""
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s

def _ac_split_list(v: str) -> list[str]:
    if not v:
        return []
    t = (v or "").replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"\s+/\s+", ",", t)
    parts = re.split(r"[,;\n]+", t)
    out: list[str] = []
    for p in parts:
        pp = p.strip().strip(".,:()[]{}<>")
        if pp:
            out.append(pp)
    return out

def _ac_is_code_only_list(v: str) -> bool:
    items = _ac_split_list(v)
    if not items:
        return False
    for it in items:
        tt = _ac_norm_code_token(it)
        if not tt or not _CODE_TOKEN_RE.fullmatch(tt):
            return False
    return True

def _ac_key_looks_like_model(k: str) -> bool:
    s = (k or "").strip()
    if not s:
        return False
    if len(s) > 80:
        return False
    if not re.search(r"\d", s):
        return False
    return bool(re.search(r"(?i)\b(epson|hp|canon|brother|xerox|kyocera|ricoh|konica|minolta|samsung|pantum|oki|lexmark|sharp)\b", s))

def _ac_norm_interface_value(v: str) -> str:
    t = (v or "").strip()
    if not t:
        return ""
    t = re.sub(r"\s*\*\s*", " / ", t)
    t = re.sub(r"\s*/\s*", " / ", t)
    t = re.sub(r"\s{2,}", " ", t).strip(" /")
    return t

def _ac_enrich_codes_and_compat(oid: str, name: str, vendor: str, params: list[tuple[str, str]], desc: str) -> list[tuple[str, str]]:
    """Adapter-first финальная нормализация параметров AkCent:
    - Интерфейс/Подключение: '*' -> '/'
    - Если 'Совместимость' содержит только коды -> перенос в 'Коды'
    - 'Коды расходников' -> 'Коды'
    - Если param-name выглядит как модель, а value как код -> модель в 'Совместимость', код в 'Коды'
    """
    out: list[tuple[str, str]] = []
    codes_accum: list[str] = []
    compat_accum: list[str] = []

    for k, v in (params or []):
        k0 = (k or "").strip()
        v0 = (v or "").strip()
        if not k0 or not v0:
            continue

        kcf = k0.casefold()


        # Диапазоны '...'
        v0 = _ac_norm_ranges(v0)

        # Чистим производителя (Epson Proj -> Epson)
        if kcf == "производитель":
            v0 = _clean_vendor(v0)
        if kcf in {"интерфейс", "интерфейсы", "подключение"}:
            v0 = _ac_norm_interface_value(v0)

        # "Epson L7160" = "C11CG15404"
        if _ac_key_looks_like_model(k0) and _CODE_TOKEN_RE.fullmatch(_ac_norm_code_token(v0)):
            if k0 not in compat_accum:
                compat_accum.append(k0)
            c = _ac_norm_code_token(v0)
            if c and c not in codes_accum:
                codes_accum.append(c)
            continue

        if kcf == "совместимость" and _ac_is_code_only_list(v0):
            for c in _ac_split_list(v0):
                cc = _ac_norm_code_token(c)
                if cc and cc not in codes_accum:
                    codes_accum.append(cc)
            continue

        if kcf in {"коды", "коды расходников"}:
            for c in _ac_split_list(v0):
                cc = _ac_norm_code_token(c)
                if cc and cc not in codes_accum:
                    codes_accum.append(cc)
            continue

        out.append((k0, v0))

    # Добираем коды из имени/описания/oid (мягко)
    text_for_codes = " ".join([oid or "", name or "", desc or ""])
    for c in _ac_extract_codes_from_fields(text_for_codes, out, vendor or ""):
        cc = _ac_norm_code_token(c)
        if cc and cc not in codes_accum:
            codes_accum.append(cc)

    if compat_accum:
        out.append(("Совместимость", ", ".join(compat_accum[:40])))
    if codes_accum:
        out.append(("Коды", ", ".join(codes_accum[:80])))

    return out


_SC_T_CODE_RE = re.compile(r"\bSC-T\d{4,5}[A-Z]?\b", re.IGNORECASE)


def _ac_fix_model_by_name(name: str, vendor: str, params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Фикс редкого, но критичного кейса AkCent:
    в названии одна модель (например SC-T5700D), а в param 'Модель' приходит другая (SC-T5200).

    Правило: если vendor=Epson и в name есть код SC-T..., то 'Модель' должна содержать этот код.
    """
    nm = (name or "").strip()
    if not nm:
        return params
    if (vendor or "").casefold() != "epson":
        return params

    mm = _SC_T_CODE_RE.search(nm)
    if not mm:
        return params

    code = (mm.group(0) or "").upper()
    if not code:
        return params

    prefix = "Epson SureColor" if "surecolor" in nm.casefold() else "Epson"
    desired = f"{prefix} {code}".strip()

    out: list[tuple[str, str]] = []
    found_model = False
    for k, v0 in (params or []):
        if (k or "").strip().casefold() == "модель":
            found_model = True
            cur = (v0 or "").strip()
            if code not in cur.upper():
                out.append(("Модель", desired))
            else:
                out.append(("Модель", cur))
        else:
            out.append((k, v0))

    if not found_model:
        out.append(("Модель", desired))

    return out

def _ac_norm_ranges(v: str) -> str:
    """Нормализует диапазоны вида '5...40' -> '5–40'."""
    s = (v or "").strip()
    if not s:
        return ""
    # 5...40 -> 5–40 ; 1.19...1.61 -> 1.19–1.61
    s = re.sub(r"(\d(?:[\d.,]*\d)?)\s*\.\.\.\s*(\d(?:[\d.,]*\d)?)", r"\1–\2", s)
    # иногда встречается '... ' без пробелов
    s = s.replace("…", "…")  # keep unicode ellipsis as-is
    return s



def _ac_norm_ranges(v: str) -> str:
    """Нормализует диапазоны вида '5...40' -> '5–40', '1.19...1.61' -> '1.19–1.61'."""
    s = (v or "").strip()
    if not s:
        return ""
    # 5...40 -> 5–40 ; 1.19...1.61 -> 1.19–1.61
    s = re.sub(r"(\d(?:[\d.,]*\d)?)\s*\.\.\.\s*(\d(?:[\d.,]*\d)?)", r"\1–\2", s)
    return s

if __name__ == "__main__":
    raise SystemExit(main())
