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
AKCENT_PARAM_DROP = {"Сопутствующие товары"}

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
    # AkCent: иногда бренд приходит как 'Epson Proj' — нормализуем к бренду Epson
    if cf in {"epson proj", "epson proj.", "epson projector"}:
        return "Epson"
    # AkCent: общий случай "Brand proj"/"Brand projector" -> Brand
    if cf.endswith(" proj") or cf.endswith(" proj.") or cf.endswith(" projector"):
        base = s.split()[0].strip()
        if base:
            return base
    # чистим "made in ..." и явные страны
    if "made in" in cf or cf in COUNTRY_VENDOR_BLACKLIST_CF:
        return ""
    return s


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
def _extract_vendor(offer: ET.Element, params: list[tuple[str, str]]) -> str:
    v = _clean_vendor(_get_text(offer.find("vendor")))
    if v:
        return v
    for k, val in params:
        if k.casefold() in ("производитель", "бренд", "brand", "manufacturer"):
            v2 = _clean_vendor(val)
            if v2:
                return v2
    # фолбэк: по имени товара (чтобы raw уже был ближе к идеалу)
    nm = _ac_norm_name(_get_text(offer.find("name")))
    ncf = (nm or "").casefold()
    # порядок важен: сначала самые частые бренды
    for b in ["HP", "Epson", "Canon", "Brother", "Xerox", "Kyocera", "Ricoh", "Panasonic", "Zebra", "Fellowes", "ViewSonic", "Mr.Pixel", "SMART", "IDPRT"]:
        bcf = b.casefold()
        if bcf in ncf:
            return b
    return ""

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

    # 3LCD: ловим и латинскую C/c, и кириллическую С/с (частая опечатка)
    t = re.sub(r"(?i)\b3l[сc]d\b", "3LCD", t)

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
    # размеры/десятичные: 2, 03 -> 2,03 (только когда после запятой >=2 цифры); X/× -> x
    s = re.sub(r"(\d),[ \t\u00A0\u202F]+(\d{2,})", r"\1,\2", s)
    s = re.sub(r"[ \t\u00A0\u202F]+X[ \t\u00A0\u202F]+", " x ", s)
    s = s.replace("×", " x ")
    # запятые/пробелы
    s = re.sub(r",\s*(\S)", r", \1", s)
    # после нормализации запятых ещё раз лечим десятичные (чтобы не получалось 2, 03)
    s = re.sub(r"(\d),\s+(\d{2,})", r"\1,\2", s)
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

_CODE_TOKEN_RE = re.compile(
    r"\bC13T\d{5,8}[A-Z]?\b"  # Epson ink/maintenance codes
    r"|\bC12C\d{6}\b"          # Epson accessory codes
    r"|\bC11[A-Z]{2}\d{5}[A-Z0-9]{0,2}\b"   # Epson C11XX12345 + suffix
    r"|\bV1[23]H\d{7,8}\b"     # Epson V12H/V13Hxxxxxxx
    r"|\bC\d{2}C\d{5,6}\b"    # Epson paper/parts (CxxCxxxxxx)
    r"|\b(?:CE|CF|CC|CB|Q)\d{3,6}[A-Z]?\b"  # HP/Canon style
    r"|\b106R\d{5}\b"          # Xerox
    r"|\b(?:TN|DR|TK)\s*-?\s*\d{3,5}[A-Z]?\b"  # Brother/Kyocera
    r"|\bMLT\s*-?\s*[A-Z]?\d{3,4}[A-Z]?\b"      # Samsung
    r"|\bCRG\s*-?\s*\d{3,4}[A-Z]?\b"            # Canon
    r"|\b[A-Z]\d{2}[A-Z]\d{3,6}\b"               # Generic
    r"|\bT\d{2}[A-Z]?\b"                          # Epson bottle short
    r"|\bW\d{4}[A-Z]\b",
    re.IGNORECASE,
)
def _ac_extract_codes_from_fields(name: str, params: list[tuple[str, str]], desc: str) -> list[str]:
    text = " ".join([name or "", desc or ""] + [f"{k} {v}" for k, v in (params or [])])
    codes = []
    for m in _CODE_TOKEN_RE.finditer(text):
        c = m.group(0).upper()
        if c not in codes:
            codes.append(c)
    return codes

def _ac_is_consumable(name: str, params: list[tuple[str, str]]) -> bool:
    ncf = (name or "").casefold()
    if any(w in ncf for w in ("чернил", "чернила", "тонер", "картридж", "drum", "драм", "фотобарабан", "лента", "этикет", "maintenance box", "ёмкость для отработанных чернил", "емкость для отработанных чернил")):
        return True
    for k, v in (params or []):
        if (k or "").casefold() == "тип" and any(w in (v or "").casefold() for w in ("чернила", "тонер", "картридж", "расход")):
            return True
    return False

def _ac_split_list(s: str) -> list[str]:
    if not s:
        return []
    parts = re.split(r"[,;/\n\r]+", s)
    out: list[str] = []
    for p in parts:
        t = p.strip().strip(" .,:()[]{}<>\"'")
        if t:
            out.append(t)
    return out

def _ac_norm_code_token(tok: str) -> str:
    t = (tok or "").strip().upper()
    t = re.sub(r"\s+", "", t)
    t = re.sub(r"^(TN|DR|TK)(\d)", r"\1-\2", t)
    t = re.sub(r"^(MLT)([A-Z]?\d)", r"\1-\2", t)
    t = re.sub(r"^(CRG)(\d)", r"\1-\2", t)
    return t

def _ac_codes_from_text(text: str) -> list[str]:
    codes: list[str] = []
    for m in _CODE_TOKEN_RE.finditer(text or ""):
        c = _ac_norm_code_token(m.group(0))
        if c and c not in codes:
            codes.append(c)
    return codes

def _ac_extract_compat_models_from_name(name: str, vendor: str) -> list[str]:
    n = (name or "").strip()
    if not n:
        return []
    tail = ""
    m = re.search(r"(?i)\bдля\s+([^,]+)", n)
    if m:
        tail = m.group(1).strip()
    else:
        last = None
        for mm in _CODE_TOKEN_RE.finditer(n):
            last = mm
        if last:
            tail = (n[last.end():] or "").strip()

    if not tail:
        return []

    tail = re.split(r"(?i)\b(черн\w*|black|cyan|magenta|yellow|голуб\w*|пурпур\w*|ж[её]лт\w*|пигмент\w*|dye|пакет|набор)\b", tail)[0].strip()
    toks: list[str] = []
    for part in re.split(r"[;/,]+", tail):
        t = part.strip()
        if t:
            toks.append(t)

    models: list[str] = []
    for t in toks:
        tt = t.strip().strip(" .,:()[]{}<>\"'")
        if not tt:
            continue
        if _CODE_TOKEN_RE.fullmatch(_ac_norm_code_token(tt)):
            continue
        if not re.fullmatch(r"[A-Za-z]{0,4}-?\d{3,5}[A-Za-z]{0,3}", tt) and not re.fullmatch(r"[A-Za-z]{0,3}\d{3,5}[A-Za-z]{0,2}", tt):
            continue

        v = (vendor or "").strip()
        vcf = v.casefold()
        out_t = tt
        if v and vcf == "epson" and not re.search(r"(?i)\bepson\b", tt):
            if re.match(r"(?i)^(L|M|XP|WF|ET|SC|SP)\d{3,5}", tt):
                out_t = f"{v} {tt}"

        if out_t not in models:
            models.append(out_t)

    return models[:40]


def _ac_enrich_codes_and_compat(oid: str, name: str, vendor: str, params: list[tuple[str, str]], desc: str) -> list[tuple[str, str]]:
    codes_vals: list[str] = []
    compat_vals: list[str] = []
    rest: list[tuple[str, str]] = []

    compat_models: list[str] = []
    codes: list[str] = []

    for k, v in (params or []):
        k0 = (k or "").strip()
        vv = (v or "").strip()
        if not k0 or not vv:
            continue

        kcf = k0.casefold()

        # Нормализация некоторых значений (до дальнейшей логики)
        if kcf in {"интерфейс", "интерфейсы", "подключение"}:
            vv = _ac_norm_interface_value(vv)

        # Пара вида "Модель принтера" = "КОД"
        if _ac_key_looks_like_model(k0) and _CODE_TOKEN_RE.fullmatch(_ac_norm_code_token(vv)):
            if k0 not in compat_models:
                compat_models.append(k0)
            c = _ac_norm_code_token(vv)
            if c and c not in codes:
                codes.append(c)
            continue

        if kcf in {"коды", "коды расходников"}:
            codes_vals.append(vv)
            continue

        if kcf == "совместимость":
            compat_vals.append(vv)
            continue

        rest.append((k0, vv))

    # коды: из oid + текста + существующих значений
    text_for_codes = " ".join([oid or "", name or "", desc or ""] + [f"{k} {v}" for k, v in rest])
    for c in _ac_codes_from_text(text_for_codes):
        if c not in codes:
            codes.append(c)

    for vv in codes_vals:
        for c in _ac_codes_from_text(vv):
            if c not in codes:
                codes.append(c)

    # совместимость: для расходников — из имени (для ...)
    if _ac_is_consumable(name, rest):
        for mm in _ac_extract_compat_models_from_name(name, vendor):
            if mm not in compat_models:
                compat_models.append(mm)

    # если поставщик дал совместимость:
    for vv in compat_vals:
        # только коды -> это 'Коды'
        if _ac_is_code_only_list(vv):
            for c in _ac_codes_from_text(vv):
                if c not in codes:
                    codes.append(c)
            continue

        # модели + коды (если случайно попали)
        for t in _ac_split_list(vv):
            tt = t.strip()
            if not tt:
                continue
            if _CODE_TOKEN_RE.fullmatch(_ac_norm_code_token(tt)):
                c = _ac_norm_code_token(tt)
                if c and c not in codes:
                    codes.append(c)
                continue
            if tt not in compat_models:
                compat_models.append(tt)

    out = list(rest)
    if compat_models:
        out.append(("Совместимость", ", ".join(compat_models[:40])))
    if codes:
        out.append(("Коды", ", ".join(codes[:60])))
    return out


def _ac_extract_colon_specs_from_desc(desc: str) -> tuple[list[tuple[str, str]], str]:
    """Извлекает характеристики из многострочного описания вида 'Ключ: Значение'."""
    if not desc:
        return [], desc

    lines = desc.splitlines()
    out_params: list[tuple[str, str]] = []
    out_lines: list[str] = []

    # Заголовки секций (обычно без ':') — не сохраняем как характеристики
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
        # не тащим URL как ключ
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

        # табличные 'Ключ: Значение'
        if ":" in s and not s.startswith("http"):
            k, v = s.split(":", 1)
            k = k.strip()
            v = v.strip()
            if is_good_key(k) and is_good_val(v):
                # дедуп
                pair = (k, v)
                if pair not in out_params:
                    out_params.append(pair)
                extracted += 1
                # строку выкидываем из текста
                continue

        out_lines.append(ln)

        if extracted >= 80:
            # страховка
            out_lines.extend(lines[len(out_lines):])
            break

    cleaned = "\n".join(out_lines).strip()
    return out_params, cleaned
