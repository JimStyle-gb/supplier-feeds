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

_CODE_TOKEN_RE = re.compile(r"\bC13T\d{5,6}[A-Z]?\b|\b[A-Z]\d{2}[A-Z]\d{3,6}\b|\bT\d{2}[A-Z]?\b|\bW\d{4}[A-Z]\b", re.IGNORECASE)
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

# main
def main() -> int:
    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, SCHEDULE_HOUR_ALMATY)

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
        extra_params, native_desc = _ac_extract_tab_specs_from_desc(native_desc)
        if extra_params:
            params_raw.extend(extra_params)
        params_raw = _ac_params_postfix(params_raw, name, native_desc)
        params = clean_params(params_raw, drop=AKCENT_PARAM_DROP)

        price_in = _extract_price_in(offer)
        if not price_in or int(price_in) < 1:
            price_missing += 1
        price = compute_price(price_in)

        vendor = _extract_vendor(offer, params)
        # AkCent: вычищаем значение параметра 'Производитель' (Proj/страны) -> бренд
        if vendor:
            fixed_params: list[tuple[str, str]] = []
            for pk, pv in params:
                if pk and pk.casefold() == 'производитель':
                    pv2 = _clean_vendor(pv) or vendor
                    fixed_params.append((pk, pv2))
                else:
                    fixed_params.append((pk, pv))
            params = fixed_params

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

if __name__ == "__main__":
    raise SystemExit(main())
