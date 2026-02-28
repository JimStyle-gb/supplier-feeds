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
def _extract_desc(offer: ET.Element) -> str:
    return _get_text(offer.find("description"))


# -------------------------------
# AkCent: табличные характеристики в description (формат: "Ключ\tЗначение")
# Переносим их в params адаптера, чтобы CS-core не гадал и не ломал другие поставщики.
# -------------------------------

_AC_KEY_BAD_CHARS_RE = re.compile(r"[!?]|\.{2,}")
_AC_FIX_REPLACEMENTS = {
    "скобкы": "скобки",
    "коэффицент": "коэффициент",
}

def _ac_fix_text(s: str) -> str:
    """AkCent-only: безопасные правки опечаток/единиц (не переписываем смысл)."""
    if not s:
        return ""
    out = s
    # опечатки (case-insensitive)
    for bad, good in _AC_FIX_REPLACEMENTS.items():
        out = re.sub(re.escape(bad), good, out, flags=re.IGNORECASE)
    # единицы: "15 литров" -> "15 л"
    out = re.sub(r"\b(\d+)\s*литр(?:ов|а)?\b", r"\1 л", out, flags=re.IGNORECASE)
    out = re.sub(r"\b(\d+)\s*лтр\.?\b", r"\1 л", out, flags=re.IGNORECASE)
    return out.strip()

def _ac_fix_param_key(k: str) -> str:
    k2 = _ac_fix_text(k)
    # точечные правки заголовков параметров
    k2 = re.sub(r"\bкоэффициент\b", "коэффициент", k2, flags=re.IGNORECASE)
    # убираем двоеточие на конце
    k2 = re.sub(r"\s*:\s*$", "", k2).strip()
    return k2

def _ac_is_plausible_key(k: str) -> bool:
    if not k:
        return False
    k = k.strip()
    if len(k) < 2 or len(k) > 80:
        return False
    # слишком "предложение" — это не ключ
    if _AC_KEY_BAD_CHARS_RE.search(k):
        return False
    # ключ не должен выглядеть как рекламная фраза
    if k.count(" ") > 10:
        return False
    return True

def _ac_merge_params(base: list[tuple[str, str]], extra: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """AkCent-only: мерджим значения одинаковых ключей, не плодим дубли."""
    if not extra:
        return base
    out: list[tuple[str, str]] = []
    idx: dict[str, int] = {}
    for k, v in base:
        k2 = _ac_fix_param_key(k)
        v2 = _ac_fix_text(v)
        out.append((k2, v2))
        idx[k2.casefold()] = len(out) - 1

    for k, v in extra:
        k2 = _ac_fix_param_key(k)
        v2 = _ac_fix_text(v)
        if not k2 or not v2:
            continue
        key_cf = k2.casefold()
        if key_cf in idx:
            old_k, old_v = out[idx[key_cf]]
            # добавляем только если новой части ещё нет
            parts = [p.strip() for p in re.split(r"\s*,\s*", old_v) if p.strip()]
            if v2 not in parts:
                parts.append(v2)
            out[idx[key_cf]] = (old_k, ", ".join(parts))
        else:
            out.append((k2, v2))
            idx[key_cf] = len(out) - 1
    return out

def _ac_extract_tab_specs_from_desc(desc: str) -> tuple[list[tuple[str, str]], str]:
    """
    Возвращает: (табличные пары key/value, description без табличных строк).
    Поддержка кейса, когда значение идёт следующими строками после "Ключ\t".
    """
    if not desc:
        return [], ""

    lines = desc.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    extracted: list[tuple[str, str]] = []
    kept: list[str] = []

    i = 0
    while i < len(lines):
        raw = lines[i]
        line = raw.strip("\n")

        # Табличная строка только по TAB. Двоеточия/прочие форматы не трогаем — пусть остаются в тексте.
        if "\t" in line:
            left, right = line.split("\t", 1)
            k = _ac_fix_param_key(left)
            v = _ac_fix_text(right)

            if _ac_is_plausible_key(k):
                # если справа пусто — значение может быть в следующих строках (Для дома / Для офиса)
                if not v:
                    vals: list[str] = []
                    j = i + 1
                    while j < len(lines):
                        nxt = lines[j].strip()
                        if not nxt:
                            break
                        if "\t" in nxt:
                            break
                        # стоп-слова/заголовки секций
                        if nxt.endswith(":") and len(nxt) < 60:
                            break
                        vals.append(_ac_fix_text(nxt))
                        # обычно 1-3 строки, не раздуваем
                        if len(vals) >= 4:
                            break
                        j += 1
                    v = ", ".join([x for x in vals if x])
                    i = j  # съели строки значений
                else:
                    i += 1

                if k and v:
                    extracted.append((k, v))
                continue

        # не табличная строка — оставляем в описании
        kept.append(raw)
        i += 1

    # чистим пустые хвосты
    cleaned_desc = "\n".join([ln for ln in kept]).strip()
    return extracted, _ac_fix_text(cleaned_desc)

def _ac_fix_params(params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """AkCent-only: правки ключей/значений после clean_params()."""
    out: list[tuple[str, str]] = []
    for k, v in params:
        k2 = _ac_fix_param_key(k)
        v2 = _ac_fix_text(v)
        if not k2 or not v2:
            continue
        out.append((k2, v2))
    return out

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
        name = _get_text(offer.find("name"))
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

        native_desc_raw = _extract_desc(offer)
        tab_pairs, native_desc = _ac_extract_tab_specs_from_desc(native_desc_raw)

        params_raw = _collect_params(offer)
        # AkCent: табличные спеки из description превращаем в params
        params_raw = _ac_merge_params(params_raw, tab_pairs)

        params = clean_params(params_raw, drop=AKCENT_PARAM_DROP)
        params = _ac_fix_params(params)
        price_in = _extract_price_in(offer)
        if not price_in or int(price_in) < 1:
            price_missing += 1
        price = compute_price(price_in)

        vendor = _extract_vendor(offer, params)

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
