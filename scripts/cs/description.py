# -*- coding: utf-8 -*-
"""
CS Description — общий сборщик HTML для <description>.

Этап 2: вынос из cs/core.py в отдельный модуль, без изменения логики.
Важно: модуль НЕ импортирует cs/core.py (чтобы не ловить циклические импорты).
"""

from __future__ import annotations

import os
import re
from typing import Sequence

from .keywords import fix_mixed_cyr_lat  # общий хелпер без циклических импортов

# Константы (вынесены из core без изменения)
CS_HR_2PX = "<hr style=\"border:none; border-top:2px solid #E7D6B7; margin:12px 0;\" />"
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
CS_WA_DIV = (
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; "
    "border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);\">"
    "&#128172; Написать в WhatsApp</a></p></div>"
)

# Regex/константы
_RE_MULTI_NL = re.compile(r"\n{3,}")
_RE_SHUKO = re.compile(r"(?i)\bshuko\b|\bшуко\b")

# Утилиты (как в core)
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


# Сборщики description/характеристик (вынесено из core, без изменения)

def norm_ws(s: str) -> str:
    s2 = (s or "").replace("\u00a0", " ").strip()
    s2 = re.sub(r"\s+", " ", s2)
    s2 = fix_mixed_cyr_lat(s2)
    s2 = _fix_desc_quality_text(s2)
    return s2.strip()

def xml_escape_text(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# XML escape для атрибутов

# Нормализация смешанных текстов (как в core)
def normalize_mixed_hyphen(s: str) -> str:
    t = s or ""
    if not t:
        return t
    # LED-индикаторы, USB-кабель, OPS-слот, Eco-режим, A3-формат
    t = _RE_MIXED_HYPHEN_LAT_CYR.sub(r"\1 \2", t)
    t = _RE_MIXED_HYPHEN_A1_CYR.sub(r"\1 \2", t)
    t = _RE_MIXED_HYPHEN_CYR_LAT.sub(r"\1 \2", t)
    return t


_RE_MIXED_SLASH_LAT_CYR = re.compile(r"([A-Za-z]{1,}[A-Za-z0-9]*)/([Ѐ-ӿ]{2,})")
_RE_MIXED_HYPHEN_LAT_CYR = re.compile(r"([A-Za-z])\s*[-–—]\s*([А-Яа-яЁё])")
_RE_MIXED_HYPHEN_A1_CYR = re.compile(r"([A-Za-z]\d{1,2})\s*[-–—]\s*([А-Яа-яЁё])")
_RE_MIXED_HYPHEN_CYR_LAT = re.compile(r"([А-Яа-яЁё])\s*[-–—]\s*([A-Za-z])")
_RE_MIXED_SLASH_CYR_LAT = re.compile(r"([Ѐ-ӿ]{2,})/([A-Za-z]{1,}[A-Za-z0-9]*)")

def normalize_mixed_slash(s: str) -> str:
    t = s or ""
    if not t:
        return t
    # Только кир/лат переходы: колодка/IEC, CD/банк, ЖК/USB, контактілер/EPO.
    # Лат/лат (RJ11/RJ45) и цифры/лат (4/IEC) не трогаем.
    for _ in range(3):  # на случай нескольких вхождений
        t2 = _RE_MIXED_SLASH_LAT_CYR.sub(r"\1 \2", t)
        t2 = _RE_MIXED_SLASH_CYR_LAT.sub(r"\1 \2", t2)
        if t2 == t:
            break
        t = t2
    return t

# Нормализация слэша между разными алфавитами (LAT <-> CYR), включая казахские буквы.
_CYR_CHAR_RE = re.compile(r"[\u0400-\u04FF]")
_LAT_CHAR_RE = re.compile(r"[A-Za-z]")

def sanitize_mixed_text(s: str) -> str:
    t = fix_mixed_cyr_lat(s)
    # Каз/рус тексты: исправляем короткие смешанные токены (ЖK -> ЖК)
    t = t.replace("ЖK", "ЖК").replace("Жk", "ЖК")
    return normalize_mixed_slash(normalize_mixed_hyphen(t))


def _fix_desc_quality_text(s: str) -> str:
    t = s or ""
    if not t:
        return t

    repl = [
        (r"(?iu)\b[LЛ][CС][DD]\b", "LCD"),
        (r"(?iu)\b[LЛ][EЕ][DD]\b", "LED"),
        (r"(?iu)\b[SЅ][NN][MМ][PР]\b", "SNMP"),
        (r"(?iu)\b[HН][DD][MМ][IІ]\b", "HDMI"),
        (r"(?iu)\b[Ff][RrГг][Oо0][Nп][Tт]\b", "Front"),
        (r"(?iu)\bc[иi]c[tт]e[mм]a\b", "система"),
        (r"(?iu)\bд[иi][cс]пл[eе]й\b", "дисплей"),
    ]
    for pat, rep in repl:
        t = re.sub(pat, rep, t)

    t = re.sub(
        r"(?iu)\b("
        r"Технология|Разрешение|Яркость|Контраст|Источник света|Оптика|Методы установки|Размер экрана|"
        r"Дистанция|Коэффициент проекции|Форматы сторон|Смарт-?система|Беспроводной дисплей|"
        r"Проводное зеркалирование|Интерфейсы|Акустика|Питание|Габариты проектора|Вес проектора|"
        r"Габариты упаковки|Вес упаковки|Языки интерфейса|Комплектация"
        r")(?=[A-Za-zА-Яа-яЁё0-9])",
        r"\1 ",
        t,
    )
    t = re.sub(r"(?iu)\bplenum\s+полост", "plenum-полост", t)
    t = re.sub(r"(?im)^\s*\.\s*$", "", t)
    t = re.sub(r"(?iu)Проводное\s+зеркалированиепо\b", "Проводное зеркалирование по", t)
    t = re.sub(r"(?iu)Смарт-?система(?=[A-Za-zА-Яа-яЁё0-9])", "Смарт-система ", t)
    return t


# Хелперы описания (как в core)
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
    t = sanitize_mixed_text(t)
    t = _fix_desc_quality_text(t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

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

def normalize_cdata_inner(inner: str) -> str:
    # Убираем мусорные пробелы/пустые строки внутри CDATA, без лишних ведущих/хвостовых переводов строк
    inner = (inner or "").strip()
    inner = _RE_MULTI_NL.sub("\n\n", inner)
    return inner

# Сборщики характеристик/описания
def build_chars_block(params_sorted: Sequence[tuple[str, str]]) -> str:
    items: list[str] = []
    for k, v in params_sorted or []:
        kk = xml_escape_text(norm_ws(k))
        vv = xml_escape_text(norm_ws(v))
        if not kk or not vv:
            continue
        items.append(f"<li><strong>{kk}:</strong> {vv}</li>")
    if not items:
        # CS: характеристики отсутствуют — выводим заглушку (единообразие + SEO)
        return "<h3>Характеристики</h3><p>Характеристики уточняются.</p>"
    return "<h3>Характеристики</h3><ul>" + "".join(items) + "</ul>"

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
        # CS: запрещено выводить название поставщика в тексте (кроме ссылок на фото)
    inner = re.sub(r"(?i)\bal[-\s]?style\b", "нашем магазине", inner)
    inner = re.sub(r"(?i)\bal[-\s]?style\.kz\b", "", inner)
    inner = re.sub(r"\s{2,}", " ", inner)
    return normalize_cdata_inner(inner)

