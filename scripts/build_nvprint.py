# -*- coding: utf-8 -*-
"""
NVPrint -> CS adapter (v65, 1C-XML "КаталогТоваров/Товары/Товар")

Core (cs/core.py) = только общее: цена/description/params/feed_meta/рендер/валидация/запись.
Этот файл = только NVPrint-специфика: скачать XML, распарсить "Товар", собрать OfferOut.
"""

from __future__ import annotations

import os
import sys
import time
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

import requests

from cs.core import (
    OfferOut,
    compute_price,
    ensure_footer_spacing,
    make_feed_meta,
    make_footer,
    make_header,
    next_run_at_hour,
    now_almaty,
    norm_ws,
    validate_cs_yml,
    write_if_changed,
)


OUT_FILE = "docs/nvprint.yml"
OUTPUT_ENCODING = "utf-8"


# Если описание уже похоже на CS — не берём native_desc (иначе будет дубль секций)
RE_DESC_HAS_CS = re.compile(r"<!--\s*WhatsApp\s*-->|<!--\s*Описание\s*-->|<h3>\s*Характеристики\s*</h3>", re.I)

# NVPrint-мусорные параметры (если встречаются)
DROP_PARAM_NAMES_CF = {
    "артикул",
    "остаток",
    "наличие",
    "в наличии",
    "сопутствующие товары",
    "sku",
    "код",  # код используем для oid, но в params не нужен
    "guid",
    "ссылканакартинку",
    "вес",
    "высота",
    "длина",
    "ширина",
    "объем",
    "объём",
    "разделкаталога",
    "разделмодели",

}


# Фильтр ассортимента NVPrint.
# ВАЖНО: фильтруем по ПРЕФИКСУ названия (а не по наличию слова внутри),
# иначе почти всё проходит из‑за слов "...для картриджа..." в тексте.
# Можно расширить через env NVPRINT_INCLUDE_PREFIXES (через запятую).
NVPRINT_INCLUDE_PREFIXES_CF = [
    "блок фотобарабана",
    "картридж",
    "печатающая головка",
    "струйный картридж",
    "тонер-картридж",
    "тонер картридж",  # бывает без дефиса
    "тонер-туба",
    "тонер туба",      # бывает без дефиса
]


_RE_WS = re.compile(r"\s+")

# Подмена похожих латинских букв на кириллицу, только когда дальше идёт кириллица.
# Нужно для случаев типа "Cтруйный" (латинская C).
_LAT2CYR = {
    "A": "А", "a": "а",
    "B": "В", "b": "в",
    "C": "С", "c": "с",
    "E": "Е", "e": "е",
    "H": "Н", "h": "н",
    "K": "К", "k": "к",
    "M": "М", "m": "м",
    "O": "О", "o": "о",
    "P": "Р", "p": "р",
    "T": "Т", "t": "т",
    "X": "Х", "x": "х",
    "Y": "У", "y": "у",
}


def _fix_mixed_ru(s: str) -> str:
    # Меняем латиницу на кириллицу ТОЛЬКО если следующая буква кириллическая.
    if not s:
        return ""
    out = []
    n = len(s)
    for i, ch in enumerate(s):
        rep = ch
        if ch in _LAT2CYR and i + 1 < n:
            nxt = s[i + 1]
            if "\u0400" <= nxt <= "\u04FF":  # кириллица
                rep = _LAT2CYR[ch]
        out.append(rep)
    return "".join(out)


def _name_for_filter(name: str) -> str:
    s = (name or "").strip()
    s = _fix_mixed_ru(s)
    s = s.casefold()
    s = _RE_WS.sub(" ", s)
    return s


def _include_by_name(name: str) -> bool:
    cf = _name_for_filter(name)
    if not cf:
        return False

    extra = (os.environ.get("NVPRINT_INCLUDE_PREFIXES") or "").strip()
    prefixes = list(NVPRINT_INCLUDE_PREFIXES_CF)
    if extra:
        for x in extra.split(","):
            x = x.strip().casefold()
            if x and x not in prefixes:
                prefixes.append(x)

    for p in prefixes:
        if p and cf.startswith(p):
            return True
    # NOTE: Только префиксная фильтрация (см. комментарий выше). Режим 'слова внутри' отключён.
    return False


@dataclass
class _Auth:
    login: str
    password: str



def _get_auth() -> _Auth | None:
    login = (os.environ.get("NVPRINT_LOGIN") or "").strip()
    pw = (os.environ.get("NVPRINT_PASSWORD") or os.environ.get("NVPRINT_PASS") or "").strip()
    if login and pw:
        return _Auth(login=login, password=pw)
    return None


def _download_xml(url: str, auth: _Auth | None) -> bytes:
    """Скачать NVPrint XML с ретраями.

    По умолчанию делает несколько попыток с backoff, чтобы не падать на временных сетевых сбоях.
    Параметры можно переопределить env:
      - NVPRINT_HTTP_RETRIES (default 4)
      - NVPRINT_TIMEOUT_CONNECT (default 20)
      - NVPRINT_TIMEOUT_READ (default 120)
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (CS bot; NVPrint adapter)",
        "Accept": "application/xml,text/xml,*/*",
    }

    def _env_int(name: str, default: int) -> int:
        try:
            v = int((os.environ.get(name, str(default)) or str(default)).strip())
            return v if v > 0 else default
        except Exception:
            return default

    retries = _env_int("NVPRINT_HTTP_RETRIES", 4)
    t_connect = _env_int("NVPRINT_TIMEOUT_CONNECT", 20)
    t_read = _env_int("NVPRINT_TIMEOUT_READ", 120)

    kwargs = {"timeout": (t_connect, t_read), "headers": headers}
    if auth:
        kwargs["auth"] = (auth.login, auth.password)

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, **kwargs)
            if r.status_code == 200 and r.content:
                return r.content
            raise RuntimeError(f"Не удалось скачать NVPrint XML: http={r.status_code} bytes={len(r.content or b'')}")
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, requests.exceptions.SSLError) as e:
            last_err = e
            if attempt >= retries:
                break
            # backoff: 1.5^n + небольшой jitter
            sleep_s = (1.5 ** (attempt - 1)) + random.uniform(0.0, 0.4)
            print(f"NVPrint: сеть/таймаут, попытка {attempt}/{retries} -> sleep {sleep_s:.1f}s ({type(e).__name__})", file=sys.stderr)
            time.sleep(sleep_s)
        except Exception as e:
            # прочие ошибки (например, странный HTTP ответ) — без ретраев
            raise

    raise RuntimeError(f"NVPrint: не удалось скачать XML после {retries} попыток: {last_err}")



def _xml_head(xml_bytes: bytes, limit: int = 2500) -> str:
    try:
        s = xml_bytes.decode("utf-8")
    except Exception:
        try:
            s = xml_bytes.decode("cp1251")
        except Exception:
            s = xml_bytes.decode("utf-8", errors="replace")
    s = s.replace("\r", "")
    return s[:limit]


def _local(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _get_text(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _pick_first_text(node: ET.Element, names: tuple[str, ...]) -> str:
    want = {n.casefold() for n in names}
    for ch in list(node):
        if _local(ch.tag).casefold() in want:
            v = _get_text(ch)
            if v:
                return v
    return ""


def _iter_children(node: ET.Element) -> list[ET.Element]:
    return list(node)


def _find_items(root: ET.Element) -> list[ET.Element]:
    offers = [el for el in root.iter() if _local(el.tag).casefold() == "offer"]
    if offers:
        return offers

    tovar = [el for el in root.iter() if _local(el.tag).casefold() == "товар"]
    if tovar:
        return tovar

    return []


def _make_oid(item: ET.Element, name: str) -> str | None:
    raw = (
        _pick_first_text(item, ("vendorCode", "article", "Артикул", "sku", "code", "Код", "Guid"))
        or (item.get("id") or "").strip()
    )
    if not raw:
        return None

    raw = raw.strip()
    out = []
    for ch in raw:
        if re.fullmatch(r"[A-Za-z0-9_.-]", ch):
            out.append(ch)
        else:
            out.append("_")
    oid = "".join(out)
    if not oid.startswith("NP"):
        oid = "NP" + oid
    return oid


def _parse_num(text: str) -> float | None:
    t = (text or "").strip()
    if not t:
        return None
    t = t.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(\.\d+)?", t)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _extract_price(item: ET.Element) -> int | None:
    prefer_keys = {
        "purchase_price", "base_price", "price",
        "цена", "цена_кзт", "ценаказахстан", "ценаkzt", "pricekzt",
        "ценасндс", "ценабезндс",
    }

    for ch in _iter_children(item):
        k = _local(ch.tag).casefold()
        if k in prefer_keys:
            n = _parse_num(_get_text(ch))
            if n is not None:
                return int(n)

    found: list[int] = []
    for el in item.iter():
        k = _local(el.tag).casefold()
        if "цена" in k or k in prefer_keys:
            n = _parse_num(_get_text(el))
            if n is not None and n > 0:
                found.append(int(n))

    if not found:
        return None

    return min(found)


def _extract_available(item: ET.Element) -> bool:
    # 1) yml-атрибут available
    av_raw = (item.get("available") or "").strip().lower()
    if av_raw in ("true", "1", "yes", "y"):
        return True
    if av_raw in ("false", "0", "no", "n"):
        return False

    # 2) 1C-формат NVPrint: <УсловияПродаж>/<Договор>/<Наличие Количество="..."/>
    # Если теги "Наличие" есть, считаем empty/"0" как 0; >0 значит в наличии.
    found_any = False
    total_qty = 0.0
    for el in item.iter():
        if _local(el.tag).casefold() != "наличие":
            continue
        found_any = True
        q = (el.get("Количество") or el.get("количество") or "").strip()
        if not q:
            continue
        q = q.replace(",", ".")
        try:
            total_qty += float(q)
        except Exception:
            continue
    if found_any:
        return total_qty > 0

    # 3) Фолбэк: иногда остаток/количество могут быть отдельными тегами
    for el in item.iter():
        tag_cf = _local(el.tag).casefold()
        if ("остат" in tag_cf) or ("колич" in tag_cf):
            n = _parse_num(_get_text(el))
            if n is not None:
                return n > 0

    # 4) fallback: иногда есть тег available/Наличие как текст
    av_tag = _pick_first_text(item, ("available", "Available", "Наличие"))
    if av_tag:
        return av_tag.strip().lower() in ("true", "1", "yes", "y", "есть", "да")

    # по умолчанию: считаем, что доступно (иначе можно "убить" ассортимент)
    return True

    if av_raw in ("false", "0", "no", "n"):
        return False

    # 2) 1C: пытаемся найти любые поля, связанные с остатком/количеством.
    qty_kz: float = 0.0
    qty_any: float = 0.0
    has_any = False

    for el in item.iter():
        tag = _local(el.tag).casefold()

        # Берём только теги, где явно фигурирует остаток/количество.
        if ("остат" not in tag) and ("колич" not in tag) and (tag not in ("qty", "quantity")):
            continue

        n = _parse_num(_get_text(el))
        if n is None:
            continue

        has_any = True
        qty_any += n

        # Казахстан — пытаемся определить по атрибутам/тексту рядом
        attrs = " ".join([str(v) for v in el.attrib.values()]).casefold()
        if "казахстан" in attrs:
            qty_kz += n

    if has_any:
        use = qty_kz if qty_kz > 0 else qty_any
        return use > 0

    # 3) fallback: иногда есть отдельное поле "Наличие"
    av_tag = _pick_first_text(item, ("available", "Available", "Наличие"))
    if av_tag:
        return av_tag.strip().lower() in ("true", "1", "yes", "y", "есть", "да")

    # По умолчанию не гасим ассортимент.
    return True


def _collect_pictures(item: ET.Element) -> list[str]:
    # 1) yml: <picture>
    pics: list[str] = []
    for el in item.iter():
        if _local(el.tag).casefold() != "picture":
            continue
        u = _get_text(el)
        if not u:
            continue
        u = u.strip()
        if u.startswith("//"):
            u = "https:" + u
        if u.startswith("/"):
            u = "https://nvprint.ru" + u
        pics.append(u)

    # 2) 1C: часто есть поле "СсылкаНаКартинку" (или похожие)
    if not pics:
        u = _pick_first_text(
            item,
            (
                "СсылкаНаКартинку",
                "СсылкаНаКартинку1",
                "СсылкаНаКартинку2",
                "СсылкаНаКартинк",
                "Картинка",
                "Фото",
                "Image",
                "Picture",
            ),
        )
        u = (u or "").strip()
        if u:
            if u.startswith("//"):
                u = "https:" + u
            if u.startswith("http://"):
                u = "https://" + u[len("http://") :]
            pics = [u]

    if not pics:
        return []

    # уникализация
    seen = set()
    out: list[str] = []
    for u in pics:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out

def _collect_params(item: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    for p in item.findall("param"):
        k = (p.get("name") or "").strip()
        v = _get_text(p)
        if not k or not v:
            continue
        if k.casefold() in DROP_PARAM_NAMES_CF:
            continue
        if k.casefold() in ("вес", "высота", "длина", "ширина", "ресурс") and v.strip() in ("0", "0.0", "0,0", "0,00", "0.00"):
            continue
        if k.casefold() == "гарантия" and v.strip().casefold() in ("0", "0 мес", "0 месяцев", "0мес"):
            continue
        k = _rename_param_key_nvprint(k)
        v = _cleanup_param_value_nvprint(k.replace(" ", ""), v) if k in ("Тип печати","Цвет печати","Совместимость с моделями") else _cleanup_param_value_nvprint(k, v)
        orig_k = k
        k = _rename_param_key_nvprint(k)
        v = _cleanup_param_value_nvprint(orig_k, v)
        out.append((k, v))

    if out:
        return out

    skip_keys = {
        "код", "артикул", "guid",
        "номенклатура", "номенклатуракратко", "наименование",
        "цена", "ценасндс", "ценабезндс", "цена_кзт", "price",
        "new_reman", "разделпрайса",
        "ссылканакартинку",
    }

    for ch in _iter_children(item):
        k = _local(ch.tag).strip()
        cf = k.casefold()
        v = _get_text(ch)
        if not v:
            continue
        if cf in skip_keys:
            continue
        if cf in DROP_PARAM_NAMES_CF:
            continue
        if cf in ("вес", "высота", "длина", "ширина", "ресурс") and v.strip() in ("0", "0.0", "0,0", "0,00", "0.00"):
            continue
        if cf == "гарантия" and v.strip().casefold() in ("0", "0 мес", "0 месяцев", "0мес"):
            continue
        k = _rename_param_key_nvprint(k)
        v = _cleanup_param_value_nvprint(k.replace(" ", ""), v) if k in ("Тип печати","Цвет печати","Совместимость с моделями") else _cleanup_param_value_nvprint(k, v)
        out.append((k, v))

    return out


def _native_desc(item: ET.Element) -> str:
    d = _pick_first_text(item, ("description", "Описание"))
    if not d:
        return ""
    if RE_DESC_HAS_CS.search(d):
        return ""
    return d





_CYR2LAT = {
    # кириллица -> латиница (конфузаблы), только внутри латинских/цифровых токенов
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X", "У": "Y",
    "а": "a", "в": "b", "е": "e", "к": "k", "м": "m", "н": "h", "о": "o", "р": "p", "с": "c", "т": "t", "х": "x", "у": "y",
}

_RE_TOKEN = re.compile(r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-._/]+")
_RE_DBL_SLASH = re.compile(r"//+")
_RE_NV_SPACE = re.compile(r"\bNV-\s+")
_RE_WS = re.compile(r"\s+")
_RE_SPACE_BEFORE_RP = re.compile(r"\s+\)")

_RE_SLASH_BEFORE_LETTER = re.compile(r"/(?!\s)(?=[A-Za-zА-Яа-я])")
_RE_SHT_MISSING_SPACE = re.compile(r"\((\d+)шт\)", re.I)
_RE_NUM_SHT_WORD = re.compile(r"\b(\d+)шт\b", re.I)
_RE_WORKCENTRE = re.compile(r"\bWorkcentr(e)?\b", re.I)


_STOP_BRAND_CF = {
    "лазерных", "струйных", "принтеров", "мфу", "копиров", "копировальных", "плоттеров",
    "принтера", "устройств", "устройства", "печати", "всех",
}

def _fix_confusables_to_latin_in_latin_tokens(s: str) -> str:
    # 1) сначала правим "латиница внутри кириллицы" (Cервисный -> Сервисный) уже делает _fix_mixed_ru
    # 2) потом правим "кириллица внутри латиницы" (СE390X -> CE390X, Kyoсera -> Kyocera)
    if not s:
        return ""
    out = []
    last = 0
    for m in _RE_TOKEN.finditer(s):
        out.append(s[last:m.start()])
        tok = m.group(0)
        has_lat = any(("A" <= ch <= "Z") or ("a" <= ch <= "z") for ch in tok)
        if has_lat:
            tok = "".join(_CYR2LAT.get(ch, ch) for ch in tok)
        out.append(tok)
        last = m.end()
    out.append(s[last:])
    return "".join(out)

def _drop_unmatched_rparens(s: str) -> str:
    if not s:
        return ""
    out = []
    bal = 0
    for ch in s:
        if ch == "(":
            bal += 1
            out.append(ch)
        elif ch == ")":
            if bal > 0:
                bal -= 1
                out.append(ch)
            else:
                # лишняя ')'
                continue
        else:
            out.append(ch)
    return "".join(out)

def _cleanup_name_nvprint(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    s = _fix_mixed_ru(s)  # латиница -> кириллица в русских словах
    s = _fix_confusables_to_latin_in_latin_tokens(s)  # кириллица -> латиница в кодах/брендах
    s = _RE_NV_SPACE.sub("NV-", s)                    # NV- 0617B025 -> NV-0617B025
    s = _RE_DBL_SLASH.sub("/", s)                     # // -> /
    s = _RE_SPACE_BEFORE_RP.sub(")", s)               # " )" -> ")"
    s = _RE_SHT_MISSING_SPACE.sub(r"(\1 шт)", s)  # (2шт) -> (2 шт)
    s = _RE_NUM_SHT_WORD.sub(r"\1 шт", s)         # 2шт -> 2 шт
    s = _RE_SLASH_BEFORE_LETTER.sub("/ ", s)       # 3020/WorkCentre -> 3020/ WorkCentre
    s = _RE_WORKCENTRE.sub("WorkCentre", s)   # Workcentre/Workcentr -> WorkCentre
    s = _drop_unmatched_rparens(s)                    # убрать лишние ')'
    s = norm_ws(s)
    s = _normalize_name_prefix(s)
    # дублирующая страховка префиксов
    s = re.sub(r"^Тонер\s+картридж\b", "Тонер-картридж", s, flags=re.I)
    s = _RE_WS.sub(" ", s).strip()
    return s

_COLOR_MAP = {
    "пурпурный": "Magenta",
    "магента": "Magenta",
    "черный": "Black",
    "чёрный": "Black",
    "желтый": "Yellow",
    "жёлтый": "Yellow",
    "голубой": "Cyan",
    "циан": "Cyan",
    "цветной": "Color",
    "color": "Color",
    "black": "Black",
    "cyan": "Cyan",
    "magenta": "Magenta",
    "yellow": "Yellow",
    "red": "Red",
}


_PARAM_KEY_MAP_NVPRINT = {
    "ТипПечати": "Тип печати",
    "ЦветПечати": "Цвет печати",
    "СовместимостьСМоделями": "Совместимость с моделями",
}

def _rename_param_key_nvprint(k: str) -> str:
    k = (k or "").strip()
    if not k:
        return ""
    return _PARAM_KEY_MAP_NVPRINT.get(k, k)

def _cleanup_param_value_nvprint(k: str, v: str) -> str:
    kk = (k or "").strip()
    vv = (v or "").strip()
    if not kk or not vv:
        return vv
    cf = kk.casefold()
    if cf in ("цветпечати", "цвет печати"):
        vv_cf = vv.casefold().strip()
        return _COLOR_MAP.get(vv_cf, vv.strip())
    if cf in ("совместимостьсмоделями", "совместимость с моделями", "модель"):
        vv = _fix_confusables_to_latin_in_latin_tokens(_fix_mixed_ru(vv))
        vv = _RE_DBL_SLASH.sub("/", vv)
        vv = _RE_SLASH_BEFORE_LETTER.sub("/ ", vv)
        vv = _RE_SPACE_BEFORE_RP.sub(")", vv)
        vv = _RE_WORKCENTRE.sub("WorkCentre", vv)
        vv = _drop_unmatched_rparens(vv)
        vv = _RE_WS.sub(" ", vv).strip()
        return vv
    return vv

def _cleanup_vendor_nvprint(vendor: str, name: str) -> str:
    v = _normalize_vendor(vendor or "")
    # убираем мусорные "категории" как бренд
    if v.casefold() in {"остальное", "прочее", "прочие", "другое", "другие", "other"}:
        v = ""
    if not v:
        v = _derive_vendor_from_name(name)  # бренд принтера из "… для Kyocera …"
    if v and v.casefold() in _STOP_BRAND_CF:
        v = ""
    if not v and "nvp" in (name or "").casefold():
        v = "NVP"
    return v.strip()
def _normalize_name_prefix(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    # единообразие префиксов
    if s.casefold().startswith("тонер картридж"):
        s = "Тонер-картридж" + s[len("Тонер картридж"):]
    if s.casefold().startswith("тонер туба"):
        s = "Тонер-туба" + s[len("Тонер туба"):]
    return s

def _normalize_vendor(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    # Если есть смесь латиницы и кириллицы (часто "Kyoсera" с кириллической 'с') — приводим похожие буквы к латинице.
    has_lat = any(("A" <= ch <= "Z") or ("a" <= ch <= "z") for ch in v)
    has_cyr = any(("А" <= ch <= "я") or (ch in "Ёё") for ch in v)
    if has_lat and has_cyr:
        table = str.maketrans({
            "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X", "У": "Y",
            "а": "a", "в": "b", "е": "e", "к": "k", "м": "m", "н": "h", "о": "o", "р": "p", "с": "c", "т": "t", "х": "x", "у": "y",
        })
        v = v.translate(table)
    return v.strip()


_RE_BRAND_AFTER_DLYA = re.compile(r"\bдля\s+([A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-._]{1,40})", re.I)
_RE_BRAND_AFTER_FOR = re.compile(r"\bfor\s+([A-Za-z0-9][A-Za-z0-9\-._]{1,40})", re.I)


def _derive_vendor_from_name(name: str) -> str:
    # Берём бренд принтера из "… для Kyocera …" или "… for HP …"
    s = (name or "").strip()
    if not s:
        return ""
    m = _RE_BRAND_AFTER_DLYA.search(s)
    if m:
        return _normalize_vendor(m.group(1))
    m = _RE_BRAND_AFTER_FOR.search(s)
    if m:
        return _normalize_vendor(m.group(1))
    return ""

def main() -> int:
    url = (os.environ.get("NVPRINT_XML_URL") or "").strip()
    if not url:
        raise RuntimeError("NVPRINT_XML_URL пустой. Укажи URL в workflow env.")

    auth = _get_auth()

    now = now_almaty()
    try:
        hour = int((os.environ.get("SCHEDULE_HOUR_ALMATY", "4") or "4").strip())
    except Exception:
        hour = 4
    next_run = next_run_at_hour(now, hour)
    strict = (os.environ.get("NVPRINT_STRICT") or "").strip().lower() in ("1", "true", "yes")
    try:
        xml_bytes = _download_xml(url, auth)
    except Exception as e:
        if strict:
            raise
        print(f"NVPrint: не удалось скачать XML ({e}). Мягкий выход без падения.\n"
              "Подсказка: чтобы падало жёстко, поставь NVPRINT_STRICT=1", file=sys.stderr)
        return 0


    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        raise RuntimeError(f"NVPrint XML не парсится: {e}\nПревью:\n{_xml_head(xml_bytes)}")

    items = _find_items(root)
    if not items:
        raise RuntimeError("Не нашёл товары в NVPrint XML.\nПревью:\n" + _xml_head(xml_bytes))

    out_offers: list[OfferOut] = []
    filtered_out = 0
    in_true = 0
    in_false = 0

    for item in items:
        name = _get_text(item.find("Номенклатура")) or _get_text(item.find("НоменклатураКратко")) or _pick_first_text(item, ("name", "title", "Наименование"))
        name = _cleanup_name_nvprint(name)

        if not name:
            continue

        # Фильтр по ключевым словам (ассортимент)
        if not _include_by_name(name):
            filtered_out += 1
            continue

        oid = _make_oid(item, name)
        if not oid:
            continue
        # По требованию: всегда считаем товар в наличии
        available = True
        in_true += 1
        pin = _extract_price(item)
        price = compute_price(pin)

        pics = _collect_pictures(item)
        vendor = _pick_first_text(item, ("vendor", "brand", "Brand", "Производитель"))
        if not vendor:
            vendor = _pick_first_text(item, ("РазделМодели",))
        if not vendor:
            vendor = _pick_first_text(item, ("РазделПрайса",))
        vendor = _cleanup_vendor_nvprint(vendor, name)



        params = _collect_params(item)
        desc = _native_desc(item)

        out_offers.append(
            OfferOut(
                oid=oid,
                name=name,
                price=price,
                available=available,
                pictures=pics,
                vendor=vendor,
                params=params,
                native_desc=desc,
            )
        )

    out_offers.sort(key=lambda o: o.oid)

    header = make_header(now, encoding=OUTPUT_ENCODING)
    feed_meta = make_feed_meta(
        "NVPrint",
        url,
        now,
        next_run,
        before=len(items),
        after=len(out_offers),
        in_true=in_true,
        in_false=in_false,
    )

    offers_xml = "\n\n".join(o.to_xml(public_vendor="CS") for o in out_offers)
    full = header + "\n" + feed_meta + "\n\n" + offers_xml + "\n" + make_footer()
    full = ensure_footer_spacing(full)

    validate_cs_yml(full)

    changed = write_if_changed(OUT_FILE, full, encoding=OUTPUT_ENCODING)
    print(f"[build_nvprint] OK | offers_in={len(items)} | offers_out={len(out_offers)} | filtered_out={filtered_out} | in_true={in_true} | in_false={in_false} | changed={'yes' if changed else 'no'} | file={OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


#!/usr/bin/env python3
