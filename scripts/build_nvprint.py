# -*- coding: utf-8 -*-
"""
NVPrint -> CS adapter (step1-safe split)

Шаг 1: безопасный вынос source/filtering.
Core и остальная NVPrint-логика пока не трогаются.
"""

from __future__ import annotations

import os
import sys
import time
import random
import re
from dataclasses import dataclass
from xml.etree import ElementTree as ET

import requests

from cs.core import (
    OfferOut,
    compute_price,
    get_public_vendor,
    next_run_dom_at_hour,
    now_almaty,
    norm_ws,
    write_cs_feed,
    write_cs_feed_raw,
)

# Первый безопасный вынос
from suppliers.nvprint.source import (
    Auth as SourceAuth,
    get_auth as _src_get_auth,
    download_xml as _src_download_xml,
    xml_head as _src_xml_head,
    get_text as _src_get_text,
    pick_first_text as _src_pick_first_text,
    iter_children as _src_iter_children,
    find_items as _src_find_items,
)
from suppliers.nvprint.filtering import (
    fix_mixed_ru as _flt_fix_mixed_ru,
    name_for_filter as _flt_name_for_filter,
    include_by_name as _flt_include_by_name,
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
    "код",
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

# Фильтр ассортимента NVPrint
NVPRINT_INCLUDE_PREFIXES_CF = [
    "блок фотобарабана",
    "картридж",
    "печатающая головка",
    "струйный картридж",
    "тонер-картридж",
    "тонер картридж",
    "тонер-туба",
    "тонер туба",
]

_RE_WS = re.compile(r"\s+")


# ---- wrappers over source/filtering (step1 safe split) ----

_Auth = SourceAuth

def _fix_mixed_ru(s: str) -> str:
    return _flt_fix_mixed_ru(s)

def _name_for_filter(name: str) -> str:
    return _flt_name_for_filter(name)

def _include_by_name(name: str) -> bool:
    return _flt_include_by_name(name, NVPRINT_INCLUDE_PREFIXES_CF, os.environ.get("NVPRINT_INCLUDE_PREFIXES") or "")

def _get_auth() -> _Auth | None:
    return _src_get_auth(
        login=(os.environ.get("NVPRINT_LOGIN") or "").strip(),
        password=(os.environ.get("NVPRINT_PASSWORD") or os.environ.get("NVPRINT_PASS") or "").strip(),
    )

def _download_xml(url: str, auth: _Auth | None) -> bytes:
    return _src_download_xml(
        url=url,
        auth=auth,
        retries=int((os.environ.get("NVPRINT_HTTP_RETRIES", "4") or "4").strip() or "4"),
        t_connect=int((os.environ.get("NVPRINT_TIMEOUT_CONNECT", "20") or "20").strip() or "20"),
        t_read=int((os.environ.get("NVPRINT_TIMEOUT_READ", "120") or "120").strip() or "120"),
    )

def _xml_head(xml_bytes: bytes, limit: int = 2500) -> str:
    return _src_xml_head(xml_bytes, limit=limit)

def _get_text(el: ET.Element | None) -> str:
    return _src_get_text(el)

def _pick_first_text(node: ET.Element, names: tuple[str, ...]) -> str:
    return _src_pick_first_text(node, names)

def _iter_children(node: ET.Element) -> list[ET.Element]:
    return _src_iter_children(node)

def _find_items(root: ET.Element) -> list[ET.Element]:
    return _src_find_items(root)


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
        # local tag helper not needed here
        k = ch.tag.split("}", 1)[1] if "}" in ch.tag else ch.tag
        k = k.casefold()
        if k in prefer_keys:
            n = _parse_num(_get_text(ch))
            if n is not None:
                return int(n)

    found: list[int] = []
    for el in item.iter():
        k = el.tag.split("}", 1)[1] if "}" in el.tag else el.tag
        k = k.casefold()
        if "цена" in k or k in prefer_keys:
            n = _parse_num(_get_text(el))
            if n is not None and n > 0:
                found.append(int(n))

    if not found:
        return None

    return min(found)


def _collect_pictures(item: ET.Element) -> list[str]:
    pics: list[str] = []
    for el in item.iter():
        tag = el.tag.split("}", 1)[1] if "}" in el.tag else el.tag
        if tag.casefold() != "picture":
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
        k = ch.tag.split("}", 1)[1] if "}" in ch.tag else ch.tag
        k = k.strip()
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
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P", "С": "C", "Т": "T", "Х": "X", "У": "Y",
    "а": "a", "в": "b", "е": "e", "к": "k", "м": "m", "н": "h", "о": "o", "р": "p", "с": "c", "т": "t", "х": "x", "у": "y",
}

_RE_TOKEN = re.compile(r"[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-._/]+")
_RE_DBL_SLASH = re.compile(r"//+")
_RE_NV_SPACE = re.compile(r"\bNV-\s+")
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
                continue
        else:
            out.append(ch)
    return "".join(out)

def _cleanup_name_nvprint(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    s = _fix_mixed_ru(s)
    s = _fix_confusables_to_latin_in_latin_tokens(s)
    s = _RE_NV_SPACE.sub("NV-", s)
    s = _RE_DBL_SLASH.sub("/", s)
    s = _RE_SPACE_BEFORE_RP.sub(")", s)
    s = _RE_SHT_MISSING_SPACE.sub(r"(\1 шт)", s)
    s = _RE_NUM_SHT_WORD.sub(r"\1 шт", s)
    s = _RE_SLASH_BEFORE_LETTER.sub("/ ", s)
    s = _RE_WORKCENTRE.sub("WorkCentre", s)
    s = _drop_unmatched_rparens(s)
    s = norm_ws(s)
    s = _normalize_name_prefix(s)
    s = re.sub(r"^Тонер\s+картридж\b", "Тонер-картридж", s, flags=re.I)
    s = _RE_WS.sub(" ", s).strip()
    return s

# ... оставшаяся NVPrint-логика без изменений ...
# Чтобы шаг был безопасным, ниже оставляем рабочие функции из монолита как есть.

_RE_PREFIX_FIXES = [
    (re.compile(r"^Cтруйный\b", re.I), "Струйный"),
    (re.compile(r"^Tонер[\s-]*картридж\b", re.I), "Тонер-картридж"),
    (re.compile(r"^Kартридж\b", re.I), "Картридж"),
]

def _normalize_name_prefix(s: str) -> str:
    for rx, rep in _RE_PREFIX_FIXES:
        if rx.search(s):
            s = rx.sub(rep, s)
            break
    return s

def _rename_param_key_nvprint(k: str) -> str:
    k0 = norm_ws((k or "").strip())
    cf = k0.casefold()
    if cf in {"цвет печати", "цвет"}:
        return "Цвет печати"
    if cf in {"тип печати", "тип"}:
        return "Тип печати"
    if cf in {"совместимость", "совместимость с моделями"}:
        return "Совместимость с моделями"
    if cf in {"код товара", "штрихкод", "штрих-код"}:
        return "ШтрихКод"
    return k0

def _cleanup_param_value_nvprint(k: str, v: str) -> str:
    s = norm_ws(v)
    if not s:
        return ""
    if (k or "").casefold() in {"тип печати", "типпечати"}:
        low = s.casefold()
        if "лазер" in low:
            return "Лазерная"
        if "струйн" in low:
            return "Струйная"
    return s

_RE_HP = re.compile(r"\b(HP|Hewlett[-\s]?Packard)\b", re.I)
_RE_CANON = re.compile(r"\bCanon\b", re.I)
_RE_XEROX = re.compile(r"\bXerox\b", re.I)
_RE_RICOH = re.compile(r"\bRicoh\b", re.I)
_RE_SAMSUNG = re.compile(r"\bSamsung\b", re.I)
_RE_KYOCERA = re.compile(r"\bKyocera\b", re.I)
_RE_BROTHER = re.compile(r"\bBrother\b", re.I)
_RE_EPSON = re.compile(r"\bEpson\b", re.I)
_RE_PANASONIC = re.compile(r"\bPanasonic\b", re.I)
_RE_LEXMARK = re.compile(r"\bLexmark\b", re.I)
_RE_KATYUSHA = re.compile(r"\b(Катюша|Katyusha)\b", re.I)

def _cleanup_vendor_nvprint(vendor: str, name: str) -> str:
    s = norm_ws((vendor or "").strip())
    hay = f"{s} {name}".strip()
    if _RE_HP.search(hay):
        return "HP"
    if _RE_CANON.search(hay):
        return "Canon"
    if _RE_XEROX.search(hay):
        return "Xerox"
    if _RE_RICOH.search(hay):
        return "Ricoh"
    if _RE_SAMSUNG.search(hay):
        return "Samsung"
    if _RE_KYOCERA.search(hay):
        return "Kyocera"
    if _RE_BROTHER.search(hay):
        return "Brother"
    if _RE_EPSON.search(hay):
        return "Epson"
    if _RE_PANASONIC.search(hay):
        return "Panasonic"
    if _RE_LEXMARK.search(hay):
        return "Lexmark"
    if _RE_KATYUSHA.search(hay):
        return "КАТЮША"
    return s

def main() -> int:
    url = (os.environ.get("NVPRINT_XML_URL") or "").strip()
    if not url:
        raise RuntimeError("NVPRINT_XML_URL пустой. Укажи URL в workflow env.")

    auth = _get_auth()

    now = now_almaty()
    now_naive = now.replace(tzinfo=None)
    try:
        hour = int((os.environ.get("SCHEDULE_HOUR_ALMATY", "4") or "4").strip())
    except Exception:
        hour = 4
    next_run = next_run_dom_at_hour(now_naive, hour, (1, 10, 20))
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

        if not _include_by_name(name):
            filtered_out += 1
            continue

        oid = _make_oid(item, name)
        if not oid:
            continue

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

    public_vendor = get_public_vendor("NVPrint")

    write_cs_feed_raw(
        out_offers,
        supplier="NVPrint",
        supplier_url=url,
        out_file="docs/raw/nvprint.yml",
        build_time=now,
        next_run=next_run,
        before=len(items),
        encoding=OUTPUT_ENCODING,
        currency_id="KZT",
    )

    changed = write_cs_feed(
        out_offers,
        supplier="NVPrint",
        supplier_url=url,
        out_file=OUT_FILE,
        build_time=now,
        next_run=next_run,
        before=len(items),
        encoding=OUTPUT_ENCODING,
        public_vendor=public_vendor,
        currency_id="KZT",
        param_priority=None,
    )

    print(
        f"[build_nvprint] OK | offers_in={len(items)} | offers_out={len(out_offers)} | filtered_out={filtered_out} | "
        f"in_true={in_true} | in_false={in_false} | changed={'yes' if changed else 'no'} | file={OUT_FILE}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
