# -*- coding: utf-8 -*-
"""
NVPrint -> CS adapter (clean wave1, source-XML aware)

Логика этой волны:
- используем реальный source XML;
- берём только договор Алматы: ТА-000079;
- оставляем только товары, где по этому договору есть Цена > 0 и Количество > 0;
- shared cs/* не трогаем;
- supplier-layer пока минимальный: build + source + params_xml.
"""

from __future__ import annotations

import os
import re
from xml.etree import ElementTree as ET

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

from suppliers.nvprint.source import (
    Auth,
    download_xml,
    find_items,
    get_auth,
    get_contract_price_qty,
    get_text,
    pick_first_text,
    xml_head,
)
from suppliers.nvprint.params_xml import (
    collect_params,
    native_desc,
)

OUT_FILE = "docs/nvprint.yml"
RAW_OUT_FILE = "docs/raw/nvprint.yml"
OUTPUT_ENCODING = "utf-8"

# Оставляем только реально нужные товарные типы.
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
_RE_DBL_SLASH = re.compile(r"//+")
_RE_SLASH_BEFORE_LETTER = re.compile(r"/(?!\s)(?=[A-Za-zА-Яа-я])")
_RE_NUM_SHT_WORD = re.compile(r"\b(\d+)шт\b", re.I)
_RE_SHT_MISSING_SPACE = re.compile(r"\((\d+)шт\)", re.I)
_RE_WORKCENTRE = re.compile(r"\bWorkcentr(e)?\b", re.I)

_BRAND_PATTERNS = [
    (re.compile(r"\b(HP|Hewlett[-\s]?Packard)\b", re.I), "HP"),
    (re.compile(r"\bCanon\b", re.I), "Canon"),
    (re.compile(r"\bXerox\b", re.I), "Xerox"),
    (re.compile(r"\bRicoh\b", re.I), "Ricoh"),
    (re.compile(r"\bSamsung\b", re.I), "Samsung"),
    (re.compile(r"\bKyocera\b", re.I), "Kyocera"),
    (re.compile(r"\bBrother\b", re.I), "Brother"),
    (re.compile(r"\bEpson\b", re.I), "Epson"),
    (re.compile(r"\bPanasonic\b", re.I), "Panasonic"),
    (re.compile(r"\bLexmark\b", re.I), "Lexmark"),
    (re.compile(r"\bOKI\b", re.I), "OKI"),
    (re.compile(r"\b(Катюша|KATYUSHA)\b", re.I), "КАТЮША"),
]

def _fix_mixed_ru(s: str) -> str:
    """
    Латиница -> кириллица только в русских словах.
    Нужна для случаев типа 'Cтруйный' -> 'Струйный'.
    """
    if not s:
        return ""
    lat2cyr = {
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
    out = []
    n = len(s)
    for i, ch in enumerate(s):
        rep = ch
        if ch in lat2cyr and i + 1 < n:
            nxt = s[i + 1]
            if "\u0400" <= nxt <= "\u04FF":
                rep = lat2cyr[ch]
        out.append(rep)
    return "".join(out)

def _cleanup_name(name: str) -> str:
    s = (name or "").strip()
    if not s:
        return ""
    s = _fix_mixed_ru(s)
    s = _RE_DBL_SLASH.sub("/", s)
    s = _RE_SLASH_BEFORE_LETTER.sub("/ ", s)
    s = _RE_SHT_MISSING_SPACE.sub(r"(\1 шт)", s)
    s = _RE_NUM_SHT_WORD.sub(r"\1 шт", s)
    s = _RE_WORKCENTRE.sub("WorkCentre", s)
    s = re.sub(r"^Тонер\s+картридж\b", "Тонер-картридж", s, flags=re.I)
    s = re.sub(r"^Тонер\s+туба\b", "Тонер-туба", s, flags=re.I)
    s = _RE_WS.sub(" ", s)
    return norm_ws(s)

def _name_for_filter(name: str) -> str:
    return _cleanup_name(name).casefold()

def _include_by_name(name: str) -> bool:
    cf = _name_for_filter(name)
    if not cf:
        return False
    return any(cf.startswith(p) for p in NVPRINT_INCLUDE_PREFIXES_CF)

def _cleanup_vendor(vendor: str, name: str, compat: str) -> str:
    hay = " ".join([vendor or "", name or "", compat or ""]).strip()
    for rx, rep in _BRAND_PATTERNS:
        if rx.search(hay):
            return rep
    return (vendor or "").strip()

def _make_oid(item: ET.Element, name: str) -> str | None:
    raw = (
        pick_first_text(item, ("Код", "Артикул", "Guid", "code", "article"))
        or (item.get("id") or "").strip()
    )
    if not raw:
        return None
    safe = []
    for ch in raw:
        if re.fullmatch(r"[A-Za-z0-9_.-]", ch):
            safe.append(ch)
        else:
            safe.append("_")
    oid = "".join(safe)
    if not oid.startswith("NP"):
        oid = "NP" + oid
    return oid

def _collect_pictures(item: ET.Element) -> list[str]:
    pics: list[str] = []
    for tag_name in ("СсылкаНаКартинку", "СсылкаНаКартинку1", "СсылкаНаКартинку2", "Picture", "Image"):
        u = pick_first_text(item, (tag_name,))
        u = (u or "").strip()
        if not u:
            continue
        if u.startswith("//"):
            u = "https:" + u
        if u.startswith("http://"):
            u = "https://" + u[len("http://"):]
        if u.startswith("/"):
            u = "https://nvprint.ru" + u
        if u not in pics:
            pics.append(u)
    return pics

def main() -> int:
    url = (os.environ.get("NVPRINT_XML_URL") or "").strip()
    if not url:
        raise RuntimeError("NVPRINT_XML_URL пустой. Укажи URL в workflow env.")

    target_contract = (os.environ.get("NVPRINT_TARGET_CONTRACT") or "ТА-000079").strip()
    auth = get_auth(
        login=(os.environ.get("NVPRINT_LOGIN") or "").strip(),
        password=(os.environ.get("NVPRINT_PASSWORD") or os.environ.get("NVPRINT_PASS") or "").strip(),
    )

    now = now_almaty()
    now_naive = now.replace(tzinfo=None)
    try:
        hour = int((os.environ.get("SCHEDULE_HOUR_ALMATY", "4") or "4").strip())
    except Exception:
        hour = 4
    next_run = next_run_dom_at_hour(now_naive, hour, (1, 10, 20))

    xml_bytes = download_xml(
        url=url,
        auth=auth,
        retries=int((os.environ.get("NVPRINT_HTTP_RETRIES", "4") or "4").strip() or "4"),
        t_connect=int((os.environ.get("NVPRINT_TIMEOUT_CONNECT", "20") or "20").strip() or "20"),
        t_read=int((os.environ.get("NVPRINT_TIMEOUT_READ", "120") or "120").strip() or "120"),
    )

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        raise RuntimeError(f"NVPrint XML не парсится: {e}\nПревью:\n{xml_head(xml_bytes)}")

    items = find_items(root)
    if not items:
        raise RuntimeError("Не нашёл товары в NVPrint XML.\nПревью:\n" + xml_head(xml_bytes))

    out_offers: list[OfferOut] = []
    filtered_prefix = 0
    filtered_contract = 0

    for item in items:
        name = (
            pick_first_text(item, ("Номенклатура", "НоменклатураКратко", "name", "title", "Наименование"))
            or ""
        ).strip()
        name = _cleanup_name(name)
        if not name:
            continue

        if not _include_by_name(name):
            filtered_prefix += 1
            continue

        contract_price, contract_qty = get_contract_price_qty(item, target_contract)
        if not contract_price or contract_price <= 0 or not contract_qty or contract_qty <= 0:
            filtered_contract += 1
            continue

        oid = _make_oid(item, name)
        if not oid:
            continue

        params = collect_params(item)
        compat = ""
        for k, v in params:
            if (k or "").casefold() == "совместимость с моделями" and v:
                compat = v
                break

        vendor = pick_first_text(item, ("Производитель", "vendor", "brand", "РазделМодели", "РазделПрайса"))
        vendor = _cleanup_vendor(vendor, name, compat)

        pics = _collect_pictures(item)
        desc = native_desc(item)

        out_offers.append(
            OfferOut(
                oid=oid,
                name=name,
                price=compute_price(int(contract_price)),
                available=True,
                pictures=pics,
                vendor=vendor,
                params=params,
                native_desc=desc,
            )
        )

    out_offers.sort(key=lambda o: o.oid)

    write_cs_feed_raw(
        out_offers,
        supplier="NVPrint",
        supplier_url=url,
        out_file=RAW_OUT_FILE,
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
        public_vendor=get_public_vendor("NVPrint"),
        currency_id="KZT",
        param_priority=None,
    )

    print(
        f"[build_nvprint] OK | offers_in={len(items)} | offers_out={len(out_offers)} | "
        f"filtered_prefix={filtered_prefix} | filtered_contract={filtered_contract} | "
        f"changed={'yes' if changed else 'no'} | file={OUT_FILE}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
