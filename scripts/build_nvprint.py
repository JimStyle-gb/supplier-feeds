#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NVPrint -> CS adapter (v65, 1C-XML "КаталогТоваров/Товары/Товар")

Core (cs/core.py) = только общее: цена/description/params/feed_meta/рендер/валидация/запись.
Этот файл = только NVPrint-специфика: скачать XML, распарсить "Товар", собрать OfferOut.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

import requests

from cs.core import (
    OfferOut,
    compute_price,
    ensure_footer_spacing,
    make_feed_meta,
    make_footer,
    make_header,
    norm_ws,
    stable_id,
    validate_cs_yml,
    write_if_changed,
)

TZ_ALMATY = ZoneInfo("Asia/Almaty")

OUT_FILE = "docs/nvprint.yml"
OUTPUT_ENCODING = "utf-8"

# Заглушка фото (если у товара нет фото/карточки)
PLACEHOLDER_PIC = "https://images.satu.kz/227774166_w1280_h1280_cid41038_pid120085106-4f006b4f.jpg?fresh=1"

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
}


def _almaty_now() -> datetime:
    return datetime.now(TZ_ALMATY).replace(tzinfo=None)


def _parse_dom_list(s: str) -> set[int]:
    out: set[int] = set()
    for x in (s or "").split(","):
        x = x.strip()
        if not x:
            continue
        try:
            out.add(int(x))
        except Exception:
            pass
    return out


def _should_run() -> bool:
    # schedule => только если dom+hour совпадают (env SCHEDULE_DOM/SCHEDULE_HOUR_ALMATY)
    # push/workflow_dispatch => всегда
    ev = (os.environ.get("GITHUB_EVENT_NAME") or "").strip().lower()
    now = _almaty_now()

    allowed = _parse_dom_list(os.environ.get("SCHEDULE_DOM", "1,10,20")) or {1, 10, 20}
    try:
        hour = int((os.environ.get("SCHEDULE_HOUR_ALMATY", "4") or "4").strip())
    except Exception:
        hour = 4

    day_ok = now.day in allowed
    hour_ok = now.hour == hour

    ok = (day_ok and hour_ok) if ev == "schedule" else True

    print(
        f"Event={ev or 'unknown'}; Almaty now: {now:%Y-%m-%d %H:%M:%S}; "
        f"allowed_dom={','.join(map(str, sorted(allowed)))}; hour={hour}; "
        f"day_ok={day_ok}; should_run={'yes' if ok else 'no'}"
    )
    return ok


def _next_run(now: datetime, *, allowed_dom: set[int], hour: int) -> datetime:
    for add_days in range(0, 370):
        d = now.date() + timedelta(days=add_days)
        if d.day not in allowed_dom:
            continue
        cand = datetime(d.year, d.month, d.day, hour, 0, 0)
        if cand > now:
            return cand
    return (now + timedelta(days=1)).replace(hour=hour, minute=0, second=0, microsecond=0)


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
    headers = {
        "User-Agent": "Mozilla/5.0 (CS bot; NVPrint adapter)",
        "Accept": "application/xml,text/xml,*/*",
    }
    kwargs = {"timeout": (10, 90), "headers": headers}
    if auth:
        kwargs["auth"] = (auth.login, auth.password)

    r = requests.get(url, **kwargs)
    if r.status_code != 200 or not r.content:
        raise RuntimeError(f"Не удалось скачать NVPrint XML: http={r.status_code} bytes={len(r.content or b'')}")
    return r.content


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


def _make_oid(item: ET.Element, name: str) -> str:
    raw = (
        _pick_first_text(item, ("vendorCode", "article", "Артикул", "sku", "code", "Код", "Guid"))
        or (item.get("id") or "").strip()
    )
    if not raw:
        raw = stable_id(name)

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
    av_raw = (item.get("available") or "").strip().lower()
    if av_raw in ("true", "1", "yes", "y"):
        return True
    if av_raw in ("false", "0", "no", "n"):
        return False

    qty_kz: float = 0.0
    qty_any: float = 0.0
    has_any = False

    for el in item.iter():
        tag = _local(el.tag).casefold()
        if tag not in ("остаток", "количество", "колво", "кол-во", "qty", "quantity"):
            continue
        n = _parse_num(_get_text(el))
        if n is None:
            continue
        has_any = True
        qty_any += n

        attrs = " ".join([str(v) for v in el.attrib.values()]).casefold()
        if "казахстан" in attrs:
            qty_kz += n

    if has_any:
        use = qty_kz if qty_kz > 0 else qty_any
        return use > 0

    av_tag = _pick_first_text(item, ("available", "Available", "Наличие"))
    if av_tag:
        return av_tag.strip().lower() in ("true", "1", "yes", "y", "есть", "да")

    return True


def _collect_pictures(item: ET.Element) -> list[str]:
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

    if not pics:
        return [PLACEHOLDER_PIC]

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
        if k.casefold() == "гарантия" and v.strip().casefold() in ("0", "0 мес", "0 месяцев", "0мес"):
            continue
        out.append((k, v))

    if out:
        return out

    skip_keys = {
        "код", "артикул", "guid",
        "номенклатура", "номенклатуракратко", "наименование",
        "цена", "ценасндс", "ценабезндс", "цена_кзт", "price",
        "new_reman", "разделпрайса",
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
        if cf == "гарантия" and v.strip().casefold() in ("0", "0 мес", "0 месяцев", "0мес"):
            continue
        out.append((k, v))

    return out


def _native_desc(item: ET.Element) -> str:
    d = _pick_first_text(item, ("description", "Описание"))
    if not d:
        return ""
    if RE_DESC_HAS_CS.search(d):
        return ""
    return d


def main() -> int:
    if not _should_run():
        return 0

    url = (os.environ.get("NVPRINT_XML_URL") or "").strip()
    if not url:
        raise RuntimeError("NVPRINT_XML_URL пустой. Укажи URL в workflow env.")

    auth = _get_auth()

    now = _almaty_now()
    allowed_dom = _parse_dom_list(os.environ.get("SCHEDULE_DOM", "1,10,20")) or {1, 10, 20}
    try:
        hour = int((os.environ.get("SCHEDULE_HOUR_ALMATY", "4") or "4").strip())
    except Exception:
        hour = 4
    next_run = _next_run(now, allowed_dom=allowed_dom, hour=hour)

    xml_bytes = _download_xml(url, auth)

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        raise RuntimeError(f"NVPrint XML не парсится: {e}\nПревью:\n{_xml_head(xml_bytes)}")

    items = _find_items(root)
    if not items:
        raise RuntimeError("Не нашёл товары в NVPrint XML.\nПревью:\n" + _xml_head(xml_bytes))

    out_offers: list[OfferOut] = []
    in_true = 0
    in_false = 0

    for item in items:
        name = _pick_first_text(item, ("name", "title", "НоменклатураКратко", "Номенклатура", "Наименование"))
        name = norm_ws(name)
        if not name:
            continue

        oid = _make_oid(item, name)

        available = _extract_available(item)
        if available:
            in_true += 1
        else:
            in_false += 1

        pin = _extract_price(item)
        price = compute_price(pin)

        pics = _collect_pictures(item)

        vendor = _pick_first_text(item, ("vendor", "brand", "Brand", "Производитель"))
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
    print(f"[nvprint] items_in={len(items)} offers_out={len(out_offers)} in_true={in_true} in_false={in_false} changed={changed} file={OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
