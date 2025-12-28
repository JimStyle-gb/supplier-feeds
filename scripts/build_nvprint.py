#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NVPrint -> CS adapter (v63)

Задача: получить XML NVPrint, превратить в CS-оферы через общий core и собрать docs/nvprint.yml.
Core должен содержать только общее (price/desc/params/feed_meta/валидация/рендер), а здесь — только NVPrint-специфика:
- откуда скачать XML
- как достать id/name/price/available/pictures/params/desc
- (опционально) NVPrint-фильтры/правки входных данных
"""

from __future__ import annotations

import os
import re
import sys
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

# Заглушка фото (если у товара нет фото/товара нет на сайте)
PLACEHOLDER_PIC = "https://images.satu.kz/227774166_w1280_h1280_cid41038_pid120085106-4f006b4f.jpg?fresh=1"

# Если описание уже похоже на CS-шаблон — не берём его как native_desc (иначе будет дубль WhatsApp/секций)
RE_DESC_HAS_CS = re.compile(r"<!--\s*WhatsApp\s*-->|<!--\s*Описание\s*-->|<h3>\s*Характеристики\s*</h3>", re.I)

# NVPrint-мусорные параметры (если встречаются)
DROP_PARAM_NAMES_CF = {
    "артикул",
    "остаток",
    "наличие",
    "в наличии",
    "сопутствующие товары",
    "sku",
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
    # По умолчанию:
    # - workflow_dispatch / push => всегда
    # - schedule => только если dom+hour совпадают с env SCHEDULE_DOM/SCHEDULE_HOUR_ALMATY
    ev = (os.environ.get("GITHUB_EVENT_NAME") or "").strip().lower()
    now = _almaty_now()

    dom = os.environ.get("SCHEDULE_DOM", "1,10,20")
    try:
        hour = int((os.environ.get("SCHEDULE_HOUR_ALMATY", "4") or "4").strip())
    except Exception:
        hour = 4

    allowed = _parse_dom_list(dom) or {1, 10, 20}
    day_ok = now.day in allowed
    hour_ok = now.hour == hour

    if ev == "schedule":
        ok = day_ok and hour_ok
    else:
        ok = True

    # диагностическая строка в логах (как у остальных)
    print(
        f"Event={ev or 'unknown'}; Almaty now: {now:%Y-%m-%d %H:%M:%S}; "
        f"allowed_dom={','.join(map(str, sorted(allowed)))}; hour={hour}; "
        f"day_ok={day_ok}; should_run={'yes' if ok else 'no'}"
    )
    return ok


def _next_run(now: datetime, *, allowed_dom: set[int], hour: int) -> datetime:
    # Считаем ближайшую сборку по правилам NVPrint (allowed_dom + фиксированный hour)
    # now — наивный (Алматы)
    for add_days in range(0, 370):
        d = now.date() + timedelta(days=add_days)
        if d.day not in allowed_dom:
            continue
        cand = datetime(d.year, d.month, d.day, hour, 0, 0)
        if cand > now:
            return cand
    # fallback на всякий случай
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
    kwargs = {"timeout": (10, 60), "headers": headers}
    if auth:
        kwargs["auth"] = (auth.login, auth.password)

    r = requests.get(url, **kwargs)
    if r.status_code != 200 or not r.content:
        raise RuntimeError(f"Не удалось скачать NVPrint XML: http={r.status_code} bytes={len(r.content or b'')}")
    return r.content


def _local(tag: str) -> str:
    # Убираем namespace: "{ns}Tag" -> "Tag"
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _xml_head(xml_bytes: bytes, limit: int = 1200) -> str:
    # Короткий превью для логов, чтобы понять структуру ответа (без огромных дампов)
    try:
        s = xml_bytes.decode("utf-8")
    except Exception:
        try:
            s = xml_bytes.decode("cp1251")
        except Exception:
            s = xml_bytes.decode("utf-8", errors="replace")
    s = s.replace("\r", "")
    return s[:limit]


def _detect_item_nodes(root: ET.Element) -> list[ET.Element]:
    # 1) Быстрый путь: любые *offer* (с любым регистром/namespace)
    offers = [el for el in root.iter() if _local(el.tag).casefold() == "offer"]
    if offers:
        return offers

    # 2) Авто-детект "строки" по частотному тегу, который содержит name+price
    name_keys = {"name", "title", "наименование"}
    price_keys = {"price", "base_price", "purchase_price", "cost", "цена", "цена_кзт", "pricekzt"}

    def has_key(el: ET.Element, keys: set[str]) -> bool:
        for ch in list(el):
            if _local(ch.tag).casefold() in keys:
                return True
        return False

    counts: dict[str, int] = {}
    candidates: list[ET.Element] = []

    for el in root.iter():
        kids = list(el)
        if len(kids) < 2:
            continue
        if not has_key(el, name_keys):
            continue
        if not has_key(el, price_keys):
            continue
        ln = _local(el.tag).casefold()
        counts[ln] = counts.get(ln, 0) + 1
        candidates.append(el)

    if not counts:
        return []

    best_tag, best_cnt = max(counts.items(), key=lambda kv: kv[1])
    # если всего 1-2 узла — это не "лист товаров"
    if best_cnt < 5:
        return []

    out = [el for el in candidates if _local(el.tag).casefold() == best_tag]
    return out

def _find_offers(root: ET.Element) -> list[ET.Element]:
    return _detect_item_nodes(root)


def _get_text(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _pick_first_text(offer: ET.Element, tags: tuple[str, ...]) -> str:
    for t in tags:
        v = _get_text(offer.find(t))
        if v:
            return v
    return ""


def _make_oid(offer: ET.Element, name: str) -> str:
    # Главное: oid должен быть стабильный, иначе будут "новые товары".
    # Берём то, что даёт поставщик: vendorCode -> @id -> article/code -> stable_id(name)
    oid = _pick_first_text(offer, ("vendorCode", "article", "code", "sku", "id"))
    if not oid:
        oid = (offer.get("id") or "").strip()

    oid = oid.strip()
    if not oid:
        oid = stable_id(name)

    # Лёгкая нормализация: оставляем безопасные символы
    out = []
    for ch in oid:
        if re.fullmatch(r"[A-Za-z0-9_.-]", ch):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


def _parse_price(text: str) -> int | None:
    t = (text or "").strip()
    if not t:
        return None
    t = t.replace("\xa0", " ").replace(" ", "")
    t = t.replace(",", ".")
    m = re.search(r"-?\d+(\.\d+)?", t)
    if not m:
        return None
    try:
        return int(float(m.group(0)))
    except Exception:
        return None


def _collect_pictures(offer: ET.Element, oid: str) -> list[str]:
    pics: list[str] = []
    for p in offer.findall("picture"):
        u = _get_text(p)
        if not u:
            continue
        u = u.strip()
        if u.startswith("//"):
            u = "https:" + u
        if u.startswith("/"):
            u = "https://nvprint.ru" + u
        pics.append(u)

    # если в исходнике нет picture — ставим заглушку
    if not pics:
        return [PLACEHOLDER_PIC]

    # дедуп
    seen = set()
    out: list[str] = []
    for u in pics:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _collect_params(offer: ET.Element) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for p in offer.findall("param"):
        k = (p.get("name") or "").strip()
        v = _get_text(p)
        if not k or not v:
            continue
        if k.casefold() in DROP_PARAM_NAMES_CF:
            continue
        # мусор: Гарантия=0
        if k.casefold() == "гарантия" and v.strip().casefold() in ("0", "0 мес", "0 месяцев", "0мес"):
            continue
        out.append((k, v))
    return out


def _native_desc(offer: ET.Element) -> str:
    d = _get_text(offer.find("description"))
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
        raise RuntimeError(f"NVPrint XML не парсится: {e}")

    offers_in = _find_offers(root)
    if not offers_in:
        raise RuntimeError("Не нашёл товары в NVPrint XML. Превью ответа:\n" + _xml_head(xml_bytes))

    out_offers: list[OfferOut] = []
    in_true = 0
    in_false = 0

    for offer in offers_in:
        name = _pick_first_text(offer, ("name", "title", "Name", "Наименование"))
        name = norm_ws(name)
        if not name:
            continue

        oid = _make_oid(offer, name)

        # available
        av_raw = (offer.get("available") or "").strip().lower()
        if av_raw in ("true", "1", "yes", "y"):
            available = True
        elif av_raw in ("false", "0", "no", "n"):
            available = False
        else:
            # fallback: иногда есть тег available
            av_tag = _pick_first_text(offer, ("available",))
            available = av_tag.strip().lower() in ("true", "1", "yes", "y")

        if available:
            in_true += 1
        else:
            in_false += 1

        # цена поставщика (вход)
        pin = _parse_price(_pick_first_text(offer, ("purchase_price", "base_price", "price", "Price")))
        price = compute_price(pin)

        pics = _collect_pictures(offer, oid)

        vendor = _pick_first_text(offer, ("vendor", "brand", "Brand", "Производитель"))
        params = _collect_params(offer)
        desc = _native_desc(offer)

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

    # стабилизируем порядок, чтобы не было случайных диффов
    out_offers.sort(key=lambda o: o.oid)

    header = make_header(now, encoding=OUTPUT_ENCODING)
    feed_meta = make_feed_meta(
        "NVPrint",
        url,
        now,
        next_run,
        before=len(offers_in),
        after=len(out_offers),
        in_true=in_true,
        in_false=in_false,
    )

    offers_xml = "\n\n".join(o.to_xml(public_vendor="CS") for o in out_offers)
    full = header + "\n" + feed_meta + "\n\n" + offers_xml + "\n" + make_footer()
    full = ensure_footer_spacing(full)

    validate_cs_yml(full)

    changed = write_if_changed(OUT_FILE, full, encoding=OUTPUT_ENCODING)
    print(f"[nvprint] offers_in={len(offers_in)} offers_out={len(out_offers)} changed={changed} file={OUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
