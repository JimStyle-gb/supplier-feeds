# -*- coding: utf-8 -*-
"""
CS Writer — сборка XML/YML и запись файлов.

Этап 3: вынос из cs/core.py в отдельный модуль, без изменения логики.
Важно: модуль НЕ импортирует cs/core.py (чтобы не ловить циклические импорты).
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Sequence

OUTPUT_ENCODING_DEFAULT = "utf-8"
CURRENCY_ID_DEFAULT = "KZT"

def xml_escape_text(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# XML escape для атрибутов

def xml_escape_attr(s: str) -> str:
    return xml_escape_text(s).replace('"', "&quot;")


# bool → "true/false"

def bool_to_xml(v: bool) -> str:
    return "true" if bool(v) else "false"


# Каноническое правило цены (4% + надбавки + хвост 900; невалидно/<=100 → 100; >=9,000,000 → 100)
# Тарифные пороги для compute_price (как в эталоне)
CS_PRICE_TIERS = [
    (101, 10_000, 3_000),
    (10_001, 25_000, 4_000),
    (25_001, 50_000, 5_000),
    (50_001, 75_000, 7_000),
    (75_001, 100_000, 10_000),
    (100_001, 150_000, 12_000),
    (150_001, 200_000, 15_000),
    (200_001, 300_000, 20_000),
    (300_001, 500_000, 25_000),
    (500_001, 750_000, 30_000),
    (750_001, 1_000_000, 35_000),
    (1_000_001, 1_500_000, 40_000),
    (1_500_001, 2_000_000, 45_000),
]

def xml_escape(s: str) -> str:
    """Экранирует текст для безопасного HTML/XML вывода."""
    if s is None:
        return ""
    s = str(s)
    return (s
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;"))

def make_header(build_time: datetime, *, encoding: str = OUTPUT_ENCODING_DEFAULT) -> str:
    return (
        f"<?xml version=\"1.0\" encoding=\"{encoding}\"?>\n"
        f"<yml_catalog date=\"{build_time:%Y-%m-%d %H:%M}\">\n"
        f"<shop><offers>\n"
    )


# Низ файла

def make_footer() -> str:
    return "</offers>\n</shop>\n</yml_catalog>\n"


# Гарантирует пустую строку после <offers> и перед </offers>

def ensure_footer_spacing(xml: str) -> str:
    xml = re.sub(r"(<offers>\n)(\n*)", r"\1\n", xml, count=1)
    xml = re.sub(r"(</offer>\n)(</offers>)", r"\1\n\2", xml)
    return xml


# CS: публичный vendor (нельзя светить названия поставщиков)

def make_feed_meta(
    supplier: str,
    supplier_url: str,
    build_time: datetime,
    next_run: datetime,
    *,
    before: int,
    after: int,
    in_true: int,
    in_false: int,
) -> str:
    lines = [
        "<!--FEED_META",
        f"Поставщик                                  | {supplier}",
        f"URL поставщика                             | {supplier_url}",
        f"Время сборки (Алматы)                      | {build_time:%Y-%m-%d %H:%M:%S}",
        f"Ближайшая сборка (Алматы)                  | {next_run:%Y-%m-%d %H:%M:%S}",
        f"Сколько товаров у поставщика до фильтра    | {before}",
        f"Сколько товаров у поставщика после фильтра | {after}",
        f"Сколько товаров есть в наличии (true)      | {in_true}",
        f"Сколько товаров нет в наличии (false)      | {in_false}",
        "-->",
    ]
    return "\n".join(lines)


# Верх файла (минимальный shop+offers; витрина будет в cs_price позже)

def build_cs_feed_xml(
    offers: Sequence["OfferOut"],
    *,
    supplier: str,
    supplier_url: str,
    build_time: datetime,
    next_run: datetime,
    before: int,
    encoding: str = OUTPUT_ENCODING_DEFAULT,
    public_vendor: str = "CS",
    currency_id: str = CURRENCY_ID_DEFAULT,
    param_priority: Sequence[str] | None = None,
) -> str:
    after = len(offers)
    in_true = sum(1 for o in offers if getattr(o, "available", False))
    in_false = after - in_true
    meta = make_feed_meta(
        supplier=supplier,
        supplier_url=supplier_url,
        build_time=build_time,
        next_run=next_run,
        before=before,
        after=after,
        in_true=in_true,
        in_false=in_false,
    )

    offers_xml = ""
    if offers:
        offers_xml = "\n\n".join(
            [
                o.to_xml(
                    currency_id=currency_id,
                    public_vendor=public_vendor,
                    param_priority=param_priority,
                )
                for o in offers
            ]
        )

    xml = make_header(build_time, encoding=encoding) + "\n" + meta + "\n\n" + offers_xml + "\n\n" + make_footer()
    return ensure_footer_spacing(xml)


# Строит СЫРОЙ XML-фид (без validate и без логики to_xml)

def build_cs_feed_xml_raw(
    offers: Sequence["OfferOut"],
    *,
    supplier: str,
    supplier_url: str,
    build_time: datetime,
    next_run: datetime,
    before: int,
    encoding: str = OUTPUT_ENCODING_DEFAULT,
    currency_id: str = CURRENCY_ID_DEFAULT,
) -> str:
    after = len(offers)
    in_true = sum(1 for o in offers if getattr(o, "available", False))
    in_false = after - in_true
    meta = make_feed_meta(
        supplier=supplier,
        supplier_url=supplier_url,
        build_time=build_time,
        next_run=next_run,
        before=before,
        after=after,
        in_true=in_true,
        in_false=in_false,
    )

    offers_xml = ""
    if offers:
        offers_xml = "\n\n".join([o.to_xml_raw(currency_id=currency_id) for o in offers])

    xml = make_header(build_time, encoding=encoding) + "\n" + meta + "\n\n" + offers_xml + "\n\n" + make_footer()
    return ensure_footer_spacing(xml)


# CS: пишет сырой фид в файл (без validate)

def write_if_changed(path: str, data: str, *, encoding: str = OUTPUT_ENCODING_DEFAULT) -> bool:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    new_bytes = data.encode(encoding, errors="strict")

    if p.exists():
        old = p.read_bytes()
        if old == new_bytes:
            return False

    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_bytes(new_bytes)
    tmp.replace(p)
    return True


# Словарь брендов для pick_vendor (упорядочен, расширяем при необходимости)
CS_BRANDS_MAP = {
    "hp": "HP",
    "hewlett": "HP",
    "canon": "Canon",
    "epson": "Epson",
    "brother": "Brother",
    "samsung": "Samsung",
    "sv": "SVC",
    "svc": "SVC",
    "apc": "APC",
    "schneider": "Schneider Electric",
    "cyberpower": "CyberPower",
    "cyber-power": "CyberPower",
    "cyber power": "CyberPower",
    "smart": "SMART",
    "idprt": "IDPRT",
    "id-prt": "IDPRT",
    "id prt": "IDPRT",
    "asus": "ASUS",
    "lenovo": "Lenovo",
    "acer": "Acer",
    "dell": "Dell",
    "logitech": "Logitech",
    "xiaomi": "Xiaomi",

    "ripo": "RIPO",
    "xerox": "Xerox",
    "kyocera": "Kyocera",
    "ricoh": "Ricoh",
    "toshiba": "Toshiba",
    "integral": "INTEGRAL",
    "pantum": "Pantum",
    "oki": "OKI",
    "lexmark": "Lexmark",
    "konica": "Konica Minolta",
    "minolta": "Konica Minolta",
    "fujifilm": "FUJIFILM",
    "huawei": "Huawei",
    "deli": "Deli",
    "olivetti": "Olivetti",
    "panasonic": "Panasonic",
    "riso": "Riso",
    "avision": "Avision",
    "fellowes": "Fellowes",
    "viewsonic": "ViewSonic",
    "philips": "Philips",
    "zebra": "Zebra",
    "euro print": "Euro Print",
    "designjet": "HP",
    "mr.pixel": "Mr.Pixel",
    "hyperx": "HyperX",
    "aoc": "AOC",
    "benq": "BenQ",
    "lg": "LG",
    "msi": "MSI",
    "gigabyte": "GIGABYTE",
    "tp-link": "TP-Link",
    "tplink": "TP-Link",
    "mikrotik": "MikroTik",
    "ubiquiti": "Ubiquiti",
    "d-link": "D-Link",
    "europrint": "Euro Print",
    "brothe": "Brother",
}
