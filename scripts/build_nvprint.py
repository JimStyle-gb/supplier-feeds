#!/usr/bin/env python3
# build_nvprint.py — генератор YML для NVPrint (windows-1251)

from __future__ import annotations

import html
import os
import re
import sys
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo


TZ = ZoneInfo("Asia/Almaty")


# ---- Константы оформления (как в результате) ----

WHATSAPP_BLOCK = (
    '<div style="font-family: Cambria, \'Times New Roman\', serif; line-height:1.5; color:#222; font-size:15px;">'
    '<p style="text-align:center; margin:0 0 12px;"><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" '
    'style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);">'
    '&#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!</a></p>'
    '<div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;">'
    '<h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3>'
    '<ul style="margin:0; padding-left:18px;"><li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li>'
    '<li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li></ul>'
    '<hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" />'
    '<h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3>'
    '<ul style="margin:0; padding-left:18px;"><li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li>'
    '<li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li>'
    '<li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>'
    '<li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li></ul>'
    '</div></div>'
)

CITIES = [
    "Алматы", "Астана", "Шымкент", "Караганда", "Актобе",
    "Павлодар", "Атырау", "Тараз", "Оскемен", "Семей",
    "Костанай", "Кызылорда", "Орал", "Петропавловск",
    "Талдыкорган", "Актау", "Темиртау", "Экибастуз", "Кокшетау",
]

PARAM_PRIORITY = [
    "Тип", "Ресурс", "Тип печати", "Цвет печати", "Цвет", "Совместимость", "Вес", "Принтеры",
]

GENERIC_VENDORS = {
    "", "NVP", "NV PRINT", "NVPRINT", "NV-PRINT", "NVPrint", "NV Print", "N V P",
}

# Бренды (включая Sindoh). Логика: ищем в name/desc/params; используем как нормальный детектор.
BRAND_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\bHP\b", re.I), "HP"),
    (re.compile(r"\bHewlett[\s-]*Packard\b", re.I), "HP"),
    (re.compile(r"\bCanon\b", re.I), "Canon"),
    (re.compile(r"\bEpson\b", re.I), "Epson"),
    (re.compile(r"\bRicoh\b", re.I), "Ricoh"),
    (re.compile(r"\bXerox\b", re.I), "Xerox"),
    (re.compile(r"\bKyocera\b", re.I), "Kyocera"),
    (re.compile(r"\bBrother\b", re.I), "Brother"),
    (re.compile(r"\bSamsung\b", re.I), "Samsung"),
    (re.compile(r"\bLexmark\b", re.I), "Lexmark"),
    (re.compile(r"\bPanasonic\b", re.I), "Panasonic"),
    (re.compile(r"\bKonica\s*Minolta\b", re.I), "Konica Minolta"),
    (re.compile(r"\bOKI\b", re.I), "OKI"),
    (re.compile(r"\bSharp\b", re.I), "Sharp"),
    (re.compile(r"\bToshiba\b", re.I), "Toshiba"),
    (re.compile(r"\bDell\b", re.I), "Dell"),
    (re.compile(r"\bRiso\b", re.I), "Riso"),
    (re.compile(r"\bFplus\b", re.I), "Fplus"),
    (re.compile(r"\bSindoh\b", re.I), "Sindoh"),
    (re.compile(r"\bКатюша\b", re.I), "Катюша"),
]


@dataclass
class Item:
    vid: str
    name: str
    base_price: float
    picture: str
    vendor_raw: str
    desc_raw: str
    available: bool
    params: Dict[str, str]


# ---- Утилиты ----

def _now_almaty() -> datetime:
    return datetime.now(TZ)


def _fmt_dt(dt: datetime, with_seconds: bool) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S" if with_seconds else "%Y-%m-%d %H:%M")


def _next_wed_0400(now: datetime) -> datetime:
    # Следующая среда 04:00 (Алматы)
    target_wd = 2  # Mon=0, Tue=1, Wed=2
    base = now.replace(second=0, microsecond=0)
    days_ahead = (target_wd - base.weekday()) % 7
    candidate = (base + timedelta(days=days_ahead)).replace(hour=4, minute=0)
    if candidate <= base:
        candidate = candidate + timedelta(days=7)
    return candidate


def _xml_escape(s: str) -> str:
    return html.escape(s, quote=False)


def _norm_spaces(s: str) -> str:
    s = s.replace("\xa0", " ")
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _safe_cdata(s: str) -> str:
    # В CDATA нельзя ']]>'
    return s.replace("]]>", "]]&gt;")


def _to_float(s: str) -> Optional[float]:
    if s is None:
        return None
    s2 = _norm_spaces(str(s))
    s2 = s2.replace(",", ".")
    s2 = re.sub(r"[^0-9.]+", "", s2)
    if not s2:
        return None
    try:
        return float(s2)
    except ValueError:
        return None


def _detect_brand(text: str) -> str:
    t = text or ""
    for rx, brand in BRAND_PATTERNS:
        if rx.search(t):
            return brand
    return ""


def _clean_vendor(v: str) -> str:
    v = _norm_spaces(v or "")
    v_up = v.upper()
    if v_up in (x.upper() for x in GENERIC_VENDORS):
        return ""
    return v


# ---- Ценообразование (4% + надбавка + округление хвостом 900/100) ----

def _adder_for_price(base: float) -> int:
    p = int(round(base))
    if 101 <= p <= 10000:
        return 3000
    if 10001 <= p <= 25000:
        return 4000
    if 25001 <= p <= 50000:
        return 5000
    if 50001 <= p <= 75000:
        return 7000
    if 75001 <= p <= 100000:
        return 10000
    if 100001 <= p <= 150000:
        return 12000
    if 150001 <= p <= 200000:
        return 15000
    if 200001 <= p <= 300000:
        return 20000
    if 300001 <= p <= 400000:
        return 25000
    return 30000


def _round_tail(price: int) -> int:
    # < 9 000 000 -> ближайшее вверх ...900; >= 9 000 000 -> ...100
    if price >= 9000000:
        base = (price // 1000) * 1000 + 100
        return base if base >= price else base + 1000
    base = (price // 1000) * 1000 + 900
    return base if base >= price else base + 1000


def calc_price_kzt(base_price: float) -> int:
    if base_price <= 0:
        return 0
    add = _adder_for_price(base_price)
    raw = int(round(base_price * 1.04 + add))
    return _round_tail(raw)


# ---- Парсинг XML (делаем максимально терпимо к схеме) ----

ID_KEYS = {"id", "xml_id", "code", "sku", "article", "articul", "vendorcode", "kod", "код", "артикул"}
NAME_KEYS = {"name", "title", "fullname", "наименование", "наименованиеполное", "именование"}
PRICE_KEYS = {"price", "цена", "стоимость", "price_kzt", "saleprice"}
QTY_KEYS = {"quantity", "qty", "stock", "остаток", "количество", "quantity_in_stock"}
VENDOR_KEYS = {"vendor", "brand", "производитель", "бренд", "вендор"}
DESC_KEYS = {"description", "desc", "описание", "text"}
PIC_KEYS = {"picture", "image", "photo", "url", "картинка", "изображение"}


def _tag_l(el: ET.Element) -> str:
    return (el.tag or "").strip().lower()


def _first_text_by_keys(el: ET.Element, keys: set) -> str:
    # 1) атрибуты
    for k, v in el.attrib.items():
        if str(k).strip().lower() in keys and v:
            return str(v)
    # 2) дочерние
    for d in el.iter():
        tl = _tag_l(d)
        if tl in keys and (d.text or "").strip():
            return d.text or ""
        # 3) property/param with name="..."
        name_attr = (d.get("name") or d.get("title") or "").strip()
        if name_attr and name_attr.lower() in keys and (d.text or "").strip():
            return d.text or ""
    return ""


def _collect_params(el: ET.Element) -> Dict[str, str]:
    params: Dict[str, str] = {}
    # собираем param/property/characteristic
    for d in el.iter():
        key = (d.get("name") or d.get("title") or "").strip()
        val = (d.text or "").strip()
        if key and val:
            params[_norm_spaces(key)] = _norm_spaces(val)
    return params


def _looks_like_item(el: ET.Element) -> bool:
    vid = _first_text_by_keys(el, ID_KEYS)
    name = _first_text_by_keys(el, NAME_KEYS)
    price = _first_text_by_keys(el, PRICE_KEYS)
    return bool(vid and name and price)


def _iter_items(root: ET.Element) -> Iterable[ET.Element]:
    # Пытаемся взять "товарные" узлы по популярным тегам, иначе — эвристикой
    preferred = {"item", "good", "product", "offer", "товар", "nomenclature", "позиция"}
    first_pass = [el for el in root.iter() if _tag_l(el) in preferred]
    picked = [el for el in first_pass if _looks_like_item(el)]
    if picked:
        return picked
    # fallback: все узлы, похожие на товар
    return [el for el in root.iter() if _looks_like_item(el)]


def _parse_available(el: ET.Element) -> bool:
    s = _first_text_by_keys(el, QTY_KEYS) or _first_text_by_keys(el, {"available"})
    s = _norm_spaces(s)
    if not s:
        return True
    if s.lower() in {"true", "yes", "y", "да"}:
        return True
    if s.lower() in {"false", "no", "n", "нет"}:
        return False
    v = _to_float(s)
    if v is None:
        return True
    return v > 0


def _parse_picture(el: ET.Element) -> str:
    # Берём первый похожий URL
    pic = _first_text_by_keys(el, PIC_KEYS)
    pic = _norm_spaces(pic)
    if pic and ("http://" in pic or "https://" in pic):
        return pic
    # Иногда фото в атрибуте
    for k, v in el.attrib.items():
        if "http" in str(v):
            return str(v).strip()
    return "http://nvprint.ru/promo/photo/nophoto.jpg"


def _http_get_xml(url: str, login: str, password: str, timeout: int) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "supplier-feeds/1.0"})
    if login and password:
        mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        mgr.add_password(None, url, login, password)
        handler = urllib.request.HTTPBasicAuthHandler(mgr)
        opener = urllib.request.build_opener(handler)
        with opener.open(req, timeout=timeout) as resp:
            return resp.read()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


# ---- Обогащение (описание ↔ характеристики ↔ param) ----

COLOR_WORDS = {
    "black": "Black",
    "cyan": "Cyan",
    "magenta": "Magenta",
    "yellow": "Yellow",
    "gray": "Gray",
    "grey": "Gray",
    "photo black": "Photo Black",
    "light cyan": "Light Cyan",
    "light magenta": "Light Magenta",
}

def _guess_type(name: str) -> str:
    n = name.lower()
    if "печатающая головка" in n:
        return "Печатающая головка"
    if "блок фотобарабана" in n:
        return "Блок фотобарабана"
    if "струй" in n and "картридж" in n:
        return "Струйный картридж"
    if "тонер" in n and "картридж" in n:
        return "Тонер-картридж"
    if "картридж" in n:
        return "Картридж"
    return ""


def _extract_resource_k(desc: str) -> str:
    # "(2300k)" -> "2300"
    m = re.search(r"\((\d{3,7})\s*k\)", desc, flags=re.I)
    if m:
        return m.group(1)
    return ""


def _extract_compatibility(text: str) -> str:
    # Берём то, что после "для", но аккуратно режем хвосты.
    t = _norm_spaces(text)
    m = re.search(r"\bдля\b\s+(.+)", t, flags=re.I)
    if not m:
        return ""
    tail = m.group(1)
    # стоп-слова/границы
    tail = re.split(r"\s*\(|\s*\bсовместим|\s*\bуниверсаль|\s*\b\(\d", tail, flags=re.I)[0]
    tail = tail.strip(" .;,:")
    # не берём слишком короткое
    if len(tail) < 6:
        return ""
    return tail


def _compat_to_printers(compat: str) -> str:
    c = _norm_spaces(compat)
    # / и ; превращаем в запятые, лишние пробелы убираем
    c = re.sub(r"\s*/\s*", "/", c)
    c = c.replace(" /", "/").replace("/ ", "/")
    c = c.replace(";", ",")
    # "A/B/C" -> "A, B, C"
    c = re.sub(r"\s*/\s*", ", ", c)
    c = re.sub(r",\s*,+", ", ", c)
    return c.strip(" ,")


def _enrich(item: Item) -> Item:
    params = dict(item.params)

    # 1) Тип (если пусто)
    if not params.get("Тип"):
        t = _guess_type(item.name)
        if t:
            params["Тип"] = t

    # 2) Тип печати
    if not params.get("Тип печати"):
        if "струй" in item.name.lower():
            params["Тип печати"] = "Струйная"
        elif "лазер" in item.name.lower():
            params["Тип печати"] = "Лазерная"

    # 3) Цвет печати (англ. цвета встречаются часто)
    if not params.get("Цвет печати"):
        s = (item.name + " " + item.desc_raw).lower()
        found = ""
        for key, val in COLOR_WORDS.items():
            if key in s:
                found = val
                break
        if found:
            params["Цвет печати"] = found

    # 4) Ресурс
    if not params.get("Ресурс"):
        r = _extract_resource_k(item.desc_raw) or _extract_resource_k(item.name)
        if r:
            params["Ресурс"] = r

    # 5) Совместимость/Принтеры из названия или описания
    text_for_compat = item.desc_raw if item.desc_raw else item.name
    compat = params.get("Совместимость", "")
    if not compat:
        compat = _extract_compatibility(text_for_compat) or _extract_compatibility(item.name)
        if compat:
            params["Совместимость"] = compat

    if not params.get("Принтеры"):
        base = params.get("Совместимость", "")
        if base:
            params["Принтеры"] = _compat_to_printers(base)
        else:
            # иногда "для ..." есть, но мы не смогли выделить — попробуем тупо
            c2 = _extract_compatibility(text_for_compat)
            if c2:
                params["Принтеры"] = _compat_to_printers(c2)

    # 6) Обогащение "обратно": если params важные, а в описании их нет — добавим коротко
    desc = _norm_spaces(item.desc_raw)
    if not desc:
        desc = item.name

    if params.get("Совместимость") and "для " not in desc.lower() and "совмест" not in desc.lower():
        desc = f"{desc} Совместимость: {params['Совместимость']}."
    elif params.get("Принтеры") and "для " not in desc.lower() and "совмест" not in desc.lower():
        desc = f"{desc} Совместимые устройства: {params['Принтеры']}."

    # vendor логика отдельно (ниже), но текст обновили
    return Item(
        vid=item.vid,
        name=item.name,
        base_price=item.base_price,
        picture=item.picture,
        vendor_raw=item.vendor_raw,
        desc_raw=desc,
        available=item.available,
        params=params,
    )


def _final_vendor(item: Item) -> str:
    # Берём исходный vendor, чистим мусор
    vendor = _clean_vendor(item.vendor_raw)

    # Общее поле для детектора
    blob = " ".join([
        item.name or "",
        item.desc_raw or "",
        " ".join([f"{k}:{v}" for k, v in item.params.items()]),
    ])

    # Частное правило: Designjet -> HP, но только если vendor пустой
    if not vendor and re.search(r"\bdesignjet\b", blob, flags=re.I):
        return "HP"

    # Нормальная логика брендов (включая Sindoh)
    det = _detect_brand(blob)

    def _vendor_by_code() -> str:
        s = (item.vid + " " + item.name + " " + item.desc_raw).upper()
        # Avision часто идёт как модель, а реальный бренд читается по коду расходника
        if re.search(r"\bTK-\d", s):
            return "Kyocera"
        if re.search(r"\bTN-\d", s) or re.search(r"\bDR-\d", s):
            return "Brother"
        if "55B5" in s:
            return "Lexmark"
        return ""

    if not vendor:
        return det or _vendor_by_code()

    # Если vendor у нас мусор/поставщик, но не пустой — всё равно отдадим детектор/код
    if vendor.upper() in (x.upper() for x in GENERIC_VENDORS):
        return det or _vendor_by_code()

    return vendor


# ---- Сборка YML ----

def _order_params(params: Dict[str, str]) -> List[Tuple[str, str]]:
    used = set()
    out: List[Tuple[str, str]] = []
    for k in PARAM_PRIORITY:
        if k in params and params[k]:
            out.append((k, params[k]))
            used.add(k)
    rest = sorted(((k, v) for k, v in params.items() if k not in used and v), key=lambda x: x[0].lower())
    out.extend(rest)
    return out


def _build_chars_html(params: List[Tuple[str, str]]) -> str:
    if not params:
        return "<h3>Характеристики</h3><ul></ul>"
    li = "".join([f"<li><strong>{_xml_escape(k)}:</strong> {_xml_escape(v)}</li>" for k, v in params])
    return f"<h3>Характеристики</h3><ul>{li}</ul>"


def _build_keywords(vendor: str, name: str, vendor_code: str, params: Dict[str, str]) -> str:
    parts: List[str] = []
    if vendor:
        parts.append(vendor)
    parts.append(name)
    parts.append(vendor_code)

    t = params.get("Тип")
    if t:
        parts.append(t)

    comp = params.get("Совместимость")
    if comp:
        parts.append(comp)

    res = params.get("Ресурс")
    if res:
        parts.append(res)

    col = params.get("Цвет печати") or params.get("Цвет")
    if col:
        parts.append(col)

    parts.extend(CITIES)
    # убираем пустые и лишние пробелы
    parts = [_norm_spaces(x) for x in parts if _norm_spaces(x)]
    return ", ".join(parts)


def build_yml(items: List[Item], src_url: str, out_file: str) -> str:
    now = _now_almaty()
    next_run = _next_wed_0400(now)

    before = len(items)
    kept = [x for x in items if x.available]
    after = len(kept)
    in_true = after
    in_false = before - after

    yml_date = _fmt_dt(now, with_seconds=False)
    build_time = _fmt_dt(now, with_seconds=True)

    src_url_meta = src_url
    if len(src_url_meta) > 60:
        src_url_meta = src_url_meta[:18] + "..." + src_url_meta[-60:]

    feed_meta = "\n".join([
        "<!--FEED_META",
        f"Поставщик                                  | NVPrint",
        f"URL поставщика                             | {src_url_meta}",
        f"Время сборки (Алматы)                      | {build_time}",
        f"Ближайшая сборка (Алматы)                  | {_fmt_dt(next_run, with_seconds=True)}",
        f"Сколько товаров у поставщика до фильтра    | {before}",
        f"Сколько товаров у поставщика после фильтра | {after}",
        f"Сколько товаров есть в наличии (true)      | {in_true}",
        f"Сколько товаров нет в наличии (false)      | {in_false}",
        "-->",
    ])

    out: List[str] = []
    out.append('<?xml version="1.0" encoding="windows-1251"?>\n')
    out.append('<!DOCTYPE yml_catalog SYSTEM "shops.dtd">\n')
    out.append(f'<yml_catalog date="{yml_date}">\n')
    out.append('<shop><offers>\n\n')
    out.append(feed_meta + "\n\n")

    for it in kept:
        # обогащаем
        it2 = _enrich(it)
        vendor = _final_vendor(it2)

        # последний шанс: если vendor пустой и есть Designjet — HP (после enrich тоже)
        if not vendor and re.search(r"\bdesignjet\b", (it2.name + " " + it2.desc_raw), flags=re.I):
            vendor = "HP"

        params_ordered = _order_params(it2.params)

        # price
        price = calc_price_kzt(it2.base_price)

        # offer
        out.append(f'<offer id="{_xml_escape(it2.vid)}" available="true">\n')
        out.append('<categoryId></categoryId>\n')
        out.append(f'<vendorCode>{_xml_escape(it2.vid)}</vendorCode>\n')
        out.append(f'<name>{_xml_escape(it2.name)}</name>\n')
        out.append(f'<price>{price}</price>\n')
        out.append(f'<picture>{_xml_escape(it2.picture)}</picture>\n')
        if vendor:
            out.append(f'<vendor>{_xml_escape(vendor)}</vendor>\n')
        out.append('<currencyId>KZT</currencyId>\n')

        # description CDATA
        desc_html = (
            "\n\n<!-- WhatsApp -->\n"
            f"{WHATSAPP_BLOCK}\n\n"
            "<!-- Описание -->\n"
            f"<h3>{_xml_escape(it2.name)}</h3><p>{_xml_escape(it2.desc_raw)}</p>\n"
            f"{_build_chars_html(params_ordered)}\n"
        )
        out.append("<description><![CDATA[" + _safe_cdata(desc_html) + "\n]]></description>\n")

        # params (в том же порядке, что и блок характеристик)
        for k, v in params_ordered:
            out.append(f'<param name="{_xml_escape(k)}">{_xml_escape(v)}</param>\n')

        # keywords
        kw = _build_keywords(vendor, it2.name, it2.vid, it2.params)
        out.append(f'<keywords>{_xml_escape(kw)}</keywords>\n')
        out.append("</offer>\n\n")

    out.append("</offers>\n</shop>\n</yml_catalog>\n")
    return "".join(out)


def main() -> int:
    url = os.environ.get("NVPRINT_XML_URL", "").strip()
    login = os.environ.get("NVPRINT_LOGIN", "").strip()
    password = os.environ.get("NVPRINT_PASSWORD", "").strip()
    out_file = os.environ.get("OUT_FILE", "docs/nvprint.yml").strip()
    timeout = int(os.environ.get("HTTP_TIMEOUT", "60").strip() or "60")

    if not url:
        print("ERROR: NVPRINT_XML_URL is not set", file=sys.stderr)
        return 2

    xml_bytes = _http_get_xml(url, login, password, timeout=timeout)
    root = ET.fromstring(xml_bytes)

    items: List[Item] = []
    for el in _iter_items(root):
        vid = _norm_spaces(_first_text_by_keys(el, ID_KEYS))
        name = _norm_spaces(_first_text_by_keys(el, NAME_KEYS))
        price_s = _first_text_by_keys(el, PRICE_KEYS)
        base = _to_float(price_s) or 0.0
        if not vid or not name or base <= 0:
            continue

        vendor_raw = _first_text_by_keys(el, VENDOR_KEYS)
        desc_raw = _first_text_by_keys(el, DESC_KEYS)
        params = _collect_params(el)

        pic = _parse_picture(el)
        available = _parse_available(el)

        items.append(Item(
            vid=vid,
            name=name,
            base_price=base,
            picture=pic,
            vendor_raw=vendor_raw,
            desc_raw=_norm_spaces(desc_raw),
            available=available,
            params=params,
        ))

    yml = build_yml(items, src_url=url, out_file=out_file)

    os.makedirs(os.path.dirname(out_file) or ".", exist_ok=True)
    with open(out_file, "wb") as f:
        f.write(yml.encode("cp1251", errors="xmlcharrefreplace"))

    print(f"Wrote: {out_file} | encoding=windows-1251 | items={len(items)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
