from __future__ import annotations

import os
import re
import sys
import math
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import xml.etree.ElementTree as ET

try:
    import requests  # type: ignore
except Exception:
    requests = None


SUPPLIER_NAME = "AkCent"
SUPPLIER_URL_DEFAULT = "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"
OUT_DEFAULT = "docs/akcent.yml"
CURRENCY_ID = "KZT"
OUTPUT_ENCODING = "utf-8"

SCHEDULE_HOUR_ALMATY = 2
ALMATY_UTC_OFFSET = 5  # Алматы: UTC+5 (без DST)

AKCENT_NAME_PREFIXES: List[str] = [
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
CITY_TAIL = "Казахстан, Алматы, Астана, Шымкент, Караганда, Актобе, Павлодар, Атырау, Тараз, Костанай, Кызылорда, Петропавловск, Талдыкорган, Актау"


AL_WA_BLOCK = (
    '<!-- WhatsApp -->\n'
    '<div style="font-family: Cambria, \'Times New Roman\', serif; line-height:1.5; color:#222; font-size:15px;">'
    '<p style="text-align:center; margin:0 0 12px;">'
    '<a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" '
    'style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; '
    'border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);">'
    '&#128172; Написать в WhatsApp</a></p></div>'
)

AL_HR_2PX = '<hr style="border:none; border-top:2px solid #E7D6B7; margin:12px 0;" />'

AL_PAY_BLOCK = (
    '<!-- Оплата и доставка -->\n'
    '<div style="font-family: Cambria, \'Times New Roman\', serif; line-height:1.5; color:#222; font-size:15px;">'
    '<div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;">'
    '<h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3>'
    '<ul style="margin:0; padding-left:18px;">'
    '<li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li>'
    '<li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li>'
    '</ul>'
    '<hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" />'
    '<h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3>'
    '<ul style="margin:0; padding-left:18px;">'
    '<li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li>'
    '<li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5 000 тг. | 3–7 рабочих дней</em></li>'
    '<li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>'
    '<li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li>'
    '</ul>'
    '</div></div>'
)

PARAM_PRIO = [
    "Производитель", "Бренд", "Вендор", "Vendor", "Brand",
    "Модель", "Артикул", "Код производителя", "Совместимость",
    "Тип", "Тип печати", "Цвет", "Ресурс", "Объем", "Ёмкость", "Гарантия", "Страна происхождения"
]
PARAM_PRIO_INDEX = {k.lower(): i for i, k in enumerate(PARAM_PRIO)}


PARAM_DROP_NAMES = {
    "Благотворительность",
    "Новинка",
    "Снижена цена",
    "Назначение",
    "Штрихкод",
    "Код товара Kaspi",
    "Код ТН ВЭД",
    "Объём",
}
PARAM_DROP_LC = {x.lower() for x in PARAM_DROP_NAMES}



# Получаем текущее время по часовому поясу Алматы
def _now_almaty() -> datetime:
    return datetime.utcnow().replace(microsecond=0) + timedelta(hours=ALMATY_UTC_OFFSET)


# Считаем время следующей плановой сборки
def _next_scheduled_run(build_time: datetime, hour: int) -> datetime:
    cand = build_time.replace(hour=hour, minute=0, second=0)
    if cand <= build_time:
        cand = cand + timedelta(days=1)
    return cand


# Получаем набор разрешённых categoryId с учётом ALSTYLE_CATEGORY_IDS

# Нормализуем пробелы и обрезаем строку
def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


# Преобразуем логическое значение в строку "true"/"false"
def _bool_str(v: bool) -> str:
    return "true" if v else "false"

# Экранируем спецсимволы в текстовом узле XML

# Проверяем, что название товара начинается с одного из заданных префиксов
def _passes_name_prefixes(name: str) -> bool:
    s = (name or "").lstrip()
    for pref in AKCENT_NAME_PREFIXES:
        if s.startswith(pref):
            return True
    return False

def _xml_escape_text(s: str) -> str:
    s = s.replace("&", "&amp;")
    s = s.replace("<", "&lt;").replace(">", "&gt;")
    return s


# Экранируем спецсимволы в значении XML-атрибута
def _xml_escape_attr(s: str) -> str:
    s = _xml_escape_text(s)
    s = s.replace('"', "&quot;")
    return s



# Парсим строку в булево значение
def _parse_bool(s: Optional[str]) -> bool:
    if s is None:
        return False
    t = s.strip().lower()
    return t in {"1", "true", "yes", "y", "да"}


# Безопасно парсим целое число из строки
def _safe_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    t = re.sub(r"[^\d]", "", s)
    if not t:
        return None
    try:
        return int(t)
    except Exception:
        return None

# Безопасно достаём текст из XML-элемента
def _get_text(node: Optional[ET.Element]) -> str:
    if node is None:
        return ""
    return (node.text or "").strip()




# Применяем ценовой хвост 900 к значению
def _tail_900(price: int) -> int:
    if price <= 100:
        return 100
    v = int(math.ceil(price / 1000.0) * 1000 - 100)
    return max(100, v)


# Считаем розничную цену по правилам наценки
def _apply_price_rule(supplier_price: Optional[int]) -> int:
    if supplier_price is None or supplier_price <= 100:
        return 100
    if supplier_price >= 9_000_000:
        return 100

    p = float(supplier_price) * 1.04

    tiers = [
        (101, 10_000, 3_000),
        (10_001, 25_000, 4_000),
        (25_001, 50_000, 5_000),
        (50_001, 75_000, 7_000),
        (75_001, 100_000, 10_000),
        (100_001, 150_000, 12_000),
        (150_001, 200_000, 15_000),
        (200_001, 300_000, 20_000),
        (300_001, 400_000, 25_000),
        (400_001, 500_000, 30_000),
        (500_001, 750_000, 40_000),
        (750_001, 1_000_000, 60_000),
        (1_000_001, 1_500_000, 80_000),
        (1_500_001, 2_000_000, 100_000),
        (2_000_001, 3_000_000, 150_000),
        (3_000_001, 4_000_000, 200_000),
        (4_000_001, 9_000_000, 250_000),
    ]

    add = 0
    for lo, hi, a in tiers:
        if lo <= supplier_price <= hi:
            add = a
            break

    out = int(round(p + add))
    out = _tail_900(out)

    if out >= 9_000_000:
        return 100
    return out



# Сортируем характеристики по приоритету и алфавиту
def _sort_params(items: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    def sk(it: Tuple[str, str]) -> Tuple[int, str]:
        k = it[0].strip()
        return (PARAM_PRIO_INDEX.get(k.lower(), 10_000), k.lower())
    return sorted(items, key=sk)


# Делаем мелкие правки текста без изменения смысла
def _fix_text_common(s: str) -> str:
    s = s or ""
    s = s.replace("Shuko", "Schuko")
    s = s.replace("Cтоечные", "Стоечные").replace("Cтоечный", "Стоечный")
    s = s.replace("Линейно-Интерактивный", "Линейно-интерактивный")
    s = s.replace("высококачетсвенную", "высококачественную")
    s = s.replace("приентеров", "принтеров")
    s = re.sub(r"\b(\d+)\s*-\s*х\b", r"\1-х", s)
    return s


_RUS2LAT = {
    "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m",
    "н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch",
    "ъ":"","ы":"y","ь":"","э":"e","ю":"yu","я":"ya"
}


# Строим slug на латинице из строки
def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            continue
        if "а" <= ch <= "я" or ch == "ё":
            out.append(_RUS2LAT.get(ch, ""))
        else:
            out.append("-")
    x = "".join(out)
    x = re.sub(r"[^a-z0-9\-]+", "-", x)
    x = re.sub(r"-{2,}", "-", x).strip("-")
    return x

# Пытаемся угадать бренд по названию товара
def _guess_vendor_from_name(name: str) -> str:
    s = _norm_spaces(name)
    if not s:
        return ""
    generic = {
        "картридж",
        "принтер",
        "мфу",
        "монитор",
        "чернила",
        "экран",
        "плоттер",
        "плоттеры",
        "проектор",
        "сканер",
        "ламинатор",
        "шредер",
        "ёмкость",
        "емкость",
        "набор",
        "экономичный",
        "панель",
        "доска",
        "дисплей",
        "интерактивная",
        "интерактивный",
        "интерактивное",
    }
    brands = {
        "Epson",
        "HP",
        "Canon",
        "Brother",
        "Kyocera",
        "Ricoh",
        "Xerox",
        "Samsung",
        "OKI",
        "NEC",
        "Panasonic",
        "Lexmark",
        "Konica",
        "Konica Minolta",
        "Sharp",
        "Dell",
        "Acer",
        "Asus",
        "BenQ",
        "ViewSonic",
        "LG",
    }
    tokens = re.split(r"[\s,/()\[\]\"'«»“”]+", s)
    for t in tokens:
        t = t.strip()
        if not t:
            continue
        if t in brands:
            return t
        if any(ch.isdigit() for ch in t):
            # Похожие на артикул куски пропускаем
            continue
        t_low = t.lower()
        if t_low in generic:
            continue
        if re.match(r"^[A-Z][a-z]{2,}$", t):
            return t
        if re.match(r"^[А-ЯЁ][а-яё]{2,}$", t):
            return t
    return ""


# Собираем SEO-ключевые слова для товара
def _build_keywords(vendor: str, name: str) -> str:
    vendor = _norm_spaces(vendor)
    name = _norm_spaces(name)

    parts: List[str] = []
    if vendor:
        parts.append(vendor)
    if name:
        parts.append(name)

    toks = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", name)
    toks = [t for t in toks if len(t) >= 2]
    for t in toks[:18]:
        parts.append(t)

    if vendor and name:
        m = re.search(r"\b[A-Za-zА-Яа-яЁё]*\d[\w\-]*\b", name)
        if m:
            parts.append(f"{vendor} {m.group(0)}")

    base_slug = _slugify(name)
    if base_slug:
        parts.append(base_slug)
        if vendor:
            parts.append(f"{base_slug}-{_slugify(vendor)}".strip("-"))

    drop = {x.strip() for x in CITY_TAIL.split(",")}
    seen = set()
    cleaned: List[str] = []
    for p in parts:
        p = _norm_spaces(p).strip(" ,")
        if not p or p in drop:
            continue
        k = p.lower()
        if k in seen:
            continue
        seen.add(k)
        cleaned.append(p)

    return ", ".join(cleaned + [CITY_TAIL])



# Превращаем родное описание в HTML-блок с <p> и <br>
def _native_desc_to_p(desc: str, name: str) -> str:
    d = (desc or "").strip()
    d = _fix_text_common(d)
    if not d:
        return f"<p>{name}</p>"
    if re.search(r"<\s*p\b", d, flags=re.I):
        return d
    d = d.replace("\r\n", "\n").replace("\r", "\n")
    d = re.sub(r"\n{2,}", "\n", d).strip()
    if "\n" in d:
        d = "<br>".join([_norm_spaces(x) for x in d.split("\n") if _norm_spaces(x)])
    return f"<p>{d}</p>"


# Собираем HTML-блок списка характеристик
def _build_chars_block(params: List[Tuple[str, str]]) -> str:
    if not params:
        return "<h3>Характеристики</h3><ul><li><strong>Гарантия:</strong> 0</li></ul>"

    items = []
    for k, v in _sort_params(params):
        k2 = _norm_spaces(k)
        v2 = _norm_spaces(v)
        if not k2 or not v2:
            continue
        items.append(f"<li><strong>{k2}:</strong> {v2}</li>")

    if not items:
        items = ["<li><strong>Гарантия:</strong> 0</li>"]
    return "<h3>Характеристики</h3><ul>" + "".join(items) + "</ul>"


# Строим финальное описание с тройным обогащением
def _build_description(name: str, native_desc: str, params: List[Tuple[str, str]]) -> str:

    name = _norm_spaces(name)
    desc = native_desc or ""

    extra_pairs: List[Tuple[str, str]] = []

    if desc and not re.search(r"<\s*p\b", desc, flags=re.I):
        tmp = desc.replace("\r\n", "\n").replace("\r", "\n")
        tmp = re.sub(r"\n{2,}", "\n", tmp)
        raw_lines = tmp.split("\n")

        cleaned_lines: List[str] = []
        heading_keys = {
            "характеристики",
            "основные характеристики",
            "основные характеристики и преимущества",
            "особенности",
            "особенности и преимущества",
            "преимущества",
            "условия гарантии",
            "примечание",
            "внимание",
        }

        for raw_line in raw_lines:
            line = raw_line.strip()
            if not line:
                cleaned_lines.append(raw_line)
                continue

            cand = re.sub(r"^[\-\•\*\u2013\u2014]\s*", "", line)

            if ":" in cand:
                key_part, val_part = cand.split(":", 1)
                key = key_part.strip()
                val = val_part.strip()
                lk = key.lower()

                if not val and lk in heading_keys:
                    continue

                if key:
                    word_count = len(re.split(r"\s+", key))
                    if word_count > 2:
                        cleaned_lines.append(raw_line)
                        continue

                if key and val and (lk not in PARAM_DROP_LC) and (lk not in heading_keys):
                    extra_pairs.append((key, val))
                    continue

            cleaned_lines.append(raw_line)

        cleaned_text = "\n".join(cleaned_lines).strip()
    else:
        cleaned_text = desc

    if extra_pairs:
        combined: List[Tuple[str, str]] = []
        seen = set()

        for k, v in params:
            k2 = _norm_spaces(k)
            v2 = _norm_spaces(v)
            if not k2 or not v2:
                continue
            lk = k2.lower()
            if lk in PARAM_DROP_LC:
                continue
            if lk in seen:
                continue
            seen.add(lk)
            combined.append((k2, v2))

        for key, val in extra_pairs:
            k2 = _norm_spaces(key)
            v2 = _norm_spaces(val)
            if not k2 or not v2:
                continue
            lk = k2.lower()
            if lk in PARAM_DROP_LC or lk in seen:
                continue
            seen.add(lk)
            combined.append((k2, v2))

        params.clear()
        params.extend(combined)

    native_html = _native_desc_to_p(cleaned_text, name)
    chars = _build_chars_block(params)

    cdata = (
        "\n"
        + AL_WA_BLOCK
        + "\n" + AL_HR_2PX
        + "\n<!-- Описание -->\n"
        + f"<h3>{name}</h3>"
        + native_html
        + "\n" + chars
        + "\n" + AL_PAY_BLOCK
        + "\n"
    )
    return f"<description><![CDATA[{cdata}]]></description>"



@dataclass
class OfferOut:
    oid: str
    available: bool
    name: str
    price: int
    vendor: str
    pictures: List[str]
    params: List[Tuple[str, str]]
    native_desc: str

    def to_xml(self) -> str:
        lines: List[str] = []
        lines.append(f'<offer id="{_xml_escape_attr(self.oid)}" available="{_bool_str(self.available)}">')
        lines.append("<categoryId></categoryId>")
        lines.append(f"<vendorCode>{_xml_escape_text(self.oid)}</vendorCode>")
        lines.append(f"<name>{_xml_escape_text(self.name)}</name>")
        lines.append(f"<price>{self.price}</price>")
        for pic in self.pictures:
            lines.append(f"<picture>{_xml_escape_text(pic)}</picture>")
        if self.vendor:
            lines.append(f"<vendor>{_xml_escape_text(self.vendor)}</vendor>")
        lines.append(f"<currencyId>{_xml_escape_text(CURRENCY_ID)}</currencyId>")
        lines.append(_build_description(self.name, self.native_desc, self.params))
        for k, v in _sort_params(self.params):
            k2 = _norm_spaces(k)
            v2 = _norm_spaces(v)
            if not k2 or not v2:
                continue
            lines.append(f'<param name="{_xml_escape_attr(k2)}">{_xml_escape_text(v2)}</param>')
        kw = _build_keywords(self.vendor, self.name)
        lines.append(f"<keywords>{_xml_escape_text(kw)}</keywords>")
        lines.append("</offer>")
        return "\n".join(lines)





def _collect_params(offer: ET.Element) -> List[Tuple[str, str]]:
    params: List[Tuple[str, str]] = []
    seen = set()
    for p in offer.findall("param"):
        k = (p.get("name") or "").strip()
        v = (p.text or "").strip()
        if not k:
            continue
        lk = k.lower()
        if lk in PARAM_DROP_LC:
            continue
        if lk in seen:
            continue
        seen.add(lk)
        if v:
            params.append((k, _fix_text_common(v)))
    return params


# Собираем список картинок для исходного оффера
def _collect_pictures(offer: ET.Element) -> List[str]:
    pics = []
    for p in offer.findall("picture"):
        u = _norm_spaces(_get_text(p))
        if u:
            pics.append(u)
    out = []
    seen = set()
    for u in pics:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out



# Берём родное описание из исходного XML-оффера
def _pick_native_desc(offer: ET.Element) -> str:
    d = offer.find("description")
    if d is not None:
        return (d.text or "").strip()
    return ""


# Нормализуем структуру оффера AkCent под общий шаблон
def _normalize_akcent_offer(offer: ET.Element) -> None:
    # Переносим характеристики из <Param> в <param>
    for p in list(offer.findall("Param")):
        name_attr = (p.get("name") or "").strip()
        new = ET.Element("param")
        if name_attr:
            new.set("name", name_attr)
        new.text = (p.text or "").strip()
        offer.append(new)
        offer.remove(p)

    # Переносим цену из блока <prices> в purchase_price/price
    prices = offer.find("prices")
    dealer_val = ""
    if prices is not None:
        for price_node in prices.findall("price"):
            ptype = (price_node.get("type") or "").strip()
            if ptype == "Цена дилерского портала KZT":
                dealer_val = (price_node.text or "").strip()
                break
        if not dealer_val:
            first = prices.find("price")
            if first is not None:
                dealer_val = (first.text or "").strip()
    if dealer_val:
        for tag in ("purchase_price", "price"):
            old = offer.find(tag)
            if old is not None:
                offer.remove(old)
        pp_el = ET.Element("purchase_price")
        pp_el.text = dealer_val
        offer.append(pp_el)
        price_el = ET.Element("price")
        price_el.text = dealer_val
        offer.append(price_el)

# Собираем структуру OfferOut для одного товара
def _build_offer_out(offer: ET.Element) -> OfferOut:
    article = _norm_spaces(offer.get("article") or "")
    offer_id = _norm_spaces(_get_text(offer.find("Offer_ID")))
    raw_id = article or offer_id or _norm_spaces(offer.get("id") or "")

    if not raw_id:
        raw_id = hashlib.md5(_get_text(offer.find("name")).encode("utf-8", errors="ignore")).hexdigest()[:10]

    oid = raw_id if raw_id.upper().startswith("AC") else f"AC{raw_id}"

    name = _fix_text_common(_norm_spaces(_get_text(offer.find("name"))))
    vendor = _fix_text_common(_norm_spaces(_get_text(offer.find("vendor"))))
    if not vendor:
        for p in offer.findall("param"):
            pname = (p.get("name") or "").strip().lower()
            if (
                "производитель" in pname
                or "бренд" in pname
                or pname in ("brand", "vendor", "manufacturer")
            ):
                vendor = _fix_text_common(_norm_spaces(p.text or ""))
                if vendor:
                    break
    if not vendor:
        vendor = _fix_text_common(_guess_vendor_from_name(name))

    av_attr = offer.get("available")
    av_tag = _get_text(offer.find("available"))
    available = _parse_bool(av_attr) or _parse_bool(av_tag)

    pics = _collect_pictures(offer)
    params = _collect_params(offer)
    native_desc = _pick_native_desc(offer)

    src_price = _safe_int(_get_text(offer.find("purchase_price"))) or _safe_int(_get_text(offer.find("price")))
    out_price = _apply_price_rule(src_price)

    return OfferOut(
        oid=oid,
        available=available,
        name=name,
        price=out_price,
        vendor=vendor,
        pictures=pics,
        params=params,
        native_desc=native_desc,
    )
def _extract_offers(root: ET.Element) -> List[ET.Element]:
    offers_node = root.find(".//offers")
    if offers_node is None:
        return []
    return list(offers_node.findall("offer"))


def _make_feed_meta(
    build_time: datetime,
    next_run: datetime,
    cnt_before: int,
    cnt_after: int,
    cnt_true: int,
    cnt_false: int,
    supplier_url: str,
) -> str:
    def row(label: str, value: str) -> str:
        return f"{label:<42} | {value}"

    lines = []
    lines.append("<!--FEED_META")
    lines.append(row("Поставщик", SUPPLIER_NAME))
    lines.append(row("URL поставщика", supplier_url))
    lines.append(row("Время сборки (Алматы)", build_time.strftime("%Y-%m-%d %H:%M:%S")))
    lines.append(row("Ближайшая сборка (Алматы)", next_run.strftime("%Y-%m-%d %H:%M:%S")))
    lines.append(row("Сколько товаров у поставщика до фильтра", str(cnt_before)))
    lines.append(row("Сколько товаров у поставщика после фильтра", str(cnt_after)))
    lines.append(row("Сколько товаров есть в наличии (true)", str(cnt_true)))
    lines.append(row("Сколько товаров нет в наличии (false)", str(cnt_false)))
    lines.append("-->")
    return "\n".join(lines)


def _ensure_footer_spacing(s: str) -> str:
    s = re.sub(r"(<shop><offers>\n)(\n*)", r"\1\n", s, count=1)
    s = re.sub(r"(</offer>\n)(</offers>)", r"\1\n\2", s, count=1)
    s = s.rstrip() + "\n"
    return s


# Атомарно записываем файл только если содержимое изменилось
def _atomic_write_if_changed(path: str, data: str, encoding: str = OUTPUT_ENCODING) -> bool:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    new_bytes = data.encode(encoding, errors="strict")

    old_bytes = b""
    if p.exists():
        try:
            old_bytes = p.read_bytes()
        except Exception:
            old_bytes = b""
    if old_bytes == new_bytes:
        return False

    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_bytes(new_bytes)
    tmp.replace(p)
    return True


def _fetch(url: str) -> bytes:
    if requests is None:
        raise RuntimeError("requests не установлен. В GitHub Actions он обычно есть.")
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    return r.content


# Проверяем, нужно ли запускаться в текущем контексте (schedule/ручной запуск)
def _should_run_now(build_time: datetime) -> bool:
    if os.getenv("FORCE_YML_REFRESH", "").strip().lower() in {"1", "true", "yes"}:
        return True
    ev = (os.getenv("GITHUB_EVENT_NAME") or "").strip().lower()
    if ev == "schedule":
        return build_time.hour == SCHEDULE_HOUR_ALMATY
    return True


# Точка входа: собираем фид AkCent и пишем YML-файл
def main() -> int:
    url = os.getenv("AKCENT_URL", SUPPLIER_URL_DEFAULT).strip() or SUPPLIER_URL_DEFAULT
    out_path = os.getenv("OUT", OUT_DEFAULT).strip() or OUT_DEFAULT

    build_time = _now_almaty()
    if not _should_run_now(build_time):
        print(f"[akcent] skip: event=schedule, now={build_time}; ждём ежедневный запуск в час {SCHEDULE_HOUR_ALMATY}:00.")
        return 0

    print(f"[akcent] Скачиваем фид: {url}")

    raw = _fetch(url)
    root = ET.fromstring(raw)

    in_offers = _extract_offers(root)
    cnt_before = len(in_offers)

    out_offers: List[OfferOut] = []
    for o in in_offers:
        name_val = _get_text(o.find("name"))
        if not _passes_name_prefixes(name_val):
            continue
        _normalize_akcent_offer(o)
        out_offers.append(_build_offer_out(o))

    cnt_after = len(out_offers)
    cnt_true = sum(1 for x in out_offers if x.available)
    cnt_false = cnt_after - cnt_true

    next_run = _next_scheduled_run(build_time, SCHEDULE_HOUR_ALMATY)

    feed_meta = _make_feed_meta(
        build_time=build_time,
        next_run=next_run,
        cnt_before=cnt_before,
        cnt_after=cnt_after,
        cnt_true=cnt_true,
        cnt_false=cnt_false,
        supplier_url=url,
    )

    header = [
        f'<?xml version="1.0" encoding="{OUTPUT_ENCODING}"?>',
        f'<yml_catalog date="{build_time.strftime("%Y-%m-%d %H:%M")}">',
        "<shop><offers>",
        "",
        feed_meta,
        "",
    ]

    body_lines: List[str] = []
    for off in out_offers:
        body_lines.append(off.to_xml())
        body_lines.append("")  # пустая строка между офферами

    footer = ["</offers>", "</shop>", "</yml_catalog>"]

    out = "\n".join(header + body_lines + footer)
    out = _ensure_footer_spacing(out)

    changed = _atomic_write_if_changed(out_path, out, encoding=OUTPUT_ENCODING)
    print(f"[akcent] Найдено офферов у поставщика: {cnt_before}")
    print(f"[akcent] В фид попало офферов: {cnt_after}")
    print(f"[akcent] В наличии true: {cnt_true}; false: {cnt_false}")
    print(f"[akcent] Записано: {out_path}; changed={'yes' if changed else 'no'}")
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
