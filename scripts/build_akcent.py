from __future__ import annotations

import html
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))

SUPPLIER_URL = os.getenv(
    "AKCENT_URL",
    os.getenv("AKCENT_SOURCE_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"),
)
OUTPUT_PATH = os.getenv("OUT_FILE", os.getenv("AKCENT_OUTPUT_PATH", "docs/akcent.yml"))
_ALLOWED_PREFIXES = [
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

PARAM_TITLE_MAP: dict[str, str] = {
    "Разрешение печати,dpi": "Разрешение печати, dpi",
    "Разрешение сканера,dpi": "Разрешение сканера, dpi",
    "Уровень шума (норм./эконом.) Дб": "Уровень шума (норм./эконом.), дБ",
    "Яркость (ANSI) лмн": "Яркость (ANSI), лм",
    "Проекционный коэффицент (Throw ratio)": "Проекционный коэффициент (throw ratio)",
}

_BRAND_BLOCKLIST = (
    "alstyle",
    "al-style",
    "copyline",
    "vtt",
    "akcent",
    "ak-cent",
    "китай",
)

_KNOWN_BRANDS = (
    "Epson",
    "Fellowes",
    "HyperX",
    "Mr.Pixel",
    "Philips",
    "SBID",
    "Smart",
    "ViewSonic",
    "Vivitek",
    "Zebra",
)

_CITY_KEYWORDS = [
    "Казахстан",
    "Алматы",
    "Астана",
    "Шымкент",
    "Караганда",
    "Актобе",
    "Павлодар",
    "Атырау",
    "Тараз",
    "Оскемен",
    "Семей",
    "Костанай",
    "Кызылорда",
    "Орал",
    "Петропавловск",
    "Талдыкорган",
    "Актау",
    "Темиртау",
    "Экибастуз",
    "Кокшетау",
]


def _translit_to_slug(text: str) -> str:
    mapping = {
        "а": "a",
        "б": "b",
        "в": "v",
        "г": "g",
        "д": "d",
        "е": "e",
        "ё": "e",
        "ж": "zh",
        "з": "z",
        "и": "i",
        "й": "y",
        "к": "k",
        "л": "l",
        "м": "m",
        "н": "n",
        "о": "o",
        "п": "p",
        "р": "r",
        "с": "s",
        "т": "t",
        "у": "u",
        "ф": "f",
        "х": "h",
        "ц": "c",
        "ч": "ch",
        "ш": "sh",
        "щ": "sch",
        "ъ": "",
        "ы": "y",
        "ь": "",
        "э": "e",
        "ю": "yu",
        "я": "ya",
    }
    text = (text or "").lower()
    res: list[str] = []
    prev_dash = False
    for ch in text:
        if ch in mapping:
            res.append(mapping[ch])
            prev_dash = False
        elif ch.isalnum():
            res.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                res.append("-")
                prev_dash = True
    slug = "".join(res).strip("-")
    return slug


def _make_keywords(name: str, vendor: str) -> str:
    parts: list[str] = []
    seen: set[str] = set()

    def add(token: str) -> None:
        token = (token or "").strip()
        if not token:
            return
        if token in seen:
            return
        seen.add(token)
        parts.append(token)

    name = (name or "").strip()
    vendor = (vendor or "").strip()

    if vendor:
        add(vendor)

    if name:
        add(name)

    tokens = re.split(r"[\s,;:!\?()\[\]\"/\+]+", name)
    words = [t for t in tokens if t and len(t) >= 3]

    for w in words:
        add(w)

    model = None
    for t in reversed(tokens):
        if any(ch.isdigit() for ch in t):
            model = t.strip()
            break

    if vendor and model:
        add(model)
        add(f"{vendor} {model}")

    base_words = [w for w in words if not re.fullmatch(r"\d+[%]?", w)]
    if base_words:
        phrase2 = " ".join(base_words[:2])
        phrase3 = " ".join(base_words[:3])
        add(_translit_to_slug(phrase2))
        add(_translit_to_slug(phrase3))
        for w in base_words[:3]:
            add(_translit_to_slug(w))

    if vendor and model:
        add(_translit_to_slug(f"{vendor} {model}"))

    for city in _CITY_KEYWORDS:
        add(city)

    if not parts:
        return ""

    result = ", ".join(parts)
    if len(result) > 2000:
        out: list[str] = []
        length = 0
        for p in parts:
            add_len = len(p) + 2 if out else len(p)
            if length + add_len > 2000:
                break
            out.append(p)
            length += add_len
        result = ", ".join(out)

    return result

WHATSAPP_BLOCK = """<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;"><p style="text-align:center; margin:0 0 12px;"><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);">&#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!</a></p><div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;"><h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3><ul style="margin:0; padding-left:18px;"><li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li><li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li></ul><hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" /><h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3><ul style="margin:0; padding-left:18px;"><li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li><li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li><li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li><li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li></ul></div></div>"""
@dataclass
class OfferData:
    id: str
    available: str
    category_id: str
    vendor_code: str
    name: str
    price: int
    pictures: list[str]
    vendor: str
    description_html: str
    params: list[tuple[str, str]]

# Делает:  decode bytes
def _decode_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")

# Делает:  name allowed
def _name_allowed(name: str) -> bool:
    n = name.strip()
    for prefix in _ALLOWED_PREFIXES:
        if n.startswith(prefix):
            return True
    return False

# Делает: нормализует значения
def _normalize_brand_name(raw: str) -> str:
    t = raw.strip()
    if not t:
        return ""

    low = t.lower()
    for bad in _BRAND_BLOCKLIST:
        if low == bad:
            return ""

    t = re.sub(r"\s*proj$", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*projector$", "", t, flags=re.IGNORECASE)
    return t.strip()

# Делает: считает цену
def _apply_price_rules(raw_price: int) -> int:
    base = int(raw_price)
    if base <= 0:
        return base

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
        (750_001, 1_000_000, 50_000),
        (1_000_001, 1_500_000, 70_000),
        (1_500_001, 2_000_000, 90_000),
        (2_000_001, 100_000_000, 100_000),
    ]

    bonus = 0
    for lo, hi, add in tiers:
        if lo <= base <= hi:
            bonus = add
            break

    if bonus == 0:
        return base

    value = base * 1.04 + bonus

    thousands = int(value) // 1000
    price = thousands * 1000 + 900
    if price < value:
        price += 1000

    if price >= 9_000_000:
        return 100

    return price

# Делает: извлекает нужные поля
def _extract_params(block: str) -> tuple[list[tuple[str, str]], list[str]]:
    params: list[tuple[str, str]] = []
    compat: list[str] = []

    for m in re.finditer(r'<Param\s+name="([^"]*)">(.*?)</Param>', block, flags=re.DOTALL | re.IGNORECASE):
        name = html.unescape(m.group(1) or "").strip()
        value = html.unescape(m.group(2) or "").strip()

        if not name:
            continue

        if name == "Сопутствующие товары":
            v = value.strip()
            if v and v.lower() not in {"нет", "none", "n/a"}:
                compat.append(v)
            continue

        if name in {
            "Наименование производителя",
            "Совместимые продукты",
            "Оригинальное разрешение",
        }:
            continue

        if name == "Производитель":
            norm_val = _normalize_brand_name(value)
            if not norm_val:
                continue
            value = norm_val

        if name in {"Тип", "Вид", "Для бренда"}:
            continue

        norm_name = PARAM_TITLE_MAP.get(name, name)
        params.append((norm_name, value))

    return params, compat

GOAL = 1000
GOAL_LOW = 900
MAX_HARD = 1200

# Делает: собирает YML текст
def _build_desc_text(plain: str) -> str:
    if len(plain) <= GOAL:
        return plain

    parts = re.split(r"(?<=[\.!?])\s+|;\s+", plain)
    parts = [p.strip() for p in parts if p.strip()]
    if not parts:
        return plain[:GOAL]

    selected: List[str] = []
    total = 0

    selected.append(parts[0])
    total = len(parts[0])

    for p in parts[1:]:
        add = (1 if total else 0) + len(p)
        if total + add > MAX_HARD:
            break
        selected.append(p)
        total += add
        if total >= GOAL_LOW:
            break

    if total < GOAL_LOW:
        for p in parts[len(selected):]:
            add = (1 if total else 0) + len(p)
            if total + add > MAX_HARD:
                break
            selected.append(p)
            total += add
            if total >= GOAL_LOW:
                break

    return " ".join(selected).strip()

# Делает: собирает YML текст
def _build_description(name: str, raw_desc: str, params: list[tuple[str, str]], compat: list[str]) -> str:
    name_html = html.escape(name.strip())
    desc_text = (raw_desc or "").strip()
    desc_text = html.unescape(desc_text)
    desc_text = re.sub(r"\s+", " ", desc_text)

    plain_name = name.strip()

    if not desc_text:
        desc_text = plain_name

    desc_text = _build_desc_text(desc_text)

    inner: list[str] = []

    inner.append("<!-- WhatsApp -->")
    inner.append(WHATSAPP_BLOCK)
    inner.append("")  # пустая строка даёт двойной перенос перед <!-- Описание -->

    inner.append("<!-- Описание -->")
    inner.append(f"<h3>{name_html}</h3><p>{html.escape(desc_text)}</p>")

    if params:
        li: list[str] = []
        for pname, pvalue in params:
            if not pvalue.strip():
                continue
            li.append(f"<li><strong>{html.escape(pname)}:</strong> {html.escape(pvalue)}</li>")
        if li:
            inner.append("<h3>Характеристики</h3><ul>" + "".join(li) + "</ul>")

    if compat:
        li2 = [f"<li>{html.escape(v)}</li>" for v in compat[:10]]
        if li2:
            inner.append("<h3>Совместимые устройства</h3><ul>" + "".join(li2) + "</ul>")

    html_block = "\n".join(inner)
    return html_block

# Делает:  guess brand
def _guess_brand(name: str, raw_desc: str, body: str) -> str:
    for pattern in (
        r'<Param\s+name="Производитель">(.*?)</Param>',
        r'<Param\s+name="Наименование производителя">(.*?)</Param>',
        r'<Param\s+name="Для бренда">(.*?)</Param>',
    ):
        m = re.search(pattern, body, flags=re.DOTALL | re.IGNORECASE)
        if m:
            brand = _normalize_brand_name(m.group(1))
            if brand:
                return brand

    text = f"{name} {html.unescape(raw_desc or '')}"
    low = text.lower()
    for b in _KNOWN_BRANDS:
        if b.lower() in low:
            return b

    if "SBID-" in text:
        return "SBID"

    return ""

# Делает: извлекает нужные поля
def _parse_offer(block: str) -> OfferData | None:
    m_head = re.match(r"<offer\b([^>]*)>(.*)</offer>", block, flags=re.DOTALL | re.IGNORECASE)
    if not m_head:
        return None

    header_attrs = m_head.group(1)
    body = m_head.group(2)

    m_name = re.search(r"<name>(.*?)</name>", body, flags=re.DOTALL | re.IGNORECASE)
    name = html.unescape(m_name.group(1).strip()) if m_name else ""
    if not name:
        return None

    if not _name_allowed(name):
        return None

    m_article = re.search(r'\barticle="([^"]*)"', header_attrs)
    article = (m_article.group(1).strip() if m_article else "")

    if not article:
        m_old_id = re.search(r'\bid="([^"]*)"', header_attrs)
        if m_old_id:
            article = m_old_id.group(1).strip()

    if not article:
        return None

    new_id = "AK" + article

    m_av = re.search(r'\bavailable="([^"]*)"', header_attrs)
    available = (m_av.group(1).strip().lower() if m_av else "true")
    available = "true" if available in {"true", "1", "yes"} else "false"

    m_cat = re.search(r"<categoryId[^>]*>(.*?)</categoryId>", body, flags=re.DOTALL | re.IGNORECASE)
    cat_id = html.unescape(m_cat.group(1).strip()) if m_cat else ""

    m_desc = re.search(r"<description>(.*?)</description>", body, flags=re.DOTALL | re.IGNORECASE)
    raw_desc = html.unescape(m_desc.group(1)) if m_desc else ""

    m_vendor = re.search(r"<vendor>(.*?)</vendor>", body, flags=re.DOTALL | re.IGNORECASE)
    vendor = html.unescape(m_vendor.group(1).strip()) if m_vendor else ""
    vendor = _normalize_brand_name(vendor)
    if not vendor:
        vendor = _guess_brand(name, raw_desc, body)

    pictures: list[str] = []
    for m in re.finditer(r"<picture>(.*?)</picture>", body, flags=re.DOTALL | re.IGNORECASE):
        url = html.unescape(m.group(1).strip())
        if url:
            pictures.append(url)

    raw_price_val = None
    m_price = re.search(
        r'<price[^>]*type="Цена дилерского портала KZT"[^>]*>(.*?)</price>',
        body,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if m_price:
        value = re.sub(r"\s", "", m_price.group(1))
        if value.isdigit():
            raw_price_val = int(value)

    if raw_price_val is None or raw_price_val <= 0:
        return None

    price = _apply_price_rules(raw_price_val)

    params, compat = _extract_params(body)

    desc_html = _build_description(name, raw_desc, params, compat)

    return OfferData(
        id=new_id,
        available=available,
        category_id=cat_id,
        vendor_code=new_id,
        name=name,
        price=price,
        pictures=pictures,
        vendor=vendor,
        description_html=desc_html,
        params=params,
    )

# Делает: скачивает исходные данные
def _download_raw_text() -> str:
    print(f"[akcent] Скачиваем фид: {SUPPLIER_URL}")
    resp = requests.get(SUPPLIER_URL, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    text = _decode_bytes(resp.content)
    return text

# Делает: экранирует текст для XML
def _escape_text(text: str) -> str:
    return html.escape(text or "", quote=False)

# Делает: собирает YML текст
def _build_yml(offers: list[OfferData], total_raw: int) -> str:
    tz_almaty = timezone(timedelta(hours=5))
    now = datetime.now(tz=tz_almaty)
    today_str = now.strftime("%Y-%m-%d %H:%M")
    meta_now = now.strftime("%Y-%m-%d %H:%M:%S")

    next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
    if next_run <= now:
        next_run = next_run + timedelta(days=1)
    meta_next = next_run.strftime("%Y-%m-%d %H:%M:%S")
    total_filtered = len(offers)
    avail_true = sum(1 for o in offers if o.available == "true")
    avail_false = total_filtered - avail_true

    header_lines = [
        '<?xml version="1.0" encoding="windows-1251"?>',
        f'<yml_catalog date="{today_str}">',
        "<shop><offers>",
        "",
        "<!--FEED_META",
        "Поставщик                                  | AkCent",
        f"URL поставщика                             | {SUPPLIER_URL}",
        f"Время сборки (Алматы)                      | {meta_now}",
        f"Ближайшая сборка (Алматы)                  | {meta_next}",
        f"Сколько товаров у поставщика до фильтра    | {total_raw}",
        f"Сколько товаров у поставщика после фильтра | {total_filtered}",
        f"Сколько товаров есть в наличии (true)      | {avail_true}",
        f"Сколько товаров нет в наличии (false)      | {avail_false}",
        "-->",
        "",
    ]

    parts: list[str] = []

    for off in offers:
        lines: list[str] = []
        lines.append(f'<offer id="{off.id}" available="{off.available}">')
        lines.append("<categoryId></categoryId>")
        lines.append(f"<vendorCode>{html.escape(off.vendor_code)}</vendorCode>")
        lines.append(f"<name>{_escape_text(off.name)}</name>")
        lines.append(f"<price>{off.price}</price>")
        for pic in off.pictures:
            lines.append(f"<picture>{html.escape(pic)}</picture>")
        if off.vendor:
            lines.append(f"<vendor>{html.escape(off.vendor)}</vendor>")
        lines.append("<currencyId>KZT</currencyId>")
        lines.append("<description><![CDATA[")
        lines.append("")
        lines.append(off.description_html)
        lines.append("")
        lines.append("]]></description>")
        for pname, pvalue in off.params:
            lines.append(f'<param name="{html.escape(pname)}">{html.escape(pvalue)}</param>')
        kw = _make_keywords(off.name, off.vendor)
        if kw:
            lines.append(f"<keywords>{_escape_text(kw)}</keywords>")
        lines.append("</offer>")
        parts.append("\n".join(lines))

    body = "\n\n".join(parts)

    footer_lines = [
        "",
        "</offers>",
        "</shop>",
        "</yml_catalog>",
    ]

    full = "\n".join(header_lines) + "\n" + body + "\n" + "\n".join(footer_lines)
    return full

# Делает: собирает итоговый YML
def build_akcent_yml(output_path: str | Path = OUTPUT_PATH) -> None:
    raw_text = _download_raw_text()

    blocks = re.findall(r"<offer\b[^>]*>.*?</offer>", raw_text, flags=re.DOTALL | re.IGNORECASE)
    total_raw = len(blocks)
    print(f"[akcent] Найдено офферов у поставщика: {total_raw}")

    offers: list[OfferData] = []

    for block in blocks:
        data = _parse_offer(block)
        if data is None:
            continue
        offers.append(data)

    print(f"[akcent] В фид попало офферов: {len(offers)}")

    yml_text = _build_yml(offers, total_raw)

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_bytes = yml_text.encode("cp1251", errors="ignore")
    out_path.write_bytes(out_bytes)
    print(f"[akcent] Готовый YML сохранён в {out_path}")

# Делает: точка входа
def main(argv: list[str] | None = None) -> int:
    _ = argv or sys.argv[1:]
    try:
        build_akcent_yml()
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"[akcent] Ошибка: {exc}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
