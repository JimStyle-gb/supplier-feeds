# -*- coding: utf-8 -*-
"""
CS Core — общее ядро для всех поставщиков.

В этом файле лежит "эталон CS":
- правила цены (4% + надбавки + хвост 900, но если цена невалидна/<=100 → 100)
- единый WhatsApp блок, HR, Оплата/Доставка
- единая сборка description + Характеристики
- единый keywords + хвост городов
- стабилизация форматирования (переводы строк, футер)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo
import hashlib
import re


# Дефолты (используются адаптерами)
OUTPUT_ENCODING_DEFAULT = "utf-8"
CURRENCY_ID_DEFAULT = "KZT"
ALMATY_TZ = "Asia/Almaty"


# Хвост городов (один и тот же для всех поставщиков)
CS_CITY_TAIL = (
    "Казахстан Алматы Нур-Султан Астана Шымкент Караганда Актобе Тараз Павлодар "
    "Усть-Каменогорск Усть Каменогорск Оскемен Семей Уральск Орал Темиртау Костанай "
    "Кызылорда Атырау Актау Кокшетау Петропавловск Талдыкорган Туркестан"
)

# WhatsApp блок (единый)
CS_WA_BLOCK = (
    "<!-- WhatsApp -->\n"
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; "
    "border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);\">"
    "&#128172; Написать в WhatsApp</a></p></div>"
)

# Горизонтальная линия (2px)
CS_HR_2PX = "<hr style=\"border:none; border-top:2px solid #E7D6B7; margin:12px 0;\" />"

# Оплата/Доставка — КАНОНИЧЕСКИЙ текст (как в твоём эталоне)
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

# Параметры, которые нужно выкидывать из <param> и из "Характеристик"
PARAM_DROP_DEFAULT = {
    "Штрихкод",
    "Новинка",
    "Снижена цена",
    "Благотворительность",
    "Код товара Kaspi",
    "Код ТН ВЭД",
    "Назначение",
    "Объём",
    "Объем",
}


# Возвращает текущее время в Алматы
def now_almaty() -> datetime:
    return datetime.now(ZoneInfo(ALMATY_TZ)).replace(tzinfo=None)


# Считает ближайший запуск на заданный час (Алматы) — для FEED_META
def next_run_at_hour(now_local: datetime, hour: int) -> datetime:
    hour = int(hour)
    candidate = now_local.replace(hour=hour, minute=0, second=0, microsecond=0)
    if candidate <= now_local:
        candidate = candidate + timedelta(days=1)
    return candidate


# Нормализует пробелы/переводы строк в строке
def norm_ws(s: str) -> str:
    s2 = (s or "").replace("\u00a0", " ").strip()
    s2 = re.sub(r"\s+", " ", s2)
    return s2.strip()


# Безопасное int из любого значения
def safe_int(v) -> int | None:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        s = s.replace(" ", "").replace("\u00a0", "")
        # иногда цена приходит как "12 345.00"
        s = s.split(".")[0]
        return int(s)
    except Exception:
        return None


# Парсит множество id из env (например "1,10,20") или из fallback списка
def parse_id_set(env_value: str | None, fallback: Iterable[int] | None = None) -> set[str]:
    out: set[str] = set()
    if env_value:
        for part in env_value.split(","):
            p = part.strip()
            if p:
                out.add(p)
    if not out and fallback:
        out = {str(int(x)) for x in fallback}
    return out


# Генератор стабильного id (если у поставщика нет id)
def stable_id(prefix: str, seed: str) -> str:
    h = hashlib.md5((seed or "").encode("utf-8", errors="ignore")).hexdigest()[:10]
    return f"{prefix}{h.upper()}"


# XML escape для текста
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
def compute_price(price_in: int | None) -> int:
    p = safe_int(price_in)
    if p is None or p <= 100:
        return 100
    if p >= 9_000_000:
        return 100

    tiers = [
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
    add = 60_000
    for lo, hi, a in tiers:
        if lo <= p <= hi:
            add = a
            break

    raw = int(p * 1.04 + add)

    # "хвост 900" (всегда заканчиваем на 900)
    out = (raw // 1000) * 1000 + 900

    if out >= 9_000_000:
        return 100
    if out <= 100:
        return 100
    return out


# Убирает мусорные параметры, пустые значения и дубли (применять всегда!)
def clean_params(
    params: Sequence[tuple[str, str]],
    *,
    drop: set[str] | None = None,
) -> list[tuple[str, str]]:
    drop_set = {norm_ws(x).casefold() for x in (drop or PARAM_DROP_DEFAULT)}
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    for k, v in params or []:
        kk = norm_ws(k)
        vv = norm_ws(v)
        if not kk or not vv:
            continue
        if kk.casefold() in drop_set:
            continue
        key_norm = kk.casefold()
        if key_norm in seen:
            continue
        seen.add(key_norm)
        out.append((kk, vv))
    return out


# Сортирует параметры: сначала приоритетные, затем по алфавиту
def sort_params(params: Sequence[tuple[str, str]], priority: Sequence[str] | None = None) -> list[tuple[str, str]]:
    pr = [norm_ws(x) for x in (priority or []) if norm_ws(x)]
    pr_map = {p.casefold(): i for i, p in enumerate(pr)}

    def key(kv):
        k = norm_ws(kv[0])
        idx = pr_map.get(k.casefold(), 10_000)
        return (idx, k.casefold())

    return sorted(list(params), key=key)


# Пробует извлечь пары "Характеристика: значение" из HTML описания (если поставщик кладёт это в description)
def enrich_params_from_desc(params: list[tuple[str, str]], desc_html: str) -> None:
    if not desc_html:
        return

    # <li><strong>Ключ:</strong> Значение</li>
    for m in re.finditer(r"<li>\s*<strong>([^<:]{1,80}):</strong>\s*([^<]{1,200})</li>", desc_html, flags=re.I):
        k = norm_ws(m.group(1))
        v = norm_ws(m.group(2))
        if k and v:
            params.append((k, v))


# Делает текст описания "без странностей" (убираем лишние пробелы)
def fix_text(s: str) -> str:
    t = (s or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    # убираем тройные пустые строки
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t


# Делает аккуратный HTML внутри CDATA (добавляет \n в начале/конце)
def normalize_cdata_inner(inner: str) -> str:
    inner = inner.strip()
    return "\n" + inner + "\n"


# Собирает keywords: vendor + name + токены из name + slug + города
def build_keywords(vendor: str, name: str, *, city_tail: str = CS_CITY_TAIL, max_tokens: int = 18) -> str:
    v = norm_ws(vendor)
    n = norm_ws(name)

    base_tokens: list[str] = []
    if v:
        base_tokens.append(v)
    if n:
        base_tokens.append(n)

    tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", n)
    tokens = [t for t in tokens if len(t) >= 2]
    # уникальные, сохраняя порядок
    seen = set()
    uniq: list[str] = []
    for t in tokens:
        tl = t.lower()
        if tl in seen:
            continue
        seen.add(tl)
        uniq.append(t)
        if len(uniq) >= max_tokens:
            break

    slug = re.sub(r"[^A-Za-zА-Яа-яЁё0-9]+", "-", n).strip("-").lower()

    parts: list[str] = []
    parts.extend(base_tokens)
    parts.extend(uniq)
    if slug:
        parts.append(slug)
    parts.append(city_tail)

    return norm_ws(" ".join(parts))


# Формирует блок "Характеристики" (HTML)
def build_chars_block(params_sorted: Sequence[tuple[str, str]]) -> str:
    if not params_sorted:
        return ""
    items: list[str] = []
    for k, v in params_sorted:
        kk = xml_escape_text(norm_ws(k))
        vv = xml_escape_text(norm_ws(v))
        if not kk or not vv:
            continue
        items.append(f"<li><strong>{kk}:</strong> {vv}</li>")
    if not items:
        return ""
    return "<h3>Характеристики</h3><ul>" + "".join(items) + "</ul>"


# Собирает description (WhatsApp + HR + Описание + Характеристики + Оплата/Доставка)
def build_description(
    name: str,
    native_desc: str,
    params_sorted: Sequence[tuple[str, str]],
    *,
    wa_block: str = CS_WA_BLOCK,
    hr_2px: str = CS_HR_2PX,
    pay_block: str = CS_PAY_BLOCK,
) -> str:
    n = norm_ws(name)
    d = fix_text(native_desc)

    desc_part = ""
    if d:
        # читаемость: \n → <br>
        d2 = xml_escape_text(d).replace("\n", "<br>")
        desc_part = f"<h3>{xml_escape_text(n)}</h3><p>{d2}</p>"
    else:
        desc_part = f"<h3>{xml_escape_text(n)}</h3><p></p>"

    chars = build_chars_block(params_sorted)

    parts: list[str] = []
    parts.append(wa_block)
    parts.append(hr_2px)
    parts.append("<!-- Описание -->")
    parts.append(desc_part)
    if chars:
        parts.append(chars)
    parts.append(pay_block)

    inner = "\n".join(parts)
    return normalize_cdata_inner(inner)


# Делает FEED_META (фиксированный вид)
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


# Пишет файл только если изменился (атомарно)
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


# Пытается определить бренд (vendor) по vendor_src / name / params / description (если пусто — public_vendor)
def pick_vendor(
    vendor_src: str,
    name: str,
    params: Sequence[tuple[str, str]],
    desc_html: str,
    *,
    public_vendor: str = "CS",
) -> str:
    v = norm_ws(vendor_src)
    if v:
        return v

    hay = " ".join(
        [name or "", desc_html or ""]
        + [f"{k} {val}" for k, val in (params or [])]
    ).lower()

    # простой словарь брендов (расширим позже при необходимости)
    brands = {
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
        "asus": "ASUS",
        "lenovo": "Lenovo",
        "acer": "Acer",
        "dell": "Dell",
        "logitech": "Logitech",
        "xiaomi": "Xiaomi",
    }
    for key, canon in brands.items():
        if re.search(rf"\b{re.escape(key)}\b", hay):
            return canon

    return norm_ws(public_vendor) or "CS"


@dataclass
class OfferOut:
    oid: str
    available: bool
    name: str
    price: int
    pictures: list[str]
    vendor: str
    params: list[tuple[str, str]]
    native_desc: str

    # Собирает XML offer (фиксированный порядок)
    def to_xml(
        self,
        *,
        currency_id: str = CURRENCY_ID_DEFAULT,
        city_tail: str = CS_CITY_TAIL,
        public_vendor: str = "CS",
        param_priority: Sequence[str] | None = None,
    ) -> str:
        name = norm_ws(self.name)
        vendor = pick_vendor(self.vendor, name, self.params, self.native_desc, public_vendor=public_vendor)

        # тройное обогащение: params + из описания
        params = list(self.params)
        enrich_params_from_desc(params, self.native_desc)

        # чистим и сортируем (ВАЖНО: чистить всегда)
        params = clean_params(params)
        params_sorted = sort_params(params, priority=list(param_priority or []))

        desc_cdata = build_description(name, self.native_desc, params_sorted)
        keywords = build_keywords(vendor, name, city_tail=city_tail)

        pics_xml = ""
        for p in self.pictures:
            pp = norm_ws(p)
            if pp:
                pics_xml += f"\n<picture>{xml_escape_text(pp)}</picture>"

        params_xml = ""
        for k, v in params_sorted:
            kk = xml_escape_attr(norm_ws(k))
            vv = xml_escape_text(norm_ws(v))
            if not kk or not vv:
                continue
            params_xml += f"\n<param name=\"{kk}\">{vv}</param>"

        out = (
            f"<offer id=\"{xml_escape_attr(self.oid)}\" available=\"{bool_to_xml(bool(self.available))}\">\n"
            f"<categoryId></categoryId>\n"
            f"<vendorCode>{xml_escape_text(self.oid)}</vendorCode>\n"
            f"<name>{xml_escape_text(name)}</name>\n"
            f"<price>{int(self.price)}</price>"
            f"{pics_xml}\n"
            f"<vendor>{xml_escape_text(vendor)}</vendor>\n"
            f"<currencyId>{xml_escape_text(currency_id)}</currencyId>\n"
            f"<description><![CDATA[{desc_cdata}]]></description>"
            f"{params_xml}\n"
            f"<keywords>{xml_escape_text(keywords)}</keywords>\n"
            f"</offer>"
        )
        return out
