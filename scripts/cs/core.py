# -*- coding: utf-8 -*-
"""
CS Core — общее ядро для всех поставщиков (шаблон CS).
Важно: этот модуль не знает, как именно скачивать/парсить товары конкретного поставщика.
Он содержит общие правила: цены, keywords, описание, сортировка params, FEED_META, форматирование XML.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import hashlib
import os
import re
from typing import Iterable, Sequence

# Константы шаблона CS
CS_CITY_TAIL = "Казахстан, Алматы, Астана, Шымкент, Караганда, Актобе, Тараз, Павлодар, Усть-Каменогорск, Семей, Уральск, Темиртау, Костанай, Кызылорда, Петропавловск, Атырау, Актау, Талдыкорган, Кокшетау"


CURRENCY_ID_DEFAULT = "KZT"
OUTPUT_ENCODING_DEFAULT = "utf-8"
ALMATY_UTC_OFFSET_HOURS = 5
CS_WA_BLOCK = (
    "<!-- WhatsApp -->\n"
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; "
    "border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);\">"
    "&#128172; Написать в WhatsApp</a></p></div>"
)

CS_HR_2PX = "<hr style=\"border:none; border-top:2px solid #E7D6B7; margin:12px 0;\" />"

CS_PAY_BLOCK = (
    "<!-- Оплата и доставка -->\n"
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<div style=\"background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;\">"
    "<h3 style=\"margin:0 0 8px; font-size:17px;\">Оплата</h3>"
    "<ul style=\"margin:0; padding-left:18px;\">"
    "<li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li>"
    "<li><strong>Удалённая оплата</strong> по <span style=\"color:#a40000;\"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li>"
    "</ul>"
    "<hr style=\"border:none; border-top:1px solid #E7D6B7; margin:12px 0;\" />"
    "<h3 style=\"margin:0 0 8px; font-size:17px;\">Доставка по Алматы и Казахстану</h3>"
    "<ul style=\"margin:0; padding-left:18px;\">"
    "<li><em><strong>ДОСТАВКА</strong> в \"квадрате\" г. Алматы — <strong>БЕСПЛАТНО!</strong></em></li>"
    "<li><strong>Самовывоз:</strong> г. Алматы</li>"
    "<li><strong>Курьер:</strong> по Алматы</li>"
    "<li><strong>Казпочта / транспортные компании:</strong> по Казахстану</li>"
    "</ul>"
    "</div></div>"
)

# Бренды (минимальный безопасный словарь, можно расширять в адаптерах)
DEFAULT_BRAND_MAP = {
    # латиница
    "hp": "HP",
    "hewlett-packard": "HP",
    "canon": "Canon",
    "epson": "Epson",
    "xerox": "Xerox",
    "brother": "Brother",
    "samsung": "Samsung",
    "lenovo": "Lenovo",
    "dell": "Dell",
    "asus": "ASUS",
    "acer": "Acer",
    "msi": "MSI",
    "gigabyte": "Gigabyte",
    "dlink": "D-Link",
    "tp-link": "TP-Link",
    "hikvision": "Hikvision",
    "dahua": "Dahua",
    "ubiquiti": "Ubiquiti",
    "mikrotik": "MikroTik",
    "apc": "APC",
    "schneider": "Schneider Electric",
    "yealink": "Yealink",
    # кириллица
    "дкс": "ДКС",
    "комус": "Комус",
}

STOP_WORDS = {
    "и", "в", "на", "для", "с", "по", "к", "от", "до", "из", "под", "над", "при",
    "the", "and", "or", "for", "with", "to", "of", "in",
}

# Параметры, которые нужно удалить из param и характеристик
DROP_PARAM_NAMES = {"штрихкод"}

RE_WS = re.compile(r"\s+")
RE_TOKEN = re.compile(r"[A-Za-zА-Яа-яЁё0-9][A-Za-zА-Яа-яЁё0-9\-_/\.]*")
RE_SLUG_BAD = re.compile(r"[^a-z0-9]+")
RE_DESC_KV = re.compile(r"(?im)^\s*([A-Za-zА-Яа-яЁё0-9][^:\n]{0,40})\s*:\s*(.{1,180})\s*$")


# Текущее время Алматы
def now_almaty() -> datetime:
    return datetime.utcnow() + timedelta(hours=ALMATY_UTC_OFFSET_HOURS)


# Следующая плановая сборка (для FEED_META)
def next_run_at_hour(build_time: datetime, hour_almaty: int) -> datetime:
    base = build_time.replace(minute=0, second=0, microsecond=0)
    if base.hour < hour_almaty:
        return base.replace(hour=hour_almaty)
    if base.hour == hour_almaty and (build_time.minute == 0 and build_time.second == 0):
        return base
    nxt = base + timedelta(days=1)
    return nxt.replace(hour=hour_almaty)


# Сжатие пробелов
def norm_ws(s: str) -> str:
    return RE_WS.sub(" ", (s or "").strip())


# Безопасный int: None/""/не число → None
def safe_int(src: str | None) -> int | None:
    if src is None:
        return None
    s = str(src).strip().replace(" ", "").replace("\xa0", "")
    if not s:
        return None
    m = re.search(r"-?\d+", s)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


# Парсинг множества id из ENV: "1,2 3\n4" → {1,2,3,4}
def parse_id_set(env_value: str | None, fallback: set[str]) -> set[str]:
    if not env_value:
        return set(fallback)
    parts = re.split(r"[,\s]+", env_value.strip())
    out = {p.strip() for p in parts if p.strip().isdigit()}
    return out if out else set(fallback)


# Экранирование текста XML
def xml_escape_text(s: str) -> str:
    if s is None:
        return ""
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


# Экранирование атрибутов XML
def xml_escape_attr(s: str) -> str:
    if s is None:
        return ""
    return xml_escape_text(s).replace('"', "&quot;")


# Приведение available к "true/false"
def bool_to_xml(v: bool) -> str:
    return "true" if v else "false"


# Слаг из строки (для keywords)
def slugify(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("ё", "е")
    s = RE_SLUG_BAD.sub("-", s)
    s = s.strip("-")
    return s


# Удаляет лишнее из строки (не ломая смысл)
def fix_text(s: str) -> str:
    s = s or ""
    s = s.replace("\r\n", "\n")
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{4,}", "\n\n\n", s)
    return s.strip()


# Ровно 2 \n в начале/конце CDATA-части
def normalize_cdata_inner(inner: str) -> str:
    b = (inner or "").replace("\r\n", "\n")
    core = b.lstrip("\n").rstrip("\n")
    return "\n" + core + "\n"


# price-rule: +4% + tier adders; хвост 900; >=9_000_000 → 100; <=100 → 100
def compute_price(price_in: int | None) -> int:
    if price_in is None or price_in <= 100:
        return 100
    if price_in >= 9_000_000:
        return 100

    p = price_in

    # надбавки
    if 101 <= p <= 10_000:
        p = int(round(p * 1.04)) + 3000
    elif 10_001 <= p <= 25_000:
        p = int(round(p * 1.04)) + 4000
    elif 25_001 <= p <= 50_000:
        p = int(round(p * 1.04)) + 5000
    elif 50_001 <= p <= 75_000:
        p = int(round(p * 1.04)) + 7000
    elif 75_001 <= p <= 100_000:
        p = int(round(p * 1.04)) + 10000
    elif 100_001 <= p <= 150_000:
        p = int(round(p * 1.04)) + 12000
    elif 150_001 <= p <= 200_000:
        p = int(round(p * 1.04)) + 14000
    elif 200_001 <= p <= 300_000:
        p = int(round(p * 1.04)) + 17000
    elif 300_001 <= p <= 400_000:
        p = int(round(p * 1.04)) + 20000
    elif 400_001 <= p <= 600_000:
        p = int(round(p * 1.04)) + 26000
    elif 600_001 <= p <= 800_000:
        p = int(round(p * 1.04)) + 32000
    elif 800_001 <= p <= 1_200_000:
        p = int(round(p * 1.04)) + 45000
    else:
        p = int(round(p * 1.04)) + 60000

    if p < 100:
        return 100

    # "хвост 900"
    p = (p // 1000) * 1000 + 900

    # > 9 млн после округлений — в 100
    if p >= 9_000_000:
        return 100

    return p


# Достает ключевые слова из названия (и чуть из бренда), добавляет слаг и города
def build_keywords(
    vendor: str,
    name: str,
    *,
    city_tail: str = CS_CITY_TAIL,
    max_tokens_from_name: int = 18,
    extra_terms: Sequence[str] | None = None,
) -> str:
    v = norm_ws(vendor)
    n = norm_ws(name)

    out: list[str] = []
    seen: set[str] = set()

    def add(x: str) -> None:
        x = norm_ws(x)
        if not x:
            return
        key = x.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(x)

    if v:
        add(v)
    if n:
        add(n)

    # токены из названия
    tokens = [t for t in RE_TOKEN.findall(n) if t]
    cleaned: list[str] = []
    for t in tokens:
        tl = t.lower().strip("._-/")
        if not tl or tl in STOP_WORDS:
            continue
        if len(tl) < 2:
            continue
        cleaned.append(t)

    # делаем более "богатый" хвост: первые N токенов
    for t in cleaned[:max_tokens_from_name]:
        add(t)

    # бренд + короткий хвост
    if v:
        # пример: "MSI 17"
        m = re.search(r"\b(\d{1,3})\b", n)
        if m:
            add(f"{v} {m.group(1)}")

    # слаг
    sl = slugify(f"{v} {n}".strip())
    if sl:
        add(sl)
        if v:
            add(f"{sl}-{slugify(v)}".strip("-"))

    # доп. термины от адаптера (НЕ ограничено 1-2 словами, можно сколько угодно)
    if extra_terms:
        for t in extra_terms:
            add(t)

    # города (единый список)
    add(city_tail)

    return ", ".join(out)


# Пытается определить бренд по тексту (name/params/desc)
def detect_brand(text: str, brand_map: dict[str, str] | None = None) -> str | None:
    m = brand_map or DEFAULT_BRAND_MAP
    t = " " + (text or "").lower() + " "
    # приоритет: более длинные ключи сначала
    for k in sorted(m.keys(), key=len, reverse=True):
        kk = k.lower()
        # match по границам слова/символов
        if re.search(rf"(?i)(?<![A-Za-zА-Яа-яЁё0-9]){re.escape(kk)}(?![A-Za-zА-Яа-яЁё0-9])", t):
            return m[k]
    return None


# Выбирает vendor: (src vendor) -> detect -> PUBLIC_VENDOR (по умолчанию "CS")
def pick_vendor(
    vendor_src: str,
    name: str,
    params: Sequence[tuple[str, str]],
    desc: str,
    *,
    public_vendor: str = "CS",
    brand_map: dict[str, str] | None = None,
) -> str:
    v = norm_ws(vendor_src)
    if v:
        return v
    joined_params = " ".join([f"{k} {val}" for k, val in params])
    text = f"{name} {joined_params} {desc}"
    b = detect_brand(text, brand_map=brand_map)
    return b or public_vendor


# Удаляет запрещённые характеристики (например, Штрихкод)
def drop_params(params: Sequence[tuple[str, str]], drop: set[str] = DROP_PARAM_NAMES) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for k, v in params:
        kk = norm_ws(k)
        vv = norm_ws(v)
        if not kk or not vv:
            continue
        if kk.lower() in drop:
            continue
        out.append((kk, vv))
    return out


# Сортировка характеристик: приоритет + алфавит
def sort_params(params: Sequence[tuple[str, str]], priority: Sequence[str]) -> list[tuple[str, str]]:
    pr = {p.lower(): i for i, p in enumerate(priority)}
    def key(kv: tuple[str, str]) -> tuple[int, str]:
        k = (kv[0] or "").lower()
        return (pr.get(k, 10_000), k)
    return sorted(list(params), key=key)


# Строит HTML блока характеристик
def build_chars_ul(params_sorted: Sequence[tuple[str, str]]) -> str:
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


# Тройное обогащение: из описания вытаскивает пары "Ключ: Значение" в params
def enrich_params_from_desc(params: list[tuple[str, str]], desc: str) -> int:
    if not desc:
        return 0
    existing = {norm_ws(k).lower() for k, _ in params}
    added = 0
    for m in RE_DESC_KV.finditer(desc):
        k = norm_ws(m.group(1))
        v = norm_ws(m.group(2))
        if not k or not v:
            continue
        # ограничение "ключ" — не слишком длинный (обычно 1–3 слова)
        if len(k.split()) > 4:
            continue
        kl = k.lower()
        if kl in DROP_PARAM_NAMES:
            continue
        if kl in existing:
            continue
        existing.add(kl)
        params.append((k, v))
        added += 1
    return added


# Собирает описание CS: WhatsApp + hr + (Описание+Характеристики) + Pay
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

    # из исходного описания делаем p-часть
    desc_part = ""
    if d:
        # аккуратные <br> для читабельности
        d2 = d.replace("\n", "<br>")
        desc_part = f"<h3>{xml_escape_text(n)}</h3><p>{d2}</p>"
    else:
        desc_part = f"<h3>{xml_escape_text(n)}</h3>"

    chars = build_chars_ul(params_sorted)

    parts: list[str] = []
    parts.append(wa_block)
    parts.append(hr_2px)
    parts.append("<!-- Описание -->")
    parts.append(desc_part)
    if chars:
        parts.append(chars)
    parts.append(pay_block)

    # В CDATA нам нужен \n в начале/конце
    inner = "\n".join(parts)
    return normalize_cdata_inner(inner)


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

    # Собирает XML offer (строго фиксированный порядок тегов)
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

        # тройное обогащение: из описания добавим пару param, затем снова сортируем
        params = drop_params(self.params)
        enrich_params_from_desc(params, self.native_desc)

        priority = list(param_priority or [])
        params_sorted = sort_params(params, priority) if priority else sorted(params, key=lambda kv: (kv[0] or "").lower())

        desc_cdata = build_description(name, self.native_desc, params_sorted)
        keywords = build_keywords(vendor, name, city_tail=city_tail)

        pics_xml = ""
        for p in self.pictures:
            if p:
                pics_xml += f"\n<picture>{xml_escape_text(p)}</picture>"

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


# Делает блок FEED_META (строго фиксированный вид)
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


# Шапка yml_catalog
def make_header(build_time: datetime, *, encoding: str = OUTPUT_ENCODING_DEFAULT) -> str:
    return (
        f"<?xml version=\"1.0\" encoding=\"{encoding}\">\n"
        f"<yml_catalog date=\"{build_time:%Y-%m-%d %H:%M}\">\n"
        f"<shop>\n"
        f"<offers>\n"
    )


# Низ yml_catalog
def make_footer() -> str:
    return "</offers>\n</shop>\n</yml_catalog>\n"


# Футерные пробелы: 2 перевода после <offers> и перед </offers>
def ensure_footer_spacing(xml: str) -> str:
    # после <offers>
    xml = re.sub(r"(<offers>\n)(\n*)", r"\1\n", xml, count=1)
    # перед </offers>
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


# Генератор стабильного id (если у поставщика нет id)
def stable_id(prefix: str, seed: str) -> str:
    h = hashlib.md5((seed or "").encode("utf-8", errors="ignore")).hexdigest()[:10]
    return f"{prefix}{h.upper()}"
