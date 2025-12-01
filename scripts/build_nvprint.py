# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
"""
NVPrint -> YML (KZT) — стиль как у остальных поставщиков (AkCent/AlStyle/Copyline).

ENV:
- NVPRINT_XML_URL (или NVPRINT_URL)
- NVPRINT_LOGIN / NVPRINT_PASSWORD (или NVPRINT_XML_USER / NVPRINT_XML_PASS)
- OUT_FILE (default: docs/nvprint.yml)
- OUT_ENCODING (default: windows-1251)
- HTTP_TIMEOUT (default: 60)
"""

from __future__ import annotations

import io
import math
import os
import re
import sys
import time
import html as _html
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

try:
    import requests
except Exception:
    requests = None


# Делает: безопасно читает env (обрезает пробелы) и возвращает default, если пусто.
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return (v.strip() if v and v.strip() else default)


# Делает: хранит настройки пайплайна (URL/таймаут/кодировка/ретраи).
class Cfg:
    DEFAULT_URL = "https://api.nvprint.ru/api/hs/getprice/398/881105302369/none/?format=xml&getallinfo=true"

    SRC_URL = _env("NVPRINT_XML_URL", _env("NVPRINT_URL", DEFAULT_URL))
    OUT_FILE = _env("OUT_FILE", "docs/nvprint.yml")
    OUT_ENCODING = _env("OUT_ENCODING", "windows-1251")

    HTTP_TIMEOUT = float(_env("HTTP_TIMEOUT", "60"))
    RETRIES = int(_env("RETRIES", "4"))
    RETRY_BACKOFF_S = float(_env("RETRY_BACKOFF_S", "2"))

    LOGIN = _env("NVPRINT_LOGIN", _env("NVPRINT_XML_USER", ""))
    PASSWORD = _env("NVPRINT_PASSWORD", _env("NVPRINT_XML_PASS", ""))


# Делает: XML-экранирование для текстовых значений в теге (name/vendor/keywords/param).
def yml_escape(s: str) -> str:
    return _html.escape((s or "").strip())


# Делает: убирает namespace из ET.tag.
def strip_ns(tag: str) -> str:
    if not tag:
        return tag
    if tag.startswith("{"):
        i = tag.rfind("}")
        if i != -1:
            return tag[i + 1 :]
    return tag


# Делает: парсит число из строки (учитывает пробелы/запятую).
def parse_number(txt: Optional[str]) -> Optional[float]:
    if not txt:
        return None
    t = txt.strip().replace("\u00A0", "").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        return float(Decimal(m.group(0)))
    except (InvalidOperation, ValueError):
        return None


# Делает: берёт текст первого прямого ребёнка по списку имён тегов.
def first_child_text(item: ET.Element, tag_names: List[str]) -> Optional[str]:
    want = {t.lower() for t in tag_names}
    for ch in item:
        if strip_ns(ch.tag).lower() in want:
            val = (ch.text or "").strip()
            if val:
                return val
    return None


# Делает: находит первого потомка (в глубину) по списку имён тегов.
def find_descendant(item: ET.Element, tag_names: List[str]) -> Optional[ET.Element]:
    want = {t.lower() for t in tag_names}
    for node in item.iter():
        if strip_ns(node.tag).lower() in want:
            return node
    return None


# Делает: возвращает текст найденного потомка по списку имён тегов.
def find_descendant_text(item: ET.Element, tag_names: List[str]) -> Optional[str]:
    node = find_descendant(item, tag_names)
    if node is None:
        return None
    txt = (node.text or "").strip()
    return txt if txt else None


# Делает: скачивает XML с ретраями и (если нужно) BasicAuth.
def read_source_bytes(cfg: Cfg) -> bytes:
    if not cfg.SRC_URL:
        raise RuntimeError("NVPRINT_XML_URL пуст")
    if requests is None:
        raise RuntimeError("requests недоступен")

    auth = (cfg.LOGIN, cfg.PASSWORD) if (cfg.LOGIN or cfg.PASSWORD) else None
    last_err: Optional[Exception] = None

    for attempt in range(1, cfg.RETRIES + 1):
        try:
            r = requests.get(cfg.SRC_URL, timeout=cfg.HTTP_TIMEOUT, auth=auth)
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized: проверь NVPRINT_LOGIN/NVPRINT_PASSWORD в secrets")
            r.raise_for_status()
            if not r.content:
                raise RuntimeError("Источник вернул пустой ответ")
            return r.content
        except Exception as e:
            last_err = e
            if attempt >= cfg.RETRIES or ("401" in str(e)):
                break
            time.sleep(cfg.RETRY_BACKOFF_S * attempt)

    raise RuntimeError(str(last_err) if last_err else "Не удалось скачать источник")


# Делает: дефолтный фильтр по НоменклатураКратко (как договорились — файл keywords не нужен).
KEYWORDS: List[str] = [
    "шлейф",
    "блок фотобарабана",
    "картридж",
    "печатающая головка",
    "струйный картридж",
    "тонер-картридж",
    "тонер-туба",
]


# Делает: нормализует строку для сравнения (lower + один пробел).
def norm_for_match(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


# Делает: проверяет, что name_short начинается с одного из KEYWORDS.
def name_starts_with_keywords(name_short: str) -> bool:
    if not KEYWORDS:
        return True
    base = norm_for_match(name_short)
    for kw in KEYWORDS:
        if base.startswith(kw):
            return True
    return False


# Делает: нормализует номер договора (рус/анг похожие буквы, пробелы/дефисы).
def _norm_contract(s: str) -> str:
    if not s:
        return ""
    tr = str.maketrans(
        {
            "А": "A",
            "В": "B",
            "Е": "E",
            "К": "K",
            "М": "M",
            "Н": "H",
            "О": "O",
            "Р": "P",
            "С": "C",
            "Т": "T",
            "Х": "X",
            "У": "Y",
            "а": "A",
            "в": "B",
            "е": "E",
            "к": "K",
            "м": "M",
            "н": "H",
            "о": "O",
            "р": "P",
            "с": "C",
            "т": "T",
            "х": "X",
            "у": "Y",
            "Ё": "E",
            "ё": "e",
        }
    )
    u = s.translate(tr).upper()
    u = re.sub(r"[\s\-\_]+", "", u)
    return u


# Делает: вытаскивает цену из "Договор" с приоритетом Казахстан (не MSK) над Москвой.
def extract_price_from_contracts(item: ET.Element) -> Optional[float]:
    price_kz: Optional[float] = None
    price_msk: Optional[float] = None

    for node in item.iter():
        if strip_ns(node.tag).lower() != "договор":
            continue

        num = (node.attrib.get("НомерДоговора") or node.attrib.get("Номердоговора") or "").strip()
        num_n = _norm_contract(num)
        if "000079" not in num_n:
            continue

        price_el = find_descendant(node, ["Цена", "price", "amount", "value"])
        val = parse_number(price_el.text if price_el is not None else None)
        if val is None or val <= 0:
            continue

        if "MSK" in num_n or "МСК" in num_n:
            price_msk = val
        else:
            price_kz = val

    if price_kz is not None and price_kz > 0:
        return price_kz
    if price_msk is not None and price_msk > 0:
        return price_msk
    return None


# Делает: правила цены (4% + наценка по диапазонам) + "хвост 900".
PriceRule = Tuple[int, int, float, int]
PRICING_RULES: List[PriceRule] = [
    (101, 10000, 4.0, 3000),
    (10001, 25000, 4.0, 4000),
    (25001, 50000, 4.0, 5000),
    (50001, 75000, 4.0, 7000),
    (75001, 100000, 4.0, 10000),
    (100001, 150000, 4.0, 12000),
    (150001, 200000, 4.0, 15000),
    (200001, 300000, 4.0, 20000),
    (300001, 400000, 4.0, 25000),
    (400001, 500000, 4.0, 30000),
    (500001, 750000, 4.0, 40000),
    (750001, 1000000, 4.0, 50000),
    (1000001, 1500000, 4.0, 70000),
    (1500001, 2000000, 4.0, 90000),
    (2000001, 100000000, 4.0, 100000),
]


# Делает: округляет вверх до "...900" (например 11321 -> 11900).
def round_up_tail_900(n: int) -> int:
    thousands = (n + 999) // 1000
    return thousands * 1000 - 100


# Делает: применяет PRICING_RULES и обрабатывает "аномальные" цены.
def compute_price_from_supplier(base_price: Optional[int]) -> int:
    if base_price is None or base_price < 100:
        return 100
    if base_price >= 9_000_000:
        return 100

    for lo, hi, pct, add in PRICING_RULES:
        if lo <= base_price <= hi:
            raw = base_price * (1.0 + pct / 100.0) + add
            return round_up_tail_900(int(math.ceil(raw)))

    raw = base_price * (1.0 + PRICING_RULES[-1][2] / 100.0) + PRICING_RULES[-1][3]
    return round_up_tail_900(int(math.ceil(raw)))


# Делает: чистит артикул (убирает NV- и пробелы).
def clean_article(raw: str) -> str:
    s = (raw or "").strip()
    s = re.sub(r"^\s*NV[\-\_\s]+", "", s, flags=re.IGNORECASE)
    s = s.replace(" ", "")
    return s


# Делает: формирует id и vendorCode из артикула (NP + cleaned).
def make_ids_from_article(article: str) -> Tuple[str, str]:
    ac = clean_article(article)
    pref = "NP" + ac
    return pref, pref


# Делает: вытаскивает список принтеров из узла "Принтеры" (уникально, по порядку).
def collect_printers(item: ET.Element) -> List[str]:
    out: List[str] = []
    node = find_descendant(item, ["Принтеры", "Printers", "PrinterList"])
    if node is not None:
        for ch in node.iter():
            if strip_ns(ch.tag).lower() in {"принтер", "printer"}:
                t = (ch.text or "").strip()
                if t:
                    out.append(re.sub(r"\s+", " ", t))
    seen = set()
    uniq: List[str] = []
    for p in out:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


# Делает: пытается определить vendor (бренд) максимально стабильно.
def guess_vendor(name_short: str, vendor_raw: str, compat_text: str) -> str:
    v = (vendor_raw or "").strip()
    if v:
        return v

    s = f"{name_short} {compat_text}".upper()
    if "NVP" in s or "NVPRINT" in s:
        return "NVP"

    brands = [
        "HP",
        "CANON",
        "XEROX",
        "KYOCERA",
        "SAMSUNG",
        "BROTHER",
        "RICOH",
        "LEXMARK",
        "KONICA",
        "MINOLTA",
        "OKI",
        "PANASONIC",
        "TOSHIBA",
        "EPSON",
        "SHARP",
    ]
    for b in brands:
        if b in s:
            return b.title() if b not in {"HP"} else "HP"

    return "NVP" if "NVP" in name_short.upper() else ""


# Делает: вытаскивает ресурс (в скобках ...k) из строки, если в XML он отсутствует.
def pull_resurs_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"\((\d{2,7})\s*[kк]\)", text, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1)


# Делает: извлекает совместимость из текста по шаблону "для ... ( ... )".
def pull_compat_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"\bдля\s+(.+?)(?:\s*\(|$)", text, flags=re.IGNORECASE)
    if not m:
        return None
    c = re.sub(r"\s+", " ", m.group(1)).strip(" -;,.")
    return c if c else None


# Делает: пытается определить тип печати (если в XML нет).
def guess_print_type(text: str) -> Optional[str]:
    t = (text or "").upper()
    if any(x in t for x in ["LASERJET", "ECOSYS", "TASKALFA", "WORKCENTRE", "COLOR LASER", "LASER SHOT", "LBP", "SCX", "MX", "MS"]):
        return "Лазерная"
    if any(x in t for x in ["INK", "INKJET", "STYLUS", "DESKJET", "PIXMA", "ECOTANK", "L3", "T0"]):
        return "Струйная"
    return None


# Делает: безопасно превращает "сырой текст" в HTML внутри <p> (экранирование + <br>).
def to_html_paragraph(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    t = _html.escape(t)
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    t = t.replace("\n\n", "<br><br>").replace("\n", "<br>")
    return t


# Делает: собирает характеристики (XML + парсинг из текста) и "обогащает" описание/param.
def build_characteristics(item: ET.Element, name_short: str, nom_full: str) -> Dict[str, str]:
    chars: Dict[str, str] = {}

    res = find_descendant_text(item, ["Ресурс", "Resurs", "Yield", "PageYield"]) or ""
    if not res or res.strip() in {"0", "0.0"}:
        res = pull_resurs_from_text(nom_full) or pull_resurs_from_text(name_short) or ""
    if res:
        res = re.sub(r"\s+", " ", res).strip()
        res = re.sub(r"[kк]$", "", res, flags=re.IGNORECASE).strip()
        chars["Ресурс"] = res

    tp = find_descendant_text(item, ["ТипПечати", "Тип печати", "PrintType"]) or ""
    if not tp:
        tp = guess_print_type(nom_full) or guess_print_type(name_short) or ""
    if tp:
        chars["Тип печати"] = re.sub(r"\s+", " ", tp).strip()

    color = find_descendant_text(item, ["ЦветПечати", "Цвет печати", "Color", "Colour"]) or ""
    if not color:
        m = re.search(r"\b(Black|Cyan|Magenta|Yellow|BK|C|M|Y)\b", name_short, flags=re.IGNORECASE)
        if m:
            color = m.group(1)
    if color:
        chars["Цвет печати"] = re.sub(r"\s+", " ", color).strip()

    compat = find_descendant_text(item, ["СовместимостьСМоделями", "Совместимость", "Compatibility", "Models"]) or ""
    if not compat:
        compat = pull_compat_from_text(nom_full) or pull_compat_from_text(name_short) or ""
    if compat:
        compat = re.sub(r"\s+", " ", compat).strip()
        chars["Совместимость с моделями"] = compat

    weight = find_descendant_text(item, ["Вес", "Weight"]) or ""
    if weight and weight.strip() not in {"0", "0.0"}:
        chars["Вес"] = re.sub(r"\s+", " ", weight).strip()

    printers = collect_printers(item)
    if printers:
        chars["Совместимые устройства"] = ", ".join(printers)

    # Дедуп: если "Совместимые устройства" уже покрывает совместимость, оставляем обе только если реально разные.
    if "Совместимость с моделями" in chars and "Совместимые устройства" in chars:
        a = norm_for_match(chars["Совместимость с моделями"])
        b = norm_for_match(chars["Совместимые устройства"])
        if a and b and (a in b or b in a):
            # оставляем более "читаемую" (почти всегда список устройств)
            chars.pop("Совместимость с моделями", None)

    return chars


# Делает: сортирует характеристики (приоритетные ключи сверху, дальше по алфавиту).
def sort_characteristics(chars: Dict[str, str]) -> List[Tuple[str, str]]:
    priority = ["Ресурс", "Тип печати", "Цвет печати", "Совместимые устройства", "Совместимость с моделями", "Вес"]
    out: List[Tuple[str, str]] = []
    used = set()

    for k in priority:
        if k in chars:
            out.append((k, chars[k]))
            used.add(k)

    rest = sorted(((k, v) for k, v in chars.items() if k not in used), key=lambda x: x[0].lower())
    out.extend(rest)
    return out


# Делает: строит HTML блока описания (h3+параграф+характеристики ul) как у остальных.
def build_description_html(title: str, body_text: str, chars: Dict[str, str]) -> str:
    h = f"<h3>{_html.escape(title)}</h3>"
    p = to_html_paragraph(body_text)
    p = f"<p>{p}</p>" if p else ""

    items = sort_characteristics(chars)
    if items:
        li = "".join([f"<li><strong>{_html.escape(k)}:</strong> {_html.escape(v)}</li>" for k, v in items if v])
        hchars = f"<h3>Характеристики</h3><ul>{li}</ul>"
    else:
        hchars = ""

    return f"{h}{p}{hchars}"


# Делает: общий WhatsApp блок (должен быть одинаковый у всех поставщиков).
WHATSAPP_BLOCK = (
    "<!-- WhatsApp -->\n"
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; "
    "border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,08);\">"
    "&#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!"
    "</a></p>"
    "<div style=\"background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;\">"
    "<h3 style=\"margin:0 0 8px; font-size:17px;\">Оплата</h3>"
    "<ul style=\"margin:0; padding-left:18px;\">"
    "<li>Kaspi QR / Kaspi Pay</li>"
    "<li>Наличные</li>"
    "</ul>"
    "<h3 style=\"margin:12px 0 8px; font-size:17px;\">Доставка</h3>"
    "<ul style=\"margin:0; padding-left:18px;\">"
    "<li>По Алматы и Казахстану</li>"
    "<li>Самовывоз</li>"
    "</ul>"
    "</div></div>"
)


# Делает: пытается определить наличие (available) из количества/остатка, если такие поля есть; иначе True.
def infer_available(item: ET.Element) -> bool:
    qty = find_descendant_text(item, ["Остаток", "Остатки", "Количество", "КолВо", "Qty", "Quantity", "Stock", "InStock", "Available"])
    n = parse_number(qty) if qty else None
    if n is None:
        return True
    return n > 0


# Делает: токенизирует строку для keywords (слова/цифры/латиница/кириллица).
def kw_tokens(s: str) -> List[str]:
    raw = re.split(r"[^0-9A-Za-zА-Яа-яЁё]+", (s or "").strip())
    out = [t for t in raw if t]
    # убираем очень короткие мусорные токены, но оставляем цифры и коды типа 'A3'
    return [t for t in out if (len(t) >= 2 or t.isdigit())]


# Делает: транслитерацию RU->LAT для keywords-слуг.
def translit_ru(s: str) -> str:
    mp = {
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
        "ц": "ts",
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
    out = []
    for ch in (s or "").lower():
        if ch in mp:
            out.append(mp[ch])
        elif re.match(r"[0-9a-z]", ch):
            out.append(ch)
        else:
            out.append("-")
    slug = "".join(out)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug


# Делает: собирает keywords в стиле остальных поставщиков (vendor, name, токены, slugs, города).
def build_keywords(vendor: str, name: str) -> str:
    v = (vendor or "").strip()
    n = (name or "").strip()

    toks = kw_tokens(n)
    last = toks[-1] if toks else ""
    v_last = f"{v} {last}".strip() if v and last else ""

    slugs = []
    tl = [translit_ru(t) for t in toks]
    tl = [t for t in tl if t]
    if len(tl) >= 2:
        slugs.append("-".join(tl[:2]))
    if len(tl) >= 3:
        slugs.append("-".join(tl[:3]))
    for t in tl[:5]:
        slugs.append(t)
    if v and last:
        slugs.append(f"{translit_ru(v)}-{translit_ru(last)}")

    cities = [
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

    parts: List[str] = []
    if v:
        parts.append(v)
    if n:
        parts.append(n)
    parts.extend(toks[:12])
    if v_last:
        parts.append(v_last)
    parts.extend([s for s in slugs if s])
    parts.extend(cities)

    # uniq preserving order
    seen = set()
    uniq: List[str] = []
    for p in parts:
        p2 = re.sub(r"\s+", " ", p.strip())
        if not p2:
            continue
        key = p2.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p2)

    return ", ".join(uniq)


# Делает: парсит один товарный узел в dict (для оффера).
def parse_item(node: ET.Element) -> Optional[Dict[str, Any]]:
    article = first_child_text(node, ["Артикул", "articul", "sku", "article", "PartNumber"])
    if not article:
        return None

    name_short = find_descendant_text(node, ["НоменклатураКратко"]) or ""
    name_short = re.sub(r"\s+", " ", name_short).strip()
    if not name_short:
        return None

    if not name_starts_with_keywords(name_short):
        return None

    nom_full = find_descendant_text(node, ["Номенклатура", "FullName", "Name"]) or name_short
    nom_full = re.sub(r"\s+", " ", nom_full).strip()

    base = extract_price_from_contracts(node)
    base_int = 100 if (base is None or base <= 0) else int(math.ceil(base))
    final_price = compute_price_from_supplier(base_int)

    vendor_raw = first_child_text(node, ["Бренд", "Производитель", "Вендор", "Brand", "Vendor"]) or ""
    picture = (
        first_child_text(node, ["СсылкаНаКартинку", "Картинка", "Изображение", "Фото", "Picture", "Image", "ФотоURL", "PictureURL"])
        or ""
    ).strip()

    chars = build_characteristics(node, name_short, nom_full)
    compat_for_vendor = chars.get("Совместимые устройства") or chars.get("Совместимость с моделями") or ""
    vendor = guess_vendor(name_short, vendor_raw, compat_for_vendor)

    offer_id, vendor_code = make_ids_from_article(article)
    available = infer_available(node)

    desc_html = build_description_html(name_short, nom_full, chars)
    # защита от "]]>" внутри CDATA
    desc_html = desc_html.replace("]]>", "]]&gt;")

    return {
        "id": offer_id,
        "available": available,
        "vendorCode": vendor_code,
        "name": name_short,
        "price": final_price,
        "picture": picture,
        "vendor": vendor,
        "description_html": desc_html,
        "params": sort_characteristics(chars),
        "keywords": build_keywords(vendor, name_short),
    }


# Делает: находит кандидатов-узлы товаров (ищем узлы-родители тега Артикул).
def guess_item_nodes(root: ET.Element) -> List[ET.Element]:
    want_art = {"артикул", "articul", "sku", "article", "partnumber"}
    parent_map = {c: p for p in root.iter() for c in list(p)}

    items: List[ET.Element] = []
    seen: set[int] = set()

    for el in root.iter():
        if strip_ns(el.tag).lower() not in want_art:
            continue
        item = parent_map.get(el)
        if item is None:
            continue
        key = id(item)
        if key in seen:
            continue
        if find_descendant(item, ["НоменклатураКратко"]) is None:
            continue
        seen.add(key)
        items.append(item)

    return items


# Делает: возвращает "сейчас" по Алматы (UTC+5) как naive datetime.
def almaty_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5)


# Делает: ближайшая 1/10/20 дата в 04:00 по Алматы.
def next_build_1_10_20_at_04() -> datetime:
    now = almaty_now()
    targets = [1, 10, 20]
    cands: List[datetime] = []
    for d in targets:
        try:
            cands.append(now.replace(day=d, hour=4, minute=0, second=0, microsecond=0))
        except ValueError:
            pass

    future = [t for t in cands if t > now]
    if future:
        return min(future)

    if now.month == 12:
        return now.replace(year=now.year + 1, month=1, day=1, hour=4, minute=0, second=0, microsecond=0)

    first_next = (now.replace(day=1, hour=4, minute=0, second=0, microsecond=0) + timedelta(days=32)).replace(day=1)
    return first_next


# Делает: форматирует FEED_META даты как "YYYY-MM-DD HH:MM:SS".
def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# Делает: рендерит FEED_META блок в нужном виде и месте.
def render_feed_meta_comment(source_url: str, offers_total: int, offers_written: int, avail_true: int, avail_false: int) -> str:
    rows = [
        ("Поставщик", "NVPrint"),
        ("URL поставщика", source_url),
        ("Время сборки (Алматы)", fmt_dt(almaty_now())),
        ("Ближайшая сборка (Алматы)", fmt_dt(next_build_1_10_20_at_04())),
        ("Сколько товаров у поставщика до фильтра", str(offers_total)),
        ("Сколько товаров у поставщика после фильтра", str(offers_written)),
        ("Сколько товаров есть в наличии (true)", str(avail_true)),
        ("Сколько товаров нет в наличии (false)", str(avail_false)),
    ]
    key_w = max(len(k) for k, _ in rows)
    lines = ["<!--FEED_META"]
    for i, (k, v) in enumerate(rows):
        end = "" if i < len(rows) - 1 else "\n-->"
        lines.append(f"{k.ljust(key_w)} | {v}{end}")
    return "\n".join(lines)


# Делает: гарантирует 2 перевода строки после <shop><offers> и перед </offers>.
def ensure_footer_spacing(s: str) -> str:
    s = s.replace("<shop><offers>\n", "<shop><offers>\n\n")
    s = re.sub(r"</offer>\n</offers>", "</offer>\n\n</offers>", s)
    return s


# Делает: рендерит готовый YML (как у остальных поставщиков).
def build_yml(cfg: Cfg, xml_bytes: bytes) -> str:
    root = ET.fromstring(xml_bytes)
    nodes = guess_item_nodes(root)
    offers_total = len(nodes)

    offers: List[Dict[str, Any]] = []
    for node in nodes:
        it = parse_item(node)
        if it:
            offers.append(it)

    avail_true = sum(1 for o in offers if o["available"])
    avail_false = sum(1 for o in offers if not o["available"])

    date_attr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    out: List[str] = []
    out.append('<?xml version="1.0" encoding="windows-1251"?>')
    out.append('<!DOCTYPE yml_catalog SYSTEM "shops.dtd">')
    out.append(f'<yml_catalog date="{date_attr}">')
    out.append("<shop><offers>")
    out.append(render_feed_meta_comment(cfg.SRC_URL, offers_total, len(offers), avail_true, avail_false))
    out.append("")

    for it in offers:
        out.append(f'<offer id="{yml_escape(it["id"])}" available="{str(it["available"]).lower()}">')
        out.append("<categoryId></categoryId>")
        out.append(f'<vendorCode>{yml_escape(it["vendorCode"])}</vendorCode>')
        out.append(f'<name>{yml_escape(it["name"])}</name>')
        out.append(f'<price>{int(it["price"])}</price>')
        if it.get("picture"):
            out.append(f'<picture>{yml_escape(it["picture"])}</picture>')
        if it.get("vendor"):
            out.append(f'<vendor>{yml_escape(it["vendor"])}</vendor>')
        out.append("<currencyId>KZT</currencyId>")

        out.append("<description><![CDATA[")
        out.append("")
        out.append(WHATSAPP_BLOCK)
        out.append("")
        out.append("<!-- Описание -->")
        out.append(it["description_html"])
        out.append("")
        out.append("]]></description>")

        for k, v in it.get("params", []):
            if v:
                out.append(f'<param name="{yml_escape(k)}">{yml_escape(v)}</param>')

        out.append(f"<keywords>{yml_escape(it['keywords'])}</keywords>")
        out.append("</offer>")
        out.append("")

    out.append("</offers></shop></yml_catalog>")
    return ensure_footer_spacing("\n".join(out))


# Делает: пишет пустой корректный YML (если источник недоступен/ошибка парсинга).
def empty_yml(cfg: Cfg) -> str:
    date_attr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    out = [
        '<?xml version="1.0" encoding="windows-1251"?>',
        '<!DOCTYPE yml_catalog SYSTEM "shops.dtd">',
        f'<yml_catalog date="{date_attr}">',
        "<shop><offers>",
        render_feed_meta_comment(cfg.SRC_URL, 0, 0, 0, 0),
        "",
        "</offers></shop></yml_catalog>",
    ]
    return ensure_footer_spacing("\n".join(out))


# Делает: точка входа скрипта.
def main() -> int:
    cfg = Cfg()
    try:
        data = read_source_bytes(cfg)
        yml = build_yml(cfg, data)
    except Exception as e:
        yml = empty_yml(cfg)
        print(f"ERROR: {e}", file=sys.stderr)

    os.makedirs(os.path.dirname(cfg.OUT_FILE) or ".", exist_ok=True)
    with io.open(cfg.OUT_FILE, "w", encoding=cfg.OUT_ENCODING, errors="ignore") as f:
        f.write(yml)

    print(f"Wrote: {cfg.OUT_FILE} | encoding={cfg.OUT_ENCODING}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
