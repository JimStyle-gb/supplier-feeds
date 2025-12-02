#!/usr/bin/env python3
# build_vtt.py — v6 (output style = AkCent)
# Правки:
# - Удаление param: "Штрих-код/Штрихкод/EAN/GTIN/Barcode", "Категория/Подкатегория",
#   "Каталожный номер", "OEM-номер" (и варианты).
# - Нормализация значений: Цвет -> "Русский (English)", Ресурс -> "#### стр."
# - Обогащение: добираем из name (цвет/мл/ресурс) + добавляем "Тип" по источнику категории.
# - Параметры <param> и блок "Характеристики" в <description> = один список (1-в-1).
# ВАЖНО: логика бренда (<vendor>) и логика цены НЕ менялись.

from __future__ import annotations

import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

try:
    import urllib3  # type: ignore
    from urllib3.exceptions import InsecureRequestWarning  # type: ignore
except Exception:
    urllib3 = None  # type: ignore
    InsecureRequestWarning = None  # type: ignore


# -------------------- Настройки (env) --------------------

BASE_URL = os.getenv("VTT_BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL = os.getenv("VTT_START_URL", f"{BASE_URL}/catalog/")

OUT_FILE = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "200"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "16"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

SLEEP_MIN = float(os.getenv("SLEEP_MIN", "0.05"))
SLEEP_MAX = float(os.getenv("SLEEP_MAX", "0.20"))

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

LOGIN = os.getenv("VTT_LOGIN", "")
PASSWORD = os.getenv("VTT_PASSWORD", "")

# SSL: если на Actions падает CERT_VERIFY_FAILED, ставь VTT_SSL_VERIFY=0 (как в workflow)
VTT_SSL_VERIFY = os.getenv("VTT_SSL_VERIFY", "1")  # 1/0 true/false
VTT_CA_BUNDLE = os.getenv("VTT_CA_BUNDLE", "")      # путь до .pem (если есть)

# Категории — вшиты в код
CATEGORIES: List[str] = [
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_COMPAT",
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_ORIG",
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_PRNTHD",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_COMPAT",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_COPY",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_ORIG",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_PRINT",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_TNR",
    "https://b2b.vtt.ru/catalog/?category=CARTMAT_CART",
    "https://b2b.vtt.ru/catalog/?category=DEV_DEV",
    "https://b2b.vtt.ru/catalog/?category=DRM_CRT",
    "https://b2b.vtt.ru/catalog/?category=DRM_UNIT",
    "https://b2b.vtt.ru/catalog/?category=PARTSPRINT_THERBLC",
    "https://b2b.vtt.ru/catalog/?category=PARTSPRINT_THERELT",
]

# Константы под стиль AkCent
SUPPLIER_NAME = "VTT"
SUPPLIER_URL = BASE_URL
OFFER_PREFIX = "VT"
DEFAULT_AVAILABLE = "true"
DEFAULT_CURRENCY = "KZT"

CITY_TAIL = (
    "Казахстан, Алматы, Астана, Шымкент, Караганда, Актобе, Павлодар, Атырау, "
    "Тараз, Оскемен, Семей, Костанай, Кызылорда, Орал, Петропавловск, "
    "Талдыкорган, Актау, Темиртау, Экибастуз, Кокшетау"
)

WHATSAPP_HTML = (
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; "
    "border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);\">"
    "&#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!"
    "</a></p>"
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
    "<li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li>"
    "<li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>"
    "<li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li>"
    "</ul>"
    "</div></div>"
)


# -------------------- Время (Алматы) --------------------

def _tz_alm():
    if ZoneInfo:
        try:
            return ZoneInfo("Asia/Almaty")
        except Exception:
            pass
    return None


_TZ_ALM = _tz_alm()


def now_alm() -> datetime:
    if _TZ_ALM:
        return datetime.now(tz=_TZ_ALM)
    return datetime.utcnow().replace(tzinfo=None) + timedelta(hours=5)


def fmt_alm(dt: datetime) -> str:
    if getattr(dt, "tzinfo", None) is not None:
        dt = dt.astimezone(_TZ_ALM) if _TZ_ALM else dt.replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def yml_catalog_date() -> str:
    return now_alm().strftime("%Y-%m-%d %H:%M")


def next_1_10_20_at_05() -> datetime:
    n = now_alm()
    base = n
    candidates = []
    for d in (1, 10, 20):
        try:
            candidates.append(base.replace(day=d, hour=5, minute=0, second=0, microsecond=0))
        except ValueError:
            pass
    future = [t for t in candidates if t > n]
    if future:
        return min(future)
    y = base.year
    m = base.month + 1
    if m == 13:
        y, m = y + 1, 1
    if getattr(base, "tzinfo", None) is not None:
        return datetime(y, m, 1, 5, 0, 0, tzinfo=base.tzinfo)
    return datetime(y, m, 1, 5, 0, 0)


# -------------------- Утилиты --------------------

def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def jitter_sleep() -> None:
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


def abs_url(href: str) -> str:
    return urljoin(BASE_URL + "/", href)


def soup_of(content: bytes) -> BeautifulSoup:
    return BeautifulSoup(content, "html.parser")


def xml_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def safe_cdata_text(s: str) -> str:
    return (s or "").replace("]]>", "]]&gt;")


def parse_bool(s: str, default: bool = True) -> bool:
    if s is None:
        return default
    v = str(s).strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def parse_int_from_text(s: str) -> Optional[int]:
    if not s:
        return None
    norm = re.sub(r"[^\d.,]+", "", s).replace(",", ".")
    try:
        val = Decimal(norm)
    except InvalidOperation:
        return None
    return int(val)


def clean_article(article: str) -> str:
    return re.sub(r"[^\w\-]+", "", (article or "").strip())


def set_query_param(url: str, key: str, value: str) -> str:
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q[key] = value
    new_q = urlencode(q, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))


def category_code_from_url(url: str) -> str:
    q = dict(parse_qsl(urlparse(url).query, keep_blank_values=True))
    return (q.get("category") or "").strip()


def type_from_category(code: str) -> str:
    c = (code or "").upper()
    if c.startswith("CARTINJ"):
        return "Струйный картридж"
    if c.startswith("CARTLAS"):
        return "Лазерный картридж"
    if c.startswith("DRM"):
        return "Фотобарабан"
    if c.startswith("DEV"):
        return "Девелопер"
    if c.startswith("PARTSPRINT_THER"):
        return "Деталь термоблока"
    if c.startswith("CARTMAT"):
        return "Расходные материалы"
    return ""


# -------------------- Правило цены (НЕ меняем) --------------------

PRICING_RULES: List[Tuple[int, int]] = [
    (5_000, 1_000),
    (10_000, 1_500),
    (20_000, 2_000),
    (50_000, 3_000),
    (100_000, 4_000),
    (200_000, 6_000),
    (500_000, 10_000),
    (1_000_000, 15_000),
    (2_000_000, 25_000),
    (5_000_000, 50_000),
    (9_000_000, 75_000),
]


def round_to_900(x: Decimal) -> int:
    if x <= 0:
        return 100
    xi = int(x.to_integral_value(rounding="ROUND_CEILING"))
    thousands = xi // 1000
    target = thousands * 1000 + 900
    if target < xi:
        target = (thousands + 1) * 1000 + 900
    return int(target)


def compute_price_from_supplier(dealer_price: Optional[int]) -> int:
    if not dealer_price or dealer_price < 100:
        return 100
    p = Decimal(dealer_price) * Decimal("1.04")
    add = 0
    for limit, plus in PRICING_RULES:
        if dealer_price <= limit:
            add = plus
            break
    if add == 0:
        add = 100_000
    out = p + Decimal(add)
    if int(out) >= 9_000_000:
        return int((out / Decimal(100)).to_integral_value(rounding="ROUND_CEILING") * 100)
    return round_to_900(out)


# -------------------- Сеть и логин --------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.6",
            "Connection": "keep-alive",
        }
    )

    if VTT_CA_BUNDLE.strip():
        s.verify = VTT_CA_BUNDLE.strip()
    else:
        s.verify = parse_bool(VTT_SSL_VERIFY, True)

    if s.verify is False:
        print("[WARN] SSL verify disabled (VTT_SSL_VERIFY=0). Это небезопасно, но обходит CERT_VERIFY_FAILED.", file=sys.stderr)
        if urllib3 and InsecureRequestWarning:
            urllib3.disable_warnings(InsecureRequestWarning)

    return s


def http_get(s: requests.Session, url: str) -> Optional[bytes]:
    last_err = None
    for _ in range(MAX_RETRIES):
        try:
            jitter_sleep()
            r = s.get(url, timeout=HTTP_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {r.status_code}")
                time.sleep(0.8)
                continue
            r.raise_for_status()
            return r.content
        except Exception as e:
            last_err = e
            time.sleep(0.8)
    print(f"[WARN] GET failed: {url} | {last_err}", file=sys.stderr)
    return None


def http_post(s: requests.Session, url: str, data: Dict[str, str], headers: Dict[str, str]) -> bool:
    last_err = None
    for _ in range(MAX_RETRIES):
        try:
            jitter_sleep()
            r = s.post(url, data=data, headers=headers, timeout=HTTP_TIMEOUT)
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {r.status_code}")
                time.sleep(0.8)
                continue
            r.raise_for_status()
            return True
        except Exception as e:
            last_err = e
            time.sleep(0.8)
    print(f"[WARN] POST failed: {url} | {last_err}", file=sys.stderr)
    return False


def extract_csrf_token(html_bytes: bytes) -> str:
    soup = soup_of(html_bytes)
    m = soup.find("meta", attrs={"name": "csrf-token"})
    v = m.get("content") if m else ""
    return (v or "").strip()


def log_in(s: requests.Session) -> bool:
    if not LOGIN or not PASSWORD:
        print("[WARN] VTT_LOGIN/VTT_PASSWORD are empty", file=sys.stderr)
        return False

    home = http_get(s, BASE_URL + "/")
    if not home:
        return False

    csrf = extract_csrf_token(home)
    headers = {"Referer": BASE_URL + "/"}
    if csrf:
        headers["X-CSRF-TOKEN"] = csrf

    ok = http_post(
        s,
        BASE_URL + "/validateLogin",
        data={"login": LOGIN, "password": PASSWORD},
        headers=headers,
    )
    if not ok:
        return False

    cat = http_get(s, START_URL)
    return bool(cat)


# -------------------- Парсинг --------------------

def normalize_vendor(v: str) -> str:
    """Бренд (НЕ трогаем, как просил)."""
    v = (v or "").strip()
    if not v:
        return ""
    key = re.sub(r"[^\w]+", "", v, flags=re.IGNORECASE).lower()
    alias = {
        "kyoceramita": "Kyocera",
        "samsungbyhp": "Samsung",
    }
    return alias.get(key, v)


def parse_pairs(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}
    box = soup.select_one("div.description.catalog_item_descr")
    if not box:
        return out
    dts = box.find_all("dt")
    dds = box.find_all("dd")
    for dt, dd in zip(dts, dds):
        k = (dt.get_text(" ", strip=True) or "").strip().strip(":")
        v = (dd.get_text(" ", strip=True) or "").strip()
        if k:
            out[k] = v
    return out


def parse_supplier_price(soup: BeautifulSoup) -> Optional[int]:
    """Цена (НЕ меняем)."""
    b = soup.select_one("span.price_main b")
    if not b:
        return None
    return parse_int_from_text(b.get_text(" ", strip=True))


def first_image_url(soup: BeautifulSoup) -> Optional[str]:
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return abs_url(str(og["content"]))
    for tag in soup.find_all(True):
        for attr in ("src", "data-src", "href", "data-img", "content", "data-large"):
            v = tag.get(attr)
            if not v or not isinstance(v, str):
                continue
            vl = v.lower()
            if (".jpg" in vl or ".jpeg" in vl or ".png" in vl) and "/images/" in vl:
                return abs_url(v)
    return None


def get_title(soup: BeautifulSoup) -> str:
    el = soup.select_one(".page_title") or soup.title or soup.find("h1")
    txt = el.get_text(" ", strip=True) if el else ""
    return txt.strip()


def get_meta_description(soup: BeautifulSoup) -> str:
    meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    out = (meta.get("content") if meta else "") or ""
    out = re.sub(r"\s+", " ", out).strip()
    return out


def split_tokens(text: str, limit: int = 14) -> List[str]:
    raw = re.split(r"[^0-9A-Za-zА-Яа-яЁё]+", text or "")
    out = []
    seen = set()
    for t in raw:
        t = t.strip()
        if not t:
            continue
        if t.lower() in seen:
            continue
        seen.add(t.lower())
        out.append(t)
        if len(out) >= limit:
            break
    return out


_CYR = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
_LAT = ["a","b","v","g","d","e","e","zh","z","i","y","k","l","m","n","o","p","r","s","t","u","f","h","ts","ch","sh","sch","","y","","e","yu","ya"]
_TR = {c: l for c, l in zip(_CYR, _LAT)}
_TR.update({c.upper(): l for c, l in zip(_CYR, _LAT)})


def translit_ru(s: str) -> str:
    return "".join(_TR.get(ch, ch) for ch in (s or ""))


def slugify(s: str) -> str:
    s = translit_ru(s).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def make_keywords(vendor: str, name: str) -> str:
    tokens = split_tokens(name, limit=12)
    slug_full = slugify(name)
    slug_2 = slugify(" ".join(tokens[:2])) if len(tokens) >= 2 else slug_full
    slug_3 = slugify(" ".join(tokens[:3])) if len(tokens) >= 3 else slug_full
    base = []
    if vendor:
        base.append(vendor)
    base.append(name)
    base.extend(tokens)
    if vendor and tokens:
        base.append(f"{vendor} {tokens[-1]}")
    if slug_2:
        base.append(slug_2)
    if slug_3 and slug_3 != slug_2:
        base.append(slug_3)
    if slug_full and slug_full not in (slug_2, slug_3):
        base.append(slug_full)
    base.append(CITY_TAIL)
    return ", ".join([x.strip() for x in base if x and x.strip()])


# -------------------- Нормализация/обогащение характеристик --------------------

DROP_KEYS = {
    "Артикул",
    "Партс-номер",
    "Вендор",

    "Категория",
    "Подкатегория",
    "Штрих-код",
    "Штрихкод",
    "EAN",
    "Barcode",
    "GTIN",

    "Каталожный номер",
    "Каталожный №",
    "Каталожный N",
    "OEM-номер",
    "OEM номер",
    "OEM No",
    "OEM",
}

def _norm_key(k: str) -> str:
    s = (k or "").strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"[\s\-\._]+", "", s)
    return s

_DROP_NORM = {_norm_key(x) for x in DROP_KEYS}

def should_drop_key(k: str) -> bool:
    return _norm_key(k) in _DROP_NORM


_COLOR_MAP = {
    "black": "Черный",
    "cyan": "Голубой",
    "magenta": "Пурпурный",
    "yellow": "Желтый",
    "grey": "Серый",
    "gray": "Серый",
    "white": "Белый",
    "red": "Красный",
    "blue": "Синий",
    "green": "Зеленый",
    "orange": "Оранжевый",
    "brown": "Коричневый",
    "pink": "Розовый",
    "violet": "Фиолетовый",
    "purple": "Фиолетовый",
}

def normalize_color_value(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return s
    parts = [p.strip() for p in re.split(r"[,/;]+", s) if p.strip()]
    out = []
    for p in parts:
        low = p.lower().strip()
        low = re.sub(r"\b(color|colour)\b", "", low).strip()
        ru = None
        if low in _COLOR_MAP:
            ru = _COLOR_MAP[low]
        else:
            for k, ruv in _COLOR_MAP.items():
                if re.search(rf"\b{k}\b", low):
                    ru = ruv
                    break
        has_cyr = bool(re.search(r"[А-Яа-яЁё]", p))
        if ru and not has_cyr:
            out.append(f"{ru} ({p})")
        else:
            out.append(p)
    return ", ".join(out)

def normalize_resource_value(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return s

    # 1) 1,1К / 7K
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*[kк]\b", s, flags=re.IGNORECASE)
    if m:
        num = m.group(1).replace(",", ".")
        try:
            pages = int((Decimal(num) * Decimal(1000)).to_integral_value(rounding="ROUND_HALF_UP"))
            return f"{pages} стр."
        except Exception:
            pass

    # 2) "1234 стр" / "1234 pages"
    m2 = re.search(r"(\d[\d\s]*)\s*(?:стр\.?|страниц|pages?|p\.)", s, flags=re.IGNORECASE)
    if m2:
        digits = re.sub(r"\s+", "", m2.group(1))
        try:
            pages = int(digits)
            return f"{pages} стр."
        except Exception:
            pass

    # 3) просто число
    m3 = re.search(r"(\d[\d\s]*)", s)
    if m3:
        digits = re.sub(r"\s+", "", m3.group(1))
        try:
            pages = int(digits)
            return f"{pages} стр."
        except Exception:
            pass

    return s

def extract_from_name(name: str) -> Dict[str, str]:
    n = name or ""
    out: Dict[str, str] = {}

    # объем
    m_ml = re.search(r"(\d+(?:[.,]\d+)?)\s*мл\b", n, flags=re.IGNORECASE)
    if m_ml:
        x = m_ml.group(1).replace(",", ".")
        out["Объем"] = f"{x.rstrip('0').rstrip('.') if '.' in x else x} мл"

    # цвет (по англ словам)
    lows = n.lower()
    found = []
    for k in _COLOR_MAP.keys():
        if re.search(rf"\b{k}\b", lows):
            found.append(k)
    if found:
        uniq = []
        for k in found:
            if k not in uniq:
                uniq.append(k)
        out["Цвет"] = ", ".join([f"{_COLOR_MAP[k]} ({k.title()})" for k in uniq])

    # ресурс
    m_k = re.search(r"(\d+(?:[.,]\d+)?)\s*[kк]\b", n, flags=re.IGNORECASE)
    if m_k:
        out["Ресурс"] = normalize_resource_value(m_k.group(0))
    else:
        m_pages = re.search(r"(\d[\d\s]*)\s*(?:стр\.?|страниц|pages?)", n, flags=re.IGNORECASE)
        if m_pages:
            out["Ресурс"] = normalize_resource_value(m_pages.group(0))

    return out

def parse_product_text(soup: BeautifulSoup) -> str:
    selectors = [
        "div.catalog_item_text",
        "div.catalog_item_descr_text",
        "div.catalog_item_descr",
        "div.item_desc",
        "div.description_text",
        "div.tab-content",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            t = el.get_text("\n", strip=True)
            t = re.sub(r"[ \t]+", " ", t)
            t = re.sub(r"\n{3,}", "\n\n", t)
            if len(t) >= 40:
                return t
    return ""

def extract_compatibility(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    # Совместимость: ...
    m = re.search(r"(?:Подходит\s*для|Совместим(?:ость)?|Для\s*принтеров)\s*[:\-]\s*(.+)", t, flags=re.IGNORECASE)
    if not m:
        return ""
    val = m.group(1).strip()
    val = re.sub(r"\s+", " ", val)
    # чуть чистим хвост
    val = re.split(r"(?:Гарантия|Условия|Доставка)\s*[:\-]", val, maxsplit=1, flags=re.IGNORECASE)[0].strip()
    if 10 <= len(val) <= 220:
        return val
    return ""

def normalize_characteristics(pairs: Dict[str, str], name: str, type_hint: str, soup: BeautifulSoup) -> List[Tuple[str, str]]:
    """Один общий набор характеристик:
    - чистим мусорные поля (в т.ч. Каталожный номер / OEM-номер / Штрих-код / Категория)
    - нормализуем значения (цвет/ресурс)
    - добираем из name (цвет/мл/ресурс)
    - добираем из описания на странице (совместимость, если присутствует)
    - добавляем "Тип" по категории (если нет)
    """
    cleaned: Dict[str, str] = {}

    # 1) dt/dd
    for k, v in (pairs or {}).items():
        if not k or not v:
            continue
        k = k.strip()
        if not k or should_drop_key(k):
            continue

        vv = (v or "").strip()
        if not vv:
            continue

        kl = k.lower()
        if "цвет" in kl:
            vv = normalize_color_value(vv)
        if "ресурс" in kl:
            vv = normalize_resource_value(vv)

        cleaned[k] = vv

    # 2) из name
    extra = extract_from_name(name or "")

    has_color = any("цвет" in k.lower() for k in cleaned.keys())
    if (not has_color) and extra.get("Цвет"):
        cleaned["Цвет"] = extra["Цвет"]

    has_ml = any(("мл" in (v or "").lower()) or ("объем" in k.lower()) for k, v in cleaned.items())
    if (not has_ml) and extra.get("Объем"):
        cleaned["Объем"] = extra["Объем"]

    has_resource = any("ресурс" in k.lower() for k in cleaned.keys())
    if (not has_resource) and extra.get("Ресурс"):
        cleaned["Ресурс"] = extra["Ресурс"]

    # 3) тип по категории
    if type_hint:
        has_type = any(_norm_key(k) == _norm_key("Тип") for k in cleaned.keys())
        if not has_type:
            cleaned["Тип"] = type_hint

    # 4) из описания (если есть)
    text = parse_product_text(soup)
    compat = extract_compatibility(text)
    if compat:
        cleaned.setdefault("Совместимость", compat)

    out: List[Tuple[str, str]] = []
    for k, v in cleaned.items():
        if not k or not v:
            continue
        if should_drop_key(k):
            continue
        out.append((k, v))

    out.sort(key=lambda x: x[0].lower())
    return out


# -------------------- Описание --------------------

def build_description_cdata(name: str, short_desc: str, characteristics: List[Tuple[str, str]]) -> str:
    name_e = (name or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    desc_e = (short_desc or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    items = []
    for k, v in characteristics:
        if not k or not v:
            continue
        kk = k.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        vv = v.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        items.append((f"<strong>{kk}:</strong>", vv))

    if items:
        li = "".join([f"<li>{kk} {vv}</li>" for kk, vv in items])
        specs = f"<h3>Характеристики</h3><ul>{li}</ul>"
    else:
        specs = "<h3>Характеристики</h3><ul></ul>"

    parts = [
        "",
        "<!-- WhatsApp -->",
        WHATSAPP_HTML,
        "",
        "<!-- Описание -->",
        f"<h3>{name_e}</h3><p>{desc_e}</p>",
        specs,
        "",
    ]
    return safe_cdata_text("\n".join(parts))


@dataclass
class Offer:
    id: str
    vendorCode: str
    name: str
    price: int
    picture: str
    vendor: str
    description_cdata: str
    params: List[Tuple[str, str]]
    keywords: str


# -------------------- Сбор ссылок --------------------

_PRODUCT_HREF_RE = re.compile(r"^/catalog/[^?]+/?$")


def collect_product_links(s: requests.Session, category_url: str) -> List[str]:
    found: List[str] = []
    seen: set[str] = set()

    for page in range(1, MAX_PAGES + 1):
        page_url = set_query_param(category_url, "page", str(page))
        b = http_get(s, page_url)
        if not b:
            break
        soup = soup_of(b)

        links: List[str] = []
        for a in soup.find_all("a", href=True):
            cls = a.get("class") or []
            if isinstance(cls, list) and "btn_pic" in cls:
                continue

            href = str(a["href"]).strip()
            if not href or href.startswith("#"):
                continue

            if href.startswith("http"):
                p = urlparse(href)
                if p.netloc and p.netloc != urlparse(BASE_URL).netloc:
                    continue
                href = p.path

            if "category=" in href or "page=" in href:
                continue
            if not _PRODUCT_HREF_RE.match(href):
                continue

            u = abs_url(href)
            if u in seen:
                continue
            seen.add(u)
            links.append(u)

        if not links:
            break

        found.extend(links)

    return found


def collect_all_products_links(s: requests.Session) -> List[Tuple[str, str]]:
    all_links: List[Tuple[str, str]] = []
    seen: set[str] = set()
    for cat_url in CATEGORIES:
        code = category_code_from_url(cat_url)
        links = collect_product_links(s, cat_url)
        for u in links:
            if u in seen:
                continue
            seen.add(u)
            all_links.append((u, code))
    return all_links


# -------------------- Парс товара --------------------

def parse_product(s: requests.Session, url: str, cat_code: str) -> Optional[Offer]:
    b = http_get(s, url)
    if not b:
        return None
    soup = soup_of(b)

    name = get_title(soup)
    if not name:
        return None

    pairs = parse_pairs(soup)
    article = (pairs.get("Артикул") or pairs.get("Партс-номер") or "").strip()
    if not article:
        return None

    vendor = normalize_vendor((pairs.get("Вендор") or "").strip())
    dealer_price = parse_supplier_price(soup)
    price = compute_price_from_supplier(dealer_price)

    picture = first_image_url(soup) or ""
    short_desc = get_meta_description(soup) or name

    article_clean = clean_article(article)
    if not article_clean:
        return None

    offer_id = OFFER_PREFIX + article_clean

    type_hint = type_from_category(cat_code)
    params = normalize_characteristics(pairs, name, type_hint, soup)

    descr_cdata = build_description_cdata(
        name=name,
        short_desc=short_desc,
        characteristics=params,
    )
    keywords = make_keywords(vendor=vendor, name=name)

    return Offer(
        id=offer_id,
        vendorCode=offer_id,
        name=name,
        price=int(price),
        picture=picture,
        vendor=vendor,
        description_cdata=descr_cdata,
        params=params,
        keywords=keywords,
    )


# -------------------- FEED_META --------------------

def render_feed_meta(offers_total: int, offers_written: int, avail_true: int, avail_false: int) -> str:
    rows = [
        ("Поставщик", SUPPLIER_NAME),
        ("URL поставщика", SUPPLIER_URL),
        ("Время сборки (Алматы)", fmt_alm(now_alm())),
        ("Ближайшая сборка (Алматы)", fmt_alm(next_1_10_20_at_05())),
        ("Сколько товаров у поставщика до фильтра", str(offers_total)),
        ("Сколько товаров у поставщика после фильтра", str(offers_written)),
        ("Сколько товаров есть в наличии (true)", str(avail_true)),
        ("Сколько товаров нет в наличии (false)", str(avail_false)),
    ]
    key_w = max(len(k) for k, _ in rows)
    lines = ["<!--FEED_META"]
    for k, v in rows:
        lines.append(f"{k.ljust(key_w)} | {v}")
    lines.append("-->")
    return "\n".join(lines)


# -------------------- Рендер YML (как AkCent) --------------------

def offer_to_xml(o: Offer) -> str:
    lines: List[str] = []
    lines.append(f"<offer id=\"{xml_escape(o.id)}\" available=\"{DEFAULT_AVAILABLE}\">")
    lines.append("<categoryId></categoryId>")
    lines.append(f"<vendorCode>{xml_escape(o.vendorCode)}</vendorCode>")
    lines.append(f"<name>{xml_escape(o.name)}</name>")
    lines.append(f"<price>{int(o.price)}</price>")
    if o.picture:
        lines.append(f"<picture>{xml_escape(o.picture)}</picture>")
    if o.vendor:
        lines.append(f"<vendor>{xml_escape(o.vendor)}</vendor>")
    lines.append(f"<currencyId>{DEFAULT_CURRENCY}</currencyId>")

    lines.append("<description><![CDATA[")
    lines.append(o.description_cdata)
    lines.append("]]></description>")

    for k, v in o.params:
        if not k or not v:
            continue
        lines.append(f"<param name=\"{xml_escape(k)}\">{xml_escape(v)}</param>")

    if o.keywords:
        lines.append(f"<keywords>{xml_escape(o.keywords)}</keywords>")

    lines.append("</offer>")
    return "\n".join(lines)


def build_yml(offers: List[Offer], offers_total: int) -> str:
    avail_true = len(offers)
    avail_false = 0

    head = [
        "<?xml version=\"1.0\" encoding=\"windows-1251\"?>",
        "<!DOCTYPE yml_catalog SYSTEM \"shops.dtd\">",
        f"<yml_catalog date=\"{yml_catalog_date()}\">",
        "<shop><offers>",
        "",
        render_feed_meta(
            offers_total=offers_total,
            offers_written=len(offers),
            avail_true=avail_true,
            avail_false=avail_false,
        ),
        "",
    ]

    body: List[str] = []
    for o in offers:
        body.append(offer_to_xml(o))
        body.append("")

    tail = [
        "</offers>",
        "</shop>",
        "</yml_catalog>",
    ]

    return "\n".join(head + body + tail)


# -------------------- Пустой файл --------------------

def write_empty_yml(reason: str) -> None:
    ensure_dir(OUT_FILE)
    meta = render_feed_meta(offers_total=0, offers_written=0, avail_true=0, avail_false=0)
    xml = "\n".join(
        [
            "<?xml version=\"1.0\" encoding=\"windows-1251\"?>",
            "<!DOCTYPE yml_catalog SYSTEM \"shops.dtd\">",
            f"<yml_catalog date=\"{yml_catalog_date()}\">",
            "<shop><offers>",
            "",
            meta,
            "",
            "</offers>",
            "</shop>",
            "</yml_catalog>",
            "",
        ]
    )
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="xmlcharrefreplace", newline="\n") as f:
        f.write(xml)
    print(f"[WARN] wrote empty feed: {OUT_FILE} | {reason}")


# -------------------- Main --------------------

def main() -> int:
    s = make_session()

    if not log_in(s):
        write_empty_yml("login_failed")
        return 0

    product_urls = collect_all_products_links(s)
    offers_total = len(product_urls)

    if offers_total == 0:
        write_empty_yml("no_products_found")
        return 0

    offers: List[Offer] = []
    seen_ids: set[str] = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(parse_product, s, u, code): (u, code) for (u, code) in product_urls}
        for fut in as_completed(futs):
            url, code = futs[fut]
            try:
                o = fut.result()
            except Exception as e:
                print(f"[WARN] parse crash: {url} | {e}", file=sys.stderr)
                continue
            if not o:
                continue

            # (на всякий) уникальность id
            if o.id in seen_ids:
                suf = re.sub(r"[^0-9a-f]+", "", format(abs(hash(url)) % (16**6), "06x"))
                oid2 = f"{o.id}-{suf}"
                o = Offer(
                    id=oid2,
                    vendorCode=oid2,
                    name=o.name,
                    price=o.price,
                    picture=o.picture,
                    vendor=o.vendor,
                    description_cdata=o.description_cdata,
                    params=o.params,
                    keywords=o.keywords,
                )
            seen_ids.add(o.id)
            offers.append(o)

    offers.sort(key=lambda x: x.id)

    ensure_dir(OUT_FILE)
    xml = build_yml(offers=offers, offers_total=offers_total)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="xmlcharrefreplace", newline="\n") as f:
        f.write(xml)

    print(f"Wrote: {OUT_FILE} | encoding={OUTPUT_ENCODING} | offers={len(offers)} (from {offers_total})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
