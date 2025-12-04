from __future__ import annotations
import os, re, io, time, html, hashlib, random
from typing import Any, Dict, List, Optional, Tuple, Set, NamedTuple
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

BASE_URL            = "https://copyline.kz"
XLSX_URL            = os.getenv("XLSX_URL", f"{BASE_URL}/files/price-CLA.xlsx")
KEYWORDS_FILE       = os.getenv("KEYWORDS_FILE", "docs/copyline_keywords.txt")
OUT_FILE            = os.getenv("OUT_FILE", "docs/copyline.yml")

ENC                 = (os.getenv("OUTPUT_ENCODING", "windows-1251") or "").lower()
FILE_ENCODING       = "cp1251" if "1251" in ENC else (ENC or "utf-8")
XML_ENCODING        = "windows-1251" if "1251" in ENC else (ENC or "utf-8")

HTTP_TIMEOUT        = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS    = int(os.getenv("REQUEST_DELAY_MS", "120"))
MIN_BYTES           = int(os.getenv("MIN_BYTES", "900"))

MAX_CRAWL_MINUTES   = int(os.getenv("MAX_CRAWL_MINUTES", "60"))
MAX_CATEGORY_PAGES  = int(os.getenv("MAX_CATEGORY_PAGES", "1200"))
MAX_WORKERS         = int(os.getenv("MAX_WORKERS", "6"))

SUPPLIER_NAME       = "Copyline"
CURRENCY            = "KZT"
VENDORCODE_PREFIX   = os.getenv("VENDORCODE_PREFIX", "CL")

UA = {"User-Agent": "Mozilla/5.0 (compatible; Copyline-XLSX-Site/3.0)"}

BLOCK_SUPPLIER_BRANDS = {"copyline", "alstyle", "vtt"}

BRAND_ALIASES = {
    "hp": "HP", "hewlettpackard": "HP",
    "canon": "Canon", "xerox": "Xerox", "brother": "Brother",
    "kyocera": "Kyocera", "ricoh": "Ricoh", "konicaminolta": "Konica Minolta",
    "epson": "Epson", "samsung": "Samsung", "lexmark": "Lexmark",
    "panasonic": "Panasonic", "sharp": "Sharp", "oki": "OKI", "toshiba": "Toshiba",
    "dell": "Dell",
    "europrint": "Euro Print", "euro print": "Euro Print",
    "nvprint": "NV Print", "nv print": "NV Print",
    "hiblack": "Hi-Black", "hi-black": "Hi-Black", "hi black": "Hi-Black",
    "profiline": "ProfiLine", "profi line": "ProfiLine",
    "staticcontrol": "Static Control", "static control": "Static Control",
    "gg": "G&G", "g&g": "G&G",
    "cactus": "Cactus", "patron": "Patron", "pitatel": "Pitatel",
    "mito": "Mito", "7q": "7Q", "uniton": "Uniton", "printpro": "PrintPro",
    "sakura": "Sakura",
    "magnetone": "MAGNETONE", "magnet one": "MAGNETONE", "magne tone": "MAGNETONE",
}

OEM_PRIORITY = [
    "HP","Canon","Xerox","Brother","Kyocera","Ricoh","Konica Minolta",
    "Epson","Samsung","Lexmark","Panasonic","Sharp","OKI","Toshiba","Dell",
]

AFTERMARKET_PRIORITY = [
    "Euro Print","NV Print","Hi-Black","ProfiLine","Static Control","G&G",
    "Cactus","Patron","Pitatel","Mito","7Q","Uniton","PrintPro","Sakura","MAGNETONE",
]

STOPWORDS_BRAND = {
    "картридж","тонер","драм","фотобарабан","узел","термоблок","девелопер","порошок",
    "бумага","ремкомплект","для","без","с","набор","черный","чёрный","цветной",
    "лазерный","струйный","принтер","мфу","ресурс","оригинальный","совместимый",
    "cartridge","toner","drum","developer","fuser","kit","unit","laser","inkjet",
}


def jitter_sleep(ms: int) -> None:
    time.sleep(max(0.0, ms/1000.0) * (1 + random.uniform(-0.15, 0.15)))


def http_get(url: str, tries: int = 3) -> Optional[bytes]:
    delay = max(0.05, REQUEST_DELAY_MS / 1000.0)
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, headers=UA, timeout=HTTP_TIMEOUT)
            if r.status_code == 200 and (len(r.content) >= MIN_BYTES if url.endswith(".xlsx") else True):
                return r.content
            last = f"http {r.status_code} size={len(r.content)}"
        except Exception as e:
            last = repr(e)
        time.sleep(delay); delay *= 1.6
    return None


def soup_of(b: bytes) -> BeautifulSoup: return BeautifulSoup(b, "html.parser")


def yml_escape(s: str) -> str: return html.escape(s or "")


def norm_ascii(s: str) -> str: return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def title_clean(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\s*\((?:Артикул|SKU|Код)\s*[:#]?\s*[^)]+\)\s*$", "", s, flags=re.I)
    return re.sub(r"\s{2,}", " ", s).strip()[:200]


def to_number(x: Any) -> Optional[float]:
    if x is None: return None
    s = str(x).replace("\xa0"," ").strip().replace(" ", "").replace(",", ".")
    if not re.search(r"\d", s): return None
    try: return float(s)
    except Exception:
        m = re.search(r"[\d.]+", s)
        return float(m.group(0)) if m else None

KEYWORD_TERMS = [
    "drum",
    "девелопер",
    "драм",
    "кабель сетевой",
    "картридж",
    "термоблок",
    "термоэлемент",
    "тонер-картридж",
]


def load_keywords(path: str) -> List[str]:
    out: List[str] = []
    for kw in KEYWORD_TERMS:
        kw = kw.strip()
        if not kw or kw.startswith("#"):
            continue
        out.append(kw)
    return out


def compile_startswith_patterns(kws: List[str]) -> List[re.Pattern]:
    return [re.compile(r"^\s*"+re.escape(kw).replace(r"\ "," ")+r"(?!\w)", re.I) for kw in kws]


def title_startswith_strict(title: str, patterns: List[re.Pattern]) -> bool:
    return bool(title) and any(p.search(title) for p in patterns)


def fetch_xlsx_bytes(url: str) -> bytes:
    b = http_get(url, tries=3)
    if not b: raise RuntimeError("Не удалось скачать XLSX.")
    return b


def detect_header_two_row(rows: List[List[Any]], scan_rows: int = 60):
    def low(x): return str(x or "").strip().lower()
    for i in range(min(scan_rows, len(rows)-1)):
        row0 = [low(c) for c in rows[i]]
        row1 = [low(c) for c in rows[i+1]]
        if any("номенклатура" in c for c in row0):
            name_col  = next((j for j,c in enumerate(row0) if "номенклатура" in c), None)
            vendor_col= next((j for j,c in enumerate(row1) if "артикул" in c), None)
            price_col = next((j for j,c in enumerate(row1) if "цена" in c or "опт" in c), None)
            if name_col is not None and vendor_col is not None and price_col is not None:
                return i, i+1, {"name": name_col, "vendor_code": vendor_col, "price": price_col}
    return -1, -1, {}

PRODUCT_RE = re.compile(r"/goods/[^/]+\.html$")


def normalize_img_to_full(url: Optional[str]) -> Optional[str]:
    if not url: return None
    u = url.strip()
    if u.startswith("//"): u = "https:"+u
    if u.startswith("/"):  u = BASE_URL+u
    m = re.match(r"^(https?://[^/]+)(/.*/)([^/]+)$", u)
    if not m: return u
    host, path, fname = m.groups()
    if not fname.startswith("full_"):
        fname = "full_"+fname.replace("thumb_","")
    return f"{host}{path}{fname}"


def extract_specs_and_text(block: BeautifulSoup) -> Tuple[str, Dict[str,str]]:
    parts, specs, kv = [], [], {}
    for ch in block.find_all(["p","h3","h4","h5","ul","ol"], recursive=False):
        tag = ch.name.lower()
        if tag in {"p","h3","h4","h5"}:
            t = re.sub(r"\s+"," ", ch.get_text(" ", strip=True)).strip()
            if t: parts.append(t)
        elif tag in {"ul","ol"}:
            for li in ch.find_all("li", recursive=False):
                t = re.sub(r"\s+"," ", li.get_text(" ", strip=True)).strip()
                if t: parts.append("- "+t)
    for tbl in block.find_all("table"):
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["th","td"])
            if len(cells) >= 2:
                k = re.sub(r"\s+"," ", cells[0].get_text(" ", strip=True)).strip()
                v = re.sub(r"\s+"," ", cells[1].get_text(" ", strip=True)).strip()
                if k and v:
                    specs.append(f"- {k}: {v}")
                    kv[k.strip().lower()] = v.strip()
    if specs and not any("технические характеристики" in p.lower() for p in parts):
        parts.append("Технические характеристики:")
    parts.extend(specs)
    return "\n".join([p for p in parts if p]).strip(), kv


def extract_brand_from_specs_kv(kv: Dict[str,str]) -> Optional[str]:
    for k, v in kv.items():
        if k.strip().lower() in {"производитель","бренд","торговая марка","brand","manufacturer"} and v.strip():
            return v.strip()
    return None


def collect_brand_candidates(text: str) -> List[str]:
    if not text: return []
    hay = text.lower()
    found: List[str] = []
    for norm, display in BRAND_ALIASES.items():
        if norm in BLOCK_SUPPLIER_BRANDS:
            continue
        if norm in norm_ascii(hay) or display.lower() in hay:
            if display not in found:
                found.append(display)
    return found


def choose_brand_oem_first(candidates: List[str]) -> Optional[str]:
    if not candidates:
        return None
    for oem in OEM_PRIORITY:
        if oem in candidates:
            return oem
    for am in AFTERMARKET_PRIORITY:
        if am in candidates:
            return am
    return candidates[0]


def sanitize_brand(b: Optional[str]) -> Optional[str]:
    if not b: return None
    out = BRAND_ALIASES.get(norm_ascii(b), re.sub(r"\s{2,}"," ", b).strip())
    return None if norm_ascii(out) in BLOCK_SUPPLIER_BRANDS else out


def brand_soft_fallback(title: str, desc: str) -> Optional[str]:
    text = f"{title or ''} {desc or ''}"
    words = re.findall(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё\-]{1,20}", text)
    for i in range(len(words)-1):
        pair = f"{words[i]} {words[i+1]}"; n = norm_ascii(pair)
        out = BRAND_ALIASES.get(n)
        if out and norm_ascii(out) not in BLOCK_SUPPLIER_BRANDS:
            return out
    for w in words:
        n = norm_ascii(w)
        out = BRAND_ALIASES.get(n)
        if out and norm_ascii(out) not in BLOCK_SUPPLIER_BRANDS:
            return out
    return None

class PriceRule(NamedTuple):
    lo: int; hi: int; pct: float; add: int
PRICING_RULES: List[PriceRule] = [
    PriceRule(   101,    10000, 4.0,  3000),
    PriceRule( 10001,    25000, 4.0,  4000),
    PriceRule( 25001,    50000, 4.0,  5000),
    PriceRule( 50001,    75000, 4.0,  7000),
    PriceRule( 75001,   100000, 4.0, 10000),
    PriceRule(100001,   150000, 4.0, 12000),
    PriceRule(150001,   200000, 4.0, 15000),
    PriceRule(200001,   300000, 4.0, 20000),
    PriceRule(300001,   400000, 4.0, 25000),
    PriceRule(400001,   500000, 4.0, 30000),
    PriceRule(500001,   750000, 4.0, 40000),
    PriceRule(750001,  1000000, 4.0, 50000),
    PriceRule(1000001, 1500000, 4.0, 70000),
    PriceRule(1500001, 2000000, 4.0, 90000),
    PriceRule(2000001,100000000,4.0,100000),
]


def _parse_float(value: str) -> float:
    value = (value or "").strip().replace(" ", "").replace(",", ".")
    if not value:
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def _calc_price(purchase_raw: str, supplier_raw: str) -> int:
    purchase = _parse_float(purchase_raw)
    supplier_price = _parse_float(supplier_raw)

    base = 0.0
    if purchase > 0:
        base = purchase
    elif supplier_price > 0:
        base = supplier_price
    else:
        return 100

    base_int = int(base)
    if base_int <= 0:
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
        if lo <= base_int <= hi:
            bonus = add
            break

    if bonus == 0:
        value = base_int * 1.04
    else:
        value = base_int * 1.04 + bonus

    thousands = int(value) // 1000
    price = thousands * 1000 + 900
    if price < value:
        price += 1000

    if price >= 9_000_000:
        return 100

    return int(price)


def _force_tail_900(n: float) -> int:
    i = int(n)
    k = max(i // 1000, 0)
    out = k * 1000 + 900
    return out if out >= 900 else 900


def compute_retail(dealer: float) -> Optional[int]:
    if dealer is None or dealer <= 0:
        return None
    return _calc_price("", str(dealer))


def _next_build_time_almaty_1_10_20_03() -> datetime:
    tz = ZoneInfo("Asia/Almaty") if ZoneInfo else None
    now = datetime.now(tz) if tz else datetime.utcnow()
    targets = [1, 10, 20]
    y, m, d = now.year, now.month, now.day
    cand_list: List[datetime] = []
    for day in targets:
        cand_list.append(datetime(y, m, day, 3, 0, 0, tzinfo=tz))
    future = [t for t in cand_list if t >= now]
    if future:
        return min(future)
    if m == 12:
        y2, m2 = y+1, 1
    else:
        y2, m2 = y, m+1
    return datetime(y2, m2, 1, 3, 0, 0, tzinfo=tz)


def _fmt_dt_alm(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def render_feed_meta_for_copyline(pairs: Dict[str, str]) -> str:
    tz = ZoneInfo("Asia/Almaty") if ZoneInfo else None
    now_alm = datetime.now(tz) if tz else datetime.utcnow()
    next_alm = _next_build_time_almaty_1_10_20_03()

    rows = [
        ("Поставщик", pairs.get("supplier","")),
        ("URL поставщика", pairs.get("source","")),
        ("Время сборки (Алматы)", _fmt_dt_alm(now_alm)),
        ("Ближайшая сборка (Алматы)", _fmt_dt_alm(next_alm)),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total","0"))),
        ("Сколько товаров у поставщика после фильтра", str(pairs.get("offers_written","0"))),
        ("Сколько товаров есть в наличии (true)", str(pairs.get("available_true","0"))),
        ("Сколько товаров нет в наличии (false)", str(pairs.get("available_false","0"))),
    ]
    key_w = max(len(k) for k,_ in rows)
    lines = ["<!--FEED_META"]
    for (k, v) in rows:
        lines.append(f"{k.ljust(key_w)} | {v}")
    lines.append("-->")
    return "\n".join(lines)

ART_PATTS = [
    re.compile(r"\(\s*Артикул\s*[:#]?\s*[A-Za-z0-9\-\._/]+\s*\)", re.IGNORECASE),
    re.compile(r"\bАртикул\s*[:#]?\s*[A-Za-z0-9\-\._/]+", re.IGNORECASE),
]


def clean_article_mentions(text: str) -> str:
    if not text: return text
    out = text
    for rx in ART_PATTS:
        out = rx.sub("", out)
    out = re.sub(r"\(\s*\)", "", out)
    out = re.sub(r"[ \t]{2,}", " ", out)
    out = re.sub(r"[ \t]+\n", "\n", out)
    out = re.sub(r"(\n\s*){3,}", "\n\n", out)
    return out.strip()


def _xml_escape_text(s: str) -> str:
    if not s:
        return ""
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )

_re_ws_norm = re.compile(r"\s+", re.U)


def _normalize_description_text(text: str) -> str:
    if not text:
        return ""
    lines = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        lines.append(s)
    if not lines:
        return ""
    joined = " ".join(lines)
    joined = _re_ws_norm.sub(" ", joined)
    return joined.strip()

GOAL = 1000
GOAL_LOW = 900
MAX_HARD = 1200


def _build_desc_text(plain: str) -> str:
    if len(plain) <= GOAL:
        return plain

    parts = re.split(r"(?<=[\.!?])\s+|;\s+", plain)
    parts = [p.strip() for p in parts if p.strip()]

    if not parts:
        return plain[:GOAL]

    selected: List[str] = []
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
    "Темиртау",
    "Актау",
    "Туркестан",
    "Талдыкорган",
    "Экибастуз",
    "Жезказган",
    "Рудный",
    "Балхаш",
    "Жанаозен",
    "Кокшетау",
]


def _translit_to_slug(text: str) -> str:
    mapping = {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
        "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i",
        "й": "y", "к": "k", "л": "l", "м": "m", "н": "n",
        "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
        "у": "u", "ф": "f", "х": "h", "ц": "c", "ч": "ch",
        "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "",
        "э": "e", "ю": "yu", "я": "ya",
    }
    text = (text or "").lower()
    res: List[str] = []
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
    parts: List[str] = []
    seen: Set[str] = set()

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

    tokens = re.split(r"[\s,;:!\?()\[\]/\+]+", name)
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
        out: List[str] = []
        length = 0
        for p in parts:
            add_len = len(p) + 2 if out else len(p)
            if length + add_len > 2000:
                break
            out.append(p)
            length += add_len
        result = ", ".join(out)
    return result

WHATSAPP_BLOCK = """<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;"><p style="text-align:center; margin:0 0 12px;"><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,.08);">&#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!</a></p><div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;"><h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3><ul style="margin:0; padding-left:18px;"><li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li><li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li></ul><hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" /><h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3><ul style="margin:0; padding-left:18px;"><li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li><li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li><li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li><li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li></ul></div></div>"""

# Делает:  render description html
def _render_description_html(name: str, desc_plain: str) -> str:
    base = (desc_plain or "").strip()
    if not base:
        base = (name or "").strip()
    base = clean_article_mentions(base)
    base = _normalize_description_text(base)
    cut = _build_desc_text(base)
    text_html = _xml_escape_text(cut)
    name_html = _xml_escape_text(name or "")
    return f"<h3>{name_html}</h3><p>{text_html}</p>"

# Делает: собирает итоговый YML
def build_yml(offers: List[Dict[str,Any]], feed_meta_str: str) -> str:
    lines: List[str] = []
    ts = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d %H:%M")
    lines.append(f'<?xml version="1.0" encoding="{XML_ENCODING}"?>')
    lines.append('')
    lines.append(f'<yml_catalog date="{ts}">')
    lines.append("<shop><offers>")
    lines.append("")
    if feed_meta_str:
        lines.append(feed_meta_str)
        lines.append("")
    first = True
    for it in offers:
        if not first:
            lines.append("")
        first = False
        offer_id = it["vendorCode"]
        lines.append(f'<offer id="{yml_escape(offer_id)}" available="true">')
        lines.append("<categoryId></categoryId>")
        lines.append(f'<vendorCode>{yml_escape(it["vendorCode"])}</vendorCode>')
        lines.append(f'<name>{yml_escape(it["title"])}</name>')
        lines.append(f'<price>{int(it["price"])}</price>')
        if it.get("picture"):
            lines.append(f'<picture>{yml_escape(it["picture"])}</picture>')
        if it.get("brand"):
            lines.append(f'<vendor>{yml_escape(it["brand"])}</vendor>')
        lines.append(f'<currencyId>{CURRENCY}</currencyId>')
        desc_plain = it.get("description") or it["title"]
        body_html = _render_description_html(it["title"], desc_plain)
        lines.append("<description><![CDATA[")
        lines.append("")
        lines.append("<!-- WhatsApp -->")
        lines.append(WHATSAPP_BLOCK)
        lines.append("")
        lines.append("<!-- Описание -->")
        lines.append(body_html)
        lines.append("")
        lines.append("]]></description>")
        kw = _make_keywords(it["title"], it.get("brand") or "")
        if kw:
            lines.append(f'<keywords>{yml_escape(kw)}</keywords>')
        lines.append("</offer>")
    lines.append("")
    lines.append("</offers>")
    lines.append("</shop>")
    lines.append("</yml_catalog>")
    return "\n".join(lines)

# Делает: точка входа
def main() -> int:
    b = fetch_xlsx_bytes(XLSX_URL)
    wb = load_workbook(io.BytesIO(b), read_only=True, data_only=True)
    sheet = max(wb.sheetnames, key=lambda n: wb[n].max_row * max(1, wb[n].max_column))
    ws = wb[sheet]
    rows = [[c for c in r] for r in ws.iter_rows(values_only=True)]
    print(f"[xls] sheet: {sheet}, rows: {len(rows)}", flush=True)

    row0, row1, idx = detect_header_two_row(rows)
    if row0 < 0:
        print("[error] Не удалось распознать шапку.", flush=True); return 2
    data_start = row1 + 1
    name_col, vendor_col, price_col = idx["name"], idx["vendor_code"], idx["price"]

    kw_list = load_keywords(KEYWORDS_FILE)
    start_patterns = compile_startswith_patterns(kw_list)

    source_rows = sum(1 for r in rows[data_start:] if any(v is not None and str(v).strip() for v in r))
    xlsx_items: List[Dict[str,Any]] = []
    want_keys: Set[str] = set()

    for r in rows[data_start:]:
        name_raw = r[name_col]
        if not name_raw: continue
        title = title_clean(str(name_raw).strip())
        if not title_startswith_strict(title, start_patterns): continue

        dealer = to_number(r[price_col])
        if dealer is None or dealer <= 0: continue

        v_raw = r[vendor_col]
        vcode = (str(v_raw).strip() if v_raw is not None else "")
        if not vcode:
            m = re.search(r"[A-ZА-Я0-9]{2,}(?:[-/–][A-ZА-Я0-9]{2,})?", title.upper())
            if m: vcode = m.group(0).replace("–","-").replace("/","-")
        if not vcode: continue

        variants = { vcode, vcode.replace("-", "") }
        if re.match(r"^[Cc]\d+$", vcode): variants.add(vcode[1:])
        if re.match(r"^\d+$", vcode):     variants.add("C"+vcode)
        for v in variants: want_keys.add(norm_ascii(v))

        retail = compute_retail(float(dealer))
        if retail is None: continue

        xlsx_items.append({
            "title": title,
            "price": float(retail),
            "vendorCode_raw": vcode,
        })

    offers_total = len(xlsx_items)
    if not xlsx_items:
        print("[error] После фильтра по startswith/цене нет позиций.", flush=True); return 2
    print(f"[xls] candidates: {offers_total}, distinct keys: {len(want_keys)}", flush=True)

    def discover_relevant_category_urls() -> List[str]:
        seeds = [f"{BASE_URL}/", f"{BASE_URL}/goods.html"]; pages=[]
        for u in seeds:
            b = http_get(u)
            if b: pages.append((u, soup_of(b)))
        if not pages: return []
        kws = load_keywords(KEYWORDS_FILE)
        urls, seen = [], set()
        for base, s in pages:
            for a in s.find_all("a", href=True):
                txt = a.get_text(" ", strip=True) or ""
                absu = urljoin(base, a["href"])
                if "copyline.kz" not in absu: continue
                if "/goods/" not in absu and not absu.endswith("/goods.html"): continue
                ok = any(re.search(r"(?i)(?<!\w)"+re.escape(kw).replace(r"\ "," ")+r"(?!\w)", txt) for kw in kws)
                if not ok:
                    slug = absu.lower()
                    if any(h in slug for h in ["drum","developer","fuser","toner","cartridge",
                                               "драм","девелопер","фьюзер","термоблок","термоэлемент","cartridg"]):
                        ok = True
                if ok and absu not in seen:
                    seen.add(absu); urls.append(absu)
        return list(dict.fromkeys(urls))

    def category_next_url(s: BeautifulSoup, page_url: str) -> Optional[str]:
        ln = s.find("link", attrs={"rel":"next"})
        if ln and ln.get("href"): return urljoin(page_url, ln["href"])
        a = s.find("a", class_=lambda c: c and "next" in c.lower())
        if a and a.get("href"): return urljoin(page_url, a["href"])
        for a in s.find_all("a", href=True):
            txt = (a.get_text(" ", strip=True) or "").lower()
            if txt in ("следующая","вперед","вперёд","next",">"): return urljoin(page_url, a["href"])
        return None

    def collect_product_urls_from_category(cat_url: str, limit_pages: int) -> List[str]:
        urls, seen_pages, page, pages_done = [], set(), cat_url, 0
        while page and pages_done < limit_pages:
            if page in seen_pages: break
            seen_pages.add(page)
            jitter_sleep(REQUEST_DELAY_MS)
            b = http_get(page)
            if not b: break
            s = soup_of(b)
            for a in s.find_all("a", href=True):
                absu = urljoin(page, a["href"])
                if PRODUCT_RE.search(absu): urls.append(absu)
            page = category_next_url(s, page); pages_done += 1
        return list(dict.fromkeys(urls))

    cats = discover_relevant_category_urls()
    if not cats:
        print("[error] Не нашли релевантных разделов.", flush=True); return 2
    pages_budget = max(1, MAX_CATEGORY_PAGES // max(1, len(cats)))

    product_urls: List[str] = []
    for cu in cats: product_urls.extend(collect_product_urls_from_category(cu, pages_budget))
    product_urls = list(dict.fromkeys(product_urls))
    print(f"[crawl] product urls: {len(product_urls)}", flush=True)

    def worker(u: str):
        try:
            jitter_sleep(REQUEST_DELAY_MS)
            b = http_get(u)
            if not b: return None
            s = soup_of(b)
            sku = None
            skuel = s.find(attrs={"itemprop":"sku"})
            if skuel:
                v = (skuel.get_text(" ", strip=True) or "").strip()
                if v: sku = v
            if not sku:
                txt = s.get_text(" ", strip=True)
                m = re.search(r"(?:Артикул|SKU|Код товара|Код)\s*[:#]?\s*([A-Za-z0-9\-\._/]{2,})", txt, flags=re.I)
                if m: sku = m.group(1)
            if not sku: return None
            src = None
            imgel = s.find("img", id=re.compile(r"^main_image_", re.I))
            if imgel and (imgel.get("src") or imgel.get("data-src")):
                src = imgel.get("src") or imgel.get("data-src")
            if not src:
                ogi = s.find("meta", attrs={"property":"og:image"})
                if ogi and ogi.get("content"): src = ogi["content"].strip()
            if not src:
                for img in s.find_all("img"):
                    t = img.get("src") or img.get("data-src") or ""
                    if any(k in t for k in ["img_products","/products/","/img/"]):
                        src = t; break
            if not src: return None
            pic = normalize_img_to_full(urljoin(u, src))
            h1 = s.find(["h1","h2"], attrs={"itemprop":"name"}) or s.find("h1") or s.find("h2")
            title = (h1.get_text(" ", strip=True) if h1 else "").strip()
            desc_txt, specs_kv = "", {}
            block = s.select_one('div[itemprop="description"].jshop_prod_description') \
                 or s.select_one('div.jshop_prod_description') \
                 or s.select_one('[itemprop="description"]')
            if block: desc_txt, specs_kv = extract_specs_and_text(block)
            cand = collect_brand_candidates(f"{title} {desc_txt}")
            spec_b = extract_brand_from_specs_kv(specs_kv)
            if spec_b:
                spec_b = sanitize_brand(spec_b)
                if spec_b and spec_b not in cand:
                    cand.append(spec_b)
            brand = choose_brand_oem_first(cand)
            return (
                { norm_ascii(sku), norm_ascii(sku.replace("-", "")) } |
                ({ norm_ascii(sku[1:]) } if re.match(r"^[Cc]\d+$", sku) else set()) |
                ({ norm_ascii('C'+sku) } if re.match(r"^\d+$", sku) else set())
            ), {"url": u, "pic": pic, "desc": desc_txt or title, "brand": brand}
        except Exception:
            return None

    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MINUTES)
    site_index: Dict[str, Dict[str, Any]] = {}
    matched_keys: Set[str] = set()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = { ex.submit(worker, u): u for u in product_urls }
        for fut in as_completed(futures):
            if datetime.utcnow() > deadline: break
            out = fut.result()
            if not out: continue
            keys, payload = out
            useful = [k for k in keys if k in want_keys and k not in matched_keys]
            if not useful: continue
            for k in useful:
                site_index[k] = payload; matched_keys.add(k)
            if len(matched_keys) % 50 == 0:
                print(f"[match] {len(matched_keys)} / {len(want_keys)}", flush=True)
            if matched_keys >= want_keys:
                print("[match] all wanted keys found.", flush=True); break

    print(f"[index] matched keys: {len(matched_keys)}", flush=True)

    offers: List[Dict[str,Any]] = []
    seen_vendorcodes: Set[str] = set()
    cnt_no_match = 0; cnt_no_picture = 0; cnt_vendors = 0

    for it in xlsx_items:
        raw_v = it["vendorCode_raw"]
        candidates = { raw_v, raw_v.replace("-", "") }
        if re.match(r"^[Cc]\d+$", raw_v): candidates.add(raw_v[1:])
        if re.match(r"^\d+$", raw_v):     candidates.add("C"+raw_v)
        found = None
        for v in candidates:
            kn = norm_ascii(v)
            if kn in site_index: found = site_index[kn]; break
        if not found: cnt_no_match += 1; continue
        if not found.get("pic"): cnt_no_picture += 1; continue

        desc  = clean_article_mentions(found.get("desc") or it["title"])
        title = it["title"]

        brand = sanitize_brand(found.get("brand"))
        if not brand:
            cand = collect_brand_candidates(f"{title} {desc}")
            brand = choose_brand_oem_first(cand)
        if not brand:
            brand = brand_soft_fallback(title, desc)
            if brand:
                brand = sanitize_brand(brand)

        if brand and norm_ascii(brand) in BLOCK_SUPPLIER_BRANDS:
            brand = None
        if brand: cnt_vendors += 1

        vendorCode = f"{VENDORCODE_PREFIX}{raw_v}"
        if vendorCode in seen_vendorcodes:
            vendorCode = f"{vendorCode}-{hashlib.sha1(title.encode('utf-8')).hexdigest()[:6]}"
        seen_vendorcodes.add(vendorCode)

        offers.append({
            "vendorCode": vendorCode,
            "title":      title,
            "price":      it["price"],
            "brand":      brand,
            "picture":    found["pic"],
            "description": desc,
        })

    offers_written = len(offers)

    meta_pairs = {
        "supplier": SUPPLIER_NAME,
        "source":   XLSX_URL,
        "offers_total":   len(xlsx_items),
        "offers_written": offers_written,
        "available_true": offers_written,
        "available_false": 0,
    }
    feed_meta_str = render_feed_meta_for_copyline(meta_pairs)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    xml = build_yml(offers, feed_meta_str)
    with open(OUT_FILE, "w", encoding=FILE_ENCODING, errors="replace") as f:
        f.write(xml)

    print(f"[done] items: {offers_written} -> {OUT_FILE}", flush=True)
    return 0

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("[fatal]", e, flush=True); sys.exit(2)
