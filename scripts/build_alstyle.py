# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle → YML (single-pass; description untouched)

Задача:
- Перестроить пайплайн в один проход по офферам (SPPO), сохранить тот же результат, но упростить порядок действий.
- Тег <description> НЕ МЕНЯЕМ — читаем только при генерации keywords.
- Сохранить всю прежнюю логику по ценам, available, vendor, vendorCode/id, валюте, картинкам, param-чистке, порядку тегов, keywords и FEED_META.

Как работает (в общих чертах):
0) Читаем исходный XML поставщика.
1) Готовим вспомогательные индексы (категории/бренды), если нужно.
2) Один проход по каждому offer:
   - фильтр по категории (если включён),
   - нормализация/автозаполнение vendor (с учётом LG и алиасов),
   - расчёт цены (+4% + фикс по диапазону, хвост 900; кап по порогу),
   - перенос available в атрибут и чистка складских тегов,
   - чистка мусорных <param>,
   - картинки (заглушки при отсутствии),
   - vendorCode (префикс AS) и синхронизация offer/@id,
   - чистка служебных тегов и атрибутов,
   - валюта KZT,
   - keywords,
   - финальный порядок детей оффера + <categoryId>0</categoryId> первым.
3) FEED_META и запись в windows-1251 (с безопасным fallback).
"""

from __future__ import annotations
import os, sys, re, time, random, hashlib, urllib.parse, requests
from typing import Dict, List, Tuple, Optional, Set
from copy import deepcopy
from xml.etree import ElementTree as ET
from datetime import datetime, timezone, timedelta

# -------- Время (Алматы) --------
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# ======================= ПАРАМЕТРЫ ОКРУЖЕНИЯ =======================
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "AlStyle").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/alstyle.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()

TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# Категории: include/exclude/off + путь к правилам
ALSTYLE_CATEGORIES_PATH = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt")
ALSTYLE_CATEGORIES_MODE = os.getenv("ALSTYLE_CATEGORIES_MODE", "off").lower()  # off|include|exclude

# Префикс vendorCode/id
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS")

# Кап по завышенным ценам
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "100"))

# Ключевые слова
SATU_KEYWORDS          = os.getenv("SATU_KEYWORDS", "auto").lower()  # auto|off
SATU_KEYWORDS_MAXLEN   = int(os.getenv("SATU_KEYWORDS_MAXLEN", "1024"))
SATU_KEYWORDS_MAXWORDS = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "1000"))
SATU_KEYWORDS_GEO      = os.getenv("SATU_KEYWORDS_GEO", "on").lower() in {"on","1","true","yes"}
SATU_KEYWORDS_GEO_MAX  = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))
SATU_KEYWORDS_GEO_LAT  = os.getenv("SATU_KEYWORDS_GEO_LAT", "on").lower() in {"on","1","true","yes"}

# Фото-заглушки
PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.al-style.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.al-style.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.al-style.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# Публичный YML: вычищаем служебные теги/атрибуты
PURGE_TAGS_AFTER        = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","article")
INTERNAL_PRICE_TAGS     = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                           "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice",
                           "max_price","maxPrice","oldprice")

# ======================= ЛОГ/УТИЛИТЫ =======================
def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARN: {msg}", file=sys.stderr, flush=True)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def now_almaty() -> datetime:
    if ZoneInfo:
        try:
            return datetime.now(ZoneInfo("Asia/Almaty"))
        except Exception:
            pass
    return datetime.utcfromtimestamp(time.time() + 5*3600)

def next_build_time_almaty() -> datetime:
    cur = now_almaty()
    t = cur.replace(hour=1, minute=0, second=0, microsecond=0)
    return t + timedelta(days=1) if cur >= t else t

def format_dt_almaty(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def load_source_bytes(src: str) -> bytes:
    """Скачиваем/читаем исходный XML поставщика; гарантируем минимальный размер."""
    if not src:
        raise RuntimeError("SUPPLIER_URL не задан")
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path, "rb") as f:
            data = f.read()
        if len(data) < MIN_BYTES:
            raise RuntimeError(f"file too small: {len(data)}")
        return data
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    last_err: Optional[Exception] = None
    for i in range(1, RETRIES + 1):
        try:
            r = sess.get(src, headers=headers, timeout=TIMEOUT_S)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            data = r.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)})")
            return data
        except Exception as e:
            last_err = e
            back = RETRY_BACKOFF * i * (1 + random.uniform(-0.2, 0.2))
            warn(f"fetch {i}/{RETRIES} failed: {e}; sleep {back:.2f}s")
            if i < RETRIES:
                time.sleep(back)
    raise RuntimeError(f"fetch failed: {last_err}")

# ======================= НОРМАЛИЗАЦИЯ ТЕКСТА/КАТЕГОРИИ =======================
def _norm_text(s: str) -> str:
    s = (s or "").replace("\u00A0", " ").lower().replace("ё", "е")
    return re.sub(r"\s+", " ", s).strip()

def _norm_cat(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s*[/>\|]\s*", " / ", s)
    return re.sub(r"\s+"," ", s).strip()

class CatRule:
    __slots__ = ("raw", "kind", "pattern")
    def __init__(self, raw: str, kind: str, pattern):
        self.raw, self.kind, self.pattern = raw, kind, pattern

def load_category_rules(path: str) -> Tuple[Set[str], List[CatRule]]:
    """Читаем docs/alstyle_categories.txt: чистые ID и строки/регексы для путей категорий."""
    if not path or not os.path.exists(path):
        return set(), []
    data: Optional[str] = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path, "r", encoding=enc) as f:
                data = f.read().replace("\ufeff","").replace("\x00","")
                break
        except Exception:
            continue
    if data is None:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read().replace("\x00","")
    ids: Set[str] = set()
    rules: List[CatRule] = []
    for ln in data.splitlines():
        s = ln.strip()
        if not s or s.lstrip().startswith("#"):
            continue
        if re.fullmatch(r"\d{2,}", s):
            ids.add(s); continue
        if len(s) >= 2 and s[0] == "/" and s[-1] == "/":
            try:
                rules.append(CatRule(s, "regex", re.compile(s[1:-1], re.I)))
                continue
            except Exception:
                continue
        rules.append(CatRule(_norm_text(s), "substr", None))
    return ids, rules

def parse_categories_tree(shop_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str]]:
    id2name: Dict[str,str] = {}
    id2parent: Dict[str,str] = {}
    cats_root = shop_el.find("categories") or shop_el.find("Categories")
    if cats_root is None:
        return id2name, id2parent
    for c in cats_root.findall("category"):
        cid = (c.attrib.get("id") or "").strip()
        if not cid:
            continue
        pid = (c.attrib.get("parentId") or "").strip()
        id2name[cid] = (c.text or "").strip()
        if pid:
            id2parent[cid] = pid
    return id2name, id2parent

def build_category_path_from_id(cat_id: str, id2name: Dict[str,str], id2parent: Dict[str,str]) -> str:
    names: List[str] = []
    cur = cat_id
    seen: Set[str] = set()
    while cur and cur not in seen and cur in id2name:
        seen.add(cur)
        names.append(id2name.get(cur, ""))
        cur = id2parent.get(cur, "")
    names = [n for n in names if n]
    return " ".join(reversed(names)) if names else ""

def category_matches_name(path_str: str, rules: List[CatRule]) -> bool:
    cat_norm = _norm_text(_norm_cat(path_str))
    for cr in rules:
        if cr.kind == "substr":
            if cr.raw and cr.raw in cat_norm:
                return True
        elif cr.pattern and cr.pattern.search(path_str or ""):
            return True
    return False

def collect_descendants(ids: Set[str], id2parent: Dict[str,str]) -> Set[str]:
    """Расширяем набор ID всеми потомками по id2parent (упрощённо через обратные ссылки)."""
    children_map: Dict[str, Set[str]] = {}
    for cid, pid in id2parent.items():
        children_map.setdefault(pid, set()).add(cid)
    out = set(ids)
    stack = list(ids)
    while stack:
        cur = stack.pop()
        for ch in children_map.get(cur, ()):
            if ch not in out:
                out.add(ch)
                stack.append(ch)
    return out

# ======================= БРЕНДЫ =======================
def _norm_key(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower().replace("ё","е")
    s = re.sub(r"[-_/]+"," ", s)
    return re.sub(r"\s+"," ", s)

SUPPLIER_BLOCKLIST = {_norm_key(x) for x in ["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS = ("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

COMMON_BRANDS = [
    "Canon","HP","Hewlett-Packard","Xerox","Brother","Epson","Samsung","Kyocera","Ricoh","Konica Minolta",
    "Lexmark","Sharp","OKI","Pantum",
    "Europrint","Katun","NV Print","Hi-Black","ProfiLine","Cactus","G&G","Static Control","Lomond","WWM","Uniton",
    "TSC","Zebra",
    "SVC","APC","Powercom","PCM","Ippon","Eaton","Vinga",
    "MSI","ASUS","Acer","Lenovo","Dell","Apple",
    "LG"  # важно: поддержка LG
]
BRAND_ALIASES = {
    "hewlett packard":"HP","konica":"Konica Minolta","konica-minolta":"Konica Minolta",
    "powercom":"Powercom","pcm":"Powercom","apc":"APC","msi":"MSI",
    "nvprint":"NV Print","nv print":"NV Print",
    "hi black":"Hi-Black","hiblack":"Hi-Black","hi-black":"Hi-Black",
    "g&g":"G&G","gg":"G&G",
    "lg":"LG"
}

def normalize_brand(raw: str) -> str:
    k = _norm_key(raw)
    return "" if (not k) or (k in SUPPLIER_BLOCKLIST) else raw.strip()

def build_brand_index(shop_el: ET.Element) -> Dict[str,str]:
    """Стартовый индекс известных брендов из готовых <vendor>."""
    idx: Dict[str,str] = {}
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return idx
    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        if ven is not None and (ven.text or "").strip():
            canon = normalize_brand(ven.text or "")
            if canon:
                idx[_norm_key(canon)] = canon
    return idx

def _find_brand_in_text(text: str) -> str:
    t = _norm_text(text)
    if not t:
        return ""
    for b in COMMON_BRANDS:
        if re.search(rf"\b{re.escape(_norm_text(b))}\b", t, flags=re.I):
            return b
    for a,canon in BRAND_ALIASES.items():
        if re.search(rf"\b{re.escape(a)}\b", t, flags=re.I):
            return canon
    m = re.match(r"^([A-Za-zА-Яа-яЁё]+)\b", (text or "").strip())
    if m:
        cand = m.group(1)
        for b in COMMON_BRANDS:
            if _norm_text(b) == _norm_text(cand):
                return b
    return ""

# ======================= ЦЕНООБРАЗОВАНИЕ =======================
PriceRule = Tuple[int,int,float,int]
PRICING_RULES: List[PriceRule] = [
    (101, 10000, 4.0, 3000),(10001, 25000, 4.0, 4000),(25001, 50000, 4.0, 5000),
    (50001, 75000, 4.0, 7000),(75001, 100000, 4.0, 10000),(100001, 150000, 4.0, 12000),
    (150001, 200000, 4.0, 15000),(200001, 300000, 4.0, 20000),(300001, 400000, 4.0, 25000),
    (400001, 500000, 4.0, 30000),(500001, 750000, 4.0, 40000),(750001, 1000000, 4.0, 50000),
    (1000001, 1500000, 4.0, 70000),(1500001, 2000000, 4.0, 90000),(2000001, 100000000, 4.0, 100000),
]

PRICE_FIELDS_DIRECT = ["purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price"]
PRICE_KEYWORDS_DEALER = re.compile(r"(дилер|dealer|опт|wholesale|b2b|закуп|purchase|оптов)", re.I)
PRICE_KEYWORDS_RRP    = re.compile(r"(rrp|ррц|розниц|retail|msrp)", re.I)

def parse_price_number(raw: str) -> Optional[float]:
    if raw is None:
        return None
    s = (raw.strip()
           .replace("\xa0", " ")
           .replace(" ", "")
           .replace("KZT","")
           .replace("kzt","")
           .replace("₸","")
           .replace(",", "."))
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except Exception:
        return None

def pick_dealer_price(offer: ET.Element) -> Tuple[Optional[float], str]:
    """Приоритет: <prices type~dealer/опт/b2b> → прямые поля → RRP."""
    dealer_candidates: List[float] = []
    rrp_candidates: List[float] = []
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            val = parse_price_number(p.text or "")
            if val is None:
                continue
            t = (p.attrib.get("type") or "")
            if PRICE_KEYWORDS_DEALER.search(t):
                dealer_candidates.append(val)
            elif PRICE_KEYWORDS_RRP.search(t):
                rrp_candidates.append(val)
    if dealer_candidates:
        return (min(dealer_candidates), "prices_dealer")

    direct: List[float] = []
    for tag in PRICE_FIELDS_DIRECT:
        el = offer.find(tag)
        if el is not None and el.text:
            v = parse_price_number(el.text)
            if v is not None:
                direct.append(v)
    if direct:
        return (min(direct), "direct_field")

    if rrp_candidates:
        return (min(rrp_candidates), "rrp_fallback")
    return (None, "missing")

def _force_tail_900(n: float) -> int:
    n_int = int(round(n))
    return (n_int // 1000) * 1000 + 900

def compute_retail(dealer: float, rules: List[PriceRule]) -> Optional[int]:
    for lo, hi, pct, add in rules:
        if lo <= dealer <= hi:
            return _force_tail_900(dealer * (1.0 + pct / 100.0) + add)
    return None

def _remove_all_price_nodes(offer: ET.Element) -> None:
    for t in ("price","Price"):
        for node in list(offer.findall(t)):
            offer.remove(node)

def strip_supplier_price_blocks(offer: ET.Element) -> None:
    """Удаляем <prices> и все внутренние полузакрытые теги цен из публичного YML."""
    remove_tags = ["prices","Prices"] + list(INTERNAL_PRICE_TAGS)
    for t in remove_tags:
        for node in list(offer.findall(t)):
            offer.remove(node)

# ======================= AVAILABLE/ЧИСТКИ/ПОРЯДОК =======================
TRUE_WORDS = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS= {"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}

def _parse_bool_str(s: str) -> Optional[bool]:
    v = _norm_text(s or "")
    return True if v in TRUE_WORDS else False if v in FALSE_WORDS else None

def _parse_int(s: str) -> Optional[int]:
    t = re.sub(r"[^\d\-]+","", s or "")
    if t in {"","-","+"}:
        return None
    try:
        return int(t)
    except Exception:
        return None

def derive_available(offer: ET.Element) -> Tuple[bool, str]:
    """Определяем наличие по available/quantity*/status."""
    avail_el = offer.find("available")
    if avail_el is not None and avail_el.text:
        b = _parse_bool_str(avail_el.text)
        if b is not None:
            return b, "tag"
    for tag in ["quantity_in_stock","quantity","stock","Stock"]:
        for node in offer.findall(tag):
            val = _parse_int(node.text or "")
            if val is not None:
                return (val > 0), "stock"
    for tag in ["status","Status"]:
        node = offer.find(tag)
        if node is not None and node.text:
            b = _parse_bool_str(node.text)
            if b is not None:
                return b, "status"
    return False, "default"

UNWANTED_PARAM_NAME_RE = re.compile(
    r"^(?:\s*(?:благотворительн\w*|снижена\s*цена|новинк\w*|"
    r"артикул(?:\s*/\s*штрихкод)?|штрихкод|"
    r"код\s*тн\s*вэд(?:\s*eaeu)?|код\s*тнвэд(?:\s*eaeu)?|тн\s*вэд|тнвэд|"
    r"tn\s*ved|hs\s*code)\s*)$",
    re.I
)
KASPI_CODE_NAME_RE = re.compile(r"^код\s+товара\s+kaspi$", re.I)

def _value_is_empty_or_noise(val: str) -> bool:
    v = (val or "").strip().lower()
    if not v or v in {"-","—","–",".","..","...","n/a","na","none","null","нет данных","не указано","неизвестно"}:
        return True
    if "http://" in v or "https://" in v or "www." in v:
        return True
    if "<" in v and ">" in v:
        return True
    return False

# ======================= КАРТИНКИ =======================
_url_head_cache: Dict[str,bool] = {}
def url_exists(url: str) -> bool:
    if not url:
        return False
    if url in _url_head_cache:
        return _url_head_cache[url]
    try:
        r = requests.head(url, timeout=PLACEHOLDER_HEAD_TIMEOUT, allow_redirects=True)
        ok = (200 <= r.status_code < 400)
    except Exception:
        ok = False
    _url_head_cache[url] = ok
    return ok

def _slug(s: str) -> str:
    if not s:
        return ""
    table = str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    base = (s or "").lower().translate(table)
    base = re.sub(r"[^a-z0-9\- ]+", "", base)
    return re.sub(r"\s+","-", base).strip("-") or "unknown"

def _placeholder_url_brand(vendor: str) -> str:
    return f"{PLACEHOLDER_BRAND_BASE}/{_slug(vendor)}.{PLACEHOLDER_EXT}"

def _placeholder_url_category(kind: str) -> str:
    return f"{PLACEHOLDER_CATEGORY_BASE}/{kind}.{PLACEHOLDER_EXT}"

def detect_kind(name: str) -> str:
    n = (name or "").lower()
    if "картридж" in n or "тонер" in n or "тонер-" in n:
        return "cartridge"
    if "ибп" in n or "ups" in n or "источник бесперебойного питания" in n:
        return "ups"
    if "мфу" in n or "printer" in n or "принтер" in n:
        return "mfp"
    return "other"

# ======================= ИЗВЛЕЧЕНИЕ АРТИКУЛА/ID =======================
ARTICUL_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)
AS_INTERNAL_ART_RE = re.compile(r"^AS\d+", re.I)

def _extract_article_from_name(name: str) -> str:
    if not name: return ""
    m = ARTICUL_RE.search(name)
    return (m.group(1) if m else "").upper()

def _extract_article_from_url(url: str) -> str:
    if not url: return ""
    try:
        path = urllib.parse.urlparse(url).path.rstrip("/")
        last = re.sub(r"\.(html?|php|aspx?)$","", path.split("/")[-1], flags=re.I)
        m = ARTICUL_RE.search(last)
        return (m.group(1) if m else last).upper()
    except Exception:
        return ""

def _normalize_code(s: str) -> str:
    s = (s or "").strip()
    if not s: return ""
    s = re.sub(r"[\s_]+","", s).replace("—","-").replace("–","-")
    return re.sub(r"[^A-Za-z0-9\-]+","", s).upper()

# ======================= KEYWORDS =======================
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
STOPWORDS_RU = {"для","и","или","на","в","из","от","по","с","к","до","при","через","над","под","о","об","у","без","про","как","это","той","тот","эта","эти",
                "бумага","бумаги","бумаг","черный","чёрный","белый","серый","цвет","оригинальный","комплект","набор","тип","модель","модели","формат","новый","новинка"}
STOPWORDS_EN = {"for","and","or","with","of","the","a","an","to","in","on","by","at","from","new","original","type","model","set","kit","pack"}
GENERIC_DROP = {"изделие","товар","продукция","аксессуар","устройство","оборудование"}

def tokenize_name(name: str) -> List[str]:
    return WORD_RE.findall(name or "")

def is_content_word(token: str) -> bool:
    t = _norm_text(token)
    return bool(t) and (t not in STOPWORDS_RU) and (t not in STOPWORDS_EN) and (t not in GENERIC_DROP) and (any(ch.isdigit() for ch in t) or "-" in t or len(t)>=3)

def build_bigrams(words: List[str]) -> List[str]:
    out: List[str] = []
    for i in range(len(words)-1):
        a, b = words[i], words[i+1]
        if is_content_word(a) and is_content_word(b):
            out.append(f"{a} {b}")
    return out

def dedup_preserve_order(words: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for w in words:
        key = _norm_text(str(w))
        if key and key not in seen:
            seen.add(key); out.append(str(w))
    return out

def translit_ru_to_lat(s: str) -> str:
    table = str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    out = (s or "").lower().translate(table)
    out = re.sub(r"[^a-z0-9\- ]+","", out)
    return re.sub(r"\s+","-", out).strip("-")

def color_tokens(name: str) -> List[str]:
    out: List[str] = []
    low = (name or "").lower()
    mapping = {"жёлт":"желтый","желт":"желтый","yellow":"yellow","черн":"черный","black":"black","син":"синий","blue":"blue",
               "красн":"красный","red":"red","зелен":"зеленый","green":"green","серебр":"серебряный","silver":"silver","циан":"cyan","магент":"magenta"}
    for k,val in mapping.items():
        if k in low:
            out.append(val)
    return dedup_preserve_order(out)

MODEL_RE = re.compile(r"\b([A-Z][A-Z0-9\-]{2,})\b", re.I)

def extract_model_tokens(name: str, description_html: str) -> List[str]:
    tokens: Set[str] = set()
    for src in (name, description_html):
        if not src:
            continue
        for m in MODEL_RE.findall(src):
            t = m.upper()
            if AS_INTERNAL_ART_RE.match(t) or not (re.search(r"[A-Z]", t) and re.search(r"\d", t)) or len(t) < 5:
                continue
            tokens.add(t)
    return list(tokens)

def geo_tokens() -> List[str]:
    if not SATU_KEYWORDS_GEO:
        return []
    toks = ["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
            "Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавловск","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]
    if SATU_KEYWORDS_GEO_LAT:
        toks += ["Kazakhstan","Almaty","Astana","Shymkent","Karaganda","Aktobe","Pavlodar","Atyrau","Taraz",
                 "Oskemen","Semey","Kostanay","Kyzylorda","Oral","Petropavlovsk","Taldykorgan","Aktau","Temirtau","Ekibastuz","Kokshetau","Rudny"]
    toks = dedup_preserve_order(toks)
    return toks[:max(0, SATU_KEYWORDS_GEO_MAX)]

def keywords_from_offer(name: str, vendor: str, description_html: str) -> str:
    if SATU_KEYWORDS == "off":
        return ""
    parts: List[str] = [vendor] if vendor else []
    parts += extract_model_tokens(name, description_html)

    raw_tokens = tokenize_name(name or "")
    modelish   = [t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content    = [t for t in raw_tokens if is_content_word(t)]
    bigr       = build_bigrams(content)
    norm = lambda tok: tok if re.search(r"[A-Z]{2,}", tok) else tok.capitalize()
    parts += modelish[:8] + bigr[:8] + [norm(t) for t in content[:10]]
    parts += color_tokens(name)

    extra: List[str] = []
    for w in parts:
        if re.search(r"[А-Яа-яЁё]", str(w) or ""):
            tr = translit_ru_to_lat(str(w))
            if tr and tr not in extra:
                extra.append(tr)
    parts += extra + geo_tokens()
    parts = [p for p in dedup_preserve_order(parts) if not AS_INTERNAL_ART_RE.match(str(p))]
    parts = parts[:SATU_KEYWORDS_MAXWORDS]

    out: List[str] = []
    total = 0
    for p in parts:
        s = str(p).strip().strip(",")
        if not s:
            continue
        add = ((", " if out else "") + s)
        if total + len(add) > SATU_KEYWORDS_MAXLEN:
            break
        out.append(s)
        total += len(add)
    return ", ".join(out)

# ======================= MAIN =======================
def main() -> None:
    log(f"Source: {SUPPLIER_URL if SUPPLIER_URL else '(not set)'}")
    data = load_source_bytes(SUPPLIER_URL)

    # Разбираем XML источника
    src_root = ET.fromstring(data)
    shop_in  = src_root.find("shop") if src_root.tag.lower() != "shop" else src_root
    if shop_in is None:
        err("XML: <shop> not found")
    offers_in = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in is None:
        err("XML: <offers> not found")
    src_offers = list(offers_in.findall("offer"))

    # Готовим выходную структуру
    out_root = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")
    out_offers = ET.SubElement(out_shop, "offers")

    # Категории (для фильтра по ID/пути)
    id2name, id2parent = parse_categories_tree(shop_in)
    ids_rule, rules_name = load_category_rules(ALSTYLE_CATEGORIES_PATH)

    # Стартовый индекс брендов
    brand_index = build_brand_index(shop_in)

    # Счётчики для FEED_META
    cnt_total = len(src_offers)
    cnt_written = 0
    cnt_av_true = 0
    cnt_av_false = 0

    # Один проход по офферам
    for src_offer in src_offers:
        # --- КОПИРУЕМ исходный оффер (description остаётся как есть внутри копии) ---
        offer = deepcopy(src_offer)

        # (1) Фильтр по категориям (если включён)
        if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
            # Получаем categoryId из исходника (если был)
            catid_node = offer.find("categoryId") or offer.find("CategoryId")
            cid = (catid_node.text or "").strip() if (catid_node is not None and catid_node.text) else ""
            path = build_category_path_from_id(cid, id2name, id2parent) if cid else ""
            hit_id = (cid in ids_rule) if cid else False
            hit_nm = category_matches_name(path, rules_name) if path else False
            hit = hit_id or hit_nm
            drop = (ALSTYLE_CATEGORIES_MODE == "exclude" and hit) or (ALSTYLE_CATEGORIES_MODE == "include" and not hit)
            if drop:
                continue  # не пишем оффер вовсе

        # (2) Нормализация/автозаполнение vendor
        v_node = offer.find("vendor")
        v_text = (v_node.text or "").strip() if (v_node is not None and v_node.text) else ""
        if v_text:
            canon = normalize_brand(v_text)
            if canon:
                if canon != v_text:
                    v_node.text = canon
                brand_index[_norm_key(canon)] = canon
            else:
                # мусорный бренд — удаляем
                offer.remove(v_node)
                v_node = None
                v_text = ""
        if not v_text:
            # угадываем по name/description
            name_txt = (offer.findtext("name") or "").strip()
            desc_html = "".join(offer.find("description").itertext()) if offer.find("description") is not None else ""
            # простое чтение innerHTML как текст-лайн для поиска бренда
            raw_desc = ET.tostring(offer.find("description"), encoding="unicode") if offer.find("description") is not None else ""
            guess = ""
            # сначала из name
            guess = _find_brand_in_text(name_txt) or guess
            # затем из HTML description (как строка)
            guess = _find_brand_in_text(raw_desc) if not guess else guess
            # затем из индекса по первому слову name
            first = re.split(r"\s+", name_txt)[0] if name_txt else ""
            f_norm = _norm_key(first)
            if not guess and f_norm in brand_index:
                guess = brand_index[f_norm]
            if guess:
                v_node = ET.SubElement(offer, "vendor"); v_node.text = guess
                brand_index[_norm_key(guess)] = guess
                v_text = guess

        # (3) Цена: выбор dealer → вычисление retail → хвост 900 → кап при завышении
        dealer, src = pick_dealer_price(offer)
        if dealer is not None and dealer >= PRICE_CAP_THRESHOLD:
            # форс-кап
            for t in ("price","Price"):
                for node in list(offer.findall(t)):
                    offer.remove(node)
            ET.SubElement(offer, "price").text = str(int(PRICE_CAP_VALUE))
        else:
            if dealer is not None and dealer > 100:
                newp = compute_retail(dealer, PRICING_RULES)
                if newp is not None:
                    for t in ("price","Price"):
                        for node in list(offer.findall(t)):
                            offer.remove(node)
                    ET.SubElement(offer, "price").text = str(int(newp))
            # убираем служебные ценовые блоки (всегда)
            strip_supplier_price_blocks(offer)

        # (4) available → в атрибут; чистим складские теги
        b, _src_av = derive_available(offer)
        # удаляем дочерний <available>, если был
        for node in list(offer.findall("available")):
            offer.remove(node)
        offer.attrib["available"] = "true" if b else "false"
        if b: cnt_av_true += 1
        else: cnt_av_false += 1
        # чистим складские теги
        for tag in ["quantity_in_stock","quantity","stock","Stock","status","Status"]:
            for node in list(offer.findall(tag)):
                offer.remove(node)

        # (5) Чистка мусорных/дублирующих <param>
        seen: Set[str] = set()
        for tag in ("param","Param"):
            for p in list(offer.findall(tag)):
                nm = (p.attrib.get("name") or "").strip()
                val = (p.text or "").strip()
                if KASPI_CODE_NAME_RE.fullmatch(nm) or UNWANTED_PARAM_NAME_RE.match(nm) or _value_is_empty_or_noise(val):
                    offer.remove(p); continue
                key = _norm_text(nm)
                if key in seen:
                    offer.remove(p); continue
                seen.add(key)

        # (6) Картинки: если нет — плейсхолдер
        pics = list(offer.findall("picture"))
        has_pic = any((p.text or "").strip() for p in pics)
        if not has_pic and PLACEHOLDER_ENABLE:
            vendor = (v_node.text or "").strip() if v_node is not None and v_node.text else ""
            name_txt = (offer.findtext("name") or "").strip()
            kind = detect_kind(name_txt)
            picked = ""
            if vendor:
                u_brand = _placeholder_url_brand(vendor)
                if url_exists(u_brand):
                    picked = u_brand
            if not picked:
                u_cat = _placeholder_url_category(kind)
                if url_exists(u_cat):
                    picked = u_cat
            if not picked:
                picked = PLACEHOLDER_DEFAULT_URL
            ET.SubElement(offer, "picture").text = picked

        # (7) vendorCode/id: извлечь артикул и префикс AS
        vc = offer.find("vendorCode")
        if vc is None:
            vc = ET.SubElement(offer, "vendorCode"); vc.text = ""
        if not (vc.text or "").strip() or (vc.text or "").strip().upper() == VENDORCODE_PREFIX.upper():
            art = _normalize_code(offer.attrib.get("article") or "") \
               or _normalize_code(_extract_article_from_name(offer.findtext("name") or "")) \
               or _normalize_code(_extract_article_from_url(offer.findtext("url") or "")) \
               or _normalize_code(offer.attrib.get("id") or "")
            if art:
                vc.text = art
        vc.text = f"{VENDORCODE_PREFIX}{(vc.text or '')}"
        if offer.attrib.get("id") != (vc.text or ""):
            offer.attrib["id"] = (vc.text or "")

        # (8) Чистка служебных тегов/атрибутов
        for t in PURGE_TAGS_AFTER:
            for node in list(offer.findall(t)):
                offer.remove(node)
        for a in PURGE_OFFER_ATTRS_AFTER:
            if a in offer.attrib:
                offer.attrib.pop(a, None)

        # (9) Валюта
        for node in list(offer.findall("currencyId")):
            offer.remove(node)
        ET.SubElement(offer, "currencyId").text = "KZT"

        # (10) Keywords (чтение description только для токенов; само описание не меняем)
        name_txt = (offer.findtext("name") or "").strip()
        vendor_txt = (offer.findtext("vendor") or "").strip()
        desc_node = offer.find("description")
        desc_html = ET.tostring(desc_node, encoding="unicode") if desc_node is not None else ""
        kw = keywords_from_offer(name_txt, vendor_txt, desc_html)
        kw_node = offer.find("keywords")
        if kw:
            if kw_node is None:
                kw_node = ET.SubElement(offer, "keywords")
            kw_node.text = kw
        else:
            if kw_node is not None:
                offer.remove(kw_node)

        # (11) Порядок детей оффера + <categoryId>0</categoryId> первым
        # Желаемый порядок
        desired = ["vendorCode","name","price","picture","vendor","currencyId","description","keywords"]
        # Собираем в buckets:
        children = list(offer)
        buckets: Dict[str, List[ET.Element]] = {k: [] for k in desired}
        others: List[ET.Element] = []
        for node in children:
            if node.tag in buckets:
                buckets[node.tag].append(node)
            else:
                others.append(node)
        # Перестраиваем
        for node in children:
            offer.remove(node)
        # Вставляем <categoryId>0</categoryId> ПЕРВЫМ (перезатираем любые старые categoryId)
        for node in list(offer.findall("categoryId")) + list(offer.findall("CategoryId")):
            offer.remove(node)
        cid = ET.Element("categoryId"); cid.text = os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0, cid)
        # Далее — по порядку
        for key in desired:
            for node in buckets[key]:
                offer.append(node)
        for node in others:
            offer.append(node)

        # Пишем оффер в выходной документ
        out_offers.append(offer)
        cnt_written += 1

    # -------- FEED_META --------
    built_alm = now_almaty()
    meta_pairs = {
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL or "file",
        "built_alm": format_dt_almaty(built_alm),
        "next_build_alm": format_dt_almaty(next_build_time_almaty()),
        "offers_total": str(cnt_total),
        "offers_written": str(cnt_written),
        "available_true": str(cnt_av_true),
        "available_false": str(cnt_av_false),
    }
    def render_feed_meta_comment(pairs: Dict[str,str]) -> str:
        rows = [
            ("Поставщик", pairs.get("supplier","")),
            ("URL поставщика", pairs.get("source","")),
            ("Время сборки (Алматы)", pairs.get("built_alm","")),
            ("Ближайшая сборка (Алматы)", pairs.get("next_build_alm","")),
            ("Сколько товаров у поставщика до фильтра", pairs.get("offers_total","0")),
            ("Сколько товаров у поставщика после фильтра", pairs.get("offers_written","0")),
            ("Сколько товаров есть в наличии (true)", pairs.get("available_true","0")),
            ("Сколько товаров нет в наличии (false)", pairs.get("available_false","0")),
        ]
        key_w = max(len(k) for k,_ in rows)
        lines = ["FEED_META"] + [f"{k.ljust(key_w)} | {v}" for k,v in rows]
        return "\n".join(lines)

    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # Красивые отступы
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # Сериализация (windows-1251) + перенос после FEED_META + пустая строка между офферами
    xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")
    xml_text = re.sub(r"(-->)\s*(<shop\b)", r"\1\n\2", xml_text, count=1)
    xml_text = re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    try:
        with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
            f.write(xml_text)
    except UnicodeEncodeError as e:
        warn(f"{ENC} can't encode some characters ({e}); writing with xmlcharrefreplace fallback")
        data_bytes = xml_text.encode(ENC, errors="xmlcharrefreplace")
        with open(OUT_FILE_YML, "wb") as f:
            f.write(data_bytes)

    # .nojekyll для GitHub Pages
    try:
        docs_dir = os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | description=AS IS")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
