# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle -> YML (DESC-FLAT edition)

База = ваш КОД2 без изменений логики.
Единственное добавление: в самом конце ПЛОСКАЯ нормализация <description>
(удаляем теги внутри description, склеиваем всё в одну строку, схлопываем
много пробелов/переносов; пустые описания не трогаем).
"""

from __future__ import annotations
import os, sys, re, time, random, hashlib, urllib.parse, requests, html
from typing import Dict, List, Tuple, Optional, Set
from copy import deepcopy
from xml.etree import ElementTree as ET
from datetime import datetime, timezone, timedelta


# === Minimal post-steps for <description> (added) ===
def _desc_fix_punct_spacing(s: str) -> str:
    """
    Keep supplier text AS-IS, only remove spaces (incl. NBSP/thin spaces)
    directly before , . ; : ! ?
    """
    if s is None:
        return s
    import re as _re
    s = _re.sub(r'[\u00A0\u2009\u200A\u202F\s]+([,.;:!?])', r'\1', s)
    return s

def _desc_normalize_multi_punct(s: str) -> str:
    """
    Normalize long punctuation runs to marketplace-friendly form:
      - any unicode ellipsis '…' (one or more) -> '...'
      - 3 or more dots -> '...'
      - runs (>=3) of [! ? ; :] — collapse to the LAST char in the run
    """
    if s is None:
        return s
    import re as _re
    s = _re.sub(r'[!?:;]{3,}', lambda m: m.group(0)[-1], s)
    s = _re.sub(r'…+', '...', s)
    s = _re.sub(r'\.{3,}', '...', s)
    return s

def fix_all_descriptions_end(out_root):
    pass

# ========== BEGIN: HTML Description Beautifier (append-only) ==========
import html as _html
import re as _re

def _strip_span_like_html(s: str) -> str:
    if s is None:
        return s
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = _re.sub(r'(?i)<\s*br\s*/?\s*>', '\n', s)
    s = _re.sub(r'(?is)<\s*(?:span|div)[^>]*>', '', s)
    s = _re.sub(r'(?is)</\s*(?:span|div)\s*>', '', s)
    s = _re.sub(r'(?is)<\s*(?:p)\s*>', '\n', s)
    s = _re.sub(r'(?is)</\s*(?:p)\s*>', '\n', s)
    s = _re.sub(r'\n{3,}', '\n\n', s)
    return s

def _clean_spaces_punct(s: str) -> str:
    if s is None:
        return s
    s = _re.sub(r'[\u00A0\u2009\u200A\u202F\s]+([,.;:!?])', r'\1', s)
    s = _re.sub(r'[!?:;]{3,}', lambda m: m.group(0)[-1], s)
    s = _re.sub(r'…+', '...', s)
    s = _re.sub(r'\.{3,}', '...', s)
    s = '\n'.join(line.strip() for line in s.splitlines())
    s = _re.sub(r'\n{3,}', '\n\n', s)
    return s

def _extract_intro_and_kv(s: str):
    if s is None:
        return "", ""
    s = s.strip()
    m = _re.search(r'(?i)\b(технические\s+характеристики|характеристики)\b[:,]?', s)
    if not m:
        return s, ""
    intro = s[:m.start()].strip()
    kv = s[m.end():].strip()
    return intro, kv

def _kv_pairs_from_text(s: str):
    pairs = []
    if not s:
        return pairs
    s = _re.sub(r'[ \t]{2,}', '\n', s)
    chunks = _re.split(r'\n|(?:\s*[;|\u2022]\s*)', s)
    for ch in chunks:
        ch = ch.strip(' \u00A0-•\t')
        if not ch:
            continue
        m = _re.match(r'([^:：]{2,}?)\s*[:：]\s*(.+)$', ch)
        if m:
            key = m.group(1).strip()
            val = m.group(2).strip()
            if len(key) > 60 and ' ' in key:
                continue
            pairs.append((key, val))
    seen = set()
    uniq = []
    for k, v in pairs:
        low = (k.lower(), v.lower())
        if low not in seen:
            seen.add(low)
            uniq.append((k, v))
    return uniq

def _html_ul_from_pairs(pairs):
    if not pairs:
        return ""
    out = ["<ul>"]
    for k, v in pairs[:40]:
        out.append('  <li><strong>{}:</strong> {}</li>'.format(_html.escape(k, quote=False), _html.escape(v, quote=False)))
    out.append("</ul>")
    return "\n".join(out)

def _quick_facts_from_params(offer_elem):
    prio = ["Вес", "Ресурс", "Гарантия", "Диагональ экрана", "Частота обновления экрана", "Тип матрицы экрана",
            "Цвет", "Тип накопителя", "Объем накопителя", "Процессор", "Видеокарта"]
    values = []
    lookup = {}
    for p in list(offer_elem.findall("param")):
        nm = (p.get("name") or "").strip()
        val = (p.text or "").strip()
        if nm and val and nm not in lookup:
            lookup[nm] = val
    for key in prio:
        if key in lookup:
            values.append((key, lookup[key]))
        if len(values) >= 5:
            break
    if not values:
        return ""
    out = ["<ul>"]
    for k, v in values:
        out.append('  <li>&#9989; {}: {}</li>'.format(_html.escape(k), _html.escape(v)))
    out.append("</ul>")
    return "\n".join(out)

def beautify_descriptions_html(root):
    for offer in root.findall(".//offer"):
        d = offer.find("description")
        if d is None:
            continue
        raw = (d.text or "").strip()
        if len(raw) < 40:
            continue
        txt = _strip_span_like_html(raw)
        txt = _clean_spaces_punct(txt)
        intro, kv_block = _extract_intro_and_kv(txt)

        name_el = offer.findtext("name", "").strip()
        title = _html.escape(name_el) if name_el else "Описание"

        html_parts = []
        html_parts.append('<h3>{}</h3>'.format(title))

        if intro:
            intro_html = _html.escape(intro)
            paras = [p.strip() for p in intro_html.split("\n") if p.strip()]
            if len(paras) <= 2:
                html_parts.append("<p>{}</p>".format(' '.join(paras)))
            else:
                html_parts.append("<p>{}</p>".format(paras[0]))
                for p_ in paras[1:5]:
                    html_parts.append("<p>{}</p>".format(p_))

        quick = _quick_facts_from_params(offer)
        if quick:
            html_parts.append(quick)

        pairs = _kv_pairs_from_text(kv_block)
        if pairs:
            html_parts.append("<h3>Характеристики</h3>")
            html_parts.append(_html_ul_from_pairs(pairs))

        html_text = "\n".join(html_parts).strip()
        if len(_re.sub(r'<[^>]+>', '', html_text)) >= 40:
            d.text = html_text

def _expand_empty_description_after_serialize(xml_bytes, enc="windows-1251"):
    try:
        _t = xml_bytes.decode(enc, errors="replace")
        _t = _re.sub(r'<description\s*/\s*>', '<description></description>', _t)
        return _t.encode(enc, errors="replace")
    except Exception:
        return xml_bytes
# ========== END: HTML Description Beautifier ==========
    # HTML beautify for <description> (safe, end-of-pipeline)
    try:
        beautify_descriptions_html(out_root)
    except Exception as e:
        print(f"desc_html_beautify_warn: {e}")
    """Run at the very end, just before ET.tostring(): spacing + multi-punct cleanup."""
    for offer in out_root.findall(".//offer"):
        d = offer.find("description")
        if d is not None and d.text:
            try:
                t = d.text
                t = _desc_fix_punct_spacing(t)
                t = _desc_normalize_multi_punct(t)
                d.text = t
            except Exception:
                pass
# === End of minimal post-steps (added) ===


try:
    from zoneinfo import ZoneInfo  # для времени Алматы в FEED_META
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

# Категории: include/exclude/off + правила из файла
ALSTYLE_CATEGORIES_PATH = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt")
ALSTYLE_CATEGORIES_MODE = os.getenv("ALSTYLE_CATEGORIES_MODE", "off").lower()  # off|include|exclude

# Префикс для vendorCode/id
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS")

# Цены: наценка по диапазонам и форс-цена при завышенных исходных
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "100"))

# Ключевые слова
SATU_KEYWORDS          = os.getenv("SATU_KEYWORDS", "auto").lower()  # auto|off
SATU_KEYWORDS_MAXLEN   = int(os.getenv("SATU_KEYWORDS_MAXLEN", "1024"))
SATU_KEYWORDS_MAXWORDS = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "1000"))
SATU_KEYWORDS_GEO      = os.getenv("SATU_KEYWORDS_GEO", "on").lower() in {"on","1","true","yes"}
SATU_KEYWORDS_GEO_MAX  = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))
SATU_KEYWORDS_GEO_LAT  = os.getenv("SATU_KEYWORDS_GEO_LAT", "on").lower() in {"on","1","true","yes"}

# Фото-заглушки (если нет картинок)
PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.al-style.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.al-style.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.al-style.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# Публичный YML: вычищаем внутренние теги
DROP_CATEGORY_ID_TAG    = True
DROP_STOCK_TAGS         = True
PURGE_TAGS_AFTER        = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","article")
INTERNAL_PRICE_TAGS     = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                           "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice",
                           "max_price","maxPrice","oldprice")

# ======================= УТИЛИТЫ =======================
def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARN: {msg}", file=sys.stderr, flush=True)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_utc_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S %Z")

def now_almaty() -> datetime:
    if ZoneInfo:
        try:
            return datetime.now(ZoneInfo("Asia/Almaty"))
        except Exception:
            pass
    # fallback: UTC+5
    return datetime.utcfromtimestamp(time.time() + 5*3600)

def format_dt_almaty(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def next_build_time_almaty() -> datetime:
    cur = now_almaty()
    t = cur.replace(hour=1, minute=0, second=0, microsecond=0)
    return t + timedelta(days=1) if cur >= t else t

def load_source_bytes(src: str) -> bytes:
    """Скачиваем/читаем исходный XML поставщика."""
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

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag)
    return (node.text or "").strip() if node is not None and node.text else ""

def remove_all(el: ET.Element, *tags: str) -> int:
    n = 0
    for t in tags:
        for x in list(el.findall(t)):
            el.remove(x)
            n += 1
    return n

def inner_html(el: ET.Element) -> str:
    """Возвращает innerHTML тега (используем только для чтения)."""
    if el is None:
        return ""
    parts: List[str] = []
    if el.text:
        parts.append(el.text)
    for child in el:
        parts.append(ET.tostring(child, encoding="unicode"))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts).strip()

# ======================= КАТЕГОРИИ: include/exclude =======================
class CatRule:
    __slots__ = ("raw", "kind", "pattern")
    def __init__(self, raw: str, kind: str, pattern):
        self.raw, self.kind, self.pattern = raw, kind, pattern

def _norm_text(s: str) -> str:
    s = (s or "").replace("\u00A0", " ").lower().replace("ё", "е")
    return re.sub(r"\s+", " ", s).strip()

def _norm_cat(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s*[/>\|]\s*", " / ", s)
    return re.sub(r"\s+"," ", s).strip()

def load_category_rules(path: str) -> Tuple[Set[str], List[CatRule]]:
    """Читаем docs/alstyle_categories.txt: чистые ID и строки/регексы для имён путей."""
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

def parse_categories_tree(shop_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,Set[str]]]:
    id2name: Dict[str,str] = {}
    id2parent: Dict[str,str] = {}
    parent2children: Dict[str,Set[str]] = {}
    cats_root = shop_el.find("categories") or shop_el.find("Categories")
    if cats_root is None:
        return id2name, id2parent, parent2children
    for c in cats_root.findall("category"):
        cid = (c.attrib.get("id") or "").strip()
        if not cid:
            continue
        pid = (c.attrib.get("parentId") or "").strip()
        id2name[cid] = (c.text or "").strip()
        if pid:
            id2parent[cid] = pid
        parent2children.setdefault(pid, set()).add(cid)
    return id2name, id2parent, parent2children

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

def collect_descendants(ids: Set[str], parent2children: Dict[str,Set[str]]) -> Set[str]:
    out = set(ids)
    stack = list(ids)
    while stack:
        cur = stack.pop()
        for ch in parent2children.get(cur, ()):
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
    "MSI","ASUS","Acer","Lenovo","Dell","Apple","LG"
]
BRAND_ALIASES = {
    "hewlett packard":"HP","konica":"Konica Minolta","konica-minolta":"Konica Minolta",
    "powercom":"Powercom","pcm":"Powercom","apc":"APC","msi":"MSI",
    "nvprint":"NV Print","nv print":"NV Print",
    "hi black":"Hi-Black","hiblack":"Hi-Black","hi-black":"Hi-Black",
    "g&g":"G&G","gg":"G&G"
}

def normalize_brand(raw: str) -> str:
    k = _norm_key(raw)
    return "" if (not k) or (k in SUPPLIER_BLOCKLIST) else raw.strip()

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, Dict[str,int]]:
    """Чистим/нормализуем <vendor>: удаляем мусор/пустое, supplier-бренды, оставляем валидные значения."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0, {}
    normalized = 0
    dropped: Dict[str,int] = {}
    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        txt = (ven.text or "").strip() if ven is not None and ven.text else ""
        if txt:
            canon = normalize_brand(txt)
            if any(m in txt.lower() for m in UNKNOWN_VENDOR_MARKERS) or (not canon):
                if ven is not None:
                    offer.remove(ven)
                key = _norm_key(txt)
                if key:
                    dropped[key] = dropped.get(key, 0) + 1
            elif canon != txt:
                ven.text = canon
                normalized += 1
    return normalized, dropped

def build_brand_index(shop_el: ET.Element) -> Dict[str,str]:
    idx: Dict[str,str] = {}
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return idx
    for offer in offers_el.findall("offer"):
        v = offer.find("vendor")
        if v is None or not (v.text or "").strip():
            continue
        canon = v.text.strip()
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

def guess_vendor_for_offer(offer: ET.Element, brand_index: Dict[str,str]) -> str:
    name  = get_text(offer, "name")
    desc  = inner_html(offer.find("description"))  # читаем, не меняем здесь
    first = re.split(r"\s+", name.strip())[0] if name else ""
    f_norm = _norm_key(first)
    if f_norm in brand_index:
        return brand_index[f_norm]
    b = _find_brand_in_text(name) or _find_brand_in_text(desc)
    if b:
        return b
    nrm = _norm_text(name)
    for br in COMMON_BRANDS:
        if re.search(rf"\b{re.escape(_norm_text(br))}\b", nrm, flags=re.I):
            return br
    return ""

def ensure_vendor_auto_fill(shop_el: ET.Element) -> int:
    """Если <vendor> пуст — пытаемся угадать по name/description (только чтение)."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    brand_index = build_brand_index(shop_el)
    touched = 0
    for offer in offers_el.findall("offer"):
        v = offer.find("vendor")
        cur = (v.text or "").strip() if (v is not None and v.text) else ""
        if cur:
            continue
        guess = guess_vendor_for_offer(offer, brand_index)
        if guess:
            if v is None:
                v = ET.SubElement(offer, "vendor")
            v.text = guess
            brand_index[_norm_key(guess)] = guess
            touched += 1
    return touched

# ======================= ЦЕНООБРАЗОВАНИЕ =======================
PriceRule = Tuple[int,int,float,int]
PRICING_RULES: List[PriceRule] = [
    (   101,    10000, 4.0,  3000),( 10001,  25000, 4.0,  4000),( 25001,  50000, 4.0,  5000),
    ( 50001,    75000, 4.0,  7000),( 75001, 100000, 4.0, 10000),(100001, 150000, 4.0, 12000),
    (150001,   200000, 4.0, 15000),(200001, 300000, 4.0, 20000),(300001, 400000, 4.0, 25000),
    (400001,   500000, 4.0, 30000),(500001, 750000, 4.0, 40000),(750001,1000000, 4.0, 50000),
    (1000001, 1500000, 4.0, 70000),(1500001,2000000, 4.0, 90000),(2000001,100000000,4.0,100000),
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

def _force_tail_900(n: int) -> int:
    return max(int(n) // 1000, 0) * 1000 + 900 if int(n) >= 0 else 900

def compute_retail(dealer: float, rules: List[PriceRule]) -> Optional[int]:
    for lo, hi, pct, add in rules:
        if lo <= dealer <= hi:
            return _force_tail_900(dealer * (1.0 + pct / 100.0) + add)
    return None

def _remove_all_price_nodes(offer: ET.Element) -> None:
    for t in ("price", "Price"):
        for node in list(offer.findall(t)):
            offer.remove(node)

def strip_supplier_price_blocks(offer: ET.Element) -> None:
    remove_all(offer, "prices", "Prices")
    for tag in INTERNAL_PRICE_TAGS:
        remove_all(offer, tag)

def reprice_offers(shop_el: ET.Element, rules: List[PriceRule]) -> Tuple[int,int,int,Dict[str,int]]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0,0,{"missing":0})
    updated = skipped = total = 0
    src_stats = {"prices_dealer":0,"direct_field":0,"rrp_fallback":0,"missing":0}
    for offer in offers_el.findall("offer"):
        total += 1
        if offer.attrib.get("_force_price", "") == "100":
            skipped += 1
            strip_supplier_price_blocks(offer)
            continue
        dealer, src = pick_dealer_price(offer)
        src_stats[src] = src_stats.get(src, 0) + 1
        if dealer is None or dealer <= 100:
            skipped += 1
            strip_supplier_price_blocks(offer)
            continue
        newp = compute_retail(dealer, rules)
        if newp is None:
            skipped += 1
            strip_supplier_price_blocks(offer)
            continue
        _remove_all_price_nodes(offer)
        ET.SubElement(offer, "price").text = str(int(newp))
        strip_supplier_price_blocks(offer)
        updated += 1
    return updated, skipped, total, src_stats

# ======================= ПАРАМЕТРЫ/МУСОР =======================
UNWANTED_PARAM_NAME_RE = re.compile(
    r"^(?:\s*(?:благотворительн\w*|снижена\s*цена|новинк\w*|"
    r"артикул(?:\s*/\s*штрихкод)?|оригинальн\w*\s*код|штрихкод|"
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

def remove_specific_params(shop_el: ET.Element) -> int:
    """Удаляем мусорные/дублирующиеся <param> — к описанию не прикасаемся."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    removed = 0
    for offer in offers_el.findall("offer"):
        seen: Set[str] = set()
        for tag in ("param","Param"):
            for p in list(offer.findall(tag)):
                nm = (p.attrib.get("name") or "").strip()
                val = (p.text or "").strip()
                if KASPI_CODE_NAME_RE.fullmatch(nm) or UNWANTED_PARAM_NAME_RE.match(nm):
                    offer.remove(p); removed += 1; continue
                if _value_is_empty_or_noise(val):
                    offer.remove(p); removed += 1; continue
                key = _norm_text(nm)
                if key in seen:
                    offer.remove(p); removed += 1; continue
                seen.add(key)
    return removed

# ======================= ФОТО-ПЛЕЙСХОЛДЕРЫ =======================
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

def ensure_placeholder_pictures(shop_el: ET.Element) -> Tuple[int,int]:
    """Если нет <picture> — подставляем заглушку по бренду/категории/дефолт."""
    if not PLACEHOLDER_ENABLE:
        return (0,0)
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0)
    added = skipped = 0
    for offer in offers_el.findall("offer"):
        pics = list(offer.findall("picture"))
        has_pic = any((p.text or "").strip() for p in pics)
        if has_pic:
            continue
        vendor = get_text(offer, "vendor").strip()
        name   = get_text(offer, "name").strip()
        kind   = detect_kind(name)
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
        added += 1
    return (added, skipped)

# ======================= НАЛИЧИЕ/ID/ПОРЯДОК/ВАЛЮТА =======================
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

def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int,int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0,0,0)
    t_cnt=f_cnt=st_cnt=ss_cnt=0
    for offer in offers_el.findall("offer"):
        b, src = derive_available(offer)
        remove_all(offer, "available")
        offer.attrib["available"] = "true" if b else "false"
        if b: t_cnt+=1
        else: f_cnt+=1
        if src=="stock": st_cnt+=1
        if src=="status": ss_cnt+=1
        if DROP_STOCK_TAGS:
            remove_all(offer, "quantity_in_stock","quantity","stock","Stock")
    return t_cnt, f_cnt, st_cnt, ss_cnt

ARTICUL_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)

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

def ensure_vendorcode_with_article(shop_el: ET.Element, prefix: str, create_if_missing: bool = True) -> Tuple[int,int,int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0,0,0,0)
    total_prefixed=created=filled_from_art=fixed_bare=0
    for offer in offers_el.findall("offer"):
        vc = offer.find("vendorCode")
        if vc is None:
            if create_if_missing:
                vc = ET.SubElement(offer,"vendorCode"); vc.text = ""
                created += 1
            else:
                continue
        if not (vc.text or "").strip() or (vc.text or "").strip().upper() == prefix.upper():
            art = _normalize_code(offer.attrib.get("article") or "") \
               or _normalize_code(_extract_article_from_name(get_text(offer,"name"))) \
               or _normalize_code(_extract_article_from_url(get_text(offer,"url"))) \
               or _normalize_code(offer.attrib.get("id") or "")
            if art:
                vc.text = art
                filled_from_art += 1
        vc.text = f"{prefix}{(vc.text or '')}"
        total_prefixed += 1
    return total_prefixed, created, filled_from_art, fixed_bare

def sync_offer_id_with_vendorcode(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    changed = 0
    for offer in offers_el.findall("offer"):
        vc = offer.find("vendorCode")
        if vc is None or not (vc.text or "").strip():
            continue
        new_id = (vc.text or "").strip()
        if offer.attrib.get("id") != new_id:
            offer.attrib["id"] = new_id
            changed += 1
    return changed

def purge_offer_tags_and_attrs_after(offer: ET.Element) -> Tuple[int,int]:
    removed_tags = removed_attrs = 0
    for t in PURGE_TAGS_AFTER:
        for node in list(offer.findall(t)):
            offer.remove(node); removed_tags += 1
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib:
            offer.attrib.pop(a, None); removed_attrs += 1
    return removed_tags, removed_attrs

def fix_currency_id(shop_el: ET.Element, default_code: str = "KZT") -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        remove_all(offer, "currencyId")
        ET.SubElement(offer, "currencyId").text = default_code
        touched += 1
    return touched

DESIRED_ORDER = ["vendorCode","name","price","picture","vendor","currencyId","description"]
def reorder_offer_children(shop_el: ET.Element) -> int:
    """Переупорядочиваем теги в оффере (описание не трогаем по содержимому)."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    changed = 0
    for offer in offers_el.findall("offer"):
        children = list(offer)
        if not children:
            continue
        buckets: Dict[str,List[ET.Element]] = {k: [] for k in DESIRED_ORDER}
        others: List[ET.Element] = []
        for node in children:
            (buckets[node.tag] if node.tag in buckets else others).append(node)
        rebuilt = [*sum((buckets[k] for k in DESIRED_ORDER), []), *others]
        if rebuilt != children:
            for node in children:
                offer.remove(node)
            for node in rebuilt:
                offer.append(node)
            changed += 1
    return changed

def ensure_categoryid_zero_first(shop_el: ET.Element) -> int:
    """Вставляем <categoryId>0</categoryId> первым элементом оффера (по требованию пользователя)."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        remove_all(offer, "categoryId", "CategoryId")
        cid = ET.Element("categoryId"); cid.text = os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0, cid)
        touched += 1
    return touched

# ======================= КЛЮЧЕВЫЕ СЛОВА =======================
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
AS_INTERNAL_ART_RE = re.compile(r"^AS\d+", re.I)

def extract_model_tokens(offer: ET.Element) -> List[str]:
    """Извлекаем модельные токены из name/description (description только читаем)."""
    tokens: Set[str] = set()
    for src in (get_text(offer,"name"), inner_html(offer.find("description"))):
        if not src:
            continue
        for m in MODEL_RE.findall(src or ""):
            t = m.upper()
            if AS_INTERNAL_ART_RE.match(t) or not (re.search(r"[A-Z]", t) and re.search(r"\d", t)) or len(t) < 5:
                continue
            tokens.add(t)
    return list(tokens)

def keywords_from_name_generic(name: str) -> List[str]:
    raw_tokens = tokenize_name(name or "")
    modelish   = [t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content    = [t for t in raw_tokens if is_content_word(t)]
    bigr       = build_bigrams(content)
    norm = lambda tok: tok if re.search(r"[A-Z]{2,}", tok) else tok.capitalize()
    out = modelish[:8] + bigr[:8] + [norm(t) for t in content[:10]]
    return dedup_preserve_order(out)

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

def build_keywords_for_offer(offer: ET.Element) -> str:
    if SATU_KEYWORDS == "off":
        return ""
    name   = get_text(offer, "name")
    vendor = get_text(offer, "vendor").strip()
    parts: List[str] = [vendor] if vendor else []
    parts += extract_model_tokens(offer) + keywords_from_name_generic(name) + color_tokens(name)
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

def ensure_keywords(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        kw = build_keywords_for_offer(offer)
        node = offer.find("keywords")
        if not kw:
            if node is not None:
                offer.remove(node)
            continue
        if node is None:
            node = ET.SubElement(offer, "keywords")
            node.text = kw
            touched += 1
        else:
            if (node.text or "") != kw:
                node.text = kw
                touched += 1
    return touched

# ======================= ПРОЧЕЕ =======================
def flag_unrealistic_supplier_prices(shop_el: ET.Element) -> int:
    """Помечаем офферы с ценами выше порога — затем принудительно ставим цену=PRICE_CAP_VALUE."""
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    flagged = 0
    for offer in offers_el.findall("offer"):
        try:
            p_txt = get_text(offer, "price")
            src_p = float(p_txt.replace(",", ".")) if p_txt else None
        except Exception:
            src_p = None
        if src_p is not None and src_p >= PRICE_CAP_THRESHOLD:
            offer.attrib["_force_price"] = str(PRICE_CAP_VALUE)
            flagged += 1
    return flagged

def enforce_forced_prices(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        if offer.attrib.get("_force_price"):
            _remove_all_price_nodes(offer)
            ET.SubElement(offer, "price").text = str(PRICE_CAP_VALUE)
            offer.attrib.pop("_force_price", None)
            touched += 1
    return touched

def render_feed_meta_comment(pairs: Dict[str,str]) -> str:
    rows = [
        ("Поставщик", pairs.get("supplier","")),
        ("URL поставщика", pairs.get("source","")),
        ("Время сборки (Алматы)", pairs.get("built_alm","")),
        ("Ближайшая сборка (Алматы)", pairs.get("next_build_alm","")),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total","0"))),
        ("Сколько товаров у поставщика после фильтра", str(pairs.get("offers_written","0"))),
        ("Сколько товаров есть в наличии (true)", str(pairs.get("available_true","0"))),
        ("Сколько товаров нет в наличии (false)", str(pairs.get("available_false","0"))),
    ]
    key_w = max(len(k) for k,_ in rows)
    lines = ["FEED_META"] + [f"{k.ljust(key_w)} | {v}" for k,v in rows]
    return "\n".join(lines)

# ======================= ФИНАЛЬНАЯ НОРМАЛИЗАЦИЯ DESCRIPTION (ПОДХОД 2) =======================
DESC_TAG_STRIP_RE = re.compile(r"<[^>]+>")

def _flatten_desc_text(desc_el: ET.Element) -> Optional[str]:
    # Берём inner HTML, убираем все теги, декодируем сущности, схлопываем пробелы/переносы
    raw_html = inner_html(desc_el)
    if not raw_html:
        return None
    txt = DESC_TAG_STRIP_RE.sub(" ", raw_html)          # теги -> пробел
    txt = html.unescape(txt)                            # &nbsp; &quot; и т.п.
    txt = txt.replace("\u00A0", " ")
    # Схлопываем все виды пробельных символов в один пробел
    txt = re.sub(r"\s+", " ", txt, flags=re.UNICODE).strip()
    return txt or None

def flatten_all_descriptions(shop_el: ET.Element) -> int:
    """Подход 2: превратить любое содержимое <description> в одну чистую строку текста.
       Пустые описания не трогаем. Никаких HTML-тегов не добавляем.
    """
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        d = offer.find("description")
        if d is None:
            continue
        new_text = _flatten_desc_text(d)
        if new_text is None:
            # есть description, но пустое — оставим как есть
            continue
        # Заменяем текст, удаляем всех детей (чтобы Tree не расставлял отступы внутри)
        d.text = new_text
        for ch in list(d):
            d.remove(ch)
        touched += 1
    return touched

# ======================= MAIN =======================
def main() -> None:
    log(f"Source: {SUPPLIER_URL if SUPPLIER_URL else '(not set)'}")
    data = load_source_bytes(SUPPLIER_URL)

    src_root = ET.fromstring(data)
    shop_in  = src_root.find("shop") if src_root.tag.lower() != "shop" else src_root
    if shop_in is None:
        err("XML: <shop> not found")

    offers_in_el = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None:
        err("XML: <offers> not found")

    src_offers = list(offers_in_el.findall("offer"))

    # Готовим выходную структуру
    out_root  = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop  = ET.SubElement(out_root, "shop")
    out_offers= ET.SubElement(out_shop, "offers")

    # 1) Копируем офферы 1:1 (дальше работаем только над полями, описание пока не трогаем)
    for o in src_offers:
        out_offers.append(deepcopy(o))

    # 2) Фильтр категорий
    removed_count = 0
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        id2name,id2parent,parent2children = parse_categories_tree(shop_in)
        rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH)
        keep_ids: Set[str] = set(rules_ids)

        if rules_names and id2name:
            for cid in id2name.keys():
                path = build_category_path_from_id(cid, id2name, id2parent)
                if category_matches_name(path, rules_names):
                    keep_ids.add(cid)

        if keep_ids and parent2children:
            keep_ids = collect_descendants(keep_ids, parent2children)

        for off in list(out_offers.findall("offer")):
            cid = get_text(off, "categoryId")
            hit = (cid in keep_ids) if cid else False
            drop = (ALSTYLE_CATEGORIES_MODE == "exclude" and hit) or (ALSTYLE_CATEGORIES_MODE == "include" and not hit)
            if drop:
                out_offers.remove(off); removed_count += 1

        log(f"Category rules ({ALSTYLE_CATEGORIES_MODE}): removed={removed_count}")
    else:
        log("Category rules (off): removed=0")

    # 3) Удаляем исходные categoryId (позже поставим 0 первым тегом)
    if DROP_CATEGORY_ID_TAG:
        for off in out_offers.findall("offer"):
            for node in list(off.findall("categoryId")) + list(off.findall("CategoryId")):
                off.remove(node)

    # 4) Флаг/форсирование цен
    flagged = flag_unrealistic_supplier_prices(out_shop); log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")

    # 5) Бренды
    ensure_vendor(out_shop)
    filled = ensure_vendor_auto_fill(out_shop); log(f"Vendors auto-filled: {filled}")

    # 6) vendorCode/id
    ensure_vendorcode_with_article(out_shop, prefix=VENDORCODE_PREFIX, create_if_missing=True)
    sync_offer_id_with_vendorcode(out_shop)

    # 7) Пересчёт розницы + принудительные цены
    reprice_offers(out_shop, PRICING_RULES)
    forced = enforce_forced_prices(out_shop); log(f"Forced price={PRICE_CAP_VALUE}: {forced}")

    # 8) Чистим мусорные <param>
    removed_params = remove_specific_params(out_shop); log(f"Params removed: {removed_params}")

    # 9) Фото-заглушки (если нет ни одной картинки)
    ph_added, _ = ensure_placeholder_pictures(out_shop); log(f"Placeholders added: {ph_added}")

    # 10) available -> в атрибут оффера, удаляем складские поля
    t_true, t_false, _, _ = normalize_available_field(out_shop)

    # 11) Валюта
    fix_currency_id(out_shop, default_code="KZT")

    # 12) Чистка служебных тегов/атрибутов
    for off in out_offers.findall("offer"):
        purge_offer_tags_and_attrs_after(off)

    # 13) Порядок тегов + categoryId=0 первым
    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)

    # 14) Ключевые слова (описание только ЧИТАЕМ при извлечении моделей)
    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    # 15) ПЛОСКАЯ нормализация описаний (ПОДХОД 2): одна строка, без HTML-тегов
    desc_touched = flatten_all_descriptions(out_shop); log(f"Descriptions flattened: {desc_touched}")

    # Красивые отступы (Python 3.9+). На плоский текст внутри <description> это не влияет.
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # FEED_META
    built_alm = now_almaty()
    meta_pairs = {
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL or "file",
        "built_alm": format_dt_almaty(built_alm),
        "next_build_alm": format_dt_almaty(next_build_time_almaty()),
        "offers_total": len(src_offers),
        "offers_written": len(list(out_offers.findall("offer"))),
        "available_true": str(t_true),
        "available_false": str(t_false),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # Сериализация
    # FINAL STEP (safe): description spacing & multi-punct normalization
    try:
        fix_all_descriptions_end(out_root)
    except Exception as _e:
        print(f"desc_end_fix_warn: {_e}")
    xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True)

    # expand self-closing <description /> to explicit open/close
    try:
        xml_bytes = _expand_empty_description_after_serialize(xml_bytes, ENC)
    except Exception as e:
        print(f"desc_selfclose_fix_warn: {e}")
    # POST-SERIALIZATION: expand self-closing <description /> to <description></description>
    try:
        _enc = ENC if 'ENC' in globals() else 'windows-1251'
        _txt = xml_bytes.decode(_enc, errors='replace')
        import re as _re
        _txt = _re.sub(r'<description\s*/>', '<description></description>', _txt)
        xml_bytes = _txt.encode(_enc, errors='replace')
    except Exception as _e:
        print(f"desc_selfclose_fix_warn: {_e}")

    xml_text  = xml_bytes.decode(ENC, errors="replace")

    # Лёгкая косметика: перенос после FEED_META и пустая строка между офферами
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
        # Безопасное сохранение с заменой неподдерживаемых символов на XML-референсы
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

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | description=DESC-FLAT")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
