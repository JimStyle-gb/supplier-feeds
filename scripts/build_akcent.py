# -*- coding: utf-8 -*-
"""
Build Akcent YML/XML (flat <offers>) with FEED_META.

Главное:
- Без <categories>; любые <categoryId> внутри офферов удаляем.
- <available>true</available> принудительно; складские/quantity-теги чистим.
- Цена: минимальная дилерская -> +4% + фикс по диапазону -> хвост ...900; <currencyId>KZT</currencyId>.
- Внутренние ценовые теги (<prices>, purchase/wholesale/b2b/oldprice и т.п.) вычищаем.
- vendor: никогда не ставим имена поставщиков (alstyle, copyline, vtt, akcent); NV Print — можно; остальные бренды — из allow-list.
- vendorCode: всегда с префиксом "AC". Если пусто/только префикс — достаём артикул (offer@article -> из <name> -> из <url> -> offer@id).
- Параметры фильтруем и вплавляем в <description> между [SPECS_BEGIN]/[SPECS_END], после чего <param> удаляем.
- FEED_META: подробный многострочный комментарий вверху; добавлены счётчики по vendorCode.
- Выход: docs/akcent.yml (Windows-1251) + копия docs/akcent.xml; создаём docs/.nojekyll.
"""

from __future__ import annotations

import os, sys, re, time, random, urllib.parse
from copy import deepcopy
from typing import Dict, List, Set, Tuple, Optional
from xml.etree import ElementTree as ET
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests

# ===================== SETTINGS =====================
SUPPLIER_NAME   = os.getenv("SUPPLIER_NAME", "akcent")
SUPPLIER_URL    = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml")

OUT_FILE_YML    = os.getenv("OUT_FILE", "docs/akcent.yml")  # публичный файл (XML/YML)
OUT_FILE_XML    = "docs/akcent.xml"                         # копия
ENC             = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "30"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF   = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))

DRY_RUN         = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AC")
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "1").lower() in {"1","true","yes"}

STRICT_VENDOR_ALLOWLIST = True        # строгий allow-list брендов
ALWAYS_AVAILABLE_TRUE   = True        # всегда <available>true</available>
DROP_STOCK_TAGS         = True        # чистим quantity/stock

DROP_CATEGORY_TREE      = True
DROP_CATEGORY_ID_TAG    = True

STRIP_INTERNAL_PRICE_TAGS = True
INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice",
)

EMBED_SPECS_IN_DESCRIPTION = True
SPECS_BEGIN_MARK = "[SPECS_BEGIN]"
SPECS_END_MARK   = "[SPECS_END]"
STRIP_ALL_PARAMS_AFTER_EMBED = True   # после вплавления характеристик удаляем все <param>/<Param>

PURGE_TAGS_AFTER = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model")
PURGE_OFFER_ATTRS_AFTER = ("type",)  # ВАЖНО: article НЕ трогаем до извлечения артикула!

# ===================== UTILS =====================
def log(msg: str) -> None: print(msg, flush=True)
def warn(msg: str) -> None: print(f"WARN: {msg}", flush=True, file=sys.stderr)
def err(msg: str, code: int = 1) -> None: print(f"ERROR: {msg}", flush=True, file=sys.stderr); sys.exit(code)

def now_utc_str() -> str: return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
def now_almaty_str() -> str:
    if ZoneInfo: return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag); return (node.text or "").strip() if node is not None and node.text else ""

def set_text(el: ET.Element, text: str) -> None: el.text = text if text is not None else ""

def iter_local(elem: ET.Element, name: str):
    for child in elem.findall(name):
        yield child

def _children_ci(el: ET.Element, name_lc: str):
    for ch in list(el):
        if ch.tag.lower() == name_lc:
            yield ch

# ===================== HTTP fetch =====================
def fetch_xml(url: str, timeout: int, retries: int, backoff: float) -> bytes:
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = sess.get(url, headers=headers, timeout=timeout, stream=True)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            data = resp.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)} bytes)")
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if not any(t in ctype for t in ("xml","text/plain","application/octet-stream")):
                head = data[:64].lstrip()
                if not head.startswith(b"<"):
                    raise RuntimeError(f"unexpected content-type: {ctype!r}")
            return data
        except Exception as e:
            last_exc = e
            sleep_s = backoff * attempt * max(0.5, 1.0 + random.uniform(-0.2, 0.2))
            warn(f"fetch attempt {attempt}/{retries} failed: {e}; sleep {sleep_s:.2f}s")
            if attempt < retries:
                time.sleep(sleep_s)
    raise RuntimeError(f"fetch failed after {retries} attempts: {last_exc}")

# ===================== Brands & Vendor =====================
def _norm_key(s: str) -> str:
    if not s: return ""
    s = s.strip().lower().replace("ё","е")
    s = re.sub(r"[-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

SUPPLIER_BLOCKLIST = {_norm_key(x) for x in ["alstyle","al-style","copyline","vtt","akcent","ak-cent","nvprint","nv print"]}
SUPPLIER_BLOCKLIST -= {"nv print", "nvprint"}  # NV Print разрешён

ALLOWED_BRANDS_CANONICAL = [
    "HP","Canon","Brother","Kyocera","Xerox","Ricoh","Epson","Samsung","Panasonic",
    "Konica Minolta","Sharp","Lexmark","Pantum","NV Print",
    "APC","ASRock","BenQ","BYINTEK","CET","Colorfix","Comix","CyberPower","Dahua","Deluxe",
    "Eaton","Europrint","Fellowes","GAMEMAX","Gigabyte","Hikvision","HSM","Huawei","HyperX",
    "iiyama","Katun","Legrand","LG","Mi","MSI","Rowe","Schneider Electric","SHIP","SVC",
    "Tecno","Wanbo","XG","XGIMI","Xiaomi","Zowie","ДКС","ViewSonic","Mr.Pixel",
]
ALLOWED_BRANDS_CANON_MAP: Dict[str,str] = { _norm_key(b): b for b in ALLOWED_BRANDS_CANONICAL }
ALLOWED_CANON_SET: Set[str] = set(ALLOWED_BRANDS_CANONICAL)

_BRAND_MAP = {
    "hewlett packard": "HP", "hp inc": "HP",
    "nvprint": "NV Print", "nv  print": "NV Print", "nv print": "NV Print",
    "konica": "Konica Minolta", "kyocera mita": "Kyocera",
    "viewsonic": "ViewSonic", "mr pixel": "Mr.Pixel",
    "asrock": "ASRock","benq":"BenQ","byintek":"BYINTEK","cet":"CET","colorfix":"Colorfix","comix":"Comix",
    "cyber power":"CyberPower","cyberpower":"CyberPower","deluxe":"Deluxe","fellowes":"Fellowes","gamemax":"GAMEMAX",
    "gigabyte":"Gigabyte","hikvision":"Hikvision","hsm":"HSM","huawei":"Huawei","hyperx":"HyperX",
    "iiyama":"iiyama","katun":"Katun","lg":"LG","mi":"Mi","msi":"MSI","rowe":"Rowe","schneiderelectric":"Schneider Electric",
    "schneider electric":"Schneider Electric","ship":"SHIP","svc":"SVC","tecno":"Tecno","wanbo":"Wanbo","xg":"XG",
    "xgimi":"XGIMI","xiaomi":"Xiaomi","zowie":"Zowie","дкс":"ДКС","europrint":"Europrint",
}

_BRAND_PATTERNS = [
    (re.compile(r"\bhp\b", re.I), "HP"),
    (re.compile(r"\bcanon\b", re.I), "Canon"),
    (re.compile(r"\bbrother\b", re.I), "Brother"),
    (re.compile(r"\bkyocera\b", re.I), "Kyocera"),
    (re.compile(r"\bxerox\b", re.I), "Xerox"),
    (re.compile(r"\bricoh\b", re.I), "Ricoh"),
    (re.compile(r"\bepson\b", re.I), "Epson"),
    (re.compile(r"\bsamsung\b", re.I), "Samsung"),
    (re.compile(r"\bpanasonic\b", re.I), "Panasonic"),
    (re.compile(r"\bkonica\s*-?\s*minolta\b", re.I), "Konica Minolta"),
    (re.compile(r"\bsharp\b", re.I), "Sharp"),
    (re.compile(r"\blexmark\b", re.I), "Lexmark"),
    (re.compile(r"\bpantum\b", re.I), "Pantum"),
    (re.compile(r"\bnv\s*-?\s*print\b", re.I), "NV Print"),
    (re.compile(r"\bviewsonic\b", re.I), "ViewSonic"),
    (re.compile(r"\bhyperx\b", re.I), "HyperX"),
    (re.compile(r"\bmr\.?\s*pixel\b", re.I), "Mr.Pixel"),
    (re.compile(r"\basrock\b", re.I), "ASRock"),
    (re.compile(r"\bbenq\b", re.I), "BenQ"),
    (re.compile(r"\bbyintek\b", re.I), "BYINTEK"),
    (re.compile(r"\bcet\b", re.I), "CET"),
    (re.compile(r"\bcolorfix\b", re.I), "Colorfix"),
    (re.compile(r"\bcomix\b", re.I), "Comix"),
    (re.compile(r"\bcyber\s*power\b", re.I), "CyberPower"),
    (re.compile(r"\bdeluxe\b", re.I), "Deluxe"),
    (re.compile(r"\bfellowes\b", re.I), "Fellowes"),
    (re.compile(r"\bgamemax\b", re.I), "GAMEMAX"),
    (re.compile(r"\bgigabyte\b", re.I), "Gigabyte"),
    (re.compile(r"\bhikvision\b", re.I), "Hikvision"),
    (re.compile(r"\bhsm\b", re.I), "HSM"),
    (re.compile(r"\bhuawei\b", re.I), "Huawei"),
    (re.compile(r"\biiyama\b", re.I), "iiyama"),
    (re.compile(r"\bkatun\b", re.I), "Katun"),
    (re.compile(r"\blg\b", re.I), "LG"),
    (re.compile(r"\bmsi\b", re.I), "MSI"),
    (re.compile(r"\browe\b", re.I), "Rowe"),
    (re.compile(r"\bschneider\s*electric\b", re.I), "Schneider Electric"),
    (re.compile(r"\bship\b", re.I), "SHIP"),
    (re.compile(r"\bsvc\b", re.I), "SVC"),
    (re.compile(r"\btecno\b", re.I), "Tecno"),
    (re.compile(r"\bwanbo\b", re.I), "Wanbo"),
    (re.compile(r"\bxgimi\b", re.I), "XGIMI"),
    (re.compile(r"\bxg\b", re.I), "XG"),
    (re.compile(r"\bxiaomi\b", re.I), "Xiaomi"),
    (re.compile(r"\bzowie\b", re.I), "Zowie"),
    (re.compile(r"\bдкс\b", re.I), "ДКС"),
    (re.compile(r"\beuro\s*print\b", re.I), "Europrint"),
    (re.compile(r"\beaton\b", re.I), "Eaton"),
    (re.compile(r"\blegrand\b", re.I), "Legrand"),
    (re.compile(r"\bfellowes\b", re.I), "Fellowes"),
]

UNKNOWN_VENDOR_MARKERS = ("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

def brand_allowed(canon: str) -> bool:
    if not STRICT_VENDOR_ALLOWLIST: return True
    return canon in ALLOWED_CANON_SET

def normalize_brand(raw: str) -> str:
    k = _norm_key(raw)
    if (not k) or (k in SUPPLIER_BLOCKLIST):
        return ""
    if k in ALLOWED_BRANDS_CANON_MAP:
        return ALLOWED_BRANDS_CANON_MAP[k]
    if k in _BRAND_MAP:
        cand = _BRAND_MAP[k]
        return cand if brand_allowed(cand) else ""
    for rg, val in _BRAND_PATTERNS:
        if rg.search(raw or ""):
            return val if brand_allowed(val) else ""
    return ""

def scan_text_for_allowed_brand(text: str) -> str:
    if not text: return ""
    for rg, val in _BRAND_PATTERNS:
        if rg.search(text) and brand_allowed(val): return val
    for allowed in ALLOWED_CANON_SET:
        if re.search(rf"\b{re.escape(allowed)}\b", text, re.I): return allowed
    return ""

def extract_brand_from_name(name: str) -> str:
    if not name: return ""
    for rg, val in _BRAND_PATTERNS:
        if rg.search(name) and brand_allowed(val): return val
    head = re.split(r"[–—\-:\(\)\[\],;|/]{1,}", name, maxsplit=1)[0].strip()
    if head:
        cand = normalize_brand(head)
        if cand and brand_allowed(cand): return cand
    return ""

def extract_brand_from_params(offer: ET.Element) -> str:
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        nm = (p.attrib.get("name") or "").strip().lower()
        if "бренд" in nm or "производ" in nm or "manufacturer" in nm or "brand" in nm:
            cand = normalize_brand((p.text or "").strip())
            if cand and brand_allowed(cand): return cand
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        txt = (p.text or "").strip()
        cand = scan_text_for_allowed_brand(txt)
        if cand: return cand
    return ""

def extract_brand_any(offer: ET.Element) -> str:
    return (extract_brand_from_params(offer)
            or extract_brand_from_name(get_text(offer, "name"))
            or scan_text_for_allowed_brand(get_text(offer, "description")))

def ensure_vendor(shop_el: ET.Element) -> Tuple[int,int,int,Dict[str,int]]:
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0,0,{})
    normalized=filled_param=filled_text=0
    dropped_names: Dict[str,int] = {}
    def drop_name(nm: str):
        if not nm: return
        key = _norm_key(nm); dropped_names[key] = dropped_names.get(key, 0) + 1

    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        txt_raw = (ven.text or "").strip() if ven is not None and ven.text else ""
        def clear_vendor():
            if ven is not None:
                drop_name(ven.text or "")
                offer.remove(ven)

        # 1) Есть бренд в vendor
        if txt_raw:
            if any(m in txt_raw.lower() for m in UNKNOWN_VENDOR_MARKERS) or (_norm_key(txt_raw) in SUPPLIER_BLOCKLIST):
                clear_vendor(); ven=None; txt_raw=""
            else:
                canon = normalize_brand(txt_raw)
                if not canon:
                    clear_vendor(); ven=None; txt_raw=""
                else:
                    if canon != txt_raw: ven.text = canon; normalized += 1
                    continue

        # 2) Из параметров
        candp = extract_brand_from_params(offer)
        if candp:
            ET.SubElement(offer, "vendor").text = candp
            filled_param += 1; continue

        # 3) Из имени/описания
        candt = extract_brand_any(offer)
        if candt:
            ET.SubElement(offer, "vendor").text = candt
            filled_text += 1; continue

    return (normalized,filled_param,filled_text,dropped_names)

# ===================== Pricing =====================
PriceRule = Tuple[int,int,float,int]
PRICING_RULES: List[PriceRule] = [
    (   101,    10000, 4.0,  3000),
    ( 10001,    25000, 4.0,  4000),
    ( 25001,    50000, 4.0,  5000),
    ( 50001,    75000, 4.0,  7000),
    ( 75001,   100000, 4.0, 10000),
    (100001,   150000, 4.0, 12000),
    (150001,   200000, 4.0, 15000),
    (200001,   300000, 4.0, 20000),
    (300001,   400000, 4.0, 25000),
    (400001,   500000, 4.0, 30000),
    (500001,   750000, 4.0, 40000),
    (750001,  1000000, 4.0, 50000),
    (1000001, 1500000, 4.0, 70000),
    (1500001, 2000000, 4.0, 90000),
    (2000001,100000000,4.0,100000),
]

def parse_price_number(raw: str) -> Optional[float]:
    if raw is None: return None
    s = raw.strip()
    if not s: return None
    s = (s.replace("\xa0"," ").replace(" ","")
           .replace("KZT","").replace("kzt","").replace("₸","")
           .replace(",","."))  # запятая -> точка
    try:
        val = float(s); return val if val > 0 else None
    except Exception:
        return None

PRICE_FIELDS = ["purchasePrice","purchase_price","wholesalePrice","wholesale_price",
                "opt_price","b2bPrice","b2b_price","price","oldprice"]

def get_dealer_price(offer: ET.Element) -> Optional[float]:
    vals: List[float] = []
    # известные плоские теги
    for tag in PRICE_FIELDS:
        el = offer.find(tag)
        if el is not None and el.text:
            v = parse_price_number(el.text)
            if v is not None: vals.append(v)
    # вложенные <prices><price>
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            v = parse_price_number(p.text or "")
            if v is not None: vals.append(v)
    return min(vals) if vals else None

def _force_tail_900(n: float) -> int:
    i = int(n)
    k = max(i // 1000, 0)
    out = k * 1000 + 900
    return out if out >= 900 else 900

def compute_retail(dealer: float, rules: List[PriceRule]) -> Optional[int]:
    for lo,hi,pct,add in rules:
        if lo <= dealer <= hi:
            val = dealer * (1.0 + pct/100.0) + add
            return _force_tail_900(val)
    return None

def reprice_offers(shop_el: ET.Element, rules: List[PriceRule]) -> Tuple[int,int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0,0)
    updated=skipped=total=0
    for offer in offers_el.findall("offer"):
        total += 1
        dealer = get_dealer_price(offer)
        if dealer is None or dealer <= 100:
            skipped += 1
            if (oldp := offer.find("oldprice")) is not None: offer.remove(oldp)
            continue
        new_price = compute_retail(dealer, rules)
        if new_price is None:
            skipped += 1
            if (oldp := offer.find("oldprice")) is not None: offer.remove(oldp)
            continue
        p = offer.find("price") or ET.SubElement(offer, "price")
        p.text = str(int(new_price))
        cur = offer.find("currencyId") or ET.SubElement(offer, "currencyId")
        cur.text = "KZT"
        # вычистим вложенные цены/служебные теги
        for node in list(offer.findall("prices")) + list(offer.findall("Prices")):
            offer.remove(node)
        for tag in INTERNAL_PRICE_TAGS:
            node = offer.find(tag)
            if node is not None: offer.remove(node)
        if (oldp := offer.find("oldprice")) is not None: offer.remove(oldp)
        updated += 1
    return updated, skipped, total

# ===================== Specs / Params / Stock =====================
def _key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

EXCLUDE_NAME_RE = re.compile(
    r"(новинк|акци|скидк|уценк|снижена\s*цена|хит продаж|топ продаж|лидер продаж|лучшая цена|"
    r"рекомендуем|подарок|к[еэ]шб[еэ]к|предзаказ|статус|ед(иница)?\s*измерени|базовая единиц|"
    r"vat|ндс|налог|доставк|самовывоз|срок поставки|кредит|рассрочк|наличие\b)",
    re.I
)

def _parse_dims(val: str) -> str:
    s = re.sub(r"\s+", " ", (val or "").strip()).replace("х", "x").replace("Х", "x").replace("*", "x")
    parts = re.split(r"[x×X]", s)
    nums: List[str] = []
    for p in parts:
        p = re.sub(r"[^\d.,]", "", p).replace(",", ".")
        if not p: continue
        try:
            n = float(p)
            nums.append(str(int(n)) if abs(n-int(n))<1e-6 else f"{n:g}")
        except Exception:
            pass
    return "x".join(nums)

def _normalize_weight_value(raw_val: str) -> str:
    s = re.sub(r"\s+", " ", (raw_val or "").strip())
    if not s: return s
    if re.search(r"\b(кг|kg)\b", s, re.I):
        return re.sub(r"\s*kg\b", " кг", s, flags=re.I)
    m = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*(?:г|g)\b", s, re.I)
    if m:
        val = float(m.group(1).replace(",", "."))
        if val >= 1000:
            kg = val / 1000.0
            return f"{int(kg)} кг" if abs(kg-int(kg))<1e-6 else f"{kg:.3g} кг"
        else:
            return re.sub(r"\bg\b", "г", f"{val:g} г", flags=re.I)
    if re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", s):
        v = float(s.replace(",", "."))
        return f"{int(v)} кг" if abs(v-int(v))<1e-6 else f"{v:.3g} кг"
    return s

def _looks_like_code_value(v: str) -> bool:
    s = (v or "").strip()
    if not s: return True
    if re.search(r"https?://", s, re.I): return True
    clean = re.sub(r"[0-9\-\_/ ]", "", s)
    ratio = len(clean) / max(len(s), 1)
    return ratio < 0.3

def build_specs_lines(offer: ET.Element) -> List[str]:
    lines: List[str] = []; seen: Set[str] = set()
    WEIGHT_KEYS = {"вес","масса","weight","net weight","gross weight"}
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name = (p.attrib.get("name") or "").strip()
        raw_val  = (p.text or "").strip()
        if not raw_name or not raw_val: continue
        k = _key(raw_name)
        if k in {
            "артикул","штрихкод","код тн вэд","код","снижена цена","скидка","акция","уценка","новинка","хит продаж","топ продаж",
            "лидер продаж","лучшая цена","рекомендуем","подарок","кэшбэк","кешбэк","предзаказ","статус","доставка","самовывоз",
            "срок поставки","наличие","кредит","рассрочка","единица измерения","базовая единица","vat","ндс","налог","сертификат",
            "сертификация","благотворительность",
        }: continue
        if EXCLUDE_NAME_RE.search(raw_name): continue
        is_weight = k in WEIGHT_KEYS
        if k.startswith("габариты"):
            raw_val = _parse_dims(raw_val) or raw_val
        elif is_weight:
            raw_val = _normalize_weight_value(raw_val)
        if (not is_weight) and _looks_like_code_value(raw_val): continue
        if re.fullmatch(r"(да|нет|y|n)", raw_val.strip(), re.I) and k in {
            "назначение","скидка","акция","снижена цена","благотворительность","наличие","статус"
        }: continue
        if k in seen: continue
        seen.add(k)
        lines.append(f"- {raw_name}: {raw_val}")
    return lines

def inject_specs_block(shop_el: ET.Element) -> Tuple[int,int]:
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0)
    offers_touched=0; lines_total=0
    spec_re = re.compile(re.escape(SPECS_BEGIN_MARK) + r".*?" + re.escape(SPECS_END_MARK), re.S)
    for offer in offers_el.findall("offer"):
        lines = build_specs_lines(offer)
        if not lines: continue
        desc_el = offer.find("description")
        curr = get_text(offer, "description")
        if curr: curr = spec_re.sub("", curr).strip()  # вырезаем старый блок SPECS, если был
        block = f"{SPECS_BEGIN_MARK}\nХарактеристики:\n" + "\n".join(lines) + f"\n{SPECS_END_MARK}"
        new_text = (curr + "\n\n" + block).strip() if curr else block
        if desc_el is None: desc_el = ET.SubElement(offer, "description")
        set_text(desc_el, new_text)
        offers_touched += 1; lines_total += len(lines)
    return offers_touched, lines_total

def strip_all_params(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None: return 0
    removed = 0
    for offer in offers_el.findall("offer"):
        for p in list(offer.findall("param")) + list(offer.findall("Param")):
            offer.remove(p); removed += 1
    return removed

def normalize_stock_always_true(shop_el: ET.Element) -> int:
    offers_el = shop_el.find("offers")
    if offers_el is None: return 0
    touched = 0
    for offer in offers_el.findall("offer"):
        avail = offer.find("available") or ET.SubElement(offer, "available")
        avail.text = "true"
        touched += 1
        if DROP_STOCK_TAGS:
            for tag in ["quantity_in_stock","quantity","stock","Stock"]:
                for node in list(offer.findall(tag)):
                    offer.remove(node)
    return touched

# ===================== Offer cleanup/helpers =====================
ARTICUL_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)

def _extract_article_from_name(name: str) -> str:
    if not name: return ""
    m = ARTICUL_RE.search(name)
    return (m.group(1) if m else "").upper()

def _extract_article_from_url(url: str) -> str:
    if not url: return ""
    try:
        path = urllib.parse.urlparse(url).path.rstrip("/")
        last = path.split("/")[-1]
        last = re.sub(r"\.(html?|php|aspx?)$", "", last, flags=re.I)
        m = ARTICUL_RE.search(last)
        return (m.group(1) if m else last).upper()
    except Exception:
        return ""

def _normalize_code(s: str) -> str:
    s = (s or "").strip()
    if not s: return ""
    s = re.sub(r"[\s_]+", "", s)
    s = s.replace("—","-").replace("–","-")
    s = re.sub(r"[^A-Za-z0-9\-]+", "", s)
    return s.upper()

def ensure_vendorcode_with_article(shop_el: ET.Element, prefix: str, create_if_missing: bool=False) -> Tuple[int,int,int,int]:
    """
    Возвращает: (prefixed_total, created_nodes, filled_from_article, fixed_bare_prefix)
    Логика:
    - Если <vendorCode> нет и create_if_missing=True — создаём пустой.
    - Если <vendorCode> пуст/равен только префиксу — берём артикул по приоритету:
      offer@article -> из <name> -> из <url> -> offer@id.
    - После этого всегда добавляем префикс (без дефиса).
    """
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
    total_prefixed=created=filled_from_art=fixed_bare=0

    for offer in offers_el.findall("offer"):
        vc = offer.find("vendorCode")
        if vc is None:
            if create_if_missing:
                vc = ET.SubElement(offer, "vendorCode")
                vc.text = ""
                created += 1
            else:
                continue

        old = (vc.text or "").strip()

        if (old == "") or (old.upper() == prefix.upper()):
            # 1) из атрибута article (не удаляем его до этой точки)
            art = _normalize_code(offer.attrib.get("article") or "")
            # 2) из <name>
            if not art:
                art = _normalize_code(_extract_article_from_name(get_text(offer, "name")))
            # 3) из <url>
            if not art:
                art = _normalize_code(_extract_article_from_url(get_text(offer, "url")))
            # 4) из offer@id
            if not art:
                art = _normalize_code(offer.attrib.get("id") or "")

            if art:
                vc.text = art
                filled_from_art += 1
            else:
                fixed_bare += 1  # останется только префикс

        # всегда добавляем префикс (политика проекта)
        vc.text = f"{prefix}{(vc.text or '')}"
        total_prefixed += 1

    return total_prefixed, created, filled_from_art, fixed_bare

def purge_offer_tags_and_attrs_after(offer: ET.Element) -> Tuple[int,int]:
    removed_tags = 0
    for t in PURGE_TAGS_AFTER:
        for node in list(offer.findall(t)):
            offer.remove(node); removed_tags += 1
    removed_attrs = 0
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib:
            offer.attrib.pop(a, None); removed_attrs += 1
    return removed_tags, removed_attrs

def count_category_ids(offer_el: ET.Element) -> int:
    return len(list(offer_el.findall("categoryId"))) + len(list(offer_el.findall("CategoryId")))

# ===================== FEED_META =====================
def render_feed_meta_comment(pairs: Dict[str, str]) -> str:
    order = [
        "supplier","source","offers_total","offers_written","prices_updated",
        "params_removed","vendors_recovered","dropped_top","available_forced",
        "categoryId_dropped","vendorcodes_filled_from_article","vendorcodes_created",
        "built_utc","built_Asia/Almaty",
    ]
    comments = {
        "supplier": "Метка поставщика",
        "source": "URL исходного XML",
        "offers_total": "Офферов у поставщика до очистки",
        "offers_written": "Офферов записано (после очистки)",
        "prices_updated": "Скольким товарам пересчитали price",
        "params_removed": "Сколько <param>/<Param> удалено",
        "vendors_recovered": "Скольким товарам восстановлен vendor",
        "dropped_top": "ТОП часто отброшенных названий",
        "available_forced": "Сколько офферов получили available=true",
        "categoryId_dropped": "Сколько тегов categoryId удалено",
        "vendorcodes_filled_from_article": "Скольким офферам проставили vendorCode из артикула",
        "vendorcodes_created": "Сколько vendorCode-узлов было создано",
        "built_utc": "Время сборки (UTC)",
        "built_Asia/Almaty": "Время сборки (Алматы)",
    }
    maxk = max(len(k) for k in order)
    maxv = max(len(str(pairs.get(k, "n/a"))) for k in order)
    lines = ["FEED_META"]
    for k in order:
        v = str(pairs.get(k, "n/a"))
        c = comments.get(k, "")
        lines.append(f"{k.ljust(maxk)} = {v.ljust(maxv)}  | {c}")
    return "\n".join(lines)

def top_dropped(d: Dict[str,int], n: int=10) -> str:
    items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return ",".join(f"{k}:{v}" for k,v in items) if items else "n/a"

# ===================== MAIN =====================
def main() -> None:
    log(f"Source: {SUPPLIER_URL}")
    data = fetch_xml(SUPPLIER_URL, TIMEOUT_S, RETRIES, RETRY_BACKOFF)
    src_root = ET.fromstring(data)

    shop_in = src_root.find("shop")
    if shop_in is None and src_root.tag.lower() == "shop":
        shop_in = src_root
    if shop_in is None:
        err("XML: <shop> not found")

    offers_in = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in is None:
        err("XML: <offers> not found")

    # Предсчёт: сколько categoryId удалим (для FEED_META)
    catid_to_drop_total = 0
    src_offers = list(iter_local(offers_in, "offer")) or list(_children_ci(offers_in, "offer"))
    for o in src_offers:
        catid_to_drop_total += count_category_ids(o)

    # Строим выходной документ
    out_root = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")
    out_offers = ET.SubElement(out_shop, "offers")

    # Копируем и базово чистим офферы (categoryId)
    mod_offers: List[ET.Element] = []
    for o in src_offers:
        mod = deepcopy(o)
        if DROP_CATEGORY_ID_TAG:
            for node in list(mod.findall("categoryId")) + list(mod.findall("CategoryId")):
                mod.remove(node)
        mod_offers.append(mod)

    # Стабильная сортировка
    def key_offer(o: ET.Element) -> Tuple[str,str,str]:
        return (get_text(o,"vendor"), get_text(o,"vendorCode"), get_text(o,"name"))
    mod_offers.sort(key=key_offer)

    for m in mod_offers:
        out_offers.append(m)

    # Восстановление/нормализация vendor
    norm_cnt, fill_param_cnt, fill_text_cnt, dropped_names = ensure_vendor(out_shop)

    # vendorCode из артикула + префикс
    total_prefixed, created_nodes, filled_from_art, fixed_bare = ensure_vendorcode_with_article(
        out_shop, prefix=VENDORCODE_PREFIX, create_if_missing=VENDORCODE_CREATE_IF_MISSING
    )

    # Пересчёт цен
    upd, skipped, total = reprice_offers(out_shop, PRICING_RULES)

    # Характеристики -> description
    specs_offers = specs_lines = 0
    if EMBED_SPECS_IN_DESCRIPTION:
        specs_offers, specs_lines = inject_specs_block(out_shop)
        if STRIP_ALL_PARAMS_AFTER_EMBED:
            strip_all_params(out_shop)

    # Наличие и очистка складских тегов
    available_forced = normalize_stock_always_true(out_shop)

    # Финальная под-очистка мусорных тегов и offer/@type (article пока остаётся в исходном XML; в публичном можно убрать, если потребуется)
    for off in out_offers.findall("offer"):
        purge_offer_tags_and_attrs_after(off)

    # Красивое форматирование
    try: ET.indent(out_root, space="  ")
    except Exception: pass

    # FEED_META
    meta_pairs = {
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "offers_total": len(src_offers),
        "offers_written": len(mod_offers),
        "prices_updated": upd,
        "params_removed": specs_lines,  # сколько строк характеристик перенесли в description
        "vendors_recovered": (fill_param_cnt + fill_text_cnt),
        "dropped_top": top_dropped(dropped_names),
        "available_forced": available_forced,
        "categoryId_dropped": catid_to_drop_total,
        "vendorcodes_filled_from_article": filled_from_art,
        "vendorcodes_created": created_nodes,
        "built_utc": now_utc_str(),
        "built_Asia/Almaty": now_almaty_str(),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    # Запись файлов
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    ET.ElementTree(out_root).write(OUT_FILE_YML, encoding=ENC, xml_declaration=True)
    ET.ElementTree(out_root).write(OUT_FILE_XML, encoding=ENC, xml_declaration=True)

    # .nojekyll для GitHub Pages
    docs_dir = os.path.dirname(OUT_FILE_YML) or "docs"
    try:
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    # Логи
    log(f"Vendor stats: normalized={norm_cnt}, filled_param={fill_param_cnt}, filled_text={fill_text_cnt}")
    log(f"VendorCode: prefixed_total={total_prefixed}, created_nodes={created_nodes}, filled_from_article={filled_from_art}, fixed_bare={fixed_bare}, prefix='{VENDORCODE_PREFIX}'")
    log(f"Pricing: updated={upd}, skipped_low_or_missing={skipped}, total_offers={total}")
    log(f"Specs block: offers={specs_offers}, lines_total={specs_lines}")
    log(f"Available forced: {available_forced}")
    log(f"Wrote: {OUT_FILE_YML} & {OUT_FILE_XML} | offers={len(mod_offers)} | encoding={ENC}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
