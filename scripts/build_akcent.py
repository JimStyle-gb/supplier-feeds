#!/usr/bin/env python3
# scripts/build_akcent.py
# -*- coding: utf-8 -*-
"""
Полный готовый скрипт сборки Akcent с исправленной логикой «Характеристики»
и встроенной фильтрацией <param> под Satu/SEO. Без внешних файлов.

Запуск локально:
  python scripts/build_akcent.py

Основное:
  • Тянет исходный XML (или читает файл), копирует <offer>.
  • Фильтрует <param> по whitelist + по «семействам» (Интерфейс, Диагональ и т.п.).
  • Формирует блок <description> с:
      — лидом (короткие плюсы), 
      — извлечёнными «Характеристиками» (поддержка «ключ: значение» и «ключ значение» без «:»),
      — «Родным описанием» (очищенным), 
      — FAQ, 
      — 3 отзывами.
  • Наличие → атрибут offer/@available, <currencyId>KZT</currencyId>.
  • Цена: выбирает минимальную «дилерскую», применяет правило +4%+диапазон и форсирует «…900».
  • Вендор: нормализация/угадывание; никогда не ставит имена поставщиков (akcent/copyline/vtt/alstyle).
  • Вставляет <categoryId>0</categoryId> первым узлом оффера.
  • Генерирует <keywords> (с GEO до 20 пунктов).
  • FEED_META в комментарии.
  • Пишет docs/akcent.yml в windows-1251 (fallback xmlcharrefreplace).
"""

from __future__ import annotations

import os, sys, re, io, time, json, random, hashlib, urllib.parse
from typing import Optional, List, Tuple, Dict, Match
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from html import unescape as _unescape
from xml.etree import ElementTree as ET

try:
    import requests  # стандартная зависимость
except Exception as e:
    print("ERROR: requests is required", file=sys.stderr)
    raise

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

SCRIPT_VERSION = "akcent-2025-10-27.v2.0"

# ===================== ENV / CONST =====================
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "Akcent").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/akcent.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()
TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN       = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AC").strip()

# фильтр по названиям можно выключить — оставляем заглушки (но не используем внешний файл)
AKCENT_KEYWORDS_MODE  = os.getenv("AKCENT_KEYWORDS_MODE", "off").lower()  # include|exclude|off
AKCENT_KEYWORDS_LIST  = [s.strip() for s in os.getenv("AKCENT_KEYWORDS", "").split(",") if s.strip()]

# Ценообразование (глобальное правило проекта)
PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = int(os.getenv("PRICE_CAP_VALUE", "100"))
PriceRule = Tuple[int,int,float,int]
PRICING_RULES: List[PriceRule] = [
    (101,10000,4.0,3000),(10001,25000,4.0,4000),(25001,50000,4.0,5000),
    (50001,75000,4.0,7000),(75001,100000,4.0,10000),(100001,150000,4.0,12000),
    (150001,200000,4.0,15000),(200001,300000,4.0,20000),(300001,400000,4.0,25000),
    (400001,500000,4.0,30000),(500001,750000,4.0,40000),(750001,1000000,4.0,50000),
    (1000001,1500000,4.0,70000),(1500001,2000000,4.0,90000),(2000001,100000000,4.0,100000),
]
INTERNAL_PRICE_TAGS=("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                     "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice","max_price","maxPrice","oldprice")
PRICE_FIELDS_DIRECT=["purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price"]
PRICE_KEYWORDS_DEALER = re.compile(r"(дилер|dealer|опт|wholesale|b2b|закуп|purchase|оптов)", re.I)
PRICE_KEYWORDS_RRP    = re.compile(r"(rrp|ррц|розниц|retail|msrp)", re.I)

# Плейсхолдеры картинок
PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.akcent.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.akcent.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.akcent.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# Чистки
DROP_CATEGORY_ID_TAG = True
DROP_STOCK_TAGS      = True
PURGE_TAGS_AFTER     = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","article")
DESIRED_ORDER=["vendorCode","name","price","picture","vendor","currencyId","description","keywords"]

# Генерация keywords
SATU_KEYWORDS_MAXLEN=1024
SATU_KEYWORDS_GEO=True
SATU_KEYWORDS_GEO_MAX=20
SATU_KEYWORDS_GEO_LAT=True

# ===================== PARAM FILTER (Satu/SEO) =====================
PARAM_FILTER_ENABLE = os.getenv("PARAM_FILTER_ENABLE", "1").lower() in {"1","true","yes","on"}

def _norm_param_name(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").strip().lower().replace("ё","е"))

DEFAULT_PARAM_WHITELIST = {
    # Принтеры/расходники/совместимость
    "совместимость", "совместимость с моделями", "принтеры", "подходит для", "модели",
    "тип печати", "цвет печати", "цвет", "ресурс", "ресурс барабана", "черный ресурс", "цветной ресурс",
    # Кабели/соединение
    "разъем", "разъемы", "разьем", "разьемы", "разъём", "разъёмы",
    "интерфейс", "интерфейсы", "длина кабеля", "материал",
    # Память/накопители
    "емкость", "объем", "объём", "форм-фактор", "формфактор", "тип памяти",
    "скорость чтения", "скорость записи",
    # Мониторы/ноутбуки/видео
    "диагональ", "разрешение", "тип матрицы", "частота обновления", "яркость", "контрастность",
    "время отклика", "угол обзора", "hdr", "версия hdmi",
    # Порты/беспроводное
    "usb", "hdmi", "displayport", "dp", "wi-fi", "wi fi", "bluetooth", "bt", "lan", "ethernet",
    # Энергия/электрика
    "мощность", "напряжение", "частота", "сила тока", "энергопотребление",
    # Общие важные
    "страна", "страна производитель", "гарантия", "комплектация",
    "вес", "размеры", "габариты",
    # Интерфейсы накопителей/карты
    "sata", "pcie", "m.2", "m2", "nvme", "micro sd", "sd", "sdhc", "sdxc",
    # Сетевое/питание
    "poe", "ip", "ip рейтинг", "ip rating",
    # Аудио/встроенное
    "микрофон", "динамики", "камера",
}
_PARAM_WL_NORM = {_norm_param_name(x) for x in DEFAULT_PARAM_WHITELIST}

_PARAM_ALLOWED_PATTERNS = [re.compile(p, re.I) for p in [
    r"^совместим", r"^принтер", r"^подходит", r"^модел",
    r"^цвет( печати)?$", r"^тип( печати| матрицы)?$", r"^ресурс",
    r"^раз(ъ|е)м", r"^интерфейс(ы)?$", r"^диагонал", r"^разрешени",
    r"^частот[аы] (обновлен|кадров)", r"^яркост", r"^контраст", r"^время отклик", r"^угол обзора$",
    r"^hdr$", r"^(usb|hdmi|displayport|dp|wi-?fi|bluetooth|bt|lan|ethernet)$",
    r"^длина кабел", r"^материал$", r"^(емкост|об(ъ|)ем|capacity)", r"^форм[- ]?фактор$",
    r"^скорост[ьи] (чт|зап)", r"^мощност", r"^напряжен", r"^частот", r"^сила тока", r"^энергопотреблен",
    r"^(вес|габарит(ы)?|размер(ы)?)$", r"^страна( производитель)?$", r"^гаранти", r"^комплектац",
    r"^(sata|pcie|m\.?2|nvme|micro ?sd|sd(hc|xc)?)$", r"^poe$", r"^ip( рейтинг| rating)?$", r"^(микрофон|динамик(и)?|камера)$",
]]

def _attr_ci(el: ET.Element, key: str) -> Optional[str]:
    k = key.lower()
    for a,v in el.attrib.items():
        if a.lower()==k:
            return v
    return None

def _param_allowed(name_raw: Optional[str]) -> bool:
    if not name_raw:
        return False
    n = _norm_param_name(name_raw)
    if n in _PARAM_WL_NORM:
        return True
    return any(p.search(n) for p in _PARAM_ALLOWED_PATTERNS)

def filter_params_for_satu(out_shop: ET.Element) -> Tuple[int,int,int]:
    """Удаляет из каждого <offer> ненужные <param> по whitelist/шаблонам."""
    if not PARAM_FILTER_ENABLE:
        return (0,0,0)
    off_el = out_shop.find("offers")
    if off_el is None:
        return (0,0,0)
    touched = kept = dropped = 0
    for offer in off_el.findall("offer"):
        changed=False
        for node in list(offer):
            if node.tag.lower()!="param":
                continue
            name_val = _attr_ci(node, "name")
            if _param_allowed(name_val):
                kept += 1
                continue
            offer.remove(node)
            dropped += 1
            changed=True
        if changed:
            touched += 1
    return (touched, kept, dropped)

# ===================== UTILS: TIME/PRINT =====================
log  = lambda m: print(m, flush=True)
warn = lambda m: print(f"WARN: {m}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_utc() -> datetime: return datetime.now(timezone.utc)
def now_almaty() -> datetime:
    try:   return datetime.now(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(time.time()+5*3600)
    except Exception: return datetime.utcfromtimestamp(time.time()+5*3600)
def format_dt_almaty(dt: datetime) -> str: return dt.strftime("%d:%m:%Y - %H:%M:%S")
def next_build_time_almaty() -> datetime:
    cur = now_almaty(); t = cur.replace(hour=1, minute=0, second=0, microsecond=0)
    return t + timedelta(days=1) if cur >= t else t

# ===================== LOAD SOURCE =====================
def load_source_bytes(src: str) -> bytes:
    """HTTP загрузка (с ретраями) или локальный файл."""
    if not src:
        raise RuntimeError("SUPPLIER_URL не задан")
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path, "rb") as f:
            data=f.read()
        if len(data) < MIN_BYTES:
            raise RuntimeError(f"file too small: {len(data)}")
        return data
    sess=requests.Session(); headers={"User-Agent":"supplier-feed-bot/1.0 (+github-actions)"}
    last=None
    for i in range(1, RETRIES+1):
        try:
            r=sess.get(src, headers=headers, timeout=TIMEOUT_S)
            if r.status_code!=200: raise RuntimeError(f"HTTP {r.status_code}")
            data=r.content
            if len(data)<MIN_BYTES: raise RuntimeError(f"too small ({len(data)})")
            return data
        except Exception as e:
            last=e; back=RETRY_BACKOFF*i*(1+random.uniform(-0.2,0.2))
            warn(f"fetch {i}/{RETRIES} failed: {e}; sleep {back:.2f}s")
            if i<RETRIES: time.sleep(back)
    raise RuntimeError(f"fetch failed: {last}")

# ===================== NAME FILTER (OPTIONAL) =====================
def _norm_name_simple(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").replace("\u00A0"," ").lower().replace("ё","е")).strip()

def name_matches_keys(name: str, keys: List[str]) -> bool:
    """Простой include/exclude по префиксам/подстрокам из AKCENT_KEYWORDS (ENV)."""
    if not keys: return False
    n=_norm_name_simple(name)
    for k in keys:
        kk=_norm_name_simple(k)
        if not kk: continue
        if n.startswith(kk) or kk in n:
            return True
    return False

# ===================== BRAND / PRICE / AVAIL / ORDER =====================
def _norm_key(s: str) -> str:
    if not s: return ""
    s=s.strip().lower().replace("ё","е")
    s=re.sub(r"[-_/]+"," ",s); s=re.sub(r"\s+"," ",s); return s

SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a","китай","china")
COMMON_BRANDS=["Canon","HP","Hewlett-Packard","Xerox","Brother","Epson","BenQ","ViewSonic","Optoma","Acer","Panasonic","Sony",
               "Konica Minolta","Ricoh","Kyocera","Sharp","OKI","Pantum","Lenovo","Dell","ASUS","Samsung","Apple","MSI"]
BRAND_ALIASES={"hewlett packard":"HP","konica":"Konica Minolta","konica-minolta":"Konica Minolta",
               "viewsonic proj":"ViewSonic","epson proj":"Epson","epson projector":"Epson","benq proj":"BenQ",
               "hp inc":"HP","nvprint":"NV Print","nv print":"NV Print","gg":"G&G","g&g":"G&G"}

def normalize_brand(raw: str) -> str:
    k=_norm_key(raw)
    return "" if (not k) or (k in SUPPLIER_BLOCKLIST) else (BRAND_ALIASES.get(k) or raw.strip())

def build_brand_index(shop_el: ET.Element) -> Dict[str,str]:
    idx={}
    off_el=shop_el.find("offers")
    if off_el is None: return idx
    for offer in off_el.findall("offer"):
        v=offer.find("vendor")
        if v is not None and (v.text or "").strip():
            canon=v.text.strip(); idx[_norm_key(canon)] = canon
    return idx

def _find_brand_in_text(text: str) -> str:
    t=(text or "").lower()
    for a,canon in BRAND_ALIASES.items():
        if re.search(rf"\b{re.escape(a)}\b", t): return canon
    for b in COMMON_BRANDS:
        if re.search(rf"\b{re.escape(b.lower())}\b", t): return b
    m=re.match(r"^([A-Za-zА-Яа-яЁё]+)\b", (text or "").strip())
    if m:
        cand=m.group(1)
        for b in COMMON_BRANDS:
            if b.lower()==cand.lower(): return b
    return ""

def guess_vendor_for_offer(offer: ET.Element, brand_index: Dict[str,str]) -> str:
    name=(offer.findtext("name") or "").strip()
    desc=inner_html(offer.find("description"))
    first=(re.split(r"\s+", name.strip())[0] if name else "")
    f_norm=_norm_key(first)
    if f_norm in brand_index: return brand_index[f_norm]
    b = _find_brand_in_text(name) or _find_brand_in_text(desc)
    return b

def ensure_vendor(shop_el: ET.Element) -> Tuple[int,int,int]:
    off_el=shop_el.find("offers")
    if off_el is None: return (0,0,0)
    idx=build_brand_index(shop_el); normalized=0; filled=0; removed=0
    for offer in off_el.findall("offer"):
        ven=offer.find("vendor")
        txt=(ven.text or "").strip() if ven is not None and ven.text else ""
        if txt:
            canon=normalize_brand(txt)
            alias=BRAND_ALIASES.get(_norm_key(txt))
            final=alias or canon
            if any(m in txt.lower() for m in UNKNOWN_VENDOR_MARKERS) or (not final):
                if ven is not None: offer.remove(ven); removed+=1
            elif final!=txt:
                ven.text=final; normalized+=1
        else:
            guess=guess_vendor_for_offer(offer, idx)
            if guess:
                if ven is None: ven=ET.SubElement(offer,"vendor")
                ven.text=guess; filled+=1
    return (normalized, filled, removed)

def parse_price_number(raw:str)->Optional[float]:
    if raw is None: return None
    s=raw.strip().replace("\xa0"," ").replace(" ","").replace("KZT","").replace("kzt","").replace("₸","").replace(",",".")
    if not s: return None
    try: v=float(s); return v if v>0 else None
    except Exception: return None

def pick_dealer_price(offer: ET.Element) -> Optional[float]:
    dealer=[]
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            val=parse_price_number(p.text or "")
            if val is None: continue
            t=(p.attrib.get("type") or "")
            if PRICE_KEYWORDS_DEALER.search(t) or not PRICE_KEYWORDS_RRP.search(t):
                dealer.append(val)
    for tag in PRICE_FIELDS_DIRECT:
        el=offer.find(tag)
        if el is not None and el.text:
            v=parse_price_number(el.text)
            if v is not None: dealer.append(v)
    return min(dealer) if dealer else None

def _force_tail_900(n: float) -> int:
    i=int(n)
    base=(i//1000)*1000
    return base+900 if base+900<=i+999 else base+1900  # всегда «…900», но не меньше базовой тысячи

def compute_retail(d:float,rules:List[PriceRule])->Optional[int]:
    for lo,hi,pct,add in rules:
        if lo<=d<=hi:
            return _force_tail_900(d*(1+pct/100.0)+add)
    return None

def _remove_all_price_nodes(offer: ET.Element):
    for t in ("price","Price"):
        for node in list(offer.findall(t)): offer.remove(node)

def strip_supplier_price_blocks(offer: ET.Element):
    remove_all(offer,"prices","Prices")
    for tag in INTERNAL_PRICE_TAGS: remove_all(offer, tag)

def reprice_offers(out_shop:ET.Element,rules:List[PriceRule])->None:
    off_el=out_shop.find("offers")
    if off_el is None: return
    for offer in off_el.findall("offer"):
        if offer.attrib.get("_force_price","")=="100":
            strip_supplier_price_blocks(offer); _remove_all_price_nodes(offer)
            ET.SubElement(offer,"price").text=str(PRICE_CAP_VALUE)
            offer.attrib.pop("_force_price",None); continue
        dealer=pick_dealer_price(offer)
        if dealer is None or dealer<=100:
            strip_supplier_price_blocks(offer); continue
        newp=compute_retail(dealer,rules)
        if newp is None:
            strip_supplier_price_blocks(offer); continue
        _remove_all_price_nodes(offer); ET.SubElement(offer,"price").text=str(int(newp)); strip_supplier_price_blocks(offer)

def flag_unrealistic_supplier_prices(out_shop: ET.Element) -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    flagged=0
    for offer in off_el.findall("offer"):
        src_p_txt = offer.findtext("price")
        try:
            src_p = float((src_p_txt or "").replace(",",".")) if src_p_txt else None
        except Exception:
            src_p = None
        if src_p is not None and src_p >= PRICE_CAP_THRESHOLD:
            offer.attrib["_force_price"]=str(PRICE_CAP_VALUE); flagged+=1
    return flagged

TRUE_WORDS={"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS={"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}
def _parse_bool_str(s: str)->Optional[bool]:
    v=(s or "").strip().lower()
    return True if v in TRUE_WORDS else False if v in FALSE_WORDS else None

def derive_available(offer: ET.Element) -> bool:
    avail_el=offer.find("available")
    if avail_el is not None and avail_el.text:
        b=_parse_bool_str(avail_el.text)
        if b is not None: return b
    for tag in ["quantity_in_stock","quantity","stock","Stock"]:
        for node in offer.findall(tag):
            try:
                val=int(re.sub(r"[^\d\-]+","", node.text or ""))
                return val>0
            except Exception:
                continue
    for tag in ["status","Status"]:
        node=offer.find(tag)
        if node is not None and node.text:
            b=_parse_bool_str(node.text)
            if b is not None: return b
    return False

def normalize_available_field(out_shop: ET.Element) -> Tuple[int,int]:
    off_el=out_shop.find("offers")
    if off_el is None: return (0,0)
    t=f=0
    for offer in off_el.findall("offer"):
        b=derive_available(offer)
        remove_all(offer,"available")
        offer.attrib["available"]="true" if b else "false"
        if DROP_STOCK_TAGS: remove_all(offer,"quantity_in_stock","quantity","stock","Stock")
        t+=1 if b else 0; f+=0 if b else 1
    return t,f

def fix_currency_id(out_shop: ET.Element, default_code: str = "KZT") -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    touched=0
    for offer in off_el.findall("offer"):
        remove_all(offer,"currencyId")
        ET.SubElement(offer,"currencyId").text=default_code
        touched+=1
    return touched

def ensure_categoryid_zero_first(out_shop: ET.Element) -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    touched=0
    for offer in off_el.findall("offer"):
        remove_all(offer,"categoryId","CategoryId")
        cid=ET.Element("categoryId"); cid.text=os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0,cid); touched+=1
    return touched

def reorder_offer_children(out_shop: ET.Element) -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    changed=0
    for offer in off_el.findall("offer"):
        children=list(offer)
        buckets={k:[] for k in DESIRED_ORDER}; others=[]
        for n in children: (buckets[n.tag] if n.tag in buckets else others).append(n)
        rebuilt=[*sum((buckets[k] for k in DESIRED_ORDER), []), *others]
        if rebuilt!=children:
            for n in children: offer.remove(n)
            for n in rebuilt:  offer.append(n)
            changed+=1
    return changed

# ===================== Kind detector =====================
def detect_kind(name: str) -> str:
    n=(name or "").lower()
    if "картридж" in n or "тонер" in n or "тонер-" in n: return "cartridge"
    if "ибп" in n or "ups" in n or "источник бесперебойного питания" in n: return "ups"
    if "проектор" in n or "projector" in n: return "projector"
    if "принтер" in n or "mfp" in n or "мфу" in n: return "mfp"
    return "other"

# ===================== Helpers: HTML/inner =====================
def inner_html(el: ET.Element) -> str:
    if el is None: return ""
    parts=[]
    if el.text: parts.append(el.text)
    for ch in el:
        parts.append(ET.tostring(ch, encoding="unicode"))
        if ch.tail: parts.append(ch.tail)
    return "".join(parts).strip()

def _html_escape_in_cdata_safe(s: str) -> str:
    return (s or "").replace("]]>", "]]&gt;")

def remove_all(el: ET.Element, *tags: str) -> int:
    n=0
    for t in tags:
        for x in list(el.findall(t)): el.remove(x); n+=1
    return n

# ===================== ХАРАКТЕРИСТИКИ: извлечение =====================
KV_KEYS_MAP = {
    "вид":"Вид",
    "назначение":"Назначение",
    "цвет печати":"Цвет печати",
    "поддерживаемые модели принтеров":"Совместимость",
    "совместимость":"Совместимость",
    "ресурс":"Ресурс",
    "технология печати":"Технология печати",
    "тип":"Тип",
}
MORE_PHRASES_RE = re.compile(r"^\s*(подробнее|читать далее|узнать больше|все детали|подробности|смотреть на сайте производителя|скачать инструкцию)\s*\.?\s*$", re.I)
URL_RE = re.compile(r"https?://\S+", re.I)

def autocorrect_minor_typos_in_html(html: str) -> str:
    s = html or ""
    s = re.sub(r"\bвысококачетсвенную\b", "высококачественную", s, flags=re.I)
    s = re.sub(r"\bприентеров\b", "принтеров", s, flags=re.I)
    s = re.sub(r"\bSC-\s*P(\d{3,4}\b)", r"SC-P\1", s)
    s = re.sub(r"SureColor\s+SC-\s*P", "SureColor SC-P", s)
    s = re.sub(r"(\d)\s*мл\b", r"\1 мл", s, flags=re.I)
    s = re.sub(r"[ ]{2,}", " ", s)
    return s

def _html_to_text(desc_html: str) -> str:
    t = re.sub(r"<br\s*/?>", "\n", desc_html or "", flags=re.I)
    t = re.sub(r"</p\s*>", "\n", t, flags=re.I)
    t = re.sub(r"<a\b[^>]*>.*?</a>", "", t, flags=re.I|re.S)
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace("\u00A0"," ")
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def _normalize_models_list(val: str) -> str:
    x = val or ""
    x = re.sub(r"\bSC-\s*P(\d{3,4}\b)", r"SC-P\1", x)
    x = re.sub(r"\s{2,}", " ", x)
    parts = re.split(r"[,\n;]+", x)
    parts = [p.strip(" .") for p in parts if p.strip()]
    seen=set(); out=[]
    for p in parts:
        if p not in seen:
            seen.add(p); out.append(p)
    return "; ".join(out)

# Новая логика — находим «ключ: значение» И «ключ значение» (без двоеточия), регистронезависимо.
TECH_KEYWORDS = [
    "тип","модель","серия","совмест
