#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, re, time, random, urllib.parse, requests, html
from copy import deepcopy
from typing import Dict, List, Tuple, Optional
from xml.etree import ElementTree as ET
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

SCRIPT_VERSION = "akcent-2025-10-23.v2.0.0"

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

AKCENT_KEYWORDS_PATH  = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt")
AKCENT_KEYWORDS_MODE  = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()  # include|exclude

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

PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.akcent.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.akcent.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.akcent.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

DROP_CATEGORY_ID_TAG = True
DROP_STOCK_TAGS      = True
PURGE_TAGS_AFTER     = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER = ("type","article")
DESIRED_ORDER=["vendorCode","name","price","picture","vendor","currencyId","description"]

SATU_KEYWORDS_MAXLEN=1024
SATU_KEYWORDS_GEO=True
SATU_KEYWORDS_GEO_MAX=20
SATU_KEYWORDS_GEO_LAT=True

PARAMS_MAX_VALUE_LEN = int(os.getenv("PARAMS_MAX_VALUE_LEN", "800"))

# ===================== UTILS =====================
def log(m: str): print(m, flush=True)
def warn(m: str): print(f"WARN: {m}", file=sys.stderr, flush=True)
def err(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_almaty() -> datetime:
    try:
        return datetime.now(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(time.time()+5*3600)
    except Exception:
        return datetime.utcfromtimestamp(time.time()+5*3600)

def format_dt_almaty(dt: datetime) -> str:
    return dt.strftime("%d:%m:%Y - %H:%M:%S")

def inner_html(el: ET.Element) -> str:
    if el is None: return ""
    parts=[]
    if el.text: parts.append(el.text)
    for ch in el:
        parts.append(ET.tostring(ch, encoding="unicode"))
        if ch.tail: parts.append(ch.tail)
    return "".join(parts).strip()

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag); return (node.text or "").strip() if node is not None and node.text else ""

def remove_all(el: ET.Element, *tags: str) -> int:
    n=0
    for t in tags:
        for x in list(el.findall(t)): el.remove(x); n+=1
    return n

def _html_escape_in_cdata_safe(s: str) -> str:
    return (s or "").replace("]]>", "]]&gt;")

# ===================== LOAD SOURCE =====================
def load_source_bytes(src: str) -> bytes:
    if not src: raise RuntimeError("SUPPLIER_URL не задан")
    if "://" not in src or src.startswith("file://"):
        path = src[7:] if src.startswith("file://") else src
        with open(path, "rb") as f: data=f.read()
        if len(data) < MIN_BYTES: raise RuntimeError(f"file too small: {len(data)}")
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

# ===================== NAME FILTER =====================
class KeySpec:
    __slots__=("raw","kind","norm","pattern")
    def __init__(self, raw: str, kind: str, norm: Optional[str], pattern: Optional[re.Pattern]):
        self.raw, self.kind, self.norm, self.pattern = raw, kind, norm, pattern

def _norm_name(s: str) -> str:
    s=(s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

def load_name_filter(path: str) -> List[KeySpec]:
    if not path or not os.path.exists(path): return []
    data=None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f: txt=f.read()
            data = txt.replace("\ufeff","").replace("\x00","")
            break
        except Exception: continue
    if data is None:
        with open(path,"r",encoding="utf-8",errors="ignore") as f: data=f.read().replace("\x00","")
    keys=[]
    for ln in data.splitlines():
        s=ln.strip()
        if not s or s.startswith("#"): continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try: keys.append(KeySpec(s,"regex",None,re.compile(s[1:-1],re.I)))
            except Exception: pass
        else:
            n=_norm_name(s)
            if n: keys.append(KeySpec(s,"prefix",n,None))
    return keys

def name_matches(name: str, keys: List[KeySpec]) -> bool:
    if not keys: return False
    norm = _norm_name(name)
    for ks in keys:
        if ks.kind=="prefix" and norm.startswith(ks.norm): return True
        if ks.kind=="regex"  and ks.pattern and ks.pattern.search(name or ""): return True
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
BRAND_ALIASES={"hewlett packard":"HP","konica":"Konica Minolta","конiка":"Konica Minolta","konica-minolta":"Konica Minolta",
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
    name=get_text(offer,"name"); desc=inner_html(offer.find("description"))
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

_force_tail_900 = lambda n: max(int(n)//1000,0)*1000+900 if int(n)>=0 else 900
def compute_retail(d:float,rules:List[PriceRule])->Optional[int]:
    for lo,hi,pct,add in rules:
        if lo<=d<=hi: return _force_tail_900(d*(1+pct/100.0)+add)
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
            offer.attrib.pop("_force_price",None)
            continue
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
        try:
            src_p = float((get_text(offer,"price") or "").replace(",",".")) if get_text(offer,"price") else None
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
        remove_all(offer,"currencyId"); ET.SubElement(offer,"currencyId").text=default_code; touched+=1
    return touched

def ensure_categoryid_zero_first(out_shop: ET.Element) -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    touched=0
    for offer in out_shop.find("offers").findall("offer"):
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

# ===================== TEXT / HTML GROOMING =====================
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
MORE_PHRASES_RE = re.compile(r"^\s*(подробнее|читать далее|узнать больше|все детали|подробности|смотреть на сайте производителя|скачать инструкцию)\s*\.?\s*$", re.I)

def maybe_unescape_html(s: str) -> str:
    if not s: return s
    if re.search(r"&lt;/?[a-zA-Z]", s):
        for _ in range(2):
            s = html.unescape(s)
            if not re.search(r"&lt;/?[a-zA-Z]", s): break
    return s

KV_KEYS_MAP = {
    "вид":"Вид","тип":"Тип","назначение":"Назначение","цвет печати":"Цвет печати",
    "поддерживаемые модели принтеров":"Совместимость","совместимость":"Совместимость",
    "совместимые продукты":"Совместимость","совместимые модели":"Совместимость",
    "подходит для моделей":"Совместимость","совместимые принтеры":"Совместимость","для моделей":"Совместимость",
    "ресурс":"Ресурс","технология печати":"Технология печати",
    "устройство":"Устройство","сканирование с планшета":"Тип сканирования","тип датчика":"Тип датчика",
    "тип лампы":"Подсветка","epson readyscan led":"Подсветка","dual lens system":"Оптика",
    "digital ice, пленка":"Обработка пленки","digital ice, непрозрачные оригиналы":"Обработка оригиналов",
    "разрешение сканера, dpi":"Оптическое разрешение","интерполяционное разрешение, dpi":"Интерполяция",
    "глубина цвета, бит":"Глубина цвета","максимальный формат сканирования":"Макс. формат",
    "скорость сканирования":"Скорость сканирования","интерфейс usb":"Подключение",
    "интерфейс ieee-1394 (firewire)":"FireWire","подключение по wi-fi":"Wi-Fi",
    "скорость печати":"Скорость печати","двусторонняя печать":"Двусторонняя печать",
    "интерфейсы":"Интерфейсы","формат":"Формат","разрешение печати":"Разрешение печати","разрешение":"Разрешение печати",
    "тип печати":"Тип печати","подача бумаги":"Подача бумаги","выход лоток":"Выходной лоток","емкость лотка":"Емкость лотка",
    "диагональ":"Дисплей",
    "яркость":"Яркость","контрастность":"Контрастность",
    "источник света":"Источник света","ресурс лампы":"Ресурс источника","входы":"Входы",
    "интерфейсы (видео)":"Входы","коррекция трапеции":"Коррекция трапеции","поддержка 3d":"Поддержка 3D",
    "мощность":"Мощность","мощность, ва":"Мощность","время автономии":"Время автономии","avr":"Стабилизация AVR","стабилизация":"Стабилизация",
    "выходные розетки":"Розетки","тип розеток":"Розетки",
    "состав поставки":"Комплектация","комплектация":"Комплектация","в комплекте":"Комплектация","комплектация поставки":"Комплектация",
    "страна происхождения":"Страна происхождения","гарантия":"Гарантия",
    "цвета":"Цвета чернил","colours":"Цвета чернил","colors":"Цвета чернил",
    "количество бумажных ящиков":"Подача бумаги",
    "бумаги":"Формат",
}

ALLOWED_PARAM_CANON = {
    "Совместимость","Тип","Назначение","Цвет печати","Ресурс","Технология печати",
    "Страна происхождения","Гарантия",
    "Тип сканирования","Тип датчика","Подсветка","Оптическое разрешение","Интерполяция",
    "Глубина цвета","Макс. формат","Скорость сканирования","Подключение","Wi-Fi","FireWire",
    "Тип печати","Разрешение печати","Скорость печати","Двусторонняя печать","Интерфейсы","Формат",
    "Подача бумаги","Выходной лоток","Емкость лотка",
    "Яркость","Контрастность","Источник света","Ресурс источника","Входы","Коррекция трапеции","Поддержка 3D",
    "Мощность","Стабилизация AVR","Стабилизация","Розетки",
    "Дисплей","Цвета чернил",
}
DISALLOWED_PARAM_NAMES = {
    "производитель","для бренда","наименование производителя","сопутствующие товары",
    "бренд","brand","manufacturer","vendor","поставщик","партномер","артикул поставщика","код на складе",
}
CANON_NAME_MAP = {
    "совместимые продукты":"Совместимость","совместимые модели":"Совместимость","совместимые принтеры":"Совместимость",
    "поддерживаемые модели принтеров":"Совместимость","для моделей":"Совместимость",
    "цвет":"Цвет печати","вид":"Тип","тип":"Тип",
    "разрешение сканера, dpi":"Оптическое разрешение","интерполяционное разрешение, dpi":"Интерполяция",
    "максимальный формат сканирования":"Макс. формат","интерфейс usb":"Подключение","подключение по wi-fi":"Wi-Fi",
    "ресурс лампы":"Ресурс источника","мощность, ва":"Мощность","тип розеток":"Розетки",
    "выход лоток":"Выходной лоток","емкость лотка":"Емкость лотка",
    "диагональ":"Дисплей","colours":"Цвета чернил","colors":"Цвета чернил",
    "количество бумажных ящиков":"Подача бумаги",
    "бумаги":"Формат",
}

STOP_SECTION_TOKENS = [
    "карты памяти","свойства","безопасность wlan","услуги мобильной и облачной печати","энергоснабжение",
    "дуплекс","поля печати","емкость отделения подачи бумаги","задний тракт","толщина","обработка мультимедиа",
    "время до первой страницы","print margin","paper tray capacity","rear paper path","thickness",
    "media handling","duplex printing speed","colours capacity","colours","colors","print from","mobile device",
    "wlan security","метод печати","конфигурация сопла","сопла","улучшение качества фотографий",
    "комплект поставки","сканирование","scan","для получения подробной информации","посетите",
    "www.","http://","https://","на сайте производителя","apple airprint","mopria",
]
MAX_SPEC_LEN  = 180

EN_RU_FIXES   = [
    (re.compile(r"\bPages/min\b", re.I), "стр/мин"),
    (re.compile(r"\bYes\b", re.I), "Да"),
    (re.compile(r"\bNo\b", re.I), "Нет"),
    (re.compile(r"\bColour\b", re.I), "Цвет"),
    (re.compile(r"\bColors?\b", re.I), "Цвет"),
    (re.compile(r"\bper\s+10\s*[x×]\s*15\s*cm\s*photo\b", re.I), "на фото 10×15 см"),
]

def _norm_resolution_text(s: str) -> str:
    t = s.replace("\u00A0", " ")
    t = re.sub(r"(?<=\d)[\.,](?=\d{3}\b)", "", t)
    t = t.replace("x", "×").replace("X", "×")
    m = re.search(r"(\d{2,5})\s*×\s*(\d{2,5})", t) or re.search(r"(\d{2,5})\s*[xX]\s*(\d{2,5})", t)
    if not m: return ""
    a, b = m.group(1), m.group(2)
    if b.endswith("06") and len(b) >= 4:
        if b[:-1] in {"300","600","720","1200","1440","2400","4800","5760"}:
            b = b[:-1]
    return f"{a}×{b} dpi"

def _norm_speed_text(s: str) -> str:
    m=re.search(r"(\d{1,3})\s*(стр|pages)\s*/?\s*мин", s, re.I)
    return f"до {m.group(1)} стр/мин" if m else ""

def _norm_interfaces(s: str) -> str:
    t=s
    repl = {"wifi":"Wi-Fi","wi-fi":"Wi-Fi","wi fi":"Wi-Fi","usb-хост":"USB-host","usb host":"USB-host",
            "ethernet":"Ethernet","bluetooth":"Bluetooth","rs-232":"RS-232","rj-45":"RJ-45",}
    for k,v in repl.items(): t=re.sub(rf"\b{k}\b", v, t, flags=re.I)
    t=re.sub(r"\s*,\s*", ", ", t); t=re.sub(r"\s{2,}"," ", t)
    return t.strip(" ,;")

_ALLOWED_FORMAT_TOKEN = re.compile(
    r"^(A\d|B\d|C6|DL|Letter|Legal|No\.?\s*10|\d{1,2}\s*[×x]\s*\d{1,2}|16:9|10\s*×\s*15|13\s*×\s*18|9\s*×\s*13)$",
    re.I
)
def _norm_format_list(s: str) -> str:
    t = s.replace("бумаги", "")
    t = re.sub(r"\b(см|cm)\b", "", t)
    t = re.sub(r"\s*[xX]\s*", "×", t)
    t = re.sub(r"\b\d+\s*-\s*\d+\s*г/м2\b", "", t, flags=re.I)
    t = re.sub(r"\b\d+\s*ватт\b", "", t, flags=re.I)
    t = re.sub(r"\b(печать без полей|печать на cd/dvd|automatic duplex|обработка мультимедиа)\b.*", "", t, flags=re.I)
    parts = [p.strip() for p in re.split(r"[;,]+", t) if p.strip()]
    keep=[]
    for p in parts:
        p = re.sub(r"\s*\(Конверт\)\s*", "", p, flags=re.I)
        p = re.sub(r"\s{2,}"," ", p)
        if _ALLOWED_FORMAT_TOKEN.match(p):
            p = re.sub(r"\s*×\s*", "×", p); keep.append(p)
    if not keep: return ""
    if len(keep) > 10: keep = keep[:10] + ["и др."]
    return ", ".join(keep)

_COLOR_WORDS = {"black":"Black","photo black":"Photo Black","cyan":"Cyan","magenta":"Magenta","yellow":"Yellow",
                "grey":"Grey","gray":"Grey","light cyan":"Light Cyan","light magenta":"Light Magenta"}
def _norm_colors(s: str) -> str:
    t = re.sub(r"\[[^\]]*\]","", s)
    t = re.sub(r"\bcapacity\b","", t, flags=re.I)
    cut = STOP_SECTION_TOKENS + ["print from","для получения подробной информации","www.","http://","https://"]
    pat = re.compile("|".join(re.escape(x) for x in cut), re.I); m = pat.search(t)
    if m: t = t[:m.start()]
    parts = re.split(r"[,/;]+|\s+\+\s+", t)
    out=[]
    for p in parts:
        w = p.strip().lower()
        if not w: continue
        if w in _COLOR_WORDS: out.append(_COLOR_WORDS[w])
        elif re.fullmatch(r"(black|grey|gray|cyan|magenta|yellow|photo black)", w):
            out.append(_COLOR_WORDS.get(w, w.title()))
    seen=set(); uniq=[]
    for x in out:
        if x not in seen: uniq.append(x); seen.add(x)
    return ", ".join(uniq)

def _norm_yesno(v: str) -> str:
    v=v.strip()
    if not v: return ""
    if re.search(r"^(да|yes|y)$", v, re.I): return "Да"
    if re.search(r"^(нет|no|n)$", v, re.I): return "Нет"
    if re.search(r"wi[\- ]?fi", v, re.I): return "Да"
    return v

def _norm_display(v: str) -> str:
    t=v.replace(",", "."); m=re.search(r"(\d{1,2}(\.\d)?)", t)
    return f"{m.group(1)} см" if m else v

def _norm_guarantee(v: str) -> str:
    t=v
    m=re.search(r"(\d{1,2})\s*(месяц|мес)", t, re.I)
    if m: return f"{m.group(1)} мес."
    m=re.search(r"(\d(\.\d)?)\s*г(од|ода|лет)?", t, re.I)
    if m:
        years=float(m.group(1))
        if abs(years-round(years))<1e-6: return f"{int(round(years))*12} мес."
        return f"{years} лет"
    return ""

def clean_spec_value(key: str, value: str) -> str:
    if not value: return ""
    s = value.replace("\u00A0"," ").strip()
    s = s.splitlines()[0].strip()
    for tok in STOP_SECTION_TOKENS:
        i = s.lower().find(tok.lower())
        if i > 0:
            s = s[:i].rstrip(" ,;:—–-"); break
    s = URL_RE.sub("", s)
    s = re.sub(r"\s{2,}", " ", s).strip(" ,;:—–-")
    if len(s) > MAX_SPEC_LEN:
        cut = re.split(r"(,|;|\)|\.)", s)
        if cut and len("".join(cut[:6])) > 40:
            s = "".join(cut[:6]).strip(" ,;:—–-") + "…"
        else:
            s = s[:MAX_SPEC_LEN].rstrip(" ,;:—–-") + "…"
    return s if re.search(r"[A-Za-zА-Яа-яЁё0-9]", s) else ""

def normalize_by_key(k: str, v: str) -> str:
    if not v: return ""
    key=k.lower(); s=v.strip()
    if key=="разрешение печати":
        got=_norm_resolution_text(s); return got or clean_spec_value(k, s)
    if key in ("интерфейсы","подключение","входы"):
        return _norm_interfaces(s)
    if key=="wi-fi":
        if re.search(r"wi[\- ]?fi", s, re.I): return "Да"
        return ""
    if key=="цвета чернил":
        return _norm_colors(s)
    if key=="формат":
        return _norm_format_list(s)
    if key=="двусторонняя печать":
        return _norm_yesno(s)
    if key=="дисплей":
        return _norm_display(s)
    if key=="гарантия":
        return _norm_guarantee(s)
    if key=="скорость печати":
        got=_norm_speed_text(s); return got or clean_spec_value(k, s)
    if key=="тип датчика":
        if "cis" in s.lower(): return "CIS (контактный)"
    if key=="подача бумаги":
        m=re.search(r"\b(\d{1,2})\b", s); return f"{m.group(1)} лотка" if m else clean_spec_value(k, s)
    if key=="цвет печати":
        low = s.lower()
        if "ч/б" in low or "монохром" in low or "черн" in low: return "ч/б"
        if "цветн" in low or "colour" in low or "color" in low: return "цветная"
        return ""
    if key=="разрешение":  # для проекторов/панелей
        return clean_spec_value(k, s.replace("XGA","XGA").replace("4k","4K"))
    return clean_spec_value(k, s)

def _dedupe_specs(specs: List[Tuple[str,str]]) -> List[Tuple[str,str]]:
    best: Dict[str,str] = {}
    for k,v in specs:
        if not v: continue
        v=v.strip(" .;—-")
        if not v or v.lower() in {"нет","-","—"}: continue
        old=best.get(k)
        if not old or len(v)>len(old): best[k]=v
    return [(k,best[k]) for k in best]

def postclean_specs(specs: List[Tuple[str,str]], kind: str) -> List[Tuple[str,str]]:
    cleaned=[]
    for k,v in specs:
        s=normalize_by_key(k, v)
        if s: cleaned.append((k,s))
    best={}
    for k,v in cleaned:
        if k not in ALLOWED_PARAM_CANON: continue
        if kind!="consumable" and k=="Совместимость":
            continue
        if k=="Совместимость":
            # отбрасываем «совместимость», если там только артикулы расходников
            if not re.search(r"[A-Za-z]{2,}-?\d{2,}", v):  # нет модельных имен устройств
                continue
        if k=="Цвет печати" and v=="ч/б":
            # если есть цвета чернил >1 — принудительно «цветная»
            colors=[vv for kk,vv in cleaned if kk=="Цвета чернил"]
            if colors and re.search(r",", colors[0]): v="цветная"
        if k=="Формат" and not re.search(r"[A-Za-z0-9]", v):
            continue
        old=best.get(k)
        if not old or len(v)>len(old): best[k]=v

    # порядок
    important_order = [
        "Тип","Назначение","Тип печати","Цвет печати","Формат","Разрешение печати","Скорость печати","Двусторонняя печать",
        "Интерфейсы","Подключение","Wi-Fi","Емкость лотка","Выходной лоток","Подача бумаги",
        "Оптическое разрешение","Скорость сканирования","Макс. формат","Тип сканирования","Тип датчика",
        "Дисплей","Цвета чернил",
        "Яркость","Контрастность","Входы","Источник света","Ресурс источника","Коррекция трапеции","Поддержка 3D",
        "Мощность","Стабилизация AVR","Стабилизация","Розетки",
        "Страна происхождения","Гарантия","Совместимость",
    ]
    order_idx={k:i for i,k in enumerate(important_order)}
    return [(k,best[k]) for k in sorted(best.keys(), key=lambda x: order_idx.get(x, 999))]

def _html_to_text(desc_html: str) -> str:
    t=re.sub(r"<br\s*/?>", "\n", desc_html or "", flags=re.I)
    t=re.sub(r"</p\s*>", "\n", t, flags=re.I)
    t=re.sub(r"<(script|style|iframe|object|embed|noscript)[^>]*>.*?</\1>", " ", t, flags=re.I|re.S)
    t=re.sub(r"<(table|thead|tbody|tr|td|th)[^>]*>.*?</\1>", " ", t, flags=re.I|re.S)
    t=re.sub(r"<a\b[^>]*>.*?</a>", " ", t, flags=re.I|re.S)
    t=re.sub(r"<[^>]+>", " ", t)
    t=t.replace("\u00A0"," ")
    t=re.sub(r"[ \t]+\n", "\n", t)
    t=re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def split_on_secondary_keys(value: str) -> List[Tuple[str,str]]:
    if not value: return [("", "")]
    s=value.strip()
    keys_sorted = sorted(set(list(KV_KEYS_MAP.keys())+list(CANON_NAME_MAP.keys())+list(KV_KEYS_MAP.values())+list(ALLOWED_PARAM_CANON)), key=len, reverse=True)
    if not keys_sorted: return [("", s)]
    pat = re.compile(r"\b(" + "|".join(re.escape(k) for k in keys_sorted) + r")\b", re.I)
    matches=list(pat.finditer(s))
    if not matches: return [("", s)]
    head = s[:matches[0].start()].strip(" ,;:—–-")
    segs=[("", head)]
    for i,mm in enumerate(matches):
        key=mm.group(1)
        end=matches[i+1].start() if i+1<len(matches) else len(s)
        val=s[mm.end():end].strip(" ,;:—–-")
        segs.append((key, val))
    return segs

def canon_param_name(name: str) -> Optional[str]:
    if not name: return None
    key = name.strip().lower().replace("ё","е")
    if key in DISALLOWED_PARAM_NAMES: return None
    if key in CANON_NAME_MAP: return CANON_NAME_MAP[key]
    if key in KV_KEYS_MAP:    return KV_KEYS_MAP[key]
    title = name.strip()
    if title in ALLOWED_PARAM_CANON: return title
    title_cap = title[:1].upper()+title[1:].lower()
    if title_cap in ALLOWED_PARAM_CANON: return title_cap
    return None

def extract_kv_specs_and_text(desc_html: str, product_name: str) -> Tuple[List[Tuple[str,str]], str]:
    txt = _html_to_text(desc_html)
    lines_raw=[]
    for l in [l.strip() for l in txt.split("\n")]:
        if not l: lines_raw.append(""); continue
        if URL_RE.search(l) or MORE_PHRASES_RE.match(l): continue
        lines_raw.append(l)

    def _norm(s:str)->str:
        s=(s or "").lower(); s=re.sub(r"[\s\-–—:;,.]+"," ", s); return s.strip()
    if lines_raw and product_name and _norm(lines_raw[0])==_norm(product_name):
        lines_raw=lines_raw[1:]

    map_keys = sorted(KV_KEYS_MAP.keys(), key=len, reverse=True)

    def try_split_kv(line: str) -> Optional[Tuple[str,str]]:
        low=line.lower().replace("ё","е")
        for k in map_keys:
            if low.startswith(k):
                rest=line[len(k):].strip(" \t:—-")
                canon = KV_KEYS_MAP[k]
                if canon_param_name(canon) is None and canon!="Комплектация": return None
                return (canon, rest) if rest else (canon, "")
        return None

    specs: List[Tuple[str,str]]=[]; bundle_items: List[str]=[]; out_lines=[]; i=0

    def push_spec(k: str, v: str):
        if not v or v.strip().lower() in {"нет","-","—"}: return
        canon = canon_param_name(k) or k
        if canon in ALLOWED_PARAM_CANON:
            specs.append((canon, v))

    while i < len(lines_raw):
        line = lines_raw[i].strip()
        if not line:
            out_lines.append(""); i+=1; continue
        kv1 = try_split_kv(line)
        if kv1:
            label, value = kv1
            if label=="Комплектация":
                if value:
                    v=value.strip(" .;")
                    if v: bundle_items.append(v)
            else:
                segs = split_on_secondary_keys(value)
                if segs:
                    head = segs[0][1] if segs[0][0]=="" else value
                    if head: push_spec(label, head)
                    for key_raw, val in segs[1:]:
                        canon_key = canon_param_name(key_raw) or KV_KEYS_MAP.get(key_raw.lower().replace("ё","е")) or key_raw
                        if canon_key=="Комплектация":
                            for v in re.split(r"[;,\n]+", val):
                                v=v.strip(" .;")
                                if v: bundle_items.append(v)
                        else:
                            push_spec(canon_key, val)
                else:
                    push_spec(label, value)
            i+=1; continue

        key_raw = line.strip(":").lower().replace("ё","е")
        label = KV_KEYS_MAP.get(key_raw)
        if label:
            i+=1; vals=[]
            while i < len(lines_raw):
                nxt=lines_raw[i].strip()
                if not nxt: i+=1; continue
                if KV_KEYS_MAP.get(nxt.strip(":").lower().replace("ё","е")) or try_split_kv(nxt): break
                vals.append(nxt); i+=1
            value=" ".join(vals).strip()
            if label=="Комплектация":
                for v in re.split(r"[;,\n]+", value):
                    v=v.strip(" .;")
                    if v: bundle_items.append(v)
            else:
                segs = split_on_secondary_keys(value)
                if segs:
                    head = segs[0][1] if segs[0][0]=="" else value
                    if head: push_spec(label, head)
                    for key_raw2, val2 in segs[1:]:
                        canon_key2 = canon_param_name(key_raw2) or KV_KEYS_MAP.get(key_raw2.lower().replace("ё","е")) or key_raw2
                        if canon_key2=="Комплектация":
                            for v in re.split(r"[;,\n]+", val2):
                                v=v.strip(" .;")
                                if v: bundle_items.append(v)
                        else:
                            push_spec(canon_key2, val2)
                else:
                    push_spec(label, value)
            continue

        out_lines.append(line); i+=1

    keys_sorted = sorted(KV_KEYS_MAP.keys(), key=len, reverse=True)
    if keys_sorted:
        keys_alt = r"\b(?:" + "|".join(re.escape(k) for k in keys_sorted) + r")\b"
        matches = list(re.finditer(keys_alt, txt, flags=re.I))
        for idx, m in enumerate(matches):
            raw_key = m.group(0).lower().replace("ё","е")
            label = KV_KEYS_MAP.get(raw_key)
            if not label: continue
            start = m.end()
            end   = matches[idx+1].start() if idx+1 < len(matches) else len(txt)
            value = txt[start:end].strip(" \t:—-.,;")
            value = re.sub(r"\s{2,}", " ", value)[:600].strip()
            if not value or value.lower() in {"нет","-","—"}: continue
            segs = split_on_secondary_keys(value)
            if segs:
                head = segs[0][1] if segs[0][0]=="" else value
                if head: push_spec(label, head)
                for key_raw2, val2 in segs[1:]:
                    canon_key2 = canon_param_name(key_raw2) or KV_KEYS_MAP.get(key_raw2.lower().replace("ё","е")) or key_raw2
                    if canon_key2=="Комплектация":
                        for v in re.split(r"[;,\n]+", val2):
                            v=v.strip(" .;")
                            if v: bundle_items.append(v)
                    else:
                        push_spec(canon_key2, val2)
            else:
                push_spec(label, value)

    if bundle_items:
        bundle="; ".join(dict.fromkeys([b for b in bundle_items if b]))
        if bundle: specs.append(("Комплектация", bundle))

    # Классификация для правил (расходник/устройство)
    def classify_product(name: str, desc_text: str) -> str:
        n=(name or "").lower()+" "+(desc_text or "").lower()
        if any(k in n for k in ["картридж","тонер","емкость для отработанных","maintenance box","чернила","порошок","ribbon","фотобарабан","drum"]):
            return "consumable"
        if any(k in n for k in ["кабель","шнур","адаптер","лоток","крышка","держатель","подставка","брекет","лампа"]):
            return "accessory"
        return "device"

    kind = classify_product(product_name, txt)
    specs = postclean_specs(_dedupe_specs(specs), kind)
    native_plain="\n".join([ln for ln in out_lines if ln.strip()]).strip()
    return specs, native_plain

ALLOWED_TAGS = ("h3","p","ul","ol","li","br","strong","em","b","i")

def sanitize_supplier_html(raw_html: str) -> str:
    s = raw_html or ""
    s = maybe_unescape_html(s)
    s = re.sub(r"<(script|style|iframe|object|embed|noscript)[^>]*>.*?</\1>", " ", s, flags=re.I|re.S)
    s = re.sub(r"</?(table|thead|tbody|tr|td|th|img)[^>]*>", " ", s, flags=re.I|re.S)
    s = re.sub(r"<a\b[^>]*>", "", s, flags=re.I); s = re.sub(r"</a>", "", s, flags=re.I)
    s = re.sub(r"<h[1-6]\b[^>]*>", "<h3>", s, flags=re.I); s = re.sub(r"</h[1-6]>", "</h3>", s, flags=re.I)
    s = re.sub(r"<div\b[^>]*>", "<p>", s, flags=re.I); s = re.sub(r"</div>", "</p>", s, flags=re.I)
    s = re.sub(r"</?span\b[^>]*>", "", s, flags=re.I)
    s = re.sub(r"(?:\s*<br\s*/?>\s*){2,}", "</p><p>", s, flags=re.I)
    s = re.sub(r"\sstyle\s*=\s*(['\"]).*?\1", "", s, flags=re.I)
    s = re.sub(r"\s(class|id|align|width|height)\s*=\s*(['\"]).*?\2", "", s, flags=re.I)
    s = re.sub(r"</?(?!"+("|".join(ALLOWED_TAGS))+r")\w+[^>]*>", " ", s, flags=re.I)
    s = re.sub(r"<(p|li|ul|ol)>\s*</\1>", "", s, flags=re.I)
    s = re.sub(r"</p>\s*<p>", "</p>\n<p>", s, flags=re.I)
    s = re.sub(r"\s{2,}", " ", s).strip()
    if s and not re.match(r"^\s*<", s):
        s = f"<p>{_html_escape_in_cdata_safe(s)}</p>"
    return s

def strip_specs_from_supplier_html(s: str) -> str:
    if not s: return s
    keys = sorted(KV_KEYS_MAP.keys(), key=len, reverse=True)
    if not keys: return s
    keys_alt = "(?:" + "|".join(re.escape(k) for k in keys) + ")"
    out = s
    out = re.sub(
        r"<h3>\s*(?:технические характеристики|характеристики|состав поставки|комплектация)\s*</h3>\s*(?:(?:<(?:ul|ol)[^>]*>.*?</(?:ul|ol)>)|(?:<p>.*?</p>))?",
        "",
        out, flags=re.I|re.S
    )
    out = re.sub(rf"<li>\s*{keys_alt}\b.*?</li>", "", out, flags=re.I|re.S)
    def _clean_p(m: re.Match) -> str:
        full = m.group(0); inner = m.group(1) or ""
        if re.match(rf"^\s*{keys_alt}\b", inner, flags=re.I): return ""
        km = re.search(rf"{keys_alt}\b", inner, flags=re.I)
        if not km: return full
        keep = inner[:km.start()].rstrip(" ,;:—-")
        if not re.search(r"[A-Za-zА-Яа-яЁё0-9]", keep): return ""
        return f"<p>{keep}</p>"
    out = re.sub(r"<p>(.*?)</p>", _clean_p, out, flags=re.I|re.S)
    out = re.sub(r"<(p|li|ul|ol)>\s*</\1>", "", out, flags=re.I|re.S)
    out = re.sub(r"</p>\s*<p>", "</p>\n<p>", out, flags=re.I|re.S)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out

def split_sentences(text: str) -> List[str]:
    t = re.sub(r"\s+", " ", text).strip()
    parts = re.split(r"(?<=[\.\!\?;])\s+(?=[A-ZА-ЯЁ0-9])", t)
    out=[]
    for p in parts:
        p=p.strip(" .,\t;—–-")
        if not p: continue
        out.append(p)
    return out

FLUFF_RE = re.compile("|".join(re.escape(x) for x in [
    "раскройте свой творческий потенциал","идеальным выбором","современные возможности",
    "просты как никогда","экономия времени и денег","дайте волю","впечатляющ","великолепн",
    "гибкие возможности","простая настройка","подойдет для","предназначен для широкого круга задач"
]), re.I)

def sentence_is_fluff(s: str) -> bool:
    if FLUFF_RE.search(s): return True
    if len(s.split())<=3: return True
    return False

def sentence_duplicates_specs(s: str, specs: List[Tuple[str,str]]) -> bool:
    low=s.lower()
    spec_keys = [k.lower() for k,_ in specs]
    if any(k in low for k in spec_keys): return True
    if len(re.findall(r"\d", s))>=4: return True
    return False

def build_lead_from_supplier(supplier_html: str, name: str, specs: List[Tuple[str,str]]) -> str:
    txt = _html_to_text(supplier_html)
    sentences = split_sentences(txt)
    # классификация для лимита
    def classify_product(name: str, desc_text: str) -> str:
        n=(name or "").lower()+" "+(desc_text or "").lower()
        if any(k in n for k in ["картридж","тонер","емкость для отработанных","maintenance box","чернила","порошок","ribbon","фотобарабан","drum"]):
            return "consumable"
        if any(k in n for k in ["кабель","шнур","адаптер","лоток","крышка","держатель","подставка","брекет","лампа"]):
            return "accessory"
        return "device"
    kind = classify_product(name, txt)
    limit = 220 if kind=="consumable" else (300 if kind=="accessory" else 600)

    clean=[]
    for s in sentences:
        if sentence_is_fluff(s): continue
        if sentence_duplicates_specs(s, specs): continue
        if URL_RE.search(s): continue
        clean.append(s)
    if not clean and sentences:
        clean = sentences[:2]
    lead=""
    for s in clean:
        add = s if not lead else (lead + ". " + s)
        if len(add) <= limit:
            lead = add
        else:
            if not lead:
                words=s.split()
                while len(" ".join(words))>limit and len(words)>5:
                    words=words[:-1]
                lead=" ".join(words).rstrip(" ,;:—–-")
            break
    lead = lead.strip(" .")
    if lead: lead += "."
    return lead

def postprocess_supplier_html(html_in: str, product_name: str) -> str:
    s = html_in or ""
    s = re.sub(r"\bhttps?://[^\s<]+", "", s, flags=re.I)
    s = re.sub(r"\bwww\.[^\s<]+", "", s, flags=re.I)
    def drop_cta_p(m: re.Match) -> str:
        inner = m.group(1)
        if MORE_PHRASES_RE.match(inner) or FLUFF_RE.search(inner):
            return ""
        return m.group(0)
    s = re.sub(r"<p>(.*?)</p>", drop_cta_p, s, flags=re.I|re.S)
    if product_name:
        pat = re.compile(rf"^\s*{re.escape(product_name)}\s*[—–\-:,]*\s*", re.I)
        def cut_name_prefix(m: re.Match) -> str:
            inner = m.group(1)
            cleaned = pat.sub("", inner).strip()
            return f"<p>{cleaned}</p>" if cleaned else ""
        s = re.sub(r"<p>(.*?)</p>", cut_name_prefix, s, flags=re.I|re.S)
    s = re.sub(r"<(p|li|ul|ol)>\s*</\1>", "", s, flags=re.I|re.S)
    s = re.sub(r"</p>\s*<p>", "</p>\n<p>", s, flags=re.I|re.S)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def render_specs_html(specs: List[Tuple[str,str]]) -> str:
    if not specs: return ""
    out=["<h3>Характеристики</h3>","<ul>"]
    for k,v in specs:
        if k=="Комплектация": continue
        out.append(f'  <li><strong>{_html_escape_in_cdata_safe(k)}:</strong> { _html_escape_in_cdata_safe(v) }</li>')
    out.append("</ul>")
    return "\n".join(out)

def render_bundle_html(specs: List[Tuple[str,str]]) -> str:
    for k,v in specs:
        if k=="Комплектация" and v:
            items=[it.strip() for it in re.split(r"[;,\n]+", v) if it.strip()]
            if not items: return ""
            out=["<h3>Состав поставки</h3>","<ul>"]
            for it in items: out.append(f"  <li>{_html_escape_in_cdata_safe(it)}</li>")
            out.append("</ul>")
            return "\n".join(out)
    return ""

def collect_allowed_params_from_existing(offer: ET.Element) -> List[Tuple[str,str]]:
    res: Dict[str,str] = {}
    for tag in ("Param","param","PARAM"):
        for pn in offer.findall(tag):
            name = pn.get("name") or pn.get("Name") or ""
            value = (pn.text or "").strip()
            if not name or not value: continue
            canon = canon_param_name(name)
            if not canon: continue
            old = res.get(canon, "")
            if len(value) > len(old):
                res[canon] = value
    return list(res.items())

def write_params_from_specs(offer: ET.Element, specs: List[Tuple[str,str]], name_for_kind: str) -> int:
    # Определяем тип для фильтра «Совместимость»
    def classify_product(name: str) -> str:
        n=(name or "").lower()
        if any(k in n for k in ["картридж","тонер","емкость для отработанных","maintenance box","чернила","порошок","ribbon","фотобарабан","drum"]):
            return "consumable"
        if any(k in n for k in ["кабель","шнур","адаптер","лоток","крышка","держатель","подставка","брекет","лампа"]):
            return "accessory"
        return "device"
    kind = classify_product(name_for_kind)

    keep_from_existing = collect_allowed_params_from_existing(offer)
    for tag in ("param","Param","PARAM"):
        for pn in list(offer.findall(tag)): offer.remove(pn)

    merged: Dict[str,str] = {}
    for k,v in keep_from_existing:
        if k in ALLOWED_PARAM_CANON and v:
            merged[k] = v.strip()

    for k,v in specs:
        if k=="Комплектация":
            continue
        if k=="Совместимость" and kind!="consumable":
            continue
        if k in ALLOWED_PARAM_CANON and v:
            v=v.strip()
            if not v: continue
            prev = merged.get(k, "")
            # защита от «Совместимости» с артикульными кодами
            if k=="Совместимость" and not re.search(r"[A-Za-z]{2,}-?\d{2,}", v):
                continue
            if len(v) > len(prev): merged[k] = v

    # принудительно «цветная», если есть список чернил >1
    if merged.get("Цвет печати","")=="ч/б":
        cols = merged.get("Цвета чернил","")
        if cols and "," in cols:
            merged["Цвет печати"]="цветная"

    important_order = [
        "Тип","Назначение","Тип печати","Цвет печати","Формат","Разрешение печати","Скорость печати","Двусторонняя печать",
        "Интерфейсы","Подключение","Wi-Fi","Емкость лотка","Выходной лоток","Подача бумаги",
        "Оптическое разрешение","Скорость сканирования","Макс. формат","Тип сканирования","Тип датчика",
        "Дисплей","Цвета чернил",
        "Яркость","Контрастность","Источник света","Ресурс источника","Входы","Коррекция трапеции","Поддержка 3D",
        "Мощность","Стабилизация AVR","Стабилизация","Розетки",
        "Страна происхождения","Гарантия","Совместимость",
    ]
    order_idx={k:i for i,k in enumerate(important_order)}

    added=0
    for k in sorted(merged.keys(), key=lambda it: order_idx.get(it, 999)):
        val = merged[k]
        if not val: continue
        if k=="Гарантия" and not re.search(r"\d", val): continue
        if len(val)>PARAMS_MAX_VALUE_LEN: val = val[:PARAMS_MAX_VALUE_LEN-1] + "…"
        node=ET.SubElement(offer,"param"); node.set("name", k); node.text=val; added+=1
    return added

def build_keywords_for_offer(offer: ET.Element) -> str:
    WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
    def tokenize(s: str) -> List[str]: return WORD_RE.findall(s or "")
    def dedup(words: List[str]) -> List[str]:
        seen=set(); out=[]
        for w in words:
            k=w.lower()
            if k and k not in seen: seen.add(k); out.append(w)
        return out
    def translit_ru_to_lat(s: str) -> str:
        table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"p","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
        out=s.lower().translate(table); out=re.sub(r"[^a-z0-9\- ]+","", out); return re.sub(r"\s+","-", out).strip("-")

    name=get_text(offer,"name"); vendor=get_text(offer,"vendor").strip()
    desc_html=inner_html(offer.find("description"))
    base=[vendor] if vendor else []
    raw_tokens=tokenize(name or "")
    modelish=[t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content=[t for t in raw_tokens if (t.lower() not in {"для","и","или","на","в","из","от","по","с","к","до","при","через","над","под","о","об","у","без","про","как","это","тип","модель","комплект","формат","новый","новинка","оригинальный"}) and (any(ch.isdigit() for ch in t) or "-" in t or len(t)>=3)]
    bigr=[]
    for i in range(len(content)-1):
        a,b=content[i],content[i+1]
        bigr.append(f"{a} {b}")
    base += list(set([t.upper() for t in modelish[:8]])) + bigr[:8] + [t.capitalize() if not re.search(r"[A-Z]{2,}",t) else t for t in content[:10]]

    # добавим латиницу для русских слов
    extra=[]
    for w in base:
        if re.search(r"[А-Яа-яЁё]", str(w)):
            tr=translit_ru_to_lat(str(w))
            if tr and tr not in extra: extra.append(tr)
    base += extra

    if SATU_KEYWORDS_GEO:
        geo=["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
             "Оскемен","Семей","Костанаи","Кызылорда","Орал","Петропавл","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]
        if SATU_KEYWORDS_GEO_LAT:
            geo += ["Kazakhstan","Almaty","Astana","Shymkent","Karaganda","Aktobe","Pavlodar","Atyrau","Taraz","Oskemen","Semey","Kostanay","Kyzylorda","Oral","Petropavl","Taldykorgan","Aktau","Temirtau","Ekibastuz","Kokshetau","Rudny"]
        base += geo[:SATU_KEYWORDS_GEO_MAX]

    # цвета из названия
    colors=[]; low=name.lower() if name else ""
    mapping={"жёлт":"желтый","желт":"желтый","yellow":"yellow","черн":"черный","black":"black","син":"синий","blue":"blue",
             "красн":"красный","red":"red","зелен":"зеленый","green":"green","серебр":"серебряный","silver":"silver","циан":"cyan","магент":"magenta"}
    for k,val in mapping.items():
        if k in low and val not in colors: colors.append(val)
    base += colors

    parts=dedup([p for p in base if p])
    res=[]; total=0
    for p in parts:
        add=((", " if res else "")+p)
        if total+len(add)>SATU_KEYWORDS_MAXLEN: break
        res.append(p); total+=len(add)
    return ", ".join(res)

def ensure_keywords(out_shop: ET.Element) -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    touched=0
    for offer in off_el.findall("offer"):
        kw=build_keywords_for_offer(offer)
        node=offer.find("keywords")
        if not kw:
            if node is not None: offer.remove(node)
            continue
        if node is None:
            node=ET.SubElement(offer,"keywords"); node.text=kw; touched+=1
        else:
            if (node.text or "")!=kw: node.text=kw; touched+=1
    return touched

ARTICUL_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)
def _extract_article_from_name(name: str) -> str:
    if not name: return ""
    m = ARTICUL_RE.search(name)
    return (m.group(1) if m else "").upper()
def _extract_article_from_url(url: str) -> str:
    if not url: return ""
    try:
        path = urllib.parse.urlparse(url).path.rstrip("/")
        last = re.sub(r"\.(html?|php|aspx?)$", "", path.split("/")[-1], flags=re.I)
        m = ARTICUL_RE.search(last)
        return (m.group(1) if m else last).upper()
    except Exception:
        return ""
def _normalize_code(s: str) -> str:
    s = (s or "").strip()
    if not s: return ""
    s = re.sub(r"[\s_]+", "", s).replace("—", "-").replace("–", "-")
    return re.sub(r"[^A-Za-z0-9\-]+", "", s).upper()

def ensure_vendorcode_with_article(out_shop: ET.Element, prefix: str, create_if_missing: bool = False) -> None:
    off_el = out_shop.find("offers")
    if off_el is None: return
    for offer in off_el.findall("offer"):
        vc = offer.find("vendorCode")
        if vc is None:
            if not create_if_missing: continue
            vc = ET.SubElement(offer, "vendorCode"); vc.text = ""
        old = (vc.text or "").strip()
        if (old == "") or (old.upper() == prefix.upper()):
            art = (_normalize_code(offer.attrib.get("article") or "") or
                   _normalize_code(_extract_article_from_name(get_text(offer, "name"))) or
                   _normalize_code(_extract_article_from_url(get_text(offer, "url"))) or
                   _normalize_code(offer.attrib.get("id") or ""))
            if art: vc.text = art
        vc.text = f"{prefix}{(vc.text or '')}"

def sync_offer_id_with_vendorcode(out_shop: ET.Element) -> None:
    off_el=out_shop.find("offers")
    if off_el is None: return
    for offer in off_el.findall("offer"):
        vc=offer.find("vendorCode")
        if vc is None: continue
        code=(vc.text or "").strip()
        if not code: continue
        if offer.attrib.get("id")!=code:
            offer.attrib["id"]=code

_url_head_cache: Dict[str,bool]={}
def url_exists(url: str) -> bool:
    if not url: return False
    if url in _url_head_cache: return _url_head_cache[url]
    try:
        r=requests.head(url, timeout=PLACEHOLDER_HEAD_TIMEOUT, allow_redirects=True)
        ok=(200<=r.status_code<400)
    except Exception: ok=False
    _url_head_cache[url]=ok; return ok
def _slug(s: str) -> str:
    if not s: return "unknown"
    table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"p","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    base=(s or "").lower().translate(table); base=re.sub(r"[^a-z0-9\- ]+","", base)
    return re.sub(r"\s+","-", base).strip("-") or "unknown"
def _placeholder_url_brand(vendor: str) -> str: return f"{PLACEHOLDER_BRAND_BASE}/{_slug(vendor)}.{PLACEHOLDER_EXT}"
def _placeholder_url_category(name: str) -> str:
    n=(name or "").lower()
    if "картридж" in n or "тонер" in n: return f"{PLACEHOLDER_CATEGORY_BASE}/cartridge.{PLACEHOLDER_EXT}"
    if "ибп" in n or "ups" in n or "источник бесперебойного питания" in n: return f"{PLACEHOLDER_CATEGORY_BASE}/ups.{PLACEHOLDER_EXT}"
    if "сканер" in n or "scanner" in n: return f"{PLACEHOLDER_CATEGORY_BASE}/scanner.{PLACEHOLDER_EXT}"
    if "проектор" in n or "projector" in n: return f"{PLACEHOLDER_CATEGORY_BASE}/projector.{PLACEHOLDER_EXT}"
    if "принтер" in n or "мфу" in n or "mfp" in n: return f"{PLACEHOLDER_CATEGORY_BASE}/mfp.{PLACEHOLDER_EXT}"
    return f"{PLACEHOLDER_CATEGORY_BASE}/other.{PLACEHOLDER_EXT}"
def ensure_placeholder_pictures(out_shop: ET.Element) -> int:
    if not PLACEHOLDER_ENABLE: return 0
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    added=0
    for offer in off_el.findall("offer"):
        pics=list(offer.findall("picture"))
        has_pic=any((p.text or "").strip() for p in pics)
        if has_pic: continue
        vendor=get_text(offer,"vendor").strip(); name=get_text(offer,"name").strip()
        picked=""
        if vendor:
            u=_placeholder_url_brand(vendor)
            if url_exists(u): picked=u
        if not picked:
            u=_placeholder_url_category(name)
            if url_exists(u): picked=u
        if not picked: picked=PLACEHOLDER_DEFAULT_URL
        ET.SubElement(offer,"picture").text=picked; added+=1
    return added

def rebuild_descriptions_preserve_supplier(out_shop: ET.Element) -> Tuple[int,int]:
    off_el=out_shop.find("offers")
    if off_el is None: return (0,0)
    changed=0; params_added_total=0
    for offer in off_el.findall("offer"):
        name=get_text(offer,"name")
        d=offer.find("description")
        raw_html=inner_html(d)
        supplier_html = sanitize_supplier_html(raw_html)
        specs, _ = extract_kv_specs_and_text(supplier_html, name)

        # вырезаем потенциальные «характеристики» из нативного текста
        supplier_html = strip_specs_from_supplier_html(supplier_html)
        supplier_html = postprocess_supplier_html(supplier_html, name)
        lead = build_lead_from_supplier(supplier_html, name, specs)

        parts=[f"<h3>{_html_escape_in_cdata_safe(name)}</h3>"]
        if lead:
            parts.append(f"<p>{_html_escape_in_cdata_safe(lead)}</p>")
        bundle_html = render_bundle_html(specs)
        if bundle_html: parts.append(bundle_html)
        specs_html = render_specs_html(specs)
        if specs_html: parts.append(specs_html)
        full_html = "\n".join([p for p in parts if p]).strip()
        placeholder=f"[[[HTML]]]{full_html}[[[/HTML]]]"

        if d is None:
            d=ET.SubElement(offer,"description"); d.text=placeholder; changed+=1
        else:
            if (d.text or "") != placeholder:
                d.text=placeholder; changed+=1

        params_added_total += write_params_from_specs(offer, specs, name)
    return (changed, params_added_total)

def _replace_html_placeholders_with_cdata(xml_text: str) -> str:
    def repl(m):
        inner=m.group(1).replace("[[[HTML]]]", "").replace("[[[/HTML]]]", "")
        inner = html.unescape(inner)
        return f"<description><![CDATA[\n{inner}\n]]></description>"
    return re.sub(
        r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>",
        repl, xml_text, flags=re.S
    )

# ===================== KEYWORDS / VENDORCODE / ORDER =====================
def ensure_keywords_all(out_shop: ET.Element) -> int:
    return ensure_keywords(out_shop)

def ensure_vendorcode_with_article_all(out_shop: ET.Element, prefix: str, create_if_missing: bool):
    ensure_vendorcode_with_article(out_shop, prefix, create_if_missing)
    sync_offer_id_with_vendorcode(out_shop)

# ===================== MAIN =====================
def main()->None:
    log("Run set -e")
    log(f"Python {sys.version.split()[0]}")
    log(f"Source: {SUPPLIER_URL}")

    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")
    offers_in=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in is None: err("XML: <offers> not found")
    src_offers=list(offers_in.findall("offer"))

    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")

    for o in src_offers:
        mod=deepcopy(o)
        if DROP_CATEGORY_ID_TAG:
            for node in list(mod.findall("categoryId"))+list(mod.findall("CategoryId")): mod.remove(node)
        out_offers.append(mod)

    keys=load_name_filter(AKCENT_KEYWORDS_PATH)
    if AKCENT_KEYWORDS_MODE=="include" and len(keys)==0:
        err("AKCENT_KEYWORDS_MODE=include, но файл docs/akcent_keywords.txt пуст или не найден.", 2)
    if (AKCENT_KEYWORDS_MODE in {"include","exclude"}) and len(keys)>0:
        for off in list(out_offers.findall("offer")):
            nm=get_text(off,"name"); hit=name_matches(nm,keys)
            drop=(AKCENT_KEYWORDS_MODE=="exclude" and hit) or (AKCENT_KEYWORDS_MODE=="include" and not hit)
            if drop: out_offers.remove(off)

    flag_unrealistic_supplier_prices(out_shop)
    ensure_vendor(out_shop)
    ensure_vendorcode_with_article_all(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AC"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )
    reprice_offers(out_shop, PRICING_RULES)
    ensure_placeholder_pictures(out_shop)

    desc_changed, params_added = rebuild_descriptions_preserve_supplier(out_shop)

    t_true, t_false = normalize_available_field(out_shop)
    fix_currency_id(out_shop, default_code="KZT")

    for off in out_offers.findall("offer"):
        for t in PURGE_TAGS_AFTER:
            for node in list(off.findall(t)): off.remove(node)
        for a in PURGE_OFFER_ATTRS_AFTER:
            if a in off.attrib: off.attrib.pop(a,None)

    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)
    ensure_keywords_all(out_shop)

    built_alm=now_almaty()
    # feed_meta — оставляем «как раньше»: без лишних полей, только ключевые строки
    out_root.insert(0, ET.Comment(
        "FEED_META\n"
        f"Поставщик              | {SUPPLIER_NAME}\n"
        f"URL поставщика        | {SUPPLIER_URL}\n"
        f"Время сборки (Алматы) | {format_dt_almaty(built_alm)}\n"
        f"Товаров исходно       | {len(src_offers)}\n"
        f"Товаров в выгрузке    | {len(list(out_offers.findall('offer')))}\n"
        f"В наличии             | {t_true}\n"
        f"Нет в наличии         | {t_false}\n"
        f"Скрипт                | {SCRIPT_VERSION}"
    ))

    try: ET.indent(out_root, space="  ")
    except Exception: pass

    xml_bytes=ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text=xml_bytes.decode(ENC, errors="replace")
    xml_text=_replace_html_placeholders_with_cdata(xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    try:
        with open(OUT_FILE_YML,"w",encoding=ENC, newline="\n") as f: f.write(xml_text)
    except UnicodeEncodeError as e:
        warn(f"{ENC} encode issue ({e}); using xmlcharrefreplace fallback")
        with open(OUT_FILE_YML,"wb") as f: f.write(xml_text.encode(ENC, errors="xmlcharrefreplace"))

    try:
        docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e: warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | {SCRIPT_VERSION} | descriptions_changed={desc_changed} | params_added={params_added}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
