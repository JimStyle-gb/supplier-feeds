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

SCRIPT_VERSION = "akcent-2025-10-23.v2.2.0"

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

# ===================== TEXT / HTML (НОВАЯ ЛОГИКА) =====================
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
MORE_PHRASES_RE = re.compile(r"^\s*(подробнее|читать далее|узнать больше|все детали|подробности|смотреть на сайте производителя|скачать инструкцию)\s*\.?\s*$", re.I)

def maybe_unescape_html(s: str) -> str:
    if not s: return s
    if re.search(r"&lt;/?[a-zA-Z]", s):
        for _ in range(2):
            s = html.unescape(s)
            if not re.search(r"&lt;/?[a-zA-Z]", s): break
    return s

ALLOWED_TAGS = ("h3","p","ul","ol","li","br","strong","em","b","i")

def sanitize_supplier_html(raw_html: str) -> str:
    """Оставляем HTML от поставщика, но чистим мусор и небезопасные теги/атрибуты."""
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

def postprocess_supplier_html(html_in: str, product_name: str) -> str:
    """Лёгкая чистка: убираем ссылки/CTA, дубли имени. Никаких характеристик отсюда не тянем."""
    s = html_in or ""
    s = re.sub(r"\bhttps?://[^\s<]+", "", s, flags=re.I)
    s = re.sub(r"\bwww\.[^\s<]+", "", s, flags=re.I)
    def drop_cta_p(m: re.Match) -> str:
        inner = m.group(1)
        if MORE_PHRASES_RE.match(inner):
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

# ===================== PARAM -> SPECS (ТОЛЬКО ИЗ <Param>) =====================
DISALLOWED_PARAM_NAMES = {
    "производитель","для бренда","наименование производителя","сопутствующие товары",
    "бренд","brand","manufacturer","vendor","поставщик","партномер","артикул поставщика","код на складе",
}
# Канонизация имён параметров
CANON_NAME_MAP = {
    "тип":"Тип", "вид":"Тип",
    "тип печати":"Тип печати",
    "цвет печати":"Цвет печати", "цветность":"Цвет печати",
    "формат":"Формат","формат бумаги":"Формат","бумаги":"Формат",
    "разрешение печати":"Разрешение печати","разрешение печати,dpi":"Разрешение печати",
    "разрешение":"Разрешение печати",
    "оптическое разрешение":"Оптическое разрешение","разрешение сканера":"Оптическое разрешение","разрешение сканера,dpi":"Оптическое разрешение",
    "скорость печати":"Скорость печати","максимальная скорость печати а4 стр/мин":"Скорость печати",
    "двусторонняя печать":"Двусторонняя печать",
    "интерфейс":"Интерфейсы","интерфейсы":"Интерфейсы","входной интерфейс":"Интерфейсы","входы":"Интерфейсы","подключение":"Интерфейсы",
    "wi-fi":"Wi-Fi",
    "подача бумаги":"Подача бумаги","количество бумажных ящиков":"Подача бумаги",
    "выход лоток":"Выходной лоток","емкость лотка":"Емкость лотка",
    "дисплей":"Дисплей","жк дисплей":"Дисплей",
    "цвета чернил":"Цвета чернил","цвета":"Цвета чернил","colors":"Цвета чернил","colours":"Цвета чернил",
    "источник света":"Источник света","ресурс лампы":"Ресурс источника","ресурс источника":"Ресурс источника",
    "яркость":"Яркость","контрастность":"Контрастность",
    "коррекция трапеции":"Коррекция трапеции","поддержка 3d":"Поддержка 3D",
    "мощность":"Мощность","мощность, ва":"Мощность",
    "стабилизация avr":"Стабилизация AVR","стабилизация":"Стабилизация",
    "розетки":"Розетки","тип розеток":"Розетки",
    "страна происхождения":"Страна происхождения",
    "гарантия":"Гарантия",
    "совместимые продукты":"Совместимость","совместимость":"Совместимость","совместимые модели":"Совместимость",
    "для моделей":"Совместимость","совместимые принтеры":"Совместимость","поддерживаемые модели принтеров":"Совместимость",
    "тип датчика":"Тип датчика","тип сканирования":"Тип сканирования",
    "подсветка":"Подсветка",
    "комплектация":"Комплектация","состав поставки":"Комплектация","в комплекте":"Комплектация",
    # Расходники
    "тип чернил":"Тип чернил","ресурс":"Ресурс","объем":"Объем","объём":"Объем",
    # Прочее
    "автоподатчик":"Автоподатчик",
}

ALLOWED_PARAM_CANON = {
    # Общие
    "Тип","Назначение","Тип печати","Цвет печати","Формат",
    "Разрешение печати","Скорость печати","Двусторонняя печать","Интерфейсы","Wi-Fi",
    "Подача бумаги","Выходной лоток","Емкость лотка","Дисплей",
    "Страна происхождения","Гарантия",
    # Сканер
    "Оптическое разрешение","Тип датчика","Тип сканирования","Макс. формат","Подсветка",
    "Скорость сканирования",
    # Проектор/панель/монитор
    "Разрешение","Яркость","Контрастность","Источник света","Ресурс источника",
    "Входы","Коррекция трапеции","Поддержка 3D","Мощность","Стабилизация AVR","Стабилизация","Розетки",
    # Струйные особенности
    "Цвета чернил","Тип чернил",
    # Расходники
    "Ресурс","Объем","Совместимость",
    # Прочее
    "Автоподатчик","Комплектация",
}

def canon_param_name(name: str) -> Optional[str]:
    if not name: return None
    key = name.strip().lower().replace("ё","е")
    if key in DISALLOWED_PARAM_NAMES: return None
    if key in CANON_NAME_MAP: return CANON_NAME_MAP[key]
    # заглавная первая буква:
    title = name.strip()
    title_cap = title[:1].upper()+title[1:].lower()
    if title_cap in ALLOWED_PARAM_CANON: return title_cap
    # допускаем уже каноничное имя:
    if title in ALLOWED_PARAM_CANON: return title
    return None

# Нормализация значений
def _norm_resolution_print(s: str) -> str:
    t=s.replace("\u00A0"," ").replace("x","×").replace("X","×")
    t=re.sub(r"(?<=\d)[\.,](?=\d{3}\b)","", t)
    m=re.search(r"(\d{2,5})\s*×\s*(\d{2,5})", t)
    return (f"{m.group(1)}×{m.group(2)} dpi" if m else s.strip())

def _norm_interfaces(s: str) -> str:
    t=s
    repl={"wifi":"Wi-Fi","wi-fi":"Wi-Fi","wi fi":"Wi-Fi","usb-хост":"USB-host","usb host":"USB-host",
          "ethernet":"Ethernet","bluetooth":"Bluetooth","rs-232":"RS-232","rj-45":"RJ-45"}
    for k,v in repl.items(): t=re.sub(rf"\b{k}\b", v, t, flags=re.I)
    t=re.sub(r"[\*/;/]\s*", ", ", t); t=re.sub(r"\s*,\s*", ", ", t)
    t=re.sub(r"\s{2,}"," ", t).strip(" ,;")
    # убрать «безопасность», «облако», «питание», если прилипло:
    junk = ["wlan security","безопасность wlan","scan-to-cloud","apple airprint","mopria","питание","power"]
    for j in junk:
        t = re.sub(rf",?\s*{re.escape(j)}[^,]*", "", t, flags=re.I)
    return t.strip(" ,;")

def _norm_yesno(s: str) -> str:
    low=s.strip().lower()
    if re.search(r"^(да|yes|y|true|есть)$", low): return "Да"
    if re.search(r"^(нет|no|n|false|отсутствует)$", low): return "Нет"
    return s.strip()

def _norm_display(s: str) -> str:
    t=s.replace(",", "."); m=re.search(r"(\d{1,2}(\.\d)?)", t)
    return f"{m.group(1)} см" if m else s.strip()

def _norm_speed(s: str) -> str:
    m=re.search(r"(\d{1,3})\s*(стр|pages)\s*/?\s*мин", s, re.I)
    return f"до {m.group(1)} стр/мин" if m else s.strip()

_ALLOWED_FORMAT_TOKEN = re.compile(
    r"^(A\d|B\d|C6|DL|Letter|Legal|No\.?\s*10|\d{1,2}\s*[×x]\s*\d{1,2}|16:9|10\s*×\s*15|13\s*×\s*18|9\s*×\s*13)$",
    re.I
)
def _norm_format(s: str) -> str:
    t = s.replace("бумаги", "")
    t = re.sub(r"\b(см|cm)\b", "", t)
    t = re.sub(r"\s*[xX]\s*", "×", t)
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

def _norm_colors_list(s: str) -> str:
    t=re.sub(r"\[[^\]]*\]","", s)
    t=re.sub(r"\bcapacity\b","", t, flags=re.I)
    parts = re.split(r"[,/;]+|\s+\+\s+", t)
    norm=[]
    mapc={"black":"Black","photo black":"Photo Black","cyan":"Cyan","magenta":"Magenta","yellow":"Yellow",
          "grey":"Grey","gray":"Grey","light cyan":"Light Cyan","light magenta":"Light Magenta"}
    for p in parts:
        w=p.strip().lower()
        if not w: continue
        if w in mapc: norm.append(mapc[w])
        else:
            if re.fullmatch(r"(black|grey|gray|cyan|magenta|yellow|photo black|light cyan|light magenta)", w):
                norm.append(mapc.get(w, w.title()))
    seen=set(); out=[]
    for x in norm:
        if x not in seen: out.append(x); seen.add(x)
    return ", ".join(out)

def clean_param_value(key: str, value: str) -> str:
    if not value: return ""
    s = value.replace("\u00A0"," ").strip()
    s = URL_RE.sub("", s)
    s = re.sub(r"\s{2,}"," ", s).strip(" ,;:.—–-")
    # ограничим «болтовню»
    if len(s) > PARAMS_MAX_VALUE_LEN:
        s = s[:PARAMS_MAX_VALUE_LEN-1] + "…"
    return s

def normalize_value_by_key(k: str, v: str) -> str:
    key=k.lower(); s=v.strip()
    if key=="разрешение печати":       return _norm_resolution_print(s)
    if key in ("интерфейсы","входы"):  return _norm_interfaces(s)
    if key=="wi-fi":                    return _norm_yesno(s)
    if key=="формат":                   return _norm_format(s)
    if key=="дисплей":                  return _norm_display(s)
    if key=="скорость печати":          return _norm_speed(s)
    if key=="цвета чернил":             return _norm_colors_list(s)
    if key=="двусторонняя печать":      return _norm_yesno(s)
    if key=="оптическое разрешение":    return _norm_resolution_print(s)
    return clean_param_value(k, s)

def classify_kind(name: str) -> str:
    n=(name or "").lower()
    if any(k in n for k in ["картридж","чернила","емкость для отработанных","maintenance box","тонер","ribbon","фотобарабан","drum"]):
        return "consumable"
    if any(k in n for k in ["пленка для ламинирования","пленка ламин","плівка"]):
        return "consumable"
    # аксессуары
    if any(k in n for k in ["кабель","шнур","адаптер","лоток","крышка","держатель","подставка","брекет","лампа"]):
        return "accessory"
    return "device"

def collect_params_canonical(offer: ET.Element) -> List[Tuple[str,str]]:
    name = get_text(offer,"name")
    kind = classify_kind(name)
    merged: Dict[str,str] = {}

    # собрать все <Param>/<param>
    for tag in ("Param","param","PARAM"):
        for pn in offer.findall(tag):
            raw_name = (pn.get("name") or pn.get("Name") or "").strip()
            raw_val  = (pn.text or "").strip()
            if not raw_name or not raw_val: continue
            canon = canon_param_name(raw_name)
            if not canon: continue
            val = normalize_value_by_key(canon, raw_val)
            if not val: continue
            # фильтр Совместимость: только для расходников
            if canon=="Совместимость" and kind!="consumable":
                continue
            # тип/вид: укоротить «МФУ Офис» -> «МФУ»
            if canon=="Тип":
                val = re.sub(r"\s+Офис\b","", val, flags=re.I).strip()
            # Цвет печати: если указаны много цветов в «Цвета чернил» — цветная
            if canon=="Цвет печати":
                cols = merged.get("Цвета чернил","")
                if (("ч/б" in val.lower()) and cols and "," in cols):
                    val="цветная"
            # Формат: отбросить пустую нормализацию
            if canon=="Формат" and not re.search(r"[A-Za-z0-9]", val):
                continue
            # сохранить длиннейшее/наиболее информативное
            old = merged.get(canon, "")
            if len(val) > len(old):
                merged[canon] = val

    # порядок вывода
    important_order = [
        "Тип","Назначение","Тип печати","Цвет печати","Формат",
        "Разрешение печати","Скорость печати","Двусторонняя печать",
        "Интерфейсы","Wi-Fi",
        "Подача бумаги","Выходной лоток","Емкость лотка",
        "Оптическое разрешение","Тип датчика","Тип сканирования","Макс. формат","Скорость сканирования","Подсветка",
        "Дисплей","Цвета чернил",
        "Яркость","Контрастность","Источник света","Ресурс источника","Входы","Коррекция трапеции","Поддержка 3D",
        "Мощность","Стабилизация AVR","Стабилизация","Розетки",
        "Страна происхождения","Гарантия","Совместимость","Комплектация",
    ]
    order_idx={k:i for i,k in enumerate(important_order)}
    return [(k, merged[k]) for k in sorted(merged.keys(), key=lambda x: order_idx.get(x, 999))]

def render_specs_html(specs: List[Tuple[str,str]]) -> str:
    if not specs: return ""
    out=["<h3>Характеристики</h3>","<ul>"]
    for k,v in specs:
        if k=="Комплектация":  # комплект поставки показываем отдельным блоком ниже
            continue
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

# ===================== KEYWORDS =====================
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
    raw_tokens=tokenize(name or "")
    modelish=[t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content=[t for t in raw_tokens if (t.lower() not in {"для","и","или","на","в","из","от","по","с","к","до","при","через","над","под","о","об","у","без","про","как","это","тип","модель","комплект","формат","новый","новинка","оригинальный"}) and (any(ch.isdigit() for ch in t) or "-" in t or len(t)>=3)]
    bigr=[]
    for i in range(len(content)-1):
        a,b=content[i],content[i+1]
        bigr.append(f"{a} {b}")
    base = ([vendor] if vendor else []) + list(set([t.upper() for t in modelish[:8]])) + bigr[:8] + [t.capitalize() if not re.search(r"[A-Z]{2,}",t) else t for t in content[:10]]

    # латиница для русских слов
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

# ===================== VENDORCODE / PLACEHOLDERS =====================
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

# ===================== DESCRIPTION REBUILD (без вытягивания характеристик) =====================
def rebuild_descriptions_from_supplier_only(out_shop: ET.Element) -> int:
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    changed=0
    for offer in off_el.findall("offer"):
        name=get_text(offer,"name")
        d=offer.find("description")
        supplier_raw=inner_html(d)
        supplier_html = sanitize_supplier_html(supplier_raw)
        supplier_html = postprocess_supplier_html(supplier_html, name)

        # Собираем «Характеристики» ТОЛЬКО из Param
        specs = collect_params_canonical(offer)
        bundle_html = render_bundle_html(specs)
        specs_html  = render_specs_html(specs)

        parts=[f"<h3>{_html_escape_in_cdata_safe(name)}</h3>"]
        if supplier_html:
            parts.append(supplier_html)
        if bundle_html: parts.append(bundle_html)
        if specs_html:  parts.append(specs_html)

        full_html = "\n".join([p for p in parts if p]).strip()
        placeholder=f"[[[HTML]]]{full_html}[[[/HTML]]]"

        if d is None:
            d=ET.SubElement(offer,"description"); d.text=placeholder; changed+=1
        else:
            if (d.text or "") != placeholder:
                d.text=placeholder; changed+=1
    return changed

# ===================== PARAMS: перезапись по канонической схеме =====================
def rewrite_params_canonical(out_shop: ET.Element) -> int:
    """Полностью пересобираем <param name="…">…</param> из канонизированных Param'ов."""
    off_el=out_shop.find("offers")
    if off_el is None: return 0
    total=0
    for offer in off_el.findall("offer"):
        name = get_text(offer,"name")
        kind = classify_kind(name)
        # собрать чистые specs из Param
        specs = collect_params_canonical(offer)
        # зачистить старые param'ы
        for tag in ("param","Param","PARAM"):
            for pn in list(offer.findall(tag)): offer.remove(pn)
        # записать по порядку
        added=0
        for k,v in specs:
            # Совместимость только для расходников
            if k=="Совместимость" and kind!="consumable":
                continue
            if not v: continue
            node=ET.SubElement(offer,"param"); node.set("name", k); node.text=v; added+=1
        total += added
    return total

# ===================== KEYWORDS / VENDORCODE / ORDER HELPERS =====================
def ensure_keywords_all(out_shop: ET.Element) -> int:
    return ensure_keywords(out_shop)

def ensure_vendorcode_with_article_all(out_shop: ET.Element, prefix: str, create_if_missing: bool):
    ensure_vendorcode_with_article(out_shop, prefix, create_if_missing)
    sync_offer_id_with_vendorcode(out_shop)

def _replace_html_placeholders_with_cdata(xml_text: str) -> str:
    def repl(m):
        inner=m.group(1).replace("[[[HTML]]]", "").replace("[[[/HTML]]]", "")
        inner = html.unescape(inner)
        return f"<description><![CDATA[\n{inner}\n]]></description>"
    return re.sub(
        r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>",
        repl, xml_text, flags=re.S
    )

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

    # include/exclude по ключам
    if os.path.exists(AKCENT_KEYWORDS_PATH):
        with open(AKCENT_KEYWORDS_PATH, "r", encoding="utf-8", errors="ignore") as f:
            has_any = any(line.strip() and not line.strip().startswith("#") for line in f.readlines())
        if AKCENT_KEYWORDS_MODE=="include" and not has_any:
            err("AKCENT_KEYWORDS_MODE=include, но файл docs/akcent_keywords.txt пуст или не найден.", 2)

    flag_unrealistic_supplier_prices(out_shop)
    ensure_vendor(out_shop)
    ensure_vendorcode_with_article_all(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AC"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )
    reprice_offers(out_shop, PRICING_RULES)
    ensure_placeholder_pictures(out_shop)

    # ОПИСАНИЕ: только поставщик (очистка) + Характеристики из Param
    desc_changed = rebuild_descriptions_from_supplier_only(out_shop)
    # PARAMS: перезаписываем канонически из Param
    params_written = rewrite_params_canonical(out_shop)

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
    # feed_meta — «как раньше»: компактный блок-комментарий
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

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | {SCRIPT_VERSION} | descriptions_changed={desc_changed} | params_written={params_written}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
