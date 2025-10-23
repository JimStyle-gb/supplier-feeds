#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_akcent.py

Готовый скрипт сборки фида для поставщика AkCent со всеми согласованными моментами:

1) Фильтр товаров — строго по <name> с использованием docs/akcent_keywords.txt (режим include|exclude).
2) FEED_META — «старый» формат в комментарии, время — Asia/Almaty.
3) Цены: берём «дилерскую/закупочную», применяем правила наценок. Нереальные цены >= PRICE_CAP_THRESHOLD → 100 KZT.
4) <vendor> нормализуем/восстанавливаем; <vendorCode> приводим к артикулу (при необходимости) и синхронизируем с offer@id (id = vendorCode с префиксом).
5) Плейсхолдеры для отсутствующих картинок (бренд/категория/дефолт).
6) <currencyId>KZT</currencyId>, атрибут offer@available рассчитывается; чистим складские/служебные поля.
7) <description>:
   - Берём «родное» HTML-описание, санитизируем и приводим к <h3>/<p>/<ul>.
   - Из «родного» текста извлекаем всё, что похоже на характеристики (строки «Ключ: значение», dpi, A4, Wi-Fi/USB/Ethernet, «стр/мин» и т. п.) → в блок «Характеристики». В тексте эти пункты удаляются.
   - Если «Состав поставки/Комплектация» встречается блоком — сохраняем отдельным списком.
   - Блок «Характеристики» дополнительно формируется из тегов <Param> (с канонизацией имён).
   - Значения чистятся от лишних пробелов, в том числе &nbsp;. После </strong> всегда ровно один пробел.
8) <param name="..."> для Сату — формируются из тегов поставщика <Param> (после канонизации/нормализации);
   мусорные имена выкидываются (например, «Сопутствующие товары», «Производитель», «Наименование производителя» и т. п.).
9) <keywords> — пересобираются (модельные токены, би-граммы, транслит, гео) — ограничение 1024 символа.
10) Пустая строка между </offer> … <offer> (для читаемости диффов/глазами).
11) Порядок полей в offer — как раньше: vendorCode, name, price, picture, vendor, currencyId, description, затем прочее.
12) Код аккуратно обрабатывает источники в Windows-1251/UTF-8.

Env-переменные (по желанию):
- SUPPLIER_URL, OUT_FILE, OUTPUT_ENCODING, AKCENT_KEYWORDS_PATH, AKCENT_KEYWORDS_MODE (include|exclude),
  PRICE_CAP_THRESHOLD, PRICE_CAP_VALUE, VENDORCODE_PREFIX, VENDORCODE_CREATE_IF_MISSING.
"""

import os, sys, re, time, random, urllib.parse, requests, html
from copy import deepcopy
from typing import Dict, List, Tuple, Optional
from xml.etree import ElementTree as ET
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

SCRIPT_VERSION = "akcent-2025-10-23.v5.1.0"

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

# === фильтр по <name> ===
AKCENT_KEYWORDS_PATH  = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt")
AKCENT_KEYWORDS_MODE  = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()  # include|exclude

# === ценообразование ===
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

# === плейсхолдеры ===
PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.akcent.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.akcent.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.akcent.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# === прочее ===
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
def err(msg: str, code: int = 1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_almaty() -> datetime:
    try:
        return datetime.now(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(time.time()+5*3600)
    except Exception:
        return datetime.utcfromtimestamp(time.time()+5*3600)

def next_build_time_almaty() -> datetime:
    cur = now_almaty()
    t = cur.replace(hour=1, minute=0, second=0, microsecond=0)
    return t + timedelta(days=1) if cur >= t else t

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

def strip_html_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", " ", s or "", flags=re.S)

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
        try:
            src_p = float((get_text(offer,"price") or "").replace(",",".")) if get_text(offer,"price") else None
        except Exception:
            src_p = None
        if src_p is not None and src_p >= PRICE_CAP_THRESHOLD:
            offer.attrib["_force_price"]=str(PRICE_CAP_VALUE); flagged+=1
    return flagged

TRUE_WORDS={"true","1","yes","y","да","есть","in stock","available","опция","опционально","option","optional"}
FALSE_WORDS={"false","0","no","n","нет","отсутствует","out of stock","unavailable","под заказ","ожидается","на заказ"}
def _parse_bool_str(s: str)->Optional[bool]:
    v=(s or "").strip().lower()
    if v in {"опция","опционально","option","optional"}: return None
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

# ===================== DESCRIPTION / SPECS =====================

MORE_PHRASES_RE = re.compile(
    r"^\s*(подробнее|читать далее|узнать больше|все детали|подробности|смотреть на сайте производителя|скачать инструкцию|click here|learn more)\s*\.?\s*$",
    re.I
)
ALLOWED_TAGS = ("h3","p","ul","ol","li","br","strong","em","b","i")

# автопоправки опечаток (минимальный набор)
TYPO_FIXES = [
    (re.compile(r"высококачетсв", re.I), "высококачеств"),
    (re.compile(r"приентер", re.I), "принтер"),
    (re.compile(r"\bконтенер", re.I), "контейнер"),
]

def maybe_unescape_html(s: str) -> str:
    if not s: return s
    if re.search(r"&lt;/?[a-zA-Z]", s):
        for _ in range(2):
            s = html.unescape(s)
            if not re.search(r"&lt;/?[a-zA-Z]", s): break
    return s

def sanitize_supplier_html(raw_html: str) -> str:
    s = raw_html or ""
    s = maybe_unescape_html(s)
    # убираем опасные/лишние теги
    s = re.sub(r"<(script|style|iframe|object|embed|noscript)[^>]*>.*?</\1>", " ", s, flags=re.I|re.S)
    s = re.sub(r"</?(table|thead|tbody|tr|td|th|img)[^>]*>", " ", s, flags=re.I|re.S)
    # ссылки → убираем теги <a>, но оставляем текст
    s = re.sub(r"<a\b[^>]*>", "", s, flags=re.I); s = re.sub(r"</a>", "", s, flags=re.I)
    # приводим заголовки к h3
    s = re.sub(r"<h[1-6]\b[^>]*>", "<h3>", s, flags=re.I); s = re.sub(r"</h[1-6]>", "</h3>", s, flags=re.I)
    # div -> p, чистим span и прочее
    s = re.sub(r"<div\b[^>]*>", "<p>", s, flags=re.I); s = re.sub(r"</div>", "</p>", s, flags=re.I)
    s = re.sub(r"</?span\b[^>]*>", "", s, flags=re.I)
    # брейки -> нормальные абзацы
    s = re.sub(r"(?:\s*<br\s*/?>\s*){2,}", "</p><p>", s, flags=re.I)
    # убираем инлайновые стили/классы
    s = re.sub(r"\sstyle\s*=\s*(['\"]).*?\1", "", s, flags=re.I)
    s = re.sub(r"\s(class|id|align|width|height)\s*=\s*(['\"]).*?\2", "", s, flags=re.I)
    # оставить только разрешённые теги
    s = re.sub(r"</?(?!"+("|".join(ALLOWED_TAGS))+r")\w+[^>]*>", " ", s, flags=re.I)
    # пустые блоки
    s = re.sub(r"<(p|li|ul|ol)>\s*</\1>", "", s, flags=re.I)
    # нормализуем x -> ×, пробелы
    s = re.sub(r"(?<=\d)\s*[xX]\s*(?=\d)", "×", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    for pat,rep in TYPO_FIXES:
        s = pat.sub(rep, s)
    return s

def wrap_bare_text_lines_to_paragraphs(html_in: str) -> str:
    if not html_in.strip(): return html_in
    lines = html_in.splitlines()
    out=[]; buf=[]
    def flush_buf():
        if not buf: return
        text = " ".join([x.strip() for x in buf if x.strip()])
        if text: out.append(f"<p>{_html_escape_in_cdata_safe(text)}</p>")
        buf.clear()
    for ln in lines:
        s=ln.strip()
        if not s: flush_buf(); continue
        if s.startswith("<"): flush_buf(); out.append(ln)
        else: buf.append(s)
    flush_buf()
    res="\n".join(out)
    if not re.search(r"<(p|ul|ol|h3)\b", res, flags=re.I):
        txt=_html_escape_in_cdata_safe(" ".join(x.strip() for x in lines if x.strip()))
        res=f"<p>{txt}</p>"
    return res

def remove_urls_and_cta(html_in: str) -> str:
    s = re.sub(r"\bhttps?://[^\s<]+", "", html_in, flags=re.I)
    s = re.sub(r"\bwww\.[^\s<]+", "", s, flags=re.I)
    def drop_cta_p(m: re.Match) -> str:
        inner = m.group(1)
        if MORE_PHRASES_RE.match(inner): return ""
        return m.group(0)
    return re.sub(r"<p>(.*?)</p>", drop_cta_p, s, flags=re.I|re.S)

def strip_name_repeats(html_in: str, product_name: str) -> str:
    if not product_name: return html_in
    pat = re.compile(rf"^\s*{re.escape(product_name)}\s*[—–\-:,]*\s*", re.I)
    def cut(m: re.Match) -> str:
        inner = m.group(1); cleaned = pat.sub("", inner).strip()
        return f"<p>{cleaned}</p>" if cleaned else ""
    return re.sub(r"<p>(.*?)</p>", cut, html_in, flags=re.I|re.S)

# --- ЕДИНАЯ ЧИСТКА ПРОБЕЛОВ ---
def _clean_ws(val: str) -> str:
    return re.sub(r"\s+", " ", (val or "").replace("\u00A0", " ")).strip(" \t\r\n,;:.-")

def _strip_tags_keep_breaks(s: str) -> str:
    t = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    t = re.sub(r"</(p|li)>", "\n", t, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t, flags=re.S)
    t = re.sub(r"\s+\n", "\n", t)
    t = re.sub(r"\n{2,}", "\n", t)
    t = re.sub(r"\s{2,}", " ", t)
    return t.strip()

def _is_spec_sentence(s: str) -> bool:
    s_low = s.lower()
    if re.search(r"\b(dpi|стр\.?/?\s*мин|ppm|wi-?fi|ethernet|usb|bluetooth|rj-?45|hdmi|display\s*port|displayport|mm|мм|см|g/\s?м2|г/\s?м2)\b", s_low):
        return True
    if re.search(r"\b(a[0-6]\b|letter|legal)\b", s_low):
        return True
    if re.search(r"\b(двусторонн\w*|auto\s*duplex|duplex)\b", s_low):
        return True
    return False

# канонизация имён для характеристик
CANON_MAP = {
    "тип":"Тип",
    "тип печати":"Тип печати",
    "цвет печати":"Цвет печати",
    "формат":"Формат",
    "разрешение":"Разрешение печати",
    "разрешение печати":"Разрешение печати",
    "оптическое разрешение":"Оптическое разрешение",
    "скорость печати":"Скорость печати",
    "интерфейсы":"Интерфейсы",
    "интерфейс":"Интерфейсы",
    "wi-fi":"Wi-Fi",
    "двусторонняя печать":"Двусторонняя печать",
    "дисплей":"Дисплей",
    "цвета чернил":"Цвета чернил",
    "тип чернил":"Тип чернил",
    "страна происхождения":"Страна происхождения",
    "гарантия":"Гарантия",
    "автоподатчик":"Автоподатчик",
    "подача бумаги":"Подача бумаги",
    "совместимость":"Совместимость",
    "совместимые продукты":"Совместимость",
    "совместимые модели":"Совместимость",
}
def canon_key(k: str) -> Optional[str]:
    key = k.strip().lower().replace("ё","е")
    return CANON_MAP.get(key)

def normalize_value_spec(key: str, val: str) -> str:
    v = _clean_ws(val)

    if key == "Скорость печати":
        m = re.search(r"(\d{1,3})", v)
        if m:
            return f"до {m.group(1)} стр/мин"

    if key in ("Разрешение печати","Оптическое разрешение"):
        vv = v.replace("x","×").replace("X","×")
        vv = re.sub(r"(?<=\d)[\.,](?=\d{3}\b)", "", vv)
        if not re.search(r"\bdpi\b", vv, re.I):
            vv += " dpi"
        return _clean_ws(vv)

    if key == "Интерфейсы":
        vv = re.sub(r"[\*/;|]+", ", ", v)
        vv = re.sub(r"\s*,\s*", ", ", vv)
        repl = {"wifi":"Wi-Fi","wi fi":"Wi-Fi","wi-fi":"Wi-Fi","ethernet":"Ethernet",
                "usb-host":"USB-host","usb host":"USB-host","bluetooth":"Bluetooth","rj-45":"RJ-45"}
        low = vv.lower()
        for k,w in repl.items():
            low = re.sub(rf"\b{k}\b", w.lower(), low)
        tokens = [t.strip() for t in low.split(",") if t.strip()]
        tokens = [t.capitalize() if t not in ("wi-fi","usb-host","rj-45") else t for t in tokens]
        norm=[]
        for t in tokens:
            t=t.replace("wi-fi","Wi-Fi").replace("usb-host","USB-host").replace("rj-45","RJ-45")
            if t not in norm: norm.append(t)
        return _clean_ws(", ".join(norm))

    if key in ("Wi-Fi","Двусторонняя печать","Автоподатчик","Дисплей"):
        low = v.lower()
        if re.search(r"^(да|yes|true|есть)$", low):  return "Да"
        if re.search(r"^(нет|no|false|отсутствует)$", low):  return "Нет"
        return _clean_ws(v)

    if key == "Формат":
        t = v.replace("бумаги","")
        t = re.sub(r"\s*[xX]\s*", "×", t)
        parts = [p.strip() for p in re.split(r"[;,]+", t) if p.strip()]
        keep = []
        for p in parts:
            if re.search(r"^(A\d|B\d|C6|DL|Letter|Legal|No\.?\s*10|\d{1,2}\s*×\s*\d{1,2}|16:9)$", p, re.I):
                keep.append(re.sub(r"\s*×\s*","×", p))
        if keep:
            if len(keep) > 10:
                keep = keep[:10] + ["и др."]
            return _clean_ws(", ".join(keep))

    return _clean_ws(v)

def parse_existing_specs_block(html_frag: str):
    specs = {}
    m = re.search(r"(?is)(<h3>\s*Характеристики\s*</h3>\s*<ul>(.*?)</ul>)", html_frag)
    if not m:
        return specs, html_frag
    ul_html = m.group(2)
    for li_m in re.finditer(r"(?is)<li>\s*<strong>([^:<]+):\s*</strong>\s*(.*?)\s*</li>", ul_html):
        k = li_m.group(1).strip()
        v = li_m.group(2).strip()
        if k and v:
            specs[k] = v
    cleaned = html_frag[:m.start()] + html_frag[m.end():]
    return specs, cleaned

def extract_bundle_block(html_frag: str):
    m = re.search(r"(?is)(<h3>\s*(Состав\s+поставки|Комплектация)\s*</h3>\s*<ul>.*?</ul>)", html_frag)
    if not m:
        return "", html_frag
    bundle_html = m.group(1)
    cleaned = html_frag[:m.start()] + html_frag[m.end():]
    return bundle_html, cleaned

def extract_specs_from_text_block(html_frag: str):
    extracted = {}

    def process_li_list(ul_html):
        nonlocal extracted
        lis = re.findall(r"(?is)<li>(.*?)</li>", ul_html)
        keep_items = []
        for item in lis:
            plain = _strip_tags_keep_breaks(item)
            took = False
            m = re.match(r"\s*([A-Za-zА-Яа-яЁё\-\s\.]+)\s*:\s*(.+)$", plain)
            if m:
                k_raw = m.group(1).strip()
                v_raw = m.group(2).strip()
                ck = canon_key(k_raw)
                if ck:
                    extracted.setdefault(ck, normalize_value_spec(ck, v_raw)); took = True
            if not took and _is_spec_sentence(plain):
                if re.search(r"\b(dpi)\b", plain, re.I):
                    k = "Оптическое разрешение" if re.search(r"скан", plain, re.I) else "Разрешение печати"
                    v = re.sub(r".*?(\d{2,5}\s*[x×]\s*\d{2,5}.*?dpi).*", r"\1", plain, flags=re.I)
                    extracted.setdefault(k, normalize_value_spec(k, v or plain)); took = True
                elif re.search(r"стр\.?/?\s*мин|ppm", plain, re.I):
                    k="Скорость печати"; m2=re.search(r"(\d{1,3})", plain)
                    v=f"до {m2.group(1)} стр/мин" if m2 else plain
                    extracted.setdefault(k, v); took = True
                elif re.search(r"\b(a[0-6]\b|letter|legal|\d+\s*[x×]\s*\d+)\b", plain, re.I):
                    k="Формат"; extracted.setdefault(k, normalize_value_spec(k, plain)); took=True
                elif re.search(r"\b(wi-?fi|ethernet|usb|bluetooth|rj-?45|hdmi|display\s*port)\b", plain, re.I):
                    k="Интерфейсы"; extracted.setdefault(k, normalize_value_spec(k, plain)); took=True
            if not took: keep_items.append(item)
        if keep_items:
            return "<ul>\n" + "\n".join(f"  <li>{it}</li>" for it in keep_items) + "\n</ul>"
        return ""

    def process_paragraph(p_html):
        nonlocal extracted
        txt = _strip_tags_keep_breaks(p_html)
        sents = re.split(r"(?<=[\.\!\?])\s+", txt)
        keep = []
        for s in sents:
            s0=s.strip()
            if not s0: continue
            took=False
            m = re.match(r"\s*([A-Za-zА-Яа-яЁё\-\s\.]+)\s*:\s*(.+)$", s0)
            if m:
                k_raw = m.group(1).strip(); v_raw = m.group(2).strip()
                ck = canon_key(k_raw)
                if ck: extracted.setdefault(ck, normalize_value_spec(ck, v_raw)); took=True
            if not took and _is_spec_sentence(s0):
                if re.search(r"\b(dpi)\b", s0, re.I):
                    k = "Оптическое разрешение" if re.search(r"скан", s0, re.I) else "Разрешение печати"
                    v = re.sub(r".*?(\d{2,5}\s*[x×]\s*\d{2,5}.*?dpi).*", r"\1", s0, flags=re.I)
                    extracted.setdefault(k, normalize_value_spec(k, v or s0)); took=True
                elif re.search(r"стр\.?/?\s*мин|ppm", s0, re.I):
                    k="Скорость печати"; m2=re.search(r"(\d{1,3})", s0)
                    v=f"до {m2.group(1)} стр/мин" if m2 else s0
                    extracted.setdefault(k, v); took=True
                elif re.search(r"\b(a[0-6]\b|letter|legal|\d+\s*[x×]\s*\d+)\b", s0, re.I):
                    k="Формат"; extracted.setdefault(k, normalize_value_spec(k, s0)); took=True
                elif re.search(r"\b(wi-?fi|ethernet|usb|bluetooth|rj-?45|hdmi|display\s*port)\b", s0, re.I):
                    k="Интерфейсы"; extracted.setdefault(k, normalize_value_spec(k, s0)); took=True
            if not took: keep.append(s0)
        out_txt = " ".join(keep).strip()
        return f"<p>{out_txt}</p>" if out_txt else ""

    cleaned = re.sub(r"(?is)<h3>\s*(Техническ[^<]*|Характеристики|Основные характеристики|Спецификации)\s*</h3>", "", html_frag)
    cleaned = re.sub(r"(?is)<ul>.*?</ul>", lambda m: process_li_list(m.group(0)), cleaned)
    cleaned = re.sub(r"(?is)<p>.*?</p>",  lambda m: process_paragraph(m.group(0)), cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return extracted, cleaned

def render_specs_dict(specs: Dict[str,str]) -> str:
    if not specs: return ""
    order = [
        "Тип","Назначение","Тип печати","Цвет печати","Формат",
        "Разрешение","Разрешение печати","Оптическое разрешение","Скорость печати",
        "Интерфейсы","Wi-Fi","Двусторонняя печать","Подача бумаги","Дисплей",
        "Цвета чернил","Тип чернил","Страна происхождения","Гарантия","Совместимость","Автоподатчик",
    ]
    key_order = {k:i for i,k in enumerate(order)}
    items = sorted(specs.items(), key=lambda kv: key_order.get(kv[0], 999))
    lines = ["<h3>Характеристики</h3>","<ul>"]
    for k,v in items:
        v = _clean_ws(normalize_value_spec(k, v))
        if not v:
            continue
        lines.append(f'  <li><strong>{_html_escape_in_cdata_safe(k)}:</strong> {_html_escape_in_cdata_safe(v)}</li>')
    lines.append("</ul>")
    return "\n".join(lines)

def convert_paragraphs_to_bullets(html_in: str) -> str:
    def bullets_from_short_lines(p_text: str) -> Optional[str]:
        t = re.sub(r"<br\s*/?>", "\n", p_text, flags=re.I)
        t = re.sub(r"<[^>]+>", "", t)
        lines=[re.sub(r"^\s*([•\-\–\—\*]|\d+\)|\d+\.)\s*", "", ln).strip() for ln in t.split("\n")]
        lines=[ln for ln in lines if ln]
        if len(lines) < 3 or len(lines) > 12: return None
        def ok(ln:str)->bool: return 3 <= len(ln) <= 160 and ln.count(".") <= 1
        cand=[ln for ln in lines if ok(ln)]
        if len(cand) < 3: return None
        out=["<ul>"] + [f"  <li>{_html_escape_in_cdata_safe(ln)}</li>" for ln in cand[:6]] + ["</ul>"]
        return "\n".join(out)
    def repl(m: re.Match) -> str:
        inner=m.group(1)
        ul=bullets_from_short_lines(inner)
        return ul if ul else m.group(0)
    return re.sub(r"(?is)<p>(.*?)</p>", repl, html_in)

def truncate_first_paragraph(html_in: str, limit_chars: int = 700) -> str:
    first_p = re.search(r"(?is)<p>(.*?)</p>", html_in)
    if not first_p: return html_in
    inner=first_p.group(1)
    if re.search(r"<ul>|<ol>", html_in, flags=re.I): return html_in
    txt = re.sub(r"<[^>]+>", "", inner).strip()
    if len(txt) <= limit_chars: return html_in
    sents = re.split(r"(?<=[\.\!\?])\s+", txt)
    acc=[]; total=0
    for s in sents:
        if not s: continue
        add = (("" if not acc else " ") + s)
        if total + len(add) > limit_chars and len(acc) >= 2: break
        acc.append(s); total += len(add)
        if len(acc) >= 4: break
    short=" ".join(acc).strip()
    short_html=f"<p>{_html_escape_in_cdata_safe(short)}</p>"
    start, end = first_p.span()
    return html_in[:start] + short_html + html_in[end:]

def compact_html_whitespace(s: str) -> str:
    s=re.sub(r"<(p|li|ul|ol)>\s*</\1>", "", s, flags=re.I)
    s=re.sub(r"</p>\s*<p>", "</p>\n<p>", s, flags=re.I)
    s=re.sub(r"\n{3,}", "\n\n", s)
    s=re.sub(r"\s{2,}", " ", s)
    return s.strip()

def parse_existing_bundle_from_specs(specs: List[Tuple[str,str]]) -> str:
    for k,v in specs:
        if k=="Комплектация" and v:
            items=[it.strip() for it in re.split(r"[;,\n]+", v) if it.strip()]
            if not items: return ""
            out=["<h3>Состав поставки</h3>","<ul>"]
            for it in items[:12]: out.append(f"  <li>{_html_escape_in_cdata_safe(it)}</li>")
            out.append("</ul>")
            return "\n".join(out)
    return ""

# ===================== PARAM → ХАРАКТЕРИСТИКИ/ПАРАМЫ =====================

DISALLOWED_PARAM_NAMES = {
    "производитель","для бренда","наименование производителя","сопутствующие товары",
    "бренд","brand","manufacturer","vendor","поставщик","партномер","артикул поставщика","код на складе",
}
CANON_NAME_MAP_BASE = {
    "тип":"Тип","вид":"Тип","тип печати":"Тип печати",
    "цвет печати":"Цвет печати","цветность":"Цвет печати",
    "формат":"Формат","формат бумаги":"Формат","бумаги":"Формат",
    "разрешение печати":"Разрешение печати","разрешение":"Разрешение печати",
    "оптическое разрешение":"Оптическое разрешение","разрешение сканера":"Оптическое разрешение","разрешение сканера,dpi":"Оптическое разрешение",
    "скорость печати":"Скорость печати","максимальная скорость печати а4 стр/мин":"Скорость печати",
    "двусторонняя печать":"Двусторонняя печать",
    "интерфейс":"Интерфейсы","интерфейсы":"Интерфейсы","входной интерфейс":"Интерфейсы","входы":"Интерфейсы","подключение":"Интерфейсы",
    "wi-fi":"Wi-Fi",
    "подача бумаги":"Подача бумаги","количество бумажных ящиков":"Подача бумаги",
    "выход лоток":"Выходной лоток","емкость лотка":"Емкость лотка",
    "дисплей":"Дисплей","жк дисплей":"Дисплей",
    "цвета чернил":"Цвета чернил","цвета":"Цвета чернил","colors":"Цвета чернил","colours":"Цвета чернил",
    "страна происхождения":"Страна происхождения","гарантия":"Гарантия",
    "совместимые продукты":"Совместимость","совместимость":"Совместимость","совместимые модели":"Совместимость","для моделей":"Совместимость",
    "тип чернил":"Тип чернил","ресурс":"Ресурс","объем":"Объем","объём":"Объем",
    "автоподатчик":"Автоподатчик","комплектация":"Комплектация","состав поставки":"Комплектация","в комплекте":"Комплектация",
}

ALLOWED_PARAM_CANON = {
    "Тип","Назначение","Тип печати","Цвет печати","Формат",
    "Разрешение печати","Разрешение","Скорость печати","Двусторонняя печать","Интерфейсы","Wi-Fi",
    "Подача бумаги","Выходной лоток","Емкость лотка","Дисплей",
    "Страна происхождения","Гарантия",
    "Оптическое разрешение","Тип датчика","Тип сканирования","Макс. формат","Подсветка","Скорость сканирования",
    "Цвета чернил","Тип чернил","Ресурс","Объем","Совместимость","Комплектация","Автоподатчик",
}

def canon_param_name(name: str, kind: str) -> Optional[str]:
    if not name: return None
    key = name.strip().lower().replace("ё","е")
    if key in DISALLOWED_PARAM_NAMES: return None
    if key == "разрешение":
        return "Разрешение" if kind in {"projector","panel","monitor","interactive"} else "Разрешение печати"
    if key in CANON_NAME_MAP_BASE: return CANON_NAME_MAP_BASE[key]
    title = name.strip()
    title_cap = title[:1].upper()+title[1:].lower()
    if title_cap in ALLOWED_PARAM_CANON: return title_cap
    if title in ALLOWED_PARAM_CANON: return title
    return None

def _norm_resolution_print(s: str) -> str:
    t=s.replace("\u00A0"," ").replace("x","×").replace("X","×")
    t=re.sub(r"(?<=\d)[\.,](?=\d{3}\b)","", t)
    m=re.search(r"(\d{2,5})\s*×\s*(\d{2,5})", t)
    return (f"{m.group(1)}×{m.group(2)} dpi" if m else _clean_ws(s))

def _norm_resolution_display(s: str) -> str:
    t=s.replace("\u00A0"," ").replace("x","×").replace("X","×")
    t=re.sub(r"\s*dpi\b","", t, flags=re.I)
    m=re.search(r"(\d{3,5})\s*×\s*(\d{3,5})", t)
    return (f"{m.group(1)}×{m.group(2)}" if m else _clean_ws(t))

def _norm_interfaces(s: str) -> str:
    return normalize_value_spec("Интерфейсы", s)

def _norm_yesno(s: str) -> str:
    low=s.strip().lower()
    if re.search(r"^(да|yes|y|true|есть)$", low): return "Да"
    if re.search(r"^(нет|no|n|false|отсутствует)$", low): return "Нет"
    if re.search(r"^(опци(я|онально)|option(al)?)$", low): return "Опционально"
    return _clean_ws(s)

def _norm_display_val(s: str) -> str:
    t=s.replace(",", ".")
    m=re.search(r"(\d{1,2}(\.\d)?)", t)
    if m: return f"{m.group(1)} см"
    yn=_norm_yesno(s)
    return "есть" if yn=="Да" else ("нет" if yn=="Нет" else _clean_ws(s))

def _norm_speed(s: str) -> str:
    m=re.search(r"(\d{1,3})\s*(стр|pages)\s*/?\s*мин", s, re.I)
    if m: return f"до {m.group(1)} стр/мин"
    m2=re.search(r"^(\d{1,3})$", s.strip())
    return f"до {m2.group(1)} стр/мин" if m2 else _clean_ws(s)

_ALLOWED_FORMAT_TOKEN = re.compile(
    r"^(A\d|B\d|C6|DL|Letter|Legal|No\.?\s*10|\d{1,2}\s*[×x]\s*\d{1,2}|16:9|10\s*×\s*15|13\s*×\s*18|9\s*×\s*13)$",
    re.I
)
def _norm_format(s: str) -> str:
    t = s.replace("бумаги", "")
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
    return _clean_ws(", ".join(keep))

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
    return _clean_ws(", ".join(out))

def _interpret_print_color(val: str) -> str:
    s=val.strip().lower()
    if re.search(r"(ч/б|монохром|1\s*цвет|один\s*цвет|black\s*only)", s): return "монохромная"
    m=re.search(r"(\d+)\s*цвет", s)
    if m and int(m.group(1))>=3: return "цветная"
    if re.search(r"(cm+y|cyan|magenta|yellow)", s): return "цветная"
    return val.strip()

def clean_param_value(key: str, value: str) -> str:
    if not value: return ""
    s = re.sub(r"https?://\S+|www\.\S+","", (value or ""), flags=re.I)
    s = _clean_ws(s)
    if len(s) > PARAMS_MAX_VALUE_LEN:
        s = s[:PARAMS_MAX_VALUE_LEN-1] + "…"
    return s

def normalize_value_by_key(k: str, v: str, kind: str) -> str:
    key=k.lower(); s=v.strip()
    if key=="разрешение печати":       return _norm_resolution_print(s)
    if key=="разрешение":              return _norm_resolution_display(s)
    if key in ("интерфейсы","входы"):  return _norm_interfaces(s)
    if key=="wi-fi":                    return _norm_yesno(s)
    if key=="формат":                   return _norm_format(s)
    if key=="дисплей":                  return _norm_display_val(s)
    if key=="скорость печати":          return _norm_speed(s)
    if key=="цвета чернил":             return _norm_colors_list(s)
    if key=="двусторонняя печать":      return _norm_yesno(s)
    if key=="оптическое разрешение":    return _norm_resolution_print(s)
    if key=="цвет печати":              return _interpret_print_color(s)
    return clean_param_value(k, s)

def classify_kind(name: str) -> str:
    n=(name or "").lower()
    if any(k in n for k in ["картридж","чернила","ёмкость для отработанных","емкость для отработанных","maintenance box","тонер","ribbon","фотобарабан","drum","пленка для ламинирования","плёнка для ламинирования"]):
        return "consumable"
    if any(k in n for k in ["кабель","шнур","адаптер","лоток","крышка","держатель","подставка","брекет","лампа"]):
        return "accessory"
    if "проектор" in n or "projector" in n: return "projector"
    if "интерактивная панел" in n: return "panel"
    if "интерактивный дисплей" in n or "интерактивная доска" in n: return "interactive"
    if "монитор" in n: return "monitor"
    return "device"

def _split_type_and_usage(val: str) -> Tuple[str, Optional[str]]:
    s=val.strip()
    tokens=re.split(r"[\s,/]+", s)
    usage=None
    for t in tokens[:]:
        low=t.lower()
        if low in {"дом","домашний","домашнее"}:
            usage="Дом"; tokens.remove(t)
        elif low in {"офис","офисный","офисное"}:
            usage="Офис"; tokens.remove(t)
    base=" ".join(tokens).strip()
    return (base or val.strip(), usage)

def collect_params_canonical(offer: ET.Element) -> List[Tuple[str,str]]:
    name = get_text(offer,"name")
    kind = classify_kind(name)
    merged: Dict[str,str] = {}
    for tag in ("Param","param","PARAM"):
        for pn in offer.findall(tag):
            raw_name = (pn.get("name") or pn.get("Name") or "").strip()
            raw_val  = (pn.text or "").strip()
            if not raw_name or not raw_val: continue
            canon = canon_param_name(raw_name, kind)
            if not canon: continue
            if canon=="Разрешение печати" and kind in {"projector","panel","monitor","interactive"}:
                canon="Разрешение"
            val = normalize_value_by_key(canon, raw_val, kind)
            if not val: continue
            if canon=="Совместимость" and kind not in {"consumable","accessory"}:
                continue
            if canon=="Тип":
                base, usage = _split_type_and_usage(val)
                base = re.sub(r"\s+Офис\b","", base, flags=re.I).strip()
                val = base or val
                if usage and "Назначение" not in merged:
                    merged["Назначение"] = usage
            if canon=="Цвет печати":
                val = _interpret_print_color(val)
            if canon=="Формат" and not re.search(r"[A-Za-z0-9]", val):
                continue
            old = merged.get(canon, "")
            if len(val) > len(old):
                merged[canon] = val

    for bkey in ("Двусторонняя печать","Wi-Fi","Автоподатчик"):
        if bkey in merged:
            merged[bkey] = _norm_yesno(merged[bkey])

    important_order = [
        "Тип","Назначение","Тип печати","Цвет печати","Формат",
        "Разрешение","Разрешение печати","Скорость печати","Двусторонняя печать",
        "Интерфейсы","Wi-Fi","Подача бумаги","Выходной лоток","Емкость лотка",
        "Оптическое разрешение","Тип датчика","Тип сканирования","Макс. формат","Скорость сканирования","Подсветка",
        "Дисплей","Цвета чернил",
        "Страна происхождения","Гарантия","Совместимость","Комплектация","Автоподатчик",
    ]
    order_idx={k:i for i,k in enumerate(important_order)}
    return [(k, merged[k]) for k in sorted(merged.keys(), key=lambda x: order_idx.get(x, 999))]

def render_specs_html(specs: List[Tuple[str,str]]) -> str:
    if not specs: return ""
    out=["<h3>Характеристики</h3>","<ul>"]
    for k,v in specs:
        v = _clean_ws(normalize_value_spec(k, v))
        if not v: continue
        out.append(f'  <li><strong>{_html_escape_in_cdata_safe(k)}:</strong> {_html_escape_in_cdata_safe(v)}</li>')
    out.append("</ul>")
    return "\n".join(out)

def render_bundle_html_from_specs(specs: List[Tuple[str,str]]) -> str:
    return parse_existing_bundle_from_specs(specs)

def improve_supplier_description_and_extract_specs(supplier_html: str, product_name: str) -> Tuple[str, Dict[str,str], str]:
    s = supplier_html or ""
    s = remove_urls_and_cta(s)
    s = strip_name_repeats(s, product_name)
    s = wrap_bare_text_lines_to_paragraphs(s)
    existing_specs, rest = parse_existing_specs_block(s)
    bundle_html, rest2 = extract_bundle_block(rest)
    extracted_specs, marketing_clean = extract_specs_from_text_block(rest2)
    marketing_clean = convert_paragraphs_to_bullets(marketing_clean)
    marketing_clean = truncate_first_paragraph(marketing_clean, limit_chars=700)
    marketing_clean = compact_html_whitespace(marketing_clean)
    specs = dict(existing_specs)
    for k,v in extracted_specs.items():
        if k not in specs or (len(v) > len(specs[k])): specs[k] = v
    return marketing_clean, specs, bundle_html

# ===================== KEYWORDS =====================
def build_keywords_for_offer(offer: ET.Element) -> str:
    WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
    def tokenize(s: str) -> List[str]: return WORD_RE.findall(s or "")
    def translit_ru_to_lat(s: str) -> str:
        table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"p","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
        out=s.lower().translate(table); out=re.sub(r"[^a-z0-9\- ]+","", out); return re.sub(r"\s+","-", out).strip("-")

    name=get_text(offer,"name"); vendor=get_text(offer,"vendor").strip()
    base=[vendor] if vendor else []
    raw_tokens=tokenize(name or "")
    modelish=[t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content=[t for t in raw_tokens if (t.lower() not in {"для","и","или","на","в","из","от","по","с","к","до","при","через","над","под","о","об","у","без","про","как","это","тип","модель","комплект","формат","новый","новинка","оригинальный"}) and (any(ch.isdigit() for ch in t) or "-" in t or len(t)>=3)]
    bigr=[]
    for i in range(len(content)-1):
        a,b=content[i],content[i+1]
        bigr.append(f"{a} {b}")
    base += list(set([t.upper() for t in modelish[:8]])) + bigr[:8] + [t.capitalize() if not re.search(r"[A-Z]{2,}",t) else t for t in content[:10]]
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
    parts=[]; seen=set()
    for p in base:
        if not p: continue
        k=p.lower()
        if k in seen: continue
        seen.add(k); parts.append(p)
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

# ===================== FEED_META =====================
def render_feed_meta_comment(pairs:Dict[str,str]) -> str:
    rows=[
        ("Поставщик", pairs.get("supplier","")),
        ("URL поставщика", pairs.get("source","")),
        ("Время сборки (Алматы)", pairs.get("built_alm","")),
        ("Ближайшее время сборки (Алматы)", pairs.get("next_build_alm","")),
        ("Последнее обновление SEO-блока", pairs.get("seo_last_update_alm","")),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total","0"))),
        ("Сколько товаров у поставщика после фильтра", str(pairs.get("offers_written","0"))),
        ("Сколько товаров есть в наличии (true)", str(pairs.get("available_true","0"))),
        ("Сколько товаров нет в наличии (false)", str(pairs.get("available_false","0"))),
    ]
    key_w=max(len(k) for k,_ in rows)
    lines=["FEED_META"] + [f"{k.ljust(key_w)} | {v}" for k,v in rows]
    return "\n".join(lines)

# ===================== Helpers: XML пост-обработка =====================
def _replace_html_placeholders_with_cdata(xml_text: str) -> str:
    def repl(m):
        inner=m.group(1).replace("[[[HTML]]]", "").replace("[[[/HTML]]]", "")
        inner = html.unescape(inner)
        return f"<description><![CDATA[\n{inner}\n]]></description>"
    return re.sub(
        r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>",
        repl, xml_text, flags=re.S
    )

# ===================== NAME NORMALIZATION =====================
def normalize_name_text(s: str) -> str:
    if not s: return s
    t=s.replace("\u00A0"," ")
    t=re.sub(r"\s{2,}"," ", t)
    t=re.sub(r"(?<=\d)\s*[xX]\s*(?=\d)", "×", t)
    t=re.sub(r"\b(Wi)[\s\-]?Fi\b", "Wi-Fi", t, flags=re.I)
    return t.strip()

# ===================== MAIN =====================
def main()->None:
    log("Run set -e                       # прерывать шаг при любой ошибке")
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
    out_shop=ET.SubElement(out_root,"shop"); offers_el=ET.SubElement(out_shop,"offers")

    # копируем офферы как есть
    for o in src_offers:
        mod=deepcopy(o)
        if DROP_CATEGORY_ID_TAG:
            for node in list(mod.findall("categoryId"))+list(mod.findall("CategoryId")): mod.remove(node)
        offers_el.append(mod)

    # === ФИЛЬТР ТОЛЬКО ПО <name> ===
    keys=load_name_filter(AKCENT_KEYWORDS_PATH)
    if AKCENT_KEYWORDS_MODE=="include" and len(keys)==0:
        err("AKCENT_KEYWORDS_MODE=include, но файл docs/akcent_keywords.txt пуст или не найден.", 2)
    filtered_out=0
    if (AKCENT_KEYWORDS_MODE in {"include","exclude"}) and len(keys)>0:
        before=len(list(offers_el.findall("offer")))
        hits=0
        for off in list(offers_el.findall("offer")):
            nm=get_text(off,"name")
            hit=name_matches(nm,keys)
            if hit: hits+=1
            drop=(AKCENT_KEYWORDS_MODE=="exclude" and hit) or (AKCENT_KEYWORDS_MODE=="include" and not hit)
            if drop:
                offers_el.remove(off); filtered_out+=1
        kept=before-filtered_out
        log(f"Filter mode: {AKCENT_KEYWORDS_MODE} | Keywords loaded: {len(keys)} | Offers before: {before} | Matched: {hits} | Removed: {filtered_out} | Kept: {kept}")
    else:
        log("Filter disabled: no keys or mode not in {include,exclude}")

    flagged = flag_unrealistic_supplier_prices(out_shop)
    log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")

    v_norm, v_filled, v_removed = ensure_vendor(out_shop)
    log(f"Vendors auto-filled: {v_filled}")

    ensure_vendorcode_with_article(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AC"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )
    sync_offer_id_with_vendorcode(out_shop)

    reprice_offers(out_shop, PRICING_RULES)
    ph_added=ensure_placeholder_pictures(out_shop)
    log(f"Placeholders added: {ph_added}")

    # Нормализация <name>
    for offer in offers_el.findall("offer"):
        nm_el = offer.find("name")
        if nm_el is not None and nm_el.text:
            nm_el.text = normalize_name_text(nm_el.text)

    # === Описание/Характеристики ===
    desc_changed = 0
    for offer in offers_el.findall("offer"):
        name=get_text(offer,"name")
        d=offer.find("description")
        supplier_raw=inner_html(d)

        # 1) Санитизация «родного» описания
        supplier_html = sanitize_supplier_html(supplier_raw)

        # 2) Извлечение характеристик из «родного» текста и удаление их из текста
        marketing_html, specs_from_desc, bundle_from_desc = improve_supplier_description_and_extract_specs(supplier_html, name)

        # 3) Характеристики из PARAM (канонизация)
        specs_from_param = collect_params_canonical(offer)

        # 4) Объединяем: Param (приоритетнее) + из «родного»
        merged_specs: Dict[str,str] = {k:v for k,v in specs_from_desc.items()}
        for k,v in specs_from_param:
            cur = merged_specs.get(k)
            if (cur is None) or (len(v) > len(cur)):
                merged_specs[k] = v

        # 5) Сборка HTML
        parts=[f"<h3>{_html_escape_in_cdata_safe(name)}</h3>"]
        if marketing_html: parts.append(marketing_html)
        # Состав поставки
        bundle_html = bundle_from_desc or render_bundle_html_from_specs(specs_from_param)
        if bundle_html: parts.append(bundle_html)
        specs_html = render_specs_dict(merged_specs)
        if specs_html:  parts.append(specs_html)

        full_html = "\n".join([p for p in parts if p]).strip()
        placeholder=f"[[[HTML]]]{full_html}[[[/HTML]]]"
        if d is None:
            d=ET.SubElement(offer,"description"); d.text=placeholder; desc_changed+=1
        else:
            if (d.text or "") != placeholder:
                d.text=placeholder; desc_changed+=1

    # === Перезаписываем <param> из канонических PARAM для Сату (без мусора) ===
    for offer in offers_el.findall("offer"):
        specs = collect_params_canonical(offer)
        for tag in ("param","Param","PARAM"):
            for pn in list(offer.findall(tag)): offer.remove(pn)
        for k,v in specs:
            if not v: continue
            node=ET.SubElement(offer,"param"); node.set("name", k); node.text=_clean_ws(v)

    t_true, t_false = normalize_available_field(out_shop)
    fix_currency_id(out_shop, default_code="KZT")

    # чистка служебных тегов
    for off in offers_el.findall("offer"):
        for t in PURGE_TAGS_AFTER:
            for node in list(off.findall(t)): off.remove(node)
        for a in PURGE_OFFER_ATTRS_AFTER:
            if a in off.attrib: off.attrib.pop(a,None)

    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)
    kw_touched=ensure_keywords(out_shop)
    log(f"Keywords updated: {kw_touched}")
    log(f"Descriptions rebuilt: {desc_changed}")

    built_alm=now_almaty()
    meta_pairs={
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "offers_total": len(src_offers),
        "offers_written": len(list(offers_el.findall("offer"))),
        "available_true": str(t_true),
        "available_false": str(t_false),
        "built_alm": format_dt_almaty(built_alm),
        "next_build_alm": format_dt_almaty(next_build_time_almaty()),
        "seo_last_update_alm": format_dt_almaty(built_alm),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    try: ET.indent(out_root, space="  ")
    except Exception: pass

    xml_bytes=ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text=xml_bytes.decode(ENC, errors="replace")

    # заменить [[[HTML]]] на CDATA
    xml_text=_replace_html_placeholders_with_cdata(xml_text)
    # пустая строка между офферами и после FEED_META
    xml_text=re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_text)
    xml_text=re.sub(r"(?s)(-->)\s*(<shop\b)", r"\1\n\2", xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    try:
        with open(OUT_FILE_YML,"w",encoding=ENC, newline="\n") as f: f.write(xml_text)
    except UnicodeEncodeError as e:
        warn(f"{ENC} encode issue ({e}); using xmlcharrefreplace fallback)")
        with open(OUT_FILE_YML,"wb") as f: f.write(xml_text.encode(ENC, errors="xmlcharrefreplace"))

    try:
        docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True); open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e: warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | script={SCRIPT_VERSION}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
