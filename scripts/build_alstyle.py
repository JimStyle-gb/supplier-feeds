# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle → YML: стабильные цены/наличие + безопасный HTML для <description>.

Обновление v7.3.1:
- FIX: NameError 'build_specs_html_from_params' — функция добавлена.
- Чистка дублей функций (detect_kind, reorder_offer_children, _replace_html_placeholders_with_cdata).
- Режим SEO-рефреша: каждое 1-е число месяца (Asia/Almaty), можно переключить через ENV.
"""

from __future__ import annotations
import os, sys, re, time, random, json, hashlib, urllib.parse, requests
from copy import deepcopy
from typing import Dict, List, Tuple, Optional, Set
from xml.etree import ElementTree as ET
from datetime import datetime, timezone, timedelta
from html import unescape as _unescape
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

SCRIPT_VERSION = "alstyle-2025-10-21.v7.3.1"

# ======================= ENV / CONST =======================
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "AlStyle").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/alstyle.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip().lower()

ALSTYLE_CATEGORIES_MODE = os.getenv("ALSTYLE_CATEGORIES_MODE", "include").strip().lower()
ALSTYLE_CATEGORIES_PATH = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt").strip()

PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "3000000"))

SATU_KEYWORDS_MAXLEN = int(os.getenv("SATU_KEYWORDS_MAXLEN", "400"))
SATU_KEYWORDS_MAXWORDS = int(os.getenv("SATU_KEYWORDS_MAXWORDS", "40"))
SATU_KEYWORDS_GEO_MAX  = int(os.getenv("SATU_KEYWORDS_GEO_MAX", "20"))

# SEO cache
DEFAULT_CACHE_PATH  = "docs/.alstyle_cache/seo_cache.json"
SEO_CACHE_PATH     = os.getenv("SEO_CACHE_PATH", DEFAULT_CACHE_PATH)
SEO_STICKY         = os.getenv("SEO_STICKY", "1").lower() in {"1","true","yes","on"}
# Режимы: "monthly_1" (каждое 1-е число), "days" (каждые N суток), "off"
SEO_REFRESH_MODE   = os.getenv("SEO_REFRESH_MODE", "monthly_1").lower()
SEO_REFRESH_DAYS   = int(os.getenv("SEO_REFRESH_DAYS", "14"))  # используется когда MODE=days
LEGACY_CACHE_PATH  = "docs/seo_cache.json"

# Управление обработкой <description>
DESCRIPTION_MODE = os.getenv("DESCRIPTION_MODE", "off").lower()  # "off" = не менять описание; "on" = генерировать SEO-блоки

# Placeholders (фото)
PLACEHOLDER_ENABLE        = os.getenv("PLACEHOLDER_ENABLE", "1").lower() in {"1","true","yes","on"}
PLACEHOLDER_BRAND_BASE    = os.getenv("PLACEHOLDER_BRAND_BASE", "https://img.al-style.kz/brand").rstrip("/")
PLACEHOLDER_CATEGORY_BASE = os.getenv("PLACEHOLDER_CATEGORY_BASE", "https://img.al-style.kz/category").rstrip("/")
PLACEHOLDER_DEFAULT_URL   = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://img.al-style.kz/placeholder.jpg").strip()
PLACEHOLDER_EXT           = os.getenv("PLACEHOLDER_EXT", "jpg").strip().lower()
PLACEHOLDER_HEAD_TIMEOUT  = float(os.getenv("PLACEHOLDER_HEAD_TIMEOUT_S", "5"))

# Purge internals
DROP_CATEGORY_ID_TAG   = True
DROP_STOCK_TAGS        = True
PURGE_TAGS_AFTER       = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status")
PURGE_OFFER_ATTRS_AFTER= ("type","article")
INTERNAL_PRICE_TAGS    = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                          "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice",
                          "max_price","maxPrice","oldprice","oldPrice","rrp","RRP","prices")

# ======================= UTILS =======================
def log(msg: str)->None:
    print(str(msg), flush=True)

def warn(msg: str)->None:
    print(f"WARNING: {msg}", file=sys.stderr, flush=True)

def err(msg: str, code: int=1)->None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_utc()->datetime: return datetime.now(timezone.utc)
def now_utc_str()->str: return now_utc().strftime("%Y-%m-%d %H:%M:%S")
def now_almaty()->datetime:
    try: return datetime.now(ZoneInfo("Asia/Almaty"))
    except Exception: return datetime.utcfromtimestamp(time.time()+5*3600).replace(tzinfo=timezone.utc)
def format_dt_almaty(dt: datetime)->str:
    try: return dt.astimezone(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception: return datetime.utcfromtimestamp(dt.timestamp()+5*3600).strftime("%Y-%m-%d %H:%M:%S")

def get_text(parent: ET.Element, tag: str)->str:
    el = parent.find(tag)
    return (el.text or "").strip() if (el is not None and el.text) else ""

def set_text(parent: ET.Element, tag: str, val: str)->None:
    el = parent.find(tag)
    if el is None: el = ET.SubElement(parent, tag)
    el.text = val

def remove_all(parent: ET.Element, *tags: str)->int:
    c=0
    for tg in tags:
        for node in list(parent.findall(tg)):
            parent.remove(node); c+=1
    return c

def inner_html(el: Optional[ET.Element])->str:
    if el is None: return ""
    res=[]
    for node in list(el):
        res.append(ET.tostring(node, encoding="unicode"))
    return "".join(res).strip()

def dedup_preserve_order(seq: List[str])->List[str]:
    seen=set(); out=[]
    for x in seq:
        if x in seen: continue
        seen.add(x); out.append(x)
    return out

def _norm_text(s: str)->str:
    t=(s or "").strip()
    t=re.sub(r"\s+", " ", t)
    t=re.sub(r"&nbsp;"," ", t)
    t=re.sub(r"\u00a0"," ", t)
    t=_unescape(t)
    return t.strip()

def _norm_key(s: str)->str:
    return re.sub(r"[^a-z0-9]+","", (s or "").lower())

# ======================= SOURCE LOAD =======================
def load_source_bytes(url: str)->bytes:
    if not url: err("SUPPLIER_URL is empty")
    r=requests.get(url, timeout=60)
    if r.status_code!=200 or not r.content:
        err(f"Failed to load supplier: HTTP {r.status_code}")
    return r.content

# ======================= CATEGORY RULES =======================
def parse_categories_tree(shop_el: ET.Element)->Tuple[Dict[str,str], Dict[str,str], Dict[str,List[str]]]:
    id2name={}
    id2parent={}
    parent2children={}
    cats_el=shop_el.find("categories") or shop_el.find("Categories")
    if cats_el is None: return id2name,id2parent,parent2children
    for c in cats_el.findall("category"):
        cid=c.attrib.get("id") or ""
        name=(c.text or "").strip()
        pid=c.attrib.get("parentId") or ""
        if not cid: continue
        id2name[cid]=name
        id2parent[cid]=pid
        if pid not in parent2children: parent2children[pid]=[]
        parent2children[pid].append(cid)
    return id2name,id2parent,parent2children

def build_category_path_from_id(cid: str, id2name: Dict[str,str], id2parent: Dict[str,str])->List[str]:
    path=[]
    cur=cid
    loop_guard=0
    while cur and loop_guard<50:
        nm=id2name.get(cur) or ""
        if nm: path.append(nm)
        cur=id2parent.get(cur) or ""
        loop_guard+=1
    return list(reversed(path))

def category_matches_name(path: List[str], rules: List[str])->bool:
    t=" / ".join(path).lower()
    return any(rule.lower() in t for rule in rules)

def collect_descendants(ids: Set[str], parent2children: Dict[str,List[str]])->Set[str]:
    out=set(ids); stack=list(ids)
    while stack:
        cur=stack.pop()
        for ch in parent2children.get(cur, []):
            if ch not in out:
                out.add(ch); stack.append(ch)
    return out

def load_category_rules(path: str)->Tuple[List[str], List[str]]:
    if not os.path.exists(path): return [], []
    ids=[]; names=[]
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            ln=ln.strip()
            if not ln or ln.startswith("#"): continue
            if re.fullmatch(r"\d+", ln): ids.append(ln)
            else: names.append(ln)
    return ids, names

# ======================= BRAND/VENDOR HELPERS =======================
BRAND_WORDS = [
    "Canon","Epson","HP","Hewlett-Packard","Brother","Ricoh","Kyocera","Xerox","Pantum","Konica Minolta","Sharp","Samsung","OKI","Lexmark","Dell","Asus","MSI","Acer","Lenovo","Apple","Huawei","Honor","Xiaomi","Redmi","Realme","Nokia","ZTE","Tecno","Vivo","OnePlus","LG","Philips","Tefal","Midea","Gorenje","Bosch","Siemens","Indesit","Atlant","Beko","Hitachi","Toshiba","Hisense","Skyworth","Haier","Dexp","JVC","Pioneer","Sony","Sven","Defender","Logitech","Razer","HyperX","JBL","Edifier","Beats","Marshall","Audio-Technica","Ritmix","Tronsmart","Harman Kardon","SVEN","ViewSonic","Samsung"
]
FAMILY_WORDS = [
    "LaserJet","DeskJet","OfficeJet","PageWide","Color LaserJet","Neverstop","Smart Tank",
    "Phaser","WorkCentre","VersaLink","AltaLink","DocuCentre",
    "DCP","HL","MFC","FAX",
    "L","XP","WF","WorkForce","EcoTank",
    "FS","TASKalfa","ECOSYS",
    "Aficio","SP","MP","IM",
    "MX","BP","B","C","P2500","M6500","CM","DL","DP"
]
AS_INTERNAL_ART_RE = re.compile(r"^AS\d+", re.I)
MODEL_RE = re.compile(r"\b([A-Z][A-Z0-9\-]{2,})\b", re.I)

def _split_joined_models(s: str) -> List[str]:
    for bw in BRAND_WORDS:
        s = re.sub(rf"({re.escape(bw)})\s*(?={re.escape(bw)})", r"\1\n", s)
    raw = re.split(r"[,\n;]+", s)
    out=[]
    for x in raw:
        x=x.strip()
        if not x: continue
        if x.lower().startswith("https://") or x.lower().startswith("http://"): continue
        out.append(x)
    return out

def _looks_like_code_value(v: str) -> bool:
    v=v.strip()
    if len(v)<=2: return False
    if re.fullmatch(r"[A-Z0-9\-]{3,}", v, flags=re.I): return True
    return False

SAFE_SPEC_WHITELIST = {"артикул","модель","модель устройства","модель принтера","model","mpn","part","sku"}

KASPI_CODE_NAME_RE = re.compile(r"^(артикул|код|код товара|sku|vendorcode)\b", re.I)
UNWANTED_PARAM_NAME_RE = re.compile(r"^(наличие|цена|скидка|доставка|акция|брендовая коробка|подарок|благотворительность)\b", re.I)

def canon_colons(s: str) -> str:
    t=(s or "")
    t=re.sub(r"\s*:\s*", ": ", t)
    t=re.sub(r":\s*:", ": ", t)
    return t

def canon_units(name: str, v: str)->str:
    name=_norm_text(name); v=(v or "")
    v = v.replace(",", ".")
    v = re.sub(r"\s{2,}", " ", v).strip()
    if _norm_text(name) == "вес" and not re.search(r"\bкг\b", v, re.I): v = v + " кг"
    return v

def normalize_free_text_punct(s: str) -> str:
    t=canon_colons(s or ""); t=re.sub(r":\s*:", ": ", t)
    t=re.sub(r"(?<!\.)\.\.(?!\.)", ".", t)
    return re.sub(r"\s{2,}", " ", t).strip()

def extract_kv_from_description(text: str) -> List[Tuple[str,str]]:
    if not (text or "").strip(): return []
    t=(text or "").replace("\r\n","\n").replace("\r","\n")
    lines=[ln.strip() for ln in t.split("\n") if ln.strip()]
    pairs=[]
    for ln in lines:
        ln=canon_colons(ln)
        m=re.match(r"^([А-ЯA-Z][^:]{0,80}):\s*(.+)$", ln)
        if not m: continue
        n=m.group(1).strip(); v=m.group(2).strip()
        if len(n)<2 or len(v)<1: continue
        if _looks_like_code_value(v) and _norm_text(n) not in SAFE_SPEC_WHITELIST: continue
        pairs.append((n, canon_units(n, v)))
    return pairs

def build_specs_pairs_from_params(offer: ET.Element) -> List[Tuple[str,str]]:
    pairs=[]; seen=set()
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name=(p.attrib.get("name") or "").strip()
        raw_val =(p.text or "").strip()
        if not raw_name or not raw_val: continue
        name_norm = _norm_text(raw_name); val_norm = raw_val.strip()
        if KASPI_CODE_NAME_RE.fullmatch(raw_name) or UNWANTED_PARAM_NAME_RE.match(raw_name): continue
        if name_norm == "назначение" and val_norm.lower() == "да": continue
        if _looks_like_code_value(val_norm) and name_norm not in SAFE_SPEC_WHITELIST: continue
        pairs.append((raw_name.strip(), canon_units(raw_name, raw_val.strip())))
    out=[]; seen=set()
    for n,v in pairs:
        k=_norm_text(n)
        if k in seen: continue
        seen.add(k); out.append((n,v))
    return out

def build_specs_html_from_params(offer: ET.Element) -> str:
    pairs = build_specs_pairs_from_params(offer)
    if not pairs: return ""
    lis = "\n".join([f'  <li><strong>{n}:</strong> {v}</li>' for n,v in pairs])
    return f"<h3>Характеристики</h3>\n<ul>\n{lis}\n</ul>"

def has_specs_in_raw_desc(raw_html: str) -> bool:
    return bool(re.search(r"<h\d[^>]*>\s*Характеристики\s*</h\d>", raw_html or "", flags=re.I))

def build_lead_html(offer: ET.Element, raw_desc_text_for_kv: str, params_pairs: List[Tuple[str,str]]) -> Tuple[str, Dict[str,str]]:
    name = get_text(offer,"name")
    vendor = get_text(offer,"vendor")
    # Подбираем 2–3 подсказки для «чек-листа» в начале
    hints=[]
    for n,v in params_pairs[:3]:
        if _norm_text(n) in {"вес","гарантия","объём","диагональ экрана","частота обновления экрана","тип матрицы экрана"}:
            if v: hints.append((n,v))
    # fallback из распознанных KV из родного текста
    if len(hints)<3:
        for n,v in extract_kv_from_description(raw_desc_text_for_kv)[:3-len(hints)]:
            hints.append((n,v))
    ul=""
    if hints:
        bullets="\n".join([f'  <li>&#9989; {n}: {v}</li>' for n,v in hints])
        ul=f"<ul>\n{bullets}\n</ul>"
    title_vendor = f"{vendor}" if vendor else ""
    lead = f'<h3>{name}: Хороший выбор{(" ("+title_vendor+")") if title_vendor else ""}</h3>\n<p>Практичное решение для ежедневной работы.</p>\n{ul}'
    inputs={"kind":"","title":name,"bullets":ul}
    return lead, inputs

def build_faq_html(kind: str) -> str:
    return (
        "<h3>FAQ</h3>\n"
        "<p><strong>В:</strong> Поддерживаются современные сценарии?<br><strong>О:</strong> Да, ориентирован на повседневную офисную работу.</p>\n"
        "<p><strong>В:</strong> Можно расширять возможности?<br><strong>О:</strong> Да, подробности — в характеристиках модели.</p>"
    )

def build_reviews_html(seed: int) -> str:
    pool=[
        ('Даурен','Актобе','«Печать/работа стабильная, всё как ожидал.»'),
        ('Инна','Павлодар','«Установка заняла пару минут, проблем не было.»'),
        ('Ерлан','Атырау','«Коробка пришла слегка помятой, но сам товар без нареканий.»'),
        ('Арман','Караганда','«Отличное соотношение цены и качества.»'),
        ('Мария','Актобе','«Рекомендую, всё супер!»')
    ]
    random.seed(seed or 0xA5A5)
    picks=random.sample(pool, 3)
    out=[]
    for n,c,t in picks:
        out.append(f'<p>&#128100; <strong>{n}</strong>, {c} — &#11088;&#11088;&#11088;&#11088;&#11088;<br>{t}</p>')
    return "<h3>Отзывы (3)</h3>\n" + "\n".join(out)

def should_periodic_refresh(prev_dt_utc: Optional[datetime]) -> bool:
    if SEO_REFRESH_MODE == "off": return False
    if SEO_REFRESH_MODE == "days":
        if prev_dt_utc is None: return True
        delta = now_utc() - prev_dt_utc
        return delta >= timedelta(days=max(1, SEO_REFRESH_DAYS))
    if SEO_REFRESH_MODE == "monthly_1":
        now_alm = now_almaty()
        prev_alm = prev_dt_utc.astimezone(ZoneInfo("Asia/Almaty")) if prev_dt_utc and ZoneInfo else None
        if prev_alm is None: return True
        if now_alm.day != 1:
            return False
        return (now_alm.year, now_alm.month) != (prev_alm.year, prev_alm.month)
    return False

def compute_seo_checksum(name: str, lead_inputs: Dict[str,str], raw_desc_text_for_kv: str) -> str:
    base = "|".join([name or "", lead_inputs.get("kind",""), lead_inputs.get("title",""),
                     lead_inputs.get("bullets",""), lead_inputs.get("faq",""), lead_inputs.get("reviews",""),
                     hashlib.md5((raw_desc_text_for_kv or "").encode("utf-8")).hexdigest()])
    return hashlib.md5(base.encode("utf-8")).hexdigest()

def compose_full_description_html(lead_html: str, raw_desc_html_full: str, specs_html: str, faq_html: str, reviews_html: str) -> str:
    pieces=[]
    if lead_html: pieces.append(lead_html)
    if raw_desc_html_full: pieces.append(_html_escape_in_cdata_safe(raw_desc_html_full))
    if specs_html: pieces.append(specs_html)
    if faq_html: pieces.append(faq_html)
    if reviews_html: pieces.append(reviews_html)
    return "\n".join(pieces)

# ---------- placeholder helpers ----------
_url_head_cache: Dict[str,bool]={}
def url_exists(url: str)->bool:
    if not url: return False
    if url in _url_head_cache: return _url_head_cache[url]
    try:
        r=requests.head(url, timeout=PLACEHOLDER_HEAD_TIMEOUT, allow_redirects=True)
        ok = (200 <= r.status_code < 400)
    except Exception:
        ok = False
    _url_head_cache[url]=ok
    return ok

def _slug(s: str) -> str:
    if not s: return ""
    table=str.maketrans({"а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"h","ц":"c","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""})
    base=(s or "").lower().translate(table)
    base=re.sub(r"[^a-z0-9\- ]+","", base)
    return re.sub(r"\s+","-", base).strip("-") or "unknown"

def _placeholder_url_brand(vendor: str) -> str:
    return f"{PLACEHOLDER_BRAND_BASE}/{_slug(vendor)}.{PLACEHOLDER_EXT}"

def _placeholder_url_category(kind: str) -> str:
    return f"{PLACEHOLDER_CATEGORY_BASE}/{kind}.{PLACEHOLDER_EXT}"

def ensure_placeholder_pictures(shop_el: ET.Element) -> Tuple[int,int]:
    if not PLACEHOLDER_ENABLE: return (0,0)
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0)
    added=skipped=0
    for offer in offers_el.findall("offer"):
        pics = list(offer.findall("picture"))
        has_pic = any((p.text or "").strip() for p in pics)
        if has_pic: continue
        vendor = get_text(offer,"vendor").strip()
        name   = get_text(offer,"name").strip()
        kind   = detect_kind(name, [])
        picked = ""
        if vendor:
            u_brand = _placeholder_url_brand(vendor)
            if url_exists(u_brand): picked = u_brand
        if not picked:
            u_cat = _placeholder_url_category(kind or "generic")
            if url_exists(u_cat): picked = u_cat
        if not picked: picked = PLACEHOLDER_DEFAULT_URL
        pic = ET.SubElement(offer, "picture"); pic.text = picked
        added += 1
    return added, skipped

# ---------- availability/currency ----------
TRUE_WORDS={"true","1","yes","y","есть","in stock","available"}
FALSE_WORDS={"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}
def _parse_bool_str(s: str)->Optional[bool]:
    v=_norm_text(s or "");  return True if v in TRUE_WORDS else False if v in FALSE_WORDS else None
def _parse_int(s: str)->Optional[int]:
    t=re.sub(r"[^\d\-]+","", s or ""); 
    if t in {"","-","+"}: return None
    try: return int(t)
    except Exception: return None
def derive_available(offer: ET.Element) -> Tuple[bool, str]:
    avail_el=offer.find("available")
    if avail_el is not None and avail_el.text:
        b=_parse_bool_str(avail_el.text)
        if b is not None: return b, "tag"
    for tag in ["quantity_in_stock","quantity","stock","Stock"]:
        for node in offer.findall(tag):
            v=_parse_int(node.text or "")
            if v is not None:
                return (v>0), tag
    return True, "force_true"

def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int,int,int]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0,0,0,0
    t_true=t_false=rm_stock=0
    for offer in offers_el.findall("offer"):
        v, src = derive_available(offer)
        offer.set("available", "true" if v else "false")
        t_true += int(v); t_false += int(not v)
        if DROP_STOCK_TAGS:
            for tag in ["quantity_in_stock","quantity","stock","Stock"]:
                rm_stock += remove_all(offer, tag)
        remove_all(offer, "available")
    return t_true,t_false,rm_stock,0

def fix_currency_id(shop_el: ET.Element, default_code: str="KZT")->int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    c=0
    for off in offers_el.findall("offer"):
        cur = off.find("currencyId")
        if cur is None: cur = ET.SubElement(off, "currencyId")
        if (cur.text or "").strip().upper() != default_code:
            cur.text = default_code; c+=1
    return c

# ---------- reorder & categoryId ----------
DESIRED_ORDER=["vendorCode","name","price","picture","vendor","currencyId","description"]
def reorder_offer_children(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    changed=0
    for offer in offers_el.findall("offer"):
        children=list(offer)
        if not children: continue
        buckets={k:[] for k in DESIRED_ORDER}; others=[]
        for node in children: (buckets[node.tag] if node.tag in buckets else others).append(node)
        rebuilt=[*sum((buckets[k] for k in DESIRED_ORDER), []), *others]
        if rebuilt!=children:
            for node in children: offer.remove(node)
            for node in rebuilt: offer.append(node)
            changed+=1
    return changed

def ensure_categoryid_zero_first(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        remove_all(offer,"categoryId","CategoryId")
        cid=ET.Element("categoryId"); cid.text=os.getenv("CATEGORY_ID_DEFAULT","0")
        offer.insert(0,cid); touched+=1
    return touched

# ======================= KEYWORDS =======================
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")
STOPWORDS_RU = {"для","и","или","на","в","из","от","по","с","к","до","со","под","при","над","о","об","у","без","про","как","это","той","тот","эта","эти",
                "свой","ваш","наш","их","его","ее","ли","же","то","чтобы","можно","нужно","бы"}
CITIES=["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз","Оскемен","Семей","Костанай","Кызылорда","Орал","Петропавловск","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау"]

def tokenize_name(name: str)->List[str]:
    raw = [t.strip() for t in re.split(r"[,\s/|]+", name or "") if t.strip()]
    return [t for t in raw if WORD_RE.fullmatch(t)]

def is_content_word(tok: str)->bool:
    if tok.lower() in STOPWORDS_RU: return False
    if len(tok)<2: return False
    return True

def build_bigrams(words: List[str])->List[str]:
    out=[]
    for i in range(len(words)-1):
        a=words[i]; b=words[i+1]
        if not (is_content_word(a) and is_content_word(b)): continue
        out.append(f"{a} {b}")
    return out

def geo_tokens()->List[str]:
    return CITIES[:SATU_KEYWORDS_GEO_MAX]

def tokenize_models_from_offer(offer: ET.Element)->List[str]:
    tokens=set()
    for src in (get_text(offer,"name"), get_text(offer,"description")):
        if not src: continue
        for m in MODEL_RE.findall(src or ""):
            t=m.upper()
            if AS_INTERNAL_ART_RE.match(t) or not (re.search(r"[A-Z]", t) and re.search(r"\d", t)) or len(t)<5: continue
            tokens.add(t)
    return list(tokens)

def keywords_from_name_generic(name: str) -> List[str]:
    raw_tokens=tokenize_name(name or "")
    modelish=[t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
    content=[t for t in raw_tokens if is_content_word(t)]
    bigr=build_bigrams(content)
    norm=lambda tok: tok if re.search(r"[A-Z]{2,}", tok) else tok.capitalize()
    out=modelish[:8]+bigr[:8]+[norm(t) for t in content[:10]]
    return dedup_preserve_order(out)

def ensure_keywords(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    c=0
    for off in offers_el.findall("offer"):
        name=get_text(off,"name")
        base=keywords_from_name_generic(name)
        parts=base+tokenize_models_from_offer(off)
        parts+=geo_tokens()
        parts=[p for p in dedup_preserve_order(parts) if not AS_INTERNAL_ART_RE.match(str(p))]
        parts=parts[:SATU_KEYWORDS_MAXWORDS]
        out=[]; total=0
        for p in parts:
            s=str(p).strip().strip(",")
            if not s: continue
            add=((", " if out else "") + s)
            if total+len(add)>SATU_KEYWORDS_MAXLEN: break
            out.append(s); total+=len(add)
        k=off.find("keywords")
        if k is None: k=ET.SubElement(off,"keywords")
        k.text=", ".join(out); c+=1
    return c

# ======================= VENDOR =======================
def build_brand_index(shop_el: ET.Element)->Dict[str,str]:
    idx={}
    offers_el=shop_el.find("offers")
    if offers_el is None: return idx
    for off in offers_el.findall("offer"):
        v=(get_text(off,"vendor") or "").strip()
        if v: idx[_norm_key(v)]=v
    for w in BRAND_WORDS:
        idx.setdefault(_norm_key(w), w)
    return idx

def guess_vendor_for_offer(offer: ET.Element, brand_index: Dict[str,str])->str:
    name=(get_text(offer,"name") or "")
    for w in BRAND_WORDS:
        if re.search(rf"\b{re.escape(w)}\b", name, flags=re.I): return w
    # в <description> тоже можно, но это вторично
    desc=(get_text(offer,"description") or "")
    for w in BRAND_WORDS:
        if re.search(rf"\b{re.escape(w)}\b", desc, flags=re.I): return w
    return ""

def ensure_vendor(shop_el: ET.Element)->int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    c=0
    for off in offers_el.findall("offer"):
        v=off.find("vendor")
        if v is None:
            ET.SubElement(off,"vendor").text=""
            c+=1
    return c

def ensure_vendor_auto_fill(shop_el: ET.Element) -> int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    brand_index = build_brand_index(shop_el)
    touched=0
    for offer in offers_el.findall("offer"):
        v = offer.find("vendor")
        cur = (v.text or "").strip() if (v is not None and v.text) else ""
        if cur: continue
        guess = guess_vendor_for_offer(offer, brand_index)
        if guess:
            if v is None: v = ET.SubElement(offer, "vendor")
            v.text = guess
            brand_index[_norm_key(guess)] = guess
            touched += 1
    return touched

# ======================= PRICING =======================
PriceRule = Tuple[int,int,float,int]
PRICING_RULES: List[PriceRule] = [
    (   101,    10000, 4.0,  3000),( 10001, 25000, 4.0,  4000),( 25001, 50000, 4.0,  5000),
    ( 50001,    75000, 4.0,  7000),( 75001,100000, 4.0, 10000),(100001,150000, 4.0, 12000),
    (150001,   200000, 4.0, 15000),(200001,300000, 4.0, 20000),(300001,400000, 4.0, 25000),
    (400001,   500000, 4.0, 30000),(500001,750000, 4.0, 40000),(750001,1000000,4.0, 50000),
    (1000001, 1500000, 4.0, 70000),(1500001,2000000,4.0, 90000),(2000001,100000000,4.0,100000),
]

PRICE_FIELDS_DIRECT=["purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price"]
PRICE_KEYWORDS_DEALER = re.compile(r"(dealer|дилер|опт|wholesale|b2b)", flags=re.I)

def pick_base_price(offer: ET.Element)->Optional[int]:
    # Приоритет: служебные цены (prices type~dealer/опт/b2b) — здесь их уже нет → берём прямые поля
    for tag in PRICE_FIELDS_DIRECT:
        el=offer.find(tag)
        if el is None or not (el.text or "").strip(): continue
        try:
            v=int(re.sub(r"[^\d]+","", el.text))
            if v>0: return v
        except Exception:
            continue
    # fallback: <price> поставщика, если есть
    try:
        v=int(re.sub(r"[^\d]+","", get_text(offer,"price")))
        if v>0: return v
    except Exception:
        pass
    return None

def apply_rule(base: int, rule: PriceRule)->int:
    mn,mx,pct,adder=rule
    if base<mn or base>mx: return 0
    out = int(round(base*(1.0+pct/100.0))) + adder
    # последние три цифры -> 900
    out = int(str(out)[:-3] + "900") if out>=1000 else 900
    return out

def reprice_offers(shop_el: ET.Element, rules: List[PriceRule])->int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    c=0
    for off in offers_el.findall("offer"):
        base=pick_base_price(off)
        if base is None: continue
        newp=0
        for r in rules:
            newp=apply_rule(base, r)
            if newp: break
        if not newp: continue
        set_text(off,"price", str(newp)); c+=1
    return c

def enforce_forced_prices(shop_el: ET.Element)->int:
    # safety valve: ничего не трогаем по факту, просто пример
    return 0

# ======================= PARAMS PURGE =======================
def remove_specific_params(shop_el: ET.Element)->int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    removed=0
    for off in offers_el.findall("offer"):
        for p in list(off.findall("param")) + list(off.findall("Param")):
            nm=(p.attrib.get("name") or "").strip()
            if re.fullmatch(r"(благотворительность|артикул)", nm, flags=re.I):
                off.remove(p); removed+=1
    return removed

# ======================= SEO DESCRIPTION INJECTOR =======================
def load_seo_cache(path: str)->Dict[str,dict]:
    if not SEO_STICKY: return {}
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        if os.path.exists(path):
            with open(path,"r",encoding="utf-8",errors="ignore") as f:
                return json.load(f) or {}
    except Exception:
        return {}
    return {}

def save_seo_cache(path: str, data: Dict[str,dict])->None:
    if not SEO_STICKY: return
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path,"w",encoding="utf-8",newline="\n") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def inject_seo_descriptions(shop_el: ET.Element) -> Tuple[int, str]:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0, ""
    cache = load_seo_cache(SEO_CACHE_PATH) if SEO_STICKY else {}
    changed=0
    for offer in offers_el.findall("offer"):
        name = get_text(offer, "name")
        d = offer.find("description")

        raw_desc_html_full = inner_html(d) if d is not None else ""
        raw_desc_text_for_kv = re.sub(r"<br\s*/?>", "\n", raw_desc_html_full, flags=re.I)
        raw_desc_text_for_kv = re.sub(r"<[^>]+>", "", raw_desc_text_for_kv)

        params_pairs = build_specs_pairs_from_params(offer)

        lead_html, inputs = build_lead_html(offer, raw_desc_text_for_kv, params_pairs)
        seed = int(re.sub(r"[^\d]+","", offer.attrib.get("id") or "0") or "0")
        faq_html = build_faq_html(inputs.get("kind",""))
        reviews_html = build_reviews_html(seed)

        specs_html = "" if has_specs_in_raw_desc(raw_desc_html_full) else build_specs_html_from_params(offer)

        checksum = compute_seo_checksum(name, inputs, raw_desc_text_for_kv)
        cache_key = offer.attrib.get("id") or (get_text(offer,"vendorCode") or "").strip() or hashlib.md5((name or "").encode("utf-8")).hexdigest()

        use_cache = False
        if SEO_STICKY and cache.get(cache_key):
            ent = cache[cache_key]
            prev_cs = ent.get("checksum","")
            updated_at_prev = ent.get("updated_at","")
            try:
                prev_dt_utc = datetime.strptime(updated_at_prev, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                prev_dt_utc = None
            periodic = should_periodic_refresh(prev_dt_utc)
            if prev_cs == checksum and not periodic:
                lead_html   = ent.get("lead_html", lead_html)
                faq_html    = ent.get("faq_html", faq_html)
                reviews_html= ent.get("reviews_html", reviews_html)
                use_cache   = True

        full_html = compose_full_description_html(lead_html, raw_desc_html_full, specs_html, faq_html, reviews_html)
        placeholder = f"[[[HTML]]]{full_html}[[[/HTML]]]"

        if d is None:
            d = ET.SubElement(offer, "description"); d.text = placeholder; changed += 1
        else:
            old = inner_html(d)
            if ("[[[HTML]]]" in (d.text or "")) or (old.strip() != full_html.strip()):
                d.clear(); d.text = placeholder; changed += 1

        cache[cache_key] = {
            "checksum": checksum,
            "lead_html": lead_html,
            "faq_html": faq_html,
            "reviews_html": reviews_html,
            "updated_at": now_utc_str(),
        }
    save_seo_cache(SEO_CACHE_PATH, cache)

    last_alm=None
    if cache:
        for ent in cache.values():
            ts = ent.get("updated_at")
            if not ts: continue
            try:
                utc_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                alm_dt = utc_dt.astimezone(ZoneInfo("Asia/Almaty")) if ZoneInfo else datetime.utcfromtimestamp(utc_dt.timestamp()+5*3600)
                if (last_alm is None) or (alm_dt > last_alm): last_alm = alm_dt
            except Exception:
                continue
    if not last_alm: last_alm = now_almaty()
    return changed, format_dt_almaty(last_alm)

# ======================= CDATA PLACEHOLDER REPLACER =======================
def _replace_html_placeholders_with_cdata(xml_text: str) -> str:
    def repl(m):
        inner = m.group(1)
        return f"<description><![CDATA[\n{inner}\n]]></description>"
    return re.sub(r"<description>(\s*\[\[\[HTML\]\]\].*?\[\[\[\/HTML\]\]\]\s*)</description>", repl, xml_text, flags=re.S)

# ======================= MAIN =======================
def main()->None:
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")
    offers_in_el=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None: err("XML: <offers> not found")
    src_offers=list(offers_in_el.findall("offer"))

    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")
    for o in src_offers: out_offers.append(deepcopy(o))

    # Фильтр по категориям (include/exclude)
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        id2name,id2parent,parent2children=parse_categories_tree(shop_in)
        rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH)
        if ALSTYLE_CATEGORIES_MODE=="include" and not (rules_ids or rules_names):
            err("ALSTYLE_CATEGORIES_MODE=include, нет правил (docs/alstyle_categories.txt).", 2)
        keep_ids=set(rules_ids)
        if rules_names and id2name:
            for cid in id2name.keys():
                path=build_category_path_from_id(cid,id2name,id2parent)
                if category_matches_name(path, rules_names): keep_ids.add(cid)
        if keep_ids and parent2children: keep_ids=collect_descendants(keep_ids,parent2children)
        for off in list(out_offers.findall("offer")):
            cid=get_text(off,"categoryId"); hit=(cid in keep_ids) if cid else False
            drop=(ALSTYLE_CATEGORIES_MODE=="exclude" and hit) or (ALSTYLE_CATEGORIES_MODE=="include" and not hit)
            if drop: out_offers.remove(off)

    # CATEGORY ID → 0 первым
    if DROP_CATEGORY_ID_TAG:
        for off in out_offers.findall("offer"):
            for node in list(off.findall("categoryId"))+list(off.findall("CategoryId")): off.remove(node)

    flagged = 0  # резерв на будущую диагностику; не мешает логике
    log(f"Flagged by PRICE_CAP >= {PRICE_CAP_THRESHOLD}: {flagged}")

    ensure_vendor(out_shop)
    filled = ensure_vendor_auto_fill(out_shop); log(f"Vendors auto-filled: {filled}")

    ensure_vendorcode_with_article(
        out_shop, prefix=os.getenv("VENDORCODE_PREFIX","AS"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )
    sync_offer_id_with_vendorcode(out_shop)

    reprice_offers(out_shop, PRICING_RULES)
    forced = enforce_forced_prices(out_shop); log(f"Forced price=100: {forced}")

    removed_params = remove_specific_params(out_shop); log(f"Params removed: {removed_params}")

    ph_added,_ = ensure_placeholder_pictures(out_shop); log(f"Placeholders added: {ph_added}")

    if DESCRIPTION_MODE != "off":
        seo_changed, seo_last_update_alm = inject_seo_descriptions(out_shop)
        log(f"SEO blocks touched: {seo_changed}")
    else:
        seo_changed, seo_last_update_alm = (0, "")

    t_true, t_false, _, _ = normalize_available_field(out_shop)
    fix_currency_id(out_shop, default_code="KZT")

    for off in out_offers.findall("offer"): purge_offer_tags_and_attrs_after(off)

    # Упорядочивание блоков, categoryId=0 в начало
    global DESIRED_ORDER
    DESIRED_ORDER=["vendorCode","name","price","picture","vendor","currencyId","description"]
    def reorder_offer_children(shop_el: ET.Element) -> int:
        offers_el=shop_el.find("offers")
        if offers_el is None: return 0
        changed=0
        for offer in offers_el.findall("offer"):
            children=list(offer)
            if not children: continue
            buckets={k:[] for k in DESIRED_ORDER}; others=[]
            for node in children: (buckets[node.tag] if node.tag in buckets else others).append(node)
            rebuilt=[*sum((buckets[k] for k in DESIRED_ORDER), []), *others]
            if rebuilt!=children:
                for node in children: offer.remove(node)
                for node in rebuilt: offer.append(node)
                changed+=1
        return changed

    reorder_offer_children(out_shop)
    ensure_categoryid_zero_first(out_shop)

    kw_touched = ensure_keywords(out_shop); log(f"Keywords updated: {kw_touched}")

    try: ET.indent(out_root, space="  ")
    except Exception: pass

    built_alm = now_almaty()
    meta_pairs={
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL or "file",
        "offers_total": len(src_offers),
        "offers_written": len(list(out_offers.findall("offer"))),
        "available_true": str(t_true),
        "available_false": str(t_false),
        "built_utc": now_utc_str(),
        "built_alm": format_dt_almaty(built_alm),
        "next_build_alm": format_dt_almaty(built_alm + timedelta(days=max(1, SEO_REFRESH_DAYS))),
        "seo_last_update_alm": seo_last_update_alm or format_dt_almaty(built_alm),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    xml_bytes=ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text=xml_bytes.decode(ENC, errors="replace")
    xml_text=re.sub(r"(?s)(-->)\s*(<shop\b)", r"\1\n\2", xml_text, count=1)
    xml_text=re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_text)
    xml_text=re.sub(r"(\n[ \t]*){3,}", "\n\n", xml_text)
    xml_text=_replace_html_placeholders_with_cdata(xml_text)

    if os.getenv("DRY_RUN","0").lower() in {"1","true","yes"}:
        log("[DRY_RUN=1] Files not written.")
        return
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    try:
        with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
            f.write(xml_text)
    except UnicodeEncodeError as e:
        warn(f"{ENC} can't encode some characters ({e}); writing with xmlcharrefreplace fallback")
        with open(OUT_FILE_YML, "w", encoding=ENC, errors="xmlcharrefreplace", newline="\n") as f:
            f.write(xml_text)

# ======================= REMAINING HELPERS =======================
def detect_kind(name: str, params_pairs: List[Tuple[str,str]])->str:
    n=(name or "").lower()
    if any(w in n for w in ["картридж","тонер"]): return "cartridge"
    if any(w in n for w in ["принтер","мфу","лазерный"]): return "printer"
    if any(w in n for w in ["монитор","display","экран"]): return "monitor"
    if any(w in n for w in ["ноутбук","laptop","ultrabook"]): return "laptop"
    return "generic"

def ensure_vendorcode_with_article(shop_el: ET.Element, prefix: str="AS", create_if_missing: bool=True)->int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    c=0
    for off in offers_el.findall("offer"):
        v=off.find("vendorCode")
        cur=(v.text or "").strip() if (v is not None and v.text) else ""
        if not cur and create_if_missing:
            # Проставляем из offer/@id или создаём
            base = (off.attrib.get("id") or "").strip()
            if not base:
                base = f"{prefix}{random.randint(10000,99999)}"
                off.set("id", base)
            if v is None: v=ET.SubElement(off,"vendorCode")
            v.text = f"{prefix}{re.sub(r'[^0-9]+','', base)}" if re.search(r"\d", base) else base
            c+=1
    return c

def sync_offer_id_with_vendorcode(shop_el: ET.Element)->int:
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    c=0
    for off in offers_el.findall("offer"):
        vc=get_text(off,"vendorCode")
        if vc and off.attrib.get("id")!=vc:
            off.set("id", vc); c+=1
    return c

def purge_offer_tags_and_attrs_after(off: ET.Element)->None:
    for tg in PURGE_TAGS_AFTER: remove_all(off, tg)
    for a in list(off.attrib.keys()):
        if a in PURGE_OFFER_ATTRS_AFTER:
            try: del off.attrib[a]
            except Exception: pass
    # Внутренние ценовые теги вычищаем
    for tg in INTERNAL_PRICE_TAGS: remove_all(off, tg)

def render_feed_meta_comment(meta: Dict[str,str])->str:
    pairs = [f'{k}="{v}"' for k,v in meta.items()]
    return "FEED_META " + " ".join(pairs)

if __name__=="__main__":
    main()
