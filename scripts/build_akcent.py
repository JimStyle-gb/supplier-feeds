#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Akcent feed builder (structure = old script) + your 6 changes.

Key deltas:
1) <offer available="..."> attribute only (child <available> removed)
2) <categoryId>0</categoryId> inserted as the first child in <offer>
3) <keywords> restored (at the end of each offer) with corrected city names
4) Specs: first collect from <Param> (keep only needed ones, DO NOT delete them), then complement from raw description
5) Raw description is paraphrased LAST, after specs are built; remove spec-like phrases from it beforehand
6) Everything else (filter by <name>, FEED_META, pricing style, separators) unchanged from old logic
"""

import os, sys, re, time, html, random, urllib.parse, requests
from copy import deepcopy
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

# ----------------- Settings / Constants -----------------
SUPPLIER_NAME = "AkCent"
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml")
OUT_FILE      = os.getenv("OUT_FILE", "docs/akcent.yml")
ENCODING_OUT  = os.getenv("OUTPUT_ENCODING", "windows-1251")

AKCENT_KEYWORDS_PATH = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt")
AKCENT_KEYWORDS_MODE = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()  # include|exclude

TIMEOUT = 35
RETRIES = 4
MIN_BYTES = 1500

# old-style pricing rules (unchanged)
PRICING_RULES = [
    (101,10000,4.0,3000),(10001,25000,4.0,4000),(25001,50000,4.0,5000),
    (50001,75000,4.0,7000),(75001,100000,4.0,10000),(100001,150000,4.0,12000),
    (150001,200000,4.0,15000),(200001,300000,4.0,20000),(300001,400000,4.0,25000),
    (400001,500000,4.0,30000),(500001,750000,4.0,40000),(750001,1000000,4.0,50000),
    (1000001,1500000,4.0,70000),(1500001,2000000,4.0,90000),(2000001,100000000,4.0,100000),
]

# Desired child order inside <offer> (old structure + your changes)
DESIRED_ORDER = [
    "categoryId", "vendorCode", "name", "price", "picture", "vendor", "currencyId", "description"
    # Params (Param/param) can follow, then <keywords> at the very end
]

# Allowed and canonical spec keys (for Satu/SEO)
CANON_MAP = {
    "тип":"Тип","вид":"Тип","назначение":"Назначение",
    "тип печати":"Тип печати","цветность":"Цвет печати","цвет печати":"Цвет печати",
    "формат":"Формат","формат бумаги":"Формат",
    "разрешение печати":"Разрешение печати","разрешение":"Разрешение",
    "оптическое разрешение":"Оптическое разрешение","разрешение сканера":"Оптическое разрешение","разрешение сканера,dpi":"Оптическое разрешение",
    "скорость печати":"Скорость печати","двусторонняя печать":"Двусторонняя печать",
    "тип чернил":"Тип чернил","цвета чернил":"Цвета чернил","ресурс":"Ресурс",
    "совместимость":"Совместимость","совместимые продукты":"Совместимость",
    "автоподатчик":"Автоподатчик","интерфейсы":"Интерфейсы","интерфейс":"Интерфейсы","входы":"Интерфейсы",
    "wi-fi":"Wi-Fi","дисплей":"Дисплей","жк дисплей":"Дисплей",
    # panels/monitors
    "диагональ":"Диагональ","разрешение экрана":"Разрешение экрана","яркость":"Яркость",
    "контрастность":"Контрастность","время отклика":"Время отклика","частота обновления":"Частота обновления",
}
ALLOWED_SPECS = set(CANON_MAP.values())

MAX_LEN_BY_KEY = {
    "Тип":60,"Назначение":120,"Тип печати":20,"Цвет печати":20,"Формат":120,
    "Разрешение":30,"Разрешение печати":30,"Оптическое разрешение":30,
    "Скорость печати":30,"Интерфейсы":120,"Дисплей":20,"Wi-Fi":5,
    "Двусторонняя печать":5,"Подача бумаги":40,"Тип чернил":60,"Цвета чернил":120,
    "Страна происхождения":40,"Гарантия":20,"Автоподатчик":5,"Совместимость":400,"Ресурс":60,
    "Диагональ":20,"Разрешение экрана":20,"Яркость":20,"Контрастность":20,"Время отклика":20,"Частота обновления":20,
}

# Cities (fixed names; keywords tag)
KZ_CITIES = [
    "Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
    "Усть-Каменогорск","Семей","Костанай","Кызылорда","Уральск","Петропавловск",
    "Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау"
]

# ----------------- Helpers -----------------
def log(msg): print(msg, flush=True)
def err(msg, code=1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def enc_safe(s: str) -> str:
    if not s: return s
    repl = {
        "\u2026":"...", "\u2014":"-", "\u2013":"-", "\u2212":"-",
        "\u2122":"",  "\u2265":">=", "\u2264":"<=", "\u00A0":" ",
        "\u2018":"'", "\u2019":"'", "\u201C":'"', "\u201D":'"', "\u2009":" ", "\u200A":" ", "\u200B":"",
        "\u00D7":"x", "×":"x",
    }
    for a,b in repl.items(): s = s.replace(a,b)
    try:
        s.encode(ENCODING_OUT); return s
    except Exception:
        return s.encode(ENCODING_OUT, errors="ignore").decode(ENCODING_OUT, errors="ignore")

def fetch_bytes(url: str) -> bytes:
    sess = requests.Session()
    last=None
    for i in range(RETRIES):
        try:
            r = sess.get(url, timeout=TIMEOUT)
            if r.status_code != 200: raise RuntimeError(f"HTTP {r.status_code}")
            if len(r.content) < MIN_BYTES: raise RuntimeError("too small")
            return r.content
        except Exception as e:
            last=e; time.sleep(1.5*(i+1))
    raise RuntimeError(str(last))

def get_text(el, tag):
    n = el.find(tag)
    return (n.text or "").strip() if n is not None and n.text else ""

# ----------------- Filter by <name> (old behavior) -----------------
def _norm_name(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").lower().replace("ё","е")).strip()

class KeySpec:
    __slots__=("raw","kind","norm","pat")
    def __init__(self, raw, kind, norm=None, pat=None): self.raw, self.kind, self.norm, self.pat = raw, kind, norm, pat

def load_name_filter(path):
    if not os.path.exists(path): return []
    data=None
    for enc in ("utf-8-sig","utf-8","cp1251","utf-16","utf-16-le","utf-16-be"):
        try:
            data=open(path,"r",encoding=enc).read(); break
        except: pass
    if data is None: data=open(path,"r",encoding="utf-8",errors="ignore").read()
    keys=[]
    for ln in data.splitlines():
        s=ln.strip()
        if not s or s.startswith("#"): continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try: keys.append(KeySpec(s,"regex",None,re.compile(s[1:-1], re.I)))
            except: pass
        else:
            keys.append(KeySpec(s,"prefix",_norm_name(s)))
    return keys

def name_matches(name: str, keys) -> bool:
    nm = _norm_name(name)
    for ks in keys:
        if ks.kind=="prefix" and nm.startswith(ks.norm): return True
        if ks.kind=="regex" and ks.pat and ks.pat.search(name): return True
    return False

# ----------------- Pricing (old style) -----------------
PRICE_TAGS_INTERNAL = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice",
                       "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
                       "min_price","minPrice","max_price","maxPrice","oldprice")
def parse_num(s):
    if not s: return None
    t = s.replace("\xa0"," ").replace(" ","").replace("KZT","").replace("₸","").replace(",",".")
    try:
        v=float(t); return v if v>0 else None
    except: return None
def pick_dealer_price(o):
    vals=[]
    for t in PRICE_TAGS_INTERNAL+("price",):
        n=o.find(t)
        if n is not None and n.text:
            v=parse_num(n.text)
            if v: vals.append(v)
    return min(vals) if vals else None
def force_tail_900(n):
    n=int(n); return (n//1000)*1000+900 if n>=0 else 900
def compute_retail(d):
    for lo,hi,pct,add in PRICING_RULES:
        if lo<=d<=hi:
            return force_tail_900(d*(1+pct/100.0)+add)
    return None
def reprice(shop):
    offers = shop.find("offers") or ET.Element("offers")
    for o in offers.findall("offer"):
        d = pick_dealer_price(o)
        if not d: 
            # strip internals
            for t in PRICE_TAGS_INTERNAL:
                for n in list(o.findall(t)): o.remove(n)
            continue
        rr = compute_retail(d)
        if rr:
            for n in list(o.findall("price")): o.remove(n)
            ET.SubElement(o,"price").text = str(int(rr))
        for t in PRICE_TAGS_INTERNAL:
            for n in list(o.findall(t)): o.remove(n)

# ----------------- Availability changes (your #1) -----------------
TRUE_WORDS = {"true","1","yes","y","да","есть","available","в наличии"}
FALSE_WORDS= {"false","0","no","n","нет","под заказ","ожидается"}
def _bool_from_str(s):
    if s is None: return None
    v=(s or "").strip().lower()
    if v in TRUE_WORDS: return True
    if v in FALSE_WORDS: return False
    return None
def derive_available(offer) -> bool:
    # try attr
    if "available" in offer.attrib:
        b=_bool_from_str(offer.attrib.get("available"))
        if b is not None: return b
    # child
    n=offer.find("available")
    if n is not None and n.text:
        b=_bool_from_str(n.text)
        if b is not None: return b
    # quantity-like
    for t in ("quantity_in_stock","quantity","stock","Stock"):
        q=offer.find(t)
        if q is not None and q.text:
            try: return int(re.sub(r"[^\d\-]+","", q.text))>0
            except: pass
    return True  # как было: считаем доступным
def set_available_attribute(shop):
    offers = shop.find("offers") or ET.Element("offers")
    on=off=0
    for o in offers.findall("offer"):
        b=derive_available(o)
        o.attrib["available"] = "true" if b else "false"
        # remove any child <available>
        for n in list(o.findall("available")): o.remove(n)
        if b: on+=1
        else: off+=1
    return on,off

# ----------------- Currency, ordering, categoryId (your #2), etc. -----------------
def ensure_categoryid_first(shop):
    offers = shop.find("offers") or ET.Element("offers")
    for o in offers.findall("offer"):
        # drop existing categoryId tags
        for n in list(o.findall("categoryId"))+list(o.findall("CategoryId")):
            o.remove(n)
        cid = ET.Element("categoryId"); cid.text = "0"
        o.insert(0, cid)

def fix_currency(shop):
    offers = shop.find("offers") or ET.Element("offers")
    for o in offers.findall("offer"):
        for n in list(o.findall("currencyId")): o.remove(n)
        ET.SubElement(o,"currencyId").text="KZT"

def reorder_offer_children(shop):
    offers = shop.find("offers") or ET.Element("offers")
    for o in offers.findall("offer"):
        ch = list(o)
        buckets = {t:[] for t in DESIRED_ORDER}; params=[]; others=[]
        for n in ch:
            if n.tag in buckets: buckets[n.tag].append(n)
            elif n.tag in ("Param","param","PARAM"): params.append(n)
            else: others.append(n)
        rebuilt = sum((buckets[t] for t in DESIRED_ORDER), []) + params + others
        if rebuilt != ch:
            for n in ch: o.remove(n)
            for n in rebuilt: o.append(n)

# ----------------- Build keywords (your #3 at the end) -----------------
def build_keywords_text(offer):
    name = get_text(offer,"name")
    vendor = get_text(offer,"vendor")
    words=[]
    if vendor: words.append(vendor)
    # model-like tokens
    for t in re.findall(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}", name or ""):
        if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t):
            up=t.upper()
            if up not in words: words.append(up)
        if len(words)>=10: break
    words += KZ_CITIES
    txt = ", ".join(words)
    return txt[:1024]

def ensure_keywords_last(shop):
    offers = shop.find("offers") or ET.Element("offers")
    k=0
    for o in offers.findall("offer"):
        # remove any existing keywords for rebuild
        for n in list(o.findall("keywords")): o.remove(n)
        kw = build_keywords_text(o)
        node = ET.SubElement(o,"keywords")
        node.text = kw
        k+=1
    return k

# ----------------- Params cleaning (your #5) -----------------
DISALLOWED_PARAM_NAMES = {
    "сопутствующие товары","бренд","brand","manufacturer","vendor","поставщик","для бренда",
    "наименование производителя","артикул","sku","код товара","модель","вид","тип товара",
}
def canon_key(k: str):
    if not k: return None
    return CANON_MAP.get(k.strip().lower().replace("ё","е"))
def clean_ws(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").replace("\u00A0"," ").replace("&nbsp;"," ")).strip(" \t,:;.-")
def yesno(s):
    v=(s or "").strip().lower()
    if v in {"да","есть","yes","true"}: return "Да"
    if v in {"нет","no","false"}: return "Нет"
    return clean_ws(s)
def norm_resolution_print(s):
    t = s.replace("x","x").replace("X","x")
    m = re.search(r"(\d{2,5})\s*[x×]\s*(\d{2,5})", t)
    return (m.group(1)+"x"+m.group(2)+" dpi") if m else clean_ws(s)
def norm_speed(s):
    m = re.search(r"(\d{1,3})\s*(стр|pages)", s, re.I)
    if m: return f"до {m.group(1)} стр/мин"
    m2 = re.match(r"^\s*(\d{1,3})\s*$", s)
    return f"до {m2.group(1)} стр/мин" if m2 else clean_ws(s)
def extract_ifaces(text: str):
    s = clean_ws(text)
    order = ["USB","USB (тип B)","USB-host","Wi-Fi","Wi-Fi Direct","Ethernet","Bluetooth","HDMI","DisplayPort","SD-карта"]
    pats = [
        (r"\bwi[\s\-]?fi\s*direct\b","Wi-Fi Direct"),(r"\bwi[\s\-]?fi\b","Wi-Fi"),
        (r"\bethernet\b","Ethernet"),(r"\brj[\s\-]?45\b","RJ-45"),
        (r"\bbluetooth\b","Bluetooth"),(r"\bhdmi\b","HDMI"),
        (r"\bdisplay\s*port|displayport\b","DisplayPort"),
        (r"\busb[\s\-]?host\b","USB-host"),(r"\bsd[\s\-]?карта|\bsd\b","SD-карта"),
        (r"\busb\b","USB"),(r"\bтип\s*b\b","USB (тип B)"),
    ]
    found=set()
    for rg,label in pats:
        if re.search(rg, s, re.I): found.add(label)
    return ", ".join([k for k in order if k in found])

def norm_value_by_key(key, val):
    k=key.lower()
    if k in {"wi-fi","дисплей","двусторонняя печать","автоподатчик"}: return yesno(val)
    if k in {"интерфейсы","интерфейс","входы"}: return extract_ifaces(val)
    if k=="скорость печати": return norm_speed(val)
    if k in {"разрешение печати","оптическое разрешение"}: return norm_resolution_print(val)
    if k=="гарантия":
        m=re.search(r"(\d{1,3})", val); return f"{m.group(1)} мес" if m else clean_ws(val)
    return clean_ws(val)

def clean_params_in_offer(offer):
    """Оставляем только полезные Param, нормализуем имена/значения, сохраняем их (не удаляем)."""
    useful=[]
    for tag in ("Param","param","PARAM"):
        for p in list(offer.findall(tag)):
            raw_k=(p.get("name") or p.get("Name") or "").strip()
            raw_v=(p.text or "").strip()
            if not raw_k or not raw_v: 
                offer.remove(p); continue
            if raw_k.strip().lower() in DISALLOWED_PARAM_NAMES:
                offer.remove(p); continue
            ck = canon_key(raw_k)
            if not ck or ck not in ALLOWED_SPECS:
                offer.remove(p); continue
            v = norm_value_by_key(ck, raw_v)
            if not v:
                offer.remove(p); continue
            # write back normalized
            p.set("name", ck)
            if len(v)>MAX_LEN_BY_KEY.get(ck,9999): v=v[:MAX_LEN_BY_KEY[ck]-3]+"..."
            p.text = clean_ws(v)
            useful.append((ck, p.text))
    return dict(useful)

# ----------------- Extract specs from raw description (your #4) -----------------
KV_KEYS = "|".join([
    "Тип","Назначение","Тип печати","Цвет печати","Формат","Разрешение печати","Оптическое разрешение",
    "Скорость печати","Интерфейсы","Wi-?Fi","Двусторонняя печать","Дисплей","Подача бумаги",
    "Тип чернил","Цвета чернил","Страна происхождения","Гарантия","Автоподатчик","Совместимость","Ресурс"
])
KV_RE = re.compile(rf"\b({KV_KEYS})\s*:\s*([^.;\n]+)", re.I)

def html_to_text(s: str)->str:
    if not s: return ""
    # unescape if needed
    for _ in range(2):
        if re.search(r"&lt;/?[a-zA-Z]", s): s = html.unescape(s)
    s = re.sub(r"<(script|style|iframe|noscript)[^>]*>.*?</\1>", " ", s, flags=re.S|re.I)
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = s.replace("\u00A0"," ").replace("&nbsp;"," ")
    s = re.sub(r"\s+"," ", s)
    return s.strip()

def extract_specs_from_description(raw_html: str):
    """Возвращает (specs_dict, txt_without_specs) — вырезает найденные пары и явные намёки."""
    txt = html_to_text(raw_html)
    found={}
    consumed_spans=[]

    # 1) Классические пары "Ключ: Значение"
    for m in KV_RE.finditer(txt):
        k = m.group(1)
        v = m.group(2)
        ck = canon_key(k)
        if ck and ck in ALLOWED_SPECS and ck not in found:
            found[ck] = norm_value_by_key(ck, v)
            consumed_spans.append((m.start(), m.end()))

    # 2) Без двоеточия (намеки)
    HINTS = [
        # Скорость печати до 32 стр./мин
        (re.compile(r"скорост[ьи]\s*печати[^0-9]*(\d{1,3})", re.I), "Скорость печати", lambda m: f"до {m.group(1)} стр/мин"),
        # Формат A4 / A5 ...
        (re.compile(r"\bформат[а]?\s*([AB]\d|A4|A5|A6|Letter|Legal|10\s*[x×]\s*15|13\s*[x×]\s*18|9\s*[x×]\s*13)", re.I),
            "Формат", lambda m: re.sub(r"\s*[x×]\s*","x", m.group(1))),
        # Оптическое/печати разрешение
        (re.compile(r"(?:оптическ\w*\s*разрешени\w*|разрешени\w*\s*печати|разрешени\w*\s*сканера)\s*[:\-]?\s*([\d\sx×]{3,}\s*dpi)", re.I),
            "Оптическое разрешение", lambda m: norm_resolution_print(m.group(1))),
        # Двусторонняя печать
        (re.compile(r"двусторонн\w+\s*печать", re.I), "Двусторонняя печать", lambda m: "Да"),
        # Интерфейсы
        (re.compile(r"\b(wi[\s\-]?fi(\s*direct)?|ethernet|usb(\s*host)?|bluetooth|hdmi|display\s*port|rj[\- ]?45)\b", re.I),
            "Интерфейсы", lambda m: extract_ifaces(m.group(0))),
        # Тип чернил
        (re.compile(r"\b(пигментн\w+|водорастворим\w+)\b", re.I), "Тип чернил", lambda m: "пигментные" if "пигмент" in m.group(1).lower() else "водорастворимые"),
    ]
    for rx, key, to_val in HINTS:
        for m in rx.finditer(txt):
            ck = key
            val = to_val(m)
            if ck not in found and ck in ALLOWED_SPECS:
                found[ck]=val
            consumed_spans.append((m.start(), m.end()))

    # Вырезаем «потреблённые» куски
    if consumed_spans:
        consumed_spans.sort()
        keep=[]; last=0
        for a,b in consumed_spans:
            if a>last: keep.append(txt[last:a])
            last=b
        keep.append(txt[last:])
        txt=" ".join(keep)
    # Приводим описание к короткому читабельному виду (чуть-чуть)
    sentences = re.split(r"(?<=[.!?])\s+", txt)
    # выбросим очень короткие/служебные/очевидно технические хвосты
    TECH = re.compile(r"(dpi|стр/мин|wi[\s\-]?fi|usb|ethernet|hdmi|mm|см|г/м2|лотк|контейнер|iso/iec)", re.I)
    cleaned=[]
    for s in sentences:
        st=s.strip(" ,;:-")
        if not st: continue
        if TECH.search(st): continue
        cleaned.append(st)
        if len(" ".join(cleaned))>600 or len(cleaned)>=3: break
    short = " ".join(cleaned) if cleaned else ""
    return found, short

# ----------------- Build HTML description -----------------
def build_description_html(name: str, intro_text: str, specs: dict) -> str:
    parts=[f"<h3>{enc_safe(name)}</h3>"]
    if intro_text:
        parts.append(f"<p>{enc_safe(intro_text)}</p>")
    if specs:
        order = ["Тип","Назначение","Тип печати","Цвет печати","Формат","Разрешение","Разрешение печати",
                 "Оптическое разрешение","Скорость печати","Интерфейсы","Wi-Fi","Двусторонняя печать",
                 "Дисплей","Подача бумаги","Тип чернил","Цвета чернил","Совместимость","Ресурс",
                 "Страна происхождения","Гарантия","Автоподатчик","Диагональ","Разрешение экрана","Яркость",
                 "Контрастность","Время отклика","Частота обновления"]
        idx={k:i for i,k in enumerate(order)}
        lines=["<h3>Характеристики</h3>","<ul>"]
        for k in sorted(specs.keys(), key=lambda z: idx.get(z,999)):
            v = specs[k]
            if v:
                v = enc_safe(v)
                lines.append(f'  <li><strong>{k}:</strong> {v}</li>')
        lines.append("</ul>")
        parts.append("\n".join(lines))
    html_out = "\n".join(parts).strip()
    # косметика пробелов
    html_out = re.sub(r"(?is)(<strong>[^<:]+:\s*</strong>)[\s]*", r"\1 ", html_out)
    html_out = re.sub(r"\s{2,}"," ", html_out)
    return html_out

# ----------------- FEED_META (old style) -----------------
def render_feed_meta(pairs):
    try:
        from zoneinfo import ZoneInfo
        tz=ZoneInfo("Asia/Almaty"); now_alm=datetime.now(tz)
    except Exception:
        now_alm=datetime.utcnow()
    # next build at 02:00
    nxt = now_alm.replace(hour=2, minute=0, second=0, microsecond=0)
    if now_alm >= nxt: 
        from datetime import timedelta
        nxt = nxt + timedelta(days=1)
    def fmt(dt): return dt.strftime("%d:%m:%Y - %H:%M:%S")
    rows=[
        ("Поставщик", pairs.get("supplier","")),
        ("URL поставщика", pairs.get("source","")),
        ("Время сборки (Алматы)", fmt(now_alm)),
        ("Ближайшее время сборки (Алматы)", fmt(nxt)),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total","0"))),
        ("Сколько товаров у поставщика после фильтра", str(pairs.get("offers_written","0"))),
        ("Сколько товаров есть в наличии (true)", str(pairs.get("available_true","0"))),
        ("Сколько товаров нет в наличии (false)", str(pairs.get("available_false","0"))),
    ]
    w=max(len(k) for k,_ in rows)
    return "FEED_META\n" + "\n".join(f"{k.ljust(w)} | {v}" for k,v in rows)

# ----------------- Main -----------------
def main():
    log(f"Source: {SUPPLIER_URL}")
    data = fetch_bytes(SUPPLIER_URL)
    root = ET.fromstring(data)
    shop_in = root.find("shop") if root.tag.lower()!="shop" else root
    if shop_in is None: err("<shop> not found")
    offers_in = shop_in.find("offers"); 
    if offers_in is None: err("<offers> not found")
    src_offers = list(offers_in.findall("offer"))

    # Build output skeleton (as old)
    out_root = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root,"shop"); out_offers = ET.SubElement(out_shop,"offers")
    for o in src_offers:
        out_offers.append(deepcopy(o))

    # Filter by <name> (old)
    keys = load_name_filter(AKCENT_KEYWORDS_PATH)
    before = len(list(out_offers.findall("offer"))); hits=0; removed=0
    if AKCENT_KEYWORDS_MODE in {"include","exclude"} and keys:
        for o in list(out_offers.findall("offer")):
            nm = get_text(o,"name")
            ok = name_matches(nm, keys)
            if ok: hits+=1
            drop = (AKCENT_KEYWORDS_MODE=="exclude" and ok) or (AKCENT_KEYWORDS_MODE=="include" and not ok)
            if drop:
                out_offers.remove(o); removed+=1
        log(f"Filter mode: {AKCENT_KEYWORDS_MODE} | Keywords loaded: {len(keys)} | Offers before: {before} | Matched: {hits} | Removed: {removed} | Kept: {before-removed}")
    else:
        log("Filter disabled or empty keywords.")

    # Pricing (old style)
    reprice(out_shop)

    # Availability attribute only + remove child tag
    on,off = set_available_attribute(out_shop)

    # Insert categoryId first
    ensure_categoryid_first(out_shop)

    # Currency
    fix_currency(out_shop)

    # Clean & keep useful Params; collect dict for specs base
    specs_by_offer = {}
    for offer in out_offers.findall("offer"):
        specs_by_offer[id(offer)] = clean_params_in_offer(offer)

    # Build specs from description FIRST, then paraphrase description LAST
    for offer in out_offers.findall("offer"):
        name = get_text(offer,"name") or "Товар"
        dnode = offer.find("description")
        raw_desc = dnode.text if (dnode is not None and dnode.text) else ""

        # (a) Extract from description
        desc_specs, intro_text = extract_specs_from_description(raw_desc)

        # (b) Merge with Param-based specs (Param has priority)
        base = specs_by_offer.get(id(offer), {})
        merged = dict(base)
        for k,v in desc_specs.items():
            if k not in merged and v: merged[k]=v
        # trim lengths & cleanup
        for k in list(merged.keys()):
            v = merged[k] or ""
            if len(v)>MAX_LEN_BY_KEY.get(k,9999):
                v=v[:MAX_LEN_BY_KEY[k]-3]+"..."
            merged[k]=clean_ws(v)

        # (c) Build final HTML description (now we paraphrase LAST)
        html_desc = build_description_html(name, intro_text, merged)

        # write description as CDATA
        if dnode is None: dnode = ET.SubElement(offer,"description")
        dnode.text = f"<![CDATA[\n{html_desc}\n]]>"

    # Reorder children (categoryId first, then old order)
    reorder_offer_children(out_shop)

    # Place <keywords> at the very end
    kw_count = ensure_keywords_last(out_shop)

    # Add separators (empty line between offers)
    children = list(out_offers)
    for i in range(len(children)-1, 0, -1):
        out_offers.insert(i, ET.Comment("OFFSEP"))

    # FEED_META comment (old style)
    meta = {
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "offers_total": len(src_offers),
        "offers_written": len(list(out_offers.findall("offer"))),
        "available_true": str(on),
        "available_false": str(off),
    }
    out_root.insert(0, ET.Comment(render_feed_meta(meta)))

    # Pretty-print & serialize CP1251-safe
    try: ET.indent(out_root, space="  ")
    except Exception: pass
    xml = ET.tostring(out_root, encoding=ENCODING_OUT, xml_declaration=True).decode(ENCODING_OUT, errors="replace")
    xml = re.sub(r"\s*<!--OFFSEP-->\s*", "\n\n  ", xml)
    xml = re.sub(r"(\n[ \t]*){3,}", "\n\n", xml)
    xml = enc_safe(xml)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=ENCODING_OUT, newline="\n") as f: f.write(xml)
    try:
        docs_dir=os.path.dirname(OUT_FILE) or "docs"
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir,".nojekyll"),"wb").close()
    except Exception: pass

    log(f"Wrote: {OUT_FILE} | offers={meta['offers_written']} | keywords={kw_count} | available on/off={on}/{off}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
