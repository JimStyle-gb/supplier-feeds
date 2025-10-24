#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, re, time, html, requests
from copy import deepcopy
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET

# =========================
# Константы / окружение
# =========================
SUPPLIER_NAME = "Akcent"
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml").strip()
OUT_FILE      = os.getenv("OUT_FILE", "docs/akcent.yml").strip()
ENCODING_OUT  = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()  # CP1251 по умолчанию

AKCENT_KEYWORDS_PATH = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt")
AKCENT_KEYWORDS_MODE = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()  # include|exclude

TIMEOUT = 35
RETRIES = 4

PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "9999999"))
PRICE_CAP_VALUE     = 100

# Наценки (минимум — как договорились раньше)
PRICING_RULES = [
    (101,10000,4.0,3000),(10001,25000,4.0,4000),(25001,50000,4.0,5000),
    (50001,75000,4.0,7000),(75001,100000,4.0,10000),(100001,150000,4.0,12000),
    (150001,200000,4.0,15000),(200001,300000,4.0,20000),(300001,400000,4.0,25000),
    (400001,500000,4.0,30000),(500001,750000,4.0,40000),(750001,1000000,4.0,50000),
    (1000001,1500000,4.0,70000),(1500001,2000000,4.0,90000),(2000001,100000000,4.0,100000),
]

# =========================
# Утилиты
# =========================
def log(msg): print(msg, flush=True)
def err(msg, code=1): print(f"ERROR: {msg}", file=sys.stderr, flush=True); sys.exit(code)

def now_alm():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Almaty"))
    except Exception:
        return datetime.utcnow()

def next_build_alm():
    cur = now_alm()
    nxt = cur.replace(hour=1, minute=0, second=0, microsecond=0)
    return (nxt + timedelta(days=1)) if cur >= nxt else nxt

def enc_safe(s: str, enc: str) -> str:
    if not s: return s
    repl = {
        "\u2026":"...", "\u2014":"-", "\u2013":"-", "\u2212":"-",
        "\u2122":"", "\u2265":">=", "\u2264":"<=", "\u2009":" ", "\u200A":" ",
        "\u200B":"", "\u2018":"'", "\u2019":"'", "\u201C":'"', "\u201D":'"', "\u00A0":" ",
    }
    for a,b in repl.items(): s = s.replace(a,b)
    try:
        s.encode(enc)
        return s
    except Exception:
        return s.encode(enc, errors="ignore").decode(enc, errors="ignore")

def get_text(el, tag):
    n = el.find(tag)
    return (n.text or "").strip() if n is not None and n.text else ""

def inner_html(el):
    if el is None: return ""
    parts=[]
    if el.text: parts.append(el.text)
    for c in el:
        parts.append(ET.tostring(c, encoding="unicode"))
        if c.tail: parts.append(c.tail)
    return "".join(parts).strip()

def strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", " ", s or "", flags=re.S)

# =========================
# Загрузка XML
# =========================
def load_bytes(url: str) -> bytes:
    sess = requests.Session()
    last = None
    for i in range(RETRIES):
        try:
            r = sess.get(url, timeout=TIMEOUT)
            if r.status_code != 200: raise RuntimeError(f"HTTP {r.status_code}")
            if len(r.content) < 1000: raise RuntimeError("too small")
            return r.content
        except Exception as e:
            last = e
            time.sleep(1.5*(i+1))
    raise RuntimeError(str(last))

# =========================
# Фильтр по <name>
# =========================
def _norm_key(s): return re.sub(r"\s+"," ", (s or "").lower().replace("ё","е")).strip()
class KeySpec:
    __slots__=("raw","kind","norm","pat")
    def __init__(self, raw, kind, norm=None, pat=None): self.raw, self.kind, self.norm, self.pat = raw, kind, norm, pat

def load_name_filter(path):
    if not os.path.exists(path): return []
    # пробуем разные кодировки
    txt = None
    for enc in ("utf-8-sig","utf-8","cp1251","utf-16","utf-16-le","utf-16-be"):
        try:
            txt = open(path,"r",encoding=enc).read()
            break
        except Exception: pass
    if txt is None:
        txt = open(path,"r",encoding="utf-8",errors="ignore").read()
    keys=[]
    for ln in txt.splitlines():
        s = ln.strip()
        if not s or s.startswith("#"): continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try: keys.append(KeySpec(s,"regex", None, re.compile(s[1:-1], re.I)))
            except Exception: pass
        else:
            keys.append(KeySpec(s,"prefix", _norm_key(s)))
    return keys

def name_matches(name: str, keys) -> bool:
    nm = name or ""
    base = _norm_key(nm)
    for ks in keys:
        if ks.kind=="prefix" and base.startswith(ks.norm): return True
        if ks.kind=="regex" and ks.pat and ks.pat.search(nm): return True
    return False

# =========================
# Наличие / валюта / порядок
# =========================
TRUE_WORDS = {"true","1","yes","y","да","есть","available","в наличии"}
FALSE_WORDS= {"false","0","no","n","нет","под заказ","ожидается"}

def _bool_from_str(s):
    if s is None: return None
    v = (s or "").strip().lower()
    if v in TRUE_WORDS: return True
    if v in FALSE_WORDS: return False
    return None

def derive_available(offer) -> bool:
    # 1) читаем АТРИБУТ <offer available="...">
    if "available" in offer.attrib:
        b = _bool_from_str(offer.attrib.get("available"))
        if b is not None: return b
    # 2) дочерний <available> (если вдруг есть)
    n = offer.find("available")
    if n is not None and n.text:
        b = _bool_from_str(n.text)
        if b is not None: return b
    # 3) quantity / status
    for t in ("quantity_in_stock","quantity","stock","Stock"):
        qn = offer.find(t)
        if qn is not None and qn.text:
            try: 
                return int(re.sub(r"[^\d\-]+","",qn.text))>0
            except: pass
    for t in ("status","Status"):
        sn = offer.find(t)
        if sn is not None and sn.text:
            b = _bool_from_str(sn.text)
            if b is not None: return b
    # по умолчанию — False
    return False

def normalize_available_field(shop):
    offers = shop.find("offers") or ET.Element("offers")
    on=off=0
    for o in offers.findall("offer"):
        b = derive_available(o)
        # фиксируем только атрибут
        o.attrib["available"] = "true" if b else "false"
        # дочерний тег вычищаем
        for n in list(o.findall("available")): o.remove(n)
        if b: on+=1
        else: off+=1
    return on, off

def fix_currency(shop):
    offers = shop.find("offers") or ET.Element("offers")
    k=0
    for o in offers.findall("offer"):
        for n in list(o.findall("currencyId")): o.remove(n)
        ET.SubElement(o,"currencyId").text="KZT"
        k+=1
    return k

def ensure_categoryid_zero_first(shop):
    offers = shop.find("offers") or ET.Element("offers")
    for o in offers.findall("offer"):
        for n in list(o.findall("categoryId"))+list(o.findall("CategoryId")):
            o.remove(n)
        cid = ET.Element("categoryId"); cid.text = os.getenv("CATEGORY_ID_DEFAULT","0")
        o.insert(0, cid)

def reorder_offer_children(shop):
    DESIRED = ["vendorCode","name","price","picture","vendor","currencyId","description"]
    offers = shop.find("offers") or ET.Element("offers")
    for o in offers.findall("offer"):
        ch = list(o)
        buckets = {t:[] for t in DESIRED}; others=[]
        for n in ch:
            (buckets[n.tag] if n.tag in buckets else others).append(n)
        rebuilt = sum((buckets[t] for t in DESIRED), []) + others
        if rebuilt != ch:
            for n in ch: o.remove(n)
            for n in rebuilt: o.append(n)

# =========================
# Цена
# =========================
PRICE_TAGS_INTERNAL = ("purchase_price","purchasePrice","wholesale_price","wholesalePrice","opt_price","optPrice",
                       "b2b_price","b2bPrice","supplier_price","supplierPrice","min_price","minPrice","max_price","maxPrice","oldprice")

def parse_num(s):
    if not s: return None
    t = s.replace("\xa0"," ").replace(" ","").replace("KZT","").replace("₸","").replace(",",".")
    try:
        v=float(t)
        return v if v>0 else None
    except: return None

def pick_dealer_price(o):
    vals=[]
    for t in PRICE_TAGS_INTERNAL:
        n=o.find(t)
        if n is not None and n.text:
            v=parse_num(n.text)
            if v: vals.append(v)
    return min(vals) if vals else None

def force_tail_900(n):
    n=int(n)
    return (n//1000)*1000+900 if n>=0 else 900

def compute_retail(d):
    for lo,hi,pct,add in PRICING_RULES:
        if lo<=d<=hi:
            return force_tail_900(d*(1+pct/100.0)+add)
    return None

def strip_supplier_price_blocks(o):
    for t in PRICE_TAGS_INTERNAL:
        for n in list(o.findall(t)): o.remove(n)
    for t in ("prices","Prices"):
        for n in list(o.findall(t)): o.remove(n)

def reprice(shop):
    offers = shop.find("offers") or ET.Element("offers")
    for o in offers.findall("offer"):
        # треш цены — принудительно ставим 100
        pnode = o.find("price")
        if pnode is not None and pnode.text:
            try:
                src = float(pnode.text.replace(",","."))
                if src >= PRICE_CAP_THRESHOLD:
                    for n in list(o.findall("price")): o.remove(n)
                    ET.SubElement(o,"price").text = str(PRICE_CAP_VALUE)
                    strip_supplier_price_blocks(o)
                    continue
            except: pass

        d = pick_dealer_price(o)
        if not d:
            strip_supplier_price_blocks(o)
            continue
        rr = compute_retail(d)
        if not rr:
            strip_supplier_price_blocks(o)
            continue
        for n in list(o.findall("price")): o.remove(n)
        ET.SubElement(o,"price").text = str(int(rr))
        strip_supplier_price_blocks(o)

# =========================
# Описание / Характеристики
# =========================
ALLOWED_TAGS = ("h3","p","ul","ol","li","br","strong","em","b","i")
DROP_SECTION_HEADS = re.compile(
    r"^\s*(состав поставки|комплект поставки|основные (свойства|характеристики)|технические характеристики)\b",
    re.I
)

def unescape_maybe(s: str)->str:
    if not s: return s
    if re.search(r"&lt;/?[a-zA-Z]", s):
        for _ in range(2):
            s = html.unescape(s)
            if not re.search(r"&lt;/?[a-zA-Z]", s): break
    return s

def sanitize_supplier_html(raw_html: str)->str:
    s = raw_html or ""
    s = unescape_maybe(s)
    # убрать скрипты/картинки/таблицы/ссылки/стили
    s = re.sub(r"<(script|style|iframe|object|embed|noscript)[^>]*>.*?</\1>", " ", s, flags=re.I|re.S)
    s = re.sub(r"</?(table|thead|tbody|tr|td|th|img)[^>]*>", " ", s, flags=re.I|re.S)
    s = re.sub(r"<a\b[^>]*>.*?</a>", " ", s, flags=re.I|re.S)
    # заголовки → h3, div → p, убрать span
    s = re.sub(r"<h[1-6]\b[^>]*>", "<h3>", s, flags=re.I); s=re.sub(r"</h[1-6]>", "</h3>", s, flags=re.I)
    s = re.sub(r"<div\b[^>]*>", "<p>", s, flags=re.I); s=re.sub(r"</div>", "</p>", s, flags=re.I)
    s = re.sub(r"</?span\b[^>]*>", " ", s, flags=re.I)
    # чистка атрибутов
    s = re.sub(r"\s(style|class|id|width|height|align)\s*=\s*(['\"]).*?\2", "", s, flags=re.I)
    # двойные <br> → новый параграф
    s = re.sub(r"(?:\s*<br\s*/?>\s*){2,}", "</p><p>", s, flags=re.I)
    # удалить URL'ы и CTA «подробнее/читать далее...»
    s = re.sub(r"\bhttps?://\S+|www\.\S+", "", s, flags=re.I)
    s = re.sub(r"<p>\s*(подробнее|читать далее|узнать больше|подробности|смотреть на сайте)\.?\s*</p>", "", s, flags=re.I)
    # убрать пустые контейнеры
    s = re.sub(r"<(p|li|ul|ol)>\s*</\1>", "", s, flags=re.I)
    # финальная нормализация пробелов
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def wrap_bare_lines_to_paragraphs(html_in: str)->str:
    if not html_in.strip(): return ""
    lines = html_in.splitlines()
    out=[]; buf=[]
    def flush():
        if not buf: return
        txt=" ".join(x.strip() for x in buf if x.strip())
        if txt: out.append(f"<p>{txt}</p>")
        buf.clear()
    for ln in lines:
        s=ln.strip()
        if not s: flush(); continue
        if s.startswith("<"): flush(); out.append(ln)
        else: buf.append(s)
    flush()
    res="\n".join(out)
    if not re.search(r"<(p|ul|ol|h3)\b", res, flags=re.I):
        res=f"<p>{strip_html(html_in).strip()}</p>"
    return res

# Карта канонических ключей
CANON_MAP = {
    "тип":"Тип","вид":"Тип","назначение":"Назначение",
    "тип печати":"Тип печати","цвет печати":"Цвет печати","цветность":"Цвет печати",
    "формат":"Формат","формат бумаги":"Формат",
    "разрешение":"Разрешение","разрешение печати":"Разрешение печати",
    "оптическое разрешение":"Оптическое разрешение","разрешение сканера":"Оптическое разрешение","разрешение сканера,dpi":"Оптическое разрешение",
    "скорость печати":"Скорость печати","интерфейсы":"Интерфейсы","интерфейс":"Интерфейсы","входы":"Интерфейсы",
    "wi-fi":"Wi-Fi","дисплей":"Дисплей","двусторонняя печать":"Двусторонняя печать",
    "подача бумаги":"Подача бумаги","тип чернил":"Тип чернил","цвета чернил":"Цвета чернил",
    "страна происхождения":"Страна происхождения","гарантия":"Гарантия","автоподатчик":"Автоподатчик",
    "совместимость":"Совместимость","совместимые продукты":"Совместимость",
    "ресурс":"Ресурс","объем":"Объем","объём":"Объем",
    "сканирование":"Сканирование","копирование":"Копирование",
}

ALLOWED_SPECS = set(CANON_MAP.values())

def canon_key(k: str):
    if not k: return None
    return CANON_MAP.get(k.strip().lower().replace("ё","е"))

def clean_ws(s: str) -> str:
    return re.sub(r"\s+"," ", (s or "").replace("\u00A0"," ").replace("&nbsp;"," ")).strip(" \t,:;.-")

# Интерфейсы
IFACE_PAT = [
    (re.compile(r"\bwi[\s\-]?fi\s*direct\b", re.I), "Wi-Fi Direct"),
    (re.compile(r"\bwi[\s\-]?fi\b", re.I),          "Wi-Fi"),
    (re.compile(r"\bethernet\b", re.I),             "Ethernet"),
    (re.compile(r"\brj[\s\-]?45\b", re.I),          "RJ-45"),
    (re.compile(r"\bbluetooth\b", re.I),            "Bluetooth"),
    (re.compile(r"\bhdmi\b", re.I),                 "HDMI"),
    (re.compile(r"\bdisplay\s*port|displayport\b", re.I), "DisplayPort"),
    (re.compile(r"\busb[\s\-]?host\b", re.I),       "USB-host"),
    (re.compile(r"\bsd[\s\-]?карта|\bsd\b", re.I),  "SD-карта"),
    (re.compile(r"\busb\b", re.I),                  "USB"),
    (re.compile(r"\bтип\s*b\b", re.I),              "USB (тип B)"),
]
IFACE_ORDER = ["USB","USB (тип B)","USB-host","Wi-Fi","Wi-Fi Direct","Ethernet","Bluetooth","HDMI","DisplayPort","SD-карта"]

def extract_ifaces(text: str):
    s = clean_ws(text)
    found=[]
    for pat,label in IFACE_PAT:
        if pat.search(s): found.append(label)
    # порядок и уникальность
    uniq=[]
    for k in IFACE_ORDER:
        if k in found and k not in uniq: uniq.append(k)
    return ", ".join(uniq)

def yesno(s):
    v=(s or "").strip().lower()
    if v in {"да","есть","yes","true"}: return "Да"
    if v in {"нет","no","false","отсутствует"}: return "Нет"
    return clean_ws(s)

def norm_resolution_print(s):
    t = s.replace("x","×").replace("X","×")
    t = re.sub(r"(?<=\d)[\.,](?=\d{3}\b)","", t)
    m = re.search(r"(\d{2,5})\s*×\s*(\d{2,5})", t)
    return (m.group(1)+"×"+m.group(2)+" dpi") if m else ""

def norm_resolution_display(s):
    t = s.replace("x","×").replace("X","×")
    m = re.search(r"(\d{3,5})\s*×\s*(\d{3,5})", t)
    return (m.group(1)+"×"+m.group(2)) if m else ""

def norm_speed(s):
    m = re.search(r"(\d{1,3})\s*(стр|pages)", s, re.I)
    if m: return f"до {m.group(1)} стр/мин"
    m2 = re.match(r"^\s*(\d{1,3})\s*$", s)
    return f"до {m2.group(1)} стр/мин" if m2 else ""

def norm_format(s):
    t=s.replace("бумаги","")
    t=re.sub(r"\s*[xX]\s*","×", t)
    parts=[p.strip() for p in re.split(r"[;,]+", t) if p.strip()]
    keep=[]
    for p in parts:
        p=re.sub(r"\s*×\s*","×", p)
        if re.fullmatch(r"(A\d|B\d|C6|DL|Letter|Legal|No\.?\s*10|\d{1,2}×\d{1,2}|10×15|13×18|9×13|16:9)", p, re.I):
            keep.append(p)
    if keep:
        if len(keep)>10: keep=keep[:10]+["и др."]
        return ", ".join(keep)
    return ""

def norm_value_by_key(key, val, kind="device"):
    k=key.lower()
    if k=="разрешение печати":     return norm_resolution_print(val)
    if k=="разрешение":            return norm_resolution_display(val)
    if k=="интерфейсы" or k=="входы": return extract_ifaces(val)
    if k=="wi-fi":                 return yesno(val)
    if k=="формат":                return norm_format(val)
    if k=="дисплей":               return "Да" if yesno(val)=="Да" else clean_ws(val)
    if k=="скорость печати":       return norm_speed(val)
    if k=="двусторонняя печать":   return yesno(val)
    if k=="оптическое разрешение": return norm_resolution_print(val)
    if k=="гарантия":
        m=re.search(r"(\d{1,3})", val); return f"{m.group(1)} мес" if m else clean_ws(val)
    return clean_ws(val)

# извлечение блока <h3>Характеристики</h3> если он в сыром HTML уже был
def remove_existing_specs_block(html_in: str):
    specs={}
    m=re.search(r"(?is)(<h3>\s*Характеристики\s*</h3>\s*<ul>(.*?)</ul>)", html_in)
    if not m: return specs, html_in
    ul = m.group(2)
    for li in re.finditer(r"(?is)<li>\s*<strong>([^:<]+):\s*</strong>\s*(.*?)\s*</li>", ul):
        k=li.group(1).strip(); v=li.group(2).strip()
        if k and v: specs[k]=v
    cleaned = html_in[:m.start()] + html_in[m.end():]
    return specs, cleaned

# вытягивание техфраз из маркетингового блока
def extract_specs_from_marketing(mark_html: str):
    text = strip_html(mark_html)
    specs = {}
    # формат
    m = re.search(r"\bформат[а]?[\s:]*((?:A\d|Letter|Legal|10\s*×?\s*15|13\s*×?\s*18|9\s*×?\s*13|16:9))", text, re.I)
    if m: specs["Формат"]=norm_format(m.group(1))
    # разрешение (dpi)
    m = re.search(r"(\d{2,5})\s*[x×]\s*(\d{2,5})\s*(dpi)?", text, re.I)
    if m:
        val = f"{m.group(1)}×{m.group(2)}"
        if m.group(3): specs["Разрешение печати"]=norm_resolution_print(val)
        else: specs["Оптическое разрешение"]=norm_resolution_print(val)
    # скорость печати
    m = re.search(r"(?:скорост[ьи]\s*печати.*?|\bдо\b)\s*(\d{1,3})\s*(?:стр|pages)", text, re.I)
    if m: specs["Скорость печати"]=f"до {m.group(1)} стр/мин"
    # интерфейсы
    iface = extract_ifaces(text)
    if iface: specs["Интерфейсы"]=iface
    if "Wi-Fi" in iface: specs["Wi-Fi"]="Да"
    # тип чернил
    if re.search(r"пигментн\w+", text, re.I) and re.search(r"водорастворим\w+", text, re.I):
        specs["Тип чернил"]="водорастворимые и пигментные"
    elif re.search(r"пигментн\w+", text, re.I):
        specs["Тип чернил"]="пигментные"
    elif re.search(r"водорастворим\w+", text, re.I):
        specs["Тип чернил"]="водорастворимые"
    # гарантия
    m = re.search(r"гаранти\w*\s*(\d{1,3})\s*мес", text, re.I)
    if m: specs["Гарантия"]=f"{m.group(1)} мес"
    # ресурс
    m = re.search(r"ресурс[^.\n]*?(\d[\d\s]{0,7})\s*(стр|страниц|мл)", text, re.I)
    if m:
        qty = re.sub(r"\s+","", m.group(1))
        unit = "стр" if m.group(2).lower().startswith("стр") else "мл"
        specs["Ресурс"]=f"{qty} {unit}"
    # сканер/копир
    if re.search(r"\bсканер\b", text, re.I): specs["Сканирование"]="Да"
    if re.search(r"\bкопир|\bкопирован", text, re.I): specs["Копирование"]="Да"
    # двусторонняя печать
    if re.search(r"\bдвусторонн\w+\s*печать\b", text, re.I): specs["Двусторонняя печать"]="Да"

    # вырезаем из маркетинга явные «технические» хвосты
    cleaned = mark_html
    # убрать строки-параграфы, начинающиеся на маркеры «служебных» разделов
    cleaned = re.sub(r"(?is)<p>\s*("+DROP_SECTION_HEADS.pattern+r").*?</p>", "", cleaned)
    # убрать фразы с форматами, dpi, ресурсами, гарантией, скоростью и т.п.
    drop_patterns = [
        r"\bформат[а]?\s*A\d\b.*",
        r"\bформат[а]?.*(Letter|Legal|10\s*×?\s*15|13\s*×?\s*18|9\s*×?\s*13|16:9).*",
        r"\d{2,5}\s*[x×]\s*\d{2,5}\s*(dpi)?",
        r"\bскорост[ьи]\s*печати.*",
        r"\bwi[\s\-]?fi(\s*direct)?\b.*",
        r"\bethernet\b.*",
        r"\bbluetooth\b.*",
        r"\bdisplay\s*port|displayport\b.*",
        r"\busb(\s*host)?\b.*",
        r"\bгаранти\w*\s*\d{1,3}\s*мес.*",
        r"\bресурс[^.\n]*?(стр|страниц|мл).*",
        r"\bпигментн\w+.*|водорастворим\w+.*",
        r"\bдвусторонн\w+\s*печать.*",
        r"\bпринтер,?\s*сканер,?\s*копир.*",
        r"\bсостав поставки.*|комплект поставки.*|технические характеристики.*|основные характеристики.*",
    ]
    def drop_in_p(m):
        inner=m.group(1)
        t=strip_html(inner).strip()
        for dp in drop_patterns:
            if re.search(dp, t, re.I):
                return ""  # выкидываем весь параграф
        return m.group(0)
    cleaned = re.sub(r"(?is)<p>(.*?)</p>", drop_in_p, cleaned)

    return specs, cleaned

# сбор параметров поставщика → канон
DISALLOWED_PARAM_NAMES = {"сопутствующие товары","бренд","brand","manufacturer","vendor","поставщик","для бренда","наименование производителя"}
MAX_LEN_BY_KEY = {
    "Тип":40,"Назначение":120,"Тип печати":20,"Цвет печати":20,"Формат":120,
    "Разрешение":30,"Разрешение печати":30,"Оптическое разрешение":30,
    "Скорость печати":30,"Интерфейсы":120,"Дисплей":20,"Wi-Fi":5,
    "Двусторонняя печать":5,"Подача бумаги":40,"Тип чернил":60,"Цвета чернил":120,
    "Страна происхождения":40,"Гарантия":20,"Автоподатчик":5,"Совместимость":400,
    "Ресурс":60,"Сканирование":5,"Копирование":5,"Объем":40
}

def collect_params_canonical(offer, vendor_text: str):
    merged={}
    for tag in ("Param","param","PARAM"):
        for p in offer.findall(tag):
            raw_k = (p.get("name") or p.get("Name") or "").strip()
            raw_v = (p.text or "").strip()
            if not raw_k or not raw_v: continue
            if raw_k.strip().lower() in DISALLOWED_PARAM_NAMES: continue
            ck = canon_key(raw_k)
            if not ck: continue
            v = norm_value_by_key(ck, raw_v)
            if not v: continue
            old = merged.get(ck,"")
            if len(v)>len(old): merged[ck]=v

    # если нет бренда в теге vendor — сохраняем «Производитель» как параметр
    if not vendor_text:
        # ищем «Производитель» у поставщика
        for tag in ("Param","param","PARAM"):
            for p in offer.findall(tag):
                if (p.get("name") or p.get("Name") or "").strip().lower()=="производитель":
                    val = clean_ws(p.text or "")
                    if val: merged["Производитель"]=val

    # привести Да/Нет и чутка подрезать длины
    out={}
    for k,v in merged.items():
        if k in {"Wi-Fi","Дисплей","Двусторонняя печать","Автоподатчик","Сканирование","Копирование"}:
            v=yesno(v)
        if k in MAX_LEN_BY_KEY and len(v)>MAX_LEN_BY_KEY[k]:
            v=v[:MAX_LEN_BY_KEY[k]-3]+"..."
        out[k]=clean_ws(v)
    return out

def fix_li_spacing(html_in: str) -> str:
    if not html_in: return html_in
    # один пробел после </strong>
    out = re.sub(r"(?is)(<strong>[^<:]+:\s*</strong>)[\s]*", r"\1 ", html_in)
    # убрать двойные пробелы/знаки
    out = re.sub(r"\s{2,}"," ", out)
    out = re.sub(r"\s+</li>", "</li>", out)
    return out

# =========================
# Ключевые слова
# =========================
def build_keywords(offer):
    name = get_text(offer,"name")
    vendor= get_text(offer,"vendor")
    geo = ["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
           "Оскемен","Семей","Костанаи","Кызылорда","Орал","Петропавл","Талдыкорган","Актау","Темиртау","Экибастуз","Кокшетау"]
    words=[]
    if vendor: words.append(vendor)
    # модели
    for t in re.findall(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}", name or ""):
        if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t):
            up=t.upper()
            if up not in words: words.append(up)
        if len(words)>=10: break
    words += geo[:20]
    txt = ", ".join(words)
    return txt[:1024]

def ensure_keywords(shop):
    k=0
    offers = shop.find("offers") or ET.Element("offers")
    for o in offers.findall("offer"):
        kw = build_keywords(o)
        node = o.find("keywords")
        if node is None:
            ET.SubElement(o,"keywords").text = kw
            k+=1
        else:
            if (node.text or "") != kw:
                node.text = kw; k+=1
    return k

# =========================
# feed_meta (старый стиль)
# =========================
def render_feed_meta(pairs):
    rows = [
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
    w = max(len(k) for k,_ in rows)
    return "FEED_META\n" + "\n".join(f"{k.ljust(w)} | {v}" for k,v in rows)

# =========================
# MAIN
# =========================
def main():
    log("Run set -e                       # прерывать шаг при любой ошибке")
    log(f"Python {sys.version.split()[0]}")
    log(f"Source: {SUPPLIER_URL}")

    data = load_bytes(SUPPLIER_URL)
    root = ET.fromstring(data)
    shop_in = root.find("shop") if root.tag.lower()!="shop" else root
    if shop_in is None: err("<shop> not found")
    offers_in = shop_in.find("offers")
    if offers_in is None: err("<offers> not found")
    src_offers = list(offers_in.findall("offer"))

    out_root = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")
    out_offers = ET.SubElement(out_shop, "offers")

    for o in src_offers:
        out_offers.append(deepcopy(o))

    # Фильтр по <name>
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
        log("Filter disabled")

    # Наличие/цены/валюта/порядок
    t_true, t_false = normalize_available_field(out_shop)
    reprice(out_shop)
    fix_currency(out_shop)
    ensure_categoryid_zero_first(out_shop)
    reorder_offer_children(out_shop)

    # Нормализуем <name> (мелкие правки)
    for o in out_offers.findall("offer"):
        n = o.find("name")
        if n is not None and n.text:
            t = n.text.replace("\u00A0"," ")
            t = re.sub(r"\s{2,}"," ", t)
            t = re.sub(r"(?<=\d)\s*[xX]\s*(?=\d)", "×", t)
            t = re.sub(r"\b(Wi)[\s\-]?Fi\b","Wi-Fi", t, flags=re.I)
            n.text = t.strip()

    # Описание + Характеристики + Параметры
    desc_changed=0
    for offer in out_offers.findall("offer"):
        name = get_text(offer,"name")
        vendor = get_text(offer,"vendor")
        dnode = offer.find("description")
        raw = inner_html(dnode)

        # 1) очистка
        s = sanitize_supplier_html(raw)
        s = wrap_bare_lines_to_paragraphs(s)

        # 2) срежем существующий блок «Характеристики» если был
        specs_from_block, s_wo_specs = remove_existing_specs_block(s)

        # 3) из маркетинга выдернем техфразы
        specs_from_mark, marketing_html = extract_specs_from_marketing(s_wo_specs)

        # маркетинговый абзац: первый <p> и не длиннее 700 символов
        short_p = ""
        m = re.search(r"(?is)<p>(.*?)</p>", marketing_html)
        if m:
            txt = strip_html(m.group(1)).strip()
            if len(txt)>700: txt = txt[:700].rsplit(" ",1)[0]+"..."
            if txt: short_p = f"<p>{html.escape(txt)}</p>"

        # 4) параметры поставщика → канон
        specs_from_param = collect_params_canonical(offer, vendor)

        # 5) слить всё и нормализовать
        merged={}
        for src in (specs_from_block, specs_from_mark, specs_from_param):
            for k,v in src.items():
                if k not in ALLOWED_SPECS: continue
                vv = norm_value_by_key(k, v)
                if not vv: continue
                old = merged.get(k,"")
                if len(vv)>len(old): merged[k]=vv

        # финальные правки + обрезки
        final={}
        for k,v in merged.items():
            vv = v
            if k in {"Wi-Fi","Дисплей","Двусторонняя печать","Автоподатчик","Сканирование","Копирование"}:
                vv = yesno(vv)
            if k in {"Разрешение","Разрешение печати","Оптическое разрешение"} and not re.search(r"\b\d{2,5}\s*×\s*\d{2,5}", vv):
                continue
            if k=="Скорость печати":
                vv = norm_speed(vv)
                if not vv: continue
            if k=="Интерфейсы":
                vv = extract_ifaces(vv)
                if not vv: continue
            if k=="Формат":
                vv = norm_format(vv)
                if not vv: continue
            vv = clean_ws(vv)
            if k in MAX_LEN_BY_KEY and len(vv)>MAX_LEN_BY_KEY[k]:
                vv = vv[:MAX_LEN_BY_KEY[k]-3]+"..."
            if vv: final[k]=vv

        # 6) собрать HTML
        parts = [f"<h3>{html.escape(name)}</h3>"]
        if short_p: parts.append(short_p)
        if final:
            order = ["Тип","Назначение","Тип печати","Цвет печати","Формат","Разрешение","Разрешение печати",
                     "Оптическое разрешение","Скорость печати","Интерфейсы","Wi-Fi","Двусторонняя печать",
                     "Дисплей","Подача бумаги","Тип чернил","Цвета чернил","Сканирование","Копирование",
                     "Совместимость","Ресурс","Страна происхождения","Гарантия","Автоподатчик","Объем"]
            idx = {k:i for i,k in enumerate(order)}
            lines = ["<h3>Характеристики</h3>","<ul>"]
            for k,v in sorted(final.items(), key=lambda kv: idx.get(kv[0],999)):
                lines.append(f'  <li><strong>{html.escape(k)}:</strong> {html.escape(v)}</li>')
            lines.append("</ul>")
            parts.append("\n".join(lines))
        html_out = "\n".join([p for p in parts if p]).strip()
        html_out = fix_li_spacing(html_out)

        # 7) перезаписать description
        if dnode is None:
            dnode = ET.SubElement(offer,"description")
        dnode.text = f"<![CDATA[\n{html_out}\n]]>"
        desc_changed += 1

        # 8) перезаписать параметры для SATU (как <Param>)
        for tag in ("Param","param","PARAM"):
            for p in list(offer.findall(tag)): offer.remove(p)
        for k,v in final.items():
            p = ET.SubElement(offer,"Param"); p.set("name", k); p.text = v

    # keywords
    kw = ensure_keywords(out_shop); log(f"Keywords updated: {kw}")
    log(f"Descriptions rebuilt: {desc_changed}")

    # feed_meta комментарий
    meta = {
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "built_alm": now_alm().strftime("%d:%m:%Y - %H:%M:%S"),
        "next_build_alm": next_build_alm().strftime("%d:%m:%Y - %H:%M:%S"),
        "seo_last_update_alm": now_alm().strftime("%d:%m:%Y - %H:%M:%S"),
        "offers_total": len(src_offers),
        "offers_written": len(list(out_offers.findall("offer"))),
        "available_true": str(len([o for o in out_offers.findall("offer") if o.attrib.get("available")=="true"])),
        "available_false": str(len([o for o in out_offers.findall("offer") if o.attrib.get("available")=="false"])),
    }
    out_root.insert(0, ET.Comment(render_feed_meta(meta)))

    # pretty
    try: ET.indent(out_root, space="  ")
    except Exception: pass

    xml_unicode = ET.tostring(out_root, encoding="unicode")

    # CDATA уже в тексте, оставляем как есть; добавим пустые строки между офферами
    xml_unicode = re.sub(r"(</offer>)\s*\n\s*(<offer\b)", r"\1\n\n\2", xml_unicode)
    xml_unicode = re.sub(r"(?s)(-->)\s*(<shop\b)", r"\1\n\2", xml_unicode)

    # CP1251-safe
    xml_unicode = enc_safe(xml_unicode, ENCODING_OUT)
    xml_decl = f'<?xml version="1.0" encoding="{ENCODING_OUT}"?>\n'
    xml_out = xml_decl + xml_unicode

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=ENCODING_OUT, newline="\n") as f:
        f.write(xml_out)

    # .nojekyll на GitHub Pages
    try:
        d=os.path.dirname(OUT_FILE) or "docs"
        os.makedirs(d, exist_ok=True)
        open(os.path.join(d,".nojekyll"),"wb").close()
    except Exception:
        pass

    log(f"Wrote: {OUT_FILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
