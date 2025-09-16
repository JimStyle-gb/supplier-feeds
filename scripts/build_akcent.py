# -*- coding: utf-8 -*-
"""
Build Akcent YML (flat <offers>) for Satu — script_version=akcent-2025-09-16.0
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

# ========================== ГЛОБАЛЬНЫЕ НАСТРОЙКИ ===========================

SCRIPT_VERSION = "akcent-2025-09-16.0"  # Версия — чтобы в FEED_META и логах видеть актуальный скрипт

# Источник и выход
SUPPLIER_NAME    = os.getenv("SUPPLIER_NAME", "akcent")
SUPPLIER_URL     = os.getenv("SUPPLIER_URL", "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml")
OUT_FILE_YML     = os.getenv("OUT_FILE", "docs/akcent.yml")
ENC              = os.getenv("OUTPUT_ENCODING", "windows-1251")

# Скачивание
TIMEOUT_S        = int(os.getenv("TIMEOUT_S", "30"))
RETRIES          = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF    = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))

# Поведение
DRY_RUN          = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# vendorCode
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AC")
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "1").lower() in {"1","true","yes"}

# Ключевые слова (фильтр по <name>)
AKCENT_KEYWORDS_PATH  = os.getenv("AKCENT_KEYWORDS_PATH", "docs/akcent_keywords.txt")
AKCENT_KEYWORDS_MODE  = os.getenv("AKCENT_KEYWORDS_MODE", "include").lower()  # include|exclude
AKCENT_KEYWORDS_DEBUG = os.getenv("AKCENT_KEYWORDS_DEBUG", "0").lower() in {"1","true","yes"}
AKCENT_DEBUG_MAX_HITS = int(os.getenv("AKCENT_DEBUG_MAX_HITS", "40"))

# Чистки/удаления
DROP_CATEGORY_ID_TAG     = True
DROP_STOCK_TAGS          = True
PURGE_TAGS_AFTER = ("Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url")
PURGE_OFFER_ATTRS_AFTER = ("type","available","article")

# Внутренние ценовые теги, которые вычищаем
INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice"
)

# ================================ УТИЛИТЫ ==================================

def log(msg: str) -> None:
    """Печатает служебное сообщение (видно в логе GitHub Actions)."""
    print(msg, flush=True)

def warn(msg: str) -> None:
    """Печатает предупреждение (используем для мягких ошибок)."""
    print(f"WARN: {msg}", file=sys.stderr, flush=True)

def err(msg: str, code: int = 1) -> None:
    """Печатает ошибку и завершает процесс кодом `code`."""
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def now_utc_str() -> str:
    """Дата/время в UTC для FEED_META."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def now_almaty_str() -> str:
    """Дата/время в Asia/Almaty для FEED_META."""
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def get_text(el: ET.Element, tag: str) -> str:
    """Возвращает текст дочернего тега `tag` (или пустую строку)."""
    node = el.find(tag)
    return (node.text or "").strip() if node is not None and node.text else ""

def _norm_name(s: str) -> str:
    """Нижний регистр, замена NBSP, схлопывание пробелов — для сравнения по ключам."""
    s = (s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

# ====================== СКАЧИВАНИЕ ИСХОДНОГО XML ==========================

def fetch_xml(url: str, timeout: int, retries: int, backoff: float) -> bytes:
    """HTTP GET с ретраями; минимальная длина для защиты от обрезанных ответов."""
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, retries+1):
        try:
            r = sess.get(url, headers=headers, timeout=timeout, stream=True)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            data = r.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)} bytes)")
            return data
        except Exception as e:
            last_exc = e
            sleep = backoff * attempt * (1.0 + random.uniform(-0.2, 0.2))
            warn(f"fetch attempt {attempt}/{retries} failed: {e}; sleep {sleep:.2f}s")
            if attempt < retries:
                time.sleep(sleep)
    raise RuntimeError(f"fetch failed after {retries} attempts: {last_exc}")

# ==================== КЛЮЧИ ДЛЯ ФИЛЬТРА ПО НАЗВАНИЮ =======================

class KeySpec:
    """Правило ключа: тип (substr/regex/word), исходная строка и re-паттерн (если нужен)."""
    __slots__=("raw","kind","pattern")
    def __init__(self, raw: str, kind: str, pattern):
        self.raw, self.kind, self.pattern = raw, kind, pattern

def load_keywords(path: str) -> List[KeySpec]:
    """Читает ключи из файла любой распространённой кодировкой; поддерживает /regex/ и ~=слово."""
    if not path or not os.path.exists(path):
        return []
    data = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f:
                txt=f.read()
            data = txt.replace("\ufeff","").replace("\x00","")
            break
        except Exception:
            continue
    if data is None:
        try:
            with open(path,"r",encoding="utf-8",errors="ignore") as f:
                data=f.read().replace("\x00","")
        except Exception:
            return []
    keys: List[KeySpec]=[]
    for ln in data.splitlines():
        s=ln.strip()
        if not s or s.lstrip().startswith("#"):
            continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try:
                keys.append(KeySpec(s,"regex",re.compile(s[1:-1],re.I)))
                continue
            except Exception:
                continue
        if s.startswith("~="):
            w=_norm_name(s[2:])
            if not w:
                continue
            keys.append(KeySpec(s,"word",re.compile(r"\b"+re.escape(w)+r"\b",re.I)))
            continue
        keys.append(KeySpec(_norm_name(s),"substr",None))
    return keys

def name_matches(name:str, keys:List[KeySpec]) -> Tuple[bool, Optional[str]]:
    """True/False и какой именно ключ сработал (для отладки)."""
    n=_norm_name(name)
    for ks in keys:
        if ks.kind=="substr":
            if ks.raw and ks.raw in n:
                return True,ks.raw
        else:
            if ks.pattern and ks.pattern.search(name or ""):
                return True,ks.raw
    return False,None

# ============================ НОРМАЛИЗАЦИЯ БРЕНДА ==========================

def _norm_key(s: str) -> str:
    """Нижний регистр, убираем лишние разделители и повторные пробелы — для корректного сравнения."""
    if not s:
        return ""
    s=s.strip().lower().replace("ё","е")
    s=re.sub(r"[-_/]+"," ",s)
    s=re.sub(r"\s+"," ",s)
    return s

SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

def normalize_brand(raw: str) -> str:
    """Пусто, если бренд плохой/служебный; иначе — очищенное исходное значение."""
    k=_norm_key(raw)
    if (not k) or (k in SUPPLIER_BLOCKLIST):
        return ""
    return raw.strip()

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, Dict[str,int]]:
    """Нормализует/удаляет бренды по правилам. Возвращает (нормализовано, топ удалённых)."""
    offers_el=shop_el.find("offers")
    if offers_el is None:
        return 0,{}
    normalized=0
    dropped: Dict[str,int]={}
    for offer in offers_el.findall("offer"):
        ven=offer.find("vendor")
        txt=(ven.text or "").strip() if ven is not None and ven.text else ""
        if txt:
            canon=normalize_brand(txt)
            if any(m in txt.lower() for m in UNKNOWN_VENDOR_MARKERS) or (not canon):
                if ven is not None:
                    offer.remove(ven)
                key=_norm_key(txt)
                if key:
                    dropped[key]=dropped.get(key,0)+1
            elif canon!=txt:
                ven.text=canon
                normalized+=1
    return normalized,dropped

# ============================== ЦЕНООБРАЗОВАНИЕ ============================

PriceRule = Tuple[int,int,float,int]  # (минимум, максимум, надбавка %, фикс. добавка)
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

PRICE_FIELDS=["purchasePrice","purchase_price","wholesalePrice","wholesale_price",
              "opt_price","b2bPrice","b2b_price","price","oldprice"]

def parse_price_number(raw:str)->Optional[float]:
    """Убираем валюта/пробелы/запятые; возвращаем float или None."""
    if raw is None:
        return None
    s=(raw.strip()
         .replace("\xa0"," ")
         .replace(" ","")
         .replace("KZT","").replace("kzt","").replace("₸","")
         .replace(",","."))
    if not s:
        return None
    try:
        v=float(s)
        return v if v>0 else None
    except Exception:
        return None

def get_dealer_price(offer:ET.Element)->Optional[float]:
    """Минимум по одиночным тегам и блокам <prices>/<price>."""
    vals=[]
    for tag in PRICE_FIELDS:
        el=offer.find(tag)
        if el is not None and el.text:
            v=parse_price_number(el.text)
            if v is not None:
                vals.append(v)
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            v=parse_price_number(p.text or "")
            if v is not None:
                vals.append(v)
    return min(vals) if vals else None

def _force_tail_900(n:float)->int:
    """Округляет вниз до ближайшего вида ХХХ900 (минимум 900)."""
    i=int(n)
    k=max(i//1000,0)
    out=k*1000+900
    return out if out>=900 else 900

def compute_retail(dealer:float,rules:List[PriceRule])->Optional[int]:
    """Ищем диапазон, считаем наценку и фикс-добавку, затем приводим к «…900»."""
    for lo,hi,pct,add in rules:
        if lo<=dealer<=hi:
            val=dealer*(1.0+pct/100.0)+add
            return _force_tail_900(val)
    return None

def reprice_offers(shop_el:ET.Element,rules:List[PriceRule])->Tuple[int,int,int]:
    """Пишем <price> и <currencyId>KZT</currencyId>, чистим внутренние ценовые теги."""
    offers_el=shop_el.find("offers")
    if offers_el is None:
        return (0,0,0)
    updated=skipped=total=0
    for offer in offers_el.findall("offer"):
        total+=1
        dealer=get_dealer_price(offer)
        if dealer is None or dealer<=100:
            skipped+=1
            node=offer.find("oldprice")
            if node is not None:
                offer.remove(node)
            continue
        newp=compute_retail(dealer,rules)
        if newp is None:
            skipped+=1
            node=offer.find("oldprice")
            if node is not None:
                offer.remove(node)
            continue
        p=offer.find("price") or ET.SubElement(offer,"price")
        p.text=str(int(newp))
        cur=offer.find("currencyId") or ET.SubElement(offer,"currencyId")
        cur.text="KZT"
        for node in list(offer.findall("prices")) + list(offer.findall("Prices")):
            offer.remove(node)
        for tag in INTERNAL_PRICE_TAGS:
            node=offer.find(tag)
            if node is not None:
                offer.remove(node)
        updated+=1
    return updated,skipped,total

# ===================== ПАРАМЕТРЫ → «ХАРАКТЕРИСТИКИ» =======================

def _key(s:str)->str:
    """Нормализует имя параметра (нижний регистр + схлопывание пробелов)."""
    return re.sub(r"\s+"," ",(s or "").strip()).lower()

EXCLUDE_NAME_RE=re.compile(
    r"(новинк|акци|скидк|уценк|снижена\s*цена|хит продаж|топ продаж|лидер продаж|лучшая цена|"
    r"рекомендуем|подарок|к[еэ]шб[еэ]к|предзаказ|статус|ед(иница)?\s*измерени|базовая единиц|"
    r"vat|ндс|налог|доставк|самовывоз|срок поставки|кредит|рассрочк|наличие\b)", re.I
)

def _looks_like_code_value(v:str)->bool:
    """Если это набор символов, цифр и дефисов/ссылок — не тянем в описание."""
    s=(v or "").strip()
    if not s:
        return True
    if re.search(r"https?://",s,re.I):
        return True
    clean=re.sub(r"[0-9\-\_/ ]","",s)
    return (len(clean)/max(len(s),1))<0.3

def build_specs_lines(offer:ET.Element)->List[str]:
    """Готовит список характеристик из <param>/<Param>, убирая мусор и дубли."""
    lines=[]; seen=set()
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name=(p.attrib.get("name") or "").strip()
        raw_val =(p.text or "").strip()
        if not raw_name or not raw_val:
            continue
        if EXCLUDE_NAME_RE.search(raw_name):
            continue
        if _looks_like_code_value(raw_val):
            continue
        k=_key(raw_name)
        if k in seen:
            continue
        seen.add(k)
        lines.append(f"- {raw_name}: {raw_val}")
    return lines

def inject_specs_block(shop_el:ET.Element)->Tuple[int,int]:
    """Добавляет блок «Характеристики» в описание; возвращает (офферов_затронуто, строк_добавлено)."""
    offers_el=shop_el.find("offers")
    if offers_el is None:
        return (0,0)
    offers_touched=0; lines_total=0
    spec_re=re.compile(r"\[SPECS_BEGIN\].*?\[SPECS_END\]", re.S)
    for offer in offers_el.findall("offer"):
        lines=build_specs_lines(offer)
        if not lines:
            continue
        desc_el=offer.find("description")
        curr=get_text(offer,"description")
        if curr:
            curr=spec_re.sub("",curr).strip()
        block="Характеристики:\n"+"\n".join(lines)
        new_text=(curr+"\n\n"+block).strip() if curr else block
        if desc_el is None:
            desc_el=ET.SubElement(offer,"description")
        desc_el.text=new_text
        offers_touched+=1; lines_total+=len(lines)
    return offers_touched,lines_total

def strip_all_params(shop_el:ET.Element)->int:
    """Удаляет все <param>/<Param> из офферов; возвращает количество удалённых тегов."""
    offers_el=shop_el.find("offers")
    if offers_el is None:
        return 0
    removed=0
    for offer in offers_el.findall("offer"):
        for p in list(offer.findall("param")) + list(offer.findall("Param")):
            offer.remove(p); removed+=1
    return removed

# ==================== ОПИСАНИЯ: СЖАТЬ В ОДНУ СТРОКУ =======================

_HTML_NBSP_RE = re.compile(r"&nbsp;", re.I)

def _clean_description_text_one_line(s:str)->str:
    """NBSP→пробел; любые пробельные последовательности (вкл. \\n, \\t) → один пробел."""
    if not s:
        return s
    s=s.replace("\r\n","\n").replace("\r","\n").replace("\u00A0"," ")
    s=_HTML_NBSP_RE.sub(" ", s)
    s=re.sub(r"\s+", " ", s)
    return s.strip()

def clean_all_descriptions_one_line(shop_el:ET.Element)->int:
    """Делает все <description> «в одну строку»; возвращает число изменённых офферов."""
    offers_el=shop_el.find("offers")
    if offers_el is None:
        return 0
    touched=0
    for offer in offers_el.findall("offer"):
        d=offer.find("description")
        if d is not None and d.text:
            cleaned=_clean_description_text_one_line(d.text)
            if cleaned!=d.text:
                d.text=cleaned; touched+=1
    return touched

# =========================== НАЛИЧИЕ/СКЛАД =================================

def normalize_stock_always_true(shop_el:ET.Element)->int:
    """Гарантирует <available>true</available> и убирает складские теги."""
    offers_el=shop_el.find("offers")
    if offers_el is None:
        return 0
    touched=0
    for offer in offers_el.findall("offer"):
        avail=offer.find("available") or ET.SubElement(offer,"available")
        avail.text="true"; touched+=1
        if DROP_STOCK_TAGS:
            for tag in ["quantity_in_stock","quantity","stock","Stock"]:
                for node in list(offer.findall(tag)):
                    offer.remove(node)
    return touched

# ======================= vendorCode / артикул ==============================

ARTICUL_RE=re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)

def _extract_article_from_name(name:str)->str:
    """Ищет похожий на артикул фрагмент в <name>."""
    if not name:
        return ""
    m=ARTICUL_RE.search(name)
    return (m.group(1) if m else "").upper()

def _extract_article_from_url(url:str)->str:
    """Берёт последний сегмент URL без .html/.php и т.п.; ищет там артикул."""
    if not url:
        return ""
    try:
        path=urllib.parse.urlparse(url).path.rstrip("/")
        last=path.split("/")[-1]
        last=re.sub(r"\.(html?|php|aspx?)$","",last,flags=re.I)
        m=ARTICUL_RE.search(last)
        return (m.group(1) if m else last).upper()
    except Exception:
        return ""

def _normalize_code(s:str)->str:
    """Удаляем пробелы/подчёркивания/«длинные тире», оставляем A-Z/0-9/дефис; UPPERCASE."""
    s=(s or "").strip()
    if not s:
        return ""
    s=re.sub(r"[\s_]+","",s)
    s=s.replace("—","-").replace("–","-")
    s=re.sub(r"[^A-Za-z0-9\-]+","",s)
    return s.upper()

def ensure_vendorcode_with_article(shop_el:ET.Element,prefix:str,create_if_missing:bool=False)->Tuple[int,int,int,int]:
    """Создаёт/заполняет vendorCode из артикулов и добавляет префикс (AC)."""
    offers_el=shop_el.find("offers")
    if offers_el is None:
        return (0,0,0,0)
    total_prefixed=created=filled_from_art=fixed_bare=0
    for offer in offers_el.findall("offer"):
        vc=offer.find("vendorCode")
        if vc is None:
            if create_if_missing:
                vc=ET.SubElement(offer,"vendorCode"); vc.text=""; created+=1
            else:
                continue
        old=(vc.text or "").strip()
        if (old=="") or (old.upper()==prefix.upper()):
            art=_normalize_code(offer.attrib.get("article") or "") \
              or _normalize_code(_extract_article_from_name(get_text(offer,"name"))) \
              or _normalize_code(_extract_article_from_url(get_text(offer,"url"))) \
              or _normalize_code(offer.attrib.get("id") or "")
            if art:
                vc.text=art; filled_from_art+=1
            else:
                # если артикул так и не нашли — оставим пустой суффикс (префикс всё равно добавим ниже)
                pass
        vc.text=f"{prefix}{(vc.text or '')}"; total_prefixed+=1
    return total_prefixed,created,filled_from_art,0

# ================= ФИНАЛЬНАЯ ЧИСТКА ТЕГОВ/АТРИБУТОВ =======================

def purge_offer_tags_and_attrs_after(offer:ET.Element)->Tuple[int,int]:
    """Чистим служебные теги и атрибуты у <offer> (они не нужны Satu)."""
    removed_tags=0
    for t in PURGE_TAGS_AFTER:
        for node in list(offer.findall(t)):
            offer.remove(node); removed_tags+=1
    removed_attrs=0
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib:
            offer.attrib.pop(a,None); removed_attrs+=1
    return removed_tags,removed_attrs

def count_category_ids(offer_el:ET.Element)->int:
    """Подсчитывает теги categoryId/CategoryId в оффере."""
    return len(list(offer_el.findall("categoryId"))) + len(list(offer_el.findall("CategoryId")))

# =========================== FEED_META (комментарий) =======================

def render_feed_meta_comment(pairs:Dict[str,str])->str:
    """Формирует две колонки: «ключ = значение» и «комментарий» (колонка комментариев строго под '|')."""
    order=[
        "script_version","supplier","source","offers_total","offers_written",
        "keywords_mode","keywords_total","filtered_by_keywords",
        "prices_updated","params_removed","vendors_recovered","dropped_top",
        "available_forced","categoryId_dropped",
        "vendorcodes_filled_from_article","vendorcodes_created",
        "built_utc","built_Asia/Almaty",
    ]
    comments={
        "script_version":"Версия скрипта (для контроля в CI)",
        "supplier":"Метка поставщика",
        "source":"URL исходного XML",
        "offers_total":"Офферов у поставщика до очистки",
        "offers_written":"Офферов записано (после очистки)",
        "keywords_mode":"Режим фильтра (include/exclude)",
        "keywords_total":"Сколько ключей загружено",
        "filtered_by_keywords":"Сколько офферов отфильтровано по keywords",
        "prices_updated":"Скольким товарам пересчитали price",
        "params_removed":"Сколько строк параметров добавлено в описание",
        "vendors_recovered":"Скольким товарам нормализован/восстановлен vendor",
        "dropped_top":"ТОП часто отброшенных названий бренда",
        "available_forced":"Сколько офферов получили available=true",
        "categoryId_dropped":"Сколько тегов categoryId удалено",
        "vendorcodes_filled_from_article":"Скольким офферам проставили vendorCode из артикула",
        "vendorcodes_created":"Сколько узлов vendorCode было создано",
        "built_utc":"Время сборки (UTC)",
        "built_Asia/Almaty":"Время сборки (Алматы)",
    }
    max_key = max(len(k) for k in order)
    left_parts = [f"{k.ljust(max_key)} = {pairs.get(k,'n/a')}" for k in order]
    max_left = max(len(s) for s in left_parts)
    lines=["FEED_META"]
    for left, k in zip(left_parts, order):
        comment = comments.get(k, "")
        lines.append(f"{left.ljust(max_left)}  | {comment}")
    return "\n".join(lines)

def top_dropped(d:Dict[str,int], n:int=10)->str:
    """Возвращает строку вида 'brand:count,brand2:count2' (первые n)."""
    items=sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return ",".join(f"{k}:{v}" for k,v in items) if items else "n/a"

# ================================= MAIN ====================================

def main()->None:
    """Скачиваем → фильтруем → нормализуем → считаем цены → пишем YML."""
    log(f"Source: {SUPPLIER_URL}")
    data=fetch_xml(SUPPLIER_URL, TIMEOUT_S, RETRIES, RETRY_BACKOFF)
    src_root=ET.fromstring(data)

    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None:
        err("XML: <shop> not found")

    offers_in=shop_in.find("offers") or shop_in.find("Offers")
    if offers_in is None:
        err("XML: <offers> not found")

    src_offers=list(offers_in.findall("offer"))
    catid_to_drop_total=sum(count_category_ids(o) for o in src_offers)

    out_root=ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop"); out_offers=ET.SubElement(out_shop,"offers")

    # Переносим офферы и сразу снимаем categoryId
    for o in src_offers:
        mod=deepcopy(o)
        if DROP_CATEGORY_ID_TAG:
            for node in list(mod.findall("categoryId")) + list(mod.findall("CategoryId")):
                mod.remove(node)
        out_offers.append(mod)

    # Фильтр по <name> согласно файлу ключей (include/exclude)
    keys=load_keywords(AKCENT_KEYWORDS_PATH)
    if AKCENT_KEYWORDS_MODE=="include" and len(keys)==0:
        err("AKCENT_KEYWORDS_MODE=include, но ключей не найдено. Проверь docs/akcent_keywords.txt.", 2)

    filtered_out=0
    if (AKCENT_KEYWORDS_MODE in {"include","exclude"}) and len(keys)>0:
        for off in list(out_offers.findall("offer")):
            nm=get_text(off,"name")
            hit,_=name_matches(nm,keys)
            drop_this=(AKCENT_KEYWORDS_MODE=="exclude" and hit) or (AKCENT_KEYWORDS_MODE=="include" and not hit)
            if drop_this:
                out_offers.remove(off); filtered_out+=1

    # Нормализуем бренды → vendorCode → цены
    norm_cnt, dropped_names = ensure_vendor(out_shop)
    total_prefixed, created_nodes, filled_from_art, fixed_bare = ensure_vendorcode_with_article(
        out_shop, prefix=VENDORCODE_PREFIX, create_if_missing=VENDORCODE_CREATE_IF_MISSING
    )
    upd, skipped, total = reprice_offers(out_shop, PRICING_RULES)

    # Параметры → описание; затем описание сжимаем; затем убираем сами <param>
    specs_offers, specs_lines = inject_specs_block(out_shop)
    removed_params = strip_all_params(out_shop)
    cleaned_desc = clean_all_descriptions_one_line(out_shop)

    # Доступность (у Akcent все true)
    available_forced = normalize_stock_always_true(out_shop)

    # Финальная чистка лишних тегов/атрибутов
    for off in out_offers.findall("offer"):
        purge_offer_tags_and_attrs_after(off)

    # Пустая строка между офферами (для читаемости)
    children = list(out_offers)
    for i in range(len(children)-1, 0, -1):
        out_offers.insert(i, ET.Comment("OFFSEP"))

    # Красивый отступ (если поддерживается)
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # FEED_META
    offers_written=len(list(out_offers.findall("offer")))
    meta_pairs={
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "offers_total": len(src_offers),
        "offers_written": offers_written,
        "keywords_mode": AKCENT_KEYWORDS_MODE if len(keys)>0 else "off",
        "keywords_total": len(keys),
        "filtered_by_keywords": filtered_out,
        "prices_updated": upd,
        "params_removed": specs_lines,
        "vendors_recovered": norm_cnt,
        "dropped_top": top_dropped(dropped_names),
        "available_forced": available_forced,
        "categoryId_dropped": catid_to_drop_total,
        "vendorcodes_filled_from_article": filled_from_art,
        "vendorcodes_created": created_nodes,
        "built_utc": now_utc_str(),
        "built_Asia/Almaty": now_almaty_str(),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # Сериализация → текст и постобработка OFFSEP в пустые строки
    xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")
    xml_text = re.sub(r"\s*<!--OFFSEP-->\s*", "\n\n  ", xml_text)
    xml_text = re.sub(r"(\n[ \t]*){3,}", "\n\n", xml_text)
    # ВАЖНО: разрыв строки между концом комментария и <shop> (фикс '... (Алматы)--><shop>')
    xml_text = re.sub(r"(-->)\s*(<shop>)", lambda m: f"{m.group(1)}\n  {m.group(2)}", xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    # Запись ТОЛЬКО YML
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
        f.write(xml_text)

    # .nojekyll для GitHub Pages
    docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
    try:
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | offers={offers_written} | encoding={ENC} | script={SCRIPT_VERSION} | desc_cleaned={cleaned_desc}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
