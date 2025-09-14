# -*- coding: utf-8 -*-
"""
Alstyle → YML for Satu (плоские <offer> внутри <offers>)
script_version = alstyle-2025-09-14.6

Коротко:
- ЧИТАЕМ официальный XML Alstyle (URL можно переопределить env-переменной SUPPLIER_URL).
- ФИЛЬТРУЕМ ТОЛЬКО по КАТЕГОРИЯМ из docs/alstyle_categories.txt:
  * можно указывать ЧИСЛОВЫЕ ID категорий (включая всех их потомков);
  * можно указывать строки (подстрока, ~=слово, /регулярка/), чтобы матчить ПОЛНЫЕ пути категорий.
- ДАЛЕЕ приводим данные к форматам Satu:
  * vendor (бренд) — чистка «псевдобрендов» и служебных;
  * vendorCode — из артикулов (article/name/url/id) + префикс AS;
  * price/currencyId — считаем розничную цену по таблице наценок, хвост ...900, KZT;
  * available — по ИСТИННОЙ доступности у поставщика (tag→stock→status→param→default);
  * description — в одну строку; «Характеристики» переносим из <param>, затем param удаляем;
  * чистим служебные теги/атрибуты.
- ПИШЕМ ТОЛЬКО docs/alstyle.yml (Windows-1251).
- FEED_META — комментарий в начале файла, аккуратно выровнен на русском.
"""

from __future__ import annotations
import os, sys, re, time, random, urllib.parse
from copy import deepcopy
from typing import Dict, List, Tuple, Optional, Set
from xml.etree import ElementTree as ET
from datetime import datetime, timezone

try:
    # Для корректного времени «Алматы» в FEED_META (Python 3.9+)
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests

# ========================== КОНФИГ ПО УМОЛЧАНИЮ ===========================

SCRIPT_VERSION = "alstyle-2025-09-14.6"  # попадёт в FEED_META

# Основные пути/источник
SUPPLIER_NAME    = os.getenv("SUPPLIER_NAME", "alstyle")
SUPPLIER_URL     = os.getenv(
    "SUPPLIER_URL",
    "https://al-style.kz/upload/catalog_export/al_style_catalog.php"  # официальный экспорт
).strip()
OUT_FILE_YML     = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC              = os.getenv("OUTPUT_ENCODING", "windows-1251")       # итоговая кодировка файла

# Сетевые настройки (на случай падений/долгих ответов)
TIMEOUT_S        = int(os.getenv("TIMEOUT_S", "30"))
RETRIES          = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF    = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))  # минимальный размер, ниже — считаем ошибкой
DRY_RUN          = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# Настройки артикулов
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS")  # префикс для vendorCode
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "1").lower() in {"1","true","yes"}

# Фильтр ТОЛЬКО по категориям (IDs и/или правила по именам)
ALSTYLE_CATEGORIES_PATH = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt")
ALSTYLE_CATEGORIES_MODE = os.getenv("ALSTYLE_CATEGORIES_MODE", "include").lower()  # off|include|exclude
ALSTYLE_CATEGORIES_DEBUG = os.getenv("ALSTYLE_CATEGORIES_DEBUG", "0").lower() in {"1","true","yes"}

# Политика чисток
DROP_CATEGORY_ID_TAG     = True   # удаляем <categoryId> из выхода
DROP_STOCK_TAGS          = True   # удаляем складские теги после вычисления available
PURGE_TAGS_AFTER = (      # лишние/служебные теги для удаления в финале
    "Offer_ID","delivery","local_delivery_cost","manufacturer_warranty","model","url","status","Status"
)
PURGE_OFFER_ATTRS_AFTER = ("type","available","article")  # чистим атрибуты в <offer>

# Закрытые/внутренние ценовые теги — из них собираем входную «закупочную» цену,
# но в финальном YML эти поля удаляем.
INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice"
)

# =============================== УТИЛИТЫ I/O ===============================

def log(msg: str) -> None:
    """Короткий лог в stdout (для GitHub Actions удобно)."""
    print(msg, flush=True)

def warn(msg: str) -> None:
    """Предупреждение в stderr (не валит сборку)."""
    print(f"WARN: {msg}", file=sys.stderr, flush=True)

def err(msg: str, code: int = 1) -> None:
    """Фатальная ошибка: печатаем и выходим с кодом."""
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def now_utc_str() -> str:
    """Текущее время UTC для FEED_META."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def now_almaty_str() -> str:
    """Текущее время Asia/Almaty для FEED_META (если нет zoneinfo — локальное)."""
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def get_text(el: ET.Element, tag: str) -> str:
    """Безопасно получить текст из подузла <tag>; вернуть '' если нет."""
    node = el.find(tag)
    return (node.text or "").strip() if node is not None and node.text else ""

def _norm_text(s: str) -> str:
    """Нормализовать произвольный текст: нижний регистр, убрать множественные пробелы, заменить неразрывные."""
    s = (s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

# ============================= ЗАГРУЗКА ИСТОЧНИКА =========================

def load_source_bytes(src: str) -> bytes:
    """
    Скачивает/читает исходный XML.
    Поддерживает: http(s), file://path, локальный путь.
    Параметры: таймаут/ретраи/минимальный размер.
    """
    if not src:
        raise RuntimeError("SUPPLIER_URL не задан")
    # file://
    if src.startswith("file://"):
        path = src[7:]
        with open(path, "rb") as f:
            data = f.read()
        if len(data) < MIN_BYTES:
            raise RuntimeError(f"file too small: {len(data)} bytes")
        return data
    # локальный файл без схемы
    if "://" not in src:
        with open(src, "rb") as f:
            data = f.read()
        if len(data) < MIN_BYTES:
            raise RuntimeError(f"file too small: {len(data)} bytes")
        return data
    # http/https
    sess = requests.Session()
    headers = {"User-Agent":"supplier-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, RETRIES+1):
        try:
            r = sess.get(src, headers=headers, timeout=TIMEOUT_S, stream=True)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            data = r.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)} bytes)")
            return data
        except Exception as e:
            last_exc = e
            sleep = RETRY_BACKOFF * attempt * (1.0 + random.uniform(-0.2, 0.2))
            warn(f"fetch attempt {attempt}/{RETRIES} failed: {e}; sleep {sleep:.2f}s")
            if attempt < RETRIES:
                time.sleep(sleep)
    raise RuntimeError(f"fetch failed after {RETRIES} attempts: {last_exc}")

# ========================= ПРАВИЛА ДЛЯ КАТЕГОРИЙ ==========================

class CatRule:
    """
    Правило сопоставления категорий «по названию пути».
    kind: 'substr' (подстрока), 'word' (слово, ~=), 'regex' (/.../).
    pattern: готовая регулярка для word/regex; для substr хранится строка.
    """
    __slots__=("raw","kind","pattern")
    def __init__(self, raw: str, kind: str, pattern):
        self.raw, self.kind, self.pattern = raw, kind, pattern

def _norm_cat(s: str) -> str:
    """Унифицировать строку пути категории: любые разделители → ' / ' и убрать лишние пробелы."""
    if not s:
        return ""
    s = s.replace("\u00A0"," ")
    s = re.sub(r"\s*[/>\|]\s*", " / ", s)   # '/', '>', '|' → " / "
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_category_rules(path: str) -> Tuple[Set[str], List[CatRule]]:
    """
    Считать файл правил категорий.
    Возвращает:
      - ids_set: множество строк-чисел (ID категорий);
      - name_rules: список CatRule (подстрока/слово/регекс) для путей категорий.
    Поддержка кодировок: UTF-8(+BOM), UTF-16, Windows-1251 (BOM и нули вычищаем).
    """
    if not path or not os.path.exists(path):
        return set(), []
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
        with open(path,"r",encoding="utf-8",errors="ignore") as f:
            data=f.read().replace("\x00","")

    ids: Set[str] = set()
    rules: List[CatRule] = []
    for ln in data.splitlines():
        s=ln.strip()
        if not s or s.lstrip().startswith("#"):
            continue
        # ЧИСЛОВОЙ ID категории
        if re.fullmatch(r"\d{2,}", s):
            ids.add(s)
            continue
        # Регулярка /.../
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try:
                rules.append(CatRule(s,"regex",re.compile(s[1:-1],re.I)))
                continue
            except Exception:
                continue
        # Целое слово ~=...
        if s.startswith("~="):
            w=_norm_text(s[2:])
            if w:
                rules.append(CatRule(s,"word",re.compile(r"\b"+re.escape(w)+r"\b",re.I)))
            continue
        # Подстрока
        rules.append(CatRule(_norm_text(s),"substr",None))
    return ids, rules

def category_matches_name(path_str: str, rules: List[CatRule]) -> bool:
    """
    Проверить, матчит ли путь категории хотя бы одно «правило по имени».
    path_str — полный путь (например: «Оргтехника / Принтеры / Лазерные»).
    """
    cat_norm = _norm_text(_norm_cat(path_str))
    for cr in rules:
        if cr.kind=="substr":
            if cr.raw and cr.raw in cat_norm:
                return True
        else:
            if cr.pattern and cr.pattern.search(path_str or ""):
                return True
    return False

# ==================== ДЕРЕВО КАТЕГОРИЙ: ID↔родители/дети ==================

def parse_categories_tree(shop_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,Set[str]]]:
    """
    Разобрать <categories><category id=".." parentId="..">Name</category></categories>.
    Возвращает:
      id2name:   {id -> name}
      id2parent: {id -> parentId}
      parent2children: {parentId -> set(childId)}
    """
    id2name: Dict[str,str]={}
    id2parent: Dict[str,str]={}
    parent2children: Dict[str,Set[str]]={}
    cats_root = shop_el.find("categories") or shop_el.find("Categories")
    if cats_root is None:
        return id2name, id2parent, parent2children
    for c in cats_root.findall("category"):
        cid = (c.attrib.get("id") or "").strip()
        if not cid:
            continue
        pid = (c.attrib.get("parentId") or "").strip()
        nm  = (c.text or "").strip()
        id2name[cid]=nm
        if pid:
            id2parent[cid]=pid
        parent2children.setdefault(pid, set()).add(cid)
    return id2name, id2parent, parent2children

def collect_descendants(ids: Set[str], parent2children: Dict[str,Set[str]]) -> Set[str]:
    """
    По множеству ID вернуть их полное множество потомков (включая исходные ID).
    BFS/стек, чтобы охватить все уровни.
    """
    if not ids:
        return set()
    out=set(ids)
    stack=list(ids)
    while stack:
        cur=stack.pop()
        for ch in parent2children.get(cur, ()):
            if ch not in out:
                out.add(ch)
                stack.append(ch)
    return out

def build_category_path_from_id(cat_id: str, id2name: Dict[str,str], id2parent: Dict[str,str]) -> str:
    """
    Восстановить полный путь категории по её ID, поднимаясь к корню:
    «Родитель / Дочерняя / ... / Текущая».
    """
    names=[]; cur=cat_id; seen=set()
    while cur and cur not in seen and cur in id2name:
        seen.add(cur)
        names.append(id2name.get(cur,""))
        cur=id2parent.get(cur,"")
    names=[n for n in names if n]
    return " / ".join(reversed(names)) if names else ""

# ====================== БРЕНДЫ (vendor) / чистка названий ==================

def _norm_key(s: str) -> str:
    """Нормализация строки для сравнения названий брендов."""
    if not s:
        return ""
    s=s.strip().lower().replace("ё","е")
    s=re.sub(r"[-_/]+"," ",s)
    s=re.sub(r"\s+"," ",s)
    return s

# Список поставщиков/служебных «брендов», которые мы удаляем
SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
# Маркеры «неизвестного» бренда
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

def normalize_brand(raw: str) -> str:
    """Вернуть «чистое» значение бренда или пустую строку, если надо удалить."""
    k=_norm_key(raw)
    if (not k) or (k in SUPPLIER_BLOCKLIST):
        return ""
    return raw.strip()

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, Dict[str,int]]:
    """
    Пройти по всем <offer> и:
      - удалить vendor с «плохими» значениями,
      - нормализовать написание валидных.
    Возвращает: (сколько нормализовано, счётчик удалённых по ключу).
    """
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

# ============================== ЦЕНООБРАЗОВАНИЕ ===========================

# Правила наценки: (нижняя_граница, верхняя_граница, +процент, +фикс)
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

# Поля, где может лежать «входная» цена у поставщика (берём минимум)
PRICE_FIELDS=[
    "purchasePrice","purchase_price","wholesalePrice","wholesale_price",
    "opt_price","b2bPrice","b2b_price","price","oldprice"
]

def parse_price_number(raw:str)->Optional[float]:
    """Распарсить число из строки цены (срезать пробелы, «₸», «KZT», запятые)."""
    if raw is None: return None
    s=(raw.strip()
           .replace("\xa0"," ")
           .replace(" ","")
           .replace("KZT","")
           .replace("kzt","")
           .replace("₸","")
           .replace(",","."))
    if not s: return None
    try:
        v=float(s)
        return v if v>0 else None
    except Exception:
        return None

def get_dealer_price(offer:ET.Element)->Optional[float]:
    """Достать минимальную «входную» цену из набора полей + <prices>...</prices>."""
    vals=[]
    for tag in PRICE_FIELDS:
        el=offer.find(tag)
        if el is not None and el.text:
            v=parse_price_number(el.text)
            if v is not None: vals.append(v)
    for prices in list(offer.findall("prices")) + list(offer.findall("Prices")):
        for p in list(prices.findall("price")) + list(prices.findall("Price")):
            v=parse_price_number(p.text or "")
            if v is not None: vals.append(v)
    return min(vals) if vals else None

def _force_tail_900(n:float)->int:
    """Округлить/сместить цену к виду ...900 (маркетинговый «хвост»)."""
    i=int(n)
    k=max(i//1000,0)
    out=k*1000+900
    return out if out>=900 else 900

def compute_retail(dealer:float,rules:List[PriceRule])->Optional[int]:
    """Посчитать розницу по таблице наценок. Вернуть None, если не попали ни в один диапазон."""
    for lo,hi,pct,add in rules:
        if lo<=dealer<=hi:
            val=dealer*(1.0+pct/100.0)+add
            return _force_tail_900(val)
    return None

def reprice_offers(shop_el:ET.Element,rules:List[PriceRule])->Tuple[int,int,int]:
    """
    Проставить/пересчитать <price> и <currencyId>KZT</currencyId>.
    Удаляем внутренние ценовые теги (<prices>, INTERNAL_PRICE_TAGS).
    Возвращает: (updated, skipped, total)
    """
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0)
    updated=skipped=total=0
    for offer in offers_el.findall("offer"):
        total+=1
        dealer=get_dealer_price(offer)
        if dealer is None or dealer<=100:  # отсечь мусор/нулевые
            skipped+=1
            node=offer.find("oldprice")
            if node is not None: offer.remove(node)
            continue
        newp=compute_retail(dealer,rules)
        if newp is None:
            skipped+=1
            node=offer.find("oldprice")
            if node is not None: offer.remove(node)
            continue
        p=offer.find("price") or ET.SubElement(offer,"price")
        p.text=str(int(newp))
        cur=offer.find("currencyId") or ET.SubElement(offer,"currencyId")
        cur.text="KZT"
        for node in list(offer.findall("prices")) + list(offer.findall("Prices")):
            offer.remove(node)
        for tag in INTERNAL_PRICE_TAGS:
            node=offer.find(tag)
            if node is not None: offer.remove(node)
        updated+=1
    return updated,skipped,total

# ===================== ПАРАМЕТРЫ → «ХАРАКТЕРИСТИКИ» =======================

def _key(s:str)->str:
    """Нормализовать ключ параметра для дедупликации."""
    return re.sub(r"\s+"," ",(s or "").strip()).lower()

# Отфильтровываем рекламно-логистические параметры (не товарные характеристики)
EXCLUDE_NAME_RE=re.compile(
    r"(новинк|акци|скидк|уценк|снижена\s*цена|хит продаж|топ продаж|лидер продаж|лучшая цена|"
    r"рекомендуем|подарок|к[еэ]шб[еэ]к|предзаказ|статус|ед(иница)?\s*измерени|базовая единиц|"
    r"vat|ндс|налог|доставк|самовывоз|срок поставки|кредит|рассрочк|наличие\b)",
    re.I
)

def _looks_like_code_value(v:str)->bool:
    """
    Грубая эвристика: «похож ли текст на код/ссылку/артикул».
    Такие значения лучше не тащить в «Характеристики».
    """
    s=(v or "").strip()
    if not s: return True
    if re.search(r"https?://",s,re.I): return True
    clean=re.sub(r"[0-9\-\_/ ]","",s)
    return (len(clean)/max(len(s),1))<0.3

def build_specs_lines(offer:ET.Element)->List[str]:
    """Собрать список строк «- Название: Значение» из <param>/<Param> с фильтрацией и дедупликацией."""
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
    """
    Вставить блок «Характеристики:» в <description> (если есть что вставлять).
    Убираем из описания старые маркеры [SPECS_BEGIN]/[SPECS_END] если встречаются.
    Возвращает: (сколько офферов дополнили, сколько строк всего вставили).
    """
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0)
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
        offers_touched+=1
        lines_total+=len(lines)
    return offers_touched,lines_total

def strip_all_params(shop_el:ET.Element)->int:
    """Полностью удалить <param>/<Param> после внедрения «Характеристик» в описание."""
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    removed=0
    for offer in offers_el.findall("offer"):
        for p in list(offer.findall("param")) + list(offer.findall("Param")):
            offer.remove(p)
            removed+=1
    return removed

# ==================== ОПИСАНИЕ: СЖАТЬ В ОДНУ СТРОКУ =======================

_HTML_NBSP_RE = re.compile(r"&nbsp;", re.I)

def _clean_description_text_one_line(s:str)->str:
    """Убрать лишние переносы/пробелы/неразрывные, вернуть одну аккуратную строку."""
    if not s: 
        return s
    s=s.replace("\r\n","\n").replace("\r","\n").replace("\u00A0"," ")
    s=_HTML_NBSP_RE.sub(" ", s)
    s=re.sub(r"\s+"," ", s)
    return s.strip()

def clean_all_descriptions_one_line(shop_el:ET.Element)->int:
    """Пройтись по всем <description> и привести к «одной строке»."""
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    touched=0
    for offer in offers_el.findall("offer"):
        d=offer.find("description")
        if d is not None and d.text:
            cleaned=_clean_description_text_one_line(d.text)
            if cleaned!=d.text:
                d.text=cleaned
                touched+=1
    return touched

# ======================= ДОСТУПНОСТЬ (available) ==========================

TRUE_WORDS  = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS = {"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}

def _parse_bool_str(s: str) -> Optional[bool]:
    """Понять булево значение из строки (на нескольких языках)."""
    if s is None: 
        return None
    v = _norm_text(s)
    if v in TRUE_WORDS:  
        return True
    if v in FALSE_WORDS: 
        return False
    return None

def _parse_int(s: str) -> Optional[int]:
    """Безопасно распарсить целое из строки (оставляем только знаки и цифры)."""
    if s is None: 
        return None
    t = re.sub(r"[^\d\-]+","", s)
    if t in {"","-","+"}: 
        return None
    try: 
        return int(t)
    except Exception: 
        return None

def derive_available(offer: ET.Element) -> Tuple[bool, str]:
    """
    Вычислить фактическую доступность оффера:
    1) явный <available>...</available> в узле;
    2) количественные поля: <quantity_in_stock|quantity|stock|Stock> > 0;
    3) статус: <status|Status> и/или параметр с именем, содержащим "статус"/"налич";
    4) по умолчанию: False.
    Возвращает (булево, откуда_взято: 'tag'|'stock'|'status'|'default').
    """
    # 1) явный текст в теге <available>
    avail_el = offer.find("available")
    if avail_el is not None and avail_el.text:
        b = _parse_bool_str(avail_el.text)
        if b is not None: 
            return b, "tag"
    # 2) склады
    for tag in ["quantity_in_stock","quantity","stock","Stock"]:
        for node in offer.findall(tag):
            val = _parse_int(node.text or "")
            if val is not None:
                return (val > 0), "stock"
    # 3) статус
    for tag in ["status","Status"]:
        node = offer.find(tag)
        if node is not None and node.text:
            b = _parse_bool_str(node.text)
            if b is not None: 
                return b, "status"
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        nm = (p.attrib.get("name") or "").strip().lower()
        if "статус" in nm or "налич" in nm:
            b = _parse_bool_str(p.text or "")
            if b is not None: 
                return b, "status"
    # 4) по умолчанию
    return False, "default"

def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int,int,int]:
    """
    Проставить единый <available>true|false</available>.
    Удалить атрибут offer[@available] и складские поля, если включена опция.
    Возвращает: (true_cnt, false_cnt, from_stock_cnt, from_status_cnt)
    """
    offers_el = shop_el.find("offers")
    if offers_el is None: 
        return (0,0,0,0)
    true_cnt = false_cnt = from_stock_cnt = from_status_cnt = 0
    for offer in offers_el.findall("offer"):
        b, src = derive_available(offer)
        # убираем одноимённый атрибут из <offer available="...">
        if "available" in offer.attrib: 
            offer.attrib.pop("available", None)
        # записываем как тег
        avail = offer.find("available") or ET.SubElement(offer, "available")
        avail.text = "true" if b else "false"
        if b: 
            true_cnt += 1
        else: 
            false_cnt += 1
        if src == "stock": 
            from_stock_cnt += 1
        if src == "status": 
            from_status_cnt += 1
        # чистим складские поля (чтобы не светить их наружу)
        if DROP_STOCK_TAGS:
            for tag in ["quantity_in_stock","quantity","stock","Stock"]:
                for node in list(offer.findall(tag)): 
                    offer.remove(node)
    return true_cnt, false_cnt, from_stock_cnt, from_status_cnt

# ======================= vendorCode / артикул =============================

ARTICUL_RE=re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)

def _extract_article_from_name(name:str)->str:
    """Попробовать найти похожий на артикул токен в <name>."""
    if not name: 
        return ""
    m=ARTICUL_RE.search(name)
    return (m.group(1) if m else "").upper()

def _extract_article_from_url(url:str)->str:
    """Достать артикулоподобный токен из последнего сегмента URL (если он есть)."""
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
    """Нормализовать артикул: убрать пробелы/символы, заменить длинные тире на дефис, сделать UPPER."""
    s=(s or "").strip()
    if not s: 
        return ""
    s=re.sub(r"[\s_]+","",s).replace("—","-").replace("–","-")
    s=re.sub(r"[^A-Za-z0-9\-]+","",s)
    return s.upper()

def ensure_vendorcode_with_article(shop_el:ET.Element,prefix:str,create_if_missing:bool=False)->Tuple[int,int,int,int]:
    """
    Гарантировать наличие у каждого <offer> тега <vendorCode>:
      - если отсутствует и разрешено create_if_missing — создаём пустой;
      - если пусто/равно префиксу — заполняем из article|name|url|id;
      - в любом случае добавляем префикс (prefix + код).
    Возвращает: (всего_префикснули, создано_узлов, заполнено_из_артикула, остались_пустые).
    """
    offers_el=shop_el.find("offers")
    if offers_el is None: 
        return (0,0,0,0)
    total_prefixed=created=filled_from_art=fixed_bare=0
    for offer in offers_el.findall("offer"):
        vc=offer.find("vendorCode")
        if vc is None:
            if create_if_missing:
                vc=ET.SubElement(offer,"vendorCode")
                vc.text=""
                created+=1
            else:
                continue
        old=(vc.text or "").strip()
        # если пусто — пробуем вытащить артикул из разных мест
        if (old=="") or (old.upper()==prefix.upper()):
            art=_normalize_code(offer.attrib.get("article") or "") \
              or _normalize_code(_extract_article_from_name(get_text(offer,"name"))) \
              or _normalize_code(_extract_article_from_url(get_text(offer,"url"))) \
              or _normalize_code(offer.attrib.get("id") or "")
            if art: 
                vc.text=art
                filled_from_art+=1
            else:   
                fixed_bare+=1
        # добавляем префикс (получается, например, ASL1234)
        vc.text=f"{prefix}{(vc.text or '')}"
        total_prefixed+=1
    return total_prefixed,created,filled_from_art,fixed_bare

# ===================== ЧИСТКА СЛУЖЕБНЫХ ТЕГОВ/АТРИБУТОВ ===================

def purge_offer_tags_and_attrs_after(offer:ET.Element)->Tuple[int,int]:
    """
    Удалить из оффера служебные теги/атрибуты, которые не нужны Satu.
    Возвращает: (сколько_тегов_удалено, сколько_атрибутов_удалено)
    """
    removed_tags=0
    for t in PURGE_TAGS_AFTER:
        for node in list(offer.findall(t)):
            offer.remove(node)
            removed_tags+=1
    removed_attrs=0
    for a in PURGE_OFFER_ATTRS_AFTER:
        if a in offer.attrib:
            offer.attrib.pop(a,None)
            removed_attrs+=1
    return removed_tags,removed_attrs

def count_category_ids(offer_el:ET.Element)->int:
    """Подсчитать, сколько у оффера было тегов categoryId/CategoryId (для метрики FEED_META)."""
    return len(list(offer_el.findall("categoryId"))) + len(list(offer_el.findall("CategoryId")))

# ========================== FEED_META (комментарий) ========================

def render_feed_meta_comment(pairs:Dict[str,str])->str:
    """
    Сформировать выровненный многострочный комментарий FEED_META.
    Слева — ключи (равняем по самому длинному), справа — пояснения на русском.
    """
    order=[
        "script_version","supplier","source","offers_total","offers_written",
        "categories_mode","categories_total","filtered_by_categories",
        "prices_updated","params_removed","vendors_recovered","dropped_top",
        "available_true","available_false","available_from_stock","available_from_status",
        "categoryId_dropped","vendorcodes_filled_from_article","vendorcodes_created",
        "built_utc","built_Asia/Almaty",
    ]
    comments={
        "script_version":"Версия скрипта (для контроля в CI)",
        "supplier":"Метка поставщика",
        "source":"URL исходного XML",
        "offers_total":"Офферов у поставщика до очистки",
        "offers_written":"Офферов записано (после очистки)",
        "categories_mode":"Режим фильтра категорий (off/include/exclude)",
        "categories_total":"Сколько правил категорий загружено",
        "filtered_by_categories":"Сколько офферов отфильтровано по категориям",
        "prices_updated":"Скольким товарам пересчитали price",
        "params_removed":"Сколько строк параметров добавлено в описание",
        "vendors_recovered":"Скольким товарам нормализован/восстановлен vendor",
        "dropped_top":"ТОП часто отброшенных названий бренда",
        "available_true":"Сколько офферов доступны (true)",
        "available_false":"Сколько офферов недоступны (false)",
        "available_from_stock":"Сколько доступностей определено по остаткам",
        "available_from_status":"Сколько доступностей определено по статусу",
        "categoryId_dropped":"Сколько тегов categoryId удалено",
        "vendorcodes_filled_from_article":"Скольким офферам проставили vendorCode из артикула",
        "vendorcodes_created":"Сколько узлов vendorCode было создано",
        "built_utc":"Время сборки (UTC)",
        "built_Asia/Almaty":"Время сборки (Алматы)",
    }
    max_key=max(len(k) for k in order)
    left=[f"{k.ljust(max_key)} = {pairs.get(k,'n/a')}" for k in order]
    max_left=max(len(s) for s in left)
    lines=["FEED_META"]
    for l,k in zip(left,order):
        lines.append(f"{l.ljust(max_left)}  | {comments.get(k,'')}")
    return "\n".join(lines)

def top_dropped(d:Dict[str,int], n:int=10)->str:
    """Первые n «чаще всего удалённых/нормализованных» брендов (для статистики в FEED_META)."""
    items=sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return ",".join(f"{k}:{v}" for k,v in items) if items else "n/a"

# ================================= MAIN ====================================

def main()->None:
    """
    Главная точка входа:
    - загрузка XML;
    - построение дерева категорий;
    - перенос офферов и фильтрация по категориям;
    - чистки и нормализации полей;
    - формирование FEED_META и запись docs/alstyle.yml.
    """
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    # В ряде фидов корень — <yml_catalog>, а в некоторых сразу <shop>
    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: 
        err("XML: <shop> not found")

    cats_el = shop_in.find("categories") or shop_in.find("Categories")
    offers_in_el = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None: 
        err("XML: <offers> not found")
    src_offers=list(offers_in_el.findall("offer"))

    # Дерево категорий для восстановления путей и поиска потомков
    id2name, id2parent, parent2children = parse_categories_tree(shop_in)
    catid_to_drop_total=sum(count_category_ids(o) for o in src_offers)

    # Заготовка выходного фида
    out_root=ET.Element("yml_catalog")
    out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop")
    out_offers=ET.SubElement(out_shop,"offers")

    # Переносим офферы как есть (фильтровать будем уже на копии)
    for o in src_offers:
        out_offers.append(deepcopy(o))

    # --- ФИЛЬТР ТОЛЬКО ПО КАТЕГОРИЯМ ---
    rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH) if ALSTYLE_CATEGORIES_MODE in {"include","exclude"} else (set(),[])
    if ALSTYLE_CATEGORIES_MODE=="include" and not (rules_ids or rules_names):
        err("ALSTYLE_CATEGORIES_MODE=include, но правил категорий не найдено. Проверь docs/alstyle_categories.txt.", 2)

    filtered_by_categories = 0
    if (ALSTYLE_CATEGORIES_MODE in {"include","exclude"}) and (rules_ids or rules_names):
        keep_ids: Set[str] = set(rules_ids)
        # Если есть правила по названиям — найдём все ID, чьи ПОЛНЫЕ пути матчатся
        if rules_names and id2name:
            for cid, nm in id2name.items():
                path = build_category_path_from_id(cid, id2name, id2parent)
                if category_matches_name(path, rules_names):
                    keep_ids.add(cid)
        # Захватываем всех потомков указанных категорий
        if keep_ids and parent2children:
            keep_ids = collect_descendants(keep_ids, parent2children)
        # Фактическая фильтрация офферов по <categoryId>
        for off in list(out_offers.findall("offer")):
            cid = get_text(off, "categoryId")
            hit = (cid in keep_ids) if cid else False
            drop_this = (ALSTYLE_CATEGORIES_MODE=="exclude" and hit) or (ALSTYLE_CATEGORIES_MODE=="include" and not hit)
            if drop_this:
                out_offers.remove(off)
                filtered_by_categories += 1

    # Теперь можно убрать <categoryId> из оставшихся (в Satu не нужен)
    if DROP_CATEGORY_ID_TAG:
        for off in out_offers.findall("offer"):
            for node in list(off.findall("categoryId")) + list(off.findall("CategoryId")):
                off.remove(node)

    # --- НОРМАЛИЗАЦИИ/ЧИСТКИ ---
    norm_cnt, dropped_names = ensure_vendor(out_shop)
    total_prefixed, created_nodes, filled_from_art, fixed_bare = ensure_vendorcode_with_article(
        out_shop, prefix=VENDORCODE_PREFIX, create_if_missing=VENDORCODE_CREATE_IF_MISSING
    )
    upd, skipped, total = reprice_offers(out_shop, PRICING_RULES)
    av_true, av_false, av_from_stock, av_from_status = normalize_available_field(out_shop)
    specs_offers, specs_lines = inject_specs_block(out_shop)
    removed_params = strip_all_params(out_shop)

    # Финальная чистка по спискам PURGE_*
    for off in out_offers.findall("offer"):
        purge_offer_tags_and_attrs_after(off)

    # Вставим пустую строку между офферами для читабельности (через комментарий)
    children=list(out_offers)
    for i in range(len(children)-1, 0, -1):
        out_offers.insert(i, ET.Comment("OFFSEP"))

    # Красиво отформатируем дерево (Python 3.9+)
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    offers_written=len(list(out_offers.findall("offer")))
    meta_pairs={
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL or "file",
        "offers_total": len(src_offers),
        "offers_written": offers_written,
        "categories_mode": ALSTYLE_CATEGORIES_MODE if (rules_ids or rules_names) else "off",
        "categories_total": len(rules_ids) + len(rules_names),
        "filtered_by_categories": filtered_by_categories,
        "prices_updated": upd,
        "params_removed": specs_lines,
        "vendors_recovered": norm_cnt,
        "dropped_top": top_dropped(dropped_names),
        "available_true": av_true,
        "available_false": av_false,
        "available_from_stock": av_from_stock,
        "available_from_status": av_from_status,
        "categoryId_dropped": catid_to_drop_total,
        "vendorcodes_filled_from_article": filled_from_art,
        "vendorcodes_created": created_nodes,
        "built_utc": now_utc_str(),
        "built_Asia/Almaty": now_almaty_str(),
    }
    # Вставляем FEED_META как комментарий в начало
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # Серилизуем в текст, заменим OFFSEP-комментарии на пустые строки
    xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")
    xml_text = re.sub(r"\s*<!--OFFSEP-->\s*", "\n\n  ", xml_text)
    xml_text = re.sub(r"(\n[ \t]*){3,}", "\n\n", xml_text)  # сжать большие пустые блоки

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    # Пишем ТОЛЬКО YML
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
        f.write(xml_text)

    # Для GitHub Pages: не генерировать сайты из docs/
    docs_dir=os.path.dirname(OUT_FILE_YML) or "docs"
    try:
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | offers={offers_written} | encoding={ENC} | script={SCRIPT_VERSION}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
