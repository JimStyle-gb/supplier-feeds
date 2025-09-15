# -*- coding: utf-8 -*-
"""
Alstyle → YML for Satu (плоские <offer> внутри <offers>)
script_version = alstyle-2025-09-15.4

Кратко, что делает скрипт:
- Скачивает исходный XML Alstyle (по умолчанию из SUPPLIER_URL).
- Фильтрует ТОЛЬКО по категориям из docs/alstyle_categories.txt (режим include|exclude).
- Нормализует бренд (<vendor>), формирует <vendorCode> из артикула (name/url/id) с префиксом AS.
- Пересчитывает цены по правилам и округляет «хвостом» ...900; currencyId=KZT.
- Определяет доступность <available> по складу/статусу.
- Переносит параметры в блок «Характеристики:» и затем удаляет <param>/<Param>.
  └ Исключения: НЕ включаем строки «Артикул» и «Благотворительность».
  └ Дополнительно: вырезаем из description любые уже имеющиеся строки вида
    «- Артикул: …» и «- Благотворительность: …».
- Описания приводим в одну строку (убираем лишние пробелы/переводы).
- FEED_META (комментарий в начале) выровнен и на русском.
- Выход: ТОЛЬКО docs/alstyle.yml (windows-1251).
"""

from __future__ import annotations
import os, sys, re, time, random, urllib.parse
from copy import deepcopy
from typing import Dict, List, Tuple, Optional, Set
from xml.etree import ElementTree as ET
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo  # для вывода времени Алматы
except Exception:
    ZoneInfo = None

import requests  # для загрузки XML по URL

# ========================== КОНСТАНТЫ И НАСТРОЙКИ ==========================

SCRIPT_VERSION = "alstyle-2025-09-15.4"

# Источник и выходные файлы
SUPPLIER_NAME    = os.getenv("SUPPLIER_NAME", "alstyle")
SUPPLIER_URL     = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML     = os.getenv("OUT_FILE", "docs/alstyle.yml")      # Выходной YML (только он)
ENC              = os.getenv("OUTPUT_ENCODING", "windows-1251")   # Кодировка выхода

# Сеть/повторы
TIMEOUT_S        = int(os.getenv("TIMEOUT_S", "30"))
RETRIES          = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF    = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN          = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# Фильтр ТОЛЬКО по категориям (ID и/или правила по названию)
ALSTYLE_CATEGORIES_PATH  = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt")
ALSTYLE_CATEGORIES_MODE  = os.getenv("ALSTYLE_CATEGORIES_MODE", "include").lower()  # off|include|exclude
ALSTYLE_CATEGORIES_DEBUG = os.getenv("ALSTYLE_CATEGORIES_DEBUG", "0").lower() in {"1","true","yes"}

# Чистка служебных тегов/атрибутов (после обработки)
DROP_CATEGORY_ID_TAG = True        # убираем <categoryId> в финальном YML
DROP_STOCK_TAGS      = True        # убираем <Stock>/<quantity> и т.п. после высчета available
PURGE_TAGS_AFTER = (               # эти теги нам не нужны в выходе
    "Offer_ID", "delivery", "local_delivery_cost", "manufacturer_warranty",
    "model", "url", "status", "Status",
)
PURGE_OFFER_ATTRS_AFTER = ("type", "available", "article")  # удаляем эти атрибуты у <offer>

# Внутренние ценовые поля, которые удаляем после перерасчета
INTERNAL_PRICE_TAGS = (
    "purchase_price","purchasePrice","wholesale_price","wholesalePrice",
    "opt_price","optPrice","b2b_price","b2bPrice","supplier_price","supplierPrice",
    "min_price","minPrice","max_price","maxPrice","oldprice"
)

# =============================== УТИЛИТЫ ЛОГОВ =============================

def log(msg: str) -> None:
    """Печать обычного лога (stdout)."""
    print(msg, flush=True)

def warn(msg: str) -> None:
    """Печать предупреждения (stderr)."""
    print(f"WARN: {msg}", file=sys.stderr, flush=True)

def err(msg: str, code: int = 1) -> None:
    """Печать ошибки и выход."""
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def now_utc_str() -> str:
    """Текущее время в UTC (строкой)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def now_almaty_str() -> str:
    """Текущее время в Алматы (строкой)."""
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def get_text(el: ET.Element, tag: str) -> str:
    """Безопасно получить .text у дочернего тега."""
    node = el.find(tag)
    return (node.text or "").strip() if node is not None and node.text else ""

def _norm_text(s: str) -> str:
    """Нормализовать текст для сравнения: убрать множественные пробелы, привести к нижнему регистру, e→ё."""
    s = (s or "").replace("\u00A0"," ").lower().replace("ё","е")
    return re.sub(r"\s+"," ",s).strip()

# ============================= ЗАГРУЗКА ИСТОЧНИКА ==========================

def load_source_bytes(src: str) -> bytes:
    """
    Загрузка исходного XML:
    - file://…  → читаем локальный файл
    - путь без схемы → читаем локальный файл
    - http(s)      → скачиваем с повторами
    """
    if not src:
        raise RuntimeError("SUPPLIER_URL не задан")
    # Локальный файл через file://
    if src.startswith("file://"):
        with open(src[7:], "rb") as f:
            data = f.read()
        if len(data) < MIN_BYTES:
            raise RuntimeError(f"file too small: {len(data)} bytes")
        return data
    # Локальный путь
    if "://" not in src:
        with open(src, "rb") as f:
            data = f.read()
        if len(data) < MIN_BYTES:
            raise RuntimeError(f"file too small: {len(data)} bytes")
        return data
    # HTTP(S)
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

# ======================== ПРАВИЛА ФИЛЬТРА КАТЕГОРИЙ ========================

class CatRule:
    """Одна строка правила фильтра категорий (подстрока/regex/word)."""
    __slots__=("raw","kind","pattern")
    def __init__(self, raw: str, kind: str, pattern):
        self.raw, self.kind, self.pattern = raw, kind, pattern

def _norm_cat(s: str) -> str:
    """Нормализуем отображение пути категории 'A / B / C'."""
    if not s: return ""
    s = s.replace("\u00A0"," ")
    s = re.sub(r"\s*[/>\|]\s*", " / ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_category_rules(path: str) -> Tuple[Set[str], List[CatRule]]:
    """
    Читает rules из файла категорий:
    - Чистые цифры → ID категорий
    - /regex/      → регулярка по названию пути
    - ~=слово      → слово целиком
    - иначе        → подстрока (без регистра)
    """
    if not path or not os.path.exists(path):
        return set(), []
    data = None
    # Пытаемся разными кодировками (на случай Windows-1251/UTF-16)
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path,"r",encoding=enc) as f:
                txt = f.read()
            data = txt.replace("\ufeff","").replace("\x00","")
            break
        except Exception:
            continue
    if data is None:
        with open(path,"r",encoding="utf-8",errors="ignore") as f:
            data = f.read().replace("\x00","")
    ids: Set[str] = set()
    rules: List[CatRule] = []
    for ln in data.splitlines():
        s = ln.strip()
        if not s or s.lstrip().startswith("#"):  # пустое/комментарий
            continue
        if re.fullmatch(r"\d{2,}", s):
            ids.add(s); continue
        if len(s)>=2 and s[0]=="/" and s[-1]=="/":
            try:
                rules.append(CatRule(s,"regex",re.compile(s[1:-1],re.I))); continue
            except Exception:
                continue
        if s.startswith("~="):
            w=_norm_text(s[2:])
            if w:
                rules.append(CatRule(s,"word", re.compile(r"\b"+re.escape(w)+r"\b",re.I)))
            continue
        rules.append(CatRule(_norm_text(s),"substr",None))
    return ids, rules

def category_matches_name(path_str: str, rules: List[CatRule]) -> bool:
    """Проверка: путь категории удовлетворяет хотя бы одному правилу по названию."""
    cat_norm = _norm_text(_norm_cat(path_str))
    for cr in rules:
        if cr.kind == "substr":
            if cr.raw and cr.raw in cat_norm:
                return True
        else:
            if cr.pattern and cr.pattern.search(path_str or ""):
                return True
    return False

# ====================== ДЕРЕВО КАТЕГОРИЙ (родители/дети) ===================

def parse_categories_tree(shop_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,Set[str]]]:
    """Парсит дерево категорий: id→name, id→parent, parent→children."""
    id2name: Dict[str,str]={}
    id2parent: Dict[str,str]={}
    parent2children: Dict[str,Set[str]]={}
    cats_root = shop_el.find("categories") or shop_el.find("Categories")
    if cats_root is None:
        return id2name, id2parent, parent2children
    for c in cats_root.findall("category"):
        cid = (c.attrib.get("id") or "").strip()
        if not cid: continue
        pid = (c.attrib.get("parentId") or "").strip()
        nm  = (c.text or "").strip()
        id2name[cid]=nm
        if pid: id2parent[cid]=pid
        parent2children.setdefault(pid, set()).add(cid)
    return id2name, id2parent, parent2children

def collect_descendants(ids: Set[str], parent2children: Dict[str,Set[str]]) -> Set[str]:
    """Расширяет множество id всеми потомками (вниз по дереву)."""
    if not ids: return set()
    out=set(ids); stack=list(ids)
    while stack:
        cur=stack.pop()
        for ch in parent2children.get(cur, ()):
            if ch not in out:
                out.add(ch); stack.append(ch)
    return out

def build_category_path_from_id(cat_id: str, id2name: Dict[str,str], id2parent: Dict[str,str]) -> str:
    """Строит строку пути вида 'Root / Sub / Leaf' для id категории."""
    names=[]; cur=cat_id; seen=set()
    while cur and cur not in seen and cur in id2name:
        seen.add(cur); names.append(id2name.get(cur,"")); cur=id2parent.get(cur,"")
    names=[n for n in names if n]
    return " / ".join(reversed(names)) if names else ""

# ===================== НОРМАЛИЗАЦИЯ БРЕНДА (vendor) =======================

def _norm_key(s: str) -> str:
    """Нормализация строки для ключа: нижний регистр, один пробел, без разделителей."""
    if not s: return ""
    s=s.strip().lower().replace("ё","е")
    s=re.sub(r"[-_/]+"," ",s)
    s=re.sub(r"\s+"," ",s)
    return s

# Не допускаем «брендами» названия поставщиков
SUPPLIER_BLOCKLIST={_norm_key(x) for x in["alstyle","al-style","copyline","akcent","ak-cent","vtt"]}
UNKNOWN_VENDOR_MARKERS=("неизвест","unknown","без бренда","no brand","noname","no-name","n/a")

def normalize_brand(raw: str) -> str:
    """Почистить бренд: убрать служебные и пустые/«unknown» варианты."""
    k=_norm_key(raw)
    if (not k) or (k in SUPPLIER_BLOCKLIST):
        return ""
    return raw.strip()

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, Dict[str,int]]:
    """
    Нормализуем <vendor> во всех офферах:
    - пустые/unknown удаляем;
    - служебные (из blocklist) удаляем;
    - если привели к канону — считаем normalized.
    """
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0,{}
    normalized=0; dropped: Dict[str,int]={}
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

# (min, max, markup_percent, fixed_add)
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

# Поля, из которых берём «закупочную/базовую» цену (берём минимум)
PRICE_FIELDS=["purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price","price","oldprice"]

def parse_price_number(raw:str)->Optional[float]:
    """Нормализуем цену из текста в число (убираем валюту, пробелы, запятые)."""
    if raw is None: return None
    s=(raw.strip()
          .replace("\xa0"," ")
          .replace(" ","")
          .replace("KZT","").replace("kzt","").replace("₸","")
          .replace(",","."))
    if not s: return None
    try:
        v=float(s)
        return v if v>0 else None
    except Exception:
        return None

def get_dealer_price(offer:ET.Element)->Optional[float]:
    """Ищем минимальную известную цену в наборе полей + блоках <prices>."""
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
    """Округлить до тысяч с «хвостом» 900 (минимум 900)."""
    i=int(n)
    k=max(i//1000,0)
    out=k*1000+900
    return out if out>=900 else 900

def compute_retail(dealer:float,rules:List[PriceRule])->Optional[int]:
    """Посчитать розничную по таблице наценок + фикс, затем округлить «...900»."""
    for lo,hi,pct,add in rules:
        if lo<=dealer<=hi:
            val=dealer*(1.0+pct/100.0)+add
            return _force_tail_900(val)
    return None

def reprice_offers(shop_el:ET.Element,rules:List[PriceRule])->Tuple[int,int,int]:
    """
    Пересчитываем цену во всех офферах:
    - price → новая розничная
    - currencyId → KZT
    - удаляем служебные ценовые поля и блоки <prices>
    Возвращает: (updated, skipped, total)
    """
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0)
    updated=skipped=total=0
    for offer in offers_el.findall("offer"):
        total+=1
        dealer=get_dealer_price(offer)
        if dealer is None or dealer<=100:
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

# ===== ПАРАМЕТРЫ → «Характеристики» (без Артикул/Благотворительность) ======

def _key(s:str)->str:
    """Ключ для дедупликации имён параметров («в одну строку», нижний регистр)."""
    return re.sub(r"\s+"," ",(s or "").strip()).lower()

# Исключаем рекламные/логистические поля + ровно «Артикул» и «Благотворительность»
EXCLUDE_NAME_RE=re.compile(
    r"(новинк|акци|скидк|уценк|снижена\s*цена|хит продаж|топ продаж|лидер продаж|лучшая цена|"
    r"рекомендуем|подарок|к[еэ]шб[еэ]к|предзаказ|статус|ед(иница)?\s*измерени|базовая единиц|"
    r"vat|ндс|налог|доставк|самовывоз|срок поставки|кредит|рассрочк|наличие\b|^артикул\b|^благотворительн)",
    re.I
)

def _looks_like_code_value(v:str)->bool:
    """
    Очень «похожее на код/ссылку» значение (почти одни цифры/символы) — пропускаем,
    чтобы не засорять «Характеристики».
    """
    s=(v or "").strip()
    if not s: return True
    if re.search(r"https?://",s,re.I): return True
    clean=re.sub(r"[0-9\-\_/ ]","",s)
    return (len(clean)/max(len(s),1))<0.3

def build_specs_lines(offer:ET.Element)->List[str]:
    """Собираем список строк «- Имя: Значение» для «Характеристики:» с фильтрами."""
    lines=[]; seen=set()
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        raw_name=(p.attrib.get("name") or "").strip()
        raw_val =(p.text or "").strip()
        if not raw_name or not raw_val: continue
        if EXCLUDE_NAME_RE.search(raw_name): continue  # тут отсекаем Артикул/Благотворительность
        if _looks_like_code_value(raw_val): continue
        k=_key(raw_name)
        if k in seen: continue
        seen.add(k)
        lines.append(f"- {raw_name}: {raw_val}")
    return lines

def inject_specs_block(shop_el:ET.Element)->Tuple[int,int]:
    """
    Вставляем блок «Характеристики:» в <description> (если есть что вставлять).
    Старый маркер [SPECS_BEGIN]...[SPECS_END] — вырезаем, чтобы не дублировать.
    Возвращаем: (сколько офферов затронуто, всего добавлено строк).
    """
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0)
    offers_touched=0; lines_total=0
    spec_re=re.compile(r"\[SPECS_BEGIN\].*?\[SPECS_END\]", re.S)
    for offer in offers_el.findall("offer"):
        lines=build_specs_lines(offer)
        if not lines: continue
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
    """Полностью удаляем исходные <param>/<Param> после вставки в описание."""
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    removed=0
    for offer in offers_el.findall("offer"):
        for p in list(offer.findall("param")) + list(offer.findall("Param")):
            offer.remove(p); removed+=1
    return removed

# === ДОП. ЧИСТКА ОПИСАНИЙ: убрать «Артикул: …» и «Благотворительность: …» ===

RE_KV_LINE = re.compile(
    r"(^|\n)\s*[-–—]?\s*(Артикул|Благотворительн\w*)\s*:\s*.*?(?=\n|$)", re.I
)

def remove_blacklisted_kv_from_descriptions(shop_el: ET.Element) -> int:
    """
    Вырезает из <description> строки:
      - «- Артикул: …»
      - «- Благотворительность: …» (и похожие формы)
    Если после вырезания «Характеристики:» остались пустыми — удаляем заголовок.
    """
    offers_el=shop_el.find("offers")
    if offers_el is None: return 0
    changed=0
    for offer in offers_el.findall("offer"):
        d=offer.find("description")
        if d is None or not d.text: continue
        text=d.text
        # 1) Удаляем нежелательные строки
        new_text = RE_KV_LINE.sub(lambda m: ("" if m.group(1)=="" else m.group(1)), text)
        # 2) Если «Характеристики:» без пунктов — убираем заголовок
        lines = [ln for ln in new_text.splitlines()]
        out_lines=[]
        i=0
        while i < len(lines):
            ln = lines[i]
            if re.match(r"^\s*Характеристики\s*:\s*$", ln, re.I):
                j=i+1
                kept_block=[]
                # собираем блок до следующей «шапки:» или конца
                while j < len(lines) and not re.match(r"^\s*\S.*:$", lines[j]):
                    if lines[j].strip()!="":
                        kept_block.append(lines[j])
                    j+=1
                # оставляем заголовок только если есть пункты «- …»
                if any(re.match(r"^\s*[-–—]\s*\S", x) for x in kept_block):
                    out_lines.append(ln)
                    out_lines.extend(kept_block)
                i=j
                continue
            else:
                out_lines.append(ln)
                i+=1
        rebuilt = "\n".join(out_lines).strip()
        if rebuilt != d.text:
            d.text = rebuilt
            changed += 1
    return changed

# ======================= ДОСТУПНОСТЬ (available) ===========================

TRUE_WORDS  = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS = {"false","0","no","n","нет","отсутствует","нет в наличии","out of stock","unavailable","под заказ","ожидается","на заказ"}

def _parse_bool_str(s: str) -> Optional[bool]:
    """Парсинг «да/нет» в булево (разные языки/формы)."""
    if s is None: return None
    v = _norm_text(s)
    if v in TRUE_WORDS:  return True
    if v in FALSE_WORDS: return False
    return None

def _parse_int(s: str) -> Optional[int]:
    """Безопасный парсинг целого из строки."""
    if s is None: return None
    t = re.sub(r"[^\d\-]+","", s)
    if t in {"","-","+"}: return None
    try: return int(t)
    except Exception: return None

def derive_available(offer: ET.Element) -> Tuple[bool, str]:
    """
    Логика определения доступности:
    1) <available> как текст → парсим.
    2) Остатки (<Stock>/<quantity> …) → >0 = True.
    3) <status>/<Param name="Статус/Наличие"> → парсим да/нет.
    4) Иначе False.
    """
    avail_el = offer.find("available")
    if avail_el is not None and avail_el.text:
        b = _parse_bool_str(avail_el.text)
        if b is not None: return b, "tag"
    for tag in ["quantity_in_stock","quantity","stock","Stock"]:
        for node in offer.findall(tag):
            val = _parse_int(node.text or "")
            if val is not None: return (val > 0), "stock"
    for tag in ["status","Status"]:
        node = offer.find(tag)
        if node is not None and node.text:
            b = _parse_bool_str(node.text)
            if b is not None: return b, "status"
    for p in list(offer.findall("param")) + list(offer.findall("Param")):
        nm = (p.attrib.get("name") or "").strip().lower()
        if "статус" in nm or "налич" in nm:
            b = _parse_bool_str(p.text or "")
            if b is not None: return b, "status"
    return False, "default"

def normalize_available_field(shop_el: ET.Element) -> Tuple[int,int,int,int]:
    """
    Проставляем нормализованный тег <available>true|false</available>
    и удаляем исходные теги остатков (если включено DROP_STOCK_TAGS).
    Возврат: статистика (true_cnt, false_cnt, from_stock_cnt, from_status_cnt).
    """
    offers_el = shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
    true_cnt = false_cnt = from_stock_cnt = from_status_cnt = 0
    for offer in offers_el.findall("offer"):
        b, src = derive_available(offer)
        # удаляем атрибут available у <offer>, если был
        if "available" in offer.attrib:
            offer.attrib.pop("available", None)
        # создаём/обновляем единый тег <available>
        avail = offer.find("available") or ET.SubElement(offer, "available")
        avail.text = "true" if b else "false"
        # статистика
        if b: true_cnt += 1
        else: false_cnt += 1
        if src == "stock":  from_stock_cnt += 1
        if src == "status": from_status_cnt += 1
        # чистим исходные складские теги
        if DROP_STOCK_TAGS:
            for tag in ["quantity_in_stock","quantity","stock","Stock"]:
                for node in list(offer.findall(tag)):
                    offer.remove(node)
    return true_cnt, false_cnt, from_stock_cnt, from_status_cnt

# ===================== vendorCode / артикул + префикс ======================

ARTICUL_RE=re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)

def _extract_article_from_name(name:str)->str:
    """Пытаемся вытащить код из <name> (похоже на артикул)."""
    if not name: return ""
    m=ARTICUL_RE.search(name)
    return (m.group(1) if m else "").upper()

def _extract_article_from_url(url:str)->str:
    """Пытаемся вытащить код из хвоста URL."""
    if not url: return ""
    try:
        path=urllib.parse.urlparse(url).path.rstrip("/")
        last=path.split("/")[-1]
        last=re.sub(r"\.(html?|php|aspx?)$","",last,flags=re.I)
        m=ARTICUL_RE.search(last)
        return (m.group(1) if m else last).upper()
    except Exception:
        return ""

def _normalize_code(s:str)->str:
    """Чистим код: убираем пробелы/символы, приводим к VER-LOOKUP виду."""
    s=(s or "").strip()
    if not s: return ""
    s=re.sub(r"[\s_]+","",s).replace("—","-").replace("–","-")
    s=re.sub(r"[^A-Za-z0-9\-]+","",s)
    return s.upper()

def ensure_vendorcode_with_article(shop_el:ET.Element,prefix:str,create_if_missing:bool=False)->Tuple[int,int,int,int]:
    """
    Гарантируем наличие <vendorCode>, где значение = prefix + артикул.
    Источники артикула: @article → <name> → URL → @id.
    Возвращаем статистику: (всего_префикснули, создано_узлов, заполнено_из_артикула, осталось_пустых).
    """
    offers_el=shop_el.find("offers")
    if offers_el is None: return (0,0,0,0)
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
        vc.text=f"{prefix}{(vc.text or '')}"
        total_prefixed+=1
    return total_prefixed,created,filled_from_art,fixed_bare

# ===================== ЧИСТКА СЛУЖЕБНЫХ ТЕГОВ/АТРИБУТОВ ====================

def purge_offer_tags_and_attrs_after(offer:ET.Element)->Tuple[int,int]:
    """Удаляем ненужные теги и атрибуты у конкретного <offer> на финальном шаге."""
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
    """Счётчик, сколько <categoryId> будет удалено (для статистики FEED_META)."""
    return len(list(offer_el.findall("categoryId"))) + len(list(offer_el.findall("CategoryId")))

# ========================== FEED_META (комментарий) ========================

def render_feed_meta_comment(pairs:Dict[str,str])->str:
    """
    Рисуем блочный человекочитаемый комментарий с выравниванием колонок.
    Пояснения к полям — по-русски (второй «столбец»).
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
    """Строка TOP N отброшенных «брендов» (служебных/пустых) для метрики."""
    items=sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
    return ",".join(f"{k}:{v}" for k,v in items) if items else "n/a"

# ================================= MAIN ====================================

def main()->None:
    """Главный конвейер сборки Alstyle → docs/alstyle.yml."""
    log(f"Source: {SUPPLIER_URL or '(not set)'}")
    data=load_source_bytes(SUPPLIER_URL)
    src_root=ET.fromstring(data)

    # 1) Находим <shop> и <offers> у источника
    shop_in=src_root.find("shop") if src_root.tag.lower()!="shop" else src_root
    if shop_in is None: err("XML: <shop> not found")
    offers_in_el = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None: err("XML: <offers> not found")
    src_offers=list(offers_in_el.findall("offer"))

    # 2) Категории (для фильтра и статистики)
    id2name, id2parent, parent2children = parse_categories_tree(shop_in)
    catid_to_drop_total=sum(count_category_ids(o) for o in src_offers)

    # 3) Готовим выходную структуру
    out_root=ET.Element("yml_catalog")
    out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop=ET.SubElement(out_root,"shop")
    out_offers=ET.SubElement(out_shop,"offers")

    # 4) Переносим все офферы как есть (дальше будем чистить/обновлять)
    for o in src_offers:
        out_offers.append(deepcopy(o))

    # 5) Фильтр по категориям
    rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH) if ALSTYLE_CATEGORIES_MODE in {"include","exclude"} else (set(),[])
    if ALSTYLE_CATEGORIES_MODE=="include" and not (rules_ids or rules_names):
        err("ALSTYLE_CATEGORIES_MODE=include, но правил категорий не найдено. Проверь docs/alstyle_categories.txt.", 2)

    filtered_by_categories = 0
    if (ALSTYLE_CATEGORIES_MODE in {"include","exclude"}) and (rules_ids or rules_names):
        keep_ids: Set[str] = set(rules_ids)
        # сбираем id по совпадениям в названии
        if rules_names and id2name:
            for cid in id2name.keys():
                path = build_category_path_from_id(cid, id2name, id2parent)
                if category_matches_name(path, rules_names):
                    keep_ids.add(cid)
        # расширяем потомками
        if keep_ids and parent2children:
            keep_ids = collect_descendants(keep_ids, parent2children)
        # фильтруем офферы
        for off in list(out_offers.findall("offer")):
            cid = get_text(off, "categoryId")
            hit = (cid in keep_ids) if cid else False
            drop_this = (ALSTYLE_CATEGORIES_MODE=="exclude" and hit) or (ALSTYLE_CATEGORIES_MODE=="include" and not hit)
            if drop_this:
                out_offers.remove(off)
                filtered_by_categories += 1

    # 6) Убираем <categoryId> в финале (чтобы не «светить» внутренние ID)
    if DROP_CATEGORY_ID_TAG:
        for off in out_offers.findall("offer"):
            for node in list(off.findall("categoryId")) + list(off.findall("CategoryId")):
                off.remove(node)

    # 7) Нормализация бренда (vendor)
    norm_cnt, dropped_names = ensure_vendor(out_shop)

    # 8) vendorCode из артикула с префиксом AS (создаём при отсутствии)
    total_prefixed, created_nodes, filled_from_art, fixed_bare = ensure_vendorcode_with_article(
        out_shop,
        prefix=os.getenv("VENDORCODE_PREFIX","AS"),
        create_if_missing=os.getenv("VENDORCODE_CREATE_IF_MISSING","1").lower() in {"1","true","yes"}
    )

    # 9) Пересчет цен
    upd, skipped, total = reprice_offers(out_shop, PRICING_RULES)

    # 10) Доступность
    av_true, av_false, av_from_stock, av_from_status = normalize_available_field(out_shop)

    # 11) Перенос параметров в «Характеристики» + удаление param
    specs_offers, specs_lines = inject_specs_block(out_shop)
    removed_params = strip_all_params(out_shop)

    # 12) Доп. чистка описаний (вырезаем строки Артикул/Благотворительность)
    removed_kv = remove_blacklisted_kv_from_descriptions(out_shop)

    # 13) Финальная чистка тегов/атрибутов каждого <offer>
    for off in out_offers.findall("offer"):
        purge_offer_tags_and_attrs_after(off)

    # 14) Визуальные разделители между офферами (для удобного диффа/чтения)
    children=list(out_offers)
    for i in range(len(children)-1, 0, -1):
        out_offers.insert(i, ET.Comment("OFFSEP"))

    # 15) Пытаемся красиво отформатировать XML (Python 3.9+)
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # 16) FEED_META — комментарий в начале файла (перед <shop>)
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
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # 17) Генерируем текст XML и правим «косметику»
    xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")

    # 17.1) Заменяем разделители офферов на пустые строки
    xml_text = re.sub(r"\s*<!--OFFSEP-->\s*", "\n\n  ", xml_text)
    # 17.2) Схлопываем «лишние» пустые строки
    xml_text = re.sub(r"(\n[ \t]*){3,}", "\n\n", xml_text)
    # 17.3) ВАЖНО: после FEED_META вставляем перевод строки перед <shop>,
    # чтобы не было слипания вида `... (Алматы)--><shop>`
    xml_text = re.sub(r"(-->)\s*(<shop>)", r"\\1\n  \\2", xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    # 18) Пишем только YML
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
        f.write(xml_text)

    # 19) Для GitHub Pages — тех. файл
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
