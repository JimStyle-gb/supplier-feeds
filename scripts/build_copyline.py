# -*- coding: utf-8 -*-
"""
Build Copyline YML (flat <offers>) for Satu
script_version = copyline-2025-09-17.0

Что делает:
- Скачивает XLSX-прайс Copyline (по умолчанию: https://copyline.kz/files/price-CLA.xlsx)
- Читает строки, вытягивает артикул/название/цены/бренд/картинку/категорию (если есть колонки)
- Фильтрует товары: <name> ДОЛЖЕН НАЧИНАТЬСЯ с фразы из docs/copyline_keywords.txt (строгий префикс)
- Считает розничную цену по правилам (4% + фикс. добавка по диапазонам) и нормализует в формат "...900"
- Создаёт <vendorCode> с префиксом CL, если возможно достать артикул
- Выходит только в docs/copyline.yml (windows-1251), БЕЗ <url>
- Добавляет FEED_META (на русском) и разрыв строки между <!--...--> и <shop>

Примечания:
- Категории: если в XLSX есть колонка "Категория/Раздел/Группа", берём её для статистики, но НЕ выводим в YML.
- Можно включить опциональный фильтр по категориям через файл docs/copyline_categories.txt (режим include).
- Доступность: если найдём колонку "наличие/остаток/stock/qty" и там >0 или "да" — ставим true, иначе по умолчанию тоже true.
"""

from __future__ import annotations
import os, sys, re, io, time, random
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests
import pandas as pd

# ========================== НАСТРОЙКИ (ENV) ==========================

SCRIPT_VERSION = "copyline-2025-09-17.0"

SUPPLIER_NAME  = os.getenv("SUPPLIER_NAME", "copyline")
SUPPLIER_URL   = os.getenv("SUPPLIER_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE_YML   = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC            = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S      = int(os.getenv("TIMEOUT_S", "40"))
RETRIES        = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF  = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES      = int(os.getenv("MIN_BYTES", "1500"))
DRY_RUN        = os.getenv("DRY_RUN", "0").lower() in {"1", "true", "yes"}

# Фильтр по НАЗВАНИЮ (строгий префикс)
CL_KEYWORDS_PATH  = os.getenv("COPYLINE_KEYWORDS_PATH", "docs/copyline_keywords.txt")
CL_KEYWORDS_MODE  = os.getenv("COPYLINE_KEYWORDS_MODE", "include").lower()   # include|exclude
CL_KEYWORDS_DEBUG = os.getenv("COPYLINE_KEYWORDS_DEBUG", "0").lower() in {"1", "true", "yes"}
CL_DEBUG_MAX_HITS = int(os.getenv("COPYLINE_DEBUG_MAX_HITS", "40"))

# Опциональный фильтр по КАТЕГОРИЯМ (как в alstyle)
CL_CATEGORIES_PATH = os.getenv("COPYLINE_CATEGORIES_PATH", "docs/copyline_categories.txt")
CL_CATEGORIES_MODE = os.getenv("COPYLINE_CATEGORIES_MODE", "").lower()       # "", "include" или "exclude"

# vendorCode
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "CL")
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "1").lower() in {"1","true","yes"}

# ========================== УТИЛИТЫ ЛОГА ===========================

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARN: {msg}", file=sys.stderr, flush=True)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def now_almaty_str() -> str:
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S")

# ========================== СКАЧАТЬ XLSX ===========================

def fetch_bytes(url: str, timeout: int, retries: int, backoff: float) -> bytes:
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
            slp = backoff * attempt * (1 + random.uniform(-0.2, 0.2))
            warn(f"fetch attempt {attempt}/{retries} failed: {e}; sleep {slp:.2f}s")
            if attempt < retries:
                time.sleep(slp)
    raise RuntimeError(f"fetch failed after {retries} attempts: {last_exc}")

# ========================== ПОИСК КОЛОНОК ==========================

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower().replace("ё", "е"))

def guess_col(cols: List[str], patterns: List[str]) -> Optional[str]:
    """Выбирает первую колонку, имя которой содержит один из шаблонов."""
    ncols = [norm(c) for c in cols]
    for p in patterns:
        p = norm(p)
        for raw, n in zip(cols, ncols):
            if p in n:
                return raw
    return None

def best_price_cols(cols: List[str]) -> List[str]:
    """Упорядоченный список кандидатов для дилерских/оптовых цен."""
    order = [
        "dealer", "дилер", "опт", "wholesale", "b2b", "закуп", "purchase",
        "price", "цена", "rrp", "розница",
    ]
    ncols = [norm(c) for c in cols]
    ranked = []
    for p in order:
        for raw, n in zip(cols, ncols):
            if p in n and raw not in ranked:
                ranked.append(raw)
    return ranked

# ======================= КЛЮЧИ/КАТЕГОРИИ ФИЛЬТР =====================

class KeySpec:
    """Правило: 'prefix' (фраза-префикс) или 'regex' (матчим с начала строки)."""
    __slots__ = ("raw", "kind", "norm", "pattern")
    def __init__(self, raw: str, kind: str, norm_key: Optional[str], pattern: Optional[re.Pattern]):
        self.raw = raw
        self.kind = kind
        self.norm = norm_key
        self.pattern = pattern

def load_prefix_keywords(path: str) -> List[KeySpec]:
    """Каждая строка => префикс (нормализуем) или /regex/ (матчим .match)."""
    if not path or not os.path.exists(path):
        return []
    data = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path, "r", encoding=enc) as f:
                data = f.read()
            break
        except Exception:
            continue
    if data is None:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read()
    keys: List[KeySpec] = []
    for ln in data.splitlines():
        s = ln.strip()
        if not s or s.lstrip().startswith("#"):
            continue
        if len(s) >= 2 and s[0] == "/" and s[-1] == "/":
            try:
                keys.append(KeySpec(s, "regex", None, re.compile(s[1:-1], re.I)))
            except Exception:
                pass
            continue
        keys.append(KeySpec(s, "prefix", norm(s), None))
    return keys

def name_matches_prefix(name: str, keys: List[KeySpec]) -> Tuple[bool, Optional[str]]:
    if not keys:
        return False, None
    n = norm(name)
    for ks in keys:
        if ks.kind == "prefix":
            if n.startswith(ks.norm or ""):
                return True, ks.raw
        else:
            if ks.pattern and ks.pattern.match(name or ""):
                return True, ks.raw
    return False, None

def load_category_rules(path: str) -> List[KeySpec]:
    """Похожие правила, но применяются к cat_path (префиксно)."""
    if not path or not os.path.exists(path):
        return []
    data = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path, "r", encoding=enc) as f:
                data = f.read()
            break
        except Exception:
            continue
    if data is None:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read()
    rules: List[KeySpec] = []
    for ln in data.splitlines():
        s = ln.strip()
        if not s or s.lstrip().startswith("#"):
            continue
        if len(s) >= 2 and s[0] == "/" and s[-1] == "/":
            try:
                rules.append(KeySpec(s, "regex", None, re.compile(s[1:-1], re.I)))
            except Exception:
                pass
            continue
        rules.append(KeySpec(s, "prefix", norm(s), None))
    return rules

def cat_matches(cat_path: str, rules: List[KeySpec]) -> Tuple[bool, Optional[str]]:
    if not rules:
        return False, None
    n = norm(cat_path)
    for ks in rules:
        if ks.kind == "prefix":
            if n.startswith(ks.norm or ""):
                return True, ks.raw
        else:
            if ks.pattern and ks.pattern.match(cat_path or ""):
                return True, ks.raw
    return False, None

# ========================= ЦЕНЫ/ПРАВИЛА ==========================

PriceRule = Tuple[int,int,float,int]  # (min, max, %, fix add)
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

def parse_price(x) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    s = s.replace("\xa0"," ").replace(" ", "").replace("KZT","").replace("kzt","").replace("₸","").replace(",", ".")
    try:
        v = float(s)
        return v if v > 0 else None
    except Exception:
        return None

def force_tail_900(n: float) -> int:
    i = int(n)
    k = max(i // 1000, 0)
    out = k * 1000 + 900
    return out if out >= 900 else 900

def compute_retail(dealer: float) -> Optional[int]:
    for lo, hi, pct, add in PRICING_RULES:
        if lo <= dealer <= hi:
            return force_tail_900(dealer * (1.0 + pct/100.0) + add)
    return None

# =========================== ПОЛЕЗНЫЕ ШТУКИ ============================

def sanitize_code(s: str) -> str:
    """Нормализует артикул/код: выкидывает пробелы/символы, upper."""
    s = (str(s) if s is not None else "").strip()
    if not s:
        return ""
    s = re.sub(r"[\s_]+", "", s)
    s = s.replace("—", "-").replace("–", "-")
    s = re.sub(r"[^A-Za-z0-9\-]+", "", s)
    return s.upper()

def clean_one_line(s: str) -> str:
    """Любые пробелы/переносы → один пробел, обрезаем по краям."""
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\r\n","\n").replace("\r","\n").replace("\u00A0"," ")
    s = re.sub(r"&nbsp;", " ", s, flags=re.I)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def looks_available(val) -> bool:
    """Эвристика наличия."""
    s = str(val).strip().lower() if val is not None else ""
    if not s:
        return True  # по умолчанию оставляем true
    if re.search(r"\b(да|в наличии|есть|true|yes|instock)\b", s):
        return True
    if re.search(r"\b(нет|ожидается|под заказ|out ?of ?stock|false|no)\b", s):
        return False
    # числа > 0 считаем "в наличии"
    try:
        v = float(s.replace(",", "."))
        return v > 0
    except Exception:
        return True

# =========================== FEED_META ============================

def render_feed_meta_comment(pairs: Dict[str, str]) -> str:
    order = [
        "script_version","supplier","source",
        "offers_total","offers_written",
        "keywords_mode","keywords_total","filtered_by_keywords",
        "categories_mode","categories_rules","filtered_by_categories",
        "prices_updated","vendors_recovered",
        "vendorcodes_created","vendorcodes_filled_from_article",
        "built_utc","built_Asia/Almaty",
    ]
    comments = {
        "script_version": "Версия скрипта",
        "supplier":       "Метка поставщика",
        "source":         "Источник (XLSX)",
        "offers_total":   "Строк в исходном прайсе",
        "offers_written": "Офферов записано (после фильтров)",
        "keywords_mode":  "Фильтр по префиксам name (include/exclude/off)",
        "keywords_total": "Сколько префиксов загружено",
        "filtered_by_keywords": "Сколько строк отсечено по name",
        "categories_mode": "Фильтр по категориям (include/exclude/off)",
        "categories_rules":"Сколько правил категорий",
        "filtered_by_categories":"Сколько строк отсечено по категориям",
        "prices_updated": "Скольким товарам выставили price",
        "vendors_recovered": "Скольким товарам нормализован vendor",
        "vendorcodes_created": "Сколько создано узлов vendorCode",
        "vendorcodes_filled_from_article": "Сколько vendorCode взяли из артикула",
        "built_utc":      "Время сборки (UTC)",
        "built_Asia/Almaty": "Время сборки (Алматы)",
    }
    max_key = max(len(k) for k in order)
    left = [f"{k.ljust(max_key)} = {pairs.get(k, 'n/a')}" for k in order]
    max_left = max(len(s) for s in left)
    lines = ["FEED_META"]
    for left_line, k in zip(left, order):
        lines.append(f"{left_line.ljust(max_left)}  | {comments.get(k,'')}")
    return "\n".join(lines)

# ============================= ОСНОВА ==============================

def main() -> None:
    log(f"Source: {SUPPLIER_URL}")
    data = fetch_bytes(SUPPLIER_URL, TIMEOUT_S, RETRIES, RETRY_BACKOFF)
    buf = io.BytesIO(data)

    # Читаем Excel (первая страница)
    df = pd.read_excel(buf, engine="openpyxl")
    if df is None or df.empty:
        err("Excel пустой или не распознан.", 2)

    cols = list(df.columns)
    # Пытаемся угадать основные колонки
    col_name   = guess_col(cols, ["name", "наименование", "товар", "product", "позиция"])
    col_art    = guess_col(cols, ["артикул", "article", "код", "model", "sku"])
    col_brand  = guess_col(cols, ["бренд", "vendor", "brand", "производитель"])
    col_desc   = guess_col(cols, ["описание", "description", "характеристики", "spec"])
    col_image  = guess_col(cols, ["картинка", "image", "фото", "picture", "img", "photo"])
    col_cat    = guess_col(cols, ["категория", "раздел", "группа", "category", "group", "section"])
    col_stock  = guess_col(cols, ["наличие", "остаток", "stock", "qty", "количество", "склад"])
    price_cols = best_price_cols(cols)

    if not col_name:
        err("Не найдена колонка с названием (name/наименование).", 2)

    # Ключевые префиксы по name
    keys = load_prefix_keywords(CL_KEYWORDS_PATH)
    if CL_KEYWORDS_MODE == "include" and len(keys) == 0:
        err("COPYLINE_KEYWORDS_MODE=include, но префиксов в docs/copyline_keywords.txt не найдено.", 2)

    # Категориальные правила (опционально)
    cat_rules: List[KeySpec] = []
    if CL_CATEGORIES_MODE in {"include", "exclude"}:
        cat_rules = load_category_rules(CL_CATEGORIES_PATH)

    # Создаём каркас YML
    out_root = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")
    out_offs = ET.SubElement(out_shop, "offers")

    offers_total = 0
    filtered_by_kw = 0
    filtered_by_cat = 0
    vendors_recovered = 0
    prices_updated = 0
    vendorcodes_created = 0
    vendorcodes_filled = 0

    # Проходим по строкам прайса
    for i, row in df.iterrows():
        offers_total += 1

        name = clean_one_line(row.get(col_name, "")) if col_name else ""
        if not name:
            continue

        # Фильтр по префиксу name
        hit, rawkey = name_matches_prefix(name, keys) if keys else (False, None)
        drop = (CL_KEYWORDS_MODE == "exclude" and hit) or (CL_KEYWORDS_MODE == "include" and not hit)
        if drop:
            filtered_by_kw += 1
            continue

        # Категория (для статистики/фильтра), НЕ выводим в YML
        cat_path = clean_one_line(row.get(col_cat, "")) if col_cat else ""
        if cat_rules:
            cat_hit, _ = cat_matches(cat_path, cat_rules)
            drop_cat = (CL_CATEGORIES_MODE == "exclude" and cat_hit) or (CL_CATEGORIES_MODE == "include" and not cat_hit)
            if drop_cat:
                filtered_by_cat += 1
                continue

        # Артикул/код
        art = sanitize_code(row.get(col_art, "")) if col_art else ""
        # Бренд
        vendor_raw = clean_one_line(row.get(col_brand, "")) if col_brand else ""
        vendor = vendor_raw.strip()
        # По общему правилу — не используем название поставщика как бренд:
        if vendor.lower() in {"copyline"}:
            vendor = ""  # оставляем пустым, если бренд не распознан

        # Картинка (если есть)
        picture = clean_one_line(row.get(col_image, "")) if col_image else ""

        # Описание — в одну строку
        desc = clean_one_line(row.get(col_desc, "")) if col_desc else ""

        # Доступность
        available = True
        if col_stock:
            available = looks_available(row.get(col_stock, ""))

        # Цена (берём МИН из доступных «ценовых» колонок)
        dealer_vals: List[float] = []
        for pc in price_cols:
            v = parse_price(row.get(pc, None))
            if v is not None:
                dealer_vals.append(v)
        dealer = min(dealer_vals) if dealer_vals else None
        retail = compute_retail(dealer) if dealer is not None and dealer > 100 else None

        # Сборка offer
        offer = ET.Element("offer")
        # id: используем артикул, иначе порядковый индекс
        offer_id = art or f"ROW{i+1}"
        offer.set("id", offer_id)
        # type/available/article не пишем (держим минимально)
        # name
        nm = ET.SubElement(offer, "name"); nm.text = name
        # picture (если есть)
        if picture:
            pic = ET.SubElement(offer, "picture"); pic.text = picture
        # vendor (только если не пустой)
        if vendor:
            ven = ET.SubElement(offer, "vendor"); ven.text = vendor
            vendors_recovered += 1
        # description (в одну строку, без пустых блоков)
        if desc:
            de = ET.SubElement(offer, "description"); de.text = desc
        # vendorCode: создаём узел даже если пустой, чтобы приставить префикс
        vc = ET.SubElement(offer, "vendorCode")
        if art:
            vc.text = art
            vendorcodes_filled += 1
        vendorcodes_created += 1
        # приставляем префикс CL (если текст уже есть, просто префиксуем)
        vc.text = f"{VENDORCODE_PREFIX}{(vc.text or '')}"

        # price / currencyId
        if retail is not None:
            pr = ET.SubElement(offer, "price"); pr.text = str(int(retail))
            cu = ET.SubElement(offer, "currencyId"); cu.text = "KZT"
            prices_updated += 1

        # available
        av = ET.SubElement(offer, "available"); av.text = "true" if available else "false"

        # Важно: <url> НЕ добавляем (по твоим правилам)
        # В YML НЕ добавляем categoryId

        out_offs.append(offer)

    # Для читабельности добавим визуальные разделители между офферами
    children = list(out_offs)
    for idx in range(len(children)-1, 0, -1):
        out_offs.insert(idx, ET.Comment("OFFSEP"))

    # Красивые отступы (если доступно)
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    # FEED_META
    offers_written = len([n for n in out_offs if isinstance(n.tag, str) and n.tag == "offer"])
    meta_pairs = {
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "offers_total": offers_total,
        "offers_written": offers_written,
        "keywords_mode": CL_KEYWORDS_MODE if len(keys) > 0 else "off",
        "keywords_total": len(keys),
        "filtered_by_keywords": filtered_by_kw,
        "categories_mode": (CL_CATEGORIES_MODE or "off"),
        "categories_rules": len(load_category_rules(CL_CATEGORIES_PATH)) if (CL_CATEGORIES_MODE in {"include","exclude"}) else 0,
        "filtered_by_categories": filtered_by_cat,
        "prices_updated": prices_updated,
        "vendors_recovered": vendors_recovered,
        "vendorcodes_created": vendorcodes_created,
        "vendorcodes_filled_from_article": vendorcodes_filled,
        "built_utc": now_utc_str(),
        "built_Asia/Almaty": now_almaty_str(),
    }
    out_root.insert(0, ET.Comment(render_feed_meta_comment(meta_pairs)))

    # Сериализация и постобработка
    xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")
    # OFFSEP → пустые строки
    xml_text = re.sub(r"\s*<!--OFFSEP-->\s*", "\n\n  ", xml_text)
    xml_text = re.sub(r"(\n[ \t]*){3,}", "\n\n", xml_text)
    # Разрыв между концом FEED_META и <shop> (фикс '--><shop>')
    xml_text = re.sub(r"(-->)\s*(<shop>)", lambda m: f"{m.group(1)}\n  {m.group(2)}", xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] Files not written.")
        return

    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
        f.write(xml_text)

    # .nojekyll для GitHub Pages (не критично, но удобно)
    docs_dir = os.path.dirname(OUT_FILE_YML) or "docs"
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
