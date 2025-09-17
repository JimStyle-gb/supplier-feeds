# -*- coding: utf-8 -*-
"""
Сборщик YML для поставщика Copyline (плоский <offers> для Satu)
script_version = copyline-2025-09-17.3

Что делает:
- Берёт XLSX по URL (SUPPLIER_URL), находит корректную шапку
  (устойчив к титульным строкам типа "Прайс-лист", merged-ячейкам и т.п.).
- Загружает префиксы/регулярки из docs/copyline_keywords.txt
  (поддержка utf-8/utf-8-sig/utf-16/le/be/windows-1251).
- Фильтрует товары: <name> ДОЛЖЕН НАЧИНАТЬСЯ с одной из фраз/регулярок.
- <vendor> — реальный бренд (если распознан), иначе не выводим тег.
- <vendorCode> — нормализованный артикул с префиксом CL.
- Цена: по умолчанию PRICE_MODE=pass — берём "как есть" (минимум из колонок цены).
  Можно включить PRICE_MODE=retail — применит наценку и хвост ...900 (как у других).
- <url> в офферах не пишется.
- <description> — приводится к одной строке.
- FEED_META — на русском, выровненный, с переносом строки перед <shop>.
- Выход: ТОЛЬКО docs/copyline.yml (Windows-1251).
"""

from __future__ import annotations
import os, sys, re, time, random, urllib.parse, io, hashlib, unicodedata
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

# ========================== НАСТРОЙКИ ===========================

SCRIPT_VERSION = "copyline-2025-09-17.3"

SUPPLIER_NAME    = os.getenv("SUPPLIER_NAME", "copyline")
SUPPLIER_URL     = os.getenv("SUPPLIER_URL", "https://copyline.kz/files/price-CLA.xlsx")
OUT_FILE_YML     = os.getenv("OUT_FILE", "docs/copyline.yml")
ENC              = os.getenv("OUTPUT_ENCODING", "windows-1251")

TIMEOUT_S        = int(os.getenv("TIMEOUT_S", "30"))
RETRIES          = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF    = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES        = int(os.getenv("MIN_BYTES", "2000"))

COPYLINE_KEYWORDS_PATH  = os.getenv("COPYLINE_KEYWORDS_PATH", "docs/copyline_keywords.txt")
COPYLINE_KEYWORDS_MODE  = os.getenv("COPYLINE_KEYWORDS_MODE", "include").lower()  # include | exclude

# Отладка фильтра: печать первых ключей и первых имён
COPYLINE_KEYWORDS_DEBUG = os.getenv("COPYLINE_KEYWORDS_DEBUG", "0").lower() in {"1","true","yes"}
COPYLINE_DEBUG_MAX_HITS = int(os.getenv("COPYLINE_DEBUG_MAX_HITS", "40" ))

# Перед префикс-матчем срезать "мусор" в начале имени (кавычки/тире/буллеты и т.п.)
COPYLINE_PREFIX_ALLOW_TRIM = os.getenv("COPYLINE_PREFIX_ALLOW_TRIM", "1").lower() in {"1","true","yes"}

# Режим цены: pass (берём как есть) | retail (пересчитываем по правилам и хвост ...900)
PRICE_MODE = os.getenv("PRICE_MODE", "pass").strip().lower()

# vendorCode
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "CL")
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "1").lower() in {"1","true","yes"}

# DRY_RUN для локальной отладки
DRY_RUN = os.getenv("DRY_RUN", "0").lower() in {"1","true","yes"}

# ================================ УТИЛИТЫ ==================================

def log(msg: str) -> None:
    """Простой лог в stdout (видно в Actions)."""
    print(msg, flush=True)

def warn(msg: str) -> None:
    """Предупреждение в stderr."""
    print(f"WARN: {msg}", file=sys.stderr, flush=True)

def err(msg: str, code: int = 1) -> None:
    """Ошибка с завершением."""
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)

def now_utc_str() -> str:
    """Время в UTC (для FEED_META)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

def now_almaty_str() -> str:
    """Время в Asia/Almaty (для FEED_META)."""
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def _nfkc(s: str) -> str:
    """Юникод-нормализация (NFKC), чтобы прибить «похожие» символы."""
    return unicodedata.normalize("NFKC", s or "")

def _norm(s: str) -> str:
    """NFKC → NBSP→пробел → ё→е → lower → схлоп пробелов."""
    s = _nfkc(s).replace("\u00A0", " ").replace("ё", "е").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

_LEADING_NOISE_RE = re.compile(r'^[\s\-\–\—•·|:/\\\[\]\(\)«»"“”„\']+')

def _strip_leading_noise(s_norm: str) -> str:
    """Снять мусор в начале нормализованной строки (кавычки/тире/буллеты/скобки и т.п.)."""
    return _LEADING_NOISE_RE.sub("", s_norm)

def _clean_one_line(s: str) -> str:
    """Сжать описание в одну строку (убрать переносы, html-неразрывные пробелы и т.п.)."""
    if not s:
        return ""
    s = _nfkc(s).replace("\r\n","\n").replace("\r","\n").replace("\u00A0"," ")
    s = re.sub(r"&nbsp;?", " ", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    # Удаляем известный мусор в "характеристиках"
    s = re.sub(r"(?:^|\s)(Артикул|Благотворительность)\s*:\s*[^;.,]+[;.,]?\s*", " ", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_money(raw: str) -> Optional[float]:
    """Строка → число KZT (не ломается на пробелах/₸/запятых)."""
    if raw is None:
        return None
    s = str(raw)
    s = (s.strip()
           .replace("\xa0", " ")
           .replace(" ", "")
           .replace("KZT", "")
           .replace("kzt", "")
           .replace("₸", "")
           .replace(",", "."))
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except Exception:
        return None

def stable_id_from(text: str) -> str:
    """Стабильный ID на основе хеша текста (на крайний случай)."""
    h = hashlib.sha1((_nfkc(text)).encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"CL-{h}"

# ======================= СКАЧИВАНИЕ XLSX ========================

def fetch_xlsx_bytes(url: str) -> bytes:
    """Скачать XLSX c ретраями; защита от «обрезанных» ответов."""
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = sess.get(url, headers=headers, timeout=TIMEOUT_S, stream=True)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            data = r.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)} bytes)")
            return data
        except Exception as e:
            last_exc = e
            back = RETRY_BACKOFF * attempt * (1.0 + random.uniform(-0.2, 0.2))
            warn(f"fetch attempt {attempt}/{RETRIES} failed: {e}; sleep {back:.2f}s")
            if attempt < RETRIES:
                time.sleep(back)
    raise RuntimeError(f"fetch failed after {RETRIES} attempts: {last_exc}")

# ===================== КЛЮЧИ (префиксы/регулярки) ==================

class KeySpec:
    """Правило фильтра: prefix (норм.фраза) или regex (.match с начала строки)."""
    __slots__ = ("raw", "kind", "norm", "pattern")
    def __init__(self, raw: str, kind: str, norm: Optional[str], pattern: Optional[re.Pattern]):
        self.raw, self.kind, self.norm, self.pattern = raw, kind, norm, pattern

def load_keywords(path: str) -> List[KeySpec]:
    """Читаем ключи из файла в разных кодировках, чистим BOM и нулевые байты."""
    if not path or not os.path.exists(path):
        return []
    data = None
    for enc in ("utf-8-sig","utf-8","utf-16","utf-16-le","utf-16-be","windows-1251"):
        try:
            with open(path, "r", encoding=enc) as f:
                data = f.read()
            data = data.replace("\ufeff", "").replace("\x00", "")
            break
        except Exception:
            continue
    if data is None:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read().replace("\x00", "")
    keys: List[KeySpec] = []
    for line in data.splitlines():
        s = line.strip()
        if not s or s.lstrip().startswith("#"):
            continue
        if len(s) >= 2 and s[0] == "/" and s[-1] == "/":
            # Регулярка, матчится С НАЧАЛА строки (re.match)
            try:
                pat = re.compile(s[1:-1], re.I)
                keys.append(KeySpec(s, "regex", None, pat))
            except Exception:
                pass
        else:
            # Обычный префикс: сравниваем с нормализованным началом имени
            keys.append(KeySpec(s, "prefix", _norm(s), None))
    return keys

def name_passes_prefix(name: str, keys: List[KeySpec]) -> Tuple[bool, Optional[str]]:
    """Проверка: имя проходит строгий префикс/регулярку? Возвращает (ok, попавший_ключ)."""
    if not keys:
        return True, None  # если список пуст — не режем
    nm = _norm(name)
    nm_trim = _strip_leading_noise(nm) if COPYLINE_PREFIX_ALLOW_TRIM else nm
    for ks in keys:
        if ks.kind == "prefix":
            if ks.norm and (nm_trim.startswith(ks.norm) or nm.startswith(ks.norm)):
                return True, ks.raw
        else:
            if ks.pattern and ks.pattern.match(name or ""):
                return True, ks.raw
    return False, None

# =========================== НОРМАЛИЗАЦИЯ БРЕНДА ===========================

def _norm_brand_key(s: str) -> str:
    if not s:
        return ""
    s = _nfkc(s).strip().lower().replace("ё", "е")
    s = re.sub(r"[-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

# Не считаем брендами имена поставщиков/служебные
SUPPLIER_BLOCKLIST = {_norm_brand_key(x) for x in ["copyline","copy line","копилайн","alstyle","akcent","vtt"]}
UNKNOWN_VENDOR_MARKERS = ("неизвест", "unknown", "без бренда", "no brand", "noname", "no-name", "n/a")

def normalize_brand(raw: str) -> str:
    """Вернуть очищенный бренд или пустую строку, если бренд некорректный/служебный."""
    k = _norm_brand_key(raw or "")
    if (not k) or (k in SUPPLIER_BLOCKLIST):
        return ""
    return raw.strip()

# ============================== ЦЕНЫ ============================

# Правила для PRICE_MODE=retail (как у других поставщиков)
PriceRule = Tuple[int, int, float, int]
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

def price_tail_900(n: float) -> int:
    """Округление вниз до вида ...900 (минимум 900)."""
    i = int(n)
    k = max(i // 1000, 0)
    out = k * 1000 + 900
    return out if out >= 900 else 900

def compute_retail(dealer: float) -> Optional[int]:
    for lo, hi, pct, add in PRICING_RULES:
        if lo <= dealer <= hi:
            val = dealer * (1.0 + pct / 100.0) + add
            return price_tail_900(val)
    return None

# ============================ РАЗБОР XLSX ============================

# Синонимы названий колонок
NAME_COLS   = {
    "name","наименование","название","товар","product",
    "наименование товара","номенклатура","полное наименование","наименование продукции"
}
SKU_COLS    = {"артикул","sku","код","part","part number","partnumber","модель"}
PRICE_COLS  = {"цена","цена закуп","опт","dealer","закуп","b2b","стоимость","price","opt","rrp","розница","цена, тг","цена тг"}
BRAND_COLS  = {"бренд","производитель","vendor","brand","maker"}
DESC_COLS   = {"описание","description","описание товара","характеристики","spec","specs"}
URL_COLS    = {"url","ссылка","link"}
IMG_COLS    = {"image","картинка","фото","picture","image url","img"}
AVAIL_COLS  = {"наличие","stock","количество","qty","остаток","доступно"}

def _merged_value_on_row(ws: Worksheet, row_idx: int, col_idx: int):
    """Вернуть значение с учётом merged-диапазонов (берём верх-левую)."""
    cell = ws.cell(row=row_idx, column=col_idx)
    if cell.value not in (None, ""):
        return cell.value
    for mr in ws.merged_cells.ranges:
        if (mr.min_row <= row_idx <= mr.max_row) and (mr.min_col <= col_idx <= mr.max_col):
            return ws.cell(row=mr.min_row, column=mr.min_col).value
    return cell.value

def try_map_headers_on_row(ws: Worksheet, row_idx: int) -> Dict[int, str]:
    """Попытка построить mapping по конкретной строке."""
    raw_vals: List[str] = []
    max_col = ws.max_column or 0
    limit = min(max_col, 60) if max_col else 60
    for c in range(1, limit + 1):
        v = _merged_value_on_row(ws, row_idx, c)
        raw_vals.append("" if v is None else str(v).strip())

    nonempty = [x for x in raw_vals if x]
    if len(nonempty) <= 1 and any("прайс" in _norm(x) for x in nonempty):
        return {}

    mapping: Dict[int, str] = {}
    for idx, raw in enumerate(raw_vals, start=1):
        key = _norm(raw)
        if not key:
            continue
        if key in NAME_COLS: mapping[idx] = "name"
        elif key in SKU_COLS: mapping[idx] = "sku"
        elif key in BRAND_COLS: mapping[idx] = "brand"
        elif key in DESC_COLS: mapping[idx] = "desc"
        elif key in URL_COLS: mapping[idx] = "url"
        elif key in IMG_COLS: mapping[idx] = "img"
        elif key in AVAIL_COLS: mapping[idx] = "avail"
        elif key in PRICE_COLS: mapping[idx] = "price"
    if "name" not in mapping.values():
        return {}
    return mapping

def find_header_row(ws: Worksheet, scan_rows: int = 60) -> Tuple[Dict[int,str], int]:
    """Скан первых N строк, ищем лучшую «шапку»."""
    max_row = ws.max_row or 0
    limit = min(max_row, scan_rows) if max_row else scan_rows
    best_map: Dict[int,str] = {}
    best_row = -1
    best_score = -1
    for r in range(1, limit + 1):
        mapping = try_map_headers_on_row(ws, r)
        if not mapping:
            continue
        score = len(mapping)
        if score > best_score:
            best_map, best_row, best_score = mapping, r, score
            if score >= 3 and "name" in mapping.values():
                break
    return best_map, best_row

def select_best_sheet(wb) -> Tuple[Worksheet, Dict[int,str], int]:
    """Выбираем лист с наилучшей шапкой."""
    best: Tuple[Optional[Worksheet], Dict[int,str], int, int] = (None, {}, -1, -1)  # (ws, map, row, score)
    for ws in wb.worksheets:
        mapping, row_idx = find_header_row(ws, scan_rows=60)
        score = len(mapping)
        if score > best[3]:
            best = (ws, mapping, row_idx, score)
    ws, mapping, row_idx, score = best
    if not ws or not mapping or row_idx <= 0:
        err("XLSX: не удалось найти строку шапки ни на одном листе (нужна колонка с названием товара).")
    return ws, mapping, row_idx

def row_to_dict(row_vals: List, mapping: Dict[int,str]) -> Dict[str,str]:
    """Строка листа → словарь полей по нашему mapping."""
    out: Dict[str,str] = {}
    for col_idx, field in mapping.items():
        val = row_vals[col_idx - 1] if col_idx - 1 < len(row_vals) else None
        s = "" if val is None else str(val).strip()
        if not s:
            continue
        if field == "price":
            prev = out.get("_price_candidates", [])
            prev.append(s)
            out["_price_candidates"] = prev
        else:
            out[field] = s
    return out

def best_dealer_price(row: Dict[str,str]) -> Optional[float]:
    """Минимальная вменяемая закупочная из всех найденных ценовых колонок."""
    vals: List[float] = []
    for s in row.get("_price_candidates", []):
        v = parse_money(s)
        if v is not None:
            vals.append(v)
    return min(vals) if vals else None

# ======================= vendorCode / артикул ==============================

ARTICUL_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{2,})\b", re.I)

def norm_code(s: str) -> str:
    """Нормализуем артикул: убрать пробелы/подчёркивания/длинные тире; оставить A-Z0-9-; UPPER."""
    if not s:
        return ""
    s = re.sub(r"[\s_]+", "", s)
    s = s.replace("—", "-").replace("–", "-")
    s = re.sub(r"[^A-Za-z0-9\-]+", "", s)
    return s.upper()

def extract_article_from_any(row: Dict[str,str]) -> str:
    """Пытаемся достать артикул из sku → имени → URL."""
    art = norm_code(row.get("sku", ""))
    if art:
        return art
    name = row.get("name", "")
    m = ARTICUL_RE.search(name or "")
    if m:
        return norm_code(m.group(1))
    url = row.get("url", "")
    if url:
        try:
            last = urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
            last = re.sub(r"\.(html?|php|aspx?|htm)$", "", last, flags=re.I)
            m2 = ARTICUL_RE.search(last)
            return norm_code(m2.group(1) if m2 else last)
        except Exception:
            pass
    return ""

# =============================== FEED_META =================================

def render_feed_meta(pairs: Dict[str, str]) -> str:
    """Собираем выровненный блок FEED_META (русский, 2 колонки)."""
    order = [
        "script_version","supplier","source",
        "offers_total","offers_written","filtered_by_keywords",
        "prices_mode","prices_updated","vendors_detected","available_set_true",
        "built_utc","built_Asia/Almaty",
    ]
    comments = {
        "script_version":"Версия скрипта (для контроля в CI)",
        "supplier":"Метка поставщика",
        "source":"URL исходного XLSX",
        "offers_total":"Позиции в исходном файле (до фильтра)",
        "offers_written":"Офферов записано (после фильтра и очистки)",
        "filtered_by_keywords":"Сколько позиций отфильтровано по префиксам",
        "prices_mode":"Режим цен (pass/retail)",
        "prices_updated":"Скольким товарам записали price",
        "vendors_detected":"Скольким товарам распознали бренд",
        "available_set_true":"Скольким офферам выставлено available=true",
        "built_utc":"Время сборки (UTC)",
        "built_Asia/Almaty":"Время сборки (Алматы)",
    }
    max_key = max(len(k) for k in order)
    lefts = [f"{k.ljust(max_key)} = {pairs.get(k,'n/a')}" for k in order]
    max_left = max(len(x) for x in lefts)
    lines = ["FEED_META"]
    for left, k in zip(lefts, order):
        lines.append(f"{left.ljust(max_left)}  | {comments[k]}")
    return "\n".join(lines)

# ================================= MAIN ====================================

def main() -> None:
    log(f"Source: {SUPPLIER_URL}")

    # 1) XLSX
    data = fetch_xlsx_bytes(SUPPLIER_URL)
    wb = load_workbook(io.BytesIO(data), data_only=True, read_only=True)

    # 2) Выбираем лист и шапку
    ws, mapping, header_row_idx = select_best_sheet(wb)
    log(f"Sheet: {ws.title} | header_row={header_row_idx} | cols={len(mapping)}")
    if COPYLINE_KEYWORDS_DEBUG:
        log(f"[DEBUG] header mapping: {mapping}")

    # 3) Ключи префиксов
    keys = load_keywords(COPYLINE_KEYWORDS_PATH)
    if COPYLINE_KEYWORDS_MODE == "include" and len(keys) == 0:
        err("COPYLINE_KEYWORDS_MODE=include, но ключей не найдено. Проверь docs/copyline_keywords.txt.", 2)

    # 4) Строки данных
    raw_rows: List[Dict[str,str]] = []
    for r in ws.iter_rows(min_row=header_row_idx + 1, values_only=True):
        row_vals = [x if x is not None else "" for x in r]
        d = row_to_dict(row_vals, mapping)
        if "name" not in d or not d["name"].strip():
            continue
        raw_rows.append(d)

    offers_total = len(raw_rows)

    if COPYLINE_KEYWORDS_DEBUG:
        log(f"[DEBUG] loaded keywords: {len(keys)}")
        for i, ks in enumerate(keys[:10], 1):
            log(f"[DEBUG] key[{i}]: {ks.raw} ({ks.kind})")
        for i, d in enumerate(raw_rows[:COPYLINE_DEBUG_MAX_HITS], 1):
            log(f"[DEBUG] name[{i}]: {d.get('name','')[:120]}")

    # 5) Фильтр по НАЧАЛУ названия
    filtered_rows: List[Dict[str,str]] = []
    filtered_out = 0
    for d in raw_rows:
        ok, _ = name_passes_prefix(d.get("name",""), keys)
        drop = (COPYLINE_KEYWORDS_MODE == "exclude" and ok) or (COPYLINE_KEYWORDS_MODE == "include" and not ok)
        if drop:
            filtered_out += 1
        else:
            filtered_rows.append(d)

    # 6) XML структура
    root = ET.Element("yml_catalog")
    root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(root, "shop")
    offers = ET.SubElement(shop, "offers")

    prices_updated = 0
    vendors_detected = 0
    available_true = 0

    for row in filtered_rows:
        name = row.get("name","").strip()
        if not name:
            continue

        # ID и артикул
        article = extract_article_from_any(row)
        offer_id = row.get("sku") or article or stable_id_from(name)
        offer = ET.SubElement(offers, "offer", {"id": offer_id})

        # name
        ET.SubElement(offer, "name").text = name

        # picture (если есть в XLSX)
        img = (row.get("img") or "").strip()
        if img:
            ET.SubElement(offer, "picture").text = img

        # vendor (бренд)
        brand = normalize_brand((row.get("brand") or "").strip())
        if brand:
            ET.SubElement(offer, "vendor").text = brand
            vendors_detected += 1

        # description (1 строка)
        desc = _clean_one_line(row.get("desc", ""))
        if desc:
            ET.SubElement(offer, "description").text = desc

        # vendorCode (префикс CL + артикул/ид)
        if VENDORCODE_CREATE_IF_MISSING or article:
            code_body = article if article else offer_id
            ET.SubElement(offer, "vendorCode").text = f"{VENDORCODE_PREFIX}{code_body}"

        # price (по режиму)
        dealer = best_dealer_price(row)
        price_val: Optional[int] = None
        if dealer is not None and dealer > 0:
            if PRICE_MODE == "retail":
                price_val = compute_retail(dealer)
            else:
                price_val = int(dealer)  # как есть (целое)
        if price_val is not None and price_val > 0:
            ET.SubElement(offer, "price").text = str(int(price_val))
            ET.SubElement(offer, "currencyId").text = "KZT"
            prices_updated += 1

        # available (пытаемся понять, иначе true)
        avail_txt = _norm(row.get("avail",""))
        is_avail = True
        if avail_txt and re.search(r"\b(0|нет|no|false|out|нет в наличии)\b", avail_txt, re.I):
            is_avail = False
        ET.SubElement(offer, "available").text = "true" if is_avail else "false"
        if is_avail:
            available_true += 1

        # ВАЖНО: <url> НЕ добавляем

    # 7) Красивые отступы
    try:
        ET.indent(root, space="  ")
    except Exception:
        pass

    # 8) FEED_META
    meta = {
        "script_version": SCRIPT_VERSION,
        "supplier": SUPPLIER_NAME,
        "source": SUPPLIER_URL,
        "offers_total": offers_total,
        "offers_written": len(list(offers.findall("offer"))),
        "filtered_by_keywords": filtered_out,
        "prices_mode": PRICE_MODE,
        "prices_updated": prices_updated,
        "vendors_detected": vendors_detected,
        "available_set_true": available_true,
        "built_utc": now_utc_str(),
        "built_Asia/Almaty": now_almaty_str(),
    }
    root.insert(0, ET.Comment(render_feed_meta(meta)))

    # 9) Разрыв строки после FEED_META
    xml_bytes = ET.tostring(root, encoding=ENC, xml_declaration=True)
    xml_text  = xml_bytes.decode(ENC, errors="replace")
    xml_text  = re.sub(r"(-->)\s*(<shop>)", lambda m: f"{m.group(1)}\n  {m.group(2)}", xml_text)

    if DRY_RUN:
        log("[DRY_RUN=1] File not written.")
        return

    # 10) Запись YML
    os.makedirs(os.path.dirname(OUT_FILE_YML) or ".", exist_ok=True)
    with open(OUT_FILE_YML, "w", encoding=ENC, newline="\n") as f:
        f.write(xml_text)

    # .nojekyll — чтобы GitHub Pages отдавал как есть
    docs_dir = os.path.dirname(OUT_FILE_YML) or "docs"
    try:
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | offers={meta['offers_written']} | encoding={ENC} | script={SCRIPT_VERSION}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
