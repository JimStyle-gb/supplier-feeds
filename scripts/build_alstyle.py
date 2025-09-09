# -*- coding: utf-8 -*-
"""
Генератор YML (Yandex Market Language) для al-style:
- Забирает исходный XML у поставщика (с ретраями и проверкой ответа).
- Фильтрует офферы по списку категорий из docs/categories_alstyle.txt.
- Сохраняет ПОЛНУЮ структуру offer (deepcopy: все атрибуты и вложенные теги).
- Нормализует/дозаполняет <vendor> (убирает «Неизвестный производитель»).
- Форсированно добавляет префикс к <vendorCode> (по умолчанию AS, без дефиса).
- ВСТАВЛЯЕТ КОММЕНТАРИЙ FEED_META со «временем у поставщика»:
  supplier_feed_date (из исходного XML), http_last_modified (заголовок),
  offers_max_update (макс. «дата обновления» в офферах, если найдена).
"""

from __future__ import annotations

import os, sys, re, time
from copy import deepcopy
from typing import Dict, List, Set, Tuple, Optional
from xml.etree import ElementTree as ET
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo  # не используем в комментарии, оставлено на будущее
except Exception:
    ZoneInfo = None

import requests


# ===================== ПАРАМЕТРЫ =====================
SUPPLIER_NAME   = os.getenv("SUPPLIER_NAME", "alstyle")
SUPPLIER_URL    = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php")
OUT_FILE        = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC             = os.getenv("OUTPUT_ENCODING", "windows-1251")
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "docs/categories_alstyle.txt")

BASIC_USER      = os.getenv("BASIC_USER") or None
BASIC_PASS      = os.getenv("BASIC_PASS") or None

TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "30"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF   = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))

# Префикс для <vendorCode> (всегда добавляется, даже если уже есть похожий).
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS")  # без дефиса по умолчанию
# Создавать <vendorCode>, если он отсутствует (по умолчанию — нет).
VENDORCODE_CREATE_IF_MISSING = os.getenv("VENDORCODE_CREATE_IF_MISSING", "0").lower() in {"1","true","yes"}


# ===================== УТИЛИТЫ =====================

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARN: {msg}", flush=True, file=sys.stderr)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", flush=True, file=sys.stderr)
    sys.exit(code)

def fetch_xml(url: str, timeout: int, retries: int, backoff: float, auth=None) -> Tuple[bytes, Dict[str, str]]:
    """
    Надёжное скачивание XML:
    - RETRY с экспоненциальной задержкой
    - Проверка статуса и минимального размера
    - Базовая проверка типа содержимого
    Возвращает (bytes, headers)
    """
    sess = requests.Session()
    headers = {"User-Agent": "alstyle-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = sess.get(url, headers=headers, timeout=timeout, auth=auth, stream=True)
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            data = resp.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)} bytes)")

            # Разрешаем xml/text/plain/octet-stream; если метка странная — проверим "похоже ли на XML".
            if not any(t in ctype for t in ("xml", "text/plain", "application/octet-stream")):
                head = data[:64].lstrip()
                if not head.startswith(b"<"):
                    raise RuntimeError(f"unexpected content-type: {ctype!r}")

            return data, dict(resp.headers)
        except Exception as e:
            last_exc = e
            warn(f"fetch attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"fetch failed after {retries} attempts: {last_exc}")

def parse_xml_bytes(data: bytes) -> ET.Element:
    return ET.fromstring(data)

def get_text(el: ET.Element, tag: str) -> str:
    node = el.find(tag)
    return (node.text or "").strip() if node is not None else ""

def iter_local(elem: ET.Element, name: str):
    for child in elem.findall(name):
        yield child

# === Вспомогательная нормализация дат (пытаемся привести к ISO, если распознаётся) ===
_DT_PATTERNS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
    "%d.%m.%Y",
    "%Y-%m-%d",
]

def normalize_dt(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    # Убираем Z
    s_try = s.replace("Z", "+00:00")
    for fmt in _DT_PATTERNS:
        try:
            dt = datetime.strptime(s_try, fmt)
            # Если без TZ — считаем naive, выводим ISO без TZ
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    return s  # вернуть как есть, если не распознали

# ===================== РАБОТА С КАТЕГОРИЯМИ =====================

def build_category_graph(cats_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,Set[str]]]:
    id2name: Dict[str, str] = {}
    id2parent: Dict[str, str] = {}
    parent2children: Dict[str, Set[str]] = {}
    for c in iter_local(cats_el, "category"):
        cid = (c.attrib.get("id") or "").strip()
        pid = (c.attrib.get("parentId") or "").strip()
        name = (c.text or "").strip()
        if not cid:
            continue
        id2name[cid] = name
        if pid:
            id2parent[cid] = pid
            parent2children.setdefault(pid, set()).add(cid)
        else:
            id2parent.setdefault(cid, "")
    return id2name, id2parent, parent2children

def collect_descendants(start_ids: Set[str], parent2children: Dict[str,Set[str]]) -> Set[str]:
    out: Set[str] = set()
    stack = list(start_ids)
    while stack:
        x = stack.pop()
        if x in out:
            continue
        out.add(x)
        for ch in parent2children.get(x, ()):
            stack.append(ch)
    return out

def collect_ancestors(ids: Set[str], id2parent: Dict[str,str]) -> Set[str]:
    out: Set[str] = set()
    for cid in ids:
        cur = cid
        while True:
            pid = id2parent.get(cur, "")
            if not pid:
                break
            out.add(pid)
            cur = pid
    return out


# ===================== ПАРСИНГ ФАЙЛА ФИЛЬТРОВ =====================

def parse_selectors(path: str) -> Tuple[Set[str], List[str], List[re.Pattern]]:
    ids_filter: Set[str] = set()
    substrings: List[str] = []
    regexps: List[re.Pattern] = []
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("re:"):
                    pat = line[3:].strip()
                    if pat:
                        try:
                            regexps.append(re.compile(pat, re.I))
                        except re.error as e:
                            warn(f"bad regex in {path!r}: {pat!r} ({e})")
                    continue
                if line.isdigit() or ":" not in line:
                    ids_filter.add(line)
                else:
                    substrings.append(line.lower())
    except FileNotFoundError:
        warn(f"{path} not found — фильтр категорий НЕ будет применён")
    return ids_filter, substrings, regexps

def cat_matches(name: str, cid: str, ids_filter: Set[str], subs: List[str], regs: List[re.Pattern]) -> bool:
    if cid in ids_filter:
        return True
    lname = (name or "").lower()
    for s in subs:
        if s and s in lname:
            return True
    for r in regs:
        try:
            if r.search(name or ""):
                return True
        except Exception:
            continue
    return False


# ===================== НОРМАЛИЗАЦИЯ/ЗАПОЛНЕНИЕ <vendor> =====================

_BRAND_MAP = {
    "hp": "HP", "hewlett packard": "HP", "hewlett packard inc": "HP", "hp inc": "HP",
    "canon": "Canon", "canon inc": "Canon",
    "brother": "Brother",
    "kyocera": "Kyocera", "kyocera mita": "Kyocera",
    "xerox": "Xerox",
    "ricoh": "Ricoh",
    "epson": "Epson",
    "samsung": "Samsung",
    "panasonic": "Panasonic",
    "konica minolta": "Konica Minolta", "konica": "Konica Minolta",
    "sharp": "Sharp",
    "lexmark": "Lexmark",
    "pantum": "Pantum",
    "nv print": "NV Print", "nvprint": "NV Print", "nv  print": "NV Print",
    "akcent": "AKCENT",
    "vtt": "VTT",
}

_BRAND_PATTERNS = [
    (re.compile(r"^\s*hp\b", re.I), "HP"),
    (re.compile(r"^\s*canon\b", re.I), "Canon"),
    (re.compile(r"^\s*brother\b", re.I), "Brother"),
    (re.compile(r"^\s*kyocera\b", re.I), "Kyocera"),
    (re.compile(r"^\s*xerox\b", re.I), "Xerox"),
    (re.compile(r"^\s*ricoh\b", re.I), "Ricoh"),
    (re.compile(r"^\s*epson\b", re.I), "Epson"),
    (re.compile(r"^\s*samsung\b", re.I), "Samsung"),
    (re.compile(r"^\s*panasonic\b", re.I), "Panasonic"),
    (re.compile(r"^\s*konica\s*-?\s*minolta\b", re.I), "Konica Minolta"),
    (re.compile(r"^\s*sharp\b", re.I), "Sharp"),
    (re.compile(r"^\s*lexmark\b", re.I), "Lexmark"),
    (re.compile(r"^\s*pantum\b", re.I), "Pantum"),
    (re.compile(r"^\s*nv\s*-?\s*print\b", re.I), "NV Print"),
    (re.compile(r"^\s*akcent\b", re.I), "AKCENT"),
    (re.compile(r"^\s*vtt\b", re.I), "VTT"),
]

def _norm_key(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def normalize_brand(raw: str) -> str:
    k = _norm_key(raw)
    if not k:
        return ""
    if k in _BRAND_MAP:
        return _BRAND_MAP[k]
    for pat, val in _BRAND_PATTERNS:
        if pat.search(raw or ""):
            return val
    return " ".join(w.capitalize() for w in k.split())

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, int, int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0, 0, 0)

    normalized = 0
    filled_param = 0
    filled_name = 0

    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        txt = (ven.text or "").strip() if ven is not None and ven.text else ""

        if txt:
            norm = normalize_brand(txt)
            if norm and norm != txt:
                ven.text = norm
                normalized += 1
            continue

        candidate = ""
        for p in offer.findall("param"):
            nm = (p.attrib.get("name") or "").strip().lower()
            if "бренд" in nm or "производ" in nm:
                candidate = (p.text or "").strip()
                if candidate:
                    break
        if candidate:
            val = normalize_brand(candidate)
            if ven is None:
                ven = ET.SubElement(offer, "vendor")
            ven.text = val
            filled_param += 1
            continue

        name_val = get_text(offer, "name")
        if name_val:
            for pat, brand in _BRAND_PATTERNS:
                if pat.search(name_val):
                    if ven is None:
                        ven = ET.SubElement(offer, "vendor")
                    ven.text = brand
                    filled_name += 1
                    break

    return (normalized, filled_param, filled_name)


# ===================== ПОСТ-ОБРАБОТКА <vendorCode> =====================

def force_prefix_vendorcode(shop_el: ET.Element, prefix: str, create_if_missing: bool = False) -> Tuple[int, int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return 0, 0
    total = 0
    created = 0
    for offer in offers_el.findall("offer"):
        vc = offer.find("vendorCode")
        if vc is None:
            if create_if_missing:
                vc = ET.SubElement(offer, "vendorCode")
                created += 1
                old = ""
            else:
                continue
        else:
            old = vc.text or ""
        vc.text = f"{prefix}{old}"
        total += 1
    return total, created


# ===================== ИЗВЛЕЧЕНИЕ ДАТ У ПОСТАВЩИКА =====================

def extract_supplier_feed_date(root: ET.Element) -> str:
    # 1) yml_catalog@date
    val = (root.attrib.get("date") or "").strip()
    if val:
        return normalize_dt(val) or val
    # 2) shop/generation-date | shop/generation_date | shop/generationDate | shop/date
    for path in ("shop/generation-date", "shop/generation_date", "shop/generationDate", "shop/date"):
        s = (root.findtext(path) or "").strip()
        if s:
            return normalize_dt(s) or s
    # 3) yml_catalog/date (редко)
    s = (root.findtext("date") or "").strip()
    if s:
        return normalize_dt(s) or s
    return ""

def extract_offers_max_update(offers_el: ET.Element) -> Tuple[str, int]:
    """
    Ищем «дату обновления» внутри офферов в распространённых местах:
      — теги: updated_at, update_date, modified, modified_time, last_update, lastmod, last_modified, date_modify, date_update
      — <param name="..."> с подстроками: обнов, update, modified
    Возвращаем (максимальная дата raw/нормализованная если смогли, сколько значений найдено).
    """
    TAGS = {
        "updated_at", "update_date", "modified", "modified_time",
        "last_update", "lastmod", "last_modified", "date_modify", "date_update"
    }
    found: List[str] = []
    for offer in offers_el.findall("offer"):
        # Теги
        for tag in TAGS:
            t = offer.find(tag)
            if t is not None and (t.text or "").strip():
                found.append((t.text or "").strip())
        # Параметры
        for p in offer.findall("param"):
            nm = (p.attrib.get("name") or "").strip().lower()
            if any(k in nm for k in ("обнов", "update", "modified", "измени")):
                if (p.text or "").strip():
                    found.append((p.text or "").strip())
    if not found:
        return ("", 0)
    # Нормализуем для сравнения
    def ts(s: str) -> Tuple[int, str]:
        norm = normalize_dt(s) or s
        # пытаться сравнить по «YYYY-MM-DD HH:MM:SS» -> в число
        m = re.match(r"(\d{4})[-.](\d{2})[-.](\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?", norm)
        if m:
            y, mo, d, h, mi, se = m.group(1,2,3,4,5,6 if m.group(6) else 0)
            try:
                return (int(f"{y}{mo}{d}{h}{mi}{se or 0}"), norm)
            except Exception:
                pass
        return (0, norm)
    best = max(found, key=lambda s: ts(s)[0])
    best_norm = normalize_dt(best) or best
    return (best_norm, len(found))


# ===================== ОСНОВНАЯ ЛОГИКА =====================

def main() -> None:
    auth = (BASIC_USER, BASIC_PASS) if BASIC_USER and BASIC_PASS else None

    log(f"Source: {SUPPLIER_URL}")
    log(f"Categories file: {CATEGORIES_FILE}")
    data, headers = fetch_xml(SUPPLIER_URL, TIMEOUT_S, RETRIES, RETRY_BACKOFF, auth=auth)
    root = parse_xml_bytes(data)

    http_last_modified = headers.get("Last-Modified", "").strip()

    supplier_feed_date = extract_supplier_feed_date(root)

    shop = root.find("shop")
    if shop is None:
        err("XML: <shop> not found")

    cats_el = shop.find("categories")
    offers_el = shop.find("offers")
    if cats_el is None or offers_el is None:
        err("XML: <categories> or <offers> not found")

    # Дата по офферам (если внутри есть поля «обновления»)
    offers_max_update, offers_updates_detected = extract_offers_max_update(offers_el)

    id2name, id2parent, parent2children = build_category_graph(cats_el)

    # Читаем фильтры
    ids_filter, subs, regs = parse_selectors(CATEGORIES_FILE)
    have_selectors = bool(ids_filter or subs or regs)

    # Определяем сохраняемые категории
    matched_cat_ids = {cid for cid, nm in id2name.items() if cat_matches(nm, cid, ids_filter, subs, regs)}
    keep_cat_ids = collect_descendants(matched_cat_ids, parent2children) if matched_cat_ids else set()

    # Фильтрация офферов (fail-closed при наличии селекторов)
    offers_in = list(iter_local(offers_el, "offer"))
    if have_selectors:
        used_offers = [o for o in offers_in if get_text(o, "categoryId") in keep_cat_ids]
        if not used_offers:
            warn("фильтры заданы, но офферов не найдено — проверь docs/categories_alstyle.txt")
    else:
        used_offers = offers_in

    # Фактические категории по найденным офферам + их предки
    used_cat_ids = {get_text(o, "categoryId") for o in used_offers if get_text(o, "categoryId")}
    used_cat_ids |= collect_ancestors(used_cat_ids, id2parent)

    # Сборка выходного XML
    out_root = ET.Element("yml_catalog")
    out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")

    # === FEED_META КОММЕНТАРИЙ С «ДАТОЙ У ПОСТАВЩИКА» ===
    meta = (
        f"FEED_META supplier={SUPPLIER_NAME} "
        f"supplier_feed_date={supplier_feed_date or 'n/a'} "
        f"http_last_modified={http_last_modified or 'n/a'} "
        f"offers_max_update={offers_max_update or 'n/a'} "
        f"offers_updates_detected={offers_updates_detected}"
    )
    out_root.insert(0, ET.Comment(meta))

    # Категории — только используемые
    out_cats = ET.SubElement(out_shop, "categories")

    def depth(cid: str) -> int:
        d = 0
        cur = cid
        while id2parent.get(cur):
            d += 1
            cur = id2parent[cur]
        return d

    for cid in sorted(used_cat_ids, key=lambda c: (depth(c), id2name.get(c, ""), c)):
        if cid not in id2name:
            continue
        attrs = {"id": cid}
        pid = id2parent.get(cid, "")
        if pid and pid in used_cat_ids:
            attrs["parentId"] = pid
        c_el = ET.SubElement(out_cats, "category", attrs)
        c_el.text = id2name.get(cid, "")

    # Офферы — глубокая копия исходных узлов
    out_offers = ET.SubElement(out_shop, "offers")
    for o in used_offers:
        out_offers.append(deepcopy(o))

    # Пост-обработка: производитель
    norm_cnt, fill_param_cnt, fill_name_cnt = ensure_vendor(out_shop)

    # Пост-обработка: префикс к <vendorCode>
    total_prefixed, created_nodes = force_prefix_vendorcode(
        out_shop,
        prefix=VENDORCODE_PREFIX,
        create_if_missing=VENDORCODE_CREATE_IF_MISSING,
    )

    # Красивый вывод
    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    ET.ElementTree(out_root).write(OUT_FILE, encoding=ENC, xml_declaration=True)

    # Логи
    log(f"Supplier feed date: {supplier_feed_date or 'n/a'}")
    log(f"HTTP Last-Modified: {http_last_modified or 'n/a'}")
    log(f"Offers max update: {offers_max_update or 'n/a'} (hits={offers_updates_detected})")
    log(f"Selectors: ids={len(ids_filter)}, subs={len(subs)}, regs={len(regs)} (present={have_selectors})")
    log(f"Vendor fixed: normalized={norm_cnt}, filled_from_param={fill_param_cnt}, filled_from_name={fill_name_cnt}")
    log(f"Prefixed vendorCode: total={total_prefixed}, created={created_nodes}, prefix='{VENDORCODE_PREFIX}', create_if_missing={VENDORCODE_CREATE_IF_MISSING}")
    log(f"Wrote: {OUT_FILE} | offers={len(used_offers)} | cats={len(used_cat_ids)} | encoding={ENC}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
