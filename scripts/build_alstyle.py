# -*- coding: utf-8 -*-
"""
Генератор YML (Yandex Market Language) для al-style:
- Забирает исходный XML у поставщика (с ретраями и проверкой ответа).
- Фильтрует офферы по списку категорий из docs/categories_alstyle.txt.
- Сохраняет ПОЛНУЮ структуру offer (deepcopy: все атрибуты и вложенные теги).
- Собирает дерево <categories> только по используемым категориям + их предкам.
- ДОПОЛНИТЕЛЬНО: в конце всегда добавляет префикс к <vendorCode> (по умолчанию AS без дефиса).
- Пишет результат в docs/alstyle.yml с кодировкой из ENV (по умолчанию windows-1251).
"""

from __future__ import annotations

import os, sys, re, time
from copy import deepcopy
from typing import Dict, List, Set, Tuple
from xml.etree import ElementTree as ET

import requests


# ===================== ПАРАМЕТРЫ =====================
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

def fetch_xml(url: str, timeout: int, retries: int, backoff: float, auth=None) -> bytes:
    """
    Надёжное скачивание XML:
    - RETRY с экспоненциальной задержкой
    - Проверка статуса и минимального размера
    - Базовая проверка типа содержимого
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

            return data
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
    # Без пространств имён — простой перебор
    for child in elem.findall(name):
        yield child


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
    """
    Возвращает:
      ids_filter: множество явных ID категорий (строкой)
      substrings: список подстрок для поиска в названии (регистронезависимо)
      regexps:    список компилированных regexp (по префиксу re:)
    """
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


# ===================== ПОСТ-ОБРАБОТКА VENDORCODE =====================

def force_prefix_vendorcode(shop_el: ET.Element, prefix: str, create_if_missing: bool = False) -> Tuple[int, int]:
    """
    ВСЕГДА добавляет 'prefix' к тексту <vendorCode>.
    НИЧЕГО не вырезает и не нормализует; дубли префиксов допускаются.
    Возвращает (total_prefixed, created_nodes).
    """
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


# ===================== ОСНОВНАЯ ЛОГИКА =====================

def main() -> None:
    auth = (BASIC_USER, BASIC_PASS) if BASIC_USER and BASIC_PASS else None

    log(f"Source: {SUPPLIER_URL}")
    log(f"Categories file: {CATEGORIES_FILE}")
    data = fetch_xml(SUPPLIER_URL, TIMEOUT_S, RETRIES, RETRY_BACKOFF, auth=auth)
    root = parse_xml_bytes(data)

    shop = root.find("shop")
    if shop is None:
        err("XML: <shop> not found")

    cats_el = shop.find("categories")
    offers_el = shop.find("offers")
    if cats_el is None or offers_el is None:
        err("XML: <categories> or <offers> not found")

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
        used_offers = offers_in  # нет фильтров — берём всё

    # Фактические категории по найденным офферам + их предки
    used_cat_ids = {get_text(o, "categoryId") for o in used_offers if get_text(o, "categoryId")}
    used_cat_ids |= collect_ancestors(used_cat_ids, id2parent)

    # Сборка выходного XML
    out_root = ET.Element("yml_catalog")
    out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")

    # Категории — только используемые
    out_cats = ET.SubElement(out_shop, "categories")

    def depth(cid: str) -> int:
        d = 0
        cur = cid
        while id2parent.get(cur):
            d += 1
            cur = id2parent[cur]
        return d

    # Стабильная сортировка: (глубина, имя, id)
    for cid in sorted(used_cat_ids, key=lambda c: (depth(c), id2name.get(c, ""), c)):
        if cid not in id2name:
            continue
        attrs = {"id": cid}
        pid = id2parent.get(cid, "")
        if pid and pid in used_cat_ids:
            attrs["parentId"] = pid
        c_el = ET.SubElement(out_cats, "category", attrs)
        c_el.text = id2name.get(cid, "")

    # Офферы — глубокая копия исходных узлов (не теряем вложенные теги/атрибуты)
    out_offers = ET.SubElement(out_shop, "offers")
    for o in used_offers:
        out_offers.append(deepcopy(o))

    # === НОВОЕ: форсированное добавление префикса к <vendorCode> ===
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

    # Логи-итоги
    log(f"Selectors: ids={len(ids_filter)}, subs={len(subs)}, regs={len(regs)} (present={have_selectors})")
    log(f"Prefixed vendorCode: total={total_prefixed}, created={created_nodes}, prefix='{VENDORCODE_PREFIX}', create_if_missing={VENDORCODE_CREATE_IF_MISSING}")
    log(f"Wrote: {OUT_FILE} | offers={len(used_offers)} | cats={len(used_cat_ids)} | encoding={ENC}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
