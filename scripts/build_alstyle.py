# -*- coding: utf-8 -*-
"""
Alstyle → YML (единый шаблон)
- FEED_META: supplier, source, source_date, built_utc, built_Asia/Almaty
- Обязательный <vendorCode> у каждого оффера (если нет — создаём), префикс "AS" (без дефиса)
- Нормализация <vendor> ТОЛЬКО по OEM-брендам; названия ПОСТАВЩИКОВ никогда не подставляем
- Фильтр по docs/categories_alstyle.txt (аналогично akcent)
- Категории: только используемые + их предки
- Канонический порядок тегов в <offer>
- Вывод: docs/alstyle.yml (по умолчанию windows-1251)
"""

from __future__ import annotations

import os, sys, re, time, hashlib
from copy import deepcopy
from typing import Dict, List, Set, Tuple, Optional
from xml.etree import ElementTree as ET
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests


# ===================== ПАРАМЕТРЫ =====================

SUPPLIER_NAME   = os.getenv("SUPPLIER_NAME", "alstyle")
SUPPLIER_URL    = (
    os.getenv("SUPPLIER_URL")
    or os.getenv("ALSTYLE_URL")
    or "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
).strip()
OUT_FILE        = os.getenv("OUT_FILE", "docs/alstyle.yml")
ENC             = os.getenv("OUTPUT_ENCODING", "windows-1251")
KEYWORDS_FILE   = os.getenv("CATEGORIES_FILE", "docs/categories_alstyle.txt")

TIMEOUT_S       = int(os.getenv("TIMEOUT_S", "30"))
RETRIES         = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF   = float(os.getenv("RETRY_BACKOFF_S", "2"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "1500"))

# Префикс для <vendorCode> (всегда добавляется).
VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS")

# Поставщики, которые НЕЛЬЗЯ подставлять в <vendor>
SUPPLIER_BLOCKLIST = {
    "akcent", "vtt", "alstyle", "copyline", "nv print", "nvprint", "nv  print"
}


# ===================== УТИЛИТЫ =====================

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"WARN: {msg}", flush=True, file=sys.stderr)

def err(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", flush=True, file=sys.stderr)
    sys.exit(code)

def fetch_xml(url: str, timeout: int, retries: int, backoff: float) -> Tuple[bytes, Dict[str, str]]:
    if not url:
        err("SUPPLIER_URL is empty — укажи URL фида.")
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = sess.get(url, headers=headers, timeout=timeout, stream=True)
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}")
            data = resp.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)} bytes)")
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


# ===================== ДАТЫ/МЕТА =====================

_DT_PATTERNS = [
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S",
    "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y",
    "%Y-%m-%d",
]

def normalize_dt(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    s_try = s.replace("Z", "+00:00")
    for fmt in _DT_PATTERNS:
        try:
            dt = datetime.strptime(s_try, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    return s

def extract_source_date(root: ET.Element) -> str:
    val = (root.attrib.get("date") or "").strip()
    if val:
        return normalize_dt(val) or val
    for path in ("shop/generation-date", "shop/generation_date", "shop/generationDate", "shop/date", "date"):
        s = (root.findtext(path) or "").strip()
        if s:
            return normalize_dt(s) or s
    return ""


# ===================== ФИЛЬТР ПО КЛЮЧАМ =====================

def _norm(s: str) -> str:
    s = (s or "").lower().replace("ё", "е").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_keywords(path: str) -> Tuple[List[str], List[re.Pattern]]:
    prefixes: List[str] = []
    regexps: List[re.Pattern] = []
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            for raw in f:
                line = (raw or "").strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("re:"):
                    pat = line[3:].strip()
                    if not pat:
                        continue
                    try:
                        regexps.append(re.compile(pat, re.I))
                    except re.error as e:
                        warn(f"bad regex in {path!r}: {pat!r} ({e})")
                else:
                    prefixes.append(_norm(line))
    except FileNotFoundError:
        warn(f"{path} not found — фильтр ключей НЕ будет применён")
    return prefixes, regexps

def matches_keywords(title: str, prefixes: List[str], regexps: List[re.Pattern]) -> bool:
    if not prefixes and not regexps:
        return True
    nm = _norm(title)
    if any(nm.startswith(p) for p in prefixes):
        return True
    for r in regexps:
        try:
            if r.search(title or ""):
                return True
        except Exception:
            continue
    return False


# ===================== НОРМАЛИЗАЦИЯ <vendor> =====================

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
]
SUPPLIER_BLOCKLIST = {
    "akcent", "vtt", "alstyle", "copyline", "nv print", "nvprint", "nv  print"
}
def normalize_brand(raw: str) -> str:
    k = _norm(raw)
    if not k:
        return ""
    if k in SUPPLIER_BLOCKLIST:
        return ""
    if k in _BRAND_MAP:
        return _BRAND_MAP[k]
    for pat, val in _BRAND_PATTERNS:
        if pat.search(raw or ""):
            return val
    return "" if k in SUPPLIER_BLOCKLIST else " ".join(w.capitalize() for w in k.split())

def ensure_vendor(shop_el: ET.Element) -> Tuple[int, int, int]:
    offers_el = shop_el.find("offers")
    if offers_el is None:
        return (0, 0, 0)
    normalized = filled_param = filled_name = 0
    for offer in offers_el.findall("offer"):
        ven = offer.find("vendor")
        txt = (ven.text or "").strip() if ven is not None and ven.text else ""
        if txt:
            norm = normalize_brand(txt)
            if norm and norm != txt:
                ven.text = norm
            continue
        candidate = ""
        for p in offer.findall("param") + offer.findall("Param"):
            nm = (p.attrib.get("name") or "").strip().lower()
            if "бренд" in nm or "производ" in nm:
                candidate = (p.text or "").strip()
                if candidate:
                    break
        if candidate:
            val = normalize_brand(candidate)
            if val:
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


# ===================== ОБЯЗАТЕЛЬНЫЙ <vendorCode> =====================

def derive_vendorcode_base(offer: ET.Element) -> str:
    base = (offer.attrib.get("article") or "").strip()
    if base:
        return base
    for tag in ("Offer_ID", "OfferID", "offer_id"):
        t = get_text(offer, tag)
        if t:
            return t
    base = (offer.attrib.get("id") or "").strip()
    if base:
        return base
    t = get_text(offer, "vendorCode")
    if t:
        return t
    name_val = get_text(offer, "name") or "UNK"
    return hashlib.md5(name_val.encode("utf-8", errors="ignore")).hexdigest()[:10].upper()

def ensure_vendorcode_with_prefix(offer: ET.Element, prefix: str) -> None:
    vc = offer.find("vendorCode")
    if vc is None:
        vc = ET.SubElement(offer, "vendorCode")
        base = derive_vendorcode_base(offer)
        vc.text = f"{prefix}{base}"
    else:
        vc.text = f"{prefix}{(vc.text or '')}"


# ===================== КАТЕГОРИИ =====================

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


# ===================== КАНОНИЗАЦИЯ ПОРЯДКА ТЕГОВ В <offer> =====================

PRIMARY_ORDER = [
    "name",
    "vendor",
    "vendorCode",
    "categoryId",
    "price",
    "currencyId",
    "oldprice",
    "url",
]
def canonicalize_offer_children(offer: ET.Element) -> None:
    children = list(offer)
    if not children:
        return
    by_tag: Dict[str, List[ET.Element]] = {}
    for ch in children:
        by_tag.setdefault(ch.tag, []).append(ch)
    for ch in children:
        offer.remove(ch)
    for tag in PRIMARY_ORDER:
        for el in by_tag.pop(tag, []):
            offer.append(el)
    for el in by_tag.pop("picture", []):
        offer.append(el)
    rest_tags = [t for t in list(by_tag.keys()) if t.lower() not in {"param"}]
    for tag in sorted(rest_tags):
        for el in by_tag.pop(tag, []):
            offer.append(el)
    for tag in ["param", "Param"]:
        for el in by_tag.pop(tag, []):
            offer.append(el)


# ===================== ОСНОВНАЯ ЛОГИКА =====================

def main() -> None:
    data, headers = fetch_xml(SUPPLIER_URL, TIMEOUT_S, RETRIES, RETRY_BACKOFF)
    root = parse_xml_bytes(data)

    source_date = extract_source_date(root)

    shop = root.find("shop")
    cats_el = shop.find("categories") if shop is not None else None
    offers_el = shop.find("offers") if shop is not None else None
    if shop is None or cats_el is None or offers_el is None:
        err("XML: <shop>/<categories>/<offers> not found")

    id2name, id2parent, _ = build_category_graph(cats_el)

    prefixes, regexps = load_keywords(KEYWORDS_FILE)
    have_filter = bool(prefixes or regexps)

    offers_in = list(iter_local(offers_el, "offer"))
    if have_filter:
        used_offers = [o for o in offers_in if matches_keywords(get_text(o, "name"), prefixes, regexps)]
        if not used_offers:
            warn("ключи заданы, но офферов не найдено — проверь docs/categories_alstyle.txt")
    else:
        used_offers = offers_in

    used_cat_ids: Set[str] = {get_text(o, "categoryId") for o in used_offers if get_text(o, "categoryId")}
    used_cat_ids = {cid for cid in used_cat_ids if cid}
    used_cat_ids |= collect_ancestors(used_cat_ids, id2parent)

    out_root = ET.Element("yml_catalog")
    out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop = ET.SubElement(out_root, "shop")

    built_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if ZoneInfo:
        built_local = datetime.now(ZoneInfo("Asia/Almaty")).strftime("%Y-%m-%d %H:%M:%S %Z")
    else:
        built_local = time.strftime("%Y-%m-%d %H:%M:%S")
    meta = (
        f"FEED_META supplier={SUPPLIER_NAME} "
        f"source={SUPPLIER_URL} "
        f"source_date={source_date or 'n/a'} "
        f"built_utc={built_utc} "
        f"built_Asia/Almaty={built_local}"
    )
    out_root.insert(0, ET.Comment(meta))

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

    out_offers = ET.SubElement(out_shop, "offers")
    for o in used_offers:
        o2 = deepcopy(o)

        # vendor: нормализуем только OEM-бренды, поставщиков не подставляем
        ven = o2.find("vendor")
        ven_txt = (ven.text or "").strip() if ven is not None and ven.text else ""
        if ven_txt:
            norm = normalize_brand(ven_txt)
            if norm and norm != ven_txt:
                ven.text = norm
        else:
            candidate = ""
            for p in o2.findall("param") + o2.findall("Param"):
                nm = (p.attrib.get("name") or "").strip().lower()
                if "бренд" in nm or "производ" in nm:
                    candidate = (p.text or "").strip()
                    if candidate:
                        break
            if candidate:
                val = normalize_brand(candidate)
                if val:
                    if ven is None:
                        ven = ET.SubElement(o2, "vendor")
                    ven.text = val
            else:
                name_val = get_text(o2, "name")
                if name_val:
                    for pat, brand in _BRAND_PATTERNS:
                        if pat.search(name_val):
                            if ven is None:
                                ven = ET.SubElement(o2, "vendor")
                            ven.text = brand
                            break

        ensure_vendorcode_with_prefix(o2, VENDORCODE_PREFIX)
        canonicalize_offer_children(o2)
        out_offers.append(o2)

    try:
        ET.indent(out_root, space="  ")
    except Exception:
        pass

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    ET.ElementTree(out_root).write(OUT_FILE, encoding=ENC, xml_declaration=True)

    log(f"Source date: {source_date or 'n/a'}")
    log(f"Keywords present: {bool(prefixes or regexps)} | prefixes={len(prefixes)} regexps={len(regexps)}")
    log(f"Wrote: {OUT_FILE} | offers={len(used_offers)} | cats={len(used_cat_ids)} | encoding={ENC}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
