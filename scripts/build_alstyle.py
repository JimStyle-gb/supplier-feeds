# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
AlStyle build script (KOD4) — description extractor update
Only <description>-related functions were changed to reliably build
the <h3>Характеристики</h3> block from text with and without ':'.
Other logic remains untouched.
"""

import os, sys, re, time, random, hashlib, urllib.parse, html
from typing import Dict, List, Tuple, Optional, Set
from copy import deepcopy
from xml.etree import ElementTree as ET
from datetime import datetime, timezone, timedelta

# ----------------- tiny log utils -----------------
def log(msg: str) -> None:
    print(msg, file=sys.stdout, flush=True)

def warn(msg: str) -> None:
    print("WARN:", msg, file=sys.stderr, flush=True)

def err(msg: str) -> None:
    print("ERROR:", msg, file=sys.stderr, flush=True)
    raise SystemExit(1)

# =====================================================================
#                 DESCRIPTION: NEW/UPDATED FUNCTIONS ONLY
# =====================================================================

import re as _re_desc

def _has_html_tags(_t: str) -> bool:
    """Detects if text already contains HTML structure."""
    return bool(_re_desc.search(r"<(p|ul|ol|li|h1|h2|h3)\b", _t or "", flags=_re_desc.I))

def _normalize_ws(_t: str) -> str:
    """Normalize whitespace/newlines and common bullet entities."""
    t = (_t or "").replace("\r", "\n")
    t = _re_desc.sub(r"[ \t]*\n[ \t]*", "\n", t)
    t = _re_desc.sub(r"\n{3,}", "\n\n", t)
    t = t.replace("&#9679;", "•").replace("●", "•").replace("&#215;", "×")
    return t.strip()

# Canonical spec keys and preferred ordering
_KEY_SYNONYMS = {
    "емкость": "Ёмкость",
    "ёмикость": "Ёмкость",
    "емкость батареи": "Ёмкость батареи",
    "ёмкость батареи": "Ёмкость батареи",
    "battery": "Ёмкость батареи",
    "питание": "Напряжение",
    "напряжение": "Напряжение",
    "ток": "Ток",
    "частота": "Частота",
    "скорость печати": "Скорость печати",
    "разъемы": "Интерфейсы",
    "разъёмы": "Интерфейсы",
    "порты": "Интерфейсы",
    "интерфейсы": "Интерфейсы",
    "габариты": "Размеры",
    "размеры": "Размеры",
    "масса": "Вес",
    "вес": "Вес",
    "давление": "Давление",
    "давление помпы": "Давление",
    "диагональ": "Диагональ",
    "объём": "Объём",
    "объем": "Объём",
    "тип": "Тип",
    "модель": "Модель",
    "бренд": "Бренд",
    "гарантия": "Гарантия",
}

_KEY_ORDER = [
    "Бренд","Модель","Тип","Мощность","Ёмкость батареи","Ёмкость","Диагональ",
    "Размеры","Вес","Интерфейсы","Совместимость","Напряжение","Ток","Частота",
    "Скорость печати","Давление","Объём","Гарантия"
]

def _canon_spec_key(k: str) -> str:
    """Map synonyms to canonical key; otherwise capitalize the first letter."""
    k_raw = (k or "").strip().strip(" .,:;—-")
    if not k_raw:
        return ""
    k0 = k_raw.lower()
    canon = _KEY_SYNONYMS.get(k0)
    return canon if canon else (k_raw[:1].upper() + k_raw[1:])

def _normalize_units(v: str) -> str:
    """Normalize units, replace x/х with ×, collapse spaces, trim punctuation."""
    s = (v or "").strip()
    if not s:
        return s
    s = _re_desc.sub(r"[\u00A0\u2009\u200A\u202F]", " ", s)  # NBSP/thin spaces -> space
    s = s.replace("мАч", "мА·ч").replace("mAh", "мА·ч")
    s = _re_desc.sub(r"(?<=\d)[xх](?=\d)", "×", s)
    s = _re_desc.sub(r"\s*×\s*", "×", s)
    s = _re_desc.sub(r"\s{2,}", " ", s)
    s = s.strip(" ;,.")
    return s

def _parse_size_kv(_t: str) -> Optional[Tuple[str,str]]:
    """Detect sizes like 200x300x50 (optional unit) -> ('Размеры','200×300×50 мм')."""
    m = _re_desc.search(r"(?i)\b(\d+(?:[.,]\d+)?)\s*[x×х]\s*(\d+(?:[.,]\d+)?)\s*[x×х]\s*(\d+(?:[.,]\d+)?)(?:\s*(мм|см|mm|cm))?", _t or "")
    if not m:
        return None
    a,b,c,unit = m.groups()
    unit_norm = (unit or "мм").lower()
    if unit_norm == "mm": unit_norm = "мм"
    if unit_norm == "cm": unit_norm = "см"
    val = f"{a}×{b}×{c} {unit_norm}"
    return ("Размеры", _normalize_units(val))

# For numeric-without-colon patterns
_KEY_WORDS = [
    "Мощность","Вес","Ёмкость батареи","Ёмкость","Давление","Диагональ","Напряжение",
    "Ток","Частота","Скорость печати","Объём","Объем","Размеры","Габариты",
    "Ёмкость резервуара","Емкость резервуара","Ёмкость чаши","Емкость чаши",
    "Интерфейсы","Порты","Разъёмы","Разъемы"
]
_KEY_WORDS_RE = "(?:" + "|".join([_re_desc.escape(k) for k in _KEY_WORDS]) + ")"
_UNIT_WORD = r'(?:Вт|ВА|В|А|мА·ч|мАч|Гц|ГГц|кг|г|л|бар|мм|см|дюйм|"|%|rpm|об/мин|мин|сек|с)'

def _extract_kv_specs(_t: str) -> List[Tuple[str,str]]:
    """
    Extract ('Ключ','Значение') from free text:
      1) explicit separators: ':', '—', '-', '='
      2) key + number + unit (without colon)
      3) reverse: number + unit + key
      4) sizes: 200x300x50
      5) "Интерфейсы ..." with no colon
    """
    t = _normalize_ws(_t)
    specs: List[Tuple[str,str]] = []
    seen: Dict[str,int] = {}

    # 4) sizes once per whole text
    sz = _parse_size_kv(t)
    if sz:
        k,v = sz
        k = _canon_spec_key(k); v = _normalize_units(v)
        seen[k] = len(specs); specs.append((k,v))

    for ln in t.split("\n"):
        s = ln.strip().strip("-•").strip()
        if not s:
            continue

        # 1) explicit separators
        m = _re_desc.match(r"(?i)^([А-ЯЁA-Za-z0-9 _./()«»\-]{2,30})\s*(?:[:=]|—|–|-)\s*([^\n]+)$", s)
        if m:
            k_raw, v_raw = m.group(1), m.group(2)
            k = _canon_spec_key(k_raw)
            v = _normalize_units(v_raw)
            if k and v:
                if k in seen:
                    idx = seen[k]
                    if len(v) > len(specs[idx][1]):
                        specs[idx] = (k, v)
                else:
                    seen[k] = len(specs); specs.append((k, v))
            continue

        # 2) key + number + unit
        m2 = _re_desc.search(fr"(?i)\b({_KEY_WORDS_RE})\s+(\d+(?:[.,]\d+)?)\s*({_UNIT_WORD})\b", s)
        if m2:
            k_raw, num, unit = m2.group(1), m2.group(2), m2.group(3)
            k = _canon_spec_key(k_raw)
            v = _normalize_units(f"{num} {unit}")
            if k and v:
                if k in seen:
                    idx = seen[k]
                    if len(v) > len(specs[idx][1]):
                        specs[idx] = (k, v)
                else:
                    seen[k] = len(specs); specs.append((k, v))

        # 3) number + unit + key
        m3 = _re_desc.search(fr"(?i)\b(\d+(?:[.,]\d+)?)\s*({_UNIT_WORD})\s+({_KEY_WORDS_RE})\b", s)
        if m3:
            num, unit, k_raw = m3.group(1), m3.group(2), m3.group(3)
            k = _canon_spec_key(k_raw)
            v = _normalize_units(f"{num} {unit}")
            if k and v:
                if k in seen:
                    idx = seen[k]
                    if len(v) > len(specs[idx][1]):
                        specs[idx] = (k, v)
                else:
                    seen[k] = len(specs); specs.append((k, v))

        # 5) "Интерфейсы ..." without colon
        m4 = _re_desc.search(r"(?i)\b(Интерфейсы|Порты|Разъёмы|Разъемы)\b\s+(.+)", s)
        if m4:
            k = _canon_spec_key(m4.group(1))
            v = _normalize_units(_re_desc.sub(r"[\s,;]+$", "", m4.group(2)))
            if k and v:
                if k in seen:
                    idx = seen[k]
                    if len(v) > len(specs[idx][1]):
                        specs[idx] = (k, v)
                else:
                    seen[k] = len(specs); specs.append((k, v))

    # Drop binary marketing noise
    cleaned: List[Tuple[str,str]] = []
    for k,v in specs:
        vv = (v or "").strip().lower()
        kk = (k or "").strip().lower()
        if vv in {"да","есть","true","yes"} and kk not in {"наличие","wi-fi","bluetooth"}:
            continue
        cleaned.append((k,v))

    # Sort: preferred order, others go after
    order_index = {key:i for i,key in enumerate(_KEY_ORDER)}
    cleaned.sort(key=lambda kv: order_index.get(kv[0], 10_000))
    return cleaned

def _extract_ports(_t: str) -> List[str]:
    """Lightweight extraction of ports/connectors list, if present."""
    ports: List[str] = []
    m = _re_desc.search(r"(?i)(Панель[^\n]{0,120}включает[^:]*:|Порты[^:]{0,40}:)\s*(.+)", _t or "")
    if m:
        tail = m.group(2)
        cut_m = _re_desc.search(r"(?:(?:\.|!|\?)\s+|\n\n|\Z)", tail)
        if cut_m:
            tail = tail[:cut_m.start()].strip()
        parts = _re_desc.split(r"[;•\n\t]|\s{2,}|\s,\s", tail)
        for p in parts:
            p = p.strip(" .;,-")
            if p:
                ports.append(p)
    return ports

def _split_sentences(_t: str) -> List[str]:
    """Simple sentence split used for the 'Описание' intro."""
    t = (_t or "").replace("..", ".")
    parts = _re_desc.split(r"(?<=[.!?])\s+(?=[А-ЯЁA-Z0-9])", t)
    return [p.strip() for p in parts if p.strip()]

def _build_html_from_plain(_t: str) -> str:
    """
    Build clean HTML:
      <h3>Описание</h3>
      [<h3>Особенности</h3>]
      [<h3>Порты и подключения</h3>]
      [<h3>Характеристики</h3>]
    """
    t = _normalize_ws(_t)
    # remove duplicated 'Характеристики' headings from supplier
    t = _re_desc.sub(r"(?mi)^Характеристики\s*:?", "", t).strip()

    ports = _extract_ports(t)
    specs = _extract_kv_specs(t)

    # strip found "k: v" occurrences from text so intro stays clean
    if specs:
        for k, v in specs[:50]:
            t = t.replace(f"{k}: {v}", "").replace(f"{k}:{v}", "").replace(f"{k} — {v}", "").replace(f"{k} - {v}", "")
        t = _re_desc.sub(r"(\n){2,}", "\n\n", t).strip()

    sents = _split_sentences(t)
    intro = " ".join(sents[:2]) if sents else t
    rest = " ".join(sents[2:]) if len(sents) > 2 else ""

    html_parts: List[str] = []
    if intro:
        html_parts.append("<h3>Описание</h3>")
        html_parts.append("<p>" + intro + "</p>")

    # heuristic features from rest
    features: List[str] = []
    for frag in _re_desc.split(r"[\n]+", rest):
        frag = frag.strip()
        if not frag:
            continue
        if "•" in frag or ";" in frag or " — " in frag:
            parts = _re_desc.split(r"[;•]|\s—\s", frag)
            cand = [p.strip(" .;,-") for p in parts if len(p.strip(" .;,-")) >= 3]
            for c in cand:
                if 3 <= len(c) <= 180:
                    features.append(c)
    if features:
        html_parts.append("<h3>Особенности</h3>")
        html_parts.append("<ul>")
        for f in features[:12]:
            html_parts.append("  <li>" + f + "</li>")
        html_parts.append("</ul>")

    if ports:
        html_parts.append("<h3>Порты и подключения</h3>")
        html_parts.append("<ul>")
        for p in ports[:15]:
            html_parts.append("  <li>" + p + "</li>")
        html_parts.append("</ul>")

    if specs:
        html_parts.append("<h3>Характеристики</h3>")
        html_parts.append("<ul>")
        for k, v in specs[:50]:
            html_parts.append(f"  <li>{k}: {v}</li>")
        html_parts.append("</ul>")

    if not html_parts:
        tmp_para = _re_desc.sub(r"\n{2,}", "</p><p>", t)
        return "<p>" + tmp_para + "</p>"
    return "\n".join(html_parts)

def _beautify_description_inner(inner: str) -> str:
    """If already HTML -> normalize; else build clean HTML from plain text."""
    if _has_html_tags(inner):
        t = _normalize_ws(inner)
        lines = [ln.strip() for ln in t.split("\n")]
        if any(ln.startswith("•") for ln in lines):
            items = [ln.lstrip("• ").strip() for ln in lines if ln.startswith("•")]
            others = [ln for ln in lines if not ln.startswith("•")]
            if items:
                t = "\n".join(others + ["<ul>"] + ["  <li>" + it + "</li>" for it in items] + ["</ul>"])
        return t
    return _build_html_from_plain(inner)

def _expand_description_selfclose_text(xml_text: str) -> str:
    """Ensure <description></description> form instead of self-closing."""
    return _re_desc.sub(r"<description\s*/\s*>", "<description></description>", xml_text or "")

def _wrap_and_beautify_description_text(xml_text: str) -> str:
    """Wrap each <description>...</description> content into CDATA and beautify."""
    def repl(m):
        inner = m.group(2) or ""
        pretty = _beautify_description_inner(inner)
        pretty = pretty.replace("]]>", "]]]]><![CDATA[>")  # protect CDATA
        return m.group(1) + "<![CDATA[" + pretty + "]]>" + m.group(3)
    return _re_desc.sub(r"(<description>)(.*?)(</description>)", repl, xml_text or "", flags=_re_desc.S)

def _postprocess_descriptions_beautify_cdata(xml_bytes: bytes, enc: str) -> bytes:
    """Post-serialization: expand, beautify, wrap into CDATA; return bytes."""
    try:
        enc_use = enc or "windows-1251"
        text = xml_bytes.decode(enc_use, errors="replace")
        text = _expand_description_selfclose_text(text)
        text = _wrap_and_beautify_description_text(text)
        return text.encode(enc_use, errors="replace")
    except Exception as e:
        warn(f"desc_beautify_post_warn: {e}")
        return xml_bytes

def _desc_fix_punct_spacing(s: str) -> str:
    """Remove spaces (incl. NBSP) before , . ; : ! ?"""
    if s is None: return s
    return re.sub(r'[\u00A0\u2009\u200A\u202F\s]+([,.;:!?])', r'\1', s)

def _desc_normalize_multi_punct(s: str) -> str:
    """Normalize punctuation runs: ellipsis and !!!??? sequences."""
    if s is None: return s
    s = re.sub(r'[!?:;]{3,}', lambda m: m.group(0)[-1], s)
    s = re.sub(r'…+', '...', s)
    s = re.sub(r'\.{3,}', '...', s)
    return s

def fix_all_descriptions_end(out_root: ET.Element) -> None:
    """Light cleanup in-tree before tostring()."""
    for offer in out_root.findall(".//offer"):
        d = offer.find("description")
        if d is not None and d.text:
            try:
                t = d.text
                t = _desc_fix_punct_spacing(t)
                t = _desc_normalize_multi_punct(t)
                d.text = t
            except Exception:
                pass

def flatten_all_descriptions(shop_el: ET.Element) -> int:
    """
    Convert supplier's description content to one clean text line (plain),
    then we will build readable HTML from it after serialization.
    """
    touched = 0
    for offer in shop_el.findall(".//offer"):
        d = offer.find("description")
        if d is None:
            d = ET.SubElement(offer, "description")
        if d is not None:
            raw = ""
            try:
                if d.text:
                    raw += d.text
                for child in list(d):
                    raw += ET.tostring(child, encoding="unicode")
            except Exception:
                raw = d.text or ""
            t = re.sub(r"<[^>]+>", " ", raw)
            t = html.unescape(t)
            t = re.sub(r"\s+", " ", t).strip()
            if t:
                d.text = t
                for child in list(d):
                    d.remove(child)
                touched += 1
    return touched

# =====================================================================
#                      OTHER LOGIC — UNCHANGED
# =====================================================================

try:
    from zoneinfo import ZoneInfo  # for FEED_META local time
except Exception:
    ZoneInfo = None

# ---------------------- ENV ----------------------
SUPPLIER_NAME = os.getenv("SUPPLIER_NAME", "AlStyle").strip()
SUPPLIER_URL  = os.getenv("SUPPLIER_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php").strip()
OUT_FILE_YML  = os.getenv("OUT_FILE", "docs/alstyle.yml").strip()
ENC           = os.getenv("OUTPUT_ENCODING", "windows-1251").strip()

TIMEOUT_S     = int(os.getenv("TIMEOUT_S", "30"))
RETRIES       = int(os.getenv("RETRIES", "4"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", "1.2"))
MIN_BYTES     = int(os.getenv("MIN_BYTES", "1500"))

ALSTYLE_CATEGORIES_MODE = os.getenv("ALSTYLE_CATEGORIES_MODE", "off").strip()  # off|include|exclude
ALSTYLE_CATEGORIES_PATH = os.getenv("ALSTYLE_CATEGORIES_PATH", "docs/alstyle_categories.txt").strip()

PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "3000000"))
PLACEHOLDER_DEFAULT_URL = os.getenv("PLACEHOLDER_DEFAULT_URL", "").strip()

PLACEHOLDER_URLS = {
    "cartridge": os.getenv("PLACEHOLDER_CARTRIDGE_URL", "").strip(),
    "ups":       os.getenv("PLACEHOLDER_UPS_URL", "").strip(),
    "mfp":       os.getenv("PLACEHOLDER_MFP_URL", "").strip(),
}

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS").strip()
DROP_STOCK_TAGS = os.getenv("DROP_STOCK_TAGS", "1").strip() in {"1","true","yes","y"}

# ---------------------- IO ----------------------
def load_source_bytes(src: str) -> bytes:
    """Load HTTP/local file with retries and size check."""
    if not src:
        err("SUPPLIER_URL not set")
    if not (src.startswith("http://") or src.startswith("https://")):
        if not os.path.exists(src):
            err(f"file not found: {src}")
        with open(src, "rb") as f:
            data = f.read()
        if len(data) < MIN_BYTES:
            raise RuntimeError(f"file too small: {len(data)}")
        return data
    import requests
    sess = requests.Session()
    headers = {"User-Agent": "supplier-feed-bot/1.0 (+github-actions)"}
    last_err: Optional[Exception] = None
    for i in range(1, RETRIES + 1):
        try:
            r = sess.get(src, headers=headers, timeout=TIMEOUT_S)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            data = r.content
            if len(data) < MIN_BYTES:
                raise RuntimeError(f"too small ({len(data)})")
            return data
        except Exception as e:
            last_err = e
            back = RETRY_BACKOFF * i * (1 + random.uniform(-0.2, 0.2))
            warn(f"fetch {i}/{RETRIES} failed: {e}; sleep {back:.2f}s")
            if i < RETRIES:
                time.sleep(back)
    raise RuntimeError(f"fetch failed: {last_err}")

# ---------------------- helpers ----------------------
def inner_html(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    parts: List[str] = []
    if el.text:
        parts.append(el.text)
    for child in list(el):
        parts.append(ET.tostring(child, encoding="unicode"))
    return "".join(parts)

def get_text(root: ET.Element, tag: str, default: str="") -> str:
    el = root.find(tag)
    return (el.text or default) if el is not None else default

def set_text(root: ET.Element, tag: str, value: str) -> ET.Element:
    el = root.find(tag)
    if el is None:
        el = ET.SubElement(root, tag)
    el.text = value
    return el

def remove_child(root: ET.Element, tag: str) -> None:
    el = root.find(tag)
    if el is not None:
        root.remove(el)

def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()

def _normalize_code(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", (s or "").strip())

def _extract_article_from_url(u: str) -> str:
    try:
        p = urllib.parse.urlparse(u or "")
        base = (p.path or "").split("/")[-1]
        return re.sub(r"[^A-Za-z0-9]+","", base)
    except Exception:
        return ""

def _extract_article_from_name(n: str) -> str:
    m = re.search(r"\b([A-Za-z]{1,6}[- ]?\d{2,})\b", n or "")
    return m.group(1) if m else ""

# ---------------------- categories ----------------------
class CatRule:
    def __init__(self, raw: str, kind: str, rx: Optional[re.Pattern]):
        self.raw = raw
        self.kind = kind
        self.rx = rx

def load_category_rules(path: str) -> Tuple[Set[str], List[CatRule]]:
    if not path or not os.path.exists(path):
        return set(), []
    rules: List[CatRule] = []
    ids: Set[str] = set()
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        data = f.read()
    for ln in data.splitlines():
        s = ln.strip()
        if not s or s.lstrip().startswith("#"):
            continue
        if re.fullmatch(r"\d{2,}", s):
            ids.add(s); continue
        if len(s) >= 2 and s[0] == "/" and s[-1] == "/":
            try:
                rules.append(CatRule(s, "regex", re.compile(s[1:-1], re.I)))
                continue
            except Exception:
                continue
        rules.append(CatRule(_norm_text(s), "substr", None))
    return ids, rules

def parse_categories_tree(shop_el: ET.Element) -> Tuple[Dict[str,str], Dict[str,str], Dict[str,Set[str]]]:
    id2name: Dict[str,str] = {}
    id2parent: Dict[str,str] = {}
    parent2children: Dict[str,Set[str]] = {}
    cats_root = shop_el.find("categories") or shop_el.find("Categories")
    if cats_root is None:
        return id2name, id2parent, parent2children
    for c in cats_root.findall("category"):
        cid = (c.attrib.get("id") or "").strip()
        if not cid:
            continue
        pid = (c.attrib.get("parentId") or "").strip()
        id2name[cid] = (c.text or "").strip()
        if pid:
            id2parent[cid] = pid
        parent2children.setdefault(pid, set()).add(cid)
    return id2name, id2parent, parent2children

def _cat_match(rule: CatRule, name: str) -> bool:
    if rule.kind == "regex":
        try:
            return bool(rule.rx.search(name))
        except Exception:
            return False
    return rule.raw in name

# ---------------------- pricing (unchanged) ----------------------
def pick_min_dealer_price(offer: ET.Element) -> Optional[float]:
    prices_el = offer.find("prices")
    best: Optional[float] = None
    if prices_el is not None:
        for p in prices_el.findall("price"):
            tp = (p.attrib.get("type") or "").strip().lower()
            if any(x in tp for x in ["dealer","опт","opt","b2b","wholesale"]):
                try:
                    v = float((p.text or "").strip().replace(",", "."))
                    best = v if best is None else min(best, v)
                except Exception:
                    pass
    for tag in ["purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price","price","oldprice"]:
        el = offer.find(tag)
        if el is not None and el.text:
            try:
                v = float((el.text or "").strip().replace(",", "."))
                best = v if best is None else min(best, v)
            except Exception:
                pass
    return best

def apply_pricing_rules(base: float) -> int:
    adders = [
        (1_000_000, 70_000), (1_500_000, 90_000), (2_000_000, 100_000),
        (750_000, 50_000), (500_000, 40_000), (400_000, 30_000),
        (300_000, 25_000), (200_000, 20_000), (150_000, 15_000),
        (100_000, 12_000), (75_000, 10_000), (50_000, 7_000),
        (25_000, 5_000), (10_000, 4_000), (100, 3_000), (0, 3_000),
    ]
    price = base * 1.04
    for thr, add in adders:
        if base >= thr:
            price += add
            break
    price = int(round(price))
    price = int(str(price)[:-3] + "900") if price >= 1000 else 900
    return price

def cleanup_price_tags(offer: ET.Element) -> None:
    for tag in ["prices","purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price","oldprice"]:
        el = offer.find(tag)
        if el is not None:
            offer.remove(el)

# ---------------------- pictures (unchanged) ----------------------
def ensure_pictures(offer: ET.Element) -> Tuple[int,int]:
    pics = offer.findall("picture")
    if pics:
        return (0, 0)
    picked = ""
    n = _norm_text(get_text(offer, "name"))
    kind = "cartridge" if "картридж" in n or "тонер" in n else "ups" if "ups" in n or "бесперебойник" in n else "mfp" if "мфу" in n or "принтер" in n else ""
    u = PLACEHOLDER_URLS.get(kind, "") if kind else ""
    if not u:
        u = PLACEHOLDER_DEFAULT_URL
    if u:
        ET.SubElement(offer, "picture").text = u
        return (1, 0)
    return (0, 1)

# ---------------------- availability/id/order/currency (unchanged) ----------------------
def _parse_int(s: str) -> Optional[int]:
    t = re.sub(r"[^\d\-]+","", s or "")
    if t in {"","-","+"}:
        return None
    try:
        return int(t)
    except Exception:
        return None

def derive_available(offer: ET.Element) -> Tuple[bool, str]:
    avail_el = offer.find("available")
    if avail_el is not None and avail_el.text:
        v = _norm_text(avail_el.text)
        if v in {"true","1","yes","y","да","есть","in stock","available"}:
            return True, "available-tag"
        if v in {"false","0","no","n","нет","unavailable","out of stock","отсутствует","под заказ","ожидается","на заказ"}:
            return False, "available-tag"
    for tag in ["stock","status","quantity","quantity_in_stock","stock_quantity"]:
        el = offer.find(tag)
        if el is not None and el.text:
            t = _norm_text(el.text)
            iv = _parse_int(t or "")
            if iv and iv > 0:
                return True, tag
            if t in {"true","1","yes","y","да","есть","in stock","available"}:
                return True, tag
            if t in {"false","0","no","n","нет","unavailable","out of stock"}:
                return False, tag
    return True, "fallback"

def ensure_available_attr(offer: ET.Element) -> None:
    b, src = derive_available(offer)
    offer.set("available", "true" if b else "false")
    if DROP_STOCK_TAGS:
        for tag in ["available","stock","status","quantity","quantity_in_stock","stock_quantity"]:
            el = offer.find(tag)
            if el is not None:
                offer.remove(el)

def insert_currency_kzt(offer: ET.Element) -> None:
    cur = offer.find("currencyId")
    if cur is None:
        cur = ET.SubElement(offer, "currencyId")
    cur.text = "KZT"

def reorder_children(offer: ET.Element) -> None:
    order = ["vendorCode","name","price","picture","vendor","currencyId","description"]
    tag2el = {child.tag: child for child in list(offer)}
    new_children = []
    for t in order:
        if t in tag2el:
            new_children.append(tag2el.pop(t))
    new_children.extend(tag2el.values())
    for child in list(offer):
        offer.remove(child)
    for child in new_children:
        offer.append(child)

def ensure_category_zero_first(offer: ET.Element) -> None:
    for c in offer.findall("categoryId"):
        offer.remove(c)
    new_c = ET.Element("categoryId"); new_c.text = "0"
    if len(list(offer)) > 0:
        offer.insert(0, new_c)
    else:
        offer.append(new_c)

# ---------------------- vendor/vendorCode (unchanged) ----------------------
def normalize_vendor(offer: ET.Element) -> None:
    v = get_text(offer, "vendor").strip()
    nv = _norm_text(v)
    if nv in {"alstyle","al-style","copyline","vtt","akcent","ak-cent","no brand","noname","неизвестный","unknown"}:
        v = ""
    if v:
        set_text(offer, "vendor", v)
    else:
        n = get_text(offer, "name")
        m = re.search(r"^\s*([A-Za-z][A-Za-z0-9\- ]{1,20})\b", n or "")
        if m:
            set_text(offer, "vendor", m.group(1))

def ensure_vendorcode_prefix(shop_el: ET.Element, prefix: str="AS") -> Tuple[int,int,int,int]:
    total_prefixed = 0
    created = 0
    filled_from_art = 0
    fixed_bare = 0
    for offer in shop_el.findall(".//offer"):
        vc = offer.find("vendorCode")
        if vc is None:
            vc = ET.SubElement(offer, "vendorCode")
            created += 1
        txt = (vc.text or "").strip()
        if txt and not txt.upper().startswith(prefix.upper()):
            vc.text = f"{prefix}{txt}"
            fixed_bare += 1
        if not vc.text or vc.text.strip().upper() == prefix.upper():
            art = _normalize_code(offer.attrib.get("article") or "") \
               or _normalize_code(_extract_article_from_name(get_text(offer,"name"))) \
               or _normalize_code(_extract_article_from_url(get_text(offer,"url"))) \
               or _normalize_code(offer.attrib.get("id") or "")
            if art:
                vc.text = art
                filled_from_art += 1
        vc.text = f"{prefix}{(vc.text or '')}"
        total_prefixed += 1
    return total_prefixed, created, filled_from_art, fixed_bare

def sync_offer_id_with_vendorcode(shop_el: ET.Element) -> int:
    fixed = 0
    for offer in shop_el.findall(".//offer"):
        vc = get_text(offer, "vendorCode")
        if vc and offer.attrib.get("id") != vc:
            offer.set("id", vc); fixed += 1
    return fixed

# ---------------------- keywords (unchanged) ----------------------
def generate_keywords(offer: ET.Element) -> str:
    toks: List[str] = []
    ven = get_text(offer, "vendor")
    if ven: toks.append(ven)
    name = get_text(offer, "name")
    if name:
        toks.extend([t for t in re.split(r"[ ,;:/|]+", name) if 3 <= len(t) <= 20])
    desc = inner_html(offer.find("description"))
    if desc:
        desc_text = re.sub(r"<[^>]+>", " ", desc)
        toks.extend([t for t in re.split(r"\s+", desc_text) if 3 <= len(t) <= 20])
    toks.extend(["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Петропавловск","Усть-Каменогорск","Атырау","Костанай"])
    seen: Set[str] = set()
    out: List[str] = []
    for t in toks:
        tt = t.lower()
        if tt not in seen:
            seen.add(tt); out.append(t)
        if len(out) >= 50:
            break
    return ", ".join(out)

# ---------------------- per-offer processing (unchanged) ----------------------
def process_offer_fields(offer: ET.Element) -> None:
    normalize_vendor(offer)
    base = pick_min_dealer_price(offer)
    if base is not None:
        price = apply_pricing_rules(base)
        set_text(offer, "price", str(price))
    cleanup_price_tags(offer)
    ensure_pictures(offer)
    ensure_available_attr(offer)
    insert_currency_kzt(offer)
    reorder_children(offer)
    ensure_category_zero_first(offer)
    kw = generate_keywords(offer)
    if kw:
        k_el = offer.find("keywords") or ET.SubElement(offer, "keywords")
        k_el.text = kw

# ---------------------- MAIN ----------------------
def main() -> None:
    data = load_source_bytes(SUPPLIER_URL)
    src_root = ET.fromstring(data)
    shop_in  = src_root.find("shop") if src_root.tag.lower() != "shop" else src_root
    if shop_in is None:
        err("XML: <shop> not found")
    offers_in_el = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None:
        err("XML: <offers> not found")
    src_offers = list(offers_in_el.findall("offer"))

    out_root  = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop  = ET.SubElement(out_root, "shop")
    out_offers= ET.SubElement(out_shop, "offers")

    for o in src_offers:
        out_offers.append(deepcopy(o))

    # category filter (unchanged minimal)
    removed_count = 0
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        id2name, id2parent, parent2children = parse_categories_tree(shop_in)
        rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH)
        def _name_of(cid: str) -> str:
            return _norm_text(id2name.get(cid, ""))
        for offer in list(out_offers.findall("offer")):
            cid_el = offer.find("categoryId")
            cid = (cid_el.text or "").strip() if cid_el is not None else ""
            cname = _name_of(cid) if cid else ""
            ok = True
            if ALSTYLE_CATEGORIES_MODE == "include":
                ok = (cid in rules_ids) or (cname and any(_cat_match(r, cname) for r in rules_names))
            else:
                ok = not ((cid in rules_ids) or (cname and any(_cat_match(r, cname) for r in rules_names)))
            if not ok:
                out_offers.remove(offer); removed_count += 1
        log(f"Category filter removed: {removed_count}")

    for offer in out_offers.findall("offer"):
        process_offer_fields(offer)

    # FEED_META
    try:
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(ZoneInfo("Asia/Almaty")) if ZoneInfo else (now_utc + timedelta(hours=5))
        total = len(list(out_offers.findall("offer")))
        meta = f"<!-- FEED_META: supplier={SUPPLIER_NAME}; fetched={SUPPLIER_URL}; build_local={now_local.strftime('%Y-%m-%d %H:%M')}; offers_total={len(src_offers)}; offers_after_filter={total} -->\n"
    except Exception:
        meta = f"<!-- FEED_META: supplier={SUPPLIER_NAME} -->\n"

    # 1) flatten descriptions to plain text
    desc_touched = flatten_all_descriptions(out_shop); log(f"Descriptions flattened: {desc_touched}")

    # 2) serialize
    try:
        xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True, short_empty_elements=False)
    except Exception:
        xml_bytes = ET.tostring(out_root, encoding="windows-1251", xml_declaration=True, short_empty_elements=False)

    # 3) light in-tree cleanup
    try:
        fix_all_descriptions_end(out_root)
    except Exception:
        pass

    # 4) beautify + wrap into CDATA (post-serialization)
    xml_bytes = _postprocess_descriptions_beautify_cdata(xml_bytes, ENC)

    # 5) insert FEED_META and expand self-closing descriptions
    try:
        txt = xml_bytes.decode(ENC, errors="replace")
        txt = _re_desc.sub(r'<\?xml[^>]*\?>', lambda m: m.group(0) + "\n" + meta, txt, count=1)
        txt = _expand_description_selfclose_text(txt)
        xml_bytes = txt.encode(ENC, errors="replace")
    except Exception as e:
        warn(f"post-serialization tweak warn: {e}")
        xml_bytes = (meta + xml_bytes.decode(ENC, errors="replace")).encode(ENC, errors="replace")

    # 6) write file
    try:
        os.makedirs(os.path.dirname(OUT_FILE_YML) or "docs", exist_ok=True)
        with open(OUT_FILE_YML, "wb") as f:
            f.write(xml_bytes)
        # .nojekyll for GH Pages
        open(os.path.join(os.path.dirname(OUT_FILE_YML) or "docs", ".nojekyll"), "wb").close()
    except Exception as e:
        err(f"write failed: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | descriptions=HTML+CDATA (with <h3>Характеристики</h3> if detected)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
