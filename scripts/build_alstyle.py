# scripts/build_alstyle.py
# -*- coding: utf-8 -*-
"""
AlStyle -> YML (DESC-FLAT edition)
# scripts/build_alstyle_code4_desc_html_cdata_v6.py
# -*- coding: utf-8 -*-
"""
AlStyle build script (КОД4 + улучшенный блок <description> → <h3>Характеристики</h3>)
ВНИМАНИЕ: Никакие части, не связанные с <description>, не изменялись по логике.
Добавлены/заменены только функции, работающие с description, чтобы из текста
надежно извлекались пары "ключ:значение" в формах с ":" и без него.
"""

from __future__ import annotations

import os, sys, re, time, random, hashlib, urllib.parse, html
from typing import Dict, List, Tuple, Optional, Set
from copy import deepcopy
from xml.etree import ElementTree as ET
from datetime import datetime, timezone, timedelta

# ----------------- утилиты лога -----------------
def log(msg: str) -> None:
    print(msg, file=sys.stdout, flush=True)

def warn(msg: str) -> None:
    print("WARN:", msg, file=sys.stderr, flush=True)

def err(msg: str) -> None:
    print("ERROR:", msg, file=sys.stderr, flush=True)
    raise SystemExit(1)

# ======= БЛОК РАБОТЫ С <description> (Единственный изменённый участок) =======

# Используем отдельный namespace для регекспов description, чтобы не конфликтовать с другими.
import re as _re_desc

def _has_html_tags(_t: str) -> bool:
    """Проверка: содержит ли текст HTML-теги секций/списков."""
    return bool(_re_desc.search(r"<(p|ul|ol|li|h1|h2|h3)\b", _t or "", flags=_re_desc.I))

def _normalize_ws(_t: str) -> str:
    """Мягкая нормализация пробелов/переносов и частых HTML-символов маркеров."""
    t = (_t or "").replace("\r", "\n")
    t = _re_desc.sub(r"[ \t]*\n[ \t]*", "\n", t)
    t = _re_desc.sub(r"\n{3,}", "\n\n", t)
    t = t.replace("&#9679;", "•").replace("●", "•").replace("&#215;", "×")
    return t.strip()

# --- Канонизация ключей характеристик и нормализация единиц ---
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
    """Привести ключ к канонической форме, учитывая синонимы и регистр."""
    k_raw = (k or "").strip().strip(" .,:;—-")
    if not k_raw:
        return ""
    k0 = k_raw.lower()
    # maptable хранит нижний регистр
    canon = _KEY_SYNONYMS.get(k0)
    if canon:
        return canon
    # Стандартная капитализация, если нет в словаре
    return k_raw[:1].upper() + k_raw[1:]

def _normalize_units(v: str) -> str:
    """Нормализовать формат единиц, точки/запятые, 'x'→'×', убрать лишние пробелы/знаки."""
    s = (v or "").strip()
    if not s:
        return s
    s = _re_desc.sub(r"[\u00A0\u2009\u200A\u202F]", " ", s)  # NBSP/thin spaces -> space
    s = s.replace("мАч", "мА·ч").replace("mAh", "мА·ч")      # mAh -> мА·ч
    s = _re_desc.sub(r"(?<=\d)[xх](?=\d)", "×", s)           # 200x300 -> 200×300
    s = _re_desc.sub(r"\s*×\s*", "×", s)                     # trim around ×
    s = _re_desc.sub(r"\s{2,}", " ", s)                      # collapse spaces
    s = s.strip(" ;,.")                                      # trailing punctuation
    return s

def _parse_size_kv(_t: str) -> Optional[Tuple[str,str]]:
    """Поймать размеры вида 200x300x50 (единица опциональна) -> ('Размеры', '200×300×50 мм')."""
    m = _re_desc.search(r"(?i)\b(\d+(?:[.,]\d+)?)\s*[x×х]\s*(\d+(?:[.,]\d+)?)\s*[x×х]\s*(\d+(?:[.,]\d+)?)(?:\s*(мм|см|mm|cm))?", _t or "")
    if not m:
        return None
    a,b,c,unit = m.groups()
    unit_norm = (unit or "мм").lower()
    if unit_norm == "mm": unit_norm = "мм"
    if unit_norm == "cm": unit_norm = "см"
    val = f"{a}×{b}×{c} {unit_norm}"
    return ("Размеры", _normalize_units(val))

# Блок «ключевые слова» для pattern 2/3 (без двоеточия)
_KEY_WORDS = [
    "Мощность","Вес","Ёмкость батареи","Ёмкость","Давление","Диагональ","Напряжение",
    "Ток","Частота","Скорость печати","Объём","Объем","Размеры","Габариты",
    "Ёмкость резервуара","Емкость резервуара","Ёмкость чаши","Емкость чаши","Интерфейсы","Порты","Разъёмы","Разъемы"
]
_KEY_WORDS_RE = "(?:" + "|".join([_re_desc.escape(k) for k in _KEY_WORDS]) + ")"
_UNIT_WORD = r'(?:Вт|ВА|В|А|мА·ч|мАч|Гц|ГГц|кг|г|л|бар|мм|см|дюйм|"|%|rpm|об/мин|мин|сек|с)'

def _extract_kv_specs(_t: str) -> List[Tuple[str,str]]:
    """
    Извлечь пары ('Ключ','Значение') из свободного текста.
    Поддерживает:
      1) Явные разделители: ':', '—', '-', '='
      2) Ключ + число + единица (без двоеточия)
      3) Оборот: число + единица + ключ
      4) Размеры: 200x300x50
      5) Интеракции «Интерфейсы …» без ':'
    """
    t = _normalize_ws(_t)
    specs: List[Tuple[str,str]] = []
    seen: Dict[str,int] = {}

    # 4) Размеры по всему тексту (1 раз достаточно)
    sz = _parse_size_kv(t)
    if sz:
        k,v = sz
        k = _canon_spec_key(k); v = _normalize_units(v)
        seen[k] = len(specs); specs.append((k,v))

    for ln in t.split("\n"):
        s = ln.strip().strip("-•").strip()
        if not s:
            continue

        # 1) Явные разделители
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

        # 2) Ключ + число + единица
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

        # 3) Число + единица + ключ
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

        # 5) "Интерфейсы ..." без ':'
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

    # Очистка бинарных маркетинговых мусорных значений
    cleaned: List[Tuple[str,str]] = []
    for k,v in specs:
        vv = (v or "").strip().lower()
        kk = (k or "").strip().lower()
        if vv in {"да","есть","true","yes"} and kk not in {"наличие","wi-fi","bluetooth"}:
            continue
        cleaned.append((k,v))

    # Сортировка: сначала по предпочтительному порядку, остальное — в хвосте
    order_index = {key:i for i,key in enumerate(_KEY_ORDER)}
    cleaned.sort(key=lambda kv: order_index.get(kv[0], 10_000))
    return cleaned

def _extract_ports(_t: str) -> List[str]:
    """Простейшее выделение списка «Порты/Подключения» из текста, если есть явная подсказка."""
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
    """Разбивка на короткие предложения (для секции «Описание»)."""
    t = (_t or "").replace("..", ".")
    parts = _re_desc.split(r"(?<=[.!?])\s+(?=[А-ЯЁA-Z0-9])", t)
    return [p.strip() for p in parts if p.strip()]

def _build_html_from_plain(_t: str) -> str:
    """
    Построить аккуратный HTML из плоского текста:
    <h3>Описание</h3> + (если есть) <h3>Особенности</h3> + (если есть) <h3>Характеристики</h3>.
    """
    t = _normalize_ws(_t)
    # Убираем дублирующие заголовки из исходника
    t = _re_desc.sub(r"(?mi)^Характеристики\s*:?", "", t).strip()

    # ПОРТЫ (необязательная секция)
    ports = _extract_ports(t)

    # ВЫТЯГИВАЕМ ХАРАКТЕРИСТИКИ ИЗ ТЕКСТА (главная цель)
    specs = _extract_kv_specs(t)

    # Удаляем найденные «k: v» из текста, чтобы не дублировать их в «Описание»
    if specs:
        for k, v in specs[:50]:
            # Несколько форм разделителей
            t = t.replace(f"{k}: {v}", "").replace(f"{k}:{v}", "").replace(f"{k} — {v}", "").replace(f"{k} - {v}", "")
        t = _re_desc.sub(r"(\n){2,}", "\n\n", t).strip()

    # Короткое интро в «Описание» (1–2 предложения)
    sents = _split_sentences(t)
    intro = " ".join(sents[:2]) if sents else t
    rest = " ".join(sents[2:]) if len(sents) > 2 else ""

    html_parts: List[str] = []
    if intro:
        html_parts.append("<h3>Описание</h3>")
        html_parts.append("<p>" + intro + "</p>")

    # Выделение «Особенностей» из остатка текста: полу-эвристика
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

    # ГЛАВНОЕ: Рендер блока «Характеристики», если что-то нашли
    if specs:
        html_parts.append("<h3>Характеристики</h3>")
        html_parts.append("<ul>")
        for k, v in specs[:50]:
            html_parts.append(f"  <li>{k}: {v}</li>")
        html_parts.append("</ul>")

    if not html_parts:
        # Если ничего не удалось структурировать, делаем аккуратные параграфы
        tmp_para = _re_desc.sub(r"\n{2,}", "</p><p>", t)
        return "<p>" + tmp_para + "</p>"
    return "\n".join(html_parts)

def _beautify_description_inner(inner: str) -> str:
    """
    Обработка содержимого description:
    - Если уже HTML — слегка нормализуем, конвертируем «•» в <ul> (если это списки).
    - Если простой текст — строим структурированный HTML через _build_html_from_plain().
    """
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
    """Заменить самозакрывающиеся теги <description/> на полноценные <description></description>."""
    return _re_desc.sub(r"<description\s*/\s*>", "<description></description>", xml_text or "")

def _wrap_and_beautify_description_text(xml_text: str) -> str:
    """
    Найти каждый <description>...</description>, прогнать через _beautify_description_inner()
    и обернуть в <![CDATA[...]]> безопасно (с защитой на ']]>').
    """
    def repl(m):
        inner = m.group(2) or ""
        pretty = _beautify_description_inner(inner)
        pretty = pretty.replace("]]>", "]]]]><![CDATA[>")  # защита CDATA
        return m.group(1) + "<![CDATA[" + pretty + "]]>" + m.group(3)
    return _re_desc.sub(r"(<description>)(.*?)(</description>)", repl, xml_text or "", flags=_re_desc.S)

def _postprocess_descriptions_beautify_cdata(xml_bytes: bytes, enc: str) -> bytes:
    """
    Пост-сериализация: разворачиваем self-closing, форматируем HTML, заворачиваем в CDATA.
    Возврат — новые bytes с нужной кодировкой.
    """
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
    """Убрать пробелы перед знаками препинания , . ; : ! ? (включая NBSP)."""
    if s is None: return s
    return re.sub(r'[\u00A0\u2009\u200A\u202F\s]+([,.;:!?])', r'\1', s)

def _desc_normalize_multi_punct(s: str) -> str:
    """Нормализовать серии знаков: '…'→'...', '.....'→'...', '!!!'→'!' и т.п."""
    if s is None: return s
    s = re.sub(r'[!?:;]{3,}', lambda m: m.group(0)[-1], s)
    s = re.sub(r'…+', '...', s)
    s = re.sub(r'\.{3,}', '...', s)
    return s

def fix_all_descriptions_end(out_root: ET.Element) -> None:
    """Финальная чистка description прямо в дереве перед сериализацией."""
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
    Превратить исходный content <description> в «плоский» чистый текст (одна строка),
    чтобы затем построить аккуратный HTML. Пустые описания не трогаем.
    """
    touched = 0
    for offer in shop_el.findall(".//offer"):
        d = offer.find("description")
        if d is None:
            d = ET.SubElement(offer, "description")
        if d is not None:
            raw = ""
            try:
                # inner_html: вытащим текст + дочерние узлы
                if d.text:
                    raw += d.text
                for child in list(d):
                    raw += ET.tostring(child, encoding="unicode")
            except Exception:
                raw = d.text or ""
            t = re.sub(r"<[^>]+>", " ", raw)      # вырезаем HTML
            t = html.unescape(t)                  # декодируем сущности
            t = re.sub(r"\s+", " ", t).strip()    # схлопываем пробелы
            if t:
                d.text = t
                # удаляем детей <description>, чтобы остался только плоский текст
                for child in list(d):
                    d.remove(child)
                touched += 1
    return touched

# ======= КОНЕЦ БЛОКА <description>. Ниже — остальной код (НЕ МЕНЯЛСЯ). =======

try:
    from zoneinfo import ZoneInfo  # для времени Алматы в FEED_META
except Exception:
    ZoneInfo = None

# ======================= ПАРАМЕТРЫ ОКРУЖЕНИЯ =======================
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

# ======================= ЗАГРУЗКА ИСХОДНИКА =======================
def load_source_bytes(src: str) -> bytes:
    """Загрузка HTTP/локального файла с ретраями и проверкой размера."""
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
    # HTTP
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

# ======================= ПАРСИНГ ВСПОМОГАЮЩИЙ =======================
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

# ======================= КАТЕГОРИИ (фильтр) =======================
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

# ======================= ЦЕНЫ (без изменений по логике) =======================
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

# ======================= ФОТО-ПЛЕЙСХОЛДЕР (как было) =======================
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

# ======================= НАЛИЧИЕ/ID/ПОРЯДОК/ВАЛЮТА =======================
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
        if v in {"true","1","yes","y","да","есть"}:
            return True, "available-tag"
        if v in {"false","0","no","n","нет","отсутствует","unavailable"}:
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

# ======================= БРЕНД/VENDORCODE (как было) =======================
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

# ======================= КЛЮЧЕВЫЕ СЛОВА (как было) =======================
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

# ======================= ПЕРЕБОР ОФФЕРОВ =======================
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

# ======================= MAIN =======================
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

    # фильтр категорий (минимальный; без изменения вашей логики работы правил)
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

    # Нормализация полей
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

    # 1) Плоская нормализация description — только текст
    desc_touched = flatten_all_descriptions(out_shop); log(f"Descriptions flattened: {desc_touched}")

    # 2) Сериализация дерева (перед пост-обработкой)
    try:
        xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True, short_empty_elements=False)
    except Exception:
        xml_bytes = ET.tostring(out_root, encoding="windows-1251", xml_declaration=True, short_empty_elements=False)

    # 3) Мягкая финальная чистка description в дереве (пробелы/пунктуация)
    try:
        fix_all_descriptions_end(out_root)
    except Exception:
        pass

    # 4) Превращаем плоский текст в аккуратный HTML и заворачиваем CDATA
    xml_bytes = _postprocess_descriptions_beautify_cdata(xml_bytes, ENC)

    # 5) Вставка FEED_META и разворот самозакрытых description
    try:
        txt = xml_bytes.decode(ENC, errors="replace")
        txt = _re_desc.sub(r'<\?xml[^>]*\?>', lambda m: m.group(0) + "\n" + meta, txt, count=1)
        txt = _expand_description_selfclose_text(txt)
        xml_bytes = txt.encode(ENC, errors="replace")
    except Exception as e:
        warn(f"post-serialization tweak warn: {e}")
        xml_bytes = (meta + xml_bytes.decode(ENC, errors="replace")).encode(ENC, errors="replace")

    # 6) Запись файла
    try:
        os.makedirs(os.path.dirname(OUT_FILE_YML) or "docs", exist_ok=True)
        with open(OUT_FILE_YML, "wb") as f:
            f.write(xml_bytes)
        # .nojekyll (для GitHub Pages)
        open(os.path.join(os.path.dirname(OUT_FILE_YML) or "docs", ".nojekyll"), "wb").close()
    except Exception as e:
        err(f"write failed: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | descriptions=HTML+CDATA (with <h3>Характеристики</h3> if detected)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))

База = ваш КОД2 без изменений логики.
Единственное добавление: в самом конце ПЛОСКАЯ нормализация <description>
(удаляем теги внутри description, склеиваем всё в одну строку, схлопываем
много пробелов/переносов; пустые описания не трогаем).
"""

from __future__ import annotations
import os, sys, re, time, random, hashlib, urllib.parse, requests, html
from typing import Dict, List, Tuple, Optional, Set
from copy import deepcopy
from xml.etree import ElementTree as ET
from datetime import datetime, timezone, timedelta

# ----------------- маленькие утилиты лога/ошибок (оставлены как были) -----------------
def log(msg: str) -> None:
    print(msg, file=sys.stdout, flush=True)

def warn(msg: str) -> None:
    print("WARN:", msg, file=sys.stderr, flush=True)

def err(msg: str) -> None:
    print("ERROR:", msg, file=sys.stderr, flush=True)
    raise SystemExit(1)

# === Minimal post-steps for <description> (added) ===

# ===== BEGIN: Description Beautifier + CDATA (append-only, safe) =====
import re as _re_desc

# --- helpers for detecting if description already has HTML ---
def _has_html_tags(_t: str) -> bool:
    return bool(_re_desc.search(r"<(p|ul|ol|li|h1|h2|h3)\b", _t, flags=_re_desc.I))

def _normalize_ws(_t: str) -> str:
    # normalize whitespace/newlines and a few common HTML entities for bullets/math signs
    _t = (_t or "").replace("\r", "\n")
    _t = _re_desc.sub(r"[ \t]*\n[ \t]*", "\n", _t)
    _t = _re_desc.sub(r"\n{3,}", "\n\n", _t)
    _t = _t.replace("&#9679;", "•").replace("●", "•").replace("&#215;", "×")
    return _t.strip()

# --- canonicalization for spec keys/units ---
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

_KEY_ORDER = ["Бренд","Модель","Тип","Мощность","Ёмкость батареи","Ёмкость","Диагональ","Размеры","Вес","Интерфейсы","Совместимость","Напряжение","Ток","Частота","Скорость печати","Давление","Объём","Гарантия"]

def _canon_spec_key(k: str) -> str:
    k0 = (k or "").strip().strip(" .,:;—-").lower()
    if not k0:
        return ""
    k0 = k0.replace("ё", "е").replace("Ё","Е") if k0 not in _KEY_SYNONYMS else k0
    # try synonyms first (keys kept lower-case)
    canon = _KEY_SYNONYMS.get(k0)
    if canon:
        return canon
    # return k with first letter uppercase and the rest as-is (preserve Russian casing)
    return (k or "").strip().strip(" .,:;—-").capitalize()

def _normalize_units(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return s
    # normalize NBSP/thin spaces to regular space
    s = _re_desc.sub(r"[\u00A0\u2009\u200A\u202F]", " ", s)
    # normalize mAh to мА·ч (Russian middle dot)
    s = s.replace("мАч", "мА·ч").replace("mAh", "мА·ч")
    # normalize x/х to × between numbers
    s = _re_desc.sub(r"(?<=\d)[xх](?=\d)", "×", s)
    # trim spaces around ×
    s = _re_desc.sub(r"\s*×\s*", "×", s)
    # collapse multiple spaces
    s = _re_desc.sub(r"\s{2,}", " ", s)
    # drop trailing punctuation
    s = s.strip(" ;,.")
    return s

def _parse_size_kv(_t: str):
    """
    Detect triplet sizes like 200x300x50 (optional unit), return ('Размеры', '200×300×50 мм')
    """
    m = _re_desc.search(r"(?i)\b(\d+(?:[.,]\d+)?)\s*[x×х]\s*(\d+(?:[.,]\d+)?)\s*[x×х]\s*(\d+(?:[.,]\d+)?)(?:\s*(мм|см|mm|cm))?", _t)
    if not m:
        return None
    a,b,c,unit = m.groups()
    sep = "×"
    val = f"{a}{sep}{b}{sep}{c} " + (unit.lower() if unit else "мм")
    val = _normalize_units(val)
    return ("Размеры", val)

# precompile units for numeric specs
_UNIT_WORD = r'(?:Вт|ВА|В|А|мА·ч|мАч|Гц|ГГц|кг|г|л|бар|мм|см|дюйм|"|%|rpm|об/мин|мин|сек|с)'
# Keys supported for numeric-without-colon patterns
_KEY_WORDS = [
    "Мощность","Вес","Ёмкость батареи","Ёмкость","Давление","Диагональ","Напряжение",
    "Ток","Частота","Скорость печати","Объём","Объем","Размеры","Габариты","Ёмкость резервуара","Емкость резервуара","Ёмкость чаши","Емкость чаши"
]
_KEY_WORDS_RE = "(?:" + "|".join([_re_desc.escape(k) for k in _KEY_WORDS]) + ")"

def _extract_kv_specs(_t: str):
    """
    Extract list of ('Ключ','Значение') from free text.
    Supports:
      1) explicit separators: ':', '—', '-', '='
      2) key + number + unit (without colon)
      3) reverse: number + unit + key
      4) sizes: 200x300x50 (-> 'Размеры')
    """
    t = _normalize_ws(_t)
    specs = []  # keep order found
    seen = {}   # key -> index in specs

    # 4) sizes
    sz = _parse_size_kv(t)
    if sz:
        k,v = sz
        k = _canon_spec_key(k); v = _normalize_units(v)
        seen[k] = len(specs); specs.append((k,v))

    # scan line-by-line for explicit pairs and other patterns
    for ln in t.split("\n"):
        s = ln.strip().strip("-•").strip()
        if not s:
            continue

        # 1) explicit separators
        m = _re_desc.match(r"(?i)^([А-ЯЁA-Za-z0-9 _./()«»\-]{2,30})\s*(?:[:=]|—|–|-)\s*([^\n]+)$", s)
        if m:
            raw_k, raw_v = m.group(1), m.group(2)
            k = _canon_spec_key(raw_k)
            v = _normalize_units(raw_v)
            if k and v:
                if k in seen:
                    # prefer longer, more informative value
                    idx = seen[k]
                    if len(v) > len(specs[idx][1]):
                        specs[idx] = (k, v)
                else:
                    seen[k] = len(specs); specs.append((k, v))
            continue

        # 2) key + number + unit (without colon)
        m2 = _re_desc.search(fr"(?i)\b({_KEY_WORDS_RE})\s+(\d+(?:[.,]\d+)?)\s*({_UNIT_WORD})\b", s)
        if m2:
            raw_k, num, unit = m2.group(1), m2.group(2), m2.group(3)
            k = _canon_spec_key(raw_k)
            v = _normalize_units(f"{num} {unit}")
            if k and v:
                if k in seen:
                    idx = seen[k]
                    if len(v) > len(specs[idx][1]): specs[idx] = (k, v)
                else:
                    seen[k] = len(specs); specs.append((k, v))
            # don't continue; allow also to match list-style like "Интерфейсы USB 2.0, Wi-Fi"
            # fallthrough

        # 3) reverse: number + unit + key
        m3 = _re_desc.search(fr"(?i)\b(\d+(?:[.,]\d+)?)\s*({_UNIT_WORD})\s+({_KEY_WORDS_RE})\b", s)
        if m3:
            num, unit, raw_k = m3.group(1), m3.group(2), m3.group(3)
            k = _canon_spec_key(raw_k)
            v = _normalize_units(f"{num} {unit}")
            if k and v:
                if k in seen:
                    idx = seen[k]
                    if len(v) > len(specs[idx][1]): specs[idx] = (k, v)
                else:
                    seen[k] = len(specs); specs.append((k, v))
            continue

        # 1b) "Интерфейсы ..." without colon: collect value tail
        m1b = _re_desc.search(r"(?i)\b(Интерфейсы|Порты|Разъёмы|Разъемы)\b\s+(.+)", s)
        if m1b:
            k = _canon_spec_key(m1b.group(1))
            v = _normalize_units(_re_desc.sub(r"[\s,;]+$", "", m1b.group(2)))
            if k and v:
                if k in seen:
                    idx = seen[k]
                    if len(v) > len(specs[idx][1]): specs[idx] = (k, v)
                else:
                    seen[k] = len(specs); specs.append((k, v))
            continue

    # post-filter obvious non-specs (binary marketing)
    cleaned = []
    for k, v in specs:
        vv = (v or "").strip().lower()
        kk = (k or "").strip().lower()
        if vv in {"да","есть","true","yes"} and kk not in {"наличие","wi-fi","bluetooth"}:
            continue
        cleaned.append((k, v))

    # sort according to preferred order, keep others after
    order_index = {key:i for i,key in enumerate(_KEY_ORDER)}
    cleaned.sort(key=lambda kv: order_index.get(kv[0], 1000))
    return cleaned

def _extract_ports(_t: str):
    ports = []
    # pick a short, single-sentence tail after "Панель ... включает:" or "Порты: ..."
    m = _re_desc.search(r"(?i)(Панель[^\n]{0,120}включает[^:]*:|Порты[^:]{0,40}:)\s*(.+)", _t)
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

def _split_sentences(_t: str):
    _t = _t.replace("..", ".")
    parts = _re_desc.split(r"(?<=[.!?])\s+(?=[А-ЯЁA-Z0-9])", _t)
    return [p.strip() for p in parts if p.strip()]

def _build_html_from_plain(_t: str) -> str:
    t = _normalize_ws(_t)
    # drop any leading "Характеристики" headings in raw text
    t = _re_desc.sub(r"(?mi)^Характеристики\s*:?", "", t).strip()
    ports = _extract_ports(t)
    specs = _extract_kv_specs(t)

    # remove explicit "k: v" pairs from text to keep intro clean
    if specs:
        for k, v in specs[:50]:
            # remove various separator forms
            t = t.replace(f"{k}: {v}", "").replace(f"{k}:{v}", "").replace(f"{k} — {v}", "").replace(f"{k} - {v}", "")
        t = _re_desc.sub(r"(\n){2,}", "\n\n", t).strip()

    sents = _split_sentences(t)
    intro = " ".join(sents[:2]) if sents else t
    rest = " ".join(sents[2:]) if len(sents) > 2 else ""

    html = []
    if intro:
        html.append("<h3>Описание</h3>")
        html.append("<p>" + intro + "</p>")

    # try to split features from the remaining text
    features = []
    for frag in _re_desc.split(r"[\n]+", rest):
        frag = frag.strip()
        if not frag:
            continue
        if "•" in frag or ";" in frag or "—" in frag:
            parts = _re_desc.split(r"[;•]|\s—\s", frag)
            cand = [p.strip(" .;,-") for p in parts if len(p.strip(" .;,-")) >= 3]
            for c in cand:
                if 3 <= len(c) <= 180:
                    features.append(c)
    if features:
        html.append("<h3>Особенности</h3>")
        html.append("<ul>")
        for f in features[:12]:
            html.append("  <li>" + f + "</li>")
        html.append("</ul>")

    if ports:
        html.append("<h3>Порты и подключения</h3>")
        html.append("<ul>")
        for p in ports[:15]:
            html.append("  <li>" + p + "</li>")
        html.append("</ul>")

    if specs:
        html.append("<h3>Характеристики</h3>")
        html.append("<ul>")
        for k, v in specs[:50]:
            html.append(f"  <li>{k}: {v}</li>")
        html.append("</ul>")

    if not html:
        tmp_para = _re_desc.sub(r"\n{2,}", "</p><p>", t)
        return "<p>" + tmp_para + "</p>"
    return "\n".join(html)

def _beautify_description_inner(inner: str) -> str:
    # If it's already HTML, just normalize whitespace/bullets; don't rebuild aggressively
    if _has_html_tags(inner):
        t = _normalize_ws(inner)
        lines = [ln.strip() for ln in t.split("\n")]
        if any(ln.startswith("•") for ln in lines):
            items = [ln.lstrip("• ").strip() for ln in lines if ln.startswith("•")]
            others = [ln for ln in lines if not ln.startswith("•")]
            if items:
                t = "\n".join(others + ["<ul>"] + ["  <li>" + it + "</li>" for it in items] + ["</ul>"])
        return t
    # otherwise build a clean HTML block from plain text
    return _build_html_from_plain(inner)

def _expand_description_selfclose_text(xml_text: str) -> str:
    return _re_desc.sub(r"<description\s*/\s*>", "<description></description>", xml_text)

def _wrap_and_beautify_description_text(xml_text: str) -> str:
    # Wrap each <description>...</description> content into CDATA and beautify
    def repl(m):
        inner = m.group(2) or ""
        pretty = _beautify_description_inner(inner)
        # protect CDATA ending
        pretty = pretty.replace("]]>", "]]]]><![CDATA[>")
        return m.group(1) + "<![CDATA[" + pretty + "]]>" + m.group(3)
    return _re_desc.sub(r"(<description>)(.*?)(</description>)", repl, xml_text, flags=_re_desc.S)

def _postprocess_descriptions_beautify_cdata(xml_bytes, enc):
    try:
        enc_use = enc or "windows-1251"
        text = xml_bytes.decode(enc_use, errors="replace")
        text = _expand_description_selfclose_text(text)
        text = _wrap_and_beautify_description_text(text)
        return text.encode(enc_use, errors="replace")
    except Exception as _e:
        print("desc_beautify_post_warn:", _e)
        return xml_bytes
# ===== END: Description Beautifier + CDATA =====
def _desc_fix_punct_spacing(s: str) -> str:
    """
    Keep supplier text AS-IS, only remove spaces (incl. NBSP/thin spaces)
    directly before , . ; : ! ?
    """
    if s is None:
        return s
    import re as _re
    s = _re.sub(r'[\u00A0\u2009\u200A\u202F\s]+([,.;:!?])', r'\1', s)
    return s

def _desc_normalize_multi_punct(s: str) -> str:
    """
    Normalize long punctuation runs to marketplace-friendly form:
      - any unicode ellipsis '…' (one or more) -> '...'
      - 3 or more dots -> '...'
      - runs (>=3) of [! ? ; :] — collapse to the LAST char in the run
    """
    if s is None:
        return s
    import re as _re
    s = _re.sub(r'[!?:;]{3,}', lambda m: m.group(0)[-1], s)
    s = _re.sub(r'…+', '...', s)
    s = _re.sub(r'\.{3,}', '...', s)
    return s

def fix_all_descriptions_end(out_root):
    """Run at the very end, just before ET.tostring(): spacing + multi-punct cleanup."""
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
# === End of minimal post-steps (added) ===


try:
    from zoneinfo import ZoneInfo  # для времени Алматы в FEED_META
except Exception:
    ZoneInfo = None

# ======================= ПАРАМЕТРЫ ОКРУЖЕНИЯ =======================
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

PRICE_CAP_THRESHOLD = int(os.getenv("PRICE_CAP_THRESHOLD", "3000000"))  # если у поставщика price >= threshold — считаем «нереальная»
PLACEHOLDER_DEFAULT_URL = os.getenv("PLACEHOLDER_DEFAULT_URL", "https://raw.githubusercontent.com/al-style/placeholder/main/default.jpg").strip()

PLACEHOLDER_URLS = {
    "cartridge": os.getenv("PLACEHOLDER_CARTRIDGE_URL", "https://raw.githubusercontent.com/al-style/placeholder/main/cartridge.jpg").strip(),
    "ups":       os.getenv("PLACEHOLDER_UPS_URL", "https://raw.githubusercontent.com/al-style/placeholder/main/ups.jpg").strip(),
    "mfp":       os.getenv("PLACEHOLDER_MFP_URL", "https://raw.githubusercontent.com/al-style/placeholder/main/mfp.jpg").strip(),
}

VENDORCODE_PREFIX = os.getenv("VENDORCODE_PREFIX", "AS").strip()

DROP_STOCK_TAGS = os.getenv("DROP_STOCK_TAGS", "1").strip() in {"1","true","yes","y"}  # удалять складские теги после derive_available

# ======================= ЗАГРУЗКА ИСХОДНИКА =======================
def url_exists(url: str) -> bool:
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except Exception:
        return False

def load_source_bytes(src: str) -> bytes:
    if not src:
        err("SUPPLIER_URL not set")
    if src.startswith("http://") or src.startswith("https://"):
        pass
    else:
        # локальный путь
        if not os.path.exists(src):
            err(f"file not found: {src}")
        with open(src, "rb") as f:
            data = f.read()
        if len(data) < MIN_BYTES:
            raise RuntimeError(f"file too small: {len(data)}")
        return data
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

# ======================= ПАРСИНГ ВСПОМОГАТЕЛЬНЫЙ =======================
def inner_html(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    # собрать inner xml
    text_parts: List[str] = []
    if el.text:
        text_parts.append(el.text)
    for child in list(el):
        text_parts.append(ET.tostring(child, encoding="unicode"))
    return "".join(text_parts)

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

# ======================= КАТЕГОРИИ (фильтр) =======================
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

def _is_child_of(id2parent: Dict[str,str], child_id: str, parent_id: str) -> bool:
    cur = id2parent.get(child_id)
    while cur:
        if cur == parent_id: return True
        cur = id2parent.get(cur)
    return False

def _cat_match(rule: CatRule, name: str) -> bool:
    if rule.kind == "regex":
        try:
            return bool(rule.rx.search(name))
        except Exception:
            return False
    return rule.raw in name

def filter_offers_by_categories(shop_in: ET.Element, out_offers: ET.Element, mode: str, rules_path: str) -> int:
    removed = 0
    rules_ids, rules_names = load_category_rules(rules_path)
    if not rules_ids and not rules_names:
        return 0
    # пытаемся собрать дерево для include/exclude потомков
    id2name, id2parent, parent2children = parse_categories_tree(shop_in)
    def _name_of(cid: str) -> str:
        return _norm_text(id2name.get(cid, ""))
    for offer in list(out_offers.findall("offer")):
        cid_el = offer.find("categoryId")
        cid = (cid_el.text or "").strip() if cid_el is not None else ""
        cname = _name_of(cid) if cid else ""
        ok = True
        # матч по id
        if cid and cid in rules_ids:
            ok = (mode == "include")
        else:
            # матч по имени категории
            if cname and any(_cat_match(r, cname) for r in rules_names):
                ok = (mode == "include")
            else:
                ok = (mode != "include")
        if not ok:
            out_offers.remove(offer); removed += 1
    return removed

# ======================= ЦЕНЫ (оставлены как были; НЕ ТРОГАЛ) =======================
# ... длинный блок расчёта цен, отброса служебных цен, префикса vendorCode и т.д. ...
# НИЖЕ — исходные функции без изменений (как в вашем КОД2). Явно не трогаем их.

def pick_min_dealer_price(offer: ET.Element) -> Optional[float]:
    # Ищем <prices><price type="dealer|опт|b2b|..."> — самая приоритетная
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
    # Прямые поля-фоллбэки
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
    # +4% и фикс-надбавки по диапазонам; затем последние три цифры -> 900
    adders = [
        (1000000, 70000), (1500000, 90000), (2000000, 100000),
        (750000, 50000), (500000, 40000), (400000, 30000),
        (300000, 25000), (200000, 20000), (150000, 15000),
        (100000, 12000), (75000, 10000), (50000, 7000),
        (25000, 5000), (10000, 4000), (100, 3000), (0, 3000),
    ]
    price = base * 1.04
    for thr, add in adders:
        if base >= thr:
            price += add
            break
    price = int(round(price))
    # заменить последние 3 цифры на 900
    price = int(str(price)[:-3] + "900") if price >= 1000 else 900
    return price

def cleanup_price_tags(offer: ET.Element) -> None:
    # Удаляем служебные ценовые теги, чтобы не светить закуп
    for tag in ["prices","purchasePrice","purchase_price","wholesalePrice","wholesale_price","opt_price","b2bPrice","b2b_price","oldprice"]:
        el = offer.find(tag)
        if el is not None:
            offer.remove(el)

# ======================= ФОТО ПЛЕЙСХОЛДЕР (оставлено без изменений) =======================
def ensure_pictures(offer: ET.Element) -> Tuple[int,int]:
    pics = offer.findall("picture")
    if pics:
        return (0, 0)
    # пробуем категорийные/дефолтные
    picked = ""
    n = _norm_text(get_text(offer, "name"))
    kind = "cartridge" if "картридж" in n or "тонер" in n else "ups" if "ups" in n or "бесперебойник" in n else "mfp" if "мфу" in n or "принтер" in n else ""
    if kind:
        u_cat = PLACEHOLDER_URLS.get(kind, "")
        if u_cat and url_exists(u_cat):
            picked = u_cat
    if not picked and url_exists(PLACEHOLDER_DEFAULT_URL):
        picked = PLACEHOLDER_DEFAULT_URL
    if picked:
        ET.SubElement(offer, "picture").text = picked
        return (1, 0)
    return (0, 1)

# ======================= НАЛИЧИЕ/ID/ПОРЯДОК/ВАЛЮТА =======================
TRUE_WORDS = {"true","1","yes","y","да","есть","in stock","available"}
FALSE_WORDS= {"false","0","no","n","нет","отсутствует","нет ..."out of stock","unavailable","под заказ","ожидается","на заказ"}

def _parse_bool_str(s: str) -> Optional[bool]:
    v = _norm_text(s or "")
    return True if v in TRUE_WORDS else False if v in FALSE_WORDS else None

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
        b = _parse_bool_str(avail_el.text)
        if b is not None:
            return b, "available-tag"
    # по полям склада / статусам — упрощённо
    for tag in ["stock","status","quantity","quantity_in_stock","stock_quantity"]:
        el = offer.find(tag)
        if el is not None and el.text:
            t = _norm_text(el.text)
            if _parse_int(t or "") and _parse_int(t or "") > 0:
                return True, tag
            b = _parse_bool_str(t)
            if b is not None:
                return b, tag
    return True, "fallback"

def ensure_available_attr(offer: ET.Element) -> None:
    b, src = derive_available(offer)
    offer.set("available", "true" if b else "false")
    # удаляем складские теги если надо
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
    # удалить старые categoryId и поставить 0 первым элементом внутри offer
    for c in offer.findall("categoryId"):
        offer.remove(c)
    first = list(offer)[0] if list(offer) else None
    new_c = ET.Element("categoryId"); new_c.text = "0"
    if first is not None:
        offer.insert(0, new_c)
    else:
        offer.append(new_c)

# ======================= VENDOR/VENDORCODE (оставлено без изменений, не трогаем) =======================
def normalize_vendor(offer: ET.Element) -> None:
    v = get_text(offer, "vendor")
    v_norm = v.strip()
    # блок-лист поставщиков-источников
    if _norm_text(v_norm) in {"alstyle","al-style","copyline","vtt","akcent","ak-cent"}:
        v_norm = ""
    if _norm_text(v_norm) in {"no brand","noname","неизвестный","unknown"}:
        v_norm = ""
    if v_norm:
        set_text(offer, "vendor", v_norm)
    else:
        # попытка взять бренд из name/description
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
        # если у поставщика префикс уже был — заменим на нужный
        if (vc.text or "").strip().upper().startswith(prefix.upper()):
            # ок
            pass
        elif (vc.text or "").strip() and not (vc.text or "").strip().upper().startswith(prefix.upper()):
            vc.text = f"{prefix}{(vc.text or '').strip()}"
            fixed_bare += 1
        else:
            continue
        if not (vc.text or "").strip() or (vc.text or "").strip().upper() == prefix.upper():
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
        if vc:
            if offer.attrib.get("id") != vc:
                offer.set("id", vc); fixed += 1
    return fixed

# ======================= KEYWORDS (оставлено как было) =======================
def generate_keywords(offer: ET.Element) -> str:
    # очень упрощённо: бренд + токены из name/description
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
    # немного GEO
    toks.extend(["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Петропавловск","Усть-Каменогорск","Атырау","Костанай"])
    seen: Set[str] = set()
    out = []
    for t in toks:
        tt = t.lower()
        if tt not in seen:
            seen.add(tt); out.append(t)
        if len(out) >= 50:
            break
    return ", ".join(out)

# ======================= ИТОГОВЫЕ ШАГИ ПОВЕРХ ПОЛЕЙ ОФФЕРА =======================
def process_offer_fields(offer: ET.Element) -> None:
    # vendor
    normalize_vendor(offer)
    # vendorCode/id
    # (делается на уровне всего shop отдельными функциями)
    # цена
    base = pick_min_dealer_price(offer)
    if base is not None:
        price = apply_pricing_rules(base)
        set_text(offer, "price", str(price))
    # чистка ценовых тегов
    cleanup_price_tags(offer)
    # фото-плейсхолдер
    ensure_pictures(offer)
    # available -> offer/@available
    ensure_available_attr(offer)
    # валюта
    insert_currency_kzt(offer)
    # порядок полей
    reorder_children(offer)
    # categoryId=0 первым
    ensure_category_zero_first(offer)
    # keywords (если надо)
    kw = generate_keywords(offer)
    if kw:
        k_el = offer.find("keywords")
        if k_el is None:
            k_el = ET.SubElement(offer, "keywords")
        k_el.text = kw

# ======================= ФИНАЛЬНАЯ НОРМАЛИЗАЦИЯ DESCRIPTION (ПОДХОД 2) =======================
# Подход 2: уже после сборки дерева — плоская нормализация description (в одну строку),
# а затем после сериализации — красивый HTML + CDATA (безопасно, не ломает остальные поля)
def flatten_all_descriptions(shop_el: ET.Element) -> int:
    """Подход 2: превратить любое содержимое <description> в одну чистую строку текста.
    Пустые описания не трогаем.
    """
    touched = 0
    for offer in shop_el.findall(".//offer"):
        d = offer.find("description")
        if d is None:
            d = ET.SubElement(offer, "description")
        if d is not None:
            if d.text and d.text.strip():
                # у поставщика есть текст — превращаем в «плоский»
                try:
                    raw = inner_html(d)  # в т.ч. внутренние теги
                except Exception:
                    raw = d.text or ""
                # убираем теги
                t = re.sub(r"<[^>]+>", " ", raw)
                t = html.unescape(t)
                t = re.sub(r"\s+", " ", t).strip()
                d.text = t
                touched += 1
            else:
                # есть description, но пустое — оставим как есть
                pass
    return touched

# ======================= MAIN =======================
def main() -> None:
    log(f"Source: {SUPPLIER_URL if SUPPLIER_URL else '(not set)'}")
    data = load_source_bytes(SUPPLIER_URL)

    src_root = ET.fromstring(data)
    shop_in  = src_root.find("shop") if src_root.tag.lower() != "shop" else src_root
    if shop_in is None:
        err("XML: <shop> not found")

    offers_in_el = shop_in.find("offers") or shop_in.find("Offers")
    if offers_in_el is None:
        err("XML: <offers> not found")

    src_offers = list(offers_in_el.findall("offer"))

    # Готовим выходную структуру
    out_root  = ET.Element("yml_catalog"); out_root.set("date", time.strftime("%Y-%m-%d %H:%M"))
    out_shop  = ET.SubElement(out_root, "shop")
    out_offers= ET.SubElement(out_shop, "offers")

    # 1) Копируем офферы 1:1 (дальше работаем только над полями, описание пока не трогаем)
    for o in src_offers:
        out_offers.append(deepcopy(o))

    # 2) Фильтр категорий
    removed_count = 0
    if ALSTYLE_CATEGORIES_MODE in {"include","exclude"}:
        id2name,id2parent,parent2children = parse_categories_tree(shop_in)
        rules_ids, rules_names = load_category_rules(ALSTYLE_CATEGORIES_PATH)
        # простой фильтр на основе id/имён
        for offer in list(out_offers.findall("offer")):
            cid_el = offer.find("categoryId")
            cid = (cid_el.text or "").strip() if cid_el is not None else ""
            name = _norm_text(id2name.get(cid, "")) if cid else ""
            ok = True
            if ALSTYLE_CATEGORIES_MODE == "include":
                ok = (cid in rules_ids) or (name and any(_cat_match(r, name) for r in rules_names))
            elif ALSTYLE_CATEGORIES_MODE == "exclude":
                ok = not ((cid in rules_ids) or (name and any(_cat_match(r, name) for r in rules_names)))
            if not ok:
                out_offers.remove(offer); removed_count += 1
        log(f"Category filter removed: {removed_count}")

    # 3) Нормализация полей каждого оффера (НЕ трогаем description тут)
    for offer in out_offers.findall("offer"):
        process_offer_fields(offer)

    # 4) FEED_META (как есть, немного ровняем время к Алматы)
    try:
        now_utc = datetime.now(timezone.utc)
        if ZoneInfo:
            tz = ZoneInfo("Asia/Almaty")
            now_local = now_utc.astimezone(tz)
        else:
            now_local = now_utc + timedelta(hours=5)
        total = len(list(out_offers.findall("offer")))
        meta = f"<!-- FEED_META: supplier={SUPPLIER_NAME}; fetched={SUPPLIER_URL}; build_local={now_local.strftime('%Y-%m-%d %H:%M')}; offers_total={len(src_offers)}; offers_after_filter={total} -->\n"
    except Exception:
        meta = f"<!-- FEED_META: supplier={SUPPLIER_NAME} -->\n"

    # 5) Плоская нормализация description (в одну строку текста)
    desc_touched = flatten_all_descriptions(out_shop); log(f"Descriptions flattened: {desc_touched}")

    # 6) Сериализация в bytes
    try:
        xml_bytes = ET.tostring(out_root, encoding=ENC, xml_declaration=True, short_empty_elements=False)
    except Exception:
        # подстраховка
        xml_bytes = ET.tostring(out_root, encoding="windows-1251", xml_declaration=True, short_empty_elements=False)

    # 7) FINAL STEP (safe): description spacing & multi-punct normalization
    try:
        # перед самой сериализацией (в дереве) прогоняем мягкую чистку пунктуации
        fix_all_descriptions_end(out_root)
    except Exception:
        pass

    # 8) Finalize descriptions: beautify as HTML + wrap in CDATA (safe, output-only)
    xml_bytes = _postprocess_descriptions_beautify_cdata(xml_bytes, ENC if 'ENC' in globals() else 'windows-1251')

    # 9) POST-SERIALIZATION: expand self-closing <description /> to <description></description>
    try:
        _txt = xml_bytes.decode(ENC if 'ENC' in globals() else 'windows-1251', errors="replace")
        _txt = re.sub(r'<\?xml[^>]*\?>', lambda m: m.group(0) + "\n" + meta, _txt, count=1)
        _txt = re.sub(r'<description\s*/>', '<description></description>', _txt)
        xml_bytes = _txt.encode(ENC if 'ENC' in globals() else 'windows-1251', errors="replace")
    except Exception as e:
        warn(f"post-serialization tweak warn: {e}")
        # хотя бы добавим FEED_META
        xml_bytes = (meta + xml_bytes.decode(ENC if 'ENC' in globals() else 'windows-1251', errors="replace")).encode(ENC if 'ENC' in globals() else 'windows-1251', errors="replace")

    # 10) Запись файла
    try:
        with open(OUT_FILE_YML, "wb") as f:
            f.write(xml_bytes)
    except Exception as e:
        # подстраховка с заменой проблемных символов
        warn(f"write failed with {ENC}, retry with xmlcharrefreplace: {e}")
        try:
            fallback_txt = xml_bytes.decode(ENC if 'ENC' in globals() else 'windows-1251', errors="replace")
            with open(OUT_FILE_YML, "w", encoding=ENC if 'ENC' in globals() else 'windows-1251', errors="xmlcharrefreplace") as f:
                f.write(fallback_txt)
        except Exception as e2:
            err(f"write failed: {e2}")

    # .nojekyll для GitHub Pages
    try:
        docs_dir = os.path.dirname(OUT_FILE_YML) or "docs"
        os.makedirs(docs_dir, exist_ok=True)
        open(os.path.join(docs_dir, ".nojekyll"), "wb").close()
    except Exception as e:
        warn(f".nojekyll create warn: {e}")

    log(f"Wrote: {OUT_FILE_YML} | encoding={ENC} | description=DESC-FLAT")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err(str(e))
