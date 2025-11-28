# scripts/build_nvprint.py
# -*- coding: utf-8 -*-
'''
NVPrint -> YML (KZT) — переписано "с нуля", но поведение/результат сохранены.

Цель: выдавать nvprint.yml в той же структуре, что и текущий пайплайн:
- <?xml version='1.0' encoding='windows-1251'?>
- <yml_catalog date="UTC YYYY-MM-DD HH:MM">
- <!--FEED_META ... --> (дд:мм:гггг - чч:мм:сс по Алматы)
- <shop><offers> ... </offers></shop></yml_catalog>
- offer: vendorCode, name, price, picture?, vendor?, currencyId, available, description?
- available всегда true (как сейчас)

Переменные окружения:
- NVPRINT_XML_URL (или NVPRINT_URL): URL источника
- NVPRINT_LOGIN / NVPRINT_PASSWORD (или NVPRINT_XML_USER / NVPRINT_XML_PASS): basic auth
- NVPRINT_KEYWORDS_FILE: путь к keywords (default docs/nvprint_keywords.txt)
- OUT_FILE: путь результата (default docs/nvprint.yml)
- OUT_ENCODING: кодировка результата (default windows-1251)
- HTTP_TIMEOUT, RETRIES, RETRY_BACKOFF_S
'''
from __future__ import annotations

import io
import math
import os
import re
import sys
import time
import html
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

try:
    import requests
except Exception:
    requests = None


# Делает: читает переменную окружения и возвращает непустое значение или default.
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return (v.strip() if v and v.strip() else default)


# Делает: хранит настройки пайплайна (с дефолтами как в текущей версии).
class Cfg:
    DEFAULT_URL = "https://api.nvprint.ru/api/hs/getprice/398/881105302369/none/?format=xml&getallinfo=true"

    SUPPLIER_URL = _env("NVPRINT_XML_URL", _env("NVPRINT_URL", DEFAULT_URL))
    OUT_FILE = _env("OUT_FILE", "docs/nvprint.yml")
    OUTPUT_ENCODING = _env("OUT_ENCODING", "windows-1251")

    HTTP_TIMEOUT = float(_env("HTTP_TIMEOUT", "45"))
    RETRIES = int(_env("RETRIES", "4"))
    RETRY_BACKOFF_S = float(_env("RETRY_BACKOFF_S", "2"))

    NV_LOGIN = _env("NVPRINT_LOGIN", _env("NVPRINT_XML_USER", ""))
    NV_PASSWORD = _env("NVPRINT_PASSWORD", _env("NVPRINT_XML_PASS", ""))

    KEYWORDS_FILE = _env("NVPRINT_KEYWORDS_FILE", "docs/nvprint_keywords.txt")


# Делает: экранирует значения для XML/YML.
def yml_escape(s: str) -> str:
    return html.escape((s or "").strip())


# Делает: убирает namespace из tag, если он есть.
def strip_ns(tag: str) -> str:
    if not tag:
        return tag
    if tag.startswith("{"):
        i = tag.rfind("}")
        if i != -1:
            return tag[i + 1 :]
    return tag


# Делает: парсит число из строки (с запятой/пробелами), возвращает float или None.
def parse_number(txt: Optional[str]) -> Optional[float]:
    if not txt:
        return None
    t = txt.strip().replace("\u00A0", "").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        return float(Decimal(m.group(0)))
    except (InvalidOperation, ValueError):
        return None


# Делает: берёт текст первого прямого ребёнка по набору имён тегов.
def first_child_text(item: ET.Element, tag_names: List[str]) -> Optional[str]:
    names = {t.lower() for t in tag_names}
    for ch in item:
        if strip_ns(ch.tag).lower() in names:
            val = (ch.text or "").strip()
            if val:
                return val
    return None


# Делает: находит первого потомка (в глубину) по набору имён тегов.
def find_descendant(item: ET.Element, tag_names: List[str]) -> Optional[ET.Element]:
    names = {t.lower() for t in tag_names}
    for node in item.iter():
        if strip_ns(node.tag).lower() in names:
            return node
    return None


# Делает: возвращает текст найденного потомка по набору имён тегов.
def find_descendant_text(item: ET.Element, tag_names: List[str]) -> Optional[str]:
    node = find_descendant(item, tag_names)
    if node is not None:
        txt = (node.text or "").strip()
        if txt:
            return txt
    return None


# Делает: скачивает XML байты с ретраями и опциональным BasicAuth.
def read_source_bytes(cfg: Cfg) -> bytes:
    if not cfg.SUPPLIER_URL:
        raise RuntimeError("SUPPLIER_URL пуст")
    if requests is None:
        raise RuntimeError("requests недоступен")

    auth = (cfg.NV_LOGIN, cfg.NV_PASSWORD) if (cfg.NV_LOGIN or cfg.NV_PASSWORD) else None
    last_err: Optional[Exception] = None

    for attempt in range(1, cfg.RETRIES + 1):
        try:
            r = requests.get(cfg.SUPPLIER_URL, timeout=cfg.HTTP_TIMEOUT, auth=auth)
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized: проверь NVPRINT_LOGIN/NVPRINT_PASSWORD в secrets")
            r.raise_for_status()
            b = r.content
            if not b:
                raise RuntimeError("Источник вернул пустой ответ")
            return b
        except Exception as e:
            last_err = e
            if attempt >= cfg.RETRIES or ("401" in str(e)):
                break
            time.sleep(cfg.RETRY_BACKOFF_S * attempt)

    raise RuntimeError(str(last_err) if last_err else "Не удалось скачать источник")


# Делает: читает текстовый файл, перебирая кодировки.
def read_text_with_encodings(path: str, encodings: List[str]) -> Optional[str]:
    if not os.path.isfile(path):
        return None
    for enc in encodings:
        try:
            with io.open(path, "r", encoding=enc) as f:
                return f.read()
        except Exception:
            continue
    try:
        with io.open(path, "rb") as f:
            raw = f.read()
        return raw.decode("latin-1", errors="ignore")
    except Exception:
        return None


# Делает: грузит ключевые слова и нормализует их (lower, пробелы, uniq).
def load_keywords(path: str) -> List[str]:
    txt = read_text_with_encodings(
        path,
        ["utf-8-sig", "utf-8", "utf-16", "cp1251", "koi8-r", "iso-8859-5", "cp866"],
    )
    if not txt:
        return []
    kws: List[str] = []
    for line in txt.splitlines():
        ln = line.strip()
        if not ln or ln.startswith("#") or ln.startswith(";"):
            continue
        ln = re.sub(r"\s+", " ", ln).strip().lower()
        if ln:
            kws.append(ln)
    seen = set()
    out: List[str] = []
    for k in kws:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out

# Делает: дефолтные keywords (если файла нет/пустой).
DEFAULT_KEYWORDS: List[str] = [
    "Блок фотобарабана",
    "Картридж",
    "Печатающая головка",
    "Струйный картридж",
    "Тонер-картридж",
    "Тонер-туба",
]


# Делает: грузит keywords из файла или берёт DEFAULT_KEYWORDS.
def get_keywords(cfg: Cfg) -> List[str]:
    kws = load_keywords(cfg.KEYWORDS_FILE)
    if kws:
        return kws
    out: List[str] = []
    seen = set()
    for k in DEFAULT_KEYWORDS:
        kk = re.sub(r"\s+", " ", (k or "").strip()).lower()
        if kk and kk not in seen:
            seen.add(kk)
            out.append(kk)
    return out


# Делает: нормализует строку для сравнения.
def norm_for_match(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


# Делает: проверяет, что короткое имя начинается с одного из keywords.
def name_starts_with_keywords(name_short: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    base = norm_for_match(name_short)
    for kw in keywords:
        if base.startswith(kw):
            return True
    return False


# Делает: нормализует номер договора (рус/анг смешение, пробелы/дефисы).
def _norm_contract(s: str) -> str:
    if not s:
        return ""
    tr = str.maketrans(
        {
            "А": "A",
            "В": "B",
            "Е": "E",
            "К": "K",
            "М": "M",
            "Н": "H",
            "О": "O",
            "Р": "P",
            "С": "C",
            "Т": "T",
            "Х": "X",
            "У": "Y",
            "а": "A",
            "в": "B",
            "е": "E",
            "к": "K",
            "м": "M",
            "н": "H",
            "о": "O",
            "р": "P",
            "с": "C",
            "т": "T",
            "х": "X",
            "у": "Y",
            "Ё": "E",
            "ё": "e",
        }
    )
    u = s.translate(tr).upper()
    u = re.sub(r"[\s\-\_]+", "", u)
    return u


# Делает: вытаскивает цену из договоров, приоритет Казахстан (не MSK) над Москвой.
def extract_price_from_contracts(item: ET.Element) -> Optional[float]:
    price_kz: Optional[float] = None
    price_msk: Optional[float] = None

    for node in item.iter():
        if strip_ns(node.tag).lower() != "договор":
            continue

        num = (node.attrib.get("НомерДоговора") or node.attrib.get("Номердоговора") or "").strip()
        num_n = _norm_contract(num)
        if "000079" not in num_n:
            continue

        price_el = find_descendant(node, ["Цена", "price", "amount", "value"])
        val = parse_number(price_el.text if price_el is not None else None)
        if val is None or val <= 0:
            continue

        if "MSK" in num_n or "МСК" in num_n:
            price_msk = val
        else:
            price_kz = val

    if price_kz is not None and price_kz > 0:
        return price_kz
    if price_msk is not None and price_msk > 0:
        return price_msk
    return None


PriceRule = Tuple[int, int, float, int]
PRICING_RULES: List[PriceRule] = [
    (101, 10000, 4.0, 3000),
    (10001, 25000, 4.0, 4000),
    (25001, 50000, 4.0, 5000),
    (50001, 75000, 4.0, 7000),
    (75001, 100000, 4.0, 10000),
    (100001, 150000, 4.0, 12000),
    (150001, 200000, 4.0, 15000),
    (200001, 300000, 4.0, 20000),
    (300001, 400000, 4.0, 25000),
    (400001, 500000, 4.0, 30000),
    (500001, 750000, 4.0, 40000),
    (750001, 1000000, 4.0, 50000),
    (1000001, 1500000, 4.0, 70000),
    (1500001, 2000000, 4.0, 90000),
    (2000001, 100000000, 4.0, 100000),
]


# Делает: округляет вверх до "…900" (1000-100, 2000-100, ...).
def round_up_tail_900(n: int) -> int:
    thousands = (n + 999) // 1000
    return thousands * 1000 - 100


# Делает: применяет PRICING_RULES к базовой цене (включая fallback 100).
def compute_price_from_supplier(base_price: Optional[int]) -> int:
    if base_price is None or base_price < 100:
        return 100
    for lo, hi, pct, add in PRICING_RULES:
        if lo <= base_price <= hi:
            raw = base_price * (1.0 + pct / 100.0) + add
            return round_up_tail_900(int(math.ceil(raw)))
    raw = base_price * (1.0 + PRICING_RULES[-1][2] / 100.0) + PRICING_RULES[-1][3]
    return round_up_tail_900(int(math.ceil(raw)))


# Делает: чистит артикул (убирает NV- и пробелы), как в текущем пайплайне.
def clean_article(raw: str) -> str:
    s = (raw or "").strip()
    s = re.sub(r"^\s*NV[\-\_\s]+", "", s, flags=re.IGNORECASE)
    s = s.replace(" ", "")
    return s


# Делает: формирует id и vendorCode из артикула (NP + cleaned).
def make_ids_from_article(article: str) -> Tuple[str, str]:
    ac = clean_article(article)
    pref = "NP" + ac
    return pref, pref


# Делает: вытаскивает список принтеров (уникальный, в исходном порядке).
def collect_printers(item: ET.Element) -> List[str]:
    printers: List[str] = []
    node = find_descendant(item, ["Принтеры"])
    if node is not None:
        for ch in node.iter():
            if strip_ns(ch.tag).lower() == "принтер":
                t = (ch.text or "").strip()
                if t:
                    printers.append(t)
    seen = set()
    uniq: List[str] = []
    for p in printers:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


# Делает: строит description (Номенклатура + "Технические характеристики" + пункты).
def build_description(item: ET.Element) -> str:
    parts: List[str] = []

    nom_full = find_descendant_text(item, ["Номенклатура"]) or ""
    nom_full = re.sub(r"\s+", " ", nom_full).strip()
    if nom_full:
        parts.append(nom_full)

    specs: List[str] = []

    resurs = find_descendant_text(item, ["Ресурс"])
    if resurs and resurs.strip() and resurs.strip() != "0":
        specs.append(f"- Ресурс: {resurs.strip()}")

    tip = find_descendant_text(item, ["ТипПечати"])
    if tip:
        specs.append(f"- Тип печати: {tip.strip()}")

    color = find_descendant_text(item, ["ЦветПечати"])
    if color:
        specs.append(f"- Цвет печати: {color.strip()}")

    compat = find_descendant_text(item, ["СовместимостьСМоделями"])
    if compat:
        compat = re.sub(r"\s+", " ", compat).strip()
        specs.append(f"- Совместимость с моделями: {compat}")

    weight = find_descendant_text(item, ["Вес"])
    if weight:
        specs.append(f"- Вес: {weight.strip()}")

    prn_list = collect_printers(item)
    if prn_list:
        specs.append(f"- Принтеры: {', '.join(prn_list)}")

    if specs:
        parts.append("Технические характеристики:")
        parts.extend(specs)

    return "\n".join(parts).strip()


# Делает: парсит один товарный узел в dict.
def parse_item(elem: ET.Element) -> Optional[Dict[str, Any]]:
    article = first_child_text(elem, ["Артикул", "articul", "sku", "article", "PartNumber"])
    if not article:
        return None

    name_short = find_descendant_text(elem, ["НоменклатураКратко"])
    if not name_short:
        return None
    name_short = re.sub(r"\s+", " ", name_short).strip()

    base = extract_price_from_contracts(elem)
    base_int = 100 if (base is None or base <= 0) else int(math.ceil(base))
    final_price = compute_price_from_supplier(base_int)

    vendor = first_child_text(elem, ["Бренд", "Производитель", "Вендор", "Brand", "Vendor"]) or ""
    picture = (
        first_child_text(
            elem,
            ["СсылкаНаКартинку", "Картинка", "Изображение", "Фото", "Picture", "Image", "ФотоURL", "PictureURL"],
        )
        or ""
    ).strip()

    description = build_description(elem)

    oid, vcode = make_ids_from_article(article)

    return {
        "id": oid,
        "vendorCode": vcode,
        "title": name_short,
        "price": final_price,
        "picture": picture,
        "vendor": vendor,
        "description": description,
    }


# Делает: находит кандидатов-узлы товаров (максимально терпимо к структуре XML).
def guess_item_nodes(root: ET.Element) -> List[ET.Element]:
    items: List[ET.Element] = []
    seen: set[int] = set()

    for node in root.iter():
        art = find_descendant(node, ["Артикул", "articul", "sku", "article", "PartNumber"])
        if art is None:
            continue
        nmk = find_descendant(node, ["НоменклатураКратко"])
        if nmk is None:
            continue

        key = id(node)
        if key in seen:
            continue
        seen.add(key)
        items.append(node)

    return items


# Делает: возвращает "сейчас" по Алматы (UTC+5).
def almaty_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5)


# Делает: находит ближайшее 1/10/20 число в 04:00 Алматы.
def next_build_1_10_20_at_04() -> datetime:
    now = almaty_now()
    targets = [1, 10, 20]
    cands: List[datetime] = []
    for d in targets:
        try:
            cands.append(now.replace(day=d, hour=4, minute=0, second=0, microsecond=0))
        except ValueError:
            pass

    future = [t for t in cands if t > now]
    if future:
        return min(future)

    if now.month == 12:
        return now.replace(year=now.year + 1, month=1, day=1, hour=4, minute=0, second=0, microsecond=0)

    first_next = (now.replace(day=1, hour=4, minute=0, second=0, microsecond=0) + timedelta(days=32)).replace(day=1)
    return first_next


# Делает: формат дд:мм:гггг - чч:мм:сс (как сейчас в meta).
def fmt_alm(dt: datetime) -> str:
    return dt.strftime("%d:%m:%Y - %H:%M:%S")


# Делает: рендерит FEED_META блок в точном формате.
def render_feed_meta_comment(pairs: Dict[str, Any]) -> str:
    now_alm = almaty_now()
    next_alm = next_build_1_10_20_at_04()
    rows = [
        ("Поставщик", "NVPrint"),
        ("URL поставщика", str(pairs.get("source", ""))),
        ("Время сборки (Алматы)", fmt_alm(now_alm)),
        ("Ближайшее время сборки (Алматы)", fmt_alm(next_alm)),
        ("Сколько товаров у поставщика до фильтра", str(pairs.get("offers_total", "0"))),
        ("Сколько товаров у поставщика после фильтра", str(pairs.get("offers_written", "0"))),
        ("Сколько товаров есть в наличии (true)", str(pairs.get("available_true", "0"))),
        ("Сколько товаров нет в наличии (false)", str(pairs.get("available_false", "0"))),
    ]
    key_w = max(len(k) for k, _ in rows)
    lines = ["<!--FEED_META"]
    for i, (k, v) in enumerate(rows):
        end = " -->" if i == len(rows) - 1 else ""
        lines.append(f"{k.ljust(key_w)} | {v}{end}")
    return "\n".join(lines)


# Делает: парсит xml -> items, фильтрует по keywords, собирает yml в текущем формате.
def parse_xml_to_yml(xml_bytes: bytes, cfg: Cfg) -> str:
    root = ET.fromstring(xml_bytes)

    keywords = get_keywords(cfg)
    nodes = guess_item_nodes(root)
    offers_total = len(nodes)

    offers: List[Dict[str, Any]] = []
    for node in nodes:
        name_short = find_descendant_text(node, ["НоменклатураКратко"]) or ""
        if not name_starts_with_keywords(name_short, keywords):
            continue
        it = parse_item(node)
        if it:
            offers.append(it)

    date_attr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append(f"<yml_catalog date=\"{date_attr}\">")

    meta_pairs = {
        "source": cfg.SUPPLIER_URL,
        "offers_total": offers_total,
        "offers_written": len(offers),
        "available_true": len(offers),  # всем true
        "available_false": 0,
    }
    out.append(render_feed_meta_comment(meta_pairs))

    out.append("<shop>")
    out.append("  <offers>")
    for it in offers:
        out.append(f"    <offer id=\"{yml_escape(it['id'])}\">")
        out.append(f"      <vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>")
        out.append(f"      <name>{yml_escape(it['title'])}</name>")
        out.append(f"      <price>{int(it['price'])}</price>")
        if it.get("picture"):
            out.append(f"      <picture>{yml_escape(it['picture'])}</picture>")
        if it.get("vendor"):
            out.append(f"      <vendor>{yml_escape(it['vendor'])}</vendor>")
        out.append("      <currencyId>KZT</currencyId>")
        out.append("      <available>true</available>")
        if it.get("description"):
            desc_clean = re.sub(r"\s+", " ", it["description"]).strip()
            out.append(f"      <description>{yml_escape(desc_clean)}</description>")
        out.append("    </offer>\n")
    out.append("  </offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)


# Делает: пишет пустой фид (как сейчас) при ошибке скачивания/парсинга.
def empty_yml(cfg: Cfg) -> str:
    date_attr = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    meta_pairs = {"source": cfg.SUPPLIER_URL, "offers_total": 0, "offers_written": 0, "available_true": 0, "available_false": 0}
    out = [
        "<?xml version='1.0' encoding='windows-1251'?>",
        f"<yml_catalog date=\"{date_attr}\">",
        render_feed_meta_comment(meta_pairs),
        "<shop>",
        "  <offers>",
        "  </offers>",
        "</shop></yml_catalog>",
    ]
    return "\n".join(out)


# Делает: точка входа.
def main() -> int:
    cfg = Cfg()
    try:
        data = read_source_bytes(cfg)
        yml = parse_xml_to_yml(data, cfg)
    except Exception as e:
        yml = empty_yml(cfg)
        print(f"ERROR: {e}", file=sys.stderr)

    os.makedirs(os.path.dirname(cfg.OUT_FILE), exist_ok=True)
    with io.open(cfg.OUT_FILE, "w", encoding=cfg.OUTPUT_ENCODING, errors="ignore") as f:
        f.write(yml)

    print(f"Wrote: {cfg.OUT_FILE} | encoding={cfg.OUTPUT_ENCODING}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
