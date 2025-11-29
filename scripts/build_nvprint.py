# scripts/build_nvprint.py
# -*- coding: utf-8 -*-

import io
import math
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple


# Делает: читает переменную окружения (строка) с дефолтом.
def _env(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None or v == "" else v


# Делает: конфиг пайплайна (env -> поля).
class Cfg:
    def __init__(self) -> None:
        self.SUPPLIER_URL = _env("NVPRINT_XML_URL", _env("NVPRINT_URL", ""))
        self.NV_LOGIN = _env("NVPRINT_LOGIN", _env("NVPRINT_XML_USER", ""))
        self.NV_PASSWORD = _env("NVPRINT_PASSWORD", _env("NVPRINT_XML_PASS", ""))

        self.OUT_FILE = _env("OUT_FILE", "docs/nvprint.yml")
        self.OUTPUT_ENCODING = _env("OUT_ENCODING", "windows-1251")

        self.HTTP_TIMEOUT = int(_env("HTTP_TIMEOUT", "80"))
        self.RETRIES = int(_env("RETRIES", "3"))

        self.OFFER_PREFIX = "NP"


# Делает: список ключевых слов (фильтр по началу названия).
KEYWORDS: List[str] = [
    "Шлейф",
    "Блок фотобарабана",
    "Блок фотобарабарана",  # на всякий случай (как в старом workflow)
    "Картридж",
    "Печатающая головка",
    "Струйный картридж",
    "Тонер-картридж",
    "Тонер-туба",
]


PriceRule = Tuple[int, int, float, int]

# Делает: правила расчёта цены (4% + надбавки, затем хвост 900).
PRICING_RULES: List[PriceRule] = [
    (0, 10000, 4.0, 300),
    (10001, 20000, 4.0, 600),
    (20001, 40000, 4.0, 1000),
    (40001, 80000, 4.0, 2000),
    (80001, 120000, 4.0, 3000),
    (120001, 160000, 4.0, 4000),
    (160001, 200000, 4.0, 5000),
    (200001, 300000, 4.0, 12000),
    (300001, 400000, 4.0, 20000),
    (400001, 500000, 4.0, 30000),
    (500001, 750000, 4.0, 40000),
    (750001, 1000000, 4.0, 50000),
    (1000001, 1500000, 4.0, 70000),
    (1500001, 2000000, 4.0, 90000),
    (2000001, 100000000, 4.0, 100000),
]


# Делает: убирает namespace у тега.
def strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


# Делает: находит первого прямого ребёнка по списку имён и возвращает его text.
def first_child_text(elem: ET.Element, names: List[str]) -> Optional[str]:
    want = {n.strip().lower() for n in names}
    for ch in list(elem):
        if strip_ns(ch.tag).lower() in want:
            if ch.text is None:
                return None
            return ch.text.strip()
    return None


# Делает: находит первого потомка по списку имён.
def find_descendant(elem: ET.Element, names: List[str]) -> Optional[ET.Element]:
    want = {n.strip().lower() for n in names}
    for node in elem.iter():
        if strip_ns(node.tag).lower() in want:
            return node
    return None


# Делает: возвращает text первого найденного потомка.
def find_descendant_text(elem: ET.Element, names: List[str]) -> Optional[str]:
    el = find_descendant(elem, names)
    if el is None or el.text is None:
        return None
    t = el.text.strip()
    return t if t != "" else None


# Делает: парсит число из строки (поддержка "12 345,67").
def parse_number(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    t = str(s).strip().replace("\u00a0", " ").replace(" ", "")
    t = t.replace(",", ".")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


# Делает: нормализует номер договора (чтобы ловить 000079 + MSK/МСК).
def _norm_contract(s: str) -> str:
    t = (s or "").upper()
    t = t.replace(" ", "")
    t = t.replace("-", "")
    t = t.replace("_", "")
    return t


# Делает: вытаскивает цену из договоров с номером 000079 (приоритет KZ, затем MSK).
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


# Делает: округляет вверх до хвоста 900.
def round_up_tail_900(n: int) -> int:
    thousands = (n + 999) // 1000
    return thousands * 1000 - 100


# Делает: применяет PRICING_RULES к базовой цене.
def compute_price_from_supplier(base_price: Optional[int]) -> int:
    if base_price is None or base_price < 100:
        return 100
    for lo, hi, pct, add in PRICING_RULES:
        if lo <= base_price <= hi:
            raw = base_price * (1.0 + pct / 100.0) + add
            return round_up_tail_900(int(math.ceil(raw)))
    raw = base_price * (1.0 + PRICING_RULES[-1][2] / 100.0) + PRICING_RULES[-1][3]
    return round_up_tail_900(int(math.ceil(raw)))


# Делает: чистит артикул (убирает NV- и пробелы).
def clean_article(raw: str) -> str:
    s = (raw or "").strip()
    s = re.sub(r"^\s*NV[\-\_\s]+", "", s, flags=re.IGNORECASE)
    s = s.replace(" ", "")
    return s


# Делает: формирует id/vendorCode (offer id = vendorCode).
def make_ids_from_article(article: str, cfg: Cfg) -> Tuple[str, str]:
    c = clean_article(article)
    oid = f"{cfg.OFFER_PREFIX}{c}"
    return oid, oid


# Делает: фильтр "название начинается с одного из KEYWORDS".
def name_starts_with_keywords(name: str, keywords: List[str]) -> bool:
    t = (name or "").strip()
    if not t:
        return False
    tt = t.casefold()
    for kw in keywords:
        if tt.startswith((kw or "").strip().casefold()):
            return True
    return False


# Делает: вытаскивает список принтеров из "Для_устройств/Принтер".
def extract_compatible_printers(item: ET.Element) -> List[str]:
    printers: List[str] = []
    for node in item.iter():
        if strip_ns(node.tag).lower() != "принтер":
            continue
        if node.text and node.text.strip():
            printers.append(re.sub(r"\s+", " ", node.text.strip()))
    seen: set[str] = set()
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
        tip = tip.strip()
        if tip:
            specs.append(f"- Тип печати: {tip}")

    cvet = find_descendant_text(item, ["Цвет"])
    if cvet:
        cvet = cvet.strip()
        if cvet:
            specs.append(f"- Цвет: {cvet}")

    type_rash = find_descendant_text(item, ["ТипРасходника"])
    if type_rash:
        type_rash = type_rash.strip()
        if type_rash:
            specs.append(f"- Тип расходника: {type_rash}")

    kod_factory = find_descendant_text(item, ["КодЗаводской"])
    if kod_factory:
        kod_factory = kod_factory.strip()
        if kod_factory:
            specs.append(f"- Заводской код: {kod_factory}")

    ean = find_descendant_text(item, ["EAN"])
    if ean:
        ean = ean.strip()
        if ean:
            specs.append(f"- EAN: {ean}")

    printers = extract_compatible_printers(item)
    if printers:
        specs.append("- Совместимые устройства: " + ", ".join(printers))

    if specs:
        if parts:
            parts.append("Технические характеристики:")
        parts.extend(specs)

    return "\n".join(parts).strip()


# Делает: парсит один товарный узел.
def parse_item(elem: ET.Element, cfg: Cfg) -> Optional[Dict[str, Any]]:
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
    oid, vcode = make_ids_from_article(article, cfg)

    return {
        "id": oid,
        "vendorCode": vcode,
        "title": name_short,
        "price": final_price,
        "picture": picture,
        "vendor": vendor,
        "description": description,
    }


# Делает: возвращает подписанный сейчас Алматы (UTC+5).
def almaty_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5)


# Делает: считает ближайшее (1/10/20) в 04:00 Алматы.
def next_build_1_10_20_at_04(now_alm: datetime) -> datetime:
    targets = [1, 10, 20]
    for d in targets:
        try:
            cand = now_alm.replace(day=d, hour=4, minute=0, second=0, microsecond=0)
            if cand > now_alm:
                return cand
        except ValueError:
            pass

    if now_alm.month == 12:
        return now_alm.replace(year=now_alm.year + 1, month=1, day=1, hour=4, minute=0, second=0, microsecond=0)
    first_next = (now_alm.replace(day=1, hour=4, minute=0, second=0, microsecond=0) + timedelta(days=32)).replace(day=1)
    return first_next


# Делает: форматирует время Алматы в YYYY-MM-DD HH:MM:SS (как у других поставщиков).
def fmt_alm(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# Делает: рендерит FEED_META блок в стиле остальных поставщиков.
def render_feed_meta_comment(cfg: Cfg, offers_total: int, offers_written: int) -> str:
    now_alm = almaty_now()
    next_alm = next_build_1_10_20_at_04(now_alm)

    rows = [
        ("Поставщик", "NVPrint"),
        ("URL поставщика", cfg.SUPPLIER_URL),
        ("Время сборки (Алматы)", fmt_alm(now_alm)),
        ("Ближайшая сборка (Алматы)", fmt_alm(next_alm)),
        ("Сколько товаров у поставщика до фильтра", str(offers_total)),
        ("Сколько товаров у поставщика после фильтра", str(offers_written)),
        ("Сколько товаров есть в наличии (true)", str(offers_written)),
        ("Сколько товаров нет в наличии (false)", "0"),
    ]
    key_w = max(len(k) for k, _ in rows)
    out = ["<!--FEED_META"]
    for k, v in rows:
        out.append(f"{k.ljust(key_w)} | {v}")
    out.append("-->")
    return "\n".join(out)


# Делает: скачивает XML (requests + basic auth).
def read_source_bytes(cfg: Cfg) -> bytes:
    if not cfg.SUPPLIER_URL:
        raise RuntimeError("NVPRINT_XML_URL (или NVPRINT_URL) пустой")

    try:
        import requests  # type: ignore
    except Exception:
        raise RuntimeError("requests недоступен")

    auth = (cfg.NV_LOGIN, cfg.NV_PASSWORD) if (cfg.NV_LOGIN or cfg.NV_PASSWORD) else None
    last_err: Optional[Exception] = None

    for _ in range(max(1, cfg.RETRIES)):
        try:
            r = requests.get(cfg.SUPPLIER_URL, timeout=cfg.HTTP_TIMEOUT, auth=auth)
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized: проверь secrets NVPRINT_LOGIN/NVPRINT_PASSWORD")
            r.raise_for_status()
            if not r.content:
                raise RuntimeError("Источник вернул пустой ответ")
            return r.content
        except Exception as e:
            last_err = e

    raise RuntimeError(str(last_err) if last_err else "Не удалось скачать XML")


# Делает: угадывает узлы товаров (берём родителя всех <Артикул>, у кого есть НоменклатураКратко).
def guess_item_nodes(root: ET.Element) -> List[ET.Element]:
    want_art = {"артикул", "articul", "sku", "article", "partnumber"}
    parent_map = {c: p for p in root.iter() for c in list(p)}

    items: List[ET.Element] = []
    seen: set[int] = set()

    for el in root.iter():
        if strip_ns(el.tag).lower() not in want_art:
            continue
        item = parent_map.get(el)
        if item is None:
            continue
        key = id(item)
        if key in seen:
            continue
        if find_descendant(item, ["НоменклатураКратко"]) is None:
            continue
        seen.add(key)
        items.append(item)

    return items


# Делает: экранирует текст для XML.
def yml_escape(s: str) -> str:
    t = "" if s is None else str(s)
    t = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", t)
    t = t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    t = t.replace('"', "&quot;").replace("'", "&apos;")
    return t


# Делает: собирает итоговый YML в стиле остальных поставщиков.
def parse_xml_to_yml(xml_bytes: bytes, cfg: Cfg) -> str:
    root = ET.fromstring(xml_bytes)

    nodes = guess_item_nodes(root)
    offers_total = len(nodes)

    offers: List[Dict[str, Any]] = []
    for node in nodes:
        name_short = find_descendant_text(node, ["НоменклатураКратко"]) or ""
        if not name_starts_with_keywords(name_short, KEYWORDS):
            continue
        it = parse_item(node, cfg)
        if it:
            offers.append(it)

    now_alm = almaty_now()
    date_attr = now_alm.strftime("%Y-%m-%d %H:%M")

    out: List[str] = []
    out.append('<?xml version="1.0" encoding="windows-1251"?>')
    out.append('<!DOCTYPE yml_catalog SYSTEM "shops.dtd">')
    out.append(f'<yml_catalog date="{date_attr}">')
    out.append("<shop><offers>")
    out.append("")
    out.append(render_feed_meta_comment(cfg, offers_total, len(offers)))
    out.append("")
    for it in offers:
        out.append(f'<offer id="{yml_escape(it["id"])}" available="true">')
        out.append("<categoryId></categoryId>")
        out.append(f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>")
        out.append(f"<name>{yml_escape(it['title'])}</name>")
        out.append(f"<price>{int(it['price'])}</price>")
        if it.get("picture"):
            out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        if it.get("vendor"):
            out.append(f"<vendor>{yml_escape(it['vendor'])}</vendor>")
        out.append("<currencyId>KZT</currencyId>")
        if it.get("description"):
            desc_clean = re.sub(r"\s+", " ", it["description"]).strip()
            out.append(f"<description>{yml_escape(desc_clean)}</description>")
        out.append("</offer>")
        out.append("")

    out.append("</offers>")
    out.append("</shop>")
    out.append("</yml_catalog>")
    return "\n".join(out) + "\n"


# Делает: строит пустой фид (если ошибка скачивания/парсинга) в таком же формате.
def empty_yml(cfg: Cfg) -> str:
    now_alm = almaty_now()
    date_attr = now_alm.strftime("%Y-%m-%d %H:%M")
    meta = render_feed_meta_comment(cfg, 0, 0)

    out: List[str] = []
    out.append('<?xml version="1.0" encoding="windows-1251"?>')
    out.append('<!DOCTYPE yml_catalog SYSTEM "shops.dtd">')
    out.append(f'<yml_catalog date="{date_attr}">')
    out.append("<shop><offers>")
    out.append("")
    out.append(meta)
    out.append("")
    out.append("</offers>")
    out.append("</shop>")
    out.append("</yml_catalog>")
    return "\n".join(out) + "\n"


# Делает: точка входа (пишет docs/nvprint.yml windows-1251).
def main() -> int:
    cfg = Cfg()
    try:
        data = read_source_bytes(cfg)
        yml = parse_xml_to_yml(data, cfg)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        yml = empty_yml(cfg)

    out_dir = os.path.dirname(cfg.OUT_FILE) or "."
    os.makedirs(out_dir, exist_ok=True)
    with io.open(cfg.OUT_FILE, "w", encoding=cfg.OUTPUT_ENCODING, errors="ignore") as f:
        f.write(yml)

    print(f"Wrote: {cfg.OUT_FILE} | encoding={cfg.OUTPUT_ENCODING}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
