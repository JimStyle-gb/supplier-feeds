# scripts/build_nvprint.py
# -*- coding: utf-8 -*-

import io
import math
import os
import re
import sys
import time
import html as _html
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests  # type: ignore
except Exception:
    requests = None


# Делает: читает переменную окружения (строка) с дефолтом.
def _env(key: str, default: str = "") -> str:
    v = os.getenv(key)
    return default if v is None or v.strip() == "" else v.strip()


# Делает: конфиг пайплайна (env -> поля).
class Cfg:
    def __init__(self) -> None:
        self.SUPPLIER_URL = _env("NVPRINT_XML_URL", _env("NVPRINT_URL", ""))
        self.NV_LOGIN = _env("NVPRINT_LOGIN", _env("NVPRINT_XML_USER", ""))
        self.NV_PASSWORD = _env("NVPRINT_PASSWORD", _env("NVPRINT_XML_PASS", ""))

        self.OUT_FILE = _env("OUT_FILE", "docs/nvprint.yml")
        self.OUTPUT_ENCODING = _env("OUT_ENCODING", "windows-1251")

        self.HTTP_TIMEOUT = float(_env("HTTP_TIMEOUT", "60"))
        self.RETRIES = int(_env("RETRIES", "6"))
        self.BACKOFF_BASE = float(_env("RETRY_BACKOFF_S", "2"))

        self.OFFER_PREFIX = "NP"


# Делает: версия скрипта (чтобы понимать, что реально запустилось).
SCRIPT_VERSION = "build_nvprint_11_keep_last_on_500"


# Делает: список ключевых слов (фильтр по началу названия).
KEYWORDS: List[str] = [
    "Шлейф",
    "Блок фотобарабана",
    "Блок фотобарабарана",
    "Картридж",
    "Печатающая головка",
    "Струйный картридж",
    "Тонер-картридж",
    "Тонер-туба",
]


PriceRule = Tuple[int, int, float, int]
ParamList = List[Tuple[str, str]]


# Делает: правила расчёта цены (4% + надбавки, затем хвост 900).
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


# Делает: WhatsApp блок (ASCII/HTML entities, чтобы windows-1251 не резал).
WHATSAPP_BLOCK = (
    "<!-- WhatsApp -->\n"
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; "
    "border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);\">"
    "&#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP"
    "</a>"
    "</p>"
    "</div>"
)


# Делает: текущее время Алматы (UTC+5) как naive datetime.
def almaty_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5)


# Делает: форматирует время Алматы в YYYY-MM-DD HH:MM:SS.
def fmt_alm(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# Делает: считает ближайшее (1/10/20) в 01:00 Алматы.
def next_build_1_10_20_at_01(now_alm: datetime) -> datetime:
    targets = [1, 10, 20]
    for d in targets:
        try:
            cand = now_alm.replace(day=d, hour=1, minute=0, second=0, microsecond=0)
            if cand > now_alm:
                return cand
        except ValueError:
            pass

    if now_alm.month == 12:
        return now_alm.replace(year=now_alm.year + 1, month=1, day=1, hour=1, minute=0, second=0, microsecond=0)
    first_next = (now_alm.replace(day=1, hour=1, minute=0, second=0, microsecond=0) + timedelta(days=32)).replace(day=1)
    return first_next


# Делает: рендерит FEED_META блок как у остальных.
def render_feed_meta_comment(cfg: Cfg, offers_total: int, offers_written: int, avail_true: int, avail_false: int) -> str:
    now_alm = almaty_now()
    next_alm = next_build_1_10_20_at_01(now_alm)

    rows = [
        ("Поставщик", "NVPrint"),
        ("URL поставщика", cfg.SUPPLIER_URL),
        ("Время сборки (Алматы)", fmt_alm(now_alm)),
        ("Ближайшая сборка (Алматы)", fmt_alm(next_alm)),
        ("Сколько товаров у поставщика до фильтра", str(offers_total)),
        ("Сколько товаров у поставщика после фильтра", str(offers_written)),
        ("Сколько товаров есть в наличии (true)", str(avail_true)),
        ("Сколько товаров нет в наличии (false)", str(avail_false)),
    ]
    key_w = max(len(k) for k, _ in rows)
    out = ["<!--FEED_META"]
    for k, v in rows:
        out.append(f"{k.ljust(key_w)} | {v}")
    out.append("-->")
    return "\n".join(out)


# Делает: убирает namespace у тега.
def strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


# Делает: возвращает текст первого найденного потомка по именам тегов.
def find_descendant_text(elem: ET.Element, names: List[str]) -> Optional[str]:
    want = {n.strip().lower() for n in names}
    for node in elem.iter():
        if strip_ns(node.tag).lower() in want:
            if node.text is None:
                continue
            t = node.text.strip()
            return t if t != "" else None
    return None


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
        return float(Decimal(m.group(0)))
    except (InvalidOperation, ValueError):
        return None


# Делает: нормализует номер договора (чтобы ловить 000079 + MSK/МСК).
def _norm_contract(s: str) -> str:
    t = (s or "").upper()
    t = t.replace(" ", "").replace("-", "").replace("_", "")
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

        price_el = None
        for x in node.iter():
            if strip_ns(x.tag).lower() in {"цена", "price", "amount", "value"}:
                price_el = x
                break

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
    if base_price >= 9_000_000:
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
    s = re.sub(r"^\s*NV[-_\s]+", "", s, flags=re.IGNORECASE)
    s = s.replace(" ", "")
    return s


# Делает: формирует id/vendorCode (offer id = vendorCode).
def make_ids_from_article(article: str, cfg: Cfg) -> Tuple[str, str]:
    c = clean_article(article)
    oid = f"{cfg.OFFER_PREFIX}{c}"
    return oid, oid


# Делает: фильтр "название начинается с одного из KEYWORDS".
def name_starts_with_keywords(name: str) -> bool:
    t = (name or "").strip()
    if not t:
        return False
    tt = t.casefold()
    for kw in KEYWORDS:
        if tt.startswith((kw or "").strip().casefold()):
            return True
    return False


# Делает: пытается определить available по остаткам/количеству; если поля нет — true.
def infer_available(item: ET.Element) -> bool:
    qty = find_descendant_text(item, ["Остаток", "Остатки", "Количество", "КолВо", "Qty", "Quantity", "Stock", "InStock"])
    n = parse_number(qty) if qty else None
    if n is None:
        return True
    return n > 0


# Делает: вытаскивает список принтеров из "Принтер"/"printer" (уникально, по порядку).
def collect_printers(item: ET.Element) -> List[str]:
    printers: List[str] = []
    for node in item.iter():
        if strip_ns(node.tag).lower() in {"принтер", "printer"}:
            if node.text and node.text.strip():
                printers.append(re.sub(r"\s+", " ", node.text.strip()))
    seen: set[str] = set()
    uniq: List[str] = []
    for p in printers:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


# Делает: вытаскивает ресурс из текста "(12000k)".
def pull_resurs_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"\((\d{2,7})\s*[kк]\)", text, flags=re.IGNORECASE)
    return m.group(1) if m else None


# Делает: собирает характеристики (XML + из текста).
def build_characteristics(item: ET.Element, name_short: str, name_full: str) -> Dict[str, str]:
    chars: Dict[str, str] = {}

    res = find_descendant_text(item, ["Ресурс", "Resurs", "Yield", "PageYield"]) or ""
    if not res or res.strip() in {"0", "0.0"}:
        res = pull_resurs_from_text(name_full) or pull_resurs_from_text(name_short) or ""
    if res:
        chars["Ресурс"] = re.sub(r"\s+", " ", res).strip()

    tp = find_descendant_text(item, ["ТипПечати", "Тип печати", "PrintType"]) or ""
    if tp:
        chars["Тип печати"] = re.sub(r"\s+", " ", tp).strip()

    color = find_descendant_text(item, ["ЦветПечати", "Цвет", "Color", "Colour"]) or ""
    if color:
        chars["Цвет печати"] = re.sub(r"\s+", " ", color).strip()

    compat = find_descendant_text(item, ["Совместимость", "Compatibility", "Models", "СовместимостьСМоделями"]) or ""
    if compat:
        chars["Совместимость с моделями"] = re.sub(r"\s+", " ", compat).strip()

    weight = find_descendant_text(item, ["Вес", "Weight"]) or ""
    if weight and weight.strip() not in {"0", "0.0"}:
        chars["Вес"] = re.sub(r"\s+", " ", weight).strip()

    printers = collect_printers(item)
    if printers:
        chars["Совместимые устройства"] = ", ".join(printers)

    if "Совместимость с моделями" in chars and "Совместимые устройства" in chars:
        a = chars["Совместимость с моделями"].casefold()
        b = chars["Совместимые устройства"].casefold()
        if a and b and (a in b or b in a):
            chars.pop("Совместимость с моделями", None)

    return chars


# Делает: сортирует характеристики (приоритетные ключи сверху, дальше по алфавиту).
def sort_characteristics(chars: Dict[str, str]) -> ParamList:
    priority = ["Ресурс", "Тип печати", "Цвет печати", "Совместимые устройства", "Совместимость с моделями", "Вес"]
    out: ParamList = []
    used: set[str] = set()

    for k in priority:
        if k in chars and chars[k]:
            out.append((k, chars[k]))
            used.add(k)

    for k in sorted((x for x in chars.keys() if x not in used), key=lambda s: s.casefold()):
        if chars[k]:
            out.append((k, chars[k]))

    return out


# Делает: переводит многострочный текст в HTML-параграф с <br>.
def to_html_paragraph(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    t = _html.escape(t)
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t.replace("\n\n", "<br><br>").replace("\n", "<br>")


# Делает: собирает HTML описания + характеристики ul.
def build_description_html(title: str, body_text: str, params: ParamList) -> str:
    tt = _html.escape(title or "")
    p = to_html_paragraph(body_text)
    p = f"<p>{p}</p>" if p else ""

    if params:
        li = "".join([f"<li><strong>{_html.escape(k)}:</strong> {_html.escape(v)}</li>" for k, v in params if v])
        hchars = f"<h3>Характеристики</h3><ul>{li}</ul>"
    else:
        hchars = ""

    return f"<h3>{tt}</h3>{p}{hchars}"


# Делает: экранирует текст для XML.
def yml_escape(s: str) -> str:
    return _html.escape((s or "").strip())


# Делает: строит keywords (простая версия, чтобы было стабильно).
def build_keywords(vendor: str, name: str) -> str:
    parts: List[str] = []
    if vendor:
        parts.append(vendor.strip())
    if name:
        parts.append(name.strip())
    txt = "; ".join(parts)
    return re.sub(r"\s+", " ", txt).strip()


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
        if find_descendant_text(item, ["НоменклатураКратко"]) is None:
            continue
        seen.add(key)
        items.append(item)

    return items


# Делает: парсит один товарный узел в dict (для оффера).
def parse_item(node: ET.Element, cfg: Cfg) -> Optional[Dict[str, Any]]:
    article = find_descendant_text(node, ["Артикул", "articul", "sku", "article", "PartNumber"])
    if not article:
        return None

    name_short = find_descendant_text(node, ["НоменклатураКратко"]) or ""
    name_short = re.sub(r"\s+", " ", name_short).strip()
    if not name_short or not name_starts_with_keywords(name_short):
        return None

    name_full = (
        find_descendant_text(node, ["Номенклатура", "Описание", "Description", "FullName", "Name"])
        or name_short
    )
    name_full = re.sub(r"\s+", " ", name_full).strip()

    base = extract_price_from_contracts(node)
    base_int = 100 if (base is None or base <= 0) else int(math.ceil(base))
    final_price = compute_price_from_supplier(base_int)

    vendor = find_descendant_text(node, ["Бренд", "Производитель", "Вендор", "Brand", "Vendor"]) or ""
    picture = (
        find_descendant_text(
            node,
            ["СсылкаНаКартинку", "Картинка", "Изображение", "Фото", "Picture", "Image", "ФотоURL", "PictureURL"],
        )
        or ""
    ).strip()

    oid, vcode = make_ids_from_article(article, cfg)
    available = infer_available(node)

    chars = build_characteristics(node, name_short, name_full)
    params = sort_characteristics(chars)

    desc_html = build_description_html(name_short, name_full, params).replace("]]>", "]]&gt;")
    keywords = build_keywords(vendor, name_short)

    return {
        "id": oid,
        "available": available,
        "vendorCode": vcode,
        "name": name_short,
        "price": final_price,
        "picture": picture,
        "vendor": vendor.strip(),
        "description_html": desc_html,
        "params": params,
        "keywords": keywords,
    }


# Делает: гарантирует 2 перевода строки после <shop><offers> и перед </offers>.
def ensure_footer_spacing(s: str) -> str:
    s = s.replace("<shop><offers>\n", "<shop><offers>\n\n")
    s = re.sub(r"</offer>\n</offers>", "</offer>\n\n</offers>", s)
    return s


# Делает: скачивает XML с ретраями и умной обработкой 5xx.
def read_source_bytes(cfg: Cfg) -> bytes:
    if not cfg.SUPPLIER_URL:
        raise RuntimeError("NVPRINT_XML_URL (или NVPRINT_URL) пустой")
    if requests is None:
        raise RuntimeError("requests недоступен")

    auth = (cfg.NV_LOGIN, cfg.NV_PASSWORD) if (cfg.NV_LOGIN or cfg.NV_PASSWORD) else None
    headers = {"User-Agent": "supplier-feeds/nvprint"}

    last_err: Optional[Exception] = None
    for attempt in range(1, max(1, cfg.RETRIES) + 1):
        try:
            r = requests.get(cfg.SUPPLIER_URL, timeout=cfg.HTTP_TIMEOUT, auth=auth, headers=headers)
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized: проверь NVPRINT_LOGIN/NVPRINT_PASSWORD")
            if r.status_code in {500, 502, 503, 504}:
                raise RuntimeError(f"{r.status_code} Server Error")
            r.raise_for_status()
            if not r.content:
                raise RuntimeError("Источник вернул пустой ответ")
            return r.content
        except Exception as e:
            last_err = e
            if attempt >= cfg.RETRIES:
                break
            # экспоненциальный бэкофф: 2,4,8,16...
            time.sleep(cfg.BACKOFF_BASE * (2 ** (attempt - 1)))

    raise RuntimeError(str(last_err) if last_err else "Не удалось скачать XML")


# Делает: собирает итоговый YML в стиле остальных поставщиков.
def build_yml(cfg: Cfg, xml_bytes: bytes) -> str:
    root = ET.fromstring(xml_bytes)
    nodes = guess_item_nodes(root)
    offers_total = len(nodes)

    offers: List[Dict[str, Any]] = []
    for node in nodes:
        it = parse_item(node, cfg)
        if it:
            offers.append(it)

    avail_true = sum(1 for o in offers if o["available"])
    avail_false = sum(1 for o in offers if not o["available"])

    now_alm = almaty_now()
    date_attr = now_alm.strftime("%Y-%m-%d %H:%M")

    out: List[str] = []
    out.append('<?xml version="1.0" encoding="windows-1251"?>')
    out.append('<!DOCTYPE yml_catalog SYSTEM "shops.dtd">')
    out.append(f'<yml_catalog date="{date_attr}">')
    out.append("<shop><offers>")
    out.append("")
    out.append(render_feed_meta_comment(cfg, offers_total, len(offers), avail_true, avail_false))
    out.append("")

    for it in offers:
        out.append(f'<offer id="{yml_escape(it["id"])}" available="{str(it["available"]).lower()}">')
        out.append("<categoryId></categoryId>")
        out.append(f"<vendorCode>{yml_escape(it['vendorCode'])}</vendorCode>")
        out.append(f"<name>{yml_escape(it['name'])}</name>")
        out.append(f"<price>{int(it['price'])}</price>")
        if it.get("picture"):
            out.append(f"<picture>{yml_escape(it['picture'])}</picture>")
        if it.get("vendor"):
            out.append(f"<vendor>{yml_escape(it['vendor'])}</vendor>")
        out.append("<currencyId>KZT</currencyId>")

        out.append("<description><![CDATA[")
        out.append("")
        out.append(WHATSAPP_BLOCK)
        out.append("")
        out.append("<!-- Описание -->")
        out.append(it["description_html"])
        out.append("")
        out.append("]]></description>")

        for k, v in it.get("params", []):
            if v:
                out.append(f'<param name="{yml_escape(k)}">{yml_escape(v)}</param>')

        if it.get("keywords"):
            out.append(f"<keywords>{yml_escape(it['keywords'])}</keywords>")

        out.append("</offer>")
        out.append("")

    out.append("</offers>")
    out.append("</shop>")
    out.append("</yml_catalog>")

    return ensure_footer_spacing("\n".join(out))


# Делает: строит пустой фид (если файла ещё нет и источник упал).
def empty_yml(cfg: Cfg) -> str:
    now_alm = almaty_now()
    date_attr = now_alm.strftime("%Y-%m-%d %H:%M")
    meta = render_feed_meta_comment(cfg, 0, 0, 0, 0)

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


# Делает: точка входа (если NVPrint API отдаёт 5xx — оставляет прошлый файл без изменений).
def main() -> int:
    print(f"NVPRINT_SCRIPT_VERSION={SCRIPT_VERSION}")

    cfg = Cfg()
    try:
        data = read_source_bytes(cfg)
        yml = build_yml(cfg, data)

        out_dir = os.path.dirname(cfg.OUT_FILE) or "."
        os.makedirs(out_dir, exist_ok=True)
        with io.open(cfg.OUT_FILE, "w", encoding=cfg.OUTPUT_ENCODING, errors="ignore") as f:
            f.write(yml)

        print(f"Wrote: {cfg.OUT_FILE} | encoding={cfg.OUTPUT_ENCODING}")
        return 0

    except Exception as e:
        # Если файл уже есть — НЕ перезатираем его пустым фидом.
        if os.path.exists(cfg.OUT_FILE):
            print(f"ERROR: {e}", file=sys.stderr)
            print(f"NOTE: supplier is down -> keeping existing {cfg.OUT_FILE} unchanged", file=sys.stderr)
            return 0

        # Если файла ещё нет — пишем пустой корректный фид.
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
