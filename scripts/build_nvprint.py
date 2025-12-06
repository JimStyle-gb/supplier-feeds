from __future__ import annotations

import os
import re
import html
import math
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

try:
    import requests
except Exception:
    requests = None

_LAT_TR = str.maketrans({
    "А":"A","В":"B","Е":"E","К":"K","М":"M","Н":"H","О":"O","Р":"P","С":"C","Т":"T","Х":"X","У":"Y",
    "а":"A","в":"B","е":"E","к":"K","м":"M","н":"H","о":"O","р":"P","с":"C","т":"T","х":"X","у":"Y",
    "Ё":"E","ё":"e",
})


SUPPLIER_URL = (os.getenv("NVPRINT_XML_URL") or "https://api.nvprint.ru/api/hs/getprice/398/881105302369/none/?format=xml&getallinfo=true").strip()
OUT_FILE     = (os.getenv("OUT_FILE") or "docs/nvprint.yml").strip()

OUTPUT_ENCODING = "windows-1251"
HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT") or "60")
RETRIES         = 4
BACKOFF_S       = 2.0

NV_LOGIN    = (os.getenv("NVPRINT_LOGIN") or os.getenv("NVPRINT_XML_USER") or "").strip()
NV_PASSWORD = (os.getenv("NVPRINT_PASSWORD") or os.getenv("NVPRINT_XML_PASS") or "").strip()

KEYWORD_PREFIXES: List[str] = [
    "Блок фотобарабана",
    "Картридж",
    "Печатающая головка",
    "Струйный картридж",
    "Тонер-картридж",
    "Тонер-туба",
]

CITIES: List[str] = [
    "Алматы", "Астана", "Шымкент", "Караганда", "Актобе", "Павлодар", "Атырау", "Тараз",
    "Оскемен", "Семей", "Костанай", "Кызылорда", "Орал", "Петропавловск",
    "Талдыкорган", "Актау", "Темиртау", "Экибастуз", "Кокшетау",
]

WHATSAPP_BLOCK = """<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;"><p style="text-align:center; margin:0 0 12px;"><a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0" style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);">&#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!</a></p><div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;"><h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3><ul style="margin:0; padding-left:18px;"><li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li><li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li></ul><hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;" /><h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3><ul style="margin:0; padding-left:18px;"><li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li><li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li><li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li><li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li></ul></div></div>"""

DESC_PREFIX = [

<!-- WhatsApp -->
" + WHATSAPP_BLOCK + "

"

PriceRule = Tuple[int, int, float, int]
PRICING_RULES: List[PriceRule] = [
    (   101,    10000, 4.0,   3000),
    ( 10001,    25000, 4.0,   4000),
    ( 25001,    50000, 4.0,   5000),
    ( 50001,    75000, 4.0,   7000),
    ( 75001,   100000, 4.0,  10000),
    (100001,   150000, 4.0,  12000),
    (150001,   200000, 4.0,  15000),
    (200001,   300000, 4.0,  20000),
    (300001,   400000, 4.0,  25000),
    (400001,   500000, 4.0,  30000),
    (500001,   750000, 4.0,  40000),
    (750001,  1000000, 4.0,  50000),
    (1000001, 1500000, 4.0,  70000),
    (1500001, 2000000, 4.0,  90000),
    (2000001,100000000,4.0, 100000),
]


def _almaty_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=5)


def _next_build_1_10_20_at_04() -> datetime:
    now = _almaty_now()
    for d in (1, 10, 20):
        try:
            t = now.replace(day=d, hour=4, minute=0, second=0, microsecond=0)
            if t > now:
                return t
        except ValueError:
            pass
    if now.month == 12:
        return now.replace(year=now.year + 1, month=1, day=1, hour=4, minute=0, second=0, microsecond=0)
    first_next = (now.replace(day=1, hour=4, minute=0, second=0, microsecond=0) + timedelta(days=32)).replace(day=1)
    return first_next


def _strip_ns(tag: str) -> str:
    if not tag:
        return tag
    if tag.startswith("{"):
        return tag.rsplit("}", 1)[-1]
    return tag


def _tag_lower(node: ET.Element) -> str:
    return _strip_ns(node.tag).lower()


def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _fix_picture_url(url: str) -> str:
    if not url:
        return ""
    u = (url or "").strip()
    if u.startswith("http://"):
        u = "https://" + u[len("http://"):]
    if " " in u:
        u = u.replace(" ", "%20")
    return u


def _unique_keep_order(xs: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for x in xs:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _parse_number(txt: Optional[str]) -> Optional[float]:
    if not txt:
        return None
    t = (txt or "").strip().replace("\u00A0", "").replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        return float(Decimal(m.group(0)))
    except (InvalidOperation, ValueError):
        return None


def _find_desc_text(elem: ET.Element, names: List[str]) -> Optional[str]:
    wanted = {n.lower() for n in names}
    for node in elem.iter():
        if _tag_lower(node) in wanted:
            t = (node.text or "").strip()
            if t:
                return t
    return None


def _download_bytes() -> bytes:
    if not SUPPLIER_URL:
        raise RuntimeError("NVPRINT_XML_URL пуст")
    if requests is None:
        raise RuntimeError("Модуль requests недоступен (pip install requests)")

    auth = (NV_LOGIN, NV_PASSWORD) if (NV_LOGIN or NV_PASSWORD) else None
    last_err: Optional[Exception] = None

    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(SUPPLIER_URL, timeout=HTTP_TIMEOUT, auth=auth)
            if r.status_code == 401:
                raise RuntimeError("401 Unauthorized: проверь NVPRINT_LOGIN/NVPRINT_PASSWORD (secrets)")
            r.raise_for_status()
            if not r.content:
                raise RuntimeError("Источник вернул пустой ответ")
            return r.content
        except Exception as e:
            last_err = e
            if attempt >= RETRIES or "401" in str(e):
                break
            time.sleep(BACKOFF_S * attempt)

    raise RuntimeError(str(last_err) if last_err else "Не удалось скачать источник")


def _norm_contract(s: str) -> str:
    if not s:
        return ""
    u = s.translate(_LAT_TR).upper()
    u = re.sub(r"[\s\-\_]+", "", u)
    return u


def _extract_base_price(item: ET.Element) -> Optional[float]:
    price_kz: Optional[float] = None
    price_msk: Optional[float] = None

    for node in item.iter():
        if _tag_lower(node) != "договор":
            continue
        num = (node.attrib.get("НомерДоговора") or node.attrib.get("Номердоговора") or "").strip()
        num_n = _norm_contract(num)
        if "000079" not in num_n:
            continue

        price_el = None
        for sub in node.iter():
            if _tag_lower(sub) in ("цена", "price", "amount", "value"):
                price_el = sub
                break

        val = _parse_number(price_el.text if price_el is not None else None)
        if val is None or val <= 0:
            continue

        if "MSK" in num_n or "МСК" in num_n:
            price_msk = val
        else:
            price_kz = val

    return price_kz if (price_kz is not None and price_kz > 0) else (price_msk if (price_msk is not None and price_msk > 0) else None)


def _round_up_tail_900(n: int) -> int:
    k = n // 1000
    cand = k * 1000 + 900
    if cand < n:
        cand = (k + 1) * 1000 + 900
    return cand

def compute_price(base_price: Optional[int]) -> int:
    if base_price is None:
        return 100

    bp = int(base_price)
    if bp < 101:
        return 100
    if bp >= 9_000_000:
        return 100

    for lo, hi, pct, add in PRICING_RULES:
        if lo <= bp <= hi:
            raw = bp * (1.0 + pct / 100.0) + add
            return _round_up_tail_900(int(math.ceil(raw)))

    raw = bp * (1.0 + PRICING_RULES[-1][2] / 100.0) + PRICING_RULES[-1][3]
    return _round_up_tail_900(int(math.ceil(raw)))

def _clean_article(raw: str) -> str:
    s = (raw or "").strip()
    s = re.sub(r"^\s*NV[\-\_\s]+", "", s, flags=re.IGNORECASE)
    s = s.replace(" ", "")
    return s


def make_id(article: str) -> str:
    return "NP" + _clean_article(article)


def name_starts_with_prefixes(name_short: str) -> bool:
    base = _norm_spaces(name_short).lower()
    for kw in KEYWORD_PREFIXES:
        if base.startswith(_norm_spaces(kw).lower()):
            return True
    return False


def detect_type(name_short: str) -> str:
    base = _norm_spaces(name_short).lower()
    for kw in KEYWORD_PREFIXES:
        if base.startswith(_norm_spaces(kw).lower()):
            return kw
    return KEYWORD_PREFIXES[0] if KEYWORD_PREFIXES else "Товар"


def collect_printers(item: ET.Element) -> List[str]:
    out: List[str] = []
    printers_node = None
    for n in item.iter():
        if _tag_lower(n) == "принтеры":
            printers_node = n
            break
    if printers_node is not None:
        for n in printers_node.iter():
            if _tag_lower(n) == "принтер":
                t = (n.text or "").strip()
                if t:
                    out.append(t)

    return _unique_keep_order(out)


_KV_LINE_RE = re.compile(r"(?m)^\s*([A-Za-zА-Яа-яЁё0-9][^:\n]{1,60})\s*[:\-]\s*([^\n]{1,160})\s*$")


def _extract_kv_pairs(text: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    t = (text or "").strip()
    if not t:
        return out

    for m in _KV_LINE_RE.finditer(t):
        k = _norm_spaces(m.group(1)).strip()
        v = _norm_spaces(m.group(2)).strip()
        if not k or not v:
            continue
        lk = k.lower()
        if lk in {"описание", "характеристики", "keywords"}:
            continue
        if len(k) > 64 or len(v) > 180:
            continue
        out.append((k, v))

    return _unique_keep_order(out)


def build_params(item: ET.Element, name_short: str, printers: Optional[List[str]] = None, desc_hint: str = "") -> List[Tuple[str, str]]:
    params: List[Tuple[str, str]] = []
    params.append(("Тип", detect_type(name_short)))

    resurs = _find_desc_text(item, ["Ресурс"])
    if resurs and resurs.strip() and resurs.strip() != "0":
        params.append(("Ресурс", _norm_spaces(resurs)))

    tip = _find_desc_text(item, ["ТипПечати"])
    if tip:
        params.append(("Тип печати", _norm_spaces(tip)))

    color = _find_desc_text(item, ["ЦветПечати"])
    if color:
        params.append(("Цвет печати", _norm_spaces(color)))

    compat = _find_desc_text(item, ["СовместимостьСМоделями"])
    if compat:
        params.append(("Совместимость", _norm_spaces(compat)))

    weight = _find_desc_text(item, ["Вес"])
    if weight:
        params.append(("Вес", _norm_spaces(weight)))

    printers = printers if printers is not None else collect_printers(item)
    if printers:
        params.append(("Принтеры", ", ".join(printers)))
# добор параметров из текстового описания (key: value)
if desc_hint:
    seen = {k.lower() for k, _ in params}
    for k, v in _extract_kv_pairs(desc_hint):
        lk = k.lower()
        if lk in seen:
            continue
        params.append((k, v))
        seen.add(lk)

    return params


def _html_li(k: str, v: str) -> str:
    return f"<li><strong>{html.escape(k)}:</strong> {html.escape(v)}</li>"


def build_description_html(title: str, short_desc: str, params: List[Tuple[str, str]]) -> str:
    title_h = html.escape(title)
    sd = _norm_spaces((short_desc or "").strip())
    if not sd:
        sd = title
    sd_h = html.escape(sd)

    lis = "".join(_html_li(k, v) for k, v in params)
    return (
        "<description><![CDATA[" +
        DESC_PREFIX +
        "<!-- Описание -->\n"
        f"<h3>{title_h}</h3><p>{sd_h}</p>\n"
        "<h3>Характеристики</h3><ul>" + lis + "</ul>\n\n"
        "]]></description>"
    )


def _latinize_like(s: str) -> str:
    return (s or "").translate(_LAT_TR)


def detect_vendor_from_text(article_raw: str, name_short: str, nom_full: str, printers: List[str]) -> str:
    blob = " ".join(x for x in [article_raw, name_short, nom_full, " ".join(printers or [])] if x).strip()
    if not blob:
        return ""

    raw_up = blob.upper()
    lat_up = _latinize_like(blob).upper()

    if "КАТЮША" in raw_up or re.search(r"\bKATYUSHA\b", lat_up):
        return "Катюша"

    if "KX-FAD" in lat_up or "KX-FAT" in lat_up:
        return "Panasonic"

    if "DESIGNJET" in lat_up:
        return "HP"

    brand_words: List[Tuple[str, str]] = [
        ("HP", r"\bHP\b|\bHEWLETT\s*PACKARD\b"),
        ("Canon", r"\bCANON\b"),
        ("Epson", r"\bEPSON\b"),
        ("Brother", r"\bBROTHER\b"),
        ("Xerox", r"\bXEROX\b"),
        ("Samsung", r"\bSAMSUNG\b"),
        ("Kyocera", r"\bKYOCERA\b|\bKYOCERA\s*MITA\b"),
        ("Ricoh", r"\bRICOH\b|\bAFICIO\b"),
        ("Konica Minolta", r"\bKONICA\s*MINOLTA\b|\bMINOLTA\b"),
        ("Oki", r"\bOKI\b"),
        ("Lexmark", r"\bLEXMARK\b"),
        ("Pantum", r"\bPANTUM\b"),
        ("Sharp", r"\bSHARP\b"),
        ("Toshiba", r"\bTOSHIBA\b"),
        ("Dell", r"\bDELL\b"),
        ("Panasonic", r"\bPANASONIC\b"),
        ("Riso", r"\bRISO\b"),
        ("Sindoh", r"\bSINDOH\b"),
    ]
    for vendor, pat in brand_words:
        if re.search(pat, lat_up):
            return vendor

    if re.search(r"\b(C-EXV|NPG|GPR|CRG)\b", lat_up):
        return "Canon"
    if re.search(r"\bC13T\d+\b", lat_up):
        return "Epson"
    if re.search(r"\b(MLT-|CLT-)\w+", lat_up):
        return "Samsung"
    if re.search(r"\b(106R|013R|006R)\w*", lat_up):
        return "Xerox"
    if re.search(r"\b(TK-|DV-|DK-)\w+", lat_up):
        return "Kyocera"
    if re.search(r"\b(TN-|DR-|LC-|BU-)\w+", lat_up):
        return "Brother"
    if re.search(r"\b(Q|CB|CC|CE|CF)\d{2,5}\w*\b", lat_up):
        return "HP"

    return ""


def build_keywords(vendor: str, title: str, vendor_code: str, params: List[Tuple[str, str]]) -> str:
    parts: List[str] = []
    for s in (vendor, title, vendor_code):
        s = _norm_spaces(s)
        if s:
            parts.append(s)

    pmap = {k: v for k, v in params}
    for k in ("Тип", "Совместимость", "Ресурс", "Цвет печати"):
        v = _norm_spaces(pmap.get(k, ""))
        if v:
            parts.append(v)

    uniq = _unique_keep_order(parts)

    uniq.extend(CITIES)
    return ", ".join(uniq)


def parse_item(node: ET.Element) -> Optional[Dict[str, Any]]:
    article = _find_desc_text(node, ["Артикул", "articul", "sku", "article", "PartNumber"])
    if not article:
        return None

    name_short = _find_desc_text(node, ["НоменклатураКратко"])
    if not name_short:
        return None
    name_short = _norm_spaces(name_short)

    if not name_starts_with_prefixes(name_short):
        return None

    base = _extract_base_price(node)
    base_int = 100 if (base is None or base <= 0) else int(math.ceil(base))
    price = compute_price(base_int)

    picture = _norm_spaces(_find_desc_text(node, ["СсылкаНаКартинку", "Картинка", "Изображение", "Фото", "Picture", "Image", "ФотоURL", "PictureURL"]) or "")
    picture = _fix_picture_url(picture)
    nom_full = _norm_spaces(_find_desc_text(node, ["Номенклатура"]) or "")

    printers = collect_printers(node)

    vendor = _norm_spaces(_find_desc_text(node, ["Бренд", "Производитель", "Вендор", "Brand", "Vendor"]) or "")
    hint_up = _latinize_like(" ".join([article, name_short, nom_full, " ".join(printers or [])])).upper()
    if "KX-FAD" in hint_up or "KX-FAT" in hint_up:
        vendor = "Panasonic"

    if not vendor:
        vendor = detect_vendor_from_text(article, name_short, nom_full, printers)

    if not vendor:
        vendor = "NVPrint"

    oid = make_id(article)

    params = build_params(node, name_short, printers, nom_full)
    desc = build_description_html(name_short, nom_full, params)
    keywords = build_keywords(vendor, name_short, oid, params)

    return {
        "id": oid,
        "vendorCode": oid,
        "name": name_short,
        "price": price,
        "picture": picture,
        "vendor": vendor,
        "description": desc,
        "params": params,
        "keywords": keywords,
        "available": True,
    }


def guess_item_nodes(root: ET.Element) -> List[ET.Element]:
    items: List[ET.Element] = []
    seen: set[int] = set()

    for n in root.iter():
        has_art = False
        has_name = False

        for sub in n.iter():
            tl = _tag_lower(sub)
            if not has_art and tl in ("артикул", "articul", "sku", "article", "partnumber") and (sub.text or "").strip():
                has_art = True
            if not has_name and tl == "номенклатуракратко" and (sub.text or "").strip():
                has_name = True
            if has_art and has_name:
                break

        if not (has_art and has_name):
            continue

        key = id(n)
        if key in seen:
            continue

        seen.add(key)
        items.append(n)

    return items


def ensure_unique_offer_ids(offers: List[Dict[str, Any]]) -> None:
    used: set[str] = set()
    counters: Dict[str, int] = {}

    for it in offers:
        base = str(it.get("id") or "")
        if not base:
            continue

        if base not in used:
            used.add(base)
            counters.setdefault(base, 1)
            continue

        n = counters.get(base, 1) + 1
        while True:
            new_id = f"{base}-{n}"
            if new_id not in used:
                break
            n += 1

        counters[base] = n
        old_id = base

        it["id"] = new_id
        it["vendorCode"] = new_id

        kw = it.get("keywords") or ""
        if kw:
            it["keywords"] = re.sub(rf"(?<!\w){re.escape(old_id)}(?!\w)", new_id, kw)

        used.add(new_id)


def render_feed_meta(source_url: str, total: int, written: int, true_cnt: int, false_cnt: int) -> str:
    now_alm = _almaty_now()
    next_alm = _next_build_1_10_20_at_04()

    rows = [
        ("Поставщик", "NVPrint"),
        ("URL поставщика", source_url),
        ("Время сборки (Алматы)", now_alm.strftime("%Y-%m-%d %H:%M:%S")),
        ("Ближайшая сборка (Алматы)", next_alm.strftime("%Y-%m-%d %H:%M:%S")),
        ("Сколько товаров у поставщика до фильтра", str(total)),
        ("Сколько товаров у поставщика после фильтра", str(written)),
        ("Сколько товаров есть в наличии (true)", str(true_cnt)),
        ("Сколько товаров нет в наличии (false)", str(false_cnt)),
    ]
    key_w = max(len(k) for k, _ in rows)

    lines = ["<!--FEED_META"]
    for k, v in rows:
        lines.append(f"{k.ljust(key_w)} | {v}")
    lines.append("-->")
    return "\n".join(lines)


def build_yml(offers: List[Dict[str, Any]], source_url: str, total_before_filter: int) -> str:
    now_alm = _almaty_now()

    out: List[str] = []
    out.append('<?xml version="1.0" encoding="windows-1251"?>')
    out.append(f'<yml_catalog date="{now_alm:%Y-%m-%d %H:%M}">')
    out.append("<shop><offers>")
    out.append("")

    written = len(offers)
    out.append(render_feed_meta(source_url, total_before_filter, written, written, 0))
    out.append("")

    for it in offers:
        out.append(f'<offer id="{it["id"]}" available="true">')
        out.append("<categoryId></categoryId>")
        out.append(f'<vendorCode>{it["vendorCode"]}</vendorCode>')
        out.append(f'<name>{html.escape(it["name"])}</name>')
        out.append(f'<price>{int(it["price"])}</price>')
        if it.get("picture"):
            out.append(f'<picture>{html.escape(it["picture"])}</picture>')
        if it.get("vendor"):
            out.append(f'<vendor>{html.escape(it["vendor"])}</vendor>')
        out.append("<currencyId>KZT</currencyId>")
        out.append(it["description"])
        for k, v in it.get("params") or []:
            out.append(f'<param name="{html.escape(k)}">{html.escape(v)}</param>')
        out.append(f'<keywords>{html.escape(it["keywords"])}</keywords>')
        out.append("</offer>")
        out.append("")

    out.append("</offers>")
    out.append("</shop>")
    out.append("</yml_catalog>")
    return "\n".join(out)


def main() -> int:
    try:
        xml_bytes = _download_bytes()
        root = ET.fromstring(xml_bytes)

        nodes = guess_item_nodes(root)
        total_before_filter = len(nodes)

        offers: List[Dict[str, Any]] = []
        for node in nodes:
            it = parse_item(node)
            if it:
                offers.append(it)

        ensure_unique_offer_ids(offers)
        yml = build_yml(offers, SUPPLIER_URL, total_before_filter)

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        yml = build_yml([], SUPPLIER_URL, 0)

    os.makedirs(os.path.dirname(OUT_FILE) or ".", exist_ok=True)
    with open(OUT_FILE, "w", encoding=OUTPUT_ENCODING, errors="ignore", newline="\n") as f:
        f.write(yml)

    print(f"Wrote: {OUT_FILE} | encoding={OUTPUT_ENCODING}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main() or 0)

