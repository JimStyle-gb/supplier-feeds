# scripts/price_seo.py
# -*- coding: utf-8 -*-
"""
SEO-обработчик для Satu.
Input : docs/price.yml   (cp1251, с FEED_META и <shop><offers>)
Output: docs/price_seo.yml (cp1251)

Что делает на каждый <offer>:
- Нормализует <vendor> (бренд).
- <name> (<=110): Бренд + Модель/Код + — тип + , ключ-параметр.
- <description>: лид (≈160–180) + Преимущества (буллеты) + Характеристики (если были)
  + Совместимость (если была) + FAQ + Сервис/доставка + Аналоги при available=false.
- Порядок тегов -> vendorCode, name, price, picture*, vendor, currencyId, available, description.
- FEED_META и окружение вокруг <shop> — не трогаем.
- FIX: Работаем только внутри <offers>…</offers> текстом; каждый <offer> санитизируем и парсим отдельно.
"""

from __future__ import annotations
import os, re, io
from typing import List, Dict, Optional
from xml.etree import ElementTree as ET

INPUT_PATH  = os.getenv("SEO_INPUT",  "docs/price.yml")
OUTPUT_PATH = os.getenv("SEO_OUTPUT", "docs/price_seo.yml")
ENC         = os.getenv("OUTPUT_ENCODING", "windows-1251")

# ---------- IO ----------
def rtext(path: str) -> str:
    with io.open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def wtext(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with io.open(path, "w", encoding=ENC, newline="\n") as f:
        f.write(text)

# ---------- locate blocks ----------
SHOP_OPEN_RX   = re.compile(r"<shop\b[^>]*>", re.I)
SHOP_CLOSE_RX  = re.compile(r"</shop>", re.I)
OFFERS_OPEN_RX = re.compile(r"<offers\b[^>]*>", re.I)
OFFERS_CLOSE_RX= re.compile(r"</offers>", re.I)
OFFER_RX       = re.compile(r"<offer\b.*?</offer>", re.I | re.S)

def split_around_shop(xml_text: str) -> tuple[str, str, str]:
    m_open = SHOP_OPEN_RX.search(xml_text)
    m_close = SHOP_CLOSE_RX.search(xml_text)
    if not (m_open and m_close and m_close.end() > m_open.start()):
        raise ValueError("XML does not contain proper <shop>...</shop> block")
    return xml_text[:m_open.start()], xml_text[m_open.start():m_close.end()], xml_text[m_close.end():]

def split_around_offers(shop_block: str) -> tuple[str, str, str]:
    mo = OFFERS_OPEN_RX.search(shop_block)
    mc = OFFERS_CLOSE_RX.search(shop_block)
    if not (mo and mc and mc.end() > mo.end()):
        raise ValueError("<offers>...</offers> not found inside <shop>")
    head = shop_block[:mo.end()]           # включая <offers>
    inner = shop_block[mo.end():mc.start()]# между тегами
    tail = shop_block[mc.start():]         # включая </offers> и всё дальше до </shop>
    return head, inner, tail

# ---------- sanitize one <offer> ----------
_ALLOWED_ENTITY_RX = re.compile(r"&(?:amp|lt|gt|quot|apos|#\d+|#x[0-9A-Fa-f]+);")
_INVALID_CTRL_RX   = re.compile(r"([\x00-\x08\x0B\x0C\x0E-\x1F])")

def escape_bad_ampersands(s: str) -> str:
    out = []
    i = 0
    L = len(s)
    while i < L:
        if s[i] == "&":
            m = _ALLOWED_ENTITY_RX.match(s, i)
            if m:
                out.append(m.group(0))
                i = m.end()
                continue
            else:
                out.append("&amp;")
        else:
            out.append(s[i])
        i += 1
    return "".join(out)

# Экранируем «сырые» знаки '<', которые НЕ начинают тег или комментарий/CDATA/doctype.
RAW_LT_RX = re.compile(r"<(?!(?:[A-Za-z_/?!]|!--|\!\[CDATA\[|!DOCTYPE))")

def sanitize_offer_xml_text(offer_xml: str) -> str:
    s = offer_xml
    s = _INVALID_CTRL_RX.sub("", s)
    s = escape_bad_ampersands(s)
    s = RAW_LT_RX.sub("&lt;", s)
    return s

# ---------- XML helpers ----------
def get_text(el: ET.Element, tag: str) -> str:
    n = el.find(tag)
    return (n.text or "").strip() if (n is not None and n.text) else ""

def set_text(el: ET.Element, tag: str, value: str) -> ET.Element:
    n = el.find(tag)
    if n is None:
        n = ET.SubElement(el, tag)
    n.text = value
    return n

# ---------- SEO heuristics ----------
ART_RE = re.compile(r"\b([A-Z0-9]{2,}[A-Z0-9\-]{1,})\b", re.I)

def normalize_vendor(v: str) -> str:
    raw = (v or "").strip()
    key = re.sub(r"[\W_]+", "", raw, flags=re.I).lower()
    aliases = {
        "viewsonicproj": "ViewSonic",
        "epsonproj": "Epson",
        "kitay": "NoName", "китай": "NoName", "noname": "NoName",
        "svc": "SVC",
        "europrint": "Europrint", "katun": "Katun", "deluxe": "Deluxe",
        "ship": "Ship", "ap": "A&P", "aandp": "A&P",
    }
    return aliases.get(key, raw or "NoName")

def detect_type_tokens(name: str, description: str) -> str:
    t = (name + " " + description).lower()
    if any(w in t for w in ["картридж", "cartridge", "тонер", "совместим"]): return "картридж"
    if any(w in t for w in ["ибп", "ups", "источник бесперебойного"]):        return "ИБП"
    if any(w in t for w in ["экран", "проекц", "проекторный экран"]):          return "экран"
    if any(w in t for w in ["кабель", "патч", "utp", "ftp", "hdmi", "vga"]):   return "кабель"
    return "товар"

def extract_model(base_name: str, vendor_code: str) -> str:
    if vendor_code:
        return vendor_code
    m = ART_RE.search(base_name or "")
    return (m.group(1).upper() if m else "").strip()

def trim_to_limit(s: str, limit: int) -> str:
    s = s.strip()
    if len(s) <= limit:
        return s
    cut = s[:limit]
    if " " in cut:
        cut = cut[:cut.rfind(" ")]
    return cut.rstrip(" .,/;:-") + "…"

def make_seo_name(brand: str, model: str, kind: str, base_name: str) -> str:
    key_param = ""
    m = re.search(r"(\d{3,4}\s*(ВА|Вт)|\d{3,5}\s*стр|A\d{1,2}|Letter|16:9|4:3)", base_name, re.I)
    if m:
        key_param = m.group(0)
    parts = [brand, model]
    if kind and kind.lower() not in model.lower():
        parts.append(f"— {kind}")
    if key_param:
        parts.append(f", {key_param}")
    candidate = " ".join(parts).replace("  ", " ").strip(" ,")
    return trim_to_limit(candidate, 110)

def first_sentence(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    m = re.search(r"([^.?!]{30,180}[.?!])", s)
    if m:
        return m.group(1).strip()
    return s[:180].strip()

def parse_specs_from_description(desc: str) -> Dict[str,str]:
    specs: Dict[str,str] = {}
    if not desc:
        return specs
    for ln in desc.splitlines():
        ln = ln.strip()
        m = re.match(r"^[—\-•]\s*([^:]+?)\s*:\s*(.+)$", ln)
        if m:
            k = re.sub(r"\s+", " ", m.group(1)).strip()
            v = re.sub(r"\s+", " ", m.group(2)).strip()
            if len(k) <= 40 and len(v) <= 200:
                specs[k] = v
    return specs

def build_lead(brand: str, model: str, kind: str, base_desc: str, price: str) -> str:
    core = f"{kind.capitalize()} {brand} {model} — надёжное решение для повседневных задач."
    if price:
        core += f" Цена: {price} ₸."
    tail = first_sentence(base_desc)
    return trim_to_limit((core + " " + tail).strip(), 180)

def build_bullets(kind: str, specs: Dict[str,str]) -> List[str]:
    bullets = ["Надёжная работа и стабильное качество печати/питания."]
    if kind == "картридж":
        if "Ресурс" in specs:
            bullets.append(f"Ресурс: {specs['Ресурс']}.")
        bullets += ["Совместимость с популярными моделями — см. ниже.", "Простая установка, чистая печать без полос."]
    elif kind == "ИБП":
        bullets += ["Защита от перепадов напряжения и кратковременных отключений.", "Оптимален для ПК/сетевого оборудования."]
    elif kind == "экран":
        bullets += ["Ровное матовое полотно для чёткой картинки без бликов.", "Подходит для офиса, обучения и домашнего кинотеатра."]
    elif kind == "кабель":
        bullets += ["Стабильная передача сигнала, стойкая изоляция.", "Простая укладка и долговечность в эксплуатации."]
    else:
        bullets.append("Оптимальный баланс цены и качества.")
    bullets.append("Гарантия и сервисная поддержка в Казахстане.")
    return bullets[:8]

def build_specs_block(kind: str, specs: Dict[str,str], name: str) -> List[str]:
    out: List[str] = []
    keys_priority = ["Тип","Мощность","Ресурс","Формат","Интерфейсы","Совместимость","Цвет","Габариты","Вес"]
    if specs:
        for k in keys_priority:
            if k in specs:
                out.append(f"— {k}: {specs[k]}")
        for k,v in specs.items():
            if f"— {k}:" not in out and len(out) < 12:
                out.append(f"— {k}: {v}")
    else:
        maybe_power = re.search(r"(\d{3,4})\s*(ВА|Вт)", name, re.I)
        if maybe_power:
            out.append(f"— Мощность: {maybe_power.group(1)} {maybe_power.group(2)}")
    return out[:12]

def extract_compatibility(text: str) -> Optional[str]:
    m = re.search(r"(Подходит к|Совместимость)\s*:\s*(.+)", text, flags=re.I)
    return m.group(2).strip() if m else None

def build_faq(kind: str) -> List[str]:
    if kind == "картридж":
        return [
            "— Подойдёт ли к моей модели принтера?\nДа, смотрите список совместимости ниже либо уточните по модели.",
            "— Какой ресурс картриджа?\nРесурс указан при 5% заполнении листа (стандарт ISO).",
            "— Есть ли гарантия?\nДа, гарантия распространяется при нормальной эксплуатации и сохранности пломб.",
        ]
    if kind == "ИБП":
        return [
            "— На сколько хватит автономности?\nЗависит от нагрузки. Для ПК 300–500Вт обычно это 5–15 минут.",
            "— Нужно ли обслуживать батареи?\nРекомендуется тест раз в 6–12 месяцев, замена по износу.",
        ]
    if kind == "экран":
        return [
            "— Какой формат выбрать?\n16:9 для фильмов/презентаций, 1:1 или 4:3 для универсальных задач.",
            "— Сложен ли монтаж?\nКрепление стандартное, комплектуется инструкцией.",
        ]
    return [
        "— Есть доставка по Казахстану?\nДа, быстрая доставка по РК и самовывоз в Алматы.",
        "— Можно ли вернуть товар?\nДа, согласно законодательству при сохранности вида/комплектации.",
    ]

def build_service_block() -> str:
    return ("Доставка по Казахстану, самовывоз в Алматы. Гарантия поставщика. "
            "Поможем с подбором аналога/замены под вашу модель.")

def reorder_offer_children(offer: ET.Element) -> None:
    order = ["vendorCode","name","price","picture","vendor","currencyId","available","description"]
    children = list(offer)
    buckets: Dict[str, List[ET.Element]] = {k: [] for k in order}
    rest: List[ET.Element] = []
    for ch in children:
        if ch.tag == "picture":
            buckets["picture"].append(ch)
        elif ch.tag in buckets and not buckets[ch.tag]:
            buckets[ch.tag].append(ch)
        else:
            rest.append(ch)
    for ch in children:
        offer.remove(ch)
    for key in ["vendorCode","name","price"]:
        if buckets[key]:
            offer.append(buckets[key][0])
    for p in buckets["picture"]:
        offer.append(p)
    for key in ["vendor","currencyId","available","description"]:
        if buckets[key]:
            offer.append(buckets[key][0])
    for x in rest:
        offer.append(x)

def enhance_offer(off: ET.Element, all_offers: List[ET.Element]) -> None:
    vc   = get_text(off, "vendorCode")
    base = get_text(off, "name")
    pr   = get_text(off, "price")
    desc = get_text(off, "description")
    brand_raw = get_text(off, "vendor")
    brand = normalize_vendor(brand_raw)
    set_text(off, "vendor", brand)

    specs = parse_specs_from_description(desc)
    kind  = detect_type_tokens(base, desc)
    model = extract_model(base, vc) or vc
    compat = extract_compatibility(desc) or extract_compatibility(base)
    available = (get_text(off, "available") or "true").lower()

    analogs: List[str] = []
    if available == "false":
        vc_pref = vc[:3]
        for candidate in all_offers:
            if candidate is off: continue
            c_vc = get_text(candidate, "vendorCode")
            c_br = normalize_vendor(get_text(candidate, "vendor"))
            if brand and c_br and c_br.lower() != brand.lower(): continue
            if c_vc.startswith(vc_pref):
                nm = get_text(candidate, "name")
                if nm: analogs.append(nm)
            if len(analogs) >= 3: break

    seo_name = make_seo_name(brand, model, kind, base)
    set_text(off, "name", seo_name)

    lead = build_lead(brand, model, kind, desc, pr)
    bullets = build_bullets(kind, specs)
    specs_block = build_specs_block(kind, specs, base)
    faq = build_faq(kind)
    service = build_service_block()

    lines: List[str] = []
    lines.append(lead)
    lines.append("")
    lines.append("Преимущества:")
    for b in bullets: lines.append(f"— {b}")
    if specs_block:
        lines.append("")
        lines.append("Характеристики:")
        lines.extend(specs_block)
    if compat:
        lines.append("")
        lines.append(f"Совместимость: {compat}")
    if faq:
        lines.append("")
        lines.append("FAQ:")
        lines.extend(faq)
    lines.append("")
    lines.append(service)
    if available == "false" and analogs:
        lines.append("")
        lines.append("Товар временно отсутствует. Рекомендуем аналоги:")
        for a in analogs:
            lines.append(f"— {a}")

    set_text(off, "description", "\n".join(lines).strip())
    reorder_offer_children(off)

# ---------- main ----------
def main() -> int:
    full = rtext(INPUT_PATH)

    # 1) отделяем <shop> и всё вокруг, чтобы не ломать FEED_META
    head, shop_block, tail = split_around_shop(full)

    # 2) отделяем <offers>…</offers> внутри shop
    shop_head, offers_inner, shop_tail = split_around_offers(shop_block)

    # 3) вытаскиваем офферы текстом
    raw_offers = OFFER_RX.findall(offers_inner)

    processed_offers_txt: List[str] = []
    for off_txt in raw_offers:
        safe_txt = sanitize_offer_xml_text(off_txt)
        # парсим отдельный оффер
        try:
            off_el = ET.fromstring(safe_txt.encode(ENC, errors="replace"))
        except Exception as e:
            # если вдруг всё ещё не парсится — экранируем любые остаточные '<'
            safe_txt2 = re.sub(r"<", "&lt;", safe_txt)
            off_el = ET.fromstring(safe_txt2.encode(ENC, errors="replace"))
        # собираем список для аналогов (минимально — сам оффер)
        enhance_offer(off_el, [off_el])
        # сериализация без декларации
        try:
            ET.indent(off_el, space="  ")
        except Exception:
            pass
        rendered = ET.tostring(off_el, encoding=ENC, method="xml").decode(ENC, errors="replace").strip()
        processed_offers_txt.append(rendered)

    # 4) склеиваем обратно внутренности <offers>:
    #    между </offer> и <offer> — пустая строка; с тем же базовым отступом, что был в файле (обычно 4 пробела до <offer>)
    #    возьмём отступ из shop_head (последняя строка)
    m_indent = re.search(r"\n([ \t]*)$", shop_head)
    base_indent = (m_indent.group(1) if m_indent else "    ")
    offer_indent = base_indent + "  "  # обычно было 6 пробелов перед <offer>

    inner_new = ("\n\n".join(offer_indent + o.replace("\n", "\n"+offer_indent) for o in processed_offers_txt)).rstrip()
    if inner_new:
        inner_new = "\n" + inner_new + "\n" + base_indent  # обрамление как в исходном

    # 5) собираем итог: head + shop_head + inner_new + shop_tail + tail
    out = head + shop_head + inner_new + shop_tail + tail
    wtext(OUTPUT_PATH, out)
    print(f"[seo] Wrote {OUTPUT_PATH} | offers={len(processed_offers_txt)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
