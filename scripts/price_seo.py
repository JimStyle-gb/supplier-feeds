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
- FEED_META-комментарии из price.yml сохраняются как есть.
- Между офферами — пустая строка.

Запуск:
  python scripts/price_seo.py
(можно в CI после build_price.py)
"""

from __future__ import annotations
import os, re, io
from typing import List, Dict, Optional
from xml.etree import ElementTree as ET

INPUT_PATH  = os.getenv("SEO_INPUT",  "docs/price.yml")
OUTPUT_PATH = os.getenv("SEO_OUTPUT", "docs/price_seo.yml")
ENC         = os.getenv("OUTPUT_ENCODING", "windows-1251")

# ---------- low-level IO ----------
def rtext(path: str) -> str:
    with io.open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def wtext(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with io.open(path, "w", encoding=ENC, newline="\n") as f:
        f.write(text)

# ---------- safe slice of <shop>…</shop> to preserve FEED_META ----------
SHOP_OPEN_RX = re.compile(r"<shop\b[^>]*>", re.I)
SHOP_CLOSE_RX = re.compile(r"</shop>", re.I)

def split_around_shop(xml_text: str) -> tuple[str, str, str]:
    """
    Возвращает (head, shop_block, tail), где head включает шапку и FEED_META,
    shop_block — содержимое от <shop> до </shop> (включая их),
    tail — всё после </shop>. Если не нашли — кидаем ValueError.
    """
    m_open = SHOP_OPEN_RX.search(xml_text)
    m_close = SHOP_CLOSE_RX.search(xml_text)
    if not (m_open and m_close and m_close.end() > m_open.start()):
        raise ValueError("XML does not contain proper <shop>...</shop> block")
    head = xml_text[:m_open.start()]
    shop_block = xml_text[m_open.start():m_close.end()]
    tail = xml_text[m_close.end():]
    return head, shop_block, tail

# ---------- small XML helpers ----------
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

    # подберём простые "аналоги" из того же бренда с похожим префиксом кода
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

    # SEO name и description
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

    # Порядок тегов
    reorder_offer_children(off)

# ---------- main ----------
def main() -> int:
    xml_in = rtext(INPUT_PATH)

    # сохраним все FEED_META и прочие комментарии/шапку как есть
    head, shop_block, tail = split_around_shop(xml_in)

    # вытащим только <offers> из shop_block и обработаем через XML
    try:
        shop_root = ET.fromstring(shop_block.encode(ENC, errors="replace"))
    except Exception as e:
        print(f"[seo] XML parse error in <shop> block: {e}")
        return 2

    offers_el = shop_root.find("offers") or shop_root.find("Offers")
    if offers_el is None:
        print("[seo] No <offers> inside <shop>")
        return 3

    offers = list(offers_el.findall("offer"))
    # пробегаемся по всем офферам
    for off in offers:
        enhance_offer(off, offers)

    # pretty print внутри shop
    try:
        ET.indent(shop_root, space="  ")
    except Exception:
        pass

    # сериализуем обратно только <shop>…</shop>
    shop_txt = ET.tostring(shop_root, encoding=ENC, method="xml").decode(ENC, errors="replace")

    # Гарантируем пустую строку между офферами
    shop_txt = re.sub(r"\s*</offer>\s*<offer\b", "</offer>\n\n  <offer", shop_txt, flags=re.S)

    # Собираем финальный документ: head + shop + tail (FEED_META остаются нетронутыми)
    out_txt = head + shop_txt + tail

    wtext(OUTPUT_PATH, out_txt)
    print(f"[seo] Wrote {OUTPUT_PATH}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
