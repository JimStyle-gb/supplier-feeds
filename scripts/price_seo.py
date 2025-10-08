# scripts/price_seo.py
# -*- coding: utf-8 -*-
"""
SEO-обработчик для Satu без XML-парсера (устойчив к "грязным" символам).
Input : docs/price.yml   (cp1251, с FEED_META и <shop><offers>)
Output: docs/price_seo.yml (cp1251)

Для каждого <offer>:
- Достаёт данные регэкспами, санитизирует, ничего не ломая.
- <name> (<=110): Бренд + Модель/Код + — тип + , ключ-параметр.
- <description>: лид (≈160–180) + Преимущества + Характеристики (если были)
  + Совместимость (если была) + FAQ + Сервис/доставка + Аналоги при available=false.
- Порядок тегов: vendorCode, name, price, picture*, vendor, currencyId, available, description.
- Прочие теги оффера сохраняются и добавляются в конце.
- FEED_META/окружение <shop> — без изменений. Между офферами — пустая строка.
"""

from __future__ import annotations
import os, re, io
from typing import List, Dict, Optional

INPUT_PATH  = os.getenv("SEO_INPUT",  "docs/price.yml")
OUTPUT_PATH = os.getenv("SEO_OUTPUT", "docs/price_seo.yml")
ENC         = os.getenv("OUTPUT_ENCODING", "windows-1251")

# ----------------- IO -----------------
def rtext(path: str) -> str:
    with io.open(path, "r", encoding=ENC, errors="replace") as f:
        return f.read()

def wtext(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with io.open(path, "w", encoding=ENC, newline="\n") as f:
        f.write(text)

# ----------------- locate blocks -----------------
SHOP_OPEN_RX   = re.compile(r"<shop\b[^>]*>", re.I)
SHOP_CLOSE_RX  = re.compile(r"</shop>", re.I)
OFFERS_OPEN_RX = re.compile(r"<offers\b[^>]*>", re.I)
OFFERS_CLOSE_RX= re.compile(r"</offers>", re.I)
OFFER_RX       = re.compile(r"<offer\b.*?</offer>", re.I | re.S)

def split_around_shop(xml_text: str) -> tuple[str, str, str]:
    mo = SHOP_OPEN_RX.search(xml_text)
    mc = SHOP_CLOSE_RX.search(xml_text)
    if not (mo and mc and mc.end() > mo.start()):
        raise ValueError("XML does not contain proper <shop>...</shop> block")
    return xml_text[:mo.start()], xml_text[mo.start():mc.end()], xml_text[mc.end():]

def split_around_offers(shop_block: str) -> tuple[str, str, str]:
    mo = OFFERS_OPEN_RX.search(shop_block)
    mc = OFFERS_CLOSE_RX.search(shop_block)
    if not (mo and mc and mc.end() > mo.end()):
        raise ValueError("<offers>...</offers> not found inside <shop>")
    head = shop_block[:mo.end()]            # включая <offers>
    inner = shop_block[mo.end():mc.start()] # между тегами
    tail = shop_block[mc.start():]          # включая </offers> и дальше до </shop>
    return head, inner, tail

# ----------------- sanitizers -----------------
_ALLOWED_ENTITY_RX = re.compile(r"&(?:amp|lt|gt|quot|apos|#\d+|#x[0-9A-Fa-f]+);")
_INVALID_CTRL_RX   = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

# Экранируем "плохие" & (которые не сущности)
def escape_bad_ampersands(s: str) -> str:
    out = []
    i = 0
    L = len(s)
    while i < L:
        ch = s[i]
        if ch == "&":
            m = _ALLOWED_ENTITY_RX.match(s, i)
            if m:
                out.append(m.group(0))
                i = m.end()
                continue
            else:
                out.append("&amp;")
        else:
            out.append(ch)
        i += 1
    return "".join(out)

# Экранируем «сырые» <, которые не открывают теги
RAW_LT_RX = re.compile(r"<(?!(?:[A-Za-z_/?!]|!--|\!\[CDATA\[|!DOCTYPE))")
def escape_raw_lt(s: str) -> str:
    return RAW_LT_RX.sub("&lt;", s)

def sanitize_text(s: str) -> str:
    if not s: return ""
    s = _INVALID_CTRL_RX.sub("", s)
    s = escape_bad_ampersands(s)
    s = escape_raw_lt(s)
    return s

# ----------------- tiny xml-ish getters via regex -----------------
# Вытаскиваем ПЕРВОЕ вхождение <tag>…</tag> (без вложенности), с безопасным DOTALL.
def get_tag_text(block: str, tag: str) -> Optional[str]:
    m = re.search(rf"<{tag}\b[^>]*>(.*?)</{tag}>", block, re.I | re.S)
    if not m: return None
    return m.group(1).strip()

# Берём ВСЕ <picture>…</picture>
def get_pictures(block: str) -> List[str]:
    return [m.strip() for m in re.findall(r"<picture\b[^>]*>(.*?)</picture>", block, re.I | re.S)]

# Вырезаем конкретные теги (для сборки «остатка»)
def remove_known_tags(block: str, known: List[str]) -> str:
    pat = r"|".join([rf"</?{re.escape(t)}\b[^>]*>.*?</{re.escape(t)}>" for t in known])
    if not pat: return block
    return re.sub(pat, "", block, flags=re.I | re.S)

# ----------------- SEO heuristics -----------------
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
    if vendor_code: return vendor_code
    m = ART_RE.search(base_name or "")
    return (m.group(1).upper() if m else "").strip()

def trim_to_limit(s: str, limit: int) -> str:
    s = s.strip()
    if len(s) <= limit: return s
    cut = s[:limit]
    if " " in cut: cut = cut[:cut.rfind(" ")]
    return cut.rstrip(" .,/;:-") + "…"

def make_seo_name(brand: str, model: str, kind: str, base_name: str) -> str:
    key_param = ""
    m = re.search(r"(\d{3,4}\s*(ВА|Вт)|\d{3,5}\s*стр|A\d{1,2}|Letter|16:9|4:3)", base_name, re.I)
    if m: key_param = m.group(0)
    parts = [brand, model]
    if kind and kind.lower() not in model.lower(): parts.append(f"— {kind}")
    if key_param: parts.append(f", {key_param}")
    candidate = " ".join(parts).replace("  ", " ").strip(" ,")
    return trim_to_limit(candidate, 110)

def first_sentence(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    m = re.search(r"([^.?!]{30,180}[.?!])", s)
    if m: return m.group(1).strip()
    return s[:180].strip()

def parse_specs_from_description(desc: str) -> Dict[str,str]:
    specs: Dict[str,str] = {}
    if not desc: return specs
    for ln in desc.splitlines():
        ln = ln.strip()
        m = re.match(r"^[—\-•]\s*([^:]+?)\s*:\s*(.+)$", ln)
        if m:
            k = re.sub(r"\s+", " ", m.group(1)).strip()
            v = re.sub(r"\s+", " ", m.group(2)).strip()
            if len(k) <= 40 and len(v) <= 200: specs[k] = v
    return specs

def build_lead(brand: str, model: str, kind: str, base_desc: str, price: str) -> str:
    core = f"{kind.capitalize()} {brand} {model} — надёжное решение для повседневных задач."
    if price: core += f" Цена: {price} ₸."
    tail = first_sentence(base_desc)
    return trim_to_limit((core + " " + tail).strip(), 180)

def build_bullets(kind: str, specs: Dict[str,str]) -> List[str]:
    bullets = ["Надёжная работа и стабильное качество печати/питания."]
    if kind == "картридж":
        if "Ресурс" in specs: bullets.append(f"Ресурс: {specs['Ресурс']}.")
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
            if k in specs: out.append(f"— {k}: {specs[k]}")
        for k,v in specs.items():
            if f"— {k}:" not in out and len(out) < 12: out.append(f"— {k}: {v}")
    else:
        maybe_power = re.search(r"(\d{3,4})\s*(ВА|Вт)", name, re.I)
        if maybe_power: out.append(f"— Мощность: {maybe_power.group(1)} {maybe_power.group(2)}")
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

# ----------------- offer processing (regex-based) -----------------
# Вырезаем открывающий тег <offer ...> и закрывающий </offer>, внутрянку возвращаем отдельно
OFFER_HEADER_RX = re.compile(r"^(\s*)<offer\b([^>]*)>(.*)</offer>\s*$", re.I | re.S)

def parse_offer(off_txt: str) -> dict:
    """
    Возвращает словарь полей + 'rest' (неизвестные теги сырым XML).
    Поля: vendorCode, name, price, pictures(list), vendor, currencyId, available, description
    """
    m = OFFER_HEADER_RX.match(off_txt)
    if not m:
        # fallback: считаем без заголовка (редко)
        inner = off_txt
        prefix = ""
        attrs = ""
    else:
        prefix, attrs, inner = m.group(1), m.group(2), m.group(3)

    # базовые поля
    vendorCode  = get_tag_text(inner, "vendorCode")  or ""
    name        = get_tag_text(inner, "name")        or ""
    price       = get_tag_text(inner, "price")       or ""
    vendor      = get_tag_text(inner, "vendor")      or ""
    currencyId  = get_tag_text(inner, "currencyId")  or ""
    available   = (get_tag_text(inner, "available")  or "").strip().lower()
    description = get_tag_text(inner, "description") or ""
    pictures    = get_pictures(inner)

    # «остальные» теги (сохраним как есть)
    known = ["vendorCode","name","price","picture","vendor","currencyId","available","description"]
    rest = remove_known_tags(inner, known).strip()

    return {
        "_prefix": prefix, "_attrs": attrs,
        "vendorCode": vendorCode, "name": name, "price": price,
        "pictures": pictures, "vendor": vendor, "currencyId": currencyId,
        "available": available if available in {"true","false"} else "",
        "description": description, "rest": rest
    }

def render_offer(data: dict) -> str:
    """
    Собираем оффер в нужном порядке, сохраняя id/атрибуты и добавляя rest в конце.
    Все тексты проходят мягкую санитацию.
    """
    px   = data.get("_prefix","")
    attrs= data.get("_attrs","")
    vc   = sanitize_text(data.get("vendorCode",""))
    nm   = sanitize_text(data.get("name",""))
    pr   = sanitize_text(data.get("price",""))
    ven  = sanitize_text(data.get("vendor",""))
    cur  = sanitize_text(data.get("currencyId",""))
    av   = (data.get("available","") or "").strip().lower()
    av   = "true" if av=="true" else ("false" if av=="false" else "")
    desc = sanitize_text(data.get("description",""))
    pics = [sanitize_text(p) for p in (data.get("pictures") or [])]
    rest = data.get("rest","").strip()

    # собираем
    out = []
    out.append(f"{px}<offer{attrs}>")
    # порядок
    if vc:  out.append(f"{px}  <vendorCode>{vc}</vendorCode>")
    if nm:  out.append(f"{px}  <name>{nm}</name>")
    if pr:  out.append(f"{px}  <price>{pr}</price>")
    for p in pics:
        out.append(f"{px}  <picture>{p}</picture>")
    if ven: out.append(f"{px}  <vendor>{ven}</vendor>")
    if cur: out.append(f"{px}  <currencyId>{cur}</currencyId>")
    if av:  out.append(f"{px}  <available>{av}</available>")
    if desc:out.append(f"{px}  <description>{desc}</description>")

    if rest:
        out.append("\n".join(line if line.startswith(px) else (px + "  " + line) for line in rest.splitlines()))

    out.append(f"{px}</offer>")
    return "\n".join(out)

# ----------------- SEO build -----------------
def enhance_one_offer(off_txt: str, all_vendor_codes: List[str]) -> str:
    d = parse_offer(off_txt)

    # исходные поля
    vc   = d["vendorCode"]
    base = d["name"]
    pr   = d["price"]
    desc = d["description"]
    brand_raw = d["vendor"]
    brand = normalize_vendor(brand_raw) or "NoName"
    d["vendor"] = brand

    # эвристики
    specs = parse_specs_from_description(desc)
    kind  = detect_type_tokens(base, desc)
    model = extract_model(base, vc) or vc
    compat = extract_compatibility(desc) or extract_compatibility(base)

    # аналоги (простая эвристика по префиксу и наличию в all_vendor_codes)
    analogs: List[str] = []
    if (d.get("available") or "").lower() == "false" and vc:
        pref = vc[:3]
        for other in all_vendor_codes:
            if other == vc: continue
            if other.startswith(pref):
                analogs.append(other)
            if len(analogs) >= 3: break

    # SEO name
    seo_name = make_seo_name(brand, model, kind, base)
    d["name"] = seo_name

    # описание
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
    if (d.get("available") or "").lower() == "false" and analogs:
        lines.append("")
        lines.append("Товар временно отсутствует. Рекомендуем аналоги:")
        for a in analogs: lines.append(f"— {a}")

    d["description"] = "\n".join(lines).strip()

    return render_offer(d)

# ----------------- main -----------------
def main() -> int:
    full = rtext(INPUT_PATH)

    # 1) head / <shop>…</shop> / tail
    head, shop_block, tail = split_around_shop(full)
    # 2) <offers>…</offers> внутри shop
    shop_head, offers_inner, shop_tail = split_around_offers(shop_block)
    # 3) все офферы как текст
    raw_offers = OFFER_RX.findall(offers_inner)
    if not raw_offers:
        # ничего не делаем
        wtext(OUTPUT_PATH, full)
        print("[seo] No offers found, copied input to output.")
        return 0

    # список всех vendorCode (для подбора «аналогов»)
    all_vcs = []
    for off in raw_offers:
        vc = get_tag_text(off, "vendorCode")
        if vc: all_vcs.append(vc.strip())

    processed: List[str] = []
    for off in raw_offers:
        processed.append(enhance_one_offer(off, all_vcs))

    # 4) склейка обратно: ровно одна пустая строка между офферами
    # найдём отступ перед первым <offer> (обычно 6 пробелов)
    indent_m = re.search(r"\n([ \t]*)<offer\b", offers_inner)
    base_indent = indent_m.group(1) if indent_m else "      "
    new_inner = ("\n\n".join(base_indent + p.replace("\n", "\n"+base_indent) for p in processed)).rstrip()

    if new_inner:
        new_inner = "\n" + new_inner + "\n" + (re.search(r"\n([ \t]*)</offers", shop_tail).group(1) if re.search(r"\n([ \t]*)</offers", shop_tail) else "    ")

    out = head + shop_head + new_inner + shop_tail + tail
    wtext(OUTPUT_PATH, out)
    print(f"[seo] Wrote {OUTPUT_PATH} | offers={len(processed)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
