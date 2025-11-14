#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build_AlStyle feed (baseline + compact description + spacing + WhatsApp + dynamic FAQ/Reviews + FEED_META)
Ready-to-run, single-file implementation.

Outputs: docs/alstyle.yml (Windows-1251)
"""

from textwrap import dedent
from pathlib import Path
import re, os, sys, html, datetime, requests

SCRIPT_NAME = "build_alstyle.py"
OUT_FILE = "docs/alstyle.yml"
SUPPLIER_URL = os.getenv("ALSTYLE_URL", "https://al-style.kz/upload/catalog_export/al_style_catalog.php")
LOCAL_SOURCE = os.getenv("ALSTYLE_LOCAL_SOURCE", "docs/alstyle_source.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

# Filter by supplier categories (include-mode)
CATEGORY_FILTER = {
    3540, 3541, 3542, 3543, 3544, 3545, 3566, 3567, 3569, 3570, 3580, 3688, 3708,
    3721, 3722, 4889, 4890, 4895, 5017, 5075, 5649, 5710, 5711, 5712, 5713, 21279,
    21281, 21291, 21356, 21367, 21368, 21369, 21370, 21371, 21372, 21451, 21498,
    21500, 21501, 21572, 21573, 21574, 21575, 21576, 21578, 21580, 21581, 21583,
    21584, 21585, 21586, 21588, 21591, 21640, 21664, 21665, 21666, 21698
}

DROP_PARAMS = {
    "Артикул", "Штрихкод", "Штрих-код", "Снижена цена", "Благотворительность",
    "Назначение", "Код ТН ВЭД", "Объём", "Объем", "Код товара Kaspi", "Новинка"
}

WHATSAPP_BLOCK = dedent("""\
<div style="font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;">
  <p style="text-align:center; margin:0 0 12px;">
    <a href="https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0"
       style="display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,.08);">
      &#128172; НАЖМИТЕ, ЧТОБЫ НАПИСАТЬ НАМ В WHATSAPP!
    </a>
  </p>

  <div style="background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;">
    <h3 style="margin:0 0 8px; font-size:17px;">Оплата</h3>
    <ul style="margin:0; padding-left:18px;">
      <li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li>
      <li><strong>Удалённая оплата</strong> по <span style="color:#8b0000;"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li>
    </ul>

    <hr style="border:none; border-top:1px solid #E7D6B7; margin:12px 0;">

    <h3 style="margin:0 0 8px; font-size:17px;">Доставка по Алматы и Казахстану</h3>
    <ul style="margin:0; padding-left:18px;">
      <li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li>
      <li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5000 тг. | 3–7 рабочих дней</em></li>
      <li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>
      <li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li>
    </ul>
  </div>
</div>
""").strip()

_re_offer = re.compile(r"(?is)<offer\b.*?</offer>")
_re_param = re.compile(r'(?is)<param\b[^>]*name="([^"]+)"[^>]*>(.*?)</param>')

def _fetch_source() -> str:
    p = Path(LOCAL_SOURCE)
    if p.exists():
        return p.read_text(encoding="utf-8", errors="ignore")
    r = requests.get(SUPPLIER_URL, timeout=40)
    r.raise_for_status()
    text = r.content.decode("utf-8", errors="ignore")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return text

def _price_adder(base: int) -> int:
    if 101 <= base <= 10_000: return 3_000
    elif 10_001 <= base <= 25_000: return 4_000
    elif 25_001 <= base <= 50_000: return 5_000
    elif 50_001 <= base <= 75_000: return 7_000
    elif 75_001 <= base <= 100_000: return 10_000
    elif 100_001 <= base <= 150_000: return 12_000
    elif 150_001 <= base <= 200_000: return 15_000
    elif 200_001 <= base <= 300_000: return 20_000
    elif 300_001 <= base <= 400_000: return 25_000
    elif 400_001 <= base <= 500_000: return 30_000
    elif 500_001 <= base <= 750_000: return 40_000
    elif 750_001 <= base <= 1_000_000: return 50_000
    elif 1_000_001 <= base <= 1_500_000: return 70_000
    elif 1_500_001 <= base <= 2_000_000: return 90_000
    elif base >= 2_000_001: return 100_000
    else: return 0

def _price_tail_900(n: int) -> int:
    if n >= 9_000_000:
        return 100
    thousands = n // 1000
    remainder = n % 1000
    if remainder > 900:
        thousands += 1
        remainder = 0
    return thousands * 1000 + 900

def _calc_retail_from_purchase(purchase: int) -> int:
    if purchase <= 0: return 0
    with_percent = int((purchase * 104 + 99) // 100)
    retail = with_percent + _price_adder(purchase)
    return _price_tail_900(retail)

def _normalize_text(t: str) -> str:
    t = t.replace("\r", "")
    t = re.sub(r"[ \t]+\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    t = t.replace("&nbsp;", " ")
    return t.strip()

def _shorten_native(text: str, limit: int = 1000) -> str:
    plain = re.sub(r"<[^>]+>", "", text)
    if len(plain) <= limit: return text.strip()
    upto = plain[:limit+200]
    m = re.search(r"[.!?]\s", upto[::-1])
    if m:
        cut = len(upto) - m.start()
        plain_cut = plain[:cut].strip()
    else:
        plain_cut = plain[:limit].strip()
    return html.escape(plain_cut)

def _priority_key(name: str) -> int:
    order = [
        "Вес", "Гарантия", "Диагональ", "Диагональ экрана", "Мощность",
        "Ёмкость", "Емкость", "Операционная система", "Процессор", "Память",
        "Цвет", "Комплектация"
    ]
    try: return order.index(name)
    except ValueError: return len(order)

def _compact_description_html(desc_html: str) -> str:
    x = desc_html
    x = re.sub(r">\s+<", "><", x)
    x = re.sub(r"[ \t]{2,}", " ", x)
    x = re.sub(r"\n{3,}", "\n\n", x)
    x = re.sub(r"(</ul>)(<h3>)", r"\\1\n\\2", x)
    x = re.sub(r"(</div>)(<h3>)", r"\\1\n\\2", x)
    return x

def _build_description(name: str, native_html: str, params: list) -> str:
    native_clean = _normalize_text(native_html)
    native_short = _shorten_native(native_clean, 1000)
    items = []
    for k, v in params:
        if not k or not v: continue
        items.append((k.strip(), v.strip()))
    items.sort(key=lambda kv: (_priority_key(kv[0]), kv[0].lower()))
    li = "".join(f"<li><strong>{html.escape(k)}:</strong> {html.escape(v)}</li>" for k, v in items)
    specs_block = f"<h3>Характеристики</h3><ul>{li}</ul>" if li else ""
    head = f"<h3>{html.escape(name)}</h3>"
    body = WHATSAPP_BLOCK + "\\n\\n" + head + f"<p>{native_short}</p>" + specs_block
    body = _compact_description_html(body)
    return f"<description>{body}</description>"

def _smart_faq_reviews(name: str, params: list, native_plain: str) -> str:
    n = name.lower()
    is_cartridge = bool(re.search(r"(картридж|ce\\d+|cf\\d+|tn-?\\d+|dr-?\\d+|np|nv print)", n))
    is_ups = "источник бесперебойного питания" in n or "ибп" in n or "ups" in n
    is_laptop = "ноутбук" in n
    if is_cartridge:
        bullets = [
            ("Совместимость", "Проверьте модель принтера в характеристиках."),
            ("Ресурс", "Оцените примерный ресурс в страницах — указан в параметрах."),
            ("Чип", "При первом запуске следуйте инструкциям на экране устройства.")
        ]
        reviews = [
            ("Ержан, Алматы", "2025-11-06", "Чёткая печать, не полосит. Хватает на долго."),
            ("Алия, Астана", "2025-11-10", "Подошёл к MFP, установка без проблем."),
            ("Сергей, Шымкент", "2025-11-12", "Цена/качество отличные, рекомендую.")
        ]
    elif is_ups:
        bullets = [
            ("На сколько хватит ИБП?", "Обычно 5–15 минут при типовой нагрузке."),
            ("Какая мощность?", "Смотрите мощность в характеристиках."),
            ("Топология?", "Линейно-интерактивный — см. параметры.")
        ]
        reviews = [
            ("Асем, Алматы", "2025-10-28", "Хватает, чтобы спокойно сохранить документы и выключить ПК."),
            ("Ерлан, Астана", "2025-11-02", "Тихая работа и адекватное время автономии для дома."),
            ("Алина, Шымкент", "2025-11-08", "Стабильно держит напряжение, индикаторы информативные.")
        ]
    elif is_laptop:
        bullets = [
            ("Для чего подходит?", "Учёба, офис и мультимедиа — ориентируйтесь на CPU/RAM/SSD."),
            ("Расширение", "Проверьте возможность апгрейда ОЗУ и слотов хранения."),
            ("Гарантия", "Срок и условия смотрите в характеристиках.")
        ]
        reviews = [
            ("Нуржан, Алматы", "2025-11-03", "Тихий, быстрый запуск, батареи хватает на день."),
            ("Марина, Астана", "2025-11-07", "Экран яркий, клавиатура удобная."),
            ("Игорь, Караганда", "2025-11-11", "За свои деньги отличный вариант.")
        ]
    else:
        bullets = [
            ("Подходит ли мне товар?", "Сверьте ключевые параметры в «Характеристики»."),
            ("Комплектация", "Смотрите, что входит в комплект, в карточке."),
            ("Гарантия", "Указана в характеристиках.")
        ]
        reviews = [
            ("Азамат, Алматы", "2025-11-01", "Соответствует описанию, доставили быстро."),
            ("Юлия, Павлодар", "2025-11-05", "Качественно упаковано, работает как нужно."),
            ("Руслан, Костанай", "2025-11-09", "Хорошее соотношение цены и качества.")
        ]
    faq_li = "".join(f'<li style="margin:0 0 8px;"><strong>{html.escape(q)}</strong><br>{html.escape(a)}</li>' for q,a in bullets)
    faq_html = f'<div style="background:#F7FAFF;border:1px solid #DDE8FF;padding:12px 14px;margin:12px 0;"><h3 style="margin:0 0 10px;font-size:17px;">FAQ — Частые вопросы</h3><ul style="margin:0;padding-left:18px;">{faq_li}</ul></div>'
    rev_cards = []
    for who, date, text in reviews:
        rev_cards.append(
            f'<div style="background:#ffffff;border:1px solid #E4F0DD;padding:10px 12px;border-radius:10px;box-shadow:0 1px 0 rgba(0,0,0,.04);margin:0 0 10px;">'
            f'<div style="font-weight:700;">{html.escape(who)} <span style="color:#888;font-weight:400;">— {html.escape(date)}</span></div>'
            f'<div style="color:#f5a623;font-size:14px;margin:2px 0 6px;" aria-label="Оценка 5 из 5">&#9733;&#9733;&#9733;&#9733;&#9733;</div>'
            f'<p style="margin:0;">{html.escape(text)}</p></div>'
        )
    rev_html = '<div style="background:#F8FFF5;border:1px solid #DDEFD2;padding:12px 14px;margin:12px 0;"><h3 style="margin:0 0 10px;font-size:17px;">Отзывы покупателей</h3>' + "".join(rev_cards) + "</div>"
    return faq_html + rev_html

def _strip_tags(text: str, tag_names):
    for t in tag_names:
        text = re.sub(rf"(?is)<\s*{t}\b[^>]*>.*?</\s*{t}\s*>", "", text)
    return text

def _extract_first(tag: str, text: str) -> str:
    m = re.search(rf"(?is)<{tag}\b[^>]*>(.*?)</{tag}>", text)
    return m.group(1).strip() if m else ""

def _extract_params(text: str):
    out = []
    for m in _re_param.finditer(text):
        name = html.unescape(m.group(1)).strip()
        val  = html.unescape(m.group(2)).strip()
        out.append((name, val))
    return out

def _remove_drop_params(text: str) -> str:
    for key in DROP_PARAMS:
        text = re.sub(rf'(?is)<param\b[^>]*name="{re.escape(key)}"[^>]*>.*?</param>\s*', "", text)
    return text

def _move_available_to_attr(text: str) -> str:
    def repl(m):
        offer = m.group(0)
        av = _extract_first("available", offer).lower()
        av = "true" if av == "true" else ("false" if av == "false" else "")
        offer2 = re.sub(r"(?is)<available\b[^>]*>.*?</available>\s*", "", offer)
        offer2 = re.sub(r'(<offer\b)([^>]*?)\s+id="', r'\1\2 id="', offer2, count=1)
        if av:
            if re.search(r'\boffer\b[^>]*\bavailable=', offer2):
                offer2 = re.sub(r'(<offer\b[^>]*\bavailable=")[^"]*"', rf'\1{av}"', offer2, count=1)
            else:
                offer2 = re.sub(r"(<offer\b)", rf'\1 available="{av}"', offer2, count=1)
        return offer2
    return _re_offer.sub(repl, text)

def _swap_price_purchase(text: str) -> str:
    def repl(m):
        offer = m.group(0)
        price = _extract_first("price", offer)
        purchase = _extract_first("purchase_price", offer)
        if purchase:
            offer = re.sub(r"(?is)<price\b[^>]*>.*?</price>", f"<price>{purchase}</price>", offer)
            offer = re.sub(r"(?is)<purchase_price\b[^>]*>.*?</purchase_price>", f"<purchase_price>{price}</purchase_price>", offer)
            try: base = int(re.sub(r"\D", "", purchase) or "0")
            except: base = 0
            retail = _calc_retail_from_purchase(base)
            if retail > 0:
                offer = re.sub(r"(?is)<price\b[^>]*>.*?</price>", f"<price>{retail}</price>", offer)
            offer = re.sub(r"(?is)\s*<purchase_price\b[^>]*>.*?</purchase_price>\s*", "", offer)
        return offer
    return _re_offer.sub(repl, text)

def _reorder_attrs_id_available(opening: str) -> str:
    attrs = re.findall(r'(\w+)="([^"]*)"', opening)
    idv = None; av = None; rest = []
    for k,v in attrs:
        if k == "id": idv = v
        elif k == "available": av = v
        else: rest.append((k,v))
    parts = ['<offer']
    if idv is not None: parts.append(f' id="{idv}"')
    if av is not None: parts.append(f' available="{av}"')
    for k,v in rest: parts.append(f' {k}="{v}"')
    parts.append('>')
    return "".join(parts)

def _prefix_vendorcode_and_id(text: str) -> str:
    def repl(m):
        o = m.group(0)
        vc = _extract_first("vendorCode", o)
        vc_norm = vc.strip()
        if not vc_norm.startswith("AS"): vc_norm = "AS" + vc_norm
        o = re.sub(r"(?is)<vendorCode\b[^>]*>.*?</vendorCode>", f"<vendorCode>{html.escape(vc_norm)}</vendorCode>", o)
        o = re.sub(r'(?is)<offer\b[^>]*>', lambda t: re.sub(r'\s+', ' ', t.group(0)), o, count=1)
        if re.search(r'(?is)<offer\b[^>]*\bid="', o):
            o = re.sub(r'(?is)(<offer\b[^>]*\bid=")[^"]*(")', rf'\1{vc_norm}\2', o, count=1)
        else:
            o = re.sub(r'(?is)<offer\b', rf'<offer id="{vc_norm}"', o, count=1)
        o = re.sub(r'(?is)<offer\b[^>]*>', lambda t: _reorder_attrs_id_available(t.group(0)), o, count=1)
        return o
    return _re_offer.sub(repl, text)

def _order_offer_tags(text: str) -> str:
    def repl(m):
        o = m.group(0)
        def take(tag):
            mm = re.search(rf'(?is)<{tag}\b[^>]*>.*?</{tag}>', o)
            return mm.group(0) if mm else ""
        cat = take("categoryId")
        vcode = take("vendorCode")
        name = take("name")
        price = take("price")
        pics = re.findall(r'(?is)<picture\b[^>]*>.*?</picture>', o)
        vendor = take("vendor")
        curr = take("currencyId")
        desc = take("description")
        params = re.findall(r'(?is)<param\b[^>]*>.*?</param>', o)
        o2 = re.sub(r'(?is)</?categoryId\b[^>]*>.*?|</?vendorCode\b[^>]*>.*?|</?name\b[^>]*>.*?|</?price\b[^>]*>.*?|</?vendor\b[^>]*>.*?|</?currencyId\b[^>]*>.*?|</?description\b[^>]*>.*?|<picture\b[^>]*>.*?</picture>|<param\b[^>]*>.*?</param>', '', o)
        o2 = _strip_tags(o2, ["url", "quantity", "quantity_in_stock", "available", "purchase_price"])
        seq = [cat, vcode, name, price] + pics + [vendor, curr, desc] + params
        middle = "".join(x for x in seq if x)
        open_tag = re.match(r'(?is)<offer\b[^>]*>', o).group(0)
        close_tag = "</offer>"
        return f"{open_tag}{middle}{close_tag}"
    return _re_offer.sub(repl, text)

def _remove_drop_params_block(offer_xml: str) -> str:
    return _remove_drop_params(offer_xml)

def _extract_params_list(body: str):
    return _extract_params(body)

def _build_offer_body(offer_xml: str) -> str:
    body = _remove_drop_params_block(offer_xml)
    name = html.unescape(_extract_first("name", body))
    native_desc = _extract_first("description", body) or ""
    params = _extract_params_list(body)
    new_desc = _build_description(name, native_desc, params)
    body = re.sub(r"(?is)<description\b[^>]*>.*?</description>", new_desc, body, count=1)
    native_plain = re.sub(r"<[^>]+>", " ", native_desc)
    faqrev = _smart_faq_reviews(name, params, native_plain)
    body = re.sub(r"(?is)</description>", f"{faqrev}</description>", body, count=1)
    return body

def _transform_offers(text: str) -> list:
    offers = [m.group(0) for m in _re_offer.finditer(text)]
    kept = []
    for o in offers:
        cat = _extract_first("categoryId", o)
        try: cat_id = int(re.sub(r"\\D","", cat) or "0")
        except: cat_id = 0
        if cat_id not in CATEGORY_FILTER: continue
        o1 = _move_available_to_attr(o)
        o2 = _swap_price_purchase(o1)
        o3 = _prefix_vendorcode_and_id(o2)
        o4 = _build_offer_body(o3)
        o5 = _order_offer_tags(o4)
        kept.append(o5)
    return kept

def _ensure_footer_spacing(out_text: str) -> str:
    out_text = out_text.replace("<shop><offers>", "<shop><offers>\\n\\n")
    out_text = re.sub(r"</offer>\\s*<offer\\b", "</offer>\\n\\n<offer", out_text)
    out_text = re.sub(r"</offer>\\s*</offers>", "</offer>\\n\\n</offers>", out_text)
    out_text = re.sub(r"</offers>\\s*</shop>", "</offers>\\n</shop>", out_text)
    out_text = re.sub(r"</shop>\\s*</yml_catalog>", "</shop>\\n</yml_catalog>", out_text)
    out_text = re.sub(r"(?is)<description>.*?</description>", lambda m: _compact_description_html(m.group(0)), out_text)
    return out_text

def main():
    src = _fetch_source()
    source_total = len(re.findall(r'(?is)<offer\\b', src))
    src = _strip_tags(src, ["url", "quantity", "quantity_in_stock", "available"])
    kept = _transform_offers(src)
    kept_text = "".join(kept)
    head = '<?xml version="1.0" encoding="windows-1251"?>\\n<yml_catalog>\\n<shop><offers>'
    tail = '</offers>\\n</shop>\\n</yml_catalog>\\n'
    out_text = head + "\\n\\n" + kept_text + "\\n\\n" + tail
    available_true = len([1 for o in kept if 'available="true"' in o.lower()])
    available_false = len([1 for o in kept if 'available="false"' in o.lower()])
    built_at = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5)))
    next_build = (built_at + datetime.timedelta(hours=10)).replace(minute=0, second=0, microsecond=0)
    feed_meta = dedent(f"""\
    <!--FEED_META
    Поставщик                                  | AlStyle
    URL поставщика                             | {SUPPLIER_URL}
    Время сборки (Алматы)                      | {built_at.strftime("%Y-%m-%d %H:%M:%S")}
    Ближайшая сборка (Алматы)                  | {next_build.strftime("%Y-%m-%d %H:%M:%S")}
    Сколько товаров у поставщика до фильтра    | {source_total}
    Сколько товаров у поставщика после фильтра | {len(kept)}
    Сколько товаров есть в наличии (true)      | {available_true}
    Сколько товаров нет в наличии (false)      | {available_false}
    -->\\n\\n""")
    out_text = feed_meta + out_text
    out_text = _ensure_footer_spacing(out_text)
    Path("docs").mkdir(exist_ok=True)
    Path(OUT_FILE).write_bytes(out_text.encode(OUTPUT_ENCODING, errors="replace"))
    print(f"OK: {OUT_FILE}, offers: {len(kept)}")

if __name__ == "__main__":
    sys.exit(main())
