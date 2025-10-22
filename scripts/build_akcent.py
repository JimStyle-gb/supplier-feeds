# scripts/build_akcent.py
# -*- coding: utf-8 -*-
# -----------------------------
#  AKCENT → YML post-builder: чистые описания, всегда available=true,
#  единый categories=0, префикс vendorCode=AC, аккуратный CDATA.
#  Готов к запуску как самостоятельный шаг пайплайна.
# -----------------------------
from __future__ import annotations

import os, sys, re, io, html, hashlib, datetime
from typing import Dict, List, Tuple, Optional
from xml.etree import ElementTree as ET

# ===================== ПАРАМЕТРЫ =====================
# Входной файл:
#  1) IN_FILE из env
#  2) docs/akcent.yml (если уже собирается другим шагом)
#  3) fallback для локальной отладки: /mnt/data/akcent (7).yml
IN_FILE  = os.getenv("IN_FILE", "docs/akcent.yml")
FALLBACK = "/mnt/data/akcent (7).yml"

# Выход:
OUT_FILE = os.getenv("OUT_FILE", "docs/akcent.yml")
OUT_ENC  = os.getenv("OUTPUT_ENCODING", "windows-1251")  # Сату любит windows-1251

# Политики:
FORCE_AVAILABLE_TRUE = True   # Всегда выставлять <available>true</available>
FORCE_CATEGORY_ID0   = True   # Принудительно categoryId=0 (чтобы не плодить «не те» категории)
WRITE_CATEGORIES     = True   # Вывести минимальный <categories> с id=0
DROP_OLDPRICE        = True   # Удалить <oldprice>
VENDOR_PREFIX        = "AC"   # Префикс для всех vendorCode (без дефиса, всегда добавлять)

# Бренды-поставщики, которые нельзя писать в <vendor> как бренд
VENDOR_BLOCKLIST = {"alstyle", "al-style", "copyline", "vtt", "akcent", "ak-cent"}

# ===================== УТИЛИТЫ =====================

def read_xml_text(path: str) -> str:
    """Считываем XML/YML как текст с авто-детектом базовой кодировки."""
    tried = []
    for enc in ("utf-8", "windows-1251", "cp1251", "utf-8-sig"):
        try:
            with io.open(path, "r", encoding=enc, errors="strict") as f:
                return f.read()
        except Exception as e:
            tried.append(f"{enc}: {e}")
    # Последняя попытка — бинарно с заменой символов (чтобы не упасть)
    with io.open(path, "rb") as f:
        data = f.read()
    try:
        return data.decode("utf-8", "replace")
    except Exception:
        return data.decode("cp1251", "replace")


def ensure_input_path(primary: str, fallback: str) -> str:
    """Возвращаем существующий путь входного файла."""
    if os.path.isfile(primary):
        return primary
    if os.path.isfile(fallback):
        return fallback
    sys.stderr.write(f"[error] IN_FILE not found: {primary!r} and fallback {fallback!r}\n")
    sys.exit(2)


def strip_html_tags_keep_breaks(s: str) -> str:
    """Убираем теги, но переносы строк/блоков превращаем в \n, чтобы сохранить структуру."""
    if not s:
        return ""
    # заменяем <br>, <p>, <li>, <div>, <hN> на переводы строк
    s = re.sub(r"(?i)<\s*(br|/p|/div|/li|/h[1-6])\s*>", "\n", s)
    s = re.sub(r"(?i)<\s*(p|div|li|h[1-6])\b[^>]*>", "\n", s)
    # удаляем остальные теги
    s = re.sub(r"<[^>]+>", "", s)
    # нормализуем пробелы
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    # нормализуем переводы строк
    s = re.sub(r"\n\s*\n\s*\n+", "\n\n", s)
    return s.strip()


def remove_inline_styles(s: str) -> str:
    """Убираем inline style="..." из HTML."""
    return re.sub(r'\s*style="[^"]*"', "", s)


def remove_emojis(s: str) -> str:
    """Убираем эмодзи и спец-символы вида &#9989; и т.п."""
    if not s:
        return s
    s = re.sub(r"&#\d+;", "", s)  # убираем HTML-числовые сущности эмодзи
    # Простая фильтрация по юникод-диапазонам (эмодзи/символы)
    return re.sub(
        r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]+", "", s
    )


def fix_double_colons(s: str) -> str:
    """Убираем удвоенные двоеточия :: -> :"""
    return re.sub(r"::+", ":", s)


def html_cleanup_inner(html_text: str) -> str:
    """Комплексная чистка HTML-содержимого описания перед сборкой."""
    if not html_text:
        return ""
    t = html.unescape(html_text)              # &quot; -> "
    t = remove_inline_styles(t)               # вырезаем style="…"
    t = remove_emojis(t)                      # убираем эмодзи
    t = fix_double_colons(t)                  # :: -> :
    # Чуть подчистим одиночные многоточия/двойные точки
    t = re.sub(r"(?<!\.)\.\.(?!\.)", ".", t)
    t = re.sub(r"\s{2,}", " ", t)
    return t.strip()


def detect_kind(name: str) -> str:
    """Определяем тип устройства по названию."""
    n = (name or "").lower()
    if any(k in n for k in ("сканер", "scanner")):
        return "scanner"
    if any(k in n for k in ("проектор", "projector", "beamer")):
        return "projector"
    if any(k in n for k in ("мфу", "mfp", "laserjet", "deskjet", "officejet")):
        return "mfp"
    if any(k in n for k in ("принтер", "printer")):
        return "printer"
    return "other"


# Карта отображения ключей (снизу всё в нижнем регистре!)
KV_MAP_SCANNER = {
    "устройство": "Устройство",
    "применение": "Применение",
    "сканирование с планшета": "Тип сканирования",
    "тип датчика": "Тип датчика",
    "тип лампы": "Подсветка",
    "epson readyscan led": "Подсветка",
    "dual lens system": "Оптика",
    "digital ice, пленка": "Обработка пленки",
    "digital ice, непрозрачные оригиналы": "Обработка оригиналов",
    "разрешение сканера, dpi": "Оптическое разрешение",
    "интерполяционное разрешение, dpi": "Интерполяция",
    "глубина цвета, бит": "Глубина цвета",
    "пленка 35 мм": "Пленка 35 мм",
    "слайды 35 мм": "Слайды 35 мм",
    "максимальный формат сканирования": "Макс. формат",
    "скорость сканирования": "Скорость сканирования",
    "интерфейс usb": "Подключение",
    "интерфейс ieee-1394 (firewire)": "FireWire",
    "подключение по wi-fi": "Wi-Fi",
}

KV_MAP_PROJECTOR = {
    "яркость": "Яркость",
    "контрастность": "Контрастность",
    "разрешение": "Разрешение",
    "лампа": "Источник света",
    "ресурс лампы": "Ресурс источника",
    "входы": "Входы",
    "интерфейсы": "Интерфейсы",
}

KV_MAP_PRINTER = {
    "технология печати": "Технология печати",
    "формат": "Формат",
    "скорость печати": "Скорость печати",
    "интерфейсы": "Интерфейсы",
    "двусторонняя печать": "Двусторонняя печать",
}

def choose_kv_map(kind: str) -> Dict[str, str]:
    if kind == "scanner":
        return KV_MAP_SCANNER
    if kind == "projector":
        return KV_MAP_PROJECTOR
    if kind in ("mfp", "printer"):
        return KV_MAP_PRINTER
    return {}


def parse_native_block_to_kv(native_html: str, kind: str) -> Tuple[Dict[str, str], Optional[str]]:
    """
    Извлекаем из <div class="native"> сырые строки и превращаем в KV по карте ключей.
    Возвращаем (features_kv, комплектация_str).
    """
    if not native_html:
        return {}, None

    # 1) уберём теги, сохраняя переносы, распакуем сущности
    text = strip_html_tags_keep_breaks(html_cleanup_inner(native_html))
    if not text:
        return {}, None

    # 2) подготовим словарь и контейнер «Комплектация»
    kv_map  = choose_kv_map(kind)
    out: Dict[str, str] = {}
    bundle_items: List[str] = []
    in_bundle = False

    # 3) Пройдём по строкам
    lines = [ln.strip(" \t-•—:") for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]

    # Небольшой помощник: берём пары "ключ  значение" по различным разделителям
    def split_kv(line: str) -> Optional[Tuple[str, str]]:
        # таб/много пробелов
        m = re.split(r"\s{2,}|\t", line, maxsplit=1)
        if len(m) == 2:
            return m[0].strip(), m[1].strip()
        # двоеточие
        m2 = line.split(":", 1)
        if len(m2) == 2:
            return m2[0].strip(), m2[1].strip()
        return None

    # Ключевые заголовки для комплектации
    BUNDLE_HEADERS = {
        "состав поставки", "комплектация", "в комплекте"
    }
    # Границы блоков-характеристик
    STOP_HEADERS = {
        "основные характеристики", "характеристики", "дополнительная информация",
        "качество сканирования", "интерфейсы", "форматы оригиналов", "скорость сканирования"
    }.union(BUNDLE_HEADERS)

    for raw in lines:
        low = raw.lower()

        # Входим/выходим из «Комплектация»
        if low in BUNDLE_HEADERS:
            in_bundle = True
            continue
        # Если встретили новый раздел/заголовок — завершаем комплект
        if in_bundle and (low in STOP_HEADERS or len(raw) < 3):
            in_bundle = False

        if in_bundle:
            # Элементы комплектации — чаще просто строки
            # Фильтруем очевидный шум
            if len(raw) >= 2 and raw.lower() not in ("да", "нет", "—", "-"):
                bundle_items.append(raw.strip(" .;"))
            continue

        # Пытаемся распознать пары ключ-значение
        kv = split_kv(raw)
        if not kv:
            continue

        k, v = kv
        kl = k.lower().strip()
        vl = v.strip()

        # Отбрасываем пустяки/мусор
        if not vl or vl in ("-", "—"):
            continue
        # Слишком общие «Да/Нет» без полезной ценности — пропустим
        if vl.lower() in ("нет", "да"):
            # оставим только для некоторых ключей, если это явно важно
            if kl not in ("двусторонняя печать", "wi-fi", "подключение по wi-fi", "интерфейс usb"):
                continue

        # Нормализуем ключ через карту
        label = None
        # точное совпадение
        if kl in kv_map:
            label = kv_map[kl]
        else:
            # мягкий поиск — ближайшее совпадение по началу
            for key, lab in kv_map.items():
                if kl.startswith(key):
                    label = lab
                    break
        if not label:
            # Если ключ не мапится — пропускаем (чтобы не раздувать список)
            continue

        # Сохраняем человекочитаемым образом
        out[label] = vl

    bundle = None
    if bundle_items:
        # Укладываем в одну строку
        bundle = "; ".join(dict.fromkeys(bundle_items))  # уникализируем порядок
    return out, bundle


def build_bullets(kind: str, kv: Dict[str, str], name: str) -> List[str]:
    """Строим 3-5 «плюсов» по типу устройства и распознанным характеристикам."""
    bullets: List[str] = []
    nlow = (name or "").lower()

    if kind == "scanner":
        # dpi
        dpi = kv.get("Оптическое разрешение") or kv.get("Интерполяция", "")
        m = re.search(r"(\d{3,4})\s*[xх×]\s*(\d{3,4})", dpi or "", flags=re.I)
        if m:
            bullets.append(f"Оптическое разрешение до {m.group(1)}×{m.group(2)} dpi")
        # датчик
        if "Тип датчика" in kv and kv["Тип датчика"]:
            bullets.append(f"Сенсор: {kv['Тип датчика']}")
        # формат
        if "Макс. формат" in kv:
            bullets.append(f"Максимальный формат — {kv['Макс. формат']}")
        # подключение
        if any(k in kv for k in ("Подключение", "Wi-Fi", "FireWire")):
            con = kv.get("Подключение") or kv.get("Wi-Fi") or kv.get("FireWire")
            bullets.append(f"Подключение: {con}")
        if not bullets:
            bullets.append("Планшетный сканер для дома и офиса")

    elif kind == "projector":
        if "Яркость" in kv:
            bullets.append(f"Яркость: {kv['Яркость']}")
        if "Разрешение" in kv:
            bullets.append(f"Разрешение: {kv['Разрешение']}")
        if "Контрастность" in kv:
            bullets.append(f"Контрастность: {kv['Контрастность']}")
        if not bullets:
            bullets.append("Подходит для презентаций и обучения")

    elif kind in ("mfp", "printer"):
        if "Скорость печати" in kv:
            bullets.append(f"Скорость печати: {kv['Скорость печати']}")
        if "Двусторонняя печать" in kv:
            bullets.append(f"Двусторонняя печать: {kv['Двусторонняя печать']}")
        if "Интерфейсы" in kv:
            bullets.append(f"Интерфейсы: {kv['Интерфейсы']}")
        if not bullets:
            bullets.append("Надёжное решение для дома и офиса")

    else:
        # other
        bullets.append("Практичное решение для повседневных задач")

    # Ограничим 3–5 пунктов
    return bullets[:5]


def build_intro(kind: str) -> str:
    if kind == "scanner":
        return "Планшетный сканер для дома и офиса."
    if kind == "projector":
        return "Проектор для презентаций, обучения и домашнего кино."
    if kind in ("mfp", "printer"):
        return "Универсальное устройство для печати и ежедневных задач."
    return "Коротко о ключевых преимуществах модели."


def render_description(name: str, kind: str, kv: Dict[str, str], bundle: Optional[str]) -> str:
    """Собираем финальный HTML в CDATA: h3 + интро + буллеты + характеристики + FAQ + отзывы."""
    title = f"{name}: ключевые преимущества"
    intro = build_intro(kind)
    bullets = build_bullets(kind, kv, name)

    # Характеристики (в человекочитаемом порядке)
    important_order = [
        "Тип сканирования", "Тип датчика", "Подсветка", "Оптическое разрешение",
        "Интерполяция", "Глубина цвета", "Макс. формат", "Скорость сканирования",
        "Подключение", "Wi-Fi", "FireWire",
        "Технология печати", "Формат", "Скорость печати", "Интерфейсы", "Двусторонняя печать",
        "Яркость", "Разрешение", "Контрастность", "Источник света", "Ресурс источника", "Входы",
    ]
    # Отсортируем kv по приоритету
    def sort_key(it: Tuple[str, str]) -> Tuple[int, str]:
        k, _ = it
        try:
            return (important_order.index(k), k)
        except ValueError:
            return (999, k)

    kv_items = [(k, v) for k, v in kv.items() if v]
    kv_items.sort(key=sort_key)

    # Комплектация — в конец списка характеристик отдельной строкой
    if bundle:
        kv_items.append(("Комплектация", bundle))

    # FAQ — компактно, 2 вопроса
    if kind == "scanner":
        faq = [
            ("Подходит для современных задач?", "Да, оптимален для домашнего и офисного использования."),
            ("Нужно ли питание от сети?", "Как правило, достаточно подключения по USB (см. характеристики)."),
        ]
    elif kind == "projector":
        faq = [
            ("Подойдёт ли для яркого помещения?", "Смотрите на яркость (ANSI lm) и контраст — указаны в характеристиках."),
            ("Какие входы доступны?", "Список входов и интерфейсов — в характеристиках модели."),
        ]
    elif kind in ("mfp", "printer"):
        faq = [
            ("Есть ли двусторонняя печать?", "Смотрите пункт «Двусторонняя печать» в характеристиках."),
            ("Поддерживается Wi-Fi?", "См. раздел «Интерфейсы/Подключение» выше."),
        ]
    else:
        faq = [
            ("Подойдёт на каждый день?", "Да, модель рассчитана на повседневные задачи."),
            ("Сложно ли в установке?", "Нет, базовая настройка занимает несколько минут."),
        ]

    # Отзывы — три компактных, без эмодзи
    reviews = [
        ("Евгений, Темиртау", "★★★★★", "Качество отличное, с задачами справляется уверенно."),
        ("Жанна, Экибастуз", "★★★★★", "Установка заняла пару минут, всё просто и понятно."),
        ("Сергей, Кокшетау", "★★★★☆", "Соотношение цены и возможностей хорошее."),
    ]

    # Собираем HTML
    parts: List[str] = []
    parts.append(f"<h3>{html.escape(title)}</h3>")
    parts.append(f"<p>{html.escape(intro)}</p>")

    if bullets:
        parts.append("<ul>")
        for b in bullets:
            parts.append(f"  <li>{html.escape(b)}</li>")
        parts.append("</ul>")

    if kv_items:
        parts.append("<h3>Характеристики</h3>")
        parts.append("<ul>")
        for k, v in kv_items:
            parts.append(f"  <li><strong>{html.escape(k)}:</strong> {html.escape(v)}</li>")
        parts.append("</ul>")

    parts.append("<h3>FAQ</h3>")
    parts.append("<p><strong>В:</strong> " + html.escape(faq[0][0]) + "<br><strong>О:</strong> " + html.escape(faq[0][1]) + "</p>")
    parts.append("<p><strong>В:</strong> " + html.escape(faq[1][0]) + "<br><strong>О:</strong> " + html.escape(faq[1][1]) + "</p>")

    parts.append("<h3>Отзывы (3)</h3>")
    for who, stars, text in reviews:
        parts.append(f"<p><strong>{html.escape(who)}</strong> — {html.escape(stars)}<br>{html.escape(text)}</p>")

    # Склеиваем + финальная лёгкая чистка
    html_out = "\n".join(parts)
    html_out = html_cleanup_inner(html_out)

    # Оборачиваем в CDATA как требует маркетплейс
    return "<![CDATA[\n" + html_out + "\n]]>"


def ensure_vendor_prefix(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    c = re.sub(r"\s+", "", str(code))
    # Всегда добавляем префикс (без дефиса), даже если уже похож — по требованию пользователя
    if not c.startswith(VENDOR_PREFIX):
        c = VENDOR_PREFIX + c
    return c


def normalize_vendor(vendor: Optional[str]) -> Optional[str]:
    """Не использовать названия поставщиков в <vendor>."""
    if not vendor:
        return None
    v = vendor.strip()
    if not v:
        return None
    if v.lower() in VENDOR_BLOCKLIST:
        return None
    # Убираем явные маркеры «no brand»
    if v.lower() in {"no brand", "noname", "неизвестный", "без бренда"}:
        return None
    return v


def text_of(elem: ET.Element, tag: str) -> Optional[str]:
    node = elem.find(tag)
    if node is None or node.text is None:
        return None
    return node.text


def get_native_html_from_description(desc_text: str) -> Optional[str]:
    """Извлекаем кусок <div class="native">…</div> если он был в исходном описании."""
    if not desc_text:
        return None
    m = re.search(r'(<div[^>]+class="[^"]*\bnative\b[^"]*"[^>]*>.*?</div>)', desc_text, flags=re.I | re.S)
    if not m:
        return None
    return m.group(1)


def make_nojekyll(path: str) -> None:
    """Создаём .nojekyll рядом с выходным файлом (для GitHub Pages)."""
    try:
        docs_dir = os.path.dirname(os.path.abspath(path))
        with io.open(os.path.join(docs_dir, ".nojekyll"), "w", encoding="utf-8") as f:
            f.write("")
    except Exception:
        pass


def build_output_yml(
    offers: List[Dict[str, Optional[str]]],
    out_path: str,
    encoding: str = "windows-1251",
    write_categories: bool = True,
) -> None:
    """Пишем итоговый YML как строку (вручную), чтобы гарантировать CDATA."""
    # Подготовим FEED_META в виде XML-комментария (как просил пользователь: «как у всех»)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta = {
        "supplier": "AKCENT",
        "generated_at": now,
        "offers_total": str(len(offers)),
        "available_forced": str(FORCE_AVAILABLE_TRUE).lower(),
        "category_id_forced": "0" if FORCE_CATEGORY_ID0 else "as is",
        "encoding": encoding,
        "vendor_prefix": VENDOR_PREFIX,
    }

    # Собираем текст
    lines: List[str] = []
    lines.append('<?xml version="1.0" encoding="%s"?>' % encoding.upper())
    # FEED_META — многострочный комментарий, важно: пустая строка после
    lines.append("<!-- FEED_META")
    for k, v in meta.items():
        lines.append(f"{k}: {v}")
    lines.append("-->")
    lines.append("<yml_catalog date=\"%s\">" % now)
    lines.append("  <shop>")

    # Минимальная секция категорий (если нужна)
    if write_categories:
        lines.append("    <categories>")
        lines.append("      <category id=\"0\">AKCENT</category>")
        lines.append("    </categories>")

    # Валюта — оставим как KZT (по умолчанию)
    lines.append("    <currencies>")
    lines.append("      <currency id=\"KZT\" rate=\"1\"/>")
    lines.append("    </currencies>")

    lines.append("    <offers>")
    for o in offers:
        oid   = o.get("id") or ""
        url   = o.get("url")
        price = o.get("price")
        picts = o.get("picture_list") or []  # список картинок
        vendc = o.get("vendorCode")
        vend  = o.get("vendor")
        name  = o.get("name") or ""
        desc  = o.get("description") or "<![CDATA[]]>"

        # Старт оффера
        lines.append(f'      <offer id="{html.escape(oid)}" available="true">')

        # name
        lines.append(f"        <name>{html.escape(name)}</name>")

        # price
        if price:
            lines.append(f"        <price>{html.escape(price)}</price>")

        # currencyId
        lines.append("        <currencyId>KZT</currencyId>")

        # categoryId
        cat_id = "0" if FORCE_CATEGORY_ID0 else (o.get("categoryId") or "0")
        lines.append(f"        <categoryId>{cat_id}</categoryId>")

        # url
        if url:
            lines.append(f"        <url>{html.escape(url)}</url>")

        # picture (несколько)
        for p in picts:
            if not p:
                continue
            lines.append(f"        <picture>{html.escape(p)}</picture>")

        # vendorCode
        if vendc:
            lines.append(f"        <vendorCode>{html.escape(vendc)}</vendorCode>")

        # vendor
        if vend:
            lines.append(f"        <vendor>{html.escape(vend)}</vendor>")

        # описание (CDATA уже внутри)
        lines.append(f"        <description>{desc}</description>")

        # Склады/наличие — по политике всегда available=true, отдельные поля не требуются
        lines.append("      </offer>")
    lines.append("    </offers>")
    lines.append("  </shop>")
    lines.append("</yml_catalog>")

    # Записываем
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with io.open(out_path, "w", encoding=encoding, errors="replace", newline="") as f:
        f.write("\n".join(lines))

    # .nojekyll для GitHub Pages
    make_nojekyll(out_path)


def main() -> None:
    # 1) Входной файл
    src = ensure_input_path(IN_FILE, FALLBACK)
    print(f"[info] input:  {src}")
    print(f"[info] output: {OUT_FILE} ({OUT_ENC})")

    # 2) Читаем XML как текст и парсим
    xml_text = read_xml_text(src)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        # Иногда в заголовке «yml» попадаются BOM/мусор — подчистим и повторим
        cleaned = re.sub(r"[^\S\r\n]+<!--", "<!--", xml_text, count=1)
        root = ET.fromstring(cleaned)

    # 3) Ищем offers
    shop = root.find(".//shop")
    offers_parent = shop.find("offers") if shop is not None else root.find(".//offers")
    if offers_parent is None:
        print("[error] <offers> not found")
        sys.exit(3)

    offers_out: List[Dict[str, Optional[str]]] = []
    total = 0
    written = 0

    for offer in offers_parent.findall("offer"):
        total += 1

        # Базовые поля
        oid   = offer.get("id") or f"AC_{total}"
        url   = text_of(offer, "url")
        name  = text_of(offer, "name") or ""
        price = text_of(offer, "price")
        vendc = text_of(offer, "vendorCode")
        vend  = text_of(offer, "vendor")
        catid = text_of(offer, "categoryId")

        # Картинки
        picture_list = [ (p.text or "").strip() for p in offer.findall("picture") if (p.text or "").strip() ]

        # Старые теги цен — вычистить из исходного дерева (чтобы не дублировать)
        if DROP_OLDPRICE:
            for tag in ("oldprice", "purchase_price", "wholesale_price", "b2b_price", "prices"):
                t = offer.find(tag)
                if t is not None:
                    offer.remove(t)

        # Принудительно available=true (в итоговой записи мы выставляем на уровне атрибута)
        if FORCE_AVAILABLE_TRUE:
            offer.set("available", "true")

        # Нормализуем vendorCode (префикс AC)
        vendc = ensure_vendor_prefix(vendc) or ensure_vendor_prefix(oid)

        # Нормализуем vendor (не ставим имена поставщиков)
        vend = normalize_vendor(vend)

        # Детект типа
        kind = detect_kind(name)

        # Забираем исходное описание (если есть) и пытаемся вытащить «native» блок
        raw_desc = text_of(offer, "description") or ""
        # raw_desc может быть уже с CDATA — извлечём содержимое
        raw_desc_inner = re.sub(r"^<!\[CDATA\[(.*)\]\]>$", r"\1", raw_desc, flags=re.S)
        native_html = get_native_html_from_description(raw_desc_inner)

        # Парсим KV + комплектацию
        kv, bundle = parse_native_block_to_kv(native_html or raw_desc_inner, kind)

        # Собираем новое описание
        desc_cdata = render_description(name=name, kind=kind, kv=kv, bundle=bundle)

        # Готовим оффер к записи
        offers_out.append({
            "id": oid,
            "url": url,
            "price": price,
            "categoryId": "0" if FORCE_CATEGORY_ID0 else (catid or "0"),
            "picture_list": picture_list,
            "vendorCode": vendc,
            "vendor": vend,
            "name": name,
            "description": desc_cdata,
        })
        written += 1

    # 4) Пишем итоговый YML вручную (ради CDATA и стабильной структуры)
    build_output_yml(offers_out, OUT_FILE, encoding=OUT_ENC, write_categories=WRITE_CATEGORIES)

    print(f"[done] offers_total={total} offers_written={written} -> {OUT_FILE}")


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        # Печатаем ошибку и выходим ненулевым кодом (чтобы GitHub Actions корректно упал)
        sys.stderr.write(f"[fatal] {ex}\n")
        sys.exit(1)
