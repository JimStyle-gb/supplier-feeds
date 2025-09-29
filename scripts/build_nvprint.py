#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NVPrint -> Satu YML
Правки:
 - <name> берём из <Номенклатура>
 - <picture> берём из <СсылкаНаКартинку>
 - <offer id> = <Артикул> без префикса NV-
 - <vendorCode> = NP + <Артикул> без префикса NV-
 - Цена: приоритет договор "ТА-000079" (Казахстан), затем "TA-000079Мск" (Москва).
   Если обе пустые или отсутствуют -> цена = 100 и дальше не трогаем.
   Иначе применяем правила наценки (как у других поставщиков) и округляем вверх до ...900.
 - available всегда true
 - Лишние теги (categories, url, quantity и пр.) не пишем.
 - Пишем Windows-1251 в docs/nvprint.yml
 - Комментарий FEED_META в стиле других поставщиков с временем Алматы.
"""

import os
import sys
import io
import re
import math
import time
import html
import random
import datetime as dt
from typing import List, Tuple, Optional
import requests
import xml.etree.ElementTree as ET

# --------------------------- Настройки/константы -----------------------------

# Источник NVPrint (жестко задан по просьбе)
NVPRINT_URL = "https://api.nvprint.ru/api/hs/getprice/398/881105302369/none/?format=xml&getallinfo=true"

# Куда пишем итоговый YML (XML-подобный)
OUT_FILE = os.environ.get("OUT_FILE", "docs/nvprint.yml")

# Таймауты и ретраи скачивания
TIMEOUT_S = int(os.environ.get("TIMEOUT_S", "30"))
RETRIES = int(os.environ.get("RETRIES", "4"))
RETRY_BACKOFF_S = float(os.environ.get("RETRY_BACKOFF_S", "1.7"))

# Скрипт-версия для FEED_META
SCRIPT_VERSION = "nvprint-2025-09-29.1"

# ----------------------------- Ценообразование --------------------------------
# Формат правила: (min_incl, max_incl, множитель, наценка_фикс)
PriceRule = Tuple[int, int, float, int]
PRICING_RULES: List[PriceRule] = [
    (   101,    10000, 4.0,  3000),
    ( 10001,    25000, 4.0,  4000),
    ( 25001,    50000, 4.0,  5000),
    ( 50001,    75000, 4.0,  7000),
    ( 75001,   100000, 4.0, 10000),
    (100001,   150000, 4.0, 12000),
    (150001,   200000, 4.0, 15000),
    (200001,   300000, 4.0, 20000),
    (300001,   400000, 4.0, 25000),
    (400001,   500000, 4.0, 30000),
    (500001,   750000, 4.0, 40000),
    (750001,  1000000, 4.0, 50000),
    (1000001, 1500000, 4.0, 70000),
    (1500001, 2000000, 4.0, 90000),
    (2000001,100000000,4.0,100000),
]

def apply_pricing(src_price: int) -> int:
    """
    Применить правила наценки. На входе целое значение цены источника (>= 100).
    1) Ищем подходящий диапазон и считаем цену: ceil(src * factor) + add
    2) Округляем ВВЕРХ до вида ...900 (чтобы последние три цифры были 900)
       Пример: 10789 -> 10900, 238958 -> 239900
    """
    # шаг 1: найдём правило
    factor, add = 4.0, 3000  # дефолт, на всякий
    for lo, hi, f, a in PRICING_RULES:
        if lo <= src_price <= hi:
            factor, add = f, a
            break
    # расчёт базовой розничной
    retail = math.ceil(src_price * factor) + add

    # шаг 2: округление вверх до ...900
    # Формула: берём потолок по тысячам, но гарантируем рост и окончание на 900.
    # Используем трюк: ceil((x + 100) / 1000) * 1000 - 100
    rounded = math.ceil((retail + 100) / 1000.0) * 1000 - 100
    if rounded < retail:
        # Перестраховка: вдруг получилось ниже; тогда просто добавим разницу до ближайших 900 выше
        thousands = (retail // 1000) * 1000
        rounded = thousands + 900
        if rounded < retail:
            rounded += 1000
    return int(rounded)

# ----------------------------- Вспомогательные --------------------------------

def yml_escape(s: str) -> str:
    """Эскейпинг для текста в YML-XML (минимальный)."""
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def norm_spaces(s: str) -> str:
    """Один пробел между словами, обрезка краёв."""
    return re.sub(r"\s+", " ", (s or "")).strip()

BRAND_HINTS = [
    "HP", "Hewlett Packard", "Canon", "Xerox", "Kyocera", "Kyocera-Mita", "Konica Minolta",
    "Brother", "Epson", "Samsung", "Ricoh", "Sharp", "Lexmark", "OKI", "Panasonic", "Toshiba",
    "Dell", "Lenovo"
]
BRAND_MAP = {
    "HEWLETT PACKARD": "HP",
    "KYOCERA-MITA": "Kyocera",
    "SAMSUNG BY HP": "Samsung",
}

def guess_vendor(text: str) -> str:
    """
    Определяем бренд по содержимому Номенклатуры.
    Ищем известные имена; маппим на нормализованные варианты. Фолбэк NV Print.
    """
    t = (text or "").upper()
    for b in BRAND_HINTS:
        if b.upper() in t:
            key = b.upper()
            return BRAND_MAP.get(key, b if b != "Kyocera-Mita" else "Kyocera")
    return "NV Print"

def pick_price_for_item(item: ET.Element) -> Optional[float]:
    """
    Забираем цену из блока <УсловияПродаж>:
      - приоритет договор НомерДоговора="ТА-000079" (Казахстан)
      - затем "TA-000079Мск" (Москва)
      - если обе пустые -> None
    Возвращаем float или None.
    """
    cond = item.find("УсловияПродаж")
    if cond is None:
        return None

    def get_price_by_contract(num: str) -> Optional[float]:
        for deal in cond.findall("Договор"):
            if deal.get("НомерДоговора") == num:
                p = (deal.findtext("Цена") or "").strip()
                if not p:
                    return None
                try:
                    return float(p.replace(",", "."))
                except:
                    return None
        return None

    # Сначала Казахстан
    p_kz = get_price_by_contract("ТА-000079")
    if p_kz is not None:
        return p_kz
    # Потом Москва
    p_msk = get_price_by_contract("TA-000079Мск")
    if p_msk is not None:
        return p_msk
    return None

def clean_article(art: str) -> str:
    """Убираем префикс NV- (без учёта регистра), и лишние пробелы."""
    art = (art or "").strip()
    return re.sub(r"^NV-","", art, flags=re.IGNORECASE)

# ----------------------------- Загрузка XML -----------------------------------

def download_xml(url: str, login: str, password: str) -> bytes:
    """
    Качаем XML NVPrint с базовой авторизацией.
    Делаем ретраи при временных сбоях сетки (кроме 401).
    """
    last_err = None
    for i in range(1, RETRIES + 1):
        try:
            resp = requests.get(url, timeout=TIMEOUT_S, auth=(login, password))
            if resp.status_code == 401:
                raise RuntimeError(f"401 Unauthorized для {url}")
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_err = e
            if i == RETRIES:
                break
            time.sleep(RETRY_BACKOFF_S * i)
    raise RuntimeError(f"Не удалось скачать источник: {last_err}")

# ----------------------------- Основная логика --------------------------------

def build_yml(xml_bytes: bytes) -> str:
    """
    Парсим поставщика и отдаём строку с готовым YML (в кодировке Unicode, далее перекодируем).
    """
    root = ET.fromstring(xml_bytes)

    # Мета
    now_local = dt.datetime.now(dt.timezone(dt.timedelta(hours=5)))  # Asia/Almaty GMT+5
    built_local = now_local.strftime("%Y-%m-%d %H:%M:%S %z")
    supplier_url_for_meta = NVPRINT_URL

    out: List[str] = []
    out.append('<?xml version="1.0" encoding="windows-1251"?>')
    out.append('<!DOCTYPE yml_catalog SYSTEM "shops.dtd">')
    out.append('<yml_catalog date="{}">'.format(dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M")))
    # FEED_META в комментарии: закрываем ровно как просили "(Алматы)-->"
    out.append("<!--FEED_META")
    out.append(f"supplier_url                  = {supplier_url_for_meta:<60} | Источник поставщика")
    out.append(f"encoding                      = windows-1251                                         | Кодировка вывода")
    out.append(f"script_version                = {SCRIPT_VERSION:<60} | Версия скрипта")
    out.append(f"built_Asia/Almaty             = {built_local:<60} | Время сборки (Алматы)-->")
    out.append("<shop>")
    out.append("  <name>nvprint</name>")
    out.append('  <currencies><currency id="KZT" rate="1" /></currencies>')
    out.append("  <offers>")

    offers = 0

    # Идём по товарам
    for item in root.findall("./Товары/Товар"):
        art_raw = (item.findtext("Артикул") or "").strip()
        if not art_raw:
            continue

        art_clean = clean_article(art_raw)                 # для id и vendorCode
        offer_id = art_clean                                # id = без NV-
        vendor_code = "NP" + art_clean                      # vendorCode = NP + без NV-

        # Название из <Номенклатура> (полное), фолбэк <НоменклатураКратко>, фолбэк артикул
        name = item.findtext("Номенклатура")
        if not name:
            name = item.findtext("НоменклатураКратко")
        if not name:
            name = art_clean
        name = norm_spaces(name)

        # Фото из <СсылкаНаКартинку>
        picture = norm_spaces(item.findtext("СсылкаНаКартинку") or "")
        if picture and not picture.lower().startswith("http"):
            # на всякий случай ничего не склеиваем, оставляем как есть
            pass

        # Бренд пытаемся угадать по названию
        vendor = guess_vendor(name)

        # Цена: договор KZ, затем MSK. Если нет цены -> 100 и больше не трогаем.
        price_src = pick_price_for_item(item)
        if price_src is None or price_src < 100:
            final_price = 100
        else:
            # Источник может дать дробь. Округлим до целого перед правилами.
            base = int(round(price_src))
            final_price = apply_pricing(base)

        # Описание: кладём нормализованную Номенклатуру одной строкой
        description = name

        # Пишем оффер
        out.append(f'    <offer id="{yml_escape(offer_id)}">')
        out.append(f"      <name>{yml_escape(name)}</name>")
        out.append(f"      <vendor>{yml_escape(vendor)}</vendor>")
        out.append(f"      <vendorCode>{yml_escape(vendor_code)}</vendorCode>")
        out.append(f"      <price>{final_price}</price>")
        out.append("      <currencyId>KZT</currencyId>")
        if picture:
            out.append(f"      <picture>{yml_escape(picture)}</picture>")
        out.append(f"      <description>{yml_escape(description)}</description>")
        out.append("      <available>true</available>")
        out.append("    </offer>")
        out.append("")  # дополнительный пустой перенос строки между офферами

        offers += 1

    out.append("  </offers>")
    out.append("</shop>")
    out.append("</yml_catalog>")

    # Вставим счётчик офферов в FEED_META (после генерации)
    # Найдём место после <!--FEED_META и добавим строку
    for i, line in enumerate(out):
        if line.strip() == "<!--FEED_META":
            out.insert(i + 1, f"offers_count                  = {offers:<60} | Кол-во офферов")
            break

    return "\n".join(out)

# --------------------------------- Main ---------------------------------------

def main():
    # Логин/пароль из переменных окружения
    login = os.environ.get("NVPRINT_LOGIN", "").strip()
    password = os.environ.get("NVPRINT_PASSWORD", "").strip()
    if not login or not password:
        print("ERROR: NVPRINT_LOGIN / NVPRINT_PASSWORD не заданы", file=sys.stderr)

    print(f"Source: {NVPRINT_URL}")

    try:
        xml_bytes = download_xml(NVPRINT_URL, login, password)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        # Даже при ошибке сохраним пустую болванку, чтобы пайплайн не ломался
        empty = '<?xml version="1.0" encoding="windows-1251"?><yml_catalog date=""><shop><name>nvprint</name><currencies><currency id="KZT" rate="1" /></currencies><offers></offers></shop></yml_catalog>'
        with open(OUT_FILE, "wb") as f:
            f.write(empty.encode("cp1251", errors="ignore"))
        print(f"Wrote: {OUT_FILE} | encoding=windows-1251")
        sys.exit(0)

    yml_text = build_yml(xml_bytes)

    # Пишем в Windows-1251
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "wb") as f:
        f.write(yml_text.encode("cp1251", errors="ignore"))

    print(f"Wrote: {OUT_FILE} | encoding=windows-1251")

if __name__ == "__main__":
    main()
