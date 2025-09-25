# scripts/build_vtt.py
# -*- coding: utf-8 -*-
"""
VTT (b2b.vtt.ru) -> YML (KZT) по общему шаблону (как alstyle/akcent)

Главное:
- Структура YML: <?xml ...?><yml_catalog><--FEED_META--><shop><offers>...</offers></shop></yml_catalog>
- Порядок полей в <offer>: name, vendor, vendorCode, price, currencyId, picture, description, available
- Префикс vendorCode: VT
- Удаляем: <categories>, <categoryId>, <url>, любые *quantity*
- <available>true</available> для всех
- ЦЕНООБРАЗОВАНИЕ: как у Akcent
  * Берём «дилерскую» цену с сайта (parse_price_kzt).
  * Если цены нет / ≤100 — пропускаем товар.
  * Применяем таблицу PRICING_RULES (процент + надбавка).
  * Финальная доводка: _force_tail_900(val) — последняя тысяча + 900 (как в Akcent).
  * В YML пишем уже готовую цену (без повторного пересчёта).
"""

from __future__ import annotations
import os, re, time, html, hashlib
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ---------------- НАСТРОЙКИ ----------------
BASE_URL        = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL       = os.getenv("START_URL", f"{BASE_URL}/catalog/")
OUT_FILE        = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN       = os.getenv("VTT_LOGIN", "").strip()
VTT_PASSWORD    = os.getenv("VTT_PASSWORD", "").strip()

CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "docs/categories_vtt.txt")

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"
ALLOW_SSL_FALLBACK = os.getenv("ALLOW_SSL_FALLBACK", "0") == "1"

HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS= int(os.getenv("REQUEST_DELAY_MS", "120"))
MIN_BYTES       = int(os.getenv("MIN_BYTES", "600"))

MAX_PAGES       = int(os.getenv("MAX_PAGES", "800"))
MAX_CRAWL_MIN   = int(os.getenv("MAX_CRAWL_MINUTES", "60"))
MAX_WORKERS     = int(os.getenv("MAX_WORKERS", "6"))

UA = {"User-Agent": "Mozilla/5.0 (compatible; VTT-Feed/akcent-pricing-1.0)"}

# ---------------- ЦЕНООБРАЗОВАНИЕ (как у Akcent) ----------------
from typing import Tuple
PriceRule = Tuple[int,int,float,int]  # (min_incl, max_incl, pct, add_kzt)
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
    (750001,  100
