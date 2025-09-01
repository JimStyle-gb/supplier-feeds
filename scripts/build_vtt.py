# -*- coding: utf-8 -*-
"""
VTT b2b → минимальный YML (только название и ссылка):
- Логин через /validateLogin с X-CSRF-TOKEN
- Обход /catalog/?page=N (N=1..MAX_PAGES) до 3 пустых подряд или до лимита времени
- Извлекаем ссылки по селектору:
  div.catalog_list_row div.cl_name .cutoff-off a[href*="/catalog/"]:not(.btn_naked)

Требует: requests, beautifulsoup4
ENV:
  BASE_URL (https://b2b.vtt.ru)
  START_URL (https://b2b.vtt.ru/catalog/)
  VTT_LOGIN, VTT_PASSWORD
  DISABLE_SSL_VERIFY ("1"|"0")
  HTTP_TIMEOUT, REQUEST_DELAY_MS, MIN_BYTES, MAX_PAGES, MAX_CRAWL_MINUTES
  OUT_FILE, OUTPUT_ENCODING
"""
from __future__ import annotations
import os, time, html, hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ------------- ENV -------------
BASE_URL         = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL        = os.getenv("START_URL", f"{BASE_URL}/catalog/")

OUT_FILE         = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING  = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN        = os.getenv("VTT_LOGIN", "")
VTT_PASSWORD     = os.getenv("VTT_PASSWORD", "")

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"
HTTP_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "180"))
MIN_BYTES          = int(os.getenv("MIN_BYTES", "700"))

MAX_PAGES          = int(os.getenv("MAX_PAGES", "800"))
MAX_CRAWL_MINUTES  = int(os.getenv("MAX_CRAWL_MINUTES", "50"))

UA = {
    "User-Agent": "Mozilla/5.0 (compatible; VTT-Minimal/1.0; +https://github.com/)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

ROOT_CAT_ID    = 9600000
ROOT_CAT_NAME  = "VTT"
CURRENCY       = "RUB"
SUPPLIER_NAME  = "vtt"

# ------------- UTILS -------------
def jitter_sleep(ms: int) -> None:
    if ms > 0:
        time.sleep(ms / 1000.0)

def soup_of(content: bytes) -> BeautifulSoup:
    return BeautifulSoup(content, "html.parser")

def good(resp: requests.Response) -> bool:
    return (resp is not None) and (resp.status_code == 200) and (len(resp.content) >= MIN_BYTES)

def is_login_page_bytes(b: bytes) -> bool:
    try:
        s = b.decode("utf-8", errors="ignore").lower()
    except Exception:
        return False
    # Признаки страницы входа
    return ("/validatelogin" in s) or ("вход для клиентов" in s)

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

def yml_escape(s: str) -> str:
    return html.escape(s or "")

def build_yml(items: List[Tuple[str, str]]) -> str:
    """
    items: list of (title, url)
    Минимальный YML: name + url, цена=0, категория одна.
    """
    out: List[str] = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append(f"<name>{yml_escape(SUPPLIER_NAME)}</name>")
    out.append(f"<currencies><currency id=\"{CURRENCY}\" rate=\"1\" /></currencies>")
    out.append("<categories>")
    out.append(f"<category id=\"{ROOT_CAT_ID}\">{yml_escape(ROOT_CAT_NAME)}</category>")
    out.append("</categories>")
    out.append("<offers>")

    seen_ids = set()
    for title, url in items:
        if not title:
            continue
        offer_id = sha1(url or title)
        if offer_id in seen_ids:
            continue
        seen_ids.add(offer_id)
        out += [
            f"<offer id=\"{offer_id}\" available=\"true\" in_stock=\"true\">",
            f"<name>{yml_escape(title)}</name>",
            f"<vendor>{yml_escape(ROOT_CAT_NAME)}</vendor>",
            "<price>0</price>",
            f"<currencyId>{CURRENCY}</currencyId>",
            f"<categoryId>{ROOT_CAT_ID}</categoryId>",
        ]
        if url:
            out.append(f"<url>{yml_escape(url)}</url>")
        # чтобы не плодить поля — описание = имя
        out.append(f"<description>{yml_escape(title)}</description>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    return "\n".join(out)

# ------------- HTTP -------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    return s

def request_get(s: requests.Session, url: str) -> Optional[requests.Response]:
    try:
        r = s.get(url, timeout=HTTP_TIMEOUT, verify=(not DISABLE_SSL_VERIFY))
        return r
    except requests.exceptions.SSLError:
        # Если внезапно упали на SSL, а проверка включена — можно принудительно выключить
        if not DISABLE_SSL_VERIFY:
            try:
                r = s.get(url, timeout=HTTP_TIMEOUT, verify=False)
                return r
            except Exception:
                return None
        return None
    except Exception:
        return None

def request_post(s: requests.Session, url: str, data: dict, headers: dict) -> Optional[requests.Response]:
    try:
        r = s.post(url, data=data, headers=headers, timeout=HTTP_TIMEOUT, verify=(not DISABLE_SSL_VERIFY))
        return r
    except requests.exceptions.SSLError:
        if not DISABLE_SSL_VERIFY:
            try:
                r = s.post(url, data=data, headers=headers, timeout=HTTP_TIMEOUT, verify=False)
                return r
            except Exception:
                return None
        return None
    except Exception:
        return None

# ------------- LOGIN -------------
def extract_csrf_token(b: bytes) -> Optional[str]:
    s = soup_of(b)
    m = s.find("meta", attrs={"name": "csrf-token"})
    return (m.get("content").strip() if m and m.get("content") else None)

def login(s: requests.Session) -> bool:
    if not VTT_LOGIN or not VTT_PASSWORD:
        print("Error: VTT_LOGIN / VTT_PASSWORD not set.")
        return False

    # 1) GET главной/логина, достать CSRF
    r0 = request_get(s, f"{BASE_URL}/")
    if not (r0 and good(r0)):
        print("Error: cannot open base page")
        return False
    token = extract_csrf_token(r0.content)

    # 2) POST /validateLogin (как на их странице)
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/",
    }
    if token:
        headers["X-CSRF-TOKEN"] = token

    r1 = request_post(
        s,
        f"{BASE_URL}/validateLogin",
        data={"login": VTT_LOGIN, "password": VTT_PASSWORD},
        headers=headers,
    )
    # Игнорируем тело ответа — просто проверим, что каталожная страница стала доступна
    r2 = request_get(s, START_URL)
    if not (r2 and good(r2)):
        print("Error: catalog not reachable after login")
        return False
    if is_login_page_bytes(r2.content):
        print("Error: still on login page (credentials?)")
        return False
    print("[login] success")
    return True

# ------------- PARSE -------------
def extract_names_urls_from_page(b: bytes) -> List[Tuple[str, str]]:
    s = soup_of(b)
    items: List[Tuple[str, str]] = []
    # точечный селектор под твою верстку: в блоке .cutoff-off второй <a> — товар
    for a in s.select('div.catalog_list_row div.cl_name .cutoff-off a[href*="/catalog/"]:not(.btn_naked)'):
        href = a.get("href") or ""
        url = urljoin(BASE_URL, href)
        title = (a.get("title") or a.get_text(" ", strip=True) or "").strip()
        if title:
            items.append((title, url))
    return items

def crawl_catalog(s: requests.Session) -> List[Tuple[str, str]]:
    deadline = datetime.utcnow() + timedelta(minutes=MAX_CRAWL_MINUTES)
    all_items: List[Tuple[str, str]] = []

    empty_streak = 0
    for page in range(1, MAX_PAGES + 1):
        if datetime.utcnow() > deadline:
            print("[crawl] deadline reached")
            break
        url = START_URL if page == 1 else f"{START_URL}?page={page}"
        r = request_get(s, url)
        if not (r and good(r)):
            empty_streak += 1
            if empty_streak >= 3:
                break
            continue
        if is_login_page_bytes(r.content):
            print("[crawl] session expired")
            break

        found = extract_names_urls_from_page(r.content)
        if found:
            all_items.extend(found)
            empty_streak = 0
        else:
            empty_streak += 1
            if empty_streak >= 3:
                break
        jitter_sleep(REQUEST_DELAY_MS)

    # Уникализируем по URL, затем по названию
    uniq: Dict[str, str] = {}
    for title, url in all_items:
        if url and url not in uniq:
            uniq[url] = title
    if not uniq:
        # fallback: по названию
        seen_titles = set()
        for title, url in all_items:
            if title not in seen_titles:
                seen_titles.add(title)
                uniq[url] = title
    items = [(t, u) for u, t in uniq.items()]
    print(f"[discover] unique names: {len(items)}")
    return items

# ------------- MAIN -------------
def main() -> int:
    if DISABLE_SSL_VERIFY:
        print("[ssl] verification disabled by env")

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

    s = make_session()
    if not login(s):
        # даже если логин не удался — пишем пустой каркас, чтобы GitHub Pages обновился
        xml = build_yml([])
        with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
            f.write(xml)
        return 2

    items = crawl_catalog(s)
    xml = build_yml(items)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write(xml)
    print(f"[done] items: {len(items)} -> {OUT_FILE}")
    return 0 if items else 1

if __name__ == "__main__":
    import sys
    try:
        sys.exit(main())
    except Exception as e:
        print("Error:", e)
        sys.exit(2)
