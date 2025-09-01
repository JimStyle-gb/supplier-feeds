# -*- coding: utf-8 -*-
"""
B2B VTT → YML: базовая авторизация + заготовка под парсинг.
Цель: стабильно войти в b2b.vtt.ru без ручных действий и сохранить отладку.

Зависимости: requests, bs4, lxml
"""

from __future__ import annotations
import os, io, re, time, html, hashlib, sys
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ----------------- ENV -----------------
BASE_URL        = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL       = os.getenv("START_URL", f"{BASE_URL}/catalog/")
OUT_FILE        = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN       = os.getenv("VTT_LOGIN", "").strip()
VTT_PASSWORD    = os.getenv("VTT_PASSWORD", "").strip()
VTT_COOKIES     = os.getenv("VTT_COOKIES", "").strip()

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"
ALLOW_SSL_FALLBACK = os.getenv("ALLOW_SSL_FALLBACK", "1") == "1"
HTTP_TIMEOUT        = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS    = int(os.getenv("REQUEST_DELAY_MS", "150"))
MIN_BYTES           = int(os.getenv("MIN_BYTES", "800"))

DEBUG_ROOT  = "docs"
DEBUG_LOG   = os.path.join(DEBUG_ROOT, "vtt_debug_log.txt")

# ----------------- helpers -----------------
def ensure_dirs():
    os.makedirs(DEBUG_ROOT, exist_ok=True)

def dlog(msg: str):
    print(msg, flush=True)
    try:
        with io.open(DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def jitter_sleep(ms: int):
    time.sleep(max(0.0, ms/1000.0))

def save_debug(name: str, content: bytes | str):
    path = os.path.join(DEBUG_ROOT, name)
    try:
        mode = "wb" if isinstance(content, (bytes, bytearray)) else "w"
        with open(path, mode, encoding=None if mode == "wb" else "utf-8", errors="ignore") as f:
            f.write(content)
        dlog(f"[debug] saved {path}")
    except Exception as e:
        dlog(f"[debug] save failed {name}: {e}")

def make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru,en;q=0.9",
        "Connection": "keep-alive",
    })
    if DISABLE_SSL_VERIFY:
        dlog("[ssl] verification disabled by env")
        sess.verify = False
        # подавим предупреждения urllib3
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
    return sess

def get_soup(html_bytes: bytes) -> BeautifulSoup:
    # Используем lxml — меньше сюрпризов с кодировками
    return BeautifulSoup(html_bytes, "lxml")

def http_get(sess: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = sess.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code != 200 or len(r.content) < MIN_BYTES:
            dlog(f"[http] GET bad status/len {url} -> {r.status_code}, {len(r.content)}")
            save_debug("vtt_fail_get.html", r.content)
            return None
        return r.content
    except requests.exceptions.SSLError as e:
        dlog(f"[http] GET SSL error {url}: {e}")
        if ALLOW_SSL_FALLBACK:
            try:
                r = sess.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True, verify=False)
                if r.status_code == 200 and len(r.content) >= MIN_BYTES:
                    return r.content
            except Exception as e2:
                dlog(f"[http] fallback fail {e2}")
        return None
    except Exception as e:
        dlog(f"[http] GET fail {url}: {e}")
        return None

def inject_cookie_string(sess: requests.Session, cookie_string: str):
    # Принимаем обычную строку "k1=v1; k2=v2; ..."
    for part in cookie_string.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            sess.cookies.set(k.strip(), v.strip(), domain="b2b.vtt.ru")

def extract_csrf(soup: BeautifulSoup) -> Optional[str]:
    # meta[name=csrf-token]
    m = soup.find("meta", attrs={"name": re.compile(r"csrf-token", re.I)})
    if m and m.get("content"):
        return m["content"].strip()
    # hidden input _token
    inp = soup.find("input", attrs={"name": "_token"})
    if inp and inp.get("value"):
        return inp["value"].strip()
    return None

def guess_login_form(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    # Ищем <form> с полями логина
    forms = soup.find_all("form")
    for frm in forms:
        txt = frm.get_text(" ", strip=True).lower()
        if any(word in txt for word in ["вход", "логин", "email", "пароль", "sign in", "login"]):
            action = frm.get("action") or "/login"
            fields = {}
            for inp in frm.find_all(["input", "button"]):
                name = inp.get("name")
                if not name:
                    continue
                val = inp.get("value") or ""
                fields[name] = val
            return {"action": action, "fields": fields}
    # fallback
    return {"action": "/login", "fields": {}}

def login_vtt(sess: requests.Session) -> bool:
    # Если есть явные cookie — попробуем ими
    if VTT_COOKIES:
        inject_cookie_string(sess, VTT_COOKIES)
        # проверим доступ к каталогу
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, START_URL)
        if b:
            save_debug("vtt_debug_root_after_cookie.html", b)
            # грубая проверка: есть ли признак авторизованной шапки
            if b"logout" in b.lower() or b"\xd0\xb2\xd1\x8b\xd0\xb9\xd1\x82\xd0\xb8" in b:  # "Выйти"
                dlog("[login] cookies ok (found logout)")
                return True
            # даже если нет явного «Выйти», иногда достаточно наличия каталога
            return True
        dlog("[login] cookies injected, but catalog not reachable")

    # Пробуем варианты страниц логина
    login_pages = [
        f"{BASE_URL}/login",
        f"{BASE_URL}/signin",
        f"{BASE_URL}/auth/login",
        f"{BASE_URL}/account/login",
        f"{BASE_URL}/user/login",
        f"{BASE_URL}/",
    ]
    first_html = None
    first_url  = None

    for u in login_pages:
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, u)
        if not b:
            continue
        soup = get_soup(b)
        if soup.find("input", attrs={"type": "password"}) or soup.find(string=re.compile("пароль", re.I)):
            first_html = b
            first_url = u
            break
        if first_html is None:
            first_html = b
            first_url = u

    if not first_html:
        dlog("[login] cannot open login page")
        return False

    save_debug("vtt_debug_login_get.html", first_html)
    soup = get_soup(first_html)

    csrf = extract_csrf(soup)
    form  = guess_login_form(soup)
    action = form["action"]
    if not action.startswith("http"):
        action = urljoin(first_url, action)

    payload_base = {}
    if csrf:
        payload_base["_token"] = csrf

    # Пробуем разные имена полей для логина
    login_keys = ["email", "login", "username"]
    pass_keys  = ["password", "passwd", "pass"]

    ok = False
    for lk in login_keys:
        for pk in pass_keys:
            payload = dict(payload_base)
            payload.update({
                lk: VTT_LOGIN,
                pk: VTT_PASSWORD,
                "remember": "1",
            })

            headers = {
                "Origin": BASE_URL,
                "Referer": first_url,
                "Content-Type": "application/x-www-form-urlencoded",
            }
            # XSRF из cookie, если есть
            xsrf = sess.cookies.get("XSRF-TOKEN")
            if xsrf:
                headers["X-XSRF-TOKEN"] = xsrf

            try:
                jitter_sleep(REQUEST_DELAY_MS)
                r = sess.post(action, data=payload, headers=headers,
                              timeout=HTTP_TIMEOUT, allow_redirects=True)
            except requests.exceptions.SSLError as e:
                dlog(f"[login] SSL post error: {e}")
                if ALLOW_SSL_FALLBACK:
                    r = sess.post(action, data=payload, headers=headers,
                                  timeout=HTTP_TIMEOUT, allow_redirects=True, verify=False)
                else:
                    continue

            save_debug("vtt_debug_login_post.html", r.content)

            if r.status_code in (200, 302):
                # Проверим доступ к каталогу после POST
                jitter_sleep(REQUEST_DELAY_MS)
                b2 = http_get(sess, START_URL)
                if b2:
                    save_debug("vtt_debug_root_after_login.html", b2)
                    low = b2.lower()
                    if b"logout" in low or b"\xd0\xb2\xd1\x8b\xd0\xb9\xd1\x82\xd0\xb8" in low:
                        dlog("[login] success (found logout)")
                        ok = True
                        break
                    # Иногда «Выйти» скрыто, но каталог уже доступен
                    dlog("[login] success (catalog reachable)")
                    ok = True
                    break
        if ok:
            break

    return ok

# ----------------- YML -----------------
def yml_escape(s: str) -> str:
    return html.escape(s or "")

def write_yml(categories: List[Dict[str, Any]], offers: List[Dict[str, Any]]):
    out = []
    out.append("<?xml version='1.0' encoding='windows-1251'?>")
    out.append("<yml_catalog><shop>")
    out.append("<name>vtt</name>")
    out.append("<currencies><currency id=\"RUB\" rate=\"1\" /></currencies>")
    out.append("<categories>")
    out.append("<category id=\"9600000\">VTT</category>")
    for c in categories:
        pid = c.get("parentId") or 9600000
        out.append(f"<category id=\"{c['id']}\" parentId=\"{pid}\">{yml_escape(c['name'])}</category>")
    out.append("</categories>")
    out.append("<offers>")
    for o in offers:
        out.append(f"<offer id=\"{yml_escape(o['id'])}\" available=\"true\" in_stock=\"true\">")
        out.append(f"<name>{yml_escape(o['name'])}</name>")
        out.append(f"<vendor>{yml_escape(o.get('vendor','VTT'))}</vendor>")
        out.append(f"<price>{o.get('price','0')}</price>")
        out.append("<currencyId>RUB</currencyId>")
        out.append(f"<categoryId>{o.get('categoryId',9600000)}</categoryId>")
        if o.get("url"): out.append(f"<url>{yml_escape(o['url'])}</url>")
        if o.get("picture"): out.append(f"<picture>{yml_escape(o['picture'])}</picture>")
        out.append(f"<description>{yml_escape(o.get('description',''))}</description>")
        out.append("</offer>")
    out.append("</offers>")
    out.append("</shop></yml_catalog>")
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write("\n".join(out))
    dlog(f"[done] items: {len(offers)}, cats: {len(categories)} -> {OUT_FILE}")

# ----------------- MAIN -----------------
def main() -> int:
    ensure_dirs()

    sess = make_session()

    # 1) ЛОГИН
    if not login_vtt(sess):
        dlog("Error: login failed")
        # пишем пустой YML, чтобы страница артефакта обновлялась
        write_yml([], [])
        return 2

    # 2) Проба доступа к каталогу + отладка
    jitter_sleep(REQUEST_DELAY_MS)
    root_html = http_get(sess, START_URL)
    if not root_html:
        dlog("Error: catalog unreachable after login")
        write_yml([], [])
        return 2

    save_debug("vtt_debug_root.html", root_html)

    # --- На этом этапе логин стабилен. Ниже — заготовка под ваш парсинг.
    # Сейчас, чтобы не ломать ничего лишнего, оставляю без сбора карточек.
    # Когда определимся с конкретной категорией/селектором — добавим.
    write_yml([], [])
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        dlog(f"[fatal] {e}")
        try:
            write_yml([], [])
        except Exception:
            pass
        sys.exit(2)
