# -*- coding: utf-8 -*-
"""
B2B VTT → YML (names-only):
- Логин и проверка каталога.
- Извлечение названий с листингов по селектору div.cutoff-off > a[href] (+ доп. селекторы).
- Если BFS не дал результатов — план B: /catalog/?onPage=120&view=tile&page=N
- Формируем минимальный YML: только <name>, без цен и фото.

env:
  BASE_URL, START_URL, OUT_FILE, OUTPUT_ENCODING
  VTT_LOGIN, VTT_PASSWORD, (опц.) VTT_COOKIES
  DISABLE_SSL_VERIFY=1, ALLOW_SSL_FALLBACK=1
  HTTP_TIMEOUT, REQUEST_DELAY_MS, MIN_BYTES, MAX_PAGES, MAX_CRAWL_MINUTES, MAX_PLANB_PAGES
"""

from __future__ import annotations
import os, io, re, sys, time, html, hashlib, json
from typing import Optional, Dict, Any, List, Tuple, Set
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit, parse_qsl, urlencode, urldefrag

import requests
from bs4 import BeautifulSoup

# ------------ ENV ------------
BASE_URL        = os.getenv("BASE_URL", "https://b2b.vtt.ru").rstrip("/")
START_URL       = os.getenv("START_URL", f"{BASE_URL}/catalog/")
OUT_FILE        = os.getenv("OUT_FILE", "docs/vtt.yml")
OUTPUT_ENCODING = os.getenv("OUTPUT_ENCODING", "windows-1251")

VTT_LOGIN       = os.getenv("VTT_LOGIN", "").strip()
VTT_PASSWORD    = os.getenv("VTT_PASSWORD", "").strip()
VTT_COOKIES     = os.getenv("VTT_COOKIES", "").strip()

DISABLE_SSL_VERIFY = os.getenv("DISABLE_SSL_VERIFY", "0") == "1"
ALLOW_SSL_FALLBACK = os.getenv("ALLOW_SSL_FALLBACK", "1") == "1"
HTTP_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "25"))
REQUEST_DELAY_MS   = int(os.getenv("REQUEST_DELAY_MS", "180"))
MIN_BYTES          = int(os.getenv("MIN_BYTES", "200"))
MAX_PAGES          = int(os.getenv("MAX_PAGES", "900"))
MAX_CRAWL_MINUTES  = int(os.getenv("MAX_CRAWL_MINUTES", "60"))
MAX_PLANB_PAGES    = int(os.getenv("MAX_PLANB_PAGES", "60"))

DEBUG_DIR = "docs"
LOG_FILE  = os.path.join(DEBUG_DIR, "vtt_debug_log.txt")

# ------------ utils ------------
def ensure_dirs():
    os.makedirs(DEBUG_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

def dlog(msg: str):
    print(msg, flush=True)
    try:
        with io.open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def save_debug(name: str, content: bytes | str):
    path = os.path.join(DEBUG_DIR, name)
    try:
        if isinstance(content, (bytes, bytearray)):
            with open(path, "wb") as f:
                f.write(content)
        else:
            with open(path, "w", encoding="utf-8", errors="ignore") as f:
                f.write(content)
        dlog(f"[debug] saved {path}")
    except Exception as e:
        dlog(f"[debug] save failed {name}: {e}")

def jitter_sleep(ms: int):
    time.sleep(max(0.0, ms/1000.0))

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru,en;q=0.9",
        "Connection": "keep-alive",
    })
    if DISABLE_SSL_VERIFY:
        dlog("[ssl] verification disabled by env")
        s.verify = False
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
    return s

def http_get(sess: requests.Session, url: str) -> Optional[bytes]:
    try:
        r = sess.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if r.status_code != 200 or len(r.content) < MIN_BYTES:
            dlog(f"[http] GET bad {url} -> {r.status_code}, len={len(r.content)}")
            return None
        return r.content
    except requests.exceptions.SSLError as e:
        dlog(f"[http] SSL {url}: {e}")
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

def get_soup(b: bytes) -> BeautifulSoup:
    # lxml надёжнее для «грязного» HTML
    return BeautifulSoup(b, "lxml")

def inject_cookie_string(sess: requests.Session, cookie_string: str):
    host = urlparse(BASE_URL).hostname
    for part in cookie_string.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            sess.cookies.set(k.strip(), v.strip(), domain=host)

def extract_csrf(soup: BeautifulSoup) -> Optional[str]:
    m = soup.find("meta", attrs={"name": re.compile(r"csrf-token", re.I)})
    if m and m.get("content"):
        return m["content"].strip()
    inp = soup.find("input", attrs={"name": "_token"})
    if inp and inp.get("value"):
        return inp["value"].strip()
    return None

def guess_login_form(soup: BeautifulSoup) -> Tuple[str, Dict[str, str]]:
    for frm in soup.find_all("form"):
        txt = frm.get_text(" ", strip=True).lower()
        if any(w in txt for w in ["вход", "логин", "email", "почта", "пароль", "sign in", "login"]):
            action = frm.get("action") or "/login"
            fields = {}
            for inp in frm.find_all(["input","button"]):
                name = inp.get("name")
                if name:
                    fields[name] = inp.get("value") or ""
            return action, fields
    return "/login", {}

def login(sess: requests.Session) -> bool:
    # 1) cookies (если заданы)
    if VTT_COOKIES:
        inject_cookie_string(sess, VTT_COOKIES)
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, START_URL)
        if b:
            save_debug("vtt_debug_root_cookie.html", b)
            return True

    # 2) логин формой
    candidates = [
        f"{BASE_URL}/login",
        f"{BASE_URL}/signin",
        f"{BASE_URL}/auth/login",
        f"{BASE_URL}/account/login",
        f"{BASE_URL}/user/login",
        f"{BASE_URL}/",
    ]
    first_html = None
    first_url = None
    for u in candidates:
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, u)
        if not b:
            continue
        sp = get_soup(b)
        if sp.find("input", attrs={"type":"password"}) or sp.find(string=re.compile("парол", re.I)):
            first_html, first_url = b, u
            break
        if first_html is None:
            first_html, first_url = b, u

    if not first_html:
        dlog("Error: login page not found")
        return False

    save_debug("vtt_debug_login_get.html", first_html)
    soup = get_soup(first_html)
    csrf = extract_csrf(soup)
    action, _ = guess_login_form(soup)
    if not action.startswith("http"):
        action = urljoin(first_url, action)

    headers = {
        "Origin": BASE_URL,
        "Referer": first_url,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    xsrf = sess.cookies.get("XSRF-TOKEN")
    if xsrf:
        headers["X-XSRF-TOKEN"] = xsrf

    payload = {}
    if csrf: payload["_token"] = csrf

    for lk in ["email", "login", "username"]:
        for pk in ["password", "passwd", "pass"]:
            pl = dict(payload)
            pl[lk] = VTT_LOGIN
            pl[pk] = VTT_PASSWORD
            pl["remember"] = "1"
            try:
                jitter_sleep(REQUEST_DELAY_MS)
                r = sess.post(action, data=pl, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True)
            except requests.exceptions.SSLError:
                if not ALLOW_SSL_FALLBACK:
                    continue
                r = sess.post(action, data=pl, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True, verify=False)

            save_debug("vtt_debug_login_post.html", r.content)
            jitter_sleep(REQUEST_DELAY_MS)
            b2 = http_get(sess, START_URL)
            if b2:
                save_debug("vtt_debug_root_after_login.html", b2)
                dlog("[login] success (catalog reachable)")
                return True

    dlog("Error: login failed")
    return False

# ------------ crawl ------------
CANON_KEEP_KEYS = {
    "page", "onPage", "view", "sort",
    "category", "section", "slug", "cat", "cid", "id",
}
DROP_EXTS = re.compile(r"\.(jpg|jpeg|png|gif|svg|webp|pdf|docx?|xlsx?)$", re.I)

def canonicalize(u: str) -> str:
    parts = urlsplit(u)
    q = parse_qsl(parts.query, keep_blank_values=True)
    keep = []
    for k, v in q:
        if k in CANON_KEEP_KEYS or re.search(r"(cat|slug|id)", k, re.I):
            keep.append((k, v))
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), urlencode(keep, doseq=True), ""))

def same_host(u: str) -> bool:
    try:
        return urlparse(u).netloc == urlparse(BASE_URL).netloc
    except Exception:
        return False

def normalize_link(base: str, href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith(("javascript:", "mailto:")):
        return None
    absu = urljoin(base, href)
    absu, _ = urldefrag(absu)
    if not same_host(absu):
        return None
    if "/catalog" not in urlparse(absu).path:
        return None
    if DROP_EXTS.search(absu):
        return None
    return canonicalize(absu)

def extract_links_generic(soup: BeautifulSoup, base: str) -> List[str]:
    urls: List[str] = []

    # обычные ссылки
    for a in soup.find_all("a", href=True):
        nu = normalize_link(base, a["href"])
        if nu: urls.append(nu)

    # data-* ссылки
    for attr in ["data-href", "data-url", "data-link", "data-target", "data-path"]:
        for el in soup.find_all(attrs={attr: True}):
            nu = normalize_link(base, el.get(attr))
            if nu: urls.append(nu)

    # onclick: location.href='...'
    for el in soup.find_all(attrs={"onclick": True}):
        oc = el.get("onclick") or ""
        m = re.search(r"location\.(?:href|assign)\(['\"]([^'\"]+)['\"]\)", oc)
        if m:
            nu = normalize_link(base, m.group(1))
            if nu: urls.append(nu)

    # rel=next
    ln = soup.find("link", rel=re.compile(r"next", re.I))
    if ln and ln.get("href"):
        nu = normalize_link(base, ln["href"])
        if nu: urls.append(nu)

    # уникализация
    return list(dict.fromkeys(urls))

def clean_text(t: str) -> str:
    t = re.sub(r"\s{2,}", " ", t or "").strip()
    return t

# ИМЕНА ТОЛЬКО ИЗ БЛОКОВ div.cutoff-off > a[href]
def extract_names_from_listing(soup: BeautifulSoup) -> List[str]:
    names: List[str] = []

    # 1) Точный блок из вашего примера:
    for a in soup.select("div.cutoff-off a[href]"):
        # первый <a> обычно без href (камера), а мы берём только с href
        txt = clean_text(a.get_text(" ", strip=True))
        href = a.get("href") or ""
        if not txt or len(txt) < 3:
            continue
        if "/catalog/" not in href:
            continue
        names.append(txt)

    # 2) Доп. подстраховка: похожие классы с пробелами/модификаторами
    if not names:
        for a in soup.select("[class*='cutoff-off'] a[href]"):
            txt = clean_text(a.get_text(" ", strip=True))
            href = a.get("href") or ""
            if txt and "/catalog/" in href and txt.lower() not in ("в корзину","купить","подробнее"):
                names.append(txt)

    # 3) Ещё один резерв (некоторые листинги могут иметь title в атрибуте)
    if not names:
        for a in soup.select("a[href*='/catalog/']"):
            title_attr = (a.get("title") or "").strip()
            if title_attr and len(title_attr) > 3:
                names.append(title_attr)

    # дедуп
    uniq = list(dict.fromkeys(names))
    return uniq

def crawl_collect_names(sess: requests.Session, start_url: str) -> List[str]:
    queue: List[str] = [start_url]
    seen: Set[str] = set()
    names_all: List[str] = []

    t0 = time.time()
    pages = 0

    while queue and pages < MAX_PAGES and (time.time() - t0) < MAX_CRAWL_MINUTES * 60:
        url = queue.pop(0)
        url = canonicalize(url)
        if url in seen:
            continue
        seen.add(url)

        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, url)
        if not b:
            continue
        soup = get_soup(b)

        if pages == 0:
            save_debug("vtt_debug_root.html", b)
        if pages < 10:
            save_debug(f"vtt_page_listing_{pages+1}.html", b)

        # имена
        names = extract_names_from_listing(soup)
        if names:
            names_all.extend(names)

        # ссылки глубже (категории/пагинация)
        for nu in extract_links_generic(soup, url):
            if nu not in seen and nu not in queue:
                queue.append(nu)

        pages += 1

        # если уже нашли много — можно остановиться по времени
        if len(names_all) > 5000:
            break

    dlog(f"[discover] BFS pages: {pages}, names_collected: {len(names_all)}")
    uniq = list(dict.fromkeys(names_all))
    dlog(f"[discover] unique names: {len(uniq)}")
    return uniq

def plan_b_collect(sess: requests.Session) -> List[str]:
    """Прямая пагинация общего каталога: /catalog/?onPage=120&view=tile&page=N"""
    names_all: List[str] = []
    base = f"{BASE_URL}/catalog/?onPage=120&view=tile"
    empty_in_row = 0
    for n in range(1, MAX_PLANB_PAGES + 1):
        url = f"{base}&page={n}"
        jitter_sleep(REQUEST_DELAY_MS)
        b = http_get(sess, url)
        if not b:
            empty_in_row += 1
            if empty_in_row >= 3:
                break
            continue
        empty_in_row = 0
        if n <= 5:
            save_debug(f"vtt_planb_{n}.html", b)
        soup = get_soup(b)
        names = extract_names_from_listing(soup)
        if not names:
            # вероятно, пагинация закончилась
            if n > 2:
                break
            else:
                continue
        names_all.extend(names)
    uniq = list(dict.fromkeys(names_all))
    dlog(f"[planB] pages tried: {MAX_PLANB_PAGES}, names: {len(uniq)}")
    return uniq

# ------------ YML (минимальный) ------------
def yml_escape(s: str) -> str:
    return html.escape(s or "")

def write_yml_with_names(names: List[str]):
    lines = []
    lines.append("<?xml version='1.0' encoding='windows-1251'?>")
    lines.append("<yml_catalog><shop>")
    lines.append("<name>vtt</name>")
    lines.append('<currencies><currency id="RUB" rate="1" /></currencies>')
    lines.append("<categories>")
    lines.append('<category id="9600000">VTT</category>')
    lines.append("</categories>")
    lines.append("<offers>")
    for n in names:
        oid = hashlib.md5(n.encode("utf-8")).hexdigest()[:12]
        lines.append(f'<offer id="{oid}" available="true" in_stock="true">')
        lines.append(f"<name>{yml_escape(n)}</name>")
        lines.append("<price>0</price>")
        lines.append("<currencyId>RUB</currencyId>")
        lines.append("<categoryId>9600000</categoryId>")
        lines.append("</offer>")
    lines.append("</offers>")
    lines.append("</shop></yml_catalog>")

    with open(OUT_FILE, "w", encoding="cp1251", errors="ignore") as f:
        f.write("\n".join(lines))
    dlog(f"[done] items: {len(names)} -> {OUT_FILE}")

# ------------ MAIN ------------
def main() -> int:
    ensure_dirs()
    sess = make_session()
    if not login(sess):
        dlog("Error: login failed")
        write_yml_with_names([])
        return 2

    names = crawl_collect_names(sess, START_URL)
    if not names:
        dlog("[info] BFS yielded 0 names, trying Plan B pagination…")
        names = plan_b_collect(sess)

    write_yml_with_names(names)
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        dlog(f"[fatal] {e}")
        try:
            write_yml_with_names([])
        except Exception:
            pass
        sys.exit(2)
