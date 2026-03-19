# -*- coding: utf-8 -*-
"""VTT source layer — session, login, requests."""

from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlencode, urlparse, urlunparse
from bs4 import BeautifulSoup
import requests


_DEFAULT_CATEGORIES: list[str] = [
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_COMPAT",
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_ORIG",
    "https://b2b.vtt.ru/catalog/?category=CARTINJ_PRNTHD",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_COMPAT",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_COPY",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_ORIG",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_PRINT",
    "https://b2b.vtt.ru/catalog/?category=CARTLAS_TNR",
    "https://b2b.vtt.ru/catalog/?category=CARTMAT_CART",
    "https://b2b.vtt.ru/catalog/?category=DEV_DEV",
    "https://b2b.vtt.ru/catalog/?category=DRM_CRT",
    "https://b2b.vtt.ru/catalog/?category=DRM_UNIT",
    "https://b2b.vtt.ru/catalog/?category=PARTSPRINT_THERBLC",
    "https://b2b.vtt.ru/catalog/?category=PARTSPRINT_THERELT",
]


@dataclass(frozen=True)
class VttCfg:
    base_url: str
    start_url: str
    categories: list[str]
    login: str
    password: str
    max_pages: int
    max_workers: int
    max_crawl_minutes: float
    delay_ms: int
    verify: object
    softfail: bool


def log(msg: str) -> None:
    print(msg, flush=True)


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name, "") or "").strip() or str(default))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name, "") or "").strip() or str(default))
    except Exception:
        return default


def cfg_from_env() -> VttCfg:
    base = (os.getenv("VTT_BASE_URL", "https://b2b.vtt.ru") or "").strip().rstrip("/")
    start = (os.getenv("VTT_START_URL", f"{base}/catalog/") or "").strip()
    cats_raw = (os.getenv("VTT_CATEGORIES", "") or "").strip()
    cats = [c.strip() for c in cats_raw.split(",") if c.strip()] if cats_raw else list(_DEFAULT_CATEGORIES)

    login = (os.getenv("VTT_LOGIN", "") or "").strip()
    password = (os.getenv("VTT_PASSWORD", "") or "").strip()

    ssl_verify = _env_bool("VTT_SSL_VERIFY", True)
    ca_bundle = (os.getenv("VTT_CA_BUNDLE", "") or "").strip()
    verify: object = ca_bundle if ca_bundle else ssl_verify

    return VttCfg(
        base_url=base,
        start_url=start,
        categories=cats,
        login=login,
        password=password,
        max_pages=_env_int("VTT_MAX_PAGES", 200),
        max_workers=_env_int("VTT_MAX_WORKERS", 10),
        max_crawl_minutes=_env_float("VTT_MAX_CRAWL_MINUTES", 18.0),
        delay_ms=_env_int("VTT_REQUEST_DELAY_MS", 80),
        verify=verify,
        softfail=_env_bool("VTT_SOFTFAIL", True),
    )


def sleep_ms(ms: int) -> None:
    if ms <= 0:
        return
    time.sleep((ms / 1000.0) * random.uniform(0.75, 1.35))


def make_session(cfg: VttCfg) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Accept-Language": "ru,en;q=0.8",
        }
    )
    return s


def request(s: requests.Session, cfg: VttCfg, method: str, url: str, *, timeout: int = 25, data: dict | None = None, headers: dict | None = None) -> requests.Response | None:
    tries = 7
    for i in range(tries):
        try:
            r = s.request(
                method=method,
                url=url,
                data=data,
                headers=headers,
                timeout=timeout,
                verify=cfg.verify,
                allow_redirects=True,
            )
            if r.status_code in (500, 502, 503, 504):
                raise requests.HTTPError(f"{r.status_code}")
            return r
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            last = (i == tries - 1)
            log(f"[http] {method} {url} fail: {e}{' (last)' if last else ''}")
            if last:
                return None
            time.sleep(min(12.0, 0.6 * (2**i)) + random.uniform(0.0, 0.6))
    return None


def get_bytes(s: requests.Session, cfg: VttCfg, url: str, *, timeout: int = 25) -> bytes | None:
    r = request(s, cfg, "GET", url, timeout=timeout)
    if not r or r.status_code != 200:
        return None
    sleep_ms(cfg.delay_ms)
    return r.content


def post_ok(s: requests.Session, cfg: VttCfg, url: str, *, data: dict, headers: dict | None = None, timeout: int = 25) -> bool:
    r = request(s, cfg, "POST", url, timeout=timeout, data=data, headers=headers)
    ok = bool(r and r.status_code in (200, 204))
    sleep_ms(cfg.delay_ms)
    return ok


def abs_url(cfg: VttCfg, href: str) -> str:
    u = (href or "").strip()
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if not u.startswith("/"):
        u = "/" + u
    return cfg.base_url + u


def set_q(url: str, key: str, value: str) -> str:
    pu = urlparse(url)
    from urllib.parse import parse_qs
    q = parse_qs(pu.query)
    q[key] = [value]
    return urlunparse(pu._replace(query=urlencode(q, doseq=True)))


def soup_from_bytes(html_bytes: bytes) -> BeautifulSoup:
    return BeautifulSoup(html_bytes, "html.parser")


def extract_csrf_token(html_bytes: bytes) -> str:
    sp = soup_from_bytes(html_bytes)
    m = sp.find("meta", attrs={"name": "csrf-token"})
    return ((m.get("content") if m else "") or "").strip()


def login(s: requests.Session, cfg: VttCfg) -> bool:
    if not cfg.login or not cfg.password:
        log("[WARN] VTT_LOGIN/VTT_PASSWORD пустые")
        return False

    home = get_bytes(s, cfg, cfg.base_url + "/")
    if not home:
        return False

    csrf = extract_csrf_token(home)
    headers = {"Referer": cfg.base_url + "/"}
    if csrf:
        headers["X-CSRF-TOKEN"] = csrf

    ok = post_ok(
        s,
        cfg,
        cfg.base_url + "/validateLogin",
        data={"login": cfg.login, "password": cfg.password},
        headers=headers,
    )
    if not ok:
        return False

    cat = get_bytes(s, cfg, cfg.start_url)
    return bool(cat)


def clone_session_with_cookies(src: requests.Session, cfg: VttCfg) -> requests.Session:
    s2 = make_session(cfg)
    try:
        s2.cookies.update(src.cookies)
    except Exception:
        pass
    return s2
