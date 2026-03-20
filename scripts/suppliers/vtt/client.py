# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/client.py

VTT probe client.
v4:
- логин повторяет старый рабочий flow из source.py:
  GET "/" -> meta csrf-token -> POST "/validateLogin" -> GET "/catalog/".
- сохраняет подробный login_report для диагностики.
- оставляет crawl helpers для временного probe-этапа.
"""

from __future__ import annotations

import re
import time
from html import unescape
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


_SCRIPT_ENDPOINT_RE = re.compile(
    r"""(?:
        https?://[^\s"'<>]+
        |
        /[A-Za-z0-9_\-./?=&%]+
    )""",
    re.X,
)


class VTTClient:
    """Сессия VTT + точный login-flow + аккуратный обход."""

    def __init__(
        self,
        base_url: str,
        login: str,
        password: str,
        *,
        timeout: int = 30,
        delay_seconds: float = 0.35,
        user_agent: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.login = login
        self.password = password
        self.timeout = timeout
        self.delay_seconds = delay_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent
                or (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120 Safari/537.36"
                ),
                "Accept-Language": "ru,en;q=0.8",
            }
        )

    def _sleep(self) -> None:
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)

    def get(self, url: str, **kwargs) -> requests.Response:
        self._sleep()
        resp = self.session.get(url, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp

    def post(self, url: str, **kwargs) -> requests.Response:
        self._sleep()
        resp = self.session.post(url, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp

    @staticmethod
    def _snippet(html: str, limit: int = 600) -> str:
        text = BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:limit]

    @staticmethod
    def _extract_csrf_from_meta(html: str) -> str:
        soup = BeautifulSoup(html or "", "html.parser")
        meta = soup.find("meta", attrs={"name": "csrf-token"})
        return ((meta.get("content") if meta else "") or "").strip()

    @staticmethod
    def _extract_hidden_token(html: str) -> str:
        soup = BeautifulSoup(html or "", "html.parser")
        for name in ("_token", "csrf-token", "csrf_token"):
            inp = soup.find("input", attrs={"name": name})
            if inp and (inp.get("value") or "").strip():
                return (inp.get("value") or "").strip()
        return ""

    def _looks_authenticated(self, html: str, current_url: str) -> bool:
        text = (html or "").lower()
        path = urlparse(current_url).path.lower()
        if "/catalog" in path and "вход для клиентов" not in text:
            return True
        protected_markers = [
            "каталог",
            "корзина",
            "заказы",
            "личный кабинет",
            "выход",
        ]
        if any(marker in text for marker in protected_markers) and "вход для клиентов" not in text:
            return True
        return False

    def login_and_verify(self) -> dict[str, object]:
        attempts: list[dict[str, object]] = []
        home_url = self.abs_url("/")
        login_url = self.abs_url("/validateLogin")
        catalog_url = self.abs_url("/catalog/")

        if not self.login or not self.password:
            return {
                "ok": False,
                "base_url": self.base_url,
                "attempts": [{"stage": "precheck", "status": "missing_credentials"}],
                "cookies": self.session.cookies.get_dict(),
                "message": "Missing VTT_LOGIN or VTT_PASSWORD.",
            }

        try:
            home_resp = self.get(home_url, allow_redirects=True)
            home_html = home_resp.text
            csrf = self._extract_csrf_from_meta(home_html)
            hidden_token = self._extract_hidden_token(home_html)
            token = csrf or hidden_token

            attempts.append(
                {
                    "stage": "open_home",
                    "status": "ok",
                    "request_url": home_url,
                    "response_url": home_resp.url,
                    "http_status": home_resp.status_code,
                    "csrf_meta_found": bool(csrf),
                    "hidden_token_found": bool(hidden_token),
                    "cookies_after_home": self.session.cookies.get_dict(),
                    "page_snippet": self._snippet(home_html),
                }
            )

            headers = {"Referer": home_url}
            if token:
                headers["X-CSRF-TOKEN"] = token

            payload = {
                "login": self.login,
                "password": self.password,
            }

            self._sleep()
            login_resp = self.session.post(
                login_url,
                data=payload,
                headers=headers,
                timeout=self.timeout,
                allow_redirects=True,
            )

            attempts.append(
                {
                    "stage": "submit_login",
                    "status": "ok" if login_resp.status_code in (200, 204) else "bad_status",
                    "submit_url": login_url,
                    "http_status": login_resp.status_code,
                    "response_url": login_resp.url,
                    "history": [r.status_code for r in login_resp.history],
                    "payload_keys": sorted(payload.keys()),
                    "headers_sent": sorted(headers.keys()),
                    "response_snippet": self._snippet(login_resp.text),
                    "cookies_after_submit": self.session.cookies.get_dict(),
                }
            )

            # Точный старый flow: после POST идём проверять каталог.
            try:
                catalog_resp = self.get(catalog_url, allow_redirects=True)
                catalog_ok = self._looks_authenticated(catalog_resp.text, catalog_resp.url)
                attempts.append(
                    {
                        "stage": "probe_catalog",
                        "status": "auth_ok" if catalog_ok else "auth_failed",
                        "probe_url": catalog_url,
                        "http_status": catalog_resp.status_code,
                        "response_url": catalog_resp.url,
                        "page_snippet": self._snippet(catalog_resp.text),
                        "cookies_after_probe": self.session.cookies.get_dict(),
                    }
                )
                if catalog_ok:
                    return {
                        "ok": True,
                        "base_url": self.base_url,
                        "landing_url": catalog_resp.url,
                        "catalog_url": catalog_url,
                        "attempts": attempts,
                        "cookies": self.session.cookies.get_dict(),
                        "message": "Login OK via legacy source.py flow.",
                    }
            except Exception as exc:  # noqa: BLE001
                attempts.append(
                    {
                        "stage": "probe_catalog",
                        "status": "error",
                        "probe_url": catalog_url,
                        "error": str(exc),
                        "cookies_after_probe": self.session.cookies.get_dict(),
                    }
                )

            return {
                "ok": False,
                "base_url": self.base_url,
                "landing_url": "",
                "catalog_url": catalog_url,
                "attempts": attempts,
                "cookies": self.session.cookies.get_dict(),
                "message": "Login failed on catalog probe.",
            }

        except Exception as exc:  # noqa: BLE001
            attempts.append(
                {
                    "stage": "open_home",
                    "status": "error",
                    "request_url": home_url,
                    "error": str(exc),
                    "cookies_after_home": self.session.cookies.get_dict(),
                }
            )
            return {
                "ok": False,
                "base_url": self.base_url,
                "attempts": attempts,
                "cookies": self.session.cookies.get_dict(),
                "message": "Login flow crashed before catalog probe.",
            }

    def crawl_same_host(
        self,
        start_urls: Iterable[str],
        *,
        max_pages: int = 500,
        allow_paths: list[str] | None = None,
        deny_patterns: list[str] | None = None,
    ) -> list[dict[str, object]]:
        """Широкий обход того же хоста."""
        queue: list[str] = []
        seen: set[str] = set()
        results: list[dict[str, object]] = []
        base_host = urlparse(self.base_url).netloc

        for url in start_urls:
            abs_url = self.abs_url(url)
            if abs_url not in seen:
                queue.append(abs_url)
                seen.add(abs_url)

        deny_patterns = deny_patterns or [
            r"/logout",
            r"/signout",
            r"/basket",
            r"/cart",
            r"/order",
            r"/checkout",
        ]

        while queue and len(results) < max_pages:
            url = queue.pop(0)
            parsed = urlparse(url)
            if parsed.netloc != base_host:
                continue
            if allow_paths and not any(parsed.path.startswith(p) for p in allow_paths):
                pass

            if any(re.search(p, url, re.I) for p in deny_patterns):
                continue

            try:
                resp = self.get(url, allow_redirects=True)
                ctype = (resp.headers.get("Content-Type") or "").lower()
                if "text/html" not in ctype:
                    results.append(
                        {
                            "url": resp.url,
                            "status_code": resp.status_code,
                            "content_type": ctype,
                            "kind": "non_html",
                        }
                    )
                    continue

                html = resp.text
                page = {
                    "url": resp.url,
                    "status_code": resp.status_code,
                    "content_type": ctype,
                    "title": self.extract_title(html),
                    "kind": self.classify_page(resp.url, html),
                    "links": [],
                    "api_like_links": self.extract_endpoint_like_strings(html, resp.url),
                    "html": html,
                }
                links = self.extract_links(resp.url, html)
                page["links"] = links
                results.append(page)

                for link in links:
                    if link in seen:
                        continue
                    parsed_link = urlparse(link)
                    if parsed_link.netloc != base_host:
                        continue
                    if parsed_link.scheme not in {"http", "https"}:
                        continue
                    if any(re.search(p, link, re.I) for p in deny_patterns):
                        continue
                    seen.add(link)
                    queue.append(link)

            except Exception as exc:  # noqa: BLE001
                results.append(
                    {
                        "url": url,
                        "kind": "error",
                        "error": str(exc),
                    }
                )

        return results

    def abs_url(self, url: str) -> str:
        return urljoin(self.base_url, url)

    @staticmethod
    def extract_title(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        if soup.title and soup.title.text:
            return " ".join(soup.title.text.split())
        h1 = soup.find("h1")
        if h1:
            return " ".join(h1.get_text(" ", strip=True).split())
        return ""

    def extract_links(self, page_url: str, html: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        out: list[str] = []
        seen: set[str] = set()

        for tag in soup.find_all(["a", "link"], href=True):
            href = (tag.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            abs_url = urljoin(page_url, href)
            abs_url = self._normalize_url(abs_url)
            if urlparse(abs_url).netloc != urlparse(self.base_url).netloc:
                continue
            if abs_url not in seen:
                seen.add(abs_url)
                out.append(abs_url)

        return out

    def extract_endpoint_like_strings(self, html: str, page_url: str) -> list[str]:
        found: list[str] = []
        seen: set[str] = set()

        for raw in _SCRIPT_ENDPOINT_RE.findall(unescape(html)):
            raw = raw.strip()
            if not raw:
                continue
            if raw.startswith("http://") or raw.startswith("https://"):
                url = raw
            elif raw.startswith("/"):
                url = urljoin(page_url, raw)
            else:
                continue
            url = self._normalize_url(url)
            if urlparse(url).netloc != urlparse(self.base_url).netloc:
                continue
            if url not in seen:
                seen.add(url)
                found.append(url)

        return found[:300]

    @staticmethod
    def classify_page(url: str, html: str) -> str:
        lower_url = url.lower()
        lower_html = html.lower()
        if re.search(r"(product|item|goods|catalog/.+/[^/]+)", lower_url):
            return "product_like"
        if "add to cart" in lower_html or "в корзину" in lower_html:
            return "product_like"
        if re.search(r"(catalog|category|group)", lower_url):
            return "category_like"
        if "<table" in lower_html and ("цена" in lower_html or "price" in lower_html):
            return "product_like"
        return "other"

    @staticmethod
    def _normalize_url(url: str) -> str:
        parsed = urlparse(url)
        clean = parsed._replace(fragment="")
        return clean.geturl()
