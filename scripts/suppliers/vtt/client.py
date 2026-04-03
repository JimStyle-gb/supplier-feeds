# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/client.py

VTT compatibility client shim.

Назначение файла на переходном этапе:
- НЕ быть вторым источником правды по login-flow;
- использовать source.py как основной рабочий слой;
- сохранить backward-safe public API для точечных probe/ручных проверок,
  пока старый client.py окончательно не будет удалён из supplier-package.

Важно:
- основная build-цепочка должна жить через source.py;
- этот файл оставляем только как compatibility/probe shim;
- после стабилизации VTT его можно будет удалить.
"""

from __future__ import annotations

import re
import time
from html import unescape
from types import SimpleNamespace
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .source import login as source_login
from .source import make_session as source_make_session


_SCRIPT_ENDPOINT_RE = re.compile(
    r"""(?:
        https?://[^\s"'<>]+
        |
        /[A-Za-z0-9_\-./?=&%]+
    )""",
    re.X,
)


class VTTClient:
    """Backward-safe probe client поверх source.py."""

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
        self.timeout = int(timeout)
        self.delay_seconds = float(delay_seconds)
        self.user_agent = user_agent

        # Источник правды по сессии теперь source.py.
        self._cfg = SimpleNamespace(
            base_url=self.base_url,
            start_url=urljoin(self.base_url, "/catalog/"),
            login_url=urljoin(self.base_url, "/validateLogin"),
            login=self.login,
            password=self.password,
            timeout_s=self.timeout,
            listing_request_delay_ms=max(0, int(round(self.delay_seconds * 1000))),
        )
        self.session: requests.Session = source_make_session(self._cfg)
        if self.user_agent:
            self.session.headers["User-Agent"] = self.user_agent
        self.session.headers.setdefault("Accept-Language", "ru,en;q=0.8")

    def _sleep(self) -> None:
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)

    def abs_url(self, path: str) -> str:
        return urljoin(self.base_url, path or "/")

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
    def _normalize_url(url: str) -> str:
        parsed = urlparse(url)
        clean = parsed._replace(fragment="")
        return clean.geturl()

    @staticmethod
    def classify_page(url: str, html: str) -> str:
        lower_url = (url or "").lower()
        lower_html = (html or "").lower()
        if re.search(r"(product|item|goods|catalog/.+/[^/]+)", lower_url):
            return "product_like"
        if "add to cart" in lower_html or "в корзину" in lower_html:
            return "product_like"
        if re.search(r"(catalog|category|group)", lower_url):
            return "category_like"
        if "<table" in lower_html and ("цена" in lower_html or "price" in lower_html):
            return "product_like"
        return "other"

    def extract_links(self, page_url: str, html: str) -> list[str]:
        soup = BeautifulSoup(html or "", "html.parser")
        out: list[str] = []
        seen: set[str] = set()

        for tag in soup.find_all(["a", "link"], href=True):
            href = (tag.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            abs_url = self._normalize_url(urljoin(page_url, href))
            if urlparse(abs_url).netloc != urlparse(self.base_url).netloc:
                continue
            if abs_url not in seen:
                seen.add(abs_url)
                out.append(abs_url)
        return out

    def extract_endpoint_like_strings(self, html: str, page_url: str) -> list[str]:
        found: list[str] = []
        seen: set[str] = set()
        for raw in _SCRIPT_ENDPOINT_RE.findall(unescape(html or "")):
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

    def login_and_verify(self) -> dict[str, object]:
        """
        Переходный backward-safe API.

        Источник правды по авторизации теперь source.login(...).
        Здесь возвращаем упрощённый probe-report, чтобы ручные проверки не ломались.
        """
        if not self.login or not self.password:
            return {
                "ok": False,
                "base_url": self.base_url,
                "attempts": [{"stage": "precheck", "status": "missing_credentials"}],
                "cookies": self.session.cookies.get_dict(),
                "message": "Missing VTT_LOGIN or VTT_PASSWORD.",
            }

        ok = False
        error_message = ""
        try:
            ok = bool(source_login(self.session, self._cfg))
        except Exception as exc:  # pragma: no cover
            error_message = str(exc)
            ok = False

        probe_url = self.abs_url("/catalog/")
        page_snippet = ""
        response_url = probe_url
        http_status = None
        if ok:
            try:
                resp = self.get(probe_url, allow_redirects=True)
                response_url = resp.url
                http_status = resp.status_code
                page_snippet = self._snippet(resp.text)
            except Exception as exc:  # pragma: no cover
                error_message = str(exc)
                ok = False

        return {
            "ok": ok,
            "base_url": self.base_url,
            "attempts": [
                {
                    "stage": "source_login",
                    "status": "auth_ok" if ok else "auth_failed",
                    "probe_url": probe_url,
                    "response_url": response_url,
                    "http_status": http_status,
                    "page_snippet": page_snippet,
                }
            ],
            "cookies": self.session.cookies.get_dict(),
            "message": "OK" if ok else (error_message or "Authentication failed."),
        }


__all__ = [
    "VTTClient",
]
