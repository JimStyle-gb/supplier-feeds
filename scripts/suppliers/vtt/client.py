# -*- coding: utf-8 -*-
"""Path: scripts/suppliers/vtt/client.py"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from html import unescape
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


_LOGIN_FIELD_CANDIDATES = [
    "login",
    "username",
    "user",
    "email",
    "user_login",
    "auth_login",
]
_PASSWORD_FIELD_CANDIDATES = [
    "password",
    "pass",
    "passwd",
    "user_password",
]
_LOGOUT_HINTS = [
    "logout",
    "выход",
    "sign out",
]
_LOGIN_HINTS = [
    "login",
    "вход",
    "sign in",
]
_SCRIPT_ENDPOINT_RE = re.compile(
    r"""(?:
        https?://[^\s"'<>]+
        |
        /[A-Za-z0-9_\-./?=&%]+
    )""",
    re.X,
)


@dataclass(slots=True)
class FormSpec:
    action: str
    method: str
    fields: dict[str, str]
    login_field: str
    password_field: str


class VTTClient:
    """Сессия VTT + логин + аккуратный обход."""

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
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0 Safari/537.36"
                ),
                "Accept-Language": "ru,en;q=0.9",
            }
        )

    def get(self, url: str, **kwargs) -> requests.Response:
        time.sleep(self.delay_seconds)
        resp = self.session.get(url, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp

    def post(self, url: str, **kwargs) -> requests.Response:
        time.sleep(self.delay_seconds)
        resp = self.session.post(url, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp

    def login_and_verify(self) -> dict[str, object]:
        """Логинится через найденную форму и возвращает мини-отчёт."""
        candidates = self._login_candidates()
        attempts: list[dict[str, object]] = []

        for page_url in candidates:
            try:
                page_resp = self.get(page_url, allow_redirects=True)
            except Exception as exc:  # noqa: BLE001
                attempts.append({"page_url": page_url, "status": "error", "error": str(exc)})
                continue

            form = self._extract_login_form(page_resp.url, page_resp.text)
            if not form:
                attempts.append({"page_url": page_url, "status": "no_login_form"})
                continue

            try:
                submit_url = urljoin(page_resp.url, form.action or "")
                payload = dict(form.fields)
                payload[form.login_field] = self.login
                payload[form.password_field] = self.password

                if form.method == "get":
                    auth_resp = self.get(submit_url, params=payload, allow_redirects=True)
                else:
                    auth_resp = self.post(submit_url, data=payload, allow_redirects=True)

                login_ok = self._looks_authenticated(auth_resp.text, auth_resp.url)
                attempts.append(
                    {
                        "page_url": page_url,
                        "submit_url": submit_url,
                        "status": "ok" if login_ok else "auth_failed",
                        "response_url": auth_resp.url,
                        "http_status": auth_resp.status_code,
                    }
                )
                if login_ok:
                    return {
                        "ok": True,
                        "base_url": self.base_url,
                        "landing_url": auth_resp.url,
                        "attempts": attempts,
                        "cookies": self.session.cookies.get_dict(),
                    }
            except Exception as exc:  # noqa: BLE001
                attempts.append(
                    {
                        "page_url": page_url,
                        "status": "submit_error",
                        "error": str(exc),
                    }
                )

        return {
            "ok": False,
            "base_url": self.base_url,
            "attempts": attempts,
            "cookies": self.session.cookies.get_dict(),
        }

    def _login_candidates(self) -> list[str]:
        roots = [
            self.base_url,
            urljoin(self.base_url, "login"),
            urljoin(self.base_url, "auth"),
            urljoin(self.base_url, "user/login"),
            urljoin(self.base_url, "account/login"),
            urljoin(self.base_url, "cabinet/login"),
        ]
        uniq: list[str] = []
        seen: set[str] = set()
        for item in roots:
            if item not in seen:
                uniq.append(item)
                seen.add(item)
        return uniq

    def _extract_login_form(self, page_url: str, html: str) -> FormSpec | None:
        soup = BeautifulSoup(html, "html.parser")
        forms = soup.find_all("form")
        ranked: list[tuple[int, FormSpec]] = []

        for form in forms:
            inputs = form.find_all("input")
            fields: dict[str, str] = {}
            login_field = ""
            password_field = ""

            for inp in inputs:
                name = (inp.get("name") or "").strip()
                input_type = (inp.get("type") or "text").strip().lower()
                value = inp.get("value") or ""
                if name:
                    fields[name] = value

                if not password_field and (
                    input_type == "password" or name.lower() in _PASSWORD_FIELD_CANDIDATES
                ):
                    password_field = name

            for inp in inputs:
                name = (inp.get("name") or "").strip()
                if not name:
                    continue
                lname = name.lower()
                input_type = (inp.get("type") or "text").strip().lower()
                if input_type in {"hidden", "password", "submit", "checkbox", "radio"}:
                    continue
                if lname in _LOGIN_FIELD_CANDIDATES or "login" in lname or "user" in lname:
                    login_field = name
                    break

            if not login_field:
                for inp in inputs:
                    name = (inp.get("name") or "").strip()
                    input_type = (inp.get("type") or "text").strip().lower()
                    if name and input_type in {"text", "email"}:
                        login_field = name
                        break

            if not (login_field and password_field):
                continue

            method = (form.get("method") or "post").strip().lower()
            action = form.get("action") or page_url
            score = 0
            form_text = " ".join(form.stripped_strings).lower()
            if any(h in form_text for h in _LOGIN_HINTS):
                score += 2
            if "csrf" in json.dumps(fields, ensure_ascii=False).lower():
                score += 1
            ranked.append(
                (
                    score,
                    FormSpec(
                        action=action,
                        method=method,
                        fields=fields,
                        login_field=login_field,
                        password_field=password_field,
                    ),
                )
            )

        if not ranked:
            return None
        ranked.sort(key=lambda x: x[0], reverse=True)
        return ranked[0][1]

    def _looks_authenticated(self, html: str, current_url: str) -> bool:
        text = html.lower()
        if any(h in text for h in _LOGOUT_HINTS):
            return True
        if "type=\"password\"" not in text and "type='password'" not in text:
            if all(h not in text for h in _LOGIN_HINTS):
                return True
        path = urlparse(current_url).path.lower()
        if any(x in path for x in ["cabinet", "account", "catalog", "b2b"]):
            return True
        return False

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
