# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/client.py

VTT probe client.
v3:
- adds Laravel-style CSRF/XSRF handling for POST /validateLogin;
- sends AJAX-like headers used by JS forms;
- tries both form-encoded and JSON login payloads;
- preserves detailed login diagnostics for docs/debug/vtt_probe/login_report.json;
- keeps public-crawl helpers for the probe step.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, asdict
from html import unescape
from typing import Any
from urllib.parse import quote, unquote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup


DEFAULT_TIMEOUT = 40
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class LoginResult:
    ok: bool
    base_url: str
    attempts: list[dict[str, Any]]
    cookies: dict[str, str]
    message: str = ""


class VTTClient:
    def __init__(
        self,
        base_url: str,
        login: str,
        password: str,
        timeout: int = DEFAULT_TIMEOUT,
        delay: float = 0.35,
    ) -> None:
        self.base_url = self._normalize_base_url(base_url)
        self.login = login
        self.password = password
        self.timeout = timeout
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": DEFAULT_UA,
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "ru,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

    @staticmethod
    def _normalize_base_url(base_url: str) -> str:
        base_url = (base_url or "").strip()
        if not base_url:
            raise ValueError("Empty base_url")
        if not base_url.startswith(("http://", "https://")):
            base_url = "https://" + base_url
        return base_url.rstrip("/") + "/"

    @property
    def origin(self) -> str:
        p = urlparse(self.base_url)
        return f"{p.scheme}://{p.netloc}"

    def _sleep(self) -> None:
        if self.delay > 0:
            time.sleep(self.delay)

    def _abs_url(self, value: str | None, fallback: str) -> str:
        if not value:
            return fallback
        return urljoin(fallback, value)

    def _cookie_dict(self) -> dict[str, str]:
        return self.session.cookies.get_dict()

    def _cookie_xsrf_decoded(self) -> str:
        token = self.session.cookies.get("XSRF-TOKEN", "")
        return unquote(token) if token else ""

    @staticmethod
    def _html_snippet(text: str, limit: int = 500) -> str:
        clean = re.sub(r"\s+", " ", BeautifulSoup(text or "", "html.parser").get_text(" ", strip=True)).strip()
        return clean[:limit]

    def _record_open(self, attempts: list[dict[str, Any]], page_url: str, response: requests.Response) -> BeautifulSoup:
        soup = BeautifulSoup(response.text or "", "html.parser")
        forms = soup.find_all("form")
        attempts.append(
            {
                "stage": "open_login_page",
                "page_url": page_url,
                "status": "ok",
                "response_url": response.url,
                "http_status": response.status_code,
                "forms_found": len(forms),
                "page_title": (soup.title.get_text(strip=True) if soup.title else ""),
                "page_has_password": bool(soup.find("input", attrs={"type": "password"})),
                "page_snippet": self._html_snippet(response.text),
                "cookies_after_open": self._cookie_dict(),
            }
        )
        return soup

    def _extract_forms(self, soup: BeautifulSoup, page_url: str) -> list[dict[str, Any]]:
        forms_out: list[dict[str, Any]] = []
        for idx, form in enumerate(soup.find_all("form")):
            inputs = form.find_all(["input", "button", "textarea", "select"])
            form_class = " ".join(form.get("class", [])).strip()
            form_id = (form.get("id") or "").strip()
            action = (form.get("action") or "").strip()
            method = (form.get("method") or "get").strip().lower()

            score = 0
            if form.find("input", attrs={"type": "password"}):
                score += 3
            if form.find("input", attrs={"name": re.compile("login|email|user", re.I)}):
                score += 2
            if "login" in form_class.lower() or "auth" in form_class.lower():
                score += 2
            if "login" in action.lower() or "validate" in action.lower():
                score += 2

            fields: list[dict[str, str]] = []
            hidden: dict[str, str] = {}
            for inp in inputs:
                tag = inp.name.lower()
                name = (inp.get("name") or "").strip()
                typ = (inp.get("type") or "").strip().lower()
                value = inp.get("value") or ""
                if name:
                    fields.append({"tag": tag, "name": name, "type": typ, "value": value})
                if tag == "input" and typ == "hidden" and name:
                    hidden[name] = value

            login_field = None
            password_field = None
            remember_field = None
            for fld in fields:
                name = fld["name"]
                typ = fld["type"]
                if not login_field and re.search(r"(login|email|user)", name, re.I):
                    login_field = name
                if not password_field and typ == "password":
                    password_field = name
                if not remember_field and re.search(r"remember", name, re.I):
                    remember_field = name

            forms_out.append(
                {
                    "index": idx,
                    "page_url": page_url,
                    "form_id": form_id,
                    "form_class": form_class,
                    "action": action,
                    "method": method or "get",
                    "score": score,
                    "fields": fields,
                    "hidden": hidden,
                    "login_field": login_field or "login",
                    "password_field": password_field or "password",
                    "remember_field": remember_field,
                }
            )
        forms_out.sort(key=lambda x: x["score"], reverse=True)
        return forms_out

    def _auth_ok(self, response: requests.Response) -> bool:
        url = (response.url or "").lower()
        body = response.text or ""
        if "/login" in url and "Вход для клиентов" in body:
            return False
        if '{"result":false}' in body.replace(" ", ""):
            return False
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get("Location", "")
            if location and "login" not in location.lower():
                return True
        protected_markers = [
            "Выход",
            "Личный кабинет",
            "Каталог",
            "Прайс",
            "Корзина",
            "Заказы",
        ]
        return any(marker in body for marker in protected_markers) and "Вход для клиентов" not in body

    def _post_login_variants(
        self,
        attempts: list[dict[str, Any]],
        page_url: str,
        form_info: dict[str, Any],
    ) -> bool:
        submit_base = self._abs_url(form_info.get("action") or "", page_url)
        login_field = form_info["login_field"]
        password_field = form_info["password_field"]
        remember_field = form_info.get("remember_field")
        hidden: dict[str, str] = dict(form_info.get("hidden") or {})
        xsrf_decoded = self._cookie_xsrf_decoded()
        token_candidates = [hidden.get("_token", ""), hidden.get("csrf-token", ""), xsrf_decoded]
        token_value = next((x for x in token_candidates if x), "")

        base_payload: dict[str, Any] = {}
        base_payload.update(hidden)
        base_payload[login_field] = self.login
        base_payload[password_field] = self.password
        if remember_field:
            # try common checkbox values
            base_payload[remember_field] = hidden.get(remember_field) or "1"

        common_headers = {
            "Referer": page_url,
            "Origin": self.origin,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/plain, */*",
        }
        if xsrf_decoded:
            common_headers["X-XSRF-TOKEN"] = xsrf_decoded
        if token_value:
            common_headers["X-CSRF-TOKEN"] = token_value

        submit_urls = []
        for candidate in [
            submit_base,
            self._abs_url("/validateLogin", page_url),
            self._abs_url("/login", page_url),
        ]:
            if candidate not in submit_urls:
                submit_urls.append(candidate)

        # Different transports often used by Laravel apps + JS frontends
        variants: list[dict[str, Any]] = []
        for submit_url in submit_urls:
            variants.extend(
                [
                    {
                        "method": "post",
                        "submit_url": submit_url,
                        "mode": "form",
                        "headers": {
                            **common_headers,
                            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        },
                        "payload": dict(base_payload),
                    },
                    {
                        "method": "post",
                        "submit_url": submit_url,
                        "mode": "json",
                        "headers": {
                            **common_headers,
                            "Content-Type": "application/json;charset=UTF-8",
                        },
                        "payload": dict(base_payload),
                    },
                ]
            )

        # Extra Laravel trick: pre-flight /sanctum/csrf-cookie if available
        sanctum_url = self._abs_url("/sanctum/csrf-cookie", page_url)
        try:
            sanctum_resp = self.session.get(
                sanctum_url,
                headers={
                    "Referer": page_url,
                    "Origin": self.origin,
                    "Accept": "application/json, text/plain, */*",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=self.timeout,
            )
            attempts.append(
                {
                    "stage": "csrf_cookie",
                    "url": sanctum_url,
                    "status": "ok",
                    "http_status": sanctum_resp.status_code,
                    "response_url": sanctum_resp.url,
                    "cookies_after": self._cookie_dict(),
                }
            )
            xsrf_decoded = self._cookie_xsrf_decoded()
            if xsrf_decoded:
                for variant in variants:
                    variant["headers"]["X-XSRF-TOKEN"] = xsrf_decoded
                    if not variant["headers"].get("X-CSRF-TOKEN"):
                        variant["headers"]["X-CSRF-TOKEN"] = xsrf_decoded
        except Exception as exc:
            attempts.append(
                {
                    "stage": "csrf_cookie",
                    "url": sanctum_url,
                    "status": "error",
                    "error": str(exc),
                    "cookies_after": self._cookie_dict(),
                }
            )

        for variant in variants:
            method = variant["method"]
            submit_url = variant["submit_url"]
            payload = variant["payload"]
            headers = variant["headers"]
            try:
                if variant["mode"] == "json":
                    resp = self.session.post(
                        submit_url,
                        json=payload,
                        headers=headers,
                        timeout=self.timeout,
                        allow_redirects=True,
                    )
                else:
                    resp = self.session.post(
                        submit_url,
                        data=payload,
                        headers=headers,
                        timeout=self.timeout,
                        allow_redirects=True,
                    )

                # follow-up check to detect established session
                post_check_url = self._abs_url("/", page_url)
                post_check = self.session.get(post_check_url, timeout=self.timeout, allow_redirects=True)

                attempt = {
                    "stage": "submit_login_form",
                    "page_url": page_url,
                    "form_index": form_info["index"],
                    "form_id": form_info.get("form_id", ""),
                    "form_class": form_info.get("form_class", ""),
                    "form_score": form_info.get("score", 0),
                    "method": method,
                    "mode": variant["mode"],
                    "submit_url": submit_url,
                    "login_field": login_field,
                    "password_field": password_field,
                    "payload_keys": list(payload.keys()),
                    "headers_sent": sorted(headers.keys()),
                    "http_status": resp.status_code,
                    "response_url": resp.url,
                    "history": [h.status_code for h in resp.history],
                    "response_title": (
                        BeautifulSoup(resp.text, "html.parser").title.get_text(strip=True)
                        if resp.text and BeautifulSoup(resp.text, "html.parser").title
                        else ""
                    ),
                    "response_has_password": "password" in resp.text.lower(),
                    "response_snippet": self._html_snippet(resp.text),
                    "post_check_url": post_check.url,
                    "post_check_ok": self._auth_ok(post_check),
                    "post_check_http_status": post_check.status_code,
                    "post_check_snippet": self._html_snippet(post_check.text),
                    "cookies": self._cookie_dict(),
                }

                body_compact = (resp.text or "").replace(" ", "").replace("\n", "")
                if self._auth_ok(resp) or self._auth_ok(post_check) or '{"result":true}' in body_compact:
                    attempt["status"] = "auth_ok"
                    attempts.append(attempt)
                    return True

                attempt["status"] = "auth_failed"
                attempts.append(attempt)
            except Exception as exc:
                attempts.append(
                    {
                        "stage": "submit_login_form",
                        "page_url": page_url,
                        "form_index": form_info["index"],
                        "form_id": form_info.get("form_id", ""),
                        "form_class": form_info.get("form_class", ""),
                        "form_score": form_info.get("score", 0),
                        "method": method,
                        "mode": variant["mode"],
                        "submit_url": submit_url,
                        "payload_keys": list(payload.keys()),
                        "headers_sent": sorted(headers.keys()),
                        "status": "submit_error",
                        "error": str(exc),
                        "cookies": self._cookie_dict(),
                    }
                )
            self._sleep()
        return False

    def login_with_report(self) -> LoginResult:
        attempts: list[dict[str, Any]] = []
        login_pages = [
            self._abs_url("/", self.base_url),
            self._abs_url("/login", self.base_url),
            self._abs_url("/login/", self.base_url),
        ]

        for page_url in login_pages:
            try:
                resp = self.session.get(page_url, timeout=self.timeout, allow_redirects=True)
                resp.raise_for_status()
                soup = self._record_open(attempts, page_url, resp)
                forms = self._extract_forms(soup, page_url)
                if not forms:
                    attempts.append(
                        {
                            "stage": "parse_forms",
                            "page_url": page_url,
                            "status": "no_forms",
                            "cookies": self._cookie_dict(),
                        }
                    )
                    continue

                for form_info in forms[:2]:
                    if self._post_login_variants(attempts, page_url, form_info):
                        return LoginResult(
                            ok=True,
                            base_url=self.base_url,
                            attempts=attempts,
                            cookies=self._cookie_dict(),
                            message="Login OK",
                        )
            except Exception as exc:
                attempts.append(
                    {
                        "stage": "open_login_page",
                        "page_url": page_url,
                        "status": "error",
                        "error": str(exc),
                        "cookies": self._cookie_dict(),
                    }
                )
            self._sleep()

        return LoginResult(
            ok=False,
            base_url=self.base_url,
            attempts=attempts,
            cookies=self._cookie_dict(),
            message="Login failed. Check attempts, page_snippet, response_snippet, payload_keys, submit_url, and headers_sent.",
        )

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        self._sleep()
        resp = self.session.get(url, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp

    def get_text(self, url: str) -> str:
        return self.get(url).text

    def get_json(self, url: str) -> Any:
        return self.get(url).json()


def save_login_report(path: str, result: LoginResult) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(asdict(result), f, ensure_ascii=False, indent=2)
