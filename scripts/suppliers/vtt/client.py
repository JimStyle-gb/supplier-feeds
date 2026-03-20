# -*- coding: utf-8 -*-
"""Path: scripts/suppliers/vtt/client.py

VTT temporary probe client.

v2:
- более агрессивная диагностика логина;
- несколько вариантов submit_url и method;
- better auth heuristics;
- сохраняет в login_report подробности по найденным формам и попыткам;
- аккуратнее обходит сайт того же хоста.
"""

from __future__ import annotations

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
    "userlogin",
    "auth_login",
    "login_name",
]
_PASSWORD_FIELD_CANDIDATES = [
    "password",
    "pass",
    "passwd",
    "user_password",
    "userpassword",
]
_LOGIN_PAGE_HINTS = [
    "login",
    "вход",
    "sign in",
    "авторизац",
    "личный кабинет",
    "вход для клиентов",
]
_LOGOUT_HINTS = [
    "logout",
    "выход",
    "sign out",
    "log out",
]
_AFTER_LOGIN_HINTS = [
    "личный кабинет",
    "кабинет",
    "каталог",
    "basket",
    "корзина",
    "profile",
    "профиль",
    "заказ",
]
_SCRIPT_ENDPOINT_RE = re.compile(
    r"""(?:
        https?://[^\s"'<>]+ |
        /[A-Za-z0-9_\-./?=&%]+(?:\.[A-Za-z0-9]+)?
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
    submit_fields: dict[str, str]
    score: int
    form_index: int
    form_id: str
    form_class: str
    form_text: str


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
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "ru,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
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
        """Логинится через найденную форму и возвращает подробный отчёт."""
        attempts: list[dict[str, object]] = []
        candidates = self._login_candidates()

        for page_url in candidates:
            try:
                page_resp = self.get(page_url, allow_redirects=True)
            except Exception as exc:  # noqa: BLE001
                attempts.append(
                    {
                        "stage": "open_login_page",
                        "page_url": page_url,
                        "status": "error",
                        "error": str(exc),
                    }
                )
                continue

            if self._looks_authenticated(page_resp.text, page_resp.url):
                return {
                    "ok": True,
                    "base_url": self.base_url,
                    "landing_url": page_resp.url,
                    "attempts": attempts
                    + [
                        {
                            "stage": "open_login_page",
                            "page_url": page_url,
                            "status": "already_authenticated",
                            "response_url": page_resp.url,
                            "http_status": page_resp.status_code,
                        }
                    ],
                    "cookies": self.session.cookies.get_dict(),
                }

            forms = self._extract_login_forms(page_resp.url, page_resp.text)
            attempts.append(
                {
                    "stage": "open_login_page",
                    "page_url": page_url,
                    "status": "ok",
                    "response_url": page_resp.url,
                    "http_status": page_resp.status_code,
                    "forms_found": len(forms),
                    "page_title": self.extract_title(page_resp.text),
                    "page_has_password": self._has_password_input(page_resp.text),
                    "page_snippet": self._snippet(page_resp.text),
                }
            )
            if not forms:
                continue

            for form in forms:
                submit_variants = self._build_submit_variants(page_resp.url, form)
                for method, submit_url, payload in submit_variants:
                    try:
                        headers = {
                            "Referer": page_resp.url,
                            "Origin": self._origin_of(page_resp.url),
                        }
                        if method == "get":
                            auth_resp = self.get(
                                submit_url,
                                params=payload,
                                headers=headers,
                                allow_redirects=True,
                            )
                        else:
                            auth_resp = self.post(
                                submit_url,
                                data=payload,
                                headers=headers,
                                allow_redirects=True,
                            )

                        login_ok = self._looks_authenticated(auth_resp.text, auth_resp.url)
                        post_check_url = ""
                        post_check_ok = False

                        if not login_ok:
                            try:
                                probe_resp = self.get(self.base_url, headers={"Referer": auth_resp.url}, allow_redirects=True)
                                post_check_url = probe_resp.url
                                post_check_ok = self._looks_authenticated(probe_resp.text, probe_resp.url)
                                login_ok = post_check_ok
                            except Exception:
                                post_check_ok = False

                        attempts.append(
                            {
                                "stage": "submit_login_form",
                                "page_url": page_url,
                                "form_index": form.form_index,
                                "form_id": form.form_id,
                                "form_class": form.form_class,
                                "form_score": form.score,
                                "method": method,
                                "submit_url": submit_url,
                                "login_field": form.login_field,
                                "password_field": form.password_field,
                                "payload_keys": sorted(payload.keys()),
                                "status": "ok" if login_ok else "auth_failed",
                                "response_url": auth_resp.url,
                                "http_status": auth_resp.status_code,
                                "history": [r.status_code for r in auth_resp.history],
                                "response_has_password": self._has_password_input(auth_resp.text),
                                "response_title": self.extract_title(auth_resp.text),
                                "response_snippet": self._snippet(auth_resp.text),
                                "post_check_url": post_check_url,
                                "post_check_ok": post_check_ok,
                                "cookies": self.session.cookies.get_dict(),
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
                                "stage": "submit_login_form",
                                "page_url": page_url,
                                "form_index": form.form_index,
                                "form_id": form.form_id,
                                "form_class": form.form_class,
                                "form_score": form.score,
                                "method": method,
                                "submit_url": submit_url,
                                "payload_keys": sorted(payload.keys()),
                                "status": "submit_error",
                                "error": str(exc),
                            }
                        )

        return {
            "ok": False,
            "base_url": self.base_url,
            "attempts": attempts,
            "cookies": self.session.cookies.get_dict(),
            "message": "Login failed. Check attempts, page_snippet, response_snippet, payload_keys and submit_url.",
        }

    def _login_candidates(self) -> list[str]:
        roots = [
            self.base_url,
            urljoin(self.base_url, "login"),
            urljoin(self.base_url, "login/"),
            urljoin(self.base_url, "auth"),
            urljoin(self.base_url, "auth/"),
            urljoin(self.base_url, "user/login"),
            urljoin(self.base_url, "account/login"),
            urljoin(self.base_url, "cabinet/login"),
        ]
        uniq: list[str] = []
        seen: set[str] = set()
        for item in roots:
            clean = self._normalize_url(item)
            if clean not in seen:
                uniq.append(clean)
                seen.add(clean)
        return uniq

    def _extract_login_forms(self, page_url: str, html: str) -> list[FormSpec]:
        soup = BeautifulSoup(html, "html.parser")
        forms = soup.find_all("form")
        ranked: list[FormSpec] = []

        for idx, form in enumerate(forms):
            inputs = form.find_all(["input", "button"])
            fields: dict[str, str] = {}
            submit_fields: dict[str, str] = {}
            login_field = ""
            password_field = ""

            for inp in inputs:
                name = (inp.get("name") or "").strip()
                value = inp.get("value") or ""
                input_type = (inp.get("type") or "text").strip().lower()
                if name and input_type != "file":
                    fields[name] = value
                if name and input_type in {"submit", "image", "button"}:
                    submit_fields[name] = value or "1"
                if name and not password_field and (
                    input_type == "password" or name.lower() in _PASSWORD_FIELD_CANDIDATES
                ):
                    password_field = name

            for inp in inputs:
                name = (inp.get("name") or "").strip()
                if not name:
                    continue
                lname = name.lower()
                input_type = (inp.get("type") or "text").strip().lower()
                if input_type in {"hidden", "password", "submit", "checkbox", "radio", "button", "image"}:
                    continue
                if lname in _LOGIN_FIELD_CANDIDATES or "login" in lname or "user" in lname or "email" in lname:
                    login_field = name
                    break

            if not login_field:
                for inp in inputs:
                    name = (inp.get("name") or "").strip()
                    input_type = (inp.get("type") or "text").strip().lower()
                    if name and input_type in {"text", "email", "tel"}:
                        login_field = name
                        break

            if not (login_field and password_field):
                continue

            method = (form.get("method") or "post").strip().lower()
            action = form.get("action") or page_url
            form_text = " ".join(form.stripped_strings).lower()
            form_id = (form.get("id") or "").strip()
            form_class = " ".join(form.get("class") or [])

            score = 0
            if any(h in form_text for h in _LOGIN_PAGE_HINTS):
                score += 3
            if "csrf" in " ".join(fields.keys()).lower() or "token" in " ".join(fields.keys()).lower():
                score += 2
            if "auth" in action.lower() or "login" in action.lower():
                score += 2
            if form_id:
                score += 1
            if submit_fields:
                score += 1

            ranked.append(
                FormSpec(
                    action=action,
                    method=method,
                    fields=fields,
                    login_field=login_field,
                    password_field=password_field,
                    submit_fields=submit_fields,
                    score=score,
                    form_index=idx,
                    form_id=form_id,
                    form_class=form_class,
                    form_text=form_text[:500],
                )
            )

        ranked.sort(key=lambda x: x.score, reverse=True)
        return ranked

    def _build_submit_variants(self, page_url: str, form: FormSpec) -> list[tuple[str, str, dict[str, str]]]:
        base_payload = dict(form.fields)
        base_payload.update(form.submit_fields)
        base_payload[form.login_field] = self.login
        base_payload[form.password_field] = self.password

        form_action_url = self._normalize_url(urljoin(page_url, form.action or ""))
        page_url_norm = self._normalize_url(page_url)
        login_url_norm = self._normalize_url(urljoin(self.base_url, "login"))

        variants: list[tuple[str, str, dict[str, str]]] = []
        seen: set[tuple[str, str, tuple[str, ...]]] = set()

        for method in [form.method or "post", "post", "get"]:
            for submit_url in [form_action_url, page_url_norm, login_url_norm]:
                key = (method, submit_url, tuple(sorted(base_payload.keys())))
                if key in seen:
                    continue
                seen.add(key)
                variants.append((method, submit_url, dict(base_payload)))

        return variants

    def _looks_authenticated(self, html: str, current_url: str) -> bool:
        lower_url = current_url.lower()
        lower_html = html.lower()
        if any(h in lower_html for h in _LOGOUT_HINTS):
            return True
        if "/login" in lower_url or "/auth" in lower_url:
            return False
        if self._has_password_input(html):
            # если есть пароль и это явно похоже на страницу входа — ещё не вошли
            if any(h in lower_html for h in _LOGIN_PAGE_HINTS):
                return False
        if any(h in lower_html for h in _AFTER_LOGIN_HINTS) and not self._has_password_input(html):
            return True
        path = urlparse(current_url).path.lower()
        if any(x in path for x in ["cabinet", "account", "catalog", "basket", "profile"]):
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
            r"/compare",
        ]

        while queue and len(results) < max_pages:
            url = queue.pop(0)
            parsed = urlparse(url)
            if parsed.netloc != base_host:
                continue
            if allow_paths and not any(parsed.path.startswith(p) for p in allow_paths):
                continue
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
                results.append({"url": url, "kind": "error", "error": str(exc)})

        return results

    def abs_url(self, url: str) -> str:
        return self._normalize_url(urljoin(self.base_url, url))

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
            abs_url = self._normalize_url(urljoin(page_url, href))
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

        return found[:500]

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
        if "product" in lower_html and "price" in lower_html:
            return "product_like"
        if "товар" in lower_html and "цена" in lower_html:
            return "product_like"
        return "other"

    @staticmethod
    def _normalize_url(url: str) -> str:
        parsed = urlparse(url)
        clean = parsed._replace(fragment="")
        return clean.geturl()

    @staticmethod
    def _snippet(html: str, max_len: int = 1200) -> str:
        text = re.sub(r"\s+", " ", BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
        return text[:max_len]

    @staticmethod
    def _has_password_input(html: str) -> bool:
        lower = html.lower()
        return ('type="password"' in lower) or ("type='password'" in lower)

    @staticmethod
    def _origin_of(url: str) -> str:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"
