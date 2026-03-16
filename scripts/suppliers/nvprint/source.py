# -*- coding: utf-8 -*-
"""NVPrint source layer: download + XML helpers."""

from __future__ import annotations

import random
import sys
import time
from dataclasses import dataclass
from xml.etree import ElementTree as ET

import requests


@dataclass
class Auth:
    login: str
    password: str


def get_auth(*, login: str, password: str) -> Auth | None:
    if login and password:
        return Auth(login=login, password=password)
    return None


def download_xml(*, url: str, auth: Auth | None, retries: int = 4, t_connect: int = 20, t_read: int = 120) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0 (CS bot; NVPrint adapter)",
        "Accept": "application/xml,text/xml,*/*",
    }
    kwargs = {"timeout": (t_connect, t_read), "headers": headers}
    if auth:
        kwargs["auth"] = (auth.login, auth.password)

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, **kwargs)
            if r.status_code == 200 and r.content:
                return r.content
            raise RuntimeError(f"Не удалось скачать NVPrint XML: http={r.status_code} bytes={len(r.content or b'')}")
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, requests.exceptions.SSLError) as e:
            last_err = e
            if attempt >= retries:
                break
            sleep_s = (1.5 ** (attempt - 1)) + random.uniform(0.0, 0.4)
            print(
                f"NVPrint: сеть/таймаут, попытка {attempt}/{retries} -> sleep {sleep_s:.1f}s ({type(e).__name__})",
                file=sys.stderr,
            )
            time.sleep(sleep_s)
    raise RuntimeError(f"NVPrint: не удалось скачать XML после {retries} попыток: {last_err}")


def xml_head(xml_bytes: bytes, limit: int = 2500) -> str:
    try:
        s = xml_bytes.decode("utf-8")
    except Exception:
        try:
            s = xml_bytes.decode("cp1251")
        except Exception:
            s = xml_bytes.decode("utf-8", errors="replace")
    s = s.replace("\r", "")
    return s[:limit]


def local(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def get_text(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def pick_first_text(node: ET.Element, names: tuple[str, ...]) -> str:
    want = {n.casefold() for n in names}
    for ch in list(node):
        if local(ch.tag).casefold() in want:
            v = get_text(ch)
            if v:
                return v
    return ""


def iter_children(node: ET.Element) -> list[ET.Element]:
    return list(node)


def find_items(root: ET.Element) -> list[ET.Element]:
    offers = [el for el in root.iter() if local(el.tag).casefold() == "offer"]
    if offers:
        return offers

    tovar = [el for el in root.iter() if local(el.tag).casefold() == "товар"]
    if tovar:
        return tovar

    return []
