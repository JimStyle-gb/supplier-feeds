# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/nvprint/source.py
NVPrint source layer — скачать XML, распарсить товары, отдать сырой item list.

Это перенос логики из монолита build_nvprint.py без смены поведения.
"""

from __future__ import annotations

import os
import random
import re
import sys
import time
from dataclasses import dataclass
from xml.etree import ElementTree as ET

import requests


@dataclass
class Auth:
    """Логин/пароль для NVPrint."""
    login: str
    password: str



def get_auth() -> Auth | None:
    """Прочитать auth из env."""
    login = (os.environ.get("NVPRINT_LOGIN") or "").strip()
    pw = (os.environ.get("NVPRINT_PASSWORD") or os.environ.get("NVPRINT_PASS") or "").strip()
    if login and pw:
        return Auth(login=login, password=pw)
    return None



def download_xml(url: str, auth: Auth | None) -> bytes:
    """Скачать XML с ретраями."""
    headers = {
        "User-Agent": "Mozilla/5.0 (CS bot; NVPrint adapter)",
        "Accept": "application/xml,text/xml,*/*",
    }

    def _env_int(name: str, default: int) -> int:
        try:
            v = int((os.environ.get(name, str(default)) or str(default)).strip())
            return v if v > 0 else default
        except Exception:
            return default

    retries = _env_int("NVPRINT_HTTP_RETRIES", 4)
    t_connect = _env_int("NVPRINT_TIMEOUT_CONNECT", 20)
    t_read = _env_int("NVPRINT_TIMEOUT_READ", 120)

    kwargs: dict = {"timeout": (t_connect, t_read), "headers": headers}
    if auth:
        kwargs["auth"] = (auth.login, auth.password)

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, **kwargs)
            if r.status_code == 200 and r.content:
                return r.content
            raise RuntimeError(
                f"Не удалось скачать NVPrint XML: http={r.status_code} bytes={len(r.content or b'')}"
            )
        except (
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.SSLError,
        ) as e:
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
    """Превью XML для ошибок."""
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
    """Локальное имя тега без namespace."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag



def get_text(el: ET.Element | None) -> str:
    """Текст XML-элемента."""
    if el is None or el.text is None:
        return ""
    return el.text.strip()



def pick_first_text(node: ET.Element, names: tuple[str, ...]) -> str:
    """Первое непустое поле из списка имён."""
    want = {n.casefold() for n in names}
    for ch in list(node):
        if local(ch.tag).casefold() in want:
            v = get_text(ch)
            if v:
                return v
    return ""



def iter_children(node: ET.Element) -> list[ET.Element]:
    """Дети узла."""
    return list(node)



def find_items(root: ET.Element) -> list[ET.Element]:
    """Найти товарные узлы offer/Товар."""
    offers = [el for el in root.iter() if local(el.tag).casefold() == "offer"]
    if offers:
        return offers
    tovar = [el for el in root.iter() if local(el.tag).casefold() == "товар"]
    if tovar:
        return tovar
    return []



def load_items(url: str, strict: bool = False) -> tuple[bytes, ET.Element, list[ET.Element]]:
    """Скачать и распарсить XML, вернуть bytes/root/items."""
    auth = get_auth()
    try:
        xml_bytes = download_xml(url, auth)
    except Exception as e:
        if strict:
            raise
        print(
            f"NVPrint: не удалось скачать XML ({e}). Мягкий выход без падения.\n"
            "Подсказка: чтобы падало жёстко, поставь NVPRINT_STRICT=1",
            file=sys.stderr,
        )
        raise RuntimeError("SOFT_EXIT") from e

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        raise RuntimeError(f"NVPrint XML не парсится: {e}\nПревью:\n{xml_head(xml_bytes)}")

    items = find_items(root)
    if not items:
        raise RuntimeError("Не нашёл товары в NVPrint XML.\nПревью:\n" + xml_head(xml_bytes))

    return xml_bytes, root, items
