# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/source.py
VTT supplier layer — source reader.

Задача файла:
- получить исходный VTT feed из URL или локального файла;
- распарсить XML/YML без business-логики;
- вернуть список сырых offer-словарей для builder.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
import os
import xml.etree.ElementTree as ET

import requests


DEFAULT_TIMEOUT_CONNECT = int(os.getenv("VTT_SOURCE_TIMEOUT_CONNECT", "20") or "20")
DEFAULT_TIMEOUT_READ = int(os.getenv("VTT_SOURCE_TIMEOUT_READ", "180") or "180")
DEFAULT_USER_AGENT = (
    os.getenv("VTT_SOURCE_USER_AGENT", "Mozilla/5.0 (compatible; CS-VTT-Bot/1.0; +https://complexsolutions.kz)")
    or "Mozilla/5.0 (compatible; CS-VTT-Bot/1.0; +https://complexsolutions.kz)"
).strip()


def fetch_vtt_source() -> bytes:
    """Читает source VTT: сначала локальный файл, потом URL."""
    src_file = (os.getenv("VTT_SOURCE_FILE", "") or "").strip()
    if src_file:
        p = Path(src_file)
        if not p.is_file():
            raise FileNotFoundError(f"VTT source file not found: {p}")
        return p.read_bytes()

    src_url = (os.getenv("VTT_SOURCE_URL", "") or "").strip()
    if not src_url:
        raise RuntimeError("VTT_SOURCE_URL or VTT_SOURCE_FILE is required")

    resp = requests.get(
        src_url,
        headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/xml,text/xml,*/*"},
        timeout=(DEFAULT_TIMEOUT_CONNECT, DEFAULT_TIMEOUT_READ),
    )
    resp.raise_for_status()
    return resp.content or b""


def parse_vtt_source(xml_bytes: bytes) -> list[dict[str, Any]]:
    """Парсит VTT XML/YML и возвращает список сырых offer-словарей."""
    if not xml_bytes:
        return []

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        raise ValueError(f"VTT source parse error: {e}") from e

    out: list[dict[str, Any]] = []
    for node in _iter_offer_nodes(root):
        item = _parse_offer_node(node)
        if item is None:
            continue
        out.append(item)
    return out


def parse_vtt_offers(xml_bytes: bytes) -> list[dict[str, Any]]:
    """Back-compat alias для build_vtt.py."""
    return parse_vtt_source(xml_bytes)


def _iter_offer_nodes(root: ET.Element):
    """Ищет все offer-узлы без привязки к namespace."""
    for node in root.iter():
        if _local_name(node.tag) == "offer":
            yield node


def _parse_offer_node(node: ET.Element) -> dict[str, Any] | None:
    """Собирает один сырой offer в dict."""
    raw_id = (node.attrib.get("id", "") or "").strip()
    name = _child_text(node, "name")
    price = _child_text(node, "price")
    vendor = _child_text(node, "vendor")
    description = _child_text(node, "description")

    item: dict[str, Any] = {
        "id": raw_id,
        "available": _parse_bool_available(node.attrib.get("available")),
        "name": name,
        "price": price,
        "vendor": vendor,
        "description": description,
        "pictures": _extract_pictures(node),
        "params": _extract_params(node),
    }

    # Совсем пустой мусор не тащим дальше.
    if not item["id"] and not item["name"] and not item["price"]:
        return None
    return item


def _extract_pictures(node: ET.Element) -> list[str]:
    """Собирает picture в исходном порядке."""
    out: list[str] = []
    for ch in node:
        if _local_name(ch.tag) != "picture":
            continue
        val = _node_text(ch)
        if not val:
            continue
        out.append(val)
    return out


def _extract_params(node: ET.Element) -> list[tuple[str, str]]:
    """Собирает все param как список (name, value)."""
    out: list[tuple[str, str]] = []
    for ch in node:
        if _local_name(ch.tag) != "param":
            continue
        key = (ch.attrib.get("name", "") or "").strip()
        val = _node_text(ch)
        if not key and not val:
            continue
        out.append((key, val))
    return out


def _child_text(node: ET.Element, child_name: str) -> str:
    """Текст первого дочернего узла по local-name."""
    child_name_cf = child_name.casefold()
    for ch in node:
        if _local_name(ch.tag).casefold() == child_name_cf:
            return _node_text(ch)
    return ""


def _node_text(node: ET.Element) -> str:
    """Безопасно вытаскивает текст узла вместе с CDATA."""
    if node is None:
        return ""
    text = "".join(node.itertext())
    return (text or "").strip()


def _parse_bool_available(raw: str | None) -> bool:
    """Переводит source available в bool."""
    val = (raw or "").strip().casefold()
    if val in {"1", "true", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "no", "n", "off"}:
        return False
    return True


def _local_name(tag: Any) -> str:
    """Возвращает local-name, даже если тег с namespace."""
    s = str(tag or "")
    if "}" in s:
        return s.rsplit("}", 1)[-1]
    return s
