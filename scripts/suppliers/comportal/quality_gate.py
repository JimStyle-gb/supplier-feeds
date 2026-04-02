# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/quality_gate.py
Minimal quality gate for ComPortal.

Задача:
- проверить сырой final-raw feed до выпуска;
- записать отчёт в docs/raw/comportal_quality_gate.txt;
- вернуть ok/skipped/report_path для build_comportal.py
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List
import xml.etree.ElementTree as ET

GENERIC_VENDORS = {
    "МФП",
    "МФУ",
    "ПРИНТЕР",
    "НОУТБУК",
    "МОНИТОР",
    "ИБП",
    "СКАНЕР",
    "ПРОЕКТОР",
    "КАРТРИДЖ",
    "ТОНЕР",
    "БАТАРЕЯ",
    "АККУМУЛЯТОР",
    "СТАБИЛИЗАТОР",
}


def _safe(text: str | None) -> str:
    return (text or "").strip()


def _parse_feed(feed_path: str) -> ET.Element:
    return ET.parse(feed_path).getroot()


def _offer_id(el: ET.Element) -> str:
    return _safe(el.get("id"))


def _param_map(offer_el: ET.Element) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for p in offer_el.findall("./param"):
        name = _safe(p.get("name"))
        value = _safe(p.text)
        if name and value:
            out[name] = value
    return out


def run_quality_gate(feed_path: str, report_path: str = "docs/raw/comportal_quality_gate.txt") -> Dict[str, object]:
    root = _parse_feed(feed_path)
    offers = root.findall(".//offer")

    critical: List[str] = []
    cosmetic: List[str] = []

    seen_ids: set[str] = set()

    for offer in offers:
        oid = _offer_id(offer)
        name = _safe(offer.findtext("./name"))
        vendor_code = _safe(offer.findtext("./vendorCode"))
        vendor = _safe(offer.findtext("./vendor"))
        price = _safe(offer.findtext("./price"))
        desc = _safe(offer.findtext("./description"))
        pmap = _param_map(offer)

        if not oid:
            critical.append("offer without id")
            continue

        if oid in seen_ids:
            critical.append(f"{oid}: duplicate offer id")
        seen_ids.add(oid)

        if not oid.startswith("CP"):
            critical.append(f"{oid}: id must start with CP")

        if not vendor_code:
            critical.append(f"{oid}: empty vendorCode")
        elif not vendor_code.startswith("CP"):
            critical.append(f"{oid}: vendorCode must start with CP")

        if not name:
            critical.append(f"{oid}: empty name")

        if not price or not re.fullmatch(r"\d+", price):
            critical.append(f"{oid}: invalid numeric price")

        if not vendor:
            critical.append(f"{oid}: empty vendor")
        elif vendor.upper() in GENERIC_VENDORS:
            critical.append(f"{oid}: generic vendor '{vendor}'")

        if not desc:
            critical.append(f"{oid}: empty description")

        if not pmap:
            cosmetic.append(f"{oid}: no params")

    report_lines: List[str] = []
    report_lines.append("# Итог проверки quality gate")
    report_lines.append("QUALITY_GATE: " + ("PASS" if not critical else "FAIL"))
    report_lines.append("# PASS = можно выпускать | FAIL = есть блокирующие проблемы")
    report_lines.append("enforce: true")
    report_lines.append("report_file: " + report_path)
    report_lines.append("critical_count: " + str(len(critical)))
    report_lines.append("cosmetic_total_count: " + str(len(cosmetic)))
    report_lines.append("")
    report_lines.append("CRITICAL:")
    report_lines.extend(critical or ["# Ошибок в этой секции нет"])
    report_lines.append("")
    report_lines.append("COSMETIC TOTAL:")
    report_lines.extend(cosmetic or ["# Ошибок в этой секции нет"])

    rp = Path(report_path)
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text("\n".join(report_lines), encoding="utf-8")

    return {
        "ok": not critical,
        "skipped": False,
        "reason": "",
        "report_path": str(rp),
    }


__all__ = ["run_quality_gate"]
