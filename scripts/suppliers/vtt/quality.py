# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/quality.py
VTT supplier layer — quality gate.

Задача файла:
- проверить docs/raw/vtt.yml после supplier-layer;
- поймать критичные VTT-ошибки до финального core;
- отделить critical от cosmetic;
- записать читаемый отчёт и вернуть итоговый статус.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
import xml.etree.ElementTree as ET


_RE_WS = re.compile(r"\s+")
_RE_INT = re.compile(r"-?\d+")
_RE_XML_TAG = re.compile(r"<[^>]+>")
_RE_HASHISH_VT = re.compile(r"^VT[0-9A-F]{12}$")
_RE_DIRTY_NAME = re.compile(r"\b(распродажа|sale|аналог|не\s+для\s+oem|без\s+чипа|восстановлен)\b", flags=re.IGNORECASE)
_RE_COLOR_TAIL = re.compile(
    r"\b(black|bk|cyan|magenta|yellow|grey|gray|photoblack|mattblack|matteblack|color|colour)\b",
    flags=re.IGNORECASE,
)

_FORBIDDEN_PARAM_NAMES = {
    "аналоги",
    "аналог",
    "штрихкод",
    "штрих-код",
    "штрих код",
    "oem-номер",
    "oem номер",
    "oem",
    "каталожный номер",
    "кат. номер",
    "part number",
    "pn",
}

_REQUIRED_PARAM_KEYS_SOFT = (
    "Партномер",
    "Тип",
)


@dataclass
class OfferCheck:
    oid: str
    name: str = ""
    price: int | None = None
    vendor: str = ""
    vendor_code: str = ""
    available: bool = True
    pictures: list[str] = field(default_factory=list)
    description: str = ""
    params: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class GateResult:
    ok: bool
    critical_count: int
    cosmetic_count: int
    report_path: str
    critical: list[str] = field(default_factory=list)
    cosmetic: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "critical_count": self.critical_count,
            "cosmetic_count": self.cosmetic_count,
            "report_path": self.report_path,
            "critical": self.critical,
            "cosmetic": self.cosmetic,
            "stats": self.stats,
            "skipped": False,
        }


def run_quality_gate(
    *,
    feed_path: str,
    policy_path: str | None = None,
    baseline_path: str | None = None,
    report_path: str,
) -> dict[str, object]:
    """Запускает VTT quality gate и пишет текстовый отчёт."""
    _ = policy_path, baseline_path  # зарезервировано под будущие конфиги

    feed_file = Path(feed_path)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)

    if not feed_file.is_file():
        result = GateResult(
            ok=False,
            critical_count=1,
            cosmetic_count=0,
            report_path=str(report_file),
            critical=[f"feed file not found: {feed_file}"],
            stats={},
        )
        report_file.write_text(_render_report(result), encoding="utf-8")
        return result.as_dict()

    offers = _parse_feed(feed_file.read_bytes())
    result = _check_offers(offers, report_path=str(report_file))
    report_file.write_text(_render_report(result), encoding="utf-8")
    return result.as_dict()


def _parse_feed(xml_bytes: bytes) -> list[OfferCheck]:
    """Парсит raw VTT feed в список OfferCheck."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        raise ValueError(f"VTT quality parse error: {e}") from e

    out: list[OfferCheck] = []
    for node in root.iter():
        if _local_name(node.tag) != "offer":
            continue

        offer = OfferCheck(
            oid=norm_ws(node.attrib.get("id", "")),
            available=_to_bool(node.attrib.get("available")),
        )

        for ch in node:
            tag = _local_name(ch.tag)
            if tag == "name":
                offer.name = _text(ch)
            elif tag == "price":
                offer.price = safe_int(_text(ch))
            elif tag == "vendor":
                offer.vendor = _text(ch)
            elif tag == "vendorCode":
                offer.vendor_code = _text(ch)
            elif tag == "picture":
                pic = _text(ch)
                if pic:
                    offer.pictures.append(pic)
            elif tag == "description":
                offer.description = _text(ch)
            elif tag == "param":
                key = norm_ws(ch.attrib.get("name", ""))
                val = _text(ch)
                offer.params.append((key, val))

        out.append(offer)
    return out


def _check_offers(offers: list[OfferCheck], *, report_path: str) -> GateResult:
    """Проверяет офферы и считает critical/cosmetic."""
    critical: list[str] = []
    cosmetic: list[str] = []
    seen_ids: set[str] = set()

    stats = {
        "offers_total": len(offers),
        "offers_with_picture": 0,
        "offers_without_picture": 0,
        "offers_with_vendor": 0,
        "offers_without_vendor": 0,
        "offers_with_partnumber": 0,
        "offers_without_partnumber": 0,
        "offers_hashlike_oid": 0,
        "offers_available_false": 0,
    }

    for offer in offers:
        oid = offer.oid or "<empty-id>"

        if offer.oid in seen_ids and offer.oid:
            critical.append(f"{oid}: duplicate offer id")
        elif offer.oid:
            seen_ids.add(offer.oid)

        if not offer.oid:
            critical.append(f"{oid}: empty offer id")
        if offer.vendor_code != offer.oid:
            critical.append(f"{oid}: vendorCode != offer/@id")
        if not offer.name:
            critical.append(f"{oid}: empty name")
        if offer.price is None or offer.price < 100:
            critical.append(f"{oid}: invalid price '{offer.price}'")

        if offer.pictures:
            stats["offers_with_picture"] += 1
        else:
            stats["offers_without_picture"] += 1
            cosmetic.append(f"{oid}: no picture in raw offer")

        if offer.vendor:
            stats["offers_with_vendor"] += 1
        else:
            stats["offers_without_vendor"] += 1
            cosmetic.append(f"{oid}: vendor is empty before core vendor picking")

        if not offer.available:
            stats["offers_available_false"] += 1
            cosmetic.append(f"{oid}: available=false in raw feed (final will force true for VT)")

        partnumber = _get_param_value(offer.params, "Партномер")
        if partnumber:
            stats["offers_with_partnumber"] += 1
        else:
            stats["offers_without_partnumber"] += 1
            cosmetic.append(f"{oid}: missing param 'Партномер'")

        if _RE_HASHISH_VT.match(offer.oid or ""):
            stats["offers_hashlike_oid"] += 1
            cosmetic.append(f"{oid}: oid looks like fallback hash, better use source id or partnumber")

        _check_description(offer, cosmetic)
        _check_name(offer, cosmetic)
        _check_params(offer, critical, cosmetic)

        for key in _REQUIRED_PARAM_KEYS_SOFT:
            if not _get_param_value(offer.params, key):
                cosmetic.append(f"{oid}: missing soft param '{key}'")

    # stop-rule: critical исправляем всегда; косметика допустима до 5
    ok = (len(critical) == 0) and (len(cosmetic) <= 5)

    return GateResult(
        ok=ok,
        critical_count=len(critical),
        cosmetic_count=len(cosmetic),
        report_path=report_path,
        critical=critical,
        cosmetic=cosmetic,
        stats=stats,
    )


def _check_description(offer: OfferCheck, cosmetic: list[str]) -> None:
    """Мягкие проверки описания."""
    oid = offer.oid or "<empty-id>"
    desc = norm_ws(_RE_XML_TAG.sub(" ", offer.description or ""))
    name = norm_ws(offer.name)
    if not desc:
        cosmetic.append(f"{oid}: native_desc is empty")
        return
    if _cmp(desc) == _cmp(name):
        cosmetic.append(f"{oid}: native_desc duplicates name")


def _check_name(offer: OfferCheck, cosmetic: list[str]) -> None:
    """Мягкие проверки name на VTT-грязь."""
    oid = offer.oid or "<empty-id>"
    name = norm_ws(offer.name)
    if not name:
        return
    if _RE_DIRTY_NAME.search(name):
        cosmetic.append(f"{oid}: dirty marketing/service marker still in name -> '{name[:120]}'")
    if _RE_COLOR_TAIL.search(name):
        cosmetic.append(f"{oid}: english color tail still in name -> '{name[:120]}'")


def _check_params(offer: OfferCheck, critical: list[str], cosmetic: list[str]) -> None:
    """Проверки params: запреты, пустышки, VTT-утечки."""
    oid = offer.oid or "<empty-id>"
    seen_pairs: set[tuple[str, str]] = set()

    for key, value in offer.params:
        k = norm_ws(key)
        v = norm_ws(value)
        kcf = k.casefold().replace("ё", "е")

        if not k or not v:
            critical.append(f"{oid}: empty param key/value -> '{k}'='{v}'")
            continue

        pair = (k, v)
        if pair in seen_pairs:
            cosmetic.append(f"{oid}: duplicate param '{k}'='{v}'")
        else:
            seen_pairs.add(pair)

        if kcf in _FORBIDDEN_PARAM_NAMES:
            critical.append(f"{oid}: forbidden raw param leaked -> '{k}'")

        if v in {"-", "--", "---", ".", "..", "..."}:
            critical.append(f"{oid}: empty-ish param value -> '{k}'='{v}'")

        if k == "Партномер" and len(v) < 3:
            critical.append(f"{oid}: too short partnumber -> '{v}'")

        if k == "Цвет" and _RE_COLOR_TAIL.search(v):
            cosmetic.append(f"{oid}: english color value in param '{k}' -> '{v}'")

        if k == "Совместимость":
            if _looks_like_code_only(v):
                cosmetic.append(f"{oid}: compatibility looks too code-like -> '{v}'")
            if len(v) < 4:
                critical.append(f"{oid}: compatibility too short -> '{v}'")


def _looks_like_code_only(text: str) -> bool:
    """Похоже ли значение только на код, а не на совместимость."""
    s = norm_ws(text).upper()
    if not s:
        return False
    compact = s.replace(" ", "")
    return bool(re.fullmatch(r"[A-ZА-ЯЁ0-9\-/]{4,}", compact) and re.search(r"\d", compact))


def _get_param_value(params: list[tuple[str, str]], key: str) -> str:
    """Ищет param по имени без учёта регистра."""
    target = norm_ws(key).casefold().replace("ё", "е")
    for k, v in params or []:
        if norm_ws(k).casefold().replace("ё", "е") == target:
            return norm_ws(v)
    return ""


def _render_report(result: GateResult) -> str:
    """Рендерит текстовый отчёт quality gate."""
    lines: list[str] = []
    lines.append("=" * 72)
    lines.append("[VTT] quality gate report")
    lines.append("=" * 72)
    lines.append(f"ok: {result.ok}")
    lines.append(f"critical_count: {result.critical_count}")
    lines.append(f"cosmetic_count: {result.cosmetic_count}")
    lines.append("stop_rule: critical=0 and cosmetic<=5")
    lines.append("-" * 72)
    if result.stats:
        lines.append("stats:")
        for key, value in result.stats.items():
            lines.append(f"  {key}: {value}")
        lines.append("-" * 72)

    lines.append("critical:")
    if result.critical:
        lines.extend([f"  - {x}" for x in result.critical])
    else:
        lines.append("  - none")

    lines.append("-" * 72)
    lines.append("cosmetic:")
    if result.cosmetic:
        lines.extend([f"  - {x}" for x in result.cosmetic])
    else:
        lines.append("  - none")

    lines.append("=" * 72)
    return "\n".join(lines) + "\n"


def _local_name(tag: str) -> str:
    """Возвращает local-name без namespace."""
    if not tag:
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _text(node: ET.Element | None) -> str:
    """Безопасно читает текст узла."""
    if node is None:
        return ""
    return norm_ws("".join(node.itertext()))


def _to_bool(raw: str | None) -> bool:
    """Переводит xml bool в python bool."""
    s = norm_ws(raw or "").casefold()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return True


def norm_ws(text: str) -> str:
    """Нормализует пробелы."""
    s = (text or "").replace("\u00a0", " ").strip()
    return _RE_WS.sub(" ", s).strip()


def safe_int(text: object) -> int | None:
    """Безопасно парсит int из строки."""
    if text is None:
        return None
    m = _RE_INT.search(str(text).replace(" ", ""))
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


def _cmp(text: str) -> str:
    """Упрощённое сравнение строк без шума."""
    s = norm_ws(text).casefold().replace("ё", "е")
    s = re.sub(r"[^0-9a-zа-я]+", " ", s, flags=re.IGNORECASE)
    return norm_ws(s)
