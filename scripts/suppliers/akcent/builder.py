# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/builder.py

AkCent supplier layer — сборка raw OfferOut.

v1 price-boundary:
- builder больше не считает финальную CS-цену;
- raw отдаёт только supplier-clean price_in;
- общий pricing остаётся в core.

Важно:
- supplier-layer сам собирает чистый raw;
- core делает только общие стандартные правки.
"""

from __future__ import annotations

from collections import Counter
import re
import xml.etree.ElementTree as ET
from typing import Any, Iterable

from cs.core import OfferOut
from cs.util import norm_ws
from suppliers.akcent.compat import clean_device_value, reconcile_params
from suppliers.akcent.desc_clean import clean_description_text
from suppliers.akcent.desc_extract import extract_desc_params
from suppliers.akcent.normalize import normalize_source_basics
from suppliers.akcent.params_xml import collect_xml_params, detect_kind_by_name, resolve_allowed_keys

try:
    from suppliers.akcent.pictures import collect_picture_urls as _collect_picture_urls  # type: ignore
except Exception:
    _collect_picture_urls = None


_RE_WS = re.compile(r"\s+")


_RE_DROP_CONSUMABLE_DESC_LINE = re.compile(
    r"(?iu)\b(?:поддерживаемые\s+модели(?:\s+принтеров|\s+устройств|\s+техники)?|"
    r"совместимые\s+модели(?:\s+техники)?|совместимые\s+продукты(?:\s+для)?|для)\s*:"
)

_RE_DEVICE_MODEL = re.compile(
    r"(?iu)"
    r"(?:(SureColor|WorkForce\s+Pro|WorkForce|EcoTank|Stylus\s+Pro|Expression|PIXMA|LaserJet)\s+)?"
    r"("
    r"(?:SC-[A-Z0-9-]+|WF-[A-Z0-9-]+|ET-\d+[A-Z0-9-]*|"
    r"L\d{4,5}[A-Z0-9-]*|T\d{4,5}[A-Z0-9-]*(?:\s*w/\s*o\s*stand)?|"
    r"P\d{4,5}[A-Z0-9-]*|B\d{4,5}[A-Z0-9-]*|C\d{4,5}[A-Z0-9-]*|"
    r"M\d{4,5}[A-Z0-9-]*|DCP-[A-Z0-9-]+|MFC-[A-Z0-9-]+)"
    r")"
)

_RE_KIND_CONSUMABLE = re.compile(
    r"(?iu)^(?:картридж|чернила|экономичный\s+набор|ёмкость\s+для\s+отработанных\s+чернил|емкость\s+для\s+отработанных\s+чернил)"
)


def _clean_text(value: Any) -> str:
    return _RE_WS.sub(" ", str(value or "").replace("\xa0", " ")).strip()


def _get_field(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj.get(name)
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


def _get_offer_el(src: Any) -> ET.Element | None:
    offer_el = _get_field(src, "offer_el", "element", "el")
    return offer_el if isinstance(offer_el, ET.Element) else None


def _iter_warranty_values(src: Any) -> list[str]:
    vals: list[str] = []
    for name in ("manufacturer_warranty", "warranty", "guarantee"):
        v = _clean_text(_get_field(src, name))
        if v:
            vals.append(v)
    offer_el = _get_offer_el(src)
    if offer_el is not None:
        for tag_name in ("manufacturer_warranty", "warranty", "guarantee"):
            el = offer_el.find(tag_name)
            if el is not None:
                txt = _clean_text("".join(el.itertext()))
                if txt:
                    vals.append(txt)
    return vals


def _collect_pictures(src: Any, *, placeholder: str) -> list[str]:
    if callable(_collect_picture_urls):
        try:
            pics = _collect_picture_urls(src, placeholder=placeholder)
            if pics:
                return [str(x) for x in pics if _clean_text(x)]
        except TypeError:
            try:
                pics = _collect_picture_urls(src)
                if pics:
                    return [str(x) for x in pics if _clean_text(x)]
            except Exception:
                pass
        except Exception:
            pass

    urls = []
    for raw in (_get_field(src, "picture_urls"), _get_field(src, "pictures")):
        if isinstance(raw, (list, tuple)):
            for item in raw:
                s = _clean_text(item)
                if s:
                    urls.append(s)
    return urls or ([placeholder] if _clean_text(placeholder) else [])


def _kind_is_consumable(kind: str, name: str) -> bool:
    text = _clean_text(kind) or _clean_text(name)
    return bool(_RE_KIND_CONSUMABLE.search(text))


def _drop_leading_device_line(text: str) -> str:
    s = _clean_text(text)
    if not s:
        return ""
    lines = [ln.strip() for ln in re.split(r"[\r\n]+", s) if ln.strip()]
    if lines and _RE_DROP_CONSUMABLE_DESC_LINE.search(lines[0]):
        lines = lines[1:]
    return "\n".join(lines).strip()


def _fallback_device_from_name(name: str) -> str:
    hits: list[str] = []
    for m in _RE_DEVICE_MODEL.finditer(_clean_text(name)):
        brand = _clean_text(m.group(1))
        model = _clean_text(m.group(2))
        if not model:
            continue
        token = f"{brand} {model}".strip() if brand else model
        token = norm_ws(token)
        if token and token not in hits:
            hits.append(token)
    return "; ".join(hits)


def _sort_params(params: Iterable[tuple[str, str]], *, priority_keys: list[str]) -> list[tuple[str, str]]:
    pri = {str(k).strip(): i for i, k in enumerate(priority_keys or [])}
    cleaned: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for key, value in params:
        k = _clean_text(key)
        v = _clean_text(value)
        if not k or not v:
            continue
        item = (k, v)
        if item in seen:
            continue
        seen.add(item)
        cleaned.append(item)

    def _key(item: tuple[str, str]) -> tuple[int, int, str, str]:
        k, v = item
        return (0 if k in pri else 1, pri.get(k, 9999), k.casefold(), v.casefold())

    cleaned.sort(key=_key)
    return cleaned


def _append_param(params: list[tuple[str, str]], key: str, value: str) -> None:
    k = _clean_text(key)
    v = _clean_text(value)
    if not k or not v:
        return
    item = (k, v)
    if item not in params:
        params.append(item)


def build_offer(
    src: Any,
    *,
    schema_cfg: dict[str, Any] | None = None,
    policy_cfg: dict[str, Any] | None = None,
) -> OfferOut | None:
    schema_cfg = dict(schema_cfg or {})
    policy_cfg = dict(policy_cfg or {})

    id_prefix = _clean_text(policy_cfg.get("id_prefix")) or "AC"
    placeholder = _clean_text(policy_cfg.get("placeholder_picture")) or "https://placehold.co/800x800/png?text=No+Photo"
    vendor_blacklist = {
        _clean_text(x).casefold()
        for x in (policy_cfg.get("vendor_blacklist_casefold") or [])
        if _clean_text(x)
    }
    priority_keys = [str(x).strip() for x in (policy_cfg.get("param_priority") or []) if str(x).strip()]

    offer_el = _get_offer_el(src)

    basics = normalize_source_basics(
        raw_id=_clean_text(_get_field(src, "raw_id", "id")),
        offer_id=_clean_text(_get_field(src, "offer_id")),
        article=_clean_text(_get_field(src, "article")),
        name=_clean_text(_get_field(src, "name")),
        model=_clean_text(_get_field(src, "model")),
        vendor=_clean_text(_get_field(src, "vendor")),
        description_text=_clean_text(_get_field(src, "description")),
        dealer_text=_clean_text(_get_field(src, "dealer_price_text", "dealer_price")),
        price_text=_clean_text(_get_field(src, "price_text", "price")),
        rrp_text=_clean_text(_get_field(src, "rrp_price_text", "rrp_price")),
        available_attr=_clean_text(_get_field(src, "available_attr")),
        available_tag=_clean_text(_get_field(src, "available_tag")),
        stock_text=_clean_text(_get_field(src, "stock_text", "stock")),
        warranty_values=_iter_warranty_values(src),
        vendor_blacklist=vendor_blacklist,
        id_prefix=id_prefix,
    )

    oid = _clean_text(basics.get("oid"))
    name = _clean_text(basics.get("name"))
    if not oid or not name:
        return None

    kind = detect_kind_by_name(name)
    xml_allowed = resolve_allowed_keys(kind, schema_cfg=schema_cfg)
    xml_params, extra_info, _xml_report = collect_xml_params(
        offer_el,
        schema_cfg=schema_cfg,
        allowed_keys=xml_allowed,
    )

    native_desc = clean_description_text(_clean_text(_get_field(src, "description")))
    native_desc = _drop_leading_device_line(native_desc) if _kind_is_consumable(kind, name) else native_desc

    desc_params = extract_desc_params(
        native_desc,
        allowed_keys=xml_allowed,
        kind=kind,
    )

    params = list(xml_params)
    xml_keys_cf = {str(k).strip().casefold() for k, _ in params}

    for key, value in desc_params:
        kcf = _clean_text(key).casefold()
        if not kcf or kcf in xml_keys_cf:
            continue
        params.append((_clean_text(key), _clean_text(value)))
        xml_keys_cf.add(kcf)

    params = reconcile_params(
        params,
        kind=kind,
        name=name,
        model=_clean_text(basics.get("model")),
        article=_clean_text(basics.get("article")),
        description_text=native_desc,
    )

    params_map = {str(k).strip().casefold(): _clean_text(v) for k, v in params if _clean_text(k) and _clean_text(v)}

    model_val = params_map.get("модель") or _clean_text(basics.get("model"))
    if model_val:
        _append_param(params, "Модель", model_val)

    if _kind_is_consumable(kind, name):
        device_val = params_map.get("для устройства") or clean_device_value(_fallback_device_from_name(name))
        if device_val:
            _append_param(params, "Для устройства", device_val)

    warranty = _clean_text(basics.get("warranty"))
    if warranty:
        _append_param(params, "Гарантия", warranty)

    params = _sort_params(params, priority_keys=priority_keys)

    pictures = _collect_pictures(src, placeholder=placeholder)

    return OfferOut(
        oid=oid,
        vendorCode=oid,
        name=name,
        price=basics.get("price_in") or 0,
        picture=pictures,
        vendor=_clean_text(basics.get("vendor")),
        available=bool(basics.get("available")),
        native_desc=native_desc,
        params=params,
    )


def build_offers(
    source_offers: Iterable[Any],
    *,
    schema_cfg: dict[str, Any] | None = None,
    policy_cfg: dict[str, Any] | None = None,
) -> tuple[list[OfferOut], dict[str, Any]]:
    source_list = list(source_offers or [])
    offers: list[OfferOut] = []

    placeholder_count = 0
    desc_params_added = 0
    kind_counts: Counter[str] = Counter()

    for src in source_list:
        offer = build_offer(src, schema_cfg=schema_cfg, policy_cfg=policy_cfg)
        if offer is None:
            continue
        offers.append(offer)

        first_pic = ""
        try:
            first_pic = str((offer.picture or [""])[0] or "")
        except Exception:
            first_pic = ""
        if "placehold.co/800x800/png?text=No+Photo" in first_pic:
            placeholder_count += 1

        kind = detect_kind_by_name(offer.name)
        kind_counts[kind or "unknown"] += 1

        params_cf = {str(k).strip().casefold() for k, _ in (offer.params or [])}
        if "совместимость" in params_cf or "для устройства" in params_cf or "коды" in params_cf:
            desc_params_added += 1

    report = {
        "before": len(source_list),
        "after": len(offers),
        "placeholder_picture_count": placeholder_count,
        "desc_params_or_consumable_fields_count": desc_params_added,
        "kind_counts": dict(sorted(kind_counts.items(), key=lambda x: x[0])),
    }
    return offers, report
