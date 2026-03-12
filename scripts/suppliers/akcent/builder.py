# -*- coding: utf-8 -*-
"""
AkCent builder layer.

Что делает:
- собирает supplier-side идеальный raw OfferOut
- использует отдельные supplier-модули
- не тащит supplier-specific эвристики в core
- schema-driven: aliases / allow_by_kind / unknown_keys / normalizers

Важно:
- auto compat / auto codes из free text НЕ генерируются
- берём только родные XML params и явно поднятые desc params
"""

from __future__ import annotations

import os
import re
from collections import Counter
from typing import Any

import yaml

from cs.core import OfferOut, compute_price

from suppliers.akcent.compat import reconcile_compat_related_params
from suppliers.akcent.desc_clean import clean_description_bulk
from suppliers.akcent.desc_extract import extract_desc_bulk
from suppliers.akcent.normalize import NormalizedOffer, normalize_offers
from suppliers.akcent.params_xml import clean_xml_params_bulk
from suppliers.akcent.pictures import apply_pictures_bulk
from suppliers.akcent.source import SourceOffer


_WS_RE = re.compile(r"\s+")
_NUM_RE = re.compile(r"(\d+)")
_RANGE_DASH_RE = re.compile(r"\s*[-–—]+\s*")
_PREFIX_CI_SPLIT_RE = re.compile(r"\s+")


def _config_dir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "config")


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        obj = yaml.safe_load(f) or {}
    if not isinstance(obj, dict):
        raise ValueError(f"Bad YAML root in {path}: expected mapping")
    return obj


def _load_schema_cfg() -> dict[str, Any]:
    return _load_yaml(os.path.join(_config_dir(), "schema.yml"))


def _load_policy_cfg() -> dict[str, Any]:
    return _load_yaml(os.path.join(_config_dir(), "policy.yml"))


def _norm_space(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _ci(s: str) -> str:
    return _norm_space(s).casefold()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        v = _norm_space(value)
        if not v:
            continue
        key = v.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def _dedupe_pairs_keep_order(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for k, v in pairs:
        kk = _norm_space(k)
        vv = _norm_space(v)
        if not kk or not vv:
            continue
        key = (_ci(kk), _ci(vv))
        if key in seen:
            continue
        seen.add(key)
        out.append((kk, vv))

    return out


def _prefix_oid(oid: str, policy_cfg: dict[str, Any]) -> str:
    prefix = _norm_space(str(policy_cfg.get("id_prefix") or "AC"))
    raw = _norm_space(oid)
    if not raw:
        return ""
    if prefix and raw.upper().startswith(prefix.upper()):
        return raw
    return f"{prefix}{raw}" if prefix else raw


def _kind_by_name(name: str, schema: dict[str, Any]) -> str:
    kind_map = schema.get("kind_by_name_prefix") or {}
    title = _norm_space(name)

    for kind, prefixes in kind_map.items():
        if not isinstance(prefixes, list):
            continue
        for prefix in prefixes:
            p = _norm_space(str(prefix or ""))
            if p and title.startswith(p):
                return str(kind)

    return "default"


def _canonical_key(key: str, schema: dict[str, Any]) -> str:
    aliases = {str(_norm_space(k)): str(_norm_space(v)) for k, v in (schema.get("aliases") or {}).items()}
    raw = _norm_space(key).strip(" :;|,-")
    if not raw:
        return ""

    mapped = aliases.get(raw, raw)

    # AkCent: унифицируем ключ кодов к одному виду для supplier-layer
    if _ci(mapped) in {"коды", "codes", "oem", "part number", "partnumber", "part number(s)"}:
        return "Коды расходников"

    if _ci(mapped) in {"совместимость", "подходит для"}:
        return "Совместимость"

    if _ci(mapped) == "модель":
        return "Модель"

    if _ci(mapped) == "гарантия":
        return "Гарантия"

    return mapped


def _value_trim(v: str) -> str:
    v = _norm_space(v)
    v = v.strip(" ;|")
    v = _RANGE_DASH_RE.sub(" - ", v)
    return _norm_space(v)


def _interfaces_cleanup(v: str) -> str:
    s = _value_trim(v)
    if not s:
        return ""
    s = s.replace("*", " ")
    s = s.replace("/", ", ").replace(";", ", ")
    s = re.sub(r"\s*,\s*", ", ", s)
    parts = [x.strip() for x in s.split(",") if x.strip()]
    parts = _dedupe_keep_order(parts)
    return ", ".join(parts)


def _warranty_to_months(v: str) -> str:
    s = _value_trim(v)
    if not s:
        return ""

    low = _ci(s)
    if low in {"нет", "без гарантии", "-", "—", "0", "0 мес"}:
        return "0"

    m_months = re.search(r"(?i)\b(\d{1,3})\s*(мес|месяц|месяца|месяцев|months?)\b", s)
    if m_months:
        return f"{int(m_months.group(1))} мес"

    m_years = re.search(r"(?i)\b(\d{1,2})\s*(год|года|лет|years?)\b", s)
    if m_years:
        return f"{int(m_years.group(1)) * 12} мес"

    m_num = re.fullmatch(r"\d{1,3}", s)
    if m_num:
        return f"{int(m_num.group(0))} мес"

    return s


def _warranty_drop_zero(v: str) -> str:
    s = _value_trim(v)
    if _ci(s) in {"0", "0 мес", "-", "—", "нет", "без гарантии"}:
        return ""
    return s


def _volume_drop_zero(v: str) -> str:
    s = _value_trim(v)
    if not s:
        return ""
    if re.fullmatch(r"(?i)0+(?:[.,]0+)?\s*(?:мл|ml|л|l)?", s):
        return ""
    return s


def _apply_named_normalizers(key: str, value: str, schema: dict[str, Any]) -> str:
    normals = schema.get("value_normalizers") or {}
    chain = list(normals.get("*") or []) + list(normals.get(key) or [])

    out = value
    for step in chain:
        step_name = _ci(str(step))
        if step_name == "trim":
            out = _value_trim(out)
        elif step_name == "collapse_spaces":
            out = _norm_space(out)
        elif step_name == "dash_ranges":
            out = _RANGE_DASH_RE.sub(" - ", out)
            out = _norm_space(out)
        elif step_name == "interfaces_cleanup":
            out = _interfaces_cleanup(out)
        elif step_name == "warranty_to_months":
            out = _warranty_to_months(out)
        elif step_name == "warranty_drop_zero":
            out = _warranty_drop_zero(out)
        elif step_name == "volume_drop_zero":
            out = _volume_drop_zero(out)

    return _value_trim(out)


def _key_passes_rules(key: str, schema: dict[str, Any]) -> bool:
    rules = schema.get("key_rules") or {}
    k = _norm_space(key)
    if not k:
        return False

    banned_exact = {_ci(x) for x in (rules.get("banned_exact") or [])}
    if _ci(k) in banned_exact:
        return False

    discard_exact = {_ci(x) for x in (schema.get("discard_exact") or [])}
    if _ci(k) in discard_exact:
        return False

    if bool(rules.get("must_contain_letter", False)) and not re.search(r"[A-Za-zА-Яа-яЁё]", k):
        return False

    max_len = int(rules.get("max_len") or 0)
    if max_len > 0 and len(k) > max_len:
        return False

    max_words = int(rules.get("max_words") or 0)
    if max_words > 0 and len(k.split()) > max_words:
        return False

    return True


def _merge_value_by_rule(key: str, old: str, new: str, schema: dict[str, Any]) -> str:
    dedupe_rules = schema.get("dedupe_rules") or {}
    rule = dedupe_rules.get(key, dedupe_rules.get("*", "keep_first"))
    old_v = _value_trim(old)
    new_v = _value_trim(new)

    if not old_v:
        return new_v
    if not new_v:
        return old_v

    if _ci(key) in {"совместимость", "коды расходников"}:
        parts = _dedupe_keep_order(
            [x.strip() for x in (old_v + "; " + new_v).split(";") if x.strip()]
        )
        return "; ".join(parts)

    if _ci(rule) == "prefer_months_max":
        old_num = int(_NUM_RE.search(old_v).group(1)) if _NUM_RE.search(old_v) else 0
        new_num = int(_NUM_RE.search(new_v).group(1)) if _NUM_RE.search(new_v) else 0
        return new_v if new_num >= old_num else old_v

    if _ci(rule) == "keep_last":
        return new_v

    return old_v


def _merge_pairs_keep_key_order(pairs: list[tuple[str, str]], schema: dict[str, Any]) -> list[tuple[str, str]]:
    order: list[str] = []
    values: dict[str, str] = {}

    for k, v in pairs:
        kk = _norm_space(k)
        vv = _value_trim(v)
        if not kk or not vv:
            continue

        if kk not in values:
            order.append(kk)
            values[kk] = vv
        else:
            values[kk] = _merge_value_by_rule(kk, values[kk], vv, schema)

    return [(k, values[k]) for k in order if values.get(k)]


def _append_extra_info(body_text: str, extra_pairs: list[tuple[str, str]]) -> str:
    body = (body_text or "").strip()
    if not extra_pairs:
        return body

    lines = [f"{k}: {v}" for k, v in extra_pairs if _norm_space(k) and _norm_space(v)]
    if not lines:
        return body

    block = "Дополнительные данные\n" + "\n".join(lines)

    if not body:
        return block

    body_cf = _ci(body)
    if "дополнительные данные" in body_cf:
        return body

    return body + "\n\n" + block


def _allow_key_for_kind(key: str, kind: str, schema: dict[str, Any]) -> bool:
    allow_by_kind = schema.get("allow_by_kind") or {}
    default_set = {_ci(x) for x in (allow_by_kind.get("default") or [])}
    kind_set = {_ci(x) for x in (allow_by_kind.get(kind) or [])}

    allowed = default_set | kind_set
    if not allowed:
        return True

    return _ci(key) in allowed


def _prepare_base_pairs(offer: NormalizedOffer, xml_pairs: list[tuple[str, str]], desc_pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []

    for k, v in xml_pairs:
        pairs.append((k, v))
    for k, v in desc_pairs:
        pairs.append((k, v))

    # Явные поля из XML-структуры поднимаем как supplier-native данные
    if offer.model:
        pairs.append(("Модель", offer.model))
    if offer.manufacturer_warranty:
        pairs.append(("Гарантия", offer.manufacturer_warranty))

    return _dedupe_pairs_keep_order(pairs)


def _apply_schema_to_offer(
    offer: NormalizedOffer,
    raw_pairs: list[tuple[str, str]],
    body_text: str,
    schema: dict[str, Any],
) -> tuple[list[tuple[str, str]], str, dict[str, Any]]:
    kind = _kind_by_name(offer.name, schema)
    unknown_cfg = schema.get("unknown_keys") or {}
    unknown_action = _ci(str(unknown_cfg.get("action") or "keep"))
    unknown_max_pairs = int(unknown_cfg.get("max_pairs") or 20)

    cleaned_pairs: list[tuple[str, str]] = []
    extra_pairs: list[tuple[str, str]] = []
    stats = Counter()

    for raw_key, raw_value in raw_pairs:
        key = _canonical_key(raw_key, schema)
        value = _value_trim(raw_value)

        if not key:
            stats["empty_key"] += 1
            continue
        if not value:
            stats["empty_value"] += 1
            continue
        if not _key_passes_rules(key, schema):
            stats["bad_key"] += 1
            continue

        value = _apply_named_normalizers(key, value, schema)
        if not value:
            stats["empty_after_normalize"] += 1
            continue

        if _allow_key_for_kind(key, kind, schema):
            cleaned_pairs.append((key, value))
            stats["allowed"] += 1
        else:
            if unknown_action == "to_extra_info" and len(extra_pairs) < unknown_max_pairs:
                extra_pairs.append((key, value))
                stats["extra_info"] += 1
            elif unknown_action == "drop":
                stats["dropped_unknown"] += 1
            else:
                cleaned_pairs.append((key, value))
                stats["kept_unknown"] += 1

    cleaned_pairs = _merge_pairs_keep_key_order(cleaned_pairs, schema)

    # supplier-specific cleanup только для уже существующих model/compat/codes
    cleaned_pairs, compat_report = reconcile_compat_related_params(cleaned_pairs)

    final_body = _append_extra_info(body_text, extra_pairs)

    report: dict[str, Any] = {
        "kind": kind,
        "stats": dict(sorted(stats.items())),
        "compat_report": compat_report,
        "extra_pairs": len(extra_pairs),
        "final_params": len(cleaned_pairs),
    }
    return cleaned_pairs, final_body, report


def build_offers(source_offers: list[SourceOffer]) -> tuple[list[OfferOut], dict[str, Any]]:
    schema = _load_schema_cfg()
    policy_cfg = _load_policy_cfg()

    normalized, norm_report = normalize_offers(source_offers)
    pictures_map, pictures_report = apply_pictures_bulk(normalized)
    xml_params_map, xml_params_report = clean_xml_params_bulk(normalized)
    cleaned_desc_map, desc_clean_report = clean_description_bulk(normalized)
    body_map, desc_params_map, desc_extract_report = extract_desc_bulk(normalized, cleaned_desc_map)

    out_offers: list[OfferOut] = []
    counters = Counter()

    for offer in normalized:
        oid = _prefix_oid(offer.oid, policy_cfg)
        if not oid:
            counters["skip_no_oid"] += 1
            continue

        raw_pairs = _prepare_base_pairs(
            offer,
            xml_params_map.get(offer.oid, []),
            desc_params_map.get(offer.oid, []),
        )

        final_params, final_body, schema_report = _apply_schema_to_offer(
            offer=offer,
            raw_pairs=raw_pairs,
            body_text=body_map.get(offer.oid, ""),
            schema=schema,
        )

        price_out = compute_price(offer.price_in)
        vendor = _norm_space(offer.vendor)
        name = _norm_space(offer.name)
        pictures = pictures_map.get(offer.oid, [])

        if not name:
            counters["skip_no_name"] += 1
            continue

        out_offers.append(
            OfferOut(
                oid=oid,
                available=bool(offer.available),
                name=name,
                price=int(price_out),
                pictures=pictures,
                vendor=vendor,
                params=final_params,
                native_desc=(final_body or "").strip(),
            )
        )

        counters["built"] += 1
        counters[f"kind__{schema_report.get('kind', 'default')}"] += 1

    report: dict[str, Any] = {
        "before": len(source_offers),
        "after": len(out_offers),
        "normalize": norm_report,
        "pictures": pictures_report,
        "xml_params": xml_params_report,
        "desc_clean": desc_clean_report,
        "desc_extract": desc_extract_report,
        "counters": dict(sorted(counters.items())),
    }
    return out_offers, report
