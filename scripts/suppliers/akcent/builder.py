# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/builder.py

AkCent supplier layer — сборка raw offer.

Логика выровнена под шаблон AlStyle:
- normalize -> pictures -> xml params -> desc clean/extract -> schema -> merge/reconcile -> OfferOut
- core не должен угадывать supplier-specific данные
- safe desc override только для ограничённых ключей
- auto compat / auto codes из free text НЕ генерируем
"""

from __future__ import annotations

import os
import re
from collections import Counter
from typing import Any

import yaml

from cs.core import OfferOut
from cs.pricing import compute_price

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

_SAFE_DESC_OVERRIDE_KEYS = {"Совместимость", "Цвет", "Технология", "Ресурс"}

# Жёсткий drop-list под текущий CS-core/validator
_HARD_DROP_PARAM_KEYS = {
    "Штрихкод",
    "Штрих-код",
    "Штрих код",
    "EAN",
    "EAN-13",
    "EAN13",
    "Barcode",
    "GTIN",
    "UPC",
    "Артикул",
    "Новинка",
    "Снижена цена",
    "Благотворительность",
    "Код товара Kaspi",
    "Код ТН ВЭД",
    "Назначение",
}
_HARD_DROP_PARAM_KEYS_CF = {str(x).strip().casefold() for x in _HARD_DROP_PARAM_KEYS}

_DIRTY_COMPAT_RE = re.compile(
    r"(?iu)\b(?:"
    r"характеристики|основные\s+характеристики|технические\s+характеристики|"
    r"модель|совместимые\s+модели|совместимость|"
    r"поддерживаемые\s+модели(?:\s+принтеров)?|поддерживаемые\s+продукты|"
    r"устройства|устройство|применение|"
    r"интерфейс|память|процессор|скорость\s+печати|"
    r"формат(?:ы)?\s+бумаги|плотность|ёмкость|емкость|"
    r"ресурс(?:\s+картриджа)?|гарантированн(?:ый|ого)\s+об(?:ъ|ь)ем\s+отпечатков|"
    r"при\s+5%\s+заполнении|количество\s+в\s+упаковке|колличество\s+в\s+упаковке"
    r")\b"
)
_DIRTY_COLOR_RE = re.compile(
    r"(?iu)\b(?:"
    r"тип\s+чернил|ресурс(?:\s+картриджа)?|количество\s+страниц|секция\s+аппарата|"
    r"совместимость|устройства|количество\s+цветов|серия|игров"
    r")\b"
)
_DIRTY_TECH_RE = re.compile(
    r"(?iu)\b(?:"
    r"количество\s+цветов|тип\s+чернил|ресурс(?:\s+картриджа)?|совместимость|"
    r"устройства|об(?:ъ|ь)ем\s+картриджа|секция\s+аппарата|серия"
    r")\b"
)
_CLEAN_TECH_RE = re.compile(
    r"(?iu)^(?:"
    r"Лазерная(?:\s+монохромная|\s+цветная)?|"
    r"Светодиодная(?:\s+монохромная|\s+цветная)?|"
    r"Струйная|Термоструйная|Матричная|Термосублимационная"
    r")$"
)
_CLEAN_RESOURCE_RE = re.compile(r"(?iu)^\d[\d\s.,]*(?:\s*(?:стр\.?|страниц|pages|copies))?$")

_COMPAT_BRAND_HINT_RE = re.compile(
    r"(?iu)\b(?:"
    r"Xerox|Canon|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Samsung|Sharp|Konica|"
    r"SMART|ViewSonic|Fellowes|Tenda|TP-Link|MikroTik|Ubiquiti|Dahua|Hikvision|"
    r"VersaLink|AltaLink|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser|"
    r"DocuColor|Versant|PrimeLink|DocuCentre|ImagePROGRAF|imageRUNNER|imagePRESS|PIXMA|"
    r"SureColor|WorkForce|LaserJet|Color\s+LaserJet|SC-[A-Z0-9]+|WF-[A-Z0-9]+|"
    r"J75|C75|D95|D110|D125"
    r")\b"
)
_XEROX_HEAVY_COMPAT_RE = re.compile(
    r"(?iu)\b(?:VersaLink|AltaLink|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser|DocuColor|Versant)\b"
)


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


def _norm_ws(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").strip())


def _ci(s: str) -> str:
    return _norm_ws(s).casefold()


def _value_trim(v: str) -> str:
    v = _norm_ws(v)
    v = v.strip(" ;|")
    v = _RANGE_DASH_RE.sub(" - ", v)
    return _norm_ws(v)


def _dedupe_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for value in values:
        v = _norm_ws(value)
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
        kk = _norm_ws(k)
        vv = _value_trim(v)
        if not kk or not vv:
            continue
        sig = (_ci(kk), _ci(vv))
        if sig in seen:
            continue
        seen.add(sig)
        out.append((kk, vv))
    return out


def _kind_by_name(name: str, schema: dict[str, Any]) -> str:
    kind_map = schema.get("kind_by_name_prefix") or {}
    title_cf = _ci(name)

    for kind, prefixes in kind_map.items():
        if not isinstance(prefixes, list):
            continue
        for prefix in prefixes:
            p = _ci(str(prefix or ""))
            if p and title_cf.startswith(p):
                return str(kind)

    return "default"


def _canonical_key(key: str, schema: dict[str, Any]) -> str:
    aliases_raw = schema.get("aliases") or {}
    aliases = {_ci(k): _norm_ws(str(v)) for k, v in aliases_raw.items()}

    raw = _norm_ws(key).strip(" :;|,-")
    if not raw:
        return ""

    mapped = aliases.get(_ci(raw), raw)

    # Дополнительная страховка на частые варианты
    low = _ci(mapped)
    if low in {"коды", "codes", "oem", "part number", "partnumber", "part number(s)"}:
        return "Коды расходников"
    if low in {"совместимость", "подходит для"}:
        return "Совместимость"
    if low == "модель":
        return "Модель"
    if low == "гарантия":
        return "Гарантия"

    # Мощность -> нормализуем единицу
    mapped = re.sub(r"\(\s*bt\s*\)", "(Вт)", mapped, flags=re.I)
    mapped = re.sub(r"\(\s*w\s*\)", "(Вт)", mapped, flags=re.I)
    mapped = re.sub(r"\bbt\b", "Вт", mapped, flags=re.I)

    return mapped


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
            out = _norm_ws(out)
        elif step_name == "dash_ranges":
            out = _RANGE_DASH_RE.sub(" - ", out)
            out = _norm_ws(out)
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
    k = _norm_ws(key)
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


def _allow_key_for_kind(key: str, kind: str, schema: dict[str, Any]) -> bool:
    allow_by_kind = schema.get("allow_by_kind") or {}
    default_set = {_ci(x) for x in (allow_by_kind.get("default") or [])}
    kind_set = {_ci(x) for x in (allow_by_kind.get(kind) or [])}

    allowed = default_set | kind_set
    if not allowed:
        return True

    return _ci(key) in allowed


def _merge_value_by_rule(key: str, old: str, new: str, schema: dict[str, Any]) -> str:
    dedupe_rules = schema.get("dedupe_rules") or {}
    rule = _ci(str(dedupe_rules.get(key, dedupe_rules.get("*", "keep_first"))))
    old_v = _value_trim(old)
    new_v = _value_trim(new)

    if not old_v:
        return new_v
    if not new_v:
        return old_v

    if _ci(key) in {"совместимость", "коды расходников"} or rule == "merge_unique":
        parts = _dedupe_keep_order([x.strip() for x in (old_v + "; " + new_v).split(";") if x.strip()])
        return "; ".join(parts)

    if rule == "prefer_months_max":
        old_num = int(_NUM_RE.search(old_v).group(1)) if _NUM_RE.search(old_v) else 0
        new_num = int(_NUM_RE.search(new_v).group(1)) if _NUM_RE.search(new_v) else 0
        return new_v if new_num >= old_num else old_v

    if rule == "keep_last":
        return new_v

    return old_v


def _merge_pairs_keep_key_order(pairs: list[tuple[str, str]], schema: dict[str, Any]) -> list[tuple[str, str]]:
    order: list[str] = []
    values: dict[str, str] = {}

    for k, v in pairs:
        kk = _norm_ws(k)
        vv = _value_trim(v)
        if not kk or not vv:
            continue

        if kk not in values:
            order.append(kk)
            values[kk] = vv
        else:
            values[kk] = _merge_value_by_rule(kk, values[kk], vv, schema)

    return [(k, values[k]) for k in order if values.get(k)]


def _is_dirty_value(key: str, value: str) -> bool:
    k = _norm_ws(key)
    v = _norm_ws(value)
    if not k or not v:
        return True

    if k == "Совместимость":
        if _DIRTY_COMPAT_RE.search(v):
            return True
        if ":" in v and re.search(r"(?iu)\b(?:характеристики|модель|совместим(?:ость|ые\s+модели)|устройства?)\b", v):
            return True
        if not _COMPAT_BRAND_HINT_RE.search(v) and len(v.split()) > 8:
            return True
        if "/" not in v and "," not in v and len(v.split()) > 10:
            return True
        return False

    if k == "Цвет":
        if _DIRTY_COLOR_RE.search(v):
            return True
        if len(v.split()) > 4:
            return True
        return False

    if k == "Технология":
        if _DIRTY_TECH_RE.search(v):
            return True
        if not _CLEAN_TECH_RE.fullmatch(v):
            return True
        return False

    if k == "Ресурс":
        if len(v) > 40:
            return True
        if not _CLEAN_RESOURCE_RE.fullmatch(v):
            return True
        return False

    return False


def _compat_looks_clean(v: str) -> bool:
    s = _norm_ws(v)
    if not s:
        return False
    if _is_dirty_value("Совместимость", s):
        return False
    if not _COMPAT_BRAND_HINT_RE.search(s):
        return False
    return True


def _prefer_desc_value(key: str, xml_val: str, desc_val: str) -> bool:
    if key not in _SAFE_DESC_OVERRIDE_KEYS:
        return False
    if not desc_val:
        return False

    xml_dirty = _is_dirty_value(key, xml_val)
    desc_dirty = _is_dirty_value(key, desc_val)

    if desc_dirty:
        return False
    if xml_dirty:
        return True

    if key == "Ресурс" and len(desc_val) < len(xml_val):
        return True

    if key == "Совместимость":
        xml_len = len(_norm_ws(xml_val))
        desc_len = len(_norm_ws(desc_val))
        if (
            desc_len >= 8
            and xml_len >= 8
            and desc_len + 40 < xml_len
            and _compat_looks_clean(desc_val)
            and desc_val.count(",") <= xml_val.count(",") + 1
        ):
            return True

    return False


def _best_desc_values(desc_params: list[tuple[str, str]]) -> dict[str, tuple[str, str]]:
    best: dict[str, tuple[str, str]] = {}

    for k, v in desc_params:
        k2 = _norm_ws(k)
        v2 = _norm_ws(v)
        if not k2 or not v2:
            continue

        key_cf = k2.casefold()
        prev = best.get(key_cf)

        if key_cf == "совместимость":
            if not _compat_looks_clean(v2):
                continue
            if prev is None:
                best[key_cf] = (k2, v2)
                continue
            prev_v = prev[1]
            if len(v2) < len(prev_v):
                best[key_cf] = (k2, v2)
            continue

        if key_cf in {"цвет", "технология", "ресурс"}:
            if _is_dirty_value(k2, v2):
                continue
            if prev is None or len(v2) < len(prev[1]):
                best[key_cf] = (k2, v2)

    return best


def _final_reconcile_params(
    params: list[tuple[str, str]],
    desc_params: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    if not params:
        return params

    best_desc = _best_desc_values(desc_params)
    compat_desc = best_desc.get("совместимость")
    if not compat_desc:
        return params

    desc_v = compat_desc[1]
    out: list[tuple[str, str]] = []

    for k, v in params:
        k2 = _norm_ws(k)
        v2 = _norm_ws(v)

        if k2.casefold() == "совместимость":
            if _is_dirty_value("Совместимость", v2):
                out.append((k2, desc_v))
                continue

            if (
                _XEROX_HEAVY_COMPAT_RE.search(v2)
                and _XEROX_HEAVY_COMPAT_RE.search(desc_v)
                and len(desc_v) + 50 < len(v2)
                and _compat_looks_clean(desc_v)
            ):
                out.append((k2, desc_v))
                continue

        out.append((k2, v2))

    return out


def merge_params(
    xml_params: list[tuple[str, str]],
    desc_params: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    XML params по умолчанию приоритетнее.
    Description-derived params дополняют,
    но могут точечно заменить грязные XML значения
    только для безопасного набора ключей.
    """
    out: list[tuple[str, str]] = []
    seen_pair: set[tuple[str, str]] = set()
    index_by_key: dict[str, int] = {}

    for k, v in xml_params:
        k2 = _norm_ws(k)
        v2 = _norm_ws(v)
        if not k2 or not v2:
            continue
        sig = (k2.casefold(), v2.casefold())
        if sig in seen_pair:
            continue
        index_by_key.setdefault(k2.casefold(), len(out))
        out.append((k2, v2))
        seen_pair.add(sig)

    for k, v in desc_params:
        k2 = _norm_ws(k)
        v2 = _norm_ws(v)
        if not k2 or not v2:
            continue

        key_cf = k2.casefold()
        sig = (key_cf, v2.casefold())
        if sig in seen_pair:
            continue

        if key_cf in index_by_key:
            idx = index_by_key[key_cf]
            xml_k, xml_v = out[idx]
            if _prefer_desc_value(xml_k, xml_v, v2):
                seen_pair.discard((xml_k.casefold(), xml_v.casefold()))
                out[idx] = (xml_k, v2)
                seen_pair.add((xml_k.casefold(), v2.casefold()))
            continue

        out.append((k2, v2))
        index_by_key[key_cf] = len(out) - 1
        seen_pair.add(sig)

    out = _final_reconcile_params(out, desc_params)
    return out


def _append_extra_info(body_text: str, extra_pairs: list[tuple[str, str]]) -> str:
    body = (body_text or "").strip()
    if not extra_pairs:
        return body

    lines = [f"{k}: {v}" for k, v in extra_pairs if _norm_ws(k) and _norm_ws(v)]
    if not lines:
        return body

    block = "Дополнительные данные\n" + "\n".join(lines)

    if not body:
        return block

    if "дополнительные данные" in _ci(body):
        return body

    return body + "\n\n" + block


def _prepare_extra_pairs(
    pairs: list[tuple[str, str]],
    *,
    max_pairs: int,
) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for k, v in pairs:
        kk = _norm_ws(k)
        vv = _value_trim(v)
        if not kk or not vv:
            continue
        if _ci(kk) in _HARD_DROP_PARAM_KEYS_CF:
            continue
        sig = (_ci(kk), _ci(vv))
        if sig in seen:
            continue
        seen.add(sig)
        out.append((kk, vv))
        if max_pairs and len(out) >= max_pairs:
            break

    return out


def _schema_transform_pairs(
    pairs: list[tuple[str, str]],
    *,
    kind: str,
    schema: dict[str, Any],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]], dict[str, Any]]:
    """
    Применяет schema к одному источнику параметров:
    - aliases
    - key rules
    - value normalizers
    - allow_by_kind
    - unknown_keys -> extra_info
    """
    unknown_cfg = schema.get("unknown_keys") or {}
    unknown_action = _ci(str(unknown_cfg.get("action") or "keep"))
    unknown_max_pairs = int(unknown_cfg.get("max_pairs") or 20) if unknown_action == "to_extra_info" else 0

    cleaned: list[tuple[str, str]] = []
    extra_pairs: list[tuple[str, str]] = []
    stats = Counter()

    for raw_key, raw_value in pairs:
        key = _canonical_key(raw_key, schema)
        value = _value_trim(raw_value)

        if not key:
            stats["empty_key"] += 1
            continue
        if not value:
            stats["empty_value"] += 1
            continue
        if _ci(key) in _HARD_DROP_PARAM_KEYS_CF:
            stats["hard_drop_forbidden"] += 1
            continue
        if not _key_passes_rules(key, schema):
            stats["bad_key"] += 1
            continue

        value = _apply_named_normalizers(key, value, schema)
        if not value:
            stats["empty_after_normalize"] += 1
            continue

        if _allow_key_for_kind(key, kind, schema):
            cleaned.append((key, value))
            stats["allowed"] += 1
        else:
            if unknown_action == "to_extra_info" and len(extra_pairs) < unknown_max_pairs:
                extra_pairs.append((key, value))
                stats["extra_info"] += 1
            elif unknown_action == "drop":
                stats["dropped_unknown"] += 1
            else:
                cleaned.append((key, value))
                stats["kept_unknown"] += 1

    cleaned = _dedupe_pairs_keep_order(cleaned)
    extra_pairs = _prepare_extra_pairs(extra_pairs, max_pairs=unknown_max_pairs)

    report: dict[str, Any] = {
        "stats": dict(sorted(stats.items())),
        "cleaned_count": len(cleaned),
        "extra_count": len(extra_pairs),
    }
    return cleaned, extra_pairs, report


def _prepare_xml_pairs(offer: NormalizedOffer, xml_pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = list(xml_pairs)

    # Явные supplier-native поля
    if offer.model:
        pairs.append(("Модель", offer.model))
    if offer.manufacturer_warranty:
        pairs.append(("Гарантия", offer.manufacturer_warranty))

    return _dedupe_pairs_keep_order(pairs)


def build_offers(
    source_offers: list[SourceOffer],
    *,
    schema_cfg: dict[str, Any] | None = None,
    policy_cfg: dict[str, Any] | None = None,
    vendor_blacklist: set[str] | None = None,
    placeholder_picture: str | None = None,
    id_prefix: str = "AC",
) -> tuple[list[OfferOut], dict[str, Any]]:
    schema = dict(schema_cfg or _load_schema_cfg())
    policy = dict(policy_cfg or _load_policy_cfg())

    if vendor_blacklist is None:
        vendor_blacklist = {
            str(x).strip().casefold()
            for x in (policy.get("vendor_blacklist_casefold") or [])
            if str(x).strip()
        }

    if not placeholder_picture:
        placeholder_picture = str(policy.get("placeholder_picture") or "").strip() or None

    normalized, norm_report = normalize_offers(
        source_offers,
        id_prefix=id_prefix or str(policy.get("id_prefix") or "AC"),
        vendor_blacklist=vendor_blacklist,
    )
    pictures_map, pictures_report = apply_pictures_bulk(normalized)
    xml_params_map, xml_params_report = clean_xml_params_bulk(normalized)
    cleaned_desc_map, desc_clean_report = clean_description_bulk(normalized)
    body_map, desc_params_map, desc_extract_report = extract_desc_bulk(normalized, cleaned_desc_map)

    out_offers: list[OfferOut] = []
    counters = Counter()

    for offer in normalized:
        oid = _norm_ws(offer.oid)
        name = _norm_ws(offer.name)
        vendor = _norm_ws(offer.vendor)
        pictures = pictures_map.get(offer.oid, offer.pictures[:])

        if not oid:
            counters["skip_no_oid"] += 1
            continue
        if not name:
            counters["skip_no_name"] += 1
            continue

        kind = _kind_by_name(name, schema)

        xml_pairs_raw = _prepare_xml_pairs(offer, xml_params_map.get(offer.oid, []))
        desc_pairs_raw = desc_params_map.get(offer.oid, [])

        xml_pairs, xml_extra, xml_schema_report = _schema_transform_pairs(
            xml_pairs_raw,
            kind=kind,
            schema=schema,
        )
        desc_pairs, desc_extra, desc_schema_report = _schema_transform_pairs(
            desc_pairs_raw,
            kind=kind,
            schema=schema,
        )

        params = merge_params(xml_pairs, desc_pairs)
        params, compat_report = reconcile_compat_related_params(params)
        params = _merge_pairs_keep_key_order(params, schema)
        params = [(k, v) for (k, v) in params if _ci(k) not in _HARD_DROP_PARAM_KEYS_CF]
        params = _dedupe_pairs_keep_order(params)

        extra_pairs = _prepare_extra_pairs(
            xml_extra + desc_extra,
            max_pairs=int((schema.get("unknown_keys") or {}).get("max_pairs") or 20),
        )

        native_desc = _append_extra_info(body_map.get(offer.oid, ""), extra_pairs)

        if offer.price_in is None or offer.price_in <= 0:
            price_out = 100
            counters["price_fallback_100"] += 1
        else:
            price_out = int(compute_price(int(offer.price_in)))

        out_offers.append(
            OfferOut(
                oid=oid,
                available=bool(offer.available),
                name=name,
                price=price_out,
                pictures=list(pictures),
                vendor=vendor,
                params=params,
                native_desc=native_desc or "",
            )
        )

        counters["built"] += 1
        counters[f"kind__{kind}"] += 1

        if extra_pairs:
            counters["offers_with_extra_info"] += 1
        if not vendor:
            counters["offers_without_vendor"] += 1
        if not offer.available:
            counters["offers_unavailable"] += 1

        # Лёгкие supplier-side counters для контроля качества merge
        if any(_ci(k) == "совместимость" for k, _ in params):
            counters["offers_with_compat"] += 1
        if any(_ci(k) == "коды расходников" for k, _ in params):
            counters["offers_with_codes"] += 1

        if xml_schema_report.get("extra_count"):
            counters["xml_extra_pairs_total"] += int(xml_schema_report["extra_count"])
        if desc_schema_report.get("extra_count"):
            counters["desc_extra_pairs_total"] += int(desc_schema_report["extra_count"])

        compat_special = compat_report.get("special_counts") or {}
        if compat_special.get("Совместимость"):
            counters["compat_special_seen"] += 1
        if compat_special.get("Коды расходников"):
            counters["codes_special_seen"] += 1

    out_offers.sort(key=lambda x: x.oid)

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
