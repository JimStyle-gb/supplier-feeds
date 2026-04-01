# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/alstyle/builder.py

AlStyle supplier layer — сборка raw offer.

v117:
- сохраняет логику v116;
- добавляет мягкий косметический post-clean для SHIP-кабелей,
  чтобы убрать дубль названия в начале body и хвост "Это ...";
- добавляет безопасный fallback-description для XG PC Game,
  если supplier-body пустой или почти пустой;
- не трогает params, pricing и quality gate.
"""

from __future__ import annotations

import re

from cs.core import OfferOut
from cs.util import norm_ws
from suppliers.alstyle.desc_clean import sanitize_native_desc
from suppliers.alstyle.desc_extract import extract_desc_body_and_spec_pairs
from suppliers.alstyle.models import SourceOffer
from suppliers.alstyle.normalize import (
    build_offer_oid,
    normalize_available,
    normalize_name,
    normalize_price_in,
    normalize_vendor,
)
from suppliers.alstyle.params_xml import collect_xml_params
from suppliers.alstyle.pictures import collect_picture_urls


_NAME_MODEL_RE = re.compile(
    r"\b(?:"
    r"(?:PG|CL|CLI|BCI|GI|PFI|CF|CE|CB|CC|CH|BH)-[A-Z0-9]{2,10}|"
    r"\d{3}[A-Z]\d{5}|"
    r"[A-Z]{1,4}\d-\d{4}-\d{3,4}|"
    r"[A-Z]{1,4}\d-[A-Z]\d{3,4}-\d{3,4}"
    r")\b",
    re.IGNORECASE,
)
_SHORT_DIGIT_SUFFIX_RE = re.compile(r"^\d{1,4}$", re.IGNORECASE)

_XEROX_INIT_KIT_RE = re.compile(
    r"(?iu)\bКомплект\s+инициализации\b.*?\b(Xerox)\s+(AltaLink|VersaLink)\s+([A-Z]?\d{4,5}(?:\s*/\s*[A-Z]?\d{4,5})*)\b"
)
_DEVICE_TOKEN_RE = re.compile(r"^[A-Z]?\d{4,5}$", re.IGNORECASE)

_SAFE_DESC_OVERRIDE_KEYS = {"Совместимость", "Цвет", "Технология", "Ресурс"}

_DIRTY_COMPAT_RE = re.compile(
    r"(?iu)\b(?:"
    r"Гарантированн(?:ый|ого)\s+об(?:ъ|ь)ем\s+отпечатков|"
    r"при\s+5%\s+заполнении|"
    r"формата\s+A4|"
    r"только\s+для\s+продажи\s+на\s+территории|"
    r"Форматы\s+бумаги|Плотность|Емкость|Ёмкость|"
    r"Скорость\s+печати|Интерфейс|Процессор|Память|"
    r"Характеристики|Модель|Совместимые\s+модели|Совместимость|"
    r"Устройства|Устройство|Применение|"
    r"Количество\s+в\s+упаковке|Колличество\s+в\s+упаковке"
    r")\b"
)
_DIRTY_COLOR_RE = re.compile(
    r"(?iu)\b(?:Тип\s+чернил|Ресурс(?:\s+картриджа)?|Количество\s+страниц|Секция\s+аппарата|"
    r"Совместимость|Устройства|Количество\s+цветов|серия|Vivobook|Vector|Gaming|игров)\b"
)
_DIRTY_TECH_RE = re.compile(
    r"(?iu)\b(?:Количество\s+цветов|Тип\s+чернил|Ресурс(?:\s+картриджа)?|Совместимость|"
    r"Устройства|Об(?:ъ|ь)ем\s+картриджа|Секция\s+аппарата|серия)\b"
)
_CLEAN_TECH_RE = re.compile(
    r"(?iu)^(?:Лазерная(?:\s+монохромная|\s+цветная)?|Светодиодная(?:\s+монохромная|\s+цветная)?|"
    r"Струйная|Термоструйная|Матричная|Термосублимационная)$"
)
_CLEAN_RESOURCE_RE = re.compile(r"(?iu)^\d[\d\s.,]*(?:\s*(?:стр\.?|страниц|pages|copies))?$")

_COMPAT_BRAND_HINT_RE = re.compile(
    r"(?iu)\b(?:"
    r"Xerox|Canon|HP|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|"
    r"VersaLink|AltaLink|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser|"
    r"DocuColor|Versant|PrimeLink|DocuCentre|ImagePROGRAF|imageRUNNER|imagePRESS|PIXMA|"
    r"J75|C75|D95|D110|D125"
    r")\b"
)
_XEROX_HEAVY_COMPAT_RE = re.compile(
    r"(?iu)\b(?:VersaLink|AltaLink|WorkCentre(?:\s+Pro)?|CopyCentre|ColorQube|Phaser|DocuColor|Versant)\b"
)


_XG_PC_NAME_RE = re.compile(r"(?iu)^XG\s+PC\s+Game\b")
_SHIP_CABLE_NAME_RE = re.compile(r"(?iu)^Кабель\s+сетевой(?:\s+самонесущий)?\s+SHIP\b")
_SHORT_SHIP_TITLE_RE = re.compile(r"(?iu)^(Кабель\s+сетевой(?:\s+самонесущий)?\s+SHIP\s+[A-Z0-9-]+)")


def _polish_ship_cable_desc(name: str, desc: str) -> str:
    """
    Косметика только для SHIP-кабелей:
    - убираем повтор короткого названия в начале body;
    - убираем лишний старт "Это ...";
    - слегка нормализуем самые частые кривые формы.

    ВАЖНО:
    не трогаем content, если это не SHIP cable narrative.
    """
    title = norm_ws(name)
    body = norm_ws(desc)
    if not title or not body:
        return body
    if not _SHIP_CABLE_NAME_RE.match(title):
        return body

    short_title = title
    m = _SHORT_SHIP_TITLE_RE.match(title)
    if m:
        short_title = norm_ws(m.group(1))

    patterns = [
        rf"(?iu)^{re.escape(title)}\s*(?:[\r\n]+|\s+)?Это\s+",
        rf"(?iu)^{re.escape(short_title)}\s*(?:[\r\n]+|\s+)?Это\s+",
        rf"(?iu)^{re.escape(title)}\s*(?:[\r\n]+|\s+)?",
        rf"(?iu)^{re.escape(short_title)}\s*(?:[\r\n]+|\s+)?",
    ]

    cleaned = body
    for pat in patterns:
        new_val = re.sub(pat, "", cleaned, count=1).strip()
        if new_val != cleaned:
            cleaned = new_val
            break

    cleaned = re.sub(r"(?iu)^Это\s+", "", cleaned).strip()
    cleaned = re.sub(r"(?iu)\bне\s+экранированный\b", "неэкранированный", cleaned)
    cleaned = re.sub(r"(?iu)\b([24])\s*-\s*х\b", r"\1-х", cleaned)
    cleaned = re.sub(r"(?iu)\b100\s+метровый\b", "100-метровый", cleaned)
    cleaned = re.sub(r"(?iu)\s+\.(?=\S)", ". ", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()

    if cleaned:
        cleaned = cleaned[:1].upper() + cleaned[1:]

    return cleaned or body


def _build_xg_fallback_desc(name: str) -> str:
    title = norm_ws(name)
    if not title:
        return ""
    return (
        f"{title} — игровой системный блок для игр, работы и повседневных задач.\n"
        "Точная комплектация и наличие уточняются по текущей конфигурации.\n"
        "Подробности уточняйте в WhatsApp."
    )


def _final_polish_native_desc(name: str, vendor: str, desc_text: str) -> str:
    body = norm_ws(desc_text)
    vendor2 = norm_ws(vendor)
    title = norm_ws(name)

    # XG PC Game: если supplier-body пустой или почти пустой, даём аккуратный fallback.
    if _XG_PC_NAME_RE.match(title) and len(body) < 24:
        return _build_xg_fallback_desc(title)

    # SHIP cables: только косметическая полировка narrative.
    if _SHIP_CABLE_NAME_RE.match(title):
        return _polish_ship_cable_desc(title, body)

    return body


def _is_dirty_value(key: str, value: str) -> bool:
    k = norm_ws(key)
    v = norm_ws(value)
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
        if re.search(r"(?iu)Canon\s+imagePRESS(?:\s+Lite)?\s+[^/]+\s+Canon\s+imageRUNNER", v):
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
    s = norm_ws(v)
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
        xml_len = len(norm_ws(xml_val))
        desc_len = len(norm_ws(desc_val))

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
        k2 = norm_ws(k)
        v2 = norm_ws(v)
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


_KNOWN_XEROX_PREFIXES = (
    "Xerox VL ",
    "Xerox AL ",
    "Xerox WC Pro ",
    "Xerox WC ",
    "Xerox CC ",
    "Xerox CQ ",
    "Xerox DC ",
    "Xerox DCP ",
    "Xerox Versant ",
    "Xerox Phaser ",
    "VL ",
    "AL ",
    "WC Pro ",
    "WC ",
    "CC ",
    "CQ ",
    "DC ",
    "DCP ",
    "Versant ",
    "Phaser ",
)


def _is_heavy_xerox_compat(value: str) -> bool:
    s = norm_ws(value)
    if len(s) < 180:
        return False
    families = {x.casefold() for x in _XEROX_HEAVY_COMPAT_RE.findall(s)}
    return len(families) >= 3


def _compact_xerox_family_names(value: str) -> str:
    s = norm_ws(value).replace(";¶", "; ").replace("¶", " ")
    replacements = (
        (r"(?iu)\bXerox\s+VersaLink\s+", "Xerox VL "),
        (r"(?iu)\bXerox\s+AltaLink\s+", "Xerox AL "),
        (r"(?iu)\bXerox\s+WorkCentre\s+Pro\s+", "Xerox WC Pro "),
        (r"(?iu)\bXerox\s+WorkCentre\s+", "Xerox WC "),
        (r"(?iu)\bXerox\s+CopyCentre\s+", "Xerox CC "),
        (r"(?iu)\bXerox\s+ColorQube\s+", "Xerox CQ "),
        (r"(?iu)\bXerox\s+DocuColor\s+", "Xerox DC "),
        (r"(?iu)\bXerox\s+Digital\s+Color\s+Press\s+", "Xerox DCP "),
        (r"(?iu)\bXerox\s+Versant\s+", "Xerox Versant "),
        (r"(?iu)\bXerox\s+Phaser\s+", "Xerox Phaser "),
        (r"(?iu)\bVersaLink\s+", "VL "),
        (r"(?iu)\bAltaLink\s+", "AL "),
        (r"(?iu)\bWorkCentre\s+Pro\s+", "WC Pro "),
        (r"(?iu)\bWorkCentre\s+", "WC "),
        (r"(?iu)\bCopyCentre\s+", "CC "),
        (r"(?iu)\bColorQube\s+", "CQ "),
        (r"(?iu)\bDocuColor\s+", "DC "),
        (r"(?iu)\bDigital\s+Color\s+Press\s+", "DCP "),
    )
    for pat, repl in replacements:
        s = re.sub(pat, repl, s)

    s = re.sub(r"(?iu)([,;])\s*Xerox\s+", r"\1 ", s)
    s = re.sub(r"\s*/\s*", "/", s)
    s = norm_ws(s).strip(" ,;")
    return s


def _split_xerox_prefix(group: str) -> tuple[str, str]:
    for prefix in _KNOWN_XEROX_PREFIXES:
        if group.startswith(prefix):
            return prefix, group[len(prefix):]
    return "", group


def _summarize_heavy_xerox_compat(value: str) -> str:
    compact = _compact_xerox_family_names(value)
    if len(compact) <= 175:
        return compact

    groups = [norm_ws(x) for x in re.split(r"\s*[,;]\s*", compact) if norm_ws(x)]
    if not groups:
        return compact

    for keep_groups, max_models in ((3, 8), (3, 6), (2, 8), (2, 6), (2, 4), (1, 10), (1, 8)):
        out: list[str] = []
        omitted_groups = len(groups) > keep_groups

        for idx, group in enumerate(groups[:keep_groups]):
            prefix, rest = _split_xerox_prefix(group)
            tokens = [norm_ws(x) for x in rest.split("/") if norm_ws(x)]

            deduped: list[str] = []
            seen: set[str] = set()
            for token in tokens:
                sig = token.casefold()
                if sig in seen:
                    continue
                seen.add(sig)
                deduped.append(token)

            omitted_models = len(deduped) > max_models
            part = (prefix + "/".join(deduped[:max_models])).strip()
            if omitted_models and idx == keep_groups - 1:
                part += " и др."
            out.append(part)

        if omitted_groups and out and not out[-1].endswith("и др."):
            out[-1] += " и др."

        summary = "; ".join(x for x in out if x).strip(" ;")
        if summary and len(summary) <= 175:
            return summary

    return compact[:172].rstrip(" ,;/") + "..."


def _final_reconcile_params(
    params: list[tuple[str, str]],
    desc_params: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    Последний reconcile-pass после merge:
    - если merged Совместимость всё ещё грязная, а clean desc Совместимость есть -> заменить;
    - если merged Xerox Совместимость слишком тяжёлая, а clean desc версия заметно компактнее -> заменить.
    """
    if not params:
        return params

    best_desc = _best_desc_values(desc_params)
    compat_desc = best_desc.get("совместимость")
    if not compat_desc:
        return params

    desc_v = compat_desc[1]
    out: list[tuple[str, str]] = []

    for k, v in params:
        k2 = norm_ws(k)
        v2 = norm_ws(v)

        if k2.casefold() == "совместимость":
            if _is_dirty_value("Совместимость", v2):
                chosen = desc_v
            elif (
                _XEROX_HEAVY_COMPAT_RE.search(v2)
                and _XEROX_HEAVY_COMPAT_RE.search(desc_v)
                and len(desc_v) + 50 < len(v2)
                and _compat_looks_clean(desc_v)
            ):
                chosen = desc_v
            else:
                chosen = v2

            if _is_heavy_xerox_compat(chosen):
                chosen = _summarize_heavy_xerox_compat(chosen)

            out.append((k2, chosen))
            continue

        out.append((k2, v2))

    return out


def merge_params(
    xml_params: list[tuple[str, str]],
    desc_params: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    XML params по умолчанию приоритетнее.
    Description-derived params только дополняют,
    но могут точечно заменить грязные XML значения
    для безопасного набора ключей.
    """
    out: list[tuple[str, str]] = []
    seen_pair: set[tuple[str, str]] = set()
    index_by_key: dict[str, int] = {}

    for k, v in xml_params:
        k2 = norm_ws(k)
        v2 = norm_ws(v)
        if not k2 or not v2:
            continue
        sig = (k2.casefold(), v2.casefold())
        if sig in seen_pair:
            continue
        index_by_key.setdefault(k2.casefold(), len(out))
        out.append((k2, v2))
        seen_pair.add(sig)

    for k, v in desc_params:
        k2 = norm_ws(k)
        v2 = norm_ws(v)
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


def _has_param(params: list[tuple[str, str]], key: str) -> bool:
    kcf = norm_ws(key).casefold()
    return any(norm_ws(k).casefold() == kcf and norm_ws(v) for k, v in params)


def _append_unique(out: list[str], seen: set[str], value: str) -> None:
    v = norm_ws(value)
    if not v:
        return
    sig = v.casefold()
    if sig in seen:
        return
    seen.add(sig)
    out.append(v)


def _append_unique_model_code(out: list[str], seen: set[str], code: str) -> None:
    c = norm_ws(code).upper()
    if not c:
        return
    sig = c.casefold()
    if sig in seen:
        return
    seen.add(sig)
    out.append(c)


def _infer_model_from_name(name: str) -> str:
    n = norm_ws(name)
    if not n:
        return ""

    prepared = re.sub(r"[\(\)\[\],;]+", " / ", n)
    prepared = re.sub(r"\s*/\s*", " / ", prepared)
    parts = [norm_ws(x) for x in prepared.split("/") if norm_ws(x)]

    out: list[str] = []
    seen: set[str] = set()
    last_full: str = ""

    for part in parts:
        full_hits = [m.group(0).upper() for m in _NAME_MODEL_RE.finditer(part)]
        if full_hits:
            for hit in full_hits:
                _append_unique_model_code(out, seen, hit)
                last_full = hit
            continue

        token = norm_ws(part).upper()
        if last_full and _SHORT_DIGIT_SUFFIX_RE.fullmatch(token):
            candidate = (last_full[:-len(token)] + token).upper()
            if _NAME_MODEL_RE.fullmatch(candidate):
                _append_unique_model_code(out, seen, candidate)
                continue

    if out:
        return " / ".join(out)
    return ""


def _expand_device_chain(seq: str) -> list[str]:
    raw_parts = [norm_ws(x).upper() for x in re.split(r"\s*/\s*", seq or "") if norm_ws(x)]
    if not raw_parts:
        return []

    out: list[str] = []
    last_prefix = ""

    for part in raw_parts:
        token = part.strip()
        if not token:
            continue

        m = re.fullmatch(r"([A-Z]?)(\d{4,5})", token)
        if not m:
            continue

        pref, digits = m.groups()
        if pref:
            last_prefix = pref
            out.append(f"{pref}{digits}")
        elif last_prefix:
            out.append(f"{last_prefix}{digits}")
        else:
            out.append(digits)

    return out


def _infer_compat_from_name(name: str) -> str:
    n = norm_ws(name)
    if not n:
        return ""

    m = _XEROX_INIT_KIT_RE.search(n)
    if not m:
        return ""

    brand = norm_ws(m.group(1))
    family = norm_ws(m.group(2))
    seq = norm_ws(m.group(3))

    models = _expand_device_chain(seq)
    if not models:
        return ""

    out: list[str] = []
    seen: set[str] = set()
    for model in models:
        if not _DEVICE_TOKEN_RE.fullmatch(model):
            continue
        _append_unique(out, seen, f"{brand} {family} {model}")

    return " / ".join(out)


def build_offer(
    src: SourceOffer,
    *,
    schema_cfg: dict,
    vendor_blacklist: set[str],
    placeholder_picture: str,
    id_prefix: str = "AS",
) -> tuple[OfferOut | None, bool]:
    raw_id = norm_ws(src.raw_id)
    name = normalize_name(src.name)
    if not raw_id or not name:
        return None, False

    oid = build_offer_oid(raw_id, prefix=id_prefix)
    available = normalize_available(src.available_attr, src.available_tag)
    pictures = collect_picture_urls(src.picture_urls, placeholder_picture=placeholder_picture)
    vendor = normalize_vendor(src.vendor, vendor_blacklist=vendor_blacklist)

    desc_src = sanitize_native_desc(src.description or "", name=name)

    xml_params = collect_xml_params(src.offer_el, schema_cfg) if src.offer_el is not None else []
    desc_body, desc_params = extract_desc_body_and_spec_pairs(desc_src, schema_cfg)
    params = merge_params(xml_params, desc_params)

    if not _has_param(params, "Модель"):
        inferred_model = _infer_model_from_name(name)
        if inferred_model:
            params.append(("Модель", inferred_model))

    if not _has_param(params, "Совместимость"):
        inferred_compat = _infer_compat_from_name(name)
        if inferred_compat:
            params.append(("Совместимость", inferred_compat))

    params = [
        (k, _summarize_heavy_xerox_compat(v) if norm_ws(k).casefold() == "совместимость" and _is_heavy_xerox_compat(v) else v)
        for k, v in params
    ]

    price_in = normalize_price_in(src.purchase_price_text, src.price_text)

    offer = OfferOut(
        oid=oid,
        available=available,
        name=name,
        price=price_in,
        pictures=pictures,
        vendor=vendor,
        params=params,
        native_desc=_final_polish_native_desc(name, vendor, (desc_body or desc_src)),
    )
    return offer, available


def build_offers(
    source_offers: list[SourceOffer],
    *,
    schema_cfg: dict,
    vendor_blacklist: set[str],
    placeholder_picture: str,
    id_prefix: str = "AS",
) -> tuple[list[OfferOut], int, int]:
    out: list[OfferOut] = []
    in_true = 0
    in_false = 0

    for src in source_offers:
        offer, available = build_offer(
            src,
            schema_cfg=schema_cfg,
            vendor_blacklist=vendor_blacklist,
            placeholder_picture=placeholder_picture,
            id_prefix=id_prefix,
        )
        if offer is None:
            continue
        if available:
            in_true += 1
        else:
            in_false += 1
        out.append(offer)

    out.sort(key=lambda x: x.oid)
    return out, in_true, in_false
