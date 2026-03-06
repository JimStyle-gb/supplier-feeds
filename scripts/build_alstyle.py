# -*- coding: utf-8 -*-
"""
AlStyle adapter (AS) — CS-шаблон (config-driven).

Адаптер делает ИДЕАЛЬНЫЙ raw:
- фильтр товаров по categoryId (include) из config/filter.yml
- schema чистит params (drop/aliases/normalizers), без гаданий по совместимости/кодам
- стабильный id/vendorCode с префиксом AS
- pictures: если нет — placeholder
- vendor не должен содержать имя поставщика

Core делает только общее (keywords/description/FEED_META/writer). Для AS в scripts/cs/policy.py
должно быть отключено вмешательство core в params (enable_clean_params=False и т.п.).
"""

from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import requests
import yaml

from cs.core import OfferOut, write_cs_feed, write_cs_feed_raw
from cs.meta import now_almaty, next_run_at_hour
from cs.pricing import compute_price
from cs.util import norm_ws, safe_int


BUILD_ALSTYLE_VERSION = "build_alstyle_v66_canon_multiline_compat_fix"

ALSTYLE_URL_DEFAULT = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
ALSTYLE_OUT_DEFAULT = "docs/alstyle.yml"
ALSTYLE_RAW_OUT_DEFAULT = "docs/raw/alstyle.yml"
ALSTYLE_ID_PREFIX = "AS"

CFG_DIR_DEFAULT = "scripts/suppliers/alstyle/config"
FILTER_FILE_DEFAULT = "filter.yml"
SCHEMA_FILE_DEFAULT = "schema.yml"
POLICY_FILE_DEFAULT = "policy.yml"  # опционально


_RE_HAS_LETTER = re.compile(r"[A-Za-zА-Яа-яЁё]")
_RE_LETTER_SLASH_LETTER = re.compile(r"([A-Za-zА-Яа-яЁё])\s*/\s*([A-Za-zА-Яа-яЁё])")


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _t(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def _parse_id_set(env: str | None, fallback: set[str]) -> set[str]:
    if not env:
        return set(fallback)
    s = env.strip()
    if not s:
        return set(fallback)
    parts = re.split(r"[\s,;]+", s)
    out = {p.strip() for p in parts if p and p.strip()}
    return out or set(fallback)


def _key_quality_ok(k: str, *, require_letter: bool, max_len: int, max_words: int) -> bool:
    kk = norm_ws(k)
    if not kk:
        return False
    if require_letter and not _RE_HAS_LETTER.search(kk):
        return False
    if max_len and len(kk) > int(max_len):
        return False
    if max_words and len(kk.split()) > int(max_words):
        return False
    return True


def _normalize_warranty_to_months(v: str) -> str:
    vv = norm_ws(v)
    if not vv:
        return ""
    low = vv.casefold()
    if low in ("нет", "no", "-", "—"):
        return ""
    m = re.search(r"(\d{1,2})\s*(год|года|лет)\b", low)
    if m:
        n = int(m.group(1))
        return f"{n*12} мес"
    if re.fullmatch(r"\d{1,3}", low):
        return f"{int(low)} мес"
    m = re.search(r"\b(\d{1,3})\b", low)
    if m and ("мес" in low or "month" in low):
        return f"{int(m.group(1))} мес"
    return vv


def _apply_value_normalizers(key: str, val: str, schema: dict[str, Any]) -> str:
    v = norm_ws(val)
    if not v:
        return ""
    vn = (schema.get("value_normalizers") or {})
    ops = vn.get(key) or vn.get(key.casefold()) or []
    for op in ops:
        if op == "warranty_months":
            v = _normalize_warranty_to_months(v)
        elif op == "trim_ws":
            v = norm_ws(v)
    # Нормализация: 'слово/Word' -> 'слово Word' только там, где slash не несёт смысл модели/совместимости
    if norm_ws(key).casefold() not in {"совместимость", "модель", "аналог модели"}:
        v = _RE_LETTER_SLASH_LETTER.sub(r"\1 \2", v)
    v = _sanitize_param_value(key, v)
    return v


def _collect_pictures(offer_el: ET.Element, placeholder: str) -> list[str]:
    pics: list[str] = []
    for p in offer_el.findall("picture"):
        u = norm_ws(_t(p))
        if u:
            pics.append(u)
    if not pics:
        pics = [placeholder]
    return pics


def _collect_params(offer_el: ET.Element, schema: dict[str, Any]) -> list[tuple[str, str]]:
    drop = {str(x).casefold() for x in (schema.get("drop_keys_casefold") or [])}
    aliases = {str(k).casefold(): str(v) for k, v in (schema.get("aliases_casefold") or {}).items()}
    rules = schema.get("key_rules") or {}
    require_letter = bool(rules.get("require_letter", True))
    max_len = int(rules.get("max_len", 60))
    max_words = int(rules.get("max_words", 9))

    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for p in offer_el.findall("param"):
        k0 = p.get("name") or ""
        v0 = _t(p)

        k = norm_ws(k0)
        v = norm_ws(v0)
        if not k or not v:
            continue

        kcf = k.casefold()
        if kcf in aliases:
            k = aliases[kcf]

        if not _key_quality_ok(k, require_letter=require_letter, max_len=max_len, max_words=max_words):
            continue

        if k.casefold() in drop:
            continue

        if k.casefold() == "назначение" and v.casefold() in ("да", "есть"):
            continue
        if k.casefold() == "безопасность" and v.casefold() == "есть":
            continue

        v2 = _apply_value_normalizers(k, v, schema)
        if not v2:
            continue

        sig = (k.casefold(), v2.casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append((k, v2))

    return out


_DESC_SPEC_START_RE = re.compile(r"(?im)^\s*(Характеристики|Основные характеристики)\s*:?\s*$")
_DESC_SPEC_STOP_RE = re.compile(
    r"(?im)^\s*(Преимущества|Комплектация|Условия гарантии|Гарантия|Примечание|Примечания|Особенности|Описание|EUROPRINT)\s*:?\s*$"
)
_DESC_SPEC_LINE_RE = re.compile(
    r"(?im)^\s*"
    r"(Модель|Аналог модели|Совместимость|Совместимые модели|Устройства|Для принтеров|"
    r"Цвет|Цвет печати|Ресурс|Ресурс картриджа|Ресурс картриджа, cтр\.|"
    r"Количество страниц|Кол-во страниц при 5% заполнении А4|Емкость|Ёмкость|Емкость лотка|Ёмкость лотка|"
    r"Степлирование|Дополнительные опции|Применение|Количество в упаковке|Колличество в упаковке)"
    r"\s*(?::|\t+|\s{2,}|[-–—])\s*(.+?)\s*$"
)
_DESC_COMPAT_LINE_RE = re.compile(r"(?im)^\s*Совместим(?:а|о|ы)?\s+с\s+(.+?)\s*$")
_DESC_COMPAT_LABEL_ONLY_RE = re.compile(r"(?im)^\s*(Совместимость|Совместимые модели|Устройства)\s*:?\s*$")
_DESC_COMPAT_SENTENCE_RE = re.compile(
    r"(?is)\bСовместим(?:а|о|ы)?\s+с\s+(.{6,220}?)(?:(?:[.!?](?:\s|$))|\n|$)"
)
_DESC_FOR_DEVICES_SENTENCE_RE = re.compile(
    r"(?is)\bдля\s+(?:устройств|принтеров(?:\s+и\s+МФУ)?|МФУ|аппаратов)\s+(.{6,220}?)(?:(?:[.!?](?:\s|$))|\n|$)"
)
_COMPAT_BRAND_HINT_RE = re.compile(
    r"(?i)\b(Xerox|Canon|HP|Hewlett|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki|"
    r"VersaLink|AltaLink|WorkCentre|DocuCentre|imageRUNNER|i-SENSYS|ECOSYS|bizhub)\b"
)
_COMPAT_MODEL_HINT_RE = re.compile(
    r"(?i)(?:\b[A-Z]{1,8}-?\d{2,5}[A-Z]{0,3}x?\b|\b\d{3,5}[A-Z]{0,3}i?\b|/\s*[A-Z]?\d{2,5}[A-Z]{0,3}x?\b)"
)
_DESC_SPEC_KEY_MAP = {
    "модель": "Модель",
    "аналог модели": "Аналог модели",
    "совместимость": "Совместимость",
    "совместимые модели": "Совместимость",
    "устройства": "Совместимость",
    "для принтеров": "Совместимость",
    "цвет": "Цвет",
    "цвет печати": "Цвет",
    "ресурс": "Ресурс",
    "ресурс картриджа": "Ресурс",
    "ресурс картриджа, cтр.": "Ресурс",
    "количество страниц": "Ресурс",
    "кол-во страниц при 5% заполнении а4": "Ресурс",
    "емкость": "Ёмкость",
    "ёмкость": "Ёмкость",
    "емкость лотка": "Ёмкость",
    "ёмкость лотка": "Ёмкость",
    "степлирование": "Степлирование",
    "дополнительные опции": "Дополнительные опции",
    "применение": "Применение",
    "количество в упаковке": "Количество в упаковке",
    "колличество в упаковке": "Количество в упаковке",
}


def _clean_desc_text(s: str) -> str:
    t = s or ""
    t = t.replace("\r", "\n")
    t = re.sub(r"(?i)<\s*br\s*/?>", "\n", t)
    t = re.sub(r"(?i)</\s*p\s*>", "\n", t)
    t = re.sub(r"(?i)<\s*p[^>]*>", "", t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace("\xa0", " ")
    return t


def _canon_desc_spec_key(k: str) -> str:
    kk = norm_ws(k).casefold()
    return _DESC_SPEC_KEY_MAP.get(kk, norm_ws(k))


def _sanitize_param_value(key: str, val: str) -> str:
    v = norm_ws(val)
    if not v:
        return ""

    # Обрезаем случайно склеенные секции
    v = re.split(
        r"(?i)\b(Преимущества|Комплектация|Условия гарантии|Примечание|Примечания|Особенности|Описание)\b",
        v,
        maxsplit=1,
    )[0].strip(" ;,.-")
    if not v:
        return ""

    kcf = norm_ws(key).casefold()

    if kcf == "совместимость":
        v = re.sub(r"(?i)^совместим(?:а|о|ы)?\s+с\s+", "", v).strip()
        v = re.sub(r"(?i)^для\s+(?:устройств|принтеров(?:\s+и\s+мфу)?|мфу|аппаратов)\s+", "", v).strip()
        v = re.sub(
            r"(?<=[A-Za-zА-Яа-яЁё0-9])(?=(Canon|Xerox|HP|Hewlett|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki)\b)",
            ", ",
            v,
        )
        v = re.sub(r"\s*/\s*", "/", v)
        v = re.sub(r"\s*,\s*", ", ", v)
        v = norm_ws(v)

    if kcf == "ёмкость":
        v = re.sub(r"(?i)^[её]мкость(?:\s+лотка)?\s*[-:–—]\s*", "", v).strip()

    if kcf == "ресурс":
        # Убираем обрезанные хвосты из source, чтобы не тащить мусор в final
        v = re.sub(r"(?i)\.\s*Ресурс указан в соответствии.*$", "", v).strip(" ;,.-")
        v = re.sub(r"(?i)Ресурс указан в соответствии.*$", "", v).strip(" ;,.-")
        v = re.sub(r"(?i)\.\s*ISO\s*/?\s*IEC\s*\d{4,6}\.?\s*[A-Za-zА-Яа-яЁё]?$", "", v).strip(" ;,.-")
        v = re.sub(r"(?<=[A-Za-zА-Яа-яЁё])\s+[A-Za-zА-Яа-яЁё]$", "", v)

    return norm_ws(v)


def _iter_desc_lines(text: str) -> list[str]:
    out: list[str] = []
    for raw in text.splitlines():
        ln = raw.strip(" 	-–—•")
        if not norm_ws(ln):
            continue
        out.append(ln)
    return out


def _is_label_only_line(raw: str) -> bool:
    ln = norm_ws(raw)
    if not ln:
        return False
    if _DESC_COMPAT_LABEL_ONLY_RE.match(ln):
        return True
    if _DESC_SPEC_START_RE.match(ln) or _DESC_SPEC_STOP_RE.match(ln):
        return True
    return False


def _join_compat_lines(lines: list[str]) -> str:
    parts: list[str] = []
    for raw in lines:
        ln = norm_ws(raw)
        if not ln:
            continue
        if _DESC_COMPAT_LABEL_ONLY_RE.match(ln):
            continue
        parts.append(ln)
    if not parts:
        return ""
    s = ", ".join(parts)
    s = re.sub(
        r"(?<=[A-Za-zА-Яа-яЁё0-9])(?=(Canon|Xerox|HP|Hewlett|Epson|Brother|Kyocera|Ricoh|Pantum|Lexmark|Konica|Minolta|OKI|Oki)\b)",
        ", ",
        s,
    )
    return norm_ws(s)


def _extract_multiline_compat_pairs(lines: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    i = 0
    n = len(lines)
    while i < n:
        cur = norm_ws(lines[i])
        if not cur:
            i += 1
            continue
        if not _DESC_COMPAT_LABEL_ONLY_RE.match(cur):
            i += 1
            continue

        j = i + 1
        buf: list[str] = []
        while j < n:
            nxt = norm_ws(lines[j])
            if not nxt:
                break
            if _DESC_SPEC_STOP_RE.match(nxt) or _DESC_SPEC_START_RE.match(nxt):
                break
            parsed = _parse_desc_spec_line(nxt)
            if parsed and parsed[0] != "Совместимость":
                break
            if _DESC_COMPAT_LABEL_ONLY_RE.match(nxt):
                j += 1
                continue
            if re.match(r"(?i)^(Производитель|Устройство|Секция аппарата|Технология печати|Гарантия)\s*$", nxt):
                break
            buf.append(nxt)
            j += 1

        cand = _join_compat_lines(buf)
        if cand and _looks_like_compatibility_value(cand):
            out.append(("Совместимость", cand))
        i = max(j, i + 1)
    return out


def _parse_desc_spec_line(raw: str) -> tuple[str, str] | None:
    ln = norm_ws(raw)
    if not ln:
        return None

    m = _DESC_SPEC_LINE_RE.match(raw)
    if not m:
        compact = re.sub(r"\t+", "  ", raw)
        compact = re.sub(r"\s{3,}", "  ", compact)
        m = _DESC_SPEC_LINE_RE.match(compact)
    if m:
        return (_canon_desc_spec_key(m.group(1)), norm_ws(m.group(2)))

    m = _DESC_COMPAT_LINE_RE.match(raw)
    if m:
        return ("Совместимость", norm_ws(m.group(1)))

    return None



def _looks_like_compatibility_value(val: str) -> bool:
    v = norm_ws(val)
    if not v or len(v) < 6:
        return False
    if not _COMPAT_BRAND_HINT_RE.search(v):
        return False
    if not _COMPAT_MODEL_HINT_RE.search(v):
        return False
    return True


def _extract_sentence_compat_pairs(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    for rx in (_DESC_COMPAT_SENTENCE_RE, _DESC_FOR_DEVICES_SENTENCE_RE):
        for m in rx.finditer(text):
            cand = norm_ws(m.group(1))
            if not cand:
                continue
            cand = re.split(
                r"(?i)\b(Преимущества|Комплектация|Условия гарантии|Примечание|Примечания|Особенности|Описание)\b",
                cand,
                maxsplit=1,
            )[0].strip(" ;,.-")
            if not _looks_like_compatibility_value(cand):
                continue
            out.append(("Совместимость", cand))

    return out


def _validate_desc_pair(key: str, val: str, schema: dict[str, Any]) -> tuple[str, str] | None:
    if not key or not val:
        return None

    drop = {str(x).casefold() for x in (schema.get("drop_keys_casefold") or [])}
    rules = schema.get("key_rules") or {}
    require_letter = bool(rules.get("require_letter", True))
    max_len = int(rules.get("max_len", 60))
    max_words = int(rules.get("max_words", 9))

    if key.casefold() in drop:
        return None
    if not _key_quality_ok(key, require_letter=require_letter, max_len=max_len, max_words=max_words):
        return None

    val2 = _apply_value_normalizers(key, val, schema)
    if not val2:
        return None

    return (key, val2)


def _extract_desc_spec_pairs(desc_src: str, schema: dict[str, Any]) -> list[tuple[str, str]]:
    text = _clean_desc_text(desc_src)
    if not text.strip():
        return []

    candidates: list[tuple[str, str]] = []

    # 1) Строгий блок характеристик
    m = _DESC_SPEC_START_RE.search(text)
    if m:
        block = text[m.end():]
        stop = _DESC_SPEC_STOP_RE.search(block)
        if stop:
            block = block[:stop.start()]
        block_lines = _iter_desc_lines(block)
        for ln in block_lines:
            pair = _parse_desc_spec_line(ln)
            if pair:
                candidates.append(pair)
        candidates.extend(_extract_multiline_compat_pairs(block_lines))

    # 2) Inline-строки по всему описанию: "Модель:", "Совместимость:", "Совместим с", "Емкость лотка -"
    all_lines = _iter_desc_lines(text)
    for ln in all_lines:
        pair = _parse_desc_spec_line(ln)
        if pair:
            candidates.append(pair)
    candidates.extend(_extract_multiline_compat_pairs(all_lines))

    # 3) Фразы в обычных предложениях: "для устройств Xerox ...", "для принтеров и МФУ Canon ..."
    candidates.extend(_extract_sentence_compat_pairs(text))

    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for key, val in candidates:
        checked = _validate_desc_pair(key, val, schema)
        if not checked:
            continue
        sig = (checked[0].casefold(), checked[1].casefold())
        if sig in seen:
            continue
        seen.add(sig)
        out.append(checked)

    return out


def _merge_params(base_params: list[tuple[str, str]], extra_params: list[tuple[str, str]]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = list(base_params)
    seen = {(norm_ws(k).casefold(), norm_ws(v).casefold()) for k, v in base_params}
    seen_keys = {norm_ws(k).casefold() for k, _ in base_params}

    for k, v in extra_params:
        kcf = norm_ws(k).casefold()
        sig = (kcf, norm_ws(v).casefold())
        if sig in seen:
            continue
        if kcf in seen_keys:
            continue
        out.append((k, v))
        seen.add(sig)
        seen_keys.add(kcf)

    return out


def _fetch_xml(url: str, *, timeout: int, login: str | None, password: str | None) -> str:
    auth = (login, password) if (login and password) else None
    r = requests.get(url, timeout=timeout, auth=auth)
    r.raise_for_status()
    return r.text


def main() -> int:
    url = (os.getenv("ALSTYLE_URL") or ALSTYLE_URL_DEFAULT).strip()
    out_file = (os.getenv("OUT_FILE") or ALSTYLE_OUT_DEFAULT).strip()
    raw_out = (os.getenv("RAW_OUT_FILE") or ALSTYLE_RAW_OUT_DEFAULT).strip()
    encoding = (os.getenv("OUTPUT_ENCODING") or "utf-8").strip() or "utf-8"

    env_hour = (os.getenv("SCHEDULE_HOUR_ALMATY") or "").strip()  # legacy env, будет сравнение после чтения policy.yml
    timeout = int(os.getenv("HTTP_TIMEOUT", "90"))

    login = os.getenv("ALSTYLE_LOGIN")
    password = os.getenv("ALSTYLE_PASSWORD")

    cfg_dir = Path(os.getenv("ALSTYLE_CFG_DIR", CFG_DIR_DEFAULT))
    filter_cfg = _read_yaml(cfg_dir / (os.getenv("ALSTYLE_FILTER_FILE") or FILTER_FILE_DEFAULT))
    schema_cfg = _read_yaml(cfg_dir / (os.getenv("ALSTYLE_SCHEMA_FILE") or SCHEMA_FILE_DEFAULT))
    policy_cfg = _read_yaml(cfg_dir / (os.getenv("ALSTYLE_POLICY_FILE") or POLICY_FILE_DEFAULT))

    # schedule hour: источник истины — policy.yml
    hour = int((policy_cfg.get("schedule_hour_almaty") or 1))
    if env_hour:
        try:
            eh = int(env_hour)
            if eh != hour:
                print(f"[build_alstyle] WARN: ignoring SCHEDULE_HOUR_ALMATY={eh}; policy.yml schedule_hour_almaty={hour}")
        except Exception:
            print(f"[build_alstyle] WARN: bad SCHEDULE_HOUR_ALMATY={env_hour!r}; using policy.yml schedule_hour_almaty={hour}")

    placeholder_picture = (
        os.getenv("PLACEHOLDER_PICTURE")
        or policy_cfg.get("placeholder_picture")
        or "https://placehold.co/800x800/png?text=No+Photo"
    )

    fallback_ids = {str(x) for x in (filter_cfg.get("category_ids") or [])}
    allowed = _parse_id_set(os.getenv("ALSTYLE_CATEGORY_IDS"), fallback_ids)

    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour=hour)

    xml_text = _fetch_xml(url, timeout=timeout, login=login, password=password)
    root = ET.fromstring(xml_text)

    offers_in = root.findall(".//offer")
    before = len(offers_in)

    out_offers: list[OfferOut] = []
    in_true = 0
    in_false = 0

    supplier_name = (policy_cfg.get("supplier") or "AlStyle").strip()
    vendor_blacklist = {str(x).casefold() for x in (policy_cfg.get("vendor_blacklist_casefold") or ["alstyle"])}

    for o in offers_in:
        cat = norm_ws(_t(o.find("categoryId")))
        if allowed and (not cat or cat not in allowed):
            continue

        raw_id = norm_ws(o.get("id") or _t(o.find("vendorCode")))
        name = norm_ws(_t(o.find("name")))
        if not name or not raw_id:
            continue

        oid = raw_id if raw_id.upper().startswith(ALSTYLE_ID_PREFIX) else f"{ALSTYLE_ID_PREFIX}{raw_id}"

        av_attr = (o.get("available") or "").strip().lower()
        if av_attr in ("true", "1", "yes"):
            available = True
        elif av_attr in ("false", "0", "no"):
            available = False
        else:
            av_tag = _t(o.find("available")).strip().lower()
            available = av_tag in ("true", "1", "yes")

        if available:
            in_true += 1
        else:
            in_false += 1

        pics = _collect_pictures(o, placeholder_picture)

        params = _collect_params(o, schema_cfg)

        vendor_src = norm_ws(_t(o.find("vendor")))
        if vendor_src and vendor_src.casefold() in vendor_blacklist:
            vendor_src = ""

        desc_src = _t(o.find("description")) or ""
        params = _merge_params(params, _extract_desc_spec_pairs(desc_src, schema_cfg))

        # Стабильный порядок params: приоритетные ключи первыми, затем по алфавиту (чтобы raw ближе к final)
        prio = [str(x) for x in (policy_cfg.get("param_priority") or [])]
        prio_cf = [p.casefold() for p in prio]
        def _pkey(kv):
            k, v = kv
            kcf = (k or "").casefold()
            try:
                idx = prio_cf.index(kcf)
            except ValueError:
                idx = 10_000
            return (idx, kcf, (v or "").casefold())
        if prio:
            params = sorted(params, key=_pkey)

        price_in = safe_int(_t(o.find("purchase_price")))
        if price_in is None:
            price_in = safe_int(_t(o.find("price")))
        price = compute_price(price_in)

        out_offers.append(
            OfferOut(
                oid=oid,
                available=available,
                name=name,
                price=price,
                pictures=pics,
                vendor=vendor_src,
                params=params,
                native_desc=desc_src,
            )
        )

    after = len(out_offers)
    out_offers.sort(key=lambda x: x.oid)

    write_cs_feed_raw(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=raw_out,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        currency_id="KZT",
    )

    changed = write_cs_feed(
        out_offers,
        supplier=supplier_name,
        supplier_url=url,
        out_file=out_file,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=encoding,
        public_vendor=os.getenv("PUBLIC_VENDOR", "CS").strip() or "CS",
        currency_id="KZT",
        param_priority=(policy_cfg.get("param_priority") or None),
    )

    print(
        f"[build_alstyle] OK | version={BUILD_ALSTYLE_VERSION} | offers_in={before} | offers_out={after} | "
        f"in_true={in_true} | in_false={in_false} | changed={'yes' if changed else 'no'} | file={out_file}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
