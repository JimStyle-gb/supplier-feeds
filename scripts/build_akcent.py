# -*- coding: utf-8 -*-
"""
AkCent adapter (config-driven, strict, no-guessing) — CS template.
Цель: адаптер отдаёт ИДЕАЛЬНЫЙ raw (фильтр, params по schema, коды/совместимость строго),
а cs/core.py делает только общие вещи (цены/keywords/шаблон описания/формат/FEED_META).

Требования:
- Фильтр ассортимента ТОЛЬКО по префиксам name (см. config/filter.yml)
- Никаких "угадываний" кодов/совместимости из name/description (только явные источники)
- Все правила params (allow/alias/normalizers) — в config/schema.yml
"""

from __future__ import annotations

import html
import os
import re
from dataclasses import dataclass
from typing import Any, Iterable
from xml.etree import ElementTree as ET

import requests
import yaml

from cs.core import (
    OfferOut,
    compute_price,
    get_public_vendor,
    next_run_at_hour,
    now_almaty,
    safe_int,
    write_cs_feed,
    write_cs_feed_raw,
)

SUPPLIER_NAME = "AkCent"
SUPPLIER_URL = "https://ak-cent.kz/export/Exchange/article_nw2/Ware02224.xml"
OUT_FILE = "docs/akcent.yml"
RAW_OUT_FILE = "docs/raw/akcent.yml"
OUTPUT_ENCODING = "utf-8"
SCHEDULE_HOUR_ALMATY = 2

BUILD_AKCENT_VERSION = "build_akcent_v54_desc_sanitize_typos"


# ----------------------------- Config loading -----------------------------

def _config_dir() -> str:
    # scripts/build_akcent.py -> scripts/suppliers/akcent/config
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "suppliers", "akcent", "config")


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        obj = yaml.safe_load(f) or {}
    if not isinstance(obj, dict):
        raise ValueError(f"Bad YAML root in {path}: expected mapping")
    return obj


def load_filter_config() -> dict[str, Any]:
    p = os.path.join(_config_dir(), "filter.yml")
    return _load_yaml(p)


def load_schema_config() -> dict[str, Any]:
    p = os.path.join(_config_dir(), "schema.yml")
    return _load_yaml(p)


# ----------------------------- Helpers: text -----------------------------

_LETTER_RE = re.compile(r"[A-Za-zА-Яа-яЁё]")


def _norm_ws(s: str) -> str:
    return " ".join((s or "").replace("\xa0", " ").strip().split())


def _clean_html_to_lines(s: str) -> list[str]:
    if not s:
        return []
    s = html.unescape(s)
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("\r", "\n")
    s = re.sub(r"\n{2,}", "\n", s)
    lines = [ln.strip() for ln in s.split("\n")]
    return [ln for ln in lines if ln]

def _apply_desc_typos(s: str) -> str:
    # ТОЛЬКО очевидные опечатки (без умных замен, чтобы не сломать смысл)
    if not s:
        return s
    fixes = {
        "высококачетсвенную": "высококачественную",
        "приентеров": "принтеров",
        "приентера": "принтера",
        "коeffицент": "коэффициент",
        "коэффицент": "коэффициент",
    }
    out = s
    for a, b in fixes.items():
        out = out.replace(a, b).replace(a.capitalize(), b.capitalize())
    return out


_BR_RE = re.compile(r"(?i)<br\s*/?>")


def _sanitize_native_desc(native_desc_html: str) -> str:
    # Улучшает читабельность длинных <br>-простыней.
    # ВАЖНО: не извлекает новые параметры и не "угадывает" ничего — только форматирование.
    if not native_desc_html:
        return native_desc_html

    # Не трогаем хвост с доп. данными (его добавляет schema)
    marker = "<!-- Доп. данные"
    if marker in native_desc_html:
        base, tail = native_desc_html.split(marker, 1)
        tail = marker + tail
    else:
        base, tail = native_desc_html, ""

    base = _apply_desc_typos(base)

    # если уже есть список — не перекраиваем
    if re.search(r"(?i)<\s*ul\b|<\s*ol\b|<\s*li\b", base):
        return (base + tail) if tail else base

    br_count = len(_BR_RE.findall(base))
    if br_count <= 10:
        return (base + tail) if tail else base

    # Превращаем в аккуратный список: <ul><li>...</li></ul>
    # Сначала превращаем в строки текста (без HTML-тегов)
    lines = _clean_html_to_lines(base)
    if len(lines) < 6:
        return (base + tail) if tail else base

    # Ограничение, чтобы не делать простыню на 200 пунктов
    lines = lines[:80]

    items = "\n".join([f"<li>{html.escape(ln)}</li>" for ln in lines if ln])
    out = f"<ul>\n{items}\n</ul>"
    return (out + "\n\n" + tail) if tail else out



def _dash_ranges(s: str) -> str:
    # "..." / "…" -> "–"
    return (s or "").replace("...", "–").replace("…", "–")


def _interfaces_cleanup(v: str) -> str:
    s = _norm_ws(v)
    if not s:
        return s
    s = s.replace("*", " ")
    s = s.replace(";", ",").replace("/", ",")
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\s{2,}", " ", s).strip(" ,")
    # убираем дубли, сохраняя порядок
    parts = [p.strip() for p in s.split(",") if p.strip()]
    seen = set()
    out = []
    for p in parts:
        key = p.casefold()
        if key not in seen:
            seen.add(key)
            out.append(p)
    return ", ".join(out)


_WARRANTY_NUM_RE = re.compile(r"\b(\d{1,3})\b")
_WARRANTY_MONTHS_RE = re.compile(r"(?i)\b(\d{1,3})\s*(мес|месяц|months?)\b")
_WARRANTY_YEARS_RE = re.compile(r"(?i)\b(\d{1,2})\s*(год|года|лет|years?)\b")

_ZERO_VOLUME_RE = re.compile(r"^0+(?:[\.,]0+)?\s*(?:мл|ml|л|l)?$", re.IGNORECASE)


def _warranty_to_months(v: str) -> str:
    s = _norm_ws(v)
    if not s:
        return s
    cf = s.casefold()
    if cf in {"нет", "без гарантии", "—", "-", "0"}:
        return "0"
    m = _WARRANTY_MONTHS_RE.search(s)
    if m:
        return f"{int(m.group(1))} мес"
    y = _WARRANTY_YEARS_RE.search(s)
    if y:
        return f"{int(y.group(1)) * 12} мес"
    # просто число -> месяцы
    n = _WARRANTY_NUM_RE.fullmatch(s)
    if n:
        return f"{int(n.group(1))} мес"
    # иногда "3 лет" без "год"
    y2 = re.search(r"(?i)\b(\d{1,2})\s*лет\b", s)
    if y2:
        return f"{int(y2.group(1)) * 12} мес"
    return s


def _warranty_drop_zero(v: str) -> str:
    s = _norm_ws(v)
    if not s:
        return s
    return "" if s in {"0", "0 мес", "—", "-"} or s.casefold() in {"нет", "без гарантии"} else s


# ----------------------------- Vendor cleanup/fallback -----------------------------

_COUNTRY_WORDS = {
    "китай", "china",
    "япония", "japan",
    "корея", "korea",
    "сша", "usa",
    "германия", "germany",
    "италия", "italy",
    "испания", "spain",
    "польша", "poland",
    "тайвань", "taiwan",
    "таиланд", "thailand",
    "вьетнам", "vietnam",
    "индия", "india",
}


def _clean_vendor(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return ""
    cf = s.casefold()

    # хвост "proj"/"projector"
    s2 = re.sub(r"\s+(proj\.?|projector)\s*$", "", s, flags=re.IGNORECASE).strip()
    cf2 = s2.casefold()

    if cf in {"epson proj", "epson projector"}:
        return "Epson"

    # если это просто страна — выбрасываем
    if cf2.replace("ё", "е") in _COUNTRY_WORDS:
        return ""
    return s2


def _build_brand_lexicon(offers: Iterable[ET.Element]) -> list[str]:
    brands = set()
    for off in offers:
        v = _clean_vendor(_get_text(off.find("vendor")))
        if v:
            brands.add(v)
    # длинные сначала, чтобы "HP Inc" не перебивалось "HP"
    return sorted(brands, key=lambda x: (-len(x), x.casefold()))


def _infer_vendor_from_name(name: str, lexicon: list[str]) -> str:
    s = name or ""
    if not s:
        return ""
    for b in lexicon:
        # whole-word match (лат/кирилл)
        if re.search(rf"(?i)\b{re.escape(b)}\b", s):
            return b
    # fallback: первое слово как бренд, если похоже на бренд
    w = s.strip().split()[0] if s.strip() else ""
    if w and len(w) <= 20 and _LETTER_RE.search(w):
        return w
    return ""


# ----------------------------- Filter -----------------------------

def _name_passes_prefix(name: str, prefixes: list[str]) -> bool:
    s = (name or "").strip()
    return any(s.startswith(p) for p in prefixes)


def _apply_drop_rules(name: str, rules: list[dict[str, Any]]) -> bool:
    # True => drop
    s = (name or "").casefold()
    for rule in rules:
        groups = rule.get("all_groups")
        if not groups:
            continue
        ok = True
        for g in groups:
            any_of = g.get("any_of") or []
            if not any(any((tok or "").casefold() in s for tok in any_of) for _ in [0]):
                ok = False
                break
        if ok:
            return True
    return False


# ----------------------------- Params extraction -----------------------------

def _get_text(el: ET.Element | None) -> str:
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def _extract_desc_kv_pairs(desc_html: str, min_lines: int) -> list[tuple[str, str]]:
    lines = _clean_html_to_lines(desc_html)
    kv = []
    for i, ln in enumerate(lines):
        if ":" in ln:
            k, v = ln.split(":", 1)
            k = k.strip()
            v = v.strip()
            if k and v:
                kv.append((i, k, v))

    # берём только последовательные блоки длиной >= min_lines
    out: list[tuple[str, str]] = []
    run: list[tuple[int, str, str]] = []
    for i, k, v in kv:
        if not run or i == run[-1][0] + 1:
            run.append((i, k, v))
        else:
            if len(run) >= min_lines:
                out.extend([(kk, vv) for _, kk, vv in run])
            run = [(i, k, v)]
    if run and len(run) >= min_lines:
        out.extend([(kk, vv) for _, kk, vv in run])
    return out


def _key_valid(key: str, key_rules: dict[str, Any]) -> bool:
    k = _norm_ws(key)
    if not k:
        return False
    if not _LETTER_RE.search(k):
        return False
    if int(key_rules.get("max_len", 60)) and len(k) > int(key_rules.get("max_len", 60)):
        return False
    if int(key_rules.get("max_words", 9)) and len(k.split()) > int(key_rules.get("max_words", 9)):
        return False
    banned = {(_norm_ws(x)).casefold() for x in (key_rules.get("banned_exact") or [])}
    if k.casefold() in banned:
        return False
    # ключ начинается с мусорных маркеров
    if re.match(r'^[•\-"\'✅]', k):
        return False
    return True


def _looks_like_model_key(k: str) -> bool:
    # Epson L7160 / HP 3103fdn / и т.п.
    s = _norm_ws(k)
    if not s or len(s) > 40:
        return False
    has_letter = bool(_LETTER_RE.search(s))
    has_digit = bool(re.search(r"\d", s))
    return has_letter and has_digit


def _apply_value_normalizers(k: str, v: str, normals: dict[str, list[str]]) -> str:
    s = v or ""
    seq = normals.get("*", []) + normals.get(k, [])
    for op in seq:
        if op == "trim":
            s = (s or "").strip()
        elif op == "collapse_spaces":
            s = _norm_ws(s)
        elif op == "dash_ranges":
            s = _dash_ranges(s)
        elif op == "interfaces_cleanup":
            s = _interfaces_cleanup(s)
        elif op == "warranty_to_months":
            s = _warranty_to_months(s)
        elif op == "warranty_drop_zero":
            s = _warranty_drop_zero(s)
        elif op == "volume_drop_zero":
            # '0', '0,0', '0,000', '0 мл' -> пусто (выкидываем мусорный объем)
            ss = _norm_ws(s)
            s = "" if (ss and _ZERO_VOLUME_RE.fullmatch(ss)) else s
        # неизвестные опы игнорим (без падения)
    return _norm_ws(s)


def _dedupe_params(params: list[tuple[str, str]], dedupe_rules: dict[str, str]) -> list[tuple[str, str]]:
    if not params:
        return params
    rule_all = dedupe_rules.get("*", "keep_first")
    # warranty special
    by_key: dict[str, list[str]] = {}
    for k, v in params:
        by_key.setdefault(k, []).append(v)

    out: list[tuple[str, str]] = []
    for k, vals in by_key.items():
        rule = dedupe_rules.get(k, rule_all)
        if rule == "keep_first":
            out.append((k, vals[0]))
        elif rule == "prefer_months_max" and k == "Гарантия":
            best = ""
            best_n = -1
            for vv in vals:
                mm = re.search(r"\b(\d{1,3})\b", vv)
                n = int(mm.group(1)) if mm else -1
                if n > best_n:
                    best_n = n
                    best = vv
            if best:
                out.append((k, best))
        else:
            out.append((k, vals[0]))
    return out


def _apply_codes_compat(params: list[tuple[str, str]], kind: str, schema: dict[str, Any]) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """
    Возвращает:
      - params_without_codes_compat
      - extra_info_kv (если что-то некуда положить)
    """
    cc = schema.get("codes_compat") or {}
    if (cc.get("mode") or "").lower() != "strict":
        return params, []

    allow_by_kind: dict[str, list[str]] = schema.get("allow_by_kind") or {}
    allow_keys = set(allow_by_kind.get("default") or []) | set(allow_by_kind.get(kind) or [])

    codes_keys = {str(x) for x in (cc.get("codes_keys") or [])}
    compat_keys = {str(x) for x in (cc.get("compat_keys") or [])}

    patterns = [re.compile(p) for p in (cc.get("code_token_patterns") or [])]

    def extract_tokens(text: str) -> list[str]:
        s = text or ""
        toks = []
        for rx in patterns:
            for m in rx.findall(s):
                if not m:
                    continue
                # m может быть tuple, но у нас простые паттерны
                tok = m if isinstance(m, str) else "".join(m)
                if tok:
                    toks.append(tok)
        # uniq preserve order
        out = []
        seen = set()
        for t in toks:
            tt = t.upper()
            if tt not in seen:
                seen.add(tt)
                out.append(t.upper())
        return out

    codes: list[str] = []
    compat: list[str] = []
    keep: list[tuple[str, str]] = []
    extra: list[tuple[str, str]] = []

    for k, v in params:
        if k in codes_keys:
            codes.extend(extract_tokens(v) or [v])
            continue
        if k in compat_keys:
            # если это только коды -> в codes
            toks = extract_tokens(v)
            if cc.get("move_compat_if_only_codes") and toks:
                rest = v
                for t in toks:
                    rest = re.sub(re.escape(t), "", rest, flags=re.IGNORECASE)
                if not _LETTER_RE.search(rest) and not re.search(r"\d", rest):
                    codes.extend(toks)
                    continue
            compat.append(_norm_ws(v))
            continue

        # KV вида "Epson L7160 = C11..." (явный сигнал)
        if cc.get("allow_model_equals_code_pairs") and _looks_like_model_key(k):
            toks = extract_tokens(v)
            if toks:
                compat.append(_norm_ws(k))
                codes.extend(toks)
                continue

        keep.append((k, v))

    # финальные поля
    def uniq_join(items: list[str]) -> str:
        out = []
        seen = set()
        for it in items:
            s = _norm_ws(it)
            if not s:
                continue
            key = s.casefold()
            if key not in seen:
                seen.add(key)
                out.append(s)
        return ", ".join(out)

    codes_val = uniq_join(codes)
    compat_val = uniq_join(compat)

    if codes_val and "Коды" in allow_keys:
        keep.append(("Коды", codes_val))
    elif codes_val:
        extra.append(("Коды", codes_val))

    if compat_val and "Совместимость" in allow_keys:
        keep.append(("Совместимость", compat_val))
    elif compat_val:
        extra.append(("Совместимость", compat_val))

    return keep, extra


def _apply_schema(name: str, params_raw: list[tuple[str, str]], native_desc: str, schema: dict[str, Any]) -> tuple[list[tuple[str, str]], str]:
    key_rules = schema.get("key_rules") or {}
    discard_exact = {(_norm_ws(x)).casefold() for x in (schema.get("discard_exact") or [])}
    aliases = {(_norm_ws(k)): str(v) for k, v in (schema.get("aliases") or {}).items()}

    # kind
    kind = "default"
    kmap = schema.get("kind_by_name_prefix") or {}
    for k, prefs in kmap.items():
        for p in prefs or []:
            if (name or "").startswith(p):
                kind = k
                break
        if kind != "default":
            break

    allow_by_kind = schema.get("allow_by_kind") or {}
    allow_keys = set(allow_by_kind.get("default") or []) | set(allow_by_kind.get(kind) or [])

    normals = schema.get("value_normalizers") or {}
    unknown_policy = schema.get("unknown_keys") or {}
    unknown_action = (unknown_policy.get("action") or "").lower()
    unknown_max = int(unknown_policy.get("max_pairs") or 0) if unknown_action == "to_extra_info" else 0

    cleaned: list[tuple[str, str]] = []
    extra_info: list[tuple[str, str]] = []

    for k, v in params_raw:
        kk = _norm_ws(k)
        vv = _norm_ws(v)
        if not kk or not vv:
            continue

        # discard exact (служебные)
        if kk.casefold() in discard_exact:
            continue

        # aliases
        if kk in aliases:
            kk = aliases[kk]

        # key quality
        if not _key_valid(kk, key_rules):
            continue

        # normalize value
        vv = _apply_value_normalizers(kk, vv, normals)
        if not vv:
            continue

        # allowlist vs extra
        if kk in allow_keys:
            cleaned.append((kk, vv))
        else:
            if unknown_max and len(extra_info) < unknown_max:
                extra_info.append((kk, vv))

    # dedupe
    cleaned = _dedupe_params(cleaned, schema.get("dedupe_rules") or {})

    # codes/compat strict
    cleaned, extra_cc = _apply_codes_compat(cleaned, kind, schema)
    if unknown_max:
        for k, v in extra_cc:
            if len(extra_info) < unknown_max:
                extra_info.append((k, v))

    # extra_info -> append to native_desc as "Доп. данные"
    if extra_info:
        items = "".join([f"<li><b>{html.escape(k)}:</b> {html.escape(v)}</li>" for k, v in extra_info])
        block = f"\n\n<!-- Доп. данные от поставщика (не превращаем в param) -->\n<ul>\n{items}\n</ul>\n"
        native_desc = (native_desc or "").strip()
        native_desc = (native_desc + block) if native_desc else block.strip()

    return cleaned, native_desc


# ----------------------------- Prices -----------------------------

def _pick_price_kzt(offer: ET.Element) -> int:
    prices_el = offer.find("prices")
    if prices_el is None:
        return 0
    prices = list(prices_el.findall("price"))
    if not prices:
        return 0

    # 1) дилерская
    for p in prices:
        if (p.attrib.get("type", "") or "").strip() == "Цена дилерского портала KZT":
            return safe_int(_get_text(p)) or 0

    # 2) RRP
    for p in prices:
        if (p.attrib.get("type", "") or "").strip().upper() == "RRP":
            return safe_int(_get_text(p)) or 0

    # 3) любая
    for p in prices:
        v = safe_int(_get_text(p))
        if v:
            return v
    return 0


# ----------------------------- Main build -----------------------------

def _download_xml(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def _parse_xml_bytes(data: bytes) -> ET.Element:
    # requests уже отдаёт bytes; ElementTree сам разберёт XML декларацию
    return ET.fromstring(data)


def build() -> None:
    fcfg = load_filter_config()
    scfg = load_schema_config()

    prefixes = [str(x) for x in (fcfg.get("allow_name_prefixes") or [])]
    drop_articles = {str(x) for x in (fcfg.get("drop_articles") or [])}
    drop_rules = fcfg.get("drop_rules") or []

    # XML source (allow override for local debugging)
    url = os.getenv("AKCENT_URL", "").strip() or SUPPLIER_URL
    if url.lower().endswith(".xml") and os.path.exists(url):
        data = open(url, "rb").read()
    else:
        data = _download_xml(url)

    root = _parse_xml_bytes(data)
    offers_el = root.find("shop/offers")
    offers = list(offers_el.findall("offer")) if offers_el is not None else []

    before = len(offers)
    brands = _build_brand_lexicon(offers)

    out_offers: list[OfferOut] = []

    for off in offers:
        name = _get_text(off.find("name"))
        if not name:
            continue

        article = (off.attrib.get("article") or "").strip()
        oid_src = article or (off.attrib.get("id") or "").strip()
        if not oid_src:
            continue

        if not _name_passes_prefix(name, prefixes):
            continue
        if article and article in drop_articles:
            continue
        if _apply_drop_rules(name, drop_rules):
            continue

        oid = f"AC{oid_src}"
        available = (off.attrib.get("available") or "").strip().lower() == "true"

        vendor = _clean_vendor(_get_text(off.find("vendor")))
        if not vendor:
            vendor = _infer_vendor_from_name(name, brands)
        if not vendor:
            vendor = get_public_vendor(SUPPLIER_NAME)

        # pictures
        pics = []
        pic = _get_text(off.find("picture"))
        if pic and pic.startswith("http"):
            pics.append(pic)
        if not pics:
            # core сам умеет placeholder, но лучше страховка
            pics.append(os.getenv("CS_PLACEHOLDER_PICTURE", "") or "https://placehold.co/800x800/png?text=No+Photo")

        # desc + params
        desc_html = _get_text(off.find("description"))
        params_raw: list[tuple[str, str]] = []

        # XML Param/param
        for p in list(off.findall("Param")) + list(off.findall("param")):
            k = (p.attrib.get("name") or "").strip()
            v = _get_text(p)
            if k and v:
                params_raw.append((k, v))

        # description KV-blocks (strict)
        src = scfg.get("sources") or {}
        desc_cfg = src.get("desc_kv_block") or {}
        if desc_cfg.get("enabled"):
            min_lines = int(desc_cfg.get("min_kv_lines") or 5)
            params_raw.extend(_extract_desc_kv_pairs(desc_html, min_lines))

        # apply schema (clean params, strict codes/compat, extra_info -> desc)
        params_clean, desc_clean = _apply_schema(name, params_raw, desc_html, scfg)

        # читабельность описания (только форматирование)
        desc_clean = _sanitize_native_desc(desc_clean or desc_html or "")

        # price
        price_in = _pick_price_kzt(off)
        price_out = compute_price(price_in)

        out_offers.append(
            OfferOut(
                oid=oid,
                available=available,
                name=name,
                price=price_out,
                pictures=pics,
                vendor=vendor,
                params=params_clean,
                native_desc=desc_clean or "",
            )
        )

    build_time = now_almaty()
    next_run = next_run_at_hour(build_time, hour=SCHEDULE_HOUR_ALMATY)

    # RAW + FINAL
    write_cs_feed_raw(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL,
        out_file=RAW_OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
    )

    write_cs_feed(
        out_offers,
        supplier=SUPPLIER_NAME,
        supplier_url=SUPPLIER_URL,
        out_file=OUT_FILE,
        build_time=build_time,
        next_run=next_run,
        before=before,
        encoding=OUTPUT_ENCODING,
        public_vendor=get_public_vendor(SUPPLIER_NAME),
    )

    print(f"[{SUPPLIER_NAME}] before={before} after={len(out_offers)} version={BUILD_AKCENT_VERSION}")


def main() -> None:
    build()


if __name__ == "__main__":
    main()
