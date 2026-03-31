# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/akcent/compat.py
AkCent supplier layer — cleanup совместимости / кодов / device-target для узкого потока.

v1:
- чистит только supplier-side поля расходки;
- не тащит supplier-specific логику в core;
- не угадывает compat/codes для нерасходки;
- умеет аккуратно дочищать Коды / Совместимость / Для устройства;
- умеет очень консервативно добирать код из name/model только для consumable.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence, Tuple

# -----------------------------
# Базовые regex
# -----------------------------

_WS_RE = re.compile(r"\s+")
_TAG_RE = re.compile(r"<[^>]+>")
_SPLIT_RE = re.compile(r"\s*(?:[,;|]|/\s(?=[A-Za-zА-Яа-я0-9]))\s*")
_BRACKET_RE = re.compile(r"[\[\]{}()]")

# Коды, которые реально встречаются в текущем потоке AkCent.
# Держим regex умеренно широким, но не агрессивным.
_CODE_RE = re.compile(
    r"\b(?:"
    r"C13T\d{4,6}[A-Z]?"
    r"|T\d{2,4}[A-Z]{0,2}\d{0,2}"
    r"|CF\d{2,4}[A-Z]?"
    r"|CE\d{2,4}[A-Z]?"
    r"|CB\d{2,4}[A-Z]?"
    r"|CC\d{2,4}[A-Z]?"
    r"|CLT-[A-Z0-9]{3,8}"
    r"|TN-?[A-Z0-9]{2,8}"
    r"|DR-?[A-Z0-9]{2,8}"
    r"|MLT-[A-Z0-9]{3,8}"
    r"|W\d{4}[A-Z]?"
    r"|B\d{3,5}[A-Z]?"
    r")\b",
    re.IGNORECASE,
)

# Для расходки по принтерам допускаем более мягкое выделение Epson-серий из начала name.
_EPSON_START_RE = re.compile(r"^(C13T\d{4,6}[A-Z]?)\b", re.IGNORECASE)

# Маркеры мусора в compat/device.
_NOISE_PARTS = (
    "оригинальный",
    "original",
    "совместимый",
    "compatible",
    "картридж",
    "чернила",
    "экономичный набор",
    "емкость для отработанных чернил",
    "ёмкость для отработанных чернил",
    "для принтера",
    "для мфу",
    "для устройства",
    "поддерживаемые модели",
    "поддерживаемые продукты",
    "supported models",
    "supported products",
)

_CONSUMABLE_PREFIXES = (
    "C13T55",
    "Ёмкость для отработанных чернил",
    "Емкость для отработанных чернил",
    "Картридж",
    "Чернила",
    "Экономичный набор",
)


# -----------------------------
# Низкоуровневые helper-ы
# -----------------------------


def _norm_spaces(text: str) -> str:
    return _WS_RE.sub(" ", (text or "").strip())



def _plain(text: str) -> str:
    text = _TAG_RE.sub(" ", text or "")
    text = _BRACKET_RE.sub(" ", text)
    return _norm_spaces(text)



def _cf(text: str) -> str:
    return _plain(text).casefold().replace("ё", "е")



def _dedupe_keep_order(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        x = _norm_spaces(item)
        if not x:
            continue
        key = x.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out



def _split_tokens(value: str) -> List[str]:
    raw = _plain(value)
    if not raw:
        return []
    parts = _SPLIT_RE.split(raw)
    out: List[str] = []
    for part in parts:
        x = _norm_spaces(part.strip(" .:-"))
        if not x:
            continue
        out.append(x)
    return _dedupe_keep_order(out)



def _is_consumable_name(name: str) -> bool:
    src = _norm_spaces(name)
    return any(src.startswith(p) for p in _CONSUMABLE_PREFIXES)



def _looks_like_device_phrase(text: str) -> bool:
    s = _cf(text)
    if not s:
        return False
    return any(mark in s for mark in (
        "epson", "hp", "canon", "brother", "xerox", "pantum", "samsung",
        "ecotank", "workforce", "expression", "surecolor", "laserjet",
        "pixma", "imageclass", "dcp", "mfc", "l3150", "l3250", "l8050",
    ))



def _clean_noise_prefixes(text: str) -> str:
    x = _norm_spaces(text)
    if not x:
        return ""
    y = x
    for marker in _NOISE_PARTS:
        y = re.sub(re.escape(marker), " ", y, flags=re.IGNORECASE)
    y = re.sub(r"^(?:для|подходит для|совместим с|совместимость|compatibility)\s+", "", y, flags=re.IGNORECASE)
    y = re.sub(r"\b(?:модель|модели|серия|series)\b\s*:?", " ", y, flags=re.IGNORECASE)
    y = _norm_spaces(y.strip(" ,;:-"))
    return y



def _titleish_device(text: str) -> str:
    x = _norm_spaces(text)
    if not x:
        return ""
    words = []
    for token in x.split():
        if re.fullmatch(r"[A-Z0-9-]{2,}", token):
            words.append(token)
        elif re.search(r"\d", token):
            # модели с цифрами не трогаем
            words.append(token)
        else:
            words.append(token[:1].upper() + token[1:])
    return _norm_spaces(" ".join(words))


# -----------------------------
# Коды
# -----------------------------


def extract_codes_from_text(*texts: Optional[str]) -> List[str]:
    found: List[str] = []
    for text in texts:
        if not text:
            continue
        for m in _CODE_RE.finditer(text):
            found.append(m.group(0).upper())
    return _dedupe_keep_order(found)



def extract_primary_code_from_name(name: str) -> str:
    src = _norm_spaces(name)
    if not src:
        return ""
    m = _EPSON_START_RE.search(src)
    if m:
        return m.group(1).upper()
    codes = extract_codes_from_text(src)
    return codes[0] if codes else ""



def clean_codes_value(value: str) -> str:
    codes = extract_codes_from_text(value)
    return ", ".join(codes)




_RE_PRIMARY_CONSUMABLE_CODE = re.compile(r"(?iu)\bC(?:11|12|13|33)[A-Z0-9]{5,10}\b")
_RE_SECONDARY_T_CODE = re.compile(r"(?iu)\bT[0-9A-Z]{5,10}\b")


def pick_name_primary_code(name: str) -> str:
    m = _RE_PRIMARY_CONSUMABLE_CODE.search(_clean_text(name))
    return _clean_text(m.group(0)).upper() if m else ""


def pick_secondary_t_code(name: str, desc: str, primary: str) -> str:
    joined = " / ".join([_clean_text(name), _clean_text(desc)]).upper()

    raw_tokens = re.findall(r"(?iu)\bT\d[A-Z0-9]{4,10}\b", joined)
    for token in raw_tokens:
        code = _clean_text(token).upper()
        code = re.split(
            r"(?iu)(?:ULTRACHROME|SINGLEPACK|INK|CARTRIDGE|BLACK|CYAN|MAGENTA|YELLOW|PHOTO|HDX|HD)",
            code,
        )[0]
        code = _clean_text(code)
        m = re.match(r"(?iu)^T\d[A-Z0-9]{4,10}$", code)
        if m:
            code = _clean_text(m.group(0)).upper()
            if code and code != primary:
                return code

    glued = re.search(
        r"(?iu)(T\d[A-Z0-9]{4,10})(?=ULTRACHROME|SINGLEPACK|INK|CARTRIDGE|BLACK|CYAN|MAGENTA|YELLOW|PHOTO|HDX|HD|$)",
        joined,
    )
    if glued:
        code = _clean_text(glued.group(1)).upper()
        if code and code != primary:
            return code

    return ""


def should_force_consumable_model(current_model: str, primary_code: str, name: str) -> bool:
    cur = _clean_text(current_model).upper()
    if not primary_code:
        return False
    if not cur:
        return True
    if cur == primary_code:
        return False
    if " " in _clean_text(current_model):
        return True
    if cur.startswith(("C11", "C12", "C13", "C33")):
        return True
    if cur in _clean_text(name).upper() and cur != primary_code:
        return True
    return False



# -----------------------------
# Совместимость / Для устройства
# -----------------------------


def _split_compat_chunks(value: str) -> List[str]:
    text = _clean_noise_prefixes(value)
    if not text:
        return []

    # Сначала грубо режем списки
    rough = _split_tokens(text)
    out: List[str] = []
    for item in rough:
        item = re.sub(r"\b(?:и|and)\b", " ", item, flags=re.IGNORECASE)
        item = _norm_spaces(item.strip(" ,;:-"))
        if not item:
            continue
        # Оставляем только то, что похоже на устройство/серию
        if _looks_like_device_phrase(item) or re.search(r"[A-Za-zА-Яа-я]+\d{2,}", item):
            out.append(_titleish_device(item))
    return _dedupe_keep_order(out)



def clean_compat_value(value: str) -> str:
    items = _split_compat_chunks(value)
    return ", ".join(items)



def clean_device_value(value: str) -> str:
    items = _split_compat_chunks(value)
    if not items:
        x = _titleish_device(_clean_noise_prefixes(value))
        return x
    return ", ".join(items)




# -----------------------------
# Consumable device normalization / extraction
# -----------------------------

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

_RE_CONSUMABLE_MODEL_TAIL = re.compile(
    r"(?iu)(?:поддерживаемые\s+модели(?:\s+принтеров|\s+устройств|\s+техники)?|"
    r"совместимые\s+модели(?:\s+техники)?|совместимые\s+продукты(?:\s+для)?)\s*:?[ \t]*(.+)$"
)
_RE_FOR_DEVICE_TAIL = re.compile(
    r"(?iu)(?:^|\b)(?:для|for)\s+((?:Epson\s+)?(?:WorkForce|SureColor|EcoTank|Stylus(?:\s+Pro)?)\b.+)$"
)
_RE_L_SERIES_PACK = re.compile(r"(?iu)\bL(\d{3,5}(?:/\d{3,5})+)\b")
_RE_WORKFORCE_MODEL = re.compile(r"(?iu)\b(?:Epson\s+)?WorkForce\s+[A-Z0-9-]+\b")
_RE_SURECOLOR_MODEL = re.compile(r"(?iu)\b(?:Epson\s+)?SureColor\s+SC-[A-Z0-9-]+\b")
_RE_ECOTANK_MODEL = re.compile(r"(?iu)\b(?:Epson\s+)?EcoTank\s+[A-Z0-9-]+\b")
_RE_STYLUS_MODEL = re.compile(r"(?iu)\b(?:Epson\s+)?Stylus(?:\s+Pro)?\s+[A-Z0-9-]+\b")
_RE_GENERIC_EPS_MODEL = re.compile(r"(?iu)\b(?:WF|SC|ET|L)-?[A-Z0-9]{2,}\b")


def _dedupe_text_items(items: Iterable[str]) -> List[str]:
    return _dedupe_keep_order(items)


def _clean_text(value: object) -> str:
    return _norm_spaces(str(value or ""))


def _title_eps_family(value: str) -> str:
    v = _clean_text(value)
    if not v:
        return ""
    v = re.sub(r"(?iu)\bepson\b", "Epson", v)
    v = re.sub(r"(?iu)\bsurecolor\b", "SureColor", v)
    v = re.sub(r"(?iu)\bworkforce\b", "WorkForce", v)
    v = re.sub(r"(?iu)\becotank\b", "EcoTank", v)
    v = re.sub(r"(?iu)\bstylus\b", "Stylus", v)
    v = re.sub(r"(?iu)\bw/?o\s*stand\b", "", v)
    return _clean_text(v)


def extract_models_from_text(text: str) -> str:
    src = _clean_text(text)
    if not src:
        return ""

    items: List[str] = []
    for rx in (_RE_SURECOLOR_MODEL, _RE_WORKFORCE_MODEL, _RE_ECOTANK_MODEL, _RE_STYLUS_MODEL):
        items.extend([_title_eps_family(m.group(0)) for m in rx.finditer(src)])

    if not items:
        for m in _RE_GENERIC_EPS_MODEL.finditer(src):
            token = _clean_text(m.group(0)).upper().replace('SC ', 'SC-').replace('WF ', 'WF-').replace('ET ', 'ET-').replace('L ', 'L')
            token = token.replace('SC- ', 'SC-').replace('WF- ', 'WF-').replace('ET- ', 'ET-')
            if token.startswith('SC-'):
                items.append(f"Epson SureColor {token}")
            elif token.startswith('WF-'):
                items.append(f"Epson WorkForce {token}")
            elif token.startswith('ET-'):
                items.append(f"Epson EcoTank {token}")
            elif token.startswith('L') and len(token) > 1 and token[1:].isdigit():
                items.append(f"Epson {token}")

    return " / ".join(_dedupe_text_items([x for x in items if x]))


def extract_explicit_epson_devices(text: str) -> str:
    src = _clean_text(text)
    if not src:
        return ""

    items: List[str] = []
    for rx in (
        re.compile(r"(?iu)(?:Epson\s+)?WorkForce\s+WF-[A-Z0-9-]+"),
        re.compile(r"(?iu)(?:Epson\s+)?SureColor\s+SC-[A-Z0-9-]+"),
        re.compile(r"(?iu)(?:Epson\s+)?EcoTank\s+ET-[A-Z0-9-]+"),
        re.compile(r"(?iu)(?:Epson\s+)?Stylus(?:\s+Pro)?\s+[A-Z0-9-]+"),
    ):
        items.extend([_title_eps_family(m.group(0)) for m in rx.finditer(src)])

    if not items:
        for m in re.finditer(r"(?iu)\b(?:WF|SC|ET)-?[A-Z0-9]{2,}\b", src):
            token = _clean_text(m.group(0)).upper().replace('SC ', 'SC-').replace('WF ', 'WF-').replace('ET ', 'ET-')
            token = token.replace('SC- ', 'SC-').replace('WF- ', 'WF-').replace('ET- ', 'ET-')
            if token.startswith('WF-'):
                items.append(f"Epson WorkForce {token}")
            elif token.startswith('SC-'):
                items.append(f"Epson SureColor {token}")
            elif token.startswith('ET-'):
                items.append(f"Epson EcoTank {token}")

    return " / ".join(_dedupe_text_items([x for x in items if x]))


def extract_direct_epson_device_list(text: str) -> str:
    src = _clean_text(text)
    if not src:
        return ""

    items: List[str] = []
    patterns = (
        re.compile(r"(?iu)(?:Epson\s+)?WorkForce\s+WF-[A-Z0-9-]+"),
        re.compile(r"(?iu)(?:Epson\s+)?SureColor\s+SC-[A-Z0-9-]+"),
        re.compile(r"(?iu)(?:Epson\s+)?EcoTank\s+ET-[A-Z0-9-]+"),
        re.compile(r"(?iu)(?:Epson\s+)?Stylus(?:\s+Pro)?\s+[A-Z0-9-]+"),
    )
    for rx in patterns:
        for m in rx.finditer(src):
            items.append(_title_eps_family(m.group(0)))

    if not items:
        last_family = ""
        for token in re.findall(r"(?iu)\b(?:WF|SC|ET)-?[A-Z0-9-]{2,}\b", src):
            t = _clean_text(token).upper().replace('SC ', 'SC-').replace('WF ', 'WF-').replace('ET ', 'ET-')
            if t.startswith('WF-'):
                items.append(f"Epson WorkForce {t}")
                last_family = 'WF'
            elif t.startswith('SC-'):
                items.append(f"Epson SureColor {t}")
                last_family = 'SC'
            elif t.startswith('ET-'):
                items.append(f"Epson EcoTank {t}")
                last_family = 'ET'
            elif last_family == 'WF' and re.match(r"^[A-Z0-9-]{4,}$", t):
                items.append(f"Epson WorkForce WF-{t}")
            elif last_family == 'SC' and re.match(r"^[A-Z0-9-]{4,}$", t):
                items.append(f"Epson SureColor SC-{t}")
            elif last_family == 'ET' and re.match(r"^[A-Z0-9-]{4,}$", t):
                items.append(f"Epson EcoTank ET-{t}")

    return " / ".join(_dedupe_text_items([x for x in items if x]))


def normalize_consumable_device_value(value: str) -> str:
    src = _clean_text(value)
    if not src:
        return ""

    src = re.sub(r"(?iu)\bw/\s*o\s*stand\b", "", src)
    src = re.sub(r"(?iu)\bsurecolor\b", "SureColor", src)
    src = re.sub(r"(?iu)\bworkforce\s+pro\b", "WorkForce Pro", src)
    src = re.sub(r"(?iu)\bworkforce\b", "WorkForce", src)
    src = re.sub(r"(?iu)\becotank\b", "EcoTank", src)
    src = re.sub(r"(?iu)\bstylus\s+pro\b", "Stylus Pro", src)
    src = re.sub(r"(?iu)\bexpression\b", "Expression", src)
    src = re.sub(r"(?iu)\bpixma\b", "PIXMA", src)
    src = re.sub(r"(?iu)\blaserjet\b", "LaserJet", src)
    src = re.sub(r"(?iu)\b([A-Z]{1,3})-\s+([A-Z0-9])", r"\1-\2", src)

    chunks: List[str] = []
    last_family = ""
    for m in _RE_DEVICE_MODEL.finditer(src):
        family = _clean_text(m.group(1))
        model = _clean_text(m.group(2))
        if not model:
            continue
        model = re.sub(r"(?iu)\bw/\s*o\s*stand\b", "", model)
        model = _clean_text(model)
        if family:
            last_family = family
        elif last_family and re.match(r"(?iu)^(?:P|T|WF|ET|L|SC-|B|C|M)\d", model):
            family = last_family
        item = f"{family} {model}".strip() if family else model
        chunks.append(item)

    if chunks:
        return " / ".join(_dedupe_text_items(chunks))

    fallback = _clean_text(clean_device_value(src))
    fallback = re.sub(r"(?iu)\bw/\s*o\s*stand\b", "", fallback)
    parts = [x for x in re.split(r"\s*(?:,|/)\s*", fallback) if _clean_text(x)]
    cleaned = _dedupe_text_items(parts)
    return " / ".join(cleaned) if cleaned else ""


def looks_generic_device_value(value: str) -> bool:
    low = _cf(value)
    if not low:
        return True
    if any(x in low for x in ["широкоформатный принтер", "принтер", "мфу", "фотопечать", "устройств epson"]):
        return True
    return not bool(_RE_DEVICE_MODEL.search(value))


def normalize_epson_device_list(value: str) -> str:
    src = _clean_text(value)
    if not src:
        return ""
    src = re.sub(r"(?iu)\bSC-\s+", "SC-", src)
    src = re.sub(r"(?iu)\bWF-\s+", "WF-", src)
    src = re.sub(r"(?iu)\bET-\s+", "ET-", src)
    src = re.sub(r"(?iu)(?<!Epson\s)\bSureColor\b", "Epson SureColor", src)
    src = re.sub(r"(?iu)(?<!Epson\s)\bWorkForce\b", "Epson WorkForce", src)
    src = re.sub(r"(?iu)(?<!Epson\s)\bEcoTank\b", "Epson EcoTank", src)
    src = re.sub(r"(?iu)(?<!Epson\s)\bStylus(?:\s+Pro)?\b", lambda m: 'Epson ' + _clean_text(m.group(0)), src)
    models = extract_models_from_text(src) or src
    parts = _dedupe_text_items([_clean_text(x) for x in re.split(r"\s*/\s*", models) if _clean_text(x)])
    return " / ".join(parts)


def extract_consumable_device_candidate(name: str, desc: str) -> str:
    text = _clean_text(desc)
    for line in text.split("\n"):
        line = _clean_text(line)
        if not line:
            continue

        m = _RE_CONSUMABLE_MODEL_TAIL.search(line)
        if m:
            cand = normalize_consumable_device_value(m.group(1))
            models = extract_models_from_text(cand)
            if models:
                return models
            if cand:
                return cand

        m_for = _RE_FOR_DEVICE_TAIL.search(line)
        if m_for:
            cand = normalize_consumable_device_value(m_for.group(1))
            models = extract_models_from_text(cand)
            if models:
                return models

    models = extract_models_from_text(text)
    if models:
        return models

    m2 = _RE_L_SERIES_PACK.search(_clean_text(name))
    if m2:
        nums = [x for x in m2.group(1).split('/') if _clean_text(x)]
        items = [f"Epson L{n}" for n in nums]
        return " / ".join(_dedupe_text_items(items))

    return ""


def normalize_consumable_device_params(params: Sequence[Tuple[str, str]], *, kind: str) -> List[Tuple[str, str]]:
    if kind != "consumable" or not params:
        return list(params or [])

    out: List[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()

    for key, value in params:
        k = _clean_text(key)
        v = _clean_text(value)
        if not k or not v:
            continue
        if _cf(k) in {"для устройства", "совместимость"}:
            v2 = normalize_consumable_device_value(v)
            if v2:
                pair = (k, v2)
                if pair not in seen:
                    seen.add(pair)
                    out.append(pair)
            continue
        pair = (k, v)
        if pair not in seen:
            seen.add(pair)
            out.append(pair)

    return out

# -----------------------------
# Сборка supplier-side cleanup
# -----------------------------


def _get_param(params: Sequence[Tuple[str, str]], key: str) -> str:
    key_cf = key.casefold()
    for k, v in params:
        if (k or "").casefold() == key_cf and (v or "").strip():
            return _norm_spaces(v)
    return ""



def _set_param(params: Sequence[Tuple[str, str]], key: str, value: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    inserted = False
    key_cf = key.casefold()
    clean_value = _norm_spaces(value)
    for k, v in params:
        if (k or "").casefold() == key_cf:
            if clean_value and not inserted:
                out.append((key, clean_value))
                inserted = True
            continue
        out.append((k, v))
    if clean_value and not inserted:
        out.append((key, clean_value))
    return out



def _drop_param(params: Sequence[Tuple[str, str]], key: str) -> List[Tuple[str, str]]:
    key_cf = key.casefold()
    return [(k, v) for k, v in params if (k or "").casefold() != key_cf]



def reconcile_consumable_params(
    params: Sequence[Tuple[str, str]],
    *,
    name: str = "",
    model: str = "",
    kind: str = "",
) -> List[Tuple[str, str]]:
    """Чистит params только для consumable-группы.

    Ничего не делает для других kind — это важно для безопасности.
    """
    if kind and kind != "consumable":
        return list(params)
    if not kind and not _is_consumable_name(name):
        return list(params)

    out = list(params)

    # 1) Коды
    raw_codes = _get_param(out, "Коды")
    clean_codes = clean_codes_value(raw_codes)
    if not clean_codes:
        clean_codes = extract_primary_code_from_name(name)
    if not clean_codes:
        clean_codes = clean_codes_value(model)
    if clean_codes:
        out = _set_param(out, "Коды", clean_codes)

    # 2) Совместимость
    raw_compat = _get_param(out, "Совместимость")
    clean_compat = clean_compat_value(raw_compat)
    if clean_compat:
        out = _set_param(out, "Совместимость", clean_compat)
    elif raw_compat:
        out = _drop_param(out, "Совместимость")

    # 3) Для устройства
    raw_device = _get_param(out, "Для устройства")
    clean_device = clean_device_value(raw_device)
    if clean_device:
        out = _set_param(out, "Для устройства", clean_device)
    elif raw_device:
        out = _drop_param(out, "Для устройства")

    # 4) Если есть Совместимость, но нет Для устройства — аккуратно дублируем укороченно
    final_device = _get_param(out, "Для устройства")
    final_compat = _get_param(out, "Совместимость")
    if final_compat and not final_device:
        # Держим device короче — максимум первые 3 модели
        parts = _split_compat_chunks(final_compat)
        if parts:
            out = _set_param(out, "Для устройства", ", ".join(parts[:3]))

    # 5) Пустые/шумные значения вычищаем
    for key in ("Коды", "Совместимость", "Для устройства"):
        value = _get_param(out, key)
        if not value or value in {"-", "—", "..."}:
            out = _drop_param(out, key)

    return out



def reconcile_params(
    params: Sequence[Tuple[str, str]],
    *,
    name: str = "",
    model: str = "",
    kind: str = "",
) -> List[Tuple[str, str]]:
    """Общая точка входа для builder.py."""
    return reconcile_consumable_params(params, name=name, model=model, kind=kind)


__all__ = [
    "extract_codes_from_text",
    "extract_primary_code_from_name",
    "pick_name_primary_code",
    "pick_secondary_t_code",
    "should_force_consumable_model",
    "clean_codes_value",
    "clean_compat_value",
    "clean_device_value",
    "extract_models_from_text",
    "extract_explicit_epson_devices",
    "extract_direct_epson_device_list",
    "extract_consumable_device_candidate",
    "normalize_consumable_device_value",
    "normalize_consumable_device_params",
    "normalize_epson_device_list",
    "looks_generic_device_value",
    "reconcile_consumable_params",
    "reconcile_params",
]
