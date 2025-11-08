# -*- coding: utf-8 -*-
# build_alstyle.py v24
# Change: avoid sticking "<offer ...><categoryId>" by inserting exactly one newline
# after the opening <offer> tag. Rest logic from v23 (sorting + blank lines).

import re, sys, pathlib, requests

URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
LOGIN = "info@complex-solutions.kz"
PASSWORD = "Aa123456"
OUT_PATH = pathlib.Path("docs/alstyle.yml")
ENC_OUT = "windows-1251"

CAT_ALLOW_STR = "3540,3541,3542,3543,3544,3545,3566,3567,3569,3570,3580,3688,3708,3721,3722,4889,4890,4895,5017,5075,5649,5710,5711,5712,5713,21279,21281,21291,21356,21367,21368,21369,21370,21371,21372,21451,21498,21500,21501,21572,21573,21574,21575,21576,21578,21580,21581,21583,21584,21585,21586,21588,21591,21640,21664,21665,21666,21698"
ALLOWED_CATS = {s.strip() for s in CAT_ALLOW_STR.split(",") if s.strip()}

def _dec(data, enc):
    for e in [enc, "utf-8", "windows-1251", "cp1251", "latin-1"]:
        if not e: continue
        try: return data.decode(e)
        except Exception: pass
    return data.decode("utf-8", errors="replace")

def _save(text):
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_bytes(text.encode(ENC_OUT, errors="replace"))

def _move_available_attr(attrs: str, body: str):
    m_av = re.search(r"(?is)<\s*available\s*>\s*(.*?)\s*<\s*/\s*available\s*>", body)
    if not m_av: return attrs, body
    val = m_av.group(1)
    if re.search(r'\savailable\s*=\s*"(?:[^"]*)"', attrs, flags=re.I):
        attrs = re.sub(r'(\savailable\s*=\s*")([^"]*)(")', lambda g: g.group(1)+val+g.group(3), attrs, flags=re.I)
    elif re.search(r"\savailable\s*=\s*'(?:[^']*)'", attrs, flags=re.I):
        attrs = re.sub(r"(\savailable\s*=\s*')([^']*)(')", lambda g: g.group(1)+val+g.group(3), attrs, flags=re.I)
    else:
        attrs = attrs.rstrip() + f' available="{val}"'
    return attrs, body

def _copy_purchase_into_price(body: str) -> str:
    m_pp = re.search(r"(?is)<\s*purchase_?price\s*>\s*(.*?)\s*<\s*/\s*purchase_?price\s*>", body)
    if not m_pp: return body
    val = m_pp.group(1)
    def _repl(m): return m.group(1) + val + m.group(3)
    return re.sub(r"(?is)(<\s*price\s*>)(.*?)(<\s*/\s*price\s*>)", _repl, body, count=1)

def _remove_simple_tags(body: str) -> str:
    def rm(text, name_regex):
        rx = re.compile(
            rf"(?is)"
            rf"(?P<pre_ws>[ \t]*)"
            rf"(?P<pre_nl>\r?\n)?"
            rf"<\s*(?:{name_regex})\b[^>]*>.*?<\s*/\s*(?:{name_regex})\s*>"
            rf"(?P<post_nl>\r?\n)?"
            rf"(?P<post_ws>[ \t]*)"
        )
        def repl(m): return "\n" if (m.group('pre_nl') or m.group('post_nl')) else ""
        return rx.sub(repl, text)
    body = rm(body, r"quantity_in_stock")
    body = rm(body, r"purchase_?price")
    body = rm(body, r"available")
    body = rm(body, r"url")
    body = rm(body, r"quantity")
    return body

def _remove_param_by_name(body: str) -> str:
    def _norm(s: str) -> str:
        s = s.lower().replace("ё", "е")
        return re.sub(r"[\s\-]+", "", s)
    to_drop = {_norm(x) for x in [
        "Артикул","Штрихкод","Штрих-код","Снижена цена","Благотворительность",
        "Назначение","Код ТН ВЭД","Объём","Объем","Код товара Kaspi","Новинка"
    ]}
    rx_line_pair = re.compile(r"(?im)^[ \t]*<\s*param\b(?P<attrs>[^>]*)>.*?</\s*param\s*>[ \t]*\r?\n?")
    rx_line_self = re.compile(r"(?im)^[ \t]*<\s*param\b(?P<attrs>[^>]*)/\s*>[ \t]*\r?\n?")
    def _line_cb(m):
        a = m.group("attrs"); ma = re.search(r'(?is)\bname\s*=\s*(["\'])(.*?)\1', a)
        if ma and _norm(ma.group(2)) in to_drop: return ""
        return m.group(0)
    body = rx_line_pair.sub(_line_cb, body)
    body = rx_line_self.sub(_line_cb, body)
    rx_inline_pair = re.compile(r"(?is)<\s*param\b(?P<attrs>[^>]*)>.*?</\s*param\s*>")
    rx_inline_self = re.compile(r"(?is)<\s*param\b(?P<attrs>[^>]*)/\s*>")
    def _inline_cb(m):
        a = m.group("attrs"); ma = re.search(r'(?is)\bname\s*=\s*(["\'])(.*?)\1', a)
        if ma and _norm(ma.group(2)) in to_drop: return ""
        return m.group(0)
    body = rx_inline_pair.sub(_inline_cb, body)
    body = rx_inline_self.sub(_inline_cb, body)
    body = re.sub(r"(?m)(?:^[ \t\u00A0]*\r?\n){2,}", "\n", body)
    return body

def _sort_offer_tags(body: str) -> str:
    def pop_all(text, rx):
        items = []
        def repl(m):
            items.append(m.group(0))
            return ""
        return rx.sub(repl, text), items
    def pop_one(text, rx):
        m = rx.search(text)
        if not m: return text, ""
        return text[:m.start()] + text[m.end():], m.group(0)

    rx_tag = lambda n: re.compile(rf"(?is)<\s*{n}\b[^>]*>.*?</\s*{n}\s*>")
    rx_picture = re.compile(r"(?is)<\s*picture\b[^>]*>.*?</\s*picture\s*>")
    rx_param = re.compile(r"(?is)<\s*param\b[^>]*?(?:/?>.*?</\s*param\s*>|/\s*>)")

    text = body
    text, categoryId = pop_one(text, rx_tag("categoryId"))
    text, vendorCode = pop_one(text, rx_tag("vendorCode"))
    text, name = pop_one(text, rx_tag("name"))
    text, price = pop_one(text, rx_tag("price"))
    text, pictures = pop_all(text, rx_picture)
    text, vendor = pop_one(text, rx_tag("vendor"))
    text, currencyId = pop_one(text, rx_tag("currencyId"))
    text, description = pop_one(text, rx_tag("description"))
    text, params = pop_all(text, rx_param)

    pieces = []
    for part in [categoryId, vendorCode, name, price]:
        if part: pieces.append(part.strip())
    for pic in pictures:
        if pic: pieces.append(pic.strip())
    for part in [vendor, currencyId, description]:
        if part: pieces.append(part.strip())
    for prm in params:
        if prm: pieces.append(prm.strip())

    tail = text.strip()
    if tail:
        pieces.append(tail)

    return "\n".join(pieces) + ("\n" if pieces else "")

def _transform_offer(chunk: str) -> str:
    m = re.match(r"(?s)\s*<offer\b([^>]*)>(.*)</offer>\s*", chunk)
    if not m: return chunk
    attrs, body = m.group(1), m.group(2)
    attrs, body = _move_available_attr(attrs, body)
    body = _copy_purchase_into_price(body)
    body = _remove_simple_tags(body)
    body = _remove_param_by_name(body)
    body = _sort_offer_tags(body)
    # ensure exactly one newline after opening <offer ...>
    if not body.startswith("\n"):
        body = "\n" + body
    return f"<offer{attrs}>{body}</offer>"

def _strip_shop_header(src: str) -> str:
    m_shop = re.search(r"(?is)<\s*shop\b[^>]*>", src)
    m_offers = re.search(r"(?is)<\s*offers\b", src)
    if not m_shop or not m_offers or m_offers.start() <= m_shop.end():
        return src
    left = src[:m_shop.end()]
    right = src[m_offers.start():]
    if not (left.endswith("\n") or right.startswith("\n") or left.endswith("\r") or right.startswith("\r")):
        return left + "\n" + right
    return left + right

def main() -> int:
    try:
        r = requests.get(URL, timeout=90, auth=(LOGIN, PASSWORD))
    except Exception as e:
        print(f"[ERROR] download failed: {e}", file=sys.stderr); return 1
    if r.status_code != 200:
        print(f"[ERROR] HTTP {r.status_code}", file=sys.stderr); return 1

    src = _dec(r.content, getattr(r, "encoding", None))
    src = _strip_shop_header(src)

    m_off = re.search(r"(?s)<offers>(.*?)</offers>", src)
    if not m_off:
        _save(src); print("[WARN] <offers> не найден — файл сохранён без изменений"); return 0

    offers_block = m_off.group(1)
    offers = re.findall(r"(?s)<offer\b.*?</offer>", offers_block)
    total = len(offers)

    re_cat = re.compile(r"<categoryId>\s*(\d+)\s*</categoryId>", flags=re.I)
    kept = []
    for ch in offers:
        m = re_cat.search(ch)
        if m and m.group(1) in ALLOWED_CATS:
            kept.append(_transform_offer(ch))

    sep = "\n\n"
    prefix = "<offers>\n\n" if kept else "<offers>\n"
    new_block = prefix + sep.join(kept) + ("\n" if kept else "") + "</offers>"
    out = re.sub(r"(?s)<offers>.*?</offers>", lambda _: new_block, src, count=1)
    _save(out)
    print(f"[OK] offers kept: {len(kept)} / {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
