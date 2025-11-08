# -*- coding: utf-8 -*-
"""
build_alstyle.py — v8
Изменения:
• Удаляем всё содержимое между <shop> и <offers> (оставляем <shop><offers>...).
• Удаляем теги: <url>, <quantity>, <quantity_in_stock>, <available>, <purchase_price> (также purchasePrice/purchaseprice).
Сохраняем:
• Фильтр по <categoryId> (ID поставщика из списка).
• Перенос значения <available>…</available> в атрибут offer available="…".
• Копирование значения из <purchase_price> в первый <price> (потом удаляем <purchase_price>-теги).
Выход: docs/alstyle.yml в windows-1251.
"""

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
    # Переносим в атрибут (поддержим ' и ")
    if re.search(r'\savailable\s*=\s*"(?:[^"]*)"', attrs, flags=re.I):
        attrs = re.sub(r'(\savailable\s*=\s*")([^"]*)(")', lambda g: g.group(1)+val+g.group(3), attrs, flags=re.I)
    elif re.search(r"\savailable\s*=\s*'(?:[^']*)'", attrs, flags=re.I):
        attrs = re.sub(r"(\savailable\s*=\s*')([^']*)(')", lambda g: g.group(1)+val+g.group(3), attrs, flags=re.I)
    else:
        attrs = attrs.rstrip() + f' available="{val}"'
    return attrs, body

def _copy_purchase_into_price(body: str) -> str:
    # Найти purchase_price / purchasePrice / purchaseprice
    m_pp = re.search(r"(?is)<\s*purchase_?price\s*>\s*(.*?)\s*<\s*/\s*purchase_?price\s*>", body)
    if not m_pp: return body
    val = m_pp.group(1)
    # Подменить содержимое первого <price>…</price>
    def _repl(m): return m.group(1) + val + m.group(3)
    return re.sub(r"(?is)(<\s*price\s*>)(.*?)(<\s*/\s*price\s*>)", _repl, body, count=1)

def _remove_simple_tags(body: str) -> str:
    """Удаляем теги по списку в пределах одного оффера."""
    def rm(text, name_regex):
        # удаляем <tag ...>...</tag> (без самозакрывающихся)
        pattern = rf"(?is)<\s*(?:{name_regex})\b[^>]*>.*?<\s*/\s*(?:{name_regex})\s*>"
        return re.sub(pattern, "", text)
    # порядок важен: сначала более длинные
    body = rm(body, r"quantity_in_stock")
    body = rm(body, r"purchase_?price")   # обе формы purchase_price / purchaseprice
    body = rm(body, r"available")
    body = rm(body, r"url")
    body = rm(body, r"quantity")
    return body

def _transform_offer(chunk: str) -> str:
    m = re.match(r"(?s)\s*<offer\b([^>]*)>(.*)</offer>\s*", chunk)
    if not m: return chunk
    attrs, body = m.group(1), m.group(2)

    # 1) перенос available в атрибут
    attrs, body = _move_available_attr(attrs, body)
    # 2) копирование purchase_price -> price (если есть)
    body = _copy_purchase_into_price(body)
    # 3) удаление тегов: url, quantity, quantity_in_stock, available, purchase_price
    body = _remove_simple_tags(body)

    return f"<offer{attrs}>{body}</offer>"

def _strip_shop_header(src: str) -> str:
    """Удаляем всё между закрывающим > тега <shop...> и началом <offers>."""
    m_shop = re.search(r"(?is)<\s*shop\b[^>]*>", src)
    m_offers = re.search(r"(?is)<\s*offers\b", src)
    if not m_shop or not m_offers: 
        return src
    if m_offers.start() <= m_shop.end():
        return src
    return src[:m_shop.end()] + src[m_offers.start():]

def main() -> int:
    try:
        r = requests.get(URL, timeout=90, auth=(LOGIN, PASSWORD))
    except Exception as e:
        print(f"[ERROR] download failed: {e}", file=sys.stderr); return 1
    if r.status_code != 200:
        print(f"[ERROR] HTTP {r.status_code}", file=sys.stderr); return 1

    src = _dec(r.content, getattr(r, "encoding", None))

    # Сначала убираем шапку между <shop> и <offers>
    src = _strip_shop_header(src)

    # Достаём блок <offers>
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

    new_block = "<offers>\n" + "\n".join(kept) + ("\n" if kept else "") + "</offers>"
    out = re.sub(r"(?s)<offers>.*?</offers>", lambda _: new_block, src, count=1)
    _save(out)
    print(f"[OK] offers kept: {len(kept)} / {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
