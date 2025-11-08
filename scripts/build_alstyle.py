# -*- coding: utf-8 -*-
"""
build_alstyle.py — v6
Изменения: добавлен обмен тегов <price> ↔ <purchase_price> внутри каждого <offer>.
Сохранил: фильтр по <categoryId> (ваш список), перенос available в атрибут (тег не удаляю).
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

def _swap_price_tags(body: str) -> str:
    # Меняем теги без коллизий через плейсхолдер
    body = re.sub(r"(?is)<\s*purchase_price\s*>", "<__PP__>", body)
    body = re.sub(r"(?is)<\s*/\s*purchase_price\s*>", "</__PP__>", body)
    body = re.sub(r"(?is)<\s*price\s*>", "<purchase_price>", body)
    body = re.sub(r"(?is)<\s*/\s*price\s*>", "</purchase_price>", body)
    body = re.sub(r"(?is)<\s*__PP__\s*>", "<price>", body)
    body = re.sub(r"(?is)<\s*/\s*__PP__\s*>", "</price>", body)
    return body

def _move_available_and_swap(chunk: str) -> str:
    m = re.match(r"(?s)\s*<offer\b([^>]*)>(.*)</offer>\s*", chunk)
    if not m: return chunk
    attrs, body = m.group(1), m.group(2)

    # swap price tags в теле
    body = _swap_price_tags(body)

    # перенос available в атрибут (тег оставляем)
    m_av = re.search(r"(?is)<\s*available\s*>\s*(.*?)\s*<\s*/\s*available\s*>", body)
    if m_av:
        val = m_av.group(1)
        if re.search(r'\savailable\s*=', attrs, flags=re.I):
            attrs = re.sub(r'(\savailable\s*=\s*")([^"]*)(")', lambda g: g.group(1)+val+g.group(3), attrs, flags=re.I)
        else:
            attrs = attrs.rstrip() + f' available="{val}"'

    return f"<offer{attrs}>{body}</offer>"

def main() -> int:
    try:
        r = requests.get(URL, timeout=90, auth=(LOGIN, PASSWORD))
    except Exception as e:
        print(f"[ERROR] download failed: {e}", file=sys.stderr); return 1
    if r.status_code != 200:
        print(f"[ERROR] HTTP {r.status_code}", file=sys.stderr); return 1

    src = _dec(r.content, getattr(r, "encoding", None))

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
            kept.append(_move_available_and_swap(ch))

    new_block = "<offers>\n" + "\n".join(kept) + ("\n" if kept else "") + "</offers>"
    out = re.sub(r"(?s)<offers>.*?</offers>", lambda _: new_block, src, count=1)
    _save(out)
    print(f"[OK] offers kept: {len(kept)} / {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
