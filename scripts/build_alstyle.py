# -*- coding: utf-8 -*-
"""
build_alstyle.py — v11 (фикс <shop><offers> слитно)
Изменил ТОЛЬКО _strip_shop_header: после вырезания содержимого между <shop> и <offers>
вставляю РОВНО один перевод строки между тегами (если его не было).
Остальное — как в v10.
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
        def repl(m):
            return "\n" if (m.group('pre_nl') or m.group('post_nl')) else ""
        return rx.sub(repl, text)
    body = rm(body, r"quantity_in_stock")
    body = rm(body, r"purchase_?price")
    body = rm(body, r"available")
    body = rm(body, r"url")
    body = rm(body, r"quantity")
    return body

def _transform_offer(chunk: str) -> str:
    m = re.match(r"(?s)\s*<offer\b([^>]*)>(.*)</offer>\s*", chunk)
    if not m: return chunk
    attrs, body = m.group(1), m.group(2)
    attrs, body = _move_available_attr(attrs, body)
    body = _copy_purchase_into_price(body)
    body = _remove_simple_tags(body)
    return f"<offer{attrs}>{body}</offer>"

def _strip_shop_header(src: str) -> str:
    """Удаляем всё между <shop> и <offers>, оставляя ОДИН перевод строки между ними (если его не было)."""
    m_shop = re.search(r"(?is)<\s*shop\b[^>]*>", src)
    m_offers = re.search(r"(?is)<\s*offers\b", src)
    if not m_shop or not m_offers or m_offers.start() <= m_shop.end():
        return src
    left = src[:m_shop.end()]
    right = src[m_offers.start():]
    # если уже есть перенос на стыке — ничего не добавляем; иначе добавляем один \n
    if not (left.endswith("\n") or right.startswith("\n") or left.endswith("\r") or right.startswith("\r")):
        return left + "\n" + right
    return left + right

def main() -> int:
    print('build_alstyle v43-min (Intro+Особенности+Характеристики)')
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

    new_block = "<offers>\n" + "\n".join(kept) + ("\n" if kept else "") + "</offers>"
    out = re.sub(r"(?s)<offers>.*?</offers>", lambda _: new_block, src, count=1)
    _save(out)
    print(f"[OK] offers kept: {len(kept)} / {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())


def _flatten_description(body: str) -> str:
    # Мини: Вступление + Особенности + Характеристики
    import re, html
    def _find_desc(b): return re.search(r"(?is)(<\s*description\b[^>]*>)(.*?)(</\s*description\s*>)", b)
    def _tag(name,b): m=re.search(rf"(?is)<\s*{name}\s*>(.*?)</\s*{name}\s*>", b); return m.group(1).strip() if m else ""
    def _clean(txt):
        if txt.lstrip().startswith("<![CDATA[]"): pass
        if txt.lstrip().startswith("<![CDATA["):
            txt=re.sub(r"(?is)^\s*<!\[CDATA\[(.*)\]\]>\s*$", r"\1", txt.strip())
        for _ in range(3):
            nt=html.unescape(txt); 
            if nt==txt: break; 
            txt=nt
        txt=txt.replace("\u00A0"," "); txt=re.sub(r"[\u200B-\u200D\uFEFF]","",txt)
        txt=re.sub(r"\r\n|\r|\n"," ",txt); txt=re.sub(r"(?is)<[^>]+>"," ",txt); txt=re.sub(r"\s+"," ",txt).strip()
        return txt
    def _params(b):
        deny={"артикул","благотворительность","код тн вэд","код товара kaspi","новинка","снижена цена","штрихкод","штрих-код","назначение","объем","объём"}
        out=[]
        for k,v in re.findall(r'(?is)<\s*param\b[^>]*\bname\s*=\s*"([^"]+)"[^>]*>(.*?)</\s*param\s*>', b):
            kk=_clean(k).strip(": ").lower()
            if kk in deny: continue
            vv=_clean(v); 
            if not vv: continue
            key=k.strip().strip(": "); key=key[:1].upper()+key[1:]
            out.append((key,vv))
        return out
    def _feats(plain):
        kw=r"(особенн|функц|режим|подсвет|защит|фильтр|индикатор|автомат|эргоном|тихий|компакт|удобн)"
        parts=re.split(r"(?<=[\.\!\?])\s+|;\s+|,\s+(?=[А-ЯA-Z])", plain)
        return [p for p in parts if re.search(kw,(p or "").lower()) and 6<=len(p)<=140][:8]

    m=_find_desc(body)
    if not m: return body
    head,raw,tail=m.group(1),m.group(2),m.group(3)
    vendor=_tag("vendor",body); name=_tag("name",body); title=(vendor+" "+name).strip() if vendor else name
    plain=_clean(raw); intro=" ".join([p for p in re.split(r"(?<=[\.\!\?])\s+",plain)[:2] if p]).strip() or plain
    feats=_feats(plain); params=_params(body)
    html_parts=[]
    if title or intro:
        t=(f"<strong>{html.escape(title)}</strong>. " if title else "")+html.escape(intro)
        html_parts.append(f"<p>{t}</p>")
    if feats:
        html_parts.append("<h3>Особенности</h3>"); html_parts.append("<ul>"+ "".join(f"<li>{html.escape(x)}</li>" for x in feats) + "</ul>")
    if params:
        html_parts.append("<h3>Характеристики</h3>"); html_parts.append("<ul>"+ "".join(f"<li><strong>{html.escape(k)}:</strong> {html.escape(v)}</li>" for k,v in params) + "</ul>")
    html_desc="".join(html_parts) if html_parts else html.escape(plain or "Описание недоступно")
    return re.sub(r"(?is)<\s*description\b[^>]*>.*?</\s*description\s*>", head+html_desc+tail, body, count=1)
