# -*- coding: utf-8 -*-
"""
build_alstyle.py — v4
Изменения относительно v3:
1) Переносим <available>true/false</available> в атрибут <offer ... available="true/false"> и удаляем тег <available> из тела оффера.
2) Меняем теги цен местами внутри каждого <offer>:
   - <price> → <purchase_price>
   - <purchase_price> → <price>
Остальное:
• Скачиваем фид по Basic Auth, фильтруем офферы по <categoryId> из ALLOWED_CATS (ID — поставщика),
• сохраняем как docs/alstyle.yml в windows-1251.
"""

import re
import sys
import pathlib
import requests

# --- Настройки ----------------------------------------------------------------
URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
LOGIN = "info@complex-solutions.kz"
PASSWORD = "Aa123456"
OUT_PATH = pathlib.Path("docs/alstyle.yml")
ENC_OUT = "windows-1251"

CAT_ALLOW_STR = "3540,3541,3542,3543,3544,3545,3566,3567,3569,3570,3580,3688,3708,3721,3722,4889,4890,4895,5017,5075,5649,5710,5711,5712,5713,21279,21281,21291,21356,21367,21368,21369,21370,21371,21372,21451,21498,21500,21501,21572,21573,21574,21575,21576,21578,21580,21581,21583,21584,21585,21586,21588,21591,21640,21664,21665,21666,21698"
ALLOWED_CATS = {s.strip() for s in CAT_ALLOW_STR.split(",") if s.strip()}

# --- Утилиты ------------------------------------------------------------------
def _decode_best(data, enc_guess):
    for enc in [enc_guess, "utf-8", "windows-1251", "cp1251", "latin-1"]:
        if not enc:
            continue
        try:
            return data.decode(enc)
        except Exception:
            pass
    return data.decode("utf-8", errors="replace")

def _encode_cp1251(text):
    return text.encode(ENC_OUT, errors="replace")

def _save_text(text: str) -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_bytes(_encode_cp1251(text))

def _norm_bool(val: str) -> str:
    """Нормализуем значение available к 'true'/'false' (на всякий случай)."""
    s = (val or "").strip().strip('"').strip("'").lower()
    if s in {"true", "1", "yes", "y", "да"}:
        return "true"
    if s in {"false", "0", "no", "n", "нет"}:
        return "false"
    # если неизвестно — оставим как есть (но в нижнем регистре)
    return s or "true"

def _swap_price_tags(text: str) -> str:
    """Меняем <price> ↔ <purchase_price> без коллизий через временный плейсхолдер."""
    # 1) purchase_price -> __PP__
    text = re.sub(r"(?is)<\s*purchase_price\s*>", "<__PP__>", text)
    text = re.sub(r"(?is)<\s*/\s*purchase_price\s*>", "</__PP__>", text)
    # 2) price -> purchase_price
    text = re.sub(r"(?is)<\s*price\s*>", "<purchase_price>", text)
    text = re.sub(r"(?is)<\s*/\s*price\s*>", "</purchase_price>", text)
    # 3) __PP__ -> price
    text = re.sub(r"(?is)<\s*__PP__\s*>", "<price>", text)
    text = re.sub(r"(?is)<\s*/\s*__PP__\s*>", "</price>", text)
    return text

def _transform_offer(chunk: str) -> str:
    """Перенос available в атрибут и своп ценовых тегов внутри одного <offer>…</offer>."""
    # Захватываем открывающий тег и тело
    m = re.match(r"(?s)^\s*<offer\b([^>]*)>(.*)</offer>\s*$", chunk)
    if not m:
        # если по какой-то причине не распарсили — просто вернём как есть
        return chunk

    attrs = m.group(1)
    body  = m.group(2)

    # 1) Достаём <available>…</available> из тела
    m_av = re.search(r"(?is)<\s*available\s*>\s*(.*?)\s*<\s*/\s*available\s*>", body)
    av_val = None
    if m_av:
        av_val = _norm_bool(m_av.group(1))
        # удаляем все теги <available> из тела
        body = re.sub(r"(?is)<\s*available\s*>.*?<\s*/\s*available\s*>", "", body)

    # 2) Свопим теги цен
    body = _swap_price_tags(body)

    # 3) Переносим available в атрибут открывающего тега
    if av_val is not None:
        if re.search(r'\savailable\s*=\s*"(?:[^"]*)"', attrs, flags=re.I):
            attrs = re.sub(r'\savailable\s*=\s*"(?:[^"]*)"', f' available="{av_val}"', attrs, flags=re.I)
        else:
            # добавляем пробел перед доступностью, чтобы сохранить формат <offer + attrs>
            attrs = (attrs.rstrip() + f' available="{av_val}"')

    # 4) Сборка обратно
    return f"<offer{attrs}>{body}</offer>"

# --- Основная логика ----------------------------------------------------------
def main() -> int:
    # Скачиваем
    try:
        r = requests.get(URL, timeout=90, auth=(LOGIN, PASSWORD))
    except Exception as e:
        print(f"[ERROR] download failed: {e}", file=sys.stderr)
        return 1
    if r.status_code != 200:
        print(f"[ERROR] HTTP {r.status_code}", file=sys.stderr)
        return 1

    src = _decode_best(r.content, getattr(r, "encoding", None))

    # Вырезаем блок <offers>
    m_offers = re.search(r"(?s)<offers>(.*?)</offers>", src)
    if not m_offers:
        _save_text(src)
        print("[WARN] <offers> контейнер не найден — файл сохранён без изменений")
        return 0

    offers_block = m_offers.group(1)

    # Разбиваем на офферы
    offers = re.findall(r"(?s)<offer\b.*?</offer>", offers_block)
    total = len(offers)
    re_cat = re.compile(r"<categoryId>\s*(\d+)\s*</categoryId>", flags=re.I)
    kept = []

    for chunk in offers:
        # фильтр по categoryId
        m = re_cat.search(chunk)
        cat = m.group(1) if m else ""
        if cat in ALLOWED_CATS:
            # трансформации внутри оффера
            kept.append(_transform_offer(chunk))

    # Собираем новый блок <offers>
    if kept:
        new_block = "<offers>\n" + "\n".join(kept) + "\n</offers>"
    else:
        new_block = "<offers></offers>"

    # Безопасная замена всего блока
    out_text = re.sub(r"(?s)<offers>.*?</offers>", lambda _: new_block, src, count=1)

    # Сохраняем
    _save_text(out_text)
    print(f"[OK] offers kept: {len(kept)} / {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
