# -*- coding: utf-8 -*-
"""
build_alstyle.py — v3 (фикс re.sub "invalid group reference")
• Скачиваем фид AlStyle по Basic Auth и сохраняем docs/alstyle.yml (cp1251).
• Фильтруем офферы по <categoryId> из списка ALLOWED_CATS (ID поставщика).
• Замена блока <offers> сделана через lambda, чтобы не интерпретировать \1, \g<...> и т.п. в тексте.
"""

import re
import sys
import pathlib
import requests

# --- Настройки ----------------------------------------------------------------
URL = "https://al-style.kz/upload/catalog_export/al_style_catalog.php"
LOGIN = "info@complex-solutions.kz"    # по просьбе пользователя
PASSWORD = "Aa123456"                  # по просьбе пользователя
OUT_PATH = pathlib.Path("docs/alstyle.yml")
ENC_OUT = "windows-1251"

# Допустимые категории (ID поставщика), перечисленные через запятую
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

# --- Основная логика ----------------------------------------------------------
def main() -> int:
    # 1) Скачиваем источник
    try:
        r = requests.get(URL, timeout=90, auth=(LOGIN, PASSWORD))
    except Exception as e:
        print(f"[ERROR] download failed: {e}", file=sys.stderr)
        return 1
    if r.status_code != 200:
        print(f"[ERROR] HTTP {r.status_code}", file=sys.stderr)
        return 1

    src = _decode_best(r.content, getattr(r, "encoding", None))

    # 2) Вырезаем <offers>...</offers>
    m_offers = re.search(r"(?s)<offers>(.*?)</offers>", src)
    if not m_offers:
        _save_text(src)
        print("[WARN] <offers> контейнер не найден — файл сохранён без изменений")
        return 0

    offers_block = m_offers.group(1)

    # 3) Список офферов
    offers = re.findall(r"(?s)<offer\b.*?</offer>", offers_block)
    total = len(offers)
    re_cat = re.compile(r"<categoryId>\s*(\d+)\s*</categoryId>")
    kept = []
    for chunk in offers:
        m = re_cat.search(chunk)
        cat = m.group(1) if m else ""
        if cat in ALLOWED_CATS:
            kept.append(chunk)

    # 4) Сборка нового блока
    if kept:
        new_block = "<offers>\n" + "\n".join(kept) + "\n</offers>"
    else:
        new_block = "<offers></offers>"

    # 5) Безопасная замена (через lambda, чтобы не было backreference-ловушки)
    out_text = re.sub(r"(?s)<offers>.*?</offers>", lambda _: new_block, src, count=1)

    # 6) Сохраняем
    _save_text(out_text)
    print(f"[OK] offers kept: {len(kept)} / {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
