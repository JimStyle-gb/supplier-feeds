# scripts/build_price.py
import re
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

# --- настройки путей ---
ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
OUT_FILE = DOCS / "price.yml"

SUPPLIERS = {
    "alstyle":  DOCS / "alstyle.yml",
    "akcent":   DOCS / "akcent.yml",
    "copyline": DOCS / "copyline.yml",
    "nvprint":  DOCS / "nvprint.yml",
    "vtt":      DOCS / "vtt.yml",
}

# Имена для разбивки
HUMAN_NAMES = {
    "alstyle":  "AlStyle",
    "akcent":   "AkCent",
    "copyline": "CopyLine",
    "nvprint":  "NVPrint",
    "vtt":      "VTT",
}

# --- утилиты времени под Алматы ---
ALMATY_TZ = timezone(timedelta(hours=6))  # UTC+6
def now_almaty():
    return datetime.now(ALMATY_TZ)

def fmt_meta_dt(dt: datetime) -> str:
    # 07:10:2025 - 19:03:59
    return dt.strftime("%d:%m:%Y - %H:%M:%S")

def yml_catalog_dt(dt: datetime) -> str:
    # 2025-10-07 14:03
    return dt.strftime("%Y-%m-%d %H:%M")

# --- парсеры ---
OFFER_RX = re.compile(r"<offer\b.*?</offer>", re.DOTALL | re.IGNORECASE)
AVAIL_RX = re.compile(r"<available>\s*(true|false)\s*</available>", re.IGNORECASE)
FEED_META_RX = re.compile(r"<!--\s*FEED_META\s*(.*?)\s*-->", re.DOTALL | re.IGNORECASE)
VENDOR_CODE_RX = re.compile(r"<vendorCode>\s*([^<\s]+)\s*</vendorCode>", re.IGNORECASE)

def read_text(p: Path) -> str:
    return p.read_text(encoding="cp1251", errors="replace")

def extract_offers(xml: str):
    return OFFER_RX.findall(xml)

def extract_feed_meta_block(xml: str):
    """Вернёт текст внутри комментария FEED_META без обёртки <!--FEED_META ... -->.
    Если не найдёт — пустую строку.
    """
    m = FEED_META_RX.search(xml)
    return (m.group(1).strip() if m else "")

def count_availability(offers: list[str]):
    t = f = 0
    for off in offers:
        m = AVAIL_RX.search(off)
        if m:
            if m.group(1).lower() == "true":
                t += 1
            else:
                f += 1
    return t, f

def dedupe_by_vendor_code(offers: list[str]):
    """Оставляем первое вхождение по <vendorCode>."""
    seen = set()
    result = []
    for off in offers:
        vm = VENDOR_CODE_RX.search(off)
        key = vm.group(1).strip() if vm else None
        if not key:
            # если нет vendorCode — оставляем как есть, но ключ None чтобы не дублировать
            result.append(off)
            continue
        if key in seen:
            continue
        seen.add(key)
        result.append(off)
    return result, len(seen)

# --- сборка ---
def main():
    # читаем все источники
    sources = {}
    for key, path in SUPPLIERS.items():
        if not path.exists():
            # допускаем отсутствие какого-то файла — просто пропустим
            continue
        txt = read_text(path)
        offers = extract_offers(txt)
        meta  = extract_feed_meta_block(txt)  # только внутренности, без повторного "FEED_META"
        sources[key] = {
            "text": txt,
            "offers": offers,
            "meta": meta,
        }

    # объединяем офферы
    merged_offers = []
    per_source_counts = {k: 0 for k in SUPPLIERS.keys()}
    for key in SUPPLIERS.keys():
        if key not in sources:
            continue
        offs = sources[key]["offers"]
        merged_offers.extend(offs)
        per_source_counts[key] = len(offs)

    # дедуп по vendorCode (по умолчанию оставляем первый)
    merged_offers, unique_vendorcodes = dedupe_by_vendor_code(merged_offers)

    # подсчёты
    total_before = sum(per_source_counts.values())
    total_after = len(merged_offers)
    avail_true, avail_false = count_availability(merged_offers)

    # разбивка строка
    breakdown_parts = []
    for key in SUPPLIERS.keys():
        if key in sources:
            breakdown_parts.append(f"{HUMAN_NAMES[key]}:{per_source_counts[key]}")
    breakdown = ", ".join(breakdown_parts)

    # время
    now = now_almaty()
    yml_dt = yml_catalog_dt(now)
    now_str = fmt_meta_dt(now)

    # собираем общий FEED_META (без лишних пробелов слева)
    merged_meta_block = (
        "<!--FEED_META\n"
        f"Поставщик                                  | Price\n"
        f"Время сборки (Алматы)                      | {now_str}\n"
        f"Сколько товаров у поставщика до фильтра    | {total_before}\n"
        f"Сколько товаров у поставщика после фильтра | {total_after}\n"
        f"Сколько товаров есть в наличии (true)      | {avail_true}\n"
        f"Сколько товаров нет в наличии (false)      | {avail_false}\n"
        f"Дубликатов по vendorCode отброшено         | 0\n"
        f"Разбивка по источникам                     | {breakdown}\n"
        f"-->\n"
    )

    # собираем конкатенацию FEED_META поставщиков (ровно как есть, без второго 'FEED_META')
    supplier_metas = []
    for key in SUPPLIERS.keys():
        if key not in sources:
            continue
        inner = sources[key]["meta"]
        if inner:
            supplier_metas.append("<!--FEED_META\n" + inner + "\n-->")
    supplier_meta_block = ("\n\n".join(supplier_metas) + "\n") if supplier_metas else ""

    # аккуратно склеиваем офферы с пустой строкой-разделителем между ними
    offers_body = ("\n\n".join(merged_offers)).rstrip() + ("\n" if merged_offers else "")

    # финальный документ
    header = "<?xml version='1.0' encoding='windows-1251'?>\n"
    yml_open = f"<yml_catalog date=\"{yml_dt}\">\n"
    shop_open = "  <shop>\n    <offers>\n"
    shop_close = "    </offers>\n  </shop>"
    yml_close = "</yml_catalog>"

    # Важный момент: между </offer> и следующим <offer> будет пустая строка (см. join выше)
    out = []
    out.append(header)
    out.append(yml_open)
    out.append(merged_meta_block)
    if supplier_meta_block:
        out.append(supplier_meta_block)
    out.append(shop_open)
    # смещаем каждую строку офферов на два пробела для ровного вида
    indented_offers = "\n".join(("      " + line if line else "")
                                for line in offers_body.splitlines())
    # так, чтобы начало каждого <offer ...> было с 6 пробелами, а между офферами — реальный пустой абзац
    indented_offers = indented_offers.replace("\n      \n", "\n\n")
    out.append(indented_offers + ("\n" if indented_offers and not indented_offers.endswith("\n") else ""))
    out.append(shop_close + "\n")
    out.append(yml_close)

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text("".join(out), encoding="cp1251", errors="replace")
    print(f"OK: written {OUT_FILE.relative_to(ROOT)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
