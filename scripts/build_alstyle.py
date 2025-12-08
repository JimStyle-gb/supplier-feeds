# ВСТАВКА ДЛЯ build_alstyle.py
# Задача: авто-фиксы грамматики/опечаток + починка дат в FEED_META (без изменения структуры),
# и строгое сохранение форматирования (только то, что нужно).

def _ensure_footer_spacing(xml_text: str) -> str:
    """Финальная доводка: фикс дат + точечные правки текста + обязательные пустые строки AlStyle."""
    import re
    from datetime import datetime, timedelta

    def _parse_build_time(s: str):
        m = re.search(r'Время сборки \(Алматы\)\s*\|\s*([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})', s)
        if not m:
            return None
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def _next_daily(dt: datetime, hour: int):
        cand = dt.replace(hour=hour, minute=0, second=0, microsecond=0)
        if cand <= dt:
            cand += timedelta(days=1)
        return cand

    # 1) Даты/время: yml_catalog date и "Ближайшая сборка" должны быть логичными
    build_time = _parse_build_time(xml_text)
    if build_time:
        # yml_catalog date = время сборки (минуты)
        xml_text = re.sub(
            r'(<yml_catalog\s+date=")[^"]+(")',
            r'\g<1>' + build_time.strftime("%Y-%m-%d %H:%M") + r'\2',
            xml_text,
            count=1
        )

        # Ближайшая сборка = следующий запуск по расписанию AlStyle (ежедневно 01:00 Алматы)
        nxt = _next_daily(build_time, 1)
        xml_text = re.sub(
            r'(Ближайшая сборка \(Алматы\)\s*\|\s*)[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}',
            r'\1' + nxt.strftime("%Y-%m-%d %H:%M:%S"),
            xml_text,
            count=1
        )

    # 2) Точечные правки по офферам (не трогаем порядок тегов)
    rx_offer = re.compile(r"<offer\b[^>]*>.*?</offer>", re.S)
    rx_name = re.compile(r"<name>(.*?)</name>", re.S)
    rx_desc = re.compile(r"<description><!\[CDATA\[(.*?)\]\]></description>", re.S)

    desc_fixes = [
        (re.compile(r"(\b4)(?:\s|&nbsp;)*[–—\-‑](?:\s|&nbsp;)*(?:х|x)\b", re.I), "4-х"),
        (re.compile(r"\b100(?:\s|&nbsp;)+метров(ый|ая|ое|ые)\b", re.I), r"100-метров\1"),
        (re.compile(r"\bили1\b", re.I), "или 1"),
        (re.compile(r"\b1(?:\s|&nbsp;)*Гигабит/сек\b", re.I), "1 Гигабит/сек"),
        (re.compile(r"\b1Гигабит/сек\b", re.I), "1 Гигабит/сек"),
        (re.compile(r"\b2\*USB(?:\s|&nbsp;)+порта\b", re.I), "2 USB-порта"),
        (re.compile(r"\b2\*USB\b", re.I), "2 USB"),
        (re.compile(r"\b3(?:\s|&nbsp;)+выходных(?:\s|&nbsp;)+разъёмов\b", re.I), "3 выходных разъёма"),
    ]

    def _fix_cdata(cdata: str, name: str) -> str:
        s = cdata

        # Локальный фикс модели (PTS-3KLN-LCD vs PTS-3KL-LCD) — только если в name есть N
        if "PTS-3KLN-LCD" in name and "PTS-3KL-LCD" in s:
            s = s.replace("PTS-3KL-LCD", "PTS-3KLN-LCD")

        for rx, rep in desc_fixes:
            s = rx.sub(rep, s)

        # Грамматика начала: "{name} Это" -> "{name} — это"
        if name:
            s = re.sub(rf"(<p>\s*){re.escape(name)}\s+Это\b", rf"\1{name} — это", s, count=1, flags=re.I)
            # Если после name сразу слово с заглавной — ставим тире
            s = re.sub(rf"(<p>\s*){re.escape(name)}\s+(?=[A-ZА-ЯЁ])", rf"\1{name} — ", s, count=1)

        return s

    def _offer_repl(m: re.Match) -> str:
        block = m.group(0)

        # Латиница/опечатки в параметрах/тексте
        block = block.replace("Мощность (Bт)", "Мощность (Вт)")
        block = block.replace("Shuko", "Schuko")

        nm = rx_name.search(block)
        name = nm.group(1) if nm else ""

        dm = rx_desc.search(block)
        if dm:
            old = dm.group(1)
            new = _fix_cdata(old, name)
            if new != old:
                block = block[:dm.start(1)] + new + block[dm.end(1):]

        return block

    xml_text = rx_offer.sub(_offer_repl, xml_text)

    # 3) Строгое форматирование AlStyle
    xml_text = re.sub(r"(<offers>\n)(?!\n)", r"\1\n", xml_text, count=1)
    xml_text = re.sub(r"(</offer>\n)(</offers>)", r"\1\n\2", xml_text, count=1)

    return xml_text
