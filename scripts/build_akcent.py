# ==== ДОБАВЬ РЯДОМ С ДРУГИМИ ХЕЛПЕРАМИ (глобально) ====
import re

TECH_KEYWORDS = [
    "тип","модель","серия","совместим","ресурс","цвет печати","технология печати","скорость печати",
    "разрешение","формат бумаги","лоток","интерфейс","порт","usb","ethernet","wi-fi","bluetooth","lan",
    "диагонал","яркост","контраст","время отклик","тип матрицы","частота обновлен","угол обзора","hdr",
    "разъем","разъём","длина кабеля","материал","емкость","объем","форм-фактор","тип памяти",
    "скорость чтения","скорость записи","мощност","напряжен","частот","сила тока","энергопотреблен",
    "вес","габарит","размер","страна","гаранти","комплектац","dpi","дюйм","мм","см","кг","вт","в","гц","лм"
]
TECH_KEYWORDS_RE = re.compile("|".join([re.escape(k) for k in TECH_KEYWORDS]), re.I)
UNITS_RE = re.compile(r"\b(\d+[.,]?\d*\s?(мм|см|м|кг|г|Вт|В|Гц|мАч|Ач|dpi|лм|ГБ|МБ|TB|Hz|V|W|A|VA|dB|°C|\"|дюйм))\b", re.I)
BRAND_WORDS = {"canon","hp","hewlett-packard","xerox","brother","epson","benq","viewsonic","optoma","acer",
               "panasonic","sony","konica minolta","ricoh","kyocera","sharp","oki","pantum","lenovo","dell","asus","samsung","apple","msi"}
STOP_KEYS = {"для","и","или","на","в","из","от","по","с","к","до","при","над","под","о","об","у","без","про","как"}

def _norm_kv_key(s: str) -> str:
    s = (s or "").strip().lower().replace("ё","е")
    return re.sub(r"\s+"," ", s)

def _likely_tech_key(k: str) -> bool:
    nk = _norm_kv_key(k)
    if not (2 <= len(nk) <= 40): return False
    if re.fullmatch(r"[\d\W]+", nk): return False               # только цифры/знаки — отбрасываем
    if nk in BRAND_WORDS: return False                           # одиночное «epson», «hp» и т.п. — не ключ
    if nk in STOP_KEYS and not TECH_KEYWORDS_RE.search(nk): return False
    return bool(TECH_KEYWORDS_RE.search(nk))

def _extract_pairs_from_native(native_text: str):
    """
    Возвращает список (key, value) из «родного» блока:
    - «Ключ: Значение»
    - «Ключ  Значение» (без «:», но с 2+ пробелами) ИЛИ «Ключ Значение», если value содержит единицы измерения.
    """
    out = []
    if not native_text:
        return out
    lines = [ln.strip() for ln in native_text.splitlines() if ln.strip()]
    for ln in lines:
        if len(ln) < 4 or len(ln) > 160:
            continue
        m = re.match(r"^\s*([A-Za-zА-Яа-яЁё0-9/().,%\"'°+\-\s]{2,50}?)[\s]*[:\-–—]\s+(.+)$", ln)
        if m:
            k, v = m.group(1).strip(), m.group(2).strip()
            if _likely_tech_key(k):
                out.append((k, v.strip().strip(".;")))
            continue
        m2 = re.match(r"^\s*([A-Za-zА-Яа-яЁё/().,%\"'°+\-\s]{2,50}?)\s{2,}(.+)$", ln)  # 2+ пробела
        if m2:
            k, v = m2.group(1).strip(), m2.group(2).strip()
            if _likely_tech_key(k):
                out.append((k, v.strip().strip(".;")))
            continue
        m3 = re.match(r"^\s*([A-Za-zА-Яа-яжЁё/().,%\"'°+\-\s]{2,50}?)\s(.+)$", ln)     # 1 пробел, но проверяем единицы
        if m3:
            k, v = m3.group(1).strip(), m3.group(2).strip()
            if UNITS_RE.search(v) and _likely_tech_key(k):
                out.append((k, v.strip().strip(".;")))
    return out
