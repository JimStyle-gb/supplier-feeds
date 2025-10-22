# ===================== HOTFIX: ensure_keywords (и зависимости) =====================
# ВСТАВЬ ЭТОТ БЛОК ОДНИМ КУСОЧКОМ ВЫШЕ main(), если сборка ругается на "name 'ensure_keywords' is not defined"
try:
    ensure_keywords
except NameError:
    import re

    # --- настройки, берём из уже объявленных констант, иначе ставим дефолты ---
    SATU_KEYWORDS_MAXLEN = globals().get("SATU_KEYWORDS_MAXLEN", 1024)
    SATU_KEYWORDS_GEO = globals().get("SATU_KEYWORDS_GEO", True)
    SATU_KEYWORDS_GEO_MAX = globals().get("SATU_KEYWORDS_GEO_MAX", 20)
    SATU_KEYWORDS_GEO_LAT = globals().get("SATU_KEYWORDS_GEO_LAT", True)

    # --- вспомогательные ---
    WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9\-]{2,}")

    def tokenize(s: str):
        return WORD_RE.findall(s or "")

    def dedup(words):
        seen = set(); out = []
        for w in words:
            k = w.lower()
            if k and k not in seen:
                seen.add(k); out.append(w)
        return out

    def translit_ru_to_lat(s: str) -> str:
        table = str.maketrans({
            "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"e","ж":"zh","з":"z","и":"i","й":"y",
            "к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f",
            "х":"h","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ы":"y","э":"e","ю":"yu","я":"ya","ь":"","ъ":""
        })
        out = (s or "").lower().translate(table)
        out = re.sub(r"[^a-z0-9\- ]+","", out)
        return re.sub(r"\s+","-", out).strip("-")

    AS_INTERNAL_ART_RE = re.compile(r"^AS\d+|^AK\d+|^AC\d+", re.I)
    MODEL_RE = re.compile(r"\b([A-Z][A-Z0-9\-]{2,})\b", re.I)

    def extract_models(text_srcs):
        tokens = set()
        for src in text_srcs or []:
            if not src: continue
            for m in MODEL_RE.findall(src):
                t = m.upper()
                if AS_INTERNAL_ART_RE.match(t):
                    continue
                if not (re.search(r"[A-Z]", t) and re.search(r"\d", t)):
                    continue
                if len(t) < 5:
                    continue
                tokens.add(t)
        return list(tokens)

    def is_content_word(t: str) -> bool:
        x = (t or "").lower()
        STOP_RU = {"для","и","или","на","в","из","от","по","с","к","до","при","через","над","под","о","об","у","без","про",
                   "как","это","тип","модель","комплект","формат","новый","новинка","оригинальный"}
        STOP_EN = {"for","and","or","with","of","the","a","an","to","in","on","by","at","from",
                   "new","original","type","model","set","kit","pack"}
        GENERIC  = {"изделие","товар","продукция","аксессуар","устройство","оборудование"}
        return (x not in STOP_RU) and (x not in STOP_EN) and (x not in GENERIC) and (
            any(ch.isdigit() for ch in x) or "-" in x or len(x) >= 3
        )

    def build_keywords_for_offer(offer):
        # util-функции берём из твоего скрипта
        name   = get_text(offer, "name")
        vendor = get_text(offer, "vendor").strip()
        desc_h = inner_html(offer.find("description"))

        base = [vendor] if vendor else []

        raw_tokens = tokenize(name or "")
        modelish   = [t for t in raw_tokens if re.search(r"[A-Za-z].*\d|\d.*[A-Za-z]", t)]
        content    = [t for t in raw_tokens if is_content_word(t)]

        bigr = []
        for i in range(len(content)-1):
            a, b = content[i], content[i+1]
            if is_content_word(a) and is_content_word(b):
                bigr.append(f"{a} {b}")

        base += extract_models([name, desc_h])
        base += modelish[:8]
        base += bigr[:8]
        base += [t.capitalize() if not re.search(r"[A-Z]{2,}", t) else t for t in content[:10]]

        # простейшая палитра цветов
        colors = []
        low = (name or "").lower()
        mapping = {
            "жёлт":"желтый","желт":"желтый","yellow":"yellow",
            "черн":"черный","black":"black",
            "син":"синий","blue":"blue",
            "красн":"красный","red":"red",
            "зелен":"зеленый","green":"green",
            "серебр":"серебряный","silver":"silver",
            "циан":"cyan","магент":"magenta"
        }
        for k, val in mapping.items():
            if k in low and val not in colors:
                colors.append(val)
        base += colors

        # транслит для русских токенов
        extra = []
        for w in base:
            if re.search(r"[А-Яа-яЁё]", str(w)):
                tr = translit_ru_to_lat(str(w))
                if tr and tr not in extra:
                    extra.append(tr)
        base += extra

        # GEO-хвост (как было)
        if SATU_KEYWORDS_GEO:
            geo = ["Казахстан","Алматы","Астана","Шымкент","Караганда","Актобе","Павлодар","Атырау","Тараз",
                   "Оскемен","Семей","Костанаи","Кызылорда","Орал","Петропавл","Талдыкорган",
                   "Актау","Темиртау","Экибастуз","Кокшетау","Рудный"]
            if SATU_KEYWORDS_GEO_LAT:
                geo += ["Kazakhstan","Almaty","Astana","Shymkent","Karaganda","Aktobe","Pavlodar","Atyrau","Taraz",
                        "Oskemen","Semey","Kostanay","Kyzylorda","Oral","Petropavl","Taldykorgan",
                        "Aktau","Temirtau","Ekibastuz","Kokshetau","Rudny"]
            base += geo[:SATU_KEYWORDS_GEO_MAX]

        parts = dedup([p for p in base if p])
        res   = []
        total = 0
        for p in parts:
            add = ((", " if res else "") + p)
            if total + len(add) > SATU_KEYWORDS_MAXLEN:
                break
            res.append(p); total += len(add)
        return ", ".join(res)

    def ensure_keywords(out_shop):
        off_el = out_shop.find("offers")
        if off_el is None:
            return 0
        touched = 0
        for offer in off_el.findall("offer"):
            kw = build_keywords_for_offer(offer)
            node = offer.find("keywords")
            if not kw:
                if node is not None:
                    offer.remove(node)
                continue
            if node is None:
                node = ET.SubElement(offer, "keywords")
                node.text = kw
                touched += 1
            else:
                if (node.text or "") != kw:
                    node.text = kw
                    touched += 1
        return touched
# =================== /HOTFIX ===================
