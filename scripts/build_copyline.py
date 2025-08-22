# ===== СТРОГИЙ ФИЛЬТР: ключевые слова/фразы ТОЛЬКО В НАЧАЛЕ НАЗВАНИЯ =====
# одиночные слова: используем "стем" (без окончаний), но ТОЛЬКО для первого токена
ALLOW_SINGLE_STEMS = {
    "drum",
    "девелопер",
    "драм",
    "картридж",
    "термоблок",
    "термоэлемент",
}

# фразы в начале: тоже по стемам (без окончаний)
# покрываем: "кабель сетевой", "сетевой кабель", "тонер картридж/картриджи/картриджа..."
ALLOW_PHRASE_STEMS = [
    ["кабель", "сетев"],
    ["сетев", "кабель"],
    ["тонер", "картридж"],
]

# стоп-слова — если встречаются где угодно, исключаем (чтобы не тащить drum-чип и т.п.)
DISALLOW_TOKENS = {"chip", "чип", "reset", "ресет"}

TOKEN_RE = re.compile(r"[A-Za-zА-Яа-я0-9]+", re.IGNORECASE)

def _tokenize_ru(text: str) -> list[str]:
    s = norm(text).lower().replace("ё", "е")
    return TOKEN_RE.findall(s)

def name_matches_filter(name: str) -> bool:
    tokens = _tokenize_ru(name)
    if not tokens:
        return False

    # 1) отсечь чипы/ресеты сразу
    if any(t in DISALLOW_TOKENS for t in tokens):
        return False

    # 2) фразы в НАЧАЛЕ по стемам: tokens[0].startswith(stem0) и tokens[1].startswith(stem1), и т.д.
    for stems in ALLOW_PHRASE_STEMS:
        if len(tokens) >= len(stems) and all(tokens[i].startswith(stems[i]) for i in range(len(stems))):
            return True

    # 3) одиночное слово: первый токен должен начинаться с одного из стемов
    if any(tokens[0].startswith(st) for st in ALLOW_SINGLE_STEMS):
        return True

    return False
