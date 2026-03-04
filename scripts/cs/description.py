# -*- coding: utf-8 -*-
"""
CS Description — общий сборщик HTML для <description>.

Этап 2: вынос из cs/core.py в отдельный модуль, без изменения логики.
Важно: модуль НЕ импортирует cs/core.py (чтобы не ловить циклические импорты).
"""

from __future__ import annotations

import re
from .keywords import fix_mixed_cyr_lat  # без циклических импортов


# Константы (вынесены из core без изменения)
CS_HR_2PX = "<hr style=\"border:none; border-top:2px solid #E7D6B7; margin:12px 0;\" />"

CS_PAY_BLOCK = (
    "<!-- Оплата и доставка -->\n"
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<div style=\"background:#FFF6E5; border:1px solid #F1E2C6; padding:12px 14px; border-radius:0; text-align:left;\">"
    "<h3 style=\"margin:0 0 8px; font-size:17px;\">Оплата</h3>"
    "<ul style=\"margin:0; padding-left:18px;\">"
    "<li><strong>Безналичный</strong> расчёт для <u>юридических лиц</u></li>"
    "<li><strong>Удалённая оплата</strong> по <span style=\"color:#8b0000;\"><strong>KASPI</strong></span> счёту для <u>физических лиц</u></li>"
    "</ul>"
    "<hr style=\"border:none; border-top:1px solid #E7D6B7; margin:12px 0;\" />"
    "<h3 style=\"margin:0 0 8px; font-size:17px;\">Доставка по Алматы и Казахстану</h3>"
    "<ul style=\"margin:0; padding-left:18px;\">"
    "<li><em><strong>ДОСТАВКА</strong> в «квадрате» г. Алматы — БЕСПЛАТНО!</em></li>"
    "<li><em><strong>ДОСТАВКА</strong> по Казахстану до 5 кг — 5 000 тг. | 3–7 рабочих дней</em></li>"
    "<li><em><strong>ОТПРАВИМ</strong> товар любой курьерской компанией!</em></li>"
    "<li><em><strong>ОТПРАВИМ</strong> товар автобусом через автовокзал «САЙРАН»</em></li>"
    "</ul>"
    "</div></div>"
)

CS_WA_BLOCK = (
    "<!-- WhatsApp -->\n"
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; "
    "border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);\">"
    "&#128172; Написать в WhatsApp</a></p></div>"
)

CS_WA_DIV = (
    "<div style=\"font-family: Cambria, 'Times New Roman', serif; line-height:1.5; color:#222; font-size:15px;\">"
    "<p style=\"text-align:center; margin:0 0 12px;\">"
    "<a href=\"https://api.whatsapp.com/send/?phone=77073270501&amp;text&amp;type=phone_number&amp;app_absent=0\" "
    "style=\"display:inline-block; background:#27ae60; color:#ffffff; text-decoration:none; padding:11px 18px; "
    "border-radius:12px; font-weight:700; box-shadow:0 2px 0 rgba(0,0,0,0.08);\">"
    "&#128172; Написать в WhatsApp</a></p></div>"
)


# Утилиты (скопировано из core, без изменения)
def norm_ws(s: str) -> str:
    s2 = (s or "").replace("\u00a0", " ").strip()
    s2 = re.sub(r"\s+", " ", s2)
    s2 = fix_mixed_cyr_lat(s2)
    return s2.strip()

def xml_escape_text(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# XML escape для атрибутов

def _truncate_text(s: str, max_len: int, *, suffix: str = "") -> str:
    # CS: безопасно режем строку по границе слова/запятой
    s = norm_ws(s)
    if max_len <= 0:
        return ""
    if len(s) <= max_len:
        return s

    cut_len = max_len - len(suffix)
    if cut_len <= 0:
        return suffix[:max_len]

    chunk = s[:cut_len].rstrip()
    # режем по последней "хорошей" границе
    for sep in (",", " ", "/", ";"):
        j = chunk.rfind(sep)
        if j >= max(0, cut_len - 40):  # не уходим слишком далеко назад
            chunk = chunk[:j].rstrip(" ,/;")
            break

    chunk = chunk.rstrip(" ,/;")
    if suffix:
        return (chunk + suffix)[:max_len]
    return chunk


# Сборщики description/характеристик (вынесено из core, без изменения)
def build_chars_block(params_sorted: Sequence[tuple[str, str]]) -> str:
    items: list[str] = []
    for k, v in params_sorted or []:
        kk = xml_escape_text(norm_ws(k))
        vv = xml_escape_text(norm_ws(v))
        if not kk or not vv:
            continue
        items.append(f"<li><strong>{kk}:</strong> {vv}</li>")
    if not items:
        # CS: характеристики отсутствуют — выводим заглушку (единообразие + SEO)
        return "<h3>Характеристики</h3><p>Характеристики уточняются.</p>"
    return "<h3>Характеристики</h3><ul>" + "".join(items) + "</ul>"

def build_description(
    name: str,
    native_desc: str,
    params_sorted: Sequence[tuple[str, str]],
    *,
    notes: Sequence[str] | None = None,
    wa_block: str = CS_WA_DIV,
    hr_2px: str = CS_HR_2PX,
    pay_block: str = CS_PAY_BLOCK,
) -> str:
    n = norm_ws(name)
    n_esc = xml_escape_text(n)

    # Тело родного описания (без <h3>)
    desc_body = _build_desc_part(n, native_desc)

    # Если родного описания нет — берём короткий summary из параметров,
    # иначе (если и параметров нет) — короткий нейтральный фолбэк.
    if not desc_body:
        sm = _build_param_summary(params_sorted)
        if sm:
            desc_body = f"<p>{xml_escape_text(sm)}</p>"
        else:
            desc_body = "<p>Подробности уточняйте в WhatsApp.</p>"

    # Характеристики (если пусто — блок не выводим)
    chars = build_chars_block(params_sorted)

    # WA: страховка, если кто-то передал старый CS_WA_BLOCK с комментарием
    w = (wa_block or "").lstrip()
    if w.startswith("<!--"):
        w = re.sub(r"^<!--.*?-->\s*\n?", "", w, flags=re.S).strip()
    if not w:
        w = CS_WA_DIV

    parts: list[str] = []
    parts.append("<!-- Наименование товара -->")
    parts.append(f"<h3>{n_esc}</h3>")

    parts.append("<!-- WhatsApp -->")
    parts.append(hr_2px)
    parts.append(w)
    parts.append(hr_2px)

    parts.append("<!-- Описание -->")
    parts.append(desc_body)

    # Примечания (вынесены из "параметров-фраз", чтобы не засорять характеристики)
    if notes:
        nn: list[str] = []
        for x in (notes or [])[:2]:
            t = xml_escape_text(norm_ws(x))
            if t:
                # косметика: город и пунктуация
                t = t.replace("Нур: Султан", "Нур-Султан").replace("Нур : Султан", "Нур-Султан")
                t = re.sub(r"\s*:\s*", ": ", t)
                t = re.sub(r"(?:,\s*){2,}", ", ", t)
                t = re.sub(r":\s*:", ": ", t)
                t = re.sub(r"\s{2,}", " ", t).strip()
                # пробел после точки/воскл/вопрос/многоточия перед заглавной буквой
                t = re.sub(r"([.!?…])([A-ZА-ЯЁ])", r"\1 \2", t)
                # пробел между цифрой и кириллицей (>=1299Рекомендуемое -> >=1299 Рекомендуемое)
                t = re.sub(r"(\d)([А-Яа-яЁё])", r"\1 \2", t)
                if len(t) > 180:
                    t = t[:180].rstrip(" ,.;") + "…"
                nn.append(t)
        if nn:
            parts.append(f"<p><strong>Примечание:</strong> " + "<br>".join(nn) + "</p>")

    if chars:
        parts.append(chars)
    parts.append(pay_block)

    inner = "\n".join([p for p in parts if p is not None and str(p).strip() != ""])
        # CS: запрещено выводить название поставщика в тексте (кроме ссылок на фото)
    inner = re.sub(r"(?i)\bal[-\s]?style\b", "нашем магазине", inner)
    inner = re.sub(r"(?i)\bal[-\s]?style\.kz\b", "", inner)
    inner = re.sub(r"\s{2,}", " ", inner)
    return normalize_cdata_inner(inner)
