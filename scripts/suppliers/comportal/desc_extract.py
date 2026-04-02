# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/comportal/desc_extract.py

ComPortal description -> params extraction.

Роль как у других поставщиков:
- не главный extractor;
- только fill-missing / safe extraction;
- не дублирует params_xml.py.

У ComPortal основной сигнал идёт из XML params, а не из narrative body.
"""

from __future__ import annotations

import re
from typing import Iterable

from cs.util import norm_ws
from suppliers.comportal.models import ParamItem


_CODE_TAIL_RE = re.compile(r"\(([A-Za-z0-9#/\-\.]+)\)\s*$")
_RESOURCE_RE = re.compile(r"(?iu)\bресурс\s*[:\-]?\s*([\d\s.,]+(?:стр(?:аниц|\.?)?)?)")
_WARRANTY_RE = re.compile(r"(?iu)\bгарант(?:ия)?\s*[:\-]?\s*(\d{1,3})\s*(?:мес|месяц|месяцев|month|months)\b")
_COLOR_RE = re.compile(
    r"(?iu)\b(ч[её]рн(?:ый|ая)?|ж[её]лт(?:ый|ая)?|голуб(?:ой|ая)?|пурпурн(?:ый|ая)?|"
    r"бел(?:ый|ая)?|сер(?:ый|ая)?|красн(?:ый|ая)?|син(?:ий|яя)|зел[её]н(?:ый|ая)?)\b"
)


def _param_map(params: Iterable[ParamItem]) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in params or []:
        name = norm_ws(p.name)
        value = norm_ws(p.value)
        if name and value and name.casefold() not in {k.casefold() for k in out}:
            out[name] = value
    return out


def _append_if_missing(out: list[ParamItem], existing: dict[str, str], *, name: str, value: str, source: str) -> None:
    if not norm_ws(name) or not norm_ws(value):
        return
    if any(k.casefold() == name.casefold() for k in existing):
        return
    out.append(ParamItem(name=norm_ws(name), value=norm_ws(value), source=source))
    existing[name] = value


def extract_desc_fill_params(
    *,
    title: str,
    desc_text: str,
    existing_params: list[ParamItem],
) -> list[ParamItem]:
    """
    Аккуратно добрать несколько полей из title/desc, только если их нет.
    """
    out = list(existing_params or [])
    existing = _param_map(out)

    ttl = norm_ws(title)
    desc = norm_ws(desc_text)

    if "Коды" not in existing and ttl:
        m = _CODE_TAIL_RE.search(ttl)
        if m:
            _append_if_missing(out, existing, name="Коды", value=m.group(1), source="desc_fill")

    if "Модель" not in existing and ttl:
        m = _CODE_TAIL_RE.search(ttl)
        if m:
            _append_if_missing(out, existing, name="Модель", value=m.group(1), source="desc_fill")

    if "Ресурс" not in existing and desc:
        m = _RESOURCE_RE.search(desc)
        if m:
            _append_if_missing(out, existing, name="Ресурс", value=m.group(1), source="desc_fill")

    if "Гарантия" not in existing and desc:
        m = _WARRANTY_RE.search(desc)
        if m:
            _append_if_missing(out, existing, name="Гарантия", value=f"{int(m.group(1))} мес", source="desc_fill")

    if "Цвет" not in existing and desc:
        m = _COLOR_RE.search(desc)
        if m:
            color = norm_ws(m.group(1))
            mapping = {
                "черный": "Чёрный",
                "черная": "Чёрный",
                "чёрный": "Чёрный",
                "чёрная": "Чёрный",
                "желтый": "Жёлтый",
                "желтая": "Жёлтый",
                "жёлтый": "Жёлтый",
                "жёлтая": "Жёлтый",
                "голубой": "Голубой",
                "голубая": "Голубой",
                "пурпурный": "Пурпурный",
                "пурпурная": "Пурпурный",
                "белый": "Белый",
                "белая": "Белый",
                "серый": "Серый",
                "серая": "Серый",
                "красный": "Красный",
                "красная": "Красный",
                "синий": "Синий",
                "синяя": "Синий",
                "зеленый": "Зелёный",
                "зелёный": "Зелёный",
                "зеленая": "Зелёный",
                "зелёная": "Зелёный",
            }
            _append_if_missing(
                out,
                existing,
                name="Цвет",
                value=mapping.get(color.casefold(), color),
                source="desc_fill",
            )

    return out


__all__ = [
    "extract_desc_fill_params",
]
