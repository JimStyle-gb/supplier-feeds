# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/params_page.py

VTT temporary backward-safe shim.

Зачем файл оставлен:
- каноническое имя extractor-модуля для CS-шаблона теперь `params.py`;
- старые импорты `params_page.py` временно не ломаем;
- вся рабочая логика уже должна жить в `params.py`.

Важно:
- здесь НЕ должно быть своей business-логики;
- здесь только re-export public API из .params;
- после зелёного прогона и перевода всех импортов файл можно удалить.
"""

from __future__ import annotations

from .params import *  # noqa: F401,F403
