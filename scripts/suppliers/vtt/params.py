# -*- coding: utf-8 -*-
"""
Path: scripts/suppliers/vtt/params.py

VTT canonical params module.

Задача этого шага:
- ввести каноническое имя файла `params.py` для supplier-package;
- не ломать текущую рабочую сборку;
- сохранить всю действующую логику в `params_page.py` до следующего патча;
- дать новый стабильный import-path: `suppliers.vtt.params`.

Важно:
- это намеренно минимальный и безопасный bridge-шаг;
- в следующем патче основная логика будет перенесена сюда,
  а `params_page.py` станет compatibility shim.
"""

from __future__ import annotations

from .params_page import *  # noqa: F401,F403
