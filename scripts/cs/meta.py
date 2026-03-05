# -*- coding: utf-8 -*-
"""
CS Meta — время сборки, next-run и вспомогательные функции для FEED_META.

Этап 5: вынос части мета-логики из cs/core.py в отдельный модуль.
Важно: модуль НЕ импортирует cs/core.py (чтобы не ловить циклические импорты).

Сейчас переносим:
- now_almaty()
- next_run_at_hour()
(подготовка к следующему шагу: вынести make_feed_meta/FEED_META полностью из writer)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


ALMATY_TZ = ZoneInfo("Asia/Almaty")


def now_almaty() -> datetime:
    """Текущее время в Алматы (timezone-aware)."""
    return datetime.now(tz=ALMATY_TZ)


def next_run_at_hour(build_time: datetime, *, hour: int) -> datetime:
    """Следующая сборка в Алматы на заданный час (0..23)."""
    bt = build_time.astimezone(ALMATY_TZ)
    target = bt.replace(hour=int(hour), minute=0, second=0, microsecond=0)
    if target <= bt:
        target = target + timedelta(days=1)
    return target
