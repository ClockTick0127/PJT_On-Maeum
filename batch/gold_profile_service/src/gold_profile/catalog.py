"""Dependency-aware invalidation helpers for rolling Gold profiles."""

from __future__ import annotations

from datetime import date, timedelta


def affected_as_of_dates(
    changed_date: date,
    *,
    window_days: int,
    through_date: date | None = None,
) -> tuple[date, ...]:
    """Profile dates whose inclusive rolling window contains ``changed_date``."""

    end = changed_date + timedelta(days=window_days - 1)
    if through_date is not None:
        end = min(end, through_date)
    if end < changed_date:
        return ()
    return tuple(
        changed_date + timedelta(days=offset)
        for offset in range((end - changed_date).days + 1)
    )
