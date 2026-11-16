"""Headline performance metrics over a finished quote-valued equity curve.

This is reporting math over curve points, not accounting: float arithmetic is
deliberate. Inputs are (unix time, equity value) pairs ordered by time.
"""

import math
import statistics
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise

DAYS_PER_YEAR = 365


def total_return_pct(points: list[tuple[int, Decimal]]) -> float | None:
    if len(points) < 2 or points[0][1] <= 0:
        return None
    initial, final = points[0][1], points[-1][1]
    return float((final - initial) / initial * 100)


def max_drawdown_pct(points: list[tuple[int, Decimal]]) -> float | None:
    if not points:
        return None
    peak = points[0][1]
    worst = Decimal(0)
    for _, value in points:
        peak = max(peak, value)
        if peak > 0:
            worst = max(worst, (peak - value) / peak)
    return float(worst * 100)


def sharpe_ratio(points: list[tuple[int, Decimal]], risk_free: float = 0.0) -> float | None:
    marks = daily_marks(points)
    returns = [
        float(after / before) - 1.0 for (_, before), (_, after) in pairwise(marks) if before > 0
    ]
    if len(returns) < 2:
        return None
    deviation = statistics.stdev(returns)
    if deviation == 0:
        return None
    annualized = statistics.mean(returns) * DAYS_PER_YEAR - risk_free
    return annualized / (deviation * math.sqrt(DAYS_PER_YEAR))


def daily_marks(points: list[tuple[int, Decimal]]) -> list[tuple[object, Decimal]]:
    """One mark per UTC day; the last point of each day wins (points are ordered).

    Days without a point carry the last known equity forward: a portfolio's
    value persists across unmarked days, so pairwise returns stay true daily
    returns even for sparse curves.
    """
    marks: dict[date, Decimal] = {}
    for time, value in points:
        marks[datetime.fromtimestamp(time, UTC).date()] = value
    if not marks:
        return []
    day = min(marks)
    last = max(marks)
    filled: list[tuple[object, Decimal]] = []
    value = marks[day]
    while day <= last:
        value = marks.get(day, value)
        filled.append((day, value))
        day += timedelta(days=1)
    return filled
