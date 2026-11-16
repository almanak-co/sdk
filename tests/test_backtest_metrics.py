"""Headline metrics over synthetic equity curves; reporting math only."""

import statistics
from decimal import Decimal

import pytest

from almanak.backtest.metrics import (
    daily_marks,
    max_drawdown_pct,
    sharpe_ratio,
    total_return_pct,
)

DAY = 86400
# 2026-08-20T00:00:00Z is exactly on a UTC day boundary.
START = 1787184000


def curve(*values):
    return [(START + index * DAY, Decimal(value)) for index, value in enumerate(values)]


def test_total_return_is_none_without_two_points_or_a_positive_start():
    assert total_return_pct([]) is None
    assert total_return_pct([(START, Decimal(100))]) is None
    assert total_return_pct(curve(0, 100)) is None
    assert total_return_pct(curve(100, 110)) == pytest.approx(10.0)
    assert total_return_pct(curve(100, 50)) == pytest.approx(-50.0)


def test_max_drawdown_tracks_the_worst_peak_to_trough_decline():
    assert max_drawdown_pct([]) is None
    assert max_drawdown_pct(curve(100)) == 0.0
    assert max_drawdown_pct(curve(100, 120, 60, 90, 130)) == pytest.approx(50.0)
    assert max_drawdown_pct(curve(100, 100, 100)) == 0.0
    assert max_drawdown_pct(curve(100, 150, 120)) == pytest.approx(20.0)


def test_sharpe_is_none_for_flat_or_short_curves():
    assert sharpe_ratio([]) is None
    assert sharpe_ratio(curve(100)) is None
    assert sharpe_ratio(curve(100, 101)) is None
    assert sharpe_ratio(curve(100, 100, 100, 100)) is None


def test_sharpe_annualizes_daily_simple_returns():
    points = curve(100, 101, 100, 102)
    returns = [0.01, -1 / 101, 0.02]
    expected = statistics.mean(returns) * 365 / (statistics.stdev(returns) * 365**0.5)
    assert sharpe_ratio(points) == pytest.approx(expected)


def test_daily_marks_take_the_last_point_of_each_utc_day():
    points = [
        (START + 100, Decimal(1)),
        (START + 200, Decimal(2)),
        (START + DAY + 100, Decimal(3)),
    ]
    assert [value for _, value in daily_marks(points)] == [Decimal(2), Decimal(3)]


def test_daily_marks_carry_equity_forward_across_unmarked_days():
    points = [
        (START, Decimal(100)),
        (START + 4 * DAY, Decimal(105)),
    ]
    marks = daily_marks(points)
    assert [value for _, value in marks] == [Decimal(100)] * 4 + [Decimal(105)]
    # The 3-day gap becomes three flat daily returns plus one 5 percent return.
    returns = [0.0, 0.0, 0.0, 0.05]
    expected = statistics.mean(returns) * 365 / (statistics.stdev(returns) * 365**0.5)
    assert sharpe_ratio(points) == pytest.approx(expected)
