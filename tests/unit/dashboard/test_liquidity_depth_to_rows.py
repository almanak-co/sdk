"""Regression tests for ``_liquidity_depth_to_rows`` tick walking.

Covers the Uniswap V3 reconstruction logic: row at tick ``X`` represents the
active liquidity for the range ``[X, X + tick_spacing)``. Walking *down*
from the current tick means crossing the upper boundary of the next-lower
range — so we subtract ``liquidity_net`` at ``X + tick_spacing``, not at
``X``. Gemini caught the off-by-one in PR #2390.
"""

from __future__ import annotations

from types import SimpleNamespace

from almanak.framework.dashboard.custom.api_client import _liquidity_depth_to_rows


def _depth(*, current_tick: int, tick_spacing: int, total_liquidity: int, ticks: dict[int, int]) -> SimpleNamespace:
    """Build a duck-typed depth object the rows-helper consumes."""
    return SimpleNamespace(
        current_tick=current_tick,
        total_liquidity=total_liquidity,
        tick_spacing=tick_spacing,
        token0_decimals=18,
        token1_decimals=6,
        ticks=[
            SimpleNamespace(tick_index=idx, liquidity_net=net) for idx, net in sorted(ticks.items())
        ],
    )


def _row(rows: list[dict], tick: int) -> dict:
    matching = [r for r in rows if r["tick_idx"] == tick]
    assert matching, f"no row for tick {tick}"
    return matching[0]


def test_downward_walk_uses_upper_boundary_liquidity_net() -> None:
    """The downward walk must subtract net at ``tick + tick_spacing``.

    Construct a depth where ticks 90 / 100 / 110 carry distinct liquidity_net
    values. Active liquidity for [90, 100) = current - net[100]. Active
    liquidity for [80, 90) = current - net[100] - net[90]. The buggy
    implementation subtracted ``net[90]`` for the [90, 100) row, which is
    the lower boundary, not the upper boundary it crosses.
    """
    rows = _liquidity_depth_to_rows(
        _depth(
            current_tick=105,
            tick_spacing=10,
            total_liquidity=1_000_000,
            ticks={
                100: 200_000,  # crossing 100 going down subtracts 200_000
                90: 50_000,
                80: 10_000,
            },
        )
    )

    # Active tick row: full current liquidity.
    assert _row(rows, 100)["liquidity_active"] == 1_000_000

    # Range [90, 100): cross tick 100 going down → -200_000.
    assert _row(rows, 90)["liquidity_active"] == 1_000_000 - 200_000

    # Range [80, 90): cross tick 90 going down → additionally -50_000.
    assert _row(rows, 80)["liquidity_active"] == 1_000_000 - 200_000 - 50_000


def test_upward_walk_uses_lower_boundary_liquidity_net() -> None:
    """The upward walk subtracts net at the new range's lower boundary."""
    rows = _liquidity_depth_to_rows(
        _depth(
            current_tick=105,
            tick_spacing=10,
            total_liquidity=1_000_000,
            ticks={110: 300_000, 120: 25_000},
        )
    )

    # Active tick row: full current liquidity.
    assert _row(rows, 100)["liquidity_active"] == 1_000_000

    # Range [110, 120): cross tick 110 going up → +300_000.
    assert _row(rows, 110)["liquidity_active"] == 1_000_000 + 300_000

    # Range [120, 130): cross tick 120 going up → additionally +25_000.
    assert _row(rows, 120)["liquidity_active"] == 1_000_000 + 300_000 + 25_000


def test_active_liquidity_clamped_to_zero() -> None:
    """Negative active liquidity is clamped to 0 in the row payload."""
    rows = _liquidity_depth_to_rows(
        _depth(
            current_tick=105,
            tick_spacing=10,
            total_liquidity=100,
            ticks={100: 500},  # would push the [90, 100) row negative
        )
    )
    assert _row(rows, 90)["liquidity_active"] == 0
