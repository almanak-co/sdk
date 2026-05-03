"""VIB-3887 — derive ``in_range`` from gateway-supplied current_tick.

Codex F4 corrected the v1 framing: framework / strategy-side connector
code **cannot** make direct RPC calls. ``pool.slot0().tick`` cannot
live in ``framework/``. The gateway-side receipt parser is the right
authority — it runs in the gateway process, which already holds the
RPC connection.

This test fences the framework-side consumer: when ``LPOpenData``
carries a ``current_tick`` field (populated by the gateway parser),
``_apply_lp_open`` derives ``in_range`` from ``tick_lower / current /
tick_upper``. When the field is missing, ``in_range`` stays None —
readers degrade gracefully.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from almanak.framework.observability.position_events import (
    IntentEventContext,
    PositionEvent,
    _apply_lp_open,
)


def _make_event() -> PositionEvent:
    return PositionEvent(
        deployment_id="d",
        position_type="LP",
        event_type="OPEN",
        chain="arbitrum",
    )


def _make_ctx(lp_open_data: Any, intent: Any | None = None) -> IntentEventContext:
    return IntentEventContext(
        intent=intent or SimpleNamespace(token0="WETH", token1="USDC"),
        result=None,
        extracted={"lp_open_data": lp_open_data},
        deployment_id="d",
        chain="arbitrum",
        ledger_entry_id="led-1",
    )


def test_in_range_true_when_current_tick_within_bracket():
    lp_open = SimpleNamespace(
        position_id=5463956,
        tick_lower=-199960,
        tick_upper=-197960,
        current_tick=-198960,  # mid-range
        liquidity=928906698473,
        amount0=891556839636852,
        amount1=2294332,
    )
    event = _make_event()
    _apply_lp_open(event, _make_ctx(lp_open))
    assert event.in_range is True


def test_in_range_false_when_current_tick_below_lower():
    lp_open = SimpleNamespace(
        position_id=1,
        tick_lower=-199960,
        tick_upper=-197960,
        current_tick=-200000,  # below lower
        liquidity=1,
        amount0=1,
        amount1=1,
    )
    event = _make_event()
    _apply_lp_open(event, _make_ctx(lp_open))
    assert event.in_range is False


def test_in_range_false_when_current_tick_at_upper_bound():
    """Upper bound is exclusive — equality counts as out of range."""
    lp_open = SimpleNamespace(
        position_id=1,
        tick_lower=-199960,
        tick_upper=-197960,
        current_tick=-197960,  # at upper bound
        liquidity=1,
        amount0=1,
        amount1=1,
    )
    event = _make_event()
    _apply_lp_open(event, _make_ctx(lp_open))
    assert event.in_range is False


def test_in_range_true_when_current_tick_at_lower_bound():
    """Lower bound is inclusive."""
    lp_open = SimpleNamespace(
        position_id=1,
        tick_lower=-199960,
        tick_upper=-197960,
        current_tick=-199960,
        liquidity=1,
        amount0=1,
        amount1=1,
    )
    event = _make_event()
    _apply_lp_open(event, _make_ctx(lp_open))
    assert event.in_range is True


def test_in_range_none_when_current_tick_missing():
    """The May 2 reproducer: gateway hasn't yet been updated to populate
    current_tick. ``in_range`` stays None — operator dashboard renders
    the "—" placeholder rather than a misleading boolean."""
    lp_open = SimpleNamespace(
        position_id=1,
        tick_lower=-199960,
        tick_upper=-197960,
        # current_tick deliberately omitted
        liquidity=1,
        amount0=1,
        amount1=1,
    )
    event = _make_event()
    _apply_lp_open(event, _make_ctx(lp_open))
    assert event.in_range is None


def test_in_range_none_when_only_one_tick_bound():
    """Defensive: if either bracket bound is missing, in_range is undefined."""
    lp_open = SimpleNamespace(
        position_id=1,
        tick_lower=-199960,
        tick_upper=None,
        current_tick=-198000,
        liquidity=1,
        amount0=1,
        amount1=1,
    )
    event = _make_event()
    _apply_lp_open(event, _make_ctx(lp_open))
    assert event.in_range is None
