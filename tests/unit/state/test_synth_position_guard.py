"""Unit tests for ``_apply_synth_position_guard`` (VIB-3917 / VIB-4098).

CodeRabbit (PR #2162) flagged that the helper changes persisted confidence
semantics on a subtle ``None`` vs ``Decimal("0")`` distinction and that the
project guideline requires ``tests/unit`` coverage for state-management
logic changes. This file locks the truth table:

    cb              v               action
    --------------  --------------  ------
    None            *               leave HIGH (unmeasured ≠ zero)
    Decimal("0")    None            leave HIGH (oracle silent)
    Decimal("0")    Decimal("0")    leave HIGH (genuine zero position)
    Decimal("0")    > 0             degrade   (true basis violation)
    > 0             *               leave HIGH

Plus the no-op short-circuits on ``value_confidence is None`` and on any
non-HIGH starting confidence.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    PositionValue,
    ValueConfidence,
)
from almanak.framework.state.gateway_state_manager import _apply_synth_position_guard
from almanak.framework.teardown.models import PositionType


def _make_snap(
    cb: Decimal | None,
    val: Decimal | None,
    *,
    initial_confidence: ValueConfidence | None = ValueConfidence.HIGH,
) -> PortfolioSnapshot:
    """Build a one-position snapshot. ``cb=None`` bypasses post_init coercion."""
    pos = PositionValue(
        position_type=PositionType.LP,
        protocol="uniswap_v3",
        chain="arbitrum",
        value_usd=val if val is not None else Decimal("0"),
        label="WETH/USDC",
        cost_basis_usd=cb if cb is not None else Decimal("0"),
    )
    if cb is None:
        pos.cost_basis_usd = None  # type: ignore[assignment]
    if val is None:
        pos.value_usd = None  # type: ignore[assignment]
    snap = PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id="UnitGuard:t",
        total_value_usd=val if val is not None else Decimal("0"),
        available_cash_usd=Decimal("0"),
        value_confidence=initial_confidence if initial_confidence is not None else ValueConfidence.HIGH,
        positions=[pos],
    )
    if initial_confidence is None:
        snap.value_confidence = None  # type: ignore[assignment]
    return snap


# --- Truth-table cases (HIGH start) -----------------------------------


@pytest.mark.parametrize(
    "cb, val",
    [
        (None, Decimal("100")),  # unmeasured basis
        (None, None),  # both unmeasured
        (Decimal("0"), None),  # oracle silent
        (Decimal("0"), Decimal("0")),  # genuine zero
        (Decimal("1.00"), Decimal("100")),  # measured basis
        (Decimal("0.50"), Decimal("0")),  # measured basis, no value
    ],
)
def test_synth_guard_preserves_high(cb, val) -> None:
    snap = _make_snap(cb, val)
    _apply_synth_position_guard(snap)
    assert snap.value_confidence == ValueConfidence.HIGH


def test_synth_guard_degrades_measured_zero_with_positive_value() -> None:
    """The only case that must degrade: measured-zero basis + positive value."""
    snap = _make_snap(Decimal("0"), Decimal("123.45"))
    _apply_synth_position_guard(snap)
    assert snap.value_confidence == ValueConfidence.ESTIMATED


# --- Short-circuit cases ----------------------------------------------


def test_synth_guard_noop_when_confidence_is_none() -> None:
    """``value_confidence is None`` returns without inspecting positions."""
    snap = _make_snap(Decimal("0"), Decimal("100"), initial_confidence=None)
    _apply_synth_position_guard(snap)
    assert snap.value_confidence is None


@pytest.mark.parametrize(
    "starting",
    [ValueConfidence.ESTIMATED, ValueConfidence.STALE, ValueConfidence.UNAVAILABLE],
)
def test_synth_guard_noop_when_not_high(starting) -> None:
    """The guard only runs against HIGH; any other starting confidence is left alone."""
    snap = _make_snap(Decimal("0"), Decimal("100"), initial_confidence=starting)
    _apply_synth_position_guard(snap)
    assert snap.value_confidence == starting


# --- Empty / no-positions case ----------------------------------------


def test_synth_guard_empty_positions_preserves_high() -> None:
    """A HIGH snapshot with no positions must not be degraded."""
    snap = PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id="UnitGuard:empty",
        total_value_usd=Decimal("0"),
        available_cash_usd=Decimal("0"),
        value_confidence=ValueConfidence.HIGH,
        positions=[],
    )
    _apply_synth_position_guard(snap)
    assert snap.value_confidence == ValueConfidence.HIGH
