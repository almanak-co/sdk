"""VIB-3882 — strategy.allocation_usd anchors portfolio baseline.

Codex F2 corrected the v1 framing (the "DEPOSIT accounting event" path
was an ad-hoc bypass). The chosen design is a generic strategy property
``StrategyBase.allocation_usd`` that defaults to reading
``config.total_value_usd`` for backwards compat, but is independently
overridable.

These tests fence the runner-side consumption: when a strategy
exposes a positive ``allocation_usd``, ``_build_metrics_for_snapshot``
uses it as the baseline; otherwise the legacy "first observed wallet
total" heuristic runs.
"""

from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

# ─── Stubs for runner deps that aren't relevant to this baseline test ─────


@dataclass
class _StubSnapshot:
    """Minimal stand-in for PortfolioSnapshot — only carries the fields
    `_build_metrics_for_snapshot` reads."""

    strategy_id: str
    timestamp: datetime
    total_value_usd: Decimal
    available_cash_usd: Decimal
    value_confidence: Any  # kept opaque
    error: str = ""


class _StubStateManager:
    """Captures the metrics object the runner is about to persist."""

    def __init__(self) -> None:
        self.saved: list[Any] = []

    async def get_portfolio_metrics(self, strategy_id: str) -> Any:
        return None  # bootstrap path


class _StubRunner:
    def __init__(self, sm: _StubStateManager) -> None:
        self.state_manager = sm
        self.config = SimpleNamespace(
            execution_mode="live",
            run_mode="live",
            chain="arbitrum",
        )
        self._last_cycle_id = "test-cycle"
        self.deployment_id = "test-deployment"


@pytest.fixture
def value_confidence_high() -> Any:
    """Pull the live ValueConfidence enum (must compare ``!= UNAVAILABLE``)."""
    from almanak.framework.runner.runner_state import ValueConfidence

    return ValueConfidence.HIGH


@pytest.fixture
def stub_runner() -> _StubRunner:
    return _StubRunner(_StubStateManager())


@pytest.fixture
def fresh_snapshot(value_confidence_high: Any) -> _StubSnapshot:
    """A snapshot with cash > 0 and positions = 0 — pre-deployment shape."""
    return _StubSnapshot(
        strategy_id="strat-h1",
        timestamp=datetime.now(tz=UTC),
        total_value_usd=Decimal("0"),
        available_cash_usd=Decimal("19.26"),  # the May 2 wallet baseline
        value_confidence=value_confidence_high,
    )


# ──────────────────────────────────────────────────────────────────────────
# Property — direct coverage of ``StrategyBase.allocation_usd``
# ──────────────────────────────────────────────────────────────────────────


def _make_strategy(total_value_usd: Any) -> Any:
    """Build a minimal subclass of ``StrategyBase`` with a config namespace."""
    from almanak.framework.strategies.base import StrategyBase

    class _Strategy(StrategyBase):
        STRATEGY_NAME = "h1-test"

        def run(self) -> None:  # abstract method satisfaction
            return None

    cfg = SimpleNamespace(
        strategy_id="strat-h1",
        chain="arbitrum",
        total_value_usd=total_value_usd,
    )
    return _Strategy(config=cfg)


def test_allocation_property_reads_config_total_value_usd():
    s = _make_strategy(total_value_usd="4.0")
    assert s.allocation_usd == Decimal("4.0")


def test_allocation_property_handles_decimal_input():
    s = _make_strategy(total_value_usd=Decimal("4.0"))
    assert s.allocation_usd == Decimal("4.0")


def test_allocation_property_returns_none_when_field_absent():
    """Strategies that don't declare ``total_value_usd`` opt out of H1
    cleanly — runner falls back to the wallet-observation baseline."""
    s = _make_strategy(total_value_usd=None)
    assert s.allocation_usd is None


def test_allocation_property_returns_none_on_zero_or_negative():
    """Zero or negative allocation is meaningless — treated as opt-out."""
    assert _make_strategy(total_value_usd="0").allocation_usd is None
    assert _make_strategy(total_value_usd="-1").allocation_usd is None


def test_allocation_property_returns_none_on_garbage_input():
    """Garbage input must NOT raise; it's a runner-startup path."""
    assert _make_strategy(total_value_usd="not-a-number").allocation_usd is None
    assert _make_strategy(total_value_usd="").allocation_usd is None


# ──────────────────────────────────────────────────────────────────────────
# Runner integration — `_build_metrics_for_snapshot` consumes allocation
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_baseline_uses_strategy_allocation_when_set(
    stub_runner: _StubRunner, fresh_snapshot: _StubSnapshot
):
    """The May 2 reproducer: wallet has $19.26, strategy declares $4 →
    baseline must be $4 (allocation), not $19.26 (wallet)."""
    from almanak.framework.runner.runner_state import _build_metrics_for_snapshot

    strategy = _make_strategy(total_value_usd="4.0")
    metrics = await _build_metrics_for_snapshot(
        stub_runner, "strat-h1", fresh_snapshot, strategy=strategy
    )
    assert metrics is not None
    assert metrics.initial_value_usd == Decimal("4.0"), (
        "VIB-3882: baseline must come from strategy.allocation_usd, "
        "not snapshot.available_cash_usd"
    )


@pytest.mark.asyncio
async def test_baseline_falls_back_to_wallet_when_allocation_missing(
    stub_runner: _StubRunner, fresh_snapshot: _StubSnapshot
):
    """Legacy strategies (no ``total_value_usd`` in config) get the
    pre-VIB-3882 baseline behaviour — first observed wallet total."""
    from almanak.framework.runner.runner_state import _build_metrics_for_snapshot

    strategy = _make_strategy(total_value_usd=None)
    metrics = await _build_metrics_for_snapshot(
        stub_runner, "strat-h1", fresh_snapshot, strategy=strategy
    )
    assert metrics is not None
    assert metrics.initial_value_usd == Decimal("19.26")


@pytest.mark.asyncio
async def test_baseline_falls_back_when_no_strategy_passed(
    stub_runner: _StubRunner, fresh_snapshot: _StubSnapshot
):
    """``strategy=None`` keeps the legacy behaviour for callers that
    don't have a strategy in scope (e.g. ``update_portfolio_metrics``).
    """
    from almanak.framework.runner.runner_state import _build_metrics_for_snapshot

    metrics = await _build_metrics_for_snapshot(
        stub_runner, "strat-h1", fresh_snapshot, strategy=None
    )
    assert metrics is not None
    assert metrics.initial_value_usd == Decimal("19.26")


@pytest.mark.asyncio
async def test_baseline_allocation_overrides_high_wallet_balance(
    stub_runner: _StubRunner, value_confidence_high: Any
):
    """The headline case: shared test wallet pre-funded with $1000;
    strategy declares $4. Baseline = $4."""
    from almanak.framework.runner.runner_state import _build_metrics_for_snapshot

    snapshot = _StubSnapshot(
        strategy_id="strat-h1",
        timestamp=datetime.now(tz=UTC),
        total_value_usd=Decimal("0"),
        available_cash_usd=Decimal("1000.00"),
        value_confidence=value_confidence_high,
    )
    strategy = _make_strategy(total_value_usd="4.0")
    metrics = await _build_metrics_for_snapshot(
        stub_runner, "strat-h1", snapshot, strategy=strategy
    )
    assert metrics is not None
    assert metrics.initial_value_usd == Decimal("4.0")
