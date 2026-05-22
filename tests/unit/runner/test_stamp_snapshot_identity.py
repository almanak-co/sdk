"""VIB-4224 ACC-01 — narrow unit test for the two `runner_state.py` helpers
that have a function-level seam for `deployment_id` stamping.

Per UAT card §4.3 (frozen at SPEC_OK iter 7): the unit test covers ONLY the
two helpers that have a dedicated function — `_stamp_snapshot_identity`
(snapshot) and `_build_metrics_for_snapshot` (metrics). The other 4 row
classes (LedgerEntry / AccountingEvent / PositionEvent /
PositionStateSnapshot) propagate `deployment_id` via constructor-arg or
augment-chain mechanisms with no helper-level mock seam; they're caught at
e2e level by the §7 D4.3b SQL count.

Distinguishability (the asymmetric drift-guard):

- `_stamp_snapshot_identity` is 3-step (`runner.deployment_id →
  snapshot.deployment_id → snapshot.deployment_id`).
- `_build_metrics_for_snapshot` is 2-step (`runner.deployment_id →
  snapshot.deployment_id`, no intermediate `snapshot.deployment_id`).

Drift guard: snapshot with non-empty `snapshot.deployment_id` + empty
`runner.deployment_id` → snapshot's value in helper #1, `deployment_id` in
helper #2. A future commit that unifies / accidentally diverges the two
chains gets caught.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.portfolio import PortfolioSnapshot, ValueConfidence
from almanak.framework.runner.runner_state import (
    _build_metrics_for_snapshot,
    _stamp_snapshot_identity,
)


def _runner(
    deployment_id: str = "X-test",
    state_manager_returns_existing: bool = False,
    sum_ledger_returns: Decimal | None = Decimal("0"),
):
    """Build a runner mock: deployment_id, state_manager.get_portfolio_metrics
    returns None (first-run path) or an existing PortfolioMetrics, and
    sum_ledger_gas_usd returns the configured Decimal.
    """
    runner = MagicMock()
    runner.deployment_id = deployment_id
    runner.config = MagicMock()
    runner._last_cycle_id = "cycle-Y"

    state_manager = MagicMock()
    if state_manager_returns_existing:
        from almanak.framework.portfolio import PortfolioMetrics
        existing = PortfolioMetrics(
            timestamp=datetime.now(UTC),
            initial_value_usd=Decimal("100"),
            total_value_usd=Decimal("100"),
            deployment_id="",
            execution_mode="",
            cycle_id="",
        )
        state_manager.get_portfolio_metrics = AsyncMock(return_value=existing)
    else:
        state_manager.get_portfolio_metrics = AsyncMock(return_value=None)
    state_manager.sum_ledger_gas_usd = AsyncMock(return_value=sum_ledger_returns)
    runner.state_manager = state_manager
    return runner


def _snapshot(
    *,
    deployment_id: str = "demo",
    total_value_usd: Decimal = Decimal("100"),
):
    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        total_value_usd=total_value_usd,
        available_cash_usd=total_value_usd,
        value_confidence=ValueConfidence.HIGH,
        deployment_id=deployment_id,
        snapshot_metadata={},
    )


# --- Forward direction: both helpers stamp deployment_id from runner ---------

def test_snapshot_helper_stamps_runner_deployment_id() -> None:
    """_stamp_snapshot_identity copies runner.deployment_id onto the snapshot."""
    runner = _runner(deployment_id="X-test")
    snapshot = _snapshot()
    with patch(
        "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
        return_value=MagicMock(value="paper"),
    ):
        _stamp_snapshot_identity(runner, snapshot)
    assert snapshot.deployment_id == "X-test"


@pytest.mark.asyncio
async def test_metrics_helper_stamps_runner_deployment_id() -> None:
    """_build_metrics_for_snapshot stamps runner.deployment_id on first-run path."""
    runner = _runner(deployment_id="X-test")
    snapshot = _snapshot()
    with patch(
        "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
        return_value=MagicMock(value="paper"),
    ):
        metrics = await _build_metrics_for_snapshot(runner, "demo", snapshot)
    assert metrics is not None
    assert metrics.deployment_id == "X-test"


# --- Reverse direction: fallback chain (empty runner.deployment_id) ----------

def test_snapshot_helper_3_step_fallback() -> None:
    """3-step chain: empty runner + non-empty snapshot.deployment_id → snapshot's value."""
    runner = _runner(deployment_id="")
    snapshot = _snapshot(deployment_id="snapshot-pre-stamped")
    with patch(
        "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
        return_value=MagicMock(value="paper"),
    ):
        _stamp_snapshot_identity(runner, snapshot)
    assert snapshot.deployment_id == "snapshot-pre-stamped"


def test_snapshot_helper_does_not_fallback_when_identity_is_empty() -> None:
    """Empty runner + empty snapshot stays empty; deployment_id is resolved at boot."""
    runner = _runner(deployment_id="")
    snapshot = _snapshot(deployment_id="")
    with patch(
        "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
        return_value=MagicMock(value="paper"),
    ):
        _stamp_snapshot_identity(runner, snapshot)
    assert snapshot.deployment_id == ""


@pytest.mark.asyncio
async def test_metrics_helper_uses_snapshot_deployment_id_when_runner_identity_is_empty() -> None:
    """No legacy fallback: metrics uses the already-stamped snapshot deployment_id."""
    runner = _runner(deployment_id="")
    snapshot = _snapshot(deployment_id="snapshot-pre-stamped")
    with patch(
        "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
        return_value=MagicMock(value="paper"),
    ):
        metrics = await _build_metrics_for_snapshot(runner, "my-strat", snapshot)
    assert metrics is not None
    assert metrics.deployment_id == "snapshot-pre-stamped"


# --- Existing-row path on metrics helper ------------------------------------

@pytest.mark.asyncio
async def test_metrics_helper_existing_row_refreshes_deployment_id_when_blank() -> None:
    """Existing-row path: refreshes deployment_id only when previously blank."""
    runner = _runner(deployment_id="X-test", state_manager_returns_existing=True)
    snapshot = _snapshot()
    with patch(
        "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
        return_value=MagicMock(value="paper"),
    ):
        metrics = await _build_metrics_for_snapshot(runner, "demo", snapshot)
    assert metrics is not None
    assert metrics.deployment_id == "X-test"
