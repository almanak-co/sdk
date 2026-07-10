"""Characterization tests for ``runner_state.capture_portfolio_snapshot``.

These tests pin the CURRENT behavior of the function as Phase 8.1b prepares
helper extraction. The snapshot path drives the ledger + dashboard valuation
pipeline; ``PortfolioSnapshot`` corruption silently breaks PnL tracking and
position-event emission, so every observable branch below must remain
unchanged after extraction.

Scope (each covered by at least one test below):

- Throttle skip when called too soon without ``force_snapshot``.
- ``force_snapshot=True`` bypass of the throttle.
- Primary path: ``PortfolioValuer`` returns a valid snapshot that persists.
- Multi-chain strategies skip the primary valuer and fall through to the
  strategy-supplied ``get_portfolio_snapshot``.
- ``PortfolioValuer`` raising -> fallback path is used.
- Fallback returns ``UNAVAILABLE`` -> framework constructs the final
  ``UNAVAILABLE`` snapshot.
- No valuation path at all -> ``UNAVAILABLE`` snapshot is still persisted
  (never skipped silently).
- Atomic co-write (``save_snapshot_and_metrics``) used when available.
- Separate writes fallback when the state manager does not implement the
  atomic helper.
- Valuation fields written into strategy state (dashboard path).
- ``AccountingPersistenceError`` propagates unchanged.
- Generic Exception -> ``UNAVAILABLE`` snapshot persisted as failure
  fallback (``runner._last_snapshot_time`` still advances).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.portfolio import (
    PortfolioSnapshot,
    ValueConfidence,
)
from almanak.framework.runner.runner_state import capture_portfolio_snapshot
from almanak.framework.state.exceptions import AccountingPersistenceError
from almanak.framework.state.state_manager import StateData

# =============================================================================
# Fixtures
# =============================================================================


def _make_state_data(deployment_id: str = "test-strategy") -> StateData:
    return StateData(
        deployment_id=deployment_id,
        version=1,
        state={},
    )


class _StateManagerStub:
    """Minimal state manager stub.

    Uses real attributes (not ``AsyncMock`` auto-attrs) so ``hasattr`` checks
    inside ``capture_portfolio_snapshot`` -- particularly for the optional
    ``save_snapshot_and_metrics`` atomic co-write helper -- reflect only the
    methods we explicitly expose per test.
    """

    def __init__(self) -> None:
        self.save_portfolio_snapshot = AsyncMock(return_value=1)
        self.save_portfolio_metrics = AsyncMock(return_value=True)
        self.get_portfolio_metrics = AsyncMock(return_value=None)
        self.load_state = AsyncMock(return_value=_make_state_data())
        self.save_state = AsyncMock()


def _make_runner(
    *,
    state_manager: object | None = None,
    portfolio_valuer: MagicMock | None = None,
    last_snapshot_time: datetime | None = None,
    snapshot_interval_seconds: int = 300,
    is_multi_chain: bool = False,
    config: object | None = None,
    deployment_id: str = "deploy-1",
    last_cycle_id: str = "cycle-1",
):
    """Build a minimal runner stub compatible with ``capture_portfolio_snapshot``.

    Only the attributes that the function actually reads are populated.
    """
    if state_manager is None:
        state_manager = _StateManagerStub()

    if portfolio_valuer is None:
        portfolio_valuer = MagicMock()
        portfolio_valuer.set_gateway_client = MagicMock()
        portfolio_valuer.value = MagicMock(return_value=None)

    runner = SimpleNamespace(
        state_manager=state_manager,
        _portfolio_valuer=portfolio_valuer,
        _last_snapshot_time=last_snapshot_time,
        _snapshot_interval_seconds=snapshot_interval_seconds,
        _is_multi_chain=is_multi_chain,
        _get_gateway_client=MagicMock(return_value=None),
        deployment_id=deployment_id,
        _last_cycle_id=last_cycle_id,
        config=config if config is not None else MagicMock(dry_run=False, paper_mode=False),
    )
    return runner


def _make_strategy(
    *,
    deployment_id: str = "test-strategy",
    chain: str = "arbitrum",
    supports_valuer: bool = True,
    supports_fallback: bool = True,
) -> MagicMock:
    """Mock strategy with configurable valuation capabilities."""
    strategy = MagicMock()
    strategy.deployment_id = deployment_id
    strategy.chain = chain

    if supports_valuer:
        strategy._get_tracked_tokens = MagicMock(return_value=[])
        strategy.create_market_snapshot = MagicMock(return_value=MagicMock())
    else:
        # Remove the attributes the primary path checks for.
        del strategy._get_tracked_tokens
        del strategy.create_market_snapshot

    if not supports_fallback:
        del strategy.get_portfolio_snapshot
    return strategy


def _make_snapshot(
    *,
    deployment_id: str = "test-strategy",
    total_value_usd: Decimal | str = "1000.50",
    confidence: ValueConfidence = ValueConfidence.HIGH,
    chain: str = "arbitrum",
    iteration_number: int = 0,
    snapshot_metadata: dict | None = None,
) -> PortfolioSnapshot:
    # VIB-4225 ACC-02: production ``PortfolioValuer.value`` always stamps
    # ``gas_native_status`` on snapshot_metadata, and
    # ``_enforce_native_gas_status_in_live`` raises in live mode on any
    # missing/non-ok status. The fixture must mirror that contract so the
    # capture-pipeline tests don't hit the runner-level enforcer's
    # missing-stamp guard. Tests that want to exercise a non-ok path can
    # pass an explicit ``snapshot_metadata={"gas_native_status": "..."}``.
    md = dict(snapshot_metadata) if snapshot_metadata else {}
    md.setdefault("gas_native_status", "ok")
    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        deployment_id=deployment_id,
        total_value_usd=Decimal(str(total_value_usd)),
        available_cash_usd=Decimal("0"),
        value_confidence=confidence,
        chain=chain,
        iteration_number=iteration_number,
        snapshot_metadata=md,
    )


# =============================================================================
# 1. Throttle
# =============================================================================


class TestThrottle:
    @pytest.mark.asyncio
    async def test_returns_none_when_interval_not_elapsed_and_not_forced(self):
        runner = _make_runner(
            last_snapshot_time=datetime.now(UTC) - timedelta(seconds=5),
            snapshot_interval_seconds=300,
        )
        strategy = _make_strategy()

        result = await capture_portfolio_snapshot(runner, strategy, iteration_number=1, force_snapshot=False)

        assert result is None
        runner.state_manager.save_portfolio_snapshot.assert_not_called()

    @pytest.mark.asyncio
    async def test_force_snapshot_bypasses_throttle(self):
        runner = _make_runner(
            last_snapshot_time=datetime.now(UTC) - timedelta(seconds=5),
            snapshot_interval_seconds=300,
        )
        runner._portfolio_valuer.value.return_value = _make_snapshot()
        strategy = _make_strategy()

        result = await capture_portfolio_snapshot(runner, strategy, iteration_number=1, force_snapshot=True)

        assert result is not None
        assert result.total_value_usd == Decimal("1000.50")
        runner.state_manager.save_portfolio_snapshot.assert_awaited()


# =============================================================================
# 2. Primary path -- PortfolioValuer
# =============================================================================


class TestPrimaryValuerPath:
    @pytest.mark.asyncio
    async def test_happy_path_uses_valuer_and_persists(self):
        runner = _make_runner()
        expected = _make_snapshot(total_value_usd="4242.00")
        runner._portfolio_valuer.value.return_value = expected
        strategy = _make_strategy()

        result = await capture_portfolio_snapshot(runner, strategy, iteration_number=7)

        assert result is expected
        runner._portfolio_valuer.value.assert_called_once()
        # Atomic co-write path
        runner.state_manager.save_portfolio_snapshot.assert_awaited_once()
        # last_snapshot_time was advanced
        assert runner._last_snapshot_time is not None

    @pytest.mark.asyncio
    async def test_gateway_client_wired_into_valuer_when_available(self):
        runner = _make_runner()
        gw_client = MagicMock()
        runner._get_gateway_client = MagicMock(return_value=gw_client)
        runner._portfolio_valuer.value.return_value = _make_snapshot()
        strategy = _make_strategy()

        await capture_portfolio_snapshot(runner, strategy, iteration_number=1)

        runner._portfolio_valuer.set_gateway_client.assert_called_once_with(gw_client)


# =============================================================================
# 3. Multi-chain skips primary path, falls back
# =============================================================================


class TestMultiChainFallback:
    @pytest.mark.asyncio
    async def test_multi_chain_now_uses_canonical_valuer(self):
        # VIB-5722: the multi-chain gate is lifted — multi-chain strategies now
        # run the canonical (chain-aware) PortfolioValuer instead of falling
        # straight to the strategy's degraded get_portfolio_snapshot (which
        # stamped $0.00 at HIGH after a real multi-chain mint).
        runner = _make_runner(is_multi_chain=True)
        runner._portfolio_valuer.value.return_value = _make_snapshot(total_value_usd="2500.00")
        strategy = _make_strategy()
        fallback_snap = _make_snapshot(total_value_usd="0.00")
        strategy.get_portfolio_snapshot = MagicMock(return_value=fallback_snap)

        result = await capture_portfolio_snapshot(runner, strategy, iteration_number=3)

        assert result is not None
        assert result.total_value_usd == Decimal("2500.00")
        runner._portfolio_valuer.value.assert_called_once()
        strategy.get_portfolio_snapshot.assert_not_called()


# =============================================================================
# 4. Primary path failure -> fallback path
# =============================================================================


class TestFallbackPath:
    @pytest.mark.asyncio
    async def test_valuer_raises_then_fallback_succeeds(self):
        runner = _make_runner()
        runner._portfolio_valuer.value.side_effect = RuntimeError("valuer exploded")
        strategy = _make_strategy()
        fallback_snap = _make_snapshot(total_value_usd="750.00")
        strategy.get_portfolio_snapshot = MagicMock(return_value=fallback_snap)

        result = await capture_portfolio_snapshot(runner, strategy, iteration_number=2)

        assert result is fallback_snap
        assert fallback_snap.iteration_number == 2

    @pytest.mark.asyncio
    async def test_valuer_returns_unavailable_snapshot_triggers_fallback(self):
        runner = _make_runner()
        unavailable = _make_snapshot(confidence=ValueConfidence.UNAVAILABLE)
        runner._portfolio_valuer.value.return_value = unavailable
        strategy = _make_strategy()
        fallback_snap = _make_snapshot(total_value_usd="999.00")
        strategy.get_portfolio_snapshot = MagicMock(return_value=fallback_snap)

        result = await capture_portfolio_snapshot(runner, strategy, iteration_number=5)

        assert result is fallback_snap
        strategy.get_portfolio_snapshot.assert_called_once()


# =============================================================================
# 5. No valuation path available -> UNAVAILABLE contract snapshot
# =============================================================================


class TestNoValuationPath:
    @pytest.mark.asyncio
    async def test_no_primary_no_fallback_persists_unavailable_snapshot(self):
        runner = _make_runner()
        # Strategy has neither tracked_tokens nor get_portfolio_snapshot
        strategy = _make_strategy(supports_valuer=False, supports_fallback=False)

        result = await capture_portfolio_snapshot(runner, strategy, iteration_number=1)

        assert result is not None
        assert result.value_confidence == ValueConfidence.UNAVAILABLE
        assert result.total_value_usd == Decimal("0")
        assert result.error is not None
        # Still persists so the equity curve sees this iteration.
        runner.state_manager.save_portfolio_snapshot.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fallback_returns_unavailable_preserves_confidence(self):
        runner = _make_runner()
        # Skip primary by disabling valuer attributes
        strategy = _make_strategy(supports_valuer=False)
        unavailable = _make_snapshot(confidence=ValueConfidence.UNAVAILABLE)
        strategy.get_portfolio_snapshot = MagicMock(return_value=unavailable)

        result = await capture_portfolio_snapshot(runner, strategy, iteration_number=1)

        assert result is not None
        assert result.value_confidence == ValueConfidence.UNAVAILABLE


# =============================================================================
# 6. Atomic co-write vs separate writes
# =============================================================================


class TestPersistenceShape:
    @pytest.mark.asyncio
    async def test_atomic_cowrite_used_when_supported(self):
        runner = _make_runner()
        runner.state_manager.save_snapshot_and_metrics = AsyncMock(return_value=42)
        runner._portfolio_valuer.value.return_value = _make_snapshot()
        strategy = _make_strategy()

        await capture_portfolio_snapshot(runner, strategy, iteration_number=1)

        runner.state_manager.save_snapshot_and_metrics.assert_awaited_once()
        # Separate save_portfolio_snapshot NOT called when atomic path is used
        runner.state_manager.save_portfolio_snapshot.assert_not_called()

    @pytest.mark.asyncio
    async def test_separate_writes_when_atomic_unavailable(self):
        runner = _make_runner()
        # No save_snapshot_and_metrics attr -> separate writes
        assert not hasattr(runner.state_manager, "save_snapshot_and_metrics") or True
        if hasattr(runner.state_manager, "save_snapshot_and_metrics"):
            del runner.state_manager.save_snapshot_and_metrics
        runner._portfolio_valuer.value.return_value = _make_snapshot()
        strategy = _make_strategy()

        await capture_portfolio_snapshot(runner, strategy, iteration_number=1)

        runner.state_manager.save_portfolio_snapshot.assert_awaited_once()
        runner.state_manager.save_portfolio_metrics.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_valuation_fields_written_into_strategy_state(self):
        runner = _make_runner()
        snap = _make_snapshot(
            total_value_usd="1234.56",
            confidence=ValueConfidence.HIGH,
            snapshot_metadata={"valuation_source": "framework"},
        )
        runner._portfolio_valuer.value.return_value = snap
        strategy = _make_strategy()

        await capture_portfolio_snapshot(runner, strategy, iteration_number=1)

        runner.state_manager.save_state.assert_awaited_once()
        saved_state, _ = runner.state_manager.save_state.call_args[0], runner.state_manager.save_state.call_args.kwargs
        state_arg = saved_state[0]
        assert state_arg.state["total_value_usd"] == "1234.56"
        assert state_arg.state["value_confidence"] == "HIGH"
        assert state_arg.state["valuation_source"] == "framework"


# =============================================================================
# 7. Error propagation
# =============================================================================


class TestErrorPropagation:
    @pytest.mark.asyncio
    async def test_accounting_persistence_error_propagates(self):
        runner = _make_runner()
        runner.state_manager.save_portfolio_snapshot = AsyncMock(
            side_effect=AccountingPersistenceError(
                write_kind="snapshot",
                deployment_id="test-strategy",
                message="DB down",
            )
        )
        runner._portfolio_valuer.value.return_value = _make_snapshot()
        strategy = _make_strategy()

        with pytest.raises(AccountingPersistenceError):
            await capture_portfolio_snapshot(runner, strategy, iteration_number=1)

    @pytest.mark.asyncio
    async def test_metrics_write_failure_does_not_duplicate_snapshot(self):
        """Regression: split-write failure on metrics must not write a 2nd UNAVAILABLE row.

        In the non-atomic path ``save_portfolio_snapshot`` can succeed and
        then ``save_portfolio_metrics`` can raise. The old behaviour let a
        plain ``Exception`` bubble into the outer handler, which wrote a
        duplicate UNAVAILABLE snapshot for the same iteration and misclassified
        a metrics-write failure as a valuation failure. Now the metrics
        exception is wrapped as ``AccountingPersistenceError`` so it propagates
        past the generic fallback and the runner flips to ACCOUNTING_FAILED
        without any duplicate row.
        """
        runner = _make_runner()
        # No save_snapshot_and_metrics attr -> separate writes path
        if hasattr(runner.state_manager, "save_snapshot_and_metrics"):
            del runner.state_manager.save_snapshot_and_metrics
        # get_portfolio_metrics returns None -> _build_metrics_for_snapshot
        # creates a fresh PortfolioMetrics. save_portfolio_metrics then raises.
        runner.state_manager.save_portfolio_metrics = AsyncMock(
            side_effect=RuntimeError("metrics backend down")
        )
        runner._portfolio_valuer.value.return_value = _make_snapshot()
        strategy = _make_strategy()

        with pytest.raises(AccountingPersistenceError) as excinfo:
            await capture_portfolio_snapshot(runner, strategy, iteration_number=1)

        assert excinfo.value.write_kind == "metrics"
        assert excinfo.value.deployment_id == "deploy-1"
        # Snapshot row was written exactly once; no duplicate UNAVAILABLE row.
        runner.state_manager.save_portfolio_snapshot.assert_awaited_once()
        persisted = runner.state_manager.save_portfolio_snapshot.call_args.args[0]
        assert persisted.value_confidence == ValueConfidence.HIGH

    @pytest.mark.asyncio
    async def test_generic_exception_persists_unavailable_snapshot(self):
        runner = _make_runner()
        # Patch _build_metrics_for_snapshot indirectly by making get_portfolio_metrics
        # raise something non-AccountingPersistenceError through the primary path.
        # Simpler: make the valuer raise a non-Exception? Must be Exception subclass
        # because the inner try/except catches all Exception. The real outer try
        # is triggered by an exception raised AFTER snapshot construction, e.g.
        # save_state failing inline. Instead, force the fallback strategy to raise.
        runner._portfolio_valuer.value.side_effect = RuntimeError("valuer error")
        strategy = _make_strategy()

        def raising_fallback(market=None):
            raise RuntimeError("fallback also exploded")

        strategy.get_portfolio_snapshot = raising_fallback

        result = await capture_portfolio_snapshot(runner, strategy, iteration_number=9)

        # Generic exception path returns None but persists UNAVAILABLE snapshot.
        assert result is None
        # Unavailable snapshot persisted as the error-fallback.
        runner.state_manager.save_portfolio_snapshot.assert_awaited_once()
        (persisted,), _ = (
            runner.state_manager.save_portfolio_snapshot.call_args.args,
            runner.state_manager.save_portfolio_snapshot.call_args.kwargs,
        )
        assert persisted.value_confidence == ValueConfidence.UNAVAILABLE
        assert persisted.iteration_number == 9
        assert runner._last_snapshot_time is not None
