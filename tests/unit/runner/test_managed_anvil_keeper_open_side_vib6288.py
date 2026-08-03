"""The production lane drives the managed-Anvil keeper — and only on Anvil (VIB-6288).

`_single_chain_async_settlement_guard` fused two jobs behind one flag: it *ran*
the managed-Anvil keeper, and it *blocked* the iteration until settlement was
terminal. `_require_terminal_async_settlement` is set only by
`almanak strat test`, so `strat run --network anvil` never ran the keeper — a
`PERP_OPEN` sat pending until it was cancelled, and every Anvil GMX checkpoint
returned green against a wallet that never held a position.

`_fill_async_orders_on_managed_anvil` separates the two. These tests pin both
halves of that separation, because getting either wrong is a production
regression rather than a missing feature:

* the keeper runs on Anvil when the barrier is NOT required (the fix), and
* nothing about it can fail or delay an iteration, and
* mainnet is a strict no-op (the blast radius), and
* `strat test` still takes the blocking path unchanged (no lane crossover).

The mainnet no-op test is the one that matters most. It is the negative control
for a change whose entire safety argument is "this only happens on a fork."
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.async_settlement import (
    AsyncSettlementBarrierResult,
    AsyncSettlementStatus,
)


def _order(protocol: str = "gmx_v2", order_id: str = "0xorder") -> SimpleNamespace:
    return SimpleNamespace(protocol=protocol, order_id=order_id, kind="INCREASE")


def _state(
    *,
    network: str,
    orders: tuple = (_order(),),
    gateway_client: object | None = None,
    settlement_mode: str = "auto",
) -> SimpleNamespace:
    execution_result = SimpleNamespace(async_orders=orders, error=None)
    return SimpleNamespace(
        strategy=SimpleNamespace(_gateway_network=network, chain="arbitrum", wallet_address="0xwallet"),
        last_execution_result=execution_result,
        last_execution_context=SimpleNamespace(chain="arbitrum"),
        gateway_client=gateway_client if gateway_client is not None else MagicMock(),
        intent=SimpleNamespace(settlement_mode=settlement_mode),
        deployment_id="deployment:abc123",
        record_metrics=False,
        start_time=0.0,
    )


@pytest.fixture
def runner() -> object:
    """A runner stub exposing only the surface the method under test touches."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    stub = SimpleNamespace(
        _require_terminal_async_settlement=False,
        config=SimpleNamespace(async_settlement_timeout_seconds=360, async_settlement_poll_interval_seconds=2),
        _get_gateway_client=lambda: MagicMock(),
        _append_settlement_receipts=MagicMock(),
        _single_chain_enrich_execution_result=MagicMock(),
        _calculate_duration_ms=lambda _start: 0,
        _record_failure=MagicMock(),
    )
    # Bind the real methods onto the stub so the test exercises shipped code,
    # not a re-implementation of it.
    stub._fill_async_orders_on_managed_anvil = (
        StrategyRunner._fill_async_orders_on_managed_anvil.__get__(stub)  # type: ignore[attr-defined]
    )
    stub._single_chain_async_settlement_guard = (
        StrategyRunner._single_chain_async_settlement_guard.__get__(stub)  # type: ignore[attr-defined]
    )
    return stub


def _settled(receipts: tuple = ({"tx_hash": "0xfill"},)) -> AsyncSettlementBarrierResult:
    return AsyncSettlementBarrierResult(
        status=AsyncSettlementStatus.SETTLED,
        terminal=True,
        attempts=1,
        elapsed_seconds=1.0,
        receipts=receipts,
    )


class TestMainnetIsAStrictNoOp:
    """The safety argument for this change is 'fork only'. Prove it."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("network", ["mainnet", "MAINNET", "", "arbitrum"])
    async def test_the_keeper_is_never_invoked_off_anvil(self, runner, monkeypatch, network) -> None:
        called = AsyncMock(return_value=_settled())
        monkeypatch.setattr("almanak.framework.runner.async_settlement.await_async_settlement", called)

        await runner._fill_async_orders_on_managed_anvil(_state(network=network))

        assert called.await_count == 0, f"managed-Anvil keeper ran on network={network!r}"

    @pytest.mark.asyncio
    async def test_the_production_guard_on_mainnet_touches_nothing(self, runner, monkeypatch) -> None:
        """Whole-guard control: `strat run` on mainnet must behave exactly as before."""
        called = AsyncMock(return_value=_settled())
        monkeypatch.setattr("almanak.framework.runner.async_settlement.await_async_settlement", called)
        state = _state(network="mainnet")

        assert await runner._single_chain_async_settlement_guard(state) is None
        assert called.await_count == 0
        runner._append_settlement_receipts.assert_not_called()
        runner._single_chain_enrich_execution_result.assert_not_called()


class TestAnvilProductionLaneFills:
    @pytest.mark.asyncio
    async def test_submission_mode_leaves_order_pending_for_strategy_authored_cancel(self, runner, monkeypatch) -> None:
        called = AsyncMock(return_value=_settled())
        monkeypatch.setattr("almanak.framework.runner.async_settlement.await_async_settlement", called)

        assert (
            await runner._single_chain_async_settlement_guard(_state(network="anvil", settlement_mode="submission"))
            is None
        )
        assert called.await_count == 0

    @pytest.mark.asyncio
    async def test_the_keeper_runs_from_the_production_guard_on_anvil(self, runner, monkeypatch) -> None:
        """The defect itself: this awaited zero times before VIB-6288."""
        called = AsyncMock(return_value=_settled())
        monkeypatch.setattr("almanak.framework.runner.async_settlement.await_async_settlement", called)
        state = _state(network="anvil")

        assert runner._require_terminal_async_settlement is False
        assert await runner._single_chain_async_settlement_guard(state) is None
        assert called.await_count == 1

    @pytest.mark.asyncio
    async def test_a_fill_is_adopted_into_the_execution_result(self, runner, monkeypatch) -> None:
        """A keeper that fills but whose receipts are dropped is an inert fix."""
        receipts = ({"tx_hash": "0xfill"},)
        monkeypatch.setattr(
            "almanak.framework.runner.async_settlement.await_async_settlement",
            AsyncMock(return_value=_settled(receipts)),
        )
        state = _state(network="anvil")

        await runner._fill_async_orders_on_managed_anvil(state)

        runner._append_settlement_receipts.assert_called_once_with(state.last_execution_result, receipts)
        # The kwargs matter, and a bare assert_called_once() does not check them:
        # drop `additional_receipts=` from the call site and the keeper receipt
        # never reaches the parser, the fill goes unrecorded, and a bare
        # called-once assertion stays green — the exact inert fix this test names.
        runner._single_chain_enrich_execution_result.assert_called_once_with(state, additional_receipts=receipts)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("declared", ["anvil", "ANVIL", "Anvil"])
    async def test_the_measured_network_is_forwarded_not_laundered(self, runner, monkeypatch, declared) -> None:
        """The strategy's own value must reach `await_async_settlement` verbatim.

        `await_async_settlement` selects its local-execution branch on this
        argument, so passing the literal `"anvil"` would make that branch
        tautologically true from this call site — a dead guard that re-tests a
        constant we just supplied, and one that would launder a non-Anvil
        network into an Anvil-shaped argument if the caller-side check above
        were ever weakened. Asserting the literal would pin exactly that bug,
        so this asserts the value came from the strategy.
        """
        called = AsyncMock(return_value=_settled())
        monkeypatch.setattr("almanak.framework.runner.async_settlement.await_async_settlement", called)

        await runner._fill_async_orders_on_managed_anvil(_state(network=declared))

        assert called.await_args.kwargs["network"] == declared, (
            "the network was not forwarded from the strategy — a hardcoded literal here "
            "silently disarms await_async_settlement's own anvil branch"
        )


class TestAccountingStillFailsClosed:
    """The one exception this lane must NOT swallow.

    ``CriticalAccountingError`` is how ``run_iteration`` learns to return
    ``ACCOUNTING_FAILED`` — consecutive-error tracking, operator alerting and
    ``finalize_run_loop`` cleanup all hang off it. The keeper's broad
    ``except Exception`` wraps *only* ``await_async_settlement``;
    ``_single_chain_enrich_execution_result`` is deliberately outside it.

    That placement is load-bearing and invisible: widening the ``try`` by two
    lines to "also cover enrichment" reads like defensive tidying, keeps every
    other test in this file green, and silently converts a halt-the-strategy
    accounting failure into a logged warning. Nothing else here would notice,
    which is precisely why this test exists.
    """

    @pytest.mark.asyncio
    async def test_a_critical_accounting_error_from_enrichment_propagates(self, runner, monkeypatch) -> None:
        from almanak.framework.execution.extract_result import CriticalAccountingError

        monkeypatch.setattr(
            "almanak.framework.runner.async_settlement.await_async_settlement",
            AsyncMock(return_value=_settled()),
        )
        runner._single_chain_enrich_execution_result = MagicMock(
            side_effect=CriticalAccountingError("keeper receipt rejected by the parser")
        )

        with pytest.raises(CriticalAccountingError):
            await runner._fill_async_orders_on_managed_anvil(_state(network="anvil"))

    @pytest.mark.asyncio
    async def test_it_propagates_through_the_production_guard_too(self, runner, monkeypatch) -> None:
        """The guard is the real entry point, and it adds no handler of its own."""
        from almanak.framework.execution.extract_result import CriticalAccountingError

        monkeypatch.setattr(
            "almanak.framework.runner.async_settlement.await_async_settlement",
            AsyncMock(return_value=_settled()),
        )
        runner._single_chain_enrich_execution_result = MagicMock(
            side_effect=CriticalAccountingError("keeper receipt rejected by the parser")
        )

        with pytest.raises(CriticalAccountingError):
            await runner._single_chain_async_settlement_guard(_state(network="anvil"))


class TestNothingHereCanFailAnIteration:
    """Production defers on async settlement; it must not start blocking on it."""

    @pytest.mark.asyncio
    async def test_a_raising_keeper_does_not_propagate(self, runner, monkeypatch) -> None:
        monkeypatch.setattr(
            "almanak.framework.runner.async_settlement.await_async_settlement",
            AsyncMock(side_effect=RuntimeError("cold fork RPC deadline exceeded")),
        )
        state = _state(network="anvil")

        assert await runner._single_chain_async_settlement_guard(state) is None
        assert state.last_execution_result.error is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status",
        [
            AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED,
            AsyncSettlementStatus.OBSERVATION_FAILED,
        ],
    )
    async def test_an_unfilled_order_does_not_fail_the_iteration(self, runner, monkeypatch, status) -> None:
        """The barrier returns ASYNC_SETTLEMENT_FAILED here. This path must not."""
        monkeypatch.setattr(
            "almanak.framework.runner.async_settlement.await_async_settlement",
            AsyncMock(
                return_value=AsyncSettlementBarrierResult(
                    status=status, terminal=False, attempts=1, elapsed_seconds=1.0, reason="no keeper"
                )
            ),
        )
        state = _state(network="anvil")

        assert await runner._single_chain_async_settlement_guard(state) is None
        assert state.last_execution_result.error is None
        runner._record_failure.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_missing_gateway_client_is_skipped_not_raised(self, runner, monkeypatch) -> None:
        monkeypatch.setattr(
            "almanak.framework.runner.async_settlement.await_async_settlement",
            AsyncMock(return_value=_settled()),
        )
        state = _state(network="anvil", gateway_client=None)
        state.gateway_client = None
        runner._get_gateway_client = lambda: None

        assert await runner._fill_async_orders_on_managed_anvil(state) is None

    @pytest.mark.asyncio
    async def test_no_async_orders_is_a_no_op(self, runner, monkeypatch) -> None:
        called = AsyncMock(return_value=_settled())
        monkeypatch.setattr("almanak.framework.runner.async_settlement.await_async_settlement", called)

        await runner._fill_async_orders_on_managed_anvil(_state(network="anvil", orders=()))

        assert called.await_count == 0


class TestStratTestLaneIsUnchanged:
    @pytest.mark.asyncio
    async def test_the_blocking_barrier_still_owns_the_terminal_lane(self, runner, monkeypatch) -> None:
        """With the flag set, the fill helper must not run — the barrier does the work."""
        called = AsyncMock(return_value=_settled())
        monkeypatch.setattr("almanak.framework.runner.async_settlement.await_async_settlement", called)
        runner._require_terminal_async_settlement = True
        fill = AsyncMock()
        runner._fill_async_orders_on_managed_anvil = fill
        state = _state(network="anvil")

        assert await runner._single_chain_async_settlement_guard(state) is None
        assert fill.await_count == 0, "the terminal lane must not double-run the keeper"
        assert called.await_count == 1
