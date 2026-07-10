"""VIB-5670 Stage 2 — runner-driven same-chain multi-chain lane.

Covers ``StrategyRunner._execute_same_chain_legs`` (and its wiring from
``_execute_multi_chain``): the replacement for the fire-and-forget
``execute_sequence`` call that produced on-chain positions with ZERO
accounting rows (the wallet-drain bug).

Assertions map to the VIB-5670 design v2/v4 test matrix:

- every successful leg is persisted through ``_persist_executed_leg`` with the
  LEG's chain/wallet, ``run_recon=True``, ``record_metrics=False``, and the
  pre-broadcast snapshot captured on the leg's own chain-scoped provider;
- a live-mode ``AccountingPersistenceError`` fail-stops the loop BEFORE the
  next leg broadcasts and still invalidates balance caches (finally);
- a leg execution failure notifies the strategy with ``success=False`` and
  fails the iteration; earlier legs keep their persistence;
- ``amount='all'`` chaining mirrors ``execute_sequence`` (resolved from the
  prior leg's ``actual_amount_received``; first-leg use raises
  ``InvalidAmountError``; mid-sequence use with no prior amount fails) plus the
  bridge lane's VIB-5346 non-fungible LP_CLOSE carve-out;
- the success ``IterationResult`` carries a summary-only aggregate
  ``ExecutionResult`` (all leg tx results, summed gas) and ``_record_success``
  fires exactly once with ``execution_proved=True``;
- ``_execute_multi_chain`` routes same-chain flows to the new lane and no
  longer calls ``execute_sequence``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.multichain import MultiChainOrchestrator
from almanak.framework.execution.orchestrator import (
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
)
from almanak.framework.intents.intent_errors import InvalidAmountError
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.runner.runner_models import IterationStatus
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner
from almanak.framework.state.exceptions import AccountingPersistenceError

# =============================================================================
# Helpers
# =============================================================================


class _IntentStub:
    """Plain intent stand-in the ``Intent`` facade stub below understands."""

    def __init__(
        self,
        *,
        chain: str | None = "arbitrum",
        chained: bool = False,
        amount_field: Decimal | None = None,
        intent_type: object | None = None,
        protocol: str | None = "uniswap_v3",
        intent_id: str = "intent-0000000000",
    ) -> None:
        self.chain = chain
        self._chained = chained
        self._amount_field = amount_field
        self.intent_type = intent_type
        self.protocol = protocol
        self.intent_id = intent_id


class _IntentFacadeStub:
    """Deterministic stand-in for the ``Intent`` static facade.

    The real facade inspects frozen pydantic intents; these tests drive
    ``_IntentStub`` objects instead, so the facade is patched at the
    ``strategy_runner`` module level with this class.
    """

    resolved_calls: list[tuple[object, Decimal]] = []

    @staticmethod
    def has_chained_amount(intent) -> bool:
        return bool(getattr(intent, "_chained", False))

    @staticmethod
    def get_chain(intent):
        return getattr(intent, "chain", None)

    @staticmethod
    def get_amount_field(intent):
        return getattr(intent, "_amount_field", None)

    @classmethod
    def set_resolved_amount(cls, intent, amount):
        cls.resolved_calls.append((intent, amount))
        resolved = _IntentStub(
            chain=intent.chain,
            intent_type=intent.intent_type,
            protocol=intent.protocol,
            intent_id=intent.intent_id,
        )
        resolved.resolved_amount = amount
        return resolved


def _make_runner() -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=0,
        enable_state_persistence=False,
        enable_alerting=False,
    )
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=AsyncMock(),
        alert_manager=MagicMock(),
        config=config,
    )
    return runner


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "dep-1"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0xstrategywallet"
    return strategy


def _make_leg(*, chain: str = "arbitrum", success: bool = True, error: str | None = None, received=None) -> MagicMock:
    """Build an ``IntentExecutionResult``-shaped leg."""
    leg = MagicMock()
    leg.chain = chain
    leg.success = success
    leg.error = error
    if received is None:
        # No chaining info: tx_result without actual_amount_received.
        leg.tx_result = MagicMock(spec=[])
    else:
        leg.tx_result = MagicMock()
        leg.tx_result.actual_amount_received = received
    return leg


def _leg_execution_result(gas_used: int = 10, gas_cost_wei: int = 1000) -> tuple[ExecutionResult, list[TransactionResult]]:
    tr = TransactionResult(tx_hash="0xabc", success=True, gas_used=gas_used, gas_cost_wei=gas_cost_wei)
    er = ExecutionResult(
        success=True,
        phase=ExecutionPhase.COMPLETE,
        transaction_results=[tr],
        total_gas_used=gas_used,
        total_gas_cost_wei=gas_cost_wei,
    )
    return er, [tr]


def _wire_happy_path(runner: StrategyRunner, legs: list[MagicMock]) -> MagicMock:
    """Wire the runner's collaborators for a per-leg loop run; returns the orchestrator."""
    orchestrator = MagicMock()
    orchestrator.primary_chain = "arbitrum"
    orchestrator.execute = AsyncMock(side_effect=legs)

    runner._persist_executed_leg = AsyncMock(return_value="")  # type: ignore[method-assign]
    runner._adapt_leg_to_execution_result = MagicMock(  # type: ignore[method-assign]
        side_effect=[_leg_execution_result(gas_used=10 * (i + 1), gas_cost_wei=1000 * (i + 1)) for i in range(len(legs))]
    )
    runner._multichain_wallet_for = MagicMock(return_value="0xlegwallet")  # type: ignore[method-assign]
    runner._balance_provider_for_chain = MagicMock(return_value=MagicMock(name="leg_provider"))  # type: ignore[method-assign]
    runner._snapshot_balances_for_intent = AsyncMock(return_value={"USDC": Decimal("5")})  # type: ignore[method-assign]
    runner._notify_intent_executed = MagicMock()  # type: ignore[method-assign]
    runner._record_success = MagicMock()  # type: ignore[method-assign]
    runner._record_failure = MagicMock()  # type: ignore[method-assign]
    return orchestrator


async def _run_lane(runner, orchestrator, intents, strategy=None):
    strategy = strategy or _make_strategy()
    _IntentFacadeStub.resolved_calls = []
    with patch("almanak.framework.runner.strategy_runner.Intent", _IntentFacadeStub):
        return await runner._execute_same_chain_legs(
            strategy=strategy,
            intents=intents,
            orchestrator=orchestrator,
            start_time=datetime.now(UTC),
            price_map={"USDC": "1"},
            price_oracle={"USDC": Decimal("1")},
        )


# =============================================================================
# Success path — per-leg persistence + aggregate
# =============================================================================


class TestSameChainLegsSuccess:
    @pytest.mark.asyncio
    async def test_each_leg_persisted_with_leg_chain_wallet_and_pre_snapshot(self):
        runner = _make_runner()
        intents = [
            _IntentStub(chain="arbitrum", intent_id="intent-aaaaaaaaaa"),
            _IntentStub(chain="hyperevm", intent_id="intent-bbbbbbbbbb"),
        ]
        legs = [_make_leg(chain="arbitrum"), _make_leg(chain="hyperevm")]
        orchestrator = _wire_happy_path(runner, legs)

        result = await _run_lane(runner, orchestrator, intents)

        assert result.status == IterationStatus.SUCCESS
        assert runner._persist_executed_leg.await_count == 2

        first = runner._persist_executed_leg.await_args_list[0].kwargs
        second = runner._persist_executed_leg.await_args_list[1].kwargs
        assert first["chain"] == "arbitrum"
        assert second["chain"] == "hyperevm"
        for call in (first, second):
            assert call["wallet_address"] == "0xlegwallet"
            assert call["run_recon"] is True
            assert call["record_metrics"] is False
            assert call["pre_snapshot"] == {"USDC": Decimal("5")}
        # Execution context carries the LEG chain, not the primary chain.
        assert first["execution_context"].chain == "arbitrum"
        assert second["execution_context"].chain == "hyperevm"
        # Per-leg provider requested for each leg's own chain.
        assert [c.args[0] for c in runner._balance_provider_for_chain.call_args_list] == ["arbitrum", "hyperevm"]
        # Market prices thread into every per-leg broadcast (parity with the
        # execute_sequence call this lane replaced).
        for call in orchestrator.execute.await_args_list:
            assert call.kwargs["price_map"] == {"USDC": "1"}
            assert call.kwargs["price_oracle"] == {"USDC": Decimal("1")}

    @pytest.mark.asyncio
    async def test_pre_snapshot_reads_via_chain_scoped_provider(self):
        runner = _make_runner()
        intents = [_IntentStub(chain="hyperevm")]
        orchestrator = _wire_happy_path(runner, [_make_leg(chain="hyperevm")])
        leg_provider = MagicMock(name="hyperevm_provider")
        runner._balance_provider_for_chain = MagicMock(return_value=leg_provider)  # type: ignore[method-assign]

        await _run_lane(runner, orchestrator, intents)

        snap_kwargs = runner._snapshot_balances_for_intent.await_args.kwargs
        assert snap_kwargs["balance_provider"] is leg_provider

    @pytest.mark.asyncio
    async def test_no_leg_provider_skips_snapshot_and_persists_unmeasured(self):
        """Local mode, no gateway, non-primary leg: recon degrades (Empty != Zero)."""
        runner = _make_runner()
        intents = [_IntentStub(chain="hyperevm")]
        orchestrator = _wire_happy_path(runner, [_make_leg(chain="hyperevm")])
        runner._balance_provider_for_chain = MagicMock(return_value=None)  # type: ignore[method-assign]

        result = await _run_lane(runner, orchestrator, intents)

        assert result.status == IterationStatus.SUCCESS
        runner._snapshot_balances_for_intent.assert_not_awaited()
        assert runner._persist_executed_leg.await_args.kwargs["pre_snapshot"] is None

    @pytest.mark.asyncio
    async def test_aggregate_is_summary_only_and_record_success_once(self):
        runner = _make_runner()
        intents = [_IntentStub(chain="arbitrum"), _IntentStub(chain="arbitrum")]
        orchestrator = _wire_happy_path(runner, [_make_leg(), _make_leg()])

        result = await _run_lane(runner, orchestrator, intents)

        aggregate = result.execution_result
        assert aggregate is not None
        assert aggregate.success is True
        # All leg tx results, summed gas (10+20 / 1000+2000 from _wire_happy_path).
        assert len(aggregate.transaction_results) == 2
        assert aggregate.total_gas_used == 30
        assert aggregate.total_gas_cost_wei == 3000
        # Summary only — no enriched financial fields on the aggregate.
        assert aggregate.position_id is None
        runner._record_success.assert_called_once_with(execution_proved=True)
        runner._record_failure.assert_not_called()
        # Iteration-level notify is owned by _persist_executed_leg; the lane
        # itself must not double-fire it on success.
        runner._notify_intent_executed.assert_not_called()

    @pytest.mark.asyncio
    async def test_balance_caches_invalidated_on_success(self):
        runner = _make_runner()
        cached_leg_provider = MagicMock()
        runner._leg_balance_providers["hyperevm"] = cached_leg_provider
        intents = [_IntentStub(chain="arbitrum")]
        orchestrator = _wire_happy_path(runner, [_make_leg()])

        await _run_lane(runner, orchestrator, intents)

        runner.balance_provider.invalidate_cache.assert_called_once()
        cached_leg_provider.invalidate_cache.assert_called_once()


# =============================================================================
# Fail-stop + failure semantics
# =============================================================================


class TestSameChainLegsFailStop:
    @pytest.mark.asyncio
    async def test_accounting_error_stops_before_next_leg_broadcasts(self):
        runner = _make_runner()
        intents = [_IntentStub(chain="arbitrum"), _IntentStub(chain="arbitrum")]
        orchestrator = _wire_happy_path(runner, [_make_leg(), _make_leg()])
        runner._persist_executed_leg = AsyncMock(  # type: ignore[method-assign]
            side_effect=AccountingPersistenceError("ledger", deployment_id="dep-1")
        )

        with pytest.raises(AccountingPersistenceError):
            await _run_lane(runner, orchestrator, intents)

        # Leg 2 never broadcast.
        assert orchestrator.execute.await_count == 1
        # Cache invalidation still ran (finally).
        runner.balance_provider.invalidate_cache.assert_called_once()
        runner._record_success.assert_not_called()

    @pytest.mark.asyncio
    async def test_leg_execution_failure_notifies_false_and_fails_iteration(self):
        runner = _make_runner()
        intents = [_IntentStub(chain="arbitrum"), _IntentStub(chain="arbitrum")]
        legs = [_make_leg(), _make_leg(success=False, error="reverted")]
        orchestrator = _wire_happy_path(runner, legs)

        result = await _run_lane(runner, orchestrator, intents)

        assert result.status == IterationStatus.EXECUTION_FAILED
        assert result.error == "[arbitrum] reverted"
        # Leg 1 persisted; failed leg 2 did not persist.
        assert runner._persist_executed_leg.await_count == 1
        # Failure notify fired with success=False and no result.
        args = runner._notify_intent_executed.call_args.args
        assert args[2] is False
        assert args[3] is None
        runner._record_failure.assert_called_once()
        runner._record_success.assert_not_called()

    @pytest.mark.asyncio
    async def test_failed_iteration_still_invalidates_caches(self):
        runner = _make_runner()
        intents = [_IntentStub(chain="arbitrum")]
        orchestrator = _wire_happy_path(runner, [_make_leg(success=False, error="boom")])

        await _run_lane(runner, orchestrator, intents)

        runner.balance_provider.invalidate_cache.assert_called_once()


# =============================================================================
# Audit fixes (Codex P1/P2)
# =============================================================================


class TestSameChainLegsAuditHardening:
    @pytest.mark.asyncio
    async def test_persist_failure_notifies_framework_truth_before_reraise(self):
        """Codex P1: a confirmed-broadcast leg whose persist fails must still
        notify with framework_success=True + save strategy state before the
        fail-closed halt — otherwise the next iteration re-opens the position
        it already holds (duplicate broadcast)."""
        runner = _make_runner()
        intents = [_IntentStub(chain="arbitrum")]
        orchestrator = _wire_happy_path(runner, [_make_leg()])
        runner._persist_executed_leg = AsyncMock(  # type: ignore[method-assign]
            side_effect=AccountingPersistenceError("ledger", deployment_id="dep-1")
        )
        strategy = _make_strategy()

        with pytest.raises(AccountingPersistenceError):
            await _run_lane(runner, orchestrator, intents, strategy=strategy)

        runner._notify_intent_executed.assert_called_once()
        n_args = runner._notify_intent_executed.call_args
        assert n_args.args[2] is False  # user verdict: the pipeline failed
        assert n_args.kwargs["framework_success"] is True  # chain truth
        strategy.save_state.assert_called_once()

    @pytest.mark.asyncio
    async def test_downgraded_leg_fail_stops_remaining_legs(self):
        """Codex P2 / design v3 #3: an enforced recon incident (or slippage
        breach) downgrades the persisted leg and halts the sequence — leg 2
        never broadcasts on a failed verdict."""
        runner = _make_runner()
        intents = [_IntentStub(chain="arbitrum"), _IntentStub(chain="arbitrum")]
        orchestrator = _wire_happy_path(runner, [_make_leg(), _make_leg()])
        runner._persist_executed_leg = AsyncMock(  # type: ignore[method-assign]
            return_value="Reconciliation incident (enforced): delta out of range"
        )

        result = await _run_lane(runner, orchestrator, intents)

        assert result.status == IterationStatus.EXECUTION_FAILED
        assert "Reconciliation incident (enforced)" in (result.error or "")
        # Leg 1 was broadcast + persisted; leg 2 never broadcast.
        assert orchestrator.execute.await_count == 1
        assert runner._persist_executed_leg.await_count == 1
        runner._record_failure.assert_called_once()
        runner._record_success.assert_not_called()


# =============================================================================
# amount='all' chaining
# =============================================================================


class TestSameChainLegsAmountChaining:
    @pytest.mark.asyncio
    async def test_amount_all_resolved_from_previous_leg_received(self):
        runner = _make_runner()
        intents = [
            _IntentStub(chain="arbitrum", intent_id="intent-aaaaaaaaaa"),
            _IntentStub(chain="arbitrum", chained=True, intent_id="intent-bbbbbbbbbb"),
        ]
        legs = [_make_leg(received=Decimal("7.5")), _make_leg()]
        orchestrator = _wire_happy_path(runner, legs)

        result = await _run_lane(runner, orchestrator, intents)

        assert result.status == IterationStatus.SUCCESS
        assert _IntentFacadeStub.resolved_calls == [(intents[1], Decimal("7.5"))]
        # The SECOND broadcast used the resolved intent, not the raw one.
        second_executed = orchestrator.execute.await_args_list[1].args[0]
        assert getattr(second_executed, "resolved_amount", None) == Decimal("7.5")

    @pytest.mark.asyncio
    async def test_amount_all_falls_back_to_intent_amount_field(self):
        """Mirrors execute_sequence: no actual_amount_received -> intent's own amount."""
        runner = _make_runner()
        intents = [
            _IntentStub(chain="arbitrum", amount_field=Decimal("3")),
            _IntentStub(chain="arbitrum", chained=True),
        ]
        legs = [_make_leg(received=None), _make_leg()]
        orchestrator = _wire_happy_path(runner, legs)

        result = await _run_lane(runner, orchestrator, intents)

        assert result.status == IterationStatus.SUCCESS
        assert _IntentFacadeStub.resolved_calls == [(intents[1], Decimal("3"))]

    @pytest.mark.asyncio
    async def test_amount_all_on_first_leg_raises_before_any_broadcast(self):
        runner = _make_runner()
        intents = [_IntentStub(chain="arbitrum", chained=True, intent_type=IntentType.SWAP)]
        orchestrator = _wire_happy_path(runner, [])

        with pytest.raises(InvalidAmountError):
            await _run_lane(runner, orchestrator, intents)

        orchestrator.execute.assert_not_awaited()
        runner._persist_executed_leg.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_amount_all_mid_sequence_without_previous_amount_fails_closed(self):
        runner = _make_runner()
        intents = [
            _IntentStub(chain="arbitrum"),
            _IntentStub(chain="arbitrum", chained=True),
        ]
        # Leg 1 yields NO chaining info at all.
        legs = [_make_leg(received=None)]
        orchestrator = _wire_happy_path(runner, legs)

        result = await _run_lane(runner, orchestrator, intents)

        assert result.status == IterationStatus.EXECUTION_FAILED
        assert "no previous step amount available" in (result.error or "")
        # Leg 2 never broadcast; leg 1 still persisted.
        assert orchestrator.execute.await_count == 1
        assert runner._persist_executed_leg.await_count == 1
        runner._record_failure.assert_called_once()

    @pytest.mark.asyncio
    async def test_nonfungible_lp_close_amount_all_left_unresolved(self):
        """VIB-5346 carve-out: never resolve a swap output into a position identity."""
        runner = _make_runner()
        lp_close = _IntentStub(
            chain="arbitrum",
            chained=True,
            intent_type=IntentType.LP_CLOSE,
            protocol="not_a_fungible_protocol",
            intent_id="intent-cccccccccc",
        )
        intents = [_IntentStub(chain="arbitrum"), lp_close]
        legs = [_make_leg(received=Decimal("9")), _make_leg()]
        orchestrator = _wire_happy_path(runner, legs)

        result = await _run_lane(runner, orchestrator, intents)

        assert result.status == IterationStatus.SUCCESS
        # NOT resolved — the raw intent (marker intact) went to the compiler guard.
        assert _IntentFacadeStub.resolved_calls == []
        assert orchestrator.execute.await_args_list[1].args[0] is lp_close


# =============================================================================
# _execute_multi_chain routing
# =============================================================================


class TestExecuteMultiChainRouting:
    @pytest.mark.asyncio
    async def test_same_chain_flow_routes_to_new_lane_not_execute_sequence(self):
        runner = _make_runner()
        strategy = _make_strategy()
        orchestrator = MagicMock(spec=MultiChainOrchestrator)
        orchestrator.primary_chain = "arbitrum"
        orchestrator.execute_sequence = AsyncMock()
        runner.execution_orchestrator = orchestrator

        sentinel = MagicMock()
        runner._execute_same_chain_legs = AsyncMock(return_value=sentinel)  # type: ignore[method-assign]

        intent = _IntentStub(chain="arbitrum")
        with (
            patch("almanak.framework.runner.strategy_runner.get_intent_destination_chain", return_value=None),
            patch("almanak.framework.runner.strategy_runner.is_cross_chain_intent", return_value=False),
        ):
            result = await runner._execute_multi_chain(
                strategy=strategy,
                intents=[intent],
                start_time=datetime.now(UTC),
                market=None,
            )

        assert result is sentinel
        runner._execute_same_chain_legs.assert_awaited_once()
        orchestrator.execute_sequence.assert_not_awaited()


# =============================================================================
# _leg_amount_received (pure helper)
# =============================================================================


class TestLegAmountReceived:
    def test_prefers_tx_result_actual_amount(self):
        leg = _make_leg(received=Decimal("2"))
        intent = _IntentStub(amount_field=Decimal("9"))
        with patch("almanak.framework.runner.strategy_runner.Intent", _IntentFacadeStub):
            assert StrategyRunner._leg_amount_received(leg, intent) == Decimal("2")

    def test_falls_back_to_amount_field_then_none(self):
        leg = _make_leg(received=None)
        with patch("almanak.framework.runner.strategy_runner.Intent", _IntentFacadeStub):
            assert StrategyRunner._leg_amount_received(leg, _IntentStub(amount_field=Decimal("4"))) == Decimal("4")
            assert StrategyRunner._leg_amount_received(leg, _IntentStub(amount_field=None)) is None
