"""Extended tests for Phase 3c ``_execute_single_chain`` step helpers.

Complements ``test_execute_single_chain_steps.py`` with extra branch
coverage for: ``_init_single_chain_state``, ``_single_chain_state_machine_loop``,
``_single_chain_execute_step`` (non-dry-run paths),
``_single_chain_pre_retry_confirmed`` (retry edge cases),
``_single_chain_slippage_guard``, ``_single_chain_handle_recon_incident``,
``_single_chain_handle_success``, ``_single_chain_handle_failure``,
``_single_chain_execute_clob``, ``_single_chain_execute_onchain``, plus the
static helper ``_build_single_chain_price_oracle``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.extracted_data import AsyncOrderData, AsyncOrderKind, AsyncOrderStatus
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionPhase,
    ExecutionResult,
    TransactionResult,
)
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    RunnerConfig,
    SingleChainExecutionState,
    StrategyRunner,
)

# =============================================================================
# Helpers
# =============================================================================


def _make_runner(
    *,
    dry_run: bool = False,
    state_manager: MagicMock | None = None,
    balance_provider: MagicMock | None = None,
    execution_orchestrator: MagicMock | None = None,
    max_retries: int = 2,
    reconciliation_enforcement: bool = False,
) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=dry_run,
        max_retries=max_retries,
        reconciliation_enforcement=reconciliation_enforcement,
    )
    if state_manager is None:
        state_manager = MagicMock()
    if balance_provider is None:
        balance_provider = MagicMock()
        balance_provider.invalidate_cache = MagicMock()
    if execution_orchestrator is None:
        execution_orchestrator = MagicMock()
        execution_orchestrator.tx_risk_config = None
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=balance_provider,
        execution_orchestrator=execution_orchestrator,
        state_manager=state_manager,
        config=config,
    )


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "test-strategy"
    strategy.chain = "arbitrum"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    return strategy


def _make_state(strategy: MagicMock, *, intent=None) -> SingleChainExecutionState:
    if intent is None:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("10"))
    return SingleChainExecutionState(
        strategy=strategy,
        intent=intent,
        start_time=datetime.now(UTC),
        deployment_id=strategy.deployment_id,
    )


# =============================================================================
# _build_single_chain_price_oracle - extended
# =============================================================================


class TestBuildPriceOracleExtended:
    def test_non_dict_oracle_returns_none(self) -> None:
        """Market returns non-dict from get_price_oracle_dict -> coerced to None path."""
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = MagicMock()
        market.get_price_oracle_dict.return_value = None
        assert StrategyRunner._build_single_chain_price_oracle(market, intent) is None

    def test_prefetch_exception_swallowed_still_returns_populated_oracle(self) -> None:
        """market.price() raising does not prevent returning the base oracle."""
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {"USDC": Decimal("1")}
        market.price.side_effect = RuntimeError("rpc blip")

        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        # ETH wasn't resolved, but USDC was -> the oracle dict is still returned
        assert result == {"USDC": Decimal("1")}

    def test_market_has_get_oracle_but_no_price_method(self) -> None:
        """When market lacks .price(), no pre-fetch happens; oracle returned as-is."""
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        # Real object, not a MagicMock, so hasattr(market, "price") is False
        market = SimpleNamespace(get_price_oracle_dict=lambda: {"USDC": Decimal("1"), "ETH": Decimal("2000")})
        result = StrategyRunner._build_single_chain_price_oracle(market, intent)
        assert result == {"USDC": Decimal("1"), "ETH": Decimal("2000")}


# =============================================================================
# _single_chain_pre_retry_confirmed - extended
# =============================================================================


class TestSingleChainPreRetryConfirmedExtended:
    @pytest.mark.asyncio
    async def test_timeout_with_empty_tx_hashes_returns_false(self) -> None:
        """Timeout occurred but no partial tx_hashes -> short-circuit is not possible."""
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 1
        state.last_execution_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.SUBMISSION,
            transaction_results=[TransactionResult(tx_hash="", success=False, gas_used=0, gas_cost_wei=0)],
            error="timeout waiting for receipt",
        )

        single_chain_orch = MagicMock()
        assert await runner._single_chain_pre_retry_confirmed(state, single_chain_orch) is False

    @pytest.mark.asyncio
    async def test_get_receipt_raises_treated_as_unconfirmed(self) -> None:
        """RPC error fetching receipt -> set all_confirmed=False, do not short-circuit."""
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 1
        state.state_machine.set_receipt = MagicMock()
        state.last_execution_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.SUBMISSION,
            transaction_results=[TransactionResult(tx_hash="0xdead", success=False, gas_used=0, gas_cost_wei=0)],
            error="timeout waiting for receipt",
        )

        single_chain_orch = MagicMock()
        single_chain_orch.submitter = MagicMock()
        single_chain_orch.submitter.get_receipt = AsyncMock(side_effect=RuntimeError("rpc down"))

        assert await runner._single_chain_pre_retry_confirmed(state, single_chain_orch) is False
        state.state_machine.set_receipt.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_last_execution_result_returns_false(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 1
        state.last_execution_result = None  # never executed

        single_chain_orch = MagicMock()
        assert await runner._single_chain_pre_retry_confirmed(state, single_chain_orch) is False

    @pytest.mark.asyncio
    async def test_multi_tx_all_confirmed_sums_gas_correctly(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 1
        state.state_machine.set_receipt = MagicMock()
        state.last_execution_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.SUBMISSION,
            transaction_results=[
                TransactionResult(tx_hash="0xa", success=False, gas_used=0, gas_cost_wei=0),
                TransactionResult(tx_hash="0xb", success=False, gas_used=0, gas_cost_wei=0),
            ],
            error="timeout while submitting",
        )

        receipts = [
            SimpleNamespace(tx_hash="0xa", success=True, gas_used=21000, gas_cost_wei=1000, logs=[]),
            SimpleNamespace(tx_hash="0xb", success=True, gas_used=50000, gas_cost_wei=5000, logs=[]),
        ]
        single_chain_orch = MagicMock()
        single_chain_orch.submitter = MagicMock()
        single_chain_orch.submitter.get_receipt = AsyncMock(side_effect=receipts)

        result = await runner._single_chain_pre_retry_confirmed(state, single_chain_orch)
        assert result is True
        # Synthesised ExecutionResult aggregates gas across both
        assert state.last_execution_result.total_gas_used == 71000
        assert state.last_execution_result.total_gas_cost_wei == 6000
        assert state.last_execution_result.success is True
        state.state_machine.set_receipt.assert_called_once()


# =============================================================================
# _single_chain_slippage_guard - extended
# =============================================================================


class TestSingleChainSlippageGuardExtended:
    @pytest.mark.asyncio
    async def test_slippage_unknown_returns_none(self) -> None:
        """slippage_bps=None means extraction failed -> don't block (belt-and-suspenders)."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.05"),
        )
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(
            slippage_bps=None,
            token_in="USDC",
            token_out="ETH",
        )
        assert await runner._single_chain_slippage_guard(state) is None

    @pytest.mark.asyncio
    async def test_max_slippage_zero_skips_guard(self) -> None:
        """When intent.max_slippage is 0, the guard must not fire even if slippage_bps is high."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0"),
        )
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(slippage_bps=500, token_in="USDC", token_out="ETH")
        assert await runner._single_chain_slippage_guard(state) is None

    @pytest.mark.asyncio
    async def test_tx_risk_config_overrides_intent_max_slippage(self) -> None:
        """When orchestrator.tx_risk_config is set, its max_slippage_bps wins over intent."""
        exec_orch = MagicMock()
        exec_orch.tx_risk_config = SimpleNamespace(max_slippage_bps=50)  # tight cap
        runner = _make_runner(execution_orchestrator=exec_orch)
        runner._write_ledger_entry = AsyncMock()
        runner._emit_execution_timeline_event = MagicMock()

        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.5"),  # 5000 bps on intent
        )
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(slippage_bps=100, token_in="USDC", token_out="ETH")

        result = await runner._single_chain_slippage_guard(state)
        # 100 > 50 (tx_risk_config wins) -> should fire
        assert result is not None
        assert result.status == IterationStatus.EXECUTION_FAILED

    @pytest.mark.asyncio
    async def test_slippage_guard_attaches_error_to_execution_result(self) -> None:
        runner = _make_runner()
        runner._write_ledger_entry = AsyncMock()
        runner._emit_execution_timeline_event = MagicMock()
        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
        )
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(slippage_bps=300, token_in="USDC", token_out="ETH")

        result = await runner._single_chain_slippage_guard(state)
        assert result is not None
        # The error message was written back onto the ExecutionResult
        assert state.last_execution_result.error is not None
        assert "Slippage circuit breaker" in state.last_execution_result.error

    @pytest.mark.asyncio
    async def test_slippage_callback_exception_does_not_abort(self) -> None:
        """on_intent_executed raising must not prevent returning the EXECUTION_FAILED result."""
        runner = _make_runner()
        runner._write_ledger_entry = AsyncMock()
        runner._emit_execution_timeline_event = MagicMock()
        strategy = _make_strategy()
        strategy.on_intent_executed.side_effect = RuntimeError("strat bug")
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
        )
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(slippage_bps=400, token_in="USDC", token_out="ETH")

        result = await runner._single_chain_slippage_guard(state)
        assert result is not None
        assert result.status == IterationStatus.EXECUTION_FAILED


# =============================================================================
# _single_chain_handle_recon_incident
# =============================================================================


class TestSingleChainHandleReconIncident:
    @pytest.mark.asyncio
    async def test_recon_incident_returns_reconciliation_failed(self) -> None:
        runner = _make_runner()
        runner._format_reconciliation_error = MagicMock(return_value="Delta out of range")
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._handle_execution_error = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )

        recon = {"incident": True, "breach_bps": 500}
        result = await runner._single_chain_handle_recon_incident(state, recon)

        assert result.status == IterationStatus.RECONCILIATION_FAILED
        assert result.balance_reconciliation is recon
        # Error written back onto execution result
        assert state.last_execution_result.error == "Delta out of range"
        # Callback fired with success=False
        strategy.on_intent_executed.assert_called_once()
        assert strategy.on_intent_executed.call_args.kwargs.get("success") is False
        # Alert dispatched
        runner._handle_execution_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recon_incident_with_none_execution_result_still_returns(self) -> None:
        runner = _make_runner()
        runner._format_reconciliation_error = MagicMock(return_value="boom")
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._handle_execution_error = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(strategy, intent=intent)
        state.last_execution_result = None

        result = await runner._single_chain_handle_recon_incident(state, {"incident": True})
        assert result.status == IterationStatus.RECONCILIATION_FAILED
        # No attempt to attach error when last_execution_result is None
        runner._handle_execution_error.assert_not_called()


# =============================================================================
# _single_chain_handle_success
# =============================================================================


class TestSingleChainHandleSuccess:
    @pytest.mark.asyncio
    async def test_clean_success_path_returns_success_and_fires_callback(self) -> None:
        runner = _make_runner()
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._reconcile_post_execution_balances = AsyncMock(return_value={"incident": False})
        # ResultEnricher: stub out enrich to a passthrough
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        state = _make_state(strategy, intent=intent)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 0
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_context = ExecutionContext(deployment_id=strategy.deployment_id)

        with patch("almanak.framework.runner.strategy_runner.ResultEnricher") as MockEnricher:
            MockEnricher.return_value.enrich.return_value = state.last_execution_result
            result = await runner._single_chain_handle_success(state)

        assert result.status == IterationStatus.SUCCESS
        strategy.on_intent_executed.assert_called_once()
        assert strategy.on_intent_executed.call_args.kwargs.get("success") is True
        runner._write_ledger_entry.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_submission_cannot_commit_success_before_terminal_settlement(self) -> None:
        """Lifecycle-only barrier blocks all success side effects on unsupported Anvil."""
        from almanak.connectors._strategy_base.runner_hook_registry import AsyncSettlementStatus
        from almanak.framework.runner.async_settlement import AsyncSettlementBarrierResult

        runner = _make_runner()
        runner._require_terminal_async_settlement = True
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._reconcile_post_execution_balances = AsyncMock(return_value={"incident": False})
        strategy = _make_strategy()
        strategy._gateway_network = "anvil"
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        state = _make_state(strategy, intent=intent)
        state.gateway_client = object()
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 0
        state.last_execution_result = ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            completed_at=datetime.now(UTC),
            async_orders=[
                AsyncOrderData(
                    protocol="gmx_v2",
                    order_id="0x" + "ab" * 32,
                    status=AsyncOrderStatus.PENDING,
                    kind=AsyncOrderKind.INCREASE,
                )
            ],
        )
        state.last_execution_context = ExecutionContext(deployment_id=strategy.deployment_id)
        barrier = AsyncSettlementBarrierResult(
            status=AsyncSettlementStatus.INFRASTRUCTURE_UNSUPPORTED,
            terminal=False,
            attempts=0,
            elapsed_seconds=0,
            reason="no managed-fork keeper simulator",
        )

        with (
            patch("almanak.framework.runner.strategy_runner.ResultEnricher") as mock_enricher,
            patch(
                "almanak.framework.runner.async_settlement.await_async_settlement",
                new=AsyncMock(return_value=barrier),
            ),
        ):
            mock_enricher.return_value.enrich.return_value = state.last_execution_result
            result = await runner._single_chain_handle_success(state)

        assert result.status == IterationStatus.ASYNC_SETTLEMENT_FAILED
        assert result.async_settlement is not None
        assert result.async_settlement["status"] == "INFRASTRUCTURE_UNSUPPORTED"
        runner._reconcile_post_execution_balances.assert_not_awaited()
        runner._write_ledger_entry.assert_not_awaited()
        runner._emit_execution_timeline_event.assert_not_called()
        strategy.on_intent_executed.assert_not_called()
        strategy.save_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_enrich_exception_still_proceeds_to_success_path(self) -> None:
        runner = _make_runner()
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._reconcile_post_execution_balances = AsyncMock(return_value={"incident": False})
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        state = _make_state(strategy, intent=intent)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 0
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_context = ExecutionContext(deployment_id=strategy.deployment_id)

        with patch("almanak.framework.runner.strategy_runner.ResultEnricher") as MockEnricher:
            MockEnricher.return_value.enrich.side_effect = RuntimeError("enricher bug")
            result = await runner._single_chain_handle_success(state)

        assert result.status == IterationStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_slippage_guard_tripped_short_circuits_before_success_commit(
        self,
    ) -> None:
        runner = _make_runner()
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._reconcile_post_execution_balances = AsyncMock(return_value={"incident": False})
        strategy = _make_strategy()
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
        )
        state = _make_state(strategy, intent=intent)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 0
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_result.swap_amounts = SimpleNamespace(slippage_bps=500, token_in="USDC", token_out="ETH")
        state.last_execution_context = ExecutionContext(deployment_id=strategy.deployment_id)

        with patch("almanak.framework.runner.strategy_runner.ResultEnricher") as MockEnricher:
            MockEnricher.return_value.enrich.return_value = state.last_execution_result
            result = await runner._single_chain_handle_success(state)

        # Slippage guard returned EXECUTION_FAILED; _single_chain_handle_success
        # propagated that instead of marking success
        assert result.status == IterationStatus.EXECUTION_FAILED
        # reconcile was NOT called because slippage guard returned early
        runner._reconcile_post_execution_balances.assert_not_called()

    @pytest.mark.asyncio
    async def test_recon_incident_observation_mode_still_returns_success(self) -> None:
        """Default observation mode: incident passes through to the SUCCESS path.

        Recon dict still lands on the IterationResult for dashboards, the
        enforcement handler is NOT called, and the circuit-breaker-adjacent
        success accounting proceeds normally.
        """
        # Default: reconciliation_enforcement=False.
        runner = _make_runner()
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._reconcile_post_execution_balances = AsyncMock(return_value={"incident": True, "breach": 1000})
        runner._format_reconciliation_error = MagicMock(return_value="recon failure (obs-mode)")
        runner._handle_execution_error = AsyncMock()
        # Spy on the enforcement handler to guarantee it is NOT called.
        runner._single_chain_handle_recon_incident = AsyncMock()  # type: ignore[method-assign]

        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        state = _make_state(strategy, intent=intent)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 0
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_context = ExecutionContext(deployment_id=strategy.deployment_id)

        with patch("almanak.framework.runner.strategy_runner.ResultEnricher") as MockEnricher:
            MockEnricher.return_value.enrich.return_value = state.last_execution_result
            result = await runner._single_chain_handle_success(state)

        assert result.status == IterationStatus.SUCCESS
        assert result.balance_reconciliation == {"incident": True, "breach": 1000}
        runner._single_chain_handle_recon_incident.assert_not_called()

    @pytest.mark.asyncio
    async def test_recon_incident_routes_to_recon_failed(self) -> None:
        # Enforcement gate (VIB-3348) is off by default; opt in explicitly here
        # since this test exercises the RECONCILIATION_FAILED branch.
        runner = _make_runner(reconciliation_enforcement=True)
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._reconcile_post_execution_balances = AsyncMock(return_value={"incident": True, "breach": 1000})
        runner._format_reconciliation_error = MagicMock(return_value="recon failure")
        runner._handle_execution_error = AsyncMock()

        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        state = _make_state(strategy, intent=intent)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 0
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_context = ExecutionContext(deployment_id=strategy.deployment_id)

        with patch("almanak.framework.runner.strategy_runner.ResultEnricher") as MockEnricher:
            MockEnricher.return_value.enrich.return_value = state.last_execution_result
            result = await runner._single_chain_handle_success(state)
        assert result.status == IterationStatus.RECONCILIATION_FAILED

    @pytest.mark.asyncio
    async def test_degraded_recon_incident_is_not_enforced(self) -> None:
        """VIB-3350 (H1): an incident on a DEGRADED report must NOT be enforced
        even with enforcement ON — an unpinned/no-receipt read cannot tell a real
        breach from the lagging-read race, so halting would punish a healthy
        strategy. It passes through to SUCCESS (logged loudly), handler not called.
        """
        runner = _make_runner(reconciliation_enforcement=True)
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._reconcile_post_execution_balances = AsyncMock(
            return_value={"incident": True, "breach": 1000, "reconciliation_degraded": True}
        )
        runner._format_reconciliation_error = MagicMock(return_value="recon failure (degraded)")
        runner._handle_execution_error = AsyncMock()
        # Spy: the enforcement finalizer must NOT be called for a degraded incident.
        runner._single_chain_handle_recon_incident = AsyncMock()  # type: ignore[method-assign]

        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("100"))
        state = _make_state(strategy, intent=intent)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 0
        state.last_execution_result = ExecutionResult(
            success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC)
        )
        state.last_execution_context = ExecutionContext(deployment_id=strategy.deployment_id)

        with patch("almanak.framework.runner.strategy_runner.ResultEnricher") as MockEnricher:
            MockEnricher.return_value.enrich.return_value = state.last_execution_result
            result = await runner._single_chain_handle_success(state)

        assert result.status == IterationStatus.SUCCESS
        runner._single_chain_handle_recon_incident.assert_not_called()
        # The degraded incident is NOT enforced, but it MUST remain observable on
        # the result so dashboards/logs still surface it (degraded != silent).
        assert result.balance_reconciliation == {
            "incident": True,
            "breach": 1000,
            "reconciliation_degraded": True,
        }


# =============================================================================
# _single_chain_handle_failure
# =============================================================================


class TestSingleChainHandleFailure:
    @pytest.mark.asyncio
    async def test_pre_execution_failure_no_diagnostics(self) -> None:
        """No last_execution_result -> diagnose_revert is skipped (compilation error)."""
        runner = _make_runner()
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._handle_execution_error = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(strategy, intent=intent)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 2
        state.state_machine.error = "compile failed"
        state.last_execution_result = None

        with patch(
            "almanak.framework.runner.strategy_runner.diagnose_revert",
            new=AsyncMock(),
        ) as mock_diagnose:
            result = await runner._single_chain_handle_failure(state)

        assert result.status == IterationStatus.EXECUTION_FAILED
        assert "compile failed" in (result.error or "")
        mock_diagnose.assert_not_called()
        runner._handle_execution_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_execution_attempted_triggers_diagnose_revert(self) -> None:
        runner = _make_runner()
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._handle_execution_error = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(strategy, intent=intent)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 2
        state.state_machine.error = "tx reverted"
        state.last_execution_result = ExecutionResult(
            success=False, phase=ExecutionPhase.CONFIRMATION, error="tx reverted"
        )

        with patch(
            "almanak.framework.runner.strategy_runner.diagnose_revert",
            new=AsyncMock(return_value=MagicMock(format=lambda: "diag")),
        ) as mock_diagnose:
            result = await runner._single_chain_handle_failure(state)

        assert result.status == IterationStatus.EXECUTION_FAILED
        mock_diagnose.assert_awaited_once()
        runner._handle_execution_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_diagnose_exception_does_not_abort(self) -> None:
        runner = _make_runner()
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._handle_execution_error = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(strategy, intent=intent)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 2
        state.state_machine.error = "reverted"
        state.last_execution_result = ExecutionResult(
            success=False, phase=ExecutionPhase.CONFIRMATION, error="reverted"
        )

        with patch(
            "almanak.framework.runner.strategy_runner.diagnose_revert",
            new=AsyncMock(side_effect=RuntimeError("diag bug")),
        ):
            result = await runner._single_chain_handle_failure(state)

        assert result.status == IterationStatus.EXECUTION_FAILED

    @pytest.mark.asyncio
    async def test_execution_result_without_error_backfilled_from_state_machine(
        self,
    ) -> None:
        """If result.error is empty, the helper copies state_machine.error onto it."""
        runner = _make_runner()
        runner._emit_execution_timeline_event = MagicMock()
        runner._write_ledger_entry = AsyncMock()
        runner._handle_execution_error = AsyncMock()
        strategy = _make_strategy()
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        state = _make_state(strategy, intent=intent)
        state.state_machine = MagicMock()
        state.state_machine.retry_count = 2
        state.state_machine.error = "sm error msg"
        state.last_execution_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.CONFIRMATION,
            error=None,  # empty -> helper backfills from state_machine.error
        )

        with patch(
            "almanak.framework.runner.strategy_runner.diagnose_revert",
            new=AsyncMock(return_value=MagicMock(format=lambda: "diag")),
        ):
            await runner._single_chain_handle_failure(state)

        # Backfill behaviour: last_execution_result.error now matches state_machine.error
        assert state.last_execution_result.error == "sm error msg"


# =============================================================================
# _single_chain_execute_clob
# =============================================================================


class TestSingleChainExecuteClob:
    @pytest.mark.asyncio
    async def test_clob_success_populates_execution_result(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        clob_handler = MagicMock()
        clob_result = SimpleNamespace(
            success=True,
            error=None,
            order_id="order-123",
            status=SimpleNamespace(value="FILLED"),
            to_prediction_fill=lambda: None,  # no fill detail
        )
        clob_handler.execute = AsyncMock(return_value=clob_result)
        state.clob_handler = clob_handler

        step_result = SimpleNamespace(action_bundle=SimpleNamespace(transactions=[]))
        execution_result = await runner._single_chain_execute_clob(state, step_result)

        assert execution_result.success is True
        assert execution_result.extracted_data["clob_status"] == "FILLED"
        assert execution_result.extracted_data["order_id"] == "order-123"
        assert state.last_execution_result is execution_result

    @pytest.mark.asyncio
    async def test_clob_with_prediction_fill_attaches_it(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        clob_handler = MagicMock()
        prediction_fill = MagicMock()
        clob_result = SimpleNamespace(
            success=True,
            error=None,
            order_id="oid",
            status=SimpleNamespace(value="FILLED"),
            to_prediction_fill=lambda: prediction_fill,
        )
        clob_handler.execute = AsyncMock(return_value=clob_result)
        state.clob_handler = clob_handler

        execution_result = await runner._single_chain_execute_clob(
            state, SimpleNamespace(action_bundle=SimpleNamespace())
        )
        assert execution_result.prediction_fill is prediction_fill

    @pytest.mark.asyncio
    async def test_clob_failure_has_no_order_id_in_extracted_data(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        clob_handler = MagicMock()
        clob_result = SimpleNamespace(
            success=False,
            error="rejected",
            order_id=None,
            status=SimpleNamespace(value="REJECTED"),
            to_prediction_fill=lambda: None,
        )
        clob_handler.execute = AsyncMock(return_value=clob_result)
        state.clob_handler = clob_handler

        execution_result = await runner._single_chain_execute_clob(
            state, SimpleNamespace(action_bundle=SimpleNamespace())
        )
        assert execution_result.success is False
        assert "order_id" not in execution_result.extracted_data
        assert execution_result.extracted_data["clob_status"] == "REJECTED"


# =============================================================================
# _single_chain_execute_onchain - native price refresh
# =============================================================================


class TestSingleChainExecuteOnchain:
    @pytest.mark.asyncio
    async def test_native_price_refreshed_from_oracle_before_execute(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.price_oracle = {"ETH": Decimal("2000")}

        # Orchestrator with a tx_risk_config that requires native price
        single_chain_orch = MagicMock()
        single_chain_orch.tx_risk_config = SimpleNamespace(
            max_gas_cost_usd=5,
            max_value_usd=1000,
            native_token_price_usd=0.0,
        )
        single_chain_orch.execute = AsyncMock(
            return_value=ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE, completed_at=datetime.now(UTC))
        )

        execution_context = ExecutionContext(deployment_id=strategy.deployment_id)
        step_result = SimpleNamespace(action_bundle=MagicMock())
        result = await runner._single_chain_execute_onchain(state, step_result, execution_context, single_chain_orch)

        assert result.success is True
        # The native token price was refreshed from the oracle
        assert single_chain_orch.tx_risk_config.native_token_price_usd == 2000.0

    @pytest.mark.asyncio
    async def test_no_tx_risk_config_skips_native_price_path(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.price_oracle = {"ETH": Decimal("2000")}

        single_chain_orch = MagicMock()
        single_chain_orch.tx_risk_config = None
        single_chain_orch.execute = AsyncMock(return_value=ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE))

        execution_context = ExecutionContext(deployment_id=strategy.deployment_id)
        await runner._single_chain_execute_onchain(
            state,
            SimpleNamespace(action_bundle=MagicMock()),
            execution_context,
            single_chain_orch,
        )
        # No attribute error; helper skips refresh cleanly
        single_chain_orch.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_price_oracle_missing_native_symbol_leaves_price_zero(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        state.price_oracle = {"USDC": Decimal("1")}  # no native ETH price

        single_chain_orch = MagicMock()
        single_chain_orch.tx_risk_config = SimpleNamespace(
            max_gas_cost_usd=5,
            max_value_usd=0,
            native_token_price_usd=0.0,
        )
        single_chain_orch.execute = AsyncMock(return_value=ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE))

        execution_context = ExecutionContext(deployment_id=strategy.deployment_id)
        await runner._single_chain_execute_onchain(
            state,
            SimpleNamespace(action_bundle=MagicMock()),
            execution_context,
            single_chain_orch,
        )
        # Stays at 0 because ETH was missing from the oracle -> risk guard fails closed
        assert single_chain_orch.tx_risk_config.native_token_price_usd == 0.0


# =============================================================================
# _single_chain_state_machine_loop
# =============================================================================


class TestSingleChainStateMachineLoop:
    @pytest.mark.asyncio
    async def test_completed_state_machine_returns_none_without_executing(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        sm = MagicMock()
        sm.is_complete = True  # already terminal
        state.state_machine = sm

        result = await runner._single_chain_state_machine_loop(state)

        assert result is None
        sm.step.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_delay_sleeps_then_continues(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        sm = MagicMock()
        # Step 1: retry_delay; step 2: is_complete terminal
        step1 = SimpleNamespace(
            retry_delay=0.0, needs_execution=False, action_bundle=None, error=None, is_complete=False
        )
        step_completion = SimpleNamespace(
            retry_delay=None,
            needs_execution=False,
            action_bundle=None,
            error=None,
            is_complete=True,
        )
        sm.is_complete = False
        sm.step = MagicMock(side_effect=[step1, step_completion])
        # Flip is_complete after the second step is consumed
        sm.retry_count = 1

        # Count iterations by toggling is_complete
        call_count = {"n": 0}

        def _step():
            call_count["n"] += 1
            if call_count["n"] == 1:
                return step1
            sm.is_complete = True
            return step_completion

        sm.step = MagicMock(side_effect=_step)
        state.state_machine = sm

        result = await runner._single_chain_state_machine_loop(state)
        assert result is None
        assert call_count["n"] == 2

    @pytest.mark.asyncio
    async def test_execute_step_returns_early_propagates(self) -> None:
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)
        sm = MagicMock()
        sm.is_complete = False

        action_bundle = MagicMock()
        step_exec = SimpleNamespace(
            retry_delay=None,
            needs_execution=True,
            action_bundle=action_bundle,
            error=None,
            is_complete=False,
        )
        sm.step = MagicMock(return_value=step_exec)
        state.state_machine = sm

        # Have _single_chain_execute_step return an early-exit result
        sentinel_result = MagicMock()
        runner._single_chain_execute_step = AsyncMock(return_value=sentinel_result)

        result = await runner._single_chain_state_machine_loop(state)
        assert result is sentinel_result

    @pytest.mark.asyncio
    async def test_step_error_without_completion_logs_warning_and_continues(self) -> None:
        """An error step with is_complete=False triggers the warn branch but loop continues."""
        runner = _make_runner()
        strategy = _make_strategy()
        state = _make_state(strategy)

        sm = MagicMock()
        call_count = {"n": 0}
        error_step = SimpleNamespace(
            retry_delay=None,
            needs_execution=False,
            action_bundle=None,
            error="boom",
            is_complete=False,
        )
        done_step = SimpleNamespace(
            retry_delay=None,
            needs_execution=False,
            action_bundle=None,
            error=None,
            is_complete=True,
        )

        def _step():
            call_count["n"] += 1
            if call_count["n"] == 1:
                return error_step
            sm.is_complete = True
            return done_step

        sm.is_complete = False
        sm.step = MagicMock(side_effect=_step)
        sm.retry_count = 1
        state.state_machine = sm
        # Match last_execution_result.error so the code takes the 'already logged' debug branch
        state.last_execution_result = SimpleNamespace(error="boom")

        result = await runner._single_chain_state_machine_loop(state)
        assert result is None
        assert call_count["n"] == 2
